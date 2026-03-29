use std::collections::VecDeque;
use std::time::Duration;

use aws_sdk_s3::Client;
use rebar::gen_server::GenServerRef;
use rebar::process::ExitReason;
use rebar::supervisor::dynamic::DynamicSupervisorHandle;
use rebar::supervisor::engine::ChildEntry;
use rebar::supervisor::spec::{ChildSpec, RestartType};
use tracing::info;

use crate::config::Config;
use crate::coordinator::{Coordinator, CoordinatorCall, CoordinatorReply};
use crate::worker::worker_loop;

pub async fn optimizer_loop(
    coordinator: GenServerRef<Coordinator>,
    worker_supervisor: DynamicSupervisorHandle,
    client: Client,
    bucket: String,
    destination: String,
    prefix: String,
    config: Config,
) -> ExitReason {
    let interval = Duration::from_secs_f64(config.optimization_interval);
    let mut speed_history: VecDeque<f64> = VecDeque::with_capacity(60);
    let mut previous_speed: f64 = 0.0;
    let mut current_worker_count = config.min_processes;

    loop {
        tokio::time::sleep(interval).await;

        // Check if all tasks are done
        let progress = coordinator
            .call(CoordinatorCall::GetProgress, Duration::from_secs(5))
            .await;

        match progress {
            Ok(CoordinatorReply::Progress { completed, total }) => {
                if completed >= total {
                    info!("All tasks completed, optimizer exiting");
                    return ExitReason::Normal;
                }
            }
            _ => {
                info!("Optimizer: coordinator unavailable, exiting");
                return ExitReason::Normal;
            }
        }

        // Get stats from coordinator
        let stats = coordinator
            .call(CoordinatorCall::GetStats, Duration::from_secs(5))
            .await;

        let bytes = match stats {
            Ok(CoordinatorReply::Stats { bytes }) => bytes,
            _ => {
                info!("Optimizer: coordinator unavailable, exiting");
                return ExitReason::Normal;
            }
        };

        let current_speed =
            bytes as f64 / config.optimization_interval / (1024.0 * 1024.0); // MB/s

        speed_history.push_back(current_speed);
        if speed_history.len() > 60 {
            speed_history.pop_front();
        }

        let avg_speed: f64 = if speed_history.is_empty() {
            current_speed
        } else {
            speed_history.iter().sum::<f64>() / speed_history.len() as f64
        };

        info!(
            "Current speed: {:.2} MB/s, Average speed: {:.2} MB/s",
            current_speed, avg_speed
        );

        if current_speed >= config.max_speed {
            info!("Maximum speed reached: {:.2} MB/s", current_speed);
            previous_speed = current_speed;
            continue;
        }

        // Check improvement threshold
        if previous_speed > 0.0 {
            let speed_increase = (current_speed - previous_speed) / previous_speed;
            if speed_increase < 0.05 {
                info!("No significant speed improvement detected. Stopping ramp-up.");
                previous_speed = current_speed;
                continue;
            }
        }

        // Scale up workers
        if current_worker_count < config.max_processes {
            let mut new_count = current_worker_count + 5;
            if new_count > config.max_processes {
                new_count = config.max_processes;
            }

            let workers_to_add = new_count - current_worker_count;
            info!(
                "Optimizing process count to {} (+{})",
                new_count, workers_to_add
            );

            for _ in 0..workers_to_add {
                let coord = coordinator.clone();
                let cl = client.clone();
                let b = bucket.clone();
                let d = destination.clone();
                let p = prefix.clone();
                let cfg = config.clone();

                let entry = ChildEntry::new(
                    ChildSpec::new("worker").restart(RestartType::Temporary),
                    move || {
                        let coord = coord.clone();
                        let cl = cl.clone();
                        let b = b.clone();
                        let d = d.clone();
                        let p = p.clone();
                        let cfg = cfg.clone();
                        async move { worker_loop(coord, cl, b, d, p, cfg).await }
                    },
                );

                match worker_supervisor.start_child(entry).await {
                    Ok(pid) => {
                        info!("Added worker process {:?}", pid);
                    }
                    Err(e) => {
                        info!("Failed to add worker: {}", e);
                    }
                }
            }

            current_worker_count = new_count;
        }

        previous_speed = current_speed;
    }
}
