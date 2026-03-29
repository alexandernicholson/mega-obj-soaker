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

        if check_all_done(&coordinator).await {
            info!("All tasks completed, optimizer exiting");
            return ExitReason::Normal;
        }

        let Ok(CoordinatorReply::Stats { bytes }) = coordinator
            .call(CoordinatorCall::Stats, Duration::from_secs(5))
            .await
        else {
            info!("Optimizer: coordinator unavailable, exiting");
            return ExitReason::Normal;
        };

        let current_speed = bytes_to_mb_per_sec(bytes, config.optimization_interval);

        update_speed_history(&mut speed_history, current_speed);

        let avg_speed: f64 = if speed_history.is_empty() {
            current_speed
        } else {
            average(&speed_history)
        };

        info!("Current speed: {current_speed:.2} MB/s, Average speed: {avg_speed:.2} MB/s");

        if should_skip_scaleup(current_speed, previous_speed, config.max_speed) {
            previous_speed = current_speed;
            continue;
        }

        let scale_ctx = ScaleContext {
            coordinator: &coordinator,
            worker_supervisor: &worker_supervisor,
            client: &client,
            bucket: &bucket,
            destination: &destination,
            prefix: &prefix,
            config: &config,
        };
        current_worker_count =
            spawn_additional_workers(&scale_ctx, current_worker_count).await;

        previous_speed = current_speed;
    }
}

#[expect(clippy::cast_precision_loss)]
fn bytes_to_mb_per_sec(bytes: u64, interval: f64) -> f64 {
    bytes as f64 / interval / (1024.0 * 1024.0)
}

#[expect(clippy::cast_precision_loss)]
fn average(values: &VecDeque<f64>) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

async fn check_all_done(coordinator: &GenServerRef<Coordinator>) -> bool {
    if let Ok(CoordinatorReply::Progress { completed, total }) = coordinator
        .call(CoordinatorCall::Progress, Duration::from_secs(5))
        .await
    {
        completed >= total
    } else {
        true
    }
}

fn update_speed_history(speed_history: &mut VecDeque<f64>, current_speed: f64) {
    speed_history.push_back(current_speed);
    if speed_history.len() > 60 {
        speed_history.pop_front();
    }
}

fn should_skip_scaleup(current_speed: f64, previous_speed: f64, max_speed: f64) -> bool {
    if current_speed >= max_speed {
        info!("Maximum speed reached: {current_speed:.2} MB/s");
        return true;
    }

    if previous_speed > 0.0 {
        let speed_increase = (current_speed - previous_speed) / previous_speed;
        if speed_increase < 0.05 {
            info!("No significant speed improvement detected. Stopping ramp-up.");
            return true;
        }
    }

    false
}

struct ScaleContext<'a> {
    coordinator: &'a GenServerRef<Coordinator>,
    worker_supervisor: &'a DynamicSupervisorHandle,
    client: &'a Client,
    bucket: &'a str,
    destination: &'a str,
    prefix: &'a str,
    config: &'a Config,
}

async fn spawn_additional_workers(ctx: &ScaleContext<'_>, current_worker_count: usize) -> usize {
    if current_worker_count >= ctx.config.max_processes {
        return current_worker_count;
    }

    let new_count = (current_worker_count + 5).min(ctx.config.max_processes);
    let workers_to_add = new_count - current_worker_count;
    info!("Optimizing process count to {new_count} (+{workers_to_add})");

    for _ in 0..workers_to_add {
        let coord = ctx.coordinator.clone();
        let cl = ctx.client.clone();
        let b = ctx.bucket.to_string();
        let d = ctx.destination.to_string();
        let p = ctx.prefix.to_string();
        let cfg = ctx.config.clone();

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

        match ctx.worker_supervisor.start_child(entry).await {
            Ok(pid) => info!("Added worker process {pid:?}"),
            Err(e) => info!("Failed to add worker: {e}"),
        }
    }

    new_count
}
