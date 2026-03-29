use std::sync::Arc;

use aws_sdk_s3::Client;
use rebar::runtime::Runtime;
use rebar::supervisor::dynamic::{DynamicSupervisorSpec, start_dynamic_supervisor};
use rebar::supervisor::engine::ChildEntry;
use rebar::supervisor::spec::{ChildSpec, RestartType};
use tracing::info;

use crate::config::Config;
use crate::coordinator::start_coordinator;
use crate::optimizer::optimizer_loop;
use crate::s3::S3Object;
use crate::worker::worker_loop;

pub async fn run_download(
    runtime: Arc<Runtime>,
    client: Client,
    bucket: String,
    prefix: String,
    destination: String,
    objects: Vec<S3Object>,
    config: Config,
) {
    if objects.is_empty() {
        tracing::warn!("No objects found to download. Exiting.");
        return;
    }

    // Create destination directory
    if let Err(e) = tokio::fs::create_dir_all(&destination).await {
        tracing::error!("Failed to create destination directory: {}", e);
        return;
    }

    // 1. Start the Coordinator GenServer
    let coordinator = start_coordinator(Arc::clone(&runtime), objects).await;
    info!("Coordinator started at {:?}", coordinator.pid());

    // 2. Start the WorkerSupervisor (DynamicSupervisor)
    let worker_supervisor = start_dynamic_supervisor(
        Arc::clone(&runtime),
        DynamicSupervisorSpec::new().max_restarts(100).max_seconds(60),
    )
    .await;
    info!("Worker supervisor started at {:?}", worker_supervisor.pid());

    // 3. Spawn initial workers under the DynamicSupervisor
    for i in 0..config.min_processes {
        let coord = coordinator.clone();
        let cl = client.clone();
        let b = bucket.clone();
        let d = destination.clone();
        let p = prefix.clone();
        let cfg = config.clone();

        let entry = ChildEntry::new(
            ChildSpec::new(format!("worker-{i}")).restart(RestartType::Temporary),
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
            Ok(pid) => info!("Worker {} started at {:?}", i, pid),
            Err(e) => tracing::error!("Failed to start worker {}: {}", i, e),
        }
    }

    // 4. Start the Optimizer as a process
    let opt_coord = coordinator.clone();
    let opt_ws = worker_supervisor.clone();
    let opt_client = client.clone();
    let opt_bucket = bucket.clone();
    let opt_dest = destination.clone();
    let opt_prefix = prefix.clone();
    let opt_config = config.clone();

    let optimizer_pid = runtime
        .spawn(move |_ctx| async move {
            optimizer_loop(
                opt_coord,
                opt_ws,
                opt_client,
                opt_bucket,
                opt_dest,
                opt_prefix,
                opt_config,
            )
            .await;
        })
        .await;
    info!("Optimizer started at {:?}", optimizer_pid);

    // 5. Wait for completion by polling the coordinator
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let progress = coordinator
            .call(
                crate::coordinator::CoordinatorCall::Progress,
                std::time::Duration::from_secs(5),
            )
            .await;

        match progress {
            Ok(crate::coordinator::CoordinatorReply::Progress { completed, total }) => {
                if completed >= total {
                    info!("Download completed: {}/{} tasks", completed, total);
                    break;
                }
            }
            _ => {
                // Coordinator gone
                break;
            }
        }
    }

    // 6. Cleanup
    worker_supervisor.shutdown();
    info!("All processes have been terminated.");
}
