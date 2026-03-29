use std::time::Duration;

use aws_sdk_s3::Client;
use rebar::gen_server::GenServerRef;
use rebar::process::ExitReason;
use tracing::{debug, info};

use crate::config::Config;
use crate::coordinator::{Coordinator, CoordinatorCall, CoordinatorCast, CoordinatorReply};
use crate::s3::download_object;

pub async fn worker_loop(
    coordinator: GenServerRef<Coordinator>,
    client: Client,
    bucket: String,
    destination: String,
    prefix: String,
    config: Config,
) -> ExitReason {
    let retry_delay = Duration::from_secs_f64(config.retry_delay);

    loop {
        let reply = coordinator
            .call(CoordinatorCall::GetTask, Duration::from_secs(5))
            .await;

        match reply {
            Ok(CoordinatorReply::Task(Some(obj))) => {
                let bytes = download_object(
                    &client,
                    &obj,
                    &bucket,
                    &destination,
                    &prefix,
                    config.max_retries,
                    retry_delay,
                )
                .await;

                let _ = coordinator.cast(CoordinatorCast::TaskComplete { bytes });
            }
            Ok(CoordinatorReply::Task(None)) => {
                // No more tasks — exit normally
                debug!("Worker: no more tasks, exiting");
                return ExitReason::Normal;
            }
            Ok(_) => {
                // Unexpected reply type
                return ExitReason::Abnormal("unexpected reply from coordinator".into());
            }
            Err(e) => {
                // Coordinator dead or timeout
                info!("Worker: coordinator unavailable ({}), exiting", e);
                return ExitReason::Normal;
            }
        }
    }
}
