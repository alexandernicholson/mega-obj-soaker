use std::path::Path;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use filetime::FileTime;
use tracing::{error, info, warn};

use crate::pattern::should_process_object;

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<i64>,
}

pub async fn create_s3_client(
    region: &str,
    endpoint_url: Option<&str>,
    _verify_ssl: bool,
) -> Client {
    let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_types::region::Region::new(region.to_string()));

    if let Some(endpoint) = endpoint_url {
        config_loader = config_loader.endpoint_url(endpoint);
    }

    let shared_config = config_loader.load().await;

    let s3_config_builder = S3ConfigBuilder::from(&shared_config).force_path_style(true);

    Client::from_conf(s3_config_builder.build())
}

pub async fn list_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
    include_patterns: &[String],
    exclude_patterns: &[String],
) -> Vec<S3Object> {
    info!(
        "Listing objects in s3://{}/{}",
        bucket, prefix
    );

    let mut objects = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix);

        if let Some(token) = &continuation_token {
            request = request.continuation_token(token);
        }

        match request.send().await {
            Ok(output) => {
                for obj in output.contents() {
                    let key = obj.key().unwrap_or_default();
                    if should_process_object(key, prefix, include_patterns, exclude_patterns) {
                        objects.push(S3Object {
                            key: key.to_string(),
                            size: obj.size().unwrap_or(0) as u64,
                            last_modified: obj
                                .last_modified()
                                .map(|dt: &aws_sdk_s3::primitives::DateTime| dt.secs()),
                        });
                    }
                }

                if output.is_truncated() == Some(true) {
                    continuation_token = output.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }
            Err(e) => {
                error!("Error listing objects: {}", e);
                break;
            }
        }
    }

    let total_size: u64 = objects.iter().map(|o| o.size).sum();
    info!(
        "Found {} objects with total size of {:.2} GB",
        objects.len(),
        total_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    objects
}

pub async fn download_object(
    client: &Client,
    obj: &S3Object,
    bucket: &str,
    destination: &str,
    prefix: &str,
    max_retries: u32,
    retry_delay: Duration,
) -> u64 {
    let relative_key = obj
        .key
        .strip_prefix(prefix)
        .unwrap_or(&obj.key)
        .trim_start_matches('/');

    let dest_path = Path::new(destination).join(relative_key);

    if let Some(parent) = dest_path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            error!("Failed to create directory {:?}: {}", parent, e);
            return 0;
        }
    }

    // Check if file exists and is up-to-date
    if dest_path.exists() {
        if let Ok(metadata) = tokio::fs::metadata(&dest_path).await {
            let local_size = metadata.len();
            if local_size == obj.size {
                if let Some(s3_mtime_secs) = obj.last_modified {
                    if let Ok(local_mtime) = metadata.modified() {
                        let local_secs = local_mtime
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64;
                        if local_secs >= s3_mtime_secs {
                            info!("File {} is up to date, skipping download", obj.key);
                            return 0;
                        }
                        info!("File {} is outdated, re-downloading", obj.key);
                    }
                } else {
                    info!(
                        "File {} exists with correct size, skipping download",
                        obj.key
                    );
                    return 0;
                }
            } else {
                info!(
                    "File {} exists but has incorrect size, resuming download",
                    obj.key
                );
            }
        }
    }

    // Download with retries
    let mut retries = 0;
    loop {
        match do_download(client, &obj.key, bucket, &dest_path).await {
            Ok(bytes) => {
                // Set file timestamp to match S3
                if let Some(s3_mtime_secs) = obj.last_modified {
                    let ft = FileTime::from_unix_time(s3_mtime_secs, 0);
                    if let Err(e) = filetime::set_file_mtime(&dest_path, ft) {
                        warn!("Failed to set mtime for {:?}: {}", dest_path, e);
                    }
                } else {
                    let now = FileTime::now();
                    let _ = filetime::set_file_mtime(&dest_path, now);
                }

                info!(
                    "Downloaded: s3://{}/{} to {:?}",
                    bucket, obj.key, dest_path
                );
                return bytes;
            }
            Err(e) => {
                retries += 1;
                error!(
                    "Error downloading {}: {}. Retry {}/{}",
                    obj.key, e, retries, max_retries
                );
                if retries >= max_retries {
                    error!(
                        "Failed to download {} after {} retries",
                        obj.key, max_retries
                    );
                    return 0;
                }
                tokio::time::sleep(retry_delay).await;
            }
        }
    }
}

async fn do_download(
    client: &Client,
    key: &str,
    bucket: &str,
    dest_path: &Path,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let resp = client.get_object().bucket(bucket).key(key).send().await?;

    let body = resp.body.collect().await?;
    let bytes = body.into_bytes();
    let len = bytes.len() as u64;

    tokio::fs::write(dest_path, &bytes).await?;

    Ok(len)
}
