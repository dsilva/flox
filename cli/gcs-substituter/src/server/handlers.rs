use std::fs::OpenOptions;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::Error;
use http::{HeaderMap, StatusCode};
use tokio::sync::Semaphore;

use crate::server::error::AppError;
use crate::server::fallback::reply_with_fallback;

static MISSING_FILE_PERMITS: Semaphore = Semaphore::const_new(1);

#[derive(Clone)]
pub struct AppState {
    pub bucket: String,
    /// When an object is not in the GCS bucket, look in a fallback location.
    /// If this is empty, we return a 404 instead.
    pub fallback_url: String,
    /// When responding with data from a fallback location for an object missing in the GCS bucket,
    /// write the fetched data to the bucket as well.
    pub fill_missing: bool,
    /// If not empty, write a list of missing objects to this file.
    pub missing_objects_filename: String,
}

pub async fn handle_root() -> &'static str {
    "This is gcs-substituter"
}

pub async fn handle_nix_cache_info() -> &'static str {
    "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n"
}

pub async fn handle_with_gcs(
    State(state): State<AppState>,
    req: Request,
) -> Result<Response, AppError> {
    let path = req.uri().path();
    let object_path = path[1..].to_string();
    println!("got path {path} and objpath {object_path}");

    let bucket = state.bucket.clone();

    let config = ClientConfig::default()
        .with_auth()
        .await
        .map_err(|e| AppError(e.into()))?;
    let gcs_client = Client::new(config);

    let gcs_result = gcs_client
        .get_object(&GetObjectRequest {
            bucket: bucket.to_string(),
            object: object_path.to_string(),
            ..Default::default()
        })
        .await;

    match gcs_result {
        Ok(object) => {
            println!("found {} with size {}", object.name, object.size);

            let download_result = gcs_client
                .download_streamed_object(
                    &GetObjectRequest {
                        bucket: bucket.to_string(),
                        object: object_path.to_string(),
                        ..Default::default()
                    },
                    &Range::default(),
                )
                .await;

            match download_result {
                Ok(stream) => {
                    let mut headers = HeaderMap::new();
                    if let Some(encoding) = object.content_encoding {
                        headers.insert("Content-Encoding", encoding.parse().unwrap());
                    }
                    if let Some(content_type) = object.content_type {
                        headers.insert("Content-Type", content_type.parse().unwrap());
                    }
                    headers.insert("Content-Length", object.size.to_string().parse().unwrap());
                    let body = Body::from_stream(stream);

                    Ok((headers, body).into_response())
                },
                Err(error) => Err(AppError(error.into())),
            }
        },
        Err(error) => match error {
            Error::Response(error_response) => {
                let code = error_response.code;
                let message = error_response.message;

                if code == 404 {
                    handle_gcs_not_found(
                        state,
                        path,
                        object_path,
                        bucket.to_string(),
                        gcs_client,
                        message,
                    )
                    .await?
                } else {
                    let status =
                        StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

                    Ok((status, message).into_response())
                }
            },
            _ => Err(AppError(error.into())),
        },
    }
}

async fn handle_gcs_not_found(
    state: AppState,
    path: &str,
    object_path: String,
    bucket: String,
    gcs_client: Client,
    message: String,
) -> Result<Result<Response, AppError>, AppError> {
    println!("Could not find in GCS: {path}");
    let missing_objects_filename = state.missing_objects_filename;
    if !missing_objects_filename.is_empty() {
        record_missing_object(path.to_string(), missing_objects_filename).await;
    }

    let fallback_url = state.fallback_url;
    Ok(if !fallback_url.is_empty() {
        let fill_missing = state.fill_missing;
        reply_with_fallback(
            path,
            object_path,
            bucket,
            gcs_client,
            message,
            fallback_url,
            fill_missing,
        )
        .await?
    } else {
        Ok((StatusCode::NOT_FOUND, message).into_response())
    })
}

async fn record_missing_object(missing_path: String, filename: String) {
    // Avoid concurrent writes or we'll get malformed lines
    let _permit = MISSING_FILE_PERMITS.acquire().await.unwrap();

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(filename.as_str())
        .expect(format!("Couldn't open {filename}").as_str());

    use std::io::Write;
    writeln!(file, "{}", missing_path).expect("Couldn't write missing path");
}
