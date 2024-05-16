use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use google_cloud_storage::client::Client;
use google_cloud_storage::http::objects::Object;
use google_cloud_storage::http::Error;
use http::{HeaderMap, StatusCode};

use crate::server::error::AppError;

pub async fn reply_with_fallback(
    path: &str,
    object_path: String,
    bucket: String,
    gcs_client: Client,
    error_response_message: String,
    fallback_url: String,
    fill_missing: bool,
) -> Result<Result<Response, AppError>, AppError> {
    let http_client = reqwest_client();
    let cache_response = http_client
        .get(format!("{fallback_url}/{object_path}"))
        .send()
        .await
        .map_err(|e| AppError(e.into()))?;

    Ok(match cache_response.error_for_status_ref() {
        Ok(_) => {
            println!("Found in fallback server: {object_path}");

            let content_length_opt = cache_response.content_length();
            let cache_response_headers = cache_response.headers();
            let content_type: String =
                header_value_or_empty(cache_response_headers, "content-type");
            let content_encoding =
                header_value_or_empty(cache_response_headers, "content-encoding");

            let mut headers = HeaderMap::new();
            if !content_type.is_empty() {
                headers.insert("Content-Type", content_type.parse().unwrap());
            }
            if !content_encoding.is_empty() {
                headers.insert("Content-Encoding", content_encoding.parse().unwrap());
            }
            if let Some(content_length) = content_length_opt {
                headers.insert(
                    "Content-Length",
                    content_length.to_string().parse().unwrap(),
                );
            }

            let mut stream = make_cloneable(cache_response.bytes_stream());

            use fork_stream::StreamExt as _;
            let shared = stream.fork();

            if fill_missing {
                let cloned = shared.clone();
                tokio::spawn(async move {
                    upload_fallback_data_to_bucket(
                        object_path,
                        bucket.to_string(),
                        gcs_client,
                        content_length_opt,
                        content_type,
                        cloned,
                    )
                    .await
                });
            }

            let body = Body::from_stream(shared);

            Ok((headers, body).into_response())
        },
        Err(_) => {
            let fallback_status = cache_response.status();
            println!("Fallback server response status {fallback_status} for {path}");
            Ok((StatusCode::NOT_FOUND, error_response_message).into_response())
        },
    })
}

async fn upload_fallback_data_to_bucket(
    object_path: String,
    bucket: String,
    gcs_client: Client,
    content_length_opt: Option<u64>,
    content_type: String,
    body: impl Stream<Item = Result<Bytes, String>> + Sized + Send + Sync + 'static,
) -> Result<Object, Error> {
    use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};

    let media_type = if content_type.is_empty() {
        "application/octet-stream".to_string()
    } else {
        content_type
    };
    let media = Media {
        name: object_path.into(),
        content_type: media_type.into(),
        content_length: content_length_opt,
    };
    let upload_type = UploadType::Simple(media);
    gcs_client
        .upload_streamed_object(
            &UploadObjectRequest {
                bucket: bucket.to_string(),
                ..Default::default()
            },
            body,
            &upload_type,
        )
        .await
}

fn header_value_or_empty(headers: &reqwest::header::HeaderMap, name: &str) -> String {
    match headers.get(name) {
        None => "".to_string(),
        Some(value) => value.to_str().unwrap_or("").to_string(),
    }
}

fn make_cloneable(
    stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Sized,
) -> impl Stream<Item = Result<Bytes, String>> + Sized {
    stream.map(|item| item.map_err(|e| "failed".into()))
}

// sharing a client instance with OneCell as recommended here:
// https://github.com/awslabs/aws-lambda-rust-runtime/issues/123
fn reqwest_client() -> &'static reqwest::Client {
    use once_cell::sync::OnceCell;
    static INSTANCE: OnceCell<reqwest::Client> = OnceCell::new();
    INSTANCE.get_or_init(reqwest::Client::new)
}

// fn flatten_reqwuest_stream(stream: impl Stream<Item=Result<Bytes, reqwest::Error>> + Sized)
//     -> impl Stream<Item=Bytes> {
//     stream
//         .map(|item| item.unwrap())
// }
