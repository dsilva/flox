use axum::routing::get;
use axum::Router;

use crate::server::handlers::{handle_nix_cache_info, handle_root, handle_with_gcs, AppState};
use crate::Args;

pub fn build_router(args: Args) -> Router {
    let state = AppState {
        bucket: args.bucket,
        fallback_url: args.fallback.unwrap_or("".to_string()),
        fill_missing: args.fill_missing,
        missing_objects_filename: args.missing_objects_filename.unwrap_or("".to_string()),
    };
    Router::new()
        .route("/", get(handle_root))
        .route("/nix-cache-info", get(handle_nix_cache_info))
        .fallback(get(handle_with_gcs))
        .with_state(state)
}
