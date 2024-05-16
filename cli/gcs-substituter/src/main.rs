use std::io::Write;

use axum::response::IntoResponse;
use axum::RequestExt;
use clap::Parser;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::server::routing::build_router;

mod server;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser)]
struct Args {
    /// Google Cloud Storage bucket name containing Nix binary cache objects
    #[arg(short, long)]
    pub bucket: String,
    /// Base URL for the fallback location to check for objects that are not in GCS
    #[arg(long)]
    pub fallback: Option<String>,
    /// Write data from the fallback location to the bucket as well
    #[arg(long, default_value_t = false)]
    pub fill_missing: bool,
    /// Write a list of missing objects to this file
    #[arg(short, long)]
    pub missing_objects_filename: Option<String>,
    /// Address to bind the server.  Defaults to the same port as nix-serve (5000)
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    pub address: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let address = args.address.clone();

    let app = build_router(args);
    let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
    println!("Listening on {address}");
    axum::serve(listener, app).await.unwrap();
}
