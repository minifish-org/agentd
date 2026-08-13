use anyhow::Result;
use clap::Parser;

mod auth;
mod config;
mod dispatch;
mod handlers;
mod responses;
mod scheduler;
mod server;
mod state;
mod turns;

pub(crate) use responses::{error_response, json_result, parse_uuid};
pub(crate) use state::AppState;

use config::{resolve_config_path, Cli};
use server::run_server;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();

    let cli = Cli::parse();
    let config_path = resolve_config_path(cli.config.as_deref());
    run_server(&config_path, cli.reset_data).await
}
