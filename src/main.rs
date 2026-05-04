use anyhow::Result;
use tracing_subscriber::{fmt, EnvFilter};

mod cli;
mod core;
mod registry;
mod ticket;
mod util;

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("ringdrop=info,warn")),
        )
        .with_target(false)
        .compact()
        .init();

    cli::run().await
}
