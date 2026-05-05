use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    ringdrop::cli::run().await
}
