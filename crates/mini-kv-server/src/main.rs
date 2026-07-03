use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = mini_kv_server::Args::parse();
    mini_kv_server::run_server(args).await
}