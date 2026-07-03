use std::time::Duration;

use clap::{Parser, Subcommand};
use mini_kv_client::MiniKvClient;

#[derive(Parser)]
#[command(name = "minikv", about = "Mini KV command line tool")]
struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:3456", env = "MINI_KV_SERVER")]
    server: String,

    #[arg(short, long, default_value = "text")]
    format: String,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
        #[arg(long)]
        ttl_secs: Option<u64>,
    },
    Delete {
        key: String,
    },
    Scan {
        start: String,
        end: String,
    },
    Prefix {
        prefix: String,
    },
    Flush,
    Health,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = MiniKvClient::new(&cli.server);

    match cli.cmd {
        Commands::Get { key } => {
            match client.get(&key).await? {
                Some(val) => println!("{val}"),
                None => println!("(not found)"),
            }
        }
        Commands::Put { key, value, ttl_secs } => {
            let ttl = ttl_secs.map(Duration::from_secs);
            client.put_with_ttl(&key, &value, ttl).await?;
            println!("OK");
        }
        Commands::Delete { key } => {
            client.delete(&key).await?;
            println!("OK");
        }
        Commands::Scan { start, end } => {
            let items = client.scan(&start, &end).await?;
            for item in items {
                println!("{} = {}", item.key, item.value);
            }
        }
        Commands::Prefix { prefix } => {
            let items = client.scan_prefix(&prefix).await?;
            for item in items {
                println!("{} = {}", item.key, item.value);
            }
        }
        Commands::Flush => {
            client.flush().await?;
            println!("OK");
        }
        Commands::Health => {
            let health = client.health().await?;
            if cli.format == "json" {
                println!("{}", serde_json::to_string_pretty(&health)?);
            } else {
                println!("status: {}", health.status);
                println!("key_count: {}", health.key_count);
                println!("ttl_key_count: {}", health.ttl_key_count);
                println!("storage_bytes: {}", health.storage_bytes);
                println!("expired_cleanup_count: {}", health.expired_cleanup_count);
                println!("uptime_secs: {}", health.uptime_secs);
            }
        }
    }

    Ok(())
}