//! A standalone connection server: it holds this computer's Crazyradios and
//! shares the Crazyflies between every program attached to it.
//!
//! ``` text
//! cargo run --release --example server                            # ws://127.0.0.1:7777
//! cargo run --release --example server -- --listen 0.0.0.0:7777   # other computers too
//! cargo run --release --example server -- --toc-cache ~/.cache/crazyflie-connection-server
//! ```

use clap::Parser;
use crazyflie_connection_server::{FileTocCache, Server, ServerConfig, serve_on};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
struct Args {
    /// Address to listen on. Use 0.0.0.0:7777 for other computers (no authentication yet)
    #[arg(long, default_value = "127.0.0.1:7777")]
    listen: SocketAddr,

    /// Seconds a Crazyflie stays connected after its last client is gone
    #[arg(long, default_value_t = 10)]
    linger: u64,

    /// Directory to keep the Crazyflies' TOCs in, so that they aren't
    /// downloaded again after a restart. Without it they are kept in memory
    #[arg(long, value_name = "DIR")]
    toc_cache: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("crazyflie_connection_server=info"),
    )
    .init();
    let args = Args::parse();

    // Listen first, so that a second server fails before it claims any Crazyradio
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .map_err(|e| format!("listening on {}: {}", args.listen, e))?;
    let mut config = ServerConfig {
        linger: Duration::from_secs(args.linger),
        ..Default::default()
    };
    if let Some(dir) = &args.toc_cache {
        let cache = FileTocCache::new(dir)
            .map_err(|e| format!("creating the TOC cache {}: {}", dir.display(), e))?;
        config = config.with_toc_cache(cache);
    }

    let server = Server::new(config).await;
    if server.radios().is_empty() {
        eprintln!("Warning: no Crazyradio could be opened, only USB Crazyflies are reachable");
    }
    println!(
        "Connection server on ws://{} with Crazyradios {:?}. Ctrl-C stops it.",
        args.listen,
        server.radios()
    );
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    serve_on(server.clone(), listener, ctrl_c).await?;
    server.shutdown().await;
    Ok(())
}
