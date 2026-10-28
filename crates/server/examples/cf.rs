//! Small CLI on the client API. The same commands work in every mode:
//!
//! ``` text
//! cargo run --example cf -- scan                        # auto: local server if running, else direct
//! cargo run --example cf -- --connection direct params radio:///80/2M/E7E7E7E7E7
//! CRAZYFLIE_CONNECTION=lab-pc:7777 cargo run --example cf -- log radio:///80/2M/E7E7E7E7E7 pm.vbat
//! ```

use clap::{Parser, Subcommand};
use crazyflie_connection_server::{Client, Crazyflie};
use futures::StreamExt;
use std::time::Duration;

#[derive(Parser)]
struct Args {
    /// direct, shared, auto, local or a server address (host, host:port, ws://…)
    #[arg(long, env = "CRAZYFLIE_CONNECTION", default_value = "auto")]
    connection: String,

    /// Client name shown to other clients
    #[arg(long)]
    name: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scan for Crazyflies
    Scan {
        /// Radio address, 10 hex digits
        #[arg(long)]
        address: Option<String>,
    },
    /// List the Crazyflies the server is connected to, and their clients
    List,
    /// The server's Crazyradios and the link statistics of the Crazyflies on each
    Radios,
    /// Firmware and TOC sizes
    Info { uri: String },
    /// List parameters with their types and values
    Params { uri: String },
    /// Read a parameter
    Get { uri: String, name: String },
    /// Set a parameter
    Set {
        uri: String,
        name: String,
        value: f64,
    },
    /// Print every parameter change, made by any client
    WatchParams { uri: String },
    /// Print log variables
    Log {
        uri: String,
        vars: Vec<String>,
        #[arg(long, default_value_t = 100)]
        period_ms: u64,
        /// Stop after this many samples
        #[arg(long)]
        count: Option<usize>,
    },
    /// Print console output
    Console {
        uri: String,
        #[arg(long)]
        no_history: bool,
    },
    /// Arm or disarm
    Arm {
        uri: String,
        #[arg(long)]
        disarm: bool,
    },
    /// High-level commander take-off
    Takeoff {
        uri: String,
        #[arg(long, default_value_t = 0.5)]
        height: f32,
        #[arg(long, default_value_t = 2.0)]
        duration: f32,
    },
    /// High-level commander landing
    Land {
        uri: String,
        #[arg(long, default_value_t = 0.0)]
        height: f32,
        #[arg(long, default_value_t = 2.0)]
        duration: f32,
    },
    /// High-level commander go-to
    Goto {
        uri: String,
        #[arg(allow_negative_numbers = true)]
        x: f32,
        #[arg(allow_negative_numbers = true)]
        y: f32,
        #[arg(allow_negative_numbers = true)]
        z: f32,
        #[arg(long, default_value_t = 0.0, allow_negative_numbers = true)]
        yaw: f32,
        #[arg(long, default_value_t = 2.0)]
        duration: f32,
        #[arg(long)]
        relative: bool,
    },
    /// High-level commander stop (motors off)
    Stop { uri: String },
    /// Emergency stop
    Estop { uri: String },
    /// Radio link statistics
    Stats { uri: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let args = Args::parse();
    let client = Client::open(&args.connection).await?;
    if let Some(name) = &args.name {
        client.set_name(name).await?;
    }
    eprintln!("Using {}", client.mode());

    match args.command {
        Command::Scan { address } => {
            let address = address.map(|a| parse_address(&a)).transpose()?;
            for cf in client.scan(address).await? {
                println!("{}{}", cf.uri, clients(&cf.clients));
            }
        }
        Command::List => {
            for cf in client.list().await? {
                println!("{}{}", cf.uri, clients(&cf.clients));
            }
        }
        Command::Radios => {
            for radio in client.radios().await? {
                println!("Radio {}", radio.index);
                for link in radio.links {
                    let s = link.statistics;
                    println!(
                        "  {}  {:.0} % acked, {:.0} packets/s sent, {:.0}/s received, {:.2} retries{}",
                        link.uri,
                        s.link_quality.unwrap_or(0.0) * 100.0,
                        s.radio_send_rate.unwrap_or(0.0),
                        s.downlink_rate.unwrap_or(0.0),
                        s.avg_retries.unwrap_or(0.0),
                        clients(&link.clients)
                    );
                }
            }
        }
        Command::Info { uri } => {
            let cf = client.attach(&uri).await?;
            println!("URI:      {}", cf.uri());
            println!(
                "Firmware: {}",
                cf.platform.firmware_version().unwrap_or("?")
            );
            println!(
                "Device:   {}",
                cf.platform.device_type_name().unwrap_or("?")
            );
            println!("Params:   {}", cf.param.names().len());
            println!("Log vars: {}", cf.log.names().len());
        }
        Command::Params { uri } => {
            let cf = client.attach(&uri).await?;
            for name in cf.param.names() {
                let value = cf.param.get_lossy(&name).await?;
                let access = if cf.param.is_writable(&name)? {
                    "rw"
                } else {
                    "ro"
                };
                println!(
                    "{name:<40} {:<4} {access} {value}",
                    cf.param.get_type(&name)?
                );
            }
        }
        Command::Get { uri, name } => {
            let cf = client.attach(&uri).await?;
            println!("{}", cf.param.get_lossy(&name).await?);
        }
        Command::Set { uri, name, value } => {
            let cf = client.attach(&uri).await?;
            cf.param.set_lossy(&name, value).await?;
        }
        Command::WatchParams { uri } => {
            let cf = client.attach(&uri).await?;
            let mut changes = cf.param.watch_change().await?;
            until_disconnect(&cf, async {
                while let Some((name, value)) = changes.next().await {
                    println!("{name} = {value}");
                }
            })
            .await;
        }
        Command::Log {
            uri,
            vars,
            period_ms,
            count,
        } => {
            let cf = client.attach(&uri).await?;
            let vars: Vec<&str> = vars.iter().map(String::as_str).collect();
            let mut stream = cf
                .log
                .subscribe(&vars, Duration::from_millis(period_ms))
                .await?;
            let mut printed = 0;
            while count.is_none_or(|count| printed < count) {
                let sample = stream.next().await?;
                let values: Vec<String> = vars
                    .iter()
                    .map(|var| format!("{var}={:.3}", sample.data.get(*var).unwrap_or(&f64::NAN)))
                    .collect();
                println!("{:>10} {}", sample.timestamp, values.join(" "));
                printed += 1;
            }
        }
        Command::Console { uri, no_history } => {
            let cf = client.attach(&uri).await?;
            let mut lines = if no_history {
                cf.console.line_stream_no_history().await?
            } else {
                cf.console.line_stream().await?
            };
            until_disconnect(&cf, async {
                while let Some(line) = lines.next().await {
                    println!("{line}");
                }
            })
            .await;
        }
        Command::Arm { uri, disarm } => {
            let cf = client.attach(&uri).await?;
            cf.supervisor.send_arming_request(!disarm).await?;
        }
        Command::Takeoff {
            uri,
            height,
            duration,
        } => {
            let cf = client.attach(&uri).await?;
            cf.high_level_commander
                .take_off(height, None, duration, None)
                .await?;
            tokio::time::sleep(Duration::from_secs_f32(duration)).await;
        }
        Command::Land {
            uri,
            height,
            duration,
        } => {
            let cf = client.attach(&uri).await?;
            cf.high_level_commander
                .land(height, None, duration, None)
                .await?;
            tokio::time::sleep(Duration::from_secs_f32(duration)).await;
            cf.high_level_commander.stop(None).await?;
        }
        Command::Goto {
            uri,
            x,
            y,
            z,
            yaw,
            duration,
            relative,
        } => {
            let cf = client.attach(&uri).await?;
            cf.high_level_commander
                .go_to(x, y, z, yaw, duration, relative, false, None)
                .await?;
            tokio::time::sleep(Duration::from_secs_f32(duration)).await;
        }
        Command::Stop { uri } => {
            let cf = client.attach(&uri).await?;
            cf.high_level_commander.stop(None).await?;
        }
        Command::Estop { uri } => {
            let cf = client.attach(&uri).await?;
            cf.supervisor.send_emergency_stop().await?;
        }
        Command::Stats { uri } => {
            let cf = client.attach(&uri).await?;
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                println!("{:?}", cf.link_service.get_statistics().await?);
            }
        }
    }
    Ok(())
}

/// Run `work` until it finishes, the Crazyflie disconnects or Ctrl-C
async fn until_disconnect(cf: &Crazyflie, work: impl Future<Output = ()>) {
    tokio::select! {
        _ = work => {}
        reason = cf.wait_disconnect() => eprintln!("Disconnected: {reason}"),
        _ = tokio::signal::ctrl_c() => {}
    }
}

fn clients(clients: &[String]) -> String {
    if clients.is_empty() {
        String::new()
    } else {
        format!("  [{}]", clients.join(", "))
    }
}

fn parse_address(address: &str) -> Result<[u8; 5], String> {
    let invalid = || format!("Invalid address {address}");
    if address.len() != 10 {
        return Err(invalid());
    }
    let mut bytes = [0u8; 5];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&address[2 * i..2 * i + 2], 16).map_err(|_| invalid())?;
    }
    Ok(bytes)
}
