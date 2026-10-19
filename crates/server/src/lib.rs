//! # Crazyflie connection server
//!
//! Share Crazyradios and Crazyflie connections between programs, on this
//! machine or across the network. Programs use one API, whether this process
//! drives the Crazyradios itself or goes through a connection server on this
//! or another machine. The mode is picked at runtime:
//!
//! ``` no_run
//! # async fn example() -> crazyflie_connection_server::Result<()> {
//! // "direct", "shared", "auto", "local" or "lab-pc:7777"; from_env() reads CRAZYFLIE_CONNECTION
//! let client = crazyflie_connection_server::Client::open("auto").await?;
//! let found = client.scan(None).await?;
//! let cf = client.attach(&found[0].uri).await?;
//! cf.param.set_lossy("led.bitmask", 3.0).await?;
//! let mut log = cf.log.subscribe(&["pm.vbat"], std::time::Duration::from_millis(100)).await?;
//! println!("{:?}", log.next().await?);
//! # Ok(())
//! # }
//! ```
//!
//! Method names follow `crazyflie-lib` where the meaning is the same. The main
//! difference is the log: instead of creating blocks, a client subscribes to
//! variables and the server manages the blocks.
//!
//! In `direct` and `shared` mode the [Server] runs inside this process (cargo
//! feature `server`, on by default), so the code above works without
//! starting a server. Without the feature only the WebSocket modes are left,
//! and crazyflie-lib, libusb and axum are not built.

mod core;
mod crazyflie;
#[cfg(feature = "server")]
mod server;

pub use crate::crazyflie::{
    Commander, Console, Crazyflie, HighLevelCommander, Lighthouse, LighthouseAngleData, LighthouseAngles, LinkService,
    Localization, Log, LogData, LogStream, Param, Platform, Supervisor,
};
pub use crazyflie_connection_protocol::{
    CfInfo, DEFAULT_PORT, FlashStart, LinkStatistics, NormalizedUri, PersistentParamState, TocItem, normalize_uri,
};

#[cfg(feature = "server")]
pub use crate::server::{Server, ServerConfig, Session, TocStore, serve, serve_on};
/// The crazyflie-lib the server runs on, for [Client::adopt] and [Crazyflie::lib]
#[cfg(feature = "server")]
pub use crazyflie_lib;

use crate::core::Core;
use crazyflie_connection_protocol::{Event, Request, Response};
use std::sync::Arc;

/// Environment variable read by [Client::from_env]
pub const CONNECTION_ENV: &str = "CRAZYFLIE_CONNECTION";

/// Client errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The server (or the Crazyflie behind it) reported an error
    #[error("{0}")]
    Server(String),
    /// The connection to the server is gone
    #[error("Connection to the server lost")]
    ConnectionLost,
    /// Opening the connection to the server failed
    #[error("{0}")]
    Transport(String),
    /// No param or log variable with this name
    #[error("Unknown name: {0}")]
    UnknownName(String),
    /// The server answered with something this client did not expect
    #[error("Unexpected reply from the server")]
    UnexpectedReply,
}

pub type Result<T> = std::result::Result<T, Error>;

/// A client of a connection server, in this process (`direct`, `shared`) or
/// over WebSocket
///
/// Cheap to clone. The connection closes when the last clone and every
/// [Crazyflie] attached through it are dropped.
#[derive(Clone)]
pub struct Client {
    core: Arc<Core>,
}

impl Client {
    /// Open a connection in the given mode:
    /// - `direct`: run the server in this process, using this machine's Crazyradios
    /// - `shared`: like `direct`, and let other programs on this machine attach
    ///   to it on `127.0.0.1:7777` (the server lives as long as this program)
    /// - `auto`: use the local server if one is running, otherwise `direct`
    /// - `local`: the server on this machine (`127.0.0.1:7777`)
    /// - anything else: a server address, `host`, `host:port` or a `ws://` URL
    pub async fn open(mode: &str) -> Result<Self> {
        #[cfg(feature = "server")]
        return Self::open_with(mode, InProcess::default()).await;
        #[cfg(not(feature = "server"))]
        {
            let server = match mode {
                "direct" | "shared" => Self::direct().await?,
                "auto" | "" | "local" => Self::connect(&local_address()).await?,
                address => Self::connect(address).await?,
            };
            server.set_name(&default_name()).await?;
            Ok(server)
        }
    }

    /// Like [Client::open], with the settings for running the server in this process
    #[cfg(feature = "server")]
    pub async fn open_with(mode: &str, in_process: InProcess) -> Result<Self> {
        let server = match mode {
            "direct" => Self::direct_with(in_process.config).await?,
            "shared" => Self::shared(in_process.config).await?,
            "auto" | "" => match Self::connect(&local_address()).await {
                Ok(server) => server,
                Err(_) if in_process.auto_shared => match Self::shared(in_process.config.clone()).await {
                    Ok(server) => server,
                    Err(e) => {
                        // The port is taken by something that isn't a server: keep it private
                        log::warn!("{e}, other programs can't attach");
                        Self::direct_with(in_process.config).await?
                    }
                },
                Err(_) => Self::direct_with(in_process.config).await?,
            },
            "local" => Self::connect(&local_address()).await?,
            address => Self::connect(address).await?,
        };
        server.set_name(&default_name()).await?;
        Ok(server)
    }

    /// Run a server in this process and let other programs on this machine
    /// attach to it on `127.0.0.1:7777`, until this connection is closed
    #[cfg(feature = "server")]
    pub async fn shared(config: ServerConfig) -> Result<Self> {
        let address = local_address();
        let listener = tokio::net::TcpListener::bind(&address)
            .await
            .map_err(|e| Error::Transport(format!("Can't listen on {address}: {e}")))?;
        let server = Server::new(config).await;
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let served = server.clone();
        tokio::spawn(async move {
            let stopped = async move {
                let _ = stopped.await;
            };
            if let Err(e) = serve_on(served, listener, stopped).await {
                log::warn!("Serving other programs failed: {e}");
            }
        });
        Ok(Self { core: Core::in_process(server, "shared".to_owned(), Some(stop)) })
    }

    /// Open the mode given by `CRAZYFLIE_CONNECTION`, or `auto` when it is not set
    pub async fn from_env() -> Result<Self> {
        Self::open(&std::env::var(CONNECTION_ENV).unwrap_or_default()).await
    }

    /// Run a server in this process
    #[cfg(feature = "server")]
    pub async fn direct() -> Result<Self> {
        Self::direct_with(ServerConfig { name: "direct".to_owned(), ..Default::default() }).await
    }

    /// Run a server with these settings in this process
    #[cfg(feature = "server")]
    pub async fn direct_with(config: ServerConfig) -> Result<Self> {
        Ok(Self::in_process(Server::new(config).await))
    }

    /// Talk to a server running in this process, e.g. one that serves
    /// Crazyflies the program connected itself ([Server::adopt])
    #[cfg(feature = "server")]
    pub fn in_process(server: Server) -> Self {
        Self { core: Core::in_process(server, "direct".to_owned(), None) }
    }

    /// Run a server in this process (not available: built without the `server` feature)
    #[cfg(not(feature = "server"))]
    pub async fn direct() -> Result<Self> {
        Err(Error::Transport("Direct and shared mode need the `server` feature".to_owned()))
    }

    /// Connect to a server over WebSocket: `host`, `host:port` or a `ws://` URL
    pub async fn connect(address: &str) -> Result<Self> {
        let url = if address.starts_with("ws://") || address.starts_with("wss://") {
            address.to_owned()
        } else if address.contains(':') {
            format!("ws://{address}/")
        } else {
            format!("ws://{address}:{DEFAULT_PORT}/")
        };
        Ok(Self { core: Core::websocket(&url).await? })
    }

    /// Where this connection goes: `direct`, `shared` or the server URL
    pub fn mode(&self) -> &str {
        self.core.mode()
    }

    /// Whether the server runs in this process, private to it (direct mode)
    pub fn is_direct(&self) -> bool {
        self.core.mode() == "direct"
    }

    /// Whether the server runs in this process (direct or shared mode), so
    /// this program drives the Crazyradios itself
    pub fn is_in_process(&self) -> bool {
        #[cfg(feature = "server")]
        return self.core.in_process_server().is_some();
        #[cfg(not(feature = "server"))]
        false
    }

    /// Serve a Crazyflie this program connected itself with crazyflie-lib
    /// (in-process modes only), e.g. with its own radio assignment, and attach
    /// to it. Other clients attach to it by `uri` as usual. Disconnecting `cf`
    /// removes it from the server
    #[cfg(feature = "server")]
    pub async fn adopt(&self, uri: &str, cf: Arc<crazyflie_lib::Crazyflie>) -> Result<Crazyflie> {
        let server = self
            .core
            .in_process_server()
            .ok_or_else(|| Error::Transport("Adopting a Crazyflie needs the server in this process".to_owned()))?;
        server.adopt(uri, cf).await.map_err(Error::Server)?;
        self.attach(uri).await
    }

    /// In direct and shared mode, disconnect every Crazyflie now instead of
    /// after the linger time, and stop serving other programs. Call it before
    /// the program exits or switches mode. Does nothing through a server: it
    /// keeps its Crazyflies for its other clients
    pub async fn close(&self) {
        #[cfg(feature = "server")]
        if let Some(server) = self.core.in_process_server() {
            server.shutdown().await;
        }
    }

    /// Name this client. Other clients see it in scan and list results
    pub async fn set_name(&self, name: &str) -> Result<()> {
        self.core.call(Request::Hello { name: name.to_owned() }).await?;
        Ok(())
    }

    /// Scan all channels for Crazyflies at `address` (default E7E7E7E7E7).
    /// URIs are normalized (`radio:///<channel>/<rate>/<address>`) in every mode
    pub async fn scan(&self, address: Option<[u8; 5]>) -> Result<Vec<CfInfo>> {
        let address = address.map(|bytes| bytes.iter().map(|b| format!("{b:02X}")).collect());
        match self.core.call(Request::Scan { address }).await? {
            Response::Scan { found } => Ok(found),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// The Crazyflies the server is connected to, with their clients
    pub async fn list(&self) -> Result<Vec<CfInfo>> {
        match self.core.call(Request::List).await? {
            Response::List { connected } => Ok(connected),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Attach to a Crazyflie. The server connects it unless another client already has
    pub async fn attach(&self, uri: &str) -> Result<Crazyflie> {
        Crazyflie::attach(self.core.clone(), uri).await
    }

    /// Follow the Crazyflies that jobs take over and give back, e.g. to attach
    /// again to one that was flashed
    pub async fn watch(&self) -> Result<futures::stream::BoxStream<'static, ServerEvent>> {
        use futures::StreamExt;
        let (guard, events) = self.core.open_server_stream();
        match self.core.call(Request::Watch { sub: guard.sub() }).await? {
            Response::Done => {}
            _ => return Err(Error::UnexpectedReply),
        }
        Ok(crate::crazyflie::event_stream(guard, events, |event| match event {
            Event::Busy { uri, reason } => Some(ServerEvent::Busy { uri, reason }),
            Event::Available { uri, connected } => Some(ServerEvent::Available { uri, connected }),
            _ => None,
        })
        .boxed())
    }

    /// Flash firmware to the Crazyflie at `uri`, a radio URI: the server
    /// restarts it into its bootloader, writes the images and restarts it into
    /// its firmware, sharing the Crazyradio with its other connections.
    /// Programs attached to the Crazyflie are disconnected. The job runs on the
    /// server and finishes even if this program goes away. `progress` is
    /// called as it goes
    pub async fn flash(&self, uri: &str, images: Vec<FlashImage>, progress: impl FnMut(FlashProgress)) -> Result<()> {
        match self.flash_swarm(&[uri], images, progress).await?.pop() {
            Some(Ok(_)) => Ok(()),
            Some(Err(reason)) => Err(Error::Server(reason)),
            None => Err(Error::UnexpectedReply),
        }
    }

    /// Flash the same firmware to the Crazyflies at `uris`, radio URIs of one
    /// platform, as one job: like [Client::flash], but the Crazyflies whose
    /// bootloaders take broadcast get each image at the same time, which takes
    /// as long for any number of them. Returns, for each of `uris` in order,
    /// how it was flashed (e.g. `broadcast`) or why not
    pub async fn flash_swarm(
        &self,
        uris: &[&str],
        images: Vec<FlashImage>,
        mut progress: impl FnMut(FlashProgress),
    ) -> Result<Vec<std::result::Result<String, String>>> {
        use base64::Engine as _;
        let images = images
            .into_iter()
            .map(|image| crazyflie_connection_protocol::FlashImage {
                target: image.target,
                start: image.start,
                data: base64::engine::general_purpose::STANDARD.encode(&image.data),
            })
            .collect();
        let keys: Vec<String> = uris
            .iter()
            .map(|uri| normalize_uri(uri).map(|uri| uri.key))
            .collect::<std::result::Result<_, String>>()
            .map_err(Error::Server)?;
        let (guard, mut events) = self.core.open_server_stream();
        let uris = uris.iter().map(|uri| uri.to_string()).collect();
        match self.core.call(Request::Flash { uris, sub: guard.sub(), images }).await? {
            Response::Done => {}
            _ => return Err(Error::UnexpectedReply),
        }
        let mut results: Vec<Option<std::result::Result<String, String>>> = keys.iter().map(|_| None).collect();
        let mut set = |uri: &str, result| {
            if let Some(index) = keys.iter().position(|key| key == uri) {
                results[index] = Some(result);
            }
        };
        loop {
            match events.recv().await {
                Some(Event::Progress { uri, step, done, total }) => progress(FlashProgress { uri, step, done, total }),
                Some(Event::Flashed { uri, how }) => set(&uri, Ok(how)),
                Some(Event::FlashFailed { uri, reason }) => set(&uri, Err(reason)),
                Some(Event::Done) => break,
                Some(Event::Ended { reason }) => return Err(Error::Server(reason)),
                Some(_) => continue,
                None => return Err(Error::ConnectionLost),
            }
        }
        Ok(results.into_iter().map(|result| result.unwrap_or_else(|| Err("No result from the server".to_owned()))).collect())
    }
}

/// A change on the server that every client can follow with [Client::watch]
#[derive(Debug, Clone)]
pub enum ServerEvent {
    /// A job took the Crazyflie over (e.g. flashing): attaching to it fails
    /// until it is available again
    Busy { uri: String, reason: String },
    /// The job on the Crazyflie is done. `connected`: the server connected it
    /// again, so attaching to it is immediate
    Available { uri: String, connected: bool },
}

/// A firmware image for [Client::flash] and [Client::flash_swarm]
#[derive(Debug, Clone)]
pub struct FlashImage {
    /// `stm32` or `nrf51`
    pub target: String,
    /// Where to write the image; the start of the firmware area when `None`
    pub start: Option<FlashStart>,
    pub data: Vec<u8>,
}

/// How far [Client::flash] or [Client::flash_swarm] is
#[derive(Debug, Clone)]
pub struct FlashProgress {
    /// The Crazyflie it is about; `None` for all of them (e.g. a broadcast)
    pub uri: Option<String>,
    /// What the job does now, e.g. `Flashing stm32`
    pub step: String,
    /// Bytes written so far while flashing, else 0 or 1 of 1
    pub done: u64,
    pub total: u64,
}

/// Settings for running the server in this process ([Client::open_with])
#[cfg(feature = "server")]
#[derive(Debug, Clone)]
pub struct InProcess {
    /// Settings of the in-process server
    pub config: ServerConfig,
    /// What `auto` runs when no server is running on this machine: `shared`
    /// (`true`, for an app that others join, like Swarmkeeper) or `direct`
    /// (`false`, for a short-lived command like cfcli)
    pub auto_shared: bool,
}

#[cfg(feature = "server")]
impl Default for InProcess {
    fn default() -> Self {
        Self { config: ServerConfig { name: "direct".to_owned(), ..Default::default() }, auto_shared: false }
    }
}

/// Whether a server is listening on this machine (what `auto` mode looks for)
pub async fn local_server_running() -> bool {
    tokio::net::TcpStream::connect(local_address()).await.is_ok()
}

fn local_address() -> String {
    format!("127.0.0.1:{DEFAULT_PORT}")
}

/// `<program> (<pid>)`, shown to other clients until [Client::set_name] is called
fn default_name() -> String {
    let program = std::env::args()
        .next()
        .and_then(|arg0| std::path::Path::new(&arg0).file_name().map(|name| name.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "client".to_owned());
    format!("{program} ({})", std::process::id())
}
