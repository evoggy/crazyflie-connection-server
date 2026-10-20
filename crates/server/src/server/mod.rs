//! The connection server
//!
//! Owns the Crazyradios and keeps one [crazyflie_lib::Crazyflie] per connected
//! Crazyflie. Any number of clients attach to the same Crazyflie through a
//! [Session] and share it: one param cache, log subscriptions instead of log
//! blocks, one console.
//!
//! A tool runs it in-process (the client's `direct` and `shared` mode).
//! [serve()] exposes it over WebSocket for other processes and machines; see
//! `crazyflie-connection-protocol` for the messages.

mod console;
mod flash;
mod memory;
mod platform;
mod session;
mod toc;
mod ws;

pub use session::Session;
pub use toc::FileTocCache;
pub use ws::{serve, serve_on};

use crazyflie_lib::crazyflie_link::crazyradio::Crazyradio;
use crazyflie_lib::crazyflie_link::{LinkContext, SharedCrazyradio};
use crazyflie_lib::{Crazyflie, TocCache};
use crazyflie_connection_protocol::{AttachInfo, CfInfo, Event, NormalizedUri, TocItem, normalize_uri};
use log::{info, warn};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{OnceCell, watch};
use toc::{ProbeCache, ServerTocCache, TocDownloads};

/// How long connecting a Crazyflie may take, TOC download included
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a Crazyflie restarted by a job gets to start its firmware. The
/// nRF51 delays the start by 5 s when an AI-deck is attached
const FIRMWARE_START: Duration = Duration::from_secs(15);
/// Channels less than this apart go on one Crazyradio, so that two radios
/// don't send on (nearly) the same channel at once
const CHANNEL_SPACING: u8 = 2;

/// Server settings
#[derive(Clone)]
pub struct ServerConfig {
    /// Name reported to clients
    pub name: String,
    /// How long a Crazyflie stays connected after its last client detached,
    /// so that a quick re-attach (e.g. consecutive cfcli commands) reuses it
    pub linger: Duration,
    /// Open every Crazyradio at start and keep them. `false` opens a radio only
    /// while a Crazyflie uses it, like a plain lib-rs program, so that other
    /// programs can use the other radios (a tool's direct mode)
    pub hold_radios: bool,
    /// Where TOCs are cached. `None` keeps them in memory
    pub toc_cache: Option<Arc<dyn TocStore>>,
    /// The tool's own link context, so that the server and the tool share the
    /// Crazyradios they open. `None` creates one
    pub link_context: Option<Arc<LinkContext>>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            name: "crazyflie-connection-server".to_owned(),
            linger: Duration::from_secs(10),
            hold_radios: true,
            toc_cache: None,
            link_context: None,
        }
    }
}

impl ServerConfig {
    /// Cache TOCs in `cache`, e.g. a tool's on-disk cache
    pub fn with_toc_cache(mut self, cache: impl TocCache) -> Self {
        self.toc_cache = Some(Arc::new(cache));
        self
    }
}

impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerConfig")
            .field("name", &self.name)
            .field("linger", &self.linger)
            .field("hold_radios", &self.hold_radios)
            .field("toc_cache", &self.toc_cache.is_some())
            .field("link_context", &self.link_context.is_some())
            .finish()
    }
}

/// A [TocCache] the server can hold as a trait object. Every [TocCache] is one
pub trait TocStore: Send + Sync + 'static {
    fn get_toc(&self, key: &[u8]) -> Option<String>;
    fn store_toc(&self, key: &[u8], toc: &str);
}

impl<T: TocCache> TocStore for T {
    fn get_toc(&self, key: &[u8]) -> Option<String> {
        TocCache::get_toc(self, key)
    }

    fn store_toc(&self, key: &[u8], toc: &str) {
        TocCache::store_toc(self, key, toc)
    }
}

/// The server. Cheap to clone; all clones share the same radios and connections
#[derive(Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

struct Inner {
    config: ServerConfig,
    link_context: Arc<LinkContext>,
    /// Radios held open for the server's lifetime, by index (`hold_radios`)
    _radios: Vec<SharedCrazyradio>,
    /// The channel of every link on each radio, used to pick a radio for `radio:///` URIs
    radio_load: Mutex<BTreeMap<usize, Vec<u8>>>,
    /// TOCs being downloaded, so that Crazyflies with the same firmware wait for one download
    toc_downloads: TocDownloads,
    cfs: Mutex<HashMap<String, Arc<OnceCell<Arc<CfEntry>>>>>,
    /// Crazyflies a job (flashing) has taken over: key → why they can't be attached to
    busy: Mutex<HashMap<String, String>>,
    /// Crazyflies put to sleep (STM32 off): attaching fails until they are woken up
    asleep: Mutex<std::collections::HashSet<String>>,
    /// [Event::Busy] and [Event::Available], for the clients that watch them
    events: tokio::sync::broadcast::Sender<Event>,
    /// Broadcast flashes so far: each uses its own broadcast address
    broadcasts: AtomicU8,
    sessions: Mutex<HashMap<u64, Weak<session::SessionShared>>>,
    next_session: AtomicU64,
    toc_cache: ServerTocCache,
    /// Set by [Server::shutdown]
    stopped: watch::Sender<bool>,
    /// Set when the WebSocket listener stops: its clients are disconnected
    clients_closed: watch::Sender<bool>,
}

/// A link's place on a Crazyradio: counted in the radio's load until released
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Slot {
    radio: usize,
    channel: u8,
}

/// A connected Crazyflie
pub(crate) struct CfEntry {
    key: String,
    /// `None` for USB and adopted Crazyflies
    slot: Option<Slot>,
    cf: Arc<Crazyflie>,
    info: AttachInfo,
    console: Arc<console::ConsoleLines>,
    /// Memory requests run one at a time, see [memory]
    memory_turn: tokio::sync::Mutex<()>,
    /// Attached sessions: id → client name
    clients: Mutex<BTreeMap<u64, String>>,
    /// Set (under the `clients` lock) once the entry is being closed. Attaching then waits for `closed`
    closing: AtomicBool,
    /// Attaches so far, so that a linger timer leaves a Crazyflie that was
    /// attached to again after it started
    attaches: AtomicU64,
    closed: watch::Sender<bool>,
    /// Why it was disconnected, set before `closed`
    lost: Mutex<Option<String>>,
}

impl Server {
    /// Create a server. With `hold_radios` it opens every Crazyradio that is not held by another program
    pub async fn new(config: ServerConfig) -> Self {
        let link_context = config.link_context.clone().unwrap_or_else(|| Arc::new(LinkContext::new()));
        let mut radios = Vec::new();
        let mut radio_load = BTreeMap::new();
        if config.hold_radios {
            for index in 0..attached_radios() {
                match link_context.get_radio(index).await {
                    Ok(radio) => {
                        radios.push(radio);
                        radio_load.insert(index, Vec::new());
                    }
                    Err(e) => warn!("Crazyradio {index} unavailable: {e}"),
                }
            }
            info!("Using Crazyradios {:?}", radio_load.keys().collect::<Vec<_>>());
        }
        let toc_cache = ServerTocCache::new(config.toc_cache.clone());

        Self {
            inner: Arc::new(Inner {
                config,
                link_context,
                _radios: radios,
                radio_load: Mutex::new(radio_load),
                toc_downloads: TocDownloads::default(),
                cfs: Mutex::new(HashMap::new()),
                busy: Mutex::new(HashMap::new()),
                asleep: Mutex::new(std::collections::HashSet::new()),
                events: tokio::sync::broadcast::Sender::new(64),
                broadcasts: AtomicU8::new(0),
                sessions: Mutex::new(HashMap::new()),
                next_session: AtomicU64::new(1),
                toc_cache,
                stopped: watch::Sender::new(false),
                clients_closed: watch::Sender::new(false),
            }),
        }
    }

    /// Server name
    pub fn name(&self) -> &str {
        &self.inner.config.name
    }

    /// Indexes of the Crazyradios this server holds
    pub fn radios(&self) -> Vec<usize> {
        self.inner.radio_load.lock().unwrap().keys().copied().collect()
    }

    /// The lib-rs `Crazyflie` behind a connected Crazyflie. For an in-process
    /// server only: anything done through it bypasses the server
    pub fn crazyflie(&self, uri: &str) -> Option<Arc<Crazyflie>> {
        let key = normalize_uri(uri).ok()?.key;
        self.entry(&key).map(|entry| entry.cf.clone())
    }

    /// Stop serving WebSocket clients and disconnect every Crazyflie, e.g.
    /// before the program exits
    pub async fn shutdown(&self) {
        self.inner.stopped.send_replace(true);
        let entries: Vec<Arc<CfEntry>> =
            self.inner.cfs.lock().unwrap().drain().filter_map(|(_, cell)| cell.get().cloned()).collect();
        for entry in entries {
            if !entry.closing.swap(true, Ordering::SeqCst) {
                info!("Disconnecting {}", entry.key);
                entry.cf.disconnect().await;
            }
        }
    }

    /// Completes once [Server::shutdown] was called
    pub(crate) fn stopped(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut stopped = self.inner.stopped.subscribe();
        async move {
            let _ = stopped.wait_for(|stopped| *stopped).await;
        }
    }

    pub(crate) fn clients_closed(&self) -> watch::Receiver<bool> {
        self.inner.clients_closed.subscribe()
    }

    pub(crate) fn close_clients(&self) {
        self.inner.clients_closed.send_replace(true);
    }

    fn entry(&self, key: &str) -> Option<Arc<CfEntry>> {
        self.inner.cfs.lock().unwrap().get(key).and_then(|cell| cell.get().cloned())
    }

    fn entries(&self) -> Vec<Arc<CfEntry>> {
        self.inner.cfs.lock().unwrap().values().filter_map(|cell| cell.get().cloned()).collect()
    }

    /// Crazyflies connected on the server, with their clients
    pub(crate) fn list(&self) -> Vec<CfInfo> {
        let mut list: Vec<CfInfo> = self.entries().iter().map(|entry| entry.cf_info()).collect();
        list.sort_by(|a, b| a.uri.cmp(&b.uri));
        list
    }

    /// Scan for Crazyflies at `address`. Results use normalized URIs
    pub(crate) async fn scan(&self, address: [u8; 5]) -> Result<Vec<CfInfo>, String> {
        // A scan holds the radio for a while, and the bootloader's 100 ms timeouts would run out
        if let Some(reason) = self.inner.busy.lock().unwrap().values().next() {
            return Err(format!("Can't scan now: {reason}"));
        }
        if attached_radios() > 0 && self.usable_radio().await.is_none() {
            return Err(RADIOS_BUSY.to_owned());
        }
        let found = self.inner.link_context.scan(address).await.map_err(|e| e.to_string())?;
        let mut result = Vec::new();
        for uri in found {
            let key = normalize_uri(&uri)?.key;
            let clients = self.entry(&key).map(|entry| entry.client_names()).unwrap_or_default();
            let radio = uri.strip_prefix("radio://").and_then(|rest| rest.split('/').next()?.parse().ok());
            result.push(CfInfo { uri: key, clients, radio });
        }
        Ok(result)
    }

    /// Attach a session to a Crazyflie, connecting it if needed
    pub(crate) async fn attach(&self, session_id: u64, name: &str, uri: &NormalizedUri) -> Result<Arc<CfEntry>, String> {
        if let Some(reason) = self.inner.busy.lock().unwrap().get(&uri.key) {
            return Err(reason.clone());
        }
        // Unless it was woken up some other way (its button)
        let asleep = self.inner.asleep.lock().unwrap().contains(&uri.key);
        if asleep {
            if !self.firmware_answers_within(uri, Duration::from_secs(1)).await {
                return Err(format!("{} is asleep (its STM32 is off). Wake it up first", uri.key));
            }
            self.inner.asleep.lock().unwrap().remove(&uri.key);
        }
        loop {
            let cell = self.inner.cfs.lock().unwrap().entry(uri.key.clone()).or_default().clone();
            let entry = cell.get_or_try_init(|| self.connect(uri)).await?.clone();
            let attached = {
                let mut clients = entry.clients.lock().unwrap();
                let open = !entry.closing.load(Ordering::SeqCst);
                if open {
                    clients.insert(session_id, name.to_owned());
                    entry.attaches.fetch_add(1, Ordering::SeqCst);
                }
                open
            };
            if attached {
                return Ok(entry);
            }
            // The connection is closing, wait for it to be gone and connect again
            let mut closed = entry.closed.subscribe();
            let _ = closed.wait_for(|closed| *closed).await;
        }
    }

    /// Detach a session. The Crazyflie is closed after the linger time if no client is left
    pub(crate) fn detach(&self, session_id: u64, key: &str) {
        let Some(entry) = self.entry(key) else { return };
        let empty = {
            let mut clients = entry.clients.lock().unwrap();
            clients.remove(&session_id);
            clients.is_empty()
        };
        if empty {
            self.close_later(entry);
        }
    }

    /// Close the Crazyflie after the linger time unless a client attached to it
    fn close_later(&self, entry: Arc<CfEntry>) {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let server = self.clone();
            let attaches = entry.attaches.load(Ordering::SeqCst);
            runtime.spawn(async move {
                tokio::time::sleep(server.inner.config.linger).await;
                server.close_if_unused(entry, attaches).await;
            });
        }
    }

    /// Close the Crazyflie unless a client is attached, or attached since
    /// there were `attaches` (its own linger timer then closes it)
    async fn close_if_unused(&self, entry: Arc<CfEntry>, attaches: u64) {
        {
            let clients = entry.clients.lock().unwrap();
            let attached_since = entry.attaches.load(Ordering::SeqCst) != attaches;
            if !clients.is_empty() || attached_since || entry.closing.swap(true, Ordering::SeqCst) {
                return;
            }
        }
        // It stays listed, closing, until the link is closed (`on_disconnected`
        // removes it): an attach meanwhile waits for that and then connects
        // again, instead of opening a second link to the Crazyflie
        info!("Disconnecting {} (no clients left)", entry.key);
        entry.cf.disconnect().await;
    }

    /// A radio this server can use: a held one, or (without `hold_radios`) the first that opens
    async fn usable_radio(&self) -> Option<usize> {
        if self.inner.config.hold_radios {
            return self.inner.radio_load.lock().unwrap().keys().next().copied();
        }
        for index in 0..attached_radios() {
            if self.inner.link_context.get_radio(index).await.is_ok() {
                return Some(index);
            }
        }
        None
    }

    /// Pick the radio for a link on `channel`: the pinned one, else one that
    /// already has links on (nearly) this channel, else the least loaded. The
    /// candidates are the held radios, or (without `hold_radios`) those that open
    async fn pick_radio(&self, pinned: Option<usize>, channel: u8) -> Result<Slot, String> {
        let radio = match pinned {
            Some(radio) => radio,
            None => {
                let candidates = if self.inner.config.hold_radios {
                    self.radios()
                } else {
                    let mut candidates = Vec::new();
                    for index in 0..attached_radios() {
                        let in_use = self.inner.radio_load.lock().unwrap().get(&index).is_some_and(|links| !links.is_empty());
                        if in_use || self.inner.link_context.get_radio(index).await.is_ok() {
                            candidates.push(index);
                        }
                    }
                    candidates
                };
                let load = self.inner.radio_load.lock().unwrap();
                match choose_radio(&load, &candidates, channel) {
                    Some(radio) => radio,
                    None if self.inner.config.hold_radios => return Err("No Crazyradio available on the server".to_owned()),
                    None if attached_radios() > 0 => return Err(RADIOS_BUSY.to_owned()),
                    None => return Err("No Crazyradio found".to_owned()),
                }
            }
        };
        self.inner.radio_load.lock().unwrap().entry(radio).or_default().push(channel);
        Ok(Slot { radio, channel })
    }

    fn release_radio(&self, slot: Option<Slot>) {
        let Some(slot) = slot else { return };
        if let Some(links) = self.inner.radio_load.lock().unwrap().get_mut(&slot.radio)
            && let Some(at) = links.iter().position(|channel| *channel == slot.channel)
        {
            links.swap_remove(at);
        }
    }

    async fn connect(&self, uri: &NormalizedUri) -> Result<Arc<CfEntry>, String> {
        let (link_uri, slot) = match uri.key.strip_prefix("radio:///") {
            Some(path) => {
                let slot = self.pick_radio(uri.radio, uri.channel().unwrap_or_default()).await?;
                let options = uri.options.as_ref().map(|o| format!("?{o}")).unwrap_or_default();
                (format!("radio://{}/{path}{options}", slot.radio), Some(slot))
            }
            None => (uri.key.clone(), None),
        };
        // A Crazyflie whose nRF51 acks the packets while its STM32 is off
        // (put to sleep by another program) or in its bootloader would keep
        // crazyflie-lib waiting for TOC answers until the connect timeout
        if slot.is_some() && !self.echoes_now(&link_uri).await {
            self.release_radio(slot);
            return Err(match self.answers(uri).await {
                Some(_) => format!(
                    "{} answers the radio but not its firmware: it is asleep (wake it up first) or in its bootloader",
                    uri.key
                ),
                None => format!("{} doesn't answer", uri.key),
            });
        }
        info!("Connecting {link_uri}");
        let cf = match self.connect_link(&link_uri).await {
            Ok(cf) => Arc::new(cf),
            Err(e) => {
                self.release_radio(slot);
                return Err(format!("Connecting {}: {e}", uri.key));
            }
        };
        let entry = self.register(&uri.key, slot, cf).await;
        info!("Connected {} on {link_uri}", entry.key);
        Ok(entry)
    }

    /// Connect over `link_uri`, first with only the cached TOCs. A Crazyflie
    /// whose TOCs aren't cached downloads them, unless another connection is
    /// downloading the same ones: it then waits for that one and connects from
    /// the cache. So a swarm with one firmware downloads its TOCs once
    async fn connect_link(&self, link_uri: &str) -> Result<Crazyflie, String> {
        let mut waited = false;
        loop {
            let probe = ProbeCache::new(self.inner.toc_cache.clone());
            let cf = self.connect_with(link_uri, probe.clone()).await?;
            let missed = probe.missed();
            if missed.is_empty() {
                return Ok(cf);
            }
            cf.disconnect().await;
            match self.inner.toc_downloads.claim(&missed) {
                Err(downloads) if !waited => {
                    waited = true;
                    let _ = tokio::time::timeout(CONNECT_TIMEOUT, downloads.finished()).await;
                }
                // Downloads them (also after waiting, should the cache not keep them)
                claim => {
                    let connected = self.connect_with(link_uri, self.inner.toc_cache.clone()).await;
                    drop(claim);
                    return connected;
                }
            }
        }
    }

    /// One connection attempt. Every Crazyflie connects at once, also the
    /// ones on one radio: a few at a time is slower, since the ones already
    /// connected keep polling the radio while the next ones connect (25 on one
    /// radio: 3.0 s at 8 at a time, 1.4 s all at once, 2026-10-19)
    async fn connect_with(&self, link_uri: &str, toc_cache: impl TocCache) -> Result<Crazyflie, String> {
        // crazyflie-link 0.5 panics when the Crazyradio fails while the link
        // starts (an unwrap in `CrazyradioConnection::open`)
        let connecting = std::panic::AssertUnwindSafe(Crazyflie::connect_from_uri(
            &self.inner.link_context,
            link_uri,
            toc_cache,
        ));
        match tokio::time::timeout(CONNECT_TIMEOUT, futures::FutureExt::catch_unwind(connecting)).await {
            Ok(Ok(Ok(cf))) => Ok(cf),
            Ok(Ok(Err(e))) => Err(e.to_string()),
            Ok(Err(_)) => Err("the Crazyradio failed (crazyflie-link panicked)".to_owned()),
            Err(_) => Err("timeout".to_owned()),
        }
    }

    /// Serve a Crazyflie that the caller connected with crazyflie-lib, e.g.
    /// with its own radio assignment. Clients attach to it by `uri` as usual.
    /// Disconnecting `cf` removes it from the server
    pub async fn adopt(&self, uri: &str, cf: Arc<Crazyflie>) -> Result<(), String> {
        let uri = normalize_uri(uri)?;
        let cell = self.inner.cfs.lock().unwrap().entry(uri.key.clone()).or_default().clone();
        let mut adopted = false;
        cell.get_or_init(|| async {
            adopted = true;
            self.register(&uri.key, None, cf).await
        })
        .await;
        if adopted { Ok(()) } else { Err(format!("{} is already connected", uri.key)) }
    }

    /// Describe a connected Crazyflie and watch for its disconnect
    async fn register(&self, key: &str, slot: Option<Slot>, cf: Arc<Crazyflie>) -> Arc<CfEntry> {
        let mut params = Vec::new();
        for name in cf.param.names() {
            let (Ok(value_type), Ok(writable)) = (cf.param.get_type(&name), cf.param.is_writable(&name)) else {
                continue;
            };
            // Not whether it is persistent: that asks the Crazyflie for each
            // param (133 round trips on 2026.08, 11 s for 25 Crazyflies on one
            // radio), so clients ask for the ones they need
            params.push(toc_item(name, value_type, writable));
        }
        params.sort_by(|a, b| a.name.cmp(&b.name));
        let mut log_vars: Vec<TocItem> = cf
            .log
            .names()
            .into_iter()
            .filter_map(|name| Some(toc_item(name.clone(), cf.log.get_type(&name).ok()?, true)))
            .collect();
        log_vars.sort_by(|a, b| a.name.cmp(&b.name));
        let info = AttachInfo {
            uri: key.to_owned(),
            firmware_version: cf.platform.firmware_version().await.ok(),
            device_type: cf.platform.device_type_name().await.ok(),
            protocol_version: cf.platform.protocol_version().await.ok(),
            params,
            log_vars,
            memories: memory::memories(&cf),
        };
        let console = console::ConsoleLines::start(&cf).await;
        let entry = Arc::new(CfEntry {
            key: key.to_owned(),
            slot,
            cf,
            info,
            console,
            memory_turn: tokio::sync::Mutex::new(()),
            clients: Mutex::new(BTreeMap::new()),
            closing: AtomicBool::new(false),
            attaches: AtomicU64::new(0),
            closed: watch::Sender::new(false),
            lost: Mutex::new(None),
        });
        let server = self.clone();
        let watched = entry.clone();
        tokio::spawn(async move {
            let reason = watched.cf.wait_disconnect().await;
            server.on_disconnected(&watched, reason);
        });
        entry
    }

    /// Mark a Crazyflie as taken over by a job, so that attaching to it fails with `reason`
    fn mark_busy(&self, key: &str, reason: String) -> Result<(), String> {
        let mut busy = self.inner.busy.lock().unwrap();
        if let Some(reason) = busy.get(key) {
            return Err(reason.clone());
        }
        busy.insert(key.to_owned(), reason);
        Ok(())
    }

    fn unmark_busy(&self, key: &str) {
        self.inner.busy.lock().unwrap().remove(key);
    }

    /// Tell the clients that watch the server
    fn announce(&self, event: Event) {
        let _ = self.inner.events.send(event);
    }

    pub(crate) fn watch_events(&self) -> tokio::sync::broadcast::Receiver<Event> {
        self.inner.events.subscribe()
    }

    /// Connect a Crazyflie again after a job, once its firmware answers, so
    /// that clients attach to it at once. It is closed after the linger time
    /// if none does
    async fn reconnect(&self, uri: &NormalizedUri) -> bool {
        if !self.firmware_answers(uri).await {
            warn!("Reconnecting after the job: {} didn't answer within {FIRMWARE_START:?}", uri.key);
            return false;
        }
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let cell = self.inner.cfs.lock().unwrap().entry(uri.key.clone()).or_default().clone();
            match cell.get_or_try_init(|| self.connect(uri)).await {
                Ok(entry) => {
                    self.close_later(entry.clone());
                    return true;
                }
                Err(e) => warn!("Reconnecting after the job: {e}"),
            }
        }
        false
    }

    /// Wait until the firmware of a Crazyflie that was just restarted answers
    /// a link echo. Connecting before then can wait for TOC answers the
    /// starting firmware never sends, while the nRF51 already acks the packets
    async fn firmware_answers(&self, uri: &NormalizedUri) -> bool {
        self.firmware_answers_within(uri, FIRMWARE_START).await
    }

    /// [Server::firmware_answers], waiting at most `wait`
    async fn firmware_answers_within(&self, uri: &NormalizedUri, wait: Duration) -> bool {
        let Some(path) = uri.key.strip_prefix("radio:///") else {
            return true;
        };
        let Ok(slot) = self.pick_radio(uri.radio, uri.channel().unwrap_or_default()).await else {
            return false;
        };
        let options = uri.options.as_ref().map(|o| format!("?{o}")).unwrap_or_default();
        let link_uri = format!("radio://{}/{path}{options}", slot.radio);
        let deadline = tokio::time::Instant::now() + wait;
        let mut answered = false;
        while !answered && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(500)).await;
            answered = self.echoes_now(&link_uri).await;
        }
        self.release_radio(Some(slot));
        answered
    }

    /// Whether the firmware behind `link_uri` answers a link echo now
    async fn echoes_now(&self, link_uri: &str) -> bool {
        match self.inner.link_context.open_link(link_uri).await {
            Ok(link) => {
                let answered = echoes(&link).await;
                link.close().await;
                answered
            }
            Err(_) => false,
        }
    }

    /// Disconnect the Crazyflie at `key`, if connected, telling its clients
    /// `reason`, and return the radio it was on (`None` for one adopted)
    async fn take_offline(&self, key: &str, reason: &str) -> Option<Option<usize>> {
        let entry = self.entry(key)?;
        self.on_disconnected(&entry, reason.to_owned());
        entry.cf.disconnect().await;
        Some(entry.slot.map(|slot| slot.radio))
    }

    fn on_disconnected(&self, entry: &Arc<CfEntry>, reason: String) {
        // Called again by the disconnect watch after take_offline
        if *entry.closed.borrow() {
            return;
        }
        info!("{} disconnected: {reason}", entry.key);
        let clients = {
            let mut cfs = self.inner.cfs.lock().unwrap();
            let mut clients = entry.clients.lock().unwrap();
            entry.closing.store(true, Ordering::SeqCst);
            remove_if_same(&mut cfs, entry);
            std::mem::take(&mut *clients)
        };
        self.release_radio(entry.slot);
        *entry.lost.lock().unwrap() = Some(reason.clone());
        entry.closed.send_replace(true);

        let sessions = self.inner.sessions.lock().unwrap();
        for session_id in clients.keys() {
            if let Some(session) = sessions.get(session_id).and_then(Weak::upgrade) {
                session.crazyflie_lost(&entry.key, &reason);
            }
        }
    }

    /// Open a session for one client. Messages for the client arrive on the returned receiver
    pub fn open_session(
        &self,
    ) -> (Session, tokio::sync::mpsc::UnboundedReceiver<crazyflie_connection_protocol::ServerMessage>) {
        let id = self.inner.next_session.fetch_add(1, Ordering::SeqCst);
        let (session, rx) = Session::new(self.clone(), id);
        self.inner.sessions.lock().unwrap().insert(id, Arc::downgrade(session.shared()));
        (session, rx)
    }

    pub(crate) fn session_closed(&self, id: u64) {
        self.inner.sessions.lock().unwrap().remove(&id);
    }

    pub(crate) fn rename_client(&self, session_id: u64, name: &str) {
        for entry in self.entries() {
            if let Some(client) = entry.clients.lock().unwrap().get_mut(&session_id) {
                *client = name.to_owned();
            }
        }
    }
}

impl CfEntry {
    fn client_names(&self) -> Vec<String> {
        self.clients.lock().unwrap().values().cloned().collect()
    }

    /// Why it was disconnected, once it is
    fn lost_reason(&self) -> String {
        self.lost.lock().unwrap().clone().unwrap_or_else(|| "Crazyflie disconnected".to_owned())
    }

    fn cf_info(&self) -> CfInfo {
        CfInfo { uri: self.key.clone(), clients: self.client_names(), radio: self.slot.map(|slot| slot.radio) }
    }
}

fn remove_if_same(cfs: &mut HashMap<String, Arc<OnceCell<Arc<CfEntry>>>>, entry: &Arc<CfEntry>) {
    let same = cfs
        .get(&entry.key)
        .and_then(|cell| cell.get())
        .is_some_and(|current| Arc::ptr_eq(current, entry));
    if same {
        cfs.remove(&entry.key);
    }
}

fn toc_item(name: String, value_type: crazyflie_lib::ValueType, writable: bool) -> TocItem {
    TocItem { name, value_type: format!("{value_type:?}").to_lowercase(), writable }
}

/// Whether the firmware behind `link` answers a link echo (CRTP port 15, channel 0)
async fn echoes(link: &crazyflie_lib::crazyflie_link::Connection) -> bool {
    use crazyflie_lib::crazyflie_link::Packet;
    for _ in 0..3 {
        if link.send_packet(Packet::new(15, 0, vec![0xEC])).await.is_err() {
            return false;
        }
        let answer = tokio::time::timeout(Duration::from_millis(200), async {
            while let Ok(packet) = link.recv_packet().await {
                if packet.get_port() == 15 && packet.get_channel() == 0 {
                    return true;
                }
            }
            false
        });
        if answer.await == Ok(true) {
            return true;
        }
    }
    false
}

const RADIOS_BUSY: &str = "Every Crazyradio is in use by another program (a connection server?)";

/// Number of attached Crazyradios, in the order crazyflie-link numbers them
fn attached_radios() -> usize {
    Crazyradio::list_serials().map(|serials| serials.len()).unwrap_or(0)
}

/// Parse a 5-byte radio address written as 10 hex digits
pub(crate) fn parse_address(address: &str) -> Result<[u8; 5], String> {
    let invalid = || format!("Invalid radio address {address}");
    if address.len() != 10 {
        return Err(invalid());
    }
    let mut bytes = [0u8; 5];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&address[2 * i..2 * i + 2], 16).map_err(|_| invalid())?;
    }
    Ok(bytes)
}

/// The radio for a link on `channel`, from `candidates` (in index order) and
/// the channels of the links already on each radio: one that has a link
/// less than [CHANNEL_SPACING] away, else the least loaded, the lowest index
/// on a tie. Like cfcli's swarm radio plan, but one link at a time
fn choose_radio(load: &BTreeMap<usize, Vec<u8>>, candidates: &[usize], channel: u8) -> Option<usize> {
    let links = |radio: &usize| load.get(radio).map_or(0, Vec::len);
    let near = |radio: &usize| {
        load.get(radio).is_some_and(|channels| channels.iter().any(|c| c.abs_diff(channel) < CHANNEL_SPACING))
    };
    candidates
        .iter()
        .filter(|radio| near(radio))
        .min_by_key(|radio| links(radio))
        .or_else(|| candidates.iter().min_by_key(|radio| links(radio)))
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_groups_share_a_radio() {
        let mut load: BTreeMap<usize, Vec<u8>> = [(0, vec![]), (1, vec![])].into();
        let mut pick = |channel| {
            let radio = choose_radio(&load, &[0, 1], channel).unwrap();
            load.get_mut(&radio).unwrap().push(channel);
            radio
        };
        // The first link of a channel goes to the least loaded radio, the
        // others on (nearly) the same channel follow it
        assert_eq!(pick(88), 0);
        assert_eq!(pick(90), 1);
        assert_eq!(pick(88), 0);
        assert_eq!(pick(87), 0);
        assert_eq!(pick(91), 1);
        assert_eq!(pick(20), 1);
        // Near links on both: the less loaded one, the lowest index on a tie
        assert_eq!(pick(89), 0);
        assert_eq!(pick(89), 1);
    }

    #[test]
    fn no_candidate_no_radio() {
        assert_eq!(choose_radio(&BTreeMap::new(), &[], 80), None);
        // A pinned link's radio isn't a candidate when it isn't held
        let load: BTreeMap<usize, Vec<u8>> = [(1, vec![80])].into();
        assert_eq!(choose_radio(&load, &[0], 80), Some(0));
    }
}
