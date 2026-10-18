//! A Crazyflie attached through a server, with subsystems named like `crazyflie-lib`

use crate::core::{Core, StreamGuard};
use crate::{Error, LinkStatistics, PersistentParamState, Result, TocItem};
use crazyflie_connection_protocol::{AttachInfo, Event, Request, Response};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// A Crazyflie attached through a [Client](crate::Client)
///
/// Other clients may be attached to the same Crazyflie. Dropping the last
/// handle detaches this client; the server disconnects the Crazyflie when no
/// client is left.
pub struct Crazyflie {
    pub param: Param,
    pub log: Log,
    pub console: Console,
    pub high_level_commander: HighLevelCommander,
    pub supervisor: Supervisor,
    pub platform: Platform,
    pub link_service: LinkService,
    handle: Arc<Handle>,
    lost: watch::Receiver<Option<String>>,
}

/// What every subsystem needs: the connection and the Crazyflie's URI
struct Handle {
    core: Arc<Core>,
    uri: String,
}

impl Handle {
    async fn call(&self, request: Request) -> Result<Response> {
        self.core.call(request).await
    }

    async fn call_done(&self, request: Request) -> Result<()> {
        match self.call(request).await? {
            Response::Done => Ok(()),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Open a stream: reserve the number, then send the subscribe request built from it
    async fn stream<T, F>(&self, request: impl FnOnce(u64) -> Request, map: F) -> Result<BoxStream<'static, T>>
    where
        T: Send + 'static,
        F: FnMut(Event) -> Option<T> + Send + 'static,
    {
        let (guard, events) = self.core.open_stream(&self.uri);
        self.call_done(request(guard.sub())).await?;
        Ok(event_stream(guard, events, map).boxed())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.core.remove_attached(&self.uri);
    }
}

/// Turn a stream's events into items until it ends. Dropping it closes the stream on the server
fn event_stream<T, F>(
    guard: StreamGuard,
    events: mpsc::UnboundedReceiver<Event>,
    map: F,
) -> impl Stream<Item = T> + Send + 'static
where
    T: Send + 'static,
    F: FnMut(Event) -> Option<T> + Send + 'static,
{
    futures::stream::unfold((guard, events, map), |(guard, mut events, mut map)| async move {
        loop {
            match events.recv().await {
                None | Some(Event::Ended { .. }) => return None,
                Some(event) => {
                    if let Some(item) = map(event) {
                        return Some((item, (guard, events, map)));
                    }
                }
            }
        }
    })
}

impl Crazyflie {
    pub(crate) async fn attach(core: Arc<Core>, uri: &str) -> Result<Self> {
        let info = match core.call(Request::Attach { uri: uri.to_owned() }).await? {
            Response::Attached(info) => info,
            _ => return Err(Error::UnexpectedReply),
        };
        let lost = core.add_attached(&info.uri);
        let handle = Arc::new(Handle { core, uri: info.uri.clone() });
        let AttachInfo { firmware_version, device_type, protocol_version, params, log_vars, .. } = info;
        Ok(Self {
            param: Param { handle: handle.clone(), toc: toc(params) },
            log: Log { handle: handle.clone(), toc: toc(log_vars) },
            console: Console { handle: handle.clone() },
            high_level_commander: HighLevelCommander { handle: handle.clone() },
            supervisor: Supervisor { handle: handle.clone() },
            platform: Platform { firmware_version, device_type, protocol_version },
            link_service: LinkService { handle: handle.clone() },
            handle,
            lost,
        })
    }

    /// Normalized URI of this Crazyflie
    pub fn uri(&self) -> &str {
        &self.handle.uri
    }

    /// The lib-rs `Crazyflie` behind this one, in direct and shared mode only. It is the
    /// instance the in-process server uses: an escape hatch for what this API
    /// does not cover yet (memories, trajectories, …). `None` through a server
    #[cfg(feature = "server")]
    pub fn lib(&self) -> Option<Arc<crazyflie_lib::Crazyflie>> {
        self.handle.core.in_process_server()?.crazyflie(&self.handle.uri)
    }

    /// Detach now, even while clones of this handle or its streams are still
    /// around (e.g. in other tasks): the streams end and further calls fail.
    /// The server disconnects the Crazyflie if no other client uses it
    pub fn detach(&self) {
        self.handle.core.detach(&self.handle.uri);
    }

    /// Wait until the Crazyflie is disconnected and return the reason
    pub async fn wait_disconnect(&self) -> String {
        let mut lost = self.lost.clone();
        match lost.wait_for(|lost| lost.is_some()).await {
            Ok(reason) => reason.clone().unwrap_or_default(),
            Err(_) => "Connection to the server lost".to_owned(),
        }
    }
}

fn toc(items: Vec<TocItem>) -> Arc<HashMap<String, TocItem>> {
    Arc::new(items.into_iter().map(|item| (item.name.clone(), item)).collect())
}

fn sorted_names(toc: &HashMap<String, TocItem>) -> Vec<String> {
    let mut names: Vec<String> = toc.keys().cloned().collect();
    names.sort();
    names
}

fn toc_type<'a>(toc: &'a HashMap<String, TocItem>, name: &str) -> Result<&'a str> {
    toc.get(name).map(|item| item.value_type.as_str()).ok_or_else(|| Error::UnknownName(name.to_owned()))
}

/// Parameters. The server keeps one cache per Crazyflie, so a value set by
/// any client is seen by all, and [Param::watch_change] reports every set
pub struct Param {
    handle: Arc<Handle>,
    toc: Arc<HashMap<String, TocItem>>,
}

impl Param {
    /// All parameter names, sorted
    pub fn names(&self) -> Vec<String> {
        sorted_names(&self.toc)
    }

    /// Firmware type of a parameter, e.g. `u8` or `f32`
    pub fn get_type(&self, name: &str) -> Result<&str> {
        toc_type(&self.toc, name)
    }

    pub fn is_writable(&self, name: &str) -> Result<bool> {
        self.toc.get(name).map(|item| item.writable).ok_or_else(|| Error::UnknownName(name.to_owned()))
    }

    /// Whether the parameter can be stored in the Crazyflie's EEPROM
    pub fn is_persistent(&self, name: &str) -> Result<bool> {
        self.toc.get(name).map(|item| item.persistent).ok_or_else(|| Error::UnknownName(name.to_owned()))
    }

    /// Default and stored value of a persistent parameter
    pub async fn persistent_get_state(&self, name: &str) -> Result<PersistentParamState> {
        let request = Request::ParamPersistentState { uri: self.handle.uri.clone(), name: name.to_owned() };
        match self.handle.call(request).await? {
            Response::PersistentState(state) => Ok(state),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Store the current value of a persistent parameter in the EEPROM
    pub async fn persistent_store(&self, name: &str) -> Result<()> {
        self.handle
            .call_done(Request::ParamPersistentStore { uri: self.handle.uri.clone(), name: name.to_owned() })
            .await
    }

    /// Clear the stored value of a persistent parameter
    pub async fn persistent_clear(&self, name: &str) -> Result<()> {
        self.handle
            .call_done(Request::ParamPersistentClear { uri: self.handle.uri.clone(), name: name.to_owned() })
            .await
    }

    /// Read a parameter as `f64`
    pub async fn get_lossy(&self, name: &str) -> Result<f64> {
        match self.handle.call(Request::ParamGet { uri: self.handle.uri.clone(), name: name.to_owned() }).await? {
            Response::Value { value } => Ok(value),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Set a parameter from an `f64`, converted to the parameter's type
    pub async fn set_lossy(&self, name: &str, value: f64) -> Result<()> {
        self.handle
            .call_done(Request::ParamSet { uri: self.handle.uri.clone(), name: name.to_owned(), value })
            .await
    }

    /// Stream of `(name, value)` for every parameter set by any client
    pub async fn watch_change(&self) -> Result<BoxStream<'static, (String, f64)>> {
        let uri = self.handle.uri.clone();
        self.handle
            .stream(
                |sub| Request::ParamWatch { uri, sub },
                |event| match event {
                    Event::ParamChanged { name, value } => Some((name, value)),
                    _ => None,
                },
            )
            .await
    }
}

/// Log variables. Subscribe to variables; the server creates the log blocks
pub struct Log {
    handle: Arc<Handle>,
    toc: Arc<HashMap<String, TocItem>>,
}

/// One log sample, like `crazyflie_lib::subsystems::log::LogData` with values as `f64`
#[derive(Debug, Clone)]
pub struct LogData {
    /// Crazyflie timestamp in milliseconds
    pub timestamp: u32,
    pub data: HashMap<String, f64>,
}

/// A running log subscription. Dropping it stops the log block on the Crazyflie
pub struct LogStream {
    _guard: StreamGuard,
    events: mpsc::UnboundedReceiver<Event>,
}

impl LogStream {
    /// Wait for the next sample. Fails when the stream ended (disconnect, log error)
    pub async fn next(&mut self) -> Result<LogData> {
        loop {
            match self.events.recv().await {
                Some(Event::Log { timestamp, values }) => {
                    return Ok(LogData { timestamp, data: values.into_iter().collect() });
                }
                Some(Event::Ended { reason }) => return Err(Error::Server(reason)),
                Some(_) => continue,
                None => return Err(Error::ConnectionLost),
            }
        }
    }
}

impl Log {
    /// All log variable names, sorted
    pub fn names(&self) -> Vec<String> {
        sorted_names(&self.toc)
    }

    /// Firmware type of a log variable, e.g. `u8` or `f32`
    pub fn get_type(&self, name: &str) -> Result<&str> {
        toc_type(&self.toc, name)
    }

    /// Log `vars` every `period` (10 ms to 2.55 s)
    pub async fn subscribe(&self, vars: &[&str], period: Duration) -> Result<LogStream> {
        for var in vars {
            toc_type(&self.toc, var)?;
        }
        let (guard, events) = self.handle.core.open_stream(&self.handle.uri);
        self.handle
            .call_done(Request::LogSubscribe {
                uri: self.handle.uri.clone(),
                sub: guard.sub(),
                vars: vars.iter().map(|var| var.to_string()).collect(),
                period_ms: period.as_millis() as u64,
            })
            .await?;
        Ok(LogStream { _guard: guard, events })
    }
}

/// Console text
pub struct Console {
    handle: Arc<Handle>,
}

impl Console {
    /// Console lines, starting with the lines printed since the Crazyflie was connected
    pub async fn line_stream(&self) -> Result<BoxStream<'static, String>> {
        self.lines(true).await
    }

    /// Console lines printed from now on
    pub async fn line_stream_no_history(&self) -> Result<BoxStream<'static, String>> {
        self.lines(false).await
    }

    async fn lines(&self, history: bool) -> Result<BoxStream<'static, String>> {
        let uri = self.handle.uri.clone();
        self.handle
            .stream(
                |sub| Request::ConsoleSubscribe { uri, sub, history },
                |event| match event {
                    Event::Console { line } => Some(line),
                    _ => None,
                },
            )
            .await
    }
}

/// High-level commander, same arguments as in `crazyflie-lib`
pub struct HighLevelCommander {
    handle: Arc<Handle>,
}

impl HighLevelCommander {
    pub async fn take_off(&self, height: f32, yaw: Option<f32>, duration: f32, group_mask: Option<u8>) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle.call_done(Request::TakeOff { uri, height, yaw, duration, group_mask }).await
    }

    pub async fn land(&self, height: f32, yaw: Option<f32>, duration: f32, group_mask: Option<u8>) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle.call_done(Request::Land { uri, height, yaw, duration, group_mask }).await
    }

    pub async fn stop(&self, group_mask: Option<u8>) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle.call_done(Request::Stop { uri, group_mask }).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn go_to(
        &self,
        x: f32,
        y: f32,
        z: f32,
        yaw: f32,
        duration: f32,
        relative: bool,
        linear: bool,
        group_mask: Option<u8>,
    ) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::GoTo { uri, x, y, z, yaw, duration, relative, linear, group_mask })
            .await
    }
}

/// Supervisor: arming and emergency stop
pub struct Supervisor {
    handle: Arc<Handle>,
}

impl Supervisor {
    pub async fn send_arming_request(&self, arm: bool) -> Result<()> {
        self.handle.call_done(Request::ArmingRequest { uri: self.handle.uri.clone(), arm }).await
    }

    pub async fn send_emergency_stop(&self) -> Result<()> {
        self.handle.call_done(Request::EmergencyStop { uri: self.handle.uri.clone() }).await
    }
}

/// Platform information, read once when the server connected the Crazyflie
pub struct Platform {
    firmware_version: Option<String>,
    device_type: Option<String>,
    protocol_version: Option<u8>,
}

impl Platform {
    pub fn firmware_version(&self) -> Option<&str> {
        self.firmware_version.as_deref()
    }

    pub fn device_type_name(&self) -> Option<&str> {
        self.device_type.as_deref()
    }

    /// CRTP protocol version
    pub fn protocol_version(&self) -> Option<u8> {
        self.protocol_version
    }
}

/// Link statistics
pub struct LinkService {
    handle: Arc<Handle>,
}

impl LinkService {
    pub async fn get_statistics(&self) -> Result<LinkStatistics> {
        match self.handle.call(Request::LinkStatistics { uri: self.handle.uri.clone() }).await? {
            Response::LinkStatistics(stats) => Ok(stats),
            _ => Err(Error::UnexpectedReply),
        }
    }
}
