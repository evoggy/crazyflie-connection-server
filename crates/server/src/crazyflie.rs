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
    pub commander: Commander,
    pub high_level_commander: HighLevelCommander,
    pub supervisor: Supervisor,
    pub localization: Localization,
    pub platform: Platform,
    pub link_service: LinkService,
    pub memory: crate::memory::Memory,
    handle: Arc<Handle>,
    lost: watch::Receiver<Option<String>>,
}

/// What every subsystem needs: the connection and the Crazyflie's URI
pub(crate) struct Handle {
    core: Arc<Core>,
    pub(crate) uri: String,
}

impl Handle {
    pub(crate) async fn call(&self, request: Request) -> Result<Response> {
        self.core.call(request).await
    }

    async fn call_done(&self, request: Request) -> Result<()> {
        match self.call(request).await? {
            Response::Done => Ok(()),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Open a stream: reserve the number, then send the subscribe request built from it
    async fn stream<T, F>(
        &self,
        request: impl FnOnce(u64) -> Request,
        map: F,
    ) -> Result<BoxStream<'static, T>>
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
pub(crate) fn event_stream<T, F>(
    guard: StreamGuard,
    events: mpsc::UnboundedReceiver<Event>,
    map: F,
) -> impl Stream<Item = T> + Send + 'static
where
    T: Send + 'static,
    F: FnMut(Event) -> Option<T> + Send + 'static,
{
    futures::stream::unfold(
        (guard, events, map),
        |(guard, mut events, mut map)| async move {
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
        },
    )
}

impl Crazyflie {
    pub(crate) async fn attach(core: Arc<Core>, uri: &str) -> Result<Self> {
        let info = match core
            .call(Request::Attach {
                uri: uri.to_owned(),
            })
            .await?
        {
            Response::Attached(info) => info,
            _ => return Err(Error::UnexpectedReply),
        };
        let lost = core.add_attached(&info.uri);
        let handle = Arc::new(Handle {
            core,
            uri: info.uri.clone(),
        });
        let AttachInfo {
            firmware_version,
            device_type,
            protocol_version,
            params,
            log_vars,
            memories,
            ..
        } = info;
        Ok(Self {
            param: Param {
                handle: handle.clone(),
                toc: toc(params),
            },
            log: Log {
                handle: handle.clone(),
                toc: toc(log_vars),
            },
            console: Console {
                handle: handle.clone(),
            },
            commander: Commander {
                handle: handle.clone(),
            },
            high_level_commander: HighLevelCommander {
                handle: handle.clone(),
            },
            supervisor: Supervisor {
                handle: handle.clone(),
            },
            localization: Localization {
                lighthouse: Lighthouse {
                    handle: handle.clone(),
                },
                loco_positioning: LocoPositioning {
                    handle: handle.clone(),
                },
            },
            platform: Platform {
                firmware_version,
                device_type,
                protocol_version,
            },
            link_service: LinkService {
                handle: handle.clone(),
            },
            memory: crate::memory::Memory {
                handle: handle.clone(),
                memories,
            },
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
        self.handle
            .core
            .in_process_server()?
            .crazyflie(&self.handle.uri)
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
    Arc::new(
        items
            .into_iter()
            .map(|item| (item.name.clone(), item))
            .collect(),
    )
}

fn sorted_names(toc: &HashMap<String, TocItem>) -> Vec<String> {
    let mut names: Vec<String> = toc.keys().cloned().collect();
    names.sort();
    names
}

fn toc_type<'a>(toc: &'a HashMap<String, TocItem>, name: &str) -> Result<&'a str> {
    toc.get(name)
        .map(|item| item.value_type.as_str())
        .ok_or_else(|| Error::UnknownName(name.to_owned()))
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
        self.toc
            .get(name)
            .map(|item| item.writable)
            .ok_or_else(|| Error::UnknownName(name.to_owned()))
    }

    /// Whether the parameter can be stored in the Crazyflie's EEPROM. Asks
    /// the Crazyflie, like crazyflie-lib, for the params whose TOC entry says
    /// there is more to know
    pub async fn is_persistent(&self, name: &str) -> Result<bool> {
        if !self.toc.contains_key(name) {
            return Err(Error::UnknownName(name.to_owned()));
        }
        let request = Request::ParamIsPersistent {
            uri: self.handle.uri.clone(),
            name: name.to_owned(),
        };
        match self.handle.call(request).await? {
            Response::Bool { value } => Ok(value),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Default and stored value of a persistent parameter
    pub async fn persistent_get_state(&self, name: &str) -> Result<PersistentParamState> {
        let request = Request::ParamPersistentState {
            uri: self.handle.uri.clone(),
            name: name.to_owned(),
        };
        match self.handle.call(request).await? {
            Response::PersistentState(state) => Ok(state),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Store the current value of a persistent parameter in the EEPROM
    pub async fn persistent_store(&self, name: &str) -> Result<()> {
        self.handle
            .call_done(Request::ParamPersistentStore {
                uri: self.handle.uri.clone(),
                name: name.to_owned(),
            })
            .await
    }

    /// Clear the stored value of a persistent parameter
    pub async fn persistent_clear(&self, name: &str) -> Result<()> {
        self.handle
            .call_done(Request::ParamPersistentClear {
                uri: self.handle.uri.clone(),
                name: name.to_owned(),
            })
            .await
    }

    /// Read a parameter as `f64`
    pub async fn get_lossy(&self, name: &str) -> Result<f64> {
        match self
            .handle
            .call(Request::ParamGet {
                uri: self.handle.uri.clone(),
                name: name.to_owned(),
            })
            .await?
        {
            Response::Value { value } => Ok(value),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Set a parameter from an `f64`, converted to the parameter's type
    pub async fn set_lossy(&self, name: &str, value: f64) -> Result<()> {
        self.handle
            .call_done(Request::ParamSet {
                uri: self.handle.uri.clone(),
                name: name.to_owned(),
                value,
            })
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
                    return Ok(LogData {
                        timestamp,
                        data: values.into_iter().collect(),
                    });
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
        Ok(LogStream {
            _guard: guard,
            events,
        })
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

/// Low-level setpoints, same arguments as in `crazyflie-lib`
pub struct Commander {
    handle: Arc<Handle>,
}

impl Commander {
    /// Roll and pitch in degrees, yaw rate in degrees/s, thrust 0-65535. Send
    /// it repeatedly; the first setpoint must have zero thrust
    pub async fn setpoint_rpyt(
        &self,
        roll: f32,
        pitch: f32,
        yawrate: f32,
        thrust: u16,
    ) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::SetpointRpyt {
                uri,
                roll,
                pitch,
                yawrate,
                thrust,
            })
            .await
    }

    /// End the low-level setpoints, so that the high-level commander takes
    /// over after `remain_valid_milliseconds`
    pub async fn notify_setpoint_stop(&self, remain_valid_milliseconds: u32) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::NotifySetpointStop {
                uri,
                remain_valid_ms: remain_valid_milliseconds,
            })
            .await
    }
}

/// High-level commander, same arguments as in `crazyflie-lib`
pub struct HighLevelCommander {
    handle: Arc<Handle>,
}

impl HighLevelCommander {
    pub async fn take_off(
        &self,
        height: f32,
        yaw: Option<f32>,
        duration: f32,
        group_mask: Option<u8>,
    ) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::TakeOff {
                uri,
                height,
                yaw,
                duration,
                group_mask,
            })
            .await
    }

    pub async fn land(
        &self,
        height: f32,
        yaw: Option<f32>,
        duration: f32,
        group_mask: Option<u8>,
    ) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::Land {
                uri,
                height,
                yaw,
                duration,
                group_mask,
            })
            .await
    }

    pub async fn stop(&self, group_mask: Option<u8>) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::Stop { uri, group_mask })
            .await
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
            .call_done(Request::GoTo {
                uri,
                x,
                y,
                z,
                yaw,
                duration,
                relative,
                linear,
                group_mask,
            })
            .await
    }

    /// Define trajectory `trajectory_id` from `num_pieces` pieces at
    /// `memory_offset` in the trajectory memory
    pub async fn define_trajectory(
        &self,
        trajectory_id: u8,
        memory_offset: u32,
        num_pieces: u8,
        trajectory_type: Option<u8>,
    ) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::DefineTrajectory {
                uri,
                id: trajectory_id,
                memory_offset,
                num_pieces,
                trajectory_type,
            })
            .await
    }

    pub async fn start_trajectory(
        &self,
        trajectory_id: u8,
        time_scale: f32,
        relative_position: bool,
        relative_yaw: bool,
        reversed: bool,
        group_mask: Option<u8>,
    ) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::StartTrajectory {
                uri,
                id: trajectory_id,
                time_scale,
                relative_position,
                relative_yaw,
                reversed,
                group_mask,
            })
            .await
    }
}

/// Supervisor: arming, emergency stop and crash recovery
pub struct Supervisor {
    handle: Arc<Handle>,
}

impl Supervisor {
    pub async fn send_arming_request(&self, arm: bool) -> Result<()> {
        self.handle
            .call_done(Request::ArmingRequest {
                uri: self.handle.uri.clone(),
                arm,
            })
            .await
    }

    pub async fn send_emergency_stop(&self) -> Result<()> {
        self.handle
            .call_done(Request::EmergencyStop {
                uri: self.handle.uri.clone(),
            })
            .await
    }

    /// Recover a Crazyflie that crashed (tumbled)
    pub async fn send_crash_recovery_request(&self) -> Result<()> {
        self.handle
            .call_done(Request::CrashRecovery {
                uri: self.handle.uri.clone(),
            })
            .await
    }
}

/// Localization systems
pub struct Localization {
    pub lighthouse: Lighthouse,
    pub loco_positioning: LocoPositioning,
}

/// Loco Positioning
pub struct LocoPositioning {
    handle: Arc<Handle>,
}

impl LocoPositioning {
    /// Send a Loco Positioning Protocol packet to anchor `dest_id` through the Crazyflie
    pub async fn send_short_lpp_packet(&self, dest_id: u8, data: &[u8]) -> Result<()> {
        let uri = self.handle.uri.clone();
        self.handle
            .call_done(Request::SendShortLppPacket {
                uri,
                dest_id,
                data: data.to_vec(),
            })
            .await
    }
}

/// Lighthouse positioning
pub struct Lighthouse {
    handle: Arc<Handle>,
}

/// Sweep angles from one base station, like `crazyflie-lib`'s
#[derive(Debug, Clone)]
pub struct LighthouseAngleData {
    pub base_station: u8,
    pub angles: LighthouseAngles,
}

/// Sweep angles of the four sensors, in radians
#[derive(Debug, Clone)]
pub struct LighthouseAngles {
    pub x: [f32; 4],
    pub y: [f32; 4],
}

impl Lighthouse {
    /// Store the geometries and calibrations of these base stations, from the
    /// lighthouse memory, in the Crazyflie's flash. Returns whether it did
    pub async fn persist_lighthouse_data(
        &self,
        geo_list: &[u8],
        calib_list: &[u8],
    ) -> Result<bool> {
        let request = Request::LighthousePersist {
            uri: self.handle.uri.clone(),
            geometries: geo_list.to_vec(),
            calibrations: calib_list.to_vec(),
        };
        match self.handle.call(request).await? {
            Response::Bool { value } => Ok(value),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// Sweep angles as the Crazyflie sends them, which it only does while the
    /// `locSrv.enLhAngleStream` param is 1
    pub async fn angle_stream(&self) -> Result<BoxStream<'static, LighthouseAngleData>> {
        let uri = self.handle.uri.clone();
        self.handle
            .stream(
                |sub| Request::LighthouseAngleSubscribe { uri, sub },
                |event| match event {
                    Event::LighthouseAngles { base_station, x, y } => Some(LighthouseAngleData {
                        base_station,
                        angles: LighthouseAngles { x, y },
                    }),
                    _ => None,
                },
            )
            .await
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
        match self
            .handle
            .call(Request::LinkStatistics {
                uri: self.handle.uri.clone(),
            })
            .await?
        {
            Response::LinkStatistics(stats) => Ok(stats),
            _ => Err(Error::UnexpectedReply),
        }
    }
}
