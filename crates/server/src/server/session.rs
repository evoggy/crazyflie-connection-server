//! One client's view of the server: its attached Crazyflies and open streams

use super::flash::{Image, Target};
use super::{CfEntry, Server, memory, parse_address};
use crazyflie_lib::Crazyflie;
use crazyflie_lib::subsystems::log::{LogPeriod, LogStream};
use crazyflie_connection_protocol::{
    ClientMessage, DEFAULT_ADDRESS, Event, LinkStatistics, PersistentParamState, Request, Response, ServerMessage,
    normalize_uri,
};
use futures::{Stream, StreamExt};
use log::debug;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

/// A client connection to the server
///
/// Requests passed to [Session::handle] run concurrently; replies and stream
/// events go to the receiver returned by [Server::open_session]. Dropping the
/// session closes its streams and detaches it from every Crazyflie.
pub struct Session {
    server: Server,
    shared: Arc<SessionShared>,
}

pub(crate) struct SessionShared {
    id: u64,
    name: Mutex<String>,
    tx: mpsc::UnboundedSender<ServerMessage>,
    /// Set when the [Session] is dropped, so requests still running don't attach
    closed: AtomicBool,
    /// Keys of the attached Crazyflies
    attached: Mutex<HashSet<String>>,
    /// Open streams by `sub`. Dropping one cancels its task
    subs: Mutex<HashMap<u64, Sub>>,
}

struct Sub {
    key: String,
    _cancel: oneshot::Sender<()>,
}

impl Session {
    pub(crate) fn new(server: Server, id: u64) -> (Self, mpsc::UnboundedReceiver<ServerMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let shared = Arc::new(SessionShared {
            id,
            name: Mutex::new(format!("client-{id}")),
            tx,
            closed: AtomicBool::new(false),
            attached: Mutex::new(HashSet::new()),
            subs: Mutex::new(HashMap::new()),
        });
        (Self { server, shared }, rx)
    }

    pub(crate) fn shared(&self) -> &Arc<SessionShared> {
        &self.shared
    }

    /// Handle one request. The reply is sent on the session's channel
    pub fn handle(&self, message: ClientMessage) {
        let server = self.server.clone();
        let shared = self.shared.clone();
        tokio::spawn(async move {
            let id = message.id;
            let tx = shared.tx.clone();
            // A request that panics (e.g. in crazyflie-link) still gets a reply
            let handled = tokio::spawn(async move { dispatch(&server, &shared, message.request).await }).await;
            let reply = match handled.unwrap_or_else(|e| Err(format!("The server failed on this request: {e}"))) {
                Ok(response) => ServerMessage::Reply { id, ok: Some(response), error: None },
                Err(error) => {
                    debug!("Request {id} failed: {error}");
                    ServerMessage::Reply { id, ok: None, error: Some(error) }
                }
            };
            let _ = tx.send(reply);
        });
    }

    /// Send an error reply for a message that could not be parsed
    pub fn reject(&self, id: u64, error: String) {
        let _ = self.shared.tx.send(ServerMessage::Reply { id, ok: None, error: Some(error) });
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.subs.lock().unwrap().clear();
        let attached: Vec<String> = self.shared.attached.lock().unwrap().drain().collect();
        for key in attached {
            self.server.detach(self.shared.id, &key);
        }
        self.server.session_closed(self.shared.id);
    }
}

impl SessionShared {
    fn name(&self) -> String {
        self.name.lock().unwrap().clone()
    }

    /// Called by the server when an attached Crazyflie disconnects
    pub(crate) fn crazyflie_lost(&self, key: &str, reason: &str) {
        self.attached.lock().unwrap().remove(key);
        self.subs.lock().unwrap().retain(|_, sub| sub.key != key);
        let _ = self.tx.send(ServerMessage::Disconnected { uri: key.to_owned(), reason: reason.to_owned() });
    }

    /// Register stream `sub` and return its cancel receiver. An existing stream with the same number is closed
    fn add_sub(&self, sub: u64, key: &str) -> oneshot::Receiver<()> {
        let (cancel, cancelled) = oneshot::channel();
        self.subs.lock().unwrap().insert(sub, Sub { key: key.to_owned(), _cancel: cancel });
        cancelled
    }
}

/// Look up an attached Crazyflie
fn attached(server: &Server, session: &SessionShared, uri: &str) -> Result<Arc<CfEntry>, String> {
    let key = normalize_uri(uri)?.key;
    if !session.attached.lock().unwrap().contains(&key) {
        return Err(format!("Not attached to {key}"));
    }
    server.entry(&key).ok_or_else(|| format!("{key} is not connected"))
}

fn lib_error(e: crazyflie_lib::Error) -> String {
    e.to_string()
}

async fn dispatch(server: &Server, session: &Arc<SessionShared>, request: Request) -> Result<Response, String> {
    if let Some(uri) = memory::uri_of(&request) {
        let entry = attached(server, session, uri)?;
        return memory::handle(&entry, request).await;
    }
    match request {
        Request::Hello { name } => {
            *session.name.lock().unwrap() = name.clone();
            server.rename_client(session.id, &name);
            Ok(Response::Hello { server: server.name().to_owned(), protocol: crazyflie_connection_protocol::PROTOCOL_VERSION })
        }
        Request::Scan { address } => {
            let address = parse_address(address.as_deref().unwrap_or(DEFAULT_ADDRESS))?;
            Ok(Response::Scan { found: server.scan(address).await? })
        }
        Request::ScanSelected { uris } => {
            let uris = uris.iter().map(|uri| normalize_uri(uri)).collect::<Result<Vec<_>, String>>()?;
            Ok(Response::Scan { found: server.scan_selected(&uris).await })
        }
        Request::List => Ok(Response::List { connected: server.list() }),
        Request::Attach { uri } => {
            let uri = normalize_uri(&uri)?;
            let entry = server.attach(session.id, &session.name(), &uri).await?;
            if session.closed.load(Ordering::SeqCst) {
                server.detach(session.id, &entry.key);
                return Err("Session closed".to_owned());
            }
            session.attached.lock().unwrap().insert(entry.key.clone());
            Ok(Response::Attached(entry.info.clone()))
        }
        Request::Detach { uri } => {
            let key = normalize_uri(&uri)?.key;
            if session.attached.lock().unwrap().remove(&key) {
                session.subs.lock().unwrap().retain(|_, sub| sub.key != key);
                server.detach(session.id, &key);
            }
            Ok(Response::Done)
        }
        Request::ParamGet { uri, name } => {
            let entry = attached(server, session, &uri)?;
            let value = entry.cf.param.get_lossy(&name).await.map_err(lib_error)?;
            Ok(Response::Value { value })
        }
        Request::ParamSet { uri, name, value } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.param.set_lossy(&name, value).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::ParamPersistentState { uri, name } => {
            let entry = attached(server, session, &uri)?;
            let state = entry.cf.param.persistent_get_state(&name).await.map_err(lib_error)?;
            Ok(Response::PersistentState(PersistentParamState {
                is_stored: state.is_stored,
                default_value: state.default_value.to_f64_lossy(),
                stored_value: state.stored_value.map(|value| value.to_f64_lossy()),
            }))
        }
        Request::ParamPersistentStore { uri, name } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.param.persistent_store(&name).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::ParamPersistentClear { uri, name } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.param.persistent_clear(&name).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::ParamWatch { uri, sub } => {
            let entry = attached(server, session, &uri)?;
            let changes = entry.cf.param.watch_change().await.map_err(lib_error)?;
            let events = changes.map(|(name, value)| Event::ParamChanged { name, value: value.to_f64_lossy() });
            forward(session, &entry, sub, events);
            Ok(Response::Done)
        }
        Request::LogSubscribe { uri, sub, vars, period_ms } => {
            let entry = attached(server, session, &uri)?;
            log_subscribe(session, &entry, sub, &vars, period_ms).await?;
            Ok(Response::Done)
        }
        Request::ConsoleSubscribe { uri, sub, history } => {
            let entry = attached(server, session, &uri)?;
            let lines = entry.console.subscribe(history);
            forward(session, &entry, sub, lines.map(|line| Event::Console { line }));
            Ok(Response::Done)
        }
        Request::LighthouseAngleSubscribe { uri, sub } => {
            let entry = attached(server, session, &uri)?;
            let angles = entry.cf.localization.lighthouse.angle_stream().await;
            let events = angles.map(|data| Event::LighthouseAngles {
                base_station: data.base_station,
                x: data.angles.x,
                y: data.angles.y,
            });
            forward(session, &entry, sub, events);
            Ok(Response::Done)
        }
        Request::Unsubscribe { sub } => {
            session.subs.lock().unwrap().remove(&sub);
            Ok(Response::Done)
        }
        Request::TakeOff { uri, height, yaw, duration, group_mask } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.high_level_commander.take_off(height, yaw, duration, group_mask).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::Land { uri, height, yaw, duration, group_mask } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.high_level_commander.land(height, yaw, duration, group_mask).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::Stop { uri, group_mask } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.high_level_commander.stop(group_mask).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::GoTo { uri, x, y, z, yaw, duration, relative, linear, group_mask } => {
            let entry = attached(server, session, &uri)?;
            entry
                .cf
                .high_level_commander
                .go_to(x, y, z, yaw, duration, relative, linear, group_mask)
                .await
                .map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::DefineTrajectory { uri, id, memory_offset, num_pieces, trajectory_type } => {
            let entry = attached(server, session, &uri)?;
            entry
                .cf
                .high_level_commander
                .define_trajectory(id, memory_offset, num_pieces, trajectory_type)
                .await
                .map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::StartTrajectory { uri, id, time_scale, relative_position, relative_yaw, reversed, group_mask } => {
            let entry = attached(server, session, &uri)?;
            entry
                .cf
                .high_level_commander
                .start_trajectory(id, time_scale, relative_position, relative_yaw, reversed, group_mask)
                .await
                .map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::SetpointRpyt { uri, roll, pitch, yawrate, thrust } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.commander.setpoint_rpyt(roll, pitch, yawrate, thrust).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::NotifySetpointStop { uri, remain_valid_ms } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.commander.notify_setpoint_stop(remain_valid_ms).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::ArmingRequest { uri, arm } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.supervisor.send_arming_request(arm).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::EmergencyStop { uri } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.supervisor.send_emergency_stop().await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::CrashRecovery { uri } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.supervisor.send_crash_recovery_request().await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::Flash { uris, sub, images } => {
            use base64::Engine as _;
            let uris = uris.iter().map(|uri| normalize_uri(uri)).collect::<Result<Vec<_>, String>>()?;
            let images = images
                .into_iter()
                .map(|image| {
                    Ok(Image {
                        target: Target::parse(&image.target)?,
                        start: image.start,
                        data: base64::engine::general_purpose::STANDARD
                            .decode(&image.data)
                            .map_err(|e| format!("The {} image isn't base64: {e}", image.target))?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            server.start_flash(uris, &session.name(), images, session.tx.clone(), sub)?;
            Ok(Response::Done)
        }
        Request::Watch { sub } => {
            watch(session, sub, server.watch_events());
            Ok(Response::Done)
        }
        Request::SendShortLppPacket { uri, dest_id, data } => {
            let entry = attached(server, session, &uri)?;
            entry.cf.localization.loco_positioning.send_short_lpp_packet(dest_id, &data).await.map_err(lib_error)?;
            Ok(Response::Done)
        }
        Request::LighthousePersist { uri, geometries, calibrations } => {
            let entry = attached(server, session, &uri)?;
            let lighthouse = &entry.cf.localization.lighthouse;
            let persisted = lighthouse.persist_lighthouse_data(&geometries, &calibrations).await.map_err(lib_error)?;
            Ok(Response::Bool { value: persisted })
        }
        // Handled above
        Request::MemoryRead { .. }
        | Request::MemoryWrite { .. }
        | Request::OneWireRead { .. }
        | Request::EepromConfigRead { .. }
        | Request::EepromConfigWrite { .. }
        | Request::LighthouseGeometryRead { .. }
        | Request::LighthouseGeometryWrite { .. }
        | Request::LighthouseCalibrationRead { .. }
        | Request::LighthouseCalibrationWrite { .. }
        | Request::TrajectoryWriteUncompressed { .. }
        | Request::TrajectoryWriteCompressed { .. }
        | Request::LocoReadIdList { .. }
        | Request::LocoReadAnchor { .. } => unreachable!("a memory request"),
        Request::Platform { uri, command } => {
            server.platform(&normalize_uri(&uri)?, command, &session.name()).await?;
            Ok(Response::Done)
        }
        Request::LinkStatistics { uri } => {
            let entry = attached(server, session, &uri)?;
            let stats = entry.cf.link_service.get_statistics().await;
            Ok(Response::LinkStatistics(LinkStatistics {
                link_quality: stats.link_quality,
                uplink_rate: stats.uplink_rate,
                downlink_rate: stats.downlink_rate,
                radio_send_rate: stats.radio_send_rate,
                avg_retries: stats.avg_retries,
                power_detector_rate: stats.power_detector_rate,
                rssi: stats.rssi,
            }))
        }
    }
}

/// How a stream task ended
enum End {
    Cancelled,
    Ended(String),
}

/// Forward the server's [Event::Busy] and [Event::Available] to the client as
/// stream `sub` until it is cancelled
fn watch(session: &Arc<SessionShared>, sub: u64, mut events: tokio::sync::broadcast::Receiver<Event>) {
    use tokio::sync::broadcast::error::RecvError;
    // Not tied to a Crazyflie: it stays open when one is lost
    let mut cancelled = session.add_sub(sub, "");
    let tx = session.tx.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut cancelled => break,
                event = events.recv() => match event {
                    Ok(event) => {
                        if tx.send(ServerMessage::Event { sub, event }).is_err() {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                },
            }
        }
    });
}

/// Forward a stream of events to the client as stream `sub` until it is
/// cancelled, the Crazyflie disconnects or the stream ends
fn forward<S>(session: &Arc<SessionShared>, entry: &Arc<CfEntry>, sub: u64, events: S)
where
    S: Stream<Item = Event> + Send + 'static,
{
    let mut cancelled = session.add_sub(sub, &entry.key);
    let mut closed = entry.closed.subscribe();
    let tx = session.tx.clone();
    let entry = entry.clone();
    tokio::spawn(async move {
        let mut events = std::pin::pin!(events);
        let end = loop {
            tokio::select! {
                _ = &mut cancelled => break End::Cancelled,
                _ = closed.wait_for(|closed| *closed) => break End::Ended(entry.lost_reason()),
                event = events.next() => match event {
                    Some(event) => {
                        if tx.send(ServerMessage::Event { sub, event }).is_err() {
                            break End::Cancelled;
                        }
                    }
                    None => break End::Ended("Stream ended".to_owned()),
                },
            }
        };
        if let End::Ended(reason) = end {
            let _ = tx.send(ServerMessage::Event { sub, event: Event::Ended { reason } });
        }
    });
}

/// Payload bytes in one log block (`LOG_MAX_LEN` in the firmware's log.c)
const LOG_BLOCK_BYTES: usize = 26;

/// Create and start the log blocks for one subscription: as many as its
/// variables need, each block's data arriving as its own [Event::Log]. The
/// blocks are stopped when the subscription is cancelled, and deleted by
/// crazyflie-lib on the next block creation
async fn log_subscribe(
    session: &Arc<SessionShared>,
    entry: &Arc<CfEntry>,
    sub: u64,
    vars: &[String],
    period_ms: u64,
) -> Result<(), String> {
    if vars.is_empty() {
        return Err("No log variables to subscribe to".to_owned());
    }
    let mut streams = Vec::new();
    for block_vars in split_into_blocks(&entry.cf, vars)? {
        match start_block(&entry.cf, &block_vars, period_ms).await {
            Ok(stream) => streams.push(stream),
            Err(e) => {
                stop_blocks(streams).await;
                return Err(e);
            }
        }
    }

    let mut cancelled = session.add_sub(sub, &entry.key);
    let mut closed = entry.closed.subscribe();
    let tx = session.tx.clone();
    tokio::spawn(async move {
        let end = loop {
            tokio::select! {
                _ = &mut cancelled => break End::Cancelled,
                _ = closed.wait_for(|closed| *closed) => break End::Ended("Crazyflie disconnected".to_owned()),
                (data, _, _) = futures::future::select_all(streams.iter().map(|stream| Box::pin(stream.next()))) => match data {
                    Ok(data) => {
                        let values = data.data.into_iter().map(|(name, value)| (name, value.to_f64_lossy())).collect();
                        let event = Event::Log { timestamp: data.timestamp, values };
                        if tx.send(ServerMessage::Event { sub, event }).is_err() {
                            break End::Cancelled;
                        }
                    }
                    Err(e) => break End::Ended(e.to_string()),
                },
            }
        };
        match end {
            End::Cancelled => stop_blocks(streams).await,
            End::Ended(reason) => {
                let _ = tx.send(ServerMessage::Event { sub, event: Event::Ended { reason } });
            }
        }
    });
    Ok(())
}

async fn start_block(cf: &Crazyflie, vars: &[String], period_ms: u64) -> Result<LogStream, String> {
    let period = LogPeriod::from_millis(period_ms).map_err(lib_error)?;
    let mut block = cf.log.create_block().await.map_err(lib_error)?;
    for var in vars {
        block.add_variable(var).await.map_err(lib_error)?;
    }
    block.start(period).await.map_err(lib_error)
}

async fn stop_blocks(streams: Vec<LogStream>) {
    for stream in streams {
        let _ = stream.stop().await;
    }
}

/// Group `vars`, in order, into log blocks of at most [LOG_BLOCK_BYTES]
fn split_into_blocks(cf: &Crazyflie, vars: &[String]) -> Result<Vec<Vec<String>>, String> {
    let mut blocks: Vec<Vec<String>> = Vec::new();
    let mut used = 0;
    for var in vars {
        let size = cf.log.get_type(var).map_err(lib_error)?.byte_length();
        if blocks.is_empty() || used + size > LOG_BLOCK_BYTES {
            blocks.push(Vec::new());
            used = 0;
        }
        blocks.last_mut().expect("pushed above").push(var.clone());
        used += size;
    }
    Ok(blocks)
}
