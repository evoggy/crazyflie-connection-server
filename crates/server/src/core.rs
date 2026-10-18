//! Request/reply matching and stream routing, shared by both transports
//!
//! Both transports reduce to two channels: client messages out, server
//! messages in. The in-process transport feeds them to a
//! [Session](crate::Session); the WebSocket transport (de)serializes them.

use crate::{Error, Result};
use crazyflie_connection_protocol::{ClientMessage, Event, Request, Response, ServerMessage};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message;

pub(crate) struct Core {
    mode: String,
    /// The in-process server in direct and shared mode
    #[cfg(feature = "server")]
    in_process: Option<crate::Server>,
    /// Shared mode: dropping it stops serving other programs
    #[cfg(feature = "server")]
    _listener: Option<oneshot::Sender<()>>,
    tx: mpsc::UnboundedSender<ClientMessage>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<ServerMessage>>>,
    /// Open streams: number → (Crazyflie URI, events)
    subs: Mutex<HashMap<u64, (String, mpsc::UnboundedSender<Event>)>>,
    attached: Mutex<HashMap<String, Attached>>,
}

/// Handles to one attached Crazyflie in this client
struct Attached {
    count: usize,
    /// `Some(reason)` once the Crazyflie is disconnected
    lost: watch::Sender<Option<String>>,
}

impl Core {
    fn new(mode: String, tx: mpsc::UnboundedSender<ClientMessage>) -> Self {
        Self {
            mode,
            #[cfg(feature = "server")]
            in_process: None,
            #[cfg(feature = "server")]
            _listener: None,
            tx,
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            subs: Mutex::new(HashMap::new()),
            attached: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(feature = "server")]
    pub(crate) fn in_process(
        server: crate::Server,
        mode: String,
        listener: Option<oneshot::Sender<()>>,
    ) -> Arc<Self> {
        let (session, mut incoming) = server.open_session();
        let in_process = server.clone();
        let (tx, mut outgoing) = mpsc::unbounded_channel::<ClientMessage>();
        tokio::spawn(async move {
            // Ends when the Core is dropped; dropping the session detaches everything
            while let Some(message) = outgoing.recv().await {
                session.handle(message);
            }
        });
        let core = Arc::new(Self { in_process: Some(in_process), _listener: listener, ..Self::new(mode, tx) });
        let weak = Arc::downgrade(&core);
        tokio::spawn(async move {
            while let Some(message) = incoming.recv().await {
                if !route(&weak, message) {
                    return;
                }
            }
            if let Some(core) = weak.upgrade() {
                core.connection_lost();
            }
        });
        core
    }

    pub(crate) async fn websocket(url: &str) -> Result<Arc<Self>> {
        let (socket, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| Error::Transport(format!("Connecting to {url}: {e}")))?;
        let (mut sink, mut stream) = socket.split();
        let (tx, mut outgoing) = mpsc::unbounded_channel::<ClientMessage>();
        tokio::spawn(async move {
            while let Some(message) = outgoing.recv().await {
                let Ok(text) = serde_json::to_string(&message) else { continue };
                if sink.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });
        let core = Arc::new(Self::new(url.to_owned(), tx));
        let weak = Arc::downgrade(&core);
        tokio::spawn(async move {
            while let Some(Ok(message)) = stream.next().await {
                let Message::Text(text) = message else { continue };
                let Ok(message) = serde_json::from_str::<ServerMessage>(text.as_str()) else {
                    log::warn!("Invalid message from server: {text}");
                    continue;
                };
                if !route(&weak, message) {
                    return;
                }
            }
            if let Some(core) = weak.upgrade() {
                core.connection_lost();
            }
        });
        Ok(core)
    }

    pub(crate) fn mode(&self) -> &str {
        &self.mode
    }

    #[cfg(feature = "server")]
    pub(crate) fn in_process_server(&self) -> Option<&crate::Server> {
        self.in_process.as_ref()
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Send a request and wait for its reply
    pub(crate) async fn call(&self, request: Request) -> Result<Response> {
        let id = self.next_id();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, reply_tx);
        if self.tx.send(ClientMessage { id, request }).is_err() {
            self.pending.lock().unwrap().remove(&id);
            return Err(Error::ConnectionLost);
        }
        match reply_rx.await {
            Ok(ServerMessage::Reply { ok: Some(response), .. }) => Ok(response),
            Ok(ServerMessage::Reply { error: Some(error), .. }) => Err(Error::Server(error)),
            Ok(_) => Err(Error::UnexpectedReply),
            Err(_) => Err(Error::ConnectionLost),
        }
    }

    /// Send a request without waiting for the reply
    pub(crate) fn send(&self, request: Request) {
        let id = self.next_id();
        let _ = self.tx.send(ClientMessage { id, request });
    }

    /// Reserve a stream number and its event channel, before the subscribe request is sent
    pub(crate) fn open_stream(self: &Arc<Self>, uri: &str) -> (StreamGuard, mpsc::UnboundedReceiver<Event>) {
        let sub = self.next_id();
        let (tx, rx) = mpsc::unbounded_channel();
        self.subs.lock().unwrap().insert(sub, (uri.to_owned(), tx));
        (StreamGuard { core: self.clone(), sub }, rx)
    }

    /// Count a new handle to an attached Crazyflie and return its disconnect watch
    pub(crate) fn add_attached(&self, key: &str) -> watch::Receiver<Option<String>> {
        let mut attached = self.attached.lock().unwrap();
        let entry = attached
            .entry(key.to_owned())
            .or_insert_with(|| Attached { count: 0, lost: watch::Sender::new(None) });
        entry.count += 1;
        entry.lost.subscribe()
    }

    /// Detach from a Crazyflie now, whatever handles are left: its streams
    /// end and further calls fail
    pub(crate) fn detach(&self, key: &str) {
        if let Some(entry) = self.attached.lock().unwrap().remove(key) {
            entry.lost.send_replace(Some("Detached".to_owned()));
            self.send(Request::Detach { uri: key.to_owned() });
        }
        self.subs.lock().unwrap().retain(|_, (uri, _)| uri != key);
    }

    /// Drop a handle. The last one detaches from the Crazyflie
    pub(crate) fn remove_attached(&self, key: &str) {
        let mut attached = self.attached.lock().unwrap();
        if let Some(entry) = attached.get_mut(key) {
            entry.count -= 1;
            if entry.count == 0 {
                attached.remove(key);
                self.send(Request::Detach { uri: key.to_owned() });
            }
        }
    }

    fn connection_lost(&self) {
        self.pending.lock().unwrap().clear();
        self.subs.lock().unwrap().clear();
        for entry in self.attached.lock().unwrap().values() {
            entry.lost.send_replace(Some("Connection to the server lost".to_owned()));
        }
    }
}

/// Route one server message. Returns `false` once the client is gone
fn route(core: &Weak<Core>, message: ServerMessage) -> bool {
    let Some(core) = core.upgrade() else { return false };
    match message {
        ServerMessage::Reply { id, .. } => {
            if let Some(reply_tx) = core.pending.lock().unwrap().remove(&id) {
                let _ = reply_tx.send(message);
            }
        }
        ServerMessage::Event { sub, event } => {
            let mut subs = core.subs.lock().unwrap();
            if let Some((_, tx)) = subs.get(&sub)
                && tx.send(event).is_err()
            {
                subs.remove(&sub);
            }
        }
        ServerMessage::Disconnected { uri, reason } => {
            if let Some(entry) = core.attached.lock().unwrap().get(&uri) {
                entry.lost.send_replace(Some(reason));
            }
            // The server ended the Crazyflie's streams with it
            core.subs.lock().unwrap().retain(|_, (key, _)| *key != uri);
        }
    }
    true
}

/// Closes a stream on the server when dropped
pub(crate) struct StreamGuard {
    core: Arc<Core>,
    sub: u64,
}

impl StreamGuard {
    pub(crate) fn sub(&self) -> u64 {
        self.sub
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.core.subs.lock().unwrap().remove(&self.sub);
        self.core.send(Request::Unsubscribe { sub: self.sub });
    }
}
