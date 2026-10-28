//! WebSocket front end: one session per WebSocket connection, one JSON message per text frame

use super::Server;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use crazyflie_connection_protocol::ClientMessage;
use futures::{SinkExt, StreamExt};
use log::{info, warn};
use std::net::SocketAddr;

/// Serve `server` over WebSocket on `addr` until [Server::shutdown]
pub async fn serve(server: Server, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_on(server, listener, std::future::pending()).await
}

/// Serve `server` over WebSocket on a bound listener until `stop` completes or
/// [Server::shutdown]. Then the clients are disconnected
pub async fn serve_on(
    server: Server,
    listener: tokio::net::TcpListener,
    stop: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    info!("Listening on ws://{}", listener.local_addr()?);
    let stopped = server.stopped();
    let app = Router::new()
        .route("/", get(upgrade))
        .with_state(server.clone());
    let result = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        tokio::select! {
            _ = stop => {}
            _ = stopped => {}
        }
    })
    .await;
    server.close_clients();
    result
}

async fn upgrade(
    ws: WebSocketUpgrade,
    State(server): State<Server>,
    axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| client(server, socket, peer))
}

async fn client(server: Server, socket: WebSocket, peer: SocketAddr) {
    info!("Client connected from {peer}");
    let mut closed = server.clients_closed();
    let (session, mut replies) = server.open_session();
    let (mut sink, mut incoming) = socket.split();

    let writer = tokio::spawn(async move {
        while let Some(message) = replies.recv().await {
            let Ok(text) = serde_json::to_string(&message) else {
                continue;
            };
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    loop {
        let message = tokio::select! {
            message = incoming.next() => message,
            _ = closed.wait_for(|closed| *closed) => break,
        };
        let Some(Ok(message)) = message else { break };
        match message {
            Message::Text(text) => match serde_json::from_str::<ClientMessage>(text.as_str()) {
                Ok(message) => session.handle(message),
                Err(e) => {
                    // Answer with the id if there is one, so hand-typed requests get feedback
                    let id = serde_json::from_str::<serde_json::Value>(text.as_str())
                        .ok()
                        .and_then(|value| value.get("id")?.as_u64());
                    match id {
                        Some(id) => session.reject(id, format!("Invalid request: {e}")),
                        None => warn!("Invalid message from {peer}: {e}"),
                    }
                }
            },
            Message::Close(_) => break,
            _ => {}
        }
    }

    drop(session);
    writer.abort();
    info!("Client {peer} disconnected");
}
