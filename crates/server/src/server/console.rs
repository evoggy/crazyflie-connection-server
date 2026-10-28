//! Console lines, split by the server. crazyflie-lib 0.8's line stream takes
//! one line from each console packet, so the second of two lines that share a
//! packet only comes with the next packet, and lines lag behind the console

use crazyflie_lib::Crazyflie;
use futures::{Stream, StreamExt};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Lines buffered for a slow subscriber before it skips some
const LINES_BUFFERED: usize = 1024;

/// The console lines of a connected Crazyflie
pub(crate) struct ConsoleLines {
    /// Every line since the Crazyflie was connected, and where new ones go.
    /// One lock, so that a subscriber gets each line once: in its history or new
    state: Mutex<(Vec<String>, broadcast::Sender<String>)>,
}

impl ConsoleLines {
    /// Split the console of `cf` into lines, from the text since it was connected
    pub(crate) async fn start(cf: &Crazyflie) -> Arc<Self> {
        let lines = Arc::new(Self {
            state: Mutex::new((Vec::new(), broadcast::Sender::new(LINES_BUFFERED))),
        });
        // The first item is the text since the connection, then each packet's
        let mut text = Box::pin(cf.console.stream().await);
        let splitter = lines.clone();
        tokio::spawn(async move {
            let mut partial = String::new();
            while let Some(chunk) = text.next().await {
                partial.push_str(&chunk);
                for line in take_lines(&mut partial) {
                    let mut state = splitter.state.lock().unwrap();
                    state.0.push(line.clone());
                    let _ = state.1.send(line);
                }
            }
        });
        lines
    }

    /// The lines from now on, after the ones since the connection with `history`
    pub(crate) fn subscribe(&self, history: bool) -> impl Stream<Item = String> + Send + 'static {
        let (old, new) = {
            let state = self.state.lock().unwrap();
            (
                if history { state.0.clone() } else { Vec::new() },
                state.1.subscribe(),
            )
        };
        let new = futures::stream::unfold(new, |mut new| async move {
            loop {
                match new.recv().await {
                    Ok(line) => return Some((line, new)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        futures::stream::iter(old).chain(new)
    }
}

/// The complete lines at the start of `partial`, which keeps the rest
fn take_lines(partial: &mut String) -> Vec<String> {
    let mut lines = Vec::new();
    while let Some(end) = partial.find('\n') {
        lines.push(partial[..end].to_owned());
        partial.drain(..=end);
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::take_lines;

    #[test]
    fn every_line_of_a_packet() {
        let mut partial = "SYS: a\nSYS: b\nSYS: pre".to_owned();
        assert_eq!(take_lines(&mut partial), vec!["SYS: a", "SYS: b"]);
        assert_eq!(partial, "SYS: pre");
        partial.push_str("diction ±1\n");
        assert_eq!(take_lines(&mut partial), vec!["SYS: prediction ±1"]);
        assert!(partial.is_empty());
    }
}
