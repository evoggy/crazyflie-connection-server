//! Requests that reach a Crazyflie without connecting to it: checking that
//! it answers, and the commands its nRF51 handles (reboot, power)

use super::{FIRMWARE_START, Server, Slot};
use crazyflie_connection_protocol::{CfInfo, Event, NormalizedUri, PlatformCommand};
use crazyflie_lib::Crazyflie;
use crazyflie_lib::crazyflie_link::Packet;
use futures::future::join_all;
use log::info;
use std::time::Duration;

/// Packets sent to a Crazyflie before it counts as not answering
const ANSWER_ATTEMPTS: usize = 3;

impl Server {
    /// The Crazyflies of `uris` that answer. Connected ones answer without
    /// radio traffic; ones a job has taken over don't
    pub(crate) async fn scan_selected(&self, uris: &[NormalizedUri]) -> Vec<CfInfo> {
        let checks = uris.iter().map(|uri| async move {
            if let Some(entry) = self.entry(&uri.key) {
                return Some(entry.cf_info());
            }
            if self.inner.busy.lock().unwrap().contains_key(&uri.key) {
                return None;
            }
            let radio = self.answers(uri).await?;
            Some(CfInfo { uri: uri.key.clone(), clients: Vec::new(), radio })
        });
        join_all(checks).await.into_iter().flatten().collect()
    }

    /// Whether the Crazyflie at `uri` answers a packet (a few tries), and on
    /// which radio (`None` over USB)
    pub(super) async fn answers(&self, uri: &NormalizedUri) -> Option<Option<usize>> {
        let Some(link) = self.link_uri(uri).await else {
            let found = self.inner.link_context.scan_selected(vec![uri.key.as_str()]).await;
            return found.is_ok_and(|found| !found.is_empty()).then_some(None);
        };
        let (link_uri, slot) = link.ok()?;
        let mut answered = false;
        for _ in 0..ANSWER_ATTEMPTS {
            if matches!(self.inner.link_context.scan_selected(vec![link_uri.as_str()]).await, Ok(found) if !found.is_empty())
            {
                answered = true;
                break;
            }
        }
        self.release_radio(Some(slot));
        answered.then_some(Some(slot.radio))
    }

    /// A radio URI on a picked radio, without options: the radio it is
    /// connected on, else the pinned one or one picked like for a connection.
    /// The radio stays counted until the slot is released. `None` for a URI
    /// that isn't a radio one
    async fn link_uri(&self, uri: &NormalizedUri) -> Option<Result<(String, Slot), String>> {
        let path = uri.key.strip_prefix("radio:///")?;
        let connected_on = self.entry(&uri.key).and_then(|entry| entry.slot).map(|slot| slot.radio);
        let picked = self.pick_radio(connected_on.or(uri.radio), uri.channel().unwrap_or_default()).await;
        Some(picked.map(|slot| (format!("radio://{}/{path}", slot.radio), slot)))
    }

    /// Send a command to the nRF51 of the Crazyflie at `uri`, for client
    /// `who`. Its clients are disconnected first. A reboot returns once the
    /// firmware answers again, and the Crazyflie is busy until then
    pub(crate) async fn platform(
        &self,
        uri: &NormalizedUri,
        command: PlatformCommand,
        new_uri: Option<&NormalizedUri>,
        who: &str,
    ) -> Result<(), String> {
        if new_uri.is_some() && command != PlatformCommand::Reboot {
            return Err("Only a reboot can bring a Crazyflie back at another URI".to_owned());
        }
        if new_uri.is_some_and(|new_uri| !new_uri.key.starts_with("radio:///")) {
            return Err("A Crazyflie can only be rebooted onto another radio URI".to_owned());
        }
        let Some(link) = self.link_uri(uri).await else {
            return Err(format!("{} can't take platform commands: the nRF51 only gets them by radio", uri.key));
        };
        let (link_uri, slot) = link?;
        let sent = self.send_platform(uri, &link_uri, command, new_uri, who).await;
        self.release_radio(Some(slot));
        sent
    }

    async fn send_platform(
        &self,
        uri: &NormalizedUri,
        link_uri: &str,
        command: PlatformCommand,
        new_uri: Option<&NormalizedUri>,
        who: &str,
    ) -> Result<(), String> {
        if let Some(reason) = self.inner.busy.lock().unwrap().get(&uri.key) {
            return Err(reason.clone());
        }
        // The commands get no answer, so first make sure it is there at all.
        // A sleeping Crazyflie answers: its nRF51 stays on
        if self.entry(&uri.key).is_none() && self.answers(uri).await.is_none() {
            return Err(format!("{} doesn't answer", uri.key));
        }
        let context = &self.inner.link_context;
        let reason = match command {
            PlatformCommand::Reboot => format!("Rebooted by {who}"),
            PlatformCommand::PowerOffAll => format!("Powered off by {who}"),
            PlatformCommand::PowerOffStm32Domain => format!("Put to sleep by {who}"),
            PlatformCommand::PowerOnStm32Domain => {
                // Its STM32 starts: like after a reboot, it is announced
                // available once its firmware answers
                Crazyflie::power_on_stm32_domain(context, link_uri).await.map_err(|e| e.to_string())?;
                self.inner.asleep.lock().unwrap().remove(&uri.key);
                let answered = self.firmware_answers(uri).await;
                self.announce(Event::Available { uri: uri.key.clone(), connected: false, moved_to: None });
                return match answered {
                    true => Ok(()),
                    false => Err(format!("Woken up, but its firmware didn't answer within {FIRMWARE_START:?}")),
                };
            }
        };
        if command != PlatformCommand::Reboot {
            if command == PlatformCommand::PowerOffStm32Domain {
                // Its clients get it back once it is woken up
                let busy = format!("{} is asleep, put to sleep by {who}", uri.key);
                self.announce(Event::Busy { uri: uri.key.clone(), reason: busy });
            }
            self.take_offline(&uri.key, &reason).await;
            info!("{}: {reason}", uri.key);
            let sent = match command {
                PlatformCommand::PowerOffAll => Crazyflie::power_off_all(context, link_uri).await,
                _ => Crazyflie::power_off_stm32_domain(context, link_uri).await,
            };
            let mut asleep = self.inner.asleep.lock().unwrap();
            match command {
                PlatformCommand::PowerOffStm32Domain if sent.is_ok() => asleep.insert(uri.key.clone()),
                _ => asleep.remove(&uri.key),
            };
            return sent.map_err(|e| e.to_string());
        }

        // Busy until the firmware answers, so that no client connects to a
        // starting firmware and waits out the connect timeout
        let busy = format!("{} is restarting, rebooted by {who}", uri.key);
        self.mark_busy(&uri.key, busy.clone())?;
        self.announce(Event::Busy { uri: uri.key.clone(), reason: busy });
        let was_connected = self.take_offline(&uri.key, &reason).await.is_some();
        info!("{}: {reason}", uri.key);
        // Where its firmware comes back: on another link when its radio
        // config was changed (rechanneling), and then it isn't connected again
        let comes_back = new_uri.unwrap_or(uri);
        let rebooted = match reboot(context, link_uri).await {
            Ok(()) if self.firmware_answers(comes_back).await => Ok(()),
            Ok(()) => Err(format!(
                "Rebooted, but its firmware didn't answer at {} within {FIRMWARE_START:?}",
                comes_back.key
            )),
            Err(e) => Err(e),
        };
        let connected = rebooted.is_ok() && was_connected && new_uri.is_none() && self.reconnect(uri).await;
        let moved_to = new_uri.filter(|_| rebooted.is_ok()).map(|new_uri| new_uri.key.clone());
        self.inner.asleep.lock().unwrap().remove(&uri.key);
        self.unmark_busy(&uri.key);
        self.announce(Event::Available { uri: uri.key.clone(), connected, moved_to });
        rebooted
    }
}

/// Restart the nRF51 and STM32 into the firmware: RESET_INIT, then RESET to
/// the firmware (cfcli's `reboot`)
async fn reboot(context: &crazyflie_lib::crazyflie_link::LinkContext, link_uri: &str) -> Result<(), String> {
    let link = context.open_link(link_uri).await.map_err(|e| e.to_string())?;
    let mut sent = Ok(());
    for data in [vec![0xFF, 0xFE, 0xFF], vec![0xFF, 0xFE, 0xF0, 0x01]] {
        sent = link.send_packet(Packet::from(data)).await.map_err(|e| e.to_string());
        if sent.is_err() {
            break;
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    link.close().await;
    sent
}
