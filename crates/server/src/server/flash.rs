//! Flashing as a job: the Crazyflie is restarted into its bootloader and
//! flashed with cfloader through a Crazyradio the server shares with its other
//! connections. Bootloader packets carry their own channel and address, so
//! they interleave with the other links' packets.

use super::Server;
use cfloader::{Bllink, CFLoader};
use crazyflie_connection_protocol::{Event, FlashStart, NormalizedUri, ServerMessage};
use crazyflie_lib::crazyflie_link::{Connection, LinkContext, Packet};
use std::time::Duration;
use tokio::sync::mpsc;

/// nRF51 bootloader commands, as cfcli and the Python lib send them
const TARGET_NRF51: u8 = 0xFE;

/// A firmware image to flash
pub(crate) struct Image {
    pub target: Target,
    pub start: Option<FlashStart>,
    pub data: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) enum Target {
    Stm32,
    Nrf51,
}

impl Target {
    pub(crate) fn parse(name: &str) -> Result<Self, String> {
        match name {
            "stm32" => Ok(Self::Stm32),
            "nrf51" => Ok(Self::Nrf51),
            other => Err(format!("Unknown flash target '{other}' (stm32 or nrf51)")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Stm32 => "stm32",
            Self::Nrf51 => "nrf51",
        }
    }
}

/// Sends a job's progress to the client that started it
#[derive(Clone)]
struct Reporter {
    tx: mpsc::UnboundedSender<ServerMessage>,
    sub: u64,
}

impl Reporter {
    fn send(&self, event: Event) {
        let _ = self.tx.send(ServerMessage::Event { sub: self.sub, event });
    }

    fn progress(&self, step: &str, done: usize, total: usize) {
        self.send(Event::Progress { step: step.to_owned(), done: done as u64, total: total as u64 });
    }
}

impl Server {
    /// Start flashing `images` to the Crazyflie at `uri`, reporting on stream
    /// `sub` of `tx`. Fails right away if it is being flashed already or isn't
    /// reached by radio
    pub(crate) fn start_flash(
        &self,
        uri: NormalizedUri,
        who: &str,
        images: Vec<Image>,
        tx: mpsc::UnboundedSender<ServerMessage>,
        sub: u64,
    ) -> Result<(), String> {
        let Some(path) = uri.key.strip_prefix("radio:///").map(str::to_owned) else {
            return Err(format!("{} can't be flashed: only Crazyflies reached by radio can", uri.key));
        };
        let reason = format!("{} is being flashed by {who}", uri.key);
        self.mark_busy(&uri.key, reason.clone())?;
        self.announce(Event::Busy { uri: uri.key.clone(), reason });
        let server = self.clone();
        let reporter = Reporter { tx, sub };
        tokio::spawn(async move {
            let result = server.flash(&uri, &path, &images, &reporter).await;
            // Still busy while reconnecting, so that no client attach starts a second connection
            let connected = result.is_ok() && server.reconnect(&uri).await;
            server.unmark_busy(&uri.key);
            server.announce(Event::Available { uri: uri.key.clone(), connected });
            reporter.send(match result {
                Ok(()) => Event::Done,
                Err(reason) => Event::Ended { reason },
            });
        });
        Ok(())
    }

    async fn flash(&self, uri: &NormalizedUri, path: &str, images: &[Image], reporter: &Reporter) -> Result<(), String> {
        let was_on = self.take_offline(&uri.key, "Being flashed").await;
        let radio = self.pick_radio(was_on.or(uri.radio)).await?;
        let result = self.flash_on(radio, path, images, reporter).await;
        self.release_radio(Some(radio));
        result
    }

    async fn flash_on(&self, radio: usize, path: &str, images: &[Image], reporter: &Reporter) -> Result<(), String> {
        reporter.progress("Restarting into the bootloader", 0, 1);
        let context = &self.inner.link_context;
        let address = enter_bootloader(context, &format!("radio://{radio}/{path}?safelink=0"))
            .await
            .map_err(|e| format!("Restarting into the bootloader: {e}"))?;
        let shared_radio = context.get_radio(radio).await.map_err(|e| e.to_string())?;
        let bllink = Bllink::new_with_radio(shared_radio, Some(&address))
            .await
            .map_err(|e| format!("Opening the bootloader link: {e}"))?;
        let mut loader = CFLoader::new(bllink).await.map_err(|e| format!("Reading the bootloader info: {e}"))?;

        for image in images {
            let info = match image.target {
                Target::Stm32 => loader.stm32_info(),
                Target::Nrf51 => loader.nrf51_info(),
            };
            let page_size = info.page_size() as u32;
            let start = match image.start {
                Some(FlashStart::Address(address)) => address,
                Some(FlashStart::Page(page)) => page as u32 * page_size,
                None => info.flash_start() as u32 * page_size,
            };
            let step = format!("Flashing {}", image.target.name());
            let reporter = reporter.clone();
            let callback = move |done: usize, total: usize| reporter.progress(&step, done, total);
            match image.target {
                Target::Stm32 => loader.flash_stm32_with_progress(start, &image.data, Some(callback)).await,
                Target::Nrf51 => loader.flash_nrf51_with_progress(start, &image.data, Some(callback)).await,
            }
            .map_err(|e| format!("Flashing the {}: {e}", image.target.name()))?;
        }

        reporter.progress("Restarting into the firmware", 0, 1);
        loader.reset_to_firmware().await.map_err(|e| format!("Restarting into the firmware: {e}"))
    }
}

/// Restart a Crazyflie that runs its firmware into its bootloader, over a
/// link without safelink, and return the address the bootloader listens on
/// (on channel 0). The same sequence as cfcli's `enter_bootloader_for_cfloader`
async fn enter_bootloader(context: &LinkContext, link_uri: &str) -> Result<[u8; 5], String> {
    let link = context.open_link(link_uri).await.map_err(|e| e.to_string())?;
    let address = reset_and_get_bootloader_address(&link).await;
    link.close().await;
    address
}

async fn reset_and_get_bootloader_address(link: &Connection) -> Result<[u8; 5], String> {
    // Safelink off, so that the nRF51 takes bootloader commands
    send(link, vec![0xFF, TARGET_NRF51, 0xFF, 0x05, 0x00]).await?;

    // RESET_INIT, again every 200 ms for up to 2 s: the answer holds the four
    // address bytes, little endian, after 0xB1
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let address = 'address: loop {
        if tokio::time::Instant::now() >= deadline {
            return Err("the nRF51 did not answer RESET_INIT within 2 s".to_owned());
        }
        send(link, vec![0xFF, TARGET_NRF51, 0xFF]).await?;
        let resend = (tokio::time::Instant::now() + Duration::from_millis(200)).min(deadline);
        while let Ok(packet) = tokio::time::timeout_at(resend, link.recv_packet()).await {
            let packet = packet.map_err(|e| e.to_string())?;
            let data = packet.get_data();
            if data.len() > 5 && data[0..2] == [TARGET_NRF51, 0xFF] {
                break 'address [0xB1, data[5], data[4], data[3], data[2]];
            }
        }
    };

    // RESET to the bootloader
    for _ in 0..10 {
        send(link, vec![0xFF, TARGET_NRF51, 0xF0, 0x00]).await?;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(address)
}

async fn send(link: &Connection, data: Vec<u8>) -> Result<(), String> {
    let packet: Packet = data.into();
    link.send_packet(packet).await.map_err(|e| e.to_string())
}
