//! Flashing as a job: the Crazyflies are restarted into their bootloaders and
//! flashed with cfloader through a Crazyradio the server shares with its other
//! connections. Bootloader packets carry their own channel and address, so
//! they interleave with the other links' packets.
//!
//! The Crazyflies of a job get the same images, so they are of one platform.
//! Those whose bootloaders take broadcast and have the same flash layout
//! receive each image once, all at the same time. Each one is then checked by
//! CRC, and the pages that didn't make it are flashed again over unicast. The
//! others are flashed one after another. The same engine as cfcli's `swarm
//! bootload flash` (`src/modules/swarm/broadcast.rs`).
//!
//! In their bootloaders all Crazyflies listen on channel 0, so that part runs
//! on one Crazyradio; only the restarts into the bootloader are spread over
//! the Crazyradios the Crazyflies were on.

use super::Server;
use cfloader::bootloader::{TARGET_NRF51, TARGET_STM32};
use cfloader::packets::InfoPacket;
use cfloader::{BROADCAST_ADDRESS, Bllink, CFLoader};
use crazyflie_connection_protocol::{Event, FlashStart, NormalizedUri, ServerMessage};
use crazyflie_lib::crazyflie_link::{Connection, LinkContext, Packet, SharedCrazyradio};
use futures::future::join_all;
use log::warn;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};

/// Crazyflies restarted into their bootloaders at the same time on each
/// Crazyradio. A Crazyflie that is restarting leaves the Crazyradio retrying,
/// so more at once slow each other down (cfcli's `BOOTLOADER_PER_RADIO`)
const BOOTLOADER_PER_RADIO: usize = 2;

/// The first bootloader protocol with broadcast flashing, on both chips
const BROADCAST_PROTOCOL: u8 = 0x11;

/// Fewest Crazyflies a broadcast is used for: it waits for the slowest
/// possible flash erase after every chunk, so for one it is slower than unicast
const MIN_BROADCAST: usize = 2;

/// Rounds of CRC check and re-flash of the pages that differ
const FIX_ATTEMPTS: usize = 3;

/// Attempts at getting the Crazyradio for the bootloaders: the links that
/// restarted the Crazyflies only just let go of it
const RADIO_ATTEMPTS: usize = 10;
const RADIO_RETRY: Duration = Duration::from_millis(300);

/// Attempts at restarting a Crazyflie from its bootloader into its firmware,
/// and scans to see whether the bootloader is gone after each
const RESTART_ATTEMPTS: usize = 3;
const BOOTLOADER_PROBES: usize = 3;

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

    /// cfloader's target
    fn id(self) -> u8 {
        match self {
            Self::Stm32 => TARGET_STM32,
            Self::Nrf51 => TARGET_NRF51,
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
        let _ = self.tx.send(ServerMessage::Event {
            sub: self.sub,
            event,
        });
    }

    fn progress(&self, uri: Option<&str>, step: &str, done: usize, total: usize) {
        self.send(Event::Progress {
            uri: uri.map(str::to_owned),
            step: step.to_owned(),
            done: done as u64,
            total: total as u64,
        });
    }
}

/// What a job did to one Crazyflie
struct Outcome {
    /// How it was flashed, or why not
    result: Result<String, String>,
    /// Connect it again after the job: it was restarted into its firmware, or
    /// the job didn't reach it but it was connected before
    reconnect: bool,
}

impl Outcome {
    fn restarted(result: Result<String, String>) -> Self {
        Self {
            result,
            reconnect: true,
        }
    }
}

/// A Crazyflie sitting in its bootloader
struct Session {
    /// Its index in the job
    index: usize,
    key: String,
    /// The Crazyradio it is reached with
    radio: usize,
    loader: CFLoader,
}

impl Session {
    /// Where its bootloader answers
    fn bootloader_uri(&mut self) -> String {
        let address: String = self
            .loader
            .bllink_mut()
            .address()
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect();
        format!(
            "radio://{}/0/2M/{address}?safelink=0&ackfilter=0",
            self.radio
        )
    }

    /// It failed and stays in its bootloader, saying where that is
    fn fail(&mut self, reason: String) -> Outcome {
        let uri = self.bootloader_uri();
        let address = uri.split('?').next().unwrap_or_default();
        Outcome {
            result: Err(format!("{reason}; left in its bootloader at {address}")),
            reconnect: false,
        }
    }

    /// Restart it into its firmware with `result`, or fail if it doesn't
    /// restart. Restarted ones are noted in `restarted`, to check that they
    /// left their bootloader
    async fn restart(
        &mut self,
        result: Result<String, String>,
        restarted: &mut Vec<(usize, String)>,
    ) -> Outcome {
        match self.loader.reset_to_firmware().await {
            Ok(()) => {
                restarted.push((self.index, self.bootloader_uri()));
                Outcome::restarted(result)
            }
            Err(e) => self.fail(format!("Restarting into the firmware: {e:#}")),
        }
    }

    fn info(&self, target: Target) -> &InfoPacket {
        match target {
            Target::Stm32 => self.loader.stm32_info(),
            Target::Nrf51 => self.loader.nrf51_info(),
        }
    }

    /// Both bootloaders take broadcast: the nRF51 receives on the broadcast
    /// address and forwards to the STM32
    fn takes_broadcast(&self) -> bool {
        self.has_crc(Target::Stm32) && self.has_crc(Target::Nrf51)
    }

    /// The bootloader of the chip has the CRC commands
    fn has_crc(&self, target: Target) -> bool {
        self.info(target).version() >= BROADCAST_PROTOCOL
    }

    fn start(&self, image: &Image) -> u32 {
        let info = self.info(image.target);
        let page_size = info.page_size() as u32;
        match image.start {
            Some(FlashStart::Address(address)) => address,
            Some(FlashStart::Page(page)) => page as u32 * page_size,
            None => info.flash_start() as u32 * page_size,
        }
    }

    /// An image for the start of the firmware area that is bigger than it
    fn too_big<'i>(&self, images: &'i [Image]) -> Option<&'i Image> {
        images.iter().find(|image| {
            let info = self.info(image.target);
            let capacity = info.n_flash_page().saturating_sub(info.flash_start()) as usize
                * info.page_size() as usize;
            image.start.is_none() && image.data.len() > capacity
        })
    }

    /// What has to be the same for a broadcast chunked for one Crazyflie to
    /// land in the right place on another
    fn layout(&self) -> [u16; 6] {
        let (nrf51, stm32) = (self.loader.nrf51_info(), self.loader.stm32_info());
        [
            nrf51.page_size(),
            nrf51.n_buff_page(),
            nrf51.flash_start(),
            stm32.page_size(),
            stm32.n_buff_page(),
            stm32.flash_start(),
        ]
    }
}

impl Server {
    /// Start flashing `images` to the Crazyflies at `uris`, reporting on stream
    /// `sub` of `tx`. Fails right away if one of them is being flashed already
    /// or isn't reached by radio
    pub(crate) fn start_flash(
        &self,
        uris: Vec<NormalizedUri>,
        who: &str,
        images: Vec<Image>,
        tx: mpsc::UnboundedSender<ServerMessage>,
        sub: u64,
    ) -> Result<(), String> {
        if uris.is_empty() {
            return Err("No Crazyflie to flash".to_owned());
        }
        for (n, uri) in uris.iter().enumerate() {
            if !uri.key.starts_with("radio:///") {
                return Err(format!(
                    "{} can't be flashed: only Crazyflies reached by radio can",
                    uri.key
                ));
            }
            if uris[..n].iter().any(|other| other.key == uri.key) {
                return Err(format!("{} is listed twice", uri.key));
            }
        }
        let reasons: Vec<String> = uris
            .iter()
            .map(|uri| format!("{} is being flashed by {who}", uri.key))
            .collect();
        for (n, (uri, reason)) in uris.iter().zip(&reasons).enumerate() {
            if let Err(e) = self.mark_busy(&uri.key, reason.clone()) {
                for marked in &uris[..n] {
                    self.unmark_busy(&marked.key);
                }
                return Err(e);
            }
        }
        for (uri, reason) in uris.iter().zip(reasons) {
            self.announce(Event::Busy {
                uri: uri.key.clone(),
                reason,
            });
        }
        let server = self.clone();
        let reporter = Reporter { tx, sub };
        tokio::spawn(async move {
            let outcomes = server.flash(&uris, &images, &reporter).await;
            // Each one is given back as soon as it is connected again. Still
            // busy while reconnecting, so that no client attach starts a second connection
            join_all(uris.into_iter().zip(outcomes).map(|(uri, outcome)| {
                let (server, reporter) = (&server, &reporter);
                async move {
                    let connected = outcome.reconnect && server.reconnect(&uri).await;
                    server.unmark_busy(&uri.key);
                    server.announce(Event::Available {
                        uri: uri.key.clone(),
                        connected,
                        moved_to: None,
                    });
                    reporter.send(match outcome.result {
                        Ok(how) => Event::Flashed { uri: uri.key, how },
                        Err(reason) => Event::FlashFailed {
                            uri: uri.key,
                            reason,
                        },
                    });
                }
            }))
            .await;
            reporter.send(Event::Done);
        });
        Ok(())
    }

    /// Flash every Crazyflie of a job. Returns an outcome for each, in order
    async fn flash(
        &self,
        uris: &[NormalizedUri],
        images: &[Image],
        reporter: &Reporter,
    ) -> Vec<Outcome> {
        let context = &self.inner.link_context;
        let mut outcomes: Vec<Option<Outcome>> = uris.iter().map(|_| None).collect();

        // Off their links, onto the Crazyradio each was on, else the pinned or least loaded one
        let mut radios = Vec::new();
        let mut was_connected = Vec::new();
        for uri in uris {
            let was_on = self.take_offline(&uri.key, "Being flashed").await;
            was_connected.push(was_on.is_some());
            radios.push(
                self.pick_radio(
                    was_on.flatten().or(uri.radio),
                    uri.channel().unwrap_or_default(),
                )
                .await,
            );
        }

        // Into the bootloader, a few Crazyflies per Crazyradio at a time
        let turns: HashMap<usize, Arc<Semaphore>> = radios
            .iter()
            .flatten()
            .map(|slot| (slot.radio, Arc::new(Semaphore::new(BOOTLOADER_PER_RADIO))))
            .collect();
        let entered = join_all(uris.iter().zip(&radios).map(|(uri, slot)| {
            let turns = &turns;
            async move {
                let radio = slot.as_ref().map_err(Clone::clone)?.radio;
                let _turn = turns[&radio].acquire().await.map_err(|e| e.to_string())?;
                reporter.progress(Some(&uri.key), "Restarting into the bootloader", 0, 1);
                let path = uri.key.trim_start_matches("radio:///");
                let address =
                    enter_bootloader(context, &format!("radio://{radio}/{path}?safelink=0"))
                        .await?;
                Ok::<_, String>((radio, address))
            }
        }))
        .await;
        for slot in radios.into_iter().flatten() {
            self.release_radio(Some(slot));
        }
        let mut in_bootloader = Vec::new();
        for (index, entered) in entered.into_iter().enumerate() {
            match entered {
                Ok((radio, address)) => in_bootloader.push((index, radio, address)),
                Err(e) => {
                    let result = Err(format!("Restarting into the bootloader: {e}"));
                    outcomes[index] = Some(Outcome {
                        result,
                        reconnect: was_connected[index],
                    });
                }
            }
        }

        // Then all on the Crazyradio of the first, on channel 0
        let mut restarted = Vec::new();
        if let Some(&(_, radio, _)) = in_bootloader.first() {
            // The bootloaders are on channel 0
            let slot = self.pick_radio(Some(radio), 0).await.ok();
            match bootloader_radio(context, radio).await {
                Ok(shared) => {
                    let sessions =
                        open_sessions(shared, radio, uris, in_bootloader, reporter, &mut outcomes)
                            .await;
                    self.flash_sessions(sessions, images, reporter, &mut outcomes, &mut restarted)
                        .await;
                }
                Err(e) => {
                    for (index, _, _) in in_bootloader {
                        let reason = format!(
                            "In the bootloader, but the Crazyradio couldn't be opened: {e}"
                        );
                        outcomes[index] = Some(Outcome {
                            result: Err(reason),
                            reconnect: false,
                        });
                    }
                }
            }
            self.release_radio(slot);
        }

        // Every Crazyflie sent back into its firmware has to have left its bootloader
        if !restarted.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        for (index, bootloader_uri) in restarted {
            reporter.progress(
                Some(&uris[index].key),
                "Checking that it left the bootloader",
                0,
                1,
            );
            if !left_bootloader(context, &bootloader_uri).await {
                let address = bootloader_uri.split('?').next().unwrap_or_default();
                let reason = format!(
                    "It is still in its bootloader, at {address}. Restart it with its power button"
                );
                outcomes[index] = Some(Outcome {
                    result: Err(reason),
                    reconnect: false,
                });
            }
        }

        outcomes
            .into_iter()
            .map(|outcome| outcome.expect("an outcome for every Crazyflie"))
            .collect()
    }

    /// Flash the Crazyflies in their bootloaders: by broadcast where they take
    /// it, one after another otherwise
    async fn flash_sessions(
        &self,
        sessions: Vec<Session>,
        images: &[Image],
        reporter: &Reporter,
        outcomes: &mut [Option<Outcome>],
        restarted: &mut Vec<(usize, String)>,
    ) {
        let mut group: Vec<Session> = Vec::new();
        let mut unicast = Vec::new();
        for mut session in sessions {
            if let Some(image) = session.too_big(images) {
                let error = format!(
                    "The {} image doesn't fit in the flash ({} bytes)",
                    image.target.name(),
                    image.data.len()
                );
                outcomes[session.index] = Some(session.restart(Err(error), restarted).await);
            } else if session.takes_broadcast()
                && group
                    .first()
                    .is_none_or(|first| first.layout() == session.layout())
            {
                group.push(session);
            } else {
                unicast.push(session);
            }
        }
        if group.len() >= MIN_BROADCAST {
            unicast.extend(
                self.broadcast(group, images, reporter, outcomes, restarted)
                    .await,
            );
        } else {
            unicast.append(&mut group);
        }

        unicast.sort_by_key(|session| session.index);
        for mut session in unicast {
            outcomes[session.index] =
                Some(match flash_unicast(&mut session, images, reporter).await {
                    Ok(how) => session.restart(Ok(how), restarted).await,
                    Err(reason) => session.fail(reason),
                });
        }
    }

    /// Broadcast each image to the group, then check every Crazyflie in it by
    /// CRC, flash what is missing again and restart it into its firmware.
    /// Returns the Crazyflies that couldn't be armed, to be flashed over unicast
    async fn broadcast(
        &self,
        group: Vec<Session>,
        images: &[Image],
        reporter: &Reporter,
        outcomes: &mut [Option<Outcome>],
        restarted: &mut Vec<(usize, String)>,
    ) -> Vec<Session> {
        // A Crazyflie that fails stays in its bootloader, armed, so every
        // broadcast gets its own address: it then can't receive a later one,
        // which may be for another platform
        let mut address = BROADCAST_ADDRESS;
        address[4] = address[4].wrapping_sub(self.inner.broadcasts.fetch_add(1, Ordering::Relaxed));

        let mut armed = Vec::new();
        let mut rest = Vec::new();
        for mut session in group {
            reporter.progress(Some(&session.key), "Arming for broadcast", 0, 1);
            match session.loader.set_broadcast_address(&address).await {
                Ok(()) => armed.push(session),
                Err(e) => {
                    warn!(
                        "{}: couldn't arm it for broadcast, flashing it over unicast: {e:#}",
                        session.key
                    );
                    rest.push(session);
                }
            }
        }
        if armed.len() < MIN_BROADCAST {
            rest.append(&mut armed);
            return rest;
        }

        for image in images {
            let start = armed[0].start(image);
            let step = format!(
                "Broadcasting {} to {} Crazyflies",
                image.target.name(),
                armed.len()
            );
            let progress = reporter.clone();
            let callback =
                move |done: usize, total: usize| progress.progress(None, &step, done, total);
            let sent = armed[0]
                .loader
                .broadcast_flash_image_with_progress(
                    image.target.id(),
                    start,
                    &image.data,
                    Some(callback),
                )
                .await;
            if let Err(e) = sent {
                let reason = format!("Broadcasting the {} image: {e:#}", image.target.name());
                for mut session in armed {
                    outcomes[session.index] = Some(session.fail(reason.clone()));
                }
                return rest;
            }
        }

        for mut session in armed {
            reporter.progress(Some(&session.key), "Checking", 0, 1);
            let mut fixed = 0;
            let mut checked = Ok(());
            for image in images {
                let start = session.start(image);
                match verify_and_fix(&mut session.loader, image, start).await {
                    Ok(pages) => fixed += pages,
                    Err(e) => {
                        checked = Err(e);
                        break;
                    }
                }
            }
            outcomes[session.index] = Some(match checked {
                Ok(()) => {
                    session
                        .restart(Ok(how("broadcast", fixed)), restarted)
                        .await
                }
                Err(reason) => session.fail(reason),
            });
        }
        rest
    }
}

/// "broadcast", "broadcast, 2 pages flashed again"
fn how(way: &str, fixed: usize) -> String {
    match fixed {
        0 => way.to_owned(),
        1 => format!("{way}, 1 page flashed again"),
        n => format!("{way}, {n} pages flashed again"),
    }
}

/// The Crazyradio, shared with the server's links
async fn bootloader_radio(context: &LinkContext, radio: usize) -> Result<SharedCrazyradio, String> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match context.get_radio(radio).await {
            Ok(shared) => return Ok(shared),
            Err(e) if attempt >= RADIO_ATTEMPTS => return Err(format!("Crazyradio {radio}: {e}")),
            Err(_) => tokio::time::sleep(RADIO_RETRY).await,
        }
    }
}

/// Read the bootloader info of every Crazyflie that entered its bootloader.
/// The ones whose bootloader doesn't answer are left in it
async fn open_sessions(
    shared: SharedCrazyradio,
    radio: usize,
    uris: &[NormalizedUri],
    in_bootloader: Vec<(usize, usize, [u8; 5])>,
    reporter: &Reporter,
    outcomes: &mut [Option<Outcome>],
) -> Vec<Session> {
    let mut sessions = Vec::new();
    for (index, _, address) in in_bootloader {
        let key = uris[index].key.clone();
        reporter.progress(Some(&key), "Reading the bootloader", 0, 1);
        let opened = async {
            let bllink = Bllink::new_with_radio(shared.clone(), Some(&address)).await?;
            CFLoader::new(bllink).await
        }
        .await;
        match opened {
            Ok(loader) => sessions.push(Session {
                index,
                key,
                radio,
                loader,
            }),
            Err(e) => {
                let address: String = address.iter().map(|b| format!("{b:02X}")).collect();
                let reason = format!(
                    "Its bootloader didn't answer ({e:#}); left in it at radio://{radio}/0/2M/{address}"
                );
                outcomes[index] = Some(Outcome {
                    result: Err(reason),
                    reconnect: false,
                });
            }
        }
    }
    sessions
}

/// Flash the images to one Crazyflie over unicast, and check them by CRC
/// where the bootloader can
async fn flash_unicast(
    session: &mut Session,
    images: &[Image],
    reporter: &Reporter,
) -> Result<String, String> {
    let mut fixed = 0;
    for image in images {
        let start = session.start(image);
        let (progress, key) = (reporter.clone(), session.key.clone());
        let step = format!("Flashing {}", image.target.name());
        let callback =
            move |done: usize, total: usize| progress.progress(Some(&key), &step, done, total);
        session
            .loader
            .flash_image_with_progress(image.target.id(), start, &image.data, Some(callback))
            .await
            .map_err(|e| format!("Flashing the {}: {e:#}", image.target.name()))?;
        if session.has_crc(image.target) {
            reporter.progress(Some(&session.key), "Checking", 0, 1);
            fixed += verify_and_fix(&mut session.loader, image, start).await?;
        }
    }
    Ok(how("unicast", fixed))
}

/// Compare an image with the flash by CRC and flash the pages that differ
/// again, up to [FIX_ATTEMPTS] times. Returns how many pages were flashed again
async fn verify_and_fix(loader: &mut CFLoader, image: &Image, start: u32) -> Result<usize, String> {
    let (target, name) = (image.target.id(), image.target.name());
    let mut failed = loader
        .verify_flash_crc(target, start, &image.data)
        .await
        .map_err(|e| format!("Checking the {name}: {e:#}"))?;
    let mut fixed = 0;
    for _ in 0..FIX_ATTEMPTS {
        if failed.is_empty() {
            return Ok(fixed);
        }
        loader
            .fix_failed_pages(target, start, &image.data, &failed)
            .await
            .map_err(|e| format!("Flashing {} pages of the {name} again: {e:#}", failed.len()))?;
        fixed += failed.len();
        failed = loader
            .verify_flash_crc(target, start, &image.data)
            .await
            .map_err(|e| format!("Checking the {name}: {e:#}"))?;
    }
    if !failed.is_empty() {
        return Err(format!(
            "{} pages of the {name} still differ after flashing them again {FIX_ATTEMPTS} times",
            failed.len()
        ));
    }
    Ok(fixed)
}

/// Restart a Crazyflie that runs its firmware into its bootloader, over a
/// link without safelink, and return the address the bootloader listens on
/// (on channel 0). The same sequence as cfcli's `enter_bootloader_for_cfloader`
async fn enter_bootloader(context: &LinkContext, link_uri: &str) -> Result<[u8; 5], String> {
    let link = context
        .open_link(link_uri)
        .await
        .map_err(|e| format!("no answer ({e})"))?;
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
            let packet = packet.map_err(|e| format!("no answer ({e})"))?;
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

/// Make sure a Crazyflie that was sent from its bootloader into its firmware
/// has left the bootloader at `bootloader_uri`, and restart it again if not.
/// Returns whether the bootloader is gone. cfcli's `left_bootloader`
async fn left_bootloader(context: &LinkContext, bootloader_uri: &str) -> bool {
    if !bootloader_answers(context, bootloader_uri).await {
        return true;
    }
    for _ in 0..RESTART_ATTEMPTS {
        // Whether this worked shows from the bootloader being gone
        if let Ok(link) = context.open_link(bootloader_uri).await {
            let _ = send(&link, vec![0xFF, TARGET_NRF51, 0xFF]).await;
            let _ = send(&link, vec![0xFF, TARGET_NRF51, 0xF0, 0x01]).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            link.close().await;
        }
        if !bootloader_answers(context, bootloader_uri).await {
            return true;
        }
    }
    false
}

async fn bootloader_answers(context: &LinkContext, bootloader_uri: &str) -> bool {
    for _ in 0..BOOTLOADER_PROBES {
        match context.scan_selected(vec![bootloader_uri]).await {
            Ok(found) if found.is_empty() => continue,
            _ => return true,
        }
    }
    false
}

async fn send(link: &Connection, data: Vec<u8>) -> Result<(), String> {
    let packet: Packet = data.into();
    link.send_packet(packet)
        .await
        .map_err(|e| format!("no answer ({e})"))
}
