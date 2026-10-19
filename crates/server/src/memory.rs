//! Memories of a Crazyflie attached through a server, named like
//! `crazyflie_lib::subsystems::memory`
//!
//! ``` no_run
//! # async fn example(cf: &crazyflie_connection_server::Crazyflie) -> crazyflie_connection_server::Result<()> {
//! use crazyflie_connection_server::memory::{LighthouseMemory, MemoryType};
//! let device = cf.memory.get_memories(Some(MemoryType::Lighthouse))[0].clone();
//! let lighthouse = cf.memory.open_memory::<LighthouseMemory>(&device).await?;
//! let geometry = lighthouse.read_geometry(0).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Every read and write is a request to the server, which opens and closes
//! the memory around it, so there is nothing to close.

use crate::crazyflie::Handle;
use crate::{Error, Result};
use base64::Engine as _;
use crazyflie_connection_protocol::{EepromConfig, OneWireMemory, Request, Response};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub use crazyflie_connection_protocol::{
    CompressedSegment, CompressedStart, LighthouseBsCalibration, LighthouseBsGeometry, LighthouseCalibrationSweep,
    LocoAnchorData, MemoryInfo, MemoryType, Poly, Poly4D,
};

/// The memories of a Crazyflie
pub struct Memory {
    pub(crate) handle: Arc<Handle>,
    pub(crate) memories: Vec<MemoryInfo>,
}

impl Memory {
    /// The Crazyflie's memories of one type, or all of them, as listed when
    /// the server connected it
    pub fn get_memories(&self, memory_type: Option<MemoryType>) -> Vec<&MemoryInfo> {
        self.memories.iter().filter(|memory| memory_type.is_none_or(|t| memory.memory_type == t)).collect()
    }

    /// Open a memory as a [RawMemory], [OwMemory], [EEPROMConfigMemory],
    /// [LighthouseMemory], [TrajectoryMemory] or [LocoMemory2]. The 1-wire and
    /// EEPROM config memories are read now
    pub async fn open_memory<T: OpenMemory>(&self, memory: &MemoryInfo) -> Result<T> {
        T::open(Device { handle: self.handle.clone(), id: memory.id, size: memory.size }).await
    }
}

/// A memory on the Crazyflie, for [Memory::open_memory]
pub struct Device {
    handle: Arc<Handle>,
    id: u8,
    size: u32,
}

impl Device {
    async fn call(&self, request: impl FnOnce(String, u8) -> Request) -> Result<Response> {
        self.handle.call(request(self.handle.uri.clone(), self.id)).await
    }

    async fn call_done(&self, request: impl FnOnce(String, u8) -> Request) -> Result<()> {
        match self.call(request).await? {
            Response::Done => Ok(()),
            _ => Err(Error::UnexpectedReply),
        }
    }
}

/// The kinds of memory [Memory::open_memory] opens
pub trait OpenMemory: Sized {
    #[doc(hidden)]
    fn open(device: Device) -> impl Future<Output = Result<Self>> + Send;
}

/// Bytes per request of [RawMemory::read_with_progress] and
/// [RawMemory::write_with_progress]
const CHUNK: usize = 1024;

/// Any memory, as bytes
pub struct RawMemory {
    device: Device,
}

impl OpenMemory for RawMemory {
    async fn open(device: Device) -> Result<Self> {
        Ok(Self { device })
    }
}

impl RawMemory {
    pub async fn read(&self, address: usize, length: usize) -> Result<Vec<u8>> {
        let (address, length) = (address as u32, length as u32);
        match self.device.call(|uri, id| Request::MemoryRead { uri, id, address, length }).await? {
            Response::Data { data } => base64::engine::general_purpose::STANDARD
                .decode(data)
                .map_err(|e| Error::Transport(format!("Memory data isn't base64: {e}"))),
            _ => Err(Error::UnexpectedReply),
        }
    }

    pub async fn write(&self, address: usize, data: &[u8]) -> Result<()> {
        let data = base64::engine::general_purpose::STANDARD.encode(data);
        let address = address as u32;
        self.device.call_done(|uri, id| Request::MemoryWrite { uri, id, address, data }).await
    }

    /// [RawMemory::read] in requests of 1 kB, calling `progress` with the
    /// bytes read so far and the total after each
    pub async fn read_with_progress(&self, address: usize, length: usize, mut progress: impl FnMut(usize, usize)) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(length);
        while data.len() < length {
            let chunk = CHUNK.min(length - data.len());
            data.extend(self.read(address + data.len(), chunk).await?);
            progress(data.len(), length);
        }
        Ok(data)
    }

    /// [RawMemory::write] in requests of 1 kB, calling `progress` with the
    /// bytes written so far and the total after each
    pub async fn write_with_progress(&self, address: usize, data: &[u8], mut progress: impl FnMut(usize, usize)) -> Result<()> {
        for (i, chunk) in data.chunks(CHUNK).enumerate() {
            self.write(address + i * CHUNK, chunk).await?;
            progress(i * CHUNK + chunk.len(), data.len());
        }
        Ok(())
    }
}

/// A deck's 1-wire memory, read when opened
pub struct OwMemory {
    memory: OneWireMemory,
}

impl OpenMemory for OwMemory {
    async fn open(device: Device) -> Result<Self> {
        match device.call(|uri, id| Request::OneWireRead { uri, id }).await? {
            Response::OneWire(memory) => Ok(Self { memory }),
            _ => Err(Error::UnexpectedReply),
        }
    }
}

impl OwMemory {
    pub fn used_pins(&self) -> u32 {
        self.memory.used_pins
    }

    pub fn vid(&self) -> u8 {
        self.memory.vid
    }

    pub fn pid(&self) -> u8 {
        self.memory.pid
    }

    /// e.g. `boardName`, `revision`
    pub fn elements(&self) -> &BTreeMap<String, String> {
        &self.memory.elements
    }
}

/// The EEPROM config, read when opened. The setters change this copy;
/// [EEPROMConfigMemory::commit] writes it
pub struct EEPROMConfigMemory {
    device: Device,
    config: EepromConfig,
}

impl OpenMemory for EEPROMConfigMemory {
    async fn open(device: Device) -> Result<Self> {
        match device.call(|uri, id| Request::EepromConfigRead { uri, id }).await? {
            Response::EepromConfig(config) => Ok(Self { device, config }),
            _ => Err(Error::UnexpectedReply),
        }
    }
}

impl EEPROMConfigMemory {
    /// Write the config to the EEPROM. The firmware applies it at boot
    pub async fn commit(&self) -> Result<()> {
        let config = self.config.clone();
        self.device.call_done(|uri, id| Request::EepromConfigWrite { uri, id, config }).await
    }

    pub fn get_radio_channel(&self) -> u8 {
        self.config.radio_channel
    }

    /// Fails for a channel above 125
    pub fn set_radio_channel(&mut self, channel: u8) -> Result<()> {
        if channel > 125 {
            return Err(Error::InvalidArgument(format!("Radio channel {channel} is above 125")));
        }
        self.config.radio_channel = channel;
        Ok(())
    }

    /// 0: 250 kbit/s, 1: 1 Mbit/s, 2: 2 Mbit/s
    pub fn get_radio_speed(&self) -> u8 {
        self.config.radio_speed
    }

    pub fn set_radio_speed(&mut self, speed: u8) {
        self.config.radio_speed = speed;
    }

    pub fn get_pitch_trim(&self) -> f32 {
        self.config.pitch_trim
    }

    pub fn set_pitch_trim(&mut self, trim: f32) {
        self.config.pitch_trim = trim;
    }

    pub fn get_roll_trim(&self) -> f32 {
        self.config.roll_trim
    }

    pub fn set_roll_trim(&mut self, trim: f32) {
        self.config.roll_trim = trim;
    }

    pub fn get_radio_address(&self) -> [u8; 5] {
        let mut address = [0; 5];
        for (i, byte) in address.iter_mut().enumerate() {
            *byte = self.config.radio_address.get(2 * i..2 * i + 2).and_then(|hex| u8::from_str_radix(hex, 16).ok()).unwrap_or(0);
        }
        address
    }

    pub fn set_radio_address(&mut self, address: [u8; 5]) {
        self.config.radio_address = address.iter().map(|b| format!("{b:02X}")).collect();
    }
}

/// The lighthouse memory: each base station's geometry and calibration
pub struct LighthouseMemory {
    device: Device,
}

impl OpenMemory for LighthouseMemory {
    async fn open(device: Device) -> Result<Self> {
        Ok(Self { device })
    }
}

/// Base stations crazyflie-lib's lighthouse memory has room for
pub const MAX_BASE_STATIONS: u8 = 16;

impl LighthouseMemory {
    /// How many base stations the firmware has room for (IDs 0 to N-1), from
    /// the memory's size: the calibrations start at 0x1000, 61 bytes each.
    /// `None` when the size doesn't tell
    pub fn supported_base_stations(&self) -> Option<u8> {
        const CALIB_START: usize = 0x1000;
        const CALIB_SIZE: usize = 61;
        let calibrations = (self.device.size as usize).checked_sub(CALIB_START)?;
        if calibrations == 0 || calibrations % CALIB_SIZE != 0 {
            return None;
        }
        Some((calibrations / CALIB_SIZE).min(MAX_BASE_STATIONS as usize) as u8)
    }

    /// The valid geometries. Without a [LighthouseMemory::supported_base_stations],
    /// all 16 IDs are tried and the ones that fail are skipped
    pub async fn read_all_geometries(&self) -> Result<HashMap<u8, LighthouseBsGeometry>> {
        let mut geometries = HashMap::new();
        for bs in 0..self.supported_base_stations().unwrap_or(MAX_BASE_STATIONS) {
            match self.read_geometry(bs).await {
                Ok(geometry) if geometry.valid => {
                    geometries.insert(bs, geometry);
                }
                Ok(_) => {}
                Err(_) if self.supported_base_stations().is_none() => {}
                Err(e) => return Err(e),
            }
        }
        Ok(geometries)
    }

    /// The valid calibrations, like [LighthouseMemory::read_all_geometries]
    pub async fn read_all_calibrations(&self) -> Result<HashMap<u8, LighthouseBsCalibration>> {
        let mut calibrations = HashMap::new();
        for bs in 0..self.supported_base_stations().unwrap_or(MAX_BASE_STATIONS) {
            match self.read_calibration(bs).await {
                Ok(calibration) if calibration.valid => {
                    calibrations.insert(bs, calibration);
                }
                Ok(_) => {}
                Err(_) if self.supported_base_stations().is_none() => {}
                Err(e) => return Err(e),
            }
        }
        Ok(calibrations)
    }

    pub async fn write_geometries(&self, geometries: &HashMap<u8, LighthouseBsGeometry>) -> Result<()> {
        for (bs, geometry) in geometries {
            self.write_geometry(*bs, geometry).await?;
        }
        Ok(())
    }

    pub async fn write_calibrations(&self, calibrations: &HashMap<u8, LighthouseBsCalibration>) -> Result<()> {
        for (bs, calibration) in calibrations {
            self.write_calibration(*bs, calibration).await?;
        }
        Ok(())
    }

    pub async fn read_geometry(&self, bs: u8) -> Result<LighthouseBsGeometry> {
        match self.device.call(|uri, id| Request::LighthouseGeometryRead { uri, id, bs }).await? {
            Response::LighthouseGeometry(geometry) => Ok(geometry),
            _ => Err(Error::UnexpectedReply),
        }
    }

    pub async fn write_geometry(&self, bs: u8, geometry: &LighthouseBsGeometry) -> Result<()> {
        let geometry = geometry.clone();
        self.device.call_done(|uri, id| Request::LighthouseGeometryWrite { uri, id, bs, geometry }).await
    }

    pub async fn read_calibration(&self, bs: u8) -> Result<LighthouseBsCalibration> {
        match self.device.call(|uri, id| Request::LighthouseCalibrationRead { uri, id, bs }).await? {
            Response::LighthouseCalibration(calibration) => Ok(calibration),
            _ => Err(Error::UnexpectedReply),
        }
    }

    pub async fn write_calibration(&self, bs: u8, calibration: &LighthouseBsCalibration) -> Result<()> {
        let calibration = calibration.clone();
        self.device.call_done(|uri, id| Request::LighthouseCalibrationWrite { uri, id, bs, calibration }).await
    }
}

/// The trajectory memory of the high-level commander
pub struct TrajectoryMemory {
    device: Device,
}

impl OpenMemory for TrajectoryMemory {
    async fn open(device: Device) -> Result<Self> {
        Ok(Self { device })
    }
}

/// Bytes an uncompressed segment takes in the trajectory memory: 33 floats
pub const POLY4D_BYTES: usize = 132;

/// Uncompressed segments per request of
/// [TrajectoryMemory::write_uncompressed_with_progress], about 1 kB
const SEGMENTS_PER_WRITE: usize = 7;

impl TrajectoryMemory {
    /// Write uncompressed segments at `start_addr`. Returns the bytes written
    pub async fn write_uncompressed(&self, segments: &[Poly4D], start_addr: usize) -> Result<usize> {
        let (segments, start_addr) = (segments.to_vec(), start_addr as u32);
        let reply = self
            .device
            .call(|uri, id| Request::TrajectoryWriteUncompressed { uri, id, start_addr, segments })
            .await?;
        written(reply)
    }

    /// [TrajectoryMemory::write_uncompressed] in requests of about 1 kB,
    /// calling `progress` with the bytes written so far and the total after each
    pub async fn write_uncompressed_with_progress(
        &self,
        segments: &[Poly4D],
        start_addr: usize,
        mut progress: impl FnMut(usize, usize),
    ) -> Result<usize> {
        let total = segments.len() * POLY4D_BYTES;
        let mut done = 0;
        for batch in segments.chunks(SEGMENTS_PER_WRITE) {
            done += self.write_uncompressed(batch, start_addr + done).await?;
            progress(done, total);
        }
        Ok(done)
    }

    /// Write a compressed trajectory, `start` and then `segments`, at
    /// `start_addr`. Returns the bytes written
    pub async fn write_compressed(
        &self,
        start: &CompressedStart,
        segments: &[CompressedSegment],
        start_addr: usize,
    ) -> Result<usize> {
        let (start, segments, start_addr) = (*start, segments.to_vec(), start_addr as u32);
        let reply = self
            .device
            .call(|uri, id| Request::TrajectoryWriteCompressed { uri, id, start_addr, start, segments })
            .await?;
        written(reply)
    }
}

fn written(reply: Response) -> Result<usize> {
    match reply {
        Response::Written { bytes } => Ok(bytes as usize),
        _ => Err(Error::UnexpectedReply),
    }
}

/// The Loco Positioning memory: the anchors and their positions
pub struct LocoMemory2 {
    device: Device,
}

/// Every anchor in the Loco memory, see [LocoMemory2::read_all]
#[derive(Debug, Clone)]
pub struct LocoSystemData {
    pub anchor_ids: Vec<u8>,
    pub active_anchor_ids: Vec<u8>,
    pub anchors: HashMap<u8, LocoAnchorData>,
}

impl OpenMemory for LocoMemory2 {
    async fn open(device: Device) -> Result<Self> {
        Ok(Self { device })
    }
}

impl LocoMemory2 {
    /// The ids of the configured anchors
    pub async fn read_id_list(&self) -> Result<Vec<u8>> {
        self.ids(false).await
    }

    /// The ids of the anchors the Crazyflie hears now
    pub async fn read_active_id_list(&self) -> Result<Vec<u8>> {
        self.ids(true).await
    }

    async fn ids(&self, active: bool) -> Result<Vec<u8>> {
        match self.device.call(|uri, id| Request::LocoReadIdList { uri, id, active }).await? {
            Response::Ids { ids } => Ok(ids),
            _ => Err(Error::UnexpectedReply),
        }
    }

    pub async fn read_anchor_data(&self, anchor: u8) -> Result<LocoAnchorData> {
        match self.device.call(|uri, id| Request::LocoReadAnchor { uri, id, anchor }).await? {
            Response::LocoAnchor(data) => Ok(data),
            _ => Err(Error::UnexpectedReply),
        }
    }

    /// The id lists and every configured anchor's data
    pub async fn read_all(&self) -> Result<LocoSystemData> {
        let anchor_ids = self.read_id_list().await?;
        let active_anchor_ids = self.read_active_id_list().await?;
        let mut anchors = HashMap::new();
        for id in &anchor_ids {
            anchors.insert(*id, self.read_anchor_data(*id).await?);
        }
        Ok(LocoSystemData { anchor_ids, active_anchor_ids, anchors })
    }
}
