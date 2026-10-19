//! Memory access for clients: the memory list, raw reads and writes, and the
//! typed memories tools use (1-wire, EEPROM config, lighthouse)
//!
//! crazyflie-lib hands a memory out with `open_memory` and only takes it back
//! with `close_memory`, so every request opens, uses and closes one in turn
//! with the Crazyflie's other memory requests. A memory a tool in this
//! process holds through `Crazyflie::lib()` is reported as in use.

use super::CfEntry;
use base64::Engine as _;
use crazyflie_connection_protocol as protocol;
use crazyflie_connection_protocol::{MemoryInfo, Request, Response};
use crazyflie_lib::Crazyflie;
use crazyflie_lib::subsystems::memory::{
    self as mem, EEPROMConfigMemory, FromMemoryBackend, LighthouseMemory, MemoryDevice, OwMemory, RadioSpeed,
    RawMemory,
};

/// The memory list of a Crazyflie, for its attach info
pub(super) fn memories(cf: &Crazyflie) -> Vec<MemoryInfo> {
    cf.memory
        .get_memories(None)
        .into_iter()
        .map(|device| MemoryInfo {
            id: device.memory_id,
            memory_type: memory_type(&device.memory_type),
            size: device.size,
            serial: device.serial.as_ref().map(|serial| serial.iter().map(|b| format!("{b:02X}")).collect()),
        })
        .collect()
}

fn memory_type(memory_type: &mem::MemoryType) -> protocol::MemoryType {
    use protocol::MemoryType as P;
    match memory_type {
        mem::MemoryType::EEPROMConfig => P::EEPROMConfig,
        mem::MemoryType::OneWire => P::OneWire,
        mem::MemoryType::DriverLed => P::DriverLed,
        mem::MemoryType::Loco => P::Loco,
        mem::MemoryType::Trajectory => P::Trajectory,
        mem::MemoryType::Loco2 => P::Loco2,
        mem::MemoryType::Lighthouse => P::Lighthouse,
        mem::MemoryType::MemoryTester => P::MemoryTester,
        mem::MemoryType::MicroSD => P::MicroSD,
        mem::MemoryType::DriverLedTiming => P::DriverLedTiming,
        mem::MemoryType::App => P::App,
        mem::MemoryType::DeckMemory => P::DeckMemory,
        mem::MemoryType::DeckCtrlDFU => P::DeckCtrlDFU,
        mem::MemoryType::DeckCtrl => P::DeckCtrl,
        mem::MemoryType::DeckMultiranger => P::DeckMultiranger,
        mem::MemoryType::DeckPaa3905 => P::DeckPaa3905,
        _ => P::UNKNOWN,
    }
}

/// The Crazyflie a memory request is for, `None` for other requests
pub(super) fn uri_of(request: &Request) -> Option<&str> {
    match request {
        Request::MemoryRead { uri, .. }
        | Request::MemoryWrite { uri, .. }
        | Request::OneWireRead { uri, .. }
        | Request::EepromConfigRead { uri, .. }
        | Request::EepromConfigWrite { uri, .. }
        | Request::LighthouseGeometryRead { uri, .. }
        | Request::LighthouseGeometryWrite { uri, .. }
        | Request::LighthouseCalibrationRead { uri, .. }
        | Request::LighthouseCalibrationWrite { uri, .. } => Some(uri),
        _ => None,
    }
}

/// Handle a memory request on an attached Crazyflie
pub(super) async fn handle(entry: &CfEntry, request: Request) -> Result<Response, String> {
    let _turn = entry.memory_turn.lock().await;
    let cf = &entry.cf;
    match request {
        Request::MemoryRead { id, address, length, .. } => {
            let data = with::<RawMemory, _>(cf, id, async |raw| raw.read(address as usize, length as usize).await).await?;
            Ok(Response::Data { data: base64::engine::general_purpose::STANDARD.encode(data) })
        }
        Request::MemoryWrite { id, address, data, .. } => {
            let data = base64::engine::general_purpose::STANDARD.decode(&data).map_err(|e| format!("The data isn't base64: {e}"))?;
            with::<RawMemory, _>(cf, id, async |raw| raw.write(address as usize, &data).await).await?;
            Ok(Response::Done)
        }
        Request::OneWireRead { id, .. } => {
            let ow = with::<OwMemory, _>(cf, id, async |ow| {
                Ok(protocol::OneWireMemory {
                    used_pins: ow.used_pins(),
                    vid: ow.vid(),
                    pid: ow.pid(),
                    elements: ow.elements().iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                })
            })
            .await?;
            Ok(Response::OneWire(ow))
        }
        Request::EepromConfigRead { id, .. } => {
            let config = with::<EEPROMConfigMemory, _>(cf, id, async |eeprom| {
                Ok(protocol::EepromConfig {
                    radio_channel: eeprom.get_radio_channel(),
                    radio_speed: eeprom.get_radio_speed().clone() as u8,
                    radio_address: eeprom.get_radio_address().iter().map(|b| format!("{b:02X}")).collect(),
                    pitch_trim: eeprom.get_pitch_trim(),
                    roll_trim: eeprom.get_roll_trim(),
                })
            })
            .await?;
            Ok(Response::EepromConfig(config))
        }
        Request::EepromConfigWrite { id, config, .. } => {
            let address = super::parse_address(&config.radio_address)?;
            let speed = RadioSpeed::try_from(config.radio_speed).map_err(|e| e.to_string())?;
            let mut eeprom = open::<EEPROMConfigMemory>(cf, id).await?;
            let written = async {
                eeprom.set_radio_channel(config.radio_channel)?;
                eeprom.set_radio_speed(speed);
                eeprom.set_radio_address(address);
                eeprom.set_pitch_trim(config.pitch_trim);
                eeprom.set_roll_trim(config.roll_trim);
                eeprom.commit().await
            }
            .await;
            let _ = cf.memory.close_memory(eeprom).await;
            written.map_err(|e| e.to_string())?;
            Ok(Response::Done)
        }
        Request::LighthouseGeometryRead { id, bs, .. } => {
            let geometry = with::<LighthouseMemory, _>(cf, id, async |lh| lh.read_geometry(bs).await).await?;
            Ok(Response::LighthouseGeometry(protocol::LighthouseBsGeometry {
                origin: geometry.origin,
                rotation_matrix: geometry.rotation_matrix,
                valid: geometry.valid,
            }))
        }
        Request::LighthouseGeometryWrite { id, bs, geometry, .. } => {
            let geometry = mem::LighthouseBsGeometry {
                origin: geometry.origin,
                rotation_matrix: geometry.rotation_matrix,
                valid: geometry.valid,
            };
            with::<LighthouseMemory, _>(cf, id, async |lh| lh.write_geometry(bs, &geometry).await).await?;
            Ok(Response::Done)
        }
        Request::LighthouseCalibrationRead { id, bs, .. } => {
            let calibration = with::<LighthouseMemory, _>(cf, id, async |lh| lh.read_calibration(bs).await).await?;
            Ok(Response::LighthouseCalibration(protocol::LighthouseBsCalibration {
                sweeps: calibration.sweeps.map(|sweep| protocol::LighthouseCalibrationSweep {
                    phase: sweep.phase,
                    tilt: sweep.tilt,
                    curve: sweep.curve,
                    gibmag: sweep.gibmag,
                    gibphase: sweep.gibphase,
                    ogeemag: sweep.ogeemag,
                    ogeephase: sweep.ogeephase,
                }),
                uid: calibration.uid,
                valid: calibration.valid,
            }))
        }
        Request::LighthouseCalibrationWrite { id, bs, calibration, .. } => {
            let calibration = mem::LighthouseBsCalibration {
                sweeps: calibration.sweeps.map(|sweep| mem::LighthouseCalibrationSweep {
                    phase: sweep.phase,
                    tilt: sweep.tilt,
                    curve: sweep.curve,
                    gibmag: sweep.gibmag,
                    gibphase: sweep.gibphase,
                    ogeemag: sweep.ogeemag,
                    ogeephase: sweep.ogeephase,
                }),
                uid: calibration.uid,
                valid: calibration.valid,
            };
            with::<LighthouseMemory, _>(cf, id, async |lh| lh.write_calibration(bs, &calibration).await).await?;
            Ok(Response::Done)
        }
        _ => Err("Not a memory request".to_owned()),
    }
}

/// Open memory `id` as a `T`, run `op` on it and close it again
async fn with<T: FromMemoryBackend, R>(
    cf: &Crazyflie,
    id: u8,
    op: impl AsyncFnOnce(&T) -> crazyflie_lib::Result<R>,
) -> Result<R, String> {
    let memory = open::<T>(cf, id).await?;
    let result = op(&memory).await;
    let _ = cf.memory.close_memory(memory).await;
    result.map_err(|e| e.to_string())
}

/// Open memory `id` as a `T`. crazyflie-lib keeps a memory that fails to open
/// as a `T` (e.g. a 1-wire memory with a bad CRC) until the Crazyflie is
/// connected again
async fn open<T: FromMemoryBackend>(cf: &Crazyflie, id: u8) -> Result<T, String> {
    let device: MemoryDevice = cf
        .memory
        .get_memories(None)
        .into_iter()
        .find(|device| device.memory_id == id)
        .cloned()
        .ok_or_else(|| format!("No memory {id}"))?;
    match cf.memory.open_memory::<T>(device).await {
        Some(Ok(memory)) => Ok(memory),
        Some(Err(e)) => Err(format!("Memory {id}: {e}")),
        None => Err(format!("Memory {id} is in use")),
    }
}
