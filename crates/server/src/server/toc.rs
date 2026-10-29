//! TOC caching for the server's connections: one download per firmware
//!
//! Crazyflies running the same firmware have the same param and log TOCs, so
//! downloading them from more than one Crazyflie is wasted radio time. A
//! connection first tries with only the cached TOCs ([ProbeCache]); one that
//! misses some claims them in [TocDownloads] and downloads them, the others
//! missing the same ones wait for it and connect from the cache.

use super::TocStore;
use crazyflie_lib::TocCache;
use log::warn;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::watch;

/// TOCs in a directory, one `<key>.json` file each, so that they outlive
/// the server (see [ServerConfig::with_toc_cache](super::ServerConfig::with_toc_cache)).
/// The same layout as Swarmkeeper's TOC cache
#[derive(Clone)]
pub struct FileTocCache {
    dir: PathBuf,
}

/// Distinguishes the temporary files of writes running at the same time
static WRITES: AtomicU64 = AtomicU64::new(0);

impl FileTocCache {
    /// A cache in `dir`, which is created if needed
    pub fn new(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    fn path(&self, key: &[u8]) -> PathBuf {
        let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        self.dir.join(format!("{hex}.json"))
    }
}

impl TocCache for FileTocCache {
    fn get_toc(&self, key: &[u8]) -> Option<String> {
        std::fs::read_to_string(self.path(key)).ok()
    }

    fn store_toc(&self, key: &[u8], toc: &str) {
        // Written to a temporary file and renamed, so that a reader never
        // sees half a TOC
        let path = self.path(key);
        let n = WRITES.fetch_add(1, Ordering::Relaxed);
        let temporary = path.with_extension(format!("json.{}-{n}.tmp", std::process::id()));
        let stored =
            std::fs::write(&temporary, toc).and_then(|()| std::fs::rename(&temporary, &path));
        if let Err(e) = stored {
            let _ = std::fs::remove_file(&temporary);
            warn!("Couldn't store a TOC in {}: {e}", path.display());
        }
    }
}

/// The server's TOC cache: the configured store, if any, and the TOCs seen
/// since the server started, so that a store that doesn't keep them (a tool's
/// "no cache" option) still gets each firmware's TOCs downloaded once
#[derive(Clone)]
pub(super) struct ServerTocCache {
    store: Option<Arc<dyn TocStore>>,
    memory: Arc<RwLock<HashMap<Vec<u8>, String>>>,
}

impl ServerTocCache {
    pub(super) fn new(store: Option<Arc<dyn TocStore>>) -> Self {
        Self {
            store,
            memory: Arc::default(),
        }
    }
}

impl TocCache for ServerTocCache {
    fn get_toc(&self, key: &[u8]) -> Option<String> {
        self.store
            .as_ref()
            .and_then(|store| store.get_toc(key))
            .or_else(|| self.memory.read().ok()?.get(key).cloned())
    }

    fn store_toc(&self, key: &[u8], toc: &str) {
        if let Ok(mut memory) = self.memory.write() {
            memory.insert(key.to_vec(), toc.to_owned());
        }
        if let Some(store) = &self.store {
            store.store_toc(key, toc);
        }
    }
}

/// The TOC cache of one connection. A TOC that isn't cached is claimed in
/// [TocDownloads] and downloaded, unless another connection is downloading
/// it: that miss is noted and answered with an empty TOC, so the connection
/// finishes right away (an empty param TOC also skips reading all the
/// values), and the caller connects again once the other download is over
#[derive(Clone)]
pub(super) struct ProbeCache {
    inner: ServerTocCache,
    downloads: TocDownloads,
    claimed: Arc<Mutex<Vec<Downloading>>>,
    missed: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl ProbeCache {
    pub(super) fn new(inner: ServerTocCache, downloads: TocDownloads) -> Self {
        Self {
            inner,
            downloads,
            claimed: Arc::default(),
            missed: Arc::default(),
        }
    }

    /// End the downloads of this connection, which wakes the connections
    /// waiting for them, and return the TOCs other connections were
    /// downloading
    pub(super) fn finish(&self) -> Vec<Vec<u8>> {
        self.claimed.lock().unwrap().clear();
        self.missed.lock().unwrap().clone()
    }
}

impl TocCache for ProbeCache {
    fn get_toc(&self, key: &[u8]) -> Option<String> {
        if let Some(toc) = TocCache::get_toc(&self.inner, key) {
            return Some(toc);
        }
        match self.downloads.claim(&[key.to_vec()]) {
            Ok(download) => {
                self.claimed.lock().unwrap().push(download);
                None
            }
            Err(_) => {
                self.missed.lock().unwrap().push(key.to_vec());
                Some("{}".to_owned())
            }
        }
    }

    fn store_toc(&self, key: &[u8], toc: &str) {
        TocCache::store_toc(&self.inner, key, toc);
    }
}

/// The TOCs being downloaded, by cache key. The entry's sender is dropped
/// when the download is over, which wakes the connections waiting for it
#[derive(Clone, Default)]
pub(super) struct TocDownloads(Arc<Mutex<HashMap<Vec<u8>, watch::Sender<()>>>>);

/// A claim on TOCs to download. It ends when this is dropped
pub(super) struct Downloading {
    downloads: TocDownloads,
    keys: Vec<Vec<u8>>,
}

/// TOCs other connections are downloading
pub(super) struct Waiting(Vec<watch::Receiver<()>>);

impl TocDownloads {
    /// Claim the TOCs `keys` for a download, unless other connections are
    /// downloading all of them already
    pub(super) fn claim(&self, keys: &[Vec<u8>]) -> Result<Downloading, Waiting> {
        let mut downloads = self.0.lock().unwrap();
        let waits: Vec<watch::Receiver<()>> = keys
            .iter()
            .filter_map(|key| downloads.get(key).map(watch::Sender::subscribe))
            .collect();
        if waits.len() == keys.len() {
            return Err(Waiting(waits));
        }
        let mine: Vec<Vec<u8>> = keys
            .iter()
            .filter(|key| !downloads.contains_key(*key))
            .cloned()
            .collect();
        for key in &mine {
            downloads.insert(key.clone(), watch::Sender::new(()));
        }
        Ok(Downloading {
            downloads: self.clone(),
            keys: mine,
        })
    }

    /// The downloads of `keys` that other connections are running now
    pub(super) fn running(&self, keys: &[Vec<u8>]) -> Waiting {
        let downloads = self.0.lock().unwrap();
        Waiting(
            keys.iter()
                .filter_map(|key| downloads.get(key).map(watch::Sender::subscribe))
                .collect(),
        )
    }
}

impl Drop for Downloading {
    fn drop(&mut self) {
        let mut downloads = self.downloads.0.lock().unwrap();
        for key in &self.keys {
            downloads.remove(key);
        }
    }
}

impl Waiting {
    /// Completes when every download waited for is over, done or failed
    pub(super) async fn finished(self) {
        for mut download in self.0 {
            // Fails once the sender is gone, which is the only thing that happens
            let _ = download.changed().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FileTocCache, ProbeCache, ServerTocCache, TocDownloads};
    use crazyflie_lib::TocCache;

    #[test]
    fn file_cache_keeps_tocs() {
        let dir = std::env::temp_dir().join(format!("ccs-toc-test-{}", std::process::id()));
        let cache = FileTocCache::new(&dir).unwrap();
        assert_eq!(cache.get_toc(&[0x01, 0xab]), None);
        cache.store_toc(&[0x01, 0xab], r#"{"a":[1,8]}"#);
        // Another instance on the same directory, as after a restart
        let again = FileTocCache::new(&dir).unwrap();
        assert_eq!(
            again.get_toc(&[0x01, 0xab]).as_deref(),
            Some(r#"{"a":[1,8]}"#)
        );
        assert!(dir.join("01ab.json").exists());
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "no temporary file left"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_miss_is_downloaded_by_the_first_connection_only() {
        let cache = ServerTocCache::new(None);
        cache.store_toc(&[1, 0xaa], r#"{"a":[1,8]}"#);
        let downloads = TocDownloads::default();
        let first = ProbeCache::new(cache.clone(), downloads.clone());
        assert_eq!(first.get_toc(&[1, 0xaa]).as_deref(), Some(r#"{"a":[1,8]}"#));
        // Not cached: this connection downloads it
        assert_eq!(first.get_toc(&[1, 2]), None);

        // Another connection missing it meanwhile gets an empty TOC, and waits
        let second = ProbeCache::new(cache.clone(), downloads.clone());
        assert_eq!(second.get_toc(&[1, 2]).as_deref(), Some("{}"));
        let missed = second.finish();
        assert_eq!(missed, vec![vec![1, 2]]);
        let waiting = tokio::spawn(downloads.running(&missed).finished());
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        // crazyflie-lib stores what the first one downloaded
        first.store_toc(&[1, 2], r#"{"b":[2,8]}"#);
        assert!(first.finish().is_empty());
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        let third = ProbeCache::new(cache, downloads);
        assert_eq!(third.get_toc(&[1, 2]).as_deref(), Some(r#"{"b":[2,8]}"#));
    }

    #[tokio::test]
    async fn one_download_per_toc() {
        let downloads = TocDownloads::default();
        let keys = vec![vec![1], vec![2]];
        let Ok(first) = downloads.claim(&keys) else {
            panic!("the first connection downloads")
        };
        let Err(second) = downloads.claim(&keys) else {
            panic!("the second waits")
        };
        // Missing one more TOC than is being downloaded: download, that one only
        let Ok(third) = downloads.claim(&[vec![1], vec![3]]) else {
            panic!("a new TOC is downloaded")
        };
        assert_eq!(third.keys, vec![vec![3]]);

        let waiting = tokio::spawn(second.finished());
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert!(downloads.claim(&keys).is_ok());
    }
}
