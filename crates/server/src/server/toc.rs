//! TOC caching for the server's connections: one download per firmware
//!
//! Crazyflies running the same firmware have the same param and log TOCs, so
//! downloading them from more than one Crazyflie is wasted radio time. A
//! connection first tries with only the cached TOCs ([ProbeCache]); one that
//! misses some claims them in [TocDownloads] and downloads them, the others
//! missing the same ones wait for it and connect from the cache.

use super::TocStore;
use crazyflie_lib::TocCache;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::watch;

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
        Self { store, memory: Arc::default() }
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

/// A TOC cache that never makes the Crazyflie download a TOC. A miss is
/// noted and answered with an empty TOC, so the connection finishes right
/// away (an empty param TOC also skips reading all the values); the caller
/// then disconnects and connects again to download it, or once it is cached
#[derive(Clone)]
pub(super) struct ProbeCache {
    inner: ServerTocCache,
    missed: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl ProbeCache {
    pub(super) fn new(inner: ServerTocCache) -> Self {
        Self { inner, missed: Arc::default() }
    }

    /// The keys that weren't in the cache
    pub(super) fn missed(&self) -> Vec<Vec<u8>> {
        self.missed.lock().unwrap().clone()
    }
}

impl TocCache for ProbeCache {
    fn get_toc(&self, key: &[u8]) -> Option<String> {
        TocCache::get_toc(&self.inner, key).or_else(|| {
            self.missed.lock().unwrap().push(key.to_vec());
            Some("{}".to_owned())
        })
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
        let waits: Vec<watch::Receiver<()>> =
            keys.iter().filter_map(|key| downloads.get(key).map(watch::Sender::subscribe)).collect();
        if waits.len() == keys.len() {
            return Err(Waiting(waits));
        }
        let mine: Vec<Vec<u8>> = keys.iter().filter(|key| !downloads.contains_key(*key)).cloned().collect();
        for key in &mine {
            downloads.insert(key.clone(), watch::Sender::new(()));
        }
        Ok(Downloading { downloads: self.clone(), keys: mine })
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
    use super::{ProbeCache, ServerTocCache, TocDownloads};
    use crazyflie_lib::TocCache;

    #[test]
    fn probe_answers_a_miss_with_an_empty_toc() {
        let cache = ServerTocCache::new(None);
        cache.store_toc(&[1, 0xaa], r#"{"a":[1,8]}"#);
        let probe = ProbeCache::new(cache);
        assert_eq!(probe.get_toc(&[1, 0xaa]).as_deref(), Some(r#"{"a":[1,8]}"#));
        assert!(probe.missed().is_empty());
        assert_eq!(probe.get_toc(&[1, 2]).as_deref(), Some("{}"));
        assert_eq!(probe.missed(), vec![vec![1, 2]]);
    }

    #[tokio::test]
    async fn one_download_per_toc() {
        let downloads = TocDownloads::default();
        let keys = vec![vec![1], vec![2]];
        let Ok(first) = downloads.claim(&keys) else { panic!("the first connection downloads") };
        let Err(second) = downloads.claim(&keys) else { panic!("the second waits") };
        // Missing one more TOC than is being downloaded: download, that one only
        let Ok(third) = downloads.claim(&[vec![1], vec![3]]) else { panic!("a new TOC is downloaded") };
        assert_eq!(third.keys, vec![vec![3]]);

        let waiting = tokio::spawn(second.finished());
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting).await.unwrap().unwrap();
        assert!(downloads.claim(&keys).is_ok());
    }
}
