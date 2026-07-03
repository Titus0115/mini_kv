use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use crate::error::Result;
use crate::storage::Storage;
use crate::wal::{
    read_all_records, write_records, WalRecord, DEFAULT_COMPACTION_THRESHOLD, WAL_FILE,
};

const MANIFEST_FILE: &str = "manifest.json";

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Manifest {
    version: u64,
}

pub struct WalEngine {
    dir: PathBuf,
    memtable: RwLock<BTreeMap<Vec<u8>, Entry>>,
    wal_size: AtomicU64,
    compaction_threshold: u64,
}

#[derive(Clone)]
struct Entry {
    value: Option<Vec<u8>>, // None = deleted
    expire_at: u64,         // 0 = never
}

impl Entry {
    fn is_expired(&self) -> bool {
        if self.expire_at == 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expire_at
    }
}

impl WalEngine {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_threshold(dir, DEFAULT_COMPACTION_THRESHOLD)
    }

    pub fn open_with_threshold(dir: impl AsRef<Path>, threshold: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let wal_path = dir.join(WAL_FILE);
        let manifest_path = dir.join(MANIFEST_FILE);

        // load or create manifest
        if !manifest_path.exists() {
            let m = Manifest { version: 1 };
            let f = fs::File::create(&manifest_path)?;
            serde_json::to_writer(f, &m)?;
        }

        // replay WAL
        let mut memtable: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();
        let mut wal_size: u64 = 0;

        if wal_path.exists() {
            let data = fs::read(&wal_path)?;
            wal_size = data.len() as u64;
            let records = read_all_records(&data)?;
            for rec in records {
                apply_record(&mut memtable, rec);
            }
        }

        Ok(Self {
            dir,
            memtable: RwLock::new(memtable),
            wal_size: AtomicU64::new(wal_size),
            compaction_threshold: threshold,
        })
    }

    fn append_wal(&self, record: &WalRecord) -> Result<()> {
        let data = record.encode();
        let wal_path = self.dir.join(WAL_FILE);
        let mut f = fs::OpenOptions::new().append(true).create(true).open(&wal_path)?;
        f.write_all(&data)?;
        f.flush()?;
        self.wal_size.fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn maybe_compact(&self) -> Result<()> {
        if self.wal_size.load(Ordering::Relaxed) < self.compaction_threshold {
            return Ok(());
        }
        self.do_compact()
    }

    pub fn do_compact(&self) -> Result<()> {
        let memtable = self.memtable.read();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut compacted = Vec::new();
        for (key, entry) in memtable.iter() {
            // skip deleted
            if entry.value.is_none() {
                continue;
            }
            // skip expired
            if entry.expire_at != 0 && now >= entry.expire_at {
                continue;
            }
            let rec = if entry.expire_at != 0 {
                WalRecord::put_with_ttl(key.clone(), entry.value.clone().unwrap(), entry.expire_at)
            } else {
                WalRecord::put(key.clone(), entry.value.clone().unwrap())
            };
            compacted.push(rec);
        }
        drop(memtable);

        // write compacted data to temp file, then rename
        let wal_path = self.dir.join(WAL_FILE);
        let tmp_path = self.dir.join("wal.log.tmp");

        {
            let mut f = fs::File::create(&tmp_path)?;
            write_records(&mut f, &compacted)?;
            f.flush()?;
        }

        fs::rename(&tmp_path, &wal_path)?;

        let new_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        self.wal_size.store(new_size, Ordering::Relaxed);

        // also purge expired/deleted entries from memtable
        let mut memtable = self.memtable.write();
        memtable.retain(|_, entry| {
            entry.value.is_some() && !entry.is_expired()
        });

        Ok(())
    }
}

fn apply_record(memtable: &mut BTreeMap<Vec<u8>, Entry>, rec: WalRecord) {
    match rec.record_type {
        crate::wal::RecordType::Put => {
            memtable.insert(
                rec.key,
                Entry {
                    value: Some(rec.value),
                    expire_at: 0,
                },
            );
        }
        crate::wal::RecordType::Ttl => {
            memtable.insert(
                rec.key,
                Entry {
                    value: Some(rec.value),
                    expire_at: rec.expire_at,
                },
            );
        }
        crate::wal::RecordType::Delete => {
            memtable.remove(&rec.key);
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl Storage for WalEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let table = self.memtable.read();
        match table.get(key) {
            Some(entry) if !entry.is_expired() => Ok(entry.value.clone()),
            _ => Ok(None),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let rec = WalRecord::put(key.to_vec(), value.to_vec());
        self.append_wal(&rec)?;
        let mut table = self.memtable.write();
        table.insert(
            key.to_vec(),
            Entry {
                value: Some(value.to_vec()),
                expire_at: 0,
            },
        );
        drop(table);
        self.maybe_compact()?;
        Ok(())
    }

    fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: Duration) -> Result<()> {
        let expire_at = now_secs() + ttl.as_secs();
        let rec = WalRecord::put_with_ttl(key.to_vec(), value.to_vec(), expire_at);
        self.append_wal(&rec)?;
        let mut table = self.memtable.write();
        table.insert(
            key.to_vec(),
            Entry {
                value: Some(value.to_vec()),
                expire_at,
            },
        );
        drop(table);
        self.maybe_compact()?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let rec = WalRecord::delete(key.to_vec());
        self.append_wal(&rec)?;
        let mut table = self.memtable.write();
        table.remove(key);
        drop(table);
        self.maybe_compact()?;
        Ok(())
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table = self.memtable.read();
        let mut result = Vec::new();
        for (k, entry) in table.range(start.to_vec()..end.to_vec()) {
            if !entry.is_expired() {
                if let Some(v) = &entry.value {
                    result.push((k.clone(), v.clone()));
                }
            }
        }
        Ok(result)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table = self.memtable.read();
        let mut result = Vec::new();
        for (k, entry) in table.iter() {
            if k.starts_with(prefix) && !entry.is_expired() {
                if let Some(v) = &entry.value {
                    result.push((k.clone(), v.clone()));
                }
            }
        }
        Ok(result)
    }

    fn flush(&self) -> Result<()> {
        let wal_path = self.dir.join(WAL_FILE);
        let f = fs::OpenOptions::new().append(true).open(&wal_path)?;
        f.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_put_get_delete() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine.put(b"hello", b"world").unwrap();
        assert_eq!(engine.get(b"hello").unwrap(), Some(b"world".to_vec()));

        engine.delete(b"hello").unwrap();
        assert_eq!(engine.get(b"hello").unwrap(), None);
    }

    #[test]
    fn test_ttl() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine
            .put_with_ttl(b"temp", b"data", Duration::from_secs(1))
            .unwrap();
        assert_eq!(engine.get(b"temp").unwrap(), Some(b"data".to_vec()));

        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(engine.get(b"temp").unwrap(), None);
    }

    #[test]
    fn test_scan() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine.put(b"k1", b"v1").unwrap();
        engine.put(b"k2", b"v2").unwrap();
        engine.put(b"k3", b"v3").unwrap();

        let result = engine.scan(b"k1", b"k3").unwrap();
        assert_eq!(result.len(), 2); // k1, k2
        assert_eq!(result[0].0, b"k1");
        assert_eq!(result[1].0, b"k2");
    }

    #[test]
    fn test_scan_prefix() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine.put(b"user:1", b"alice").unwrap();
        engine.put(b"user:2", b"bob").unwrap();
        engine.put(b"item:1", b"widget").unwrap();

        let result = engine.scan_prefix(b"user:").unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_recovery() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        {
            let engine = WalEngine::open(&path).unwrap();
            engine.put(b"a", b"1").unwrap();
            engine.put(b"b", b"2").unwrap();
        }

        let engine = WalEngine::open(&path).unwrap();
        assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(engine.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_compaction() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open_with_threshold(dir.path(), 100).unwrap();

        // write enough data to trigger compaction
        for i in 0..50u32 {
            let key = format!("key_{i}").into_bytes();
            let val = vec![0u8; 100];
            engine.put(&key, &val).unwrap();
        }
        // overwrite some keys
        for i in 0..10u32 {
            let key = format!("key_{i}").into_bytes();
            engine.put(&key, b"overwritten").unwrap();
        }
        // delete some keys
        for i in 10..20u32 {
            let key = format!("key_{i}").into_bytes();
            engine.delete(&key).unwrap();
        }

        engine.do_compact().unwrap();

        // verify data integrity after compaction
        assert_eq!(engine.get(b"key_0").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(engine.get(b"key_10").unwrap(), None);
        assert_eq!(engine.get(b"key_30").unwrap().unwrap().len(), 100);
    }
}
