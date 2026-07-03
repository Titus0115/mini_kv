use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};

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
    wal_file: Mutex<fs::File>,
    wal_size: AtomicU64,
    compaction_threshold: u64,
    cleanup_count: AtomicU64,
}

#[derive(Clone)]
struct Entry {
    value: Option<Vec<u8>>,
    expire_at: u64, // 0 = never; Unix millisecond timestamp otherwise
}

impl Entry {
    fn is_expired(&self) -> bool {
        if self.expire_at == 0 {
            return false;
        }
        now_ms() >= self.expire_at
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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

        if !manifest_path.exists() {
            let m = Manifest { version: 1 };
            let f = fs::File::create(&manifest_path)?;
            serde_json::to_writer(f, &m)?;
        }

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

        let wal_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&wal_path)?;

        Ok(Self {
            dir,
            memtable: RwLock::new(memtable),
            wal_file: Mutex::new(wal_file),
            wal_size: AtomicU64::new(wal_size),
            compaction_threshold: threshold,
            cleanup_count: AtomicU64::new(0),
        })
    }

    fn append_wal(&self, record: &WalRecord) -> Result<()> {
        let data = record.encode();
        let mut f = self.wal_file.lock();
        f.write_all(&data)?;
        f.flush()?;
        f.sync_all()?;
        self.wal_size
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn reopen_wal_file(&self) -> Result<()> {
        let wal_path = self.dir.join(WAL_FILE);
        let new_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&wal_path)?;
        let mut f = self.wal_file.lock();
        *f = new_file;
        Ok(())
    }

    pub fn maybe_compact(&self) -> Result<()> {
        if self.wal_size.load(Ordering::Relaxed) < self.compaction_threshold {
            return Ok(());
        }
        self.do_compact()
    }

    pub fn do_compact(&self) -> Result<()> {
        let now = now_ms();
        let memtable = self.memtable.read();

        let mut compacted = Vec::new();
        for (key, entry) in memtable.iter() {
            if entry.value.is_none() {
                continue;
            }
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

        let wal_path = self.dir.join(WAL_FILE);
        let tmp_path = self.dir.join("wal.log.tmp");

        {
            let mut f = fs::File::create(&tmp_path)?;
            write_records(&mut f, &compacted)?;
            f.flush()?;
            f.sync_all()?;
        }

        fs::rename(&tmp_path, &wal_path)?;

        let new_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        self.wal_size.store(new_size, Ordering::Relaxed);

        self.reopen_wal_file()?;

        let mut memtable = self.memtable.write();
        memtable.retain(|_, entry| entry.value.is_some() && !entry.is_expired());

        Ok(())
    }

    pub fn purge_expired(&self) -> usize {
        let mut memtable = self.memtable.write();
        let before = memtable.len();
        memtable.retain(|_, entry| !entry.is_expired());
        let purged = before - memtable.len();
        self.cleanup_count
            .fetch_add(purged as u64, Ordering::Relaxed);
        purged
    }

    /// 返回当前 key 总数（不含已过期）
    pub fn key_count(&self) -> usize {
        let table = self.memtable.read();
        table.iter().filter(|(_, e)| !e.is_expired()).count()
    }

    /// 返回当前带 TTL 的 key 数量（不含已过期）
    pub fn ttl_key_count(&self) -> usize {
        let table = self.memtable.read();
        table
            .iter()
            .filter(|(_, e)| e.expire_at != 0 && !e.is_expired())
            .count()
    }

    /// 返回 WAL 文件大小（字节）
    pub fn storage_bytes(&self) -> u64 {
        let wal_path = self.dir.join(WAL_FILE);
        std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0)
    }

    /// 返回 janitor 累计清理的过期 key 数量
    pub fn expired_cleanup_count(&self) -> u64 {
        self.cleanup_count.load(Ordering::Relaxed)
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
        // TTL=0 表示永不过期
        let expire_at = if ttl.as_millis() == 0 {
            0
        } else {
            now_ms() + ttl.as_millis() as u64
        };
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
        // 如果 start >= end，返回空结果（避免 BTreeMap range panic）
        if start >= end {
            return Ok(Vec::new());
        }
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
        let f = self.wal_file.lock();
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
    fn test_ttl_millis() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine
            .put_with_ttl(b"temp", b"data", Duration::from_millis(500))
            .unwrap();
        assert_eq!(engine.get(b"temp").unwrap(), Some(b"data".to_vec()));

        std::thread::sleep(Duration::from_millis(800));
        assert_eq!(engine.get(b"temp").unwrap(), None);
    }

    #[test]
    fn test_ttl_secs() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine
            .put_with_ttl(b"temp2", b"data2", Duration::from_secs(1))
            .unwrap();
        assert_eq!(engine.get(b"temp2").unwrap(), Some(b"data2".to_vec()));

        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(engine.get(b"temp2").unwrap(), None);
    }

    #[test]
    fn test_scan() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine.put(b"k1", b"v1").unwrap();
        engine.put(b"k2", b"v2").unwrap();
        engine.put(b"k3", b"v3").unwrap();

        let result = engine.scan(b"k1", b"k3").unwrap();
        assert_eq!(result.len(), 2);
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
    fn test_ttl_recovery() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        {
            let engine = WalEngine::open(&path).unwrap();
            engine.put(b"perm", b"keeps").unwrap();
            engine
                .put_with_ttl(b"expiring", b"gone", Duration::from_secs(1))
                .unwrap();
        }

        std::thread::sleep(Duration::from_secs(2));

        let engine = WalEngine::open(&path).unwrap();
        assert_eq!(engine.get(b"perm").unwrap(), Some(b"keeps".to_vec()));
        assert_eq!(engine.get(b"expiring").unwrap(), None);
    }

    #[test]
    fn test_compaction() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open_with_threshold(dir.path(), 100).unwrap();

        for i in 0..50u32 {
            let key = format!("key_{i}").into_bytes();
            let val = vec![0u8; 100];
            engine.put(&key, &val).unwrap();
        }
        for i in 0..10u32 {
            let key = format!("key_{i}").into_bytes();
            engine.put(&key, b"overwritten").unwrap();
        }
        for i in 10..20u32 {
            let key = format!("key_{i}").into_bytes();
            engine.delete(&key).unwrap();
        }

        engine.do_compact().unwrap();

        assert_eq!(engine.get(b"key_0").unwrap(), Some(b"overwritten".to_vec()));
        assert_eq!(engine.get(b"key_10").unwrap(), None);
        assert_eq!(engine.get(b"key_30").unwrap().unwrap().len(), 100);
    }

    #[test]
    fn test_purge_expired() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine.put(b"stay", b"here").unwrap();
        engine
            .put_with_ttl(b"go", b"away", Duration::from_millis(100))
            .unwrap();

        std::thread::sleep(Duration::from_millis(200));
        let purged = engine.purge_expired();
        assert_eq!(purged, 1);
        assert_eq!(engine.get(b"stay").unwrap(), Some(b"here".to_vec()));
        assert_eq!(engine.get(b"go").unwrap(), None);
    }

    #[test]
    fn test_empty_key() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine.put(b"", b"empty_key_value").unwrap();
        assert_eq!(engine.get(b"").unwrap(), Some(b"empty_key_value".to_vec()));

        engine.delete(b"").unwrap();
        assert_eq!(engine.get(b"").unwrap(), None);
    }

    #[test]
    fn test_delete_nonexistent() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine.delete(b"no_such_key").unwrap();
        assert_eq!(engine.get(b"no_such_key").unwrap(), None);
    }

    #[test]
    fn test_overwrite_key() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        engine.put(b"k", b"v1").unwrap();
        assert_eq!(engine.get(b"k").unwrap(), Some(b"v1".to_vec()));

        engine.put(b"k", b"v2").unwrap();
        assert_eq!(engine.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_scan_empty_result() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();

        let result = engine.scan(b"a", b"z").unwrap();
        assert!(result.is_empty());

        let result = engine.scan_prefix(b"nonexistent:").unwrap();
        assert!(result.is_empty());
    }

    /// 并发读测试：10 个线程同时 get 同一个 key，不 panic
    #[test]
    fn test_concurrent_reads() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        engine.put(b"key", b"value").unwrap();

        let engine = std::sync::Arc::new(engine);
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let e = engine.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        let v = e.get(b"key").unwrap();
                        assert_eq!(v, Some(b"value".to_vec()));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }

    /// 并发写测试：10 个线程同时 put 不同 key，不 panic、不丢数据
    #[test]
    fn test_concurrent_writes() {
        let dir = TempDir::new().unwrap();
        let engine = std::sync::Arc::new(WalEngine::open(dir.path()).unwrap());

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let e = engine.clone();
                std::thread::spawn(move || {
                    let key = format!("key_{}", i);
                    e.put(key.as_bytes(), b"value").unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(engine.key_count(), 10);
    }

    /// 并发读写测试：5 个线程读 + 5 个线程写，不 panic
    #[test]
    fn test_concurrent_read_write() {
        let dir = TempDir::new().unwrap();
        let engine = std::sync::Arc::new(WalEngine::open(dir.path()).unwrap());

        let mut handles = vec![];
        for i in 0..5 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                let key = format!("rw_key_{}", i);
                e.put(key.as_bytes(), b"value").unwrap();
            }));
        }
        for _ in 0..5 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    let _ = e.get(b"rw_key_0");
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    /// 空 value 测试
    #[test]
    fn test_empty_value() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        engine.put(b"key", b"").unwrap();
        let v = engine.get(b"key").unwrap();
        assert_eq!(v, Some(b"".to_vec()));
    }

    /// 大 value 测试（1MB）
    #[test]
    fn test_large_value() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        let big = vec![42u8; 1024 * 1024];
        engine.put(b"bigkey", &big).unwrap();
        let v = engine.get(b"bigkey").unwrap();
        assert_eq!(v, Some(big));
    }

    /// 特殊字符 key 测试（含 null 字节和高位字节）
    #[test]
    fn test_special_char_key() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        let key = b"k\x00ey\xff\xfe";
        engine.put(key, b"value").unwrap();
        let v = engine.get(key).unwrap();
        assert_eq!(v, Some(b"value".to_vec()));
    }

    /// 前缀扫描：空 prefix 返回所有 key
    #[test]
    fn test_scan_prefix_empty_prefix() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
        engine.put(b"c", b"3").unwrap();
        let result = engine.scan_prefix(b"").unwrap();
        assert_eq!(result.len(), 3);
    }

    /// 前缀扫描：无匹配返回空
    #[test]
    fn test_scan_prefix_no_match() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        engine.put(b"abc", b"1").unwrap();
        let result = engine.scan_prefix(b"xyz").unwrap();
        assert!(result.is_empty());
    }

    /// 前缀扫描：结果按 key 字典序排列
    #[test]
    fn test_scan_prefix_order() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        engine.put(b"user:3", b"c").unwrap();
        engine.put(b"user:1", b"a").unwrap();
        engine.put(b"user:2", b"b").unwrap();
        let result = engine.scan_prefix(b"user:").unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, b"user:1");
        assert_eq!(result[1].0, b"user:2");
        assert_eq!(result[2].0, b"user:3");
    }

    /// 范围扫描：反向范围返回空
    #[test]
    fn test_scan_empty_range() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        engine.put(b"m", b"1").unwrap();
        let result = engine.scan(b"z", b"a").unwrap();
        assert!(result.is_empty());
    }

    /// TTL=0 表示永不过期
    #[test]
    fn test_ttl_zero_means_no_expiry() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        engine
            .put_with_ttl(b"key", b"value", std::time::Duration::from_millis(0))
            .unwrap();
        let v = engine.get(b"key").unwrap();
        assert_eq!(v, Some(b"value".to_vec()));
    }

    /// Compaction 后删除的 key 不再出现
    #[test]
    fn test_compaction_removes_tombstones() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open_with_threshold(dir.path(), 256).unwrap();
        engine.put(b"key1", b"val1").unwrap();
        engine.put(b"key2", b"val2").unwrap();
        engine.delete(b"key1").unwrap();
        // 触发 compaction
        for i in 0..100 {
            let k = format!("pad_{}", i);
            engine.put(k.as_bytes(), &vec![0u8; 64]).unwrap();
        }
        engine.do_compact().unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), None);
        assert_eq!(engine.get(b"key2").unwrap(), Some(b"val2".to_vec()));
    }

    /// 多次 compaction 数据始终正确
    #[test]
    fn test_multiple_compactions() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open_with_threshold(dir.path(), 256).unwrap();
        for round in 0..3 {
            for i in 0..50 {
                let k = format!("r{}_key_{}", round, i);
                engine.put(k.as_bytes(), b"val").unwrap();
            }
            engine.do_compact().unwrap();
        }
        // 验证第一轮的 key 仍然存在（因为没有被覆盖）
        assert_eq!(engine.get(b"r0_key_0").unwrap(), Some(b"val".to_vec()));
    }

    /// open 不存在的目录时自动创建
    #[test]
    fn test_open_nonexistent_dir() {
        let dir = TempDir::new().unwrap();
        let new_dir = dir.path().join("sub1").join("sub2");
        let engine = WalEngine::open(&new_dir);
        assert!(engine.is_ok());
    }

    /// 连续多次 flush 不报错
    #[test]
    fn test_flush_idempotent() {
        let dir = TempDir::new().unwrap();
        let engine = WalEngine::open(dir.path()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine.flush().unwrap();
        engine.flush().unwrap();
        engine.flush().unwrap();
    }
}
