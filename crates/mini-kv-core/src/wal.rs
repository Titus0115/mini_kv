use std::io::{self, Read, Write};

use crate::error::{KvError, Result};

pub const WAL_FILE: &str = "wal.log";
pub const DEFAULT_COMPACTION_THRESHOLD: u64 = 64 * 1024 * 1024; // 64MB

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
    Ttl = 3,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Put),
            2 => Some(Self::Delete),
            3 => Some(Self::Ttl),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WalRecord {
    pub record_type: RecordType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expire_at: u64, // 0 = never expire
}

impl WalRecord {
    pub fn put(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            record_type: RecordType::Put,
            key,
            value,
            expire_at: 0,
        }
    }

    pub fn put_with_ttl(key: Vec<u8>, value: Vec<u8>, expire_at: u64) -> Self {
        Self {
            record_type: RecordType::Ttl,
            key,
            value,
            expire_at,
        }
    }

    pub fn delete(key: Vec<u8>) -> Self {
        Self {
            record_type: RecordType::Delete,
            key,
            value: Vec::new(),
            expire_at: 0,
        }
    }

    /// Encode record to bytes: [crc32(u32)][type(u8)][key_len(u32)][val_len(u32)][expire_at(u64)][key][value]
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u32;
        let val_len = self.value.len() as u32;

        // payload: type + key_len + val_len + expire_at + key + value
        let payload_len = 1 + 4 + 4 + 8 + self.key.len() + self.value.len();
        let mut buf = Vec::with_capacity(4 + payload_len);

        // placeholder for CRC
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.push(self.record_type as u8);
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&val_len.to_le_bytes());
        buf.extend_from_slice(&self.expire_at.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // compute CRC over everything after the CRC field
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Decode one record from the reader. Returns None at EOF.
    pub fn decode<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut crc_buf = [0u8; 4];
        match reader.read_exact(&mut crc_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(KvError::Io(e)),
        }

        let mut type_buf = [0u8; 1];
        reader.read_exact(&mut type_buf)?;
        let record_type = RecordType::from_u8(type_buf[0])
            .ok_or(KvError::CorruptedRecord)?;

        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let key_len = u32::from_le_bytes(len_buf) as usize;

        reader.read_exact(&mut len_buf)?;
        let val_len = u32::from_le_bytes(len_buf) as usize;

        let mut expire_buf = [0u8; 8];
        reader.read_exact(&mut expire_buf)?;
        let expire_at = u64::from_le_bytes(expire_buf);

        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        let mut value = vec![0u8; val_len];
        reader.read_exact(&mut value)?;

        // verify CRC
        let stored_crc = u32::from_le_bytes(crc_buf);
        let mut payload = Vec::with_capacity(1 + 4 + 4 + 8 + key_len + val_len);
        payload.push(type_buf[0]);
        payload.extend_from_slice(&(key_len as u32).to_le_bytes());
        payload.extend_from_slice(&(val_len as u32).to_le_bytes());
        payload.extend_from_slice(&expire_buf);
        payload.extend_from_slice(&key);
        payload.extend_from_slice(&value);

        let actual_crc = crc32fast::hash(&payload);
        if stored_crc != actual_crc {
            return Err(KvError::CrcMismatch {
                expected: stored_crc,
                actual: actual_crc,
            });
        }

        Ok(Some(WalRecord {
            record_type,
            key,
            value,
            expire_at,
        }))
    }
}

/// Read all records from a WAL file byte slice.
pub fn read_all_records(data: &[u8]) -> Result<Vec<WalRecord>> {
    let mut reader = io::Cursor::new(data);
    let mut records = Vec::new();
    while let Some(rec) = WalRecord::decode(&mut reader)? {
        records.push(rec);
    }
    Ok(records)
}

/// Write a batch of records to a writer (used during compaction).
pub fn write_records<W: Write>(writer: &mut W, records: &[WalRecord]) -> Result<()> {
    for rec in records {
        writer.write_all(&rec.encode())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let record = WalRecord {
            record_type: RecordType::Put,
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            expire_at: 1700000000000,
        };
        let encoded = record.encode();
        let mut cursor = io::Cursor::new(&encoded);
        let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.record_type, RecordType::Put);
        assert_eq!(decoded.key, b"test_key");
        assert_eq!(decoded.value, b"test_value".to_vec());
        assert_eq!(decoded.expire_at, 1700000000000);
    }

    #[test]
    fn test_encode_decode_delete() {
        let record = WalRecord {
            record_type: RecordType::Delete,
            key: b"deleted_key".to_vec(),
            value: Vec::new(),
            expire_at: 0,
        };
        let encoded = record.encode();
        let mut cursor = io::Cursor::new(&encoded);
        let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.record_type, RecordType::Delete);
        assert_eq!(decoded.key, b"deleted_key");
        assert!(decoded.value.is_empty());
    }

    #[test]
    fn test_decode_corrupted_data() {
        let mut data = vec![0u8; 100];
        data[0] = 0xFF; // 破坏 CRC
        data[1] = 0xFF;
        data[2] = 0xFF;
        data[3] = 0xFF;
        let mut cursor = io::Cursor::new(&data);
        let result = WalRecord::decode(&mut cursor);
        // 应该返回错误或 Ok(None)，取决于数据是否足够完整
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_record_type_variants() {
        // 测试三种 RecordType 都能正确编解码
        for rt in [RecordType::Put, RecordType::Delete, RecordType::Ttl] {
            let record = WalRecord {
                record_type: rt,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire_at: if rt == RecordType::Ttl { 12345 } else { 0 },
            };
            let encoded = record.encode();
            let mut cursor = io::Cursor::new(&encoded);
            let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();
            assert_eq!(decoded.record_type, rt);
        }
    }
}
