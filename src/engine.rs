//! This module implements a persistent key-value storage engine using an append-only log.
//! It provides CRUD operations with automatic log compaction for optimization.

use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

/// Core storage engine that provides CRUD operations with log compaction.
#[derive(Clone)]
pub struct Engine {
    log: Arc<Mutex<Log>>,         // Append-only log file for persistence.
    key_map: Arc<RwLock<KeyMap>>, // In-memory key-value index for fast lookups.
}

// Internal type alias for the key-value store.
type KeyMap = std::collections::BTreeMap<Vec<u8>, Vec<u8>>;

impl Engine {
    /// Creates a new storage engine instance.
    /// Initializes the log and rebuilds the in-memory key map.
    /// Immediately runs log compaction to optimize storage.
    pub fn new(path: PathBuf) -> Self {
        let log = Arc::new(Mutex::new(Log::new(path)));
        let key_map = Arc::new(RwLock::new(log.lock().unwrap().build_key_map()));
        let mut s = Self { log, key_map };
        s.compact().expect("Failed to compact log");
        s
    }

    /// Retrieves a value for the given key.
    pub async fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let key_map = self.key_map.read().unwrap();
        key_map.get(key).map(|v| v.clone())
    }

    /// Sets a value for the given key.
    /// If the value is empty, the key is deleted.
    /// Returns error if key > 1KB or value > 256KB.
    pub async fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), std::io::Error> {
        // Validate key and value sizes.
        if key.len() > 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Key length exceeds 1k",
            ));
        }
        if value.len() > 256 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Value length exceeds 256k",
            ));
        }

        // Use empty value to signal deletion.
        if value.is_empty() {
            return self.del(key).await;
        }

        let mut key_map = self.key_map.write().unwrap();

        // Skip update if the value has not changed.
        if let Some(existing) = key_map.get(key) {
            if *existing == value {
                return Ok(());
            }
        }

        self.log.lock().unwrap().write_entry(key, &value);
        key_map.insert(key.to_vec(), value);
        Ok(())
    }

    /// Deletes the value for the given key.
    /// If the key does not exist, the operation is a no-op.
    pub async fn del(&mut self, key: &[u8]) -> Result<(), std::io::Error> {
        let mut key_map = self.key_map.write().unwrap();
        if key_map.get(key).is_none() {
            return Ok(());
        }

        self.log.lock().unwrap().write_entry(key, &[]);
        key_map.remove(key);
        Ok(())
    }

    /// Returns an iterator over a range of key-value pairs.
    pub async fn scan<'a>(
        &'a mut self,
        range: Range<Vec<u8>>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>, std::io::Error> {
        let key_map = self.key_map.read().unwrap();
        let range: Vec<_> = key_map
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(Box::new(range.into_iter()))
    }

    // Flushes the underlying log file to ensure data persistence.
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.log.lock().unwrap().file.sync_all()
    }

    /// Compacts the log to remove stale entries.
    /// A new log file with only valid entries is constructed and replaces the old log.
    fn compact(&mut self) -> Result<(), std::io::Error> {
        // Define a temporary file path for the new log.
        let mut tmp_path = self.log.lock().unwrap().path.clone();
        tmp_path.set_extension("new");
        let (mut new_log, new_key_map) = self.construct_log(tmp_path)?;

        // Swap the new log file with the existing one.
        std::fs::rename(&new_log.path, &self.log.lock().unwrap().path)?;
        new_log.path = self.log.lock().unwrap().path.clone();

        self.log = Arc::new(Mutex::new(new_log));
        self.key_map = Arc::new(RwLock::new(new_key_map));
        Ok(())
    }

    /// Constructs a new log file by copying only valid key-value entries.
    fn construct_log(&mut self, path: PathBuf) -> Result<(Log, KeyMap), std::io::Error> {
        let mut new_key_map = KeyMap::new();
        let mut new_log = Log::new(path);
        new_log.file.set_len(0)?;

        // Copy valid entries from the existing key map.
        let key_map = self.key_map.read().unwrap();
        for (key, value) in key_map.iter() {
            new_log.write_entry(key, value);
            new_key_map.insert(key.to_vec(), value.clone());
        }
        Ok((new_log, new_key_map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_engine() {
        let path = PathBuf::from("test.db");
        let mut engine = Engine::new(path.clone());

        // Test set and get
        let key = b"key";
        let value = b"value";
        engine.set(key, value.to_vec()).await.unwrap();
        let get_value = engine.get(key).await.unwrap();
        assert_eq!(
            get_value,
            value,
            "Expected: {}, Got: {}",
            String::from_utf8_lossy(value),
            String::from_utf8_lossy(&get_value)
        );

        // Test del
        engine.del(key).await.unwrap();
        let get_value = engine.get(key).await;
        assert_eq!(
            get_value,
            None,
            "Expected: {}, Got: {}",
            String::from_utf8_lossy(&[]),
            String::from_utf8_lossy(get_value.as_deref().unwrap_or_default())
        );

        // Test scan
        let start_key = b"a";
        let end_key = b"z";
        engine
            .set(start_key, b"start_value".to_vec())
            .await
            .unwrap();
        engine.set(end_key, b"end_value".to_vec()).await.unwrap();
        let mut end_key_extended = Vec::new();
        end_key_extended.extend_from_slice(end_key);
        end_key_extended.extend_from_slice(&[1u8]);
        let result = engine
            .scan(start_key.to_vec()..end_key_extended)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        let expected = vec![
            (start_key.to_vec(), b"start_value".to_vec()),
            (end_key.to_vec(), b"end_value".to_vec()),
        ];
        let expected_strings: Vec<(String, String)> = expected
            .iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(k).into_owned(),
                    String::from_utf8_lossy(v).into_owned(),
                )
            })
            .collect();
        let result_strings: Vec<(String, String)> = result
            .iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(k).into_owned(),
                    String::from_utf8_lossy(v).into_owned(),
                )
            })
            .collect();
        assert_eq!(
            result_strings, expected_strings,
            "Expected: {:?}, Got: {:?}",
            expected_strings, result_strings
        );

        // Clean up
        drop(engine);
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        use tokio::sync::Mutex;
        let path = PathBuf::from("concurrent.db");
        let engine = Arc::new(Mutex::new(Engine::new(path.clone())));

        let tasks: Vec<_> = (0..10)
            .map(|i| {
                let engine = engine.clone();
                tokio::spawn(async move {
                    let key = format!("key_{}", i).into_bytes();
                    let value = format!("value_{}", i).into_bytes();
                    engine.lock().await.set(&key, value.clone()).await.unwrap();
                    let got = engine.lock().await.get(&key).await.unwrap();
                    assert_eq!(got, value);
                })
            })
            .collect();

        for t in tasks {
            t.await.unwrap();
        }

        drop(engine);
        std::fs::remove_file(path).unwrap();
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

/// Log manages the append-only log file used for data persistence.
struct Log {
    path: PathBuf,
    file: std::fs::File,
}

impl Log {
    /// Creates a new log instance.
    /// Ensures the directory for the log file exists.
    fn new(path: PathBuf) -> Self {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).unwrap()
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        Self { path, file }
    }

    /// Rebuilds the in-memory key map by scanning the log file.
    /// Iterates through each log entry and applies insertions and deletions.
    fn build_key_map(&mut self) -> KeyMap {
        let mut len_buf = [0u8; 4];
        let mut key_map = KeyMap::new();
        let file_len = self.file.metadata().unwrap().len();
        let mut r = BufReader::new(&mut self.file);
        let mut pos = r.seek(SeekFrom::Start(0)).unwrap();

        while pos < file_len {
            r.read_exact(&mut len_buf).unwrap();
            let key_len = u32::from_be_bytes(len_buf);
            r.read_exact(&mut len_buf).unwrap();
            let value_len = u32::from_be_bytes(len_buf);
            let value_pos = pos + 4 + 4 + key_len as u64;

            let mut key = vec![0; key_len as usize];
            r.read_exact(&mut key).unwrap();

            let mut value = vec![0; value_len as usize];
            r.read_exact(&mut value).unwrap();

            // Remove the key if the entry represents a deletion.
            if value_len == 0 {
                key_map.remove(&key);
            } else {
                key_map.insert(key, value);
            }

            pos = value_pos + value_len as u64;
        }
        key_map
    }

    /// Writes a key-value entry to the log.
    /// Entry format: [key_len (4 bytes)][value_len (4 bytes)][key][value]
    fn write_entry(&mut self, key: &[u8], value: &[u8]) {
        if key.len() > 1024 || value.len() > 256 * 1024 {
            panic!("Key or value length exceeds allowed limit");
        }
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let len = 4 + 4 + key_len + value_len;

        // Append the entry at the end of the file.
        let _ = self.file.seek(SeekFrom::End(0)).unwrap();
        let mut w = BufWriter::with_capacity(len as usize, &mut self.file);

        let mut buffer = Vec::with_capacity(len as usize);
        // Build the entry buffer: header lengths then key and value.
        buffer.extend_from_slice(&key_len.to_be_bytes());
        buffer.extend_from_slice(&value_len.to_be_bytes());
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(&value);

        w.write_all(&buffer).unwrap();
        w.flush().unwrap();
    }
}

impl Clone for Log {
    fn clone(&self) -> Self {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)
            .unwrap();
        Self {
            path: self.path.clone(),
            file,
        }
    }
}
