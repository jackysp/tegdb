//! Tegdb Engine: A persistent key-value store with an append-only log and automatic compaction.
//! This module implements CRUD operations and log rebuilding to maintain data integrity.

use crate::log;

use std::path::PathBuf;
use std::sync::Arc;
use std::ops::Range;
use dashmap::DashMap;

/// Core storage engine that provides CRUD operations with log compaction.
#[derive(Clone)]
pub struct Engine {
    log: Arc<log::Log>,
    key_map: Arc<DashMap<Vec<u8>, Vec<u8>>>,
}

impl Engine {
    /// Creates a new Engine instance.
    /// Initializes the underlying log, reconstructs the in-memory key map from the log,
    /// and performs an immediate compaction to optimize storage.
    pub fn new(path: PathBuf) -> Self {
        let log = Arc::new(log::Log::new(path));
        let built_map = log.build_key_map();
        let key_map = Arc::new(DashMap::new());
        for (k, v) in built_map {
            key_map.insert(k, v);
        }
        let mut s = Self { log, key_map };
        s.compact().expect("Failed to compact log");
        s
    }

    /// Retrieves the value associated with the given key asynchronously.
    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.key_map.get(key).map(|entry| entry.value().clone())
    }

    /// Inserts or updates the value for the given key.
    /// If an empty value is provided, the key is removed.
    /// Returns an error if the key or value exceeds predefined size limits.
    pub async fn set(&self, key: &[u8], value: Vec<u8>) -> Result<(), std::io::Error> {
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
        if value.is_empty() {
            return self.del(key).await;
        }
        if let Some(existing) = self.key_map.get(key) {
            if *existing == value {
                return Ok(());
            }
        }
        self.log.write_entry(key, &value);
        self.key_map.insert(key.to_vec(), value);
        Ok(())
    }

    /// Deletes a key-value pair from the store.
    /// If the key does not exist, the operation is a no-op.
    pub async fn del(&self, key: &[u8]) -> Result<(), std::io::Error> {
        if self.key_map.get(key).is_none() {
            return Ok(());
        }
        self.log.write_entry(key, &[]);
        self.key_map.remove(key);
        Ok(())
    }

    /// Returns an iterator over key-value pairs within the specified range.
    pub async fn scan<'a>(
        &'a self,
        range: Range<Vec<u8>>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>, std::io::Error> {
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = self
            .key_map
            .iter()
            .filter(|entry| entry.key() >= &range.start && entry.key() < &range.end)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Box::new(results.into_iter()))
    }

    /// Flushes the current log and shuts down the log writer to ensure data persistence.
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.log.writer.flush();
        self.log.writer.shutdown();
        Ok(())
    }

    /// Compacts the log by building a new log file containing only valid entries.
    /// The new log replaces the old one to reclaim storage space.
    fn compact(&mut self) -> Result<(), std::io::Error> {
        let mut tmp_path = self.log.path.clone();
        tmp_path.set_extension("new");
        let (mut new_log, new_key_map) = self.construct_log(tmp_path)?;
        std::fs::rename(&new_log.path, &self.log.path)?;
        new_log.path = self.log.path.clone();
        self.log = Arc::new(new_log);
        self.key_map = Arc::new(DashMap::new());
        for (k, v) in new_key_map {
            self.key_map.insert(k, v);
        }
        Ok(())
    }

    /// Constructs a compacted log file and a corresponding key map based on valid entries.
    fn construct_log(&mut self, path: PathBuf) -> Result<(log::Log, DashMap<Vec<u8>, Vec<u8>>), std::io::Error> {
        let new_key_map = DashMap::new();
        let new_log = log::Log::new(path);
        {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&new_log.path)?;
            file.set_len(0)?;
        }
        for entry in self.key_map.iter() {
            new_log.write_entry(entry.key(), entry.value());
            new_key_map.insert(entry.key().clone(), entry.value().clone());
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
        let engine = Engine::new(path.clone());
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
        engine.del(key).await.unwrap();
        let get_value = engine.get(key).await;
        assert_eq!(
            get_value,
            None,
            "Expected: {}, Got: {}",
            String::from_utf8_lossy(&[]),
            String::from_utf8_lossy(get_value.as_deref().unwrap_or_default())
        );
        let start_key = b"a";
        let end_key = b"z";
        engine.set(start_key, b"start_value".to_vec()).await.unwrap();
        engine.set(end_key, b"end_value".to_vec()).await.unwrap();
        let mut end_key_extended = Vec::new();
        end_key_extended.extend_from_slice(end_key);
        end_key_extended.extend_from_slice(&[1u8]);
        let result = engine.scan(start_key.to_vec()..end_key_extended)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        let expected = vec![
            (start_key.to_vec(), b"start_value".to_vec()),
            (end_key.to_vec(), b"end_value".to_vec()),
        ];
        let expected_strings: Vec<(String, String)> = expected.iter().map(|(k, v)| {
            (String::from_utf8_lossy(k).into_owned(), String::from_utf8_lossy(v).into_owned())
        }).collect();
        let result_strings: Vec<(String, String)> = result.iter().map(|(k, v)| {
            (String::from_utf8_lossy(k).into_owned(), String::from_utf8_lossy(v).into_owned())
        }).collect();
        assert_eq!(
            result_strings, expected_strings,
            "Expected: {:?}, Got: {:?}",
            expected_strings, result_strings
        );
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
