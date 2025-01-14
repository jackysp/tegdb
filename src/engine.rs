use lru::LruCache;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::PathBuf;
use crate::sql::{parse_sql, SQLQuery};

pub struct Engine {
    log: Log,
    key_map: KeyMap,
    lru_cache: LruCache<Vec<u8>, Vec<u8>>,
}

type KeyMap = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;

impl Engine {
    pub fn new(path: PathBuf) -> Self {
        let mut log = Log::new(path);
        let key_map = log.build_key_map();
        let lru_cache = LruCache::new(NonZeroUsize::new(1_000_000).unwrap());
        let mut s = Self {
            log,
            key_map,
            lru_cache,
        };
        s.compact();
        s
    }

    pub fn get(&mut self, key: &[u8]) -> Vec<u8> {
        if let Some(cached_value) = self.lru_cache.get(&key.to_vec()) {
            return cached_value.clone();
        }
        if let Some((value_pos, value_len)) = self.key_map.get(key) {
            let value = self.log.read_value(*value_pos, *value_len);
            self.lru_cache.put(key.to_vec(), value.clone());
            value
        } else {
            Vec::new()
        }
    }

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) {
        if value.len() == 0 {
            self.del(key);
            return;
        }

        if let Some(cached_value) = self.lru_cache.get(&key.to_vec()) {
            if &value == cached_value {
                return;
            }
        }

        let (pos, len) = self.log.write_entry(key, &*value);
        let value_len = value.len() as u32;
        self.key_map.insert(
            key.to_vec(),
            (pos + len as u64 - value_len as u64, value_len),
        );
        self.lru_cache.put(key.to_vec(), value);
    }

    pub fn del(&mut self, key: &[u8]) {
        if self.key_map.get(key).is_none() {
            return;
        }

        self.log.write_entry(key, &[]);
        self.key_map.remove(key);
        self.lru_cache.pop(&key.to_vec());
    }

    pub fn scan<'a>(
        &'a mut self,
        range: Range<Vec<u8>>,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a> {
        let range = self.key_map.range(range);
        let log = &mut self.log;
        let lru_cache = &mut self.lru_cache;
        Box::new(range.map(move |(key, &(value_pos, value_len))| {
            let value = if let Some(cached_value) = lru_cache.get(key) {
                cached_value.clone()
            } else {
                let value = log.read_value(value_pos, value_len);
                lru_cache.put(key.clone(), value.clone());
                value
            };
            (key.clone(), value)
        }))
    }

    pub fn execute_sql(&mut self, query: &str) -> Result<String, String> {
        match parse_sql(query) {
            Ok((_, sql_query)) => match sql_query {
                SQLQuery::Select { columns, table } => {
                    if columns.len() != 1 || columns[0] != "value" {
                        return Err("Only SELECT value FROM t WHERE key = ? is supported".to_string());
                    }
                    let key = table.as_bytes();
                    let value = self.get(key);
                    Ok(format!("SELECT result: {}", String::from_utf8_lossy(&value)))
                }
                SQLQuery::Insert { table, values } => {
                    if values.len() != 2 {
                        return Err("Only INSERT INTO t VALUES (\"key\", \"value\") is supported".to_string());
                    }
                    let key = values[0].as_bytes();
                    let value = values[1].as_bytes().to_vec();
                    self.set(key, value);
                    Ok("INSERT query executed".to_string())
                }
                SQLQuery::Update { table, set } => {
                    if set.len() != 1 || set[0].0 != "value" {
                        return Err("Only UPDATE t SET value = ? WHERE key = ? is supported".to_string());
                    }
                    let key = table.as_bytes();
                    let value = set[0].1.as_bytes().to_vec();
                    self.set(key, value);
                    Ok("UPDATE query executed".to_string())
                }
                SQLQuery::Delete { table } => {
                    let key = table.as_bytes();
                    self.del(key);
                    Ok("DELETE query executed".to_string())
                }
            },
            Err(_) => Err("Failed to parse SQL query".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_engine() {
        let path = PathBuf::from("test.db");
        let mut engine = Engine::new(path.clone());

        // Test set and get
        let key = b"key";
        let value = b"value";
        engine.set(key, value.to_vec());
        let get_value = engine.get(key);
        assert_eq!(
            get_value,
            value,
            "Expected: {}, Got: {}",
            String::from_utf8_lossy(value),
            String::from_utf8_lossy(&get_value)
        );

        // Test del
        engine.del(key);
        let get_value = engine.get(key);
        assert_eq!(
            get_value,
            [],
            "Expected: {}, Got: {}",
            String::from_utf8_lossy(&[]),
            String::from_utf8_lossy(&get_value)
        );

        // Test scan
        let start_key = b"a";
        let end_key = b"z";
        engine.set(start_key, b"start_value".to_vec());
        engine.set(end_key, b"end_value".to_vec());
        let mut end_key_extended = Vec::new();
        end_key_extended.extend_from_slice(end_key);
        end_key_extended.extend_from_slice(&[1u8]);
        let result = engine
            .scan(start_key.to_vec()..end_key_extended)
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

        // Test execute_sql
        let select_query = "SELECT value FROM key";
        let insert_query = "INSERT INTO key VALUES (key, value)";
        let update_query = "UPDATE key SET value = value";
        let delete_query = "DELETE FROM key";

        assert_eq!(
            engine.execute_sql(select_query),
            Ok("SELECT result: value".to_string())
        );
        assert_eq!(
            engine.execute_sql(insert_query),
            Ok("INSERT query executed".to_string())
        );
        assert_eq!(
            engine.execute_sql(update_query),
            Ok("UPDATE query executed".to_string())
        );
        assert_eq!(
            engine.execute_sql(delete_query),
            Ok("DELETE query executed".to_string())
        );

        // Clean up
        drop(engine);
        fs::remove_file(path).unwrap();
    }
}

impl Engine {
    fn flush(&mut self) {
        self.log.file.sync_all().unwrap();
    }

    fn construct_log(&mut self, path: PathBuf) -> (Log, KeyMap) {
        let mut new_key_map = KeyMap::new();
        let mut new_log = Log::new(path);
        new_log.file.set_len(0).unwrap();
        for (key, (value_pos, value_len)) in self.key_map.iter() {
            let value = self.log.read_value(*value_pos, *value_len);
            let (pos, len) = new_log.write_entry(key, &*value);
            new_key_map.insert(
                key.to_vec(),
                (pos + len as u64 - *value_len as u64, *value_len),
            );
        }
        (new_log, new_key_map)
    }

    fn compact(&mut self) {
        let mut tmp_path = self.log.path.clone();
        tmp_path.set_extension("new");
        let (mut new_log, new_key_map) = self.construct_log(tmp_path);

        std::fs::rename(&new_log.path, &self.log.path).unwrap();
        new_log.path = self.log.path.clone();

        self.log = new_log;
        self.key_map = new_key_map;
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.flush();
        self.compact()
    }
}

struct Log {
    path: PathBuf,
    file: std::fs::File,
}

impl Log {
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

            if value_len == 0 {
                key_map.remove(&key);
            } else {
                key_map.insert(key, (value_pos, value_len));
            }

            r.seek_relative(value_len as i64).unwrap();
            pos = value_pos + value_len as u64;
        }
        key_map
    }

    fn read_value(&mut self, value_pos: u64, value_len: u32) -> Vec<u8> {
        let mut value = vec![0; value_len as usize];
        self.file.seek(SeekFrom::Start(value_pos)).unwrap();
        self.file.read_exact(&mut value).unwrap();
        value
    }

    fn write_entry(&mut self, key: &[u8], value: &[u8]) -> (u64, u32) {
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let len = 4 + 4 + key_len + value_len;

        let pos = self.file.seek(SeekFrom::End(0)).unwrap();
        let mut w = BufWriter::with_capacity(len as usize, &mut self.file);

        let mut buffer = Vec::with_capacity(len as usize);
        buffer.extend_from_slice(&key_len.to_be_bytes());
        buffer.extend_from_slice(&value_len.to_be_bytes());
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(&value);

        w.write_all(&buffer).unwrap();
        w.flush().unwrap();

        (pos, len)
    }
}
