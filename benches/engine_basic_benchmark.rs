use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use redb::{Database, ReadableTable, TableDefinition};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;
use std::path::PathBuf;
use tegdb::Engine;
use tokio::runtime::Runtime;

fn engine_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut engine = Engine::new(PathBuf::from("test.db"));
    let key = b"key";
    let value = b"value";

    let mut group = c.benchmark_group("engine_basic");
    group.throughput(Throughput::Elements(1));

    group.bench_function("set", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine.set(black_box(key), black_box(value.to_vec())).await.unwrap();
            });
        })
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine.get(black_box(key)).await.unwrap();
            });
        })
    });

    group.bench_function("scan", |b| {
        let start_key = b"a";
        let end_key = b"z";
        b.iter(|| {
            rt.block_on(async {
                let _ = engine
                    .scan(black_box(start_key.to_vec())..black_box(end_key.to_vec()))
                    .await
                    .unwrap()
                    .collect::<Vec<_>>();
            });
        })
    });

    group.bench_function("del", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine.del(black_box(key)).await.unwrap();
            });
        })
    });

    group.finish();
}

fn sled_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let path = "sled";
    let db = sled::open(path).unwrap();
    let key = b"key";
    let value = b"value";

    let mut group = c.benchmark_group("sled_basic");
    group.throughput(Throughput::Elements(1));

    group.bench_function("insert", |b| {
        b.iter(|| {
            rt.block_on(async {
                db.insert(black_box(key), black_box(value)).unwrap();
            });
        })
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = db.get(black_box(key)).unwrap().map(|v| v.to_vec());
            });
        })
    });

    group.bench_function("scan", |b| {
        let start_key = "a";
        let end_key = "z";
        b.iter(|| {
            rt.block_on(async {
                let _ = db
                    .range(black_box(start_key)..black_box(end_key))
                    .values()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            });
        })
    });

    group.bench_function("remove", |b| {
        b.iter(|| {
            rt.block_on(async {
                db.remove(black_box(key)).unwrap();
            });
        })
    });

    group.finish();
}

fn redb_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let path = PathBuf::from("redb");
    let db = Database::create(path).unwrap();
    let table_def: TableDefinition<&str, &str> = TableDefinition::new("my_table");

    let key = "key";
    let value = "value";

    let mut group = c.benchmark_group("redb_basic");
    group.throughput(Throughput::Elements(1));

    group.bench_function("put", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tx = db.begin_write().unwrap();
                {
                    let mut table = tx.open_table(table_def).unwrap();
                    table.insert(black_box(key), black_box(value)).unwrap();
                }
                tx.commit().unwrap();
            });
        })
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tx = db.begin_read().unwrap();
                let table = tx.open_table(table_def).unwrap();
                table.get(black_box(key)).unwrap();
            });
        })
    });

    group.bench_function("scan", |b| {
        let start_key = "a";
        let end_key = "z";
        b.iter(|| {
            rt.block_on(async {
                let tx = db.begin_read().unwrap();
                let table = tx.open_table(table_def).unwrap();
                let _ = table
                    .range(black_box(start_key)..black_box(end_key))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            });
        })
    });

    group.bench_function("del", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tx = db.begin_write().unwrap();
                {
                    let mut table = tx.open_table(table_def).unwrap();
                    table.remove(black_box(key)).unwrap();
                }
                tx.commit().unwrap();
            });
        })
    });

    group.finish();
}

fn sqlite_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let temp_file = NamedTempFile::new().unwrap();
    let conn = Connection::open(temp_file.path()).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS test (key TEXT, value TEXT NOT NULL)",
        [],
    )
    .unwrap();

    let key = "key";
    let value = "value";

    let mut group = c.benchmark_group("sqlite_basic");
    group.throughput(Throughput::Elements(1));

    group.bench_function("insert", |b| {
        b.iter(|| {
            rt.block_on(async {
                conn.execute("INSERT INTO test (key, value) VALUES (?1, ?2)", params![key, value])
                    .unwrap();
            });
        })
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: String = conn
                    .query_row("SELECT value FROM test WHERE key = ?1", params![key], |row| row.get(0))
                    .unwrap();
            });
        })
    });

    group.bench_function("scan", |b| {
        let start_key = "a";
        let end_key = "z";
        b.iter(|| {
            rt.block_on(async {
                let mut stmt = conn
                    .prepare("SELECT key, value FROM test WHERE key >= ?1 AND key <= ?2")
                    .unwrap();
                let _ = stmt
                    .query_map(params![start_key, end_key], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            });
        })
    });

    group.bench_function("delete", |b| {
        b.iter(|| {
            rt.block_on(async {
                conn.execute("DELETE FROM test WHERE key = ?1", params![key]).unwrap();
            });
        })
    });

    group.finish();
}

criterion_group!(benches, engine_benchmark, sled_benchmark, redb_benchmark, sqlite_benchmark);
criterion_main!(benches);
