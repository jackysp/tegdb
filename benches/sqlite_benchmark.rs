//! Benchmark tests for SQLite operations using rusqlite and Criterion.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

fn sqlite_benchmark(c: &mut Criterion) {
    // Create a new Tokio runtime.
    let rt = Runtime::new().unwrap();
    // Create a temporary file for the SQLite database.
    let temp_file = NamedTempFile::new().unwrap();
    let conn = Connection::open(temp_file.path()).unwrap();

    // Create table if it doesn't exist.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test (key TEXT, value TEXT NOT NULL)",
        [],
    )
    .unwrap();

    let key = "key";
    let value = "value";

    let mut group = c.benchmark_group("sqlite_basic");
    group.throughput(Throughput::Elements(1));

    // Benchmark for inserting a record.
    group.bench_function("insert", |b| {
        b.iter(|| {
            rt.block_on(async {
                conn.execute(
                    "INSERT INTO test (key, value) VALUES (?1, ?2)",
                    params![key, value],
                )
                .unwrap();
            });
        })
    });

    // Benchmark for retrieving a record.
    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: String = conn
                    .query_row(
                        "SELECT value FROM test WHERE key = ?1",
                        params![key],
                        |row| row.get(0),
                    )
                    .unwrap();
            });
        })
    });

    // Benchmark for scanning a range of records.
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

    // Benchmark for deleting a record.
    group.bench_function("delete", |b| {
        b.iter(|| {
            rt.block_on(async {
                conn.execute("DELETE FROM test WHERE key = ?1", params![key])
                    .unwrap();
            });
        })
    });

    group.finish();
}

criterion_group!(sqlite_benches, sqlite_benchmark);
criterion_main!(sqlite_benches);
