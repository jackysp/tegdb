//! Benchmark tests for redb database operations using Criterion.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use redb::{Database, ReadableTable, TableDefinition};
use std::path::PathBuf;
use tokio::runtime::Runtime;

fn redb_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let path = PathBuf::from("redb");
    let db = Database::create(path).unwrap();
    let table_def: TableDefinition<&str, &str> = TableDefinition::new("my_table");

    let key = "key";
    let value = "value";

    let mut group = c.benchmark_group("redb_basic");
    group.throughput(Throughput::Elements(1));

    // Benchmark for put operation.
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

    // Benchmark for get operation.
    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tx = db.begin_read().unwrap();
                let table = tx.open_table(table_def).unwrap();
                table.get(black_box(key)).unwrap();
            });
        })
    });

    // Benchmark for scan operation.
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

    // Benchmark for delete operation.
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

async fn redb_seq_benchmark(c: &mut Criterion, value_size: usize) {
    let path = PathBuf::from("redb");
    let db = Database::create(path).unwrap();
    let table_def: TableDefinition<&str, &str> = TableDefinition::new("my_table");

    let value = vec![0; value_size];
    let value_str = String::from_utf8(value).unwrap();

    let mut group = c.benchmark_group(format!("redb_seq_{}", value_size));
    group.throughput(Throughput::Elements(1));

    // Sequential benchmark for put.
    group.bench_function("put", |b| {
        let mut i = 0;
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let tx = db.begin_write().unwrap();
                    {
                        let key = format!("key{}", i);
                        let mut table = tx.open_table(table_def).unwrap();
                        table
                            .insert(black_box(key.as_str()), black_box(value_str.as_str()))
                            .unwrap();
                        i += 1;
                    }
                    tx.commit().unwrap();
                });
            });
        })
    });

    // Sequential benchmark for get.
    group.bench_function("get", |b| {
        let mut i = 0;
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let tx = db.begin_read().unwrap();
                    let table = tx.open_table(table_def).unwrap();
                    let key = format!("key{}", i);
                    table.get(black_box(key.as_str())).unwrap();
                    i += 1;
                });
            });
        })
    });

    // Sequential benchmark for delete.
    group.bench_function("del", |b| {
        let mut i = 0;
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let tx = db.begin_write().unwrap();
                    {
                        let key = format!("key{}", i);
                        let mut table = tx.open_table(table_def).unwrap();
                        table.remove(black_box(key.as_str())).unwrap();
                        i += 1;
                    }
                    tx.commit().unwrap();
                });
            });
        })
    });

    group.finish();
}

/// Benchmark with a short value size.
fn redb_short_benchmark(c: &mut Criterion) {
    let value_size = 1024;
    let rt = Runtime::new().unwrap();
    rt.block_on(redb_seq_benchmark(c, value_size));
}

/// Benchmark with a long value size.
fn redb_long_benchmark(c: &mut Criterion) {
    let value_size = 255_000;
    let rt = Runtime::new().unwrap();
    rt.block_on(redb_seq_benchmark(c, value_size));
}

criterion_group!(
    redb_benches,
    redb_benchmark,
    redb_short_benchmark,
    redb_long_benchmark
);
criterion_main!(redb_benches);
