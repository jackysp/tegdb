use criterion::{criterion_group, criterion_main, Criterion, black_box, Throughput};
use tokio::runtime::Runtime;
use std::path::PathBuf;
use tegdb::Engine;

fn concurrency_engine_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("engine_concurrent");
    // We're doing 4 operations per iteration
    group.throughput(Throughput::Elements(4));

    group.bench_function("set", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent.db").ok();
            rt.block_on(async {
                let engine = Engine::new(PathBuf::from("concurrent.db"));
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key = b"key";
                    let value = b"value";
                    let mut engine = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        engine.set(key, value.to_vec()).await.unwrap();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent.db").ok();
            rt.block_on(async {
                let engine = Engine::new(PathBuf::from("concurrent.db"));
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key = b"key";
                    let mut engine = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = engine.get(key).await;
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    group.bench_function("scan", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent.db").ok();
            rt.block_on(async {
                let engine = Engine::new(PathBuf::from("concurrent.db"));
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let mut engine = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = engine
                            .scan(b"a".to_vec()..b"z".to_vec())
                            .await
                            .unwrap()
                            .collect::<Vec<_>>();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    group.bench_function("del", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent.db").ok();
            rt.block_on(async {
                let engine = Engine::new(PathBuf::from("concurrent.db"));
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key = b"key";
                    let mut engine = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = engine.del(key).await;
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });
    
    group.finish();
}

fn concurrency_sled_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("sled_concurrent");
    // We're doing 4 operations per iteration
    group.throughput(Throughput::Elements(4));

    group.bench_function("insert", |b| {
        b.iter(|| {
            std::fs::remove_dir_all("concurrent_sled").ok();
            rt.block_on(async {
                let db = sled::open("concurrent_sled").unwrap();
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key = b"key";
                    let db = db.clone();
                    tasks.push(tokio::spawn(async move {
                        db.insert(&key, b"value").unwrap();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent_sled").ok();
            rt.block_on(async {
                let db = sled::open("concurrent_sled").unwrap();
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key = b"key";
                    tasks.push(tokio::spawn({
                        let db = db.clone();
                        async move {
                            let _ = db.get(&key).unwrap();
                        }
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    group.bench_function("scan", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent_sled").ok();
            rt.block_on(async {
                let db = sled::open("concurrent_sled").unwrap();
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let db = db.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = db.range(black_box("a")..black_box("z")).values().collect::<Result<Vec<_>, _>>();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    group.bench_function("remove", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent_sled").ok();
            rt.block_on(async {
                let db = sled::open("concurrent_sled").unwrap();
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key = b"key";
                    tasks.push(tokio::spawn({
                        let db = db.clone();
                        async move {
                            let _ = db.remove(&key);
                        }
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

criterion_group!(
    concurrent_benches,
    concurrency_engine_benchmark,
    concurrency_sled_benchmark,
);
criterion_main!(concurrent_benches);
