use criterion::{criterion_group, criterion_main, Criterion, black_box};
use tokio::runtime::Runtime;
use std::path::PathBuf;
use tegdb::Engine;

fn concurrency_engine_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("engine concurrent set", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent.db").ok();
            rt.block_on(async {
                let engine = Engine::new(PathBuf::from("concurrent.db"));
                let mut tasks = Vec::new();
                for i in 0..4 {
                    let key = format!("key_{}", i).into_bytes();
                    let value = format!("value_{}", i).into_bytes();
                    let mut engine = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        engine.set(&key, value).await.unwrap();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    c.bench_function("engine concurrent get", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent.db").ok();
            rt.block_on(async {
                let engine = Engine::new(PathBuf::from("concurrent.db"));
                let mut tasks = Vec::new();
                for i in 0..4 {
                    let key = format!("get_key_{}", i).into_bytes();
                    let mut engine = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = engine.get(&key).await;
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    c.bench_function("engine concurrent scan", |b| {
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

    c.bench_function("engine concurrent del", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent.db").ok();
            rt.block_on(async {
                let engine = Engine::new(PathBuf::from("concurrent.db"));
                let mut tasks = Vec::new();
                for i in 0..4 {
                    let key = format!("del_key_{}", i).into_bytes();
                    let mut engine = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = engine.del(&key).await;
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });
}

fn concurrency_sled_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("sled concurrent insert", |b| {
        b.iter(|| {
            std::fs::remove_dir_all("concurrent_sled").ok();
            rt.block_on(async {
                let db = sled::open("concurrent_sled").unwrap();
                let mut tasks = Vec::new();
                for i in 0..4 {
                    let key = format!("sled_key_{}", i).into_bytes();
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
    c.bench_function("sled concurrent get", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent_sled").ok();
            rt.block_on(async {
                let db = sled::open("concurrent_sled").unwrap();
                let mut tasks = Vec::new();
                for i in 0..4 {
                    let key = format!("sled_get_key_{}", i).into_bytes();
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
    c.bench_function("sled concurrent scan", |b| {
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
    c.bench_function("sled concurrent remove", |b| {
        b.iter(|| {
            std::fs::remove_file("concurrent_sled").ok();
            rt.block_on(async {
                let db = sled::open("concurrent_sled").unwrap();
                let mut tasks = Vec::new();
                for i in 0..4 {
                    let key = format!("sled_remove_{}", i).into_bytes();
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
}

criterion_group!(
    concurrent_benches,
    concurrency_engine_benchmark,
    concurrency_sled_benchmark,
);
criterion_main!(concurrent_benches);
