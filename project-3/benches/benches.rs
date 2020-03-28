use kvs::{KvStore, KvsEngine, SledKvsEngine};

use criterion::{BatchSize, criterion_group, criterion_main, Criterion};
use rand::Rng;
use tempfile::TempDir;

pub fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Set Bench Group");
    
    group.bench_function("kvs", |b| {
        b.iter_batched(|| {
            let temp_dir = TempDir::new().unwrap();
            (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
        },
        |(mut kvstore, _temp_dir)| {
            for i in 1..(1 << 12) {
                kvstore.set(format!("key{}", i), "value".to_owned()).unwrap();
            }
        },
        BatchSize::SmallInput,
    )});

    group.bench_function("sled", |b| {
        b.iter_batched(|| {
            let temp_dir = TempDir::new().unwrap();
            (SledKvsEngine::new(sled::open(temp_dir.path()).unwrap()), temp_dir)
        },
        |(mut kvstore, _temp_dir)| {
            for i in 1..(1 << 12) {
                kvstore.set(format!("key{}", i), "value".to_owned()).unwrap();
            }
        },
        BatchSize::SmallInput,
    )});

    group.finish();
}

pub fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Set Bench Group");
    
    group.bench_function("kvs", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut kvstore = KvStore::open(temp_dir.path()).unwrap();
        for i in 1..(1 << 12) {
            kvstore.set(format!("key{}", i), "value".to_owned()).unwrap();
        }
        let mut rng = rand::thread_rng();
        b.iter(|| {
            kvstore.get(format!("key{}", rng.gen_range(1, 1 << 12))).unwrap();
        });
    });

    group.bench_function("sled", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut kvstore = SledKvsEngine::new(sled::open(temp_dir.path()).unwrap());
        for i in 1..(1 << 12) {
            kvstore.set(format!("key{}", i), "value".to_owned()).unwrap();
        }
        let mut rng = rand::thread_rng();
        b.iter(|| {
            kvstore.get(format!("key{}", rng.gen_range(1, 1 << 12))).unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);