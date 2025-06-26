use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

const N: usize = 100_000; // Number of random kmers

fn random_kmers() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(0);
    (0..N).map(|_| rng.gen()).collect()
}

fn bench_plusmin_one_hash_original(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("original", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                sum = sum.wrapping_add(1 - ((black_box(kmer) as u32 & 1) << 1) as i32);
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_arithmetic(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("arithmetic", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                sum = sum.wrapping_add(if (black_box(kmer) as u32 & 1) == 0 { 1 } else { -1 });
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_subtraction(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("subtraction", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                sum = sum.wrapping_add(1 - 2 * (black_box(kmer) as u32 & 1) as i32);
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_xor(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("xor", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                sum = sum.wrapping_add(1 - 2 * ((black_box(kmer) as u32 ^ 1) & 1) as i32);
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_negation(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("negation", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                sum = sum.wrapping_add(-((black_box(kmer) as u32 & 1) as i32 * 2 - 1));
            }
            black_box(sum)
        })
    });
}

// Advanced bit manipulation techniques
fn bench_plusmin_one_hash_sign_extend(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("sign_extend", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                // Convert 0/1 to -1/1 using sign extension
                let bit = (black_box(kmer) as u32 & 1) as i32;
                sum = sum.wrapping_add((bit << 1) - 1);
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_direct_convert(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("direct_convert", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                // Direct bit to value conversion
                let bit = (black_box(kmer) as u32 & 1) as i32;
                sum = sum.wrapping_add(1 - (bit << 1));
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_conditional_move(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("conditional_move", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                // Use conditional move (CMOV) instruction
                let is_odd = (black_box(kmer) as u32 & 1) != 0;
                sum = sum.wrapping_add(if is_odd { -1 } else { 1 });
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_bit_manipulation(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("bit_manipulation", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                // Pure bit manipulation
                let bit = black_box(kmer) as u32 & 1;
                sum = sum.wrapping_add(((bit ^ 1) << 1) as i32 - 1);
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_lookup_table(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("lookup_table", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            // Small lookup table: [1, -1] for even/odd
            let lookup = [1i32, -1i32];
            for &kmer in &kmers {
                let index = (black_box(kmer) as u32 & 1) as usize;
                sum = sum.wrapping_add(lookup[index]);
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_sign_bit(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("sign_bit", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                // Convert LSB directly to sign bit: 0 -> +1, 1 -> -1
                // Start with 1, then set sign bit based on LSB
                let lsb = black_box(kmer) as u32 & 1;
                let result = 1i32 | ((lsb as i32) << 31);
                sum = sum.wrapping_add(result);
            }
            black_box(sum)
        })
    });
}

fn bench_plusmin_one_hash_sign_bit_alt(c: &mut Criterion) {
    let kmers = random_kmers();
    c.bench_function("sign_bit_alt", |b| {
        b.iter(|| {
            let mut sum = 0i32;
            for &kmer in &kmers {
                // Alternative: use the LSB to conditionally set the sign bit
                let lsb = black_box(kmer) as u32 & 1;
                let result = if lsb != 0 { -1i32 } else { 1i32 };
                sum = sum.wrapping_add(result);
            }
            black_box(sum)
        })
    });
}

criterion_group!(
    name = plusmin_benchmarks;
    config = Criterion::default();
    targets = 
        bench_plusmin_one_hash_original,
        bench_plusmin_one_hash_arithmetic,
        bench_plusmin_one_hash_subtraction,
        bench_plusmin_one_hash_xor,
        bench_plusmin_one_hash_negation,
        bench_plusmin_one_hash_sign_extend,
        bench_plusmin_one_hash_direct_convert,
        bench_plusmin_one_hash_conditional_move,
        bench_plusmin_one_hash_bit_manipulation,
        bench_plusmin_one_hash_lookup_table,
        bench_plusmin_one_hash_sign_bit,
        bench_plusmin_one_hash_sign_bit_alt
);
criterion_main!(plusmin_benchmarks); 