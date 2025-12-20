use bascet_derive::Budget;
use bounded_integer::BoundedU64;
use bytesize::ByteSize;

#[derive(Budget)]
struct TestConfig {
    #[threads(Total)]
    total_threads: BoundedU64<4, { u64::MAX }>,

    #[mem(Total)]
    total_mem: ByteSize,

    #[threads(Read, 50.0)]
    read: BoundedU64<2, { u64::MAX }>,

    #[threads(Debarcode, 25.0)]
    debarcode: BoundedU64<1, { u64::MAX }>,

    #[threads(Write, 25.0)]
    write: BoundedU64<1, { u64::MAX }>,

    #[mem(StreamArena, 60.0)]
    stream_arena: ByteSize,

    #[mem(StreamBuffer, 60.0)]
    stream_buffer: ByteSize,
}

#[test]
fn test_budget() {
    let cfg = TestConfig::builder()
        .total_threads(BoundedU64::new(4).unwrap())
        .total_mem(ByteSize::gib(16))  // use percentage default: 60% of 16 GiB
        .build();

    // Print all values
    println!("Total threads: {}", cfg.total_threads);
    println!("Total mem: {}", cfg.total_mem);
    println!("Read threads: {}", cfg.threads::<Read>().get());
    println!("Debarcode threads: {}", cfg.threads::<Debarcode>().get());
    println!("Write threads: {}", cfg.threads::<Write>().get());
    println!("StreamArena mem: {}", cfg.mem::<StreamArena>());
    println!("StreamBuffer mem: {}", cfg.mem::<StreamBuffer>());

    // Test overridden value
    assert_eq!(*cfg.threads::<Read>(), BoundedU64::new(8).unwrap());

    // Test percentage defaults
    assert_eq!(*cfg.threads::<Debarcode>(), BoundedU64::new(4).unwrap());  // 25% of 16
    assert_eq!(*cfg.threads::<Write>(), BoundedU64::new(4).unwrap());  // 25% of 16

    // Memory budgets: 40% and 60% of 16 GiB
    let expected_arena = ByteSize::gib(16).as_u64() * 60 / 100;
    let expected_buffer = ByteSize::gib(16).as_u64() * 60 / 100;
    assert_eq!(cfg.mem::<StreamArena>().as_u64(), expected_arena);
    assert_eq!(cfg.mem::<StreamBuffer>().as_u64(), expected_buffer);

    // Test validation
    cfg.validate();
    
    // Test spawn
    let handles = cfg.spawn::<Read, _, _>(|| 42);
    assert_eq!(handles.len(), 8);

    for handle in handles {
        assert_eq!(handle.join().unwrap(), 42);
    }
}