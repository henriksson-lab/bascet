use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bascet_core::runtime::Pool;

#[test]
fn broadcast_reaches_every_thread_and_survives_panic() {
    let pool = Pool::spawn(0, 2, 0);

    pool.broadcast(|_| Box::new(|| panic!("worker panic")));

    let hits = Arc::new(AtomicU32::new(0));
    pool.broadcast(|_| {
        let hits = Arc::clone(&hits);
        Box::new(move || {
            hits.fetch_add(1, Ordering::Relaxed);
        })
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(hits.load(Ordering::Relaxed), 2);
}
