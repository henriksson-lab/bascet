use std::thread;
use std::time::Duration;

#[test]
fn close_voids_buffer() {
    let (items_tx, items_rx) = kanal::bounded::<u32>(8);
    items_tx.send(1).unwrap();
    items_tx.send(2).unwrap();
    items_tx.close().unwrap();
    assert!(items_rx.recv().is_err());
}

#[test]
fn keeper_holds_buffer_across_sender_drop() {
    let (items_tx, items_rx) = kanal::bounded::<u32>(8);
    let keeper = items_tx.clone();
    items_tx.send(1).unwrap();
    items_tx.send(2).unwrap();
    drop(items_tx);
    assert_eq!(items_rx.recv().unwrap(), 1);
    assert_eq!(items_rx.recv().unwrap(), 2);
    drop(keeper);
    assert!(items_rx.recv().is_err());
}

#[test]
fn close_wakes_blocked_sender() {
    let (items_tx, items_rx) = kanal::bounded::<u32>(1);
    items_tx.send(1).unwrap();
    let handle = thread::spawn(move || items_tx.send(2));
    thread::sleep(Duration::from_millis(50));
    items_rx.close().unwrap();
    assert!(handle.join().unwrap().is_err());
}
