//! Addresses for the suites that open nodes over real TCP.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};

/// An address on `ip` that no other test of this run is given, free as it is
/// handed out.
///
/// Its port is below the range the system draws from for a socket bound to
/// port 0 or dialling out (32768 and up on Linux). A port a listener bound to
/// port 0 was given and let go is drawn again: another test of the binary
/// took it before the node bound it, or while the node was closed, and the
/// node failed with "address already in use".
pub fn free_addr(ip: &str) -> SocketAddr {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    // Two runs on one machine start apart.
    let base = std::process::id() % 200 * 100;
    loop {
        let port = 10_000 + (base + NEXT.fetch_add(1, Ordering::Relaxed)) % 20_000;
        let port = u16::try_from(port).expect("below 30000");
        // Held by something else on the machine: the next one.
        if let Ok(listener) = std::net::TcpListener::bind((ip, port)) {
            return listener.local_addr().unwrap();
        }
    }
}
