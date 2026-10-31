//! Running an in-memory Turso future to completion from synchronous code.

use std::future::Future;
use std::task::{Context, Poll, Waker};

/// Polls `fut` to completion without a runtime. For a future over an
/// in-memory Turso database only: its I/O completes inside the poll that
/// issued it (`MemoryIO`), so every `Pending` is followed by progress. Lets
/// the simulation's synchronous oracles (`committed_set`, `events_of`, …)
/// read a Turso store as they read the in-memory ones.
pub fn block_inline<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        if let Poll::Ready(value) = fut.as_mut().poll(&mut cx) {
            return value;
        }
    }
    panic!("an in-memory Turso operation did not complete inline");
}
