//! Virtual clock for deterministic simulation.
//!
//! Provides controlled time progression for testing distributed
//! systems without real-time delays.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Virtual clock for deterministic time control.
///
/// All time-based operations in the simulation use this clock
/// instead of real system time. Time only advances when explicitly
/// requested, making tests deterministic and fast.
#[derive(Clone)]
pub struct VirtualClock {
    /// Current virtual time in nanoseconds
    now_nanos: Arc<AtomicU64>,
}

impl VirtualClock {
    /// Create a new virtual clock starting at time zero.
    pub fn new() -> Self {
        Self {
            now_nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a virtual clock starting at a specific time.
    pub fn starting_at(time: Duration) -> Self {
        Self {
            now_nanos: Arc::new(AtomicU64::new(time.as_nanos() as u64)),
        }
    }

    /// Get current virtual time.
    pub fn now(&self) -> Duration {
        Duration::from_nanos(self.now_nanos.load(Ordering::SeqCst))
    }

    /// Advance time by the given duration.
    pub fn advance(&self, duration: Duration) {
        self.now_nanos
            .fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
    }

    /// Set time to a specific value.
    ///
    /// Warning: Setting time backwards may cause issues with timers.
    pub fn set(&self, time: Duration) {
        self.now_nanos
            .store(time.as_nanos() as u64, Ordering::SeqCst);
    }

    /// Get current time as milliseconds.
    pub fn now_millis(&self) -> u64 {
        self.now().as_millis() as u64
    }

    /// Advance time to a specific point (if in the future).
    pub fn advance_to(&self, time: Duration) {
        let current = self.now();
        if time > current {
            self.set(time);
        }
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Timer that fires at a specific virtual time.
#[derive(Clone)]
pub struct VirtualTimer {
    clock: VirtualClock,
    fire_at: Duration,
}

impl VirtualTimer {
    /// Create a timer that fires after the given delay.
    pub fn new(clock: VirtualClock, delay: Duration) -> Self {
        let fire_at = clock.now() + delay;
        Self { clock, fire_at }
    }

    /// Create a timer that fires at a specific time.
    pub fn at(clock: VirtualClock, time: Duration) -> Self {
        Self {
            clock,
            fire_at: time,
        }
    }

    /// Check if the timer is ready to fire.
    pub fn is_ready(&self) -> bool {
        self.clock.now() >= self.fire_at
    }

    /// Get remaining time until the timer fires.
    pub fn remaining(&self) -> Duration {
        self.fire_at.saturating_sub(self.clock.now())
    }

    /// Get the time when this timer will fire.
    pub fn fire_time(&self) -> Duration {
        self.fire_at
    }

    /// Reset the timer with a new delay from now.
    pub fn reset(&mut self, delay: Duration) {
        self.fire_at = self.clock.now() + delay;
    }
}

/// A collection of pending timers, ordered by fire time.
pub struct TimerQueue {
    clock: VirtualClock,
    timers: Vec<(Duration, u64)>, // (fire_at, id)
    next_id: u64,
}

impl TimerQueue {
    /// Create a new timer queue.
    pub fn new(clock: VirtualClock) -> Self {
        Self {
            clock,
            timers: Vec::new(),
            next_id: 0,
        }
    }

    /// Schedule a timer, returns timer ID.
    pub fn schedule(&mut self, delay: Duration) -> u64 {
        let id = self.next_id;
        self.next_id += 1;

        let fire_at = self.clock.now() + delay;
        self.timers.push((fire_at, id));
        self.timers.sort_by_key(|(t, _)| *t);

        id
    }

    /// Cancel a timer by ID.
    pub fn cancel(&mut self, id: u64) -> bool {
        if let Some(pos) = self.timers.iter().position(|(_, i)| *i == id) {
            self.timers.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get the next timer that's ready to fire.
    pub fn pop_ready(&mut self) -> Option<u64> {
        if let Some((fire_at, _)) = self.timers.first() {
            if *fire_at <= self.clock.now() {
                return Some(self.timers.remove(0).1);
            }
        }
        None
    }

    /// Get all ready timers.
    pub fn drain_ready(&mut self) -> Vec<u64> {
        let now = self.clock.now();
        let mut ready = Vec::new();

        while let Some((fire_at, _)) = self.timers.first() {
            if *fire_at <= now {
                ready.push(self.timers.remove(0).1);
            } else {
                break;
            }
        }

        ready
    }

    /// Get the time of the next scheduled timer.
    pub fn next_fire_time(&self) -> Option<Duration> {
        self.timers.first().map(|(t, _)| *t)
    }

    /// Check if any timers are pending.
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }

    /// Get count of pending timers.
    pub fn len(&self) -> usize {
        self.timers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_starts_at_zero() {
        let clock = VirtualClock::new();
        assert_eq!(clock.now(), Duration::ZERO);
    }

    #[test]
    fn test_clock_advance() {
        let clock = VirtualClock::new();
        clock.advance(Duration::from_millis(100));
        assert_eq!(clock.now(), Duration::from_millis(100));

        clock.advance(Duration::from_millis(50));
        assert_eq!(clock.now(), Duration::from_millis(150));
    }

    #[test]
    fn test_clock_set() {
        let clock = VirtualClock::new();
        clock.set(Duration::from_secs(10));
        assert_eq!(clock.now(), Duration::from_secs(10));
    }

    #[test]
    fn test_clock_clone_shares_time() {
        let clock1 = VirtualClock::new();
        let clock2 = clock1.clone();

        clock1.advance(Duration::from_millis(100));
        assert_eq!(clock2.now(), Duration::from_millis(100));
    }

    #[test]
    fn test_timer_not_ready_initially() {
        let clock = VirtualClock::new();
        let timer = VirtualTimer::new(clock.clone(), Duration::from_millis(100));

        assert!(!timer.is_ready());
        assert_eq!(timer.remaining(), Duration::from_millis(100));
    }

    #[test]
    fn test_timer_ready_after_advance() {
        let clock = VirtualClock::new();
        let timer = VirtualTimer::new(clock.clone(), Duration::from_millis(100));

        clock.advance(Duration::from_millis(100));
        assert!(timer.is_ready());
        assert_eq!(timer.remaining(), Duration::ZERO);
    }

    #[test]
    fn test_timer_queue() {
        let clock = VirtualClock::new();
        let mut queue = TimerQueue::new(clock.clone());

        let id1 = queue.schedule(Duration::from_millis(100));
        let id2 = queue.schedule(Duration::from_millis(50));
        let id3 = queue.schedule(Duration::from_millis(150));

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.next_fire_time(), Some(Duration::from_millis(50)));

        // No timers ready yet
        assert!(queue.pop_ready().is_none());

        // Advance past first timer
        clock.advance(Duration::from_millis(60));
        assert_eq!(queue.pop_ready(), Some(id2));
        assert!(queue.pop_ready().is_none());

        // Advance past remaining timers
        clock.advance(Duration::from_millis(100));
        let ready = queue.drain_ready();
        assert_eq!(ready.len(), 2);
        assert!(ready.contains(&id1));
        assert!(ready.contains(&id3));
    }

    #[test]
    fn test_timer_cancel() {
        let clock = VirtualClock::new();
        let mut queue = TimerQueue::new(clock.clone());

        let id1 = queue.schedule(Duration::from_millis(100));
        let _id2 = queue.schedule(Duration::from_millis(50));

        assert!(queue.cancel(id1));
        assert!(!queue.cancel(id1)); // Already cancelled

        assert_eq!(queue.len(), 1);
    }
}
