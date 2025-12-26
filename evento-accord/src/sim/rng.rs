//! Deterministic random number generator for simulation.
//!
//! Provides reproducible randomness using a seeded PRNG.
//! Same seed always produces the same sequence.

use std::cell::RefCell;
use std::time::Duration;

/// Seeded pseudo-random number generator for deterministic simulation.
///
/// Uses xorshift64 algorithm - simple, fast, and sufficient for testing.
/// The same seed will always produce the same sequence of values.
pub struct SimRng {
    state: RefCell<u64>,
}

impl SimRng {
    /// Create a new RNG with the given seed.
    ///
    /// Seed of 0 is automatically converted to 1 (xorshift requires non-zero state).
    pub fn new(seed: u64) -> Self {
        let seed = if seed == 0 { 1 } else { seed };
        Self {
            state: RefCell::new(seed),
        }
    }

    /// Generate next random u64.
    pub fn next_u64(&self) -> u64 {
        let mut state = self.state.borrow_mut();
        // xorshift64 algorithm
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// Generate random u32.
    pub fn next_u32(&self) -> u32 {
        self.next_u64() as u32
    }

    /// Generate random usize in range [0, max).
    pub fn next_usize(&self, max: usize) -> usize {
        if max == 0 {
            0
        } else {
            (self.next_u64() as usize) % max
        }
    }

    /// Generate random f64 in range [0.0, 1.0).
    pub fn next_f64(&self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64)
    }

    /// Generate random bool with given probability of being true.
    pub fn next_bool(&self, probability: f64) -> bool {
        self.next_f64() < probability
    }

    /// Generate random duration in range [min, max].
    pub fn next_duration(&self, min: Duration, max: Duration) -> Duration {
        if max <= min {
            return min;
        }
        let range_nanos = max.as_nanos() - min.as_nanos();
        let offset = ((self.next_f64() * range_nanos as f64) as u128) as u64;
        min + Duration::from_nanos(offset)
    }

    /// Generate random duration up to max.
    pub fn next_duration_up_to(&self, max: Duration) -> Duration {
        self.next_duration(Duration::ZERO, max)
    }

    /// Choose a random element from a slice.
    pub fn choose<'a, T>(&self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            None
        } else {
            Some(&items[self.next_usize(items.len())])
        }
    }

    /// Choose and remove a random element from a Vec.
    pub fn choose_remove<T>(&self, items: &mut Vec<T>) -> Option<T> {
        if items.is_empty() {
            None
        } else {
            let idx = self.next_usize(items.len());
            Some(items.swap_remove(idx))
        }
    }

    /// Shuffle a slice in place (Fisher-Yates algorithm).
    pub fn shuffle<T>(&self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.next_usize(i + 1);
            items.swap(i, j);
        }
    }

    /// Generate a random subset of indices.
    pub fn random_subset(&self, len: usize, count: usize) -> Vec<usize> {
        if count >= len {
            return (0..len).collect();
        }

        let mut indices: Vec<usize> = (0..len).collect();
        self.shuffle(&mut indices);
        indices.truncate(count);
        indices.sort();
        indices
    }

    /// Fork the RNG to create an independent stream.
    ///
    /// Useful for parallel simulation branches.
    pub fn fork(&self) -> Self {
        // Use next_u64() to get a seed for the child, then call next_u64() again
        // to advance the parent so they diverge
        let child_seed = self.next_u64();
        let _ = self.next_u64(); // Advance parent past child's starting point
        Self::new(child_seed)
    }

    /// Get current state (for debugging/serialization).
    pub fn state(&self) -> u64 {
        *self.state.borrow()
    }
}

impl Clone for SimRng {
    fn clone(&self) -> Self {
        Self {
            state: RefCell::new(*self.state.borrow()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_seed_same_sequence() {
        let rng1 = SimRng::new(12345);
        let rng2 = SimRng::new(12345);

        for _ in 0..100 {
            assert_eq!(rng1.next_u64(), rng2.next_u64());
        }
    }

    #[test]
    fn test_different_seed_different_sequence() {
        let rng1 = SimRng::new(12345);
        let rng2 = SimRng::new(54321);

        // Very unlikely to be equal
        assert_ne!(rng1.next_u64(), rng2.next_u64());
    }

    #[test]
    fn test_zero_seed_works() {
        let rng = SimRng::new(0);
        // Should not panic and should produce values
        let _ = rng.next_u64();
    }

    #[test]
    fn test_next_f64_range() {
        let rng = SimRng::new(42);
        for _ in 0..1000 {
            let v = rng.next_f64();
            assert!(v >= 0.0 && v < 1.0);
        }
    }

    #[test]
    fn test_next_bool_probability() {
        let rng = SimRng::new(42);
        let mut count = 0;
        let trials = 10000;

        for _ in 0..trials {
            if rng.next_bool(0.3) {
                count += 1;
            }
        }

        // Should be roughly 30% (with some tolerance)
        let ratio = count as f64 / trials as f64;
        assert!(ratio > 0.25 && ratio < 0.35);
    }

    #[test]
    fn test_next_duration() {
        let rng = SimRng::new(42);
        let min = Duration::from_millis(10);
        let max = Duration::from_millis(100);

        for _ in 0..100 {
            let d = rng.next_duration(min, max);
            assert!(d >= min && d <= max);
        }
    }

    #[test]
    fn test_choose() {
        let rng = SimRng::new(42);
        let items = vec![1, 2, 3, 4, 5];

        for _ in 0..100 {
            let choice = rng.choose(&items);
            assert!(choice.is_some());
            assert!(items.contains(choice.unwrap()));
        }

        let empty: Vec<i32> = vec![];
        assert!(rng.choose(&empty).is_none());
    }

    #[test]
    fn test_shuffle() {
        let rng = SimRng::new(42);
        let mut items = vec![1, 2, 3, 4, 5];
        let original = items.clone();

        rng.shuffle(&mut items);

        // Same elements
        items.sort();
        assert_eq!(items, original);
    }

    #[test]
    fn test_shuffle_deterministic() {
        let mut items1 = vec![1, 2, 3, 4, 5];
        let mut items2 = vec![1, 2, 3, 4, 5];

        SimRng::new(42).shuffle(&mut items1);
        SimRng::new(42).shuffle(&mut items2);

        assert_eq!(items1, items2);
    }

    #[test]
    fn test_fork() {
        let rng = SimRng::new(42);
        let _ = rng.next_u64(); // Advance state

        let forked = rng.fork();

        // Forked RNG should produce different sequence than parent
        // (since it's seeded with a derived value)
        assert_ne!(rng.next_u64(), forked.next_u64());
    }

    #[test]
    fn test_random_subset() {
        let rng = SimRng::new(42);

        let subset = rng.random_subset(10, 5);
        assert_eq!(subset.len(), 5);
        for &i in &subset {
            assert!(i < 10);
        }

        // Should be sorted
        for i in 1..subset.len() {
            assert!(subset[i - 1] < subset[i]);
        }

        // Requesting more than available
        let subset = rng.random_subset(5, 10);
        assert_eq!(subset.len(), 5);
    }
}
