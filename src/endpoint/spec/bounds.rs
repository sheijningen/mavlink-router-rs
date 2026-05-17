//! Range-check helpers used by the query-string applier (`query.rs`). Each
//! helper takes the already-parsed numeric value, the key name (for error
//! reporting), and the inclusive bounds; on out-of-range it builds
//! [`SpecError::InvalidQueryValue`] with a uniform `"must be in MIN..=MAX,
//! got N"` reason so operator-visible error text is consistent across every
//! knob the parser bounds-checks.

use super::error::SpecError;

/// Inclusive range check for `usize` knobs (capacities, byte counts).
pub(crate) fn check_usize_range(
    n: usize,
    key: &'static str,
    min: usize,
    max: usize,
) -> Result<usize, SpecError> {
    if n < min || n > max {
        Err(SpecError::InvalidQueryValue {
            key,
            reason: format!("must be in {min}..={max}, got {n}"),
        })
    } else {
        Ok(n)
    }
}

/// Inclusive range check for `u64` knobs (time intervals in ms / s).
pub(crate) fn check_u64_range(
    n: u64,
    key: &'static str,
    min: u64,
    max: u64,
) -> Result<u64, SpecError> {
    if n < min || n > max {
        Err(SpecError::InvalidQueryValue {
            key,
            reason: format!("must be in {min}..={max}, got {n}"),
        })
    } else {
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usize_in_range_passes_through() {
        assert_eq!(check_usize_range(5, "k", 1, 10).unwrap(), 5);
        assert_eq!(check_usize_range(1, "k", 1, 10).unwrap(), 1);
        assert_eq!(check_usize_range(10, "k", 1, 10).unwrap(), 10);
    }

    #[test]
    fn usize_below_min_rejected_with_bounds_in_reason() {
        let err = check_usize_range(0, "k", 1, 10).unwrap_err();
        match err {
            SpecError::InvalidQueryValue { key, reason } => {
                assert_eq!(key, "k");
                assert!(reason.contains("1..=10"), "reason was: {reason}");
                assert!(reason.contains("got 0"), "reason was: {reason}");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn usize_above_max_rejected_with_bounds_in_reason() {
        let err = check_usize_range(11, "k", 1, 10).unwrap_err();
        match err {
            SpecError::InvalidQueryValue { key, reason } => {
                assert_eq!(key, "k");
                assert!(reason.contains("1..=10"), "reason was: {reason}");
                assert!(reason.contains("got 11"), "reason was: {reason}");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn u64_in_range_passes_through() {
        assert_eq!(check_u64_range(500, "ms", 100, 1000).unwrap(), 500);
    }

    #[test]
    fn u64_out_of_range_rejected() {
        assert!(check_u64_range(99, "ms", 100, 1000).is_err());
        assert!(check_u64_range(1001, "ms", 100, 1000).is_err());
    }
}
