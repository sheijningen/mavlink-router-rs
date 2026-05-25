//! Inclusive-range checks for numeric query-string knobs, surfacing a
//! uniform `"must be in MIN..=MAX, got N"` [`SpecError::InvalidQueryValue`].

use super::error::SpecError;

/// Inclusive range check for `u64` knobs (time intervals in ms / s).
pub(crate) fn check_u64_range(
    value: u64,
    key: &'static str,
    min: u64,
    max: u64,
) -> Result<u64, SpecError> {
    if value < min || value > max {
        Err(SpecError::InvalidQueryValue {
            key,
            reason: format!("must be in {min}..={max}, got {value}"),
        })
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_in_range_passes_through() {
        assert_eq!(check_u64_range(500, "ms", 100, 1000).unwrap(), 500);
    }

    #[test]
    fn u64_below_min_rejected_with_bounds_in_reason() {
        let err = check_u64_range(99, "ms", 100, 1000).unwrap_err();
        match err {
            SpecError::InvalidQueryValue { key, reason } => {
                assert_eq!(key, "ms");
                assert!(reason.contains("100..=1000"), "reason was: {reason}");
                assert!(reason.contains("got 99"), "reason was: {reason}");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn u64_above_max_rejected_with_bounds_in_reason() {
        let err = check_u64_range(1001, "ms", 100, 1000).unwrap_err();
        match err {
            SpecError::InvalidQueryValue { key, reason } => {
                assert_eq!(key, "ms");
                assert!(reason.contains("100..=1000"), "reason was: {reason}");
                assert!(reason.contains("got 1001"), "reason was: {reason}");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
