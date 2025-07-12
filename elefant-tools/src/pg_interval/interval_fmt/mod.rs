mod postgres;

use std::ops::Neg;

/// Safely maps a i64 value to a unsigned number
/// without any overflow issues.
fn safe_abs_u64(mut num: i64) -> u64 {
    let max = i64::MAX;
    let max_min = max.neg();
    if num <= max_min {
        let result = max as u64;
        num += max;
        num *= -1;
        result + num as u64
    } else {
        num.unsigned_abs()
    }
}

/// Pads a i64 value with a width of 2.
fn pad_i64(val: i64) -> String {
    let num = if val < 0 {
        safe_abs_u64(val)
    } else {
        val as u64
    };
    format!("{num:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abs_safe_u64() {
        let min = i64::MIN;
        let actual = safe_abs_u64(min);
        let expected = 9_223_372_036_854_775_808;
        assert_eq!(actual, expected);
    }
}
