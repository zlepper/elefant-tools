use crate::pg_interval::interval_norm::IntervalNorm;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl Interval {
    /// Create a new instance of interval from the months, days, and microseconds.
    pub fn new(months: i32, days: i32, microseconds: i64) -> Interval {
        Interval {
            months,
            days,
            microseconds,
        }
    }

    /// Output the interval as a postgres interval string.
    pub fn to_postgres(self) -> String {
        IntervalNorm::from(self).into_postgres()
    }
}

#[cfg(test)]
mod tests {
    use super::Interval;

    #[test]
    fn test_new_interval_pos() {
        let interval = Interval::new(1, 1, 30);
        assert_eq!(interval.months, 1);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.microseconds, 30);
    }

    #[test]
    fn test_new_interval_neg() {
        let interval = Interval::new(-1, -1, -30);
        assert_eq!(interval.months, -1);
        assert_eq!(interval.days, -1);
        assert_eq!(interval.microseconds, -30);
    }

    #[test]
    fn test_clone() {
        let interval = Interval::new(1, 1, 30);
        let test_interval = interval;
        assert_eq!(interval, test_interval);
    }

    #[test]
    fn test_equality() {
        let interval = Interval::new(1, 1, 30);
        let different_interval = Interval::new(1, 3, 3);
        assert_ne!(interval, different_interval);
    }

    #[test]
    fn test_postgres_1() {
        let interval = Interval::new(12, 0, 0);
        let output = interval.to_postgres();
        assert_eq!(String::from("1 year"), output);
    }

    #[test]
    fn test_postgres_2() {
        let interval = Interval::new(13, 0, 0);
        let output = interval.to_postgres();
        assert_eq!(String::from("1 year 1 mons"), output);
    }

    #[test]
    fn test_postgres_3() {
        let interval = Interval::new(13, 1, 0);
        let output = interval.to_postgres();
        assert_eq!(String::from("1 year 1 mons 1 days"), output);
    }

    #[test]
    fn test_postgres_4() {
        let interval = Interval::new(13, 1, 3600000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("1 year 1 mons 1 days 01:00:00"), output);
    }

    #[test]
    fn test_postgres_5() {
        let interval = Interval::new(13, 1, 4200000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("1 year 1 mons 1 days 01:10:00"), output);
    }

    #[test]
    fn test_postgres_6() {
        let interval = Interval::new(13, 1, 4215000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("1 year 1 mons 1 days 01:10:15"), output);
    }

    #[test]
    fn test_postgres_7() {
        let interval = Interval::new(0, 0, 3600000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("01:00:00"), output);
    }

    #[test]
    fn test_postgres_8() {
        let interval = Interval::new(0, 0, 4200000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("01:10:00"), output);
    }

    #[test]
    fn test_postgres_9() {
        let interval = Interval::new(0, 0, 4215000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("01:10:15"), output);
    }

    #[test]
    fn test_postgres_10() {
        let interval = Interval::new(-12, 0, 0);
        let output = interval.to_postgres();
        assert_eq!(String::from("-1 year"), output);
    }

    #[test]
    fn test_postgres_11() {
        let interval = Interval::new(-13, 0, 0);
        let output = interval.to_postgres();
        assert_eq!(String::from("-1 year -1 mons"), output);
    }

    #[test]
    fn test_postgres_12() {
        let interval = Interval::new(-13, -1, 0);
        let output = interval.to_postgres();
        assert_eq!(String::from("-1 year -1 mons -1 days"), output);
    }

    #[test]
    fn test_postgres_13() {
        let interval = Interval::new(-13, -1, -3600000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("-1 year -1 mons -1 days -01:00:00"), output);
    }

    #[test]
    fn test_postgres_14() {
        let interval = Interval::new(-13, -1, -4200000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("-1 year -1 mons -1 days -01:10:00"), output);
    }

    #[test]
    fn test_postgres_16() {
        let interval = Interval::new(0, 0, -3600000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("-01:00:00"), output);
    }

    #[test]
    fn test_postgres_17() {
        let interval = Interval::new(0, 0, -4200000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("-01:10:00"), output);
    }

    #[test]
    fn test_postgres_18() {
        let interval = Interval::new(0, 0, -4215000000);
        let output = interval.to_postgres();
        assert_eq!(String::from("-01:10:15"), output);
    }

    #[test]
    fn test_postgres_19() {
        let interval = Interval::new(0, 3, 0);
        let output = interval.to_postgres();
        assert_eq!(String::from("3 days"), output);
    }
}
