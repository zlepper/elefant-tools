use crate::pg_interval::interval_norm::IntervalNorm;

impl IntervalNorm {
    /// Produces a postgres compliant interval string.
    pub fn into_postgres(self) -> String {
        if self.is_zeroed() {
            return "00:00:00".to_owned();
        }
        
        let mut interval = String::new();

        if self.years != 0 {
            interval.push_str(&self.years.to_string());
            interval.push_str(" year ");
        }

        if self.months != 0 {
            interval.push_str(&self.months.to_string());
            interval.push_str(" mons ");
        }

        if self.days != 0 {
            interval.push_str(&self.days.to_string());
            interval.push_str(" days ");
        }
        
        if self.is_time_present() {
            if !self.is_time_interval_pos() {
                interval.push('-');
            }

            let hours = super::pad_i64(self.hours);

            interval.push_str(&hours);
            interval.push(':');
            interval.push_str(&super::pad_i64(self.minutes));
            interval.push(':');
            interval.push_str(&super::pad_i64(self.seconds));

            if self.microseconds != 0 {
                interval.push_str(&format!(".{:06}", super::safe_abs_u64(self.microseconds)))
            }
        }
        
        interval.trim().to_owned()
    }
}