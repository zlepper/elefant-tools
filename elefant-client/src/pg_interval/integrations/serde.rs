use crate::pg_interval::Interval;
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::Formatter;

impl Serialize for Interval {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_postgres())
    }
}

impl<'de> Deserialize<'de> for Interval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(IntervalVisitor)
    }
}

struct IntervalVisitor;

impl Visitor<'_> for IntervalVisitor {
    type Value = Interval;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a string representing a Postgres interval")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match Interval::from_postgres(v) {
            Ok(interval) => Ok(interval),
            Err(e) => Err(Error::custom(format!(
                "Invalid Postgres interval string: {e:?}"
            ))),
        }
    }
}
