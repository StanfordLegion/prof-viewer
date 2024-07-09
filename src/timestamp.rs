use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Deserialize, Serialize)]
pub struct Timestamp(pub i64 /* ns */);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimestampParseError {
    InvalidValue,
    NoUnit,
    InvalidUnit,
}

impl Timestamp {
    pub fn parse(s: &str) -> Result<Timestamp, TimestampParseError> {
        let s = s.trim();
        let split_idx = s
            .find(|c| !(char::is_ascii_digit(&c) || c == '.'))
            .ok_or(TimestampParseError::NoUnit)?;

        let (value_s, unit_s) = s.split_at(split_idx);
        let value = value_s
            .parse::<f64>()
            .map_err(|_| TimestampParseError::InvalidValue)?;
        let unit = unit_s.trim().to_lowercase();

        let factor = match unit.as_str() {
            "ns" => 1,
            "us" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            _ => return Err(TimestampParseError::InvalidUnit),
        };

        Ok(Timestamp((value * factor as f64) as i64))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let units: TimestampUnits = (*self).into();
        write!(
            f,
            "{}",
            TimestampDisplay {
                timestamp: *self,
                units,
                include_units: true
            }
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Deserialize, Serialize)]
pub struct Interval {
    pub start: Timestamp,
    pub stop: Timestamp, // exclusive
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let units: TimestampUnits = (*self).into();
        let duration = Timestamp(self.duration_ns());
        let duration_units: TimestampUnits = duration.into();
        write!(
            f,
            "from {} to {} (duration: {})",
            TimestampDisplay {
                timestamp: self.start,
                units,
                include_units: false
            },
            TimestampDisplay {
                timestamp: self.stop,
                units,
                include_units: true
            },
            TimestampDisplay {
                timestamp: duration,
                units: duration_units,
                include_units: true
            }
        )
    }
}

impl Interval {
    pub fn new(start: Timestamp, stop: Timestamp) -> Self {
        Self { start, stop }
    }
    pub fn center(self) -> Timestamp {
        Timestamp(self.start.0 + self.duration_ns() / 2)
    }
    pub fn duration_ns(self) -> i64 {
        self.stop.0 - self.start.0
    }
    pub fn contains(self, point: Timestamp) -> bool {
        point >= self.start && point < self.stop
    }
    pub fn overlaps(self, other: Interval) -> bool {
        !(other.stop <= self.start || other.start >= self.stop)
    }
    pub fn intersection(self, other: Interval) -> Self {
        Self {
            start: Timestamp(self.start.0.max(other.start.0)),
            stop: Timestamp(self.stop.0.min(other.stop.0)),
        }
    }
    pub fn union(self, other: Interval) -> Self {
        Self {
            start: Timestamp(self.start.0.min(other.start.0)),
            stop: Timestamp(self.stop.0.max(other.stop.0)),
        }
    }
    // Convert a timestamp into [0,1] relative space
    pub fn unlerp(self, time: Timestamp) -> f32 {
        (time.0 - self.start.0) as f32 / (self.duration_ns() as f32)
    }
    // Convert [0,1] relative space into a timestamp
    pub fn lerp(self, value: f32) -> Timestamp {
        Timestamp((value * (self.duration_ns() as f32)).round() as i64 + self.start.0)
    }
    // Grow (shrink) interval by duration_ns on either side.
    pub fn grow(self, duration_ns: i64) -> Self {
        Self {
            start: Timestamp(self.start.0 - duration_ns),
            stop: Timestamp(self.stop.0 + duration_ns),
        }
    }
    // Translate interval by duration_ns on both sides.
    pub fn translate(self, duration_ns: i64) -> Self {
        Self {
            start: Timestamp(self.start.0 + duration_ns),
            stop: Timestamp(self.stop.0 + duration_ns),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimestampUnits {
    divisor: i64,
    digits_after_separator: i64,
    unit_name: &'static str,
}

impl From<Timestamp> for TimestampUnits {
    fn from(timestamp: Timestamp) -> TimestampUnits {
        // Time is stored in nanoseconds. But display in larger units if possible.
        let ns = timestamp.0;
        const NS_PER_NS: i64 = 1;
        const NS_PER_US: i64 = 1_000;
        const NS_PER_MS: i64 = 1_000_000;
        const NS_PER_S: i64 = 1_000_000_000;
        let divisor;
        let digits_after_separator;
        let unit_name;
        if ns >= NS_PER_S {
            divisor = NS_PER_S;
            digits_after_separator = 3;
            unit_name = "s";
        } else if ns >= NS_PER_MS {
            divisor = NS_PER_MS;
            digits_after_separator = 3;
            unit_name = "ms";
        } else if ns >= NS_PER_US {
            divisor = NS_PER_US;
            digits_after_separator = 3;
            unit_name = "us";
        } else {
            divisor = NS_PER_NS;
            digits_after_separator = 0;
            unit_name = "ns";
        }
        TimestampUnits {
            divisor,
            digits_after_separator,
            unit_name,
        }
    }
}

impl From<Interval> for TimestampUnits {
    fn from(interval: Interval) -> TimestampUnits {
        // Time is stored in nanoseconds. But display in larger units if possible.
        let ns = interval.stop.0;
        let duration = interval.duration_ns();
        const NS_PER_NS: i64 = 1;
        const NS_PER_US: i64 = 1_000;
        const NS_PER_MS: i64 = 1_000_000;
        const NS_PER_S: i64 = 1_000_000_000;
        let divisor;
        let digits_after_separator;
        let unit_name;
        if ns >= NS_PER_S {
            divisor = NS_PER_S;
            if duration >= NS_PER_MS {
                digits_after_separator = 3;
            } else if duration >= NS_PER_US {
                digits_after_separator = 6;
            } else {
                digits_after_separator = 9;
            }
            unit_name = "s";
        } else if ns >= NS_PER_MS {
            divisor = NS_PER_MS;
            if duration >= NS_PER_US {
                digits_after_separator = 3;
            } else {
                digits_after_separator = 6;
            }
            unit_name = "ms";
        } else if ns >= NS_PER_US {
            divisor = NS_PER_US;
            digits_after_separator = 3;
            unit_name = "us";
        } else {
            divisor = NS_PER_NS;
            digits_after_separator = 0;
            unit_name = "ns";
        }
        TimestampUnits {
            divisor,
            digits_after_separator,
            unit_name,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TimestampDisplay {
    pub timestamp: Timestamp,
    pub units: TimestampUnits,
    pub include_units: bool,
}

impl fmt::Display for TimestampDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let TimestampUnits {
            divisor,
            unit_name,
            digits_after_separator,
        } = self.units;
        let ns = self.timestamp.0;
        let units = ns / divisor;
        write!(f, "{units}")?;
        if digits_after_separator > 0 {
            write!(f, ".")?;
            let remainder = ns % divisor;
            if digits_after_separator >= 3 {
                let r0 = remainder / (divisor / 1_000);
                write!(f, "{r0:0>3}")?;
            }
            if digits_after_separator >= 6 {
                let r1 = remainder / (divisor / 1_000_000) % 1_000;
                write!(f, " {r1:0>3}")?;
            }
            if digits_after_separator >= 9 {
                let r2 = remainder / (divisor / 1_000_000_000) % 1_000;
                write!(f, " {r2:0>3}")?;
            }
        }
        if self.include_units {
            write!(f, " {unit_name}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod timestamp_parse {
        use super::*;

        #[test]
        fn test_s() {
            assert_eq!(Timestamp::parse("123.4 s"), Ok(Timestamp(123_400_000_000)));
        }

        #[test]
        fn test_ms() {
            assert_eq!(Timestamp::parse("234.5 ms"), Ok(Timestamp(234_500_000)));
        }

        #[test]
        fn test_us() {
            assert_eq!(Timestamp::parse("345.6 us"), Ok(Timestamp(345_600)));
        }

        #[test]
        fn test_ns() {
            assert_eq!(Timestamp::parse("567.0 ns"), Ok(Timestamp(567)));
        }

        #[test]
        fn test_s_upper() {
            assert_eq!(Timestamp::parse("123.4 S"), Ok(Timestamp(123_400_000_000)));
        }

        #[test]
        fn test_ms_upper() {
            assert_eq!(Timestamp::parse("234.5 MS"), Ok(Timestamp(234_500_000)));
        }

        #[test]
        fn test_us_upper() {
            assert_eq!(Timestamp::parse("345.6 US"), Ok(Timestamp(345_600)));
        }

        #[test]
        fn test_ns_upper() {
            assert_eq!(Timestamp::parse("567.0 NS"), Ok(Timestamp(567)));
        }

        #[test]
        fn test_s_nospace() {
            assert_eq!(Timestamp::parse("123.4s"), Ok(Timestamp(123_400_000_000)));
        }

        #[test]
        fn test_ms_nospace() {
            assert_eq!(Timestamp::parse("234.5ms"), Ok(Timestamp(234_500_000)));
        }

        #[test]
        fn test_us_nospace() {
            assert_eq!(Timestamp::parse("345.6us"), Ok(Timestamp(345_600)));
        }

        #[test]
        fn test_ns_nospace() {
            assert_eq!(Timestamp::parse("567.0ns"), Ok(Timestamp(567)));
        }

        #[test]
        fn test_s_spaces() {
            assert_eq!(
                Timestamp::parse("  123.4  s  "),
                Ok(Timestamp(123_400_000_000))
            );
        }

        #[test]
        fn test_ms_spaces() {
            assert_eq!(
                Timestamp::parse("  234.5  ms  "),
                Ok(Timestamp(234_500_000))
            );
        }

        #[test]
        fn test_us_spaces() {
            assert_eq!(Timestamp::parse("  345.6  us  "), Ok(Timestamp(345_600)));
        }

        #[test]
        fn test_ns_spaces() {
            assert_eq!(Timestamp::parse("  567.0  ns  "), Ok(Timestamp(567)));
        }

        #[test]
        fn test_no_unit() {
            assert_eq!(Timestamp::parse("500.0"), Err(TimestampParseError::NoUnit));
        }

        #[test]
        fn test_no_value() {
            assert_eq!(
                Timestamp::parse("ms"),
                Err(TimestampParseError::InvalidValue)
            );
        }

        #[test]
        fn test_invalid_unit() {
            assert_eq!(
                Timestamp::parse("500.0 foo"),
                Err(TimestampParseError::InvalidUnit)
            );
        }

        #[test]
        fn test_invalid_value() {
            assert_eq!(
                Timestamp::parse("foo ms"),
                Err(TimestampParseError::InvalidValue)
            );
        }

        #[test]
        fn test_invalid_value2() {
            assert_eq!(
                Timestamp::parse("500.0.0 ms"),
                Err(TimestampParseError::InvalidValue)
            );
        }

        #[test]
        fn test_invalid_value3() {
            assert_eq!(
                Timestamp::parse("500.0.0"),
                Err(TimestampParseError::NoUnit)
            );
        }

        #[test]
        fn test_extra() {
            assert_eq!(
                Timestamp::parse("500.0 ms asdfadf"),
                Err(TimestampParseError::InvalidUnit)
            );
        }
    }

    mod interval {
        use super::*;

        #[test]
        fn test_center() {
            let i0 = Interval::new(Timestamp(0), Timestamp(10));
            let i1 = Interval::new(Timestamp(0), Timestamp(0));
            assert_eq!(i0.center(), Timestamp(5));
            assert_eq!(i1.center(), Timestamp(0));
        }

        #[test]
        fn test_duration_ns() {
            let i0 = Interval::new(Timestamp(0), Timestamp(10));
            let i1 = Interval::new(Timestamp(0), Timestamp(0));
            assert_eq!(i0.duration_ns(), 10);
            assert_eq!(i1.duration_ns(), 0);
        }

        #[test]
        fn test_contains() {
            // Intervals are exclusive: they do NOT contain stop
            let i0 = Interval::new(Timestamp(2), Timestamp(4));
            assert!(!i0.contains(Timestamp(1)));
            assert!(i0.contains(Timestamp(2))); // Included
            assert!(i0.contains(Timestamp(3)));
            assert!(!i0.contains(Timestamp(4))); // Not included!
            assert!(!i0.contains(Timestamp(5)));
        }

        #[test]
        fn test_overlap() {
            // Intervals are exclusive: they do NOT contain stop
            let i0 = Interval::new(Timestamp(0), Timestamp(1));
            let i1 = Interval::new(Timestamp(1), Timestamp(2));
            assert!(!i0.overlaps(i1));
            assert!(!i1.overlaps(i0));

            let i2 = Interval::new(Timestamp(0), Timestamp(2));
            let i3 = Interval::new(Timestamp(1), Timestamp(3));
            assert!(i2.overlaps(i3));
            assert!(i3.overlaps(i2));

            // Non-empty intervals always overlap themselves
            assert!(i2.overlaps(i2));
            assert!(i3.overlaps(i3));

            // Empty intervals overlap nothing, not even themselves
            let i4 = Interval::new(Timestamp(4), Timestamp(4));
            assert!(!i4.overlaps(i4));
        }

        #[test]
        fn test_intersection() {
            let i0 = Interval::new(Timestamp(0), Timestamp(10));
            let i1 = Interval::new(Timestamp(5), Timestamp(15));
            assert_eq!(
                i0.intersection(i1),
                Interval::new(Timestamp(5), Timestamp(10))
            );
            assert_eq!(
                i1.intersection(i0),
                Interval::new(Timestamp(5), Timestamp(10))
            );
        }

        #[test]
        fn test_union() {
            let i0 = Interval::new(Timestamp(0), Timestamp(10));
            let i1 = Interval::new(Timestamp(5), Timestamp(15));
            assert_eq!(i0.union(i1), Interval::new(Timestamp(0), Timestamp(15)));
            assert_eq!(i1.union(i0), Interval::new(Timestamp(0), Timestamp(15)));
        }

        #[test]
        fn test_grow() {
            let i0 = Interval::new(Timestamp(5), Timestamp(10));
            let i1 = Interval::new(Timestamp(20), Timestamp(20));
            assert_eq!(i0.grow(2), Interval::new(Timestamp(3), Timestamp(12)));
            assert_eq!(i1.grow(2), Interval::new(Timestamp(18), Timestamp(22)));
        }

        #[test]
        fn test_translate() {
            let origin = Interval::new(Timestamp(1000), Timestamp(2000));
            let expect = Interval::new(Timestamp(1250), Timestamp(2250));
            assert_eq!(origin.translate(250), expect);

            let expect = Interval::new(Timestamp(750), Timestamp(1750));
            assert_eq!(origin.translate(-250), expect);
        }
    }

    mod timestamp_units_from_timestamp {
        use super::*;

        #[test]
        fn test_s() {
            let units: TimestampUnits = Timestamp(123_456_789_012).into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000_000,
                    unit_name: "s",
                    digits_after_separator: 3
                }
            );
        }

        #[test]
        fn test_ms() {
            let units: TimestampUnits = Timestamp(123_456_789).into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000,
                    unit_name: "ms",
                    digits_after_separator: 3
                }
            );
        }

        #[test]
        fn test_us() {
            let units: TimestampUnits = Timestamp(123_456).into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000,
                    unit_name: "us",
                    digits_after_separator: 3
                }
            );
        }

        #[test]
        fn test_ns() {
            let units: TimestampUnits = Timestamp(123).into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1,
                    unit_name: "ns",
                    digits_after_separator: 0
                }
            );
        }
    }

    mod timestamp_units_from_interval {
        use super::*;

        #[test]
        fn test_s() {
            let i0 = Interval::new(Timestamp(0), Timestamp(123_456_789_012));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000_000,
                    unit_name: "s",
                    digits_after_separator: 3
                }
            );
        }

        #[test]
        fn test_s_duration_ms() {
            let i0 = Interval::new(Timestamp(123_000_000_000), Timestamp(123_456_789_012));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000_000,
                    unit_name: "s",
                    digits_after_separator: 3
                }
            );
        }

        #[test]
        fn test_s_duration_us() {
            let i0 = Interval::new(Timestamp(123_456_000_000), Timestamp(123_456_789_012));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000_000,
                    unit_name: "s",
                    digits_after_separator: 6
                }
            );
        }

        #[test]
        fn test_s_duration_ns() {
            let i0 = Interval::new(Timestamp(123_456_789_000), Timestamp(123_456_789_012));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000_000,
                    unit_name: "s",
                    digits_after_separator: 9
                }
            );
        }

        #[test]
        fn test_ms() {
            let i0 = Interval::new(Timestamp(0), Timestamp(123_456_789));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000,
                    unit_name: "ms",
                    digits_after_separator: 3
                }
            );
        }

        #[test]
        fn test_ms_duration_us() {
            let i0 = Interval::new(Timestamp(123_000_000), Timestamp(123_456_789));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000,
                    unit_name: "ms",
                    digits_after_separator: 3
                }
            );
        }

        #[test]
        fn test_ms_duration_ns() {
            let i0 = Interval::new(Timestamp(123_456_000), Timestamp(123_456_789));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000_000,
                    unit_name: "ms",
                    digits_after_separator: 6
                }
            );
        }

        #[test]
        fn test_us() {
            let i0 = Interval::new(Timestamp(0), Timestamp(123_456));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1_000,
                    unit_name: "us",
                    digits_after_separator: 3
                }
            );
        }

        #[test]
        fn test_ns() {
            let i0 = Interval::new(Timestamp(123), Timestamp(456));
            let units: TimestampUnits = i0.into();
            assert_eq!(
                units,
                TimestampUnits {
                    divisor: 1,
                    unit_name: "ns",
                    digits_after_separator: 0
                }
            );
        }
    }

    mod timestamp_display {
        use super::*;

        #[test]
        fn test_s() {
            let t0 = Timestamp(123_456_789_012);
            assert_eq!(&format!("{}", t0), "123.456 s");
        }

        #[test]
        fn test_ms() {
            let t0 = Timestamp(123_456_789);
            assert_eq!(&format!("{}", t0), "123.456 ms");
        }

        #[test]
        fn test_us() {
            let t0 = Timestamp(123_456);
            assert_eq!(&format!("{}", t0), "123.456 us");
        }

        #[test]
        fn test_ns() {
            let t0 = Timestamp(123);
            assert_eq!(&format!("{}", t0), "123 ns");
        }
    }

    mod interval_display {
        use super::*;

        #[test]
        fn test_s() {
            let i0 = Interval::new(Timestamp(0), Timestamp(123_456_789_012));
            assert_eq!(
                &format!("{}", i0),
                "from 0.000 to 123.456 s (duration: 123.456 s)"
            );
        }

        #[test]
        fn test_s_duration_ms() {
            let i0 = Interval::new(Timestamp(123_000_000_000), Timestamp(123_456_789_012));
            assert_eq!(
                &format!("{}", i0),
                "from 123.000 to 123.456 s (duration: 456.789 ms)"
            )
        }

        #[test]
        fn test_s_duration_us() {
            let i0 = Interval::new(Timestamp(123_456_000_000), Timestamp(123_456_789_012));
            assert_eq!(
                &format!("{}", i0),
                "from 123.456 000 to 123.456 789 s (duration: 789.012 us)"
            );
        }

        #[test]
        fn test_s_duration_ns() {
            let i0 = Interval::new(Timestamp(123_456_789_000), Timestamp(123_456_789_012));
            assert_eq!(
                &format!("{}", i0),
                "from 123.456 789 000 to 123.456 789 012 s (duration: 12 ns)"
            );
        }

        #[test]
        fn test_ms() {
            let i0 = Interval::new(Timestamp(0), Timestamp(123_456_789));
            assert_eq!(
                &format!("{}", i0),
                "from 0.000 to 123.456 ms (duration: 123.456 ms)"
            );
        }

        #[test]
        fn test_ms_duration_us() {
            let i0 = Interval::new(Timestamp(123_000_000), Timestamp(123_456_789));
            assert_eq!(
                &format!("{}", i0),
                "from 123.000 to 123.456 ms (duration: 456.789 us)"
            );
        }

        #[test]
        fn test_ms_duration_ns() {
            let i0 = Interval::new(Timestamp(123_456_000), Timestamp(123_456_789));
            assert_eq!(
                &format!("{}", i0),
                "from 123.456 000 to 123.456 789 ms (duration: 789 ns)"
            );
        }

        #[test]
        fn test_us() {
            let i0 = Interval::new(Timestamp(0), Timestamp(123_456));
            assert_eq!(
                &format!("{}", i0),
                "from 0.000 to 123.456 us (duration: 123.456 us)"
            );
        }

        #[test]
        fn test_ns() {
            let i0 = Interval::new(Timestamp(0), Timestamp(123));
            assert_eq!(&format!("{}", i0), "from 0 to 123 ns (duration: 123 ns)");
        }
    }
}
