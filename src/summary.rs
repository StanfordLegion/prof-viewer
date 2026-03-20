use crate::data::{SampleFormat, UtilPoint};
use crate::timestamp::{Interval, Timestamp};

pub fn slice_utilization(utilization: &[UtilPoint], interval: Interval) -> &[UtilPoint] {
    let first_index = utilization
        .partition_point(|p| p.time < interval.start)
        .saturating_sub(1);

    let mut last_index =
        utilization[first_index..].partition_point(|p| p.time < interval.stop) + first_index;
    if last_index + 1 < utilization.len() {
        last_index += 1;
    }

    &utilization[first_index..last_index]
}

/// Resamples the provided step (i.e., start-sampled) utilization to the
/// specified resolution and sample format.
pub fn resample_step_utilization(
    step_utilization: &[UtilPoint],
    interval: Interval,
    num_samples: u64,
    sample_format: SampleFormat,
) -> Vec<UtilPoint> {
    let start_time = interval.start.0;
    let duration = interval.duration_ns();
    let num_samples = num_samples as i64;

    let mut utilization = Vec::new();
    let mut last_p = UtilPoint {
        time: Timestamp(0),
        util: 0.0,
    };
    let mut step_it = slice_utilization(step_utilization, interval)
        .iter()
        .peekable();
    for sample in 0..num_samples {
        let sample_interval = Interval::new(
            Timestamp(duration * sample / num_samples + start_time),
            Timestamp(duration * (sample + 1) / num_samples + start_time),
        );
        if sample_interval.is_empty() {
            continue;
        }

        let mut sample_util = 0.0;
        while let Some(p) = step_it.next_if(|p| p.time < sample_interval.stop) {
            if p.time < sample_interval.start {
                last_p = *p;
                continue;
            }

            // This is a step utilization. So utilization p.util begins on time
            // p.time. That means the previous utilization stop at time p.time-1.
            let last_duration = Interval::new(last_p.time, Timestamp(p.time.0)) // - 1))
                .intersection(sample_interval)
                .duration_ns();
            sample_util += last_duration as f64 * last_p.util as f64;

            last_p = *p;
        }
        if last_p.time < sample_interval.stop {
            let last_duration = sample_interval.subtract_before(last_p.time).duration_ns();
            sample_util += last_duration as f64 * last_p.util as f64;
        }

        sample_util /= sample_interval.duration_ns() as f64;
        assert!((0.0..=1.0).contains(&sample_util));

        let sample_point = match sample_format {
            SampleFormat::Start => sample_interval.start,
            SampleFormat::Center => sample_interval.center(),
        };

        utilization.push(UtilPoint {
            time: sample_point,
            util: sample_util as f32,
        });
    }
    utilization
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_steps1_samples1() {
        let step_utilization = vec![UtilPoint {
            time: Timestamp(5),
            util: 1.0,
        }];
        let interval = Interval::new(Timestamp(0), Timestamp(10));
        let num_samples = 1;
        let expected = vec![UtilPoint {
            time: Timestamp(0),
            util: 0.5,
        }];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Start,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_center_steps1_samples1() {
        let step_utilization = vec![UtilPoint {
            time: Timestamp(5),
            util: 1.0,
        }];
        let interval = Interval::new(Timestamp(0), Timestamp(10));
        let num_samples = 1;
        let expected = vec![UtilPoint {
            time: Timestamp(5),
            util: 0.5,
        }];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Center,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_start_steps1_samples2() {
        let step_utilization = vec![UtilPoint {
            time: Timestamp(5),
            util: 1.0,
        }];
        let interval = Interval::new(Timestamp(0), Timestamp(10));
        let num_samples = 2;
        let expected = vec![
            UtilPoint {
                time: Timestamp(0),
                util: 0.0,
            },
            UtilPoint {
                time: Timestamp(5),
                util: 1.0,
            },
        ];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Start,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_center_steps1_samples2() {
        let step_utilization = vec![UtilPoint {
            time: Timestamp(5),
            util: 1.0,
        }];
        let interval = Interval::new(Timestamp(0), Timestamp(10));
        let num_samples = 2;
        let expected = vec![
            UtilPoint {
                time: Timestamp(2),
                util: 0.0,
            },
            UtilPoint {
                time: Timestamp(7),
                util: 1.0,
            },
        ];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Center,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_start_steps1_samples3() {
        let step_utilization = vec![UtilPoint {
            time: Timestamp(5),
            util: 1.0,
        }];
        let interval = Interval::new(Timestamp(0), Timestamp(10));
        let num_samples = 3;
        let expected = vec![
            UtilPoint {
                time: Timestamp(0),
                util: 0.0,
            },
            UtilPoint {
                time: Timestamp(3),
                util: 1.0 / 3.0,
            },
            UtilPoint {
                time: Timestamp(6),
                util: 1.0,
            },
        ];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Start,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_center_steps1_samples3() {
        let step_utilization = vec![UtilPoint {
            time: Timestamp(5),
            util: 1.0,
        }];
        let interval = Interval::new(Timestamp(0), Timestamp(10));
        let num_samples = 3;
        let expected = vec![
            UtilPoint {
                time: Timestamp(1),
                util: 0.0,
            },
            UtilPoint {
                time: Timestamp(4),
                util: 1.0 / 3.0,
            },
            UtilPoint {
                time: Timestamp(8),
                util: 1.0,
            },
        ];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Center,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_start_steps3_samples4() {
        let step_utilization = vec![
            UtilPoint {
                time: Timestamp(5),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(15),
                util: 0.0,
            },
        ];
        let interval = Interval::new(Timestamp(0), Timestamp(20));
        let num_samples = 4;
        let expected = vec![
            UtilPoint {
                time: Timestamp(0),
                util: 0.0,
            },
            UtilPoint {
                time: Timestamp(5),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(10),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(15),
                util: 0.0,
            },
        ];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Start,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_center_steps3_samples4() {
        let step_utilization = vec![
            UtilPoint {
                time: Timestamp(5),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(15),
                util: 0.0,
            },
        ];
        let interval = Interval::new(Timestamp(0), Timestamp(20));
        let num_samples = 4;
        let expected = vec![
            UtilPoint {
                time: Timestamp(2),
                util: 0.0,
            },
            UtilPoint {
                time: Timestamp(7),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(12),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(17),
                util: 0.0,
            },
        ];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Center,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_start_interval_subset() {
        let step_utilization = vec![
            UtilPoint {
                time: Timestamp(5),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(15),
                util: 0.0,
            },
            UtilPoint {
                time: Timestamp(25),
                util: 1.0,
            },
        ];
        let interval = Interval::new(Timestamp(10), Timestamp(20));
        let num_samples = 3;
        let expected = vec![
            UtilPoint {
                time: Timestamp(10),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(13),
                util: 2.0 / 3.0,
            },
            UtilPoint {
                time: Timestamp(16),
                util: 0.0,
            },
        ];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Start,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_center_interval_subset() {
        let step_utilization = vec![
            UtilPoint {
                time: Timestamp(5),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(15),
                util: 0.0,
            },
            UtilPoint {
                time: Timestamp(25),
                util: 1.0,
            },
        ];
        let interval = Interval::new(Timestamp(10), Timestamp(20));
        let num_samples = 3;
        let expected = vec![
            UtilPoint {
                time: Timestamp(11),
                util: 1.0,
            },
            UtilPoint {
                time: Timestamp(14),
                util: 2.0 / 3.0,
            },
            UtilPoint {
                time: Timestamp(18),
                util: 0.0,
            },
        ];
        let result = resample_step_utilization(
            &step_utilization,
            interval,
            num_samples,
            SampleFormat::Center,
        );
        assert_eq!(result, expected);
    }
}
