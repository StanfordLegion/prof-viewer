use crate::data::UtilPoint;
use crate::timestamp::{Interval, Timestamp};

/// Converts the step utilization into a sample utilization, where each
/// utilization point (sample) represents the average utilization over a
/// certain time interval. The sample is located in the middle of the
/// interval.
pub fn convert_step_to_sample_utilization(
    step_utilization: &[UtilPoint],
    interval: Interval,
    num_samples: u64,
) -> Vec<UtilPoint> {
    let start_time = interval.start.0;
    let duration = interval.duration_ns();
    let num_samples = num_samples as i64;

    let first_index = step_utilization
        .partition_point(|p| p.time < interval.start)
        .saturating_sub(1);

    let mut last_index =
        step_utilization[first_index..].partition_point(|p| p.time < interval.stop) + first_index;
    if last_index + 1 < step_utilization.len() {
        last_index += 1;
    }

    let mut utilization = Vec::new();
    let mut last_p = UtilPoint {
        time: Timestamp(0),
        util: 0.0,
    };
    let mut step_it = step_utilization[first_index..last_index].iter().peekable();
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
        utilization.push(UtilPoint {
            time: sample_interval.center(),
            util: sample_util as f32,
        });
    }
    utilization
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_steps1_samples1() {
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
        let result = convert_step_to_sample_utilization(&step_utilization, interval, num_samples);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_steps1_samples2() {
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
        let result = convert_step_to_sample_utilization(&step_utilization, interval, num_samples);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_steps1_samples3() {
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
        let result = convert_step_to_sample_utilization(&step_utilization, interval, num_samples);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_steps3_samples4() {
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
        let result = convert_step_to_sample_utilization(&step_utilization, interval, num_samples);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_interval_subset() {
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
        let result = convert_step_to_sample_utilization(&step_utilization, interval, num_samples);
        assert_eq!(result, expected);
    }
}
