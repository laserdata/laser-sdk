use serde::{Deserialize, Serialize};

use crate::BenchError;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub struct PairedObservation {
    pub raw_throughput: f64,
    pub laser_throughput: f64,
    pub raw_latency_ns: f64,
    pub laser_latency_ns: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub struct ConfidenceInterval {
    pub estimate: f64,
    pub lower_95: f64,
    pub upper_95: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PairedAnalysis {
    pub pairs: usize,
    pub throughput_ratio: ConfidenceInterval,
    pub latency_ratio: ConfidenceInterval,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub struct C2Gate {
    pub throughput_lower_bound: f64,
    pub latency_upper_bound: f64,
    pub passed: bool,
}

/// Analyze paired Laser-to-raw ratios with a deterministic bootstrap over pair medians.
///
/// # Errors
///
/// Returns an error for fewer than two pairs, non-finite values, zero raw values, or too few bootstrap samples.
pub fn analyze_paired(
    observations: &[PairedObservation],
    bootstrap_samples: usize,
    seed: u64,
) -> Result<PairedAnalysis, BenchError> {
    if observations.len() < 2 || bootstrap_samples < 100 {
        return Err(BenchError::Invalid(
            "paired analysis requires at least two pairs and 100 bootstrap samples".to_owned(),
        ));
    }
    let mut throughput = Vec::with_capacity(observations.len());
    let mut latency = Vec::with_capacity(observations.len());
    for observation in observations {
        let values = [
            observation.raw_throughput,
            observation.laser_throughput,
            observation.raw_latency_ns,
            observation.laser_latency_ns,
        ];
        if values.iter().any(|value| !value.is_finite())
            || observation.raw_throughput <= 0.0
            || observation.raw_latency_ns <= 0.0
        {
            return Err(BenchError::Invalid(
                "paired observations must be finite with positive raw values".to_owned(),
            ));
        }
        throughput.push(observation.laser_throughput / observation.raw_throughput);
        latency.push(observation.laser_latency_ns / observation.raw_latency_ns);
    }
    Ok(PairedAnalysis {
        pairs: observations.len(),
        throughput_ratio: bootstrap_median(&throughput, bootstrap_samples, seed),
        latency_ratio: bootstrap_median(&latency, bootstrap_samples, seed ^ 0x9e37_79b9_7f4a_7c15),
    })
}

/// Evaluate the strict direct-streaming claim bounds.
#[must_use]
pub fn evaluate_c2(
    analysis: &PairedAnalysis,
    throughput_lower_bound: f64,
    latency_upper_bound: f64,
) -> C2Gate {
    C2Gate {
        throughput_lower_bound,
        latency_upper_bound,
        passed: analysis.throughput_ratio.lower_95 >= throughput_lower_bound
            && analysis.latency_ratio.upper_95 <= latency_upper_bound,
    }
}

fn bootstrap_median(values: &[f64], samples: usize, seed: u64) -> ConfidenceInterval {
    let mut random = XorShift64::new(seed);
    let mut distribution = Vec::with_capacity(samples);
    let mut sample = Vec::with_capacity(values.len());
    for _ in 0..samples {
        sample.clear();
        for _ in values {
            sample.push(values[random.index(values.len())]);
        }
        distribution.push(median(&mut sample));
    }
    distribution.sort_by(f64::total_cmp);
    let lower = percentile_index(samples, 25, 1_000);
    let upper = percentile_index(samples, 975, 1_000);
    let mut original = values.to_vec();
    ConfidenceInterval {
        estimate: median(&mut original),
        lower_95: distribution[lower],
        upper_95: distribution[upper],
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

fn percentile_index(length: usize, numerator: usize, denominator: usize) -> usize {
    length
        .saturating_mul(numerator)
        .div_ceil(denominator)
        .saturating_sub(1)
        .min(length - 1)
}

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn index(&mut self, length: usize) -> usize {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        usize::try_from(value % u64::try_from(length).expect("sample length should fit u64"))
            .expect("sample index should fit usize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_identical_paired_arms_when_analyzed_then_should_center_ratios_at_one() {
        let observations = vec![
            PairedObservation {
                raw_throughput: 1_000.0,
                laser_throughput: 1_000.0,
                raw_latency_ns: 10.0,
                laser_latency_ns: 10.0,
            };
            10
        ];
        let analysis =
            analyze_paired(&observations, 1_000, 42).expect("paired observations should analyze");
        assert!((analysis.throughput_ratio.estimate - 1.0).abs() < f64::EPSILON);
        assert!((analysis.latency_ratio.estimate - 1.0).abs() < f64::EPSILON);
        assert!(evaluate_c2(&analysis, 0.99, 1.01).passed);
    }

    #[test]
    fn given_repeatable_overhead_when_evaluated_then_should_fail_c2() {
        let observations = vec![
            PairedObservation {
                raw_throughput: 1_000.0,
                laser_throughput: 950.0,
                raw_latency_ns: 10.0,
                laser_latency_ns: 11.0,
            };
            10
        ];
        let analysis =
            analyze_paired(&observations, 1_000, 42).expect("paired observations should analyze");
        assert!(!evaluate_c2(&analysis, 0.99, 1.01).passed);
    }
}
