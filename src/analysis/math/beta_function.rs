// Implementation of the Beta function and related methods in Rust
// Ported from: https://github.com/AndreyAkinshin/perfolizer
// License: Apache-2.0

use super::gamma_function::log_gamma;

pub fn beta_complete_log_value(a: f64, b: f64) -> f64 {
    log_gamma(a) + log_gamma(b) - log_gamma(a + b)
}

/// Regularized incomplete beta function Ix(a, b)
pub fn beta_regularized_incomplete_value(a: f64, b: f64, x: f64) -> f64 {
    // The implementation is inspired by "Incomplete Beta Function in C" (Lewis Van Winkle, 2017)
    // https://codeplea.com/incomplete-beta-function-c

    debug_assert!(a >= 0.0);
    debug_assert!(b >= 0.0);

    let eps = 1e-8;
    if x < eps {
        return 0.0;
    }
    if x > 1.0 - eps {
        return 1.0;
    }
    if a < eps && b < eps {
        return 0.5;
    }
    if a < eps {
        return 1.0;
    }
    if b < eps {
        return 0.0;
    }

    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - beta_regularized_incomplete_value(b, a, 1.0 - x);
    }

    // Lentz's algorithm for continued fraction
    fn normalize(z: f64) -> f64 {
        if z.abs() < 1e-30 { 1e-30 } else { z }
    }

    let max_iteration_count = 300;
    let mut u = 1.0;
    let mut v = 0.0;
    let mut f = 1.0;

    for i in 0..=max_iteration_count {
        let m = (i / 2) as f64;
        let d = if i == 0 {
            1.0
        } else if i % 2 == 0 {
            m * (b - m) * x / ((a + 2.0 * m - 1.0) * (a + 2.0 * m))
        } else {
            -((a + m) * (a + b + m) * x) / ((a + 2.0 * m) * (a + 2.0 * m + 1.0))
        };

        u = normalize(1.0 + d / u);
        v = 1.0 / normalize(1.0 + d * v);
        let uv = u * v;
        f *= uv;

        if (uv - 1.0).abs() < eps {
            break;
        }
    }

    // Ix(a, b) = x^a * (1-x)^b / (a*B(a, b)) * (f - 1)
    ((x.ln() * a + (1.0 - x).ln() * b - beta_complete_log_value(a, b)).exp() / a) * (f - 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-6;

    #[test]
    pub fn test_beta_complete_log_value() {
        for a in 1..=20 {
            for b in 1..=20 {
                let actual = beta_complete_log_value(a as f64, b as f64);
                let expected = (factorial((a - 1) as f64) * factorial((b - 1) as f64)
                    / factorial((a + b - 1) as f64))
                .ln();
                assert!((expected - actual).abs() < EPSILON);
            }
        }
    }

    fn factorial(n: f64) -> f64 {
        let mut result = 1.0;
        for i in 2..=(n as u64) {
            result *= i as f64;
        }
        result
    }
}
