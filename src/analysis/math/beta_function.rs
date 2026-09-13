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

/// Regularized incomplete beta inverse value
pub fn beta_regularized_incomplete_inverse_value(a: f64, b: f64, p: f64) -> f64 {
    // Based on "Numerical Recipes", 3rd edition, p. 273

    debug_assert!(a >= 0.0);
    debug_assert!(b >= 0.0);

    if p <= 0.0 {
        return 0.0;
    }
    if p >= 1.0 {
        return 1.0;
    }

    let eps = 1e-8;
    let mut x;

    if a >= 1.0 && b >= 1.0 {
        let pp = if p < 0.5 { p } else { 1.0 - p };
        let t = (-2.0 * pp.ln()).sqrt();
        x = (2.30753 + t * 0.27061) / (1.0 + t * (0.99229 + t * 0.04481)) - t;
        if p < 0.5 {
            x = -x;
        }
        let al = (x * x - 3.0) / 6.0;
        let h = 2.0 / (1.0 / (2.0 * a - 1.0) + 1.0 / (2.0 * b - 1.0));
        let w = x * (al + h).sqrt() / h
            - (1.0 / (2.0 * b - 1.0) - 1.0 / (2.0 * a - 1.0)) * (al + 5.0 / 6.0 - 2.0 / (3.0 * h));
        x = a / (a + b * (2.0 * w).exp());
    } else {
        let lna = (a / (a + b)).ln();
        let lnb = (b / (a + b)).ln();
        let t = (a * lna).exp() / a;
        let u = (b * lnb).exp() / b;
        let w = t + u;
        x = if p < t / w {
            (a * w * p).powf(1.0 / a)
        } else {
            1.0 - (b * w * (1.0 - p)).powf(1.0 / b)
        };
    }

    let afac = -log_gamma(a) - log_gamma(b) + log_gamma(a + b);
    for iter in 0..10 {
        if x < eps || x > 1.0 - eps {
            return x;
        }
        let error = beta_regularized_incomplete_value(a, b, x) - p;
        let t = ((a - 1.0) * x.ln() + (b - 1.0) * (1.0 - x).ln() + afac).exp();
        let u = error / t;
        let denom = 1.0 - 0.5 * 1.0_f64.min(u * ((a - 1.0) / x - (b - 1.0) / (1.0 - x)));
        let t = u / denom;
        x -= t;

        if x <= 0.0 {
            x = 0.5 * (x + t);
        }
        if x >= 1.0 {
            x = 0.5 * (x + t + 1.0);
        }
        if t.abs() < eps * x && iter > 0 {
            break;
        }
    }

    x
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
