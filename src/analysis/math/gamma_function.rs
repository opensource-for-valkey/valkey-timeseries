use std::f64::consts::PI;

pub fn log_gamma(x: f64) -> f64 {
    debug_assert!(x > 1e-5, "log_gamma: x should be positive");

    if x < 1.0 {
        return stirling_approximation_log(x + 3.0) - (x * (x + 1.0) * (x + 2.0)).ln();
    }
    if x < 2.0 {
        return stirling_approximation_log(x + 2.0) - (x * (x + 1.0)).ln();
    }
    if x < 3.0 {
        return stirling_approximation_log(x + 1.0) - x.ln();
    }

    stirling_approximation_log(x)
}

// sum = sum(b[2*n] / (2n * (2n-1) * x^(2n-1)))
fn get_series_value(x: f64) -> f64 {
    // Bernoulli numbers
    const B2: f64 = 1.0 / 6.0;
    const B4: f64 = -1.0 / 30.0;
    const B6: f64 = 1.0 / 42.0;
    const B8: f64 = -1.0 / 30.0;
    const B10: f64 = 5.0 / 66.0;

    B2 / 2.0 / x
        + B4 / 12.0 / (x * x * x)
        + B6 / 30.0 / (x * x * x * x * x)
        + B8 / 56.0 / (x * x * x * x * x * x * x)
        + B10 / 90.0 / (x * x * x * x * x * x * x * x * x)
}

fn stirling_approximation_log(x: f64) -> f64 {
    x * x.ln() - x + 0.5 * (2.0 * PI / x).ln() + get_series_value(x)
}

#[cfg(test)]
mod tests {
    use super::log_gamma;

    #[test]
    fn test_gamma_log_simple() {
        let log_gamma_5 = log_gamma(5.0);
        assert!((log_gamma_5 - 24.0f64.ln()).abs() < 1e-8);
    }
}
