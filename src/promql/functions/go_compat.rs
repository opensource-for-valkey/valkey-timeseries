//! Go compatible function to match prometheus semantics
//!
//! Go's math package development discussion confirms that accuracy was intentionally relaxed for
//! Sinh (along with Acos, Cosh, Exp, Exp2, and others) — essentially wherever exponential functions are
//! involved — prioritizing speed over correctness to the last ULP. We want to maintain compatibility
//! with Prometheus, so we implement copy `go`'s approach.
//!
//! See https://groups.google.com/g/golang-dev/c/WpAWBRFD6mI for more details.
/// Copyright 2009 The Go Authors. All rights reserved.
/// Use of this source code is governed by a BSD-style
/// license that can be found in the LICENSE file.
/*
    Floating-point hyperbolic sine and cosine.

    The exponential func is called for arguments
    greater in magnitude than 0.5.

    A series is used for arguments smaller in magnitude than 0.5.

    Cosh(x) is computed from the exponential func for
    all arguments.
*/
/// Sinh returns the hyperbolic sine of x.
///
/// Special cases are:
///
///    Sinh(±0) = ±0
///    Sinh(±Inf) = ±Inf
///    Sinh(NaN) = NaN
#[allow(clippy::excessive_precision)]
pub(super) fn sinh(mut x: f64) -> f64 {
    // The coefficients are #2029 from Hart & Cheney. (20.36D)
    const P0: f64 = -0.6307673640497716991184787251e+6_f64;
    const P1: f64 = -0.8991272022039509355398013511e+5_f64;
    const P2: f64 = -0.2894211355989563807284660366e+4_f64;
    const P3: f64 = -0.2630563213397497062819489e+2_f64;
    const Q0: f64 = -0.6307673640497716991212077277e+6_f64;
    const Q1: f64 = 0.1521517378790019070696485176e+5_f64;
    const Q2: f64 = -0.173678953558233699533450911e+3_f64;

    let mut sign = false;
    if x.is_sign_negative() {
        x = -x;
        sign = true
    }

    let mut temp = if x > 21.0 {
        x.exp() * 0.5
    } else if x > 0.5 {
        let ex = x.exp();
        (ex - 1.0 / ex) * 0.5
    } else {
        let x_sq = x * x;
        let mut temp = (((P3 * x_sq + P2) * x_sq + P1) * x_sq + P0) * x;
        temp /= ((x_sq + Q2) * x_sq + Q1) * x_sq + Q0;
        temp
    };

    if sign {
        temp = -temp
    }

    temp
}

/// Cosh returns the hyperbolic cosine of x.
///
/// Special cases are:
///
///    Cosh(±0) = 1
///    Cosh(±Inf) = +Inf
///    Cosh(NaN) = NaN
pub(super) fn cosh(mut x: f64) -> f64 {
    x = x.abs();
    if x > 21.0 {
        return x.exp() * 0.5;
    }
    let ex = x.exp();
    (ex + 1_f64 / ex) * 0.5
}
