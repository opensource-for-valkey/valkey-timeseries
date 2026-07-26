//! Port of `org.urbcomp.startdb.compress.elf.utils.Elf64Utils`.
//!
//! All floating-point operations mirror the Java reference exactly so that the
//! produced bit stream is byte-for-byte compatible. Unsupported magnitudes (the
//! ones where the reference would throw) are reported via `Err` so the caller
//! can fall back to storing the raw value losslessly (Elf case `10`).

const F: [i32; 21] = [
    0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57, 60, 64, 67,
];

const MAP_10IP: [f64; 21] = [
    1.0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20,
];

const MAP_10IN: [f64; 21] = [
    1.0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
    1e-15, 1e-16, 1e-17, 1e-18, 1e-19, 1e-20,
];

const MAP_SP_GREATER_1: [f64; 10] = [1.0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9];

const MAP_SP_LESS_1: [f64; 11] = [
    1.0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10,
];

const LOG_2_10: f64 = std::f64::consts::LN_10 / std::f64::consts::LN_2;
// Note: the reference computes `Math.log(10)/Math.log(2)`; LN_10/LN_2 is the
// same quotient and only feeds `ceil`, so the result is identical.

/// `getFAlpha`: number of fraction bits needed to encode `alpha` decimal digits.
pub fn get_f_alpha(alpha: i32) -> i32 {
    if alpha < 0 {
        panic!("The argument should be greater than 0");
    }
    if alpha >= F.len() as i32 {
        (alpha as f64 * LOG_2_10).ceil() as i32
    } else {
        F[alpha as usize]
    }
}

/// `get10iP`: 10^i for i >= 0.
fn get_10ip(i: i32) -> Result<f64, ()> {
    if i < 0 {
        return Err(());
    }
    if i >= MAP_10IP.len() as i32 {
        Ok(format!("1.0E{}", i).parse::<f64>().unwrap())
    } else {
        Ok(MAP_10IP[i as usize])
    }
}

/// `get10iN`: 10^(-i) for i >= 0.
pub fn get_10in(i: i32) -> Result<f64, ()> {
    if i < 0 {
        return Err(());
    }
    if i >= MAP_10IN.len() as i32 {
        Ok(format!("1.0E-{}", i).parse::<f64>().unwrap())
    } else {
        Ok(MAP_10IN[i as usize])
    }
}

/// `getSP`: the decimal "scale position" of `|v|` (floor of log10 magnitude).
pub fn get_sp(v: f64) -> i32 {
    if v >= 1.0 {
        let mut i = 0;
        while i < MAP_SP_GREATER_1.len() as i32 - 1 {
            if v < MAP_SP_GREATER_1[(i + 1) as usize] {
                return i;
            }
            i += 1;
        }
    } else {
        let mut i = 1;
        while i < MAP_SP_LESS_1.len() as i32 {
            if v >= MAP_SP_LESS_1[i as usize] {
                return -i;
            }
            i += 1;
        }
    }
    (v.log10()).floor() as i32
}

/// `getSPAnd10iNFlag`: returns `(sp, is_exact_power_of_10_le_1)`.
fn get_sp_and_10in_flag(v: f64) -> (i32, i32) {
    if v >= 1.0 {
        let mut i = 0;
        while i < MAP_SP_GREATER_1.len() as i32 - 1 {
            if v < MAP_SP_GREATER_1[(i + 1) as usize] {
                return (i, 0);
            }
            i += 1;
        }
    } else {
        let mut i = 1;
        while i < MAP_SP_LESS_1.len() as i32 {
            if v >= MAP_SP_LESS_1[i as usize] {
                let flag = if v == MAP_SP_LESS_1[i as usize] { 1 } else { 0 };
                return (-i, flag);
            }
            i += 1;
        }
    }
    let log10v = v.log10();
    let sp = log10v.floor() as i32;
    let flag = if log10v == (log10v as i64) as f64 {
        1
    } else {
        0
    };
    (sp, flag)
}

/// `getSignificantCount`: number of decimal significant digits of `v`.
///
/// Returns `Err` for unsupported magnitudes (negative scale lookup) or if the
/// search exceeds a safety bound, so the caller can fall back to raw storage.
fn get_significant_count(v: f64, sp: i32, last_beta_star: i32) -> Result<i32, ()> {
    let mut i: i32;
    if last_beta_star != i32::MAX && last_beta_star != 0 {
        i = (last_beta_star - sp - 1).max(1);
    } else if last_beta_star == i32::MAX {
        i = 17 - sp - 1;
    } else if sp >= 0 {
        i = 1;
    } else {
        i = -sp;
    }

    let mut temp = v * get_10ip(i)?;
    let mut temp_long = temp as i64;
    let mut guard = 0u32;
    while temp_long as f64 != temp {
        i += 1;
        guard += 1;
        if guard > 5000 {
            return Err(());
        }
        temp = v * get_10ip(i)?;
        temp_long = temp as i64;
    }

    if temp / get_10ip(i)? != v {
        Ok(17)
    } else {
        while i > 0 && temp_long % 10 == 0 {
            i -= 1;
            temp_long /= 10;
        }
        Ok(sp + i + 1)
    }
}

/// `getAlphaAndBetaStar`: returns `(alpha, beta_star)`.
pub fn get_alpha_and_beta_star(v: f64, last_beta_star: i32) -> Result<(i32, i32), ()> {
    let v = if v < 0.0 { -v } else { v };
    let (sp, flag) = get_sp_and_10in_flag(v);
    let beta = get_significant_count(v, sp, last_beta_star)?;
    let alpha = beta - sp - 1;
    let beta_star = if flag == 1 { 0 } else { beta };
    Ok((alpha, beta_star))
}

/// `roundUp`: recover the original value from an erased `v_prime`.
pub fn round_up(v: f64, alpha: i32) -> Result<f64, ()> {
    let scale = get_10ip(alpha)?;
    Ok(if v < 0.0 {
        (v * scale).floor() / scale
    } else {
        (v * scale).ceil() / scale
    })
}
