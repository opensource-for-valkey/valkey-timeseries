//! Port of `org.urbcomp.startdb.compress.elf.utils.Elf64Utils`.
//!
//! All floating-point operations mirror the Java reference exactly so that the
//! produced bit stream is byte-for-byte compatible. Unsupported magnitudes (the
//! ones where the reference would throw) are reported via `Err` so the caller
//! can fall back to storing the raw value losslessly (Elf case `10`).

const F: [i32; 21] = [
    0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57, 60, 64, 67,
];

/// `10^i` for every `i` whose result is finite (`10^308` is the largest). Indexing past the
/// end is `inf`, which is what parsing `"1.0E309"` gave; the old 21-entry table fell back
/// to `format!` + `parse` for anything larger, and the significant-digit search below could
/// take that path 5,000 times for one tiny value (see `get_significant_count`).
const MAP_10IP: [f64; 309] = [
    1.0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31, 1e32,
    1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39, 1e40, 1e41, 1e42, 1e43, 1e44, 1e45, 1e46, 1e47, 1e48,
    1e49, 1e50, 1e51, 1e52, 1e53, 1e54, 1e55, 1e56, 1e57, 1e58, 1e59, 1e60, 1e61, 1e62, 1e63, 1e64,
    1e65, 1e66, 1e67, 1e68, 1e69, 1e70, 1e71, 1e72, 1e73, 1e74, 1e75, 1e76, 1e77, 1e78, 1e79, 1e80,
    1e81, 1e82, 1e83, 1e84, 1e85, 1e86, 1e87, 1e88, 1e89, 1e90, 1e91, 1e92, 1e93, 1e94, 1e95, 1e96,
    1e97, 1e98, 1e99, 1e100, 1e101, 1e102, 1e103, 1e104, 1e105, 1e106, 1e107, 1e108, 1e109, 1e110,
    1e111, 1e112, 1e113, 1e114, 1e115, 1e116, 1e117, 1e118, 1e119, 1e120, 1e121, 1e122, 1e123,
    1e124, 1e125, 1e126, 1e127, 1e128, 1e129, 1e130, 1e131, 1e132, 1e133, 1e134, 1e135, 1e136,
    1e137, 1e138, 1e139, 1e140, 1e141, 1e142, 1e143, 1e144, 1e145, 1e146, 1e147, 1e148, 1e149,
    1e150, 1e151, 1e152, 1e153, 1e154, 1e155, 1e156, 1e157, 1e158, 1e159, 1e160, 1e161, 1e162,
    1e163, 1e164, 1e165, 1e166, 1e167, 1e168, 1e169, 1e170, 1e171, 1e172, 1e173, 1e174, 1e175,
    1e176, 1e177, 1e178, 1e179, 1e180, 1e181, 1e182, 1e183, 1e184, 1e185, 1e186, 1e187, 1e188,
    1e189, 1e190, 1e191, 1e192, 1e193, 1e194, 1e195, 1e196, 1e197, 1e198, 1e199, 1e200, 1e201,
    1e202, 1e203, 1e204, 1e205, 1e206, 1e207, 1e208, 1e209, 1e210, 1e211, 1e212, 1e213, 1e214,
    1e215, 1e216, 1e217, 1e218, 1e219, 1e220, 1e221, 1e222, 1e223, 1e224, 1e225, 1e226, 1e227,
    1e228, 1e229, 1e230, 1e231, 1e232, 1e233, 1e234, 1e235, 1e236, 1e237, 1e238, 1e239, 1e240,
    1e241, 1e242, 1e243, 1e244, 1e245, 1e246, 1e247, 1e248, 1e249, 1e250, 1e251, 1e252, 1e253,
    1e254, 1e255, 1e256, 1e257, 1e258, 1e259, 1e260, 1e261, 1e262, 1e263, 1e264, 1e265, 1e266,
    1e267, 1e268, 1e269, 1e270, 1e271, 1e272, 1e273, 1e274, 1e275, 1e276, 1e277, 1e278, 1e279,
    1e280, 1e281, 1e282, 1e283, 1e284, 1e285, 1e286, 1e287, 1e288, 1e289, 1e290, 1e291, 1e292,
    1e293, 1e294, 1e295, 1e296, 1e297, 1e298, 1e299, 1e300, 1e301, 1e302, 1e303, 1e304, 1e305,
    1e306, 1e307, 1e308,
];

/// `10^-i` for every `i` whose result is non-zero (`10^-323` is the smallest subnormal
/// power of ten). Past the end is `0.0`, as the parse gave. Both tables are literal powers
/// of ten, which are bit-identical to the correctly rounded parse of `"1.0E±i"`.
const MAP_10IN: [f64; 324] = [
    1.0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
    1e-15, 1e-16, 1e-17, 1e-18, 1e-19, 1e-20, 1e-21, 1e-22, 1e-23, 1e-24, 1e-25, 1e-26, 1e-27,
    1e-28, 1e-29, 1e-30, 1e-31, 1e-32, 1e-33, 1e-34, 1e-35, 1e-36, 1e-37, 1e-38, 1e-39, 1e-40,
    1e-41, 1e-42, 1e-43, 1e-44, 1e-45, 1e-46, 1e-47, 1e-48, 1e-49, 1e-50, 1e-51, 1e-52, 1e-53,
    1e-54, 1e-55, 1e-56, 1e-57, 1e-58, 1e-59, 1e-60, 1e-61, 1e-62, 1e-63, 1e-64, 1e-65, 1e-66,
    1e-67, 1e-68, 1e-69, 1e-70, 1e-71, 1e-72, 1e-73, 1e-74, 1e-75, 1e-76, 1e-77, 1e-78, 1e-79,
    1e-80, 1e-81, 1e-82, 1e-83, 1e-84, 1e-85, 1e-86, 1e-87, 1e-88, 1e-89, 1e-90, 1e-91, 1e-92,
    1e-93, 1e-94, 1e-95, 1e-96, 1e-97, 1e-98, 1e-99, 1e-100, 1e-101, 1e-102, 1e-103, 1e-104,
    1e-105, 1e-106, 1e-107, 1e-108, 1e-109, 1e-110, 1e-111, 1e-112, 1e-113, 1e-114, 1e-115, 1e-116,
    1e-117, 1e-118, 1e-119, 1e-120, 1e-121, 1e-122, 1e-123, 1e-124, 1e-125, 1e-126, 1e-127, 1e-128,
    1e-129, 1e-130, 1e-131, 1e-132, 1e-133, 1e-134, 1e-135, 1e-136, 1e-137, 1e-138, 1e-139, 1e-140,
    1e-141, 1e-142, 1e-143, 1e-144, 1e-145, 1e-146, 1e-147, 1e-148, 1e-149, 1e-150, 1e-151, 1e-152,
    1e-153, 1e-154, 1e-155, 1e-156, 1e-157, 1e-158, 1e-159, 1e-160, 1e-161, 1e-162, 1e-163, 1e-164,
    1e-165, 1e-166, 1e-167, 1e-168, 1e-169, 1e-170, 1e-171, 1e-172, 1e-173, 1e-174, 1e-175, 1e-176,
    1e-177, 1e-178, 1e-179, 1e-180, 1e-181, 1e-182, 1e-183, 1e-184, 1e-185, 1e-186, 1e-187, 1e-188,
    1e-189, 1e-190, 1e-191, 1e-192, 1e-193, 1e-194, 1e-195, 1e-196, 1e-197, 1e-198, 1e-199, 1e-200,
    1e-201, 1e-202, 1e-203, 1e-204, 1e-205, 1e-206, 1e-207, 1e-208, 1e-209, 1e-210, 1e-211, 1e-212,
    1e-213, 1e-214, 1e-215, 1e-216, 1e-217, 1e-218, 1e-219, 1e-220, 1e-221, 1e-222, 1e-223, 1e-224,
    1e-225, 1e-226, 1e-227, 1e-228, 1e-229, 1e-230, 1e-231, 1e-232, 1e-233, 1e-234, 1e-235, 1e-236,
    1e-237, 1e-238, 1e-239, 1e-240, 1e-241, 1e-242, 1e-243, 1e-244, 1e-245, 1e-246, 1e-247, 1e-248,
    1e-249, 1e-250, 1e-251, 1e-252, 1e-253, 1e-254, 1e-255, 1e-256, 1e-257, 1e-258, 1e-259, 1e-260,
    1e-261, 1e-262, 1e-263, 1e-264, 1e-265, 1e-266, 1e-267, 1e-268, 1e-269, 1e-270, 1e-271, 1e-272,
    1e-273, 1e-274, 1e-275, 1e-276, 1e-277, 1e-278, 1e-279, 1e-280, 1e-281, 1e-282, 1e-283, 1e-284,
    1e-285, 1e-286, 1e-287, 1e-288, 1e-289, 1e-290, 1e-291, 1e-292, 1e-293, 1e-294, 1e-295, 1e-296,
    1e-297, 1e-298, 1e-299, 1e-300, 1e-301, 1e-302, 1e-303, 1e-304, 1e-305, 1e-306, 1e-307, 1e-308,
    1e-309, 1e-310, 1e-311, 1e-312, 1e-313, 1e-314, 1e-315, 1e-316, 1e-317, 1e-318, 1e-319, 1e-320,
    1e-321, 1e-322, 1e-323,
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

/// `get10iP`: 10^i for i >= 0 (`inf` once it overflows).
#[inline]
fn get_10ip(i: i32) -> Result<f64, ()> {
    if i < 0 {
        return Err(());
    }
    Ok(MAP_10IP.get(i as usize).copied().unwrap_or(f64::INFINITY))
}

/// `get10iN`: 10^(-i) for i >= 0 (`0.0` once it underflows).
#[inline]
pub fn get_10in(i: i32) -> Result<f64, ()> {
    if i < 0 {
        return Err(());
    }
    Ok(MAP_10IN.get(i as usize).copied().unwrap_or(0.0))
}

/// `getSP`: the decimal "scale position" of `|v|` (floor of log10 magnitude).
///
/// A table scan, as in the reference. Deriving the scale from the binary exponent plus one
/// corrective compare was tried (2026-09-16) and measured 8 % slower on encode, 35 % on
/// erasable data: for values in the usual magnitudes this loop is one to three predicted
/// compares, cheaper than the exponent arithmetic and two table loads.
#[inline(always)]
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

    // Once `v * 10^i` is past `i64::MAX` the cast saturates and `temp_long as f64` can
    // never equal `temp` again (it only grows with `i`), so the search below would run to
    // its guard for nothing. For a value like `1e-300` that was 5,000 iterations, each one
    // formatting and parsing a power of ten: ~230 µs of main-thread time per sample, on the
    // default encoding, from any client. Bail out at once instead; the outcome (`Err`, the
    // value is stored raw) is what the exhausted guard produced.
    const SATURATES: f64 = 9_223_372_036_854_775_808.0; // 2^63: `i64::MAX as f64`
    let mut temp = v * get_10ip(i)?;
    if temp > SATURATES {
        return Err(());
    }
    let mut temp_long = temp as i64;
    let mut guard = 0u32;
    while temp_long as f64 != temp {
        i += 1;
        guard += 1;
        if guard > 5000 {
            return Err(());
        }
        temp = v * get_10ip(i)?;
        if temp > SATURATES {
            return Err(());
        }
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
#[inline(always)]
pub fn round_up(v: f64, alpha: i32) -> Result<f64, ()> {
    let scale = get_10ip(alpha)?;
    Ok(if v < 0.0 {
        (v * scale).floor() / scale
    } else {
        (v * scale).ceil() / scale
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tables are the correctly rounded powers of ten the string parse produced.
    #[test]
    fn power_tables_match_parsing() {
        for (i, entry) in MAP_10IP.iter().enumerate() {
            let parsed: f64 = format!("1.0E{i}").parse().unwrap();
            assert_eq!(entry.to_bits(), parsed.to_bits(), "10^{i}");
        }
        for (i, entry) in MAP_10IN.iter().enumerate() {
            let parsed: f64 = format!("1.0E-{i}").parse().unwrap();
            assert_eq!(entry.to_bits(), parsed.to_bits(), "10^-{i}");
        }
        assert_eq!(get_10ip(309), Ok(f64::INFINITY));
        assert_eq!(get_10in(324), Ok(0.0));
        assert_eq!(get_10ip(-1), Err(()));
        assert_eq!(get_10in(-1), Err(()));
    }

    /// A search whose product overflows must fail fast (the value is then stored raw), not
    /// run the 5,000-iteration guard. That is any magnitude below ~1e-292 on a fresh stream
    /// (`last_beta_star == MAX` starts the search at `10^(16 - sp)`, past `10^308`) and a
    /// subnormal from any state. Debug build, so the budget is generous: the old path took seconds here.
    #[test]
    fn overflowing_searches_fail_fast() {
        let cases = [
            (1e-300, i32::MAX),
            (1e-295, i32::MAX),
            (f64::MIN_POSITIVE, i32::MAX),
            (5e-324, i32::MAX),
            (5e-324, 0),
            (5e-324, 5),
            (5e-324, 17),
        ];
        let start = std::time::Instant::now();
        for _ in 0..5_000 {
            for (v, last) in cases {
                assert_eq!(
                    get_alpha_and_beta_star(v, last),
                    Err(()),
                    "{v:e} last {last}"
                );
            }
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() < 500,
            "40,000 overflowing searches took {elapsed:?}"
        );
    }

    /// With a known `beta_star` a tiny power of ten is a fast, legitimate erasure: the
    /// search starts near the right scale and never overflows.
    #[test]
    fn tiny_powers_of_ten_erase_when_the_scale_is_known() {
        assert_eq!(get_alpha_and_beta_star(1e-300, 0), Ok((300, 0)));
        assert_eq!(get_alpha_and_beta_star(1e-300, 5), Ok((300, 0)));
    }

    /// Values in the range that used to go through the string-parse fallback still get
    /// the same digit counts.
    #[test]
    fn small_magnitudes_still_erase() {
        assert_eq!(get_alpha_and_beta_star(1e-5, i32::MAX), Ok((5, 0)));
        assert_eq!(get_alpha_and_beta_star(1.5e-5, i32::MAX), Ok((6, 2)));
        assert_eq!(get_alpha_and_beta_star(1e-20, i32::MAX), Ok((20, 0)));
        assert_eq!(get_alpha_and_beta_star(123.456, i32::MAX), Ok((3, 6)));
    }
}
