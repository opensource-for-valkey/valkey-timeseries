use crate::common::{MultiSample, Sample, Timestamp};
use crate::labels::Label;
use std::ffi::CString;
use std::os::raw::{c_char, c_long};
use valkey_module::{
    Context, Status, VALKEYMODULE_POSTPONED_ARRAY_LEN, ValkeyModule_ReplySetArrayLength,
    ValkeyModuleCtx, ValkeyResult, ValkeyString, raw,
};

/// A small trait that allows reply helpers to accept either a raw
/// `*mut raw::RedisModuleCtx` or a `&Context` and get the underlying raw
/// context pointer.
pub(crate) trait IntoRawCtx {
    fn into_raw(self) -> *mut raw::RedisModuleCtx;
}

impl IntoRawCtx for *mut raw::RedisModuleCtx {
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self
    }
}

impl IntoRawCtx for &Context {
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.ctx
    }
}

pub fn reply_with_str<C: IntoRawCtx>(ctx: C, s: &str) -> Status {
    let msg = CString::new(s).unwrap_or_else(|_| {
        // Remove any interior NUL bytes to ensure CString::new cannot fail here.
        let sanitized: String = s.chars().filter(|c| *c != '\0').collect();
        CString::new(sanitized).unwrap()
    });
    raw::reply_with_simple_string(ctx.into_raw(), msg.as_ptr())
}

pub fn reply_with_valkey_string<C: IntoRawCtx>(ctx: C, s: &ValkeyString) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_string(raw_ctx, s.inner)
}

/// Reply with a borrowed byte slice, without copying into a [`ValkeyValue`].
///
/// Use this on hot paths where you already hold a `&[u8]` (for example a
/// slice borrowed from an open key) and do not want to allocate a `Vec`
/// just to construct [`ValkeyValue::StringBuffer`]. Wraps
/// [`ValkeyModule_ReplyWithStringBuffer`](https://valkey.io/topics/modules-api-ref/#ValkeyModule_ReplyWithStringBuffer).
#[allow(clippy::must_use_candidate)]
pub fn reply_with_slice<C: IntoRawCtx>(ctx: C, s: &[u8]) -> Status {
    raw::reply_with_string_buffer(ctx.into_raw(), s.as_ptr().cast::<c_char>(), s.len())
}

pub fn reply_with_bulk_string<C: IntoRawCtx>(ctx: C, s: &str) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_string_buffer(raw_ctx, s.as_ptr().cast::<c_char>(), s.len())
}

pub fn reply_with_string_iter<C: IntoRawCtx>(ctx: C, v: impl Iterator<Item = String>) {
    let raw_ctx = ctx.into_raw();
    reply_with_postponed_array(raw_ctx);
    let mut len = 0;
    for s in v {
        reply_with_bulk_string(raw_ctx, &s);
        len += 1;
    }
    reply_with_array_len(raw_ctx, len);
}

pub fn reply_label_ex<C: IntoRawCtx>(ctx: C, label: &str, value: Option<&str>) {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, 2);
    reply_with_bulk_string(raw_ctx, label);
    if let Some(value) = value {
        reply_with_bulk_string(raw_ctx, value);
    } else {
        raw::reply_with_null(raw_ctx);
    }
}

pub fn reply_label<C: IntoRawCtx>(ctx: C, label: &str, value: &str) {
    let value = if value.is_empty() { None } else { Some(value) };
    reply_label_ex(ctx, label, value);
}

pub fn reply_with_labels<C: IntoRawCtx>(ctx: C, labels: &[Label]) {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, labels.len());
    for label in labels {
        reply_label(raw_ctx, &label.name, &label.value);
    }
}

pub fn reply_with_sample_ex<C: IntoRawCtx>(ctx: C, timestamp: Timestamp, value: f64) {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, 2);
    reply_with_integer(raw_ctx, timestamp);
    raw::reply_with_double(raw_ctx, value);
}

#[inline]
pub fn reply_with_sample<C: IntoRawCtx>(ctx: C, sample: &Sample) {
    reply_with_sample_ex(ctx, sample.timestamp, sample.value);
}

pub fn reply_with_samples<C: IntoRawCtx>(ctx: C, samples: impl Iterator<Item = Sample>) {
    let raw_ctx = ctx.into_raw();
    reply_with_postponed_array(raw_ctx);

    let mut len = 0;
    for sample in samples {
        reply_with_sample(raw_ctx, &sample);
        len += 1;
    }

    reply_with_array_len(raw_ctx, len);
}

/// One multi-aggregation row: `[timestamp, value_1, ..., value_n]` with one
/// value per aggregator, in the order the aggregators were specified.
pub fn reply_with_multi_sample<C: IntoRawCtx>(ctx: C, row: &MultiSample) {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, 1 + row.values.len());
    reply_with_integer(raw_ctx, row.timestamp);
    for value in &row.values {
        raw::reply_with_double(raw_ctx, *value);
    }
}

pub fn reply_with_multi_samples<C: IntoRawCtx, T: std::borrow::Borrow<MultiSample>>(
    ctx: C,
    rows: impl Iterator<Item = T>,
) {
    let raw_ctx = ctx.into_raw();
    reply_with_postponed_array(raw_ctx);

    let mut len = 0;
    for row in rows {
        reply_with_multi_sample(raw_ctx, row.borrow());
        len += 1;
    }

    reply_with_array_len(raw_ctx, len);
}

pub fn reply_with_integer<C: IntoRawCtx>(ctx: C, value: i64) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_long_long(raw_ctx, value)
}

pub fn reply_with_usize<C: IntoRawCtx>(ctx: C, value: usize) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_long_long(raw_ctx, value as i64)
}

pub fn reply_with_double<C: IntoRawCtx>(ctx: C, value: f64) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_double(raw_ctx, value)
}

pub fn reply_with_bool<C: IntoRawCtx>(ctx: C, value: bool) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_bool(raw_ctx, value.into())
}

fn str_as_legal_resp_string(s: &str) -> CString {
    let mut bytes = s.as_bytes().to_owned();
    for b in &mut bytes {
        if *b == b'\r' || *b == b'\n' || *b == b'\0' {
            *b = b' ';
        }
    }
    CString::new(bytes).unwrap()
}

pub fn reply_with_simple_string<C: IntoRawCtx>(ctx: C, s: &str) -> Status {
    let raw_ctx = ctx.into_raw();
    let msg = str_as_legal_resp_string(s);
    raw::reply_with_simple_string(raw_ctx, msg.as_ptr())
}

pub fn reply_error_string<C: IntoRawCtx>(ctx: C, s: &str) -> Status {
    let raw_ctx = ctx.into_raw();
    let msg = str_as_legal_resp_string(s);
    unsafe { raw::RedisModule_ReplyWithError.unwrap()(raw_ctx, msg.as_ptr()).into() }
}

pub fn reply_with_null<C: IntoRawCtx>(ctx: C) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_null(raw_ctx)
}

pub fn reply_with_map<C: IntoRawCtx>(ctx: C, len: usize) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_map(raw_ctx, len as c_long)
}

pub fn reply_with_array<C: IntoRawCtx>(ctx: C, len: usize) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_array(raw_ctx, len as c_long)
}

pub fn reply_with_array_len<C: IntoRawCtx>(ctx: C, len: usize) -> Status {
    let raw_ctx = ctx.into_raw() as *mut ValkeyModuleCtx;
    unsafe {
        ValkeyModule_ReplySetArrayLength
            .expect("ValkeyModule_ReplySetArrayLength function pointer not set")(
            raw_ctx,
            len as c_long,
        )
    }
    Status::Ok
}

pub fn reply_with_postponed_array<C: IntoRawCtx>(ctx: C) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_array(raw_ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long)
}

#[allow(clippy::must_use_candidate)]
pub fn reply<C: IntoRawCtx>(ctx: C, result: ValkeyResult) -> Status {
    let ctx = Context::new(ctx.into_raw());
    ctx.reply(result)
}
