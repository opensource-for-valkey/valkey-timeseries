use crate::common::{MultiSample, Sample, Timestamp};
use crate::labels::Label;
use std::ffi::CString;
use std::os::raw::{c_char, c_long};
use valkey_module::{
    Context, ContextFlags, Status, VALKEYMODULE_POSTPONED_ARRAY_LEN,
    ValkeyModule_ReplySetArrayLength, ValkeyModuleCtx, ValkeyResult, ValkeyString, raw,
};

/// True when the client this reply targets negotiated RESP3 (HELLO 3).
/// Commands whose RESP3 reply is structurally different from the RESP2 one
/// (e.g. TS.MRANGE's map-of-series form) branch on this.
pub fn is_resp3_client<C: IntoRawCtx>(ctx: C) -> bool {
    Context::new(ctx.into_raw())
        .get_flags()
        .contains(ContextFlags::FLAGS_RESP3)
}

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

pub fn reply_with_str(ctx: &Context, s: &str) -> Status {
    let msg = CString::new(s).unwrap_or_else(|_| {
        // Remove any interior NUL bytes to ensure CString::new cannot fail here.
        let sanitized: String = s.chars().filter(|c| *c != '\0').collect();
        CString::new(sanitized).unwrap()
    });
    raw::reply_with_simple_string(ctx.ctx, msg.as_ptr())
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

/// RESP3 form of a label set: a map of name -> value, with a null value for a
/// label that exists in the request but not on the series (SELECTED_LABELS).
pub fn reply_with_labels_map<'a, C: IntoRawCtx>(
    ctx: C,
    labels: impl ExactSizeIterator<Item = &'a Label>,
) {
    let raw_ctx = ctx.into_raw();
    reply_with_map(raw_ctx, labels.len());
    for label in labels {
        reply_with_bulk_string(raw_ctx, &label.name);
        if label.value.is_empty() {
            raw::reply_with_null(raw_ctx);
        } else {
            reply_with_bulk_string(raw_ctx, &label.value);
        }
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

/// One pivoted row: `[timestamp, [value, ...]]`.
///
/// The values are nested in their own array rather than flattened into the row as
/// [`reply_with_multi_sample`] does — TS.NRANGE's row spans several series, so the value list
/// is a unit of its own.
pub fn reply_with_pivot_row<C: IntoRawCtx>(ctx: C, row: &MultiSample) {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, 2);
    reply_with_integer(raw_ctx, row.timestamp);
    reply_with_array(raw_ctx, row.values.len());
    for value in &row.values {
        raw::reply_with_double(raw_ctx, *value);
    }
}

pub fn reply_with_pivot_rows<C: IntoRawCtx, T: std::borrow::Borrow<MultiSample>>(
    ctx: C,
    rows: impl Iterator<Item = T>,
) {
    let raw_ctx = ctx.into_raw();
    reply_with_postponed_array(raw_ctx);

    let mut len = 0;
    for row in rows {
        reply_with_pivot_row(raw_ctx, row.borrow());
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

/// Reply with a set of bulk strings.
///
/// RESP2 clients receive a regular array (the wire form of a set there); RESP3
/// clients receive a native set reply via `ValkeyModule_ReplyWithSet`. Used by
/// `TS.QUERYLABELS`, whose reply is a set of distinct label names or values.
pub fn reply_with_string_set<C: IntoRawCtx>(ctx: C, values: &[String]) -> Status {
    let raw_ctx = ctx.into_raw();
    if is_resp3_client(raw_ctx) {
        raw::reply_with_set(raw_ctx, values.len() as c_long);
    } else {
        reply_with_array(raw_ctx, values.len());
    }
    for value in values {
        reply_with_bulk_string(raw_ctx, value);
    }
    Status::Ok
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
