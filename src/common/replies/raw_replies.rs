use crate::common::{MultiSample, Sample, Timestamp};
use crate::labels::Label;
use std::ffi::CString;
use std::os::raw::{c_char, c_long};
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    Context, ContextFlags, Status, VALKEYMODULE_POSTPONED_ARRAY_LEN,
    ValkeyModule_ReplySetArrayLength, ValkeyModuleCtx, ValkeyResult, raw,
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
    reply_with_counted_array(raw_ctx, samples, |sample| {
        reply_with_sample(raw_ctx, &sample);
    });
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
    reply_with_counted_array(raw_ctx, rows, |row| {
        reply_with_multi_sample(raw_ctx, row.borrow());
    });
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
    reply_with_counted_array(raw_ctx, rows, |row| {
        reply_with_pivot_row(raw_ctx, row.borrow());
    });
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
///
/// Takes any collection whose iterator knows its length, so a `&BTreeSet<String>`
/// or a slice is written as it is, without first being collected into a `Vec`.
/// The length is written as the reply header before any element, so this relies on
/// `ExactSizeIterator::len` being exact, which the standard collections guarantee. A
/// wrong length from a custom iterator would corrupt the reply stream, not just this reply.
pub fn reply_with_string_set<C, I>(ctx: C, values: I) -> Status
where
    C: IntoRawCtx,
    I: IntoIterator,
    I::IntoIter: ExactSizeIterator,
    I::Item: AsRef<str>,
{
    let raw_ctx = ctx.into_raw();
    let values = values.into_iter();
    if is_resp3_client(raw_ctx) {
        raw::reply_with_set(raw_ctx, values.len() as c_long);
    } else {
        reply_with_array(raw_ctx, values.len());
    }
    for value in values {
        reply_with_bulk_string(raw_ctx, value.as_ref());
    }
    Status::Ok
}

/// Reply with an array whose length is only known once every element has been written.
///
/// Opens a postponed-length array, calls `emit` once per item, then fixes the length with
/// `ValkeyModule_ReplySetArrayLength`. `emit` must write exactly one reply element per call —
/// the length is the number of items, not the number of calls to the reply API — so a row made of
/// several values has to be wrapped in its own array. Filtering belongs in `items`: skipped items
/// never reach `emit` and are not counted.
///
/// This is the only way to write a postponed array. Opening one and then writing a second array
/// header instead of setting the length leaves the first array's length unset and desynchronizes
/// the connection, so the open and close halves are private to this module.
pub fn reply_with_counted_array<C: IntoRawCtx, T>(
    ctx: C,
    items: impl IntoIterator<Item = T>,
    mut emit: impl FnMut(T),
) {
    let raw_ctx = ctx.into_raw();
    reply_with_postponed_array(raw_ctx);

    let mut len = 0;
    for item in items {
        emit(item);
        len += 1;
    }

    reply_with_array_len(raw_ctx, len);
}

fn reply_with_array_len<C: IntoRawCtx>(ctx: C, len: usize) -> Status {
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

fn reply_with_postponed_array<C: IntoRawCtx>(ctx: C) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_array(raw_ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long)
}

/// Reply with a simple string. `\r`, `\n` and NUL, which would end or corrupt it on the wire,
/// become spaces.
pub fn reply_with_simple_string<C: IntoRawCtx>(ctx: C, s: &str) -> Status {
    let msg = str_as_legal_resp_string(s);
    raw::reply_with_simple_string(ctx.into_raw(), msg.as_ptr())
}

fn reply_with_key(raw_ctx: *mut raw::RedisModuleCtx, key: ValkeyValueKey) -> Status {
    match key {
        ValkeyValueKey::Integer(i) => raw::reply_with_long_long(raw_ctx, i),
        ValkeyValueKey::String(s) => reply_with_bulk_string(raw_ctx, &s),
        ValkeyValueKey::BulkString(b) => reply_with_slice(raw_ctx, &b),
        ValkeyValueKey::BulkValkeyString(s) => raw::reply_with_string(raw_ctx, s.inner),
        ValkeyValueKey::Bool(b) => raw::reply_with_bool(raw_ctx, b.into()),
    }
}

/// Forward a [`ValkeyResult`] to the client.
///
/// `Context::reply` does the same, but its error path maps each `char` of the message to a byte
/// and garbles any non-ASCII text in it — a key name or label value echoed in an error. Errors
/// here go through [`reply_error_string`], which keeps UTF-8 intact.
#[allow(clippy::must_use_candidate)]
pub fn reply<C: IntoRawCtx>(ctx: C, result: ValkeyResult) -> Status {
    let raw_ctx = ctx.into_raw();
    match result {
        Ok(ValkeyValue::Bool(v)) => raw::reply_with_bool(raw_ctx, v.into()),
        Ok(ValkeyValue::Integer(v)) => raw::reply_with_long_long(raw_ctx, v),
        Ok(ValkeyValue::Float(v)) => raw::reply_with_double(raw_ctx, v),
        Ok(ValkeyValue::SimpleStringStatic(s)) => reply_with_simple_string(raw_ctx, s),
        Ok(ValkeyValue::SimpleString(s)) => reply_with_simple_string(raw_ctx, &s),
        Ok(ValkeyValue::BulkString(s)) => reply_with_bulk_string(raw_ctx, &s),
        Ok(ValkeyValue::BigNumber(s)) => {
            raw::reply_with_big_number(raw_ctx, s.as_ptr().cast::<c_char>(), s.len())
        }
        Ok(ValkeyValue::VerbatimString((format, data))) => raw::reply_with_verbatim_string(
            raw_ctx,
            data.as_ptr().cast(),
            data.len(),
            format.0.as_ptr().cast(),
        ),
        Ok(ValkeyValue::BulkValkeyString(s)) => raw::reply_with_string(raw_ctx, s.inner),
        Ok(ValkeyValue::StringBuffer(s)) => reply_with_slice(raw_ctx, &s),
        Ok(ValkeyValue::Array(array)) => {
            reply_with_array(raw_ctx, array.len());
            for elem in array {
                reply(raw_ctx, Ok(elem));
            }
            Status::Ok
        }
        Ok(ValkeyValue::Map(map)) => {
            reply_with_map(raw_ctx, map.len());
            for (key, value) in map {
                reply_with_key(raw_ctx, key);
                reply(raw_ctx, Ok(value));
            }
            Status::Ok
        }
        Ok(ValkeyValue::OrderedMap(map)) => {
            reply_with_map(raw_ctx, map.len());
            for (key, value) in map {
                reply_with_key(raw_ctx, key);
                reply(raw_ctx, Ok(value));
            }
            Status::Ok
        }
        Ok(ValkeyValue::Set(set)) => {
            raw::reply_with_set(raw_ctx, set.len() as c_long);
            for elem in set {
                reply_with_key(raw_ctx, elem);
            }
            Status::Ok
        }
        Ok(ValkeyValue::OrderedSet(set)) => {
            raw::reply_with_set(raw_ctx, set.len() as c_long);
            for elem in set {
                reply_with_key(raw_ctx, elem);
            }
            Status::Ok
        }
        Ok(ValkeyValue::Null) => raw::reply_with_null(raw_ctx),
        Ok(ValkeyValue::NoReply) => Status::Ok,
        Ok(ValkeyValue::StaticError(s)) => reply_error_string(raw_ctx, s),
        Err(ValkeyError::WrongArity) => {
            // A key-position request has no client to answer.
            if Context::new(raw_ctx).is_keys_position_request() {
                Status::Err
            } else {
                unsafe { raw::RedisModule_WrongArity.unwrap()(raw_ctx).into() }
            }
        }
        Err(ValkeyError::WrongType) => {
            reply_error_string(raw_ctx, ValkeyError::WrongType.to_string().as_str())
        }
        Err(ValkeyError::String(s)) => reply_error_string(raw_ctx, s.as_str()),
        Err(ValkeyError::Str(s)) => reply_error_string(raw_ctx, s),
    }
}
