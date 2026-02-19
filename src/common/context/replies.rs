use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use std::os::raw::{c_char, c_long};
use std::{collections::BTreeSet, ffi::CString};
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    Context, Status, VALKEYMODULE_POSTPONED_ARRAY_LEN, ValkeyError, ValkeyResult, ValkeyValue, raw,
};

/// A small trait that allows reply helpers to accept either a raw
/// `*mut raw::RedisModuleCtx` or a `&Context` and get the underlying raw
/// context pointer.
pub trait IntoRawCtx {
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

pub fn reply_with_bulk_string<C: IntoRawCtx>(ctx: C, s: &str) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_string_buffer(raw_ctx, s.as_ptr().cast::<c_char>(), s.len())
}

pub fn reply_with_string_iter<C: IntoRawCtx>(ctx: C, v: impl Iterator<Item = String>) {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_array(raw_ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long);
    let mut len = 0;
    for s in v {
        reply_with_bulk_string(raw_ctx, &s);
        len += 1;
    }
    raw::reply_with_array(raw_ctx, len as c_long);
}

pub fn reply_with_btree_set<C: IntoRawCtx>(ctx: C, v: &BTreeSet<String>) {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, v.len());
    for s in v {
        reply_with_bulk_string(raw_ctx, s);
    }
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

pub fn reply_with_sample_ex<C: IntoRawCtx>(ctx: C, timestamp: Timestamp, value: f64) -> Status {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, 2);
    reply_with_i64(raw_ctx, timestamp);
    raw::reply_with_double(raw_ctx, value)
}

#[inline]
pub fn reply_with_sample<C: IntoRawCtx>(ctx: C, sample: &Sample) -> Status {
    reply_with_sample_ex(ctx, sample.timestamp, sample.value)
}

pub fn reply_with_samples<C: IntoRawCtx>(ctx: C, samples: impl Iterator<Item = Sample>) {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_array(raw_ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long);

    let mut len = 0;
    for sample in samples {
        reply_with_sample(raw_ctx, &sample);
        len += 1;
    }

    reply_with_array(raw_ctx, len);
}

pub fn reply_with_i64<C: IntoRawCtx>(ctx: C, value: i64) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_long_long(raw_ctx, value)
}

pub fn reply_with_usize<C: IntoRawCtx>(ctx: C, value: usize) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_long_long(raw_ctx, value as i64)
}

pub fn reply_with_f64<C: IntoRawCtx>(ctx: C, value: f64) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_double(raw_ctx, value)
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

pub fn reply_bulk_string<C: IntoRawCtx>(ctx: C, value: &str) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_string_buffer(raw_ctx, value.as_ptr().cast::<c_char>(), value.len())
}

pub fn reply_null<C: IntoRawCtx>(ctx: C) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_null(raw_ctx)
}

pub fn reply_with_array<C: IntoRawCtx>(ctx: C, len: usize) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_array(raw_ctx, len as c_long)
}

pub fn reply_with_postponed_array<C: IntoRawCtx>(ctx: C) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_array(raw_ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long)
}

pub fn reply_with_map<C: IntoRawCtx>(ctx: C, len: usize) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_map(raw_ctx, len as c_long)
}

pub fn reply_with_string_key<C: IntoRawCtx>(ctx: C, value: &str) -> Status {
    let raw_ctx = ctx.into_raw();
    raw::reply_with_string_buffer(raw_ctx, value.as_ptr().cast::<c_char>(), value.len())
}

pub fn reply_with_key<C: IntoRawCtx>(ctx: C, result: ValkeyValueKey) -> Status {
    let raw_ctx = ctx.into_raw();
    match result {
        ValkeyValueKey::Integer(i) => raw::reply_with_long_long(raw_ctx, i),
        ValkeyValueKey::String(s) => {
            raw::reply_with_string_buffer(raw_ctx, s.as_ptr().cast::<c_char>(), s.len())
        }
        ValkeyValueKey::BulkString(b) => {
            raw::reply_with_string_buffer(raw_ctx, b.as_ptr().cast::<c_char>(), b.len())
        }
        ValkeyValueKey::BulkValkeyString(s) => raw::reply_with_string(raw_ctx, s.inner),
        ValkeyValueKey::Bool(b) => raw::reply_with_bool(raw_ctx, b.into()),
    }
}

#[allow(clippy::must_use_candidate)]
pub fn reply<C: IntoRawCtx>(ctx: C, result: ValkeyResult) -> Status {
    let raw_ctx = ctx.into_raw();
    match result {
        Ok(ValkeyValue::Bool(v)) => raw::reply_with_bool(raw_ctx, v.into()),
        Ok(ValkeyValue::Integer(v)) => raw::reply_with_long_long(raw_ctx, v),
        Ok(ValkeyValue::Float(v)) => raw::reply_with_double(raw_ctx, v),
        Ok(ValkeyValue::SimpleStringStatic(s)) => {
            let msg = CString::new(s).unwrap();
            raw::reply_with_simple_string(raw_ctx, msg.as_ptr())
        }

        Ok(ValkeyValue::SimpleString(s)) => {
            let msg = CString::new(s).unwrap();
            raw::reply_with_simple_string(raw_ctx, msg.as_ptr())
        }

        Ok(ValkeyValue::BulkString(s)) => {
            raw::reply_with_string_buffer(raw_ctx, s.as_ptr().cast::<c_char>(), s.len())
        }

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

        Ok(ValkeyValue::StringBuffer(s)) => {
            raw::reply_with_string_buffer(raw_ctx, s.as_ptr().cast::<c_char>(), s.len())
        }

        Ok(ValkeyValue::Array(array)) => {
            raw::reply_with_array(raw_ctx, array.len() as c_long);

            for elem in array {
                reply(raw_ctx, Ok(elem));
            }

            Status::Ok
        }

        Ok(ValkeyValue::Map(map)) => {
            raw::reply_with_map(raw_ctx, map.len() as c_long);

            for (key, value) in map {
                reply_with_key(raw_ctx, key);
                reply(raw_ctx, Ok(value));
            }

            Status::Ok
        }

        Ok(ValkeyValue::OrderedMap(map)) => {
            raw::reply_with_map(raw_ctx, map.len() as c_long);

            for (key, value) in map {
                reply_with_key(raw_ctx, key);
                reply(raw_ctx, Ok(value));
            }

            Status::Ok
        }

        Ok(ValkeyValue::Set(set)) => {
            raw::reply_with_set(raw_ctx, set.len() as c_long);
            set.into_iter().for_each(|e| {
                reply_with_key(raw_ctx, e);
            });

            Status::Ok
        }

        Ok(ValkeyValue::OrderedSet(set)) => {
            raw::reply_with_set(raw_ctx, set.len() as c_long);
            set.into_iter().for_each(|e| {
                reply_with_key(raw_ctx, e);
            });

            Status::Ok
        }

        Ok(ValkeyValue::Null) => raw::reply_with_null(raw_ctx),

        Ok(ValkeyValue::NoReply) => Status::Ok,

        Ok(ValkeyValue::StaticError(s)) => reply_error_string(raw_ctx, s),

        Err(ValkeyError::WrongArity) => unsafe {
            raw::RedisModule_WrongArity.unwrap()(raw_ctx).into()
        },

        Err(ValkeyError::WrongType) => {
            reply_error_string(raw_ctx, ValkeyError::WrongType.to_string().as_str())
        }

        Err(ValkeyError::String(s)) => reply_error_string(raw_ctx, s.as_str()),

        Err(ValkeyError::Str(s)) => reply_error_string(raw_ctx, s),
    }
}
