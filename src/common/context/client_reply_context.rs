use crate::common::context::replies::IntoRawCtx;
use std::ffi::CString;
use std::os::raw::{c_char, c_long};
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    Status, VALKEYMODULE_POSTPONED_ARRAY_LEN, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
    raw,
};

/// A helper module for generating client replies. Its main reason for being is to minimize
/// allocations while returning large data to the client, as well as to provide convenience
/// methods over raw valkey calls.
///
/// ## Background
///
/// The valkey module `Context` struct provides a convenient `reply()` method for returning
/// structured data to the client. It ergonomically handles responses of various shapes, converting
/// them internally to proper protocol responses.
///
/// The cost of this is that structures have to be converted into the `ValkeyValue` enum, which can
/// involve a lot of unnecessary allocations and copying, especially for large data. For example, while
/// returning sample data, we can possibly return thousands or tens of thousands of items to the client.
/// The `reply()` method would require us to convert all of these items into `ValkeyValue` instances, which
/// can be expensive in terms of both time and memory.
///
/// For efficiency, we would prefer to write directly to the client using raw calls and avoid the overhead
/// of constructing large intermediate structures. However, the raw API is more challenging to use,
/// so we want to encapsulate it in a safe and convenient way.
///
/// Furthermore, in the context of a blocked client, this allows us to reply to the client without having to
/// construct a full `Context` struct, which would require a GIL lock.
///
pub(crate) struct ClientReplyContext {
    pub(crate) ctx: *mut raw::RedisModuleCtx,
}

impl ClientReplyContext {
    pub fn new<C: IntoRawCtx>(ctx: C) -> Self {
        Self {
            ctx: ctx.into_raw(),
        }
    }

    pub fn reply_error_string(&self, s: &str) -> Status {
        let msg = str_as_legal_resp_string(s);
        unsafe { raw::RedisModule_ReplyWithError.unwrap()(self.ctx, msg.as_ptr()).into() }
    }

    pub fn reply_with_string(&self, value: &ValkeyString) -> Status {
        raw::reply_with_string(self.ctx, value.inner)
    }

    pub fn reply_with_simple_string(&self, s: &str) -> Status {
        let msg = CString::new(s).unwrap_or_else(|_| {
            // Remove any interior NUL bytes to ensure CString::new cannot fail here.
            let sanitized: String = s.chars().filter(|c| *c != '\0').collect();
            CString::new(sanitized).unwrap()
        });
        raw::reply_with_simple_string(self.ctx, msg.as_ptr())
    }

    pub fn reply_with_bulk_string(&self, value: &str) -> Status {
        raw::reply_with_string_buffer(self.ctx, value.as_ptr().cast::<c_char>(), value.len())
    }
    pub fn reply_with_null(&self) -> Status {
        raw::reply_with_null(self.ctx)
    }

    pub fn reply_with_array(&self, len: usize) -> Status {
        raw::reply_with_array(self.ctx, len as c_long)
    }

    pub fn reply_with_postponed_array(&self) -> Status {
        raw::reply_with_array(self.ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long)
    }

    pub fn reply_with_map(&self, len: usize) -> Status {
        raw::reply_with_map(self.ctx, len as c_long)
    }

    pub fn reply_with_string_key(&self, value: &str) -> Status {
        raw::reply_with_string_buffer(self.ctx, value.as_ptr().cast::<c_char>(), value.len())
    }

    pub fn reply_with_i64(&self, value: i64) -> Status {
        raw::reply_with_long_long(self.ctx, value)
    }

    pub fn reply_with_usize(&self, value: usize) -> Status {
        raw::reply_with_long_long(self.ctx, value as i64)
    }

    pub fn reply_with_f64(&self, value: f64) -> Status {
        raw::reply_with_double(self.ctx, value)
    }

    pub fn reply_with_bool(&self, value: bool) -> Status {
        raw::reply_with_bool(self.ctx, value.into())
    }

    pub(crate) fn log_internal<L: Into<ValkeyLogLevel>>(&self, level: L, message: &str) {
        if cfg!(test) {
            return;
        }

        let level = CString::new(level.into().as_ref()).unwrap();
        let fmt = CString::new(message).unwrap();
        unsafe { raw::RedisModule_Log.unwrap()(self.ctx, level.as_ptr(), fmt.as_ptr()) }
    }

    pub fn reply_with_key(&self, result: ValkeyValueKey) -> Status {
        match result {
            ValkeyValueKey::Integer(i) => raw::reply_with_long_long(self.ctx, i),
            ValkeyValueKey::String(s) => {
                raw::reply_with_string_buffer(self.ctx, s.as_ptr().cast::<c_char>(), s.len())
            }
            ValkeyValueKey::BulkString(b) => {
                raw::reply_with_string_buffer(self.ctx, b.as_ptr().cast::<c_char>(), b.len())
            }
            ValkeyValueKey::BulkValkeyString(s) => self.reply_with_string(&s),
            ValkeyValueKey::Bool(b) => self.reply_with_bool(b),
        }
    }

    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, result: ValkeyResult) -> Status {
        match result {
            Ok(ValkeyValue::Bool(v)) => self.reply_with_bool(v),
            Ok(ValkeyValue::Integer(v)) => raw::reply_with_long_long(self.ctx, v),
            Ok(ValkeyValue::Float(v)) => raw::reply_with_double(self.ctx, v),
            Ok(ValkeyValue::SimpleStringStatic(s)) => self.reply_with_simple_string(s),

            Ok(ValkeyValue::SimpleString(s)) => self.reply_with_simple_string(&s),

            Ok(ValkeyValue::BulkString(s)) => {
                raw::reply_with_string_buffer(self.ctx, s.as_ptr().cast::<c_char>(), s.len())
            }

            Ok(ValkeyValue::BigNumber(s)) => {
                raw::reply_with_big_number(self.ctx, s.as_ptr().cast::<c_char>(), s.len())
            }

            Ok(ValkeyValue::VerbatimString((format, data))) => raw::reply_with_verbatim_string(
                self.ctx,
                data.as_ptr().cast(),
                data.len(),
                format.0.as_ptr().cast(),
            ),

            Ok(ValkeyValue::BulkValkeyString(s)) => self.reply_with_string(&s),

            Ok(ValkeyValue::StringBuffer(s)) => {
                raw::reply_with_string_buffer(self.ctx, s.as_ptr().cast::<c_char>(), s.len())
            }

            Ok(ValkeyValue::Array(array)) => {
                self.reply_with_array(array.len());

                for elem in array {
                    self.reply(Ok(elem));
                }

                Status::Ok
            }

            Ok(ValkeyValue::Map(map)) => {
                raw::reply_with_map(self.ctx, map.len() as c_long);

                for (key, value) in map {
                    self.reply_with_key(key);
                    self.reply(Ok(value));
                }

                Status::Ok
            }

            Ok(ValkeyValue::OrderedMap(map)) => {
                raw::reply_with_map(self.ctx, map.len() as c_long);

                for (key, value) in map {
                    self.reply_with_key(key);
                    self.reply(Ok(value));
                }

                Status::Ok
            }

            Ok(ValkeyValue::Set(set)) => {
                raw::reply_with_set(self.ctx, set.len() as c_long);
                set.into_iter().for_each(|e| {
                    self.reply_with_key(e);
                });

                Status::Ok
            }

            Ok(ValkeyValue::OrderedSet(set)) => {
                raw::reply_with_set(self.ctx, set.len() as c_long);
                set.into_iter().for_each(|e| {
                    self.reply_with_key(e);
                });

                Status::Ok
            }

            Ok(ValkeyValue::Null) => self.reply_with_null(),

            Ok(ValkeyValue::NoReply) => Status::Ok,

            Ok(ValkeyValue::StaticError(s)) => self.reply_error_string(s),

            Err(ValkeyError::WrongArity) => unsafe {
                raw::RedisModule_WrongArity.unwrap()(self.ctx).into()
            },

            Err(ValkeyError::WrongType) => {
                self.reply_error_string(ValkeyError::WrongType.to_string().as_str())
            }

            Err(ValkeyError::String(s)) => self.reply_error_string(s.as_str()),

            Err(ValkeyError::Str(s)) => self.reply_error_string(s),
        }
    }

    pub fn log_debug(&self, message: &str) {
        self.log_internal(ValkeyLogLevel::Debug, message);
    }

    pub fn log_notice(&self, message: &str) {
        self.log_internal(ValkeyLogLevel::Notice, message);
    }

    pub fn log_verbose(&self, message: &str) {
        self.log_internal(ValkeyLogLevel::Verbose, message);
    }

    pub fn log_warning(&self, message: &str) {
        self.log_internal(ValkeyLogLevel::Warning, message);
    }
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
