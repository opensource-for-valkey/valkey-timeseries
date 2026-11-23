use crate::error_consts;
use crate::series::TimeSeries;
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use std::ops::{Deref, DerefMut};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

pub struct SeriesGuard<'a> {
    series: *const TimeSeries,
    _marker: std::marker::PhantomData<&'a TimeSeries>,
}

impl<'a> SeriesGuard<'a> {
    pub(crate) fn from_key(ctx: &'a Context, key: &ValkeyString) -> ValkeyResult<Self> {
        let value_key = ctx.open_key(key);
        match value_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
            Ok(Some(series)) => {
                // Cast to raw pointer before value_key is dropped, mirroring SeriesGuardMut
                let ptr = series as *const TimeSeries;
                // SAFETY: ptr points into Valkey's keyspace, valid for 'a (tied to ctx lifetime)
                Ok(unsafe { SeriesGuard::from_raw(ptr) })
            }
            Ok(None) => Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
            Err(_e) => Err(ValkeyError::WrongType),
        }
    }

    /// # Safety
    /// The pointer must remain valid for lifetime 'a.
    unsafe fn from_raw(series: *const TimeSeries) -> Self {
        Self {
            series,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a> Deref for SeriesGuard<'a> {
    type Target = TimeSeries;

    fn deref(&self) -> &Self::Target {
        // SAFETY: pointer is valid for 'a as guaranteed by construction
        unsafe { &*self.series }
    }
}

impl<'a> AsRef<TimeSeries> for SeriesGuard<'a> {
    fn as_ref(&self) -> &TimeSeries {
        self.deref()
    }
}

pub struct SeriesGuardMut<'a> {
    pub(crate) series: &'a mut TimeSeries,
}

/// Whether a failed writable open ([`SeriesGuardMut::from_key`], and so `get_timeseries_mut` /
/// `try_get_timeseries_mut`) was refused because the key is in use, rather than missing or of
/// another type.
///
/// Valkey 9.2+ refuses a writable open of a key a forkless save is writing out, when the running
/// command did not declare it (a compaction destination, a key found by a selector) or no
/// command is running (a background task). The key is still there and still a series: a caller
/// must not treat the refusal as the key being gone (dropping a rule, marking an id stale). The
/// server only starts a forkless save when every module declares `HANDLE_FORKLESS`, which this
/// module does not yet, so today no open is refused.
pub fn is_key_in_use(err: &ValkeyError) -> bool {
    matches!(err, ValkeyError::Str(msg) if *msg == error_consts::KEY_IN_USE)
}

impl<'a> SeriesGuardMut<'a> {
    /// The series stored under `key`, borrowed writably for as long as `ctx` lives. The guard
    /// cannot outlive the context, so it cannot be kept past the command (or the GIL hold)
    /// that opened it:
    ///
    /// ```compile_fail
    /// use valkey_module::{Context, ValkeyString};
    /// use valkey_timeseries::series::SeriesGuardMut;
    ///
    /// fn leak(ctx: &Context, key: &ValkeyString) -> SeriesGuardMut<'static> {
    ///     SeriesGuardMut::from_key(ctx, key).unwrap()
    /// }
    /// ```
    ///
    /// Fails with [`error_consts::KEY_NOT_FOUND`] for a missing key, `WrongType` for another
    /// kind of value, and [`error_consts::KEY_IN_USE`] when the server refuses the writable open
    /// (see [`is_key_in_use`]).
    pub fn from_key(ctx: &'a Context, key: &ValkeyString) -> ValkeyResult<Self> {
        // A writable open fails only when the server returns no handle, which on Valkey 9.2+
        // means a forkless save holds the key; a missing key still gets a handle.
        let value_key = ctx
            .open_key_writable(key)
            .map_err(|_| ValkeyError::Str(error_consts::KEY_IN_USE))?;
        match value_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
            Ok(Some(series)) => Ok(SeriesGuardMut { series }),
            Ok(None) => Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
            Err(_e) => Err(ValkeyError::WrongType),
        }
    }

    pub fn get_series_mut(&mut self) -> &mut TimeSeries {
        self.deref_mut()
    }
}

impl Deref for SeriesGuardMut<'_> {
    type Target = TimeSeries;

    fn deref(&self) -> &Self::Target {
        self.series
    }
}

impl DerefMut for SeriesGuardMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.series
    }
}

#[cfg(test)]
mod tests {
    use crate::series::SeriesGuardMut;

    #[test]
    fn only_the_in_use_error_counts_as_in_use() {
        use super::is_key_in_use;
        use crate::error_consts;
        use valkey_module::ValkeyError;

        assert!(is_key_in_use(&ValkeyError::Str(error_consts::KEY_IN_USE)));
        assert!(!is_key_in_use(&ValkeyError::Str(
            error_consts::KEY_NOT_FOUND
        )));
        assert!(!is_key_in_use(&ValkeyError::WrongType));
    }

    #[test]
    fn series_guard_mut_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SeriesGuardMut<'static>>();
    }
}
