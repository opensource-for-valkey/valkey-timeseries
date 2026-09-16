use crate::common::logging::log_warning;
use crate::series::chunks::stream::traits::BitRead;

// These wrappers sit on the Gorilla decoder's per-bit path. The `format!` in the error arm
// used to live inline, which was enough to keep LLVM from inlining the whole wrapper -- so
// every prefix bit and every header field cost a call. The logging now lives in `#[cold]`
// helpers and the success path is a plain forwarded call.

#[cold]
#[inline(never)]
fn log_read_bool_error(e: &std::io::Error) {
    log_warning(format!("bitstream read_bool error: {e}"));
}

#[cold]
#[inline(never)]
fn log_read_bits_error(e: &std::io::Error) {
    log_warning(format!("bitstream read_bits error: {e}"));
}

#[inline(always)]
pub(crate) fn read_bool<R: BitRead>(reader: &mut R) -> std::io::Result<bool> {
    match reader.read_bit() {
        Ok(v) => Ok(v),
        Err(e) => {
            log_read_bool_error(&e);
            Err(e)
        }
    }
}

#[inline(always)]
pub(crate) fn read_bits<R: BitRead>(reader: &mut R, bits: u32) -> std::io::Result<u64> {
    match reader.read_bits(bits) {
        Ok(v) => Ok(v),
        Err(e) => {
            log_read_bits_error(&e);
            Err(e)
        }
    }
}
