#[cfg(test)]
use crate::is_module_initialized;
use std::borrow::Cow;
use valkey_module::logging::ValkeyLogLevel;

/// The server logs a module's message by passing it to `printf` as the *format*
/// string, after converting it to a C string with a panic on interior NUL. Text
/// that came from a peer or a client (a key, a handler name, an error) could
/// therefore read stray arguments through a `%` directive, or abort the server
/// through a NUL. Escape both.
fn printf_safe(message: &str) -> Cow<'_, str> {
    if !message.contains(['%', '\0']) {
        return Cow::Borrowed(message);
    }
    Cow::Owned(message.replace('%', "%%").replace('\0', "\\0"))
}

fn log(level: ValkeyLogLevel, message: &str) {
    // if the module is not initialized, log to the console directly instead of possibly causing a panic
    #[cfg(test)]
    if !is_module_initialized() {
        match level {
            ValkeyLogLevel::Warning => eprintln!("{message}"),
            _ => println!("{message}"),
        }
        return;
    }
    valkey_module::logging::log(level, printf_safe(message));
}

pub fn log_notice<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Notice, message.as_ref());
}

pub fn log_debug<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Debug, message.as_ref());
}

pub fn log_warning<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Warning, message.as_ref());
}

pub fn log_error<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Warning, message.as_ref());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_printf_safe_escapes_directives_and_nul() {
        assert!(matches!(printf_safe("plain text"), Cow::Borrowed(_)));
        assert_eq!(printf_safe("handler %s%n"), "handler %%s%%n");
        assert_eq!(printf_safe("unknown\0handler"), "unknown\\0handler");
    }
}
