use crate::commands::CommandArgIterator;
use crate::common::context::replies::*;
use crate::common::humanize::humanize_duration;
use crate::config::{CONFIG_VALUE_NONE, CONFIGS, ConfigDesc, ConfigValue};
use std::time::Duration;
use valkey_module::{Context, ValkeyError, ValkeyResult};

/// Emits one configuration value, choosing the reply type from the value itself.
fn reply_config_value(ctx: &Context, value: &ConfigValue) {
    let _ = match value {
        ConfigValue::Integer(v) => reply_with_integer(ctx, *v),
        ConfigValue::Float(v) => reply_with_double(ctx, *v),
        ConfigValue::Boolean(b) => reply_with_bulk_string(ctx, if *b { "yes" } else { "no" }),
        ConfigValue::String(s) => reply_with_bulk_string(ctx, s),
        ConfigValue::DurationMs(ms) => {
            let humanized = humanize_duration(&Duration::from_millis(*ms as u64));
            reply_with_bulk_string(ctx, &humanized)
        }
    };
}

/// The key/value pairs emitted for one parameter in VERBOSE mode, in reply order.
///
/// Returning the pairs rather than emitting them inline keeps the array length and the
/// emitted fields in lockstep — a field cannot be added without the count following it.
fn verbose_fields(desc: &ConfigDesc) -> Vec<(&'static str, Option<ConfigValue>)> {
    vec![
        ("name", Some(ConfigValue::str(desc.name))),
        ("type", Some(ConfigValue::str(desc.kind.as_str()))),
        ("default", Some(desc.default.clone())),
        ("min", desc.min.clone()),
        ("max", desc.max.clone()),
        ("value", Some((desc.read)())),
        ("description", Some(ConfigValue::str(desc.description))),
        ("mutable", Some(ConfigValue::Boolean(desc.is_mutable()))),
    ]
}

/// Emits a single config entry in verbose format as a flat key/value list, suitable for RESP3
/// maps or RESP2 arrays. `min`/`max` are reported as "none" for parameters that have no range.
fn reply_config_verbose(ctx: &Context, desc: &ConfigDesc) {
    let fields = verbose_fields(desc);
    reply_with_array(ctx, fields.len() * 2);

    for (key, value) in &fields {
        reply_with_str(ctx, key);
        match value {
            Some(value) => reply_config_value(ctx, value),
            None => {
                let _ = reply_with_bulk_string(ctx, CONFIG_VALUE_NONE);
            }
        }
    }
}

/// Lists configuration options.
///
/// Syntax: `TS._DEBUG LIST_CONFIGS [VERBOSE]`
///
/// Without VERBOSE, replies with a flat array of config names.
/// With VERBOSE, replies with an array of arrays, each containing key/value pairs for name,
/// type, default, min, max, value, description, and mutability. Every field is read from the
/// configuration registry, so the reply always covers exactly the parameters the module
/// registered.
pub(super) fn list_configs_cmd(ctx: &Context, itr: &mut CommandArgIterator) -> ValkeyResult<()> {
    let mut verbose = false;

    if let Some(arg) = itr.peek() {
        if arg.eq_ignore_ascii_case(b"verbose") {
            verbose = true;
            itr.next();
        } else {
            return Err(ValkeyError::Str(
                "Syntax error: unexpected argument, expected VERBOSE",
            ));
        }
    }

    reply_with_array(ctx, CONFIGS.len());

    for desc in CONFIGS {
        if verbose {
            reply_config_verbose(ctx, desc);
        } else {
            reply_with_bulk_string(ctx, desc.name);
        }
    }

    Ok(())
}
