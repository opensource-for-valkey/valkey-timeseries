use crate::common::context::notify_keyspace_event;
use crate::error_consts;
use crate::series::{get_timeseries_mut, try_get_timeseries_mut};
use std::ffi::CStr;
use valkey_module::{AclPermissions, Context, VALKEY_OK, ValkeyError, ValkeyResult, ValkeyString};

const DELETERULE_SRC_EVENT: &CStr = c"ts.deleterule:src";
const DELETERULE_DEST_EVENT: &CStr = c"ts.deleterule:dest";

acl_categories!(TS_DELETERULE, "ts.deleterule", "write timeseries");
///
/// TS.DELETERULE sourceKey destKey
///
/// Deletes a compaction rule.
/// The user must be authorized to write to both sourceKey and destKey.
/// The rule is removed from the sourceKey, and the src_series field in destKey is cleared, but
/// the destination series is not deleted.
///
#[valkey_module_macros::command({
    name: "ts.deleterule",
    flags: [Write, DenyOOM],
    summary: "Delete a compaction rule between a source and destination time series.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: 3,
    key_spec: [{
        flags: [ReadWrite, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 })
    }]
})]
pub fn ts_deleterule_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Check for minimum number of arguments: command, sourceKey, destKey
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }

    let source_key = &args[1];
    let dest_key = &args[2];

    // A self-rule cannot exist, and opening the same key twice as mutable
    // guards would create two aliases to the same series value.
    if source_key == dest_key {
        return Err(ValkeyError::Str(error_consts::COMPACTION_RULE_NOT_FOUND));
    }

    // Get source time series (must exist, writable)
    let mut source_series = get_timeseries_mut(ctx, source_key, Some(AclPermissions::UPDATE))?;

    // Get the destination series. A destination that does not exist can not be the
    // target of a rule, so it is reported as a missing rule rather than a missing
    // key — matching RTS, which only ever looks the destination up through the
    // source's rule list.
    let Some(mut dest_series) =
        try_get_timeseries_mut(ctx, dest_key, Some(AclPermissions::UPDATE))?
    else {
        return Err(ValkeyError::Str(error_consts::COMPACTION_RULE_NOT_FOUND));
    };

    let Some(_rule) = source_series.remove_compaction_rule(dest_key.as_slice()) else {
        return Err(ValkeyError::Str(error_consts::COMPACTION_RULE_NOT_FOUND));
    };

    // Clear the destination's source link — unless it names another source, in which case the
    // removed rule was already stale and the link belongs to a live rule.
    let links_back = dest_series
        .src_series
        .as_ref()
        .is_some_and(|src| src.points_to(source_key.as_slice()));
    if links_back {
        dest_series.src_series = None;
    }

    // Replicate the command
    ctx.replicate_verbatim();

    notify_keyspace_event(ctx, DELETERULE_SRC_EVENT, source_key);
    notify_keyspace_event(ctx, DELETERULE_DEST_EVENT, dest_key);

    VALKEY_OK
}
