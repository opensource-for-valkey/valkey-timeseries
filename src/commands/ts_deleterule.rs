use crate::error_consts;
use crate::series::get_timeseries_mut;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, VALKEY_OK, ValkeyError, ValkeyResult, ValkeyString,
};

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

    // Get source time series (must exist, writable)
    let mut source_series = get_timeseries_mut(
        ctx,
        source_key,
        true,
        Some(AclPermissions::UPDATE),
    )?
    .expect(
        "BUG in delete_rule: should have returned a value before this point (must_exist = true)",
    );

    // Get the destination series. A destination that does not exist can not be the
    // target of a rule, so it is reported as a missing rule rather than a missing
    // key — matching RTS, which only ever looks the destination up through the
    // source's rule list.
    let Some(mut dest_series) =
        get_timeseries_mut(ctx, dest_key, false, Some(AclPermissions::UPDATE))?
    else {
        return Err(ValkeyError::Str(error_consts::COMPACTION_RULE_NOT_FOUND));
    };

    let dest_id = dest_series.id;
    let Some(_rule) = source_series.remove_compaction_rule(dest_id) else {
        return Err(ValkeyError::Str(error_consts::COMPACTION_RULE_NOT_FOUND));
    };

    // Clear the src_series field in the destination series
    dest_series.src_series = None;

    // Replicate the command
    ctx.replicate_verbatim();

    ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.deleterule:src", source_key);
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.deleterule:dest", dest_key);

    VALKEY_OK
}
