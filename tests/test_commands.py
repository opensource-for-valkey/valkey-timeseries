import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeSeriesCommandKeys(ValkeyTimeSeriesTestCaseBase):
    """Verifies key extraction (``COMMAND GETKEYS``) and documentation (``COMMAND DOCS``) for
    the module's commands.

    These expectations mirror the key specs and summaries declared via the
    ``#[valkey_module_macros::command(...)]`` annotations on each handler in ``src/commands/*``
    and were confirmed against a running server. Keyed commands must report their key
    positions through ``COMMAND GETKEYS``; selector-based commands carry no key specs and must
    report that they have no key arguments.
    """

    # (argv after the command name is appended) -> keys COMMAND GETKEYS must return.
    # Argument values only need to satisfy the declared arity; GETKEYS relies solely on the
    # key specs, so payloads are placeholders.
    KEYED_COMMANDS = [
        (["TS.CREATE", "k"], [b"k"]),
        (["TS.ALTER", "k"], [b"k"]),
        (["TS.ADD", "k", "1000", "1.0"], [b"k"]),
        (["TS.ADDBULK", "k", "data"], [b"k"]),
        (["TS.GET", "k"], [b"k"]),
        (["TS.INFO", "k"], [b"k"]),
        (["TS.DEL", "k", "1", "2"], [b"k"]),
        (["TS.INCRBY", "k", "1"], [b"k"]),
        (["TS.DECRBY", "k", "1"], [b"k"]),
        (["TS.RANGE", "k", "-", "+"], [b"k"]),
        (["TS.REVRANGE", "k", "-", "+"], [b"k"]),
        (["TS.OUTLIERS", "k", "-", "+", "MAD", "3"], [b"k"]),
        (["TS.MADD", "k1", "1", "1.0", "k2", "2", "2.0"], [b"k1", b"k2"]),
        (["TS.JOIN", "k1", "k2", "-", "+"], [b"k1", b"k2"]),
        # numkeys-style key spec: the count at index 1 says how many keys follow.
        (["TS.NRANGE", "2", "k1", "k2", "-", "+"], [b"k1", b"k2"]),
        (["TS.NRANGE", "1", "k1", "-", "+"], [b"k1"]),
        (["TS.NREVRANGE", "2", "k1", "k2", "-", "+"], [b"k1", b"k2"]),
        (["TS.CREATERULE", "src", "dst", "AGGREGATION", "avg", "60000"], [b"src", b"dst"]),
        (["TS.DELETERULE", "src", "dst"], [b"src", b"dst"]),
    ]

    # Selector-based commands take no keys; COMMAND GETKEYS must report that.
    KEYLESS_COMMANDS = [
        ["TS.MGET", "FILTER", "a=b"],
        ["TS.MRANGE", "-", "+", "FILTER", "a=b"],
        ["TS.MREVRANGE", "-", "+", "FILTER", "a=b"],
        ["TS.MDEL", "FILTER", "a=b"],
        ["TS.QUERYINDEX", "FILTER", "a=b"],
        ["TS.QUERYLABELS", "LABELS", "FILTER", "a=b"],
        ["TS.CARD", "FILTER", "a=b"],
        ["TS.LABELNAMES", "FILTER", "a=b"],
        ["TS.LABELVALUES", "label", "FILTER", "a=b"],
        ["TS.METRICNAMES", "FILTER", "a=b"],
        ["TS.LABELSTATS"],
    ]

    def getkeys(self, argv):
        # Single-string form bypasses the client's COMMAND response callback, which does not
        # understand the GETKEYS reply shape.
        return self.client.execute_command("COMMAND GETKEYS " + " ".join(argv))

    def test_getkeys_for_keyed_commands(self):
        for argv, expected in self.KEYED_COMMANDS:
            keys = self.getkeys(argv)
            assert keys == expected, (
                f"GETKEYS for '{argv[0]}' expected {expected}, got {keys}"
            )

    def test_getkeys_for_keyless_commands(self):
        for argv in self.KEYLESS_COMMANDS:
            with pytest.raises(ResponseError) as exc_info:
                self.getkeys(argv)
            assert "no key arguments" in str(exc_info.value).lower(), (
                f"GETKEYS for '{argv[0]}' should report no key arguments, got: {exc_info.value}"
            )

    def test_command_docs_metadata(self):
        # Every user-facing command is registered with a summary, complexity and since via the
        # command-info annotations. Spot-check a representative set across the read/write and
        # keyed/keyless categories.
        for command in ["TS.CREATE", "TS.ADD", "TS.RANGE", "TS.MGET", "TS.CREATERULE"]:
            docs = self.client.execute_command(f"COMMAND DOCS {command}")
            assert docs and docs[0].decode().upper() == command, (
                f"COMMAND DOCS did not return an entry for {command}"
            )
            fields = docs[1]
            meta = {
                fields[i].decode(): fields[i + 1] for i in range(0, len(fields), 2)
            }
            assert meta.get("summary"), f"{command} is missing a COMMAND DOCS summary"
            assert meta.get("complexity"), f"{command} is missing a COMMAND DOCS complexity"
            assert meta.get("since"), f"{command} is missing a COMMAND DOCS since"
