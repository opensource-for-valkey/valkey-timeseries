from multiprocessing.util import info

import pytest

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeSeriesCommand(ValkeyTimeSeriesTestCaseBase):
    """Verifies the ``COMMAND INFO`` metadata (arity, flags and legacy key range) that the
    module registers for every user-facing ``TS.*`` command.

    The expectations below are derived directly from the
    ``#[valkey_module_macros::command(...)]`` annotations on each command handler in
    ``src/commands/*`` and were confirmed against a running server. They intentionally mirror
    the observed code rather than any prior assumption about the metadata.
    """

    # command -> (arity, first_key, last_key, step, flags)
    #
    # Selector-based commands (MGET, MRANGE, QUERYINDEX, CARD, the LABEL* family, ...) match
    # series by FILTER expressions rather than positional keys, so they expose no key range
    # (0, 0, 0). MADD takes a key every third argument (1, -1, 3); JOIN/CREATERULE/DELETERULE
    # take two adjacent keys (1, 2, 1); the remaining keyed commands take a single key at
    # position 1 (1, 1, 1).
    COMMAND_INFO = {
        "TS.CREATE":      (-2, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.ALTER":       (-2, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.ADD":         (-4, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.ADDBULK":     (-3, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.GET":         (-2, 1,  1, 1, [b"readonly", b"module", b"fast"]),
        "TS.MGET":        (-2, 0,  0, 0, [b"readonly", b"module", b"fast"]),
        "TS.MADD":        (-4, 1, -1, 3, [b"write", b"denyoom", b"module"]),
        "TS.DEL":         (-3, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.DECRBY":      (-3, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.INCRBY":      (-3, 1,  1, 1, [b"write", b"denyoom", b"module"]),
        "TS.JOIN":        (-4, 1,  2, 1, [b"readonly", b"module"]),
        "TS.MDEL":        (-2, 0,  0, 0, [b"write", b"denyoom", b"module"]),
        "TS.MRANGE":      (-4, 0,  0, 0, [b"readonly", b"module"]),
        "TS.MREVRANGE":   (-4, 0,  0, 0, [b"readonly", b"module"]),
        "TS.RANGE":       (-4, 1,  1, 1, [b"readonly", b"module"]),
        "TS.REVRANGE":    (-4, 1,  1, 1, [b"readonly", b"module"]),
        "TS.INFO":        (-2, 1,  1, 1, [b"readonly", b"module"]),
        "TS.QUERYINDEX":  (-2, 0,  0, 0, [b"readonly", b"module"]),
        "TS.CARD":        (-1, 0,  0, 0, [b"readonly", b"module"]),
        "TS.LABELNAMES":  (-1, 0,  0, 0, [b"readonly", b"module"]),
        "TS.LABELVALUES": (-2, 0,  0, 0, [b"readonly", b"module"]),
        "TS.METRICNAMES": (-1, 0,  0, 0, [b"readonly", b"module"]),
        "TS.LABELSTATS":  (-1, 0,  0, 0, [b"readonly", b"module"]),
        "TS.CREATERULE":  (-6, 1,  2, 1, [b"write", b"denyoom", b"module"]),
        "TS.DELETERULE":   (3, 1,  2, 1, [b"write", b"denyoom", b"module"]),
        "TS.OUTLIERS":    (-6, 1,  1, 1, [b"readonly", b"denyoom", b"module"]),
        # Analysis/forecasting commands. Those accepting a STORE clause declare a second,
        # keyword-based key spec for the destination series, which the server reports as
        # `movablekeys`; their legacy key range still covers only the source key at
        # position 1. TS.SANITIZE writes the sanitized samples back to the source series,
        # so its source key spec is RW rather than RO.
        "TS.XCORR":          (-6, 1, 2, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.BACKTEST":       (-8, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.DECOMPOSE":      (-4, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.PERIODS":        (-4, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.AUTOCORRELATION":(-5, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.STATS":          (-2, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.FEATURES":       (-4, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.STATIONARITY":   (-4, 1, 1, 1, [b"readonly", b"denyoom", b"module"]),
        "TS.TREND":          (-4, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
        "TS.FORECAST":       (-8, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
        "TS.AUTOFORECAST":   (-5, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
        "TS.FILLGAPS":       (-4, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
        "TS.SANITIZE":       (-4, 1, 1, 1, [b"write", b"denyoom", b"module", b"movablekeys"]),
    }

    # Commands whose optional STORE clause names a destination series, and the argument
    # prefix needed to reach that clause. COMMAND GETKEYS must report the destination in
    # addition to the source key so the server can route and ACL-check it.
    STORE_COMMANDS = {
        "TS.FILLGAPS": ["src", "0", "100"],
        "TS.SANITIZE": ["src", "0", "100"],
        "TS.TREND": ["src", "0", "100"],
        "TS.FORECAST": ["src", "0", "100", "MODELS", "AutoARIMA", "HORIZON", "5"],
        "TS.AUTOFORECAST": ["src", "0", "100", "HORIZON", "5"],
    }

    def command_info(self, command):
        # Use the single-string form so the raw reply is returned unmodified. The multi-arg
        # form ('COMMAND', 'INFO', command) triggers the client's COMMAND response callback,
        # which reshapes the reply.
        info = self.client.execute_command(f"COMMAND INFO {command}")
        assert info and info[0] is not None, f"Command {command} is not registered"
        return info[0]

    def getkeys(self, command, *args):
        # As with command_info, the single-string form avoids the client's COMMAND
        # response callback, which would otherwise try to reshape the key list.
        argv = " ".join(str(a) for a in args)
        keys = self.client.execute_command(f"COMMAND GETKEYS {command} {argv}")
        return [k.decode() if isinstance(k, bytes) else k for k in keys]

    def test_command_arity(self):
        for command, expected in self.COMMAND_INFO.items():
            info = self.command_info(command)
            assert info[1] == expected[0], (
                f"Arity mismatch for '{command}': expected {expected[0]}, got {info[1]}"
            )

    def test_command_flags(self):
        for command, expected in self.COMMAND_INFO.items():
            info = self.command_info(command)
            assert sorted(info[2]) == sorted(expected[4]), (
                f"Flags mismatch for '{command}': expected {expected[4]}, got {info[2]}"
            )

    def test_command_key_range(self):
        for command, expected in self.COMMAND_INFO.items():
            info = self.command_info(command)
            first, last, step = info[3], info[4], info[5]
            assert (first, last, step) == (expected[1], expected[2], expected[3]), (
                f"Key range mismatch for '{command}': expected "
                f"{(expected[1], expected[2], expected[3])}, got {(first, last, step)}"
            )

    def test_getkeys_reports_store_destination(self):
        """The STORE destination must be discoverable via COMMAND GETKEYS.

        The keyword-based key spec is what lets the server route and ACL-check the
        destination series; without it only the source key would be reported.
        """
        for command, prefix in self.STORE_COMMANDS.items():
            keys = self.getkeys(command, *prefix, "STORE", "dest")
            assert keys == ["src", "dest"], (
                f"GETKEYS mismatch for '{command}' with STORE: expected "
                f"['src', 'dest'], got {keys}"
            )

    def test_getkeys_without_store(self):
        """Without a STORE clause only the source key is reported."""
        for command, prefix in self.STORE_COMMANDS.items():
            keys = self.getkeys(command, *prefix)
            assert keys == ["src"], (
                f"GETKEYS mismatch for '{command}' without STORE: expected "
                f"['src'], got {keys}"
            )
