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
    # position 1 (1, 1, 1). NRANGE/NREVRANGE take a `numkeys key [key ...]` prefix, which the
    # legacy triple cannot express: they report no key range and carry `movablekeys` instead,
    # so clients must ask COMMAND GETKEYS (see test_commands.py).
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
        "TS.NRANGE":      (-5, 0,  0, 0, [b"readonly", b"module", b"movablekeys"]),
        "TS.NREVRANGE":   (-5, 0,  0, 0, [b"readonly", b"module", b"movablekeys"]),
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
    }

    def command_info(self, command):
        # Use the single-string form so the raw reply is returned unmodified. The multi-arg
        # form ('COMMAND', 'INFO', command) triggers the client's COMMAND response callback,
        # which reshapes the reply.
        info = self.client.execute_command(f"COMMAND INFO {command}")
        assert info and info[0] is not None, f"Command {command} is not registered"
        return info[0]

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
