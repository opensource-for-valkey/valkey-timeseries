"""Regression tests for a corrupt or truncated RDB. It must fail the load, not crash the server.

The module's RDB bounds checks (`rdb_load_len` / `MAX_RDB_COLLECTION_LEN`, the foreign-encver
guard, and the uncompressed-chunk sample-capacity cap) all sit behind reads that, without the
`HANDLE_IO_ERRORS` module option, reach `serverPanic` the moment the stream underruns -- so for a
truncated payload those checks were unreachable and a corrupt RDB aborted the server during load.

The pre-existing unit coverage for the capacity cap exercises `deserialize_raw` over an in-memory
buffer, which returns `Err` whether or not the option is declared, so it passes even when the real
RDB path aborts. These tests drive actual RDB files through actual server startup instead.

A clean outcome is the module either loading the payload or rejecting it with a logged error.
The failure being guarded against is a module panic: a Guru Meditation / bug report / stack trace.

These tests deliberately assert on the module's verdict *in the log* rather than on the server
process exiting. On any RDB read error for a file on disk, the server re-parses it through
`redis_check_rdb_main` to produce a diagnostic, and `rdbLoadCheckModuleValue` (rdb.c) loops
    while ((opcode = rdbLoadLen(rdb, NULL)) != RDB_MODULE_OPCODE_EOF)
where a short read makes `rdbLoadLen` return `RDB_LENERR` (UINT64_MAX), never
`RDB_MODULE_OPCODE_EOF` (0) -- so for some truncation offsets the server spins forever in its
own check code, after the module has already reported the error correctly. That is a
valkey-server defect, not a module one, and waiting on process exit would make this suite hang
on it.
"""

import os
import random
import shutil
import subprocess
import time

import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from common import VALKEY_SERVER_PATH

# Markers the server emits when it dies on an unhandled module fault, rather than
# reporting a load failure and exiting cleanly.
# The module's own rejection line, from `rdb_load` in src/series/series_data_type.rs.
MODULE_REJECTED = 'Failed to load series from RDB'
SERVER_READY = 'Ready to accept connections'

CRASH_MARKERS = (
    'Guru Meditation',
    'STACK TRACE',
    'VALKEY BUG REPORT',
    'Unrecoverable error reading from module datatype',
    'panicked at',
    'crashed by signal',
)


class TestRdbCorruptLoad(ValkeyTimeSeriesTestCaseBase):

    def _module_path(self):
        return self.server.args.get('loadmodule') or os.path.abspath(
            os.environ['MODULE_PATH']
        )

    def _good_rdb(self):
        """Populate a series and return the bytes of a valid RDB containing it."""
        client = self.client
        client.execute_command('TS.CREATE', 'rdb_s1', 'LABELS', 'host', 'a', 'region', 'b')
        for i in range(1, 201):
            client.execute_command('TS.ADD', 'rdb_s1', 1000 + i * 1000, i)
        client.execute_command('TS.CREATE', 'rdb_s2', 'LABELS', 'host', 'c')
        client.execute_command('TS.ADD', 'rdb_s2', 5000, 1.5)
        client.execute_command('SAVE')

        cfg = client.config_get('dir')['dir'], client.config_get('dbfilename')['dbfilename']
        path = os.path.join(*cfg)
        data = open(path, 'rb').read()
        assert len(data) > 100, f'unexpectedly small RDB at {path}'
        return data

    def _load_attempt(self, tmpdir, rdb_bytes, port):
        """Start a server against `rdb_bytes`. Returns (outcome, log text)."""
        # The server chdir()s to `--dir` while parsing its config, so --logfile and
        # --dir must both be absolute or it cannot even open its log.
        tmpdir = os.path.abspath(tmpdir)
        shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir)
        open(os.path.join(tmpdir, 'test.rdb'), 'wb').write(rdb_bytes)
        log = os.path.join(tmpdir, 'srv.log')

        def read_log():
            # The server echoes fragments of the corrupt payload, so the log is not
            # guaranteed to be valid UTF-8.
            if not os.path.exists(log):
                return ''
            with open(log, 'rb') as fh:
                return fh.read().decode('utf-8', 'replace')

        # Child output goes to a file, never a pipe: the RDB check prints enough on a
        # corrupt file to fill a pipe buffer and block the server forever.
        console = os.path.join(tmpdir, 'console.out')
        with open(console, 'wb') as out:
            proc = subprocess.Popen(
                [VALKEY_SERVER_PATH, '--port', str(port), '--daemonize', 'no',
                 '--loadmodule', self._module_path(), '--logfile', log,
                 '--dir', tmpdir, '--dbfilename', 'test.rdb', '--save', ''],
                stdout=out, stderr=subprocess.STDOUT,
            )
        outcome, deadline = None, time.time() + 20
        try:
            while time.time() < deadline:
                text = read_log()
                if any(m in text for m in CRASH_MARKERS):
                    outcome = 'CRASH'
                    break
                if MODULE_REJECTED in text:
                    outcome = 'MODULE_REJECTED'
                    break
                if SERVER_READY in text:
                    outcome = 'LOADED'
                    break
                if proc.poll() is not None:
                    # Server rejected the file before the module was ever asked.
                    outcome = 'SERVER_REJECTED'
                    break
                time.sleep(0.05)
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()

        text = read_log()
        if not text:
            # The server failed before opening its log (e.g. a bad config path).
            with open(console, 'rb') as fh:
                text = fh.read().decode('utf-8', 'replace')
        # An exit is only clean if it left no crash marker behind.
        if any(m in text for m in CRASH_MARKERS):
            outcome = 'CRASH'
        return outcome or 'UPSTREAM_HANG', text

    def _marker(self, text):
        return next((l.strip() for l in text.splitlines()
                     if any(m in l for m in CRASH_MARKERS)), '(no marker)')

    def test_truncated_rdb_fails_cleanly(self):
        good = self._good_rdb()
        port = self.get_bind_port()
        tmpdir = os.path.join(self.testdir, 'rdb_trunc')

        # Sample the whole file rather than every byte: the interesting region is the
        # module payload, but the sweep is kept cheap enough for CI.
        cuts = list(range(1, len(good), max(1, len(good) // 60)))
        outcomes = {}
        for cut in cuts:
            outcome, text = self._load_attempt(tmpdir, good[:cut], port)
            assert outcome != 'CRASH', \
                f'truncation at {cut}/{len(good)} bytes panicked the module: {self._marker(text)}'
            outcomes[outcome] = outcomes.get(outcome, 0) + 1

        # The module must actually have been exercised, or this proves nothing.
        assert outcomes.get('MODULE_REJECTED', 0) > 0, \
            f'no truncation reached the module loader; outcomes={outcomes}'

    def test_corrupt_rdb_fails_cleanly(self):
        good = self._good_rdb()
        port = self.get_bind_port()
        tmpdir = os.path.join(self.testdir, 'rdb_corrupt')
        rng = random.Random(11)
        outcomes = {}

        for trial in range(40):
            b = bytearray(good)
            for _ in range(rng.randint(1, 10)):
                b[rng.randrange(len(b))] = rng.randrange(256)
            outcome, text = self._load_attempt(tmpdir, bytes(b), port)
            assert outcome != 'CRASH', \
                f'corrupt RDB (trial {trial}) panicked the module: {self._marker(text)}'
            outcomes[outcome] = outcomes.get(outcome, 0) + 1

        assert outcomes.get('MODULE_REJECTED', 0) > 0, \
            f'no corrupt payload reached the module loader; outcomes={outcomes}'

    def test_unmodified_rdb_still_loads(self):
        """Guards the two sweeps above: if the baseline stopped loading they would pass vacuously."""
        good = self._good_rdb()
        port = self.get_bind_port()
        tmpdir = os.path.join(self.testdir, 'rdb_baseline')
        outcome, text = self._load_attempt(tmpdir, good, port)
        assert outcome == 'LOADED', (
            f'valid RDB must load; got {outcome}\n'
            f'module={self._module_path()}\nLOG:\n{text[-3000:]}')
