"""Regression tests for a malformed TS._RESTORE, ensuring that a malformed or truncated payload must not abort the server.

`TS._RESTORE` reconstructs a series through the module type's `rdb_load`. Without the
`HANDLE_IO_ERRORS` module option, the server's `moduleRDBLoadError` reaches `serverPanic` the
moment the payload stream underruns -- inside `rdb_load`, before the handler's null check or any
of the module's own bounds checks can run. With the option declared, the short read is recorded
on the RedisModuleIO instead and surfaces as a clean error.

The command is registered `write timeseries admin`, so `+@timeseries` grants it; a truncated or
corrupt payload is reachable by any client holding that category.
"""

import os
import random
import re
import pytest

from valkeytestframework.util.waiters import *
from valkeytestframework.valkey_test_case import ValkeyAction
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeseriesRestoreMalformed(ValkeyTimeSeriesTestCaseBase):

    def _genuine_payload(self, key='payload_src', bucket_duration=None):
        """Produce a real TS._RESTORE payload by way of the command-format AOF.

        `aof_rewrite` emits `TS._RESTORE key <blob>`, where the blob is what the type's
        `rdb_save` produced -- the exact input shape this command reads in production.
        """
        client = self.client
        client.config_set('aof-use-rdb-preamble', 'no')

        # Populate before enabling AOF so the rewrite this triggers already contains the series.
        if bucket_duration is not None:
            destination = f'{key}:destination'
            client.execute_command('TS.CREATE', destination)
        client.execute_command('TS.CREATE', key, 'LABELS', 'host', 'a', 'region', 'b')
        if bucket_duration is not None:
            client.execute_command(
                'TS.CREATERULE', key, destination,
                'AGGREGATION', 'sum', bucket_duration,
            )
        for i in range(1, 201):
            client.execute_command('TS.ADD', key, 1000 + i * 1000, i)

        client.config_set('appendonly', 'yes')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)

        # The harness gives each server its own per-port `appenddirname`.
        server_dir = client.config_get('dir')['dir']
        aof_dir = os.path.join(server_dir, client.config_get('appenddirname')['appenddirname'])
        needle = b'TS._RESTORE\r\n$%d\r\n%s\r\n' % (len(key), key.encode())

        def find_payload():
            """Return the base-AOF bytes carrying this key's TS._RESTORE, once written."""
            if not os.path.isdir(aof_dir):
                return None
            for name in os.listdir(aof_dir):
                if not name.endswith('.base.aof'):
                    continue
                data = open(os.path.join(aof_dir, name), 'rb').read()
                if needle in data:
                    return data
            return None

        # The rewrite lands asynchronously; poll rather than racing it.
        wait_for_true(lambda: find_payload() is not None, timeout=30)
        data = find_payload()
        assert data is not None

        i = data.index(needle)
        p = data.index(b'\r\n', i + len(needle) - 2) + 2
        m = re.match(rb'\$(\d+)\r\n', data[p:])
        assert m, 'could not locate the TS._RESTORE payload bulk string'
        start = p + m.end()
        payload = data[start:start + int(m.group(1))]
        assert len(payload) > 0
        return payload

    def test_restore_garbage_payload_does_not_crash(self):
        """Payloads that are not module output at all are rejected, not fatal."""
        client = self.client
        for i, blob in enumerate([b'', b'garbage', b'\x01\x02\x03', b'A' * 32, b'\x00' * 256]):
            try:
                client.execute_command('TS._RESTORE', f'garbage{i}', blob)
            except Exception as e:
                assert 'failed to deserialize' in str(e), f'unexpected error for {blob!r}: {e}'
            else:
                raise AssertionError(f'TS._RESTORE accepted a garbage payload: {blob!r}')
            assert client.ping()
        assert self.server.is_alive()

    def test_restore_truncated_payload_does_not_crash(self):
        """Every truncation of a genuine payload underruns the stream mid-parse."""
        client = self.client
        payload = self._genuine_payload()

        # The untruncated payload must still restore cleanly, or the test proves nothing.
        assert client.execute_command('TS._RESTORE', 'restored', payload)
        assert self.ts_info('restored')['totalSamples'] == 200

        for cut in list(range(0, len(payload), 7)) + [len(payload) - 1]:
            try:
                client.execute_command('TS._RESTORE', f'trunc{cut}', payload[:cut])
            except Exception as e:
                assert 'failed to deserialize' in str(e), f'unexpected error at cut {cut}: {e}'
            else:
                raise AssertionError(f'TS._RESTORE accepted a payload truncated to {cut} bytes')
            assert client.ping(), f'server died on truncation at {cut} bytes'
        assert self.server.is_alive()

    def test_restore_corrupt_payload_does_not_crash(self):
        """Bit-flips inside a full-length payload hit the interior decoders."""
        client = self.client
        payload = self._genuine_payload(key='fuzz_src')
        rng = random.Random(7)

        for trial in range(300):
            b = bytearray(payload)
            for _ in range(rng.randint(1, 8)):
                b[rng.randrange(len(b))] = rng.randrange(256)
            try:
                client.execute_command('TS._RESTORE', f'fuzz{trial}', bytes(b))
            except Exception:
                pass  # any clean error is fine; only a dead server is a failure
            assert client.ping(), f'server died on corrupt payload, trial {trial}'
        assert self.server.is_alive()

    def test_restore_zero_duration_rule_is_rejected(self):
        """A restored zero-duration rule is rejected before a later write can divide by zero."""
        client = self.client
        payload = self._genuine_payload(key='zero_duration_src', bucket_duration=12345)

        # RedisModule_SaveUnsigned stores the opcode followed by an RDB length.
        # 12345 is a two-byte 14-bit length (0x70, 0x39); retain that width when
        # changing the value to zero so the rest of the payload stays aligned.
        encoded_duration = b'\x02\x70\x39'
        zero_duration = b'\x02\x40\x00'
        assert payload.count(encoded_duration) == 1
        malformed = payload.replace(encoded_duration, zero_duration)

        with pytest.raises(Exception, match='failed to deserialize'):
            client.execute_command('TS._RESTORE', 'zero_duration_restored', malformed)

        assert client.ping()
        assert self.server.is_alive()
