"""Persistence interop with RedisTimeSeries 8.8 (test plan §7.4).

Both engines register the same module type name — `TSDB-TYPE` — with
incompatible payload formats (ours: encver 1; RTS 8.8: encver 9). RTS→valkey
RDB/DUMP migration is explicitly NOT supported (owner decision, 2026-07-16;
DIV-0010): there is no format converter and none is planned. Migration is by
re-ingest (see COMPATIBILITY.md). This module pins the *defined-failure*
behavior so it is discovered by tests, not by users:

  - RESTORE of an RTS-produced DUMP payload must fail cleanly: error reply,
    no key created, server healthy afterwards. (Today the server's RDB
    version footer rejects it before the module is reached.)
  - A foreign-encver TSDB-TYPE payload that gets past the envelope (e.g. an
    RDB produced by RTS on an older Redis whose RDB version valkey accepts)
    must be refused by the module's own encoding-version guard.
  - Loading an RTS-produced RDB file must be refused with a clear log
    message — never misparsed. Fixture: test-data/rts-8.8-timeseries.rdb
    (generated output of the pinned reference image; keys `k` with samples).
  - The reverse direction (our DUMP into the reference) is out of our
    control; the observed clean rejection is pinned as documentation.

Written clean-room from public documentation and black-box observation of
the reference server (see tests/compat/README.md).
"""

import os
import socket
import subprocess
import time

import pytest
import valkey
from valkey.exceptions import ResponseError

from common import VALKEY_SERVER_PATH, get_module_path

_COMPAT_DIR = os.path.dirname(os.path.abspath(__file__))
RTS_RDB_FIXTURE = os.path.join(
    os.path.dirname(os.path.dirname(_COMPAT_DIR)), "test-data", "rts-8.8-timeseries.rdb"
)

# --- CRC-64/Jones (reflected), as used by redis/valkey DUMP footers ---------

_CRC_TBL = []
for _i in range(256):
    _crc = _i
    for _ in range(8):
        _crc = (_crc >> 1) ^ 0x95AC9329AC4BC9B5 if _crc & 1 else _crc >> 1
    _CRC_TBL.append(_crc)


def _crc64(data: bytes) -> int:
    crc = 0
    for b in data:
        crc = _CRC_TBL[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc


def _with_encver(payload: bytes, encver: int) -> bytes:
    """Rewrite a MODULE_2 DUMP payload's module-type encver and fix the CRC."""
    assert payload[0] == 7, "expected RDB_TYPE_MODULE_2 opcode"
    assert payload[1] == 0x81, "expected 64-bit length marker for the module id"
    mid = int.from_bytes(payload[2:10], "big")
    mid = (mid & ~0x3FF) | encver
    body = payload[:2] + mid.to_bytes(8, "big") + payload[10:-8]
    return body + _crc64(body).to_bytes(8, "little")


@pytest.fixture
def clients(subject_url, reference_url):
    subject = valkey.Valkey.from_url(subject_url)
    reference = valkey.Valkey.from_url(reference_url)
    subject.flushall()
    reference.flushall()
    yield subject, reference
    subject.close()
    reference.close()


class TestDumpRestore:
    def test_rts_dump_into_subject_fails_cleanly(self, clients):
        subject, reference = clients
        reference.execute_command("TS.CREATE", "src", "LABELS", "sensor", "s1")
        reference.execute_command("TS.ADD", "src", 100, 1.5)
        payload = reference.execute_command("DUMP", "src")

        with pytest.raises(ResponseError):
            subject.execute_command("RESTORE", "dst", 0, payload)

        # Clean failure: no partially-created key, server healthy and writable.
        assert subject.execute_command("EXISTS", "dst") == 0
        assert subject.ping()
        assert subject.execute_command("TS.ADD", "after", 100, 1.0) == 100

    def test_foreign_encver_rejected_by_module_guard(self, clients):
        """A TSDB-TYPE payload with a foreign encoding version must be refused
        by the module itself, independent of the server's RDB-version
        envelope (which is what rejects genuine Redis-8.8 payloads today)."""
        subject, _ = clients
        subject.execute_command("TS.CREATE", "own")
        subject.execute_command("TS.ADD", "own", 100, 1.5)
        payload = subject.execute_command("DUMP", "own")

        # Sanity: the same payload with its true encver round-trips.
        assert subject.execute_command("RESTORE", "own-copy", 0, payload) == b"OK"

        mutated = _with_encver(payload, 9)  # RTS 8.8's encver
        with pytest.raises(ResponseError, match="Bad data format"):
            subject.execute_command("RESTORE", "own-9", 0, mutated)
        assert subject.execute_command("EXISTS", "own-9") == 0
        assert subject.ping()

    def test_our_dump_into_reference_documented_behavior(self, clients):
        """We can't control the reverse direction; pin what the reference
        observably does with our payload so the compatibility page can state
        it (plan §7.4). Observed: clean 'Bad data format' rejection."""
        subject, reference = clients
        subject.execute_command("TS.CREATE", "ours")
        subject.execute_command("TS.ADD", "ours", 100, 1.5)
        payload = subject.execute_command("DUMP", "ours")

        with pytest.raises(ResponseError):
            reference.execute_command("RESTORE", "ours-import", 0, payload)
        assert reference.execute_command("EXISTS", "ours-import") == 0
        assert reference.ping()


class TestRdbFileLoad:
    def test_rts_rdb_file_refused_cleanly(self, tmp_path):
        """Start a fresh subject server on an RTS-produced RDB file: it must
        refuse the file with a clear log message or come up without the data
        — never misparse it into live keys."""
        assert os.path.exists(RTS_RDB_FIXTURE), f"missing fixture {RTS_RDB_FIXTURE}"
        rdb = tmp_path / "dump.rdb"
        rdb.write_bytes(open(RTS_RDB_FIXTURE, "rb").read())
        logfile = tmp_path / "server.log"

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

        proc = subprocess.Popen(
            [
                VALKEY_SERVER_PATH,
                "--port", str(port),
                "--loadmodule", get_module_path(),
                "--dir", str(tmp_path),
                "--dbfilename", "dump.rdb",
                "--save", "",
                "--logfile", str(logfile),
            ],
            cwd=str(tmp_path),
        )
        try:
            deadline = time.monotonic() + 15
            up = False
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    break  # refused and exited — the expected path today
                try:
                    probe = valkey.Valkey(port=port, socket_connect_timeout=0.3)
                    probe.ping()
                    up = True
                    break
                except Exception:
                    time.sleep(0.2)

            log = logfile.read_text() if logfile.exists() else ""
            refusal_markers = (
                "Can't handle RDB format version",
                "cannot load TSDB-TYPE RDB payload",
                "Error loading",
            )
            if up:
                # Server started anyway: the RTS data must NOT have been
                # misparsed into a live series.
                probe = valkey.Valkey(port=port)
                assert probe.execute_command("EXISTS", "k") == 0, (
                    "RTS RDB key materialized on the subject — possible misparse"
                )
                assert any(m in log for m in refusal_markers), log[-2000:]
            else:
                assert proc.poll() is not None, "server neither up nor exited"
                assert any(m in log for m in refusal_markers), log[-2000:]
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)
