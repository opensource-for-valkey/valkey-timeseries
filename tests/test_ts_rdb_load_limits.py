"""Whatever a write command accepts, an RDB load must accept back.

A series `rdb_load` that returns an error aborts the whole RDB load (or the RESTORE), so a
load-side limit tighter than the write path turns an accepted write into a server that cannot
start. The postings-index aux body is softer — a rejected body falls back to a per-key rebuild —
but that rebuild then runs on every restart. These cases pin the shapes the write path accepts
that the loaders once refused.
"""
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_equal

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

PRELOADED_LOG = "Preloaded postings index"
DISCARDED_LOG = "Discarding persisted postings index"

# More than MAX_LABELS_PER_SERIES (128): `TS.CREATE ... LABELS` does not cap the count.
MANY_LABELS = 200
# Past the index body's former 64 KiB per-key ceiling.
LONG = 100 * 1024


def labels_of(client, key):
    info = client.execute_command("TS.INFO", key)
    as_map = dict(zip(info[::2], info[1::2]))
    labels = as_map.get(b"labels") or as_map.get("labels")
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in labels
    }


class TestRdbLoadAcceptsWhatWritesAccept(ValkeyTimeSeriesTestCaseBase):

    def _restart(self, client):
        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)
        return self.server.get_new_client()

    def test_series_with_more_labels_than_the_per_series_cap_survives_restart(self):
        client = self.server.get_new_client()
        pairs = []
        for i in range(MANY_LABELS):
            pairs += [f"l{i}", f"v{i}"]
        client.execute_command("TS.CREATE", "many", "LABELS", *pairs)
        client.execute_command("TS.ADD", "many", 1000, 1.0)

        client = self._restart(client)

        assert client.execute_command("EXISTS", "many") == 1
        labels = labels_of(client, "many")
        assert len(labels) == MANY_LABELS
        assert labels["l199"] == "v199"
        assert client.execute_command("TS.QUERYINDEX", "l199=v199") == [b"many"]

    def test_series_with_more_labels_than_the_per_series_cap_survives_dump_restore(self):
        client = self.server.get_new_client()
        pairs = []
        for i in range(MANY_LABELS):
            pairs += [f"l{i}", f"v{i}"]
        client.execute_command("TS.CREATE", "many", "LABELS", *pairs)
        client.execute_command("TS.ADD", "many", 1000, 1.0)

        payload = client.execute_command("DUMP", "many")
        client.execute_command("DEL", "many")
        assert client.execute_command("RESTORE", "many", 0, payload) == b"OK"
        assert len(labels_of(client, "many")) == MANY_LABELS

    def test_long_label_value_and_key_name_preload_the_index(self):
        client = self.server.get_new_client()
        long_value = "v" * LONG
        long_key = "k" * LONG
        client.execute_command("TS.CREATE", "short", "LABELS", "blob", long_value, "job", "api")
        client.execute_command("TS.CREATE", long_key, "LABELS", "job", "api")

        client = self._restart(client)

        # The persisted index was used, not thrown away for a rebuild.
        assert self.server.verify_string_in_logfile(PRELOADED_LOG)
        assert not self.server.verify_string_in_logfile(DISCARDED_LOG)
        assert client.execute_command("TS.QUERYINDEX", f"blob={long_value}") == [b"short"]
        assert sorted(client.execute_command("TS.QUERYINDEX", "job=api")) == sorted(
            [b"short", long_key.encode()]
        )

    def test_series_with_more_chunks_than_the_load_preallocation_survives_restart(self):
        """The loader reserves room for at most MAX_RDB_PREALLOC (1024) chunks up front and
        grows past that as chunks arrive; a series with more must load back intact."""
        client = self.server.get_new_client()
        # The smallest chunk holds three uncompressed samples.
        client.execute_command("TS.CREATE", "many_chunks", "ENCODING", "UNCOMPRESSED",
                               "CHUNK_SIZE", 48)
        samples = 4000
        pipe = client.pipeline(transaction=False)
        for i in range(samples):
            pipe.execute_command("TS.ADD", "many_chunks", 1000 + i, i)
        pipe.execute()

        def info(c):
            raw = c.execute_command("TS.INFO", "many_chunks")
            as_map = dict(zip(raw[::2], raw[1::2]))
            return {(k.decode() if isinstance(k, bytes) else k): v for k, v in as_map.items()}

        chunks = info(client)["chunkCount"]
        assert chunks > 1024, f"only {chunks} chunks; the test needs more than 1024"
        before = client.execute_command("TS.RANGE", "many_chunks", "-", "+")

        client = self._restart(client)
        assert info(client)["chunkCount"] == chunks
        assert info(client)["totalSamples"] == samples
        assert client.execute_command("TS.RANGE", "many_chunks", "-", "+") == before
