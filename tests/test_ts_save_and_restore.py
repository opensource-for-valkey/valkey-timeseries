from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_equal

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase, parse_info_response


def get_info(client, key):
    """Helper to get the info of a time series."""
    info = client.execute_command("TS.INFO", key)
    return parse_info_response(info)


class TestTimeseriesSaveRestore(ValkeyTimeSeriesTestCaseBase):

    def test_basic_save_and_restore(self):
        client = self.server.get_new_client()
        ts_add_result_1 = client.execute_command('TS.ADD testSave 1000 1.0')
        assert ts_add_result_1 == 1000
        ts_exists_result_1 = client.execute_command('EXISTS testSave')
        assert ts_exists_result_1 == 1
        ts_info_result_1 = get_info(client, 'testSave')
        assert ts_add_result_1 is not None
        curr_item_count_1 = self.num_keys()
        # cmd debug digest
        server_digest = client.execute_command("DEBUG DIGEST")
        assert server_digest != None or 0000000000000000000000000000000000000000
        object_digest = client.execute_command('DEBUG DIGEST-VALUE testSave')

        # save rdb, restart sever
        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)
        restored_server_digest = client.execute_command("DEBUG", "DIGEST")
        restored_object_digest = client.execute_command('DEBUG DIGEST-VALUE testSave')
        assert restored_server_digest == server_digest
        assert restored_object_digest == object_digest
        self.server.verify_string_in_logfile("Loading RDB produced by Valkey")
        self.server.verify_string_in_logfile("Done loading RDB, keys loaded: 1, keys expired: 0")

        # verify restore results
        curr_item_count_2 = self.num_keys()
        assert curr_item_count_2 == curr_item_count_1
        ts_exists_result_2 = client.execute_command('EXISTS testSave')
        assert ts_exists_result_2 == 1
        ts_info_result_2 = get_info(client, 'testSave')

        # Remove memory usage from the info result for comparison
        del ts_info_result_2['memoryUsage']
        del ts_info_result_1['memoryUsage']
        assert ts_info_result_2 == ts_info_result_1

    def test_basic_save_many(self):
        client = self.server.get_new_client()
        count = 500
        for i in range(0, count):
            name = str(i) + "key"

            ts_add_result_1 = client.execute_command('TS.ADD', name, 1000, 1.0)
            assert ts_add_result_1 == 1000

        curr_item_count_1 = self.num_keys()
        assert curr_item_count_1 == count
        # save rdb, restart sever
        client.bgsave()
        self.server.wait_for_save_done()

        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)
        self.server.verify_string_in_logfile("Loading RDB produced by Valkey")
        self.server.verify_string_in_logfile("Done loading RDB, keys loaded: 500, keys expired: 0")

        # verify restore results
        curr_item_count_1 = self.num_keys()

        assert curr_item_count_1 == count

    def test_save_restore_series_with_compaction_rules(self):
        """Test save/restore with series having compaction rules"""
        client = self.server.get_new_client()
        source_key = 'test:source:compaction'
        dest_key = 'test:dest:compaction'

        # Create source and destination series
        client.execute_command('TS.CREATE', source_key,
                               'RETENTION', 86400000,  # 24 hours
                               'LABELS', 'sensor', 'temperature', 'location', 'room1')
        client.execute_command('TS.CREATE', dest_key)

        # Add compaction rule
        client.execute_command('TS.CREATERULE', source_key, dest_key,
                               'AGGREGATION', 'avg', 10000)

        # Add some data to trigger compaction
        base_ts = 1000000
        samples = [
            (base_ts, 10.0),
            (base_ts + 5000, 20.0),
            (base_ts + 15000, 30.0),  # This should finalize the first bucket
            (base_ts + 18000, 40.0),
        ]

        for ts, value in samples:
            client.execute_command('TS.ADD', source_key, ts, value)

        # Get the original state
        original_source_info = get_info(client, source_key)
        original_dest_info = get_info(client, dest_key)
        original_source_samples = client.execute_command('TS.RANGE', source_key, '-', '+')
        original_dest_samples = client.execute_command('TS.RANGE', dest_key, '-', '+')

        # Verify compaction occurred
        assert len(original_dest_samples) >= 1, "Compaction should have occurred"

        # Save and restart
        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Verify after restore
        restored_source_info = get_info(client, source_key)
        restored_dest_info = get_info(client, dest_key)
        restored_source_samples = client.execute_command('TS.RANGE', source_key, '-', '+')
        restored_dest_samples = client.execute_command('TS.RANGE', dest_key, '-', '+')

        # Remove memory usage for comparison
        for info in [original_source_info, original_dest_info, restored_source_info, restored_dest_info]:
            del info['memoryUsage']

        assert restored_source_info == original_source_info, "Source series info should match after restore"
        assert restored_dest_info == original_dest_info, "Destination series info should match after restore"
        assert restored_source_samples == original_source_samples, "Source series samples should match after restore"
        assert restored_dest_samples == original_dest_samples, "Destination series samples should match after restore"

        # Verify rules are still functional after restore
        client.execute_command('TS.ADD', source_key, base_ts + 25000, 50.0)
        new_dest_samples = client.execute_command('TS.RANGE', dest_key, '-', '+')
        assert len(new_dest_samples) >= len(restored_dest_samples), "Compaction should still work"

    def test_save_restore_multiple_series_with_mixed_properties(self):
        """Test save/restore with multiple series having different properties"""
        client = self.server.get_new_client()

        # Series 1: Basic series
        key1 = 'series:basic'
        client.execute_command('TS.CREATE', key1)
        client.execute_command('TS.ADD', key1, 1000, 10.0)

        # Series 2: With labels and retention
        key2 = 'series:labeled'
        client.execute_command('TS.CREATE', key2,
                               'RETENTION', 300000,
                               'LABELS', 'type', 'sensor', 'id', '123')
        for i in range(20):
            client.execute_command('TS.ADD', key2, 1000 + i * 1000, i * 2.0)

        # Series 3: With compaction (source)
        key3 = 'series:source'
        key4 = 'series:compacted'
        client.execute_command('TS.CREATE', key3, 'CHUNK_SIZE', 64)
        client.execute_command('TS.CREATE', key4)
        client.execute_command('TS.CREATERULE', key3, key4, 'AGGREGATION', 'sum', 5000)

        # Add data to trigger compaction
        base_ts = 10000
        for i in range(30):
            client.execute_command('TS.ADD', key3, base_ts + i * 200, i)

        # Series 5: Large series with multiple chunks
        key5 = 'series:large'
        client.execute_command('TS.CREATE', key5, 'CHUNK_SIZE', 128)
        for i in range(200):
            client.execute_command('TS.ADD', key5, 20000 + i * 100, i * 0.5)

        all_keys = [key1, key2, key3, key4, key5]

        # Get original states
        original_states = {}
        for key in all_keys:
            info = get_info(client, key)
            samples = client.execute_command('TS.RANGE', key, '-', '+')
            original_states[key] = {'info': info, 'samples': samples}

        # Verify we have the expected number of keys
        assert self.num_keys() == len(all_keys)

        # Save and restart
        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Verify all series are restored correctly
        assert self.num_keys() == len(all_keys)

        for key in all_keys:
            # Verify key exists
            assert client.execute_command('EXISTS', key) == 1

            # Verify info and samples
            restored_info = get_info(client, key)
            restored_samples = client.execute_command('TS.RANGE', key, '-', '+')

            # Remove memory usage for comparison
            del original_states[key]['info']['memoryUsage']
            del restored_info['memoryUsage']

            assert restored_info == original_states[key]['info']
            assert restored_samples == original_states[key]['samples']

        # Verify compaction rule still works
        client.execute_command('TS.ADD', key3, base_ts + 50000, 999)
        new_compacted_samples = client.execute_command('TS.RANGE', key4, '-', '+')
        assert len(new_compacted_samples) >= len(original_states[key4]['samples'])

    # ------------------------------------------------------------------
    # Encoding-specific RDB round-trip helpers and tests
    # ------------------------------------------------------------------

    def _roundtrip_encoding(self, client, encoding, expected_encoding, expected_chunk_type, key=None):
        """
        Create a series with the given ENCODING, populate it with enough
        samples to span multiple chunks, snapshot its state, perform a
        bgsave/restart cycle, and assert that the encoding name, sample
        data, and per-key digest are all identical after restore.

        Returns the key name so callers can do additional assertions.
        """
        if key is None:
            key = f'enc_roundtrip:{encoding.lower()}'

        # Use a small CHUNK_SIZE (128 bytes) to guarantee multiple chunks
        # are created, exercising both the header and per-chunk RDB paths.
        client.execute_command(
            'TS.CREATE', key,
            'ENCODING', encoding,
            'CHUNK_SIZE', 128,
            'LABELS', 'encoding', encoding.lower(),
        )

        # Add 200 samples to force the series across many chunks
        base_ts = 1_000_000
        for i in range(200):
            client.execute_command('TS.ADD', key, base_ts + i * 1000, float(i) * 1.5)

        # Snapshot state before save
        pre_info = get_info(client, key)
        pre_samples = client.execute_command('TS.RANGE', key, '-', '+')
        pre_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', key)

        # Confirm the encoding is what we expect before the cycle
        assert pre_info['encoding'] == expected_encoding, (
            f"Expected encoding '{expected_encoding}' before save, "
            f"got '{pre_info['encoding']}'"
        )
        assert pre_info['chunkType'] == expected_chunk_type, (
            f"Expected chunkType '{expected_chunk_type}' before save, "
            f"got '{pre_info['chunkType']}'"
        )
        assert pre_info['totalSamples'] == 200

        # Save RDB and restart the server
        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Snapshot state after restore
        post_info = get_info(client, key)
        post_samples = client.execute_command('TS.RANGE', key, '-', '+')
        post_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', key)

        # Encoding must survive the round-trip
        assert post_info['encoding'] == expected_encoding, (
            f"Encoding changed after restore: expected '{expected_encoding}', "
            f"got '{post_info['encoding']}'"
        )
        assert post_info['chunkType'] == expected_chunk_type, (
            f"chunkType changed after restore: expected '{expected_chunk_type}', "
            f"got '{post_info['chunkType']}'"
        )

        # All samples must be identical
        assert post_samples == pre_samples, (
            f"Samples changed after restore for encoding '{encoding}'"
        )

        # Digest must be byte-for-byte identical
        assert post_digest == pre_digest, (
            f"Digest changed after restore for encoding '{encoding}'"
        )

        # Info must match (excluding memoryUsage which can legitimately differ)
        del pre_info['memoryUsage']
        del post_info['memoryUsage']
        assert post_info == pre_info, (
            f"TS.INFO changed after restore for encoding '{encoding}'"
        )

        return key

    def test_save_restore_uncompressed_encoding(self):
        """RDB round-trip preserves data and encoding for UNCOMPRESSED chunks."""
        client = self.server.get_new_client()
        self._roundtrip_encoding(client, 'UNCOMPRESSED', 'uncompressed', 'uncompressed')

    def test_save_restore_gorilla_encoding(self):
        """RDB round-trip preserves data and encoding for GORILLA chunks."""
        client = self.server.get_new_client()
        self._roundtrip_encoding(client, 'GORILLA', 'gorilla', 'compressed')

    def test_save_restore_chimp_encoding(self):
        """RDB round-trip preserves data and encoding for CHIMP chunks.

        Also verifies that the COMPRESSED alias resolves to the same
        chimp encoding after a save/restore cycle.
        """
        client = self.server.get_new_client()
        self._roundtrip_encoding(client, 'CHIMP', 'chimp', 'compressed')

        # COMPRESSED is an alias for the default encoding, Chimp – confirm the
        # encoding name stored to RDB is still 'chimp' after restore.
        alias_key = 'enc_roundtrip:compressed_alias'
        client.execute_command(
            'TS.CREATE', alias_key,
            'ENCODING', 'COMPRESSED',
            'CHUNK_SIZE', 128,
        )
        for i in range(50):
            client.execute_command('TS.ADD', alias_key, 2_000_000 + i * 1000, float(i))

        pre_info = get_info(client, alias_key)
        assert pre_info['encoding'] == 'chimp', (
            "COMPRESSED alias should resolve to 'chimp' encoding"
        )

        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        post_info = get_info(client, alias_key)
        assert post_info['encoding'] == 'chimp', (
            "COMPRESSED alias: encoding should still read 'chimp' after restore"
        )
        assert post_info['chunkType'] == 'compressed'

    def test_save_restore_all_encodings_digest_match(self):
        """All three chunk encodings produce identical per-key digests after a
        single bgsave/restart cycle.  This catches any encoding-specific
        regression in a single test run.
        """
        client = self.server.get_new_client()

        encoding_cases = [
            ('UNCOMPRESSED', 'uncompressed', 'uncompressed'),
            ('GORILLA',      'gorilla',      'compressed'),
            ('CHIMP',        'chimp',        'compressed'),
        ]

        keys = []
        base_ts = 5_000_000
        for encoding, _exp_enc, _exp_ct in encoding_cases:
            key = f'all_enc:{encoding.lower()}'
            keys.append(key)
            client.execute_command(
                'TS.CREATE', key,
                'ENCODING', encoding,
                'CHUNK_SIZE', 128,
                'LABELS', 'encoding', encoding.lower(),
            )
            # Use 150 samples per series with varied values to stress the
            # encoding paths (mix of monotone increments and small floats)
            for i in range(150):
                client.execute_command(
                    'TS.ADD', key,
                    base_ts + i * 1000,
                    round(i * 0.73 + (i % 7) * 3.14, 4),
                )

        # Verify all series report the expected encoding before saving
        for (encoding, exp_enc, exp_ct), key in zip(encoding_cases, keys):
            info = get_info(client, key)
            assert info['encoding'] == exp_enc, (
                f"Pre-save: key '{key}' expected encoding '{exp_enc}', got '{info['encoding']}'"
            )
            assert info['chunkType'] == exp_ct, (
                f"Pre-save: key '{key}' expected chunkType '{exp_ct}', got '{info['chunkType']}'"
            )
            assert info['totalSamples'] == 150

        # Capture per-key digests before save
        pre_digests = {
            key: client.execute_command('DEBUG', 'DIGEST-VALUE', key)
            for key in keys
        }
        pre_server_digest = client.execute_command('DEBUG', 'DIGEST')

        # Save RDB and restart
        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Server-level digest must be identical
        post_server_digest = client.execute_command('DEBUG', 'DIGEST')
        assert post_server_digest == pre_server_digest, (
            "Server digest changed after restore across all encodings"
        )

        # Every per-key digest must be identical, and the encoding must be unchanged
        for (encoding, exp_enc, exp_ct), key in zip(encoding_cases, keys):
            post_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', key)
            assert post_digest == pre_digests[key], (
                f"Digest changed after restore for encoding '{encoding}' (key '{key}')"
            )

            post_info = get_info(client, key)
            assert post_info['encoding'] == exp_enc, (
                f"Post-restore: key '{key}' expected encoding '{exp_enc}', got '{post_info['encoding']}'"
            )
            assert post_info['chunkType'] == exp_ct, (
                f"Post-restore: key '{key}' expected chunkType '{exp_ct}', got '{post_info['chunkType']}'"
            )

            post_samples = client.execute_command('TS.RANGE', key, '-', '+')
            assert len(post_samples) == 150, (
                f"Sample count changed after restore for encoding '{encoding}'"
            )

    def test_save_restore_preserves_exact_digest(self):
        """Test that save/restore preserves exact digest for complex series"""
        client = self.server.get_new_client()
        keys = []

        # Create various series with different properties
        for i in range(5):
            key = f'digest_test_{i}'
            keys.append(key)

            if i == 0:
                # Basic series
                client.execute_command('TS.CREATE', key)
            elif i == 1:
                # Series with labels
                client.execute_command('TS.CREATE', key, 'LABELS', 'idx', str(i))
            elif i == 2:
                # Series with custom chunk size
                client.execute_command('TS.CREATE', key, 'CHUNK_SIZE', 64)
            elif i == 3:
                # Series with retention
                client.execute_command('TS.CREATE', key, 'RETENTION', 600000)
            else:
                # Series with multiple properties
                client.execute_command('TS.CREATE', key,
                                       'CHUNK_SIZE', 48,
                                       'RETENTION', 300000,
                                       'LABELS', 'test', 'multi')

            # Add data to each series
            for j in range(20 + i * 10):
                client.execute_command('TS.ADD', key, 1000 + j * 1000, j * (i + 1))

        # Get original digests
        original_server_digest = client.execute_command('DEBUG DIGEST')
        original_object_digests = {}
        for key in keys:
            original_object_digests[key] = client.execute_command('DEBUG DIGEST-VALUE', key)

        # Save and restart
        client.bgsave()
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Verify digests are identical
        restored_server_digest = client.execute_command('DEBUG DIGEST')
        assert restored_server_digest == original_server_digest

        for key in keys:
            restored_digest = client.execute_command('DEBUG DIGEST-VALUE', key)
            assert restored_digest == original_object_digests[key]
