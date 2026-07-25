import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseDebugMode


class TestTimeSeriesDebug(ValkeyTimeSeriesTestCaseDebugMode):
    """Test suite for TS._DEBUG command"""

    def set_debug_mode(self, value: bool = True):
        """Override to set debug mode before each test"""
        config_value = 'yes' if value else 'no'
        self.client.execute_command('CONFIG', 'SET', 'ts.debug-mode', config_value)

    def test_debug_help(self):
        """Test TS._DEBUG HELP subcommand"""
        self.set_debug_mode()

        result = self.client.execute_command('TS._DEBUG', 'HELP')

        # The result should be a flat array of command-description pairs
        assert isinstance(result, list)
        assert len(result) % 2 == 0  # Even number of elements
        assert len(result) >= 2  # At least one help entry

        # Check for expected subcommands in help text
        commands = [result[i] for i in range(0, len(result), 2)]
        assert any(b'STRINGPOOLSTATS' in cmd for cmd in commands)
        assert any(b'LIST_CONFIGS' in cmd for cmd in commands)

    def test_debug_help_no_extra_args(self):
        """Test TS._DEBUG HELP rejects extra arguments"""
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command('TS._DEBUG', 'HELP', 'extra')

    def test_debug_stringpoolstats(self):
        """Test TS._DEBUG STRINGPOOLSTATS subcommand"""
        self.set_debug_mode()
        # Create some time series with labels to populate the string pool
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'sensor', 'temp', 'room', 'A')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'sensor', 'humidity', 'room', 'B')

        result = self.client.execute_command('TS._DEBUG', 'STRINGPOOLSTATS')

        # Result should be an array with 4 elements: [GlobalStats, ByRefcount, BySize, <reserved>]
        assert isinstance(result, list)
        assert len(result) == 4

        print(result)

        # First element: GlobalStats - should be a bucket stats array
        global_stats = result[0]
        print(global_stats)
        assert isinstance(global_stats, list)
        assert len(global_stats) == 12  # 6 key-value pairs

        # Check that expected stat keys are present
        stat_keys = [global_stats[i].decode('utf-8') for i in range(0, len(global_stats), 2)]
        assert 'count' in stat_keys
        assert 'bytes' in stat_keys
        assert 'avgSize' in stat_keys
        assert 'allocated' in stat_keys
        assert 'avgAllocated' in stat_keys
        assert 'utilization' in stat_keys

        # Second element: ByRefcount - array of (refcount, bucket) pairs
        by_refcount = result[1]
        assert isinstance(by_refcount, list)

        # Third element: BySize - array of (size, bucket) pairs
        by_size = result[2]
        assert isinstance(by_size, list)

    def test_debug_stringpoolstats_no_extra_args(self):
        """Test TS._DEBUG STRINGPOOLSTATS rejects extra arguments"""
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command('TS._DEBUG', 'STRINGPOOLSTATS', 10, 'bah')

    def test_debug_list_configs_compact(self):
        """Test TS._DEBUG LIST_CONFIGS in compact mode (default)"""
        result = self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS')

        # Should return a flat array of config names
        assert isinstance(result, list)
        assert len(result) > 0

        # Check for expected config names
        config_names = [name.decode() if isinstance(name, bytes) else name for name in result]
        assert 'ts-chunk-size' in config_names
        assert 'ts-encoding' in config_names
        assert 'ts-duplicate-policy' in config_names
        assert 'ts-retention-policy' in config_names
        assert 'ts-num-threads' in config_names

    def test_debug_list_configs_verbose(self):
        """Test TS._DEBUG LIST_CONFIGS VERBOSE"""

        self.set_debug_mode()

        result = self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS', 'VERBOSE')

        # Should return an array of config detail arrays
        assert isinstance(result, list)
        assert len(result) > 0

        # Each config should have 16 elements (8 key-value pairs)
        for config_entry in result:
            assert isinstance(config_entry, list)
            assert len(config_entry) == 16

            # Convert to dict for easier assertion
            config_dict = {}
            for i in range(0, len(config_entry), 2):
                key = config_entry[i].decode() if isinstance(config_entry[i], bytes) else config_entry[i]
                value = config_entry[i + 1]
                config_dict[key] = value

            # Check required keys
            assert 'name' in config_dict
            assert 'type' in config_dict
            assert 'default' in config_dict
            assert 'min' in config_dict
            assert 'max' in config_dict
            assert 'value' in config_dict
            assert 'description' in config_dict
            assert 'mutable' in config_dict

            # Every config documents itself
            description = config_dict['description']
            if isinstance(description, bytes):
                description = description.decode()
            assert description.strip()

            # Validate type values
            config_type = config_dict['type'].decode() if isinstance(config_dict['type'], bytes) else config_dict[
                'type']
            assert config_type in ['integer', 'float', 'boolean', 'string', 'duration', 'enum']

    def test_debug_list_configs_verbose_case_insensitive(self):
        """Test TS._DEBUG LIST_CONFIGS with case-insensitive VERBOSE keyword"""
        result_upper = self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS', 'VERBOSE')
        result_lower = self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS', 'verbose')
        result_mixed = self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS', 'VeRbOsE')

        # All should return the same structure
        assert len(result_upper) == len(result_lower) == len(result_mixed)

    def test_debug_list_configs_specific_configs(self):
        """Test that LIST_CONFIGS includes all expected configuration parameters"""
        self.set_debug_mode()

        result = self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS', 'VERBOSE')

        config_names = []
        for config_entry in result:
            name_value = config_entry[1]  # Value after 'name' key
            name = name_value.decode() if isinstance(name_value, bytes) else name_value
            config_names.append(name)

        expected_configs = [
            'ts-chunk-size',
            'ts-encoding',
            'ts-duplicate-policy',
            'ts-retention-policy',
            'ts-compaction-policy',
            'ts-decimal-digits',
            'ts-significant-digits',
            'ts-ignore-max-time-diff',
            'ts-ignore-max-value-diff',
            'ts-num-threads',
            'ts-fanout-command-timeout',
            'ts-cluster-map-expiration-ms',
            'ts-index-build-max-memory',
            'ts-fanout-aggregation-pushdown',
            'ts-index-persist',
            'debug-mode',
        ]

        for expected in expected_configs:
            assert expected in config_names, f"Expected config '{expected}' not found"

        # LIST_CONFIGS reports from the module's configuration registry, so it must cover every
        # registered parameter and nothing else.
        assert sorted(config_names) == sorted(expected_configs)

    def test_debug_unknown_subcommand(self):
        """Test TS._DEBUG with an unknown subcommand"""
        with pytest.raises(ResponseError, match="Unknown subcommand.*try HELP"):
            self.client.execute_command('TS._DEBUG', 'INVALID_SUBCOMMAND')

    def test_debug_no_subcommand(self):
        """Test TS._DEBUG without any subcommand"""
        with pytest.raises(ResponseError):
            self.client.execute_command('TS._DEBUG')

    def test_debug_case_insensitive_subcommands(self):
        """Test that TS._DEBUG subcommands are case-insensitive"""
        # These should all work without errors
        self.client.execute_command('TS._DEBUG', 'help')
        self.client.execute_command('TS._DEBUG', 'HELP')
        self.client.execute_command('TS._DEBUG', 'HeLp')

        self.client.execute_command('TS._DEBUG', 'stringpoolstats')
        self.client.execute_command('TS._DEBUG', 'STRINGPOOLSTATS')

        self.client.execute_command('TS._DEBUG', 'list_configs')
        self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS')

    def test_debug_list_configs_invalid_argument(self):
        """Test TS._DEBUG LIST_CONFIGS with an invalid argument"""
        with pytest.raises(ResponseError, match="Syntax error"):
            self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS', 'INVALID')

    def test_debug_stringpoolstats_with_data(self):
        """Test that STRINGPOOLSTATS reflects actual string pool usage"""
        # Get baseline stats
        self.set_debug_mode()

        baseline = self.client.execute_command('TS._DEBUG', 'STRINGPOOLSTATS')
        baseline_count = baseline[0][1]  # Count from GlobalStats

        # Create multiple series with labels to increase string pool usage
        for i in range(10):
            self.client.execute_command(
                'TS.CREATE', f'ts_pool_{i}',
                'LABELS', 'sensor', f'sensor_{i}', 'location', f'loc_{i}'
            )

        # Get new stats
        new_stats = self.client.execute_command('TS._DEBUG', 'STRINGPOOLSTATS')
        new_count = new_stats[0][1]  # Count from GlobalStats

        # Should have more strings in the pool
        assert new_count > baseline_count

    def test_debug_config_values_match_defaults(self):
        """Test that LIST_CONFIGS VERBOSE shows correct default values"""
        result = self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS', 'VERBOSE')

        # Find ts-duplicate-policy config
        dup_policy_config = None
        for config_entry in result:
            config_dict = {}
            for i in range(0, len(config_entry), 2):
                key = config_entry[i].decode() if isinstance(config_entry[i], bytes) else config_entry[i]
                value = config_entry[i + 1]
                if isinstance(value, bytes):
                    value = value.decode()
                config_dict[key] = value

            if config_dict.get('name') == 'ts-duplicate-policy':
                dup_policy_config = config_dict
                break

        assert dup_policy_config is not None
        # Lowercase: LIST_CONFIGS reports the default the module actually registers with the
        # server, which is what CONFIG GET ts.ts-duplicate-policy returns.
        assert dup_policy_config['default'] == 'block'
        assert dup_policy_config['type'] == 'enum'

    def _verbose_configs(self) -> dict:
        """LIST_CONFIGS VERBOSE as {name: {field: value}}, with bytes decoded."""
        configs = {}
        for entry in self.client.execute_command('TS._DEBUG', 'LIST_CONFIGS', 'VERBOSE'):
            fields = {}
            for i in range(0, len(entry), 2):
                key = entry[i].decode() if isinstance(entry[i], bytes) else entry[i]
                value = entry[i + 1]
                fields[key] = value.decode() if isinstance(value, bytes) else value
            configs[fields['name']] = fields
        return configs

    def test_debug_list_configs_reports_live_values(self):
        """LIST_CONFIGS reports each parameter's current value, not its default."""
        self.set_debug_mode()

        before = self._verbose_configs()
        assert before['ts-chunk-size']['value'] == 4096
        assert before['ts-index-persist']['value'] == 'yes'
        assert before['ts-compaction-policy']['value'] == ''
        assert before['ts-decimal-digits']['value'] == 'none'

        # ts-num-threads cannot be changed after startup; everything else can
        assert before['ts-num-threads']['mutable'] == 'no'
        assert before['ts-chunk-size']['mutable'] == 'yes'

        self.client.config_set('ts.ts-chunk-size', 8192)
        self.client.config_set('ts.ts-index-persist', 'no')
        self.client.config_set('ts.ts-compaction-policy', 'max:1m:1h')
        self.client.config_set('ts.ts-decimal-digits', '3')
        self.client.config_set('ts.ts-retention-policy', '5000')

        after = self._verbose_configs()
        assert after['ts-chunk-size']['value'] == 8192
        assert after['ts-index-persist']['value'] == 'no'
        assert after['ts-compaction-policy']['value'] == 'max:1m:1h'
        assert after['ts-decimal-digits']['value'] == 3
        assert after['ts-retention-policy']['value'] == '5s'

        # Defaults are fixed metadata and must not follow the live value
        assert after['ts-chunk-size']['default'] == 4096
        assert after['ts-retention-policy']['default'] == '0ms'

        # Clearing the compaction policy must clear what is reported
        self.client.config_set('ts.ts-compaction-policy', '')
        assert self._verbose_configs()['ts-compaction-policy']['value'] == ''

        # Rounding is mutually exclusive: setting decimal digits leaves significant unset
        assert after['ts-significant-digits']['value'] == 'none'


def _parse_top_k_entry(entry: list) -> dict:
    """Parse a flat 8-element TopKEntry array into a dict."""
    assert len(entry) == 8, f"Expected 8 elements in TopKEntry, got {len(entry)}: {entry}"
    result = {}
    for i in range(0, len(entry), 2):
        key = entry[i].decode() if isinstance(entry[i], bytes) else entry[i]
        val = entry[i + 1]
        if isinstance(val, bytes):
            val = val.decode()
        result[key] = val
    return result


class TestStringPoolStatsTopK(ValkeyTimeSeriesTestCaseDebugMode):
    """Tests for STRINGPOOLSTATS top-k entries (Reply[4]=TopKByRef, Reply[5]=TopKBySize)."""

    # ── helpers ──────────────────────────────────────────────────────────────

    def _stats(self, k: int) -> list:
        return self.client.execute_command("TS._DEBUG", "STRINGPOOLSTATS", str(k))

    def _top_k_by_ref(self, k: int) -> list[dict]:
        return [_parse_top_k_entry(e) for e in self._stats(k)[4]]

    def _top_k_by_size(self, k: int) -> list[dict]:
        return [_parse_top_k_entry(e) for e in self._stats(k)[5]]

    # ── response shape ────────────────────────────────────────────────────────

    def test_topk_response_has_six_elements_when_k_gt_zero(self):
        self.client.execute_command("TS.CREATE", "ts_shape", "LABELS", "x", "y")
        result = self._stats(1)
        assert len(result) == 6, f"Expected 6 reply elements with k>0, got {len(result)}"

    def test_no_topk_without_k_argument(self):
        self.client.execute_command("TS.CREATE", "ts_nok", "LABELS", "a", "b")
        result = self.client.execute_command("TS._DEBUG", "STRINGPOOLSTATS")
        assert len(result) == 4, f"Expected 4 reply elements without k, got {len(result)}"

    def test_topk_entry_has_expected_keys(self):
        self.client.execute_command("TS.CREATE", "ts_keys", "LABELS", "env", "prod")
        entries = self._top_k_by_size(1)
        assert len(entries) >= 1
        assert set(entries[0].keys()) == {"value", "refCount", "bytes", "allocated"}

    # ── top-k by size ─────────────────────────────────────────────────────────

    def test_topk_by_size_count_equals_k(self):
        for i in range(5):
            label = f"label_topk_size_{i}" + ("x" * i)  # ascending lengths
            self.client.execute_command("TS.CREATE", f"ts_sz_{i}", "LABELS", "k", label)
        entries = self._top_k_by_size(3)
        assert len(entries) == 3

    def test_topk_by_size_sorted_descending(self):
        for i in range(5):
            label = "a" * (i + 1)
            self.client.execute_command("TS.CREATE", f"ts_desc_{i}", "LABELS", "k", label)
        entries = self._top_k_by_size(4)
        sizes = [e["bytes"] for e in entries]
        assert sizes == sorted(sizes, reverse=True), f"Not sorted descending: {sizes}"

    def test_topk_by_size_largest_string_is_first(self):
        long_label = "z" * 40
        self.client.execute_command("TS.CREATE", "ts_long", "LABELS", "k", long_label)
        self.client.execute_command("TS.CREATE", "ts_short", "LABELS", "k", "tiny")
        entries = self._top_k_by_size(1)
        assert len(entries) == 1
        assert entries[0]["value"] == "k=" + long_label

    def test_topk_by_size_bytes_field_matches_string_length(self):
        label = "exactlength"
        self.client.execute_command("TS.CREATE", "ts_exact", "LABELS", "mykey", label)
        entries = self._top_k_by_size(5)
        expected = "mykey=" + label
        matched = [e for e in entries if e["value"] == expected]
        assert matched, f"'{label}' not found in top-k by size"
        assert matched[0]["bytes"] == len(expected)

    def test_topk_by_size_allocated_gte_bytes(self):
        self.client.execute_command("TS.CREATE", "ts_alloc", "LABELS", "k", "some_value")
        for entry in self._top_k_by_size(5):
            assert entry["allocated"] >= entry["bytes"], (
                f"allocated ({entry['allocated']}) < bytes ({entry['bytes']}) "
                f"for '{entry['value']}'"
            )

    def test_topk_by_size_k_larger_than_pool_returns_all(self):
        # 3 distinct labels with unique names to guarantee pool presence
        for v in ("uniq_alpha", "uniq_beta", "uniq_gamma"):
            self.client.execute_command("TS.CREATE", f"ts_{v}", "LABELS", "k", v)
        entries = self._top_k_by_size(1000)
        values = [e["value"] for e in entries]
        for v in ("k=uniq_alpha", "k=uniq_beta", "k=uniq_gamma"):
            assert v in values

    # ── top-k by ref count ────────────────────────────────────────────────────

    def test_topk_by_ref_count_equals_k(self):
        for i in range(4):
            self.client.execute_command(
                "TS.CREATE", f"ts_ref_cnt_{i}", "LABELS", "sensor", f"ref_sensor_{i}"
            )
        entries = self._top_k_by_ref(2)
        assert len(entries) == 2

    def test_topk_by_ref_sorted_descending(self):
        # Re-use the same label across multiple series to drive up its ref count
        shared = "shared_label_ref_sort"
        for i in range(5):
            self.client.execute_command(
                "TS.CREATE", f"ts_refsort_{i}", "LABELS", "k", shared
            )
        # Add some unique labels with lower ref counts
        for i in range(3):
            self.client.execute_command(
                "TS.CREATE", f"ts_uniqref_{i}", "LABELS", "k", f"unique_ref_sort_{i}"
            )
        entries = self._top_k_by_ref(5)
        ref_counts = [e["refCount"] for e in entries]
        assert ref_counts == sorted(ref_counts, reverse=True), (
            f"top_k_by_ref not sorted descending: {ref_counts}"
        )

    def test_topk_by_ref_most_referenced_label_is_first(self):
        shared = "hottest_label_ref"
        # Create many series sharing the same label value → high ref count
        for i in range(8):
            self.client.execute_command(
                "TS.CREATE", f"ts_hot_{i}", "LABELS", "k", shared
            )
        self.client.execute_command("TS.CREATE", "ts_cold", "LABELS", "k", "cold_label_ref")
        entries = self._top_k_by_ref(2)
        assert entries[0]["value"] == "k=" + shared, (
            f"Expected '{shared}' to be most-referenced, got '{entries[0]['value']}'"
        )
        assert entries[0]["refCount"] > entries[1]["refCount"]

    def test_topk_by_ref_ref_count_field_is_positive(self):
        self.client.execute_command("TS.CREATE", "ts_posref", "LABELS", "k", "posref_val")
        for entry in self._top_k_by_ref(5):
            assert entry["refCount"] >= 1, (
                f"refCount should be >= 1, got {entry['refCount']} for '{entry['value']}'"
            )

    def test_topk_by_ref_low_ref_label_excluded_when_k_is_small(self):
        shared = "multi_ref_excl"
        for i in range(6):
            self.client.execute_command(
                "TS.CREATE", f"ts_mref_{i}", "LABELS", "k", shared
            )
        # This unique label will have a much lower ref count
        self.client.execute_command("TS.CREATE", "ts_rare", "LABELS", "k", "rare_label_excl")
        entries = self._top_k_by_ref(1)
        assert len(entries) == 1
        assert entries[0]["value"] == "k=" + shared
        assert all(e["value"] != "k=rare_label_excl" for e in entries)

    # ── entry field cross-checks ──────────────────────────────────────────────

    def test_topk_entry_value_is_non_empty_string(self):
        self.client.execute_command("TS.CREATE", "ts_nonempty", "LABELS", "k", "nonempty_val")
        for entry in self._top_k_by_size(5):
            assert isinstance(entry["value"], str) and len(entry["value"]) > 0

    def test_topk_by_size_and_by_ref_same_k_consistent_pool(self):
        """Both lists must reflect the same pool snapshot: no unknown values."""
        for i in range(3):
            self.client.execute_command(
                "TS.CREATE", f"ts_cross_{i}", "LABELS", "k", f"cross_val_{i}"
            )
        result = self._stats(3)
        by_ref_values = {_parse_top_k_entry(e)["value"] for e in result[4]}
        by_size_values = {_parse_top_k_entry(e)["value"] for e in result[5]}
        # Both sets must be non-empty and contain only strings
        assert all(isinstance(v, str) for v in by_ref_values | by_size_values)


def _parse_memory_savings(result: list) -> dict:
    """Parse the flat 4-element MemorySavings array (result[3]) into a dict."""
    savings = result[3]
    assert len(savings) == 4, f"Expected 4 elements in MemorySavings, got {len(savings)}"
    return {
        savings[0].decode(): int(savings[1]),  # memorySavedBytes -> int
        savings[2].decode(): float(savings[3]),  # memorySavedPct   -> float
    }


class TestStringPoolStatsLargeScale(ValkeyTimeSeriesTestCaseDebugMode):
    """Large-scale top-k and memory-savings tests for STRINGPOOLSTATS."""

    # Number of series / unique label values used throughout these tests.
    NUM_SERIES = 200
    # How many distinct label values are shared across all series (drives ref counts up).
    NUM_SHARED_VALUES = 5
    # k used for top-k requests.
    K = 10

    def _stats(self, k: int) -> list:
        return self.client.execute_command("TS._DEBUG", "STRINGPOOLSTATS", str(k))

    def _setup_large_pool(self):
        """
        Create NUM_SERIES time-series so that:
        - 'region' label cycles over NUM_SHARED_VALUES values  → high ref counts
        - 'uid'    label is unique per series                  → large pool size
        - 'descr'  label has a long value on every 10th series → large strings in pool
        """
        shared_regions = [f"large_region_{r}" for r in range(self.NUM_SHARED_VALUES)]
        for i in range(self.NUM_SERIES):
            region = shared_regions[i % self.NUM_SHARED_VALUES]
            uid = f"large_uid_{i:04d}"
            labels = ["region", region, "uid", uid]
            if i % 10 == 0:
                # Long unique description to populate top-k-by-size
                labels += ["descr", f"large_description_series_{i:04d}_" + "x" * 30]
            self.client.execute_command("TS.CREATE", f"ts_large_{i}", "LABELS", *labels)

    # ── top-k correctness at scale ────────────────────────────────────────────

    def test_large_topk_by_ref_returns_exactly_k(self):
        self._setup_large_pool()
        result = self._stats(self.K)
        by_ref = [_parse_top_k_entry(e) for e in result[4]]
        assert len(by_ref) == self.K, f"Expected {self.K} entries, got {len(by_ref)}"

    def test_large_topk_by_size_returns_exactly_k(self):
        self._setup_large_pool()
        result = self._stats(self.K)
        by_size = [_parse_top_k_entry(e) for e in result[5]]
        assert len(by_size) == self.K, f"Expected {self.K} entries, got {len(by_size)}"

    def test_large_topk_by_ref_sorted_descending(self):
        self._setup_large_pool()
        result = self._stats(self.K)
        ref_counts = [_parse_top_k_entry(e)["refCount"] for e in result[4]]
        assert ref_counts == sorted(ref_counts, reverse=True), (
            f"top_k_by_ref not sorted descending: {ref_counts}"
        )

    def test_large_topk_by_size_sorted_descending(self):
        self._setup_large_pool()
        result = self._stats(self.K)
        sizes = [_parse_top_k_entry(e)["bytes"] for e in result[5]]
        assert sizes == sorted(sizes, reverse=True), (
            f"top_k_by_size not sorted descending: {sizes}"
        )

    def test_large_topk_by_ref_top_entries_are_shared_region_labels(self):
        """The most-referenced strings must be the shared region label values."""
        self._setup_large_pool()
        result = self._stats(self.K)
        top_values = [_parse_top_k_entry(e)["value"] for e in result[4]]

        expected = {f"region=large_region_{r}" for r in range(self.NUM_SHARED_VALUES)}
        actual = set(top_values)

        missing = expected - actual
        assert not missing, (
            f"Missing shared region labels from top-k by ref. Missing={sorted(missing)} "
            f"TopValues={top_values}"
        )

    def test_large_topk_by_ref_top_entry_ref_count_reflects_sharing(self):
        """The highest ref count must be >= NUM_SERIES / NUM_SHARED_VALUES."""
        self._setup_large_pool()
        result = self._stats(self.K)
        top_ref_count = _parse_top_k_entry(result[4][0])["refCount"]
        min_expected = self.NUM_SERIES // self.NUM_SHARED_VALUES
        assert top_ref_count >= min_expected, (
            f"Top refCount {top_ref_count} < expected minimum {min_expected}"
        )

    def test_large_topk_by_size_top_entries_are_long_descriptions(self):
        """Long 'descr' label values must dominate the top-k-by-size list."""
        self._setup_large_pool()
        result = self._stats(self.K)
        by_size = [_parse_top_k_entry(e) for e in result[5]]

        # Every long description is > 40 bytes; uid / region labels are < 20 bytes.
        assert by_size[0]["bytes"] > 40, (
            f"Expected a long description at position 0, got {by_size[0]}"
        )

    def test_large_topk_all_entry_fields_valid(self):
        """Every entry across both top-k lists must have internally consistent fields."""
        self._setup_large_pool()
        result = self._stats(self.K)
        for entry_raw in result[4] + result[5]:
            e = _parse_top_k_entry(entry_raw)
            assert isinstance(e["value"], str) and len(e["value"]) > 0
            assert e["bytes"] == len(e["value"].encode()), (
                f"bytes field mismatch for '{e['value']}': "
                f"reported {e['bytes']}, actual {len(e['value'].encode())}"
            )
            assert e["allocated"] >= e["bytes"], (
                f"allocated ({e['allocated']}) < bytes ({e['bytes']}) for '{e['value']}'"
            )
            assert e["refCount"] >= 1

    # ── memory savings assertions ─────────────────────────────────────────────

    def test_memory_savings_structure(self):
        self._setup_large_pool()
        result = self._stats(self.K)
        savings = _parse_memory_savings(result)
        assert set(savings.keys()) == {"memorySavedBytes", "memorySavedPct"}, (
            f"Unexpected keys: {savings.keys()}"
        )

    def test_memory_savings_positive_when_labels_shared(self):
        """Shared label values must produce non-zero savings."""
        self._setup_large_pool()
        result = self._stats(self.K)
        savings = _parse_memory_savings(result)
        assert savings["memorySavedBytes"] > 0, (
            "Expected memorySavedBytes > 0 with shared labels"
        )
        assert savings["memorySavedPct"] > 0.0, (
            "Expected memorySavedPct > 0 with shared labels"
        )

    def test_memory_savings_pct_between_0_and_100(self):
        self._setup_large_pool()
        result = self._stats(self.K)
        savings = _parse_memory_savings(result)
        assert 0.0 <= savings["memorySavedPct"] <= 100.0, (
            f"memorySavedPct out of range: {savings['memorySavedPct']}"
        )

    def test_memory_savings_pct_significant_with_high_sharing(self):
        """
        With NUM_SERIES series sharing only NUM_SHARED_VALUES region labels,
        savings should be substantial (> 30 %).
        """
        self._setup_large_pool()
        result = self._stats(self.K)
        savings = _parse_memory_savings(result)
        assert savings["memorySavedPct"] > 30.0, (
            f"Expected >30% memory savings with high label sharing, "
            f"got {savings['memorySavedPct']:.1f}%"
        )

    def test_memory_savings_zero_when_no_sharing(self):
        """Series with entirely unique labels must report zero savings."""
        for i in range(20):
            self.client.execute_command(
                "TS.CREATE", f"ts_nosave_{i}",
                "LABELS", "uid_nosave", f"nosave_unique_value_{i:04d}"
            )
        result = self._stats(self.K)
        savings = _parse_memory_savings(result)
        assert savings["memorySavedBytes"] == 0, (
            f"Expected memorySavedBytes == 0 with unique-only labels, "
            f"got {savings['memorySavedBytes']}"
        )
        assert savings["memorySavedPct"] == 0.0, (
            f"Expected memorySavedPct == 0.0, got {savings['memorySavedPct']}"
        )

    def test_memory_savings_increases_with_more_sharing(self):
        """Adding more series that share the same label value increases saved bytes."""
        shared = "savings_growth_label"

        # Baseline: a few series sharing the label
        for i in range(5):
            self.client.execute_command(
                "TS.CREATE", f"ts_sg_base_{i}", "LABELS", "k", shared
            )
        baseline = _parse_memory_savings(self._stats(self.K))

        # Add many more series with the same shared label
        for i in range(5, 50):
            self.client.execute_command(
                "TS.CREATE", f"ts_sg_extra_{i}", "LABELS", "k", shared
            )
        after = _parse_memory_savings(self._stats(self.K))

        assert after["memorySavedBytes"] > baseline["memorySavedBytes"], (
            f"Savings did not increase: baseline={baseline['memorySavedBytes']}, "
            f"after={after['memorySavedBytes']}"
        )
        assert after["memorySavedPct"] >= baseline["memorySavedPct"]

    def test_memory_savings_consistent_with_top_k_ref_counts(self):
        """
        The saved bytes reported must be >= the savings implied by the top-k-by-ref
        entries alone (since top-k is only a subset of the full pool).
        """
        self._setup_large_pool()
        result = self._stats(self.K)
        savings = _parse_memory_savings(result)

        # Lower-bound savings from top-k-by-ref entries only
        topk_implied_savings = sum(
            _parse_top_k_entry(e)["allocated"] * (_parse_top_k_entry(e)["refCount"] - 1)
            for e in result[4]
            if _parse_top_k_entry(e)["refCount"] > 1
        )
        assert savings["memorySavedBytes"] >= topk_implied_savings, (
            f"Total savings ({savings['memorySavedBytes']}) < "
            f"implied by top-k entries alone ({topk_implied_savings})"
        )
