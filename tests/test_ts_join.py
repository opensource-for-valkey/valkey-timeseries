import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTSJoin(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):
        """set up any state specific to the execution of the given class (which
        usually contains tests).
        """

        # Create a test series
        self.ts1 = "test_ts1"
        self.ts2 = "test_ts2"

        # Create two time series
        self.client.execute_command("TS.CREATE", self.ts1, "DUPLICATE_POLICY", "last")
        self.client.execute_command("TS.CREATE", self.ts2, "DUPLICATE_POLICY", "last")

        # Base timestamp
        self.now = 1000

        # Add data to the first series
        for i in range(10):
            ts = self.now + i * 1000
            self.client.execute_command("TS.ADD", self.ts1, ts, i * 10)

        # Add data to the second series (some overlapping, some unique)
        for i in range(5, 15):
            ts = self.now + i * 1000
            self.client.execute_command("TS.ADD", self.ts2, ts, i * 5)

    def test_inner_join(self):
        """Test inner join operation"""

        self.setup_data()

        # Inner join should only return matching timestamps
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "INNER"
        )

        # Should have 5 results (timestamps overlap from 5-9)
        assert len(result) == 5

        # Check the structure of the result
        for item in result:
            # Each row should have 2 elements: left sample, right sample
            assert len(item) == 2

            # Get the timestamp and values
            left, right = item

            ts = left[0]
            left_val = left[1]
            right_val = right[1]

            # Calculate expected values
            idx = (ts - self.now) // 1000
            expected_left = idx * 10
            expected_right = idx * 5

            # Verify values
            assert float(left_val) == expected_left
            assert float(right_val) == expected_right

    def test_left_join(self):
        """Test left-join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "LEFT"
        )

        # Should have 10 results (all timestamps from left series)
        assert len(result) == 10

        for item in result:
            left, right = item

            ts = left[0]
            left_val = left[1]

            # Calculate the expected left value
            idx = (ts - self.now) // 1000
            expected_left = idx * 10

            # Verify left value
            assert float(left_val) == expected_left

            # For timestamps 0-4, the right value should be None
            if idx < 5:
                assert right is None
            else:
                # For timestamps 5-9, the right value should match
                right_val = right[1]
                expected_right = idx * 5
                assert float(right_val) == expected_right

    def test_right_join(self):
        """Test right join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "RIGHT"
        )

        # Should have 10 results (all timestamps from the right series)
        assert len(result) == 10

        for item in result:
            left, right = item

            ts = right[0]
            right_val = float(right[1])

            # Calculate the expected right value
            idx = (ts - self.now) // 1000
            expected_right = idx * 5

            # Verify right value
            assert right_val == expected_right

            # For timestamps 10-14, the left value should be None
            if idx >= 10:
                assert left is None
            else:
                # For timestamps 5-9, the left value should match
                expected_left = idx * 10
                left_val = float(left[1])
                assert left_val == expected_left

    def test_full_join(self):
        """Test full join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "FULL"
        )

        # Should have 15 results (all unique timestamps from both series)
        assert len(result) == 15

        for item in result:
            left, right = item

            ts = left[0] if left is not None else right[0]

            # Calculate the index
            idx = (ts - self.now) // 1000

            # Check left value
            if idx < 10:
                expected_left = idx * 10
                left_val = float(left[1])
                assert left_val == expected_left
            else:
                assert left is None

            # Check the right value
            if 5 <= idx < 15:
                expected_right = idx * 5
                right_val = float(right[1])
                assert right_val == expected_right
            else:
                assert right is None

    def test_anti_join(self):
        """Test anti-join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "ANTI"
        )

        # Should return timestamps in the left that aren't in the right (0-4)
        assert len(result) == 5

        for item in result:
            left, right = item

            ts = left[0]
            left_val = left[1]
            # Calculate expected value
            idx = (ts - self.now) // 1000
            expected_left = idx * 10

            # Verify values
            assert float(left_val) == expected_left
            assert right is None
            assert idx < 5  # Only indexes 0-4 should be present

    def test_semi_join(self):
        """Test semi-join operation"""

        self.setup_data()

        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now, self.now + 15000,
            "SEMI"
        )

        # Should return timestamps in the left that are also in the right (5-9)
        assert len(result) == 5

        for item in result:
            left, right = item

            ts = left[0]
            # Calculate expected value
            idx = (ts - self.now) // 1000
            expected_left = idx * 10

            # Verify values
            left_val = left[1]
            assert float(left_val) == expected_left
            assert right is None  # The right value should be None
            assert 5 <= idx < 10  # Only indexes 5-9 should be present

    def test_asof_join(self):
        """Test as-of join operation with basic functionality"""

        self.setup_data()

        # Create a special series for as-of test with non-matching timestamps
        asof_ts = "test_asof"
        self.client.execute_command("TS.CREATE", asof_ts)

        # Add data with timestamps slightly before regular points
        for i in range(5):
            ts = self.now + i * 1000 - 200  # 200ms before each point
            self.client.execute_command("TS.ADD", asof_ts, ts, i * 100)

        # Test ASOF join with PREVIOUS strategy and tolerance
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, asof_ts,
            self.now, self.now + 10000,
            "ASOF", "PREVIOUS", "500"  # 500ms tolerance
        )

        # Verify we get matches within tolerance
        assert len(result) > 0

        for item in result:
            left, right = item

            ts = left[0]
            idx = (ts - self.now) // 1000

            # Check if within tolerance range
            if 0 < idx < 5:
                # Should have a match
                right_ts = right[0]
                right_val = float(right[1])
                # Verify the right timestamp is within tolerance
                assert abs(ts - right_ts) <= 500
                # Verify the value
                expected_idx = (right_ts - (self.now - 200)) // 1000
                assert float(right_val) == expected_idx * 100

    def test_reducer_coalesce(self):
        """Verify coalesce reducer returns the right operand when only right exists."""
        self.setup_data()

        # Right-only timestamps are indexes 10-14 (self.now+10000 .. self.now+14000)
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now + 10000, self.now + 14000,
            "RIGHT", "REDUCE", "coalesce"
        )

        assert len(result) == 5

        for ts, value in result:
            idx = (ts - self.now) // 1000
            expected_right = idx * 5
            assert float(value) == expected_right

    def test_reducer_cmp(self):
        """Test CMP reducer which returns -1, 0, or 1 based on comparison."""
        self.setup_data()

        # Test 1: INNER join with CMP - left < right
        # At indexes 5-9: left = idx*10, right = idx*5
        # So left > right, should return 1.0
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now + 5000, self.now + 9000,
            "INNER", "REDUCE", "cmp"
        )

        assert len(result) == 5
        for ts, value in result:
            # left > right, so cmp should return 1.0
            assert float(value) == 1.0

        # Test 2: LEFT join with equal values scenario
        # Create a special case where we can test equality
        ts_equal1 = "test_cmp_equal1"
        ts_equal2 = "test_cmp_equal2"
        self.client.execute_command("TS.CREATE", ts_equal1)
        self.client.execute_command("TS.CREATE", ts_equal2)

        base = 2000
        # Add matching values
        for i in range(5):
            ts = base + i * 1000
            self.client.execute_command("TS.ADD", ts_equal1, ts, i * 10)
            self.client.execute_command("TS.ADD", ts_equal2, ts, i * 10)

        result = self.client.execute_command(
            "TS.JOIN", ts_equal1, ts_equal2,
            base, base + 5000,
            "INNER", "REDUCE", "cmp"
        )

        assert len(result) == 5
        for ts, value in result:
            # Values are equal, so cmp should return 0.0
            assert float(value) == 0.0

        # Test 3: LEFT join with left < right scenario
        ts_less1 = "test_cmp_less1"
        ts_less2 = "test_cmp_less2"
        self.client.execute_command("TS.CREATE", ts_less1)
        self.client.execute_command("TS.CREATE", ts_less2)

        base = 3000
        for i in range(5):
            ts = base + i * 1000
            self.client.execute_command("TS.ADD", ts_less1, ts, (i + 1) * 5)  # Smaller values
            self.client.execute_command("TS.ADD", ts_less2, ts, (i + 1) * 10)  # Larger values

        result = self.client.execute_command(
            "TS.JOIN", ts_less1, ts_less2,
            base, base + 5000,
            "INNER", "REDUCE", "cmp"
        )

        assert len(result) == 5
        for ts, value in result:
            # left < right, so cmp should return -1.0
            assert float(value) == -1.0

    def test_reducer_pct_change(self):
        """Test PCT_CHANGE reducer which computes percentage change from left to right."""
        self.setup_data()

        # Test 1: INNER join with PCT_CHANGE
        # At indexes 5-9: left = idx*10, right = idx*5
        # pct_change = (right - left) / left = (idx*5 - idx*10) / (idx*10) = -0.5
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, self.ts2,
            self.now + 5000, self.now + 9000,
            "INNER", "REDUCE", "pct_change"
        )

        assert len(result) == 5
        for ts, value in result:
            idx = (ts - self.now) // 1000
            left_val = idx * 10
            right_val = idx * 5
            expected = (right_val - left_val) / left_val
            assert float(value) == expected
            assert float(value) == -0.5

        # Test 2: Positive percentage change
        ts_pos1 = "test_pct_pos1"
        ts_pos2 = "test_pct_pos2"
        self.client.execute_command("TS.CREATE", ts_pos1)
        self.client.execute_command("TS.CREATE", ts_pos2)

        base = 4000
        # Add values where right > left (positive growth)
        for i in range(1, 6):
            ts = base + i * 1000
            left_val = i * 10
            right_val = i * 20  # 100% increase
            self.client.execute_command("TS.ADD", ts_pos1, ts, left_val)
            self.client.execute_command("TS.ADD", ts_pos2, ts, right_val)

        result = self.client.execute_command(
            "TS.JOIN", ts_pos1, ts_pos2,
            base, base + 6000,
            "INNER", "REDUCE", "pct_change"
        )

        assert len(result) == 5
        for ts, value in result:
            # 100% increase: (20 - 10) / 10 = 1.0
            assert float(value) == 1.0

        # Test 3: Zero left value (division by zero edge case)
        ts_zero1 = "test_pct_zero1"
        ts_zero2 = "test_pct_zero2"
        self.client.execute_command("TS.CREATE", ts_zero1)
        self.client.execute_command("TS.CREATE", ts_zero2)

        base = 5000
        # When left = 0, should return 0.0 (per implementation)
        self.client.execute_command("TS.ADD", ts_zero1, base, 0.0)
        self.client.execute_command("TS.ADD", ts_zero2, base, 100.0)

        result = self.client.execute_command(
            "TS.JOIN", ts_zero1, ts_zero2,
            base, base + 1000,
            "INNER", "REDUCE", "pct_change"
        )

        assert len(result) == 1
        ts, value = result[0]
        # Division by zero case should return 0.0
        assert float(value) == 0.0

        # Test 4: Small values with fractional change
        ts_frac1 = "test_pct_frac1"
        ts_frac2 = "test_pct_frac2"
        self.client.execute_command("TS.CREATE", ts_frac1)
        self.client.execute_command("TS.CREATE", ts_frac2)

        base = 6000
        self.client.execute_command("TS.ADD", ts_frac1, base, 100.0)
        self.client.execute_command("TS.ADD", ts_frac2, base, 150.0)

        result = self.client.execute_command(
            "TS.JOIN", ts_frac1, ts_frac2,
            base, base + 1000,
            "INNER", "REDUCE", "pct_change"
        )

        assert len(result) == 1
        ts, value = result[0]
        # (150 - 100) / 100 = 0.5 (50% increase)
        assert float(value) == 0.5

    def test_asof_strategies(self):
        """Test different ASOF join strategies with comprehensive scenarios"""

        self.setup_data()

        # Create a special series for as-of test
        asof_ts = "test_asof_strategies"
        self.client.execute_command("TS.CREATE", asof_ts)

        # Add sparse data (only at specific points)
        # Points: 1500, 3500, 6500, 9500
        sparse_points = {
            self.now + 1500: 10,
            self.now + 3500: 20,
            self.now + 6500: 30,
            self.now + 9500: 40
        }
        for ts, val in sparse_points.items():
            self.client.execute_command("TS.ADD", asof_ts, ts, val)

        # Test PREVIOUS (backward) strategy
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, asof_ts,
            self.now, self.now + 10000,
            "ASOF", "PREVIOUS", "2000"
        )

        # Verify PREVIOUS matches
        for item in result:
            left, right = item

            ts = left[0]
            right_ts = right[0]
            right_val = float(right[1])

            if ts == self.now + 2000:
                # Should match with 1500 (closest previous within tolerance)
                assert right_ts == self.now + 1500
                assert float(right_val) == 10
            elif ts == self.now + 4000:
                # Should match with 3500
                assert right_ts == self.now + 3500
                assert float(right_val) == 20

        # Test NEXT (forward) strategy
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, asof_ts,
            self.now, self.now + 10000,
            "ASOF", "NEXT", "2000"
        )

        # Verify NEXT matches
        for item in result:
            left, right = item

            ts = left[0]
            right_ts = right[0]
            right_val = float(right[1])

            if ts == self.now + 2000:
                # Should match with 3500 (closest next within tolerance)
                assert right_ts == self.now + 3500
                assert float(right_val) == 20
            elif ts == self.now + 5000:
                # Should match with 6500
                assert right_ts == self.now + 6500
                assert float(right_val) == 30

        # Test NEAREST strategy
        result = self.client.execute_command(
            "TS.JOIN", self.ts1, asof_ts,
            self.now, self.now + 10000,
            "ASOF", "NEAREST", "2000"
        )

        # Verify NEAREST matches
        for item in result:
            left, right = item

            ts = left[0]
            right_ts = right[0]
            right_val = float(right[1])

            if ts == self.now + 2000:
                # Should match with 1500 (1500 is closer than 3500)
                assert right_ts == self.now + 1500
                assert float(right_val) == 10
            elif ts == self.now + 5000:
                # Should match with either 3500 or 6500 (both 1500ms away)
                # Implementation may choose either
                assert right_ts in [self.now + 3500, self.now + 6500]

    def test_asof_tolerance_boundaries(self):
        """Test ASOF join tolerance edge cases"""

        # Create test series
        left_series = "test_asof_left"
        right_series = "test_asof_right"
        self.client.execute_command("TS.CREATE", left_series)
        self.client.execute_command("TS.CREATE", right_series)

        base_time = 10000

        # Left series: points at 0, 1000, 2000, 3000, 4000
        for i in range(5):
            ts = base_time + i * 1000
            self.client.execute_command("TS.ADD", left_series, ts, i)

        # Right series: points at 500, 1700, 3200
        right_points = {
            base_time + 500: 100,
            base_time + 1700: 200,
            base_time + 3200: 300
        }
        for ts, val in right_points.items():
            self.client.execute_command("TS.ADD", right_series, ts, val)

        # Test with an exact tolerance boundary (PREVIOUS)
        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 5000,
            "ASOF", "PREVIOUS", "500"  # Exactly 500ms tolerance
        )

        # At timestamp 1000, the point at 500 is exactly 500ms away - should match
        matched_at_1000 = False

        # Validates series join at the exact tolerance boundary
        for item in result:
            left, right = item

            ts = left[0]
            right_stamp = right[0]
            right_val = float(right[1])

            if ts == base_time + 1000:
                assert right_stamp == base_time + 500
                assert right_val == 100
                matched_at_1000 = True

        assert matched_at_1000, "Should match at exactly tolerance boundary"

        # Test with just outside tolerance (PREVIOUS)
        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 5000,
            "ASOF", "PREVIOUS", "499"  # Just under 500ms tolerance
        )

        # At timestamp 1000, the point at 500 is 500ms away - should NOT match
        for item in result:
            left, right = item

            ts = left[0]

            if ts == base_time + 1000:
                # Should be None or not have the 500ms point
                if right is not None:
                    assert False, "Should not match outside tolerance"

        # Test with zero tolerance (exact matches only)
        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 5000,
            "ASOF", "NEAREST", "0"
        )

        # Should have no matches since there are no exact timestamp matches
        for item in result:
            left, right = item
            assert right is None, "Zero tolerance should require exact matches"

    def test_asof_with_no_matches(self):
        """Test ASOF join when no points are within tolerance"""

        # Create test series
        left_series = "test_asof_nomatch_left"
        right_series = "test_asof_nomatch_right"
        self.client.execute_command("TS.CREATE", left_series)
        self.client.execute_command("TS.CREATE", right_series)

        base_time = 20000

        # Left series: points at 0, 1000, 2000
        for i in range(3):
            ts = base_time + i * 1000
            self.client.execute_command("TS.ADD", left_series, ts, i)

        # Right series: points very far away (10 seconds later)
        for i in range(3):
            ts = base_time + 10000 + i * 1000
            self.client.execute_command("TS.ADD", right_series, ts, i * 100)

        # Test with small tolerance - no matches expected
        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 3000,
            "ASOF", "NEXT", "100"  # Only 100ms tolerance
        )

        assert len(result) == 0

    def test_asof_multiple_candidates(self):
        """Test ASOF join behavior when multiple candidates are within tolerance"""

        # Create test series
        left_series = "test_asof_multi_left"
        right_series = "test_asof_multi_right"
        self.client.execute_command("TS.CREATE", left_series)
        self.client.execute_command("TS.CREATE", right_series)

        base_time = 30000

        # Left series: single point
        self.client.execute_command("TS.ADD", left_series, base_time + 5000, 50)

        # Right series: multiple points near the left point
        right_points = {
            base_time + 4500: 100,  # 500ms before
            base_time + 4800: 200,  # 200ms before
            base_time + 5200: 300,  # 200ms after
            base_time + 5500: 400  # 500ms after
        }
        for ts, val in right_points.items():
            self.client.execute_command("TS.ADD", right_series, ts, val)

        # Test PREVIOUS - should match the closest previous
        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 10000,
            "ASOF", "PREVIOUS", "1000"
        )

        assert len(result) == 1
        left_val, right = result[0]
        assert right is not None
        right_ts = right[0]
        right_val = float(right[1])

        # Should match 4800 (closest previous within tolerance)
        assert right_ts == base_time + 4800
        assert right_val == 200

        # Test NEXT - should match closest next
        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 10000,
            "ASOF", "NEXT", "1000"
        )

        assert len(result) == 1
        left, right = result[0]
        assert right is not None

        right_ts = right[0]
        right_val = float(right[1])

        # Should match 5200 (closest next within tolerance)
        assert right_ts == base_time + 5200
        assert right_val == 300

        # Test NEAREST - should match the absolutely closest
        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 10000,
            "ASOF", "NEAREST", "1000"
        )

        assert len(result) == 1
        left, right = result[0]
        assert right is not None

        right_ts = right[0]

        # Should match either 4800 or 5200 (both 200ms away)
        assert right_ts in [base_time + 4800, base_time + 5200]

    def test_asof_with_reducer(self):
        """Test ASOF join combined with reducer operations"""

        # Create test series
        left_series = "test_asof_reducer_left"
        right_series = "test_asof_reducer_right"
        self.client.execute_command("TS.CREATE", left_series)
        self.client.execute_command("TS.CREATE", right_series)

        base_time = 40000

        # Left series: points at regular intervals
        for i in range(5):
            ts = base_time + i * 1000
            self.client.execute_command("TS.ADD", left_series, ts, i * 10)

        # Right series: slightly offset points
        for i in range(5):
            ts = base_time + i * 1000 - 100  # 100ms before each left point
            self.client.execute_command("TS.ADD", right_series, ts, i * 5)

        # Test ASOF with SUM reducer
        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 5000,
            "ASOF", "PREVIOUS", "200",
            "REDUCE", "sum"
        )

        # Verify reduced values
        for i, item in enumerate(result):
            ts, value = item
            expected_left = (i + 1) * 10
            expected_right = (i + 1) * 5
            expected_sum = expected_left + expected_right
            assert float(value) == expected_sum

    def smoketest_all_join_reducers(self):
        """Test different join reducers"""

        self.setup_data()

        reducers = [
            "abs_diff", "avg", "cmp", "div", "eq", "gt", "gte", "lt", "lte", "sum", "max", "min",
            "ne", "mul", "mod", "pow", "sgn_diff", "pct_change", "coalesce"
        ]

        for reducer in reducers:
            result = self.client.execute_command(
                "TS.JOIN", self.ts1, self.ts2,
                self.now + 5000, self.now + 6000,  # Test one point for each
                "INNER", "REDUCE", reducer
            )

            # Should have at least one result
            assert len(result) > 0

            # Each result should be a [timestamp, value] pair
            for item in result:
                assert len(item) == 2

    def test_asof_edge_cases(self):
        """Test ASOF join edge cases"""

        # Create test series
        left_series = "test_asof_edge_left"
        right_series = "test_asof_edge_right"
        self.client.execute_command("TS.CREATE", left_series)
        self.client.execute_command("TS.CREATE", right_series)

        base_time = 50000

        # Test with empty right series
        self.client.execute_command("TS.ADD", left_series, base_time, 100)

        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 1000,
            "ASOF", "NEAREST", "1000"
        )

        assert len(result) == 0

        # Test with single matching point
        self.client.execute_command("TS.ADD", right_series, base_time, 200)

        result = self.client.execute_command(
            "TS.JOIN", left_series, right_series,
            base_time, base_time + 1000,
            "ASOF", "NEAREST", "0"  # Exact match required
        )

        assert len(result) == 1
        left, right = result[0]
        assert right is not None

        right_ts = right[0]
        right_val = float(right[1])
        assert right_ts == base_time
        assert right_val == 200

    def test_error_cases(self):
        """Test error cases"""

        self.setup_data()

        # Test with the same key for both series
        with pytest.raises(ResponseError, match="TSDB: duplicate join keys"):
            self.client.execute_command(f"TS.JOIN {self.ts1} {self.ts1} {self.now} {self.now + 15000}")

        # Test with a non-existent key
        with pytest.raises(ResponseError, match="TSDB: the key does not exist"):
            self.client.execute_command(f"TS.JOIN {self.ts1} nonexistent {self.now} {self.now + 15000}")

        # Test with an invalid join type
        with pytest.raises(ResponseError, match="TSDB: invalid JOIN command argument"):
            self.client.execute_command(f"TS.JOIN {self.ts1} {self.ts2} {self.now} {self.now + 15000} INVALID_TYPE")

        # Test with invalid reducer
        with pytest.raises(ResponseError, match="TSDB: unknown binary op 'invalid_op'"):
            self.client.execute_command(
                f"TS.JOIN {self.ts1} {self.ts2} {self.now} {self.now + 15000} INNER REDUCE invalid_op"
            )

        # REDUCE must be disallowed for SEMI joins
        with pytest.raises(ResponseError, match="TSDB: cannot use REDUCE with SEMI or ANTI joins"):
            self.client.execute_command(
                f"TS.JOIN {self.ts1} {self.ts2} {self.now} {self.now + 15000} SEMI REDUCE sum"
            )

        # REDUCE must be disallowed for ANTI joins
        with pytest.raises(ResponseError, match="TSDB: cannot use REDUCE with SEMI or ANTI joins"):
            self.client.execute_command(
                f"TS.JOIN {self.ts1} {self.ts2} {self.now} {self.now + 15000} ANTI REDUCE sum"
            )

    def test_count_limits_joined_rows_not_inputs(self):
        """COUNT used to truncate each input first, so an INNER join whose matches lie past
        the first COUNT samples of the left series returned nothing."""
        self.setup_data()
        # ts1 covers 1000..10000, ts2 6000..15000: the first matches are at 6000 and 7000.
        result = self.client.execute_command("TS.JOIN", self.ts1, self.ts2, "-", "+", "COUNT", 2)
        # Each row is [left sample, right sample].
        assert [row[0][0] for row in result] == [6000, 7000]

    def test_asof_without_tolerance_is_unlimited(self):
        """An omitted TOLERANCE is documented as no limit; it used to mean exact matches only."""
        self.client.execute_command("TS.ADD", "asof:l", 10, 1)
        self.client.execute_command("TS.ADD", "asof:l", 20, 2)
        self.client.execute_command("TS.ADD", "asof:r", 5, 10)
        self.client.execute_command("TS.ADD", "asof:r", 15, 20)
        result = self.client.execute_command("TS.JOIN", "asof:l", "asof:r", "-", "+", "ASOF", "PREVIOUS")
        assert [(row[0][0], row[1][0]) for row in result] == [(10, 5), (20, 15)]

        exact_only = self.client.execute_command(
            "TS.JOIN", "asof:l", "asof:r", "-", "+", "ASOF", "PREVIOUS", 0)
        assert exact_only == []
