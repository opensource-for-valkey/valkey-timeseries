from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from data_helpers import load_power_consumption_data


class TestTsEncodingIntegration(ValkeyTimeSeriesTestCaseBase):
    MAX_GROUPS = 3
    MAX_SAMPLES_PER_GROUP = 60

    def _select_power_data(self):
        data = load_power_consumption_data()
        selected = {}
        for group_key in sorted(data.keys())[:self.MAX_GROUPS]:
            selected[group_key] = data[group_key][: self.MAX_SAMPLES_PER_GROUP]
        return selected

    def _ingest_for_encoding(self, encoding, selected_rows):
        keys_by_group = {}
        pipeline = self.client.pipeline(transaction=False)

        for group_key, rows in selected_rows.items():
            region, location_type = group_key.split(':')
            key = f"enc:{encoding.lower()}:{region}:{location_type}"
            keys_by_group[group_key] = key

            self.client.execute_command(
                "TS.CREATE",
                key,
                "ENCODING",
                encoding,
                "LABELS",
                "region",
                region,
                "location_type",
                location_type,
                "encoding",
                encoding.lower(),
            )

            for ts, consumption in rows:
                pipeline.execute_command("TS.ADD", key, ts, consumption)

        pipeline.execute()
        return keys_by_group

    def test_encoding_types_with_power_consumption_data(self):
        selected_rows = self._select_power_data()
        assert selected_rows, "Power consumption fixture returned no rows"

        cases = [
            ("UNCOMPRESSED", "uncompressed", "uncompressed"),
            ("COMPRESSED", "chimp", "compressed"),
            ("GORILLA", "gorilla", "compressed"),
            ("CHIMP", "chimp", "compressed"),
        ]

        keys_by_encoding = {}

        for encoding, expected_encoding, expected_chunk_type in cases:
            keys_by_group = self._ingest_for_encoding(encoding, selected_rows)
            keys_by_encoding[encoding] = keys_by_group

            for group_key, key in keys_by_group.items():
                info = self.ts_info(key)
                expected_samples = len(selected_rows[group_key])
                assert info["encoding"] == expected_encoding
                assert info["chunkType"] == expected_chunk_type
                assert info["totalSamples"] == expected_samples

        baseline_encoding = "UNCOMPRESSED"
        for group_key in selected_rows.keys():
            baseline_key = keys_by_encoding[baseline_encoding][group_key]
            baseline_rows = self.client.execute_command("TS.RANGE", baseline_key, "-", "+")

            for encoding, _, _ in cases:
                current_key = keys_by_encoding[encoding][group_key]
                current_rows = self.client.execute_command("TS.RANGE", current_key, "-", "+")
                assert current_rows == baseline_rows

