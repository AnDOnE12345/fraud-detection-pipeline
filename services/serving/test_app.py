from unittest import TestCase
from unittest.mock import patch
import pandas as pd
from app import category_stats, _read


class CategoryStatsTest(TestCase):
    def row(self, partition, offset, excluded=0):
        return {"source_topic": "transactions", "source_partition": partition,
                "source_offset": offset, "stats_window_start": "2026-08-03T12:00:00+00:00",
                "stats_window_end": "2026-08-03T12:01:00+00:00", "merchant_category": "shop",
                "is_fraud": partition, "fraud_score": partition * 0.5,
                "ingest_time": "2026-08-03T12:00:30+00:00", "velocity_excluded": excluded}

    def test_two_partitions_are_added_and_replay_is_deduplicated(self):
        rows = [self.row(0, i) for i in range(6)] + [self.row(1, i) for i in range(4)]
        result = category_stats(pd.DataFrame(rows + rows)).iloc[0]
        self.assertEqual(result["total"], 10)
        self.assertEqual(result["flagged"], 4)
        self.assertAlmostEqual(result["avg_fraud_score"], 0.2)

    def test_late_record_does_not_replace_historical_aggregate(self):
        rows = [self.row(0, i) for i in range(6)] + [self.row(1, 0, excluded=1)]
        self.assertEqual(category_stats(pd.DataFrame(rows)).iloc[0]["total"], 6)

    @patch("app.DeltaTable", side_effect=OSError("storage unavailable"))
    def test_storage_error_does_not_become_empty_dashboard(self, _table):
        with self.assertRaises(OSError):
            _read("s3://bucket/table")
