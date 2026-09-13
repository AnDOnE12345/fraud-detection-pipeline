from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
from deltalake import DeltaTable
from unittest import TestCase

from app import WindowState, enrich, fixed_window, fraud_signals, parse_event_time, process_batch, recover


class ProcessorRulesTest(TestCase):
    def test_fraud_score_components_are_bounded(self) -> None:
        amount_flag, merchant_flag, score, is_fraud = fraud_signals(1600.0, 0.9)

        self.assertEqual((amount_flag, merchant_flag, is_fraud), (1, 1, 1))
        self.assertEqual(score, 0.95)

    def test_normal_transaction_is_not_flagged(self) -> None:
        amount_flag, merchant_flag, score, is_fraud = fraud_signals(100.0, 0.1)

        self.assertEqual((amount_flag, merchant_flag, is_fraud), (0, 0, 0))
        self.assertEqual(score, 0.0813)

    def test_fixed_window_uses_minute_boundary(self) -> None:
        event_time = datetime(2026, 8, 3, 12, 34, 45, tzinfo=timezone.utc)

        start, end = fixed_window(event_time)

        self.assertEqual(start, datetime(2026, 8, 3, 12, 34, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 8, 3, 12, 35, tzinfo=timezone.utc))

    def test_missing_event_time_is_marked_as_fallback(self) -> None:
        parsed, used_fallback = parse_event_time(None)

        self.assertTrue(used_fallback)
        self.assertIsNotNone(parsed.tzinfo)


class RecoveryTest(TestCase):
    def setUp(self):
        self.at = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
        self.event = {"card_id": "card", "merchant_id": "merchant", "amount": 10.0}

    def row(self, state, offset, at=None, card="card"):
        return enrich({**self.event, "card_id": card, "event_time": (at or self.at).isoformat()},
                      0, offset, int(self.at.timestamp() * 1000), {}, state)

    def test_restart_preserves_eleventh_payment_alert(self):
        state = WindowState()
        records = [self.row(state, offset) for offset in range(10)]
        recovered = WindowState.restore(records)
        result = self.row(recovered, 10)
        self.assertEqual(result["tx_count"], 11)
        self.assertEqual(result["velocity_alert"], 1)

    def test_old_event_of_new_card_is_excluded_by_partition_watermark(self):
        state = WindowState()
        self.row(state, 0)
        result = self.row(state, 1, self.at - timedelta(minutes=10), "new-card")
        self.assertEqual(result["velocity_excluded"], 1)
        self.assertNotIn("new-card", state.cards)

    def test_inactive_cards_are_evicted(self):
        state = WindowState()
        for offset in range(100):
            self.row(state, offset, card=str(offset))
        self.row(state, 100, self.at + timedelta(minutes=4), "active")
        self.assertEqual(set(state.cards), {"active"})

    def test_lateness_boundary_and_out_of_order_window(self):
        state = WindowState()
        self.row(state, 0)
        boundary = self.row(state, 1, self.at - timedelta(seconds=120))
        self.assertEqual(boundary["velocity_excluded"], 0)
        self.assertEqual(boundary["tx_count"], 1)  # Later event is outside this window.
        self.assertEqual(self.row(state, 2, self.at - timedelta(seconds=121))["velocity_excluded"], 1)

    def test_invalid_time_fallback_is_replay_deterministic(self):
        a = enrich({**self.event, "event_time": 42}, 0, 0, 1000, {}, WindowState())
        b = enrich({**self.event, "event_time": 42}, 0, 0, 1000, {}, WindowState())
        self.assertEqual(a["event_time"], b["event_time"])
        self.assertEqual(a["event_time_fallback"], 1)

    def test_far_future_time_uses_kafka_timestamp_without_advancing_watermark(self):
        state = WindowState()
        kafka_timestamp = int(self.at.timestamp() * 1000)
        future = enrich(
            {**self.event, "event_time": (self.at + timedelta(days=1)).isoformat()},
            0, 0, kafka_timestamp, {}, state,
        )
        normal = enrich(
            {**self.event, "event_time": (self.at + timedelta(seconds=1)).isoformat()},
            0, 1, kafka_timestamp + 1000, {}, state,
        )

        self.assertEqual(future["event_time"], self.at.isoformat())
        self.assertEqual(future["event_time_fallback"], 1)
        self.assertEqual(normal["velocity_excluded"], 0)

    def test_delta_roundtrip_restart_and_replay(self):
        with TemporaryDirectory() as folder:
            with patch("app.partition_path", return_value=str(Path(folder) / "table")):
                state = WindowState()
                messages = [SimpleNamespace(value={**self.event, "event_time": self.at.isoformat()},
                                            partition=0, offset=i, timestamp=int(self.at.timestamp()*1000))
                            for i in range(11)]
                with patch("builtins.print"):
                    process_batch(messages[:10], {}, state, -1)
                    restored, offset = recover(0)
                    self.assertEqual(offset, 9)
                    process_batch(messages, {}, restored, offset)
                    again, offset = recover(0)
                    self.assertEqual(offset, 10)
                    self.assertEqual(len(again.cards["card"]), 11)
                    process_batch(messages, {}, again, offset)
                    final, _ = recover(0)
                    self.assertEqual(len(final.cards["card"]), 11)
                    frame = DeltaTable(str(Path(folder) / "table")).to_pandas()
                    last = frame.sort_values("source_offset").iloc[-1]
                    self.assertEqual(len(frame), 11)
                    self.assertEqual(last["velocity_alert"], 1)
                    self.assertEqual(last["tx_count"], 11)
