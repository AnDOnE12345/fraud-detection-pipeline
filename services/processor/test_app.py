from datetime import datetime, timezone
from unittest import TestCase

from app import fixed_window, fraud_signals, parse_event_time


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
