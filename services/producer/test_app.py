from collections import Counter
from unittest import TestCase
from unittest.mock import patch

from fastapi import BackgroundTasks

from app import (
    HIGH_AMOUNT,
    MERCHANTS,
    RISK_THRESHOLD,
    SimulateRequest,
    _simulation_scenarios,
    _synthetic_transaction,
    simulate,
)


class SyntheticStreamTest(TestCase):
    def test_ratio_is_exact_and_reasons_are_balanced(self) -> None:
        scenarios = _simulation_scenarios(1000, 0.15)
        counts = Counter(scenarios)

        self.assertEqual(len(scenarios), 1000)
        self.assertEqual(counts["normal"], 850)
        self.assertEqual(
            counts["amount"] + counts["merchant"] + counts["both"], 150
        )
        self.assertLessEqual(
            max(counts["amount"], counts["merchant"], counts["both"])
            - min(counts["amount"], counts["merchant"], counts["both"]),
            1,
        )

    def test_each_scenario_matches_its_rule_flags(self) -> None:
        merchant_risks = {merchant_id: risk for merchant_id, _, risk in MERCHANTS}
        expected = {
            "normal": (False, False),
            "amount": (True, False),
            "merchant": (False, True),
            "both": (True, True),
        }

        for scenario, expected_flags in expected.items():
            with self.subTest(scenario=scenario):
                event = _synthetic_transaction(scenario)
                actual_flags = (
                    event["amount"] > HIGH_AMOUNT,
                    merchant_risks[event["merchant_id"]] >= RISK_THRESHOLD,
                )
                self.assertEqual(actual_flags, expected_flags)

    def test_half_of_one_event_rounds_to_one_flagged_event(self) -> None:
        scenarios = _simulation_scenarios(1, 0.5)

        self.assertNotEqual(scenarios, ["normal"])

    @patch("app.get_producer")
    def test_large_simulation_returns_immediate_plan(self, _get_producer) -> None:
        tasks = BackgroundTasks()

        response = simulate(
            SimulateRequest(count=1000, fraud_ratio=0.15, burst_card=True), tasks
        )

        self.assertTrue(response["accepted"])
        self.assertEqual(response["scheduled"], 1015)
        self.assertEqual(response["planned"]["normal"], 850)
        self.assertEqual(response["planned"]["velocity_burst"], 15)
        self.assertEqual(len(tasks.tasks), 1)
