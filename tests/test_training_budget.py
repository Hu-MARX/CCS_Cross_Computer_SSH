import unittest

from trainctl.training_budget import TrainingTimeBudget


class TrainingBudgetTests(unittest.TestCase):
    def test_stop_only_after_optimizer_step_not_accumulation_batch(self):
        now = [0]
        budget = TrainingTimeBudget(10, clock=lambda: now[0])
        budget.start(40)
        now[0] = 9
        self.assertFalse(budget.after_batch(41))
        now[0] = 11
        self.assertFalse(budget.after_batch(41))
        self.assertTrue(budget.after_batch(42))
        self.assertEqual(budget.elapsed, 11)

    def test_disabled_and_invalid_budgets(self):
        budget = TrainingTimeBudget()
        budget.start(0)
        self.assertFalse(budget.after_batch(1))
        for value in (0, -1, True, float('inf'), float('nan'), '48h'):
            with self.assertRaises(ValueError):
                TrainingTimeBudget(value)
