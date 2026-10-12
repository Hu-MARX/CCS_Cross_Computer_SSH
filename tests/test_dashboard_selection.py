import unittest
from dashboard import server


class DashboardSelectionTests(unittest.TestCase):
    def setUp(self):
        self.old = dict(run_id='run_'+'a'*32, worker='lab118', kind='train',
                        recorded_status='RUNNING', created_at='2026-09-05T00:00:00Z')
        self.current = dict(run_id='run_'+'b'*32, worker='ubuntu-second', kind='train',
                            recorded_status='QUEUED', created_at='2026-09-09T00:00:00Z')

    def test_default_uses_newest_pending_training_and_its_worker(self):
        self.assertEqual(server.select_primary([self.old, self.current]), self.current)

    def test_explicit_selection_preserves_registered_worker(self):
        self.assertEqual(server.select_primary([self.old, self.current], self.current['run_id'])['worker'], 'ubuntu-second')

    def test_unknown_selection_fails_closed(self):
        with self.assertRaises(ValueError):
            server.select_primary([self.old], self.current['run_id'])

    def test_no_catalog_does_not_invent_a_legacy_run(self):
        with self.assertRaises(ValueError):
            server.select_primary([])

    def test_completed_only_falls_back_to_newest_record(self):
        self.current['recorded_status'] = 'SUCCEEDED'
        self.old['recorded_status'] = 'SUCCEEDED'
        self.assertEqual(server.select_primary([self.old, self.current]), self.current)
