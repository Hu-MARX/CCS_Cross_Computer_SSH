"""Pause observation/export must not turn an ACK into process quiescence."""
import json
import unittest
from dashboard.feed import observation
from controller.results import task_result


class PauseObservationTests(unittest.TestCase):
    def test_pause_is_bounded_and_survives_result_export(self):
        at = '2026-01-01T00:00:00Z'
        latest = observation({'lifecycle':'CANCELLED', 'pause':{
            'state':'checkpoint_committed', 'quiescent':False, 'safe_to_resume':False,
            'request_id':'pause-one', 'request':{'requested_at':at, 'expires_at':at,
                                               'reason':'do not expose this request payload'},
            'checkpoint':{'id':'ckpt_'+'a'*32, 'global_step':7, 'path':'/private/model.ckpt'},
            'verification':{'bytes_verified_at':at, 'current_bytes_rehashed':False}}}, at)
        result = task_result({'run_id':'run_example', 'latest':latest, 'connection':'stale'})
        self.assertEqual(result['pause']['state'], 'checkpoint_committed')
        self.assertFalse(result['pause']['safe_to_resume'])
        self.assertEqual(result['pause']['checkpoint_step'], 7)
        self.assertNotIn('/private', json.dumps(result['pause']))
        self.assertNotIn('request payload', json.dumps(result['pause']))

    def test_claimed_pause_without_quiescence_is_invalid_not_ready(self):
        latest = observation({'pause':{'state':'paused','quiescent':False,'safe_to_resume':True}}, 'now')
        self.assertEqual(latest['pause']['state'], 'invalid')
        self.assertFalse(latest['pause']['safe_to_resume'])


if __name__ == '__main__':
    unittest.main()
