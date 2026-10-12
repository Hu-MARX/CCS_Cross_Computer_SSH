import threading
import unittest

from controller.fleet import WorkerBinding, WorkerDirectory
from controller.remote import FakeRemoteRunner


class SlowRemote(FakeRemoteRunner):
    def __init__(self, release):
        super().__init__()
        self.release = release
        self.calls = 0

    def health(self):
        self.calls += 1
        self.release.wait(3)
        return super().health()


class FleetBudgetTests(unittest.TestCase):
    def test_slow_worker_times_out_and_fast_worker_remains_available(self):
        release = threading.Event()
        slow = SlowRemote(release)
        directory = WorkerDirectory([WorkerBinding('slow', slow), WorkerBinding('fast', FakeRemoteRunner())])
        try:
            result = directory.status(timeout_seconds=.1, worker_timeout_seconds=.05)
            by_id = {item['worker_id']: item for item in result['workers']}
            self.assertEqual(by_id['slow']['error']['code'], 'WORKER_OBSERVATION_TIMEOUT')
            self.assertEqual(by_id['fast']['state'], 'available')
            again = directory.status(timeout_seconds=.1, worker_timeout_seconds=.05)
            self.assertEqual(again['workers'][0]['error']['code'], 'WORKER_OBSERVATION_BUSY')
            self.assertEqual(slow.calls, 1)
        finally:
            release.set()

    def test_repeated_timeout_never_grows_observation_capacity(self):
        release = threading.Event()
        workers = [SlowRemote(release) for _ in range(12)]
        directory = WorkerDirectory([WorkerBinding('worker' + str(i), remote) for i, remote in enumerate(workers)])
        try:
            first = directory.status(timeout_seconds=.1, worker_timeout_seconds=.05)
            self.assertEqual(sum(worker.calls for worker in workers), 4)
            second = directory.status(timeout_seconds=.1, worker_timeout_seconds=.05)
            self.assertEqual(sum(worker.calls for worker in workers), 4)
            self.assertEqual(len(first['workers']), 12)
            self.assertTrue(all(item['error']['code'] == 'WORKER_OBSERVATION_BUSY' for item in second['workers']))
        finally:
            release.set()

    def test_fast_large_pool_is_served_in_batches(self):
        directory = WorkerDirectory([WorkerBinding('worker' + str(i), FakeRemoteRunner()) for i in range(17)])
        result = directory.status()
        self.assertEqual(len(result['workers']), 17)
        self.assertTrue(all(item['state'] == 'available' for item in result['workers']))


if __name__ == '__main__':
    unittest.main()
