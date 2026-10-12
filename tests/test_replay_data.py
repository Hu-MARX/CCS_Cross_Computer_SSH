import importlib.util
import tempfile
import unittest
from pathlib import Path

AVAILABLE = importlib.util.find_spec('torch') is not None
if AVAILABLE:
    import random
    import numpy as np
    import torch
    from torch.utils.data import DataLoader, Dataset, RandomSampler
    from trainctl.replay_data import ReplayLoader

    class RandomData(Dataset):
        def __len__(self):
            return 12

        def __getitem__(self, index):
            return torch.tensor([index, random.random(), np.random.rand(), torch.rand(()).item()], dtype=torch.float64), index


@unittest.skipUnless(AVAILABLE, 'Configured training environment required')
class ReplayTests(unittest.TestCase):
    def loader(self, workers=0, replacement=False, identity='fixture'):
        dataset = RandomData()
        sampler = RandomSampler(dataset, replacement=replacement, num_samples=16 if replacement else None)
        return ReplayLoader(DataLoader(dataset, batch_size=2, sampler=sampler, num_workers=workers), seed=42, identity=identity)

    def collect(self, loader):
        rows = []
        for batch in loader:
            tokens = loader.acknowledge()
            rows.append((batch[0].clone(), tokens))
        return rows

    def test_only_acknowledged_not_prefetched_batches_advance_cursor(self):
        first = self.loader()
        iterator = iter(first)
        next(iterator)
        first.acknowledge()
        expected = next(iterator)
        next(iterator)  # fetched, but neither of these two batches was trained
        state = first.state_dict()
        self.assertEqual(state['committed'], 2)
        resumed = self.loader()
        resumed.load_state_dict(state)
        actual = next(iter(resumed))
        self.assertTrue(torch.equal(actual[0], expected[0]))
        iterator.close()

    def test_one_worker_prefetch_and_zero_workers_have_identical_inputs(self):
        zero, one = self.collect(self.loader()), self.collect(self.loader(workers=1))
        self.assertEqual(len(zero), 6)
        for a, b in zip(zero, one):
            self.assertTrue(torch.equal(a[0], b[0]))
            self.assertEqual(a[1], b[1])

    def test_replacement_sampling_is_replayable(self):
        reference = self.collect(self.loader(replacement=True))
        first = self.loader(workers=1, replacement=True)
        iterator = iter(first)
        next(iterator)
        first.acknowledge()
        next(iterator)
        first.acknowledge()
        state = first.state_dict()
        iterator.close()
        resumed = self.loader(workers=1, replacement=True)
        resumed.load_state_dict(state)
        for actual, expected in zip(self.collect(resumed), reference[2:]):
            self.assertTrue(torch.equal(actual[0], expected[0]))
            self.assertEqual(actual[1], expected[1])

    def test_epoch_transition_and_rng_isolation(self):
        random.seed(7)
        np.random.seed(7)
        torch.manual_seed(7)
        before = torch.get_rng_state().clone()
        python_before, numpy_before = random.getstate(), np.random.get_state()
        loader = self.loader()
        old = self.collect(loader)
        self.assertTrue(torch.equal(before, torch.get_rng_state()))
        self.assertEqual(python_before, random.getstate())
        self.assertTrue(np.array_equal(numpy_before[1], np.random.get_state()[1]))
        loader.sampler.set_epoch(1)
        fresh = self.collect(loader)
        self.assertEqual(fresh[0][1][0][0], 1)
        self.assertFalse(torch.equal(old[0][0], fresh[0][0]))

    def test_contract_mismatch_rejected(self):
        state = self.loader().state_dict()
        with self.assertRaisesRegex(ValueError, 'identity'):
            self.loader(identity='changed').load_state_dict(state)
        state['committed'] = 1
        with self.assertRaisesRegex(ValueError, 'complete batch'):
            self.loader().load_state_dict(state)

    def test_partial_epoch_final_is_not_a_safe_resume_point(self):
        from trainctl.replay_data import replay_checkpoint_state
        state = self.loader().state_dict()
        state['committed'] = 4
        partial = {'epoch': 1, 'trainctl_replay': {'loader': state}}
        self.assertFalse(replay_checkpoint_state(partial)['safe_to_resume'])
        state['committed'] = state['count']
        self.assertTrue(replay_checkpoint_state(partial)['safe_to_resume'])

    def test_failed_loading_does_not_commit_the_next_batch(self):
        loader = self.loader()
        iterator = iter(loader)
        next(iterator)
        with self.assertRaisesRegex(RuntimeError, 'injected'):
            raise RuntimeError('injected training failure before acknowledgment')
        self.assertEqual(loader.state_dict()['committed'], 0)
        iterator.close()

    def test_albumentations_private_rng_is_reset_per_occurrence(self):
        import albumentations as alb
        from types import SimpleNamespace
        from trainctl.replay_data import ReplayDataset
        class PrivateRandomData(Dataset):
            def __init__(self, seed):
                self._transforms = SimpleNamespace(master_transform=SimpleNamespace(transform=
                    alb.Compose([alb.RandomBrightnessContrast(p=1)], seed=seed)))
            def __len__(self):
                return 1
            def __getitem__(self, index):
                image = np.arange(192, dtype=np.uint8).reshape(8, 8, 3)
                return torch.from_numpy(self._transforms.master_transform.transform(image=image)['image'])
        key = (0, 0, 0)
        one = ReplayDataset(PrivateRandomData(1), 42)[key][0]
        two = ReplayDataset(PrivateRandomData(2), 42)[key][0]
        self.assertTrue(torch.equal(one, two), 'Private transform RNG was not seeded by the occurrence')

    def test_single_and_multi_view_nested_tensors_are_traceable(self):
        from types import SimpleNamespace
        from trainctl.lightning_replay import traceable_samples
        nested = SimpleNamespace(tensors=torch.ones(2), mask=torch.zeros(2, dtype=torch.bool))

        single = traceable_samples(nested)
        multi = traceable_samples({'master': nested})

        self.assertTrue(torch.equal(single['tensors'], nested.tensors))
        self.assertTrue(torch.equal(single['mask'], nested.mask))
        self.assertTrue(torch.equal(multi['master']['tensors'], nested.tensors))
