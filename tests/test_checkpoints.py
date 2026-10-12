import json
import tempfile
import unittest
from pathlib import Path

from trainctl.checkpoints import CheckpointStore


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = CheckpointStore(self.root / 'checkpoints', keep=2, min_free_bytes=0)

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def validate(path):
        value = json.loads(path.read_text())
        if 'optimizer' not in value:
            raise ValueError('Missing optimizer state')
        return value

    def save(self, step, pinned=False):
        return self.store.commit(lambda p: p.write_text(json.dumps({'step': step, 'optimizer': [step]})),
            self.validate, metadata={'contract': 'same', 'pinned': pinned})

    def test_corrupt_latest_falls_back_and_copy_survives_retention(self):
        first = self.save(1)
        latest = self.save(2)
        (self.store.directory / latest['file']).write_bytes(b'broken')
        result = self.store.restore_copy(self.root / 'resume.ckpt', self.validate, contract='same')
        self.assertEqual(result['state']['step'], 1)
        self.assertEqual(result['rejected'][0]['id'], latest['id'])
        self.save(3)
        self.save(4)
        self.assertFalse((self.store.directory / first['file']).exists())
        self.assertEqual(self.validate(self.root / 'resume.ckpt')['step'], 1)

    def test_failed_save_does_not_publish_or_prune_previous_points(self):
        first, second = self.save(1), self.save(2)
        with self.assertRaises(ValueError):
            self.store.commit(lambda p: p.write_text('{}'), self.validate, metadata={})
        self.assertEqual({r['id'] for r in self.store.records()}, {first['id'], second['id']})
        self.assertFalse(list(self.store.directory.glob('*.partial')))

    def test_keeps_two_valid_and_pinned_milestone_and_unmanaged_files(self):
        milestone = self.save(1, pinned=True)
        other = self.store.directory / 'user-weight.pth'
        other.write_bytes(b'preserve')
        self.save(2)
        self.save(3)
        self.save(4)
        self.assertEqual({r['state']['step'] for r in self.store.records()}, {1, 3, 4})
        self.assertTrue((self.store.directory / milestone['file']).exists())
        self.assertEqual(other.read_bytes(), b'preserve')

    def test_no_valid_checkpoint_never_starts_fresh_or_overwrites_destination(self):
        self.save(1)
        with self.assertRaisesRegex(ValueError, 'NO_VALID_CHECKPOINT'):
            self.store.restore_copy(self.root / 'resume.ckpt', self.validate, contract='changed')
        self.assertFalse((self.root / 'resume.ckpt').exists())
        (self.root / 'resume.ckpt').write_bytes(b'keep')
        with self.assertRaises(ValueError):
            self.store.restore_copy(self.root / 'resume.ckpt', self.validate, contract='same')
        self.assertEqual((self.root / 'resume.ckpt').read_bytes(), b'keep')

    def test_uncommitted_binary_and_bad_manifest_are_ignored(self):
        (self.store.directory / ('ckpt_' + 'f'*32 + '.ckpt')).write_bytes(b'orphan')
        (self.store.directory / ('ckpt_' + 'e'*32 + '.json')).write_text('{')
        self.assertEqual(self.store.records(), [])

    def test_restore_revalidates_semantics_even_with_matching_hash(self):
        self.save(1)
        def invalid(path):
            raise ValueError('Unsupported state')
        with self.assertRaisesRegex(ValueError, 'NO_VALID_CHECKPOINT'):
            self.store.restore_copy(self.root / 'resume.ckpt', invalid, contract='same')


if __name__ == '__main__':
    unittest.main()
