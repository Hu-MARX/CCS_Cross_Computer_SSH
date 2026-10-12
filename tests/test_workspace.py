import json
import tempfile
import unittest
from pathlib import Path

from trainctl.run_store import RunStore, TrainCtlError
from trainctl.workspace import WorkspaceManager


class WorkspaceManagerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.allowed = self.base / 'PycharmProject'
        self.project = self.allowed / 'embodied'
        self.project.mkdir(parents=True)
        (self.project / 'policy.py').write_bytes(b'VALUE = 1\n')
        self.manager = WorkspaceManager(self.base / 'state', {'workspace_roots': [str(self.allowed)]})

    def tearDown(self):
        self.temp.cleanup()

    def execute(self, **request):
        return self.manager.execute(request)

    def test_bounded_inspection_read_and_search(self):
        listing = self.execute(action='inspect', mode='list', project=str(self.project))
        self.assertEqual(listing['entries'][0]['path'], 'policy.py')
        read = self.execute(action='inspect', mode='read', project=str(self.project), path='policy.py')
        self.assertEqual(read['content'], 'VALUE = 1\n')
        found = self.execute(action='inspect', mode='search', project=str(self.project), query='VALUE')
        self.assertEqual(found['matches'], [{'path': 'policy.py', 'match': 'content'}])

    def test_recursive_tree_is_bounded_and_hides_control_contents(self):
        source = self.project / 'models' / 'world'
        source.mkdir(parents=True)
        (source / 'model.py').write_text('class World: pass\n', encoding='utf-8')
        (self.project / '.env').write_text('TOKEN=secret\n', encoding='utf-8')
        git = self.project / '.git'
        git.mkdir()
        (git / 'config').write_text('secret\n', encoding='utf-8')

        tree = self.execute(action='inspect', mode='tree', project=str(self.project), depth=3, limit=20)
        by_path = {row['path']: row for row in tree['entries']}
        self.assertEqual(by_path['models/world/model.py']['kind'], 'file')
        self.assertTrue(by_path['.env']['restricted'])
        self.assertTrue(by_path['.git']['restricted'])
        self.assertNotIn('.git/config', by_path)
        self.assertFalse(tree['limit_reached'])

        shallow = self.execute(action='inspect', mode='tree', project=str(self.project), depth=1, limit=20)
        self.assertTrue(shallow['depth_limited'])
        self.assertNotIn('models/world', {row['path'] for row in shallow['entries']})

        limited = self.execute(action='inspect', mode='tree', project=str(self.project), depth=3, limit=1)
        self.assertTrue(limited['limit_reached'])
        self.assertEqual(len(limited['entries']), 1)

    def test_recursive_tree_rejects_unbounded_requests_and_control_target(self):
        for depth in (0, 9, True):
            with self.assertRaisesRegex(TrainCtlError, 'depth must be 1..8'):
                self.execute(action='inspect', mode='tree', project=str(self.project), depth=depth)
        for limit in (0, 5001, True):
            with self.assertRaisesRegex(TrainCtlError, 'limit must be 1..5000'):
                self.execute(action='inspect', mode='tree', project=str(self.project), limit=limit)
        git = self.project / '.git'
        git.mkdir()
        with self.assertRaisesRegex(TrainCtlError, 'cannot be inventoried'):
            self.execute(action='inspect', mode='tree', project=str(self.project), path='.git')

    def test_outside_root_traversal_and_symlink_rejected(self):
        outside = self.base / 'outside'; outside.mkdir()
        with self.assertRaisesRegex(TrainCtlError, 'outside configured'):
            self.execute(action='inspect', project=str(outside))
        with self.assertRaisesRegex(TrainCtlError, 'traversal'):
            self.execute(action='inspect', mode='read', project=str(self.project), path='../outside')
        link = self.project / 'link.py'
        try:
            link.symlink_to(self.project / 'policy.py')
        except OSError:
            self.skipTest('symlinks unavailable')
        with self.assertRaisesRegex(TrainCtlError, 'symlinks'):
            self.execute(action='inspect', mode='read', project=str(self.project), path='link.py')

    def test_stage_diff_apply_and_restore_use_exact_digests(self):
        staged = self.execute(action='stage', project=str(self.project), operations=[
            {'op': 'write', 'path': 'policy.py', 'content': 'VALUE = 2\n'},
            {'op': 'write', 'path': 'world/model.py', 'content': 'class World: pass\n'}])
        self.assertEqual((self.project / 'policy.py').read_text(), 'VALUE = 1\n')
        diff = self.execute(action='diff', change_id=staged['change_id'])
        self.assertIn('-VALUE = 1', diff['diff'])
        self.assertIn('+VALUE = 2', diff['diff'])
        with self.assertRaisesRegex(TrainCtlError, 'exact plan_sha256'):
            self.execute(action='apply', change_id=staged['change_id'], plan_sha256='wrong')
        applied = self.execute(action='apply', change_id=staged['change_id'], plan_sha256=staged['plan_sha256'])
        self.assertEqual((self.project / 'policy.py').read_text(), 'VALUE = 2\n')
        self.assertTrue((self.project / 'world/model.py').exists())
        with self.assertRaisesRegex(TrainCtlError, 'exact applied_sha256'):
            self.execute(action='restore', change_id=staged['change_id'], applied_sha256='wrong')
        restored = self.execute(action='restore', change_id=staged['change_id'], applied_sha256=applied['applied_sha256'])
        self.assertEqual(restored['status'], 'restored')
        self.assertEqual((self.project / 'policy.py').read_text(), 'VALUE = 1\n')
        self.assertFalse((self.project / 'world/model.py').exists())

    def test_hash_change_blocks_apply(self):
        staged = self.execute(action='stage', project=str(self.project), operations=[
            {'op': 'write', 'path': 'policy.py', 'content': 'VALUE = 2\n'}])
        (self.project / 'policy.py').write_text('USER EDIT\n', encoding='utf-8')
        with self.assertRaisesRegex(TrainCtlError, 'changed after staging'):
            self.execute(action='apply', change_id=staged['change_id'], plan_sha256=staged['plan_sha256'])
        self.assertEqual((self.project / 'policy.py').read_text(), 'USER EDIT\n')

    def test_staged_overlay_can_be_discarded_but_record_is_retained(self):
        staged = self.execute(action='stage', project=str(self.project), operations=[
            {'op': 'write', 'path': 'candidate.py', 'content': 'candidate = True\n'}])
        with self.assertRaisesRegex(TrainCtlError, 'exact plan_sha256'):
            self.execute(action='discard', change_id=staged['change_id'], plan_sha256='wrong')
        discarded = self.execute(action='discard', change_id=staged['change_id'],
                                 plan_sha256=staged['plan_sha256'])
        self.assertEqual(discarded['status'], 'discarded')
        self.assertFalse((self.project / 'candidate.py').exists())
        changes = self.execute(action='changes')
        self.assertEqual(changes['changes'][0]['change_id'], staged['change_id'])
        self.assertEqual(changes['changes'][0]['status'], 'discarded')

    def test_trash_is_recoverable_and_protected_files_are_denied(self):
        obsolete = self.project / 'obsolete.py'; obsolete.write_bytes(b'old\n')
        staged = self.execute(action='stage', project=str(self.project), operations=[
            {'op': 'trash', 'path': 'obsolete.py'}])
        applied = self.execute(action='apply', change_id=staged['change_id'], plan_sha256=staged['plan_sha256'])
        self.assertFalse(obsolete.exists())
        self.execute(action='restore', change_id=staged['change_id'], applied_sha256=applied['applied_sha256'])
        self.assertEqual(obsolete.read_text(), 'old\n')
        weights = self.project / 'weights'; weights.mkdir(); (weights / 'best.pt').write_bytes(b'model')
        with self.assertRaisesRegex(TrainCtlError, 'cannot be changed'):
            self.execute(action='stage', project=str(self.project), operations=[
                {'op': 'trash', 'path': 'weights/best.pt'}])
        with self.assertRaisesRegex(TrainCtlError, 'cannot be changed'):
            self.execute(action='stage', project=str(self.project), operations=[
                {'op': 'write', 'path': 'weights/best.pt', 'content': 'not a model'}])

    def test_credentials_and_version_control_contents_are_not_exposed(self):
        (self.project / '.env').write_text('TOKEN=secret')
        git = self.project / '.git'; git.mkdir(); (git / 'config').write_text('secret')
        with self.assertRaisesRegex(TrainCtlError, 'cannot be read'):
            self.execute(action='inspect', mode='read', project=str(self.project), path='.env')
        found = self.execute(action='inspect', mode='search', project=str(self.project), query='secret')
        self.assertEqual(found['matches'], [])

    def test_large_existing_file_cannot_be_overwritten_or_trashed(self):
        large = self.project / 'large.py'; large.write_bytes(b'x' * (1024 * 1024 + 1))
        for operation in ({'op': 'write', 'path': 'large.py', 'content': 'small'},
                          {'op': 'trash', 'path': 'large.py'}):
            with self.assertRaisesRegex(TrainCtlError, 'over 1 MiB'):
                self.execute(action='stage', project=str(self.project), operations=[operation])

    def test_run_store_requires_explicit_workspace_roots(self):
        store = RunStore(self.base / 'empty-worker')
        with self.assertRaisesRegex(TrainCtlError, 'outside configured'):
            store.workspace({'action': 'inspect', 'project': str(self.project)})


if __name__ == '__main__':
    unittest.main()
