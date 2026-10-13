"""Trusted adapters prove loaded state and real advancement through one Interface."""
import copy
import json
from pathlib import Path
import tempfile
import unittest


class RecoveryAuditTests(unittest.TestCase):
    @staticmethod
    def selection(root):
        checkpoint = root / 'selected.ckpt'
        checkpoint.write_bytes(b'trusted-adapter-input-fixture')
        state = dict(contract='f'*64, global_step=5, optimizer_steps=5,
                     recovery_fingerprints={key:'a'*64 for key in
                         ('model', 'optimizer', 'scheduler', 'precision', 'rng', 'sampler', 'loop')},
                     replay={'safe_to_resume': True})
        return dict(path=str(checkpoint), state=state,
                    selected=dict(id='ckpt_'+'1'*32, sha256='b'*64, metadata={'contract':'f'*64}))

    def test_loaded_and_advanced_are_distinct_and_require_state_change(self):
        from trainctl.recovery_audit import RecoveryAudit
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            checkpoint = root / 'selected.ckpt'
            checkpoint.write_bytes(b'local-test-checkpoint')
            state = dict(contract='f'*64, global_step=5, optimizer_steps=5,
                         recovery_fingerprints={key:'a'*64 for key in
                             ('model', 'optimizer', 'scheduler', 'precision', 'rng', 'sampler', 'loop')},
                         replay={'safe_to_resume': True})
            selection = dict(path=str(checkpoint), state=state,
                             selected=dict(id='ckpt_'+'1'*32, sha256='b'*64,
                                           metadata={'contract':'f'*64}))
            audit = RecoveryAudit(selection, root, 'f'*64)
            loaded = audit.confirm_loaded(copy.deepcopy(state))
            self.assertEqual(loaded['state'], 'loaded')
            self.assertFalse(loaded['advancement_verified'])
            with self.assertRaises(ValueError):
                audit.confirm_advanced(copy.deepcopy(state))
            advanced = copy.deepcopy(state)
            advanced.update(global_step=6, optimizer_steps=6)
            advanced['recovery_fingerprints'].update(model='c'*64, optimizer='d'*64)
            result = audit.confirm_advanced(advanced)
            self.assertEqual(result['state'], 'advanced')
            self.assertTrue(result['advancement_verified'])
            self.assertEqual(result['optimizer_updates_since_restore'], 1)
            self.assertEqual(json.loads((root/'restore-audit.json').read_text())['state'], 'advanced')

    def test_loaded_mismatch_or_missing_component_never_records_success(self):
        from trainctl.recovery_audit import RecoveryAudit
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            selection = self.selection(root)
            audit = RecoveryAudit(selection, root, 'f'*64)
            for key in ('model', 'optimizer', 'precision', 'rng', 'sampler', 'loop', 'scheduler'):
                with self.subTest(component=key):
                    wrong = copy.deepcopy(selection['state'])
                    wrong['recovery_fingerprints'][key] = 'c'*64
                    with self.assertRaisesRegex(ValueError, 'LOADED_STATE_MISMATCH'):
                        audit.confirm_loaded(wrong)
            missing = copy.deepcopy(selection['state'])
            del missing['recovery_fingerprints']['rng']
            with self.assertRaisesRegex(ValueError, 'COMPONENT_SUMMARY_REQUIRED'):
                audit.confirm_loaded(missing)
            self.assertFalse((root/'restore-audit.json').exists())

    def test_counters_alone_or_model_change_alone_do_not_prove_training(self):
        from trainctl.recovery_audit import RecoveryAudit
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            selection = self.selection(root)
            audit = RecoveryAudit(selection, root, 'f'*64)
            changed = copy.deepcopy(selection['state'])
            changed.update(global_step=6, optimizer_steps=6)
            changed['recovery_fingerprints'].update(model='c'*64, optimizer='d'*64)
            with self.assertRaisesRegex(ValueError, 'LOADED_CONFIRMATION_REQUIRED'):
                audit.confirm_advanced(changed)
            audit.confirm_loaded(selection['state'])
            for component in ('model', 'optimizer'):
                partial = copy.deepcopy(changed)
                partial['recovery_fingerprints'][component] = 'a'*64
                with self.subTest(unchanged=component), self.assertRaisesRegex(ValueError, 'NO_REAL_ADVANCEMENT'):
                    audit.confirm_advanced(partial)
            audit.confirm_advanced(changed)
            with self.assertRaisesRegex(ValueError, 'CANNOT_DOWNGRADE'):
                audit.confirm_loaded(selection['state'])

    def test_conflicting_source_and_unsafe_counter_cannot_reuse_an_audit(self):
        from trainctl.recovery_audit import RecoveryAudit
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            selection = self.selection(root)
            RecoveryAudit(selection, root, 'f'*64).confirm_loaded(selection['state'])
            changed = copy.deepcopy(selection)
            changed['selected']['sha256'] = 'c'*64
            with self.assertRaisesRegex(ValueError, 'EXISTING_CONFLICT'):
                RecoveryAudit(changed, root, 'f'*64).confirm_loaded(changed['state'])
            bad = copy.deepcopy(selection['state'])
            bad['optimizer_steps'] = True
            with self.assertRaisesRegex(ValueError, 'COUNTER_INVALID'):
                RecoveryAudit(selection, root, 'f'*64).confirm_loaded(bad)


if __name__ == '__main__':
    unittest.main()
