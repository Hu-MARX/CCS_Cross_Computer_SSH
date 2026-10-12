import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from controller.configuration import read_configuration, spec_digest
from controller.results import descriptor
from dashboard.feed import atomic_json
from examples.mock_workflow import make_spec
from scripts.build_config_report import build

RUN = 'run_' + 'c' * 32


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.directory = self.root / RUN
        self.directory.mkdir()
        self.spec = make_spec('train').to_dict()
        self.task = descriptor(RUN, self.spec)
        self.source = self.directory / 'source.py'
        self.source.write_text('raise RuntimeError("must never execute")', encoding='utf-8')
        self.report = {'schema_version': 1, 'run_id': RUN, 'spec_sha256': spec_digest(self.spec),
            'evidence': None, 'parameters': [{'key': 'lr', 'basis': 'frozen_code', 'value': .0001,
                                            'source_files': ['source.py']}],
            'sources': [{'file': 'source.py', 'sha256': hashlib.sha256(self.source.read_bytes()).hexdigest()}]}

    def tearDown(self):
        self.temp.cleanup()

    def save(self):
        atomic_json(self.directory/'configuration.json', self.report)

    def test_missing_is_explicit_and_no_files_created(self):
        self.assertEqual(read_configuration(self.root, self.task)['state'], 'not_recorded')
        self.assertFalse((self.directory/'configuration.json').exists())

    def test_verified_sources_exposed_without_executing_source(self):
        self.save()
        result = read_configuration(self.root, self.task)
        self.assertEqual(result['state'], 'available')
        self.assertTrue(result['local_sources_verified'])
        self.assertEqual(result['parameters'][0]['value'], .0001)

    def test_wrong_run_spec_or_evidence_hide_all_parameters(self):
        for key, value in [('run_id', 'another'), ('spec_sha256', '0'*64), ('evidence', {'unexpected': True})]:
            changed = copy.deepcopy(self.report)
            changed[key] = value
            atomic_json(self.directory/'configuration.json', changed)
            result = read_configuration(self.root, self.task)
            self.assertEqual(result['state'], 'invalid')
            self.assertEqual(result['parameters'], [])

    def test_changed_source_rejects_stale_report(self):
        self.save()
        self.source.write_text('changed', encoding='utf-8')
        self.assertEqual(read_configuration(self.root, self.task)['state'], 'invalid')

    def test_path_traversal_and_unreferenced_source_rejected(self):
        self.report['sources'][0]['file'] = '../source.py'
        self.save()
        self.assertEqual(read_configuration(self.root, self.task)['state'], 'invalid')
        self.report['sources'][0]['file'] = 'source.py'
        self.report['parameters'][0]['source_files'] = ['unknown.py']
        self.save()
        self.assertEqual(read_configuration(self.root, self.task)['state'], 'invalid')

    def test_normalized_spec_hash_stable_and_changed_flags_differ(self):
        raw = {'schema_version': 1, 'name': 'train', 'runtime': {}, 'command': {'argv': ['python', 'train.py']}}
        from controller.domain import ExperimentSpec
        self.assertEqual(spec_digest(raw), spec_digest(ExperimentSpec.from_dict(raw).to_dict()))
        edited = copy.deepcopy(raw)
        edited['command']['argv'].extend(['--batch-size', '2'])
        self.assertNotEqual(spec_digest(raw), spec_digest(edited))

    def test_builder_uses_submitted_overrides_and_runtime_recipe(self):
        defaults = {'seed': 42, 'lr': 1e-4, 'lr_encoder': 1.5e-4, 'weight_decay': 1e-4,
                    'resolution': 560, 'gcr_topk': 8, 'gcr_projection_dim': 128,
                    'gcr_max_residual_scale': .2, 'gcr_detach_auxiliary': True}
        source = 'raise RuntimeError("never execute")\n' + '\n'.join(
            f'parser.add_argument("--{key}", default={value!r})' for key, value in defaults.items())
        hashes = {}
        for name in ('train_gcr.py', 'cvfdetr_replay.py', 'param_groups.py', 'config.py'):
            (self.directory/name).write_text(source, encoding='utf-8')
            hashes[name] = hashlib.sha256((self.directory/name).read_bytes()).hexdigest()
        raw = make_spec('train').to_dict()
        raw['command']['argv'] = ['python', '/srv/ml-orchestrator/app/examples/cvfdetr_snapshot.py',
                                 '--batch-size', '1', '--grad-accum', '2', '--epochs', '50']
        base = '/srv/ml-orchestrator/runs/'+RUN+'/source-snapshot/'
        contract = {'sources': {'train_gcr.py': hashes['train_gcr.py']}, 'adapter_sha256': hashes['cvfdetr_replay.py'],
                    'recipe': {'batch_size': 1, 'grad_accum': 2, 'num_workers': 1, 'limit_train_batches': None},
                    'dataset': '/not-read', 'python': '3.11', 'torch': 'mock', 'lightning': 'mock'}
        for name, value in {'spec.json': raw, 'experiment-contract.json': contract,
                            'restore-audit.json': {'from_run': None, 'state': {'global_step': 0}},
                            'snapshot-imports.json': {'research_source': base+'research-code',
                               'adapter': base+'orchestrator-code/examples/cvfdetr_replay.py'}}.items():
            atomic_json(self.directory/name, value)
        with patch('scripts.build_config_report.AUDITED', hashes):
            result = build(self.directory, RUN)
            params = {x['key']: x for x in result['parameters']}
            self.assertEqual(params['epochs']['value'], 50)
            self.assertEqual(params['batch_size']['value'], 1)
            self.assertEqual(params['effective_batch']['value'], 2)
            self.assertIsNone(params['current_lr']['value'])
            self.assertEqual(params['lr']['basis'], 'frozen_code')
            contract['recipe']['batch_size'] = 4
            atomic_json(self.directory/'experiment-contract.json', contract)
            with self.assertRaisesRegex(ValueError, 'recipe mismatch'):
                build(self.directory, RUN)
            (self.directory/'train_gcr.py').write_text('unexpected version')
            with self.assertRaisesRegex(ValueError, 'new review'):
                build(self.directory, RUN)
