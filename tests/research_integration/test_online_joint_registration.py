"""Explicit research-context integration; excluded from core unittest discovery.

CCS_RESEARCH_ROOT must name the research directory containing scripts/ and
config/. Only the registration script and its declarative contract are read;
no model implementation is imported or executed.
"""
import base64
import copy
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

# The external registration script prepends its sibling CCS checkout. Preload
# its dependencies from the CCS environment selected by the verifier instead.
import controller.configuration
import controller.repository
import dashboard.feed
import dashboard.fullfit_metrics


class OnlineJointRegistrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configured = os.environ.get('CCS_RESEARCH_ROOT')
        if not configured:
            raise FileNotFoundError(
                'Research integration requires CCS_RESEARCH_ROOT pointing to the research directory')
        root = Path(configured).resolve()
        path = root / 'scripts/register_online_joint_dashboard.py'
        contract_path = root / 'config/sparsecoop-online-joint-a-v1.json'
        for required in (path, contract_path):
            if not required.is_file():
                raise FileNotFoundError('Required research integration fixture is missing: ' + str(required))
        spec = importlib.util.spec_from_file_location('online_joint_dashboard_registration', path)
        cls.module = importlib.util.module_from_spec(spec)
        prior_path = sys.path[:]
        try:
            spec.loader.exec_module(cls.module)
        finally:
            sys.path[:] = prior_path
        cls.contract = json.loads(contract_path.read_text(encoding='utf-8'))

    def fixture(self, root, resume=False):
        raw = root / ('resume' if resume else 'initial')
        raw.mkdir()
        c = copy.deepcopy(self.contract)
        c.update(epochs=3, execution_key='joint-resume' if resume else 'joint-initial',
                 output='training/joint-resume' if resume else 'training/joint-initial',
                 stop_after_updates=None if resume else 2,
                 resume=dict(checkpoint='prior.pth', sha256='a'*64) if resume else None)
        bundle = dict(contract=c, driver="raise RuntimeError('Never execute bundled source')",
                      modules={'module': 'also_not_executed = True'}, manifest={'code.py': 'b'*64},
                      input_hashes={'input.bin': 'c'*64}, partition={'fixture': 'fit only'})
        encoded = base64.b64encode(gzip.compress(json.dumps(bundle).encode())).decode()
        submission = dict(worker_id='ubuntu-second', spec=dict(schema_version=1, name='joint-test',
            runtime={}, command=dict(argv=['python', '-c', "base64.b64decode('" + encoded + "')"])))
        run = 'run_' + ('b' if resume else 'a') * 32
        task = dict(run_id=run, worker='ubuntu-second', spec_sha256=self.module.spec_digest(submission['spec']),
                    evidence=None)
        plans = ['d'*64] * 3
        step = 3 if resume else 2
        report = dict(contract=c, code_manifest=bundle['manifest'], plan_sha256=plans,
            training_signature=self.module.training_signature(bundle, plans), total_batches=11,
            batches_per_epoch=[3, 4, 4], global_batches=step, optimizer_updates=step,
            completed_epochs=1 if resume else 0, trainable_parameters=dict(vehicle=123, drone=456),
            last_progress=dict(event='training_progress', step=step, epoch=1, epoch_batch=step,
                epoch_batches=3, total_steps=11, loss=2.5, lr=.0001, optimizer_updates=step,
                loss_parts=dict(vehicle=1.5, drone=1., protection=0.)))
        if resume:
            report['resumed_from'] = dict(restored_updates=2)
        for name, body in [('submit-payload.json', submission),
                           ('ccs-submit.json', dict(ok=True, data=dict(result=dict(id=run)))),
                           ('report.json' if not resume else 'progress-abc.json', report)]:
            (raw / name).write_text(json.dumps(body), encoding='utf-8')
        name = 'report' if not resume else 'progress-abc'
        witness = dict(worker_id='ubuntu-second', project=c['project'],
            path=c['output'] + ('/report.json' if not resume else '/progress.json'),
            sha256=hashlib.sha256((raw / (name + '.json')).read_bytes()).hexdigest())
        (raw / (name + '-witness.json')).write_text(json.dumps(witness), encoding='utf-8')
        return raw, task, report

    def test_initial_and_resume_bind_complete_horizon_without_database_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results = []
            for resume in [False, True]:
                raw, task, report = self.fixture(root, resume)
                with patch.object(self.module.RunRepository, 'catalog', return_value=[task]):
                    result = self.module.register(raw, workspace_root=root)
                results.append(result)
                config = self.module.read_configuration(root / 'CCS/data/config-evidence', task)
                values = {item['key']: item['value'] for item in config['parameters']}
                self.assertEqual(config['state'], 'available')
                self.assertEqual(config['log_bridge']['max_steps'], 11)
                self.assertEqual(config['log_bridge']['epoch_batch_counts'], [3, 4, 4])
                self.assertEqual(values['effective_batch_size'], 4)
                self.assertEqual(values['micro_batch_size'], 1)
                self.assertEqual(values['vehicle_lr'], .0001)
                self.assertEqual(values['drone_lr'], .0002)
                self.assertEqual(values['protection_weight'], 0.)
                self.assertEqual(values['resume_step'], 2 if resume else 0)
                self.assertFalse((root / 'CCS/data/orchestrator.db').exists())
            self.assertNotEqual(results[0]['run_id'], results[1]['run_id'])
            self.assertEqual(results[0]['signature'], results[1]['signature'])

    def test_tampered_digest_or_foreign_worker_cannot_register(self):
        for mutation in ['digest', 'worker', 'spec', 'signature']:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                raw, task, report = self.fixture(root)
                witness_path = raw / 'report-witness.json'
                witness = json.loads(witness_path.read_text())
                if mutation == 'digest':
                    witness['sha256'] = 'f'*64
                elif mutation == 'worker':
                    witness['worker_id'] = 'lab118'
                elif mutation == 'spec':
                    task['spec_sha256'] = 'f'*64
                else:
                    report['training_signature'] = 'f'*64
                    (raw / 'report.json').write_text(json.dumps(report), encoding='utf-8')
                    witness['sha256'] = hashlib.sha256((raw / 'report.json').read_bytes()).hexdigest()
                witness_path.write_text(json.dumps(witness), encoding='utf-8')
                with patch.object(self.module.RunRepository, 'catalog', return_value=[task]):
                    with self.assertRaises(ValueError):
                        self.module.register(raw, workspace_root=root)
                self.assertFalse((root / 'CCS/data/config-evidence').exists())
