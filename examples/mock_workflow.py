"""CPU-only, synthetic workflow acceptance demo. NEVER contacts SSH or a GPU.

Run from the project: python examples/mock_workflow.py --directory data/mock-workflow
All metrics are deliberately synthetic. This is not a production scheduler.
"""
import argparse
import copy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from controller.domain import ExperimentSpec, RunStatus, utc_now
from controller.orchestrator import Orchestrator
from controller.remote import FakeRemoteRunner, RemoteEvent
from controller.repository import RunRepository
from controller.results import descriptor
from dashboard.catalog import Catalog
from dashboard.feed import atomic_json


class MockRemote(FakeRemoteRunner):
    """Durable fake remote with production-shaped samples and artifacts."""
    def __init__(self, directory):
        super().__init__()
        self.directory = Path(directory)
        self.path = self.directory / 'mock-remote.json'
        self.offline = False
        if self.path.exists():
            saved = json.loads(self.path.read_text())
            self.keys, self.runs = saved['keys'], saved['runs']
            for run in self.runs.values():
                run['status'] = RunStatus(run['status'])
                run['events'] = [RemoteEvent(**event) for event in run['events']]

    def save(self):
        runs = {key: {**value, 'events': [asdict(e) for e in value['events']]} for key, value in self.runs.items()}
        atomic_json(self.path, {'keys': self.keys, 'runs': runs})

    def online(self):
        if self.offline:
            raise ConnectionError('SIMULATED disconnect; no network used')

    def submit(self, spec, idempotency_key):
        self.online()
        if idempotency_key in self.keys:
            found = self.runs[self.keys[idempotency_key]]
            if found['spec'] != spec.to_dict():
                raise ValueError('Idempotency key conflicts with a different spec')
        run_id = super().submit(spec, idempotency_key)
        self.runs[run_id].setdefault('spec', spec.to_dict())
        self.runs[run_id].setdefault('samples', [])
        self.save()
        return run_id

    def record(self, run_id, status, phase, metrics, exit_code=None):
        self.advance(run_id, status, exit_code=exit_code)
        rows = self.runs[run_id]['samples']
        rows.append({'id': len(rows) + 1, 'run_id': run_id, 'created_at': utc_now(), 'payload': {
            'lifecycle': status.value, 'observation_state': 'fresh',
            'execution': {'ExecMainStatus': exit_code},
            'progress': {'phase': phase, 'step': metrics.get('progress/completed'),
                         'updated_at': utc_now(), 'metrics': metrics}}})
        self.save()

    def monitor(self):
        self.online()
        return {'observed_at': utc_now(), 'observation_state': 'fresh', 'health': {'gpu': []}}

    def alerts(self):
        self.online()
        return []

    def samples(self, run_id, after_id=0, limit=200):
        self.online()
        return copy.deepcopy([x for x in self.runs[run_id]['samples'] if x['id'] > after_id][:limit])

    def artifacts(self, run_id):
        self.online()
        path = self.directory / (run_id + '-synthetic-report.json')
        atomic_json(path, {'synthetic': True, 'run_id': run_id})
        return [{'path': str(path), 'size': path.stat().st_size,
                 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}]


def make_spec(kind, source=None, checkpoint=None):
    task = {'kind': kind, 'inputs': []}
    if source:
        task.update(source_run_id=source, checkpoint=checkpoint)
    return ExperimentSpec.from_dict({'schema_version': 1, 'name': 'SIMULATION ' + kind,
        'project': {}, 'runtime': {}, 'command': {'argv': ['synthetic-only']},
        'outputs': {'artifact_glob': '*.json'}, 'task': task})


def run_demo(directory, outcome='success'):
    directory = Path(directory)
    remote = MockRemote(directory)
    repo = RunRepository(directory / 'controller.db')
    controller = Orchestrator(repo, remote, worker='simulation')
    train_spec = make_spec('train')
    train = controller.submit(train_spec, idempotency_key='mock-train-v1')
    catalog = Catalog(remote, descriptor(train.id, train_spec.to_dict(), 'simulation'), directory / 'cache',
        discover=lambda: RunRepository.catalog(directory / 'controller.db', 'simulation'))
    if not remote.runs[train.id]['status'].terminal:
        status = RunStatus.FAILED if outcome == 'failure' else RunStatus.SUCCEEDED
        remote.record(train.id, status, 'finished', {'train/loss': 0.5, 'progress/completed': 2,
            'progress/total': 2}, 1 if status == RunStatus.FAILED else 0)
    remote.offline = outcome == 'disconnect'
    catalog.poll()
    result = catalog.view()['result']
    test = None
    # This demo contains its own explicit synthetic-only approval, not a real-test authorization.
    if result['observation']['state'] == 'fresh' and result['execution']['status'] == 'SUCCEEDED':
        checkpoint_path = directory / 'synthetic-checkpoint.json'
        atomic_json(checkpoint_path, {'synthetic': True, 'source_run_id': train.id})
        checkpoint = {'path': str(checkpoint_path), 'sha256': hashlib.sha256(checkpoint_path.read_bytes()).hexdigest()}
        test = controller.submit(make_spec('test', train.id, checkpoint), idempotency_key='mock-test-v1')
        if not remote.runs[test.id]['status'].terminal:
            remote.record(test.id, RunStatus.RUNNING, 'test', {'progress/completed': 1, 'progress/total': 2})
            catalog.poll()
            if outcome == 'test_failure':
                remote.record(test.id, RunStatus.FAILED, 'test', {'progress/completed': 1, 'progress/total': 2}, 1)
            else:
                remote.record(test.id, RunStatus.SUCCEEDED, 'test_finished',
                    {'progress/completed': 2, 'progress/total': 2, 'test/mAP': 0.42, 'test/AP50': 0.61}, 0)
        catalog.poll()
    report = {'synthetic': True, 'train': catalog.view()['result'],
              'test': catalog.view(test.id)['result'] if test else None, 'remote_run_count': len(remote.runs)}
    atomic_json(directory / 'acceptance.json', report)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, default=Path('data/mock-workflow'))
    parser.add_argument('--outcome', choices=['success', 'failure', 'disconnect', 'test_failure'], default='success')
    args = parser.parse_args()
    print(json.dumps(run_demo(args.directory, args.outcome), ensure_ascii=True, indent=2))
