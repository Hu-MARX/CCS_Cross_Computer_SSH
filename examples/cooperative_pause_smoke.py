"""CPU-only JSON counter demonstrating full-state pause and audited recovery.

This fixture has no model framework or historical research implementation.  Run
it as a declared CCS task with task.pause managed-checkpoint-v1.  Checkpoint
semantics are inspected in this trusted training process, not by the worker.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from pathlib import Path

from controller.configuration import spec_digest
from trainctl.checkpoints import CheckpointStore
from trainctl.pause_control import checkpoint_and_pause, request_digest
from trainctl.progress import ProgressReporter
from trainctl.recovery_input import RecoveryInput


CONTRACT = hashlib.sha256(b'ccs-cpu-counter-full-state-v1').hexdigest()
_PARTS = ('model', 'optimizer', 'scheduler', 'precision', 'rng', 'sampler', 'loop')


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def initial_state(contract):
    return {'contract': contract, 'global_step': 0, 'optimizer_steps': 0,
            'model': {'counter': 0}, 'optimizer': {'updates': 0, 'accumulator': 0},
            'scheduler': {'last_step': 0}, 'precision': {'mode': 'cpu-integer'},
            'rng': {'seed': 17, 'draws': 0}, 'sampler': {'cursor': 0},
            'loop': {'completed_updates': 0}, 'replay': {'safe_to_resume': True}}


def advance(state):
    # One complete deterministic optimizer update; no partial accumulation.
    state['global_step'] += 1
    step = state['global_step']
    state['optimizer_steps'] += 1
    state['model']['counter'] += step
    state['optimizer']['updates'] += 1
    state['optimizer']['accumulator'] += step * step
    state['scheduler']['last_step'] = step
    state['rng']['draws'] = step
    state['sampler']['cursor'] = step
    state['loop']['completed_updates'] = step


def inspect_state(value):
    if not isinstance(value, dict) or set(value) != set(initial_state(CONTRACT)):
        raise ValueError('CPU fixture checkpoint lacks full state')
    step = value['global_step']
    if (not isinstance(value['contract'], str) or not re.fullmatch(r'[a-f0-9]{64}', value['contract']) or
            type(step) is not int or not 0 <= step <= 2**63 - 1 or
            type(value['optimizer_steps']) is not int or value['optimizer_steps'] != step or
            any(not isinstance(value[key], dict) for key in _PARTS) or
            value['model'].get('counter') != step * (step + 1) // 2 or
            value['optimizer'].get('updates') != step or
            value['optimizer'].get('accumulator') != step * (step + 1) * (2 * step + 1) // 6 or
            value['scheduler'].get('last_step') != step or value['rng'].get('draws') != step or
            value['sampler'].get('cursor') != step or value['loop'].get('completed_updates') != step or
            value['replay'] != {'safe_to_resume': True}):
        raise ValueError('CPU fixture checkpoint state is inconsistent or not safely replayable')
    return {'contract': value['contract'], 'global_step': step, 'optimizer_steps': value['optimizer_steps'],
            'recovery_fingerprints': {key: fingerprint(value[key]) for key in _PARTS},
            'replay': dict(value['replay'])}


def validate_checkpoint(path):
    return inspect_state(json.loads(Path(path).read_text(encoding='utf-8')))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--steps', type=int, default=80)
    parser.add_argument('--delay', type=float, default=0.05)
    parser.add_argument('--resume-from-run')
    args = parser.parse_args(argv)
    if args.steps < 1 or not 0 <= args.delay <= 10:
        parser.error('steps must be positive and delay must be in 0..10')
    run_dir = Path(os.environ['TRAINCTL_RUN_DIR']).absolute()
    run_id = run_dir.name
    spec = json.loads((run_dir / 'spec.json').read_text(encoding='utf-8'))
    declaration = spec['task']['pause']
    contract = declaration['contract']
    digest = os.environ.get('TRAINCTL_SPEC_SHA256') or spec_digest(spec)
    output = Path.cwd() / 'orchestrator_runs' / run_id
    store = CheckpointStore(output / 'checkpoints', min_free_bytes=0)
    local_store = None
    if args.resume_from_run:
        # RecoveryInput independently copies the checkpoint before source pruning.
        if not re.fullmatch(r'run_[a-f0-9]{32}', args.resume_from_run):
            parser.error('resume-from-run must identify a managed Run')
        local_store = CheckpointStore(output.parent / args.resume_from_run / 'checkpoints', min_free_bytes=0)
    selection = RecoveryInput(validate_checkpoint).select(output / 'resume.ckpt', contract, local_store=local_store)
    audit = None
    if selection:
        from trainctl.recovery_audit import RecoveryAudit
        if args.resume_from_run:
            selection['source'] = {'kind': 'local-managed', 'source_run_id': args.resume_from_run}
        state = json.loads(Path(selection['path']).read_text(encoding='utf-8'))
        audit = RecoveryAudit(selection, run_dir, contract)
        audit.confirm_loaded(inspect_state(state))
    else:
        state = initial_state(contract)
    reporter = ProgressReporter(run_dir, interval=0)
    reporter.emit('training', step=state['global_step'], metrics={'counter': state['model']['counter']}, force=True)

    def save_checkpoint(request=None):
        metadata = {'contract': contract, 'run_id': run_id, 'pinned': request is not None}
        if request is not None:
            metadata['pause_request_sha256'] = request_digest(request)
        return store.commit(lambda path: path.write_text(json.dumps(state, allow_nan=False), encoding='utf-8'),
                            validate_checkpoint, metadata=metadata)

    while state['global_step'] < args.steps:
        advance(state)
        if audit is not None:
            audit.confirm_advanced(inspect_state(state))
            audit = None
        reporter.emit('training', step=state['global_step'], metrics={'counter': state['model']['counter']})
        print(json.dumps({'event': 'counter_update', 'step': state['global_step'],
                          'counter': state['model']['counter']}), flush=True)
        checkpoint_and_pause(run_dir, declaration, checkpoint_directory=store.directory,
                             spec_digest=digest, global_step=state['global_step'], save_checkpoint=save_checkpoint)
        time.sleep(args.delay)
    record = save_checkpoint()
    print(json.dumps({'event': 'counter_complete', 'step': state['global_step'],
                      'counter': state['model']['counter'], 'checkpoint_id': record['id']}), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
