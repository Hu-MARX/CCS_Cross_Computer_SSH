"""CPU-only acceptance Adapter for the gated recovery-input path."""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from trainctl.recovery_input import RecoveryInput
from trainctl.run_store import RunStore


def inspect_json_checkpoint(path):
    value = json.loads(Path(path).read_text(encoding='utf-8'))
    if (not isinstance(value, dict) or value.get('schema_version') != 1 or
            not isinstance(value.get('contract'), str) or
            type(value.get('global_step')) is not int):
        raise ValueError('invalid synthetic checkpoint')
    return {'contract': value['contract'], 'global_step': value['global_step']}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=('produce', 'consume'), required=True)
    parser.add_argument('--contract', required=True)
    parser.add_argument('--hold-seconds', type=float, default=120)
    args = parser.parse_args(argv)
    if args.hold_seconds <= 0:
        raise ValueError('hold-seconds must be positive')
    run_dir = Path(os.environ['TRAINCTL_RUN_DIR']).absolute()
    if run_dir.name != os.environ.get('TRAINCTL_RUN_ID'):
        raise ValueError('unexpected CCS run directory')

    if args.mode == 'produce':
        checkpoint = run_dir / 'synthetic-checkpoint.json'
        RunStore._atomic_json(checkpoint, {
            'schema_version': 1,
            'contract': args.contract,
            'global_step': 3,
        })
        RunStore._atomic_json(run_dir / 'synthetic-ready.json', {
            'schema_version': 1,
            'checkpoint': str(checkpoint),
        })
        print('SYNTHETIC_CHECKPOINT_READY ' + str(checkpoint), flush=True)
        time.sleep(args.hold_seconds)
        raise RuntimeError('acceptance controller did not interrupt the producer')

    selection = RecoveryInput(inspect_json_checkpoint).select(
        run_dir / 'resume-source.ckpt', args.contract,
    )
    if selection is None or selection['state']['global_step'] != 3:
        raise ValueError('gated recovery input was not restored')
    RunStore._atomic_json(run_dir / 'recovery-smoke-result.json', {
        'schema_version': 1,
        'status': 'passed',
        'source': selection['source'],
        'selected_sha256': selection['selected']['sha256'],
        'restored_global_step': selection['state']['global_step'],
    })
    print('RECOVERY_INPUT_SMOKE_PASSED', flush=True)


if __name__ == '__main__':
    main()
