"""Run in the task's interpreter, before its training entry is invoked."""
import hashlib
import json
import sys
from pathlib import Path

# Direct script execution makes the verifier independent of cwd/PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from controller.domain import ExperimentSpec, utc_now
from trainctl.evidence import EvidenceStore
from trainctl.run_store import RunStore


def spec_digest(spec):
    return hashlib.sha256(json.dumps(spec.to_dict(), sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()


def main():
    run_dir, phase = Path(sys.argv[1]).resolve(), sys.argv[2]
    if phase not in ('preflight', 'launch'):
        raise ValueError('Invalid gate phase')
    spec = ExperimentSpec.from_dict(json.loads((run_dir / 'spec.json').read_text(encoding='utf-8')))
    binding = spec.evidence
    result = {'phase': phase, 'checked_at': utc_now(), 'verified': False,
              'evidence': binding, 'spec_sha256': spec_digest(spec), 'nonce': sys.argv[3], 'issues': []}
    try:
        submitted = json.loads((run_dir / 'manifest.json').read_text(encoding='utf-8'))
        if submitted['config'] != spec.to_dict() or submitted.get('evidence') != binding:
            raise ValueError('Submitted experiment specification changed')
        store = EvidenceStore(run_dir.parent.parent / 'evidence')
        manifest = store.manifest(binding['bundle_id'], binding['manifest_sha256'])
        index = 1
        while spec.command['argv'][index] in ('-u', '-B'):
            index += 1
        entry = Path(spec.command['argv'][index]).absolute()
        roots = {group['name']: Path(group['root']) for group in manifest['plan']['groups']}
        if not any(row['mode'] == 'copy' and (roots[row['group']] / row['path']).absolute() == entry for row in manifest['files']):
            raise ValueError('Python entry is not a copied source in the evidence bundle')
        result.update(store.verify(binding['bundle_id'], binding['manifest_sha256'],
                                   check_sources=True, check_environment=True))
        if result['verified'] and phase == 'launch' and binding.get('execution_mode') == 'snapshot':
            from trainctl.source_snapshot import prepare
            result['snapshot'] = prepare(run_dir, binding, manifest)
    except Exception as exc:
        result['verified'] = False
        result['issues'].append({'kind': 'gate', 'error': str(exc)})
    RunStore._atomic_json(run_dir / ('evidence-' + phase + '.json'), result)
    print('EVIDENCE_CHECK ' + json.dumps({'phase': phase, 'verified': result['verified'],
                                       'bundle_id': binding['bundle_id'], 'issues': len(result['issues'])}), flush=True)
    return 0 if result['verified'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
