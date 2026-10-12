"""Per-run selected-file snapshots. Unix permissions prevent accidental writes,
not deliberate chmod by the owner or writes by root; this is not a sandbox.
"""
import os
import shutil
from pathlib import Path

from .evidence import EvidenceStore, _plain, _relative, _read
from .run_store import RunStore


def prepare(run_dir, binding, manifest):
    if os.name != 'posix':
        raise ValueError('Snapshot execution currently requires Linux/POSIX permissions')
    destination = run_dir / 'source-snapshot'
    needed = sum(row['size'] for row in manifest['files'] if row['mode'] == 'copy')
    if shutil.disk_usage(run_dir).free < needed + 5 * 1024**3:
        raise ValueError('Insufficient free space for per-run source snapshot')
    destination.mkdir(exist_ok=False)
    bundle = run_dir.parent.parent / 'evidence' / binding['bundle_id'] / 'files'
    copied = 0
    for row in manifest['files']:
        if row['mode'] != 'copy':
            continue
        relative = _relative(row['group']) / _relative(row['path'])
        source = _plain(bundle / relative)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if _read(source, target, row['size']) != {key: row[key] for key in ('size', 'sha256')}:
            raise ValueError('Snapshot copy hash mismatch: ' + str(relative))
        target.chmod(0o444)
        copied += 1
    for directory, _, _ in os.walk(destination, topdown=False):
        Path(directory).chmod(0o555)
    report = {'root': str(destination), 'files': copied, 'evidence': binding,
              'protection': 'POSIX 0444 files / 0555 directories; owner can chmod; not a security sandbox'}
    RunStore._atomic_json(run_dir / 'source-snapshot.json', report)
    return report


def launch(run_dir, spec, env):
    argv = list(spec.command['argv'])
    if not spec.evidence or spec.evidence.get('execution_mode') != 'snapshot':
        return argv, env
    manifest = EvidenceStore(run_dir.parent.parent / 'evidence').manifest(
        spec.evidence['bundle_id'], spec.evidence['manifest_sha256'])
    snapshot = _plain(run_dir / 'source-snapshot', directory=True)
    groups = sorted((g for g in manifest['plan']['groups'] if g['mode'] == 'copy'),
                    key=lambda g: len(g['root']), reverse=True)

    def mapped(value):
        path = Path(value).absolute()
        for group in groups:
            try:
                relative = path.relative_to(Path(group['root']))
            except ValueError:
                continue
            return snapshot / group['name'] / relative
        raise ValueError('PYTHONPATH/entry is not covered by snapshot: ' + value)

    index = 1
    while argv[index] in ('-u', '-B'):
        index += 1
    argv[index] = str(_plain(mapped(argv[index])))
    env = dict(env)
    env['PYTHONPATH'] = os.pathsep.join(str(_plain(mapped(p), directory=True))
                                      for p in env.get('PYTHONPATH', '').split(os.pathsep) if p)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    env['PYTHONNOUSERSITE'] = '1'
    env['TRAINCTL_SNAPSHOT_ROOT'] = str(snapshot)
    return argv, env
