"""Bounded-I/O evidence observer in the experiment interpreter, without CUDA.

Small files and inventory are checked each minute; large files rotate at up to
4 MiB per second. A clean partial pass is NOT a full-data verification claim.
Observed drift remains sticky even if a file is subsequently restored.
"""
import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from controller.domain import utc_now
from trainctl.evidence import EvidenceStore, _plain, _relative, _signature, _read, environment_record
from trainctl.run_store import RunStore


SCAN_BYTES_PER_SECOND = 1024**2
ROTATION_INTERVAL_SECONDS = 24 * 60 * 60


class Auditor:
    def __init__(self, run_dir, binding, *, rotation_interval_seconds=ROTATION_INTERVAL_SECONDS,
                 scan_bytes_per_second=SCAN_BYTES_PER_SECOND):
        self.run_dir, self.binding = run_dir, binding
        self.rotation_interval_seconds = rotation_interval_seconds
        self.scan_bytes_per_second = scan_bytes_per_second
        self.store = EvidenceStore(run_dir.parent.parent / 'evidence')
        self.manifest = self.store.manifest(binding['bundle_id'], binding['manifest_sha256'])
        roots = {g['name']: Path(g['root']) for g in self.manifest['plan']['groups']}
        self.fast, self.slow = [], []
        self.target_scopes = {}
        for row in self.manifest['files']:
            rel = _relative(row['path'])
            if row['mode'] == 'copy':
                # Snapshot runs execute copied bytes, so later development in
                # the mutable source tree is deployment drift, not execution
                # drift. We still observe it, but report it in a separate scope.
                paths = [('source', roots[row['group']] / rel)]
                paths.append(('bundle', self.store.root / binding['bundle_id'] / 'files' / row['group'] / rel))
                if binding.get('execution_mode') == 'snapshot':
                    paths.append(('snapshot', run_dir / 'source-snapshot' / row['group'] / rel))
            else:
                paths = [('source', roots[row['group']] / rel)]
            for kind, path in paths:
                scope = ('deployment' if binding.get('execution_mode') == 'snapshot'
                         and row['mode'] == 'copy' and kind == 'source' else 'execution')
                self.target_scopes[(kind, str(path))] = scope
                target = (kind, path, row)
                (self.fast if row['mode'] == 'copy' and row['size'] <= 1024**2 else self.slow).append(target)
        self.issues, self.seen = [], set()
        self.deployment_issues, self.deployment_seen = [], set()
        self.observer_warnings, self.observer_seen = [], set()
        self.index, self.rounds, self.checked, self.bytes_read = 0, 0, 0, 0
        self.current = None
        self.last_fast = self.last_round = None
        self.next_rotation_monotonic = None

    def issue(self, kind, path, error):
        key = (kind, str(path))
        scope = self.target_scopes.get(key, 'execution')
        seen = self.deployment_seen if scope == 'deployment' else self.seen
        issues = self.deployment_issues if scope == 'deployment' else self.issues
        if key not in seen:
            seen.add(key)
            if len(issues) < 100:
                issues.append({'kind': kind, 'path': str(path), 'error': str(error), 'at': utc_now()})

    def observer_warning(self, code, kind, path, error):
        scope = self.target_scopes.get((kind, str(path)), 'execution')
        key = (scope, kind, str(path), code)
        if key not in self.observer_seen:
            self.observer_seen.add(key)
            if len(self.observer_warnings) < 100:
                self.observer_warnings.append({'code': code, 'scope': scope, 'kind': kind,
                    'path': str(path), 'error': str(error), 'at': utc_now()})

    def observer_recovered(self, kind, path):
        scope, value = self.target_scopes.get((kind, str(path)), 'execution'), str(path)
        self.observer_warnings = [item for item in self.observer_warnings
                                  if (item['scope'], item['kind'], item['path']) != (scope, kind, value)]
        self.observer_seen = {key for key in self.observer_seen if key[:3] != (scope, kind, value)}

    def fast_pass(self):
        self.store.manifest(self.binding['bundle_id'], self.binding['manifest_sha256'])
        if environment_record() != self.manifest['environment']:
            self.issue('environment', '', 'Interpreter package inventory changed')
        for kind, path, row in self.fast:
            try:
                if _signature(path)[2] != row['size']:
                    raise ValueError('Size differs')
                actual = _read(path, budget=row['size'])
                self.observer_recovered(kind, path)
                if actual != {k: row[k] for k in ('size', 'sha256')}:
                    raise ValueError('Content differs')
                if kind == 'snapshot' and os.name == 'posix' and path.stat().st_mode & 0o222:
                    raise ValueError('Snapshot file became writable')
            except ValueError as exc:
                if str(exc) in {'Evidence byte budget exceeded', 'Source changed while capturing evidence'}:
                    self.observer_warning('verification_race', kind, path, exc)
                else:
                    self.issue(kind, path, exc)
            except OSError as exc:
                if isinstance(exc, FileNotFoundError):
                    self.issue(kind, path, exc)
                else:
                    self.observer_warning('read_error', kind, path, exc)
        self.last_fast = utc_now()

    def step(self, budget=SCAN_BYTES_PER_SECOND):
        """Read at most budget bytes; never read an unbounded/changed file."""
        if not self.slow:
            return False
        kind, path, row = self.slow[self.index]
        try:
            if self.current is None:
                before = _signature(path)
                if before[2] != row['size']:
                    raise ValueError('Size differs')
                self.current = (_plain(path).open('rb'), before, hashlib.sha256(), 0)
            stream, before, digest, size = self.current
            block = stream.read(min(budget, max(1, row['size'] - size + 1)))
            size += len(block)
            self.bytes_read += len(block)
            digest.update(block)
            self.current = (stream, before, digest, size)
            if size > row['size']:
                raise ValueError('File grew during verification')
            if block and size < row['size']:
                return False
            if before != _signature(path):
                raise ValueError('Source changed while verifying evidence')
            self.observer_recovered(kind, path)
            if digest.hexdigest() != row['sha256'] or size != row['size']:
                raise ValueError('Content differs')
        except ValueError as exc:
            if str(exc) in {'File grew during verification', 'Source changed while verifying evidence'}:
                self.observer_warning('verification_race', kind, path, exc)
            else:
                self.issue(kind, path, exc)
        except OSError as exc:
            if isinstance(exc, FileNotFoundError):
                self.issue(kind, path, exc)
            else:
                self.observer_warning('read_error', kind, path, exc)
        self.close_file()
        self.checked += 1
        self.index = (self.index + 1) % len(self.slow)
        if self.index == 0:
            self.rounds += 1
            self.last_round = utc_now()
            self.next_rotation_monotonic = time.monotonic() + self.rotation_interval_seconds
            return True
        return False

    def rotation_due(self, now=None):
        if not self.slow:
            return False
        now = time.monotonic() if now is None else now
        return self.next_rotation_monotonic is None or now >= self.next_rotation_monotonic

    def close_file(self):
        if self.current:
            self.current[0].close()
            self.current = None

    def report(self, state='observing'):
        wait = (0 if self.next_rotation_monotonic is None else
                max(0, self.next_rotation_monotonic - time.monotonic()))
        deployment_targets = sum(scope == 'deployment' for scope in self.target_scopes.values())
        unknown_scopes = {item['scope'] for item in self.observer_warnings}
        coverage = ('not_started' if self.last_fast is None and self.rounds == 0 else
                    'selected_scope_checked' if self.last_fast is not None and (not self.slow or self.rounds) else
                    'partial')
        return {'schema_version': 2, 'state': state, 'updated_at': utc_now(), 'evidence': self.binding,
                'execution_integrity': {
                    'state': ('violation_observed' if self.seen else
                              'unknown' if 'execution' in unknown_scopes else 'no_violation_observed'),
                    'issue_count': len(self.seen), 'issues': self.issues,
                },
                'deployment': {
                    'state': ('changed' if self.deployment_seen else
                              'unknown' if 'deployment' in unknown_scopes else
                              'unchanged' if deployment_targets else 'not_applicable'),
                    'issue_count': len(self.deployment_seen), 'issues': self.deployment_issues,
                },
                'observer': {
                    'state': state,
                    'health': 'failed' if state == 'error' else 'degraded' if self.observer_seen else 'healthy',
                    'coverage': coverage, 'warning_count': len(self.observer_seen),
                    'warnings': self.observer_warnings,
                },
                'drift_detected': bool(self.seen), 'issue_count': len(self.seen), 'issues': self.issues,
                'last_fast_check_at': self.last_fast, 'last_full_rotation_at': self.last_round,
                'rotations_completed': self.rounds, 'large_targets': len(self.slow),
                'large_targets_checked': self.checked, 'rotation_position': self.index,
                'bytes_read': self.bytes_read, 'scan_rate_limit_bytes_per_second': self.scan_bytes_per_second,
                'rotation_interval_seconds': self.rotation_interval_seconds,
                'seconds_until_next_rotation': wait,
                'scope': 'Selected files only; periodic samples, not continuous immutability',
                'policy': 'Alert and retain evidence; do not terminate training'}


def main():
    run_dir = Path(sys.argv[1]).resolve()
    report_path = run_dir / 'evidence-runtime.json'
    auditor = None
    try:
        binding = json.loads((run_dir / 'spec.json').read_text())['evidence']
        auditor = Auditor(run_dir, binding)
        fast_due, publish_due = 0, 0
        while not (run_dir / 'evidence-watch-stop').exists():
            now = time.monotonic()
            if now >= fast_due:
                auditor.fast_pass()
                fast_due = time.monotonic() + 60
            # Fill this tick's budget across small images, not one image/second.
            if auditor.rotation_due(now):
                before = auditor.bytes_read
                for _ in range(128):
                    remaining = auditor.scan_bytes_per_second - (auditor.bytes_read - before)
                    if remaining <= 0 or not auditor.slow:
                        break
                    if auditor.step(remaining):
                        break
            if now >= publish_due:
                RunStore._atomic_json(report_path, auditor.report())
                publish_due = time.monotonic() + 5
            time.sleep(1)
        RunStore._atomic_json(report_path, auditor.report('closed'))
    except Exception as exc:
        report = auditor.report('error') if auditor else {'state': 'error', 'updated_at': utc_now()}
        RunStore._atomic_json(report_path, {**report, 'error': str(exc)})
        return 2
    finally:
        if auditor:
            auditor.close_file()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
