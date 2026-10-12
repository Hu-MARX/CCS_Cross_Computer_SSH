"""Supervised, bounded evidence verification before queueing and GPU leasing."""
import time
from pathlib import Path
from uuid import uuid4

from controller.domain import RunStatus, utc_now
from .resources import Interrupted


def check(store, run_dir, spec, workdir, env, execute, phase):
    if spec.evidence is None:
        return
    report_path = run_dir / ('evidence-' + phase + '.json')
    nonce = uuid4().hex
    store._append_event(run_dir, 'evidence_check_started', {'phase': phase, 'evidence': spec.evidence})
    store._atomic_json(run_dir / 'evidence-gate.json', {'phase': phase, 'state': 'checking', 'at': utc_now(),
                                                      'evidence': spec.evidence})
    try:
        code = execute([spec.command['argv'][0], '-u', str(Path(__file__).with_name('evidence_probe.py')),
                        str(run_dir), phase, nonce], workdir=workdir, env=env, run_dir=run_dir,
                       deadline=time.monotonic() + spec.evidence.get('timeout_seconds', 600),
                       store=store, phase=RunStatus.PREPARING, started_event='evidence_probe_started')
        report = store._read_json(report_path, {})
        if code or not report.get('verified') or report.get('evidence') != spec.evidence or report.get('nonce') != nonce:
            raise ValueError('EVIDENCE_REJECTED: ' + phase + '; see evidence-' + phase + '.json')
        store._atomic_json(run_dir / 'evidence-gate.json', {**report, 'state': 'passed'})
        store._append_event(run_dir, 'evidence_check_passed', {'phase': phase, 'evidence': spec.evidence,
                                                             'spec_sha256': report['spec_sha256']})
    except Exception as exc:
        error = 'EVIDENCE_TIMEOUT' if isinstance(exc, Interrupted) and str(exc) == 'TIMEOUT' else str(exc)
        store._atomic_json(run_dir / 'evidence-gate.json', {'phase': phase, 'state': 'failed', 'at': utc_now(),
                                                          'error': error, 'evidence': spec.evidence})
        store._append_event(run_dir, 'evidence_check_failed', {'phase': phase, 'error': error, 'evidence': spec.evidence})
        if error == 'EVIDENCE_TIMEOUT':
            raise Interrupted(error) from exc
        raise
