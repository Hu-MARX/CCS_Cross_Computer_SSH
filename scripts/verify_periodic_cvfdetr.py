"""Optional GPU fault-injection recipe; preview only until --confirm-run.

The explicitly supplied stage must contain the configured CVFDETR adapter and
damage_checkpoint_probe.py. They are project-specific, not CCS core software.
The probe must target --remote-root. No project or host is selected by default.
"""
import json
import argparse
import subprocess
import time
import re
import shlex
from pathlib import Path, PurePosixPath
from uuid import uuid4

from controller.domain import ExperimentSpec, RunStatus
from controller.remote import OpenSSHRunner, RemoteRunnerError


def cancel_and_wait(remote, run, wait):
    try:
        remote.cancel(run, 'interrupt after three periodic checkpoints for acceptance')
    except RemoteRunnerError as exc:
        if exc.code != 'CANCEL_PENDING':
            raise
    assert wait(run).status == RunStatus.CANCELLED


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--stage', required=True, help='Prepared absolute remote staging directory')
    parser.add_argument('--spec', type=Path, required=True, help='Explicit source experiment JSON')
    parser.add_argument('--probe-python', required=True, help='Absolute remote Python for the fault probe')
    parser.add_argument('--remote-root', default='/srv/ml-orchestrator')
    parser.add_argument('--trainctl-executable', default='trainctl')
    parser.add_argument('--port', type=int, default=22)
    parser.add_argument('--known-hosts', type=Path, default=Path.home() / '.ssh/known_hosts')
    parser.add_argument('--identity-file', type=Path, default=Path.home() / '.ssh/id_ed25519')
    parser.add_argument('--existing-source')
    parser.add_argument('--confirm-run', action='store_true',
                        help='Execute the shown GPU interruption/corruption/cleanup recipe')
    args = parser.parse_args(argv)
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.:-]*', args.host):
        parser.error('--host must be an SSH alias or host without whitespace')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', args.user) or not 1 <= args.port <= 65535:
        parser.error('Invalid --user or --port')
    for name in ('stage', 'remote_root', 'probe_python'):
        value = getattr(args, name)
        if not PurePosixPath(value).is_absolute() or '\x00' in value:
            parser.error(f'--{name.replace("_", "-")} must be an absolute POSIX path')
    if (args.trainctl_executable != 'trainctl'
            and not PurePosixPath(args.trainctl_executable).is_absolute()):
        parser.error('--trainctl-executable must be trainctl or an absolute POSIX path')
    if args.existing_source and not re.fullmatch(r'run_[a-f0-9]{32}', args.existing_source):
        parser.error('Invalid --existing-source')
    args.stage = args.stage.rstrip('/') or '/'
    return args


def acceptance_spec(args):
    raw = json.loads(args.spec.read_text(encoding='utf-8'))
    raw['name'] = 'periodic-checkpoint-source-acceptance'
    raw.setdefault('runtime', {}).setdefault('environment_variables', {})['PYTHONPATH'] = args.stage + '/src'
    raw['resources'].update(max_duration_seconds=600, max_queue_seconds=60)
    raw['command']['argv'] = [raw['command']['argv'][0], '-u', args.stage + '/cvfdetr_managed.py',
        '--batch-size', '1', '--grad-accum', '1', '--num-workers', '0', '--epochs', '2',
        '--limit-train-batches', '32', '--max-steps', '20', '--checkpoint-steps', '4', '--probe-hold-step', '12']
    ExperimentSpec.from_dict(raw)
    return raw


def fault_command(args, source):
    return shlex.join(['env', 'PYTHONPATH=' + args.stage + '/src',
                       'TRAINCTL_ROOT=' + args.remote_root, args.probe_python,
                       args.stage + '/damage_checkpoint_probe.py', source])


def main(argv=None):
    args = parse_args(argv)
    raw = acceptance_spec(args)
    if not args.confirm_run:
        print(json.dumps({'schema_version': 1, 'execution_state': 'not_sent',
            'host': args.host, 'user': args.user, 'port': args.port,
            'remote_root': args.remote_root, 'trainctl_executable': args.trainctl_executable,
            'source_run': args.existing_source, 'source_spec': raw,
            'fault_command': fault_command(args, args.existing_source or '<source_run>'),
            'actions': ['check GPU idle', 'submit source unless existing',
                        'interrupt source', 'corrupt staged checkpoint',
                        'submit recovery', 'wait and clean up created runs'],
            'next_step': 'Review this recipe, then repeat with --confirm-run'}, indent=2), flush=True)
        return 0
    remote = OpenSSHRunner(args.host, user=args.user, port=args.port,
        remote_root=args.remote_root, trainctl_executable=args.trainctl_executable,
        known_hosts=str(args.known_hosts), identity_file=str(args.identity_file))
    created = []
    report = {'status': 'incomplete'}
    def submit(spec):
        run = remote.submit(ExperimentSpec.from_dict(spec), 'periodic-' + uuid4().hex)
        created.append(run)
        print(json.dumps({'submitted': run, 'name': spec['name']}), flush=True)
        return run
    def wait(run, marker=None):
        deadline = time.monotonic() + 620
        previous = None
        while time.monotonic() < deadline:
            state = remote.snapshot(run)
            if state.status != previous:
                print(json.dumps({'run': run, 'status': state.status.value}), flush=True)
                previous = state.status
            if marker and marker in remote.logs(run)['text']:
                return state
            if state.status.terminal:
                if marker:
                    raise AssertionError('Run ended before checkpoint marker: ' + remote.logs(run, 'stderr')['text'][-6000:])
                return state
            time.sleep(3)
        raise TimeoutError(run)
    ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=10',
           '-o', 'UserKnownHostsFile=' + str(args.known_hosts),
           '-i', str(args.identity_file), '-p', str(args.port), args.user + '@' + args.host]
    try:
        processes = subprocess.run(ssh + ['nvidia-smi --query-compute-apps=pid --format=csv,noheader'],
            capture_output=True, text=True, encoding='utf-8', check=True, timeout=30)
        if processes.stdout.strip():
            raise RuntimeError('GPU already has compute processes; not starting acceptance')
        source = args.existing_source or submit(raw)
        report['source_run'] = source
        if args.existing_source:
            assert remote.snapshot(source).status == RunStatus.CANCELLED
        else:
            wait(source, 'PERIODIC_CHECKPOINTS_READY_FOR_INTERRUPT')
            cancel_and_wait(remote, source, wait)
        fault = subprocess.run(ssh + [fault_command(args, source)],
            capture_output=True, text=True, encoding='utf-8', check=True, timeout=60)
        report['fault'] = json.loads(fault.stdout)
        print(json.dumps(report['fault']), flush=True)
        resumed = json.loads(json.dumps(raw))
        resumed['name'] = 'periodic-checkpoint-resume-acceptance'
        resumed['command']['argv'] = resumed['command']['argv'][:-2] + ['--resume-from-run', source]
        target = submit(resumed)
        report['resumed_run'] = target
        state = wait(target)
        if state.status != RunStatus.SUCCEEDED:
            raise AssertionError(remote.logs(target, 'stderr')['text'][-7000:])
        assert 'MANAGED_CHECKPOINT_RUN_PASSED' in remote.logs(target)['text']
        report.update(status='passed', artifacts=remote.artifacts(target))
        print(json.dumps(report), flush=True)
    finally:
        for run in created:
            if not remote.snapshot(run).status.terminal:
                try:
                    remote.cancel(run, 'periodic checkpoint acceptance cleanup')
                except RemoteRunnerError as exc:
                    print(json.dumps({'cleanup_pending': run, 'error': str(exc)}), flush=True)
        Path('data').mkdir(exist_ok=True)
        path = Path('data') / ('periodic-checkpoint-acceptance-' + uuid4().hex + '.json')
        path.write_text(json.dumps(report, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
