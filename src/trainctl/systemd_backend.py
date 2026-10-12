"""User-systemd adapter; never grant the worker root or arbitrary sudo access."""
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path


def boot_id():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def unit_name(root, run_id):
    if not re.fullmatch(r'run_[a-f0-9]{32}', run_id):
        raise ValueError('Invalid run identifier')
    namespace = hashlib.sha256(str(Path(root).resolve()).encode()).hexdigest()[:10]
    return f'mlrun-{namespace}-{run_id}.service'


def call(argv, *, allow_missing=False):
    env = os.environ.copy()
    if hasattr(os, 'getuid'):
        env.setdefault('XDG_RUNTIME_DIR', f'/run/user/{os.getuid()}')
        env.setdefault('DBUS_SESSION_BUS_ADDRESS', f"unix:path={env['XDG_RUNTIME_DIR']}/bus")
    result = subprocess.run(argv, capture_output=True, text=True, encoding='utf-8', timeout=20, env=env)
    if result.returncode and not (allow_missing and 'LoadState=not-found' in result.stdout):
        raise RuntimeError(f"systemd command failed: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout


def inspect_unit(unit):
    if not re.fullmatch(r'mlrun-[a-f0-9]{10}-run_[a-f0-9]{32}\.service', unit):
        raise ValueError('Invalid managed unit name')
    output = call(['systemctl', '--user', 'show', unit,
        '--property=LoadState,ActiveState,SubState,Result,ExecMainStatus,MainPID,ControlGroup,MemoryCurrent,CPUUsageNSec'], allow_missing=True)
    return dict(line.split('=', 1) for line in output.splitlines() if '=' in line)


def is_quiescent(state):
    if state.get('ActiveState') not in {'inactive', 'failed'}:
        return False
    group = state.get('ControlGroup', '')
    if not group:
        return True
    root = Path('/sys/fs/cgroup')
    path = (root / group.lstrip('/') / 'cgroup.events').resolve()
    if not path.is_relative_to(root):
        raise ValueError('Invalid cgroup path')
    try:
        return 'populated 0' in path.read_text().splitlines()
    except FileNotFoundError:
        return True


def start(run_dir, execution):
    source_root = str(Path(__file__).resolve().parents[1])
    call(['systemd-run', '--user', '--quiet', '--service-type=exec',
          '--unit=' + execution['unit'], '--property=KillMode=control-group',
          '--property=TimeoutStopSec=15', '--property=Restart=no',
          '--property=WorkingDirectory=' + str(run_dir),
          '--property=StandardOutput=append:' + str(run_dir / 'runner.log'),
          '--property=StandardError=append:' + str(run_dir / 'runner.log'),
          '--setenv=PYTHONPATH=' + source_root, '--setenv=PYTHONUNBUFFERED=1',
          '--', sys.executable, '-m', 'trainctl.runner', str(run_dir)])


def stop(unit):
    # Asynchronous: cancellation is confirmed only after cgroup quiescence.
    call(['systemctl', '--user', 'stop', '--no-block', unit])
