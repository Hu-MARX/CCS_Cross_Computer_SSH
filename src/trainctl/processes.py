"""Bounded, privacy-preserving discovery of compute workloads on Linux.

The dashboard needs enough information to reveal an unregistered workload, but
must not expose full command lines, environment variables, or arbitrary paths.
"""
from __future__ import annotations

import os
from pathlib import Path

from controller.domain import utc_now


KEYWORDS = ('python', 'torchrun', 'accelerate', 'deepspeed', 'isaac', 'mujoco',
            'habitat', 'gazebo', 'ros2', 'roscore')
INTERNAL_MODULES = {'-m trainctl.worker', '-m trainctl.cli', '-m trainctl.evidence_watch'}
ACTIVE = {'QUEUED', 'PREPARING', 'RUNNING', 'EVALUATING', 'LOST'}


def _text(path: Path, limit=65536):
    try:
        return path.read_bytes()[:limit].decode('utf-8', errors='replace')
    except (OSError, ValueError):
        return ''


def _managed_processes(runs: Path):
    pids = set()
    for directory in list(runs.glob('run_*'))[:4096]:
        try:
            import json
            status = json.loads((directory / 'status.json').read_text(encoding='utf-8'))
            pid = status.get('pid')
            if status.get('status') in ACTIVE and type(pid) is int and pid > 1:
                pids.add(pid)
        except (OSError, ValueError, TypeError):
            continue
    groups = set()
    getpgid = getattr(os, 'getpgid', None)
    for pid in pids:
        try:
            if getpgid:
                groups.add(getpgid(pid))
        except OSError:
            pass
    return pids, groups


def _descends_from(pid, ancestors, proc):
    seen = set()
    current = pid
    for _ in range(64):
        if current in ancestors:
            return True
        if current <= 1 or current in seen:
            return False
        seen.add(current)
        status = _text(proc / str(current) / 'status', 16384)
        parent = next((line.split(':', 1)[1].strip() for line in status.splitlines()
                       if line.startswith('PPid:')), '')
        try:
            current = int(parent)
        except ValueError:
            return False
    return False


def discover(runs: Path, proc: Path = Path('/proc'), limit=128, *, supported=None):
    """Return sanitized workload identities; never return argv or full cwd."""
    if supported is None:
        supported = os.name == 'posix'
    if not supported or not proc.is_dir():
        return {'state': 'unsupported', 'observed_at': utc_now(), 'items': []}
    managed_pids, managed_groups = _managed_processes(runs)
    items = []
    for directory in list(proc.iterdir())[:32768]:
        if not directory.name.isdigit():
            continue
        pid = int(directory.name)
        if pid in {os.getpid(), os.getppid()}:
            continue
        try:
            raw = (directory / 'cmdline').read_bytes()[:65536]
        except OSError:
            raw = b''
        argv = [part.decode('utf-8', errors='replace') for part in raw.split(b'\0') if part]
        comm = _text(directory / 'comm', 256).strip()
        identity = ' '.join([Path(value).name.lower() for value in ([comm] + argv[:4])])
        if not any(keyword in identity for keyword in KEYWORDS):
            continue
        script = next((Path(value).name for value in argv[1:8]
                       if value.lower().endswith(('.py', '.sh'))), None)
        module = None
        try:
            candidate = argv[argv.index('-m') + 1]
            if candidate and all(part.replace('_', '').replace('-', '').isalnum()
                                 for part in candidate.split('.')):
                module = '-m ' + candidate
        except (ValueError, IndexError):
            pass
        status = _text(directory / 'status', 16384)
        fields = {}
        for line in status.splitlines():
            if ':' in line:
                key, value = line.split(':', 1)
                if key in {'State', 'VmRSS'}:
                    fields[key] = value.strip()
        try:
            getpgid = getattr(os, 'getpgid', None)
            pgid = getpgid(pid) if getpgid else None
        except OSError:
            pgid = None
        try:
            project = Path(os.readlink(directory / 'cwd')).name or None
        except OSError:
            project = None
        memory_kib = None
        try:
            memory_kib = int(fields.get('VmRSS', '').split()[0])
        except (ValueError, IndexError):
            pass
        managed = (pid in managed_pids or pgid in managed_groups or
                   _descends_from(pid, managed_pids, proc))
        if not managed and module in INTERNAL_MODULES:
            continue
        if not managed and script is None and module is None:
            continue
        items.append({'pid': pid, 'launcher': Path(argv[0]).name if argv else comm,
                      'script': script or module, 'project': project,
                      'state': fields.get('State', '').split(' ', 1)[0] or None,
                      'memory_bytes': memory_kib * 1024 if memory_kib is not None else None,
                      'managed': managed})
        if len(items) >= limit:
            break
    items.sort(key=lambda item: (not item['managed'], item['pid']))
    return {'state': 'available', 'observed_at': utc_now(), 'truncated': len(items) >= limit,
            'items': items}
