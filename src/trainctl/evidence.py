"""Write-once, explicitly scoped evidence bundles; not a filesystem sandbox.

Capture copies selected code/config and hashes selected data. Verification needs
the manifest digest returned to the caller, not a mutable self-signed sidecar.
No execution, network calls, environment-variable dumps, or dataset modification.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import stat
import sys
from pathlib import Path, PurePosixPath
from uuid import uuid4

from controller.domain import utc_now


def _json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _relative(value):
    if not isinstance(value, str) or not value or '\\' in value or ':' in value:
        raise ValueError('Invalid relative evidence path')
    path = PurePosixPath(value)
    if path.is_absolute() or any(p in ('', '.', '..') for p in value.split('/')):
        raise ValueError('Invalid relative evidence path')
    return Path(*path.parts)


def _plain(path, directory=False):
    path = Path(path).absolute()
    if path.resolve() != path or path.is_symlink():
        raise ValueError('Symlinks/junctions are not allowed in evidence paths')
    mode = path.stat().st_mode
    if not (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)):
        raise ValueError('Expected a regular evidence ' + ('directory' if directory else 'file'))
    return path


def _signature(path):
    value = _plain(path).stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _read(path, destination=None, budget=None):
    before = _signature(path)
    digest, size = hashlib.sha256(), 0
    with path.open('rb') as source:
        output = destination.open('xb') if destination else None
        try:
            while block := source.read(1024 * 1024):
                size += len(block)
                if budget is not None and size > budget:
                    raise ValueError('Evidence byte budget exceeded')
                digest.update(block)
                if output:
                    output.write(block)
            if output:
                output.flush()
                os.fsync(output.fileno())
        finally:
            if output:
                output.close()
    if before != _signature(path) or size != before[2]:
        raise ValueError('Source changed while capturing evidence')
    return {'size': size, 'sha256': digest.hexdigest()}


def _fsync_directory(path):
    if os.name != 'nt':
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def environment_record():
    # Do not collect direct_url.json, pip index URLs, credentials or env vars.
    packages = sorted({(item.metadata['Name'], item.version)
                       for item in importlib.metadata.distributions() if item.metadata['Name']})
    return {'python': sys.version, 'executable': sys.executable, 'platform': platform.platform(),
            'packages': [{'name': name, 'version': version} for name, version in packages],
            'scope': 'Current interpreter inventory; not a locked/rebuilt environment'}


class EvidenceStore:
    def __init__(self, root):
        self.root = Path(root).absolute()

    def capture(self, plan):
        if not isinstance(plan, dict) or type(plan.get('schema_version')) is not int or plan.get('schema_version') != 1 or set(plan) - {'schema_version', 'groups', 'max_bytes', 'max_files', 'min_free_bytes'}:
            raise ValueError('Unsupported evidence plan')
        limits = {key: plan.get(key, default) for key, default in
                  [('max_bytes', 512 * 1024**2), ('max_files', 10000), ('min_free_bytes', 5 * 1024**3)]}
        if any(type(v) is not int or v < (0 if k == 'min_free_bytes' else 1) for k, v in limits.items()):
            raise ValueError('Invalid evidence limits')
        entries, names, total, copy_size = [], set(), 0, 0
        groups = plan.get('groups')
        if not isinstance(groups, list) or not groups:
            raise ValueError('Explicit evidence groups are required')
        for group in groups:
            if not isinstance(group, dict) or set(group) != {'name', 'root', 'mode', 'files'}:
                raise ValueError('Evidence groups require name/root/mode/files only')
            name, mode = group['name'], group['mode']
            if not isinstance(name, str) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', name) or name in names:
                raise ValueError('Invalid/duplicate evidence group')
            names.add(name)
            if mode not in ('copy', 'hash') or not isinstance(group['root'], str) or not Path(group['root']).is_absolute():
                raise ValueError('Evidence mode must be copy/hash with an absolute root')
            root = _plain(Path(group['root']), directory=True)
            if not isinstance(group['files'], list) or not group['files']:
                raise ValueError('Explicit nonempty file list required; no recursive auto-discovery')
            seen = set()
            for value in group['files']:
                relative = _relative(value)
                source = _plain(root / relative)
                if source in seen:
                    raise ValueError('Duplicate evidence file')
                seen.add(source)
                size = source.stat().st_size
                total += size
                copy_size += size if mode == 'copy' else 0
                entries.append((name, mode, value, source))
                if len(entries) > limits['max_files'] or total > limits['max_bytes']:
                    raise ValueError('Evidence plan exceeds file/byte budget')
        ancestor = self.root
        while not ancestor.exists() and not ancestor.is_symlink():
            ancestor = ancestor.parent
        _plain(ancestor, directory=True)
        self.root.mkdir(parents=True, exist_ok=True)
        _plain(self.root, directory=True)
        if shutil.disk_usage(self.root).free < copy_size + limits['min_free_bytes']:
            raise ValueError('Insufficient free space for evidence snapshot')
        token = uuid4().hex
        stage, destination = self.root / ('.partial-' + token), self.root / ('bundle_' + token)
        stage.mkdir()
        # Incomplete captures are retained for diagnosis, never visible as bundles.
        rows, consumed = [], 0
        for name, mode, relative, source in entries:
            target = stage / 'files' / name / _relative(relative) if mode == 'copy' else None
            if target:
                target.parent.mkdir(parents=True, exist_ok=True)
            record = _read(source, target, limits['max_bytes'] - consumed)
            consumed += record['size']
            rows.append({'group': name, 'path': relative, 'mode': mode, **record})
        # A second pass detects ordinary edits after a file's initial capture.
        for entry, row in zip(entries, rows):
            if _read(entry[3], budget=limits['max_bytes']) != {key: row[key] for key in ('size', 'sha256')}:
                raise ValueError('Source drift detected before evidence commit')
        manifest = {'schema_version': 1, 'bundle_id': destination.name, 'created_at': utc_now(),
                    'plan': plan, 'files': rows, 'environment': environment_record(),
                    'scope': 'Explicit selected files only; write-once API, not OS immutability or execution isolation'}
        raw = _json(manifest)
        with (stage / 'manifest.json').open('xb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        for directory, _, _ in os.walk(stage, topdown=False):
            _fsync_directory(directory)
        os.rename(stage, destination)
        _fsync_directory(self.root)
        return {'bundle_id': destination.name, 'manifest_sha256': hashlib.sha256(raw).hexdigest(),
                'files': len(rows), 'hashed_bytes': consumed,
                'copied_bytes': sum(row['size'] for row in rows if row['mode'] == 'copy'),
                'path': str(destination), 'environment_python': sys.executable}

    def manifest(self, bundle_id, expected_sha256):
        if not re.fullmatch(r'bundle_[a-f0-9]{32}', bundle_id) or not re.fullmatch(r'[a-f0-9]{64}', expected_sha256):
            raise ValueError('Bundle id and external manifest SHA256 are required')
        directory = _plain(self.root / bundle_id, directory=True)
        raw = _plain(directory / 'manifest.json').read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise ValueError('Evidence manifest does not match caller digest')
        manifest = json.loads(raw)
        if manifest['schema_version'] != 1 or manifest['bundle_id'] != bundle_id:
            raise ValueError('Unsupported evidence manifest')
        return manifest

    def verify(self, bundle_id, expected_sha256, *, check_sources=False, check_environment=False):
        manifest = self.manifest(bundle_id, expected_sha256)
        directory = self.root / bundle_id
        roots = {group['name']: Path(group['root']) for group in manifest['plan']['groups']}
        issues = []
        if check_environment and environment_record() != manifest['environment']:
            issues.append({'kind': 'environment', 'error': 'Current interpreter inventory differs from captured inventory'})
        for row in manifest['files']:
            relative = _relative(row['path'])
            if not re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', row['group']):
                raise ValueError('Invalid evidence group')
            targets = [('copy', directory / 'files' / row['group'] / relative)] if row['mode'] == 'copy' else []
            if check_sources:
                targets.append(('source', roots[row['group']] / relative))
            for kind, path in targets:
                try:
                    actual = _read(path, budget=row['size'])
                    if actual != {key: row[key] for key in ('size', 'sha256')}:
                        raise ValueError('Content differs')
                except (ValueError, OSError) as exc:
                    issues.append({'kind': kind, 'group': row['group'], 'path': row['path'], 'error': str(exc)})
        return {'bundle_id': bundle_id, 'verified': not issues, 'issues': issues,
                'source_check': 'checked' if check_sources else 'not_checked',
                'environment_check': 'checked' if check_environment else 'not_checked',
                'copied_files_checked': sum(row['mode'] == 'copy' for row in manifest['files']),
                'hash_only_files_checked': sum(row['mode'] == 'hash' for row in manifest['files']) if check_sources else 0,
                'files': len(manifest['files']), 'manifest_sha256': expected_sha256}
