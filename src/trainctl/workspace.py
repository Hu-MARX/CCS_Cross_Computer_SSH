"""Transactional, allow-listed remote source workspace.

One deep module owns path confinement, symlink rejection, staged overlays,
optimistic hash checks, recoverable trash, rollback, and bounded inspection.
Callers submit one request and never manipulate server paths directly.
"""
from __future__ import annotations

import difflib
import hashlib
import json
import os
import re
import shutil
from pathlib import Path, PurePosixPath
from uuid import uuid4

from controller.domain import utc_now
from .locking import file_lock
from .run_store import TrainCtlError


CHANGE_ID = re.compile(r'change_[a-f0-9]{32}')
MAX_FILE = 1024 * 1024
MAX_CHANGE = 16 * 1024 * 1024
PROTECTED_PARTS = {'data', 'dataset', 'datasets', 'checkpoint', 'checkpoints', 'weights'}
CONTROL_PARTS = {'.git', '.hg', '.svn', '.ml-orchestrator', '.trainctl'}
PROTECTED_SUFFIXES = {'.pt', '.pth', '.ckpt', '.safetensors', '.onnx', '.npy', '.npz',
                      '.h5', '.hdf5', '.parquet', '.zip', '.tar'}
SENSITIVE_NAMES = {'.env', '.git-credentials', 'id_rsa', 'id_ed25519', 'credentials.json',
                   'secrets.json'}


def _sha(path):
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open('rb') as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json_sha(value):
    raw = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(raw).hexdigest()


def _atomic_json(path, value):
    _atomic_bytes(path, json.dumps(value, ensure_ascii=True, sort_keys=True).encode(), 0o600)


def _atomic_bytes(path, content, mode=0o644):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.' + path.name + '.' + uuid4().hex + '.tmp')
    try:
        with temporary.open('xb') as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class WorkspaceManager:
    """Small request/result interface over all remote workspace invariants."""

    def __init__(self, state_root, config):
        self.state = Path(state_root).resolve() / 'workspaces'
        roots = config.get('workspace_roots', [])
        if (not isinstance(roots, list) or len(roots) > 16 or
                any(not isinstance(value, str) or not Path(value).is_absolute() for value in roots)):
            raise TrainCtlError('INVALID_WORKSPACE_CONFIG', 'workspace_roots must be a bounded list of absolute paths')
        if any(Path(value).is_symlink() for value in roots):
            raise TrainCtlError('INVALID_WORKSPACE_CONFIG', 'workspace roots cannot be symlinks')
        self.roots = [Path(value).resolve() for value in roots]
        self.state.mkdir(parents=True, exist_ok=True)
        if os.name == 'posix':
            os.chmod(self.state, 0o700)

    def execute(self, request):
        if not isinstance(request, dict) or not isinstance(request.get('action'), str):
            raise TrainCtlError('INVALID_WORKSPACE_REQUEST', 'workspace request requires an action')
        action = request['action']
        if action == 'inspect':
            return self._inspect(request)
        if action == 'stage':
            return self._stage(request)
        if action == 'diff':
            return self._diff(request)
        if action == 'apply':
            return self._apply(request)
        if action == 'restore':
            return self._restore(request)
        if action == 'changes':
            return self._changes(request)
        if action == 'discard':
            return self._discard(request)
        raise TrainCtlError('INVALID_WORKSPACE_ACTION', 'unsupported workspace action')

    def _project(self, value):
        if not isinstance(value, str) or not Path(value).is_absolute():
            raise TrainCtlError('INVALID_WORKSPACE_PATH', 'project must be an absolute path')
        lexical = Path(value)
        if lexical.is_symlink():
            raise TrainCtlError('WORKSPACE_SYMLINK', 'project symlinks are not permitted')
        project = lexical.resolve()
        if not project.is_dir() or not any(project == root or project.is_relative_to(root) for root in self.roots):
            raise TrainCtlError('WORKSPACE_OUTSIDE_ROOT', 'project is outside configured workspace roots')
        self._reject_symlinks(project, Path('.'))
        return project

    @staticmethod
    def _relative(value):
        if not isinstance(value, str) or not value or '\\' in value:
            raise TrainCtlError('INVALID_WORKSPACE_PATH', 'path must be a POSIX relative path')
        path = PurePosixPath(value)
        if path.is_absolute() or any(part in {'', '.', '..'} for part in path.parts):
            raise TrainCtlError('INVALID_WORKSPACE_PATH', 'path traversal is not permitted')
        return Path(*path.parts)

    @staticmethod
    def _reject_symlinks(project, relative):
        current = project
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise TrainCtlError('WORKSPACE_SYMLINK', 'workspace symlinks are not permitted')
            if not current.exists():
                break

    def _target(self, project, value):
        relative = self._relative(value)
        self._reject_symlinks(project, relative)
        target = project / relative
        if target.exists() and not target.is_file() and not target.is_dir():
            raise TrainCtlError('INVALID_WORKSPACE_PATH', 'unsupported filesystem entry')
        return relative, target

    @staticmethod
    def _protected(relative, target):
        parts = {part.lower() for part in relative.parts}
        return bool(parts & (PROTECTED_PARTS | CONTROL_PARTS) or
                    target.suffix.lower() in PROTECTED_SUFFIXES or
                    target.name.lower() in SENSITIVE_NAMES)

    def _inspect(self, request):
        project = self._project(request.get('project'))
        mode = request.get('mode', 'list')
        relative, target = self._target(project, request.get('path', '.')) if request.get('path') not in (None, '.', '') else (Path('.'), project)
        if mode == 'list':
            if not target.is_dir():
                raise TrainCtlError('WORKSPACE_NOT_DIRECTORY', 'list target is not a directory')
            limit = request.get('limit', 200)
            if type(limit) is not int or not 1 <= limit <= 500:
                raise TrainCtlError('INVALID_WORKSPACE_LIMIT', 'list limit must be 1..500')
            rows = []
            entries = sorted(target.iterdir(), key=lambda path: (not path.is_dir(), path.name.lower()))
            for entry in entries[:limit]:
                rows.append({'path': (relative / entry.name).as_posix(),
                             'kind': 'symlink' if entry.is_symlink() else 'directory' if entry.is_dir() else 'file',
                             'size': entry.stat().st_size if entry.is_file() and not entry.is_symlink() else None})
            return {'action': 'inspect', 'mode': mode, 'project': str(project), 'path': relative.as_posix(),
                    'entries': rows, 'truncated': len(entries) > limit}
        if mode == 'tree':
            if not target.is_dir():
                raise TrainCtlError('WORKSPACE_NOT_DIRECTORY', 'tree target is not a directory')
            depth = request.get('depth', 4)
            limit = request.get('limit', 2000)
            if type(depth) is not int or not 1 <= depth <= 8:
                raise TrainCtlError('INVALID_WORKSPACE_DEPTH', 'tree depth must be 1..8')
            if type(limit) is not int or not 1 <= limit <= 5000:
                raise TrainCtlError('INVALID_WORKSPACE_LIMIT', 'tree limit must be 1..5000')
            if set(part.lower() for part in relative.parts) & CONTROL_PARTS:
                raise TrainCtlError('WORKSPACE_SENSITIVE', 'version-control contents cannot be inventoried')
            rows, limit_reached, depth_limited = [], False, False

            def visit(directory, prefix, level):
                nonlocal limit_reached, depth_limited
                try:
                    entries = sorted(directory.iterdir(), key=lambda path: path.name.lower())
                except OSError:
                    rows.append({'path': prefix.as_posix(), 'kind': 'unreadable', 'size': None,
                                 'restricted': True})
                    return
                for entry in entries:
                    if len(rows) >= limit:
                        limit_reached = True
                        return
                    entry_relative = prefix / entry.name
                    restricted = (entry.name.lower() in SENSITIVE_NAMES or
                                  bool(set(part.lower() for part in entry_relative.parts) & CONTROL_PARTS))
                    try:
                        if entry.is_symlink():
                            kind, size = 'symlink', None
                        elif entry.is_dir():
                            kind, size = 'directory', None
                        elif entry.is_file():
                            kind, size = 'file', entry.stat().st_size
                        else:
                            kind, size = 'unsupported', None
                    except OSError:
                        kind, size, restricted = 'unreadable', None, True
                    row = {'path': entry_relative.as_posix(), 'kind': kind, 'size': size}
                    if restricted:
                        row['restricted'] = True
                    rows.append(row)
                    if kind == 'directory' and not restricted:
                        if level < depth:
                            visit(entry, entry_relative, level + 1)
                            if limit_reached:
                                return
                        else:
                            depth_limited = True

            visit(target, relative, 1)
            return {'action': 'inspect', 'mode': mode, 'project': str(project),
                    'path': relative.as_posix(), 'depth': depth, 'entries': rows,
                    'truncated': limit_reached or depth_limited,
                    'limit_reached': limit_reached, 'depth_limited': depth_limited}
        if mode == 'read':
            if not target.is_file() or target.is_symlink():
                raise TrainCtlError('WORKSPACE_NOT_FILE', 'read target is not a regular file')
            if target.name.lower() in SENSITIVE_NAMES or set(part.lower() for part in relative.parts) & CONTROL_PARTS:
                raise TrainCtlError('WORKSPACE_SENSITIVE', 'sensitive and version-control files cannot be read')
            raw = target.read_bytes()
            if len(raw) > MAX_FILE or b'\0' in raw:
                raise TrainCtlError('WORKSPACE_FILE_UNREADABLE', 'file is binary or exceeds 1 MiB')
            return {'action': 'inspect', 'mode': mode, 'project': str(project), 'path': relative.as_posix(),
                    'sha256': hashlib.sha256(raw).hexdigest(), 'content': raw.decode('utf-8', errors='replace')}
        if mode == 'search':
            query = request.get('query')
            limit = request.get('limit', 100)
            if not isinstance(query, str) or not 1 <= len(query) <= 200 or type(limit) is not int or not 1 <= limit <= 200:
                raise TrainCtlError('INVALID_WORKSPACE_SEARCH', 'search requires a bounded query and limit')
            if not target.is_dir():
                raise TrainCtlError('WORKSPACE_NOT_DIRECTORY', 'search target is not a directory')
            rows, visited = [], 0
            for base, directories, files in os.walk(target, followlinks=False):
                base_path = Path(base)
                directories[:] = [name for name in directories
                                  if not (base_path / name).is_symlink() and name.lower() not in CONTROL_PARTS]
                for name in files:
                    path = base_path / name
                    visited += 1
                    if visited > 10000:
                        break
                    try:
                        rel = path.relative_to(project)
                        if (path.is_symlink() or path.stat().st_size > MAX_FILE or
                                path.name.lower() in SENSITIVE_NAMES or
                                set(part.lower() for part in rel.parts) & CONTROL_PARTS):
                            continue
                        relative_path = rel.as_posix()
                        if query.lower() in relative_path.lower():
                            rows.append({'path': relative_path, 'match': 'name'})
                        else:
                            raw = path.read_bytes()
                            if b'\0' not in raw and query.lower() in raw.decode('utf-8', errors='replace').lower():
                                rows.append({'path': relative_path, 'match': 'content'})
                    except OSError:
                        continue
                    if len(rows) >= limit:
                        break
                if len(rows) >= limit or visited > 10000:
                    break
            return {'action': 'inspect', 'mode': mode, 'project': str(project), 'query': query,
                    'matches': rows, 'truncated': len(rows) >= limit or visited > 10000}
        raise TrainCtlError('INVALID_WORKSPACE_INSPECT_MODE', 'inspect mode must be list, tree, read or search')

    def _stage(self, request):
        project = self._project(request.get('project'))
        operations = request.get('operations')
        if not isinstance(operations, list) or not 1 <= len(operations) <= 128:
            raise TrainCtlError('INVALID_WORKSPACE_CHANGE', 'stage requires 1..128 operations')
        change_id = 'change_' + uuid4().hex
        directory = self.state / change_id
        directory.mkdir(mode=0o700)
        rows, names, total = [], set(), 0
        try:
            for operation in operations:
                if not isinstance(operation, dict) or operation.get('op') not in {'write', 'trash'}:
                    raise TrainCtlError('INVALID_WORKSPACE_CHANGE', 'operation must be write or trash')
                relative, target = self._target(project, operation.get('path'))
                name = relative.as_posix()
                if name in names:
                    raise TrainCtlError('INVALID_WORKSPACE_CHANGE', 'duplicate operation path')
                names.add(name)
                if target.exists() and not target.is_file():
                    raise TrainCtlError('WORKSPACE_NOT_FILE', 'operations only support regular files')
                if target.exists() and target.stat().st_size > MAX_FILE:
                    raise TrainCtlError('WORKSPACE_FILE_TOO_LARGE', 'existing files over 1 MiB cannot be changed')
                if self._protected(relative, target):
                    raise TrainCtlError('WORKSPACE_PROTECTED', 'protected data, model, credential and control paths cannot be changed')
                before = _sha(target)
                expected = operation.get('expected_sha256')
                if expected is not None and expected != before:
                    raise TrainCtlError('WORKSPACE_CHANGED', 'expected file hash does not match current content')
                row = {'op': operation['op'], 'path': name, 'base_sha256': before,
                       'preexisting': target.exists()}
                if operation['op'] == 'write':
                    content = operation.get('content')
                    if not isinstance(content, str):
                        raise TrainCtlError('INVALID_WORKSPACE_CHANGE', 'write requires UTF-8 text content')
                    raw = content.encode('utf-8')
                    total += len(raw)
                    if len(raw) > MAX_FILE or total > MAX_CHANGE:
                        raise TrainCtlError('WORKSPACE_CHANGE_TOO_LARGE', 'workspace change exceeds size budget')
                    staged = directory / 'overlay' / relative
                    _atomic_bytes(staged, raw)
                    row['staged_sha256'] = hashlib.sha256(raw).hexdigest()
                else:
                    if not target.exists():
                        raise TrainCtlError('WORKSPACE_NOT_FILE', 'trash target does not exist')
                rows.append(row)
            manifest = {'schema_version': 1, 'change_id': change_id, 'status': 'staged',
                        'project': str(project), 'created_at': utc_now(), 'operations': rows}
            manifest['plan_sha256'] = _json_sha(manifest)
            _atomic_json(directory / 'change.json', manifest)
            return self._summary(manifest)
        except BaseException:
            shutil.rmtree(directory, ignore_errors=True)
            raise

    def _load(self, change_id):
        if not isinstance(change_id, str) or not CHANGE_ID.fullmatch(change_id):
            raise TrainCtlError('INVALID_CHANGE_ID', 'invalid workspace change id')
        path = self.state / change_id / 'change.json'
        try:
            manifest = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as exc:
            raise TrainCtlError('CHANGE_NOT_FOUND', 'workspace change was not found') from exc
        if manifest.get('change_id') != change_id or manifest.get('schema_version') != 1:
            raise TrainCtlError('INVALID_CHANGE_RECORD', 'workspace change identity is invalid')
        return path.parent, manifest

    def _changes(self, request):
        limit = request.get('limit', 100)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise TrainCtlError('INVALID_WORKSPACE_LIMIT', 'change limit must be 1..200')
        rows = []
        for path in sorted(self.state.glob('change_*/change.json'), reverse=True):
            if not CHANGE_ID.fullmatch(path.parent.name):
                continue
            try:
                _, manifest = self._load(path.parent.name)
                rows.append(self._summary(manifest))
            except TrainCtlError:
                continue
            if len(rows) >= limit:
                break
        return {'action': 'changes', 'changes': rows, 'truncated': len(rows) >= limit}

    @staticmethod
    def _summary(manifest):
        return {key: manifest.get(key) for key in ('schema_version', 'change_id', 'status', 'project',
                                                   'created_at', 'applied_at', 'restored_at', 'discarded_at',
                                                   'plan_sha256', 'applied_sha256')}

    def _discard(self, request):
        directory, manifest = self._load(request.get('change_id'))
        if manifest.get('status') != 'staged' or request.get('plan_sha256') != manifest.get('plan_sha256'):
            raise TrainCtlError('DISCARD_NOT_APPROVED', 'staged change and exact plan_sha256 are required')
        with file_lock(self.state / 'workspace.lock'):
            directory, manifest = self._load(manifest['change_id'])
            if manifest.get('status') != 'staged':
                raise TrainCtlError('CHANGE_NOT_STAGED', 'workspace change is no longer staged')
            overlay = directory / 'overlay'
            if overlay.exists():
                shutil.rmtree(overlay)
            manifest['status'], manifest['discarded_at'] = 'discarded', utc_now()
            _atomic_json(directory / 'change.json', manifest)
            return self._summary(manifest)

    def _diff(self, request):
        directory, manifest = self._load(request.get('change_id'))
        project = self._project(manifest['project'])
        chunks, size, truncated = [], 0, False
        for operation in manifest['operations']:
            relative, target = self._target(project, operation['path'])
            if operation['op'] == 'trash':
                piece = f'--- {relative.as_posix()}\n+++ /trash/{relative.as_posix()}\n'
            else:
                before = target.read_text(encoding='utf-8', errors='replace').splitlines(True) if target.exists() else []
                after = (directory / 'overlay' / relative).read_text(encoding='utf-8', errors='replace').splitlines(True)
                piece = ''.join(difflib.unified_diff(before, after, fromfile='a/' + relative.as_posix(),
                                                     tofile='b/' + relative.as_posix(), n=3))
            encoded = piece.encode('utf-8')
            if size + len(encoded) > 256 * 1024:
                truncated = True
                break
            chunks.append(piece); size += len(encoded)
        return {**self._summary(manifest), 'action': 'diff', 'diff': ''.join(chunks), 'truncated': truncated}

    def _apply(self, request):
        directory, manifest = self._load(request.get('change_id'))
        if manifest.get('status') != 'staged' or request.get('plan_sha256') != manifest.get('plan_sha256'):
            raise TrainCtlError('CHANGE_NOT_APPROVED', 'staged change and exact plan_sha256 are required')
        project = self._project(manifest['project'])
        with file_lock(self.state / 'workspace.lock'):
            directory, manifest = self._load(manifest['change_id'])
            if manifest.get('status') != 'staged':
                raise TrainCtlError('CHANGE_NOT_STAGED', 'workspace change is no longer staged')
            targets = []
            for operation in manifest['operations']:
                relative, target = self._target(project, operation['path'])
                if _sha(target) != operation['base_sha256']:
                    raise TrainCtlError('WORKSPACE_CHANGED', 'project changed after staging; create a new plan')
                targets.append((operation, relative, target))
            completed = []
            try:
                for operation, relative, target in targets:
                    backup = directory / 'backup' / relative
                    if operation['preexisting']:
                        backup.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(target, backup)
                    if operation['op'] == 'write':
                        staged = directory / 'overlay' / relative
                        mode = target.stat().st_mode & 0o777 if target.exists() else 0o644
                        _atomic_bytes(target, staged.read_bytes(), mode)
                        operation['applied_mode'] = mode
                    else:
                        target.unlink()
                    operation['applied_sha256'] = _sha(target)
                    completed.append((operation, relative, target))
            except BaseException:
                self._rollback(directory, completed)
                raise
            manifest['status'], manifest['applied_at'] = 'applied', utc_now()
            manifest['applied_sha256'] = _json_sha({'plan_sha256': manifest['plan_sha256'],
                'files': [(row['path'], row.get('applied_sha256')) for row in manifest['operations']]})
            _atomic_json(directory / 'change.json', manifest)
            return self._summary(manifest)

    @staticmethod
    def _rollback(directory, completed):
        for operation, relative, target in reversed(completed):
            backup = directory / 'backup' / relative
            if operation['preexisting'] and backup.exists():
                _atomic_bytes(target, backup.read_bytes(), backup.stat().st_mode & 0o777)
            else:
                target.unlink(missing_ok=True)

    def _restore(self, request):
        directory, manifest = self._load(request.get('change_id'))
        if manifest.get('status') != 'applied' or request.get('applied_sha256') != manifest.get('applied_sha256'):
            raise TrainCtlError('RESTORE_NOT_APPROVED', 'applied change and exact applied_sha256 are required')
        project = self._project(manifest['project'])
        with file_lock(self.state / 'workspace.lock'):
            directory, manifest = self._load(manifest['change_id'])
            if manifest.get('status') != 'applied':
                raise TrainCtlError('CHANGE_NOT_APPLIED', 'workspace change is not applied')
            targets = []
            for operation in manifest['operations']:
                relative, target = self._target(project, operation['path'])
                if _sha(target) != operation.get('applied_sha256'):
                    raise TrainCtlError('WORKSPACE_CHANGED', 'project changed after apply; refusing to overwrite it')
                targets.append((operation, relative, target))
            completed = []
            try:
                for operation, relative, target in targets:
                    backup = directory / 'backup' / relative
                    if operation['preexisting']:
                        _atomic_bytes(target, backup.read_bytes(), backup.stat().st_mode & 0o777)
                    else:
                        target.unlink(missing_ok=True)
                    completed.append((operation, relative, target))
            except BaseException:
                # Return already-restored paths to their approved applied state.
                for operation, relative, target in reversed(completed):
                    if operation['op'] == 'write':
                        staged = directory / 'overlay' / relative
                        _atomic_bytes(target, staged.read_bytes(), operation.get('applied_mode', 0o644))
                    else:
                        target.unlink(missing_ok=True)
                raise
            manifest['status'], manifest['restored_at'] = 'restored', utc_now()
            _atomic_json(directory / 'change.json', manifest)
            return self._summary(manifest)
