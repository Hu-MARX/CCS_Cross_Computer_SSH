"""Offline installation/configuration diagnostics; never run configured commands."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from .process import ProcessStopped, run_bounded


class DiagnosticError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


def command_value(raw):
    try:
        value = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise DiagnosticError('INVALID_COMMAND', '--command-json must be a JSON argv array') from exc
    if (not isinstance(value, list) or not value or len(value) > 128 or
            any(not isinstance(item, str) or not item or '\x00' in item for item in value)):
        raise DiagnosticError('INVALID_COMMAND', 'command requires 1-128 nonempty argv strings')
    return value


def absolute_executable(value, cwd):
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate.absolute()
    if candidate.parent != Path('.'):
        return (cwd / candidate).absolute()
    found = shutil.which(value)
    if found:
        return Path(found).absolute()
    raise DiagnosticError('PROGRAM_NOT_FOUND', 'program was not found; specify an absolute executable path')


def _probe_python(executable):
    # Fixed local metadata probe, unrelated to worker configuration or task code.
    code = ('import sys,json,importlib.metadata as m,importlib.util as u; '
            'print(json.dumps({"python":list(sys.version_info[:3]),'
            '"ccs_available":u.find_spec("ccs") is not None,'
            '"mcp_version":m.version("mcp") if u.find_spec("mcp") else None}))')
    try:
        result = run_bounded([str(executable), '-I', '-c', code], timeout=8,
                             max_stdout_bytes=8192, max_stderr_bytes=8192)
        if result.returncode != 0 or len(result.stdout) > 8192:
            raise ValueError('probe failed')
        return json.loads(result.stdout)
    except (OSError, ValueError, ProcessStopped) as exc:
        raise DiagnosticError('PYTHON_PROBE_FAILED', 'interpreter metadata probe failed; select a Python 3.11+ executable') from exc


def doctor(*, command=None, cwd=None, python=None, mcp_config=None, require_mcp=False):
    """Return actionable local facts, without opening keys or invoking gateways."""
    checks = []
    directory = Path(cwd or Path.cwd()).absolute()

    def check(name, ok, code, message):
        checks.append({'name': name, 'ok': bool(ok), 'code': 'OK' if ok else code,
                       'message': message})

    check('cwd', directory.is_dir(), 'CWD_NOT_FOUND',
          'working directory exists' if directory.is_dir() else 'working directory does not exist; use an absolute --cwd')
    resolved_command = None
    if command is not None:
        try:
            argv = command_value(command) if isinstance(command, str) else command_value(json.dumps(command))
            executable = absolute_executable(argv[0], directory)
            check('gateway_program', executable.is_file(), 'PROGRAM_NOT_FOUND',
                  'gateway executable exists' if executable.is_file() else 'gateway executable does not exist')
            resolved_command = [str(executable), *argv[1:]]
            for argument in argv[1:]:
                if argument.lower().endswith(('.py', '.ps1')):
                    path = Path(argument)
                    if not path.is_absolute():
                        path = directory / path
                    check('entry_file', path.is_file(), 'ENTRY_FILE_NOT_FOUND',
                          'entry script exists' if path.is_file() else 'entry script is absent under --cwd; use an absolute entry path')
            for index, argument in enumerate(argv[:-1]):
                if argument in {'--fleet', '--known-hosts', '--identity-file'}:
                    path = Path(argv[index + 1])
                    if not path.is_absolute():
                        path = directory / path
                    check(argument[2:] + '_path', path.is_file(), 'CONFIG_FILE_NOT_FOUND',
                          'referenced file exists' if path.is_file() else 'referenced file is absent under --cwd; use an absolute path')
                    if argument == '--fleet' and path.is_file():
                        try:
                            if path.stat().st_size > 1024 * 1024:
                                raise ValueError('too large')
                            value = json.loads(path.read_text(encoding='utf-8'))
                            from controller.fleet import directory_from_config
                            from controller.remote import OpenSSHRunner
                            # Constructor validation only: no SSH/read of credentials.
                            directory_from_config(value, OpenSSHRunner)
                            check('fleet_structure', True, '', 'fleet configuration is valid')
                        except (OSError, ValueError, TypeError):
                            check('fleet_structure', False, 'INVALID_FLEET_CONFIG', 'fleet configuration is not a valid bounded worker-pool JSON')
        except DiagnosticError as exc:
            check('gateway_program', False, exc.code, exc.message)
    metadata = None
    try:
        interpreter = absolute_executable(python or sys.executable, directory)
        metadata = _probe_python(interpreter)
        supported = tuple(metadata.get('python', [])) >= (3, 11)
        check('python', supported, 'PYTHON_VERSION_UNSUPPORTED',
              'Python 3.11+ available' if supported else 'CCS requires Python 3.11 or newer')
        needs_package = require_mcp or (resolved_command is not None and any(
            resolved_command[index:index + 2] == ['-m', module]
            for index in range(len(resolved_command) - 1)
            for module in ('controller.gateway_cli', 'ccs.mcp_server')))
        if needs_package:
            check('ccs_package', metadata.get('ccs_available'), 'CCS_PACKAGE_NOT_INSTALLED',
                  'CCS package available' if metadata.get('ccs_available') else 'install CCS into the selected interpreter before using the module launcher')
        if require_mcp:
            version = metadata.get('mcp_version')
            try:
                parts = tuple(int(part) for part in version.split('.')[:2])
                compatible = (2, 2) <= parts < (3, 0)
            except (AttributeError, ValueError):
                compatible = False
            check('mcp_dependency', compatible, 'MCP_DEPENDENCY_INCOMPATIBLE',
                  'MCP >=2.2,<3 available' if compatible else 'install cross-computer-ssh[mcp] with this interpreter; required mcp>=2.2,<3')
    except DiagnosticError as exc:
        check('python', False, exc.code, exc.message)
    if mcp_config:
        try:
            path = Path(mcp_config)
            if path.stat().st_size > 1024 * 1024:
                raise ValueError('too large')
            config = json.loads(path.read_text(encoding='utf-8'))
            servers = config.get('mcpServers') if isinstance(config, dict) else None
            valid = (isinstance(servers, dict) and 1 <= len(servers) <= 64 and
                     all(isinstance(server, dict) and isinstance(server.get('command'), str) and
                         isinstance(server.get('args', []), list) and
                         all(isinstance(item, str) for item in server.get('args', []))
                         for server in servers.values()))
            check('mcp_config', valid, 'INVALID_MCP_CONFIG',
                  'MCP configuration structure is valid' if valid else 'expected mcpServers mapping with command string and args array')
            if valid:
                for server in servers.values():
                    executable = absolute_executable(server['command'], directory)
                    check('mcp_launcher', executable.is_file(), 'PROGRAM_NOT_FOUND',
                          'MCP launcher exists' if executable.is_file() else 'MCP launcher does not exist')
                    args = server.get('args', [])
                    is_ccs = any(args[index:index + 2] == ['-m', 'ccs.mcp_server']
                                 for index in range(len(args) - 1)) or executable.stem == 'ccs-mcp'
                    if is_ccs:
                        def option(name, required=False):
                            if name not in args:
                                if required:
                                    raise DiagnosticError('INVALID_MCP_CONFIG', 'CCS launcher requires ' + name)
                                return None
                            index = args.index(name) + 1
                            if index >= len(args):
                                raise DiagnosticError('INVALID_MCP_CONFIG', name + ' requires a value')
                            return args[index]
                        configured_cwd = option('--cwd') or str(directory)
                        if not Path(configured_cwd).is_absolute():
                            configured_cwd = str(directory / configured_cwd)
                        gateway = option('--command-json', required=True)
                        # Reuse only local checks; this recursion has no config or remote probe.
                        nested = doctor(command=gateway, cwd=configured_cwd,
                                        python=python or (str(executable) if '-m' in args else sys.executable),
                                        require_mcp=True)
                        checks.extend({**item, 'name': 'configured_' + item['name']} for item in nested['checks'])
        except (OSError, ValueError) as exc:
            check('mcp_config', False, getattr(exc, 'code', 'INVALID_MCP_CONFIG'),
                  getattr(exc, 'message', 'configuration cannot be read as bounded UTF-8 JSON'))
    return {'schema_version': 1, 'ok': all(item['ok'] for item in checks), 'checks': checks,
            'cwd': str(directory), 'resolved_command': resolved_command,
            'interpreter': metadata, 'remote_probe': 'not_requested'}


def configuration_plan(*, command, cwd, python=None, output=None, name='ccs', read_only=True):
    directory = Path(cwd).absolute()
    if not directory.is_dir():
        raise DiagnosticError('CWD_NOT_FOUND', 'configuration requires an existing --cwd')
    argv = command_value(command) if isinstance(command, str) else command_value(json.dumps(command))
    executable = absolute_executable(argv[0], directory)
    if not executable.is_file():
        raise DiagnosticError('PROGRAM_NOT_FOUND', 'gateway executable does not exist')
    interpreter = absolute_executable(python or sys.executable, directory)
    if not interpreter.is_file():
        raise DiagnosticError('PROGRAM_NOT_FOUND', 'Python executable does not exist')
    if not isinstance(name, str) or not name.strip() or len(name) > 64:
        raise DiagnosticError('INVALID_CONFIG_NAME', 'server name must contain 1-64 characters')
    argv[0] = str(executable)
    # Resolve entry/config paths while preserving shell-free argv and UTF-8.
    path_options = {'--fleet', '--db', '--audit-db', '--replica-root', '--known-hosts', '--identity-file', '-File'}
    for index in range(1, len(argv)):
        value = argv[index]
        if ((argv[index - 1] in path_options and value != '-') or
                value.lower().endswith(('.py', '.ps1'))):
            argv[index] = str((directory / value).absolute()) if not Path(value).is_absolute() else value
    args = ['-m', 'ccs.mcp_server', '--command-json', json.dumps(argv, ensure_ascii=False), '--cwd', str(directory)]
    if read_only:
        args.append('--read-only')
    content = {'mcpServers': {name: {'command': str(interpreter), 'args': args}}}
    target = str(Path(output).absolute()) if output else None
    body = {'target': target, 'content': content}
    digest = hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True,
                                      separators=(',', ':')).encode('utf-8')).hexdigest()
    return {**body, 'plan_sha256': digest, 'written': False,
            'next_step': 'review target and content; then use --write --confirm-sha256 with this digest'}


def write_configuration(plan, confirmed_sha256):
    body = {'target': plan['target'], 'content': plan['content']}
    actual = hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True,
                                      separators=(',', ':')).encode('utf-8')).hexdigest()
    if not plan['target'] or confirmed_sha256 != plan['plan_sha256'] or actual != confirmed_sha256:
        raise DiagnosticError('CONFIG_CONFIRMATION_REQUIRED', 'write requires --output and the exact preview --confirm-sha256')
    target = Path(plan['target'])
    try:
        with target.open('x', encoding='utf-8', newline='\n') as stream:
            stream.write(json.dumps(plan['content'], ensure_ascii=False, indent=2) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise DiagnosticError('CONFIG_ALREADY_EXISTS', 'configuration already exists; choose a new output path') from exc
    return {**plan, 'written': True}
