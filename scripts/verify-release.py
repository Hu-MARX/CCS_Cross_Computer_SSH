"""Build and test standalone wheel/sdist installs. No SSH or hardware work."""

from __future__ import annotations

import argparse
from email.parser import Parser
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import zipfile


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_RESOURCE = 'ccs/resources/action-contracts.json'
REQUIRED_SOURCE_FILES = (
    'pyproject.toml', 'MANIFEST.in', 'scripts/verify.py', 'scripts/verify-local.ps1',
    'scripts/verify-release.py', 'schemas/ccs-request.schema.json',
    'schemas/ccs-result.schema.json', 'schemas/experiment-spec.schema.json',
    'docs/RELEASING.md', 'docs/SUPPORT-MATRIX.md', 'examples/agent-smoke-spec.json',
    'tests/test_verification_groups.py', 'tests/test_researchflow.py',
    'tests/fixtures/researchflow/research-project.json',
    'tests/fixtures/researchflow/config/literature-profile.yaml',
    'tests/research_integration/test_online_joint_registration.py',
    'src/' + REQUIRED_RESOURCE,
)

# Copied into unrelated cwd and executed by each installed interpreter. Every
# gateway child is patched to FakeRemoteRunner, so no SSH command can be made.
INSTALLED_SMOKE = r'''
import asyncio
import copy
from importlib import metadata, resources
import json
import os
from pathlib import Path
import subprocess
import sys

import ccs
import controller
from ccs import CCSClient, CCSPlan, CCSReadCursor

expected_version, with_mcp, forbidden_root = sys.argv[1], sys.argv[2] == 'yes', Path(sys.argv[3]).resolve()
package_path = Path(ccs.__file__).resolve()
assert forbidden_root not in package_path.parents, package_path
assert Path(sys.prefix).resolve() in package_path.parents, (sys.prefix, package_path)
assert metadata.version('cross-computer-ssh') == expected_version
assert controller.__version__ == expected_version
assert 'PYTHONPATH' not in os.environ and 'PYTHONHOME' not in os.environ
resource = resources.files('ccs').joinpath('resources', 'action-contracts.json')
assert isinstance(json.loads(resource.read_text(encoding='utf-8')), dict)
bin_dir = Path(sys.executable).parent
entrypoint = bin_dir / ('ccsctl.exe' if os.name == 'nt' else 'ccsctl')
assert entrypoint.is_file(), entrypoint
before = set(Path.cwd().rglob('*'))
for arguments in (['describe'], ['describe', '--action', 'run.submit'],
                  ['describe', '--schema', 'request'], ['describe', '--schema', 'result'],
                  ['describe', '--schema', 'experiment-spec']):
    result = subprocess.run([str(entrypoint), *arguments], capture_output=True, check=True)
    envelope = json.loads(result.stdout.decode('utf-8'))
    assert envelope['ok'] is True and envelope['error'] is None, envelope
    assert isinstance(envelope['data'], dict) and envelope['data'], envelope
offline = CCSClient([str(entrypoint)], cwd=Path.cwd())
assert isinstance(offline.describe(), dict)
assert isinstance(offline.describe('run.submit'), dict)
assert before == set(Path.cwd().rglob('*')), 'offline description created runtime state'
doctor_args = [str(entrypoint), 'doctor', '--python', sys.executable, '--cwd', str(Path.cwd())]
if with_mcp:
    doctor_args.append('--require-mcp')
diagnostic = json.loads(subprocess.run(doctor_args, capture_output=True, check=True).stdout)
assert diagnostic['data']['ok'] and diagnostic['data']['remote_probe'] == 'not_requested', diagnostic
assert before == set(Path.cwd().rglob('*')), 'offline doctor created runtime state'
runtime = Path.cwd() / 'runtime'
runtime.mkdir()
fake_gateway = ('from controller import gateway_cli; '
                'from controller.remote import FakeRemoteRunner; '
                'gateway_cli.OpenSSHRunner = lambda *args, **kwargs: FakeRemoteRunner(); '
                'raise SystemExit(gateway_cli.main())')
command = [sys.executable, '-c', fake_gateway, '--host', 'fake-worker',
           '--db', str(runtime / 'runs.db'), '--audit-db', str(runtime / 'audit.db'),
           '--replica-root', str(runtime / 'replicas')]
sdk = CCSClient(command, cwd=runtime, actor_id='release-smoke', actor_kind='agent')
spec = {'schema_version': 1, 'name': 'local-release-smoke', 'project': {}, 'runtime': {},
        'command': {'argv': ['python3', '-c', 'print("local fixture")']},
        'resources': {'max_duration_seconds': 30}, 'task': {'kind': 'analysis', 'inputs': []}}
payload = {'worker_id': 'fake-worker', 'idempotency_key': 'release-sdk-smoke', 'spec': spec}
health = sdk.request('worker.health', {}, dry_run=False)
assert health['executed'] is True
plan = sdk.plan('run.submit', payload, request_id='release-sdk-plan')
assert plan.data['executed'] is False
plan.save(runtime / 'plan.json')
restored = CCSPlan.load(runtime / 'plan.json')
assert restored.request == plan.request and restored.plan_sha256 == plan.plan_sha256
confirmed = sdk.confirm(restored)
assert confirmed['executed'] is True and confirmed['result']['status'] == 'QUEUED', confirmed
config_arguments = [str(entrypoint), 'mcp-config', '--python', sys.executable,
    '--command-json', json.dumps(command), '--cwd', str(runtime), '--output', str(runtime / 'mcp-new.json')]
preview = json.loads(subprocess.run(config_arguments, capture_output=True, check=True).stdout)['data']
assert not (runtime / 'mcp-new.json').exists() and '--read-only' in preview['content']['mcpServers']['ccs']['args']
created = json.loads(subprocess.run([*config_arguments, '--write', '--confirm-sha256', preview['plan_sha256']],
                                  capture_output=True, check=True).stdout)['data']
assert created['written'] and (runtime / 'mcp-new.json').is_file()
seed_gateway = ('from controller import gateway_cli; '
                'from controller.remote import FakeRemoteRunner; '
                'from controller.domain import ExperimentSpec,RunStatus; '
                'r=FakeRemoteRunner(); rid=r.submit(ExperimentSpec.from_dict(' + repr(spec) + '),"seed"); '
                'r.advance(rid,RunStatus.SUCCEEDED); '
                'gateway_cli.OpenSSHRunner=lambda *args,**kwargs:r; '
                'raise SystemExit(gateway_cli.main())')
seed_command = list(command)
seed_command[2] = seed_gateway
seeded = CCSClient(seed_command, cwd=runtime)
cursor = CCSReadCursor('run.events', 'remote_0001', worker_id='fake-worker')
events = list(seeded.iter_events('remote_0001', worker_id='fake-worker', cursor=cursor,
                               total_timeout=15, limit=1))
assert events and cursor.value == events[-1]['seq'], (events, cursor.to_dict())
assert seeded.wait_run('remote_0001', worker_id='fake-worker', total_timeout=15)['status'] == 'SUCCEEDED'
assert list(seeded.iter_logs('remote_0001', worker_id='fake-worker', total_timeout=15)) == []

async def mcp_smoke():
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters
    mcp_entrypoint = bin_dir / ('ccs-mcp.exe' if os.name == 'nt' else 'ccs-mcp')
    assert mcp_entrypoint.is_file(), mcp_entrypoint
    mcp_command = list(command)
    mcp_command[mcp_command.index('--db') + 1] = str(runtime / 'mcp-runs.db')
    mcp_command[mcp_command.index('--audit-db') + 1] = str(runtime / 'mcp-audit.db')
    parameters = StdioServerParameters(command=str(mcp_entrypoint),
        args=['--command-json', json.dumps(mcp_command), '--cwd', str(runtime), '--actor-id', 'release-mcp'],
        cwd=Path.cwd(), env=dict(os.environ))
    async with Client(parameters, read_timeout_seconds=30) as client:
        tools = {tool.name for tool in (await client.list_tools()).tools}
        assert tools == {'ccs_read', 'ccs_plan', 'ccs_execute'}, tools
        described = await client.call_tool('ccs_read', {'action': 'gateway.describe', 'payload': {}})
        assert not described.is_error, described
        mcp_payload = dict(payload, idempotency_key='release-mcp-smoke')
        response = await client.call_tool('ccs_plan',
            {'action': 'run.submit', 'payload': mcp_payload, 'request_id': 'release-mcp-plan'})
        assert not response.is_error, response
        prepared = response.structured_content['data']
        arguments = {key: prepared['request'][key] for key in ('action', 'payload', 'request_id')}
        arguments['plan_sha256'] = prepared['plan']['plan_sha256']
        tampered = copy.deepcopy(arguments)
        tampered['payload']['spec']['name'] = 'tampered'
        rejected = await client.call_tool('ccs_execute', tampered)
        assert rejected.is_error and rejected.structured_content['error']['code'] == 'CONFIRMATION_REQUIRED', rejected
        executed = await client.call_tool('ccs_execute', arguments)
        assert not executed.is_error and executed.structured_content['data']['executed'] is True, executed
        assert executed.structured_content['data']['result']['status'] == 'QUEUED', executed
    parameters.args += ['--read-only']
    async with Client(parameters, read_timeout_seconds=30) as client:
        assert {tool.name for tool in (await client.list_tools()).tools} == {'ccs_read'}
        blocked = await client.call_tool('ccs_read', {'action': 'workspace.execute',
            'payload': {'request': {'action': 'stage'}}})
        assert blocked.is_error, blocked
        described = await client.call_tool('ccs_read', {'action': 'gateway.describe', 'payload': {}})
        assert not described.is_error, described

if with_mcp:
    asyncio.run(mcp_smoke())
print(json.dumps({'status': 'passed', 'version': expected_version, 'python': sys.version,
                 'platform': sys.platform, 'package_path': str(package_path),
                 'cli': 'passed', 'sdk': 'passed', 'resource': 'passed',
                 'p1_helpers_doctor_config': 'passed',
                 'mcp_read_only': 'passed' if with_mcp else 'not-requested',
                 'mcp_stdio_confirmation': 'passed' if with_mcp else 'not-requested',
                 'mcp_version': metadata.version('mcp') if with_mcp else None}))
'''


def clean_environment(temp_root: Path) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items()
           if key.upper() not in {'PYTHONPATH', 'PYTHONHOME', 'CCS_RESEARCH_ROOT'}}
    env['TEMP'] = env['TMP'] = env['TMPDIR'] = str(temp_root)
    env['PYTHONNOUSERSITE'] = '1'
    env['PIP_DISABLE_PIP_VERSION_CHECK'] = '1'
    return env


def archive_inventory(wheel: Path, sdist: Path) -> dict:
    with zipfile.ZipFile(wheel) as archive:
        wheel_files = set(archive.namelist())
        if REQUIRED_RESOURCE not in wheel_files:
            raise ValueError('Wheel is missing installed contract resource: ' + REQUIRED_RESOURCE)
        metadata_names = [name for name in wheel_files if name.endswith('.dist-info/METADATA')]
        if len(metadata_names) != 1:
            raise ValueError('Wheel must contain exactly one METADATA file')
        metadata = Parser().parsestr(archive.read(metadata_names[0]).decode('utf-8'))
        core_requires = [requirement for requirement in metadata.get_all('Requires-Dist', [])
                         if 'extra ==' not in requirement]
        if core_requires:
            raise ValueError('Core wheel unexpectedly requires third-party packages: ' + str(core_requires))
    with tarfile.open(sdist, 'r:gz') as archive:
        names = archive.getnames()
        source_files = {'/'.join(name.split('/')[1:]) for name in names}
        missing = sorted(set(REQUIRED_SOURCE_FILES) - source_files)
        if missing:
            raise ValueError('Sdist is missing independent verification inputs: ' + ', '.join(missing))
    return {'version': metadata['Version'], 'wheel_files': sorted(wheel_files),
            'sdist_files': sorted(source_files), 'core_requires_dist': core_requires}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, help='New evidence directory outside the CCS source tree')
    parser.add_argument('--mcp-version', help='Exact optional MCP SDK version to install and test (e.g. 2.2.0)')
    parser.add_argument('--skip-build', action='store_true', help='Reuse --artifacts-dir wheels/sdist for another dependency variant')
    parser.add_argument('--artifacts-dir', help='Existing artifact directory required with --skip-build')
    parser.add_argument('--require-linux', action='store_true')
    args = parser.parse_args(argv)
    output = Path(args.output_dir).resolve()
    created_output = False
    report = {'status': 'failed', 'platform': sys.platform, 'python': sys.version,
              'mcp_requested_version': args.mcp_version, 'installs': [], 'commands': [],
              'linux_acceptance': 'pending' if not sys.platform.startswith('linux') else 'installed-smoke-only'}
    try:
        if output == ROOT or ROOT in output.parents:
            raise ValueError('Release evidence must be outside the CCS source tree')
        if output.exists():
            raise ValueError('Use a fresh output directory to avoid mixing release evidence: ' + str(output))
        if args.require_linux and not sys.platform.startswith('linux'):
            raise ValueError('--require-linux requires actual Linux; Windows skips do not qualify')
        if args.skip_build and not args.artifacts_dir:
            raise ValueError('--skip-build requires explicit --artifacts-dir')
        if not args.skip_build and args.artifacts_dir:
            raise ValueError('--artifacts-dir is only used with --skip-build')
        output.mkdir(parents=True)
        created_output = True
        temporary = output / 'temp'
        temporary.mkdir()
        env = clean_environment(temporary)
        logs = output / 'logs'
        logs.mkdir()

        def run(command: list[str], cwd: Path, name: str) -> str:
            result = subprocess.run(command, cwd=cwd, env=env, capture_output=True)
            (logs / (name + '.stdout.log')).write_bytes(result.stdout)
            (logs / (name + '.stderr.log')).write_bytes(result.stderr)
            report['commands'].append({'name': name, 'argv': command, 'cwd': str(cwd), 'exit_code': result.returncode})
            if result.returncode:
                raise RuntimeError(f'{name} failed ({result.returncode}); see {logs / (name + ".stderr.log")}')
            return result.stdout.decode('utf-8', errors='replace')

        if args.skip_build:
            artifacts = Path(args.artifacts_dir).resolve()
        else:
            if importlib.util.find_spec('build') is None:
                raise ValueError('Release building requires developer tool build: python -m pip install build')
            artifacts = output / 'artifacts'
            run([sys.executable, '-m', 'build', '--wheel', '--sdist', '--outdir', str(artifacts), str(ROOT)],
                output, 'build')
        wheels, sdists = list(artifacts.glob('*.whl')), list(artifacts.glob('*.tar.gz'))
        if len(wheels) != 1 or len(sdists) != 1:
            raise ValueError('Artifact directory must contain one wheel and one sdist')
        wheel, sdist = wheels[0].resolve(), sdists[0].resolve()
        inventory = archive_inventory(wheel, sdist)
        report['version'] = inventory['version']
        (output / 'inventory.json').write_text(json.dumps(inventory, indent=2) + '\n', encoding='utf-8')
        report['artifacts'] = [{'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
                               for path in (wheel, sdist)]
        for kind, artifact in (('wheel', wheel), ('sdist', sdist)):
            print('Release acceptance: ' + kind, flush=True)
            environment = output / (kind + '-venv')
            run([sys.executable, '-m', 'venv', str(environment)], output, kind + '-create-venv')
            interpreter = environment / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
            unrelated = output / (kind + '-unrelated-cwd')
            unrelated.mkdir()
            run([str(interpreter), '-m', 'pip', 'install', '--no-deps', str(artifact)], unrelated, kind + '-install')
            if args.mcp_version:
                run([str(interpreter), '-m', 'pip', 'install', 'mcp==' + args.mcp_version], unrelated,
                    kind + '-mcp-install')
            smoke = unrelated / 'installed_smoke.py'
            smoke.write_text(INSTALLED_SMOKE, encoding='utf-8')
            stdout = run([str(interpreter), str(smoke), inventory['version'],
                          'yes' if args.mcp_version else 'no', str(ROOT)], unrelated, kind + '-smoke')
            result = json.loads(stdout.strip().splitlines()[-1])
            if args.mcp_version and result['mcp_version'] != args.mcp_version:
                raise ValueError('Installed MCP does not match requested exact version')
            report['installs'].append({'kind': kind, **result})
        report['status'] = 'passed'
    except (ValueError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        report['error'] = str(exc)
        print('ERROR: ' + str(exc), file=sys.stderr)
    finally:
        # A rejected existing path must not be altered just to write a report.
        if created_output:
            (output / 'release-summary.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, ensure_ascii=True, separators=(',', ':')))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
