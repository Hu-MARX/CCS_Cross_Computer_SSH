"""Loopback-only, capability-protected read-only HTTP UI; no remote mutations."""
import argparse
import hmac
import json
import re
import secrets
import os
import hashlib
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

from controller.remote import OpenSSHRunner
from controller.fleet import directory_from_config
from .feed import atomic_json
from .catalog import Catalog
from controller.repository import RunRepository
from controller.orchestrator import Orchestrator
from controller.workflow import WorkflowEngine
from controller.results import descriptor
from controller import __version__ as LOADED_VERSION

ASSETS = Path(__file__).with_name('web')


class RuntimeIdentity:
    """Startup source identity + bounded disk drift detection, never hot reload.

    Only package source/assets are read; no runtime database, fleet file,
    credential, task payload or path is included in the browser projection.
    The digest identifies source at startup, not a proof of Python bytecode.
    """
    def __init__(self, root=None, *, version=LOADED_VERSION, interval=30):
        self.root = Path(root or ASSETS.parent.parent).resolve()
        self.interval, self.last_check = interval, float('-inf')
        self.lock = threading.Lock()
        self.state = {'loaded_version': version, 'started_at': self._now(),
                      'loaded_source_sha256': None, 'disk_source_sha256': None,
                      'restart_required': False, 'source_state': 'unavailable',
                      'checked_at': None}
        self.poll()
        self.state['loaded_source_sha256'] = self.state['disk_source_sha256']

    @staticmethod
    def _now():
        from .feed import now
        return now()

    def _digest(self):
        digest, count, total = hashlib.sha256(), 0, 0
        directories = 0
        for folder, children, names in os.walk(self.root, followlinks=False):
            children[:] = sorted(name for name in children if not name.startswith('.') and
                name not in {'__pycache__', 'data', 'runtime', 'private', 'config', 'releases', 'snapshots'} and
                not (Path(folder) / name).is_symlink())
            directories += 1
            if directories > 128:
                raise ValueError('Source identity directory budget exceeded')
            for name in sorted(names):
                path = Path(folder) / name
                if path.suffix not in {'.py', '.js', '.css', '.html'}:
                    continue
                if path.is_symlink() or not path.resolve().is_relative_to(self.root):
                    raise ValueError('Source identity cannot follow links')
                count += 1
                size = path.stat().st_size
                total += size
                if count > 512 or size > 4 * 1024 * 1024 or total > 16 * 1024 * 1024:
                    raise ValueError('Source identity budget exceeded')
                digest.update(path.relative_to(self.root).as_posix().encode())
                digest.update(b'\0')
                digest.update(hashlib.sha256(path.read_bytes()).digest())
        return digest.hexdigest()

    def poll(self):
        with self.lock:
            if time.monotonic() - self.last_check < self.interval:
                return
            self.last_check = time.monotonic()
            try:
                fingerprint = self._digest()
                self.state.update(disk_source_sha256=fingerprint, source_state='available',
                    restart_required=bool(self.state['loaded_source_sha256'] and
                                          fingerprint != self.state['loaded_source_sha256']))
            except (OSError, ValueError):
                self.state['source_state'] = 'unavailable'
            self.state['checked_at'] = self._now()

    def view(self):
        with self.lock:
            return dict(self.state)


def select_primary(records, run_id=None):
    """Select a display target, never an execution worker or a fabricated Run.

    Recorded lifecycle is only a startup preference; live views still require
    fresh observations before counting a Run as active.
    """
    if run_id is not None:
        matches = [item for item in records if item['run_id'] == run_id]
        if len(matches) != 1:
            raise ValueError('Requested dashboard Run is not uniquely registered')
        return matches[0]
    if not records:
        raise ValueError('No registered Runs; select a registered research Run first')
    pending = {'RUNNING', 'EVALUATING', 'QUEUED', 'PREPARING'}
    candidates = [item for item in records
                  if item.get('kind') == 'train' and item.get('recorded_status') in pending]
    return max(candidates or records, key=lambda item: (item.get('created_at') or '', item['run_id']))


def private_token(runtime, run_id, port):
    """Keep the user's local tab working across restarts, not across other runs."""
    try:
        record = json.loads(runtime.read_text(encoding='utf-8'))
        parsed = urlsplit(record['url'])
        if (record.get('run_id') == run_id and parsed.scheme == 'http' and parsed.hostname == '127.0.0.1'
                and parsed.port == port and re.fullmatch(r'[A-Za-z0-9_-]{32,64}', parsed.fragment)):
            return parsed.fragment
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return secrets.token_urlsafe(32)


def server(feed, port, token, runtime=None):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # No request URLs, auth headers or tokens in access logs.

        def reply(self, code, body, content_type='application/json; charset=utf-8'):
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Referrer-Policy', 'no-referrer')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
            self.end_headers()
            self.wfile.write(body)

        def authorized(self, *, preference=False):
            hosts = {f'127.0.0.1:{self.server.server_port}', f'localhost:{self.server.server_port}'}
            origin = self.headers.get('Origin')
            site = self.headers.get('Sec-Fetch-Site')
            if (self.headers.get('Host') not in hosts or site == 'cross-site' or
                    (origin and origin not in {'http://' + host for host in hosts}) or
                    (preference and not origin and site != 'same-origin')):
                self.reply(403, b'{"error":"Forbidden origin"}')
                return False
            if not hmac.compare_digest(self.headers.get('Authorization', '').encode(),
                                       ('Bearer ' + token).encode()):
                self.reply(401, b'{"error":"Open the dashboard using its private launch link"}')
                return False
            return True

        def do_GET(self):
            hosts = {f'127.0.0.1:{self.server.server_port}', f'localhost:{self.server.server_port}'}
            if self.headers.get('Host') not in hosts:
                return self.reply(403, b'{"error":"Forbidden origin"}')
            origin = self.headers.get('Origin')
            path = urlsplit(self.path).path
            if path in ('/api/state', '/api/result'):
                if (self.headers.get('Sec-Fetch-Site') == 'cross-site' or
                    (origin and origin not in {'http://' + host for host in hosts})):
                    return self.reply(403, b'{"error":"Forbidden origin"}')
                if not hmac.compare_digest(self.headers.get('Authorization', '').encode(), ('Bearer ' + token).encode()):
                    return self.reply(401, b'{"error":"Open the dashboard using its private launch link"}')
                query = parse_qs(urlsplit(self.path).query)
                if set(query) - {'run_id'} or len(query.get('run_id', [])) > 1:
                    return self.reply(400, b'{"error":"Invalid query"}')
                run_id = query.get('run_id', [None])[0]
                try:
                    if isinstance(feed, Catalog):
                        view = feed.view(run_id)
                    else:
                        view = feed.view()
                        if run_id is not None and run_id != view['run_id']:
                            raise KeyError(run_id)
                except KeyError:
                    return self.reply(404, b'{"error":"Unknown catalog run"}')
                if runtime is not None:
                    view['dashboard_runtime'] = runtime.view()
                body = json.dumps(view['result'] if path == '/api/result' else view,
                                  ensure_ascii=True, allow_nan=False).encode()
                return self.reply(200, body)
            assets = {'/': ('index.html', 'text/html; charset=utf-8'),
                      '/dashboard.css': ('dashboard.css', 'text/css; charset=utf-8'),
                      '/dashboard.js': ('dashboard.js', 'text/javascript; charset=utf-8')}
            if path not in assets:
                return self.reply(404, b'{"error":"Not found"}')
            name, mime = assets[path]
            return self.reply(200, (ASSETS / name).read_bytes(), mime)

        def do_POST(self):
            # The only writable surface is a local polling preference. No Run,
            # worker, filesystem or gateway mutation is exposed by HTTP.
            if self.path != '/api/focus' or not isinstance(feed, Catalog):
                return self.reply(405, b'{"error":"Read-only dashboard"}')
            if not self.authorized(preference=True):
                return
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 256 or self.headers.get('Content-Type') != 'application/json':
                    raise ValueError('Invalid preference')
                self.connection.settimeout(2)
                value = json.loads(self.rfile.read(length))
                if (not isinstance(value, dict) or 'run_id' not in value or
                        set(value) - {'run_id', 'expected_run_id'} or
                        not isinstance(value['run_id'], str) or
                        ('expected_run_id' in value and not isinstance(value['expected_run_id'], str))):
                    raise ValueError('Invalid preference')
                focus = feed.set_focus(value['run_id'], expected_run_id=value.get('expected_run_id'))
            except (ValueError, UnicodeError, TimeoutError):
                return self.reply(400, b'{"error":"Invalid preference"}')
            except KeyError:
                return self.reply(404, b'{"error":"Unknown catalog run or worker binding"}')
            return self.reply(200, json.dumps(focus, allow_nan=False).encode())

    return ThreadingHTTPServer(('127.0.0.1', port), Handler)


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--run-id', help='Registered Run to display; otherwise newest pending training')
    parser.add_argument('--spec', type=Path, help='Optional consistency check against registered spec')
    parser.add_argument('--interval', type=int, default=30)
    parser.add_argument('--local-root', type=Path, default=Path.cwd(),
                        help='Local workspace containing data/ (default: current directory)')
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument('--fleet', type=Path, help='Explicit worker-pool configuration')
    target.add_argument('--host', help='Explicit SSH alias or host for a single worker')
    parser.add_argument('--user', help='SSH user for --host; otherwise use SSH configuration')
    parser.add_argument('--remote-root', default='/srv/ml-orchestrator')
    parser.add_argument('--trainctl-executable', default='trainctl',
                        help='trainctl or its absolute remote executable path')
    parser.add_argument('--known-hosts', type=Path, default=Path.home() / '.ssh/known_hosts')
    parser.add_argument('--identity-file', type=Path, default=Path.home() / '.ssh/id_ed25519')
    args = parser.parse_args(argv)
    if (args.run_id is not None and not re.fullmatch(r'run_[a-f0-9]{32}', args.run_id)) or not 1024 <= args.port <= 65535:
        parser.error('Invalid run id or port')
    if args.fleet and (args.user is not None or args.remote_root != '/srv/ml-orchestrator'
                       or args.trainctl_executable != 'trainctl'):
        parser.error('--user, --remote-root and --trainctl-executable belong in the fleet configuration')
    return args, parser


def main(argv=None):
    args, parser = parse_args(argv)
    root = args.local_root.resolve()
    fleet_provider = remote_resolver = None
    catalog_workers = [args.host] if args.host else []
    worker_aliases = {}
    primary_worker = args.host
    if args.fleet is not None:
        if args.fleet.stat().st_size > 1024 * 1024:
            parser.error('Fleet configuration exceeds 1 MiB')
        fleet_value = json.loads(args.fleet.read_text(encoding='utf-8-sig'))
        worker_directory = directory_from_config(
            fleet_value, OpenSSHRunner, known_hosts=str(args.known_hosts),
            identity_file=str(args.identity_file),
        )
        primary_worker = worker_directory.default_worker_id
        remote = worker_directory.remote_for(primary_worker)
        fleet_provider = worker_directory.status
        catalog_workers = list(worker_directory.worker_ids)
        worker_aliases = {item['host']: item['id'] for item in fleet_value['workers']}
        catalog_workers.extend(worker_aliases)

        def remote_resolver(item):
            worker_id = worker_aliases.get(item.get('worker'), item.get('worker'))
            return worker_directory.remote_for(worker_id)
    else:
        remote = OpenSSHRunner(args.host, user=args.user, remote_root=args.remote_root,
            trainctl_executable=args.trainctl_executable,
            known_hosts=str(args.known_hosts), identity_file=str(args.identity_file))
    directory = root / 'data/dashboard'
    database = root / 'data/orchestrator.db'
    repository = RunRepository(database)
    workflow_engine = WorkflowEngine(database, Orchestrator(repository, remote, worker=primary_worker))

    def workflow_catalog():
        summaries = workflow_engine.execute({'action': 'list', 'limit': 20})['studies']
        return {'schema_version': 1, 'studies': [
            workflow_engine.execute({'action': 'show', 'study_id': item['id']}) for item in summaries
        ]}

    def registered_runs():
        result, seen = [], set()
        for worker in catalog_workers:
            for item in RunRepository.catalog(database, worker, limit=100):
                if item['run_id'] in seen:
                    continue
                item['worker'] = worker_aliases.get(item.get('worker'), item.get('worker'))
                result.append(item)
                seen.add(item['run_id'])
        return sorted(result, key=lambda item: (item.get('created_at') or '', item['run_id']), reverse=True)

    def discover_runs():
        return registered_runs()[:20]

    try:
        primary = select_primary(registered_runs(), args.run_id)
    except ValueError as exc:
        parser.error(str(exc))
    args.run_id = primary['run_id']
    if args.spec is not None:
        spec = json.loads(args.spec.read_text(encoding='utf-8'))
        if descriptor(args.run_id, spec)['spec_sha256'] != primary['spec_sha256']:
            parser.error('Supplied spec does not match the registered Run')
    # Routing follows the selected record, not the fleet default worker.
    if remote_resolver is not None:
        remote = remote_resolver(primary)

    bridge_factory = None
    if args.fleet is not None:
        from .log_metrics import LogBridge
        bridge_factory = lambda task: LogBridge(root, task, args.fleet, args.known_hosts, args.identity_file)
    feed = Catalog(remote, primary, directory, interval=args.interval,
                   discover=discover_runs, workflows=workflow_catalog,
                   fleet=fleet_provider, remote_resolver=remote_resolver,
                   bridge_factory=bridge_factory)
    token = private_token(directory / 'server.json', args.run_id, args.port)
    runtime = RuntimeIdentity()
    http = server(feed, args.port, token, runtime)  # Bind before starting any polling; duplicate launch fails safely.
    url = f'http://127.0.0.1:{args.port}/#{token}'
    atomic_json(directory / 'server.json', {'pid': os.getpid(), 'url': url, 'run_id': args.run_id,
                                         'port': args.port, 'runtime': runtime.view()})
    stop = threading.Event()
    # Fixed collectors follow the local preference, not a startup-only Run.
    feed.enable_focus_collectors()
    threading.Thread(target=feed.run_focused, args=(stop,),
                     kwargs={'before_poll': runtime.poll}, daemon=True).start()
    threading.Thread(target=feed.run_focused, args=(stop,),
                     kwargs={'logs': True}, daemon=True).start()
    collector = threading.Thread(target=feed.run, args=(stop,), daemon=True)
    collector.start()
    print(f'Dashboard listening on http://127.0.0.1:{args.port}/ (private link in data/dashboard/server.json)', flush=True)
    try:
        http.serve_forever(poll_interval=.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        http.server_close()


if __name__ == '__main__':
    main()
