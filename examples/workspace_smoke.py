"""End-to-end reversible workspace check against a configured worker."""
import argparse
import json
from uuid import uuid4

from controller.remote import OpenSSHRunner, RemoteRunnerError


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--known-hosts', required=True)
    parser.add_argument('--identity-file', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--remote-root', default='/srv/ml-orchestrator')
    parser.add_argument('--executable', default='trainctl')
    args = parser.parse_args()
    remote = OpenSSHRunner(args.host, user=args.user, remote_root=args.remote_root, known_hosts=args.known_hosts,
                           identity_file=args.identity_file, trainctl_executable=args.executable)
    relative = '.orchestrator-workspace-smoke-' + uuid4().hex + '.txt'
    marker = 'reversible workspace smoke check\n'
    staged = applied = None
    try:
        staged = remote.workspace({'action': 'stage', 'project': args.project,
            'operations': [{'op': 'write', 'path': relative, 'content': marker}]})
        diff = remote.workspace({'action': 'diff', 'change_id': staged['change_id']})
        if marker.strip() not in diff['diff']:
            raise RuntimeError('staged diff omitted marker')
        applied = remote.workspace({'action': 'apply', 'change_id': staged['change_id'],
                                    'plan_sha256': staged['plan_sha256']})
        read = remote.workspace({'action': 'inspect', 'mode': 'read',
                                 'project': args.project, 'path': relative})
        if read['content'] != marker:
            raise RuntimeError('applied content mismatch')
        restored = remote.workspace({'action': 'restore', 'change_id': staged['change_id'],
                                     'applied_sha256': applied['applied_sha256']})
        try:
            remote.workspace({'action': 'inspect', 'mode': 'read',
                              'project': args.project, 'path': relative})
            raise RuntimeError('restored new file still exists')
        except RemoteRunnerError as exc:
            if exc.code != 'WORKSPACE_NOT_FILE':
                raise
        print(json.dumps({'ok': True, 'change_id': staged['change_id'],
                          'final_status': restored['status'], 'temporary_path': relative}))
        applied = None
    finally:
        if applied is not None and staged is not None:
            remote.workspace({'action': 'restore', 'change_id': staged['change_id'],
                              'applied_sha256': applied['applied_sha256']})


if __name__ == '__main__':
    main()
