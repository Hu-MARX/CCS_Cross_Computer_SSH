"""CPU-only progress workload for service independence and crash cleanup tests."""
import argparse
import json
import os
import subprocess
import sys
import time

from trainctl.progress import ProgressReporter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--descendant', action='store_true')
    args = parser.parse_args()
    child = None
    if args.descendant:
        # New session deliberately escapes the supervisor's POSIX process group.
        # systemd must still clean it through the containing cgroup.
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'], start_new_session=True)
    print(json.dumps({'pid': os.getpid(), 'descendant_pid': child.pid if child else None}), flush=True)
    reporter = ProgressReporter(interval=1)
    try:
        for step in range(args.seconds):
            reporter.emit('train', step=step, metrics={'loss': 1/(step+1), 'optimizer_updates_this_process': step}, force=True)
            time.sleep(1)
        reporter.emit('finished', step=args.seconds, force=True)
    finally:
        if child:
            child.terminate()
            child.wait(timeout=5)


if __name__ == '__main__':
    main()
