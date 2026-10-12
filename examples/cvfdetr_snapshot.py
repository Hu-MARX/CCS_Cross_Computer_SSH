"""Snapshot-v1 entry; outputs/data stay under the original PycharmProject."""
import os
import sys
from pathlib import Path


def main():
    root = Path(os.environ['TRAINCTL_SNAPSHOT_ROOT']).resolve()
    if Path(__file__).resolve() != root / 'orchestrator-code/examples/cvfdetr_snapshot.py':
        raise ValueError('Snapshot entry must execute from its per-run copy')
    project = Path('/srv/research/cvfdetr')
    # Do not leave cwd/original research paths available for lazy imports.
    sys.path[:] = [p for p in sys.path if p and not Path(p).resolve().is_relative_to(project)]
    source = root / 'research-code'
    sys.path.insert(0, str(source / 'src'))
    import cvfdetr_replay
    cvfdetr_replay.main(source_project=source)


if __name__ == '__main__':
    main()
