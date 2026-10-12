"""Local entrypoint without installing/updating packages or the worker."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from dashboard.server import main

if __name__ == '__main__':
    main()
