"""Optional framework-independent progress publisher; only global rank zero writes."""
import json
import os
import time
from pathlib import Path
from uuid import uuid4


class ProgressReporter:
    def __init__(self, run_dir=None, interval=15):
        value = run_dir or os.environ.get('TRAINCTL_RUN_DIR')
        self.directory = Path(value) if value else None
        self.interval = interval
        self.last = 0.0

    def emit(self, phase, *, step=None, metrics=None, force=False):
        if self.directory is None:
            return
        now = time.monotonic()
        if not force and now - self.last < self.interval:
            return
        from controller.domain import utc_now
        payload = {'updated_at': utc_now(), 'phase': str(phase), 'step': step, 'metrics': metrics or {}}
        data = json.dumps(payload, allow_nan=False)
        if len(data.encode()) > 64 * 1024:
            raise ValueError('Progress record too large')
        temporary = self.directory / ('progress.' + uuid4().hex + '.tmp')
        try:
            with temporary.open('w', encoding='utf-8') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.directory / 'progress.json')
        finally:
            temporary.unlink(missing_ok=True)
        self.last = now
