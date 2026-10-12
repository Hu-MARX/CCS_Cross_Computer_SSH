"""Read-only configuration evidence, separate from live metrics and run policy."""
import hashlib
import json
from pathlib import Path
from .results import SAFE_ID


def spec_digest(spec):
    from .domain import ExperimentSpec
    normalized = ExperimentSpec.from_dict(spec).to_dict()
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def read_configuration(directory, task):
    """Local-only bounded reads; never interpret source or touch remote files."""
    missing = {'state': 'not_recorded', 'parameters': [], 'sources': []}
    run_id = task['run_id']
    if not SAFE_ID.fullmatch(run_id):
        return {**missing, 'state': 'invalid', 'error': 'Invalid run id'}
    path = Path(directory) / run_id / 'configuration.json'
    if not path.exists():
        return missing
    try:
        if path.stat().st_size > 128 * 1024:
            raise ValueError('Configuration report exceeds limit')
        report = json.loads(path.read_text(encoding='utf-8'))
        if (report.get('schema_version') != 1 or report.get('run_id') != run_id or
                report.get('spec_sha256') != task.get('spec_sha256') or
                report.get('evidence') != task.get('evidence')):
            raise ValueError('Configuration run/spec/evidence mismatch')
        sources, parameters = report['sources'], report['parameters']
        if not isinstance(sources, list) or not 1 <= len(sources) <= 16:
            raise ValueError('Invalid configuration sources')
        if not isinstance(parameters, list) or len(parameters) > 128:
            raise ValueError('Invalid configuration parameters')
        source_ids = set()
        for source in sources:
            name = source['file']
            if not isinstance(name, str) or not SAFE_ID.fullmatch(name):
                raise ValueError('Invalid evidence filename')
            target = path.parent / name
            if target.is_symlink() or target.stat().st_size > 256 * 1024:
                raise ValueError('Unsafe or oversized configuration evidence')
            if hashlib.sha256(target.read_bytes()).hexdigest() != source['sha256']:
                raise ValueError('Configuration source hash mismatch: ' + name)
            source_ids.add(name)
        for item in parameters:
            if (not isinstance(item, dict) or item.get('basis') not in
                    {'submitted', 'recorded', 'frozen_code', 'derived', 'not_recorded'} or
                    not isinstance(item.get('source_files'), list) or
                    not set(item['source_files']).issubset(source_ids)):
                raise ValueError('Invalid parameter provenance')
        return {**report, 'state': 'available', 'local_sources_verified': True}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return {**missing, 'state': 'invalid', 'error': str(exc)}
