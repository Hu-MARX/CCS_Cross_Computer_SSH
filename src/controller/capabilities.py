"""Versioned CCS worker interface declaration and compatibility negotiation."""

from __future__ import annotations

from typing import Any, Mapping

from . import __version__


PROTOCOL_NAME = 'ccs-worker'
PROTOCOL_MAJOR = 1
PROTOCOL_MINOR = 0

_CAPABILITIES = {
    'checkpoint.export': 1,
    'checkpoint.import': 1,
    'checkpoint.input_gate': 1,
    'checkpoint.managed': 1,
    'evidence.bundle': 1,
    'evidence.launch_gate': 1,
    'evidence.runtime': 2,
    'run.artifacts': 1,
    'run.artifacts.page': 1,
    'run.cancel': 1,
    'run.events.page': 1,
    'run.logs': 1,
    'run.lookup': 1,
    'run.observe': 1,
    'run.submit': 1,
    'runtime.configuration': 1,
    'source.snapshot': 1,
    'task.metadata': 1,
    'worker.health': 1,
    'worker.monitor': 1,
    'worker.processes': 1,
    'worker.storage': 1,
    'worker.telemetry': 1,
    'workspace.control': 1,
}


def _software_version() -> str:
    return __version__


def worker_interface() -> dict[str, Any]:
    """Return a fresh, JSON-safe declaration of this worker's public interface."""
    return {
        'schema_version': 1,
        'protocol': {'name': PROTOCOL_NAME, 'major': PROTOCOL_MAJOR, 'minor': PROTOCOL_MINOR},
        'software': {'name': 'cross-computer-ssh', 'version': _software_version()},
        'capabilities': dict(_CAPABILITIES),
    }


def requirements_for_spec(spec: Any) -> dict[str, int]:
    """Derive the minimum worker behavior needed to submit one ExperimentSpec."""
    required = {'run.submit': 1}
    if getattr(spec, 'task', None) is not None:
        required['task.metadata'] = 1
        if spec.task.get('checkpoint') is not None:
            required['checkpoint.input_gate'] = 1
    evidence = getattr(spec, 'evidence', None)
    if evidence is not None:
        required.update({'evidence.bundle': 1, 'evidence.launch_gate': 1})
        if evidence.get('execution_mode', 'source') == 'snapshot':
            required['source.snapshot'] = 1
    return dict(sorted(required.items()))


def negotiate_worker(health: Mapping[str, Any], required: Mapping[str, int] | None = None) -> dict[str, Any]:
    """Classify a reachable worker without conflating compatibility with connectivity."""
    requirements = _requirements(required)
    descriptor = health.get('ccs_interface') if isinstance(health, Mapping) else None
    if descriptor is None:
        return _result(
            'legacy', requirements, None,
            reason='worker is reachable but does not declare a CCS interface',
        )

    error = _descriptor_error(descriptor)
    if error is not None:
        return _result('incompatible', requirements, descriptor, reason=error)

    protocol = descriptor['protocol']
    if protocol['major'] != PROTOCOL_MAJOR:
        return _result(
            'incompatible', requirements, descriptor,
            reason=f"protocol major {protocol['major']} is incompatible with required major {PROTOCOL_MAJOR}",
        )

    available = descriptor['capabilities']
    missing = [
        {'name': name, 'required': minimum, 'available': available.get(name)}
        for name, minimum in requirements.items()
        if available.get(name, 0) < minimum
    ]
    if missing:
        return _result(
            'incompatible', requirements, descriptor, missing=missing,
            reason='worker does not provide all required capabilities',
        )
    return _result('compatible', requirements, descriptor, reason='worker interface is compatible')


def _requirements(required: Mapping[str, int] | None) -> dict[str, int]:
    values = dict(required or {})
    for name, minimum in values.items():
        if not isinstance(name, str) or not name or type(minimum) is not int or minimum < 1:
            raise ValueError('capability requirements must map non-empty names to positive integer versions')
    return dict(sorted(values.items()))


def _descriptor_error(value: Any) -> str | None:
    if not isinstance(value, dict) or value.get('schema_version') != 1:
        return 'worker CCS interface descriptor is malformed or uses an unsupported schema'
    protocol = value.get('protocol')
    if (not isinstance(protocol, dict) or protocol.get('name') != PROTOCOL_NAME or
            type(protocol.get('major')) is not int or type(protocol.get('minor')) is not int or
            protocol['major'] < 0 or protocol['minor'] < 0):
        return 'worker CCS protocol declaration is malformed or unknown'
    capabilities = value.get('capabilities')
    if not isinstance(capabilities, dict) or any(
            not isinstance(name, str) or not name or type(capability_version) is not int or capability_version < 1
            for name, capability_version in capabilities.items()):
        return 'worker capability declaration is malformed'
    software = value.get('software')
    if (not isinstance(software, dict) or not isinstance(software.get('name'), str) or
            not isinstance(software.get('version'), str)):
        return 'worker software declaration is malformed'
    return None


def _result(state: str, required: dict[str, int], descriptor: Any, *, reason: str,
            missing: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    protocol = descriptor.get('protocol') if isinstance(descriptor, dict) else None
    software = descriptor.get('software') if isinstance(descriptor, dict) else None
    available = descriptor.get('capabilities') if isinstance(descriptor, dict) else None
    return {
        'state': state,
        'reachable': True,
        'compatible': state == 'compatible',
        'reason': reason,
        'protocol': protocol if isinstance(protocol, dict) else None,
        'software': software if isinstance(software, dict) else None,
        'required_capabilities': required,
        'available_capabilities': available if isinstance(available, dict) else {},
        'missing_capabilities': missing or [],
    }
