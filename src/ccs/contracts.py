"""Offline, packaged contracts shared by the gateway, SDK and MCP adapters.

Only the standard library is needed. The canonical JSON data is loaded through
importlib.resources so installed clients never depend on a checkout or cwd.
"""
from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from importlib.resources import files
import json
import math
import ntpath
from pathlib import PurePosixPath
import re
from typing import Any


class ContractError(ValueError):
    def __init__(self, field_errors, *, code='INVALID_REQUEST', message='payload violates its action contract'):
        super().__init__(message)
        self.code, self.message = code, message
        self.retryable, self.execution_state = False, 'not_sent'
        self.field_errors = field_errors[:32]


@lru_cache(maxsize=1)
def _catalog():
    return json.loads(files('ccs').joinpath('resources', 'action-contracts.json').read_text(encoding='utf-8'))


def _standalone(schema):
    result = deepcopy(schema)
    result['$schema'] = 'https://json-schema.org/draft/2020-12/schema'
    definitions = _catalog()['$defs']
    used = {}

    def include(node):
        if isinstance(node, dict):
            reference = node.get('$ref', '')
            if reference.startswith('#/$defs/'):
                name = reference.split('/')[-1]
                if name not in used:
                    used[name] = deepcopy(definitions[name])
                    include(used[name])
            for item in node.values():
                include(item)
        elif isinstance(node, list):
            for item in node:
                include(item)

    include(result)
    if used:
        result['$defs'] = used
    return result


def _contract(action, value):
    result = deepcopy(value)
    result['action'] = action
    result['input_schema'] = _standalone(value['input_schema'])
    result['output_schema'] = _standalone(value['output_schema'])
    return result


def describe(action=None, workspace_action=None):
    """Return an independent, JSON-serializable catalog or selected contract."""
    catalog = _catalog()
    if workspace_action is not None and action != 'workspace.execute':
        raise ContractError([_error('payload.workspace_action', 'invalid_selection',
                                  'workspace_action requires action workspace.execute')])
    if action is None:
        return {'schema_version': catalog['schema_version'], 'compatibility': deepcopy(catalog['compatibility']),
                'actions': {name: _contract(name, item) for name, item in catalog['actions'].items()}}
    if not isinstance(action, str) or action not in catalog['actions']:
        raise ContractError([_error('payload.action', 'unsupported_action', 'unsupported action')],
                            code='UNSUPPORTED_ACTION', message='unsupported action contract')
    result = _contract(action, catalog['actions'][action])
    if action == 'workspace.execute':
        if workspace_action is None:
            result['workspace_actions'] = {name: _contract(action, item) | {'workspace_action': name}
                                           for name, item in catalog['workspace_actions'].items()}
        elif not isinstance(workspace_action, str) or workspace_action not in catalog['workspace_actions']:
            raise ContractError([_error('payload.workspace_action', 'unsupported_action', 'unsupported workspace action')])
        else:
            result = _contract(action, catalog['workspace_actions'][workspace_action])
            result['workspace_action'] = workspace_action
    return result


def experiment_spec_schema():
    return _standalone({'$ref': '#/$defs/experimentSpec'})


def request_schema():
    catalog = _catalog()
    result = deepcopy(catalog['request_envelope'])
    result['properties']['action'] = {'enum': list(catalog['actions'])}
    result['allOf'] = [
        {'if': {'properties': {'action': {'const': name}}, 'required': ['action']},
         'then': {'properties': {'payload': deepcopy(item['input_schema'])}}}
        for name, item in catalog['actions'].items()
    ]
    return _standalone(result)


def result_schema():
    catalog = _catalog()
    result = deepcopy(catalog['result_envelope'])
    result['properties']['action'] = {'enum': list(catalog['actions'])}
    result['allOf'] = [
        {'if': {'properties': {'action': {'const': name}, 'executed': {'const': True}},
                'required': ['action', 'executed']},
         'then': {'properties': {'result': deepcopy(item['output_schema'])}}}
        for name, item in catalog['actions'].items()
    ]
    return _standalone(result)


def _error(path, code, message):
    return {'path': path, 'code': code, 'message': message}


def _json_errors(value, path, depth=0):
    if depth > 64:
        return [_error(path, 'too_deep', 'JSON nesting exceeds 64 levels')]
    if type(value) is str:
        try:
            value.encode('utf-8')
        except UnicodeEncodeError:
            return [_error(path, 'invalid_encoding', 'text must encode as UTF-8')]
        return []
    if value is None or type(value) in (bool, int):
        return []
    if type(value) is float:
        return [] if math.isfinite(value) else [_error(path, 'non_finite', 'number must be finite')]
    if isinstance(value, (dict, list)):
        errors = []
        for key, item in (value.items() if isinstance(value, dict) else enumerate(value)):
            if isinstance(value, dict) and not isinstance(key, str):
                return [_error(path, 'invalid_key', 'object keys must be strings')]
            child = f'{path}.{key}' if isinstance(value, dict) else f'{path}[{key}]'
            errors.extend(_json_errors(item, child, depth + 1))
            if len(errors) >= 32:
                break
        return errors[:32]
    return [_error(path, 'invalid_type', 'value must be JSON serializable')]


def _matches_type(value, name):
    return {'object': isinstance(value, dict), 'array': isinstance(value, list),
            'string': isinstance(value, str), 'integer': type(value) is int,
            'number': type(value) in (int, float), 'boolean': type(value) is bool,
            'null': value is None}.get(name, False)


def _validate(value, schema, path, defs):
    if schema is True:
        return []
    if schema is False:
        return [_error(path, 'forbidden', 'field is not permitted')]
    if '$ref' in schema:
        ref = schema['$ref']
        if not ref.startswith('#/$defs/'):
            raise RuntimeError('contract contains a non-local schema reference')
        return _validate(value, defs[ref.split('/')[-1]], path, defs)
    errors = []
    if 'type' in schema:
        types = schema['type'] if isinstance(schema['type'], list) else [schema['type']]
        if not any(_matches_type(value, name) for name in types):
            return [_error(path, 'invalid_type', 'expected ' + ' or '.join(types))]
    if 'const' in schema and (value != schema['const'] or isinstance(value, bool) != isinstance(schema['const'], bool)):
        errors.append(_error(path, 'invalid_value', 'value does not match the required constant'))
    if 'enum' in schema and value not in schema['enum']:
        errors.append(_error(path, 'invalid_value', 'value is not an allowed choice'))
    for keyword in ('allOf',):
        for branch in schema.get(keyword, []):
            errors.extend(_validate(value, branch, path, defs))
    if 'if' in schema:
        keyword = 'else' if _validate(value, schema['if'], path, defs) else 'then'
        errors.extend(_validate(value, schema.get(keyword, {}), path, defs))
    for keyword in ('oneOf', 'anyOf'):
        if keyword not in schema:
            continue
        branches = schema[keyword]
        discriminator = schema.get('x-discriminator')
        if isinstance(value, dict) and discriminator:
            selected = value.get(discriminator, schema.get('x-discriminator-default'))
            candidates = [branch for branch in branches
                          if branch.get('properties', {}).get(discriminator, {}).get('const') == selected]
            if len(candidates) == 1:
                errors.extend(_validate(value, candidates[0], path, defs))
                continue
            errors.append(_error(path + '.' + discriminator, 'invalid_value', 'unsupported action or mode'))
            continue
        alternatives = [_validate(value, branch, path, defs) for branch in branches]
        count = sum(not item for item in alternatives)
        if count == 0:
            errors.extend(min(alternatives, key=len))
        elif keyword == 'oneOf' and count != 1:
            errors.append(_error(path, 'ambiguous_value', 'value must match exactly one variant'))
    if isinstance(value, dict):
        for name in schema.get('required', []):
            if name not in value:
                errors.append(_error(path + '.' + name, 'required', 'required field is missing'))
        for key in ('minProperties', 'maxProperties'):
            if key in schema and (len(value) < schema[key] if key == 'minProperties' else len(value) > schema[key]):
                errors.append(_error(path, 'out_of_range', f'object violates {key} {schema[key]}'))
        properties = schema.get('properties', {})
        for name, item in value.items():
            if name in properties:
                errors.extend(_validate(item, properties[name], path + '.' + name, defs))
            elif schema.get('additionalProperties') is False:
                # The field name identifies the typo; payload values never enter errors.
                errors.append(_error(path + '.' + name[:128], 'unknown_field', 'field is not permitted'))
            elif isinstance(schema.get('additionalProperties'), dict):
                errors.extend(_validate(item, schema['additionalProperties'], path + '.' + name, defs))
    if isinstance(value, list):
        for key in ('minItems', 'maxItems'):
            if key in schema and (len(value) < schema[key] if key == 'minItems' else len(value) > schema[key]):
                errors.append(_error(path, 'out_of_range', f'array violates {key} {schema[key]}'))
        for index, item in enumerate(value):
            errors.extend(_validate(item, schema.get('items', {}), f'{path}[{index}]', defs))
    if isinstance(value, str):
        for key in ('minLength', 'maxLength'):
            if key in schema and (len(value) < schema[key] if key == 'minLength' else len(value) > schema[key]):
                errors.append(_error(path, 'out_of_range', f'string violates {key} {schema[key]}'))
        if 'pattern' in schema and not re.search(schema['pattern'], value):
            errors.append(_error(path, 'invalid_format', 'string does not match the required format'))
    if type(value) in (int, float):
        for key, invalid in [('minimum', lambda n: value < n), ('maximum', lambda n: value > n),
                             ('exclusiveMinimum', lambda n: value <= n), ('exclusiveMaximum', lambda n: value >= n)]:
            if key in schema and invalid(schema[key]):
                errors.append(_error(path, 'out_of_range', f'number violates {key} {schema[key]}'))
    return errors[:32]


def _semantic_errors(action, payload):
    errors = []
    if action == 'gateway.describe' and 'workspace_action' in payload and payload.get('action') != 'workspace.execute':
        errors.append(_error('payload.workspace_action', 'invalid_selection', 'requires action workspace.execute'))
    if action == 'run.submit':
        spec = payload['spec']
        if spec.get('evidence') is not None:
            argv = spec['command']['argv']
            absolute = lambda item: PurePosixPath(item).is_absolute() or ntpath.isabs(item)
            index = 1
            while index < len(argv) and argv[index] in ('-u', '-B'):
                index += 1
            if not absolute(argv[0]) or index >= len(argv) or not absolute(argv[index]) or not argv[index].endswith('.py'):
                errors.append(_error('payload.spec.command.argv', 'invalid_evidence_entry',
                                     'bound runs require absolute Python and .py entry paths with optional -u/-B'))
            if (spec.get('evaluation') or {}).get('argv'):
                errors.append(_error('payload.spec.evaluation.argv', 'incompatible_field',
                                     'bound runs require evaluation in the same Python entry'))
    if action == 'workspace.execute':
        request = payload['request']
        if request['action'] == 'stage':
            seen, total = set(), 0
            for index, operation in enumerate(request['operations']):
                # PurePosixPath normalizes repeated separators and dot segments just like the worker.
                name = PurePosixPath(operation['path']).as_posix()
                if name in seen:
                    errors.append(_error(f'payload.request.operations[{index}].path', 'duplicate_path', 'operation path is repeated'))
                seen.add(name)
                if operation['op'] == 'write':
                    try:
                        size = len(operation['content'].encode('utf-8'))
                    except UnicodeEncodeError:
                        errors.append(_error(f'payload.request.operations[{index}].content', 'invalid_encoding', 'text must encode as UTF-8'))
                        continue
                    total += size
                    if size > 1024 * 1024:
                        errors.append(_error(f'payload.request.operations[{index}].content', 'too_large', 'UTF-8 file exceeds 1 MiB'))
            if total > 16 * 1024 * 1024:
                errors.append(_error('payload.request.operations', 'too_large', 'UTF-8 change exceeds 16 MiB'))
    return errors


def validate_request(request: Any) -> None:
    """Validate the shared envelope without resolving targets or opening state."""
    errors = _json_errors(request, 'request')
    if not errors:
        errors = _validate(request, _catalog()['request_envelope'], 'request', _catalog()['$defs'])
    if errors:
        raise ContractError(errors, message='request violates the envelope contract')


def validate_payload(action: str, payload: Any) -> str:
    """Validate locally before remote routing and return read/mutation classification."""
    catalog = _catalog()
    if not isinstance(action, str) or action not in catalog['actions']:
        raise ContractError([_error('action', 'unsupported_action', 'unsupported action')],
                            code='UNSUPPORTED_ACTION', message='unsupported CCS action')
    errors = _json_errors(payload, 'payload')
    if not errors:
        errors = _validate(payload, catalog['actions'][action]['input_schema'], 'payload', catalog['$defs'])
    if not errors:
        errors = _semantic_errors(action, payload)
    if errors:
        raise ContractError(errors)
    if action == 'workspace.execute':
        return catalog['workspace_actions'][payload['request']['action']]['classification']
    return catalog['actions'][action]['classification']
