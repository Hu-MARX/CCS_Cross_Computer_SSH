"""Shared bounded page limits and opaque inventory cursor encoding."""
from __future__ import annotations

import base64
import json
import hashlib

DEFAULT_PAGE_BYTES = 1024 * 1024
MAX_PAGE_BYTES = 16 * 1024 * 1024


class PageItemTooLarge(ValueError):
    code, retryable = 'PAGE_ITEM_TOO_LARGE', False

    def __init__(self, needed):
        self.message = f'request max_bytes of at least {needed}; cursor unchanged'
        super().__init__(self.message)


def validate_page(limit, max_bytes):
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError('page limit must be an integer between 1 and 1000')
    if type(max_bytes) is not int or not 1024 <= max_bytes <= MAX_PAGE_BYTES:
        raise ValueError('page max_bytes must be between 1024 and 16777216')


def artifact_cursor(run_id, path, last_path):
    scope = hashlib.sha256(json.dumps([run_id, path], ensure_ascii=False,
                                     separators=(',', ':')).encode('utf-8')).hexdigest()
    raw = json.dumps([1, scope, last_path], ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    return base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')


def artifact_after(cursor, run_id, path):
    if cursor is None:
        return ''
    if not isinstance(cursor, str) or not cursor or len(cursor) > 8192:
        raise ValueError('invalid artifact cursor')
    try:
        value = json.loads(base64.b64decode(cursor + '=' * (-len(cursor) % 4), altchars=b'-_', validate=True))
    except (ValueError, UnicodeError) as exc:
        raise ValueError('invalid artifact cursor') from exc
    scope = hashlib.sha256(json.dumps([run_id, path], ensure_ascii=False,
                                     separators=(',', ':')).encode('utf-8')).hexdigest()
    if (not isinstance(value, list) or len(value) != 3 or value[:2] != [1, scope] or
            not isinstance(value[2], str)):
        raise ValueError('artifact cursor belongs to a different run or path filter')
    return value[2]


def bounded_items(items, *, key, initial_cursor, encode_cursor, limit, max_bytes):
    validate_page(limit, max_bytes)
    selected, used, truncated = [], 2, False
    iterator = iter(items)
    has_more = False
    for item in iterator:
        length = len(json.dumps(item, ensure_ascii=False).encode('utf-8')) + 1
        if used + length > max_bytes:
            if not selected:
                raise PageItemTooLarge(used + length)
            has_more, truncated = True, True
            break
        selected.append(item)
        used += length
        if len(selected) == limit:
            has_more = next(iterator, None) is not None
            break
    cursor = encode_cursor(key(selected[-1])) if selected else initial_cursor
    return selected, cursor, has_more, truncated
