"""Synchronize published schemas with the canonical installed action contracts."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from ccs.contracts import experiment_spec_schema, request_schema, result_schema


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='report drift without writing')
    args = parser.parse_args()
    mismatch = []
    for name, schema in (
            ('ccs-request.schema.json', request_schema()),
            ('ccs-result.schema.json', result_schema()),
            ('experiment-spec.schema.json', experiment_spec_schema())):
        target = ROOT / 'schemas' / name
        same = target.exists() and json.loads(target.read_text(encoding='utf-8')) == schema
        if not same:
            mismatch.append(name)
            if not args.check:
                target.write_text(json.dumps(schema, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'drift' if args.check and mismatch else 'synchronized',
                      'changed': mismatch}, ensure_ascii=True))
    return int(args.check and bool(mismatch))


if __name__ == '__main__':
    raise SystemExit(main())
