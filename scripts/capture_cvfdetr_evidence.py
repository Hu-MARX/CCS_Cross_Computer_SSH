"""Explicit project baseline capture; no training or final-test set access.

Runs inside the configured CVFDETR interpreter. --include-images hashes every
train/val image referenced by the copied annotations (without copying images).
"""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath

from trainctl.evidence import EvidenceStore


def research_files(project):
    selected = list((project / 'src').rglob('*.py'))
    # DINO resolves these resources relative to __file__, not dataset/workdir.
    selected.extend((project / 'src/rfdetr/models/backbone/dinov2_configs').glob('*.json'))
    return sorted(path.relative_to(project).as_posix() for path in selected) + ['train_gcr.py']


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--include-images', action='store_true')
    parser.add_argument('--root', type=Path, required=True, help='Configured local worker state root')
    parser.add_argument('--project', type=Path, required=True, help='Project source to capture without importing')
    parser.add_argument('--dataset', type=Path, required=True, help='Dataset with train/val COCO annotations')
    parser.add_argument('--app', type=Path, required=True, help='CCS source checkout')
    parser.add_argument('--adapter', action='append', default=[],
                        help='Optional adapter path relative to --app; may be repeated')
    parser.add_argument('--spec', type=Path, help='Optional declarative experiment JSON to copy')
    args = parser.parse_args(argv)
    for name in ('root', 'project', 'dataset', 'app'):
        if not getattr(args, name).is_absolute():
            parser.error(f'--{name} must be an absolute path')
    if args.spec is not None and not args.spec.is_absolute():
        parser.error('--spec must be an absolute path')
    return args


def main(argv=None):
    args = parse_args(argv)
    project, dataset, app = args.project, args.dataset, args.app
    research = research_files(project)
    software = sorted(str(path.relative_to(app)) for path in (app / 'src').rglob('*.py'))
    software.extend(args.adapter)
    software.append('pyproject.toml')
    groups = [
        {'name': 'research-code', 'root': str(project), 'mode': 'copy', 'files': research},
        {'name': 'orchestrator-code', 'root': str(app), 'mode': 'copy', 'files': software},
        {'name': 'annotations', 'root': str(dataset), 'mode': 'copy',
         'files': ['annotations/train_cocoformat.json', 'annotations/val_cocoformat.json']},
        {'name': 'worker-config', 'root': str(args.root), 'mode': 'copy', 'files': ['worker.json']},
        {'name': 'capture-recipe', 'root': str(Path(__file__).resolve().parent), 'mode': 'copy',
         'files': ['capture_cvfdetr_evidence.py']},
    ]
    if args.spec is not None:
        groups.append({'name': 'experiment-spec', 'root': str(args.spec.parent),
                       'mode': 'copy', 'files': [args.spec.name]})
    images = set()
    annotation_hashes = {}
    if args.include_images:
        for split in ('train', 'val'):
            annotation_bytes = (dataset / f'annotations/{split}_cocoformat.json').read_bytes()
            annotation_hashes[f'annotations/{split}_cocoformat.json'] = hashlib.sha256(annotation_bytes).hexdigest()
            annotation = json.loads(annotation_bytes)
            for row in annotation['images']:
                value = row['file_name']
                relative = PurePosixPath(value)
                if relative.is_absolute() or '\\' in value or ':' in value or any(p in ('', '.', '..') for p in value.split('/')):
                    raise ValueError('Unsafe image path in annotation')
                # The research loader joins dataset/split/file_name.
                path = dataset / split / value
                if not path.is_file() or path.resolve() != path:
                    raise ValueError('Missing or linked referenced image: ' + str(path))
                images.add(str(path.relative_to(dataset)))
        groups.append({'name': 'train-val-images', 'root': str(dataset), 'mode': 'hash', 'files': sorted(images)})
    # These are resource ceilings; image content is hashed without being copied.
    plan = {'schema_version': 1, 'groups': groups, 'max_bytes': 64 * 1024**3, 'max_files': 100000}
    print(json.dumps({'phase': 'capture', 'image_files': len(images), 'image_coverage':
                      'all referenced train/val images' if args.include_images else 'not included',
                      'test_set': 'not accessed'}), flush=True)
    store = EvidenceStore(Path(args.root) / 'evidence')
    receipt = store.capture(plan)
    manifest = json.loads((Path(receipt['path']) / 'manifest.json').read_text())
    if args.include_images:
        captured_annotations = {row['path']: row['sha256'] for row in manifest['files'] if row['group'] == 'annotations'}
        if captured_annotations != annotation_hashes:
            raise RuntimeError('Annotation changed between image enumeration and capture; baseline is not accepted')
    print(json.dumps({'phase': 'committed', 'receipt': receipt}), flush=True)
    verification = store.verify(receipt['bundle_id'], receipt['manifest_sha256'], check_sources=True, check_environment=True)
    if not verification['verified']:
        raise RuntimeError(json.dumps(verification))
    print(json.dumps({'phase': 'verified', 'receipt': receipt, 'verification': verification}), flush=True)


if __name__ == '__main__':
    main()
