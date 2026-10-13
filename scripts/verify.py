"""Portable core, MCP and research verification with explicit skip evidence."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import re
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
GROUPS = ('core', 'mcp', 'research')
LINUX_REQUIRED_SKIP_REASONS = {
    'Symlink privilege unavailable', 'symlinks unavailable',
    'POSIX read-only permissions', 'Linux snapshot subprocess acceptance',
    'Ubuntu process-group verification',
}
WINDOWS_ONLY_SKIP_REASONS = {'Windows job cleanup fault injection'}
PLATFORM_SKIP_REASONS = LINUX_REQUIRED_SKIP_REASONS | WINDOWS_ONLY_SKIP_REASONS
OPTIONAL_TRAINING_REASON = 'Configured training environment required'


class _CountedTextResult(unittest.TextTestResult):
    """Count executed parent outcomes; raw failures/errors may contain subtests."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parent_outcomes = []
        self._current_parent = None
        self._fixture_errors = self._fixture_failures = 0
        self._fixture_skips = 0
        self._subtests = {'subtests_run': 0, 'passed_subtests': 0,
                         'subtest_failures': 0, 'subtest_errors': 0, 'skipped_subtests': 0}

    def startTest(self, test):
        self._current_parent = (test, len(self._parent_outcomes))
        self._parent_outcomes.append('unreported')
        super().startTest(test)

    def stopTest(self, test):
        super().stopTest(test)
        self._current_parent = None

    def _parent_outcome(self, test, outcome):
        parent = getattr(test, 'test_case', test)
        if self._current_parent is None or parent is not self._current_parent[0]:
            return False  # e.g. setUpClass errors: no parent test was started.
        index = self._current_parent[1]
        priority = {'unreported': 0, 'passed': 1, 'skipped': 2,
                    'expected_failure': 3, 'unexpected_success': 4, 'failed': 5}
        if priority[outcome] > priority[self._parent_outcomes[index]]:
            self._parent_outcomes[index] = outcome
        return True

    def addSuccess(self, test):
        self._parent_outcome(test, 'passed')
        super().addSuccess(test)

    def addFailure(self, test, err):
        if not self._parent_outcome(test, 'failed'):
            self._fixture_failures += 1
        super().addFailure(test, err)

    def addError(self, test, err):
        if not self._parent_outcome(test, 'failed'):
            self._fixture_errors += 1
        super().addError(test, err)

    def addSubTest(self, test, subtest, err):
        self._subtests['subtests_run'] += 1
        if err is None:
            self._subtests['passed_subtests'] += 1
        else:
            key = 'subtest_failures' if issubclass(err[0], test.failureException) else 'subtest_errors'
            self._subtests[key] += 1
            self._parent_outcome(test, 'failed')
        super().addSubTest(test, subtest, err)

    def addSkip(self, test, reason):
        if not self._parent_outcome(test, 'skipped'):
            self._fixture_skips += 1
        if getattr(test, 'test_case', None) is not None:
            self._subtests['subtests_run'] += 1
            self._subtests['skipped_subtests'] += 1
        super().addSkip(test, reason)

    def addExpectedFailure(self, test, err):
        self._parent_outcome(test, 'expected_failure')
        super().addExpectedFailure(test, err)

    def addUnexpectedSuccess(self, test):
        self._parent_outcome(test, 'unexpected_success')
        super().addUnexpectedSuccess(test)

    def counts(self):
        return {'passed_tests': self._parent_outcomes.count('passed'),
                'failed_tests': self._parent_outcomes.count('failed'),
                'skipped_tests': self._parent_outcomes.count('skipped'),
                'expected_failure_tests': self._parent_outcomes.count('expected_failure'),
                'unexpected_success_tests': self._parent_outcomes.count('unexpected_success'),
                'unreported_tests': self._parent_outcomes.count('unreported'),
                'fixture_errors': self._fixture_errors, 'fixture_failures': self._fixture_failures,
                'fixture_skips': self._fixture_skips,
                **self._subtests}


def selected_modules(group: str, root: Path = ROOT) -> list[Path]:
    if group == 'research':
        return sorted((root / 'tests' / 'research_integration').glob('test_*.py'))
    if group == 'all':
        return [path for item in GROUPS for path in selected_modules(item, root)]
    return [path for path in sorted((root / 'tests').glob('test_*.py'))
            if (path.name == 'test_mcp_server.py') == (group == 'mcp')]


def check_mcp_version() -> str:
    try:
        version = importlib.metadata.version('mcp')
    except importlib.metadata.PackageNotFoundError as exc:
        raise ValueError('MCP verification requires mcp>=2.2,<3; install the [mcp] extra') from exc
    match = re.fullmatch(r'(\d+)\.(\d+)(?:\.(\d+))?(?:\+[A-Za-z0-9.-]+)?', version)
    if not match or (int(match[1]), int(match[2])) < (2, 2) or int(match[1]) != 2:
        raise ValueError(f'Unsupported MCP SDK {version}; MCP verification requires mcp>=2.2,<3')
    return version


def preflight(group: str, research_root: str | None, require_linux: bool = False) -> dict:
    if require_linux and not sys.platform.startswith('linux'):
        raise ValueError('--require-linux requires an actual Linux interpreter; Windows skips are not Linux acceptance')
    dependencies = {}
    groups = GROUPS if group == 'all' else (group,)
    if 'mcp' in groups:
        dependencies['mcp'] = check_mcp_version()
    if 'research' in groups:
        if research_root is None:
            raise ValueError('Research verification requires explicit --research-root pointing to the research directory')
        root = Path(research_root).resolve()
        if not (root / 'scripts' / 'register_online_joint_dashboard.py').is_file():
            raise ValueError(f'Research root lacks scripts/register_online_joint_dashboard.py: {root}')
        dependencies['research_root'] = str(root)
    return dependencies


def load_group(group: str, root: Path = ROOT) -> unittest.TestSuite:
    modules = selected_modules(group, root)
    if not modules:
        raise ValueError(f'No tests discovered for required group {group}')
    suite = unittest.TestSuite()
    for path in modules:
        # The root pattern never recurses into optional integration groups.
        suite.addTests(unittest.TestLoader().discover(str(path.parent), pattern=path.name,
                                                     top_level_dir=str(path.parent)))
    return suite


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--group', choices=(*GROUPS, 'all'), default='core')
    parser.add_argument('--research-root')
    parser.add_argument('--report', help='Write a JSON verification record outside runtime databases')
    parser.add_argument('--require-linux', action='store_true')
    args = parser.parse_args(argv)
    report = {'group': args.group, 'platform': sys.platform, 'python': sys.version,
              'status': 'failed', 'groups': [], 'excluded_modules': [], 'dependencies': {}}
    previous_research_root = os.environ.get('CCS_RESEARCH_ROOT')
    previous_python_path = os.environ.get('PYTHONPATH')
    previous_cwd = Path.cwd()
    previous_sys_path = list(sys.path)
    try:
        report['dependencies'] = preflight(args.group, args.research_root, args.require_linux)
        if args.research_root is not None:
            os.environ['CCS_RESEARCH_ROOT'] = str(Path(args.research_root).resolve())
        sys.path[:0] = [str(ROOT), str(ROOT / 'src'), str(ROOT / 'tests')]
        os.environ['PYTHONPATH'] = str(ROOT / 'src')
        os.chdir(ROOT)
        groups = GROUPS if args.group == 'all' else (args.group,)
        selected = {str(path) for group in groups for path in selected_modules(group)}
        report['excluded_modules'] = [str(path.relative_to(ROOT)) for path in selected_modules('all')
                                      if str(path) not in selected]
        passed = True
        for group in groups:
            print(f'\nCCS verification group: {group}', flush=True)
            result = unittest.TextTestRunner(verbosity=2, resultclass=_CountedTextResult).run(load_group(group))
            skips = [{'test': test.id(), 'reason': reason,
                      'category': 'optional-training-environment' if reason == OPTIONAL_TRAINING_REASON else 'platform'}
                     for test, reason in result.skipped]
            unexpected = [item for item in skips if group != 'core' or
                          item['reason'] not in PLATFORM_SKIP_REASONS | {OPTIONAL_TRAINING_REASON} or
                          (args.require_linux and item['reason'] in LINUX_REQUIRED_SKIP_REASONS)]
            accepted = result.wasSuccessful() and not unexpected
            report['groups'].append({'group': group, 'tests_run': result.testsRun,
                                    **result.counts(),
                                    'failures': len(result.failures), 'errors': len(result.errors),
                                    'skipped': skips, 'status': 'passed' if accepted else 'failed'})
            if unexpected:
                print('ERROR: required checks were skipped: ' + json.dumps(unexpected), file=sys.stderr)
            passed = passed and accepted
        report['status'] = 'passed' if passed else 'failed'
    except (ValueError, ImportError, OSError) as exc:
        report['error'] = str(exc)
        print('ERROR: ' + str(exc), file=sys.stderr)
    finally:
        sys.path[:] = previous_sys_path
        os.chdir(previous_cwd)
        if previous_python_path is None:
            os.environ.pop('PYTHONPATH', None)
        else:
            os.environ['PYTHONPATH'] = previous_python_path
        if previous_research_root is None:
            os.environ.pop('CCS_RESEARCH_ROOT', None)
        else:
            os.environ['CCS_RESEARCH_ROOT'] = previous_research_root
        if args.report:
            path = Path(args.report).resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps(report, ensure_ascii=True, separators=(',', ':')))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
