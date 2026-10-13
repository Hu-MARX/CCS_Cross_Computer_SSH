"""Verifier counts parent tests separately from subtest/fixture outcomes."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch


_spec = importlib.util.spec_from_file_location('ccs_verification_counts',
    Path(__file__).resolve().parents[1] / 'scripts' / 'verify.py')
verify = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verify)


def run_fixture(suite, *, require_linux=False):
    stdout, stderr = io.StringIO(), io.StringIO()
    arguments = ['--group', 'core'] + (['--require-linux'] if require_linux else [])
    with (patch.object(verify, 'load_group', return_value=suite),
          patch.object(verify.sys, 'platform', 'linux'),
          contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr)):
        status = verify.main(arguments)
    return status, json.loads(stdout.getvalue().strip().splitlines()[-1])


class VerificationCountTests(unittest.TestCase):
    def test_five_subtest_errors_in_two_parent_tests_leave_one_pass_not_negative_three(self):
        class Fixture(unittest.TestCase):
            def test_errors(self):
                for number in range(5):
                    with self.subTest(number=number):
                        raise RuntimeError('expected inner fixture error')

            def test_pass(self):
                pass

        status, report = run_fixture(unittest.defaultTestLoader.loadTestsFromTestCase(Fixture))
        counts = report['groups'][0]
        self.assertEqual(status, 1)
        self.assertEqual(counts['tests_run'], 2)
        self.assertEqual(counts['errors'], 5)
        self.assertEqual(counts['passed_tests'], 1)
        self.assertEqual(counts['failed_tests'], 1)
        self.assertEqual(counts['subtests_run'], 5)
        self.assertEqual(counts['subtest_errors'], 5)

    def test_mixed_subtest_outcomes_count_one_failed_parent_and_preserve_raw_skips(self):
        class Fixture(unittest.TestCase):
            def test_mixed(self):
                with self.subTest(kind='failure'):
                    self.fail('expected fixture assertion')
                with self.subTest(kind='error'):
                    raise RuntimeError('expected fixture error')
                for number in range(2):
                    with self.subTest(kind='skip', number=number):
                        self.skipTest(verify.OPTIONAL_TRAINING_REASON)

            def test_pass(self):
                for number in range(2):
                    with self.subTest(number=number):
                        pass

        status, report = run_fixture(unittest.defaultTestLoader.loadTestsFromTestCase(Fixture))
        counts = report['groups'][0]
        self.assertEqual(status, 1)
        self.assertEqual(counts['passed_tests'], 1)
        self.assertEqual(counts['failed_tests'], 1)
        self.assertEqual(counts['skipped_tests'], 0)  # This skipped parent also failed.
        self.assertEqual(counts['failures'], 1)
        self.assertEqual(counts['errors'], 1)
        self.assertEqual(len(counts['skipped']), 2)
        self.assertTrue(all(item['category'] == 'optional-training-environment' for item in counts['skipped']))
        self.assertEqual(counts['subtests_run'], 6)
        self.assertEqual(counts['passed_subtests'], 2)
        self.assertEqual(counts['subtest_failures'], 1)
        self.assertEqual(counts['subtest_errors'], 1)
        self.assertEqual(counts['skipped_subtests'], 2)

    def test_repeated_subtest_skips_count_one_parent_and_linux_required_skip_still_fails(self):
        class OptionalFixture(unittest.TestCase):
            def test_skips(self):
                for number in range(3):
                    with self.subTest(number=number):
                        self.skipTest(verify.OPTIONAL_TRAINING_REASON)

        status, report = run_fixture(unittest.defaultTestLoader.loadTestsFromTestCase(OptionalFixture))
        counts = report['groups'][0]
        self.assertEqual(status, 0)
        self.assertEqual(counts['passed_tests'], 0)
        self.assertEqual(counts['skipped_tests'], 1)
        self.assertEqual(counts['skipped_subtests'], 3)
        self.assertEqual(len(counts['skipped']), 3)

        class LinuxFixture(unittest.TestCase):
            def test_linux_check(self):
                self.skipTest('Ubuntu process-group verification')

        status, report = run_fixture(unittest.defaultTestLoader.loadTestsFromTestCase(LinuxFixture), require_linux=True)
        self.assertEqual(status, 1)
        self.assertEqual(report['groups'][0]['skipped_tests'], 1)
        self.assertEqual(report['groups'][0]['skipped'][0]['reason'], 'Ubuntu process-group verification')

    def test_class_fixture_error_does_not_subtract_from_unstarted_parent_tests(self):
        class Fixture(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                raise RuntimeError('expected fixture setup error')

            def test_never_started(self):
                pass

        status, report = run_fixture(unittest.defaultTestLoader.loadTestsFromTestCase(Fixture))
        counts = report['groups'][0]
        self.assertEqual(status, 1)
        self.assertEqual(counts['tests_run'], 0)
        self.assertEqual(counts['passed_tests'], 0)
        self.assertEqual(counts['failed_tests'], 0)
        self.assertEqual(counts['errors'], 1)
        self.assertEqual(counts['fixture_errors'], 1)

    def test_expected_failure_and_unexpected_success_are_not_reported_as_passed(self):
        class Fixture(unittest.TestCase):
            @unittest.expectedFailure
            def test_expected_failure(self):
                self.fail('expected fixture failure')

            @unittest.expectedFailure
            def test_unexpected_success(self):
                pass

        status, report = run_fixture(unittest.defaultTestLoader.loadTestsFromTestCase(Fixture))
        counts = report['groups'][0]
        self.assertEqual(status, 1)
        self.assertEqual(counts['tests_run'], 2)
        self.assertEqual(counts['passed_tests'], 0)
        self.assertEqual(counts['expected_failure_tests'], 1)
        self.assertEqual(counts['unexpected_success_tests'], 1)

    def test_parent_accounting_is_per_execution_not_string_id(self):
        class Fixture(unittest.TestCase):
            def test_failed(self):
                for number in range(2):
                    with self.subTest(number=number):
                        self.fail('expected repeated fixture failure')

        case = Fixture('test_failed')
        status, report = run_fixture(unittest.TestSuite([case, case]))
        counts = report['groups'][0]
        self.assertEqual(status, 1)
        self.assertEqual(counts['tests_run'], 2)
        self.assertEqual(counts['failed_tests'], 2)
        self.assertEqual(counts['failures'], 4)
        self.assertEqual(counts['passed_tests'], 0)


if __name__ == '__main__':
    unittest.main()
