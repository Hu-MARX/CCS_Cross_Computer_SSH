import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from controller.domain import RunStatus
from controller.remote import RemoteRunnerError


class AcceptanceCancellationTests(unittest.TestCase):
    def test_pending_cancellation_waits_for_confirmed_terminal_state(self):
        spec = importlib.util.spec_from_file_location('acceptance', Path(__file__).resolve().parents[1] / 'scripts/verify_periodic_cvfdetr.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        remote = SimpleNamespace(cancel=Mock(side_effect=RemoteRunnerError('CANCEL_PENDING', 'still cleaning', retryable=True)))
        wait = Mock(return_value=SimpleNamespace(status=RunStatus.CANCELLED))
        module.cancel_and_wait(remote, 'run_test', wait)
        remote.cancel.assert_called_once()
        wait.assert_called_once_with('run_test')
