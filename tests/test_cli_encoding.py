import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from controller.cli import main


class CliEncodingTests(unittest.TestCase):
    def test_unicode_log_roundtrips_through_gbk_console(self):
        buffer = io.BytesIO()
        console = io.TextIOWrapper(buffer, encoding='gbk')
        with patch('controller.cli.OpenSSHRunner') as remote:
            remote.return_value.logs.return_value = {'text': '训练 💡'}
            with redirect_stdout(console):
                code = main(['--host', 'worker', '--db', ':memory:', 'logs', 'run_test'])
        console.flush()
        result = json.loads(buffer.getvalue().decode('gbk'))
        self.assertEqual(code, 0)
        self.assertEqual(result['data']['text'], '训练 💡')


if __name__ == '__main__':
    unittest.main()
