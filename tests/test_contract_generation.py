"""Portable generated contracts must not become whole-file Windows diffs."""
import contextlib
import importlib.util
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


class ContractGenerationTests(unittest.TestCase):
    def generator(self):
        location = Path(__file__).resolve().parents[1] / 'scripts' / 'sync-contract-schemas.py'
        spec = importlib.util.spec_from_file_location('ccs_schema_generation_fixture', location)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_generation_uses_lf_and_check_detects_crlf_format_drift(self):
        generator = self.generator()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'schemas').mkdir()
            with (patch.object(generator, 'ROOT', root),
                  patch.object(sys, 'argv', ['sync-contract-schemas.py']),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(generator.main(), 0)
                generated = root / 'schemas' / 'ccs-result.schema.json'
                content = generated.read_bytes()
                self.assertNotIn(b'\r', content)
                with patch.object(sys, 'argv', ['sync-contract-schemas.py', '--check']):
                    self.assertEqual(generator.main(), 0)
                    generated.write_bytes(content.replace(b'\n', b'\r\n'))
                    self.assertEqual(generator.main(), 1)
                self.assertEqual(generator.main(), 0)
                self.assertEqual(generated.read_bytes(), content)


if __name__ == '__main__':
    unittest.main()
