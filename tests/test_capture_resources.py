import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from trainctl.evidence import EvidenceStore


class CaptureResourceTests(unittest.TestCase):
    def test_research_capture_preserves_adjacent_model_json(self):
        location = Path(__file__).resolve().parents[1] / 'scripts/capture_cvfdetr_evidence.py'
        module_spec = importlib.util.spec_from_file_location('capture_recipe', location)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            project = root / 'research'
            resource = Path('src/rfdetr/models/backbone/dinov2_configs/dinov2_small.json')
            (project / resource).parent.mkdir(parents=True)
            (project / resource).write_text('{"hidden_size":384}')
            (project / 'train_gcr.py').write_text('pass')
            (project / 'src/rfdetr/model.py').write_text('pass')
            receipt = EvidenceStore(root / 'evidence').capture({'schema_version': 1, 'min_free_bytes': 0,
                'groups': [{'name': 'research-code', 'root': str(project), 'mode': 'copy',
                            'files': module.research_files(project)}]})
            # Same package-relative JSON read used by the real DINO get_config.
            copied = Path(receipt['path']) / 'files/research-code' / resource
            self.assertEqual(json.loads(copied.read_text())['hidden_size'], 384)
