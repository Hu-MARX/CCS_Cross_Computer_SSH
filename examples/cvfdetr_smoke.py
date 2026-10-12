"""Isolated one-batch GPU acceptance; never resume or overwrite research outputs."""
import json
import os
import re
import importlib.util
import sys
from pathlib import Path


def main():
    project = Path('/srv/research/cvfdetr')
    run_id = os.environ.get('TRAINCTL_RUN_ID', '')
    if not re.fullmatch(r'run_[a-f0-9]{32}', run_id):
        raise RuntimeError('Must be launched by trainctl')
    if Path.cwd().resolve() != project:
        raise RuntimeError('Unexpected working directory')
    output = project / 'orchestrator_runs' / run_id
    output.mkdir(mode=0o770, exist_ok=False)
    for variable, directory in {'XDG_CACHE_HOME': 'cache', 'MPLCONFIGDIR': 'matplotlib',
                                 'TORCH_HOME': 'torch', 'HF_HOME': 'huggingface'}.items():
        os.environ[variable] = str(output / directory)
        (output / directory).mkdir(parents=True, exist_ok=True)
    (output / 'cache/torch/kernels').mkdir(parents=True, exist_ok=True)
    os.environ['HF_HUB_OFFLINE'] = '1'
    os.environ['WANDB_MODE'] = 'disabled'
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError('CUDA unavailable in configured Conda environment')
    details = {'run_id': run_id, 'python': sys.executable, 'torch': torch.__version__,
               'cuda': torch.version.cuda, 'gpu': torch.cuda.get_device_name(0),
               'output': str(output), 'mode': 'one training and one validation batch'}
    (output / 'environment.json').write_text(json.dumps(details, indent=2), encoding='utf-8')
    print(json.dumps(details), flush=True)
    sys.argv = [str(project / 'train_gcr.py'), '--dataset_dir',
                '/srv/datasets/example-coco', '--output_dir', str(output),
                '--epochs', '1', '--batch_size', '1', '--grad_accum', '1', '--num_workers', '0', '--fast_dev_run']
    from trainctl.lightning_progress import TrainingProgressCallback
    module_spec = importlib.util.spec_from_file_location('research_smoke_train', sys.argv[0])
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    original_trainer = module.Trainer
    def monitored_trainer(*args, **kwargs):
        kwargs['callbacks'] = [*kwargs.get('callbacks', []), TrainingProgressCallback()]
        return original_trainer(*args, **kwargs)
    module.Trainer = monitored_trainer
    module.main()
    (output / 'smoke-result.json').write_text(json.dumps({'status': 'passed', **details}, indent=2), encoding='utf-8')
    print('CVFDETR_GPU_SMOKE_PASSED', flush=True)


if __name__ == '__main__':
    main()
