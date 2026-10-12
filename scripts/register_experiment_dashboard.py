"""Register hash-bound local experiment metadata for the read-only dashboard.

Does not execute source, contact workers, or mutate a training Run.
"""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from controller.configuration import spec_digest, read_configuration
from controller.repository import RunRepository
from dashboard.feed import atomic_json, now


def register(definition_path, request_path):
    def bounded(path):
        path = Path(path)
        if path.is_symlink() or path.stat().st_size > 256*1024:
            raise ValueError('Unsafe or oversized metadata')
        return json.loads(path.read_text(encoding='utf-8'))
    definition, request = bounded(definition_path), bounded(request_path)
    run, worker = definition['run_id'], definition['worker_id']
    if not re.fullmatch(r'run_[a-f0-9]{32}', run):
        raise ValueError('Invalid Run ID')
    spec = request['payload']['spec']
    records = RunRepository.catalog(ROOT/'data/orchestrator.db', worker, limit=100)
    task = next((r for r in records if r['run_id'] == run), None)
    if (task is None or request['payload']['worker_id'] != worker
            or task['spec_sha256'] != spec_digest(spec)):
        raise ValueError('Registered Run/worker/spec mismatch')
    digest = definition['effective_config_sha256']
    steps = definition.get('max_steps', definition.get('stop_after_updates'))
    epochs = definition.get('nominal_epochs')
    if (not re.fullmatch('[a-f0-9]{64}', digest) or type(steps) is not int or steps <= 0
            or type(epochs) is not int or epochs <= 0 or steps % epochs):
        raise ValueError('Invalid configuration/progress binding')
    out = ROOT/'data/config-evidence'/run
    source = out/'experiment-definition.json'
    atomic_json(source, definition)
    labels = {'experiment_id':'实验编号','worker_id':'服务器','batch_size':'单卡 batch',
              'cameras':'相机数','nominal_epochs':'名义 epochs','stop_after_updates':'目标更新步数',
              'max_steps':'目标更新步数','optimizer':'优化器及参数','fp16':'混合精度',
              'lr_schedule':'学习率策略','seed':'随机种子','initialization':'初始化',
              'scope':'实验范围','effective_config_sha256':'有效配置摘要'}
    parameters = [dict(key=key,label=label,value=definition[key],group='实验配置',
                       basis='submitted',source_files=[source.name],note='来自已登记实验定义；日志摘要一致后才接入指标')
                  for key,label in labels.items() if key in definition]
    report = dict(schema_version=1,run_id=run,spec_sha256=task['spec_sha256'],
                  evidence=task.get('evidence'),reviewed_at=now(),parameters=parameters,
                  sources=[dict(file=source.name,sha256=hashlib.sha256(source.read_bytes()).hexdigest())],
                  log_bridge=dict(worker_id=worker,config_sha256=digest,max_steps=steps,
                                  steps_per_epoch=steps//epochs,epochs=epochs))
    atomic_json(out/'configuration.json', report)
    assert read_configuration(out.parent,task)['state']=='available'
    return dict(run_id=run,parameters=len(parameters),state='available')


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--definition',required=True)
    p.add_argument('--request',required=True)
    a=p.parse_args()
    print(json.dumps(register(a.definition,a.request)))
