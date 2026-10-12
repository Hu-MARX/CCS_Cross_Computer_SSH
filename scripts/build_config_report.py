"""Build an explicitly audited CVFDETR configuration report from local copies.

No SSH, imports of research code, model loading, dataset reads or execution.
Unknown source versions fail closed instead of guessing parameter precedence.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from controller.configuration import spec_digest
from controller.results import SAFE_ID
from dashboard.feed import atomic_json, now

AUDITED = {
    'train_gcr.py': '0b7eb7ce2e44276b50c0724e71e540371af419248f4133e30a613b182c82da6a',
    'cvfdetr_replay.py': 'bd0059cf9c525a32b2a9bcb08485eb56b93db415cc464b6055ccaa697546982e',
    'param_groups.py': 'b52a49e34c9b82deb8427d0a9c133ea91d4f305fd67aad39bbf4d0da0bf8ab12',
    'config.py': 'd5a7086041ca786a622aa8eace72b41fd35e875337ccb3a489ad7f86e7b5b23c',
}


def defaults(path):
    values = {}
    tree = ast.parse(path.read_text(encoding='utf-8'))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'add_argument':
            if node.args and isinstance(node.args[0], ast.Constant):
                for keyword in node.keywords:
                    if keyword.arg == 'default':
                        try:
                            values[node.args[0].value] = ast.literal_eval(keyword.value)
                        except (ValueError, TypeError):
                            pass
    return values


def build(directory, run_id):
    directory = Path(directory)
    if not SAFE_ID.fullmatch(run_id):
        raise ValueError('Invalid run id')
    def read(name):
        return json.loads((directory / name).read_text(encoding='utf-8'))
    for name, digest in AUDITED.items():
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest:
            raise ValueError('Source version requires a new review: ' + name)
    spec, contract, restore, imports = [read(name) for name in (
        'spec.json', 'experiment-contract.json', 'restore-audit.json', 'snapshot-imports.json')]
    base = '/srv/ml-orchestrator/runs/' + run_id
    research = base + '/source-snapshot/research-code'
    adapter = base + '/source-snapshot/orchestrator-code/examples/cvfdetr_replay.py'
    if imports['research_source'] != research or imports['adapter'] != adapter:
        raise ValueError('Runtime import paths do not match the requested snapshot')
    if contract['sources']['train_gcr.py'] != AUDITED['train_gcr.py'] or contract['adapter_sha256'] != AUDITED['cvfdetr_replay.py']:
        raise ValueError('Runtime contract source mismatch')
    parser = argparse.ArgumentParser()
    for flag, value in [('--epochs', 100), ('--batch-size', 2), ('--grad-accum', 4),
                        ('--num-workers', 1), ('--checkpoint-steps', 1000), ('--checkpoint-seconds', 1800)]:
        parser.add_argument(flag, type=int, default=value)
    argv = spec['command']['argv']
    # Reviewed wrapper forwards only these flags; no resume/time/smoke overrides are supported here.
    entry = argv.index('/srv/ml-orchestrator/app/examples/cvfdetr_snapshot.py')
    args = parser.parse_args(argv[entry + 1:])
    for name in ('batch_size', 'grad_accum', 'num_workers'):
        if contract['recipe'][name] != getattr(args, name):
            raise ValueError('Submitted/runtime recipe mismatch: ' + name)
    if contract['recipe']['limit_train_batches'] is not None or restore['from_run'] is not None:
        raise ValueError('Limited/resumed runs require a separate configuration review')
    d = defaults(directory / 'train_gcr.py')
    params = []
    def add(key, label, value, basis, files, note='', group='训练'):
        params.append({'key': key, 'label': label, 'value': value, 'basis': basis,
                       'source_files': files, 'note': note, 'group': group})
    add('epochs', '总 epochs', args.epochs, 'submitted', ['spec.json', 'cvfdetr_replay.py'])
    for key, label in [('batch_size', '每设备 batch size'), ('grad_accum', '梯度累积'), ('num_workers', 'DataLoader workers')]:
        add(key, label, getattr(args, key), 'recorded', ['experiment-contract.json', 'spec.json'])
    add('devices', '训练设备数', 1, 'frozen_code', ['train_gcr.py'], 'Trainer(devices=1)；未读取运行进程内部状态')
    add('effective_batch', '标称有效 batch size', args.batch_size * args.grad_accum, 'derived',
        ['spec.json', 'train_gcr.py'], '每设备 batch × 梯度累积 × 1设备；末尾不足批次或AMP跳步不等于同样更新量')
    add('precision', '精度', '16-mixed', 'frozen_code', ['train_gcr.py'])
    add('seed', '随机种子', d['--seed'], 'frozen_code', ['train_gcr.py', 'cvfdetr_replay.py'], '冻结入口默认值，适配器未覆盖')
    add('optimizer', '优化器', 'AdamW', 'frozen_code', ['train_gcr.py'], 'configure_optimizers', '优化')
    for key, label in [('lr', '基础学习率（配置）'), ('lr_encoder', '编码器基础学习率（配置）'), ('weight_decay', '基础 weight decay')]:
        add(key, label, d['--' + key], 'frozen_code', ['train_gcr.py', 'cvfdetr_replay.py'],
            '不是每个参数组的实时值；分组规则可能覆盖', '优化')
    add('lr_component_decay', 'decoder 学习率倍率', 0.7, 'frozen_code', ['config.py', 'param_groups.py'], 'decoder: lr × lr_component_decay', '优化')
    add('decoder_lr', 'decoder 初始学习率（推导）', d['--lr'] * .7, 'derived', ['train_gcr.py', 'config.py', 'param_groups.py'], '未加载优化器运行状态', '优化')
    add('scheduler', '学习率调度器', 'LambdaLR × 1.0 / step', 'frozen_code', ['train_gcr.py'], '保持各组初始化倍率；不是实时学习率采样', '优化')
    add('warmup', '调度器 warmup', '无（固定倍率）', 'frozen_code', ['train_gcr.py'], group='优化')
    add('current_lr', '实时学习率 / 完整参数组', None, 'not_recorded', [], '当前回调未上报，不加载checkpoint补猜', '优化')
    add('model', '模型配置 / 结构', 'RFDETRLargeConfig / CVF-DETR-GCR', 'frozen_code', ['train_gcr.py'], group='模型与数据')
    add('pretrain', 'pretrain_weights', 'None（本入口未指定预训练权重）', 'frozen_code', ['train_gcr.py'], group='模型与数据')
    add('classes', '类别数量', 4, 'frozen_code', ['train_gcr.py'], group='模型与数据')
    add('resolution', '基础分辨率', d['--resolution'], 'frozen_code', ['train_gcr.py'], 'multi_scale=True，实际训练输入尺寸可变化', '模型与数据')
    for key, label in [('gcr_topk', 'GCR top-k'), ('gcr_projection_dim', 'GCR 投影维数'), ('gcr_max_residual_scale', 'GCR 最大残差比例'), ('gcr_detach_auxiliary', '辅助特征停止梯度')]:
        add(key, label, d['--'+key], 'frozen_code', ['train_gcr.py', 'cvfdetr_replay.py'], group='模型与数据')
    add('dataset', '数据集路径', contract['dataset'], 'recorded', ['experiment-contract.json'], '只读取已有记录，未扫描数据集', '模型与数据')
    add('splits', '本次数据阶段', 'train + val；无自动 test', 'frozen_code', ['train_gcr.py', 'cvfdetr_replay.py'], group='模型与数据')
    add('sample_counts', 'train / val 样本数', None, 'not_recorded', [], '本轮不遍历或读取标注数据', '模型与数据')
    add('augmentation', '增强配置', 'multi_scale + square_resize_div_64；CPU增强', 'frozen_code', ['train_gcr.py'], '其他增强细节未完整展开', '模型与数据')
    add('environment', '解释器 / Conda 路径', argv[0], 'submitted', ['spec.json'], group='环境与恢复')
    for key in ('python', 'torch', 'lightning'):
        add(key, key, contract[key], 'recorded', ['experiment-contract.json'], '启动时记录的版本，不是当前环境重新探测', '环境与恢复')
    add('resume', '是否续训 / 初始 step', '未续训 / ' + str(restore['state']['global_step']), 'recorded', ['restore-audit.json'], group='环境与恢复')
    add('checkpoint_schedule', '周期检查点', f'{args.checkpoint_steps} steps 或 {args.checkpoint_seconds} 秒', 'submitted', ['spec.json', 'cvfdetr_replay.py'], '到达安全更新边界后保存，并非严格定时', '环境与恢复')
    duration = spec.get('resources', {}).get('max_duration_seconds')
    add('time_limit', '训练时间上限', '未设置' if duration is None else str(duration)+' 秒', 'submitted', ['spec.json', 'cvfdetr_replay.py'], group='环境与恢复')
    add('lr_logging', '当前学习率记录', '原 LearningRateMonitor 未保留', 'frozen_code', ['cvfdetr_replay.py'], '未来任务需增加实际参数组采集；本次不注入运行进程', '优化')
    output = '/srv/research/cvfdetr/orchestrator_runs/' + run_id
    paths = {'spec.json': base+'/spec.json', 'snapshot-imports.json': base+'/snapshot-imports.json',
             'train_gcr.py': research+'/train_gcr.py', 'param_groups.py': research+'/src/rfdetr/training/param_groups.py',
             'config.py': research+'/src/rfdetr/config.py', 'cvfdetr_replay.py': adapter,
             'experiment-contract.json': output+'/experiment-contract.json', 'restore-audit.json': output+'/restore-audit.json'}
    report = {'schema_version': 1, 'run_id': run_id, 'spec_sha256': spec_digest(spec), 'evidence': spec.get('evidence'),
              'reviewed_at': now(), 'method': 'audited_frozen_source_and_startup_records', 'parameters': params,
              'sources': [{'file': name, 'remote_path': path, 'sha256': hashlib.sha256((directory/name).read_bytes()).hexdigest()}
                          for name, path in paths.items()]}
    atomic_json(directory/'configuration.json', report)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    report = build(root/'data/config-evidence'/args.run_id, args.run_id)
    print(json.dumps({'run_id': args.run_id, 'parameters': len(report['parameters']), 'sources': len(report['sources'])}))
