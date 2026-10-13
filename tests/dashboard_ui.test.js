// Pure UI-state unit tests with tiny element doubles; no browser or remote calls.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
function page(savedSelection=null,search=''){
  const storage=new Map([['ccs.selectedRun',savedSelection]]);
  const elements=new Map();
  const context2d=new Proxy({}, {get:(target,key)=>target[key]||(()=>{}),set:(target,key,value)=>(target[key]=value,true)});
  function element(){return {textContent:'',className:'',style:{},children:[],value:'',clientWidth:600,clientHeight:245,
    set innerHTML(value){throw new Error('Unsafe HTML insertion');},
    addEventListener(){},setAttribute(){},getContext(){return context2d;},replaceChildren(){this.children=[];},append(value){this.children.push(value);}};}
  const sandbox={document:{getElementById(id){if(!elements.has(id))elements.set(id,element());return elements.get(id);},createElement:element},
    location:{hash:'#local-token',search},URLSearchParams,sessionStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value)},window:{devicePixelRatio:1,addEventListener(){}},fetch:()=>new Promise(()=>{}),
    AbortSignal:{timeout:()=>undefined},setInterval(){},console};
  vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../src/dashboard/web/dashboard.js'),'utf8'),sandbox);
  elements.get('metric').value='loss';
  return {elements,render(value){sandbox.fixture=value;vm.runInContext('data=fixture;render(data)',sandbox);},run(code){return vm.runInContext(code,sandbox);}};
}
function state(){return {run_id:'run_test',name:'CVFDETR',epochs:50,connection:'fresh',catching_up:false,errors:{},points:[],alerts:[],
  sample_age_seconds:3,interval_seconds:30,latest:{status:'RUNNING',epoch:0,step:12,updates:10,phase:'train',progress_state:'advancing',loss:null},monitor:{health:{gpu:[]}}};}

test('pause observation separates request delivery checkpoint save and confirmed exit',()=>{
  const p=page(),d=state();p.render(d);
  assert.match(p.elements.get('pause-state').textContent,/未声明支持/);
  d.latest.pause={state:'requested',checkpoint_step:null};p.render(d);
  assert.match(p.elements.get('pause-state').textContent,/等待训练器保存/);
  d.latest.pause={state:'checkpoint_committed',checkpoint_step:100,quiescent:false,safe_to_resume:false};p.render(d);
  assert.match(p.elements.get('pause-state').textContent,/已保存.*完整退出/);
  assert.equal(p.elements.get('pause-step').textContent,'100');
  assert.doesNotMatch(p.elements.get('pause-state').textContent,/已安全暂停/);
  d.latest.pause={state:'expired'};p.render(d);
  assert.match(p.elements.get('pause-note').textContent,/不强杀.*不自动续训/);
});

test('only fresh cancelled runs with independent quiescence render a verified pause',()=>{
  const p=page(),d=state();d.latest.pause={state:'paused',quiescent:true,safe_to_resume:true,checkpoint_step:100};p.render(d);
  assert.doesNotMatch(p.elements.get('pause-state').textContent,/已安全暂停/);
  d.latest.status='CANCELLED';p.render(d);
  assert.equal(p.elements.get('pause-state').textContent,'已安全暂停');
  assert.equal(p.elements.get('pause-state').className,'badge good');
  d.connection='stale';p.render(d);
  assert.match(p.elements.get('pause-state').textContent,/上次暂停记录.*过期/);
  assert.equal(p.elements.get('pause-state').className,'badge neutral');
  d.connection='fresh';d.latest.pause.quiescent=false;p.render(d);
  assert.match(p.elements.get('pause-state').textContent,/证据无效/);
});

test('local refresh failure removes a cached safe-pause success badge',async()=>{
  const p=page(),d=state();d.latest.status='CANCELLED';d.latest.pause={state:'paused',quiescent:true,safe_to_resume:true};p.render(d);
  await p.run("busy=false;fetch=async()=>{throw new Error('offline')};refresh()");
  assert.match(p.elements.get('pause-state').textContent,/上次暂停记录.*过期/);
  assert.equal(p.elements.get('pause-state').className,'badge neutral');
});

test('dashboard runtime drift is distinct from worker version and another tab focus',()=>{
  const p=page(),d=state();d.dashboard_runtime={loaded_version:'0.6.1',restart_required:true};
  d.collector_focus={run_id:'run_other',worker:'worker-two'};p.render(d);
  assert.match(p.elements.get('dashboard-version').textContent,/v0.6.1/);
  assert.match(p.elements.get('dashboard-runtime-note').textContent,/源码已变化.*无需重启训练/);
  assert.match(p.elements.get('collector-focus').textContent,/worker-two.*另一标签页.*普通刷新不抢占/);
  assert.equal(p.run('selectedRun'),null);
});

test('reported recovery projection separates logs checkpoint and replay without claiming verified',()=>{
  const p=page(),d=state();d.recovery={logged_step:14680,progress_step:14692,
    checkpoint_step:14600,replay_steps:92,resume_capability:'reported_only',safe_to_resume:null,
    checkpoint_source:'trainer atomic-checkpoint receipt'};p.render(d);
  assert.equal(p.elements.get('recovery-log-step').textContent,'14,680');
  assert.equal(p.elements.get('recovery-progress-step').textContent,'14,692');
  assert.equal(p.elements.get('recovery-checkpoint-step').textContent,'14,600');
  assert.match(p.elements.get('recovery-replay').textContent,/截至已观测进展需重算 92 步.*非实际断电丢失上界/);
  assert.match(p.elements.get('recovery-note').textContent,/尚未验证.*不触发续训/);
  d.recovery={resume_capability:'unknown',replay_steps:null};p.render(d);
  assert.match(p.elements.get('recovery-replay').textContent,/未知/);
  assert.equal(p.elements.get('recovery-checkpoint-step').textContent,'—');
  d.recovery={resume_capability:'verified',replay_steps:0};p.render(d);
  assert.match(p.elements.get('recovery-note').textContent,/worker 报告可恢复.*非实际恢复测试/);
});

test('explicit focus posts once but regular refresh never reclaims another tab focus',async()=>{
  const p=page('run_test');
  await p.run(`busy=false;globalThis.posts=[];render=()=>{};
    fetch=async (url,options)=>{
      if(options.method==='POST'){posts.push(JSON.parse(options.body));return {ok:true,json:async()=>({run_id:'run_test',worker:'worker-one'})};}
      return {ok:true,json:async()=>({run_id:'run_test'})};
    };
    selectRun('run_test');`);
  await new Promise(resolve=>setImmediate(resolve));
  await p.run('refresh()');
  assert.equal(p.run('posts.length'),1);
  assert.equal(p.run('posts[0].run_id'),'run_test');
  assert.deepEqual(Array.from(p.run('Object.keys(posts[0])')),['run_id']);
});

test('automatic follow changes display but does not steal a different tab collector focus',()=>{
  const p=page('run_old');p.run('followLive=true;focusPending=false');
  p.run(`acceptSelection({run_id:'run_old',collector_focus:{run_id:'run_foreign'},catalog:[
    {run_id:'run_old',last_status:'SUCCEEDED',connection:'fresh'},
    {run_id:'run_live',last_status:'RUNNING',connection:'fresh'}]})`);
  assert.equal(p.run('selectedRun'),'run_live');
  assert.equal(p.run('focusPending'),false);
});

test('full-fit epoch and receipt rendering never claims nominal coverage or binary verification',()=>{
  const p=page(),d=state();d.epochs=100;d.latest.epoch=4;d.latest.step=4910;d.latest.updates=4908;
  d.latest.metrics={'train/completed_epochs':4,'train/epoch_batch':740,'train/epoch_batches':1043};
  d.latest.checkpoint={state:'reported_committed',step:4900,safe_to_resume:null,created_at:null};
  d.log_metrics={epoch_basis:'reported_fullfit',collected_at:'2026-09-16T00:00:00Z',fresh:true};
  p.render(d);
  assert.equal(p.elements.get('epoch').textContent,'5 / 100');
  assert.equal(p.elements.get('epoch-progress').value,4);
  assert.match(p.elements.get('epoch-note').textContent,/完整 fit.*已完成 4 轮/);
  assert.doesNotMatch(p.elements.get('epoch-note').textContent,/名义|不是完整遍历/);
  assert.match(p.elements.get('checkpoint-state').textContent,/已报告提交/);
  assert.match(p.elements.get('checkpoint-safe').textContent,/恢复前需校验文件/);
  assert.equal(p.elements.get('updates').textContent,'4,908');
});

test('initial refresh follows active run instead of completed startup primary',async()=>{
  const p=page();
  await p.run(`busy=false; globalThis.calls=[]; globalThis.shown=[];
    render=d=>shown.push(d.run_id);
    fetch=async url=>{calls.push(url);return {ok:true,json:async()=>url.includes('run_live')
      ? {run_id:'run_live',latest:{step:42},points:[{step:42}]}
      : {run_id:'run_old',catalog:[{run_id:'run_old',last_status:'SUCCEEDED',connection:'fresh'},
          {run_id:'run_live',last_status:'RUNNING',connection:'fresh'}]}}};
    refresh();`);
  // The redirect is started by refresh's finally; drain its JSON promise.
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(p.run('data.run_id'),'run_live');
  assert.equal(p.run('data.latest.step'),42);
  assert.equal(p.run('data.points.length'),1);
});

test('explicit selection survives reload and is not replaced by an active run',()=>{
  const first=page();first.run("selectRun('run_history')");
  const saved=first.run("sessionStorage.getItem('ccs.selectedRun')");
  const reloaded=page(saved);
  assert.equal(reloaded.run('selectedRun'),'run_history');
  assert.equal(reloaded.run("acceptSelection({run_id:'run_history',catalog:[{run_id:'run_live',last_status:'RUNNING',connection:'fresh'}]})"),true);
  assert.equal(reloaded.run('selectedRun'),'run_history');
});

test('history view exposes active training and offers follow mode without hiding history',()=>{
  const p=page('run_history'),d=state();d.run_id='run_history';d.latest.status='CANCELLED';
  d.catalog=[{run_id:'run_history',worker:'ubuntu-second',name:'resume-v1',last_status:'CANCELLED',connection:'fresh'},
    {run_id:'run_live',source_run_id:'run_history',worker:'ubuntu-second',name:'resume-v2',last_status:'RUNNING',connection:'fresh',step:42840,loss:12.5}];
  p.render(d);
  assert.match(p.elements.get('selection-note').textContent,/resume-v2/);
  assert.equal(p.elements.get('follow-live').disabled,false);
  assert.equal(p.elements.get('task-select').children[0].value,'run_live');
  assert.equal(p.elements.get('active-run-overview').children.length,1);
  assert.equal(p.elements.get('task-select').value,'run_history');
});

test('follow mode switches cancelled ancestor to running resume and renders only fresh target data',async()=>{
  const p=page('run_history');
  await p.run(`busy=false;globalThis.shown=[];render=d=>shown.push(d.run_id);
    fetch=async url=>({ok:true,json:async()=>url.includes('run_live')
      ? {run_id:'run_live',latest:{step:42840},points:[{step:42840}]}
      : {run_id:'run_history',catalog:[{run_id:'run_history',worker:'ubuntu-second',last_status:'CANCELLED',connection:'fresh'},
          {run_id:'run_live',worker:'ubuntu-second',source_run_id:'run_history',last_status:'RUNNING',connection:'fresh'}]}});
    followRunning();`);
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(p.run('data.run_id'),'run_live');
  assert.equal(p.run('data.latest.step'),42840);
  assert.deepEqual(Array.from(p.run('shown')),['run_live']);
});

test('explicit view link overrides remembered old run without changing the training',()=>{
  const p=page('run_history','?run_id=run_live&follow=1');
  assert.equal(p.run('selectedRun'),'run_live');
});
test('stale lab118 history is not counted as active alongside current ubuntu training',()=>{
  const p=page(),d=state();d.run_id='current';d.catalog=[
    {run_id:'old',name:'old',worker:'lab118',last_status:'RUNNING',connection:'stale'},
    {run_id:'current',name:'current',worker:'ubuntu-second',last_status:'RUNNING',connection:'fresh'}
  ];p.render(d);
  assert.equal(p.elements.get('overview-running').textContent,'1');
  assert.equal(p.elements.get('active-run-count').textContent,'1 个活跃实验');
  assert.equal(p.elements.get('active-run-overview').children[0].children[1].textContent,'ubuntu-second');
  assert.equal(p.elements.get('side-worker').textContent,'ubuntu-second');
  assert.match(p.elements.get('task-overview').children[1].children[3].children[0].textContent,/未确认/);
  d.run_id='old';d.connection='stale';p.render(d);
  assert.equal(p.elements.get('side-worker').textContent,'lab118');
  assert.match(p.elements.get('state').textContent,/上次状态/);
});
test('first epoch is human-numbered and missing loss/GPU is not zero',()=>{
  const p=page();p.render(state());assert.equal(p.elements.get('epoch').textContent,'1 / 50');
  assert.equal(p.elements.get('epoch-progress').value,0);assert.equal(p.elements.get('loss').textContent,'—');
  assert.equal(p.elements.get('gpu-memory').textContent,'—');assert.equal(p.run('numeric(null)'),null);
});
test('CPU utilization, load, threads and system memory render independently from GPU',()=>{
  const p=page(),d=state();d.monitor.health.cpu={model:'AMD Ryzen Test CPU',logical_cores:24,utilization_percent:37.5,load_1m:2.25,load_5m:1.75,load_15m:1.5};
  d.monitor.health.memory={total_bytes:32*1024**3,available_bytes:20*1024**3,used_bytes:12*1024**3,utilization_percent:37.5};p.render(d);
  assert.equal(p.elements.get('cpu-util').textContent,'37.5%');
  assert.equal(p.elements.get('cpu-name').textContent,'AMD Ryzen Test CPU');
  assert.equal(p.elements.get('cpu-load').textContent,'2.25');
  assert.equal(p.elements.get('cpu-cores').textContent,'24');
  assert.equal(p.elements.get('system-memory').textContent,'12.0 / 32.0 GiB');
  assert.equal(p.elements.get('memory-progress').value,37.5);
});
test('worker compatibility is distinct from connectivity',()=>{
  const p=page(),d=state();d.worker_compatibility={state:'legacy',reason:'old worker'};p.render(d);
  assert.match(p.elements.get('worker-compatibility').textContent,/旧版/);
  d.worker_compatibility={state:'incompatible',reason:'protocol major mismatch'};p.render(d);
  assert.match(p.elements.get('worker-compatibility').textContent,/不兼容/);
  assert.equal(p.elements.get('connection').textContent,'● 观测正常');
});
test('stale observation cannot render current green running state',()=>{
  const p=page(), d=state();d.connection='stale';p.render(d);
  assert.match(p.elements.get('state').textContent,/上次状态/);assert.equal(p.elements.get('state').className,'state neutral');
  assert.equal(p.elements.get('progress-state').textContent,'待确认');
});
test('success reaches epoch target and untrusted alerts remain plain text',()=>{
  const p=page(),d=state();d.latest.status='SUCCEEDED';d.latest.epoch=50;
  d.alerts=[{kind:'<script>bad()</script>',opened_at:'2026-09-04T12:00:00Z'}];p.render(d);
  assert.equal(p.elements.get('epoch').textContent,'50 / 50');assert.equal(p.elements.get('epoch-progress').value,100);
  assert.equal(p.elements.get('alerts').children[0].textContent,'<script>bad()</script>');
});
test('validation_finished samples render in the validation curve',()=>{
  const p=page(),d=state();p.elements.get('metric').value='val_loss';
  d.points=[{step:10,phase:'validation_finished',val_loss:2,at:'2026-09-04T12:00:00Z'}];p.render(d);
  assert.equal(p.elements.get('point-count').textContent,'1 个有效采样步');
});
test('test progress uses reported counts and exposes generic metrics safely',()=>{
  const p=page(),d=state();d.epochs=null;d.latest.phase='test';
  d.result={task:{kind:'test',source_run_id:'run_train'},progress:{completed:4,total:10},metrics:{'test/mAP':{value:.42,phase:'test',sample_at:'2026-09-04T12:00:00Z'}}};
  d.catalog=[{run_id:d.run_id,name:'测试',kind:'test'},{run_id:'run_train',name:'训练',kind:'train'}];p.render(d);
  assert.equal(p.elements.get('epoch').textContent,'4 / 10');assert.equal(p.elements.get('epoch-progress').value,40);
  assert.equal(p.elements.get('task-source').textContent,'run_train');
  assert.equal(p.elements.get('task-select').children.length,2);
  assert.equal(p.elements.get('metric-values').children[0].children[0].textContent,'test/mAP');
});
test('switching to an empty task clears metrics instead of carrying previous results',()=>{
  const p=page(),d=state();d.result={task:{kind:'test'},metrics:{'test/AP50':{value:.6}}};p.render(d);
  const next=state();next.run_id='run_analysis';next.result={task:{kind:'analysis'},metrics:{}};p.render(next);
  assert.equal(p.elements.get('metric-values').children.length,0);
  assert.equal(p.elements.get('epoch').textContent,'— / —');
  assert.equal(p.elements.get('task-kind').textContent,'分析');
});
test('export after disconnect or old polling marks stale without changing lifecycle',()=>{
  const p=page(),d=state();d.result={task:{kind:'train'},execution:{status:'RUNNING'},observation:{state:'fresh',polled_at:'2000-01-01T00:00:00Z'}};p.render(d);
  assert.equal(p.run('exportSnapshot(data).observation.state'),'stale');
  assert.equal(p.run('exportSnapshot(data).execution.status'),'RUNNING');
  assert.equal(d.result.observation.state,'fresh');
  d.connection='stale';d.result.observation.polled_at=new Date().toISOString();p.render(d);
  assert.equal(p.run('exportSnapshot(data).observation.state'),'stale');
});
test('configuration distinguishes base learning rate from missing live metrics',()=>{
  const p=page(),d=state();d.result={task:{kind:'train'},metrics:{},configuration:{state:'available',sources:[],parameters:[
    {key:'lr',label:'基础学习率',group:'优化',value:.0001,basis:'frozen_code',source_files:['train.py']},
    {key:'batch_size',label:'batch',group:'训练',value:1,basis:'recorded',source_files:['contract.json']}
  ]}};p.render(d);
  assert.equal(p.elements.get('config-lr').textContent,'1.000e-4');
  assert.equal(p.elements.get('config-batch').textContent,'1');
  assert.match(p.elements.get('config-live-lr').textContent,/未上报/);
  assert.equal(p.elements.get('config-values').children[0].children[2].textContent,'冻结源码核对');
});
test('invalid or missing configuration clears previous task values',()=>{
  const p=page(),d=state();d.result={configuration:{state:'available',sources:[],parameters:[{key:'optimizer',value:'AdamW',source_files:[]}]}};p.render(d);
  d.result.configuration={state:'invalid',error:'hash mismatch',parameters:[]};p.render(d);
  assert.equal(p.elements.get('config-optimizer').textContent,'未记录');
  assert.equal(p.elements.get('config-values').children.length,0);
  assert.match(p.elements.get('config-note').textContent,/校验失败/);
});
test('effective runtime optimizer groups are distinct from reviewed static configuration',()=>{
  const p=page(),d=state();d.result={metrics:{'optimizer/0/group/0/lr':{value:7e-5,sample_at:'2026-09-05T00:00:00Z'}},configuration:{state:'available',sources:[],parameters:[{key:'lr',value:1e-4,source_files:[]}]},runtime_configuration:{schema_version:1,captured_at:'2026-09-05T00:00:00Z',source:'constructed_trainer_state',trainer:{type:'Trainer',precision:'16-mixed',world_size:1,accumulate_grad_batches:2},limits:{parameter_group_count:1,truncated:false},optimizers:[{index:0,type:'AdamW',groups:[{index:0,lr:7e-5,initial_lr:1e-4,weight_decay:0}]}],schedulers:[{index:0,type:'LambdaLR',interval:'step',frequency:1}]}};p.render(d);
  assert.match(p.elements.get('runtime-state').textContent,/已由训练框架采集/);
  assert.equal(p.elements.get('runtime-values').children.length,4);
  assert.match(p.elements.get('runtime-values').children[2].children[1].textContent,/7.000e-5/);
  assert.match(p.elements.get('config-live-lr').textContent,/optimizer\/0\/group\/0\/lr/);
  assert.equal(p.elements.get('config-lr').textContent,'1.000e-4');
});
test('legacy run explicitly says runtime state was not collected',()=>{
  const p=page(),d=state();d.result={configuration:{state:'not_recorded',parameters:[],sources:[]},metrics:{},runtime_configuration:null};p.render(d);
  assert.match(p.elements.get('runtime-state').textContent,/未采集/);
  assert.equal(p.elements.get('runtime-values').children.length,0);
});
test('evidence v2 separates execution deployment and observer state',()=>{
  const p=page(),d=state();d.latest.evidence={schema_version:2,state:'observing',
    execution_integrity:{state:'no_violation_observed',issue_count:0},
    deployment:{state:'changed',issue_count:9},observer:{health:'degraded',coverage:'partial',warning_count:1}};
  p.render(d);
  assert.match(p.elements.get('evidence').textContent,/执行输入未发现变化/);
  assert.match(p.elements.get('evidence').textContent,/部署目录已更新/);
  assert.match(p.elements.get('evidence').textContent,/观测器降级/);
});
test('legacy snapshot verification race is labeled without claiming execution drift',()=>{
  const p=page(),d=state();d.latest.evidence={state:'observing',drift_detected:true};
  d.alerts=[{kind:'evidence_drift',compatibility:'legacy_snapshot_verification_race',opened_at:'2026-09-05T00:00:00Z'}];
  p.render(d);
  assert.match(p.elements.get('evidence').textContent,/未证明执行快照变化/);
  assert.match(p.elements.get('alerts').children[0].textContent,/旧观测器读取竞争/);
});
test('concurrent overview counts tasks and keeps unmanaged argv private',()=>{
  const p=page(),d=state();d.catalog=[
    {run_id:d.run_id,name:'训练',kind:'train',last_status:'RUNNING',connection:'fresh',epoch:2,epochs:50,gpu_index:0},
    {run_id:'run_queue',name:'仿真',kind:'simulate',last_status:'QUEUED',connection:'fresh',gpu_index:0},
    {run_id:'run_fail',name:'构建',kind:'build',last_status:'FAILED',connection:'fresh'}];
  d.workloads={state:'available',observed_at:new Date().toISOString(),items:[{pid:9,launcher:'python',script:'manual.py',project:'robot',memory_bytes:1024,state:'R',managed:false}]};p.render(d);
  assert.equal(p.elements.get('overview-running').textContent,'1');
  assert.equal(p.elements.get('overview-queued').textContent,'1');
  assert.equal(p.elements.get('overview-problem').textContent,'1');
  assert.equal(p.elements.get('task-overview').children.length,3);
  assert.equal(p.elements.get('unmanaged-workloads').children[0].children[1].textContent,'python / manual.py');
});
test('live experiment overview exposes Ubuntu metrics without switching the detail view',()=>{
  const p=page(),d=state();d.catalog=[
    {run_id:d.run_id,name:'CVFDETR',worker:'lab118',kind:'train',last_status:'RUNNING',connection:'fresh',epoch:20,epochs:50,step:120000,loss:5.01,live_lr:1e-4,sample_at:'2026-09-06T03:14:39Z'},
    {run_id:'run_ubuntu',name:'RF-DETR',worker:'ubuntu-second',kind:'train',last_status:'RUNNING',connection:'fresh',epoch:0,epochs:50,step:728,loss:5.7038,live_lr:9.997687e-5,sample_at:'2026-09-06T03:15:00Z'}
  ];p.render(d);
  const rows=p.elements.get('active-run-overview').children;
  assert.equal(rows.length,2);
  assert.equal(rows[1].children[1].textContent,'ubuntu-second');
  assert.equal(rows[1].children[2].textContent,'1 / 50 epochs');
  assert.equal(rows[1].children[3].textContent,'728');
  assert.equal(rows[1].children[4].textContent,'5.7038');
  assert.equal(rows[1].children[5].textContent,'9.998e-5');
  d.run_id='run_ubuntu';d.result={task:{kind:'train',worker:'ubuntu-second'}};p.render(d);
  assert.equal(p.elements.get('detail-worker').textContent,'UBUNTU-SECOND / LIVE OBSERVATION');
});
test('fleet overview shows both workers and keeps selection with the upper agent',()=>{
  const p=page(),d=state();d.fleet={state:'available',selection_policy:'upper_agent_explicit',workers:[
    {worker_id:'lab118',labels:['cuda','rtx3090'],connection:{host:'100.64.0.1',port:22,user:'mlrunner'},node:{hostname:'lab118',platform:'Linux-a',python:'3.12.3'},state:'available',compatibility:{state:'legacy'},workload_count:5,managed_workload_count:5,workload_state:'available',cpu:{model:'Intel Test CPU',logical_cores:24,utilization_percent:32.5,load_1m:3.2},memory:{used_bytes:12*1024**3,total_bytes:32*1024**3,utilization_percent:37.5},gpu:[{index:'0',name:'RTX 3090',utilization_percent:'13',memory_used_mb:'11699',memory_total_mb:'24576',temperature_c:'76'}],disk:{free_bytes:600*1024**3}},
    {worker_id:'ubuntu-second',labels:['cuda','rtx5070ti'],connection:{host:'100.64.0.2',port:22,user:'ubuntu'},node:{hostname:'ubuntu',platform:'Linux-b',python:'3.12.3'},state:'available',compatibility:{state:'compatible'},workload_count:0,managed_workload_count:0,workload_state:'available',cpu:{model:'AMD Ryzen Test CPU',logical_cores:16,utilization_percent:18.5,load_1m:1.2},memory:{used_bytes:10*1024**3,total_bytes:16*1024**3,utilization_percent:62.5},gpu:[{index:'0',name:'RTX 5070 Ti',utilization_percent:'4',memory_used_mb:'348',memory_total_mb:'16303',temperature_c:'34'},{index:'1',name:'Test GPU',utilization_percent:'0',memory_used_mb:'0',memory_total_mb:'8192',temperature_c:'28'}],disk:{free_bytes:560*1024**3}}
  ]};p.render(d);
  assert.equal(p.elements.get('worker-count').textContent,'2 台 · 2 台可达');
  assert.equal(p.elements.get('worker-overview').children.length,2);
  assert.match(p.elements.get('worker-overview').children[1].children[1].textContent,/ubuntu.*100\.64\.0\.2:22.*Python 3\.12\.3/);
  assert.match(p.elements.get('worker-overview').children[1].children[3].textContent,/CPU · AMD Ryzen Test CPU · 16 线程 · 18.5% · load 1.2.*内存 10.0\/16.0 GiB.*GPU 0 · RTX 5070 Ti.*GPU 1 · Test GPU/);
  assert.match(p.elements.get('worker-note').textContent,/上层 Agent/);
  assert.doesNotMatch(p.elements.get('worker-note').textContent,/自动选/);
});
test('disabled worker remains visible with its quarantine reason',()=>{
  const p=page(),d=state();d.fleet={state:'available',selection_policy:'upper_agent_explicit',workers:[
    {worker_id:'unsafe-node',labels:['cuda'],connection:{host:'100.64.0.9',port:22,user:'ubuntu'},node:{hostname:'ubuntu',platform:'Linux',python:'3.13'},state:'disabled',disabled_reason:'memory verification failed',compatibility:{state:'compatible'},workload_count:0,managed_workload_count:0,workload_state:'available',gpu:[{index:'0',name:'RTX',utilization_percent:'0',memory_used_mb:'0',memory_total_mb:'16303'}],disk:{free_bytes:500*1024**3}}
  ]};p.render(d);
  assert.equal(p.elements.get('worker-count').textContent,'1 台 · 0 台可达');
  assert.match(p.elements.get('worker-overview').children[0].children[2].textContent,/已隔离.*memory verification failed/);
});
test('study stages show gates and verified artifact bindings without action controls',()=>{
  const p=page(),d=state();d.workflows={state:'available',studies:[{study:{id:'study_demo',name:'CVFDETR 研究',status:'BLOCKED',plan_sha256:'a'.repeat(64)},stages:[
    {id:'train',name:'训练',kind:'train',status:'SUCCEEDED',block_reason:null,run_id:'run_train',run_link:{kind:'adopted',adopted_by:'codex',metadata_upgrade:'legacy_kind_and_empty_inputs'},decision_package:{readiness:'ready_for_scientific_review',package_sha256:'c'.repeat(64),checkpoint_count:1},approved:false,artifact_bindings:[]},
    {id:'test',name:'最终测试',kind:'test',status:'BLOCKED',block_reason:'approval_required',run_id:null,approved:false,artifact_bindings:[{source_run_id:'run_train',path:'/models/best.ckpt',sha256:'b'.repeat(64)}]}
  ],recommendations:[{stage_id:'test',decision:'PROCEED'}]}]};p.render(d);
  assert.equal(p.elements.get('workflow-count').textContent,'1 个研究计划 · 2 个阶段');
  assert.equal(p.elements.get('workflow-stages').children.length,2);
  assert.match(p.elements.get('workflow-stages').children[0].children[5].textContent,/可供科研审查 · 1 个 checkpoint · c{12}/);
  assert.match(p.elements.get('workflow-stages').children[0].children[6].textContent,/已纳管 · codex · 兼容旧任务元数据/);
  assert.match(p.elements.get('workflow-stages').children[1].children[3].textContent,/等待用户批准/);
  assert.match(p.elements.get('workflow-stages').children[1].children[4].textContent,/best.ckpt/);
});
