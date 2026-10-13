"use strict";
const $ = id => document.getElementById(id);
const token = location.hash.slice(1);
let data = null, busy = false, selectedRun = null, requestVersion = 0, focusPending = false, focusExpected = null;
try { const saved=sessionStorage.getItem('ccs.selectedRun'); if(/^run_[a-zA-Z0-9_-]+$/.test(saved||''))selectedRun=saved; } catch (_) {}
let followLive=!selectedRun;
try { if(sessionStorage.getItem('ccs.followLive')==='true')followLive=true; } catch (_) {}
const viewParams=new URLSearchParams(location.search||'');
const requestedRun=viewParams.get('run_id');
if(/^run_[a-zA-Z0-9_-]+$/.test(requestedRun||'')){
  selectedRun=requestedRun;followLive=viewParams.get('follow')==='1';
}
function isActive(item){return item.connection==='fresh'&&['RUNNING','EVALUATING','PREPARING','QUEUED'].includes(item.last_status);}
function orderedCatalog(items){
  const rank=item=>isActive(item)?(['RUNNING','EVALUATING'].includes(item.last_status)?0:1):2;
  return [...items].sort((a,b)=>rank(a)-rank(b)||String(b.created_at||'').localeCompare(String(a.created_at||'')));
}
function preferredActive(value){
  const catalog=orderedCatalog(value.catalog||[]),active=catalog.filter(isActive);
  const current=catalog.find(item=>item.run_id===selectedRun);
  if(current&&isActive(current))return current;
  const byId=new Map(catalog.map(item=>[item.run_id,item]));
  const successor=active.find(item=>{
    if(!current||item.worker!==current.worker)return false;
    const visited=new Set();let id=item.source_run_id;
    while(id&&!visited.has(id)){if(id===current.run_id)return true;visited.add(id);id=(byId.get(id)||{}).source_run_id;}
    return false;
  });
  return successor||active.find(item=>current&&item.worker===current.worker)||active[0];
}
function followRunning(){
  followLive=true;focusPending=true;focusExpected=null;requestVersion++;
  try { sessionStorage.setItem('ccs.followLive','true'); } catch (_) {}
  refresh();
}
function selectRun(run){
  selectedRun=run;followLive=false;focusPending=true;focusExpected=null;requestVersion++;
  try { sessionStorage.setItem('ccs.selectedRun',run);sessionStorage.setItem('ccs.followLive','false'); } catch (_) {}
  refresh();
}
function acceptSelection(value){
  if(selectedRun&&!followLive)return true;
  const active=preferredActive(value);
  const previous=selectedRun;
  selectedRun=active?active.run_id:value.run_id;
  // Automatic continuation may keep its own focus, but never steal focus
  // from an explicit selection in another tab. The server also checks this
  // expected owner, covering a selection race between GET and POST.
  const focus=value.collector_focus;
  if(!focusPending&&selectedRun!==previous&&(!focus||focus.run_id===previous||(!previous&&focus.run_id===value.run_id))){
    focusPending=true;focusExpected=focus?focus.run_id:null;
  }
  try { sessionStorage.setItem('ccs.selectedRun',selectedRun);sessionStorage.setItem('ccs.followLive',String(followLive)); } catch (_) {}
  if(selectedRun!==value.run_id){requestVersion++;return false;}
  return true;
}
const labels = {RUNNING:"任务运行中",PREPARING:"启动检查中",QUEUED:"排队中",EVALUATING:"评价中",SUCCEEDED:"已完成",FAILED:"失败 / 中断",CANCELLED:"已取消",LOST:"执行失联",UNKNOWN:"未知"};
const phases = {train:"训练阶段",validation:"验证阶段",validation_finished:"验证结束",test:"测试阶段",test_finished:"测试结束",analysis:"分析阶段",build:"构建阶段",simulate:"仿真阶段",benchmark:"基准测试",data:"数据处理",checkpoint:"保存检查点",finished:"执行结束"};
const kinds = {train:"训练",test:"测试",analysis:"分析",build:"构建",simulate:"仿真",benchmark:"基准测试",data:"数据处理",unspecified:"未声明类型"};
const nums = value => typeof value === "number" && Number.isFinite(value);
const numeric = value => value == null || value === "" || !Number.isFinite(Number(value)) ? null : Number(value);
const fmt = (v, digits=0) => nums(v) ? v.toLocaleString("zh-CN",{maximumFractionDigits:digits}) : "—";
const date = v => v && !Number.isNaN(Date.parse(v)) ? new Date(v).toLocaleString("zh-CN",{hour12:false}) : "—";
const gb = v => nums(v) ? (v/1024**3).toFixed(1)+" GiB" : "—";
const text = (id,v) => { $(id).textContent = v; };
function badge(id,value,kind){text(id,value);$(id).className=(id==="state"?"state ":"badge ")+kind;}
function exportSnapshot(d){
  const result=JSON.parse(JSON.stringify(d.result));
  result.exported_at=new Date().toISOString();
  const polled=Date.parse((result.observation||{}).polled_at);
  if(d.connection!=="fresh"||!Number.isFinite(polled)||Date.now()-polled>90000)result.observation.state="stale";
  return result;
}
function configValue(value){
  if(value===null||value===undefined)return "未记录";
  if(typeof value==="boolean")return value?"是":"否";
  if(nums(value))return value!==0&&Math.abs(value)<0.001?value.toExponential(3):fmt(value,6);
  return String(value);
}
function renderConfiguration(d){
  const config=(d.result||{}).configuration||{state:"not_recorded",parameters:[],sources:[]};
  const available=config.state==="available", params=available?config.parameters:[];
  const byKey=Object.fromEntries(params.map(p=>[p.key,p]));
  const basis={submitted:"提交配置",recorded:"启动记录",frozen_code:"冻结源码核对",derived:"条件推导",not_recorded:"未记录"};
  text("config-note",available?"来源已核对 · 配置值与实时观测分开显示；源码推导不等于读取了运行中的优化器状态。":config.state==="invalid"?"配置证据校验失败，不显示旧参数："+(config.error||""):"此任务尚无已核对配置记录，不沿用其他任务参数。");
  for(const [id,key] of [["config-optimizer","optimizer"],["config-batch","batch_size"],["config-accum","grad_accum"],["config-effective","effective_batch"],["config-lr","lr"],["config-precision","precision"]])text(id,configValue((byKey[key]||{}).value));
  $("config-values").replaceChildren();
  for(const p of params){const row=document.createElement("tr");for(const value of [p.group+" / "+p.label,configValue(p.value),basis[p.basis]||p.basis,(p.source_files||[]).join(" · ")+(p.note?" — "+p.note:"")]){const cell=document.createElement("td");cell.textContent=value;row.append(cell);}$("config-values").append(row);}
  $("config-sources").replaceChildren();
  for(const s of available?config.sources:[]){const item=document.createElement("li");item.textContent=`${s.file} · ${s.remote_path} · SHA256 ${s.sha256}`;$("config-sources").append(item);}
  text("config-review-time",available?"核对时间："+date(config.reviewed_at):"未核对");
  const live=Object.entries((d.result||{}).metrics||{}).filter(([key])=>/^(lr([/_-]|$)|learning_rate|optimizer\/\d+\/group\/\d+\/lr$)/i.test(key));
  text("config-live-lr",live.length?"学习率采样"+(d.connection==="fresh"?"":"（可能过期）")+"："+live.map(([key,item])=>key+" = "+configValue(item.value)+" · "+date(item.sample_at)).join("；"):"实时学习率：未上报。上方为基础配置值，不绘制推测的学习率曲线。");
  const runtime=(d.result||{}).runtime_configuration;
  $("runtime-values").replaceChildren();
  if(runtime&&runtime.schema_version===1){
    badge("runtime-state","已由训练框架采集",d.connection==="fresh"?"good":"neutral");
    text("runtime-note",`采集时间 ${date(runtime.captured_at)} · ${runtime.source||"未知来源"} · ${(runtime.limits||{}).parameter_group_count??"—"} 个参数组${(runtime.limits||{}).truncated?"（记录已截断）":""}`);
    const rows=[];
    rows.push(["Trainer / 精度",`${(runtime.trainer||{}).type||"—"} / ${(runtime.trainer||{}).precision||"—"}`]);
    rows.push(["world size / 梯度累积",`${configValue((runtime.trainer||{}).world_size)} / ${configValue((runtime.trainer||{}).accumulate_grad_batches)}`]);
    for(const optimizer of runtime.optimizers||[])for(const group of optimizer.groups||[])rows.push([
      `优化器 ${optimizer.index} · 参数组 ${group.index}${group.name?" · "+group.name:""}`,
      `${optimizer.type} · lr ${configValue(group.lr)} · initial_lr ${configValue(group.initial_lr)} · weight_decay ${configValue(group.weight_decay)}`]);
    for(const scheduler of runtime.schedulers||[])rows.push([`调度器 ${scheduler.index}`,`${scheduler.type} · ${scheduler.interval} · frequency ${configValue(scheduler.frequency)}`]);
    for(const values of rows){const row=document.createElement("tr");for(const value of values){const cell=document.createElement("td");cell.textContent=value;row.append(cell);}$("runtime-values").append(row);}
  }else{
    badge("runtime-state",runtime&&runtime.state==="invalid"?"运行时记录无效":"当前任务未采集","neutral");
    text("runtime-note",runtime&&runtime.error?runtime.error:"该能力仅用于部署后的新任务；不会注入正在运行的训练进程。");
  }
}
function elapsed(value){
  const start=Date.parse(value||"");if(!Number.isFinite(start))return "—";
  const seconds=Math.max(0,Math.floor((Date.now()-start)/1000));
  if(seconds<60)return seconds+" 秒";if(seconds<3600)return Math.floor(seconds/60)+" 分";
  if(seconds<86400)return Math.floor(seconds/3600)+" 小时 "+Math.floor(seconds%3600/60)+" 分";
  return Math.floor(seconds/86400)+" 天 "+Math.floor(seconds%86400/3600)+" 小时";
}
function catalogProgress(item){
  const progress=item.progress||{};
  if(nums(progress.completed)&&nums(progress.total))return `${fmt(progress.completed)} / ${fmt(progress.total)}`;
  if(item.kind==="train"&&nums(item.epoch)&&nums(item.epochs))return `${fmt(item.last_status==="SUCCEEDED"?item.epochs:Math.min(item.epochs,item.epoch+1))} / ${fmt(item.epochs)} epochs`;
  if(nums(item.step))return "step "+fmt(item.step);
  return item.progress_state==="suspected_stall"?"疑似停滞":"—";
}
function evidenceStatus(ev,alerts){
  if(ev.schema_version===2){
    const execution=({violation_observed:"执行输入已确认变化",no_violation_observed:"执行输入未发现变化",unknown:"执行完整性待确认"}[(ev.execution_integrity||{}).state]||"执行完整性待确认");
    const deployment=({changed:"部署目录已更新（不等于执行快照变化）",unchanged:"部署目录未见变化",not_applicable:"无独立部署目录",unknown:"部署目录核验待确认"}[(ev.deployment||{}).state]||"部署目录状态待确认");
    const observer=({healthy:"观测器正常",degraded:"观测器降级",failed:"观测器故障"}[(ev.observer||{}).health]||"观测器状态待确认");
    return `${execution} · ${deployment} · ${observer}`;
  }
  if((alerts||[]).some(a=>!a.resolved_at&&a.compatibility==="legacy_snapshot_verification_race"))return "旧观测器读取竞争，未证明执行快照变化";
  return ev.drift_detected?"旧版证据变化（范围无法判定，保留历史）":ev.state==="error"?"观察器异常":ev.state?"已检查范围未发现漂移":"等待检查";
}
function renderFleet(d){
  const report=d.fleet||{state:"not_collected",workers:[]},workers=report.workers||[];
  const reachable=workers.filter(item=>item.state==="available").length;
  text("worker-count",`${workers.length} 台 · ${reachable} 台可达`);
  text("worker-note",report.state==="available"?"执行位置由上层 Agent 显式指定；CCS 仅报告资源与负载事实。":report.error?"Worker 状态读取异常："+report.error:"尚未读取多从机状态。");
  $("worker-overview").replaceChildren();
  const states={available:"可达",disabled:"已隔离",unreachable:"不可达",incompatible:"接口不兼容"};
  const compatibility={compatible:"CCS v1 兼容",legacy:"旧版接口",incompatible:"能力不兼容"};
  for(const item of workers){
    const row=document.createElement("tr"),connection=item.connection||{},node=item.node||{};
    const endpoint=connection.host?`${connection.host}:${connection.port||22}`:"端点未记录";
    const nodeText=[node.hostname||"节点名未记录",endpoint,connection.user?`SSH ${connection.user}`:null,node.python?`Python ${node.python}`:null,node.platform||null].filter(Boolean).join(" · ");
    const devices=(item.gpu||[]).map(gpu=>{const used=numeric(gpu.memory_used_mb),total=numeric(gpu.memory_total_mb),util=numeric(gpu.utilization_percent),temp=numeric(gpu.temperature_c);return `GPU ${gpu.index??"?"} · ${gpu.name||"名称未记录"} · ${nums(util)?fmt(util)+"%":"利用率未记录"} · ${nums(used)&&nums(total)?(used/1024).toFixed(1)+"/"+(total/1024).toFixed(1)+" GiB":"显存未记录"}${nums(temp)?" · "+fmt(temp)+" °C":""}`;});
    const gpuText=devices.length?devices.join(" / "):"未发现 GPU";
    const cpu=item.cpu||{},cpuUtil=numeric(cpu.utilization_percent),cpuLoad=numeric(cpu.load_1m),cpuCores=numeric(cpu.logical_cores);
    const cpuText=`CPU · ${cpu.model||"型号未记录"}${nums(cpuCores)?" · "+fmt(cpuCores)+" 线程":""}${nums(cpuUtil)?" · "+fmt(cpuUtil,1)+"%":" · 利用率未记录"}${nums(cpuLoad)?" · load "+fmt(cpuLoad,2):""}`;
    const memory=item.memory||{},memoryUsed=numeric(memory.used_bytes),memoryTotal=numeric(memory.total_bytes);
    const memoryText=nums(memoryUsed)&&nums(memoryTotal)?`内存 ${gb(memoryUsed).replace(" GiB","")}/${gb(memoryTotal)}`:"内存未记录";
    const resourceText=[cpuText,memoryText,gpuText].join(" / ");
    const workload=nums(item.workload_count)?`${fmt(item.workload_count)} 个进程 · ${fmt(item.managed_workload_count)} 个受管`:item.workload_state||"暂不可用";
    const statusText=`${states[item.state]||item.state||"未知"} · ${compatibility[(item.compatibility||{}).state]||(item.compatibility||{}).state||"接口待确认"}${item.disabled_reason?" · "+item.disabled_reason:""}`;
    const values=[item.worker_id||"未知",nodeText,statusText,resourceText,workload,gb((item.disk||{}).free_bytes),(item.labels||[]).join(" · ")||"无标签"];
    for(const value of values){const cell=document.createElement("td");cell.textContent=value;row.append(cell);}$("worker-overview").append(row);
  }
}
function renderOverview(d){
  const catalog=orderedCatalog(d.catalog||[]);
  const statuses=catalog.map(item=>item.last_status||"UNKNOWN");
  text("overview-count",catalog.length+" 个任务");
  text("overview-running",String(catalog.filter(item=>item.connection==="fresh"&&["RUNNING","EVALUATING"].includes(item.last_status)).length));
  text("overview-queued",String(catalog.filter(item=>item.connection==="fresh"&&["QUEUED","PREPARING"].includes(item.last_status)).length));
  text("overview-terminal",String(statuses.filter(s=>["SUCCEEDED","FAILED","CANCELLED"].includes(s)).length));
  text("overview-problem",String(catalog.filter((item,index)=>["FAILED","LOST"].includes(statuses[index])||
    (item.connection==="stale"&&!["SUCCEEDED","FAILED","CANCELLED"].includes(statuses[index]))).length));
  $("task-overview").replaceChildren();
  for(const item of catalog){
    const row=document.createElement("tr"),status=item.last_status||"UNKNOWN";
    const name=document.createElement("td"),button=document.createElement("button");button.className="overview-task";button.textContent=item.name||item.run_id;
    const id=document.createElement("small");id.textContent=item.run_id;button.append(id);button.addEventListener("click",()=>selectRun(item.run_id));name.append(button);row.append(name);
    const worker=document.createElement("td");worker.textContent=item.worker||"未记录";row.append(worker);
    const type=document.createElement("td");type.textContent=kinds[item.kind]||item.kind||"未声明";row.append(type);
    const uncertain=item.connection!=="fresh"&&!['SUCCEEDED','FAILED','CANCELLED'].includes(status);
    const state=document.createElement("td"),chip=document.createElement("span");chip.className="overview-status "+(uncertain?"unknown":status.toLowerCase());chip.textContent=(uncertain?"当前未确认 · 上次：":"")+(labels[status]||status);state.append(chip);row.append(state);
    const progress=document.createElement("td");progress.textContent=catalogProgress(item);row.append(progress);
    const resource=document.createElement("td");resource.textContent=(nums(item.gpu_index)?"GPU "+item.gpu_index:"未申请 GPU")+(nums(item.memory_bytes)?" · "+gb(item.memory_bytes):"");row.append(resource);
    const duration=document.createElement("td");duration.textContent=elapsed(item.started_at||item.created_at);row.append(duration);
    const observed=document.createElement("td");observed.textContent=date(item.sample_at||item.updated_at);row.append(observed);
    $("task-overview").append(row);
  }
  const workloads=d.workloads||{state:"not_collected",items:[]}, unmanaged=(workloads.items||[]).filter(item=>!item.managed);
  text("unmanaged-count",workloads.state==="available"?unmanaged.length+" 个":workloads.state==="unsupported"?"系统不支持":"暂不可用");
  text("workload-note",workloads.state==="available"?`扫描时间 ${date(workloads.observed_at)}；仅显示脱敏身份，不读取完整命令行。${workloads.truncated?"结果已截断。":""}`:workloads.error?"扫描失败："+workloads.error:"当前从机版本尚未提供安全的进程清单。部署更新后自动启用。");
  $("unmanaged-workloads").replaceChildren();
  for(const item of unmanaged){const row=document.createElement("tr");for(const value of [item.pid,(item.launcher||"—")+(item.script?" / "+item.script:""),item.project||"—",gb(item.memory_bytes),item.state||"—"]){const cell=document.createElement("td");cell.textContent=value;row.append(cell);}$("unmanaged-workloads").append(row);}
}
function renderActiveRuns(d){
  const active=(d.catalog||[]).filter(item=>item.connection==="fresh"&&["RUNNING","EVALUATING"].includes(item.last_status||"UNKNOWN"));
  text("active-run-count",active.length+" 个活跃实验");
  text("active-run-note",active.length?"无需切换任务即可比较各 Worker 的最新训练事实；完整曲线和配置仍在下方单任务详情中。":"当前没有已观测到的运行中实验。");
  $("active-run-overview").replaceChildren();
  for(const item of active){
    const row=document.createElement("tr"),name=document.createElement("td"),button=document.createElement("button");
    button.className="overview-task";button.textContent=item.name||item.run_id;
    const id=document.createElement("small");id.textContent=item.run_id;button.append(id);
    button.addEventListener("click",()=>selectRun(item.run_id));name.append(button);row.append(name);
    for(const value of [item.worker||"未记录",catalogProgress(item),fmt(item.step),fmt(item.loss,4),nums(item.live_lr)?item.live_lr.toExponential(3):"—",date(item.sample_at)]){
      const cell=document.createElement("td");cell.textContent=value;row.append(cell);
    }
    $("active-run-overview").append(row);
  }
}
function renderWorkflows(d){
  const report=d.workflows||{state:"not_collected",studies:[]},studies=report.studies||[];
  const stageCount=studies.reduce((total,item)=>total+(item.stages||[]).length,0);
  text("workflow-count",`${studies.length} 个研究计划 · ${stageCount} 个阶段`);
  text("workflow-note",report.state==="available"?(studies.length?"读取本机工作流数据库；这里只展示事实和准入条件，不自动执行批准、停止或最终测试。":"尚未创建 Study；当前独立训练 Run 仍在上方正常监测。"):(report.error?"工作流目录读取异常："+report.error:"尚未读取工作流目录。"));
  $("workflow-stages").replaceChildren();
  const gates={dependency_failed:"上游失败",dependency_pending:"等待上游",artifact_binding_required:"等待产物",artifact_binding_unavailable:"产物连接不可用",artifact_binding_invalid:"产物清单无效",artifact_binding_changed:"产物已变化",recommendation_required:"等待科研建议",recommendation_wait:"建议等待",recommendation_stop:"建议停止",recommendation_stale:"建议证据已过期",recommendation_binding_mismatch:"建议与产物不匹配",approval_required:"等待用户批准",run_budget_exhausted:"运行预算耗尽",remote_unavailable:"从机暂不可用",study_stopped:"研究计划已停止"};
  for(const entry of studies){
    const study=entry.study||{},recommendations=entry.recommendations||[];
    for(const stage of entry.stages||[]){
      const row=document.createElement("tr"), latest=[...recommendations].reverse().find(item=>item.stage_id===stage.id);
      const binding=(stage.artifact_bindings||[]).map(item=>{const name=String(item.path||"").split(/[\\/]/).pop()||"未命名产物";return `${name} · ${String(item.sha256||"").slice(0,12)}… · ${item.source_run_id||"未知来源"}`;}).join("；")||"静态输入或尚未绑定";
      const gate=stage.block_reason?gates[stage.block_reason]||stage.block_reason:stage.status==="PENDING"?"等待调度":"准入条件满足";
      const decision=stage.decision_package, decisionPackage=decision?`${decision.readiness==="ready_for_scientific_review"?"可供科研审查":"可供故障审查"} · ${decision.checkpoint_count||0} 个 checkpoint · ${String(decision.package_sha256||"").slice(0,12)}…`:["SUCCEEDED","FAILED","CANCELLED"].includes(stage.status)?"尚未生成":"等待阶段终态";
      const link=stage.run_link||{}, runLink=stage.run_id?`${stage.run_id}\n${link.kind==="adopted"?`已纳管 · ${link.adopted_by||"未记录操作者"}${link.metadata_upgrade?" · 兼容旧任务元数据":""}`:"由工作流提交"}`:"尚未创建 Run";
      const values=[`${study.name||study.id}\n${String(study.plan_sha256||"").slice(0,12)}…`,`${stage.name||stage.id}\n${kinds[stage.kind]||stage.kind||"未声明"}`,labels[stage.status]||stage.status||"未知",gate+(latest?` · ${latest.decision}`:""),binding,decisionPackage,runLink];
      for(const value of values){const cell=document.createElement("td");cell.textContent=value;row.append(cell);}
      $("workflow-stages").append(row);
    }
  }
}
function render(d){
  renderFleet(d);
  renderWorkflows(d);
  renderConfiguration(d);
  renderActiveRuns(d);
  renderOverview(d);
  const dashboard=d.dashboard_runtime||{},focus=d.collector_focus||{};
  text("dashboard-version",dashboard.loaded_version?`v${dashboard.loaded_version} · 启动 ${date(dashboard.started_at)} · 源码 ${(dashboard.loaded_source_sha256||'未知').slice(0,12)}`:"运行版本尚未上报");
  text("dashboard-runtime-note",dashboard.restart_required?"本地源码已变化：当前进程仍使用启动时版本，需要重启本地观测台；无需重启训练。":
    dashboard.source_state==='unavailable'?"源码版本检查暂不可用；不能确认本地文件与运行进程一致。":"运行进程版本独立于从机版本；此处不执行自动重启。");
  $("dashboard-runtime-note").className=dashboard.restart_required?'notice warning':'muted';
  text("collector-focus",focus.run_id?`优先采集：${focus.worker||'未记录从机'} · ${focus.run_id}${focus.run_id!==d.run_id?'（另一标签页选择；当前详情仍保留你的选择）':''}。最近显式选择生效，普通刷新不抢占。`:"等待采集焦点；任务选择只改变本地读取优先级，不修改远端任务。");
  const compatibility=d.worker_compatibility||{}, protocol=compatibility.protocol||{}, software=compatibility.software||{};
  const compatibilityText={compatible:`兼容 · 协议 ${protocol.major??"—"}.${protocol.minor??"—"} · v${software.version||"未知"}`,
    legacy:"旧版从机 · 能力未声明",incompatible:"接口不兼容 · 禁止新操作",unavailable:"协商暂不可用",not_collected:"等待协商"};
  text("worker-compatibility",compatibilityText[compatibility.state]||"状态未知");
  const task=(d.result||{}).task||d.task||{}, kind=task.kind||"unspecified";
  const catalog=orderedCatalog(d.catalog||[{run_id:d.run_id,name:d.name,kind}]);
  $("task-select").replaceChildren();
  for(const item of catalog){const option=document.createElement("option");option.value=item.run_id;option.textContent=`${labels[item.last_status]||'状态未确认'} · ${item.worker||"未知 Worker"} · ${kinds[item.kind]||item.kind} · ${item.name} · ${item.run_id.slice(-8)}`;$("task-select").append(option);}
  $("task-select").value=d.run_id;
  const selected=catalog.find(item=>item.run_id===d.run_id)||{};
  const live=preferredActive(d), other=live&&live.run_id!==d.run_id;
  $("follow-live").disabled=!live;
  text("follow-live",followLive?'正在跟随运行中任务':'跟随运行中任务');
  text("selection-note",other?`当前详情不是活跃任务。${live.worker||'未知从机'} 正在运行 ${live.name||live.run_id}；点击“跟随运行中任务”查看进度、配置与 loss。`:
    followLive?'自动跟随已启用；当前任务结束后优先显示同服务器的续训任务。':'当前为手动查看模式；刷新不会改变选择，可点击“跟随运行中任务”返回实时训练。');
  $("selection-note").className=other?'notice warning':'muted';
  text("side-worker",selected.worker||"未记录");
  text("detail-worker",String(selected.worker||"WORKER").toUpperCase()+" / LIVE OBSERVATION");
  text("task-kind",kinds[kind]||kind);
  text("task-source",task.source_run_id||"无关联来源记录");
  text("task-checkpoint",task.checkpoint?`${task.checkpoint.path} · SHA256 ${task.checkpoint.sha256}`:"未声明测试输入权重（不等于没有恢复点）");
  text("task-inputs",(task.inputs||[]).map(x=>`${x.path} · SHA256 ${x.sha256}`).join("\n")||"未声明任务输入清单；已有证据绑定保持独立");
  text("catalog-note",d.catalog_error?"本地任务目录读取异常："+d.catalog_error:"关注任务单独采集；其他活跃任务优先、历史任务轮询补齐。只列出本机登记的近期任务，慢查询时以各自观测时间为准。");
  $("metric-values").replaceChildren();
  const metrics=(d.result||{}).metrics||Object.fromEntries(Object.entries((d.latest||{}).metrics||{}).map(([key,value])=>[key,{value}]));
  for(const [key,item] of Object.entries(metrics)){const row=document.createElement("tr");for(const value of [key,fmt(item.value,6),phases[item.phase]||item.phase||"—",date(item.sample_at)]){const cell=document.createElement("td");cell.textContent=value;row.append(cell);}$("metric-values").append(row);}
  text("metrics-note",Object.keys(metrics).length?"最新上报的标量指标；不是最终结果验收，也不保证每项在当前阶段重新计算。":"此任务尚无通用指标采样；旧缓存需等待新采样。");
  const inventory=(d.result||{}).artifacts||{state:"not_collected",items:[]};
  $("artifact-list").replaceChildren();
  for(const item of inventory.items||[]){const row=document.createElement("li");row.textContent=`${item.path} · ${fmt(item.size)} bytes${item.sha256?" · SHA256 "+item.sha256:" · 未提供哈希"}`;$("artifact-list").append(row);}
  text("artifact-note",({collected:"已获取产物清单（未下载文件）",truncated:"仅显示前200项产物",unavailable:"产物清单暂不可用",requires_explicit_collection:"未声明非递归产物路径，需要显式采集",not_collected:"结束后采集产物清单"}[inventory.state]||inventory.state)+(inventory.error?" · "+inventory.error:""));
  const oldMetric=$("metric").value;
  const metricKeys=new Set(Object.keys(metrics));for(const point of d.points||[])for(const key of Object.keys(point.metrics||{}))metricKeys.add(key);
  $("metric").replaceChildren();
  for(const [key,label] of [["loss","训练 Loss"],["val_loss","验证 Loss"],...[...metricKeys].sort().map(k=>["metric:"+k,k])]){const option=document.createElement("option");option.value=key;option.textContent=label;$("metric").append(option);}
  $("metric").value=oldMetric&&(!oldMetric.startsWith("metric:")||metricKeys.has(oldMetric.slice(7)))?oldMetric:kind==="test"&&metricKeys.size?"metric:"+[...metricKeys][0]:"loss";
  const p=d.latest||{}, good=d.connection==="fresh", status=p.status||"UNKNOWN", terminal=["SUCCEEDED","FAILED","CANCELLED"].includes(status);
  text("run-name",d.name);text("run-id",d.run_id);text("side-status",labels[status]||status);
  badge("state",(good?"":"上次状态：")+(labels[status]||status),!good?"neutral":status==="FAILED"?"danger":status==="CANCELLED"?"warning":"good");
  badge("connection",good?"● 观测正常":d.catching_up?"◌ 补齐历史中":"● 观测未确认",good?"good":"warning");
  const errors=Object.entries(d.errors||{}).map(([key,value])=>`${key}: ${value}`);
  $("notice").className="notice"+(good?"":" warning");
  text("notice",errors.length?"连接或采集异常；保留最后记录，不将失联视为训练失败。 "+errors.join(" · "):d.catching_up?"正在分批补齐历史采样，显示的进度可能滞后。每轮只取200条，避免一次性拉取大量数据。":!good?"监测数据暂未确认新鲜，请以采样时间为准；不会因此改变训练状态。":"只读观测 · 从机持续执行训练，页面仅显示已有监测记录。");
  text("phase",phases[p.phase]||p.phase||"等待阶段信息");text("observed","训练采样 "+date(p.at));
  const e=nums(p.epoch)?Math.min(d.epochs||Infinity,p.epoch+1):null;
  text("epoch",fmt(status==="SUCCEEDED"?d.epochs:e)+" / "+fmt(d.epochs));
  $("epoch-progress").value=nums(d.epochs)&&d.epochs>0?(status==="SUCCEEDED"?100:Math.max(0,Math.min(100,100*(p.epoch||0)/d.epochs))):0;
  text("epoch-note",terminal?"执行已结束，结果以任务状态为准":"进度条按已完成轮数计 · 不估算 ETA");
  text("epoch-label","EPOCH 进度");
  if(kind!=="train"&&kind!=="unspecified"){
    const progress=(d.result||{}).progress||{}, done=progress.completed,total=progress.total;
    text("epoch-label","任务进度（上报单位）");text("epoch",fmt(done)+" / "+fmt(total));
    $("epoch-progress").value=nums(done)&&nums(total)&&total>0?Math.min(100,Math.max(0,done/total*100)):0;
    text("epoch-note",nums(done)&&nums(total)?"实际已处理 / 总量 · 未上报时不估算":"等待任务上报 completed / total");
  }
  text("updates",fmt(p.updates));text("step","全局步数 "+fmt(p.step));text("loss",fmt(p.loss,4));
  text("progress-state",!good?"待确认":({advancing:"持续推进",suspected_stall:"疑似停滞",finished:"执行结束",unreported:"暂无进度"}[p.progress_state]||"等待采样"));
  if(d.log_metrics){
    const lm=d.log_metrics;
    if(lm.epoch_basis==="reported_fullfit"){
      const metrics=p.metrics||{}, completed=metrics['train/completed_epochs'];
      text("epoch-note",`完整 fit 逐轮训练 · 已完成 ${fmt(completed)} 轮 · 本轮 ${fmt(metrics['train/epoch_batch'])} / ${fmt(metrics['train/epoch_batches'])} batch`);
      if(nums(completed)&&nums(d.epochs))$("epoch-progress").value=100*completed/d.epochs;
    }else text("epoch-note","名义 epoch 由已记录更新步数换算；时序采样不是完整遍历");
    text("observed","指标来自训练 JSON 日志 · 最近采集 "+date(lm.collected_at)+(lm.fresh?"":" · 指标可能过期"));
    text("progress-state",lm.error?"日志采集异常":lm.has_more?"补齐历史日志":lm.fresh?"已采集训练指标":"等待新指标");
    text("metrics-note","loss、step 和学习率来自训练记录；时间为本机采集时间，横轴为全局 batch 步数，优化器实际更新次数单独显示；不是验证精度。"+(lm.error?" 采集错误："+lm.error:""));
  }
  text("heartbeat","采样距今 "+fmt(d.sample_age_seconds)+" 秒");
  const m=d.monitor||{}, health=m.health||{}, gpu=(health.gpu||[]).find(g=>String(g.index)==="0")||{},cpu=health.cpu||{},memory=health.memory||{};
  const cpuCores=numeric(cpu.logical_cores),cpuUtil=numeric(cpu.utilization_percent),cpuLoad=numeric(cpu.load_1m);
  text("gpu-name",gpu.name||cpu.model||"计算节点 · 尚无资源记录");
  text("compute-chip",gpu.name?"CPU + GPU":"CPU");
  text("side-gpu",[cpu.model?(cpu.model+(nums(cpuCores)?" · "+fmt(cpuCores)+" 线程":"")):null,gpu.name?(gpu.name+" · GPU "+(gpu.index??"0")):null].filter(Boolean).join(" / ")||"尚无资源记录");
  const util=numeric(gpu.utilization_percent), used=numeric(gpu.memory_used_mb), total=numeric(gpu.memory_total_mb);
  text("gpu-util",nums(util)?fmt(util)+"%":"—");text("gpu-memory",nums(used)&&nums(total)?(used/1024).toFixed(1)+" / "+(total/1024).toFixed(1)+" GiB":"—");
  $("gpu-progress").value=total>0?Math.min(100,Math.max(0,used/total*100)):0;
  text("cpu-util",nums(cpuUtil)?fmt(cpuUtil,1)+"%":"—");text("cpu-name",cpu.model||"—");
  text("cpu-load",nums(cpuLoad)?fmt(cpuLoad,2):"—");text("cpu-cores",nums(cpuCores)?fmt(cpuCores):"—");
  const memoryUsed=numeric(memory.used_bytes),memoryTotal=numeric(memory.total_bytes),memoryUtil=numeric(memory.utilization_percent);
  text("system-memory",nums(memoryUsed)&&nums(memoryTotal)?gb(memoryUsed).replace(" GiB","")+" / "+gb(memoryTotal):"—");
  $("memory-progress").value=nums(memoryUtil)?Math.min(100,Math.max(0,memoryUtil)):memoryTotal>0?Math.min(100,Math.max(0,memoryUsed/memoryTotal*100)):0;
  text("gpu-temp",gpu.temperature_c==null?"—":String(gpu.temperature_c)+" °C");text("process-memory",gb(p.memory_bytes));
  text("disk-free",gb((health.disk||{}).free_bytes));text("gpu-time","资源采样 "+date(m.observed_at)+(good?"":" · 可能过期"));
  const c=p.checkpoint||{}, ev=p.evidence||{};
  const ck={committed:"已提交",reported_committed:"训练端已报告提交",saving:"保存中",waiting:"等待首次保存",failed:"保存失败"};
  badge("checkpoint-state",ck[c.state]||"尚无记录",c.state==="failed"?"danger":c.state==="committed"?"good":"neutral");
  text("checkpoint-step",fmt(c.step));text("checkpoint-time",date(c.created_at));
  text("checkpoint-safe",c.state==="reported_committed"?"已读取恢复点收据；恢复前需校验文件":c.safe_to_resume===true?"最近点可重放恢复":c.safe_to_resume===false?"不可直接恢复，需回退安全点":"尚未报告");
  const recovery=d.recovery||{};
  text("recovery-log-step",fmt(recovery.logged_step));
  text("recovery-progress-step",fmt(recovery.progress_step));
  text("recovery-checkpoint-step",fmt(recovery.checkpoint_step));
  text("recovery-replay",nums(recovery.replay_steps)?"截至已观测进展需重算 "+fmt(recovery.replay_steps)+" 步（非实际断电丢失上界）":"未知 · 无可比较的同口径记录");
  const recoveryLabels={reported_only:"训练器报告，尚未验证恢复；恢复前需校验文件",verified:"worker 报告可恢复（非实际恢复测试）",not_resumable:"不可直接恢复，需选择安全点",unknown:"恢复能力未知"};
  text("recovery-note",(recoveryLabels[recovery.resume_capability]||"恢复能力未知")+(recovery.checkpoint_source?" · 断点来源："+recovery.checkpoint_source:"")+"。日志步数不等于完整断点；本页不触发续训。");
  const active=(d.alerts||[]).filter(a=>!a.resolved_at);
  text("evidence",evidenceStatus(ev,active));
  text("rotations",fmt(ev.rotations_completed)+(ev.rotations_completed===0?" · 尚未完成全轮":""));
  badge("alert-count",String(active.length)+" 项",active.length?"warning":"neutral");
  $("alerts").replaceChildren();
  const alertNames={run_failed:"训练失败 / 中断",suspected_stall:"训练进度疑似停滞",disk_low:"磁盘空间不足",evidence_drift:"旧版证据变化（范围无法判定）",evidence_execution_integrity:"执行输入完整性违规",evidence_deployment_changed:"部署目录已更新（执行快照独立）",evidence_observer_warning:"证据观测器降级",evidence_monitor_failed:"证据观察器异常",checkpoint_failed:"检查点保存失败",checkpoint_overdue:"检查点更新超期",observation_failed:"从机观测失败"};
  if(!active.length){const p=document.createElement("p");p.className="clear-alert";p.textContent=good?"✓ 当前无未恢复告警":"尚无未恢复告警记录，连接状态待确认";$("alerts").append(p);}
  for(const a of active){const item=document.createElement("div");item.className="alert-item";item.textContent=a.compatibility==="legacy_snapshot_verification_race"?"旧观测器读取竞争（未证明执行快照变化）":alertNames[a.kind]||a.kind;const detail=document.createElement("small");detail.textContent=date(a.opened_at)+(a.error?" · "+a.error:"");item.append(detail);$("alerts").append(item);}
  text("history-note",d.downsampled?"较早历史已抽稀以控制本机内存；曲线不是逐批全量记录。":"保留已同步的监测采样；断线后会从本机游标继续补齐。");
  text("refresh-note",`主任务采集间隔 ${d.interval_seconds} 秒 · 历史任务轮询 · 页面每10秒读取本地缓存`);
  chart();
}
function chart(){
  const canvas=$("chart"), ctx=canvas.getContext("2d"), w=canvas.clientWidth, h=canvas.clientHeight, ratio=window.devicePixelRatio||1;
  canvas.width=w*ratio;canvas.height=h*ratio;ctx.scale(ratio,ratio);ctx.clearRect(0,0,w,h);
  const key=$("metric").value, generic=key.startsWith("metric:"), title=generic?key.slice(7):key==="loss"?"训练 Loss":"验证 Loss";text("series-name",title);
  let points=((data||{}).points||[]).map(p=>({...p,step:nums(p.step)?p.step:(p.metrics||{})["progress/completed"],value:generic?(p.metrics||{})[key.slice(7)]:p[key]})).filter(p=>nums(p.step)&&nums(p.value)&&(generic?(!title.startsWith("val/")||["validation","validation_finished","finished"].includes(p.phase)):key==="loss"?p.phase==="train":["validation","validation_finished","finished"].includes(p.phase)));
  // One rendered point per step; no repeated flat lines from unchanged worker samples.
  points=Array.from(new Map(points.map(p=>[p.step,p])).values()).sort((a,b)=>a.step-b.step);
  text("point-count",fmt(points.length)+" 个有效采样步");$("chart-empty").hidden=points.length>0;$("chart-empty").style.display=points.length?"none":"grid";
  canvas.setAttribute("aria-label",`${title}，${points.length}个采样步，横轴为全局步数`);
  if(!points.length)return;
  if(points.length>1400){const stride=Math.ceil(points.length/1400), last=points[points.length-1];points=points.filter((_,i)=>i%stride===0);if(points[points.length-1]!==last)points.push(last);}
  const left=48,right=w-12,top=20,bottom=h-30;
  let xmin=points[0].step,xmax=points[points.length-1].step,ymin=Math.min(...points.map(p=>p.value)),ymax=Math.max(...points.map(p=>p.value));
  if(xmax===xmin)xmax=xmin+1;const pad=Math.max((ymax-ymin)*.12,.01);ymin-=pad;ymax+=pad;
  const x=v=>left+(v-xmin)/(xmax-xmin)*(right-left), y=v=>bottom-(v-ymin)/(ymax-ymin)*(bottom-top);
  ctx.font="10px Segoe UI";ctx.lineWidth=1;
  for(let i=0;i<5;i++){const val=ymin+(ymax-ymin)*i/4,yy=y(val);ctx.strokeStyle="#e7edf1";ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(right,yy);ctx.stroke();ctx.fillStyle="#82929e";ctx.textAlign="right";ctx.fillText(val.toFixed(2),left-8,yy+3);}
  for(let i=0;i<4;i++){const val=xmin+(xmax-xmin)*i/3;ctx.textAlign=i===0?"left":i===3?"right":"center";ctx.fillText(Math.round(val).toLocaleString(),x(val),h-10);}
  ctx.strokeStyle="#326cca";ctx.lineWidth=1.8;ctx.beginPath();let previous=null;
  for(const p of points){if(!previous||Date.parse(p.at)-Date.parse(previous.at)>90000)ctx.moveTo(x(p.step),y(p.value));else ctx.lineTo(x(p.step),y(p.value));previous=p;}ctx.stroke();
  const last=points[points.length-1];ctx.fillStyle="#087e70";ctx.beginPath();ctx.arc(x(last.step),y(last.value),3,0,Math.PI*2);ctx.fill();
}
async function refresh(){
  if(busy)return;busy=true;$("refresh").disabled=true;
  const version=requestVersion;
  try{if(!token)throw new Error("缺少本机访问令牌，请从启动脚本提供的私有链接打开。");const response=await fetch("/api/state"+(selectedRun?"?run_id="+encodeURIComponent(selectedRun):""),{headers:{Authorization:"Bearer "+token},cache:"no-store",signal:AbortSignal.timeout(8000)});if(!response.ok)throw new Error("看板访问失败：HTTP "+response.status);const value=await response.json();if(version===requestVersion&&acceptSelection(value)){
    if(focusPending&&selectedRun){
      const focusedRun=selectedRun;
      const preferenceBody={run_id:focusedRun};if(focusExpected)preferenceBody.expected_run_id=focusExpected;
      const preference=await fetch('/api/focus',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify(preferenceBody),cache:'no-store',signal:AbortSignal.timeout(8000)});
      if(!preference.ok)throw new Error('采集焦点切换失败：HTTP '+preference.status);
      const focus=await preference.json();
      if(version!==requestVersion)return;
      focusPending=false;value.collector_focus=focus;
    }
    if(version===requestVersion){data=value;render(data);}
  }}
  catch(error){if(version!==requestVersion)return;badge("connection","本机看板连接异常","warning");$("notice").className="notice warning";text("notice",String(error.message)+" 保留上次画面，不能据此判断训练中断。");text("progress-state","待确认");if(data){data.connection="stale";badge("state","上次状态："+(labels[(data.latest||{}).status]||"未知"),"neutral");text("gpu-time","资源采样 "+date((data.monitor||{}).observed_at)+" · 连接中断，可能过期");}}
  finally{busy=false;$("refresh").disabled=false;if(version!==requestVersion)refresh();}
}
$("task-select").addEventListener("change",()=>{text("notice","正在切换任务；下方仍为上一个任务的记录。");selectRun($("task-select").value);});
$("follow-live").addEventListener("click",followRunning);
$("export-result").addEventListener("click",()=>{if(!data||!data.result)return;const file=new Blob([JSON.stringify(exportSnapshot(data),null,2)],{type:"application/json"}),url=URL.createObjectURL(file),link=document.createElement("a");link.href=url;link.download=data.run_id+"-result.json";link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
$("refresh").addEventListener("click",refresh);$("metric").addEventListener("change",chart);window.addEventListener("resize",chart);refresh();setInterval(refresh,10000);
