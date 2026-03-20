// ===== B2B 计划与作业单 =====
var API_URL = "https://api.ck91888.cn";
var KEY_STORAGE = "b2b_plan_k_v1";
var B2B_EARLIEST_DAY = "2020-01-01";

// ===== 防重提交 =====
var _submitting = false;
function genRequestId(){ return "rid-" + Date.now() + "-" + Math.random().toString(36).slice(2,8); }
var _planPendingRid = null;
var _woPendingRid = null;
var _foPendingRid = null;
var _scPendingRid = null;
function getOrCreateRid(kind){
  if(kind==="plan"){ if(!_planPendingRid) _planPendingRid=genRequestId(); return _planPendingRid; }
  if(kind==="wo"){ if(!_woPendingRid) _woPendingRid=genRequestId(); return _woPendingRid; }
  if(kind==="fo"){ if(!_foPendingRid) _foPendingRid=genRequestId(); return _foPendingRid; }
  if(kind==="sc"){ if(!_scPendingRid) _scPendingRid=genRequestId(); return _scPendingRid; }
  return genRequestId();
}
function clearRid(kind){
  if(kind==="plan") _planPendingRid=null;
  else if(kind==="wo") _woPendingRid=null;
  else if(kind==="fo") _foPendingRid=null;
  else if(kind==="sc") _scPendingRid=null;
}

// ===== 工具函数 =====
function esc(s){ return String(s||"").replace(/[&<>"']/g, function(c){ return({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]; }); }
function pad2(n){ return String(n).padStart(2,"0"); }
function getKey(){ try{ return localStorage.getItem(KEY_STORAGE)||""; }catch(e){ return ""; } }
function setKey(k){ try{ localStorage.setItem(KEY_STORAGE, k); }catch(e){} }
function clearKey(){ try{ localStorage.removeItem(KEY_STORAGE); }catch(e){} }

function kstToday(){
  var d = new Date(Date.now() + 9*3600*1000);
  return d.getUTCFullYear() + "-" + pad2(d.getUTCMonth()+1) + "-" + pad2(d.getUTCDate());
}
function kstTomorrow(){
  var d = new Date(Date.now() + 9*3600*1000 + 24*3600*1000);
  return d.getUTCFullYear() + "-" + pad2(d.getUTCMonth()+1) + "-" + pad2(d.getUTCDate());
}
function kstYesterday(){
  var d = new Date(Date.now() + 9*3600*1000 - 24*3600*1000);
  return d.getUTCFullYear() + "-" + pad2(d.getUTCMonth()+1) + "-" + pad2(d.getUTCDate());
}

async function fetchApi(params){
  params.k = getKey();
  try{
    var res = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params)
    });
    return await res.json();
  }catch(e){ return { ok:false, error:"network: " + e }; }
}

// ===== 中文显示映射 =====
var PLAN_STATUS_LABEL = {
  pending: "未到货", arrived: "已到货", processing: "操作中",
  completed: "已完成", abnormal: "异常", cancelled: "已作废"
};
var BIZ_TYPE_LABEL = {
  b2c: "B2C", b2b: "B2B", inventory_op: "库存操作", return_op: "退件操作",
  b2c_inbound: "B2C入库", b2b_inbound: "B2B入库", direct_transfer: "直接转发", other: "其他"
};
var BIZ_TYPE_NEW_KEYS = ["b2c","b2b","inventory_op","return_op"];
function bizBadge(bt){
  var label = BIZ_TYPE_LABEL[bt] || bt || "未知";
  var cls = BIZ_TYPE_NEW_KEYS.indexOf(bt) >= 0 ? "biz biz-" + bt : "biz biz-legacy";
  return '<span class="'+cls+'">'+esc(label)+'</span>';
}
var WO_STATUS_LABEL = {
  draft: "草稿", issued: "操作中", working: "操作中", completed: "已完成", cancelled: "已取消"
};
// 状态流转按钮文案（区别于显示文案）
var WO_STATUS_BTN_LABEL = {
  issued: "下发", completed: "完成", cancelled: "取消"
};
var DETAIL_MODE_LABEL = { sku_based: "按SKU", carton_based: "按箱" };
// operation_mode / outbound_mode 直接存中文，显示时兜底
function modeDisplay(val){ return val || "(旧单未定义)"; }

var PLAN_NEXT_STATUS = {
  pending: ["arrived","cancelled"],
  arrived: ["processing","abnormal","cancelled"],
  processing: ["completed","abnormal"],
  completed: [],
  abnormal: ["processing","cancelled"],
  cancelled: []
};
// 可编辑的状态（非终态）
var PLAN_EDITABLE = { pending:1, arrived:1, processing:1, abnormal:1 };

var PLAN_STATUS_PRIORITY = { abnormal:0, processing:1, arrived:2, pending:3, completed:4, cancelled:5 };
var WO_STATUS_PRIORITY = { working:0, issued:1, draft:2, completed:3, cancelled:4 };
var PLAN_INCOMPLETE_STATUS = { pending:1, arrived:1, processing:1, abnormal:1 };
var WO_INCOMPLETE_STATUS = { draft:1, issued:1, working:1 };

// ===== 登录 =====
function doLogin(){
  var k = document.getElementById("loginKey").value.trim();
  if(!k){ document.getElementById("loginErr").textContent = "请输入口令"; return; }
  setKey(k);
  document.getElementById("loginErr").textContent = "";
  fetchApi({ action:"b2b_plan_list", start_day: kstToday(), end_day: kstToday() }).then(function(res){
    if(res && res.ok){
      showMain();
    } else {
      clearKey();
      document.getElementById("loginErr").textContent = "口令错误或网络异常";
    }
  });
}
function doLogout(){
  clearKey();
  document.getElementById("v-main").style.display = "none";
  document.getElementById("v-login").style.display = "";
}
function showMain(){
  document.getElementById("v-login").style.display = "none";
  document.getElementById("v-main").style.display = "";
  goHome();
}

// ===== 视图切换 =====
var ALL_VIEWS = ["v-home","v-plan_create","v-wo_create","v-plan_list","v-plan_detail","v-wo_list","v-wo_detail","v-fo_create","v-fo_list","v-fo_detail","v-sc_create","v-sc_list","v-sc_detail","v-doc_list","v-wave_list"];
var _currentViewName = "home";
function goView(name, opts){
  ALL_VIEWS.forEach(function(v){ document.getElementById(v).style.display = (v === "v-" + name) ? "" : "none"; });
  _currentViewName = name;
  if(opts && opts.skipInit) return;
  if(name === "plan_list") initPlanList();
  if(name === "wo_list") initWoList();
  if(name === "fo_list") initFoList();
  if(name === "sc_list") initScList();
  if(name === "doc_list") initDocList();
  if(name === "wave_list") initWaveList();
}

// ===== 导航栈 =====
var _navStack = [];
var NAV_STACK_MAX = 20;

var _currentDetailId = null;

function navCaptureState(){
  var v = _currentViewName;
  var entry = { view: v, tab: _currentTab };
  // 详情页：记住当前正在看的 ID
  if(v === "plan_detail" || v === "wo_detail" || v === "fo_detail" || v === "sc_detail"){
    entry.detailId = _currentDetailId;
  }
  if(v === "plan_list"){
    entry.state = {
      "pl-start": document.getElementById("pl-start").value,
      "pl-end": document.getElementById("pl-end").value,
      scope: _currentPlanScope
    };
  } else if(v === "wo_list"){
    entry.state = {
      "wl-start": document.getElementById("wl-start").value,
      "wl-end": document.getElementById("wl-end").value,
      scope: _currentWoScope
    };
  } else if(v === "fo_list"){
    entry.state = {
      "fl-start": document.getElementById("fl-start").value,
      "fl-end": document.getElementById("fl-end").value
    };
  } else if(v === "sc_list"){
    entry.state = {
      "scl-start": document.getElementById("scl-start").value,
      "scl-end": document.getElementById("scl-end").value
    };
  } else if(v === "doc_list"){
    entry.state = {
      "dl-start": document.getElementById("dl-start").value,
      "dl-end": document.getElementById("dl-end").value,
      "dl-kind": document.getElementById("dl-kind").value,
      "dl-keyword": document.getElementById("dl-keyword").value,
      viewMode: _docViewMode,
      page: _docPage
    };
  } else if(v === "wave_list"){
    entry.state = {
      "hw-start": document.getElementById("hw-start").value,
      "hw-end": document.getElementById("hw-end").value,
      "hw-biz": document.getElementById("hw-biz").value,
      "hw-task": document.getElementById("hw-task").value,
      "hw-keyword": document.getElementById("hw-keyword").value
    };
  }
  return entry;
}

function navPush(){
  _navStack.push(navCaptureState());
  if(_navStack.length > NAV_STACK_MAX) _navStack.shift();
}

// 编辑保存成功后，弹掉栈顶的 detail 快照，避免"返回回到同一详情页"
function navDropTopIf(viewName){
  if(_navStack.length > 0 && _navStack[_navStack.length - 1].view === viewName){
    _navStack.pop();
  }
}

function navRestoreState(entry){
  var v = entry.view;
  var st = entry.state || {};
  // 切换视图但跳过 init
  goView(v, { skipInit: true });
  // 恢复 tab 高亮
  if(entry.tab){
    _currentTab = entry.tab;
    var btns = document.querySelectorAll("#mainTabBar button");
    var tabOrder = ["home","plan","wo","fo","sc","wave"];
    for(var i = 0; i < btns.length && i < tabOrder.length; i++){
      btns[i].className = (tabOrder[i] === entry.tab) ? "tab-active" : "";
    }
  }

  if(v === "plan_list"){
    document.getElementById("pl-start").value = st["pl-start"] || "";
    document.getElementById("pl-end").value = st["pl-end"] || "";
    _currentPlanScope = st.scope || "unfinished";
    reloadCurrentPlanList();
  } else if(v === "wo_list"){
    document.getElementById("wl-start").value = st["wl-start"] || "";
    document.getElementById("wl-end").value = st["wl-end"] || "";
    _currentWoScope = st.scope || "today";
    // 恢复标题
    var titleEl = document.getElementById("wl-title");
    if(_currentWoScope === "next3") titleEl.textContent = "未来三天出库作业单";
    else if(_currentWoScope === "overdue") titleEl.textContent = "逾期未完成出库作业单";
    else if(_currentWoScope === "today") titleEl.textContent = "今日出库作业单";
    else titleEl.textContent = "出库作业单列表";
    reloadCurrentWoList();
  } else if(v === "fo_list"){
    document.getElementById("fl-start").value = st["fl-start"] || "";
    document.getElementById("fl-end").value = st["fl-end"] || "";
    loadFoList();
  } else if(v === "sc_list"){
    document.getElementById("scl-start").value = st["scl-start"] || "";
    document.getElementById("scl-end").value = st["scl-end"] || "";
    loadScList();
  } else if(v === "doc_list"){
    document.getElementById("dl-start").value = st["dl-start"] || "";
    document.getElementById("dl-end").value = st["dl-end"] || "";
    document.getElementById("dl-kind").value = st["dl-kind"] || "";
    document.getElementById("dl-keyword").value = st["dl-keyword"] || "";
    _docViewMode = st.viewMode || "summary";
    _docPage = st.page || 1;
    // 更新 toggle 按钮
    var btns2 = document.querySelectorAll("#docViewToggle button");
    for(var j=0;j<btns2.length;j++) btns2[j].className = "";
    if(_docViewMode === "summary") btns2[0].className = "vt-active";
    else if(_docViewMode === "session") btns2[1].className = "vt-active";
    loadDocList();
  } else if(v === "wave_list"){
    document.getElementById("hw-start").value = st["hw-start"] || "";
    document.getElementById("hw-end").value = st["hw-end"] || "";
    document.getElementById("hw-biz").value = st["hw-biz"] || "";
    document.getElementById("hw-task").value = st["hw-task"] || "";
    document.getElementById("hw-keyword").value = st["hw-keyword"] || "";
    loadWaveList();
  } else if(v === "plan_detail" && entry.detailId){
    goPlanDetail(entry.detailId, true);
  } else if(v === "wo_detail" && entry.detailId){
    goWoDetail(entry.detailId, true);
  } else if(v === "fo_detail" && entry.detailId){
    goFoDetail(entry.detailId, true);
  } else if(v === "sc_detail" && entry.detailId){
    goScDetail(entry.detailId, true);
  } else if(v === "home"){
    loadHome();
  }
}

// 返回按钮的默认列表页映射
var TAB_DEFAULT_LIST = {
  plan: "plan_list", wo: "wo_list", fo: "fo_list", sc: "sc_list", wave: "doc_list"
};

function goBack(){
  if(_navStack.length > 0){
    navRestoreState(_navStack.pop());
  } else {
    // 栈空 → 回当前 tab 默认列表页
    var defaultView = TAB_DEFAULT_LIST[_currentTab];
    if(defaultView) goView(defaultView);
    else goHome();
  }
}

// ===== Tab 导航 =====
var _currentTab = "home";
function goTab(tab){
  _navStack = [];
  _currentTab = tab;
  var btns = document.querySelectorAll("#mainTabBar button");
  var tabOrder = ["home","plan","wo","fo","sc","wave"];
  for(var i = 0; i < btns.length && i < tabOrder.length; i++){
    btns[i].className = (tabOrder[i] === tab) ? "tab-active" : "";
  }
  if(tab === "home"){ goHome(); return; }
  if(tab === "plan"){ _planListScope = "unfinished"; goView("plan_list"); return; }
  if(tab === "wo"){ _woListScope = "next3"; goView("wo_list"); return; }
  if(tab === "fo"){ goView("fo_list"); return; }
  if(tab === "sc"){ goView("sc_list"); return; }
  if(tab === "wave"){ goView("doc_list"); return; }
}

function goHome(){
  goView("home");
  loadHome();
}

// ===== 导航入口：新建（清空编辑状态） =====
function goNewPlan(){
  navPush();
  _editingPlanId = null;
  goView("plan_create");
  initPlanForm(null);
}
function goNewWo(){
  navPush();
  _editingWoId = null;
  goView("wo_create");
  initWoCreate(null);
}
function goNewSc(){
  navPush();
  goView("sc_create");
  initScCreate();
}
function goNewFo(fromPlan){
  navPush();
  _editingFoId = null;
  _foSourcePlanId = null;
  goView("fo_create");
  initFoForm(null, fromPlan||null);
}

function kstDayOffset(days){
  var d = new Date(Date.now() + 9*3600*1000 + days*24*3600*1000);
  return d.getUTCFullYear() + "-" + pad2(d.getUTCMonth()+1) + "-" + pad2(d.getUTCDate());
}

// ===== 首页 =====
var _planListScope = "unfinished";
var _woListScope = "today";
var _currentPlanScope = "unfinished";
var _currentWoScope = "today";

function loadHome(){
  var today = kstToday();
  var day1 = kstDayOffset(1);
  var day3 = kstDayOffset(3);
  document.getElementById("todayPill").textContent = today;

  ["ip-unfinished","ip-next3","wo-today","sc-today"].forEach(function(p){
    document.getElementById(p + "-count").textContent = "--";
    document.getElementById(p + "-body").innerHTML = '<div class="q-empty">加载中...</div>';
  });
  document.getElementById("home-alerts").innerHTML = "";

  Promise.all([
    fetchApi({ action:"b2b_plan_list", start_day:B2B_EARLIEST_DAY, end_day:today }),
    fetchApi({ action:"b2b_plan_list", start_day:day1, end_day:day3 }),
    fetchApi({ action:"b2b_wo_list", start_day:today, end_day:day3 }),
    fetchApi({ action:"b2b_scan_batch_list", start_day:today, end_day:today })
  ]).then(function(results){
    var allPlans = (results[0] && results[0].ok) ? (results[0].plans||[]) : [];
    var next3Plans = (results[1] && results[1].ok) ? (results[1].plans||[]) : [];
    var wos = (results[2] && results[2].ok) ? (results[2].workorders||[]) : [];
    var scs = (results[3] && results[3].ok) ? (results[3].batches||[]) : [];

    // 未完成入库计划（plan_day<=today && 未完成状态）
    var unfinished = allPlans.filter(function(p){ return PLAN_INCOMPLETE_STATUS[p.status]; });
    var todayUnfinished = unfinished.filter(function(p){ return p.plan_day === today; });
    var historyUnfinished = unfinished.filter(function(p){ return p.plan_day < today; });
    renderHomeUnfinished(unfinished, todayUnfinished, historyUnfinished);

    // 未来三天入库计划
    renderHomeNext3Plans(next3Plans, day1);

    renderHomeCard("wo-today", wos, null, "wo");
    renderHomeCardSimple("sc-today", scs, SC_STATUS_LABEL, ["open","closed","cancelled"]);

    // 提醒区
    var alerts = [];
    wos.forEach(function(w){
      if(w.has_cancel_notice) alerts.push({ cls:"alert-cancel", text:"❌ "+w.workorder_id+" 已取消 — "+esc(w.customer_name), id:w.workorder_id, type:"wo" });
    });
    wos.forEach(function(w){
      if(w.has_update_notice && !w.has_cancel_notice) alerts.push({ cls:"alert-update", text:"⚠ "+w.workorder_id+" 已变更 — "+esc(w.customer_name), id:w.workorder_id, type:"wo" });
    });
    // 待发货确认提醒
    wos.forEach(function(w){
      if(w.status==="completed" && w.pickup_vehicle_no && !w.shipment_confirmed_at){
        alerts.push({ cls:"alert-update", text:"🚚 待发货确认: "+w.workorder_id+" · 车牌 "+esc(w.pickup_vehicle_no)+(w.pickup_driver_name ? " · 司机 "+esc(w.pickup_driver_name) : ""), id:w.workorder_id, type:"wo" });
      }
    });
    scs.forEach(function(b){
      if(b.status === "open") alerts.push({ cls:"alert-open", text:"📋 核对进行中: "+b.batch_id+" — "+esc(b.batch_name), id:b.batch_id, type:"sc" });
    });
    var alertEl = document.getElementById("home-alerts");
    if(alerts.length > 0){
      alertEl.innerHTML = '<div style="font-size:14px;font-weight:700;margin-bottom:6px;">需要关注</div>' +
        alerts.map(function(a){
          var onclick = a.type==="wo" ? "goWoDetail('"+esc(a.id)+"')" : a.type==="sc" ? "goScDetail('"+esc(a.id)+"')" : "goFoDetail('"+esc(a.id)+"')";
          return '<div class="alert-card '+a.cls+'" onclick="'+onclick+'">'+a.text+'</div>';
        }).join("");
    }
  });
}

function renderHomeUnfinished(all, todayItems, historyItems){
  var countEl = document.getElementById("ip-unfinished-count");
  var bodyEl = document.getElementById("ip-unfinished-body");
  countEl.textContent = "共 "+all.length+" 单"+(all.length ? "（今日 "+todayItems.length+" + 历史 "+historyItems.length+"）" : "");

  if(!all.length){ bodyEl.innerHTML = '<div class="q-empty">全部已完成 ✓</div>'; return; }

  var statusCounts = {};
  todayItems.forEach(function(p){ statusCounts[p.status] = (statusCounts[p.status]||0) + 1; });
  var tags = [];
  ["abnormal","processing","arrived","pending"].forEach(function(s){
    if(statusCounts[s]) tags.push('<span class="st st-'+s+'">'+esc(PLAN_STATUS_LABEL[s])+' '+statusCounts[s]+'</span>');
  });
  var html = '';
  if(tags.length) html += '<div class="card-status-row">' + tags.join(' ') + '</div>';
  if(historyItems.length > 0){
    var earliest = historyItems[0].plan_day;
    historyItems.forEach(function(p){ if(p.plan_day < earliest) earliest = p.plan_day; });
    html += '<div class="overdue-warn" onclick="goCardDetail(\'plan\',\'overdue\');event.stopPropagation();">⚠ 历史未完成 '+historyItems.length+' 单（最早 '+esc(earliest)+'）</div>';
  }
  if(!html) html = '<div class="q-empty">暂无</div>';
  bodyEl.innerHTML = html;
}

function renderHomeNext3Plans(plans, day1){
  var countEl = document.getElementById("ip-next3-count");
  var bodyEl = document.getElementById("ip-next3-body");
  countEl.textContent = "共 "+plans.length+" 单";

  if(!plans.length){ bodyEl.innerHTML = '<div class="q-empty">暂无</div>'; return; }

  var dayLabels = ["明天","后天","大后天"];
  var byDay = {};
  plans.forEach(function(p){
    if(!byDay[p.plan_day]) byDay[p.plan_day] = [];
    byDay[p.plan_day].push(p);
  });

  var days = Object.keys(byDay).sort();
  var html = '';
  days.forEach(function(day, idx){
    var label = dayLabels[idx] || day;
    var items = byDay[day];
    html += '<div class="next3-day">'+esc(label)+' ('+esc(day)+') — '+items.length+' 单</div>';
    var show = items.slice(0, 3);
    show.forEach(function(p){
      html += '<div class="next3-item">· '+esc(p.customer_name)+(p.expected_arrival_time ? ' · '+esc(p.expected_arrival_time) : '')+' · '+esc((p.goods_summary||"").substring(0,20))+'</div>';
    });
    if(items.length > 3) html += '<div class="next3-item" style="color:#999;">还有 '+(items.length-3)+' 单...</div>';
  });
  bodyEl.innerHTML = html;
}

function renderHomeCard(prefix, items, overdueItems, type){
  var countEl = document.getElementById(prefix + "-count");
  var bodyEl = document.getElementById(prefix + "-body");
  countEl.textContent = "共 " + items.length + " 单";

  var labels = (type === "plan") ? PLAN_STATUS_LABEL : WO_STATUS_LABEL;
  var order = (type === "plan")
    ? ["abnormal","processing","arrived","pending","completed","cancelled"]
    : ["working","issued","draft","completed","cancelled"];

  var statusCounts = {};
  items.forEach(function(item){ statusCounts[item.status] = (statusCounts[item.status]||0) + 1; });

  var tags = [];
  order.forEach(function(s){
    if(statusCounts[s]) tags.push('<span class="st st-'+s+'">'+esc(labels[s])+' '+statusCounts[s]+'</span>');
  });

  var html = '';
  if(tags.length > 0){
    html += '<div class="card-status-row">' + tags.join(' ') + '</div>';
  } else {
    html += '<div class="q-empty">暂无</div>';
  }

  if(overdueItems && overdueItems.length > 0){
    var earliest = overdueItems[0].plan_day;
    overdueItems.forEach(function(item){ if(item.plan_day < earliest) earliest = item.plan_day; });
    html += '<div class="overdue-warn" onclick="goCardDetail(\''+type+'\',\'overdue\');event.stopPropagation();">' +
      '⚠ 逾期未完成 '+overdueItems.length+' 单（最早 '+esc(earliest)+'）</div>';
  }

  bodyEl.innerHTML = html;
}

function renderHomeCardSimple(prefix, items, labelMap, order){
  var countEl = document.getElementById(prefix + "-count");
  var bodyEl = document.getElementById(prefix + "-body");
  countEl.textContent = "共 " + items.length + " 条";
  var statusCounts = {};
  items.forEach(function(item){ statusCounts[item.status] = (statusCounts[item.status]||0) + 1; });
  var tags = [];
  order.forEach(function(s){
    if(statusCounts[s]) tags.push('<span class="st st-'+s+'">'+esc(labelMap[s]||s)+' '+statusCounts[s]+'</span>');
  });
  bodyEl.innerHTML = tags.length > 0 ? '<div class="card-status-row">' + tags.join(' ') + '</div>' : '<div class="q-empty">暂无</div>';
}

function goCardDetail(type, scope){
  if(type === "plan"){
    _planListScope = scope;
    goView("plan_list");
  } else {
    _woListScope = scope;
    goView("wo_list");
  }
}

function changePlanStatus(plan_id, status){
  if(status === "cancelled"){
    if(!confirm("确认作废计划 " + plan_id + "？\n作废后不可恢复。")) return;
  }
  fetchApi({ action:"b2b_plan_update_status", plan_id:plan_id, status:status, updated_by:"" }).then(function(res){
    if(res && res.ok){
      if(document.getElementById("v-home").style.display !== "none") loadHome();
      if(document.getElementById("v-plan_list").style.display !== "none") reloadCurrentPlanList();
      if(document.getElementById("v-plan_detail").style.display !== "none") goPlanDetail(plan_id, true);
    } else {
      alert("状态更新失败: " + (res&&res.error||"unknown"));
    }
  });
}

// ===== 入库计划列表 =====
function initPlanList(){
  var today = kstToday();
  var scope = _planListScope;
  _planListScope = "unfinished";
  _currentPlanScope = scope;

  if(scope === "unfinished"){
    document.getElementById("pl-start").value = B2B_EARLIEST_DAY;
    document.getElementById("pl-end").value = today;
    loadPlanListByRange("unfinished");
  } else if(scope === "next3"){
    document.getElementById("pl-start").value = kstDayOffset(1);
    document.getElementById("pl-end").value = kstDayOffset(3);
    loadPlanListByRange("next3");
  } else if(scope === "tomorrow"){
    var tmr = kstTomorrow();
    document.getElementById("pl-start").value = tmr;
    document.getElementById("pl-end").value = tmr;
    loadPlanListByRange(null);
  } else if(scope === "overdue"){
    document.getElementById("pl-start").value = B2B_EARLIEST_DAY;
    document.getElementById("pl-end").value = kstYesterday();
    loadPlanListByRange("overdue");
  } else {
    document.getElementById("pl-start").value = today;
    document.getElementById("pl-end").value = today;
    loadPlanListByRange(null);
  }
}

function loadPlanListByRange(mode){
  var s = document.getElementById("pl-start").value;
  var e = document.getElementById("pl-end").value;
  if(!s || !e){ alert("请选择日期"); return; }
  if(mode === "unfinished" || mode === "overdue" || mode === "next3"){
    _currentPlanScope = mode;
  } else {
    _currentPlanScope = "custom";
  }
  var titleEl = document.getElementById("pl-title");
  var resultEl = document.getElementById("pl-result");
  resultEl.innerHTML = '<div class="q-empty">加载中...</div>';

  if(mode === "unfinished") titleEl.textContent = "未完成入库计划";
  else if(mode === "next3") titleEl.textContent = "未来三天入库计划";
  else if(mode === "overdue") titleEl.textContent = "逾期未完成计划";
  else titleEl.textContent = "入库计划列表";

  fetchApi({ action:"b2b_plan_list", start_day:s, end_day:e }).then(function(res){
    if(!res || !res.ok){ resultEl.innerHTML = '<div class="bad">查询失败</div>'; return; }
    var all = res.plans || [];
    if(mode === "unfinished"){
      var today = kstToday();
      var incomplete = all.filter(function(p){ return PLAN_INCOMPLETE_STATUS[p.status]; });
      var todayInc = incomplete.filter(function(p){ return p.plan_day === today; });
      var historyInc = incomplete.filter(function(p){ return p.plan_day < today; });
      renderPlanList(resultEl, todayInc, historyInc);
    } else if(mode === "next3"){
      renderPlanNext3List(resultEl, all);
    } else if(mode === "overdue"){
      var overdue = all.filter(function(p){ return PLAN_INCOMPLETE_STATUS[p.status]; });
      renderPlanList(resultEl, [], overdue);
    } else {
      renderPlanList(resultEl, all, []);
    }
  });
}

function renderPlanNext3List(container, plans){
  if(!plans.length){ container.innerHTML = '<div class="q-empty">暂无计划</div>'; return; }
  var byDay = {};
  plans.forEach(function(p){
    if(!byDay[p.plan_day]) byDay[p.plan_day] = [];
    byDay[p.plan_day].push(p);
  });
  var targets = [
    { day: kstDayOffset(1), label: "明天" },
    { day: kstDayOffset(2), label: "后天" },
    { day: kstDayOffset(3), label: "大后天" }
  ];
  var html = '';
  var first = true;
  targets.forEach(function(t){
    var items = byDay[t.day];
    if(!items || !items.length) return;
    items.sort(function(a,b){
      var sp = (PLAN_STATUS_PRIORITY[a.status]||9) - (PLAN_STATUS_PRIORITY[b.status]||9);
      if(sp !== 0) return sp;
      return (a.is_accounted||0) - (b.is_accounted||0);
    });
    html += '<div class="list-section-title" style="margin-top:'+(first?'0':'12')+'px;">📅 '+esc(t.label)+' ('+esc(t.day)+') — '+items.length+' 单</div>';
    html += items.map(renderPlanRow).join("");
    first = false;
  });
  if(!html) html = '<div class="q-empty">暂无计划</div>';
  container.innerHTML = html;
}

function renderPlanList(container, plans, overduePlans){
  var html = '';

  if(overduePlans.length > 0){
    overduePlans.sort(function(a,b){
      if(a.plan_day !== b.plan_day) return a.plan_day < b.plan_day ? -1 : 1;
      var sp = (PLAN_STATUS_PRIORITY[a.status]||9) - (PLAN_STATUS_PRIORITY[b.status]||9);
      if(sp !== 0) return sp;
      return (a.is_accounted||0) - (b.is_accounted||0);
    });
    html += '<div class="list-section-title overdue-section-title">⚠ 逾期未完成（'+overduePlans.length+' 单）</div>';
    html += overduePlans.map(renderPlanRow).join("");
  }

  if(plans.length > 0){
    plans.sort(function(a,b){
      var sp = (PLAN_STATUS_PRIORITY[a.status]||9) - (PLAN_STATUS_PRIORITY[b.status]||9);
      if(sp !== 0) return sp;
      return (a.is_accounted||0) - (b.is_accounted||0);
    });
    if(overduePlans.length > 0){
      html += '<div class="list-section-title" style="margin-top:12px;">📥 今日未完成（'+plans.length+' 单）</div>';
    }
    html += plans.map(renderPlanRow).join("");
  }

  if(!html) html = '<div class="q-empty">暂无计划</div>';
  container.innerHTML = html;
}

function renderPlanRow(p){
  var dimClass = (p.status==="cancelled") ? " row-dim" : "";
  var btns = (PLAN_NEXT_STATUS[p.status]||[]).map(function(s){
    return '<button onclick="event.stopPropagation();changePlanStatus(\''+esc(p.plan_id)+'\',\''+s+'\')">'+esc(PLAN_STATUS_LABEL[s]||s)+'</button>';
  }).join("");
  var editBtn = PLAN_EDITABLE[p.status] ? '<button onclick="event.stopPropagation();goEditPlan(\''+esc(p.plan_id)+'\')">编辑</button>' : '';
  var foBtn = (p.status==="arrived"||p.status==="processing") ? '<button onclick="event.stopPropagation();goNewFoFromPlan(\''+esc(p.plan_id)+'\',\''+esc(p.plan_day)+'\',\''+esc(p.customer_name)+'\',\''+esc(p.goods_summary||"")+'\',\''+esc(p.purpose_text||"")+'\')" style="background:#8e24aa;color:#fff;">+ 现场记录</button>' : '';
  var accTag = p.is_accounted ? '<span class="acc-tag acc-yes">已记帐</span>' : '<span class="acc-tag acc-no">未记帐</span>';
  var accBtn = p.is_accounted
    ? '<span class="acc-btn" onclick="event.stopPropagation();setPlanAccounted(\''+esc(p.plan_id)+'\',0)">撤销记帐</span>'
    : '<span class="acc-btn" onclick="event.stopPropagation();setPlanAccounted(\''+esc(p.plan_id)+'\',1)">标记记帐</span>';
  return '<div class="wo-row'+dimClass+'" onclick="goPlanDetail(\''+esc(p.plan_id)+'\')">' +
    '<div><span class="st st-'+esc(p.status)+'">'+esc(PLAN_STATUS_LABEL[p.status]||p.status)+'</span> ' +
    '<b>'+esc(p.customer_name)+'</b> ' + bizBadge(p.biz_type) + accTag + accBtn +
    ' <span class="muted" style="font-size:11px;">'+esc(p.plan_id)+' · '+esc(p.plan_day)+'</span></div>' +
    '<div class="meta">'+esc(p.goods_summary) + (p.expected_arrival_time ? ' · 预计'+esc(p.expected_arrival_time) : '') + '</div>' +
    (p.purpose_text ? '<div class="meta">用途: '+esc(p.purpose_text)+'</div>' : '') +
    (p.remark ? '<div class="meta">备注: '+esc(p.remark)+'</div>' : '') +
    '<div class="status-btns">' + btns + editBtn + foBtn + '</div>' +
  '</div>';
}

// ===== 入库计划详情（含关联） =====
function goPlanDetail(plan_id, _skipNav){
  if(!_skipNav) navPush();
  _currentDetailId = plan_id;
  goView("plan_detail");
  var card = document.getElementById("plan-detail-card");
  card.innerHTML = '<div class="muted">加载中...</div>';

  fetchApi({ action:"b2b_plan_list", start_day:B2B_EARLIEST_DAY, end_day:"2099-12-31" }).then(function(res){
    if(!res || !res.ok){ card.innerHTML = '<div class="bad">加载失败</div>'; return; }
    var found = null;
    (res.plans||[]).forEach(function(p){ if(p.plan_id === plan_id) found = p; });
    if(!found){ card.innerHTML = '<div class="bad">未找到计划 '+esc(plan_id)+'</div>'; return; }
    var p = found;

    var statusBtns = (PLAN_NEXT_STATUS[p.status]||[]).map(function(s){
      return '<button onclick="event.stopPropagation();changePlanStatus(\''+esc(p.plan_id)+'\',\''+s+'\')" class="'+(s==="cancelled"?"bad":"primary")+'" style="width:auto;padding:8px 16px;font-size:13px;">'+esc(PLAN_STATUS_LABEL[s]||s)+'</button>';
    }).join(" ");
    var editBtn = PLAN_EDITABLE[p.status] ? ' <button onclick="goEditPlan(\''+esc(p.plan_id)+'\')" style="width:auto;padding:8px 16px;font-size:13px;">编辑</button>' : '';
    var foBtn = (p.status==="arrived"||p.status==="processing") ?
      ' <button onclick="goNewFoFromPlan(\''+esc(p.plan_id)+'\',\''+esc(p.plan_day)+'\',\''+esc(p.customer_name)+'\',\''+esc(p.goods_summary||"")+'\',\''+esc(p.purpose_text||"")+'\')" style="width:auto;padding:8px 16px;font-size:13px;background:#8e24aa;color:#fff;">+ 现场记录</button>' : '';

    card.innerHTML =
      '<div style="font-size:18px;font-weight:800;margin-bottom:10px;">' +
        esc(p.plan_id) + ' <span class="st st-'+esc(p.status)+'">'+esc(PLAN_STATUS_LABEL[p.status]||p.status)+'</span> ' + bizBadge(p.biz_type) +
      '</div>' +
      '<div class="detail-field"><b>客户:</b> '+esc(p.customer_name)+'</div>' +
      '<div class="detail-field"><b>计划到货日:</b> '+esc(p.plan_day)+'</div>' +
      '<div class="detail-field"><b>货物摘要:</b> '+esc(p.goods_summary||"(无)")+'</div>' +
      (p.expected_arrival_time ? '<div class="detail-field"><b>预计到达:</b> '+esc(p.expected_arrival_time)+'</div>' : '') +
      (p.purpose_text ? '<div class="detail-field"><b>用途:</b> '+esc(p.purpose_text)+'</div>' : '') +
      (p.remark ? '<div class="detail-field"><b>备注:</b> '+esc(p.remark)+'</div>' : '') +
      '<div class="detail-field muted" style="font-size:12px;"><b>创建人:</b> '+esc(p.created_by)+' · '+new Date(p.created_at).toLocaleString() +
      (p.status_updated_at ? ' · 更新: '+new Date(p.status_updated_at).toLocaleString() : '') +
      (p.status_updated_by ? ' ('+esc(p.status_updated_by)+')' : '') + '</div>' +
      '<div class="detail-field">' +
        (p.is_accounted
          ? '<span class="acc-tag acc-yes">已记帐</span> <span class="muted" style="font-size:12px;">记帐人: '+esc(p.accounted_by||"")+' · '+( p.accounted_at ? new Date(p.accounted_at).toLocaleString() : "")+'</span> <span class="acc-btn" onclick="setPlanAccounted(\''+esc(p.plan_id)+'\',0)">撤销记帐</span>'
          : '<span class="acc-tag acc-no">未记帐</span> <span class="acc-btn" onclick="setPlanAccounted(\''+esc(p.plan_id)+'\',1)">标记记帐</span>') +
      '</div>' +
      '<div style="margin:12px 0;">' + statusBtns + editBtn + foBtn + '</div>' +
      '<div id="plan-cross-fo" class="cross-ref"><div class="cross-ref-title">关联现场记录</div><div class="muted">加载中...</div></div>' +
      '<div id="plan-cross-wo" class="cross-ref"><div class="cross-ref-title">同日同客户作业单 <span class="weak-label">（弱关联：同日期+同客户名）</span></div><div class="muted">加载中...</div></div>';

    // 关联现场记录
    fetchApi({ action:"b2b_field_op_list", start_day:B2B_EARLIEST_DAY, end_day:"2099-12-31" }).then(function(foRes){
      var el = document.getElementById("plan-cross-fo");
      if(!el) return;
      var matched = (foRes && foRes.ok) ? (foRes.records||[]).filter(function(r){ return r.source_plan_id === plan_id; }) : [];
      if(!matched.length){
        el.innerHTML = '<div class="cross-ref-title">关联现场记录</div><div class="q-empty">暂无</div>';
      } else {
        el.innerHTML = '<div class="cross-ref-title">关联现场记录（'+matched.length+' 条）</div>' +
          matched.map(function(r){
            return '<div class="cross-ref-row" onclick="goFoDetail(\''+esc(r.record_id)+'\')">' +
              '<span class="st st-'+esc(r.status)+'">'+esc(FO_STATUS_LABEL[r.status]||r.status)+'</span> ' +
              '<b>'+esc(r.record_id)+'</b> · '+esc(r.customer_name)+' · '+esc(FO_OP_TYPE_LABEL[r.operation_type]||r.operation_type) +
            '</div>';
          }).join("");
      }
    });

    // 同日同客户作业单
    fetchApi({ action:"b2b_wo_list", start_day:p.plan_day, end_day:p.plan_day }).then(function(woRes){
      var el = document.getElementById("plan-cross-wo");
      if(!el) return;
      var matched = (woRes && woRes.ok) ? (woRes.workorders||[]).filter(function(w){ return w.customer_name === p.customer_name; }) : [];
      if(!matched.length){
        el.innerHTML = '<div class="cross-ref-title">同日同客户作业单 <span class="weak-label">（弱关联）</span></div><div class="q-empty">暂无</div>';
      } else {
        el.innerHTML = '<div class="cross-ref-title">同日同客户作业单（'+matched.length+' 单）<span class="weak-label">（弱关联：同日期+同客户名）</span></div>' +
          matched.map(function(w){
            var noticeTags = '';
            if(w.has_cancel_notice) noticeTags += ' <span class="cancel-notice-tag">已取消</span>';
            if(w.has_update_notice) noticeTags += ' <span class="notice-tag">已变更</span>';
            return '<div class="cross-ref-row" onclick="goWoDetail(\''+esc(w.workorder_id)+'\')">' +
              '<span class="st st-'+esc(w.status)+'">'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</span> ' +
              '<b>'+esc(w.workorder_id)+'</b> · '+esc(w.customer_name)+noticeTags +
            '</div>';
          }).join("");
      }
    });
  });
}

// ===== 作业记录：单据台账 =====
var _docListData = [];
var _docListFiltered = [];
var _docViewMode = "summary";  // summary | session | raw
var _docPage = 1;
var _docPageSize = 50;
var _docTotal = 0;

var WAVE_KIND_LABEL = {
  b2c_pick:"B2C拣货", b2c_tally:"B2C理货", b2c_batch_out:"B2C批量出库",
  b2b_inbound_tally:"B2B入库理货", b2b_workorder:"B2B工单操作", b2b_field_op:"B2B现场记录"
};

var LINK_STATUS_LABEL = {
  unlinked:"仅作业流水", no_binding:"无绑定", no_record:"无记录",
  ambiguous_binding:"绑定冲突",
  internal_bound_result_missing:"内部·无结果单", internal_bound_result_draft:"内部·草稿",
  internal_bound_result_completed:"内部·已完成",
  external_bound_result_missing:"外部·无结果单", external_bound_result_draft:"外部·草稿",
  external_bound_result_completed:"外部·已完成",
  plan_linked_wo_bound:"有计划+有工单", plan_linked_wo_unbound:"有计划·无工单",
  independent_wo_bound:"独立+有工单", independent:"独立记录"
};

function linkStatusCls(ls){
  if(!ls) return "link-default";
  if(ls.indexOf("completed")>=0 || ls==="plan_linked_wo_bound") return "link-completed";
  if(ls.indexOf("draft")>=0 || ls.indexOf("unbound")>=0) return "link-draft";
  if(ls.indexOf("missing")>=0 || ls==="no_binding" || ls==="no_record" || ls==="ambiguous_binding") return "link-missing";
  if(ls==="unlinked") return "link-unlinked";
  return "link-default";
}

function switchDocView(mode){
  _docViewMode = mode;
  if(mode === "raw"){
    goView("wave_list");
    return;
  }
  goView("doc_list");
}

function initDocList(){
  var today = kstToday();
  if(!document.getElementById("dl-start").value) document.getElementById("dl-start").value = today;
  if(!document.getElementById("dl-end").value) document.getElementById("dl-end").value = today;
  // 更新 toggle 按钮状态
  var btns = document.querySelectorAll("#docViewToggle button");
  for(var i=0;i<btns.length;i++) btns[i].className = "";
  if(_docViewMode === "summary") btns[0].className = "vt-active";
  else if(_docViewMode === "session") btns[1].className = "vt-active";
  _docPage = 1;
  loadDocList();
}

function loadDocList(){
  var s = document.getElementById("dl-start").value;
  var e = document.getElementById("dl-end").value;
  if(!s || !e){ alert("请选择日期"); return; }
  var kind = document.getElementById("dl-kind").value;
  var el = document.getElementById("dl-result");
  el.innerHTML = '<div class="q-empty">加载中...</div>';
  document.getElementById("dl-pager").style.display = "none";

  var kw = (document.getElementById("dl-keyword").value||"").trim();
  var params = {
    action: "collab_doc_list",
    start_day: s, end_day: e,
    summary_mode: _docViewMode === "summary" ? "1" : "0",
    page: _docPage, page_size: _docPageSize
  };
  if(kind) params.wave_kind = kind;
  if(kw) params.keyword = kw;

  fetchApi(params).then(function(res){
    if(!res || !res.ok){
      el.innerHTML = '<div class="bad">查询失败: '+esc(res&&res.error||"")+'</div>';
      return;
    }
    _docListData = res.docs || [];
    _docTotal = res.total || 0;
    _docListFiltered = _docListData;
    renderDocList(_docListFiltered);
    renderDocPager();
  });
}

var _docKwTimer = null;
function docKeywordKeyup(ev){
  if(ev && ev.key === "Enter"){
    if(_docKwTimer) clearTimeout(_docKwTimer);
    _docPage = 1;
    loadDocList();
    return;
  }
  // 非回车：本地即时过滤当前页数据（视觉响应），同时 debounce 500ms 后触发后端搜索
  filterDocListLocal();
  if(_docKwTimer) clearTimeout(_docKwTimer);
  _docKwTimer = setTimeout(function(){ _docPage = 1; loadDocList(); }, 500);
}

function filterDocListLocal(){
  var kw = (document.getElementById("dl-keyword").value||"").trim().toLowerCase();
  if(!kw){
    _docListFiltered = _docListData;
  } else {
    _docListFiltered = _docListData.filter(function(d){
      return (d.wave_id||"").toLowerCase().indexOf(kw)>=0 ||
             (d.session||"").toLowerCase().indexOf(kw)>=0 ||
             (d.customer_name||"").toLowerCase().indexOf(kw)>=0 ||
             (d.work_day_kst||"").indexOf(kw)>=0;
    });
  }
  renderDocList(_docListFiltered);
}

function renderDocList(docs){
  var el = document.getElementById("dl-result");
  if(!docs.length){
    el.innerHTML = '<div class="q-empty">暂无记录</div>';
    return;
  }
  var isSummary = _docViewMode === "summary";
  var html = '<div style="font-size:12px;color:#888;margin-bottom:4px;">本页 '+docs.length+' 条 / 共 '+_docTotal+' 条</div>';

  html += docs.map(function(d){
    var kindLabel = WAVE_KIND_LABEL[d.wave_kind] || d.task || "";
    var kindCls = "doc-kind-tag doc-kind-" + (d.doc_class||"wave_only");
    var linkLabel = LINK_STATUS_LABEL[d.link_status] || d.link_status || "";
    var linkCls = "link-tag " + linkStatusCls(d.link_status);

    var line1 = '<span class="'+kindCls+'">'+esc(kindLabel)+'</span> ';
    line1 += '<code>'+esc(d.wave_id)+'</code>';
    if(d.customer_name) line1 += ' · '+esc(d.customer_name);
    line1 += ' <span class="'+linkCls+'">'+esc(linkLabel)+'</span>';

    var line2parts = [];
    line2parts.push(esc(d.work_day_kst || ""));
    if(d.first_ms) line2parts.push(new Date(d.first_ms).toLocaleTimeString() + '~' + new Date(d.last_ms).toLocaleTimeString());
    if(isSummary && d.session_count > 1) line2parts.push(d.session_count + ' sessions');
    if(!isSummary && d.session) line2parts.push('session:' + esc((d.session||"").slice(-8)));
    var bc = d.session_badge_count || 0;
    if(bc) line2parts.push(bc + '人[' + esc(d.session_badge_list||"") + ']');
    if(d.record_count > 1) line2parts.push('×'+d.record_count);

    var line3 = "";
    if(d.wave_kind === "b2b_workorder" && d.link_status && d.link_status !== "no_binding"){
      var parts = [];
      if(d.operation_mode) parts.push(esc(d.operation_mode));
      if(d.box_count) parts.push('箱:'+d.box_count);
      if(d.pallet_count) parts.push('托:'+d.pallet_count);
      if(d.packed_qty) parts.push('件:'+d.packed_qty);
      if(d.sku_kind_count) parts.push('SKU:'+d.sku_kind_count);
      if(d.label_count) parts.push('标签:'+d.label_count);
      if(d.packed_box_count) parts.push('打包箱:'+d.packed_box_count);
      if(d.did_rebox) parts.push('换箱:'+d.rebox_count);
      if(d.needs_forklift_pick) parts.push('叉车托:'+d.forklift_pallet_count);
      if(d.remark) parts.push('备注:'+esc(d.remark));
      if(parts.length) line3 = '<div class="meta">'+parts.join(' · ')+'</div>';
      // 内部工单有更多信息
      if(d.wo_status){
        var woInfo = '<span class="st st-'+esc(d.wo_status)+'">'+esc(WO_STATUS_LABEL[d.wo_status]||d.wo_status)+'</span>';
        if(d.has_cancel_notice) woInfo += ' <span class="cancel-notice-tag">已取消</span>';
        if(d.has_update_notice) woInfo += ' <span class="notice-tag">已变更</span>';
        line3 = '<div class="meta">'+woInfo+'</div>' + line3;
      }
      if(d.confirm_badge) line3 += '<div class="meta">确认: '+esc(d.confirm_badge)+(d.confirmed_by?' ('+esc(d.confirmed_by)+')':'')+'</div>';
    } else if(d.wave_kind === "b2b_field_op" && d.link_status !== "no_record"){
      var parts = [];
      if(d.operation_type) parts.push(esc(FO_OP_TYPE_LABEL[d.operation_type]||d.operation_type));
      if(d.fo_status) parts.push('<span class="st st-'+esc(d.fo_status)+'">'+esc(FO_STATUS_LABEL[d.fo_status]||d.fo_status)+'</span>');
      if(d.input_box_count) parts.push('入箱:'+d.input_box_count);
      if(d.output_box_count) parts.push('出箱:'+d.output_box_count);
      if(d.output_pallet_count) parts.push('出托:'+d.output_pallet_count);
      if(d.packed_qty) parts.push('件:'+d.packed_qty);
      if(d.label_count) parts.push('标签:'+d.label_count);
      if(d.source_plan_id) parts.push('计划:'+esc(d.source_plan_id));
      if(d.bound_workorder_id) parts.push('工单:'+esc(d.bound_workorder_id));
      if(parts.length) line3 = '<div class="meta">'+parts.join(' · ')+'</div>';
    }

    // 跳转按钮
    var btns = "";
    if(d.wave_kind === "b2b_workorder"){
      btns = '<button style="width:auto;padding:2px 10px;font-size:11px;margin:2px 4px 0 0;" onclick="event.stopPropagation();goWoDetail(\''+esc(d.wave_id)+'\')">查看工单</button>';
    } else if(d.wave_kind === "b2b_field_op"){
      btns = '<button style="width:auto;padding:2px 10px;font-size:11px;margin:2px 4px 0 0;" onclick="event.stopPropagation();goFoDetail(\''+esc(d.wave_id)+'\')">查看记录</button>';
    }

    return '<div class="doc-row">' +
      '<div>'+line1+'</div>' +
      '<div class="meta">'+line2parts.join(' · ')+'</div>' +
      line3 +
      (btns ? '<div>'+btns+'</div>' : '') +
    '</div>';
  }).join("");

  el.innerHTML = html;
}

function renderDocPager(){
  var pagerEl = document.getElementById("dl-pager");
  var totalPages = Math.ceil(_docTotal / _docPageSize) || 1;
  if(totalPages <= 1){ pagerEl.style.display = "none"; return; }
  pagerEl.style.display = "";
  pagerEl.innerHTML =
    '<button '+((_docPage<=1)?'disabled':'')+' onclick="docPageGo(-1)">上一页</button>' +
    '<span>第 '+_docPage+' / '+totalPages+' 页</span>' +
    '<button '+((_docPage>=totalPages)?'disabled':'')+' onclick="docPageGo(1)">下一页</button>';
}

function docPageGo(delta){
  _docPage += delta;
  if(_docPage < 1) _docPage = 1;
  loadDocList();
}

function exportDocCsv(){
  var s = document.getElementById("dl-start").value;
  var e = document.getElementById("dl-end").value;
  if(!s || !e){ alert("请选择日期"); return; }
  var kind = document.getElementById("dl-kind").value;
  var kw = (document.getElementById("dl-keyword").value||"").trim();

  var params = new URLSearchParams();
  params.set("action","collab_doc_export");
  params.set("k", getKey());
  params.set("start_day", s);
  params.set("end_day", e);
  params.set("summary_mode", _docViewMode === "summary" ? "1" : "0");
  if(kind) params.set("wave_kind", kind);
  if(kw) params.set("keyword", kw);

  var url = API_URL + "?" + params.toString();
  // 用隐藏 a 标签触发下载
  var a = document.createElement("a");
  a.href = url;
  a.download = "doc_ledger_" + s + "_" + e + ".csv";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

// ===== 作业记录：原始波次流水（保留） =====
var _waveListData = [];
var TASK_LABEL_SHORT = {
  "B2C拣货":"拣货","B2C理货":"理货","B2C批量出库":"批量出库",
  "B2B入库理货":"B2B理货","B2B工单操作":"工单操作","B2B现场记录":"现场记录"
};

function initWaveList(){
  var today = kstToday();
  if(!document.getElementById("hw-start").value) document.getElementById("hw-start").value = today;
  if(!document.getElementById("hw-end").value) document.getElementById("hw-end").value = today;
  loadWaveList();
}

function loadWaveList(){
  var s = document.getElementById("hw-start").value;
  var e = document.getElementById("hw-end").value;
  if(!s || !e){ alert("请选择日期"); return; }
  var biz = document.getElementById("hw-biz").value;
  var task = document.getElementById("hw-task").value;
  var el = document.getElementById("hw-result");
  el.innerHTML = '<div class="q-empty">加载中...</div>';
  var params = { action:"collab_wave_list", start_day:s, end_day:e };
  if(biz) params.biz = biz;
  if(task) params.task = task;
  fetchApi(params).then(function(res){
    if(!res || !res.ok){ el.innerHTML = '<div class="bad">查询失败: '+esc(res&&res.error||"")+'</div>'; return; }
    _waveListData = res.waves || [];
    renderWaveList(_waveListData);
  });
}

function filterWaveListLocal(){
  var kw = (document.getElementById("hw-keyword").value||"").trim().toLowerCase();
  if(!kw){ renderWaveList(_waveListData); return; }
  renderWaveList(_waveListData.filter(function(w){
    return (w.wave_id||"").toLowerCase().indexOf(kw)>=0 ||
           (w.session||"").toLowerCase().indexOf(kw)>=0 ||
           (w.operator_id||"").toLowerCase().indexOf(kw)>=0 ||
           (w.customer_name||"").toLowerCase().indexOf(kw)>=0;
  }));
}

function renderWaveList(waves){
  var el = document.getElementById("hw-result");
  if(!waves.length){ el.innerHTML = '<div class="q-empty">暂无记录</div>'; return; }
  el.innerHTML = '<div style="font-size:12px;color:#888;margin-bottom:4px;">共 '+waves.length+' 条</div>' +
    waves.map(function(w, idx){
      var timeStr = new Date(w.first_ms).toLocaleTimeString();
      var taskLabel = TASK_LABEL_SHORT[w.task] || w.task;
      var bizCls = w.biz === "B2C" ? "biz biz-b2c" : "biz biz-b2b";
      var summary = "";
      if(w.detail_type === "b2b_workorder" && w.detail_found){
        summary = esc(w.customer_name||"") + " · " + esc(WO_STATUS_LABEL[w.wo_status]||w.wo_status||"");
        if(w.has_cancel_notice) summary += ' <span class="cancel-notice-tag">已取消</span>';
        if(w.has_update_notice) summary += ' <span class="notice-tag">已变更</span>';
      } else if(w.detail_type === "b2b_field_op" && w.detail_found){
        summary = esc(w.customer_name||"") + " · " + esc(FO_STATUS_LABEL[w.fo_status]||w.fo_status||"");
      }
      return '<div class="wave-row" onclick="toggleWaveDetail('+idx+')">' +
        '<div><span class="'+bizCls+'">'+esc(w.biz)+'</span> <b>'+esc(taskLabel)+'</b> · <code>'+esc(w.wave_id)+'</code></div>' +
        '<div class="meta" style="color:#666;font-size:12px;margin-top:2px;">'+timeStr+' · session:'+esc((w.session||"").slice(-8))+' · 操作员:'+esc(w.operator_id||"(无)")+(w.record_count>1?' · ×'+w.record_count:'')+'</div>' +
        (summary ? '<div class="meta" style="color:#666;font-size:12px;">'+summary+'</div>' : '') +
        '<div id="wave-detail-'+idx+'" style="display:none;"></div>' +
      '</div>';
    }).join("");
}

function toggleWaveDetail(idx){
  var el = document.getElementById("wave-detail-"+idx);
  if(!el) return;
  if(el.style.display !== "none"){ el.style.display = "none"; return; }
  el.style.display = "";
  var w = _waveListData[idx];
  if(!w){ el.innerHTML = ""; return; }

  var html = '<div class="wave-detail-box">';
  html += '<div><b>wave_id:</b> '+esc(w.wave_id)+'</div>';
  html += '<div><b>session:</b> '+esc(w.session)+'</div>';
  html += '<div><b>biz/task:</b> '+esc(w.biz)+' / '+esc(w.task)+'</div>';
  html += '<div><b>操作员:</b> '+esc(w.operator_id||"(无)")+'</div>';
  html += '<div><b>首次:</b> '+new Date(w.first_ms).toLocaleString()+' <b>最后:</b> '+new Date(w.last_ms).toLocaleString()+' <b>次数:</b> '+w.record_count+'</div>';

  if(w.detail_type === "b2b_workorder" && w.detail_found){
    html += '<hr style="margin:6px 0;border:none;border-top:1px solid #ddd;">';
    html += '<div style="font-weight:700;">工单摘要</div>';
    html += '<div>状态: <span class="st st-'+esc(w.wo_status)+'">'+esc(WO_STATUS_LABEL[w.wo_status]||w.wo_status)+'</span> 客户: '+esc(w.customer_name)+'</div>';
    if(w.outbound_destination) html += '<div>目的地: '+esc(w.outbound_destination)+'</div>';
    if(w.order_ref_no) html += '<div>발주번호: '+esc(w.order_ref_no)+'</div>';
    html += '<div>箱:'+w.outbound_box_count+' 托:'+w.outbound_pallet_count+'</div>';
    if(w.has_cancel_notice) html += '<div style="color:#d32f2f;font-weight:700;">❌ 已取消</div>';
    if(w.has_update_notice) html += '<div style="color:#e65100;font-weight:700;">⚠ 已变更</div>';
    if(w.result_status){
      html += '<div style="margin-top:4px;font-weight:700;">执行结果: '+esc(w.result_status)+(w.result_operation_mode?' · '+esc(w.result_operation_mode):'')+(w.result_confirm_badge?' · 确认:'+esc(w.result_confirm_badge):'')+'</div>';
    }
    html += '<div style="margin-top:6px;"><button style="width:auto;padding:4px 12px;font-size:12px;" onclick="event.stopPropagation();goWoDetail(\''+esc(w.wave_id)+'\')">查看工单详情</button></div>';
  } else if(w.detail_type === "b2b_field_op" && w.detail_found){
    html += '<hr style="margin:6px 0;border:none;border-top:1px solid #ddd;">';
    html += '<div style="font-weight:700;">现场记录摘要</div>';
    html += '<div>状态: <span class="st st-'+esc(w.fo_status)+'">'+esc(FO_STATUS_LABEL[w.fo_status]||w.fo_status)+'</span> 客户: '+esc(w.customer_name)+'</div>';
    if(w.source_plan_id) html += '<div>来源计划: '+esc(w.source_plan_id)+'</div>';
    if(w.bound_workorder_id) html += '<div>绑定作业单: '+esc(w.bound_workorder_id)+'</div>';
    if(w.operation_type) html += '<div>类型: '+esc(FO_OP_TYPE_LABEL[w.operation_type]||w.operation_type)+'</div>';
    html += '<div style="margin-top:6px;"><button style="width:auto;padding:4px 12px;font-size:12px;" onclick="event.stopPropagation();goFoDetail(\''+esc(w.wave_id)+'\')">查看记录详情</button></div>';
  }

  html += '</div>';
  el.innerHTML = html;
}

// ===== 入库计划 新建/编辑 =====
var _editingPlanId = null;

function initPlanForm(data){
  var isEdit = !!data;
  document.getElementById("pc-title").textContent = isEdit ? "编辑入库计划" : "新建入库计划";
  document.getElementById("pc-id-bar").style.display = isEdit ? "" : "none";
  document.getElementById("pc-id-bar").textContent = isEdit ? ("编号: " + data.plan_id + "（不可修改）") : "";
  document.getElementById("pc-submit-btn").textContent = isEdit ? "保存修改" : "保存";
  // 编辑模式隐藏创建人
  document.getElementById("pc-creator-group").style.display = isEdit ? "none" : "";

  document.getElementById("pc-day").value = isEdit ? data.plan_day : kstToday();
  document.getElementById("pc-customer").value = isEdit ? data.customer_name : "";
  var bizSel = document.getElementById("pc-biz");
  if(isEdit && BIZ_TYPE_NEW_KEYS.indexOf(data.biz_type) < 0){
    // 旧值不在新选项中，临时追加让回填生效
    var opt = document.createElement("option");
    opt.value = data.biz_type;
    opt.textContent = BIZ_TYPE_LABEL[data.biz_type] || data.biz_type;
    bizSel.appendChild(opt);
  }
  bizSel.value = isEdit ? data.biz_type : "b2c";
  document.getElementById("pc-summary").value = isEdit ? data.goods_summary : "";
  document.getElementById("pc-arrival").value = isEdit ? (data.expected_arrival_time||"") : "";
  document.getElementById("pc-purpose").value = isEdit ? (data.purpose_text||"") : "";
  document.getElementById("pc-remark").value = isEdit ? (data.remark||"") : "";
  document.getElementById("pc-creator").value = "";
  document.getElementById("pc-result").textContent = "";
}

function goEditPlan(plan_id){
  // 从 API 拉最新数据，再进入编辑模式
  var _navSnap = navCaptureState();
  fetchApi({ action:"b2b_plan_list", start_day:B2B_EARLIEST_DAY, end_day:"2099-12-31" }).then(function(res){
    if(!res || !res.ok){ alert("加载失败"); return; }
    var found = null;
    (res.plans||[]).forEach(function(p){ if(p.plan_id === plan_id) found = p; });
    if(!found){ alert("未找到计划 " + plan_id); return; }
    if(!PLAN_EDITABLE[found.status]){ alert("当前状态不允许编辑"); return; }
    _navStack.push(_navSnap);
    _editingPlanId = plan_id;
    goView("plan_create");
    initPlanForm(found);
  });
}

function submitPlan(){
  var day = document.getElementById("pc-day").value;
  var customer = document.getElementById("pc-customer").value.trim();
  var biz = document.getElementById("pc-biz").value;
  var summary = document.getElementById("pc-summary").value.trim();
  var arrival = document.getElementById("pc-arrival").value.trim();
  var purpose = document.getElementById("pc-purpose").value.trim();
  var remark = document.getElementById("pc-remark").value.trim();
  var creator = document.getElementById("pc-creator").value.trim();

  if(!day){ alert("请选择计划到货日"); return; }
  if(!customer){ alert("请输入客户名"); return; }
  if(!summary){ alert("请输入货物摘要"); return; }
  if(!_editingPlanId && !creator){ alert("请输入创建人"); return; }

  var btn = document.getElementById("pc-submit-btn");
  if(_editingPlanId){
    // 编辑模式
    if(_submitting) return;
    _submitting = true;
    btn.disabled = true; btn.textContent = "保存中...";
    fetchApi({
      action:"b2b_plan_update", plan_id:_editingPlanId,
      plan_day:day, customer_name:customer, biz_type:biz,
      goods_summary:summary, expected_arrival_time:arrival,
      purpose_text:purpose, remark:remark
    }).then(function(res){
      _submitting = false; btn.disabled = false; btn.textContent = "保存修改";
      if(res && res.ok){
        alert("修改成功！");
        _editingPlanId = null;
        _navStack = [];
        goHome();
      } else {
        document.getElementById("pc-result").innerHTML = '<span class="bad">修改失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  } else {
    // 新建模式
    if(_submitting) return;
    _submitting = true;
    btn.disabled = true; btn.textContent = "创建中...";
    fetchApi({
      action:"b2b_plan_create", plan_day:day, customer_name:customer, biz_type:biz,
      goods_summary:summary, expected_arrival_time:arrival, purpose_text:purpose,
      remark:remark, created_by:creator, request_id:getOrCreateRid("plan")
    }).then(function(res){
      _submitting = false; btn.disabled = false; btn.textContent = "保存";
      if(res && res.ok){
        clearRid("plan");
        alert("创建成功！编号: " + res.plan_id);
        _editingPlanId = null;
        _navStack = [];
        goHome();
      } else {
        document.getElementById("pc-result").innerHTML = '<span class="bad">创建失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  }
}

// ===== 出库作业单 新建/编辑 =====
var _editingWoId = null;
var _wcLineCount = 0;
var _wcPrevDetailMode = "sku_based"; // 记住上次 detail_mode，用于切换确认

function initWoCreate(data){
  var isEdit = !!data;
  _wcLineCount = 0;
  _wcPrevDetailMode = isEdit ? (data.detail_mode || "sku_based") : "sku_based";

  var editTitle = isEdit ? ("编辑作业单" + (data.status === "draft" ? "（草稿）" : "")) : "新建出库作业单";
  document.getElementById("wc-title").textContent = editTitle;
  document.getElementById("wc-id-bar").style.display = isEdit ? "" : "none";
  document.getElementById("wc-id-bar").textContent = isEdit ? ("作业单号: " + data.workorder_id + "（不可修改）") : "";
  document.getElementById("wc-submit-btn").textContent = isEdit ? "保存修改" : "保存（草稿）";
  // 新建显示创建人，编辑显示编辑人
  document.getElementById("wc-creator-group").style.display = "";
  if(isEdit){
    document.getElementById("wc-creator-label").textContent = "编辑人";
    document.getElementById("wc-creator").placeholder = "你的名字（必填）";
  } else {
    document.getElementById("wc-creator-label").textContent = "创建人";
    document.getElementById("wc-creator").placeholder = "你的名字（必填）";
  }

  document.getElementById("wc-detail-mode").value = isEdit ? (data.detail_mode || "sku_based") : "sku_based";
  document.getElementById("wc-day").value = isEdit ? data.plan_day : kstToday();
  document.getElementById("wc-op-mode").value = isEdit ? (data.operation_mode||"") : "";
  document.getElementById("wc-ob-mode").value = isEdit ? (data.outbound_mode||"") : "";
  document.getElementById("wc-customer").value = isEdit ? data.customer_name : "";
  document.getElementById("wc-destination").value = isEdit ? (data.outbound_destination||"") : "";
  document.getElementById("wc-order-ref").value = isEdit ? (data.order_ref_no||"") : "";
  document.getElementById("wc-ext-no").value = isEdit ? (data.external_workorder_no||"") : "";
  document.getElementById("wc-box-count").value = isEdit && data.outbound_box_count ? data.outbound_box_count : "";
  document.getElementById("wc-pallet-count").value = isEdit && data.outbound_pallet_count ? data.outbound_pallet_count : "";
  document.getElementById("wc-instr").value = isEdit ? (data.instruction_text||"") : "";
  document.getElementById("wc-creator").value = "";
  document.getElementById("wc-result").textContent = "";
  document.getElementById("wc-summary").textContent = "";

  // 重建明细表格
  wcRebuildTable(isEdit ? (data.detail_mode || "sku_based") : "sku_based");

  // 编辑模式：预填明细行
  if(isEdit && data._lines){
    data._lines.forEach(function(ln){
      wcAddLineWithData(ln);
    });
  } else {
    wcAddLine();
  }
}

function wcDetailModeChanged(){
  var newMode = document.getElementById("wc-detail-mode").value;
  if(newMode === _wcPrevDetailMode) return;

  // 检查当前是否有数据
  var hasData = false;
  var rows = document.querySelectorAll("#wc-lines-body tr");
  for(var i = 0; i < rows.length; i++){
    var inputs = rows[i].querySelectorAll("input[data-f]");
    for(var j = 0; j < inputs.length; j++){
      if(inputs[j].value.trim()){ hasData = true; break; }
    }
    if(hasData) break;
  }

  if(hasData){
    if(!confirm("切换明细模式将清空当前所有明细行，确认？")){
      document.getElementById("wc-detail-mode").value = _wcPrevDetailMode;
      return;
    }
  }

  _wcPrevDetailMode = newMode;
  _wcLineCount = 0;
  wcRebuildTable(newMode);
  wcAddLine();
}

function wcRebuildTable(mode){
  var head = document.getElementById("wc-lines-head");
  var body = document.getElementById("wc-lines-body");
  _wcLineCount = 0;
  if(mode === "carton_based"){
    head.innerHTML = '<tr><th>#</th><th>箱号</th><th>数量</th><th>长cm</th><th>宽cm</th><th>高cm</th><th>重量kg</th><th>备注</th><th></th></tr>';
  } else {
    head.innerHTML = '<tr><th>#</th><th>产品编码</th><th>产品名称</th><th>数量</th><th>长cm</th><th>宽cm</th><th>高cm</th><th>重量kg</th><th>备注</th><th></th></tr>';
  }
  body.innerHTML = "";
}

function wcAddLine(){
  _wcLineCount++;
  var n = _wcLineCount;
  var mode = document.getElementById("wc-detail-mode").value;
  var body = document.getElementById("wc-lines-body");
  var tr = document.createElement("tr");
  tr.id = "wc-line-" + n;
  if(mode === "carton_based"){
    tr.innerHTML =
      '<td>'+n+'</td>' +
      '<td><input data-f="carton_no" placeholder="箱号" /></td>' +
      '<td><input data-f="qty" type="number" placeholder="数量" /></td>' +
      '<td><input data-f="length_cm" type="number" placeholder="" /></td>' +
      '<td><input data-f="width_cm" type="number" placeholder="" /></td>' +
      '<td><input data-f="height_cm" type="number" placeholder="" /></td>' +
      '<td><input data-f="weight_kg" type="number" placeholder="" /></td>' +
      '<td><input data-f="remark" placeholder="" /></td>' +
      '<td><button style="padding:2px 8px;font-size:11px;width:auto;margin:0;" onclick="wcRemoveLine('+n+')">删</button></td>';
  } else {
    tr.innerHTML =
      '<td>'+n+'</td>' +
      '<td><input data-f="sku_code" placeholder="编码" /></td>' +
      '<td><input data-f="product_name" placeholder="名称" /></td>' +
      '<td><input data-f="qty" type="number" placeholder="数量" /></td>' +
      '<td><input data-f="length_cm" type="number" placeholder="" /></td>' +
      '<td><input data-f="width_cm" type="number" placeholder="" /></td>' +
      '<td><input data-f="height_cm" type="number" placeholder="" /></td>' +
      '<td><input data-f="weight_kg" type="number" placeholder="" /></td>' +
      '<td><input data-f="remark" placeholder="" /></td>' +
      '<td><button style="padding:2px 8px;font-size:11px;width:auto;margin:0;" onclick="wcRemoveLine('+n+')">删</button></td>';
  }
  body.appendChild(tr);
}

function wcAddLineWithData(ln){
  wcAddLine();
  var tr = document.getElementById("wc-line-" + _wcLineCount);
  if(!tr) return;
  var fields = ["sku_code","product_name","carton_no","qty","length_cm","width_cm","height_cm","weight_kg","remark"];
  fields.forEach(function(f){
    var input = tr.querySelector('input[data-f="'+f+'"]');
    if(input && ln[f]) input.value = ln[f];
  });
}

function wcRemoveLine(n){
  var tr = document.getElementById("wc-line-" + n);
  if(tr) tr.remove();
}

function wcCollectLines(){
  var rows = document.querySelectorAll("#wc-lines-body tr");
  var lines = [];
  for(var i = 0; i < rows.length; i++){
    var inputs = rows[i].querySelectorAll("input[data-f]");
    var obj = {};
    for(var j = 0; j < inputs.length; j++){
      obj[inputs[j].getAttribute("data-f")] = inputs[j].value.trim();
    }
    if(Number(obj.qty) > 0) lines.push(obj);
  }
  return lines;
}

function goEditWo(workorder_id){
  var _navSnap = navCaptureState();
  fetchApi({ action:"b2b_wo_detail", workorder_id:workorder_id }).then(function(res){
    if(!res || !res.ok){ alert("加载失败"); return; }
    var w = res.workorder;
    var editable = ["draft","issued","working"];
    if(editable.indexOf(w.status) < 0){ alert("当前状态不允许编辑"); return; }
    _navStack.push(_navSnap);
    _editingWoId = workorder_id;
    w._lines = res.lines || [];
    goView("wo_create");
    initWoCreate(w);
  });
}

function submitWo(){
  var detailMode = document.getElementById("wc-detail-mode").value;
  var opMode = document.getElementById("wc-op-mode").value;
  var obMode = document.getElementById("wc-ob-mode").value;
  var day = document.getElementById("wc-day").value;
  var customer = document.getElementById("wc-customer").value.trim();
  var destination = document.getElementById("wc-destination").value.trim();
  var orderRef = document.getElementById("wc-order-ref").value.trim();
  var extNo = document.getElementById("wc-ext-no").value.trim();
  var boxCount = Number(document.getElementById("wc-box-count").value) || 0;
  var palletCount = Number(document.getElementById("wc-pallet-count").value) || 0;
  var instr = document.getElementById("wc-instr").value.trim();
  var creator = document.getElementById("wc-creator").value.trim();
  var lines = wcCollectLines();

  if(!day){ alert("请选择计划出库日"); return; }
  if(!customer){ alert("请输入客户名"); return; }
  if(!creator){ alert(_editingWoId ? "请输入编辑人" : "请输入创建人"); return; }
  if(boxCount < 0 || palletCount < 0){ alert("出库箱数和托盘数不能为负数"); return; }
  if(detailMode === "carton_based"){
    if(boxCount <= 0 && palletCount <= 0){ alert("按箱模式下，出库箱数或出库托盘数至少填一项"); return; }
  } else {
    if(lines.length === 0){ alert("请至少录入一行明细（数量>0）"); return; }
  }

  var btn = document.getElementById("wc-submit-btn");
  if(_submitting) return;
  _submitting = true;
  var origBtnText = btn.textContent;
  btn.disabled = true;

  if(_editingWoId){
    // 编辑模式
    btn.textContent = "保存中...";
    fetchApi({
      action:"b2b_wo_update", workorder_id:_editingWoId,
      detail_mode:detailMode, operation_mode:opMode, outbound_mode:obMode,
      plan_day:day, customer_name:customer,
      external_workorder_no:extNo, instruction_text:instr,
      outbound_destination:destination, order_ref_no:orderRef,
      outbound_box_count:boxCount, outbound_pallet_count:palletCount,
      edited_by:creator, lines:lines
    }).then(function(res){
      _submitting = false; btn.disabled = false; btn.textContent = origBtnText;
      if(res && res.ok){
        var id = _editingWoId;
        _editingWoId = null;
        navDropTopIf("wo_detail");
        goWoDetail(id, true);
      } else {
        document.getElementById("wc-result").innerHTML = '<span class="bad">修改失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  } else {
    // 新建模式
    btn.textContent = "创建中...";
    fetchApi({
      action:"b2b_wo_create", detail_mode:detailMode, operation_mode:opMode,
      outbound_mode:obMode, plan_day:day,
      customer_name:customer,
      external_workorder_no:extNo, instruction_text:instr,
      outbound_destination:destination, order_ref_no:orderRef,
      outbound_box_count:boxCount, outbound_pallet_count:palletCount,
      created_by:creator, lines:lines, request_id:getOrCreateRid("wo")
    }).then(function(res){
      _submitting = false; btn.disabled = false; btn.textContent = origBtnText;
      if(res && res.ok){
        clearRid("wo");
        _editingWoId = null;
        goWoDetail(res.workorder_id, true);
      } else {
        document.getElementById("wc-result").innerHTML = '<span class="bad">创建失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  }
}

// ===== 作业单列表 =====
function initWoList(){
  var today = kstToday();
  var tmr = kstTomorrow();
  var yesterday = kstYesterday();
  var scope = _woListScope;
  _woListScope = "today";
  _currentWoScope = scope;
  var titleEl = document.getElementById("wl-title");

  if(scope === "next3"){
    titleEl.textContent = "未来三天出库作业单";
    document.getElementById("wl-start").value = today;
    document.getElementById("wl-end").value = kstDayOffset(2);
    loadWoListByScope("next3");
  } else if(scope === "tomorrow"){
    titleEl.textContent = "明日出库作业单";
    document.getElementById("wl-start").value = tmr;
    document.getElementById("wl-end").value = tmr;
    loadWoList();
  } else if(scope === "overdue"){
    titleEl.textContent = "逾期未完成出库作业单";
    document.getElementById("wl-start").value = "";
    document.getElementById("wl-end").value = "";
    loadWoListByScope("overdue");
  } else {
    titleEl.textContent = "今日出库作业单";
    document.getElementById("wl-start").value = today;
    document.getElementById("wl-end").value = today;
    loadWoListByScope("today");
  }
}

// 手动点「查询」按钮 — 纯日期范围，不带逾期分区
function loadWoList(){
  var s = document.getElementById("wl-start").value;
  var e = document.getElementById("wl-end").value;
  if(!s || !e){ alert("请选择日期"); return; }
  _currentWoScope = "custom";
  document.getElementById("wl-title").textContent = "出库作业单列表";
  var el = document.getElementById("wl-result");
  el.innerHTML = '<div class="q-empty">加载中...</div>';
  fetchApi({ action:"b2b_wo_list", start_day:s, end_day:e }).then(function(res){
    if(!res || !res.ok){ el.innerHTML = '<div class="bad">查询失败</div>'; return; }
    renderWoList(el, res.workorders||[], []);
  });
}

// 从首页卡片进入 — 带逾期分区
function loadWoListByScope(scope){
  var today = kstToday();
  var yesterday = kstYesterday();
  var el = document.getElementById("wl-result");
  el.innerHTML = '<div class="q-empty">加载中...</div>';

  if(scope === "overdue"){
    fetchApi({ action:"b2b_wo_list", start_day:B2B_EARLIEST_DAY, end_day:yesterday }).then(function(res){
      var all = (res && res.ok) ? (res.workorders||[]) : [];
      var overdue = all.filter(function(w){ return WO_INCOMPLETE_STATUS[w.status]; });
      renderWoList(el, [], overdue);
    });
  } else if(scope === "next3"){
    var endDay = kstDayOffset(2);
    Promise.all([
      fetchApi({ action:"b2b_wo_list", start_day:today, end_day:endDay }),
      fetchApi({ action:"b2b_wo_list", start_day:B2B_EARLIEST_DAY, end_day:yesterday })
    ]).then(function(results){
      var wos = (results[0] && results[0].ok) ? (results[0].workorders||[]) : [];
      var all = (results[1] && results[1].ok) ? (results[1].workorders||[]) : [];
      var overdue = all.filter(function(w){ return WO_INCOMPLETE_STATUS[w.status]; });
      renderWoList(el, wos, overdue);
    });
  } else {
    Promise.all([
      fetchApi({ action:"b2b_wo_list", start_day:today, end_day:today }),
      fetchApi({ action:"b2b_wo_list", start_day:B2B_EARLIEST_DAY, end_day:yesterday })
    ]).then(function(results){
      var wos = (results[0] && results[0].ok) ? (results[0].workorders||[]) : [];
      var all = (results[1] && results[1].ok) ? (results[1].workorders||[]) : [];
      var overdue = all.filter(function(w){ return WO_INCOMPLETE_STATUS[w.status]; });
      renderWoList(el, wos, overdue);
    });
  }
}

function renderWoList(container, wos, overdueWos){
  var html = '';

  if(overdueWos.length > 0){
    overdueWos.sort(function(a,b){
      if(a.plan_day !== b.plan_day) return a.plan_day < b.plan_day ? -1 : 1;
      var sp = (WO_STATUS_PRIORITY[a.status]||9) - (WO_STATUS_PRIORITY[b.status]||9);
      if(sp !== 0) return sp;
      return (a.is_accounted||0) - (b.is_accounted||0);
    });
    html += '<div class="list-section-title overdue-section-title">⚠ 逾期未完成（'+overdueWos.length+' 单）</div>';
    html += overdueWos.map(renderWoRow).join("");
  }

  if(wos.length > 0){
    wos.sort(function(a,b){
      var sp = (WO_STATUS_PRIORITY[a.status]||9) - (WO_STATUS_PRIORITY[b.status]||9);
      if(sp !== 0) return sp;
      return (a.is_accounted||0) - (b.is_accounted||0);
    });
    if(overdueWos.length > 0){
      var woSectionTitle = (_currentWoScope === "next3") ? "未来三天作业单" : "当期作业单";
      html += '<div class="list-section-title" style="margin-top:12px;">📦 '+woSectionTitle+'（'+wos.length+' 单）</div>';
    }
    html += wos.map(renderWoRow).join("");
  }

  if(!html) html = '<div class="q-empty">暂无作业单</div>';
  container.innerHTML = html;
}

function renderWoRow(w){
  var dimClass = (w.status==="cancelled") ? " row-dim" : "";
  var opLabel = modeDisplay(w.operation_mode);
  var obLabel = modeDisplay(w.outbound_mode);
  var noticeTag = '';
  // 优先级：取消强提醒 > 变更强提醒 > 弱标签
  if(w.has_cancel_notice){
    noticeTag = ' <span class="cancel-notice-tag">❌ 已取消，禁止发出</span>' +
      ' <button onclick="event.stopPropagation();woNoticeAction(\''+esc(w.workorder_id)+'\',\'cancelled\',\'ack\')" class="ack-btn">已确认取消</button>';
  } else if(w.status==="cancelled" && w.cancel_ack_at){
    noticeTag = ' <span class="ack-tag">已确认取消</span>' +
      ' <button onclick="event.stopPropagation();woNoticeAction(\''+esc(w.workorder_id)+'\',\'cancelled\',\'unack\')" class="unack-btn">取消确认</button>';
  } else if(w.has_update_notice){
    noticeTag = ' <span class="notice-tag">⚠ 要求已更新</span>' +
      ' <button onclick="event.stopPropagation();woNoticeAction(\''+esc(w.workorder_id)+'\',\'updated\',\'ack\')" class="ack-btn">已确认查看</button>';
  } else if(w.update_ack_at){
    noticeTag = ' <span class="ack-tag">已确认变更</span>' +
      ' <button onclick="event.stopPropagation();woNoticeAction(\''+esc(w.workorder_id)+'\',\'updated\',\'unack\')" class="unack-btn">取消确认</button>';
  }
  var accTag = w.is_accounted ? '<span class="acc-tag acc-yes">已记帐</span>' : '<span class="acc-tag acc-no">未记帐</span>';
  var accBtn = w.is_accounted
    ? '<span class="acc-btn" onclick="event.stopPropagation();setWoAccounted(\''+esc(w.workorder_id)+'\',0)">撤销记帐</span>'
    : '<span class="acc-btn" onclick="event.stopPropagation();setWoAccounted(\''+esc(w.workorder_id)+'\',1)">标记记帐</span>';
  var shipTag = '';
  if(w.status==="completed"){
    if(w.shipment_confirmed_at) shipTag = ' <span class="ship-tag ship-confirmed">已发货</span>';
    else if(w.pickup_vehicle_no) shipTag = ' <span class="ship-tag ship-pending-confirm">待发货确认</span>';
    else shipTag = ' <span class="ship-tag ship-pending-vehicle">待录车辆</span>';
  }
  return '<div class="wo-row'+dimClass+'" onclick="goWoDetail(\''+esc(w.workorder_id)+'\')">' +
    '<div><span class="st st-'+esc(w.status)+'">'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</span> ' +
    '<b>'+esc(w.workorder_id)+'</b> · '+esc(w.customer_name) + noticeTag + shipTag + accTag + accBtn + '</div>' +
    '<div class="meta">'+esc(w.plan_day)+' · '+esc(opLabel)+' · '+esc(obLabel) +
    ' · '+(isCartonNoLine(w,[]) && fmtOutboundQty(w.outbound_box_count, w.outbound_pallet_count)
      ? fmtOutboundQty(w.outbound_box_count, w.outbound_pallet_count)
      : w.total_qty+(w.total_qty_unit||"")+(w.total_weight_kg ? ' · '+w.total_weight_kg+'kg' : '')) +
    (w.external_workorder_no ? ' · WMS:'+esc(w.external_workorder_no) : '') + '</div>' +
  '</div>';
}

// ===== 作业单详情 =====
function goWoDetail(id, _skipNav){
  if(!_skipNav) navPush();
  _currentDetailId = id;
  goView("wo_detail");
  var card = document.getElementById("wo-detail-card");
  card.innerHTML = '<div class="muted">加载中...</div>';
  fetchApi({ action:"b2b_wo_detail", workorder_id:id }).then(function(res){
    if(!res || !res.ok){ card.innerHTML = '<div class="bad">加载失败: '+esc(res&&res.error||"")+'</div>'; return; }
    var w = res.workorder;
    var lines = res.lines || [];
    var isSku = (w.detail_mode || w.outbound_mode) !== "carton_based";

    // 状态流转按钮
    var WO_TRANSITIONS = {
      draft: ["issued","cancelled"], issued: ["completed","cancelled"],
      working: ["completed"], completed: [], cancelled: []
    };
    var statusBtns = (WO_TRANSITIONS[w.status]||[]).map(function(s){
      return '<button onclick="changeWoStatus(\''+esc(w.workorder_id)+'\',\''+s+'\')" class="'+(s==="cancelled"?"bad":"primary")+'" style="width:auto;padding:8px 16px;font-size:13px;">'+esc(WO_STATUS_BTN_LABEL[s]||WO_STATUS_LABEL[s]||s)+'</button>';
    }).join(" ");

    // 编辑按钮（draft/issued/working 都可编辑）
    var WO_EDITABLE = ["draft","issued","working"];
    var editBtn = (WO_EDITABLE.indexOf(w.status) >= 0) ?
      ' <button onclick="goEditWo(\''+esc(w.workorder_id)+'\')" style="width:auto;padding:8px 16px;font-size:13px;">编辑</button>' : '';

    // 模式显示
    var opLabel = modeDisplay(w.operation_mode);
    var obLabel = modeDisplay(w.outbound_mode);
    var dmLabel = DETAIL_MODE_LABEL[w.detail_mode] || DETAIL_MODE_LABEL[w.outbound_mode] || "(旧单未定义)";

    // 明细表
    var lineHead, lineRows;
    if(isSku){
      lineHead = '<tr><th>#</th><th>产品编码</th><th>产品名称</th><th>数量</th><th>长cm</th><th>宽cm</th><th>高cm</th><th>重量kg</th><th>备注</th></tr>';
      lineRows = lines.map(function(ln){
        return '<tr><td>'+ln.line_no+'</td><td>'+esc(ln.sku_code)+'</td><td>'+esc(ln.product_name)+'</td><td>'+ln.qty+'</td>' +
          '<td>'+(ln.length_cm||"")+'</td><td>'+(ln.width_cm||"")+'</td><td>'+(ln.height_cm||"")+'</td><td>'+(ln.weight_kg||"")+'</td><td>'+esc(ln.remark)+'</td></tr>';
      }).join("");
    } else {
      lineHead = '<tr><th>#</th><th>箱号</th><th>数量</th><th>长cm</th><th>宽cm</th><th>高cm</th><th>重量kg</th><th>备注</th></tr>';
      lineRows = lines.map(function(ln){
        return '<tr><td>'+ln.line_no+'</td><td>'+esc(ln.carton_no)+'</td><td>'+ln.qty+'</td>' +
          '<td>'+(ln.length_cm||"")+'</td><td>'+(ln.width_cm||"")+'</td><td>'+(ln.height_cm||"")+'</td><td>'+(ln.weight_kg||"")+'</td><td>'+esc(ln.remark)+'</td></tr>';
      }).join("");
    }

    // 提醒条（优先级：取消强提醒 > 变更强提醒 > 弱标签）
    var noticeHtml = '';
    if(w.has_cancel_notice){
      noticeHtml = '<div style="background:#ffebee;border:2px solid #d32f2f;border-radius:8px;padding:10px 14px;margin-bottom:10px;">' +
        '<div style="font-size:16px;font-weight:800;color:#d32f2f;">❌ 该工单已取消，禁止发出</div>' +
        '<button onclick="woNoticeAction(\''+esc(w.workorder_id)+'\',\'cancelled\',\'ack\')" style="margin-top:8px;width:auto;padding:6px 16px;font-size:13px;background:#d32f2f;color:#fff;border:none;border-radius:6px;cursor:pointer;">已确认取消</button>' +
        '</div>';
    } else if(w.status==="cancelled" && w.cancel_ack_at){
      noticeHtml = '<div style="background:#f5f5f5;border:1px solid #ccc;border-radius:8px;padding:8px 14px;margin-bottom:10px;font-size:12px;color:#888;">' +
        '已确认取消 · ' + new Date(w.cancel_ack_at).toLocaleString() +
        (w.cancel_ack_by ? ' · 确认人: '+esc(w.cancel_ack_by) : '') +
        ' <button onclick="woNoticeAction(\''+esc(w.workorder_id)+'\',\'cancelled\',\'unack\')" style="margin-left:8px;padding:2px 10px;font-size:11px;background:#eee;color:#666;border:1px solid #ccc;border-radius:4px;cursor:pointer;">取消确认</button></div>';
    }
    if(w.has_update_notice && !w.has_cancel_notice){
      noticeHtml += '<div style="background:#fff3e0;border:2px solid #e65100;border-radius:8px;padding:10px 14px;margin-bottom:10px;">' +
        '<div style="font-size:15px;font-weight:800;color:#e65100;">⚠ 该工单在操作中已被编辑，请按最新要求执行</div>' +
        '<div style="font-size:12px;color:#bf360c;margin-top:4px;">最近编辑时间: ' + (w.last_edited_at ? new Date(w.last_edited_at).toLocaleString() : '未知') +
        (w.last_edited_by ? ' · 编辑人: '+esc(w.last_edited_by) : '') + '</div>' +
        '<button onclick="woNoticeAction(\''+esc(w.workorder_id)+'\',\'updated\',\'ack\')" style="margin-top:8px;width:auto;padding:6px 16px;font-size:13px;background:#e65100;color:#fff;border:none;border-radius:6px;cursor:pointer;">已确认查看变更</button>' +
        '</div>';
    } else if(w.update_ack_at && !w.has_update_notice && !w.has_cancel_notice){
      noticeHtml += '<div style="background:#f5f5f5;border:1px solid #ccc;border-radius:8px;padding:8px 14px;margin-bottom:10px;font-size:12px;color:#888;">' +
        '已确认变更 · ' + new Date(w.update_ack_at).toLocaleString() +
        (w.update_ack_by ? ' · 确认人: '+esc(w.update_ack_by) : '') +
        ' <button onclick="woNoticeAction(\''+esc(w.workorder_id)+'\',\'updated\',\'unack\')" style="margin-left:8px;padding:2px 10px;font-size:11px;background:#eee;color:#666;border:1px solid #ccc;border-radius:4px;cursor:pointer;">取消确认</button></div>';
    }

    card.innerHTML =
      '<div style="font-size:18px;font-weight:800;margin-bottom:10px;">' +
        esc(w.workorder_id) + ' <span class="st st-'+esc(w.status)+'">'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</span>' +
      '</div>' +
      noticeHtml +
      '<div class="detail-field"><b>客户:</b> '+esc(w.customer_name)+'</div>' +
      '<div class="detail-field"><b>计划出库日:</b> '+esc(w.plan_day)+'</div>' +
      '<div class="detail-field"><b>操作模式:</b> '+esc(opLabel)+'</div>' +
      '<div class="detail-field"><b>出库模式:</b> '+esc(obLabel)+'</div>' +
      (w.outbound_destination ? '<div class="detail-field"><b>出库目的地:</b> '+esc(w.outbound_destination)+'</div>' : '') +
      (w.order_ref_no ? '<div class="detail-field"><b>발주번호:</b> '+esc(w.order_ref_no)+'</div>' : '') +
      (fmtOutboundQty(w.outbound_box_count, w.outbound_pallet_count) ? '<div class="detail-field"><b>出库量:</b> '+esc(fmtOutboundQty(w.outbound_box_count, w.outbound_pallet_count))+'</div>' : '') +
      '<div class="detail-field"><b>汇总:</b> '+(isCartonNoLine(w,lines) ? '未录入逐箱汇总' : w.total_qty+(w.total_qty_unit||"")+' · '+w.total_weight_kg+'kg' + (w.total_cbm ? ' · '+w.total_cbm+'m³' : '')) + '</div>' +
      (w.external_workorder_no ? '<div class="detail-field"><b>WMS工单号:</b> '+esc(w.external_workorder_no)+'</div>' : '') +
      (w.instruction_text ? '<div class="detail-field"><b>作业指示:</b> '+esc(w.instruction_text)+'</div>' : '') +
      '<div class="detail-field muted" style="font-size:12px;"><b>创建人:</b> '+esc(w.created_by)+' · 创建时间: '+new Date(w.created_at).toLocaleString()+'</div>' +
      '<div class="detail-field">' +
        (w.is_accounted
          ? '<span class="acc-tag acc-yes">已记帐</span> <span class="muted" style="font-size:12px;">记帐人: '+esc(w.accounted_by||"")+' · '+(w.accounted_at ? new Date(w.accounted_at).toLocaleString() : "")+'</span> <span class="acc-btn" onclick="setWoAccounted(\''+esc(w.workorder_id)+'\',0)">撤销记帐</span>'
          : '<span class="acc-tag acc-no">未记帐</span> <span class="acc-btn" onclick="setWoAccounted(\''+esc(w.workorder_id)+'\',1)">标记记帐</span>') +
      '</div>' +

      renderWoPickupSection(w) +

      '<div style="margin:12px 0;" class="no-print">' + statusBtns + editBtn +
        ' <button onclick="printWo(\''+esc(w.workorder_id)+'\')" style="width:auto;padding:8px 16px;font-size:13px;">打印</button>' +
      '</div>' +

      // 附件区（动态渲染）
      '<div id="att-section-wrap" class="no-print"></div>' +

      '<div style="font-size:14px;font-weight:700;margin:12px 0 6px;">明细 ('+lines.length+'行)</div>' +
      (lines.length === 0 && !isSku
        ? '<div style="color:#888;font-size:13px;margin:8px 0;">未录入逐箱明细（以顶部出库量为准）</div>'
        : '<div style="overflow-x:auto;"><table class="line-table"><thead>'+lineHead+'</thead><tbody>'+lineRows+'</tbody>' +
          '<tfoot><tr style="font-weight:700;"><td colspan="'+(isSku?3:2)+'">合计</td><td>'+w.total_qty+'</td>' +
          '<td colspan="3"></td><td>'+w.total_weight_kg+'</td><td></td></tr></tfoot></table></div>');

    // 渲染附件区
    renderAttachmentSection(w.workorder_id, w.status, res.attachments || []);

    // 现场执行结果摘要
    var resultDiv = document.createElement("div");
    resultDiv.className = "cross-ref";
    resultDiv.innerHTML = '<div class="cross-ref-title">现场执行结果</div><div class="muted">加载中...</div>';
    var detailCard = document.getElementById("wo-detail-card");
    detailCard.appendChild(resultDiv);

    fetchApi({ action:"b2b_op_result_list", day_kst:w.plan_day }).then(function(resR){
      if(!resR || !resR.ok){ resultDiv.innerHTML = '<div class="cross-ref-title">现场执行结果</div><div class="muted">查询失败</div>'; return; }
      var matched = (resR.results||[]).filter(function(r){ return r.source_order_no === w.workorder_id; });
      if(!matched.length){ resultDiv.innerHTML = '<div class="cross-ref-title">现场执行结果</div><div class="q-empty">暂无</div>'; return; }
      resultDiv.innerHTML = '<div class="cross-ref-title">现场执行结果（'+matched.length+' 条）</div>' +
        matched.map(function(r){
          var stLabel = r.status === "completed" ? "已完成" : (r.status === "draft" ? "草稿" : r.status);
          return '<div style="border-bottom:1px solid #f0f0f0;padding:4px 0;font-size:12px;">' +
            '<span class="st st-'+esc(r.status)+'">'+esc(stLabel)+'</span> '+esc(r.operation_mode||"(未填)") +
            (r.outbound_box_count ? ' · 箱:'+r.outbound_box_count : '') +
            (r.outbound_pallet_count ? ' · 托:'+r.outbound_pallet_count : '') +
            (r.confirm_badge ? ' · 确认:'+esc(r.confirm_badge) : '') +
          '</div>';
        }).join("");
    });
  });
}

function renderWoPickupSection(w){
  if(w.status !== "completed") return '';
  var html = '<div class="pickup-section">';
  html += '<div class="pickup-title">🚚 提货车辆信息</div>';
  if(w.pickup_vehicle_no){
    html += '<div class="detail-field"><b>车牌号:</b> '+esc(w.pickup_vehicle_no)+'</div>';
    html += '<div class="detail-field"><b>司机姓名:</b> '+esc(w.pickup_driver_name||"(未填)")+'</div>';
    html += '<div class="detail-field"><b>司机电话:</b> '+esc(w.pickup_driver_phone||"(未填)")+'</div>';
    if(w.pickup_remark) html += '<div class="detail-field"><b>备注:</b> '+esc(w.pickup_remark)+'</div>';
    html += '<div class="detail-field muted" style="font-size:12px;"><b>登记人:</b> '+esc(w.pickup_recorded_by)+' · '+(w.pickup_recorded_at ? new Date(w.pickup_recorded_at).toLocaleString() : "")+'</div>';

    if(w.shipment_confirmed_at){
      html += '<div class="detail-field" style="margin-top:8px;"><span class="ship-tag ship-confirmed">已发货确认</span> <span class="muted" style="font-size:12px;">确认人: '+esc(w.shipment_confirmed_by)+' · '+new Date(w.shipment_confirmed_at).toLocaleString()+'</span></div>';
    } else {
      html += '<div style="margin-top:8px;" class="no-print">' +
        '<button onclick="woSetPickupInfo(\''+esc(w.workorder_id)+'\')" style="width:auto;padding:6px 14px;font-size:13px;">修改车辆信息</button> ' +
        '<button onclick="woConfirmShipped(\''+esc(w.workorder_id)+'\')" style="width:auto;padding:6px 14px;font-size:13px;background:#27ae60;color:#fff;">确认已发货</button>' +
        '</div>';
    }
  } else {
    html += '<div class="q-empty">尚未登记车辆信息</div>';
    html += '<div class="no-print"><button onclick="woSetPickupInfo(\''+esc(w.workorder_id)+'\')" style="width:auto;padding:6px 14px;font-size:13px;">登记车辆信息</button></div>';
  }
  html += '</div>';
  return html;
}

function woSetPickupInfo(id){
  var vehicleNo = prompt("车牌号（必填）：");
  if(vehicleNo === null) return;
  vehicleNo = vehicleNo.trim();
  if(!vehicleNo){ alert("车牌号不能为空"); return; }
  var driverName = prompt("司机姓名：") || "";
  var driverPhone = prompt("司机电话：") || "";
  var remark = prompt("提货备注（可留空）：") || "";
  var recordedBy = prompt("登记人（必填）：");
  if(recordedBy === null) return;
  recordedBy = recordedBy.trim();
  if(!recordedBy){ alert("登记人不能为空"); return; }

  fetchApi({
    action:"b2b_wo_set_pickup_info", workorder_id:id,
    pickup_vehicle_no:vehicleNo, pickup_driver_name:driverName.trim(),
    pickup_driver_phone:driverPhone.trim(), pickup_remark:remark.trim(),
    pickup_recorded_by:recordedBy
  }).then(function(res){
    if(res && res.ok){
      alert("车辆信息已登记");
      goWoDetail(id, true);
    } else {
      alert("登记失败: "+(res&&res.error||"unknown"));
    }
  });
}

function woConfirmShipped(id){
  var confirmedBy = prompt("确认人姓名（必填）：");
  if(confirmedBy === null) return;
  confirmedBy = confirmedBy.trim();
  if(!confirmedBy){ alert("确认人不能为空"); return; }
  if(!confirm("确认该作业单已发货？\n确认后首页提醒将消失。")) return;

  fetchApi({ action:"b2b_wo_confirm_shipped", workorder_id:id, shipment_confirmed_by:confirmedBy }).then(function(res){
    if(res && res.ok){
      alert("已确认发货");
      goWoDetail(id, true);
    } else {
      alert("确认失败: "+(res&&res.error||"unknown"));
    }
  });
}

function changeWoStatus(id, status){
  var label = WO_STATUS_BTN_LABEL[status] || WO_STATUS_LABEL[status] || status;
  if(status === "cancelled"){
    if(!confirm("确认取消作业单 "+id+"？")) return;
  } else {
    if(!confirm("确认将 "+id+" 状态改为「"+label+"」？")) return;
  }
  fetchApi({ action:"b2b_wo_update_status", workorder_id:id, status:status }).then(function(res){
    if(res && res.ok){
      goWoDetail(id, true);
    } else {
      alert("状态更新失败: "+(res&&res.error||"unknown"));
    }
  });
}

function woNoticeAction(id, kind, op){
  var ackBy = "";
  if(op === "ack"){
    ackBy = prompt("请输入你的名字（确认人）：");
    if(ackBy === null) return;
    ackBy = ackBy.trim();
    if(!ackBy){ alert("确认人不能为空"); return; }
  } else {
    if(!confirm("确认取消确认？提醒将重新出现。")) return;
  }
  fetchApi({ action:"b2b_wo_ack_notice", workorder_id:id, kind:kind, op:op, ack_by:ackBy }).then(function(res){
    if(res && res.ok){
      if(document.getElementById("wo-detail-card").innerHTML.indexOf(id) >= 0){
        goWoDetail(id, true);
      } else {
        reloadCurrentWoList();
      }
    } else {
      alert("操作失败: "+(res&&res.error||"unknown"));
    }
  });
}

// ===== 记帐标记 =====
function setPlanAccounted(plan_id, val){
  if(val){
    var by = prompt("请输入记帐人姓名：");
    if(by === null) return;
    by = by.trim();
    if(!by){ alert("记帐人不能为空"); return; }
    fetchApi({ action:"b2b_plan_set_accounted", plan_id:plan_id, is_accounted:1, accounted_by:by }).then(function(res){
      if(res && res.ok){ refreshAfterAccounted("plan", plan_id); } else { alert("操作失败: "+(res&&res.error||"")); }
    });
  } else {
    if(!confirm("确认撤销记帐？")) return;
    fetchApi({ action:"b2b_plan_set_accounted", plan_id:plan_id, is_accounted:0, accounted_by:"" }).then(function(res){
      if(res && res.ok){ refreshAfterAccounted("plan", plan_id); } else { alert("操作失败: "+(res&&res.error||"")); }
    });
  }
}
function setWoAccounted(wo_id, val){
  if(val){
    var by = prompt("请输入记帐人姓名：");
    if(by === null) return;
    by = by.trim();
    if(!by){ alert("记帐人不能为空"); return; }
    fetchApi({ action:"b2b_wo_set_accounted", workorder_id:wo_id, is_accounted:1, accounted_by:by }).then(function(res){
      if(res && res.ok){ refreshAfterAccounted("wo", wo_id); } else { alert("操作失败: "+(res&&res.error||"")); }
    });
  } else {
    if(!confirm("确认撤销记帐？")) return;
    fetchApi({ action:"b2b_wo_set_accounted", workorder_id:wo_id, is_accounted:0, accounted_by:"" }).then(function(res){
      if(res && res.ok){ refreshAfterAccounted("wo", wo_id); } else { alert("操作失败: "+(res&&res.error||"")); }
    });
  }
}
function refreshAfterAccounted(type, id){
  if(type === "plan"){
    var detailCard = document.getElementById("plan-detail-card");
    if(detailCard && detailCard.innerHTML.indexOf(id) >= 0){ goPlanDetail(id, true); } else { reloadCurrentPlanList(); }
  } else {
    var detailCard = document.getElementById("wo-detail-card");
    if(detailCard && detailCard.innerHTML.indexOf(id) >= 0){ goWoDetail(id, true); } else { reloadCurrentWoList(); }
  }
}
function reloadCurrentPlanList(){
  if(_currentPlanScope === "unfinished" || _currentPlanScope === "overdue" || _currentPlanScope === "next3"){
    loadPlanListByRange(_currentPlanScope);
  } else {
    loadPlanListByRange(null);
  }
}
function reloadCurrentWoList(){
  if(_currentWoScope === "next3" || _currentWoScope === "today" || _currentWoScope === "overdue"){
    loadWoListByScope(_currentWoScope);
  } else {
    loadWoList();
  }
}

// ===== 附件区 =====
var ATT_MAX = 3;
var ATT_ALLOWED = { "image/jpeg":1, "image/png":1, "image/webp":1 };
var ATT_MAX_SIZE = 5 * 1024 * 1024; // 5MB
var ATT_CAN_UPLOAD = { draft:1, issued:1 };
var ATT_CAN_DELETE = { draft:1 };

function attFileUrl(attachment_id){
  return API_URL + "?action=b2b_attachment_file&id=" + encodeURIComponent(attachment_id) + "&k=" + encodeURIComponent(getKey());
}

function renderAttachmentSection(workorder_id, status, attachments){
  var wrap = document.getElementById("att-section-wrap");
  if(!wrap) return;

  var canUpload = !!ATT_CAN_UPLOAD[status] && attachments.length < ATT_MAX;
  var canDelete = !!ATT_CAN_DELETE[status];

  var html = '<div class="att-section">';
  html += '<div class="att-title">操作要求附件（' + attachments.length + '/' + ATT_MAX + ' 张）</div>';
  html += '<div class="att-grid">';

  attachments.forEach(function(att){
    var url = attFileUrl(att.attachment_id);
    var timeStr = new Date(att.created_at).toLocaleString();
    html += '<div class="att-item">';
    html += '<img class="att-thumb" src="'+esc(url)+'" onclick="showLightbox(\''+esc(url)+'\')" alt="'+esc(att.file_name)+'" />';
    html += '<div class="att-fname" title="'+esc(att.file_name)+'">'+esc(att.file_name)+'</div>';
    html += '<div class="att-time">'+esc(timeStr)+'</div>';
    if(canDelete){
      html += '<button class="att-del" onclick="deleteAttachment(\''+esc(att.attachment_id)+'\',\''+esc(workorder_id)+'\')">删除</button>';
    }
    html += '</div>';
  });

  if(canUpload){
    html += '<div class="att-item">';
    html += '<div class="att-upload-btn" onclick="document.getElementById(\'att-file-input\').click()">+ 上传图片</div>';
    html += '<input type="file" id="att-file-input" accept="image/jpeg,image/png,image/webp" style="display:none;" ' +
      'onchange="uploadAttachment(this,\''+esc(workorder_id)+'\')" />';
    html += '</div>';
  }

  html += '</div>'; // att-grid
  html += '<div id="att-msg" class="att-msg"></div>';
  html += '</div>'; // att-section
  wrap.innerHTML = html;
}

function uploadAttachment(input, workorder_id){
  var msgEl = document.getElementById("att-msg");
  if(msgEl) msgEl.innerHTML = "";
  if(!input.files || !input.files[0]) return;
  var file = input.files[0];
  input.value = ""; // 重置允许重复选同一文件

  // 前端格式校验
  if(!ATT_ALLOWED[file.type]){
    if(msgEl) msgEl.innerHTML = '<span class="bad">不支持的格式，仅允许 jpg/png/webp</span>';
    return;
  }
  // 前端大小校验
  if(file.size > ATT_MAX_SIZE){
    if(msgEl) msgEl.innerHTML = '<span class="bad">文件过大，单张上限 5MB</span>';
    return;
  }

  if(msgEl) msgEl.innerHTML = '<span class="muted">上传中...</span>';

  var fd = new FormData();
  fd.append("file", file);
  fd.append("workorder_id", workorder_id);
  fd.append("uploaded_by", "");
  fd.append("k", getKey());

  fetch(API_URL + "?action=b2b_attachment_upload", {
    method: "POST",
    body: fd
  }).then(function(r){ return r.json(); }).then(function(res){
    if(res && res.ok){
      // 刷新附件区
      goWoDetail(workorder_id, true);
    } else {
      if(msgEl) msgEl.innerHTML = '<span class="bad">上传失败: '+esc(res&&res.error||"unknown")+'</span>';
    }
  }).catch(function(err){
    if(msgEl) msgEl.innerHTML = '<span class="bad">网络错误: '+esc(String(err))+'</span>';
  });
}

function deleteAttachment(attachment_id, workorder_id){
  if(!confirm("确认删除此附件？删除后不可恢复。")) return;
  fetchApi({ action:"b2b_attachment_delete", attachment_id:attachment_id }).then(function(res){
    if(res && res.ok){
      goWoDetail(workorder_id, true);
    } else {
      alert("删除失败: " + (res&&res.error||"unknown"));
    }
  });
}

function showLightbox(url){
  var overlay = document.createElement("div");
  overlay.className = "lightbox-overlay";
  overlay.onclick = function(){ document.body.removeChild(overlay); };
  var img = document.createElement("img");
  img.src = url;
  img.onclick = function(e){ e.stopPropagation(); };
  overlay.appendChild(img);
  document.body.appendChild(overlay);
}

// ===== 模板下载 =====
var SKU_TPL_HEADERS = ["产品编码","产品名称","数量","长cm","宽cm","高cm","重量kg","备注"];
var CARTON_TPL_HEADERS = ["箱号","数量","长cm","宽cm","高cm","重量kg","备注"];
// SKU 字段映射（表头→data-f属性名）
var SKU_FIELD_MAP = {"产品编码":"sku_code","产品名称":"product_name","数量":"qty","长cm":"length_cm","宽cm":"width_cm","高cm":"height_cm","重量kg":"weight_kg","备注":"remark"};
var CARTON_FIELD_MAP = {"箱号":"carton_no","数量":"qty","长cm":"length_cm","宽cm":"width_cm","高cm":"height_cm","重量kg":"weight_kg","备注":"remark"};

function wcDownloadTemplate(){
  var mode = document.getElementById("wc-detail-mode").value;
  var headers = (mode === "carton_based") ? CARTON_TPL_HEADERS : SKU_TPL_HEADERS;
  var fileName = (mode === "carton_based") ? "箱模式模板.xlsx" : "SKU模式模板.xlsx";

  var ws = XLSX.utils.aoa_to_sheet([headers]);
  // 设置列宽
  ws["!cols"] = headers.map(function(){ return { wch: 14 }; });
  var wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "明细");
  XLSX.writeFile(wb, fileName);
}

// ===== Excel 导入 =====
function wcImportExcel(input){
  var errEl = document.getElementById("wc-import-err");
  errEl.innerHTML = "";
  if(!input.files || !input.files[0]) return;
  var file = input.files[0];
  // 重置 input 以允许再次选择同一文件
  input.value = "";

  var reader = new FileReader();
  reader.onload = function(e){
    try {
      var data = new Uint8Array(e.target.result);
      var wb = XLSX.read(data, { type:"array" });
      var ws = wb.Sheets[wb.SheetNames[0]];
      var rows = XLSX.utils.sheet_to_json(ws, { header:1, defval:"" });
    } catch(ex){
      errEl.innerHTML = '<span class="bad">文件解析失败: '+esc(String(ex))+'</span>';
      return;
    }

    if(!rows || rows.length < 2){
      errEl.innerHTML = '<span class="bad">文件为空或只有表头，无数据行</span>';
      return;
    }

    var mode = document.getElementById("wc-detail-mode").value;
    var isCarton = (mode === "carton_based");
    var fieldMap = isCarton ? CARTON_FIELD_MAP : SKU_FIELD_MAP;
    var expectedHeaders = isCarton ? CARTON_TPL_HEADERS : SKU_TPL_HEADERS;

    // 解析表头行 → 建立列索引
    var headerRow = rows[0];
    var colIndex = {}; // fieldName → colIdx
    for(var c = 0; c < headerRow.length; c++){
      var h = String(headerRow[c]).trim();
      if(fieldMap[h]) colIndex[fieldMap[h]] = c;
    }

    // 检查必要列是否存在
    var missingCols = [];
    if(isCarton){
      if(colIndex["carton_no"] === undefined) missingCols.push("箱号");
      if(colIndex["qty"] === undefined) missingCols.push("数量");
    } else {
      if(colIndex["sku_code"] === undefined && colIndex["product_name"] === undefined) missingCols.push("产品编码 或 产品名称");
      if(colIndex["qty"] === undefined) missingCols.push("数量");
    }
    if(missingCols.length > 0){
      errEl.innerHTML = '<span class="bad">缺少必要列: '+esc(missingCols.join("、"))+'</span>';
      return;
    }

    // 逐行解析 + 校验
    var parsed = [];
    var errors = [];
    for(var r = 1; r < rows.length; r++){
      var row = rows[r];
      // 跳过完全空行
      var allEmpty = true;
      for(var cc = 0; cc < row.length; cc++){
        if(String(row[cc]).trim()){ allEmpty = false; break; }
      }
      if(allEmpty) continue;

      var lineNum = r + 1; // Excel 行号（含表头）
      var obj = {};
      for(var field in colIndex){
        obj[field] = String(row[colIndex[field]] === undefined ? "" : row[colIndex[field]]).trim();
      }

      // 校验数量
      var qtyVal = Number(obj.qty);
      if(!obj.qty || isNaN(qtyVal) || qtyVal <= 0){
        errors.push("第"+lineNum+"行: 数量必须是大于0的数字");
      }

      // SKU 模式：编码或名称至少一个
      if(!isCarton){
        if(!obj.sku_code && !obj.product_name){
          errors.push("第"+lineNum+"行: 产品编码和产品名称至少填一个");
        }
      }
      // 箱模式：箱号不能为空
      if(isCarton){
        if(!obj.carton_no){
          errors.push("第"+lineNum+"行: 箱号不能为空");
        }
      }

      // 长宽高重量：填了就必须是 >= 0 的合法数字
      var numFields = [["length_cm","长cm"],["width_cm","宽cm"],["height_cm","高cm"],["weight_kg","重量kg"]];
      for(var nf = 0; nf < numFields.length; nf++){
        var val = obj[numFields[nf][0]];
        if(val !== "" && val !== undefined){
          var num = Number(val);
          if(isNaN(num) || num < 0){
            errors.push("第"+lineNum+"行: "+numFields[nf][1]+"必须是>=0的数字");
          }
        }
      }

      parsed.push(obj);
    }

    if(parsed.length === 0){
      errEl.innerHTML = '<span class="bad">未找到有效数据行</span>';
      return;
    }

    if(errors.length > 0){
      errEl.innerHTML = '<span class="bad">导入失败：<br>' + errors.map(function(e){ return '• '+esc(e); }).join('<br>') + '</span>';
      return;
    }

    // 覆盖确认
    var hasExisting = false;
    var existingRows = document.querySelectorAll("#wc-lines-body tr");
    for(var er = 0; er < existingRows.length; er++){
      var inputs = existingRows[er].querySelectorAll("input[data-f]");
      for(var ei = 0; ei < inputs.length; ei++){
        if(inputs[ei].value.trim()){ hasExisting = true; break; }
      }
      if(hasExisting) break;
    }
    if(hasExisting){
      if(!confirm("导入将替换当前所有明细行，确认？")) return;
    }

    // 清空并填充
    _wcLineCount = 0;
    document.getElementById("wc-lines-body").innerHTML = "";
    parsed.forEach(function(obj){
      wcAddLineWithData(obj);
    });

    errEl.innerHTML = '<span class="ok">成功导入 '+parsed.length+' 行</span>';
  };
  reader.readAsArrayBuffer(file);
}

// ===== 本地生成二维码 data URL =====
function makeQrDataUrl(text){
  var qr = qrcode(0, "M");
  qr.addData(text);
  qr.make();
  return qr.createDataURL(4, 0);
}

// 出库量格式化（详情页+打印页共用）
function fmtOutboundQty(box, pallet){
  var parts = [];
  if(box) parts.push(box + "箱");
  if(pallet) parts.push(pallet + "托");
  return parts.join(" / ");
}

// carton_based 且无逐箱明细汇总（lines空或汇总全为0）
function isCartonNoLine(w, lines){
  var isSku = (w.detail_mode || w.outbound_mode) !== "carton_based";
  if(isSku) return false;
  if(lines && lines.length > 0) return false;
  return (!w.total_qty && !w.total_weight_kg && !w.total_cbm);
}

// ===== 打印 =====
function printWo(id){
  fetchApi({ action:"b2b_wo_detail", workorder_id:id }).then(function(res){
    if(!res || !res.ok){ alert("加载失败"); return; }
    var w = res.workorder;
    var lines = res.lines || [];
    var attachments = res.attachments || [];
    var attCount = attachments.length;
    var isSku = (w.detail_mode || w.outbound_mode) !== "carton_based";
    var qrDataUrl = makeQrDataUrl(w.workorder_id);

    var opLabel = w.operation_mode || "";
    var obLabel = w.outbound_mode || "";

    // 明细表格 HTML
    var thead, tbody;
    if(isSku){
      thead = '<tr><th>#</th><th>产品编码</th><th>产品名称</th><th>数量</th><th>长cm</th><th>宽cm</th><th>高cm</th><th>重量kg</th><th>备注</th></tr>';
      tbody = lines.map(function(ln){
        return '<tr><td>'+ln.line_no+'</td><td>'+esc(ln.sku_code)+'</td><td>'+esc(ln.product_name)+'</td><td>'+ln.qty+'</td>' +
          '<td>'+(ln.length_cm||"")+'</td><td>'+(ln.width_cm||"")+'</td><td>'+(ln.height_cm||"")+'</td><td>'+(ln.weight_kg||"")+'</td><td>'+esc(ln.remark)+'</td></tr>';
      }).join("");
    } else {
      thead = '<tr><th>#</th><th>箱号</th><th>数量</th><th>长cm</th><th>宽cm</th><th>高cm</th><th>重量kg</th><th>备注</th></tr>';
      tbody = lines.map(function(ln){
        return '<tr><td>'+ln.line_no+'</td><td>'+esc(ln.carton_no)+'</td><td>'+ln.qty+'</td>' +
          '<td>'+(ln.length_cm||"")+'</td><td>'+(ln.width_cm||"")+'</td><td>'+(ln.height_cm||"")+'</td><td>'+(ln.weight_kg||"")+'</td><td>'+esc(ln.remark)+'</td></tr>';
      }).join("");
    }
    var footColspan = isSku ? 3 : 2;

    // 附件页 HTML（每张一页）
    var attPagesHtml = "";
    attachments.forEach(function(att, idx){
      var url = attFileUrl(att.attachment_id);
      attPagesHtml +=
        '<div class="att-page">' +
          '<div class="att-page-header">' +
            '<span class="label">作业单号：</span>'+esc(w.workorder_id) +
            '<span style="float:right;">附件 '+(idx+1)+'/'+attCount+'</span>' +
          '</div>' +
          '<div class="att-img-wrap"><img class="att-img" src="'+esc(url)+'" alt="附件'+(idx+1)+'"/></div>' +
          '<div class="att-fname">'+esc(att.file_name)+'</div>' +
        '</div>';
    });

    var html = '<!doctype html><html><head><meta charset="utf-8"/>' +
      '<title>打印 - '+esc(w.workorder_id)+'</title>' +
      '<style>' +
      'body{font-family:"Microsoft YaHei","Helvetica Neue",sans-serif;margin:20px 30px;color:#000;}' +
      '.print-header{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:3px solid #000;padding-bottom:10px;margin-bottom:14px;}' +
      '.print-title{font-size:22px;font-weight:900;}' +
      '.print-sub{font-size:13px;color:#333;margin-top:4px;}' +
      '.qr-box{text-align:center;}' +
      '.qr-label{font-size:10px;color:#666;margin-top:2px;}' +
      '.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px 20px;font-size:13px;margin-bottom:14px;}' +
      '.info-grid .label{font-weight:700;}' +
      'table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px;}' +
      'th,td{border:1px solid #333;padding:5px 6px;text-align:left;}' +
      'th{background:#eee;font-weight:700;}' +
      'tfoot td{font-weight:700;}' +
      '.sig-row{display:flex;gap:40px;margin-top:30px;font-size:13px;}' +
      '.sig-item{flex:1;}' +
      '.sig-line{border-bottom:1px solid #333;height:30px;margin-top:4px;}' +
      '.att-hint{margin-top:16px;font-size:12px;color:#666;text-align:center;border-top:1px dashed #ccc;padding-top:8px;}' +
      '.att-page{page-break-before:always;text-align:center;}' +
      '.att-page-header{font-size:14px;font-weight:700;border-bottom:2px solid #000;padding-bottom:8px;margin-bottom:16px;text-align:left;}' +
      '.att-img-wrap{display:flex;justify-content:center;align-items:flex-start;}' +
      '.att-img{max-width:170mm;max-height:230mm;object-fit:contain;}' +
      '.att-fname{margin-top:10px;font-size:11px;color:#555;word-break:break-all;overflow-wrap:break-word;}' +
      '@media print{@page{size:A4;margin:15mm 20mm;} body{margin:0;}}' +
      '</style></head><body>' +

      '<div class="print-header">' +
        '<div>' +
          '<div class="print-title">出库作业单</div>' +
          '<div class="print-sub">CK 仓储</div>' +
        '</div>' +
        '<div class="qr-box"><img width="120" height="120" src="'+qrDataUrl+'" alt="QR"/><div class="qr-label">'+esc(w.workorder_id)+'</div></div>' +
      '</div>' +

      '<div class="info-grid">' +
        '<div><span class="label">作业单号：</span>'+esc(w.workorder_id)+'</div>' +
        '<div><span class="label">状态：</span>'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</div>' +
        '<div><span class="label">客户：</span>'+esc(w.customer_name)+'</div>' +
        '<div><span class="label">计划出库日：</span>'+esc(w.plan_day)+'</div>' +
        (opLabel ? '<div><span class="label">操作模式：</span>'+esc(opLabel)+'</div>' : '') +
        (obLabel ? '<div><span class="label">出库模式：</span>'+esc(obLabel)+'</div>' : '') +
        (w.outbound_destination ? '<div><span class="label">出库目的地：</span>'+esc(w.outbound_destination)+'</div>' : '') +
        (w.order_ref_no ? '<div><span class="label">발주번호：</span>'+esc(w.order_ref_no)+'</div>' : '') +
        (fmtOutboundQty(w.outbound_box_count, w.outbound_pallet_count) ? '<div><span class="label">出库量：</span>'+fmtOutboundQty(w.outbound_box_count, w.outbound_pallet_count)+'</div>' : '') +
        '<div><span class="label">汇总：</span>'+(isCartonNoLine(w,lines) ? '未录入逐箱汇总' : w.total_qty+(w.total_qty_unit||"")+' · '+w.total_weight_kg+'kg'+(w.total_cbm?' · '+w.total_cbm+'m³':''))+'</div>' +
        (w.external_workorder_no ? '<div><span class="label">WMS工单号：</span>'+esc(w.external_workorder_no)+'</div>' : '') +
        (w.instruction_text ? '<div style="grid-column:1/-1;"><span class="label">作业指示：</span>'+esc(w.instruction_text)+'</div>' : '') +
      '</div>' +

      (lines.length === 0 && !isSku
        ? '<div style="color:#888;font-size:13px;margin:12px 0;">无逐箱明细（以顶部出库量为准）</div>'
        : '<table><thead>'+thead+'</thead><tbody>'+tbody+'</tbody>' +
          '<tfoot><tr><td colspan="'+footColspan+'">合计</td><td>'+w.total_qty+'</td><td colspan="3"></td><td>'+w.total_weight_kg+'</td><td></td></tr></tfoot></table>') +

      '<div class="sig-row">' +
        '<div class="sig-item"><span class="label">制单人：</span>'+esc(w.created_by)+'<div class="sig-line"></div></div>' +
        '<div class="sig-item"><span class="label">仓库确认：</span><div class="sig-line"></div></div>' +
        '<div class="sig-item"><span class="label">客户签收：</span><div class="sig-line"></div></div>' +
        '<div class="sig-item"><span class="label">日期：</span><div class="sig-line"></div></div>' +
      '</div>' +

      (attCount > 0 ? '<div class="att-hint">附件共 '+attCount+' 页，请翻页查看</div>' : '') +

      attPagesHtml +

      '<script>' +
      'window.onload=function(){' +
        'var imgs=document.querySelectorAll("img.att-img");' +
        'if(!imgs.length){window.print();return;}' +
        'var loaded=0,total=imgs.length;' +
        'function check(){loaded++;if(loaded>=total)window.print();}' +
        'for(var i=0;i<total;i++){' +
          'if(imgs[i].complete){check();}' +
          'else{imgs[i].onload=check;imgs[i].onerror=check;}' +
        '}' +
      '};' +
      '</script>' +
      '</body></html>';

    var win = window.open("","_blank");
    win.document.write(html);
    win.document.close();
  });
}

// ===== 现场作业记录 =====
var FO_STATUS_LABEL = {
  draft: "草稿", recording: "记录中", completed: "已完成", cancelled: "已作废"
};
var FO_OP_TYPE_LABEL = {
  box_op: "箱子操作", palletize: "打托", bulk_in_out: "整进整出", unload: "卸货", other: "其他"
};
var FO_NEXT_STATUS = {
  draft: ["recording","cancelled"],
  recording: ["completed","cancelled"],
  completed: [],
  cancelled: []
};
var FO_EDITABLE = { draft:1, recording:1 };
var FO_INCOMPLETE_STATUS = { draft:1, recording:1 };
var FO_STATUS_PRIORITY = { recording:0, draft:1, completed:2, cancelled:3 };

var _editingFoId = null;
var _foSourcePlanId = null;

function goNewFoFromPlan(plan_id, plan_day, customer_name, goods_summary, purpose_text){
  navPush();
  _editingFoId = null;
  _foSourcePlanId = plan_id;
  goView("fo_create");
  initFoForm(null, { plan_id:plan_id, plan_day:plan_day, customer_name:customer_name, goods_summary:goods_summary, purpose_text:purpose_text });
}

function initFoForm(data, fromPlan){
  var isEdit = !!data;
  document.getElementById("fo-title").textContent = isEdit ? "编辑现场作业记录" : "新建现场作业记录";
  document.getElementById("fo-id-bar").style.display = isEdit ? "" : "none";
  document.getElementById("fo-id-bar").textContent = isEdit ? ("编号: " + data.record_id + "（不可修改）") : "";
  document.getElementById("fo-submit-btn").textContent = isEdit ? "保存修改" : "保存";
  document.getElementById("fo-creator-group").style.display = isEdit ? "none" : "";

  // source plan bar
  var srcBar = document.getElementById("fo-src-bar");
  if(isEdit && data.source_plan_id){
    srcBar.style.display = "";
    srcBar.textContent = "来源入库计划: " + data.source_plan_id;
  } else if(!isEdit && fromPlan && fromPlan.plan_id){
    srcBar.style.display = "";
    srcBar.textContent = "来源入库计划: " + fromPlan.plan_id;
  } else {
    srcBar.style.display = "none";
  }

  if(isEdit){
    _foSourcePlanId = data.source_plan_id || null;
    document.getElementById("fo-day").value = data.plan_day;
    document.getElementById("fo-optype").value = data.operation_type;
    document.getElementById("fo-customer").value = data.customer_name;
    document.getElementById("fo-summary").value = data.goods_summary || "";
    document.getElementById("fo-input-box").value = data.input_box_count || 0;
    document.getElementById("fo-output-box").value = data.output_box_count || 0;
    document.getElementById("fo-output-pallet").value = data.output_pallet_count || 0;
    document.getElementById("fo-instr").value = data.instruction_text || "";
    document.getElementById("fo-creator").value = "";
  } else if(fromPlan){
    _foSourcePlanId = fromPlan.plan_id || null;
    document.getElementById("fo-day").value = fromPlan.plan_day || kstToday();
    document.getElementById("fo-optype").value = "other";
    document.getElementById("fo-customer").value = fromPlan.customer_name || "";
    document.getElementById("fo-summary").value = fromPlan.goods_summary || "";
    document.getElementById("fo-input-box").value = 0;
    document.getElementById("fo-output-box").value = 0;
    document.getElementById("fo-output-pallet").value = 0;
    document.getElementById("fo-instr").value = fromPlan.purpose_text || "";
    document.getElementById("fo-creator").value = "";
  } else {
    _foSourcePlanId = null;
    document.getElementById("fo-day").value = kstToday();
    document.getElementById("fo-optype").value = "other";
    document.getElementById("fo-customer").value = "";
    document.getElementById("fo-summary").value = "";
    document.getElementById("fo-input-box").value = 0;
    document.getElementById("fo-output-box").value = 0;
    document.getElementById("fo-output-pallet").value = 0;
    document.getElementById("fo-instr").value = "";
    document.getElementById("fo-creator").value = "";
  }
  document.getElementById("fo-result").textContent = "";
}

function submitFo(){
  var day = document.getElementById("fo-day").value;
  var optype = document.getElementById("fo-optype").value;
  var customer = document.getElementById("fo-customer").value.trim();
  var summary = document.getElementById("fo-summary").value.trim();
  var inputBox = Number(document.getElementById("fo-input-box").value) || 0;
  var outputBox = Number(document.getElementById("fo-output-box").value) || 0;
  var outputPallet = Number(document.getElementById("fo-output-pallet").value) || 0;
  var instr = document.getElementById("fo-instr").value.trim();
  var creator = document.getElementById("fo-creator").value.trim();

  if(!day){ alert("请选择作业日期"); return; }
  if(!customer){ alert("请输入客户名"); return; }
  if(!_editingFoId && !creator){ alert("请输入创建人"); return; }

  var foBtn = document.getElementById("fo-submit-btn");
  if(_submitting) return;
  _submitting = true;
  var foOrigText = foBtn.textContent;
  foBtn.disabled = true;

  if(_editingFoId){
    // 编辑模式 — 全量提交
    foBtn.textContent = "保存中...";
    fetchApi({
      action:"b2b_field_op_update", record_id:_editingFoId, sub:"edit",
      plan_day:day, customer_name:customer, goods_summary:summary,
      operation_type:optype, input_box_count:inputBox, output_box_count:outputBox,
      output_pallet_count:outputPallet, instruction_text:instr
    }).then(function(res){
      _submitting = false; foBtn.disabled = false; foBtn.textContent = foOrigText;
      if(res && res.ok){
        alert("修改成功！");
        navDropTopIf("fo_detail");
        goFoDetail(_editingFoId, true);
      } else {
        document.getElementById("fo-result").innerHTML = '<span class="bad">修改失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  } else {
    // 新建模式
    foBtn.textContent = "创建中...";
    var params = {
      action:"b2b_field_op_create", plan_day:day, customer_name:customer,
      goods_summary:summary, operation_type:optype,
      input_box_count:inputBox, output_box_count:outputBox,
      output_pallet_count:outputPallet, instruction_text:instr, created_by:creator,
      request_id:getOrCreateRid("fo")
    };
    if(_foSourcePlanId) params.source_plan_id = _foSourcePlanId;
    fetchApi(params).then(function(res){
      _submitting = false; foBtn.disabled = false; foBtn.textContent = foOrigText;
      if(res && res.ok){
        clearRid("fo");
        alert("创建成功！编号: " + res.record_id);
        goFoDetail(res.record_id, true);
      } else {
        document.getElementById("fo-result").innerHTML = '<span class="bad">创建失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  }
}

function goEditFo(record_id){
  var _navSnap = navCaptureState();
  fetchApi({ action:"b2b_field_op_detail", record_id:record_id }).then(function(res){
    if(!res || !res.ok){ alert("加载失败"); return; }
    var r = res.record;
    if(!FO_EDITABLE[r.status]){ alert("当前状态不允许编辑"); return; }
    _navStack.push(_navSnap);
    _editingFoId = record_id;
    goView("fo_create");
    initFoForm(r, null);
  });
}

// ===== 现场作业记录列表 =====
function initFoList(){
  var today = kstToday();
  document.getElementById("fl-start").value = today;
  document.getElementById("fl-end").value = today;
  loadFoList();
}

function loadFoList(){
  var s = document.getElementById("fl-start").value;
  var e = document.getElementById("fl-end").value;
  if(!s || !e){ alert("请选择日期"); return; }
  var el = document.getElementById("fl-result");
  el.innerHTML = '<div class="q-empty">加载中...</div>';
  fetchApi({ action:"b2b_field_op_list", start_day:s, end_day:e }).then(function(res){
    if(!res || !res.ok){ el.innerHTML = '<div class="bad">查询失败</div>'; return; }
    renderFoList(el, res.records||[]);
  });
}

function renderFoList(container, records){
  if(!records.length){
    container.innerHTML = '<div class="q-empty">暂无记录</div>';
    return;
  }

  // 分区：未完成在前
  var incomplete = records.filter(function(r){ return FO_INCOMPLETE_STATUS[r.status]; });
  var done = records.filter(function(r){ return !FO_INCOMPLETE_STATUS[r.status]; });

  incomplete.sort(function(a,b){ return (FO_STATUS_PRIORITY[a.status]||9) - (FO_STATUS_PRIORITY[b.status]||9); });
  done.sort(function(a,b){ return (FO_STATUS_PRIORITY[a.status]||9) - (FO_STATUS_PRIORITY[b.status]||9); });

  var html = '';
  if(incomplete.length > 0){
    html += '<div class="list-section-title">进行中（'+incomplete.length+' 条）</div>';
    html += incomplete.map(renderFoRow).join("");
  }
  if(done.length > 0){
    if(incomplete.length > 0) html += '<div class="list-section-title" style="margin-top:12px;">已结束（'+done.length+' 条）</div>';
    html += done.map(renderFoRow).join("");
  }
  container.innerHTML = html;
}

function renderFoRow(r){
  var dimClass = (r.status==="cancelled") ? " row-dim" : "";
  var boundTag = r.bound_workorder_id ? ' <span class="bound-badge">已绑定 '+esc(r.bound_workorder_id)+'</span>' : '';
  var srcTag = r.source_plan_id ? ' <span class="muted" style="font-size:11px;">← '+esc(r.source_plan_id)+'</span>' : '';
  return '<div class="wo-row'+dimClass+'" onclick="goFoDetail(\''+esc(r.record_id)+'\')">' +
    '<div><span class="st st-'+esc(r.status)+'">'+esc(FO_STATUS_LABEL[r.status]||r.status)+'</span>'+boundTag+' ' +
    '<b>'+esc(r.record_id)+'</b> · '+esc(r.customer_name)+srcTag+'</div>' +
    '<div class="meta">'+esc(r.plan_day)+' · '+esc(FO_OP_TYPE_LABEL[r.operation_type]||r.operation_type) +
    ' · 入'+r.input_box_count+'箱 → 出'+r.output_box_count+'箱 / '+r.output_pallet_count+'托</div>' +
  '</div>';
}

// ===== 现场作业记录详情 =====
function goFoDetail(id, _skipNav){
  if(!_skipNav) navPush();
  _currentDetailId = id;
  goView("fo_detail");
  var card = document.getElementById("fo-detail-card");
  card.innerHTML = '<div class="muted">加载中...</div>';
  fetchApi({ action:"b2b_field_op_detail", record_id:id }).then(function(res){
    if(!res || !res.ok){ card.innerHTML = '<div class="bad">加载失败: '+esc(res&&res.error||"")+'</div>'; return; }
    var r = res.record;

    // 状态按钮
    var statusBtns = (FO_NEXT_STATUS[r.status]||[]).map(function(s){
      return '<button onclick="changeFoStatus(\''+esc(r.record_id)+'\',\''+s+'\')" class="'+(s==="cancelled"?"bad":"primary")+'" style="width:auto;padding:8px 16px;font-size:13px;">'+esc(FO_STATUS_LABEL[s]||s)+'</button>';
    }).join(" ");

    // 编辑按钮
    var editBtn = FO_EDITABLE[r.status] ?
      ' <button onclick="goEditFo(\''+esc(r.record_id)+'\')" style="width:auto;padding:8px 16px;font-size:13px;">编辑</button>' : '';

    // 绑定按钮：仅 completed 且未绑定
    var bindBtn = (r.status === "completed" && !r.bound_workorder_id) ?
      ' <button onclick="showBindWo(\''+esc(r.record_id)+'\')" style="width:auto;padding:8px 16px;font-size:13px;background:#8e24aa;color:#fff;">绑定作业单</button>' : '';

    // 绑定信息
    var boundInfo = r.bound_workorder_id ?
      '<div class="detail-field" style="background:#f3e5f5;padding:8px 12px;border-radius:8px;margin:8px 0;">' +
      '<b>已绑定作业单:</b> <span style="color:#8e24aa;font-weight:700;">'+esc(r.bound_workorder_id)+'</span>' +
      (r.bound_at ? ' · 绑定时间: '+new Date(r.bound_at).toLocaleString() : '') +
      ' <button onclick="goWoDetail(\''+esc(r.bound_workorder_id)+'\')" style="width:auto;padding:4px 12px;font-size:12px;margin-left:8px;">查看作业单</button></div>' : '';

    card.innerHTML =
      '<div style="font-size:18px;font-weight:800;margin-bottom:10px;">' +
        esc(r.record_id) + ' <span class="st st-'+esc(r.status)+'">'+esc(FO_STATUS_LABEL[r.status]||r.status)+'</span>' +
        (r.bound_workorder_id ? ' <span class="bound-badge">已绑定</span>' : '') +
      '</div>' +
      (r.source_plan_id ? '<div class="detail-field" style="color:#2e7d32;"><b>来源入库计划:</b> '+esc(r.source_plan_id)+'</div>' : '<div class="detail-field muted"><b>来源:</b> 独立新建</div>') +
      '<div class="detail-field"><b>客户:</b> '+esc(r.customer_name)+'</div>' +
      '<div class="detail-field"><b>作业日期:</b> '+esc(r.plan_day)+'</div>' +
      '<div class="detail-field"><b>操作类型:</b> '+esc(FO_OP_TYPE_LABEL[r.operation_type]||r.operation_type)+'</div>' +
      '<div class="detail-field"><b>货物摘要:</b> '+esc(r.goods_summary||"(无)")+'</div>' +
      '<div class="detail-field"><b>输入箱数:</b> '+r.input_box_count+'</div>' +
      '<div class="detail-field"><b>产出箱数:</b> '+r.output_box_count+'</div>' +
      '<div class="detail-field"><b>产出托盘数:</b> '+r.output_pallet_count+'</div>' +
      (r.instruction_text ? '<div class="detail-field"><b>作业说明:</b> '+esc(r.instruction_text)+'</div>' : '') +
      '<div class="detail-field muted" style="font-size:12px;"><b>创建人:</b> '+esc(r.created_by)+' · 创建时间: '+new Date(r.created_at).toLocaleString() +
      (r.completed_at ? ' · 完成时间: '+new Date(r.completed_at).toLocaleString() : '') + '</div>' +
      boundInfo +
      '<div style="margin:12px 0;">' + statusBtns + editBtn + bindBtn + '</div>' +
      '<div id="fo-bind-area"></div>';
  });
}

function changeFoStatus(id, status){
  var label = FO_STATUS_LABEL[status] || status;
  if(status === "cancelled"){
    if(!confirm("确认作废记录 "+id+"？")) return;
  } else {
    if(!confirm("确认将 "+id+" 状态改为「"+label+"」？")) return;
  }
  fetchApi({ action:"b2b_field_op_update", record_id:id, sub:"status", status:status }).then(function(res){
    if(res && res.ok){
      goFoDetail(id, true);
    } else {
      alert("状态更新失败: "+(res&&res.error||"unknown"));
    }
  });
}

// ===== 绑定作业单 =====
function showBindWo(record_id){
  var area = document.getElementById("fo-bind-area");
  area.innerHTML = '<div class="muted">加载作业单列表...</div>';

  // 加载最近 30 天的作业单供选择
  var today = kstToday();
  var d30 = new Date(Date.now() + 9*3600*1000 - 30*24*3600*1000);
  var start30 = d30.getUTCFullYear() + "-" + pad2(d30.getUTCMonth()+1) + "-" + pad2(d30.getUTCDate());

  fetchApi({ action:"b2b_wo_list", start_day:start30, end_day:today }).then(function(res){
    if(!res || !res.ok){ area.innerHTML = '<div class="bad">加载失败</div>'; return; }
    var wos = (res.workorders||[]).filter(function(w){ return w.status !== "cancelled"; });
    if(!wos.length){
      area.innerHTML = '<div class="muted">最近30天没有可绑定的作业单</div>';
      return;
    }

    var html = '<div style="border:1px solid #ce93d8;border-radius:10px;padding:12px;margin-top:8px;background:#fce4ec;">';
    html += '<div style="font-size:14px;font-weight:700;margin-bottom:8px;">选择要绑定的正式作业单</div>';
    html += '<select id="fo-bind-select" style="width:100%;margin-bottom:8px;">';
    wos.forEach(function(w){
      html += '<option value="'+esc(w.workorder_id)+'">'+esc(w.workorder_id)+' · '+esc(w.customer_name)+' · '+esc(w.plan_day)+' · '+esc(WO_STATUS_LABEL[w.status]||w.status)+'</option>';
    });
    html += '</select>';
    html += '<button class="primary" style="width:auto;padding:8px 16px;font-size:13px;" onclick="doBindWo(\''+esc(record_id)+'\')">确认绑定</button>';
    html += ' <button style="width:auto;padding:8px 16px;font-size:13px;" onclick="document.getElementById(\'fo-bind-area\').innerHTML=\'\'">取消</button>';
    html += '</div>';
    area.innerHTML = html;
  });
}

function doBindWo(record_id){
  var sel = document.getElementById("fo-bind-select");
  if(!sel) return;
  var workorder_id = sel.value;
  if(!workorder_id){ alert("请选择作业单"); return; }
  if(!confirm("确认将 "+record_id+" 绑定到 "+workorder_id+"？\n绑定后不可更改。")) return;

  fetchApi({ action:"b2b_field_op_update", record_id:record_id, sub:"bind", workorder_id:workorder_id }).then(function(res){
    if(res && res.ok){
      alert("绑定成功！");
      goFoDetail(record_id, true);
    } else {
      alert("绑定失败: "+(res&&res.error||"unknown"));
    }
  });
}

// ===== 出库扫码核对 =====
var SC_STATUS_LABEL = { open:"进行中", closed:"已关闭", cancelled:"已作废" };
var _scImportedItems = []; // 导入预览数据

function initScCreate(){
  document.getElementById("sc-day").value = kstToday();
  document.getElementById("sc-name").value = "";
  document.getElementById("sc-creator").value = "";
  document.getElementById("sc-import-err").innerHTML = "";
  document.getElementById("sc-preview").style.display = "none";
  document.getElementById("sc-submit-btn").style.display = "none";
  document.getElementById("sc-result").textContent = "";
  _scImportedItems = [];
}

var SC_TPL_HEADERS = ["出库条码","计划箱数","客户名","货物摘要"];

function scDownloadTemplate(){
  var ws = XLSX.utils.aoa_to_sheet([SC_TPL_HEADERS]);
  ws["!cols"] = SC_TPL_HEADERS.map(function(){ return { wch: 16 }; });
  var wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "核对计划");
  XLSX.writeFile(wb, "出库核对模板.xlsx");
}

function scImportExcel(input){
  var errEl = document.getElementById("sc-import-err");
  errEl.innerHTML = "";
  document.getElementById("sc-preview").style.display = "none";
  document.getElementById("sc-submit-btn").style.display = "none";
  _scImportedItems = [];
  if(!input.files || !input.files[0]) return;
  var file = input.files[0];
  input.value = "";

  var reader = new FileReader();
  reader.onload = function(e){
    try {
      var data = new Uint8Array(e.target.result);
      var wb = XLSX.read(data, { type:"array" });
      var ws = wb.Sheets[wb.SheetNames[0]];
      var rows = XLSX.utils.sheet_to_json(ws, { header:1, defval:"" });
    } catch(ex){
      errEl.innerHTML = '<span class="bad">文件解析失败: '+esc(String(ex))+'</span>';
      return;
    }

    if(!rows || rows.length < 2){
      errEl.innerHTML = '<span class="bad">文件为空或只有表头，无数据行</span>';
      return;
    }

    // 解析表头
    var headerRow = rows[0];
    var colMap = {};
    var FIELD_MAP = {"出库条码":"barcode","计划箱数":"count","客户名":"customer","货物摘要":"summary"};
    for(var c=0; c<headerRow.length; c++){
      var h = String(headerRow[c]).trim();
      if(FIELD_MAP[h]) colMap[FIELD_MAP[h]] = c;
    }
    if(colMap.barcode === undefined){ errEl.innerHTML = '<span class="bad">缺少"出库条码"列</span>'; return; }
    if(colMap.count === undefined){ errEl.innerHTML = '<span class="bad">缺少"计划箱数"列</span>'; return; }

    // 解析数据行 + 校验
    var items = [];
    var errors = [];
    var seen = {}; // barcode -> [行号]
    for(var r=1; r<rows.length; r++){
      var row = rows[r];
      var bc = String(row[colMap.barcode]||"").trim();
      var cnt = String(row[colMap.count]||"").trim();
      var cust = colMap.customer !== undefined ? String(row[colMap.customer]||"").trim() : "";
      var summ = colMap.summary !== undefined ? String(row[colMap.summary]||"").trim() : "";

      // 跳过全空行
      if(!bc && !cnt) continue;

      var rowNum = r + 1;
      if(!bc){ errors.push("第"+rowNum+"行: 出库条码为空"); continue; }

      var cntNum = parseInt(cnt, 10);
      if(!cntNum || cntNum <= 0 || String(cntNum) !== cnt){
        errors.push("第"+rowNum+"行: 计划箱数必须为正整数，当前值: "+cnt);
        continue;
      }

      // 重复检查
      if(!seen[bc]) seen[bc] = [];
      seen[bc].push(rowNum);

      items.push({ outbound_barcode:bc, expected_box_count:cntNum, customer_name:cust, goods_summary:summ, _row:rowNum });
    }

    // 汇总重复条码
    var dupErrors = [];
    for(var k in seen){
      if(seen[k].length > 1){
        dupErrors.push("条码 \""+k+"\" 重复出现在第 "+seen[k].join("、")+" 行");
      }
    }

    if(dupErrors.length > 0){
      errors = dupErrors.concat(errors);
    }

    if(errors.length > 0){
      errEl.innerHTML = '<span class="bad">导入校验失败（整单拒绝）：<br>' + errors.map(esc).join("<br>") + '</span>';
      return;
    }

    if(items.length === 0){
      errEl.innerHTML = '<span class="bad">没有有效数据行</span>';
      return;
    }

    // 预览
    _scImportedItems = items;
    var totalBoxes = 0;
    var tbody = document.getElementById("sc-preview-body");
    tbody.innerHTML = items.map(function(it, i){
      totalBoxes += it.expected_box_count;
      return '<tr><td>'+(i+1)+'</td><td>'+esc(it.outbound_barcode)+'</td><td>'+it.expected_box_count+'</td><td>'+esc(it.customer_name)+'</td><td>'+esc(it.goods_summary)+'</td></tr>';
    }).join("");
    document.getElementById("sc-preview-summary").textContent = "合计: "+items.length+" 种条码, "+totalBoxes+" 箱";
    document.getElementById("sc-preview").style.display = "";
    document.getElementById("sc-submit-btn").style.display = "";
    errEl.innerHTML = '<span style="color:#2e7d32;">导入成功，请确认预览后点击"确认创建批次"</span>';
  };
  reader.readAsArrayBuffer(file);
}

function submitSc(){
  if(_submitting) return;
  var day = document.getElementById("sc-day").value;
  var name = document.getElementById("sc-name").value.trim();
  var creator = document.getElementById("sc-creator").value.trim();

  if(!day){ alert("请选择核对日期"); return; }
  if(!name){ alert("请输入批次名称"); return; }
  if(!creator){ alert("请输入创建人"); return; }
  if(!_scImportedItems.length){ alert("请先导入 Excel"); return; }

  _submitting = true;
  // 清理 _row 字段
  var items = _scImportedItems.map(function(it){
    return { outbound_barcode:it.outbound_barcode, expected_box_count:it.expected_box_count, customer_name:it.customer_name, goods_summary:it.goods_summary };
  });

  document.getElementById("sc-submit-btn").disabled = true;
  document.getElementById("sc-submit-btn").textContent = "创建中...";

  fetchApi({
    action:"b2b_scan_batch_create",
    check_day:day, batch_name:name, created_by:creator,
    items:JSON.stringify(items),
    request_id:getOrCreateRid("sc")
  }).then(function(res){
    _submitting = false;
    document.getElementById("sc-submit-btn").disabled = false;
    document.getElementById("sc-submit-btn").textContent = "确认创建批次";
    if(res && res.ok){
      clearRid("sc");
      alert("创建成功！批次编号: " + res.batch_id + "\n条码: " + res.total_barcodes + " 种\n总箱数: " + res.total_expected_boxes);
      goScDetail(res.batch_id, true);
    } else {
      document.getElementById("sc-result").innerHTML = '<span class="bad">创建失败: '+esc(res&&res.error||"unknown")+'</span>';
    }
  });
}

// ===== 核对批次列表 =====
function initScList(){
  var today = kstToday();
  document.getElementById("scl-start").value = today;
  document.getElementById("scl-end").value = today;
  loadScList();
}

function loadScList(){
  var s = document.getElementById("scl-start").value;
  var e = document.getElementById("scl-end").value;
  if(!s || !e){ alert("请选择日期"); return; }
  var el = document.getElementById("scl-result");
  el.innerHTML = '<div class="q-empty">加载中...</div>';
  fetchApi({ action:"b2b_scan_batch_list", start_day:s, end_day:e }).then(function(res){
    if(!res || !res.ok){ el.innerHTML = '<div class="bad">查询失败</div>'; return; }
    var batches = res.batches || [];
    if(!batches.length){ el.innerHTML = '<div class="q-empty">暂无核对批次</div>'; return; }
    el.innerHTML = batches.map(function(b){
      var dimClass = (b.status==="cancelled") ? " row-dim" : "";
      return '<div class="wo-row'+dimClass+'" onclick="goScDetail(\''+esc(b.batch_id)+'\')">' +
        '<div><span class="st st-'+esc(b.status)+'">'+esc(SC_STATUS_LABEL[b.status]||b.status)+'</span> ' +
        '<b>'+esc(b.batch_id)+'</b> · '+esc(b.batch_name)+'</div>' +
        '<div class="meta">'+esc(b.check_day)+' · '+b.total_barcodes+'种条码 · '+b.total_expected_boxes+'箱 · 创建人:'+esc(b.created_by)+'</div>' +
      '</div>';
    }).join("");
  });
}

// ===== 核对批次详情 =====
function goScDetail(batch_id, _skipNav){
  if(!_skipNav) navPush();
  _currentDetailId = batch_id;
  goView("sc_detail");
  var card = document.getElementById("sc-detail-card");
  card.innerHTML = '<div class="muted">加载中...</div>';
  fetchApi({ action:"b2b_scan_batch_detail", batch_id:batch_id }).then(function(res){
    if(!res || !res.ok){ card.innerHTML = '<div class="bad">加载失败: '+esc(res&&res.error||"")+'</div>'; return; }
    var b = res.batch;
    var items = res.items || [];
    var unplanned = res.unplanned || [];
    var doneBoxes = res.done_boxes;
    var totalBoxes = res.total_expected_boxes;
    var pct = res.progress_percent;

    // 分类
    var missing = [], done = [], over = [];
    items.forEach(function(it){
      if(it.scanned_count < it.expected_box_count) missing.push(it);
      else if(it.scanned_count === it.expected_box_count) done.push(it);
      else over.push(it);
    });
    // 未出库按差额降序
    missing.sort(function(a,b2){ return (b2.expected_box_count - b2.scanned_count) - (a.expected_box_count - a.scanned_count); });

    // 操作按钮
    var closeBtn = (b.status === "open") ?
      '<button onclick="closeScanBatch(\''+esc(b.batch_id)+'\')" style="width:auto;padding:8px 16px;font-size:13px;background:#e65100;color:#fff;border-color:#e65100;">关闭批次</button>' : '';
    var refreshBtn = '<button onclick="goScDetail(\''+esc(b.batch_id)+'\',true)" style="width:auto;padding:8px 16px;font-size:13px;">刷新</button>';

    var html = '';

    // 基本信息
    html += '<div style="font-size:18px;font-weight:800;margin-bottom:6px;">' +
      esc(b.batch_id) + ' <span class="st st-'+esc(b.status)+'">'+esc(SC_STATUS_LABEL[b.status]||b.status)+'</span></div>';
    html += '<div class="detail-field"><b>批次名称:</b> '+esc(b.batch_name)+'</div>';
    html += '<div class="detail-field"><b>核对日期:</b> '+esc(b.check_day)+'</div>';
    html += '<div class="detail-field"><b>条码种类:</b> '+b.total_barcodes+' 种</div>';
    html += '<div class="detail-field muted" style="font-size:12px;"><b>创建人:</b> '+esc(b.created_by)+' · 创建时间: '+new Date(b.created_at).toLocaleString() +
      (b.closed_at ? ' · 关闭时间: '+new Date(b.closed_at).toLocaleString() : '') + '</div>';

    // 进度
    html += '<div class="sc-progress">' +
      '进度: '+doneBoxes+' / '+totalBoxes+' 箱 ('+pct+'%)' +
      '<div class="sc-progress-bar"><div class="sc-progress-fill" style="width:'+pct+'%;"></div></div></div>';

    // 操作按钮
    html += '<div style="margin:10px 0;">' + refreshBtn + ' ' + closeBtn + '</div>';

    // 未完成清单
    html += '<div class="sc-section"><div class="sc-section-title sc-missing">⚠ 未完成（'+missing.length+' 种）</div>';
    if(missing.length){
      html += missing.map(function(it){
        var diff = it.expected_box_count - it.scanned_count;
        return '<div class="sc-item"><b>'+esc(it.outbound_barcode)+'</b>' +
          (it.customer_name ? ' · '+esc(it.customer_name) : '') +
          (it.goods_summary ? ' · '+esc(it.goods_summary) : '') +
          ' — 计划'+it.expected_box_count+'箱 已扫'+it.scanned_count+'箱 <b class="sc-missing">差'+diff+'箱</b></div>';
      }).join("");
    } else {
      html += '<div class="q-empty">无（全部已扫完）</div>';
    }
    html += '</div>';

    // 已完成清单
    html += '<div class="sc-section"><div class="sc-section-title sc-done">✓ 已完成（'+done.length+' 种）</div>';
    if(done.length){
      html += done.map(function(it){
        return '<div class="sc-item"><b>'+esc(it.outbound_barcode)+'</b>' +
          (it.customer_name ? ' · '+esc(it.customer_name) : '') +
          ' — '+it.expected_box_count+'箱 ✓</div>';
      }).join("");
    } else {
      html += '<div class="q-empty">无</div>';
    }
    html += '</div>';

    // 多扫清单
    var overPallets = res.over_pallets || {};
    html += '<div class="sc-section"><div class="sc-section-title sc-over">⚠ 多扫（'+over.length+' 种）</div>';
    if(over.length){
      html += over.map(function(it){
        var extra = it.scanned_count - it.expected_box_count;
        var palletTag = overPallets[it.outbound_barcode] ? ' <span class="pallet-tag">托盘: '+esc(overPallets[it.outbound_barcode])+'</span>' : '';
        return '<div class="sc-item"><b>'+esc(it.outbound_barcode)+'</b>' +
          (it.customer_name ? ' · '+esc(it.customer_name) : '') +
          ' — 计划'+it.expected_box_count+'箱 已扫'+it.scanned_count+'箱 <b class="sc-over">多扫'+extra+'箱</b>' + palletTag + '</div>';
      }).join("");
    } else {
      html += '<div class="q-empty">无</div>';
    }
    html += '</div>';

    // 计划外条码
    html += '<div class="sc-section"><div class="sc-section-title sc-unplanned">● 计划外条码（'+unplanned.length+' 种）</div>';
    if(unplanned.length){
      html += unplanned.map(function(u){
        var palletTag = u.pallets ? ' <span class="pallet-tag">托盘: '+esc(u.pallets)+'</span>' : '';
        return '<div class="sc-item"><b class="sc-unplanned">'+esc(u.outbound_barcode)+'</b> — 扫了'+u.scan_times+'次（不在计划内）' + palletTag + '</div>';
      }).join("");
    } else {
      html += '<div class="q-empty">无</div>';
    }
    html += '</div>';

    card.innerHTML = html;
  });
}

function closeScanBatch(batch_id){
  // 第一步：拿统计
  fetchApi({ action:"b2b_scan_batch_close", batch_id:batch_id }).then(function(res){
    if(!res || !res.ok){
      alert("关闭失败: "+(res&&res.error||"unknown"));
      return;
    }
    if(res.action === "confirm_needed"){
      var msg = "确认关闭批次 "+batch_id+"？\n\n当前状态：";
      if(res.missing_count > 0) msg += "\n· "+res.missing_count+"种条码未扫完（差"+res.missing_boxes+"箱）";
      if(res.over_count > 0) msg += "\n· "+res.over_count+"种条码多扫";
      if(res.unplanned_count > 0) msg += "\n· "+res.unplanned_count+"种计划外条码";
      if(res.missing_count===0 && res.over_count===0 && res.unplanned_count===0) msg += "\n全部正常完成。";
      msg += "\n\n关闭后不可继续扫码。";
      if(!confirm(msg)) return;
      // 第二步：确认关闭
      fetchApi({ action:"b2b_scan_batch_close", batch_id:batch_id, confirm:"true" }).then(function(res2){
        if(res2 && res2.ok){
          alert("批次已关闭！");
          goScDetail(batch_id, true);
        } else {
          alert("关闭失败: "+(res2&&res2.error||"unknown"));
        }
      });
    }
  });
}

// ===== 初始化 =====
(function(){
  var today = kstToday();
  document.getElementById("todayPill").textContent = today;

  // 填充 wave task 下拉
  var taskSel = document.getElementById("hw-task");
  if(taskSel){
    ["B2C拣货","B2C理货","B2C批量出库","B2B入库理货","B2B工单操作","B2B现场记录"].forEach(function(t){
      var o = document.createElement("option"); o.value = t; o.textContent = t; taskSel.appendChild(o);
    });
  }

  if(getKey()){
    fetchApi({ action:"b2b_plan_list", start_day:today, end_day:today }).then(function(res){
      if(res && res.ok) showMain();
    });
  }
})();
