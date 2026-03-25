// app.js - CK Warehouse FE (single file) 
// ✅ Multi-device session: scan to join session (CKSESSION|PS-...)
// ✅ Cross-device badge lock: join/leave is SYNC (server confirms lock)
// ✅ Fast UX: start/wave/end are queued (async)
// ✅ Global session close: backend session_close is session-only (global), so end happens ONCE
// ✅ One end record: write Events row with task="SESSION" only once
// ✅ PACK-like tasks auto create session + auto end after last leave: PACK/退件入库/质检/废弃处理
// ✅ NEW: 批量出库（流程同入库理货）：start + 扫出库单号(去重计数) + join/leave + end

var LOCK_URL = "https://ck-warehouse-api.ck91888.workers.dev";

/** ===== Router ===== */
var pages = [
  "home","badge","global_menu","b2c_menu",
  "import_menu","import_unload","import_scan_pallet","import_loadout","import_pickup","import_problem",
  "b2b_menu","b2b_unload","b2b_tally","b2b_workorder","b2b_workorder_simple","b2b_outbound","b2b_inventory","b2b_field_op","b2b_scan_check",
  "b2c_tally","b2c_pick","b2c_pack","b2c_bulkout","b2c_return","b2c_qc","b2c_inventory","b2c_disposal","b2c_relabel",
  "warehouse_cleanup",
  "active_now",
  "report",
  "global_sessions",
  "correction",
  "wms_import",
  "daily_features"
];



/** ===== Admin (Hidden) =====
 * - 7 clicks on title => unlock
 * - report page/button hidden unless unlocked
 * - report data fetch requires admin key (sent as k)
 */
var ADMIN_DENY_ONCE = {};
function adminKey_(){ try{ return String(sessionStorage.getItem("admin_k")||""); }catch(e){ return ""; } }
function adminIsUnlocked_(){
  try{ return sessionStorage.getItem("admin_unlocked")==="1" && !!adminKey_(); }catch(e){ return false; }
}
function adminSet_(key){
  try{
    sessionStorage.setItem("admin_k", String(key||"").trim());
    sessionStorage.setItem("admin_unlocked","1");
  }catch(e){}
  adminApplyUI_();
}
function adminClear_(){
  try{
    sessionStorage.removeItem("admin_k");
    sessionStorage.removeItem("admin_unlocked");
  }catch(e){}
  adminApplyUI_();
}
async function adminVerifyKey_(key){
  key = String(key||"").trim();
  if(!key) return false;
  try{
    var res = await fetchApi({ action:"admin_events_tail", k:key, limit: 1 });
    return !!(res && res.ok===true);
  }catch(e){
    return false;
  }
}
async function adminUnlockFlow_(){
  var key = prompt("管理员口令（仅你自己知道）：") || "";
  key = String(key).trim();
  if(!key) return;
  setStatus("验证口令中... ⏳", true);
  var ok = await adminVerifyKey_(key);
  if(ok){
    adminSet_(key);
    setStatus("管理员模式已开启 ✅", true);
    alert("管理员模式已开启 ✅\n现在可以在【总控台】看到【劳效/工时汇总】按钮。");
  }else{
    adminClear_();
    setStatus("口令错误 ❌", false);
    alert("口令不正确（或后端未设置 ADMINKEY）。");
  }
}
function adminApplyUI_(){
  var show = adminIsUnlocked_() ? "block" : "none";
  var btn = document.getElementById("btnReport");
  if(btn) btn.style.display = show;
  var btnS = document.getElementById("btnSessions");
  if(btnS) btnS.style.display = show;
  var btnC = document.getElementById("btnCorrection");
  if(btnC) btnC.style.display = show;
  var btnW = document.getElementById("btnWmsImport");
  if(btnW) btnW.style.display = show;
  var btnDF = document.getElementById("btnDailyFeatures");
  if(btnDF) btnDF.style.display = show;
}
function bindAdminEasterEgg_(){
  var el = document.querySelector(".title");
  if(!el) return;

  var cnt = 0;
  var last = 0;

  el.style.cursor = "pointer";
  el.addEventListener("click", function(){
    var now = Date.now();
    if(now - last > 2000) cnt = 0; // 2秒内连点
    last = now;
    cnt++;
    if(cnt >= 7){
      cnt = 0;
      adminUnlockFlow_();
    }
  });
}

function setHash(page){ location.hash = "#/" + page; }
function getHashPage(){
  var h = (location.hash || "").trim();
  if(!h || h === "#") return "home";
  var m = h.match(/^#\/(.+)$/);
  if(!m) return "home";
  var p = m[1];
  // admin-only pages
  if((p==="report" || p==="global_sessions" || p==="correction" || p==="wms_import" || p==="daily_features") && !adminIsUnlocked_()){
    if(!ADMIN_DENY_ONCE[p]){ ADMIN_DENY_ONCE[p]=1; alert("管理员功能：请在标题处连续点击 7 次解锁"); }
    return "home";
  }
  return pages.indexOf(p) >= 0 ? p : "home";
}

function renderPages(){
  applyPageSession_();
  var cur = getHashPage();
  for(var i=0;i<pages.length;i++){
    var p = pages[i];
    var el = document.getElementById("page-"+p);
    if(el) el.style.display = (p===cur) ? "block" : "none";
  }

  if(cur==="home" || cur==="global_menu" || cur==="b2c_menu" || cur==="import_menu" || cur==="b2b_menu"){ fetchOperatorOpenSessions(); }

  if(cur==="badge"){ refreshUI(); refreshDaUI(); }

  if(cur==="b2c_tally"){ restoreState(); renderActiveLists(); renderInboundCountUI(); refreshUI(); }
  if(cur==="b2c_bulkout"){ restoreState(); renderActiveLists(); renderBulkOutUI(); refreshUI(); }
  if(cur==="b2c_pick"){ syncLeaderPickUI(); restoreState(); renderActiveLists(); renderWaveUI(); refreshUI(); }
  if(cur==="b2c_pack"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_return"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_qc"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_disposal"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_relabel"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="import_unload"){ restoreState(); renderActiveLists(); refreshUI(); updateReturnButton_(); }
  if(cur==="import_scan_pallet"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="import_loadout"){ restoreState(); renderActiveLists(); refreshUI(); updateReturnButton_(); }
  if(cur==="import_pickup"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="import_problem"){ restoreState(); renderActiveLists(); refreshUI(); }

  if(cur==="b2b_menu"){ refreshUI(); }
  if(cur==="b2b_unload"){ restoreState(); renderActiveLists(); refreshUI(); updateReturnButton_(); }
  if(cur==="b2b_tally"){ restoreState(); renderActiveLists(); renderB2bTallyUI(); refreshUI(); }
  if(cur==="b2b_workorder"){ _b2bBindingsLoaded = false; _b2bSelfHealPending = false; restoreState(); renderActiveLists(); renderB2bWorkorderUI(); loadB2bBindings(); loadB2bResults(); refreshUI(); if(!currentSessionId) tryRecoverB2bSession_(); }
  if(cur==="b2b_workorder_simple"){ _b2bBindingsLoaded = false; _b2bSelfHealPending = false; restoreState(); renderActiveLists(); refreshUI(); smInitPage_(); }
  if(cur==="b2b_outbound"){ restoreState(); renderActiveLists(); refreshUI(); updateReturnButton_(); }
  if(cur==="b2b_inventory"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2b_field_op"){ restoreState(); renderActiveLists(); renderB2bFieldOpUI(); refreshUI(); }
  if(cur==="b2b_scan_check"){ initScanCheckPage(); }
  if(cur==="b2c_inventory"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="warehouse_cleanup"){ restoreState(); renderActiveLists(); refreshUI(); }

  // 离开页面时清除定时器
  if(_activeNowTimer){ clearInterval(_activeNowTimer); _activeNowTimer = null; }
  if(relabelTimerHandle){ clearInterval(relabelTimerHandle); relabelTimerHandle = null; }
  if(cur==="active_now"){
    refreshActiveNow();
    _activeNowTimer = setInterval(refreshActiveNow, 30000);
  }
  if(cur==="global_sessions"){
    // 设置默认日期为今天(KST)
    var gsFrom = document.getElementById("gsDateFrom");
    var gsTo = document.getElementById("gsDateTo");
    if(gsFrom && !gsFrom.value){
      var kstNow = new Date(Date.now() + 9*3600*1000);
      var today = kstNow.getUTCFullYear() + "-" + pad2_(kstNow.getUTCMonth()+1) + "-" + pad2_(kstNow.getUTCDate());
      gsFrom.value = today;
      gsTo.value = today;
    }
    refreshGlobalSessions();
  }
  if(cur==="b2c_menu"){ refreshUI(); }
  if(cur==="import_menu"){ refreshUI(); }

  // 进入任务页时异步同步服务器在岗列表（解决换设备/刷新后本地列表为空的问题）
  var taskPages = ["b2c_tally","b2c_bulkout","b2c_pick","b2c_pack","b2c_return","b2c_qc","b2c_disposal","b2c_relabel",
    "import_unload","import_scan_pallet","import_loadout","import_pickup","import_problem",
    "b2b_unload","b2b_tally","b2b_workorder","b2b_workorder_simple","b2b_outbound",
    "b2b_inventory","b2b_field_op","b2c_inventory","warehouse_cleanup"];
  if(taskPages.indexOf(cur) >= 0 && currentSessionId){
    syncActiveFromServer_();
  }
}
window.addEventListener("hashchange", renderPages);

function go(p){ setHash(p); }
function back(){ if(history.length > 1) history.back(); else setHash("home"); }
if(!location.hash) setHash("home");

/** ===== Globals ===== */
var scanner = null;
var scanMode = null;
var _scanCameras = [];
var _scanCurrentCamId = null;
var _scanCurrentLabel = "";
var _scanOnScan = null;
var _scanQrboxFn = null;

var currentSessionId = null; // ✅ 每任务独立趟次：进入页面时按任务切换

// ===== Per-task session (A: every START creates a new session) =====
var CUR_CTX = { biz:null, task:null, page:null };
function ctxKey_(biz, task){
  return "session_id_" + encodeURIComponent(String(biz||"NA")) + "__" + encodeURIComponent(String(task||"NA"));
}
function getSess_(biz, task){
  return localStorage.getItem(ctxKey_(biz,task)) || null;
}
function setSess_(biz, task, sid){
  localStorage.setItem(ctxKey_(biz,task), String(sid||"").trim());
  // remember last ctx for menus
  localStorage.setItem("last_ctx_page", String(CUR_CTX.page||""));
  localStorage.setItem("last_ctx_biz", String(biz||""));
  localStorage.setItem("last_ctx_task", String(task||""));
}
function clearSess_(biz, task){
  localStorage.removeItem(ctxKey_(biz,task));
}
function getLastCtx_(){
  try{
    return {
      biz: localStorage.getItem("last_ctx_biz") || "",
      task: localStorage.getItem("last_ctx_task") || "",
      page: localStorage.getItem("last_ctx_page") || ""
    };
  }catch(e){ return {biz:"",task:"",page:""}; }
}

// page -> biz/task (must match your index.html page ids)
var PAGE_CTX = {
  // ===== B2C =====
  "b2c_tally":   { biz:"B2C", task:"理货" },
  "b2c_pick":    { biz:"B2C", task:"拣货" },
  "b2c_relabel": { biz:"B2C", task:"换单" },
  "b2c_bulkout": { biz:"B2C", task:"批量出库" },
  "b2c_pack":    { biz:"B2C", task:"打包" },
  "b2c_return":  { biz:"B2C", task:"退件入库" },
  "b2c_qc":      { biz:"B2C", task:"质检" },
  "b2c_disposal":{ biz:"B2C", task:"废弃处理" },
  "b2c_inventory":{ biz:"B2C", task:"B2C盘点" },

  // ===== 进口快件 =====
  "import_unload":      { biz:"进口", task:"卸货" },
  "import_scan_pallet": { biz:"进口", task:"过机扫描码托" },
  "import_loadout":     { biz:"进口", task:"装柜/出货" },
  "import_pickup":      { biz:"进口", task:"取/送货" },
  "import_problem":     { biz:"进口", task:"问题处理" },

  // ===== B2B =====
  "b2b_unload":    { biz:"B2B", task:"B2B卸货" },
  "b2b_tally":     { biz:"B2B", task:"B2B入库理货" },
  "b2b_workorder": { biz:"B2B", task:"B2B工单操作" },
  "b2b_outbound":  { biz:"B2B", task:"B2B出库" },
  "b2b_inventory": { biz:"B2B", task:"B2B盘点" },
  "b2b_field_op":  { biz:"B2B", task:"B2B现场记录" },

  // ===== 仓库整理 =====
  "warehouse_cleanup": { biz:"仓库", task:"仓库整理" }
};

// 双语任务名：biz/task -> 中文 / 한국어
var TASK_DISPLAY = {
  "B2C/理货":         "B2C 理货 / B2C 검수",
  "B2C/拣货":          "B2C 拣货 / B2C 피킹",
  "B2C/换单":       "B2C 换单 / B2C 재라벨",
  "B2C/批量出库":       "B2C 批量出库 / B2C 일괄출고",
  "B2C/打包":          "B2C 打包 / B2C 포장",
  "B2C/退件入库":       "B2C 退件入库 / B2C 반품입고",
  "B2C/质检":           "B2C 质检 / B2C 품검",
  "B2C/废弃处理":       "B2C 废弃处理 / B2C 폐기",
  "B2C/B2C盘点":        "B2C 盘点 / B2C 재고조사",
  "进口/卸货":        "进口 卸货 / 수입 하차",
  "进口/过机扫描码托": "进口 过机扫描 / 수입 기계스캔",
  "进口/装柜/出货":   "进口 装柜出货 / 수입 컨테이너적재",
  "进口/取/送货":     "进口 取/送货 / 수입 픽업·배송",
  "进口/问题处理":     "进口 问题处理 / 수입 문제처리",
  "B2B/B2B卸货":        "B2B 卸货 / B2B 하차",
  "B2B/B2B入库理货":    "B2B 理货 / B2B 입고정리",
  "B2B/B2B工单操作":    "B2B 工单 / B2B 작업지시",
  "B2B/B2B出库":        "B2B 出库 / B2B 출고",
  "B2B/B2B盘点":        "B2B 盘点 / B2B 재고조사",
  "B2B/B2B现场记录":    "B2B 现场记录 / B2B 현장작업",
  "仓库/仓库整理": "仓库整理 / 창고정리",
};
function taskDisplayLabel(biz, task){
  return TASK_DISPLAY[biz + "/" + task] || (biz + " / " + task);
}
function pageForTask(biz, task){
  for(var p in PAGE_CTX){
    if(PAGE_CTX[p].biz === biz && PAGE_CTX[p].task === task) return p;
  }
  return null;
}

async function fetchOperatorOpenSessions(){
  var op = getOperatorId();
  if(!op) return;
  try{
    var res = await jsonp(LOCK_URL, { action:"operator_open_sessions", operator_id: op });
    var panel = document.getElementById("openSessionsPanel");
    var list = document.getElementById("openSessionsList");
    if(!panel || !list) return;
    if(!res || res.ok !== true || !res.sessions || res.sessions.length === 0){
      panel.style.display = "none";
      return;
    }
    panel.style.display = "";
    list.innerHTML = res.sessions.map(function(s){
      var label = taskDisplayLabel(s.biz, s.task);
      var age = fmtDur(Date.now() - (s.created_ms || 0));
      var page = pageForTask(s.biz, s.task);
      var btn = page
        ? '<button class="small" style="width:auto;white-space:nowrap;"'+
            ' data-sid="'+esc(s.session)+'" data-biz="'+esc(s.biz)+'" data-task="'+esc(s.task)+'" data-page="'+esc(page)+'"'+
            ' onclick="restoreOpenSession(this.dataset.sid,this.dataset.biz,this.dataset.task,this.dataset.page)">进入 / 이동</button>'
        : '';
      return '<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #f0f0f0;">'+
        '<div style="flex:1;">'+
          '<div style="font-weight:700;font-size:13px;">'+esc(label)+'</div>'+
          '<div class="muted" style="font-size:11px;margin-top:2px;">'+esc(s.session)+' · '+esc(age)+'前开始 / '+esc(age)+' 전 시작</div>'+
        '</div>'+
        btn+
      '</div>';
    }).join("");
  }catch(e){ /* silent */ }
}

function restoreOpenSession(sid, biz, task, page){
  CUR_CTX = { biz: biz, task: task, page: page };
  currentSessionId = sid;
  setSess_(biz, task, sid);
  SESSION_INFO_CACHE = { sid: null, ts: 0, data: null };
  restoreState();
  renderActiveLists();
  refreshUI();
  fetchOperatorOpenSessions();
  go(page);
}

function applyPageSession_(){
  var cur = getHashPage();
  var ctx = PAGE_CTX[cur];
  if(ctx){
    CUR_CTX = { biz: ctx.biz, task: ctx.task, page: cur };
    currentSessionId = getSess_(ctx.biz, ctx.task);
  }else{
    CUR_CTX = { biz:null, task:null, page: cur };
    // menus: show last task session (read-only)
    var last = getLastCtx_();
    currentSessionId = (last.biz && last.task) ? getSess_(last.biz, last.task) : null;
  }
}

var scannedWaves = new Set();
var scannedInbounds = new Set();
var scannedBulkOutOrders = new Set();
var scannedB2bTallyOrders = new Set();
var scannedB2bWorkorders = new Set();
var b2bWorkorderBindings = {}; // { orderNo: { source_type, match_status, day_kst, internal_workorder_id, wo_summary } }
var _b2bBindingsLoaded = false; // true after first loadB2bBindings() success
var _b2bSelfHealPending = false; // guard: one self-heal reload at a time
var b2bWorkorderResults = {};  // { resultKey: { operation_mode, box_count, pallet_count, ... } }  key = day_kst||source_type||source_order_no
var _sessionOwnerInfo = null; // { created_by_operator, owner_operator_id, owner_changed_at, owner_changed_by }

// ===== B2B Simple Mode State =====
var _smCurrentOrder = null; // 当前工单号 (source_order_no)
var _smLabor = []; // labor details from server
var _smWorkorderStatuses = {}; // source_order_no → { workflow_status, temporary_completed_at, ... }
var _smIsSimpleMode = false; // 当前是否在简化模式
var _smBindingInFlight = false; // 绑定请求进行中，拦截依赖工单的操作

// ===== B2B Simple Mode i18n Dictionary =====
var SM_T = {
  title:          ["B2B 工单操作（简化模式）","B2B 작업지시 (간편 모드)"],
  scan_wo:        ["扫工单","작업지시 스캔"],
  scan_worker:    ["扫人员","작업자 스캔"],
  temp_complete:  ["暂时完成","임시 완료"],
  record_result:  ["录结果","결과 입력"],
  finish_wo:      ["结束工单","작업 확인"],
  leave_task:     ["离开本环节","나가기"],
  temp_unload:    ["临时卸货","임시 하차"],
  temp_loadout:   ["临时装车","임시 상차"],
  return_orig:    ["返回原工单","복귀"],
  end_session:    ["结束趟次","세션 종료"],
  no_session:     ["无当前趟次","현재 세션 없음"],
  working:        ["作业中","작업중"],
  pending_result: ["待录结果","결과 대기"],
  pending_review: ["待确认","확인 대기"],
  completed:      ["已完成","완료"],
  no_worker:      ["无","없음"],
  worker_name:    ["员工姓名","이름"],
  badge_no:       ["工牌号","명찰"],
  start_time:     ["开始时间","시작시간"],
  duration:       ["时长","시간"],
  status:         ["状态","상태"],
  active:         ["在岗","활동중"],
  closed:         ["已结束","종료"],
  confirm_badge_title:   ["扫职员工牌确认","직원 명찰 스캔 확인"],
  confirm_badge_hint:    ["必须扫描职员工牌（EMP-...）","직원 명찰(EMP-...) 스캔 필수"],
  temp_complete_confirm: ["确认暂时完成此工单？\n将截断所有参与人工时。","이 작업지시를 임시 완료하시겠습니까?\n모든 참여자의 작업시간이 마감됩니다."],
  no_current_order:      ["请先扫工单","먼저 작업지시를 스캔하세요"],
  session_auto_created:  ["已自动创建趟次","세션 자동 생성됨"],
  already_bound:         ["该工单已绑定","이미 바인딩됨"],
  worker_joined:         ["已加入工单","작업지시에 참여함"],
  result_saved:          ["结果已保存","결과 저장됨"],
  wo_confirmed:          ["工单已确认完成","작업지시 확인 완료"],
  end_blocked_working:   ["还有作业中的工单，不能结束趟次","작업중인 작업지시가 있어 세션을 종료할 수 없습니다"],
  leave_confirm:         ["确认离开本环节？","이 작업에서 나가시겠습니까?"],
  scan_worker_title:     ["扫码工牌（加入工单）","명찰 스캔 (작업지시 참여)"],
  no_active_labor:       ["当前无人在此工单作业","현재 이 작업지시에 참여자가 없습니다"],
  order_not_working:     ["该工单不在作业中状态","이 작업지시는 작업중 상태가 아닙니다"]
};
function smt_(key){ var t = SM_T[key]; return t ? t[0] + " / " + t[1] : key; }
function smtz_(key){ var t = SM_T[key]; return t ? t[0] : key; } // 仅中文
function smtk_(key){ var t = SM_T[key]; return t ? t[1] : key; } // 仅韩文
function smWfLabel_(wf){
  if(wf==="working") return smt_("working");
  if(wf==="pending_result") return smt_("pending_result");
  if(wf==="pending_review") return smt_("pending_review");
  if(wf==="completed") return smt_("completed");
  return "-";
}
function smWfColor_(wf){
  if(wf==="working") return "#e65100";
  if(wf==="pending_result") return "#f57f17";
  if(wf==="pending_review") return "#6a1b9a";
  if(wf==="completed") return "#2e7d32";
  return "#999";
}

var lastScanAt = 0;
var scanBusy = false;
var globalBusy = false; // 防止快速连点导致并发请求

// ===== B2B 工单专用扫码常量 =====
var B2B_CONFIRM_WINDOW_MS = 800;
var B2B_SUCCESS_COOLDOWN_MS = 800;
var B2B_DENY_PREFIX = []; // 可扩展：已知误扫前缀
var _b2bPendingCode = null;
var _b2bPendingTime = 0;
var _b2bCooldownUntil = 0;
var _scanFeedbackTimer = null;

function acquireBusy_(){
  if(globalBusy){ setStatus("处理中，请稍候 / 잠시만요...", false); return false; }
  globalBusy = true;
  return true;
}
function releaseBusy_(){ globalBusy = false; }
// ===== Speed test / Perf =====
var PERF_ON = true; // ✅ 需要测速就 true，不要就 false
function perfLog_(msg){
  try{ console.log("[PERF]", msg); }catch(e){}
}

var currentDaId = localStorage.getItem("da_id") || null;

var laborAction = null;
var laborBiz = null;
var laborTask = null;
var _pendingAutoSession = null; // { biz, task } — auto-session 延迟创建上下文
var _justCreatedAutoSid = null; // 刚兑现的 auto-session id，join 失败时用于回滚
var _laborDedupMap = {}; // { semanticKey: { state:"inflight"|"done", ts:ms } }
var LABOR_DEDUP_TTL = 4000;
var LABOR_INFLIGHT_TTL = 10000; // inflight 最长有效期，超时自动过期
function _laborDedupKey(action,biz,task,sid,badge){ return action+"|"+biz+"|"+task+"|"+(sid||"")+"|"+badge; }
function _laborDedupCheck(key){
  var e=_laborDedupMap[key]; if(!e) return null;
  var age=Date.now()-e.ts;
  if(e.state==="inflight" && age<LABOR_INFLIGHT_TTL) return "inflight";
  if(e.state==="done" && age<LABOR_DEDUP_TTL) return "done";
  delete _laborDedupMap[key]; return null;
}
function _laborDedupMarkInflight(key){ _laborDedupMap[key]={state:"inflight",ts:Date.now()}; }
function _laborDedupMarkDone(key){ _laborDedupMap[key]={state:"done",ts:Date.now()}; }
function _laborDedupClear(key){ delete _laborDedupMap[key]; }

var activePick = new Set();
var activeRelabel = new Set();
var activePack = new Set();
var activeTally = new Set();
var activeBulkOut = new Set();
var activeReturn = new Set();
var activeQc = new Set();
var activeDisposal = new Set();
var activeImportUnload = new Set();
var activeImportScanPallet = new Set();
var activeImportLoadout = new Set();
var activeB2bUnload = new Set();
var activeB2bTally = new Set();
var activeB2bWorkorder = new Set();
var activeB2bOutbound = new Set();
var activeB2bInventory = new Set();
var activeB2bFieldOp = new Set();
var activeB2cInventory = new Set();
var activeWarehouseCleanup = new Set();
var activeImportPickup = new Set();
var activeImportProblem = new Set();
// 备注映射：badge -> note（本地缓存，用于UI显示）
var importPickupNotes = {};
var importProblemNotes = {};

var relabelTimerHandle = null;
var relabelStartTs = null;

var leaderPickBadge = localStorage.getItem("leader_pick_badge") || null;
var leaderPickOk = localStorage.getItem("leader_pick_ok") === "1";

/** ===== Session state (server) ===== */
var SESSION_INFO_CACHE = { sid: null, ts: 0, data: null };
var SESSION_INFO_TTL_MS = 30000;

async function sessionInfoServer_(sid){
  var session = String(sid || currentSessionId || "").trim();
  if(!session) throw new Error("missing session");

  var now = Date.now();
  if(SESSION_INFO_CACHE.sid === session && (now - SESSION_INFO_CACHE.ts) < SESSION_INFO_TTL_MS && SESSION_INFO_CACHE.data){
    return SESSION_INFO_CACHE.data;
  }

  var res = await jsonp(LOCK_URL, { action: "session_info", session: session });
  if(!res || res.ok !== true) throw new Error((res && res.error) ? res.error : "session_info_failed");

  SESSION_INFO_CACHE = { sid: session, ts: now, data: res };
  return res;
}

async function isSessionClosedAsync_(){
  if(!currentSessionId) return false;
  try{
    var info = await sessionInfoServer_(currentSessionId);
    var st = String(info.status || "").trim().toUpperCase();
    return st === "CLOSED";
  }catch(e){
    // 查询失败时不强行阻断，避免现场卡死；但会少一层保护
    return false;
  }
}

async function guardSessionOpenOrAlert_(msgWhenClosed){
  if(!currentSessionId) return true;
  var closed = await isSessionClosedAsync_();
  if(!closed) return true;

  cleanupLocalSession_();
  alert(msgWhenClosed || "该趟次已结束，请重新开始或扫码加入新的趟次。");
  setStatus("该趟次已结束（已自动清除，请重新开始）", false);
  return false;
}

/** ===== Session join via QR ===== */
function sessionQrPayload_(sessionId){
  var b = CUR_CTX ? (CUR_CTX.biz || "") : "";
  var t = CUR_CTX ? (CUR_CTX.task || "") : "";
  return "CKSESSION|" + String(sessionId || "").trim() + "|" + b + "|" + t;
}
function parseSessionQr_(text){
  var t = String(text || "").trim();
  if(!t) return null;
  if(t.indexOf("CKSESSION|") === 0){
    var parts = t.split("|");
    var sid  = parts[1] ? parts[1].trim() : "";
    var biz  = parts[2] ? parts[2].trim() : "";
    var task = parts[3] ? parts[3].trim() : "";
    return sid ? { sid: sid, biz: biz, task: task } : null;
  }
  if(t.indexOf("PS-") === 0) return { sid: t, biz: "", task: "" };
  return null;
}

/** 自动绑定 session — biz/task 可来自 QR 或从服务器读取，无需手动选择 */
async function bindSessionWithCtx_(sid, biz, task){
  // 0. 先检查 session 是否仍然 OPEN（清缓存确保实时）
  SESSION_INFO_CACHE = { sid: null, ts: 0, data: null };
  try{
    var chk = await sessionInfoServer_(sid);
    if(chk && String(chk.status || "").toUpperCase() === "CLOSED"){
      alert("该趟次已结束（CLOSED），无法加入。请扫新的趟次二维码。");
      return false;
    }
    // 顺便取 biz/task
    if(!biz) biz = chk.biz || "";
    if(!task) task = chk.task || "";
  }catch(e){ /* 网络异常时继续 */ }

  // 1. 还没有就用当前页面上下文
  if(!biz || !task){
    var pageCtx = PAGE_CTX[getHashPage()] || null;
    if(pageCtx){ biz = pageCtx.biz; task = pageCtx.task; }
  }
  if(!biz || !task){
    alert("无法自动识别该趟次的任务类型，请在对应任务页面内扫码加入。");
    return false;
  }
  CUR_CTX = { biz: biz, task: task, page: getHashPage() };
  currentSessionId = sid;
  setSess_(biz, task, currentSessionId);
  SESSION_INFO_CACHE = { sid: null, ts: 0, data: null };
  restoreState();
  renderActiveLists();
  refreshUI();
  alert("已加入趟次 ✅\n" + sid + "\n任务：" + biz + " / " + task);
  setStatus("已加入趟次 ✅ " + sid, true);
  return true;
}

async function joinExistingSessionByInput(){
  var input = prompt("请输入趟次 ID（例如 PS-20260303-001）：\n세션 ID를 입력하세요:") || "";
  input = String(input).trim();
  if(!input) return;

  var parsed = parseSessionQr_(input);
  var sid = parsed ? parsed.sid : (input.indexOf("PS-") === 0 ? input : null);
  if(!sid){ alert("趟次 ID 格式不正确。"); return; }

  await bindSessionWithCtx_(sid, parsed ? parsed.biz : "", parsed ? parsed.task : "");
}

async function joinExistingSessionByScan(){
  scanMode = "session_join";
  document.getElementById("scanTitle").textContent = "扫码加入趟次 / 세션 QR 스캔";
  await openScannerCommon();
}

function showSessionQr(){
  var box = document.getElementById("sessionQrBox");
  if(!box) { alert("缺少 sessionQrBox"); return; }

  box.innerHTML = "";
  if(!currentSessionId){
    box.innerHTML = '<div class="muted">当前任务没有 session（请先点【开始】生成新趟次，或扫码加入）。</div>';
    return;
  }

  var payload = sessionQrPayload_(currentSessionId);

  var wrap = document.createElement("div");
  wrap.style.display = "flex";
  wrap.style.flexDirection = "column";
  wrap.style.alignItems = "center";
  wrap.style.gap = "8px";

  var label = document.createElement("div");
  label.className = "pill";
  label.textContent = "session: " + currentSessionId +
    (CUR_CTX ? "  |  " + CUR_CTX.biz + " / " + CUR_CTX.task : "");
  wrap.appendChild(label);

  var qrEl = document.createElement("div");
  wrap.appendChild(qrEl);

  box.appendChild(wrap);
  new QRCode(qrEl, { text: payload, width: 180, height: 180 });
}

/** ===== Persist / Restore ===== */
function keyWaves(){ return "waves_" + (currentSessionId || "NA"); }
function keyActivePick(){ return "activePick_" + (currentSessionId || "NA"); }
function keyActiveRelabel(){ return "activeRelabel_" + (currentSessionId || "NA"); }
function keyActivePack(){ return "activePack_" + (currentSessionId || "NA"); }
function keyActiveTally(){ return "activeTally_" + (currentSessionId || "NA"); }
function keyActiveBulkOut(){ return "activeBulkOut_" + (currentSessionId || "NA"); }
function keyActiveReturn(){ return "activeReturn_" + (currentSessionId || "NA"); }
function keyActiveQc(){ return "activeQc_" + (currentSessionId || "NA"); }
function keyActiveDisposal(){ return "activeDisposal_" + (currentSessionId || "NA"); }
function keyActiveImportUnload(){ return "activeImportUnload_" + (currentSessionId || "NA"); }
function keyActiveImportScanPallet(){ return "activeImportScanPallet_" + (currentSessionId || "NA"); }
function keyActiveImportLoadout(){ return "activeImportLoadout_" + (currentSessionId || "NA"); }
function keyActiveB2bUnload(){ return "activeB2bUnload_" + (currentSessionId || "NA"); }
function keyActiveB2bTally(){ return "activeB2bTally_" + (currentSessionId || "NA"); }
function keyActiveB2bWorkorder(){ return "activeB2bWorkorder_" + (currentSessionId || "NA"); }
function keyActiveB2bOutbound(){ return "activeB2bOutbound_" + (currentSessionId || "NA"); }
function keyActiveB2bInventory(){ return "activeB2bInventory_" + (currentSessionId || "NA"); }
function keyActiveB2bFieldOp(){ return "activeB2bFieldOp_" + (currentSessionId || "NA"); }
function keyActiveB2cInventory(){ return "activeB2cInventory_" + (currentSessionId || "NA"); }
function keyActiveWarehouseCleanup(){ return "activeWarehouseCleanup_" + (currentSessionId || "NA"); }
function keyActiveImportPickup(){ return "activeImportPickup_" + (currentSessionId || "NA"); }
function keyActiveImportProblem(){ return "activeImportProblem_" + (currentSessionId || "NA"); }
function keyImportPickupNotes(){ return "importPickupNotes_" + (currentSessionId || "NA"); }
function keyImportProblemNotes(){ return "importProblemNotes_" + (currentSessionId || "NA"); }

function keyInbounds(){ return "inbounds_" + (currentSessionId || "NA"); }
function keyBulkOutOrders(){ return "bulkoutOrders_" + (currentSessionId || "NA"); }
function keyB2bTallyOrders(){ return "b2bTallyOrders_" + (currentSessionId || "NA"); }
function keyB2bWorkorders(){ return "b2bWorkorders_" + (currentSessionId || "NA"); }

/**
 * TASK_REGISTRY — 每条记录描述一个"在岗任务"：
 *   task     : 任务名（与 biz/task 字段对应）
 *   get/set  : 读写对应的 activeXxx Set
 *   countId  : 显示人数的 DOM 元素 id（可为 null）
 *   listId   : 显示名单的 DOM 元素 id（可为 null）
 *   keyFn    : 返回 localStorage key 的函数
 *   emptyMsg : leaveWork 时列表为空的提示文字
 *
 * 增加新任务类型：只需在这里加一行，其余函数自动覆盖。
 */
var TASK_REGISTRY = [
  { task:"拣货",         get:function(){return activePick;},           set:function(s){activePick=s;},           countId:"pickCount",              listId:"pickActiveList",              keyFn:keyActivePick,          emptyMsg:"当前没有人在拣货作业中（无需退出）。" },
  { task:"换单",      get:function(){return activeRelabel;},        set:function(s){activeRelabel=s;},        countId:"relabelCount",           listId:"relabelActiveList",           keyFn:keyActiveRelabel,       emptyMsg:"当前没有人在换单作业中（无需退出）。" },
  { task:"打包",         get:function(){return activePack;},           set:function(s){activePack=s;},           countId:"packCount",              listId:"packActiveList",              keyFn:keyActivePack,          emptyMsg:"当前没有人在验货贴单打包作业中（无需退出）。" },
  { task:"理货",        get:function(){return activeTally;},          set:function(s){activeTally=s;},          countId:"tallyCount",             listId:"tallyActiveList",             keyFn:keyActiveTally,         emptyMsg:"当前没有人在理货作业中（无需退出）。" },
  { task:"批量出库",      get:function(){return activeBulkOut;},        set:function(s){activeBulkOut=s;},        countId:"bulkoutCount",           listId:"bulkoutActiveList",           keyFn:keyActiveBulkOut,       emptyMsg:"当前没有人在批量出库作业中（无需退出）。" },
  { task:"退件入库",      get:function(){return activeReturn;},         set:function(s){activeReturn=s;},         countId:"returnCount",            listId:"returnActiveList",            keyFn:keyActiveReturn,        emptyMsg:"当前没有人在退件入库作业中（无需退出）。" },
  { task:"质检",         get:function(){return activeQc;},             set:function(s){activeQc=s;},             countId:"qcCount",                listId:"qcActiveList",                keyFn:keyActiveQc,            emptyMsg:"当前没有人在质检作业中（无需退出）。" },
  { task:"废弃处理",      get:function(){return activeDisposal;},       set:function(s){activeDisposal=s;},       countId:"disposalCount",          listId:"disposalActiveList",          keyFn:keyActiveDisposal,      emptyMsg:"当前没有人在废弃处理作业中（无需退出）。" },
  { task:"卸货",         get:function(){return activeImportUnload;},   set:function(s){activeImportUnload=s;},   countId:"importUnloadCount",      listId:"importUnloadActiveList",      keyFn:keyActiveImportUnload,  emptyMsg:"当前没有人在卸货作业中（无需退出）。" },
  { task:"过机扫描码托",  get:function(){return activeImportScanPallet;},set:function(s){activeImportScanPallet=s;},countId:"importScanPalletCount", listId:"importScanPalletActiveList",  keyFn:keyActiveImportScanPallet, emptyMsg:"当前没有人在过机扫描码托作业中（无需退出）。" },
  { task:"装柜/出货",    get:function(){return activeImportLoadout;},  set:function(s){activeImportLoadout=s;},  countId:"importLoadoutCount",     listId:"importLoadoutActiveList",     keyFn:keyActiveImportLoadout, emptyMsg:"当前没有人在装柜/出货作业中（无需退出）。" },
  { task:"B2B卸货",      get:function(){return activeB2bUnload;},      set:function(s){activeB2bUnload=s;},      countId:"b2bUnloadCount",         listId:"b2bUnloadActiveList",         keyFn:keyActiveB2bUnload,     emptyMsg:"当前没有人在B2B卸货作业中（无需退出）。" },
  { task:"B2B入库理货",  get:function(){return activeB2bTally;},       set:function(s){activeB2bTally=s;},       countId:"b2bTallyCount",          listId:"b2bTallyActiveList",          keyFn:keyActiveB2bTally,      emptyMsg:"当前没有人在B2B入库理货作业中（无需退出）。" },
  { task:"B2B工单操作",  get:function(){return activeB2bWorkorder;},   set:function(s){activeB2bWorkorder=s;},   countId:"b2bWorkorderCount",      listId:"b2bWorkorderActiveList",      keyFn:keyActiveB2bWorkorder,  emptyMsg:"当前没有人在B2B工单操作作业中（无需退出）。" },
  { task:"B2B出库",      get:function(){return activeB2bOutbound;},    set:function(s){activeB2bOutbound=s;},    countId:"b2bOutboundCount",       listId:"b2bOutboundActiveList",       keyFn:keyActiveB2bOutbound,   emptyMsg:"当前没有人在B2B出库作业中（无需退出）。" },
  { task:"B2B盘点",      get:function(){return activeB2bInventory;},   set:function(s){activeB2bInventory=s;},   countId:"b2bInventoryCount",      listId:"b2bInventoryActiveList",      keyFn:keyActiveB2bInventory,  emptyMsg:"当前没有人在B2B盘点作业中（无需退出）。" },
  { task:"B2B现场记录",  get:function(){return activeB2bFieldOp;},    set:function(s){activeB2bFieldOp=s;},    countId:"b2bFieldOpCount",        listId:"b2bFieldOpActiveList",        keyFn:keyActiveB2bFieldOp,    emptyMsg:"当前没有人在B2B现场记录作业中（无需退出）。" },
  { task:"B2C盘点",      get:function(){return activeB2cInventory;},   set:function(s){activeB2cInventory=s;},   countId:"b2cInventoryCount",      listId:"b2cInventoryActiveList",      keyFn:keyActiveB2cInventory,  emptyMsg:"当前没有人在B2C盘点作业中（无需退出）。" },
  { task:"仓库整理",      get:function(){return activeWarehouseCleanup;},set:function(s){activeWarehouseCleanup=s;},countId:"warehouseCleanupCount",listId:"warehouseCleanupActiveList",  keyFn:keyActiveWarehouseCleanup, emptyMsg:"当前没有人在仓库整理作业中（无需退出）。" },
  { task:"取/送货",        get:function(){return activeImportPickup;},   set:function(s){activeImportPickup=s;},   countId:"importPickupCount",      listId:"importPickupActiveList",      keyFn:keyActiveImportPickup,  emptyMsg:"当前没有人在取/送货作业中（无需退出）。" },
  { task:"问题处理",        get:function(){return activeImportProblem;},  set:function(s){activeImportProblem=s;},  countId:"importProblemCount",      listId:"importProblemActiveList",      keyFn:keyActiveImportProblem, emptyMsg:"当前没有人在问题处理作业中（无需退出）。" }
];

function taskReg_(task){
  for(var i=0;i<TASK_REGISTRY.length;i++){
    if(TASK_REGISTRY[i].task === task) return TASK_REGISTRY[i];
  }
  return null;
}

var RECENT_MAX = 80;
function keyRecent(){ return "recentEventIds_" + (currentSessionId || "NA"); }
function loadRecent(){
  try{ return JSON.parse(localStorage.getItem(keyRecent()) || "[]"); }catch(e){ return []; }
}
function saveRecent(arr){
  localStorage.setItem(keyRecent(), JSON.stringify(arr.slice(-RECENT_MAX)));
}
function hasRecent(eventId){
  var arr = loadRecent();
  return arr.indexOf(eventId) >= 0;
}
function addRecent(eventId){
  var arr = loadRecent();
  arr.push(eventId);
  if(arr.length > RECENT_MAX) arr = arr.slice(arr.length-RECENT_MAX);
  saveRecent(arr);
}
function makeEventId(params){
  return [
    makeDeviceId(),
    (currentSessionId||"NA"),
    (params.wave_id||""),
    (params.biz||""),
    (params.task||""),
    (params.event||""),
    (params.badgeRaw||""),
    String(Date.now()),
    Math.random().toString(36).slice(2,8)
  ].join("|");
}

function persistState(){
  if(!currentSessionId) return;
  localStorage.setItem(keyWaves(), JSON.stringify(Array.from(scannedWaves)));
  localStorage.setItem(keyInbounds(), JSON.stringify(Array.from(scannedInbounds)));
  localStorage.setItem(keyBulkOutOrders(), JSON.stringify(Array.from(scannedBulkOutOrders)));
  localStorage.setItem(keyB2bTallyOrders(), JSON.stringify(Array.from(scannedB2bTallyOrders)));
  localStorage.setItem(keyB2bWorkorders(), JSON.stringify(Array.from(scannedB2bWorkorders)));
  TASK_REGISTRY.forEach(function(reg){
    localStorage.setItem(reg.keyFn(), JSON.stringify(Array.from(reg.get())));
  });
  localStorage.setItem(keyImportPickupNotes(), JSON.stringify(importPickupNotes));
  localStorage.setItem(keyImportProblemNotes(), JSON.stringify(importProblemNotes));
}

function restoreState(){
  if(!currentSessionId) return;
  try{ scannedWaves = new Set(JSON.parse(localStorage.getItem(keyWaves()) || "[]")); }catch(e){ scannedWaves = new Set(); }
  try{ scannedInbounds = new Set(JSON.parse(localStorage.getItem(keyInbounds()) || "[]")); }catch(e){ scannedInbounds = new Set(); }
  try{ scannedBulkOutOrders = new Set(JSON.parse(localStorage.getItem(keyBulkOutOrders()) || "[]")); }catch(e){ scannedBulkOutOrders = new Set(); }
  try{ scannedB2bTallyOrders = new Set(JSON.parse(localStorage.getItem(keyB2bTallyOrders()) || "[]")); }catch(e){ scannedB2bTallyOrders = new Set(); }
  try{ scannedB2bWorkorders = new Set(JSON.parse(localStorage.getItem(keyB2bWorkorders()) || "[]")); }catch(e){ scannedB2bWorkorders = new Set(); }
  TASK_REGISTRY.forEach(function(reg){
    try{ reg.set(new Set(JSON.parse(localStorage.getItem(reg.keyFn()) || "[]"))); }catch(e){ reg.set(new Set()); }
  });
  try{ importPickupNotes = JSON.parse(localStorage.getItem(keyImportPickupNotes()) || "{}"); }catch(e){ importPickupNotes = {}; }
  try{ importProblemNotes = JSON.parse(localStorage.getItem(keyImportProblemNotes()) || "{}"); }catch(e){ importProblemNotes = {}; }
}

/** ===== Sync active list from server (multi-device) ===== */
async function syncActiveFromServer_(){
  if(!currentSessionId) return;
  try{
    var res = await jsonp(LOCK_URL, { action:"session_info", session: currentSessionId }, { skipBusy: true });
    if(!res || !res.ok) return;
    // 缓存 owner 信息
    _sessionOwnerInfo = {
      created_by_operator: res.created_by_operator || "",
      owner_operator_id: res.owner_operator_id || res.created_by_operator || "",
      owner_changed_at: res.owner_changed_at || 0,
      owner_changed_by: res.owner_changed_by || ""
    };
    updateB2bOwnerDisplay_();
    var serverLocks = res.active || [];
    // 按 task 分组
    var byTask = {};
    serverLocks.forEach(function(lk){
      var t = lk.task || "";
      if(!byTask[t]) byTask[t] = [];
      byTask[t].push(lk);
    });
    var changed = false;
    TASK_REGISTRY.forEach(function(reg){
      var serverItems = byTask[reg.task] || [];
      var serverSet = new Set(serverItems.map(function(lk){ return lk.badge; }));
      var localSet = reg.get();
      // 添加服务器有但本地没有的
      serverItems.forEach(function(lk){
        if(!localSet.has(lk.badge)){ localSet.add(lk.badge); changed = true; }
      });
      // 移除本地有但服务器已不存在的（已在其他设备leave）
      Array.from(localSet).forEach(function(b){
        if(!serverSet.has(b)){
          localSet.delete(b); changed = true;
          // leave 清备注
          if(reg.task === "取/送货") delete importPickupNotes[b];
          if(reg.task === "问题处理") delete importProblemNotes[b];
        }
      });
      // 恢复 join_note 备注
      serverItems.forEach(function(lk){
        if(!lk.join_note) return;
        if(reg.task === "取/送货" && importPickupNotes[lk.badge] !== lk.join_note){ importPickupNotes[lk.badge] = lk.join_note; changed = true; }
        if(reg.task === "问题处理" && importProblemNotes[lk.badge] !== lk.join_note){ importProblemNotes[lk.badge] = lk.join_note; changed = true; }
      });
    });
    if(changed){
      persistState();
      renderActiveLists();
    }
  }catch(e){ /* 静默失败 */ }
}

/** ===== Utils ===== */
function getOperatorId(){
  return localStorage.getItem("operator_id") || "";
}
// 保留 makeDeviceId 供内部调用，统一走 operator_id
function makeDeviceId(){ return getOperatorId(); }

function showOperatorSetup(isChanging){
  var modal = document.getElementById("operatorSetupModal");
  var cancelRow = document.getElementById("operatorSetupCancelRow");
  var input = document.getElementById("operatorIdInput");
  if(!modal) return;
  if(isChanging){
    if(cancelRow) cancelRow.style.display = "";
    if(input) input.value = getOperatorId();
  } else {
    if(cancelRow) cancelRow.style.display = "none";
    if(input) input.value = "";
  }
  modal.classList.add("show");
}
function hideOperatorSetup(){
  var modal = document.getElementById("operatorSetupModal");
  if(modal) modal.classList.remove("show");
}
function operatorSetupClose(){ hideOperatorSetup(); }

function saveOperatorId(raw){
  raw = (raw || "").trim();
  if(!raw || !isOperatorBadge(raw)){
    alert("无效工牌格式 / 잘못된 명찰 형식\n\n格式: EMP-001|张三 / DA-...|名字\n형식: EMP-001|이름 / DA-...|이름");
    return false;
  }
  localStorage.setItem("operator_id", raw);
  hideOperatorSetup();
  refreshUI();
  fetchOperatorOpenSessions();
  return true;
}
function operatorSetupScan(){
  hideOperatorSetup();
  scanMode = "operator_setup";
  document.getElementById("scanTitle").textContent = "扫码工牌 / 명찰 스캔";
  openScannerCommon().catch(function(e){ console.error(e); showOperatorSetup(!!getOperatorId()); });
}
function operatorSetupConfirm(){
  var input = document.getElementById("operatorIdInput");
  saveOperatorId(input ? input.value : "");
}

function makePickSessionId(){
  var d = new Date();
  var yyyy = d.getFullYear();
  var mm = String(d.getMonth()+1).padStart(2,'0');
  var dd = String(d.getDate()).padStart(2,'0');
  var hh = String(d.getHours()).padStart(2,'0');
  var mi = String(d.getMinutes()).padStart(2,'0');
  var ss = String(d.getSeconds()).padStart(2,'0');
  // 取工牌 ID 部分（| 前），去掉非字母数字字符，最多取 8 位
  var opId = getOperatorId().split("|")[0].replace(/[^A-Za-z0-9]/g, "").slice(0, 8) || "NOSET";
  return "PS-" + yyyy + mm + dd + "-" + hh + mi + ss + "-" + opId;
}

function setStatus(msg, ok){
  if(ok===undefined) ok=true;
  var el = document.getElementById("status");
  if(!el) return;
  el.className = "pill " + (ok ? "ok" : "bad");
  el.textContent = msg;
}
// ===== Anti double-click / Net busy guard =====
var NET_BUSY = false;
var NET_BUSY_TIMER = null;

function netBusyOn_(action){
  NET_BUSY = true;
  // 安全超时：30秒后自动释放，防止异常情况下永久锁死
  if(NET_BUSY_TIMER) clearTimeout(NET_BUSY_TIMER);
  NET_BUSY_TIMER = setTimeout(function(){
    if(NET_BUSY){
      NET_BUSY = false;
      console.warn("[NET_BUSY] safety timeout released after 30s");
    }
  }, 30000);
  // 给用户一个明确提示，避免疯狂连点
  if(action){
    setStatus("请求中... " + action + "（请勿重复点击）⏳", true);
  }
}

function netBusyOff_(){
  NET_BUSY = false;
  if(NET_BUSY_TIMER){ clearTimeout(NET_BUSY_TIMER); NET_BUSY_TIMER = null; }
}
function refreshUI(){
  var dev = document.getElementById("device");
  var ses = document.getElementById("session");
  if(dev){
    var op = getOperatorId();
    if(op){
      var p = parseBadge(op);
      dev.textContent = p.name ? p.id + " · " + p.name : p.id;
    } else {
      dev.textContent = "未设置 / 미설정";
    }
  }
  if(ses){
    if(CUR_CTX && CUR_CTX.biz && CUR_CTX.task){
      ses.textContent = (currentSessionId || "无 / 없음") + "  [" + CUR_CTX.biz + "/" + CUR_CTX.task + "]";
    }else{
      ses.textContent = currentSessionId || "无 / 없음";
    }
  }
}

/** ===== Network pill ===== */
function refreshNet(){
  var el = document.getElementById("netPill");
  if(!el) return;
  el.textContent = navigator.onLine ? "Online" : "Offline";
  el.style.borderColor = navigator.onLine ? "#0a0" : "#b00";
}
window.addEventListener("online", refreshNet);
window.addEventListener("offline", refreshNet);

/** ===== JSONP (with PERF) ===== */
function jsonp(url, params, opts){
  var skipBusy = opts && opts.skipBusy; // ✅ 队列 flush 用：不占 NET_BUSY 锁
  return new Promise(function(resolve, reject){
    var action = (params && params.action) ? String(params.action) : "";
    // ✅ 只有用户发起的请求才检查/占用 NET_BUSY
    if(!skipBusy){
      if(NET_BUSY){
        reject(new Error("busy: previous request not finished"));
        return;
      }
      netBusyOn_(action);
    }
    var cb = "cb_" + Math.random().toString(16).slice(2);
    var qs = [];
    for(var k in params){
      if(!params.hasOwnProperty(k)) continue;
      qs.push(encodeURIComponent(k) + "=" + encodeURIComponent(params[k]));
    }
    qs.push("callback=" + encodeURIComponent(cb));
    var src = url + "?" + qs.join("&");

    // URL 过长保护（浏览器一般限制 ~8000 字符，保守取 6000）
    if(src.length > 6000){
      if(!skipBusy) netBusyOff_();
      reject(new Error("URL 过长（" + src.length + " 字符），请减少参数数据量。"));
      return;
    }

    // PERF
    var t0 = Date.now();
    if(PERF_ON && action && !skipBusy){
      setStatus("请求中... " + action + " ⏳", true);
    }

    var script = document.createElement("script");
    var timer = setTimeout(function(){
      cleanup();
      var dt = Date.now() - t0;
      if(PERF_ON && action && !skipBusy){
        setStatus("超时 ❌ " + action + " " + dt + "ms", false);
        perfLog_("TIMEOUT action=" + action + " dt=" + dt + "ms src=" + src);
      }
      reject(new Error("jsonp timeout"));
    }, 20000);

    function cleanup(){
      try{ delete window[cb]; }catch(e){ window[cb]=undefined; }
      if(script && script.parentNode) script.parentNode.removeChild(script);
      clearTimeout(timer);
      if(!skipBusy) netBusyOff_();
    }

    window[cb] = function(data){
      cleanup();
      var dt = Date.now() - t0;
      var ok = data && data.ok === true;

      if(PERF_ON && action && !skipBusy){
        setStatus((ok ? "完成 ✅ " : "失败 ❌ ") + action + " " + dt + "ms", ok);
        perfLog_((ok ? "OK" : "BAD") + " action=" + action + " dt=" + dt + "ms");
      }
      resolve(data);
    };

    script.onerror = function(){
      cleanup();
      var dt = Date.now() - t0;
      if(PERF_ON && action && !skipBusy){
        setStatus("错误 ❌ " + action + " " + dt + "ms", false);
        perfLog_("ERROR action=" + action + " dt=" + dt + "ms src=" + src);
      }
      reject(new Error("jsonp error"));
    };

    script.src = src;
    document.body.appendChild(script);
  });
}

/** ===== fetchApi (admin endpoints, POST body hides sensitive key) ===== */
async function fetchApi(params){
  try{
    var res = await fetch(LOCK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params)
    });
    return await res.json();
  }catch(e){
    return { ok:false, error:"network_error" };
  }
}

/** ===== Async event queue (non-locking events) ===== */
var QUEUE_KEY = "event_queue_v1";
var FLUSHING = false;

function loadQueue_(){
  try{ return JSON.parse(localStorage.getItem(QUEUE_KEY) || "[]"); }catch(e){ return []; }
}
function saveQueue_(q){
  localStorage.setItem(QUEUE_KEY, JSON.stringify(q.slice(-200)));
}
function enqueueEvent_(payload){
  var q = loadQueue_();
  q.push({ payload: payload, tries: 0, enq_ms: Date.now() });
  saveQueue_(q);
}

async function flushQueue_(){
  if(FLUSHING) return;
  if(!navigator.onLine) return;

  var q = loadQueue_();
  if(q.length === 0) return;

  FLUSHING = true;
  try{
    var keep = [];
    var dropped = 0;
    for(var i=0;i<q.length;i++){
      var item = q[i];
      try{
        item.tries = (item.tries||0) + 1;
        await submitEventSync_(item.payload, true);
      }catch(e){
        if(item.tries < 8){ keep.push(item); }
        else { dropped++; }
      }
    }
    saveQueue_(keep);
    if(dropped > 0){
      setStatus("⚠️ " + dropped + " 条数据上传失败已丢弃，请检查网络", false);
    }
  } finally {
    FLUSHING = false;
  }
}

setInterval(function(){ flushQueue_(); }, 5000);
window.addEventListener("online", function(){ flushQueue_(); });

/** ===== Submit event (sync) ===== */
async function submitEventSync_(o, silent){
  var params = {
    action: "event_submit",
    event_id: o.event_id || "",
    event: o.event || "",
    biz: o.biz || "",
    task: o.task || "",
    pick_session_id: o.pick_session_id || "NA",
    wave_id: o.wave_id || "",
    da_id: o.da_id || "",
    operator_id: getOperatorId(),
    client_ms: (o.client_ms || Date.now()),
    note: o.note || ""
  };
  if(o.temp_switch) params.temp_switch = "1";

  var res = await jsonp(LOCK_URL, params, silent ? { skipBusy: true } : undefined);

  if(!res || res.ok !== true){
  var er = (res && res.error) ? String(res.error) : "提交失败：event_submit failed";
  if(er === "task_not_started"){
    throw new Error("该环节还没点【开始】。\n请先点击【开始理货/开始拣货/开始换单/开始批量出库】再扫码加入。");
  }
  if(er === "session_closed"){
    cleanupLocalSession_();
    throw new Error("该趟次已被关闭（可能是管理员强制结束）。\n本地已自动清除，请重新开始新的趟次。");
  }
    if(er === "operator_has_open_session"){
  var ob = (res && res.open_biz) ? res.open_biz : "";
  var ot = (res && res.open_task) ? res.open_task : "";
  var os = (res && res.open_session) ? res.open_session : "";
  throw new Error(
    "本机还有未结束趟次：\n" +
    (ob && ot ? (ob + " / " + ot) : "") +
    (os ? ("\n" + os) : "") +
    "\n\n请先点击【结束】把上一趟结束后，再开始新的。"
  );
}
  throw new Error(er);
}

  if(res.locked === false){
    var lk = res.lock || {};
    var msg =
      "该工牌已在其它设备作业中，无法加入。\n\n" +
      "占用任务: " + (lk.task || "未知") + "\n" +
      "占用设备: " + (lk.operator_id || "未知") + "\n" +
      "占用趟次: " + (lk.session || "未知") + "\n\n" +
      "请先在原设备退出（leave）后再加入。";
    throw new Error(msg);
  }

  return res;
}

async function submitEventSyncWithRetry_(payload, maxRetries){
  maxRetries = maxRetries || 2;
  var lastErr;
  for(var attempt = 0; attempt <= maxRetries; attempt++){
    try{
      return await submitEventSync_(payload);
    }catch(e){
      lastErr = e;
      var msg = String(e && e.message ? e.message : e);
      if(msg.indexOf("task_not_started") >= 0 ||
         msg.indexOf("session_closed") >= 0 ||
         msg.indexOf("operator_has_open_session") >= 0 ||
         msg.indexOf("已在其它设备") >= 0 ||
         msg.indexOf("locked_by_other") >= 0){
        throw e;
      }
      if(attempt < maxRetries){
        await new Promise(function(r){ setTimeout(r, 1000 * (attempt + 1)); });
      }
    }
  }
  throw lastErr;
}

async function submitEvent(o){
  enqueueEvent_(o);
  flushQueue_();
  return { ok:true, queued:true };
}

/** ===== Global session close helpers ===== */
async function sessionCloseServer_(){
  if(!currentSessionId) throw new Error("missing session");
  var res = await jsonp(LOCK_URL, {
    action: "session_close",
    session: currentSessionId,
    operator_id: getOperatorId()
  });
  if(!res || res.ok !== true) throw new Error(res && res.error ? res.error : "session_close_failed");
  return res;
}

function formatActiveListForAlert_(active){
  if(!active || !active.length) return "";
  return active.map(function(x){
    return (x.badge||"") + " (" + (x.task||"") + ")";
  }).join("\n");
}

// 按指定上下文清理某个 source task 的本地 session + 缓存（不影响 currentSessionId / CUR_CTX）
function clearTaskLocalState_(biz, task, sid){
  if(!sid) return;
  // 临时切换 currentSessionId 以计算 localStorage key
  var savedSid = currentSessionId;
  currentSessionId = sid;
  // 清 localStorage 缓存（key 依赖 currentSessionId）
  localStorage.removeItem(keyWaves());
  localStorage.removeItem(keyInbounds());
  localStorage.removeItem(keyBulkOutOrders());
  localStorage.removeItem(keyB2bTallyOrders());
  localStorage.removeItem(keyB2bWorkorders());
  localStorage.removeItem(keyRecent());
  localStorage.removeItem(keyImportPickupNotes());
  localStorage.removeItem(keyImportProblemNotes());
  // 恢复 currentSessionId
  currentSessionId = savedSid;
  // 清该任务的 active set
  var reg = taskReg_(task);
  if(reg) reg.set(new Set());
  // 清该任务对应备注
  if(task === "取/送货"){ importPickupNotes = {}; }
  if(task === "问题处理"){ importProblemNotes = {}; }
  // 清 session 映射
  clearSess_(biz, task);
}

function cleanupLocalSession_(){
  localStorage.removeItem(keyWaves());
  localStorage.removeItem(keyInbounds());
  localStorage.removeItem(keyBulkOutOrders());
  localStorage.removeItem(keyB2bTallyOrders());
  localStorage.removeItem(keyB2bWorkorders());
  localStorage.removeItem(keyRecent());
  localStorage.removeItem(keyImportPickupNotes());
  localStorage.removeItem(keyImportProblemNotes());
  importPickupNotes = {};
  importProblemNotes = {};
  TASK_REGISTRY.forEach(function(reg){
    localStorage.removeItem(reg.keyFn());
    reg.set(new Set());
  });

  if(CUR_CTX && CUR_CTX.biz && CUR_CTX.task){
    clearSess_(CUR_CTX.biz, CUR_CTX.task);
  }
  currentSessionId = null;
  leaderPickOk = false; localStorage.setItem("leader_pick_ok", "0");
  refreshUI();
  fetchOperatorOpenSessions();
}

async function endSessionGlobal_(){
  if(!currentSessionId){ setStatus("没有未结束趟次", false); return "no_session"; }

  // 最多重试3次（lock释放可能有延迟）
  var r;
  for(var _retry = 0; _retry < 3; _retry++){
    r = await sessionCloseServer_();
    if(!r.blocked) break;
    if(_retry < 2) await new Promise(function(res){ setTimeout(res, 800); });
  }
  if(r.blocked){
    if(r.reason === "pending_b2b_results"){
      var orders = (r.pending_orders || []).map(function(o){
        var st = o.result_status === "missing" ? "未录入" : (o.result_status === "draft" ? "草稿未提交" : o.result_status);
        return o.source_order_no + " 〔" + st + "〕";
      }).join("\n");
      var msg = "当前还有未完成提交的工单结果单，不能结束本趟作业。\n\n请先完成以下工单的结果单：\n" + orders +
        "\n\n趟次：" + currentSessionId;
      setStatus("有未完成结果单，禁止结束", false);
      if(getHashPage() !== "b2b_workorder"){
        msg += "\n\n点【确定】跳转到B2B工单操作页处理。";
        if(confirm(msg)){
          setSess_("B2B", "B2B工单操作", currentSessionId);
          go("b2b_workorder");
        }
      } else {
        alert(msg);
      }
      return "blocked";
    }
    var msg = "还有人员未退出，不能结束。\n\n" + formatActiveListForAlert_(r.active);
    setStatus("还有人员未退出，禁止结束", false);
    alert(msg);
    return "blocked";
  }
  if(r.already_closed){
    alert("该趟次已结束（无需重复结束）");
    setStatus("该趟次已结束（无需重复结束）", true);
    cleanupLocalSession_();
    return "already_closed";
  }

  var endBiz = (CUR_CTX && CUR_CTX.biz) ? String(CUR_CTX.biz) : "B2C"; // ✅ 用当前任务的 biz
  var evId = makeEventId({ event:"end", biz: endBiz, task:"SESSION", wave_id:"", badgeRaw:"" });
  if(!hasRecent(evId)){
    try{
      await submitEventSync_({ event:"end", event_id: evId, biz: endBiz, task:"SESSION", pick_session_id: currentSessionId }, true);
    }catch(e){
      // 同步失败时降级到异步队列，确保不丢失
      submitEvent({ event:"end", event_id: evId, biz: endBiz, task:"SESSION", pick_session_id: currentSessionId });
    }
    addRecent(evId);
  }

  setStatus("趟次已结束 ✅", true);
  cleanupLocalSession_();
  return "closed";
}

async function endAllTask(biz, task){
  if(!acquireBusy_()) return;
  try{
    var sid = getSess_(biz, task);
    if(!sid){ alert("当前没有进行中的作业 / 진행 중인 작업 없음"); return; }
    var reg = taskReg_(task);
    var cnt = reg ? reg.get().size : 0;
    if(!confirm("确定全员结束？" + cnt + "人将自动退出。\n전원 종료하시겠습니까? " + cnt + "명 자동 퇴장.")) return;
    currentSessionId = sid;
    CUR_CTX = { biz: biz, task: task, page: getHashPage() };
    await endSessionGlobal_();
    refreshUI();
    renderActiveLists();
  }finally{ releaseBusy_(); }
}

function taskAutoSession_(task){
  return task === "打包" || task === "退件入库" || task === "质检" || task === "废弃处理" || task === "卸货" || task === "过机扫描码托" || task === "装柜/出货"
    || task === "B2B卸货" || task === "B2B出库" || task === "B2B盘点"
    || task === "B2C盘点" || task === "仓库整理"
    || task === "取/送货" || task === "问题处理";
}

async function tryAutoEndSessionAfterLeave_(){
  if(!taskAutoSession_(laborTask)) return;
  if(!currentSessionId) return;

  try{
    var r = await sessionCloseServer_();
    if(r && r.blocked) return;

    if(r && r.already_closed){
      cleanupLocalSession_();
      return;
    }

    if(r && r.closed){
      var endBiz = (laborBiz && String(laborBiz).trim()) ? String(laborBiz).trim()
           : ((CUR_CTX && CUR_CTX.biz) ? String(CUR_CTX.biz) : "B2C"); // ✅ 优先用本次 leave 的 biz
      var evIdEnd = makeEventId({ event:"end", biz: endBiz, task:"SESSION", wave_id:"", badgeRaw:"" });
      if(!hasRecent(evIdEnd)){
        try{
          await submitEventSync_({ event:"end", event_id: evIdEnd, biz: endBiz, task:"SESSION", pick_session_id: currentSessionId }, true);
        }catch(e2){
          submitEvent({ event:"end", event_id: evIdEnd, biz: endBiz, task:"SESSION", pick_session_id: currentSessionId });
        }
        addRecent(evIdEnd);
      }
      cleanupLocalSession_();
      return;
    }
  }catch(e){}
}

/** ===== Badge helpers ===== */
function parseBadge(code){
  var raw = (code || "").trim();
  var parts = raw.split("|");
  var id = (parts[0] || "").trim();
  var name = (parts[1] || "").trim();
  return { raw: raw, id: id, name: name };
}
function isDaId(id){ return /^DA-\d{6,8}-.+$/.test(id); }
function isEmpId(id){ return /^EMP-.+$/.test(id); }
function isPermanentDaId(id){ return /^DAF-.+$/.test(id); }
function isOperatorBadge(raw){
  var p = parseBadge(raw);
  return isDaId(p.id) || isEmpId(p.id) || isPermanentDaId(p.id);
}

/** ===== Active (local cache only) — driven by TASK_REGISTRY ===== */
function isAlreadyActive(task, badge){
  var reg = taskReg_(task);
  return reg ? reg.get().has(badge) : false;
}
function applyActive(task, action, badge){
  var reg = taskReg_(task);
  if(!reg) return;
  if(action==="join") reg.get().add(badge);
  if(action==="leave") reg.get().delete(badge);
}

/** ===== Render lists ===== */
function badgeDisplay(raw){
  var p = parseBadge(raw);
  return p.name ? (p.id + "｜" + p.name) : p.id;
}
function renderSetToHtml(setObj){
  var arr = Array.from(setObj);
  if(arr.length === 0) return '<span class="muted">无 / 없음</span>';
  return arr.map(function(x){ return '<span class="tag">' + esc(badgeDisplay(x)) + '</span>'; }).join("");
}

function renderSetWithNotesToHtml(setObj, notesMap){
  var arr = Array.from(setObj);
  if(arr.length === 0) return '<span class="muted">无 / 없음</span>';
  return arr.map(function(x){
    var display = badgeDisplay(x);
    var note = notesMap[x];
    if(note) display += " · " + note;
    return '<span class="tag">' + esc(display) + '</span>';
  }).join("");
}

function renderActiveLists(){
  TASK_REGISTRY.forEach(function(reg){
    var cnt = reg.countId ? document.getElementById(reg.countId) : null;
    var lst = reg.listId ? document.getElementById(reg.listId) : null;
    if(cnt) cnt.textContent = String(reg.get().size);
    if(lst){
      if(reg.task === "取/送货") lst.innerHTML = renderSetWithNotesToHtml(reg.get(), importPickupNotes);
      else if(reg.task === "问题处理") lst.innerHTML = renderSetWithNotesToHtml(reg.get(), importProblemNotes);
      else lst.innerHTML = renderSetToHtml(reg.get());
    }
  });
}

function renderWaveUI(){
  var c = document.getElementById("waveCount");
  var l = document.getElementById("waveList");
  if(c) c.textContent = String(scannedWaves.size);
  if(l){
    if(scannedWaves.size === 0){
      l.innerHTML = '<span class="muted">无 / 없음</span>';
    }else{
      var arr = Array.from(scannedWaves);
      var show = arr.slice(Math.max(0, arr.length - 30));
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+esc(String(x))+'</span>'; }).join(" ");
    }
  }
}

function renderInboundCountUI(){
  var c = document.getElementById("inboundCount");
  var l = document.getElementById("inboundList");
  if(c) c.textContent = String(scannedInbounds.size);
  if(l){
    if(scannedInbounds.size === 0){
      l.innerHTML = '<span class="muted">无 / 없음</span>';
    }else{
      var arr = Array.from(scannedInbounds);
      var show = arr.slice(Math.max(0, arr.length - 30));
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+esc(String(x))+'</span>'; }).join(" ");
    }
  }
}

function renderBulkOutUI(){
  var c = document.getElementById("bulkoutOrderCount");
  var l = document.getElementById("bulkoutOrderList");
  if(c) c.textContent = String(scannedBulkOutOrders.size);
  if(l){
    if(scannedBulkOutOrders.size === 0){
      l.innerHTML = '<span class="muted">无 / 없음</span>';
    }else{
      var arr = Array.from(scannedBulkOutOrders);
      var show = arr.slice(Math.max(0, arr.length - 30));
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+esc(String(x))+'</span>'; }).join(" ");
    }
  }
}

/** ===== Global Active Now (legacy) ===== */
function esc(s){
  return String(s||"").replace(/[&<>"']/g,function(c){
    return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]);
  });
}
function fmtDur(ms){
  if(!ms || ms<0) return "";
  var sec = Math.floor(ms/1000);
  var h = Math.floor(sec/3600);
  var m = Math.floor((sec%3600)/60);
  if(h>0) return h + " 小时 " + m + " 分钟";
  return m + " 分钟";
}

var _activeNowData = [];
var _activeNowAsof = 0;
var _activeNowDetailKey = null; // "biz/task" currently shown in detail
var _activeNowTimer = null;

async function refreshActiveNow(){
  try{
    setStatus("拉取在岗中... ⏳", true);
    var res = await jsonp(LOCK_URL, { action:"active_now" });
    if(!res || res.ok !== true){
      setStatus("在岗拉取失败 ❌ " + (res && res.error ? res.error : ""), false);
      return;
    }
    _activeNowData = res.active || [];
    _activeNowAsof = res.asof || Date.now();

    var meta = document.getElementById("activeNowMeta");
    if(meta){
      var taskSet = {};
      _activeNowData.forEach(function(x){ taskSet[(x.biz||"")+"/"+(x.task||"")] = 1; });
      var taskCount = Object.keys(taskSet).length;
      meta.textContent = "在岗 " + _activeNowData.length + " 人 · 覆盖 " + taskCount + " 个任务 ｜ " + new Date(_activeNowAsof).toLocaleTimeString();
    }

    // 如果当前在详情页，刷新详情；否则渲染索引
    if(_activeNowDetailKey){
      var parts = _activeNowDetailKey.split("/");
      renderActiveNowDetail_(parts[0], parts.slice(1).join("/"));
    } else {
      renderActiveNowIndex_();
    }
    setStatus("在岗已更新 ✅", true);
  }catch(e){
    setStatus("在岗拉取异常 ❌ " + e, false);
  }
}

function renderActiveNowIndex_(){
  _activeNowDetailKey = null;
  var titleEl = document.getElementById("activeNowTitle");
  if(titleEl) titleEl.textContent = "实时在岗";
  var indexEl = document.getElementById("activeNowIndex");
  var detailEl = document.getElementById("activeNowDetail");
  if(detailEl) detailEl.style.display = "none";
  if(!indexEl) return;
  indexEl.style.display = "";

  // 按 biz/task 分组统计
  var by = {};
  _activeNowData.forEach(function(x){
    var k = (x.biz||"") + "/" + (x.task||"");
    if(!by[k]) by[k] = { biz: x.biz||"", task: x.task||"", count: 0 };
    by[k].count++;
  });

  if(Object.keys(by).length === 0){
    indexEl.innerHTML = '<div class="muted" style="padding:10px 0;">当前无人在岗</div>';
    return;
  }

  // 按业务线分组，每组内固定排序
  var BIZ_ORDER = ["B2C", "进口", "B2B", "仓库"];
  var TASK_SORT = {
    "拣货":1, "打包":2, "过机扫描码托":3, "换单":4, "退件入库":5, "质检":6,
    "理货":7, "批量出库":8, "废弃处理":9, "B2C盘点":10,
    "卸货":1, "装柜/出货":2, "取/送货":3, "问题处理":4,
    "B2B卸货":1, "B2B入库理货":2, "B2B工单操作":3, "B2B出库":4, "B2B盘点":5,
    "仓库整理":1
  };

  var html = "";
  BIZ_ORDER.forEach(function(biz){
    // 收集该 biz 下有人的任务
    var tasks = [];
    Object.keys(by).forEach(function(k){
      if(by[k].biz === biz) tasks.push(by[k]);
    });
    if(tasks.length === 0) return;

    tasks.sort(function(a,b){ return (TASK_SORT[a.task]||99) - (TASK_SORT[b.task]||99); });

    html += '<div style="margin-top:12px;margin-bottom:6px;font-size:13px;font-weight:700;color:#666;">' + esc(biz) + '</div>';
    html += '<div class="grid2">';
    tasks.forEach(function(t){
      // 卡片只显示任务名，不带 biz 前缀
      var shortName = t.task;
      html += '<button style="text-align:left;line-height:1.3;" ' +
        'data-biz="'+esc(t.biz)+'" data-task="'+esc(t.task)+'" onclick="activeNowShowDetail(this.dataset.biz,this.dataset.task)">' +
        '<div style="font-size:13px;">' + esc(shortName) + '</div>' +
        '<div style="font-size:22px;font-weight:800;margin-top:4px;">' + t.count + ' <small style="font-size:13px;">人</small></div>' +
        '</button>';
    });
    html += '</div>';
  });

  // 处理不在 BIZ_ORDER 里的其他 biz（兜底）
  Object.keys(by).forEach(function(k){
    if(BIZ_ORDER.indexOf(by[k].biz) === -1){
      html += '<div style="margin-top:12px;margin-bottom:6px;font-size:13px;font-weight:700;color:#666;">' + esc(by[k].biz || "其他") + '</div>';
      html += '<div class="grid2"><button style="text-align:left;line-height:1.3;" ' +
        'data-biz="'+esc(by[k].biz)+'" data-task="'+esc(by[k].task)+'" onclick="activeNowShowDetail(this.dataset.biz,this.dataset.task)">' +
        '<div style="font-size:13px;">' + esc(by[k].task) + '</div>' +
        '<div style="font-size:22px;font-weight:800;margin-top:4px;">' + by[k].count + ' <small style="font-size:13px;">人</small></div>' +
        '</button></div>';
    }
  });

  indexEl.innerHTML = html;
}

function activeNowShowDetail(biz, task){
  _activeNowDetailKey = biz + "/" + task;
  renderActiveNowDetail_(biz, task);
}

function renderActiveNowDetail_(biz, task){
  var titleEl = document.getElementById("activeNowTitle");
  var indexEl = document.getElementById("activeNowIndex");
  var detailEl = document.getElementById("activeNowDetail");
  if(indexEl) indexEl.style.display = "none";
  if(!detailEl) return;
  detailEl.style.display = "";

  var workers = _activeNowData.filter(function(x){
    return (x.biz||"") === biz && (x.task||"") === task;
  });

  if(titleEl) titleEl.textContent = task + " · " + workers.length + " 人";

  var now = Date.now();
  var isAdmin = adminIsUnlocked_();

  detailEl.innerHTML = workers.length === 0
    ? '<div class="muted">无人在岗</div>'
    : workers.map(function(x){
        var dur = fmtDur(now - (x.since||now));
        var forceBtn = isAdmin
          ? '<button class="small bad" style="margin-top:6px;width:auto;" ' +
              'data-badge="'+esc(x.badge)+'" data-task="'+esc(x.task||"")+'" data-session="'+esc(x.session||"")+'" data-biz="'+esc(x.biz||"")+'" ' +
              'onclick="adminForceLeave(this)">强制下线 / 강제 퇴장</button>'
          : "";
        return '<div style="border:1px solid #eee;border-radius:12px;padding:10px;margin:8px 0;">' +
          '<div style="font-weight:700;">'+esc(x.badge)+'</div>' +
          '<div class="muted" style="margin-top:4px;">已在岗 '+esc(dur)+'</div>' +
          forceBtn +
        '</div>';
      }).join("");
}

function activeNowBack(){
  if(_activeNowDetailKey){
    renderActiveNowIndex_();
  } else {
    back();
  }
}

async function adminForceLeave(btn){
  if(!adminIsUnlocked_()){ alert("请先解锁管理员模式"); return; }
  var badge = btn.getAttribute("data-badge") || "";
  var task = btn.getAttribute("data-task") || "";
  var session = btn.getAttribute("data-session") || "";
  var biz = btn.getAttribute("data-biz") || "";
  var ok = confirm("强制下线 / 강제 퇴장\n\n工牌：" + badge + "\n任务：" + biz + " / " + task + "\n\n确定要强制下线吗？\n정말 강제 퇴장하시겠습니까?");
  if(!ok) return;
  try{
    setStatus("强制下线中... ⏳", true);
    var res = await fetchApi({ action:"admin_force_leave", k:adminKey_(), badge:badge, task:task, session:session, biz:biz });
    if(!res || res.ok !== true){
      setStatus("强制下线失败 ❌ " + (res && res.error ? res.error : ""), false);
      alert("强制下线失败：" + (res && res.error ? res.error : "unknown"));
      return;
    }
    setStatus("强制下线成功 ✅", true);
    await refreshActiveNow();
  }catch(e){
    setStatus("强制下线异常 ❌ " + e, false);
    alert("强制下线异常：" + e);
  }
}

var _globalSessionsData = [];
var _globalSessionsFilter = null; // "OPEN" | "CLOSED" | null
var _globalSessionsViewSession = null; // session ID being viewed for events

var GS_TASK_MAP = {
  "B2C": ["理货","拣货","打包","退件入库","质检","废弃处理","换单","B2C盘点","批量出库"],
  "B2B": ["B2B卸货","B2B入库理货","B2B工单操作","B2B现场记录","B2B出库","B2B盘点"],
  "进口": ["卸货","过机扫描码托","装柜/出货","取/送货","问题处理"],
  "仓库": ["仓库整理"]
};

function gsUpdateTasks(){
  var biz = document.getElementById("gsBiz").value;
  var sel = document.getElementById("gsTask");
  sel.innerHTML = '<option value="">全部</option>';
  var tasks = GS_TASK_MAP[biz] || [];
  tasks.forEach(function(t){
    sel.innerHTML += '<option value="' + esc(t) + '">' + esc(t) + '</option>';
  });
}

function gsKstDayStartMs_(dayKey){
  if(!dayKey) return 0;
  return Date.parse(dayKey + "T00:00:00.000Z") - 9*3600*1000;
}
function gsKstDayEndMs_(dayKey){
  if(!dayKey) return 0;
  return gsKstDayStartMs_(dayKey) + 24*3600*1000 - 1;
}

async function refreshGlobalSessions(){
  var metaEl = document.getElementById("sessionListMeta");
  if(metaEl) metaEl.textContent = "加载中... ⏳";

  var params = { action:"admin_sessions_list", k:adminKey_() };

  var dateFrom = document.getElementById("gsDateFrom");
  var dateTo = document.getElementById("gsDateTo");
  var bizSel = document.getElementById("gsBiz");
  var taskSel = document.getElementById("gsTask");

  if(dateFrom && dateFrom.value) params.since_ms = gsKstDayStartMs_(dateFrom.value);
  if(dateTo && dateTo.value) params.until_ms = gsKstDayEndMs_(dateTo.value);
  if(bizSel && bizSel.value) params.biz = bizSel.value;
  if(taskSel && taskSel.value) params.task = taskSel.value;

  try{
    var res = await fetchApi(params);
    if(!res || res.ok !== true){
      if(metaEl) metaEl.textContent = "加载失败 ❌ " + (res && res.error ? res.error : "");
      return;
    }
    _globalSessionsData = res.sessions || [];
    var open = _globalSessionsData.filter(function(s){ return s.status==="OPEN"; }).length;
    var closed = _globalSessionsData.length - open;
    if(metaEl) metaEl.textContent = "共 " + _globalSessionsData.length + " 条 ｜ OPEN: " + open + " ｜ CLOSED: " + closed;

    _globalSessionsViewSession = null;
    var eventsEl = document.getElementById("globalSessionsEvents");
    if(eventsEl) eventsEl.style.display = "none";

    if(_globalSessionsFilter){
      renderGlobalSessionsDetail_(_globalSessionsFilter);
    } else {
      renderGlobalSessionsIndex_();
    }
  }catch(e){
    if(metaEl) metaEl.textContent = "加载异常 ❌ " + e;
  }
}

function renderGlobalSessionsIndex_(){
  _globalSessionsFilter = null;
  _globalSessionsViewSession = null;
  var titleEl = document.getElementById("globalSessionsTitle");
  if(titleEl) titleEl.textContent = "全局Session / Sessions";
  var indexEl = document.getElementById("globalSessionsIndex");
  var detailEl = document.getElementById("globalSessionsDetail");
  var eventsEl = document.getElementById("globalSessionsEvents");
  if(detailEl) detailEl.style.display = "none";
  if(eventsEl) eventsEl.style.display = "none";
  if(!indexEl) return;
  indexEl.style.display = "";

  var open = _globalSessionsData.filter(function(s){ return s.status==="OPEN"; });
  var closed = _globalSessionsData.filter(function(s){ return s.status!=="OPEN"; });

  indexEl.innerHTML = '<div class="grid2">' +
    '<button data-status="OPEN" onclick="globalSessionsShowDetail(this.dataset.status)" style="text-align:left;line-height:1.3;">' +
      '<div style="font-size:13px;" class="ok">● OPEN</div>' +
      '<div style="font-size:22px;font-weight:800;margin-top:4px;">' + open.length + ' <small style="font-size:13px;">条</small></div>' +
    '</button>' +
    '<button data-status="CLOSED" onclick="globalSessionsShowDetail(this.dataset.status)" style="text-align:left;line-height:1.3;">' +
      '<div style="font-size:13px;" class="muted">● CLOSED</div>' +
      '<div style="font-size:22px;font-weight:800;margin-top:4px;">' + closed.length + ' <small style="font-size:13px;">条</small></div>' +
    '</button>' +
  '</div>';
}

function globalSessionsShowDetail(status){
  _globalSessionsFilter = status;
  _globalSessionsViewSession = null;
  renderGlobalSessionsDetail_(status);
}

function renderGlobalSessionsDetail_(status){
  var titleEl = document.getElementById("globalSessionsTitle");
  var indexEl = document.getElementById("globalSessionsIndex");
  var detailEl = document.getElementById("globalSessionsDetail");
  var eventsEl = document.getElementById("globalSessionsEvents");
  if(indexEl) indexEl.style.display = "none";
  if(eventsEl) eventsEl.style.display = "none";
  if(!detailEl) return;
  detailEl.style.display = "";

  var list = _globalSessionsData.filter(function(s){
    return status === "OPEN" ? s.status === "OPEN" : s.status !== "OPEN";
  });
  if(titleEl) titleEl.textContent = status + " · " + list.length + "条";

  if(list.length === 0){
    detailEl.innerHTML = '<div class="muted">暂无记录</div>';
    return;
  }

  detailEl.innerHTML = list.map(function(s){
    var activeList = (s.active||[]).map(function(lk){ return esc(lk.badge||""); }).join(", ");
    var forceEndBtn = (s.status==="OPEN" && (!s.active || s.active.length===0))
      ? '<button class="small bad" style="margin-top:6px;width:auto;" data-session="'+esc(s.session)+'" onclick="adminForceEndSession(this)">强制结束</button>'
      : "";
    var taskLabel = (s.biz && s.task) ? taskDisplayLabel(s.biz, s.task) : (s.biz||"-");
    var viewEventsBtn = '<button class="small" style="margin-top:6px;width:auto;margin-left:6px;" data-session="'+esc(s.session)+'" onclick="gsViewEvents(this.dataset.session)">查看/修正事件</button>';
    var sourceTag = (s.source === "manual_correction")
      ? ' <span style="background:#f0ad4e;color:#fff;font-size:10px;padding:1px 5px;border-radius:4px;margin-left:6px;">补录</span>'
      : '';
    return (
      '<div style="border:1px solid #eee;border-radius:12px;padding:10px;margin:8px 0;">' +
        '<div style="font-weight:700;font-size:13px;">'+esc(s.session)+sourceTag+'</div>' +
        '<div style="margin-top:4px;">'+esc(taskLabel)+'</div>' +
        '<div class="muted" style="margin-top:2px;font-size:12px;">创建: '+new Date(s.created_ms||0).toLocaleString()+' ｜ 操作员: '+esc(s.created_by_operator||"-")+'</div>' +
        (s.closed_ms ? '<div class="muted" style="font-size:12px;">关闭: '+new Date(s.closed_ms).toLocaleString()+'</div>' : '') +
        (s.active && s.active.length>0
          ? '<div class="muted" style="margin-top:4px;">在岗('+s.active.length+'): '+activeList+'</div>'
          : '') +
        '<div>' + forceEndBtn + viewEventsBtn + '</div>' +
      '</div>'
    );
  }).join("");
}

// ===== 查看/修正 Session 事件 =====
var _gsEventsData = [];

async function gsViewEvents(session){
  _globalSessionsViewSession = session;
  var detailEl = document.getElementById("globalSessionsDetail");
  var eventsEl = document.getElementById("globalSessionsEvents");
  var titleEl = document.getElementById("globalSessionsTitle");
  if(detailEl) detailEl.style.display = "none";
  if(!eventsEl) return;
  eventsEl.style.display = "";
  eventsEl.innerHTML = '<div class="muted">加载事件中...</div>';
  if(titleEl) titleEl.textContent = "事件详情";

  try{
    var res = await fetchApi({ action:"admin_session_events", k:adminKey_(), session:session });
    if(!res || res.ok !== true){
      eventsEl.innerHTML = '<div class="muted">加载失败: ' + esc(res && res.error ? res.error : "unknown") + '</div>';
      return;
    }
    _gsEventsData = res.events || [];
    gsRenderEvents_(session);
  }catch(e){
    eventsEl.innerHTML = '<div class="muted">加载异常: ' + esc(String(e)) + '</div>';
  }
}

function gsRenderEvents_(session){
  var el = document.getElementById("globalSessionsEvents");
  if(!el) return;

  var html = '<div style="font-weight:700;margin-bottom:8px;">Session: ' + esc(session) + ' (' + _gsEventsData.length + '条事件)</div>';

  if(_gsEventsData.length === 0){
    html += '<div class="muted">该 Session 无事件记录</div>';
    el.innerHTML = html;
    return;
  }

  // 用卡片式展示每条事件，更清晰也方便编辑
  _gsEventsData.forEach(function(ev, idx){
    var evColor = ev.event === "join" ? "#27ae60" : ev.event === "leave" ? "#e74c3c" : ev.event === "start" ? "#3498db" : ev.event === "end" ? "#8e44ad" : "#666";
    var dimStyle = Number(ev.ok) === 0 ? 'opacity:0.5;' : '';
    html += '<div style="border:1px solid #eee;border-radius:10px;padding:10px;margin:6px 0;font-size:13px;' + dimStyle + '">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center;">';
    html += '<span style="color:' + evColor + ';font-weight:700;font-size:14px;">' + esc(ev.event) + '</span>';
    html += '<span class="muted" style="font-size:11px;">' + esc(corrFmtKst_(ev.server_ms)) + '</span>';
    html += '</div>';
    html += '<div style="margin-top:4px;">';
    html += '<b>工牌:</b> ' + esc(badgeName_(ev.badge) || ev.badge || "-");
    html += ' &nbsp; <b>任务:</b> ' + esc((ev.biz||"") + "/" + (ev.task||""));
    html += '</div>';
    if(ev.wave_id){
      html += '<div style="margin-top:2px;"><b>单号:</b> ' + esc(ev.wave_id) + '</div>';
    }
    if(ev.note){
      html += '<div style="margin-top:2px;color:#888;"><b>备注:</b> ' + esc(ev.note) + '</div>';
    }
    if(Number(ev.ok) === 0){
      html += '<div style="margin-top:2px;color:#999;font-size:11px;">(blocked)</div>';
    }
    html += '<div style="margin-top:6px;display:flex;gap:4px;flex-wrap:wrap;">';
    if(Number(ev.ok) !== 0){
      html += '<button style="width:auto;font-size:11px;padding:3px 10px;" onclick="gsEditField('+idx+',\'time\')">改时间</button>';
      html += '<button style="width:auto;font-size:11px;padding:3px 10px;" onclick="gsEditField('+idx+',\'wave_id\')">改单号</button>';
      html += '<button style="width:auto;font-size:11px;padding:3px 10px;" onclick="gsEditField('+idx+',\'note\')">改备注</button>';
      html += '<button style="width:auto;font-size:11px;padding:3px 10px;" onclick="gsEditField('+idx+',\'badge\')">改工牌</button>';
    }
    html += '<button style="width:auto;font-size:11px;padding:3px 10px;background:#e74c3c;color:#fff;border-color:#e74c3c;" onclick="gsDeleteEvent('+idx+')">删除</button>';
    html += '</div></div>';
  });
  el.innerHTML = html;
}

async function gsEditField(idx, field){
  var ev = _gsEventsData[idx];
  if(!ev) return;

  var params = { action:"admin_event_update", k:adminKey_(), event_id:ev.event_id };
  var label, current, input;

  if(field === "time"){
    label = "时间(KST)";
    current = corrFmtKst_(ev.server_ms);
    input = prompt("修改" + label + "\n事件: " + ev.event + " | " + (badgeName_(ev.badge)||ev.badge) + "\n\n格式: YYYY-MM-DD HH:MM", current);
    if(!input) return;
    var cleaned = input.trim().replace(/\s+/g, "T");
    var newMs = corrKstToMs_(cleaned.substring(0,16));
    if(!newMs){ alert("时间格式错误"); return; }
    params.new_ms = newMs;
    if(!confirm("确认修改时间？\n原: " + current + "\n新: " + corrFmtKst_(newMs))) return;
  } else if(field === "wave_id"){
    label = "单号";
    current = ev.wave_id || "";
    input = prompt("修改" + label + "\n事件: " + ev.event + " | " + (badgeName_(ev.badge)||ev.badge) + "\n\n当前: " + (current||"(空)"), current);
    if(input === null) return;
    params.new_wave_id = input.trim();
    if(!confirm("确认修改单号？\n原: " + (current||"(空)") + "\n新: " + (params.new_wave_id||"(空)"))) return;
  } else if(field === "note"){
    label = "备注";
    current = ev.note || "";
    input = prompt("修改" + label + "\n事件: " + ev.event + " | " + (badgeName_(ev.badge)||ev.badge) + "\n\n当前: " + (current||"(空)"), current);
    if(input === null) return;
    params.new_note = input.trim();
    if(!confirm("确认修改备注？\n原: " + (current||"(空)") + "\n新: " + (params.new_note||"(空)"))) return;
  } else if(field === "badge"){
    label = "工牌";
    current = ev.badge || "";
    input = prompt("修改" + label + "\n事件: " + ev.event + " | 时间: " + corrFmtKst_(ev.server_ms) + "\n\n当前: " + current, current);
    if(!input) return;
    params.new_badge = input.trim();
    if(!params.new_badge){ alert("工牌不能为空"); return; }
    if(!confirm("确认修改工牌？\n原: " + current + "\n新: " + params.new_badge)) return;
  } else {
    return;
  }

  try{
    var res = await fetchApi(params);
    if(!res || res.ok !== true){ alert("修改失败: " + (res&&res.error||"unknown")); return; }
    alert("修改成功");
    gsViewEvents(ev.session);
  }catch(e){ alert("异常: "+e); }
}

async function gsDeleteEvent(idx){
  var ev = _gsEventsData[idx];
  if(!ev) return;
  if(!confirm("确认删除？\n\n事件: " + ev.event + " | " + (badgeName_(ev.badge)||ev.badge) + "\n时间: " + corrFmtKst_(ev.server_ms) + "\n\n删除后不可恢复！")) return;
  try{
    var res = await fetchApi({ action:"admin_event_delete", k:adminKey_(), event_id:ev.event_id });
    if(!res || res.ok !== true){ alert("删除失败: " + (res&&res.error||"unknown")); return; }
    alert("已删除");
    gsViewEvents(ev.session);
  }catch(e){ alert("异常: "+e); }
}

function globalSessionsBack(){
  if(_globalSessionsViewSession){
    _globalSessionsViewSession = null;
    var eventsEl = document.getElementById("globalSessionsEvents");
    if(eventsEl) eventsEl.style.display = "none";
    renderGlobalSessionsDetail_(_globalSessionsFilter);
  } else if(_globalSessionsFilter){
    renderGlobalSessionsIndex_();
  } else {
    back();
  }
}

async function adminForceEndSession(btn){
  if(!adminIsUnlocked_()){ alert("请先解锁管理员模式"); return; }
  var session = btn.getAttribute("data-session") || "";
  if(!session){ alert("session 参数缺失"); return; }
  var ok = confirm("强制结束Session / 강제 종료\n\nSession：" + session + "\n\n确定要强制结束吗？\n정말 강제 종료하시겠습니까?");
  if(!ok) return;
  try{
    setStatus("强制结束中... ⏳", true);
    var res = await fetchApi({ action:"admin_force_end_session", k:adminKey_(), session:session });
    if(!res || res.ok !== true){
      setStatus("强制结束失败 ❌ " + (res && res.error ? res.error : ""), false);
      alert("强制结束失败：" + (res && res.error ? res.error : "unknown"));
      return;
    }
    setStatus("强制结束成功 ✅", true);
    await refreshGlobalSessions();
  }catch(e){
    setStatus("强制结束异常 ❌ " + e, false);
    alert("强制结束异常：" + e);
  }
}

/** ===== Start / End: Generic + Task-specific ===== */
async function startGeneric_(e, biz, task, page, resetFn, postRenderFn){
  if(!acquireBusy_()) return;
  if(currentSessionId){
    var ok = confirm("当前已有进行中的趟次：" + currentSessionId + "\n\n确定要放弃当前趟次、重新开始一个新趟次吗？\n（一般请取消，继续当前趟次。）");
    if(!ok){ releaseBusy_(); return; }
    // 尝试关闭旧 session，避免孤儿 session
    var closeRes;
    try{ closeRes = await sessionCloseServer_(); }catch(e2){ closeRes = {}; }
    if(closeRes.blocked){
      if(closeRes.reason === "pending_b2b_results"){
        var orders = (closeRes.pending_orders || []).map(function(o){
          var st = o.result_status === "missing" ? "未录入" : (o.result_status === "draft" ? "草稿未提交" : o.result_status);
          return o.source_order_no + " 〔" + st + "〕";
        }).join("\n");
        setStatus("有未完成结果单，无法开始新任务", false);
        var goMsg = "当前还有未完成提交的工单结果单，不能开始新任务。\n\n请先完成以下工单的结果单：\n" + orders +
          "\n\n趟次：" + currentSessionId + "\n\n点【确定】跳转到B2B工单操作页处理。";
        if(confirm(goMsg)){
          setSess_("B2B", "B2B工单操作", currentSessionId);
          go("b2b_workorder");
        }
      } else {
        setStatus("旧趟次未能释放，无法开始新任务", false);
        alert("旧趟次未能释放（" + (closeRes.reason || "未知原因") + "），请先结束当前趟次。");
      }
      return;
    }
    if(closeRes.closed || closeRes.already_closed){
      cleanupLocalSession_();
    } else {
      setStatus("旧趟次状态异常，无法开始新任务", false);
      alert("旧趟次状态异常，请先确认当前作业状态。");
      return;
    }
  }
  var btn = e && e.target ? e.target : null;
  var origText = btn ? btn.textContent : "";
  if(btn){ btn.disabled = true; btn.textContent = "处理中..."; }

  try{
    var newSid = makePickSessionId();
    var evId = makeEventId({ event:"start", biz:biz, task:task, wave_id:"", badgeRaw:"" });
    await submitEventSync_({ event:"start", event_id: evId, biz:biz, task:task, pick_session_id: newSid }, true);
    addRecent(evId);

    currentSessionId = newSid;
    CUR_CTX = { biz: biz, task: task, page: page };
    setSess_(biz, task, newSid);
    if(resetFn) resetFn();
    persistState();
    refreshUI();
    renderActiveLists();
    if(postRenderFn) postRenderFn();
    setStatus(task + "开始 ✅ 新趟次: " + newSid, true);
    fetchOperatorOpenSessions();
  }catch(err){
    setStatus(task + "开始失败 ❌ " + err, false);
    alert(String(err));
  }finally{
    if(btn){ btn.disabled = false; btn.textContent = origText; }
    releaseBusy_();
  }
}

function startTally(e){ startGeneric_(e, "B2C", "理货", "b2c_tally", function(){ scannedInbounds = new Set(); activeTally = new Set(); }, renderInboundCountUI); }
async function endTally(){ if(!acquireBusy_()) return; try{ await endSessionGlobal_(); }finally{ releaseBusy_(); } }

function startBulkOut(e){ startGeneric_(e, "B2C", "批量出库", "b2c_bulkout", function(){ scannedBulkOutOrders = new Set(); activeBulkOut = new Set(); }, renderBulkOutUI); }
async function endBulkOut(){ if(!acquireBusy_()) return; try{ await endSessionGlobal_(); }finally{ releaseBusy_(); } }

/** ===== B2B Tally (like B2C Tally) ===== */
function startB2bTally(e){ startGeneric_(e, "B2B", "B2B入库理货", "b2b_tally", function(){ scannedB2bTallyOrders = new Set(); activeB2bTally = new Set(); }, renderB2bTallyUI); }
async function endB2bTally(){ if(!acquireBusy_()) return; try{ await endSessionGlobal_(); }finally{ releaseBusy_(); } }

async function openScannerB2bTallyOrder(){
  if(!currentSessionId){ setStatus("请先开始理货", false); return; }
  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再扫码，请重新开始。"))) return;
  scanMode = "b2b_tally_order";
  document.getElementById("scanTitle").textContent = "扫码理货单号（B2B）";
  await openScannerCommon();
}

function manualAddB2bTallyOrder(){
  var inp = document.getElementById("b2bTallyManualInput");
  if(!inp) return;
  var val = String(inp.value || "").trim();
  if(!val){ alert("请输入理货单号"); return; }
  if(!currentSessionId){ alert("请先点【开始理货】"); return; }
  if(scannedB2bTallyOrders.has(val)){
    alert("已记录（去重）✅\n" + val);
    renderB2bTallyUI(); inp.value = ""; return;
  }
  scannedB2bTallyOrders.add(val);
  persistState(); renderB2bTallyUI();
  var evId = makeEventId({ event:"wave", biz:"B2B", task:"B2B入库理货", wave_id: val, badgeRaw:"" });
  if(!hasRecent(evId)){
    submitEvent({ event:"wave", event_id: evId, biz:"B2B", task:"B2B入库理货", pick_session_id: currentSessionId, wave_id: val });
    addRecent(evId);
  }
  setStatus("已记录理货单（待上传）✅ " + val, true);
  inp.value = "";
}

function renderB2bTallyUI(){
  var c = document.getElementById("b2bTallyOrderCount");
  var l = document.getElementById("b2bTallyOrderList");
  if(c) c.textContent = String(scannedB2bTallyOrders.size);
  if(l){
    if(scannedB2bTallyOrders.size === 0){
      l.innerHTML = '<span class="muted">无 / 없음</span>';
    }else{
      var arr = Array.from(scannedB2bTallyOrders);
      var show = arr.slice(Math.max(0, arr.length - 30));
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+esc(String(x))+'</span>'; }).join(" ");
    }
  }
}

/** ===== B2B Workorder (like B2C BulkOut) ===== */
function startB2bWorkorder(e){ startGeneric_(e, "B2B", "B2B工单操作", "b2b_workorder", function(){ scannedB2bWorkorders = new Set(); activeB2bWorkorder = new Set(); b2bWorkorderBindings = {}; _b2bBindingsLoaded = false; _b2bSelfHealPending = false; b2bWorkorderResults = {}; _sessionOwnerInfo = null; }, renderB2bWorkorderUI); }
async function endB2bWorkorder(){ if(!acquireBusy_()) return; try{ await endSessionGlobal_(); }finally{ releaseBusy_(); } }

function callB2bOpBind(orderNo){
  if(!currentSessionId) return;
  jsonp(LOCK_URL, {
    action:"b2b_op_bind",
    session_id: currentSessionId,
    badge: getOperatorId() || "",
    bound_task: "B2B工单操作",
    source_order_no: orderNo
  }, { skipBusy: true }).then(function(res){
    if(!res || !res.ok){
      // 服务端失败：回滚本地去重，允许重试
      scannedB2bWorkorders.delete(orderNo);
      delete b2bWorkorderBindings[orderNo];
      persistState(); renderB2bWorkorderUI();
      setStatus("⚠️ 绑定失败，请重试: " + orderNo, false);
      return;
    }
    if(res.duplicate){
      b2bWorkorderBindings[orderNo] = { source_type: res.source_type, match_status: res.match_status, day_kst: res.day_kst || "", internal_workorder_id: res.internal_workorder_id || null, wo_summary: res.wo_summary || null };
      showScanFeedback_("该工单已绑定: " + orderNo, "#fffbe6", "#b8860b", 1500);
    } else {
      b2bWorkorderBindings[orderNo] = { source_type: res.source_type, match_status: res.match_status, day_kst: res.day_kst || "", internal_workorder_id: res.internal_workorder_id || null, wo_summary: res.wo_summary || null };
      if(res.source_type === "internal_b2b_workorder"){
        var qt = (res.wo_summary && res.wo_summary.qty_text) ? " · " + res.wo_summary.qty_text : "";
        showScanFeedback_("已绑定本系统工单 " + orderNo + qt, "#e6ffe6", "#006400", 2000);
      } else {
        showScanFeedback_("已记录外部工单 " + orderNo + "，待WMS匹配", "#fff3e0", "#e65100", 2000);
      }
    }
    renderB2bWorkorderUI();
  }).catch(function(e){
    console.error("b2b_op_bind error", e);
    // 网络失败：回滚本地去重，允许重试
    scannedB2bWorkorders.delete(orderNo);
    delete b2bWorkorderBindings[orderNo];
    persistState(); renderB2bWorkorderUI();
    setStatus("⚠️ 绑定失败（网络错误），请重试: " + orderNo, false);
  });
}

function loadB2bBindings(onDone){
  if(!currentSessionId){ if(typeof onDone === "function") onDone(); return; }
  var reqSid = currentSessionId; // 捕获发起时的 session，用于回调校验
  jsonp(LOCK_URL, { action:"b2b_op_bind_list", session_id: reqSid }).then(function(res){
    if(currentSessionId !== reqSid){ if(typeof onDone === "function") onDone(); return; } // session 已切换，丢弃旧结果
    if(!res || !res.ok){ _b2bBindingsLoaded = true; _b2bSelfHealPending = false; if(typeof onDone === "function") onDone(); return; }
    var bindings = res.bindings || [];
    // 完全以服务端为准，清空本地残留，防止幽灵工单
    scannedB2bWorkorders = new Set();
    b2bWorkorderBindings = {};
    for(var i=0; i<bindings.length; i++){
      var b = bindings[i];
      b2bWorkorderBindings[b.source_order_no] = {
        source_type: b.source_type, match_status: b.match_status,
        day_kst: b.day_kst || "", internal_workorder_id: b.internal_workorder_id || null,
        wo_summary: b.wo_summary || null
      };
      scannedB2bWorkorders.add(b.source_order_no);
    }
    _b2bBindingsLoaded = true;
    _b2bSelfHealPending = false;
    persistState();
    renderB2bWorkorderUI();
    if(typeof onDone === "function") onDone();
  }).catch(function(e){ console.error("b2b_op_bind_list error", e); if(currentSessionId === reqSid){ _b2bBindingsLoaded = true; _b2bSelfHealPending = false; } if(typeof onDone === "function") onDone(); });
}

// ===== B2B 现场结果单 =====
function b2bResultKey_(dayKst, sourceType, orderNo){ return dayKst + "||" + sourceType + "||" + orderNo; }
function b2bResultKeyFromBinding_(orderNo){
  var b = b2bWorkorderBindings[orderNo];
  return b ? b2bResultKey_(b.day_kst, b.source_type, orderNo) : null;
}
var B2B_OP_MODE_LABELS = { pack_outbound:"打包出库", move_and_palletize:"纯搬箱打托" };

function fmtResultSummary(r){
  if(!r) return "";
  var parts = [B2B_OP_MODE_LABELS[r.operation_mode] || r.operation_mode];

  if(r.operation_mode === "pack_outbound"){
    if(r.sku_kind_count) parts.push(r.sku_kind_count + "品");
    if(r.packed_qty) parts.push(r.packed_qty + "件");
    if(r.box_count) parts.push(r.box_count + "箱");
    if(r.pallet_count) parts.push(r.pallet_count + "托");
    if(r.used_carton){
      var ct = ["用纸箱"];
      if(r.big_carton_count) ct.push("大"+r.big_carton_count);
      if(r.small_carton_count) ct.push("小"+r.small_carton_count);
      parts.push(ct.join(" · "));
    }
    if(r.label_count) parts.push("贴标"+r.label_count);
    if(r.photo_count) parts.push("拍照"+r.photo_count);
    if(r.has_pallet_detail) parts.push("含打托明细");
  } else {
    // move_and_palletize
    if(r.box_count) parts.push(r.box_count + "箱");
    if(r.pallet_count) parts.push(r.pallet_count + "托");
    if(r.did_pack) parts.push("打包"+( r.packed_box_count || 0)+"箱");
    if(r.did_rebox) parts.push("换箱"+(r.rebox_count || 0)+"箱");
    if(r.label_count) parts.push("贴标"+r.label_count);
  }

  if(r.needs_forklift_pick){
    var fp = ["含叉车找货"];
    if(r.forklift_pallet_count) fp.push(r.forklift_pallet_count + "托");
    if(r.rack_pick_location_count) fp.push(r.rack_pick_location_count + "货位");
    parts.push(fp.join(" · "));
  }
  return parts.join(" · ");
}

function loadB2bResults(){
  if(!currentSessionId) return;
  jsonp(LOCK_URL, { action:"b2b_op_result_list_by_session", session_id: currentSessionId }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){ console.error("b2b_op_result_list_by_session error", res && res.error); return; }
    b2bWorkorderResults = {};
    (res.results || []).forEach(function(r){
      var rk = b2bResultKey_(r.day_kst, r.source_type, r.source_order_no);
      b2bWorkorderResults[rk] = r;
    });
    renderB2bWorkorderUI();
  }).catch(function(e){ console.error("b2b_op_result_list_by_session error", e); });
}

async function tryRecoverB2bSession_(){
  if(currentSessionId) return;
  if(getHashPage() !== "b2b_workorder") return;
  var op = getOperatorId();
  if(!op) return;
  try{
    var res = await jsonp(LOCK_URL, { action:"operator_open_sessions", operator_id: op }, { skipBusy: true });
    if(!res || !res.ok || !res.sessions) return;
    var found = null;
    for(var i = 0; i < res.sessions.length; i++){
      if(res.sessions[i].biz === "B2B" && res.sessions[i].task === "B2B工单操作"){
        found = res.sessions[i]; break;
      }
    }
    if(!found) return;
    // 自动恢复
    currentSessionId = found.session;
    CUR_CTX = { biz: "B2B", task: "B2B工单操作", page: "b2b_workorder" };
    setSess_("B2B", "B2B工单操作", found.session);
    SESSION_INFO_CACHE = { sid: null, ts: 0, data: null };
    restoreState();
    syncActiveFromServer_();
    loadB2bBindings();
    loadB2bResults();
    renderActiveLists();
    renderB2bWorkorderUI();
    refreshUI();
    setStatus("已自动恢复未结束的作业趟次 ✅ " + found.session, false);
  }catch(e){ /* silent */ }
}

function openResultForm(orderNo, seedData){
  var binding = b2bWorkorderBindings[orderNo];
  if(!binding || !binding.day_kst || !binding.source_type){
    alert("绑定信息缺失，请刷新后重试");
    return;
  }
  var kstDay = binding.day_kst;
  var modal = document.getElementById("b2bResultModal");
  var body = document.getElementById("b2bResultBody");
  if(!modal || !body) return;
  modal.style.display = "flex";
  body.innerHTML = '<div class="muted">加载中...</div>';

  jsonp(LOCK_URL, { action:"b2b_op_result_get", day_kst: kstDay, source_order_no: orderNo }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){ body.innerHTML = '<div style="color:red;">'+esc(res&&res.error||"加载失败")+'</div>'; return; }
    var serverData = res.result || {};
    // seedData 优先（从确认层取消返回时恢复本地未保存输入）
    var r = seedData ? _mergeResultSeed(serverData, seedData) : serverData;
    var cust = res.customer_name || r.customer_name || "";
    var pt = res.participation || {};
    var om = r.operation_mode || "pack_outbound";

    var isCrossDay = kstDay !== kstDayKey_(Date.now());
    var html = '<div style="font-size:16px;font-weight:800;margin-bottom:6px;">现场结果单</div>';
    html += '<div style="font-size:13px;color:#555;margin-bottom:4px;">工单: <b>'+esc(orderNo)+'</b>'+(cust ? ' · '+esc(cust) : '')+'</div>';
    if(isCrossDay){
      html += '<div style="background:#fff3e0;color:#e65100;padding:4px 8px;border-radius:4px;font-size:12px;font-weight:700;margin-bottom:6px;">绑定日: '+esc(kstDay)+'（非今天，跨天旧 session）</div>';
    }
    html += '<div style="font-size:12px;color:#999;margin-bottom:10px;padding:6px 8px;background:#f5f5f5;border-radius:6px;">' +
      '工单共用结果单 · 参与 session: '+pt.session_count+' · 参与人: '+pt.badge_count +
      (pt.badges && pt.badges.length > 0 ? '<br>'+pt.badges.map(function(b){ var p=b.split("|"); return esc(p[1]||p[0]); }).join(", ") : '') +
    '</div>';
    if(r.status === "completed"){
      html += '<div style="color:#2e7d32;font-weight:700;margin-bottom:8px;">✅ 已完成提交';
      if(r.confirm_badge) html += ' · 工牌: '+esc(r.confirm_badge);
      else if(r.confirmed_by) html += ' · 确认人: '+esc(r.confirmed_by);
      html += '</div>';
      html += '<div style="margin-bottom:10px;"><button onclick="revertResultToDraft(\''+esc(orderNo)+'\')" style="font-size:12px;padding:4px 12px;background:#ff9800;color:#fff;border:none;border-radius:4px;cursor:pointer;">↩ 退回草稿</button></div>';
    }

    html += '<div style="margin-bottom:8px;"><label style="font-size:13px;font-weight:700;">作业类型</label>' +
      '<select id="rf-mode" style="width:100%;margin-top:2px;" onchange="rfToggleMode()">' +
        '<option value="pack_outbound"'+(om==="pack_outbound"?' selected':'')+'>打包出库</option>' +
        '<option value="move_and_palletize"'+(om==="move_and_palletize"?' selected':'')+'>纯搬箱打托</option>' +
      '</select></div>';

    // === 打包出库 字段区 ===
    html += '<div id="rf-pack-area" style="'+(om==="move_and_palletize"?"display:none;":"")+'">';
    html += _rfField("品项数", "rf-sku", "number", r.sku_kind_count, "0", "1");
    html += _rfField("打包件数", "rf-packed-qty", "number", r.packed_qty, "0", "1");
    html += _rfField("总出库箱数", "rf-box", "number", r.box_count, "0", "0.5");
    html += _rfField("打包箱数", "rf-packed-box", "number", r.packed_box_count, "0", "0.5");
    html += _rfSwitch("rf-carton", "是否用了纸箱", r.used_carton, "rfToggleCarton()");
    html += '<div id="rf-carton-fields" style="'+(r.used_carton?"":"display:none;")+'padding-left:12px;border-left:3px solid #1976d2;margin-bottom:8px;">' +
      _rfField("大箱数", "rf-big-carton", "number", r.big_carton_count, "0", "1") +
      _rfField("小箱数", "rf-small-carton", "number", r.small_carton_count, "0", "1") +
    '</div>';
    html += _rfField("出库托数", "rf-pallet", "number", r.pallet_count, "0", "0.5");
    html += _rfField("贴标数量", "rf-label", "number", r.label_count, "0", "1");
    html += _rfField("拍照数量", "rf-photo", "number", r.photo_count, "0", "1");
    html += _rfSwitch("rf-pallet-detail", "是否制作了打托明细", r.has_pallet_detail);
    html += '</div>';

    // === 搬箱打托 字段区 ===
    html += '<div id="rf-move-area" style="'+(om==="pack_outbound"?"display:none;":"")+'">';
    html += _rfSwitch("rf-did-pack", "是否打包", r.did_pack, "rfToggleDidPack()");
    html += '<div id="rf-did-pack-fields" style="'+(r.did_pack?"":"display:none;")+'padding-left:12px;border-left:3px solid #1976d2;margin-bottom:8px;">' +
      _rfField("打包箱数", "rf-move-packed-box", "number", r.packed_box_count, "0", "0.5") +
    '</div>';
    html += _rfSwitch("rf-did-rebox", "是否换箱", r.did_rebox, "rfToggleDidRebox()");
    html += '<div id="rf-did-rebox-fields" style="'+(r.did_rebox?"":"display:none;")+'padding-left:12px;border-left:3px solid #1976d2;margin-bottom:8px;">' +
      _rfField("换箱数", "rf-rebox-count", "number", r.rebox_count, "0", "1") +
    '</div>';
    html += _rfField("贴标数量", "rf-move-label", "number", r.label_count, "0", "1");
    html += _rfField("出库箱数", "rf-move-box", "number", r.box_count, "0", "0.5");
    html += _rfField("出库托数", "rf-move-pallet", "number", r.pallet_count, "0", "0.5");
    html += '</div>';

    // === 公共区：叉车找货 + 备注 ===
    var nfp = r.needs_forklift_pick ? 1 : 0;
    html += _rfSwitch("rf-fork", "需要叉车找货", nfp, "rfToggleFork()");
    html += '<div id="rf-fork-fields" style="'+(nfp?"":"display:none;")+'padding-left:12px;border-left:3px solid #ff9800;margin-bottom:8px;">' +
      _rfField("叉车取货托数", "rf-fork-pallet", "number", r.forklift_pallet_count, "0", "0.5") +
      _rfField("涉及货位数", "rf-fork-loc", "number", r.rack_pick_location_count, "0", "1") +
    '</div>';

    html += '<div style="margin-bottom:10px;"><label style="font-size:13px;font-weight:700;">备注</label>' +
      '<textarea id="rf-remark" rows="2" style="width:100%;margin-top:2px;">'+ esc(r.remark||"") +'</textarea></div>';

    html += '<input type="hidden" id="rf-order-no" value="'+esc(orderNo)+'" />';
    html += '<input type="hidden" id="rf-day-kst" value="'+esc(kstDay)+'" />';
    html += '<div style="display:flex;gap:8px;">' +
      '<button onclick="submitResult(\'draft\')" style="flex:1;padding:10px;font-size:14px;">保存草稿</button>' +
      '<button onclick="submitResult(\'completed\')" style="flex:1;padding:10px;font-size:14px;background:#2e7d32;color:#fff;border:none;border-radius:6px;">完成提交</button>' +
    '</div>';

    body.innerHTML = html;
  }).catch(function(e){ body.innerHTML = '<div style="color:red;">网络错误</div>'; });
}

// --- seedData 合并：用 payload 字段覆盖服务端数据，保留服务端的 status/confirm 等元数据 ---
function _mergeResultSeed(server, seed){
  var r = {};
  for(var k in server) r[k] = server[k];
  // 用 seed payload 覆盖所有表单字段
  var fields = ["operation_mode","sku_kind_count","packed_qty","box_count","packed_box_count",
    "used_carton","big_carton_count","small_carton_count","pallet_count","label_count","photo_count",
    "has_pallet_detail","did_pack","did_rebox","rebox_count",
    "needs_forklift_pick","forklift_pallet_count","rack_pick_location_count","remark"];
  for(var i=0;i<fields.length;i++){
    var f = fields[i];
    if(seed[f] !== undefined) r[f] = seed[f];
  }
  return r;
}

// --- 表单构建 helpers ---
function _rfField(label, id, type, val, min, step){
  return '<div style="margin-bottom:8px;"><label style="font-size:13px;font-weight:700;">'+label+'</label>' +
    '<input id="'+id+'" type="'+type+'" min="'+min+'" step="'+step+'" value="'+(val||0)+'" style="width:100%;margin-top:2px;" /></div>';
}
function _rfSwitch(id, label, val, onchange){
  return '<div style="margin-bottom:8px;"><label style="font-size:13px;font-weight:700;"><input id="'+id+'" type="checkbox" '+(val?'checked':'')+
    (onchange ? ' onchange="'+onchange+'"' : '') + ' /> '+label+'</label></div>';
}

// --- Toggle functions ---
function rfToggleMode(){
  var m = (document.getElementById("rf-mode") || {}).value;
  var packArea = document.getElementById("rf-pack-area");
  var moveArea = document.getElementById("rf-move-area");
  if(packArea) packArea.style.display = m === "move_and_palletize" ? "none" : "";
  if(moveArea) moveArea.style.display = m === "pack_outbound" ? "none" : "";
}
function rfToggleFork(){
  var cb = document.getElementById("rf-fork");
  var fields = document.getElementById("rf-fork-fields");
  if(cb && fields){
    fields.style.display = cb.checked ? "" : "none";
    if(!cb.checked){ _rfClear(["rf-fork-pallet","rf-fork-loc"]); }
  }
}
function rfToggleCarton(){
  var cb = document.getElementById("rf-carton");
  var fields = document.getElementById("rf-carton-fields");
  if(cb && fields){
    fields.style.display = cb.checked ? "" : "none";
    if(!cb.checked){ _rfClear(["rf-big-carton","rf-small-carton"]); }
  }
}
function rfToggleDidPack(){
  var cb = document.getElementById("rf-did-pack");
  var fields = document.getElementById("rf-did-pack-fields");
  if(cb && fields){
    fields.style.display = cb.checked ? "" : "none";
    if(!cb.checked){ _rfClear(["rf-move-packed-box"]); }
  }
}
function rfToggleDidRebox(){
  var cb = document.getElementById("rf-did-rebox");
  var fields = document.getElementById("rf-did-rebox-fields");
  if(cb && fields){
    fields.style.display = cb.checked ? "" : "none";
    if(!cb.checked){ _rfClear(["rf-rebox-count"]); }
  }
}
function _rfClear(ids){
  for(var i=0;i<ids.length;i++){ var el=document.getElementById(ids[i]); if(el) el.value="0"; }
}

function _rfVal(id){ return Number((document.getElementById(id) || {}).value) || 0; }
function _rfChk(id){ return (document.getElementById(id) || {}).checked ? 1 : 0; }

function _collectResultPayload(st, extraFields){
  var orderNo = (document.getElementById("rf-order-no") || {}).value || "";
  if(!orderNo) return null;
  var dayKst = (document.getElementById("rf-day-kst") || {}).value || "";
  if(!dayKst){ alert("绑定日期信息缺失，请关闭后重新打开"); return null; }
  var om = (document.getElementById("rf-mode") || {}).value || "pack_outbound";
  var isPack = om === "pack_outbound";

  // 公共 & 主开关
  var needs_forklift_pick = _rfChk("rf-fork");
  var used_carton = isPack ? _rfChk("rf-carton") : 0;
  var has_pallet_detail = isPack ? _rfChk("rf-pallet-detail") : 0;
  var did_pack = isPack ? 0 : _rfChk("rf-did-pack");
  var did_rebox = isPack ? 0 : _rfChk("rf-did-rebox");

  // 按模式取值，联动归零
  var payload = {
    action: "b2b_op_result_upsert",
    day_kst: dayKst,
    source_order_no: orderNo,
    session_id: currentSessionId || "",
    operation_mode: om,
    sku_kind_count: isPack ? _rfVal("rf-sku") : 0,
    packed_qty: isPack ? _rfVal("rf-packed-qty") : 0,
    box_count: isPack ? _rfVal("rf-box") : _rfVal("rf-move-box"),
    packed_box_count: isPack ? _rfVal("rf-packed-box") : (did_pack ? _rfVal("rf-move-packed-box") : 0),
    used_carton: used_carton,
    big_carton_count: used_carton ? _rfVal("rf-big-carton") : 0,
    small_carton_count: used_carton ? _rfVal("rf-small-carton") : 0,
    pallet_count: isPack ? _rfVal("rf-pallet") : _rfVal("rf-move-pallet"),
    label_count: isPack ? _rfVal("rf-label") : _rfVal("rf-move-label"),
    photo_count: isPack ? _rfVal("rf-photo") : 0,
    has_pallet_detail: has_pallet_detail,
    did_pack: did_pack,
    did_rebox: did_rebox,
    rebox_count: did_rebox ? _rfVal("rf-rebox-count") : 0,
    needs_forklift_pick: needs_forklift_pick,
    forklift_pallet_count: needs_forklift_pick ? _rfVal("rf-fork-pallet") : 0,
    rack_pick_location_count: needs_forklift_pick ? _rfVal("rf-fork-loc") : 0,
    remark: (document.getElementById("rf-remark") || {}).value || "",
    status: st,
    created_by: getOperatorId() || "",
    confirmed_by: "",
    confirm_badge: ""
  };
  if(extraFields) for(var k in extraFields) payload[k] = extraFields[k];
  return payload;
}

function _doSubmitResult(payload){
  // 简化模式：注入 result_entered_by + workflow_status
  if(_smIsSimpleMode && getHashPage() === "b2b_workorder_simple"){
    var opId = getOperatorId();
    var opP = parseBadge(opId);
    if(!payload.result_entered_by_badge) payload.result_entered_by_badge = opP.id || opId;
    if(!payload.result_entered_by_name) payload.result_entered_by_name = opP.name || "";
    if(payload.status === "draft" && !payload.workflow_status) payload.workflow_status = "pending_review";
  }
  var st = payload.status;
  jsonp(LOCK_URL, payload, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){ alert("保存失败: " + (res&&res.error||"unknown")); return; }
    closeResultForm();
    loadB2bResults();
    setStatus(st === "completed" ? "结果已提交 ✅" : (_smIsSimpleMode ? smt_("result_saved") + " ✅" : "草稿已保存 ✅"), true);
    if(_smIsSimpleMode) smRender_();
  }).catch(function(){ alert("网络错误，请重试"); });
}

function submitResult(st){
  // 简化模式：结果单只保存为 draft (pending_review)，completed 由确认工牌流程走
  if(_smIsSimpleMode && getHashPage() === "b2b_workorder_simple"){
    if(st === "completed"){
      // 简化模式下不允许直接 complete，引导用户通过"结束工单"按钮
      if(!confirm("⚠️ 确认完成提交？\n\n完成后该结果将进入正式统计。")) return;
      _showBadgeConfirmLayer();
      return;
    }
    var payload = _collectResultPayload("draft");
    if(!payload) return;
    payload.workflow_status = "pending_review";
    _doSubmitResult(payload);
    return;
  }
  if(st === "completed"){
    // 第一步：强确认
    if(!confirm("⚠️ 确认完成提交？\n\n完成后该结果将进入正式统计，请确认数据无误。")) return;
    // 第二步：弹出扫工牌确认层
    _showBadgeConfirmLayer();
    return;
  }
  // 草稿直接保存
  var payload = _collectResultPayload("draft");
  if(!payload) return;
  _doSubmitResult(payload);
}

function _showBadgeConfirmLayer(){
  var body = document.getElementById("b2bResultBody");
  if(!body) return;
  var orderNo = (document.getElementById("rf-order-no") || {}).value || "";
  // 保存当前表单数据到临时变量，因为 body 即将被替换
  var tmpPayload = _collectResultPayload("completed");
  if(!tmpPayload) return;
  window._rfPendingPayload = tmpPayload;
  window._rfConfirmBadgeRaw = "";

  body.innerHTML = '<div style="text-align:center;padding:20px 0;">' +
    '<div style="font-size:18px;font-weight:800;margin-bottom:12px;">🔒 扫职员工牌确认完成</div>' +
    '<div style="font-size:13px;color:#555;margin-bottom:12px;">工单: <b>'+esc(orderNo)+'</b></div>' +
    '<div style="font-size:13px;color:#c00;margin-bottom:16px;">必须扫描职员工牌（EMP-...），不支持手动输入</div>' +
    '<input id="rf-confirm-badge" type="text" readonly placeholder="等待扫描职员工牌..." ' +
      'style="width:90%;font-size:18px;padding:12px;text-align:center;border:2px solid #2e7d32;border-radius:8px;margin-bottom:12px;background:#f5f5f5;" />' +
    '<button onclick="_openBadgeScanner()" style="width:90%;padding:12px;font-size:15px;background:#1565c0;color:#fff;border:none;border-radius:8px;margin-bottom:12px;cursor:pointer;">📷 开始扫描职员工牌</button>' +
    '<div style="display:flex;gap:8px;justify-content:center;">' +
      '<button onclick="_cancelBadgeConfirm()" style="flex:1;padding:10px;font-size:14px;">取消</button>' +
      '<button onclick="_doBadgeConfirmSubmit()" style="flex:1;padding:10px;font-size:14px;background:#2e7d32;color:#fff;border:none;border-radius:6px;">确认完成</button>' +
    '</div>' +
  '</div>';
}

async function _openBadgeScanner(){
  scanMode = "b2b_result_confirm_badge";
  await openScannerCommon();
}

function _cancelBadgeConfirm(){
  var pending = window._rfPendingPayload;
  var orderNo = pending ? pending.source_order_no : "";
  window._rfPendingPayload = null;
  window._rfConfirmBadgeRaw = "";
  if(orderNo){
    openResultForm(orderNo, pending);
  } else {
    closeResultForm();
  }
  setStatus("已取消完成提交", true);
}

function _doBadgeConfirmSubmit(){
  var badgeRaw = window._rfConfirmBadgeRaw || "";
  if(!badgeRaw){
    alert("请先点击「开始扫描职员工牌」扫描 EMP 工牌\n\n不支持手动输入");
    return;
  }
  var p = parseBadge(badgeRaw);
  if(!isEmpId(p.id)){
    alert("工牌格式无效，必须是职员工牌（EMP-...）");
    return;
  }
  var payload = window._rfPendingPayload;
  if(!payload){ alert("表单数据丢失，请重新操作"); closeResultForm(); return; }
  payload.confirm_badge = p.id;
  payload.confirmed_by = p.name || p.id;
  window._rfPendingPayload = null;
  window._rfConfirmBadgeRaw = "";
  _doSubmitResult(payload);
}

function revertResultToDraft(orderNo){
  if(!confirm("确认退回草稿？\n将清除完成确认记录，状态改回草稿。")) return;
  var binding = b2bWorkorderBindings[orderNo];
  if(!binding || !binding.day_kst){ alert("绑定信息缺失，请刷新后重试"); return; }
  var kstDay = binding.day_kst;
  jsonp(LOCK_URL, { action:"b2b_op_result_get", day_kst: kstDay, source_order_no: orderNo }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){ alert("加载失败"); return; }
    var r = res.result || {};
    var payload = {
      action: "b2b_op_result_upsert",
      day_kst: kstDay,
      source_order_no: orderNo,
      session_id: currentSessionId || "",
      operation_mode: r.operation_mode || "pack_outbound",
      sku_kind_count: r.sku_kind_count || 0,
      packed_qty: r.packed_qty || 0,
      box_count: r.box_count || 0,
      packed_box_count: r.packed_box_count || 0,
      used_carton: r.used_carton || 0,
      big_carton_count: r.big_carton_count || 0,
      small_carton_count: r.small_carton_count || 0,
      pallet_count: r.pallet_count || 0,
      label_count: r.label_count || 0,
      photo_count: r.photo_count || 0,
      has_pallet_detail: r.has_pallet_detail || 0,
      did_pack: r.did_pack || 0,
      did_rebox: r.did_rebox || 0,
      rebox_count: r.rebox_count || 0,
      needs_forklift_pick: r.needs_forklift_pick || 0,
      forklift_pallet_count: r.forklift_pallet_count || 0,
      rack_pick_location_count: r.rack_pick_location_count || 0,
      remark: r.remark || "",
      status: "draft",
      created_by: r.created_by || "",
      confirmed_by: "",
      confirm_badge: ""
    };
    _doSubmitResult(payload);
  }).catch(function(){ alert("网络错误"); });
}

function unbindB2bWorkorder(orderNo){
  if(!confirm("确认解绑工单 "+orderNo+"？\n\n仅删除当前作业中的绑定关系，不删除原工单。")) return;
  var b = b2bWorkorderBindings[orderNo];
  var srcType = (b && b.source_type) || "internal_b2b_workorder";
  jsonp(LOCK_URL, {
    action: "b2b_op_unbind",
    session_id: currentSessionId || "",
    source_type: srcType,
    source_order_no: orderNo
  }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){ alert("解绑失败: "+(res&&res.error||"unknown")); return; }
    scannedB2bWorkorders.delete(orderNo);
    delete b2bWorkorderBindings[orderNo];
    persistState();
    renderB2bWorkorderUI();
    var msg = "已解绑 "+orderNo;
    if(res.remaining_bindings === 0) msg += "（该工单已无任何绑定）";
    setStatus(msg+" ✅", true);
  }).catch(function(){ alert("网络错误，请重试"); });
}

function closeResultForm(){
  var modal = document.getElementById("b2bResultModal");
  if(modal) modal.style.display = "none";
}

/** ===== B2B Field Op (现场记录) ===== */
var FO_OP_LABELS = { box_op:"箱子操作", palletize:"打托", bulk_in_out:"整进整出", unload:"卸货", other:"其他" };
var _foSelectedRecord = null;  // { record_id, operation_type, customer_name, source_plan_id, plan_day, goods_summary }
var FO_LOCAL_KEY = "b2bFieldOpRecord";
function keyFoRecord(){ return FO_LOCAL_KEY + "_" + (currentSessionId || "NA"); }
function saveFoRecord(r){ localStorage.setItem(keyFoRecord(), JSON.stringify(r)); }
function loadFoRecord(){ try{ return JSON.parse(localStorage.getItem(keyFoRecord())); }catch(e){ return null; } }
function clearFoRecord(){ localStorage.removeItem(keyFoRecord()); }

// 页面进入时渲染
function renderB2bFieldOpUI(){
  var hasSession = !!currentSessionId && CUR_CTX && CUR_CTX.task === "B2B现场记录";
  var selArea = document.getElementById("foSelectionArea");
  var workArea = document.getElementById("foWorkingArea");
  if(!selArea || !workArea) return;

  if(hasSession){
    selArea.style.display = "none";
    workArea.style.display = "";
    // 从本地恢复 FO 信息（先显示本地快照，再异步拉最新）
    var r = _foSelectedRecord || loadFoRecord();
    if(r){
      _foSelectedRecord = r;
      renderFoWorkingCard_(r);
      // 异步从服务端刷新最新数据
      if(r.record_id) refreshFoFromServer_(r.record_id);
    } else {
      document.getElementById("foWorkingId").textContent = "(FO 信息不可用)";
      document.getElementById("foWorkingDetail").innerHTML = "";
    }
  } else {
    selArea.style.display = "";
    workArea.style.display = "none";
    _foSelectedRecord = null;
    loadFoPlans();
  }
}
function renderFoWorkingCard_(r){
  document.getElementById("foWorkingId").textContent = r.record_id + " · " + (FO_OP_LABELS[r.operation_type] || r.operation_type);
  var lines = [];
  if(r.customer_name) lines.push("客户: " + r.customer_name);
  if(r.source_plan_id) lines.push("来源: " + r.source_plan_id);
  if(r.goods_summary) lines.push("货物: " + r.goods_summary);
  // 时间行：建单为主，作业日次要，完成可选
  var timeInfo = [];
  if(r.created_at) timeInfo.push("建单: " + fmtFoTime_(r.created_at));
  if(r.plan_day) timeInfo.push("作业日: " + r.plan_day);
  if(r.completed_at) timeInfo.push("完成: " + fmtFoTime_(r.completed_at));
  // 结果摘要
  var sum = [];
  if(r.sku_kind_count > 0) sum.push(r.sku_kind_count + "品");
  if(r.packed_qty > 0) sum.push(r.packed_qty + "件");
  if(r.output_box_count > 0) sum.push("出" + r.output_box_count + "箱");
  if(r.packed_box_count > 0) sum.push("封" + r.packed_box_count + "箱");
  if(r.output_pallet_count > 0) sum.push(r.output_pallet_count + "托");
  if(r.label_count > 0) sum.push("贴标" + r.label_count);
  if(r.photo_count > 0) sum.push("拍照" + r.photo_count);
  var detailEl = document.getElementById("foWorkingDetail");
  detailEl.innerHTML = esc(lines.join(" · ")) +
    (timeInfo.length ? '<br><span class="muted" style="font-size:11px;">' + esc(timeInfo.join(" · ")) + '</span>' : '') +
    (sum.length ? '<br><span style="font-size:12px;color:#1976d2;font-weight:500;">' + esc(sum.join(" · ")) + '</span>' : '');
}
function fmtFoTime_(t){ if(!t) return ""; try{ return new Date(t).toLocaleString(); }catch(e){ return String(t); } }
function refreshFoFromServer_(recordId){
  jsonp(LOCK_URL, { action:"b2b_field_op_detail", record_id:recordId, k:getFoKey_() }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok || !res.record) return;
    var fresh = res.record;
    _foSelectedRecord = fresh;
    saveFoRecord(fresh);
    renderFoWorkingCard_(fresh);
  }).catch(function(){});
}

// 加载入库计划下拉
function loadFoPlans(){
  var sel = document.getElementById("foPlanSelect");
  if(!sel) return;
  sel.innerHTML = '<option value="">加载中...</option>';
  document.getElementById("foRecordArea").style.display = "none";
  document.getElementById("foConfirmInfo").style.display = "none";
  document.getElementById("foStartBtn").style.display = "none";

  var today = kstToday_();
  var d30ago = new Date(Date.now() + 9*3600*1000 - 30*24*3600*1000);
  var start30 = d30ago.getUTCFullYear() + "-" + pad2_(d30ago.getUTCMonth()+1) + "-" + pad2_(d30ago.getUTCDate());

  jsonp(LOCK_URL, { action:"b2b_plan_list", start_day:start30, end_day:today, k:getFoKey_() }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){
      sel.innerHTML = '<option value="">加载失败</option>';
      return;
    }
    var plans = (res.plans||[]).filter(function(p){ return p.status === "arrived" || p.status === "processing"; });
    if(!plans.length){
      sel.innerHTML = '<option value="">暂无 arrived/processing 的入库计划</option>';
      return;
    }
    // 按日期倒序
    plans.sort(function(a,b){ return a.plan_day > b.plan_day ? -1 : a.plan_day < b.plan_day ? 1 : 0; });
    var html = '<option value="">请选择入库计划...</option>';
    plans.forEach(function(p){
      html += '<option value="'+esc(p.plan_id)+'" data-day="'+esc(p.plan_day)+'" data-customer="'+esc(p.customer_name)+'" data-summary="'+esc(p.goods_summary||"")+'">' +
        esc(p.plan_id) + ' · ' + esc(p.customer_name) + ' · ' + esc(p.plan_day) + ' · ' + esc(p.goods_summary||"") + '</option>';
    });
    sel.innerHTML = html;
  });
}

function getFoKey_(){ try{ return localStorage.getItem("b2b_plan_k_v1")||""; }catch(e){ return ""; } }
function kstToday_(){ var d = new Date(Date.now() + 9*3600*1000); return d.getUTCFullYear() + "-" + pad2_(d.getUTCMonth()+1) + "-" + pad2_(d.getUTCDate()); }
function pad2_(n){ return String(n).padStart(2,"0"); }

// 选择入库计划后，加载该计划下的 FO 记录
function onFoPlanSelected(){
  var sel = document.getElementById("foPlanSelect");
  var planId = sel.value;
  document.getElementById("foConfirmInfo").style.display = "none";
  document.getElementById("foStartBtn").style.display = "none";
  _foSelectedRecord = null;
  if(!planId){
    document.getElementById("foRecordArea").style.display = "none";
    return;
  }
  document.getElementById("foRecordArea").style.display = "";
  var recSel = document.getElementById("foRecordSelect");
  recSel.innerHTML = '<option value="">加载中...</option>';

  var opt = sel.options[sel.selectedIndex];
  var planDay = opt.getAttribute("data-day") || "";
  var customer = opt.getAttribute("data-customer") || "";
  var summary = opt.getAttribute("data-summary") || "";

  // 查 FO 记录：用该计划的 plan_day 范围查，然后 filter source_plan_id
  jsonp(LOCK_URL, { action:"b2b_field_op_list", start_day:"2020-01-01", end_day:"2099-12-31", k:getFoKey_() }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){
      recSel.innerHTML = '<option value="">加载失败</option>';
      return;
    }
    var records = (res.records||[]).filter(function(r){
      return r.source_plan_id === planId && (r.status === "draft" || r.status === "recording");
    });
    var html = '<option value="">请选择 (共'+records.length+'条)...</option>';
    records.forEach(function(r){
      html += '<option value="'+esc(r.record_id)+'" data-json=\''+esc(JSON.stringify(r))+'\'>' +
        esc(r.record_id) + ' · ' + esc(FO_OP_LABELS[r.operation_type]||r.operation_type) + ' · ' + esc(r.status==="draft"?"草稿":"记录中") + '</option>';
    });
    recSel.innerHTML = html;
  });
}

// 选择 FO 记录后，显示确认信息
function onFoRecordSelected(){
  var sel = document.getElementById("foRecordSelect");
  var opt = sel.options[sel.selectedIndex];
  document.getElementById("foConfirmInfo").style.display = "none";
  document.getElementById("foStartBtn").style.display = "none";
  _foSelectedRecord = null;
  if(!sel.value) return;

  try{
    var r = JSON.parse(opt.getAttribute("data-json"));
    _foSelectedRecord = r;
    showFoConfirm(r);
  }catch(e){}
}

function showFoConfirm(r){
  document.getElementById("foConfirmId").textContent = r.record_id + " · " + (FO_OP_LABELS[r.operation_type]||r.operation_type);
  var confirmText = "客户: " + (r.customer_name||"") +
    " | 来源: " + (r.source_plan_id||"独立新建");
  if(r.created_at) confirmText += " | 建单: " + fmtFoTime_(r.created_at);
  if(r.plan_day) confirmText += " | 作业日: " + r.plan_day;
  if(r.goods_summary) confirmText += " | " + r.goods_summary;
  document.getElementById("foConfirmDetail").textContent = confirmText;
  document.getElementById("foConfirmInfo").style.display = "";
  document.getElementById("foStartBtn").style.display = "";
}

// 快速新建 FO
function foQuickCreate(){
  var planSel = document.getElementById("foPlanSelect");
  var planId = planSel.value;
  if(!planId){ alert("请先选择入库计划"); return; }

  var opt = planSel.options[planSel.selectedIndex];
  var planDay = opt.getAttribute("data-day") || kstToday_();
  var customer = opt.getAttribute("data-customer") || "";
  var summary = opt.getAttribute("data-summary") || "";
  var opType = document.getElementById("foQuickOpType").value;

  var btn = event && event.target ? event.target : null;
  if(btn){ btn.disabled = true; btn.textContent = "创建中..."; }

  // Step 1: create FO as draft
  jsonp(LOCK_URL, {
    action:"b2b_field_op_create", k:getFoKey_(),
    plan_day:planDay, customer_name:customer, goods_summary:summary,
    operation_type:opType, source_plan_id:planId, created_by:getOperatorId()
  }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){
      alert("快速新建失败: " + (res&&res.error||"unknown"));
      if(btn){ btn.disabled = false; btn.textContent = "快速新建"; }
      return;
    }
    var recordId = res.record_id;

    // Step 2: draft → recording
    jsonp(LOCK_URL, {
      action:"b2b_field_op_update", k:getFoKey_(),
      record_id:recordId, sub:"status", status:"recording"
    }, { skipBusy:true }).then(function(res2){
      if(btn){ btn.disabled = false; btn.textContent = "快速新建"; }
      if(!res2 || !res2.ok){
        alert("快速新建成功（" + recordId + "）但状态切换失败: " + (res2&&res2.error||"unknown") + "\n不允许继续开工，请在 /b2b/ 页面手动处理。");
        return;
      }
      // 成功：设置选中并显示确认（先用本地快照，再从服务端拉真实 created_at）
      _foSelectedRecord = {
        record_id:recordId, source_plan_id:planId, plan_day:planDay,
        customer_name:customer, goods_summary:summary, operation_type:opType, status:"recording"
      };
      showFoConfirm(_foSelectedRecord);
      setStatus("快速新建成功 ✅ " + recordId, true);
      // 异步拉真实 created_at 刷新确认卡片
      jsonp(LOCK_URL, { action:"b2b_field_op_detail", record_id:recordId, k:getFoKey_() }, { skipBusy:true }).then(function(res3){
        if(res3 && res3.ok && res3.record){
          _foSelectedRecord = res3.record;
          showFoConfirm(_foSelectedRecord);
        }
      }).catch(function(){});
    });
  });
}

// 开始作业
async function startB2bFieldOp(e){
  if(!_foSelectedRecord || !_foSelectedRecord.record_id){
    alert("请先选择或新建一条现场作业记录");
    return;
  }
  if(!acquireBusy_()) return;
  var btn = e && e.target ? e.target : null;
  var origText = btn ? btn.textContent : "";
  if(btn){ btn.disabled = true; btn.textContent = "处理中..."; }

  try{
    var r = _foSelectedRecord;

    // Step 1: 如果 FO 当前是 draft，先切换到 recording
    if(r.status === "draft"){
      var statusRes = await jsonp(LOCK_URL, {
        action:"b2b_field_op_update", k:getFoKey_(),
        record_id:r.record_id, sub:"status", status:"recording"
      }, { skipBusy:true });
      if(!statusRes || !statusRes.ok){
        throw new Error("FO状态切换失败: " + (statusRes&&statusRes.error||"unknown") + "\n不允许继续开工。");
      }
      r.status = "recording";
    }

    // Step 2: 创建 session + 写 start event
    if(currentSessionId){
      var ok = confirm("当前已有进行中的趟次：" + currentSessionId + "\n\n确定要放弃当前趟次、重新开始一个新趟次吗？");
      if(!ok){ throw new Error("用户取消"); }
      var closeRes;
      try{ closeRes = await sessionCloseServer_(); }catch(e2){ closeRes = {}; }
      if(closeRes.blocked){
        if(closeRes.reason === "pending_b2b_results"){
          var orders = (closeRes.pending_orders || []).map(function(o){
            var st = o.result_status === "missing" ? "未录入" : (o.result_status === "draft" ? "草稿未提交" : o.result_status);
            return o.source_order_no + " 〔" + st + "〕";
          }).join("\n");
          setStatus("有未完成结果单，无法开始新任务", false);
          var goMsg2 = "当前还有未完成提交的工单结果单，不能开始新任务。\n\n请先完成以下工单的结果单：\n" + orders +
            "\n\n趟次：" + currentSessionId + "\n\n点【确定】跳转到B2B工单操作页处理。";
          if(confirm(goMsg2)){
            setSess_("B2B", "B2B工单操作", currentSessionId);
            go("b2b_workorder");
          }
        } else {
          setStatus("旧趟次未能释放，无法开始新任务", false);
          alert("旧趟次未能释放（" + (closeRes.reason || "未知原因") + "），请先结束当前趟次。");
        }
        throw new Error("用户取消");
      }
      if(closeRes.closed || closeRes.already_closed){
        cleanupLocalSession_();
      } else {
        setStatus("旧趟次状态异常，无法开始新任务", false);
        alert("旧趟次状态异常，请先确认当前作业状态。");
        throw new Error("用户取消");
      }
    }

    var newSid = makePickSessionId();
    var evId = makeEventId({ event:"start", biz:"B2B", task:"B2B现场记录", wave_id:"", badgeRaw:"" });
    await submitEventSync_({ event:"start", event_id:evId, biz:"B2B", task:"B2B现场记录", pick_session_id:newSid }, true);
    addRecent(evId);

    currentSessionId = newSid;
    CUR_CTX = { biz:"B2B", task:"B2B现场记录", page:"b2b_field_op" };
    setSess_("B2B", "B2B现场记录", newSid);

    // Step 3: 写 wave event — wave_id = FO-xxx（仅此一次）
    var waveEvId = makeEventId({ event:"wave", biz:"B2B", task:"B2B现场记录", wave_id:r.record_id, badgeRaw:"" });
    await submitEventSync_({ event:"wave", event_id:waveEvId, biz:"B2B", task:"B2B现场记录", pick_session_id:newSid, wave_id:r.record_id }, true);
    addRecent(waveEvId);

    // Step 4: 持久化 FO 信息到 localStorage
    activeB2bFieldOp = new Set();
    saveFoRecord(r);
    persistState();
    refreshUI();
    renderB2bFieldOpUI();
    setStatus("B2B现场记录开始 ✅ " + r.record_id + " 趟次: " + newSid, true);
    fetchOperatorOpenSessions();

  }catch(err){
    setStatus("开始失败 ❌ " + err, false);
    if(String(err) !== "Error: 用户取消") alert(String(err));
  }finally{
    if(btn){ btn.disabled = false; btn.textContent = origText; }
    releaseBusy_();
  }
}

async function endB2bFieldOp(){
  if(!acquireBusy_()) return;
  try{
    var recordId = _foSelectedRecord && _foSelectedRecord.record_id;
    var result = await endSessionGlobal_();
    if(result === "closed" || result === "already_closed"){
      // session 关闭（含已关闭）后，把现场记录状态改为 completed
      if(recordId){
        var foOk = false;
        try{
          var foRes = await jsonp(LOCK_URL, { action:"b2b_field_op_update", k:getFoKey_(), sub:"status", record_id: recordId, status:"completed" }, { skipBusy:true });
          if(foRes && foRes.ok) foOk = true;
          else alert("现场记录状态更新失败: " + (foRes && foRes.error || "unknown") + "\nSession已关闭，但FO记录未完成，请在B2B管理页手动改为completed");
        }catch(e){
          console.error("fo status→completed error", e);
          alert("现场记录状态更新失败（网络错误）\nSession已关闭，但FO记录未完成，请在B2B管理页手动改为completed");
        }
        if(!foOk) return; // FO更新失败：保留本地上下文，不清理
      }
      clearFoRecord();
      _foSelectedRecord = null;
    }
    // blocked / no_session: 不清理本地 FO，保留页面可用
  }finally{
    releaseBusy_();
  }
}

function foEditDetail(){
  var r = _foSelectedRecord;
  if(!r || !r.record_id){ alert("没有选中的记录"); return; }
  if(r.status !== "draft" && r.status !== "recording"){ alert("当前状态不允许编辑（仅草稿/记录中可编辑）"); return; }
  openFoResultForm(r.record_id);
}

function openFoResultForm(recordId){
  var modal = document.getElementById("foResultModal");
  var body = document.getElementById("foResultBody");
  if(!modal || !body) return;
  modal.style.display = "flex";
  body.innerHTML = '<div class="muted">加载中...</div>';

  jsonp(LOCK_URL, { action:"b2b_field_op_detail", record_id:recordId, k:getFoKey_() }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){ body.innerHTML = '<div style="color:red;">'+esc(res&&res.error||"加载失败")+'</div>'; return; }
    var r = res.record;
    var ot = r.operation_type || "box_op";

    var html = '<div style="font-size:16px;font-weight:800;margin-bottom:6px;">现场记录结果单</div>';

    // 只读信息块
    html += '<div style="font-size:12px;color:#555;margin-bottom:10px;padding:8px 10px;background:#f5f5f5;border-radius:6px;">';
    html += '<div><b>'+esc(r.record_id)+'</b> · '+esc(r.customer_name||"")+'</div>';
    if(r.source_plan_id) html += '<div>来源计划: '+esc(r.source_plan_id)+'</div>';
    if(r.created_at) html += '<div>建单: '+fmtFoTime_(r.created_at)+'</div>';
    html += '<div>作业日: '+esc(r.plan_day)+'</div>';
    if(r.goods_summary) html += '<div>货物: '+esc(r.goods_summary)+'</div>';
    if(r.input_box_count) html += '<div>入库箱数: '+r.input_box_count+'</div>';
    if(r.instruction_text) html += '<div>作业说明: '+esc(r.instruction_text)+'</div>';
    html += '</div>';

    // 操作类型下拉
    html += '<div style="margin-bottom:8px;"><label style="font-size:13px;font-weight:700;">操作类型</label>' +
      '<select id="fo-rf-optype" style="width:100%;margin-top:2px;">' +
        '<option value="box_op"'+(ot==="box_op"?' selected':'')+'>箱子操作</option>' +
        '<option value="palletize"'+(ot==="palletize"?' selected':'')+'>打托</option>' +
        '<option value="bulk_in_out"'+(ot==="bulk_in_out"?' selected':'')+'>整进整出</option>' +
        '<option value="unload"'+(ot==="unload"?' selected':'')+'>卸货</option>' +
        '<option value="other"'+(ot==="other"?' selected':'')+'>其他</option>' +
      '</select></div>';

    // 结果字段（复用 _rfField / _rfSwitch）
    html += _rfField("品项数", "fo-rf-sku", "number", r.sku_kind_count, "0", "1");
    html += _rfField("打包件数", "fo-rf-packed-qty", "number", r.packed_qty, "0", "1");
    html += _rfField("产出箱数", "fo-rf-out-box", "number", r.output_box_count, "0", "0.5");
    html += _rfField("打包箱数", "fo-rf-packed-box", "number", r.packed_box_count, "0", "0.5");
    html += _rfSwitch("fo-rf-carton", "是否用了纸箱", r.used_carton, "foRfToggleCarton()");
    html += '<div id="fo-rf-carton-fields" style="'+(r.used_carton?"":"display:none;")+'padding-left:12px;border-left:3px solid #1976d2;margin-bottom:8px;">' +
      _rfField("大箱数", "fo-rf-big-carton", "number", r.big_carton_count, "0", "1") +
      _rfField("小箱数", "fo-rf-small-carton", "number", r.small_carton_count, "0", "1") +
    '</div>';
    html += _rfSwitch("fo-rf-did-rebox", "是否换箱", r.did_rebox, "foRfToggleRebox()");
    html += '<div id="fo-rf-rebox-fields" style="'+(r.did_rebox?"":"display:none;")+'padding-left:12px;border-left:3px solid #1976d2;margin-bottom:8px;">' +
      _rfField("换箱数", "fo-rf-rebox-count", "number", r.rebox_count, "0", "1") +
    '</div>';
    html += _rfField("产出托数", "fo-rf-out-pallet", "number", r.output_pallet_count, "0", "0.5");
    html += _rfSwitch("fo-rf-fork", "需要叉车找货", r.needs_forklift_pick, "foRfToggleFork()");
    html += '<div id="fo-rf-fork-fields" style="'+(r.needs_forklift_pick?"":"display:none;")+'padding-left:12px;border-left:3px solid #ff9800;margin-bottom:8px;">' +
      _rfField("叉车取货托数", "fo-rf-fork-pallet", "number", r.forklift_pallet_count, "0", "0.5") +
      _rfField("涉及货位数", "fo-rf-fork-loc", "number", r.rack_pick_location_count, "0", "1") +
    '</div>';
    html += _rfField("贴标数量", "fo-rf-label", "number", r.label_count, "0", "1");
    html += _rfField("拍照数量", "fo-rf-photo", "number", r.photo_count, "0", "1");
    html += _rfSwitch("fo-rf-pallet-detail", "是否制作了打托明细", r.has_pallet_detail);

    // 备注
    html += '<div style="margin-bottom:10px;"><label style="font-size:13px;font-weight:700;">结果备注</label>' +
      '<textarea id="fo-rf-remark" rows="2" style="width:100%;margin-top:2px;">'+esc(r.remark||"")+'</textarea></div>';

    // 隐藏字段
    html += '<input type="hidden" id="fo-rf-record-id" value="'+esc(recordId)+'" />';

    // 按钮
    html += '<div style="display:flex;gap:8px;">' +
      '<button onclick="submitFoResult()" style="flex:1;padding:10px;font-size:14px;background:#1976d2;color:#fff;border:none;border-radius:6px;">保存结果</button>' +
    '</div>';

    body.innerHTML = html;
  }).catch(function(){ body.innerHTML = '<div style="color:red;">网络错误</div>'; });
}

function closeFoResultForm(){
  var modal = document.getElementById("foResultModal");
  if(modal) modal.style.display = "none";
}

// 现场记录结果单 toggle 函数
function foRfToggleCarton(){
  var cb = document.getElementById("fo-rf-carton");
  var f = document.getElementById("fo-rf-carton-fields");
  if(cb && f){ f.style.display = cb.checked ? "" : "none"; if(!cb.checked) _rfClear(["fo-rf-big-carton","fo-rf-small-carton"]); }
}
function foRfToggleRebox(){
  var cb = document.getElementById("fo-rf-did-rebox");
  var f = document.getElementById("fo-rf-rebox-fields");
  if(cb && f){ f.style.display = cb.checked ? "" : "none"; if(!cb.checked) _rfClear(["fo-rf-rebox-count"]); }
}
function foRfToggleFork(){
  var cb = document.getElementById("fo-rf-fork");
  var f = document.getElementById("fo-rf-fork-fields");
  if(cb && f){ f.style.display = cb.checked ? "" : "none"; if(!cb.checked) _rfClear(["fo-rf-fork-pallet","fo-rf-fork-loc"]); }
}

function submitFoResult(){
  var recordId = (document.getElementById("fo-rf-record-id") || {}).value || "";
  if(!recordId) return;
  var r = _foSelectedRecord;
  if(!r){ alert("记录信息丢失"); return; }

  var _foBadge = parseBadge(getOperatorId());
  var used_carton = _rfChk("fo-rf-carton");
  var did_rebox = _rfChk("fo-rf-did-rebox");
  var needs_forklift = _rfChk("fo-rf-fork");

  var payload = {
    action: "b2b_field_op_update", k: getFoKey_(),
    record_id: recordId, sub: "edit",
    // 主记录字段保持原值
    plan_day: r.plan_day, customer_name: r.customer_name,
    goods_summary: r.goods_summary || "", instruction_text: r.instruction_text || "",
    input_box_count: r.input_box_count || 0,
    // 结果字段从表单取
    operation_type: (document.getElementById("fo-rf-optype") || {}).value || "box_op",
    sku_kind_count: _rfVal("fo-rf-sku"),
    packed_qty: _rfVal("fo-rf-packed-qty"),
    output_box_count: _rfVal("fo-rf-out-box"),
    packed_box_count: _rfVal("fo-rf-packed-box"),
    used_carton: used_carton,
    big_carton_count: used_carton ? _rfVal("fo-rf-big-carton") : 0,
    small_carton_count: used_carton ? _rfVal("fo-rf-small-carton") : 0,
    did_rebox: did_rebox,
    rebox_count: did_rebox ? _rfVal("fo-rf-rebox-count") : 0,
    output_pallet_count: _rfVal("fo-rf-out-pallet"),
    needs_forklift_pick: needs_forklift,
    forklift_pallet_count: needs_forklift ? _rfVal("fo-rf-fork-pallet") : 0,
    rack_pick_location_count: needs_forklift ? _rfVal("fo-rf-fork-loc") : 0,
    label_count: _rfVal("fo-rf-label"),
    photo_count: _rfVal("fo-rf-photo"),
    has_pallet_detail: _rfChk("fo-rf-pallet-detail"),
    did_pack: 0,
    remark: (document.getElementById("fo-rf-remark") || {}).value || "",
    confirm_badge: _foBadge.id,
    confirmed_by: _foBadge.name || _foBadge.id
  };

  setStatus("保存结果中... ⏳", true);
  jsonp(LOCK_URL, payload, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){
      setStatus("保存失败 ❌", false);
      alert("保存失败: " + (res&&res.error||"unknown"));
      return;
    }
    setStatus("结果已保存 ✅", true);
    closeFoResultForm();
    refreshFoFromServer_(recordId);
  }).catch(function(e){
    setStatus("保存失败 ❌ " + e, false);
    alert("保存失败: " + e);
  });
}

/** ===== 通用临时去卸货 / 返回原任务 ===== */

// 根据当前 biz 决定目标卸货任务
function unloadTarget_(biz){
  if(biz === "B2B") return { biz:"B2B", task:"B2B卸货", page:"b2b_unload" };
  if(biz === "进口") return { biz:"进口", task:"卸货", page:"import_unload" };
  return null;
}
function loadoutTarget_(biz){
  if(biz === "B2B") return { biz:"B2B", task:"B2B出库", page:"b2b_outbound" };
  if(biz === "进口") return { biz:"进口", task:"装柜/出货", page:"import_loadout" };
  return null;
}
// 通用目标解析
function tempSwitchTarget_(srcBiz, kind){
  if(kind === "unload") return unloadTarget_(srcBiz);
  if(kind === "loadout") return loadoutTarget_(srcBiz);
  return null;
}
var TEMP_KIND_LABEL = { unload:"卸货", loadout:"装车" };
var TEMP_KIND_LABEL_KR = { unload:"하차", loadout:"상차" };
// 所有可能的临时目标任务名（用于检测）
var TEMP_TARGET_TASKS = {
  "B2B卸货":"unload", "卸货":"unload",
  "B2B出库":"loadout", "装柜/出货":"loadout"
};

// 获取当前任务对应的扫码记录（用于保存/恢复）
function getScannedItems_(task){
  if(task === "B2B工单操作") return Array.from(scannedB2bWorkorders);
  if(task === "B2B入库理货") return Array.from(scannedB2bTallyOrders);
  if(task === "理货")       return Array.from(scannedInbounds);
  if(task === "批量出库")    return Array.from(scannedBulkOutOrders);
  if(task === "拣货")       return Array.from(scannedWaves);
  return [];
}
function restoreScannedItems_(task, items){
  if(!items || !items.length) return;
  if(task === "B2B工单操作") items.forEach(function(w){ scannedB2bWorkorders.add(w); });
  if(task === "B2B入库理货") items.forEach(function(w){ scannedB2bTallyOrders.add(w); });
  if(task === "理货")       items.forEach(function(w){ scannedInbounds.add(w); });
  if(task === "批量出库")    items.forEach(function(w){ scannedBulkOutOrders.add(w); });
  if(task === "拣货")       items.forEach(function(w){ scannedWaves.add(w); });
}

// 获取卸货任务的 active set
function getUnloadActiveSet_(task){
  var reg = taskReg_(task);
  return reg ? reg.get() : new Set();
}

var TEMP_SWITCH_KEY = "tempSwitchCtx";
function loadTempSwitchCtx_(){
  var saved = localStorage.getItem(TEMP_SWITCH_KEY);
  // 兼容旧 key
  if(!saved) saved = localStorage.getItem("tempSwitchFromWorkorder");
  if(!saved) return null;
  try{
    var ctx = JSON.parse(saved);
    // 过期检查：超过12小时自动清除
    if(ctx.timestamp && Date.now() - ctx.timestamp > 12 * 3600 * 1000){
      localStorage.removeItem(TEMP_SWITCH_KEY);
      localStorage.removeItem("tempSwitchFromWorkorder");
      return null;
    }
    // 兼容旧格式（最老版本）
    if(!ctx.sourceBiz && ctx.workorderSession){
      ctx.sourceBiz = "B2B"; ctx.sourceTask = "B2B工单操作"; ctx.sourcePage = "b2b_workorder";
      ctx.sourceSession = ctx.workorderSession;
      ctx.scannedItems = ctx.workorders || [];
      ctx.unloadBiz = "B2B"; ctx.unloadTask = "B2B卸货"; ctx.unloadPage = "b2b_unload";
    }
    if(!ctx.badges && ctx.badge) ctx.badges = [ctx.badge];
    // 兼容旧字段 unloadBiz/unloadTask/unloadPage → targetBiz/targetTask/targetPage
    if(!ctx.targetBiz && ctx.unloadBiz){
      ctx.targetBiz = ctx.unloadBiz;
      ctx.targetTask = ctx.unloadTask;
      ctx.targetPage = ctx.unloadPage;
      ctx.targetKind = "unload";
    }
    // 确保 targetKind 有值
    if(!ctx.targetKind){
      ctx.targetKind = TEMP_TARGET_TASKS[ctx.targetTask] || "unload";
    }
    return ctx;
  }catch(e){ return null; }
}
function saveTempSwitchCtx_(ctx){
  localStorage.setItem(TEMP_SWITCH_KEY, JSON.stringify(ctx));
  localStorage.removeItem("tempSwitchFromWorkorder");
}
function clearTempSwitchCtx_(){
  localStorage.removeItem(TEMP_SWITCH_KEY);
  localStorage.removeItem("tempSwitchFromWorkorder");
}

async function tempSwitchToUnload(){
  if(!acquireBusy_()) return;
  try{
    await _smCloseLaborBeforeTempSwitch_();
    await tempSwitchToTarget_("unload");
  }finally{ releaseBusy_(); }
}
async function tempSwitchToLoadout(){
  if(!acquireBusy_()) return;
  try{
    await _smCloseLaborBeforeTempSwitch_();
    await tempSwitchToTarget_("loadout");
  }finally{ releaseBusy_(); }
}
// 简化模式热修：临时切走前关闭当前工单所有 active labor detail
async function _smCloseLaborBeforeTempSwitch_(){
  if(!_smIsSimpleMode || !currentSessionId) return;
  // 关闭当前 session 下所有 active labor（不限单张工单，因为切走是整人切走）
  try{
    await jsonp(LOCK_URL, {
      action: "b2b_simple_labor_leave_all",
      session_id: currentSessionId,
      temp_switch: "1"
    }, { skipBusy:true });
  }catch(e){ console.error("_smCloseLaborBeforeTempSwitch_ error", e); }
}
async function tempSwitchToTarget_(kind){
  var kindLabel = TEMP_KIND_LABEL[kind] || kind;
  // 识别当前任务
  var srcBiz = CUR_CTX && CUR_CTX.biz;
  var srcTask = CUR_CTX && CUR_CTX.task;
  var srcPage = CUR_CTX && CUR_CTX.page;
  if(!srcBiz || !srcTask){ alert("当前没有进行中的任务，无法切换。"); return; }

  // 不能从目标任务切到同类目标
  if(TEMP_TARGET_TASKS[srcTask] === kind){ alert("当前已经在"+kindLabel+"任务中。"); return; }

  // 确定目标
  var target = tempSwitchTarget_(srcBiz, kind);
  if(!target){
    // B2C/仓库等，让用户选择
    var opts, choices;
    if(kind === "unload"){
      opts = "1. B2B卸货\n2. 进口快件卸货";
      choices = { "1":{ biz:"B2B", task:"B2B卸货", page:"b2b_unload" }, "2":{ biz:"进口", task:"卸货", page:"import_unload" } };
    } else {
      opts = "1. B2B出库\n2. 进口 装柜/出货";
      choices = { "1":{ biz:"B2B", task:"B2B出库", page:"b2b_outbound" }, "2":{ biz:"进口", task:"装柜/出货", page:"import_loadout" } };
    }
    var c = prompt("当前环节无对应"+kindLabel+"，请选择：\n"+opts+"\n\n输入 1 或 2：");
    if(!c) return;
    target = choices[c.trim()];
    if(!target){ alert("无效选择"); return; }
  }

  var srcSid = getSess_(srcBiz, srcTask);
  if(!srcSid){ alert("当前没有进行中的作业Session，无法切换。"); return; }

  // 获取当前在岗人员
  var reg = taskReg_(srcTask);
  var members = reg ? Array.from(reg.get()) : [];
  if(members.length === 0){ alert("当前任务中没有在岗人员，请先加入作业再切换。"); return; }

  // 选人
  var badges;
  if(members.length === 1){
    if(!confirm("确定要临时去"+kindLabel+"吗？\n\n工牌：" + badgeDisplay(members[0]) + "\n\n将自动退出当前任务 → 跳转到"+kindLabel+"页面")) return;
    badges = [members[0]];
  } else {
    var list = members.map(function(m, i){ return (i+1) + ". " + badgeDisplay(m); }).join("\n");
    var choice = prompt("请选择要临时去"+kindLabel+"的人员（多选用逗号分隔，A=全部）：\n\n" + list);
    if(!choice) return;
    choice = choice.trim();
    if(choice.toUpperCase() === "A"){
      badges = members.slice();
    } else {
      var indices = choice.split(/[,，\s]+/);
      var seen = {};
      badges = [];
      for(var i = 0; i < indices.length; i++){
        var idx = parseInt(indices[i], 10) - 1;
        if(isNaN(idx) || idx < 0 || idx >= members.length){ alert("无效序号：" + indices[i]); return; }
        if(!seen[idx]){ seen[idx] = true; badges.push(members[idx]); }
      }
    }
    if(badges.length === 0){ alert("未选择任何人员"); return; }
    var names = badges.map(function(b){ return badgeDisplay(b); }).join("、");
    if(!confirm("确定要以下 " + badges.length + " 人临时去"+kindLabel+"吗？\n\n" + names + "\n\n将自动退出当前任务 → 跳转到"+kindLabel+"页面")) return;
  }

  var srcLabel = taskDisplayLabel(srcBiz, srcTask);

  // ===== 1. PREFLIGHT：创建/复用目标 session（源任务还没动）=====
  setStatus("临时切换中... 正在准备"+kindLabel+"环境 ⏳", true);
  var targetSid = getSess_(target.biz, target.task);
  var targetCreated = false;
  if(targetSid){
    try{
      var sInfo = await jsonp(LOCK_URL, { action:"session_info", session: targetSid }, { skipBusy: true });
      if(sInfo && String(sInfo.status||"").toUpperCase() === "CLOSED"){
        clearSess_(target.biz, target.task);
        targetSid = null;
      }
    }catch(e){ /* 查询失败继续使用 */ }
  }
  if(!targetSid){
    targetSid = makePickSessionId();
    var evStart = makeEventId({ event:"start", biz:target.biz, task:target.task, wave_id:"", badgeRaw:"" });
    try{
      await submitEventSync_({ event:"start", event_id: evStart, biz:target.biz, task:target.task, pick_session_id: targetSid, temp_switch: true }, true);
      addRecent(evStart);
      targetCreated = true;
      var targetReg = taskReg_(target.task);
      if(targetReg) targetReg.set(new Set());
    }catch(e){
      // 目标 session 创建失败 → 直接中止，源任务完全没动
      setStatus("切换失败 ❌", false);
      alert("切换失败：无法创建"+kindLabel+"趟次。\n\n原因：" + (e.message || e) + "\n\n当前任务未受影响，可继续操作。");
      return;
    }
  }
  setSess_(target.biz, target.task, targetSid);

  // ===== 2. LEAVE 源任务（目标已确认可用）=====
  setStatus("正在退出" + srcLabel + "（" + badges.length + "人）⏳", true);
  currentSessionId = srcSid;
  CUR_CTX = { biz: srcBiz, task: srcTask, page: srcPage };
  var leftBadges = [];
  for(var j = 0; j < badges.length; j++){
    try{
      var b = badges[j];
      var evLeave = makeEventId({ event:"leave", biz:srcBiz, task:srcTask, wave_id:"", badgeRaw: b });
      await submitEventSyncWithRetry_({ event:"leave", event_id: evLeave, biz:srcBiz, task:srcTask, pick_session_id: srcSid, da_id: b });
      addRecent(evLeave);
      applyActive(srcTask, "leave", b);
      leftBadges.push(b);
    }catch(e){
      alert("退出失败（" + badgeDisplay(badges[j]) + "）：" + e + "\n该人员将留在原任务。");
    }
  }
  persistState();

  if(leftBadges.length === 0){
    // 回滚刚创建的空目标 session
    if(targetCreated){
      var rollOk = false;
      try{
        currentSessionId = targetSid;
        var rrr = await sessionCloseServer_();
        rollOk = !!(rrr && (rrr.closed || rrr.already_closed));
      }catch(_){}
      clearSess_(target.biz, target.task);
      var tReg = taskReg_(target.task);
      if(tReg) tReg.set(new Set());
      if(rollOk){
        setStatus("全部退出失败，切换已取消 ❌", false);
        alert("切换失败：所有人员退出原任务失败。\n目标空趟次已回收，当前任务未受影响。");
      } else {
        setStatus("全部退出失败，目标趟次清理失败 ⚠️", false);
        alert("切换失败：所有人员退出原任务失败。\n\n源任务未受影响，但目标趟次清理失败。\n趟次ID: " + targetSid + "\n请联系管理员处理。");
      }
    } else {
      setStatus("全部退出失败，切换已取消 ❌", false);
      alert("切换失败：所有人员退出原任务失败。\n当前任务未受影响，可继续操作。");
    }
    // 恢复源任务上下文
    currentSessionId = srcSid;
    CUR_CTX = { biz: srcBiz, task: srcTask, page: srcPage };
    persistState(); refreshUI(); renderActiveLists();
    fetchOperatorOpenSessions();
    return;
  }

  // ===== 3. JOIN 目标任务 =====
  setStatus("正在加入"+kindLabel+"... ⏳", true);
  currentSessionId = targetSid;
  CUR_CTX = { biz: target.biz, task: target.task, page: target.page };

  var joinedTarget = [];
  var joinFailedBadges = [];
  for(var u = 0; u < leftBadges.length; u++){
    try{
      var evJoin = makeEventId({ event:"join", biz:target.biz, task:target.task, wave_id:"", badgeRaw: leftBadges[u] });
      await submitEventSyncWithRetry_({ event:"join", event_id: evJoin, biz:target.biz, task:target.task, pick_session_id: targetSid, da_id: leftBadges[u] });
      addRecent(evJoin);
      applyActive(target.task, "join", leftBadges[u]);
      joinedTarget.push(leftBadges[u]);
    }catch(e){
      joinFailedBadges.push(leftBadges[u]);
    }
  }

  // ===== 4. ROLLBACK 处理 join 失败的 badge =====
  if(joinedTarget.length === 0){
    // 全部 join 失败 → 整体回滚：把所有 leftBadges rejoin 回源任务
    setStatus("加入"+kindLabel+"全部失败，正在恢复原任务... ⏳", true);

    // 清理 ghost target session（无人成功 join）
    if(targetCreated){
      try{
        currentSessionId = targetSid;
        await sessionCloseServer_();
      }catch(e){ /* best effort close */ }
    }
    clearSess_(target.biz, target.task);
    var targetReg2 = taskReg_(target.task);
    if(targetReg2) targetReg2.set(new Set());

    currentSessionId = srcSid;
    CUR_CTX = { biz: srcBiz, task: srcTask, page: srcPage };
    var rollbackOk = 0;
    for(var r = 0; r < leftBadges.length; r++){
      try{
        var evRejoin = makeEventId({ event:"join", biz:srcBiz, task:srcTask, wave_id:"", badgeRaw: leftBadges[r] });
        await submitEventSyncWithRetry_({ event:"join", event_id: evRejoin, biz:srcBiz, task:srcTask, pick_session_id: srcSid, da_id: leftBadges[r] });
        addRecent(evRejoin);
        applyActive(srcTask, "join", leftBadges[r]);
        rollbackOk++;
      }catch(e){ /* best effort */ }
    }
    persistState();
    if(rollbackOk === leftBadges.length){
      setStatus("切换失败，已恢复原任务 ❌", false);
      alert("切换失败：无法加入"+kindLabel+"。\n已自动恢复原任务（" + rollbackOk + "人），可继续操作。");
    } else if(rollbackOk > 0){
      setStatus("切换失败，部分恢复 ⚠️", false);
      alert("切换失败：无法加入"+kindLabel+"。\n\n自动恢复结果：" + rollbackOk + "/" + leftBadges.length + " 人已恢复原任务。\n未恢复的人员请在原任务页手动重新加入。");
    } else {
      setStatus("切换失败，恢复也失败 ⚠️", false);
      alert("切换失败：无法加入"+kindLabel+"，且自动恢复原任务也失败。\n\n请手动操作：\n1. 回到原任务（" + srcLabel + "）页面\n2. 重新加入作业\n\n当前session：" + srcSid);
    }
    return;
  }

  // 部分 join 失败 → 失败的 badge 自动回滚到源任务
  if(joinFailedBadges.length > 0){
    currentSessionId = srcSid;
    CUR_CTX = { biz: srcBiz, task: srcTask, page: srcPage };
    var partialRollbackOk = 0;
    for(var rp = 0; rp < joinFailedBadges.length; rp++){
      try{
        var evRejoinP = makeEventId({ event:"join", biz:srcBiz, task:srcTask, wave_id:"", badgeRaw: joinFailedBadges[rp] });
        await submitEventSyncWithRetry_({ event:"join", event_id: evRejoinP, biz:srcBiz, task:srcTask, pick_session_id: srcSid, da_id: joinFailedBadges[rp] });
        addRecent(evRejoinP);
        applyActive(srcTask, "join", joinFailedBadges[rp]);
        partialRollbackOk++;
      }catch(e){ /* best effort */ }
    }
    persistState();
    var failNames = joinFailedBadges.map(function(b){ return badgeDisplay(b); }).join("、");
    if(partialRollbackOk === joinFailedBadges.length){
      alert("以下人员加入"+kindLabel+"失败，已自动恢复到原任务：\n" + failNames);
    } else {
      alert("以下人员加入"+kindLabel+"失败：\n" + failNames + "\n\n其中 " + partialRollbackOk + "/" + joinFailedBadges.length + " 人已恢复原任务，其余请手动在原任务页重新加入。");
    }
    // 恢复上下文到目标（继续为成功的 badge 完成切换）
    currentSessionId = targetSid;
    CUR_CTX = { biz: target.biz, task: target.task, page: target.page };
  }

  persistState();

  // ===== 5. 保存上下文 + 导航 =====
  saveTempSwitchCtx_({
    badges: joinedTarget,
    sourceBiz: srcBiz, sourceTask: srcTask, sourcePage: srcPage,
    sourceSession: srcSid, scannedItems: getScannedItems_(srcTask),
    targetKind: kind, targetBiz: target.biz, targetTask: target.task, targetPage: target.page,
    timestamp: Date.now()
  });

  setStatus("已切换到"+kindLabel+" ✅（" + joinedTarget.length + "/" + badges.length + "人已加入）", false);
  go(target.page);
  refreshUI();
  renderActiveLists();
  updateReturnButton_();
}

async function returnFromTempUnload(){
  if(!acquireBusy_()) return;
  try{ await returnFromTempTarget_(); }finally{ releaseBusy_(); }
}
async function returnFromTempTarget(){
  if(!acquireBusy_()) return;
  try{ await returnFromTempTarget_(); }finally{ releaseBusy_(); }
}
async function returnFromTempTarget_(){
  var ctx = loadTempSwitchCtx_();
  if(!ctx){ alert("没有找到切换记录"); return; }

  var kindLabel = TEMP_KIND_LABEL[ctx.targetKind] || "目标任务";
  var allBadges = ctx.badges || [];
  var srcSid = ctx.sourceSession;
  var srcBiz = ctx.sourceBiz;
  var srcTask = ctx.sourceTask;
  var srcPage = ctx.sourcePage;
  var tgtBiz = ctx.targetBiz || ctx.unloadBiz || "B2B";
  var tgtTask = ctx.targetTask || ctx.unloadTask || "B2B卸货";
  var tgtPage = ctx.targetPage || ctx.unloadPage || "b2b_unload";
  var srcLabel = taskDisplayLabel(srcBiz, srcTask);

  // 如果上下文里badges为空（B设备远程检测），用当前目标任务在岗名单
  if(allBadges.length === 0){
    allBadges = Array.from(getUnloadActiveSet_(tgtTask));
  }
  if(allBadges.length === 0){ alert("没有找到工牌信息，请先在"+kindLabel+"页加入作业。"); return; }

  // 选择要返回的人
  var returning;
  if(allBadges.length === 1){
    if(!confirm("确定返回" + srcLabel + "吗？\n\n工牌：" + badgeDisplay(allBadges[0]) + "\n\n将自动退出"+kindLabel+" → 重新加入原任务")) return;
    returning = allBadges.slice();
  } else {
    var list = allBadges.map(function(m, i){ return (i+1) + ". " + badgeDisplay(m); }).join("\n");
    var choice = prompt("请选择要返回" + srcLabel + "的人员（多选用逗号分隔，A=全部）：\n\n" + list);
    if(!choice) return;
    choice = choice.trim();
    if(choice.toUpperCase() === "A"){
      returning = allBadges.slice();
    } else {
      var indices = choice.split(/[,，\s]+/);
      var seen2 = {};
      returning = [];
      for(var k = 0; k < indices.length; k++){
        var idx = parseInt(indices[k], 10) - 1;
        if(isNaN(idx) || idx < 0 || idx >= allBadges.length){ alert("无效序号：" + indices[k]); return; }
        if(!seen2[idx]){ seen2[idx] = true; returning.push(allBadges[idx]); }
      }
    }
    if(returning.length === 0){ alert("未选择任何人员"); return; }
    var names = returning.map(function(b){ return badgeDisplay(b); }).join("、");
    if(!confirm("确定以下 " + returning.length + " 人返回" + srcLabel + "吗？\n\n" + names)) return;
  }

  // 1. 逐个 leave 目标任务（如果还在岗，释放锁）
  var tgtSid = getSess_(tgtBiz, tgtTask);
  if(tgtSid){
    var toLeave = returning.filter(function(b){ return isAlreadyActive(tgtTask, b); });
    if(toLeave.length > 0){
      setStatus("正在退出"+kindLabel+"（" + toLeave.length + "人）... ⏳", true);
      currentSessionId = tgtSid;
      CUR_CTX = { biz: tgtBiz, task: tgtTask, page: tgtPage };
      for(var i = 0; i < toLeave.length; i++){
        try{
          var evLeave = makeEventId({ event:"leave", biz:tgtBiz, task:tgtTask, wave_id:"", badgeRaw: toLeave[i] });
          await submitEventSyncWithRetry_({ event:"leave", event_id: evLeave, biz:tgtBiz, task:tgtTask, pick_session_id: tgtSid, da_id: toLeave[i] });
          addRecent(evLeave);
          applyActive(tgtTask, "leave", toLeave[i]);
        }catch(e){
          alert("退出"+kindLabel+"失败（" + badgeDisplay(toLeave[i]) + "）：" + e + "\n将继续处理其余人员。");
        }
      }
      persistState();
      // 触发自动结束（如果最后一人离开），保护源任务数据
      var savedSrcReg = taskReg_(srcTask);
      var savedSrcActive = savedSrcReg ? new Set(savedSrcReg.get()) : null;
      var savedSrcSess = getSess_(srcBiz, srcTask);
      laborTask = tgtTask; laborBiz = tgtBiz;
      await tryAutoEndSessionAfterLeave_();
      // 恢复源任务数据
      if(savedSrcReg && savedSrcActive) savedSrcReg.set(savedSrcActive);
      if(savedSrcSess) setSess_(srcBiz, srcTask, savedSrcSess);
    }
  }

  // 2. 检查源 session 是否还在
  try{
    var info = await jsonp(LOCK_URL, { action:"session_info", session: srcSid }, { skipBusy: true });
    if(info && String(info.status||"").toUpperCase() === "CLOSED"){
      clearTaskLocalState_(srcBiz, srcTask, srcSid);
      clearTempSwitchCtx_();
      persistState();
      alert("原任务趟次已被关闭（可能已被其他人结束），需要重新开始。");
      go(srcPage);
      refreshUI();
      renderActiveLists();
      updateReturnButton_();
      fetchOperatorOpenSessions();
      return;
    }
  }catch(e){
    // 查询失败，继续尝试 rejoin
  }

  // 3. 逐个 rejoin 源任务
  setStatus("正在返回" + srcLabel + "（" + returning.length + "人）... ⏳", true);
  currentSessionId = srcSid;
  CUR_CTX = { biz: srcBiz, task: srcTask, page: srcPage };
  setSess_(srcBiz, srcTask, srcSid);
  var joinedCount = 0;
  for(var j = 0; j < returning.length; j++){
    try{
      var evJoin = makeEventId({ event:"join", biz:srcBiz, task:srcTask, wave_id:"", badgeRaw: returning[j] });
      await submitEventSyncWithRetry_({ event:"join", event_id: evJoin, biz:srcBiz, task:srcTask, pick_session_id: srcSid, da_id: returning[j] });
      addRecent(evJoin);
      applyActive(srcTask, "join", returning[j]);
      joinedCount++;
    }catch(e){
      alert("返回失败（" + badgeDisplay(returning[j]) + "）：" + e + "\n将继续处理其余人员。");
    }
  }

  // 恢复扫码记录
  restoreScannedItems_(srcTask, ctx.scannedItems);
  persistState();

  // 4. 更新 localStorage：移除已返回的人，保留仍在目标的人
  var remaining = allBadges.filter(function(b){ return returning.indexOf(b) < 0; });
  if(remaining.length > 0){
    ctx.badges = remaining;
    saveTempSwitchCtx_(ctx);
  } else {
    clearTempSwitchCtx_();
  }

  // 5. 导航 + 同步服务器最新在岗状态
  syncActiveFromServer_();
  if(remaining.length > 0){
    refreshUI();
    renderActiveLists();
    updateReturnButton_();
    setStatus("已返回 " + joinedCount + " 人，还有 " + remaining.length + " 人在"+kindLabel+" ✅", false);
  } else {
    go(srcPage);
    refreshUI();
    renderActiveLists();
    // 刷新对应任务的扫码UI
    if(srcTask === "B2B工单操作") renderB2bWorkorderUI();
    if(srcTask === "B2B入库理货") renderB2bTallyUI();
    if(srcTask === "B2B现场记录") renderB2bFieldOpUI();
    if(srcTask === "理货") renderInboundCountUI();
    if(srcTask === "批量出库") renderBulkOutUI();
    updateReturnButton_();
    setStatus("全部已返回" + srcLabel + " ✅（" + joinedCount + "/" + returning.length + "人）", false);
  }
}

function tempTargetBack_(){
  var ctx = loadTempSwitchCtx_();
  if(ctx){
    var kindLabel = TEMP_KIND_LABEL[ctx.targetKind] || "目标任务";
    var go_ = confirm("您正在临时"+kindLabel+"中，直接返回会导致"+kindLabel+"锁未释放。\n\n• 点【确定】→ 先自动返回原任务再离开\n• 点【取消】→ 留在当前页");
    if(go_) returnFromTempTarget();
    return;
  }
  back();
}
// 兼容旧函数名
function tempUnloadBack_(){ tempTargetBack_(); }
function b2bUnloadBack(){ tempTargetBack_(); }

var TEMP_TARGET_PAGES = ["b2b_unload","import_unload","b2b_outbound","import_loadout"];
var TEMP_TARGET_BTN_IDS = {
  b2b_unload: "btnReturnFromTemp_b2b_unload",
  import_unload: "btnReturnFromTemp_import_unload",
  b2b_outbound: "btnReturnFromTemp_b2b_outbound",
  import_loadout: "btnReturnFromTemp_import_loadout"
};

function updateReturnButton_(){
  var ctx = loadTempSwitchCtx_();
  var show = !!ctx;
  var curPage = getHashPage();

  // 同时处理旧 ID 和新 ID
  var oldBtns = [
    document.getElementById("btnReturnFromUnload_b2b"),
    document.getElementById("btnReturnFromUnload_import")
  ];
  oldBtns.forEach(function(b){ if(b) b.style.display = "none"; });

  TEMP_TARGET_PAGES.forEach(function(pg){
    var btn = document.getElementById(TEMP_TARGET_BTN_IDS[pg]);
    if(btn) btn.style.display = (curPage === pg && show) ? "block" : "none";
  });

  // 兼容旧按钮 ID
  if(show && curPage === "b2b_unload"){
    var old1 = document.getElementById("btnReturnFromUnload_b2b");
    if(old1) old1.style.display = "block";
  }
  if(show && curPage === "import_unload"){
    var old2 = document.getElementById("btnReturnFromUnload_import");
    if(old2) old2.style.display = "block";
  }

  if(!show) detectRemoteTempSwitch_();
}

function detectRemoteTempSwitch_(){
  var op = getOperatorId();
  if(!op) return;
  jsonp(LOCK_URL, { action:"operator_open_sessions", operator_id: op }, { skipBusy: true }).then(function(res){
    if(!res || !res.sessions) return;
    // 检测是否有 非目标session + 目标session 同时在开
    var srcSession = null;
    var targetHits = {}; // task → session info
    for(var i = 0; i < res.sessions.length; i++){
      var s = res.sessions[i];
      var tKind = TEMP_TARGET_TASKS[s.task];
      if(tKind){ targetHits[s.task] = { biz:s.biz, task:s.task, kind:tKind }; continue; }
      if(!srcSession) srcSession = s; // 第一个非目标 session 当作源
    }
    if(!srcSession) return;
    // 取第一个匹配的目标任务
    var tgtKey = Object.keys(targetHits)[0];
    if(!tgtKey) return;
    var tgt = targetHits[tgtKey];
    var tgtPage = pageForTask(tgt.biz, tgt.task) || "b2b_unload";
    var srcPage = pageForTask(srcSession.biz, srcSession.task) || "home";
    var activeBadges = Array.from(getUnloadActiveSet_(tgt.task));

    saveTempSwitchCtx_({
      badges: activeBadges,
      sourceBiz: srcSession.biz, sourceTask: srcSession.task, sourcePage: srcPage,
      sourceSession: srcSession.session, scannedItems: [],
      targetKind: tgt.kind, targetBiz: tgt.biz, targetTask: tgt.task, targetPage: tgtPage,
      timestamp: Date.now(), fromRemote: true
    });
    setSess_(srcSession.biz, srcSession.task, srcSession.session);
    // 显示对应按钮
    var curPage = getHashPage();
    var btnId = TEMP_TARGET_BTN_IDS[curPage];
    if(btnId){
      var btn = document.getElementById(btnId);
      if(btn) btn.style.display = "block";
    }
    // 兼容旧按钮 ID
    if(curPage === "b2b_unload"){ var ob = document.getElementById("btnReturnFromUnload_b2b"); if(ob) ob.style.display = "block"; }
    if(curPage === "import_unload"){ var ob2 = document.getElementById("btnReturnFromUnload_import"); if(ob2) ob2.style.display = "block"; }
  }).catch(function(){});
}

// ===== B2B 工单号合法性判断（仅约束扫码结果） =====
function isValidB2bWorkorderCode_(code){
  if(!/^[A-Z0-9\-]{8,40}$/.test(code)) return false;
  for(var i=0;i<B2B_DENY_PREFIX.length;i++){
    if(code.indexOf(B2B_DENY_PREFIX[i]) === 0) return false;
  }
  return true;
}

async function openScannerB2bWorkorder(){
  if(!currentSessionId){ setStatus("请先开始操作", false); return; }
  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再扫码，请重新开始。"))) return;
  _b2bPendingCode = null; _b2bPendingTime = 0;
  scanMode = "b2b_workorder";
  document.getElementById("scanTitle").textContent = "扫码工单号（B2B）— 连续扫码模式：扫完请手动点关闭";
  await openScannerCommon();
  updateScanRecentList_();
}

function manualAddB2bWorkorder(){
  var inp = document.getElementById("b2bWorkorderManualInput");
  if(!inp) return;
  var val = String(inp.value || "").trim();
  if(!val){ alert("请输入工单号"); return; }
  if(!currentSessionId){ alert("请先点【开始操作】"); return; }
  if(scannedB2bWorkorders.has(val)){
    alert("该工单已绑定\n" + val);
    renderB2bWorkorderUI(); inp.value = ""; return;
  }
  scannedB2bWorkorders.add(val);
  persistState(); renderB2bWorkorderUI();
  var evId = makeEventId({ event:"wave", biz:"B2B", task:"B2B工单操作", wave_id: val, badgeRaw:"" });
  if(!hasRecent(evId)){
    submitEvent({ event:"wave", event_id: evId, biz:"B2B", task:"B2B工单操作", pick_session_id: currentSessionId, wave_id: val });
    addRecent(evId);
  }
  callB2bOpBind(val);
  setStatus("绑定中... " + val, true);
  inp.value = "";
}

function renderB2bWorkorderUI(){
  var c = document.getElementById("b2bWorkorderOrderCount");
  var l = document.getElementById("b2bWorkorderOrderList");
  if(c) c.textContent = String(scannedB2bWorkorders.size);
  updateB2bOwnerDisplay_();

  // 跨天 session 提示
  var banner = document.getElementById("b2bCrossDayBanner");
  if(banner){
    var hasCrossDay = false;
    scannedB2bWorkorders.forEach(function(x){
      var b = b2bWorkorderBindings[x];
      if(b && b.day_kst && b.day_kst !== kstDayKey_(Date.now())) hasCrossDay = true;
    });
    banner.style.display = hasCrossDay ? "" : "none";
  }

  if(!l) return;
  if(scannedB2bWorkorders.size === 0){
    l.innerHTML = '<span class="muted">无 / 없음</span>';
    return;
  }
  var arr = Array.from(scannedB2bWorkorders);
  var show = arr.slice(Math.max(0, arr.length - 30));

  // 按结果状态分组
  var groups = { completed:[], draft:[], none:[] };
  show.forEach(function(x){
    var rk = b2bResultKeyFromBinding_(x);
    var r = rk ? b2bWorkorderResults[rk] : null;
    if(r && r.status === "completed") groups.completed.push(x);
    else if(r) groups.draft.push(x);
    else groups.none.push(x);
  });

  var html = "";
  var sections = [
    { key:"completed", label:"已完成", items:groups.completed },
    { key:"draft",     label:"待补记录", items:groups.draft },
    { key:"none",      label:"未录结果", items:groups.none }
  ];
  sections.forEach(function(sec){
    if(!sec.items.length) return;
    html += '<div style="font-size:12px;font-weight:700;color:#555;margin:8px 0 4px;border-bottom:1px solid #eee;padding-bottom:2px;">'+sec.label+'（'+sec.items.length+'）</div>';
    html += sec.items.map(function(x){ return renderB2bBindCard_(x); }).join("");
  });
  l.innerHTML = html;
}

function updateB2bOwnerDisplay_(){
  var area = document.getElementById("b2bOwnerArea");
  if(!area) return;
  if(!currentSessionId || !_sessionOwnerInfo){
    area.style.display = "none";
    return;
  }
  area.style.display = "";
  var creatorEl = document.getElementById("b2bSessionCreator");
  var ownerEl = document.getElementById("b2bSessionOwner");
  var btnEl = document.getElementById("btnTransferOwner");
  if(creatorEl) creatorEl.textContent = _sessionOwnerInfo.created_by_operator || "-";
  if(ownerEl) ownerEl.textContent = _sessionOwnerInfo.owner_operator_id || "-";

  // 按钮：只有当前操作员===当前owner 且 active>=2 时显示
  if(btnEl){
    var curOp = getOperatorId();
    var isOwner = curOp && curOp === _sessionOwnerInfo.owner_operator_id;
    // 检查 active 人数（从 B2B工单操作 task registry）
    var reg = null;
    for(var i=0;i<TASK_REGISTRY.length;i++){ if(TASK_REGISTRY[i].task==="B2B工单操作"){ reg=TASK_REGISTRY[i]; break; } }
    var activeCount = reg ? reg.get().size : 0;
    btnEl.style.display = (isOwner && activeCount >= 2) ? "" : "none";
  }
}

async function transferSessionOwner(){
  if(!currentSessionId){ alert("无当前趟次"); return; }
  if(!_sessionOwnerInfo){ alert("负责人信息未加载，请稍后重试"); return; }
  var curOp = getOperatorId();
  if(!curOp){ alert("未设置操作员"); return; }
  if(curOp !== _sessionOwnerInfo.owner_operator_id){
    alert("只有当前负责人才能交接。\n当前负责人: " + _sessionOwnerInfo.owner_operator_id);
    return;
  }

  // 从 active 列表排除当前 owner
  var reg = null;
  for(var i=0;i<TASK_REGISTRY.length;i++){ if(TASK_REGISTRY[i].task==="B2B工单操作"){ reg=TASK_REGISTRY[i]; break; } }
  if(!reg){ alert("任务注册未找到"); return; }
  var activeSet = reg.get();
  var candidates = [];
  activeSet.forEach(function(b){ if(b !== curOp) candidates.push(b); });
  if(candidates.length === 0){ alert("当前无其他在岗人员可交接"); return; }

  // 选择目标
  var msg = "选择交接目标（输入序号）：\n";
  candidates.forEach(function(b, idx){
    var p = parseBadge(b);
    msg += (idx+1) + ". " + (p.name ? p.id + " " + p.name : b) + "\n";
  });
  var choice = prompt(msg);
  if(!choice) return;
  var idx = parseInt(choice, 10) - 1;
  if(isNaN(idx) || idx < 0 || idx >= candidates.length){ alert("无效选择"); return; }
  var toOp = candidates[idx];
  var toP = parseBadge(toOp);
  var toLabel = toP.name ? toP.id + " " + toP.name : toOp;

  if(!confirm("确认将负责人交接给 " + toLabel + " ？\n\n交接后对方将成为该趟次负责人。")) return;

  try{
    setStatus("正在交接负责人... ⏳", true);
    var res = await jsonp(LOCK_URL, {
      action: "session_transfer_owner",
      session: currentSessionId,
      from_operator_id: curOp,
      to_operator_id: toOp,
      operator_id: curOp
    }, { skipBusy: false });
    if(!res || !res.ok){
      var errMsg = "交接失败";
      if(res && res.error === "owner_mismatch") errMsg = "负责人已变更为 " + (res.current_owner || "?") + "，请刷新重试";
      else if(res && res.error === "to_operator_not_active") errMsg = "目标人员已不在作业中";
      else if(res && res.error === "to_operator_has_open_session") errMsg = "目标人员已有其他未结束趟次（" + (res.open_biz||"") + "/" + (res.open_task||"") + "）";
      else if(res && res.error === "already_owner") errMsg = "目标已是当前负责人";
      else if(res && res.error) errMsg += "：" + res.error;
      setStatus("交接失败 ❌", false);
      alert(errMsg);
      return;
    }
    // 成功：刷新
    _sessionOwnerInfo = {
      created_by_operator: _sessionOwnerInfo.created_by_operator,
      owner_operator_id: toOp,
      owner_changed_at: res.owner_changed_at || Date.now(),
      owner_changed_by: curOp
    };
    updateB2bOwnerDisplay_();
    fetchOperatorOpenSessions();
    setStatus("负责人已交接给 " + toLabel + " ✅", false);
    alert("负责人已交接给 " + toLabel);
  }catch(e){
    setStatus("交接失败（网络错误）❌", false);
    alert("交接失败，请检查网络后重试");
  }
}

function renderB2bBindCard_(x){
  var b = b2bWorkorderBindings[x];
  var rk = b2bResultKeyFromBinding_(x);
  var r = rk ? b2bWorkorderResults[rk] : null;

  // 无 binding 信息
  if(!b || !b.day_kst || !b.source_type){
    // 尚在加载中：显示占位，不显示错误
    if(!_b2bBindingsLoaded){
      return '<div style="background:#f5f5f5;border:1px solid #e0e0e0;border-radius:8px;padding:8px 10px;margin:4px 0;">' +
        '<div style="font-size:14px;font-weight:600;">' + esc(String(x)) + '</div>' +
        '<div style="font-size:11px;color:#888;margin-top:3px;">加载绑定信息中...</div>' +
      '</div>';
    }
    // 已加载完但仍缺失：触发一次自愈重建
    if(!_b2bSelfHealPending){
      _b2bSelfHealPending = true;
      _b2bBindingsLoaded = false;
      setTimeout(function(){ loadB2bBindings(); }, 0);
      return '<div style="background:#f5f5f5;border:1px solid #e0e0e0;border-radius:8px;padding:8px 10px;margin:4px 0;">' +
        '<div style="font-size:14px;font-weight:600;">' + esc(String(x)) + '</div>' +
        '<div style="font-size:11px;color:#888;margin-top:3px;">正在自动恢复绑定信息...</div>' +
      '</div>';
    }
    // 自愈已尝试仍失败：显示错误
    return '<div style="background:#fff0f0;border:1px solid #ef9a9a;border-radius:8px;padding:8px 10px;margin:4px 0;">' +
      '<div style="font-size:14px;font-weight:600;">' + esc(String(x)) + '</div>' +
      '<div style="font-size:11px;color:#c62828;margin-top:3px;">绑定信息缺失，请刷新重试</div>' +
      '<div style="margin-top:5px;"><button onclick="unbindB2bWorkorder(\''+esc(x)+'\')" style="font-size:11px;padding:3px 10px;border-radius:4px;cursor:pointer;background:#fff;color:#c62828;border:1px solid #ef9a9a;">解绑</button></div>' +
    '</div>';
  }

  // 类型标签
  var typeTag;
  if(b.source_type === "internal_b2b_workorder"){
    typeTag = '<span style="display:inline-block;background:#c8e6c9;color:#2e7d32;font-size:10px;padding:1px 6px;border-radius:3px;margin-right:4px;">🏠 本系统工单</span>';
  } else if(b.source_type === "external_wms_workorder"){
    typeTag = '<span style="display:inline-block;background:#fff3e0;color:#e65100;font-size:10px;padding:1px 6px;border-radius:3px;margin-right:4px;">📦 外部WMS工单</span>';
  } else {
    typeTag = '<span style="display:inline-block;background:#e0e0e0;color:#555;font-size:10px;padding:1px 6px;border-radius:3px;margin-right:4px;">📝 待确认</span>';
  }

  // 跨天绑定日标签
  var dayTag = "";
  if(b.day_kst !== kstDayKey_(Date.now())){
    dayTag = '<span style="display:inline-block;background:#fff3e0;color:#e65100;font-size:10px;padding:1px 6px;border-radius:3px;margin-right:4px;">绑定日:'+esc(b.day_kst)+'</span>';
  }

  // 结果状态标签
  var statusTag;
  if(r && r.status === "completed"){
    statusTag = '<span style="display:inline-block;background:#e8f5e9;color:#2e7d32;font-size:10px;padding:1px 6px;border-radius:3px;">已完成</span>';
  } else if(r){
    statusTag = '<span style="display:inline-block;background:#fff8e1;color:#f57c00;font-size:10px;padding:1px 6px;border-radius:3px;">草稿</span>';
  } else {
    statusTag = '<span style="display:inline-block;background:#f5f5f5;color:#999;font-size:10px;padding:1px 6px;border-radius:3px;">未录结果</span>';
  }

  // 工单号行
  var line1 = '<div style="display:flex;align-items:center;flex-wrap:wrap;gap:3px;">' + typeTag + dayTag + statusTag + '</div>';
  var line2 = '<div style="font-size:14px;font-weight:600;margin-top:3px;">' + esc(String(x)) + '</div>';

  // 工单摘要（内部工单的数量信息）
  var qtyLine = "";
  if(b.source_type === "internal_b2b_workorder" && b.wo_summary && b.wo_summary.qty_text){
    qtyLine = '<div style="font-size:11px;color:#666;margin-top:2px;">计划: '+esc(b.wo_summary.qty_text)+'</div>';
  }
  if(b.source_type === "external_wms_workorder" && !r){
    qtyLine = '<div style="font-size:11px;color:#e65100;margin-top:2px;">待WMS匹配</div>';
  }

  // 结果摘要
  var resultLine = "";
  if(r){
    resultLine = '<div style="font-size:11px;color:#444;margin-top:3px;line-height:1.4;">' + esc(fmtResultSummary(r)) + '</div>';
  } else {
    resultLine = '<div style="font-size:11px;color:#bbb;margin-top:3px;">暂无结果记录</div>';
  }

  // 操作按钮
  var btnLine = '<div style="display:flex;gap:6px;margin-top:5px;">';
  btnLine += '<button onclick="openResultForm(\''+esc(x)+'\')" style="font-size:11px;padding:3px 10px;border-radius:4px;cursor:pointer;background:#1976d2;color:#fff;border:none;">'+(r?'编辑结果':'录入结果')+'</button>';
  btnLine += '<button onclick="unbindB2bWorkorder(\''+esc(x)+'\')" style="font-size:11px;padding:3px 10px;border-radius:4px;cursor:pointer;background:#fff;color:#c62828;border:1px solid #ef9a9a;">解绑</button>';
  btnLine += '</div>';

  // 卡片背景色
  var cardBg = "#fff";
  if(r && r.status === "completed") cardBg = "#f9fdf9";
  else if(r) cardBg = "#fffdf5";

  return '<div style="background:'+cardBg+';border:1px solid #e0e0e0;border-radius:8px;padding:8px 10px;margin:4px 0;">' +
    line1 + line2 + qtyLine + resultLine + btnLine + '</div>';
}

/** ===== B2B Simple Mode Functions ===== */

// --- Session auto-start: 扫工单时自动创建/复用 session ---
async function smEnsureSession_(){
  // 已有 B2B工单操作 session → 复用
  var sid = getSess_("B2B", "B2B工单操作");
  if(sid){
    currentSessionId = sid;
    CUR_CTX = { biz:"B2B", task:"B2B工单操作", page:"b2b_workorder_simple" };
    _smIsSimpleMode = true;
    return sid;
  }
  // 查服务端是否有 open session
  var opId = getOperatorId();
  if(opId){
    try{
      var osRes = await jsonp(LOCK_URL, { action:"operator_open_sessions", operator_id: opId }, { skipBusy:true });
      if(osRes && osRes.ok){
        var sessions = osRes.sessions || [];
        for(var i=0;i<sessions.length;i++){
          if(sessions[i].biz==="B2B" && sessions[i].task==="B2B工单操作"){
            sid = sessions[i].session;
            currentSessionId = sid;
            CUR_CTX = { biz:"B2B", task:"B2B工单操作", page:"b2b_workorder_simple" };
            setSess_("B2B","B2B工单操作",sid);
            _smIsSimpleMode = true;
            return sid;
          }
        }
      }
    }catch(e){}
  }
  // 无 session → 自动创建
  var newSid = makePickSessionId();
  var evId = makeEventId({ event:"start", biz:"B2B", task:"B2B工单操作", wave_id:"", badgeRaw:"" });
  await submitEventSync_({ event:"start", event_id: evId, biz:"B2B", task:"B2B工单操作", pick_session_id: newSid }, true);
  addRecent(evId);
  currentSessionId = newSid;
  CUR_CTX = { biz:"B2B", task:"B2B工单操作", page:"b2b_workorder_simple" };
  setSess_("B2B","B2B工单操作",newSid);
  _smIsSimpleMode = true;
  // 初始化旧模式共享状态
  scannedB2bWorkorders = new Set(); activeB2bWorkorder = new Set();
  b2bWorkorderBindings = {}; _b2bBindingsLoaded = false; _b2bSelfHealPending = false;
  b2bWorkorderResults = {}; _sessionOwnerInfo = null;
  persistState();
  setStatus(smtz_("session_auto_created") + " ✅ " + newSid, true);
  fetchOperatorOpenSessions();
  return newSid;
}

// --- 页面初始化 ---
function smInitPage_(){
  _smIsSimpleMode = true;
  _smBindingInFlight = false;
  CUR_CTX = { biz:"B2B", task:"B2B工单操作", page:"b2b_workorder_simple" };
  var sid = getSess_("B2B","B2B工单操作");
  if(sid){
    currentSessionId = sid;
    loadB2bBindings(function(){
      // bindings 加载完成后恢复当前工单
      _smRestoreCurrentOrder_();
      smRender_();
    });
    loadB2bResults();
    smLoadLabor_();
    // 先立即尝试恢复（可能 scannedB2bWorkorders 还有旧值）
    _smRestoreCurrentOrder_();
  }
  smRender_();
}

// --- 扫工单 ---
async function smScanWorkorder(){
  if(!acquireBusy_()) return;
  try{
    await smEnsureSession_();
    _b2bPendingCode = null; _b2bPendingTime = 0;
    scanMode = "b2b_workorder_simple";
    document.getElementById("scanTitle").textContent = "扫码工单号 / 작업지시 스캔";
    await openScannerCommon();
    updateScanRecentList_();
  }catch(e){
    setStatus("创建趟次失败: " + e, false);
    alert("创建趟次失败: " + e);
  }finally{ releaseBusy_(); }
}

// 处理简化模式扫到的工单码
function smHandleWorkorderScan_(code){
  var codeW = code.trim().toUpperCase();
  if(!codeW) return;
  if(!isValidB2bWorkorderCode_(codeW)){
    showScanFeedback_("不是有效工单号 / 유효하지 않은 작업지시: " + codeW, "#fff0f0", "#c00", 1500);
    return;
  }
  // 已绑定 → 切换为当前工单
  if(scannedB2bWorkorders.has(codeW)){
    _smCurrentOrder = codeW;
    _smPersistCurrentOrder_();
    showScanFeedback_(smtz_("already_bound") + ": " + codeW, "#fffbe6", "#b8860b", 1200);
    smRender_();
    return;
  }
  // 二次确认
  var nowW = Date.now();
  if(_b2bPendingCode !== codeW){
    _b2bPendingCode = codeW; _b2bPendingTime = nowW;
    setStatus("已识别 " + codeW + "，等待确认...", true);
    return;
  }
  if(nowW - _b2bPendingTime > B2B_CONFIRM_WINDOW_MS){
    _b2bPendingTime = nowW;
    setStatus("已识别 " + codeW + "，等待确认...", true);
    return;
  }
  // 确认通过
  _b2bPendingCode = null; _b2bPendingTime = 0;
  _b2bCooldownUntil = nowW + B2B_SUCCESS_COOLDOWN_MS;
  scannedB2bWorkorders.add(codeW); persistState();
  _smCurrentOrder = codeW;
  _smPersistCurrentOrder_();
  _smBindingInFlight = true;
  var evIdW = makeEventId({ event:"wave", biz:"B2B", task:"B2B工单操作", wave_id: codeW, badgeRaw:"" });
  if(!hasRecent(evIdW)){
    submitEvent({ event:"wave", event_id: evIdW, biz:"B2B", task:"B2B工单操作", pick_session_id: currentSessionId, wave_id: codeW });
    addRecent(evIdW);
  }
  _smCallBindWithLock_(codeW);
  showScanFeedback_("⏳ 绑定中 " + codeW + "...", "#e3f2fd", "#1565c0", 1500);
  smRender_();
  try{ if(navigator.vibrate) navigator.vibrate(80); }catch(e){}
  updateScanRecentList_();
}

// 手动添加工单
function smManualAddWorkorder(){
  var inp = document.getElementById("smManualInput");
  if(!inp) return;
  var val = String(inp.value || "").trim();
  if(!val){ alert("请输入工单号 / 작업지시 번호를 입력하세요"); return; }
  // 自动确保 session
  if(!currentSessionId){
    smEnsureSession_().then(function(){
      _smDoManualAdd(val, inp);
    }).catch(function(e){ alert("创建趟次失败: " + e); });
    return;
  }
  _smDoManualAdd(val, inp);
}
function _smDoManualAdd(val, inp){
  if(scannedB2bWorkorders.has(val)){
    _smCurrentOrder = val;
    _smPersistCurrentOrder_();
    smRender_();
    alert(smtz_("already_bound") + "\n" + val);
    inp.value = ""; return;
  }
  scannedB2bWorkorders.add(val); persistState();
  _smCurrentOrder = val;
  _smPersistCurrentOrder_();
  _smBindingInFlight = true;
  var evIdW = makeEventId({ event:"wave", biz:"B2B", task:"B2B工单操作", wave_id: val, badgeRaw:"" });
  if(!hasRecent(evIdW)){
    submitEvent({ event:"wave", event_id: evIdW, biz:"B2B", task:"B2B工单操作", pick_session_id: currentSessionId, wave_id: val });
    addRecent(evIdW);
  }
  _smCallBindWithLock_(val);
  smRender_();
  inp.value = "";
}

// 简化模式专用绑定 — 带 _smBindingInFlight 锁 + 细化错误提示
function _smCallBindWithLock_(orderNo){
  callB2bOpBind(orderNo);
  // callB2bOpBind 是 fire-and-forget，需要 hook 它的 resolve/reject 清除锁
  // 因为 callB2bOpBind 内部用 jsonp().then/catch 处理，我们在下一个微任务后轮询 bindings
  // 更好的方式：监听 _b2bBindingsLoaded + bindings[orderNo] 是否出现
  var checkCount = 0;
  var checkTimer = setInterval(function(){
    checkCount++;
    if(b2bWorkorderBindings[orderNo] || checkCount > 50){ // 最多 5 秒
      _smBindingInFlight = false;
      clearInterval(checkTimer);
      smRender_();
    }
    // 绑定失败（被 callB2bOpBind 回滚 — scannedB2bWorkorders 里已没有）
    if(!scannedB2bWorkorders.has(orderNo)){
      _smBindingInFlight = false;
      clearInterval(checkTimer);
      setStatus("⚠️ 工单绑定失败，请重新扫码: " + orderNo, false);
      smRender_();
    }
  }, 100);
}

// --- 扫人员 ---
async function smScanWorker(){
  if(!currentSessionId){ alert("趟次丢失，请刷新页面 / 세션 없음, 새로고침하세요"); return; }
  if(_smBindingInFlight){ alert("工单绑定中，请稍候... / 바인딩 진행 중..."); return; }
  // 兜底：只有 1 张工单且未选中 → 自动选中
  if(!_smCurrentOrder && scannedB2bWorkorders.size === 1){
    _smCurrentOrder = Array.from(scannedB2bWorkorders)[0];
    _smPersistCurrentOrder_();
    smRender_();
  }
  if(!_smCurrentOrder){ alert("请先扫工单 / 먼저 작업지시를 스캔하세요"); return; }
  scanMode = "b2b_simple_worker";
  document.getElementById("scanTitle").textContent = smtz_("scan_worker_title");
  await openScannerCommon();
}

// 处理简化模式扫到的人员工牌
async function smHandleWorkerScan_(code){
  if(!isOperatorBadge(code)){
    setStatus("无效工牌 / 유효하지 않은 명찰: " + code, false);
    return;
  }
  var p = parseBadge(code);
  if(!currentSessionId){
    setStatus("趟次丢失，请刷新页面 / 세션 없음", false);
    return;
  }
  if(_smBindingInFlight){
    setStatus("工单绑定中，请稍候... / 바인딩 진행 중...", false);
    return;
  }
  // 兜底：只有 1 张工单且未选中 → 自动选中
  if(!_smCurrentOrder && scannedB2bWorkorders.size === 1){
    _smCurrentOrder = Array.from(scannedB2bWorkorders)[0];
    _smPersistCurrentOrder_();
  }
  if(!_smCurrentOrder){
    setStatus("请先扫工单 / 먼저 작업지시를 스캔하세요", false);
    return;
  }
  if(!b2bWorkorderBindings[_smCurrentOrder]){
    setStatus("工单 " + _smCurrentOrder + " 绑定未完成，请稍候重试", false);
    return;
  }
  scanBusy = true;
  try{
    // 1. 主系统 join（如果还没在 task 中）
    var evId = makeEventId({ event:"join", biz:"B2B", task:"B2B工单操作", wave_id:"", badgeRaw: p.raw });
    if(!hasRecent(evId)){
      var res = await submitEventSync_({ event:"join", event_id: evId, biz:"B2B", task:"B2B工单操作", pick_session_id: currentSessionId, badge: p.raw }, true);
      addRecent(evId);
      // 更新本地 active list
      var reg = taskReg_("B2B工单操作");
      if(reg){ var s = reg.get(); s.add(p.raw); reg.set(s); renderActiveLists(); }
    }
    // 2. labor detail join
    var laborRes = await jsonp(LOCK_URL, {
      action:"b2b_simple_labor_join",
      session_id: currentSessionId,
      source_order_no: _smCurrentOrder,
      operator_badge: p.id,
      operator_name: p.name || "",
      entry_mode: "simple_mode"
    }, { skipBusy:true });
    if(!laborRes || !laborRes.ok){
      var errMsg = (laborRes && laborRes.error) || "unknown";
      if(errMsg === "workorder_not_bound") errMsg = "工单绑定未完成 / 바인딩 미완료";
      setStatus("labor加入失败: " + errMsg, false);
      return;
    }
    showScanFeedback_((p.name||p.id) + " " + smtz_("worker_joined"), "#e6ffe6", "#006400", 1500);
    smLoadLabor_();
    smRender_();
    try{ if(navigator.vibrate) navigator.vibrate(80); }catch(e){}
  }catch(e){
    setStatus("网络错误，加入失败: " + e, false);
  }finally{
    scanBusy = false;
  }
}

// --- 暂时完成 ---
async function smTempComplete(){
  if(!currentSessionId){ alert("趟次丢失，请刷新页面 / 세션 없음, 새로고침하세요"); return; }
  if(_smBindingInFlight){ alert("工单绑定中，请稍候... / 바인딩 진행 중..."); return; }
  if(!_smCurrentOrder){ alert("请先扫工单 / 먼저 작업지시를 스캔하세요"); return; }
  if(!confirm(smtz_("temp_complete_confirm"))) return;
  if(!acquireBusy_()) return;
  try{
    setStatus("暂时完成处理中... ⏳", true);
    var res = await jsonp(LOCK_URL, {
      action:"b2b_simple_temp_complete",
      session_id: currentSessionId,
      source_order_no: _smCurrentOrder,
      operator_id: getOperatorId()
    });
    if(!res || !res.ok){
      alert("暂时完成失败: " + (res&&res.error||"unknown"));
      return;
    }
    setStatus(smtz_("temp_complete") + " ✅ " + _smCurrentOrder, true);
    // 刷新数据
    smLoadLabor_();
    loadB2bResults();
    smRender_();
  }catch(e){
    alert("暂时完成失败: " + e);
  }finally{ releaseBusy_(); }
}

// --- 录结果（复用现有结果单表单）---
function smOpenResultForCurrent(){
  if(_smBindingInFlight){ alert("工单绑定中，请稍候... / 바인딩 진행 중..."); return; }
  if(!_smCurrentOrder){ alert("请先扫工单 / 먼저 작업지시를 스캔하세요"); return; }
  // 确保 bindings 已加载
  if(!b2bWorkorderBindings[_smCurrentOrder]){
    alert("工单绑定信息未加载，请稍后重试 / 바인딩 정보 미로드");
    return;
  }
  // 复用现有结果单表单
  openResultForm(_smCurrentOrder);
}

// --- 结束工单（扫确认工牌 → completed）---
async function smConfirmWorkorder(){
  if(!_smCurrentOrder){ alert(smt_("no_current_order")); return; }
  var binding = b2bWorkorderBindings[_smCurrentOrder];
  if(!binding || !binding.day_kst){
    alert("工单绑定信息缺失 / 바인딩 정보 없음");
    return;
  }
  // 检查是否有结果（至少要有 pending_review 或 draft）
  var rk = b2bResultKeyFromBinding_(_smCurrentOrder);
  var r = rk ? b2bWorkorderResults[rk] : null;
  if(!r){
    alert("请先录入结果单 / 먼저 결과를 입력하세요");
    return;
  }
  // 弹出扫工牌确认层
  _smShowConfirmLayer(_smCurrentOrder, binding.day_kst);
}

function _smShowConfirmLayer(orderNo, dayKst){
  var modal = document.getElementById("b2bResultModal");
  var body = document.getElementById("b2bResultBody");
  if(!modal || !body) return;
  modal.style.display = "flex";
  window._smConfirmOrderNo = orderNo;
  window._smConfirmDayKst = dayKst;
  window._smConfirmBadgeRaw = "";

  body.innerHTML = '<div style="text-align:center;padding:20px 0;">' +
    '<div style="font-size:18px;font-weight:800;margin-bottom:12px;">🔒 ' + smt_("confirm_badge_title") + '</div>' +
    '<div style="font-size:13px;color:#555;margin-bottom:12px;">工单 / 작업지시: <b>'+esc(orderNo)+'</b></div>' +
    '<div style="font-size:13px;color:#c00;margin-bottom:16px;">' + smt_("confirm_badge_hint") + '</div>' +
    '<input id="sm-confirm-badge" type="text" readonly placeholder="等待扫描... / 스캔 대기..." ' +
      'style="width:90%;font-size:18px;padding:12px;text-align:center;border:2px solid #6a1b9a;border-radius:8px;margin-bottom:12px;background:#f5f5f5;" />' +
    '<button onclick="_smOpenConfirmScanner()" style="width:90%;padding:12px;font-size:15px;background:#6a1b9a;color:#fff;border:none;border-radius:8px;margin-bottom:12px;cursor:pointer;">📷 开始扫描 / 스캔 시작</button>' +
    '<div style="display:flex;gap:8px;justify-content:center;">' +
      '<button onclick="_smCancelConfirm()" style="flex:1;padding:10px;font-size:14px;">取消 / 취소</button>' +
      '<button onclick="_smDoConfirm()" style="flex:1;padding:10px;font-size:14px;background:#6a1b9a;color:#fff;border:none;border-radius:6px;">确认完成 / 확인</button>' +
    '</div>' +
  '</div>';
}

async function _smOpenConfirmScanner(){
  scanMode = "b2b_simple_confirm_badge";
  await openScannerCommon();
}

function _smCancelConfirm(){
  window._smConfirmOrderNo = null;
  window._smConfirmBadgeRaw = "";
  var modal = document.getElementById("b2bResultModal");
  if(modal) modal.style.display = "none";
}

async function _smDoConfirm(){
  var badgeRaw = window._smConfirmBadgeRaw || "";
  if(!badgeRaw){
    alert("请先扫描职员工牌 / 먼저 직원 명찰을 스캔하세요");
    return;
  }
  var bp = parseBadge(badgeRaw);
  if(!isEmpId(bp.id)){
    alert("工牌格式无效，必须是职员工牌（EMP-...）/ 직원 명찰(EMP-...) 필수");
    return;
  }
  var orderNo = window._smConfirmOrderNo;
  var dayKst = window._smConfirmDayKst;
  if(!orderNo || !dayKst){ alert("数据丢失，请重新操作"); _smCancelConfirm(); return; }

  // 加载最新结果
  try{
    var getRes = await jsonp(LOCK_URL, { action:"b2b_op_result_get", day_kst: dayKst, source_order_no: orderNo }, { skipBusy:true });
    if(!getRes || !getRes.ok){ alert("加载结果失败"); return; }
    var r = getRes.result || {};

    // 用现有数据 + confirmed + reviewed 一起提交 completed
    var payload = {
      action: "b2b_op_result_upsert",
      day_kst: dayKst,
      source_order_no: orderNo,
      session_id: currentSessionId || "",
      operation_mode: r.operation_mode || "pack_outbound",
      sku_kind_count: r.sku_kind_count || 0,
      packed_qty: r.packed_qty || 0,
      box_count: r.box_count || 0,
      packed_box_count: r.packed_box_count || 0,
      used_carton: r.used_carton || 0,
      big_carton_count: r.big_carton_count || 0,
      small_carton_count: r.small_carton_count || 0,
      pallet_count: r.pallet_count || 0,
      label_count: r.label_count || 0,
      photo_count: r.photo_count || 0,
      has_pallet_detail: r.has_pallet_detail || 0,
      did_pack: r.did_pack || 0,
      did_rebox: r.did_rebox || 0,
      rebox_count: r.rebox_count || 0,
      needs_forklift_pick: r.needs_forklift_pick || 0,
      forklift_pallet_count: r.forklift_pallet_count || 0,
      rack_pick_location_count: r.rack_pick_location_count || 0,
      remark: r.remark || "",
      status: "completed",
      created_by: r.created_by || getOperatorId() || "",
      confirm_badge: bp.id,
      confirmed_by: bp.name || bp.id,
      // simple mode extended fields
      reviewed_by_badge: bp.id,
      reviewed_by_name: bp.name || bp.id,
      workflow_status: "completed"
    };
    var uRes = await jsonp(LOCK_URL, payload, { skipBusy:true });
    if(!uRes || !uRes.ok){
      alert("确认失败: " + (uRes&&uRes.error||"unknown"));
      return;
    }
    _smCancelConfirm();
    setStatus(smtz_("wo_confirmed") + " ✅ " + orderNo, true);
    loadB2bResults();
    smRender_();
  }catch(e){
    alert("确认失败: " + e);
  }
}

// --- 离开本环节 ---
async function smLeaveTask(){
  if(!confirm(smt_("leave_confirm"))) return;
  if(!acquireBusy_()) return;
  try{
    var opId = getOperatorId();
    if(!opId){ alert("未设置操作员"); return; }
    // 1. 关闭所有 active labor details
    if(currentSessionId){
      await jsonp(LOCK_URL, {
        action:"b2b_simple_labor_leave",
        session_id: currentSessionId,
        operator_badge: opId
      }, { skipBusy:true });
    }
    // 2. 主系统 leave
    await leaveWork_("B2B","B2B工单操作");
    _smClearCurrentOrder_();
    _smIsSimpleMode = false;
    _smBindingInFlight = false;
  }catch(e){
    setStatus("离开失败: " + e, false);
  }finally{ releaseBusy_(); }
}

// --- 结束趟次 ---
async function smEndSession(){
  if(!currentSessionId){ setStatus(smt_("no_session"), false); return; }
  if(!acquireBusy_()) return;
  try{
    // 简化模式关闭：传 simple_mode=1
    var res = await jsonp(LOCK_URL, {
      action: "session_close",
      session: currentSessionId,
      operator_id: getOperatorId(),
      simple_mode: "1"
    });
    if(!res || res.ok !== true){ alert("关闭失败: " + (res&&res.error||"")); return; }
    if(res.blocked){
      if(res.reason === "still_active"){
        alert("还有人员未退出 / 아직 퇴장하지 않은 작업자가 있습니다\n\n" + formatActiveListForAlert_(res.active));
      } else if(res.reason === "unpaired_joins"){
        alert("有未配对的 join / 매칭되지 않은 참여자\n\n" + (res.badges||[]).join(", "));
      } else if(res.reason === "working_b2b_orders"){
        alert(smtz_("end_blocked_working") + "\n\n" + (res.pending_orders||[]).map(function(o){ return o.source_order_no; }).join(", "));
      } else {
        alert("无法结束: " + (res.reason||""));
      }
      return;
    }
    if(res.already_closed){
      alert("该趟次已结束 / 이미 종료된 세션");
    }
    // 写 end 事件
    var evId = makeEventId({ event:"end", biz:"B2B", task:"SESSION", wave_id:"", badgeRaw:"" });
    if(!hasRecent(evId)){
      try{
        await submitEventSync_({ event:"end", event_id: evId, biz:"B2B", task:"SESSION", pick_session_id: currentSessionId }, true);
      }catch(e){
        submitEvent({ event:"end", event_id: evId, biz:"B2B", task:"SESSION", pick_session_id: currentSessionId });
      }
      addRecent(evId);
    }
    setStatus("趟次已结束 / 세션 종료됨 ✅", true);
    _smClearCurrentOrder_();
    _smLabor = []; _smWorkorderStatuses = {};
    _smIsSimpleMode = false;
    _smBindingInFlight = false;
    cleanupLocalSession_();
    smRender_();
  }catch(e){
    setStatus("结束失败: " + e, false);
    alert("结束失败: " + e);
  }finally{ releaseBusy_(); }
}

// --- 加载 labor details ---
function smLoadLabor_(){
  if(!currentSessionId) return;
  jsonp(LOCK_URL, { action:"b2b_simple_labor_list", session_id: currentSessionId }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok) return;
    _smLabor = res.labor || [];
    smRender_();
  }).catch(function(e){ console.error("smLoadLabor_ error", e); });
}

// --- 渲染 ---
function smRender_(){
  // 顶部状态
  var elSid = document.getElementById("smSessionId");
  var elOwner = document.getElementById("smOwner");
  var elCurOrder = document.getElementById("smCurrentOrder");
  var elCurStatus = document.getElementById("smCurrentStatus");
  var elWorkerCount = document.getElementById("smWorkerCount");
  var elTotalMin = document.getElementById("smTotalMinutes");
  if(elSid) elSid.textContent = currentSessionId || "-";
  if(elOwner) elOwner.textContent = (_sessionOwnerInfo && _sessionOwnerInfo.owner_operator_id) || getOperatorId() || "-";
  if(elCurOrder) elCurOrder.textContent = _smCurrentOrder || "-";

  // 当前工单 workflow_status
  var curWf = "";
  if(_smCurrentOrder){
    var rk = b2bResultKeyFromBinding_(_smCurrentOrder);
    var r = rk ? b2bWorkorderResults[rk] : null;
    curWf = (r && r.workflow_status) || (r && r.status === "completed" ? "completed" : (r ? "pending_result" : ""));
  }
  if(elCurStatus){
    if(curWf){
      elCurStatus.innerHTML = '<span style="color:'+smWfColor_(curWf)+';font-weight:700;">'+smWfLabel_(curWf)+'</span>';
    } else {
      elCurStatus.textContent = "-";
    }
  }

  // 当前工单参与人数
  var activeLabor = _smLabor.filter(function(l){ return l.source_order_no === _smCurrentOrder && l.status === "active"; });
  if(elWorkerCount) elWorkerCount.textContent = String(activeLabor.length);

  // 累计工时
  var totalMin = 0;
  var curLabor = _smLabor.filter(function(l){ return l.source_order_no === _smCurrentOrder; });
  var now = Date.now();
  curLabor.forEach(function(l){
    if(l.duration_minutes) totalMin += l.duration_minutes;
    else if(l.status === "active" && l.join_ms) totalMin += (now - l.join_ms) / 60000;
  });
  if(elTotalMin) elTotalMin.textContent = totalMin > 0 ? Math.round(totalMin) + " min" : "-";

  // 参与人员列表
  var elLaborList = document.getElementById("smLaborList");
  if(elLaborList){
    if(activeLabor.length === 0){
      elLaborList.innerHTML = '<span class="muted">' + smt_("no_worker") + '</span>';
    } else {
      var lhtml = "";
      activeLabor.forEach(function(l){
        var mins = l.join_ms ? Math.round((now - l.join_ms)/60000) : 0;
        var t0 = l.join_ms ? new Date(l.join_ms).toTimeString().slice(0,5) : "-";
        lhtml += '<div style="display:flex;justify-content:space-between;padding:2px 0;border-bottom:1px solid #eee;">' +
          '<span>'+(l.operator_name||l.operator_badge)+'</span>' +
          '<span style="color:#555;font-size:11px;">'+t0+' · '+mins+' min · <b style="color:#2e7d32;">'+smtz_("active")+'</b></span></div>';
      });
      elLaborList.innerHTML = lhtml;
    }
  }

  // 工单列表 — 按 workflow_status 分组
  var elOrderList = document.getElementById("smOrderList");
  var elOrderTotal = document.getElementById("smOrderTotal");
  var allOrders = Array.from(scannedB2bWorkorders);
  if(elOrderTotal) elOrderTotal.textContent = "(" + allOrders.length + ")";

  if(elOrderList){
    if(allOrders.length === 0){
      elOrderList.innerHTML = '<span class="muted">无 / 없음</span>';
    } else {
      // 分组
      var groups = { working:[], pending_result:[], pending_review:[], completed:[], other:[] };
      allOrders.forEach(function(orderNo){
        var rk = b2bResultKeyFromBinding_(orderNo);
        var r = rk ? b2bWorkorderResults[rk] : null;
        var wf = (r && r.workflow_status) || "";
        if(!wf){
          // 推断 workflow_status from status
          if(r && r.status === "completed") wf = "completed";
          else if(r && r.status === "draft") wf = "pending_result";
          else wf = "other";
        }
        if(groups[wf]) groups[wf].push(orderNo);
        else groups.other.push(orderNo);
      });

      var sections = [
        { key:"working", label:smt_("working"), color:"#e65100", items:groups.working },
        { key:"pending_result", label:smt_("pending_result"), color:"#f57f17", items:groups.pending_result },
        { key:"pending_review", label:smt_("pending_review"), color:"#6a1b9a", items:groups.pending_review },
        { key:"completed", label:smt_("completed"), color:"#2e7d32", items:groups.completed },
        { key:"other", label:"其他 / 기타", color:"#999", items:groups.other }
      ];

      var html = "";
      sections.forEach(function(sec){
        if(!sec.items.length) return;
        html += '<div style="font-size:12px;font-weight:700;color:'+sec.color+';margin:8px 0 4px;border-bottom:1px solid #eee;padding-bottom:2px;">'+sec.label+'（'+sec.items.length+'）</div>';
        sec.items.forEach(function(orderNo){
          var isCurrent = orderNo === _smCurrentOrder;
          var bg = isCurrent ? "#e3f2fd" : "#fff";
          var border = isCurrent ? "#42a5f5" : "#e0e0e0";
          var b = b2bWorkorderBindings[orderNo] || {};
          var rk = b2bResultKeyFromBinding_(orderNo);
          var r = rk ? b2bWorkorderResults[rk] : null;

          html += '<div onclick="_smSelectOrder(\''+esc(orderNo)+'\')" style="background:'+bg+';border:1px solid '+border+';border-radius:8px;padding:8px 10px;margin:3px 0;cursor:pointer;">';
          html += '<div style="font-size:13px;font-weight:600;">'+esc(orderNo);
          if(isCurrent) html += ' <span style="color:#1565c0;font-size:11px;">◀ 当前</span>';
          html += '</div>';
          // 摘要
          if(b.wo_summary && b.wo_summary.qty_text){
            html += '<div style="font-size:11px;color:#666;">'+esc(b.wo_summary.qty_text)+'</div>';
          }
          if(r){
            html += '<div style="font-size:11px;color:#444;">'+esc(fmtResultSummary(r))+'</div>';
          }
          // 操作按钮
          html += '<div style="display:flex;gap:4px;margin-top:4px;">';
          html += '<button onclick="event.stopPropagation();_smSelectOrder(\''+esc(orderNo)+'\');smOpenResultForCurrent()" style="font-size:10px;padding:2px 8px;border-radius:3px;background:#1976d2;color:#fff;border:none;cursor:pointer;">录结果</button>';
          var wfSt = (r && r.workflow_status) || (r && r.status === "completed" ? "completed" : "");
          if(wfSt !== "completed"){
            html += '<button onclick="event.stopPropagation();_smSelectOrder(\''+esc(orderNo)+'\');smConfirmWorkorder()" style="font-size:10px;padding:2px 8px;border-radius:3px;background:#6a1b9a;color:#fff;border:none;cursor:pointer;">确认完成</button>';
          }
          html += '</div>';
          html += '</div>';
        });
      });
      elOrderList.innerHTML = html;
    }
  }

  // 返回原工单按钮
  var btnReturn = document.getElementById("btnSmReturnFromTemp");
  if(btnReturn){
    btnReturn.style.display = (typeof _tempSwitchCtx !== "undefined" && _tempSwitchCtx) ? "" : "none";
  }
}

function _smSelectOrder(orderNo){
  _smCurrentOrder = orderNo;
  _smPersistCurrentOrder_();
  smRender_();
}
function _smPersistCurrentOrder_(){
  if(currentSessionId && _smCurrentOrder){
    try{ localStorage.setItem("sm_current_order_" + currentSessionId, _smCurrentOrder); }catch(e){}
  }
}
function _smRestoreCurrentOrder_(){
  if(!currentSessionId) return;
  var saved = localStorage.getItem("sm_current_order_" + currentSessionId);
  if(saved && scannedB2bWorkorders.has(saved)){
    _smCurrentOrder = saved;
    return;
  }
  // 兜底：只有 1 张工单时自动选中
  if(!_smCurrentOrder && scannedB2bWorkorders.size === 1){
    _smCurrentOrder = Array.from(scannedB2bWorkorders)[0];
    _smPersistCurrentOrder_();
  }
}
function _smClearCurrentOrder_(){
  if(currentSessionId){
    try{ localStorage.removeItem("sm_current_order_" + currentSessionId); }catch(e){}
  }
  _smCurrentOrder = null;
}

function startPicking(e){ startGeneric_(e, "B2C", "拣货", "b2c_pick", function(){ scannedWaves = new Set(); activePick = new Set(); leaderPickOk = false; localStorage.setItem("leader_pick_ok", "0"); syncLeaderPickUI(); }); }

function setRelabelTimerText(text){
  var el = document.getElementById("relabelTimer");
  if(el) el.textContent = text;
}
function startRelabelTimer(){
  if(relabelTimerHandle) clearInterval(relabelTimerHandle);
  relabelTimerHandle = setInterval(function(){
    if(!relabelStartTs) return;
    var sec = Math.floor((Date.now() - relabelStartTs)/1000);
    var mm = String(Math.floor(sec/60)).padStart(2,'0');
    var ss = String(sec%60).padStart(2,'0');
    setRelabelTimerText("进行中: " + mm + ":" + ss);
  }, 1000);
}

function startRelabel(e){ startGeneric_(e, "B2C", "换单", "b2c_relabel", function(){ activeRelabel = new Set(); relabelStartTs = Date.now(); startRelabelTimer(); }); }
async function endRelabel(){ if(!acquireBusy_()) return; try{ await endSessionGlobal_(); }finally{ releaseBusy_(); } }

/** ===== PICK end ===== */
async function endPicking(){
  if(!acquireBusy_()) return;
  try{ await endSessionGlobal_(); }finally{ releaseBusy_(); }
}

async function openScannerWave(){
  if(!currentSessionId){ setStatus("请先开始拣货 / 먼저 시작", false); return; }
  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再扫码波次，请重新开始。"))) return;

  scanMode = "wave";
  document.getElementById("scanTitle").textContent = "扫码波次 / 웨이브 스캔";
  await openScannerCommon();
}

async function leaderLoginPick(){
  if(!currentSessionId){ setStatus("请先开始拣货再组长登录 / 먼저 시작", false); return; }
  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再组长登录，请重新开始。"))) return;

  scanMode = "leaderLoginPick";
  document.getElementById("scanTitle").textContent = "扫码组长工牌登录 / 팀장 로그인";
  await openScannerCommon();
}

async function openScannerInboundCount(){
  if(!currentSessionId){ setStatus("请先开始理货 / 먼저 시작", false); return; }
  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再扫码入库单，请重新开始。"))) return;

  scanMode = "inbound_count";
  document.getElementById("scanTitle").textContent = "扫码入库单号（计数/去重）";
  await openScannerCommon();
}

async function openScannerBulkOutOrder(){
  if(!currentSessionId){ setStatus("请先开始批量出库 / 먼저 시작", false); return; }
  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再扫码出库单，请重新开始。"))) return;

  scanMode = "bulkout_order";
  document.getElementById("scanTitle").textContent = "扫码出库单号（计数/去重）";
  await openScannerCommon();
}

/** ===== Trip note helper (取/送货, 问题处理 need a note on join) ===== */
function needsTripNote_(task){
  return task === "取/送货" || task === "问题处理";
}

/** ===== Labor (join/leave) ===== */
async function joinWork(biz, task){
  if(!acquireBusy_()) return;
  try{ await joinWork_(biz, task); }finally{ releaseBusy_(); }
}
async function joinWork_(biz, task){
  biz = String(biz||"").trim();
  task = String(task||"").trim();

  // ✅ 每任务独立 session：先拿到该任务的 session
  var sid = getSess_(biz, task);

  // 自动 session 的任务：延迟到真正扫码 join 成功时才创建 session
  if(!sid && taskAutoSession_(task)){
    _pendingAutoSession = { biz: biz, task: task };
    laborAction = "join"; laborBiz = biz; laborTask = task;
    scanMode = "labor";
    document.getElementById("scanTitle").textContent = "扫码工牌（加入）";
    await openScannerCommon();
    return;
  }

  if(!sid){
    // 非自动任务（TALLY/PICK/RELABEL/批量出库）：必须先点"开始"生成新趟次
    setStatus("请先点【开始】生成新趟次", false);
    alert("该环节必须先点【开始】生成新趟次（session），再扫码加入。");
    return;
  }

  currentSessionId = sid;
  CUR_CTX = { biz: biz, task: task, page: getHashPage() };
  refreshUI();

  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再加入作业，请重新开始或加入新趟次。"))) return;

  laborAction = "join"; laborBiz = biz; laborTask = task;
  scanMode = "labor";
  document.getElementById("scanTitle").textContent = "扫码工牌（加入）";
  await openScannerCommon();
}


async function leaveWork(biz, task){
  if(!acquireBusy_()) return;
  try{ await leaveWork_(biz, task); }finally{ releaseBusy_(); }
}
async function leaveWork_(biz, task){
  biz = String(biz||"").trim();
  task = String(task||"").trim();

  var sid = getSess_(biz, task);
  if(!sid){
    setStatus("该任务没有 session（请先开始/加入）", false);
    alert("该任务还没有开始过（没有 session）。\n请先点【开始】或先加入趟次。");
    return;
  }

  currentSessionId = sid;
  CUR_CTX = { biz: biz, task: task, page: getHashPage() };
  refreshUI();

  // ✅ leave 不再检查 session 是否关闭 — 即使 session 已关闭，也要允许释放锁
  // 只做提示，不阻断
  var sessionClosed_ = await isSessionClosedAsync_();

  var reg_ = taskReg_(task);
  if(reg_ && reg_.get().size === 0){
    if(sessionClosed_){
      // session 已关闭且本地无人 → 直接清理本地状态
      alert("该趟次已结束，本地状态已清理。");
      cleanupLocalSession_();
      return;
    }
    // session 未关闭但本地为空 → 可能刷新/换设备导致未同步，允许扫码尝试释放锁
    var choice = confirm("本机名单为空（可能未同步），但趟次仍在进行中。\n\n【确定】→ 打开扫码器，扫工牌尝试退出并释放服务器锁\n【取消】→ 返回");
    if(!choice) return;
    // 放行到下面的扫码流程
  }

  laborAction = "leave"; laborBiz = biz; laborTask = task;
  scanMode = "labor";
  document.getElementById("scanTitle").textContent = "扫码工牌（退出）";
  await openScannerCommon();
}


/** ===== Badge / Employee / Bind ===== */
function setDaStatus(msg, ok){
  if(ok===undefined) ok=true;
  var el = document.getElementById("daStatus");
  if(!el) return;
  el.className = ok ? "ok" : "bad";
  el.textContent = msg;
}
function refreshDaUI(){
  var el = document.getElementById("daText");
  if(el) el.textContent = currentDaId || "无";
}

function generateDailyBadgesByName(){
  try{
    var ta = document.getElementById("daNameList");
    if(!ta){ alert("找不到 daNameList"); return; }
    var names = normalizeNames(ta.value);
    if(names.length===0){ alert("请先输入日当姓名（每行一个）"); return; }

    var listEl = document.getElementById("badgeList");
    if(!listEl) return;
    listEl.innerHTML = "";
    setDaStatus("生成中...", true);

    var d = new Date();
    var yy = String(d.getFullYear()).slice(-2);
    var mm = String(d.getMonth()+1).padStart(2,'0');
    var dd = String(d.getDate()).padStart(2,'0');
    var dateStr = yy + mm + dd;

    names.forEach(function(name, idx){
      var da = "DA-" + dateStr + "-" + name;

      var evId = makeEventId({ event:"daily_checkin", biz:"DAILY", task:"BADGE", wave_id:"", badgeRaw:da });
      if(!hasRecent(evId)){
        submitEvent({ event:"daily_checkin", event_id: evId, biz:"DAILY", task:"BADGE", pick_session_id:"NA", da_id: da });
        addRecent(evId);
      }

      var box = document.createElement("div");
      box.style.border = "1px solid #ddd";
      box.style.borderRadius = "12px";
      box.style.padding = "10px";
      var safeId = "qrn_" + dateStr + "_" + idx;
      var labelDiv = document.createElement("div");
      labelDiv.style.cssText = "font-weight:700;margin-bottom:6px;";
      labelDiv.textContent = da;
      var qrDiv = document.createElement("div");
      qrDiv.id = safeId;
      box.appendChild(labelDiv);
      box.appendChild(qrDiv);
      listEl.appendChild(box);
      new QRCode(document.getElementById(safeId), { text: "DA-" + dateStr + "-" + name, width: 160, height: 160 });

      currentDaId = da;
      localStorage.setItem("da_id", currentDaId);
    });

    refreshDaUI();
    setDaStatus("按姓名批量生成完成 ✅ 共 " + names.length + " 个", true);
    alert("已生成日当工牌 ✅ 共 " + names.length + " 个\n格式：DA-" + dateStr + "-姓名\n建议截图/打印发放。");
  }catch(e){
    setDaStatus("生成失败 ❌ " + e, false);
  }
}

function padNum(n, width){
  var s = String(n);
  return s.length>=width ? s : ("0".repeat(width-s.length)+s);
}
function normalizeNames(text){
  return (text||"").split(/\r?\n/).map(function(s){return s.trim();}).filter(Boolean);
}

function generatePermanentDaBadges(){
  var ta = document.getElementById("daPermanentNames");
  if(!ta){ alert("找不到 daPermanentNames"); return; }
  var names = normalizeNames(ta.value);
  if(names.length===0){ alert("请先输入长期日当姓名（每行一个）"); return; }

  var listEl = document.getElementById("daPermanentList");
  if(!listEl){ alert("找不到 daPermanentList"); return; }
  listEl.innerHTML = "";

  names.forEach(function(name, idx){
    var payload = "DAF-" + name;
    var safeKey = "pdaq_" + idx;

    var box = document.createElement("div");
    box.style.border = "1px solid #ddd";
    box.style.borderRadius = "12px";
    box.style.padding = "10px";
    var labelDiv2 = document.createElement("div");
    labelDiv2.style.cssText = "font-weight:700;margin-bottom:6px;";
    labelDiv2.textContent = payload;
    var qrDiv2 = document.createElement("div");
    qrDiv2.id = safeKey;
    box.appendChild(labelDiv2);
    box.appendChild(qrDiv2);
    listEl.appendChild(box);
    new QRCode(document.getElementById(safeKey), { text: "DAF-" + name, width: 160, height: 160 });
  });

  alert("已生成长期日当工牌 ✅ 共 " + names.length + " 个\n建议截图/打印发放（以后每天都用这一张）。");
}

async function bindBadgeToSession(){
  try{
    if(!currentSessionId){ setDaStatus("请先开始某个作业再绑定 / 먼저 시작", false); return; }
    if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能绑定工牌，请重新开始。"))) return;

    scanMode = "badgeBind";
    document.getElementById("scanTitle").textContent = "扫码工牌（绑定） / 명찰 연결";
    await openScannerCommon();
  }catch(e){
    setDaStatus("绑定失败 ❌ " + e, false);
  }
}

function generateEmployeeBadges(){
  var ta = document.getElementById("empNames");
  if(!ta){ alert("找不到 empNames"); return; }
  var names = normalizeNames(ta.value);
  if(names.length===0){ alert("请先输入员工名字（每行一个）"); return; }

  var listEl = document.getElementById("empBadgeList");
  if(!listEl){ alert("找不到 empBadgeList"); return; }
  listEl.innerHTML = "";

  names.forEach(function(name, idx){
    var payload = "EMP-" + name;
    var safeKey = "empqr_" + idx;

    var box = document.createElement("div");
    box.style.border = "1px solid #ddd";
    box.style.borderRadius = "12px";
    box.style.padding = "10px";
    var labelDiv3 = document.createElement("div");
    labelDiv3.style.cssText = "font-weight:700;margin-bottom:6px;";
    labelDiv3.textContent = payload;
    var qrDiv3 = document.createElement("div");
    qrDiv3.id = safeKey;
    box.appendChild(labelDiv3);
    box.appendChild(qrDiv3);
    listEl.appendChild(box);
    new QRCode(document.getElementById(safeKey), { text: "EMP-" + name, width: 160, height: 160 });
  });

  alert("已生成员工工牌 ✅ 共 "+names.length+" 个\n建议截图/打印此页面发放。");
}

/** ===== Leader UI ===== */
function syncLeaderPickUI(){
  var info = document.getElementById("leaderInfoPick");
  var btnEnd = document.getElementById("btnEndPick");
  if(!info || !btnEnd) return;

  if(leaderPickOk && leaderPickBadge){
    info.textContent = "组长已登录 ✅ " + leaderPickBadge;
    btnEnd.style.display = "block";
  }else{
    info.textContent = leaderPickBadge ? ("组长未确认（本趟需登录）: " + leaderPickBadge) : "组长未登录 / 팀장 미 로그인";
    btnEnd.style.display = "none";
  }
}

/** ===== Scanner overlay ===== */
function showOverlay(){ var el=document.getElementById("scannerOverlay"); if(el) el.classList.add("show"); }
function hideOverlay(){ var el=document.getElementById("scannerOverlay"); if(el) el.classList.remove("show"); }
async function pauseScanner(){ try{ if(scanner) await scanner.pause(true); }catch(e){} }

async function openScannerCommon(){
  showOverlay();
  document.getElementById("reader").innerHTML = "";

  try{ if(scanner){ await scanner.stop(); await scanner.clear(); } }catch(e){}

  // ✅ 同时支持 QR 码和常见条形码格式（CODE_128 / CODE_39 / EAN_13 / EAN_8 / UPC_A / ITF / CODABAR）
  var supportedFormats = [
    Html5QrcodeSupportedFormats.QR_CODE,
    Html5QrcodeSupportedFormats.CODE_128,
    Html5QrcodeSupportedFormats.CODE_39,
    Html5QrcodeSupportedFormats.EAN_13,
    Html5QrcodeSupportedFormats.EAN_8,
    Html5QrcodeSupportedFormats.UPC_A,
    Html5QrcodeSupportedFormats.ITF,
    Html5QrcodeSupportedFormats.CODABAR
  ];
  scanner = new Html5Qrcode("reader", {
    formatsToSupport: supportedFormats,
    experimentalFeatures: { useBarCodeDetectorIfSupported: true }
  });

  // ✅ 条形码乱码过滤：检测可疑字符比例，拦截明显误读
  function looksGarbled_(text){
    if(!text || text.length < 2) return false;
    // 正常工单号/工牌由字母、数字、横杠、斜杠、下划线、点组成
    var normal = text.replace(/[A-Za-z0-9\u4e00-\u9fff\uAC00-\uD7AF\-_\/\.\|,\s]/g, "");
    // 如果超过30%是特殊字符，很可能是乱码
    return (normal.length / text.length) > 0.3;
  }

  var onScan = async (decodedText) => {
    var code = decodedText.trim();
    try { code = decodeURIComponent(code); } catch(e) {}
    if(scanBusy) return;

    var now = Date.now();
    var throttle = (scanMode === "b2b_workorder" || scanMode === "b2b_workorder_simple") ? 300 : 900;
    if(now - lastScanAt < throttle) return;
    lastScanAt = now;

    // ✅ 乱码检测：对非 QR 码场景（工单/单号扫码）自动拦截可疑结果
    if(scanMode !== "operator_setup" && scanMode !== "session_join" && scanMode !== "labor" && scanMode !== "badgeBind" && scanMode !== "leaderLoginPick" && scanMode !== "b2b_result_confirm_badge" && scanMode !== "b2b_simple_worker" && scanMode !== "b2b_simple_confirm_badge"){
      if(looksGarbled_(code)){
        setStatus("⚠️ 扫码结果异常（疑似乱码）：" + code + " — 请重新扫码或手动输入", false);
        return;
      }
    }

    if(scanMode === "operator_setup"){
      if(!isOperatorBadge(code)){ setStatus("无效工牌 / 잘못된 명찰 ❌", false); return; }
      var saved = saveOperatorId(code);
      if(saved){
        await closeScanner();
        setStatus("操作员已设置 ✅ " + parseBadge(code).id, true);
      }
      return;
    }

    if(scanMode === "b2b_result_confirm_badge"){
      var bp = parseBadge(code);
      if(!isEmpId(bp.id)){
        setStatus("⚠️ 请扫描职员工牌（EMP-...），当前扫到: " + code, false);
        try{ if(navigator.vibrate) navigator.vibrate([100,50,100]); }catch(e){}
        return;
      }
      window._rfConfirmBadgeRaw = code;
      var inp = document.getElementById("rf-confirm-badge");
      if(inp) inp.value = (bp.name ? bp.name + " (" + bp.id + ")" : bp.id);
      await closeScanner();
      setStatus("职员工牌已扫描 ✅ " + bp.id, true);
      return;
    }

    // B2B Simple Mode: 扫工单
    if(scanMode === "b2b_workorder_simple"){
      smHandleWorkorderScan_(code);
      return;
    }

    // B2B Simple Mode: 扫人员
    if(scanMode === "b2b_simple_worker"){
      scanBusy = true;
      try{
        await smHandleWorkerScan_(code);
        await closeScanner();
      }finally{ scanBusy = false; }
      return;
    }

    // B2B Simple Mode: 扫确认工牌
    if(scanMode === "b2b_simple_confirm_badge"){
      var bpSm = parseBadge(code);
      if(!isEmpId(bpSm.id)){
        setStatus("⚠️ 请扫描职员工牌（EMP-...）/ 직원 명찰(EMP-...) 필수", false);
        try{ if(navigator.vibrate) navigator.vibrate([100,50,100]); }catch(e){}
        return;
      }
      window._smConfirmBadgeRaw = code;
      var inpSm = document.getElementById("sm-confirm-badge");
      if(inpSm) inpSm.value = (bpSm.name ? bpSm.name + " (" + bpSm.id + ")" : bpSm.id);
      await closeScanner();
      setStatus("职员工牌已扫描 / 직원 명찰 스캔됨 ✅ " + bpSm.id, true);
      return;
    }

    if(scanMode === "session_join"){
      var parsed = parseSessionQr_(code);
      if(!parsed){ setStatus("不是趟次二维码（CKSESSION|...）", false); return; }

      scanBusy = true;
      await pauseScanner();
      try{
        await bindSessionWithCtx_(parsed.sid, parsed.biz, parsed.task);
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }

    if(scanMode === "inbound_count"){
      var code2 = decodedText.trim();
      if(!code2){ setStatus("入库单号为空", false); return; }

      scanBusy = true;
      await pauseScanner();
      try{
        if(scannedInbounds.has(code2)){
          setStatus("已记录（去重）✅ " + code2, true);
          alert("已记录（去重）✅\n" + code2);
          renderInboundCountUI();
          await closeScanner();
          return;
        }

        scannedInbounds.add(code2);
        persistState();
        renderInboundCountUI();

        var evIdX = makeEventId({ event:"wave", biz:"B2C", task:"理货", wave_id: code2, badgeRaw:"" });
        if(!hasRecent(evIdX)){
          submitEvent({ event:"wave", event_id: evIdX, biz:"B2C", task:"理货", pick_session_id: currentSessionId, wave_id: code2 });
          addRecent(evIdX);
        }

        setStatus("已记录入库单（待上传）✅ " + code2, true);
        alert("已记录入库单 ✅\n" + code2 + "\n当前累计：" + scannedInbounds.size);
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }

    if(scanMode === "bulkout_order"){
      var code3 = decodedText.trim();
      if(!code3){ setStatus("出库单号为空", false); return; }

      scanBusy = true;
      await pauseScanner();
      try{
        if(scannedBulkOutOrders.has(code3)){
          setStatus("已记录（去重）✅ " + code3, true);
          alert("已记录（去重）✅\n" + code3);
          renderBulkOutUI();
          await closeScanner();
          return;
        }

        scannedBulkOutOrders.add(code3);
        persistState();
        renderBulkOutUI();

        var evIdB = makeEventId({ event:"wave", biz:"B2C", task:"批量出库", wave_id: code3, badgeRaw:"" });
        if(!hasRecent(evIdB)){
          submitEvent({ event:"wave", event_id: evIdB, biz:"B2C", task:"批量出库", pick_session_id: currentSessionId, wave_id: code3 });
          addRecent(evIdB);
        }

        setStatus("已记录出库单（待上传）✅ " + code3, true);
        alert("已记录出库单 ✅\n" + code3 + "\n当前累计：" + scannedBulkOutOrders.size);
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }

    if(scanMode === "b2b_tally_order"){
      var codeT = decodedText.trim();
      if(!codeT){ setStatus("理货单号为空", false); return; }
      scanBusy = true;
      await pauseScanner();
      try{
        if(scannedB2bTallyOrders.has(codeT)){
          setStatus("已记录（去重）✅ " + codeT, true);
          alert("已记录（去重）✅\n" + codeT);
          renderB2bTallyUI(); await closeScanner(); return;
        }
        scannedB2bTallyOrders.add(codeT); persistState(); renderB2bTallyUI();
        var evIdT = makeEventId({ event:"wave", biz:"B2B", task:"B2B入库理货", wave_id: codeT, badgeRaw:"" });
        if(!hasRecent(evIdT)){
          submitEvent({ event:"wave", event_id: evIdT, biz:"B2B", task:"B2B入库理货", pick_session_id: currentSessionId, wave_id: codeT });
          addRecent(evIdT);
        }
        setStatus("已记录理货单（待上传）✅ " + codeT, true);
        alert("已记录理货单 ✅\n" + codeT + "\n当前累计：" + scannedB2bTallyOrders.size);
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }

    if(scanMode === "b2b_workorder"){
      var codeW = decodedText.trim().toUpperCase();
      if(!codeW){ return; }

      // 冷却期内忽略
      var nowW = Date.now();
      if(nowW < _b2bCooldownUntil) return;

      // 合法性判断
      if(!isValidB2bWorkorderCode_(codeW)){
        showScanFeedback_("不是有效工单号: " + codeW, "#fff0f0", "#c00", 1500);
        setStatus("⚠️ 不是有效工单号: " + codeW, false);
        return;
      }

      // 已记录去重（不关闭扫码器，继续扫）
      if(scannedB2bWorkorders.has(codeW)){
        showScanFeedback_("该工单已绑定: " + codeW, "#fffbe6", "#b8860b", 1200);
        setStatus("该工单已绑定 ✅ " + codeW, true);
        return;
      }

      // 二次确认机制
      if(_b2bPendingCode !== codeW){
        _b2bPendingCode = codeW;
        _b2bPendingTime = nowW;
        setStatus("已识别 " + codeW + "，等待确认...", true);
        return;
      }
      if(nowW - _b2bPendingTime > B2B_CONFIRM_WINDOW_MS){
        _b2bPendingTime = nowW;
        setStatus("已识别 " + codeW + "，等待确认...", true);
        return;
      }

      // 二次确认通过，记录
      _b2bPendingCode = null; _b2bPendingTime = 0;
      _b2bCooldownUntil = nowW + B2B_SUCCESS_COOLDOWN_MS;
      scannedB2bWorkorders.add(codeW); persistState(); renderB2bWorkorderUI();
      var evIdW = makeEventId({ event:"wave", biz:"B2B", task:"B2B工单操作", wave_id: codeW, badgeRaw:"" });
      if(!hasRecent(evIdW)){
        submitEvent({ event:"wave", event_id: evIdW, biz:"B2B", task:"B2B工单操作", pick_session_id: currentSessionId, wave_id: codeW });
        addRecent(evIdW);
      }
      callB2bOpBind(codeW);
      showScanFeedback_("⏳ 绑定中 " + codeW + "...", "#e3f2fd", "#1565c0", 1500);
      setStatus("绑定中... " + codeW, true);
      updateScanRecentList_();
      try{ if(navigator.vibrate) navigator.vibrate(80); }catch(e){}
      // 连续扫码：不关闭扫码器
      return;
    }

    if(scanMode === "wave"){
      var waveOk = /^\d{4}-\d{4}-\d+$/.test(code);
      if(!waveOk){
        var useAnyway = confirm("波次格式不是标准格式（标准：2026-0224-6）\n\n扫到的内容：" + code + "\n\n仍然要记录吗？");
        if(!useAnyway){ setStatus("已取消", false); return; }
      }
      if(scannedWaves.has(code)){ setStatus("重复波次已忽略 ⏭️ " + code, false); return; }

      scannedWaves.add(code);
      persistState();
      renderWaveUI();

      scanBusy = true;
      await pauseScanner();
      try{
        var evId = makeEventId({ event:"wave", biz:"B2C", task:"拣货", wave_id: code, badgeRaw:"" });
        if(hasRecent(evId)){ setStatus("重复扫码已忽略 ⏭️ " + code, false); await closeScanner(); return; }

        submitEvent({ event:"wave", event_id: evId, biz:"B2C", task:"拣货", pick_session_id: currentSessionId, wave_id: code });
        addRecent(evId);

        alert("已记录波次 ✅ " + code + "\n当前累计：" + scannedWaves.size);
        setStatus("已记录波次（待上传）✅ " + code, true);
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }

    if(scanMode === "badgeBind"){
      if(!isOperatorBadge(code)){ setDaStatus("无效工牌（DA-... / DAF-...|名字 / EMP-...|名字）", false); return; }
      var p = parseBadge(code);

      scanBusy = true;
      await pauseScanner();
      try{
        var evId2 = makeEventId({ event:"bind_daily", biz:"DAILY", task:"BADGE", wave_id:"", badgeRaw:p.raw });
        if(hasRecent(evId2)){ setDaStatus("重复扫码已忽略 ⏭️", false); await closeScanner(); return; }

        submitEvent({ event:"bind_daily", event_id: evId2, biz:"DAILY", task:"BADGE", pick_session_id: currentSessionId, da_id: p.raw });
        addRecent(evId2);

        currentDaId = p.raw; localStorage.setItem("da_id", currentDaId); refreshDaUI();
        alert("已绑定工牌 ✅ " + p.raw);
        setDaStatus("绑定成功（待上传）✅", true);
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }

    if(scanMode === "labor"){
      if(!isOperatorBadge(code)){ setStatus("无效工牌（DA-... / DAF-...|名字 / EMP-...|名字）", false); return; }
      var p2 = parseBadge(code);

      // ✅ 语义去重：同一badge+action+task+session 在处理中/TTL内不重复请求
      var _ddKey = _laborDedupKey(laborAction, laborBiz, laborTask, currentSessionId, p2.raw);
      var _ddState = _laborDedupCheck(_ddKey);
      if(_ddState === "inflight"){
        setStatus("正在处理中，请勿重复扫描 ⏳ " + p2.raw, false);
        return;
      }
      if(_ddState === "done"){
        setStatus("重复扫描已忽略 ⏭️ " + p2.raw, false);
        return;
      }

      if(laborAction === "leave" && !isAlreadyActive(laborTask, p2.raw)){
        // ✅ 本地列表可能不准（刷新/换设备），不再硬拦截，改为确认后继续
        var goOn = confirm("该工牌不在本机名单中（可能是刷新或换设备导致）。\n\n仍要尝试退出并释放服务器锁吗？");
        if(!goOn){
          setStatus("已取消退出", false);
          await closeScanner();
          return;
        }
      }

      if(laborAction === "join" && isAlreadyActive(laborTask, p2.raw)){
        alert("已在作业中 ✅ " + p2.raw);
        setStatus("已在作业中 ✅", true);
        await closeScanner();
        return;
      }

      scanBusy = true;
      await pauseScanner();

      // ✅ 取/送货、问题处理：join 时弹出 prompt 输入备注
      var tripNote = "";
      if(laborAction === "join" && needsTripNote_(laborTask)){
        tripNote = prompt("请输入本趟备注（去哪取/送货 或 处理了什么）：") || "";
        tripNote = tripNote.trim();
        if(!tripNote){
          alert("请输入备注后重试");
          scanBusy = false;
          await closeScanner();
          return;
        }
      }

      _laborDedupMarkInflight(_ddKey);
      setStatus("处理中... 请稍等 ⏳（join/leave 需确认锁）", true);

      // ✅ 延迟创建 auto-session：扫到有效工牌后才真正创建
      if(_pendingAutoSession && laborAction === "join"){
        var pa = _pendingAutoSession;
        var newSid = makePickSessionId();
        var evIdStart = makeEventId({ event:"start", biz:pa.biz, task:pa.task, wave_id:"", badgeRaw:"" });
        if(!hasRecent(evIdStart)){
          try{
            await submitEventSync_({ event:"start", event_id: evIdStart, biz: pa.biz, task: pa.task, pick_session_id: newSid }, true);
            addRecent(evIdStart);
          }catch(e){
            _pendingAutoSession = null;
            _laborDedupClear(_ddKey);
            scanBusy = false;
            setStatus("创建趟次失败 ❌ " + e, false);
            alert("创建趟次失败：" + String(e));
            await closeScanner();
            return;
          }
        }
        // 服务器确认后写入本地状态
        currentSessionId = newSid;
        CUR_CTX = { biz: pa.biz, task: pa.task, page: getHashPage() };
        setSess_(pa.biz, pa.task, newSid);
        // 清空该任务的本地 active set
        var _r = taskReg_(pa.task);
        if(_r) _r.set(new Set());
        if(pa.task==="取/送货") importPickupNotes = {};
        if(pa.task==="问题处理") importProblemNotes = {};
        persistState(); refreshUI();
        _justCreatedAutoSid = newSid;
        _pendingAutoSession = null;
        // auto-session 兑现后 currentSessionId 已变，清旧键+重算新键
        _laborDedupClear(_ddKey);
        _ddKey = _laborDedupKey(laborAction, laborBiz, laborTask, currentSessionId, p2.raw);
        _laborDedupMarkInflight(_ddKey);
      }

      try{
        var evId = makeEventId({ event:laborAction, biz:laborBiz, task:laborTask, wave_id:"", badgeRaw:p2.raw });
        if(hasRecent(evId)){ setStatus("重复扫描已忽略 ⏭️", false); await closeScanner(); return; }

        var submitPayload = {
          event: laborAction,
          event_id: evId,
          biz: laborBiz,
          task: laborTask,
          pick_session_id: currentSessionId,
          da_id: p2.raw
        };
        if(tripNote) submitPayload.note = tripNote;

        var syncRes = await submitEventSyncWithRetry_(submitPayload);

        _justCreatedAutoSid = null; // join 成功，不再需要回滚
        addRecent(evId);
        _laborDedupMarkDone(_ddKey);

        applyActive(laborTask, laborAction, p2.raw);

        // ✅ 本地缓存备注（用于 UI 显示）
        if(laborAction === "join" && tripNote){
          if(laborTask === "取/送货") importPickupNotes[p2.raw] = tripNote;
          if(laborTask === "问题处理") importProblemNotes[p2.raw] = tripNote;
        }
        if(laborAction === "leave"){
          if(laborTask === "取/送货") delete importPickupNotes[p2.raw];
          if(laborTask === "问题处理") delete importProblemNotes[p2.raw];
        }

        renderActiveLists();
        persistState();

        if(laborAction === "leave"){
          // ✅ 检查锁是否真的释放了
          if(syncRes && syncRes.lock_released === false){
            console.warn("[LEAVE] lock_released=false for badge:", p2.raw);
          }
          await tryAutoEndSessionAfterLeave_();
        }

        alert((laborAction === "join" ? "已加入 ✅ " : "已退出 ✅ ") + p2.raw);
        setStatus((laborAction === "join" ? "加入成功 ✅ " : "退出成功 ✅ ") + p2.raw, true);
        await closeScanner();
      } catch(e){
        // ✅ 回滚刚创建的 auto-session（start 成功但 join 失败）
        if(_justCreatedAutoSid && currentSessionId === _justCreatedAutoSid){
          var rollSid = _justCreatedAutoSid;
          _justCreatedAutoSid = null;

          // 事后核实：join 是否其实已在服务端成功（弱网回包丢失）
          var joinActuallyOk = false;
          try{
            var infoRes = await jsonp(LOCK_URL, { action:"session_info", session: rollSid }, { skipBusy: true });
            if(infoRes && infoRes.ok && Array.isArray(infoRes.active)){
              joinActuallyOk = infoRes.active.some(function(lk){ return lk.badge === p2.raw; });
            }
          }catch(_){}

          if(joinActuallyOk){
            // join 实际已成功：补齐正常成功路径的本地副作用
            addRecent(evId);
            _laborDedupMarkDone(_ddKey);
            applyActive(laborTask, "join", p2.raw);
            if(tripNote){
              if(laborTask === "取/送货") importPickupNotes[p2.raw] = tripNote;
              if(laborTask === "问题处理") importProblemNotes[p2.raw] = tripNote;
            }
            renderActiveLists(); persistState();
            setStatus("网络异常，但已确认加入成功 ✅ " + p2.raw, true);
            alert("网络波动，但服务端确认已成功加入 ✅\n" + p2.raw);
            await closeScanner();
          } else {
            // join 确实没成功：清去重+清本地+关服务端空 session
            _laborDedupClear(_ddKey);
            if(CUR_CTX) clearSess_(CUR_CTX.biz, CUR_CTX.task);
            currentSessionId = null;
            persistState(); refreshUI();
            var rollOk = false;
            try{
              var rr = await jsonp(LOCK_URL, { action:"session_close", session: rollSid, operator_id: getOperatorId() || "" });
              rollOk = !!(rr && rr.ok);
            }catch(_){}
            if(rollOk){
              setStatus("加入失败，已自动撤销空趟次 ❌ " + e, false);
              alert("加入失败，已自动撤销刚创建的趟次。\n" + e);
            } else {
              setStatus("加入失败，服务端趟次关闭失败 ❌", false);
              alert("加入失败。\n\n本地已清理，但服务端趟次关闭失败，请刷新后重试或联系管理员。\n\n趟次ID: " + rollSid + "\n原因: " + e);
            }
          }
        } else {
          _laborDedupClear(_ddKey);
          setStatus("提交失败 ❌ " + e, false);
          alert("提交失败，请重试。\n" + e);
        }
      } finally { scanBusy = false; _justCreatedAutoSid = null; await closeScanner(); }
      return;
    }

    if(scanMode === "leaderLoginPick"){
      if(!isOperatorBadge(code)){ setStatus("无效工牌（请扫 EMP-xxx|名字）", false); return; }
      var p3 = parseBadge(code);
      if(!p3.id.startsWith("EMP-")){ setStatus("请扫组长员工工牌（EMP-xxx|名字）", false); return; }

      scanBusy = true;
      await pauseScanner();
      try{
        leaderPickBadge = p3.raw; localStorage.setItem("leader_pick_badge", leaderPickBadge);
        leaderPickOk = true; localStorage.setItem("leader_pick_ok", "1"); syncLeaderPickUI();
        alert("组长登录成功 ✅ " + p3.raw);
        setStatus("组长登录成功 ✅", true);
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }

  };

  // ✅ 自适应扫描区域
  var qrboxFn = function(viewfinderWidth, viewfinderHeight){
    if(scanMode === "b2b_workorder" || scanMode === "b2b_workorder_simple"){
      // B2B 工单专用：宽 90% × 高 15%，适配横向一维码
      var w = Math.floor(viewfinderWidth * 0.90);
      var h = Math.floor(viewfinderHeight * 0.15);
      if(w < 280) w = Math.min(280, viewfinderWidth - 10);
      if(h < 50) h = Math.min(50, viewfinderHeight - 10);
      return { width: w, height: h };
    }
    // 通用：宽 85% × 高 30%
    var w = Math.floor(viewfinderWidth * 0.85);
    var h = Math.floor(viewfinderHeight * 0.30);
    if(w < 250) w = Math.min(250, viewfinderWidth - 10);
    if(h < 80) h = Math.min(80, viewfinderHeight - 10);
    return { width: w, height: h };
  };

  // 缓存 onScan 和 qrboxFn 供 switchCamera 复用
  _scanOnScan = onScan;
  _scanQrboxFn = qrboxFn;

  var isIOS = /iPad|iPhone|iPod/i.test(navigator.userAgent) && !window.MSStream;

  if(isIOS){
    // ✅ iOS：只用 facingMode，getCameras label 通常为空
    try{
      await scanner.start({ facingMode: "environment" }, { fps: 10, qrbox: qrboxFn }, onScan);
      _scanCurrentCamId = null;
      _scanCurrentLabel = "iOS 后置";
    }catch(e){
      var iosCams = await Html5Qrcode.getCameras();
      var iosId = iosCams && iosCams[0] ? iosCams[0].id : null;
      await scanner.start(iosId, { fps: 10, qrbox: qrboxFn }, onScan);
      _scanCurrentCamId = iosId;
      _scanCurrentLabel = (iosCams && iosCams[0] && iosCams[0].label) || "camera";
    }
    _scanCameras = [];
  }else{
    // ✅ Android：优先 getCameras + 黑白名单选主摄
    var cams = [];
    try{ cams = await Html5Qrcode.getCameras(); }catch(e){}
    _scanCameras = cams || [];

    var chosen = null;
    if(_scanCameras.length > 0){
      // 检查 localStorage 缓存
      var cached = localStorage.getItem("ck_preferred_cam");
      if(cached){
        for(var ci=0;ci<_scanCameras.length;ci++){
          if(_scanCameras[ci].id === cached){ chosen = _scanCameras[ci]; break; }
        }
      }
      if(!chosen) chosen = pickBestCamera_(_scanCameras);
    }

    var started = false;
    if(chosen){
      try{
        await scanner.start(chosen.id, { fps: 10, qrbox: qrboxFn }, onScan);
        _scanCurrentCamId = chosen.id;
        _scanCurrentLabel = chosen.label || "camera";
        localStorage.setItem("ck_preferred_cam", chosen.id);
        localStorage.setItem("ck_preferred_cam_label", chosen.label || "");
        started = true;
      }catch(e){}
    }
    if(!started){
      // fallback: facingMode
      try{
        await scanner.start({ facingMode: "environment" }, { fps: 10, qrbox: qrboxFn }, onScan);
        _scanCurrentCamId = null;
        _scanCurrentLabel = "environment";
      }catch(e2){
        var fb = _scanCameras[0];
        await scanner.start(fb ? fb.id : null, { fps: 10, qrbox: qrboxFn }, onScan);
        _scanCurrentCamId = fb ? fb.id : null;
        _scanCurrentLabel = fb ? (fb.label || "camera") : "camera";
      }
    }
  }

  // ✅ 更新切换镜头 UI
  updateCamSwitchUI_(isIOS);

  // ✅ 启动成功后：zoom=最小值 + 连续对焦
  await applyCamOptimizations_();
}

// ===== 摄像头黑白名单筛选 =====
function pickBestCamera_(cams){
  var blacklist = /tele|telephoto|长焦|macro|微距|periscope|潜望|portrait|人像|zoom|3x|5x|10x/i;
  var tier1 = /main|wide|1x|广角|主|rear\s*0|back\s*0|camera\s*0/i;
  var tier2 = /rear|back|facing back/i;
  var tier3 = /ultra|超广/i;

  var candidates = cams.filter(function(c){ return !blacklist.test(c.label); });
  if(candidates.length === 0) candidates = cams;

  // 第一优先
  for(var i=0;i<candidates.length;i++){
    if(tier1.test(candidates[i].label)) return candidates[i];
  }
  // 第二优先
  for(var i=0;i<candidates.length;i++){
    if(tier2.test(candidates[i].label)) return candidates[i];
  }
  // 次优：ultra/超广
  for(var i=0;i<candidates.length;i++){
    if(tier3.test(candidates[i].label)) return candidates[i];
  }
  return candidates[0];
}

// ===== zoom=min + continuous autofocus =====
async function applyCamOptimizations_(){
  try{
    var videoElem = document.querySelector("#reader video");
    if(videoElem && videoElem.srcObject){
      var tracks = videoElem.srcObject.getVideoTracks();
      if(tracks && tracks[0]){
        var capabilities = typeof tracks[0].getCapabilities === "function" ? tracks[0].getCapabilities() : {};
        var advancedConstraints = {};
        if(capabilities.zoom){ advancedConstraints.zoom = capabilities.zoom.min || 1.0; }
        if(capabilities.focusMode && capabilities.focusMode.indexOf("continuous") >= 0){
          advancedConstraints.focusMode = "continuous";
        }
        if(Object.keys(advancedConstraints).length > 0){
          await tracks[0].applyConstraints({ advanced: [advancedConstraints] });
        }
      }
    }
  }catch(e){ /* iOS Safari 等不支持，静默跳过 */ }
}

// ===== 更新切换镜头 UI =====
function updateCamSwitchUI_(isIOS){
  var labelEl = document.getElementById("camCurrentLabel");
  var btnEl = document.getElementById("btnSwitchCam");
  if(!labelEl || !btnEl) return;
  if(isIOS || _scanCameras.length < 2){
    labelEl.textContent = "";
    btnEl.style.display = "none";
  }else{
    labelEl.textContent = _scanCurrentLabel || "camera";
    btnEl.style.display = "inline-block";
  }
}

// ===== 扫码浮层内反馈 =====
function showScanFeedback_(text, bgColor, textColor, durationMs){
  var el = document.getElementById("scanFeedback");
  if(!el) return;
  if(_scanFeedbackTimer){ clearTimeout(_scanFeedbackTimer); _scanFeedbackTimer = null; }
  el.textContent = text;
  el.style.background = bgColor;
  el.style.color = textColor;
  el.style.opacity = "1";
  el.style.display = "block";
  _scanFeedbackTimer = setTimeout(function(){
    el.style.opacity = "0";
    setTimeout(function(){ el.style.display = "none"; }, 400);
    _scanFeedbackTimer = null;
  }, durationMs || 1200);
}

function updateScanRecentList_(){
  var el = document.getElementById("scanRecentList");
  if(!el) return;
  if(scanMode !== "b2b_workorder" && scanMode !== "b2b_workorder_simple"){ el.style.display = "none"; return; }
  var arr = Array.from(scannedB2bWorkorders);
  var recent = arr.slice(Math.max(0, arr.length - 3));
  if(recent.length === 0){ el.style.display = "none"; return; }
  el.style.display = "block";
  el.innerHTML = "最近已扫: " + recent.map(function(x){ return "<b>" + esc(x) + "</b>"; }).join(" | ");
}

function clearScanFeedback_(){
  var fb = document.getElementById("scanFeedback");
  if(fb){ fb.style.display = "none"; fb.style.opacity = "0"; fb.textContent = ""; }
  var rl = document.getElementById("scanRecentList");
  if(rl){ rl.style.display = "none"; rl.innerHTML = ""; }
  if(_scanFeedbackTimer){ clearTimeout(_scanFeedbackTimer); _scanFeedbackTimer = null; }
}

// ===== 切换镜头 =====
async function switchCamera(){
  if(!scanner || _scanCameras.length < 2) return;
  var lines = [];
  for(var i=0;i<_scanCameras.length;i++){
    var mark = (_scanCameras[i].id === _scanCurrentCamId) ? " ← 当前" : "";
    lines.push((i+1) + ". " + (_scanCameras[i].label || "camera " + (i+1)) + mark);
  }
  var input = prompt("选择镜头编号：\n\n" + lines.join("\n"));
  if(!input) return;
  var idx = parseInt(input, 10) - 1;
  if(isNaN(idx) || idx < 0 || idx >= _scanCameras.length) return;
  var cam = _scanCameras[idx];
  if(cam.id === _scanCurrentCamId) return;

  try{
    await scanner.stop();
    await scanner.start(cam.id, { fps: 10, qrbox: _scanQrboxFn }, _scanOnScan);
    _scanCurrentCamId = cam.id;
    _scanCurrentLabel = cam.label || "camera";
    localStorage.setItem("ck_preferred_cam", cam.id);
    localStorage.setItem("ck_preferred_cam_label", cam.label || "");
    updateCamSwitchUI_(false);
    await applyCamOptimizations_();
  }catch(e){
    alert("切换镜头失败: " + e);
  }
}

async function closeScanner(){
  try{
    if(scanner){ await scanner.stop(); await scanner.clear(); scanner = null; }
  }catch(e){}
  _pendingAutoSession = null; // 关闭扫码器时丢弃未兑现的 auto-session
  clearScanFeedback_();
  hideOverlay();
}
function closeScannerWithFallback(){
  var wasOperatorSetup = (scanMode === "operator_setup");
  closeScanner().then(function(){
    // 若扫码器是为设置操作员而开的，关闭时恢复弹窗
    if(wasOperatorSetup) showOperatorSetup(!!getOperatorId());
  });
}

function comingSoon(msg){
  alert((msg||"准备中") + "\n\n我们会逐步上线。");
}

/** ===== localStorage 清理：删除超过7天的旧 session 数据 ===== */
function cleanupOldLocalStorage_(){
  try{
    var now = Date.now();
    var keysToCheck = [];
    for(var i=0; i<localStorage.length; i++){
      keysToCheck.push(localStorage.key(i));
    }
    // 收集所有带 PS- session ID 的 key（含 session_id_ 前缀的映射 key）
    var sessionKeys = {};
    keysToCheck.forEach(function(k){
      if(!k) return;
      // 匹配 key 中嵌入的 PS-YYYYMMDD-... 或 PS-YYMMDD-... 格式
      var m = k.match(/PS-(\d{8})-/) || k.match(/PS-(\d{6})-/);
      if(!m) return;
      var dateStr = m[1];
      if(!sessionKeys[dateStr]) sessionKeys[dateStr] = [];
      sessionKeys[dateStr].push(k);
    });
    // 删除超过7天的
    for(var dateStr in sessionKeys){
      var y, mo, d;
      if(dateStr.length === 8){
        y = parseInt(dateStr.substring(0,4),10);
        mo = parseInt(dateStr.substring(4,6),10) - 1;
        d = parseInt(dateStr.substring(6,8),10);
      } else {
        // 6位: YYMMDD
        y = 2000 + parseInt(dateStr.substring(0,2),10);
        mo = parseInt(dateStr.substring(2,4),10) - 1;
        d = parseInt(dateStr.substring(4,6),10);
      }
      var ts = new Date(y, mo, d).getTime();
      if(now - ts > 7 * 24 * 3600 * 1000){
        sessionKeys[dateStr].forEach(function(k){ localStorage.removeItem(k); });
      }
    }
  }catch(e){}
}


/** ===== Report (Admin-only) ===== */
var REPORT_CACHE = { header:[], rows:[], summary:[], people:[], timeline:[], anomalies_list:[], meta:{}, task_efficiency:[] };
var REPORT_COST_PER_MIN = 290; // 韩币/人·分钟

function fmtHM_(min){
  if(!min || min <= 0) return "0m";
  var h = Math.floor(min / 60);
  var m = min % 60;
  if(h > 0) return h + "h" + (m > 0 ? String(m).padStart(2,"0") + "m" : "");
  return m + "m";
}
function badgeName_(badge){
  var s = String(badge||"");
  if(s.startsWith("DA-")){ var m = s.match(/^DA-\d{6,8}-(.+)$/); if(m) return m[1]; }
  if(s.startsWith("EMP-")) return s.substring(4);
  if(s.startsWith("DAF-")) return s.substring(4);
  return s;
}
function badgeType_(badge){
  var s = String(badge||"");
  if(s.startsWith("EMP-")) return "员工";
  if(s.startsWith("DAF-")) return "长期日当";
  if(s.startsWith("DA-")) return "日当";
  return "其他";
}

function pad2_(n){ n=String(n); return n.length<2 ? ("0"+n) : n; }
function kstDayKey_(ms){
  var d = new Date(ms + 9*3600*1000);
  return d.getUTCFullYear() + "-" + pad2_(d.getUTCMonth()+1) + "-" + pad2_(d.getUTCDate());
}
function kstDayStartMs_(dayKey){
  return Date.parse(dayKey + "T00:00:00.000Z") - 9*3600*1000;
}
function kstDayEndMs_(dayKey){
  return kstDayStartMs_(dayKey) + 24*3600*1000 - 1;
}
function msToMin_(ms){ return Math.round((ms||0)/60000); }
function fmtTs_(ms){
  if(!ms) return "-";
  try{ return new Date(ms).toLocaleString(); }catch(e){ return String(ms); }
}

function setReportDefaultDates_(){
  var fromEl = document.getElementById("reportDateFrom");
  var toEl = document.getElementById("reportDateTo");
  if(!fromEl || !toEl) return;

  var today = kstDayKey_(Date.now());
  if(!fromEl.value) fromEl.value = today;
  if(!toEl.value) toEl.value = today;
}

function reportLoadRange(){
  if(!adminIsUnlocked_()){
    alert("管理员功能：请先解锁（标题连点 7 次）");
    return;
  }
  var k = adminKey_();
  if(!k){ alert("未检测到管理员口令，请重新解锁"); return; }

  var fromEl = document.getElementById("reportDateFrom");
  var toEl = document.getElementById("reportDateTo");
  var dayFrom = String((fromEl && fromEl.value) || "").trim();
  var dayTo = String((toEl && toEl.value) || "").trim();
  if(!dayFrom || !dayTo){
    alert("请选择开始和结束日期（KST）");
    return;
  }
  if(dayFrom > dayTo){
    alert("开始日期不能晚于结束日期");
    return;
  }

  var startMs = kstDayStartMs_(dayFrom);
  var endMs = kstDayEndMs_(dayTo);
  var rangeLabel = dayFrom + " ~ " + dayTo + " (KST)";

  // 前扩 7 天拉取，保证区间前 join + 区间内 leave 的 overlap 不丢
  var fetchSinceMs = startMs - 7 * 24 * 3600 * 1000;

  setStatus("拉取区间数据中... ⏳", true);

  fetchApi({
    action: "admin_events_tail",
    k: k,
    limit: 20000,
    since_ms: String(fetchSinceMs),
    until_ms: String(endMs)
  }).then(function(res){
    if(!res || res.ok !== true){
      setStatus("拉取失败 ❌ " + (res && res.error ? res.error : "unknown"), false);
      alert("拉取失败：" + (res && res.error ? res.error : "unknown"));
      return;
    }

    REPORT_CACHE.header = res.header || [];
    REPORT_CACHE.rows = res.rows || [];
    REPORT_CACHE.meta = {
      asof: res.asof || Date.now(),
      dayFrom: dayFrom,
      dayTo: dayTo,
      rangeLabel: rangeLabel
    };

    buildReportSummary_();
    renderReport_();
    if((REPORT_CACHE.rows||[]).length >= 20000){
      var wEl = document.getElementById("reportMeta");
      if(wEl) wEl.textContent += " | ⚠️ 已达上限20000条，数据可能不完整，请缩小日期范围";
    }
    setStatus("拉取完成 ✅", true);
  }).catch(function(e){
    setStatus("拉取异常 ❌", false);
    alert("拉取异常：" + String(e && e.message ? e.message : e));
  });
}

function reportLoadToday(){
  var dayKey = kstDayKey_(Date.now());
  var fromEl = document.getElementById("reportDateFrom");
  var toEl = document.getElementById("reportDateTo");
  if(fromEl) fromEl.value = dayKey;
  if(toEl) toEl.value = dayKey;
  reportLoadRange();
}

function fmtKST_(ms){
  if(!ms) return "-";
  var d = new Date(ms + 9*3600*1000);
  return d.getUTCFullYear() + "-" + pad2_(d.getUTCMonth()+1) + "-" + pad2_(d.getUTCDate()) +
    " " + pad2_(d.getUTCHours()) + ":" + pad2_(d.getUTCMinutes()) + ":" + pad2_(d.getUTCSeconds());
}

function buildReportSummary_(){
  var header = REPORT_CACHE.header || [];
  var rows = REPORT_CACHE.rows || [];

  var iServer = header.indexOf("server_ms");
  var iEvent = header.indexOf("event");
  var iBadge = header.indexOf("badge");
  var iBiz = header.indexOf("biz");
  var iTask = header.indexOf("task");
  var iSession = header.indexOf("session");
  var iWaveId = header.indexOf("wave_id");
  var iOperator = header.indexOf("operator_id");
  var iNote = header.indexOf("note");
  var iOk = header.indexOf("ok");

  var active = {}; // badge -> {t, biz, task, session, note}
  var acc = {};    // badge -> { total_ms, tasks: {biz|task:ms} }
  var timelineByBadge = {}; // badge -> [{biz,task,start_ms,end_ms,duration_ms,status,session,note}]
  var anomalies = { open:0, leave_without_join:0, rejoin_without_leave:0 };
  var anomaliesList = [];

  // session-level collection
  var sessionInfo = {}; // session -> {biz, task, start_ms, end_ms, operator, badges:Set, waves:Set}

  function ensureSession(sid, biz, task){
    if(!sid) return;
    if(!sessionInfo[sid]) sessionInfo[sid] = { biz:biz||"", task:task||"", start_ms:0, end_ms:0, operator:"", badges:{}, waves:{} };
  }

  function addDur(badge, biz, task, dur){
    if(!acc[badge]) acc[badge] = { total_ms:0, tasks:{} };
    acc[badge].total_ms += dur;
    var k = biz + "|" + task;
    acc[badge].tasks[k] = (acc[badge].tasks[k]||0) + dur;
  }
  function addTimeline(badge, biz, task, startMs, endMs, status, session, note){
    if(!timelineByBadge[badge]) timelineByBadge[badge] = [];
    timelineByBadge[badge].push({
      biz: biz || "", task: task || "",
      start_ms: startMs || 0, end_ms: endMs || 0,
      duration_ms: Math.max(0, (endMs||0) - (startMs||0)),
      status: status || "NORMAL",
      session: session || "", note: note || ""
    });
  }
  function addAnomaly(type, badge, biz, task, atMs, note){
    anomaliesList.push({
      type: type || "unknown", badge: badge || "",
      biz: biz || "", task: task || "",
      at_ms: atMs || 0, note: note || ""
    });
  }

  var now = Date.now();
  // 报表区间上界：历史查询不能用 now，最多到查询的 until_ms
  var meta = REPORT_CACHE.meta || {};
  var reportUntilMs = meta.dayTo ? kstDayEndMs_(meta.dayTo) : now;
  // 如果查询区间包含今天，用 now（可能正在进行中）
  if(reportUntilMs >= now) reportUntilMs = now;
  // 报表区间下界
  var reportSinceMs = meta.dayFrom ? kstDayStartMs_(meta.dayFrom) : 0;

  // First pass: collect ALL events for session-level data
  for(var r=0;r<rows.length;r++){
    var row = rows[r];
    if(!row) continue;
    if(iOk >= 0){
      var okv = row[iOk];
      if(String(okv).toLowerCase()==="false" || Number(okv)===0) continue;
    }

    var ev = String(row[iEvent]||"").trim();
    var badge = String(row[iBadge]||"").trim();
    var biz = String(row[iBiz]||"").trim();
    var task = String(row[iTask]||"").trim();
    var t = Number(row[iServer]||0) || 0;
    var sid = iSession >= 0 ? String(row[iSession]||"").trim() : "";
    var waveId = iWaveId >= 0 ? String(row[iWaveId]||"").trim() : "";
    var operator = iOperator >= 0 ? String(row[iOperator]||"").trim() : "";
    var note = iNote >= 0 ? String(row[iNote]||"").trim() : "";

    // session-level: start/end/wave
    if(ev === "start" && sid){
      ensureSession(sid, biz, task);
      if(!sessionInfo[sid].start_ms || t < sessionInfo[sid].start_ms) sessionInfo[sid].start_ms = t;
      if(operator) sessionInfo[sid].operator = operator;
    }else if(ev === "end" && sid){
      ensureSession(sid, biz, task);
      if(!sessionInfo[sid].end_ms || t > sessionInfo[sid].end_ms) sessionInfo[sid].end_ms = t;
    }else if(ev === "wave" && sid){
      ensureSession(sid, biz, task);
      if(waveId) sessionInfo[sid].waves[waveId] = true;
    }

    // join/leave processing
    if(ev === "join"){
      if(!badge) continue;
      if(active[badge]){
        // clamp to report range for duration calculation
        var clampedStart = Math.max(active[badge].t, reportSinceMs);
        var clampedEnd = Math.min(t, reportUntilMs);
        var durRejoin = Math.max(0, clampedEnd - clampedStart);
        addDur(badge, active[badge].biz, active[badge].task, durRejoin);
        addTimeline(badge, active[badge].biz, active[badge].task, clampedStart, clampedEnd, "AUTO_CLOSE_REJOIN", active[badge].session, active[badge].note);
        anomalies.rejoin_without_leave++;
        addAnomaly("rejoin_without_leave", badge, active[badge].biz, active[badge].task, t, "join 前未 leave，已自动截断上一段");
      }
      active[badge] = { t:t, biz:biz, task:task, session:sid, note:note };
      if(sid){
        ensureSession(sid, biz, task);
        sessionInfo[sid].badges[badge] = true;
      }
    }else if(ev === "leave"){
      if(!badge) continue;
      if(active[badge]){
        // clamp to report range for duration calculation
        var clampedStart2 = Math.max(active[badge].t, reportSinceMs);
        var clampedEnd2 = Math.min(t, reportUntilMs);
        var durLeave = Math.max(0, clampedEnd2 - clampedStart2);
        addDur(badge, active[badge].biz, active[badge].task, durLeave);
        addTimeline(badge, active[badge].biz, active[badge].task, clampedStart2, clampedEnd2, "NORMAL", active[badge].session, active[badge].note);
        delete active[badge];
      }else{
        anomalies.leave_without_join++;
        addAnomaly("leave_without_join", badge, biz, task, t, "leave 无对应 join");
      }
    }
  }

  // 还在岗的按 reportUntilMs 结算（历史查询不会算到现在）
  Object.keys(active).forEach(function(b){
    anomalies.open++;
    var capMs = reportUntilMs;
    var durOpen = Math.max(0, capMs - Math.max(active[b].t, reportSinceMs));
    addDur(b, active[b].biz, active[b].task, durOpen);
    addTimeline(b, active[b].biz, active[b].task, active[b].t, capMs, "OPEN", active[b].session, active[b].note);
    addAnomaly("open_not_left", b, active[b].biz, active[b].task, capMs, "统计截止时仍在岗");
  });

  // 展平成表格
  var out = [];
  var people = [];
  Object.keys(acc).sort().forEach(function(badge){
    var obj = acc[badge];
    var tasks = obj.tasks || {};

    var taskRows = [];
    Object.keys(tasks).sort().forEach(function(k){
      var parts = k.split("|");
      var mins = msToMin_(tasks[k]);
      taskRows.push({ biz: parts[0]||"", task: parts[1]||"", minutes: mins });

      out.push({
        badge: badge, biz: parts[0]||"", task: parts[1]||"",
        minutes: mins, total_minutes: msToMin_(obj.total_ms)
      });
    });

    taskRows.sort(function(a,b){ return b.minutes - a.minutes; });
    people.push({ badge: badge, total_minutes: msToMin_(obj.total_ms), tasks: taskRows });
  });

  // 按类型排序：员工→长期日当→日当，同类型按工时降序
  var typeOrder = { "\u5458\u5DE5":0, "\u957F\u671F\u65E5\u5F53":1, "\u65E5\u5F53":2 };
  function daSuffix__(badge){
    var s = String(badge||"");
    if(!s.startsWith("DA-")) return "";
    var last = s.charAt(s.length - 1);
    return (last >= "A" && last <= "Z") ? last : "";
  }
  people.sort(function(a,b){
    var ta = typeOrder[badgeType_(a.badge)]; if(ta===undefined) ta=9;
    var tb = typeOrder[badgeType_(b.badge)]; if(tb===undefined) tb=9;
    if(ta !== tb) return ta - tb;
    if(ta === 2){
      var sa = daSuffix__(a.badge), sb = daSuffix__(b.badge);
      if(sa !== sb) return sa.localeCompare(sb);
    }
    return b.total_minutes - a.total_minutes;
  });

  var timeline = Object.keys(timelineByBadge).sort().map(function(badge){
    var items = (timelineByBadge[badge] || []).slice().sort(function(a,b){
      return (a.start_ms||0) - (b.start_ms||0);
    }).map(function(x){
      return {
        biz: x.biz, task: x.task,
        start_ms: x.start_ms, end_ms: x.end_ms,
        minutes: msToMin_(x.duration_ms), status: x.status,
        session: x.session, note: x.note
      };
    });
    return { badge: badge, items: items };
  });

  anomaliesList.sort(function(a,b){ return (b.at_ms||0) - (a.at_ms||0); });

  // Build detail_rows (工时明细): one row per work segment
  var detailRows = [];
  timeline.forEach(function(tl){
    (tl.items || []).forEach(function(it){
      detailRows.push({
        badge: tl.badge, biz: it.biz, task: it.task,
        session: it.session,
        join_time: fmtKST_(it.start_ms), leave_time: fmtKST_(it.end_ms),
        minutes: it.minutes, status: it.status, note: it.note
      });
    });
  });

  // Build session_summary (趟次汇总): one row per session
  var sessionSummary = [];
  Object.keys(sessionInfo).sort().forEach(function(sid){
    var s = sessionInfo[sid];
    var badgeList = Object.keys(s.badges).sort();
    var waveList = Object.keys(s.waves).sort();
    var totalMs = 0;
    // sum durations from timeline segments belonging to this session
    timeline.forEach(function(tl){
      (tl.items||[]).forEach(function(it){
        if(it.session === sid) totalMs += (it.minutes||0);
      });
    });
    sessionSummary.push({
      session: sid, biz: s.biz, task: s.task,
      start_time: fmtKST_(s.start_ms), end_time: fmtKST_(s.end_ms),
      total_minutes: totalMs, worker_count: badgeList.length,
      wave_count: waveList.length,
      wave_list: waveList.join("; "),
      workers: badgeList.join("; "),
      operator: s.operator
    });
  });

  REPORT_CACHE.summary = out;
  REPORT_CACHE.people = people;
  REPORT_CACHE.timeline = timeline;
  REPORT_CACHE.anomalies_list = anomaliesList;
  REPORT_CACHE.meta.anomalies = anomalies;
  REPORT_CACHE.detail_rows = detailRows;
  REPORT_CACHE.session_summary = sessionSummary;

  // Task efficiency: aggregate wave counts and person-minutes by biz|task
  var taskEff = {};
  sessionSummary.forEach(function(s){
    var k = s.biz + "|" + s.task;
    if(!taskEff[k]) taskEff[k] = { biz: s.biz, task: s.task, total_person_minutes: 0, total_waves: 0, workers: {} };
    taskEff[k].total_person_minutes += s.total_minutes;
    taskEff[k].total_waves += s.wave_count;
    s.workers.split("; ").forEach(function(w){ if(w) taskEff[k].workers[w] = true; });
  });
  REPORT_CACHE.task_efficiency = Object.keys(taskEff).map(function(k){
    var e = taskEff[k];
    var ph = e.total_person_minutes / 60;
    return {
      biz: e.biz, task: e.task,
      total_person_minutes: e.total_person_minutes,
      total_waves: e.total_waves,
      unique_workers: Object.keys(e.workers).length,
      person_hours: ph,
      efficiency: ph > 0 ? (e.total_waves / ph) : 0
    };
  }).filter(function(e){ return e.total_waves > 0; })
    .sort(function(a,b){ return b.total_person_minutes - a.total_person_minutes; });
}

function renderReport_(){
  var metaEl = document.getElementById("reportMeta");
  var overviewEl = document.getElementById("reportOverview");
  var efficiencyEl = document.getElementById("reportEfficiency");
  var anomaliesEl = document.getElementById("reportAnomalies");
  var peopleEl = document.getElementById("reportPeople");
  var timelineEl = document.getElementById("reportTimeline");
  var tableEl = document.getElementById("reportTable");

  var m = REPORT_CACHE.meta || {};
  var anomalies = (m.anomalies || {});
  var sum = REPORT_CACHE.summary || [];
  var people = REPORT_CACHE.people || [];
  var timeline = REPORT_CACHE.timeline || [];
  var anomaliesList = REPORT_CACHE.anomalies_list || [];
  var effList = REPORT_CACHE.task_efficiency || [];

  if(metaEl) metaEl.textContent =
    "区间(KST): " + (m.rangeLabel||"-") +
    " ｜ 事件数=" + (REPORT_CACHE.rows||[]).length +
    (anomalies.open > 0 ? " ｜ 仍在岗=" + anomalies.open : "");

  // ===== 总览卡片 =====
  if(overviewEl){
    var totalMinutes = 0;
    people.forEach(function(p){ totalMinutes += p.total_minutes; });
    var avgMinutes = people.length > 0 ? Math.round(totalMinutes / people.length) : 0;
    var totalCost = totalMinutes * REPORT_COST_PER_MIN;

    // 按人员类型统计
    var typeStats = {};
    people.forEach(function(p){
      var t = badgeType_(p.badge);
      if(!typeStats[t]) typeStats[t] = { count:0, minutes:0 };
      typeStats[t].count++;
      typeStats[t].minutes += p.total_minutes;
    });

    var ovHtml =
      '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:16px;">' +
        '<div style="background:#f0f7ff;border-radius:12px;padding:16px;text-align:center;">' +
          '<div style="font-size:32px;font-weight:900;color:#2c3e50;">' + people.length + '</div>' +
          '<div style="font-size:13px;color:#666;margin-top:4px;">出勤人数</div>' +
        '</div>' +
        '<div style="background:#f0fff4;border-radius:12px;padding:16px;text-align:center;">' +
          '<div style="font-size:32px;font-weight:900;color:#27ae60;">' + fmtHM_(totalMinutes) + '</div>' +
          '<div style="font-size:13px;color:#666;margin-top:4px;">总工时</div>' +
        '</div>' +
        '<div style="background:#fffbf0;border-radius:12px;padding:16px;text-align:center;">' +
          '<div style="font-size:32px;font-weight:900;color:#e67e22;">' + fmtHM_(avgMinutes) + '</div>' +
          '<div style="font-size:13px;color:#666;margin-top:4px;">人均工时</div>' +
        '</div>' +
        '<div style="background:#fff0f0;border-radius:12px;padding:16px;text-align:center;">' +
          '<div style="font-size:28px;font-weight:900;color:#e74c3c;">\u20A9' + esc(totalCost.toLocaleString()) + '</div>' +
          '<div style="font-size:13px;color:#666;margin-top:4px;">累计人力费</div>' +
        '</div>' +
      '</div>';

    // 人员类型分布
    var typeKeys = Object.keys(typeStats).sort();
    if(typeKeys.length > 0){
      ovHtml += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;">';
      typeKeys.forEach(function(t){
        var st = typeStats[t];
        ovHtml += '<span class="tag" style="font-size:13px;">' + esc(t) + ': ' + st.count + '人 / ' + fmtHM_(st.minutes) + '</span>';
      });
      ovHtml += '</div>';
    }

    // 任务汇总表
    var taskTotals = {};
    var taskWorkerSets = {};
    sum.forEach(function(s){
      var k = s.biz + "|" + s.task;
      taskTotals[k] = (taskTotals[k] || 0) + s.minutes;
      if(!taskWorkerSets[k]) taskWorkerSets[k] = {};
      taskWorkerSets[k][s.badge] = true;
    });
    var taskKeys = Object.keys(taskTotals).sort(function(a,b){ return taskTotals[b] - taskTotals[a]; });
    var totalTaskMin = 0;
    taskKeys.forEach(function(k){ totalTaskMin += taskTotals[k]; });

    if(taskKeys.length > 0){
      ovHtml += '<div class="listBox"><b>任务汇总</b><div style="margin-top:8px;overflow:auto;"><table style="border-collapse:collapse;width:100%;">';
      ovHtml += '<tr>' +
        '<th style="text-align:left;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">任务</th>' +
        '<th style="text-align:left;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">参与人数</th>' +
        '<th style="text-align:right;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">总工时</th>' +
        '<th style="text-align:right;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">人力费(\u20A9)</th>' +
        '<th style="text-align:right;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">占比</th>' +
      '</tr>';
      taskKeys.forEach(function(k){
        var mins = taskTotals[k];
        var badgeList = Object.keys(taskWorkerSets[k] || {});
        var workerTotal = badgeList.length;
        // 按类型统计人数
        var tc = {};
        badgeList.forEach(function(b){
          var t = badgeType_(b);
          tc[t] = (tc[t]||0) + 1;
        });
        var typeParts = [];
        if(tc["员工"]) typeParts.push("员工" + tc["员工"]);
        if(tc["长期日当"]) typeParts.push("长期" + tc["长期日当"]);
        if(tc["日当"]) typeParts.push("日当" + tc["日当"]);
        if(tc["其他"]) typeParts.push("其他" + tc["其他"]);
        var typeDetail = typeParts.length > 0 ? typeParts.join(" / ") : "";
        var pct = totalTaskMin > 0 ? Math.round(mins / totalTaskMin * 100) : 0;
        var cost = mins * REPORT_COST_PER_MIN;
        ovHtml += '<tr>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;font-size:13px;">' + esc(k.replace("|"," / ")) + '</td>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;font-size:13px;">' +
            '<b>' + workerTotal + '</b>人' +
            '<div style="font-size:11px;color:#888;">' + esc(typeDetail) + '</div>' +
          '</td>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;text-align:right;font-weight:700;font-size:13px;">' + fmtHM_(mins) + '</td>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;text-align:right;font-size:13px;color:#e74c3c;">' + esc(cost.toLocaleString()) + '</td>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;text-align:right;font-size:12px;">' +
            '<div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">' +
              '<div style="width:60px;height:8px;background:#f0f0f0;border-radius:4px;overflow:hidden;">' +
                '<div style="width:'+Math.max(2,pct)+'%;height:100%;background:#3498db;border-radius:4px;"></div>' +
              '</div>' + pct + '%</div></td></tr>';
      });
      ovHtml += '</table></div></div>';
    }
    overviewEl.innerHTML = ovHtml;
  }

  // 效率指标暂时隐藏（等WMS系统对接后再启用）
  if(efficiencyEl) efficiencyEl.innerHTML = '';

  // ===== 异常列表 =====
  if(anomaliesEl){
    if(anomaliesList.length===0){
      anomaliesEl.innerHTML = '';
    }else{
      anomaliesEl.innerHTML =
        '<div style="font-weight:700;margin-bottom:6px;">异常列表 (' + anomaliesList.length + ')</div>' +
        '<div style="max-height:300px;overflow:auto;">' +
        anomaliesList.map(function(a){
          return '<div style="border:1px solid #ffe2a8;background:#fffaf0;border-radius:12px;padding:10px;margin:8px 0;">' +
            '<div style="font-weight:700;">' + esc(badgeName_(a.badge) || a.badge || "?") + ' ｜ ' + esc(a.type) + '</div>' +
            '<div class="muted" style="margin-top:4px;">' + esc(a.biz + '/' + a.task) + ' ｜ ' + esc(fmtTs_(a.at_ms)) + '</div>' +
            (a.note ? '<div class="muted" style="margin-top:2px;">' + esc(a.note) + '</div>' : '') +
          '</div>';
        }).join('') + '</div>';
    }
  }

  // ===== 人员工时表 =====
  if(peopleEl){
    if(people.length===0){
      peopleEl.innerHTML = '<div class="muted">暂无人员汇总</div>';
    }else{
      var pHtml = '<div class="listBox"><b>人员工时（全部 ' + people.length + ' 人）</b><div style="margin-top:8px;overflow:auto;">';
      pHtml += '<table style="border-collapse:collapse;width:100%;">';
      pHtml += '<tr>' +
        '<th style="text-align:left;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">姓名</th>' +
        '<th style="text-align:left;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">类型</th>' +
        '<th style="text-align:right;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">总工时</th>' +
        '<th style="text-align:left;padding:6px 10px;border-bottom:2px solid #eee;font-size:13px;">任务明细</th>' +
      '</tr>';
      var maxMin = people[0] ? people[0].total_minutes : 1;
      people.forEach(function(p){
        var tasks = (p.tasks||[]).slice().sort(function(a,b){ return b.minutes - a.minutes; });
        var taskStr = tasks.map(function(t){
          var pct = p.total_minutes > 0 ? Math.round(t.minutes / p.total_minutes * 100) : 0;
          return t.biz + '/' + t.task + ' ' + fmtHM_(t.minutes) + '(' + pct + '%)';
        }).join(', ');
        var barW = maxMin > 0 ? Math.max(2, Math.round(p.total_minutes / maxMin * 100)) : 0;
        pHtml += '<tr>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;font-size:13px;font-weight:700;white-space:nowrap;">' + esc(badgeName_(p.badge)) + '</td>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;font-size:12px;color:#888;white-space:nowrap;">' + esc(badgeType_(p.badge)) + '</td>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;text-align:right;font-weight:700;font-size:13px;white-space:nowrap;">' +
            '<div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">' +
              '<div style="width:50px;height:8px;background:#f0f0f0;border-radius:4px;overflow:hidden;">' +
                '<div style="width:'+barW+'%;height:100%;background:#27ae60;border-radius:4px;"></div>' +
              '</div>' + fmtHM_(p.total_minutes) + '</div></td>' +
          '<td style="padding:6px 10px;border-bottom:1px solid #f5f5f5;font-size:12px;color:#666;">' + esc(taskStr) + '</td>' +
        '</tr>';
      });
      pHtml += '</table></div></div>';
      peopleEl.innerHTML = pHtml;
    }
  }

  // ===== 时间线（折叠） =====
  if(timelineEl){
    if(timeline.length===0){
      timelineEl.innerHTML = '';
    }else{
      var tlHtml = '<div style="margin-top:10px;">' +
        '<button onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\'none\'?\'block\':\'none\'" ' +
          'style="width:auto;min-width:160px;font-size:13px;">展开/收起时间线 (' + timeline.length + '人)</button>' +
        '<div style="display:none;margin-top:10px;">';
      tlHtml += timeline.map(function(x){
        var lines = (x.items || []).map(function(it){
          var statusText = it.status === "OPEN" ? "（未退出）" : (it.status === "AUTO_CLOSE_REJOIN" ? "（自动截断）" : "");
          var extra = "";
          if(it.note) extra += ' · <span style="color:#e67e22;">' + esc(it.note) + '</span>';
          return '<div style="border-top:1px dashed #eee;padding:4px 0;font-size:12px;">' +
            esc(it.biz + '/' + it.task) + ' ' + esc(String(it.minutes)) + '分 ' + esc(statusText) + extra +
            '<div class="muted" style="font-size:11px;">' + esc(fmtTs_(it.start_ms)) + ' → ' + esc(fmtTs_(it.end_ms)) + '</div></div>';
        }).join("");
        return '<div style="border:1px solid #eee;border-radius:12px;padding:10px;margin:8px 0;">' +
          '<div style="font-weight:700;">' + esc(badgeName_(x.badge)) + ' <span class="muted" style="font-weight:400;font-size:12px;">' + esc(x.badge) + '</span></div>' +
          (lines || '<div class="muted">无</div>') + '</div>';
      }).join("");
      tlHtml += '</div></div>';
      timelineEl.innerHTML = tlHtml;
    }
  }

  // ===== 明细表（折叠） =====
  if(tableEl){
    if(sum.length===0){
      tableEl.innerHTML = '<div class="muted">暂无数据</div>';
    } else {
      var dtHtml = '<div style="margin-top:10px;">' +
        '<button onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\'none\'?\'block\':\'none\'" ' +
          'style="width:auto;min-width:160px;font-size:13px;">展开/收起明细表 (' + sum.length + '行)</button>' +
        '<div style="display:none;margin-top:10px;overflow:auto;border:1px solid #eee;border-radius:12px;">' +
        '<table style="border-collapse:collapse;width:100%;min-width:600px;">' +
        '<tr>' +
          '<th style="text-align:left;border-bottom:1px solid #eee;padding:6px 8px;background:#fafafa;font-size:12px;">姓名</th>' +
          '<th style="text-align:left;border-bottom:1px solid #eee;padding:6px 8px;background:#fafafa;font-size:12px;">任务</th>' +
          '<th style="text-align:right;border-bottom:1px solid #eee;padding:6px 8px;background:#fafafa;font-size:12px;">工时</th>' +
          '<th style="text-align:right;border-bottom:1px solid #eee;padding:6px 8px;background:#fafafa;font-size:12px;">个人合计</th>' +
        '</tr>';
      for(var i=0;i<sum.length;i++){
        var r = sum[i];
        dtHtml += '<tr>' +
          '<td style="border-bottom:1px solid #f2f2f2;padding:6px 8px;font-size:12px;">' + esc(badgeName_(r.badge)) + '</td>' +
          '<td style="border-bottom:1px solid #f2f2f2;padding:6px 8px;font-size:12px;">' + esc(r.biz + '/' + r.task) + '</td>' +
          '<td style="border-bottom:1px solid #f2f2f2;padding:6px 8px;text-align:right;font-size:12px;">' + fmtHM_(r.minutes) + '</td>' +
          '<td style="border-bottom:1px solid #f2f2f2;padding:6px 8px;text-align:right;font-weight:700;font-size:12px;">' + fmtHM_(r.total_minutes) + '</td>' +
        '</tr>';
      }
      dtHtml += '</table></div></div>';
      tableEl.innerHTML = dtHtml;
    }
  }
}

function csvVal_(v){
  var s = String(v==null?"":v);
  if(s.indexOf(",")>=0 || s.indexOf('"')>=0 || s.indexOf("\n")>=0)
    return '"' + s.replace(/"/g,'""') + '"';
  return s;
}

function downloadCSV_(filename, headerArr, dataRows){
  var csv = [];
  csv.push(headerArr.join(","));
  for(var i=0;i<dataRows.length;i++){
    csv.push(dataRows[i].map(csvVal_).join(","));
  }
  var blob = new Blob(["\uFEFF" + csv.join("\n")], {type:"text/csv;charset=utf-8;"});
  var a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
}

function reportExportCSV(){
  reportExportDetailCSV();
}

function reportExportDetailCSV(){
  if(!adminIsUnlocked_()){
    alert("管理员功能：请先解锁（标题连点 7 次）");
    return;
  }
  var detail = REPORT_CACHE.detail_rows || [];
  if(detail.length===0){
    alert("没有可导出的工时明细（先拉取数据）");
    return;
  }
  var h = ["工牌","业务线","任务","趟次session","加入时间(KST)","退出时间(KST)","工时(分钟)","状态","备注"];
  var data = detail.map(function(r){
    return [r.badge, r.biz, r.task, r.session, r.join_time, r.leave_time, r.minutes, r.status, r.note];
  });
  var rangeText = (REPORT_CACHE.meta.rangeLabel || "range").replace(/[^0-9A-Za-z_-]+/g, "_");
  downloadCSV_("ck_工时明细_" + rangeText + "_" + Date.now() + ".csv", h, data);
}

function reportExportSessionCSV(){
  if(!adminIsUnlocked_()){
    alert("管理员功能：请先解锁（标题连点 7 次）");
    return;
  }
  var ss = REPORT_CACHE.session_summary || [];
  if(ss.length===0){
    alert("没有可导出的趟次汇总（先拉取数据）");
    return;
  }
  var h = ["趟次session","业务线","任务","开始时间(KST)","结束时间(KST)","总工时(人*分钟)","参与人数","单号数量","单号列表","参与人员","发起人"];
  var data = ss.map(function(r){
    return [r.session, r.biz, r.task, r.start_time, r.end_time, r.total_minutes, r.worker_count, r.wave_count, r.wave_list, r.workers, r.operator];
  });
  var rangeText = (REPORT_CACHE.meta.rangeLabel || "range").replace(/[^0-9A-Za-z_-]+/g, "_");
  downloadCSV_("ck_趟次汇总_" + rangeText + "_" + Date.now() + ".csv", h, data);
}

function reportGenerateDaily(){
  var people = REPORT_CACHE.people || [];
  var effList = REPORT_CACHE.task_efficiency || [];
  var m = REPORT_CACHE.meta || {};
  if(people.length === 0){
    alert("请先拉取报表数据");
    return;
  }

  var totalMinutes = 0;
  people.forEach(function(p){ totalMinutes += p.total_minutes; });
  var avgMinutes = people.length > 0 ? Math.round(totalMinutes / people.length) : 0;
  var totalCost = totalMinutes * REPORT_COST_PER_MIN;

  // 按人员类型统计
  var typeStats = {};
  people.forEach(function(p){
    var t = badgeType_(p.badge);
    if(!typeStats[t]) typeStats[t] = { count:0, minutes:0 };
    typeStats[t].count++;
    typeStats[t].minutes += p.total_minutes;
  });

  // 按任务统计
  var taskTotals = {};
  var taskWorkerSets = {};
  var sum = REPORT_CACHE.summary || [];
  sum.forEach(function(r){
    var k = r.biz + "/" + r.task;
    taskTotals[k] = (taskTotals[k] || 0) + r.minutes;
    if(!taskWorkerSets[k]) taskWorkerSets[k] = {};
    taskWorkerSets[k][r.badge] = true;
  });

  var lines = [];
  lines.push("====== CK 仓库日报 ======");
  lines.push("日期范围(KST): " + (m.rangeLabel || "-"));
  lines.push("生成时间: " + new Date().toLocaleString("zh-CN", {timeZone:"Asia/Seoul"}));
  lines.push("");
  lines.push("【概览】");
  lines.push("  出勤人数: " + people.length + " 人");
  lines.push("  总工时: " + fmtHM_(totalMinutes));
  lines.push("  人均工时: " + fmtHM_(avgMinutes));
  lines.push("  累计人力费: ₩" + totalCost.toLocaleString());
  lines.push("");

  // 人员类型分布
  var typeKeys = Object.keys(typeStats);
  if(typeKeys.length > 0){
    lines.push("【人员分布】");
    typeKeys.forEach(function(t){
      var s = typeStats[t];
      lines.push("  " + t + ": " + s.count + "人, " + fmtHM_(s.minutes));
    });
    lines.push("");
  }

  // 任务汇总
  var taskKeys = Object.keys(taskTotals).sort(function(a,b){ return taskTotals[b] - taskTotals[a]; });
  if(taskKeys.length > 0){
    lines.push("【任务汇总】");
    var totalTaskMin = 0;
    taskKeys.forEach(function(k){ totalTaskMin += taskTotals[k]; });
    taskKeys.forEach(function(k){
      var mins = taskTotals[k];
      var badgeList = Object.keys(taskWorkerSets[k] || {});
      var tc = {};
      badgeList.forEach(function(b){ var t = badgeType_(b); tc[t] = (tc[t]||0) + 1; });
      var tp = [];
      if(tc["员工"]) tp.push("员工"+tc["员工"]);
      if(tc["长期日当"]) tp.push("长期"+tc["长期日当"]);
      if(tc["日当"]) tp.push("日当"+tc["日当"]);
      var pct = totalTaskMin > 0 ? Math.round(mins / totalTaskMin * 100) : 0;
      var cost = mins * REPORT_COST_PER_MIN;
      lines.push("  " + k + " | " + badgeList.length + "人(" + tp.join("/") + ") | " + fmtHM_(mins) + " | ₩" + cost.toLocaleString() + " | " + pct + "%");
    });
    lines.push("");
  }

  // 人员明细
  lines.push("【人员明细】");
  people.forEach(function(p){
    var tasks = (p.tasks||[]).slice().sort(function(a,b){ return b.minutes - a.minutes; });
    var taskStr = tasks.map(function(t){
      var pct = p.total_minutes > 0 ? Math.round(t.minutes / p.total_minutes * 100) : 0;
      return t.biz + "/" + t.task + " " + fmtHM_(t.minutes) + "(" + pct + "%)";
    }).join(", ");
    lines.push("  " + badgeName_(p.badge) + " [" + badgeType_(p.badge) + "] " + fmtHM_(p.total_minutes) + " → " + taskStr);
  });
  lines.push("");
  lines.push("========================");

  var text = lines.join("\n");
  var outputDiv = document.getElementById("reportDailyOutput");
  if(outputDiv){
    outputDiv.style.display = "block";
    var ta = outputDiv.querySelector("textarea");
    if(ta) ta.value = text;
  }
}

function copyDailyReport(){
  var outputDiv = document.getElementById("reportDailyOutput");
  if(!outputDiv) return;
  var ta = outputDiv.querySelector("textarea");
  if(!ta || !ta.value){
    alert("没有日报内容，请先生成");
    return;
  }
  ta.select();
  try{
    document.execCommand("copy");
    alert("已复制到剪贴板 ✓");
  }catch(e){
    // fallback for modern browsers
    navigator.clipboard.writeText(ta.value).then(function(){
      alert("已复制到剪贴板 ✓");
    }).catch(function(){
      alert("复制失败，请手动选择复制");
    });
  }
}

// ===== 补录修正 =====
var CORR_TASK_MAP = {
  "B2C": ["理货","拣货","打包","退件入库","质检","废弃处理","换单","B2C盘点","批量出库"],
  "B2B": ["B2B卸货","B2B入库理货","B2B工单操作","B2B现场记录","B2B出库","B2B盘点"],
  "进口": ["卸货","过机扫描码托","装柜/出货","取/送货","问题处理"],
  "仓库": ["仓库整理"]
};
var _corrHistory = []; // 本次session的补录记录

function corrUpdateTasks(){
  var biz = document.getElementById("corrBiz").value;
  var sel = document.getElementById("corrTask");
  sel.innerHTML = '<option value="">-- 选择任务 --</option>';
  var tasks = CORR_TASK_MAP[biz] || [];
  tasks.forEach(function(t){
    sel.innerHTML += '<option value="' + esc(t) + '">' + esc(t) + '</option>';
  });
}

function corrKstToMs_(datetimeLocal){
  // datetime-local 是用户本地时间，但我们要求用户输入的是KST
  // 解析为 "YYYY-MM-DDTHH:MM" 当作 KST(UTC+9) 转成 ms
  if(!datetimeLocal) return 0;
  var parts = datetimeLocal.split("T");
  if(parts.length !== 2) return 0;
  var iso = parts[0] + "T" + parts[1] + ":00.000+09:00";
  var ms = new Date(iso).getTime();
  return isNaN(ms) ? 0 : ms;
}

function corrFmtKst_(ms){
  if(!ms) return "-";
  var d = new Date(ms + 9*3600*1000);
  return d.getUTCFullYear() + "-" + pad2_(d.getUTCMonth()+1) + "-" + pad2_(d.getUTCDate()) +
    " " + pad2_(d.getUTCHours()) + ":" + pad2_(d.getUTCMinutes());
}

function corrPreview(){
  if(!adminIsUnlocked_()){
    alert("请先解锁管理员模式（标题连点7次）");
    return;
  }
  var badge = document.getElementById("corrBadge").value.trim();
  var biz = document.getElementById("corrBiz").value;
  var task = document.getElementById("corrTask").value;
  var joinTime = document.getElementById("corrJoinTime").value;
  var leaveTime = document.getElementById("corrLeaveTime").value;
  var session = document.getElementById("corrSession").value.trim();
  var note = document.getElementById("corrNote").value.trim() || "manual_correction";

  if(!badge){ alert("请输入工牌"); return; }
  if(!biz){ alert("请选择业务线"); return; }
  if(!task){ alert("请选择任务"); return; }
  if(!joinTime){ alert("请选择加入时间"); return; }
  if(!leaveTime){ alert("请选择退出时间"); return; }

  var joinMs = corrKstToMs_(joinTime);
  var leaveMs = corrKstToMs_(leaveTime);
  if(!joinMs || !leaveMs){ alert("时间格式错误"); return; }
  if(leaveMs <= joinMs){ alert("退出时间必须晚于加入时间"); return; }

  var durMin = Math.round((leaveMs - joinMs) / 60000);
  if(durMin > 720){ alert("工时超过12小时（" + durMin + "分钟），请确认时间是否正确"); return; }

  if(!session){
    // 自动生成 session
    var kd = new Date(joinMs + 9*3600*1000);
    session = "PS-" + kd.getUTCFullYear().toString().slice(2) + pad2_(kd.getUTCMonth()+1) + pad2_(kd.getUTCDate()) +
      "-" + pad2_(kd.getUTCHours()) + pad2_(kd.getUTCMinutes()) + "00-CORR";
  }

  var previewEl = document.getElementById("corrPreviewArea");
  var submitEl = document.getElementById("corrSubmitArea");
  previewEl.style.display = "block";
  submitEl.style.display = "block";
  previewEl.innerHTML =
    '<div style="font-weight:700;margin-bottom:6px;">预览补录内容：</div>' +
    '<div>工牌: <b>' + esc(badge) + '</b></div>' +
    '<div>业务/任务: <b>' + esc(biz) + ' / ' + esc(task) + '</b></div>' +
    '<div>Session: <b>' + esc(session) + '</b></div>' +
    '<div>加入时间(KST): <b>' + esc(corrFmtKst_(joinMs)) + '</b></div>' +
    '<div>退出时间(KST): <b>' + esc(corrFmtKst_(leaveMs)) + '</b></div>' +
    '<div>工时: <b>' + durMin + ' 分钟 (' + fmtHM_(durMin) + ')</b></div>' +
    '<div>备注: ' + esc(note) + '</div>';

  // 暂存数据供提交用
  previewEl.dataset.badge = badge;
  previewEl.dataset.biz = biz;
  previewEl.dataset.task = task;
  previewEl.dataset.session = session;
  previewEl.dataset.joinMs = joinMs;
  previewEl.dataset.leaveMs = leaveMs;
  previewEl.dataset.note = note;
}

async function corrSubmit(){
  if(!adminIsUnlocked_()){
    alert("请先解锁管理员模式");
    return;
  }
  var previewEl = document.getElementById("corrPreviewArea");
  var resultEl = document.getElementById("corrResult");
  var badge = previewEl.dataset.badge;
  var biz = previewEl.dataset.biz;
  var task = previewEl.dataset.task;
  var session = previewEl.dataset.session;
  var joinMs = Number(previewEl.dataset.joinMs);
  var leaveMs = Number(previewEl.dataset.leaveMs);
  var note = previewEl.dataset.note || "manual_correction";

  if(!badge || !biz || !task || !joinMs || !leaveMs){
    alert("数据异常，请重新预览");
    return;
  }

  var ok = confirm(
    "确认补录？\n\n" +
    "工牌: " + badge + "\n" +
    "任务: " + biz + " / " + task + "\n" +
    "时间: " + corrFmtKst_(joinMs) + " ~ " + corrFmtKst_(leaveMs) + "\n\n" +
    "提交后将写入两条事件（join + leave），报表中会体现此工时。"
  );
  if(!ok) return;

  resultEl.textContent = "提交中... ";

  try{
    // 原子写入 join+leave（后端 D1 batch 事务，不会出现只成功一半）
    var r = await fetchApi({
      action: "admin_manual_correction_pair",
      k: adminKey_(),
      badge: badge, biz: biz, task: task, session: session,
      join_ms: joinMs, leave_ms: leaveMs,
      operator_id: getOperatorId() || "",
      note: note
    });
    if(!r || r.ok !== true){
      resultEl.textContent = "补录失败（整次未生效）: " + (r && r.error ? r.error : "unknown");
      return;
    }

    var durMin = Math.round((leaveMs - joinMs) / 60000);
    resultEl.innerHTML = '<span style="color:#27ae60;font-weight:700;">补录成功!</span> ' +
      esc(badge) + ' | ' + esc(biz+'/'+task) + ' | ' + fmtHM_(durMin);

    // 记录到本地历史
    _corrHistory.unshift({
      badge: badge, biz: biz, task: task, session: session,
      joinMs: joinMs, leaveMs: leaveMs, durMin: durMin, note: note,
      at: Date.now()
    });
    corrRenderHistory_();

    // 清空表单
    document.getElementById("corrPreviewArea").style.display = "none";
    document.getElementById("corrSubmitArea").style.display = "none";
    document.getElementById("corrJoinTime").value = "";
    document.getElementById("corrLeaveTime").value = "";
    document.getElementById("corrNote").value = "";
  }catch(e){
    resultEl.textContent = "提交异常: " + e;
  }
}

function corrRenderHistory_(){
  var el = document.getElementById("corrHistory");
  if(!el) return;
  if(_corrHistory.length === 0){ el.textContent = "无"; return; }
  var html = '';
  _corrHistory.forEach(function(h){
    html += '<div style="border:1px solid #e0e0e0;border-radius:8px;padding:8px;margin:6px 0;font-size:13px;">' +
      '<b>' + esc(h.badge) + '</b> | ' + esc(h.biz + '/' + h.task) + ' | ' + fmtHM_(h.durMin) +
      '<div class="muted" style="font-size:12px;">' + corrFmtKst_(h.joinMs) + ' ~ ' + corrFmtKst_(h.leaveMs) +
      (h.note ? ' | ' + esc(h.note) : '') + '</div></div>';
  });
  el.innerHTML = html;
}

function adminLogout(){
  adminClear_();
  alert("已退出管理员模式");
  setHash("home");
}


/** ===== WMS Data Import Module ===== */
var _wmsWorkbook = null;
var _wmsFileName = "";
var _wmsPreviewRows = [];  // [{header:[], rows:[[]]}]
// _wmsReuseBatchId 已移除，改为 wmsConfirmImport() 内局部变量 reuseBatchId
var _wmsCurrentSheet = "";

function wmsSetStatus_(msg, ok){
  var el = document.getElementById("wmsStatus");
  if(el){
    el.textContent = msg;
    el.style.color = ok === false ? "#e74c3c" : ok === true ? "#27ae60" : "#666";
  }
}

function wmsFileSelected(input){
  var file = input && input.files && input.files[0];
  wmsClearPreview();
  if(!file) return;

  if(typeof XLSX === "undefined" || !window.XLSX){
    wmsSetStatus_("❌ SheetJS 库未加载（XLSX is not defined）。请检查网络后刷新页面重试。", false);
    return;
  }

  _wmsFileName = file.name;
  wmsSetStatus_("已选择：" + file.name + "（" + (file.size/1024).toFixed(1) + " KB）正在解析... ⏳");

  var reader = new FileReader();
  reader.onerror = function(){
    wmsSetStatus_("❌ 文件读取失败：" + (reader.error || "unknown"), false);
  };
  reader.onload = function(e){
    try{
      var data = new Uint8Array(e.target.result);
      _wmsWorkbook = XLSX.read(data, { type:"array", cellDates:true, dateNF:'yyyy-mm-dd hh:mm:ss' });
      var names = _wmsWorkbook.SheetNames || [];
      if(names.length === 0){
        wmsSetStatus_("❌ 文件中没有任何 Sheet", false);
        return;
      }
      wmsSetStatus_("✅ 解析成功：" + _wmsFileName + " — " + names.length + " 个 Sheet", true);
      // 显示 sheet 选择器
      var sel = document.getElementById("wmsSheetSelect");
      sel.innerHTML = "";
      names.forEach(function(n){
        var opt = document.createElement("option");
        opt.value = n; opt.textContent = n;
        sel.appendChild(opt);
      });
      document.getElementById("wmsSheetSelector").style.display = "block";
      // 自动预览第一个 sheet
      wmsSheetChanged();
    }catch(ex){
      wmsSetStatus_("❌ 解析失败：" + String(ex), false);
    }
  };
  reader.readAsArrayBuffer(file);
}

function wmsSheetChanged(){
  var sel = document.getElementById("wmsSheetSelect");
  _wmsCurrentSheet = sel.value;
  if(!_wmsWorkbook || !_wmsCurrentSheet) return;
  var ws = _wmsWorkbook.Sheets[_wmsCurrentSheet];
  if(!ws){ alert("Sheet 不存在"); return; }

  // 转为 JSON（第一行作为表头）
  var rawRows = XLSX.utils.sheet_to_json(ws, { header:1, raw:false, dateNF:'yyyy-mm-dd hh:mm:ss' });
  // 过滤全空行
  rawRows = rawRows.filter(function(r){
    return r.some(function(c){ return c !== null && c !== undefined && String(c).trim() !== ""; });
  });
  if(rawRows.length === 0){ alert("Sheet 为空"); return; }

  var header = rawRows[0].map(function(h){ return String(h||"").trim(); });
  var dataRows = rawRows.slice(1);

  _wmsPreviewRows = { header: header, rows: dataRows, totalRows: dataRows.length };

  // 显示预览
  var meta = document.getElementById("wmsPreviewMeta");
  meta.textContent = "📄 " + _wmsFileName + " → Sheet: " + _wmsCurrentSheet + " | 字段: " + header.length + " | 数据行: " + dataRows.length;

  var table = document.getElementById("wmsPreviewTable");
  var html = "<thead><tr>";
  header.forEach(function(h){ html += "<th style='border:1px solid #ddd;padding:4px 8px;background:#f5f5f5;white-space:nowrap;'>" + esc(h) + "</th>"; });
  html += "</tr></thead><tbody>";
  var previewCount = Math.min(20, dataRows.length);
  for(var i=0;i<previewCount;i++){
    html += "<tr>";
    for(var j=0;j<header.length;j++){
      var val = (dataRows[i] && dataRows[i][j] !== undefined && dataRows[i][j] !== null) ? String(dataRows[i][j]) : "";
      html += "<td style='border:1px solid #eee;padding:3px 6px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>" + esc(val) + "</td>";
    }
    html += "</tr>";
  }
  if(dataRows.length > 20){
    html += "<tr><td colspan='" + header.length + "' style='text-align:center;color:#999;padding:6px;'>... 还有 " + (dataRows.length - 20) + " 行 ...</td></tr>";
  }
  html += "</tbody>";
  table.innerHTML = html;

  document.getElementById("wmsPreviewArea").style.display = "block";
  document.getElementById("wmsImportResult").textContent = "";
}

function wmsContentFingerprint_(header, rows){
  function norm(v){ return (v == null ? "" : String(v)).trim().replace(/\r\n/g,"\n").replace(/\r/g,"\n"); }
  var parts = [];
  parts.push("H:" + header.map(norm).join("\t"));
  parts.push("N:" + rows.length);
  for(var i=0;i<rows.length;i++){
    var r = rows[i];
    if(Array.isArray(r)) parts.push(r.map(norm).join("\t"));
    else{
      var vals = [];
      for(var j=0;j<header.length;j++) vals.push(norm(r[header[j]]));
      parts.push(vals.join("\t"));
    }
  }
  var s = parts.join("\n");
  // djb2 hash
  var h = 5381;
  for(var k=0;k<s.length;k++) h = ((h << 5) + h + s.charCodeAt(k)) & 0xFFFFFFFF;
  return (h >>> 0).toString(16);
}

function wmsClearPreview(){
  _wmsWorkbook = null;
  _wmsFileName = "";
  _wmsPreviewRows = [];
  _wmsCurrentSheet = "";
  document.getElementById("wmsSheetSelector").style.display = "none";
  document.getElementById("wmsPreviewArea").style.display = "none";
  document.getElementById("wmsImportResult").textContent = "";
  var input = document.getElementById("wmsFileInput");
  if(input) input.value = "";
  var descEl = document.getElementById("wmsSourceTypeDesc");
  if(descEl) descEl.textContent = "";
}

async function wmsConfirmImport(){
  if(!_wmsPreviewRows || !_wmsPreviewRows.header || !_wmsPreviewRows.rows){
    alert("没有可导入的数据"); return;
  }
  if(!adminIsUnlocked_()){ alert("请先解锁管理员模式"); return; }

  var sourceType = document.getElementById("wmsSourceType").value;
  if(!sourceType){ alert("请先选择数据类型"); return; }

  var businessDay = (document.getElementById("wmsBusinessDay") || {}).value || "";
  if(sourceType === "b2c_pack_import"){
    if(!businessDay || !/^\d{4}-\d{2}-\d{2}$/.test(businessDay)){
      alert("进口打包表必须填写业务日期（YYYY-MM-DD）"); return;
    }
  }
  if(businessDay && !/^\d{4}-\d{2}-\d{2}$/.test(businessDay)){
    alert("业务日期格式不合法，需要 YYYY-MM-DD"); return;
  }

  var header = _wmsPreviewRows.header;
  var rows = _wmsPreviewRows.rows;
  if(rows.length === 0){ alert("数据为空"); return; }

  // 重复导入检测：内容指纹硬拦截 + 文件名软提醒
  var reuseBatchId = "";  // partial 重试时复用的 batch_id（局部变量，不跨调用残留）
  var fingerprint = wmsContentFingerprint_(header, rows);
  try{
    var dupRes = await fetchApi({
      action: "wms_check_duplicate",
      k: adminKey_(),
      source_file: _wmsFileName,
      sheet_name: _wmsCurrentSheet,
      row_count: rows.length,
      content_fingerprint: fingerprint,
      source_type: sourceType
    });
    if(dupRes && dupRes.ok && dupRes.block){
      // 硬拦截：已有 completed 批次，内容完全相同
      var bm = (dupRes.block_matches||[]).map(function(m){
        return "文件: " + (m.source_file||"?") + " | Sheet: " + (m.sheet_name||"?") + " | " + (m.total_rows||0) + "行(已入" + (m.inserted_rows||0) + ") | 导入时间: " + fmtKST_(m.created_ms) + " | 批次: " + (m.import_batch_id||"?");
      }).join("\n");
      alert("❌ 禁止重复导入！\n\n该文件内容与已完成的导入完全相同（source_type + 内容指纹匹配）。\n\n已有记录：\n" + bm);
      return;
    }
    if(dupRes && dupRes.ok && dupRes.partial_warn){
      // 部分导入提醒：复用最新 partial batch 的 import_batch_id
      var sorted = (dupRes.partial_matches||[]).slice().sort(function(a,b){ return (b.updated_ms||b.created_ms||0) - (a.updated_ms||a.created_ms||0); });
      var pm = sorted.map(function(m){
        return "批次: " + (m.import_batch_id||"?") + " | 已入 " + (m.inserted_rows||0) + "/" + (m.total_rows||"?") + "行 | " + fmtKST_(m.updated_ms||m.created_ms);
      }).join("\n");
      if(!confirm("⚠️ 发现未完成的同内容导入记录：\n" + pm + "\n\n将复用已有批次继续导入，已成功的行会自动跳过。\n确定继续导入吗？")){
        return;
      }
      reuseBatchId = sorted[0].import_batch_id;
    }
    if(dupRes && dupRes.ok && dupRes.has_name_duplicate){
      // 软提醒：文件名/Sheet/行数相同，但内容不同
      var ni = (dupRes.name_matches||[]).map(function(m){ return fmtKST_(m.created_ms) + " (" + (m.source_file||"?") + ")"; }).join(", ");
      if(!confirm("⚠️ 发现同文件名/Sheet/行数的历史导入记录：\n" + ni + "\n\n内容指纹不同，可能是更新版数据。\n确定继续导入吗？")){
        return;
      }
    }
  }catch(e){ wmsSetStatus_("⚠️ 重复检测失败（导入可继续）: " + e, false); }

  if(!confirm("确认导入 " + rows.length + " 行数据到后端？\n\n文件：" + _wmsFileName + "\nSheet：" + _wmsCurrentSheet)) return;

  // 生成本次导入批次ID（partial 重试时复用已有 batch_id）
  var batchId = reuseBatchId || ("WMS-" + Date.now() + "-" + Math.random().toString(36).slice(2,8));

  // 构建导入数据：每行转为 {header[i]: value} 的对象
  var records = [];
  for(var i=0;i<rows.length;i++){
    var obj = {};
    for(var j=0;j<header.length;j++){
      var val = (rows[i] && rows[i][j] !== undefined && rows[i][j] !== null) ? String(rows[i][j]) : "";
      obj[header[j]] = val;
    }
    records.push(obj);
  }

  var resultEl = document.getElementById("wmsImportResult");
  resultEl.textContent = "正在提交 " + records.length + " 行... ⏳";

  // 分批提交（每批200行，避免请求体过大），共用同一个 batchId
  var BATCH = 200;
  var totalInserted = 0;
  var totalSkipped = 0;
  var errors = [];
  var totalSummary = { s_empty_order:0, s_zero_qty:0, s_empty_bizday:0, s_loc_unknown:0 };

  for(var b=0; b<records.length; b+=BATCH){
    var batch = records.slice(b, b+BATCH);
    try{
      var res = await fetchApi({
        action: "wms_import",
        k: adminKey_(),
        import_batch_id: batchId,
        row_offset: b,
        total_rows: records.length,
        content_fingerprint: fingerprint,
        source_type: sourceType,
        business_day_kst: businessDay,
        source_file: _wmsFileName,
        sheet_name: _wmsCurrentSheet,
        header: header,
        rows: batch
      });
      if(res && res.ok){
        totalInserted += (res.inserted || 0);
        totalSkipped += (res.skipped || 0);
        if(res.summary){
          totalSummary.s_empty_order += (res.summary.s_empty_order||0);
          totalSummary.s_zero_qty += (res.summary.s_zero_qty||0);
          totalSummary.s_empty_bizday += (res.summary.s_empty_bizday||0);
          totalSummary.s_loc_unknown += (res.summary.s_loc_unknown||0);
        }
      }else{
        errors.push("批次" + (Math.floor(b/BATCH)+1) + "失败: " + (res && res.error ? res.error : "unknown"));
      }
    }catch(e){
      errors.push("批次" + (Math.floor(b/BATCH)+1) + "异常: " + e);
    }
    resultEl.textContent = "已提交 " + Math.min(b+BATCH, records.length) + "/" + records.length + " 行... ⏳";
  }

  // 构建校验摘要
  var sourceLabel = {"b2c_order_export":"B2C订单表","b2c_pack_import":"进口打包表","import_express":"进口快件表","change_order_export":"换单表","return_inbound_export":"退件入库表","return_qc_export":"质检表"}[sourceType] || sourceType;
  var lines = [sourceLabel + ": ✅ 插入 " + totalInserted + " 行"];
  if(totalSkipped) lines.push("⏭️ 跳过(重复/汇总) " + totalSkipped + " 行");

  // 校验项：根据 source_type 决定 severity
  var warnings = [];
  var criticals = [];
  if(totalSummary.s_empty_order > 0) warnings.push(totalSummary.s_empty_order + " 行订单号为空");
  if(totalSummary.s_zero_qty > 0) warnings.push(totalSummary.s_zero_qty + " 行数量为0");
  if(totalSummary.s_loc_unknown > 0) warnings.push(totalSummary.s_loc_unknown + " 行储位类型未知");
  if(totalSummary.s_empty_bizday > 0){
    if(sourceType === "b2c_pack_import"){
      criticals.push(totalSummary.s_empty_bizday + " 行业务日期为空");
    }else{
      warnings.push(totalSummary.s_empty_bizday + " 行业务日期为空");
    }
  }
  if(criticals.length) lines.push("❌ " + criticals.join(", "));
  if(warnings.length) lines.push("⚠️ " + warnings.join(", "));
  if(!criticals.length && !warnings.length) lines.push("🟢 数据质量无异常");
  if(errors.length) lines.push("❗ 错误: " + errors.join("; "));

  var msg = lines.join("\n");

  // 导入成功后清空界面，回到初始状态
  wmsClearPreview();
  wmsSetStatus_(msg, errors.length === 0 && criticals.length === 0);
  alert(msg);
  wmsLoadRecent();
}

async function wmsLoadRecent(){
  var el = document.getElementById("wmsRecentImports");
  if(!el) return;
  if(!adminIsUnlocked_()){ el.textContent = "需要管理员权限"; return; }
  el.textContent = "加载中...";
  try{
    var res = await fetchApi({ action:"wms_list", k:adminKey_(), limit:30 });
    if(!res || !res.ok){ el.textContent = "加载失败: " + (res&&res.error?res.error:"unknown"); return; }
    var batches = res.batches || [];
    if(batches.length === 0){ el.textContent = "暂无导入记录"; return; }
    var html = "<div style='font-size:13px;'>";
    batches.forEach(function(b){
      var bid = b.import_batch_id || "";
      var label = bid.indexOf("LEGACY-") === 0 ? "历史导入" : (bid || "未知批次");
      var rowInfo = b.total_rows ? (b.inserted_rows||0) + "/" + b.total_rows + " 行" : (b.row_count||0) + " 行";
      var statusTag = b.status === "partial" ? ' <span style="color:#e65100;">⏳未完成</span>' : (b.status === "completed" ? ' <span style="color:#388e3c;">✅</span>' : "");
      html += "<div style='padding:6px 0;border-bottom:1px solid #f0f0f0;'>" +
        "<div><b>" + esc(b.source_file||"?") + "</b> → " + esc(b.sheet_name||"?") + statusTag + "</div>" +
        "<div style='color:#666;font-size:12px;'>" + rowInfo + " | " + fmtKST_(b.updated_ms||b.created_ms) + " | " + esc(label) + "</div>" +
        "</div>";
    });
    html += "</div>";
    el.innerHTML = html;
  }catch(e){
    el.textContent = "加载异常: " + e;
  }
}

// ===== WMS source_type 映射描述 =====
var WMS_SOURCE_DESC = {
  "b2c_order_export": "B2C订单表\n波次号→拣货波次 | 物流单号→订单号 | 商品数量→件数 | 发货时间→完成日期 | 储位号→储位(查表映射大/小货位) | 箱型编码→箱型",
  "b2c_pack_import": "进口打包表\n分拣单号→拣货波次 | 物流单号→订单号 | 货品总数量→件数 | 货主→owner | 储位类型固定=小货位\n⚠️ 发货时间不可信，需手动指定业务日期",
  "import_express": "进口快件表\n运单号→订单号 | 称重重量→重量 | 发货单位→owner | 入库时间→完成日期 | 件数固定=1",
  "change_order_export": "换单表\n快递单号→订单号(主产出键) | 发货时间→完成日期 | 商户名称→owner | 状态→已发出才计入产出 | 件数固定=1",
  "return_inbound_export": "退件入库表\n包裹号→订单号(主产出键) | 仓库签收时间→完成日期 | 商户名称→owner | 数量→件数 | 重量(KG)→重量 | 体积→体积 | 不过滤状态",
  "return_qc_export": "质检表\n包裹号→订单号 | 质检时间→完成日期 | 商户名称→owner | 数量→件数(产出单位) | 重量(KG)→重量 | 体积→体积 | 只统计 质检=已质检"
};
(function(){
  var sel = document.getElementById("wmsSourceType");
  var desc = document.getElementById("wmsSourceTypeDesc");
  var bdWrap = document.getElementById("wmsBusinessDayWrap");
  var bdHint = document.getElementById("wmsBusinessDayHint");
  if(sel && desc){
    sel.addEventListener("change", function(){
      desc.textContent = WMS_SOURCE_DESC[sel.value] || "";
      if(bdWrap){
        if(sel.value === "b2c_pack_import"){
          bdWrap.style.display = "";
          if(bdHint) bdHint.textContent = "⚠️ 进口打包表的发货时间不可信，请手动指定这批数据所属的业务日期";
        }else{
          bdWrap.style.display = "none";
          if(bdHint) bdHint.textContent = "";
        }
      }
    });
  }
})();

// ===== Daily Features =====
async function dfRefresh(){
  var s = document.getElementById("dfStartDate").value;
  var e = document.getElementById("dfEndDate").value;
  if(!s || !e){ alert("请选择日期区间"); return; }
  if(!adminIsUnlocked_()){ alert("请先解锁管理员模式"); return; }
  var el = document.getElementById("dfStatus");

  // Step 1: precheck
  el.textContent = "正在检查数据依赖...";
  try{
    var pc = await fetchApi({ action:"admin_refresh_precheck", k:adminKey_(), start_date:s, end_date:e });
    if(!pc || !pc.ok){ el.textContent = "预检查失败: " + (pc&&pc.error?pc.error:"unknown"); return; }

    // 构建 confirm 文案
    var gaps = pc.gaps || [];
    var confirmMsg = "刷新日期范围: " + s + " ~ " + e;
    if(gaps.length === 0){
      confirmMsg += "\n\n✅ 所有日期的 6 个数据源均有数据，确认刷新？";
    }else{
      confirmMsg += "\n\n⚠️ 以下日期/数据源缺少数据：\n";
      for(var gi=0; gi<gaps.length; gi++){
        confirmMsg += "• " + gaps[gi].day + ": 缺少 " + gaps[gi].label + "\n";
      }
      confirmMsg += "\n缺少数据的任务产出将为 0。确认继续刷新？";
    }
    if(!confirm(confirmMsg)) { el.textContent = "已取消刷新"; return; }
  }catch(ex){
    // precheck 失败不阻断，降级为简单 confirm
    if(!confirm("⚠️ 数据依赖检查失败（" + ex + "）\n\n是否仍然继续刷新 " + s + " ~ " + e + "？")){
      el.textContent = "已取消刷新"; return;
    }
  }

  // Step 2: 执行刷新
  el.textContent = "正在刷新/重建...";
  try{
    var res = await fetchApi({ action:"admin_refresh_daily", k:adminKey_(), start_date:s, end_date:e });
    if(res && res.ok){
      el.textContent = "刷新完成，共 " + (res.refreshed||0) + " 条特征";
      dfRenderDashboard_(res.features || []);
      dfRenderTable_(res.features || []);

      // Step 3: 展示 post_warnings
      var pw = res.post_warnings || [];
      if(pw.length === 0){
        alert("✅ 刷新完成，" + (res.refreshed||0) + " 条特征，数据无异常");
      }else{
        var warnLines = ["⚠️ 刷新完成，但发现以下问题："];
        for(var wi=0; wi<pw.length; wi++){
          var icon = pw[wi].level === "warning" ? "⚠️" : "ℹ️";
          warnLines.push(icon + " " + pw[wi].day + " " + pw[wi].msg);
        }
        alert(warnLines.join("\n"));
      }
    }else{
      el.textContent = "刷新失败: " + (res&&res.error?res.error:"unknown");
    }
  }catch(ex){ el.textContent = "刷新异常: " + ex; }
}

async function dfQuery(){
  var s = document.getElementById("dfStartDate").value;
  var e = document.getElementById("dfEndDate").value;
  if(!s || !e){ alert("请选择日期区间"); return; }
  if(!adminIsUnlocked_()){ alert("请先解锁管理员模式"); return; }
  var el = document.getElementById("dfStatus");
  el.textContent = "查询中...";
  try{
    var res = await fetchApi({ action:"admin_daily_productivity", k:adminKey_(), start_date:s, end_date:e });
    if(res && res.ok){
      var f = res.features || [];
      el.textContent = "查询完成，共 " + f.length + " 条";
      dfRenderDashboard_(f);
      dfRenderTable_(f);
    }else{
      el.textContent = "查询失败: " + (res&&res.error?res.error:"unknown");
    }
  }catch(ex){ el.textContent = "查询异常: " + ex; }
}

function dfRenderDashboard_(features){
  var el = document.getElementById("dfDashboard");
  if(!el) return;
  if(!features || features.length === 0){ el.innerHTML = ""; return; }

  var KEY_TASKS = [
    { biz:"B2C", task:"B2C拣货" },
    { biz:"B2C", task:"B2C打包" },
    { biz:"进口", task:"过机扫描码托" },
    { biz:"B2C", task:"换单" },
    { biz:"B2C", task:"退件入库" },
    { biz:"B2C", task:"质检" }
  ];

  // 按天分组
  var dayMap = {}; // day → { "B2C拣货": feature, ... }
  var days = [];
  for(var i=0;i<features.length;i++){
    var f = features[i];
    if(!dayMap[f.day_kst]){ dayMap[f.day_kst] = {}; days.push(f.day_kst); }
    dayMap[f.day_kst][f.task] = f;
  }
  days.sort();

  var html = "";
  for(var di=0;di<days.length;di++){
    var day = days[di];
    var tasks = dayMap[day];
    html += "<div style='margin-bottom:16px;'>";
    html += "<div style='font-weight:bold;margin-bottom:2px;'>📋 " + day + " 关键任务验收看板</div>";
    html += "<div style='font-size:12px;color:#666;margin-bottom:4px;'>红色：数据异常，需先处理 | 黄色：需关注 | 分摊数据主要来自进口打包表</div>";
    html += "<table style='border-collapse:collapse;width:100%;font-size:13px;'>";
    html += "<tr style='background:#f0f0f0;'>";
    var headers = ["任务","总单量","直接单量","分摊单量","总件量","直接件量","分摊件量","作业人数","人效(单/人时)"];
    for(var hi=0;hi<headers.length;hi++) html += "<th style='border:1px solid #ccc;padding:4px 6px;text-align:center;'>" + headers[hi] + "</th>";
    html += "</tr>";

    for(var ti=0;ti<KEY_TASKS.length;ti++){
      var kt = KEY_TASKS[ti];
      var f = tasks[kt.task] || null;
      var isScan = (kt.task === "过机扫描码托" || kt.task === "换单" || kt.task === "退件入库" || kt.task === "质检");
      var missing = !f;

      // 取值
      var orderCount = f ? (f.wms_order_count||0) : 0;
      var direct = f ? (f.wms_order_count_direct||0) : 0;
      var allocated = f ? (f.wms_order_count_allocated||0) : 0;
      var qty = f ? (f.wms_qty||0) : 0;
      var qtyDirect = f ? (f.wms_qty_direct||0) : 0;
      var qtyAlloc = f ? (f.wms_qty_allocated||0) : 0;
      var workers = f ? (f.unique_workers||0) : 0;
      var eff = f ? (f.efficiency_per_person_hour||0) : 0;

      // 标色规则
      var rowStyle = "";
      if(missing || orderCount === 0){
        rowStyle = "background:#fdd;"; // 标红
      }else if(workers === 0 || eff === 0){
        rowStyle = "background:#fff3cd;"; // 标黄
      }

      // allocated 单元格标红: B2C任务 direct>0 但 allocated=0
      var allocCellStyle = "";
      if(!isScan && !missing && direct > 0 && allocated === 0){
        allocCellStyle = "background:#fdd;font-weight:bold;";
      }

      html += "<tr style='" + rowStyle + "'>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;font-weight:bold;'>" + kt.task + (missing ? "（缺失）" : "") + "</td>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;'>" + orderCount + "</td>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;'>" + direct + "</td>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;" + allocCellStyle + "'>" + (isScan ? "-" : allocated) + "</td>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;'>" + qty + "</td>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;'>" + qtyDirect + "</td>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;'>" + (isScan ? "-" : qtyAlloc) + "</td>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;" + (workers===0&&!missing ? "background:#fff3cd;" : "") + "'>" + workers + "</td>";
      html += "<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;" + (eff===0&&!missing&&orderCount>0 ? "background:#fff3cd;" : "") + "'>" + eff + "</td>";
      html += "</tr>";
    }
    html += "</table></div>";
  }
  el.innerHTML = html;
}

function dfRenderTable_(features){
  var el = document.getElementById("dfResult");
  if(!el) return;
  if(!features || features.length === 0){ el.innerHTML = "<div class='muted'>无数据</div>"; return; }
  var cols = ["day_kst","biz","task","total_person_minutes","unique_workers","session_count",
    "event_wave_count","wms_wave_count","wms_order_count","wms_order_count_direct","wms_order_count_allocated",
    "wms_qty","wms_qty_direct","wms_qty_allocated","wms_box_count","wms_pallet_count",
    "wms_weight","wms_volume","relocated_package_count","relocation_rate","relocation_type_summary",
    "final_location_type_summary","final_location_unknown_count",
    "anomaly_count","efficiency_per_person_hour","source_summary"];
  var colLabels = {
    "day_kst":"日期","biz":"业务","task":"任务","total_person_minutes":"总作业分钟",
    "unique_workers":"作业人数","session_count":"作业次数","event_wave_count":"事件波次",
    "wms_wave_count":"WMS波次","wms_order_count":"总单量","wms_order_count_direct":"直接单量",
    "wms_order_count_allocated":"分摊单量","wms_qty":"总件量","wms_qty_direct":"直接件量",
    "wms_qty_allocated":"分摊件量","wms_box_count":"箱数","wms_pallet_count":"托盘数",
    "wms_weight":"重量(kg)","wms_volume":"体积(m³)",
    "relocated_package_count":"换位包裹数","relocation_rate":"换位率","relocation_type_summary":"换位类型分布",
    "final_location_type_summary":"最终货位分布","final_location_unknown_count":"未知货位数",
    "anomaly_count":"异常次数","efficiency_per_person_hour":"人效(单/人时)",
    "source_summary":"数据来源"
  };
  var html = "<table style='width:100%;border-collapse:collapse;font-size:12px;'><thead><tr>";
  cols.forEach(function(c){ html += "<th style='border:1px solid #ddd;padding:3px 6px;background:#f5f5f5;white-space:nowrap;'>" + esc(colLabels[c]||c) + "</th>"; });
  html += "</tr></thead><tbody>";
  features.forEach(function(f){
    html += "<tr>";
    cols.forEach(function(c){
      var v = f[c];
      if(typeof v === "number") v = Math.round(v*100)/100;
      if(c === "relocation_rate" && typeof v === "number"){
        v = Math.round(v * 100) + "%";
      }
      if(c === "source_summary" && typeof v === "string" && v){
        var srcMap = {"b2c_order_export":"B2C订单表","b2c_pack_import":"进口打包表","import_express":"进口快件表","import_express:StarFans":"进口快件表（StarFans）","change_order_export":"换单表","return_inbound_export":"退件入库表","return_qc_export":"质检表"};
        v = v.split(",").map(function(s){ return srcMap[s.trim()]||s.trim(); }).join(" + ");
      }
      html += "<td style='border:1px solid #eee;padding:2px 5px;white-space:nowrap;'>" + esc(String(v!=null?v:"")) + "</td>";
    });
    html += "</tr>";
  });
  html += "</tbody></table>";
  el.innerHTML = html;
}

/** ===== B2B 扫码核对 (主系统) ===== */
var _scCurrentBatch = null; // { batch_id, batch_name, status, ... }
var _scBusy = false;        // 防连击
var _scCurrentPallet = "";  // 当前托盘号

function scBackToMenu(){ _scClearBatch(); back(); }

function initScanCheckPage(){
  _scCurrentBatch = null;
  document.getElementById("scSelectArea").style.display = "";
  document.getElementById("scWorkArea").style.display = "none";
  document.getElementById("scStartBtn").style.display = "none";
  document.getElementById("scLastResult").style.display = "none";
  // 加载 open 批次
  var sel = document.getElementById("scBatchSelect");
  sel.innerHTML = '<option value="">加载中...</option>';
  var today = kstToday_();
  jsonp(LOCK_URL, { action:"b2b_scan_batch_list", start_day:"2020-01-01", end_day:"2099-12-31" }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){ sel.innerHTML = '<option value="">加载失败</option>'; return; }
    var open = (res.batches || []).filter(function(b){ return b.status === "open"; });
    if(!open.length){ sel.innerHTML = '<option value="">暂无进行中的批次</option>'; return; }
    var html = '<option value="">请选择批次...</option>';
    open.forEach(function(b){
      html += '<option value="'+esc(b.batch_id)+'">'+esc(b.batch_id)+' · '+esc(b.batch_name)+' ('+esc(b.check_day)+')</option>';
    });
    sel.innerHTML = html;
    // 恢复 localStorage
    var saved = _scLoadBatch();
    if(saved){
      sel.value = saved;
      if(sel.value === saved) onScBatchSelected();
    }
  });
}

function _scSaveBatch(batchId){ try{ localStorage.setItem("b2b_sc_batch_v1", batchId); }catch(e){} }
function _scLoadBatch(){ try{ return localStorage.getItem("b2b_sc_batch_v1")||""; }catch(e){ return ""; } }
function _scClearBatch(){ try{ localStorage.removeItem("b2b_sc_batch_v1"); }catch(e){} }

function onScBatchSelected(){
  var v = document.getElementById("scBatchSelect").value;
  document.getElementById("scStartBtn").style.display = v ? "" : "none";
}

function scSetPallet(){
  var v = document.getElementById("scPalletInput").value.trim();
  _scCurrentPallet = v;
  try{ localStorage.setItem("b2b_sc_pallet_v1", v); }catch(e){}
  _scUpdatePalletDisplay();
  document.getElementById("scBarcodeInput").focus();
}
function _scUpdatePalletDisplay(){
  var el = document.getElementById("scPalletDisplay");
  if(_scCurrentPallet){
    el.textContent = "当前托盘：" + _scCurrentPallet;
  } else {
    el.textContent = "未设置托盘号（扫码将不记录托盘）";
  }
}

function scStartScan(){
  var batchId = document.getElementById("scBatchSelect").value;
  if(!batchId){ alert("请先选择批次"); return; }
  if(!getOperatorId()){ alert("请先在主系统设置操作员工牌后，再进入扫码核对\n\n（不需要B2B子系统登录，但需要主系统操作员工牌）"); return; }
  _scSaveBatch(batchId);
  // 恢复托盘号
  try{ _scCurrentPallet = localStorage.getItem("b2b_sc_pallet_v1") || ""; }catch(e){ _scCurrentPallet = ""; }
  document.getElementById("scPalletInput").value = _scCurrentPallet;
  _scUpdatePalletDisplay();
  // 拉取详情以获取进度和汇总
  jsonp(LOCK_URL, { action:"b2b_scan_batch_detail", batch_id:batchId }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok){ alert("加载批次失败: "+(res&&res.error||"")); return; }
    if(res.batch.status !== "open"){
      alert("此批次已关闭，无法扫码");
      return;
    }
    _scCurrentBatch = res.batch;
    document.getElementById("scSelectArea").style.display = "none";
    document.getElementById("scWorkArea").style.display = "";
    document.getElementById("scBatchInfo").textContent = res.batch.batch_id + " · " + res.batch.batch_name;
    // 更新进度
    _scUpdateProgress(res.done_boxes, res.total_expected_boxes, res.progress_percent);
    // 更新汇总
    _scUpdateSummary(res.items, res.unplanned);
    // 聚焦输入框
    document.getElementById("scBarcodeInput").value = "";
    document.getElementById("scLastResult").style.display = "none";
    setTimeout(function(){ document.getElementById("scBarcodeInput").focus(); }, 100);
  });
}

function _scUpdateProgress(doneBoxes, totalBoxes, pct){
  document.getElementById("scProgressText").textContent = doneBoxes + " / " + totalBoxes + " 箱 (" + pct + "%)";
  document.getElementById("scProgressFill").style.width = pct + "%";
}

function _scUpdateSummary(items, unplanned){
  // done = scanned >= expected（含多扫）, over = scanned > expected, missing = scanned < expected
  var done=0, missing=0, over=0;
  (items||[]).forEach(function(it){
    if(it.scanned_count >= it.expected_box_count) done++;
    if(it.scanned_count > it.expected_box_count) over++;
    if(it.scanned_count < it.expected_box_count) missing++;
  });
  var unplannedCount = (unplanned||[]).length;
  var html = '<span style="color:#388e3c;font-weight:700;">已完成: '+done+'种</span>';
  html += ' &nbsp; <span style="color:#d32f2f;font-weight:700;">未完成: '+missing+'种</span>';
  html += ' &nbsp; <span style="color:#e65100;font-weight:700;">多扫: '+over+'种</span>';
  html += ' &nbsp; <span style="color:#616161;font-weight:700;">计划外: '+unplannedCount+'种</span>';
  document.getElementById("scSummaryContent").innerHTML = html;
}

function scDoScan(){
  if(_scBusy) return;
  var input = document.getElementById("scBarcodeInput");
  var bc = input.value.trim();
  if(!bc){ return; }
  if(!_scCurrentBatch){ alert("请先选择批次"); return; }

  // 如果输入框有新值但未点设定，自动同步
  var palletInput = document.getElementById("scPalletInput").value.trim();
  if(palletInput !== _scCurrentPallet){
    _scCurrentPallet = palletInput;
    try{ localStorage.setItem("b2b_sc_pallet_v1", palletInput); }catch(e){}
    _scUpdatePalletDisplay();
  }

  _scBusy = true;
  var opId = getOperatorId() || "";
  jsonp(LOCK_URL, {
    action:"b2b_scan_do",
    batch_id: _scCurrentBatch.batch_id,
    outbound_barcode: bc,
    scanned_by: opId,
    pallet_no: _scCurrentPallet
  }, { skipBusy:true }).then(function(res){
    _scBusy = false;
    if(!res || !res.ok){
      var err = (res && res.error) || "unknown";
      if(err.indexOf("not open") >= 0){
        _scShowResult("❌ 批次已关闭，无法继续扫码", "#d32f2f", "#ffebee", bc, "");
        return;
      }
      _scShowResult("❌ 扫码失败: " + err, "#d32f2f", "#ffebee", bc, "");
      return;
    }
    // 清空+聚焦
    input.value = "";
    input.focus();

    if(res.planned){
      // 更新进度
      _scUpdateProgress(res.done_boxes, res.total_expected_boxes, res.progress_percent);
      var diff = res.diff;
      if(diff < 0){
        // 还差
        _scShowResult("✅ " + bc + "  " + res.scanned_count + "/" + res.expected + "  还差" + Math.abs(diff) + "箱",
          "#1b5e20", "#e8f5e9", bc, "ok");
      } else if(diff === 0){
        // 刚好完成
        _scShowResult("🎉 " + bc + "  " + res.scanned_count + "/" + res.expected + "  已完成！",
          "#e65100", "#fff3e0", bc, "done");
      } else {
        // 多扫
        _scShowResult("⚠ " + bc + "  " + res.scanned_count + "/" + res.expected + "  多扫" + diff + "箱",
          "#d32f2f", "#ffebee", bc, "over");
      }
    } else {
      // 计划外
      _scShowResult("❌ " + bc + "  计划外条码", "#616161", "#f5f5f5", bc, "unplanned");
    }
    // 延迟刷新汇总（不阻塞扫码）
    _scRefreshSummary();
  });
}

function _scShowResult(text, color, bgColor, barcode, type){
  var el = document.getElementById("scLastResult");
  el.style.display = "";
  el.style.background = bgColor;
  el.style.color = color;
  el.style.border = "2px solid " + color;
  el.style.fontSize = "24px";
  el.style.fontWeight = "800";
  el.style.lineHeight = "1.4";
  el.innerHTML = text;
}

function _scRefreshSummary(){
  if(!_scCurrentBatch) return;
  jsonp(LOCK_URL, { action:"b2b_scan_batch_detail", batch_id:_scCurrentBatch.batch_id }, { skipBusy:true }).then(function(res){
    if(!res || !res.ok) return;
    if(res.batch.status !== "open"){
      _scShowResult("❌ 批次已被关闭", "#d32f2f", "#ffebee", "", "");
      document.getElementById("scBarcodeInput").disabled = true;
    }
    _scUpdateSummary(res.items, res.unplanned);
  });
}

function scUndoLast(){
  if(_scBusy) return;
  if(!_scCurrentBatch){ alert("请先选择批次"); return; }
  if(!confirm("确认撤销上一扫？")) return;

  _scBusy = true;
  jsonp(LOCK_URL, {
    action:"b2b_scan_undo",
    batch_id: _scCurrentBatch.batch_id,
    operator_id: getOperatorId() || ""
  }, { skipBusy:true }).then(function(res){
    _scBusy = false;
    if(!res || !res.ok){
      var err = (res && res.error) || "unknown";
      if(err.indexOf("nothing to undo") >= 0){
        alert("没有可撤销的扫码记录");
      } else if(err.indexOf("not open") >= 0){
        alert("批次已关闭，无法撤销");
      } else {
        alert("撤销失败: " + err);
      }
      return;
    }
    if(res.was_planned){
      _scUpdateProgress(res.done_boxes, res.total_expected_boxes, res.progress_percent);
      _scShowResult("↩ 已撤销: " + res.undone_barcode + "  当前 " + res.new_scanned_count + " 箱",
        "#e65100", "#fff3e0", res.undone_barcode, "undo");
    } else {
      _scShowResult("↩ 已撤销计划外条码: " + res.undone_barcode,
        "#616161", "#f5f5f5", res.undone_barcode, "undo");
    }
    _scRefreshSummary();
    document.getElementById("scBarcodeInput").focus();
  });
}

function scExitScan(){
  _scCurrentBatch = null;
  _scClearBatch();
  document.getElementById("scSelectArea").style.display = "";
  document.getElementById("scWorkArea").style.display = "none";
  document.getElementById("scBarcodeInput").disabled = false;
  document.getElementById("scLastResult").style.display = "none";
  // 回到选择区，不离开页面
  initScanCheckPage();
}

/** ===== init ===== */
refreshNet();
applyPageSession_();
refreshUI();
if(!getOperatorId()) showOperatorSetup(false);
else fetchOperatorOpenSessions();
restoreState();
renderActiveLists();
bindAdminEasterEgg_();
adminApplyUI_();
setReportDefaultDates_();
renderPages();
flushQueue_();
cleanupOldLocalStorage_();

