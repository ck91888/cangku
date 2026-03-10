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
  "b2b_menu","b2b_unload","b2b_tally","b2b_workorder","b2b_outbound","b2b_inventory",
  "b2c_tally","b2c_pick","b2c_pack","b2c_bulkout","b2c_return","b2c_qc","b2c_inventory","b2c_disposal","b2c_relabel",
  "warehouse_cleanup",
  "active_now",
  "report",
  "global_sessions",
  "correction"
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
  if((p==="report" || p==="global_sessions" || p==="correction") && !adminIsUnlocked_()){
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
  if(cur==="import_loadout"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="import_pickup"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="import_problem"){ restoreState(); renderActiveLists(); refreshUI(); }

  if(cur==="b2b_menu"){ refreshUI(); }
  if(cur==="b2b_unload"){ restoreState(); renderActiveLists(); refreshUI(); updateReturnButton_(); }
  if(cur==="b2b_tally"){ restoreState(); renderActiveLists(); renderB2bTallyUI(); refreshUI(); }
  if(cur==="b2b_workorder"){ restoreState(); renderActiveLists(); renderB2bWorkorderUI(); refreshUI(); }
  if(cur==="b2b_outbound"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2b_inventory"){ restoreState(); renderActiveLists(); refreshUI(); }
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
    "b2b_unload","b2b_tally","b2b_workorder","b2b_outbound",
    "b2b_inventory","b2c_inventory","warehouse_cleanup"];
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

var lastScanAt = 0;
var scanBusy = false;
var globalBusy = false; // 防止快速连点导致并发请求

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
    var serverLocks = res.active || [];
    // 按 task 分组
    var byTask = {};
    serverLocks.forEach(function(lk){
      var t = lk.task || "";
      if(!byTask[t]) byTask[t] = [];
      byTask[t].push(lk.badge);
    });
    var changed = false;
    TASK_REGISTRY.forEach(function(reg){
      var serverBadges = byTask[reg.task] || [];
      var serverSet = new Set(serverBadges);
      var localSet = reg.get();
      // 添加服务器有但本地没有的
      serverBadges.forEach(function(b){
        if(!localSet.has(b)){ localSet.add(b); changed = true; }
      });
      // 移除本地有但服务器已不存在的（已在其他设备leave）
      Array.from(localSet).forEach(function(b){
        if(!serverSet.has(b)){ localSet.delete(b); changed = true; }
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
}

async function endSessionGlobal_(){
  if(!currentSessionId){ setStatus("没有未结束趟次", false); return; }

  // 最多重试3次（lock释放可能有延迟）
  var r;
  for(var _retry = 0; _retry < 3; _retry++){
    r = await sessionCloseServer_();
    if(!r.blocked) break;
    if(_retry < 2) await new Promise(function(res){ setTimeout(res, 800); });
  }
  if(r.blocked){
    var msg = "还有人员未退出，不能结束。\n\n" + formatActiveListForAlert_(r.active);
    setStatus("还有人员未退出，禁止结束", false);
    alert(msg);
    return;
  }
  if(r.already_closed){
    alert("该趟次已结束（无需重复结束）");
    setStatus("该趟次已结束（无需重复结束）", true);
    cleanupLocalSession_();
    return;
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
  if(h>0) return h + "h" + String(m).padStart(2,"0") + "m";
  return m + "m";
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
    if(meta) meta.textContent = "在岗: " + _activeNowData.length + " 人 ｜ " + new Date(_activeNowAsof).toLocaleTimeString();

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
  if(titleEl) titleEl.textContent = "全局在岗 / Active Now";
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

  var keys = Object.keys(by).sort();
  if(keys.length === 0){
    indexEl.innerHTML = '<div class="muted" style="padding:10px 0;">当前无人在岗 / 현재 없음</div>';
    return;
  }

  indexEl.innerHTML = '<div class="grid2">' +
    keys.map(function(k){
      var t = by[k];
      var label = taskDisplayLabel(t.biz, t.task);
      return '<button style="text-align:left;line-height:1.3;" ' +
        'data-biz="'+esc(t.biz)+'" data-task="'+esc(t.task)+'" onclick="activeNowShowDetail(this.dataset.biz,this.dataset.task)">' +
        '<div style="font-size:13px;">' + esc(label) + '</div>' +
        '<div style="font-size:22px;font-weight:800;margin-top:4px;">' + t.count + ' <small style="font-size:13px;">人</small></div>' +
        '</button>';
    }).join("") +
  '</div>';
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

  var label = taskDisplayLabel(biz, task);
  var workers = _activeNowData.filter(function(x){
    return (x.biz||"") === biz && (x.task||"") === task;
  });

  if(titleEl) titleEl.textContent = label + " · " + workers.length + "人";

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
          '<div class="muted" style="margin-top:4px;">在岗: '+esc(dur)+'</div>' +
          '<div class="muted" style="font-size:12px;margin-top:2px;">session: '+esc(x.session||"")+'</div>' +
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
  "B2B": ["B2B卸货","B2B入库理货","B2B工单操作","B2B出库","B2B盘点"],
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
    return (
      '<div style="border:1px solid #eee;border-radius:12px;padding:10px;margin:8px 0;">' +
        '<div style="font-weight:700;font-size:13px;">'+esc(s.session)+'</div>' +
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
    try{ await sessionCloseServer_(); }catch(e2){ /* 关闭失败不阻断新建 */ }
    cleanupLocalSession_();
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
function startB2bWorkorder(e){ startGeneric_(e, "B2B", "B2B工单操作", "b2b_workorder", function(){ scannedB2bWorkorders = new Set(); activeB2bWorkorder = new Set(); }, renderB2bWorkorderUI); }
async function endB2bWorkorder(){ if(!acquireBusy_()) return; try{ await endSessionGlobal_(); }finally{ releaseBusy_(); } }

/** ===== 通用临时去卸货 / 返回原任务 ===== */

// 根据当前 biz 决定目标卸货任务
function unloadTarget_(biz){
  if(biz === "B2B") return { biz:"B2B", task:"B2B卸货", page:"b2b_unload" };
  if(biz === "进口") return { biz:"进口", task:"卸货", page:"import_unload" };
  return null;
}

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
    // 兼容旧格式
    if(!ctx.sourceBiz && ctx.workorderSession){
      ctx.sourceBiz = "B2B"; ctx.sourceTask = "B2B工单操作"; ctx.sourcePage = "b2b_workorder";
      ctx.sourceSession = ctx.workorderSession;
      ctx.scannedItems = ctx.workorders || [];
      ctx.unloadBiz = "B2B"; ctx.unloadTask = "B2B卸货"; ctx.unloadPage = "b2b_unload";
    }
    if(!ctx.badges && ctx.badge) ctx.badges = [ctx.badge];
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
  try{ await tempSwitchToUnload_(); }finally{ releaseBusy_(); }
}
async function tempSwitchToUnload_(){
  // 识别当前任务
  var srcBiz = CUR_CTX && CUR_CTX.biz;
  var srcTask = CUR_CTX && CUR_CTX.task;
  var srcPage = CUR_CTX && CUR_CTX.page;
  if(!srcBiz || !srcTask){ alert("当前没有进行中的任务，无法切换。"); return; }

  // 不能从卸货切到卸货
  if(srcTask === "B2B卸货" || srcTask === "卸货"){ alert("当前已经在卸货任务中。"); return; }

  // 确定目标卸货
  var target = unloadTarget_(srcBiz);
  if(!target){
    // B2C/仓库等，让用户选择
    var c = prompt("当前环节无对应卸货，请选择：\n1. B2B卸货\n2. 进口快件卸货\n\n输入 1 或 2：");
    if(!c) return;
    c = c.trim();
    if(c === "1") target = { biz:"B2B", task:"B2B卸货", page:"b2b_unload" };
    else if(c === "2") target = { biz:"进口", task:"卸货", page:"import_unload" };
    else { alert("无效选择"); return; }
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
    if(!confirm("确定要临时去卸货吗？\n\n工牌：" + badgeDisplay(members[0]) + "\n\n将自动退出当前任务 → 跳转到卸货页面")) return;
    badges = [members[0]];
  } else {
    var list = members.map(function(m, i){ return (i+1) + ". " + badgeDisplay(m); }).join("\n");
    var choice = prompt("请选择要临时去卸货的人员（多选用逗号分隔，A=全部）：\n\n" + list);
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
    if(!confirm("确定要以下 " + badges.length + " 人临时去卸货吗？\n\n" + names + "\n\n将自动退出当前任务 → 跳转到卸货页面")) return;
  }

  var srcLabel = taskDisplayLabel(srcBiz, srcTask);
  setStatus("临时切换中... 正在退出" + srcLabel + "（" + badges.length + "人）⏳", true);

  // 1. 逐个 leave 当前任务（释放锁）
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
      alert("退出失败（" + badgeDisplay(badges[j]) + "）：" + e + "\n\n已成功退出 " + leftBadges.length + " 人，将继续切换。");
    }
  }
  persistState();

  if(leftBadges.length === 0){
    setStatus("全部退出失败 ❌", false);
    return;
  }

  // 2. 自动 start 卸货 session + 自动 join 所有人
  setStatus("正在加入卸货... ⏳", true);
  var unloadSid = getSess_(target.biz, target.task);
  // 检查已有卸货session是否还开着
  if(unloadSid){
    try{
      var sInfo = await jsonp(LOCK_URL, { action:"session_info", session: unloadSid }, { skipBusy: true });
      if(sInfo && String(sInfo.status||"").toUpperCase() === "CLOSED"){
        clearSess_(target.biz, target.task);
        unloadSid = null;
      }
    }catch(e){ /* 查询失败继续使用 */ }
  }
  if(!unloadSid){
    unloadSid = makePickSessionId();
    var evStart = makeEventId({ event:"start", biz:target.biz, task:target.task, wave_id:"", badgeRaw:"" });
    try{
      await submitEventSync_({ event:"start", event_id: evStart, biz:target.biz, task:target.task, pick_session_id: unloadSid }, true);
      addRecent(evStart);
      var unloadReg = taskReg_(target.task);
      if(unloadReg) unloadReg.set(new Set());
    }catch(e){
      setStatus("创建卸货趟次失败 ❌ " + e, false);
      alert("创建卸货趟次失败：" + e + "\n\n已退出原任务，请手动在卸货页加入。");
      saveTempSwitchCtx_({
        badges: leftBadges, sourceBiz: srcBiz, sourceTask: srcTask, sourcePage: srcPage,
        sourceSession: srcSid, scannedItems: getScannedItems_(srcTask),
        unloadBiz: target.biz, unloadTask: target.task, unloadPage: target.page,
        timestamp: Date.now()
      });
      go(target.page); refreshUI(); updateReturnButton_();
      return;
    }
  }
  currentSessionId = unloadSid;
  CUR_CTX = { biz: target.biz, task: target.task, page: target.page };
  setSess_(target.biz, target.task, unloadSid);

  var joinedUnload = [];
  var joinFailed = [];
  for(var u = 0; u < leftBadges.length; u++){
    try{
      var evJoin = makeEventId({ event:"join", biz:target.biz, task:target.task, wave_id:"", badgeRaw: leftBadges[u] });
      await submitEventSyncWithRetry_({ event:"join", event_id: evJoin, biz:target.biz, task:target.task, pick_session_id: unloadSid, da_id: leftBadges[u] });
      addRecent(evJoin);
      applyActive(target.task, "join", leftBadges[u]);
      joinedUnload.push(leftBadges[u]);
    }catch(e){
      joinFailed.push(badgeDisplay(leftBadges[u]));
    }
  }
  if(joinFailed.length > 0){
    alert("以下人员加入卸货失败，请手动加入：\n" + joinFailed.join("\n"));
  }
  persistState();

  // 3. 保存上下文到 localStorage
  saveTempSwitchCtx_({
    badges: leftBadges,
    sourceBiz: srcBiz, sourceTask: srcTask, sourcePage: srcPage,
    sourceSession: srcSid, scannedItems: getScannedItems_(srcTask),
    unloadBiz: target.biz, unloadTask: target.task, unloadPage: target.page,
    timestamp: Date.now()
  });

  // 4. 导航到卸货页
  setStatus("已切换到卸货 ✅（" + joinedUnload.length + "/" + leftBadges.length + "人已加入）", false);
  go(target.page);
  refreshUI();
  renderActiveLists();
  updateReturnButton_();
}

async function returnFromTempUnload(){
  if(!acquireBusy_()) return;
  try{ await returnFromTempUnload_(); }finally{ releaseBusy_(); }
}
async function returnFromTempUnload_(){
  var ctx = loadTempSwitchCtx_();
  if(!ctx){ alert("没有找到切换记录"); return; }

  var allBadges = ctx.badges || [];
  var srcSid = ctx.sourceSession;
  var srcBiz = ctx.sourceBiz;
  var srcTask = ctx.sourceTask;
  var srcPage = ctx.sourcePage;
  var ulBiz = ctx.unloadBiz || "B2B";
  var ulTask = ctx.unloadTask || "B2B卸货";
  var srcLabel = taskDisplayLabel(srcBiz, srcTask);

  // 如果上下文里badges为空（B设备远程检测），用当前卸货在岗名单
  if(allBadges.length === 0){
    allBadges = Array.from(getUnloadActiveSet_(ulTask));
  }
  if(allBadges.length === 0){ alert("没有找到工牌信息，请先在卸货页加入作业。"); return; }

  // 选择要返回的人
  var returning;
  if(allBadges.length === 1){
    if(!confirm("确定返回" + srcLabel + "吗？\n\n工牌：" + badgeDisplay(allBadges[0]) + "\n\n将自动退出卸货 → 重新加入原任务")) return;
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

  // 1. 逐个 leave 卸货（如果还在岗，释放锁）
  var unloadSid = getSess_(ulBiz, ulTask);
  if(unloadSid){
    var toLeave = returning.filter(function(b){ return isAlreadyActive(ulTask, b); });
    if(toLeave.length > 0){
      setStatus("正在退出卸货（" + toLeave.length + "人）... ⏳", true);
      currentSessionId = unloadSid;
      CUR_CTX = { biz: ulBiz, task: ulTask, page: ctx.unloadPage || "b2b_unload" };
      for(var i = 0; i < toLeave.length; i++){
        try{
          var evLeave = makeEventId({ event:"leave", biz:ulBiz, task:ulTask, wave_id:"", badgeRaw: toLeave[i] });
          await submitEventSyncWithRetry_({ event:"leave", event_id: evLeave, biz:ulBiz, task:ulTask, pick_session_id: unloadSid, da_id: toLeave[i] });
          addRecent(evLeave);
          applyActive(ulTask, "leave", toLeave[i]);
        }catch(e){
          alert("退出卸货失败（" + badgeDisplay(toLeave[i]) + "）：" + e + "\n将继续处理其余人员。");
        }
      }
      persistState();
      // 触发自动结束（如果最后一人离开），保护源任务数据
      var savedSrcReg = taskReg_(srcTask);
      var savedSrcActive = savedSrcReg ? new Set(savedSrcReg.get()) : null;
      var savedSrcSess = getSess_(srcBiz, srcTask);
      laborTask = ulTask; laborBiz = ulBiz;
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
      alert("原任务趟次已被关闭（可能已被其他人结束），需要重新开始。");
      clearTempSwitchCtx_();
      go(srcPage);
      updateReturnButton_();
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

  // 4. 更新 localStorage：移除已返回的人，保留仍在卸货的人
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
    setStatus("已返回 " + joinedCount + " 人，还有 " + remaining.length + " 人在卸货 ✅", false);
  } else {
    go(srcPage);
    refreshUI();
    renderActiveLists();
    // 刷新对应任务的扫码UI
    if(srcTask === "B2B工单操作") renderB2bWorkorderUI();
    if(srcTask === "B2B入库理货") renderB2bTallyUI();
    if(srcTask === "理货") renderInboundCountUI();
    if(srcTask === "批量出库") renderBulkOutUI();
    updateReturnButton_();
    setStatus("全部已返回" + srcLabel + " ✅（" + joinedCount + "/" + returning.length + "人）", false);
  }
}

function tempUnloadBack_(){
  var ctx = loadTempSwitchCtx_();
  if(ctx){
    var go_ = confirm("您正在临时卸货中，直接返回会导致卸货锁未释放。\n\n• 点【确定】→ 先自动返回原任务再离开\n• 点【取消】→ 留在卸货页");
    if(go_) returnFromTempUnload();
    return;
  }
  back();
}
// 兼容旧函数名
function b2bUnloadBack(){ tempUnloadBack_(); }

function updateReturnButton_(){
  // 两个卸货页都可能有返回按钮
  var btns = [
    document.getElementById("btnReturnFromUnload_b2b"),
    document.getElementById("btnReturnFromUnload_import")
  ];
  var ctx = loadTempSwitchCtx_();
  var show = !!ctx;

  // 如果本机没有切换记录，查服务端检测
  if(!show){
    btns.forEach(function(b){ if(b) b.style.display = "none"; });
    detectRemoteTempSwitch_();
    return;
  }
  // 只显示当前卸货页对应的按钮
  var curPage = getHashPage();
  btns.forEach(function(b){
    if(!b) return;
    if(b.id === "btnReturnFromUnload_b2b")
      b.style.display = (curPage === "b2b_unload" && show) ? "block" : "none";
    if(b.id === "btnReturnFromUnload_import")
      b.style.display = (curPage === "import_unload" && show) ? "block" : "none";
  });
}

function detectRemoteTempSwitch_(){
  var op = getOperatorId();
  if(!op) return;
  jsonp(LOCK_URL, { action:"operator_open_sessions", operator_id: op }, { skipBusy: true }).then(function(res){
    if(!res || !res.sessions) return;
    // 检测是否有 非卸货session + 卸货session 同时在开
    var srcSession = null;
    var inB2bUnload = false, inImportUnload = false;
    for(var i = 0; i < res.sessions.length; i++){
      var s = res.sessions[i];
      if(s.biz === "B2B" && s.task === "B2B卸货"){ inB2bUnload = true; continue; }
      if(s.biz === "进口" && s.task === "卸货"){ inImportUnload = true; continue; }
      if(!srcSession) srcSession = s; // 第一个非卸货 session 当作源
    }
    if(!srcSession) return;
    var inUnload = inB2bUnload || inImportUnload;
    if(!inUnload) return;

    var ulBiz = inB2bUnload ? "B2B" : "进口";
    var ulTask = inB2bUnload ? "B2B卸货" : "卸货";
    var ulPage = inB2bUnload ? "b2b_unload" : "import_unload";
    var srcPage = pageForTask(srcSession.biz, srcSession.task) || "home";
    var activeBadges = Array.from(getUnloadActiveSet_(ulTask));

    saveTempSwitchCtx_({
      badges: activeBadges,
      sourceBiz: srcSession.biz, sourceTask: srcSession.task, sourcePage: srcPage,
      sourceSession: srcSession.session, scannedItems: [],
      unloadBiz: ulBiz, unloadTask: ulTask, unloadPage: ulPage,
      timestamp: Date.now(), fromRemote: true
    });
    setSess_(srcSession.biz, srcSession.task, srcSession.session);
    // 显示对应按钮
    var curPage = getHashPage();
    var btn;
    if(curPage === "b2b_unload") btn = document.getElementById("btnReturnFromUnload_b2b");
    if(curPage === "import_unload") btn = document.getElementById("btnReturnFromUnload_import");
    if(btn) btn.style.display = "block";
  }).catch(function(){});
}

async function openScannerB2bWorkorder(){
  if(!currentSessionId){ setStatus("请先开始操作", false); return; }
  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再扫码，请重新开始。"))) return;
  scanMode = "b2b_workorder";
  document.getElementById("scanTitle").textContent = "扫码工单号（B2B）";
  await openScannerCommon();
}

function manualAddB2bWorkorder(){
  var inp = document.getElementById("b2bWorkorderManualInput");
  if(!inp) return;
  var val = String(inp.value || "").trim();
  if(!val){ alert("请输入工单号"); return; }
  if(!currentSessionId){ alert("请先点【开始操作】"); return; }
  if(scannedB2bWorkorders.has(val)){
    alert("已记录（去重）✅\n" + val);
    renderB2bWorkorderUI(); inp.value = ""; return;
  }
  scannedB2bWorkorders.add(val);
  persistState(); renderB2bWorkorderUI();
  var evId = makeEventId({ event:"wave", biz:"B2B", task:"B2B工单操作", wave_id: val, badgeRaw:"" });
  if(!hasRecent(evId)){
    submitEvent({ event:"wave", event_id: evId, biz:"B2B", task:"B2B工单操作", pick_session_id: currentSessionId, wave_id: val });
    addRecent(evId);
  }
  setStatus("已记录工单（待上传）✅ " + val, true);
  inp.value = "";
}

function renderB2bWorkorderUI(){
  var c = document.getElementById("b2bWorkorderOrderCount");
  var l = document.getElementById("b2bWorkorderOrderList");
  if(c) c.textContent = String(scannedB2bWorkorders.size);
  if(l){
    if(scannedB2bWorkorders.size === 0){
      l.innerHTML = '<span class="muted">无 / 없음</span>';
    }else{
      var arr = Array.from(scannedB2bWorkorders);
      var show = arr.slice(Math.max(0, arr.length - 30));
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+esc(String(x))+'</span>'; }).join(" ");
    }
  }
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

  // 自动 session 的任务：第一次 join 时自动开新趟次，并同步发送 start
  if(!sid && taskAutoSession_(task)){
    var newSid = makePickSessionId();

    // ✅ 先调服务器确认，避免有未关闭趟次时写入错误的本地 session
    var evIdStart = makeEventId({ event:"start", biz:biz, task: task, wave_id:"", badgeRaw:"" });
    if(!hasRecent(evIdStart)){
      try{
        await submitEventSync_({ event:"start", event_id: evIdStart, biz: biz, task: task, pick_session_id: newSid }, true);
        addRecent(evIdStart);
      }catch(e){
        setStatus("加入失败 ❌ " + e, false);
        alert(String(e));
        return;
      }
    }

    // ✅ 服务器确认后才写入本地状态
    sid = newSid;
    currentSessionId = sid;
    CUR_CTX = { biz: biz, task: task, page: getHashPage() };
    setSess_(biz, task, sid);

    // 清空该任务的本地状态
    if(task==="打包") activePack = new Set();
    if(task==="退件入库") activeReturn = new Set();
    if(task==="质检") activeQc = new Set();
    if(task==="废弃处理") activeDisposal = new Set();
    if(task==="卸货") activeImportUnload = new Set();
    if(task==="过机扫描码托") activeImportScanPallet = new Set();
    if(task==="装柜/出货") activeImportLoadout = new Set();
    if(task==="B2B卸货") activeB2bUnload = new Set();
    if(task==="B2B出库") activeB2bOutbound = new Set();
    if(task==="B2B盘点") activeB2bInventory = new Set();
    if(task==="B2C盘点") activeB2cInventory = new Set();
    if(task==="仓库整理") activeWarehouseCleanup = new Set();
    if(task==="取/送货") { activeImportPickup = new Set(); importPickupNotes = {}; }
    if(task==="问题处理") { activeImportProblem = new Set(); importProblemNotes = {}; }
    persistState(); refreshUI();
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
    var endIt = confirm("当前没有人在" + task + "作业中，无需退出。\n\n如果您想结束本趟次，请点【确定】；\n返回继续作业请点【取消】。");
    if(endIt) await endSessionGlobal_();
    return;
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
    if(now - lastScanAt < 900) return;
    lastScanAt = now;

    // ✅ 乱码检测：对非 QR 码场景（工单/单号扫码）自动拦截可疑结果
    if(scanMode !== "operator_setup" && scanMode !== "session_join" && scanMode !== "labor" && scanMode !== "badgeBind" && scanMode !== "leaderLoginPick"){
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
      var codeW = decodedText.trim();
      if(!codeW){ setStatus("工单号为空", false); return; }
      scanBusy = true;
      await pauseScanner();
      try{
        if(scannedB2bWorkorders.has(codeW)){
          setStatus("已记录（去重）✅ " + codeW, true);
          alert("已记录（去重）✅\n" + codeW);
          renderB2bWorkorderUI(); await closeScanner(); return;
        }
        scannedB2bWorkorders.add(codeW); persistState(); renderB2bWorkorderUI();
        var evIdW = makeEventId({ event:"wave", biz:"B2B", task:"B2B工单操作", wave_id: codeW, badgeRaw:"" });
        if(!hasRecent(evIdW)){
          submitEvent({ event:"wave", event_id: evIdW, biz:"B2B", task:"B2B工单操作", pick_session_id: currentSessionId, wave_id: codeW });
          addRecent(evIdW);
        }
        setStatus("已记录工单（待上传）✅ " + codeW, true);
        alert("已记录工单 ✅\n" + codeW + "\n当前累计：" + scannedB2bWorkorders.size);
        await closeScanner();
      } finally { scanBusy = false; }
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

      setStatus("处理中... 请稍等 ⏳（join/leave 需确认锁）", true);

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

        addRecent(evId);

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
        setStatus("提交失败 ❌ " + e, false);
        alert("提交失败，请重试。\n" + e);
      } finally { scanBusy = false; }
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

  // ✅ 自适应扫描区域：宽度占视频85%（条形码需要横向全入），高度30%
  var qrboxFn = function(viewfinderWidth, viewfinderHeight){
    var w = Math.floor(viewfinderWidth * 0.85);
    var h = Math.floor(viewfinderHeight * 0.30);
    // 最低保底尺寸
    if(w < 250) w = Math.min(250, viewfinderWidth - 10);
    if(h < 80) h = Math.min(80, viewfinderHeight - 10);
    return { width: w, height: h };
  };

  // ✅ Html5Qrcode.start() 第一个参数只接受 { facingMode } 或 cameraId 字符串
  // width/height/zoom/focusMode 等不能放这里，否则 iOS Safari 直接拒绝
  try{
    await scanner.start({ facingMode: "environment" }, { fps: 10, qrbox: qrboxFn }, onScan);
  }catch(e){
    // facingMode 失败（部分安卓），fallback 用摄像头 ID
    try{
      var cams = await Html5Qrcode.getCameras();
      // 优先选广角主摄，排除长焦/微距
      var camId = null;
      if(cams && cams.length > 1){
        var dominated = /tele|长焦|macro|微距/i;
        var preferred = /wide|广角|main|主|rear\s*0|back\s*0|camera\s*0|facing back/i;
        var candidates = cams.filter(function(c){ return !dominated.test(c.label); });
        if(candidates.length === 0) candidates = cams;
        for(var ci=0;ci<candidates.length;ci++){
          if(preferred.test(candidates[ci].label)){ camId = candidates[ci].id; break; }
        }
        if(!camId) camId = candidates[0].id;
      } else if(cams && cams[0]){
        camId = cams[0].id;
      }
      await scanner.start(camId, { fps: 10, qrbox: qrboxFn }, onScan);
    }catch(e2){
      // 最终 fallback：取第一个可用摄像头
      var cams2 = await Html5Qrcode.getCameras();
      var camId2 = cams2 && cams2[0] ? cams2[0].id : null;
      await scanner.start(camId2, { fps: 10, qrbox: qrboxFn }, onScan);
    }
  }

  // ✅ 启动成功后：尝试设置 zoom=最小值 + 连续对焦（OPPO 防长焦锁定）
  // 用 try/catch 包裹，iOS 不支持会静默跳过
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
  }catch(e){ /* iOS Safari 等不支持 getCapabilities/applyConstraints，静默跳过 */ }
}

async function closeScanner(){
  try{
    if(scanner){ await scanner.stop(); await scanner.clear(); scanner = null; }
  }catch(e){}
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

  setStatus("拉取区间数据中... ⏳", true);

  fetchApi({
    action: "admin_events_tail",
    k: k,
    limit: 20000,
    since_ms: String(startMs),
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
        var durRejoin = Math.max(0, t - active[badge].t);
        addDur(badge, active[badge].biz, active[badge].task, durRejoin);
        addTimeline(badge, active[badge].biz, active[badge].task, active[badge].t, t, "AUTO_CLOSE_REJOIN", active[badge].session, active[badge].note);
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
        var durLeave = Math.max(0, t - active[badge].t);
        addDur(badge, active[badge].biz, active[badge].task, durLeave);
        addTimeline(badge, active[badge].biz, active[badge].task, active[badge].t, t, "NORMAL", active[badge].session, active[badge].note);
        delete active[badge];
      }else{
        anomalies.leave_without_join++;
        addAnomaly("leave_without_join", badge, biz, task, t, "leave 无对应 join");
      }
    }
  }

  // 还在岗的按 now 结算
  Object.keys(active).forEach(function(b){
    anomalies.open++;
    var durOpen = Math.max(0, now - active[b].t);
    addDur(b, active[b].biz, active[b].task, durOpen);
    addTimeline(b, active[b].biz, active[b].task, active[b].t, now, "OPEN", active[b].session, active[b].note);
    addAnomaly("open_not_left", b, active[b].biz, active[b].task, now, "统计截止时仍在岗");
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
  "B2B": ["B2B卸货","B2B入库理货","B2B工单操作","B2B出库","B2B盘点"],
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
    // 插入 join
    var r1 = await fetchApi({
      action: "admin_event_insert",
      k: adminKey_(),
      badge: badge, biz: biz, task: task, session: session,
      event: "join", custom_ms: joinMs,
      operator_id: getOperatorId() || "",
      note: note
    });
    if(!r1 || r1.ok !== true){
      resultEl.textContent = "join 插入失败: " + (r1 && r1.error ? r1.error : "unknown");
      return;
    }

    // 插入 leave
    var r2 = await fetchApi({
      action: "admin_event_insert",
      k: adminKey_(),
      badge: badge, biz: biz, task: task, session: session,
      event: "leave", custom_ms: leaveMs,
      operator_id: getOperatorId() || "",
      note: note
    });
    if(!r2 || r2.ok !== true){
      resultEl.textContent = "leave 插入失败: " + (r2 && r2.error ? r2.error : "unknown");
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

