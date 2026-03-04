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
  "import_menu","import_unload","import_scan_pallet","import_loadout",
  "b2b_menu","b2b_unload","b2b_tally","b2b_workorder","b2b_outbound","b2b_inventory",
  "b2c_tally","b2c_pick","b2c_pack","b2c_bulkout","b2c_return","b2c_qc","b2c_inventory","b2c_disposal","b2c_relabel",
  "warehouse_cleanup",
  "active_now",
  "report",
  "global_sessions"
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
    var res = await jsonp(LOCK_URL, { action:"admin_events_tail", k:key, limit: 1 });
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
  var btn = document.getElementById("btnReport");
  if(btn) btn.style.display = adminIsUnlocked_() ? "block" : "none";
  var btnS = document.getElementById("btnSessions");
  if(btnS) btnS.style.display = adminIsUnlocked_() ? "block" : "none";
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
  if((p==="report" || p==="global_sessions") && !adminIsUnlocked_()){
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
  if(cur==="b2c_pick"){ syncLeaderPickUI(); restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_pack"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_return"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_qc"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_disposal"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_relabel"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="import_unload"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="import_scan_pallet"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="import_loadout"){ restoreState(); renderActiveLists(); refreshUI(); }

  if(cur==="b2b_menu"){ refreshUI(); }
  if(cur==="b2b_unload"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2b_tally"){ restoreState(); renderActiveLists(); renderB2bTallyUI(); refreshUI(); }
  if(cur==="b2b_workorder"){ restoreState(); renderActiveLists(); renderB2bWorkorderUI(); refreshUI(); }
  if(cur==="b2b_outbound"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2b_inventory"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="b2c_inventory"){ restoreState(); renderActiveLists(); refreshUI(); }
  if(cur==="warehouse_cleanup"){ restoreState(); renderActiveLists(); refreshUI(); }

  if(cur==="active_now"){ refreshActiveNow(); }
  if(cur==="global_sessions"){ refreshGlobalSessions(); }
  if(cur==="b2c_menu"){ refreshUI(); }
  if(cur==="import_menu"){ refreshUI(); }
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
  "b2c_tally":   { biz:"B2C", task:"TALLY" },
  "b2c_pick":    { biz:"B2C", task:"PICK" },
  "b2c_relabel": { biz:"B2C", task:"RELABEL" },
  "b2c_bulkout": { biz:"B2C", task:"批量出库" },
  "b2c_pack":    { biz:"B2C", task:"PACK" },
  "b2c_return":  { biz:"B2C", task:"退件入库" },
  "b2c_qc":      { biz:"B2C", task:"质检" },
  "b2c_disposal":{ biz:"B2C", task:"废弃处理" },
  "b2c_inventory":{ biz:"B2C", task:"B2C盘点" },

  // ===== 进口快件 =====
  "import_unload":      { biz:"IMPORT", task:"卸货" },
  "import_scan_pallet": { biz:"IMPORT", task:"过机扫描码托" },
  "import_loadout":     { biz:"IMPORT", task:"装柜/出货" },

  // ===== B2B =====
  "b2b_unload":    { biz:"B2B", task:"B2B卸货" },
  "b2b_tally":     { biz:"B2B", task:"B2B入库理货" },
  "b2b_workorder": { biz:"B2B", task:"B2B工单操作" },
  "b2b_outbound":  { biz:"B2B", task:"B2B出库" },
  "b2b_inventory": { biz:"B2B", task:"B2B盘点" },

  // ===== 仓库整理 =====
  "warehouse_cleanup": { biz:"WAREHOUSE", task:"仓库整理" }
};

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

var relabelTimerHandle = null;
var relabelStartTs = null;

var leaderPickBadge = localStorage.getItem("leader_pick_badge") || null;
var leaderPickOk = false;
var pendingLeaderEnd = null;

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
  // 1. 若 QR 里没有 biz/task，先从服务器查
  if(!biz || !task){
    try{
      var info = await sessionInfoServer_(sid);
      biz  = info.biz  || "";
      task = info.task || "";
    }catch(e){ /* 网络异常时继续尝试页面上下文 */ }
  }
  // 2. 还没有就用当前页面上下文
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
  { task:"PICK",         get:function(){return activePick;},           set:function(s){activePick=s;},           countId:"pickCount",              listId:"pickActiveList",              keyFn:keyActivePick,          emptyMsg:"当前没有人在拣货作业中（无需退出）。" },
  { task:"RELABEL",      get:function(){return activeRelabel;},        set:function(s){activeRelabel=s;},        countId:"relabelCount",           listId:"relabelActiveList",           keyFn:keyActiveRelabel,       emptyMsg:"当前没有人在换单作业中（无需退出）。" },
  { task:"PACK",         get:function(){return activePack;},           set:function(s){activePack=s;},           countId:"packCount",              listId:"packActiveList",              keyFn:keyActivePack,          emptyMsg:"当前没有人在验货贴单打包作业中（无需退出）。" },
  { task:"TALLY",        get:function(){return activeTally;},          set:function(s){activeTally=s;},          countId:"tallyCount",             listId:"tallyActiveList",             keyFn:keyActiveTally,         emptyMsg:"当前没有人在理货作业中（无需退出）。" },
  { task:"批量出库",      get:function(){return activeBulkOut;},        set:function(s){activeBulkOut=s;},        countId:"bulkoutCount",           listId:"bulkoutActiveList",           keyFn:keyActiveBulkOut,       emptyMsg:"当前没有人在批量出库作业中（无需退出）。" },
  { task:"退件入库",      get:function(){return activeReturn;},         set:function(s){activeReturn=s;},         countId:"returnCount",            listId:"returnActiveList",            keyFn:keyActiveReturn,        emptyMsg:"当前没有人在退件入库作业中（无需退出）。" },
  { task:"质检",         get:function(){return activeQc;},             set:function(s){activeQc=s;},             countId:null,                     listId:null,                          keyFn:keyActiveQc,            emptyMsg:"当前没有人在质检作业中（无需退出）。" },
  { task:"废弃处理",      get:function(){return activeDisposal;},       set:function(s){activeDisposal=s;},       countId:null,                     listId:null,                          keyFn:keyActiveDisposal,      emptyMsg:"当前没有人在废弃处理作业中（无需退出）。" },
  { task:"卸货",         get:function(){return activeImportUnload;},   set:function(s){activeImportUnload=s;},   countId:"importUnloadCount",      listId:"importUnloadActiveList",      keyFn:keyActiveImportUnload,  emptyMsg:"当前没有人在卸货作业中（无需退出）。" },
  { task:"过机扫描码托",  get:function(){return activeImportScanPallet;},set:function(s){activeImportScanPallet=s;},countId:"importScanPalletCount", listId:"importScanPalletActiveList",  keyFn:keyActiveImportScanPallet, emptyMsg:"当前没有人在过机扫描码托作业中（无需退出）。" },
  { task:"装柜/出货",    get:function(){return activeImportLoadout;},  set:function(s){activeImportLoadout=s;},  countId:"importLoadoutCount",     listId:"importLoadoutActiveList",     keyFn:keyActiveImportLoadout, emptyMsg:"当前没有人在装柜/出货作业中（无需退出）。" },
  { task:"B2B卸货",      get:function(){return activeB2bUnload;},      set:function(s){activeB2bUnload=s;},      countId:"b2bUnloadCount",         listId:"b2bUnloadActiveList",         keyFn:keyActiveB2bUnload,     emptyMsg:"当前没有人在B2B卸货作业中（无需退出）。" },
  { task:"B2B入库理货",  get:function(){return activeB2bTally;},       set:function(s){activeB2bTally=s;},       countId:"b2bTallyCount",          listId:"b2bTallyActiveList",          keyFn:keyActiveB2bTally,      emptyMsg:"当前没有人在B2B入库理货作业中（无需退出）。" },
  { task:"B2B工单操作",  get:function(){return activeB2bWorkorder;},   set:function(s){activeB2bWorkorder=s;},   countId:"b2bWorkorderCount",      listId:"b2bWorkorderActiveList",      keyFn:keyActiveB2bWorkorder,  emptyMsg:"当前没有人在B2B工单操作作业中（无需退出）。" },
  { task:"B2B出库",      get:function(){return activeB2bOutbound;},    set:function(s){activeB2bOutbound=s;},    countId:"b2bOutboundCount",       listId:"b2bOutboundActiveList",       keyFn:keyActiveB2bOutbound,   emptyMsg:"当前没有人在B2B出库作业中（无需退出）。" },
  { task:"B2B盘点",      get:function(){return activeB2bInventory;},   set:function(s){activeB2bInventory=s;},   countId:"b2bInventoryCount",      listId:"b2bInventoryActiveList",      keyFn:keyActiveB2bInventory,  emptyMsg:"当前没有人在B2B盘点作业中（无需退出）。" },
  { task:"B2C盘点",      get:function(){return activeB2cInventory;},   set:function(s){activeB2cInventory=s;},   countId:"b2cInventoryCount",      listId:"b2cInventoryActiveList",      keyFn:keyActiveB2cInventory,  emptyMsg:"当前没有人在B2C盘点作业中（无需退出）。" },
  { task:"仓库整理",      get:function(){return activeWarehouseCleanup;},set:function(s){activeWarehouseCleanup=s;},countId:"warehouseCleanupCount",listId:"warehouseCleanupActiveList",  keyFn:keyActiveWarehouseCleanup, emptyMsg:"当前没有人在仓库整理作业中（无需退出）。" }
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
    (params.badgeRaw||"")
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
}

/** ===== Utils ===== */
function makeDeviceId(){
  var id = localStorage.getItem("device_id");
  if(!id){
    id = "DEV-" + Math.random().toString(16).slice(2) + "-" + Date.now().toString().slice(-6);
    localStorage.setItem("device_id", id);
  }
  return id;
}

function makePickSessionId(){
  var d = new Date();
  var yyyy = d.getFullYear();
  var mm = String(d.getMonth()+1).padStart(2,'0');
  var dd = String(d.getDate()).padStart(2,'0');
  var hh = String(d.getHours()).padStart(2,'0');
  var mi = String(d.getMinutes()).padStart(2,'0');
  var ss = String(d.getSeconds()).padStart(2,'0');
  return "PS-" + yyyy + mm + dd + "-" + hh + mi + ss + "-" + makeDeviceId().slice(-6);
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

function netBusyOn_(action){
  NET_BUSY = true;
  // 给用户一个明确提示，避免疯狂连点
  if(action){
    setStatus("请求中... " + action + "（请勿重复点击）⏳", true);
  }
}

function netBusyOff_(){
  NET_BUSY = false;
}
function refreshUI(){
  var dev = document.getElementById("device");
  var ses = document.getElementById("session");
  if(dev) dev.textContent = makeDeviceId();
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
function jsonp(url, params){
  return new Promise(function(resolve, reject){
    // ✅ 如果上一个请求还没结束，直接拒绝，防止连点堆积导致越来越卡
var action = (params && params.action) ? String(params.action) : "";
if(NET_BUSY){
  reject(new Error("busy: previous request not finished"));
  return;
}
netBusyOn_(action);
    var cb = "cb_" + Math.random().toString(16).slice(2);
    var qs = [];
    for(var k in params){
      if(!params.hasOwnProperty(k)) continue;
      qs.push(encodeURIComponent(k) + "=" + encodeURIComponent(params[k]));
    }
    qs.push("callback=" + encodeURIComponent(cb));
    var src = url + "?" + qs.join("&");

    // PERF
    var t0 = Date.now();
    if(PERF_ON && action){
      setStatus("请求中... " + action + " ⏳", true);
    }

    var script = document.createElement("script");
    var timer = setTimeout(function(){
      cleanup();
      var dt = Date.now() - t0;
      if(PERF_ON && action){
        setStatus("超时 ❌ " + action + " " + dt + "ms", false);
        perfLog_("TIMEOUT action=" + action + " dt=" + dt + "ms src=" + src);
      }
      reject(new Error("jsonp timeout"));
    }, 12000);

    function cleanup(){
      try{ delete window[cb]; }catch(e){ window[cb]=undefined; }
      if(script && script.parentNode) script.parentNode.removeChild(script);
      clearTimeout(timer);
      // ✅ 释放网络忙状态
      netBusyOff_();
    }

    window[cb] = function(data){
      cleanup();
      var dt = Date.now() - t0;
      var ok = data && data.ok === true;

      if(PERF_ON && action){
        setStatus((ok ? "完成 ✅ " : "失败 ❌ ") + action + " " + dt + "ms", ok);
        perfLog_((ok ? "OK" : "BAD") + " action=" + action + " dt=" + dt + "ms");
      }
      resolve(data);
    };

    script.onerror = function(){
      cleanup();
      var dt = Date.now() - t0;
      if(PERF_ON && action){
        setStatus("错误 ❌ " + action + " " + dt + "ms", false);
        perfLog_("ERROR action=" + action + " dt=" + dt + "ms src=" + src);
      }
      reject(new Error("jsonp error"));
    };

    script.src = src;
    document.body.appendChild(script);
  });
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
    for(var i=0;i<q.length;i++){
      var item = q[i];
      try{
        item.tries = (item.tries||0) + 1;
        await submitEventSync_(item.payload, true);
      }catch(e){
        if(item.tries < 8) keep.push(item);
      }
    }
    saveQueue_(keep);
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
    device_id: makeDeviceId(),
    client_ms: (o.client_ms || Date.now())
  };

  var res = await jsonp(LOCK_URL, params);

  if(!res || res.ok !== true){
  var er = (res && res.error) ? String(res.error) : "提交失败：event_submit failed";
  if(er === "task_not_started"){
    throw new Error("该环节还没点【开始】。\n请先点击【开始理货/开始拣货/开始换单/开始批量出库】再扫码加入。");
  }
  if(er === "session_closed"){
    cleanupLocalSession_();
    throw new Error("该趟次已被关闭（可能是管理员强制结束）。\n本地已自动清除，请重新开始新的趟次。");
  }
    if(er === "device_has_open_session"){
  const ob = (res && res.open_biz) ? res.open_biz : "";
  const ot = (res && res.open_task) ? res.open_task : "";
  const os = (res && res.open_session) ? res.open_session : "";
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
      "占用设备: " + (lk.device_id || "未知") + "\n" +
      "占用趟次: " + (lk.session || "未知") + "\n\n" +
      "请先在原设备退出（leave）后再加入。";
    throw new Error(msg);
  }

  return res;
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
    device_id: makeDeviceId()
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
  TASK_REGISTRY.forEach(function(reg){
    localStorage.removeItem(reg.keyFn());
    reg.set(new Set());
  });

  if(CUR_CTX && CUR_CTX.biz && CUR_CTX.task){
    clearSess_(CUR_CTX.biz, CUR_CTX.task);
  }
  currentSessionId = null;
  refreshUI();
}

async function endSessionGlobal_(){
  if(!currentSessionId){ setStatus("没有未结束趟次", false); return; }

  var r = await sessionCloseServer_();
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
    submitEvent({ event:"end", event_id: evId, biz: endBiz, task:"SESSION", pick_session_id: currentSessionId });
     addRecent(evId);
  }

  setStatus("趟次结束已记录（待上传）✅", true);
  cleanupLocalSession_();
}

function taskAutoSession_(task){
  return task === "PACK" || task === "退件入库" || task === "质检" || task === "废弃处理" || task === "卸货" || task === "过机扫描码托" || task === "装柜/出货"
    || task === "B2B卸货" || task === "B2B出库" || task === "B2B盘点"
    || task === "B2C盘点" || task === "仓库整理";
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
        submitEvent({ event:"end", event_id: evIdEnd, biz: endBiz, task:"SESSION", pick_session_id: currentSessionId });
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
function isDaId(id){ return /^DA-\d{8}-.+$/.test(id); }
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
  return arr.map(function(x){ return '<span class="tag">' + badgeDisplay(x) + '</span>'; }).join("");
}

function renderActiveLists(){
  TASK_REGISTRY.forEach(function(reg){
    var cnt = reg.countId ? document.getElementById(reg.countId) : null;
    var lst = reg.listId ? document.getElementById(reg.listId) : null;
    if(cnt) cnt.textContent = String(reg.get().size);
    if(lst) lst.innerHTML = renderSetToHtml(reg.get());
  });
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
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+String(x)+'</span>'; }).join(" ");
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
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+String(x)+'</span>'; }).join(" ");
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

async function refreshActiveNow(){
  try{
    setStatus("拉取在岗中... ⏳", true);
    var res = await jsonp(LOCK_URL, { action:"active_now" });

    if(!res || res.ok !== true){
      setStatus("在岗拉取失败 ❌ " + (res && res.error ? res.error : ""), false);
      alert("在岗拉取失败：" + (res && res.error ? res.error : "unknown"));
      return;
    }

    var allActive = res.active || [];
    var asof = res.asof || Date.now();

    // 筛选
    var filterEl = document.getElementById("activeNowFilterBiz");
    var filterBiz = filterEl ? filterEl.value : "";
    var active = filterBiz ? allActive.filter(function(x){ return (x.biz||"") === filterBiz; }) : allActive;

    var meta = document.getElementById("activeNowMeta");
    if(meta) meta.textContent = "人数: " + active.length + " ｜ " + new Date(asof).toLocaleString();

    var by = {};
    active.forEach(function(x){
      var k = (x.biz||"") + " / " + (x.task||"");
      by[k] = (by[k]||0) + 1;
    });

    var sumEl = document.getElementById("activeNowSummary");
    if(sumEl){
      var keys = Object.keys(by).sort();
      sumEl.innerHTML = keys.length
        ? keys.map(function(k){ return '<span class="tag">'+esc(k)+': '+by[k]+'</span>'; }).join(" ")
        : '<span class="muted">当前无人在岗</span>';
    }

    var listEl = document.getElementById("activeNowList");
    if(listEl){
      if(active.length===0){
        listEl.innerHTML = '<div class="muted">当前无人在岗</div>';
      }else{
        var now = Date.now();
        var isAdmin = adminIsUnlocked_();
        listEl.innerHTML = active.map(function(x){
          var dur = fmtDur(now - (x.since||now));
          var forceBtn = isAdmin
            ? '<button class="small bad" style="margin-top:6px;width:auto;" ' +
                'data-badge="'+esc(x.badge)+'" data-task="'+esc(x.task||"")+'" data-session="'+esc(x.session||"")+'" data-biz="'+esc(x.biz||"")+'" data-device="'+esc(x.device_id||"")+'" ' +
                'onclick="adminForceLeave(this)">强制下线 / 강제 퇴장</button>'
            : "";
          return (
            '<div style="border:1px solid #eee;border-radius:12px;padding:10px;margin:8px 0;">' +
              '<div style="font-weight:700;">'+esc(x.badge)+'</div>' +
              '<div class="muted" style="margin-top:4px;">作业: '+esc(x.biz)+' / '+esc(x.task)+' ｜ 在岗: '+esc(dur)+'</div>' +
              '<div class="muted" style="margin-top:4px;">session: '+esc(x.session||"")+' ｜ device: '+esc(x.device_id||"")+'</div>' +
              forceBtn +
            '</div>'
          );
        }).join("");
      }
    }

    setStatus("在岗已更新 ✅", true);
  }catch(e){
    setStatus("在岗拉取异常 ❌ " + e, false);
    alert("在岗拉取异常：" + e);
  }
}

async function adminForceLeave(btn){
  if(!adminIsUnlocked_()){ alert("请先解锁管理员模式"); return; }
  var badge = btn.getAttribute("data-badge") || "";
  var task = btn.getAttribute("data-task") || "";
  var session = btn.getAttribute("data-session") || "";
  var biz = btn.getAttribute("data-biz") || "";
  var device_id = btn.getAttribute("data-device") || "";
  var ok = confirm("强制下线 / 강제 퇴장\n\n工牌：" + badge + "\n任务：" + biz + " / " + task + "\n\n确定要强制下线吗？\n정말 강제 퇴장하시겠습니까?");
  if(!ok) return;
  try{
    setStatus("强制下线中... ⏳", true);
    var res = await jsonp(LOCK_URL, { action:"admin_force_leave", k:adminKey_(), badge:badge, task:task, session:session, biz:biz, device_id:device_id });
    if(!res || res.ok !== true){
      setStatus("强制下线失败 ❌ " + (res && res.error ? res.error : ""), false);
      alert("强制下线失败：" + (res && res.error ? res.error : "unknown"));
      return;
    }
    setStatus("强制下线成功 ✅", true);
    refreshActiveNow();
  }catch(e){
    setStatus("强制下线异常 ❌ " + e, false);
    alert("强制下线异常：" + e);
  }
}

async function refreshGlobalSessions(){
  var filterEl = document.getElementById("sessionFilterBiz");
  var filterBiz = filterEl ? filterEl.value : "";
  var metaEl = document.getElementById("sessionListMeta");
  var contentEl = document.getElementById("sessionListContent");
  if(metaEl) metaEl.textContent = "加载中... ⏳";
  if(contentEl) contentEl.innerHTML = "";
  try{
    var params = { action:"admin_sessions_list", k:adminKey_() };
    if(filterBiz) params.biz = filterBiz;
    var res = await jsonp(LOCK_URL, params);
    if(!res || res.ok !== true){
      if(metaEl) metaEl.textContent = "加载失败 ❌ " + (res && res.error ? res.error : "");
      return;
    }
    var sessions = res.sessions || [];
    if(metaEl) metaEl.textContent = "共 " + sessions.length + " 条 ｜ " + new Date(res.asof||Date.now()).toLocaleString();
    if(!contentEl) return;
    if(sessions.length===0){
      contentEl.innerHTML = '<div class="muted">暂无Session记录</div>';
      return;
    }
    contentEl.innerHTML = sessions.map(function(s){
      var statusClass = s.status==="OPEN" ? "ok" : "muted";
      var activeList = (s.active||[]).map(function(lk){ return esc(lk.badge||""); }).join(", ");
      var forceEndBtn = "";
      if(s.status==="OPEN" && (!s.active || s.active.length===0)){
        forceEndBtn = '<button class="small bad" style="margin-top:6px;width:auto;" data-session="'+esc(s.session)+'" onclick="adminForceEndSession(this)">强制结束 / 강제 종료</button>';
      }
      return (
        '<div style="border:1px solid #eee;border-radius:12px;padding:10px;margin:8px 0;">' +
          '<div style="font-weight:700;font-size:13px;">'+esc(s.session)+'</div>' +
          '<div class="muted" style="margin-top:4px;">'+
            '状态: <span class="'+statusClass+'">'+esc(s.status)+'</span> ｜ ' +
            'biz: '+esc(s.biz||"-")+' ｜ task: '+esc(s.task||"-")+
          '</div>' +
          '<div class="muted" style="margin-top:4px;">创建: '+new Date(s.created_ms||0).toLocaleString()+'</div>' +
          (s.active && s.active.length>0
            ? '<div class="muted" style="margin-top:4px;">在岗('+s.active.length+'): '+activeList+'</div>'
            : '<div class="muted" style="margin-top:4px;">当前无在岗人员</div>') +
          forceEndBtn +
        '</div>'
      );
    }).join("");
  }catch(e){
    if(metaEl) metaEl.textContent = "加载异常 ❌ " + e;
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
    var res = await jsonp(LOCK_URL, { action:"admin_force_end_session", k:adminKey_(), session:session });
    if(!res || res.ok !== true){
      setStatus("强制结束失败 ❌ " + (res && res.error ? res.error : ""), false);
      alert("强制结束失败：" + (res && res.error ? res.error : "unknown"));
      return;
    }
    setStatus("强制结束成功 ✅", true);
    refreshGlobalSessions();
  }catch(e){
    setStatus("强制结束异常 ❌ " + e, false);
    alert("强制结束异常：" + e);
  }
}

/** ===== Start / End: B2C tasks ===== */
  async function startTally(){
  // ✅ 防连点保护
  if(currentSessionId){
    var ok = confirm("当前已有进行中的趟次：" + currentSessionId + "\n\n确定要放弃当前趟次、重新开始一个新趟次吗？\n（一般请取消，继续当前趟次。）");
    if(!ok) return;
  }
  var btn = event && event.target ? event.target : null;
  if(btn){ btn.disabled = true; btn.textContent = "处理中..."; }


  try{
    var biz = "B2C", task = "TALLY";

    // ✅ A：每次开始都新建本任务趟次
    currentSessionId = makePickSessionId();
    CUR_CTX = { biz: biz, task: task, page: "b2c_tally" };
    setSess_(biz, task, currentSessionId);

    // ✅ 清空本任务本地状态（避免上一趟数据带入）
    scannedInbounds = new Set();
    activeTally = new Set();
    persistState();
    refreshUI();

    // ✅ start 必须同步确认（后端会强制：没 start 不允许 join）
    var evId = makeEventId({ event:"start", biz:biz, task:task, wave_id:"", badgeRaw:"" });
    await submitEventSync_({ event:"start", event_id: evId, biz:biz, task:task, pick_session_id: currentSessionId }, true);
    addRecent(evId);

    renderActiveLists();
    renderInboundCountUI();
    setStatus("理货开始 ✅ 新趟次: " + currentSessionId, true);
    }catch(e){
    setStatus("理货开始失败 ❌ " + e, false);
    alert(String(e));
  }finally{
    if(btn){ btn.disabled = false; btn.textContent = "开始理货 시작"; }
  }
}
async function endTally(){ return endSessionGlobal_(); }

async function startBulkOut(){
  if(currentSessionId){
    var ok = confirm("当前已有进行中的趟次：" + currentSessionId + "\n\n确定要放弃当前趟次、重新开始一个新趟次吗？\n（一般请取消，继续当前趟次。）");
    if(!ok) return;
  }
  var btn = event && event.target ? event.target : null;
  if(btn){ btn.disabled = true; btn.textContent = "处理中..."; }

  try{
    var biz = "B2C", task = "批量出库";

    currentSessionId = makePickSessionId();
    CUR_CTX = { biz: biz, task: task, page: "b2c_bulkout" };
    setSess_(biz, task, currentSessionId);

    scannedBulkOutOrders = new Set();
    activeBulkOut = new Set();
    persistState();
    refreshUI();

    var evId = makeEventId({ event:"start", biz:biz, task:task, wave_id:"", badgeRaw:"" });
    await submitEventSync_({ event:"start", event_id: evId, biz:biz, task:task, pick_session_id: currentSessionId }, true);
    addRecent(evId);

    renderActiveLists();
    renderBulkOutUI();
    setStatus("批量出库开始 ✅ 新趟次: " + currentSessionId, true);
    }catch(e){
    setStatus("理货开始失败 ❌ " + e, false);
    alert(String(e));
  }finally{
    if(btn){ btn.disabled = false; btn.textContent = "开始批量出库 시작"; }
  }
}
async function endBulkOut(){ return endSessionGlobal_(); }

/** ===== B2B Tally (like B2C Tally) ===== */
async function startB2bTally(){
  if(currentSessionId){
    var ok = confirm("当前已有进行中的趟次：" + currentSessionId + "\n\n确定要放弃当前趟次、重新开始一个新趟次吗？");
    if(!ok) return;
  }
  var btn = event && event.target ? event.target : null;
  if(btn){ btn.disabled = true; btn.textContent = "处理中..."; }
  try{
    var biz = "B2B", task = "B2B入库理货";
    currentSessionId = makePickSessionId();
    CUR_CTX = { biz: biz, task: task, page: "b2b_tally" };
    setSess_(biz, task, currentSessionId);
    scannedB2bTallyOrders = new Set();
    activeB2bTally = new Set();
    persistState(); refreshUI();
    var evId = makeEventId({ event:"start", biz:biz, task:task, wave_id:"", badgeRaw:"" });
    await submitEventSync_({ event:"start", event_id: evId, biz:biz, task:task, pick_session_id: currentSessionId }, true);
    addRecent(evId);
    renderActiveLists(); renderB2bTallyUI();
    setStatus("B2B理货开始 ✅ 新趟次: " + currentSessionId, true);
  }catch(e){
    setStatus("B2B理货开始失败 ❌ " + e, false); alert(String(e));
  }finally{
    if(btn){ btn.disabled = false; btn.textContent = "开始理货 시작"; }
  }
}
async function endB2bTally(){ return endSessionGlobal_(); }

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
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+String(x)+'</span>'; }).join(" ");
    }
  }
}

/** ===== B2B Workorder (like B2C BulkOut) ===== */
async function startB2bWorkorder(){
  if(currentSessionId){
    var ok = confirm("当前已有进行中的趟次：" + currentSessionId + "\n\n确定要放弃当前趟次、重新开始一个新趟次吗？");
    if(!ok) return;
  }
  var btn = event && event.target ? event.target : null;
  if(btn){ btn.disabled = true; btn.textContent = "处理中..."; }
  try{
    var biz = "B2B", task = "B2B工单操作";
    currentSessionId = makePickSessionId();
    CUR_CTX = { biz: biz, task: task, page: "b2b_workorder" };
    setSess_(biz, task, currentSessionId);
    scannedB2bWorkorders = new Set();
    activeB2bWorkorder = new Set();
    persistState(); refreshUI();
    var evId = makeEventId({ event:"start", biz:biz, task:task, wave_id:"", badgeRaw:"" });
    await submitEventSync_({ event:"start", event_id: evId, biz:biz, task:task, pick_session_id: currentSessionId }, true);
    addRecent(evId);
    renderActiveLists(); renderB2bWorkorderUI();
    setStatus("B2B工单操作开始 ✅ 新趟次: " + currentSessionId, true);
  }catch(e){
    setStatus("B2B工单操作开始失败 ❌ " + e, false); alert(String(e));
  }finally{
    if(btn){ btn.disabled = false; btn.textContent = "开始操作 시작"; }
  }
}
async function endB2bWorkorder(){ return endSessionGlobal_(); }

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
      l.innerHTML = show.map(function(x){ return '<span class="tag">'+String(x)+'</span>'; }).join(" ");
    }
  }
}

async function startPicking(){
  if(currentSessionId){
    var ok = confirm("当前已有进行中的趟次：" + currentSessionId + "\n\n确定要放弃当前趟次、重新开始一个新趟次吗？\n（一般请取消，继续当前趟次。）");
    if(!ok) return;
  }
  var btn = event && event.target ? event.target : null;
  if(btn){ btn.disabled = true; btn.textContent = "处理中..."; }

  try{
    var biz = "B2C", task = "PICK";

    currentSessionId = makePickSessionId();
    CUR_CTX = { biz: biz, task: task, page: "b2c_pick" };
    setSess_(biz, task, currentSessionId);

    scannedWaves = new Set();
    activePick = new Set();
    leaderPickOk = false;
    syncLeaderPickUI();
    persistState();
    refreshUI();

    var evId = makeEventId({ event:"start", biz:biz, task:task, wave_id:"", badgeRaw:"" });
    await submitEventSync_({ event:"start", event_id: evId, biz:biz, task:task, pick_session_id: currentSessionId }, true);
    addRecent(evId);

    renderActiveLists();
    setStatus("拣货开始 ✅ 新趟次: " + currentSessionId, true);
    }catch(e){
    setStatus("拣货开始失败 ❌ " + e, false);
    alert(String(e));
  }finally{
    if(btn){ btn.disabled = false; btn.textContent = "开始拣货 시작"; }
  }
}

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

async function startRelabel(){
  if(currentSessionId){
    var ok = confirm("当前已有进行中的趟次：" + currentSessionId + "\n\n确定要放弃当前趟次、重新开始一个新趟次吗？\n（一般请取消，继续当前趟次。）");
    if(!ok) return;
  }
  var btn = event && event.target ? event.target : null;
  if(btn){ btn.disabled = true; btn.textContent = "处理中..."; }

  try{
    var biz = "B2C", task = "RELABEL";

    currentSessionId = makePickSessionId();
    CUR_CTX = { biz: biz, task: task, page: "b2c_relabel" };
    setSess_(biz, task, currentSessionId);

    activeRelabel = new Set();
    relabelStartTs = Date.now();
    startRelabelTimer();
    persistState();
    refreshUI();

    var evId = makeEventId({ event:"start", biz:biz, task:task, wave_id:"", badgeRaw:"" });
    await submitEventSync_({ event:"start", event_id: evId, biz:biz, task:task, pick_session_id: currentSessionId }, true);
    addRecent(evId);

    renderActiveLists();
    setStatus("换单开始 ✅ 新趟次: " + currentSessionId, true);
    }catch(e){
    setStatus("换单开始失败 ❌ " + e, false);
    alert(String(e));
  }finally{
    if(btn){ btn.disabled = false; btn.textContent = "开始换单 시작"; }
  }
}
async function endRelabel(){ return endSessionGlobal_(); }

/** ===== PICK end (leader confirmation) ===== */
async function endPicking(){
  try{
    if(!currentSessionId){ setStatus("没有未结束趟次", false); return; }
    if(!(await guardSessionOpenOrAlert_("该趟次已结束（无法结束拣货），请重新开始新的趟次。"))) return;

    if(!leaderPickOk){ setStatus("需要组长先登录（扫码）", false); return; }
    if(activePick.size > 0){
      setStatus("还有人员未退出，禁止结束", false);
      alert("还有人员未退出作业，不能结束拣货。");
      return;
    }

    pendingLeaderEnd = { biz:"B2C", task:"PICK" };
    scanMode = "leaderEndPick";
    document.getElementById("scanTitle").textContent = "扫码组长工牌确认结束 / 팀장 종료 확인";
    await openScannerCommon();
  }catch(e){
    setStatus("结束确认失败 ❌ " + e, false);
  }
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

/** ===== Labor (join/leave) ===== */
async function joinWork(biz, task){
  biz = String(biz||"").trim();
  task = String(task||"").trim();

  // ✅ 每任务独立 session：先拿到该任务的 session
  var sid = getSess_(biz, task);

  // 自动 session 的任务：第一次 join 时自动开新趟次，并同步发送 start
  if(!sid && taskAutoSession_(task)){
    sid = makePickSessionId();
    currentSessionId = sid;
    CUR_CTX = { biz: biz, task: task, page: getHashPage() };
    setSess_(biz, task, sid);

    // 清空该任务的本地状态
    if(task==="PACK") activePack = new Set();
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
    persistState(); refreshUI();

    // ✅ start 必须同步确认（否则后端会拒绝 join）
    var evIdStart = makeEventId({ event:"start", biz:biz, task: task, wave_id:"", badgeRaw:"" });
    if(!hasRecent(evIdStart)){
      await submitEventSync_({ event:"start", event_id: evIdStart, biz: biz, task: task, pick_session_id: sid }, true);
      addRecent(evIdStart);
    }
  }

  if(!sid){
    // 非自动任务（TALLY/PICK/RELABEL/批量出库）：必须先点“开始”生成新趟次
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

  if(!(await guardSessionOpenOrAlert_("该趟次已结束：不能再退出作业，请重新开始或加入新趟次。"))) return;

  var reg_ = taskReg_(task);
  if(reg_ && reg_.get().size === 0){ alert(reg_.emptyMsg || ("当前没有人在" + task + "作业中（无需退出）。")); return; }

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
    var yyyy = d.getFullYear();
    var mm = String(d.getMonth()+1).padStart(2,'0');
    var dd = String(d.getDate()).padStart(2,'0');
    var dateStr = yyyy + mm + dd;

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
      box.innerHTML = '<div style="font-weight:700;margin-bottom:6px;">' + da + '</div><div id="' + safeId + '"></div>';
      listEl.appendChild(box);
      new QRCode(document.getElementById(safeId), { text: "DA-" + dateStr + "-" + encodeURIComponent(name), width: 160, height: 160 });

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
    box.innerHTML = '<div style="font-weight:700;margin-bottom:6px;">'+payload+'</div><div id="'+safeKey+'"></div>';
    listEl.appendChild(box);
    new QRCode(document.getElementById(safeKey), { text: "DAF-" + encodeURIComponent(name), width: 160, height: 160 });
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
    box.innerHTML = '<div style="font-weight:700;margin-bottom:6px;">'+payload+'</div><div id="'+safeKey+'"></div>';
    listEl.appendChild(box);
    new QRCode(document.getElementById(safeKey), { text: "EMP-" + encodeURIComponent(name), width: 160, height: 160 });
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
  scanner = new Html5Qrcode("reader");

  var onScan = async (decodedText) => {
    var code = decodedText.trim();
    try { code = decodeURIComponent(code); } catch(e) {}
    if(scanBusy) return;

    var now = Date.now();
    if(now - lastScanAt < 900) return;
    lastScanAt = now;

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

        var evIdX = makeEventId({ event:"wave", biz:"B2C", task:"TALLY", wave_id: code2, badgeRaw:"" });
        if(!hasRecent(evIdX)){
          submitEvent({ event:"wave", event_id: evIdX, biz:"B2C", task:"TALLY", pick_session_id: currentSessionId, wave_id: code2 });
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
      var ok = /^\d{4}-\d{4}-\d+$/.test(code);
      if(!ok){ setStatus("波次格式不对（例：2026-0224-6）", false); return; }
      if(scannedWaves.has(code)){ setStatus("重复波次已忽略 ⏭️ " + code, false); return; }

      scannedWaves.add(code);
      persistState();

      scanBusy = true;
      await pauseScanner();
      try{
        var evId = makeEventId({ event:"wave", biz:"B2C", task:"PICK", wave_id: code, badgeRaw:"" });
        if(hasRecent(evId)){ setStatus("重复扫码已忽略 ⏭️ " + code, false); await closeScanner(); return; }

        submitEvent({ event:"wave", event_id: evId, biz:"B2C", task:"PICK", pick_session_id: currentSessionId, wave_id: code });
        addRecent(evId);

        alert("已记录波次 ✅ " + code);
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
        alert("该工牌不在当前作业名单中，无法退出。\n请确认是否扫错工牌。");
        setStatus("不在岗，无法退出 ❌", false);
        await closeScanner();
        return;
      }

      if(laborAction === "join" && isAlreadyActive(laborTask, p2.raw)){
        alert("已在作业中 ✅ " + p2.raw);
        setStatus("已在作业中 ✅", true);
        await closeScanner();
        return;
      }

      scanBusy = true;
      await pauseScanner();
      setStatus("处理中... 请稍等 ⏳（join/leave 需确认锁）", true);

      try{
        var evId = makeEventId({ event:laborAction, biz:laborBiz, task:laborTask, wave_id:"", badgeRaw:p2.raw });
        if(hasRecent(evId)){ setStatus("重复扫描已忽略 ⏭️", false); await closeScanner(); return; }

        await submitEventSync_({
          event: laborAction,
          event_id: evId,
          biz: laborBiz,
          task: laborTask,
          pick_session_id: currentSessionId,
          da_id: p2.raw
        });

        addRecent(evId);

        applyActive(laborTask, laborAction, p2.raw);
        renderActiveLists();
        persistState();

        if(laborAction === "leave"){
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
        leaderPickOk = true; syncLeaderPickUI();
        alert("组长登录成功 ✅ " + p3.raw);
        setStatus("组长登录成功 ✅", true);
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }

    if(scanMode === "leaderEndPick"){
      if(!isOperatorBadge(code)){ setStatus("无效工牌（请扫 EMP-xxx|名字）", false); return; }
      var p4 = parseBadge(code);
      if(!p4.id.startsWith("EMP-")){ setStatus("请扫组长员工工牌（EMP-xxx|名字）", false); return; }

      scanBusy = true;
      await pauseScanner();
      try{
        leaderPickBadge = p4.raw; localStorage.setItem("leader_pick_badge", leaderPickBadge);
        await endSessionGlobal_();
        pendingLeaderEnd = null;
        leaderPickOk = false;
        refreshUI(); syncLeaderPickUI();
        await closeScanner();
      } finally { scanBusy = false; }
      return;
    }
  };

  try{
    await scanner.start({ facingMode: "environment" }, { fps: 10, qrbox: { width: 240, height: 240 } }, onScan);
  }catch(e){
    var cams = await Html5Qrcode.getCameras();
    var camId = cams && cams[0] ? cams[0].id : null;
    await scanner.start(camId, { fps:10, qrbox:{width:240,height:240}}, onScan);
  }
}

async function closeScanner(){
  try{
    if(scanner){ await scanner.stop(); await scanner.clear(); scanner = null; }
  }catch(e){}
  hideOverlay();
}

function comingSoon(msg){
  alert((msg||"准备中") + "\n\n我们会逐步上线。");
}


/** ===== Report (Admin-only) ===== */
var REPORT_CACHE = { header:[], rows:[], summary:[], people:[], timeline:[], anomalies_list:[], meta:{} };

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

  jsonp(LOCK_URL, {
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

function buildReportSummary_(){
  var header = REPORT_CACHE.header || [];
  var rows = REPORT_CACHE.rows || [];

  var iServer = header.indexOf("server_ms");
  var iEvent = header.indexOf("event");
  var iBadge = header.indexOf("badge");
  var iBiz = header.indexOf("biz");
  var iTask = header.indexOf("task");
  var iOk = header.indexOf("ok");
  if(iOk < 0) iOk = header.indexOf("ok"); // just in case

  var active = {}; // badge -> {t, biz, task}
  var acc = {};    // badge -> { total_ms, tasks: {biz|task:ms} }
  var timelineByBadge = {}; // badge -> [{biz,task,start_ms,end_ms,duration_ms,status}]
  var anomalies = { open:0, leave_without_join:0, rejoin_without_leave:0 };
  var anomaliesList = [];

  function addDur(badge, biz, task, dur){
    if(!acc[badge]) acc[badge] = { total_ms:0, tasks:{} };
    acc[badge].total_ms += dur;
    var k = biz + "|" + task;
    acc[badge].tasks[k] = (acc[badge].tasks[k]||0) + dur;
  }
  function addTimeline(badge, biz, task, startMs, endMs, status){
    if(!timelineByBadge[badge]) timelineByBadge[badge] = [];
    timelineByBadge[badge].push({
      biz: biz || "",
      task: task || "",
      start_ms: startMs || 0,
      end_ms: endMs || 0,
      duration_ms: Math.max(0, (endMs||0) - (startMs||0)),
      status: status || "NORMAL"
    });
  }
  function addAnomaly(type, badge, biz, task, atMs, note){
    anomaliesList.push({
      type: type || "unknown",
      badge: badge || "",
      biz: biz || "",
      task: task || "",
      at_ms: atMs || 0,
      note: note || ""
    });
  }

  var now = Date.now();
  for(var r=0;r<rows.length;r++){
    var row = rows[r];
    if(!row) continue;
    if(iOk >= 0){
      var okv = row[iOk];
      if(String(okv).toLowerCase()==="false" || Number(okv)===0) continue;
    }

    var ev = String(row[iEvent]||"").trim();
    if(ev!=="join" && ev!=="leave") continue;

    var badge = String(row[iBadge]||"").trim();
    if(!badge) continue;
    var biz = String(row[iBiz]||"").trim();
    var task = String(row[iTask]||"").trim();
    var t = Number(row[iServer]||0) || 0;

    if(ev==="join"){
      // 若已有未结束，先按当前时间截断
      if(active[badge]){
        var durRejoin = Math.max(0, t - active[badge].t);
        addDur(badge, active[badge].biz, active[badge].task, durRejoin);
        addTimeline(badge, active[badge].biz, active[badge].task, active[badge].t, t, "AUTO_CLOSE_REJOIN");
        anomalies.rejoin_without_leave++;
        addAnomaly("rejoin_without_leave", badge, active[badge].biz, active[badge].task, t, "join 前未 leave，已自动截断上一段");
      }
      active[badge] = { t:t, biz:biz, task:task };
    }else if(ev==="leave"){
      if(active[badge]){
        var durLeave = Math.max(0, t - active[badge].t);
        addDur(badge, active[badge].biz, active[badge].task, durLeave);
        addTimeline(badge, active[badge].biz, active[badge].task, active[badge].t, t, "NORMAL");
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
    addTimeline(b, active[b].biz, active[b].task, active[b].t, now, "OPEN");
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
      taskRows.push({
        biz: parts[0]||"",
        task: parts[1]||"",
        minutes: mins
      });

      out.push({
        badge: badge,
        biz: parts[0]||"",
        task: parts[1]||"",
        minutes: mins,
        total_minutes: msToMin_(obj.total_ms)
      });
    });

    taskRows.sort(function(a,b){ return b.minutes - a.minutes; });
    people.push({
      badge: badge,
      total_minutes: msToMin_(obj.total_ms),
      tasks: taskRows
    });
  });

  people.sort(function(a,b){ return b.total_minutes - a.total_minutes; });

  var timeline = Object.keys(timelineByBadge).sort().map(function(badge){
    var items = (timelineByBadge[badge] || []).slice().sort(function(a,b){
      return (a.start_ms||0) - (b.start_ms||0);
    }).map(function(x){
      return {
        biz: x.biz,
        task: x.task,
        start_ms: x.start_ms,
        end_ms: x.end_ms,
        minutes: msToMin_(x.duration_ms),
        status: x.status
      };
    });
    return { badge: badge, items: items };
  });

  anomaliesList.sort(function(a,b){ return (b.at_ms||0) - (a.at_ms||0); });

  REPORT_CACHE.summary = out;
  REPORT_CACHE.people = people;
  REPORT_CACHE.timeline = timeline;
  REPORT_CACHE.anomalies_list = anomaliesList;
  REPORT_CACHE.meta.anomalies = anomalies;
}

function renderReport_(){
  var metaEl = document.getElementById("reportMeta");
  var anomaliesEl = document.getElementById("reportAnomalies");
  var peopleEl = document.getElementById("reportPeople");
  var timelineEl = document.getElementById("reportTimeline");
  var tableEl = document.getElementById("reportTable");
  if(!metaEl || !anomaliesEl || !peopleEl || !timelineEl || !tableEl) return;

  var m = REPORT_CACHE.meta || {};
  var anomalies = (m.anomalies || {});
  var sum = REPORT_CACHE.summary || [];
  var people = REPORT_CACHE.people || [];
  var timeline = REPORT_CACHE.timeline || [];
  var anomaliesList = REPORT_CACHE.anomalies_list || [];

  metaEl.textContent =
    "区间(KST): " + (m.rangeLabel||"-") +
    " ｜ rows=" + (REPORT_CACHE.rows||[]).length +
    " ｜ open=" + (anomalies.open||0) +
    " ｜ leave无join=" + (anomalies.leave_without_join||0) +
    " ｜ 重复join=" + (anomalies.rejoin_without_leave||0);

  if(anomaliesList.length===0){
    anomaliesEl.innerHTML = '<div class="muted">异常列表：无</div>';
  }else{
    anomaliesEl.innerHTML =
      '<div style="font-weight:700;margin-bottom:6px;">异常列表</div>' +
      anomaliesList.map(function(a){
        return (
          '<div style="border:1px solid #ffe2a8;background:#fffaf0;border-radius:12px;padding:10px;margin:8px 0;">' +
            '<div style="font-weight:700;">' + esc(a.badge || "未知工牌") + ' ｜ ' + esc(a.type) + '</div>' +
            '<div class="muted" style="margin-top:4px;">' + esc(a.biz + '/' + a.task) + ' ｜ ' + esc(fmtTs_(a.at_ms)) + '</div>' +
            '<div class="muted" style="margin-top:4px;">' + esc(a.note || "") + '</div>' +
          '</div>'
        );
      }).join('');
  }

  if(people.length===0){
    peopleEl.innerHTML = '<div class="muted">暂无人员汇总</div>';
  }else{
    peopleEl.innerHTML = people.map(function(p){
      var taskText = (p.tasks||[]).map(function(t){
        return esc(t.biz + "/" + t.task) + ": " + esc(String(t.minutes)) + " 分";
      }).join(" ｜ ");

      return (
        '<div style="border:1px solid #eee;border-radius:12px;padding:10px;margin:8px 0;">' +
          '<div style="font-weight:700;">' + esc(p.badge) + ' ｜ 总工时 ' + esc(String(p.total_minutes)) + ' 分</div>' +
          '<div class="muted" style="margin-top:6px;">' + (taskText || "无任务") + '</div>' +
        '</div>'
      );
    }).join("");
  }

  if(timeline.length===0){
    timelineEl.innerHTML = '<div class="muted">时间线：暂无</div>';
  }else{
    timelineEl.innerHTML =
      '<div style="font-weight:700;margin-bottom:6px;">每人时间线（join → leave）</div>' +
      timeline.map(function(x){
        var lines = (x.items || []).map(function(it){
          var statusText = it.status === "OPEN" ? "（未退出）" : (it.status === "AUTO_CLOSE_REJOIN" ? "（重复join自动截断）" : "");
          return (
            '<div style="border-top:1px dashed #eee;padding:6px 0;">' +
              '<div>' + esc(it.biz + '/' + it.task) + ' ｜ ' + esc(String(it.minutes)) + ' 分 ' + esc(statusText) + '</div>' +
              '<div class="muted">' + esc(fmtTs_(it.start_ms)) + ' → ' + esc(fmtTs_(it.end_ms)) + '</div>' +
            '</div>'
          );
        }).join("");

        return (
          '<div style="border:1px solid #eee;border-radius:12px;padding:10px;margin:8px 0;">' +
            '<div style="font-weight:700;">' + esc(x.badge) + '</div>' +
            (lines || '<div class="muted" style="margin-top:6px;">无时间段</div>') +
          '</div>'
        );
      }).join("");
  }

  if(sum.length===0){
    tableEl.innerHTML = '<div class="muted">暂无数据（今天还没有 join/leave）</div>';
    return;
  }

  // table
  var html = '<div style="overflow:auto;border:1px solid #eee;border-radius:12px;">';
  html += '<table style="border-collapse:collapse;width:100%;min-width:700px;">';
  html += '<tr>' +
    '<th style="text-align:left;border-bottom:1px solid #eee;padding:8px;background:#fafafa;">badge</th>' +
    '<th style="text-align:left;border-bottom:1px solid #eee;padding:8px;background:#fafafa;">biz</th>' +
    '<th style="text-align:left;border-bottom:1px solid #eee;padding:8px;background:#fafafa;">task</th>' +
    '<th style="text-align:right;border-bottom:1px solid #eee;padding:8px;background:#fafafa;">minutes</th>' +
    '<th style="text-align:right;border-bottom:1px solid #eee;padding:8px;background:#fafafa;">badge total</th>' +
    '</tr>';

  for(var i=0;i<sum.length;i++){
    var r = sum[i];
    html += '<tr>' +
      '<td style="border-bottom:1px solid #f2f2f2;padding:8px;">' + esc(r.badge) + '</td>' +
      '<td style="border-bottom:1px solid #f2f2f2;padding:8px;">' + esc(r.biz) + '</td>' +
      '<td style="border-bottom:1px solid #f2f2f2;padding:8px;">' + esc(r.task) + '</td>' +
      '<td style="border-bottom:1px solid #f2f2f2;padding:8px;text-align:right;">' + esc(r.minutes) + '</td>' +
      '<td style="border-bottom:1px solid #f2f2f2;padding:8px;text-align:right;">' + esc(r.total_minutes) + '</td>' +
      '</tr>';
  }
  html += '</table></div>';
  tableEl.innerHTML = html;
}

function reportExportCSV(){
  if(!adminIsUnlocked_()){
    alert("管理员功能：请先解锁（标题连点 7 次）");
    return;
  }
  var sum = REPORT_CACHE.summary || [];
  if(sum.length===0){
    alert("没有可导出的数据（先点：拉取今天数据）");
    return;
  }

  var csv = [];
  csv.push(["badge","biz","task","minutes","badge_total_minutes"].join(","));
  for(var i=0;i<sum.length;i++){
    var r = sum[i];
    csv.push([
      '"' + String(r.badge).replace(/"/g,'""') + '"',
      '"' + String(r.biz).replace(/"/g,'""') + '"',
      '"' + String(r.task).replace(/"/g,'""') + '"',
      String(r.minutes||0),
      String(r.total_minutes||0)
    ].join(","));
  }

  var blob = new Blob([csv.join("\n")], {type:"text/csv;charset=utf-8;"});
  var a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  var rangeText = (REPORT_CACHE.meta.rangeLabel || "range").replace(/[^0-9A-Za-z_-]+/g, "_");
  a.download = "ck_report_" + rangeText + "_" + Date.now() + ".csv";
  a.click();
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
restoreState();
renderActiveLists();
bindAdminEasterEgg_();
adminApplyUI_();
setReportDefaultDates_();
renderPages();
flushQueue_();

