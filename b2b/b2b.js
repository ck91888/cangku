// ===== B2B 计划与作业单 =====
var API_URL = "https://api.ck91888.cn";
var KEY_STORAGE = "b2b_plan_k_v1";

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
  b2c_inbound: "B2C入库", b2b_inbound: "B2B入库", direct_transfer: "直接转发", other: "其他"
};
var WO_STATUS_LABEL = {
  draft: "草稿", issued: "已下发", working: "作业中", completed: "已完成", cancelled: "已取消"
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
var ALL_VIEWS = ["v-home","v-plan_create","v-wo_create","v-wo_list","v-wo_detail"];
function goView(name){
  ALL_VIEWS.forEach(function(v){ document.getElementById(v).style.display = (v === "v-" + name) ? "" : "none"; });
  if(name === "wo_list") initWoList();
}
function goHome(){
  goView("home");
  loadHome();
}

// ===== 导航入口：新建（清空编辑状态） =====
function goNewPlan(){
  _editingPlanId = null;
  goView("plan_create");
  initPlanForm(null);
}
function goNewWo(){
  _editingWoId = null;
  goView("wo_create");
  initWoCreate(null);
}

// ===== 首页 =====
function loadHome(){
  var today = kstToday();
  var tmr = kstTomorrow();
  document.getElementById("todayPill").textContent = today;
  loadPlansQuadrant(today, today, "ip-today-list", "ip-today-count");
  loadPlansQuadrant(tmr, tmr, "ip-tmr-list", "ip-tmr-count");
  loadWoQuadrant(today, today, "wo-today-list", "wo-today-count");
  loadWoQuadrant(tmr, tmr, "wo-tmr-list", "wo-tmr-count");
}

function loadPlansQuadrant(start, end, listId, countId){
  fetchApi({ action:"b2b_plan_list", start_day:start, end_day:end }).then(function(res){
    var el = document.getElementById(listId);
    var countEl = document.getElementById(countId);
    if(!res || !res.ok){ el.innerHTML = '<div class="q-empty">加载失败</div>'; return; }
    var plans = res.plans || [];
    plans.sort(function(a,b){ return (PLAN_STATUS_PRIORITY[a.status]||9) - (PLAN_STATUS_PRIORITY[b.status]||9); });
    if(countEl) countEl.textContent = plans.length + "条";
    if(plans.length === 0){ el.innerHTML = '<div class="q-empty">暂无计划</div>'; return; }
    el.innerHTML = plans.map(function(p){
      var dimClass = (p.status==="cancelled") ? " row-dim" : "";
      var btns = (PLAN_NEXT_STATUS[p.status]||[]).map(function(s){
        return '<button onclick="changePlanStatus(\''+esc(p.plan_id)+'\',\''+s+'\')">'+esc(PLAN_STATUS_LABEL[s]||s)+'</button>';
      }).join("");
      // 编辑按钮（非终态）
      var editBtn = PLAN_EDITABLE[p.status] ? '<button onclick="event.stopPropagation();goEditPlan(\''+esc(p.plan_id)+'\')">编辑</button>' : '';
      return '<div class="plan-row'+dimClass+'">' +
        '<div><span class="st st-'+esc(p.status)+'">'+esc(PLAN_STATUS_LABEL[p.status]||p.status)+'</span> ' +
        '<b>'+esc(p.customer_name)+'</b> <span class="muted" style="font-size:11px;">'+esc(BIZ_TYPE_LABEL[p.biz_type]||p.biz_type)+'</span></div>' +
        '<div class="meta">'+esc(p.goods_summary) + (p.expected_arrival_time ? ' · 预计'+esc(p.expected_arrival_time) : '') + '</div>' +
        (p.purpose_text ? '<div class="meta">用途: '+esc(p.purpose_text)+'</div>' : '') +
        (p.remark ? '<div class="meta">备注: '+esc(p.remark)+'</div>' : '') +
        '<div class="status-btns">' + btns + editBtn + '</div>' +
      '</div>';
    }).join("");
  });
}

function changePlanStatus(plan_id, status){
  if(status === "cancelled"){
    if(!confirm("确认作废计划 " + plan_id + "？\n作废后不可恢复。")) return;
  }
  fetchApi({ action:"b2b_plan_update_status", plan_id:plan_id, status:status, updated_by:"" }).then(function(res){
    if(res && res.ok){
      loadHome();
    } else {
      alert("状态更新失败: " + (res&&res.error||"unknown"));
    }
  });
}

function loadWoQuadrant(start, end, listId, countId){
  fetchApi({ action:"b2b_wo_list", start_day:start, end_day:end }).then(function(res){
    var el = document.getElementById(listId);
    var countEl = document.getElementById(countId);
    if(!res || !res.ok){ el.innerHTML = '<div class="q-empty">加载失败</div>'; return; }
    var wos = res.workorders || [];
    wos.sort(function(a,b){ return (WO_STATUS_PRIORITY[a.status]||9) - (WO_STATUS_PRIORITY[b.status]||9); });
    if(countEl) countEl.textContent = wos.length + "条";
    if(wos.length === 0){ el.innerHTML = '<div class="q-empty">暂无作业单</div>'; return; }
    el.innerHTML = wos.map(function(w){
      var dimClass = (w.status==="cancelled") ? " row-dim" : "";
      var opLabel = modeDisplay(w.operation_mode);
      var obLabel = modeDisplay(w.outbound_mode);
      return '<div class="wo-row'+dimClass+'" onclick="goWoDetail(\''+esc(w.workorder_id)+'\')">' +
        '<div><span class="st st-'+esc(w.status)+'">'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</span> ' +
        '<b>'+esc(w.workorder_id)+'</b></div>' +
        '<div class="meta">'+esc(w.customer_name)+' · '+esc(opLabel)+' · '+esc(obLabel) +
        ' · '+w.total_qty+(w.total_qty_unit||"") +
        (w.total_weight_kg ? ' · '+w.total_weight_kg+'kg' : '') + '</div>' +
      '</div>';
    }).join("");
  });
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
  document.getElementById("pc-biz").value = isEdit ? data.biz_type : "b2c_inbound";
  document.getElementById("pc-summary").value = isEdit ? data.goods_summary : "";
  document.getElementById("pc-arrival").value = isEdit ? (data.expected_arrival_time||"") : "";
  document.getElementById("pc-purpose").value = isEdit ? (data.purpose_text||"") : "";
  document.getElementById("pc-remark").value = isEdit ? (data.remark||"") : "";
  document.getElementById("pc-creator").value = "";
  document.getElementById("pc-result").textContent = "";
}

function goEditPlan(plan_id){
  // 从 API 拉最新数据，再进入编辑模式
  fetchApi({ action:"b2b_plan_list", start_day:"2020-01-01", end_day:"2099-12-31" }).then(function(res){
    if(!res || !res.ok){ alert("加载失败"); return; }
    var found = null;
    (res.plans||[]).forEach(function(p){ if(p.plan_id === plan_id) found = p; });
    if(!found){ alert("未找到计划 " + plan_id); return; }
    if(!PLAN_EDITABLE[found.status]){ alert("当前状态不允许编辑"); return; }
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

  if(_editingPlanId){
    // 编辑模式
    fetchApi({
      action:"b2b_plan_update", plan_id:_editingPlanId,
      plan_day:day, customer_name:customer, biz_type:biz,
      goods_summary:summary, expected_arrival_time:arrival,
      purpose_text:purpose, remark:remark
    }).then(function(res){
      if(res && res.ok){
        alert("修改成功！");
        _editingPlanId = null;
        goHome();
      } else {
        document.getElementById("pc-result").innerHTML = '<span class="bad">修改失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  } else {
    // 新建模式
    fetchApi({
      action:"b2b_plan_create", plan_day:day, customer_name:customer, biz_type:biz,
      goods_summary:summary, expected_arrival_time:arrival, purpose_text:purpose,
      remark:remark, created_by:creator
    }).then(function(res){
      if(res && res.ok){
        alert("创建成功！编号: " + res.plan_id);
        _editingPlanId = null;
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

  document.getElementById("wc-title").textContent = isEdit ? "编辑草稿作业单" : "新建出库作业单";
  document.getElementById("wc-id-bar").style.display = isEdit ? "" : "none";
  document.getElementById("wc-id-bar").textContent = isEdit ? ("作业单号: " + data.workorder_id + "（不可修改）") : "";
  document.getElementById("wc-submit-btn").textContent = isEdit ? "保存修改" : "保存（草稿）";
  // 编辑模式隐藏创建人
  document.getElementById("wc-creator-group").style.display = isEdit ? "none" : "";

  document.getElementById("wc-detail-mode").value = isEdit ? (data.detail_mode || "sku_based") : "sku_based";
  document.getElementById("wc-day").value = isEdit ? data.plan_day : kstToday();
  document.getElementById("wc-op-mode").value = isEdit ? (data.operation_mode||"") : "";
  document.getElementById("wc-ob-mode").value = isEdit ? (data.outbound_mode||"") : "";
  document.getElementById("wc-customer").value = isEdit ? data.customer_name : "";
  document.getElementById("wc-customer-kr").value = isEdit ? (data.customer_name_kr||"") : "";
  document.getElementById("wc-ext-no").value = isEdit ? (data.external_workorder_no||"") : "";
  document.getElementById("wc-instr").value = isEdit ? (data.instruction_text||"") : "";
  document.getElementById("wc-remark").value = isEdit ? (data.remark||"") : "";
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
  fetchApi({ action:"b2b_wo_detail", workorder_id:workorder_id }).then(function(res){
    if(!res || !res.ok){ alert("加载失败"); return; }
    var w = res.workorder;
    if(w.status !== "draft"){ alert("只有草稿状态允许编辑"); return; }
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
  var customerKr = document.getElementById("wc-customer-kr").value.trim();
  var extNo = document.getElementById("wc-ext-no").value.trim();
  var instr = document.getElementById("wc-instr").value.trim();
  var remark = document.getElementById("wc-remark").value.trim();
  var creator = document.getElementById("wc-creator").value.trim();
  var lines = wcCollectLines();

  if(!day){ alert("请选择计划出库日"); return; }
  if(!customer){ alert("请输入客户名"); return; }
  if(lines.length === 0){ alert("请至少录入一行明细（数量>0）"); return; }

  if(_editingWoId){
    // 编辑模式
    fetchApi({
      action:"b2b_wo_update", workorder_id:_editingWoId,
      detail_mode:detailMode, operation_mode:opMode, outbound_mode:obMode,
      plan_day:day, customer_name:customer, customer_name_kr:customerKr,
      external_workorder_no:extNo, instruction_text:instr, remark:remark,
      lines:lines
    }).then(function(res){
      if(res && res.ok){
        var id = _editingWoId;
        _editingWoId = null;
        goWoDetail(id);
      } else {
        document.getElementById("wc-result").innerHTML = '<span class="bad">修改失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  } else {
    // 新建模式
    fetchApi({
      action:"b2b_wo_create", detail_mode:detailMode, operation_mode:opMode,
      outbound_mode:obMode, plan_day:day,
      customer_name:customer, customer_name_kr:customerKr,
      external_workorder_no:extNo, instruction_text:instr, remark:remark,
      created_by:creator, lines:lines
    }).then(function(res){
      if(res && res.ok){
        _editingWoId = null;
        goWoDetail(res.workorder_id);
      } else {
        document.getElementById("wc-result").innerHTML = '<span class="bad">创建失败: '+esc(res&&res.error||"unknown")+'</span>';
      }
    });
  }
}

// ===== 作业单列表 =====
function initWoList(){
  document.getElementById("wl-start").value = kstToday();
  document.getElementById("wl-end").value = kstTomorrow();
  loadWoList();
}
function loadWoList(){
  var s = document.getElementById("wl-start").value;
  var e = document.getElementById("wl-end").value;
  if(!s || !e){ alert("请选择日期"); return; }
  fetchApi({ action:"b2b_wo_list", start_day:s, end_day:e }).then(function(res){
    var el = document.getElementById("wl-result");
    if(!res || !res.ok){ el.innerHTML = '<div class="bad">查询失败</div>'; return; }
    var wos = res.workorders || [];
    wos.sort(function(a,b){ return (WO_STATUS_PRIORITY[a.status]||9) - (WO_STATUS_PRIORITY[b.status]||9); });
    if(wos.length === 0){ el.innerHTML = '<div class="muted">暂无作业单</div>'; return; }
    el.innerHTML = wos.map(function(w){
      var dimClass = (w.status==="cancelled") ? " row-dim" : "";
      var opLabel = modeDisplay(w.operation_mode);
      var obLabel = modeDisplay(w.outbound_mode);
      return '<div class="wo-row'+dimClass+'" onclick="goWoDetail(\''+esc(w.workorder_id)+'\')">' +
        '<div><span class="st st-'+esc(w.status)+'">'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</span> ' +
        '<b>'+esc(w.workorder_id)+'</b> · '+esc(w.customer_name)+'</div>' +
        '<div class="meta">'+esc(w.plan_day)+' · '+esc(opLabel)+' · '+esc(obLabel) +
        ' · '+w.total_qty+(w.total_qty_unit||"")+
        (w.total_weight_kg ? ' · '+w.total_weight_kg+'kg' : '') +
        (w.external_workorder_no ? ' · WMS:'+esc(w.external_workorder_no) : '') + '</div>' +
      '</div>';
    }).join("");
  });
}

// ===== 作业单详情 =====
function goWoDetail(id){
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
      draft: ["issued","cancelled"], issued: ["working","cancelled"],
      working: ["completed"], completed: [], cancelled: []
    };
    var statusBtns = (WO_TRANSITIONS[w.status]||[]).map(function(s){
      return '<button onclick="changeWoStatus(\''+esc(w.workorder_id)+'\',\''+s+'\')" class="'+(s==="cancelled"?"bad":"primary")+'" style="width:auto;padding:8px 16px;font-size:13px;">'+esc(WO_STATUS_LABEL[s]||s)+'</button>';
    }).join(" ");

    // 编辑草稿按钮
    var editBtn = (w.status === "draft") ?
      ' <button onclick="goEditWo(\''+esc(w.workorder_id)+'\')" style="width:auto;padding:8px 16px;font-size:13px;">编辑草稿</button>' : '';

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

    card.innerHTML =
      '<div style="font-size:18px;font-weight:800;margin-bottom:10px;">' +
        esc(w.workorder_id) + ' <span class="st st-'+esc(w.status)+'">'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</span>' +
      '</div>' +
      '<div class="detail-field"><b>客户:</b> '+esc(w.customer_name)+(w.customer_name_kr ? ' ('+esc(w.customer_name_kr)+')' : '')+'</div>' +
      '<div class="detail-field"><b>计划出库日:</b> '+esc(w.plan_day)+'</div>' +
      '<div class="detail-field"><b>操作模式:</b> '+esc(opLabel)+'</div>' +
      '<div class="detail-field"><b>出库模式:</b> '+esc(obLabel)+'</div>' +
      '<div class="detail-field"><b>汇总:</b> '+w.total_qty+(w.total_qty_unit||"")+' · '+w.total_weight_kg+'kg' + (w.total_cbm ? ' · '+w.total_cbm+'m³' : '') + '</div>' +
      (w.external_workorder_no ? '<div class="detail-field"><b>WMS工单号:</b> '+esc(w.external_workorder_no)+'</div>' : '') +
      (w.instruction_text ? '<div class="detail-field"><b>作业指示:</b> '+esc(w.instruction_text)+'</div>' : '') +
      (w.remark ? '<div class="detail-field"><b>备注:</b> '+esc(w.remark)+'</div>' : '') +
      '<div class="detail-field muted" style="font-size:12px;"><b>创建人:</b> '+esc(w.created_by)+' · 创建时间: '+new Date(w.created_at).toLocaleString()+'</div>' +

      '<div style="margin:12px 0;" class="no-print">' + statusBtns + editBtn +
        ' <button onclick="printWo(\''+esc(w.workorder_id)+'\')" style="width:auto;padding:8px 16px;font-size:13px;">打印</button>' +
      '</div>' +

      '<div style="font-size:14px;font-weight:700;margin:12px 0 6px;">明细 ('+lines.length+'行)</div>' +
      '<div style="overflow-x:auto;"><table class="line-table"><thead>'+lineHead+'</thead><tbody>'+lineRows+'</tbody>' +
      '<tfoot><tr style="font-weight:700;"><td colspan="'+(isSku?3:2)+'">合计</td><td>'+w.total_qty+'</td>' +
      '<td colspan="3"></td><td>'+w.total_weight_kg+'</td><td></td></tr></tfoot></table></div>';
  });
}

function changeWoStatus(id, status){
  var label = WO_STATUS_LABEL[status] || status;
  if(status === "cancelled"){
    if(!confirm("确认取消作业单 "+id+"？")) return;
  } else {
    if(!confirm("确认将 "+id+" 状态改为「"+label+"」？")) return;
  }
  fetchApi({ action:"b2b_wo_update_status", workorder_id:id, status:status }).then(function(res){
    if(res && res.ok){
      goWoDetail(id);
    } else {
      alert("状态更新失败: "+(res&&res.error||"unknown"));
    }
  });
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

// ===== 打印 =====
function printWo(id){
  fetchApi({ action:"b2b_wo_detail", workorder_id:id }).then(function(res){
    if(!res || !res.ok){ alert("加载失败"); return; }
    var w = res.workorder;
    var lines = res.lines || [];
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
        '<div><span class="label">客户：</span>'+esc(w.customer_name)+(w.customer_name_kr?' ('+esc(w.customer_name_kr)+')':'')+'</div>' +
        '<div><span class="label">计划出库日：</span>'+esc(w.plan_day)+'</div>' +
        (opLabel ? '<div><span class="label">操作模式：</span>'+esc(opLabel)+'</div>' : '') +
        (obLabel ? '<div><span class="label">出库模式：</span>'+esc(obLabel)+'</div>' : '') +
        '<div><span class="label">汇总：</span>'+w.total_qty+(w.total_qty_unit||"")+' · '+w.total_weight_kg+'kg'+(w.total_cbm?' · '+w.total_cbm+'m³':'')+'</div>' +
        (w.external_workorder_no ? '<div><span class="label">WMS工单号：</span>'+esc(w.external_workorder_no)+'</div>' : '') +
        (w.instruction_text ? '<div style="grid-column:1/-1;"><span class="label">作业指示：</span>'+esc(w.instruction_text)+'</div>' : '') +
        (w.remark ? '<div style="grid-column:1/-1;"><span class="label">备注：</span>'+esc(w.remark)+'</div>' : '') +
      '</div>' +

      '<table><thead>'+thead+'</thead><tbody>'+tbody+'</tbody>' +
      '<tfoot><tr><td colspan="'+footColspan+'">合计</td><td>'+w.total_qty+'</td><td colspan="3"></td><td>'+w.total_weight_kg+'</td><td></td></tr></tfoot></table>' +

      '<div class="sig-row">' +
        '<div class="sig-item"><span class="label">制单人：</span>'+esc(w.created_by)+'<div class="sig-line"></div></div>' +
        '<div class="sig-item"><span class="label">仓库确认：</span><div class="sig-line"></div></div>' +
        '<div class="sig-item"><span class="label">客户签收：</span><div class="sig-line"></div></div>' +
        '<div class="sig-item"><span class="label">日期：</span><div class="sig-line"></div></div>' +
      '</div>' +

      '<script>window.onload=function(){window.print();};</script>' +
      '</body></html>';

    var win = window.open("","_blank");
    win.document.write(html);
    win.document.close();
  });
}

// ===== 初始化 =====
(function(){
  var today = kstToday();
  document.getElementById("todayPill").textContent = today;
  if(getKey()){
    fetchApi({ action:"b2b_plan_list", start_day:today, end_day:today }).then(function(res){
      if(res && res.ok) showMain();
    });
  }
})();
