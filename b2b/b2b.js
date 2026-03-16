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

// ===== 登录 =====
function doLogin(){
  var k = document.getElementById("loginKey").value.trim();
  if(!k){ document.getElementById("loginErr").textContent = "请输入口令"; return; }
  setKey(k);
  document.getElementById("loginErr").textContent = "";
  // 尝试调用一个接口验证口令
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
  if(name === "wo_create") initWoCreate();
  if(name === "wo_list") initWoList();
}
function goHome(){
  goView("home");
  loadHome();
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

var PLAN_STATUS_LABEL = {
  pending: "未到货", arrived: "已到货", processing: "操作中",
  completed: "已完成", abnormal: "异常", cancelled: "已作废"
};
var BIZ_TYPE_LABEL = {
  b2c_inbound: "B2C入库", b2b_inbound: "B2B入库", direct_transfer: "直接转发", other: "其他"
};
var PLAN_NEXT_STATUS = {
  pending: ["arrived","cancelled"],
  arrived: ["processing","abnormal","cancelled"],
  processing: ["completed","abnormal"],
  completed: [],
  abnormal: ["processing","cancelled"],
  cancelled: []
};

function loadPlansQuadrant(start, end, listId, countId){
  fetchApi({ action:"b2b_plan_list", start_day:start, end_day:end }).then(function(res){
    var el = document.getElementById(listId);
    var countEl = document.getElementById(countId);
    if(!res || !res.ok){ el.innerHTML = '<div class="q-empty">加载失败</div>'; return; }
    var plans = res.plans || [];
    if(countEl) countEl.textContent = plans.length + "条";
    if(plans.length === 0){ el.innerHTML = '<div class="q-empty">暂无计划</div>'; return; }
    el.innerHTML = plans.map(function(p){
      var btns = (PLAN_NEXT_STATUS[p.status]||[]).map(function(s){
        return '<button onclick="changePlanStatus(\''+esc(p.plan_id)+'\',\''+s+'\')">'+esc(PLAN_STATUS_LABEL[s]||s)+'</button>';
      }).join("");
      return '<div class="plan-row">' +
        '<div><span class="st st-'+esc(p.status)+'">'+esc(PLAN_STATUS_LABEL[p.status]||p.status)+'</span> ' +
        '<b>'+esc(p.customer_name)+'</b> <span class="muted" style="font-size:11px;">'+esc(BIZ_TYPE_LABEL[p.biz_type]||p.biz_type)+'</span></div>' +
        '<div class="meta">'+esc(p.goods_summary) + (p.expected_arrival_time ? ' · 预计'+esc(p.expected_arrival_time) : '') + '</div>' +
        (p.purpose_text ? '<div class="meta">用途: '+esc(p.purpose_text)+'</div>' : '') +
        (p.remark ? '<div class="meta">备注: '+esc(p.remark)+'</div>' : '') +
        (btns ? '<div class="status-btns">'+btns+'</div>' : '') +
      '</div>';
    }).join("");
  });
}

function changePlanStatus(plan_id, status){
  var label = PLAN_STATUS_LABEL[status] || status;
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

var WO_STATUS_LABEL = {
  draft: "草稿", issued: "已下发", working: "作业中", completed: "已完成", cancelled: "已取消"
};
var MODE_LABEL = { sku_based: "SKU模式", carton_based: "箱模式" };

function loadWoQuadrant(start, end, listId, countId){
  fetchApi({ action:"b2b_wo_list", start_day:start, end_day:end }).then(function(res){
    var el = document.getElementById(listId);
    var countEl = document.getElementById(countId);
    if(!res || !res.ok){ el.innerHTML = '<div class="q-empty">加载失败</div>'; return; }
    var wos = res.workorders || [];
    if(countEl) countEl.textContent = wos.length + "条";
    if(wos.length === 0){ el.innerHTML = '<div class="q-empty">暂无作业单</div>'; return; }
    el.innerHTML = wos.map(function(w){
      return '<div class="wo-row" onclick="goWoDetail(\''+esc(w.workorder_id)+'\')">' +
        '<div><span class="st st-'+esc(w.status)+'">'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</span> ' +
        '<b>'+esc(w.workorder_id)+'</b></div>' +
        '<div class="meta">'+esc(w.customer_name)+' · '+esc(MODE_LABEL[w.outbound_mode]||w.outbound_mode) +
        ' · '+w.total_qty+(w.total_qty_unit||"") +
        (w.total_weight_kg ? ' · '+w.total_weight_kg+'kg' : '') + '</div>' +
      '</div>';
    }).join("");
  });
}

// ===== 新建入库计划 =====
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

  fetchApi({
    action:"b2b_plan_create", plan_day:day, customer_name:customer, biz_type:biz,
    goods_summary:summary, expected_arrival_time:arrival, purpose_text:purpose,
    remark:remark, created_by:creator
  }).then(function(res){
    var el = document.getElementById("pc-result");
    if(res && res.ok){
      el.innerHTML = '<span class="ok">创建成功！编号: <b>'+esc(res.plan_id)+'</b></span>';
      // 清空表单
      document.getElementById("pc-customer").value = "";
      document.getElementById("pc-summary").value = "";
      document.getElementById("pc-arrival").value = "";
      document.getElementById("pc-purpose").value = "";
      document.getElementById("pc-remark").value = "";
    } else {
      el.innerHTML = '<span class="bad">创建失败: '+esc(res&&res.error||"unknown")+'</span>';
    }
  });
}

// ===== 新建出库作业单 =====
var _wcLineCount = 0;
function initWoCreate(){
  _wcLineCount = 0;
  document.getElementById("wc-day").value = kstToday();
  document.getElementById("wc-customer").value = "";
  document.getElementById("wc-customer-kr").value = "";
  document.getElementById("wc-ext-no").value = "";
  document.getElementById("wc-instr").value = "";
  document.getElementById("wc-remark").value = "";
  document.getElementById("wc-creator").value = "";
  document.getElementById("wc-result").textContent = "";
  document.getElementById("wc-summary").textContent = "";
  wcModeChanged();
}
function wcModeChanged(){
  var mode = document.getElementById("wc-mode").value;
  var head = document.getElementById("wc-lines-head");
  var body = document.getElementById("wc-lines-body");
  _wcLineCount = 0;
  if(mode === "carton_based"){
    head.innerHTML = '<tr><th>#</th><th>箱号</th><th>数量</th><th>长cm</th><th>宽cm</th><th>高cm</th><th>重量kg</th><th>备注</th><th></th></tr>';
  } else {
    head.innerHTML = '<tr><th>#</th><th>产品编码</th><th>产品名称</th><th>数量</th><th>长cm</th><th>宽cm</th><th>高cm</th><th>重量kg</th><th>备注</th><th></th></tr>';
  }
  body.innerHTML = "";
  wcAddLine();
}
function wcAddLine(){
  _wcLineCount++;
  var n = _wcLineCount;
  var mode = document.getElementById("wc-mode").value;
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
function submitWo(){
  var mode = document.getElementById("wc-mode").value;
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

  fetchApi({
    action:"b2b_wo_create", outbound_mode:mode, plan_day:day,
    customer_name:customer, customer_name_kr:customerKr,
    external_workorder_no:extNo, instruction_text:instr, remark:remark,
    created_by:creator, lines:lines
  }).then(function(res){
    var el = document.getElementById("wc-result");
    if(res && res.ok){
      el.innerHTML = '<span class="ok">创建成功！作业单号: <b>'+esc(res.workorder_id)+'</b>' +
        '  明细'+res.lines_count+'行 · 总数量'+res.total_qty+' · 总重量'+res.total_weight_kg+'kg</span>';
    } else {
      el.innerHTML = '<span class="bad">创建失败: '+esc(res&&res.error||"unknown")+'</span>';
    }
  });
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
    if(wos.length === 0){ el.innerHTML = '<div class="muted">暂无作业单</div>'; return; }
    el.innerHTML = wos.map(function(w){
      return '<div class="wo-row" onclick="goWoDetail(\''+esc(w.workorder_id)+'\')">' +
        '<div><span class="st st-'+esc(w.status)+'">'+esc(WO_STATUS_LABEL[w.status]||w.status)+'</span> ' +
        '<b>'+esc(w.workorder_id)+'</b> · '+esc(w.customer_name)+'</div>' +
        '<div class="meta">'+esc(w.plan_day)+' · '+esc(MODE_LABEL[w.outbound_mode]||w.outbound_mode) +
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
    var isSku = w.outbound_mode === "sku_based";

    // 状态流转按钮
    var WO_TRANSITIONS = {
      draft: ["issued","cancelled"], issued: ["working","cancelled"],
      working: ["completed"], completed: [], cancelled: []
    };
    var statusBtns = (WO_TRANSITIONS[w.status]||[]).map(function(s){
      return '<button onclick="changeWoStatus(\''+esc(w.workorder_id)+'\',\''+s+'\')" class="'+(s==="cancelled"?"bad":"primary")+'" style="width:auto;padding:8px 16px;font-size:13px;">'+esc(WO_STATUS_LABEL[s]||s)+'</button>';
    }).join(" ");

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
      '<div class="detail-field"><b>出库模式:</b> '+esc(MODE_LABEL[w.outbound_mode]||w.outbound_mode)+'</div>' +
      '<div class="detail-field"><b>汇总:</b> '+w.total_qty+(w.total_qty_unit||"")+' · '+w.total_weight_kg+'kg' + (w.total_cbm ? ' · '+w.total_cbm+'m³' : '') + '</div>' +
      (w.external_workorder_no ? '<div class="detail-field"><b>WMS工单号:</b> '+esc(w.external_workorder_no)+'</div>' : '') +
      (w.instruction_text ? '<div class="detail-field"><b>作业指示:</b> '+esc(w.instruction_text)+'</div>' : '') +
      (w.remark ? '<div class="detail-field"><b>备注:</b> '+esc(w.remark)+'</div>' : '') +
      '<div class="detail-field muted" style="font-size:12px;"><b>创建人:</b> '+esc(w.created_by)+' · 创建时间: '+new Date(w.created_at).toLocaleString()+'</div>' +

      '<div style="margin:12px 0;" class="no-print">' + statusBtns +
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
      goWoDetail(id); // 刷新详情
    } else {
      alert("状态更新失败: "+(res&&res.error||"unknown"));
    }
  });
}

// ===== 打印 =====
function printWo(id){
  // 详情已在页面上，直接触发打印
  window.print();
}

// ===== 初始化 =====
(function(){
  var today = kstToday();
  document.getElementById("todayPill").textContent = today;
  document.getElementById("pc-day").value = today;
  // 检查是否已有口令
  if(getKey()){
    fetchApi({ action:"b2b_plan_list", start_day:today, end_day:today }).then(function(res){
      if(res && res.ok) showMain();
    });
  }
})();
