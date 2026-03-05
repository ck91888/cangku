// ===== 配置 =====
var LOCK_URL = "https://ck-warehouse-api.ck91888.workers.dev"; // 你主系统同款 :contentReference[oaicite:4]{index=4}
var KEY_STORAGE = "leader_view_k_v1";

// ===== 小工具 =====
function esc(s){
  return String(s||"").replace(/[&<>"']/g,function(c){
    return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]);
  });
}
function pad2_(n){ n=String(n); return n.length<2 ? ("0"+n) : n; }

// 你原系统用“+9小时偏移”来实现 KST 日期边界 :contentReference[oaicite:5]{index=5}
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
function fmtDur(ms){
  if(!ms || ms<0) return "";
  var sec = Math.floor(ms/1000);
  var h = Math.floor(sec/3600);
  var m = Math.floor((sec%3600)/60);
  if(h>0) return h + "h" + String(m).padStart(2,"0") + "m";
  return m + "m";
}
function setAsofPill_(ms){
  var el = document.getElementById("asofPill");
  if(!el) return;
  try{ el.textContent = new Date(ms).toLocaleString(); }
  catch(e){ el.textContent = String(ms||"--"); }
}

// ===== JSONP（从你 app.js 精简版改出来的） :contentReference[oaicite:6]{index=6} =====
function jsonp(url, params){
  return new Promise(function(resolve, reject){
    var cb = "cb_" + Math.random().toString(16).slice(2);
    var qs = [];
    for(var k in params){
      if(!params.hasOwnProperty(k)) continue;
      qs.push(encodeURIComponent(k) + "=" + encodeURIComponent(params[k]));
    }
    qs.push("callback=" + encodeURIComponent(cb));
    var src = url + "?" + qs.join("&");

    var script = document.createElement("script");
    var timer = setTimeout(function(){
      cleanup();
      reject(new Error("jsonp timeout"));
    }, 12000);

    function cleanup(){
      try{ delete window[cb]; }catch(e){ window[cb]=undefined; }
      if(script && script.parentNode) script.parentNode.removeChild(script);
      clearTimeout(timer);
    }

    window[cb] = function(data){
      cleanup();
      resolve(data);
    };
    script.onerror = function(){
      cleanup();
      reject(new Error("jsonp error"));
    };
    script.src = src;
    document.body.appendChild(script);
  });
}

// ===== 登录/口令 =====
function getKey_(){
  try{ return String(localStorage.getItem(KEY_STORAGE)||""); }catch(e){ return ""; }
}
function setKey_(k){
  try{ localStorage.setItem(KEY_STORAGE, String(k||"").trim()); }catch(e){}
}
function clearKey_(){
  try{ localStorage.removeItem(KEY_STORAGE); }catch(e){}
}

function saveKey(){
  var v = String(document.getElementById("keyInput").value||"").trim();
  if(!v){ alert("请输入口令"); return; }
  setKey_(v);
  alert("已保存 ✅");
  loadToday(); // 保存后直接拉今天
}
function logout(){
  clearKey_();
  document.getElementById("keyInput").value = "";
  alert("已退出");
  document.getElementById("reportMeta").textContent = "未登录：请先输入口令";
  document.getElementById("reportTop").innerHTML = "";
  document.getElementById("reportTable").innerHTML = "";
}

// ===== Tabs =====
function showTab(name){
  document.getElementById("tab-active").style.display = (name==="active") ? "" : "none";
  document.getElementById("tab-report").style.display  = (name==="report") ? "" : "none";
}

// ===== 全局在岗（公开接口 active_now） :contentReference[oaicite:7]{index=7} =====
async function refreshActive(){
  var meta = document.getElementById("activeMeta");
  var box = document.getElementById("activeCards");
  if(meta) meta.textContent = "刷新中…";
  try{
    var res = await jsonp(LOCK_URL, { action:"active_now" });
    if(!res || res.ok !== true){
      if(meta) meta.textContent = "拉取失败：" + (res && res.error ? res.error : "unknown");
      return;
    }
    setAsofPill_(res.asof || Date.now());

    var active = res.active || [];
    // group by biz/task
    var groups = {}; // key -> {biz,task,list:[]}
    active.forEach(function(lk){
      var biz = String(lk.biz||"").trim();
      var task = String(lk.task||"").trim();
      var key = biz + " / " + task;
      if(!groups[key]) groups[key] = { biz:biz, task:task, list:[] };
      groups[key].list.push(lk);
    });

    var keys = Object.keys(groups).sort(function(a,b){
      return (groups[b].list.length||0) - (groups[a].list.length||0);
    });

    if(meta){
      meta.textContent = "在岗人数合计：" + active.length + " ｜ 任务数：" + keys.length;
    }

    if(keys.length===0){
      box.innerHTML = '<div class="muted">当前无在岗</div>';
      return;
    }

    box.innerHTML = keys.map(function(k){
      var g = groups[k];
      var tags = g.list
        .sort(function(x,y){ return String(x.badge||"").localeCompare(String(y.badge||"")); })
        .map(function(lk){
          var age = lk.since ? fmtDur(Date.now() - Number(lk.since||0)) : "";
          var text = String(lk.badge||"");
          if(age) text += " · " + age;
          return '<span class="tag">'+esc(text)+'</span>';
        }).join("");

      return (
        '<div style="border:1px solid #eee;border-radius:16px;padding:12px;margin:10px 0;">' +
          '<div style="display:flex;justify-content:space-between;align-items:flex-end;gap:10px;">' +
            '<div>' +
              '<div style="font-weight:900;font-size:16px;">'+esc(k)+'</div>' +
              '<div class="sub">在岗 '+g.list.length+' 人</div>' +
            '</div>' +
            '<div class="bigNum">'+g.list.length+'</div>' +
          '</div>' +
          '<div style="margin-top:8px;">'+ (tags || '<span class="muted">无</span>') +'</div>' +
        '</div>'
      );
    }).join("");

  }catch(e){
    if(meta) meta.textContent = "异常：" + String(e && e.message ? e.message : e);
  }
}

// ===== 报表（用 admin_events_tail 拉 join/leave，然后在浏览器里算） :contentReference[oaicite:8]{index=8} =====
function setDefaultDates_(){
  var today = kstDayKey_(Date.now());
  var fromEl = document.getElementById("fromDate");
  var toEl = document.getElementById("toDate");
  if(fromEl && !fromEl.value) fromEl.value = today;
  if(toEl && !toEl.value) toEl.value = today;
}

function loadToday(){
  setDefaultDates_();
  loadReport();
}

async function loadReport(){
  var k = getKey_();
  var meta = document.getElementById("reportMeta");
  if(!k){
    meta.textContent = "未登录：请先输入口令（只用于拉取工时报表）";
    return;
  }

  var dayFrom = String(document.getElementById("fromDate").value||"").trim();
  var dayTo = String(document.getElementById("toDate").value||"").trim();
  if(!dayFrom || !dayTo){ alert("请选择开始/结束日期"); return; }
  if(dayFrom > dayTo){ alert("开始日期不能晚于结束日期"); return; }

  var startMs = kstDayStartMs_(dayFrom);
  var endMs = kstDayEndMs_(dayTo);

  meta.textContent = "拉取中…";
  try{
    var res = await jsonp(LOCK_URL, {
      action:"admin_events_tail",
      k:k,
      limit:20000,
      since_ms:String(startMs),
      until_ms:String(endMs)
    });

    if(!res || res.ok !== true){
      meta.textContent = "拉取失败：" + (res && res.error ? res.error : "unknown");
      return;
    }

    setAsofPill_(res.asof || Date.now());

    var header = res.header || [];
    var rows = res.rows || [];

    var out = buildSummary_(header, rows);
    renderReport_(dayFrom, dayTo, rows.length, out);

  }catch(e){
    meta.textContent = "异常：" + String(e && e.message ? e.message : e);
  }
}

function buildSummary_(header, rows){
  var iServer = header.indexOf("server_ms");
  var iEvent  = header.indexOf("event");
  var iBadge  = header.indexOf("badge");
  var iBiz    = header.indexOf("biz");
  var iTask   = header.indexOf("task");
  var iOk     = header.indexOf("ok");

  var active = {}; // badge -> {t,biz,task}
  var acc = {};    // badge -> { total_ms, tasks: {k:ms} }
  var totalsByTask = {}; // "biz/task" -> ms
  var anomalies = { open:0, leave_without_join:0, rejoin_without_leave:0 };

  function addDur(badge, biz, task, dur){
    if(!acc[badge]) acc[badge] = { total_ms:0, tasks:{} };
    acc[badge].total_ms += dur;
    var k = biz + "/" + task;
    totalsByTask[k] = (totalsByTask[k]||0) + dur;
    acc[badge].tasks[k] = (acc[badge].tasks[k]||0) + dur;
  }

  var now = Date.now();
  for(var r=0; r<rows.length; r++){
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
      if(active[badge]){
        // join 前没 leave：自动截断上一段
        var durRejoin = Math.max(0, t - active[badge].t);
        addDur(badge, active[badge].biz, active[badge].task, durRejoin);
        anomalies.rejoin_without_leave++;
      }
      active[badge] = { t:t, biz:biz, task:task };
    }else{
      // leave
      if(!active[badge]){
        anomalies.leave_without_join++;
        continue;
      }
      var dur = Math.max(0, t - active[badge].t);
      addDur(badge, active[badge].biz, active[badge].task, dur);
      delete active[badge];
    }
  }

  // 仍 open 的：截到 now
  Object.keys(active).forEach(function(badge){
    var a = active[badge];
    var durOpen = Math.max(0, now - a.t);
    addDur(badge, a.biz, a.task, durOpen);
    anomalies.open++;
  });

  // 生成 summary rows：badge/biz/task/minutes/total_minutes
  var summary = [];
  var people = [];
  Object.keys(acc).sort().forEach(function(badge){
    var o = acc[badge];
    var totalMin = Math.round(o.total_ms/60000);
    people.push({ badge:badge, total_minutes: totalMin });

    Object.keys(o.tasks).forEach(function(k){
      var parts = k.split("/");
      var biz = parts[0]||"";
      var task = parts[1]||"";
      summary.push({
        badge: badge,
        biz: biz,
        task: task,
        minutes: Math.round(o.tasks[k]/60000),
        total_minutes: totalMin
      });
    });
  });

  // 排序：先 badge，再 task
  summary.sort(function(a,b){
    if(a.badge!==b.badge) return a.badge.localeCompare(b.badge);
    if(a.biz!==b.biz) return a.biz.localeCompare(b.biz);
    return a.task.localeCompare(b.task);
  });

  // totalsByTask -> list
  var taskTotals = Object.keys(totalsByTask).map(function(k){
    return { key:k, minutes: Math.round(totalsByTask[k]/60000) };
  }).sort(function(a,b){ return b.minutes - a.minutes; });

  people.sort(function(a,b){ return b.total_minutes - a.total_minutes; });

  return { anomalies: anomalies, summary: summary, taskTotals: taskTotals, people: people };
}

function renderReport_(dayFrom, dayTo, rowCount, out){
  var meta = document.getElementById("reportMeta");
  var top = document.getElementById("reportTop");
  var table = document.getElementById("reportTable");

  meta.textContent =
    "区间(KST): " + dayFrom + " ~ " + dayTo +
    " ｜ rows=" + rowCount +
    " ｜ open=" + out.anomalies.open +
    " ｜ leave无join=" + out.anomalies.leave_without_join +
    " ｜ 重复join=" + out.anomalies.rejoin_without_leave;

  // 顶部：按任务汇总 + TOP 人员
  var taskHtml = out.taskTotals.slice(0,12).map(function(x){
    return '<span class="tag">'+esc(x.key)+' · '+esc(x.minutes)+'m</span>';
  }).join("");

  var peopleHtml = out.people.slice(0,12).map(function(p){
    return '<span class="tag">'+esc(p.badge)+' · '+esc(p.total_minutes)+'m</span>';
  }).join("");

  top.innerHTML =
    '<div class="listBox"><b>按任务汇总（Top 12）</b><div style="margin-top:6px;">'+(taskHtml||'<span class="muted">无</span>')+'</div></div>' +
    '<div class="listBox"><b>人员总工时（Top 12）</b><div style="margin-top:6px;">'+(peopleHtml||'<span class="muted">无</span>')+'</div></div>';

  // 明细表
  var sum = out.summary || [];
  if(sum.length===0){
    table.innerHTML = '<div class="muted">暂无数据（区间内没有 join/leave）</div>';
    return;
  }

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
  table.innerHTML = html;
}

// ===== 一键刷新 + 自动刷新 =====
async function refreshAll(){
  await refreshActive();
  // 报表不强制刷新（避免频繁拉 20000 rows），用户可点“拉取区间数据”
}

(function init(){
  // 默认日期=今天
  var today = kstDayKey_(Date.now());
  document.getElementById("fromDate").value = today;
  document.getElementById("toDate").value = today;

  // 如果本地已保存 key，填回输入框（可选）
  var k = getKey_();
  if(k) document.getElementById("keyInput").value = k;

  refreshActive();
  // Active now 每30秒自动刷新
  setInterval(refreshActive, 30000);
})();
