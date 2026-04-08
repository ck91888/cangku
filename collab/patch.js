(function(){
  function pad3(n){ n=Number(n)||0; return String(n).padStart(3,'0'); }
  async function buildPrettyMapForDate(planDate){
    var res=await api({action:'v2_inbound_plan_list',start_date:planDate,end_date:planDate,status:''});
    var items=(res&&res.ok&&res.items)?res.items.slice():[];
    items.sort(function(a,b){ var ta=String(a.created_at||''); var tb=String(b.created_at||''); if(ta===tb) return String(a.id||'').localeCompare(String(b.id||'')); return ta.localeCompare(tb); });
    var map={};
    items.forEach(function(it,idx){ map[it.id]='RU-'+String(planDate||'').replace(/-/g,'')+'-'+pad3(idx+1); });
    return map;
  }
  async function prettyInboundNo(plan){ if(!plan||!plan.id||!plan.plan_date) return plan&&plan.id?plan.id:''; var map=await buildPrettyMapForDate(plan.plan_date); return map[plan.id]||plan.id; }
  function unitLabelSafe(key){ if(typeof unitTypeLabel==='function') return unitTypeLabel(key); return key; }
  function collectInboundLinesStrict(){ var lines=typeof getIbcLines==='function'?getIbcLines():[]; return (lines||[]).filter(function(ln){ return ln&&Number(ln.planned_qty||0)>0; }); }
  window.submitInbound = async function(){
    var date=document.getElementById('ibc-date').value||kstToday();
    var customer=document.getElementById('ibc-customer').value.trim();
    var biz=document.getElementById('ibc-biz').value;
    var cargo=document.getElementById('ibc-cargo').value.trim();
    var arrival=document.getElementById('ibc-arrival').value.trim();
    var purpose=document.getElementById('ibc-purpose').value.trim();
    var remark=document.getElementById('ibc-remark').value.trim();
    var lines=collectInboundLinesStrict();
    var autoOb=document.getElementById('ibc-auto-ob').checked;
    if(!customer){ alert(L('customer')+' '+L('required')+'!'); return; }
    if(lines.length===0){ alert(L('plan_lines')+' '+L('required')+'!'); return; }
    if(!cargo){ cargo=lines.map(function(ln){ return unitLabelSafe(ln.unit_type)+' '+ln.planned_qty; }).join(' / '); }
    var payload={ action:'v2_inbound_plan_create', plan_date:date, customer:customer, biz_class:biz, cargo_summary:cargo, expected_arrival:arrival, purpose:purpose, remark:remark, lines:lines, created_by:getUser() };
    if(autoOb){ payload.auto_create_outbound=true; payload.ob_operation_mode=(document.getElementById('ibc-ob-opmode')||{}).value||''; payload.ob_outbound_mode=(document.getElementById('ibc-ob-outmode')||{}).value||''; payload.ob_instruction=(document.getElementById('ibc-ob-instruction')||{}).value||''; }
    var res=await api(payload);
    if(res&&res.ok){ var pretty=await prettyInboundNo({id:res.id,plan_date:date}); var msg=L('success')+': '+pretty; if(res.outbound_id) msg+='\n'+L('auto_create_outbound')+': '+res.outbound_id; alert(msg); document.getElementById('ibc-customer').value=''; document.getElementById('ibc-cargo').value=''; document.getElementById('ibc-arrival').value=''; document.getElementById('ibc-purpose').value=''; document.getElementById('ibc-remark').value=''; document.getElementById('ibcLinesBody').innerHTML=''; document.getElementById('ibc-auto-ob').checked=false; document.getElementById('ibcAutoObFields').style.display='none'; if(typeof addIbcLine==='function') addIbcLine(); goTab('inbound'); }
    else { alert(L('error')+': '+(res?res.error:'unknown')); }
  };
  window.loadInboundList = async function(){
    var body=document.getElementById('inboundListBody'); if(!body) return; body.innerHTML='<div class="card muted">'+L('loading')+'</div>';
    var start=document.getElementById('ibFilterStart').value; var end=document.getElementById('ibFilterEnd').value; var status=document.getElementById('ibFilterStatus').value;
    var res=await api({action:'v2_inbound_plan_list',start_date:start,end_date:end,status:status});
    if(!res||!res.ok){ body.innerHTML='<div class="card muted">'+L('error')+'</div>'; return; }
    var items=res.items||[]; if(items.length===0){ body.innerHTML='<div class="card muted">'+L('no_data')+'</div>'; return; }
    var byDate={}; items.forEach(function(p){ var d=p.plan_date||''; if(!byDate[d]) byDate[d]=[]; byDate[d].push(p); });
    Object.keys(byDate).forEach(function(d){ byDate[d].sort(function(a,b){ var ta=String(a.created_at||''); var tb=String(b.created_at||''); if(ta===tb) return String(a.id||'').localeCompare(String(b.id||'')); return ta.localeCompare(tb); }); byDate[d].forEach(function(p,idx){ p._pretty_no='RU-'+String(d||'').replace(/-/g,'')+'-'+pad3(idx+1); }); });
    items.sort(function(a,b){ var da=String(a.plan_date||''); var db=String(b.plan_date||''); if(da!==db) return db.localeCompare(da); return String(b.created_at||'').localeCompare(String(a.created_at||'')); });
    var html='<div class="card">';
    items.forEach(function(p){ html+='<div class="list-item" onclick="openInboundDetail(\''+esc(p.id)+'\')"><div class="item-title"><span class="st st-'+esc(p.status)+'">'+esc(stLabel(p.status))+'</span> <span class="biz-tag biz-'+esc(p.biz_class)+'">'+esc(bizLabel(p.biz_class))+'</span> '+esc(p._pretty_no||p.id)+' · '+esc(p.customer||'--')+'</div><div class="item-meta">'+esc(p.plan_date||'')+' · '+esc(p.cargo_summary||'')+' · '+esc(fmtTime(p.created_at))+'</div></div>'; });
    html+='</div>'; body.innerHTML=html;
  };
  var _origLoadInboundDetail=window.loadInboundDetail;
  window.loadInboundDetail=async function(){ await _origLoadInboundDetail(); if(!_currentInboundId) return; var res=await api({action:'v2_inbound_plan_detail',id:_currentInboundId}); if(!res||!res.ok||!res.plan) return; var pretty=await prettyInboundNo(res.plan); window._currentInboundPretty=pretty; window._currentInboundPlanCache=res.plan; var body=document.getElementById('inboundDetailBody'); if(!body) return; var titleEl=body.querySelector('.card div[style*="font-size:16px"]'); if(titleEl) titleEl.textContent=pretty; };
  window.printIbQr=function(){ var body=document.getElementById('inboundDetailBody'); if(!body) return; var title=window._currentInboundPretty||_currentInboundId||''; var qrCanvas=document.querySelector('#ibDetailQr canvas'); var qrImg=qrCanvas?qrCanvas.toDataURL('image/png'):''; var tables=body.querySelectorAll('table.line-table'); var linesHtml=tables.length?tables[0].outerHTML:''; var firstCard=body.querySelector('.card'); var summaryHtml=firstCard?firstCard.innerHTML:''; var win=window.open('','_blank'); var html='<html><head><title>'+esc(title)+'</title><style>body{font-family:Arial,Helvetica,sans-serif;padding:24px;color:#111;}h1{font-size:28px;margin:0 0 8px;text-align:center;}.meta{margin:12px 0 18px;font-size:14px;line-height:1.6;} .qr{text-align:center;margin:16px 0;} table{width:100%;border-collapse:collapse;margin-top:12px;font-size:14px;} th,td{border:1px solid #bbb;padding:8px;text-align:left;} .small{font-size:12px;color:#666;text-align:center;margin-top:8px;}</style></head><body><h1>'+esc(title)+'</h1>'+(qrImg?'<div class="qr"><img src="'+qrImg+'" style="width:180px;height:180px;"></div>':'')+'<div class="meta">'+summaryHtml+'</div>'+linesHtml+'<div class="small">Printed from CK Warehouse V2</div><script>window.print();<\/script></body></html>'; win.document.write(html); win.document.close(); };
  function ensureOneLine(){ var tbody=document.getElementById('ibcLinesBody'); if(tbody&&!tbody.children.length&&typeof addIbcLine==='function') addIbcLine(); }
  document.addEventListener('DOMContentLoaded',function(){ ensureOneLine(); var btn=document.getElementById('btnNewInbound'); if(btn){ btn.addEventListener('click',function(){ setTimeout(ensureOneLine,30); }); } });
})();