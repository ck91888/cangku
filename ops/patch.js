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
  async function prettyInboundNo(plan){
    if(!plan||!plan.id||!plan.plan_date) return plan&&plan.id?plan.id:'';
    try{ var map=await buildPrettyMapForDate(plan.plan_date); return map[plan.id]||plan.id; }catch(e){ return plan.id; }
  }
  function unitLabelSafe(key){
    if(typeof unitLabel==='function') return unitLabel(key);
    return key;
  }
  function stripDiffRequired(){
    var diffArea=document.getElementById('unloadDiffArea');
    if(!diffArea) return;
    var lbl=diffArea.querySelector('label');
    if(lbl) lbl.textContent='差异备注（可选） / 차이 메모(선택)';
  }
  function selectedPrettyFromDropdown(){
    var sel=document.getElementById('unloadPlanSelect');
    if(!sel||!sel.options||sel.selectedIndex<0) return '';
    var txt=sel.options[sel.selectedIndex].text||'';
    var m=txt.match(/\[(RU-\d{8}-\d{3})\]/);
    return m?m[1]:'';
  }
  async function renderUnloadPlanCard(planData, prettyNo){
    if(!planData||!planData.plan) return;
    _unloadPlanData=planData;
    var p=planData.plan;
    var lines=planData.lines||[];
    var card=document.getElementById('unloadPlanCard');
    var info=document.getElementById('unloadPlanInfo');
    var area=document.getElementById('unloadPlanLinesArea');
    if(card) card.style.display='';
    if(info){
      info.innerHTML='<div><b>'+esc(prettyNo||p.id)+'</b> | '+esc(p.plan_date)+' | '+esc(p.customer||'')+'</div>'+
        '<div class="muted">'+esc(p.cargo_summary||'')+(p.remark?' — '+esc(p.remark):'')+'</div>';
    }
    if(area){
      if(lines.length>0){
        var tbl='<table class="mini-table"><tr><th>类型/유형</th><th>计划/계획</th></tr>';
        lines.forEach(function(ln){ tbl+='<tr><td>'+unitLabelSafe(ln.unit_type)+'</td><td>'+ln.planned_qty+'</td></tr>'; });
        tbl+='</table>';
        area.innerHTML=tbl;
      } else {
        area.innerHTML='<span class="muted">无明细 / 명세 없음</span>';
      }
    }
  }
  async function previewSelectedPlan(){
    var sel=document.getElementById('unloadPlanSelect');
    var card=document.getElementById('unloadPlanCard');
    if(!sel||!card) return;
    var planId=sel.value||'';
    if(!planId){ card.style.display='none'; return; }
    var res=await api({action:'v2_inbound_plan_detail',id:planId});
    if(res&&res.ok&&res.plan){
      var pretty=selectedPrettyFromDropdown()||await prettyInboundNo(res.plan);
      await renderUnloadPlanCard(res,pretty);
    } else {
      card.style.display='none';
    }
  }

  window.loadInboundPlans = async function(selectId){
    var sel=document.getElementById(selectId);
    if(!sel) return;
    var current=sel.value||'';
    var res=await api({action:'v2_inbound_plan_list',start_date:'',end_date:'',status:''});
    var items=(res&&res.ok&&res.items)?res.items.slice():[];
    var byDate={};
    items.forEach(function(p){ if(p.status==='completed'||p.status==='cancelled') return; var d=p.plan_date||''; if(!byDate[d]) byDate[d]=[]; byDate[d].push(p); });
    var opts='<option value="">-- 选择入库计划/입고계획 선택 --</option>';
    Object.keys(byDate).sort().reverse().forEach(function(d){
      byDate[d].sort(function(a,b){ var ta=String(a.created_at||''); var tb=String(b.created_at||''); if(ta===tb) return String(a.id||'').localeCompare(String(b.id||'')); return ta.localeCompare(tb); });
      byDate[d].forEach(function(p,idx){
        var pretty='RU-'+String(d||'').replace(/-/g,'')+'-'+pad3(idx+1);
        opts+='<option value="'+esc(p.id)+'">['+pretty+'] '+esc(p.customer||'')+' - '+esc(p.cargo_summary||'')+'</option>';
      });
    });
    sel.innerHTML=opts;
    if(current) sel.value=current;
    if(selectId==='unloadPlanSelect') previewSelectedPlan();
  };

  var _origShowUnloadWorking=window.showUnloadWorking;
  window.showUnloadWorking=function(job){
    _origShowUnloadWorking(job);
    stripDiffRequired();
    if(_unloadPlanData&&_unloadPlanData.plan){
      var fallback=selectedPrettyFromDropdown();
      prettyInboundNo(_unloadPlanData.plan).then(function(pretty){ renderUnloadPlanCard(_unloadPlanData, pretty||fallback||_unloadPlanData.plan.id); });
    }
  };

  var _origInitUnload=window.initUnload;
  window.initUnload=async function(){
    await _origInitUnload();
    stripDiffRequired();
    var sel=document.getElementById('unloadPlanSelect');
    if(sel&&!sel._oaiBound){ sel.addEventListener('change', previewSelectedPlan); sel._oaiBound=true; }
    if(!_activeJobId) previewSelectedPlan();
  };

  window.unloadComplete = async function(){
    if(!_activeJobId) return;
    var resultLines=getUnloadResultLines();
    if(resultLines.length===0){ alert('请至少填写一项实际数量 / 실제 수량을 최소 1건 입력하세요'); return; }
    var planLines=(_unloadPlanData&&_unloadPlanData.lines)||[];
    var diffNote=((document.getElementById('unloadDiffNote')||{}).value||'').trim();
    if(planLines.length>0){
      var hasDiff=false; var actualMap={};
      resultLines.forEach(function(r){ actualMap[r.unit_type]=r.actual_qty; });
      planLines.forEach(function(ln){ if((actualMap[ln.unit_type]||0)!==(ln.planned_qty||0)) hasDiff=true; });
      if(hasDiff&&!diffNote) diffNote='现场实收数量与计划数量不一致';
    }
    var remark=((document.getElementById('unloadRemark')||{}).value||'').trim();
    var res=await api({action:'v2_unload_job_finish',job_id:_activeJobId,worker_id:getWorkerId(),result_lines:resultLines,diff_note:diffNote,remark:remark,complete_job:true});
    if(res&&res.ok){ var msg='卸货已完成 / 하차 완료'; if(res.no_doc) msg+='\n（无单卸货已自动生成反馈 / 서류 없는 하차 피드백 자동 생성됨）'; alert(msg); clearActiveJob(); _unloadPlanData=null; goPage('home'); }
    else if(res&&res.error==='others_still_working'){ alert('还有'+res.active_count+'人参与中，无法完成 / 아직 '+res.active_count+'명 참여 중, 완료 불가'); }
    else if(res&&res.error==='empty_result'){ alert(res.message||'至少填写一项实际数量'); }
    else if(res&&res.error==='diff_note_required'){ alert('系统仍要求差异备注，已记录默认说明后请重试 / 시스템이 차이 메모를 요구합니다'); }
    else { alert('失败/실패: '+(res?res.error:'unknown')); }
  };

  document.addEventListener('DOMContentLoaded', function(){
    stripDiffRequired();
    var sel=document.getElementById('unloadPlanSelect');
    if(sel&&!sel._oaiBound){ sel.addEventListener('change', previewSelectedPlan); sel._oaiBound=true; }
  });
})();