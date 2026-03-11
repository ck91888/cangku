const LOCK_TTL_MS = 8 * 60 * 60 * 1000; // 8 hours
const EVENTS_HEADER = [
  "server_ms","client_ms","event_id","event","badge","biz","task","session","wave_id","operator_id","ok","note"
];

// ===== Durable Object: Locks =====
export class LocksDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._locks = null; // badge -> lock
  }

  async _load() {
    if (this._locks) return;
    this._locks = (await this.state.storage.get("locks")) || {};
  }

  _cleanup(now) {
    const locks = this._locks || {};
    let changed = false;
    for (const badge of Object.keys(locks)) {
      const lk = locks[badge];
      if (!lk) { delete locks[badge]; changed = true; continue; }
      if (lk.expires_at && lk.expires_at < now) { delete locks[badge]; changed = true; }
    }
    return changed;
  }

  async _save() {
    await this.state.storage.put("locks", this._locks || {});
  }

  async fetch(request) {
    const url = new URL(request.url);
    const method = request.method.toUpperCase();

    let body = {};
    if (method === "POST") {
      const ct = request.headers.get("content-type") || "";
      if (ct.includes("application/json")) {
        body = await request.json().catch(() => ({}));
      } else {
        const txt = await request.text().catch(() => "");
        body = Object.fromEntries(new URLSearchParams(txt));
      }
    } else {
      body = Object.fromEntries(url.searchParams);
    }

    const action = String(body.action || "").trim();
    const now = Date.now();

    await this._load();
    const changed = this._cleanup(now);
    if (changed) await this._save();

    if (action === "lock_acquire") {
      const badge = String(body.badge || "").trim();
      const biz = String(body.biz || "").trim();
      const task = String(body.task || "").trim();
      const session = String(body.session || "").trim();
      if (!badge) return Response.json({ ok:false, error:"missing badge" });
      if (!task) return Response.json({ ok:false, error:"missing task" });

      const locks = this._locks;
      const cur = locks[badge];
      const expires = now + LOCK_TTL_MS;

      if (!cur || (cur.expires_at && cur.expires_at < now)) {
        locks[badge] = { badge, biz, task, session, since: now, locked_at: now, expires_at: expires };
        await this._save();
        return Response.json({ ok:true, locked:true, lock: locks[badge] });
      }

      if (cur.biz === biz && cur.task === task && cur.session === session) {
        cur.locked_at = now;
        cur.expires_at = expires;
        locks[badge] = cur;
        await this._save();
        return Response.json({ ok:true, locked:true, lock: cur });
      }

      return Response.json({ ok:true, locked:false, reason:"locked_by_other", lock: cur });
    }

    if (action === "lock_release") {
      const badge = String(body.badge || "").trim();
      const task = String(body.task || "").trim();
      const session = String(body.session || "").trim();
      if (!badge) return Response.json({ ok:false, error:"missing badge" });

      const locks = this._locks;
      const cur = locks[badge];
      if (!cur) return Response.json({ ok:true, released:false, reason:"not_found" });

      if (task && cur.task !== task) return Response.json({ ok:true, released:false, reason:"different_task", lock: cur });
      if (session && cur.session !== session) return Response.json({ ok:true, released:false, reason:"different_session", lock: cur });

      delete locks[badge];
      await this._save();
      return Response.json({ ok:true, released:true });
    }

    if (action === "lock_status") {
      const badge = String(body.badge || "").trim();
      if (!badge) return Response.json({ ok:false, error:"missing badge" });

      const cur = (this._locks || {})[badge];
      if (!cur) return Response.json({ ok:true, found:false });

      if (cur.expires_at && cur.expires_at < now) {
        delete this._locks[badge];
        await this._save();
        return Response.json({ ok:true, found:false });
      }

      return Response.json({ ok:true, found:true, lock: cur });
    }

    if (action === "locks_by_session") {
      const session = String(body.session || "").trim();
      if (!session) return Response.json({ ok:false, error:"missing session" });

      const out = [];
      for (const badge of Object.keys(this._locks || {})) {
        const lk = this._locks[badge];
        if (!lk) continue;
        if (lk.expires_at && lk.expires_at < now) continue;
        if (String(lk.session || "").trim() === session) out.push(lk);
      }
      return Response.json({ ok:true, session, asof: now, active: out });
    }

    if (action === "locks_all") {
      const out = [];
      for (const badge of Object.keys(this._locks || {})) {
        const lk = this._locks[badge];
        if (!lk) continue;
        if (lk.expires_at && lk.expires_at < now) continue;
        out.push(lk);
      }
      return Response.json({ ok:true, asof: now, active: out });
    }

    if (action === "lock_force_release") {
      const badge = String(body.badge || "").trim();
      if (!badge) return Response.json({ ok:false, error:"missing badge" });

      const locks = this._locks;
      if (!locks[badge]) return Response.json({ ok:true, released:false, reason:"not_found" });
      delete locks[badge];
      await this._save();
      return Response.json({ ok:true, released:true });
    }

    if (action === "ping") {
      return Response.json({ ok:true, asof: now, pong:true });
    }

    return Response.json({ ok:false, error:"unknown action (DO): " + action });
  }
}

// ===== Worker HTTP API =====
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

function jsonpOrJson(obj, callback) {
  if (callback) {
    return new Response(`${callback}(${JSON.stringify(obj)});`, {
      headers: { "content-type": "application/javascript; charset=utf-8", ...CORS_HEADERS }
    });
  }
  return new Response(JSON.stringify(obj), {
    headers: { "content-type": "application/json; charset=utf-8", ...CORS_HEADERS }
  });
}

async function readBody(request) {
  const method = request.method.toUpperCase();
  if (method === "GET") return {};
  const ct = request.headers.get("content-type") || "";
  if (ct.includes("application/json")) {
    return await request.json().catch(() => ({}));
  }
  const txt = await request.text().catch(() => "");
  return Object.fromEntries(new URLSearchParams(txt));
}

function locksStub(env) {
  const id = env.LOCKS.idFromName("global");
  return env.LOCKS.get(id);
}

async function ensureSessionOpen(env, session, operator_id, biz, task) {
  const sid = String(session || "").trim();
  if (!sid) return;

  const now = Date.now();
  await env.DB.prepare(
    `INSERT INTO sessions(session,status,created_ms,created_by_operator,closed_ms,closed_by_operator,biz,task)
     VALUES(?, 'OPEN', ?, ?, NULL, NULL, ?, ?)
     ON CONFLICT(session) DO UPDATE SET
       biz=CASE WHEN excluded.biz!='' THEN excluded.biz ELSE sessions.biz END,
       task=CASE WHEN excluded.task!='' THEN excluded.task ELSE sessions.task END`
  ).bind(sid, now, String(operator_id||""), String(biz||""), String(task||"")).run();
}

async function getSession(env, session) {
  const sid = String(session || "").trim();
  if (!sid) return null;
  const r = await env.DB.prepare(
    `SELECT session,status,created_ms,created_by_operator,closed_ms,closed_by_operator,biz,task
     FROM sessions WHERE session=? LIMIT 1`
  ).bind(sid).first();
  return r || null;
}

// ===== Task state (enforce "start before join") =====
const REQUIRE_START_TASKS = new Set(["理货","拣货","换单","批量出库","B2B入库理货","B2B工单操作"]);

function requireStart_(task){
  return REQUIRE_START_TASKS.has(String(task||"").trim());
}

async function taskStateStatus_(env, session, biz, task){
  const r = await env.DB.prepare(
    `SELECT status FROM task_state WHERE session=? AND biz=? AND task=? LIMIT 1`
  ).bind(String(session||""), String(biz||""), String(task||"")).first();
  return r ? String(r.status || "").toUpperCase() : "";
}

async function taskStateOpen_(env, session, biz, task, started_ms, operator_id){
  await env.DB.prepare(`
    INSERT INTO task_state(session,biz,task,status,started_ms,ended_ms,started_by_operator,ended_by_operator)
    VALUES(?,?,?,'OPEN',?,NULL,?,NULL)
    ON CONFLICT(session,biz,task) DO UPDATE SET
      status='OPEN',
      started_ms=excluded.started_ms,
      ended_ms=NULL,
      started_by_operator=excluded.started_by_operator,
      ended_by_operator=NULL
  `).bind(
    String(session||""), String(biz||""), String(task||""),
    Number(started_ms||0), String(operator_id||"")
  ).run();
}

async function taskStateClose_(env, session, biz, task, ended_ms, operator_id){
  await env.DB.prepare(`
    UPDATE task_state
    SET status='CLOSED', ended_ms=?, ended_by_operator=?
    WHERE session=? AND biz=? AND task=?
  `).bind(
    Number(ended_ms||0), String(operator_id||""),
    String(session||""), String(biz||""), String(task||"")
  ).run();
}

async function taskStateCloseAll_(env, session, ended_ms, operator_id){
  await env.DB.prepare(
    `UPDATE task_state SET status='CLOSED', ended_ms=?, ended_by_operator=? WHERE session=? AND status='OPEN'`
  ).bind(Number(ended_ms||0), String(operator_id||""), String(session||"")).run();
}

// ===== Admin-only: events_tail =====
function isAdmin_(p, env){
  const key = String(p.k || "").trim();             // 前端传 k=口令
  const secret = String(env.ADMINKEY || "").trim(); // 后端 secret
  return !!(secret && key && key === secret);
}
function isView_(p, env){
  const key = String(p.k || "").trim();            // 前端还是传 k=口令
  const secret = String(env.VIEWKEY || "").trim(); // 新增只读口令
  return !!(secret && key && key === secret);
}
export default {
  async fetch(request, env, ctx) {
    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);
    const q = Object.fromEntries(url.searchParams);
    const body = await readBody(request);
    const p = { ...q, ...body };

    const action = String(p.action || "").trim();
    const callback = String(p.callback || "").trim();
    const now = Date.now();

    // JSONP callback 验证：防止注入
    if (callback && !/^[a-zA-Z_$][a-zA-Z0-9_$]*$/.test(callback)) {
      return new Response('invalid callback', { status: 400 });
    }

    if (action === "ping") {
      return jsonpOrJson({ ok:true, asof: now, pong:true }, callback);
    }

    if (action === "lock_acquire" || action === "lock_release" || action === "lock_status") {
      const stub = locksStub(env);
      const r = await stub.fetch("https://locks/do", {
        method: "POST",
        headers: { "content-type":"application/json" },
        body: JSON.stringify(p)
      });
      const data = await r.json();
      return jsonpOrJson(data, callback);
    }

    if (action === "active_now") {
      const stub = locksStub(env);
      const r = await stub.fetch("https://locks/do", {
        method: "POST",
        headers: { "content-type":"application/json" },
        body: JSON.stringify({ action:"locks_all" })
      });
      const data = await r.json();
      return jsonpOrJson({ ok:true, version:"active_now_from_locks_v1", asof: now, active: data.active || [] }, callback);
    }

    if (action === "session_info") {
      const session = String(p.session || p.pick_session_id || "").trim();
      if (!session) return jsonpOrJson({ ok:false, error:"missing session" }, callback);

      const s = await getSession(env, session);

      const stub = locksStub(env);
      const r = await stub.fetch("https://locks/do", {
        method: "POST",
        headers: { "content-type":"application/json" },
        body: JSON.stringify({ action:"locks_by_session", session })
      });
      const lockInfo = await r.json();

      return jsonpOrJson({
        ok:true,
        asof: now,
        session,
        status: (s && s.status ? String(s.status).toUpperCase() : "OPEN"),
        created_ms: s?.created_ms || 0,
        created_by_operator: s?.created_by_operator || "",
        closed_ms: s?.closed_ms || 0,
        closed_by_operator: s?.closed_by_operator || "",
        biz: s?.biz || "",
        task: s?.task || "",
        active: lockInfo.active || []
      }, callback);
    }

    if (action === "session_close") {
      const session = String(p.session || p.pick_session_id || "").trim();
      const operator_id = String(p.operator_id || "").trim();
      if (!session) return jsonpOrJson({ ok:false, error:"missing session" }, callback);
      if (!operator_id) return jsonpOrJson({ ok:false, error:"missing operator_id" }, callback);

      const s = await getSession(env, session);

      if (s && String(s.status || "").toUpperCase() === "CLOSED") {
        return jsonpOrJson({
          ok:true,
          already_closed:true,
          session,
          closed_ms: s.closed_ms || 0,
          closed_by_operator: s.closed_by_operator || ""
        }, callback);
      }

      const stub = locksStub(env);
      const r = await stub.fetch("https://locks/do", {
        method: "POST",
        headers: { "content-type":"application/json" },
        body: JSON.stringify({ action:"locks_by_session", session })
      });
      const lockInfo = await r.json();
      const active = lockInfo.active || [];
      if (active.length > 0) {
        return jsonpOrJson({ ok:true, blocked:true, reason:"still_active", session, active }, callback);
      }

      const closed_ms = Date.now();
      await env.DB.prepare(
        `UPDATE sessions SET status='CLOSED', closed_ms=?, closed_by_operator=? WHERE session=?`
      ).bind(closed_ms, operator_id, session).run();

      // ✅ 关闭该 session 下所有 task_state
      await taskStateCloseAll_(env, session, closed_ms, operator_id);

      return jsonpOrJson({ ok:true, closed:true, session, closed_ms, closed_by_operator: operator_id }, callback);
    }

    if (action === "events_tail" || action === "admin_events_tail") {
      // events_tail: 需要 admin 权限; admin_events_tail: admin 或 view 权限
      if (action === "events_tail" && !isAdmin_(p, env)) {
        return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      }
      if (action === "admin_events_tail" && !(isAdmin_(p, env) || isView_(p, env))) {
        return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      }

      const limit = Math.min(Math.max(parseInt(p.limit || "5000", 10) || 5000, 1), 20000);
      const sinceMs = parseInt(p.since_ms || "0", 10) || 0;
      const untilMs = parseInt(p.until_ms || "0", 10) || 0;
      const session = String(p.session || "").trim();
      const biz = String(p.biz || "").trim();
      const task = String(p.task || "").trim();

      let where = "WHERE 1=1";
      const binds = [];
      if (sinceMs) { where += " AND server_ms >= ?"; binds.push(sinceMs); }
      if (untilMs) { where += " AND server_ms <= ?"; binds.push(untilMs); }
      if (session) { where += " AND session = ?"; binds.push(session); }
      if (biz) { where += " AND biz = ?"; binds.push(biz); }
      if (task) { where += " AND task = ?"; binds.push(task); }

      const sql = `
        SELECT server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note
        FROM events
        ${where}
        ORDER BY server_ms DESC
        LIMIT ?
      `;
      binds.push(limit);

      const rs = await env.DB.prepare(sql).bind(...binds).all();
      const rowsDesc = rs.results || [];
      const rows = rowsDesc.reverse().map(r => EVENTS_HEADER.map(k => r[k]));
      return jsonpOrJson({ ok:true, asof: now, header: EVENTS_HEADER, rows }, callback);
    }

    if (action === "event_submit") {
      const server_ms = Date.now();

      const event_id = String(p.event_id || "").trim();
      const event = String(p.event || "").trim();
      const badge = String(p.badge || p.da_id || "").trim();
      const biz = String(p.biz || "").trim();
      const task = String(p.task || "").trim();
      const session = String(p.session || p.pick_session_id || "").trim();
      const wave_id = String(p.wave_id || "").trim();
      const operator_id = String(p.operator_id || "").trim();
      const client_ms = Number(p.client_ms || p.ts_ms || 0) || 0;
      const note = String(p.note || "").trim();

      if (!event_id) return jsonpOrJson({ ok:false, error:"missing event_id" }, callback);
      if (!event) return jsonpOrJson({ ok:false, error:"missing event" }, callback);
      if (!biz) return jsonpOrJson({ ok:false, error:"missing biz" }, callback);
      if (!task) return jsonpOrJson({ ok:false, error:"missing task" }, callback);

      // 阻止已关闭 session 继续写入（允许 SESSION/end）
      if (session) {
        const s = await getSession(env, session);
        const closed = s && String(s.status || "").toUpperCase() === "CLOSED";
        const allowEnd = (String(task).trim() === "SESSION" && String(event).trim() === "end");
        const allowLeave = (String(event).trim() === "leave"); // ✅ leave 必须放行，否则锁永远释放不了
        if (closed && !allowEnd && !allowLeave) {
          await env.DB.prepare(
            `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
          ).bind(server_ms, client_ms, event_id, event, badge, biz, task, session, wave_id, operator_id, 0, "session_closed_blocked").run();

          return jsonpOrJson({ ok:false, error:"session_closed", status:"CLOSED", session }, callback);
        }
      }

      // ===== start =====
      if (event === "start" && session) {
        if (!operator_id) return jsonpOrJson({ ok:false, error:"missing operator_id" }, callback);

        // 查这个设备是否已有未结束 session
        // 豁免：拣货/B2B卸货/进口卸货 session 允许与其他 session 同时存在（临时去卸货场景）
        const open = await env.DB.prepare(
          `SELECT session,biz,task FROM sessions
           WHERE status='OPEN' AND created_by_operator=?
           AND NOT (biz='B2C' AND task='拣货')
           AND NOT (biz='B2B' AND task='B2B卸货')
           AND NOT (biz='进口' AND task='卸货')
           ORDER BY created_ms DESC
           LIMIT 1`
        ).bind(operator_id).first();

        // 反向检查：如果当前要 start 的不是卸货/拣货，也要排除已有的卸货/拣货 session
        const isExempt = (biz==='B2C' && task==='拣货') || (biz==='B2B' && task==='B2B卸货') || (biz==='进口' && task==='卸货');

        if (open && String(open.session || "") !== session && !isExempt) {
          return jsonpOrJson({
            ok:false,
            error:"operator_has_open_session",
            open_session: String(open.session),
            open_biz: String(open.biz||""),
            open_task: String(open.task||"")
          }, callback);
        }

        await ensureSessionOpen(env, session, operator_id, biz, task);

        // ✅✅ 关键修复：start 时把该任务标记为 OPEN，join 才不会说"没开始"
        if (task && task !== "SESSION" && requireStart_(task)) {
          await taskStateOpen_(env, session, biz, task, server_ms, operator_id);
        }
      }

      // ===== end =====
      if (event === "end" && session) {
        // session 结束：把所有任务都关掉
        if (task === "SESSION") {
          await taskStateCloseAll_(env, session, server_ms, operator_id);
        } else {
          // 普通任务结束：只关本任务
          await taskStateClose_(env, session, biz, task, server_ms, operator_id);
        }
      }

      // ===== join =====
      if (event === "join") {
        if (!badge) return jsonpOrJson({ ok:false, error:"missing badge for join" }, callback);

        // ✅ 强制：这些任务必须先 start 才能 join
        if (session && task !== "SESSION" && requireStart_(task)) {
          const st = await taskStateStatus_(env, session, biz, task);
          if (st !== "OPEN") {
            await env.DB.prepare(
              `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
            ).bind(server_ms, client_ms, event_id, "join_fail", badge, biz, task, session, wave_id, operator_id, 0, "task_not_started").run();

            return jsonpOrJson({ ok:false, error:"task_not_started", biz, task, session }, callback);
          }
        }

        const stub = locksStub(env);
        const lr = await (await stub.fetch("https://locks/do", {
          method: "POST",
          headers: { "content-type":"application/json" },
          body: JSON.stringify({ action:"lock_acquire", badge, biz, task, session, operator_id })
        })).json();

        if (!lr || lr.ok !== true) {
          await env.DB.prepare(
            `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
          ).bind(server_ms, client_ms, event_id, "join_fail", badge, biz, task, session, wave_id, operator_id, 0, "lock_error").run();

          return jsonpOrJson({ ok:false, error:"lock_error" }, callback);
        }

        if (lr.locked !== true) {
          await env.DB.prepare(
            `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
          ).bind(server_ms, client_ms, event_id, "join_fail", badge, biz, task, session, wave_id, operator_id, 0, "locked_by_other").run();

          return jsonpOrJson({ ok:true, locked:false, reason: lr.reason || "locked_by_other", lock: lr.lock || null }, callback);
        }

        const ins = await env.DB.prepare(
          `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
        ).bind(server_ms, client_ms, event_id, event, badge, biz, task, session, wave_id, operator_id, 1, note).run();

        if (ins.meta && ins.meta.changes === 0) {
          // duplicate event_id: release the lock we just acquired (带条件释放，防止误杀其他session的锁)
          try {
            await stub.fetch("https://locks/do", {
              method: "POST",
              headers: { "content-type":"application/json" },
              body: JSON.stringify({ action:"lock_release", badge, task, session, operator_id })
            });
          } catch(e) { /* best effort */ }
          return jsonpOrJson({ ok:true, duplicate:true }, callback);
        }
        return jsonpOrJson({ ok:true, saved:true, locked:true }, callback);
      }

      // ===== leave =====
      if (event === "leave") {
        if (!badge) return jsonpOrJson({ ok:false, error:"missing badge for leave" }, callback);

        const ins = await env.DB.prepare(
          `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
        ).bind(server_ms, client_ms, event_id, event, badge, biz, task, session, wave_id, operator_id, 1, note).run();

        // ✅ 同步等待锁释放，不再 fire-and-forget
        const stub = locksStub(env);
        let lockReleased = false;
        try {
          const lr = await (await stub.fetch("https://locks/do", {
            method: "POST",
            headers: { "content-type":"application/json" },
            body: JSON.stringify({ action:"lock_release", badge, task, session, operator_id })
          })).json();

          if (lr && lr.released) {
            lockReleased = true;
          } else if (lr && !lr.released && (lr.reason === "different_task" || lr.reason === "different_session")) {
            // Badge已移至其他task/session，旧锁已被覆盖，不应强制释放新锁
            lockReleased = true;
          } else if (lr && !lr.released && lr.reason === "not_found") {
            lockReleased = true; // 锁已经不存在，视为成功
          }
        } catch(e) {
          // 网络异常时 fallback：仍用带条件释放，避免误杀其他session的锁
          try {
            await stub.fetch("https://locks/do", {
              method: "POST",
              headers: { "content-type":"application/json" },
              body: JSON.stringify({ action:"lock_release", badge, task, session, operator_id })
            });
            lockReleased = true;
          } catch(e2) { /* 彻底失败 */ }
        }

        if (ins.meta && ins.meta.changes === 0) return jsonpOrJson({ ok:true, duplicate:true, lock_released: lockReleased }, callback);
        return jsonpOrJson({ ok:true, saved:true, lock_released: lockReleased }, callback);
      }

      // ===== other events =====
      const ins = await env.DB.prepare(
        `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(server_ms, client_ms, event_id, event, badge, biz, task, session, wave_id, operator_id, 1, note).run();

      if (ins.meta && ins.meta.changes === 0) return jsonpOrJson({ ok:true, duplicate:true }, callback);
      return jsonpOrJson({ ok:true, saved:true }, callback);
    }

    if (action === "operator_open_sessions") {
      const operator_id = String(p.operator_id || "").trim();
      if (!operator_id) return jsonpOrJson({ ok:false, error:"missing operator_id" }, callback);
      const rows = await env.DB.prepare(
        `SELECT session, biz, task, created_ms FROM sessions
         WHERE status='OPEN' AND created_by_operator=?
         ORDER BY created_ms DESC LIMIT 10`
      ).bind(operator_id).all();
      return jsonpOrJson({ ok:true, sessions: rows.results || [] }, callback);
    }

    if (action === "admin_force_leave") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const badge = String(p.badge || "").trim();
      if (!badge) return jsonpOrJson({ ok:false, error:"missing badge" }, callback);
      const task = String(p.task || "").trim();
      const session = String(p.session || "").trim();
      const operator_id = String(p.operator_id || "").trim();
      const biz = String(p.biz || "").trim();
      const server_ms = now;
      const event_id = "admin-fl-" + badge + "-" + now;
      await env.DB.prepare(
        `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(server_ms, server_ms, event_id, "leave", badge, biz||"ADMIN", task||"ADMIN", session||"", "", operator_id||"", 1, "admin_force_leave").run();
      const stub = locksStub(env);
      await stub.fetch("https://locks/do", {
        method: "POST",
        headers: { "content-type":"application/json" },
        body: JSON.stringify({ action:"lock_force_release", badge })
      }).catch(() => {});
      return jsonpOrJson({ ok:true, released:true }, callback);
    }

    // ===== 补录修正：管理员手动插入 join/leave 事件（指定自定义时间戳） =====
    if (action === "admin_event_insert") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const badge = String(p.badge || "").trim();
      const biz = String(p.biz || "").trim();
      const task = String(p.task || "").trim();
      const session = String(p.session || "").trim();
      const event = String(p.event || "").trim(); // join or leave
      const custom_ms = Number(p.custom_ms || 0);  // 自定义时间戳(ms)
      const operator_id = String(p.operator_id || "").trim();
      const note = String(p.note || "").trim() || "manual_correction";

      if (!badge) return jsonpOrJson({ ok:false, error:"missing badge" }, callback);
      if (!biz) return jsonpOrJson({ ok:false, error:"missing biz" }, callback);
      if (!task) return jsonpOrJson({ ok:false, error:"missing task" }, callback);
      if (event !== "join" && event !== "leave") return jsonpOrJson({ ok:false, error:"event must be join or leave" }, callback);
      if (!custom_ms || custom_ms < 1000000000000) return jsonpOrJson({ ok:false, error:"invalid custom_ms (need ms timestamp)" }, callback);

      // 确保 session 记录存在（补录时自动生成的 session 可能不在 sessions 表中）
      if (session) {
        await ensureSessionOpen(env, session, operator_id, biz, task);
      }

      const event_id = "manual-" + event + "-" + badge + "-" + custom_ms + "-" + now;
      await env.DB.prepare(
        `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(custom_ms, custom_ms, event_id, event, badge, biz, task, session, "", operator_id, 1, note).run();

      return jsonpOrJson({ ok:true, inserted:true, event_id, event, badge, custom_ms }, callback);
    }

    // ===== 修正：查询某 session 的全部事件 =====
    if (action === "admin_session_events") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const session = String(p.session || "").trim();
      if (!session) return jsonpOrJson({ ok:false, error:"missing session" }, callback);
      const rs = await env.DB.prepare(
        `SELECT server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note
         FROM events WHERE session=? ORDER BY server_ms ASC`
      ).bind(session).all();
      return jsonpOrJson({ ok:true, events: rs.results || [] }, callback);
    }

    // ===== 修正：修改事件字段(时间/单号/备注/业务/任务/工牌) =====
    if (action === "admin_event_update") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const event_id = String(p.event_id || "").trim();
      if (!event_id) return jsonpOrJson({ ok:false, error:"missing event_id" }, callback);
      const existing = await env.DB.prepare(`SELECT * FROM events WHERE event_id=?`).bind(event_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"event_id not found" }, callback);

      const sets = [];
      const binds = [];
      // 可修改的字段
      if (p.new_ms !== undefined && p.new_ms !== null && p.new_ms !== "") {
        const ms = Number(p.new_ms);
        if (ms < 1000000000000) return jsonpOrJson({ ok:false, error:"invalid new_ms" }, callback);
        sets.push("server_ms=?", "client_ms=?"); binds.push(ms, ms);
      }
      if (p.new_wave_id !== undefined) { sets.push("wave_id=?"); binds.push(String(p.new_wave_id).trim()); }
      if (p.new_note !== undefined) { sets.push("note=?"); binds.push(String(p.new_note).trim()); }
      if (p.new_badge !== undefined) { sets.push("badge=?"); binds.push(String(p.new_badge).trim()); }
      if (p.new_biz !== undefined) { sets.push("biz=?"); binds.push(String(p.new_biz).trim()); }
      if (p.new_task !== undefined) { sets.push("task=?"); binds.push(String(p.new_task).trim()); }

      if (sets.length === 0) return jsonpOrJson({ ok:false, error:"no fields to update" }, callback);

      binds.push(event_id);
      await env.DB.prepare(`UPDATE events SET ${sets.join(",")} WHERE event_id=?`).bind(...binds).run();
      return jsonpOrJson({ ok:true, updated:true, event_id, fields: sets.length }, callback);
    }

    // ===== 修正：删除错误事件 =====
    if (action === "admin_event_delete") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const event_id = String(p.event_id || "").trim();
      if (!event_id) return jsonpOrJson({ ok:false, error:"missing event_id" }, callback);
      const existing = await env.DB.prepare(`SELECT event_id,event,badge,biz,task FROM events WHERE event_id=?`).bind(event_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"event_id not found" }, callback);
      await env.DB.prepare(`DELETE FROM events WHERE event_id=?`).bind(event_id).run();
      return jsonpOrJson({ ok:true, deleted:true, event_id, detail: existing }, callback);
    }

    if (action === "admin_sessions_list") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const biz = String(p.biz || "").trim();
      const task = String(p.task || "").trim();
      const since_ms = parseInt(p.since_ms || "0", 10) || 0;
      const until_ms = parseInt(p.until_ms || "0", 10) || 0;
      const limit = Math.min(Math.max(parseInt(p.limit || "200", 10) || 200, 1), 500);

      let where = "WHERE 1=1";
      const binds = [];
      if (biz) { where += " AND biz=?"; binds.push(biz); }
      if (task) { where += " AND task=?"; binds.push(task); }
      if (since_ms) { where += " AND created_ms>=?"; binds.push(since_ms); }
      if (until_ms) { where += " AND created_ms<=?"; binds.push(until_ms); }

      const sessionsSql = `SELECT session,status,biz,task,created_ms,created_by_operator,closed_ms,closed_by_operator FROM sessions ${where} ORDER BY created_ms DESC LIMIT ?`;
      binds.push(limit);
      const sessionRows = (await env.DB.prepare(sessionsSql).bind(...binds).all()).results || [];
      const stub = locksStub(env);
      const allLocksR = await stub.fetch("https://locks/do", {
        method: "POST",
        headers: { "content-type":"application/json" },
        body: JSON.stringify({ action:"locks_all" })
      });
      const allLocksData = await allLocksR.json();
      const allLocks = allLocksData.active || [];
      const locksBySession = {};
      for (const lk of allLocks) {
        const s = String(lk.session || "");
        if (!locksBySession[s]) locksBySession[s] = [];
        locksBySession[s].push(lk);
      }
      const result = [];
      for (const s of sessionRows) {
        const activeLocks = locksBySession[String(s.session)] || [];
        result.push({
          session: s.session,
          status: String(s.status || "OPEN").toUpperCase(),
          biz: s.biz || "",
          task: s.task || "",
          created_ms: s.created_ms || 0,
          created_by_operator: s.created_by_operator || "",
          closed_ms: s.closed_ms || 0,
          closed_by_operator: s.closed_by_operator || "",
          active: activeLocks
        });
      }
      return jsonpOrJson({ ok:true, asof: now, sessions: result }, callback);
    }

    if (action === "admin_force_end_session") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const session = String(p.session || "").trim();
      if (!session) return jsonpOrJson({ ok:false, error:"missing session" }, callback);
      const stub = locksStub(env);
      const locksR = await stub.fetch("https://locks/do", {
        method: "POST",
        headers: { "content-type":"application/json" },
        body: JSON.stringify({ action:"locks_by_session", session })
      });
      const locksData = await locksR.json();
      const activeLocks = locksData.active || [];
      const released = [];
      for (const lk of activeLocks) {
        const badge = String(lk.badge || "").trim();
        if (!badge) continue;
        await stub.fetch("https://locks/do", {
          method: "POST",
          headers: { "content-type":"application/json" },
          body: JSON.stringify({ action:"lock_force_release", badge })
        }).catch(() => {});
        const evId = "admin-force-end-" + badge + "-" + now;
        await env.DB.prepare(
          `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
        ).bind(now, now, evId, "leave", badge, String(lk.biz||""), String(lk.task||""), session, "", String(lk.operator_id||""), 1, "admin_force_end").run();
        released.push(badge);
      }
      await env.DB.prepare(
        `UPDATE sessions SET status='CLOSED', closed_ms=?, closed_by_operator='admin' WHERE session=?`
      ).bind(now, session).run();
      // ✅ 关闭该 session 下所有 task_state
      await taskStateCloseAll_(env, session, now, "admin");
      const endEvId = "admin-force-end-session-" + session + "-" + now;
      await env.DB.prepare(
        `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(now, now, endEvId, "end", "", "ADMIN", "SESSION", session, "", "", 1, "admin_force_end_session").run();
      return jsonpOrJson({ ok:true, released, session }, callback);
    }

    // ===== WMS 数据导入 =====
    if (action === "wms_import") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const source_file = String(p.source_file || "").trim();
      const sheet_name = String(p.sheet_name || "").trim();
      const import_batch_id = String(p.import_batch_id || "").trim();
      const content_fingerprint = String(p.content_fingerprint || "").trim();
      const source_type = String(p.source_type || "").trim();
      const row_offset = parseInt(p.row_offset || "0", 10) || 0;
      const param_business_day = String(p.business_day_kst || "").trim();
      const header = p.header || [];
      const rows = p.rows || [];
      if (!source_file) return jsonpOrJson({ ok:false, error:"missing source_file" }, callback);
      if (!source_type || !["b2c_order_export","b2c_pack_import","import_express"].includes(source_type))
        return jsonpOrJson({ ok:false, error:"invalid source_type" }, callback);
      if (!Array.isArray(rows) || rows.length === 0) return jsonpOrJson({ ok:false, error:"empty rows" }, callback);
      if (source_type === "b2c_pack_import") {
        if (!param_business_day || !/^\d{4}-\d{2}-\d{2}$/.test(param_business_day))
          return jsonpOrJson({ ok:false, error:"b2c_pack_import 必须提供合法的 business_day_kst (YYYY-MM-DD)" }, callback);
      }
      if (param_business_day && !/^\d{4}-\d{2}-\d{2}$/.test(param_business_day))
        return jsonpOrJson({ ok:false, error:"business_day_kst 格式不合法，需要 YYYY-MM-DD" }, callback);

      // 通用 pick：大小写模糊匹配列名
      function pickField(r, keys) {
        for (const k of keys) {
          for (const rk of Object.keys(r)) {
            if (rk.toLowerCase().replace(/[\s_-]/g, "") === k.toLowerCase().replace(/[\s_-]/g, "")) return String(r[rk] || "").trim();
          }
        }
        return "";
      }

      // completed_at → YYYY-MM-DD (KST)
      function toDayKst(raw) {
        if (!raw) return "";
        const s = String(raw).trim();
        const m1 = s.match(/^(\d{4})[\/-](\d{1,2})[\/-](\d{1,2})/);
        if (m1) return m1[1] + "-" + m1[2].padStart(2,"0") + "-" + m1[3].padStart(2,"0");
        // Excel 序列号
        if (/^\d{5}(\.\d+)?$/.test(s)) {
          const serial = parseFloat(s);
          if (serial > 40000 && serial < 60000) {
            const d = new Date((serial - 25569) * 86400000);
            if (!isNaN(d.getTime())) return d.getFullYear() + "-" + String(d.getMonth()+1).padStart(2,"0") + "-" + String(d.getDate()).padStart(2,"0");
          }
        }
        return "";
      }

      // b2c_order_export 需要查 location_types 表
      let locLookup = {};
      if (source_type === "b2c_order_export") {
        const locCodes = new Set();
        for (const r of rows) {
          const lc = String(pickField(r, ["储位号","location_code","location","储位"]) || "").trim().replace(/;+$/,"");
          if (lc) locCodes.add(lc);
        }
        if (locCodes.size > 0) {
          const arr = Array.from(locCodes);
          for (let li = 0; li < arr.length; li += 50) {
            const chunk = arr.slice(li, li + 50);
            const ph = chunk.map(() => "?").join(",");
            const rs = await env.DB.prepare(
              `SELECT location_code, location_type FROM location_types WHERE location_code IN (${ph})`
            ).bind(...chunk).all();
            for (const row of (rs.results || [])) locLookup[row.location_code] = row.location_type;
          }
        }
      }

      let inserted = 0;
      let skipped = 0;
      let s_empty_order = 0, s_zero_qty = 0, s_empty_bizday = 0, s_loc_unknown = 0;
      const batchStmt = env.DB.prepare(
        `INSERT OR IGNORE INTO wms_outputs(import_id,import_batch_id,content_fingerprint,source_file,sheet_name,row_index,
          biz,task,order_no,wave_no,sku,qty,box_count,pallet_count,signed_at,completed_at,raw_json,created_ms,
          source_type,task_scope,pick_wave_no,location_code,location_type,box_code,weight,owner_name,completed_day_kst,business_day_kst)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
      );

      const stmts = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i] || {};
        const import_id = import_batch_id + "|" + (row_offset + i);
        const raw_json = JSON.stringify(r);

        let biz="", task="", task_scope="", order_no="", wave_no="", sku="", qty=0;
        let box_count=0, pallet_count=0, signed_at="", completed_at="";
        let pick_wave_no="", location_code="", location_type="", box_code="";
        let weight=0, owner_name="";

        if (source_type === "b2c_order_export") {
          biz = "B2C"; task_scope = "B2C拣货,B2C打包";
          pick_wave_no = pickField(r, ["波次号","wave_no","pick_wave_no"]);
          order_no = pickField(r, ["物流单号","运单号","order_no","单号"]);
          qty = parseInt(pickField(r, ["商品数量","qty","数量","件数"]), 10) || 0;
          completed_at = pickField(r, ["发货时间","completed_at","完成时间"]);
          location_code = String(pickField(r, ["储位号","location_code","location","储位"]) || "").trim().replace(/;+$/,"");
          box_code = pickField(r, ["箱型编码","box_code","箱型"]);
          sku = pickField(r, ["sku","SKU","品名","商品"]);
          location_type = location_code ? (locLookup[location_code] || "未知") : "";
        } else if (source_type === "b2c_pack_import") {
          biz = "B2C"; task_scope = "B2C拣货,B2C打包";
          pick_wave_no = pickField(r, ["分拣单号","sort_no","pick_wave_no"]);
          order_no = pickField(r, ["物流单号","运单号","order_no","单号"]);
          qty = parseInt(pickField(r, ["货品总数量","qty","数量","件数"]), 10) || 0;
          sku = pickField(r, ["sku","SKU","品名","商品"]);
          owner_name = pickField(r, ["货主","owner","货主名称"]);
          location_type = "小货位";
          // 跳过汇总/脏行
          if (!order_no || order_no === "NA" || owner_name.indexOf("合计") === 0) { skipped++; continue; }
        } else if (source_type === "import_express") {
          biz = "进口"; task_scope = "过机扫描码托,B2C打包:StarFans";
          order_no = pickField(r, ["运单号","order_no","物流单号","单号"]);
          qty = 1;
          weight = parseFloat(pickField(r, ["称重重量","weight","重量"])) || 0;
          owner_name = pickField(r, ["发货单位","owner","shipper","发货人"]);
          completed_at = pickField(r, ["入库时间","completed_at","签收时间","完成时间"]);
        }

        wave_no = pick_wave_no || pickField(r, ["wave_no","waveno","波次号"]);
        const completed_day_kst = toDayKst(completed_at);
        const business_day_kst = param_business_day || completed_day_kst;

        // 校验计数
        if (!order_no) s_empty_order++;
        if (qty === 0) s_zero_qty++;
        if (!business_day_kst) s_empty_bizday++;
        if (location_type === "未知") s_loc_unknown++;

        stmts.push(batchStmt.bind(
          import_id, import_batch_id, content_fingerprint, source_file, sheet_name, i,
          biz, task, order_no, wave_no, sku, qty, box_count, pallet_count, signed_at, completed_at, raw_json, now,
          source_type, task_scope, pick_wave_no, location_code, location_type, box_code, weight, owner_name, completed_day_kst, business_day_kst
        ));
      }


      // D1 batch (最多同时执行)
      const results = await env.DB.batch(stmts);
      for (const r of results) {
        if (r.meta && r.meta.changes > 0) inserted++;
        else skipped++;
      }

      const summary = { s_empty_order, s_zero_qty, s_empty_bizday, s_loc_unknown };
      return jsonpOrJson({ ok:true, inserted, skipped, total: rows.length, import_batch_id, source_type, summary }, callback);
    }

    // ===== WMS 导入记录查询（按批次聚合） =====
    if (action === "wms_list") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const limit = Math.min(Math.max(parseInt(p.limit || "30", 10) || 30, 1), 200);
      const rs = await env.DB.prepare(
        `SELECT import_batch_id, source_file, sheet_name, COUNT(*) as row_count, MAX(created_ms) as created_ms
         FROM wms_outputs GROUP BY import_batch_id ORDER BY created_ms DESC LIMIT ?`
      ).bind(limit).all();
      return jsonpOrJson({ ok:true, batches: rs.results || [] }, callback);
    }

    // ===== WMS 重复检测（文件名规则 + 内容指纹） =====
    if (action === "wms_check_duplicate") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const source_file = String(p.source_file || "").trim();
      const sheet_name = String(p.sheet_name || "").trim();
      const row_count = parseInt(p.row_count || "0", 10) || 0;
      const content_fingerprint = String(p.content_fingerprint || "").trim();
      const source_type = String(p.source_type || "").trim();

      // 1) 内容指纹硬拦截：同 source_type + content_fingerprint → block
      let block = false;
      let block_matches = [];
      if (content_fingerprint && source_type) {
        const rs2 = await env.DB.prepare(
          `SELECT import_batch_id, source_type, source_file, sheet_name, COUNT(*) as row_count, MAX(created_ms) as created_ms
           FROM wms_outputs
           WHERE content_fingerprint=? AND source_type=? AND content_fingerprint!=''
           GROUP BY import_batch_id
           LIMIT 3`
        ).bind(content_fingerprint, source_type).all();
        block_matches = rs2.results || [];
        block = block_matches.length > 0;
      }

      // 2) 文件名软提醒：同 source_file + sheet_name + row_count（全量历史）
      let name_matches = [];
      if (!block) {
        const rs1 = await env.DB.prepare(
          `SELECT import_batch_id, source_type, source_file, sheet_name, COUNT(*) as row_count, MAX(created_ms) as created_ms
           FROM wms_outputs
           WHERE source_file=? AND sheet_name=?
           GROUP BY import_batch_id
           HAVING COUNT(*)=?
           LIMIT 5`
        ).bind(source_file, sheet_name, row_count).all();
        name_matches = rs1.results || [];
      }

      return jsonpOrJson({
        ok: true,
        block,
        block_matches,
        has_name_duplicate: name_matches.length > 0,
        name_matches
      }, callback);
    }

    // ===== 日特征汇总：查询 =====
    if (action === "admin_daily_productivity") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const start_date = String(p.start_date || "").trim();
      const end_date = String(p.end_date || "").trim();
      if (!start_date || !end_date) return jsonpOrJson({ ok:false, error:"missing start_date or end_date" }, callback);
      const rs = await env.DB.prepare(
        `SELECT * FROM daily_productivity_features WHERE day_kst >= ? AND day_kst <= ? ORDER BY day_kst, biz, task`
      ).bind(start_date, end_date).all();
      return jsonpOrJson({ ok:true, features: rs.results || [] }, callback);
    }

    // ===== 刷新前依赖检查 =====
    if (action === "admin_refresh_precheck") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const start_date = String(p.start_date || "").trim();
      const end_date = String(p.end_date || "").trim();
      if (!start_date || !end_date) return jsonpOrJson({ ok:false, error:"missing start_date or end_date" }, callback);

      // 与 admin_refresh_daily 完全一致的口径查 3 个数据源
      // 1) b2c_order_export: completed_day_kst
      const rs1 = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst, COUNT(*) as cnt
         FROM wms_outputs WHERE source_type='b2c_order_export'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();
      // 2) b2c_pack_import: business_day_kst
      const rs2 = await env.DB.prepare(
        `SELECT business_day_kst as day_kst, COUNT(*) as cnt
         FROM wms_outputs WHERE source_type='b2c_pack_import'
           AND business_day_kst >= ? AND business_day_kst <= ? AND business_day_kst != ''
         GROUP BY business_day_kst`
      ).bind(start_date, end_date).all();
      // 3) import_express: completed_day_kst
      const rs3 = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst, COUNT(*) as cnt
         FROM wms_outputs WHERE source_type='import_express'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();

      // 按天汇总
      const dayMap = {}; // day → { b2c_order_export, b2c_pack_import, import_express }
      // 生成日期列表
      const d = new Date(start_date + "T00:00:00+09:00");
      const dEnd = new Date(end_date + "T00:00:00+09:00");
      while (d <= dEnd) {
        const ds = d.toISOString().slice(0, 10);
        dayMap[ds] = { b2c_order_export: 0, b2c_pack_import: 0, import_express: 0 };
        d.setDate(d.getDate() + 1);
      }
      for (const r of (rs1.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].b2c_order_export = r.cnt; }
      for (const r of (rs2.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].b2c_pack_import = r.cnt; }
      for (const r of (rs3.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].import_express = r.cnt; }

      // 生成 gaps
      const gaps = [];
      for (const [day, counts] of Object.entries(dayMap)) {
        if (counts.b2c_order_export === 0) gaps.push({ day, source_type: "b2c_order_export", label: "B2C订单表" });
        if (counts.b2c_pack_import === 0) gaps.push({ day, source_type: "b2c_pack_import", label: "进口打包表" });
        if (counts.import_express === 0) gaps.push({ day, source_type: "import_express", label: "进口快件表" });
      }

      return jsonpOrJson({ ok: true, day_counts: dayMap, gaps }, callback);
    }

    // ===== 日特征汇总：刷新/重建指定日期区间 =====
    if (action === "admin_refresh_daily") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      try {
      const start_date = String(p.start_date || "").trim();
      const end_date = String(p.end_date || "").trim();
      if (!start_date || !end_date) return jsonpOrJson({ ok:false, error:"missing start_date or end_date" }, callback);

      // KST 日期 → UTC 毫秒范围（KST = UTC+9）
      const startMs = new Date(start_date + "T00:00:00+09:00").getTime();
      const endMs = new Date(end_date + "T23:59:59.999+09:00").getTime();
      if (isNaN(startMs) || isNaN(endMs)) return jsonpOrJson({ ok:false, error:"invalid date" }, callback);

      const TASKS = ["B2C拣货", "B2C打包", "过机扫描码托"];

      // Step1: 劳动数据 — join/leave 事件
      const evRs = await env.DB.prepare(
        `SELECT substr(datetime(server_ms/1000,'unixepoch','+9 hours'),1,10) as day_kst,
                biz, task, badge, session, event, server_ms
         FROM events
         WHERE event IN ('join','leave') AND ok=1 AND server_ms >= ? AND server_ms <= ?
         ORDER BY badge, session, server_ms`
      ).bind(startMs, endMs).all();
      const evRows = evRs.results || [];

      // Step2: session_count, event_wave_count, anomaly_count
      const sessRs = await env.DB.prepare(
        `SELECT substr(datetime(server_ms/1000,'unixepoch','+9 hours'),1,10) as day_kst,
                biz, task, COUNT(DISTINCT session) as session_count
         FROM events WHERE event='start' AND ok=1 AND server_ms >= ? AND server_ms <= ?
         GROUP BY day_kst, biz, task`
      ).bind(startMs, endMs).all();

      const waveRs = await env.DB.prepare(
        `SELECT substr(datetime(server_ms/1000,'unixepoch','+9 hours'),1,10) as day_kst,
                biz, task, COUNT(*) as wave_count
         FROM events WHERE event='wave' AND ok=1 AND server_ms >= ? AND server_ms <= ?
         GROUP BY day_kst, biz, task`
      ).bind(startMs, endMs).all();

      const anomRs = await env.DB.prepare(
        `SELECT substr(datetime(server_ms/1000,'unixepoch','+9 hours'),1,10) as day_kst,
                biz, task, COUNT(*) as anomaly_count
         FROM events WHERE event='join_fail' AND server_ms >= ? AND server_ms <= ?
         GROUP BY day_kst, biz, task`
      ).bind(startMs, endMs).all();

      // Step3: WMS 产出 — B2C拣货 (direct: b2c_order_export, allocated: b2c_pack_import)
      const wmsPickDirectRs = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst,
                COUNT(DISTINCT order_no) as order_count, SUM(qty) as qty_sum,
                COUNT(DISTINCT pick_wave_no) as wave_count,
                SUM(box_count) as box_sum, SUM(pallet_count) as pallet_sum
         FROM wms_outputs
         WHERE source_type = 'b2c_order_export'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();
      const wmsPickAllocRs = await env.DB.prepare(
        `SELECT business_day_kst as day_kst,
                COUNT(DISTINCT order_no) as order_count, SUM(qty) as qty_sum,
                COUNT(DISTINCT pick_wave_no) as wave_count,
                SUM(box_count) as box_sum, SUM(pallet_count) as pallet_sum
         FROM wms_outputs
         WHERE source_type = 'b2c_pack_import'
           AND business_day_kst >= ? AND business_day_kst <= ? AND business_day_kst != ''
         GROUP BY business_day_kst`
      ).bind(start_date, end_date).all();

      // Step4: WMS 产出 — B2C打包 (direct: b2c_order_export + import_express StarFans, allocated: b2c_pack_import)
      const wmsPackDirectRs = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst,
                COUNT(DISTINCT order_no) as order_count, SUM(qty) as qty_sum,
                SUM(box_count) as box_sum
         FROM wms_outputs
         WHERE ((source_type = 'b2c_order_export')
                OR (source_type='import_express' AND LOWER(TRIM(owner_name))='starfans'))
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();
      const wmsPackAllocRs = await env.DB.prepare(
        `SELECT business_day_kst as day_kst,
                COUNT(DISTINCT order_no) as order_count, SUM(qty) as qty_sum,
                SUM(box_count) as box_sum
         FROM wms_outputs
         WHERE source_type = 'b2c_pack_import'
           AND business_day_kst >= ? AND business_day_kst <= ? AND business_day_kst != ''
         GROUP BY business_day_kst`
      ).bind(start_date, end_date).all();

      // Step5: WMS 产出 — 过机扫描码托 (import_express 全量)
      const wmsScanRs = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst,
                COUNT(DISTINCT order_no) as wms_order_count,
                SUM(qty) as wms_qty,
                SUM(weight) as wms_weight
         FROM wms_outputs
         WHERE source_type='import_express'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();

      // 工时侧任务名 → WMS 侧任务名映射（仅 B2C 业务）
      function mapTask(biz, task) {
        if (biz === "B2C" && task === "拣货") return "B2C拣货";
        if (biz === "B2C" && task === "打包") return "B2C打包";
        return task;
      }

      // 合并计算
      // key = day_kst|biz|task
      const featureMap = {};
      function getOrCreate(day, biz, task) {
        const k = day + "|" + biz + "|" + task;
        if (!featureMap[k]) {
          featureMap[k] = {
            day_kst: day, biz, task,
            total_person_minutes: 0, unique_workers: 0, session_count: 0,
            event_wave_count: 0, wms_wave_count: 0, wms_order_count: 0,
            wms_qty: 0, wms_box_count: 0, wms_pallet_count: 0, wms_weight: 0,
            wms_order_count_direct: 0, wms_order_count_allocated: 0,
            wms_qty_direct: 0, wms_qty_allocated: 0,
            anomaly_count: 0, correction_count: 0, efficiency_per_person_hour: 0,
            source_summary: "", updated_ms: now
          };
        }
        return featureMap[k];
      }

      // join/leave 配对计算工时
      // 按 badge+session 分组，配对 join→leave
      const laborMap = {}; // badge|session → [{event, server_ms, day_kst, biz, task}]
      for (const e of evRows) {
        const key = e.badge + "|" + e.session;
        if (!laborMap[key]) laborMap[key] = [];
        laborMap[key].push(e);
      }

      // 每个 badge+session 的事件已按 server_ms 排序
      const workersByDayBizTask = {}; // day|biz|task → Set of badges
      for (const [, events] of Object.entries(laborMap)) {
        let pendingJoin = null;
        for (const e of events) {
          if (e.event === "join") {
            pendingJoin = e;
          } else if (e.event === "leave" && pendingJoin) {
            // 配对成功: 归到 join 所在日
            const minutes = (e.server_ms - pendingJoin.server_ms) / 60000;
            if (minutes > 0 && minutes < 1440) { // 最多 24 小时
              const mappedTask = mapTask(pendingJoin.biz, pendingJoin.task);
              const f = getOrCreate(pendingJoin.day_kst, pendingJoin.biz, mappedTask);
              f.total_person_minutes += minutes;
              const wk = pendingJoin.day_kst + "|" + pendingJoin.biz + "|" + mappedTask;
              if (!workersByDayBizTask[wk]) workersByDayBizTask[wk] = new Set();
              workersByDayBizTask[wk].add(pendingJoin.badge);
            }
            pendingJoin = null;
          }
        }
      }

      // unique_workers
      for (const [k, badges] of Object.entries(workersByDayBizTask)) {
        const [day, biz, task] = k.split("|");
        const f = getOrCreate(day, biz, task);
        f.unique_workers = badges.size;
      }

      // session_count
      for (const r of (sessRs.results || [])) {
        const f = getOrCreate(r.day_kst, r.biz, mapTask(r.biz, r.task));
        f.session_count = r.session_count;
      }

      // event_wave_count
      for (const r of (waveRs.results || [])) {
        const f = getOrCreate(r.day_kst, r.biz, mapTask(r.biz, r.task));
        f.event_wave_count = r.wave_count;
      }

      // anomaly_count
      for (const r of (anomRs.results || [])) {
        const f = getOrCreate(r.day_kst, r.biz, mapTask(r.biz, r.task));
        f.anomaly_count = r.anomaly_count;
      }

      // WMS: B2C拣货 — direct
      for (const r of (wmsPickDirectRs.results || [])) {
        const f = getOrCreate(r.day_kst, "B2C", "B2C拣货");
        f.wms_order_count_direct += (r.order_count || 0);
        f.wms_qty_direct += (r.qty_sum || 0);
        f.wms_wave_count += (r.wave_count || 0);
        f.wms_box_count += (r.box_sum || 0);
        f.wms_pallet_count += (r.pallet_sum || 0);
        f.source_summary = "b2c_order_export,b2c_pack_import";
      }
      // WMS: B2C拣货 — allocated
      for (const r of (wmsPickAllocRs.results || [])) {
        const f = getOrCreate(r.day_kst, "B2C", "B2C拣货");
        f.wms_order_count_allocated += (r.order_count || 0);
        f.wms_qty_allocated += (r.qty_sum || 0);
        f.wms_wave_count += (r.wave_count || 0);
        f.wms_box_count += (r.box_sum || 0);
        f.wms_pallet_count += (r.pallet_sum || 0);
        if (!f.source_summary) f.source_summary = "b2c_order_export,b2c_pack_import";
      }

      // WMS: B2C打包 — direct
      for (const r of (wmsPackDirectRs.results || [])) {
        const f = getOrCreate(r.day_kst, "B2C", "B2C打包");
        f.wms_order_count_direct += (r.order_count || 0);
        f.wms_qty_direct += (r.qty_sum || 0);
        f.wms_box_count += (r.box_sum || 0);
        f.source_summary = "b2c_order_export,b2c_pack_import,import_express:StarFans";
      }
      // WMS: B2C打包 — allocated
      for (const r of (wmsPackAllocRs.results || [])) {
        const f = getOrCreate(r.day_kst, "B2C", "B2C打包");
        f.wms_order_count_allocated += (r.order_count || 0);
        f.wms_qty_allocated += (r.qty_sum || 0);
        f.wms_box_count += (r.box_sum || 0);
        if (!f.source_summary) f.source_summary = "b2c_order_export,b2c_pack_import,import_express:StarFans";
      }

      // WMS: 过机扫描码托 (全部为 direct，无 allocated)
      for (const r of (wmsScanRs.results || [])) {
        const f = getOrCreate(r.day_kst, "进口", "过机扫描码托");
        f.wms_order_count_direct += (r.wms_order_count || 0);
        f.wms_qty_direct += (r.wms_qty || 0);
        f.wms_weight += (r.wms_weight || 0);
        f.source_summary = "import_express";
      }

      // 汇总 direct + allocated → total，然后计算效率
      for (const f of Object.values(featureMap)) {
        f.wms_order_count = f.wms_order_count_direct + f.wms_order_count_allocated;
        f.wms_qty = f.wms_qty_direct + f.wms_qty_allocated;
        if (f.total_person_minutes > 0 && f.wms_order_count > 0) {
          f.efficiency_per_person_hour = Math.round((f.wms_order_count / (f.total_person_minutes / 60)) * 100) / 100;
        }
      }

      // 落表: 先删旧数据（日期区间内全部 task），再写入
      await env.DB.prepare(
        `DELETE FROM daily_productivity_features WHERE day_kst >= ? AND day_kst <= ?`
      ).bind(start_date, end_date).run();

      // 写入
      const insStmt = env.DB.prepare(
        `INSERT INTO daily_productivity_features(day_kst,biz,task,total_person_minutes,unique_workers,session_count,
          event_wave_count,wms_wave_count,wms_order_count,wms_qty,wms_box_count,wms_pallet_count,wms_weight,
          wms_order_count_direct,wms_order_count_allocated,wms_qty_direct,wms_qty_allocated,
          anomaly_count,correction_count,efficiency_per_person_hour,source_summary,updated_ms)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
      );
      const insStmts = [];
      const features = Object.values(featureMap);
      for (const f of features) {
        insStmts.push(insStmt.bind(
          f.day_kst, f.biz, f.task, Math.round(f.total_person_minutes * 100)/100, f.unique_workers, f.session_count,
          f.event_wave_count, f.wms_wave_count, f.wms_order_count, f.wms_qty, f.wms_box_count, f.wms_pallet_count,
          Math.round(f.wms_weight * 100)/100,
          f.wms_order_count_direct, f.wms_order_count_allocated, f.wms_qty_direct, f.wms_qty_allocated,
          f.anomaly_count, f.correction_count, f.efficiency_per_person_hour, f.source_summary, now
        ));
      }
      if (insStmts.length > 0) await env.DB.batch(insStmts);

      // post_warnings: 只检查 3 个关键任务
      const post_warnings = [];
      const KEY_TASKS = [
        { biz: "B2C", task: "B2C拣货" },
        { biz: "B2C", task: "B2C打包" },
        { biz: "进口", task: "过机扫描码托" }
      ];
      // 收集该日期范围内 b2c_pack_import 缺失的天
      const packMissingDays = new Set();
      {
        const daysInRange = new Set();
        const dd = new Date(start_date + "T00:00:00+09:00");
        const ddEnd = new Date(end_date + "T00:00:00+09:00");
        while (dd <= ddEnd) { daysInRange.add(dd.toISOString().slice(0, 10)); dd.setDate(dd.getDate() + 1); }
        const packRs = await env.DB.prepare(
          `SELECT DISTINCT business_day_kst FROM wms_outputs
           WHERE source_type='b2c_pack_import' AND business_day_kst >= ? AND business_day_kst <= ? AND business_day_kst != ''`
        ).bind(start_date, end_date).all();
        const packDays = new Set((packRs.results || []).map(r => r.business_day_kst));
        for (const d of daysInRange) { if (!packDays.has(d)) packMissingDays.add(d); }
      }

      for (const f of features) {
        const isKey = KEY_TASKS.some(t => t.biz === f.biz && t.task === f.task);
        if (!isKey) continue;

        // 关键任务产出为 0
        if (f.wms_order_count === 0) {
          post_warnings.push({ day: f.day_kst, task: f.task, level: "warning", msg: f.task + " 产出为 0" });
        }
        // B2C拣货/B2C打包: allocated=0 的谨慎检查（产出已为0时不重复报）
        if ((f.task === "B2C拣货" || f.task === "B2C打包") && f.wms_order_count > 0) {
          if (f.wms_order_count_allocated === 0) {
            // 仅当 direct > 0 或 b2c_pack_import 该天缺失 时报 warning
            if (f.wms_order_count_direct > 0) {
              post_warnings.push({ day: f.day_kst, task: f.task, level: "warning", msg: f.task + " direct=" + f.wms_order_count_direct + " 但 allocated=0" });
            } else if (packMissingDays.has(f.day_kst)) {
              post_warnings.push({ day: f.day_kst, task: f.task, level: "warning", msg: f.task + " allocated=0（该日缺少 b2c_pack_import 数据）" });
            }
          }
        }
        // 无人员出勤
        if (f.unique_workers === 0) {
          post_warnings.push({ day: f.day_kst, task: f.task, level: "info", msg: f.task + " 无人员出勤" });
        }
      }

      return jsonpOrJson({ ok:true, refreshed: features.length, features, post_warnings }, callback);
      } catch (e) {
        return jsonpOrJson({ ok:false, error:"admin_refresh_daily failed: " + String(e && e.message || e) }, callback);
      }
    }

    return jsonpOrJson({ ok:false, error:"unknown action: " + action }, callback);
  }
};
