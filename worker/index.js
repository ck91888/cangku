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
    `INSERT OR IGNORE INTO sessions(session,status,created_ms,created_by_operator,closed_ms,closed_by_operator,biz,task)
     VALUES(?, 'OPEN', ?, ?, NULL, NULL, ?, ?)`
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

        if (ins.meta && ins.meta.changes === 0) return jsonpOrJson({ ok:true, duplicate:true }, callback);
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
          // 网络异常时 fallback：尝试强制释放
          try {
            await stub.fetch("https://locks/do", {
              method: "POST",
              headers: { "content-type":"application/json" },
              body: JSON.stringify({ action:"lock_force_release", badge })
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

    if (action === "admin_sessions_list") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const biz = String(p.biz || "").trim();
      let sessionsSql = biz
        ? `SELECT session,status,biz,task,created_ms,created_by_operator,closed_ms,closed_by_operator FROM sessions WHERE biz=? ORDER BY created_ms DESC LIMIT 50`
        : `SELECT session,status,biz,task,created_ms,created_by_operator,closed_ms,closed_by_operator FROM sessions ORDER BY created_ms DESC LIMIT 50`;
      const sessionRows = biz
        ? (await env.DB.prepare(sessionsSql).bind(biz).all()).results || []
        : (await env.DB.prepare(sessionsSql).all()).results || [];
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

    return jsonpOrJson({ ok:false, error:"unknown action: " + action }, callback);
  }
};
