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
  // multipart/form-data — 用于附件上传，返回特殊标记让调用方用 request 原始 formData
  if (ct.includes("multipart/form-data")) {
    return { _multipart: true };
  }
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

async function ensureSessionOpen(env, session, operator_id, biz, task, opt_created_ms, opt_source) {
  const sid = String(session || "").trim();
  if (!sid) return;

  const ts = opt_created_ms || Date.now();
  const source = opt_source || "scan";
  await env.DB.prepare(
    `INSERT INTO sessions(session,status,created_ms,created_by_operator,closed_ms,closed_by_operator,biz,task,source,owner_operator_id)
     VALUES(?, 'OPEN', ?, ?, NULL, NULL, ?, ?, ?, ?)
     ON CONFLICT(session) DO UPDATE SET
       created_ms=MIN(sessions.created_ms, excluded.created_ms),
       biz=CASE WHEN excluded.biz!='' THEN excluded.biz ELSE sessions.biz END,
       task=CASE WHEN excluded.task!='' THEN excluded.task ELSE sessions.task END`
  ).bind(sid, ts, String(operator_id||""), String(biz||""), String(task||""), source, String(operator_id||"")).run();
}

async function getSession(env, session) {
  const sid = String(session || "").trim();
  if (!sid) return null;
  const r = await env.DB.prepare(
    `SELECT session,status,created_ms,created_by_operator,closed_ms,closed_by_operator,biz,task,owner_operator_id,owner_changed_at,owner_changed_by
     FROM sessions WHERE session=? LIMIT 1`
  ).bind(sid).first();
  return r || null;
}

// ===== Task state (enforce "start before join") =====
const REQUIRE_START_TASKS = new Set(["理货","拣货","换单","批量出库","B2B入库理货","B2B工单操作","B2B现场记录"]);

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

// ===== 布尔归一化 helper =====
function toBool01(v) {
  return (v === 1 || v === true || v === "1" || v === "true") ? 1 : 0;
}

// ===== B2B工单操作: 未完成结果单检查 =====
async function getPendingB2bOpResultsForSession_(db, sessionId) {
  const bindings = await db.prepare(
    `SELECT DISTINCT day_kst, source_type, source_order_no FROM b2b_operation_bindings WHERE session_id=?`
  ).bind(sessionId).all();
  const rows = bindings.results || [];
  const pending = [];
  for (const b of rows) {
    const result = await db.prepare(
      `SELECT status FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
    ).bind(b.day_kst, b.source_type, b.source_order_no).first();
    const st = result ? result.status : "missing";
    if (st !== "completed") {
      pending.push({ day_kst: b.day_kst, source_type: b.source_type, source_order_no: b.source_order_no, result_status: st });
    }
  }
  return pending;
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

// ===== 补录后重算 session 状态（基于全部 events） =====
async function recalcSessionStatus_(env, session, operator_id) {
  const sid = String(session || "").trim();
  if (!sid) return;

  // 查该 session 的所有有效事件（join/leave/start/end）
  const rs = await env.DB.prepare(
    `SELECT badge, event, biz, task, server_ms FROM events
     WHERE session=? AND event IN ('join','leave','start','end') AND ok=1
     ORDER BY server_ms ASC`
  ).bind(sid).all();
  const rows = rs.results || [];

  if (rows.length === 0) {
    // 无任何事件 → CLOSED（无人无操作）
    await env.DB.prepare(
      `UPDATE sessions SET status='CLOSED', closed_ms=0, closed_by_operator=? WHERE session=?`
    ).bind(String(operator_id || "manual_correction"), sid).run();
    await taskStateCloseAll_(env, sid, 0, String(operator_id || "manual_correction"));
    return;
  }

  // 1) 按 badge 统计 join - leave，判断是否还有人在岗
  const badgeCounts = {};
  let maxLeaveMs = 0;
  // 1b) 按 (biz,task) 统计 join - leave 净计数，判断该 task 是否仍有人在岗
  const taskJoinNet = {};    // "biz|task" → join_count - leave_count
  const taskEarliestJoinMs = {}; // "biz|task" → earliest join server_ms
  // 2) 按 (biz,task) 计数 start/end，判断 task 级别状态
  const taskStartCount = {};  // "biz|task" → start 次数
  const taskEndCount = {};    // "biz|task" → end 次数
  const taskStartMs = {};     // "biz|task" → earliest start server_ms
  let hasSessionEnd = false;
  let sessionEndMs = 0;
  let maxTaskEndMs = 0;       // 普通 task end 的最大时间

  for (const r of rows) {
    if (r.event === "join") {
      if (!badgeCounts[r.badge]) badgeCounts[r.badge] = 0;
      badgeCounts[r.badge]++;
      const jtk = (r.biz || "") + "|" + (r.task || "");
      taskJoinNet[jtk] = (taskJoinNet[jtk] || 0) + 1;
      if (!taskEarliestJoinMs[jtk] || r.server_ms < taskEarliestJoinMs[jtk]) taskEarliestJoinMs[jtk] = r.server_ms;
    }
    if (r.event === "leave") {
      if (!badgeCounts[r.badge]) badgeCounts[r.badge] = 0;
      badgeCounts[r.badge]--;
      if (r.server_ms > maxLeaveMs) maxLeaveMs = r.server_ms;
      const ltk = (r.biz || "") + "|" + (r.task || "");
      taskJoinNet[ltk] = (taskJoinNet[ltk] || 0) - 1;
    }
    if (r.event === "start") {
      if (r.task === "SESSION") {
        // session 级 start 不影响 task_state，仅表示 session 开始
      } else {
        const tk = (r.biz || "") + "|" + (r.task || "");
        taskStartCount[tk] = (taskStartCount[tk] || 0) + 1;
        if (!taskStartMs[tk] || r.server_ms < taskStartMs[tk]) taskStartMs[tk] = r.server_ms;
      }
    }
    if (r.event === "end") {
      if (r.task === "SESSION") {
        hasSessionEnd = true;
        if (r.server_ms > sessionEndMs) sessionEndMs = r.server_ms;
      } else {
        const tk = (r.biz || "") + "|" + (r.task || "");
        taskEndCount[tk] = (taskEndCount[tk] || 0) + 1;
        if (r.server_ms > maxTaskEndMs) maxTaskEndMs = r.server_ms;
      }
    }
  }

  // 按 (biz,task) 判断：start_count > end_count → 该 task 仍 started
  const allTaskKeys = new Set([...Object.keys(taskStartCount), ...Object.keys(taskEndCount)]);
  const taskStillOpen = {};  // "biz|task" → boolean
  for (const tk of allTaskKeys) {
    taskStillOpen[tk] = (taskStartCount[tk] || 0) > (taskEndCount[tk] || 0);
  }

  // 任意 badge 的 join > leave → 仍有人在岗
  const anyPersonOpen = Object.values(badgeCounts).some(c => c > 0);
  // session 未被 end 事件关闭
  const notSessionEnded = !hasSessionEnd;

  // session 应为 OPEN 条件：有人在岗 或 有任务仍 started（且没被 SESSION end 关闭）
  const shouldOpen = notSessionEnded && (anyPersonOpen || Object.values(taskStillOpen).some(v => v));

  const op = String(operator_id || "manual_correction");

  if (shouldOpen) {
    await env.DB.prepare(
      `UPDATE sessions SET status='OPEN', closed_ms=NULL, closed_by_operator=NULL WHERE session=?`
    ).bind(sid).run();

    // 同步 task_state：基于 start/end 和 join/leave 两个维度
    // 合并所有出现过的 biz|task 键（start/end + join/leave）
    const allKeys = new Set([...allTaskKeys, ...Object.keys(taskJoinNet)]);
    for (const tk of allKeys) {
      const parts = tk.split("|");
      const biz = parts[0], task = parts[1];
      const openByStartEnd = !!taskStillOpen[tk];
      const openByJoinLeave = (taskJoinNet[tk] || 0) > 0;
      if (openByStartEnd || openByJoinLeave) {
        const startMs = taskStartMs[tk] || taskEarliestJoinMs[tk] || 0;
        await taskStateOpen_(env, sid, biz, task, startMs, op);
      } else {
        await taskStateClose_(env, sid, biz, task, maxTaskEndMs || maxLeaveMs || Date.now(), op);
      }
    }
  } else {
    // CLOSED：取 sessionEndMs / maxLeaveMs / maxTaskEndMs 中最大的作为 closed_ms
    const closedMs = Math.max(sessionEndMs, maxLeaveMs, maxTaskEndMs) || 0;
    await env.DB.prepare(
      `UPDATE sessions SET status='CLOSED', closed_ms=?, closed_by_operator=? WHERE session=?`
    ).bind(closedMs, op, sid).run();
    // 同步关闭所有 task_state
    await taskStateCloseAll_(env, sid, closedMs, op);
  }
}

export default {
  async fetch(request, env, ctx) {
    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    // ===== 一次性迁移系统（带标记，幂等） =====
    if (!env._migrationsChecked) {
      try {
        await env.DB.prepare(`CREATE TABLE IF NOT EXISTS _migrations(key TEXT PRIMARY KEY, ran_at INTEGER)`).run();

        // --- v1: sessions 表加 source 列 + 历史 -CORR 回填 source ---
        const m1 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v1_add_source_column'`).first();
        if (!m1) {
          try {
            await env.DB.prepare(`ALTER TABLE sessions ADD COLUMN source TEXT DEFAULT 'scan'`).run();
          } catch(e) { /* 列已存在则忽略 */ }
          await env.DB.prepare(`UPDATE sessions SET source='manual_correction' WHERE session LIKE '%-CORR' AND (source IS NULL OR source='scan')`).run();
          // 迁移完整成功后才写标记
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v1_add_source_column', ?)`).bind(Date.now()).run();
        }

        // --- v2: 回填历史补录 session 的 created_ms / status / closed_ms ---
        const m2 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v2_backfill_corr_sessions'`).first();
        if (!m2) {
          // Step 1: created_ms = 该 session 最早的 join/leave 事件时间
          await env.DB.prepare(`
            UPDATE sessions SET created_ms = (
              SELECT MIN(server_ms) FROM events
              WHERE events.session = sessions.session AND ok=1 AND event IN ('join','leave')
            )
            WHERE source='manual_correction'
              AND EXISTS (SELECT 1 FROM events WHERE events.session = sessions.session AND ok=1 AND event IN ('join','leave'))
          `).run();

          // Step 2: 逐个 session 重算 status / closed_ms（只基于 join/leave）
          const corrRows = await env.DB.prepare(`SELECT session FROM sessions WHERE source='manual_correction'`).all();
          for (const row of (corrRows.results || [])) {
            const sid = row.session;
            const evRs = await env.DB.prepare(
              `SELECT badge, event, server_ms FROM events
               WHERE session=? AND event IN ('join','leave') AND ok=1
               ORDER BY server_ms ASC`
            ).bind(sid).all();
            const evts = evRs.results || [];
            if (evts.length === 0) continue;

            const badgeCounts = {};
            let maxLeaveMs = 0;
            for (const e of evts) {
              if (!badgeCounts[e.badge]) badgeCounts[e.badge] = 0;
              if (e.event === "join") badgeCounts[e.badge]++;
              if (e.event === "leave") {
                badgeCounts[e.badge]--;
                if (e.server_ms > maxLeaveMs) maxLeaveMs = e.server_ms;
              }
            }
            const anyOpen = Object.values(badgeCounts).some(c => c > 0);
            if (anyOpen) {
              await env.DB.prepare(
                `UPDATE sessions SET status='OPEN', closed_ms=NULL, closed_by_operator=NULL WHERE session=?`
              ).bind(sid).run();
            } else {
              await env.DB.prepare(
                `UPDATE sessions SET status='CLOSED', closed_ms=?, closed_by_operator='backfill' WHERE session=?`
              ).bind(maxLeaveMs, sid).run();
            }
          }

          // 迁移完整成功后才写标记
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v2_backfill_corr_sessions', ?)`).bind(Date.now()).run();
        }

        // --- v3: B2B 计划与作业单表 ---
        const m3 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v3_b2b_tables'`).first();
        if (!m3) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_inbound_plans(
            plan_id TEXT PRIMARY KEY,
            plan_day TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            biz_type TEXT NOT NULL DEFAULT 'other',
            goods_summary TEXT NOT NULL DEFAULT '',
            expected_arrival_time TEXT DEFAULT '',
            purpose_text TEXT DEFAULT '',
            remark TEXT DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            status_updated_by TEXT DEFAULT '',
            status_updated_at INTEGER DEFAULT 0,
            created_by TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL DEFAULT 0
          )`).run();

          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_workorders(
            workorder_id TEXT PRIMARY KEY,
            external_workorder_no TEXT DEFAULT '',
            outbound_mode TEXT NOT NULL DEFAULT 'sku_based',
            status TEXT NOT NULL DEFAULT 'draft',
            customer_name TEXT NOT NULL DEFAULT '',
            customer_name_kr TEXT DEFAULT '',
            plan_day TEXT NOT NULL,
            planned_start_at TEXT DEFAULT '',
            planned_end_at TEXT DEFAULT '',
            total_qty REAL DEFAULT 0,
            total_qty_unit TEXT DEFAULT '',
            total_weight_kg REAL DEFAULT 0,
            total_cbm REAL DEFAULT 0,
            instruction_text TEXT DEFAULT '',
            remark TEXT DEFAULT '',
            created_by TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL DEFAULT 0,
            issued_at INTEGER DEFAULT 0,
            completed_at INTEGER DEFAULT 0,
            cancelled_at INTEGER DEFAULT 0
          )`).run();

          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_workorder_lines(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workorder_id TEXT NOT NULL,
            line_no INTEGER NOT NULL DEFAULT 0,
            line_type TEXT NOT NULL DEFAULT 'sku',
            sku_code TEXT DEFAULT '',
            product_name TEXT DEFAULT '',
            carton_no TEXT DEFAULT '',
            qty REAL NOT NULL DEFAULT 0,
            length_cm REAL DEFAULT 0,
            width_cm REAL DEFAULT 0,
            height_cm REAL DEFAULT 0,
            weight_kg REAL DEFAULT 0,
            remark TEXT DEFAULT ''
          )`).run();

          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_b2b_wol_woid ON b2b_workorder_lines(workorder_id)`).run();

          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v3_b2b_tables', ?)`).bind(Date.now()).run();
        }

        // --- v4: B2B 作业单三模式拆分（operation_mode / outbound_mode 改含义 / detail_mode 新增） ---
        const m4 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v4_b2b_wo_3modes'`).first();
        if (!m4) {
          // 新增 detail_mode 列，默认 sku_based
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN detail_mode TEXT DEFAULT 'sku_based'`).run();
          // 新增 operation_mode 列
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN operation_mode TEXT DEFAULT ''`).run();
          // 把旧 outbound_mode（sku_based/carton_based）迁移到 detail_mode
          await env.DB.prepare(`UPDATE b2b_workorders SET detail_mode = outbound_mode WHERE outbound_mode IN ('sku_based','carton_based')`).run();
          // 清空旧 outbound_mode（旧值不是真实出库模式）
          await env.DB.prepare(`UPDATE b2b_workorders SET outbound_mode = '' WHERE outbound_mode IN ('sku_based','carton_based')`).run();

          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v4_b2b_wo_3modes', ?)`).bind(Date.now()).run();
        }

        // --- v5: B2B 作业单附件表 ---
        const m5 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v5_b2b_attachments'`).first();
        if (!m5) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_workorder_attachments (
            attachment_id TEXT PRIMARY KEY,
            workorder_id TEXT NOT NULL,
            file_key TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            content_type TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            uploaded_by TEXT DEFAULT '',
            created_at INTEGER NOT NULL
          )`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v5_b2b_attachments', ?)`).bind(Date.now()).run();
        }

        // --- v6: B2B 作业单新增出库目的地/发注番号/箱托数 ---
        const m6 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v6_b2b_wo_extra_fields'`).first();
        if (!m6) {
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN outbound_destination TEXT NOT NULL DEFAULT ''`).run();
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN order_ref_no TEXT NOT NULL DEFAULT ''`).run();
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN outbound_box_count REAL NOT NULL DEFAULT 0`).run();
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN outbound_pallet_count REAL NOT NULL DEFAULT 0`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v6_b2b_wo_extra_fields', ?)`).bind(Date.now()).run();
        }

        // --- v7: 现场作业记录表 ---
        const m7 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v7_b2b_field_ops'`).first();
        if (!m7) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_field_ops (
            record_id TEXT PRIMARY KEY,
            source_plan_id TEXT,
            plan_day TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            goods_summary TEXT DEFAULT '',
            operation_type TEXT NOT NULL DEFAULT 'other',
            input_box_count REAL DEFAULT 0,
            output_box_count REAL DEFAULT 0,
            output_pallet_count REAL DEFAULT 0,
            instruction_text TEXT DEFAULT '',
            status TEXT NOT NULL DEFAULT 'draft',
            created_by TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            completed_at INTEGER,
            bound_workorder_id TEXT,
            bound_at INTEGER
          )`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v7_b2b_field_ops', ?)`).bind(Date.now()).run();
        }

        // --- v8: 出库扫码核对三表 ---
        const m8 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v8_b2b_scan_check'`).first();
        if (!m8) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_scan_batches (
            batch_id TEXT PRIMARY KEY,
            check_day TEXT NOT NULL,
            batch_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            total_barcodes INTEGER NOT NULL DEFAULT 0,
            total_expected_boxes INTEGER NOT NULL DEFAULT 0,
            created_by TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            closed_at INTEGER
          )`).run();
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_scan_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            outbound_barcode TEXT NOT NULL,
            expected_box_count INTEGER NOT NULL,
            customer_name TEXT DEFAULT '',
            goods_summary TEXT DEFAULT '',
            scanned_count INTEGER NOT NULL DEFAULT 0,
            UNIQUE(batch_id, outbound_barcode)
          )`).run();
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_scan_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            outbound_barcode TEXT NOT NULL,
            is_planned INTEGER NOT NULL,
            scanned_by TEXT NOT NULL,
            scanned_at INTEGER NOT NULL,
            undone INTEGER NOT NULL DEFAULT 0,
            undone_at INTEGER
          )`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v8_b2b_scan_check', ?)`).bind(Date.now()).run();
        }

        // v9: b2b_operation_bindings — 扫码绑定作业对象（兼容本系统+外部WMS工单）
        const m9 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v9_b2b_operation_bindings'`).first();
        if (!m9) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_operation_bindings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            badge TEXT NOT NULL DEFAULT '',
            bound_task TEXT NOT NULL DEFAULT '',
            source_type TEXT NOT NULL,
            source_order_no TEXT NOT NULL,
            internal_workorder_id TEXT,
            day_kst TEXT NOT NULL,
            match_status TEXT NOT NULL DEFAULT '',
            matched_wms_ref TEXT NOT NULL DEFAULT '',
            bound_at INTEGER NOT NULL,
            resolved_at INTEGER,
            created_at INTEGER NOT NULL,
            UNIQUE(session_id, source_type, source_order_no)
          )`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v9_b2b_operation_bindings', ?)`).bind(Date.now()).run();
        }

        // v10: b2b_operation_results — 现场结果单（工单级，非session级）
        const m10 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v10_b2b_operation_results'`).first();
        if (!m10) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_operation_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_kst TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_order_no TEXT NOT NULL,
            internal_workorder_id TEXT,
            customer_name TEXT NOT NULL DEFAULT '',
            operation_mode TEXT NOT NULL DEFAULT 'pack_outbound',
            sku_kind_count REAL NOT NULL DEFAULT 0,
            box_count REAL NOT NULL DEFAULT 0,
            pallet_count REAL NOT NULL DEFAULT 0,
            needs_forklift_pick INTEGER NOT NULL DEFAULT 0,
            forklift_pallet_count REAL NOT NULL DEFAULT 0,
            rack_pick_location_count REAL NOT NULL DEFAULT 0,
            remark TEXT NOT NULL DEFAULT '',
            photo_urls_json TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL DEFAULT 'draft',
            created_by TEXT NOT NULL DEFAULT '',
            confirmed_by TEXT NOT NULL DEFAULT '',
            first_session_id TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            completed_at INTEGER,
            UNIQUE(day_kst, source_type, source_order_no)
          )`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v10_b2b_operation_results', ?)`).bind(Date.now()).run();
        }

        // v11: b2b_scan_logs 增加 pallet_no 字段
        const m11 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v11_scan_log_pallet'`).first();
        if (!m11) {
          await env.DB.prepare(`ALTER TABLE b2b_scan_logs ADD COLUMN pallet_no TEXT NOT NULL DEFAULT ''`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v11_scan_log_pallet', ?)`).bind(Date.now()).run();
        }

        // v12: b2b_workorders 增加变更提醒字段
        const m12 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v12_wo_update_notice'`).first();
        if (!m12) {
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN has_update_notice INTEGER NOT NULL DEFAULT 0`).run();
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN last_edited_at INTEGER`).run();
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN last_edited_by TEXT NOT NULL DEFAULT ''`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v12_wo_update_notice', ?)`).bind(Date.now()).run();
        }

        // v13: b2b_workorders 增加变更确认字段
        const m13 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v13_wo_update_ack'`).first();
        if (!m13) {
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN update_ack_at INTEGER`).run();
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN update_ack_by TEXT NOT NULL DEFAULT ''`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v13_wo_update_ack', ?)`).bind(Date.now()).run();
        }

        // v14: b2b_workorders 增加取消提醒字段
        const m14 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v14_wo_cancel_notice'`).first();
        if (!m14) {
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN has_cancel_notice INTEGER NOT NULL DEFAULT 0`).run();
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN cancel_ack_at INTEGER`).run();
          await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN cancel_ack_by TEXT NOT NULL DEFAULT ''`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v14_wo_cancel_notice', ?)`).bind(Date.now()).run();
        }

        // v15: b2b_operation_results 增加 confirm_badge 字段
        const m15 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v15_result_confirm_badge'`).first();
        if (!m15) {
          await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN confirm_badge TEXT NOT NULL DEFAULT ''`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v15_result_confirm_badge', ?)`).bind(Date.now()).run();
        }

        // v16: 现场结果单+现场记录 字段完善
        const m16 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v16_result_fields_extend'`).first();
        if (!m16) {
          // b2b_operation_results 新增 11 列
          const resCols = [
            "packed_qty REAL NOT NULL DEFAULT 0",
            "packed_box_count REAL NOT NULL DEFAULT 0",
            "used_carton INTEGER NOT NULL DEFAULT 0",
            "big_carton_count REAL NOT NULL DEFAULT 0",
            "small_carton_count REAL NOT NULL DEFAULT 0",
            "label_count REAL NOT NULL DEFAULT 0",
            "photo_count REAL NOT NULL DEFAULT 0",
            "has_pallet_detail INTEGER NOT NULL DEFAULT 0",
            "did_pack INTEGER NOT NULL DEFAULT 0",
            "did_rebox INTEGER NOT NULL DEFAULT 0",
            "rebox_count REAL NOT NULL DEFAULT 0"
          ];
          for (const col of resCols) {
            await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN ${col}`).run();
          }
          // b2b_field_ops 新增 14 列（含叉车3列）
          const foCols = [
            "packed_qty REAL NOT NULL DEFAULT 0",
            "packed_box_count REAL NOT NULL DEFAULT 0",
            "used_carton INTEGER NOT NULL DEFAULT 0",
            "big_carton_count REAL NOT NULL DEFAULT 0",
            "small_carton_count REAL NOT NULL DEFAULT 0",
            "label_count REAL NOT NULL DEFAULT 0",
            "photo_count REAL NOT NULL DEFAULT 0",
            "has_pallet_detail INTEGER NOT NULL DEFAULT 0",
            "did_pack INTEGER NOT NULL DEFAULT 0",
            "did_rebox INTEGER NOT NULL DEFAULT 0",
            "rebox_count REAL NOT NULL DEFAULT 0",
            "needs_forklift_pick INTEGER NOT NULL DEFAULT 0",
            "forklift_pallet_count REAL NOT NULL DEFAULT 0",
            "rack_pick_location_count REAL NOT NULL DEFAULT 0"
          ];
          for (const col of foCols) {
            await env.DB.prepare(`ALTER TABLE b2b_field_ops ADD COLUMN ${col}`).run();
          }
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v16_result_fields_extend', ?)`).bind(Date.now()).run();
        }

        // --- v17: 台账查询索引 ---
        const m17 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v17_doc_ledger_indexes'`).first();
        if (!m17) {
          // wave 主查询索引：(event, ok) 等值过滤在前，server_ms 范围在中，GROUP BY 列在后
          // 选择 (event, ok, server_ms) 而非把 biz/task 放前面，因为 biz/task 是可选过滤条件
          // server_ms 范围扫描后，SQLite 可利用后续列加速 GROUP BY
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_events_wave_agg ON events(event, ok, server_ms, biz, task, wave_id, session)`).run();
          // join 事件→session 参与工牌反推索引
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_events_join_session_badge ON events(event, ok, session, badge)`).run();
          // 结果单按日期查询
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_b2b_op_results_day ON b2b_operation_results(day_kst)`).run();
          // 现场记录按日期查询
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_b2b_field_ops_plan_day ON b2b_field_ops(plan_day)`).run();
          // 绑定表按日期+单号查询
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_b2b_op_bindings_day_source ON b2b_operation_bindings(day_kst, source_order_no)`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v17_doc_ledger_indexes', ?)`).bind(Date.now()).run();
        }

        // --- v18: 入库计划+出库作业单 记帐字段 ---
        const m18 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v18_accounted_fields'`).first();
        if (!m18) {
          const tables = ["b2b_inbound_plans", "b2b_workorders"];
          for (const tbl of tables) {
            try { await env.DB.prepare(`ALTER TABLE ${tbl} ADD COLUMN is_accounted INTEGER NOT NULL DEFAULT 0`).run(); } catch(e) {}
            try { await env.DB.prepare(`ALTER TABLE ${tbl} ADD COLUMN accounted_at INTEGER`).run(); } catch(e) {}
            try { await env.DB.prepare(`ALTER TABLE ${tbl} ADD COLUMN accounted_by TEXT NOT NULL DEFAULT ''`).run(); } catch(e) {}
          }
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v18_accounted_fields', ?)`).bind(Date.now()).run();
        }

        // --- v19: 幂等请求表 ---
        const m19 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v19_idempotency_keys'`).first();
        if (!m19) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS api_idempotency_keys (
            request_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            result_id TEXT NOT NULL DEFAULT '',
            response_json TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
          )`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v19_idempotency_keys', ?)`).bind(Date.now()).run();
        }

        // --- v20: 幂等表主键改为 (action, request_id) ---
        const m20 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v20_idempotency_composite_pk'`).first();
        if (!m20) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS api_idempotency_keys_v2 (
            action TEXT NOT NULL,
            request_id TEXT NOT NULL,
            result_id TEXT NOT NULL DEFAULT '',
            response_json TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            PRIMARY KEY (action, request_id)
          )`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO api_idempotency_keys_v2(action,request_id,result_id,response_json,created_at) SELECT action,request_id,result_id,response_json,created_at FROM api_idempotency_keys`).run();
          await env.DB.prepare(`DROP TABLE IF EXISTS api_idempotency_keys`).run();
          await env.DB.prepare(`ALTER TABLE api_idempotency_keys_v2 RENAME TO api_idempotency_keys`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v20_idempotency_composite_pk', ?)`).bind(Date.now()).run();
        }

        // --- v21: 出库作业单车辆信息+发货确认 ---
        const m21 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v21_wo_pickup_shipment'`).first();
        if (!m21) {
          const cols21 = [
            ["pickup_vehicle_no","TEXT NOT NULL DEFAULT ''"],
            ["pickup_driver_name","TEXT NOT NULL DEFAULT ''"],
            ["pickup_driver_phone","TEXT NOT NULL DEFAULT ''"],
            ["pickup_remark","TEXT NOT NULL DEFAULT ''"],
            ["pickup_recorded_by","TEXT NOT NULL DEFAULT ''"],
            ["pickup_recorded_at","INTEGER"],
            ["shipment_confirmed_by","TEXT NOT NULL DEFAULT ''"],
            ["shipment_confirmed_at","INTEGER"]
          ];
          for (const [col, def] of cols21) {
            try { await env.DB.prepare(`ALTER TABLE b2b_workorders ADD COLUMN ${col} ${def}`).run(); } catch(e) {}
          }
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v21_wo_pickup_shipment', ?)`).bind(Date.now()).run();
        }

        // v22: 现场记录结果字段统一——补齐 sku_kind_count / remark / confirm_badge / confirmed_by
        const m22 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v22_field_op_result_unify'`).first();
        if (!m22) {
          const foCols22 = [
            ["sku_kind_count","INTEGER NOT NULL DEFAULT 0"],
            ["remark","TEXT NOT NULL DEFAULT ''"],
            ["confirm_badge","TEXT NOT NULL DEFAULT ''"],
            ["confirmed_by","TEXT NOT NULL DEFAULT ''"]
          ];
          for (const [col, def] of foCols22) {
            try { await env.DB.prepare(`ALTER TABLE b2b_field_ops ADD COLUMN ${col} ${def}`).run(); } catch(e) {}
          }
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v22_field_op_result_unify', ?)`).bind(Date.now()).run();
        }

        // v23: WMS 导入批次跟踪（支持部分成功后重试）
        const m23 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v23_wms_import_batches'`).first();
        if (!m23) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS wms_import_batches (
            import_batch_id TEXT PRIMARY KEY,
            content_fingerprint TEXT NOT NULL DEFAULT '',
            source_type TEXT NOT NULL DEFAULT '',
            source_file TEXT NOT NULL DEFAULT '',
            sheet_name TEXT NOT NULL DEFAULT '',
            total_rows INTEGER NOT NULL DEFAULT 0,
            inserted_rows INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'partial',
            created_ms INTEGER NOT NULL DEFAULT 0,
            updated_ms INTEGER NOT NULL DEFAULT 0
          )`).run();
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS wms_import_batch_chunks (
            import_batch_id TEXT NOT NULL,
            row_offset INTEGER NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            created_ms INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (import_batch_id, row_offset)
          )`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v23_wms_import_batches', ?)`).bind(Date.now()).run();
        }

        // v24: session owner transfer（趟次交接）
        const m24 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v24_session_owner_transfer'`).first();
        if (!m24) {
          try { await env.DB.prepare(`ALTER TABLE sessions ADD COLUMN owner_operator_id TEXT`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE sessions ADD COLUMN owner_changed_at INTEGER`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE sessions ADD COLUMN owner_changed_by TEXT`).run(); } catch(e) {}
          // 回填：owner_operator_id = created_by_operator
          await env.DB.prepare(`UPDATE sessions SET owner_operator_id = created_by_operator WHERE owner_operator_id IS NULL`).run();
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v24_session_owner_transfer', ?)`).bind(Date.now()).run();
        }

        // v25: B2B simple mode — labor details table + result extensions
        const m25 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v25_b2b_simple_mode'`).first();
        if (!m25) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_operation_labor_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_kst TEXT NOT NULL,
            session_id TEXT NOT NULL,
            source_type TEXT NOT NULL DEFAULT '',
            source_order_no TEXT NOT NULL DEFAULT '',
            internal_workorder_id TEXT,
            operator_badge TEXT NOT NULL,
            operator_name TEXT NOT NULL DEFAULT '',
            segment_no INTEGER NOT NULL DEFAULT 1,
            join_ms INTEGER NOT NULL,
            leave_ms INTEGER,
            duration_minutes REAL,
            entry_mode TEXT NOT NULL DEFAULT 'simple_mode',
            temp_switch_flag INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
          )`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_bld_day ON b2b_operation_labor_details(day_kst)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_bld_session ON b2b_operation_labor_details(session_id)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_bld_order ON b2b_operation_labor_details(source_order_no)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_bld_badge ON b2b_operation_labor_details(operator_badge)`).run();
          // Extend b2b_operation_results
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN result_entered_by_badge TEXT NOT NULL DEFAULT ''`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN result_entered_by_name TEXT NOT NULL DEFAULT ''`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN result_entered_at INTEGER`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN confirmed_at INTEGER`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN reviewed_by_badge TEXT NOT NULL DEFAULT ''`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN reviewed_by_name TEXT NOT NULL DEFAULT ''`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN reviewed_at INTEGER`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN temporary_completed_at INTEGER`).run(); } catch(e) {}
          try { await env.DB.prepare(`ALTER TABLE b2b_operation_results ADD COLUMN workflow_status TEXT NOT NULL DEFAULT ''`).run(); } catch(e) {}
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v25_b2b_simple_mode', ?)`).bind(Date.now()).run();
        }

        const m26 = await env.DB.prepare(`SELECT 1 FROM _migrations WHERE key='v26_fo_multi_bind'`).first();
        if (!m26) {
          await env.DB.prepare(`CREATE TABLE IF NOT EXISTS b2b_field_op_wo_bindings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id TEXT NOT NULL,
            workorder_id TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0,
            bind_note TEXT DEFAULT '',
            bound_by TEXT NOT NULL DEFAULT '',
            bound_at INTEGER NOT NULL,
            UNIQUE(record_id, workorder_id)
          )`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_fowb_record ON b2b_field_op_wo_bindings(record_id)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_fowb_wo ON b2b_field_op_wo_bindings(workorder_id)`).run();
          // 迁移旧数据：把 field_ops.bound_workorder_id 写入 bindings 表
          const oldBinds = await env.DB.prepare(
            `SELECT record_id, bound_workorder_id, bound_at FROM b2b_field_ops WHERE bound_workorder_id IS NOT NULL AND bound_workorder_id != ''`
          ).all();
          for (const ob of (oldBinds.results || [])) {
            await env.DB.prepare(
              `INSERT OR IGNORE INTO b2b_field_op_wo_bindings(record_id, workorder_id, is_primary, bound_by, bound_at) VALUES(?,?,1,'migrated',?)`
            ).bind(ob.record_id, ob.bound_workorder_id, ob.bound_at || Date.now()).run();
          }
          await env.DB.prepare(`INSERT OR IGNORE INTO _migrations(key, ran_at) VALUES('v26_fo_multi_bind', ?)`).bind(Date.now()).run();
        }
      } catch(e) {
        // 迁移失败不阻断请求，下次冷启动会重试（幂等）
      }
      env._migrationsChecked = true;
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
      const activeList = lockInfo.active || [];

      // 为 active 人员附带最近一次 join 的 note（用于取/送货、问题处理备注恢复）
      if (activeList.length > 0) {
        const noteRows = await env.DB.prepare(
          `SELECT badge, task, note FROM events
           WHERE session=? AND event='join' AND ok=1 AND note IS NOT NULL AND note!=''
           ORDER BY server_ms DESC`
        ).bind(session).all();
        const noteMap = {}; // badge|task -> note (取最新一条)
        for (const nr of (noteRows.results || [])) {
          const nk = nr.badge + "|" + nr.task;
          if (!noteMap[nk]) noteMap[nk] = nr.note;
        }
        for (const lk of activeList) {
          lk.join_note = noteMap[lk.badge + "|" + lk.task] || "";
        }
      }

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
        owner_operator_id: s?.owner_operator_id || s?.created_by_operator || "",
        owner_changed_at: s?.owner_changed_at || 0,
        owner_changed_by: s?.owner_changed_by || "",
        active: activeList
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
      const simple_mode_b2b = String(p.simple_mode || "") === "1" && s && s.biz === "B2B" && s.task === "B2B工单操作";

      // ★ 简化模式预清理：在标准锁/配平检查之前，自动处理所有 leave + 释放锁
      if (simple_mode_b2b) {
        // 1) 阻塞：仍有 working 工单（有人在岗）
        const blockRs = await env.DB.prepare(
          `SELECT r.source_order_no, r.workflow_status FROM b2b_operation_results r
           JOIN b2b_operation_bindings b ON b.day_kst=r.day_kst AND b.source_type=r.source_type AND b.source_order_no=r.source_order_no AND b.session_id=?
           WHERE r.workflow_status IN ('working')`
        ).bind(session).all();
        const blockOrders = (blockRs.results || []);
        if (blockOrders.length > 0) {
          return jsonpOrJson({ ok:true, blocked:true,
            reason: "working_b2b_orders",
            session, pending_orders: blockOrders.map(r => ({ source_order_no: r.source_order_no, result_status: r.workflow_status }))
          }, callback);
        }
        // 2) 关闭所有残留 active labor details
        await env.DB.prepare(
          `UPDATE b2b_operation_labor_details SET leave_ms=?, duration_minutes=ROUND((? - join_ms)/60000.0, 2), status='closed', updated_at=?
           WHERE session_id=? AND status='active'`
        ).bind(now, now, now, session).run();
        // 3) 自动 leave 所有未配对 badge + 释放锁
        const jlPre = await env.DB.prepare(
          `SELECT badge, event FROM events WHERE session=? AND event IN ('join','leave') AND ok=1 ORDER BY server_ms ASC`
        ).bind(session).all();
        const jlCount = {};
        for (const ev of (jlPre.results || [])) {
          if (!jlCount[ev.badge]) jlCount[ev.badge] = 0;
          jlCount[ev.badge] += (ev.event === 'join' ? 1 : -1);
        }
        const unpaired = Object.entries(jlCount).filter(([_, c]) => c > 0);
        for (const [badge] of unpaired) {
          const leaveEvId = `auto_leave_${session}_${badge}_${now}`;
          await env.DB.prepare(
            `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
          ).bind(now, now, leaveEvId, 'leave', badge, s.biz, s.task, session, '', operator_id, 1, 'auto_leave_simple_close').run();
          try {
            await stub.fetch("https://locks/do", {
              method: "POST",
              headers: { "content-type":"application/json" },
              body: JSON.stringify({ action:"lock_release", badge, task: s.task, session, operator_id })
            });
          } catch(e) { /* best effort */ }
        }
      }

      // ★ 标准检查：活跃锁
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

      // ★ 标准检查：join/leave 配平
      const jlRs = await env.DB.prepare(
        `SELECT badge, event FROM events
         WHERE session=? AND event IN ('join','leave') AND ok=1
         ORDER BY server_ms ASC`
      ).bind(session).all();
      const badgeJLCount = {};
      for (const r of (jlRs.results || [])) {
        if (!badgeJLCount[r.badge]) badgeJLCount[r.badge] = 0;
        badgeJLCount[r.badge] += (r.event === 'join' ? 1 : -1);
      }
      const unpairedBadges = Object.entries(badgeJLCount).filter(([_, c]) => c > 0).map(([b]) => b);
      if (unpairedBadges.length > 0) {
        return jsonpOrJson({ ok:true, blocked:true, reason:"unpaired_joins", session, badges: unpairedBadges }, callback);
      }

      // B2B工单操作: 旧模式未完成结果单拦截
      if (s && s.biz === "B2B" && s.task === "B2B工单操作" && !simple_mode_b2b) {
        const pending = await getPendingB2bOpResultsForSession_(env.DB, session);
        if (pending.length > 0) {
          return jsonpOrJson({ ok:true, blocked:true, reason:"pending_b2b_results", session, pending_orders: pending }, callback);
        }
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
      const temp_switch = String(p.temp_switch || "") === "1";

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
           WHERE status='OPEN' AND COALESCE(owner_operator_id, created_by_operator)=?
           AND NOT (biz='B2C' AND task='拣货')
           AND NOT (biz='B2B' AND task='B2B卸货')
           AND NOT (biz='进口' AND task='卸货')
           ORDER BY created_ms DESC
           LIMIT 1`
        ).bind(operator_id).first();

        // 反向检查：如果当前要 start 的不是卸货/拣货，也要排除已有的卸货/拣货 session
        // 装车任务（B2B出库/装柜出货）仅在 temp_switch=1 时豁免，普通开工仍受限
        const isExempt = (biz==='B2C' && task==='拣货') || (biz==='B2B' && task==='B2B卸货') || (biz==='进口' && task==='卸货')
          || (temp_switch && ((biz==='B2B' && task==='B2B出库') || (biz==='进口' && task==='装柜/出货')));

        if (open && String(open.session || "") !== session && !isExempt) {
          return jsonpOrJson({
            ok:false,
            error:"operator_has_open_session",
            open_session: String(open.session),
            open_biz: String(open.biz||""),
            open_task: String(open.task||"")
          }, callback);
        }

        // exempt 任务同类去重：同一 operator + 同一 biz/task 最多 1 个 OPEN session
        if (isExempt) {
          const dupExempt = await env.DB.prepare(
            `SELECT session, biz, task FROM sessions
             WHERE status='OPEN' AND COALESCE(owner_operator_id, created_by_operator)=? AND biz=? AND task=?
             LIMIT 1`
          ).bind(operator_id, biz, task).first();
          if (dupExempt && String(dupExempt.session || "") !== session) {
            return jsonpOrJson({
              ok:false,
              error:"duplicate_exempt_session",
              open_session: String(dupExempt.session),
              open_biz: String(dupExempt.biz||""),
              open_task: String(dupExempt.task||"")
            }, callback);
          }
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
         WHERE status='OPEN' AND COALESCE(owner_operator_id, created_by_operator)=?
           AND (source IS NULL OR source != 'manual_correction')
         ORDER BY created_ms DESC LIMIT 10`
      ).bind(operator_id).all();
      return jsonpOrJson({ ok:true, sessions: rows.results || [] }, callback);
    }

    // ===== 趟次交接 / owner 转移 =====
    if (action === "session_transfer_owner") {
      const session = String(p.session || "").trim();
      const from_operator_id = String(p.from_operator_id || "").trim();
      const to_operator_id = String(p.to_operator_id || "").trim();
      const operator_id = String(p.operator_id || "").trim();
      if (!session) return jsonpOrJson({ ok:false, error:"missing session" }, callback);
      if (!to_operator_id) return jsonpOrJson({ ok:false, error:"missing to_operator_id" }, callback);
      if (!operator_id) return jsonpOrJson({ ok:false, error:"missing operator_id" }, callback);

      // 1. 查 session
      const sess = await env.DB.prepare(
        `SELECT session,status,created_by_operator,owner_operator_id FROM sessions WHERE session=? LIMIT 1`
      ).bind(session).first();
      if (!sess) return jsonpOrJson({ ok:false, error:"session_not_found" }, callback);
      if (String(sess.status || "").toUpperCase() !== "OPEN") return jsonpOrJson({ ok:false, error:"session_not_open" }, callback);

      // 2. 确认当前 owner，不信前端传值
      const currentOwner = String(sess.owner_operator_id || sess.created_by_operator || "");
      if (from_operator_id && from_operator_id !== currentOwner) {
        return jsonpOrJson({ ok:false, error:"owner_mismatch", current_owner: currentOwner }, callback);
      }
      if (to_operator_id === currentOwner) {
        return jsonpOrJson({ ok:false, error:"already_owner" }, callback);
      }

      // 3. to_operator_id 必须在 active 列表
      const stub = locksStub(env);
      const lr = await stub.fetch("https://locks/do", {
        method: "POST",
        headers: { "content-type":"application/json" },
        body: JSON.stringify({ action:"locks_by_session", session })
      });
      const lockData = await lr.json();
      const activeList = lockData.active || [];
      const toInActive = activeList.some(lk => String(lk.badge || "") === to_operator_id);
      if (!toInActive) return jsonpOrJson({ ok:false, error:"to_operator_not_active" }, callback);

      // 4. to_operator_id 不能已有阻塞性 OPEN session（复用 start 同口径）
      const blocking = await env.DB.prepare(
        `SELECT session,biz,task FROM sessions
         WHERE status='OPEN' AND COALESCE(owner_operator_id, created_by_operator)=?
         AND session!=?
         AND NOT (biz='B2C' AND task='拣货')
         AND NOT (biz='B2B' AND task='B2B卸货')
         AND NOT (biz='进口' AND task='卸货')
         ORDER BY created_ms DESC LIMIT 1`
      ).bind(to_operator_id, session).first();
      if (blocking) {
        return jsonpOrJson({
          ok:false, error:"to_operator_has_open_session",
          open_session: String(blocking.session),
          open_biz: String(blocking.biz || ""),
          open_task: String(blocking.task || "")
        }, callback);
      }

      // 5. 更新 owner
      const transferTs = Date.now();
      await env.DB.prepare(
        `UPDATE sessions SET owner_operator_id=?, owner_changed_at=?, owner_changed_by=? WHERE session=?`
      ).bind(to_operator_id, transferTs, operator_id, session).run();

      // 6. 审计记录
      const transferEvId = "owner_transfer_" + session + "_" + transferTs;
      await env.DB.prepare(
        `INSERT OR IGNORE INTO events(server_ms, client_ms, event_id, event, badge, biz, task, session, wave_id, operator_id, ok, note)
         VALUES(?, ?, ?, 'owner_transfer', ?, ?, ?, ?, '', ?, 1, ?)`
      ).bind(
        transferTs, transferTs, transferEvId,
        to_operator_id,
        sess.biz || "", sess.task || "", session,
        operator_id,
        "from:" + currentOwner + " to:" + to_operator_id
      ).run();

      return jsonpOrJson({
        ok:true,
        session,
        from_operator_id: currentOwner,
        to_operator_id,
        owner_changed_at: transferTs
      }, callback);
    }

    if (action === "admin_force_leave") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const badge = String(p.badge || "").trim();
      const biz = String(p.biz || "").trim();
      const task = String(p.task || "").trim();
      const session = String(p.session || "").trim();
      if (!badge) return jsonpOrJson({ ok:false, error:"missing badge" }, callback);
      if (!biz) return jsonpOrJson({ ok:false, error:"missing biz" }, callback);
      if (!task) return jsonpOrJson({ ok:false, error:"missing task" }, callback);
      if (!session) return jsonpOrJson({ ok:false, error:"missing session" }, callback);
      const operator_id = String(p.operator_id || "").trim();

      // 检查 join/leave 净计数，只有 net>0（有未配平 join）才允许插 leave
      const netRs = await env.DB.prepare(
        `SELECT event, COUNT(*) as cnt FROM events
         WHERE badge=? AND biz=? AND task=? AND session=? AND event IN ('join','leave') AND ok=1
         GROUP BY event`
      ).bind(badge, biz, task, session).all();
      let joinCnt = 0, leaveCnt = 0;
      for (const r of (netRs.results || [])) {
        if (r.event === "join") joinCnt = r.cnt;
        if (r.event === "leave") leaveCnt = r.cnt;
      }
      if (joinCnt <= leaveCnt) {
        return jsonpOrJson({ ok:false, error:"no_open_join_to_force_leave", badge, session, join_count: joinCnt, leave_count: leaveCnt }, callback);
      }

      const server_ms = now;
      const event_id = "admin-fl-" + badge + "-" + now;
      await env.DB.prepare(
        `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(server_ms, server_ms, event_id, "leave", badge, biz, task, session, "", operator_id || "", 1, "admin_force_leave").run();
      const stub = locksStub(env);
      let lockReleased = false;
      try {
        const relR = await stub.fetch("https://locks/do", {
          method: "POST",
          headers: { "content-type":"application/json" },
          body: JSON.stringify({ action:"lock_release", badge, task, session })
        });
        const relData = await relR.json();
        lockReleased = !!relData.released;
      } catch (_) {}
      await recalcSessionStatus_(env, session, operator_id || "admin_force_leave");
      return jsonpOrJson({ ok:true, released:true, lock_released: lockReleased }, callback);
    }

    // ===== 补录修正：原子写入 join+leave 一对事件 =====
    if (action === "admin_manual_correction_pair") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const badge = String(p.badge || "").trim();
      const biz = String(p.biz || "").trim();
      const task = String(p.task || "").trim();
      const session = String(p.session || "").trim();
      const join_ms = Number(p.join_ms || 0);
      const leave_ms = Number(p.leave_ms || 0);
      const operator_id = String(p.operator_id || "").trim();
      const note = String(p.note || "").trim() || "manual_correction";

      if (!badge) return jsonpOrJson({ ok:false, error:"missing badge" }, callback);
      if (!biz) return jsonpOrJson({ ok:false, error:"missing biz" }, callback);
      if (!task) return jsonpOrJson({ ok:false, error:"missing task" }, callback);
      if (!join_ms || join_ms < 1000000000000) return jsonpOrJson({ ok:false, error:"invalid join_ms" }, callback);
      if (!leave_ms || leave_ms < 1000000000000) return jsonpOrJson({ ok:false, error:"invalid leave_ms" }, callback);
      if (leave_ms <= join_ms) return jsonpOrJson({ ok:false, error:"leave_ms must be after join_ms" }, callback);

      // deterministic event_id：同一组补录参数永远生成相同 id，防止重复提交
      const idBase = [badge, biz, task, session, join_ms, leave_ms].join("|");
      const joinEventId = "mc-join-" + idBase;
      const leaveEventId = "mc-leave-" + idBase;

      // 前置重复检测：分别检查 join / leave 是否已存在
      const dupRs = await env.DB.prepare(
        `SELECT event_id FROM events WHERE event_id IN (?, ?)`
      ).bind(joinEventId, leaveEventId).all();
      const existingIds = new Set((dupRs.results || []).map(r => r.event_id));
      const joinExists = existingIds.has(joinEventId);
      const leaveExists = existingIds.has(leaveEventId);

      if (joinExists && leaveExists) {
        // 完整重复 → 拦截
        return jsonpOrJson({ ok:false, error:"duplicate manual correction: this join/leave pair already exists" }, callback);
      }

      if (!joinExists && !leaveExists) {
        // 全新 → 原子 batch 写入两条
        let batchResults;
        try {
          batchResults = await env.DB.batch([
            env.DB.prepare(
              `INSERT INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
            ).bind(join_ms, join_ms, joinEventId, "join", badge, biz, task, session, "", operator_id, 1, note),
            env.DB.prepare(
              `INSERT INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
            ).bind(leave_ms, leave_ms, leaveEventId, "leave", badge, biz, task, session, "", operator_id, 1, note)
          ]);
        } catch (batchErr) {
          return jsonpOrJson({ ok:false, error:"atomic insert failed: " + String(batchErr.message || batchErr) }, callback);
        }
        const joinChanges = batchResults[0]?.meta?.changes ?? 0;
        const leaveChanges = batchResults[1]?.meta?.changes ?? 0;
        if (joinChanges !== 1 || leaveChanges !== 1) {
          return jsonpOrJson({ ok:false, error:"insert incomplete: join=" + joinChanges + " leave=" + leaveChanges }, callback);
        }
      } else {
        // 半条残留 → 自动补齐缺失的那一条
        const missingEvent = joinExists ? "leave" : "join";
        const missingId = joinExists ? leaveEventId : joinEventId;
        const missingMs = joinExists ? leave_ms : join_ms;
        try {
          const repairResult = await env.DB.prepare(
            `INSERT INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
          ).bind(missingMs, missingMs, missingId, missingEvent, badge, biz, task, session, "", operator_id, 1, note).run();
          if ((repairResult?.meta?.changes ?? 0) !== 1) {
            return jsonpOrJson({ ok:false, error:"partial repair failed: " + missingEvent + " not inserted" }, callback);
          }
        } catch (repairErr) {
          return jsonpOrJson({ ok:false, error:"partial repair failed: " + String(repairErr.message || repairErr) }, callback);
        }
      }

      // 事件已落库，处理 session/task_state 副作用
      if (session) {
        await ensureSessionOpen(env, session, operator_id, biz, task, join_ms, "manual_correction");
        await taskStateOpen_(env, session, biz, task, join_ms, operator_id);
        await recalcSessionStatus_(env, session, operator_id);
      }

      const repaired = joinExists || leaveExists;
      return jsonpOrJson({ ok:true, inserted:true, repaired, join_event_id: joinEventId, leave_event_id: leaveEventId, badge, join_ms, leave_ms }, callback);
    }

    // ===== 补录修正：管理员手动插入单条 join/leave 事件（指定自定义时间戳） =====
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

      // deterministic event_id：同一组参数永远相同，防止重复提交
      const event_id = "me-" + [event, badge, biz, task, session, custom_ms].join("|");

      // 前置重复检测（在任何副作用之前）
      const dupEv = await env.DB.prepare(
        `SELECT event_id FROM events WHERE event_id=? LIMIT 1`
      ).bind(event_id).first();
      if (dupEv) {
        return jsonpOrJson({ ok:false, error:"duplicate manual event: this event already exists", event_id }, callback);
      }

      // 真正 INSERT 事件
      const insResult = await env.DB.prepare(
        `INSERT INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(custom_ms, custom_ms, event_id, event, badge, biz, task, session, "", operator_id, 1, note).run();
      if ((insResult?.meta?.changes ?? 0) !== 1) {
        return jsonpOrJson({ ok:false, error:"insert failed", event_id }, callback);
      }

      // 事件已落库，再处理 session/task_state 副作用
      if (session) {
        await ensureSessionOpen(env, session, operator_id, biz, task, custom_ms, "manual_correction");
        if (event === "join") {
          await taskStateOpen_(env, session, biz, task, custom_ms, operator_id);
        }
        await recalcSessionStatus_(env, session, operator_id);
      }

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

      const isMcPair = event_id.startsWith("mc-");
      const isMeSingle = event_id.startsWith("me-");
      const keyFieldChanged = p.new_badge !== undefined || p.new_biz !== undefined
        || p.new_task !== undefined || p.new_ms !== undefined;
      // note: session 字段不在可修改列表中，无需检查

      // mc-* 成对补录：禁止修改会破坏 pair 语义的关键字段
      if (isMcPair && keyFieldChanged) {
        return jsonpOrJson({ ok:false, error:"mc-* pair event: cannot edit badge/biz/task/time directly. Delete the pair and re-create via admin_manual_correction_pair" }, callback);
      }

      // me-* 单条补录：关键字段变更时重算 event_id
      let newEventId = event_id;
      if (isMeSingle && keyFieldChanged) {
        const finalBadge = p.new_badge !== undefined ? String(p.new_badge).trim() : existing.badge;
        const finalBiz = p.new_biz !== undefined ? String(p.new_biz).trim() : existing.biz;
        const finalTask = p.new_task !== undefined ? String(p.new_task).trim() : existing.task;
        const finalMs = (p.new_ms !== undefined && p.new_ms !== null && p.new_ms !== "") ? Number(p.new_ms) : existing.server_ms;
        newEventId = "me-" + [existing.event, finalBadge, finalBiz, finalTask, existing.session, finalMs].join("|");

        // 新 event_id 与旧相同（值未实际改变）则无需换 id
        if (newEventId !== event_id) {
          const dupNew = await env.DB.prepare(
            `SELECT event_id FROM events WHERE event_id=? LIMIT 1`
          ).bind(newEventId).first();
          if (dupNew) {
            return jsonpOrJson({ ok:false, error:"update would create duplicate event_id: " + newEventId }, callback);
          }
          sets.push("event_id=?"); binds.push(newEventId);
        }
      }

      binds.push(event_id);
      await env.DB.prepare(`UPDATE events SET ${sets.join(",")} WHERE event_id=?`).bind(...binds).run();

      // 重算该事件所属 session 的状态
      if (existing.session) {
        await recalcSessionStatus_(env, existing.session, String(p.operator_id || "").trim());
      }

      return jsonpOrJson({ ok:true, updated:true, event_id: newEventId, old_event_id: event_id !== newEventId ? event_id : undefined, fields: sets.length }, callback);
    }

    // ===== 修正：删除错误事件 =====
    if (action === "admin_event_delete") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const event_id = String(p.event_id || "").trim();
      if (!event_id) return jsonpOrJson({ ok:false, error:"missing event_id" }, callback);
      const existing = await env.DB.prepare(`SELECT event_id,event,badge,biz,task,session FROM events WHERE event_id=?`).bind(event_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"event_id not found" }, callback);
      const deletedSession = existing.session || "";

      // mc-* 成对补录：联动删除配对事件，不留半条
      let pairEventId = null;
      let pairDetail = null;
      if (event_id.startsWith("mc-join-")) {
        pairEventId = "mc-leave-" + event_id.slice("mc-join-".length);
      } else if (event_id.startsWith("mc-leave-")) {
        pairEventId = "mc-join-" + event_id.slice("mc-leave-".length);
      }
      if (pairEventId) {
        pairDetail = await env.DB.prepare(`SELECT event_id,event,badge,biz,task,session FROM events WHERE event_id=?`).bind(pairEventId).first();
        // batch 删除：两条一起删
        await env.DB.batch([
          env.DB.prepare(`DELETE FROM events WHERE event_id=?`).bind(event_id),
          env.DB.prepare(`DELETE FROM events WHERE event_id=?`).bind(pairEventId)
        ]);
      } else {
        await env.DB.prepare(`DELETE FROM events WHERE event_id=?`).bind(event_id).run();
      }

      // 重算被删事件所属 session 的状态
      if (deletedSession) {
        await recalcSessionStatus_(env, deletedSession, String(p.operator_id || "").trim());
      }

      return jsonpOrJson({ ok:true, deleted:true, event_id, pair_event_id: pairEventId || undefined, pair_found: !!pairDetail, detail: existing }, callback);
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
      // overlap 条件：session 与查询区间有交集（跨天 session 不遗漏）
      if (since_ms) { where += " AND (closed_ms IS NULL OR closed_ms >= ?)"; binds.push(since_ms); }
      if (until_ms) { where += " AND created_ms <= ?"; binds.push(until_ms); }

      const sessionsSql = `SELECT session,status,biz,task,created_ms,created_by_operator,closed_ms,closed_by_operator,source,owner_operator_id FROM sessions ${where} ORDER BY created_ms DESC LIMIT ?`;
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
          source: s.source || "scan",
          owner_operator_id: s.owner_operator_id || s.created_by_operator || "",
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
      // 1) 释放仍属于该 session 的 active lock（带 session 条件，不误删新 session 的锁）
      for (const lk of activeLocks) {
        const badge = String(lk.badge || "").trim();
        if (!badge) continue;
        try {
          const relR = await stub.fetch("https://locks/do", {
            method: "POST",
            headers: { "content-type":"application/json" },
            body: JSON.stringify({ action:"lock_release", badge, session })
          });
          const relData = await relR.json();
          if (relData.released) released.push(badge);
          // different_session / not_found → 不释放，不报错
        } catch (_) {}
      }

      // 2) 检查所有 join/leave 未配平的 badge（含锁已过期但未 leave 的）
      const jlRs = await env.DB.prepare(
        `SELECT badge, biz, task, event FROM events
         WHERE session=? AND event IN ('join','leave') AND ok=1
         ORDER BY server_ms ASC`
      ).bind(session).all();
      const badgeNet = {};   // "badge|biz|task" → join_count - leave_count
      const badgeMeta = {};  // "badge|biz|task" → {badge, biz, task}
      for (const r of (jlRs.results || [])) {
        const k = r.badge + "|" + r.biz + "|" + r.task;
        if (!badgeNet[k]) { badgeNet[k] = 0; badgeMeta[k] = { badge: r.badge, biz: r.biz, task: r.task }; }
        badgeNet[k] += (r.event === "join" ? 1 : -1);
      }
      const autoLeaved = [];
      for (const [k, net] of Object.entries(badgeNet)) {
        if (net <= 0) continue; // 已配平
        const m = badgeMeta[k];
        for (let i = 0; i < net; i++) {
          const evId = "admin-force-leave-" + m.badge + "-" + m.biz + "-" + m.task + "-" + session + "-" + now + "-" + i;
          await env.DB.prepare(
            `INSERT OR IGNORE INTO events(server_ms,client_ms,event_id,event,badge,biz,task,session,wave_id,operator_id,ok,note)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
          ).bind(now, now, evId, "leave", m.badge, m.biz, m.task, session, "", "", 1, "admin_force_end").run();
        }
        autoLeaved.push({ badge: m.badge, biz: m.biz, task: m.task, count: net });
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
      return jsonpOrJson({ ok:true, released, auto_leaved: autoLeaved, session }, callback);
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
      if (!source_type || !["b2c_order_export","b2c_pack_import","import_express","change_order_export","return_inbound_export","return_qc_export"].includes(source_type))
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
          source_type,task_scope,pick_wave_no,location_code,location_type,box_code,weight,owner_name,completed_day_kst,business_day_kst,volume)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
      );

      const stmts = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i] || {};
        const import_id = import_batch_id + "|" + (row_offset + i);
        const raw_json = JSON.stringify(r);

        let biz="", task="", task_scope="", order_no="", wave_no="", sku="", qty=0;
        let box_count=0, pallet_count=0, signed_at="", completed_at="";
        let pick_wave_no="", location_code="", location_type="", box_code="";
        let weight=0, owner_name="", volume=0;

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
        } else if (source_type === "change_order_export") {
          biz = "B2C"; task_scope = "换单";
          order_no = pickField(r, ["快递单号","express_no","快递号"]);
          qty = 1;
          owner_name = pickField(r, ["商户名称","商户","owner","owner_name"]);
          completed_at = pickField(r, ["发货时间","completed_at","完成时间"]);
          signed_at = pickField(r, ["状态","status"]);  // 存状态，refresh 时过滤 已发出
        } else if (source_type === "return_inbound_export") {
          biz = "B2C"; task_scope = "退件入库";
          order_no = pickField(r, ["包裹号","package_no","包裹"]);
          if (!order_no) { skipped++; continue; }  // 跳过空行
          qty = parseInt(pickField(r, ["数量","qty","件数"]), 10) || 0;
          weight = parseFloat(pickField(r, ["重量（KG）","重量(KG)","重量","weight"])) || 0;
          volume = parseFloat(pickField(r, ["体积","volume"])) || 0;
          owner_name = pickField(r, ["商户名称","商户","owner","owner_name"]);
          completed_at = pickField(r, ["仓库签收时间","签收时间","completed_at"]);
          location_code = pickField(r, ["储位号","location_code","储位"]);
        } else if (source_type === "return_qc_export") {
          biz = "B2C"; task_scope = "质检";
          order_no = pickField(r, ["包裹号","package_no","包裹"]);
          if (!order_no) { skipped++; continue; }  // 跳过空行
          qty = parseInt(pickField(r, ["数量","qty","件数"]), 10) || 0;
          weight = parseFloat(pickField(r, ["重量（KG）","重量(KG)","重量","weight"])) || 0;
          volume = parseFloat(pickField(r, ["体积","volume"])) || 0;
          owner_name = pickField(r, ["商户名称","商户","owner","owner_name"]);
          completed_at = pickField(r, ["质检时间","qc_time","completed_at"]);
          signed_at = pickField(r, ["质检","qc_status"]);  // 存质检状态，refresh 时过滤 已质检
          location_code = pickField(r, ["储位号","location_code","储位"]);
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
          source_type, task_scope, pick_wave_no, location_code, location_type, box_code, weight, owner_name, completed_day_kst, business_day_kst, volume
        ));
      }


      // D1 batch (最多同时执行)
      const results = await env.DB.batch(stmts);
      for (const r of results) {
        if (r.meta && r.meta.changes > 0) inserted++;
        else skipped++;
      }

      // ── chunk 跟踪 & 批次状态 ──
      const total_rows = parseInt(p.total_rows || "0", 10) || 0;
      await env.DB.prepare(
        `INSERT OR REPLACE INTO wms_import_batch_chunks(import_batch_id, row_offset, row_count, created_ms)
         VALUES(?,?,?,?)`
      ).bind(import_batch_id, row_offset, rows.length, now).run();
      // 汇总已覆盖行数（每个 chunk 按 row_offset 去重）
      const chunkSum = await env.DB.prepare(
        `SELECT COALESCE(SUM(row_count),0) as processed FROM wms_import_batch_chunks WHERE import_batch_id=?`
      ).bind(import_batch_id).first();
      const processedRows = chunkSum ? (chunkSum.processed || 0) : 0;
      const batchStatus = (total_rows > 0 && processedRows >= total_rows) ? "completed" : "partial";
      await env.DB.prepare(
        `INSERT INTO wms_import_batches(import_batch_id, content_fingerprint, source_type, source_file, sheet_name, total_rows, inserted_rows, status, created_ms, updated_ms)
         VALUES(?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(import_batch_id) DO UPDATE SET
           inserted_rows = inserted_rows + excluded.inserted_rows,
           status = excluded.status,
           updated_ms = excluded.updated_ms`
      ).bind(import_batch_id, content_fingerprint, source_type, source_file, sheet_name, total_rows, inserted, batchStatus, now, now).run();

      const summary = { s_empty_order, s_zero_qty, s_empty_bizday, s_loc_unknown };
      return jsonpOrJson({ ok:true, inserted, skipped, total: rows.length, import_batch_id, source_type, summary }, callback);
    }

    // ===== WMS 导入记录查询（优先 wms_import_batches，旧数据 fallback wms_outputs） =====
    if (action === "wms_list") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const limit = Math.min(Math.max(parseInt(p.limit || "30", 10) || 30, 1), 200);
      // 新表：有状态的批次
      const rs1 = await env.DB.prepare(
        `SELECT import_batch_id, source_file, sheet_name, source_type, total_rows, inserted_rows, status, created_ms, updated_ms
         FROM wms_import_batches ORDER BY updated_ms DESC LIMIT ?`
      ).bind(limit).all();
      const newBatches = rs1.results || [];
      const seenIds = new Set(newBatches.map(b => b.import_batch_id));
      // 旧表 fallback：补充没有 batch 表记录的历史批次
      const remaining = limit - newBatches.length;
      let legacyBatches = [];
      if (remaining > 0) {
        const rs2 = await env.DB.prepare(
          `SELECT import_batch_id, source_file, sheet_name, source_type, COUNT(*) as row_count, MAX(created_ms) as created_ms
           FROM wms_outputs GROUP BY import_batch_id ORDER BY created_ms DESC LIMIT ?`
        ).bind(limit).all();
        for (const b of (rs2.results || [])) {
          if (!seenIds.has(b.import_batch_id)) {
            legacyBatches.push({ ...b, total_rows: b.row_count, inserted_rows: b.row_count, status: "completed", updated_ms: b.created_ms });
            if (legacyBatches.length >= remaining) break;
          }
        }
      }
      const batches = newBatches.concat(legacyBatches);
      return jsonpOrJson({ ok:true, batches }, callback);
    }

    // ===== WMS 重复检测（文件名规则 + 内容指纹） =====
    if (action === "wms_check_duplicate") {
      if (!isAdmin_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const source_file = String(p.source_file || "").trim();
      const sheet_name = String(p.sheet_name || "").trim();
      const row_count = parseInt(p.row_count || "0", 10) || 0;
      const content_fingerprint = String(p.content_fingerprint || "").trim();
      const source_type = String(p.source_type || "").trim();

      // 1) 内容指纹检测：查 wms_import_batches
      //    completed → 硬拦截(block)，partial → 软提醒(warn，允许重试)
      let block = false;
      let block_matches = [];
      let partial_warn = false;
      let partial_matches = [];
      if (content_fingerprint && source_type) {
        const rs2 = await env.DB.prepare(
          `SELECT import_batch_id, source_type, source_file, sheet_name, total_rows, inserted_rows, status,
                  created_ms, updated_ms
           FROM wms_import_batches
           WHERE content_fingerprint=? AND source_type=? AND content_fingerprint!=''
           ORDER BY updated_ms DESC, created_ms DESC
           LIMIT 5`
        ).bind(content_fingerprint, source_type).all();
        const matches = rs2.results || [];
        for (const m of matches) {
          if (m.status === "completed") {
            block = true;
            block_matches.push(m);
          } else {
            partial_matches.push(m);
          }
        }
        if (!block && partial_matches.length > 0) partial_warn = true;
      }

      // 2) 文件名软提醒：同 source_file + sheet_name + row_count（全量历史）
      let name_matches = [];
      if (!block && !partial_warn) {
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
        partial_warn,
        partial_matches,
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

    // ===== B2B工单操作 日报追溯明细 =====
    if (action === "admin_daily_b2b_wo_detail") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const day_kst = String(p.day_kst || "").trim();
      if (!day_kst || !/^\d{4}-\d{2}-\d{2}$/.test(day_kst)) return jsonpOrJson({ ok:false, error:"invalid day_kst" }, callback);

      // 口径与 admin_refresh_daily Step9 完全一致：
      // source_type='internal_b2b_workorder', match_status='direct_internal', GROUP BY internal_workorder_id, MIN(bound_at)
      // 首次绑定日 = day_kst 的工单才计入

      // 1) included: 首次绑定日=day_kst 且 status != cancelled
      const inclRs = await env.DB.prepare(
        `SELECT sub.internal_workorder_id, sub.first_day_kst, sub.first_bound_at, sub.binding_count,
                w.customer_name, w.status, w.outbound_box_count, w.outbound_pallet_count,
                w.total_weight_kg, w.total_cbm, w.detail_mode
         FROM (
           SELECT internal_workorder_id,
                  MIN(bound_at) as first_bound_at,
                  COUNT(*) as binding_count,
                  (SELECT day_kst FROM b2b_operation_bindings b2
                   WHERE b2.internal_workorder_id = b1.internal_workorder_id
                     AND b2.source_type='internal_b2b_workorder'
                   ORDER BY b2.bound_at ASC LIMIT 1) as first_day_kst
           FROM b2b_operation_bindings b1
           WHERE source_type='internal_b2b_workorder'
             AND match_status='direct_internal'
             AND internal_workorder_id IS NOT NULL
           GROUP BY internal_workorder_id
         ) sub
         JOIN b2b_workorders w ON w.workorder_id = sub.internal_workorder_id
         WHERE w.status != 'cancelled'
           AND sub.first_day_kst = ?
         ORDER BY sub.first_bound_at ASC`
      ).bind(day_kst).all();

      const included = (inclRs.results || []).map(r => ({
        workorder_id: r.internal_workorder_id,
        customer_name: r.customer_name || "",
        status: r.status,
        detail_mode: r.detail_mode,
        outbound_box_count: r.outbound_box_count || 0,
        outbound_pallet_count: r.outbound_pallet_count || 0,
        total_weight_kg: r.total_weight_kg || 0,
        total_cbm: r.total_cbm || 0,
        first_bound_at: r.first_bound_at,
        first_day_kst: r.first_day_kst,
        binding_count: r.binding_count
      }));

      // 2) summary
      let sum_box = 0, sum_pallet = 0, sum_weight = 0, sum_volume = 0;
      for (const w of included) {
        sum_box += w.outbound_box_count;
        sum_pallet += w.outbound_pallet_count;
        sum_weight += w.total_weight_kg;
        sum_volume += w.total_cbm;
      }
      const summary = {
        order_count: included.length,
        box_count: sum_box,
        pallet_count: sum_pallet,
        weight: Math.round(sum_weight * 100) / 100,
        volume: Math.round(sum_volume * 10000) / 10000
      };

      // 3) excluded_cancelled: 首次绑定日=day_kst 但 status=cancelled
      const exclRs = await env.DB.prepare(
        `SELECT sub.internal_workorder_id, sub.first_bound_at,
                w.customer_name, w.status
         FROM (
           SELECT internal_workorder_id,
                  MIN(bound_at) as first_bound_at,
                  (SELECT day_kst FROM b2b_operation_bindings b2
                   WHERE b2.internal_workorder_id = b1.internal_workorder_id
                     AND b2.source_type='internal_b2b_workorder'
                   ORDER BY b2.bound_at ASC LIMIT 1) as first_day_kst
           FROM b2b_operation_bindings b1
           WHERE source_type='internal_b2b_workorder'
             AND match_status='direct_internal'
             AND internal_workorder_id IS NOT NULL
           GROUP BY internal_workorder_id
         ) sub
         JOIN b2b_workorders w ON w.workorder_id = sub.internal_workorder_id
         WHERE w.status = 'cancelled'
           AND sub.first_day_kst = ?`
      ).bind(day_kst).all();

      const excluded_cancelled = (exclRs.results || []).map(r => ({
        workorder_id: r.internal_workorder_id,
        customer_name: r.customer_name || "",
        first_bound_at: r.first_bound_at
      }));

      return jsonpOrJson({ ok:true, day_kst, included, summary, excluded_cancelled }, callback);
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
      // 4) change_order_export: completed_day_kst (只统计 已发出)
      const rs4 = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst, COUNT(*) as cnt
         FROM wms_outputs WHERE source_type='change_order_export'
           AND signed_at='已发出'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();
      // 5) return_inbound_export: completed_day_kst (不过滤状态)
      const rs5 = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst, COUNT(*) as cnt
         FROM wms_outputs WHERE source_type='return_inbound_export'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();
      // 6) return_qc_export: completed_day_kst (只统计 已质检)
      const rs6 = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst, COUNT(*) as cnt
         FROM wms_outputs WHERE source_type='return_qc_export'
           AND signed_at='已质检'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();

      // 按天汇总
      const dayMap = {}; // day → { b2c_order_export, b2c_pack_import, import_express, change_order_export, return_inbound_export, return_qc_export }
      // 生成日期列表
      // 纯 UTC 日期迭代，避免时区偏移导致日期减一天
      const d = new Date(start_date + "T00:00:00Z");
      const dEnd = new Date(end_date + "T00:00:00Z");
      while (d <= dEnd) {
        const ds = d.getUTCFullYear() + "-" + String(d.getUTCMonth()+1).padStart(2,"0") + "-" + String(d.getUTCDate()).padStart(2,"0");
        dayMap[ds] = { b2c_order_export: 0, b2c_pack_import: 0, import_express: 0, change_order_export: 0, return_inbound_export: 0, return_qc_export: 0 };
        d.setUTCDate(d.getUTCDate() + 1);
      }
      for (const r of (rs1.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].b2c_order_export = r.cnt; }
      for (const r of (rs2.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].b2c_pack_import = r.cnt; }
      for (const r of (rs3.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].import_express = r.cnt; }
      for (const r of (rs4.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].change_order_export = r.cnt; }
      for (const r of (rs5.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].return_inbound_export = r.cnt; }
      for (const r of (rs6.results || [])) { if (dayMap[r.day_kst]) dayMap[r.day_kst].return_qc_export = r.cnt; }

      // 生成 gaps
      const gaps = [];
      for (const [day, counts] of Object.entries(dayMap)) {
        if (counts.b2c_order_export === 0) gaps.push({ day, source_type: "b2c_order_export", label: "B2C订单表" });
        if (counts.b2c_pack_import === 0) gaps.push({ day, source_type: "b2c_pack_import", label: "进口打包表" });
        if (counts.import_express === 0) gaps.push({ day, source_type: "import_express", label: "进口快件表" });
        if (counts.change_order_export === 0) gaps.push({ day, source_type: "change_order_export", label: "换单表" });
        if (counts.return_inbound_export === 0) gaps.push({ day, source_type: "return_inbound_export", label: "退件入库表" });
        if (counts.return_qc_export === 0) gaps.push({ day, source_type: "return_qc_export", label: "质检表" });
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

      const TASKS = ["B2C拣货", "B2C打包", "过机扫描码托", "换单", "退件入库", "质检"];

      // ── 通用 helper：将一段工时按 KST 自然日切片 ──
      // 返回 [{day_kst, minutes}]
      // KST 日界 = UTC 15:00 前一天 → UTC 15:00 = KST 00:00
      const KST_OFFSET = 9 * 3600 * 1000;
      function kstDayOf(ms) {
        const d = new Date(ms + KST_OFFSET);
        return d.getUTCFullYear() + "-" + String(d.getUTCMonth()+1).padStart(2,"0") + "-" + String(d.getUTCDate()).padStart(2,"0");
      }
      function kstDayStartMs(dayStr) {
        return new Date(dayStr + "T00:00:00+09:00").getTime();
      }
      function sliceLaborByDay_(joinMs, leaveMs, rangeStart, rangeEnd) {
        // clamp to query range
        const s = Math.max(joinMs, rangeStart);
        const e = Math.min(leaveMs, rangeEnd);
        if (s >= e) return [];
        const slices = [];
        let cur = s;
        while (cur < e) {
          const day = kstDayOf(cur);
          const nextDayMs = kstDayStartMs(day) + 24 * 3600 * 1000; // KST 次日 00:00
          const sliceEnd = Math.min(nextDayMs, e);
          const mins = (sliceEnd - cur) / 60000;
          if (mins > 0) slices.push({ day_kst: day, minutes: mins });
          cur = sliceEnd;
        }
        return slices;
      }

      // Step1: 劳动数据 — join/leave 事件
      // 主查询：range 内 + leave 后扩 24h（跨天 leave 滞后）
      const laborQueryEnd = endMs + 24 * 3600 * 1000;
      const evRs = await env.DB.prepare(
        `SELECT biz, task, badge, session, event, server_ms
         FROM events
         WHERE event IN ('join','leave') AND ok=1 AND server_ms >= ? AND server_ms <= ?
         ORDER BY badge, session, server_ms`
      ).bind(startMs, laborQueryEnd).all();
      const evRows = evRs.results || [];

      // 补查：检测 orphan leave（首个事件即 leave，说明 join 在 startMs 之前）
      // 按 badge+session+biz+task 精确匹配补回最近一条 join
      const bsFirstEvent = {}; // "badge|session|biz|task" → first event type
      for (const e of evRows) {
        const k = e.badge + "|" + e.session + "|" + e.biz + "|" + e.task;
        if (!(k in bsFirstEvent)) bsFirstEvent[k] = e.event;
      }
      const orphanKeys = Object.entries(bsFirstEvent)
        .filter(([_, ev]) => ev === 'leave')
        .map(([k]) => k.split("|")); // [badge, session, biz, task]
      for (const [badge, session, biz, task] of orphanKeys) {
        const sup = await env.DB.prepare(
          `SELECT biz, task, badge, session, event, server_ms
           FROM events
           WHERE event='join' AND ok=1 AND badge=? AND session=? AND biz=? AND task=? AND server_ms < ?
           ORDER BY server_ms DESC LIMIT 1`
        ).bind(badge, session, biz, task, startMs).first();
        if (sup) evRows.push(sup);
      }
      // 补查：silent badge+task — 区间前已 join 且 startMs 时仍未配平（按 badge+session+biz+task 粒度）
      // 必须检查 join_count > leave_count，不能把已 leave 的人误补成 silent open
      const sessOverlapRs = await env.DB.prepare(
        `SELECT session, biz, task, created_ms, closed_ms
         FROM sessions
         WHERE created_ms <= ? AND (closed_ms IS NULL OR closed_ms >= ?)
         AND biz != '' AND task != ''`
      ).bind(endMs, startMs).all();
      // 收集 evRows 中已有事件的 badge+session+biz+task 组合
      const bsbtInEvRows = new Set(evRows.map(e => e.badge + "|" + e.session + "|" + e.biz + "|" + e.task));
      let silentSupCount = 0;
      for (const s of (sessOverlapRs.results || [])) {
        // 查该 session 区间前所有 join/leave（判断 startMs 时的 open 状态）
        const jlRs = await env.DB.prepare(
          `SELECT biz, task, badge, session, event, server_ms
           FROM events
           WHERE event IN ('join','leave') AND ok=1 AND session=? AND server_ms < ?
           ORDER BY badge, biz, task, server_ms ASC`
        ).bind(s.session, startMs).all();
        // 按 badge|biz|task 统计净计数 + 记录最后一条 join
        const btNet = {};    // "badge|biz|task" → join_count - leave_count
        const btLastJoin = {}; // "badge|biz|task" → 最近 join 行
        for (const r of (jlRs.results || [])) {
          const btk = r.badge + "|" + r.biz + "|" + r.task;
          if (!btNet[btk]) btNet[btk] = 0;
          if (r.event === "join") {
            btNet[btk]++;
            btLastJoin[btk] = r;
          } else {
            btNet[btk]--;
          }
        }
        // 只补 join_count > leave_count（startMs 时仍 open）的 badge+biz+task
        for (const [btk, net] of Object.entries(btNet)) {
          if (net <= 0) continue; // 已配平或 leave 多于 join，不补
          const lastJoin = btLastJoin[btk];
          if (!lastJoin) continue;
          if (bsbtInEvRows.has(lastJoin.badge + "|" + lastJoin.session + "|" + lastJoin.biz + "|" + lastJoin.task)) continue;
          evRows.push(lastJoin);
          silentSupCount++;
        }
      }

      // 重排序（orphan leave 补 join + silent badge 补 join 后统一排序）
      if (orphanKeys.length > 0 || silentSupCount > 0) {
        evRows.sort((a, b) => {
          if (a.badge < b.badge) return -1; if (a.badge > b.badge) return 1;
          if (a.session < b.session) return -1; if (a.session > b.session) return 1;
          return a.server_ms - b.server_ms;
        });
      }

      // Step2: session_count（按 overlap 统计，跨天 session 每天都计入）, event_wave_count, anomaly_count
      // 按天分组：session 与哪些天有 overlap 就计入哪些天
      const sessCountMap = {}; // day|biz|task → Set of session ids
      for (const s of (sessOverlapRs.results || [])) {
        const sStart = Math.max(s.created_ms || 0, startMs);
        const sEnd = s.closed_ms ? Math.min(s.closed_ms, endMs) : endMs;
        if (sStart > sEnd) continue;
        const mappedTask = mapTask(s.biz, s.task);
        // iterate days this session overlaps
        let cur = sStart;
        while (cur <= sEnd) {
          const day = kstDayOf(cur);
          const nextDayMs = kstDayStartMs(day) + 24 * 3600 * 1000;
          const k = day + "|" + s.biz + "|" + mappedTask;
          if (!sessCountMap[k]) sessCountMap[k] = new Set();
          sessCountMap[k].add(s.session);
          cur = nextDayMs;
        }
      }

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

      // correction_count：统计补录事件（mc-*原子接口 + me-*单条接口 + 旧note='manual_correction'）
      const corrRs = await env.DB.prepare(
        `SELECT substr(datetime(server_ms/1000,'unixepoch','+9 hours'),1,10) as day_kst,
                biz, task, COUNT(*) as correction_count
         FROM events WHERE event IN ('join','leave') AND ok=1
           AND (event_id LIKE 'mc-%' OR event_id LIKE 'me-%' OR note='manual_correction')
           AND server_ms >= ? AND server_ms <= ?
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

      // Step6: WMS 产出 — 换单 (change_order_export, 只统计 已发出, 全 direct 无 allocated)
      const wmsChangeOrderRs = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst,
                COUNT(DISTINCT order_no) as wms_order_count,
                COUNT(DISTINCT order_no) as wms_qty
         FROM wms_outputs
         WHERE source_type='change_order_export'
           AND signed_at='已发出'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();

      // Step7: WMS 产出 — 退件入库 (return_inbound_export, 不过滤状态, 全 direct)
      const wmsReturnInboundRs = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst,
                COUNT(DISTINCT order_no) as wms_order_count,
                SUM(qty) as wms_qty,
                SUM(weight) as wms_weight,
                SUM(volume) as wms_volume
         FROM wms_outputs
         WHERE source_type='return_inbound_export'
           AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''
         GROUP BY completed_day_kst`
      ).bind(start_date, end_date).all();

      // Step8: WMS 产出 — 质检 (return_qc_export, 只统计 已质检, 产出单位=件数)
      const wmsReturnQcRs = await env.DB.prepare(
        `SELECT completed_day_kst as day_kst,
                SUM(qty) as wms_order_count,
                SUM(qty) as wms_qty,
                SUM(weight) as wms_weight,
                SUM(volume) as wms_volume
         FROM wms_outputs
         WHERE source_type='return_qc_export'
           AND signed_at='已质检'
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
            wms_qty: 0, wms_box_count: 0, wms_pallet_count: 0, wms_weight: 0, wms_volume: 0,
            relocated_package_count: 0, relocation_rate: 0, relocation_type_summary: "",
            final_location_type_summary: "", final_location_unknown_count: 0,
            wms_order_count_direct: 0, wms_order_count_allocated: 0,
            wms_qty_direct: 0, wms_qty_allocated: 0,
            anomaly_count: 0, correction_count: 0, efficiency_per_person_hour: 0,
            source_summary: "", updated_ms: now
          };
        }
        return featureMap[k];
      }

      // 预生成关键任务种子行：即使当天完全无数据也保证有全 0 行
      {
        const seedTasks = [
          { biz: "B2C", task: "B2C拣货" },
          { biz: "B2C", task: "B2C打包" },
          { biz: "进口", task: "过机扫描码托" },
          { biz: "B2C", task: "换单" },
          { biz: "B2C", task: "退件入库" },
          { biz: "B2C", task: "质检" },
          { biz: "B2B", task: "B2B工单操作" }
        ];
        const sd = new Date(start_date + "T00:00:00Z");
        const ed = new Date(end_date + "T00:00:00Z");
        while (sd <= ed) {
          const ds = sd.getUTCFullYear() + "-" + String(sd.getUTCMonth()+1).padStart(2,"0") + "-" + String(sd.getUTCDate()).padStart(2,"0");
          for (const st of seedTasks) getOrCreate(ds, st.biz, st.task);
          sd.setUTCDate(sd.getUTCDate() + 1);
        }
      }

      // join/leave 配对计算工时（按 KST 自然日切片）
      // 按 badge+session+biz+task 聚合，避免同 badge 同 session 不同 task 的 join/leave 串配
      const laborMap = {}; // badge|session|biz|task → [{event, server_ms, biz, task}]
      for (const e of evRows) {
        const key = e.badge + "|" + e.session + "|" + e.biz + "|" + e.task;
        if (!laborMap[key]) laborMap[key] = [];
        laborMap[key].push(e);
      }

      const workersByDayBizTask = {}; // day|biz|task → Set of badges
      for (const [, events] of Object.entries(laborMap)) {
        let pendingJoin = null;
        for (const e of events) {
          if (e.event === "join") {
            pendingJoin = e;
          } else if (e.event === "leave" && pendingJoin) {
            const totalMin = (e.server_ms - pendingJoin.server_ms) / 60000;
            if (totalMin > 0 && totalMin < 1440) {
              const mappedTask = mapTask(pendingJoin.biz, pendingJoin.task);
              const slices = sliceLaborByDay_(pendingJoin.server_ms, e.server_ms, startMs, endMs);
              for (const sl of slices) {
                const f = getOrCreate(sl.day_kst, pendingJoin.biz, mappedTask);
                f.total_person_minutes += sl.minutes;
                const wk = sl.day_kst + "|" + pendingJoin.biz + "|" + mappedTask;
                if (!workersByDayBizTask[wk]) workersByDayBizTask[wk] = new Set();
                workersByDayBizTask[wk].add(pendingJoin.badge);
              }
            }
            pendingJoin = null;
          }
        }
        // ★ open session：join 无对应 leave，视为在岗到 endMs，按 endMs 截止切片
        if (pendingJoin) {
          const mappedTask = mapTask(pendingJoin.biz, pendingJoin.task);
          const slices = sliceLaborByDay_(pendingJoin.server_ms, endMs, startMs, endMs);
          for (const sl of slices) {
            const f = getOrCreate(sl.day_kst, pendingJoin.biz, mappedTask);
            f.total_person_minutes += sl.minutes;
            const wk = sl.day_kst + "|" + pendingJoin.biz + "|" + mappedTask;
            if (!workersByDayBizTask[wk]) workersByDayBizTask[wk] = new Set();
            workersByDayBizTask[wk].add(pendingJoin.badge);
          }
        }
      }

      // unique_workers
      for (const [k, badges] of Object.entries(workersByDayBizTask)) {
        const [day, biz, task] = k.split("|");
        const f = getOrCreate(day, biz, task);
        f.unique_workers = badges.size;
      }

      // session_count（overlap 口径）
      for (const [k, sessions] of Object.entries(sessCountMap)) {
        const [day, biz, task] = k.split("|");
        const f = getOrCreate(day, biz, task);
        f.session_count = sessions.size;
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

      // correction_count
      for (const r of (corrRs.results || [])) {
        const f = getOrCreate(r.day_kst, r.biz, mapTask(r.biz, r.task));
        f.correction_count = r.correction_count;
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

      // WMS: 换单 (全部为 direct，无 allocated，只统计 已发出)
      for (const r of (wmsChangeOrderRs.results || [])) {
        const f = getOrCreate(r.day_kst, "B2C", "换单");
        f.wms_order_count_direct += (r.wms_order_count || 0);
        f.wms_qty_direct += (r.wms_qty || 0);
        f.source_summary = "change_order_export";
      }

      // WMS: 退件入库 (全部为 direct，无 allocated，不过滤状态)
      for (const r of (wmsReturnInboundRs.results || [])) {
        const f = getOrCreate(r.day_kst, "B2C", "退件入库");
        f.wms_order_count_direct += (r.wms_order_count || 0);
        f.wms_qty_direct += (r.wms_qty || 0);
        f.wms_weight += (r.wms_weight || 0);
        f.wms_volume += (r.wms_volume || 0);
        f.source_summary = "return_inbound_export";
      }

      // WMS: 质检 (全部为 direct，无 allocated，只统计 已质检，产出单位=件数)
      for (const r of (wmsReturnQcRs.results || [])) {
        const f = getOrCreate(r.day_kst, "B2C", "质检");
        f.wms_order_count_direct += (r.wms_order_count || 0);
        f.wms_qty_direct += (r.wms_qty || 0);
        f.wms_weight += (r.wms_weight || 0);
        f.wms_volume += (r.wms_volume || 0);
        f.source_summary = "return_qc_export";
      }

      // Step9: B2B工单操作 — 以 completed 结果单为主产出来源
      // 口径：b2b_operation_results.status='completed'，按 day_kst 聚合
      // 涵盖 internal_b2b_workorder + external_wms_workorder
      {
        // 9a: 聚合 completed 结果单产出
        const b2bResultRs = await env.DB.prepare(
          `SELECT day_kst,
                  COUNT(*) as completed_count,
                  SUM(COALESCE(packed_qty, 0)) as packed_qty_sum,
                  SUM(COALESCE(box_count, 0)) as box_count_sum,
                  SUM(COALESCE(pallet_count, 0)) as pallet_count_sum,
                  SUM(COALESCE(label_count, 0)) as label_count_sum,
                  SUM(COALESCE(rebox_count, 0)) as rebox_count_sum,
                  SUM(COALESCE(forklift_pallet_count, 0)) as forklift_pallet_count_sum,
                  SUM(COALESCE(rack_pick_location_count, 0)) as rack_pick_location_count_sum
           FROM b2b_operation_results
           WHERE status='completed'
             AND day_kst >= ? AND day_kst <= ?
           GROUP BY day_kst`
        ).bind(start_date, end_date).all();

        for (const r of (b2bResultRs.results || [])) {
          const f = getOrCreate(r.day_kst, "B2B", "B2B工单操作");
          f.wms_order_count_direct += (r.completed_count || 0);
          f.wms_qty_direct += (r.packed_qty_sum || 0);
          f.wms_box_count += (r.box_count_sum || 0);
          f.wms_pallet_count += (r.pallet_count_sum || 0);
          f.source_summary = "b2b_operation_results_completed";
        }

        // 9b: 重量/体积 — 仅 completed 且 source_type='internal_b2b_workorder' 的结果单
        //     通过 internal_workorder_id 关联 b2b_workorders 取重量体积
        //     external_wms_workorder 无重量体积来源，允许为 0
        const b2bWeightRs = await env.DB.prepare(
          `SELECT r.day_kst,
                  SUM(COALESCE(w.total_weight_kg, 0)) as weight_sum,
                  SUM(COALESCE(w.total_cbm, 0)) as volume_sum
           FROM b2b_operation_results r
           JOIN b2b_workorders w ON w.workorder_id = r.internal_workorder_id
           WHERE r.status='completed'
             AND r.source_type='internal_b2b_workorder'
             AND r.internal_workorder_id IS NOT NULL
             AND r.day_kst >= ? AND r.day_kst <= ?
           GROUP BY r.day_kst`
        ).bind(start_date, end_date).all();

        for (const r of (b2bWeightRs.results || [])) {
          const f = getOrCreate(r.day_kst, "B2B", "B2B工单操作");
          f.wms_weight += (r.weight_sum || 0);
          f.wms_volume += (r.volume_sum || 0);
        }
      }

      // 货位影响因子
      {
        // 辅助函数：解析储位号串，返回所有有效货位编码
        function parseLocCodes(locStr) {
          if (!locStr) return [];
          const parts = String(locStr).split(";").filter(p => p.trim());
          return parts.map(p => p.split(":")[0].trim()).filter(c => c);
        }

        // --- 退件入库：只看最终入库货位类型 ---
        {
          const rawRs = await env.DB.prepare(
            `SELECT order_no, location_code, completed_day_kst FROM wms_outputs
             WHERE source_type='return_inbound_export'
               AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''`
          ).bind(start_date, end_date).all();

          const dayPkgMap = {}; // day → { order_no → location_code }
          for (const r of (rawRs.results || [])) {
            const dk = r.completed_day_kst;
            if (!dayPkgMap[dk]) dayPkgMap[dk] = {};
            if (!dayPkgMap[dk][r.order_no]) dayPkgMap[dk][r.order_no] = r.location_code;
          }

          // 收集所有最终货位编码
          const allLocCodes = new Set();
          for (const pkgs of Object.values(dayPkgMap)) {
            for (const loc of Object.values(pkgs)) {
              const codes = parseLocCodes(loc);
              if (codes.length > 0) allLocCodes.add(codes[codes.length - 1]);
            }
          }

          // 批量查 location_types
          const locTypeMap = {};
          const locArr = Array.from(allLocCodes);
          for (let li = 0; li < locArr.length; li += 50) {
            const chunk = locArr.slice(li, li + 50);
            const ph = chunk.map(() => "?").join(",");
            const ltRs = await env.DB.prepare(
              `SELECT location_code, location_type FROM location_types WHERE location_code IN (${ph})`
            ).bind(...chunk).all();
            for (const row of (ltRs.results || [])) locTypeMap[row.location_code] = row.location_type;
          }

          // 按天统计最终货位类型分布
          for (const [day, pkgs] of Object.entries(dayPkgMap)) {
            const f = getOrCreate(day, "B2C", "退件入库");
            let largeCount = 0, smallCount = 0, unknownCount = 0;

            for (const [, loc] of Object.entries(pkgs)) {
              const codes = parseLocCodes(loc);
              if (codes.length === 0) { unknownCount++; continue; }
              const finalCode = codes[codes.length - 1];
              const locType = locTypeMap[finalCode] || "未知";
              if (locType === "大货位") largeCount++;
              else if (locType === "小货位") smallCount++;
              else unknownCount++;
            }

            const parts = [];
            if (largeCount > 0) parts.push("大货位:" + largeCount);
            if (smallCount > 0) parts.push("小货位:" + smallCount);
            if (unknownCount > 0) parts.push("未知:" + unknownCount);
            f.final_location_type_summary = parts.join(",");
            f.final_location_unknown_count = unknownCount;
            // 退件入库不计算换位指标，保持 0/空
          }
        }

        // --- 质检：保持换位轨迹逻辑不变 ---
        {
          const rawRs = await env.DB.prepare(
            `SELECT order_no, location_code, completed_day_kst FROM wms_outputs
             WHERE source_type='return_qc_export' AND signed_at='已质检'
               AND completed_day_kst >= ? AND completed_day_kst <= ? AND completed_day_kst != ''`
          ).bind(start_date, end_date).all();

          const dayPkgMap = {};
          for (const r of (rawRs.results || [])) {
            const dk = r.completed_day_kst;
            if (!dayPkgMap[dk]) dayPkgMap[dk] = {};
            if (!dayPkgMap[dk][r.order_no]) dayPkgMap[dk][r.order_no] = r.location_code;
          }

          const allLocCodes = new Set();
          for (const pkgs of Object.values(dayPkgMap)) {
            for (const loc of Object.values(pkgs)) {
              const codes = parseLocCodes(loc);
              if (codes.length >= 2) { allLocCodes.add(codes[codes.length - 2]); allLocCodes.add(codes[codes.length - 1]); }
            }
          }

          const locTypeMap = {};
          const locArr = Array.from(allLocCodes);
          for (let li = 0; li < locArr.length; li += 50) {
            const chunk = locArr.slice(li, li + 50);
            const ph = chunk.map(() => "?").join(",");
            const ltRs = await env.DB.prepare(
              `SELECT location_code, location_type FROM location_types WHERE location_code IN (${ph})`
            ).bind(...chunk).all();
            for (const row of (ltRs.results || [])) locTypeMap[row.location_code] = row.location_type;
          }

          for (const [day, pkgs] of Object.entries(dayPkgMap)) {
            const f = getOrCreate(day, "B2C", "质检");
            let totalPkgs = 0, relocatedPkgs = 0;
            const typeCounts = {};

            for (const [, loc] of Object.entries(pkgs)) {
              totalPkgs++;
              const codes = parseLocCodes(loc);
              if (codes.length >= 2 && codes[codes.length - 2] !== codes[codes.length - 1]) {
                relocatedPkgs++;
                const fromType = locTypeMap[codes[codes.length - 2]] || "未知";
                const toType = locTypeMap[codes[codes.length - 1]] || "未知";
                const key = fromType + "→" + toType;
                typeCounts[key] = (typeCounts[key] || 0) + 1;
              }
            }

            f.relocated_package_count = relocatedPkgs;
            f.relocation_rate = totalPkgs > 0 ? Math.round((relocatedPkgs / totalPkgs) * 100) / 100 : 0;
            f.relocation_type_summary = Object.entries(typeCounts).map(([k, v]) => k + ":" + v).join(",");
          }
        }
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
          event_wave_count,wms_wave_count,wms_order_count,wms_qty,wms_box_count,wms_pallet_count,wms_weight,wms_volume,
          wms_order_count_direct,wms_order_count_allocated,wms_qty_direct,wms_qty_allocated,
          anomaly_count,correction_count,efficiency_per_person_hour,source_summary,updated_ms,
          relocated_package_count,relocation_rate,relocation_type_summary,
          final_location_type_summary,final_location_unknown_count)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
      );
      const insStmts = [];
      const features = Object.values(featureMap);
      for (const f of features) {
        insStmts.push(insStmt.bind(
          f.day_kst, f.biz, f.task, Math.round(f.total_person_minutes * 100)/100, f.unique_workers, f.session_count,
          f.event_wave_count, f.wms_wave_count, f.wms_order_count, f.wms_qty, f.wms_box_count, f.wms_pallet_count,
          Math.round(f.wms_weight * 100)/100, Math.round(f.wms_volume * 10000)/10000,
          f.wms_order_count_direct, f.wms_order_count_allocated, f.wms_qty_direct, f.wms_qty_allocated,
          f.anomaly_count, f.correction_count, f.efficiency_per_person_hour, f.source_summary, now,
          f.relocated_package_count, f.relocation_rate, f.relocation_type_summary,
          f.final_location_type_summary, f.final_location_unknown_count
        ));
      }
      if (insStmts.length > 0) await env.DB.batch(insStmts);

      // post_warnings: 检查 6 个关键任务
      const post_warnings = [];
      const KEY_TASKS = [
        { biz: "B2C", task: "B2C拣货" },
        { biz: "B2C", task: "B2C打包" },
        { biz: "进口", task: "过机扫描码托" },
        { biz: "B2C", task: "换单" },
        { biz: "B2C", task: "退件入库" },
        { biz: "B2C", task: "质检" }
      ];
      // 收集该日期范围内 b2c_pack_import 缺失的天
      const packMissingDays = new Set();
      {
        const daysInRange = new Set();
        // 纯 UTC 日期迭代，避免时区偏移导致日期减一天
        const dd = new Date(start_date + "T00:00:00Z");
        const ddEnd = new Date(end_date + "T00:00:00Z");
        while (dd <= ddEnd) {
          const ds = dd.getUTCFullYear() + "-" + String(dd.getUTCMonth()+1).padStart(2,"0") + "-" + String(dd.getUTCDate()).padStart(2,"0");
          daysInRange.add(ds);
          dd.setUTCDate(dd.getUTCDate() + 1);
        }
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

    // ===== B2B 入库计划 =====
    if (action === "b2b_plan_create") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const req_id = String(p.request_id || "").trim();
      const plan_day = String(p.plan_day || "").trim();
      const customer_name = String(p.customer_name || "").trim();
      const biz_type = String(p.biz_type || "").trim();
      const goods_summary = String(p.goods_summary || "").trim();
      const expected_arrival_time = String(p.expected_arrival_time || "").trim();
      const purpose_text = String(p.purpose_text || "").trim();
      const remark = String(p.remark || "").trim();
      const created_by = String(p.created_by || "").trim();

      const BIZ_NEW = ["b2c","b2b","inventory_op","return_op"];
      if (!plan_day || !/^\d{4}-\d{2}-\d{2}$/.test(plan_day)) return jsonpOrJson({ ok:false, error:"invalid plan_day" }, callback);
      if (!customer_name) return jsonpOrJson({ ok:false, error:"missing customer_name" }, callback);
      if (!goods_summary) return jsonpOrJson({ ok:false, error:"missing goods_summary" }, callback);
      if (!BIZ_NEW.includes(biz_type)) return jsonpOrJson({ ok:false, error:"invalid biz_type, must be: " + BIZ_NEW.join("/") }, callback);
      if (!created_by) return jsonpOrJson({ ok:false, error:"missing created_by" }, callback);

      // 幂等 claim（校验通过后才占位）
      if (req_id) {
        const dup = await env.DB.prepare(`SELECT response_json FROM api_idempotency_keys WHERE action='b2b_plan_create' AND request_id=?`).bind(req_id).first();
        if (dup && dup.response_json) return jsonpOrJson(JSON.parse(dup.response_json), callback);
        if (dup) return jsonpOrJson({ ok:false, error:"request_in_progress", retryable:true }, callback);
        const ins = await env.DB.prepare(`INSERT OR IGNORE INTO api_idempotency_keys(action,request_id,created_at) VALUES('b2b_plan_create',?,?)`).bind(req_id, Date.now()).run();
        if (!ins.meta?.changes) {
          const dup2 = await env.DB.prepare(`SELECT response_json FROM api_idempotency_keys WHERE action='b2b_plan_create' AND request_id=?`).bind(req_id).first();
          if (dup2 && dup2.response_json) return jsonpOrJson(JSON.parse(dup2.response_json), callback);
          return jsonpOrJson({ ok:false, error:"request_in_progress", retryable:true }, callback);
        }
      }

      let plan_id = null;
      try {
        // 生成 plan_id：IP-YYMMDD-NNN（后端原子生成，避免撞号）
        const dayTag = plan_day.slice(2).replace(/-/g, ""); // "260316"
        const maxRow = await env.DB.prepare(
          `SELECT plan_id FROM b2b_inbound_plans WHERE plan_id LIKE ? ORDER BY plan_id DESC LIMIT 1`
        ).bind("IP-" + dayTag + "-%").first();
        let seq = 1;
        if (maxRow && maxRow.plan_id) {
          const parts = maxRow.plan_id.split("-");
          seq = (parseInt(parts[2], 10) || 0) + 1;
        }
        plan_id = "IP-" + dayTag + "-" + String(seq).padStart(3, "0");

        await env.DB.prepare(
          `INSERT INTO b2b_inbound_plans(plan_id,plan_day,customer_name,biz_type,goods_summary,expected_arrival_time,purpose_text,remark,status,created_by,created_at)
           VALUES(?,?,?,?,?,?,?,?,'pending',?,?)`
        ).bind(plan_id, plan_day, customer_name, biz_type, goods_summary, expected_arrival_time, purpose_text, remark, created_by, now).run();

        const respPlan = { ok:true, plan_id };
        if (req_id) {
          await env.DB.prepare(`UPDATE api_idempotency_keys SET result_id=?, response_json=? WHERE action='b2b_plan_create' AND request_id=?`).bind(plan_id, JSON.stringify(respPlan), req_id).run();
        }
        return jsonpOrJson(respPlan, callback);
      } catch(e) {
        if (req_id) {
          const exists = plan_id && await env.DB.prepare(`SELECT 1 FROM b2b_inbound_plans WHERE plan_id=?`).bind(plan_id).first();
          if (exists) {
            const respPlan = { ok:true, plan_id };
            try { await env.DB.prepare(`UPDATE api_idempotency_keys SET result_id=?, response_json=? WHERE action='b2b_plan_create' AND request_id=?`).bind(plan_id, JSON.stringify(respPlan), req_id).run(); } catch(_){}
            return jsonpOrJson(respPlan, callback);
          }
          await env.DB.prepare(`DELETE FROM api_idempotency_keys WHERE action='b2b_plan_create' AND request_id=? AND response_json=''`).bind(req_id).run();
        }
        throw e;
      }
    }

    if (action === "b2b_plan_list") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const start_day = String(p.start_day || "").trim();
      const end_day = String(p.end_day || "").trim();
      if (!start_day || !end_day) return jsonpOrJson({ ok:false, error:"missing start_day or end_day" }, callback);

      const rs = await env.DB.prepare(
        `SELECT * FROM b2b_inbound_plans WHERE plan_day >= ? AND plan_day <= ? ORDER BY plan_day ASC, created_at ASC`
      ).bind(start_day, end_day).all();
      return jsonpOrJson({ ok:true, plans: rs.results || [] }, callback);
    }

    if (action === "b2b_plan_update_status") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const plan_id = String(p.plan_id || "").trim();
      const status = String(p.status || "").trim();
      const updated_by = String(p.updated_by || "").trim();

      if (!plan_id) return jsonpOrJson({ ok:false, error:"missing plan_id" }, callback);
      const VALID = ["pending","arrived","processing","completed","abnormal","cancelled"];
      if (!VALID.includes(status)) return jsonpOrJson({ ok:false, error:"invalid status, must be: " + VALID.join("/") }, callback);

      const existing = await env.DB.prepare(`SELECT plan_id, status FROM b2b_inbound_plans WHERE plan_id=?`).bind(plan_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"plan_id not found" }, callback);

      // cancelled 是终态，不可再改
      if (existing.status === "cancelled") return jsonpOrJson({ ok:false, error:"plan already cancelled" }, callback);

      await env.DB.prepare(
        `UPDATE b2b_inbound_plans SET status=?, status_updated_by=?, status_updated_at=? WHERE plan_id=?`
      ).bind(status, updated_by, now, plan_id).run();

      return jsonpOrJson({ ok:true, plan_id, status }, callback);
    }

    // ===== B2B 入库计划编辑（不改 status） =====
    if (action === "b2b_plan_update") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const plan_id = String(p.plan_id || "").trim();
      if (!plan_id) return jsonpOrJson({ ok:false, error:"missing plan_id" }, callback);

      const existing = await env.DB.prepare(`SELECT plan_id, status FROM b2b_inbound_plans WHERE plan_id=?`).bind(plan_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"plan_id not found" }, callback);

      // 只有非终态允许编辑
      if (existing.status === "completed") return jsonpOrJson({ ok:false, error:"completed plan cannot be edited" }, callback);
      if (existing.status === "cancelled") return jsonpOrJson({ ok:false, error:"cancelled plan cannot be edited" }, callback);

      const plan_day = String(p.plan_day || "").trim();
      const customer_name = String(p.customer_name || "").trim();
      const biz_type = String(p.biz_type || "").trim();
      const goods_summary = String(p.goods_summary || "").trim();
      const expected_arrival_time = String(p.expected_arrival_time || "").trim();
      const purpose_text = String(p.purpose_text || "").trim();
      const remark = String(p.remark || "").trim();

      const BIZ_ALL = ["b2c","b2b","inventory_op","return_op","b2c_inbound","b2b_inbound","direct_transfer","other"];
      if (!plan_day || !/^\d{4}-\d{2}-\d{2}$/.test(plan_day)) return jsonpOrJson({ ok:false, error:"invalid plan_day" }, callback);
      if (!customer_name) return jsonpOrJson({ ok:false, error:"missing customer_name" }, callback);
      if (!goods_summary) return jsonpOrJson({ ok:false, error:"missing goods_summary" }, callback);
      if (!BIZ_ALL.includes(biz_type)) return jsonpOrJson({ ok:false, error:"invalid biz_type" }, callback);

      await env.DB.prepare(
        `UPDATE b2b_inbound_plans SET plan_day=?, customer_name=?, biz_type=?, goods_summary=?, expected_arrival_time=?, purpose_text=?, remark=? WHERE plan_id=?`
      ).bind(plan_day, customer_name, biz_type, goods_summary, expected_arrival_time, purpose_text, remark, plan_id).run();

      return jsonpOrJson({ ok:true, plan_id }, callback);
    }

    // ===== 入库计划 记帐标记 =====
    if (action === "b2b_plan_set_accounted") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const plan_id = String(p.plan_id || "").trim();
      const is_accounted = parseInt(p.is_accounted) || 0;
      const accounted_by = String(p.accounted_by || "").trim();
      if (!plan_id) return jsonpOrJson({ ok:false, error:"missing plan_id" }, callback);
      if (is_accounted && !accounted_by) return jsonpOrJson({ ok:false, error:"accounted_by required" }, callback);

      const plan = await env.DB.prepare(`SELECT plan_id FROM b2b_inbound_plans WHERE plan_id=?`).bind(plan_id).first();
      if (!plan) return jsonpOrJson({ ok:false, error:"plan not found" }, callback);

      if (is_accounted) {
        await env.DB.prepare(`UPDATE b2b_inbound_plans SET is_accounted=1, accounted_at=?, accounted_by=? WHERE plan_id=?`)
          .bind(Date.now(), accounted_by, plan_id).run();
      } else {
        await env.DB.prepare(`UPDATE b2b_inbound_plans SET is_accounted=0, accounted_at=NULL, accounted_by='' WHERE plan_id=?`)
          .bind(plan_id).run();
      }
      return jsonpOrJson({ ok:true, plan_id, is_accounted }, callback);
    }

    // ===== B2B 出库作业单 =====
    if (action === "b2b_wo_create") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const req_id_wo = String(p.request_id || "").trim();

      // 三模式字段（向后兼容：旧前端可能只传 outbound_mode=sku_based/carton_based）
      let detail_mode = String(p.detail_mode || "").trim();
      let operation_mode = String(p.operation_mode || "").trim();
      let outbound_mode = String(p.outbound_mode || "").trim();
      // 向后兼容：旧前端传 outbound_mode=sku_based/carton_based 时自动映射
      if (["sku_based","carton_based"].includes(outbound_mode) && !detail_mode) {
        detail_mode = outbound_mode;
        outbound_mode = "";
      }
      if (!detail_mode || !["sku_based","carton_based"].includes(detail_mode))
        return jsonpOrJson({ ok:false, error:"invalid detail_mode, must be sku_based or carton_based" }, callback);

      const customer_name = String(p.customer_name || "").trim();
      const plan_day = String(p.plan_day || "").trim();
      const external_workorder_no = String(p.external_workorder_no || "").trim();
      const planned_start_at = String(p.planned_start_at || "").trim();
      const planned_end_at = String(p.planned_end_at || "").trim();
      const instruction_text = String(p.instruction_text || "").trim();
      const created_by = String(p.created_by || "").trim();
      const outbound_destination = String(p.outbound_destination || "").trim();
      const order_ref_no = String(p.order_ref_no || "").trim();
      const outbound_box_count = Number(p.outbound_box_count || 0);
      const outbound_pallet_count = Number(p.outbound_pallet_count || 0);
      const lines = p.lines; // array of line objects

      if (!customer_name) return jsonpOrJson({ ok:false, error:"missing customer_name" }, callback);
      if (!plan_day || !/^\d{4}-\d{2}-\d{2}$/.test(plan_day)) return jsonpOrJson({ ok:false, error:"invalid plan_day" }, callback);
      if (!created_by) return jsonpOrJson({ ok:false, error:"missing created_by" }, callback);
      if (outbound_box_count < 0 || outbound_pallet_count < 0) return jsonpOrJson({ ok:false, error:"outbound counts cannot be negative" }, callback);
      if (detail_mode === "carton_based") {
        if (outbound_box_count <= 0 && outbound_pallet_count <= 0) return jsonpOrJson({ ok:false, error:"carton_based requires outbound_box_count or outbound_pallet_count > 0" }, callback);
        if (!Array.isArray(lines)) return jsonpOrJson({ ok:false, error:"lines must be array" }, callback);
      } else {
        if (!Array.isArray(lines) || lines.length === 0) return jsonpOrJson({ ok:false, error:"at least 1 line required" }, callback);
      }

      // 幂等 claim（校验通过后才占位）
      if (req_id_wo) {
        const dup = await env.DB.prepare(`SELECT response_json FROM api_idempotency_keys WHERE action='b2b_wo_create' AND request_id=?`).bind(req_id_wo).first();
        if (dup && dup.response_json) return jsonpOrJson(JSON.parse(dup.response_json), callback);
        if (dup) return jsonpOrJson({ ok:false, error:"request_in_progress", retryable:true }, callback);
        const ins = await env.DB.prepare(`INSERT OR IGNORE INTO api_idempotency_keys(action,request_id,created_at) VALUES('b2b_wo_create',?,?)`).bind(req_id_wo, Date.now()).run();
        if (!ins.meta?.changes) {
          const dup2 = await env.DB.prepare(`SELECT response_json FROM api_idempotency_keys WHERE action='b2b_wo_create' AND request_id=?`).bind(req_id_wo).first();
          if (dup2 && dup2.response_json) return jsonpOrJson(JSON.parse(dup2.response_json), callback);
          return jsonpOrJson({ ok:false, error:"request_in_progress", retryable:true }, callback);
        }
      }

      let workorder_id = null;
      try {
        // 生成 workorder_id：WO-YYMMDD-NNN
        const dayTag = plan_day.slice(2).replace(/-/g, "");
        const maxRow = await env.DB.prepare(
          `SELECT workorder_id FROM b2b_workorders WHERE workorder_id LIKE ? ORDER BY workorder_id DESC LIMIT 1`
        ).bind("WO-" + dayTag + "-%").first();
        let seq = 1;
        if (maxRow && maxRow.workorder_id) {
          const parts = maxRow.workorder_id.split("-");
          seq = (parseInt(parts[2], 10) || 0) + 1;
        }
        workorder_id = "WO-" + dayTag + "-" + String(seq).padStart(3, "0");

        // 汇总明细 + 构造 batch 语句
        let total_qty = 0, total_weight_kg = 0, total_cbm = 0;
        const line_type = detail_mode === "carton_based" ? "carton" : "sku";
        const total_qty_unit = detail_mode === "carton_based" ? "箱" : "件";
        const batchStmts = [];

        for (let i = 0; i < lines.length; i++) {
          const ln = lines[i];
          const qty = Number(ln.qty || 0);
          const w = Number(ln.weight_kg || 0);
          const l = Number(ln.length_cm || 0);
          const wd = Number(ln.width_cm || 0);
          const h = Number(ln.height_cm || 0);
          total_qty += qty;
          total_weight_kg += w;
          if (l > 0 && wd > 0 && h > 0) total_cbm += (l * wd * h) / 1000000;

          batchStmts.push(
            env.DB.prepare(
              `INSERT INTO b2b_workorder_lines(workorder_id,line_no,line_type,sku_code,product_name,carton_no,qty,length_cm,width_cm,height_cm,weight_kg,remark)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
            ).bind(
              workorder_id, i + 1, line_type,
              String(ln.sku_code || ""), String(ln.product_name || ""), String(ln.carton_no || ""),
              qty, l, wd, h, w, String(ln.remark || "")
            )
          );
        }

        total_cbm = Math.round(total_cbm * 1000) / 1000;
        total_weight_kg = Math.round(total_weight_kg * 1000) / 1000;

        batchStmts.push(
          env.DB.prepare(
            `INSERT INTO b2b_workorders(workorder_id,external_workorder_no,outbound_mode,detail_mode,operation_mode,status,customer_name,plan_day,planned_start_at,planned_end_at,total_qty,total_qty_unit,total_weight_kg,total_cbm,instruction_text,created_by,created_at,outbound_destination,order_ref_no,outbound_box_count,outbound_pallet_count)
             VALUES(?,?,?,?,?,'draft',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
          ).bind(
            workorder_id, external_workorder_no, outbound_mode, detail_mode, operation_mode,
            customer_name,
            plan_day, planned_start_at, planned_end_at,
            total_qty, total_qty_unit, total_weight_kg, total_cbm,
            instruction_text, created_by, now,
            outbound_destination, order_ref_no, outbound_box_count, outbound_pallet_count
          )
        );

        await env.DB.batch(batchStmts);

        const respWo = { ok:true, workorder_id, lines_count: lines.length, total_qty, total_weight_kg, total_cbm };
        if (req_id_wo) {
          await env.DB.prepare(`UPDATE api_idempotency_keys SET result_id=?, response_json=? WHERE action='b2b_wo_create' AND request_id=?`).bind(workorder_id, JSON.stringify(respWo), req_id_wo).run();
        }
        return jsonpOrJson(respWo, callback);
      } catch(e) {
        if (req_id_wo) {
          const woRow = workorder_id && await env.DB.prepare(`SELECT total_qty, total_weight_kg, total_cbm FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
          const lineCnt = woRow && await env.DB.prepare(`SELECT COUNT(*) AS c FROM b2b_workorder_lines WHERE workorder_id=?`).bind(workorder_id).first();
          if (woRow && lineCnt && lineCnt.c === lines.length) {
            const respWo = { ok:true, workorder_id, lines_count: lines.length, total_qty: woRow.total_qty, total_weight_kg: woRow.total_weight_kg, total_cbm: woRow.total_cbm };
            try { await env.DB.prepare(`UPDATE api_idempotency_keys SET result_id=?, response_json=? WHERE action='b2b_wo_create' AND request_id=?`).bind(workorder_id, JSON.stringify(respWo), req_id_wo).run(); } catch(_){}
            return jsonpOrJson(respWo, callback);
          }
          await env.DB.prepare(`DELETE FROM api_idempotency_keys WHERE action='b2b_wo_create' AND request_id=? AND response_json=''`).bind(req_id_wo).run();
        }
        throw e;
      }
    }

    if (action === "b2b_wo_list") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const start_day = String(p.start_day || "").trim();
      const end_day = String(p.end_day || "").trim();
      if (!start_day || !end_day) return jsonpOrJson({ ok:false, error:"missing start_day or end_day" }, callback);

      let where = "WHERE plan_day >= ? AND plan_day <= ?";
      const binds = [start_day, end_day];
      const statusFilter = String(p.status || "").trim();
      if (statusFilter) { where += " AND status=?"; binds.push(statusFilter); }

      const rs = await env.DB.prepare(
        `SELECT * FROM b2b_workorders ${where} ORDER BY plan_day ASC, created_at ASC`
      ).bind(...binds).all();
      return jsonpOrJson({ ok:true, workorders: rs.results || [] }, callback);
    }

    if (action === "b2b_wo_detail") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const workorder_id = String(p.workorder_id || "").trim();
      if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);

      const wo = await env.DB.prepare(`SELECT * FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
      if (!wo) return jsonpOrJson({ ok:false, error:"workorder not found" }, callback);

      const linesRs = await env.DB.prepare(
        `SELECT * FROM b2b_workorder_lines WHERE workorder_id=? ORDER BY line_no ASC`
      ).bind(workorder_id).all();

      const attRs = await env.DB.prepare(
        `SELECT attachment_id, workorder_id, file_name, file_size, content_type, sort_order, uploaded_by, created_at
         FROM b2b_workorder_attachments WHERE workorder_id=? ORDER BY sort_order ASC`
      ).bind(workorder_id).all();

      return jsonpOrJson({ ok:true, workorder: wo, lines: linesRs.results || [], attachments: attRs.results || [] }, callback);
    }

    // ===== B2B 出库作业单编辑（仅 draft 状态，原子 batch） =====
    if (action === "b2b_wo_update") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const workorder_id = String(p.workorder_id || "").trim();
      if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);

      const existing = await env.DB.prepare(`SELECT workorder_id, status FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"workorder not found" }, callback);
      const WO_EDITABLE = ["draft","issued","working"];
      if (!WO_EDITABLE.includes(existing.status)) return jsonpOrJson({ ok:false, error:"current status (" + existing.status + ") cannot be edited" }, callback);

      const edited_by = String(p.edited_by || "").trim();

      // 三模式字段
      const detail_mode = String(p.detail_mode || "").trim();
      const operation_mode = String(p.operation_mode || "").trim();
      const outbound_mode = String(p.outbound_mode || "").trim();
      if (!detail_mode || !["sku_based","carton_based"].includes(detail_mode))
        return jsonpOrJson({ ok:false, error:"invalid detail_mode" }, callback);

      const customer_name = String(p.customer_name || "").trim();
      const plan_day = String(p.plan_day || "").trim();
      const external_workorder_no = String(p.external_workorder_no || "").trim();
      const instruction_text = String(p.instruction_text || "").trim();
      const outbound_destination = String(p.outbound_destination || "").trim();
      const order_ref_no = String(p.order_ref_no || "").trim();
      const outbound_box_count = Number(p.outbound_box_count || 0);
      const outbound_pallet_count = Number(p.outbound_pallet_count || 0);
      const lines = p.lines;

      if (!customer_name) return jsonpOrJson({ ok:false, error:"missing customer_name" }, callback);
      if (!plan_day || !/^\d{4}-\d{2}-\d{2}$/.test(plan_day)) return jsonpOrJson({ ok:false, error:"invalid plan_day" }, callback);
      if (outbound_box_count < 0 || outbound_pallet_count < 0) return jsonpOrJson({ ok:false, error:"outbound counts cannot be negative" }, callback);
      if (detail_mode === "carton_based") {
        if (outbound_box_count <= 0 && outbound_pallet_count <= 0) return jsonpOrJson({ ok:false, error:"carton_based requires outbound_box_count or outbound_pallet_count > 0" }, callback);
        if (!Array.isArray(lines)) return jsonpOrJson({ ok:false, error:"lines must be array" }, callback);
      } else {
        if (!Array.isArray(lines) || lines.length === 0) return jsonpOrJson({ ok:false, error:"at least 1 line required" }, callback);
      }

      // 构造原子 batch：删旧明细 → 插新明细 → 更新主表
      const batchStmts = [];

      // 1. 删除旧明细
      batchStmts.push(
        env.DB.prepare(`DELETE FROM b2b_workorder_lines WHERE workorder_id=?`).bind(workorder_id)
      );

      // 2. 插入新明细 + 汇总
      let total_qty = 0, total_weight_kg = 0, total_cbm = 0;
      const line_type = detail_mode === "carton_based" ? "carton" : "sku";
      const total_qty_unit = detail_mode === "carton_based" ? "箱" : "件";

      for (let i = 0; i < lines.length; i++) {
        const ln = lines[i];
        const qty = Number(ln.qty || 0);
        const w = Number(ln.weight_kg || 0);
        const l = Number(ln.length_cm || 0);
        const wd = Number(ln.width_cm || 0);
        const h = Number(ln.height_cm || 0);
        total_qty += qty;
        total_weight_kg += w;
        if (l > 0 && wd > 0 && h > 0) total_cbm += (l * wd * h) / 1000000;

        batchStmts.push(
          env.DB.prepare(
            `INSERT INTO b2b_workorder_lines(workorder_id,line_no,line_type,sku_code,product_name,carton_no,qty,length_cm,width_cm,height_cm,weight_kg,remark)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
          ).bind(
            workorder_id, i + 1, line_type,
            String(ln.sku_code || ""), String(ln.product_name || ""), String(ln.carton_no || ""),
            qty, l, wd, h, w, String(ln.remark || "")
          )
        );
      }

      total_cbm = Math.round(total_cbm * 1000) / 1000;
      total_weight_kg = Math.round(total_weight_kg * 1000) / 1000;

      // 3. 更新主表（不改 status / workorder_id / created_by / created_at）
      // 操作中（issued/working）被编辑时打提醒标记并清空旧确认
      const isActiveEdit = (existing.status === "issued" || existing.status === "working");
      if (isActiveEdit && !edited_by) return jsonpOrJson({ ok:false, error:"missing edited_by" }, callback);
      const noticeSql = isActiveEdit ? ", has_update_notice=1, last_edited_at=" + now + ", last_edited_by=?, update_ack_at=NULL, update_ack_by=''" : "";
      const updateBinds = [
        detail_mode, operation_mode, outbound_mode, customer_name,
        plan_day, external_workorder_no, instruction_text,
        outbound_destination, order_ref_no, outbound_box_count, outbound_pallet_count,
        total_qty, total_qty_unit, total_weight_kg, total_cbm
      ];
      if (isActiveEdit) updateBinds.push(edited_by);
      updateBinds.push(workorder_id);

      batchStmts.push(
        env.DB.prepare(
          `UPDATE b2b_workorders SET detail_mode=?, operation_mode=?, outbound_mode=?, customer_name=?, plan_day=?, external_workorder_no=?, instruction_text=?, outbound_destination=?, order_ref_no=?, outbound_box_count=?, outbound_pallet_count=?, total_qty=?, total_qty_unit=?, total_weight_kg=?, total_cbm=?${noticeSql} WHERE workorder_id=?`
        ).bind(...updateBinds)
      );

      // 原子执行
      await env.DB.batch(batchStmts);

      return jsonpOrJson({ ok:true, workorder_id, lines_count: lines.length, total_qty, total_weight_kg, total_cbm }, callback);
    }

    if (action === "b2b_wo_update_status") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const workorder_id = String(p.workorder_id || "").trim();
      const status = String(p.status || "").trim();
      const updated_by = String(p.updated_by || "").trim();

      if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);
      const VALID = ["draft","issued","working","completed","cancelled"];
      if (!VALID.includes(status)) return jsonpOrJson({ ok:false, error:"invalid status" }, callback);

      const existing = await env.DB.prepare(`SELECT workorder_id, status FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"workorder not found" }, callback);

      // 状态流转校验
      const TRANSITIONS = {
        "draft": ["issued", "cancelled"],
        "issued": ["working", "completed", "cancelled"],
        "working": ["completed"],
        "completed": [],
        "cancelled": []
      };
      const allowed = TRANSITIONS[existing.status] || [];
      if (!allowed.includes(status))
        return jsonpOrJson({ ok:false, error:"cannot change from " + existing.status + " to " + status }, callback);

      // 写入状态 + 对应时间戳
      let tsCol = "";
      if (status === "issued") tsCol = ", issued_at=" + now;
      else if (status === "completed") tsCol = ", completed_at=" + now;
      else if (status === "cancelled") tsCol = ", cancelled_at=" + now + ", has_cancel_notice=1, cancel_ack_at=NULL, cancel_ack_by=''";

      await env.DB.prepare(
        `UPDATE b2b_workorders SET status=?${tsCol} WHERE workorder_id=?`
      ).bind(status, workorder_id).run();

      return jsonpOrJson({ ok:true, workorder_id, status }, callback);
    }

    // ===== B2B 出库作业单 — 提醒确认/取消确认（通用） =====
    if (action === "b2b_wo_ack_notice") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const workorder_id = String(p.workorder_id || "").trim();
      const ack_by = String(p.ack_by || "").trim();
      const kind = String(p.kind || "updated").trim();   // "updated" | "cancelled"
      const op = String(p.op || "ack").trim();            // "ack" | "unack"
      if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);
      if (op === "ack" && !ack_by) return jsonpOrJson({ ok:false, error:"missing ack_by" }, callback);

      const existing = await env.DB.prepare(`SELECT workorder_id, has_update_notice, has_cancel_notice FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"workorder not found" }, callback);

      if (kind === "updated") {
        if (op === "ack") {
          await env.DB.prepare(
            `UPDATE b2b_workorders SET has_update_notice=0, update_ack_at=?, update_ack_by=? WHERE workorder_id=?`
          ).bind(now, ack_by, workorder_id).run();
        } else {
          await env.DB.prepare(
            `UPDATE b2b_workorders SET has_update_notice=1, update_ack_at=NULL, update_ack_by='' WHERE workorder_id=?`
          ).bind(workorder_id).run();
        }
      } else if (kind === "cancelled") {
        if (op === "ack") {
          await env.DB.prepare(
            `UPDATE b2b_workorders SET has_cancel_notice=0, cancel_ack_at=?, cancel_ack_by=? WHERE workorder_id=?`
          ).bind(now, ack_by, workorder_id).run();
        } else {
          await env.DB.prepare(
            `UPDATE b2b_workorders SET has_cancel_notice=1, cancel_ack_at=NULL, cancel_ack_by='' WHERE workorder_id=?`
          ).bind(workorder_id).run();
        }
      } else {
        return jsonpOrJson({ ok:false, error:"invalid kind, must be: updated/cancelled" }, callback);
      }

      return jsonpOrJson({ ok:true, workorder_id, kind, op }, callback);
    }

    // ===== 出库作业单 记帐标记 =====
    if (action === "b2b_wo_set_accounted") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const workorder_id = String(p.workorder_id || "").trim();
      const is_accounted = parseInt(p.is_accounted) || 0;
      const accounted_by = String(p.accounted_by || "").trim();
      if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);
      if (is_accounted && !accounted_by) return jsonpOrJson({ ok:false, error:"accounted_by required" }, callback);

      const wo = await env.DB.prepare(`SELECT workorder_id FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
      if (!wo) return jsonpOrJson({ ok:false, error:"workorder not found" }, callback);

      if (is_accounted) {
        await env.DB.prepare(`UPDATE b2b_workorders SET is_accounted=1, accounted_at=?, accounted_by=? WHERE workorder_id=?`)
          .bind(Date.now(), accounted_by, workorder_id).run();
      } else {
        await env.DB.prepare(`UPDATE b2b_workorders SET is_accounted=0, accounted_at=NULL, accounted_by='' WHERE workorder_id=?`)
          .bind(workorder_id).run();
      }
      return jsonpOrJson({ ok:true, workorder_id, is_accounted }, callback);
    }

    // ===== 车辆信息登记 =====
    if (action === "b2b_wo_set_pickup_info") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const workorder_id = String(p.workorder_id || "").trim();
      const pickup_vehicle_no = String(p.pickup_vehicle_no || "").trim();
      const pickup_driver_name = String(p.pickup_driver_name || "").trim();
      const pickup_driver_phone = String(p.pickup_driver_phone || "").trim();
      const pickup_remark = String(p.pickup_remark || "").trim();
      const pickup_recorded_by = String(p.pickup_recorded_by || "").trim();
      if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);
      if (!pickup_vehicle_no) return jsonpOrJson({ ok:false, error:"missing pickup_vehicle_no" }, callback);
      if (!pickup_recorded_by) return jsonpOrJson({ ok:false, error:"missing pickup_recorded_by" }, callback);

      const wo = await env.DB.prepare(`SELECT status, shipment_confirmed_at FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
      if (!wo) return jsonpOrJson({ ok:false, error:"workorder not found" }, callback);
      if (wo.status !== "completed") return jsonpOrJson({ ok:false, error:"only completed workorders can set pickup info" }, callback);

      await env.DB.prepare(
        `UPDATE b2b_workorders SET pickup_vehicle_no=?, pickup_driver_name=?, pickup_driver_phone=?, pickup_remark=?, pickup_recorded_by=?, pickup_recorded_at=? WHERE workorder_id=?`
      ).bind(pickup_vehicle_no, pickup_driver_name, pickup_driver_phone, pickup_remark, pickup_recorded_by, Date.now(), workorder_id).run();
      return jsonpOrJson({ ok:true, workorder_id }, callback);
    }

    // ===== 确认已发货 =====
    if (action === "b2b_wo_confirm_shipped") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const workorder_id = String(p.workorder_id || "").trim();
      const shipment_confirmed_by = String(p.shipment_confirmed_by || "").trim();
      if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);
      if (!shipment_confirmed_by) return jsonpOrJson({ ok:false, error:"missing shipment_confirmed_by" }, callback);

      const wo = await env.DB.prepare(`SELECT status, pickup_vehicle_no FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
      if (!wo) return jsonpOrJson({ ok:false, error:"workorder not found" }, callback);
      if (wo.status !== "completed") return jsonpOrJson({ ok:false, error:"only completed workorders can confirm shipment" }, callback);
      if (!wo.pickup_vehicle_no) return jsonpOrJson({ ok:false, error:"must set pickup info before confirming shipment" }, callback);

      await env.DB.prepare(
        `UPDATE b2b_workorders SET shipment_confirmed_by=?, shipment_confirmed_at=? WHERE workorder_id=?`
      ).bind(shipment_confirmed_by, Date.now(), workorder_id).run();
      return jsonpOrJson({ ok:true, workorder_id }, callback);
    }

    // ===== B2B 附件上传（multipart/form-data） =====
    if (action === "b2b_attachment_upload") {
      // multipart 请求需要重新解析 formData
      const formData = await request.formData().catch(() => null);
      if (!formData) return jsonpOrJson({ ok:false, error:"invalid multipart body" }, callback);

      const k = formData.get("k") || "";
      const pAuth = { k };
      if (!isAdmin_(pAuth, env) && !isView_(pAuth, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);

      const workorder_id = String(formData.get("workorder_id") || "").trim();
      const uploaded_by = String(formData.get("uploaded_by") || "").trim();
      const file = formData.get("file");

      if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);
      if (!file || !file.size) return jsonpOrJson({ ok:false, error:"missing file" }, callback);

      // 校验作业单存在 + 状态
      const wo = await env.DB.prepare(`SELECT workorder_id, status FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
      if (!wo) return jsonpOrJson({ ok:false, error:"workorder not found" }, callback);
      if (wo.status !== "draft" && wo.status !== "issued")
        return jsonpOrJson({ ok:false, error:"当前状态不允许上传附件（仅草稿/已下发）" }, callback);

      // 校验文件格式
      const ALLOWED_TYPES = { "image/jpeg":".jpg", "image/png":".png", "image/webp":".webp" };
      const ct = file.type || "";
      if (!ALLOWED_TYPES[ct])
        return jsonpOrJson({ ok:false, error:"不支持的文件格式，仅允许 jpg/png/webp" }, callback);

      // 校验文件大小（5MB）
      if (file.size > 5 * 1024 * 1024)
        return jsonpOrJson({ ok:false, error:"文件过大，单张上限 5MB" }, callback);

      // 校验附件数量上限（后端为准）
      const countRs = await env.DB.prepare(
        `SELECT COUNT(*) as cnt FROM b2b_workorder_attachments WHERE workorder_id=?`
      ).bind(workorder_id).first();
      if (countRs && countRs.cnt >= 3)
        return jsonpOrJson({ ok:false, error:"已达上限（每张作业单最多 3 张附件）" }, callback);

      // 生成 attachment_id
      const shortRand = Math.random().toString(16).slice(2, 6);
      const attachment_id = "ATT-" + workorder_id + "-" + now + "-" + shortRand;
      const ext = ALLOWED_TYPES[ct];
      const file_key = "b2b-att/" + workorder_id + "/" + attachment_id + ext;

      // sort_order
      const maxSort = await env.DB.prepare(
        `SELECT MAX(sort_order) as mx FROM b2b_workorder_attachments WHERE workorder_id=?`
      ).bind(workorder_id).first();
      const sort_order = (maxSort && maxSort.mx != null) ? maxSort.mx + 1 : 0;

      // 写入 R2
      const fileData = await file.arrayBuffer();
      await env.R2_BUCKET.put(file_key, fileData, {
        httpMetadata: { contentType: ct }
      });

      // 写入 D1（如果失败则回滚 R2）
      try {
        await env.DB.prepare(
          `INSERT INTO b2b_workorder_attachments(attachment_id, workorder_id, file_key, file_name, file_size, content_type, sort_order, uploaded_by, created_at)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).bind(attachment_id, workorder_id, file_key, file.name || "unnamed", file.size, ct, sort_order, uploaded_by, now).run();
      } catch (dbErr) {
        // D1 写入失败 → 立刻删除刚写入的 R2 对象，避免孤儿文件
        await env.R2_BUCKET.delete(file_key).catch(() => {});
        return jsonpOrJson({ ok:false, error:"元数据写入失败: " + String(dbErr) }, callback);
      }

      return jsonpOrJson({ ok:true, attachment_id, file_name: file.name || "unnamed" }, callback);
    }

    // ===== B2B 附件删除 =====
    if (action === "b2b_attachment_delete") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const attachment_id = String(p.attachment_id || "").trim();
      if (!attachment_id) return jsonpOrJson({ ok:false, error:"missing attachment_id" }, callback);

      // 1. 先查 attachment 记录
      const att = await env.DB.prepare(
        `SELECT attachment_id, workorder_id, file_key FROM b2b_workorder_attachments WHERE attachment_id=?`
      ).bind(attachment_id).first();
      if (!att) return jsonpOrJson({ ok:false, error:"attachment not found" }, callback);

      // 2. 校验作业单状态（仅草稿可删）
      const wo = await env.DB.prepare(
        `SELECT status FROM b2b_workorders WHERE workorder_id=?`
      ).bind(att.workorder_id).first();
      if (!wo || wo.status !== "draft")
        return jsonpOrJson({ ok:false, error:"仅草稿状态允许删除附件" }, callback);

      // 3. 先删 R2 对象
      await env.R2_BUCKET.delete(att.file_key).catch(() => {});

      // 4. 再删 D1 记录
      await env.DB.prepare(
        `DELETE FROM b2b_workorder_attachments WHERE attachment_id=?`
      ).bind(attachment_id).run();

      return jsonpOrJson({ ok:true }, callback);
    }

    // ===== B2B 附件文件读取（GET，用于 <img src>） =====
    // 安全说明：第一版通过 URL query 传递口令 k，仅适用于内部试跑。
    // 长期方案应改为签名 URL 或 cookie 认证。
    if (action === "b2b_attachment_file") {
      if (!isAdmin_(p, env) && !isView_(p, env))
        return new Response("unauthorized", { status: 403, headers: CORS_HEADERS });

      const attachment_id = String(p.id || p.attachment_id || "").trim();
      if (!attachment_id)
        return new Response("missing id", { status: 400, headers: CORS_HEADERS });

      const att = await env.DB.prepare(
        `SELECT file_key, content_type FROM b2b_workorder_attachments WHERE attachment_id=?`
      ).bind(attachment_id).first();
      if (!att)
        return new Response("not found", { status: 404, headers: CORS_HEADERS });

      const obj = await env.R2_BUCKET.get(att.file_key);
      if (!obj)
        return new Response("file not found in storage", { status: 404, headers: CORS_HEADERS });

      return new Response(obj.body, {
        headers: {
          "Content-Type": att.content_type,
          "Cache-Control": "private, max-age=3600",
          ...CORS_HEADERS
        }
      });
    }

    // ===== B2B 现场作业记录 =====

    if (action === "b2b_field_op_create") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const req_id_fo = String(p.request_id || "").trim();

      const plan_day = String(p.plan_day || "").trim();
      const customer_name = String(p.customer_name || "").trim();
      const goods_summary = String(p.goods_summary || "").trim();
      const source_plan_id = String(p.source_plan_id || "").trim() || null;
      const operation_type = String(p.operation_type || "other").trim();
      const input_box_count = Number(p.input_box_count) || 0;
      const output_box_count = Number(p.output_box_count) || 0;
      const output_pallet_count = Number(p.output_pallet_count) || 0;
      const instruction_text = String(p.instruction_text || "").trim();
      const created_by = String(p.created_by || "").trim();

      // 新增字段（v16）
      const fo_packed_qty = Math.max(0, Number(p.packed_qty || 0));
      const fo_label_count = Math.max(0, Number(p.label_count || 0));
      const fo_photo_count = Math.max(0, Number(p.photo_count || 0));
      const fo_used_carton = toBool01(p.used_carton);
      const fo_has_pallet_detail = toBool01(p.has_pallet_detail);
      const fo_did_pack = toBool01(p.did_pack);
      const fo_did_rebox = toBool01(p.did_rebox);
      const fo_needs_forklift_pick = toBool01(p.needs_forklift_pick);
      const fo_packed_box_count = fo_did_pack ? Math.max(0, Number(p.packed_box_count || 0)) : 0;
      const fo_big_carton_count = fo_used_carton ? Math.max(0, Number(p.big_carton_count || 0)) : 0;
      const fo_small_carton_count = fo_used_carton ? Math.max(0, Number(p.small_carton_count || 0)) : 0;
      const fo_rebox_count = fo_did_rebox ? Math.max(0, Number(p.rebox_count || 0)) : 0;
      const fo_forklift_pallet_count = fo_needs_forklift_pick ? Math.max(0, Number(p.forklift_pallet_count || 0)) : 0;
      const fo_rack_pick_location_count = fo_needs_forklift_pick ? Math.max(0, Number(p.rack_pick_location_count || 0)) : 0;

      if (!plan_day || !/^\d{4}-\d{2}-\d{2}$/.test(plan_day)) return jsonpOrJson({ ok:false, error:"invalid plan_day" }, callback);
      if (!customer_name) return jsonpOrJson({ ok:false, error:"missing customer_name" }, callback);
      if (!created_by) return jsonpOrJson({ ok:false, error:"missing created_by" }, callback);

      const VALID_OP = ["box_op","palletize","bulk_in_out","unload","other"];
      if (!VALID_OP.includes(operation_type)) return jsonpOrJson({ ok:false, error:"invalid operation_type, must be: " + VALID_OP.join("/") }, callback);

      // 如果指定了 source_plan_id，校验存在性
      if (source_plan_id) {
        const srcPlan = await env.DB.prepare(`SELECT plan_id FROM b2b_inbound_plans WHERE plan_id=?`).bind(source_plan_id).first();
        if (!srcPlan) return jsonpOrJson({ ok:false, error:"source_plan_id not found: " + source_plan_id }, callback);
      }

      // 幂等 claim（校验通过后才占位）
      if (req_id_fo) {
        const dup = await env.DB.prepare(`SELECT response_json FROM api_idempotency_keys WHERE action='b2b_field_op_create' AND request_id=?`).bind(req_id_fo).first();
        if (dup && dup.response_json) return jsonpOrJson(JSON.parse(dup.response_json), callback);
        if (dup) return jsonpOrJson({ ok:false, error:"request_in_progress", retryable:true }, callback);
        const ins = await env.DB.prepare(`INSERT OR IGNORE INTO api_idempotency_keys(action,request_id,created_at) VALUES('b2b_field_op_create',?,?)`).bind(req_id_fo, Date.now()).run();
        if (!ins.meta?.changes) {
          const dup2 = await env.DB.prepare(`SELECT response_json FROM api_idempotency_keys WHERE action='b2b_field_op_create' AND request_id=?`).bind(req_id_fo).first();
          if (dup2 && dup2.response_json) return jsonpOrJson(JSON.parse(dup2.response_json), callback);
          return jsonpOrJson({ ok:false, error:"request_in_progress", retryable:true }, callback);
        }
      }

      let record_id = null;
      try {
        // 生成 record_id：FO-YYMMDD-NNN
        const dayTag = plan_day.slice(2).replace(/-/g, "");
        const maxRow = await env.DB.prepare(
          `SELECT record_id FROM b2b_field_ops WHERE record_id LIKE ? ORDER BY record_id DESC LIMIT 1`
        ).bind("FO-" + dayTag + "-%").first();
        let seq = 1;
        if (maxRow && maxRow.record_id) {
          const parts = maxRow.record_id.split("-");
          seq = (parseInt(parts[2], 10) || 0) + 1;
        }
        record_id = "FO-" + dayTag + "-" + String(seq).padStart(3, "0");

        await env.DB.prepare(
          `INSERT INTO b2b_field_ops(record_id,source_plan_id,plan_day,customer_name,goods_summary,operation_type,
           input_box_count,output_box_count,output_pallet_count,instruction_text,
           packed_qty,packed_box_count,used_carton,big_carton_count,small_carton_count,
           label_count,photo_count,has_pallet_detail,did_pack,did_rebox,rebox_count,
           needs_forklift_pick,forklift_pallet_count,rack_pick_location_count,
           status,created_by,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'draft',?,?)`
        ).bind(record_id, source_plan_id, plan_day, customer_name, goods_summary, operation_type,
          input_box_count, output_box_count, output_pallet_count, instruction_text,
          fo_packed_qty, fo_packed_box_count, fo_used_carton, fo_big_carton_count, fo_small_carton_count,
          fo_label_count, fo_photo_count, fo_has_pallet_detail, fo_did_pack, fo_did_rebox, fo_rebox_count,
          fo_needs_forklift_pick, fo_forklift_pallet_count, fo_rack_pick_location_count,
          created_by, now).run();

        const respFo = { ok:true, record_id };
        if (req_id_fo) {
          await env.DB.prepare(`UPDATE api_idempotency_keys SET result_id=?, response_json=? WHERE action='b2b_field_op_create' AND request_id=?`).bind(record_id, JSON.stringify(respFo), req_id_fo).run();
        }
        return jsonpOrJson(respFo, callback);
      } catch(e) {
        if (req_id_fo) {
          const exists = record_id && await env.DB.prepare(`SELECT 1 FROM b2b_field_ops WHERE record_id=?`).bind(record_id).first();
          if (exists) {
            const respFo = { ok:true, record_id };
            try { await env.DB.prepare(`UPDATE api_idempotency_keys SET result_id=?, response_json=? WHERE action='b2b_field_op_create' AND request_id=?`).bind(record_id, JSON.stringify(respFo), req_id_fo).run(); } catch(_){}
            return jsonpOrJson(respFo, callback);
          }
          await env.DB.prepare(`DELETE FROM api_idempotency_keys WHERE action='b2b_field_op_create' AND request_id=? AND response_json=''`).bind(req_id_fo).run();
        }
        throw e;
      }
    }

    if (action === "b2b_field_op_list") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const start_day = String(p.start_day || "").trim();
      const end_day = String(p.end_day || "").trim();
      const statusParam = String(p.status || "").trim();

      let where = "1=1";
      const binds = [];
      if (start_day && end_day) { where += " AND plan_day >= ? AND plan_day <= ?"; binds.push(start_day, end_day); }
      if (statusParam) {
        const arr = statusParam.split(",").map(s => s.trim()).filter(Boolean);
        if (arr.length) { where += " AND status IN (" + arr.map(() => "?").join(",") + ")"; binds.push(...arr); }
      }

      const rs = await env.DB.prepare(
        `SELECT *, substr(datetime(created_at/1000,'unixepoch','+9 hours'),1,10) AS created_day_kst FROM b2b_field_ops WHERE ${where} ORDER BY created_at DESC`
      ).bind(...binds).all();
      return jsonpOrJson({ ok:true, records: rs.results || [] }, callback);
    }

    if (action === "b2b_field_op_detail") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const record_id = String(p.record_id || "").trim();
      if (!record_id) return jsonpOrJson({ ok:false, error:"missing record_id" }, callback);

      const row = await env.DB.prepare(`SELECT * FROM b2b_field_ops WHERE record_id=?`).bind(record_id).first();
      if (!row) return jsonpOrJson({ ok:false, error:"record_id not found" }, callback);
      const brs = await env.DB.prepare(`SELECT id, workorder_id, is_primary, bind_note, bound_by, bound_at FROM b2b_field_op_wo_bindings WHERE record_id=? ORDER BY is_primary DESC, bound_at ASC`).bind(record_id).all();
      row.bindings = brs.results || [];
      return jsonpOrJson({ ok:true, record: row }, callback);
    }

    if (action === "b2b_field_op_update") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const record_id = String(p.record_id || "").trim();
      if (!record_id) return jsonpOrJson({ ok:false, error:"missing record_id" }, callback);

      const existing = await env.DB.prepare(`SELECT * FROM b2b_field_ops WHERE record_id=?`).bind(record_id).first();
      if (!existing) return jsonpOrJson({ ok:false, error:"record_id not found" }, callback);

      const sub = String(p.sub || "").trim(); // "edit" | "status" | "bind"

      // --- 编辑字段：只允许 draft / recording，全量提交，source_plan_id 不可改 ---
      if (sub === "edit") {
        if (existing.status !== "draft" && existing.status !== "recording") {
          return jsonpOrJson({ ok:false, error:"only draft/recording can be edited" }, callback);
        }
        const plan_day = String(p.plan_day || "").trim();
        const customer_name = String(p.customer_name || "").trim();
        const goods_summary = String(p.goods_summary || "").trim();
        const operation_type = String(p.operation_type || "").trim();
        const input_box_count = Number(p.input_box_count) || 0;
        const output_box_count = Number(p.output_box_count) || 0;
        const output_pallet_count = Number(p.output_pallet_count) || 0;
        const instruction_text = String(p.instruction_text || "").trim();

        // 新增字段（v16）
        const e_packed_qty = Math.max(0, Number(p.packed_qty || 0));
        const e_label_count = Math.max(0, Number(p.label_count || 0));
        const e_photo_count = Math.max(0, Number(p.photo_count || 0));
        const e_used_carton = toBool01(p.used_carton);
        const e_has_pallet_detail = toBool01(p.has_pallet_detail);
        const e_did_pack = toBool01(p.did_pack);
        const e_did_rebox = toBool01(p.did_rebox);
        const e_needs_forklift_pick = toBool01(p.needs_forklift_pick);
        const e_packed_box_count = e_did_pack ? Math.max(0, Number(p.packed_box_count || 0)) : 0;
        const e_big_carton_count = e_used_carton ? Math.max(0, Number(p.big_carton_count || 0)) : 0;
        const e_small_carton_count = e_used_carton ? Math.max(0, Number(p.small_carton_count || 0)) : 0;
        const e_rebox_count = e_did_rebox ? Math.max(0, Number(p.rebox_count || 0)) : 0;
        const e_forklift_pallet_count = e_needs_forklift_pick ? Math.max(0, Number(p.forklift_pallet_count || 0)) : 0;
        const e_rack_pick_location_count = e_needs_forklift_pick ? Math.max(0, Number(p.rack_pick_location_count || 0)) : 0;

        // v22 新增字段
        const e_sku_kind_count = Math.max(0, Math.round(Number(p.sku_kind_count || 0)));
        const e_remark = String(p.remark || "").trim();
        const e_confirm_badge = String(p.confirm_badge || "").trim();
        const e_confirmed_by = String(p.confirmed_by || "").trim();

        if (!plan_day || !/^\d{4}-\d{2}-\d{2}$/.test(plan_day)) return jsonpOrJson({ ok:false, error:"invalid plan_day" }, callback);
        if (!customer_name) return jsonpOrJson({ ok:false, error:"missing customer_name" }, callback);
        if (!operation_type) return jsonpOrJson({ ok:false, error:"missing operation_type" }, callback);

        const VALID_OP = ["box_op","palletize","bulk_in_out","unload","other"];
        if (!VALID_OP.includes(operation_type)) return jsonpOrJson({ ok:false, error:"invalid operation_type" }, callback);

        await env.DB.prepare(
          `UPDATE b2b_field_ops SET plan_day=?, customer_name=?, goods_summary=?, operation_type=?,
           input_box_count=?, output_box_count=?, output_pallet_count=?, instruction_text=?,
           packed_qty=?, packed_box_count=?, used_carton=?, big_carton_count=?, small_carton_count=?,
           label_count=?, photo_count=?, has_pallet_detail=?, did_pack=?, did_rebox=?, rebox_count=?,
           needs_forklift_pick=?, forklift_pallet_count=?, rack_pick_location_count=?,
           sku_kind_count=?, remark=?, confirm_badge=?, confirmed_by=?
           WHERE record_id=?`
        ).bind(plan_day, customer_name, goods_summary, operation_type,
          input_box_count, output_box_count, output_pallet_count, instruction_text,
          e_packed_qty, e_packed_box_count, e_used_carton, e_big_carton_count, e_small_carton_count,
          e_label_count, e_photo_count, e_has_pallet_detail, e_did_pack, e_did_rebox, e_rebox_count,
          e_needs_forklift_pick, e_forklift_pallet_count, e_rack_pick_location_count,
          e_sku_kind_count, e_remark, e_confirm_badge, e_confirmed_by,
          record_id).run();

        return jsonpOrJson({ ok:true, record_id }, callback);
      }

      // --- 状态变更：按状态机走 ---
      if (sub === "status") {
        const new_status = String(p.status || "").trim();
        const FO_NEXT = {
          draft: ["recording","cancelled"],
          recording: ["completed","cancelled"],
          completed: [],
          cancelled: []
        };
        const allowed = FO_NEXT[existing.status];
        if (!allowed || !allowed.includes(new_status)) {
          return jsonpOrJson({ ok:false, error:"cannot change from " + existing.status + " to " + new_status }, callback);
        }

        let completedAt = existing.completed_at;
        if (new_status === "completed") completedAt = now;

        await env.DB.prepare(
          `UPDATE b2b_field_ops SET status=?, completed_at=? WHERE record_id=?`
        ).bind(new_status, completedAt, record_id).run();

        return jsonpOrJson({ ok:true, record_id, status: new_status }, callback);
      }

      // --- 绑定作业单（多绑定） ---
      if (sub === "bind") {
        if (existing.status !== "completed") {
          return jsonpOrJson({ ok:false, error:"only completed records can be bound" }, callback);
        }
        const workorder_id = String(p.workorder_id || "").trim();
        if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);
        const bind_note = String(p.bind_note || "").trim();
        const bound_by = String(p.bound_by || "").trim();

        const wo = await env.DB.prepare(`SELECT workorder_id FROM b2b_workorders WHERE workorder_id=?`).bind(workorder_id).first();
        if (!wo) return jsonpOrJson({ ok:false, error:"workorder_id not found: " + workorder_id }, callback);

        // 检查是否已绑定过同一个
        const dup = await env.DB.prepare(`SELECT 1 FROM b2b_field_op_wo_bindings WHERE record_id=? AND workorder_id=?`).bind(record_id, workorder_id).first();
        if (dup) return jsonpOrJson({ ok:false, error:"已绑定过该作业单: " + workorder_id }, callback);

        // 判断是否为第一条绑定（设为 primary）
        const cnt = await env.DB.prepare(`SELECT COUNT(*) AS c FROM b2b_field_op_wo_bindings WHERE record_id=?`).bind(record_id).first();
        const isPrimary = (cnt && cnt.c > 0) ? 0 : 1;

        await env.DB.prepare(
          `INSERT INTO b2b_field_op_wo_bindings(record_id, workorder_id, is_primary, bind_note, bound_by, bound_at) VALUES(?,?,?,?,?,?)`
        ).bind(record_id, workorder_id, isPrimary, bind_note, bound_by, now).run();

        // 同步 field_ops.bound_workorder_id（兼容旧读取）
        if (!existing.bound_workorder_id) {
          await env.DB.prepare(`UPDATE b2b_field_ops SET bound_workorder_id=?, bound_at=? WHERE record_id=?`).bind(workorder_id, now, record_id).run();
        }

        return jsonpOrJson({ ok:true, record_id, workorder_id }, callback);
      }

      // --- 解绑作业单 ---
      if (sub === "unbind") {
        const workorder_id = String(p.workorder_id || "").trim();
        if (!workorder_id) return jsonpOrJson({ ok:false, error:"missing workorder_id" }, callback);

        await env.DB.prepare(`DELETE FROM b2b_field_op_wo_bindings WHERE record_id=? AND workorder_id=?`).bind(record_id, workorder_id).run();

        // 同步 field_ops.bound_workorder_id
        const remain = await env.DB.prepare(`SELECT workorder_id FROM b2b_field_op_wo_bindings WHERE record_id=? ORDER BY is_primary DESC, bound_at ASC LIMIT 1`).bind(record_id).first();
        await env.DB.prepare(`UPDATE b2b_field_ops SET bound_workorder_id=?, bound_at=? WHERE record_id=?`).bind(remain ? remain.workorder_id : null, remain ? now : null, record_id).run();

        return jsonpOrJson({ ok:true, record_id, unbound: workorder_id }, callback);
      }

      return jsonpOrJson({ ok:false, error:"invalid sub, must be: edit/status/bind/unbind" }, callback);
    }

    // ===== 扫码绑定作业对象 =====

    if (action === "b2b_op_bind") {
      const session_id = String(p.session_id || "").trim();
      const badge = String(p.badge || "").trim();
      const source_order_no = String(p.source_order_no || "").trim();
      const bound_task = String(p.bound_task || "").trim();

      if (!session_id) return jsonpOrJson({ ok:false, error:"missing session_id" }, callback);
      if (!source_order_no) return jsonpOrJson({ ok:false, error:"missing source_order_no" }, callback);

      // KST 日期
      const kstOffset = 9 * 60 * 60 * 1000;
      const day_kst = new Date(now + kstOffset).toISOString().slice(0, 10);

      // 检查是否已绑定
      const existing = await env.DB.prepare(
        `SELECT id, source_type, match_status, day_kst, internal_workorder_id FROM b2b_operation_bindings WHERE session_id=? AND source_order_no=?`
      ).bind(session_id, source_order_no).first();
      if (existing) {
        return jsonpOrJson({ ok:true, duplicate:true, source_type: existing.source_type, match_status: existing.match_status, day_kst: existing.day_kst, internal_workorder_id: existing.internal_workorder_id || null, msg:"该工单已绑定" }, callback);
      }

      // 查本系统工单
      const wo = await env.DB.prepare(
        `SELECT workorder_id, customer_name, outbound_box_count, outbound_pallet_count, total_qty, total_qty_unit, status FROM b2b_workorders WHERE workorder_id=?`
      ).bind(source_order_no).first();

      let source_type, match_status, internal_workorder_id = null;
      let wo_summary = null;

      if (wo) {
        source_type = "internal_b2b_workorder";
        match_status = "direct_internal";
        internal_workorder_id = wo.workorder_id;
        // 产出摘要
        const parts = [];
        if (wo.outbound_box_count) parts.push(wo.outbound_box_count + "箱");
        if (wo.outbound_pallet_count) parts.push(wo.outbound_pallet_count + "托");
        if (parts.length === 0 && wo.total_qty) parts.push(wo.total_qty + (wo.total_qty_unit || ""));
        wo_summary = { customer_name: wo.customer_name, qty_text: parts.join(" / "), status: wo.status };
      } else {
        source_type = "external_wms_workorder";
        match_status = "pending_wms_match";
      }

      await env.DB.prepare(
        `INSERT INTO b2b_operation_bindings(session_id, badge, bound_task, source_type, source_order_no, internal_workorder_id, day_kst, match_status, matched_wms_ref, bound_at, created_at)
         VALUES(?,?,?,?,?,?,?,?,'',?,?)`
      ).bind(session_id, badge, bound_task, source_type, source_order_no, internal_workorder_id, day_kst, match_status, now, now).run();

      return jsonpOrJson({ ok:true, source_type, match_status, source_order_no, day_kst, internal_workorder_id, wo_summary }, callback);
    }

    if (action === "b2b_op_bind_list") {
      const session_id = String(p.session_id || "").trim();
      if (!session_id) return jsonpOrJson({ ok:false, error:"missing session_id" }, callback);

      const rs = await env.DB.prepare(
        `SELECT * FROM b2b_operation_bindings WHERE session_id=? ORDER BY bound_at ASC`
      ).bind(session_id).all();

      // 对 internal 类型补充工单摘要
      const bindings = rs.results || [];
      for (let i = 0; i < bindings.length; i++) {
        if (bindings[i].source_type === "internal_b2b_workorder" && bindings[i].internal_workorder_id) {
          const wo = await env.DB.prepare(
            `SELECT customer_name, outbound_box_count, outbound_pallet_count, total_qty, total_qty_unit, status FROM b2b_workorders WHERE workorder_id=?`
          ).bind(bindings[i].internal_workorder_id).first();
          if (wo) {
            const parts = [];
            if (wo.outbound_box_count) parts.push(wo.outbound_box_count + "箱");
            if (wo.outbound_pallet_count) parts.push(wo.outbound_pallet_count + "托");
            if (parts.length === 0 && wo.total_qty) parts.push(wo.total_qty + (wo.total_qty_unit || ""));
            bindings[i].wo_summary = { customer_name: wo.customer_name, qty_text: parts.join(" / "), status: wo.status };
          }
        }
      }

      return jsonpOrJson({ ok:true, bindings }, callback);
    }

    // ===== 解绑工单 =====

    if (action === "b2b_op_unbind") {
      const session_id = String(p.session_id || "").trim();
      const source_type = String(p.source_type || "").trim();
      const source_order_no = String(p.source_order_no || "").trim();
      if (!session_id) return jsonpOrJson({ ok:false, error:"missing session_id" }, callback);
      if (!source_type) return jsonpOrJson({ ok:false, error:"missing source_type" }, callback);
      if (!source_order_no) return jsonpOrJson({ ok:false, error:"missing source_order_no" }, callback);

      // 删前先查出真实 day_kst
      const bindRow = await env.DB.prepare(
        `SELECT day_kst FROM b2b_operation_bindings WHERE session_id=? AND source_type=? AND source_order_no=? LIMIT 1`
      ).bind(session_id, source_type, source_order_no).first();
      const binding_day_kst = bindRow ? bindRow.day_kst : "";

      const del = await env.DB.prepare(
        `DELETE FROM b2b_operation_bindings WHERE session_id=? AND source_type=? AND source_order_no=?`
      ).bind(session_id, source_type, source_order_no).run();

      // 按真实 day_kst 查剩余绑定数
      let remaining = 0;
      if (binding_day_kst) {
        const remain = await env.DB.prepare(
          `SELECT COUNT(*) as cnt FROM b2b_operation_bindings WHERE source_type=? AND source_order_no=? AND day_kst=?`
        ).bind(source_type, source_order_no, binding_day_kst).first();
        remaining = (remain && remain.cnt) || 0;
      }

      return jsonpOrJson({ ok:true, deleted: del.meta && del.meta.changes || 0, remaining_bindings: remaining, binding_day_kst }, callback);
    }

    // ===== 现场结果单 =====

    if (action === "b2b_op_result_get") {
      const day_kst = String(p.day_kst || "").trim();
      const source_order_no = String(p.source_order_no || "").trim();
      if (!day_kst || !source_order_no) return jsonpOrJson({ ok:false, error:"missing day_kst or source_order_no" }, callback);

      // 必须当日绑定过（按 day_kst + source_order_no 校验）
      const bind = await env.DB.prepare(
        `SELECT source_type, internal_workorder_id FROM b2b_operation_bindings WHERE day_kst=? AND source_order_no=? LIMIT 1`
      ).bind(day_kst, source_order_no).first();
      if (!bind) return jsonpOrJson({ ok:false, error:"workorder not bound today" }, callback);

      const row = await env.DB.prepare(
        `SELECT * FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
      ).bind(day_kst, bind.source_type, source_order_no).first();

      // 参与信息（当日 + 同来源 + 同工单）
      const partRs = await env.DB.prepare(
        `SELECT COUNT(DISTINCT session_id) as session_count, COUNT(DISTINCT badge) as badge_count FROM b2b_operation_bindings WHERE day_kst=? AND source_type=? AND source_order_no=? AND badge != ''`
      ).bind(day_kst, bind.source_type, source_order_no).all();
      const badgeRs = await env.DB.prepare(
        `SELECT DISTINCT badge FROM b2b_operation_bindings WHERE day_kst=? AND source_type=? AND source_order_no=? AND badge != ''`
      ).bind(day_kst, bind.source_type, source_order_no).all();
      const part = (partRs.results || [])[0] || { session_count:0, badge_count:0 };
      const badges = (badgeRs.results || []).map(r => r.badge);

      // 自动带入客户名
      let customer_name = "";
      if (bind.internal_workorder_id) {
        const wo = await env.DB.prepare(`SELECT customer_name FROM b2b_workorders WHERE workorder_id=?`).bind(bind.internal_workorder_id).first();
        if (wo) customer_name = wo.customer_name || "";
      }

      return jsonpOrJson({
        ok:true, result: row || null,
        source_type: bind.source_type,
        internal_workorder_id: bind.internal_workorder_id || null,
        customer_name,
        participation: { session_count: part.session_count, badge_count: part.badge_count, badges }
      }, callback);
    }

    if (action === "b2b_op_result_upsert") {
      const day_kst = String(p.day_kst || "").trim();
      const source_order_no = String(p.source_order_no || "").trim();
      const session_id = String(p.session_id || "").trim();
      if (!day_kst || !source_order_no) return jsonpOrJson({ ok:false, error:"missing day_kst or source_order_no" }, callback);

      // 必须当日绑定过
      const bind = await env.DB.prepare(
        `SELECT source_type, internal_workorder_id FROM b2b_operation_bindings WHERE day_kst=? AND source_order_no=? LIMIT 1`
      ).bind(day_kst, source_order_no).first();
      if (!bind) return jsonpOrJson({ ok:false, error:"workorder not bound today" }, callback);

      const operation_mode = String(p.operation_mode || "pack_outbound").trim();
      if (operation_mode !== "pack_outbound" && operation_mode !== "move_and_palletize")
        return jsonpOrJson({ ok:false, error:"invalid operation_mode" }, callback);

      const sku_kind_count = Math.max(0, Number(p.sku_kind_count || 0));
      const box_count = Math.max(0, Number(p.box_count || 0));
      const pallet_count = Math.max(0, Number(p.pallet_count || 0));
      const packed_qty = Math.max(0, Number(p.packed_qty || 0));
      const packed_box_count_raw = Math.max(0, Number(p.packed_box_count || 0));
      const label_count = Math.max(0, Number(p.label_count || 0));
      const photo_count = Math.max(0, Number(p.photo_count || 0));
      const remark = String(p.remark || "").trim();

      // 主开关字段 — 统一 toBool01
      const used_carton = toBool01(p.used_carton);
      const has_pallet_detail = toBool01(p.has_pallet_detail);
      const did_pack = toBool01(p.did_pack);
      const did_rebox = toBool01(p.did_rebox);
      const needs_forklift_pick = toBool01(p.needs_forklift_pick);

      // 联动归零
      const big_carton_count = used_carton ? Math.max(0, Number(p.big_carton_count || 0)) : 0;
      const small_carton_count = used_carton ? Math.max(0, Number(p.small_carton_count || 0)) : 0;
      const packed_box_count = (operation_mode === "move_and_palletize" && !did_pack) ? 0 : packed_box_count_raw;
      const rebox_count = did_rebox ? Math.max(0, Number(p.rebox_count || 0)) : 0;
      const forklift_pallet_count = needs_forklift_pick ? Math.max(0, Number(p.forklift_pallet_count || 0)) : 0;
      const rack_pick_location_count = needs_forklift_pick ? Math.max(0, Number(p.rack_pick_location_count || 0)) : 0;
      const new_status = String(p.status || "draft").trim();
      if (new_status !== "draft" && new_status !== "completed")
        return jsonpOrJson({ ok:false, error:"invalid status" }, callback);
      const confirm_badge = String(p.confirm_badge || "").trim();
      const confirmed_by = String(p.confirmed_by || "").trim();

      // completed 必须有职员工牌确认
      if (new_status === "completed") {
        if (!confirm_badge) return jsonpOrJson({ ok:false, error:"missing confirm_badge" }, callback);
        if (!/^EMP-.+$/.test(confirm_badge)) return jsonpOrJson({ ok:false, error:"invalid confirm_badge, employee badge (EMP-...) required" }, callback);
        // ★ 跨 session 活跃 labor 检查：该工单在其他设备仍有 active labor 时禁止 completed
        const activeLaborCross = await env.DB.prepare(
          `SELECT session_id, operator_badge FROM b2b_operation_labor_details
           WHERE source_order_no=? AND day_kst=? AND status='active' LIMIT 5`
        ).bind(source_order_no, day_kst).all();
        if ((activeLaborCross.results || []).length > 0) {
          const info = (activeLaborCross.results || []).map(r => r.operator_badge + '@' + r.session_id.slice(-6));
          return jsonpOrJson({ ok:false, error:"cross_session_active_labor", active_labor: info,
            msg:"该工单在其他设备仍有作业中的人员，请先完成或暂停 / 다른 기기에서 아직 작업 중인 인원이 있습니다" }, callback);
        }
      }

      // 客户名
      let customer_name = String(p.customer_name || "").trim();
      if (!customer_name && bind.internal_workorder_id) {
        const wo = await env.DB.prepare(`SELECT customer_name FROM b2b_workorders WHERE workorder_id=?`).bind(bind.internal_workorder_id).first();
        if (wo) customer_name = wo.customer_name || "";
      }

      const existing = await env.DB.prepare(
        `SELECT id, status FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
      ).bind(day_kst, bind.source_type, source_order_no).first();

      // draft 退回时清空确认痕迹
      const final_confirm_badge = new_status === "draft" ? "" : confirm_badge;
      const final_confirmed_by = new_status === "draft" ? "" : confirmed_by;

      // v25: simple mode extended fields (backwards compatible — empty string / null = no change)
      const result_entered_by_badge = String(p.result_entered_by_badge || "").trim();
      const result_entered_by_name = String(p.result_entered_by_name || "").trim();
      const result_entered_at = result_entered_by_badge ? now : null;
      const confirmed_at = (new_status === "completed" && final_confirm_badge) ? now : (new_status === "draft" ? null : null);
      const reviewed_by_badge = new_status === "completed" ? String(p.reviewed_by_badge || final_confirm_badge || "").trim() : (new_status === "draft" ? "" : "");
      const reviewed_by_name = new_status === "completed" ? String(p.reviewed_by_name || final_confirmed_by || "").trim() : (new_status === "draft" ? "" : "");
      const reviewed_at = (new_status === "completed" && reviewed_by_badge) ? now : null;
      const workflow_status_param = String(p.workflow_status || "").trim();

      if (existing) {
        // 更新
        const completed_at = new_status === "completed" ? now : null;
        await env.DB.prepare(
          `UPDATE b2b_operation_results SET operation_mode=?, sku_kind_count=?, box_count=?, pallet_count=?,
           packed_qty=?, packed_box_count=?, used_carton=?, big_carton_count=?, small_carton_count=?,
           label_count=?, photo_count=?, has_pallet_detail=?, did_pack=?, did_rebox=?, rebox_count=?,
           needs_forklift_pick=?, forklift_pallet_count=?, rack_pick_location_count=?,
           remark=?, status=?, confirmed_by=?, confirm_badge=?, customer_name=?, updated_at=?, completed_at=?,
           result_entered_by_badge=COALESCE(NULLIF(?,''), result_entered_by_badge),
           result_entered_by_name=COALESCE(NULLIF(?,''), result_entered_by_name),
           result_entered_at=COALESCE(?, result_entered_at),
           confirmed_at=COALESCE(?, confirmed_at),
           reviewed_by_badge=CASE WHEN ?='draft' THEN '' WHEN ?!='' THEN ? ELSE reviewed_by_badge END,
           reviewed_by_name=CASE WHEN ?='draft' THEN '' WHEN ?!='' THEN ? ELSE reviewed_by_name END,
           reviewed_at=COALESCE(?, reviewed_at),
           workflow_status=CASE WHEN ?!='' THEN ? ELSE workflow_status END
           WHERE id=?`
        ).bind(operation_mode, sku_kind_count, box_count, pallet_count,
          packed_qty, packed_box_count, used_carton, big_carton_count, small_carton_count,
          label_count, photo_count, has_pallet_detail, did_pack, did_rebox, rebox_count,
          needs_forklift_pick, forklift_pallet_count, rack_pick_location_count,
          remark, new_status, final_confirmed_by, final_confirm_badge, customer_name, now, completed_at,
          result_entered_by_badge, result_entered_by_name, result_entered_at, confirmed_at,
          new_status, reviewed_by_badge, reviewed_by_badge,
          new_status, reviewed_by_name, reviewed_by_name,
          reviewed_at,
          workflow_status_param, workflow_status_param,
          existing.id).run();
        return jsonpOrJson({ ok:true, id: existing.id, created: false }, callback);
      } else {
        // 新建
        const completed_at = new_status === "completed" ? now : null;
        const ins = await env.DB.prepare(
          `INSERT INTO b2b_operation_results(day_kst, source_type, source_order_no, internal_workorder_id,
           customer_name, operation_mode, sku_kind_count, box_count, pallet_count,
           packed_qty, packed_box_count, used_carton, big_carton_count, small_carton_count,
           label_count, photo_count, has_pallet_detail, did_pack, did_rebox, rebox_count,
           needs_forklift_pick, forklift_pallet_count, rack_pick_location_count,
           remark, photo_urls_json, status, created_by, confirmed_by, confirm_badge, first_session_id,
           created_at, updated_at, completed_at,
           result_entered_by_badge, result_entered_by_name, result_entered_at,
           confirmed_at, reviewed_by_badge, reviewed_by_name, reviewed_at,
           workflow_status, temporary_completed_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'[]',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
        ).bind(day_kst, bind.source_type, source_order_no, bind.internal_workorder_id || null,
          customer_name, operation_mode, sku_kind_count, box_count, pallet_count,
          packed_qty, packed_box_count, used_carton, big_carton_count, small_carton_count,
          label_count, photo_count, has_pallet_detail, did_pack, did_rebox, rebox_count,
          needs_forklift_pick, forklift_pallet_count, rack_pick_location_count,
          remark, new_status, String(p.created_by || "").trim(), final_confirmed_by, final_confirm_badge, session_id,
          now, now, completed_at,
          result_entered_by_badge, result_entered_by_name, result_entered_at,
          confirmed_at, reviewed_by_badge, reviewed_by_name, reviewed_at,
          workflow_status_param, null).run();
        return jsonpOrJson({ ok:true, id: ins.meta && ins.meta.last_row_id, created: true }, callback);
      }
    }

    if (action === "b2b_op_result_list") {
      const day_kst = String(p.day_kst || "").trim();
      if (!day_kst) return jsonpOrJson({ ok:false, error:"missing day_kst" }, callback);
      const rs = await env.DB.prepare(
        `SELECT * FROM b2b_operation_results WHERE day_kst=? ORDER BY created_at ASC`
      ).bind(day_kst).all();
      return jsonpOrJson({ ok:true, results: rs.results || [] }, callback);
    }

    // ===== 按 session 拉取该 session 所有绑定对应的结果单 =====
    if (action === "b2b_op_result_list_by_session") {
      const session_id = String(p.session_id || "").trim();
      if (!session_id) return jsonpOrJson({ ok:false, error:"missing session_id" }, callback);

      // 查该 session 的所有 binding 的 (day_kst, source_type, source_order_no)
      const bindings = await env.DB.prepare(
        `SELECT DISTINCT day_kst, source_type, source_order_no FROM b2b_operation_bindings WHERE session_id=?`
      ).bind(session_id).all();
      const bRows = bindings.results || [];
      if (bRows.length === 0) return jsonpOrJson({ ok:true, results:[] }, callback);

      const results = [];
      for (const b of bRows) {
        const row = await env.DB.prepare(
          `SELECT * FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
        ).bind(b.day_kst, b.source_type, b.source_order_no).first();
        if (row) results.push(row);
      }
      return jsonpOrJson({ ok:true, results }, callback);
    }

    // ===== B2B Simple Mode: Labor Details =====

    if (action === "b2b_simple_labor_join") {
      const session_id = String(p.session_id || "").trim();
      const source_order_no = String(p.source_order_no || "").trim();
      const operator_badge = String(p.operator_badge || "").trim();
      const operator_name = String(p.operator_name || "").trim();
      const entry_mode = String(p.entry_mode || "simple_mode").trim();
      if (!session_id || !source_order_no || !operator_badge)
        return jsonpOrJson({ ok:false, error:"missing required fields" }, callback);

      const bind = await env.DB.prepare(
        `SELECT source_type, internal_workorder_id, day_kst FROM b2b_operation_bindings WHERE session_id=? AND source_order_no=? LIMIT 1`
      ).bind(session_id, source_order_no).first();
      if (!bind) return jsonpOrJson({ ok:false, error:"workorder_not_bound" }, callback);

      // completed 工单不允许再加人
      const chkResult = await env.DB.prepare(
        `SELECT workflow_status, status FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
      ).bind(bind.day_kst, bind.source_type, source_order_no).first();
      if (chkResult && (chkResult.workflow_status === "completed" || chkResult.status === "completed"))
        return jsonpOrJson({ ok:false, error:"workorder_completed" }, callback);

      // 已 active 则去重
      const existingLd = await env.DB.prepare(
        `SELECT id FROM b2b_operation_labor_details WHERE session_id=? AND source_order_no=? AND operator_badge=? AND status='active'`
      ).bind(session_id, source_order_no, operator_badge).first();
      if (existingLd) return jsonpOrJson({ ok:true, duplicate:true, id: existingLd.id }, callback);

      const segRs = await env.DB.prepare(
        `SELECT MAX(segment_no) as max_seg FROM b2b_operation_labor_details WHERE session_id=? AND source_order_no=? AND operator_badge=?`
      ).bind(session_id, source_order_no, operator_badge).first();
      const segment_no = ((segRs && segRs.max_seg) || 0) + 1;

      const ins = await env.DB.prepare(
        `INSERT INTO b2b_operation_labor_details(day_kst,session_id,source_type,source_order_no,internal_workorder_id,operator_badge,operator_name,segment_no,join_ms,entry_mode,status,created_at,updated_at)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(bind.day_kst, session_id, bind.source_type, source_order_no, bind.internal_workorder_id||null,
        operator_badge, operator_name, segment_no, now, entry_mode, 'active', now, now).run();

      // 确保 result 行存在 + workflow_status=working
      const existResult = await env.DB.prepare(
        `SELECT id, workflow_status FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
      ).bind(bind.day_kst, bind.source_type, source_order_no).first();
      if (existResult) {
        if (!existResult.workflow_status || existResult.workflow_status === '') {
          await env.DB.prepare(`UPDATE b2b_operation_results SET workflow_status='working', updated_at=? WHERE id=?`).bind(now, existResult.id).run();
        }
      }
      return jsonpOrJson({ ok:true, id: ins.meta && ins.meta.last_row_id, segment_no }, callback);
    }

    if (action === "b2b_simple_labor_leave") {
      const session_id = String(p.session_id || "").trim();
      const source_order_no = String(p.source_order_no || "").trim();
      const operator_badge = String(p.operator_badge || "").trim();
      if (!session_id || !operator_badge)
        return jsonpOrJson({ ok:false, error:"missing required fields" }, callback);

      let sql = `SELECT id, join_ms FROM b2b_operation_labor_details WHERE session_id=? AND operator_badge=? AND status='active'`;
      const binds = [session_id, operator_badge];
      if (source_order_no) { sql += ` AND source_order_no=?`; binds.push(source_order_no); }

      const rows = await env.DB.prepare(sql).bind(...binds).all();
      let closed = 0;
      for (const r of (rows.results || [])) {
        const dur = (now - r.join_ms) / 60000;
        await env.DB.prepare(
          `UPDATE b2b_operation_labor_details SET leave_ms=?, duration_minutes=?, status='closed', updated_at=? WHERE id=?`
        ).bind(now, Math.round(dur*100)/100, now, r.id).run();
        closed++;
      }
      return jsonpOrJson({ ok:true, closed }, callback);
    }

    if (action === "b2b_simple_temp_complete") {
      const session_id = String(p.session_id || "").trim();
      const source_order_no = String(p.source_order_no || "").trim();
      if (!session_id || !source_order_no)
        return jsonpOrJson({ ok:false, error:"missing required fields" }, callback);

      const bind = await env.DB.prepare(
        `SELECT source_type, internal_workorder_id, day_kst FROM b2b_operation_bindings WHERE session_id=? AND source_order_no=? LIMIT 1`
      ).bind(session_id, source_order_no).first();
      if (!bind) return jsonpOrJson({ ok:false, error:"workorder_not_bound" }, callback);

      // completed 工单不允许再暂时完成
      const chkR = await env.DB.prepare(
        `SELECT workflow_status, status FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
      ).bind(bind.day_kst, bind.source_type, source_order_no).first();
      if (chkR && (chkR.workflow_status === "completed" || chkR.status === "completed"))
        return jsonpOrJson({ ok:false, error:"workorder_completed" }, callback);

      // 关闭所有 active labor details
      const activeLabor = await env.DB.prepare(
        `SELECT id, join_ms, operator_badge FROM b2b_operation_labor_details WHERE session_id=? AND source_order_no=? AND status='active'`
      ).bind(session_id, source_order_no).all();
      for (const r of (activeLabor.results || [])) {
        const dur = (now - r.join_ms) / 60000;
        await env.DB.prepare(
          `UPDATE b2b_operation_labor_details SET leave_ms=?, duration_minutes=?, status='closed', updated_at=? WHERE id=?`
        ).bind(now, Math.round(dur*100)/100, now, r.id).run();
      }

      // 确保 result 行存在，设 workflow_status=pending_result + temporary_completed_at
      const existResult = await env.DB.prepare(
        `SELECT id FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
      ).bind(bind.day_kst, bind.source_type, source_order_no).first();
      if (existResult) {
        await env.DB.prepare(
          `UPDATE b2b_operation_results SET workflow_status='pending_result', temporary_completed_at=?, updated_at=? WHERE id=?`
        ).bind(now, now, existResult.id).run();
      } else {
        let customer_name = "";
        if (bind.internal_workorder_id) {
          const wo = await env.DB.prepare(`SELECT customer_name FROM b2b_workorders WHERE workorder_id=?`).bind(bind.internal_workorder_id).first();
          if (wo) customer_name = wo.customer_name || "";
        }
        await env.DB.prepare(
          `INSERT INTO b2b_operation_results(day_kst,source_type,source_order_no,internal_workorder_id,customer_name,status,workflow_status,temporary_completed_at,first_session_id,created_by,created_at,updated_at,photo_urls_json)
           VALUES(?,?,?,?,?,'draft','pending_result',?,?,?,?,?,'[]')`
        ).bind(bind.day_kst, bind.source_type, source_order_no, bind.internal_workorder_id||null,
          customer_name, now, session_id, String(p.operator_id||""), now, now).run();
      }
      return jsonpOrJson({ ok:true, temporary_completed_at: now, closed_labor: (activeLabor.results||[]).length }, callback);
    }

    // 临时切走时批量关闭所有 active labor detail（带 temp_switch_flag）
    if (action === "b2b_simple_labor_leave_all") {
      const session_id = String(p.session_id || "").trim();
      if (!session_id) return jsonpOrJson({ ok:false, error:"missing session_id" }, callback);
      const is_temp = String(p.temp_switch || "") === "1";

      const rows = await env.DB.prepare(
        `SELECT id, join_ms FROM b2b_operation_labor_details WHERE session_id=? AND status='active'`
      ).bind(session_id).all();
      let closed = 0;
      for (const r of (rows.results || [])) {
        const dur = (now - r.join_ms) / 60000;
        await env.DB.prepare(
          `UPDATE b2b_operation_labor_details SET leave_ms=?, duration_minutes=?, status='closed', temp_switch_flag=?, updated_at=? WHERE id=?`
        ).bind(now, Math.round(dur*100)/100, is_temp ? 1 : 0, now, r.id).run();
        closed++;
      }
      return jsonpOrJson({ ok:true, closed }, callback);
    }

    // ===== 删除空工单（误扫移除）=====
    if (action === "b2b_simple_delete_empty") {
      const session_id = String(p.session_id || "").trim();
      const source_order_no = String(p.source_order_no || "").trim();
      if (!session_id || !source_order_no)
        return jsonpOrJson({ ok:false, error:"missing required fields" }, callback);

      // 安全检查1: 该 binding 存在
      const bind = await env.DB.prepare(
        `SELECT id, day_kst, source_type FROM b2b_operation_bindings WHERE session_id=? AND source_order_no=?`
      ).bind(session_id, source_order_no).first();
      if (!bind) return jsonpOrJson({ ok:false, error:"binding_not_found" }, callback);

      // 安全检查2: 不存在任何 labor_details 记录（含已关闭的）
      const anyLab = await env.DB.prepare(
        `SELECT COUNT(*) as cnt FROM b2b_operation_labor_details WHERE session_id=? AND source_order_no=?`
      ).bind(session_id, source_order_no).first();
      if (anyLab && anyLab.cnt > 0)
        return jsonpOrJson({ ok:false, error:"has_labor_history" }, callback);

      // 安全检查3: 不存在非 draft 的 result
      const result = await env.DB.prepare(
        `SELECT id, workflow_status, status FROM b2b_operation_results WHERE day_kst=? AND source_type=? AND source_order_no=?`
      ).bind(bind.day_kst, bind.source_type, source_order_no).first();
      if (result && (result.status !== "draft" || (result.workflow_status && result.workflow_status !== "draft" && result.workflow_status !== "working")))
        return jsonpOrJson({ ok:false, error:"has_non_draft_result" }, callback);

      // 删除 binding
      await env.DB.prepare(`DELETE FROM b2b_operation_bindings WHERE id=?`).bind(bind.id).run();
      // 删除 draft result（如有）
      if (result) {
        await env.DB.prepare(`DELETE FROM b2b_operation_results WHERE id=?`).bind(result.id).run();
      }

      return jsonpOrJson({ ok:true, deleted_order: source_order_no }, callback);
    }

    if (action === "b2b_simple_labor_list") {
      const session_id = String(p.session_id || "").trim();
      if (!session_id) return jsonpOrJson({ ok:false, error:"missing session_id" }, callback);
      const source_order_no = String(p.source_order_no || "").trim();

      let sql = `SELECT * FROM b2b_operation_labor_details WHERE session_id=?`;
      const binds = [session_id];
      if (source_order_no) { sql += ` AND source_order_no=?`; binds.push(source_order_no); }
      sql += ` ORDER BY join_ms ASC`;
      const rs = await env.DB.prepare(sql).bind(...binds).all();
      return jsonpOrJson({ ok:true, labor: rs.results || [] }, callback);
    }

    // ===== 出库扫码核对 =====

    if (action === "b2b_scan_batch_create") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const req_id_sc = String(p.request_id || "").trim();

      const check_day = String(p.check_day || "").trim();
      const batch_name = String(p.batch_name || "").trim();
      const created_by = String(p.created_by || "").trim();

      if (!check_day || !/^\d{4}-\d{2}-\d{2}$/.test(check_day)) return jsonpOrJson({ ok:false, error:"invalid check_day" }, callback);
      if (!batch_name) return jsonpOrJson({ ok:false, error:"missing batch_name" }, callback);
      if (!created_by) return jsonpOrJson({ ok:false, error:"missing created_by" }, callback);

      // items: JSON array of { outbound_barcode, expected_box_count, customer_name?, goods_summary? }
      let items;
      try {
        items = typeof p.items === "string" ? JSON.parse(p.items) : p.items;
      } catch(e) {
        return jsonpOrJson({ ok:false, error:"invalid items JSON" }, callback);
      }
      if (!Array.isArray(items) || items.length === 0) return jsonpOrJson({ ok:false, error:"items must be non-empty array" }, callback);

      // 校验每行 + 检查重复条码
      const seenBarcodes = new Set();
      let totalExpected = 0;
      for (let i = 0; i < items.length; i++) {
        const it = items[i];
        const bc = String(it.outbound_barcode || "").trim();
        const cnt = parseInt(it.expected_box_count, 10);
        if (!bc) return jsonpOrJson({ ok:false, error:"row " + (i+1) + ": missing outbound_barcode" }, callback);
        if (!cnt || cnt <= 0) return jsonpOrJson({ ok:false, error:"row " + (i+1) + ": expected_box_count must be positive integer" }, callback);
        if (seenBarcodes.has(bc)) return jsonpOrJson({ ok:false, error:"duplicate barcode: " + bc }, callback);
        seenBarcodes.add(bc);
        totalExpected += cnt;
      }

      // 幂等 claim（校验通过后才占位）
      if (req_id_sc) {
        const dup = await env.DB.prepare(`SELECT response_json FROM api_idempotency_keys WHERE action='b2b_scan_batch_create' AND request_id=?`).bind(req_id_sc).first();
        if (dup && dup.response_json) return jsonpOrJson(JSON.parse(dup.response_json), callback);
        if (dup) return jsonpOrJson({ ok:false, error:"request_in_progress", retryable:true }, callback);
        const ins = await env.DB.prepare(`INSERT OR IGNORE INTO api_idempotency_keys(action,request_id,created_at) VALUES('b2b_scan_batch_create',?,?)`).bind(req_id_sc, Date.now()).run();
        if (!ins.meta?.changes) {
          const dup2 = await env.DB.prepare(`SELECT response_json FROM api_idempotency_keys WHERE action='b2b_scan_batch_create' AND request_id=?`).bind(req_id_sc).first();
          if (dup2 && dup2.response_json) return jsonpOrJson(JSON.parse(dup2.response_json), callback);
          return jsonpOrJson({ ok:false, error:"request_in_progress", retryable:true }, callback);
        }
      }

      let batch_id = null;
      try {
        // 生成 batch_id: SC-YYMMDD-NNN
        const dayTag = check_day.slice(2).replace(/-/g, "");
        const maxRow = await env.DB.prepare(
          `SELECT batch_id FROM b2b_scan_batches WHERE batch_id LIKE ? ORDER BY batch_id DESC LIMIT 1`
        ).bind("SC-" + dayTag + "-%").first();
        let seq = 1;
        if (maxRow && maxRow.batch_id) {
          const parts = maxRow.batch_id.split("-");
          seq = (parseInt(parts[2], 10) || 0) + 1;
        }
        batch_id = "SC-" + dayTag + "-" + String(seq).padStart(3, "0");

        // 批量写入：batch + items
        const stmts = [];
        stmts.push(env.DB.prepare(
          `INSERT INTO b2b_scan_batches(batch_id,check_day,batch_name,status,total_barcodes,total_expected_boxes,created_by,created_at) VALUES(?,?,?,'open',?,?,?,?)`
        ).bind(batch_id, check_day, batch_name, items.length, totalExpected, created_by, now));

        for (const it of items) {
          const bc = String(it.outbound_barcode || "").trim();
          const cnt = parseInt(it.expected_box_count, 10);
          const cust = String(it.customer_name || "").trim();
          const summary = String(it.goods_summary || "").trim();
          stmts.push(env.DB.prepare(
            `INSERT INTO b2b_scan_items(batch_id,outbound_barcode,expected_box_count,customer_name,goods_summary,scanned_count) VALUES(?,?,?,?,?,0)`
          ).bind(batch_id, bc, cnt, cust, summary));
        }

        await env.DB.batch(stmts);

        const respSc = { ok:true, batch_id, total_barcodes: items.length, total_expected_boxes: totalExpected };
        if (req_id_sc) {
          await env.DB.prepare(`UPDATE api_idempotency_keys SET result_id=?, response_json=? WHERE action='b2b_scan_batch_create' AND request_id=?`).bind(batch_id, JSON.stringify(respSc), req_id_sc).run();
        }
        return jsonpOrJson(respSc, callback);
      } catch(e) {
        if (req_id_sc) {
          const batchRow = batch_id && await env.DB.prepare(`SELECT 1 FROM b2b_scan_batches WHERE batch_id=?`).bind(batch_id).first();
          const itemCnt = batchRow && await env.DB.prepare(`SELECT COUNT(*) AS c FROM b2b_scan_items WHERE batch_id=?`).bind(batch_id).first();
          if (batchRow && itemCnt && itemCnt.c === items.length) {
            const respSc = { ok:true, batch_id, total_barcodes: items.length, total_expected_boxes: totalExpected };
            try { await env.DB.prepare(`UPDATE api_idempotency_keys SET result_id=?, response_json=? WHERE action='b2b_scan_batch_create' AND request_id=?`).bind(batch_id, JSON.stringify(respSc), req_id_sc).run(); } catch(_){}
            return jsonpOrJson(respSc, callback);
          }
          await env.DB.prepare(`DELETE FROM api_idempotency_keys WHERE action='b2b_scan_batch_create' AND request_id=? AND response_json=''`).bind(req_id_sc).run();
        }
        const msg = String(e && e.message || e);
        if (msg.includes("UNIQUE")) return jsonpOrJson({ ok:false, error:"duplicate barcode in batch" }, callback);
        return jsonpOrJson({ ok:false, error:"batch create failed: " + msg }, callback);
      }
    }

    if (action === "b2b_scan_batch_list") {
      const hasKey = isAdmin_(p, env) || isView_(p, env);
      const start_day = String(p.start_day || "").trim();
      const end_day = String(p.end_day || "").trim();
      if (!start_day || !end_day) return jsonpOrJson({ ok:false, error:"missing start_day or end_day" }, callback);

      const sql = hasKey
        ? `SELECT * FROM b2b_scan_batches WHERE check_day >= ? AND check_day <= ? ORDER BY check_day DESC, created_at DESC`
        : `SELECT * FROM b2b_scan_batches WHERE check_day >= ? AND check_day <= ? AND status='open' ORDER BY check_day DESC, created_at DESC`;
      const rs = await env.DB.prepare(sql).bind(start_day, end_day).all();
      return jsonpOrJson({ ok:true, batches: rs.results || [] }, callback);
    }

    if (action === "b2b_scan_batch_detail") {
      const hasKey = isAdmin_(p, env) || isView_(p, env);
      const batch_id = String(p.batch_id || "").trim();
      if (!batch_id) return jsonpOrJson({ ok:false, error:"missing batch_id" }, callback);

      const batch = await env.DB.prepare(`SELECT * FROM b2b_scan_batches WHERE batch_id=?`).bind(batch_id).first();
      if (!batch) return jsonpOrJson({ ok:false, error:"batch_id not found" }, callback);
      if (!hasKey && batch.status !== "open") return jsonpOrJson({ ok:false, error:"batch is not open" }, callback);

      const itemsRs = await env.DB.prepare(
        `SELECT * FROM b2b_scan_items WHERE batch_id=? ORDER BY item_id ASC`
      ).bind(batch_id).all();
      const items = itemsRs.results || [];

      // 计划外条码汇总（含托盘号）
      const unplannedRs = await env.DB.prepare(
        `SELECT outbound_barcode, COUNT(*) as scan_times, GROUP_CONCAT(DISTINCT CASE WHEN pallet_no!='' THEN pallet_no END) as pallets FROM b2b_scan_logs WHERE batch_id=? AND is_planned=0 AND undone=0 GROUP BY outbound_barcode ORDER BY scan_times DESC`
      ).bind(batch_id).all();
      const unplanned = unplannedRs.results || [];

      // 多扫条码的托盘号（计划内且 scanned > expected 的条码）
      const overBarcodes = items.filter(it => it.scanned_count > it.expected_box_count).map(it => it.outbound_barcode);
      let over_pallets = {};
      if (overBarcodes.length > 0) {
        const placeholders = overBarcodes.map(() => "?").join(",");
        const opRs = await env.DB.prepare(
          `SELECT outbound_barcode, GROUP_CONCAT(DISTINCT CASE WHEN pallet_no!='' THEN pallet_no END) as pallets FROM b2b_scan_logs WHERE batch_id=? AND is_planned=1 AND undone=0 AND outbound_barcode IN (${placeholders}) GROUP BY outbound_barcode`
        ).bind(batch_id, ...overBarcodes).all();
        for (const r of (opRs.results || [])) {
          over_pallets[r.outbound_barcode] = r.pallets || "";
        }
      }

      // 进度
      let doneBoxes = 0;
      for (const it of items) {
        doneBoxes += Math.min(it.scanned_count, it.expected_box_count);
      }

      return jsonpOrJson({
        ok:true, batch, items, unplanned, over_pallets,
        done_boxes: doneBoxes,
        total_expected_boxes: batch.total_expected_boxes,
        progress_percent: batch.total_expected_boxes > 0 ? Math.round(doneBoxes * 100 / batch.total_expected_boxes) : 0
      }, callback);
    }

    // ===== 协同中心：作业记录波次/单号 只读查询（已优化：批量查询+内存merge） =====
    if (action === "collab_wave_list") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const start_day = String(p.start_day || "").trim();
      const end_day = String(p.end_day || "").trim();
      if (!start_day || !end_day) return jsonpOrJson({ ok:false, error:"missing start_day or end_day" }, callback);

      const startMs = new Date(start_day + "T00:00:00+09:00").getTime();
      const endMs   = new Date(end_day   + "T23:59:59.999+09:00").getTime();
      const filterBiz  = String(p.biz || "").trim();
      const filterTask = String(p.task || "").trim();
      const filterKw   = String(p.keyword || "").trim().toLowerCase();

      // 步骤1: 主聚合查询（含 operator_id 子查询，消灭 N+1）
      let whereParts = ["e1.event='wave'", "e1.ok=1", "e1.server_ms >= ?", "e1.server_ms <= ?"];
      const binds = [startMs, endMs];
      if (filterBiz)  { whereParts.push("e1.biz=?");  binds.push(filterBiz); }
      if (filterTask) { whereParts.push("e1.task=?"); binds.push(filterTask); }
      if (filterKw) {
        whereParts.push("(e1.wave_id LIKE ? OR e1.session LIKE ?)");
        const kwLike = "%" + filterKw + "%";
        binds.push(kwLike, kwLike);
      }
      const wc = whereParts.join(" AND ");

      const mainSql = `SELECT e1.biz, e1.task, e1.session, e1.wave_id,
                              MIN(e1.server_ms) as first_ms, MAX(e1.server_ms) as last_ms,
                              COUNT(*) as record_count,
                              (SELECT e2.operator_id FROM events e2
                               WHERE e2.event='wave' AND e2.ok=1
                               AND e2.biz=e1.biz AND e2.task=e1.task AND e2.session=e1.session AND e2.wave_id=e1.wave_id
                               ORDER BY e2.server_ms DESC LIMIT 1) as operator_id
                       FROM events e1
                       WHERE ${wc}
                       GROUP BY e1.biz, e1.task, e1.session, e1.wave_id
                       ORDER BY first_ms DESC`;

      const rs = await env.DB.prepare(mainSql).bind(...binds).all();
      let waves = (rs.results || []);

      // 计算 day_kst
      for (const w of waves) {
        w.operator_id = w.operator_id || "";
        w.day_kst = start_day === end_day ? start_day : new Date(w.first_ms + 9*3600*1000).toISOString().slice(0,10);
        w.detail_type = "generic_wave";
        w.detail_found = false;
      }

      // keyword 补充过滤 operator_id（子查询结果在主查询后才可用）
      if (filterKw) {
        waves = waves.filter(w =>
          (w.wave_id || "").toLowerCase().includes(filterKw) ||
          (w.session || "").toLowerCase().includes(filterKw) ||
          (w.operator_id || "").toLowerCase().includes(filterKw)
        );
      }

      // 步骤2: 批量 enrich B2B工单操作
      const woWaves = waves.filter(w => w.task === "B2B工单操作");
      if (woWaves.length > 0) {
        const woIds = [...new Set(woWaves.map(w => w.wave_id))];
        const woSessions = [...new Set(woWaves.map(w => w.session))];
        // 并行查 workorders + bindings（用 binding.day_kst 做结果查询的 key）
        const woMap = {};
        const bindingMap = {}; // session|source_order_no → {day_kst, source_type}
        const resultMap = {};
        const pWo = (async () => {
          for (let i = 0; i < woIds.length; i += 80) {
            const batch = woIds.slice(i, i + 80);
            const ph = batch.map(() => "?").join(",");
            const wrs = await env.DB.prepare(
              `SELECT workorder_id, status, customer_name, outbound_destination, order_ref_no,
                      outbound_box_count, outbound_pallet_count, has_update_notice, has_cancel_notice
               FROM b2b_workorders WHERE workorder_id IN (${ph})`
            ).bind(...batch).all();
            for (const wo of (wrs.results || [])) woMap[wo.workorder_id] = wo;
          }
        })();
        // 查 bindings 获取真实 day_kst + source_type
        const pBind = (async () => {
          for (let i = 0; i < woSessions.length; i += 80) {
            const batch = woSessions.slice(i, i + 80);
            const ph = batch.map(() => "?").join(",");
            const brs = await env.DB.prepare(
              `SELECT session_id, source_order_no, day_kst, source_type
               FROM b2b_operation_bindings WHERE session_id IN (${ph})`
            ).bind(...batch).all();
            for (const b of (brs.results || [])) {
              bindingMap[b.session_id + "|" + b.source_order_no] = { day_kst: b.day_kst, source_type: b.source_type };
            }
          }
        })();
        await Promise.all([pWo, pBind]);

        // 用 binding.day_kst 查结果单
        const resultKeys = new Set();
        for (const w of woWaves) {
          const bk = bindingMap[w.session + "|" + w.wave_id];
          if (bk) resultKeys.add(bk.day_kst + "|" + bk.source_type + "|" + w.wave_id);
        }
        const rkArr = [...resultKeys];
        for (let i = 0; i < rkArr.length; i += 80) {
          const batch = rkArr.slice(i, i + 80);
          // 逐条查效率不高，改为用 source_order_no IN + day_kst IN 批量查
          const orderNos = [...new Set(batch.map(k => k.split("|")[2]))];
          const days = [...new Set(batch.map(k => k.split("|")[0]))];
          const ph = orderNos.map(() => "?").join(",");
          const dayPh = days.map(() => "?").join(",");
          const rrs = await env.DB.prepare(
            `SELECT day_kst, source_type, source_order_no, status, operation_mode, confirm_badge
             FROM b2b_operation_results WHERE source_order_no IN (${ph}) AND day_kst IN (${dayPh})`
          ).bind(...orderNos, ...days).all();
          for (const r of (rrs.results || [])) resultMap[r.day_kst + "|" + r.source_type + "|" + r.source_order_no] = r;
        }

        for (const w of woWaves) {
          w.detail_type = "b2b_workorder";
          const wo = woMap[w.wave_id];
          if (wo) {
            w.detail_found = true;
            w.wo_status = wo.status; w.customer_name = wo.customer_name || "";
            w.outbound_destination = wo.outbound_destination || "";
            w.order_ref_no = wo.order_ref_no || "";
            w.outbound_box_count = wo.outbound_box_count || 0;
            w.outbound_pallet_count = wo.outbound_pallet_count || 0;
            w.has_update_notice = wo.has_update_notice || 0;
            w.has_cancel_notice = wo.has_cancel_notice || 0;
          }
          // 用 binding 的真实 day_kst + source_type 查结果
          const bk = bindingMap[w.session + "|" + w.wave_id];
          if (bk) {
            const result = resultMap[bk.day_kst + "|" + bk.source_type + "|" + w.wave_id];
            if (result) {
              w.result_status = result.status || "";
              w.result_operation_mode = result.operation_mode || "";
              w.result_confirm_badge = result.confirm_badge || "";
            }
          }
        }
      }

      // 步骤3: 批量 enrich B2B现场记录
      const foWaves = waves.filter(w => w.task === "B2B现场记录");
      if (foWaves.length > 0) {
        const foIds = [...new Set(foWaves.map(w => w.wave_id))];
        const foMap = {};
        for (let i = 0; i < foIds.length; i += 80) {
          const batch = foIds.slice(i, i + 80);
          const ph = batch.map(() => "?").join(",");
          const frs = await env.DB.prepare(
            `SELECT record_id, status, customer_name, source_plan_id, bound_workorder_id, operation_type
             FROM b2b_field_ops WHERE record_id IN (${ph})`
          ).bind(...batch).all();
          for (const f of (frs.results || [])) foMap[f.record_id] = f;
        }
        for (const w of foWaves) {
          w.detail_type = "b2b_field_op";
          const fo = foMap[w.wave_id];
          if (fo) {
            w.detail_found = true;
            w.fo_status = fo.status; w.customer_name = fo.customer_name || "";
            w.source_plan_id = fo.source_plan_id || "";
            w.bound_workorder_id = fo.bound_workorder_id || "";
            w.operation_type = fo.operation_type || "";
          }
        }
      }

      return jsonpOrJson({ ok:true, waves }, callback);
    }

    // ===== 协同中心：按工单明细（B2B工单操作专用） =====
    if (action === "collab_workorder_detail") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const start_day = String(p.start_day || "").trim();
      const end_day   = String(p.end_day || "").trim();
      if (!start_day || !end_day) return jsonpOrJson({ ok:false, error:"missing start_day or end_day" }, callback);

      const filterStatus = String(p.status || "").trim();   // draft | temporary_completed | completed | no_result | ""
      const filterQ      = String(p.q || "").trim().toLowerCase();
      const page      = Math.max(1, parseInt(p.page) || 1);
      const page_size = Math.min(200, Math.max(1, parseInt(p.page_size) || 50));

      // ---- Step 1: 以 bindings.day_kst 为日期口径，聚合出工单列表 ----
      let bWhere = ["b.day_kst >= ?", "b.day_kst <= ?"];
      let bBinds = [start_day, end_day];

      // 搜索
      if (filterQ) {
        const qLike = "%" + filterQ + "%";
        bWhere.push(`(b.source_order_no LIKE ? OR b.session_id LIKE ? OR b.badge LIKE ?
          OR b.source_order_no IN (SELECT source_order_no FROM b2b_operation_results WHERE customer_name LIKE ? AND day_kst >= ? AND day_kst <= ?)
          OR b.source_order_no IN (SELECT source_order_no FROM b2b_operation_labor_details WHERE (operator_badge LIKE ? OR operator_name LIKE ?) AND day_kst >= ? AND day_kst <= ?))`);
        bBinds.push(qLike, qLike, qLike, qLike, start_day, end_day, qLike, qLike, start_day, end_day);
      }

      const bWhereClause = bWhere.join(" AND ");

      // 先拿工单 distinct keys（day_kst + source_order_no）
      const keySql = `SELECT b.day_kst, b.source_type, b.source_order_no, b.internal_workorder_id,
            GROUP_CONCAT(DISTINCT b.session_id) as session_ids,
            COUNT(DISTINCT b.session_id) as session_count,
            MAX(b.bound_at) as last_activity_at
          FROM b2b_operation_bindings b
          WHERE ${bWhereClause}
          GROUP BY b.day_kst, b.source_type, b.source_order_no
          ORDER BY last_activity_at DESC`;

      const allKeysRs = await env.DB.prepare(keySql).bind(...bBinds).all();
      let allKeys = allKeysRs.results || [];

      // ---- Step 2: 批量查 results ----
      const orderNos = [...new Set(allKeys.map(k => k.source_order_no))];
      const resultMap = {};  // "day_kst|source_order_no" → result row
      for (let i = 0; i < orderNos.length; i += 80) {
        const batch = orderNos.slice(i, i + 80);
        const ph = batch.map(() => "?").join(",");
        const rrs = await env.DB.prepare(
          `SELECT day_kst, source_type, source_order_no, status, workflow_status, operation_mode,
                  customer_name, sku_kind_count, box_count, pallet_count,
                  packed_qty, packed_box_count, label_count, photo_count,
                  did_pack, did_rebox, rebox_count,
                  needs_forklift_pick, forklift_pallet_count, rack_pick_location_count,
                  remark, confirm_badge, confirmed_by,
                  result_entered_by_badge, result_entered_by_name, result_entered_at,
                  confirmed_at, reviewed_by_badge, reviewed_by_name, reviewed_at,
                  temporary_completed_at, completed_at, created_at, updated_at
           FROM b2b_operation_results WHERE day_kst >= ? AND day_kst <= ? AND source_order_no IN (${ph})`
        ).bind(start_day, end_day, ...batch).all();
        for (const rr of (rrs.results || [])) {
          resultMap[rr.day_kst + "|" + rr.source_order_no] = rr;
        }
      }

      // ---- Step 3: 批量查 workorders（内部工单） ----
      const internalIds = [...new Set(allKeys.filter(k => k.source_type === "internal_b2b_workorder").map(k => k.source_order_no))];
      const woMap = {};
      for (let i = 0; i < internalIds.length; i += 80) {
        const batch = internalIds.slice(i, i + 80);
        const ph = batch.map(() => "?").join(",");
        const wrs = await env.DB.prepare(
          `SELECT workorder_id, status, customer_name, outbound_destination, order_ref_no,
                  outbound_box_count, outbound_pallet_count, operation_mode,
                  has_update_notice, has_cancel_notice
           FROM b2b_workorders WHERE workorder_id IN (${ph})`
        ).bind(...batch).all();
        for (const w of (wrs.results || [])) woMap[w.workorder_id] = w;
      }

      // ---- Step 4: 计算 display_status 并过滤 ----
      function calcDisplayStatus(result, wo) {
        if (!result) return "无结果单";
        const wfs = (result.workflow_status || "").trim();
        if (wfs === "completed") return "已完成";
        if (wfs === "pending_result") return "暂时完成";
        if (wfs === "pending_review") return "待审核";
        if (wfs === "working") return "操作中";
        const st = (result.status || "").trim();
        if (st === "completed") return "已完成";
        if (st === "draft") return "草稿";
        return "草稿";
      }

      // 组装行并过滤 status
      let assembled = [];
      for (const k of allKeys) {
        const rk = k.day_kst + "|" + k.source_order_no;
        const result = resultMap[rk] || null;
        const wo = woMap[k.source_order_no] || null;
        const ds = calcDisplayStatus(result, wo);

        if (filterStatus) {
          if (filterStatus === "draft" && ds !== "草稿") continue;
          if (filterStatus === "working" && ds !== "操作中") continue;
          if (filterStatus === "temporary_completed" && ds !== "暂时完成") continue;
          if (filterStatus === "completed" && ds !== "已完成") continue;
          if (filterStatus === "no_result" && ds !== "无结果单") continue;
        }

        assembled.push({
          day_kst: k.day_kst,
          source_type: k.source_type,
          source_order_no: k.source_order_no,
          internal_workorder_id: k.internal_workorder_id || "",
          session_ids_str: k.session_ids || "",
          session_count: k.session_count || 0,
          last_activity_at: k.last_activity_at || 0,
          result, wo, display_status: ds
        });
      }

      const total = assembled.length;
      const paged = assembled.slice((page - 1) * page_size, page * page_size);

      // ---- Step 5: 批量查 labor_details（只查当页） ----
      const pagedOrderNos = [...new Set(paged.map(r => r.source_order_no))];
      const laborMap = {};  // "day_kst|source_order_no" → [{badge, name, minutes}]
      for (let i = 0; i < pagedOrderNos.length; i += 80) {
        const batch = pagedOrderNos.slice(i, i + 80);
        const ph = batch.map(() => "?").join(",");
        const lrs = await env.DB.prepare(
          `SELECT day_kst, source_order_no, operator_badge, operator_name,
                  SUM(COALESCE(duration_minutes, 0)) as total_minutes
           FROM b2b_operation_labor_details
           WHERE day_kst >= ? AND day_kst <= ? AND source_order_no IN (${ph})
           GROUP BY day_kst, source_order_no, operator_badge
           ORDER BY total_minutes DESC`
        ).bind(start_day, end_day, ...batch).all();
        for (const lr of (lrs.results || [])) {
          const lk = lr.day_kst + "|" + lr.source_order_no;
          if (!laborMap[lk]) laborMap[lk] = [];
          laborMap[lk].push({
            badge: lr.operator_badge || "",
            display_name: lr.operator_name || lr.operator_badge || "",
            minutes: Math.round((lr.total_minutes || 0) * 100) / 100
          });
        }
      }

      // ---- Step 6: 批量查 sessions 摘要（只查当页） ----
      const pagedSessionIds = [];
      for (const r of paged) {
        if (r.session_ids_str) r.session_ids_str.split(",").forEach(s => { if (s) pagedSessionIds.push(s); });
      }
      const uniqueSessionIds = [...new Set(pagedSessionIds)];
      const sessionMap = {};
      for (let i = 0; i < uniqueSessionIds.length; i += 80) {
        const batch = uniqueSessionIds.slice(i, i + 80);
        const ph = batch.map(() => "?").join(",");
        const srs = await env.DB.prepare(
          `SELECT session, status, created_ms, closed_ms, owner_operator_id
           FROM sessions WHERE session IN (${ph})`
        ).bind(...batch).all();
        for (const sr of (srs.results || [])) {
          sessionMap[sr.session] = {
            session_id: sr.session,
            started_at: sr.created_ms || 0,
            ended_at: sr.closed_ms || 0,
            status: sr.status || "",
            owner_badge: sr.owner_operator_id || ""
          };
        }
      }

      // ---- Step 7: 组装最终输出 ----
      const docs = paged.map(function(r) {
        const lk = r.day_kst + "|" + r.source_order_no;
        const workers = laborMap[lk] || [];
        const totalMinutes = workers.reduce(function(sum, w){ return sum + w.minutes; }, 0);

        const sessionIdArr = r.session_ids_str ? r.session_ids_str.split(",").filter(Boolean) : [];
        const sessions = sessionIdArr.map(function(sid){
          return sessionMap[sid] || { session_id: sid, started_at: 0, ended_at: 0, status: "", owner_badge: "" };
        });

        // result summary
        const rs = r.result || {};
        const resultSummary = r.result ? {
          status: rs.status || "draft",
          workflow_status: rs.workflow_status || "",
          operation_mode: rs.operation_mode || "",
          customer_name: rs.customer_name || "",
          box_count: rs.box_count || 0,
          pallet_count: rs.pallet_count || 0,
          packed_qty: rs.packed_qty || 0,
          sku_kind_count: rs.sku_kind_count || 0,
          packed_box_count: rs.packed_box_count || 0,
          label_count: rs.label_count || 0,
          photo_count: rs.photo_count || 0,
          did_pack: rs.did_pack || 0,
          did_rebox: rs.did_rebox || 0,
          rebox_count: rs.rebox_count || 0,
          needs_forklift_pick: rs.needs_forklift_pick || 0,
          forklift_pallet_count: rs.forklift_pallet_count || 0,
          rack_pick_location_count: rs.rack_pick_location_count || 0,
          remark: rs.remark || "",
          confirm_badge: rs.confirm_badge || "",
          confirmed_by: rs.confirmed_by || ""
        } : null;

        // wo info
        const wo = r.wo || {};
        const woInfo = r.wo ? {
          wo_status: wo.status || "",
          customer_name: wo.customer_name || "",
          operation_mode: wo.operation_mode || "",
          has_update_notice: wo.has_update_notice || 0,
          has_cancel_notice: wo.has_cancel_notice || 0
        } : null;

        const customerName = (rs.customer_name) || (wo.customer_name) || "";

        return {
          workorder_id: r.source_order_no,
          source_type: r.source_type,
          internal_workorder_id: r.internal_workorder_id,
          biz: "B2B",
          task: "B2B工单操作",
          operation_day: r.day_kst,
          display_status: r.display_status,
          workflow_status: (rs.workflow_status) || "",
          result_status: (rs.status) || "",
          customer_name: customerName,
          worker_count: workers.length,
          total_minutes: Math.round(totalMinutes * 100) / 100,
          workers: workers,
          session_count: r.session_count,
          session_ids: sessionIdArr,
          sessions: sessions,
          result_summary: resultSummary,
          wo_info: woInfo,
          last_activity_at: r.last_activity_at
        };
      });

      return jsonpOrJson({
        ok: true, docs, total, page, page_size,
        has_more: page * page_size < total
      }, callback);
    }

    // ===== 协同中心：单据台账查询（session级 / summary级） =====
    if (action === "collab_doc_list" || action === "collab_doc_export") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const start_day = String(p.start_day || "").trim();
      const end_day   = String(p.end_day || "").trim();
      if (!start_day || !end_day) return jsonpOrJson({ ok:false, error:"missing start_day or end_day" }, callback);

      const startMs = new Date(start_day + "T00:00:00+09:00").getTime();
      const endMs   = new Date(end_day   + "T23:59:59.999+09:00").getTime();
      const filterWaveKind = String(p.wave_kind || "").trim();
      const filterKw       = String(p.keyword || "").trim().toLowerCase();
      const summaryMode    = String(p.summary_mode || "1") === "1";  // 默认汇总视图
      const isExport       = action === "collab_doc_export";
      const page      = Math.max(1, parseInt(p.page) || 1);
      const page_size = isExport ? 5000 : Math.min(200, Math.max(1, parseInt(p.page_size) || 50));

      // wave_kind ↔ (biz, task) 映射
      const WAVE_KIND_MAP = {
        b2c_pick:           { biz:"B2C", task:"拣货" },
        b2c_tally:          { biz:"B2C", task:"理货" },
        b2c_batch_out:      { biz:"B2C", task:"批量出库" },
        b2b_inbound_tally:  { biz:"B2B", task:"B2B入库理货" },
        b2b_workorder:      { biz:"B2B", task:"B2B工单操作" },
        b2b_field_op:       { biz:"B2B", task:"B2B现场记录" }
      };
      const TASK_TO_KIND = {};
      for (const [k, v] of Object.entries(WAVE_KIND_MAP)) TASK_TO_KIND[v.biz + "|" + v.task] = k;
      const DOC_CLASS = { b2b_workorder:"full_doc", b2b_field_op:"full_doc" };

      // 构建主查询 WHERE
      let whereParts = ["event='wave'", "ok=1", "server_ms >= ?", "server_ms <= ?"];
      let binds = [startMs, endMs];
      if (filterWaveKind && WAVE_KIND_MAP[filterWaveKind]) {
        whereParts.push("biz=?", "task=?");
        binds.push(WAVE_KIND_MAP[filterWaveKind].biz, WAVE_KIND_MAP[filterWaveKind].task);
      }
      const dayExpr = "substr(datetime(server_ms/1000,'unixepoch','+9 hours'),1,10)";

      // keyword 全局搜索：wave_id / session / work_day_kst / customer_name
      // customer_name 用子查询覆盖三张表，无截断、无 bind 上限问题
      if (filterKw) {
        const kwLike = "%" + filterKw + "%";
        whereParts.push(`(wave_id LIKE ? OR session LIKE ? OR ${dayExpr} LIKE ?
          OR wave_id IN (SELECT source_order_no FROM b2b_operation_results WHERE customer_name LIKE ?)
          OR wave_id IN (SELECT record_id FROM b2b_field_ops WHERE customer_name LIKE ?)
          OR wave_id IN (SELECT workorder_id FROM b2b_workorders WHERE customer_name LIKE ?))`);
        binds.push(kwLike, kwLike, kwLike, kwLike, kwLike, kwLike);
      }
      const whereClause = whereParts.join(" AND ");

      let rows, total;
      if (summaryMode) {
        // --- Summary 级: GROUP BY biz, task, work_day_kst, wave_id ---
        const countSql = `SELECT COUNT(*) as cnt FROM (SELECT 1 FROM events WHERE ${whereClause} GROUP BY biz, task, ${dayExpr}, wave_id)`;
        const countRs = await env.DB.prepare(countSql).bind(...binds).first();
        total = countRs ? countRs.cnt : 0;

        const mainSql = `SELECT biz, task, wave_id,
          ${dayExpr} as work_day_kst,
          COUNT(DISTINCT session) as session_count,
          MIN(server_ms) as first_ms, MAX(server_ms) as last_ms,
          COUNT(*) as record_count
          FROM events WHERE ${whereClause}
          GROUP BY biz, task, ${dayExpr}, wave_id
          ORDER BY first_ms DESC
          LIMIT ? OFFSET ?`;
        const rs = await env.DB.prepare(mainSql).bind(...binds, page_size, (page - 1) * page_size).all();
        rows = rs.results || [];
      } else {
        // --- Session 级: GROUP BY biz, task, work_day_kst, wave_id, session ---
        const countSql = `SELECT COUNT(*) as cnt FROM (SELECT 1 FROM events WHERE ${whereClause} GROUP BY biz, task, ${dayExpr}, wave_id, session)`;
        const countRs = await env.DB.prepare(countSql).bind(...binds).first();
        total = countRs ? countRs.cnt : 0;

        const mainSql = `SELECT biz, task, wave_id, session,
          ${dayExpr} as work_day_kst,
          MIN(server_ms) as first_ms, MAX(server_ms) as last_ms,
          COUNT(*) as record_count
          FROM events WHERE ${whereClause}
          GROUP BY biz, task, ${dayExpr}, wave_id, session
          ORDER BY first_ms DESC
          LIMIT ? OFFSET ?`;
        const rs = await env.DB.prepare(mainSql).bind(...binds, page_size, (page - 1) * page_size).all();
        rows = rs.results || [];
      }

      // 补充 wave_kind / doc_class
      for (const r of rows) {
        r.wave_kind = TASK_TO_KIND[r.biz + "|" + r.task] || "unknown";
        r.doc_class = DOC_CLASS[r.wave_kind] || "wave_only";
        r.link_status = (r.doc_class === "wave_only") ? "unlinked" : "pending_enrich";
      }

      // ===== 批量查 session 参与工牌 =====
      // V1 口径：session_badge_count / session_badge_list = session 内 event='join' 的 DISTINCT badge
      // 不是按 wave 时间窗精确切片的人数，是整个 session 的参与人员
      const allSessions = [...new Set(rows.map(r => r.session).filter(Boolean))];
      const badgeMap = {};  // session → { badge_count, badge_list }
      if (allSessions.length > 0) {
        // D1 单次 bind 上限约 100，分批
        for (let i = 0; i < allSessions.length; i += 80) {
          const batch = allSessions.slice(i, i + 80);
          const ph = batch.map(() => "?").join(",");
          const brs = await env.DB.prepare(
            `SELECT session, COUNT(DISTINCT badge) as badge_count, GROUP_CONCAT(DISTINCT badge) as badge_list
             FROM events WHERE event='join' AND ok=1 AND session IN (${ph})
             GROUP BY session`
          ).bind(...batch).all();
          for (const b of (brs.results || [])) {
            badgeMap[b.session] = { badge_count: b.badge_count || 0, badge_list: b.badge_list || "" };
          }
        }
      }

      // summary 模式需要合并多 session 的 badge
      if (summaryMode && allSessions.length > 0) {
        // 需要知道每个 (biz,task,day,wave_id) 包含哪些 session → 查一次
        const sessionMapSql = `SELECT biz, task, ${dayExpr} as work_day_kst, wave_id, session
          FROM events WHERE ${whereClause} AND session IN (${allSessions.map(()=>"?").join(",")})
          GROUP BY biz, task, ${dayExpr}, wave_id, session`;
        // 但这个查询太重，改为：直接用 rows 的 wave_id 去查对应 sessions
        const waveKeys = rows.map(r => r.biz + "|" + r.task + "|" + r.work_day_kst + "|" + r.wave_id);
        const waveKeySet = [...new Set(waveKeys)];

        // 批量查每个 wave 的 sessions
        const waveSessionMap = {};  // waveKey → [session, ...]
        for (let i = 0; i < waveKeySet.length; i += 20) {
          const batch = waveKeySet.slice(i, i + 20);
          // 用 OR 条件构建
          const orParts = [];
          const orBinds = [];
          for (const key of batch) {
            const [biz, task, day, wid] = key.split("|");
            orParts.push(`(biz=? AND task=? AND ${dayExpr}=? AND wave_id=?)`);
            orBinds.push(biz, task, day, wid);
          }
          const sessionRs = await env.DB.prepare(
            `SELECT biz, task, ${dayExpr} as work_day_kst, wave_id, session
             FROM events WHERE event='wave' AND ok=1 AND server_ms >= ? AND server_ms <= ?
             AND (${orParts.join(" OR ")})
             GROUP BY biz, task, ${dayExpr}, wave_id, session`
          ).bind(startMs, endMs, ...orBinds).all();
          for (const sr of (sessionRs.results || [])) {
            const k = sr.biz + "|" + sr.task + "|" + sr.work_day_kst + "|" + sr.wave_id;
            if (!waveSessionMap[k]) waveSessionMap[k] = [];
            waveSessionMap[k].push(sr.session);
          }
        }

        // 合并 badge
        for (const r of rows) {
          const k = r.biz + "|" + r.task + "|" + r.work_day_kst + "|" + r.wave_id;
          const sessions = waveSessionMap[k] || [];
          const allBadges = new Set();
          for (const s of sessions) {
            const b = badgeMap[s];
            if (b && b.badge_list) b.badge_list.split(",").forEach(x => { if (x) allBadges.add(x); });
          }
          r.session_badge_count = allBadges.size;
          r.session_badge_list = [...allBadges].join(",");
        }
      } else {
        // session 级直接匹配
        for (const r of rows) {
          const b = badgeMap[r.session] || {};
          r.session_badge_count = b.badge_count || 0;
          r.session_badge_list = b.badge_list || "";
        }
      }

      // ===== Enrichment: B2B工单操作 =====
      const woRows = rows.filter(r => r.wave_kind === "b2b_workorder");
      if (woRows.length > 0) {
        // 步骤1: 批量查 bindings，key = session_id + source_order_no(=wave_id)
        const woSessions = [...new Set(woRows.map(r => r.session).filter(Boolean))];
        const bindingMap = {};  // "session|wave_id" → binding
        const ambiguousSet = new Set();  // "session|wave_id" 有多条 binding

        for (let i = 0; i < woSessions.length; i += 80) {
          const batch = woSessions.slice(i, i + 80);
          const ph = batch.map(() => "?").join(",");
          const brs = await env.DB.prepare(
            `SELECT session_id, source_type, source_order_no, internal_workorder_id, day_kst, badge, bound_at
             FROM b2b_operation_bindings WHERE session_id IN (${ph})`
          ).bind(...batch).all();
          for (const b of (brs.results || [])) {
            const key = b.session_id + "|" + b.source_order_no;
            if (bindingMap[key]) {
              // 同一 session+source_order_no 出现多条 → ambiguous
              ambiguousSet.add(key);
            } else {
              bindingMap[key] = b;
            }
          }
        }

        // 收集需要查 results 的 (day_kst, source_type, source_order_no) 组合
        const resultKeys = new Set();
        const internalWoIds = new Set();
        for (const r of woRows) {
          const bk = (r.session || "") + "|" + r.wave_id;
          const binding = bindingMap[bk];
          if (binding) {
            resultKeys.add(binding.day_kst + "|" + binding.source_type + "|" + binding.source_order_no);
            if (binding.source_type === "internal_b2b_workorder") {
              internalWoIds.add(binding.source_order_no);
            }
          }
          // summary 模式下可能没有 session → 用 wave_id+day 查 binding
        }

        // summary 模式补充：wave_id 直接查 bindings（不限 day_kst，用 binding 自身 day_kst），含冲突检测
        const summaryAmbiguousSet = new Set();  // day_kst|source_order_no 有不一致 binding
        if (summaryMode) {
          const waveIds = [...new Set(woRows.map(r => r.wave_id))];
          for (let i = 0; i < waveIds.length; i += 80) {
            const batch = waveIds.slice(i, i + 80);
            const ph = batch.map(() => "?").join(",");
            const brs = await env.DB.prepare(
              `SELECT session_id, source_type, source_order_no, internal_workorder_id, day_kst, badge, bound_at
               FROM b2b_operation_bindings WHERE source_order_no IN (${ph})`
            ).bind(...batch).all();
            for (const b of (brs.results || [])) {
              resultKeys.add(b.day_kst + "|" + b.source_type + "|" + b.source_order_no);
              if (b.source_type === "internal_b2b_workorder") internalWoIds.add(b.source_order_no);
              // summary 模式：按 wave_id 聚合 binding，检测是否有不一致
              const wk = b.source_order_no;
              const existing = bindingMap["_wave_" + wk];
              if (!existing) {
                bindingMap["_wave_" + wk] = b;
              } else {
                // 检查 source_type / internal_workorder_id / day_kst 是否一致
                if (existing.source_type !== b.source_type ||
                    existing.day_kst !== b.day_kst ||
                    (existing.internal_workorder_id || "") !== (b.internal_workorder_id || "")) {
                  summaryAmbiguousSet.add(wk);
                }
              }
            }
          }
        }

        // 步骤2 & 3 并行: 查 results + 查 workorders
        const resultMap = {};  // "day_kst|source_type|source_order_no" → result
        const woMap = {};      // workorder_id → wo

        const resultKeysArr = [...resultKeys];
        const internalWoIdsArr = [...internalWoIds];

        const p2 = (async () => {
          for (let i = 0; i < resultKeysArr.length; i += 30) {
            const batch = resultKeysArr.slice(i, i + 30);
            const orParts = batch.map(() => "(day_kst=? AND source_type=? AND source_order_no=?)");
            const orBinds = [];
            for (const k of batch) {
              const [d, st, so] = k.split("|");
              orBinds.push(d, st, so);
            }
            const rrs = await env.DB.prepare(
              `SELECT day_kst, source_type, source_order_no, status, operation_mode,
                      confirm_badge, confirmed_by, sku_kind_count, box_count, pallet_count,
                      packed_qty, packed_box_count, used_carton, big_carton_count, small_carton_count,
                      label_count, photo_count, did_pack, did_rebox, rebox_count,
                      needs_forklift_pick, forklift_pallet_count, rack_pick_location_count, remark,
                      internal_workorder_id, customer_name
               FROM b2b_operation_results WHERE ${orParts.join(" OR ")}`
            ).bind(...orBinds).all();
            for (const rr of (rrs.results || [])) {
              resultMap[rr.day_kst + "|" + rr.source_type + "|" + rr.source_order_no] = rr;
            }
          }
        })();

        const p3 = (async () => {
          for (let i = 0; i < internalWoIdsArr.length; i += 80) {
            const batch = internalWoIdsArr.slice(i, i + 80);
            const ph = batch.map(() => "?").join(",");
            const wrs = await env.DB.prepare(
              `SELECT workorder_id, status, customer_name, outbound_destination, order_ref_no,
                      outbound_box_count, outbound_pallet_count, operation_mode,
                      has_update_notice, has_cancel_notice
               FROM b2b_workorders WHERE workorder_id IN (${ph})`
            ).bind(...batch).all();
            for (const w of (wrs.results || [])) woMap[w.workorder_id] = w;
          }
        })();

        await Promise.all([p2, p3]);

        // 步骤4: merge
        for (const r of woRows) {
          let binding;
          if (summaryMode) {
            const swk = r.wave_id;
            if (summaryAmbiguousSet.has(swk)) { r.link_status = "ambiguous_binding"; continue; }
            binding = bindingMap["_wave_" + swk];
          } else {
            const bk = (r.session || "") + "|" + r.wave_id;
            if (ambiguousSet.has(bk)) { r.link_status = "ambiguous_binding"; continue; }
            binding = bindingMap[bk];
          }

          if (!binding) { r.link_status = "no_binding"; continue; }

          r.source_type = binding.source_type || "";
          r.internal_workorder_id = binding.internal_workorder_id || "";
          const prefix = binding.source_type === "internal_b2b_workorder" ? "internal" :
                         binding.source_type === "external_wms_workorder" ? "external" : "other";

          const rk = binding.day_kst + "|" + binding.source_type + "|" + binding.source_order_no;
          const result = resultMap[rk];
          if (!result) {
            r.link_status = prefix + "_bound_result_missing";
          } else {
            const rs_status = result.status || "draft";
            r.link_status = prefix + "_bound_result_" + rs_status;
            r.customer_name = result.customer_name || "";
            r.operation_mode = result.operation_mode || "";
            r.result_status = rs_status;
            r.confirm_badge = result.confirm_badge || "";
            r.confirmed_by = result.confirmed_by || "";
            r.sku_kind_count = result.sku_kind_count || 0;
            r.box_count = result.box_count || 0;
            r.pallet_count = result.pallet_count || 0;
            r.packed_qty = result.packed_qty || 0;
            r.packed_box_count = result.packed_box_count || 0;
            r.used_carton = result.used_carton || 0;
            r.big_carton_count = result.big_carton_count || 0;
            r.small_carton_count = result.small_carton_count || 0;
            r.label_count = result.label_count || 0;
            r.photo_count = result.photo_count || 0;
            r.did_pack = result.did_pack || 0;
            r.did_rebox = result.did_rebox || 0;
            r.rebox_count = result.rebox_count || 0;
            r.needs_forklift_pick = result.needs_forklift_pick || 0;
            r.forklift_pallet_count = result.forklift_pallet_count || 0;
            r.rack_pick_location_count = result.rack_pick_location_count || 0;
            r.remark = result.remark || "";
          }

          // 内部工单补充
          if (binding.source_type === "internal_b2b_workorder") {
            const wo = woMap[binding.source_order_no];
            if (wo) {
              if (!r.customer_name) r.customer_name = wo.customer_name || "";
              r.wo_status = wo.status || "";
              r.outbound_destination = wo.outbound_destination || "";
              r.order_ref_no = wo.order_ref_no || "";
              r.wo_box_count = wo.outbound_box_count || 0;
              r.wo_pallet_count = wo.outbound_pallet_count || 0;
              r.has_update_notice = wo.has_update_notice || 0;
              r.has_cancel_notice = wo.has_cancel_notice || 0;
              if (!r.operation_mode) r.operation_mode = wo.operation_mode || "";
            }
          }
        }
      }

      // ===== Enrichment: B2B现场记录 =====
      const foRows = rows.filter(r => r.wave_kind === "b2b_field_op");
      if (foRows.length > 0) {
        const foIds = [...new Set(foRows.map(r => r.wave_id))];
        const foMap = {};

        for (let i = 0; i < foIds.length; i += 80) {
          const batch = foIds.slice(i, i + 80);
          const ph = batch.map(() => "?").join(",");
          const frs = await env.DB.prepare(
            `SELECT record_id, status, customer_name, source_plan_id, bound_workorder_id, operation_type,
                    plan_day, input_box_count, output_box_count, output_pallet_count,
                    packed_qty, packed_box_count, used_carton, big_carton_count, small_carton_count,
                    label_count, photo_count, did_pack, did_rebox, rebox_count,
                    needs_forklift_pick, forklift_pallet_count, rack_pick_location_count,
                    created_by, created_at, completed_at
             FROM b2b_field_ops WHERE record_id IN (${ph})`
          ).bind(...batch).all();
          for (const f of (frs.results || [])) foMap[f.record_id] = f;
        }

        for (const r of foRows) {
          const fo = foMap[r.wave_id];
          if (!fo) { r.link_status = "no_record"; continue; }

          const planLinked = !!(fo.source_plan_id);
          const woBound = !!(fo.bound_workorder_id);
          if (planLinked && woBound) r.link_status = "plan_linked_wo_bound";
          else if (planLinked) r.link_status = "plan_linked_wo_unbound";
          else if (woBound) r.link_status = "independent_wo_bound";
          else r.link_status = "independent";

          r.customer_name = fo.customer_name || "";
          r.fo_status = fo.status || "";
          r.operation_type = fo.operation_type || "";
          r.source_plan_id = fo.source_plan_id || "";
          r.bound_workorder_id = fo.bound_workorder_id || "";
          r.plan_day = fo.plan_day || "";
          r.input_box_count = fo.input_box_count || 0;
          r.output_box_count = fo.output_box_count || 0;
          r.output_pallet_count = fo.output_pallet_count || 0;
          r.packed_qty = fo.packed_qty || 0;
          r.packed_box_count = fo.packed_box_count || 0;
          r.used_carton = fo.used_carton || 0;
          r.big_carton_count = fo.big_carton_count || 0;
          r.small_carton_count = fo.small_carton_count || 0;
          r.label_count = fo.label_count || 0;
          r.photo_count = fo.photo_count || 0;
          r.did_pack = fo.did_pack || 0;
          r.did_rebox = fo.did_rebox || 0;
          r.rebox_count = fo.rebox_count || 0;
          r.needs_forklift_pick = fo.needs_forklift_pick || 0;
          r.forklift_pallet_count = fo.forklift_pallet_count || 0;
          r.rack_pick_location_count = fo.rack_pick_location_count || 0;
          r.created_by = fo.created_by || "";
          r.fo_created_at = fo.created_at || 0;
          r.fo_completed_at = fo.completed_at || 0;
        }
      }

      // ===== 返回或导出 =====
      if (isExport) {
        // CSV 导出（UTF-8 BOM）
        const CSV_HEADERS = [
          "作业日期","业务线","任务类型","类型标签","单据分类","单号",
          "关联状态","客户名","状态",
          "操作类型/模式","关联工单/计划",
          "首次作业时间","最后作业时间",
          summaryMode ? "session数" : "session",
          "参与人数(V1:session级)","参与工牌列表(V1:session级)",
          "扫码次数",
          "箱数","托数","件数","SKU种数",
          "打包箱数","标签数","照片数",
          "是否用纸箱","大纸箱数","小纸箱数",
          "是否换箱","换箱次数",
          "是否叉车拣货","叉车托数","库位数",
          "备注","确认工牌","确认人"
        ];
        const fmtTime = ms => ms ? new Date(ms).toISOString().replace("T"," ").slice(0,19) : "";
        const WAVE_KIND_LABEL = {
          b2c_pick:"B2C拣货", b2c_tally:"B2C理货", b2c_batch_out:"B2C批量出库",
          b2b_inbound_tally:"B2B入库理货", b2b_workorder:"B2B工单操作", b2b_field_op:"B2B现场记录"
        };

        const csvRows = [CSV_HEADERS];
        for (const r of rows) {
          let status = "";
          let opMode = "";
          let relatedDoc = "";
          if (r.wave_kind === "b2b_workorder") {
            status = r.result_status || r.wo_status || "";
            opMode = r.operation_mode || "";
            relatedDoc = r.internal_workorder_id || "";
          } else if (r.wave_kind === "b2b_field_op") {
            status = r.fo_status || "";
            opMode = r.operation_type || "";
            relatedDoc = [r.source_plan_id, r.bound_workorder_id].filter(Boolean).join(" / ");
          }
          csvRows.push([
            r.work_day_kst || "",
            r.biz || "",
            r.task || "",
            WAVE_KIND_LABEL[r.wave_kind] || r.wave_kind || "",
            r.doc_class || "",
            r.wave_id || "",
            r.link_status || "",
            r.customer_name || "",
            status,
            opMode,
            relatedDoc,
            fmtTime(r.first_ms),
            fmtTime(r.last_ms),
            summaryMode ? (r.session_count || 1) : (r.session || ""),
            r.session_badge_count || 0,
            r.session_badge_list || "",
            r.record_count || 0,
            r.box_count || r.output_box_count || 0,
            r.pallet_count || r.output_pallet_count || 0,
            r.packed_qty || 0,
            r.sku_kind_count || 0,
            r.packed_box_count || 0,
            r.label_count || 0,
            r.photo_count || 0,
            r.used_carton || 0,
            r.big_carton_count || 0,
            r.small_carton_count || 0,
            r.did_rebox || 0,
            r.rebox_count || 0,
            r.needs_forklift_pick || 0,
            r.forklift_pallet_count || 0,
            r.rack_pick_location_count || 0,
            r.remark || "",
            r.confirm_badge || "",
            r.confirmed_by || ""
          ]);
        }

        const csvContent = csvRows.map(row =>
          row.map(cell => {
            const s = String(cell);
            return s.includes(",") || s.includes('"') || s.includes("\n")
              ? '"' + s.replace(/"/g, '""') + '"' : s;
          }).join(",")
        ).join("\r\n");

        const BOM = "\uFEFF";
        return new Response(BOM + csvContent, {
          headers: {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": `attachment; filename="doc_ledger_${start_day}_${end_day}.csv"`,
            "Access-Control-Allow-Origin": "*"
          }
        });
      }

      // JSON 返回
      return jsonpOrJson({
        ok: true,
        docs: rows,
        total,
        page,
        page_size,
        has_more: page * page_size < total,
        badge_note: "V1: session_badge_count/session_badge_list 口径为 session 内 join 事件的 DISTINCT badge，非按 wave 时间窗精确切片"
      }, callback);
    }

    if (action === "b2b_scan_batch_close") {
      if (!isAdmin_(p, env) && !isView_(p, env)) return jsonpOrJson({ ok:false, error:"unauthorized" }, callback);
      const batch_id = String(p.batch_id || "").trim();
      if (!batch_id) return jsonpOrJson({ ok:false, error:"missing batch_id" }, callback);

      const batch = await env.DB.prepare(`SELECT * FROM b2b_scan_batches WHERE batch_id=?`).bind(batch_id).first();
      if (!batch) return jsonpOrJson({ ok:false, error:"batch_id not found" }, callback);
      if (batch.status !== "open") return jsonpOrJson({ ok:false, error:"batch is not open" }, callback);

      // 统计异常情况
      const itemsRs = await env.DB.prepare(`SELECT * FROM b2b_scan_items WHERE batch_id=?`).bind(batch_id).all();
      const items = itemsRs.results || [];
      let missingCount = 0, missingBoxes = 0, overCount = 0;
      for (const it of items) {
        if (it.scanned_count < it.expected_box_count) {
          missingCount++;
          missingBoxes += (it.expected_box_count - it.scanned_count);
        }
        if (it.scanned_count > it.expected_box_count) overCount++;
      }
      const unplannedRs = await env.DB.prepare(
        `SELECT COUNT(DISTINCT outbound_barcode) as cnt FROM b2b_scan_logs WHERE batch_id=? AND is_planned=0 AND undone=0`
      ).bind(batch_id).first();
      const unplannedCount = unplannedRs ? unplannedRs.cnt : 0;

      const confirm = String(p.confirm || "").trim();
      if (confirm !== "true") {
        // 返回统计，等前端二次确认
        return jsonpOrJson({
          ok:true, action:"confirm_needed",
          missing_count: missingCount, missing_boxes: missingBoxes,
          over_count: overCount, unplanned_count: unplannedCount
        }, callback);
      }

      // 确认关闭
      await env.DB.prepare(
        `UPDATE b2b_scan_batches SET status='closed', closed_at=? WHERE batch_id=?`
      ).bind(now, batch_id).run();

      return jsonpOrJson({ ok:true, batch_id, status:"closed" }, callback);
    }

    if (action === "b2b_scan_do") {
      const batch_id = String(p.batch_id || "").trim();
      const outbound_barcode = String(p.outbound_barcode || "").trim();
      const scanned_by = String(p.scanned_by || "").trim();
      const pallet_no = String(p.pallet_no || "").trim();

      if (!batch_id) return jsonpOrJson({ ok:false, error:"missing batch_id" }, callback);
      if (!outbound_barcode) return jsonpOrJson({ ok:false, error:"missing outbound_barcode" }, callback);
      if (!scanned_by) return jsonpOrJson({ ok:false, error:"missing scanned_by" }, callback);

      const batch = await env.DB.prepare(`SELECT status, total_expected_boxes FROM b2b_scan_batches WHERE batch_id=?`).bind(batch_id).first();
      if (!batch) return jsonpOrJson({ ok:false, error:"batch_id not found" }, callback);
      if (batch.status !== "open") return jsonpOrJson({ ok:false, error:"batch is not open, cannot scan" }, callback);

      // 查是否计划内
      const item = await env.DB.prepare(
        `SELECT item_id, expected_box_count, scanned_count FROM b2b_scan_items WHERE batch_id=? AND outbound_barcode=?`
      ).bind(batch_id, outbound_barcode).first();

      if (item) {
        // 计划内：原子写日志 + 更新计数
        await env.DB.batch([
          env.DB.prepare(
            `INSERT INTO b2b_scan_logs(batch_id,outbound_barcode,is_planned,scanned_by,scanned_at,undone,pallet_no) VALUES(?,?,1,?,?,0,?)`
          ).bind(batch_id, outbound_barcode, scanned_by, now, pallet_no),
          env.DB.prepare(
            `UPDATE b2b_scan_items SET scanned_count=scanned_count+1 WHERE batch_id=? AND outbound_barcode=?`
          ).bind(batch_id, outbound_barcode)
        ]);

        const newCount = item.scanned_count + 1;
        const diff = newCount - item.expected_box_count;

        // 算批次进度
        const progressRs = await env.DB.prepare(
          `SELECT SUM(MIN(scanned_count, expected_box_count)) as done FROM b2b_scan_items WHERE batch_id=?`
        ).bind(batch_id).first();
        const doneBoxes = progressRs ? (progressRs.done || 0) : 0;

        return jsonpOrJson({
          ok:true, planned:true,
          outbound_barcode, expected: item.expected_box_count,
          scanned_count: newCount, diff,
          done_boxes: doneBoxes,
          total_expected_boxes: batch.total_expected_boxes,
          progress_percent: batch.total_expected_boxes > 0 ? Math.round(doneBoxes * 100 / batch.total_expected_boxes) : 0
        }, callback);
      } else {
        // 计划外：只写日志
        await env.DB.prepare(
          `INSERT INTO b2b_scan_logs(batch_id,outbound_barcode,is_planned,scanned_by,scanned_at,undone,pallet_no) VALUES(?,?,0,?,?,0,?)`
        ).bind(batch_id, outbound_barcode, scanned_by, now, pallet_no).run();

        return jsonpOrJson({ ok:true, planned:false, outbound_barcode }, callback);
      }
    }

    if (action === "b2b_scan_undo") {
      const batch_id = String(p.batch_id || "").trim();
      if (!batch_id) return jsonpOrJson({ ok:false, error:"missing batch_id" }, callback);
      const operator_id = String(p.operator_id || "").trim();
      if (!operator_id) return jsonpOrJson({ ok:false, error:"missing operator_id" }, callback);

      const batch = await env.DB.prepare(`SELECT status, total_expected_boxes FROM b2b_scan_batches WHERE batch_id=?`).bind(batch_id).first();
      if (!batch) return jsonpOrJson({ ok:false, error:"batch_id not found" }, callback);
      if (batch.status !== "open") return jsonpOrJson({ ok:false, error:"batch is not open, cannot undo" }, callback);

      // 找当前操作员最近一条有效日志
      const lastLog = await env.DB.prepare(
        `SELECT * FROM b2b_scan_logs WHERE batch_id=? AND undone=0 AND scanned_by=? ORDER BY log_id DESC LIMIT 1`
      ).bind(batch_id, operator_id).first();
      if (!lastLog) return jsonpOrJson({ ok:false, error:"nothing to undo" }, callback);

      if (lastLog.is_planned === 1) {
        // 计划内：原子撤销日志 + 减计数
        await env.DB.batch([
          env.DB.prepare(
            `UPDATE b2b_scan_logs SET undone=1, undone_at=? WHERE log_id=?`
          ).bind(now, lastLog.log_id),
          env.DB.prepare(
            `UPDATE b2b_scan_items SET scanned_count=MAX(scanned_count-1,0) WHERE batch_id=? AND outbound_barcode=?`
          ).bind(batch_id, lastLog.outbound_barcode)
        ]);

        // 查更新后的计数
        const updated = await env.DB.prepare(
          `SELECT scanned_count FROM b2b_scan_items WHERE batch_id=? AND outbound_barcode=?`
        ).bind(batch_id, lastLog.outbound_barcode).first();

        // 算批次进度
        const progressRs = await env.DB.prepare(
          `SELECT SUM(MIN(scanned_count, expected_box_count)) as done FROM b2b_scan_items WHERE batch_id=?`
        ).bind(batch_id).first();
        const doneBoxes = progressRs ? (progressRs.done || 0) : 0;

        return jsonpOrJson({
          ok:true, undone_barcode: lastLog.outbound_barcode,
          was_planned:true, new_scanned_count: updated ? updated.scanned_count : 0,
          done_boxes: doneBoxes,
          total_expected_boxes: batch.total_expected_boxes,
          progress_percent: batch.total_expected_boxes > 0 ? Math.round(doneBoxes * 100 / batch.total_expected_boxes) : 0
        }, callback);
      } else {
        // 计划外：只标记撤销
        await env.DB.prepare(
          `UPDATE b2b_scan_logs SET undone=1, undone_at=? WHERE log_id=?`
        ).bind(now, lastLog.log_id).run();

        return jsonpOrJson({
          ok:true, undone_barcode: lastLog.outbound_barcode, was_planned:false
        }, callback);
      }
    }

    return jsonpOrJson({ ok:false, error:"unknown action: " + action }, callback);
  }
};
