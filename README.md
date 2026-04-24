# CK 仓库作业系统 / CK 창고 작업 시스템

仁川 CK 海外仓的工时追踪与作业管理系统。工人扫工牌 join/leave，系统实时记录工时、锁定工牌防重复，管理员可查看全局在岗、劳效报表与补录修正。支持 20 个作业环节、多设备协作、临时卸货切换、QR/条形码双模扫码。

## 部署架构

| 组件 | 地址 | 说明 |
|------|------|------|
| 前端 (GitHub Pages) | https://ck91888.github.io/cangku | SPA，hash 路由，纯静态 |
| 管理看板 | https://ck91888.github.io/cangku/leader/ | 只读看板：全局在岗 + 工时汇总 |
| 后端 API (Cloudflare Worker) | https://apiold.ck91888.cn | 旧域统一入口 |
| 数据库 | Cloudflare D1 (`ck_warehouse`) | SQLite，存事件/Session/任务状态 |
| 分布式锁 | Cloudflare Durable Object (`LocksDO`) | 全局单例，工牌并发锁 |
| 代码仓库 | https://github.com/ck91888/cangku | 前后端同仓 |

## 文件结构

```
index.html          前端主页面（所有任务页 SPA，hash 路由，1800+ 行）
app.js              前端全部逻辑（扫码、状态管理、网络请求，4200+ 行）
style.css           全局样式
leader/
  index.html        管理看板页面
  leader.js         看板逻辑（全局在岗 + 劳效/工时汇总报表）
worker/
  index.js          Cloudflare Worker 后端（API + Durable Object，850+ 行）
  wrangler.toml     部署配置（D1 绑定、DO 绑定）
```

## 业务模块（20 个作业环节）

### B2C 出口电商
| 环节 | 类型 | 流程 |
|------|------|------|
| 入库理货 | 手动 session | start → join/leave → 扫入库单号(去重计数) → end |
| 拣货 | 手动 session | start → 组长登录 → join/leave → 扫波次号 → end |
| 换单 | 手动 session | start → join/leave → end |
| 批量出库 | 手动 session | start → join/leave → 扫出库单号(去重计数) → end |
| 验货贴单打包 | 自动 session | 扫工牌 join/leave（最后一人 leave 自动结束） |
| 退件入库 | 自动 session | 扫工牌 join/leave |
| 质检 | 自动 session | 扫工牌 join/leave |
| 废弃处理 | 自动 session | 扫工牌 join/leave |
| B2C 盘点 | 自动 session | 扫工牌 join/leave |

### 进口快件 / 수입 택배
| 环节 | 类型 | 流程 |
|------|------|------|
| 卸货 | 自动 session | 扫工牌 join/leave（支持临时卸货切换） |
| 过机扫描码托 | 自动 session | 扫工牌 join/leave |
| 装柜/出货 | 自动 session | 扫工牌 join/leave |
| 取/送货 | 自动 session | 扫工牌 join + **输入备注（去哪/做什么）** → leave |
| 问题处理 | 自动 session | 扫工牌 join + **输入备注** → leave |

### B2B 大宗
| 环节 | 类型 | 流程 |
|------|------|------|
| B2B 卸货 | 自动 session | 扫工牌 join/leave（支持临时卸货切换） |
| B2B 入库理货 | 手动 session | start → join/leave → 扫理货单号(去重) → end |
| B2B 工单操作 | 手动 session | start → join/leave → 扫工单号(去重) → end |
| B2B 出库 | 自动 session | 扫工牌 join/leave |
| B2B 盘点 | 自动 session | 扫工牌 join/leave |

### 通用
| 环节 | 类型 | 流程 |
|------|------|------|
| 仓库整理 | 自动 session | 扫工牌 join/leave |

> **手动 session**：需先点"开始"创建趟次，再扫码加入。
> **自动 session**：第一次 join 时自动创建趟次，最后一人 leave 自动结束。

## 工牌规则

| 类型 | QR 内容格式 | 说明 |
|------|-------------|------|
| 员工 | `EMP-姓名` | 长期使用，支持中韩文 |
| 长期日当 | `DAF-姓名` | 固定工牌，长期使用 |
| 每日日当 | `DA-YYYYMMDD-姓名` | 每天生成，带日期标识 |

系统内可批量生成工牌二维码（员工/长期日当/每日日当），截图打印即用。

## 核心机制

### 工牌锁 (Badge Lock)
- Durable Object 全局单例管理所有锁，同一工牌同时只能在一个任务中
- join → `lock_acquire`（带 biz/task/session 条件），leave → `lock_release`（带条件校验，防误杀）
- 锁有 **8 小时 TTL** 自动过期，防止忘记 leave 导致永久锁定
- 重复 event_id 的 join 请求自动做带条件释放（不会误杀其他 session 的锁）
- 管理员可强制下线释放锁

### Session 管理
- 每个任务独立 session，格式 `PS-YYYYMMDD-HHMMSS-操作员ID`
- 手动 session 任务（理货/拣货/换单/批量出库/B2B入库理货/B2B工单操作）需先 start
- session 关闭前检查是否还有人在岗，有人则阻止关闭（最多重试 3 次等锁释放）
- 跨设备：扫 session 二维码（`CKSESSION|PS-...|biz|task`）可在另一台设备加入同一趟次
- 重新开始时自动关闭旧 session，避免孤儿 session 累积

### 临时卸货切换
- 任何任务中的工人可临时切到卸货，完成后一键返回原任务
- 系统自动保存/恢复：源 session、在岗人员、已扫单号
- 支持部分人员返回（多选/全选）

### 事件系统
- **同步事件**（join/leave）：等待服务器确认后才更新 UI，失败可重试 2 次
- **异步事件**（start/end/wave/bind）：进入队列，5 秒间隔刷新，失败重试最多 8 次，超限通知用户
- 所有事件写入 D1 `events` 表，含服务端毫秒时间戳（KST = UTC+9）
- 事件去重：`INSERT OR IGNORE`（服务端）+ `event_id` 本地缓存（前端）

### 扫码引擎
- 基于 Html5Qrcode，同时支持 **QR 码 + 7 种条形码**（CODE_128/CODE_39/EAN_13/EAN_8/UPC_A/ITF/CODABAR）
- 智能镜头选择：优先广角主摄，排除长焦/微距镜头（解决 OPPO 等多摄手机问题）
- 启动后强制 zoom=1 + 连续自动对焦，适合近距离扫码
- 内置乱码检测：特殊字符占比 >30% 自动拦截，提示用户重扫或手动输入
- 自适应扫描区域：宽 85% × 高 30%，适配条形码横向全入

### 本地状态
- localStorage 持久化在岗列表、已扫单号、session 映射等
- 进入任务页时异步从服务器同步在岗列表（双向：加服务器有的、删本地多的）
- 启动时自动清理超过 7 天的旧 session 数据

## 管理员功能

### 前端管理入口
标题连续点击 7 次 → 输入口令 → 解锁：
- **劳效/工时汇总**：按日期区间拉取事件，生成人均工时 + 劳效排名，支持 CSV 导出
- **全局 Session 管理**：查看所有 OPEN/CLOSED session，可强制结束
- **全局在岗**：所有任务的在岗人员一览，可强制下线
- **事件补录修正**：管理员可手动插入/修改/删除 join/leave 事件（自定义时间戳），修正漏刷

### 管理看板 (`/leader/`)
独立页面，输入只读口令后可查看：
- **全局在岗**（无需口令）：实时在岗人员 + 人数统计
- **劳效/工时汇总**：客户端计算 join→leave 工时，处理异常（重复 join/无 join 的 leave/未关闭等）

## 后端 API

请求方式：前端用 **JSONP**（GET `?action=xxx&callback=cb`），管理操作用 **POST JSON**（fetchApi）。

### 公开接口
| action | 说明 |
|--------|------|
| `ping` | 健康检查 |
| `event_submit` | 提交事件（join/leave/start/end/wave 等），join/leave 含锁操作 |
| `session_info` | 查询 session 状态 + 在岗人员列表 |
| `session_close` | 关闭 session（需所有人先 leave） |
| `operator_open_sessions` | 查询操作员的未关闭 session |
| `active_now` | 全局在岗查询（所有任务） |

### 管理员接口（需 `k=ADMINKEY`）
| action | 说明 |
|--------|------|
| `admin_events_tail` | 按日期区间拉取事件数据 |
| `admin_force_leave` | 强制下线（释放锁 + 写 leave 事件） |
| `admin_sessions_list` | 查看所有 session 列表 |
| `admin_force_end_session` | 强制结束 session |
| `admin_event_insert` | 补录 join/leave 事件（指定自定义时间戳） |
| `admin_event_update` | 修改事件字段（时间/单号/备注/工牌/业务/任务） |
| `admin_event_delete` | 删除错误事件 |
| `admin_session_events` | 查询指定 session 的全部事件 |

### 只读接口（需 `k=VIEWKEY`）
| action | 说明 |
|--------|------|
| `admin_events_tail` | 同上（看板用） |

## 数据库表

### events
```sql
server_ms      INTEGER    -- 服务端时间戳 (ms)
client_ms      INTEGER    -- 客户端时间戳 (ms)
event_id       TEXT PK    -- 唯一事件ID（设备|session|biz|task|event|badge|时间戳|随机）
event          TEXT       -- join / leave / start / end / wave / bind_daily / join_fail
badge          TEXT       -- 工牌 (DA-xxx / DAF-xxx / EMP-xxx)
biz            TEXT       -- 业务线 (B2C / B2B / 进口 / DAILY)
task           TEXT       -- 任务名 (理货 / 拣货 / 换单 / ...)
session        TEXT       -- 趟次ID (PS-...)
wave_id        TEXT       -- 波次/单号
operator_id    TEXT       -- 操作设备ID
ok             INTEGER    -- 1=成功 0=失败
note           TEXT       -- 备注
```

### sessions
```sql
session                TEXT PK     -- 趟次ID
status                 TEXT        -- OPEN / CLOSED
created_ms             INTEGER
created_by_operator    TEXT
closed_ms              INTEGER
closed_by_operator     TEXT
biz                    TEXT        -- 补录时自动更新
task                   TEXT        -- 补录时自动更新
```

### task_state
```sql
(session, biz, task)   PK          -- 联合主键
status                 TEXT        -- OPEN / CLOSED
started_ms             INTEGER
ended_ms               INTEGER
started_by_operator    TEXT
ended_by_operator      TEXT
```

## 并发豁免

以下任务允许与其他 session 同时存在（支持"临时去卸货"场景）：
- B2C 拣货
- B2B 卸货
- 进口卸货

## 开发部署

```bash
# 前端：推送到 main 分支自动部署 GitHub Pages
git push origin main

# 后端：部署 Cloudflare Worker（含 D1 + Durable Object）
cd worker && npx wrangler deploy

# 环境变量（在 Cloudflare Dashboard 设置）
# ADMINKEY  — 管理员口令
# VIEWKEY   — 只读看板口令
```
