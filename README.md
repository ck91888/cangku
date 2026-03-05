# CK 仓库作业系统 / CK 창고 작업 시스템

仁川 CK 仓库的工时追踪与作业管理系统。工人扫工牌 join/leave，系统记录工时、锁定工牌防重复，管理员可查看在岗与报表。

## 部署

| 组件 | 地址 |
|------|------|
| 前端 (GitHub Pages) | https://ck91888.github.io/cangku |
| 后端 (Cloudflare Worker) | https://ck-warehouse-api.ck91888.workers.dev |
| 数据库 | Cloudflare D1 (`ck_warehouse`) |
| 锁 (并发控制) | Cloudflare Durable Object (`LocksDO`) |
| 代码仓库 | https://github.com/ck91888/cangku |

## 文件结构

```
index.html      前端页面（所有页面SPA，hash路由）
app.js          前端逻辑（扫码、状态管理、网络请求）
style.css       样式
worker/
  index.js      Cloudflare Worker 后端（API + Durable Object）
  wrangler.toml 部署配置
```

## 业务模块

### B2C
| 环节 | 流程 |
|------|------|
| 入库理货 | start → join/leave → 扫入库单号(去重计数) → end |
| 拣货 | start → join/leave → 扫波次号 → end |
| 换单 | start → join/leave → end |
| 验货贴单打包 | 扫工牌join → 扫工牌leave（自动session） |
| 批量出库 | start → join/leave → 扫出库单号(去重计数) → end |
| 退件入库 | 扫工牌join → 扫工牌leave（自动session） |
| 质检 | 扫工牌join → 扫工牌leave（自动session） |
| 废弃处理 | 扫工牌join → 扫工牌leave（自动session） |
| 盘点 | 扫工牌join → 扫工牌leave（自动session） |

### 进口快件 / 수입 택배
| 环节 | 流程 |
|------|------|
| 卸货 | 扫工牌join → 扫工牌leave（自动session） |
| 过机扫描码托 | 扫工牌join → 扫工牌leave（自动session） |
| 装柜/出货 | 扫工牌join → 扫工牌leave（自动session） |
| 取/送货 | 扫工牌join + **输入备注** → 扫工牌leave（自动session） |
| 问题处理 | 扫工牌join + **输入备注** → 扫工牌leave（自动session） |

### B2B
| 环节 | 流程 |
|------|------|
| 卸货 | 扫工牌join → 扫工牌leave（自动session） |
| 入库理货 | start → join/leave → 扫理货单号(去重) → end |
| 工单操作 | start → join/leave → 扫工单号(去重) → end |
| 出库 | 扫工牌join → 扫工牌leave（自动session） |
| 盘点 | 扫工牌join → 扫工牌leave（自动session） |

### 仓库整理
扫工牌join → 扫工牌leave（自动session）

## 工牌规则

| 类型 | 格式 | 说明 |
|------|------|------|
| 员工 | `EMP-姓名` | 长期使用，支持中韩文 |
| 长期日当 | `DAF-姓名` | 一人一张，长期使用 |
| 每日日当 | `DA-YYYYMMDD-姓名` | 每天生成，带日期 |

工牌二维码内容格式：`类型-标识\|姓名`（竖线分隔ID和名字）

## 核心机制

### 工牌锁 (Badge Lock)
- 同一工牌同时只能在一个任务中（Durable Object 全局锁）
- join 时 acquire lock，leave 时 release lock
- 锁有8小时TTL自动过期，防止忘记leave导致永久锁定
- 管理员可强制下线释放锁

### Session 管理
- 每个任务独立 session（`PS-YYYYMMDD-HHMMSS-操作员`）
- 自动session任务：第一次join时自动创建session
- 手动session任务（理货/拣货/换单/批量出库等）：需先点"开始"
- session关闭时检查是否还有人在岗，有人则阻止关闭
- 跨设备支持：扫session二维码可在另一台设备加入同一趟次

### 事件系统
- 关键操作（join/leave）同步提交，等待服务器确认
- 非关键操作（start/end/wave）异步队列，失败重试最多8次
- 所有事件写入 D1 `events` 表，含服务端时间戳
- 事件去重：`INSERT OR IGNORE` + 前端 `event_id` 去重

### 本地状态
- localStorage 持久化在岗列表、已扫单号等
- 进入任务页时异步从服务器同步在岗列表（双向同步：加缺的、删多的）
- 启动时自动清理超过7天的旧session数据

## 管理员功能

标题连续点击7次 → 输入口令 → 解锁：
- **劳效/工时汇总**：按日期区间拉取事件数据，生成人员工时报表，支持CSV导出
- **全局Session管理**：查看所有OPEN/CLOSED session，可强制结束
- **全局在岗**：查看所有任务的在岗人员，可强制下线

## 后端 API

所有请求通过 JSONP（GET `?action=xxx&callback=cb`）。

| action | 说明 |
|--------|------|
| `ping` | 健康检查 |
| `event_submit` | 提交事件（join/leave/start/end/wave等） |
| `lock_acquire` / `lock_release` | 工牌锁操作（通过 Durable Object） |
| `lock_status` / `locks_by_session` | 查询锁状态 |
| `active_now` | 全局在岗查询 |
| `session_info` / `session_close` | Session 查询/关闭 |
| `operator_open_sessions` | 查询操作员的未关闭session |
| `admin_events_tail` | 管理员拉取事件（需口令） |
| `admin_force_leave` | 管理员强制下线 |
| `admin_sessions_list` | 管理员查看所有session |
| `admin_force_end_session` | 管理员强制结束session |

## 数据库表

### events
```
server_ms, client_ms, event_id(PK), event, badge, biz, task, session, wave_id, operator_id, ok, note
```

### sessions
```
session(PK), status(OPEN/CLOSED), created_ms, created_by_operator, closed_ms, closed_by_operator, biz, task
```

### task_state
```
(session, biz, task)(PK), status(OPEN/CLOSED), started_ms, ended_ms, started_by_operator, ended_by_operator
```

## 开发部署

```bash
# 前端：推送到 main 分支自动部署 GitHub Pages
git push origin main

# 后端：手动部署 Cloudflare Worker
cd worker && npx wrangler deploy
```
