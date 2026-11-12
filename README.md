# Scan Time Normalizer — 航空货运扫描时间归一化 & 批次订阅消费 API

将承运商导出的**本地扫描时间**按 IANA 时区规则归一化为 **UTC**，并把"全部 UTC 确定"的交付
持久化为编号批次，供多个入仓任务以**租约 + 确认**的方式各自接续消费；任务崩溃、响应丢失或
服务重启都不会跳批。

## 功能一览

### 1. `POST /normalize`（保留原有能力）

- 每条扫描记录的本地时间对应 **0、1 或 2 个合法 UTC 候选**（夏令时回拨重复 / 拨快不存在）；
- 航段/转运衔接约束两条记录间的 UTC 耗时落在闭区间 `[min_seconds, max_seconds]`，
  **全部衔接必须同时满足**（2-SAT 精确求解）；
- 仅剩一个候选 → `determined`（确定）给出 `utc`；多个 → `needs_review`（待核对）返回全部候选；
- 无合法候选或衔接整体无解 → **HTTP 409**，不返回部分转换。

### 2. `/subscriptions`（新增：订阅消费）

- **提交** `POST /subscriptions/deliveries`：提交交付号 + 现有归一化请求。先跑归一化，
  **仅当所有记录均为 `determined`（全部 UTC 确定）才保存完整请求与结果**，并按提交顺序
  分配**单调递增批次号**（1, 2, 3 …）。同交付号同内容（语义相等，与 JSON 键序无关）返回
  原批次；同交付号异内容 → **409 拒绝**且不写入。
- **注册订阅者** `POST /subscriptions/subscribers`：每个入仓任务一个订阅者，从首批开始消费。
- **领取** `POST /subscriptions/claim`：领取该订阅者**最早未确认的整批**，获得全局唯一
  `lease_id` 与 **30 秒租期**；租期内再次领取同批 → **409**。租约到期后可重领同批，
  系统换发**新 lease_id**，旧号被标记过期、**永远不能推进进度**（确认旧号返回 410）。
- **确认** `POST /subscriptions/confirm`：凭**当前有效**的 lease_id 确认，进度在同一事务中
  推进到下一批；重复确认返回**同一张回执**（幂等）。
- **进度查询** `GET /subscriptions/subscribers/{id}`：查看下一批序号、已确认批次数与活跃租约。
- 不同订阅者各自维护独立进度、**互不阻塞**；批次、进度、租约、回执全部落在 **SQLite**
  （WAL + `synchronous=FULL`，写操作统一 `BEGIN IMMEDIATE` 串行化），并发领取/确认、
  响应丢失或服务重启均**不跳批、不重批**。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

服务启动后：

- 交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>
  —— 可直接演示提交、领取、过期重领、确认全流程；
- 健康检查：`GET http://localhost:8000/healthz`；
- SQLite 数据文件位于命名卷 `scanwarehouse_data` 内的 `/data/scanwarehouse.db`，
  容器重启后批次/进度/租约/回执均保留。

环境变量（`docker-compose.yml` 已给出默认值）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SCAN_DB_PATH` | `/data/scanwarehouse.db` | SQLite 数据库文件路径 |
| `SCAN_LEASE_SECONDS` | `30` | 领取租约时长（秒） |

## 接口说明（订阅消费）

### `POST /subscriptions/deliveries`

请求体：`{"delivery_id": "...", "request": { ...与 /normalize 完全相同的请求体... }}`

- **201**：`batch_seq`（递增编号）、完整 `request` 与 `result`、`replayed`（是否为重复提交）；
- **409 `not_all_utc_determined`**：存在 `needs_review` 记录，**不保存、不占编号**，
  响应附带本次归一化结果与 `pending_records`；
- **409 `no_legal_candidate` / `no_global_solution`**：归一化本身失败；
- **409 `delivery_content_conflict`**：同交付号曾以不同内容提交。

### `POST /subscriptions/subscribers`

请求体 `{}`（自动生成 id）或 `{"subscriber_id": "wh-task-7"}`（自定义，重复注册幂等）。

### `POST /subscriptions/claim`

请求体 `{"subscriber_id": "..."}`：

- **200**：`lease_id`、`batch_seq`、`delivery_id`、完整批次内容、`leased_at` /
  `expires_at`（UTC）、`lease_seconds`；
- **404 `subscriber_not_found` / `no_batch_available`**；
- **409 `lease_active`**：该批仍被本订阅者的有效租约持有。

### `POST /subscriptions/confirm`

请求体 `{"subscriber_id": "...", "lease_id": "..."}`：

- **200**：回执（`receipt_id`、`lease_id`、`batch_seq`、`delivery_id`、`confirmed_at`），
  重复确认返回完全相同的回执；
- **404 `lease_not_found`**、**403 `lease_wrong_subscriber`**、
  **410 `lease_expired`**（旧号不能推进进度，需重新领取换新号）。

## curl 全流程演示

```bash
# 1) 提交两个全部 UTC 确定的交付，得到批次 1、2
curl -s -X POST http://localhost:8000/subscriptions/deliveries \
  -H 'Content-Type: application/json' \
  -d '{"delivery_id":"AWB-0001","request":{"records":[
        {"id":"DEP_PVG","local_time":"2024-11-03T09:00:00","timezone":"Asia/Shanghai"},
        {"id":"ARR_ANC","local_time":"2024-11-03T01:30:00","timezone":"America/Anchorage"},
        {"id":"DEP_ANC","local_time":"2024-11-03T03:10:00","timezone":"America/Anchorage"}],
      "links":[
        {"from_id":"DEP_PVG","to_id":"ARR_ANC","min_seconds":32400,"max_seconds":39600},
        {"from_id":"ARR_ANC","to_id":"DEP_ANC","min_seconds":3600,"max_seconds":7200}]}}'

# 2) 注册一个入仓任务（订阅者）
SID=$(curl -s -X POST http://localhost:8000/subscriptions/subscribers \
  -H 'Content-Type: application/json' -d '{}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["subscriber_id"])')

# 3) 领取最早未确认整批（首批），拿到 lease_id，30 秒租期
LID=$(curl -s -X POST http://localhost:8000/subscriptions/claim \
  -H 'Content-Type: application/json' -d "{\"subscriber_id\":\"$SID\"}" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_id"])')

# 租期内重复领取 -> 409 lease_active
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/subscriptions/claim \
  -H 'Content-Type: application/json' -d "{\"subscriber_id\":\"$SID\"}"

# 4) 确认有效租约 -> 进度推进；再确认一次 -> 同一张回执
curl -s -X POST http://localhost:8000/subscriptions/confirm \
  -H 'Content-Type: application/json' -d "{\"subscriber_id\":\"$SID\",\"lease_id\":\"$LID\"}"

# 5) 租约过期后（演示时可把 SCAN_LEASE_SECONDS 调小）重领 -> 新 lease_id；
#    用旧 lease_id 确认 -> 410 lease_expired，进度不变、不跳批
```

## 可靠性设计要点

1. **单事务串行化**：提交、领取、确认都在一个 `BEGIN IMMEDIATE` 事务内完成，SQLite 写锁
   让并发请求天然排队；批次号由 `coalesce(max(batch_seq),0)+1` 在事务内分配，无重复编号。
2. **不跳批**：订阅者进度 `next_seq` 只在"租约有效 + 确认成功"的同一事务里 +1；
   过期/被取代的旧 lease_id 一律 410，无法推进进度。
3. **崩溃可恢复**：WAL + 全同步刷盘，提交即落盘；重启后从 SQLite 恢复进度与回执，
   未确认的租约到期后即可重领（换发新号）。
4. **幂等**：交付提交（交付号 + 内容哈希）、订阅者注册、确认回执均可安全重试；
   内容哈希基于规范化 JSON（键排序、紧凑分隔），与报文键序和空白无关。
5. **订阅者隔离**：每个订阅者独立一行进度与独立租约集合，部分唯一索引
   `uq_active_lease` 保证同一订阅者同一批次至多一条活跃租约，不同订阅者互不阻塞。

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest httpx
uvicorn app.main:app --reload          # 本地起服务（默认 /data/...，可用 SCAN_DB_PATH 覆盖）
python -m pytest tests/ -q             # 39 个用例
```

测试覆盖：原 19 个归一化用例 + 20 个订阅用例（编号/幂等/内容冲突、全部 UTC 闸门、
租约互斥、过期重领换号、旧号 410、回执幂等、不跳批、订阅者独立、**重启持久化**、
多线程并发领取/确认/提交）。

## 目录结构

```
app/
  main.py           # FastAPI 入口、/normalize、生命周期建表、异常映射、Swagger 示例
  schemas.py        # 归一化请求/响应模型与输入校验
  solver.py         # UTC 候选生成 + 2-SAT 全局可行性 + 不可约冲突子集
  subs_schemas.py   # 订阅 API 的请求/响应模型
  subscriptions.py  # 提交/注册/领取/确认/进度路由 + 全部 UTC 确定闸门
  storage.py        # SQLite 表结构、IMMEDIATE 事务、租约/进度/回执状态机
  errors.py         # 存储层领域错误 -> HTTP 响应
  settings.py       # SCAN_DB_PATH / SCAN_LEASE_SECONDS 环境变量
tests/
  test_api.py           # 归一化 19 个用例（保持不变）
  test_subscriptions.py # 订阅消费 20 个用例（含并发与重启）
Dockerfile / docker-compose.yml / requirements.txt
```
