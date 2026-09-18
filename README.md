# 返还文物交接账

本项目服务跨境返还文物的接收、保管与入藏协作，建立每件物品从**查获 → 交接 →
入境 → 入藏**的完整责任链。领域对象分为返还案件、接收批次、器物（套/单件）、
器物组成、保管地点、交接事件、文件哈希档案与待处置队列。

核心约束：

- “套”按套与组件计数，可包含单件；**每个单件只能处于一条有效保管链上**
  （合套后单件保留独立身份，但 `member_of` 唯一归属到套，不重复计数）。
- 交接状态采用 `拟定`、`待双方签署`、`已生效`、`已撤回`、`已更正`。
  移交方、接收方**各自按所在时区**签署（保留原始本地时间 `at_local`+`tz`，
  同时记录 UTC），**双方签署齐全**后交接才生效，状态才能向下推进。
- 已生效/已确认记录**不得原地覆盖**：数量拆分、成套合并、身份更正、交接更正
  都生成新对象或新事件并引用原对象，旧记录保留 `已拆分/已并入/已更正` 状态，
  前后关系可双向追溯。
- 照片、清单、鉴定文件**只保存内容哈希（SHA-256）与版本关系**，不保存文件本体；
  同哈希不能重复作为新版本。
- 重复回调凭 `Idempotency-Key` 返回首次结果（同键不同内容报 409），
  **不会制造第二次交接**。
- 超期未签、实物数量不符、保管链阶段跳跃/缺少生效交接凭证，自动进入
  **待处置队列**。

## 架构

事件溯源（event sourcing）+ 仅追加账本：

| 文件 | 职责 |
| --- | --- |
| `events.py` | 事件信封、规范化哈希、时区解析、状态/阶段/原因常量、领域异常 |
| `store.py` | append-only `journal.jsonl`（哈希链）、幂等索引 `idem.json`、快照 `snapshot.json`、启动重放与完整性校验 |
| `app.py` | 领域服务：命令校验、状态机、拆分/合并/更正、待处置队列、谱系与溯源、审计报告 |
| `web.py` | `/api/*` JSON HTTP 路由（写接口支持 `Idempotency-Key`） |
| `service.py` | 运行入口：`--check/--seed/--sweep/--port`，并保留 `/health` 契约 |
| `seed.py` | 两批返还样例（华盛顿、纽约） |
| `service_contract.py` / `test_custody.py` | 契约测试与领域/并发/审计测试 |

每条事件含 `seq` 与 `prev_hash`，自身哈希覆盖全部业务字段（含幂等指纹）。
任意一行被改写、删除或乱序，`--check` 或 `GET /api/audit` 都会报
`JournalIntegrityError`。

## 运行

```bash
python3 service.py --check                 # 基础配置 + 账本哈希链校验
python3 service.py --seed                  # 写入两批返还样例（幂等，已存在则跳过）
python3 service.py --sweep                 # 扫描超期未签交接，入待处置队列
python3 service.py --port 8000             # 启动 HTTP 服务（默认数据目录 ./data）
python3 service.py --data-dir ./data --port 8000
```

健康检查：`GET /health` →
`{"status":"ok","service":"artifact-custody","name":"返还文物交接账"}`

## 主要接口

写接口均可带请求头 `Idempotency-Key: <键>` 实现回调幂等。

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/cases` `/api/locations` `/api/batches` | 建案件、保管地点、接收批次 |
| `POST /api/artifacts` | 登记器物（`kind`=套/单件；`quantity` 或 `components`） |
| `GET  /api/artifacts/{id}/trace` | 从任一器物（含历史标识）反查完整来源、责任链、文件与事件时间线 |
| `GET  /api/artifacts/{id}/lineage` | 拆分/合并/更正/成套的前后谱系 |
| `POST /api/artifacts/{id}/split` | 拆套（数量守恒；原单件用 `reuse_singleton` 安置） |
| `POST /api/artifacts/merge` | 成套合并（同批次、同阶段） |
| `POST /api/artifacts/{id}/identity` | 身份更正（旧标识保留并指向新标识） |
| `POST /api/chain` | 登记责任链节点（查获/交接/入境/入藏；跳跃或缺凭证入队） |
| `POST /api/handovers` | 建立交接（拟定或直接提交，可 `corrects` 引用原交接） |
| `POST /api/handovers/{id}/submit` `/sign` `/withdraw` `/resubmit` `/correct` | 提交、签署、撤回、重提、更正 |
| `POST /api/documents` `POST /api/documents/{id}/versions` | 登记文件哈希、追加版本 |
| `GET  /api/queue` `POST /api/queue/{id}/resolve` `POST /api/sweep-overdue` | 待处置队列 |
| `GET  /api/events` | 只读事件流（含哈希链） |
| `GET  /api/audit` | 可核验审计档案：哈希链校验 + 独立重放投影 + 快照指纹对照 |

签署示例（双方各按所在时区）：

```json
POST /api/handovers/ho_.../sign
{ "party": "from", "actor": "ICE授权官员",
  "at_local": "2026-09-10T16:30", "tz": "America/New_York",
  "observed_quantity": 11 }
```
```json
{ "party": "to", "actor": "国家文物局接收人",
  "at_local": "2026-09-11T10:15", "tz": "Asia/Shanghai",
  "observed_quantity": 11 }
```

## 样例数据

- **华盛顿批次**（ICE 华盛顿办公室）：石刻造像一套 4 件、汉代彩绘陶俑一套 6 件
  按套计数，霸王龙骨架化石单件追踪；移交方 9/10 华盛顿时间、接收方 9/11 北京
  时间分两日签署后生效；鉴定书保留两个版本哈希。
- **纽约批次**（纽约县地区检察官办公室）：狼鳍鱼化石、恐龙蛋化石 2 枚全部单件
  追踪；交接待签署且已过截止时间，`--sweep` 后进入待处置队列。

## 测试

```bash
npm test                 # = python3 -m unittest -v service_contract test_custody
```

覆盖：并发双方签署恰好生效一次、同方并发回调只记一笔、重复回调幂等、撤回重提、
跨日双时区、拆分数量守恒/合并/身份更正的双向溯源、超期/数量不符/断链入队、
哈希链防篡改（改字段/删行）、重启重放与编号续号、文件哈希版本去重，以及
完整 HTTP 流程。
