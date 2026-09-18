# 返还文物交接账

服务跨境返还文物从**查获 → 交接 → 入境 → 入藏**的全链责任记录。
核心目标：从任一器物（按套的造像/陶俑，或单件追踪的恐龙骨架、蛋化石）
都能反查完整来源；任何修订都不抹去历史；异常状态自动进入待处置队列。

## 设计要点

- **事件溯源 + 仅追加**：所有状态变化都是事件（`events` 表只 INSERT 不 UPDATE/DELETE）。
  每个事件信封含 `prev_hash`，对 `schema/event_id/stream_id/seq/stream_seq/
  event_type/payload/prev_hash/recorded_at` 的规范化 JSON 求 SHA-256，形成全局哈希链；
  每个实体（案件/批次/器物/交接/文件/事项）有独立流，带连续 `stream_seq`。
- **可核验审计档案**：每个事件同步追加一行 `audit.jsonl`（fsync 落盘）。
  `verify_audit.py` 仅凭 JSONL 即可重算哈希链、校验流序号并用同一套投影规则独立重放，
  可与 SQLite 库交叉比对链头。改动任意一个历史字节，链即断裂，服务拒绝启动。
- **双方时区签署**：交接在双方各自时区签署，签名同时保存本地时间（含 IANA 时区与偏移）
  和 UTC；同一时刻的两个签名 `utc` 相等。所有必需签署方都签署后状态才推进为“已生效”，
  保管责任与实物位置随之推进。
- **不原地覆盖**：
  - 交接更正 = 新生效事件 `corrects_handover_id` 引用原事件，原事件状态置“已更正”但内容不动；
  - 数量拆分 / 成套合并 / 身份更正 = 修订记录保存 before/after、inputs/outputs 与前后器物的
    `parents/children` 谱系；
  - 文件（照片/清单/鉴定文件）只存 **SHA-256 哈希、大小与版本关系**，新版本通过
    `supersedes_version` 指向旧版本，旧哈希永久保留。
- **幂等与并发**：每个写命令接受 `client_key`（境外系统回调号）。数据库对 `client_key`
  建唯一索引，对 `(handover_id, party_key)` 建签署唯一约束；所有命令在单锁 +
  `BEGIN IMMEDIATE` 事务内“读投影—校验—落事件”。重复回调返回原事件（`replayed:true`），
  绝不产生第二次交接；并发同方签署只有一方成功，双方并发签署恰好在第二次完成生效。
- **待处置队列**（自动扫描、按 类型+对象+明细 去重，不重复立项）：
  - 超期未签：超过 `sign_deadline` 仍停留在“待双方签署”；
  - 实物数量不符：生效后清点 `actual_qty` 与交接清单 `expected_qty` 不一致；
  - 保管链断点：必备环节缺失，或相邻生效交接的交出/接收责任主体不衔接
    （入境含“海关接收 / 查验放行”两段，按生效时间逐跳校验）。

状态约定：`拟定` → `待双方签署` → `已生效`；可 `已撤回`（未签署前，撤回后可重提）；
已生效记录被更正后为 `已更正`。事项状态：`待处置 → 处置中 → 已关闭`。

## 数据模型

案件 Case ──< 批次 Batch（境外执法机构、交接城市/日期、时区）
&nbsp;&nbsp;&nbsp;&nbsp;└──< 器物 Artifact（`按套`单位“套” / `单件`单位“件”）
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├──< 单件 Piece（蛋化石逐枚、骨架编号，单件独立保管链）
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── 修订 Revision（拆分 / 合并 / 身份更正，前后关联）
交接 Handover（查获/交接/入境/入藏，双方、时区签名、清单数量、实收数量、地点）
文件 FileRecord（照片/清单/鉴定文件，仅哈希 + 版本链）
地点 Location（查验场地/口岸/库房/展厅）
事项 Issue（超期未签 / 实物数量不符 / 保管链断点）

## 运行

```bash
python3 service.py --check            # 基础检查 + 重算审计链
python3 service.py --port 8000        # 默认库 data/ledger.db
python3 service.py --port 8000 --db data/sample.db
python3 seed_sample.py                # 建立两批返还样例
python3 verify_audit.py data/sample.audit.jsonl --db data/sample.db
npm test                              # 基线契约 + 应用测试
python3 -m unittest test_app -v       # 35 项领域/并发/审计测试
```

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 服务身份 |
| POST/GET | `/cases` `/cases/{id}` | 返还案件 |
| POST/GET | `/locations` | 保管地点 |
| POST/GET | `/batches?case_id=` `/batches/{id}` | 接收批次（含境外机构与时区） |
| POST/GET | `/artifacts?batch_id=` `/artifacts/{id}` | 器物登记（kind=按套/单件，单件随附 pieces） |
| POST | `/artifacts/{id}/pieces` | 单件追踪器物追加单件 |
| GET | `/artifacts/{id}/provenance` | **完整来源反查**（案件/批次、修订谱系、各环节交接与签署、文件、逐单件保管链、事件哈希锚点） |
| POST | `/revisions/split` `/revisions/merge` `/revisions/correct` | 数量拆分 / 成套合并 / 身份更正 |
| GET | `/revisions` `/revisions/{id}` | 修订记录（before/after） |
| POST/GET | `/files?ref_type=&ref_id=` `/files/{id}` | 登记文件哈希 |
| POST | `/files/{id}/versions` | 新版本（指向上一版，旧版保留） |
| POST/GET | `/handovers?stage=` `/handovers/{id}` | 起草交接 |
| POST | `/handovers/{id}/submit` `/sign` `/receive` `/withdraw` | 提交签署 / 某方签署 / 清点接收 / 撤回 |
| GET/POST | `/issues` `/issues/scan` | 待处置队列、手动扫描 |
| POST | `/issues/{id}/acknowledge` `/resolve` | 受理 / 关闭 |
| GET | `/audit/events?stream_id=` | 事件流（含 hash/prev_hash） |
| POST | `/audit/verify` | 全量重算哈希链并独立重放 |

起草交接支持 `corrects_handover_id`（引用已生效原事件做更正）。所有写请求可带
`client_key`；签署体含 `party_key / signer / signed_at`（不带偏移时按该方时区解释），
可选 `signature_payload`（服务端计算其哈希作为签名指纹）或 `signature_ref`。

## 样例数据

`seed_sample.py` 建立案件 NCHA-RET-2024-007：

- **第一日 2024-02-28 · 华盛顿**，美国移民与海关执法局（ICE, America/New_York）移交：
  石造像 1 套、彩绘陶俑 2 套（按套计数）、恐龙骨架 1 件（单件追踪）；
- **第二日 2024-03-01 · 纽约**，纽约县地区检察官办公室（DANY）移交：
  恐龙蛋化石 6 枚（逐枚编号）、陶俑 1 套。

每批走完整链条：境外方查获单方签署 → 双方跨时区交接（并清点，数量相符）→
入境海关接收/放行两段 → 入藏国博库房。样例最终链完整、待处置队列为空；
测试另覆盖超期、数量不符、断点、篡改等异常路径。
