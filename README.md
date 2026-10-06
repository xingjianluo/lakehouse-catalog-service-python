# 本地湖仓表目录服务

用本地元数据（JSON）与数据文件（JSONL）模拟湖仓表快照，提供版本化提交、
快照隔离读取、历史版本访问与表结构演进能力。核心逻辑在 `lakehouse/`，
`main.py` 提供 FastAPI HTTP 入口。

## 目录结构

```
lakehouse/
  models.py      # Column / Schema / PartitionFile / Snapshot / CommitRecord
  errors.py      # 可区分错误类型，每个错误带 machine-readable reason
  schema.py      # 结构演进兼容性判定与默认值回填
  storage.py     # 本地存储：原子写（tmp + os.replace）
  transaction.py # 跨表事务数据模型：MultiTableTransaction / TableChange / TransactionResult
  catalog.py     # 目录核心：提交协议、快照读取、pin 与清理、跨表事务
main.py          # FastAPI 入口，错误 reason -> HTTP 状态码映射
tests/           # 并发冲突、幂等重试、不兼容演进、跨表事务等测试
```

## 磁盘布局

```
<root>/<table>/
  table.json                 # 表指针：current_snapshot_id / next_snapshot_id（原子替换）
  snapshots/<id>.json        # 不可变快照元数据（schema + 文件清单）
  commits/<commit_id>.json   # 提交记录（幂等键 + 审计，含拒绝原因）
  pins.json                  # 快照引用计数
  data/<commit_id>/...       # 分区数据文件（jsonl，按提交隔离）
<root>/_transactions/<txn_id>.json  # 跨表事务记录（幂等键 + 各表快照/拒绝原因）
```

## 提交协议（乐观并发控制）

1. `begin_commit(table)` 记录基版本 `base_snapshot_id`，事务内暂存全部变更
   （新增分区、删除文件、新结构）。
2. `commit(tx, commit_id=...)` 在表级锁内执行：
   - **幂等**：`commit_id` 已存在提交记录时，直接返回首次结果（成功快照或
     相同的拒绝原因），重试不产生新快照；
   - **冲突检测**：`base_snapshot_id != current_snapshot_id` 时整体拒绝，
     记录审计并抛出 `commit_conflict`（detail 含期望/实际基版本）；
   - **结构演进判定**：在提交点一次性完成，不兼容则整体拒绝，
     并发提交不会读到中间态；
   - **原子生效**：数据文件 → 快照元数据 → 提交记录依次落盘，
     最后原子替换 `table.json` 切换表指针。整体生效或整体失败，
     读者任一时刻只能看到完整快照。
3. HTTP 入口 `POST /tables/{table}/commits` 把 begin+commit 合并为一次请求，
   `base_snapshot_id` 为必填可空字段（`null` 表示建表后尚无提交）。

## 跨表事务

一笔事务（`txn_id` 标识）可同时覆盖多张表：**全部生效或全部不生效**。

提交协议（`commit_tables`，HTTP `POST /transactions/{txn_id}/commits`）：

1. 按表名排序依次加表级锁 —— 并发事务不会死锁，**书写顺序不影响结果**。
2. **幂等**：`txn_id` 已有事务记录时直接返回首次结果（同一组快照，或相同的
   拒绝原因），重试/重放不产生新版本。
3. **校验阶段**：逐表检查基版本与结构演进。任一表失败则整笔拒绝，
   `transaction_conflict` 的 `detail.tables` 逐表列出各自的 reason 与细节
   （哪张表、期望/实际基版本、或具体 schema 不兼容原因）。
4. **生效阶段**：校验全部通过后逐表暂存（数据文件 → 快照 → 提交记录），
   先落事务记录，最后统一翻各表指针 —— 校验通过后生效阶段不再产生新的失败，
   并发提交不会出现只有一部分表生效。

一致性读取（`GET /transactions/{txn_id}/snapshots` 或
`/transactions/{txn_id}/tables/{table}/snapshot`）：

- 按 `txn_id` 返回该事务在**所有参与表**上产生的一组快照，这组快照互相自洽
  （都能看到这笔事务里的全部变更）。
- 可区分的拒绝（与普通读取失败不同）：
  - `transaction_not_found`（404）：事务不存在、事务被拒绝、或该表未参与此事务；
  - `transaction_snapshot_unavailable`（410）：事务曾成功，但对应快照已被
    清理回收 —— 强调"曾经存在、现在读不到"，与 `snapshot_not_found` 区分。
- 若需要长期可读，可对事务产生的各表快照 `pin_snapshot` 防止被清理回收。

## 快照保留与清理策略

- 读取：`current_snapshot` / `get_snapshot(id)` / `get_snapshot_by_commit(commit_id)`
  / `read_rows` / `list_partitions`，分区信息与所选快照严格一致。
- 不存在或已回收的版本抛出 `snapshot_not_found`（HTTP 404）。
- `cleanup(table)` 回收条件：不在最新 `retain_last` 个快照内、且未被 pin。
  当前快照永不回收。回收时同时删除不再被任何保留快照引用的数据文件。
- `pin_snapshot` / `unpin_snapshot` 维护引用计数：**被 pin 的历史版本
  绝不因清理而不可读**，解除 pin 后才可被回收。

## 结构兼容规则

| 变更 | 结论 | reason |
| --- | --- | --- |
| 新增列（带默认值） | 允许，历史行读取时按默认值回填 | — |
| 类型放宽（int→long→double、float→double） | 允许 | — |
| 类型收窄 / 跨族变更 | 拒绝（422） | `schema_type_narrowing` |
| 删除列 | 拒绝（422） | `schema_column_dropped` |
| 列改名 | 拒绝（422） | `schema_column_renamed` |
| 新增列无默认值 | 拒绝（422） | `schema_default_missing` |

删列/改名会导致历史数据静默错位，因此一律拒绝；演进判定失败时提交整体失败，
表指针不移动，暂存数据不可见。

## 错误与状态码

所有失败返回 `{"reason", "message", "detail"}`：

- 404：`table_not_found`、`snapshot_not_found`、`transaction_not_found`
- 409：`commit_conflict`、`table_already_exists`、`transaction_conflict`
  （`detail.tables` 逐表给出失败原因）
- 410：`transaction_snapshot_unavailable`（事务快照已被回收）
- 422：`schema_*` 系列（见上表）

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest           # 全部测试（实时打印 commit_id / txn_id / snapshot_id / 判定依据）
uv run pytest tests/test_catalog.py::test_concurrent_commits_exactly_one_wins
uv run pytest tests/test_transaction.py   # 跨表事务：并发/幂等/一致读取/回收拒绝

# 启动 HTTP 服务（数据目录可用 LAKEHOUSE_ROOT 指定，默认临时目录）
LAKEHOUSE_ROOT=/tmp/wh uv run python main.py   # 监听 127.0.0.1 随机端口
```

HTTP 示例：

```bash
curl -X POST :$PORT/tables -d '{"table":"t","columns":[{"name":"id","type":"long"}]}' -H 'content-type: application/json'
curl -X POST :$PORT/tables/t/commits -d '{"commit_id":"c1","base_snapshot_id":null,"adds":[{"partition":{"d":"1"},"rows":[{"id":1}]}]}' -H 'content-type: application/json'
curl ":$PORT/tables/t/rows?snapshot_id=1"

# 跨表事务：一次提交覆盖 detail 与 summary 两张表
curl -X POST :$PORT/transactions/txn-1/commits -d '{"tables":{
  "detail": {"base_snapshot_id":null,"adds":[{"partition":{"d":"1"},"rows":[{"id":1}]}]},
  "summary":{"base_snapshot_id":null,"adds":[{"partition":{"d":"1"},"rows":[{"id":1}]}]}
}}' -H 'content-type: application/json'
curl :$PORT/transactions/txn-1/snapshots          # 整组自洽快照
curl :$PORT/transactions/txn-1/tables/detail/snapshot
```
