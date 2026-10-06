# 本地湖仓表目录服务

用本地元数据（JSON）与数据文件（JSONL）模拟湖仓表快照，提供版本化提交、
快照隔离读取、历史版本访问与表结构演进能力。核心逻辑在 `lakehouse/`，
`main.py` 提供 FastAPI HTTP 入口。

## 目录结构

```
lakehouse/
  models.py    # Column / Schema / PartitionFile / Snapshot / CommitRecord
  errors.py    # 可区分错误类型，每个错误带 machine-readable reason
  schema.py    # 结构演进兼容性判定与默认值回填
  storage.py   # 本地存储：原子写（tmp + os.replace）
  catalog.py   # 目录核心：提交协议、快照读取、pin 与清理、跨表事务
main.py        # FastAPI 入口，错误 reason -> HTTP 状态码映射
tests/         # 并发冲突、幂等重试、不兼容演进、跨表事务等测试
```

## 磁盘布局

```
<root>/<table>/
  table.json                 # 表指针：current_snapshot_id / next_snapshot_id（原子替换）
  snapshots/<id>.json        # 不可变快照元数据（schema + 文件清单）
  commits/<commit_id>.json   # 提交记录（幂等键 + 审计，含拒绝原因）
  pins.json                  # 快照引用计数
  data/<commit_id>/...       # 分区数据文件（jsonl，按提交隔离）
<root>/txns/<txn_id>.json    # 跨表事务记录（幂等键 + 整组快照 / 逐表拒绝原因）
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

## 跨表事务

一次提交可同时覆盖多张表（如明细表 + 汇总表）：**全部生效或全部不生效**。

### 提交语义

1. `begin_txn([tables])` 记录每张表当时的基版本；每张表的变更（新增分区、
   删除文件、新结构）暂存在各自的 change 句柄上，写法与单表一致。
2. `commit_txn(txn, txn_id=...)` 在全局事务锁 + 全部表锁（按表名排序获取，
   与单表提交互斥、无死锁）内分三阶段执行：
   - **逐表校验**：每张表独立做基版本冲突检测与结构演进判定，**收集全部
     失败**——任一表失败则整笔拒绝，抛出 `txn_conflict`，
     `detail["failures"]` 逐表给出 `{reason, detail}`（哪张表、为什么），
     未过期的表也不会生效；
   - **全部暂存**：所有表校验通过后，逐表写数据文件、组装快照；
   - **整体落盘**：先写全部快照与 per-table 提交记录（commit_id = txn_id），
     再写全局事务记录 `txns/<txn_id>.json`（committed），最后逐表原子切换
     表指针。读者按事务标识永远只能看到完整的一组快照。
3. **幂等**：`txn_id` 是幂等键。同一 `txn_id` 重试直接返回首次结果——
   成功返回同一组快照，失败返回相同的逐表拒绝原因，不产生新版本。
4. **顺序无关**：事务内表的书写顺序不影响结果（内部按表名排序处理）。

### 按事务读取

- `get_txn_snapshots(txn_id)` 返回整组快照：互相自洽，每张表都能看到这笔
  事务里的全部变更；`get_txn_snapshot(txn_id, table)` 取其中一张。
- 可区分的拒绝（与普通读取失败 `snapshot_not_found` 不同）：
  - `txn_not_found`（404）：该事务从未提交过；
  - `txn_not_committed`（409）：事务被整体拒绝，没有可读快照组，
    detail 里带逐表失败原因；
  - `txn_table_not_involved`（404）：请求的表没参与这笔事务；
  - `txn_snapshot_unavailable`（410）：事务产生的快照已被清理回收，
    按事务读不到当时的版本。

## 错误与状态码

所有失败返回 `{"reason", "message", "detail"}`：

- 404：`table_not_found`、`snapshot_not_found`、`txn_not_found`、
  `txn_table_not_involved`
- 409：`commit_conflict`、`table_already_exists`、`txn_conflict`、
  `txn_not_committed`
- 410：`txn_snapshot_unavailable`
- 422：`schema_*` 系列（见上表）

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest           # 全部测试（实时打印 commit_id / txn_id / snapshot_id / 判定依据）
uv run pytest tests/test_catalog.py::test_concurrent_commits_exactly_one_wins
uv run pytest tests/test_txn.py          # 跨表事务：并发冲突 / 幂等重放 / 一致读取 / 不可用拒绝

# 启动 HTTP 服务（数据目录可用 LAKEHOUSE_ROOT 指定，默认临时目录）
LAKEHOUSE_ROOT=/tmp/wh uv run python main.py   # 监听 127.0.0.1 随机端口
```

HTTP 示例：

```bash
curl -X POST :$PORT/tables -d '{"table":"t","columns":[{"name":"id","type":"long"}]}' -H 'content-type: application/json'
curl -X POST :$PORT/tables/t/commits -d '{"commit_id":"c1","base_snapshot_id":null,"adds":[{"partition":{"d":"1"},"rows":[{"id":1}]}]}' -H 'content-type: application/json'
curl ":$PORT/tables/t/rows?snapshot_id=1"

# 跨表事务：明细表 + 汇总表一笔提交（全部生效或全部拒绝）
curl -X POST :$PORT/txns -d '{
  "txn_id": "txn-1",
  "tables": [
    {"table":"detail","base_snapshot_id":null,"adds":[{"partition":{"d":"1"},"rows":[{"id":1}]}]},
    {"table":"summary","base_snapshot_id":null,"adds":[{"partition":{"d":"1"},"rows":[{"id":10}]}]}
  ]}' -H 'content-type: application/json'
curl :$PORT/txns/txn-1                        # 整组自洽快照
curl :$PORT/txns/txn-1/tables/detail/rows     # 事务内某张表当时的行
```
