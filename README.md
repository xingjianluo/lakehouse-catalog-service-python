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
  catalog.py   # 目录核心：提交协议、快照读取、pin 与清理
main.py        # FastAPI 入口，错误 reason -> HTTP 状态码映射
tests/         # 并发冲突、幂等重试、不兼容演进等测试
```

## 磁盘布局

```
<root>/<table>/
  table.json                 # 表指针：current_snapshot_id / next_snapshot_id（原子替换）
  snapshots/<id>.json        # 不可变快照元数据（schema + 文件清单）
  commits/<commit_id>.json   # 提交记录（幂等键 + 审计，含拒绝原因）
  pins.json                  # 快照引用计数
  data/<commit_id>/...       # 分区数据文件（jsonl，按提交隔离）
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

## 错误与状态码

所有失败返回 `{"reason", "message", "detail"}`：

- 404：`table_not_found`、`snapshot_not_found`
- 409：`commit_conflict`、`table_already_exists`
- 422：`schema_*` 系列（见上表）

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest           # 全部测试（实时打印 commit_id / snapshot_id / 判定依据）
uv run pytest tests/test_catalog.py::test_concurrent_commits_exactly_one_wins

# 启动 HTTP 服务（数据目录可用 LAKEHOUSE_ROOT 指定，默认临时目录）
LAKEHOUSE_ROOT=/tmp/wh uv run python main.py   # 监听 127.0.0.1 随机端口
```

HTTP 示例：

```bash
curl -X POST :$PORT/tables -d '{"table":"t","columns":[{"name":"id","type":"long"}]}' -H 'content-type: application/json'
curl -X POST :$PORT/tables/t/commits -d '{"commit_id":"c1","base_snapshot_id":null,"adds":[{"partition":{"d":"1"},"rows":[{"id":1}]}]}' -H 'content-type: application/json'
curl ":$PORT/tables/t/rows?snapshot_id=1"
```
