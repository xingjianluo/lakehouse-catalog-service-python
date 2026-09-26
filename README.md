# Lakehouse Catalog Service（本地湖仓表目录服务）

用**本地元数据 + 本地数据文件**模拟湖仓表快照，提供：

* **版本化提交与快照隔离**：并发写入同表时提交整体生效/整体失败；
  基于过期版本提交被拒绝并给出可区分原因；读者任何时刻只看到完整快照。
* **按快照 / commit_id 读取历史与分区**：分区信息与所选快照严格一致；
  不存在（404）与已回收（410）版本明确区分；被引用的历史版本清理后仍可读。
* **表结构演进与兼容判定**：新增列必须有确定默认值与回填语义；
  类型收窄、删列、改名被拒绝并给出可区分 reason；演进无中间态暴露。

## 1. 安装与运行

```bash
uv sync --group dev

# 启动 HTTP 服务（默认仓库目录可用 LAKEHOUSE_WAREHOUSE 覆盖）
uv run python main.py            # http://127.0.0.1:8000  （/docs 为 Swagger）
```

## 2. HTTP 快速体验

```bash
B=http://127.0.0.1:8000

# 建表（分区列必须在 schema 中）
curl -s -XPOST $B/tables -H 'content-type: application/json' -d '{
  "name":"events",
  "columns":[
    {"name":"id","type":"int","nullable":false},
    {"name":"msg","type":"string"},
    {"name":"dt","type":"string"}],
  "partition_by":["dt"],
  "commit_id":"create-1"}'

# 提交必须带 base_version（初始为 1）
curl -s -XPOST $B/tables/events/commit -H 'content-type: application/json' -d '{
  "commit_id":"c2","base_version":1,
  "append":[{"rows":[{"id":1,"msg":"a","dt":"d1"}],"partition":{"dt":"d1"}}]}'

# 再基于旧 base=1 提交 -> 409 concurrent_commit_conflict / base_version_not_current
curl -s -i -XPOST $B/tables/events/commit -H 'content-type: application/json' -d '{
  "commit_id":"c3","base_version":1,
  "append":[{"rows":[{"id":2,"msg":"b","dt":"d1"}],"partition":{"dt":"d1"}}]}'

# 幂等重试：同一个 commit_id 永远不产生新版本（200 duplicate_commit）
curl -s -XPOST $B/tables/events/commit -H 'content-type: application/json' -d '{
  "commit_id":"c2","base_version":1,
  "append":[{"rows":[{"id":9}],"partition":{"dt":"d1"}}]}'

# 按版本 / commit_id / 分区读历史
curl -s "$B/tables/events/data?version=2"
curl -s "$B/tables/events/data?commit_id=c2&partition=dt=d1"

# 结构演进：新增列（默认值 + 回填策略 on_read|materialized）
curl -s -XPOST $B/tables/events/commit -H 'content-type: application/json' -d '{
  "commit_id":"c4","base_version":2,
  "schema_change":{"action":"add_column","name":"score","type":"int",
                   "default":0,"backfill":"on_read"}}'

# 不兼容变更 -> 422，reason 区分（如 column_drop_not_supported）
curl -s -i -XPOST $B/tables/events/commit -H 'content-type: application/json' -d '{
  "commit_id":"c5","base_version":3,
  "schema_change":{"action":"drop_column","name":"msg"}}'

# 固定历史版本 + 清理；被固定版本仍可读，被清理版本读 -> 410
curl -s -XPOST $B/tables/events/snapshots/2/tag
curl -s -XPOST $B/tables/events/expire -H 'content-type: application/json' \
  -d '{"min_snapshots":1}'
curl -s -i "$B/tables/events/data?version=1"     # 410 snapshot_expired
curl -s    "$B/tables/events/snapshots"          # 含 expired_versions
```

错误体统一为 `{"error": <code>, "message": ..., "reason": ..., ...}`，
HTTP 状态与 code 的对应见 [docs/architecture.md](docs/architecture.md)。

## 3. Python API

```python
from lakehouse_catalog.catalog import LakehouseCatalog
from lakehouse_catalog.errors import ConcurrentCommitConflictError

cat = LakehouseCatalog("/tmp/lakehouse_wh", require_explicit_base=True)
cat.create_table("events",
                 [{"name":"id","type":"int","nullable":False},
                  {"name":"dt","type":"string"}],
                 partition_by=["dt"], commit_id="create-1")
cat.commit("events",
           {"base_version":1,
            "append":[{"rows":[{"id":1,"dt":"d1"}],
                       "partition":{"dt":"d1"}}]},
           commit_id="c2")
print(cat.read_table("events", commit_id="c2"))
```

## 4. 本地验证

```bash
# 全部单测（并发冲突 / 幂等重试 / 不兼容结构变更 / 隔离 / 保留 / HTTP）
uv run pytest

# 查看判定日志（提交标识、快照版本、decision/reason）
uv run pytest -s tests/test_concurrency.py
```

测试矩阵：

| 文件 | 覆盖内容 |
|---|---|
| `tests/test_concurrency.py` | 同 base 并发仅一个成功、冲突原因、失败不留部分写入、失败后可重试 |
| `tests/test_idempotency.py` | 重复 commit_id 不重复应用、携带过期 base 仍幂等、并发重复重试 |
| `tests/test_snapshots.py` | 版本/commit_id 读取一致、分区与快照严格一致、404 vs 410、标签保护 |
| `tests/test_schema_evolution.py` | 默认值/回填、拓宽、收窄/删列/改名按 reason 拒绝、拒绝不落盘 |
| `tests/test_isolation.py` | materialized 演进期间读者只见完整快照、并发演进仅一个生效 |
| `tests/test_retention_persistence.py` | 重启后历史/墓碑可读、清理只删无引用文件 |
| `tests/test_api.py` | HTTP 状态码/错误码与 Python 入口结论一致 |

## 5. 文档

* [docs/architecture.md](docs/architecture.md)：磁盘布局、提交协议、
  快照隔离、结构兼容规则与错误码；
* [docs/retention.md](docs/retention.md)：保留/清理策略、墓碑与 410 语义。
