# 架构与提交协议

本地湖仓表目录服务，完全基于本地文件系统模拟湖仓（Iceberg 风格）的
**版本化元数据 + 不可变数据文件**。所有入口（Python API 与 FastAPI HTTP）
都经过同一个 `LakehouseCatalog`，因此无论从哪里进入，并发、历史与兼容性
结论完全一致。

## 1. 磁盘布局

```
<warehouse-root>/
  .locks/<table>.lock          # 跨进程 flock（与表目录生命周期解耦）
  <table>/
    metadata.json              # 当前指针 + 所有仍保留的快照/模式
    versions/v000001.json      # 每次提交的不可变版本记录（审计/历史）
    data/<file_id>.json        # 不可变数据文件（行以 JSON 存储）
    ledger.jsonl               # 只追加提交账本（结果与版本墓碑）
    retention.json             # 保留策略 + 显式快照标签
```

关键不变量：

* `data/*.json` 与 `versions/*.json` **只写不改**；
* `metadata.json` 通过「同目录临时文件 + fsync + `os.replace` 原子改名」
  整体切换，读取方任何时刻只可能看到旧版本或新版本的**完整内容**；
* 新数据文件与新版本记录**先落盘**，最后才翻转 `metadata.json` 指针，
  因此不会出现元数据指向尚未写完文件的情况。

## 2. 提交协议（乐观并发 + 表级串行）

提交必须声明它所依据的快照版本 `base_version`（HTTP 入口强制；
Python 便捷模式下缺省表示当前版本）。服务端处理流程：

1. 获取该表的排他锁（进程内 `RLock` + 跨进程 `flock`），并**重新从磁盘**
   读取最新元数据——判定永远基于最新持久化状态，与入口无关。
2. **幂等检查优先**：若 `commit_id` 已存在于只追加账本，直接返回其原版本
   （HTTP 200 + `duplicate_commit`），绝不重复应用；即使重试携带的是
   过期 base，幂等判定也优先于冲突判定。
3. **base 版本判定**：
   | 情况 | 错误码 | HTTP | reason |
   |---|---|---|---|
   | base == current | 允许提交 | 200 | `base_current` |
   | base < current 且仍保留 | `concurrent_commit_conflict` | 409 | `base_version_not_current` |
   | base < current 但已被清理 | `stale_base_version` | 409 | `base_version_expired` |
   | base > current | `validation_error` | 400 | `base_version_ahead_of_current` |
   | base 从未存在 | `validation_error` | 400 | `unknown_base_version` |
   | 未提供 base（严格模式） | `validation_error` | 400 | `missing_base_version` |
4. **先校验、后写盘**：模式演进的兼容性在触碰任何数据前完成；不兼容则
   整个提交被拒绝，磁盘上不产生任何文件。
5. 写入全部新数据文件与不可变版本记录，再原子替换 `metadata.json`，
   最后向账本追加一条已提交记录并 fsync。

「整体生效或整体失败」：原子改名保证元数据只在最后一步翻转；失败路径
不翻转指针（建表失败还会回滚新建目录），因此读者永远看不到部分写入。

## 3. 快照隔离与历史读取

每个快照（`Snapshot`）是一个不可变的整体，**同时**绑定：

* 单调递增的 `version` 与客户提供的 `commit_id`；
* 当时的 `schema_version`；
* 完整的不可变数据文件列表 `file_ids`；
* 当时的分区规范 `partition_spec`。

读取时先解析快照，再取该快照绑定的模式与分区规范，然后逐文件读取、按
**该模式**投影行。因此：

* 历史读取绝不会把「旧文件列表」与「新模式」混用；
* 分区过滤严格以**所选快照**的分区规范为准：对该快照不存在的分区键
  过滤会被拒绝（`unknown_partition_for_snapshot`），而不是静默返回空；
* 可按 `version` 或 `commit_id` 定位快照（二者不能同时指定）。

读取失败原因可区分：

| 情况 | 错误码 | HTTP |
|---|---|---|
| 版本从未存在 / commit_id 未知 | `snapshot_not_found` | 404 |
| 版本曾存在但已被清理 | `snapshot_expired` | 410 |

## 4. 模式演进与兼容规则

支持的类型：`int < long < double`（数值拓宽链）、`string`、`boolean`。

| 变更 | 结论 | reason |
|---|---|---|
| 新增可空列 | 允许（隐式 `null` 默认） | — |
| 新增非空列，带合法显式默认值 | 允许 | — |
| 新增非空列但无默认值 | 拒绝 | `missing_default_for_new_column` |
| 默认值与列类型不符 | 拒绝 | `default_type_mismatch` |
| 重名新增列 | 拒绝 | `column_already_exists` |
| `int→long`、`long→double`、`int→double` | 允许（拓宽） | — |
| `double→long/int` 等收窄 | 拒绝 | `type_narrowing` |
| 跨族变更（如 `int→string`） | 拒绝 | `incompatible_type_change` |
| 删除列 | 拒绝 | `column_drop_not_supported` |
| 重命名列 | 拒绝 | `column_rename_not_supported` |

删列、改名、收窄之所以一律拒绝，是因为它们会让历史数据文件中的字节
在新模式下被**静默**解释成另一列或另一个值——这正是湖仓表格式要避免的
结论。若确需「改名」，应新增一列并由上层迁移数据。

### 默认值与回填语义

新增带默认值的列，对「已存在行」如何落实由 `backfill` 决定（确定且
显式）：

* `on_read`（默认）：不重写历史文件；旧文件按当前模式读取时，缺失列
  以该列默认值投影。历史快照仍绑定旧模式，所以读旧版本看不到新列。
* `materialized`：在同一次提交中，为所有被继承的存活文件生成带新列的
  新文件，旧文件从新快照移除。切换仍然只发生在最后一次原子元数据改名，
  因此并发读者看不到「新模式 + 半重写文件」的中间态。被旧快照引用的旧
  文件继续保留，直到没有任何存活快照引用才会被清理。

## 5. 可观测性

`lakehouse_catalog` logger 对每次接受/拒绝/读取/清理打印结构化字段：
`commit_id`、`base_version`、`version`（快照版本）、`schema_version`、
`decision`（accept / reject / read / cleanup）与 `reason`（判定依据）。
pytest 以 DEBUG 级别捕获并在 `-s` 时输出，见 `tests/conftest.py`。
