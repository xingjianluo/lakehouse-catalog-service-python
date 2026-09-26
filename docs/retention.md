# 快照保留与清理策略

## 1. 策略模型

保留判定在表锁内进行（与提交互斥），一个快照只要满足**任意一条**即保留：

1. 它是当前快照（永不清理）；
2. 它被显式**标签**（tag）固定（长生命周期引用）；
3. 它属于最近的 `min_snapshots` 个版本（默认 5，至少为 1）；
4. 它的年龄小于 `max_age_seconds`（可选；未配置则不按年龄保留）。

策略保存在 `<table>/retention.json`，结构：

```json
{
  "policy": {"min_snapshots": 5, "max_age_seconds": null, "tags": [2]},
  "tags": [2]
}
```

调用 `expire_snapshots(policy=...)`（HTTP `POST /tables/{t}/expire`）时，
传入的策略会被持久化，作为后续默认；标签始终来自持久化状态。

## 2. 清理动作与安全性

被选中的过期版本，在**同一次表锁临界区**内完成：

* 从 `metadata.json` 中移除该快照；
* 移除已无任何存活快照引用的模式版本（schema GC）；
* 删除**仅被过期快照引用**的数据文件；仍被任何存活快照引用的文件
  （例如 `overwrite` 前的旧文件、被标签固定版本引用的文件）一律保留；
* 删除对应的不可变版本记录 `versions/vNNNNNN.json`；
* 最后原子写回 `metadata.json`。

清理是崩溃安全的：数据文件先删、元数据后切换；若中途崩溃，最坏情况是
留下孤儿数据文件，可再次执行清理回收，绝不会出现元数据引用已删文件。

## 3. 账本墓碑：expired ≠ never existed

清理**不会**删除 `ledger.jsonl`。账本记录了每张表上所有曾成功提交的
`commit_id → version`。因此：

* 读取已回收版本 → `snapshot_expired`（HTTP **410**）；
* 读取从未存在的版本 → `snapshot_not_found`（HTTP **404**）；
* 基于已回收版本继续提交 → `stale_base_version`（409，
  reason=`base_version_expired`），与普通并发冲突
  （reason=`base_version_not_current`）明确区分。

`list_snapshots`（HTTP `GET /tables/{t}/snapshots`）同时返回存活快照与
`expired_versions`，便于调用方解释历史。

## 4. 固定仍需读取的历史版本

```
POST /tables/{table}/snapshots/{version}/tag
```

被标签固定的版本在清理时一定保留，其数据文件、模式、版本记录均可读。
这满足「仍被引用的历史版本不得因清理而不可读」。

## 5. 示例（见测试用例）

* `tests/test_snapshots.py::test_tagged_snapshot_survives_cleanup_and_stays_readable`
* `tests/test_retention_persistence.py::test_cleanup_keeps_referenced_versions_only`
* `tests/test_retention_persistence.py::test_overwrite_reclaims_old_files_only_after_new_snapshot_committed`
