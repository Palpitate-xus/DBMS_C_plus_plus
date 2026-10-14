# R48：故障注入回归必须完成真实 recovery owner 的显式重试

## 原始失败

冻结主分支 `f4087f5d` 的完整注册回归实际在四个驱动失败：
`insert_bloom_cleanup_test`、`missing_btree_file_guard_test`、
`savepoint_insert_failure_test`、`update_page_failure_test`。
原始日志保留于 `/tmp/dbms-review-full-20261014.WxKBUtcx/full-regression.log`。

这些夹具让索引文件缺失或让 heap checksum 损坏，并在真实 undo 尚不能完成时
要求事务已经终止、允许普通查询。[R17](issue-review-r17-rollback-recovery-ownership.md)
的正确合约是保留原 XID/undo/锁与恢复图像，不发出虚假终止 ABORT；不能为了
旧断言把数据库改回丢失恢复 owner 的行为。

## 夹具修改方法

四个驱动保留原故障语句、IO_ERROR、真实数据/索引及健康重试断言。仅在实际
发生部分写入、无法完成 undo 的路径增加以下完整阶段：

1. 检查同一 pending XID；故障仍在时显式 rollback 必须仍报 IO_ERROR。
2. 缺失索引夹具还检查普通访问确实被58030拒绝，不能先查询再假设事务已恢复。
3. 恢复夹具提前保存的确切健康文件或 page image，不调用被 retirement 的旧指针。
4. 显式重试原 rollback，成功后要求事务/锁已结束，原 heap/索引数据断言保持。
5. B-tree 维护控制再次移除真实 sidecar 后执行原 REINDEX，不把夹具修复冒充
   自动维护；Bloom 仍验证无 stale RID 及成功插入恰好一个 RID 映射。

UPDATE 的两个 prewrite 故障控制仍要求自动终止，未被改成允许 pending；只有
原 AFTER trigger 已执行写入、损坏妨碍 undo 的路径增加修复/重试。
SAVEPOINT 的最终要求仍是外层完整回滚、两条旧记录/索引均不存在且可重新写入。
本项没有修改产品实现，也不声称一般 I/O 故障均可自动修复。

## 实际验证

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 实际结果 |
| --- | --- |
| 四个原驱动，主分支完整注册 | 全部真实失败，未覆写完整日志 |
| 四个加强驱动，主分支实际生产对象 | 分别exit0，r48-fixture-second.log / r48-btree-second.log / r48-savepoint-first.log / r48-update-first.log |
| 四完整驱动，各五个新的隔离 cwd | 20次全0，r48-four-repeat5.log；全部58份当前生产receipts及不变输入已检查 |
| 含R46/R47的候选组合 | 四驱动与两个完整邻居全0，r48-final-six-confirmed.log；全部58份当前生产receipts及结束输入核对 |

夹具开发时一次错误调用 private closeAllIndexes 的编译失败及最初 runner
优先选择原注册CPP的失败日志均保留。最终只恢复自有确切路径，使用真实
generation 检查，不访问 private API，也不跨 ABI 借用对象。

首次候选七项批次在四个加强驱动通过后，原 rollback graph 邻居的 DROP DATABASE
断言失败；未将该批次标为七项通过。单独带状态观测的原控制随后通过，原因
仍待核验，另列R51，不能据此宣称全套已经绿。这里的最终六项明确不包含该
尚需复查的观察项。没有放宽原超时，也没有新增 sanitizer/崩溃恢复通过声明。
