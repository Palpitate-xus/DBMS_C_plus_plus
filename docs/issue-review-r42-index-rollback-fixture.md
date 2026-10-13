# R42：索引故障回归必须验证未完成回滚的恢复合约

O02 的原生邻居在旧 `!inTransaction()` 断言 exit 134。故障日志显示 row/index
undo 尚未完成；[R17](issue-review-r17-rollback-recovery-ownership.md) 正确保留
事务恢复图像，不能伪造终止 ABORT。此项修正回归夹具，未修改产品实现。

夹具现在先保存自有健康主键/次级索引的确切字节。原故障 UPDATE 仍必须报
IO_ERROR；事务仍在，故障未移除时显式 rollback 仍报 IO_ERROR，且 XID 不变。
关闭自有索引句柄并恢复健康图像后，再显式 rollback 必须成功、结束事务、
释放锁，heap 行与初始行完全相同。然后重新注入原索引故障，让原 REINDEX、
健康写入及两条索引访问验证仍在真实损坏前提下执行。

所有原 IndexScan/BitmapHeapScan 的 XX001、无行及关闭断言保留。不声称失败
UPDATE/rollback 自动修好了无关索引；REINDEX 仍是显式修复操作。

证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`：

| 检查 | 实际结果 |
| --- | --- |
| 原夹具，`o02-neighbor-native.log` | 旧终止事务断言 exit 134 |
| 加强夹具与 rollback graph、pending buffer flush、EXISTS 三完整邻居 | 四完整 native exit 0，`r42-candidate-native.log` |
| 加强夹具，五次新的独立临时 cwd 重复 | 五者 exit 0，`r42-repeat5-confirmed-native.log` |

原 CPP 副本 SHA256 为
`4584ddbc7da62a7d89ba0f7f9460a6b4a0eea43ea45bef86c22f45e322f205e6`。
原生驱动用当前主分支实际生产对象新链接，全部58份源/头/flags receipts
核对且输入不变；这里不是新的 sanitizer 或全注册回归结论。
