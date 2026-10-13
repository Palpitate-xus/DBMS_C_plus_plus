# R23：持久化不健康时禁止维护任务 dispatch

## 修复

原 `loadLocked` 把有效前缀直接加入 jobs；文件坏尾部使加载失败，却留下
Queued 任务给 worker 执行。worker 也不检查 persistenceHealthy；写入 Running
失败仍调用 runner，写终态失败仍 dispatch 后续队列。

现在完整解析到临时 jobs/nextId，只有整份输入和 EOF 检查成功才交换；
无符号字段仅接收十进制数字，拒绝符号和会令 nextId 溢出的最大 job ID。
提交时明确拒绝标识耗尽，不发布 ID 0 或覆盖历史任务。

worker 的等待谓词以 persistenceHealthy 门禁；只有 Running 状态持久成功
才取得任务 ID 并调用 runner。失败时未运行任务保留 Queued/startedAt=0，
后续队列不执行。启动恢复发布、submit、取消或终态持久失败均进入 unavailable
状态；cancel 不得在该状态继续覆写坏文件。正常完成、取消、限速和 Running
重启恢复保留。修复未添加自动重试：必须修复持久化条件并重新启动 manager。

原子 rename 后目录 fsync 失败的磁盘结果可能不确定，因此不能承诺任何任意
外部 runner 的跨崩溃 exactly-once；本项保证检测到持久失败后不 dispatch。
已经在运行的任务仍依原 staging/取消协议结束，不回滚外部任务的既有副作用。

## 实际验证

证据目录 `/tmp/dbms-review-root-conflict.Kdz38nRU/`，原生产基线 `fb095eb4`。

| 检查 | 结果 |
| --- | --- |
| 新 49 项故障检查，原 MaintenanceJobManager.cpp | 28 失败，exit 1 |
| 同一 49 项检查，最终候选 O2 | 0 失败，exit 0 |
| 同一驱动 ASan/UBSan，含 leak 检查 | exit 0 |
| 同一 O2 驱动连续 20 次 | 全部 exit 0 |
| 标准生产构建，真实重编 MaintenanceJobManager.cpp/链接 | exit 0 |
| 新驱动及原 maintenance_job_manager_test，链接真实完整生产对象 | 两者 exit 0 |

故障注入复用已有 dormant `IndexFileUtil` 目录 sync 钩子，不引入生产配置
开关或新公共头文件。同步第一任务，依次精确注入其终态或第二任务 Running
的写入故障；不是靠竞速猜测 dispatch。还验证坏尾部、重复 ID、符号数值、
ID 溢出、原文件不被覆写、启动恢复发布失败、正常修复后重启只执行一次。
测试只清理 mkdtemp 创建的自有目录。

日志：`r23-original-gate.log`、`r23-candidate-gate.log`、
`r23-candidate-gate-sanitized.log`、`r23-candidate-repeat20.log`、
`r23-candidate-production-build.log`、`r23-production-native-gates.log`。
原生完整生产链接前验证全部 58 个当前源/头/flags receipts，结束检查输入未变。

额外 `div14_feature_gate_test.py` 首次 exit 1，在其第 260 行类型别名拒绝
CREATE TABLE 的正常协议接收中 timeout，未到维护任务用例；原记录保留在
`r23-div14-whole-protocol.log`，不计为完整维护协议通过，不放宽 timeout。
该私树不含 Root 已独立合入的 R17 等后续修复；Root 集成后须重新跑完整
驱动，任何剩余失败继续记录，不据局部 native 成功宣称总清单完成。

Root `07f71677` 已实际集成复验：真实变化源新编译及完整生产链接exit 0；
十一完整native（含新/原maintenance驱动）、五完整相关协议及未改动的完整
DIV14能力驱动均exit 0，记录见主[修复清单](review-fixes-2026-10-13.md)。
