# 冻结18d85f4e的完整注册回归终态

2026-10-14标准未过滤`bash scripts/build_tests.sh`完整结束，退出码1。
这是一轮固定输入的历史结果，不代表后续修复组合已通过完整回归。

| 输入/产物 | SHA256或固定值 |
| --- | --- |
| HEAD | 18d85f4e8e8057cbe78c1e014fea49d872b6914d |
| 全部tracked输入 | 5616970c1097f3d6f6828a5b2a667a9a2fbafe947e1dffddafe439367ae5b795 |
| 生产binary | 3f1c6502e0502758af9fdc11e8c324738465de3b19ea098ed4b32bd74f9f11f4 |
| 生产cache输入签名 | 04b6b76d7076dd2c94395e28c861aecf76550f4bd3443cbbbe13f71b9ac882a8 |
| 完整日志 | 4e5c1bc90d8bef09451f67155989fb153e07e12f22267fa46a9eda6ca7495244 |

运行结束的HEAD、tracked输入、binary、cache和58份生产对象receipt均保持不变；
11:51 UTC另行只读核对同样通过，之后才允许主分支集成。原日志目录为
`/tmp/dbms-review-full-20261014-r56.O0AC4HsS/`，包含`run-terminal.log`、
`full-regression.log`、`expected-drivers.txt`和`driver-summary.log`。
这是本机证据路径，不是可移植发行产物。

| 类别 | 注册入口 | PASSED退出标记 | FAILED |
| --- | --- | --- | --- |
| native | 846 | 830 | 16 |
| 真实Main frontend | 3 | 3 | 0 |
| Python注册入口 | 490 | 486 | 4 |
| 合计 | 1339 | 1319 | 20 |

严格汇总核对全部1339个入口各出现一次，没有missing、extra或duplicate。
PASSED标记包含1个TLS环境skip，不能将其当作TLS runtime实际通过。
本轮不声称sanitizer全量验收或CMake验证。

## 全部失败及后续方法

| 原失败入口 | 实际观察 | 后续处理 |
| --- | --- | --- |
| alter_inherit_test | 跨DDL借用旧catalog代 | R64夹具，保留旧代阳性及原断言 |
| alter_sequence_quoted_options_test | 同上 | R64 |
| alter_statistics_test | 同上 | R64 |
| ddl_ast_bridge_test | 同上 | R64 |
| enum_quoted_type_identity_test | 同上 | R64 |
| enum_test | 同上 | R64 |
| runtime_wiring_smoke_test | 同上 | R64 |
| sequence_full_test | 同上 | R64 |
| trigger_test | 同上 | R64 |
| database_directory_guard_test | rename到已存在symlink应为重复目标，却未得到TABLE_ALREADY_EXISTS | 待查实际前置检查顺序；不自动并入暂缓的DROP调查 |
| shared_heap_engine_generation_test | 重建DB后第二engine.beginTransaction失败 | 待记录实际状态并分离generation/ownership原因 |
| snapshot_export_import_test | domain目录58030未捕获而abort | R66可覆盖后台异常边界；原样重跑另遇reader.beginTransaction失败，尚未闭环 |
| snapshot_exporter_visibility_test | domain目录58030未捕获而abort | 独立栈证实backgroundBufferFlush；R66修复后仍须当前完整驱动复验 |
| table_owner_atomicity_test | 首次get(info)前未创建保留目录，3D000 | 先核对原夹具bootstrap前提，保留全部故障/owner断言；不重启权限专项 |
| table_owner_peer_retry_test | 同上 | 同上 |
| view_test | 原借用引用过期；刷新后发现原replace断言要求改变既有列名/类型 | R67按实际PG18.6规则修复产品及错误预期，不仅刷新getter |
| div14_feature_gate_test.py | 等待restore maintenance任务时连接中断 | 原因未定；保留原运行并复验真实后台路径 |
| copy_error_lock_release_protocol_e2e_test.py | 等待协议Ready超时 | 原因未定；不提高超时或删断言 |
| extended_literal_transaction_demand_protocol_e2e_test.py | SELECT 1+2*3得到55P03而非结果 | 待查literal查询是否错误申请DB事务ownership |
| database_directory_guard_protocol_e2e_test.py | rename到已存在symlink得到55006，应为42P04 | 与对应native一起核对错误优先级 |

R64/R66等候选定向门禁通过，不会把本轮20项FAILED改写成PASSED。
新组合必须实际构建并分别记录集成门禁；用户暂缓的R65不在集成范围。
原273项功能总账仍含partial/unverified及15项原始用户暂缓内容。
所有提交仅本地，未push，GitHub Actions继续禁用。
