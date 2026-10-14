# 冻结 f4087f5d 的完整注册回归终态

本报告是固定输入的历史验收记录，不代表后续候选或当前主分支全绿。
2026-10-14运行标准未过滤 `bash scripts/build_tests.sh`，04:25启动，06:38终止，
实际退出码1。所有生产源真实重编，没有把私树对象充作主分支构建。

## 输入与终态

| 项目 | 固定值 |
| --- | --- |
| HEAD | f4087f5d4eaf423dd712b559639de00d0d101f8c |
| 全部tracked输入SHA256 | 22a869a06a918ef86e6a736a586fbfbe43548c36fab35cf200002639a6fbe5de |
| 实际生产binary SHA256 | f16deabca10f8bed0a6b2201951a71bd69882c4261bb0d52ef782c28070d0e70 |
| 生产cache输入签名 | 67e4538d295929549e40bff2816eefd7236520584b42cd22980b7311a79c0920 |
| 完整原日志SHA256 | 9faf0420d0fa22bf5b6ab646dccd2fb1052bbe3bec4be492d09abe77fe19ec92 |
| 运行结束检查 | tracked输入、binary、cache和58份生产receipt不变，HEAD不变 |

证据目录 `/tmp/dbms-review-full-20261014.WxKBUtcx/`。`supervisor.log`、
`full-regression.log`、`expected-drivers.txt`及原严格`driver-summary.log`保留。
原生、frontend、协议均使用本轮实际构建产物和默认超时，不修改驱动列表来过滤失败。

| 类别 | 注册入口 | PASSED退出标记 | FAILED |
| --- | --- | --- | --- |
| native | 829 | 824 | 5 |
| 真实Main frontend | 3 | 3 | 0 |
| Python注册入口 | 481 | 473 | 8 |
| 合计 | 1313 | 1300 | 13 |

1300个PASSED标记中含1个TLS环境skip：构建使用TLS stub，statement_timeout_tls
驱动明确跳过。它不是TLS runtime通过；不能把1300写成1300项实际功能全过。
CMake未安装，不声称CMake构建已验证；本轮也不是sanitizer全量验收。

## 严格汇总遗漏与独立审计

原prepared_srf_host_ownership成功提示打印字面反斜杠n，导致随后PASSED标记
不在行首。严格脚本因此记录1312条、1299个PASSED及1项missing；原结果不覆盖。
独立`audit-terminal-markers.py`仅额外接受该完整精确原行，不宽泛截取任意输出中的
PASSED字样，核对1313个预期入口各一条、无重复/额外入口，得到上表。
独立结果保存为`independent-terminal-audit.log`。换行夹具独立修正见R52。

## 全部失败及处理归属

| 原失败入口 | 观察 | 修复/复验归属 |
| --- | --- | --- |
| insert_bloom_cleanup_test | 索引丢失使rollback pending，原夹具误认为已终止 | R48 |
| missing_btree_file_guard_test | 同上，多个索引图像未修复 | R48 |
| savepoint_insert_failure_test | heap故障使undo未完成，原夹具误判终态 | R48 |
| update_page_failure_test | AFTER trigger部分写入后heap损坏使undo未完成 | R48 |
| matview_test | unpopulated INSERT source返回0A000而非55000 | R49 |
| interval_column_modifier_protocol_e2e_test | UNKNOWN文本丢失标量INTERVAL目标范围 | R53 |
| aggregate_expression_scope_protocol_e2e_test | 未提供参数被legacy描述符先推成TEXT | R54 |
| legacy_sql_pattern_consumers_protocol_e2e_test | BYTEA被目录错误注册成用户BLOB类型 | R55 |
| window_type_protocol_e2e_test | ARRAY_AGG窗口的纯元数据绑定缺失 | R56 |
| materialized_view_dml_target_protocol_e2e_test | 同R49的来源错误码 | R49；新组合超时另记R59 |
| missing_index_protocol_e2e_test | pending undo后状态报送触发未捕获异常、连接断开 | R57 |
| alter_sequence_quoted_options_protocol_e2e_test | 启动连接errno103，未执行SQL | R58 |
| truncate_owned_sequence_restart_protocol_e2e_test | 同上 | R58 |

R58两个原完整驱动在主分支ee303db6之前的R53组合、原输入/默认超时独立复跑均0，
`r58-root-alter-sequence-retry.log`和`r58-root-truncate-sequence-retry.log`；历史
启动失败原因未确定，不能据此声称修复了启动竞态。R59原完整71个target控制及
source/事务控制在主分支原样复跑0，`r59-root-target-unchanged-retry.log`；同样
保留较早组合的超时，未通过延长超时/减少控制掩盖问题。
上述重跑日志位于`/tmp/dbms-review-r46.ifu6SWLp/`，不是覆盖原完整运行日志。

## 后续主分支集成

R46/R47/R48/R49/R52分别以47b4e04b/34bff4ea/ce332459/e1ab8111/ebbf795c
合入master，之后真实Root fresh58全部0；输入、全部receipts及标准binary stamp
一致，见root-r46-r49-fresh58.log。R50为9ea19c2d，R53为5cdccf4a，
R54为c5b90fbc，R55为ee303db6，各次变化CPP分别真实标准重编，不挪用私树对象。
每项仍需按当前树的实际门禁记录验收，不以f408这轮替代新组合全量。

集成native批次原11项通过后在rollback_recovery_graph的DROP断言失败，原日志
root-r46-r50-native.log保留。随后完整原驱动及matview、WITH parser、SRF换行、
INTERVAL五项原样重跑0，root-r51-retry-native.log；R51仍是原因未定的观察项，
不能将原失败改称整批全过。后续更完整当前组合检查与下一轮全注册回归继续。

所有提交仅在本地，未push，Actions禁用。原273功能总账仍有partial/unverified，
用户暂缓专项仍暂缓；本报告既不自动完成总账，也不宣称所有错误已消除。
