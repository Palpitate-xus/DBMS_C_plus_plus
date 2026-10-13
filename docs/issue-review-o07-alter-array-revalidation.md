# O07：ALTER ARRAY 历史超时的完整复验

保留原审查中的10秒超时记录，不将其改写为成功或声称已证明环境原因。
主分支 `2b180517` 的真实新构建上，原
`tests/alter_array_type_envelope_protocol_e2e_test.py` 未作任何修改，完整执行：

| 原驱动运行配置 | 实际结果 | 日志 |
| --- | --- | --- |
| 当前正常默认15秒 | exit 0 | `root-o07-alter-array-whole-protocol.log` |
| 显式 `DBMS_PROTOCOL_TEST_TIMEOUT=10` | exit 0 | `root-o07-alter-array-whole-10s-protocol.log` |

日志目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`。原非空/多维/NULL/空数组、
两个索引、ALTER 到 BIGINT[]、typmod、失败重写后的保持与 unique 数组全部
运行，未延长第二次超时、过滤 SQL 或移除断言。

状态为“当前完整复验通过，历史超时原因未定”。此项没有新增产品修复，不
展开用户暂缓的 CTAS/后台 checkpoint 专项，也不代替最终完整注册回归。
