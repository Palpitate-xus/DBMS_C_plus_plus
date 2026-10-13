# O06：物化视图刷新历史超时的完整复验

旧全量的失败保留；本次使用主分支 R41 后、R24/R43 前的真实已构建二进制
冻结副本，完整原 `tests/matview_refresh_protocol_e2e_test.py` 未修改：

| 配置 | 实际终态 |
| --- | --- |
| 正常默认15秒 | exit 0 |
| 显式原10秒配置 | exit 0 |

证据在 `/tmp/dbms-review-fix-donor.N9sdas6f/`，分别为
`o06-root-before-original-protocol.log`、`o06-root-before-original-10s-protocol.log`。
冻结二进制 SHA256：
`98e4c88582af6d2e7ae732fa80df374adebd2dc5c8be1f913004bd61b818ba89`。

完整覆盖原 typed 空串/NULL/字面量NULL/空格/换行，unpopulated 查询的55000、
限定命名空间/search_path、WITH NO DATA/WITH DATA、CONCURRENTLY 0A000、
失败替换保持旧行和事务控制。未过滤语句、延长第二次超时或修改断言。

当前完整复验通过；历史超时原因仍未确定，没有新增产品修复。不展开用户
暂缓的 CTAS/后台 checkpoint 专项，也不代替最终完整注册回归。
