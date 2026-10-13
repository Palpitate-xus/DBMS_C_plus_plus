# O02：索引损坏断言应服从实际访问路径

原协议驱动把使用 prepared heap child 的 EXISTS 也要求为 XX001；在主分支
`2b180517` 上，原驱动完整运行 exit 1，唯一失败是该查询正确返回 `t`。
这不是索引读取错误被吞掉：EXISTS 没有读取损坏的 B-tree。修正测试，不改
产品访问路径，不为了制造错误让 heap reader 探查无关索引。

保留原全部 SQL、六个真正索引/bitmap/聚合读的严格 XX001 与无行断言；前三个
简单查询另以 EXPLAIN 验证 IndexScan 或 BitmapHeapScan。EXISTS 保留匹配行
精确 `t`，新增不匹配行精确 `f`，损坏前后各验证一次。原 heap 全行与会话
`SELECT 42` 控制也保留，不能把任意成功或空结果算作通过。

证据在 `/tmp/dbms-review-fix-donor.N9sdas6f/`：

- `o02-original-protocol.log`：原完整驱动 exit 1；原文件副本 SHA256 为
  `8898dae0432b9bf9db2bbd2492f095b106ba65a6a5133263a17e8596a3610dee`。
- `o02-plan-aware-protocol.log`、`o02-plan-aware-final-protocol.log`：两次
  过宽的计划断言失败并保留。不能把通用 EXPLAIN 的 TypedSource 外壳当成
  普通聚合接收器的实际物理子计划。
- `o02-plan-aware-accepted-protocol.log`：修正后完整驱动 exit 0。
- `o02-prepared-exists-whole-protocol.log` 和 `o02-prepared-exists-native.log`：
  完整 EXISTS 协议与原生驱动（19 控制）exit 0。

索引原生邻居首次 exit 134 的旧回滚断言另外记录为
[R42](issue-review-r42-index-rollback-fixture.md)，不将失败批次记为全部通过。
