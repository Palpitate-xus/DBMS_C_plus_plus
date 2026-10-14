# R81：staging消失不等于恢复作业完成

状态：夹具修正已验收，完整及重复门禁通过。

R79组合的额外串行门禁前五个完整入口（PG、真实取消、CLI、COPY、DIV14）
均通过；新restore_parameter_status入口在staging消失后第一次SHOW JOB得到
55P03。连接仍正常返回idle。物理恢复先rename staging，随后还需完成恢复校验、
释放ownership并发布终态；故目录消失只能证明已进入publication，不能证明
管理员目录已可读或job succeeded。原失败保留在
`/tmp/dbms-review-r46.ifu6SWLp/r79-final-serial-protocol-logs/`。

新夹具在这一阶段复用R78已经验证的restore-only轮询：只允许精确55P03消息，
要求idle和没有伪成功数据/CommandComplete，保留原20秒总期限；成功响应仍
严格核对12个字段OID、job ID和最终succeeded。其余错误及failed/cancelled不
会被吞掉，不再复制第二套宽松的错误处理。不修改产品或管理员权限规则。

实际staging窗口内SET/GUC、同一role、SHOW running/精确冲突以及stage仍存在
的原断言均不改变；恢复后的原行数据、GUC和连接断言也保留。该项仅修夹具把
文件系统阶段当成作业终态的错误假设，不把首次失败改写成通过。

`r81-first-protocols.log`三个完整入口0（真实stage、R78五组负控、原DIV14）。
随后真实stage完整入口五次独立重复全0，每次单独保留日志和输入/binary/58份
receipt fence：`r81-repeat-1.log`至`r81-repeat-5.log`及对应driver日志目录。
`r81-final-serial-protocols.log`七个完整入口串行全0：完整PG、真实取消协议、CLI、
COPY、DIV14、真实stage及五组负控；到达最终fence且输入/binary/58份receipt不变。
整体终态在`r81-repeats-and-serial.log`，全部位于
`/tmp/dbms-review-r46.ifu6SWLp/`。没有产品源代码或二进制变化，仍为R79的
eba80763b7b0168a72ca2dc5e6a98236dddb3fb58086d30673c2492c094abe2c。

此组合定向门禁不能代替最新1362入口全量终态，下一轮需另冻结已提交输入。
