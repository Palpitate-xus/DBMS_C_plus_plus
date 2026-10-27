# R307：序列重名创建错误缺失 SQLSTATE 42P07（OPEN）

状态：已复现、未修复、非用户暂缓。本项不是 R306 的完成标签错误，亦不在
用户刚暂缓的 prepared planning/portal 模块内。

R306 完整 100 夹具在主源 PostgreSQL 18.6 全通过。十种序列创建前缀各
Simple/Extended 的同 namespace 重名创建，PG 均 42P07；产品均 XX000。
双方均无成功 CommandComplete、无 DataRow，Ready I，后续 SELECT 42 和
DROP 正常。20 是场景数，不是 20 个独立 bug。完整断言保留在已注册夹具中。

方法：追踪实际序列 DDL 的重名错误产生处与结构化错误/协议映射，让真正的
关系名冲突携带 duplicate_table/42P07；避免在协议层对任意含 already exists
的消息一律替换错误码。应保留 IF NOT EXISTS notice、schema/临时命名空间、
现有对象不变、失败事务及 Simple/Extended 恢复控制，补充原生 DDL 回归后
独立 commit。当前没有本项候选或修复后证据，不因 R306 标签修正而关闭。

最初 TEMP 后普通 CREATE 的负控 namespace 错误已经在 PG 验证并修正，不把
该驱动误判计入本项。实际依据为：
/tmp/dbms-r306-sequence-command-tag.nZJ7AvcS/completed-original-tag-v2-reference-old-receipt.json。
不恢复任何用户已暂缓模块；总目标未完成，不 push，Actions 禁用。

## 已完成的只读定位

在本次受控接受副本中，`DdlExecutor::executeCreateSequence` 的 catalog
`existing` 分支打印 `ERROR: relation ... already exists` 后返回 true，未携带
SQLSTATE。`NetworkServer.cpp` 已有解析合法 `(SQLSTATE ...)` 诊断的路径；
本次不改该映射，更不能对所有 already exists 文本统一填 42P07。
这是后续修复入口的源码证据，不是本项代码已修复的声明。
