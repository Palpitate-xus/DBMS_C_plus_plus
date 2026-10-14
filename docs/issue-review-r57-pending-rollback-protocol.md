# R57：失败物理回滚必须保留协议连接和恢复路径

## 问题与修改

冻结完整missing_index驱动在索引缺失后的INSERT发生连接中断。主分支独立复现
进程SIGABRT(-6)；实际异常栈为状态信息报送→目录路径→pending rollback所有权
拒绝，异常逃出了连接线程。原错误本已是58030，随后取状态却把连接杀死。

修复不改变权限、认证或角色功能：

- 隐式语句的物理rollback未完成时，其仍活跃的事务也进入协议failed block，
  ReadyForQuery为E，普通语句报25P02；不能把它当成健康隐式事务继续执行。
- failed block报告未改变的有效角色状态时复用此前已报告的值，不因发送状态
  再打开与错误恢复无关的目录；健康状态和角色改变仍走原路径。
- 显式ROLLBACK失败直接携带实际DBStatus对应的结构化SQLSTATE，避免把IO_ERROR
  再丢成无状态的“Rollback failed”/XX000。

原missing_index夹具保留缺文件、错误读取、数据、唯一性及REINDEX断言。新增
失败后连接仍活着、E状态、普通查询拒绝、未修复ROLLBACK仍58030/E、恢复真实
健康索引图像后显式ROLLBACK回到I。随后再次注入同一缺文件，保持原REINDEX
修复测试；不允许夹具以清空数据或放宽错误码通过。

## 实际验证

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 结果 |
| --- | --- |
| 原完整/加强驱动在未修正主分支 | 均连接关闭，r57-root-original-reproduce.log、r57-strengthened-original.log |
| 主分支真实异常栈 | r57-root-current-server.log和r57-root-current-stack-symbols.log，确认状态报送异常逃逸 |
| 第一候选 | 连接已保留，但未修复ROLLBACK报XX000；r57-first-strengthened-candidate.log，失败保留 |
| 最终完整故障驱动 | 全0，r57-second-missing_index_protocol_e2e_test.py.log |
| 五次独立完整重复 | 全0，r57-repeat5.log及各原输入驱动日志，三个索引故障均执行 |
| 完整native邻居 | 五项全0，r57-final-current-native.log；包含四个pending rollback夹具与buffer-owner控制 |
| 完整协议邻居 | missing_index、DISCARD pending、index corruption、subtransaction error lock release四项全0 |
| 标准生产构建 | Network真实O2重编和后续Main真实O2重编/链接均0；r57-first/second-standard-build.log，当前58份receipts及native终态输入一致 |

原完整postgres_protocol也执行，但在未改的role_rows断言得到空行、exit1，记录于
r57-second-postgres_protocol_test.py.log。此前R46组合也出现过相同观察、独立复验
通过；当前原因未定。本项不把该批次五个入口记为全绿，也不据此重开用户暂缓的
权限专项。缺失索引核心回归、恢复状态和五次重复不依赖该角色断言。

另一早期诊断在CREATE TABLE阶段超时，未复现索引故障；其日志保留但不当作根因
证据。固定输入的两次SIGABRT与实际throw栈足以定位此次连接线程错误。所有日志
不覆盖原完整f408运行，本项仍须主分支独立集成及后续全注册验收。
