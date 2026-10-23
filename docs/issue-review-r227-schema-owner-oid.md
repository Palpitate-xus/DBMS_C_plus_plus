# R227：SQL CREATE SCHEMA 写入无效的 owner OID

状态：未来 SQL 创建路径的有限修复已验收；不是整个模式/ACL/角色要求完成。

## 问题与修复

R225 的真实目录查询发现，实际 SQL CREATE SCHEMA 新建的模式为 nspowner=0。
createSchemaNamespace 旧代码无条件调用 createNamespace(name, INVALID_OID)。
同一查询源能准确读回原生目录记录的 owner=42，因此问题在创建端，不能在查询端造 10。

修复只在显式传入 SQL 会话时，按既有 effectiveSessionRole 值查找实际集群角色行，
把该行的 OID 与模式写入同一个 DDL 事务。使用真实 OID，不按用户名猜测、不创建角色，
不修改默认 search_path 或角色选择策略。R210 的暂缓范围没有恢复。

无 SQL 会话的原生接口不借用旁路 currentSession，仍保留未知 owner=0 的既有接口语义；
人工构造但没有真实角色行的原生 SQL 会话也不伪造管理员 OID。
不会推测或回填历史 owner=0 的模式；需要真实所有权证据的历史修复仍未覆盖。
现有 IF NOT EXISTS 不改原有模式的所有者，失败、回滚路径不留下新的模式行。

## 验证

新增 tests/schema_owner_oid_test.cpp 由既有 glob 自动注册，完整 34 项。
覆盖实际 SQL DDL、默认/显式现有角色、不同且非固定的角色 OID、混合大小写角色、
IF NOT EXISTS、显式引擎/SQL 会话与旁路会话隔离、无会话原生接口、真实外层 DDL
事务回滚、一次目录 fsync 故障、持久化与驱逐后读取、事务/会话不泄漏。

| 实际运行 | 结果 |
| --- | --- |
| 最终同源旧版，两侧各完整 34 项 | 各退出 1，8 项 owner 检查失败 |
| 最终同源修复版，两侧各完整 34 项 | 各退出 0，零失败 |
| 既有相关回归，每侧 10 native + 8 协议驱动 | 全部退出 0 |

候选共 38 次程序/驱动入口实际退出 0。包含模式发布、独立引擎、目录冷读取、表 owner
原子性/同级更新重试、完整 403 项限定表授权、模式依赖清理、目录查询及只读保存点。
未运行完整 1,527 个入口；937 native + 587 Python + 3 Main 只是纯提交投影的清单数量。
未运行用户暂缓的默认协议主入口，不声称整库 sanitizer、完整模式权限或全部 273 项完成。

首版夹具在已经写入数据之后构造第二个引擎，额外发生前置/下游失败：两侧完整 34 项
各失败 15 项。保留首源码、诊断源码、二进制和日志。把两个引擎的启动放在所有数据写入
之前，保留全部 34 项断言，重编旧版后各剩 8 项 owner 失败；最终修复再通过同一源码。
诊断输出保留实际状态和错误，不把最初 15 项误报为 15 个产品缺陷。

## 证据与边界

本地目录：/tmp/dbms-r227-schema-owner.mE8XBNo5。
首轮旧基线 completed-old-baseline-receipt.json；诊断为 completed-diagnostic-baseline-receipt.json；
最终同源旧基线 completed-final-old-baseline-receipt.json；新专项 completed-new34-runtime-receipt.json；
构建与相关回归分别为 completed-build-receipt.json、completed-candidate-receipt.json。

只 fresh 编译 DdlExecutor，每侧其他生产对象、全部自有头、编译器和 stub 逐一核对后复用。
工作区 62 个生产对象/134 份头，纯提交 60 个/129 份；两侧分别链接，布局和 stub 不混用。
这不是正常/重复构建或本轮全部 fresh。提交投影不含未接受 R218/R224/R225 和暂缓 R222。
R226 保持用户跳过，R225 的协议/二进制等剩余失败仍开放，不由本项宣称完成。

通过独立 Git index 仅提交 DdlExecutor、本项新原生测试和本文档，保留其他工作区文件及
原 staged 18 路径，不 push、不启用 Actions。原 273 项及冻结 1,492 项历史结果不重写。

| 记录 | SHA256 |
| --- | --- |
| 最终同源旧基线 | `a16b323d068ecdf8403bcafdbdf09277a3bb25f99e84da9b907b2de17bee6c95` |
| 两侧构建 | `61569e841dc8d98617dc11a63411b57ba72ad6418e1752a1d85fff36b5d9fd77` |
| 两侧新专项 | `a4515512efbcc2774b32633339c81ac3acb69d3645776d9fdb61bdabc9f59ddc` |
| 完整 38 次候选运行 | `ca6092d4f6be8a249cea5f06424e79722ae3bee8e3af983a717b750996d484de` |
