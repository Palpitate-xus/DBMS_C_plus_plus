# R155：DROP SCHEMA 保留已解码枚举的实际命名空间

状态：有限修复已验收；本项独立于用户暂缓的 R151 枚举自动数组。
本项只处理标量枚举辅助名称的归属与清理，不恢复任何暂缓模块。

## 问题与修改方法

~~~sql
CREATE SCHEMA "EnumOwner";
CREATE TYPE "EnumOwner"."a.b" AS ENUM ('z','a');
DROP SCHEMA "EnumOwner" CASCADE;
~~~

PG 18.6 成功；原实现将已经解码的 EnumOwner.a.b 再当 SQL 限定名解析，
报 XX001：invalid auxiliary namespace identity。引号、大小写和 public 中带点的名称
还会导致归属误判，可能漏删本 schema 的 sidecar，或错误选择其它 schema 的对象。

| 问题 | 修改方法 |
| --- | --- |
| 已解码名称被再次按 SQL 解析 | 从真实 pg_type 枚举行与 pg_namespace 的 OID 关联建立辅助存储键到 namespace 的映射 |
| 大小写和特殊字符丢失 | 使用目录中的原始名称逐字节匹配，不做小写折叠、前缀猜测或删引号 |
| public 名称内含点 | 目录证明它属于 public；不能按字符串前缀归到另一个 schema |
| 辅助键对应多个目录身份 | 在任何删除前报 XX001；不任选一个 namespace |
| 旧 native 未注册对象 | 保留现有无歧义普通拼写 fallback；其它辅助格式仍走原分支 |

实现位于 [DdlExecutor.cpp](../src/commands/DdlExecutor.cpp)，只新增私有 CPP 逻辑；
没有改 Catalog 生成器、数组身份、共享头、Main、heap/schema 格式或权限分支。

## 验证

| 输入 | 完整控制 | 实际结果 |
| --- | --- | --- |
| 实际 PG 18.6，正式协议夹具 | 87 | 0 失败 |
| 冻结旧 Root / 已接受 R149 clean | 各 87 | 各 25 失败、退出 1 |
| 最终原生夹具，旧实现 | 142 | 42 失败、退出 1 |
| 当前 Root / 仅本项 accepted-base clean | 各 142 native / 87 wire | 均 0 失败 |
| Root 完整相邻入口 | 12 native + 9 wire + 3 Main = 24 | 全部退出 0 |
| 独立 clean 完整相邻入口 | 6 native + 4 wire = 10 | 全部退出 0 |

[142 项原生](../tests/enum_namespace_owner_test.cpp)检查真实目录与 sidecar 的一致性：
两 namespace 的同名类型、大小写/引号/点/空格/UTF-8/含方括号的标量名称、
public 带点名称不被误删、真实 RESTRICT/CASCADE、冷加载、旧 native 未注册名称，
以及故意种入竞争身份时拒绝删除且保留原对象。
[87 项协议](../tests/enum_namespace_owner_protocol_e2e_test.py)检查同样的实际 DDL、
保存点回滚、重建、错误恢复、OID/字段长度/typmod 和 Ready 状态，不执行数组 DDL。

最初 136 项原生旧红 40 失败保留；扩展到 142 项后旧红为 42。
中间原生缺少显式 DbError include 的编译失败保留。
最初旧协议读取器遇到错误后仍强取 RowDescription，提前退出 1；修正为缺失描述也记录
断言失败后，同一 87 条 SQL/预期在 PG 全过、两个旧版各完成 25 失败，未删任何控制。

## 构建与版本边界

Root 正常构建仅一个 DDL CPP、O2、repeat0，全部 58 个当前 receipt 有效。
Main/共享头未变：核对六个源输入、全部头/flags 和对象 SHA 后复用 R148 三个真实
Main 对象，新链接当前 57 个 O2 库并实际隔离运行；不冒充本轮 fresh Main TU。
clean 只含本项两个 partial 源/注册和两个完整夹具：一个 fresh DDL O2、
56 个已核对 R148 fresh58 的生产对象和当前 R149 evaluator；真实 server 无 stubs。
所有旧/新 clean 与 donor 的已跟踪文件永久冻结。

仅本项生产代码、注册、两个夹具及相关文档进入本地 commit。
[R152 显示差异](issue-review-r152-regtype-identifier-output.md)只保存问题与方法，
其两个夹具和注册保持未接受；R151 源码/夹具/注册及其它原有待定改动不混入。
已接受 575 MD、907 native + 546 Python + 3 Main = 1456；
工作区 578 MD、914 + 553 + 3 = 1470，仍有 14 个未接受入口。
无新 1456 全量回归、TLS 或整库 sanitizer 证明；原 273 状态仍 22/166/70/15，
总清单未完成，不 push，Actions 仍禁用。
