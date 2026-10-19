# R169：Simple Query 多语句消息的语法检查时机（开放）

状态：实际复现；完整102项参考全0、旧产品27失败，修复候选尚未完成产品和干净版本验收。

## 已复现的问题

R167 的完整 663 项协议回归在 PG18.6 全0；Root V2 有6项失败，
其中两项是 `DROP DOMAIN schema.guard; garbage` 及其 CASCADE 变体。
PG 直接返回 ErrorResponse(42601)、ReadyForQuery，Root 却先发出
DROP DOMAIN 的 CommandComplete，再报告语法错误。后续域存活检查通过：
既有隐式批事务已回滚 DDL，不能把本例描述为持久数据丢失。

新增完整102项对照另确认：以单独字符串、dollar字符串或右括号作尾片段时，
旧拆分器会丢弃非法片段，产品仅返回DROP的C/Z；随后再次DROP返回42704，
说明这些私有UUID域已实际删除。PG同一输入先报42601，随后DROP成功。
这与原garbage两例的回滚不同，不能把存活性结论跨用到新增输入。
数字尾片段还实际触发XX000 substr越界；nextval控制观察到前缀执行导致后续值为2而非1。

证据为 `/tmp/dbms-review-r46.ifu6SWLp/r168-v2-root-results/domain_drop_target_protocol_e2e_test.log`。
完整 Root52 终态1，50入口通过、2入口失败；原native189的空quoted目标错误另属R167，
本项不通过删除或放宽原663控制来“通过”。

## 根因和修改方法

[NetworkServer](../src/network/NetworkServer.cpp)先检查整条消息的词法错误，
随后拆分并逐句执行，没有在执行首句前识别后续已能确认的原始语法错误。
拆分器还用首个SQL关键字是否为空筛除片段，可能把单独数字、字符串等非空片段当成空语句。

新增parser的error-only语法证明接口，在Q消息开始执行、COPY处理和隐式BEGIN之前扫描片段。
范围仅词法错误、不可能的命令头、已审计的严格DROP DOMAIN语法；
其他已知SQL和项目命令头不解析，保持逐句分析、对象查找、错误与执行时机。
语法失败沿用现有Q词法错误的I/T/E和回滚边界，不另造事务所有权。
拆分器用token是否为空区分注释/空语句与非空非法片段。

这不是完整PostgreSQL raw grammar实现；尚不能证明的语法继续由既有逐句路径处理。
不能依据错误码42601一律提前报错：CREATE DOMAIN的NULL/NOT NULL冲突、UNIQUE等
在PG中由执行阶段拒绝，前面合法语句的结果必须保留。
V1的广泛42601方案因这一静态反例被否决，原草稿冻结保留；V2收窄后又发现
维护命令头可能被误拦，V3补全backup、restore、cancel、dump、clear、pg_switch_wal、switch。
这些维护命令仅做native解析不拒绝检查，没有执行备份、恢复或其他维护操作。

## 验证计划与边界

V3固定native104、wire102；保留原两个失败批次，覆盖非法片段、词法错误、
nextval前缀不执行、CREATE后引用、函数body/注释/引用分隔、空消息、
先前CommandComplete必须保留的分析/执行错误、显式事务I/T/E及恢复。
DataRow长度、列描述、错误码、完整消息序列和清理均检查；UUID对象与私有数据目录隔离。
原V1/V2、V3草稿和收据分别保留，不把草稿数量当作测试通过。

最终夹具路径为 `tests/simple_query_raw_syntax_test.cpp` 和
`tests/simple_query_raw_syntax_protocol_e2e_test.py`，尚未在Root注册或接受。
V3草稿 `/tmp/dbms-r169-rawsyntax-v3.1E9Lczc9`；
新增parser头接口后，两端全部58生产CPP及三个真实Main TU需重新编译并核对，不能复用旧头对象。
R167空目标修正是独立依赖；完整原R167189/663、R164570/956、R168207/332等控制不能减少。
PG实际完整102/0，冻结旧Root完整102/27失败（含后继控制受旧错误状态影响，不等于27个独立根因）。
参考由共享runner核对server_version_num=180006；旧binary SHA256为
`a7867ec6e967cf30d84b19194aa82b45fb9fbd267f744509a9ce91721555bba2`。
日志`/tmp/dbms-review-r46.ifu6SWLp/r169-v3-{reference,old-root}-protocol.log`；
原V1/V2未运行，没有伪造旧94/96结果；尚无新产品104/102通过声明。

不涉及用户暂缓的R156、R151、R51或安全专项；原273未完成，不push，Actions保持禁用。
