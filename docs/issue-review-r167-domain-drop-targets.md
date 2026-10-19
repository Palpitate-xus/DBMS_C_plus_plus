# R167：DROP DOMAIN限定目标被拆成多个对象（开放）

状态：修复候选与完整夹具已准备，最终产品验收及生产提交尚未完成。

## 实测错误

[R164](issue-review-r164-routine-custom-type-declarations.md)完整956项协议夹具在
PostgreSQL18.6全0；组合Root的四个失败之一发生在合法清理语句
`DROP DOMAIN public.d_kind`。项目返回42704，错误把目标写为public；应删除实际限定域。
该夹具使用每次运行独立的UUID对象名；这里是去掉UUID的逻辑写法。

[parser](../src/parser/parser.cpp)的parseDropDomain逐token填入objectNames，
将限定目标拆成public、点号、类型名；[executor](../src/commands/DdlExecutor.cpp)
只读取front()。这不是R164函数声明选择的错误。

## 修改方法和边界

复用现有DROP TYPE/SEQUENCE限定标识符语法状态机，保留原引用、大小写及组件边界，
完整处理IF EXISTS、CASCADE/RESTRICT及尾随语法错误；不要在executor猜测重拼token。
多目标还需显式执行前限制：当前executor只处理第一个对象，
不能让正确解析的多目标语法变成“删除第一个、忽略其余”。
若不实现原子多目标操作，应在任何数据修改前明确拒绝，并验证所有域均保留。
该产品限制不能被当成与PG一致的多目标实现。

待验证裸名、限定名、quoted schema/type、点号/双引号、选项、坏分隔符、
缺失目标、错误后恢复，以及原R164全部956项清理控制。
不涉及用户暂缓的R51缓存锁流程、R151自动枚举数组或R156类型化常量。

证据：`/tmp/dbms-review-r46.ifu6SWLp/r162-v6-root-results/`
下的custom_scalar_routine_declaration_protocol_e2e_test.log；完整956/4及整轮29终态1保留。

## 候选与参考校正

候选保留完整限定/引用目标、严格分隔符和单一尾部选项，解析失败按42601返回；
单目标查找后使用实际domain info.name进行删除及事务记录。
多目标在事务及任何修改前报0A000，这是明确的产品限制，不是PG多目标兼容实现。

PG18.6真实Parse验证表明cascade、restrict、if及其限定写法可作为名字，
不能因其像选项而一律拒绝。原始8条观察在`r167-grammar-reference.log`，不是8项通过。
首轮完整663项参考运行有28个夹具错误，日志`r167-v1-reference-protocol.log`保留：
一类是合法的search_path ParameterStatus；另两条输入实际为语义目标，
三分量跨库名在Parse有效、执行报0A000，既有域的[]目标在Parse有效、执行报42809。
不能把它们都要求为Parse 42601，也不能在成功Parse后直接复用已占用的语句名。

同一663项输入全部保留，修正精确可选Status及这两个控制的阶段/语句生命周期，
`r167-v2-reference-protocol.log`实际663/0。普通坏语法仍检查同名失败Parse后的重试；
语义目标检查先关闭成功Parse的owner再复用同名语句。原夹具保存在
`/tmp/dbms-review-r46.ifu6SWLp/r167-v1-original-fixtures.tar`，未把失败记录改写成成功。
原生固定189项，包括解析、真实删除、namespace存活性、伪造AST及多目标无修改策略。
全部产品入口、原R164完整956项及独立clean构建仍待本轮实际验收。
这里不宣称实现全部数据库限定名或数组domain目录能力。

同一修正后的663项在冻结旧Root二进制实际完成，251失败，
见`r167-r168-old-domain_drop_target_protocol_e2e_test.log`；没有提前停在首个失败。
新两端各23个native TU及各3个真实Main TU已全部编译成功，
不等于测试已执行；生产fresh58构建及后续每端52入口仍在进行。

## V2产品运行检查点

Root正常genuine fresh58生产O2、repeat0及全部58对象输入收据已核对。
23项native完整运行中，本项固定189有1失败：`DROP DOMAIN ""`仍被parser接受，
虽然executor报42601，却没有在PG要求的Parse阶段拒绝；不删除该控制。
其余22项native入口均0；原R164完整956已0失败，包括原四个DROP DOMAIN/SCHEMA清理。
整轮52仍在运行，没有称为全绿。本项补充空decoded target检查已仅放入新的V3 clean，
Root在本轮输入冻结结束前不改源码；完整失败记录`r168-v2-root-runtime.log`保留。
