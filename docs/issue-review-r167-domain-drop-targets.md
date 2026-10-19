# R167：DROP DOMAIN限定目标被拆成多个对象（开放）

状态：已实测、未修复，未提交生产修复。

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
