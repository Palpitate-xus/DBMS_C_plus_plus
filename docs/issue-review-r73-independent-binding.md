# R73：纯常量查询在绑定或折叠后错误申请物理ownership

状态：已复现，修复候选等待完整生产构建及门禁。

原完整extended_literal_transaction_demand协议在另一连接持有DDL锁时，
SELECT 1+2*3应返回7，实际55P03。本次原样复跑仍在同一SQL失败。
独立native进一步分离：1+2*3及布尔比较在bind阶段误申请锁，字符串连接在
evaluate阶段误申请锁；原10检查中3失败，实际行/函数/显式类型访问仍正确被阻塞。
证据保留于本机`/tmp/dbms-review-r46.ifu6SWLp/r73-old-native.log`、
`r73-old-stages-native.log`和`r73-old-protocol.log`。

修复方法分两部分：只有既有whole-AST证明为database-independent、且当前真正
处于logical deferred transaction时，绑定使用真实builtin type producer的不可变
物理身份，而不读取另一个DDL owner的目录；其余查询保留原物理fence。
规划器生成的自包含primitive datum必须保留“已分析常量”来源，不能再当成SQL
声明类型重新查目录/转换。parser的真实typed literal默认没有该来源；record、
对象OID、数组、domain等复杂值不走这个primitive捷径。

新native扩充到15检查，覆盖实际session上下文的纯绑定、规划、准备和求值，以及
NULL、精确numeric、BIT、布尔与空结果；仍要求表、函数、显式cast访问报告55P03。
原完整extended协议必须原样通过；不能只修绑定而漏掉折叠后的求值路径，也不能
关闭数据库锁、执行函数/子查询证明纯性或给所有builtin函数放行。

首轮15native及十一完整native、原完整extended与四个协议邻居均0；但扩大对照的
八个隐式输入表达式仍有两项55P03：1='1'和NULL=1。实际PG八项均正确，保留
`r73-expanded-implicit-discovery.log`。整数比较的UNKNOWN转换又经SQL cast重新
解析已知输入类型；须复用真正整数input codec，并把分析后的typed Const保留为
已解析primitive来源。首轮通过不能据此关闭本项。
