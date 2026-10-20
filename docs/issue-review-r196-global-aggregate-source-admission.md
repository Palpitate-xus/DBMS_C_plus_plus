# R196：全局聚合保留目标／来源 AST，并正确描述派生 COUNT(*)

状态：R193 保存的两种目录聚合反例已修复；不是整个聚合或 Describe 功能族完成。

## 反例与两个处理层

R193 的额外探针发现：已建表在 pg_class 中存在一行时，`count(*)+1` 返回 0A000，
而 `count(*) FROM (SELECT relname FROM pg_catalog.pg_class WHERE …) AS c`
把派生来源当成关系名称，返回 42P01。原完整探针与失败日志保留，没有改成任意可接受结果。
PostgreSQL 18.6 对同一 SQL、行值、简单/扩展协议和类型描述实际通过。

Main 的 SUM/COUNT 入口额外限制了“单一直接调用目标、无来源或物理表来源”，
使合法算术目标及逻辑来源落回旧字符串适配器。修改移除这两条不必要的限制，
继续由已有整个查询形状检查决定是否允许执行，保留 CTE、分组、窗口、锁、排序等原边界。
复用已有已绑定聚合／来源执行器，不重新解析派生来源为物理表名，也不伪造目录行或返回值。

第一版实际修复了执行结果，但完整扩展检查又发现：派生 COUNT 的 Statement Describe
仍发 NoData，消息为 ParseComplete、ParameterDescription、NoData，而不是结果描述。
已有纯绑定描述器把 COUNT 参数中的语法星号当成了不可接受的投影星号。

第二层修改只在已有描述器中识别真正聚合目标 `COUNT(*)` 的解析器 LiteralExpr 星号：
恰好一个参数、未声明字面量类型、不是 SQL 子查询。普通投影星号、引用列、字符串 `'*'`、
其他聚合／窗口／FILTER／未支持形状不因此获得新许可。
随后仍从纯 `prepareBoundQuery` 的实际输出声明生成名称、OID、typmod 和宽度；
不执行 SELECT、打开来源或调用标量函数来“采样”类型。这不是暂缓的结构化结果整体重构。

## 永久回归与独立参考

原 pg_class 夹具全部保留，加入原来完整保存的 12 项额外控制，包含：
七列 COUNT、两行计数、COUNT 算术、标量子查询、派生来源 COUNT、限定别名，
简单/扩展协议均检查精确结果、Describe、结果 OID 与命令标签。
原先失败的派生 COUNT 现在正确描述为 bigint/OID20，再返回精确子查询行数。

原 prepared_global_aggregate 的 55 项检查全部保留，新增 12 项顶层控制后共 67 项：
多个聚合、COUNT/SUM 算术、派生 COUNT/SUM、空输入 COUNT=0/SUM=NULL、
LEFT JOIN 的行数及非 NULL 计数、INNER 自连接、含 NULL 的 VALUES 来源。
这些普通表 67 项在修改前两套服务器上已经通过，是路径变更后的非回归控制，
不能描述为“67 个旧版失败”。目录探针的原失败才是本项主要反例。

私有 PostgreSQL 18.6 实际完整通过两个扩展夹具：pg_class 两种协议及额外 12 项，
全局聚合 68 项（参考端 BEGIN 多一项）。永久文件安装后另一次同样完整参考也通过。
均验证实际版本 180006、默认 fsync、只新建本批实例，完成后自有服务器正常停止 0。
全局聚合参考使用其真正 `--reference18` 开关，而非产品路径的普通参数。

最终两套生产库各完成 18 个完整入口，共 36 个退出 0：
pg_class_query、prepared_global_aggregate、ordinary_aggregate_order_keys、
aggregate_argument_expression、aggregate_expression_scope、aggregate_filter_clause、
aggregate_arithmetic_scope、aggregate_result_type_describe、ordinary_aggregate_identity、
boolean_aggregate_expression、null_safe_join、self_join_range_identity、derived_type、
cte_prepared_owner、index_corruption、transaction_begin_grammar、pg_settings_query、
computed_query_host_metadata。
各自完成标记逐项核对；包含 190 条有序聚合原始记录全部 compatibilityPass、
906 项聚合身份、152 项布尔聚合、331 项元数据检查，保留原错误和副作用断言。
安装永久测试后，两个完整新夹具在两套服务器上另外实际执行四次，均退出 0。

## 构建、迭代与版本范围

第一阶段两套各 fresh 编译 Main 并链接；工作区一侧直接走正常 build.sh，重复零编译/零链接。
第一阶段完整 34 次实际结果为 32 通过、两个新派生 Describe 失败，原记录不改写。
第二阶段两套各 fresh 编译 NetworkServer 并重新链接，匹配复用第一阶段 Main；
工作区同样使用正常构建，重复零编译/零链接。未改共享头或存储格式，
每套其余匹配生产对象、127 个公共头、编译器和参数均逐字节检查。
永久测试安装后的再次正常构建仍零编译/零链接，实际默认 dbms_main 也运行完整回归。
正常构建发生于本项提交前，按精确代码 hash 绑定；工作区含原有未接受改动，不是纯提交正常构建证明。
两个阶段原可变 Main/NetworkServer 对象及服务器均先保存字节副本，历史 receipt 不改写。

## 本机证据

- 第一阶段 34 次：`/tmp/dbms-r196-aggregate-0_5twx61/completed-candidate-receipt.json`，SHA256 `d0e26f2d99534ba90d3d6a306458ce9cbf157a443ef4968e49922adb0717604d`。
- 纯提交元数据单元构建：`/tmp/dbms-r196-aggregate-0_5twx61/completed-pure-metadata-v2-build.json`，SHA256 `e20feaf2cc69ec9269d6d3a41d8aefa1f8c0f5aa7e957c0ea69ae0ad1001075d`。
- 最终两套 36 次及工作区正常构建：`/tmp/dbms-r196-metadata-v2.j3186dtt/completed-candidate-receipt.json`，SHA256 `668301183b854589a9a92ec87951456bf94ffc3a11b2740e67ad30ea00efca01`。
- 永久文件四次完整执行及永久 PG 参考：`/tmp/dbms-r196-permanent-fixtures.pw7wwd_8/completed-permanent-receipt.json`，SHA256 `b4ae5cb4efd043512c45dc7d18d81caa4120906c3849ec9f08c3837572d12fa6`。
- 首次完整 PG18 参考：`/tmp/dbms-private-pg18-full-fixture.cvwk8urb/completed-reference-receipt.json`，SHA256 `59cfbdf78da56942dbf97ea223c63e9871098d4840429cc941794ec82e9a72e1`。

这些 `/tmp` 是本机临时证据，不是可移植发布包。本项不包含 COPY 候选或测试调度延迟。
提交仅接收自身 Main/NetworkServer hunk、两个既有夹具及自有文档，原工作区改动与 18 个暂存 R185 路径保留。
无新增注册入口。原冻结全量仍为 1481 PASS/11 FAIL，不被本次定向结果替换；
尚无修复后全 1492、完整 TLS/sanitizer 或原 273 项全部闭合证明。
用户已跳过的默认协议 search_path、R185 及其他暂缓分支不恢复。不 push，Actions 不启用。
