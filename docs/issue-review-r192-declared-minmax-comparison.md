# R192：列声明驱动的 MIN/MAX 比较

状态：有限修复候选已通过两侧各26个完整入口；本文件随本项独立提交。
不表示全部1492入口或原273要求已完成。

## 问题、影响与修改方法

| 问题 | 实际反例 | 修改方法 |
| --- | --- | --- |
| 数字外观TEXT被当数值 | C排序下对TEXT值'2'和'10'求MIN，原为2，应为10 | 保留列的TEXT声明和collation，不猜测内容类型 |
| BIGINT被double舍入 | 9007199254740992与9007199254740993被当相等，正负值都能选错 | 用声明类型的runtime-cell运算符比较精确整数 |
| NUMERIC精度丢失 | 9007199254740992.0000与.0001舍入成同一double | 使用既有typed NUMERIC比较，不转double |
| NaN排序错误 | REAL/DOUBLE最大值可能停在有限值或Infinity，最小值也依输入次序出错 | 使用既有浮点SQL比较规则 |
| 整数数组按文本排序 | {10}在{2}之前，MIN/MAX颠倒 | 使用数组声明及元素类型比较；已带[]的元数据不重复加后缀 |
| 枚举忽略声明顺序 | z/a/空标签的声明序被字典序代替 | 使用真实DDL产生的列enumValues顺序，空标签仍是真实值 |

修改位于[ExecutionPlan.cpp](../src/executor/ExecutionPlan.cpp)，同时覆盖GroupAggregateOp和
ParallelGroupAggregateOp的普通列MIN/MAX。共用私有DeclaredAggregateLess，经既有ExprEvaluator
和RuntimeCell参数AST比较，不重新解释为SQL字符串；每次open共用一个evaluator，
各聚合保留其列声明和collation。真实NULL仍由已有NULL标志跳过，不把空文本或字面量NULL改成NULL。
枚举序来自实际CREATE TYPE/CREATE TABLE获取并持久化的列元数据。
不改变公共头、存储格式、分组/并行划分、FILTER、输出结构或其他聚合。
未补齐legacy任意表达式类型推断、完整MIN/MAX签名集合或全部catalog依赖。

## 原失败、参考与完整验收

[parallel_exec_test.cpp](../tests/parallel_exec_test.cpp)保留所有原检查，并加入696项编号检查：
14类列、4个真实heap输入行，保留RID/真实NULL来源回放为512行；4种输入次序、
serial/实际4worker、全局/分组，逐格核对MIN/MAX值与NULL标志。
672项值/NULL检查、16项结果形状、8项实际worker启用，共696项，另有必要的数据准备断言。
期望值固定列出，不调用被测比较器生成自己的参考。
另保留原552项全部输入，并增加NUMERIC碰撞、整数数组和枚举144项。

| 完整运行 | accepted-only | 工作区Root |
| --- | ---: | ---: |
| 原比较器，最初完整552 | 160失败/退出1 | 160失败/退出1 |
| 原比较器，最终同字节完整696 | 280失败/退出1 | 280失败/退出1 |
| 修复后，同字节外部696复现 | 0失败/退出0 | 0失败/退出0 |
| 修复后，保留全部旧检查的完整parallel_exec_test | 696完成标记及原完成标记均通过 | 696完成标记及原完成标记均通过 |
| 最终两侧各26个完整进程 | 全部退出0 | 全部退出0 |

26进程为17个完整仓库native、8个完整协议入口及1个外部原696复现。
仓库native：parallel_exec、floating_order_by、aggregate_argument_expression、aggregate_expression_scope、
aggregate_bool、legacy_boolean_aggregate_argument、parallel_boolean_aggregate_expression、prepared_global_aggregate、
aggregate_result_type_metadata、scalar_aggregate_ambiguity、ordinary_aggregate_order_keys、aggregate_percentile、
enum、money、numeric_column_modifier、temporal_storage_codec、array_functions。
协议：aggregate_expression_scope、prepared_global_aggregate、boolean_aggregate_expression、
aggregate_argument_expression、ordinary_aggregate_order_keys、window_aggregate_arguments、
numeric_column_modifier、enum_empty_label。
完整408 worker、152 wire、190 raw观察均保留；190条另核对编号连续和全部compatibilityPass。

实际私有PostgreSQL18.6/180006独立执行353项检查：相同14类声明、512行重复输入、
4种次序和全局/分组的固定期望均通过，包含SQL NULL、空文本与空枚举标签。
只新建本批私有Unix-socket实例和本批枚举类型；现有用户/参考实例未连接或修改，
未关闭fsync，完成后该自有PG实例正常退出0。不是PG侧实际parallel worker证明。

## 候选迭代与边界

V1的11类552项和参考281项均保留；V1候选两侧各26进程实际全部通过。
后续扩展中的V4旧版696准备尝试在首行insertRow断言中止，无完整696标记，
不能计为696项比较失败，也未执行Root侧。该夹具误用了泛型enum占位列而无真实类型定义。
最终V5以真实CREATE TYPE/CREATE TABLE取得枚举列声明，再构造实际输入表；
输入、期望和新增边界均保留，未删除枚举或将空标签改成NULL。
原准备错误日志保留；它不被包装成产品MIN/MAX错值，原生泛型enum构造契约也不据此认定完成。
中间V2/V3仅有准备文件，不冒称执行或通过。

最终两侧各fresh编译1生产CPP、18native对象；原696旧红另外各fresh编译1个probe。
其余57生产对象、stub、127公共头、编译器及flags逐一校验，所有新程序重新链接。
Main对象是同源donor，不冒称fresh Main TU或accepted-only正常构建。
最多两个编译器，数据库进程严格串行；仅暂停自有原全量调度器，等其原子树自然结束，
每批最终恢复调度器。原SQL、断言、各夹具原超时和持久化设置没有弱化。

用户跳过的R185排序中断、默认postgres_protocol_test错误码项及其他暂缓分支不恢复。
通过工作区结果不代表接受其中其他改动。本项用独立索引只提交自身生产hunk、既有测试和报告；
原R185暂存CPP仅在新HEAD上精确平移，其余17个暂存路径和所有其他工作区文件字节保持原值。
原273全文及22complete/166partial/70unverified/15deferred保持原值。
TYPE-01/TYPE-02、QRY-07、OPT-12等整条要求仍需各自完整验收，有限反例修复不能关闭整族。
没有新增注册入口：921native+568Python+3Main=1492，源码2206；新增报告后607份MD。
工作区另有既有12个未接受入口、暂缓R185的1入口及4份未接受MD，不计入接受数量。
原全量仍冻结2442e4c7，原失败不被新结果覆盖；不是修复后全1492通过。不push，Actions禁用。

## 本机证据

- 最终两侧52进程：`/tmp/dbms-r192-minmax-candidate-v5.17pgv2px/completed-candidate-receipt.json`，SHA256 `eeb23782601bc0972add9ef39508980e9cdedb37e2933b2bbc4506ce960d7c3d`。
- 最终旧比较器完整696：`/tmp/dbms-r192-minmax-candidate-v5.17pgv2px/completed-original696-receipt.json`，SHA256 `ee1e537e4f2e988837bf5cd28d6d45c1e351c36d975049cbe9394c3fd36d4257`。
- PG18.6完整353：`/tmp/dbms-r192-pg18-reference-v4.1fw6k32_/completed-reference-receipt.json`，SHA256 `e262b26e1be4cf01abb9a2a02fc2be4f59fabf4a486043358bcfa532d95cfcf3`。
- 最初552旧红：`/tmp/dbms-r192-minmax-declared-observation.ktt9e97c/completed-observation-receipt.json`，SHA256 `aa896d04b0dce16983a0452e43c4ca756a8243ad44ab2c61d9b145e2998e3e59`。
- 首轮V1两侧52进程：`/tmp/dbms-r192-minmax-candidate-v1.pgdwg1l9/completed-candidate-receipt.json`，SHA256 `e73a5d3277a469f7c9c01e65ee2aaf3ad4496850c6ca0f64d3bfb4f984a80780`。
- V4准备失败：`/tmp/dbms-r192-minmax-candidate-v4.ey3uoryx/accepted-original696-runtime.log`；调度器恢复receipt同目录保留。

以上/tmp为本机临时证据，不是可移植发布包、TLS或整库sanitizer证明。

## 正常构建提交验收

工作区以本项最终源码实际执行原scripts/build.sh，仅编译1个生产CPP并链接；
重复构建零编译、零链接。其余57生产对象、127公共头及全部源码/测试字节保持不变。
新生产库重新链接并完整执行parallel_exec、原外部696、floating_order_by、enum、money共5个原生进程，
正常生成的工作区dbms_main再执行prepared_global_aggregate和ordinary_aggregate_order_keys完整协议入口，
全部退出0；696、旧PAR和R191的104完成标记均核对，190条raw另核对连续编号及全部兼容断言。
测试对象复用本批同源匹配对象，不冒称本次重编测试或Main TU。
旧工作区CPP对象及服务器在缓存更新前复制到本批证据目录，原hash保留，历史receipt不改写。
该正常构建发生于本项提交之前，receipt按确切源码hash绑定；不冒称accepted-only正常构建。
自有全量调度器已恢复；暂存内容不因构建而改变。

正常构建及7进程receipt：`/tmp/dbms-r192-root-normal.zme__2y4/completed-normal-build-receipt.json`，SHA256 `242f90a92d1d7aabd3c3f6e026550f68173a7326d352f1683afb244b3472f27d`。
这仍不是修复后全1492或原273要求全部完成。不push，Actions禁用。
