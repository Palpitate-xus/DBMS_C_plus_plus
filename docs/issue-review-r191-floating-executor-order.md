# R191：执行器 FLOAT/DOUBLE 特殊值排序

状态：完成有限修复。已接受源码投影和工作区各19个完整仓库入口均实际退出0，
另各执行一次同字节的原104项外部复现并通过。不是全1492入口或原273要求全部完成。

## 问题、影响与修改方法

| 问题 | 已观察到的事实 | 修改方法 |
| --- | --- | --- |
| SortOp直接使用C++浮点小于号 | FLOAT/DOUBLE含NaN时，原104项在两侧均完整执行、各78失败；直接SortOp及计划器降序均出现错序 | 比较前显式处理NaN，非NaN小于NaN；两个NaN等价，有限值继续原比较 |
| NaN破坏排序等价关系 | NaN与任意值的原生小于比较均为false，排序可能连普通有限值也排错 | 为该分支建立严格弱序，不把NaN当作所有数值的共同等价类 |
| 旧回归只覆盖其他排序调用者 | 原floating_order_by_test通过，不能证明Volcano SortOp特殊值正确 | 在同一既有测试中保留所有原检查，新增真实heap/NULL来源回放和计划器104项 |

NaN大于其他非NaN值的参考规则见
[PostgreSQL 18浮点类型文档](https://www.postgresql.org/docs/18/datatype-numeric.html#DATATYPE-FLOAT)。
本轮核对官方规则，没有声称新执行了真实PostgreSQL差分数据库实验。

生产修改限于[ExecutionPlan.cpp](../src/executor/ExecutionPlan.cpp)的SortOp浮点比较分支。
ASC/DESC先选左右比较值；左侧NaN不小于任何值，右侧NaN大于非NaN，
普通有限值、无穷大和相等值沿用原生比较。NULL位置仍由原独立分支决定。
没有改变排序中断、work_mem/spill、索引、numeric精度、存储格式或公共头。
decimal沿用原分支的值提取，本项不将其精度或全部特殊值支持认定为完成。

## 原失败与完整验收

[floating_order_by_test.cpp](../tests/floating_order_by_test.cpp)新增104项覆盖两种物理浮点列、
八种旋转/反转输入、升降序、默认和显式NULL位置；96项直接SortOp，8项实际计划器。
输入包含负无穷、四个有限值、正无穷、NaN和真实NULL；保留heap来源而非伪造空字符串NULL。
期望id序列写明，不使用待修比较器计算参考结果。未规定相等值的内部稳定顺序。

| 完整运行 | 已接受源码投影 | 工作区 |
| --- | ---: | ---: |
| 原104项、原比较器 | 78失败，退出1 | 78失败，退出1 |
| 同字节原104项、新比较器 | 0失败，退出0 | 0失败，退出0 |
| 保留旧检查并新增104项的完整floating_order_by_test | 退出0，两个完成标记均存在 | 退出0，两个完成标记均存在 |
| 其余12个完整仓库原生入口 | 全部退出0 | 全部退出0 |
| 六个完整协议入口 | 全部退出0 | 全部退出0 |

13个仓库原生入口：floating_order_by、floating_special_values、floating_key_semantics、
floating_precision、floating_zero、analyze_sort_permutation、prepared_sort_child_identity、
prepared_identity_sort、temporal_storage_codec、temporal_storage_layout、physical_attribute_length、
aggregate_percentile、aggregate_argument_expression。
六个协议入口：window_numeric_partition、ordinary_aggregate_order_keys、window_aggregate_arguments、
order_by_duplicate_alias、scientific_numeric_column、computed_query_host_metadata。
均执行原完整输入和适用完成标记，没有抽取其中的断言代替完整入口。

## 构建、版本与暂缓边界

父提交为`2bf24e16`。两侧各fresh编译1个生产CPP和14个原生测试对象，
其余57个生产对象及stub为逐一核对源、127公共头、编译器、flags和对象hash的donor。
所有测试程序和服务器重新链接；Main对象是同源匹配donor，不冒称fresh Main TU或normal/repeat构建。
构建最多两个编译器；运行全部串行。仅停止自有全量调度器，等待原运行子树自然结束后才工作，
最终恢复调度器；原全量输入、每个夹具的超时和持久化设置均未改变。

工作区仍含未接受的R185及其他已有改动，它们的通过结果不等于被接受。
本提交只包含本项生产hunk、既有测试和本报告；R185排序中断改动不进入提交。
由于同一CPP含已暂缓的暂存内容，使用独立索引提交已接受源码投影，并将原暂存内容
精确平移到新HEAD：撤回本项hunk必须重建原暂存blob，其余17个暂存路径及所有其他文件字节保留。
这不是清空、接受或回退用户暂存区。

用户刚跳过的默认postgres_protocol_test错误码项保持未处理，其原失败不覆盖。
原全量1492仍冻结在2442e4c7继续执行；它不包含本项新源码，不能记作修复后全量通过。
R187未定位的原exit2原因仍开放，原273状态22complete/166partial/70unverified/15deferred保持原值。
本项没有新增注册入口：已接受仍921native+568Python+3Main=1492，源码2206；新增本报告后606份MD。
工作区另含既有12个未接受入口和暂缓R185的1个入口及4份未接受MD，不纳入接受计数。
不push，GitHub Actions保持禁用。

## 本机证据

- 原完整104项观察：`/tmp/dbms-r191-sort-special-observation.zxye9jdq/completed-observation-receipt.json`，SHA256 `4ec14066c42b8704aaacecc74520833894ccfe35943e7fd8f678f606a0296bee`。
- 修复候选两侧各20进程：`/tmp/dbms-r191-sort-candidate.z7k3ctv6/completed-candidate-receipt.json`，SHA256 `48155cfe51ac404ec0693c4db7d8d82d410bed068059a4c96c9568cfc36a4d36`。
- 准备与暂存区快照：`/tmp/dbms-review-r46.ifu6SWLp/r191-prepared-candidates-receipt.json`，SHA256 `016db6187e384a9bf3f9e92992ee7d5a8771164a1444072adf7641ffe49ac681`。

以上/tmp路径是本机临时证据，不是可移植发布包或CI验收。
重复NaN/正负零的专门等价组排列、其他全部算子及整库sanitizer没有在本轮新增穷举证明。
本项证明上述明确输入和调用路径，不替代总账要求级验收。
