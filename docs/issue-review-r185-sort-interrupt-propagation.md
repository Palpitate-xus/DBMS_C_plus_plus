# R185：执行器CPU排序阶段的取消与超时传播

状态：本项排序阶段缺陷已修复并通过两侧有限验收；P0-14、OPT-10和OPT-17整体仍未完成。
源码、永久80控制回归、原子要求账和本记录在同一独立本地提交中。

## 反例、原因与修改方法

旧[ExecutionPlan.cpp](../src/executor/ExecutionPlan.cpp)中12处std::sort/stable_sort调用
在消耗完子输入后仍可长时间进行CPU排序，其比较循环没有检查待处理的取消/超时。
外层checked cursor在open返回后检查，不能替代排序过程中的协作检查。

修复统一使用两个CPP私有helper：算法开始前、结束后及每128次比较检查既有中断状态。
比较器副本共享本次调用的计数器；仍执行原比较器，不改变类型、NULL顺序、tie规则、
排序键求值/缓存、稳定/非稳定算法选择、角色/权限、持久化或用户语句。
共享计数不按比较器复制重置；size_t回绕定义良好。128是比较次数间隔，不是毫秒延迟保证，
单次类型比较内部不会被本补丁抢占。

12处覆盖prepared SELECT/聚合/集合排序、Window排序及最终排序、legacy Sort、
ordered/distinct aggregate、parallel hash的RID排序、MergeJoin两侧和并行worker局部排序。
公共127头、接口和Main/其他native测试体不变，不重开用户暂缓的语义或生命周期调查。

## 永久回归与原始失败保留

[executor_sort_interrupt_test.cpp](../tests/executor_sort_interrupt_test.cpp)完整80控制：
legacy原始整数流和真实prepared逻辑来源；0/1/17/257行、两个方向、无中断状态、空闲状态、
用户取消与statement timeout。取消在输入EOF发布，或由真实libstdc++ stable_sort的nothrow
临时分配入口确定性发布。分配及比较均是真实实现，没有伪造排序结果、计时阈值或信号取栈。
每个适用控制检查EOF及分配钩子的前提；正常控制核对全部有序行，负向核对57014及准确消息。
测试结束恢复前一中断状态，并检查close能完成；这不是完整ResourceOwner证明。

| 实际观察 | 进程退出 | 完整结果 | 解释 |
| --- | ---: | --- | --- |
| 首版旧clean/Root | 各1 | 各80控制/52失败 | 40取消问题加12个夹具槽宽假失败，不能称52产品bug |
| 修正夹具后的旧clean/Root | 各1 | 各80控制/40失败 | 40正常控制全过，取消/超时控制全失败 |
| 新候选 | 0 | 80控制/0失败 | 同一修正夹具，全控制保留 |
| 最终clean/Root | 各0 | 各80控制/0失败 | 各自当前库与真实执行入口 |

首版用4字节原始整数，却误把makeIntColumn的scale=4当字节数，实际声明bigint/8字节。
第二版仅改为scale=2的int/4字节并增加显式类型/槽宽断言，全部80控制保留；
旧实现再次完整40失败。两版源码、完整失败日志及修改前提均保存，不删除检查或改写旧终态。
40是同一跨路径缺陷的控制数，不是40个独立bug。

## 最终构建与完整邻近入口

| 版本 | native | 真实Main | 协议 | CLI | 整批实际退出 |
| --- | ---: | ---: | ---: | ---: | ---: |
| accepted-only clean | 12/12 | 3/3 | 3/3 | 1/1 | 0 |
| 工作区Root | 12/12 | 3/3 | 3/3 | 1/1 | 0 |

每侧19完整入口保留原断言、默认协议15秒及夹具自己更严格的5秒/CLI15秒期限，不改fsync。
原ordered aggregate190额外核对全部raw序号、OBSERVED状态、结构及compatibilityPass；
原aggregate identity906、statement timeout协议/CLI均完整通过，三个Main不链接test_stubs。
所有数据库运行严格串行，且不与编译/链接批次重叠。实际入口：

- `native executor_sort_interrupt_test`。
- `native ordinary_aggregate_order_keys_test`。
- `native aggregate_bool_test`。
- `native aggregate_percentile_test`。
- `native aggregate_argument_expression_test`。
- `native prepared_global_aggregate_test`。
- `native scalar_aggregate_ambiguity_test`。
- `native prepared_source_context_test`。
- `native set_operation_volcano_test`。
- `native window_functions_test`。
- `native enum_test`。
- `native parallel_exec_test`。
- `main explain`。
- `main window`。
- `main session`。
- `protocol ordinary_aggregate_order_keys_protocol_e2e_test`。
- `protocol ordinary_aggregate_identity_protocol_e2e_test`。
- `protocol statement_timeout_protocol_e2e_test`。
- `cli statement_timeout_cli_e2e_test`。

每侧74对象、16当前程序、127头独立核对。Root正常构建只重编1个ExecutionPlan TU并链接，
重复构建零编译/零链接；其余57生产对象为已验证donor。clean验收组合复用前段候选fresh1CPP
及其余57匹配生产对象，不冒称clean也执行normal/repeat或58对象全部fresh。
Root fresh6native+6已验证native donor；clean focused fresh5native+7已验证donor，其中80对象
来自前段同源候选fresh编译。每侧三个Main对象均验证当前main.cpp/全部头/已接受测试体投影后复用，
新链接当前57生产库；没有冒称fresh Main TU。

提交的全部2207源码投影与tested clean逐blob一致；Root2326保留其他既有源码改动。
合入前新恢复副本保存2325源码与118缓存文件。正常构建只改变ExecutionPlan对象、其签名、
server及全源码构建印记；其余114缓存文件保持原字节，当前58对象签名和构建印记均重算验证。
独立复核首轮误要求全源码印记不变而退出1；失败日志保留。修正的是验收器对印记语义的
假设，新增当前签名重算及全部58签名检查，程序测试、源码和断言没有因此修改。

本轮登记922native+568Python+3Main=1493，Root928+574+3=1505，原12未接受入口仍分离。
跟踪606MD、Root609MD，三份未接受文档保留。这不是全1493、修复后完整72、TLS或整库sanitizer验收。
以前[两侧完整72](issue-review-current-full72-revalidation.md)是修复前输入，不能用本项19替换或改写。

## 原273项的要求级账

[review-requirement-clauses.json](review-requirement-clauses.json)保留全部273个原ID、要求全文、
原标签和原证据引用；既有22 complete只是原声明，不被当作新的要求级证明。
本轮只完成三个条目的原子拆解：P0-14十一条、OPT-10十二条、OPT-17十五条，共38条。
其余270尚未拆解，其中包含原15用户暂缓项；不表示恢复暂缓，也不是270项都已审完。
下面“局部”仅指本项排序阶段证据；没有任何一条原子要求或原功能项因此标记proven。

| 原子条件 | 原文要求范围 | 当前证据状态 |
| --- | --- | --- |
| `P0-14.work_mem_budget` | 实际执行按work_mem约束工作内存，不是仅保存或展示配置。 | Sort路径反证，未完成 |
| `P0-14.temporary_spill` | 超出工作内存时使用临时文件spill，保持结果和错误语义。 | Sort路径反证，未完成 |
| `P0-14.connection_limits` | 实施连接数量限额。 | 尚无充分证据 |
| `P0-14.worker_limits` | 实施worker数量限额。 | 尚无充分证据 |
| `P0-14.memory_limits` | 实施内存使用限额。 | 尚无充分证据 |
| `P0-14.io_limits` | 实施I/O使用限额。 | 尚无充分证据 |
| `P0-14.cancel_propagation` | 取消请求传播到所有相关执行阶段，含CPU物化/排序。 | 仅排序阶段局部证据 |
| `P0-14.timeout_propagation` | 超时传播到所有相关执行阶段。 | 仅排序阶段局部证据 |
| `P0-14.disk_full_fail_safe` | 磁盘满时维持fail-safe行为与数据库不变量。 | 尚无充分证据 |
| `P0-14.descriptor_exhaustion_fail_safe` | 文件描述符耗尽时维持fail-safe行为与数据库不变量。 | 尚无充分证据 |
| `P0-14.out_of_memory_fail_safe` | OOM时维持fail-safe行为与数据库不变量。 | 尚无充分证据 |
| `OPT-10.sort_work_mem` | sort实际使用work_mem预算。 | Sort路径反证，未完成 |
| `OPT-10.sort_spill` | sort超预算时支持临时文件spill。 | Sort路径反证，未完成 |
| `OPT-10.hash_work_mem` | hash实际使用work_mem预算。 | 尚无充分证据 |
| `OPT-10.hash_spill` | hash超预算时支持临时文件spill。 | 尚无充分证据 |
| `OPT-10.aggregate_work_mem` | aggregate实际使用work_mem预算。 | 尚无充分证据 |
| `OPT-10.aggregate_spill` | aggregate超预算时支持临时文件spill。 | 尚无充分证据 |
| `OPT-10.window_work_mem` | window实际使用work_mem预算。 | 尚无充分证据 |
| `OPT-10.window_spill` | window超预算时支持临时文件spill。 | 尚无充分证据 |
| `OPT-10.join_work_mem` | join实际使用work_mem预算。 | 尚无充分证据 |
| `OPT-10.join_spill` | join超预算时支持临时文件spill。 | 尚无充分证据 |
| `OPT-10.batch_processing` | 相关执行节点支持有界批处理，不以一次性全量物化代替批处理。 | 尚无充分证据 |
| `OPT-10.skew_handling` | 相关执行节点处理数据倾斜。 | 尚无充分证据 |
| `OPT-17.operators_cancel` | executor cancellation贯穿全部算子。 | 仅排序阶段局部证据 |
| `OPT-17.operators_interrupt` | interrupt贯穿全部算子。 | 仅排序阶段局部证据 |
| `OPT-17.operators_statement_timeout` | statement timeout贯穿全部算子。 | 仅排序阶段局部证据 |
| `OPT-17.operators_error_cleanup` | error cleanup贯穿全部算子。 | 尚无充分证据 |
| `OPT-17.operators_resource_owner` | resource owner贯穿全部算子。 | 尚无充分证据 |
| `OPT-17.workers_cancel` | executor cancellation贯穿全部worker。 | 尚无充分证据 |
| `OPT-17.workers_interrupt` | interrupt贯穿全部worker。 | 尚无充分证据 |
| `OPT-17.workers_statement_timeout` | statement timeout贯穿全部worker。 | 尚无充分证据 |
| `OPT-17.workers_error_cleanup` | error cleanup贯穿全部worker。 | 尚无充分证据 |
| `OPT-17.workers_resource_owner` | resource owner贯穿全部worker。 | 尚无充分证据 |
| `OPT-17.io_cancel` | executor cancellation贯穿全部I/O。 | 尚无充分证据 |
| `OPT-17.io_interrupt` | interrupt贯穿全部I/O。 | 尚无充分证据 |
| `OPT-17.io_statement_timeout` | statement timeout贯穿全部I/O。 | 尚无充分证据 |
| `OPT-17.io_error_cleanup` | error cleanup贯穿全部I/O。 | 尚无充分证据 |
| `OPT-17.io_resource_owner` | resource owner贯穿全部I/O。 | 尚无充分证据 |

work_mem/temp spill仍没有实际接入SortOp的全量输入缓冲路径；hash/aggregate/window/join的
预算、spill、批处理、skew及全部worker/I/O/error cleanup/ResourceOwner仍需分别实现或验证。
本项不覆盖其他文件的排序、全部CPU物化/解析或阻塞I/O，也不关闭P0-14/OPT-10/OPT-17。
后续按[总计划](review-closeout-plan.md)继续原子审计和P0实现，不把“字段存在”或小专项全绿当作整族完成。
原273仍22complete/166partial/70unverified/15deferred，总目标未完成，明确暂缓不恢复。
R164历史两次超时原因仍未确定；本项不借旧矩阵通过倒推原因已修。不push，不启用Actions。

## 本地证据

以下路径是本机临时证据，不是可移植发布产物；永久80夹具、原有完整邻居和原子范围账保存在仓库。

- 当前构建/运行：`/tmp/dbms-r185-clean-focused-compiler.j3n_ydf8/completed-compiler-receipt.json`，SHA256 `1156bbf26bf7aa56284cfe4ba4490c3015b46dd0bdfab7372a7140bb0192fb5e`。
- 当前构建/运行：`/tmp/dbms-r185-clean-focused-runtime.ptijibw9/completed-runtime-receipt.json`，SHA256 `a3f214ec6d6a402325ba27943888f5b4ae1f6a03b89d4d3ae8b8ae535437dcf0`。
- 当前构建/运行：`/tmp/dbms-r185-root-focused-compiler.ani6a9xo/completed-compiler-receipt.json`，SHA256 `30d4798532ca5737b505cd7d95521872bc7cd2baf10a905fe50b905c612da448`。
- 当前构建/运行：`/tmp/dbms-r185-root-focused-runtime.6rp__8oz/completed-runtime-receipt.json`，SHA256 `20b815891c075c79667eec3947c8ed436a4e04a1ad7985e83f6822db4fc98f0a`。
- 旧V2反例：`/tmp/dbms-r185-old-interrupt-v2.qhl1uq6z/completed-observation-receipt.json`，SHA256 `33683f836eece971f4e0c58a6aa72796cef68748f128e96d1eba9649adf3d854`。
- 新恢复副本：`/tmp/dbms-r185-root-preinstall-recovery.3owvple6/completed-recovery-receipt.json`，SHA256 `8bdcc15f17414297ff60d6b99ccc50d3a1400716fdddb0776f344ba5986f0cca`。
- 精确合入：`/tmp/dbms-r185-root-preinstall-recovery.3owvple6/completed-install-receipt.json`，SHA256 `8f0bf7506467f3d6fec2aacc56d52389c25b23e168ba2a0c9aaf1eb0f8307c30`。
- 独立预提交复核：`/tmp/dbms-r185-independent-pre-proof.vvl1i9_g/completed-proof.json`，SHA256 `11f415bb0b70378e335a31f22e8db591ba25d30f6987a8c4065ce448711a3a36`。
- 首轮印记守卫失败：`/tmp/dbms-review-r46.ifu6SWLp/r185-independent-pre-proof-driver.log`；修正后完整复核见上方receipt。
