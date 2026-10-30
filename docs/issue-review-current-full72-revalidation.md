# 当前源码的原完整72入口复验

后续[R185排序中断修复](issue-review-r185-sort-interrupt-propagation.md)各19入口另记。
本页72对应其CPP修复前输入，不能改称修复后已重新跑72。

后续公共测试helper修复见[R184](issue-review-r184-test-data-directory-isolation.md)。
本页完整72对应该脚本修复前源码；C++/程序未变，但不能改称修复后新跑72。

状态：accepted-only clean与工作区Root各72入口实际全部退出0；不是全1492入口或原273要求全部完成。
本轮只有重新链接、验证和证据文档，没有生产修复，也没有证明历史超时的原因已消除。

## 输入、构建与执行边界

已接受生产源码为`17f06229`，本轮文档父提交为`a49b75a7`。
保留原35native、34协议、三个真实Main的72入口及原顺序、原断言和所有适用footer；
R176协议另外独立核对完整190条raw观察的序号、结构和compatibilityPass。
R164仍执行原完整956项，默认socket15秒不变，不改fsync、不缩短SQL或过滤失败。

每侧97对象、39当前程序、127公共头独立核对；58生产、35native、一个stub、三个Main
都是已验证匹配的donor对象。本轮零C++编译，38条真实新链接使用当前57生产库，
服务器为已验证当前产物；不冒称全fresh、normal/repeat build或TLS/sanitizer验收。
三个Main使用已接受测试体的路径投影并包含当前main.cpp，不链接test_stubs，
不夹带工作区未接受的session测试体。全部2206已接受源码blob与tested clean一致，
工作区2325源文件及其他改动保留，运行前后逐门检查源码和全部对象/程序。

所有数据库入口严格串行，clean结束后才启动Root；没有编译与数据库运行重叠。
native/Main各有显式私有数据目录，协议入口移除继承数据目录；R164只读采样仅跟踪自有进程。

## 实际结果

| 版本 | native | 协议 | 真实Main | 整批实际退出 |
| --- | ---: | ---: | ---: | ---: |
| accepted-only clean | 35/35 | 34/34 | 3/3 | 0 |
| 工作区Root | 35/35 | 34/34 | 3/3 | 0 |

完整入口如下。0表示进程退出0且原夹具适用footer通过；没有footer的旧native仍按原退出门验收，
不凭空声称它有新增footer。

| 序号 | 类型 | 原入口 | clean | Root |
| ---: | --- | --- | ---: | ---: |
| 1 | native | [simple_query_raw_syntax_test](../tests/simple_query_raw_syntax_test.cpp) | 0 | 0 |
| 2 | native | [domain_drop_target_test](../tests/domain_drop_target_test.cpp) | 0 | 0 |
| 3 | native | [composite_namespace_owner_test](../tests/composite_namespace_owner_test.cpp) | 0 | 0 |
| 4 | native | [generic_stored_signature_test](../tests/generic_stored_signature_test.cpp) | 0 | 0 |
| 5 | native | [custom_scalar_routine_declaration_test](../tests/custom_scalar_routine_declaration_test.cpp) | 0 | 0 |
| 6 | native | [angle_conversion_range_test](../tests/angle_conversion_range_test.cpp) | 0 | 0 |
| 7 | native | [encoding_boundaries_test](../tests/encoding_boundaries_test.cpp) | 0 | 0 |
| 8 | native | [encoding_functions_test](../tests/encoding_functions_test.cpp) | 0 | 0 |
| 9 | native | [scalar_signature_test](../tests/scalar_signature_test.cpp) | 0 | 0 |
| 10 | native | [stored_function_engine_owner_test](../tests/stored_function_engine_owner_test.cpp) | 0 | 0 |
| 11 | native | [stored_function_scalar_resolver_test](../tests/stored_function_scalar_resolver_test.cpp) | 0 | 0 |
| 12 | native | [stored_function_metadata_namespace_test](../tests/stored_function_metadata_namespace_test.cpp) | 0 | 0 |
| 13 | native | [native_scalar_signature_ownership_test](../tests/native_scalar_signature_ownership_test.cpp) | 0 | 0 |
| 14 | native | [unbound_scalar_signature_preparation_test](../tests/unbound_scalar_signature_preparation_test.cpp) | 0 | 0 |
| 15 | native | [native_float8_callback_override_test](../tests/native_float8_callback_override_test.cpp) | 0 | 0 |
| 16 | native | [float8_scalar_signature_test](../tests/float8_scalar_signature_test.cpp) | 0 | 0 |
| 17 | native | [math_functions_test](../tests/math_functions_test.cpp) | 0 | 0 |
| 18 | native | [composite_test](../tests/composite_test.cpp) | 0 | 0 |
| 19 | native | [native_schema_namespace_owner_test](../tests/native_schema_namespace_owner_test.cpp) | 0 | 0 |
| 20 | native | [native_schema_domain_atomicity_test](../tests/native_schema_domain_atomicity_test.cpp) | 0 | 0 |
| 21 | native | [native_schema_domain_dependency_test](../tests/native_schema_domain_dependency_test.cpp) | 0 | 0 |
| 22 | native | [domain_ddl_io_error_test](../tests/domain_ddl_io_error_test.cpp) | 0 | 0 |
| 23 | native | [domain_namespace_facts_test](../tests/domain_namespace_facts_test.cpp) | 0 | 0 |
| 24 | native | [domain_catalog_text_test](../tests/domain_catalog_text_test.cpp) | 0 | 0 |
| 25 | native | [domain_ancestry_metadata_test](../tests/domain_ancestry_metadata_test.cpp) | 0 | 0 |
| 26 | native | [domain_default_snapshot_test](../tests/domain_default_snapshot_test.cpp) | 0 | 0 |
| 27 | native | [legacy_scalar_routine_arguments_test](../tests/legacy_scalar_routine_arguments_test.cpp) | 0 | 0 |
| 28 | native | [native_binary_length_signature_test](../tests/native_binary_length_signature_test.cpp) | 0 | 0 |
| 29 | native | [aggregate_bool_test](../tests/aggregate_bool_test.cpp) | 0 | 0 |
| 30 | native | [aggregate_percentile_test](../tests/aggregate_percentile_test.cpp) | 0 | 0 |
| 31 | native | [aggregate_argument_expression_test](../tests/aggregate_argument_expression_test.cpp) | 0 | 0 |
| 32 | native | [aggregate_expression_scope_test](../tests/aggregate_expression_scope_test.cpp) | 0 | 0 |
| 33 | native | [prepared_global_aggregate_test](../tests/prepared_global_aggregate_test.cpp) | 0 | 0 |
| 34 | native | [scalar_aggregate_ambiguity_test](../tests/scalar_aggregate_ambiguity_test.cpp) | 0 | 0 |
| 35 | native | [ordinary_aggregate_order_keys_test](../tests/ordinary_aggregate_order_keys_test.cpp) | 0 | 0 |
| 36 | protocol | [simple_query_raw_syntax_protocol_e2e_test](../tests/simple_query_raw_syntax_protocol_e2e_test.py) | 0 | 0 |
| 37 | protocol | [domain_drop_target_protocol_e2e_test](../tests/domain_drop_target_protocol_e2e_test.py) | 0 | 0 |
| 38 | protocol | [composite_namespace_owner_protocol_e2e_test](../tests/composite_namespace_owner_protocol_e2e_test.py) | 0 | 0 |
| 39 | protocol | [custom_scalar_routine_declaration_protocol_e2e_test](../tests/custom_scalar_routine_declaration_protocol_e2e_test.py) | 0 | 0 |
| 40 | protocol | [binary_builtin_array_parameter_protocol_e2e_test](../tests/binary_builtin_array_parameter_protocol_e2e_test.py) | 0 | 0 |
| 41 | protocol | [whole_scalar_parameter_inference_protocol_e2e_test](../tests/whole_scalar_parameter_inference_protocol_e2e_test.py) | 0 | 0 |
| 42 | protocol | [generic_stored_signature_protocol_e2e_test](../tests/generic_stored_signature_protocol_e2e_test.py) | 0 | 0 |
| 43 | protocol | [qualified_scalar_parse_validation_protocol_e2e_test](../tests/qualified_scalar_parse_validation_protocol_e2e_test.py) | 0 | 0 |
| 44 | protocol | [qualified_projection_routine_identity_protocol_e2e_test](../tests/qualified_projection_routine_identity_protocol_e2e_test.py) | 0 | 0 |
| 45 | protocol | [angle_conversion_range_protocol_e2e_test](../tests/angle_conversion_range_protocol_e2e_test.py) | 0 | 0 |
| 46 | protocol | [encoding_boundaries_protocol_e2e_test](../tests/encoding_boundaries_protocol_e2e_test.py) | 0 | 0 |
| 47 | protocol | [bytea_declared_identity_protocol_e2e_test](../tests/bytea_declared_identity_protocol_e2e_test.py) | 0 | 0 |
| 48 | protocol | [scalar_signature_protocol_e2e_test](../tests/scalar_signature_protocol_e2e_test.py) | 0 | 0 |
| 49 | protocol | [float8_scalar_signature_protocol_e2e_test](../tests/float8_scalar_signature_protocol_e2e_test.py) | 0 | 0 |
| 50 | protocol | [stored_function_arity_protocol_e2e_test](../tests/stored_function_arity_protocol_e2e_test.py) | 0 | 0 |
| 51 | protocol | [stored_function_where_execution_protocol_e2e_test](../tests/stored_function_where_execution_protocol_e2e_test.py) | 0 | 0 |
| 52 | protocol | [typed_null_bind_protocol_e2e_test](../tests/typed_null_bind_protocol_e2e_test.py) | 0 | 0 |
| 53 | protocol | [prepared_text_parameter_width_protocol_e2e_test](../tests/prepared_text_parameter_width_protocol_e2e_test.py) | 0 | 0 |
| 54 | protocol | [binary_float_parameter_roundtrip_protocol_e2e_test](../tests/binary_float_parameter_roundtrip_protocol_e2e_test.py) | 0 | 0 |
| 55 | protocol | [schema_namespace_atomicity_protocol_e2e_test](../tests/schema_namespace_atomicity_protocol_e2e_test.py) | 0 | 0 |
| 56 | protocol | [enum_namespace_owner_protocol_e2e_test](../tests/enum_namespace_owner_protocol_e2e_test.py) | 0 | 0 |
| 57 | protocol | [quoted_type_identity_protocol_e2e_test](../tests/quoted_type_identity_protocol_e2e_test.py) | 0 | 0 |
| 58 | protocol | [drop_routine_qualified_name_protocol_e2e_test](../tests/drop_routine_qualified_name_protocol_e2e_test.py) | 0 | 0 |
| 59 | protocol | [domain_ancestry_protocol_e2e_test](../tests/domain_ancestry_protocol_e2e_test.py) | 0 | 0 |
| 60 | protocol | [domain_default_lifecycle_protocol_e2e_test](../tests/domain_default_lifecycle_protocol_e2e_test.py) | 0 | 0 |
| 61 | protocol | [domain_default_transaction_protocol_e2e_test](../tests/domain_default_transaction_protocol_e2e_test.py) | 0 | 0 |
| 62 | protocol | [update_domain_default_protocol_e2e_test](../tests/update_domain_default_protocol_e2e_test.py) | 0 | 0 |
| 63 | protocol | [update_domain_default_transaction_protocol_e2e_test](../tests/update_domain_default_transaction_protocol_e2e_test.py) | 0 | 0 |
| 64 | protocol | [domain_foreign_key_base_protocol_e2e_test](../tests/domain_foreign_key_base_protocol_e2e_test.py) | 0 | 0 |
| 65 | protocol | [quoted_unqualified_scalar_projection_protocol_e2e_test](../tests/quoted_unqualified_scalar_projection_protocol_e2e_test.py) | 0 | 0 |
| 66 | protocol | [abs_column_identity_protocol_e2e_test](../tests/abs_column_identity_protocol_e2e_test.py) | 0 | 0 |
| 67 | protocol | [computed_query_host_metadata_protocol_e2e_test](../tests/computed_query_host_metadata_protocol_e2e_test.py) | 0 | 0 |
| 68 | protocol | [ordinary_aggregate_identity_protocol_e2e_test](../tests/ordinary_aggregate_identity_protocol_e2e_test.py) | 0 | 0 |
| 69 | protocol | [ordinary_aggregate_order_keys_protocol_e2e_test](../tests/ordinary_aggregate_order_keys_protocol_e2e_test.py) | 0 | 0 |
| 70 | main | [explain](../tests/frontend/explain_primary_error_cleanup.cpp) | 0 | 0 |
| 71 | main | [window](../tests/frontend/window_default_order.cpp) | 0 | 0 |
| 72 | main | [session](../tests/frontend/session_execution_lifetime.cpp) | 0 | 0 |

## R164历史失败不被新结果改写

两次历史R176 clean完整72均是71通过、R164 socket超时，无956成功footer。
当前两个完整矩阵中的原956均通过，只能说明这些当前运行未复现；不证明旧超时已修复、
与R176无关或只有I/O一个原因。旧日志、失败终态和此前独立956通过记录继续保留，
详见[R164记录](issue-review-r164-routine-custom-type-declarations.md)。

## 总账与下一步

两侧版本、文档状态、兼容契约和六项总账单测均退出0；两侧
`scripts/check_gap_progress.py --require-complete`仍实际退出1：
22 complete、166 partial、70 unverified、15 deferred_by_user。
complete标签没有在本轮被重新逐条证明。72入口中仅有下列15条原总账的直接引用测试，
而且运行引用测试也不自动证明整条要求的全部原子条件：

| 原ID | 原状态 | 本矩阵包含的直接引用测试数 |
| --- | --- | ---: |
| P0-01 | partial | 1 |
| P0-02 | partial | 1 |
| P0-16 | partial | 1 |
| SQL-03 | partial | 2 |
| SQL-04 | partial | 5 |
| TYPE-01 | partial | 2 |
| TYPE-02 | partial | 1 |
| TYPE-05 | complete | 1 |
| TYPE-07 | partial | 1 |
| TYPE-17 | partial | 1 |
| FUNC-01 | partial | 1 |
| FUNC-05 | partial | 5 |
| FUNC-06 | partial | 3 |
| QRY-01 | partial | 1 |
| TXN-02 | partial | 3 |

其余258条没有本矩阵中的直接引用测试，不等于失败或没有任何间接覆盖，但不能据此宣称全量验收。
后续继续[要求级验收计划](review-closeout-plan.md)：分解原273要求，优先P0和未验证/部分项，
针对实际反例实现、差分验证、独立commit后才改变对应状态；不把新增微小补丁等同于整族完成。

已核实仍未闭合的例子：OPT-08仍有明确heap复查而非真正index-only实现；
[ReplicationManager](../src/replication/ReplicationManager.cpp)拒绝非默认standby、非空primary连接
和启用同步复制，不能把fail-closed误称P0-11物理复制已实现；
[SortOp](../src/executor/ExecutionPlan.cpp)仍在内存物化完整输入，该路径未使用work_mem或临时spill，
不能用配置字段的存在关闭P0-14/OPT-10。其他算子和资源限额需逐一验证，不能从此推断全部行为。

单独的浮点特殊值排序复现稿仍仅在临时证据目录，未运行、未确认缺陷，也未注册或接受为修复；
需要在本矩阵结束后再编译并执行，不能把源码怀疑写成已复现bug。
用户明确暂缓的原15项和后续特定模块保持原边界；本轮不增加新的暂缓ID。
不push，GitHub Actions保持禁用。

## 本地可追溯证据

下列是本机临时证据路径和hash，不是可移植发布产物或自动CI结果。

- clean完整72：`/tmp/dbms-current-full72-clean-runtime.9fsj65qn/completed-runtime-receipt.json`，SHA256 `5478e4d27e7050e5e2516eb3400d741a9f57136ddd64946a49347e4ff4d504e5`。
- root完整72：`/tmp/dbms-current-full72-root-runtime.9d6zj4gt/completed-runtime-receipt.json`，SHA256 `18f4e042cdab92faa4ef8dc3210a1b4e7e9e6b11314e0f0f48179a91be09df91`。
- 独立终态检查：`/tmp/dbms-current-full72-independent-proof.80r9rv43/completed-proof.json`，SHA256 `fe09f2be467d9c67a35ab1eb1f1e214b4bddbce0ee661fd1690cb7a69400937f`。
- 静态检查：`/tmp/dbms-current-ledger-static.cds6jkb6/completed-static-receipt.json`，SHA256 `b9c81566f829b1f90e3085277ec4a9e79ea30606c68697f14100201d0050d7ee`。
- 原273逐引用覆盖：`/tmp/dbms-current-full72-independent-proof.80r9rv43/all273-cited-fixture-coverage.json`，SHA256 `7953e748efd1600dfa30c027e744615059cfead49488740a6cfa0ee995700c12`；只是引用覆盖，不是语义证明。
