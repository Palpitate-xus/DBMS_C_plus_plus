# R200：标量桥接必须发布已有 SQL NULL 位

状态：有限 NULL 位修复已通过下列完整定向验证；不是整个结果载体或 ProjectSet 需求验收。

## 已确认的问题与修改方法

公开的 `StorageEngine::queryExpr` 结构化重载同时返回字符串单元格和独立 SQL NULL 位。
其原生 SelectExpr 扩展行路径在标量助手没有提供 `knownNull` 时，用字符串是否等于 `NULL` 猜测 NULL。
UPPER(`null`) 和 REVERSE(`LLUN`) 实际都返回非 NULL 文本 `NULL`，旧路径却把 NULL 位标为真。
普通不扩展的路径会再次求值以确定 NULL；这不意味着原生扩展路径已经正确。

根因不是 UTF-8 或数组元素本身：多处助手已有 `ExprEvalResult`／`ExprValue.isNull`，
却只返回显示文本，丢弃已有标记。修改集中在现有私有 `applyScalarFunc`：

- 统一发布其 27 个已有带类型结果的返回点，其中 7 处已有单独发布、20 处遗漏；保留已有正确行为。
- 严格函数已确定实参为 SQL NULL 的早退分支发布 true。
- 手工 REPLACE／REVERSE 在已通过原严格 NULL 检查后返回文本，明确发布 false。

没有新增执行、根据文本猜类型、改异常／锁／事务接口或改变公共 API、头文件、存储和结果载体。
原有字符串返回值保留，结构化消费者使用已经存在的 optional NULL 标记，不需要重复求值来辨认这些结果。
本项不是整库结构化结果重构，也未声称修复所有 legacy 实参求值次数或任意 SRF 组合。

## 必须区分的 SQL 边界

修复前普通线协议查询能区分空文本、文本 `NULL` 和 SQL NULL；真实写入函数在六行上恰好调用六次，
WHERE false／LIMIT 0 没有调用。最初假设没有在这些普通 SQL 场景复现，不能把它们写成旧红。

`SELECT id,upper(t),unnest(ARRAY[1]) FROM ...` 则在项目 SQL 入口返回 0A000。
真实 PG18 接受该查询；项目的物理来源 ProjectSet 尚未完整实现，不能把这个拒绝算作兼容通过。
本项修复的是已公开 C++ API 的既有原生扩展路径，不会因此声称已实现该 SQL 功能或恢复暂缓专项。

## 旧红及独立参考

新原生夹具保留 23 个函数／形式、七行真实文本／NULL 数据，普通与扩展两种路径，
正反排序以及无界、0、1、9 行需求；逐行核对原始单元格、NULL 位、源 id、扩展元素和结果形状。
另保留 14 个精确错误码及无遗留事务检查。共 385 个控制，两套旧库均完整执行，
各 84 项失败、实际退出 1；这是重复输入／消费形式的失败控制，不是 84 个独立 bug。

实际私有 PostgreSQL 18.6／180006 对应的完整 368 个值／NULL／排序／LIMIT 控制全部通过，
同查命令标记和真实 OID；保留默认 fsync 并正常停止实例。
PG 中以 `ORDER BY id,n` 明确重复源行内元素顺序；C++ API 使用它既有的源顺序和数组展开顺序。
该参考证明相同操作的预期值和 NULL 区别，不是 PostgreSQL 暴露项目 C++ API 或项目 SQL 已支持 ProjectSet 的证明。

最初原生观察脚本错误地用 Root 的头文件比对 clean donor，编译前即被校验拒绝。
修正为两侧各自的 127 头文件后运行；两侧存在未接受的 OutputCapture 头差异，不能混用。
这不是接受该外来头修改，也不把该守卫失败算作产品缺陷。

## 修复后构建与回归

两套程序的新完整 385 项均通过；它们各另运行 15 个完整原生入口和 19 个完整协议入口，
共 70 个实际进程退出 0，所需完整完成标记均核对。

- 原生共每侧 16 个：新 385、文本 locale 259、string_functions、range_functions、range_bound_projection、
  scalar_signature、native_scalar_signature_ownership、utf8_split_linear 76、OVERLAY 原检查及新增 12、
  numeric_division_projection、math_projection_error、gcd_lcm_projection_semantics、coalesce_projection_null、
  nullif_projection_semantics、expression_null_predicate、null_predicate_projection。
- 协议共每侧 19 个：text_case_locale 360、table_structured、table_case_ast、scalar_function_order_structured、
  scalar_signature 132、qualified_projection_routine_identity 637、builtin_range_catalog 121、
  plpgsql_collation_label、join_collation_role、computed_query_host_metadata 331、prepared_global_aggregate 67、
  pg_class_query、OVERLAY 94、utf8_split_linear 329、string_length_signature 654、
  quoted_unqualified_scalar_projection 791、ordinary_aggregate_identity 906、reverse_linear 96、repeat_empty_input 80。

每侧 fresh 编译 1 个 TableManage 并重新链接服务器，其他 57 个生产对象和 stub 按本侧
来源／头文件／编译器／参数／对象核对复用，不冒称 Main 或所有 58 个单元 fresh。
每侧原生单元有 8 个本轮 fresh（含旧红阶段编译后保持不变的新 385），另 8 个复用经核对的 R199 对象；
16 个原生程序都新链接本侧当前库。工作区实际正常构建只编译这 1 个 CPP 并链接，
再次正常构建零编译／零链接，实际默认 dbms_main 参与完整工作区检查。

这些验证不证明 27 个返回点的每个条件分支或所有 legacy API 都已覆盖；
没有将原生扩展路径的有限修复写成 SQL ProjectSet 功能已实现。

## 本机证据

- 本项目录：`/tmp/dbms-r200-case-null.ra3V09Ie/`。
- 原生 API 原始观察：`completed-native-observation-receipt.json`，SHA256 `c71a1305caf0e84c2f80de2b9c163c7a15ac2085fa83e109df1a35e6cb68bf8d`；退出 0 仅表示采集完成。
- 完整旧红：`completed-baseline-receipt.json`，SHA256 `4201c7dc6e17424b427b05e74d272cd5fff1859d7ecd189d2c8a839f6c7f61ac`。
- 候选准备：`completed-prepared-receipt.json`，SHA256 `519b9c89373decb2744bd47df63b1d410203429198673164a58220d51a193d3e`。
- 私有 PG368：`/tmp/dbms-private-pg18-full-fixture.svs0ml93/completed-reference-receipt.json`，
  SHA256 `2db81ff02e52903c0517bf0a992399533a44f1446326460e375efdb82a5f8cad`。
- 初始 PG14 采集：`/tmp/dbms-private-pg18-full-fixture.l9ng081a/completed-reference-receipt.json`，
  SHA256 `d708e6bb3f3f21732a940a6b00fa10e75a4a8f4370ef58a9fb7df9585d3ffa73`；不是 14 项项目兼容通过声明。
- 两侧完整 70 入口及正常构建：`completed-candidate-receipt.json`，
  SHA256 `d1552bb9e6020bbec64ff33c7c418ef824acdae69e87005219f7bc3e8791e761`。
- 独立本地提交与提交后核对保存在 `completed-commit-receipt.json`、`completed-postcommit-receipt.json`。

这些临时路径不是可移植发布包；工作区通过不表示接受其他未提交候选。
明确暂缓的 COPY／R197、R185、R137、R146、默认协议 search_path 和其他模块不恢复。
原 273 项仍为 22 complete、166 partial、70 unverified、15 deferred；总目标未完成。
冻结 1492 的 1481 PASS / 11 FAIL 不改写。不 push，GitHub Actions 保持禁用。
