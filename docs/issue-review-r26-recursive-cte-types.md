# R26：递归 CTE 的静态类型与 typmod 声明

## 问题

原 binder 和 Main 递归物化路径只比较列数。例如：

```sql
WITH RECURSIVE q(n) AS (
  SELECT 1 UNION ALL SELECT (n+1)::bigint FROM q WHERE n<2
) SELECT n FROM q;
```

原程序返回 1、2，OID 仍为 integer；PostgreSQL 18.6 在分析阶段报 42804。
varchar 长度、numeric 精度/scale 等同样没有检查，空递归项也错误通过。
原 wire Parse 没有进入 CTE 的整体 binder；错误 seed 中的 nextval 还可能先执行。

## 修改

- binder 从非递归项获得 seed descriptor；根据绑定来源的 CTE 身份判断是否真实
  自引用，不把所有 WITH RECURSIVE 下的 UNION 都当成递归。非自引用 UNION 仍按
  普通共同类型处理，内层同名 CTE 不误认外层递归来源。
- 共同输出的类型、OID 和 typmod 必须与 seed 相同；允许较窄 RHS 隐式提升到
  seed 类型。相同 domain 声明保留 domain，不同候选才解析真实 base type。
- 保留 CAST、物理列、derived/scalar 子查询、CASE、COALESCE/GREATEST/LEAST 和
  VALUES 的静态 modifier。集合的 modifier 仅在全部输入同共同类型且同 modifier
  时保留。VARCHAR 声明不错误套用存储分配上限；时间精度按 PostgreSQL 上限归一。
- 算术表达式使用已有共享 `arithmetic_detail::resultType`，不再把左操作数类型
  当作算术结果类型，否则 smallint seed 的 `n+1` 会漏掉 integer 提升。
- Main 在递归 CTE 物化前调用同一纯 binder；协议 Parse/Describe 对顶层递归
  WITH 声明调用同一入口，并使用真正的 metadata-only 参数槽，不替换为 NULL SQL。
  没有运行 seed、递归项或用户例程来发现类型。
- 内部 domain 身份与 wire descriptor 分开：内部保留相同 domain，RowDescription
  按 PG 规则返回其基础 OID、宽度和 modifier。自定义类型从真实 catalog namespace
  得到限定名，名为 text 的 enum 不会被误认成 builtin TEXT。

本项不重写递归物化算法，不补齐所有 SQL PREPARE 模板的分析阶段，也不宣称所有
嵌套查询形状都已获得新的协议描述入口。现有 SQL PREPARE 模板发布逻辑另属其接口
的后续工作；其执行到 binder/Main 时应用本项检查。

## 可重复证据

新增注册驱动 `recursive_cte_type_declaration_protocol_e2e_test.py`：

| 控制 | 数量 | 检查内容 |
| --- | ---: | --- |
| 错误声明 | 32 | Simple/Parse 的类型、长度、精度、空 RHS、物理列、typed 参数、domain 和同名 enum |
| 合法执行 | 10 | 普通递归、较窄 RHS、相同 modifier、CASE/COALESCE、NULL、非自引用 UNION |
| Parse/Describe | 7 | 不执行、OID/typmod、typed 参数、domain 去壳、enum 身份、时间精度归一 |
| fixture 与无副作用 | 8 | 7 个真实建表/类型控制，以及 currval 必须为未定义 55000 |

共 57 项，PostgreSQL 18.6 全部通过。使用连接内 TEMP 表/sequence 和唯一随机 schema，
finally 删除本次 schema；不修改参考库的已有对象。

R31 冻结基准 SHA-256：
`c86a681d65fdb01f3ccdb01a297eb6b91f8321f3f4fbff8efa9b0ac3ef267b28`。
相同最终驱动 17 项通过、40 项失败，exit 1：32 个拒绝项没有得到正确的 42804，
7 个 Describe 未返回所需 descriptor，currval 已为 1。部分非法 bit/time 声明原来
返回 XX000，而不是成功返回行；这些差异不混称为 40 个独立缺陷。

候选真实标准构建后 SHA-256：
`a55ca1e4c3e3a22bd5e1fbf1dcbac45f13b845c3464475c24ac6d95534d465ac`。
最终 57 项全部通过，exit 0。

新增 `recursive_cte_type_declaration_test.cpp` 的 23 项只使用 metadata callbacks，
验证 scalar/derived 传递、嵌套 CTE 遮蔽、带括号 seed、modifier、UNKNOWN seed 及相同
domain 的内部 OID。native domain OID 90001 是该 API 测试自己的类型描述 fixture，
不是参考库 OID。与原 query_binding、set_operation_binding、case_common_type、
values_common_type_binding、arithmetic_result_type_inference 共 6 个 native 全部通过。

最终候选还通过原完整 `postgres_protocol_test.py`，以及 10 个未缩减邻近协议驱动：
cte_prepared_owner、cte_relation_scope、case_common_type、values_common_type_binding、
arithmetic_result_type、explain_typed_execution、protocol_parameter_type_identity、
enum_comparison_binding、domain_ancestry、set_operation_structured。原输出是本目录的
`final-*.log`。本项没有运行整个项目的全量套件。

## 中间失败保留

日志目录 `/tmp/dbms-review-r26-evidence.F4aJdla1/` 保留全部版本：

- 第一候选的 36 项中，Simple 已改正 11/12 错误声明；12 个 Parse 和 smallint
  算术声明仍失败。该输出促成协议入口和共享算术结果类型修复。
- 第二候选原 46 项全过；追加 domain 正向控制后，确证 base seed/domain RHS
  被误拒绝，随后修复共同类型的 domain 规则。
- 第三候选的 51 项仅同 domain 的 wire OID 仍错误，随后补齐内部/wire 区分。
- 最初 domain fixture 使用 pg_temp schema alias，当前 DDL 返回 3F000；本项未
  扩展该 DDL 支持，改用双方支持的真实随机 schema，原日志仍在。
- `protocol-pg18-all.log` 曾把 domain OID 当成 PG wire 预期而失败；真实 PG 输出
  为基础 OID 23。修正预期后，内部 domain 保留由独立 native 断言负责。

最终 PG、基准、候选日志分别为 `protocol-pg18-accepted.log`、
`protocol-baseline-accepted.log`、`protocol-final-candidate.log`。
