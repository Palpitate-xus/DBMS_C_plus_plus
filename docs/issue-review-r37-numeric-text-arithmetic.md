# R37：显式字符类型不得隐式参加数值算术

## 问题与边界

`SELECT '1e-40'::text+1e-40` 原本被接受；NULL 输入、空表、LIMIT 0 和
未执行的 CASE 分支也未在分析时拒绝不存在的运算符。PostgreSQL 18.6 对这些
显式 TEXT/数值组合返回 `42883`。错误不仅影响加法，同一接纳路径还影响
减、乘、除、余和乘方。VARCHAR 也不能因其内容像数字而获得隐式数值运算。

未定类型的 SQL 字符串是不同的情况：`'2'+1::numeric`、`'2'+1` 仍合法，
显式 `'2'::text::numeric+1` 也合法。INTEGER/BIGINT/NUMERIC 混算和已有浮点
提升规则应保留，不把此项扩大成所有 PostgreSQL 运算符/用户类型的完整解析器。

## 根因与实施记录

共享 `arithmetic_detail::resultType()` 对已知数值与显式字符类型组合返回空，
调用者随后落入宽松的类型合并或值转换。初次尝试只在 whole-query binder
拒绝，native 60 控制已全过，但协议 109 控制仍有 77 失败：旧执行/Describe
路径不一定经过该 binder。这份中间失败证据保留，未提交为完成的修复。

统一判断现已在共享算术类型入口完成：只看类型声明，真正 UNKNOWN 输入可以
继承数值对端类型；已声明的 TEXT/字符类型不能这样转换。NULL/空行/表达式
是否被执行不应改变静态运算符的合法性。旧求值器的 UNKNOWN 字符串标记需
保留，由 metadata 路径区分显式 VARCHAR；不能通过运行用户函数猜类型。

旧标量路径的 `prepareArrayTypes()` 在实际求值前调用相同算术类型规则，
避免先运行 nextval 再发现运算符不合法。协议 Parse 新增有限的实际算术 AST
检查入口：普通无源/单表 SELECT、VALUES INSERT、普通 UPDATE 复用完整
metadata binder，参数保留真实声明 OID 的 MetadataPlaceholder，不将参数
改写成 NULL SQL；在发布 prepared statement 前完成签名检查。复杂查询图
继续由既有准备入口负责，没有改变行计划、窗口、事务或网络取消机制。

## 验证记录

初始基线为 `08113d71`。所有下述候选检查均已取得实际终态：

| 已取得记录 | 结果 |
| --- | --- |
| PostgreSQL 18.6，新协议 109 控制 | 全通过，exit 0 |
| 初始基线，新协议 109 控制 | 81 失败，exit 1 |
| 初始基线，native 60 控制 | 38 失败，exit 134 |
| 仅 binder 中间候选 | native 通过，但协议仍 77 失败 |
| 仅共享类型头中间候选 | 协议仍 42 失败，含错误阶段和 nextval 副作用 |
| 最终候选，native 60 控制 | 全通过，exit 0 |
| 最终候选，协议 109 控制 | 全通过，exit 0 |

新增测试覆盖六种二元算术、双方向字符/数值、VARCHAR、NULL、合法 UNKNOWN
和显式 numeric cast、整数/浮点混用、空表、死 CASE 分支、LIMIT 0、写入绑定、
参数 Parse OID、错误 Parse 不先发送 ParseComplete，以及元数据不调用 nextval。
协议驱动已注册，原生由既有发现机制纳入。共享协议断言增加了 message kind
日志，保留既有 SQL、状态和值/OID断言，以便复查失败阶段。

七个原有 native 邻居完整通过：numeric_gcd_lcm_type、
scientific_numeric_arithmetic、scientific_numeric_literal、
arithmetic_result_type_inference、prepared_primitive_assignment_input、
numeric_column_modifier、case_integer_literal_binding（后缀 `_test.cpp`）。
八个协议邻居完整通过：numeric_gcd_lcm_type、scientific_numeric_arithmetic、
scientific_numeric_literal、arithmetic_result_type、protocol_parameter_type_identity、
prepared_primitive_assignment、case_integer_literal_binding、numeric_column_modifier
（后缀 `_protocol_e2e_test.py`）。

完整 `postgres_protocol_test.py` 首次并行运行在取消测试后的
`CREATE TABLE cancel_wait` 超时，记录 exit 1；未改断言、未增加原 10 秒 socket
timeout、未修改取消模块，同一冻结候选串行重跑完整驱动 exit 0。
这不是宣称修复了该非算术超时；两次记录均保留。

证据目录：`/tmp/dbms-review-r37-evidence.O9XtA5Rz/`。因修改公共类型头文件，
候选已真实重编全部 58 个生产翻译单元并 exit 0；随后真实重编新增检查的
NetworkServer.cpp 和 expr_helper.cpp 并重新链接。不把旧头文件对象重新标记
为新对象；所有 native 链接前逐对象核对当前源码与编译签名。

最终冻结 `dbms_main.r37-final.frozen` SHA-256：
`54b7d530e046743b4e2e81305941a574c438fe890b2024a564321bfe4518908c`。
最终协议为 `protocol-final.log`、`protocol-pg18-final.log`，原生及邻居为
`native-final-neighbors.log`、`final-*_protocol_e2e_test.log`，完整协议重跑为
`postgres_protocol_final-retry.log`。中间候选失败日志和冻结文件未覆盖。
