# R217：区分普通 case_when 函数与真正 CASE 的结果列名

## 已复现的命名错误与修改方法

普通 SQL 函数可以叫 `case_when`。它的默认列名必须来自函数标识符，不能由
函数拼写、参数个数或最后一个参数猜成 CASE。错误不只影响显示：CAST 会丢失
强函数名，CTE/derived table 按预期列名查找时也可能报 42703。

| 错误 | 原因 | 修改方法 |
| --- | --- | --- |
| 裸 `case_when(-2)` 的 Simple 结果叫 `case` | 公共 projectionLabel 把名字匹配到 case_when 的 FunctionCallExpr 当作 CASE。 | 普通 FunctionCallExpr 一律按 SQL 标识符解码；只有真正 CaseExpr 使用 ELSE 命名规则。 |
| CAST/CTE/derived 的函数名被改为类型名或最后一个参数名 | 同一错误把函数的强名字降成弱 `case`，或错误继承第 3 个参数的列名。 | 修正公共推导源头，保持强名字沿 CAST 传播并成为实际可绑定的派生字段。 |
| 真正 CASE 的 Extended 名称总是 `case` | 协议描述入口对 CaseExpr 另有硬编码，没有继承 ELSE 的强列名/函数名。 | 复用同一个纯 AST projectionLabel，既保留弱名 `case`，也保留强 ELSE 名字。 |
| 内部 legacy CASE 包装器可能受影响 | Native SelectExpr 的 case_when 是历史求值协议，和 SQL 函数 AST 不是一回事。 | 不改包装器的值、类型、分支求值和显式 displayName；保留原生默认 ELSE/NULL 回归。 |

对照 [PostgreSQL FigureColnameInternal](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_target.c)：
函数调用使用最后一个函数名标识符；真正 CaseExpr 从 ELSE 推导，弱名字回落到 `case`；
CAST 保留强名字。实现不通过执行函数、读取数据行或按结果文本推断名字。

## 原始证据与测试范围

本地完整记录：`/tmp/dbms-r217-case-function-label.83xZJ69v`。
父提交 `1db77b17eeaf9a26017c1501497bfa0194ef4d42` 的接受树和保留既有修改的工作区树
分别匹配各自 60 个生产对象、129 个头文件和编译器/选项。Root 与 pure 对象/头文件不混用。

| 完整用例 | 修复前 pure | 修复前 Root | 独立参考 |
| --- | --- | --- | --- |
| 原始未修改 28 项诊断 | 1 项失败、退出 1 | 1 项失败、退出 1 | 私有 PG18.6 全部通过。 |
| 原生 2092 项语法/声明/所有者/CTE/derived 检查 | 536 项失败、退出 1 | 536 项失败、退出 1 | 实际 AST 区分 FunctionCallExpr 与 CaseExpr；固定清单不允许失败。 |
| 独立命名空间的真实协议 1837 项 | 294 项失败、退出 1 | 294 项失败、退出 1 | 私有 PG18.6 全部通过、正常停止。 |
| 同一 schema 的重载组合 1808 项 | 1264 项失败、退出 1 | 1264 项失败、退出 1 | 私有 PG18.6 全部通过；另列 R222，不能声称本项修复了它。 |

1837 项包含裸/引号/限定/大小写调用，1/2/3 个参数，CAST、运算符、显式别名，
真实表、空表、WHERE false/LIMIT 0，CTE/derived 名称查找，重复 Bind、四种参数/结果格式。
另以私有序列核对 Describe、Bind/Describe 和零行不会执行 volatile 同名函数，
真正 CASE 的未选中分支不会执行；被需求的调用按实际行数执行。
所有结果、OID、宽度、typmod、格式、完整消息顺序及 binary 字节均有断言。

首轮公共推导修改后，原生 2092 项通过，但协议 1837 项仍失败 74 项：
逐消息核对全部实际值与类型均正确，错误仅为协议入口硬编码的 CASE 名称。
这些失败保留，随后补协议入口；未删除或放宽 1837 项断言。

最终构建、相关回归和独立本地提交尚待完成；此处不宣称验收。

## R222：同名函数重载持久化与解析尚未修复

最初完整 1808 项在创建同一 schema 的 2/3/0 参数同名函数时即报 42P07。
大量后续失败受这些 setup 失败影响，不能把它们说成 1264 个独立命名错误。
`src/commands/TableManage.cpp` 的 udfPath 只使用 schema/function.func，createUDF
只要该文件存在就拒绝 CREATE，没有以输入参数签名区分 routine 身份。
该实现目前与 PostgreSQL 的重载要求不一致；本轮没有修复或允许该错误。

最小复现（全新自有 schema 内）：

```sql
CREATE FUNCTION f(p integer) RETURNS integer LANGUAGE SQL AS $$SELECT p+1$$;
CREATE FUNCTION f(p integer,q integer) RETURNS integer LANGUAGE SQL AS $$SELECT p+q$$;
```

第二条应成功；当前两树返回 42P07。完整原始源码、全部 SQL/断言和失败记录保存在
未验收草稿 `tests/ordinary_case_function_overload_protocol_draft.py` 及本地 source snapshot，
没有把失败改成通过，也没有删除重载要求。1837 项命名用例采用独立 schema 的各参数版本，
用于不被重载创建失败遮住的命名、值、类型及需求次数检查；两组用途分开记录。

后续修改必须覆盖：以输入签名持久化 routine 身份并兼容旧文件；匹配实际参数声明的
查询/执行解析；CREATE/OR REPLACE、精确签名 DROP/ALTER、catalog OID 和事务回滚；
重启后读回、同参数个数不同类型、UNKNOWN/NULL、歧义和无匹配错误。
不能只放开“文件已存在”的检查并覆盖旧函数，不能仅按参数个数选一个函数。
用户已暂缓的 R173 类型依赖及其他模块不得借此自动恢复，相关未覆盖要求仍须如实保留。

原 273 项状态与冻结 1492 项结果不因专项结果改写；这不是整个函数系统或总清单完成。
R220 后续检查及全部用户暂缓项保持原状态，不 push，不启用 Actions，staged 18 路径不夹带。
