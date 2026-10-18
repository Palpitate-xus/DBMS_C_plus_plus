# R156：REGTYPE 计划常量丢失身份及内置类型呈现（开放）

状态：已复现、未修复。不能以 R152 的最小引用修复关闭本项。
本项使用已有内置数组身份，不恢复用户暂缓的 R151 自动枚举数组。

## 实际问题

~~~sql
CREATE SCHEMA ref_shadow;
CREATE TYPE ref_shadow.text AS ENUM ('z','a');
SET search_path TO ref_shadow,pg_catalog;
SELECT (25::oid::regtype::oid = 25::oid);
~~~

PG 返回 t；冻结的 R155 accepted-only server 与 R152 accepted-only clean 均返回 f。
显式的物理 OID 不应因搜索路径中的同名类型而变成另一个 OID；
这不是单纯的多余引号问题。

| 已复现路径 | PG 18.6 | R152 clean | 修改方向 |
| --- | --- | --- | --- |
| OID25 / OID18 → REGTYPE → OID | 与原 OID 相等 | f | 计划常量保留物理身份，不能从显示名重新 lookup |
| OID1009 的相同身份往返 | t | 42704 | 保留真实数组身份及元素身份，不借 R151 目录候选绕过 |
| 隐藏的内置 text/name/char 呈现 | pg_catalog 限定 | text/name 或仅引用 char | 按真实 OID 与可见性生成规范名称 |
| 内置 text[] / char[] → text | 有限名加数组后缀 | 42704 | 不把内置数组呈现重新绑定到同名用户枚举 |
| OID1083 / 1114 呈现 | time/timestamp without time zone | time/timestamp | 补全 PG 的标准类型特殊输出规则 |
| json 关键字 → REGTYPE OID | 114 | 同名用户枚举，比较为 f | 声明语法别名固定真实 builtin；保留引用后的用户 json 与可遮蔽 JSONB |

## 代码证据与修改方法

[计划常量](../src/expression/prepared_query_execution.cpp)的 constantExpression
只复制 typeName 与引用后的显示 value；objectOid、elementOids、recordFields
仅影响 primitive 判断，没有作为新 Literal 的物理数据保留。
[evalLiteral](../src/expression/ExprEvaluator.cpp)随后把非 primitive 的 typed literal
重新从 UNKNOWN 文本执行输入转换；evalCast 在物理 OID 存在时本来能够保留它。
这条代码链与实际身份偏移一致，不能只给字符串加引号来掩盖数据载体丢失。

修复应使用不可变、query-owned 的完整 typed 常量数据，携带 OID、元素 OID、
record fields、NULL 与 collation；审查全部 Literal clone、规划/求值/旧适配器入口，
不能为取类型执行 SQL，也不能改变既有 lazy/opaque callback 归属。
若改 AST/共享头，必须重新执行正常 fresh58 O2 和真实 Main fresh TU 门禁。

[REGTYPE 输出](../src/expression/regtype_input.h)的 builtin 分支还缺少规范时间别名及
非特殊 builtin 的搜索路径可见性；应对照 PG format_type.c 的真实 OID 特殊规则，
不能对所有 builtin 统一加引号或统一强制裸名。
[声明别名](../src/catalog/declared_type.h)缺少 JSON 关键字的 builtin 语法身份；
这是另一处 owner，修复时要保留显式引用用户类型和 JSONB 的正常遮蔽语义。

## 观察与版本边界

实际 PG 18.6、冻结 R155 server、R152 clean 各完成同一 34 条观察；
R152 clean 与参考有 14 条语义差异，不是 14 个独立根因，也不是 34 项通过。
int4/numeric/interval/bit 和隐式 pg_catalog 在前时的身份控制保留，不能只留下失败样本。
所有 schema/type 属于独立私有前缀，创建与清理均成功。

首版 28 条用未限定的 ::text，被同名 enum text 自身遮蔽，双方均有合法 42846；
错误消息中的随机 schema 差异不计产品错误。
第二版只把目标改为 ::pg_catalog.text，保留 28 条，观察 11 条语义差异；
第三版增加六个物理身份控制，34 条中有 14 条差异。原版本/日志全部保留。
本轮还对新的 R152 clean 实测相同 34 条及 14 条差异，没有从旧结果推算。

脚本/原始日志位于本机 /tmp/dbms-review-r46.ifu6SWLp/r156-*。
本项没有生产修复、永久注册夹具或验收；仍需完整 native/协议、参数/NULL/零行、
数组/record、搜索路径变化及规划边界验证，不能缩成只修 bare text 显示。
