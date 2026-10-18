# R151：枚举自动数组目录与尚未通过的协议门禁

状态：用户于 2026-10-18 要求“skip it”，本项暂缓，停止实现与验收。
完整 103 项协议仍有 60 项失败；341 项原生通过不能代替协议验收。
现有候选源码、两个夹具及注册原样保留，未提交、未计已修复。
普通继续不恢复本项；恢复需用户明确要求。独立 REGTYPE 显示问题另行处理，
不以依赖修复重开本项，也不把仍开放的 R150 阈值重载标为完成。

## 已复现的问题

~~~sql
CREATE SCHEMA enum_array_review;
CREATE TYPE enum_array_review.rank AS ENUM ('10','2','1');
SELECT NULL::enum_array_review.rank[];
~~~

PostgreSQL 能解析真实数组身份，原程序报 42704：array type for declared type does not exist。
这不是 WIDTH_BUCKET 独有的问题：typeof、CAST、Describe、Bind 和客户端数组声明都会受影响。

[声明类型解析](../src/catalog/declared_type.h)正确要求标量的 typarray 指向实际数组行；
原[目录创建](../src/catalog/catalog.cpp)和[枚举 DDL](../src/commands/DdlExecutor.cpp)
只登记标量枚举，没有生成这个配套身份。不能用任意数字 OID 或把数组解释成 TEXT 绕过。

参考依据为 [CREATE TYPE](https://www.postgresql.org/docs/18/sql-createtype.html) 及
本机 PG 18.6 pg_type.c 的 makeArrayTypeName/moveArrayTypeName。
特别是 PG 18 的冲突策略为数字后缀，不是不断重复前导下划线。

| 要求 | 正确行为 |
| --- | --- |
| 标量/数组身份 | 两个稳定且不同的 OID；typarray/typelem 双向一致 |
| 数组描述 | category A、base kind、变长长度、正确 namespace、初始 owner、alignment/storage 和 array codec 身份 |
| 命名 | 前导下划线、UTF-8 完整边界、最多 63 字节；冲突时追加 _1、_2 等 |
| 显式类型占用旧自动名 | 重命名原自动数组但保持 OID，不能覆盖或拒绝合法的新枚举 |
| 依赖 | 数组到元素为 internal，到 namespace 为 normal；标签仍只属于枚举标量 |
| 生命周期 | DROP 元素自动带走数组；独立 DROP 自动数组要 2BP01，CASCADE 也不绕过 |
| 持久化 | 冷加载、只读元数据和后续发布保持身份；只读读取不能编造新 OID |
| 协议 | NULL、零行和参数依然使用真实数组 OID，不得冒充 TEXT |

## 当前候选做了什么

候选只修改私有 Catalog/Ddl CPP、增加一条协议注册和两个夹具：
自动数组生成、碰撞改名及稳定 OID、目录依赖、配对删除、独立删除保护、
普通可写加载/enum promotion 的缺失配对修复。只读快照不会分配身份。
没有改 Main、共享头、schema/heap 格式、权限检查或任何已跳过专项。

尚未提交的永久夹具路径为 tests/enum_array_catalog_test.cpp、
tests/enum_array_catalog_protocol_e2e_test.py。它们的完整断言必须保留：

- 原生 341 项涵盖布局、函数身份、双向关联、命名/UTF-8、命名冲突、
  另一 namespace 的同名类型、标签、冷加载、旧缺失行修复、只读快照和删除计划。
- 协议 103 项涵盖真实 schema/enum、生成名、稳定 OID、最小引用名称、
  Statement/Portal Describe、文本/binary NULL、真实 OID Bind、零行、错误恢复和 DROP。

## 实际证据，不能合并成“已通过”

| 输入 | 完整控制 | 结果 |
| --- | --- | --- |
| PG 18.6 正式协议夹具 | 103 | 0 失败 |
| 冻结旧 Root | 103 | 86 失败，退出 1 |
| 已接受 R149 clean server | 103 | 86 失败，退出 1 |
| 旧原生目录实现 | 341 | 280 失败，退出 1 |
| 当前 Root 原生候选 | 341 | 0 失败 |
| 当前 accepted-base clean 原生候选 | 341 | 0 失败 |
| 当前 Root 完整协议候选 | 103 | 60 失败，退出 1 |
| 当前 clean 完整协议候选 | 103 | 60 失败，退出 1 |

第一版 native 只是 DropBehavior 作用域拼写错误、编译退出 1，未冒充运行结果。
第一版 PG 夹具实际 103 项全匹配，但常数误写 99、退出 1；
仅将计数修正为 103 后才得到正式参考结果。原脚本/日志保留。

当前生产正常编译两个 CPP、O2、repeat0，全部 58 个 receipt 与当前输入匹配。
clean 为两个 fresh O2 CPP，加 55 个 R148 fresh58 生产对象和当前 R149 evaluator，
核对全部头/flags、未改 CPP、原 receipt 及对象字节，server 无 stubs、无伪造 cache stamp。
当前没有声称重编或重跑 Main，没有当前完整回归/整库 sanitizer 证明。
已构建的 clean/donor 已跟踪文件保持永久冻结。

## 60 项失败的三个根因与未来恢复方法（本项已暂缓）

1. [regtype 呈现](../src/expression/regtype_input.h)总是给用户类型及 schema 加双引号，
   例如输出 “"schema"."plain"[]”，而 PG 输出 “schema.plain[]”。
   应保留总是加引号的查找串用于精确身份判断，
   另外按 PG 的 ASCII/关键字规则生成最小引用的输出；复用统一 quote_identifier 规则，
   不对任意数据字符串做引号删除。搜索路径可见性、保留字、特殊名和数组必须一起验证。
2. [协议列描述](../src/network/NetworkServer.cpp)将非 builtin 的已声明类型回退为 OID25；
   已有 query binder 其实持有目录 OID。应让真实 prepared output/目录声明成为类型来源，
   保留 enum/array 的 OID、长度和 typmod，并保持 domain 的既有 wire-base 语义。
   不能执行表达式取样，也不能只在非 NULL 数据上修补。
3. 63 字节 ASCII 名和 63 字节 UTF-8 名的 schema-qualified CREATE TYPE 仍失败，
   而两条参考创建均成功。[枚举存储校验](../src/commands/TableManage.cpp)
   将完整限定名交给只允许 64 字节的通用 metadata object name 校验。
   应分别处理 SQL schema/类型标识符边界与存储中的限定逻辑名；
   保留相关安全/元数据约束，不通过删除长名称测试或整体放宽共享路径校验绕过。

未来明确恢复本项时，这些修复应各自独立复现、验证和版本管理，
再重跑原封不动的 341/103 全部控制；当前不继续本项门禁或扩展候选。
若共享头或 Main 改动，仍需正常 fresh58 O2 与三个 fresh 真实 Main TU；
仅私有 CPP 改动也必须核对 Main/头/flags/对象来源并新链接当前 57 个库、实际隔离执行。
只有完整协议、相邻完整入口、独立 clean 与输入归属复核全过，才能验收本候选。

[R150 阈值重载](issue-review-r150-threshold-width-bucket.md)仍开放，61 条观察没有删掉任何类型。
即便 R151 最终完成，仍不能将阈值函数或全部 TYPE-08/CAT-04/FUNC-01 标为完成。
当前仍无新 1454 全注册/TLS/整库 sanitizer；原 273 的 22/166/70/15 不变。
本次仅提交用户暂缓记录，不提交这个仍未通过协议的生产候选。
跳过前 source manifest 2181 项的 SHA256 为
2edc8a53249770a533f3ac0c41bf07cbb0e2f28979d95f95046553b897c69c9a；
工作区 29 个 tracked 修改、17 个 untracked 项及其内容均保留，索引不纳入候选。
本机 /tmp/dbms-review-r46.ifu6SWLp/r151-user-skip-* 保留跳过前差异、
状态、逐文件源码 SHA256 及两夹具归档；原 273 项及其状态未改，不 push、Actions 禁用。
