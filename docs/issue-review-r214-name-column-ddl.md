# R214：NAME / NAME[] 列声明与实际读写

状态：按用户本轮 `skip it` 暂缓。停止本项修复与验证，不计为已解决。

## 问题及修改方法

R212 的完整 257 项集成测试中，实际 `CREATE TABLE (... n name,a name[])`
报 42704，随后私有表不存在导致 69 条操作报 42P01。NAME 的表达式、函数和发送编码器
已有支持，但 `DdlExecutor::columnDefToColumn` 的手工内建类型分派遗漏 NAME。
不能用只检查类型注册表/函数的 `name_declaration_test.cpp` 代替建表验收。

计划：保留完整原 257 项；新增实际列声明、属性、约束、读写与类型身份测试；
先跑 PostgreSQL 18.6 及旧版，再只改列转换路径，验证后独立本地提交。

候选使用已有元数据声明解析器识别真正的内建 NAME，而不是把所有 `name` 字符串当成内建类型。
正确处理 `pg_catalog.name`、带引号的合法拼写和同名用户 domain 的搜索顺序；
内建身份不得被另一模式的 domain/enum 误捕获。
实际布局交给现有类型注册表：标量固定 64 字节、数组可变长，保留列名、NULL、默认值、CHECK 等属性。
NAME 不接受类型修饰符，必须在通用数值修饰符转换前以 42601 拒绝。
不改变注册表接口、物理格式、参数接收、search_path 默认值或 current_schema() 投影逻辑。

## 新验证及参考期望纠正

`tests/name_column_ddl_test.cpp.draft` 为 165 项原生检查。
覆盖七种合法拼写、真实持久化布局/OID19/OID1003、默认插入、NAME/数组实际行、
字面量 `NULL` 与 SQL NULL 位、CHECK/NOT NULL 拒绝且不留残行、修饰符及无效身份，
以及同名用户 domain、显式内建和内建优先时的列选择。
原生拒绝接口检验 Boolean/DbError 和数据库状态，不将 stdout 冒充结构化 SQLSTATE。

`tests/name_column_ddl_protocol_draft.py` 为 279 项完整协议检查。
检查七种拼写的真实 CREATE/INSERT/UPDATE/DEFAULT/约束/查询/删除，
标量及数组的值、空串、SQL NULL、OID、宽度、typmod、来源、格式、tag 和成功消息顺序；
84 条非法修饰符及无效类型身份严格核对 SQLSTATE，无数据或成功 tag。
错误只验类别，不声称所有 Parse/Bind/Execute 时机一致。

首轮参考 279 项有 3 条期望错误：在用户 domain `name` 先于 pg_catalog 的路径下，
测试将显式内建 NAME 列再转成裸 `name`，实际应解析成用户 integer domain，PG 正确报 22P02。
首版源码和完整失败保留。仅将这三处输出转换明确为 `pg_catalog.name`，保留建表、原值和全部 279 项。
改后同一完整 279 项及未经修改的原 257 项在新建 PostgreSQL 18.6 实例全部通过，
默认 fsync 不变，停止/服务退出均 0。

旧纯已接受代码与旧工作区分别完整执行：原生 165 项各 66 次失败；
新协议 279 项各 257 次失败；原集成 257 项各 70 次失败，实际退出均 1。
这些是重复路径/后续操作，不是对应数量的独立 bug；原日志不改写。

## 回归和版本边界

候选尚未验收，不能称 165/279/原 257 已在产品通过。
候选首个纯代码原生入口完整 165 项仍有 35 次失败，退出 1；
随后的诊断日志中完整新协议 279 项有 141 次失败、原协议 257 项有 65 次失败。
后两次诊断未生成独立终态 receipt，不能据此声称整套驱动验收。
失败集中于默认值、CHECK 和后续写入；字符串出现在持久化文件中，不足以证明语义正确。
本轮只撤回 R214 的五处 DDL 候选变换及一条新测试注册，保留修改前已有的其他工作区改动。
原候选可由本地 `owned-transformation.json` 和两侧原文件重建；测试与失败资料继续保留。
普通 `continue` 不恢复本项，须用户明确要求恢复。
已有 R212 发送修复不得回退。NAME 二进制输入、完整类型/依赖图、所有自定义类型与数组能力不由此关闭。
R210 默认值、R211 实际 current_schema() 值和 R213 引号列名仍为普通待修；用户已跳过项不恢复。
不得提交工作区的其他未接受候选或 staged 18 路径；不 push，不启用 Actions。
原 273 项及冻结原 1,492 结果保持原始记录。

## 本地记录

目录 `/tmp/dbms-r214-name-ddl.cdiFls72` 为本地诊断资料，不是发布依赖。

| 记录 | SHA256 |
| --- | --- |
| prepared receipt | `9a18c3668ac03ad5d0123075403205e9dca3ca0877d7d1e8bd8a67f453b7bfbe` |
| baseline receipt | `775bd263301967068d138d97c2ee68c0c40420ed32e7bbb11a3b472ea47b6479` |
| PG V1 完整 279 失败、原 257 通过 receipt | `/tmp/dbms-private-pg18-full-fixture._aml7a1r/completed-reference-receipt.json`，`50e203104901732f49605f28f852d2a9d29f36169ed4aeed69f3d07cebab833b` |
| 保留 V1 279 夹具 | `v1-name_column_ddl_protocol_e2e_test.py`，`2f29263aedf27d31c045df196d3032cb66201b091f38efcc05b4a4d9cfeff057` |
| PG 最终完整 279 +原 257 receipt | `/tmp/dbms-private-pg18-full-fixture.cm1sbctt/completed-reference-receipt.json`，`dfbd6e8c6bb2d82710aa6f63994e7e375c4f22d0e862ac72bbee6b83d5cc3bc6` |

暂缓夹具已按原字节保留为上述草稿名，避免被默认原生入口扫描或独立 E2E 注册规则误纳入。
这只是暂缓现场整理，不代表恢复 R214 修复或通过其验收；既有失败和参考记录不改写。
