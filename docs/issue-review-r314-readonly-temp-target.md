# R314：只读事务的临时表写入与显式 TEMP 查找顺序

状态：本项有限事务/目标解析修复已验收；独立 R315 尚未修复，原始总清单未完成。

对应原始 `TXN-02`、`SQL-05`、`CAT-13` 的有限子集，不宣称完整隔离级别、
search_path、catalog 或临时对象生命周期已经闭合。序列会话状态 R312、
TEMP VIEW R310、角色默认 search_path R210、通用 catalog/结果载体等既有
暂缓范围不恢复；本项不修改 ACL、角色或 catalog 存储格式。

## 实际问题与修复方法

| 问题 | 旧产品 | PostgreSQL 18.6 | 方法 |
| --- | --- | --- | --- |
| 只读事务写入现有 `pg_temp.t` | INSERT/UPDATE/MERGE 被误拒绝为 `25006`；原生 DML bridge 的 DELETE 也误拒绝 | 临时表允许写入 | 保留 AST 的原始限定名称，比较实际解析出的物理目标与本会话临时对象身份；不拿限定字符串直接查未限定名字的 map |
| quoted 临时表名称 | `"pg_temp"."R314.Dot"` 等同样误拒绝 | 正常写入 | 使用现有 qualified-name 解码器，保留引号内大小写及字面点号 |
| 显式 `search_path=public,pg_temp` | 同名 TEMP 总被提前选中，只读写入错误成功 | public 在前，应拒绝永久表写入 | legacy DML 解析器和 prepared relation metadata 都只在未显式列出 TEMP 时插入隐式 TEMP；显式 TEMP 在指定位置参与查找 |

只读判断复用实际写入方的目标解析，不扫描任意前缀、不根据消息字符串猜测，
不把“存在一个同名 TEMP”当作目标一定是 TEMP。保留 native transient table
现有登记契约；独立的权限判断函数未改。普通 public 表仍拒绝写入，读写事务、
临时表数据回滚、只读模式维持和回滚复位均在专项中验证。

`pg_catalog` 的其他优先级规则、完整普通 SELECT legacy 路由、其他会话的
临时 namespace、角色默认配置和完整物理 catalog 身份不是本项完成声明。

## 证据与冻结输入

证据目录：`/tmp/dbms-r314-readonly-temp-target.z04LzlSq/`。
`completed-observations-receipt.json` 保存 PostgreSQL 18.6、Root 隔离投影、
纯接受副本各 284 项观察及全部原始报文。旧产品各有 42 项结果/SQLSTATE/
事务状态相关差异；另外五个 Simple DELETE 的额外空 RowDescription 属于
独立 R315，不删除这些请求或断言。

永久原生专项 124 项，旧两侧各失败 32 项；最终协议专项 284 项，参考端
全部通过，旧两侧各失败 47 项。最初 fixture 将 RowDescription 字段名 bytes
与 str 比较，且在旧产品缺少描述报文时直接中止；已在生产修改前纠正两处
读取/校验方式，保留所有 SQL、预期值、状态与报文规则，初版及日志另存。
最终冻结记录为 `completed-final-fixtures-frozen-receipt.json`。

覆盖未限定/限定/quoted TEMP、quoted 字面点号表名、同名 public/TEMP、
INSERT/UPDATE/DELETE/MERGE、读写前置设置、只读写入后的可见行、失败事务
状态、回滚后数据、Simple 与 Extended 的空 Parse/Bind 完成报文及精确顺序。
原生检查另覆盖显式实际临时 namespace、prepared source identity 和每次操作
后的只读模式。DataRow 内容、事务就绪状态、SQLSTATE、命令标签和所声明的
元数据字段精确验证。RESET 的角色相关默认值仍是已暂缓 R210；只校验该
ParameterStatus 的名称/格式，不将项目默认 `public` 冒充 PostgreSQL 默认值。

候选初验原生 124 两侧均通过；再执行同一 284 项原始观察，两侧的数据、
SQLSTATE 和 ReadyForQuery 与参考端一致。严格协议专项各剩五项 R315，
均为 Simple DELETE 的多余零列描述，不能写成 284 全通过。

## 验收状态

当前普通矩阵 152 条执行记录：140 条退出 0、12 条非零；scoped UBSAN
36 条：28 条退出 0、8 条非零，无 UBSAN 诊断。合计 188 条有限执行记录，
168 条退出 0、20 条非零，不是整库全绿。全部上一轮 154 条运行输入保留，
并增加完整事务模式/隔离/保存点/chain 回归；原有脚本和默认超时未改。

其中 16 条非零来自既有用户暂缓的 numeric 边界、prepared planning、TEMP VIEW、
DO 等行为，失败身份逐条比对；另外四条是本次普通/SAN、两侧 284 专项的
各五个 R315 报文失败。R315 未修复、未暂缓，不能归为用户允许忽略的差异。
原生 124 两侧在普通和 scoped UBSAN 均通过；同一冻结 284 的数据/状态/
标签/类型声明/ReadyForQuery 已与实际 PostgreSQL 参考逐项相符，唯独上述
独立多发报文仍保留严格失败。相同 DELETE 文本在 SET 前后有不同含义，
比较失败身份时同时保留预期状态、标签和事务就绪状态，不仅按 SQL 文本归类。

实际构建：四个生产单元全新编译（TableManage / DmlExecutor 各两侧），
118 个生产对象和两个 stub 经 source/header/MMD/compiler/flags 匹配后复用。
新增及事务原生测试共十个单元全新编译，六十个旧原生和六个真实 Main
测试对象匹配复用；共 76 个测试程序、两个服务器重新链接。头文件和 Main
均未修改，不宣称本轮 122 个生产单元或六个 Main 测试全新编译。

scoped UBSAN 新编译上述两个生产文件和新原生专项各两份，配合三十个
匹配的既有插桩对象，重新链接二十个原生程序和两个服务器。所有本项修改
生产文件均插桩，其他匹配的 parser/diagnostic/network/evaluator 继续保留，
Main 和其他库仍为普通构建；不宣称全库插桩或完整全量测试通过。

当前实际记录：`completed-runtime-build-receipt.json`、
`completed-regression-build-receipt.json`、`completed-candidate-receipt.json`、
`completed-scoped-ubsan-receipt.json`、
`completed-component-verification-receipt.json` 和
`completed-classified-candidate-receipt.json`。历史 1492 基线和失败不覆盖。
版本/文档/兼容契约/总账/注册静态检查在 Root 和纯副本分别验证；
`--require-complete` 仍必须失败，273 项原始总账的完成数保持不变。

仅提交本项隔离验证差异和 R315 的独立问题记录；保留原暂存区 18 项变更
及所有无关工作区内容。不 push，不启用 Actions；R315 是下一项优先工作。
