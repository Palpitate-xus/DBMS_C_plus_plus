# R46：原生与 SQL schema 操作必须共享真实目录及事务 owner

## 已复现的问题

主分支 `f4087f5d` 的原生 `createSchema/dropSchema` 只处理物理 marker 和名称
前缀，没有同步 `pg_namespace`、关系依赖及辅助对象。`schemaExists(public)`
直接返回真，导致已提交的 DROP 后仍能通过原生存在性检查。已加载目录还会
继续接受被删除命名空间中的 DOMAIN/function 声明。仅按 `schema__` 前缀删除
也不能区分该 schema 的表与 public 中恰好包含此前缀的引号名称。

独立工作区保留的旧探针在当前主分支真实对象上重新编译、链接；原 namespace
探针有 18 条失败断言，不计作 18 个独立缺陷。新的 46 条完整协议控制在实际
PostgreSQL 18.6（180006）全部通过；主分支运行到第 20 条 CASCADE 时返回
XX000 并终止，不能称为已执行完 46 条。

其他原生基线还复现了普通非数据库目录被自动创建 catalog、public marker
bootstrap、不完整 domain 依赖清理和 decoded-dot routine 身份混淆。reserved
cluster bootstrap 旧夹具调用了主分支尚不存在的 `transactionDatabase()`，
其编译失败只是夹具 API 不匹配，不冒充产品失败。

## 修复方法

1. SQL 与原生 CREATE/DROP SCHEMA 共用 `SchemaNamespace` 入口。调用方提供
   实际 engine/database；原生调用不伪造 SQL 管理员，不替换环境 session。
   SQL 保留原权限检查和真实 session，物理 marker 原语仅供 owner/undo 使用。
2. 以 namespace OID 和真实关系目录规划 RESTRICT/CASCADE。低层 storage-only
   表保持不自动导入目录的既有合约；已登记关系优先使用其 canonical namespace。
   辅助函数/过程分别使用 namespace/name，不将 decoded 名称中的点重新切分。
3. 删除真实域及其派生域的外部依赖列，而非删除整个外部表。按真实 ordinal
   从后向前执行列变更，保留 NULL、空串、宽整数及其余列；同步依赖/目录信息。
4. 使用同一个实际数据库事务、statement image 和保存点恢复。只读事务拒绝
   写入，不能把另一数据库的操作混入已有事务；每个持久化结果都必须检查。
5. 目录服务以受数据库锁保护的 generation lease 代替 retired model 析构发布。
   干净 peer 修改更换 generation；未发布修改冲突必须报错，不能覆盖或丢弃。
   外层事务中的文件发布不是 COMMIT：同引擎 reader 也必须保留父事务开始前的
   committed generation，保存点物理恢复不得丢失它。
6. public 由真实 marker/目录状态决定。只为首次实际 namespace bootstrap 创建
   checked regular marker，不能恢复一个已提交的 DROP；占用/异常路径报 I/O。
   普通 catalog lookup 不创建不存在的数据库，reserved `info` scaffolding 仍由
   其现有明确入口创建，不增加用户、权限或认证功能。

## 验证记录

证据目录：`/tmp/dbms-review-r46.ifu6SWLp/`。

| 输入 / 门禁 | 实际结果 |
| --- | --- |
| 主分支原 namespace 探针 | 18 条断言失败；日志在全注册目录的 legacy-schema-namespace-probe.log |
| PostgreSQL 18.6 完整协议控制 | 46 条全部通过，postgres-reference46.log |
| 主分支同一协议输入 | 第 20 条 CASCADE 返回 XX000，root-original46-protocol.log |
| 第一候选 fresh58 | 58 个生产单元实际编译并核对 receipts/stamp，exit 0 |
| 第一候选原生批次 | 13 个完整驱动中 12 通过；同引擎 reader 观察到未提交 namespace，first-native.log |
| 第一候选完整协议 | 46 条通过，first-protocol.log |
| 第二候选 fresh58 | 公共头变化后再次实际编译 58 个生产单元，exit 0 |
| 第二候选 | cold mutable checkout 的初始化检查过严，新增原生在 setup exit 134；未提交为修复完成 |
| 第三候选 | 正式重编两个变化 CPP、46 协议全过；强化保存点 reader 控制仍报 55P03，third-same-engine-probe.log |
| 第四候选正式构建 | 两个变化 CPP 实际 O2 重编并链接，exit 0；未借用其他工作树对象 |
| 第四候选完整新协议 | 46 条全部通过，fourth-final-protocol.log |
| 第四候选最终原生组合 | 35 个不同完整驱动通过；三处旧夹具前提及一次 optional 断言编译错误均保留，下述更正未修改生产输入 |
| 完整协议邻居 | 最终十个完整驱动全0；首次 postgres_protocol_test 在 role_rows 为空处 exit 1，原日志保留；未改输入/断言/超时的独立完整重跑 exit 0，不宣称确定了历史原因 |

原生覆盖 empty/public/nonpublic namespace、原生/SQL/cold lookup、DOMAIN/UDF
声明拒绝、RESTRICT/CASCADE、低层 storage-only 关系、独立实际 engine 与 decoy
session、外层回滚/保存点/重启、真实 domain 依赖及 NULL/空串/宽值、peer/同引擎
reader、retained row lease、dirty conflict、目录删除重建和 marker/bootstrap 错误。

既有 DDL 夹具在回滚后重新获取真实 catalog，不再解引用已 eviction 的借用引用。
辅助对象夹具同时创建真实 app_aux.bump 与 public."app_aux.bump"，要求后者保留；
不通过删除阳性对象或放宽原数据断言掩盖问题。

domain/routine 冷校验夹具过去假设原生 DROP 不加载 catalog；完整 namespace
owner 改变了该 setup 前提。夹具在 DROP 后明确 evict，再保留原来的冷缓存、
3F000、零副作用和 public 不恢复断言。DROP TABLE 夹具也重新读取当前 owner，
并复制 namespace OID，不把旧 generation 错当作自动更新的 live model。
首批13与19原生分别保留1与2个夹具失败；更正四完整夹具在同一生产输入上全0，
另外三个完整 marker/写入原子性驱动全0。最终35入口证明不是一轮全注册通过。

最终可复查日志为 fourth-final-native.log、final-neighbor-native.log、
final-owner-fixtures-corrected.log、final-marker-gates.log、
final-neighbor-protocol.log。完整默认协议的失败与后续重跑分别见
final-neighbor-postgres_protocol_test.log、final-postgres-protocol-serial-retry.log。
没有修改该默认协议的 SQL/断言/超时，也没有据此实现用户暂缓的权限功能。

当前生产二进制 SHA256：
`37d45d76b2e0d369480564e2a0bbbccbdccd7bcad9fb6c2fbf30ac4ff6e60133`。
静态检查471份Markdown、715个本地内联目标、501份Python AST与13份shell语法
全0；文档状态、版本和兼容契约检查全0。原273项状态仍为22 complete、166
partial、70 unverified、15 deferred_by_user，未因这项缺陷修复提升功能状态。

## 范围

冷 reader 遇到其他 DDL owner 且没有 committed model 时仍 fail closed（55P03），
不是 PostgreSQL 完整目录 MVCC。更广 schema relocation/目录事务功能仍按总账
单独核验，不以本缺陷关闭整个 CAT/DDL family。CTAS/后台 checkpoint 专项、
R15 shutdown 和安全/TDE 用户暂缓范围不重开；普通调用路径的锁集成不构成这些
专项的完成声明。完整注册回归另在冻结主分支运行，不能用候选局部通过替代终态。
