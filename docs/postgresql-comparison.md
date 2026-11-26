# 与 PostgreSQL 18 的功能和性能差距

> 历史基准原始数据不随 Git 仓库提供。本文中的 `benchmarks/` 路径仅指可能存在的本地可选证据；新克隆仓库没有这些文件，原始结果只能在另行取得数据后复核。文中的测试与性能摘要仍保留。

## 2026-11-26 · 在同一数据库内移动普通视图

PostgreSQL 先锁定源视图，再检查执行者能否在目标模式创建对象。它改写关系的目录记录和模式依赖，视图的行类型也会移动。DuckDB 的 `ALTER VIEW` 文档展示事务内改名，依赖该视图的定义需要单独更新。SQLite 的视图文档介绍创建和删除，`ALTER TABLE` 用于修改表。 [PostgreSQL 实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L17765-L17910) · [DuckDB ALTER VIEW](https://duckdb.org/docs/2.0/sql/statements/alter_view.html) · [SQLite CREATE VIEW](https://www.sqlite.org/lang_createview.html) · [SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html)

本项目保留视图的编号并更新模式依赖。由于依赖视图保存的是 SQL，移动时还会改写并重新检查引用。实际移动依赖整库快照来恢复，视图行类型和只锁定涉及对象的实现仍有差距。 [功能测试](test-report.md#alter-view-set-schema-20261126) · [性能报告](performance-report-2026-10-30.md#alter-view-set-schema-cost-20261126)

## 2026-11-26 · 普通视图改名

PostgreSQL 修改目录中的关系名称和该视图的行类型名，其他对象继续按编号引用它。DuckDB 的视图改名随事务提交或回滚，依赖该视图的定义需要单独更新。SQLite 的 `ALTER TABLE` 改名适用于表。 [PostgreSQL ALTER VIEW](https://www.postgresql.org/docs/18/sql-alterview.html) · [PostgreSQL 改名实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L4022-L4102) · [DuckDB ALTER VIEW](https://duckdb.org/docs/current/sql/statements/alter_view) · [SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html)

本项目保留视图的目录编号，并把依赖它的普通视图写成指向新名字的查询。支持的触发器随视图改名。真正改名仍需整库回滚备份，视图的行类型尚未实现。 [功能测试](test-report.md#alter-view-rename-20261126) · [性能报告](performance-report-2026-10-30.md#alter-view-rename-cost-20261126)

## 2026-11-26 · 回滚备份少读一遍文件

PostgreSQL 的物理备份在传输文件时更新校验值，并默认等备份写入磁盘。SQLite 的在线备份按页复制，可以分次进行。DuckDB 通过检查点把日志中的写入合入数据库文件。 [PostgreSQL 备份实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/backup/basebackup.c#L1659-L1668) · [PostgreSQL 备份落盘](https://www.postgresql.org/docs/18/app-pgbasebackup.html) · [SQLite 在线备份](https://www.sqlite.org/backup.html) · [DuckDB 检查点](https://duckdb.org/docs/1.5/sql/statements/checkpoint)

本项目的回滚备份在复制顶层文件时计算校验值并同步。普通表改名仍复制整库。PostgreSQL 更新目录里的表名和对应行类型名，表的数据文件无需改名或复制。 [PostgreSQL 改名实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L4022-L4102) · [功能测试](test-report.md#transaction-backup-copy-20261126) · [性能报告](performance-report-2026-10-30.md#transaction-backup-copy-cost-20261126)

## 2026-11-26 · 跳过不存在的表

PostgreSQL 在执行普通 `ALTER TABLE` 前查找目标关系，带 `IF EXISTS` 时对缺表发出提示。SQLite 的 `ALTER TABLE` 直接修改保存的建表语句。DuckDB 的 `ALTER TABLE` 修改目录，改动随事务提交或回滚。 [PostgreSQL 实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/tcop/utility.c#L1200-L1257) · [SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html) · [DuckDB ALTER TABLE](https://duckdb.org/docs/1.5/sql/statements/alter_table)

本项目在支持的表修改中检查 `IF EXISTS`。显式事务遇到缺表时无需先取得数据库独占锁来备份。实际改名继续使用整库恢复文件，保护同一事务内已完成的其他表修改和写入。 [功能测试](test-report.md#alter-if-exists-20261126) · [性能报告](performance-report-2026-10-30.md#alter-if-exists-cost-20261126)

## 2026-11-26 · 改名时检查原名和目标名

PostgreSQL 支持 `ALTER TABLE IF EXISTS ... RENAME TO ...`，原表不存在时发出提示。SQLite 的改名语法更改同一个数据库内保存的表定义。DuckDB 也从目录层修改表名，视图引用需要另行处理。 [PostgreSQL ALTER TABLE](https://www.postgresql.org/docs/18/sql-altertable.html) · [SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html) · [DuckDB ALTER TABLE](https://duckdb.org/docs/1.5/sql/statements/alter_table)

项目普通表改名的原表缺失、缺少模式、重名和语法错误已经过对照。单条改名在写文件之前才备份，真正成功的改名仍使用整库备份保护回滚。PostgreSQL 允许用 `ALTER TABLE ... RENAME` 改部分其他关系对象的名字，本项目通过各对象的命令处理这些操作。 [功能测试](test-report.md#rename-name-checks-20261126) · [性能报告](performance-report-2026-10-30.md#rename-name-checks-cost-20261126)

## 2026-11-26 · 备份文件的同步

PostgreSQL 的 `pg_basebackup` 从运行中的集群取备份并生成文件清单，默认在客户端逐个打开并同步文件。SQLite 的在线备份接口按数据库页复制。DuckDB 的 `CHECKPOINT` 将日志同步到数据库文件，`EXPORT DATABASE` 则导出定义和表数据。 [PostgreSQL pg_basebackup](https://www.postgresql.org/docs/18/app-pgbasebackup.html) · [SQLite 在线备份](https://www.sqlite.org/backup.html) · [DuckDB CHECKPOINT](https://duckdb.org/docs/1.5/sql/statements/checkpoint) · [DuckDB EXPORT DATABASE](https://duckdb.org/docs/1.5/sql/statements/export)

项目的物理备份仍使用自己的备份格式。较大的备份可以同时校验和同步多份文件，同一流程也用于表改名等需要整库回滚备份的操作。普通表改名在 PostgreSQL 中只更新目录，项目的整库回滚备份仍是两者的主要成本差别。 [功能测试](test-report.md#backup-file-sync-20261126) · [性能报告](performance-report-2026-10-30.md#backup-file-sync-cost-20261126) · [PostgreSQL 改名](https://www.postgresql.org/docs/18/sql-altertable.html)

## 2026-11-26 · 普通表改名

PostgreSQL 在持有目标关系锁时更新目录里的表名及其行类型名，数据文件不随名称改动。SQLite 在 `sqlite_schema` 中重写表定义和引用，表内容保持原样。DuckDB 在事务中修改目录，改名可以回滚，依赖它的视图需要单独更新。 [PostgreSQL 18.6 实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L4022-L4102) · [SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html) · [DuckDB ALTER TABLE](https://duckdb.org/docs/1.5/sql/statements/alter_table)

本项目的文件名仍包含表名。这次保留了同一数据库的写入日志和其他表的缓存，仍通过整库备份保护改名。普通表编号、外键、视图和保存点已对照。普通表目前缺少 PostgreSQL 为每张表建立的行类型。 [功能测试](test-report.md#table-rename-caches-20261126) · [性能报告](performance-report-2026-10-30.md#table-rename-caches-cost-20261126)

## 2026-11-26 · 复用表定义和依赖记录

PostgreSQL 在 `pg_depend` 保存对象之间的依赖，用于 `DROP RESTRICT` 和 `DROP CASCADE`，并在提交后让其他进程更新目录缓存。SQLite 使用数据库头里的模式版本识别表结构变化。DuckDB 允许在一个写入进程中缓存目录并通过多版本控制处理并发。 [PostgreSQL 依赖目录](https://www.postgresql.org/docs/18/catalog-pg-depend.html) · [PostgreSQL 缓存更新](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/cache/inval.c) · [SQLite 文件格式](https://www.sqlite.org/fileformat2.html#schema_cookie) · [DuckDB 并发](https://duckdb.org/docs/1.5/connect/concurrency)

项目复用已保存的表定义和依赖记录。每次发布继续核对目录文件版本。视图、外键、改名和保存点已做同一组 SQL 对照。当前目录存储仍使用独立文件，并发和完整日志恢复还有差距。 [功能测试](test-report.md#relation-publication-20261126) · [性能报告](performance-report-2026-10-30.md#relation-publication-cost-20261126)

## 2026-11-26 · 复用已保存的列定义

PostgreSQL 在目录记录变更后让其他连接更新相应的缓存。SQLite 通过文件头里的模式版本判断是否需要重新读取表结构。DuckDB 可以在同一写入进程内缓存目录，通过多版本控制处理并发。 [PostgreSQL 目录缓存](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/cache/inval.c) · [SQLite 文件格式](https://www.sqlite.org/fileformat2.html#schema_cookie) · [DuckDB 并发](https://duckdb.org/docs/1.5/connect/concurrency)

本项目在未修改列定义的事务中复用已生成的文件内容，同时检查目录文件版本。真实写入仍生成新内容。列数较多时，这也减少普通事务启动和数据查询的成本。 [功能测试](test-report.md#attribute-publication-20261126) · [性能报告](performance-report-2026-10-30.md#attribute-publication-cost-20261126)

## 2026-11-26 · 复用已保存的注释内容

PostgreSQL 记录数据页变更，并在事务提交后通知其他连接更新目录缓存。SQLite 的回滚日志流程保存并写回变更页。DuckDB 的检查点将写入日志合入数据库文件。 [PostgreSQL WAL](https://www.postgresql.org/docs/18/wal-intro.html) · [PostgreSQL 目录缓存](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/cache/inval.c#L7-L33) · [SQLite 原子提交](https://www.sqlite.org/atomiccommit.html#writing_changes_to_the_database_file) · [DuckDB 检查点](https://duckdb.org/docs/1.5/sql/statements/checkpoint)

本项目保留上次生成的注释文件内容，发生修改后才重新生成。事务启动继续检查文件是否被其他连接或进程更新。两版本的完整查询、启动事务、数据改动和注释写入均有实测对照。 [功能测试](test-report.md#description-publication-20261126) · [性能报告](performance-report-2026-10-30.md#description-publication-cost-20261126)

## 2026-11-26 · 按编号定位注释范围

PostgreSQL 18.6 为 `pg_description` 建立了按对象编号、目录编号和列号排序的唯一 B-tree 索引。SQLite 的多列索引可利用开头的列定位记录。DuckDB 使用数据组的最小值和最大值跳过无匹配的组，也可以为部分等值查询使用 ART 索引。 [PostgreSQL 目录索引](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_description.h#L46-L64) · [SQLite 查询规划](https://www.sqlite.org/queryplanner.html#multicolumn) · [DuckDB 索引](https://duckdb.org/docs/1.5/guides/performance/indexing)

项目按内存中的对象编号定位范围，再统计当前可见的记录。两个连接的实测显示，PostgreSQL 在写入注释未提交时读到先前已提交的记录。项目的读请求在事务启动时等写入释放数据库级锁。PostgreSQL 在被注释对象上取得锁，SQLite 的 WAL 模式允许读旧快照，DuckDB 在同一进程内使用多版本并发控制。 [PostgreSQL COMMENT](https://www.postgresql.org/docs/18/sql-comment.html) · [PostgreSQL 事务隔离](https://www.postgresql.org/docs/18/transaction-iso.html) · [SQLite 事务隔离](https://www.sqlite.org/isolation.html) · [DuckDB 并发](https://duckdb.org/docs/1.5/connect/concurrency) · [本项目测试](test-report.md#description-range-20261126)

## 2026-11-26 · 直接统计注释记录

PostgreSQL 可以从索引读取筛选所需的列，并根据该事务能看到的记录计数。SQLite 的精确计数可以在 B-tree 上完成。DuckDB 分批统计输入行，并可以将已确认不含空值的 `COUNT(列)` 转为 `COUNT(*)`。 [PostgreSQL 索引扫描](https://www.postgresql.org/docs/18/indexes-index-only-scans.html) · [SQLite Count 指令](https://www.sqlite.org/opcode.html#Count) · [DuckDB 计数实现](https://github.com/duckdb/duckdb/blob/v1.5.0/src/function/aggregate/distributive/count.cpp)

本项目直接用当前查询可见的注释记录统计总数和分项数量。编号范围仍需逐条比较，注释目录的实体索引和并发读取还有差距。 [测试报告](test-report.md#description-count-20261126) · [性能报告](performance-report-2026-10-30.md#description-count-cost-20261126)

## 2026-11-26 · 比较对象编号时省去重复目录检查

PostgreSQL 18.6 为三种整数到 `oid` 登记隐式转换，32 位整数可以保留原有位模式。它也可以从多列索引中按对象编号查找注释。SQLite 可以用包含查询字段的索引避免回表。DuckDB 的列最值记录可以跳过不可能匹配的数据组。 [PostgreSQL 转换定义](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_cast.dat#L103-L112) · [PostgreSQL 多列索引](https://www.postgresql.org/docs/18/indexes-multicolumn.html) · [SQLite 查询规划](https://www.sqlite.org/queryplanner.html#covidx) · [DuckDB 索引](https://duckdb.org/docs/current/sql/indexes)

本项目执行已选定的编号比较时直接转换整数，显式使用的内置编号常量可以提前解析。用于比较的内置类型与搜索路径中的用户类型分别按各自规则确定。注释计数仍需扫描并检查每条候选记录。 [测试报告](test-report.md#oid-comparison-20261126) · [性能报告](performance-report-2026-10-30.md#oid-comparison-cost-20261126)

## 2026-11-26 · 注释目录可按名称查找

PostgreSQL 18.6 在 `pg_class` 登记注释目录，支持按名称和搜索路径转换为 `regclass`。SQLite 允许查询 `sqlite_schema`，该表没有登记自身。DuckDB 通过 `duckdb_tables()` 和 `duckdb_columns()` 提供对象编号和列信息。 [PostgreSQL 目录定义](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_description.h#L46-L64) · [PostgreSQL 对象编号](https://www.postgresql.org/docs/18/datatype-oid.html) · [SQLite 模式表](https://www.sqlite.org/schematab.html) · [DuckDB 目录函数](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions)

本项目登记固定关系编号 2609、四列及数据库自身分配的行类型。按搜索路径解析同名表，直接选择目录列时向客户端返回来源编号。执行计划显示实际使用的目录扫描，PostgreSQL 在受测的对象编号过滤条件下使用实体索引。本项目尚未补齐 PostgreSQL 的预置注释、注释索引以及 `psql \d` 所查的 `pg_policy`。 [测试报告](test-report.md#description-relation-20261126) · [性能报告](performance-report-2026-10-30.md#description-relation-cost-20261126)

## 2026-11-26 · 只备份注释文件

PostgreSQL 从基础备份重放归档的预写日志，可以在指定位置停止。SQLite 的回滚日志保存旧内容，预写日志模式则把改动写入日志后再合入数据库文件。DuckDB 将对象注释保存为目录元数据。 [PostgreSQL 恢复说明](https://www.postgresql.org/docs/18/continuous-archiving.html) · [SQLite 日志说明](https://sqlite.org/wal.html#how_wal_works) · [DuckDB 注释说明](https://duckdb.org/docs/1.5/sql/statements/comment_on)

本项目只为注释文件保留写入前的内容，随后建表时再补记建表的恢复信息。带恢复目标的启动目前只处理仍在初始时间线、保留完整且有提交时间的日志，并且除了待清理的注释事务之外没有其他目标后的提交。已清理的旧注释改动缺少通用的归档日志重放，带建表记录的日志仍不能按这一途径恢复。 [测试报告](test-report.md#comment-compact-undo-20261126) · [性能报告](performance-report-2026-10-30.md#comment-compact-undo-cost-20261126)

## 2026-11-26 · 保存注释时缩小备份范围

PostgreSQL 18.6 用对象编号定位并修改单条注释记录。SQLite 的回滚日志保存将被修改的页。DuckDB 把注释保存在目录对象的元数据中。 [PostgreSQL 注释写入源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/comment.c#L149-L200) · [SQLite 回滚日志](https://sqlite.org/atomiccommit.html#creating_a_rollback_journal) · [DuckDB COMMENT ON](https://duckdb.org/docs/1.5/sql/statements/comment_on)

本项目沿用建表恢复日志，在写入前保存相关目录和旧版兼容注释文件。普通写入只更新这次注释涉及的文件。事务已经需要整库备份，或旧目录需要补登记表结构时，沿用完整备份流程。数据库级写锁仍持有到事务结束。该版本带恢复目标的启动会拒绝尚未清理的这类日志。 [回滚测试](test-report.md#comment-file-undo-20261126) · [性能报告](performance-report-2026-10-30.md#comment-file-undo-cost-20261126)

## 2026-11-26 · 读取已保存的对象注释

PostgreSQL 18.6 的 `pg_description` 用对象编号、所属目录和子对象编号标识注释，`obj_description` 和 `col_description` 读取对应记录。DuckDB 也支持 `COMMENT ON`，可用目录函数查看注释。SQLite 在 `sqlite_schema` 中保存对象的建表或建索引定义。 [PostgreSQL 注释目录](https://www.postgresql.org/docs/18/catalog-pg-description.html) · [PostgreSQL 注释函数源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/system_functions.sql#L274-L310) · [DuckDB COMMENT ON](https://duckdb.org/docs/1.5/sql/statements/comment_on) · [SQLite 模式表](https://sqlite.org/schematab.html)

本项目读取通过 `COMMENT ON` 保存的数据库内注释，并保留多行文本。PostgreSQL 预置的系统对象注释和跨数据库共享对象目录还有缺口。原版 `psql` 的 `\dn+` 还需要暂缓的模式权限字段。 [用法](MANUAL.md#查看对象注释) · [功能测试](test-report.md#object-description-20261126) · [性能对照](performance-report-2026-10-30.md#object-description-cost-20261126)

一条 SQL 先调用会改写注释的函数，再读取注释时，本项目会立即读到新内容。PostgreSQL 18.6 在该语句中仍读到旧内容，下一条语句才读到新内容。 [PostgreSQL 函数可见性说明](https://www.postgresql.org/docs/18/xfunc-volatility.html)

该版本已经能直接查询 `pg_description`。后续版本补齐了按名称查找和执行计划支持。

## 2026-11-26 · 在一次查询中复用模式编号索引

PostgreSQL 18.6 分别按模式名和编号建唯一索引，系统目录缓存使用这些索引。SQLite 用数据库文件头的模式版本检测结构变更。DuckDB 的 `duckdb_schemas()` 初始化时收集模式列表，再分批返回。 [PostgreSQL 18.6 模式目录](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_namespace.h#L50-L55) · [SQLite 文件格式](https://sqlite.org/fileformat2.html#schema_cookie) · [DuckDB 1.5 源码](https://github.com/duckdb/duckdb/blob/v1.5.0/src/function/table/system/duckdb_schemas.cpp#L43-L65)

本项目筛选模式目录时，第一次执行归属判断会读取已经核验的编号索引，后续行可复用同一份索引。旧目录中同一编号存在多条记录时，用原有编号查找选中的名称。若查询执行中改动了模式，后续判断会重新读取目录。 [测试报告](test-report.md#namespace-identity-index-20261126) · [性能报告](performance-report-2026-10-30.md#namespace-identity-index-cost-20261126)

原版 `psql` 18.6 的 `\dn 模式名` 会另查复制发布信息。本项目仍缺这项查询所需的发布目录。`\dn+ 模式名` 也仍缺少模式权限字段。 [psql 模式列表源码](https://github.com/postgres/postgres/blob/REL_18_6/src/bin/psql/describe.c#L4886-L4958)

## 2026-11-26 · 筛选其他连接的临时模式

PostgreSQL 18.6 用模式编号从系统目录缓存查模式名，再判断名称前缀。SQLite 的虚拟机把行值放入寄存器供函数读取，DuckDB 按批向执行算子传递各列的数据。 [PostgreSQL 18.6 模式名称查找](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c#L3483-L3508) · [SQLite Function 指令](https://sqlite.org/opcode.html#Function) · [DuckDB 向量格式](https://duckdb.org/docs/1.5/internals/vector)

前一版读取模式列表时，用该行已有的名称判断归属。一般参数和普通表里的编号仍要查目录。查询期间有模式变动时，会重新按编号查询。 [功能测试](test-report.md#temp-schema-source-reuse-20261126) · [性能对照](performance-report-2026-10-30.md#temp-schema-source-reuse-cost-20261126)

## 2026-11-26 · 查询临时模式的编号和归属

PostgreSQL 的 `pg_my_temp_schema()` 返回当前连接自己的编号。`pg_is_other_temp_schema(oid)` 可按编号排除其他临时模式，也识别 PostgreSQL 用于存放大字段的临时模式名称。SQLite 用 `PRAGMA database_list` 列出当前连接的数据库，DuckDB 提供 `duckdb_schemas()` 查询模式。 [PostgreSQL 18.6 判断源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c#L3442-L3509) · [SQLite 数据库列表](https://sqlite.org/pragma.html#pragma_database_list) · [DuckDB 1.4 模式目录](https://duckdb.org/docs/1.4/sql/meta/duckdb_table_functions#duckdb_schemas)

本项目支持两条 SQL 调用，并经实际连接对照参数、编号类型和保存点回滚。项目重启或连接退出会清理自己的临时模式记录，PostgreSQL 可以留下空模式供以后复用。项目的临时表也没有单独的 TOAST 模式。 [测试报告](test-report.md#temp-schema-inspection-20261126) · [PostgreSQL 清理源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c#L4321-L4348)

## 2026-11-26 · 按连接识别临时模式

PostgreSQL 18.6 在首次使用临时模式时才初始化它，按连接记录是否已经初始化。SQLite 在首次创建临时表时才创建临时数据库文件。DuckDB 把临时表放在当前连接的 `temp.main` 中。 [PostgreSQL 18.6 初始化源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c#L4133-L4252) · [SQLite 临时数据库](https://sqlite.org/tempfiles.html#temp_databases) · [DuckDB 1.4 临时表](https://duckdb.org/docs/1.4/sql/statements/create_table#temporary-tables)

项目的新库也在首次使用时创建临时模式。通过 `pg_temp` 查表编号时，旧库中遗留的目录记录不会让尚未初始化的连接误报表不存在。两边的 `current_schema` 都可以在 `pg_temp` 优先时创建临时模式，并在事务或保存点回滚时撤销。DuckDB 也提供 `current_schema()`，默认返回 `main`。 [首次使用测试](test-report.md#lazy-temp-namespace-20261126) · [当前模式测试](test-report.md#current-schema-temp-20261126) · [PostgreSQL 18.6 搜索路径源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c#L4523-L4551) · [DuckDB 1.4 函数说明](https://duckdb.org/docs/1.4/sql/functions/utility#current_schema)

PostgreSQL 在事务或子事务结束时分别处理临时模式的状态。DuckDB 将临时表与连接绑定，SQLite 把临时表放在独立的临时数据库里。本项目现在只登记待撤销的临时模式记录，实例重启后由现有的临时对象清理流程移除孤立记录。首次初始化仍持有目录写入的数据库级锁，事务结束前，其他连接读取普通表也可能等锁超时。 [PostgreSQL 18.6 回滚源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c#L4245-L4319) · [DuckDB 1.4 临时表](https://duckdb.org/docs/1.4/sql/statements/create_table#temporary-tables) · [SQLite 临时数据库](https://sqlite.org/tempfiles.html#temp_databases) · [性能报告](performance-report-2026-10-30.md#temp-namespace-row-undo-cost-20261126)

PostgreSQL 创建模式时向带索引的 `pg_namespace` 插入一行。本项目使用模式目录文件，确认整套目录文件仍是当前版本后，只为这次写入编码模式记录。检查发现目录文件缺失或已被其他连接替换时，沿用完整的目录校验和写入流程。 [PostgreSQL 18.6 模式写入](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/pg_namespace.c#L67-L92) · [本项目性能报告](performance-report-2026-10-30.md#temp-schema-scoped-publication-cost-20261126)

## 2026-11-26 · 表名前带上数据库名

PostgreSQL 将 `数据库名.模式名.表名` 中的数据库名与当前连接的数据库核对，再在指定模式中找表。显式文本转换和未定型字面量对缺失模式使用不同的错误码。SQLite 可以通过两段名称指定主库、临时库或附加数据库。DuckDB 支持分别指定目录和模式。 [PostgreSQL 18.6 名称查找源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c#L417-L504) · [PostgreSQL 18.6 转换源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/regproc.c#L817-L856) · [SQLite 对象名称](https://sqlite.org/lang_naming.html) · [DuckDB 1.4.2 目录查找源码](https://github.com/duckdb/duckdb/blob/v1.4.2/src/catalog/catalog.cpp#L395-L433)

本项目的 `regclass` 输入现在可以带上当前数据库名。写其他库名返回 `0A000`。普通模式不存在时，文本或变长字符串转换返回 `3F000`，未定型字面量返回 `42P01`。 [用法](MANUAL.md#从文本列查找表的编号) · [测试](test-report.md#regclass-qualified-database-20261126)

## 2026-11-26 · 用查询参数指定要查看的表

PostgreSQL 的索引扫描在执行时计算依赖运行时数据的查找值，重新扫描时会重算。SQLite 将等值条件的右侧求值写入用于查找的寄存器。DuckDB 在支持提前筛选的扫描中遇到参数时，可在执行时重新绑定筛选条件。 [PostgreSQL 18.6 索引扫描源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeIndexscan.c#L504-L610) · [SQLite 3.51.0 等值条件源码](https://github.com/sqlite/sqlite/blob/version-3.51.0/src/wherecode.c#L775-L805) · [DuckDB 1.4.2 条件筛选源码](https://github.com/duckdb/duckdb/blob/v1.4.2/src/optimizer/pushdown/pushdown_get.cpp#L8-L30)

本项目在直接查询 `pg_attribute`、第一项条件按对象编号等值筛选时，可先用文本参数查找表编号，再为匹配的列生成查询行。参数值来自本次执行。后续表达式如果改变了查找结果，扫描按原顺序继续交给查询自身筛选。目录元数据仍取自整份快照。 [测试](test-report.md#catalog-runtime-key-20261126) · [性能对照](performance-report-2026-10-30.md#catalog-runtime-key-cost-20261126)

## 2026-11-26 · 从文本查找表

PostgreSQL 18.6 将 `text` 和 `varchar` 转成 `regclass` 时按关系名查找。显示结果时按对象编号读取目录缓存，再决定是否带上模式名。SQLite 的 `PRAGMA table_xinfo` 先定位指定的表，再遍历它的列。DuckDB 按名称查到候选目录项后，选择当前事务可见的版本。 [PostgreSQL 18.6 转换和输出源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/regproc.c) · [PostgreSQL 18.6 转换声明](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_cast.dat#L247-L250) · [SQLite 3.51.0 表信息源码](https://github.com/sqlite/sqlite/blob/version-3.51.0/src/pragma.c#L1158-L1205) · [DuckDB 1.4.2 目录查找源码](https://github.com/duckdb/duckdb/blob/v1.4.2/src/catalog/catalog_set.cpp#L538-L550)

该版本用当前事务可见的目录按名称和编号查找对应表，沿用原有的目录文件校验。文本 `007` 可以查到名为 `"007"` 的表，直接写下的 `'007'::regclass` 则表示编号 7。该轮测试中，目录查询还会逐行求值运行时文本。 [测试](test-report.md#regclass-runtime-20261126) · [性能对照](performance-report-2026-10-30.md#regclass-runtime-cost-20261126)

## 2026-11-26 · 按表名查列时怎样确定对象

PostgreSQL 可以在解析阶段把 `regclass` 字面量转换成实际对象编号，在相关对象变更或搜索路径变化时重新分析已准备的语句。SQLite 执行预编译语句时核对结构版本号。DuckDB 用目录版本与身份识别需要重新绑定的语句。 [PostgreSQL 对象编号类型](https://www.postgresql.org/docs/18/datatype-oid.html) · [PostgreSQL PREPARE](https://www.postgresql.org/docs/18/sql-prepare.html) · [SQLite 结构版本](https://www.sqlite.org/fileformat.html#schema_cookie) · [DuckDB 目录版本检查](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.2/src/main/prepared_statement_data.cpp)

本项目复用当前语句校验字面表名时得到的对象编号，用于已绑定为 `oid` 的等值比较。显式文本转换及参数保留运行时查找。已有预编译语句在同名表改名又回滚后的绑定与 PostgreSQL 存在差异。 [测试](test-report.md#regclass-catalog-filter-20261126) · [性能对照](performance-report-2026-10-30.md#regclass-catalog-cost-20261126)

## 2026-11-26 · 用表编号缩小列目录的读取范围

PostgreSQL 转换未知类型的字符串时可在解析阶段生成已定型的常量；`pg_attribute` 有按表编号、列序号查找的索引。SQLite 分析 `WHERE` 的比较条件，再为符合条件的项选择索引。DuckDB 的优化器把可预先计算的表达式变成常量。 [PostgreSQL 18.6 类型转换源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_coerce.c) · [PostgreSQL 18.6 列目录声明](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/include/catalog/pg_attribute.h) · [SQLite 条件分析](https://www.sqlite.org/optoverview.html#where_clause_analysis) · [DuckDB 常量折叠源码](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.2/src/optimizer/rule/constant_folding.cpp)

本项目确认等值条件中的常量实际使用内置 `oid` 类型，再将各种写法转换成同一个编号。用表名转换得到 `regclass` 的情况仍需要查询实际目录。 [测试结果](test-report.md#typed-oid-catalog-filter-20261126) · [性能对照](performance-report-2026-10-30.md#typed-oid-catalog-cost-20261126)

## 2026-11-26 · 关闭时保留目录所需的数据

PostgreSQL 后端按类型编号维护类型缓存，并注册目录变更的失效回调。SQLite 的关闭接口要求先释放连接等资源。DuckDB 的数据库实例先清理附加数据库，再释放调度器、日志管理器和缓冲区。 [PostgreSQL 18.6 类型缓存源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/cache/typcache.c) · [SQLite 关闭接口](https://www.sqlite.org/c3ref/initialize.html) · [DuckDB 1.4.2 数据库实例源码](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.2/src/main/database.cpp)

本项目把内置类型表保留到进程结束。关闭期间如果需要刷新目录，表结构和索引名称直接从文件读取。 [验证记录](test-report.md#catalog-shutdown-20261126)

## 2026-11-24 · 从文件读取已有目录

PostgreSQL 可以按键查询系统目录缓存，需要修改记录的调用方可显式复制该记录。SQLite 在 `sqlite_schema` 保存定义对象的 SQL，并用文件头的结构版本核对已准备的语句。DuckDB 从目录项集合中查找对象，并按事务决定适用的版本。 [PostgreSQL 18.6 目录缓存源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/cache/syscache.c) · [SQLite 结构表](https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema) · [DuckDB 1.4.2 目录项源码](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.2/src/catalog/catalog_set.cpp)

本项目直接从文件读取目录时仍解析和核对记录，只为可写模型构造发布时的对照副本。Linux 会把同一轮的目录文件检查集中到一个描述符下。直接读取的时间缩短，已有目录模型的 SQL 仍承担重复的文件状态检查。 [性能对照](performance-report-2026-10-30.md#catalog-cold-read-cost-20261124)

## 2026-11-24 · 表结构变化后如何刷新缓存

PostgreSQL 记录系统目录变更，在命令结束、提交或回滚时按可见性规则处理缓存失效。SQLite 用数据库文件头中的结构版本号检查预编译语句。DuckDB 对预编译语句核对所依赖数据库的目录身份和版本。 [PostgreSQL 18.6 缓存失效源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/cache/inval.c) · [SQLite 文件格式](https://www.sqlite.org/fileformat.html#schema_cookie) · [DuckDB 1.4.2 预编译语句源码](https://github.com/duckdb/duckdb/blob/v1.4.2/src/main/prepared_statement_data.cpp)

本项目的表定义和索引名称分别保存在文件里。再次读取时核对文件身份、长度和时间，命中时可复用已解析的内容。数据库原有的事务读取规则照常生效。 [用例和恢复验证](test-report.md#witnessed-index-files-20261124) · [性能对照](performance-report-2026-10-30.md#witnessed-index-files-cost-20261124)

## 2026-11-24 · 读取索引定义时如何识别函数

PostgreSQL 在分析 SQL 时为函数调用选择具体函数编号和结果类型。`pg_get_indexdef` 执行时通过系统目录缓存按编号取得索引记录。SQLite 可以用 `PRAGMA index_xinfo` 查询索引列，DuckDB 通过 `duckdb_indexes()` 的 `sql` 字段给出索引定义。 [PostgreSQL 18.6 函数绑定](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_func.c) · [PostgreSQL 18.6 索引定义](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/ruleutils.c) · [SQLite 索引列](https://www.sqlite.org/pragma.html#pragma_index_xinfo) · [DuckDB 索引目录](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_indexes)

本项目在行执行阶段沿用已绑定的返回类型。定向跟踪显示，五列索引详情查询的目录文件状态检查从约 396–408 次减为 276 次，表定义和索引名称文件各打开 6 次。 [性能对照](performance-report-2026-10-30.md#bound-catalog-results-cost-20261124)

## 2026-11-24 · 检查表和索引的列信息

PostgreSQL 在 `pg_attribute` 上按表编号和列序号建索引，并为按列序号查找保留缓存。SQLite 的 `PRAGMA table_xinfo` 返回指定表的列。DuckDB 通过 `duckdb_columns()` 返回各表的列信息。 [PostgreSQL 18.6 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_attribute.h) · [SQLite 列说明](https://www.sqlite.org/pragma.html#pragma_table_xinfo) · [DuckDB 列信息](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_columns)

本项目读取已有列目录时，先按对象编号归拢系统列，再检查每个对象应有的列。对象增多时，检查次数随列数和对象数增长，省去为每张表重复扫描整个目录的开销。读取单个对象的操作仍会经过目录校验。 [性能对照](performance-report-2026-10-30.md#system-attribute-validation-cost-20261124)

## 2026-11-24 · 查询索引的列

PostgreSQL 在 `pg_attribute` 为索引的每一列保存一条记录，`psql` 通过 `pg_index.indnkeyatts` 确定哪些列是键。SQLite 的 `PRAGMA index_xinfo` 区分键和辅助定位列。DuckDB 的 `duckdb_indexes()` 列出显式创建的索引和对应的建索引语句。 [PostgreSQL 列目录](https://www.postgresql.org/docs/18/catalog-pg-attribute.html) · [PostgreSQL 索引目录](https://www.postgresql.org/docs/18/catalog-pg-index.html) · [SQLite 索引列](https://www.sqlite.org/pragma.html#pragma_index_xinfo) · [DuckDB 索引目录](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_indexes)

本项目普通 B 树和哈希索引使用已有目录文件保存列记录，原版 `psql` 可读取。简单的五列索引详情查询在本机测得约 8–9 毫秒，PostgreSQL 18.6 约 0.3–0.5 毫秒。 [功能测试](test-report.md#index-attributes-20261124) · [性能对照](performance-report-2026-10-30.md#index-attributes-cost-20261124)

<a id="index-predicate-comparison-20261124"></a>
## 2026-11-24 · 部分索引条件如何保存和显示

PostgreSQL 在 `pg_index.indpred` 中保存条件的表达式树，用 `pg_get_expr` 生成可读 SQL。SQLite 的部分索引也只收录条件为真的行，建索引语句保存在 `sqlite_schema`。DuckDB 的分区范围摘要可以依据筛选条件跳过部分行组。 [PostgreSQL 的索引目录](https://www.postgresql.org/docs/18/catalog-pg-index.html) · [SQLite 部分索引](https://www.sqlite.org/partialindex.html) · [SQLite 结构目录](https://www.sqlite.org/schematab.html) · [DuckDB 的范围摘要](https://duckdb.org/docs/current/guides/performance/indexing#zonemaps)

本项目在原有索引文件中保留条件的 SQL。从目录读取时，用实际列号、列类型和 PostgreSQL 内置运算符编号生成所支持的表达式树。条件的转换范围及查询规划限制见验证记录。 [功能测试](test-report.md#index-predicate-20261124) · [性能对照](performance-report-2026-10-30.md#index-predicate-cost-20261124)

<a id="catalog-join-filter-comparison-20261124"></a>
## 2026-11-24 · 目录连接的筛选顺序

PostgreSQL 会在生成执行计划时选择访问路径，`EXPLAIN` 可以显示筛选发生在哪个环节。SQLite 记录外连接中的 `ON` 条件来源，以保留和 `WHERE` 不同的语义。DuckDB 会把等值条件传给其他等价列，再将条件下推到数据读取。 [PostgreSQL 执行计划](https://www.postgresql.org/docs/18/using-explain.html) · [SQLite 的条件分析](https://www.sqlite.org/optoverview.html#where_clause_analysis) · [DuckDB 的优化方法](https://duckdb.org/docs/current/internals/overview)

本项目在三个目录的连接中，沿已绑定的对象编号相等条件缩小读取范围。左连接只向可补空值的一侧传递条件。原查询中的 `ON` 和 `WHERE` 仍参与执行。此路径用于符合条件的目录连接，通用连接优化继续跟踪。 [验证范围](test-report.md#catalog-join-filter-20261124) · [性能对照](performance-report-2026-10-30.md#catalog-join-filter-cost-20261124)

<a id="primary-constraint-comparison-20261124"></a>
## 2026-11-24 · 主键、外键与索引的关联

PostgreSQL 的 `pg_constraint` 保存约束编号、定义中的列号和相关索引编号。主键和引用该主键的外键会指向同一个索引。SQLite 用 `PRAGMA foreign_key_list` 查看外键声明。DuckDB 的 `duckdb_constraints()` 提供约束位置、列名和外键引用目标。 [PostgreSQL 约束目录](https://www.postgresql.org/docs/18/catalog-pg-constraint.html) · [SQLite 外键列表](https://www.sqlite.org/pragma.html#pragma_foreign_key_list) · [DuckDB 约束信息](https://duckdb.org/docs/1.5/sql/meta/duckdb_table_functions#duckdb_constraints)

本项目在已有的约束目录中登记主键约束编号，并关联实际使用的主键索引。外键列与被引用的主键是同一组列时，关联该主键索引。其他唯一键及完整的约束依赖仍待实现。 [验证范围](test-report.md#primary-constraint-20261124) · [性能对照](performance-report-2026-10-30.md#primary-constraint-cost-20261124)

<a id="primary-index-comparison-20261124"></a>
## 2026-11-24 · 主键索引的编号和来源

PostgreSQL 为主键约束和支撑它的索引分别分配编号，并用 `pg_constraint.conindid` 连接两者。SQLite 可以用 `PRAGMA index_list` 的 `pk` 标记识别主键索引。DuckDB 的 `duckdb_indexes()` 列出显式索引，主键可通过 `duckdb_constraints()` 查询。 [PostgreSQL 约束目录](https://www.postgresql.org/docs/18/catalog-pg-constraint.html) · [SQLite 索引列表](https://www.sqlite.org/pragma.html#pragma_index_list) · [DuckDB 索引信息](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_indexes)

本项目从表定义读取主键列，登记独立索引编号，数据仍由表原有的主键索引维护。`pg_index` 及索引列信息可按编号查询。 [查询示例](MANUAL.md#查询主键索引) · [性能报告](performance-report-2026-10-30.md#primary-index-cost-20261124)

<a id="pg-index-comparison-20261124"></a>
## 2026-11-24 · 按表查询索引

PostgreSQL 的 `pg_index` 记录索引与表的对象编号，分开记录键列数和总列数。SQLite 的 `PRAGMA index_list` 区分显式索引、主键和唯一约束，`PRAGMA index_xinfo` 可读取索引列。DuckDB 在 `duckdb_indexes()` 中列出显式创建的索引，并通过 `duckdb_constraints()` 提供约束信息。 [PostgreSQL 索引目录](https://www.postgresql.org/docs/18/catalog-pg-index.html) · [SQLite 索引列表](https://www.sqlite.org/pragma.html#pragma_index_list) · [DuckDB 索引信息](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_indexes)

本项目使用已经保存的索引编号、所属表和索引定义。按表号或索引号等值筛选时，只读取匹配对象的定义。 [使用说明](MANUAL.md#按表查询索引) · [性能报告](performance-report-2026-10-30.md#pg-index-cost-20261124)

<a id="index-definition-comparison-20261124"></a>
## 2026-11-24 · 查询索引的定义

PostgreSQL 根据索引编号读取索引目录，再用当前的表名、列名、访问方式和附带列生成建索引语句。SQLite 的 `sqlite_schema` 保存建索引 SQL，`PRAGMA index_xinfo` 提供索引列。DuckDB 的 `duckdb_indexes()` 可以列出显式索引及其定义。 [PostgreSQL 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/ruleutils.c) · [SQLite 结构目录](https://www.sqlite.org/schematab.html) · [SQLite 索引列](https://www.sqlite.org/pragma.html#pragma_index_xinfo) · [DuckDB 索引信息](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_indexes)

本项目从实际索引编号和依赖关系找到所属表，只读取该表保存的索引定义。可以还原普通表上显式创建的普通 B 树、唯一 B 树及哈希索引。表达式索引仍待补齐。 [使用说明](MANUAL.md#查询索引的定义) · [性能报告](performance-report-2026-10-30.md#index-definition-cost-20261124)

<a id="array-subquery-comparison-20261124"></a>
## 2026-11-24 · 把查询结果收进数组

PostgreSQL 逐行收集 `ARRAY(SELECT ...)` 的结果，并保留列类型和空值。DuckDB 的同类语法返回列表。SQLite 可用 `json_group_array` 生成 JSON 数组，列表的顺序可在聚合调用中指定。 [PostgreSQL 表达式说明](https://www.postgresql.org/docs/18/sql-expressions.html#SQL-SYNTAX-ARRAY-CONSTRUCTORS) · [PostgreSQL 执行器源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeSubplan.c) · [DuckDB 子查询](https://duckdb.org/docs/current/sql/expressions/subqueries#array-subqueries) · [SQLite JSON 聚合](https://www.sqlite.org/json1.html#array_and_object_aggregate_functions)

本项目直接从已绑定的子查询读取带类型的值，按子查询实际输出次序生成数组。只在本次查询内复用不依赖外层行的结果。分组读取需要按外层行分别执行对应的子查询。 [查询示例](MANUAL.md#把查询结果收进数组) · [性能对照](performance-report-2026-10-30.md#array-subquery-cost-20261124)

<a id="roles-view-comparison-20261124"></a>
## 2026-11-24 · 公开角色视图

PostgreSQL 从共享角色目录读取角色属性，将密码字段替换为固定的掩码，并从角色设置目录加入全局默认配置。SQLite 把访问检查交给宿主应用的授权回调。DuckDB 的嵌入式进程也由宿主控制数据库访问。 [PostgreSQL 视图源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/system_views.sql) · [SQLite 授权接口](https://www.sqlite.org/c3ref/set_authorizer.html) · [DuckDB 安全模型](https://duckdb.org/security#security-model)

本项目的 `pg_roles` 从当前实例的共享目录读取真实编号和属性。明确按编号查找时，只取对应角色。查询结果始终使用固定掩码。角色级的默认配置、角色成员目录和行访问规则目录仍待实现。 [使用说明](MANUAL.md#列出数据库角色) · [测试报告](test-report.md#roles-view-20261124)

<a id="partition-ancestors-comparison-20261124"></a>
## 2026-11-24 · 查询分区归属

PostgreSQL 18.6 用继承目录的表编号连接子分区与父表。`pg_partition_ancestors` 从输入的分区开始向上查找。SQLite 的 `PRAGMA table_list` 提供每张表的基本信息，虚拟表可以提供带参数的行来源。DuckDB 的 Hive 分区读取器从目录结构取得分区键，可以跳过与查询条件无关的文件。 [PostgreSQL 分区函数源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/partitionfuncs.c) · [PostgreSQL 父表查询源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/partition.c) · [SQLite 表清单](https://www.sqlite.org/pragma.html#pragma_table_list) · [SQLite 虚拟表](https://www.sqlite.org/vtab.html#table_valued_functions) · [DuckDB 文件分区](https://duckdb.org/docs/current/data/partitioning/hive_partitioning)

本项目用表的实际目录编号和已保存的分区定义查找。普通表走按编号查找，可直接结束。旧版的子分区需要有明确的目录标记和唯一匹配的父表定义。普通继承关系不参与查询。独立子分区、分区索引和 PostgreSQL 的完整分区目录仍有差距。 [使用说明](MANUAL.md#查询分区归属) · [测试报告](test-report.md#partition-ancestors-20261124)

<a id="foreign-key-definition-comparison-20261124"></a>
## 2026-11-24 · 还原外键的 SQL 定义

PostgreSQL 18.6 按约束编号读取当前定义。它根据列号取得当前列名，根据搜索路径决定是否写出引用表所在的模式，并省略默认的 `NO ACTION`。SQLite 的 `PRAGMA foreign_key_list` 把动作与引用列分字段返回。DuckDB 的 `duckdb_constraints()` 还提供约束的 SQL 片段。 [PostgreSQL 函数源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/ruleutils.c) · [SQLite 查询外键](https://www.sqlite.org/pragma.html#pragma_foreign_key_list) · [DuckDB 约束信息](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_constraints)

本项目由已登记的普通外键、当前表定义和验证状态还原相应 SQL，同时提供 `confupdtype` 和 `confdeltype`。旧版已经合并保存的动作沿用记录中的 `RESTRICT`。更新或删除被引用行时，`NO ACTION` 与 `RESTRICT` 的完整执行时机差异仍待补齐。 [测试报告](test-report.md#foreign-key-definition-20261124)

<a id="foreign-key-catalog-comparison-20261124"></a>
## 2026-11-24 · 查询外键和它引用的列

PostgreSQL 18.6 把约束登记为 `pg_constraint` 对象，外键行关联本表、引用表和两侧的列号。约束名只需在同一张表内唯一，约束编号可以独立查找。SQLite 用 `PRAGMA foreign_key_list` 列出指定表的外键。DuckDB 的 `duckdb_constraints()` 返回约束名称、定义、列和引用目标，也提供该约束在表定义中的位置。 [PostgreSQL 目录源码](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_constraint.h) · [SQLite 外键查询](https://www.sqlite.org/pragma.html#pragma_foreign_key_list) · [DuckDB 约束信息](https://duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_constraints)

本项目沿用实际执行外键的表定义，从目录分配并保存独立编号。查询用当前表的列号和现有约束状态生成结果。这一轮覆盖具备目录身份的普通表外键，随后加入了定义显示。分区约束、其他约束类型和支持外键的索引编号仍待补齐。 [目录测试](test-report.md#foreign-key-catalog-20261124) · [定义显示](test-report.md#foreign-key-definition-20261124)

<a id="catalog-oid-lookup-comparison-20261124"></a>
## 2026-11-24 · 按编号查找类型和排序规则

PostgreSQL 18.6 为 `pg_type.oid` 和 `pg_collation.oid` 定义唯一 B 树索引，两者也用于系统目录缓存。SQLite 在关联子查询中可以使用可用索引缩小读取范围。DuckDB 对关联子查询执行去关联改写，可以把逐行子查询转换为连接等操作。 [PostgreSQL 类型目录源码](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_type.h) · [PostgreSQL 排序规则目录源码](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_collation.h) · [SQLite 查询计划](https://www.sqlite.org/eqp.html) · [DuckDB 关联子查询](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql)

本项目在这两张系统表组成的查询中，利用外层已有的对象编号，只把匹配的目录记录转换成行，再执行原有筛选条件。类型记录继续取自执行开始时持有的目录快照。类型目录仍需扫描快照中的编号，跨查询的持久索引和更普遍的子查询改写仍有差距。 [测试范围](test-report.md#catalog-oid-lookup-20261124) · [性能对照](performance-report-2026-10-30.md#catalog-oid-lookup-cost-20261124)

<a id="collation-catalog-comparison-20261124"></a>
## 2026-11-24 · 查询列使用的排序规则

PostgreSQL 用 `pg_attribute.attcollation` 连接 `pg_collation`，目录包含实现方式、编码、区域设置和版本。`psql` 只在列的排序规则与类型默认值不同时显示名称。SQLite 的 `PRAGMA collation_list` 列出连接可用的排序规则。DuckDB 用 `PRAGMA collations` 返回可用名称。 [PostgreSQL 排序规则目录](https://www.postgresql.org/docs/18/catalog-pg-collation.html) · [psql 18.6 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/bin/psql/describe.c) · [SQLite PRAGMA](https://www.sqlite.org/pragma.html#pragma_collation_list) · [DuckDB PRAGMA](https://duckdb.org/docs/stable/configuration/pragmas#collations)

本项目从实际列定义登记支持的内置排序规则。共有的 `default`、`C`、`POSIX` 和 `ucs_basic` 使用 PostgreSQL 的编号，项目自带的三个比较规则使用保留编号。用户自定义规则、操作系统提供的其他规则及完整 ICU 支持仍需补齐。 [功能验证](test-report.md#collation-catalog-20261124) · [性能对照](performance-report-2026-10-30.md#collation-catalog-cost-20261124)

<a id="column-default-expression-comparison-20261124"></a>
## 2026-11-24 · 查询列的默认值

PostgreSQL 在 `pg_attrdef.adbin` 保存表达式树。`pg_get_expr` 从树中生成 SQL，并在需要时用当前关系解释列编号。SQLite 的 `PRAGMA table_xinfo` 直接提供默认值。DuckDB 的 `information_schema.columns` 提供 SQL 格式的默认表达式。 [PostgreSQL 默认值目录](https://www.postgresql.org/docs/18/catalog-pg-attrdef.html) · [PostgreSQL 18.6 显示实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/ruleutils.c) · [SQLite 列信息](https://www.sqlite.org/pragma.html#pragma_table_xinfo) · [DuckDB 列信息](https://duckdb.org/docs/current/sql/meta/information_schema#columns-columns)

本项目读取已登记的默认值，按实际类型生成受支持的常量节点，并用存下来的对象编号查找当前名称。只查默认值编号或列号时跳过表达式。普通运算、序列调用和生成列引用还需要其他节点的实现。 [功能验证](test-report.md#column-default-expression-20261124) · [性能对照](performance-report-2026-10-30.md#column-default-expression-cost-20261124)

<a id="format-type-comparison-20261124"></a>
## 2026-11-24 · 查询列的类型和长度

PostgreSQL 从类型目录缓存获取指定编号的类型及数组元素，按搜索路径决定是否加上模式名。长度和精度来自列保存的修饰值，按类型的输出函数生成。SQLite 的 `PRAGMA table_xinfo` 直接给出列声明中的类型及隐藏列标记。DuckDB 的 `duckdb_columns()` 把类型名、类型编号、精度和小数位数分别列出。 [PostgreSQL 18.6 类型格式化实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/format_type.c) · [SQLite 列信息](https://www.sqlite.org/pragma.html#pragma_table_xinfo) · [DuckDB 列信息](https://duckdb.org/docs/stable/sql/meta/duckdb_table_functions.html#duckdb_columns)

本项目也从类型编号生成显示名称，使用类型和数组元素间的真实目录关联，并读取列实际保存的长度或精度。可以从不可变的内置类型声明确定结果时，跳过整个目录的读取和复制。搜索路径可能选中同名用户类型时，使用当前目录判断。 [功能验证](test-report.md#format-type-20261124) · [性能对照](performance-report-2026-10-30.md#format-type-cost-20261124)

<a id="pg-access-method-comparison-20261124"></a>
## 2026-11-24 · 查表和索引的访问方式

PostgreSQL 通过 `pg_class.relam` 关联 `pg_am`，并通过 `pg_get_userbyid` 将所有者编号转为角色名。SQLite 的 `pragma_index_list` 报告每张表的索引来源，包括主键、唯一约束和显式索引。DuckDB 将显式索引和约束的元数据分别放在 `duckdb_indexes()` 与 `duckdb_constraints()`。 [PostgreSQL 访问方式目录](https://www.postgresql.org/docs/18/catalog-pg-am.html) · [PostgreSQL 18.6 目录编号](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_am.dat) · [SQLite 索引元数据](https://www.sqlite.org/pragma.html#pragma_index_list) · [DuckDB 元数据函数](https://duckdb.org/docs/stable/sql/meta/duckdb_table_functions.html)

本项目复用实际的索引维护方式和持久化索引名称。堆表和 PostgreSQL 共有的访问方式采用相同编号，项目自己的 Bloom 和全文索引使用保留编号。复合 B 树按 B 树显示。访问方式的处理函数、自定义访问方式以及索引和约束目录仍有差距。 [测试](test-report.md#pg-access-method-20261124) · [性能](performance-report-2026-10-30.md#pg-access-method-cost-20261124)

<a id="psql-catalog-discovery-comparison-20261124"></a>
## 2026-11-24 · psql 按名称查找表

PostgreSQL 的 `psql` 通过 SQL 连接 `pg_class` 和 `pg_namespace`，用正则匹配表名，再通过 `pg_table_is_visible` 处理搜索路径里的同名关系。PostgreSQL 用目录缓存查找关系，并按模式、选项和排序规则复用已编译的正则表达式。SQLite 的 `.tables` 从各个已连接数据库的 `sqlite_schema` 读取名称。DuckDB 可用 SQL `DESCRIBE` 直接查看表或查询的列。 [psql 18.6 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/bin/psql/describe.c) · [PostgreSQL 搜索路径](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c) · [PostgreSQL 正则缓存](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/regexp.c) · [SQLite 命令行工具](https://www.sqlite.org/cli.html#querying_the_database_schema) · [DuckDB DESCRIBE](https://duckdb.org/docs/stable/sql/statements/describe.html)

本项目使用现有目录中的关系编号与名称索引完成搜索路径判断。正则匹配在每个执行线程中保留最近使用的四个模式。普通表的名称查找已经通过原版 `psql` 18.6 验证。 [测试](test-report.md#psql-catalog-discovery-20261124) · [性能对照](performance-report-2026-10-30.md#psql-catalog-discovery-cost-20261124)

<a id="scram-libpq-comparison-20261124"></a>
## 2026-11-24 · 原版 psql 通过 SCRAM 登录

PostgreSQL 从连接启动消息选择账户。libpq 在 SCRAM 第一条消息中将用户名留空。服务端的认证挑战和最终签名使用消息长度界定数据。DuckDB 的 PostgreSQL 扩展使用 libpq 连接参数。SQLite 由调用进程直接打开数据库文件。 [PostgreSQL 18 SASL 认证](https://www.postgresql.org/docs/18/sasl-authentication.html) · [PostgreSQL 18 消息格式](https://www.postgresql.org/docs/18/protocol-message-formats.html) · [DuckDB PostgreSQL 扩展](https://duckdb.org/docs/current/core_extensions/postgres.html) · [SQLite 进程模型](https://www.sqlite.org/serverless.html)

本项目接受 libpq 留空的认证用户名，并按协议长度发送挑战和签名。认证仍基于启动账户的验证器，客户端显式填写非空用户名时还会核对它是否与启动账户一致。实际对照覆盖原版 `psql` 18.6 的本地 TCP 登录、失败密码及分页查询。 [协议验证](test-report.md#scram-libpq-20261124) · [计时](performance-report-2026-10-30.md#scram-libpq-cost-20261124)

<a id="sql-bound-page-comparison-20261124"></a>
## 2026-11-24 · 用 SQL 准备并复用分页查询

PostgreSQL `PREPARE` 分析查询并保留参数声明。`EXECUTE` 先计算参数，再根据缓存设置和使用情况选择具体计划或通用计划。规划器可用已知的行数和偏移估计读取量。SQLite 的客户端可以调用 `sqlite3_bind_int64` 给已经准备的语句绑定整数。DuckDB 允许用 SQL 准备语句，并通过 `EXECUTE` 传入位置参数。 [PostgreSQL 18 PREPARE](https://www.postgresql.org/docs/18/sql-prepare.html) · [PostgreSQL 18.6 EXECUTE](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/prepare.c#L149-L182) · [SQLite 参数绑定](https://www.sqlite.org/c3ref/bind_blob.html) · [DuckDB 预编译语句](https://duckdb.org/docs/current/sql/query_syntax/prepared_statements)

本项目把 SQL `EXECUTE` 的参数求值结果直接交给普通 `SELECT` 的页大小、偏移和 `oid` 范围读取。`EXPLAIN EXECUTE` 可以查看这次运行的读取方式。已有的事务可见性和索引适用条件同样生效。该执行方式每次使用当次参数形成计划，通用计划缓存和任意分页表达式仍有待实现。 [功能验证](test-report.md#sql-bound-page-20261124) · [性能对照](performance-report-2026-10-30.md#sql-bound-page-cost-20261124)

<a id="bound-row-count-comparison-20261124"></a>
## 2026-11-24 · 应用传入分页行数

PostgreSQL 将 `LIMIT` 和 `OFFSET` 的表达式转换为 `bigint`，执行时先取偏移再取行数。绑定的空行数表示不限制，空偏移按 0 处理。`FETCH … WITH TIES` 中直接写 `NULL` 会报错，参数传 `NULL` 可以返回余下全部记录。SQLite 同样允许行数表达式，但其 `LIMIT NULL` 会报错，负上限表示不限制。DuckDB 提供预编译语句的位置参数以及 `LIMIT` 和 `OFFSET`。 [PostgreSQL 18.6 类型转换](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_clause.c#L1743-L1785) · [PostgreSQL 18.6 执行](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c#L328-L413) · [SQLite 行数规则](https://www.sqlite.org/lang_select.html#the_limit_clause) · [DuckDB 预编译语句](https://duckdb.org/docs/current/sql/query_syntax/prepared_statements) · [DuckDB 查询末尾子句](https://duckdb.org/docs/current/sql/statements/select)

本项目在普通外层 `SELECT` 和其 `EXPLAIN` 中确认直接分页参数的类型和位置，再将每次绑定的小页数交给原有查询计划。编号过滤或投影中再次使用同一参数时保留原来的类型。执行检查覆盖负值、偏移与行数同时为负时的报错顺序，以及并列结果。任意行数表达式、嵌套子查询中的分页参数及更一般的重复参数类型推断仍有差距。 [验证范围](test-report.md#bound-row-count-20261124) · [性能测试](performance-report-2026-10-30.md#bound-row-count-cost-20261124)

<a id="oid-page-comparison-20261124"></a>
## 2026-11-24 · 按对象编号排序并取前几条

PostgreSQL 可直接沿 B 树正读或倒读，配合 `LIMIT` 时可在读够行数后停止。SQLite 也可以沿索引顺序读出数据，省去排序。DuckDB 的 TopN 优化在扫描输入时维护所需的最前几行，减少排序开销。 [PostgreSQL 18 索引排序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [SQLite 查询规划](https://www.sqlite.org/queryplanner.html#sorting_by_index) · [DuckDB 查询优化](https://duckdb.org/2024/11/14/optimizers)

本项目的普通表在单个 `oid` 列上有完整 B 树时，沿编号顺序取够 `OFFSET + LIMIT` 行即可停止。`WITH TIES` 会继续读取相同编号，直到读到下一种编号或查完范围。读出的记录仍会检查事务可见性和实际编号。当前索引省略空值，因此需要查空值组成的前缀或并列组时会扫描表。交叉列筛选、多列排序、行级安全策略和无法使用索引检查事务可见性的情况也保留表扫描。 [结果及执行计划验证](test-report.md#oid-page-20261124) · [性能对照](performance-report-2026-10-30.md#oid-page-cost-20261124)

<a id="oid-range-comparison-20261124"></a>
## 2026-11-24 · 用索引查一段对象编号

PostgreSQL 18.6 的 B 树通过 `oid_ops` 支持等值和四种大小比较，按无符号 32 位编号排序。SQLite 为 `BETWEEN` 生成两个可用于索引的边界。DuckDB 根据每个数据块的最小值和最大值跳过不可能匹配的块。 [PostgreSQL 操作符类](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_amop.dat#L159-L169) · [PostgreSQL 的编号比较](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/oid.c#L299-L332) · [SQLite 范围优化](https://sqlite.org/optoverview.html#the_between_optimization) · [DuckDB 数据块过滤](https://duckdb.org/docs/1.4/guides/performance/indexing#zonemaps)

本项目合并同一普通表列上的范围边界，在完整的单列 B 树中找出候选位置。实际读取仍核对可见性和表中存储的值。候选位置分布在过多表页时保留表扫描。混合条件和宽范围仍有进一步优化空间。 [功能验证](test-report.md#oid-range-20261124) · [性能对照](performance-report-2026-10-30.md#oid-range-cost-20261124)

<a id="oid-column-comparison-20261124"></a>
## 2026-11-24 · 在普通表保存对象编号

PostgreSQL 的 `oid` 是 4 字节无符号对象编号。文本输入支持十进制、十六进制和八进制，也接受可用 32 位有符号整数表示的负值作为原始位模式。DuckDB 的 `UINTEGER` 同样用 4 字节保存完整的无符号范围。SQLite 用整数存储类，按值选择磁盘上的编码宽度。 [PostgreSQL 对象编号](https://www.postgresql.org/docs/18/datatype-oid.html) · [PostgreSQL 18.6 文本输入](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/numutils.c#L852) · [DuckDB 整数类型](https://duckdb.org/docs/lts/sql/data_types/numeric) · [SQLite 存储类型](https://www.sqlite.org/datatype3.html)

本项目复用已有的对象编号读写逻辑，普通表可保存整个无符号范围，可按数值排序、创建主键或单列索引。`oid[]` 和其他对象引用类型的完整目录解析仍待实现。范围索引见上方的后续更新。 [功能验证](test-report.md#oid-column-20261124) · [性能对照](performance-report-2026-10-30.md#oid-column-cost-20261124)

<a id="integer-cast-join-limit-comparison-20261123"></a>
## 2026-11-23 · 连接得到足够结果后停止读行

PostgreSQL 18.6 的执行器按 `LIMIT` 向子计划取行，规划时也会考虑返回首批行前的开销。SQLite 的子查询可以通过协程逐行交给外层，也可以在需反复读取时暂存结果。DuckDB 的流式限制算子按批处理，取够所需行后就结束。 [PostgreSQL 执行器](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c) · [PostgreSQL 计划示例](https://www.postgresql.org/docs/18/using-explain.html) · [SQLite 子查询协程](https://www.sqlite.org/optoverview.html#subquery_co_routines) · [DuckDB 流式限制算子](https://github.com/duckdb/duckdb/blob/v1.4.4/src/execution/operator/helper/physical_streaming_limit.cpp)

PostgreSQL 把 `smallint` 和 `integer` 转成 `oid` 时按 32 位无符号编号解释，`bigint` 则必须位于 0 到 4,294,967,295 之间。本项目的受测整数转换采用同样的结果和错误码。 [PostgreSQL 转换表](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_cast.dat) · [PostgreSQL bigint 转换](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/int8.c#L1311)

本项目让已绑定的整数等值连接直接使用两侧的查询结果。子查询可直接转换作为连接键的整数列。子查询只在外层需要时读行，连接仍可重用已读的右侧行，排序和筛选继续由 SQL 计划处理。完整的连接顺序和启动代价选择仍待实现。 [功能验证](test-report.md#integer-cast-join-limit-20261123) · [性能对照](performance-report-2026-10-30.md#integer-cast-join-limit-cost-20261123)

<a id="oid-join-comparison-20261123"></a>
## 2026-11-23 · 用表编号连接目录

PostgreSQL 18.6 给 `pg_inherits` 创建了以子表编号和父表序号为键的 B 树，也给父表编号单独建索引。是否从索引查找由查询计划选择。SQLite 在预计有大量重复查找时可创建语句级 B 树。DuckDB 可以先按连接键建立哈希表，再用另一侧的键查找候选行。 [PostgreSQL 18.6 目录索引](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_inherits.h) · [PostgreSQL 执行计划](https://www.postgresql.org/docs/18/using-explain.html) · [SQLite 临时索引](https://www.sqlite.org/optoverview.html#automatic_query_time_indexes) · [DuckDB 哈希连接](https://duckdb.org/2022/05/27/iejoin)

本项目沿用整数等值连接已有的查找方法。完成对右侧的首次读取后，连续编号可以直接定位行，其他编号可以在内存用量允许时通过已排序的键定位。原来的等值条件继续核对候选。目录查询仍会读取各输入的数据和表结构。 [测试报告](test-report.md#oid-join-20261123) · [性能报告](performance-report-2026-10-30.md#oid-join-cost-20261123)

<a id="pg-inherits-comparison-20261123"></a>
## 2026-11-23 · 查询直接表继承关系

PostgreSQL 的 `pg_inherits` 为每个直接父子关系返回表编号与父表顺序，也覆盖分区索引及分区正在解除的状态。SQLite 的 `sqlite_schema` 记录表、索引、视图和触发器，DuckDB 提供查询表和约束等信息的目录函数。 [PostgreSQL pg_inherits](https://www.postgresql.org/docs/18/catalog-pg-inherits.html) · [SQLite 目录](https://www.sqlite.org/schematab.html) · [DuckDB 目录函数](https://www.duckdb.org/docs/1.5/sql/meta/duckdb_table_functions)

本项目通过同一份继承记录向查询和系统目录提供普通表之间的直接关系。解除继承后保留其余父表的原序号，重新加入时取当前最大序号加一。目录返回 `pg_class` 里实际的表编号。已有分区使用父表内部的存储文件，独立分区表和分区索引仍需接入。 [功能和恢复验证](test-report.md#pg-inherits-20261123) · [性能对照](performance-report-2026-10-30.md#pg-inherits-cost-20261123)

<a id="qualified-inheritance-comparison-20261123"></a>
## 2026-11-23 · 继承关系按完整表名选择父表

PostgreSQL 的 `INHERITS` 会保存父子关系，普通父表查询包含子表数据，未限定的名字由搜索路径解析。SQLite 和 DuckDB 的 `CREATE TABLE AS SELECT` 提供按查询结果创建独立表的方式，DuckDB 还给出了只复制列名和类型的写法。 [PostgreSQL 建表](https://www.postgresql.org/docs/18/sql-createtable.html) · [PostgreSQL 搜索路径](https://www.postgresql.org/docs/18/ddl-schemas.html#DDL-SCHEMAS-PATH) · [SQLite 建表](https://www.sqlite.org/lang_createtable.html) · [DuckDB 建表](https://duckdb.org/docs/lts/sql/statements/create_table)

本项目的建表、加入和移除继承现在可用 schema 限定的父表名。重启后仍按写入时选中的表读取，修改搜索路径可以影响后续建表。受测范围是普通表的命名、绑定和持久化。完整目录接口、约束和权限传播仍有差距。 [测试报告](test-report.md#qualified-inheritance-20261123) · [性能报告](performance-report-2026-10-30.md#qualified-inheritance-cost-20261123)

<a id="timetz-ties-comparison-20261123"></a>
## 2026-11-23 · 按时间分页时从索引读取页尾并列记录

PostgreSQL 的 `WITH TIES` 在正常页尾后继续向下游取行，直到排序键变化。兼容排序的 B 树能按需提供这些行。SQLite 也可以按索引顺序读，DuckDB 的 TopN 则在排序输入中只保留当前领先的一部分。 [PostgreSQL 执行器源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c) · [SQLite 索引排序](https://www.sqlite.org/queryplanner.html#sorting_by_index) · [DuckDB TopN](https://duckdb.org/2024/11/14/optimizers#optimizers)

本项目可以在普通单列 `TIMETZ` 查询用索引读到 `OFFSET` 之后的页尾，继续核对同键记录，并将第一条不同记录交给现有分页执行器判断。排序键仍按换算后的 UTC 时钟和偏移区分。索引跳过空值，因此空值可能影响边界时会读取完整输入。候选行、索引页和内存都有上限。覆盖索引、多列排序的提前停止仍有差距。 [测试报告](test-report.md#timetz-ties-20261123) · [性能报告](performance-report-2026-10-30.md#timetz-ties-cost-20261123)

<a id="timetz-page-comparison-20261123"></a>
## 2026-11-23 · 按带偏移的时间翻页

PostgreSQL 可以正向或反向读 B 树，在找到 `LIMIT` 所需的行数后停下。SQLite 也可正反向读取索引来满足排序。DuckDB 的 TopN 在输入中保留当前最靠前的 N 行，以减少排序和内存开销。 [PostgreSQL 索引与排序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [SQLite 索引排序](https://www.sqlite.org/queryplanner.html#sorting_by_index) · [DuckDB TopN](https://duckdb.org/2024/11/14/optimizers#optimizers)

本项目在普通单列 `TIMETZ` 的小页查询中，用索引读到 `OFFSET + LIMIT` 所需的行，再从表里核对完整键和可见性。常规排序和行数限制仍处理这批已经缩小的候选。由于现有索引跳过空值，空值可能影响页首或页尾时继续使用完整排序。带并列记录的分页见上方更新。 [功能验证](test-report.md#timetz-page-20261123) · [性能对照](performance-report-2026-10-30.md#timetz-page-cost-20261123)

PostgreSQL 选索引时使用计划器已收集的表索引信息，DuckDB 在已有的逻辑计划中识别可合并的排序和行数限制。本项目的普通小页查询也由执行计划根据已解析的列类型选择索引。移除了针对带偏移时间的额外表结构查询，整数主键排序也随之少做一次查询。 [PostgreSQL 收集索引信息](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/util/plancat.c) · [PostgreSQL 选择索引路径](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/path/indxpath.c) · [DuckDB TopN 实现](https://github.com/duckdb/duckdb/blob/main/src/optimizer/topn_optimizer.cpp)

<a id="timetz-range-comparison-20261123"></a>
## 2026-11-23 · 用索引查找一段带偏移的时间

PostgreSQL 可以用 B 树查找小于、大于或介于两个端点之间的值。`TIMETZ` 先比较换算后的 UTC 时钟，再比较偏移。SQLite 分析 `BETWEEN` 时为索引生成两个边界。DuckDB 可以用数据块的最小值和最大值跳过范围之外的块。 [PostgreSQL B 树](https://www.postgresql.org/docs/18/indexes-types.html#INDEXES-TYPES-BTREE) · [PostgreSQL 18.6 时间比较实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/date.c#L2368) · [SQLite 范围优化](https://www.sqlite.org/optoverview.html#the_between_optimization) · [DuckDB 数据块过滤](https://duckdb.org/docs/1.4/guides/performance/indexing#zonemaps)

本项目可为普通单列索引合并同列的上下界。读取会限制访问的索引页和候选行数量，按实际表数据核对时间值与事务可见性。包含大量候选行或仍在读取旧版本时可以改走表扫描。 [功能验证](test-report.md#timetz-range-20261123) · [性能对照](performance-report-2026-10-30.md#timetz-range-cost-20261123)

<a id="timetz-index-comparison-20261123"></a>
## 2026-11-23 · 用索引查找指定的带偏移时间

PostgreSQL 18.6 给 `TIMETZ` 定义了 B 树比较规则，可用于等值和范围查询。普通索引读取用行位置访问数据页并检查记录可见性。SQLite 的普通索引也根据行号读取数据页。DuckDB 的单列 ART 索引用于符合条件的高选择性等值和 `IN` 查询。 [PostgreSQL 18.6 操作符类](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_opclass.dat#L129) · [PostgreSQL 索引读取](https://www.postgresql.org/docs/18/indexes-index-only-scans.html) · [SQLite 按索引查找](https://www.sqlite.org/queryplanner.html#lookup_by_index) · [DuckDB 索引](https://duckdb.org/docs/1.4/guides/performance/indexing#art-index-scans)

本项目在普通表的单列 `TIMETZ` 等值查找中复用现有 B 树和行可见性检查。查到行位置后核对实际值。只有索引覆盖整列且能服务当前视图时，查询才可以从空的查找结果直接返回。历史快照使用数据页上的旧版本。部分索引的条件推断及仅从索引读取仍有差距。单列范围查询见上方本轮更新。 [功能验证](test-report.md#timetz-index-20261123) · [性能对照](performance-report-2026-10-30.md#timetz-index-cost-20261123)

<a id="timetz-storage-comparison-20261123"></a>
## 2026-11-23 · 保存带 UTC 偏移的一天内时间

PostgreSQL 18.6 分别保存本地微秒和 UTC 偏移，排序先比较换算后的时钟，再按向西偏移的秒数区分同一时刻的值。DuckDB 提供带偏移的 `TIMETZ`，其文档示例以 UTC 显示。SQLite 日期时间函数将输入中的偏移换算到 UTC，可以用文本、实数或整数列保存日期时间。 [PostgreSQL 比较实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/date.c#L2368) · [DuckDB 时间类型](https://duckdb.org/docs/1.4/sql/data_types/time) · [SQLite 日期时间存储](https://www.sqlite.org/datatype3.html#date_and_time_datatype)

本项目的 `TIMETZ` 标量列用 12 字节记录本地时钟和偏移。普通表写入时按列精度舍入，同样的比较规则用于筛选、排序和索引键。会话时区推断、命名时区输入与数组存储还有差距。 [功能验证](test-report.md#timetz-storage-20261123) · [性能报告](performance-report-2026-10-30.md#timetz-storage-cost-20261123)

<a id="timetz-precision-comparison-20261123"></a>
## 2026-11-23 · 带 UTC 偏移的时间精度

PostgreSQL 18.6 保存本地时钟和 UTC 偏移，转换到 `TIMETZ(p)` 时只舍入本地时钟。二进制表示为本地微秒数和向西偏移的秒数。DuckDB 也有微秒精度的 `TIMETZ`，其文档示例的显示结果换算为 UTC。SQLite 的日期时间函数使用 UTC，将输入中的偏移用于换算。 [PostgreSQL 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/date.c#L2207) · [DuckDB 时间类型](https://duckdb.org/docs/1.4/sql/data_types/time) · [SQLite 日期时间函数](https://www.sqlite.org/lang_datefunc.html#time_values)

本项目在转换明确写出数值偏移的时间时舍入小数，并支持协议中明确声明的 `TIMETZ` 标量参数。列写入在后续通过同一解析器实现。 [使用说明](MANUAL.md#日期时间类型) · [转换测试](test-report.md#timetz-precision-20261123)

<a id="temp-prepare-subqueries-comparison-20261123"></a>
## 2026-11-23 · 准备语句时检查子查询引用的临时表

PostgreSQL 18.6 在事务中记住临时对象的访问，并拒绝对该事务执行 `PREPARE TRANSACTION`。SQLite 的临时表属于创建它的连接，关闭连接时销毁临时数据库。DuckDB 的临时表也随连接结束而删除。 [PostgreSQL 准备事务说明](https://www.postgresql.org/docs/18/sql-prepare-transaction.html) · [PostgreSQL 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/transam/xact.c#L2419) · [SQLite 临时数据库](https://www.sqlite.org/tempfiles.html#temp_databases) · [DuckDB 临时表](https://duckdb.org/docs/1.4/sql/statements/create_table#temporary-tables)

本项目在 SQL `PREPARE` 和协议 `Parse` 中检查子查询、表函数的参数、行数限制及 `WITH` 主体等位置。拒绝准备事务时沿用现有的顶层回滚。完整的两阶段提交资源恢复仍需继续验证。 [协议与恢复测试](test-report.md#temp-prepare-subqueries-20261123)

<a id="extended-query-syntax-comparison-20261123"></a>
## 2026-11-23 · 扩展查询在一条消息内共用语法分析

PostgreSQL 18.6 在收到 `Parse` 消息后完成语法和参数分析，存入客户端命名或未命名的语句。SQLite 的 `sqlite3_prepare_v2` 把 SQL 编译为可执行的语句对象。DuckDB 支持先准备带参数的语句，再提供实际值执行。 [PostgreSQL 18.6 消息处理源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/tcop/postgres.c#L1403) · [PostgreSQL 扩展查询流程](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY) · [SQLite 语句编译](https://www.sqlite.org/c3ref/prepare.html) · [DuckDB 预编译语句](https://duckdb.org/docs/1.4/sql/query_syntax/prepared_statements)

本项目让同一条协议消息中的语法检查和字段说明共用解析结果。绑定和目录检查继续按各自的阶段运行。128 行 VALUES 的准备耗时从约 13 毫秒降到约 5 毫秒。同轮 PostgreSQL 18.6 约需 0.2 毫秒。 [测试与解析次数](test-report.md#extended-query-syntax-20261123) · [性能对照](performance-report-2026-10-30.md#extended-query-syntax-cost-20261123)

<a id="between-bind-types-comparison-20261123"></a>
## 2026-11-23 · 参数类型由第一项范围比较决定

PostgreSQL 把 `BETWEEN` 按顺序展开成两项比较。第一项比较可确定参数类型，后续位置读取同一个类型。SQLite 的参数在运行时绑定，两个比较可分别应用类型亲和性。DuckDB 的范围比较会把三个输入转换为同一类型。 [PostgreSQL 18.6 范围比较](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_expr.c#L1231) · [PostgreSQL 18.6 参数绑定](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_param.c#L170) · [SQLite 类型亲和性](https://www.sqlite.org/datatype3.html#type_conversions_prior_to_comparison) · [DuckDB 范围比较](https://duckdb.org/docs/1.4/sql/expressions/comparison_operators#between-and-is-not-null)

本项目在已支持的单值整数和位串范围查询中，先从第一项比较确定参数的输入类型，再按实际 SQL 编号报告类型。完整的参数推断和改写规则仍有差距。 [协议对照](test-report.md#between-bind-types-20261123) · [准备与执行用时](performance-report-2026-10-30.md#between-bind-types-cost-20261123)

<a id="primary-key-projection-comparison-20261123"></a>
## 2026-11-23 · 按主键翻页时跳过未用的长文本

PostgreSQL 通常在需要某个值时才读取另存的 TOAST 数据。SQLite 能从包含所需列的覆盖索引直接返回结果。DuckDB 读取 Parquet 时可以只读取查询需要的列。本项目会在有序单列整数主键读取和部分小范围读取中跳过未使用的外部值，继续从表页检查行及事务可见性。覆盖索引和可见性映射仍需验证和实现。 [PostgreSQL TOAST](https://www.postgresql.org/docs/18/storage-toast.html) · [PostgreSQL 18.6 读取源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/common/detoast.c) · [SQLite 覆盖索引](https://www.sqlite.org/queryplanner.html#covidx) · [DuckDB 列读取](https://duckdb.org/docs/1.4/data/parquet/overview#partial-reading) · [性能对照](performance-report-2026-10-30.md#primary-key-projection-cost-20261123)

<a id="ordered-heap-pages-comparison-20261123"></a>
## 2026-11-23 · 有序主键分页可以按表页合并读取

PostgreSQL 的位图扫描会按表页访问候选，查询要求索引顺序时还要排序。仅索引扫描可在表页的可见性标记满足条件时省去读表。SQLite 的覆盖索引可直接按键顺序取所需列，DuckDB 的列式执行可以按块读取，并跳过无需查看的数据。本项目仍在同一事务快照下核对各条记录，可以先把本批候选按表页分组，读取后再还原主键顺序。仅索引读取、覆盖列以及可见性映射的维护仍有差距。 [PostgreSQL 位图扫描](https://www.postgresql.org/docs/18/indexes-bitmap-scans.html) · [PostgreSQL 仅索引扫描](https://www.postgresql.org/docs/18/indexes-index-only-scans.html) · [SQLite 覆盖索引](https://www.sqlite.org/queryplanner.html#covidx) · [DuckDB 索引与数据块](https://www.duckdb.org/docs/1.5/guides/performance/indexing) · [取页对照](performance-report-2026-10-30.md#ordered-heap-pages-cost-20261123)

<a id="integer-primary-or-comparison-20261123"></a>
## 2026-11-23 · `OR` 连接的整数范围可以合并读取

PostgreSQL 18.6 查找能分别用索引的 `OR` 分支，可将它们的行位置合并成位图，再按表页访问。SQLite 尝试把同列等值条件改写为 `IN`，其他适用分支可以分别执行索引查找。DuckDB 可把同列常量比较交给扫描器，连续整数等值条件可以合成一段范围，其他条件可以用于跳过数据块。 [PostgreSQL 路径选择源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/path/indxpath.c) · [PostgreSQL 位图扫描](https://www.postgresql.org/docs/18/indexes-bitmap-scans.html) · [SQLite 查询优化](https://www.sqlite.org/optoverview.html#or_optimizations) · [DuckDB 筛选合并源码](https://github.com/duckdb/duckdb/blob/main/src/optimizer/filter_combiner.cpp)

本项目对同一整数主键的 `OR` 合并重叠和相邻的范围，共用一次快照读取。候选很多或位置分散时，可以在读表时按这些范围直接筛选。跨列或带小数的两类主键排序查询继续走表达式筛选，并按整数顺序排序。通用多索引位图和依赖统计信息的选择仍待覆盖。 [对照测试](test-report.md#integer-primary-or-20261123) · [性能对照](performance-report-2026-10-30.md#integer-primary-or-cost-20261123)

<a id="explicit-between-comparison-20261123"></a>
## 2026-11-23 · 显式指定 `BETWEEN` 是否考虑端点顺序

PostgreSQL 18.6 将 `SYMMETRIC` 展开为两组比较，分别用原顺序和反向顺序进行判断。对空值使用 SQL 的三值逻辑，执行时可能重复求值。`ASYMMETRIC` 与普通 `BETWEEN` 含义相同。SQLite 文档中的普通 `BETWEEN` 只对被比较的值求值一次。DuckDB 的优化器可以把同一列的常量上下界合并为范围。 [PostgreSQL 比较谓词](https://www.postgresql.org/docs/18/functions-comparison.html) · [PostgreSQL 18.6 转换源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_expr.c) · [SQLite 范围比较](https://www.sqlite.org/lang_expr.html#the_between_operator) · [DuckDB 筛选合并源码](https://github.com/duckdb/duckdb/blob/main/src/optimizer/filter_combiner.cpp)

本项目的两种显式范围模式复用现有比较和索引读取。可确认的非空整数端点在规划时确定顺序，动态端点按实际比较求值。已验证普通筛选、分页、现有整数范围分区、约束和重启后的视图。更广泛的比较类型和通用索引合并仍待覆盖。 [对照验证](test-report.md#explicit-between-20261123) · [查询用时](performance-report-2026-10-30.md#explicit-between-cost-20261123)

<a id="disjoint-integer-primary-comparison-20261123"></a>
## 2026-11-23 · 用主键索引查找不相连的几段编号

PostgreSQL 可以把不同索引扫描得到的行位置合并成位图。位图按表页顺序访问，查询要求索引键排序时还要排序。SQLite 可对分别满足索引条件的 `OR` 分支进行查找，再按行位置合并，并通过估算选择路径。DuckDB 用最小值和最大值跳过无关数据块，单列 ART 索引面向高选择性的等值及 `IN` 查询。 [PostgreSQL 位图扫描](https://www.postgresql.org/docs/18/indexes-bitmap-scans.html) · [SQLite 查询优化](https://www.sqlite.org/optoverview.html#or_optimizations) · [DuckDB 索引](https://www.duckdb.org/docs/1.5/guides/performance/indexing)

本项目从常量整数 `NOT BETWEEN` 及附加边界算出单列主键仍可取到的几段编号。读全部匹配项时，共用候选数量和表页覆盖限制。只按主键取一页时，沿指定方向读够就停。通用 `OR` 的索引合并、多索引位图和基于统计信息的代价估计仍有差距。 [读行验证](test-report.md#disjoint-integer-primary-20261123) · [性能对照](performance-report-2026-10-30.md#disjoint-integer-primary-cost-20261123)

<a id="integer-not-between-comparison-20261123"></a>
## 2026-11-23 · 读取整数范围以外的数据

PostgreSQL 把普通 `NOT BETWEEN` 转成两个由 `OR` 连接的不等式，并可根据分区边界排除无关分区。SQLite 对每项都能用索引的部分 `OR` 条件，可以分别读取再合并。DuckDB 在列存表上用数据块统计信息判断哪些块可跳过。 [PostgreSQL 比较语法源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_expr.c) · [PostgreSQL 分区文档](https://www.postgresql.org/docs/18/ddl-partitioning.html#DDL-PARTITION-PRUNING) · [SQLite 查询优化](https://www.sqlite.org/optoverview.html) · [DuckDB 块统计跳过](https://github.com/duckdb/duckdb/blob/main/src/storage/table/row_group.cpp)

这一版复用原有的整数分区判断。普通表上，若其他条件和主键的取值范围把结果缩成一个连续区间，就可直接定位主键。剩下两段时，这一版仍读取整表并直接比较整数。 [验证](test-report.md#integer-not-between-20261123) · [性能](performance-report-2026-10-30.md#integer-not-between-cost-20261123) · [后续的多段读取](#disjoint-integer-primary-comparison-20261123)

<a id="integer-between-range-comparison-20261123"></a>
## 2026-11-23 · 将 `BETWEEN` 交给范围读取

PostgreSQL 18 的语法分析会把普通 `BETWEEN` 转换成包含两端的两个比较。SQLite 为索引选择增加等价的上界和下界，在两个条件都用到索引时可省去运行时的原条件。DuckDB 的筛选合并器会把常量端点交给比较条件合并。 [PostgreSQL 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_expr.c) · [SQLite 查询优化](https://www.sqlite.org/optoverview.html#the_between_optimization) · [DuckDB 源码](https://github.com/duckdb/duckdb/blob/main/src/optimizer/filter_combiner.cpp)

本项目在普通表的有符号整数读路径中提取可确认的上下界，同时保留原 SQL 表达式供其他路径使用。适用主键索引时，`BETWEEN` 与两个比较可以读取同样的候选行。对任意可比较类型的范围选路、分区裁剪和更通用的代价判断仍有差距。 [验证与限制](test-report.md#integer-between-range-20261123) · [性能对比](performance-report-2026-10-30.md#integer-between-range-cost-20261123)

这一版用反向端点对照了显式 `SYMMETRIC` 和 `ASYMMETRIC` 及各自的否定写法。PostgreSQL 18.6 的四条查询均返回布尔值，这一版均返回 `42703`。普通的 `NOT BETWEEN` 在同一组反向及空值端点用例中与 PostgreSQL 一致。 [后续的显式范围语法](#explicit-between-comparison-20261123)

<a id="integer-primary-page-reuse-comparison-20261123"></a>
## 2026-11-23 · 同页的候选记录共用一次表页读取

PostgreSQL 的位图堆扫描让索引标出记录的位置，再按物理顺序读取表页。同一页中还有候选时，继续在页内取下一条。SQLite 的 B 树游标也利用同一页内移动的低成本，表和索引分开的查询仍可能需要按行号查表。DuckDB 在列存表里按一组行检查可见性和筛选条件，再读取这一组需要的列。 [PostgreSQL 执行计划](https://www.postgresql.org/docs/18/using-explain.html) · [PostgreSQL 位图堆扫描源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeBitmapHeapscan.c) · [SQLite 多行查询](https://www.sqlite.org/queryplanner.html#multiple_result_rows) · [DuckDB 表扫描源码](https://github.com/duckdb/duckdb/blob/main/src/storage/table/row_group.cpp)

本项目为同一页上的相邻候选复用一份表页副本，每次最多保留一页。有序读保持主键顺序，无排序的小范围读保持表内顺序。按主键查出的记录分散时，仍可能逐条取页。通用位图扫描的批量读页和基于代价的选择仍需覆盖更多索引和查询。 [测试范围](test-report.md#integer-primary-page-reuse-20261123) · [计时对照](performance-report-2026-10-30.md#integer-primary-page-reuse-cost-20261123)

<a id="complete-integer-range-comparison-20261123"></a>
## 2026-11-23 · 先用主键索引定位范围，再读取记录

PostgreSQL 可为 B-tree 范围比较选择索引扫描或位图堆扫描，命中范围扩大时也可能选择整表扫描。SQLite 根据条件估计可排除多少记录，决定使用哪一个索引。DuckDB 用分组的最小值和最大值跳过区间外的数据，单列 ART 索引另有选择性阈值。本项目先读取符合范围的有序整数键，只有能完整取得所有候选且这些记录集中在少量表页时才从该索引读取记录。 [PostgreSQL 索引类型](https://www.postgresql.org/docs/18/indexes-types.html#INDEXES-TYPES-BTREE) · [PostgreSQL 执行计划](https://www.postgresql.org/docs/18/using-explain.html) · [SQLite 范围优化](https://www.sqlite.org/optoverview.html#range_queries) · [DuckDB 索引](https://www.duckdb.org/docs/1.5/guides/performance/indexing)

<a id="stored-integer-identity-comparison-20261123"></a>
## 2026-11-23 · 化简不改变整数值的运算

DuckDB 的算术化简规则能去掉整数加零、减零和乘一等运算，并保留原返回类型。在 2,048 行、单整数主键的本地对照中，PostgreSQL 18.6 对 `id<=128` 选用主键索引，对 `id+0<=128` 选用顺序扫描。SQLite 3.51.1 的计划也从主键范围访问切换到全表扫描。两个数据库都支持为表达式建立索引。本项目这一轮仍扫描普通表的可见行，把符合条件的整数表达式在扫描中直接按原值比较。 [DuckDB 算术化简](https://github.com/duckdb/duckdb/blob/main/src/optimizer/rule/arithmetic_simplification.cpp) · [PostgreSQL 表达式索引](https://www.postgresql.org/docs/18/indexes-expressional.html) · [SQLite 表达式索引](https://www.sqlite.org/expridx.html)

<a id="stored-where-reuse-comparison-20261123"></a>
## 2026-11-23 · 重用扫描已经完成的条件检查

PostgreSQL 为索引扫描保留尚未满足的条件，索引给出的近似匹配仍会用实际记录复查。SQLite 在生成字节码时可以跳过索引已能保证的条件，并考虑外联接和传递条件的限制。DuckDB 区分完全交给扫描的条件和只下推了一部分的条件。本项目在扫描确认一行满足完整的整数和空值条件后，把检查结果交给同一条语句的上层筛选。缓存行和联接输出会按原条件判断。 [PostgreSQL 索引计划](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/plan/createplan.c) · [PostgreSQL 索引复查](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeIndexscan.c) · [SQLite 条件字节码](https://github.com/sqlite/sqlite/blob/master/src/wherecode.c) · [DuckDB 下推实现](https://github.com/duckdb/duckdb/blob/main/src/optimizer/pushdown/pushdown_get.cpp)

<a id="stored-filter-prefix-comparison-20261123"></a>
## 2026-11-23 · 在构造结果行之前筛选

PostgreSQL 18.6 的扫描执行器先取出一行，检查查询条件，通过后才计算结果列。SQLite 为 `UNION`、`INTERSECT` 和 `EXCEPT` 的每个分支分别生成访问计划，分支可以使用索引只访问满足范围的记录。DuckDB 会将适用的条件下推到表扫描，尽早缩小中间结果。项目这次让已支持的整数和空值检查持续作用于当前扫描，减少交给上层查询的行。测试中的表扫描仍读取可见行。 [PostgreSQL 扫描执行器](https://github.com/postgres/postgres/blob/REL_18_6/src/include/executor/execScan.h) · [SQLite 复合查询计划](https://www.sqlite.org/eqp.html#compound_queries) · [DuckDB 查询优化](https://duckdb.org/2024/11/14/optimizers#filter-pull-up--filter-pushdown) · [本项目性能报告](performance-report-2026-10-30.md#stored-filter-prefix-cost-20261123)

<a id="ordinary-set-comparison-20261123"></a>
## 2026-11-23 · 普通集合运算逐对确定类型

PostgreSQL 按语法树从内向外应用共同类型规则。两个 `NULL` 先合并时，这一对得到文本类型，与外层整数合并会报类型错误。DuckDB 在规划集合运算时逐个给子计划加必要的类型转换。SQLite 复合查询的重复行比较保留值原有的存储类型。本项目把普通 `SELECT` 的逐对类型结果交给同一棵执行树，包括上层用于报告列类型的子查询和只读 `WITH`。[PostgreSQL 共同类型规则](https://www.postgresql.org/docs/18/typeconv-union-case.html) · [DuckDB 转换规划](https://github.com/duckdb/duckdb/blob/main/src/planner/binder/query_node/plan_setop.cpp) · [SQLite 复合查询](https://www.sqlite.org/lang_select.html#compound_select_statements)

<a id="set-input-types-comparison-20261123"></a>
## 2026-11-23 · 集合运算在比较之前转换输入

PostgreSQL 按输出列选择共同类型，并把无类型的常量转换到该类型，再执行合并、求交和求差。DuckDB 先根据各个子查询确定输出类型，再在规划阶段为输入加入相应的转换。SQLite 的复合查询按输入值比较重复行，比较时保留值原有的存储类型。本项目让适用的集合运算读取已经转换的行，然后按类型比较这些值。[PostgreSQL 18.6 分析器](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/analyze.c) · [DuckDB 类型推断](https://github.com/duckdb/duckdb/blob/main/src/planner/binder/query_node/bind_setop_node.cpp) · [DuckDB 转换规划](https://github.com/duckdb/duckdb/blob/main/src/planner/binder/query_node/plan_setop.cpp) · [SQLite 复合查询](https://www.sqlite.org/lang_select.html#compound_select_statements)

<a id="array-set-comparison-20261123"></a>
## 2026-11-23 · 把数组展开的结果与其他查询合并

PostgreSQL 逐层分析集合运算的左右查询，并在执行前为相应的列选择共同类型。DuckDB 的绑定器先分析子查询，再为集合运算确定输出列的类型。SQLite 的复合查询按值比较重复行，比较时保留值原有的存储类型。项目沿用 PostgreSQL 的共同类型规则，并让适用的 `UNION ALL` 由同一个查询计划逐行读取两边的数据。[PostgreSQL 共同类型规则](https://www.postgresql.org/docs/18/typeconv-union-case.html) · [DuckDB 集合运算绑定器](https://github.com/duckdb/duckdb/blob/main/src/planner/binder/query_node/bind_setop_node.cpp) · [SQLite 复合查询](https://www.sqlite.org/lang_select.html#compound_select_statements)

<a id="query-syntax-reuse-comparison-20261123"></a>
## 2026-11-23 · 重用一次查询中的语法信息

PostgreSQL 的普通查询先生成原始语法树，再逐条执行名称和类型分析。DuckDB 客户端上下文从语句迭代器取得已解析的语句，再交给规划器。SQLite 在准备语句时把 SQL 编译成供执行器使用的字节码。本项目在当前调用的结果说明检查中共用一份未绑定的语法树，各检查仍使用各自的目录和类型分析规则。[PostgreSQL 普通查询入口](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/tcop/postgres.c) · [DuckDB 客户端查询入口](https://github.com/duckdb/duckdb/blob/main/src/main/client_context.cpp) · [SQLite 准备语句](https://sqlite.org/c3ref/prepare.html)

<a id="array-query-startup-comparison-20261123"></a>
## 2026-11-23 · 减少 SQL 解析中的重复分词

PostgreSQL 的原始解析入口把词法扫描器交给语法分析器，之后再解析列和类型。DuckDB 的解析器先分词，再用标记游标遍历各条语句并保留原文位置。SQLite 在准备语句时将 SQL 编译成执行所用的字节码。本项目在一次语法解析内复用已有的词法标记，并让数组里的独立数字直接形成常量表达式。[PostgreSQL 原始解析](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parser.c) · [DuckDB 解析](https://github.com/duckdb/duckdb/blob/main/src/parser/parser.cpp) · [SQLite 准备语句](https://sqlite.org/c3ref/prepare.html)

<a id="integer-array-cast-comparison-20261123"></a>
## 2026-11-23 · 数组展开前如何处理元素类型

PostgreSQL 在构造数组时使用已确定的元素类型，逐项展开时保存数组遍历状态。DuckDB 展开列表时按位置读取已求值的子向量。SQLite 的虚拟表先在 `xFilter` 接收输入，再通过游标逐行读取。本项目复用三种内置整数类型的固定定义，避免为每个元素复制数据库目录。元素转换、求值顺序和用户命名类型的查询沿用原规则。[PostgreSQL 数组构造](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/execExprInterp.c) · [PostgreSQL 数组展开](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/arrayfuncs.c) · [DuckDB 展开实现](https://github.com/duckdb/duckdb/blob/main/src/execution/operator/projection/physical_unnest.cpp) · [SQLite 游标接口](https://sqlite.org/vtab.html#the_xfilter_method)

<a id="unnest-from-comparison-20261123"></a>
## 2026-11-23 · 从数组读取数据行

PostgreSQL 在分析 `FROM` 时，把符合条件的多参数 `unnest` 拆成多个内置单参数调用，再按位置合并输出。DuckDB 支持在同一 `SELECT` 中并排展开多个列表，短列表补空值。SQLite 的 `json_each` 可以逐行读取 JSON 数组的直接元素。本项目把 `FROM unnest` 接到已有的函数来源执行逻辑，沿用类型、空值和行号处理。[PostgreSQL 解析实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_clause.c) · [PostgreSQL 数组函数](https://www.postgresql.org/docs/18/functions-array.html) · [DuckDB 列表展开](https://www.duckdb.org/docs/1.5/sql/query_syntax/unnest) · [SQLite `json_each`](https://sqlite.org/json1.html#jeach)

<a id="function-projection-comparison-20261123"></a>
## 2026-11-23 · 传递已经生成的函数列

PostgreSQL 在单函数且结果类型一致时直接把数据读入扫描槽。DuckDB 对直接列引用复用输入向量，只为计算列另行准备输出缓冲。SQLite 的虚拟表通过当前行游标按列返回值。本项目让查询执行器按已绑定的列位置直接提供函数值，并复用同一次执行中已经确定的类型名称。[PostgreSQL 函数扫描](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeFunctionscan.c) · [DuckDB 投影执行](https://github.com/duckdb/duckdb/blob/main/src/execution/operator/projection/physical_projection.cpp) · [SQLite 取列接口](https://sqlite.org/vtab.html#the_xcolumn_method) · [性能测试](performance-report-2026-10-30.md#function-projection-cost-20261123)

<a id="rows-from-comparison-20261123"></a>
## 2026-11-23 · 并排读取多个函数

PostgreSQL 的 `ROWS FROM` 让多个函数按各自的结果顺序并排列出，较短的结果用 `NULL` 补齐。首次取行时，函数扫描会保存各函数的完整结果。DuckDB 在同一 `SELECT` 中展开多个列表时，也会为短列表补空值。SQLite 把 `generate_series` 之类的函数接到虚拟表接口，由虚拟表游标提供行。本项目使用已有的逐行函数执行器组合结果。[PostgreSQL 表函数](https://www.postgresql.org/docs/18/queries-table-expressions.html#QUERIES-TABLEFUNCTIONS) · [PostgreSQL 扫描源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeFunctionscan.c) · [DuckDB 展开列表](https://duckdb.org/docs/lts/sql/query_syntax/unnest) · [SQLite 虚拟表](https://www.sqlite.org/vtab.html)

<a id="series-function-comparison-20261123"></a>
## 2026-11-23 · 从函数读取等差序列

PostgreSQL 的 `generate_series` 支持整数、小数和时间，`WITH ORDINALITY` 会接在结果后面添加从 1 开始的 `bigint` 行号。其普通函数扫描第一次取行时把函数结果存入临时行集。SQLite 的实现用虚拟表逐行生成整数，可在适用时把范围与条数限制交给函数。DuckDB 按批生成结果，包含终点的 `generate_series` 与排除终点的 `range` 各有一套入口。[PostgreSQL 函数](https://www.postgresql.org/docs/18/functions-srf.html) · [PostgreSQL 函数扫描](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeFunctionscan.c) · [SQLite 实现](https://github.com/sqlite/sqlite/blob/master/ext/misc/series.c) · [DuckDB 实现](https://github.com/duckdb/duckdb/blob/main/src/function/table/range.cpp)

本项目在受支持的查询中按需生成序列，`WITH ORDINALITY` 会在筛选和排序前为函数输出编号。参数求值和列说明分别由实际执行与查询绑定完成。函数参数引用同级其他表列的用法仍待补齐。[测试](test-report.md#series-source-20261123) · [性能](performance-report-2026-10-30.md#series-source-cost-20261123)

## 2026-11-23 · 写入 CTE 需要声明可读取的输出

PostgreSQL 和 DuckDB 允许在 `WITH` 里写入数据。后续查询读取的是写入命令的 `RETURNING` 输出。PostgreSQL 仍会执行语句中未被引用的写入。SQLite 将 `RETURNING` 的输出交给客户端，当前语法无法把它接到后续查询。本项目用 PostgreSQL 18.6 核对普通语句中的引用错误、表内容与序列值。[PostgreSQL WITH](https://www.postgresql.org/docs/18/queries-with.html) · [DuckDB WITH](https://duckdb.org/docs/2.0/sql/query_syntax/with) · [SQLite RETURNING](https://sqlite.org/lang_returning.html) · [测试报告](test-report.md#cte-command-visibility-20261123)

## 2026-11-23 · 保留零列查询的结果行

PostgreSQL 允许普通 `SELECT` 省略列清单，并在每条数据行消息中写入零列计数。SQLite 的 `SELECT` 语法要求结果列，DuckDB 的 `FROM` 简写默认选择全部列。此次以 PostgreSQL 的行数、报错和消息顺序为对照。[PostgreSQL SELECT](https://www.postgresql.org/docs/18/sql-select.html) · [PostgreSQL 数据行格式](https://www.postgresql.org/docs/18/protocol-message-formats.html) · [SQLite SELECT](https://sqlite.org/lang_select.html) · [DuckDB FROM](https://duckdb.org/docs/2.0/sql/query_syntax/from)

本项目的普通表、只读 CTE 和派生表中的受支持查询会发送真实空行；无筛选或排序的普通表扫描可以跳过读取大字段。零列表的写入与恢复仍有差距。[测试](test-report.md#zero-width-read-20261123) · [性能](performance-report-2026-10-30.md#zero-width-read-cost-20261123)

## 2026-11-23 · 外层查询按需读取内部结果

PostgreSQL 的普通 `LIMIT` 在达到条数后停止从子节点取行。单次引用且符合条件的只读 CTE 也可与外层合并。SQLite 使用 `MATERIALIZED` 作为查询优化的边界，DuckDB 通常先保留 CTE 并根据使用方式决定是否合并。[PostgreSQL WITH](https://www.postgresql.org/docs/18/queries-with.html) · [PostgreSQL LIMIT 实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c) · [SQLite WITH](https://sqlite.org/lang_with.html) · [DuckDB WITH](https://duckdb.org/docs/2.0/sql/query_syntax/with)

本项目的受支持只读 CTE 和派生表现在按外层需求提供数据，保持内部列类型及 NULL。复杂 CTE 合并、表达式消除和任意查询形状仍待补齐。[测试](test-report.md#logical-read-demand-20261123) · [性能](performance-report-2026-10-30.md#logical-read-demand-cost-20261123)

## 2026-11-23 · 普通查询出错时返回的结果

PostgreSQL 在结果接收器启动时发送列说明，执行中逐行发送数据，发生错误时结束当前查询。SQLite 和 DuckDB 的 C API 也可以分别取得已准备语句的列信息及执行结果，只有 PostgreSQL 的比较涉及网络消息顺序。[PostgreSQL 消息规则](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY) · [PostgreSQL 结果接收器](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/common/printtup.c) · [SQLite 列说明](https://www.sqlite.org/c3ref/column_count.html) · [DuckDB 预编译接口](https://duckdb.org/docs/1.5/clients/c/prepared)

本项目的普通 `Query` 消息已能在受支持的只读查询执行失败时，先发送列说明和已经完成的行，再返回错误。列名、类型及 NULL 由实际结果接收器提供。[协议对照](test-report.md#simple-query-runtime-result-20261123) · [性能](performance-report-2026-10-30.md#simple-query-runtime-result-cost-20261123)

## 2026-11-23 · 判断计数条件是否可能成立

PostgreSQL 区分条件必为 `FALSE` 和条件无法为 `TRUE` 两种结论，并将后者用于识别互相矛盾的筛选。DuckDB 合并同一表达式上的常量比较，发现冲突时可生成空结果。SQLite 在选择表和索引访问条件时，会沿等式关系寻找可以使用的常量比较。[PostgreSQL 条件证明](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/util/predtest.c) · [DuckDB 条件合并](https://github.com/duckdb/duckdb/blob/v1.5.6/src/optimizer/filter_combiner.cpp) · [SQLite 查询条件](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/where.c)

本项目已在普通单表计数中判断受支持的 `AND` 条件是否可能为真。范围越界和比较冲突的普通请求中位数分别为 4.196 和 4.387 毫秒，同轮 PostgreSQL 为 1.064 和 1.092 毫秒。更一般的条件推导仍需覆盖多列关系、连接和更多表达式。[测试](test-report.md#ordinary-count-constraints-20261123) · [性能](performance-report-2026-10-30.md#ordinary-count-constraints-cost-20261123)

## 2026-11-23 · 单表计数

PostgreSQL 的聚合节点随输入行更新聚合状态，整表 `COUNT(*)` 需要扫描表或包含全部行的索引。SQLite 的 `Count` 指令可遍历 B-tree 页来获得精确行数。DuckDB 用批次行数累计 `COUNT(*)`，按列计数时检查有效值。[PostgreSQL 聚合实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeAgg.c) · [PostgreSQL 计数文档](https://www.postgresql.org/docs/18/functions-aggregate.html) · [SQLite 指令](https://www.sqlite.org/opcode.html) · [DuckDB 计数实现](https://github.com/duckdb/duckdb/blob/v1.5.6/src/function/aggregate/distributive/count.cpp)

本项目在普通表行通过快照检查后直接累计受支持的计数，同一个查询里的多个计数共用一次扫描。实测 8,355 行且全部符合筛选条件时，普通请求中位数为 31.299 毫秒，PostgreSQL 为 3.433 毫秒。扫描仍逐行读取并检查数据，列向量聚合、并行聚合、覆盖索引计数和完整的分组聚合能力仍有差距。[测试](test-report.md#ordinary-stored-count-20261123) · [性能对照](performance-report-2026-10-30.md#ordinary-stored-count-cost-20261123)

## 2026-11-23 · 在读取表行时应用已准备的比较条件

PostgreSQL 在执行器初始化时把表达式整理成一组步骤，SQLite 把查询生成字节码后执行。DuckDB 的算子处理固定大小的列向量，并有专门的常量向量表示。[PostgreSQL 执行器](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/README) · [SQLite 字节码](https://www.sqlite.org/opcode.html) · [DuckDB 执行格式](https://duckdb.org/docs/1.5/internals/vector)

本项目把受支持的文字长度、文字和整数比较准备成直接检查，普通表和复合主键分页共用。普通表在取回可见行后筛选，减少送去组装和排序的行。通用表达式求值、列向量处理和任意筛选下推仍有差距。[验证](test-report.md#ordinary-stored-predicate-20261123) · [性能](performance-report-2026-10-30.md#ordinary-stored-predicate-cost-20261123)

## 2026-11-23 · 后续索引批次与数据页复用

PostgreSQL 的 B-tree 扫描可继续访问下一页，其成本模型考虑索引顺序与表中物理顺序的相关性。SQLite 使用游标定位和逐步移动。DuckDB 可使用区段的最小值和最大值排除不相关行组。[PostgreSQL 扫描](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/nbtree/nbtsearch.c) · [PostgreSQL 成本计算](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/path/costsize.c) · [SQLite 游标](https://www.sqlite.org/opcode.html#btree) · [DuckDB 索引](https://duckdb.org/docs/1.5/guides/performance/indexing)

本项目在固定的复合主键组内从上批末尾续读，核对相同物理键处已读过的条目。连续命中同一个数据页时使用同一份页副本，并按相邻或跳跃读页估计成本。结果仍经事务快照和完整键校验。可以通过区间末尾确认无匹配，超出成本或页数预算时回到表扫描。通用游标、跨组顺序和基于统计信息的成本估计仍有差距。[协议测试](test-report.md#composite-primary-refill-20261123) · [性能对照](performance-report-2026-10-30.md#composite-primary-refill-cost-20261123)

## 2026-11-23 · 边筛选边读取有序的复合主键分页

PostgreSQL 可以在索引扫描中追加筛选，`LIMIT` 取够符合条件的行便结束请求。SQLite 会比较有序索引扫描和其他访问路径的成本。DuckDB 将 `ORDER BY` 与 `LIMIT` 合并成 TopN，减少排序所需的内存和工作量。[PostgreSQL 执行计划](https://www.postgresql.org/docs/18/using-explain.html) · [SQLite 排序规划](https://sqlite.org/optoverview.html#order_by_optimizations) · [DuckDB 优化器](https://duckdb.org/2024/11/14/optimizers)

本项目可以在固定复合主键前导列后，检查受支持的文字长度、空值和简单比较，再按整数末列顺序交出满足条件的一页。查找和筛选的成本超过预算时会改用原查询路径。通用筛选、稀疏筛选的成本估计，以及跨组有序读取仍有差距。[协议测试](test-report.md#composite-primary-filtered-order-20261123) · [性能对照](performance-report-2026-10-30.md#composite-primary-filtered-order-cost-20261123)

## 2026-11-23 · 从区间的一端读取一页复合主键结果

PostgreSQL 的 B-tree 支持正向或反向读取，符合排序的索引可在达到 `LIMIT` 时停止。SQLite 在多列索引中先定位前导等值组，再按后续列读取。DuckDB 1.5 文档中的 ART 扫描面向单列筛选，未用于消除排序。[PostgreSQL 索引排序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [SQLite 查找与排序](https://sqlite.org/queryplanner.html#searching_and_sorting_with_a_multi_column_index) · [DuckDB 索引](https://duckdb.org/docs/1.5/guides/performance/indexing)

本项目使用已有的 `0xC556` 格式，从整数范围的一端读取一页，并在返回前核对表里的主键值、顺序和事务可见性。索引存储前导键的摘要，如果读到的候选未能组成完整的一页，可以回到表扫描。当前按索引排序的范围是一组前导等值键。跨组排序、不同列的混合顺序和任意查询的有序索引选择仍有差距。[协议测试](test-report.md#composite-primary-prefix-20261123) · [性能对照](performance-report-2026-10-30.md#composite-primary-prefix-cost-20261123)

## 2026-11-23 · 用已有顺序读取一页复合主键结果

PostgreSQL 可以正向或反向读 B-tree 来满足对应的 `ORDER BY`，并在取够 `LIMIT` 所需的行后结束。SQLite 可以先在多列索引中定位前导等值组，再按后续列顺序读取。DuckDB 1.5 的 ART 查询路径以选择性筛选为主，文档所述的 ART 查询扫描不会消除排序。[PostgreSQL 索引排序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [SQLite 查找与排序](https://sqlite.org/queryplanner.html#searching_and_sorting_with_a_multi_column_index) · [DuckDB 索引](https://duckdb.org/docs/1.5/guides/performance/indexing)

本项目在已有的 `0xC556` 索引上核对前导列并按整数末列顺序输出。执行后确认实际使用了范围索引才省去排序。支持正向和反向读取。简单分页可以在取得 `OFFSET + LIMIT` 条符合条件的行时停止读取表行。索引页读取仍使用有上限的候选遍历。多个前导组间的全局顺序、任意排序键和更大的候选范围仍有差距。[协议测试](test-report.md#composite-primary-order-20261123) · [性能对照](performance-report-2026-10-30.md#composite-primary-order-cost-20261123)

## 2026-11-23 · 表扫描前准备比较条件

PostgreSQL 18.6 在初始化执行器时把查询条件转换成可执行步骤，扫描时读取当前行和参数。SQLite 在生成字节码时翻译筛选条件。DuckDB 的筛选算子用已经绑定的表达式批量选出符合条件的行。[PostgreSQL 执行器](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/README) · [SQLite 架构](https://www.sqlite.org/arch.html) · [DuckDB 筛选算子](https://github.com/duckdb/duckdb/blob/v1.5.6/src/execution/operator/filter/physical_filter.cpp)

本项目在一次扫描开始前准备可确认类型的文本相等比较和有符号整数比较，包括参数落入普通扫描中后的内置类型常量。执行器使用行的实际 NULL 状态和完整值，整数按 64 位比较。复杂表达式、会忽略大小写等区别的比较规则和有错误的转换继续走原有表达式路径。[协议测试](test-report.md#stored-literal-filter-20261123) · [性能对照](performance-report-2026-10-30.md#stored-literal-filter-cost-20261123)

## 2026-11-23 · 给定前导键的范围查询

PostgreSQL 的多列 B-tree 可用前导列的等值和相邻列的范围限制索引扫描，也可按索引顺序输出。SQLite 用多列索引定位第一个匹配项后顺序查找相邻记录，查询前导列等值时可按后一列的索引顺序返回。DuckDB 1.5 的 ART 查询扫描适用于无表达式的单列索引，区间筛选还可借助数据块的最小值和最大值。[PostgreSQL 多列索引](https://www.postgresql.org/docs/18/indexes-multicolumn.html) · [PostgreSQL 索引排序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [SQLite 查找与排序](https://sqlite.org/queryplanner.html#searching_and_sorting_with_a_multi_column_index) · [DuckDB 索引](https://duckdb.org/docs/1.5/guides/performance/indexing)

本项目在 20 字节键中使用前导列摘要和有序的整数末列。确定前导等值键后可只读取期望区间的索引页，原 SQL 仍核对真实值。这轮计时包含显式排序。[正确性与恢复](test-report.md#composite-primary-range-20261123) · [性能对照](performance-report-2026-10-30.md#composite-primary-range-cost-20261123)

## 2026-11-23 · 共享长前缀的复合主键

PostgreSQL 的多列 B-tree 按列比较完整键，支持使用前导列的等值条件及相邻列的范围条件来缩小扫描。其哈希索引只支持单列等值搜索，表中的值仍要复核。SQLite 的 B-tree 记录按列从左到右比较。DuckDB 1.5 用 ART 实施约束，查询中的 ART 索引扫描适用于无表达式的单列索引。[PostgreSQL 多列索引](https://www.postgresql.org/docs/18/indexes-multicolumn.html) · [PostgreSQL 哈希索引](https://www.postgresql.org/docs/18/hash-index.html) · [SQLite 记录排序](https://sqlite.org/fileformat.html#record_sort_order) · [DuckDB 1.5 索引](https://duckdb.org/docs/1.5/guides/performance/indexing)

这轮测试中的 `0xC555` 格式沿用 4 KiB 的 B+ 树页，并保存完整键的固定宽度摘要以定位等值候选。数据库逐列核对表中的原值。事务锁仍按旧前缀选择，同一前缀的写入仍可能等待。[功能验证](test-report.md#composite-primary-fingerprint-20261123) · [与 PostgreSQL 18.6 的计时](performance-report-2026-10-30.md#composite-primary-fingerprint-cost-20261123)

## 2026-11-23 · 用索引定位要修改或删除的行

PostgreSQL 用语句快照选择目标，默认隔离级别下遇到并发修改会等待，再在已提交的新版本上检查原条件。SQLite 的 `UPDATE` 和 `DELETE` 用通用条件规划器定位行，并按语句特点选择直接写入或先收集行标识。DuckDB 将部分更新改写为删除再插入，带主键或唯一约束的表需要维护 ART 索引。[PostgreSQL 事务隔离](https://www.postgresql.org/docs/18/transaction-iso.html) · [SQLite 更新](https://github.com/sqlite/sqlite/blob/master/src/update.c) · [SQLite 删除](https://github.com/sqlite/sqlite/blob/master/src/delete.c) · [DuckDB 索引](https://duckdb.org/docs/1.5/sql/indexes)

本项目可以用完整复合键定位单表修改或删除的候选。语句快照与当前索引对不上时，沿用表扫描。进入写入前仍使用原有行锁及重查流程，带来源表、范围条件及分区表的复合主键写入路径仍有差距。[并发验证](test-report.md#composite-primary-mutation-20261123) · [性能](performance-report-2026-10-30.md#composite-primary-mutation-cost-20261123)

## 2026-11-23 · 多列条件一起定位记录

PostgreSQL 的多列 B-tree 优先根据前导列的等值条件和相邻范围列缩小扫描。SQLite 的多列索引也能同时处理用 `AND` 连接的多个条件。DuckDB 1.5 用 ART 实施主键和唯一约束，文档中的查询索引扫描适用于单列索引。[PostgreSQL 多列索引](https://www.postgresql.org/docs/18/indexes-multicolumn.html) · [SQLite 查询规划](https://www.sqlite.org/queryplanner.html#multi_column_indices) · [DuckDB 1.5 索引](https://duckdb.org/docs/1.5/guides/performance/indexing)

这轮测试中的 `0xC554` B+ 树保存完整复合键的前 20 字节，多个键可能落在同一前缀下。数据库按原表值核对候选。旧快照、部分键和范围查询使用原有扫描。[测试](test-report.md#composite-primary-lookup-20261123) · [性能](performance-report-2026-10-30.md#composite-primary-lookup-cost-20261123)

## 2026-11-23 · 多列一起构成的主键

PostgreSQL 的多列唯一 B-tree 逐列判等，所有索引列相等时才产生冲突，并结合行版本判断唯一性。SQLite 给索引记录保留每列的类型和值，并按列比较。DuckDB 自动创建 ART 索引来实施复合主键和唯一约束。[PostgreSQL 多列唯一索引](https://www.postgresql.org/docs/18/indexes-unique.html) · [PostgreSQL 唯一性检查](https://www.postgresql.org/docs/18/index-unique-checks.html) · [SQLite 记录格式](https://www.sqlite.org/fileformat.html#record_format) · [DuckDB 约束](https://duckdb.org/docs/current/sql/constraints)

这轮测试中的 `0xC554` 索引先按 20 字节前缀查找候选，再读取完整列值，逐列确认重复。`0xC552` 复合索引和会跨物理键判等的比较规则使用表扫描。同一前缀的并发写入可能等待。[测试报告](test-report.md#composite-primary-keys-20261123) · [性能报告](performance-report-2026-10-30.md#composite-primary-keys-cost-20261123)

## 2026-11-23 · 开头相同的长主键

PostgreSQL 的 B-tree 可保存同键的多个物理行版本，约束检查结合完整键和行可见性。SQLite 在索引记录放不下一页时把其余内容放在溢出页。DuckDB 由 ART 按编码键查找，并用它支持主键约束。[PostgreSQL 唯一性检查](https://www.postgresql.org/docs/18/index-unique-checks.html) · [SQLite 索引格式](https://sqlite.org/fileformat.html#b_tree_pages) · [DuckDB 索引](https://duckdb.org/docs/current/guides/performance/indexing)

本项目在普通单列文本主键索引中保留同前缀的行位置，取出可见候选后比较完整值。长前缀下的候选越多，等值查找需要核对的行也越多。复合主键编码和分区表的跨分区约束仍有差距。[测试](test-report.md#long-text-primary-keys-20261123) · [性能](performance-report-2026-10-30.md#long-text-primary-keys-cost-20261123)

## 2026-11-22 · 按键定位索引记录

PostgreSQL 的 B-tree 能根据扫描条件和页边界结束查找。SQLite 按记录格式比较索引键，较长的键可以跨到溢出页。DuckDB 的 ART 按完整编码键定位叶节点，再返回该键的行位置。[PostgreSQL 扫描](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/nbtree/nbtsearch.c#L1713) · [SQLite 索引格式](https://sqlite.org/fileformat.html#b_tree_pages) · [DuckDB ART](https://github.com/duckdb/duckdb/blob/main/src/execution/index/art/art.cpp#L739)

本项目的多行等值查找读取键所在的叶页范围。长度超出 20 字节时，现有索引保存前缀。本项目的二级索引能保留有同一前缀的多个行位置。这一轮试验中，旧主键索引仍会遗漏同一前缀下的其他位置。[历史试验](test-report.md#bounded-btree-equality-20261122) · [性能](performance-report-2026-10-30.md#bounded-btree-equality-cost-20261122)

## 2026-11-22 · 表内文字相等和约束检查

PostgreSQL 对确定性规则下的文本相等比较先检查长度，然后比较字节。`CHAR` 在计算长度时跳过尾部空格。SQLite 的 `BINARY` 和 `RTRIM` 也区分保留或跳过尾部空格的比较。DuckDB 默认使用字节比较，并可另选忽略大小写、重音或规范化的规则。[PostgreSQL 文本](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/varlena.c#L1632) · [PostgreSQL CHAR](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/varchar.c#L701) · [SQLite 排序规则](https://www.sqlite.org/datatype3.html#collating_sequences) · [DuckDB 排序规则](https://duckdb.org/docs/1.5/sql/expressions/collations)

本项目的默认文本相等比较使用同样的字节规则。批量检查唯一键和外键时，适用的列可按完整键查找，从而省去逐行两两比较。同机 512 行无索引文本等值查询在本项目约需 10–12 毫秒，PostgreSQL 18.6 约需 0.26–0.34 毫秒。[测试报告](test-report.md#stored-text-equality-20261122) · [性能报告](performance-report-2026-10-30.md#stored-text-equality-cost-20261122)

## 2026-11-22 · 用文本函数筛选行

PostgreSQL 在初始化表达式时，根据已解析的函数编号准备可重复调用的函数和参数空间。SQLite 在编译表达式时把函数定义写入虚拟机指令。DuckDB 在执行已绑定的函数时按数据块计算参数和结果。[PostgreSQL 表达式执行](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/execExpr.c#L2557) · [SQLite 表达式编译](https://github.com/sqlite/sqlite/blob/master/src/expr.c#L5550) · [DuckDB 函数执行](https://github.com/duckdb/duckdb/blob/main/src/execution/expression_executor/execute_function.cpp)

本项目将受支持的普通文本函数筛选交给已绑定的查询执行器，扫描时直接计算每行的参数。PostgreSQL 的 `substring(... FROM ... FOR ...)` 专用语法指定系统函数。本项目也按此处理，逗号参数形式仍按 `search_path` 解析。[PostgreSQL 语法](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/gram.y#L15019)

同样查找 512 行，新版的截取筛选约需 12–15 毫秒，PostgreSQL 18.6 约需 0.22–0.36 毫秒。普通 `label = ...` 对照在本项目还是 22–24 毫秒。[测试报告](test-report.md#ordinary-text-function-filter-20261122) · [性能报告](performance-report-2026-10-30.md#ordinary-text-function-filter-cost-20261122)

## 2026-11-22 · 文本相等比较

PostgreSQL 18.6 对确定性排序规则下的文本相等和不等比较使用字节比较，大小顺序则使用对应的排序规则。SQLite 区分逐字节比较的 `BINARY`、忽略大小写的 `NOCASE` 和忽略末尾空格的 `RTRIM`。DuckDB 默认逐字节比较，并提供忽略大小写、重音和规范化后比较的选项。[PostgreSQL 文本比较代码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/varlena.c#L1628) · [SQLite 排序规则](https://www.sqlite.org/datatype3.html#collating_sequences) · [DuckDB 排序规则](https://www.duckdb.org/docs/2.0/sql/expressions/collations)

本项目对现有确定性内置规则的文本相等比较直接核对字节。`CHAR` 先执行原有的尾部空格处理。大小顺序、`NOCASE` 和显式系统区域设置继续调用相应的比较规则。[测试报告](test-report.md#text-equality-deterministic-20261122) · [性能报告](performance-report-2026-10-30.md#text-equality-deterministic-cost-20261122)

## 2026-11-22 · 用 SQL 查询当前设置

PostgreSQL 18.6 的 `pg_settings` 是一个普通视图，数据来自 `pg_show_all_settings()`，因此查询可以使用表达式、筛选、分组和连接。SQLite 把只读 PRAGMA 的结果提供为表函数。DuckDB 的 `duckdb_settings()` 也返回可以用普通 SQL 查询的行。[PostgreSQL 系统视图](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/system_views.sql) · [SQLite 表函数](https://www.sqlite.org/pragma.html#pragma_functions) · [DuckDB 设置表函数](https://www.duckdb.org/docs/current/sql/meta/duckdb_table_functions#duckdb_settings)

本项目让已有三个设置字段作为普通查询来源，通过绑定查询确定列信息，执行时才读取当前连接的值。已支持数值转换、排序和连接。设置字段和分组功能仍需补齐。[测试](test-report.md#pg-settings-expressions-20261122) · [性能](performance-report-2026-10-30.md#pg-settings-expressions-cost-20261122)

## 2026-11-22 · 改名后继续使用序列

PostgreSQL 用 `regclass` 保存序列的对象编号。改名后，已经保存的编号仍指向同一序列。`nextval` 分出的值在回滚后仍视作已用。SQLite 的 `AUTOINCREMENT` 按表在 `sqlite_sequence` 中记录编号。DuckDB 的改名回归仍期望 `ALTER SCHEMA ... RENAME TO` 返回功能尚未实现。[PostgreSQL 对象编号](https://www.postgresql.org/docs/18/datatype-oid.html) · [PostgreSQL 序列函数](https://www.postgresql.org/docs/18/functions-sequence.html) · [SQLite 编号分配](https://www.sqlite.org/autoinc.html) · [DuckDB 改名测试](https://github.com/duckdb/duckdb/blob/main/test/sql/alter/rename_schema/rename_schema.test)

本项目在同一事务中移动序列文件、更新列所有权和受支持的授权记录。普通视图可保存序列编号，回滚或恢复时序列继续保留已经发出的值。无法确定旧视图引用的序列时，改名会被拒绝。[测试与限制](test-report.md#schema-rename-sequences-20261122) · [性能报告](performance-report-2026-10-30.md#schema-rename-sequences-cost-20261122)

## 2026-11-22 · 整个 schema 改名

PostgreSQL 18.6 检查 schema 所有权和数据库的建 schema 权限，随后更新原目录记录中的名称，保留对象编号。SQLite 使用已附加数据库的名字限定表。DuckDB 的回归测试要求 `ALTER SCHEMA ... RENAME TO` 返回功能尚未实现的错误。[PostgreSQL 改名实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/schemacmds.c#L230) · [SQLite 对象名称](https://www.sqlite.org/lang_naming.html) · [DuckDB 回归测试](https://github.com/duckdb/duckdb/blob/main/test/sql/alter/rename_schema/rename_schema.test)

本项目的表和视图文件使用 schema 名称。改名沿用原目录编号，把文件、依赖视图和权限记录在同一事务中一起更新。含物化视图、自定义类型或例程的 schema 仍需继续实现。[测试](test-report.md#schema-rename-20261122) · [性能](performance-report-2026-10-30.md#schema-rename-cost-20261122)

## 2026-11-22 · 表改名或迁移后的视图

PostgreSQL 按原对象编号更新表名，视图保存的引用仍指向同一张表。SQLite 会更新受影响的视图定义文本。DuckDB 的表改名条款说明引用该表的视图需要另行更新。[PostgreSQL 改表名实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L4022) · [SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html#alter_table_rename) · [DuckDB ALTER TABLE](https://www.duckdb.org/docs/current/sql/statements/alter_table#rename-table)

本项目通过原对象编号找到依赖视图，再更新定义中绑定到它的表名和列前缀。重新绑定会校验引用的对象、列和输出类型。视图更新与改表名或移动表一起提交。[行为和恢复](test-report.md#view-table-rename-20261122) · [性能](performance-report-2026-10-30.md#view-table-rename-cost-20261122)

## 2026-11-22 · 创建视图时确定来源

PostgreSQL 在创建或替换视图时查找其来源，保存绑定后的查询。SQLite 把持久视图的读取范围限制在同一个数据库。DuckDB 读取视图时用独立的绑定器，并优先查找视图所在的 catalog 和 schema。[PostgreSQL CREATE VIEW](https://www.postgresql.org/docs/18/sql-createview.html) · [SQLite 来源绑定](https://github.com/sqlite/sqlite/blob/master/src/attach.c) · [DuckDB 视图绑定](https://github.com/duckdb/duckdb/blob/main/src/planner/binder/tableref/bind_basetableref.cpp)

本项目保存新建或替换视图时选中的来源 schema。查询按保存的 schema 找表。缺少依赖记录的旧视图和视图的权限规则仍待完成。[正确性与恢复](test-report.md#view-source-identity-20261122) · [性能](performance-report-2026-10-30.md#view-source-identity-cost-20261122)

## 2026-11-22 · 查询时连接普通视图

PostgreSQL 先把视图展开成查询树，再由计划器处理可合并的子查询。SQLite 在满足条件时把视图的查询合并到外层，以减少临时表。DuckDB 的普通视图在被引用时执行保存的查询。[PostgreSQL 视图规则](https://www.postgresql.org/docs/18/rules-views.html) · [SQLite 的子查询合并](https://www.sqlite.org/optoverview.html#subquery_flattening) · [DuckDB 普通视图](https://duckdb.org/docs/sql/statements/create_view)

本项目将已绑定的视图作为连接输入，沿用现有的连接算子。受支持的连接视图可以把行直接交给外层查询。扩展协议从绑定结果取得列描述，描述阶段无需执行视图。[对照范围](test-report.md#join-ordinary-views-20261122)

## 2026-11-22 · 基础表改列名后保留已有视图

PostgreSQL 18.6 更新目录中的列名，视图仍按原表和列编号读取。SQLite 在解析时标出引用的位置，改相关建表 SQL 后检查是否仍能解析。DuckDB 的列改名重新绑定修改后的表定义和约束。[PostgreSQL 改列名](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L3618) · [PostgreSQL 视图规则](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/rewrite/rewriteDefine.c) · [SQLite 改列名](https://www.sqlite.org/lang_altertable.html#alter_table_rename_column) · [DuckDB 改列名](https://github.com/duckdb/duckdb/blob/main/src/catalog/catalog_entry/duck_table_entry.cpp#L391)

本项目按目录里的依赖查找相关普通视图，从绑定结果识别同名列的来源。改写后再次绑定，检查物理依赖、列引用、连接键和输出类型。原有视图目录保留其输出列名，修改后的文件随改名一起提交。[验证范围](test-report.md#rename-dependent-views-20261122)

## 2026-11-22 · 改列名后保留检查和计算

PostgreSQL 18.6 保留列编号，只更新目录中的名称。保存的表达式用列编号定位，显示时读取新名字。SQLite 保留建表 SQL，从已解析的列引用找到需要修改的原文位置。DuckDB 修改检查表达式中的列引用，再绑定新的表定义。[PostgreSQL 改名](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L3618) · [PostgreSQL 表达式显示](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/ruleutils.c#L7161) · [SQLite 改名实现](https://github.com/sqlite/sqlite/blob/master/src/alter.c#L1037) · [DuckDB 改名实现](https://github.com/duckdb/duckdb/blob/main/src/catalog/catalog_entry/duck_table_entry.cpp#L391)

本项目的表结构保存表达式文本。改名从已解析的列引用定位原文，保留字符串、函数名和表名等其他内容，再解析改写后的表达式进行核对。普通表的检查、生成列、事务和重启已分别验证。[测试范围](test-report.md#rename-quoted-column-references-20261122)

## 2026-11-22 · 导入时检查自定义类型的约束

PostgreSQL 18.6 先把字段转换成域的基础类型，再执行域约束。约束拒绝了该值时可跳过该行。检查表达式执行出错时会中止导入。DuckDB 可以用 `CREATE TYPE` 为基础类型起别名，其 CSV 读取也提供跳过转换错误的选项。SQLite 的 `STRICT` 表会拒绝无法无损转换的值。[PostgreSQL 域输入实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/domains.c) · [DuckDB 类型别名](https://duckdb.org/docs/2.0/sql/statements/create_type) · [DuckDB CSV 错误处理](https://duckdb.org/docs/2.0/data/csv/reading_faulty_csv_files) · [SQLite STRICT](https://www.sqlite.org/stricttables.html)

本项目在导入字段中使用当前域定义，核对了标量、继承和数组域，以及被跳过行的默认值求值。新建表能识别写入表结构的域检查，避免对同一输入重复执行。旧表中没有记录来源的检查仍按原来方式执行。[测试范围](test-report.md#copy-domain-input-20261122)

## 2026-11-22 · 跳过字段转换失败的行

PostgreSQL 18.6 的 `ON_ERROR ignore` 可以跳过字段类型转换失败的记录，超过 `REJECT_LIMIT` 时会报错并撤销导入。参考测试中，两个被跳过的记录仍调用了列清单之外的序列默认值。DuckDB 的 CSV 读取还可跳过结构错误，并选择把坏行记入临时表。SQLite 命令行通过 `.import` 按 CSV 格式读取数据，可为表启用 `STRICT` 类型检查。[PostgreSQL 导入实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/copyfromparse.c) · [DuckDB 错误处理](https://duckdb.org/docs/2.0/data/csv/reading_faulty_csv_files) · [SQLite 导入](https://www.sqlite.org/cli.html#importing_files_as_csv_or_other_formats) · [SQLite STRICT](https://www.sqlite.org/stricttables.html)

本项目在启动写入前处理字段转换。跳过一行后，列清单之外的默认值仍会求值。超过上限、默认表达式失败或表约束失败时，整批导入会撤销。[对照测试](test-report.md#copy-ignore-bad-rows-20261122)

## 2026-11-22 · 字段中的默认值先于后续字段求值

PostgreSQL 18.6 在转换文本或 CSV 记录时处理字段中的默认值标记，随后计算列清单之外的默认值。SQLite 的 `INSERT` 按位置匹配列清单和 `VALUES` 中的值，省略的列采用默认值。DuckDB 的 `INSERT` 也区分显式 `DEFAULT` 和未提供的列。本项目使用现有的写入转换处理带标记的导入行，并核对了序列与整数、字符长度错误的先后顺序。[PostgreSQL 导入实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/copyfromparse.c) · [SQLite INSERT](https://www.sqlite.org/lang_insert.html) · [DuckDB INSERT](https://duckdb.org/docs/2.0/sql/statements/insert) · [测试范围](test-report.md#copy-default-evaluation-order-20261122)

## 2026-11-22 · 导入身份列与转换错误

PostgreSQL 18.6 的 `COPY FROM` 直接采用文件中的身份列值。导入时省略该列，或者为该字段使用默认值标记，会从序列取号。本项目在普通表的客户端文本和 CSV 导入中接入了这条规则，并对比了显式编号、序列取号和回滚结果。[PostgreSQL COPY](https://www.postgresql.org/docs/18/sql-copy.html) · [对照报告](test-report.md#copy-identity-input-20261122)

## 2026-11-22 · 导入时使用列默认值

PostgreSQL 18.6 在解析 COPY 字段时识别默认值标记，再调用当前列默认表达式。DuckDB 的 `INSERT` 支持省略列或显式写 `DEFAULT`。SQLite 的 `INSERT` 也支持通过省略列或使用 `DEFAULT VALUES` 来采用默认值。[PostgreSQL COPY](https://www.postgresql.org/docs/18/sql-copy.html) · [PostgreSQL 导入实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/copyfromparse.c) · [DuckDB INSERT](https://duckdb.org/docs/2.0/sql/statements/insert) · [SQLite INSERT](https://www.sqlite.org/lang_insert.html)

本项目用原有的写入代码求取默认值。所测的列默认值、域默认值及序列均可通过标记写入，`DEFAULT NULL::text` 的定义存储仍有差异。二进制和服务器文件的更多选项仍待实现。[正确性](test-report.md#copy-default-marker-20261122) · [性能](performance-report-2026-10-30.md#copy-default-marker-cost-20261122)

## 2026-11-22 · CSV 导入

PostgreSQL 18.6 读取完整的 CSV 记录，再解释字段和空值。引号内换行会保留，`HEADER MATCH` 按导入列的顺序核对列名。DuckDB 可自动识别 CSV 格式，也支持指定空值标记并行读取文件。SQLite 的命令行通过 `.import` 读取带引号的 CSV。[PostgreSQL 导入实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/copyfromparse.c) · [DuckDB CSV 导入](https://duckdb.org/docs/2.0/data/csv/overview) · [SQLite 导入](https://www.sqlite.org/cli.html#importing_files_as_csv_or_other_formats)

本项目在解析当前记录时保留引号、转义和换行状态，同一条记录可以跨多个客户端数据消息。写入仍使用现有的 COPY 事务。并行读取服务器文件和更多编码仍有差距。[导入测试](test-report.md#copy-csv-in-20261122) · [性能报告](performance-report-2026-10-30.md#copy-csv-in-cost-20261122)

## 2026-11-22 · CSV 导出

PostgreSQL 18.6 对文本值是否需要加引号单独判断。CSV 的空值和同字样的文本用引号区分，列名行单独发送。DuckDB 支持设置 CSV 文件的分隔符、空值与强制加引号的列。SQLite 在命令行用 CSV 模式格式化查询结果。[PostgreSQL 18.6 实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/copyto.c) · [DuckDB COPY](https://duckdb.org/docs/2.0/sql/statements/copy) · [SQLite 导出](https://www.sqlite.org/cli.html#export_to_csv)

本项目已将 CSV 输出接入已有的逐行导出。编码时复用一行的输出空间，列名和数据都使用所选格式。二进制格式及服务器端文件选项仍有差距。[协议测试](test-report.md#copy-csv-out-20261122) · [性能报告](performance-report-2026-10-30.md#copy-csv-out-cost-20261122)

## 2026-11-22 · 并发事务和目录写入

PostgreSQL 按查询访问的表取得相应的锁，普通 `SELECT` 使用 `ACCESS SHARE`。SQLite 在事务无法取得文件访问权时可返回 `SQLITE_BUSY`，应用可设置最长等待时间。DuckDB 在单个进程内用多版本数据和乐观并发控制，发生实际写入冲突时返回事务冲突。[PostgreSQL 锁](https://www.postgresql.org/docs/18/explicit-locking.html) · [SQLite 事务](https://www.sqlite.org/lang_transaction.html) · [DuckDB 并发](https://duckdb.org/docs/current/connect/concurrency)

本项目的连接会在开始事务前检查目录文件。新版遇到同一进程里正在保存目录的连接时，先释放本次取得的数据库锁，再按锁等待时间重试。更细粒度的目录锁和跨进程锁队列仍待补齐。[测试报告](test-report.md#begin-catalog-contention-20261122) · [性能报告](performance-report-2026-10-30.md#begin-catalog-contention-cost-20261122)

## 2026-11-22 · 导出查询结果

PostgreSQL 18.6 可以把查询计划执行出来的行直接交给 COPY 编码器，再逐行发给客户端。它也支持把带 `RETURNING` 的写入结果交给同一个导出入口。DuckDB 支持把查询结果导出为 CSV、Parquet 或 JSON 等文件。SQLite 的命令行工具执行普通查询，再用 `.mode` 和 `.once` 把结果写到文件。[PostgreSQL 18.6 实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/copyto.c) · [PostgreSQL COPY](https://www.postgresql.org/docs/18/sql-copy.html) · [DuckDB COPY](https://duckdb.org/docs/2.0/sql/statements/copy) · [SQLite 导出](https://www.sqlite.org/cli.html#export_to_csv)

本项目使用现有查询计划逐行读取结果，再用现有的 COPY 文本传输发送。需要排序等处理时，仍由查询计划决定需要暂存的数据。写入语句的结果、更多输出格式和查询导出到服务器文件仍有差距。[测试报告](test-report.md#copy-query-stdout-20261122) · [性能报告](performance-report-2026-10-30.md#copy-query-stdout-cost-20261122)

## 2026-11-22 · 按编号查询系统类型

PostgreSQL 18.6 在 `pg_type.oid` 上建有唯一 B-tree 索引。实测查询已知编号和不存在的编号都使用了 `pg_type_oid_index`。SQLite 把 `sqlite_schema` 存在普通表的 B-tree 中，按 `rowid` 可点查，按对象名时实测扫描这张表。DuckDB 1.4.3 的 `duckdb_types()` 收集已登记的类型，再分批返回数据。[PostgreSQL 类型目录索引](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_type.h#L265-L269) · [SQLite 文件格式](https://www.sqlite.org/fileformat2.html#storage_of_the_sql_database_schema) · [DuckDB 类型目录实现](https://github.com/duckdb/duckdb/blob/v1.4.3/src/function/table/system/duckdb_types.cpp#L60-L84)

本项目在内存目录快照中先按明确的常量编号筛选，再为匹配记录生成查询结果。快照依然按原路径读取，直接利用持久索引定位目录记录和目录的完整多版本存储仍有差距。[测试报告](test-report.md#pg-type-oid-source-20261122) · [性能报告](performance-report-2026-10-30.md#pg-type-oid-source-cost-20261122)

## 2026-11-22 · COPY 按实际表导入和导出

PostgreSQL 为 `COPY` 打开实际表并取得相应的锁，然后检查列和权限。SQLite 的命令行工具用 `.import --schema` 指定导入位置，导出时执行查询。DuckDB 在绑定 `COPY` 时确定导入表或导出查询，支持将数据导出到多个文件格式。[PostgreSQL 18.6 COPY 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/copy.c#L123-L206) · [SQLite 导入导出](https://www.sqlite.org/cli.html#csv_import) · [DuckDB COPY](https://duckdb.org/docs/stable/sql/statements/copy)

本项目让客户端的 `COPY` 先取得事务的数据访问权，再绑定实际表和输出列。对同名 `pg_type`，系统表、用户表和临时表按连接设置选择。系统表已能按列输出部分数据，系统表导入和更多文件格式仍有差距。[测试报告](test-report.md#copy-relation-selection-20261122) · [性能报告](performance-report-2026-10-30.md#copy-relation-selection-cost-20261122)

## 2026-11-22 · 查找已经存在的同名表

PostgreSQL 18.6 按顺序查找 `search_path`，自动补上省略的 `pg_catalog` 和当前连接的临时模式。两者可通过显式写出改变位置。SQLite 使用固定的临时库、主库、附加库顺序。DuckDB 先查临时表和配置指定的路径，再查默认系统目录。[PostgreSQL 18.6 查找实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/namespace.c#L65-L86) · [SQLite 对象查找](https://sqlite.org/lang_naming.html) · [DuckDB 查找实现](https://github.com/duckdb/duckdb/blob/v1.4.3/src/catalog/catalog_search_path.cpp#L254-L270)

本项目采用 PostgreSQL 的顺序查找 `pg_type`，并将选出的表身份用于列绑定、普通查询、协议结果描述和写入目标。支持普通和临时同名表，写入系统目录返回 `0A000`。其他系统表和建表、维护命令的查找仍需分别接入和验证。[测试报告](test-report.md#pg-type-search-path-20261122) · [性能报告](performance-report-2026-10-30.md#pg-type-search-path-cost-20261122)

## 2026-11-22 · 查看列表查询的计划

PostgreSQL 使用列统计估算 `IN` 一类列表条件，也可以在 `ANALYZE` 后用扩展统计估算有关联的同表条件。SQLite 将 `ANALYZE` 采集的表和索引统计留给优化器，DuckDB 的 `ANALYZE` 用于更新选择连接顺序所需的统计。本项目在生成 `EXPLAIN` 的过滤节点时复用同一列的统计和表结构。列表条件仍逐值估算，相关多列的估算算法仍有差距。[PostgreSQL 统计](https://www.postgresql.org/docs/18/planner-stats.html) · [PostgreSQL 列表估算源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/selfuncs.c) · [SQLite ANALYZE](https://www.sqlite.org/lang_analyze.html) · [DuckDB ANALYZE](https://duckdb.org/docs/2.0/sql/statements/analyze) · [性能报告](performance-report-2026-10-30.md#explain-filter-statistics-cost-20261122)

## 2026-11-22 · 事务开始时检查目录的开销

PostgreSQL 18.6 在事务提交时通知其他连接哪些目录数据需要重新读取。SQLite 在数据库文件头维护表结构版本，运行已准备的语句时会检查它。DuckDB 用目录标识和版本判断是否需要重新分析已准备的语句。本项目仍逐个检查目录文件，在热目录上用同一轮检查决定是否需要写入并核对冲突。收到其他实例发布的版本后，目录服务加载新的对象，已有连接可用完所持有的版本。[PostgreSQL 目录通知](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/cache/inval.c#L1097-L1158) · [SQLite 表结构版本](https://sqlite.org/fileformat.html#schema_cookie) · [DuckDB 目录版本](https://github.com/duckdb/duckdb/blob/v1.4.3/src/main/prepared_statement_data.cpp#L26-L74) · [性能报告](performance-report-2026-10-30.md#catalog-publication-one-scan-cost-20261122)

## 2026-11-22 · 单独查询连接设置的开销

PostgreSQL 先给事务分配虚拟编号，持久编号通常在第一次写入时分配。SQLite 在 SQL 实际访问数据库时开启相应的读写事务，部分连接级 `PRAGMA` 可以直接执行。DuckDB 也提供读取当前连接设置的 `current_setting` 函数。本项目在确认调用属于 `pg_catalog` 且参数为基础类型后，直接读写连接设置。单条普通查询使用已有的语句设置快照，扩展请求以自身的同步周期提交或撤销设置。[PostgreSQL 事务编号](https://www.postgresql.org/docs/18/transaction-id.html) · [SQLite 事务](https://www.sqlite.org/lang_transaction.html) · [DuckDB 设置](https://duckdb.org/docs/2.0/configuration/overview)

## 2026-11-22 · 通过查询读写连接设置

PostgreSQL 通过 `current_setting` 读取运行中的设置，`set_config` 使用与 `SET` 相同的事务机制。DuckDB 也有 `current_setting`，用 `SET` 或 `PRAGMA` 更改设置。SQLite 为部分只读 `PRAGMA` 提供可查询的表函数。本项目将两种 SQL 函数接入原有标量函数和事务设置处理，覆盖工作内存和三项超时。预编译时只确定输入和输出类型，执行时才读取或更改当前连接。[PostgreSQL 函数文档](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-ADMIN-SET) · [PostgreSQL 18.6 实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/misc/guc_funcs.c) · [DuckDB 配置](https://duckdb.org/docs/2.0/configuration/overview) · [SQLite PRAGMA 表函数](https://www.sqlite.org/pragma.html#pragma_functions)

## 2026-11-22 · 锁等待时间与事务设置

PostgreSQL 为事务中的参数变更保存可回滚的历史，普通 `SET` 在提交后保留，`SET LOCAL` 在事务结束时恢复。SQLite 的 `busy_timeout` 属于连接，按等待时间累计决定是否返回 `SQLITE_BUSY`。DuckDB 支持会话级设置，并用乐观并发控制处理对同一行的冲突写入。本项目复用保存点的设置快照，并用版本号在服务器更新三项超时默认值后刷新连接。普通请求只需查看一次版本号，无需为每次读取默认值加锁。[PostgreSQL SET](https://www.postgresql.org/docs/18/sql-set.html) · [PostgreSQL 18.6 参数实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/misc/guc.c) · [SQLite busy timeout](https://www.sqlite.org/c3ref/busy_timeout.html) · [DuckDB 并发](https://duckdb.org/docs/2.0/connect/concurrency)

## 2026-11-22 · 表锁和数据页锁升级

PostgreSQL 18.6 的表锁按模式检查冲突，并保留同一事务已持有的锁。页内读写由缓冲区锁协调。SQLite 的写入锁按数据库文件管理，DuckDB 用乐观并发控制处理同一行写入。本项目在转换表锁和数据页锁时保留原锁，在 Linux 上原子转换底层文件锁。当前 SQL 对照覆盖单表 `ACCESS SHARE` 和 `ACCESS EXCLUSIVE`，数据页锁由本项目原生用例验证。[PostgreSQL 表锁](https://www.postgresql.org/docs/18/sql-lock.html) · [PostgreSQL 18.6 缓冲区源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/storage/buffer/bufmgr.c) · [SQLite 锁机制](https://www.sqlite.org/lockingv3.html) · [DuckDB 并发](https://duckdb.org/docs/current/connect/concurrency)

## 2026-11-22 · 共享行锁升级

PostgreSQL 18 在多笔事务要升级互相冲突的行锁时会检测死锁。回滚到保存点时，保存点前的锁仍有效。SQLite 以数据库文件为单位协调写入，DuckDB 用乐观并发控制处理对同一行的写入冲突。本项目保留等待升级的原共享锁，连接间的死锁由同一个服务进程识别。Linux 进程间的行锁也在原有文件描述上转换，使失败的升级不释放原锁。[PostgreSQL 锁机制](https://www.postgresql.org/docs/18/explicit-locking.html#LOCKING-ROWS) · [SQLite 锁机制](https://www.sqlite.org/lockingv3.html) · [DuckDB 并发](https://duckdb.org/docs/current/connect/concurrency) · [Linux 行锁文档](https://man7.org/linux/man-pages/man2/fcntl_locking.2.html)

## 2026-11-22 · 查询时锁住分区表记录

PostgreSQL 18.6 的 `LockRows` 按实际目标关系和记录位置取得指定强度的锁。多笔事务可同时持有共享行锁，更新和删除需等待。`LIMIT` 达到读取数量后停止加锁，经过 `OFFSET` 跳过的记录仍被锁住。SQLite 的写入协调以数据库事务为单位。DuckDB 的并发写入采用乐观冲突检测。[PostgreSQL 锁机制](https://www.postgresql.org/docs/18/explicit-locking.html#LOCKING-ROWS) · [PostgreSQL 18.6 行锁源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLockRows.c) · [SQLite 事务](https://sqlite.org/lang_transaction.html) · [DuckDB 并发](https://duckdb.org/docs/current/connect/concurrency)

本项目在普通单表查询中采用实际分区的共享或排他行锁。并发修改提交后，默认隔离级别可继续检查同一分区的新版本。发生跨分区移动时返回 `40001`。较弱的 PostgreSQL 行锁模式以及复杂查询组合仍有差距。[测试报告](test-report.md#partition-share-20261122)

## 2026-11-22 · 更新分区表中的记录

PostgreSQL 18.6 在记录不再符合原分区条件时，删除旧版本，再把新版本写入匹配的分区。它把跨分区移动记在旧版本的行指针上，等待这条记录的另一笔写入据此返回 `40001`。SQLite 和 DuckDB 的 `UPDATE` 文档同样允许用原来的列值计算新值。本项目用锁定后的原记录计算本行的全部赋值，同分区保留新旧行的关联，跨分区则保留实际的来源和写入位置。[PostgreSQL UPDATE](https://www.postgresql.org/docs/18/sql-update.html) · [PostgreSQL 18.6 执行源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeModifyTable.c) · [SQLite UPDATE](https://sqlite.org/lang_update.html) · [DuckDB UPDATE](https://duckdb.org/docs/2.0/sql/statements/update)

目前覆盖已经配好的部分持久分区表。独立叶关系、分区索引、带来源表的更新和完整的分区行锁组合仍有差距。[测试报告](test-report.md#partition-update-20261122) · [性能报告](performance-report-2026-10-30.md#partition-update-cost-20261122)

## 2026-11-22 · 删除分区表中的记录

PostgreSQL 18.6 用实际目标关系和元组位置调用表访问方法，在需要时等待并发修改并重新检查。SQLite 可为没有筛选、返回列或触发器的整表删除选择清空路径。DuckDB 的事务实现使用撤销信息保留旧版本可见性。[PostgreSQL 删除执行源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeModifyTable.c) · [SQLite DELETE](https://sqlite.org/lang_delete.html#the_truncate_optimization) · [DuckDB 事务](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)

本项目在受支持的现有分区布局中，按实际叶分区锁定和删除行。预写日志包含父表物理身份和叶编号，回滚按实际叶页面恢复原记录。`ONLY` 指定父表自身时返回零行，普通 `RETURNING` 保留空值。独立叶关系、分区索引、带来源的 `DELETE USING` 及更广的分区事务支持仍待补齐。[测试报告](test-report.md#partition-delete-20261122) · [性能报告](performance-report-2026-10-30.md#partition-delete-cost-20261122)

## 2026-11-22 · 减少分区扫描中重复的表定义查找

PostgreSQL 18.6 对已经解析的分区边界做二分查找。SQLite 对有序索引先定位范围起点，再顺序读匹配记录。DuckDB 用数据组的边界跳过无法匹配的组。[PostgreSQL 分区裁剪源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/partitioning/partprune.c) · [SQLite 查询规划](https://sqlite.org/queryplanner.html) · [DuckDB 最小值和最大值索引](https://duckdb.org/docs/2.0/guides/performance/indexing)

本项目减少单个整数条件的区间分配，并在普通分区扫描中复用已经加锁读取的表定义和数据目录。这也包括所有分区都需要读取的情况。与 PostgreSQL 的通用边界索引、多列分区键和独立叶关系仍有差距。[测试报告](test-report.md#partition-scan-reuse-20261122) · [性能报告](performance-report-2026-10-30.md#partition-scan-reuse-cost-20261122)

## 2026-11-22 · 按整数范围跳过无关分区

PostgreSQL 18.6 依据分区边界和查询条件生成裁剪步骤，用交集处理 `AND`，用并集处理 `OR`，也可以在执行时根据参数进一步缩小范围。SQLite 在满足条件时分别使用索引处理各个 `OR` 条件，再合并行位置。DuckDB 利用每组数据的最小值和最大值跳过不可能命中的数据组。[PostgreSQL 分区裁剪](https://www.postgresql.org/docs/18/ddl-partitioning.html#DDL-PARTITION-PRUNING) · [PostgreSQL 18.6 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/partitioning/partprune.c) · [SQLite OR 优化](https://sqlite.org/optoverview.html#or_optimizations) · [DuckDB 最小值和最大值索引](https://duckdb.org/docs/2.0/guides/performance/indexing)

本项目用已经存储的范围边界检查整数分区列的直接比较和空值条件。数据库可以在普通查询和符合条件的排序查询中跳过无关的分区文件，读取后仍逐行核对完整条件。多列分区键的通用裁剪、对任意运行时参数的重新裁剪，以及独立分区表的创建和维护仍有差距。[测试报告](test-report.md#integer-range-pruning-20261122) · [性能报告](performance-report-2026-10-30.md#integer-range-pruning-cost-20261122)

## 2026-11-22 · 分页后读取要返回的长字段

PostgreSQL 18 可以在排序和分页后计算某些输出表达式，并保留排序、去重和分组所需的值。SQLite 的排序引用优化可以在排序记录中保存数据行的定位信息，稍后再读取需要的列。DuckDB 也有按需回取列值的优化。[PostgreSQL SELECT](https://www.postgresql.org/docs/18/sql-select.html) · [SQLite 排序引用源码](https://github.com/sqlite/sqlite/blob/master/src/select.c#L1060-L1110) · [DuckDB 延迟读取研究](https://www.duckdb.org/library/selective-late-materialization/)

本项目对符合条件的分区分页在排序中保存原始行和该行的空值信息，跳过 `OFFSET` 指定的行后再展开当前页的长值。候选可按同一格式写入排序临时文件。排序键、行过滤和易变表达式仍由相应的执行计划处理。[验证范围](test-report.md#partition-offset-output-20261122) · [性能报告](performance-report-2026-10-30.md#partition-offset-output-cost-20261122)

## 2026-11-22 · 分区排序按页接收记录

PostgreSQL 18.6 给排序器传入所需记录数，符合条件时用有界堆保留排名靠前的记录。SQLite 在已保留 `LIMIT + OFFSET` 条时比较新记录，只有排得更靠前才替换当前最后一条。DuckDB 的优化器也会将符合条件的排序和 `LIMIT` 合并为 `TopN`。[PostgreSQL 排序节点](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeSort.c) · [PostgreSQL 有界堆](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/sort/tuplesort.c) · [SQLite 排序源码](https://github.com/sqlite/sqlite/blob/master/src/select.c#L843-L866) · [DuckDB 优化器](https://duckdb.org/2024/11/14/optimizers)

本项目原有的有界排序已经按 `LIMIT + OFFSET` 保留候选。符合条件的分区查询现在按页送入记录，在内存中先比较排序键，再为可能入选的记录读取其他字段。排序仍要检查每个分区。遇到内存不足时，现有排序器将数据写入临时文件。[功能验证](test-report.md#partition-order-input-20261122) · [性能报告](performance-report-2026-10-30.md#partition-order-input-cost-20261122)

## 2026-11-22 · 分区查询按 LIMIT 停止读取

PostgreSQL 18.6 的 `Limit` 节点按需要从下游请求记录，`Append` 可以按顺序向各子计划取行。SQLite 的 `OffsetLimit` 指令计算分页需要读取的行数。DuckDB 的文档将 `LIMIT` 定义为作用于查询结果的操作，并说明无排序时结果顺序可能变化。[PostgreSQL Limit 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c) · [PostgreSQL Append 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeAppend.c) · [SQLite 指令](https://www.sqlite.org/opcode.html) · [DuckDB LIMIT 文档](https://duckdb.org/docs/2.0/sql/query_syntax/limit)

本项目在符合条件且只查询一张分区表时，取到 `OFFSET + LIMIT` 条匹配记录就停止读取。这次改动沿用分区过滤、事务可见性和输出处理。独立分区关系、通用按需扫描及 PostgreSQL 客户端按批取行的交互还有差距。[验证范围](test-report.md#partition-limit-prefix-20261122) · [性能报告](performance-report-2026-10-30.md#partition-limit-prefix-cost-20261122)

## 2026-11-22 · 读取分区时复用表定义

PostgreSQL 的 `Append` 从各叶分区的扫描节点取行，长字段可在真正需要时再展开。SQLite 在执行已编译语句时检查模式变更，必要时重新编译。DuckDB 的扫描支持只读取查询所需的列。它们分别说明了扫描状态复用、模式失效检查和按需读列的做法。[PostgreSQL Append 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeAppend.c#L335-L352) · [PostgreSQL 长值存储](https://www.postgresql.org/docs/18/storage-toast.html) · [SQLite 语句编译](https://sqlite.org/c3ref/prepare.html) · [DuckDB Parquet 扫描](https://duckdb.org/docs/data/parquet)

本项目在已持有表锁的分区扫描中共用一次读到的父表定义和存储目录。处理每个叶分区时仍核对实际文件，行级权限也使用这次读取的表定义。固定长度的行直接交给后续操作，已有行缓冲可以交接给排序和输出。长文本继续按原顺序读取、解码和报错。[验证范围](test-report.md#partition-scan-schema-20261122) · [性能报告](performance-report-2026-10-30.md#partition-scan-schema-cost-20261122)

## 2026-11-22 · 分区查询中的 OR 和空值

PostgreSQL 用 `Append` 等节点合并子计划，各叶分区有自己的行身份。优化器只剪掉条件明确排除的分区。SQLite 可以分别用索引查找各个 `OR` 条件，再按同一张表的行身份合并。DuckDB 用选择向量关联数据位置，并维护单独的空值标记。[PostgreSQL Append 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeAppend.c#L335-L352) · [分区裁剪](https://www.postgresql.org/docs/18/ddl-partitioning.html#DDL-PARTITION-PRUNING) · [SQLite OR 优化](https://www.sqlite.org/optoverview.html#or_optimizations) · [DuckDB 数据向量](https://duckdb.org/docs/current/internals/vector) · [DuckDB 空值标记](https://duckdb.org/docs/current/clients/c/vector)

本项目的各分区仍由父表管理。读取时逐行判断 `OR` 条件，每个命中的实际记录进入结果一次。两个分区的记录可以有相同的页号和槽号，也可以投影出同样的值，这两种情况下都应各自返回。数据库从实际记录提取空值标记，并随原行保留到排序和输出。

## 2026-11-22 · 向分区表写入并处理回滚

PostgreSQL 18.6 在执行插入时选择实际的叶关系，首次写到某一叶关系时才准备相应的写入状态。执行器的行容器同时保留来源表编号、行位置和各列是否为空。SQLite 的隐式事务在语句执行结束时提交，显式事务由提交或回滚结束。DuckDB 为批量修改记录撤销信息，提交后可用预写日志恢复数据。[PostgreSQL 分区](https://www.postgresql.org/docs/18/ddl-partitioning.html) · [写入路由源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/execPartition.c#L194-L253) · [行容器源码](https://github.com/postgres/postgres/blob/REL_18_6/src/include/executor/tuptable.h#L104-L125) · [SQLite 事务](https://www.sqlite.org/lang_transaction.html) · [DuckDB 事务实现](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)

本项目仍由父表管理分区文件。事务写入记录父表的物理编号、写入的分区以及分区内的位置，提交和撤销操作都定位到实际的数据文件。查询从同一份记录读取数据和空值标记，两个分区恰好使用相同的页和槽时仍能区分。独立子表身份、支持分区的行索引，以及更新、删除和两阶段提交仍需后续工作。[测试范围](test-report.md#transactional-partition-insert-20261122) · [性能报告](performance-report-2026-10-30.md#transactional-partition-insert-cost-20261122)

## 2026-11-22 · 清空分区表的恢复范围

PostgreSQL 的各个叶分区有自己的关系编号。清空父表时会处理各叶分区，单独清空一个叶分区时保留兄弟分区的数据。PostgreSQL 为需要回滚的关系切换文件编号。SQLite 可以直接清空符合条件的无条件 `DELETE`，DuckDB 为批量修改记录撤销信息。[PostgreSQL TRUNCATE](https://www.postgresql.org/docs/18/sql-truncate.html) · [PostgreSQL 18.6 源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/tablecmds.c#L2079-L2131) · [SQLite DELETE](https://www.sqlite.org/lang_delete.html#the_truncate_optimization) · [DuckDB 事务实现](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)

本项目的叶分区文件由父表管理。符合条件的父表可以一起保存各分区的恢复文件，清空会释放对应的缓存，重放日志时核对要写的文件。对照使用原生接口准备本项目的实际数据分区，用 SQL 比较父表的查询和清空。该轮对照早于上面的事务插入改动。通过 SQL 创建独立叶分区，以及为重用名称的分区分配独立文件身份，仍有差距。[验证范围](test-report.md#truncate-partition-undo-20261122)

## 2026-11-22 · 一次清空多张普通表

PostgreSQL 可以在一条 `TRUNCATE` 里清空多张表，整组由事务决定是否生效。SQLite 在回滚日志模式下以一份共同记录决定多个数据库文件的事务结果。DuckDB 用事务的撤销记录找回原来的数据，再由提交决定哪些修改生效。[PostgreSQL 18 的 TRUNCATE](https://www.postgresql.org/docs/18/sql-truncate.html) · [SQLite 多文件原子提交](https://www.sqlite.org/atomiccommit.html#multi_file_commit) · [DuckDB 事务实现](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)

本项目在开始清空前，为语句涉及的整组普通持久表准备一份恢复记录。每张表分别核对文件归属和物理编号，共用一份统计文件备份。提交前任何一张表失败，恢复会处理整组。显式事务、重置自增编号及包含外部表空间表等不符合条件的组合使用全库备份，独立物理存储代仍待实现。[恢复测试](test-report.md#truncate-multi-table-undo-20261122)

## 2026-11-22 · 清空一张表时按表保存回滚数据

PostgreSQL 18.6 为需要保留回滚能力的表分配新文件编号，让旧文件留到事务结束。SQLite 的回滚日志保存将被改写的原始页面。DuckDB 的撤销缓冲区记录事务涉及的数据变化。[PostgreSQL 18.6 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L2079-L2131) · [SQLite 原子提交](https://www.sqlite.org/atomiccommit.html#creating_a_rollback_journal_file) · [DuckDB 事务实现](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)

这一轮先支持单独清空默认位置中一张有物理编号的普通持久表。数据库复制目标表的数据、附属文件和共享统计文件。提交前退出后，可按日志决定是否还原这些文件。[恢复测试](test-report.md#truncate-table-undo-20261122) · [同等日志同步下的性能测试](performance-report-2026-10-30.md#truncate-table-undo-cost-20261122)

## 2026-11-21 · 清空表日志与事务结果

PostgreSQL 18.6 清空既有表时分配新的文件编号，并按事务结果清理多余的文件代。SQLite 的 WAL 模式把修改后的页面追加到日志，用提交记录确定生效位置。DuckDB 用 WAL 恢复已提交的数据变更，并用撤销缓冲区保存事务涉及的变化。[PostgreSQL 18.6 清空表源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L2079-L2131) · [PostgreSQL 提交及中止时的文件清理](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/storage.c#L613-L684) · [SQLite WAL](https://www.sqlite.org/wal.html#how_wal_works) · [DuckDB 并发事务](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)

本项目在这一轮为独立执行、具有持久物理编号的 SQL `TRUNCATE` 加入事务号。恢复只为有提交记录且在恢复时间点内的清空建立重做边界。受测版本用全库备份保留旧文件及其附属状态，显式事务中的清空和保存点仍由原流程处理。[测试报告](test-report.md#transactional-truncate-wal-20261121)

## 2026-11-21 · 清空表时保存回滚数据的成本

PostgreSQL 18.6 可以为既有表创建新的空存储，把旧文件留到提交后回收。SQLite 通过回滚日志保存即将修改的原始页面。DuckDB 用撤销缓冲区记录发生变化的数据，并用预写日志恢复已提交的更改。这些做法将事务记录限制在有关的数据。[PostgreSQL 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L2105-L2131) · [SQLite 原子提交](https://www.sqlite.org/atomiccommit.html#creating_a_rollback_journal_file) · [DuckDB 的事务实现](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)

这一轮的 SQL `TRUNCATE` 使用全库备份，其中包括无关表和外部表空间。这里的测试减少了备份内的文件遍历，读文件计算校验值时，在同一文件描述符上同步数据。底层清空日志的提交归属在后续一轮加入。[测试报告](test-report.md#backup-manifest-sync-20261121) · [性能报告](performance-report-2026-10-30.md#backup-manifest-sync-cost-20261121) · [清空表日志](#2026-11-21--清空表日志与事务结果)

## 2026-11-21 · 清空表后只生成一份空索引

PostgreSQL 18.6 对既有表切换到新的空存储后，重建一次表索引和长值索引。SQLite 在满足条件的整表 `DELETE` 中免去逐行访问。DuckDB 的无条件 `DELETE` 和 `TRUNCATE` 走同一路径，部分空间可以在检查点回收。[PostgreSQL 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L2105-L2131) · [SQLite 的清空优化](https://www.sqlite.org/lang_delete.html#the_truncate_optimization) · [DuckDB TRUNCATE](https://duckdb.org/docs/lts/sql/statements/delete#the-truncate-statement)

本项目确认表的数据文件刚重置后，直接生成空索引。普通 `REINDEX` 仍从表中读取有效记录。重建沿用已有的文件切换和恢复机制，旧索引中独有的定义和参数保留到新索引发布。先前清空后的再次打开和重建已省去。[测试报告](test-report.md#truncate-single-index-rebuild-20261121) · [性能报告](performance-report-2026-10-30.md#truncate-single-index-rebuild-cost-20261121)

## 2026-11-21 · 清空表和改列名时保护索引

PostgreSQL 18.6 对既有表执行事务性的 `TRUNCATE` 时，分配空的物理存储，重建索引，并在提交时处置旧文件。SQLite 可在符合条件的全表 `DELETE` 中清空表内的数据，免去逐行访问。DuckDB 将 `TRUNCATE` 作为无条件 `DELETE` 处理，磁盘空间通常到检查点才回收。[PostgreSQL 源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/tablecmds.c) · [SQLite 的清空优化](https://www.sqlite.org/lang_delete.html#the_truncate_optimization) · [DuckDB TRUNCATE](https://duckdb.org/docs/lts/sql/statements/delete#the-truncate-statement)

本项目在清空表前检查会删除和重建的文件，保留邻表的所有文件。改列名前也检查索引目标路径，并按计划更新文件和索引定义。两次实际文件路径冲突仍被拒绝，PostgreSQL 能分别保存两个索引。默认和外部共享表空间的普通操作以及事务回滚对照一致。[测试报告](test-report.md#truncate-column-ownership-20261121)

## 2026-11-21 · 建索引时核对文件归属

PostgreSQL 18 用索引自己的文件编号定位物理文件。SQLite 在 `sqlite_schema` 中为索引记录 B 树根页。DuckDB 将索引放在数据库文件内部。这些实现能分别保存表 `a` 的列 `b_c` 和表 `a_b` 的列 `c` 上的索引。本项目按名称拼接文件路径，遇到同一物理路径时会拒绝写入以保留已有索引。消除这类名称冲突仍需要按独立编号定位索引。[PostgreSQL 文件布局](https://www.postgresql.org/docs/18/storage-file-layout.html) · [SQLite 文件格式](https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema) · [DuckDB 存储说明](https://duckdb.org/faq#can-duckdb-save-data-to-disk) · [本项目测试](test-report.md#index-file-ownership-20261121)

## 2026-11-21 · 表改名和旧名重用

PostgreSQL 18.6 在 `pg_class` 中改表名，表和索引文件仍通过各自的编号定位。SQLite 通过改写 `sqlite_schema` 中的 SQL 定义来重命名，表里的数据不必重写。DuckDB 在目录操作中复制并更改表项的名称，同时更新受影响的约束表达式。[PostgreSQL 重命名源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/tablecmds.c) · [PostgreSQL 文件布局](https://www.postgresql.org/docs/18/storage-file-layout.html) · [SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html) · [DuckDB 目录源码](https://raw.githubusercontent.com/duckdb/duckdb/main/src/catalog/catalog_entry/duck_table_entry.cpp)

本项目当前的文件名包含表名。重命名先核对归属，只移动目标表的文件，建表也可以区分旧名留下的冲突文件和仍由邻表使用的文件。默认及共享外部表空间的 64 项对照与 PostgreSQL 18.6 一致。真有两个对象指向同一物理路径时，仍需报错保护已有文件。按编号区分文件的存储设计仍待完成。[测试报告](test-report.md#rename-table-file-ownership-20261121) · [存储布局差距](issue-sto-02-relation-storage-namespace.md)

## 2026-11-21 · 删除表时核对索引文件归属

PostgreSQL 18 用 `relfilenode` 编号定位普通表和索引的文件，表的 SQL 名称在目录中维护。SQLite 在 `sqlite_schema` 中分别记录对象名和 B 树根页。DuckDB 把多张表及其索引保存在同一个数据库文件里。这三种布局都允许仅因名称有共同前缀的表使用各自的存储空间。[PostgreSQL 文件布局](https://www.postgresql.org/docs/18/storage-file-layout.html) · [PostgreSQL 18.6 重命名源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/tablecmds.c) · [SQLite 文件格式](https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema) · [DuckDB 持久化说明](https://duckdb.org/faq)

本项目仍按表名和索引键拼接物理文件名。普通删表现在对可能属于多张表的文件核对持久定义，保留属于邻表的文件。两张表声明同一路径时会拒绝删除。直接删表在默认和共享外部表空间的 46 项正常操作均与 PostgreSQL 18.6 对照一致。[测试报告](test-report.md#drop-table-file-ownership-20261121) · [存储布局差距](issue-sto-02-relation-storage-namespace.md)

## 2026-11-21 · 两列文字沿用已确定的比较方式

PostgreSQL 18.6 在一列使用默认规则、另一列声明了非默认规则时选用后者。比较按 `C` 排序的文本时可以使用字节顺序，定长 `CHAR` 先去掉填充空格。SQLite 也区分按字节比较的 `BINARY` 和忽略尾空格的 `RTRIM`。DuckDB 在选定二进制规则时省去生成排序键的步骤。[PostgreSQL 规则选择](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_collate.c) · [PostgreSQL 文本比较](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/varlena.c) · [PostgreSQL 定长字符比较](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/varchar.c) · [SQLite 比较规则](https://www.sqlite.org/datatype3.html#collation) · [DuckDB 比较规则绑定](https://raw.githubusercontent.com/duckdb/duckdb/main/src/planner/collation_binding.cpp)

本项目在普通单表筛选中先确定两列的位置。选定的是二进制规则且两侧是 `TEXT` 或 `VARCHAR` 时，可以逐行直接比较这两个值。带 `LIMIT` 的普通表筛选复用这一比较方式，扫描打开后再核实表定义和列位置。两列同用数据库默认规则时，在等权重时区分不同字节。定长 `CHAR` 由现有字符比较处理。[测试报告](test-report.md#declared-binary-comparison-20261121)

## 2026-11-21 · 处理两列声明的排序规则冲突

PostgreSQL 18.6 根据规则对象判断两列是否使用同一规则，即使两个规则的排序结果相同。不同的列声明会形成未确定的规则，真正执行需要规则的比较时才报错。文字拼接可以生成结果，规则冲突由外层需要比较时处理。SQLite 的比较采用不同的选择顺序，先看显式指定的规则，再按左右操作数选择列声明。DuckDB 的文档说明，比较两个不同规则的输入时需要显式选定共用的规则。[PostgreSQL 排序规则](https://www.postgresql.org/docs/18/collation.html) · [PostgreSQL 规则推导源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_collate.c) · [SQLite 选择顺序](https://www.sqlite.org/datatype3.html#assigning_collating_sequences_from_sql) · [DuckDB 排序规则](https://duckdb.org/docs/preview/sql/expressions/collations)

本项目在普通单表筛选中用已绑定的列位置读取声明规则，分开处理规则对象和实际比较方式。由 `CASE`、`COALESCE` 和 `||` 产生的文字保留外层比较需要的规则信息。显式覆盖仍优先。独立声明的规则采用相同语言设置时，仍能识别两者的冲突。[测试报告](test-report.md#implicit-collation-filter-20261121)

## 2026-11-21 · 查询可以指定文本的比较规则

PostgreSQL 18.6 先核实类型和排序规则名，再决定比较要使用哪一条规则。语句中指定的规则优先于列的声明，两条显式规则必须指向同一个对象。使用索引时比较和索引的规则也需匹配。SQLite 单独记录比较两侧的显式规则，并按其自身的先左后右顺序选择。DuckDB 在绑定比较时把所选规则接到类型化表达式上。[PostgreSQL 名称和类型检查](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_expr.c) · [PostgreSQL 规则推导](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_collate.c) · [PostgreSQL 索引规则](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/path/indxpath.c) · [SQLite 比较](https://raw.githubusercontent.com/sqlite/sqlite/master/src/expr.c) · [DuckDB 比较绑定](https://raw.githubusercontent.com/duckdb/duckdb/main/src/planner/binder/expression/bind_comparison_expression.cpp)

本项目保留 `WHERE` 中的完整比较，先解析规则身份，再把所选规则用于文本比较。普通筛选支持内置规则和现有 libc 自定义规则，包括用 schema 限定的名称。规则的名称和它实际使用的语言设置分别处理。这样，两个都使用 `C` 设置的不同规则仍可识别为冲突。[测试报告](test-report.md#explicit-collation-filter-20261121)

## 2026-11-21 · 比较同一行的两个列值

PostgreSQL 18.6 检查索引条件中另一侧是否也引用了当前表，用有符号 64 位整数直接比较 `BIGINT`。SQLite 记录比较两侧分别引用了哪些表，用这些信息判断条件能否作为索引查找。DuckDB 先绑定两个操作数，再确定它们的比较类型。[PostgreSQL 索引条件](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/path/indxpath.c) · [PostgreSQL BIGINT 比较](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/int8.c) · [SQLite 筛选分析](https://raw.githubusercontent.com/sqlite/sqlite/master/src/whereexpr.c) · [DuckDB 比较绑定](https://raw.githubusercontent.com/duckdb/duckdb/main/src/planner/binder/expression/bind_comparison_expression.cpp)

本项目保留普通查询中列引用的含义。`id = peer_id` 逐行比较，`id = 7` 仍可通过 `id` 的索引查找。当比较两侧是支持的有符号整数列，或使用相同普通比较规则的 `TEXT` 列时，执行前确定两列的位置，每行只读取这两个值进行比较。[测试报告](test-report.md#same-row-select-20261121) · [性能报告](performance-report-2026-10-30.md#same-row-select-cost-20261121)

## 2026-11-21 · 查询区分数字和带引号的列名

PostgreSQL 18.6 分别处理列引用和常量。SQLite 以表达式节点识别筛选条件实际引用的列。DuckDB 把解析出的列名绑定到当前查询的输入。本项目在普通筛选条件中保留解析得到的右侧含义。数字可以作为索引键。双引号列名保留完整的列对列比较，由运行时读取两个值并按实际类型比较。确定操作数形状和确定字面量类型时可复用同一次解析。[PostgreSQL 表达式分析](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_expr.c) · [SQLite 筛选分析](https://raw.githubusercontent.com/sqlite/sqlite/master/src/whereexpr.c) · [DuckDB 列绑定](https://raw.githubusercontent.com/duckdb/duckdb/main/src/planner/binder/expression/bind_columnref_expression.cpp) · [本项目验证](test-report.md#narrow-select-20261121)

## 2026-11-21 · 删除时筛选记录并保留数字常量

PostgreSQL 18.6 将筛选后的行交给删除步骤，并在并发事务已更新该行时按需要重新检查。SQLite 根据查询和约束选择在扫描中直接删行，或先收集标识再删除。DuckDB 把筛选放在删行节点之前。本项目继续先收集候选行，然后加锁并执行删除。对受测有符号整数等值条件，可在首次扫描时借用已经读取的空值信息，省去按行重复查找。[PostgreSQL 删除执行](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeModifyTable.c) · [SQLite 删除执行](https://raw.githubusercontent.com/sqlite/sqlite/master/src/delete.c) · [DuckDB 删除绑定](https://raw.githubusercontent.com/duckdb/duckdb/main/src/planner/binder/statement/bind_delete.cpp)

数字常量通过筛选步骤时保留为数值，避免 `id = 7` 被误当成 `id = "7"`。首次筛选和等待其他事务后的重查都已核对。外键的另一处差异仍待处理。显式 `ON DELETE RESTRICT` 阻止删除时，本项目返回 `23503`，PostgreSQL 18.6 返回 `23001`。项目的旧表定义会把 `RESTRICT` 和 `NO ACTION` 归为同一种动作，处理错误码前需要能保留两者的区别。[本项目验证](test-report.md#narrow-delete-20261121) · [PostgreSQL 外键错误](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/ri_triggers.c)

## 2026-11-21 · 更新时筛选记录，查询时复用列类型

PostgreSQL 18.6 的扫描节点先检查条件，再把匹配记录交给后续步骤。SQLite 用 `WHERE` 扫描逻辑生成要更新的记录。DuckDB 先绑定实际目标表，将条件作为筛选步骤放在更新之前。本项目在受支持的普通整数等值更新中也先筛选，可以省去重新读取明显不匹配的整行。原 SQL 比较仍负责最终决定，并在并发修改后对锁定的新行重新检查。[PostgreSQL 扫描](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/include/executor/execScan.h) · [SQLite 更新](https://raw.githubusercontent.com/sqlite/sqlite/master/src/update.c) · [DuckDB 更新绑定](https://raw.githubusercontent.com/duckdb/duckdb/main/src/planner/binder/statement/bind_update.cpp)

PostgreSQL 把已打开的表登记在本次执行的数据结构里供后续步骤使用，`LIMIT` 也会在取够后停止向子节点取行。本项目的普通表 `LIMIT 1` 本来就会停止读行。这轮省去的是查询为直接列引用准备输出时，重复初始化函数类型推断的工作。[PostgreSQL 表绑定](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/execUtils.c) · [PostgreSQL LIMIT](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeLimit.c)

## 2026-11-20 · 普通表支持更多列

PostgreSQL 18.6 规定表最多 1,600 列，实际还受行大小限制，并用行头的位图区分空值。SQLite 默认最多 2,000 列，也提醒宽列声明可能增加语句准备成本。DuckDB 的列式存储把同一列的值放在一起，并用单独的位图记录空值。本项目把内存里的表定义改为按实际列数保存，沿用原有的磁盘定义和空值位图。普通表的列数上限可到 1,600，某些列组合仍受固定字段占用和单页行宽约束。[PostgreSQL 限制](https://www.postgresql.org/docs/18/limits.html) · [PostgreSQL 行头定义](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/include/access/htup_details.h) · [SQLite 限制](https://sqlite.org/limits.html) · [DuckDB 列式数据](https://duckdb.org/docs/2.0/clients/c/vector) · [本项目验证](test-report.md#wide-table-20261120)

## 2026-11-20 · 重复查看两表连接计划

PostgreSQL 18.6 的 `EXPLAIN` 把已分析的查询交给规划器，随后按选项决定是否执行计划。SQLite 为 `SELECT` 解析表和列名，补充类型后再生成指令。DuckDB 的计划提取也把解析结果交给规划器，再由优化器处理。本项目在构建旧式两表连接计划时复用本次已核验的表结构。普通 `EXPLAIN` 命中缓存时可直接返回已有结果，表和连接列仍按实际引用重新核验。[PostgreSQL 计划生成](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/explain.c) · [SQLite 的 SELECT 准备](https://raw.githubusercontent.com/sqlite/sqlite/master/src/select.c) · [DuckDB 计划提取](https://raw.githubusercontent.com/duckdb/duckdb/main/src/main/client_context.cpp) · [实测结果](test-report.md#legacy-join-explain-reuse-20261120)

## 2026-11-20 · 查看带筛选或分页的整数等值连接

PostgreSQL 18.6 先规划查询，只有 `EXPLAIN ANALYZE` 才运行查询以取得实际行数等指标。DuckDB 让被分析的计划正常执行，再由性能统计模块生成报告。SQLite 的 `EXPLAIN QUERY PLAN` 主要展示计划结构和索引选择。本项目让受支持的整数等值连接在普通查询和 `EXPLAIN` 中使用同一计划构建器。加入 `ANALYZE` 后，报告真实执行过的节点行数。[PostgreSQL 执行计划](https://www.postgresql.org/docs/18/sql-explain.html) · [DuckDB 执行报告](https://raw.githubusercontent.com/duckdb/duckdb/main/src/execution/operator/helper/physical_explain_analyze.cpp) · [SQLite 查询计划](https://sqlite.org/eqp.html) · [本项目验证](test-report.md#integer-join-explain-20261120)

## 2026-11-20 · 按连续整数键直接找到连接记录

PostgreSQL 18.6 为归并连接估算成本时，可以利用输入已有的顺序。SQLite 对按 `rowid` 排序的查询也可以沿原有顺序读取，包括反向读取。DuckDB 的连接执行器在单整数键的值域足够小、键无重复时，可以按键计算数组位置。本项目在已有连接扫描上验证另一种情况。右侧整批键按读取顺序连续递增或递减时，编号之差就是缓存中的位置，额外空间只需存下起点、方向和行数。[PostgreSQL 归并连接规划](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/path/joinpath.c) · [SQLite 查询规划](https://sqlite.org/queryplanner.html#sorting_by_rowid) · [DuckDB 整数键连接](https://github.com/duckdb/duckdb/blob/main/src/execution/operator/join/perfect_hash_join_executor.cpp) · [本项目实测](test-report.md#integer-join-direct-20261120)

## 2026-11-20 · 减少整数等值连接的重复比较

PostgreSQL 的哈希连接会先建立右侧的哈希表，之后按左侧的键查找候选行。右连接和全连接还要补回没有匹配的右侧行。DuckDB 的哈希连接也将建立查找表、查找候选和读取未匹配行分成不同阶段，内存不足时还可分批处理。SQLite 发现同一侧会被反复查找时，可以为当前语句建立临时索引。[PostgreSQL 哈希连接](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeHashjoin.c) · [DuckDB 哈希连接](https://github.com/duckdb/duckdb/blob/main/src/execution/operator/join/physical_hash_join.cpp) · [SQLite 临时索引](https://sqlite.org/optoverview.html#automatic_query_time_indexes)

本项目利用连接已有的右侧记录。第一次扫描读到末尾后，下一次查找才按整数键和原行号建立有序索引。候选仍由原连接条件确认。索引受 `work_mem` 限制，输出一小批就停止的查询也能保持原来的读取顺序。

## 2026-11-20 · 连接或子查询中的表继承

PostgreSQL 将是否读取后代保存在每次表引用的解析结果上。两次引用同一张表时，可以只给其中一次加 `ONLY`。连接和子查询的规划器据各自的表引用展开后代。本项目也按单次表引用使用继承扫描，并在相关子查询重用扫描时更新其外层行。[PostgreSQL 表引用语法](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/gram.y) · [PostgreSQL 绑定](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_relation.c)

SQLite 根据查询形状选择展开 `FROM` 子查询、保存临时结果，或逐行向外层查询提供数据。DuckDB 会把相关子查询转换成其他关系运算，例如连接和汇总，以减少对同一份数据的反复扫描。本项目这轮复用了现有的子查询执行和关联参数绑定，查询仍可能为多条外层记录重复扫描。[SQLite 子查询优化](https://sqlite.org/optoverview.html#subquery_co_routines) · [DuckDB 相关子查询](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql)

## 2026-11-20 · 查询有多层继承的表

PostgreSQL 18.6 在规划时找出父表和所有后代，为每张表加锁。同一张后代表经由多条继承路径找到时，只加入一次。各表的列顺序可以不同，规划器按列名找到对应列，再检查类型、类型修饰值和排序规则。`ONLY` 则把读取范围限定在指定的表。[PostgreSQL 继承说明](https://www.postgresql.org/docs/18/ddl-inherit.html) · [查找后代表](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/catalog/pg_inherits.c) · [列对应关系](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/util/appendinfo.c)

SQLite 的 `UNION ALL` 也会合并多个查询结果，由末尾的 `ORDER BY` 和 `LIMIT` 处理完整结果。DuckDB 的 `UNION ALL BY NAME` 支持按列名合并显式指定的输入。本项目的普通继承查询从继承关系确定输入，按父表定义读取各表，再对合并结果执行查询的筛选、汇总、排序和分页。[SQLite SELECT](https://sqlite.org/lang_select.html#compound_select_statements) · [DuckDB 按列名合并](https://duckdb.org/2025/01/10/union-by-name)

## 2026-11-20 · 字符串函数接收完整表达式

PostgreSQL 18.6 会先检查和化简参数。选中的函数声明为严格函数、且某个参数已知为 `NULL` 时，可把调用化简为同类型的 `NULL`。从数据行读到的空值则属于执行阶段，其他参数已按顺序求值。SQLite 的函数语法也接收表达式，DuckDB 提供同名的文本函数。本项目用完整查询的解析结果执行普通单表投影，并在执行前检查所选函数的声明和常量参数。三套数据库的类型选择和副作用仍应分别验证。[PostgreSQL 常量化简](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/util/clauses.c) · [PostgreSQL 参数求值](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/execExpr.c) · [SQLite 表达式语法](https://sqlite.org/lang_expr.html) · [DuckDB 字符串函数](https://duckdb.org/docs/stable/sql/functions/text.html) · [对照结果](test-report.md#scalar-string-arguments-20261120)

## 2026-11-20 · 建表恢复记录的清理可以重试

PostgreSQL 18.6 按物理关系标识登记事务结束时要删除的文件。它的磁盘管理实现还区分立即删除和检查点后删除的文件。SQLite 的回滚日志模式先同步修改后的数据，再删除回滚日志。DuckDB 把目录变更纳入事务。本项目的建表撤销记录由多个文件组成。先把清理凭据同步到数据库目录，再逐项删除用过的记录，下一次启动就能识别剩余文件。[PostgreSQL 关系文件](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/catalog/storage.c) · [PostgreSQL 文件清理](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/storage/smgr/md.c) · [SQLite 回滚日志](https://www.sqlite.org/atomiccommit.html) · [DuckDB 事务](https://duckdb.org/2024/09/25/changing-data-with-confidence-and-acid) · [本项目测试](test-report.md#create-undo-cleanup-20261120)

## 2026-11-20 · 外部表空间暂时不可用时的建表恢复

PostgreSQL 18 的外部表空间与主数据目录一起构成数据库实例。在 PostgreSQL 18.6 实测中，崩溃后能查询已提交表，也能重用未提交的表名。前两次重启后旧文件仍在，同名新表分配了另一个文件编号。本项目按表名组织文件。启动先确认待清理的外部目录可用，核对新表的物理编号并同步文件删除，再移除表定义。目录恢复可用后才能继续清理。[PostgreSQL 表空间](https://www.postgresql.org/docs/18/manage-ag-tablespaces.html) · [两套系统的实测](test-report.md#external-create-recovery-20261120)

## 2026-11-20 · 撤销事务里新建的表

PostgreSQL 18.6 以关系标识登记需要随事务撤销的文件，并按子事务归属处理保存点。SQLite 的回滚日志保存修改前的数据页。DuckDB 将表结构变更纳入事务。本项目在普通建表前保存受影响的目录文件，并登记新表的物理编号。启动在处理后续整库备份后撤销早先的新表，其他行修改继续由现有日志恢复处理。持有同一数据库的目录写入锁会增加并发建表期间的等待或锁冲突。[PostgreSQL 存储回滚](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/catalog/storage.c) · [SQLite 回滚日志](https://www.sqlite.org/atomiccommit.html) · [DuckDB 事务](https://duckdb.org/2024/09/25/changing-data-with-confidence-and-acid) · [恢复验证](test-report.md#ordinary-create-durable-undo-20261120)

## 2026-11-20 · 表清单更名后的目录同步

PostgreSQL 18.6 的 `durable_rename` 把写入文件和同步父目录作为持久更名的不同步骤。SQLite 的回滚日志模式先确保数据落盘，再删除日志以结束事务。DuckDB 的表结构变更可以随事务撤销。本项目的表清单已完成文件写入和更名时，才会在目录同步失败后重试。这一步用于确认清单已落盘。撤销整条建表语句还需要记录事务私有文件和目录变更。[PostgreSQL 18.6 文件同步](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/common/file_utils.c) · [SQLite 回滚日志](https://sqlite.org/atomiccommit.html) · [DuckDB 事务](https://duckdb.org/2024/09/25/changing-data-with-confidence-and-acid) · [本项目测试](test-report.md#table-list-directory-sync-20261120)

## 2026-11-20 · 普通建表的文件写入与事务撤销

PostgreSQL 18.6 用物理关系编号登记事务中创建的文件，撤销动作随子事务层级管理。SQLite 在回滚日志模式中先保存将要改写的数据页，再写入数据库文件。DuckDB 把表结构变更纳入事务，可以随事务回滚。本项目先让新表的物理编号和完整定义同时落盘，为恢复时核对归属提供依据。显式事务仍需要记录并撤销整次建表的后续文件和目录变更。[PostgreSQL 18.6 关系文件](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/catalog/storage.c) · [SQLite 回滚日志](https://sqlite.org/atomiccommit.html) · [DuckDB 事务](https://duckdb.org/2024/09/25/changing-data-with-confidence-and-acid) · [实际对照](test-report.md#create-schema-publication-20261120)

## 2026-11-20 · 验证时间点恢复的归档输入

PostgreSQL 使用时间线历史文件判断混合归档中的日志属于哪个恢复分支。SQLite 的在线备份提供一致的数据文件快照。DuckDB 的检查点将日志同步到数据库文件。本项目按基础备份选中的时间线合并外部归档，在独立目录核对连续性和重叠字节，再发布恢复后的目录。完整的时间线历史选择以及更多恢复目标仍待实现。[PostgreSQL 时间线](https://www.postgresql.org/docs/18/continuous-archiving.html#BACKUP-TIMELINES) · [SQLite 在线备份](https://sqlite.org/backup.html) · [DuckDB 检查点](https://duckdb.org/docs/2.0/sql/statements/checkpoint) · [本项目恢复验证](test-report.md#pitr-stage-archive-20261120)

## 2026-11-19 · 从活动日志与归档判定事务结果

PostgreSQL 的连续归档恢复需要保留从备份开始的连续 WAL 段。本项目在普通启动时，也要从活动日志与归档核对遗留事务备份的提交结果。新版继续读过切换段产生的填充，归档回收后则验证备份末段的重叠字节和所需记录链。对旧版生成的部分活动日志，可按物理顺序继续读到最后一条有效记录。当轮尚未完成完整的时间线历史选择和普通建表的持久撤销。[PostgreSQL 18 连续归档](https://www.postgresql.org/docs/18/continuous-archiving.html) · [本项目验证](test-report.md#wal-switch-terminal-recovery-20261119)

SQLite 用每个日志页记录的版本标记和累积校验和区分有效记录与上次检查点留下的内容。DuckDB 的检查点会把日志内容同步到数据库文件，并协调正在运行的事务。本次采用的是本项目自己的段和记录格式。[SQLite 日志文件格式](https://www.sqlite.org/fileformat2.html#the_write_ahead_log) · [DuckDB 检查点](https://duckdb.org/docs/2.0/sql/statements/checkpoint)

## 2026-11-19 · 建表时先检查是否需要写入

PostgreSQL 先解析创建表的命名空间，`IF NOT EXISTS` 遇到已存在的关系就发提示并返回。SQLite 和 DuckDB 也允许用这个选项跳过已存在的表。本项目把自动提交普通建表的事务备份移到检查之后，避免为跳过或无效的语句复制已有数据。这一轮实际创建新表仍使用整库事务备份。[PostgreSQL 18.6 创建检查](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_utilcmd.c#L170-L202) · [SQLite 建表](https://www.sqlite.org/lang_createtable.html) · [DuckDB 建表](https://duckdb.org/docs/2.0/sql/statements/create_table#if-not-exists) · [实测结果](performance-report-2026-10-30.md#create-validate-before-backup-cost-20261119)

## 2026-11-19 · 未提交建表与崩溃恢复

PostgreSQL 18.6 的受测事务先改写已有表，再建两张表。崩溃后，原有表恢复到已提交状态，两张新表均不可见。PostgreSQL 为持久关系写入物理创建日志，并在事务中按关系身份登记待清理文件。SQLite 将结构定义保存在 `sqlite_schema`，建表走写事务。DuckDB 的事务也隔离未提交修改，并在回滚时撤销修改。[PostgreSQL 关系存储](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/catalog/storage.c) · [SQLite 事务](https://www.sqlite.org/lang_transaction.html) · [SQLite 结构目录](https://www.sqlite.org/schematab.html) · [DuckDB 事务](https://duckdb.org/docs/2.0/sql/statements/transactions)

本项目复现了普通建表在崩溃后留下空表的问题。整库复制原型通过了新表清理测试，成功建表的额外代价过高，未合入。后续的关系级恢复需要能验证目录项和文件属于哪一次建表，并在跨越提交、回滚到保存点、日志回收和同名重建时保留这一证据。[行为对照](test-report.md#ddl-image-sequence-recovery-20261119) · [原型性能](performance-report-2026-10-30.md#ordinary-create-recovery-cost-20261119)

当前的表、列、默认值和依赖记录分别写入文件。进程可能在只写完部分文件时中断。按新表撤销前，需要在写入前记录物理表和目录对象的身份，恢复时按身份清理已经写出的部分。回滚到保存点还需要记录每张表各自的撤销结果，因为外层事务之后仍可以提交。[实现入口](../src/commands/DdlExecutor.cpp) · [目录写入](../src/catalog/catalog.cpp)

## 2026-11-19 · 临时表与崩溃恢复

PostgreSQL 将临时表数据文件绑定到会话，持久表的数据页才进入通常的 WAL 路径。重启回收遗留文件，临时命名空间也有独立的清理过程。本项目现有日志可包含临时表页镜像，恢复时借助创建记录核对其归属并跳过该页，普通表继续按物理关系编号恢复。两边受测的结果包括已提交普通表数据仍可读取，失效会话的临时表消失，表名可以重新使用。[PostgreSQL 存储创建](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/catalog/storage.c) · [PostgreSQL 临时命名空间](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/catalog/namespace.c) · [本项目验证](test-report.md#temporary-table-crash-recovery-20261119)

## 2026-11-19 · 为后续更新预留页内空间

PostgreSQL 和本项目现在都能在普通堆表建表时指定 `FILLFACTOR`，默认值为 100。受测的合法值、越界值、重复设置和跳过已有表的结果一致。PostgreSQL 保存通用关系选项并从 `pg_class.reloptions` 公开；本项目将填充比例保存在表的参数文件里供存储层读取，目录查询和其他表级选项仍有缺口。[PostgreSQL 18 文档](https://www.postgresql.org/docs/18/sql-createtable.html) · [本项目对照](test-report.md#create-table-fillfactor-20261119)

## 2026-11-19 · 插入记录时怎样查找空位

PostgreSQL 优先尝试上次插入的页，再从分层空闲空间映射取候选，或者在缺少线索时尝试尾页。锁住页后会再次确认实际空间，淘汰不合适的候选时可更新映射。本项目的空闲空间记录按页保存百分比，先尝试最多两条推荐和尾页，然后回退到原有逐页查找。分层搜索、上次插入页缓存和批量写入的缓冲区策略仍有差距。[PostgreSQL 选页源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/access/heap/hio.c) · [空闲空间源码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/storage/freespace/freespace.c) · [本项目验证](test-report.md#insert-page-choice-20261119)

## 2026-11-19 · 更新后的新版本写到哪里

PostgreSQL 和本项目都可优先使用原页，并利用空闲空间记录选择其他页。本项目的空闲空间记录仍是按页保存的百分比，推荐失效时回退扫描。PostgreSQL 的分层空闲空间搜索、选择性跳过索引写入和版本链清理仍有差距。[PostgreSQL 选页实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/access/heap/hio.c) · [PostgreSQL 更新实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/access/heap/heapam.c) · [本项目测试](test-report.md#update-version-placement-20261119)

## 2026-11-19 · 并发修改同一行时怎样处理

在 `READ COMMITTED` 下，PostgreSQL 先按语句开始时能看到的数据寻找目标行。遇到其他事务正在修改的行时，它等该事务结束，并对最新版本重查条件。较强隔离级别下，另一事务已经提交的修改会导致 `40001`。PostgreSQL 的 `EvalPlanQual` 还能重新检查连接等更复杂的执行计划。[PostgreSQL 隔离级别](https://www.postgresql.org/docs/18/transaction-iso.html) · [PostgreSQL 18.6 修改执行器](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeModifyTable.c)

SQLite 同时只允许一个写事务。在 WAL 模式下，持有旧快照的读事务尝试升级为写事务时，会收到 `SQLITE_BUSY_SNAPSHOT`。DuckDB 1.4 允许不同线程同时写不同的行，两个线程修改同一行时，后一个操作收到冲突错误。[SQLite 隔离说明](https://www.sqlite.org/isolation.html) · [DuckDB 1.4 并发说明](https://duckdb.org/docs/1.4/connect/concurrency)

项目采用 PostgreSQL 的语义处理所测普通写入，并扩展到使用类型绑定执行的 `UPDATE ... FROM` 和 `DELETE ... USING`。两个数据库都用原先匹配的来源记录重查新目标值。项目的旧版退回路径、完整的执行计划重查、分区行移动和锁级别仍待补齐。[普通写入验证](test-report.md#same-row-writes-20261119) · [带来源表的对照](test-report.md#joined-row-writes-20261119)

## 2026-11-19 · 用主键找要更新或删除的行

PostgreSQL 可以从执行计划的索引扫描取得要修改的行。项目现在可借单列整数主键索引直接定位部分 `UPDATE` 和 `DELETE` 的目标。旧快照、索引未命中及复杂条件仍走表扫描。[PostgreSQL 修改执行器](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeModifyTable.c) · [测试报告](test-report.md#integer-primary-key-mutation-20261119)

项目把这种查找扩展到带来源表的常见写入。受测的整数主键命中可以绕开与目标无关的数据页。受测主键未命中时，项目仍扫描目标并计算来源，而 PostgreSQL 18.6 的对照语句省去了来源计算。面向连接的通用计划选择仍有差距。[PostgreSQL 执行计划](https://www.postgresql.org/docs/18/using-explain.html) · [对照结果](test-report.md#joined-primary-key-target-20261119)

## 2026-11-19 · 分区表统计和重写参数

两边的父表统计进度都按已完成分区数以及当前分区的采样页数报告。PostgreSQL 可报告当前分区的表编号，默认还会继续更新各个子表自己的统计。项目当前支持父表管理的物理分区，单独的子表编号和统计仍待实现。两边都拒绝指定正数并行工作数的 `VACUUM FULL`，允许 `PARALLEL 0`。[PostgreSQL 进度字段](https://www.postgresql.org/docs/18/progress-reporting.html) · [实测范围](test-report.md#partition-analyze-vacuum-parallel-20261119)

## 2026-11-19 · 重写表时怎样找空位

PostgreSQL 18.6 的堆重写会在当前页填入记录，达到页空间限制后按页批量提交并切到下一页。项目重写保留原有的放置规则，使用内存里的可用空间记录直接找到第一个够用的页，因此还能回填较早的页。两边都在重写时避开从头查找每个可能的页。文件格式和页空间策略各有不同。[PostgreSQL 堆重写实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/access/heap/rewriteheap.c)

## 2026-11-19 · 三个表维护进度视图

PostgreSQL 18.6 和本项目都把 `VACUUM FULL` 进度放在 `pg_stat_progress_cluster` 中。普通清理和统计更新各有自己的视图。项目按实际页和已写入的行计数，表编号来自目录。数据库编号、分区统计、限速等待时间、自动维护进程和视图组合查询仍有差距。[PostgreSQL 进度说明](https://www.postgresql.org/docs/18/progress-reporting.html) · [本项目用法](MANUAL.md#查看表维护进度)

## 2026-11-19 · 指定要分析的列

PostgreSQL 18.6 和本项目都支持在 `ANALYZE` 或 `VACUUM (ANALYZE)` 的表名后指定列。共同检查覆盖按列刷新、多表、重复和缺失列、带引号的列名，以及重写后继续读取未选列的统计。采样实现、统计类型覆盖、继承统计和命令进度视图仍有差距。[PostgreSQL ANALYZE](https://www.postgresql.org/docs/18/sql-analyze.html) · [本项目验证](test-report.md#vacuum-target-columns-20261119)

## 2026-11-19 · 默认重写同时整理主表和长值

PostgreSQL 18.6 默认在一次事务中替换主表、长值附表及其索引。本项目现在也能用同一条 `VACUUM FULL` 重写主表和已有的长值存储。两边都用带实际删除记录的样本验证了文件缩小、剩余值和索引查找。文件格式、维护统计、带列清单的 `VACUUM` 和跨重写时间点恢复仍有差距。[PostgreSQL 重写实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/cluster.c) · [本项目验证](test-report.md#vacuum-combined-full-20261119)

## 2026-11-18 · 可分别清理主表和长值

与 PostgreSQL 18.6 一样，普通 `VACUUM` 可分别指定主表和长值清理。`ANALYZE` 可以独立运行。两者也都支持用 `VACUUM (FULL, PROCESS_MAIN FALSE)` 只重写长值数据和索引。本项目使用自己的文件格式和恢复记录。带列清单的 `VACUUM`、完整的长值重写历史回放和 PostgreSQL 的维护统计仍有差距。[PostgreSQL VACUUM](https://www.postgresql.org/docs/18/sql-vacuum.html) · [本项目验证](test-report.md#vacuum-toast-full-20261118)

## 2026-11-18 · 普通清理和长值

PostgreSQL 从关系目录确定是否需要清理长值附表。本项目先确认自己的长值文件存在并含数据页，再扫描主表来确定哪些值被引用。这个改进覆盖无外置长值的情况。列的存储方式、压缩算法和可见性范围仍有差距。[PostgreSQL 清理实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/vacuum.c) · [本项目测试](test-report.md#vacuum-empty-toast-20261118)

## 2026-11-18 · 重写后的 BRIN 摘要

PostgreSQL 的 `VACUUM FULL` 用新文件重写表并重建关联索引。本项目已修复该操作完成后 BRIN 仍指向旧表标识的问题。BRIN 使用项目自己的文件格式，自动摘要和完整的索引维护仍有差距。[PostgreSQL 表重写代码](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/cluster.c) · [本项目测试](test-report.md#brin-vacuum-full-generation-20261118)

PostgreSQL 18.6 可以清除单个范围的摘要、补建单个范围的摘要，或补建所有缺少的摘要。52 次参考操作覆盖了重复调用、追加数据和事务回滚。去掉一个摘要后，查询仍会读取该范围里的数据页。本项目目前重建整份摘要。要实现按范围维护，需要区分「没有摘要，必须读取原数据」和「该范围确实没有行」。[PostgreSQL BRIN 维护](https://www.postgresql.org/docs/18/brin.html#BRIN-OPERATION) · [维护函数](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-ADMIN-INDEX)

## 2026-11-18 · 写入时查找要维护的索引

PostgreSQL 从系统目录取得索引清单，并通过关系缓存复用清单。本项目现在为五种索引共用一次目录遍历。文件名仍参与识别索引，统一的索引目录与逐行维护是后续差距。[PostgreSQL 18.6 关系缓存](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/cache/relcache.c) · [本项目测试](test-report.md#specialized-index-discovery-20261118)

## 2026-11-18 · 整数主键按索引顺序分页

PostgreSQL 可以按数值顺序读 B 树并在取够行时结束。本项目在默认表空间的普通单列整数主键索引上也支持这一行为。范围条件由同一整数主键与整数字面量或这次传入的非空有符号整数参数组成时，项目可以按范围定位起点。实际取得有序主键页后，可保留输入顺序，仍会读取和验证这些行。复合键、通用代价选择以及部分未声明参数类型的协议行为仍有差距。[按参数定位](test-report.md#prepared-integer-primary-key-range-20261118) · [复用索引顺序](test-report.md#presorted-integer-primary-key-page-20261118)

PostgreSQL 的有序索引扫描可以在读取行后检查额外的筛选条件，再用 `LIMIT` 控制返回行数。本项目已支持其中直接检查普通整数、布尔和文本列是否为 NULL 的用法。通用筛选表达式和索引选择仍有差距。[测试范围](test-report.md#integer-primary-key-null-filter-20261118) · [性能报告](performance-report-2026-10-30.md#integer-primary-key-null-filter-cost-20261118)

## 2026-11-18 · 排序后计算查询结果

PostgreSQL 可以按索引顺序读取，也可以先扫描再排序。实测中，走索引时会按需转换数组，普通堆表排序会先做简单转换，较昂贵的转换可放在排序之后。该批改善了项目版的取行行为。[测试范围](test-report.md#ordered-projection-20261118) · [实际性能](performance-report-2026-10-30.md#ordered-projection-cost-20261118) · [随后实现的整数主键分页](test-report.md#integer-primary-key-page-20261118)

## 2026-11-18 · 从包含联接的子查询写入

补齐所测普通嵌套 `INSERT … SELECT`，旧版通过 260 项，新版与 PostgreSQL 18.6 均通过 273 项。覆盖多层子查询、数组域、有限取数、延迟错误、重新绑定参数及崩溃恢复。写入性能比较项目新旧版本。视图、带 `WITH` 的写入和集合查询的完整组合仍待验证。[测试范围](test-report.md#nested-insert-20261118) · [性能报告](performance-report-2026-10-30.md#nested-insert-cost-20261118)

## 2026-11-18 · 按整数数组查找联接行

这一批核对重复匹配、不同数组值落入同一查找位置、有限取数和崩溃恢复。PostgreSQL 18.6 作为结果对照，查询耗时比较本项目的新旧版本。通用哈希联接和超出内存后的磁盘分批执行仍有差距。[测试范围](test-report.md#array-lookup-20261118) · [实际性能](performance-report-2026-10-30.md#array-lookup-cost-20261118)

## 2026-11-18 · 整数数组联接与带域类型的空值

新对照覆盖三种整数数组、重复匹配、不同形状和下界、空值、多个联接键和取数顺序，并验证普通同域空值写入。PostgreSQL 18.6 与当时的项目版均通过 218 项。需要先排序的来源仍有已知差异。[正反向测试](test-report.md#integer-array-join-20261118) · [实际性能](performance-report-2026-10-30.md#integer-array-join-cost-20261118) · [嵌套写入后续修复](test-report.md#nested-insert-20261118)

## 2026-11-18 · 用数组列连接表

新增 128 项对照，旧版通过 69 项，新版与 PostgreSQL 18.6 全部通过。覆盖四种联接、类型和字段来源、数组长度限制，以及相同或不同域的查询写入。不同基础类型的数组需要先统一类型。独立的跨数组比较和部分 `JOIN ON` 转换仍有差距。外联接空值写入的后续结果见上节。[测试与剩余差距](test-report.md#array-using-20261118) · [实际性能](performance-report-2026-10-30.md#array-using-cost-20261118)

## 2026-11-18 · 派生表里的数组

所测派生查询和 `INSERT … SELECT` 保留了普通数组和数组域的实际类型、下界、引用编号与 NULL。原始 106 项对照从 58 项提高到 95 项，余下差异涉及 PostgreSQL 展开后多次执行的域检查、视图结果类型和视图写入。新增回归覆盖查询写入、数组排序和去重，两边均通过 149 项。[矩阵与剩余差距](test-report.md#derived-arrays-20261118) · [实际性能](performance-report-2026-10-30.md#derived-array-cost-20261118)

## 2026-11-18 · 内置数组的域支持

双方所测的 244 项读写和协议结果一致，覆盖默认值、约束、继承、显式转换、数组元素长度及回滚。PostgreSQL 可以修改已有数组域列和域约束，本项目对所测结构修改会返回 `0A000`。数组元素为域、触发器和 NULL 赋值规则仍有差距。派生表读写的后续结果见上节。原功能总账的 273 项状态保持。[矩阵和实际失败](test-report.md#array-domains-20261118)

本批与 PostgreSQL 对照正确性。耗时测量比较本项目的新旧版本，覆盖查询、插入、更新和建表。此前与 PostgreSQL 的速度倍率仍对应其原始的小表测试。[本批性能测量](performance-report-2026-10-30.md#array-domain-cost-20261118)

## 2026-11-17：引用字段的查询进展

类型和表引用的所测普通查询现在返回名称，引用数组排序、去重、临时表类型和父子表统一排序分页已有实际回归。新增 243 条协议检查在本项目与 PostgreSQL 18.6 上均通过，29 个选定 C++ 入口通过。完整对照仍发现继承 CAST 漏读子表、来源属性编号差异；当时的数组域建表和从视图 INSERT SELECT 返回不支持。两份完整诊断保留全部正向预期，原 273 项状态及用户暂缓范围不变。[测试结果与剩余差距](test-report.md#stored-reference-output-20261117)。

本次只比较项目新旧版本，没有重测与 PostgreSQL 的速度倍率。2,688 次实际 SQL 计时均保留并独立核对，六项比较都超过预设波动门槛，尚不能确认稳定提速或排除回退。[逐项性能结果](performance-report-2026-10-30.md#stored-reference-output-cost-20261117)。下文较早的倍率仅适用于其当时冻结的程序与工作负载。

## 2026-11-08：锁升级和两阶段事务的未完成差距

原生锁升级失败测试的 866 项断言中有 258 项失败，涉及旧锁保留和保存点恢复；这是测试断言数量，不是 SQL 或 PostgreSQL 对照结果。另在单一共享锁场景的 43 对分配注入中，原实现有 3 个故障点；独立进程锁抽样也观察到部分失败清理后无法取得锁，但其子探针未保存 syscall、errno 和原始 wait 状态。[测试范围和限制](test-report.md#native-lock-fault-review-20261108)。

两阶段锁交接的原生诊断在一次分配失败后观察到锁所有权交接不完整及进程锁继续阻塞；尚无该故障的 SQL PREPARE、WAL 或重启结果，不能推断持久化损坏。上述问题未修复，不能宣称事务锁行为达到 PostgreSQL；功能账目仍保持开放，R51 DROP DATABASE/cache contention 等用户暂缓范围不变。

## 2026-11-08：与 PostgreSQL 的小表查询对照

同机单连接的七项查询中，五项达到预设计时门槛：D43 耗时为 PostgreSQL 18.6 的 23.50～65.52 倍（各项三轮中位数）；另两项因 PostgreSQL CPU 批次过短而未计算倍率。测试仅覆盖指定的 64 行暖运行和常量查询，共享主机负载及前后基线漂移明显，不能外推为全库或生产负载倍率。[测试方法、逐项结果及限制](performance-report-2026-10-30.md#pg18-d43-cross-20261108)。

**当前查询差距：** 普通表零列投影可能漏发真实行，并受未选大字段故障影响；某些函数参数中的转义引号和拼接会产生错误成功值。所测写入及冷重启控制与 PostgreSQL 一致，不能将查询反例扩展为已证实的持久化错误。[测试与限制](test-report.md#zero-projection-argument-audit-20261108)。

在冻结基线的所测 `replace` 查询中，参数内的序列调用和除零错误被遗漏；实际 PostgreSQL 对照以及严格 UDF、CASE 和 COALESCE 的范围控制见[标量参数副作用对照](test-report.md#ordinary-scalar-effects-20261108)。该结果不能泛化为所有严格函数或所有执行路径，现有功能账目不变。

扩展到 89 条普通 SQL 和冷重启动作后，含页 pin 修复的冻结版本仍有 13 条语义差异，包括非 NULL 参数的序列副作用和一条动态 DDL 重入观察；所测写入及重启值与 PostgreSQL 一致。另在项目私有存储格式中，三个变长字段目录损坏样例虽有有效页校验和，查询仍返回错误成功值；这不是 PostgreSQL 磁盘格式对照，详见[损坏目录验证](test-report.md#row-variable-directory-corruption-20261108)。两项缺口均未修复。

## 2026-11-08：numeric 十进制对数

`ln`、`log` 和 `log10` 的 numeric 路径改为十进制计算，修复已复现的高精度错误值和默认值冷重启反例；浮点重载保留。最终 CMake 程序、选定 SQL／事务／冷重启与独立原语测试的范围见[测试报告](test-report.md#numeric-log-decimal-20261108)。Numeric 精度和 scale 仍各限 1000 位，部分错误诊断与 PostgreSQL 不同；长除数的所有舍入边界尚未证明完全一致。零列缺陷仍开放。

本次新旧性能对照在共享主机上波动较大，部分查询有变慢结果，后续复测也有正有负，不能宣称稳定提速或没有回退。[逐项结果与限制](performance-report-2026-10-30.md#numeric-log-cost-20261108)。

## 2026-11-08：内置函数签名缓存

内置函数的固定签名改为只读、进程生命周期的共享声明，避免在绑定和调用时反复创建；数组位置函数的参数相关签名仍逐次生成，同名函数覆盖和参数求值路径保持既有语义。选定回归与异常分配注入的范围见[测试报告](test-report.md#d43-signature-cache-20261108)。

在共享主机上，同项目新旧版本的 64 行查询中，部分函数有改善，但旧版自身漂移较大，不能据此宣称全库提速；逐项结果与限制见[性能报告](performance-report-2026-10-30.md#d43-signature-cache-20261108)。D43 的对数精度与零列缺陷当时仍未修复，详见测试报告。

**D40/D42/D43 的历史数据正确性问题：** 高精度 `ln`／`log` 可能返回错误成功值，并可通过 DEFAULT 写入、跨重启保留；零列查询可能漏发实际行，零列表部分读写也失败。D43 的零列反例和对数默认值冷重启问题均已复现；该段保留旧版实测；当前对数结果见上方，零列问题仍开放。详见[对数测试](test-report.md#numeric-log-gap-20261108)与[零列测试](test-report.md#zero-column-gap-20261108)。

## 2026-11-08：十进制指数与浮点函数类型

`exp(numeric)` 改用十进制计算，保留 PostgreSQL 18.6 的结果标度与中间舍入行为；`exp`、`ln`、`log` 和 `log10` 按参数类型区分 numeric 与 double precision 重载。此前 `exp(100::numeric)` 和 `exp(-40::integer)` 的错误结果可能被写入默认值，现有针对这些示例的查询及持久化回归用于防止再次发生。

实现参考 [PostgreSQL 18.6 numeric.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/numeric.c) 和 [float.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/float.c)。[SQLite 数学函数](https://www.sqlite.org/lang_mathfunc.html)通常返回近似值，[DuckDB 的 exp 实现](https://github.com/duckdb/duckdb/blob/v1.5.6/extension/core_functions/scalar/math/numeric.cpp)使用 double；两者不能替代 PostgreSQL numeric 结果作为精度判据。

本项目 Numeric 的有效精度和 scale 仍各限 1000 位，部分 PostgreSQL 可计算的结果返回 `22003`；D40 的 `ln`／`log` 高精度数值路径存在差距（当前状态见上方），D39 的部分 binary Numeric 输入边界在 D40 尚未重新验证。有限测试不代表全面 PostgreSQL 兼容；六条简单 SQL 的新旧测量波动较大，部分变慢，不能宣称全库提速。详见[测试报告](test-report.md#d40-numeric-exp-20261108)及[性能报告](performance-report-2026-10-30.md#d40-numeric-exp-cost-20261108)。

## 2026-11-08：大数和超越函数差距（D39 历史）

本轮修复部分大 numeric 定点／`V` 格式在展示补零或移位时触及项目内部精度上限的问题；限定的模板与类型现在按十进制舍入。整数长除法改为复用局部余数缓冲区，减少反复分配；不改变 1000 位上限。`exp`／`ln`／`log` 不再把部分超出 double 范围的 numeric 静默当成零，精确的 10 的幂采用逐位验证和范围检查。无法精确支持的部分输入会明确返回 `22003`，不是 PostgreSQL 等价。

**D39 历史风险：** `exp(100::numeric)` 曾与 PostgreSQL 不同，某些 integer／float8 的极小 `exp` 曾错误返回零，这些值可作为默认值持久化。后续实现及当前限制见上方最新状态；D39 并未解决完整十进制超越函数、默认值来源或复杂格式模板。

D39 正常冻结候选的 61 个选定原生入口与 27 组既有协议对照通过，完整套件未运行。针对定点格式的每版本 1,365 个动作涵盖直接查询、事务和冷重启，已测 `FMPL` 默认值与 PG 一致；复杂模板仍有差距。组件基准有 CPU 改善，真实 SQL 窗口有快有慢且漂移明显，不能声称整体数据库提速。详情见[测试报告](test-report.md#d39-numeric-20261108)与[性能报告](performance-report-2026-10-30.md#d39-numeric-cost-20261108)。



## 2026-11-08：数值科学格式与目录差距

本轮修正 `div(NaN, 0)` 的处理顺序：NaN 先于零除数处理；有限值和无穷除以零仍报 `22012`。简单 `9`／`0`／小数点模板的 numeric 和 bigint `EEEE`／`eeee` 科学记数格式改为精确十进制舍入，避免极大或极小值因转换成二进制浮点而静默显示为零。integer 等其他类型仍沿原路径；含 `G`、引号、`FM` 等复杂模板的既有差异未在本轮解决。

Linux 上，目录发布状态的一次扫描复用已打开的父目录句柄，仍逐个检查目录文件的身份和时间戳，并在结束前核对路径仍指向同一目录；描述符不可用时回退旧检查路径，非 Linux 保留旧路径。扫描期间目录换出后又换回等竞态仍不能保证检出，不能称作跨进程线性一致。

D37 正常冻结候选的 58 个选定原生入口实际退出 0；27 组既有协议的 1,105 对回复没有未解释变化。NaN 的 82 个动作在 PG18.6 对照中结果、SQLSTATE 和事务状态一致；简单科学格式的 210 个样例与 PG 的结果及描述符一致。完整套件未运行，其他模板、无约束 numeric 范围、错误回复帧及默认值来源仍有差距。此前文档称 `div(NaN,0)` 不同属于历史结果，已被本节覆盖。详情见[测试报告](test-report.md#d37-numeric-catalog-20261108)和[性能报告](performance-report-2026-10-30.md#d37-catalog-scan-20261108)。



## 2026-11-08：数值与默认值差距

本轮修正部分 numeric 范围错误被 DEFAULT 当作字面量回退的问题：已求值的算术范围异常转换为 `22003`，整数商和余数使用直接整数除法，避免舍入或过大的中间乘积；`to_char` 已绑定的数值格式化范围异常也转换为 `22003`。这些处理仅覆盖对应异常和已测路径，其他无结构错误仍可能回退原文。旧式原生默认值兼容有变化：原文恰为这些会越界的表达式时，现在报错。

PostgreSQL 18 的无约束 numeric 支持的范围远大于项目当前 1000 位内部上限；合法输入、中间值和结果都可能触及项目限制。[PostgreSQL 数值类型](https://www.postgresql.org/docs/18/datatype-numeric.html)。`div(NaN, 0)` 的既有差异、其他函数的未结构化错误回退、目录锁等待和 `pg_attrdef.adbin` 等差距仍未解决。旧 D30 正文对所测算术默认值“写入原文”的描述属于历史结果，已由本节局部修复；不代表所有默认值问题均已修复。

D35 冻结候选 55 个标准原生入口与 1 个二进制入口均实际退出 0；11 组数值和默认值协议对照、27 组 D34→D35 逐动作比较和选定 SSI 对照已完成。`to_char` 大数默认值现返回 `22003`，已测 INSERT、UPDATE、批量插入和 ADD COLUMN 不再写入 SQL 原文；PostgreSQL 对该合法输入返回格式化值，仍不等价。未运行完整测试套件。详细计数、旧失败与性能测量限制见[测试报告](test-report.md#d35-numeric-tochar-20261108)及[性能报告](performance-report-2026-10-30.md#d35-numeric-tochar-performance-20261108)。



## 2026-11-08：D30 目录与事务差距

本轮对普通 `pg_attrdef`／`pg_depend` SELECT 使用实际目录字段的按需快照，声明与数据在同一把锁下复制；公共完整快照仍保留默认表达式和依赖。`pg_attrdef.adbin` 的 PostgreSQL 节点树仍未实现，不声称目录完整等价。活动事务与 SSI 登记改为进程寿命，避免全局数据库对象退出时访问已析构的登记容器。

D30 实际通过 52 个选定标准原生入口及 1 个独立二进制入口，合计 53；23 组协议与 D28 的 994 条成对回复在行、tag、SQLSTATE、Ready、T7 和错误／通知上相同。针对投影的 PG／D27／D28／D30 热冷对照共 76 条回复，按实际 OID 对应后选定行、tag、SQLSTATE、Ready 无差异，但 PG 原始行描述符 T7 仍有 4 项差异。完整发现审计的 2,023 条回复包含历史和参照，不能说成 2,023 条新测试；未运行完整测试套件。

已知缺陷仍未修复：合法默认表达式 `(('1e500'::numeric * '1e500'::numeric)::text)` 在 PostgreSQL 18.6 得到 1001 位结果，而 D27／D28／D30 会把 SQL 源文写入 TEXT；现有无约束 numeric、持久默认值来源和无 SQLSTATE 回退均需继续修复。并发对照中 PostgreSQL 可在另一会话 DDL 未提交时立即读取旧目录，D27／D28／D30 的同类查询等待超过 5 秒，ROLLBACK 后才返回；未采集线程等待栈，库级锁路径只是依据代码和时间顺序的推断。

仅对选定单连接暖缓存工作负载测量：D27→D30→D27 固定顺序的目录读取窗口为 27 个实际正常退出进程、27,648 条 SQL；事务窗口为 9 个进程、3,456 条计时 SQL，另有 324 条控制 SQL。两窗口完整回复字节一致、无 mismatch。各场景结果有正有负，ordinary `pg_attrdef` 相对前／后基线墙钟分别退化 5.8%／6.5%，两项生产变更同时存在且每角色仅 3 次，不能据此归因或证明投影改善，也不能宣称整体提速或与 PostgreSQL 性能等价。

设计与差距参考 [PostgreSQL 默认值](https://www.postgresql.org/docs/18/ddl-default.html)、[pg_attrdef](https://www.postgresql.org/docs/18/catalog-pg-attrdef.html)、[numeric 范围](https://www.postgresql.org/docs/18/datatype-numeric.html)、[SQLite DEFAULT](https://www.sqlite.org/lang_createtable.html) 和 [DuckDB v1.4.1 默认值绑定实现](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.1/src/planner/binder/statement/bind_insert.cpp)。SQLite 与 DuckDB 行为不等同于 PostgreSQL。


## D27 DEFAULT 错误对照与限制（2026-11-08）

本轮修正 INSERT 遗漏列 DEFAULT 求值中已有 SQLSTATE 或有效结尾 SQLSTATE 标记的错误传播，并对 substring 负长度保留 `22011`；目录声明读取按需省去 DEFAULT 对象图复制，公共默认快照仍完整。没有 SQLSTATE 且没有有效标记的旧/native 求值失败仍可退回原始字面量；域、旧数据及全部错误类型不构成全面兼容承诺。

D24 的 `catalog_default_snapshot_demand_test` 实际退出 `-11`，诊断指向进程退出时内置目录声明生命周期问题；D27 的 ASan 只注入运行时库而非全核心插桩，该控制正常退出 0，不能称为完整 ASan 构建。D26 的 `regexp_replace('abc','[','z')` DEFAULT 在热态和冷重启后均错误写入表达式文本；D27 返回 PostgreSQL 的 `2201B` 且未写入行。历史失败记录保留。

最终 D27 49 个常规原生入口加 1 个二进制控制，共 50 个实际退出 0；22 个协议族独立审计覆盖 1,998 SQL、76 个正常结束 Main。五族 DEFAULT 错误独立对照记录 342 SQL、48 个正常结束 Main，所比较语义字段没有候选差异；两个族的原始行描述符 T7 仍有差异，不能称为完整 wire 或 PostgreSQL 等价。未运行完整测试套件，功能仍为未接受草稿。

指定成本测试为共享服务器暖缓存、单连接、32 行热表：D23 与 D27 按每场景旧版前/新版/旧版后各 3 个进程固定顺序执行，共 27 个正常结束 Main、36,720 条 SQL；各场景完整回复字节一致。24 组墙钟和 CPU 结果有正有负，narrow 前后基线波动尤大；没有证明整体加速，也没有 PostgreSQL 速度对比。 本次对照只说明所列语义字段，并不证明 PostgreSQL 全功能、协议完全等价或生产性能差距。 [报告](performance-report-2026-10-30.md#default-runtime-snapshot-evidence-20261108)。


## 最新 P43/D23（2026-11-07）：DEFAULT 真实对象依赖

依据PG18.6实际运行和固定版本源码实现对象常量早绑定、默认对象独立身份、所属列自动依赖及正常引用。50条主对照在实际目标OID对应后，返回值／标签／SQLSTATE／Ready无差异；扩展32条仍保留晚绑定序列缺失错误码和删列目录占位差异。完整T7另列，不能从这些用例推导全库兼容。

46个选定原生入口及两份正式协议脚本通过。原273项保持22 complete／166 partial／70 unverified／15用户暂缓，292条历史保持。P43未接受，正常性能NOTRUN，目录全行／通用表达式依赖与其他非暂缓缺口继续推进。[证据与边界](performance-report-2026-10-30.md#attribute-default-objects-draft-20261107)。


<!-- P43-D20 unaccepted draft checkpoint -->
## 最新 P43/D20（2026-11-07）：普通旧数组目录取得有限闭环

本轮对照真实PG18.6的类型／字段目录和内建类型省略依赖的处理，完成有实际物理schema佐证的普通表数组升级；保留已存在的正常类型边并调整其目标，不补造缺失的内建依赖。真实旧库21条操作链的值、NULL、事务状态和tag一致，按各自真实表OID对应后完整7字段T7也一致；这只覆盖选定用例。

原273项保持22 complete／166 partial／70 unverified／15用户暂缓。REGCLASS默认依赖、完整序列与恢复、目录全行、其他非暂缓功能及性能差距仍开放；正常成本未测，不能据此缩小整库速度差距。[结果和未完项](performance-report-2026-10-30.md#physical-array-catalog-upgrade-draft-20261107)；下方D18及更早数据为对应历史版本。


<!-- P43-D18 unaccepted draft checkpoint -->
## 最新源代码检查点：P43/D18 仍未接受（2026-11-07）

目录／类型有实际进展：保存真实 `pg_type`／`pg_attribute`／`pg_class` 声明和系统属性元数据，110个已有内建类型的有限物理属性投影与实际PG参照一致；6个目录字段改用真实 `C` 排序规则，选定 `pg_type` 别名／CTE的完整来源描述匹配PG。元数据声明不等于实现全部值、堆格式或SQL系统列；未提供真实值生产者的字段仍拒绝，不能把短schema或虚构NULL称为完整目录。

原273项仍为22 complete／166 partial／70 unverified／15用户暂缓，未关闭任何项目。REGCLASS依赖、旧数组迁移、序列完整语义和恢复、生成列ALTER TYPE、错误阶段及其他非暂缓功能仍需补齐；本批正常性能未测试，无法据此缩小整库性能差距。版本、全部观察及边界见[草稿报告](performance-report-2026-10-30.md#identity-sequence-catalog-draft-20261107)。下方性能数字保留对应历史程序口径。


## 最新 P42（2026-11-07）：读取提交成本的局部进展

参考PG18.6未分配持久XID／未写WAL的提交分支和DuckDB ChangesMade检查，为本项目已证实无持久事务变更的读取消除无用COMMIT／CLOG同步；项目仍在BEGIN分配内部保留ID，尚非PG完整惰性XID／事务实现。95条SQL在实际PG／旧／新程序完整跑完，模型值／NULL／tag／事务状态正确，旧新请求回复逐字节一致；映射实际items OID后仍保留8处catalog或未映射TEMP／aux来源观察，不称为全PG协议等价。

621次正常成本与全部负向、33项选定原生中的2个未通过项详见[报告](performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)。这是本项目两版对照，没有新增PG速度基准；原273状态仍22／166／70／15，Goal active，继续补齐普通功能和性能。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## 2026-11-07：长VARCHAR接近PG，功能和性能仍partial

新增VARCHAR逻辑字符长度元数据：显式长度支持1..10485760字符，未指定长度使用独立的无界标记，名义物理dsize仍不超过65535。列声明、pg_attribute／RowDescription、普通写入／更新、数组元素、domain／generated赋值、ALTER及项目逻辑导出使用真实长度。标量不再把65535字节误当成字符上限；已验证70000字符／245000字节UTF8和1MiB无界值。内部无界字节上限为1073741819，但没有实际验收1GiB值或整个PG字符类型族。CHAR旧限制、完整encoding／collation语义仍开放。

参考[PostgreSQL18字符类型说明](https://www.postgresql.org/docs/18/datatype-character.html)及[固定varchar.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/utils/adt/varchar.c)的字符计数、上限和无界语义；[固定hio.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/heap/hio.c)的bulk-insert当前缓冲与目标页说明了保留搜索位置的设计。这里仅实现每个长值的局部游标，并保留串行物理布局证明。[SQLite](https://www.sqlite.org/datatype3.html)和[DuckDB](https://duckdb.org/docs/stable/sql/data_types/text)通常忽略VARCHAR(n)长度，不作为PG有界语义依据。PG／SQLite原HTTP字节及固定PG源码、响应头与SHA保存；DuckDB直接抓取HTTP403的响应也保留，另存官方网页提取结果，不将它说成HTTP200原文。未进行跨数据库速度排名。

新141条VARCHAR SQL在独立PG18.6、旧bf9、候选C6均完整执行，三角色共423条核心SQL、305行／1249单元／132 NULL、228,538,937字节；10个正常Main0及2个预期SIGKILL -9。全值、顺序、重复数、NULL、状态／tag／Ready和全部七个描述字段与实际PG核对，关系OID只用各自真实探针映射。C6普通数据要求通过，仍有17个pg_class来源描述差异，以及3个大常量ADD DEFAULT的PG实现限制差异：PG对70000字节常量报54000，随后读取／DROP列报42703，项目接受并正确存取；这些差异保持开放，strict PG=false。137条V1参考模型的4个假设被真实PG否定，首次actual1及全部原始数据保留；141条V2在任何候选执行前冻结，另加4097字节默认值成功控制，没有按候选结果降低要求。

旧typeMod=-1按原dsize字符上限读取，不从65535猜测成无界；新显式正长度和私有-2保存在已有ITM1扩展，-2输出PG时为-1。旧数据可以由新版读取；旧程序没有识别这些新VARCHAR修饰符，不能承诺降级读取新写的schema。旧无界意图缺少可判定来源时不自动迁移，须明确新DDL／ALTER。名义dsize、SQL字符数和实际UTF8字节数分别处理。没有改变元组布局、校验、TOAST指针格式或WAL格式；仍是项目自定义存储，不是PG物理格式等价。

本批功能有进展，长值写入部分提速；全面提速尚未成立。新增1MiB混合插入墙钟中位1801.508072ms，相对旧bf9首测／同旧回测-10.720%／-10.799%；更新RETURNING 411.759537ms，-13.372%／-12.280%；重新插入1431.222130ms，-9.197%／-10.414%。对应服务器CPU分别-12.689%／-12.822%、-20.934%／-21.267%、-12.666%／-12.762%。

负向保持OPEN：新长值DELETE RETURNING墙钟43.553088ms，+9.954%／+23.830%，服务器CPU+67.962%／+39.067%；首次VACUUM墙钟25.753918ms，+22.574%／+1.265%；FULL墙钟218.226818ms，+32.204%／+21.215%，服务器CPU+7.756%／+1.774%。新增完整值读取墙钟+2.080%／-2.014%，结果混合。通用270项矩阵的30个区域，墙钟6低于两旧／18高于两旧／6居间；服务器CPU10／8／12，客户端CPU6／14／10。没有排除、重采或把漂移归因为已证实的产品原因；全部负向与原因继续开放。

原273状态不变，新增partial历史291；无PG性能排名。[完整报告](performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 2026-11-07：FULL开销有限改善，仍非PG功能／性能等价

普通VACUUM FULL在备份前与重建后，只刷新目标逻辑关系及其分区／TOAST缓存，避免把其他表的脏页一并刷出。物理文件已同步，且在任何重建写入前全部unlink：这些文件优先用硬链接保留旧inode作为回滚映像，失败则完整复制；schema和specialized-dirty元数据继续独立复制。每份备份的fsync、完整大小／模式／SHA256校验，以及manifest、pending、替换文件／schema、WAL、COMMIT和undo的持久化顺序全部保留。本批只改TableManage.cpp的vacuumFull，没有修改全局缓存、index_file、WAL格式、CRC或恢复授权规则。

参考[固定PostgreSQL18 bulk_write.c源码](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/smgr/bulk_write.c)的关系fork同步与批量写入/WAL安排；[SQLite原子提交说明](https://www.sqlite.org/atomiccommit.html)说明恢复日志、数据页与提交标志之间的持久化顺序；[DuckDB single_file_block_manager.cpp](https://raw.githubusercontent.com/duckdb/duckdb/main/src/storage/single_file_block_manager.cpp)的WriteHeader在发布新header前后同步。三份本批HTTP200原文、响应头及SHA256均入证据，DuckDB main以保存的快照SHA为准。这些资料支持限定刷盘范围及保留提交屏障的设计判断；本项目仍使用自身备份／代次协议，没有实现三个数据库算法等价，也没有进行三库速度排名。

本批只证明固定字段FULL墙钟低于两侧旧版：-11.57%／-1.51%；单长／多长FULL服务器CPU均较低，墙钟各为-3.35%／+1.84%、-1.35%／+3.87%，结果混合。没有证明数据库全面提速，也没有追回P39相对更早84bb的全部恢复成本。

明显负向保持OPEN：首次普通VACUUM多长字段墙钟C1中位67.010218ms，旧c8ca首测30.951480ms／回测25.722458ms，即+116.501%／+160.512%。原三次样本分别为C1 [71.510152,26.679347,67.010218]ms、旧首测[35.992784,30.951480,25.821788]ms、旧回测[25.722458,23.927581,29.365169]ms，全部保留。单长字段大批重新插入的墙钟也+51.70%／+58.00%。未因负向重跑、排除样本或更改预期。

本批仍只验收未来普通关系FULL的有限重写／关闭／强杀／回滚恢复。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际损坏库继续保留。跨FULL的PITR／物理基备未验收；数据库排他锁范围仍大于PG表级锁，未验收分区FULL冷开或高并发等价。硬链接需要文件系统支持，实际失败会走复制回退；不宣称所有平台／文件系统已验证。原15项安全／TDE及额外用户暂缓范围保持，这六种FULL故障不是已暂缓的通用IO／分配器／保存点矩阵，也未恢复执行图／child-provider或retained planning／portal工作。

原22、62、72、28条SQL及实际wide OID控制全部重新执行，共185完整请求／2588行／11613单元／969NULL／45,817,233请求回复字节，8个正常Main0和1个预期SIGKILL -9，2315个闭库文件核对。全部完整请求与回复逐字节等于P39真实c8ca观察，再由独立解码器检查完整值／NULL／全部T7／错误／NOTICE／tag／Ready。原PG18.6参照逐字节复用，未声称本批新跑PG；62条中的9项、72条中的7项、28条中的3项SQL子分区差异继续开放，strict PG=false。

原273状态不变，新增partial历史290；下方旧批按历史程序解释。[完整报告](performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 2026-11-07：普通FULL恢复取得有限进展

修复普通VACUUM FULL仍沿用旧物理身份、冷启动把旧页镜像重放到压缩后堆的问题。每次重写分配新物理代次；同一内部事务记录出生、旧代次退休意图／完成及COMMIT。替换前先持久化只属于该关系的原文件、完整模式／大小／SHA256清单及pending记录，替换文件与schema全部落盘后才提交。失败和未提交崩溃先恢复旧文件／schema，再重放WAL；已提交只清理备份。完成回滚的durable receipt防止遗留备份覆盖后续已提交DML。旧09 schema仅在FULL成功时发布新代次及专门的0x23完成记录，普通旧名称重用仍拒绝错误归属。没有放宽CRC、物理身份或页缺口校验。

参考[固定PostgreSQL18源码cluster.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/commands/cluster.c)中的新堆／文件交换与事务索引重建，以及[PG18 VACUUM](https://www.postgresql.org/docs/18/sql-vacuum.html)对FULL的ACCESS EXCLUSIVE锁与额外空间说明；[SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html)通过临时库及rollback journal／WAL替换保证恢复。[DuckDB2.0 VACUUM文档](https://duckdb.org/docs/2.0/sql/statements/vacuum.html)不支持FULL，并将ART回收放在checkpoint存储维护中。前三份原文／HTTP200／响应头／SHA已保存，DuckDB原文和原始HTTP200 receipt从P38逐字节复用。本实现采用本项目的每关系备份与代次方案，没有实现三库算法等价或三库速度排名。

本批没有证明全面提速。270项原样读写矩阵保留27个全新数据库、54个正常Main及1098条完整回复。30个区域的墙钟：7低于两旧／13高于两旧／10居间；服务器CPU：5低于两旧／14高于两旧／11居间；客户端CPU：7低于两旧／12高于两旧／11居间。所有90项比较均列出，性能原因OPEN。 vacuum-full-fixed墙钟 +64.55%／+78.39%；vacuum-full-fixed服务器CPU +10.12%／+10.45%；vacuum-full-single墙钟 +78.30%／+112.77%；vacuum-full-single服务器CPU +17.97%／+38.94%；vacuum-full-overflow墙钟 +131.53%／+114.90%；vacuum-full-overflow服务器CPU +18.05%／+13.79%（相对旧84首测／同旧回测，正数为更慢）。

本批验收只覆盖未来普通关系FULL的有限重写／关闭／强杀／回滚恢复情形。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际失败库仍保留。跨FULL的PITR／物理基备重建未验收，不能仅凭新生命周期记录重建重写后的完整堆。内部事务暂时使用数据库排他锁，再取表锁和cache锁，并非PG表级并发等价；热态分区既有回归通过，不宣称分区冷开闭环。原始15项安全／TDE暂缓与额外用户暂缓记录保持；这六项不是已暂缓的通用IO／分配器／保存点故障矩阵，也未修改执行图／child-provider、retained planning／portal或CPU-sort interrupt。

原62、72、28条SQL及一条实际wide OID控制在本批C1重新执行，共163请求、458行／1800单元／231NULL／33,383,364字节，6个正常Main0和1个预期SIGKILL -9。所有完整请求与回复均与P38实际84bb逐字节一致，独立第二解码器核对全部值、NULL、T7、错误、NOTICE、tag和Ready；复用的是已认证归档中的真实PG18.6参照，没有本批新跑PG的声明。原62的9项差异、72的7项差异及28的3项SQL子分区差异继续保留，strict PG=false。

原273状态不变，新增partial历史289；下方旧批的OPEN说明保留其历史身份。[完整报告](performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：TEXT容量进展与仍存在的恢复／性能差距

修复标量TEXT把持久化名义宽度65535误当逻辑值上限的问题：统一的字符字节限制函数对非数组TEXT使用1,073,741,819字节上限，并让旧式UPDATE校验也使用该函数。既有TOAST读回、插入和行构造因此一致处理完整长值；表结构dsize仍为65535，内联引用和存储格式不变。实际验证到1MiB，未进行接近1GiB的端到端容量验证。旧数据可由新程序读取；旧程序不能读取新写入的超64KiB TEXT，不能据此承诺降级后的值兼容。

原62条SQL重新用旧版和当前执行，共124请求、4个Main正常0，完整请求与回复逐字节一致，真实PG18.6原始oracle和既有9项差异保持。P37原28条维护SQL计划字节不变，另在真实PG／旧版／最终C2重新执行，共84核心请求、144行、553单元、129NULL、2,873,498字节、6个Main正常0；额外4条实际OID／PG版本控制单列。原TEXT70003插入、立即读回、冷开读回3个确切缺口现与PG完整值和T7相符，其余25条旧／新完整回复字节不变。28条中24项值／状态／tag匹配、1项按各库真实正OID核对，仍有SQL子分区创建0A000、子表目录身份及冷开身份缺行3项差异，strict PG=false。

本批完整1500次正常计时没有证明全面提速。仅ID查询服务器CPU比旧版首测／回测高1.78%／1.86%；单长字段首次VACUUM CPU高10.98%／13.76%，墙钟高3.66%／9.08%。多长字段首次VACUUM墙钟低18.80%／高3.52%，CPU低0.33%／高3.27%，属于混合结果。全部负向、旧版漂移、首轮与长尾保留，原因OPEN；本批交付为TEXT容量修复，没有用局部结果宣称整体性能改善。

新发现的普通VACUUM FULL／WAL恢复缺陷仍OPEN，属于下一项必须处理的非暂缓工作。原始旧模式22条矩阵，两版各完成初开16条并正常退出0；当前16条全值正确，但之后冷启动失败，两版各6条冷开SQL未运行。旧版首次观察器的cleanup断言遮蔽了已退出子进程的wait状态，因此其冷开actual exit未记录；修正观察器后只继续原先未启动的当前角色，实际冷开退出-6，未重跑或补写旧版状态。提前运行的独立审计也因缺少完整receipt实际退出1，失败保留。独立CRC32C核对定位LSN6784528的有效8192字节页镜像：cv_sig、物理身份3、旧块22；FULL后同身份的堆仅18页，重放拒绝跨越4页的缺口。CRC为0xd116b066，名义行宽0是合法format2而非本次原因。本批未修改恢复代码；需要正确的重写物理代次／恢复边界，不能靠忽略错误或无界补页关闭。实际失败快照和正常旧模式混合长值种子均保留在本地。

参考PG／SQLite／DuckDB的逻辑长度与表示约束，原文存档，没有三库性能排序。原273项状态不变，历史288为partial，Goal active。[完整报告](performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：维护安全修复与仍有差距的成本

修复普通 VACUUM 的长字段引用收集：在原有页锁和 pin 内，直接读取所属物理元组的 NULL 位图、变长字段目录和有界字节跨度，不再通过可能读取调用者 RID/NULL 状态的执行器取值接口。固定区大小与变长列索引每次维护预先计算，普通内联值无需复制整行或逐值构造字符串。活动快照保护、所有物理存活槽的遍历、锁／pin、索引／分块校验和回收策略保持。

本批1500项完整正常计时没有证明全面提速。在共同的真实after-DELETE种子上，单长字段首次VACUUM服务器CPU比旧版及旧版回测低8.36%／7.73%；多长字段首次VACUUM墙钟高9.83%／27.97%，CPU分别低1.92%／高35.29%。在共同after-UPDATE种子上，仅选ID的CPU高8.43%／8.39%，墙钟高9.77%／14.77%；三个全列查询墙钟也高于两次旧版，完整正负结果保留，原因OPEN。

本批实际PG缺口包括：TEXT70003字符INSERT在产品返回22023而PG成功，立即读取与冷开读取缺该行；SQL `CREATE TABLE ... PARTITION OF` 返回0A000，子表目录身份及冷开身份读取缺行。原生分区物理存储控制不代表这项SQL功能已实现，也不代表逻辑分区扫描已通过。

PostgreSQL所属tuple值／NULL及SQLite溢出页引用作为机制参考，原文存档；没有PG速度对比。新28条SQL中21项值／状态／tag相符、1项实际OID分别核对、6项差异保留，原62条中的9项差异仍在。功能总账273项状态不变，历史287仍partial，Goal active。[完整报告](performance-report-2026-10-30.md#vacuum-physical-references-20261107)。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：普通查询执行开销进一步降低

扫描展开长字段时，借用该物理行已捕获的 NULL 位图；排序后的投影在核对真实来源、具体 Sort 类型和全部物理列声明后，借用排序已保存的位图。借用仅存在于一次取行调用内，退出与异常均恢复原绑定；表结构不匹配、Sort 子类及其他来源继续原路径。没有新增页／allocator 缓存或改变文件身份检查本身。

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

原样 270 操作矩阵的 30 个区域，墙钟 19 低于两旧／2 高于两旧／9 居间，服务器 CPU 为 12／9／9，客户端 CPU 为 9／16／5。单长字段 VACUUM 墙钟高约 22.90%／28.87%，多长字段 VACUUM 高约 19.10%／4.85%；另有 9 个区域服务器 CPU 高于两旧，全部保留，原因仍 OPEN。

PostgreSQL tuple slots、SQLite排序记录取列及DuckDB validity mask作为机制参考；没有三库速度对比或完整PG等价结论。原273项状态不变，新增历史记录仍partial；TOAST既有断言与原9条PG协议差异仍OPEN。[完整对照](performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
<a id="view-column-dependencies-current"></a>
## 2026-11-06：进一步补齐静态视图依赖（P35-C4）

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

28 条真实依赖删除得到 2BP01，29 条普通事务／保存点的值与状态匹配 PG；201 条主矩阵、42 条真实旧库升级、13 条整表／视图删除和 12 条编译目录视图仍完整保留。主矩阵 3、旧库 5、编译目录别名 1 条运行差异仍 OPEN；完整声明／诊断并未归一化成 PG 通过。普通 pg_attribute 89 条调研已保存，SQL 来源尚未补齐。

原 273 项和 22／166／70／15 状态不变，新增历史记录为 partial；没有关闭整个 CAT-02/06/07/10/16 家族。正常 270+27 操作表明本轮没有整体提速，创建视图墙钟更高，负向原因继续跟进。[完整结果](performance-report-2026-10-30.md#view-column-dependencies-20261106)。本批范围是普通 SQL 的静态 DDL 关系／列依赖。视图来源 T7 的零 origin、列别名运行时重绑定、部分嵌套视图、字面量／envelope 运行时、含歧义历史的旧视图、物化视图、函数／类型依赖及大量视图的扩展性仍有缺口；普通 pg_attribute 查询尚未补齐。用户暂缓范围保持，未修改执行图／child-provider、portal 或通用故障矩阵。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
<a id="projected-toast-demand-current"></a>
## 2026-11-06：投影需求读取的有限进展

调研 PostgreSQL tuple slot、SQLite Column 和 DuckDB 投影下推后，直接物理部分列查询采用需求字段解码。仅取 id 的单长字段表墙钟相对旧首测/旧回测为 -54.47% / -47.06%，多长字段表为 -61.28% / -61.63%。全列仍使用原路径；所有写入、维护和普通 SELECT 的完整对照及负向见[报告](performance-report-2026-10-30.md#projected-toast-demand-20261106)，没有 PostgreSQL 速度等价结论。

原严格协议 9 项差异和额外 41 查询中的 24 项差异仍开放；`pg_class` 仍为 34 列声明/7 列可执行值的有限实现。总账状态保持 22 complete/166 partial/70 unverified/15 deferred_by_user。本次只增加部分进展，未关闭 QRY-01、QRY-13 或 STO-06。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
<a id="pg-class-declaration-current"></a>
## 2026-11-06 pg_class declaration: implemented subset and open gaps

The accepted candidate is normal CMake C++17/O2 C2 `280ad4a8`; the first C1 is rejected and retained as evidence. This is a partial catalog feature, with no speed improvement or full PostgreSQL compatibility claim.

Primary design references at the pinned PostgreSQL18.6 source commit: [pg_class.h](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/catalog/pg_class.h), [pg_type.dat](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/catalog/pg_type.dat), and [AclItem layout](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/utils/acl.h). Declared identities/layout are kept separate from this engine's implemented value capabilities.

| Area | Actual evidence | Remaining work |
|---|---|---|
| Owned declaration | One real class1259;34 continuous attributes; rowtype83/array273; actual field-type closure. Persisted34 rows match PG on six declaration fields each. | No physical catalog heap/MVCC; the `pg_attribute` SQL route remains42P01. |
| Type declarations | Five actual type rows match PG on nine identity/layout fields each; `pg_type` SQL returns those rows. `aclitem` is16 bytes/alignment `d`. | Value/result codecs, including NULL, are unsupported0A000; no ACL semantics are added. |
| Ordinary pg_class queries | Self identity is now present; seven values remain executable. All60 other replies in the old62-request pair are byteexact. | Source origins,27 value columns and wildcard are unfinished. New41 strict PG reference passes; product17/41 match,24 differ. |
| Persistence/lifecycle | Real old-program database copy upgrades, then restarts normally;20 replies equal fresh C2 bytes; prior class/type/attribute rows remain exact. | This finite normal-path evidence does not close generic IO/OOM/savepoint fault matrices. |
| Ordinary SQL cost | One complete270-timing cohort,27 DB contexts/54 Main normal0, all raw data retained. | Wall medians13/30 and CPU16/30 are above both old medians; no general acceleration claim, causes remain open. |

The first candidate's12-byte `aclitem` and NULL output labeled TEXT25 are rejected, retained observations. They are corrected by C2's16-byte declaration and explicit unavailable-input error. Raw native qualified quoted table DDL has four unchanged failed checks in the initial fixtures; the corresponding normalMain quoted DDL passes. These API observations are kept separately. Original273 ledger states and user deferrals are unchanged. [Performance and artifact report](performance-report-2026-10-30.md#pg-class-declaration-20261106).
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 普通日志追加与实际查询成本（2026-11-06，P32-C1）

正常CMake C++17/O2、旧版→C1→同旧版各三轮的固定矩阵完整结束：27个数据库上下文、54个正常Main、270个计时区间，校验1098响应、96624行/386388单元/27864个NULL。30案例的服务器CPU为16项低于两旧、6项高于两旧、8项居间；墙钟为13低/5高/12居间。固定/单BYTEA直接仅id查询有有限收益，但多LOB直接全列和仅id的服务器CPU均高于两旧，其中仅id墙钟也回退，全列墙钟居间。全部负向、旧回测漂移及长尾保留，未证明一致SQL提速、稳定吞吐或PG速度等价；原因仍OPEN，P30原负向证据保持。

原58个严格协议目标及4个额外冷锚控制全部保留，两代产品各完成62请求、目标各53/58，原9项普通差异未关闭，strict CLI仍为1；全部62请求/响应原始字节一致。每代实际207行/848单元/156个NULL，4个初始/冷Main均取得真实子进程wait0、PID/组消失。复用此前真实PG18.6参考，没有重跑PG，没有把目录来源元数据或选中错误的TEZ/EZ差异改成PASS或暂缓。

本批优化本地普通日志路径，不是PG性能对照；未执行新的PG参考或关闭catalog/CAT02、IDX-07与其他未完成能力。日志字节回归通过不等于全部SQL或事务恢复等价，用户暂停范围保持。[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与可逆原始证据（可选本地文件 `benchmarks/legacy-log-append-20261106.json.gz`）。以下历史保留。

<!-- CURRENT_ORDINARY_PROJECT_BITMAP_20261106 -->

## 当前行NULL位图复用与按需求捕获（2026-11-06，P30-C2）

普通物理表的直接投影现在按选中列需求，复用当前行已有的完整NULL位图；局部作用域在正常或异常退出时恢复，只选非空物理列时不再复制。既有值提取、虚拟列求值顺序、TOAST处理、文件身份校验函数和暂停范围保持。

正常CMake C++17/O2的最终C2，在固定/单BYTEA/多LOB三类直接完整查询中，墙钟中位数对前后两次旧版降低约9%–32%，服务器CPU降低约11%–35%；这是本机各三样本的有限结果。主键投影负控仍有回退，固定表服务器CPU为+26.19%/+25.84%，单BYTEA主键查询墙钟为+8.03%/+3.65%。跳过多余复制没有消除这些回退，其根因未证，不能据此归因频率、负载或噪声，也不能称全库提速。

原273项仍为22 complete/166 partial/70 unverified/15 deferred_by_user，`IDX-07`保持`unverified`且`evidence=[]`。完整PG参考58/58；三代产品均完成全58目标与4冷控制，目标各53/58，另4冷锚控制仍失败。原9项目录/普通错误帧差异完整保留、没有改为PASS或新增暂缓。仅新增1个正式CPP回归；whole58 known-gap协议草稿未运行、未登记。Goal active，默认程序未替换，无服务重启、分支切换或推送。

完整原始分布、既有差异、许可和验收边界见[本批性能报告](performance-report-2026-10-30.md#ordinary-project-bitmap-20261106)与可逆压缩证据（可选本地文件 `benchmarks/ordinary-project-bitmap-20261106.json.gz`）。以下P29及更早内容为对应代别的历史记录。

<!-- CURRENT_MULTILOB_20261106 -->

## 多个大字段存储与真实命令结果（2026-11-06，P29-C2）

原七列建表同时含两个 BYTEA 和两个 TEXT 时，类型逻辑上限之和会超过堆头的 uint32 字段。现在仅对 format 2 中这种确实含变长列且最小实际元组可装页的溢出定义，将堆头名义宽度记为 0；可表示的旧宽度、列上限、实际变长元组和 TOAST 格式保持。UNLOGGED 初始化分叉在原有维护入口校验自身宽度，不匹配时重建。执行结果另显式标记无 RETURNING 的计数命令，普通 WITH DML 也据实际执行结果发送命令帧。

原19项定义/排序规则诊断在C1及C2均完整通过：每代父进程2154项检查，两个工作进程合计604项（452+152）。原失败发生在旧七列建表前置阶段，不能把它写成19项全已执行。另C1/旧P28C4双代12工作进程共2662项检查、6组真实冷重启RID对照全过；C2正式物理生命周期用例为10工作进程2483项、父进程17809项，覆盖真实大值/NULL/RID、备份恢复、分区、UNLOGGED及维护路径。以上是不同固定夹具和进程角色，不能相加冒充全仓测试数。

固定SimpleQuery文本格式0的37+3条、七列全值/NULL/十六进制BYTEA/列描述七项/命令与Ready检查，PG18.6首次40/40，C1为38/40，C2为40/40；C2自包含正式协议用例另一次同40控制也通过。C1保留的两项是DELETE多发空RowDescription帧，行值/NULL/状态/命令原本正确。C2按真实DML命令角色修正该帧形状，并保留SELECT/RETURNING结果形状；没有靠搜索字符串中的RETURNING判断。PG与本地分别读取真实关系OID进行来源锚定，不要求两个数据库的原始OID或整段原始协议字节相同。

原constraint154协议回归在C2完整通过。独立结果形状mini21参考PG21/21，C1为13/21，C2为19/21，仍保留两个普通缺口：零列一行SELECT缺少一个零列DataRow；零列表RETURNING *的错误代码实际42P01而PG为42601。它们未被改为PASS。

两个产品专属普通WAL角色共实际12条动作：COMMIT确认后强杀与未提交UPDATE确认后强杀，各自退出为预期-9，随后正常Main首先打开同一目录完成恢复并正常退出0；没有由原生读取器抢先恢复，也没有PG崩溃参考。后续在4份正常关闭后的独立副本上运行真实公共HeapScanCursor，34行/238单元/44个NULL和全部真实RID身份检查通过，四观察者均正常0且PID/进程组消失。初始到冷开共同键1..7不变，未提交角色全部8个旧键恢复，提交角色7个未修改旧键保持，实际修改键3及新键使用真实唯一RID。原WAL回执不回写；独立sidecar补充可见性证据，绝不把LP_NORMAL或虚构SQL ctid当可见行。

正常 CMake C++17/O2 固定成本对照已完整执行：旧版—C2—同一旧版各三轮，固定表及单 BYTEA 表各256行，另有C2专属多LOB表128行。168句实际计时、546个响应、24个正常Main及42个只读副本观察者均完成；全值/NULL/来源/冷开与真实RID验证通过。结果不支持本批普遍提速：16个可比操作中，墙钟与服务端CPU各为1项低于两旧、12项高于两旧、3项介于两旧；固定表查询在原双表流程中服务端CPU中位数15.667→30.044→18.536ms，C2增加91.76%/62.08%。全部变慢项、原三样本、长尾及漂移保留。DELETE旧版还多一个空T帧，不能当完全相同协议工作量；新多LOB因旧版建表失败，仅描述新版成本，不计算提速比。

后续固定CPU8的单表、三代正常构建窄诊断另有12次目标观测与24个正常Main；C2查询墙钟中位数22.086ms，对两旧24.181/26.336ms，服务端CPU14.326ms，对两旧15.406/16.605ms；C2相对C1的CPU中位数仍增加4.40%。这个更窄条件未重现原流程的大幅差异，但不能冲销原负向结果、归因于CPU亲和性或证明稳定收益，回退原因仍未确认。四角色用户态CPU采样的512次完整查询均通过，显示共同热点在逐列NULL检查及文件身份核验；采样计时没有混入正常性能表。

最终验证对象是候选C2（SHA `b53f853b356e9c8cf1e8fedf9832201d9bf705cf81d817b35314b4660e334594`），默认安装程序未替换，未推送或切换分支。原53.45MiB性能JSON当前保留为5.70MiB gzip，未压缩基准文件已被Git忽略；本批完整证据同样采用可逐字节还原的gzip归档。只对核验可恢复的生成副本安排清理，Git历史不重写。性能目标继续，不能把功能修复称为全库优化完成。

这是有限的 partial 进展，Goal active。原 273 项状态保持 22 complete/166 partial/70 unverified/15 deferred_by_user；`IDX-07` 仍为 `unverified`、`evidence=[]`。零列行集、零列表和原 TOAST 损坏夹具的普通缺口仍保留，用户暂缓边界保持。本批不证明完整 PostgreSQL 数据类型上限、文件格式、WAL 或协议等价。

完整定义、首轮失败与测试边界见[本批报告](performance-report-2026-10-30.md#multilob-storage-20261106)和可逆压缩证据（可选本地文件 `benchmarks/multilob-storage-20261106.json.gz`）。下方 P28 及更早段落是对应代别的历史记录。

<!-- CURRENT_CONSTRAINT_VALIDATION_20261106 -->

## CHECK/FK NOT VALID 与真实历史验证（2026-11-06，P28-C4）

`ADD CONSTRAINT ... NOT VALID` 仍在真实元数据锁下绑定 CHECK/FK 定义，只跳历史行验证；后续写入仍由原真实约束执行链检查。`VALIDATE CONSTRAINT` 对未验证的 CHECK/FK 扫描真实可见历史行，仅在成功后发布 validated 标记；失败保留定义和未验证状态，已验证约束不重复求值。真实 SQL NULL 与非 NULL 空字符串分别处理。CHECK 的 volatile 函数不因 volatility 标记被一律禁止；定义绑定不执行用户函数，实际验证仍按到达的行求值。

正常候选 C4 为 CMake C++17/O2，程序 SHA256 `5cd5fd9e15f72257d13f2622e180d643e5cc93fffe8a128cbf8e820f64caf539`；before 为 P27-C2 `dd5d9b85…`。原生父测试 4,896 checks/0 failures（含文件快照断言），其中真实 lifecycle worker 779/cold worker 158；两 worker 正常退出，捕获两份当时文件映像，不能将这些层级的计数相加。真实目标 PageAllocator fetch 38 个观测中 18 个为零、20 个为非零 fetch，另有真实读取正对照证明 hook 确实生效；这只说明历史验证访问路径，不代表整句无 I/O。原协议 142 主流程+12 冷控制共154条全部匹配 SQLSTATE、完整值/NULL、真实本地 relation OID 的 T7、tag 和 Ready；58份参数快照、5项保留标志检查通过。修改前同源用例实际83条（56匹配/27差异）及71条 NOTRUN；C3全部154条中有4处不匹配，原失败保留，C4修复后原断言未放宽。独立 append10 全10条通过：已经 validated 的带 nextval CHECK 不再求值，PK/UNIQUE VALIDATE为42809、缺失名42704。新增真实 PostgreSQL18.6 mini20 全20条符合首预测；产品对应19条+6条冷控制全25条、13项真实 `.params` 检查通过，ASCII collation 只比较大小写语义角色，PG使用真实 ICU、本地使用 nocase，不声称 provider/OID/Unicode 等价。mini19 v1接收器在启动前缺少 Path import：0SQL/0owner，原首错误保留后仅修接收器，不称产品重试或产品失败。原159参考的3个预期行快照错误及 reserved RESET 首预测错误都保留，后续 oracle 仅从已记录 PG 离线更正。完整原始帧不是逐字节 PG 等价：mini19主流程12组 raw 相同、7组不同（错误文本字段或真实本地 OID），不抹掉差异。

普通 ADD CHECK/FK 与同样有效数据的旧实现可比较所需验证成本。NOT VALID 可描述去掉不需要的历史扫描，但旧版多做扫描，不能称同扫描工作量提速；旧 VALIDATE 仅改标记，其耗时不是正确验证的性能基线。整个普通 ALTER TABLE 仍有 DDL 物理回滚快照、文件复制/摘要/同步成本，目标 PA fetch=0 不表示整句 O(1)、无堆文件读取或无 I/O。

固定0/128/1024行×三轮before-first/candidate/before-return，27个独立主流程+27次同数据冷重启均正常0；216个ADD/VALIDATE计时点、3024份响应、2384640个完整单元格（157950个NULL）、18360份物理文件映像（其中2700份堆映像）全部独立核对。1,024行时，CHECK NOT VALID wall中位数对前/后旧版分别−63.939%/−61.677%、server CPU −71.962%/−72.695%；FK NOT VALID wall −40.047%/−39.400%、CPU −15.482%/−13.469%，这两项是消除旧版多做的历史扫描。普通ADD CHECK wall −18.399%/−14.808%，CPU +0.124%/−4.360%；普通ADD FK wall −38.575%/−35.423%，CPU −4.131%/−0.022%。补上真实历史检查的CHECK VALIDATE wall +52.762%/+43.290%、CPU +260.317%/+251.282%，属于新增正确性工作成本，不能与旧假验证比较成等价速度。

负结果也保留：空表普通ADD CHECK CPU +29.920%/+35.993%；1,024行已验证FK重复VALIDATE wall对首/返旧版为−36.791%/+51.673%。全样本最长单点17.977415s，旧版wall中位数往返漂移−58.325%～+60.792%，server CPU漂移−28.186%～+4.689%。完整72行metric统计含各角色raw3/min/p50/max、双基准差值与旧版漂移，没有剔除负值/长尾，没有稳定倍数、因果结论或PG速度等价声明。计时是普通socket请求至Ready，包含DDL物理快照成本；process CPU与proc快照非原子、含所有服务线程，不是独占算子成本。

本批四个生产文件、两份新增回归、原登记入口和既有文档在 master 做本地版本管理，使用 `Palpitate-xus <palpitate.xus@outlook.com>`。正常验收程序独立冻结，根目录运行程序仍为 `425165af…`。原始证据只以完整可逆 gzip 入库；53.45MB 的旧 analyze/reindex JSON 已由约5.7MiB gzip替换，旧提交历史未重写。恢复步骤和逐流SHA保存在归档内，构建目录中的原始文件不再直接进入Git。

本批为有限普通约束生命周期的 partial 进展，Goal active。原 273 项状态保持（22 complete/166 partial/70 unverified/15 deferred_by_user）；`IDX-07` 仍是 `unverified`、`evidence=[]`。本批 GiST 页树资料只是未实现研究，不能将 P27 的 batch partial 记录当作 IDX-07 已部分验证。原用户暂缓边界保持。

完整代别、首轮失败、普通成本全表与范围见[本批报告](performance-report-2026-10-30.md#constraint-validation-20261106)和压缩原始证据（可选本地文件 `benchmarks/constraint-validation-20261106.json.gz`）。下方 P27 及更早段落保持历史口径。

<!-- CURRENT_GIST_PROBE_READER_20261106 -->

## GiST 读取开销与空文件回退（2026-11-06，P27-C2）

本次优化完整验证后才发布候选，零字节旧文本文件不再被误认为可信空集。合法V2/V3和非空旧文本继续兼容，V2空键覆盖限制不变。与先前实际PG18.6记录作75项离线对照，全部值及七字段描述符相同，原始帧73项相同；两处DELETE额外T差异仍使严格全协议通过为false。两端关系OID仅按各自真实目录锚定，未重跑PG或比较PG速度。

读取设计参考固定版本的[PG18.6 gistScanPage](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/gist/gistget.c)、[SQLite3.50.4 payload读取](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/btree.c)和[DuckDB1.4.3 ART查找](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/index/art/art.cpp)。PG有页锁、split NSN和一致性重检；SQLite借用本地payload并为溢出提供拥有缓冲；ART有真正树查找。本地小文件缓冲仍完整遍历flat sidecar，没有这些引擎的树、WAL和并发等价。

机制计数减少C++read调用和new请求，同时增加累计请求字节；正常SQL是否改善只能看[本轮完整对照](performance-report-2026-10-30.md#gist-probe-reader-20261106)，不能用instrumented单次时钟替代。既有普通缺口、完整GiST目标和用户暂缓边界均保持。

<!-- CURRENT_GIST_EMPTY_BOUNDS_20261106 -->

## GiST 空字符串、边界与 LIKE 检索（2026-11-06，P26-C4）

当前物理heap格式的存储列构建/REINDEX会写GiST V3，覆盖真实非NULL空字符串；NULL仍由真实位图区分。V2编码本来允许零长度，缺陷在旧producer省略空键，V3标记新的覆盖保证。旧V2/文本文件继续严格读取；无法证明覆盖、CRC、文件归属或可见性时交回原堆来源。有限空端点、缺失端点、严格/非严格边界和字面DEL分别表示；最终完整谓词、MVCC、NULL及TOAST仍检查。

LIKE必要字节前缀使用单独的tryGiSTSearchLikePrefix，保留原pattern/escape角色；默认locale下不把字节前缀的权限推广到SQL标量比较。无有效Unicode上界时保留无上界，避免DEL哨兵截断。普通EXPLAIN的>=空串不再独立触发全宽索引遍历；JSON usedIndex仅在真实索引候选读取成功时为true。

实现依据分别保留固定版本的[PostgreSQL btree_gist源码](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/contrib/btree_gist/btree_utils_var.c)、[PG text consistent](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/contrib/btree_gist/btree_text.c)、[SQLite记录格式](https://www.sqlite.org/fileformat2.html#record_format)和[DuckDB ART键实现](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/index/art/art_key.cpp)。空数据、NULL、端点存在性分别处理；PG btree_gist的text叶项可精确判断，不将PG全部GiST称为有损。23份官方源码/许可证与研究行锚随归档保存；研究不是运行验收。

真实PG18.6安装并加载btree_gist后重放75条共享SQL；最终C4逐项比较3231行/19382单元（661个NULL）、248列描述符的1736个元数据字段。75/75语义及描述符一致，73/75完整帧一致；两次DELETE本项目额外发送零字段T，PG只发CZ，严格全协议通过仍为false。两端关系OID各由真实目录锚定；PG正常退出、PID已回收，未比较PG访问方法格式、路径或速度。

固定九进程、七类相同SQL的wall中位数有4项低于两次旧版、3项高于两次旧版；服务器CPU为5低/2高。非空等值wall减少84.385%/83.171%，密集非NULL查询减少80.038%/77.317%；高端范围wall增加26.674%/50.555%，不存在范围增加62.435%/60.745%，低端含空范围增加0.617%/3.536%。REINDEX wall增加6.065%/6.366%，其V3多写298个真实非NULL空键，不能当等字节重建速度比较。完整首轮/同旧回测和全部三时钟如下，主键与无索引控制变化不作因果归因，未证明统一提速或PG性能等价。

本地GiST仍是flat sidecar，每次候选检索完整校验文件，并非PG的分页GiST树、opclass/KNN/WAL/concurrent全功能。虚拟键仍保留旧覆盖来源；native-only丢失定义、虚拟Bloom NEW NULL、既有missing-memory协议失败及其不完整旧版配对仍开放。下划线文件名歧义现在安全回堆，物理文件命名/关系身份布局仍需后续改造。原用户暂缓范围保持，Goal active；本批为partial。

当前清单为2383个测试文件：原2378中2374份逐字保持，4份只迁移当前GiST格式断言并加强V2兼容/空串真实RID检查，另增4个CPP及1个协议测试。顶层CPP1119（含链接桩）、全部CPP1124/其中子目录5；登记681个driver/661个独立E2E。源248/头177，754份Markdown、725份保护记录保持；25份旧gzip逐字保持，新增第26份。原273项及22complete/166partial/70unverified/15用户暂缓状态、原276条历史逐字保持，仅prepend第277条partial。

详细表格见[本轮报告](performance-report-2026-10-30.md#gist-empty-bounds-20261106)。以下 P25 及更早段落均保留当时的实现、计数和未完事项；本轮状态以本节为准。


<!-- CURRENT_REINDEX_ACCESS_METHODS_20261106 -->

## 普通访问方法 REINDEX 与空文本生命周期（2026-11-06）

本批补入普通 `REINDEX TABLE` 的 B-tree/Hash/Bloom/FTI/GIN/GiST/SPGiST/BRIN 重建、物理 Bloom NULL/空文本维护与 UPPER/LOWER 表达式索引 SQL 桥接。有效旧头的 BRIN `pages_per_range` 与 Bloom `k` 保留；`m` 随真实新键集按既有 loader 默认128 bitsPerEntry重算。旧头损坏/缺失时回退宽64/k7。默认程序仍为 `425165…`，没有服务重启或默认升级。

当前统计：2378 测试文件（原2375字节不变加2 CPP及1协议），顶层CPP1115（含链接桩）、总CPP1120/其中子目录5；登记680个driver/660个独立E2E。源248/头177，754 Markdown、725保护记录；24份旧gzip逐字保持，归档总计25份。原273项22/166/70/15及275条历史保持，仅新增partial第276条。

GiST 空文本编码、native-only 丢失定义、虚拟 Bloom NEW NULL 和上述失败均保留普通未完；不关闭原273项。

完整代别、首轮失败、工作量差异与全部成本表见[本批报告](performance-report-2026-10-30.md#普通访问方法-reindex-与空文本生命周期2026-11-06)。以下P24及更早段落保留当时历史，不把旧数字当作当前统计。


### 实现机制与源码依据

[PostgreSQL 18.6 index.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/catalog/index.c) 的普通 TABLE 重建遍历该关系全部索引，使用原目录/关系描述符定义、建立新物理代，并由事务与 WAL 管理。`BuildIndexInfo` 提取键数/列号、表达式和谓词等；AM routine、opclass/collation/reloptions 仍依附实际 relation，不能称都由该函数复制。[SQLite 3.50.4 build.c](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/build.c) 复用 existing Index 定义并经 sorter 重填；其[ pager.c](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/pager.c) 承担 pager/journal 的提交与回滚。[DuckDB 1.4.3 CREATE ART](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/operator/schema/physical_create_art_index.cpp) 是构建机制参考，不作为 PG REINDEX 兼容 oracle。

本地采用真实权威定义、先私有构建后统一发布，并将可共享的一次扫描限制到最多8个 builder；mixed V2 标记提供本项目文件交换/恢复协议，不声称完整 PG transactional WAL 或并发等价。23份固定版本官方源码和许可证均归档；源码研究与最终实际执行的24条 PG18.6 共享对照分别保留证据。

下方 P24 及更早段落为历史记录。


<!-- CURRENT_BRIN_OPERATOR_CONSISTENCY_20261106 -->

最新P24-C2将普通BRIN摘要比较改为运算符专用路径；仍校验min<=max、所有文件/CRC/identity/dirty/snapshot fences与完整heap Filter复核，NULL flags不新增求值，legacy timestamp原路径保留。实际功能门禁及全部首次结果、固定9进程的完整维护/查询wall、CPU、长尾、负数与漂移见[性能报告](performance-report-2026-10-30.md)和压缩证据（可选本地文件 `benchmarks/brin-operator-pruning-20261106.json.gz`）；不称完整PG BRIN、所有SQL提速或因果已隔离。性能外层runner因label覆盖退出1、九backend正常0，二者分开记录；原source/plan/complete=false回执保留，后续仅离线finalize，不重采样、不当产品故障。默认425保持，partial、Goal active。旧2374测试逐字不变+1 native=2375；679/659 roster、源248/头177、顶层CPP1113/全部1118、754MD/725保护记录保持，历史275，旧23gzip保持+新24。下方P23及更早段落为历史记录。


<!-- CURRENT_BRIN_MAINTENANCE_OPTIONS_20261106 -->

最新P23-C1修复BRIN在VACUUM FULL、加列、删列和TRUNCATE/reset重建时丢失pages_per_range：健康V3配置在重建前保存，宽度1和SQL默认128均保持；缺失/旧格式/损坏仍沿用64回退。生产只改TableManage.cpp四处捕获/四次传参。新协议42命令/12镜像/4路径检查、25相关原生、6专项协议、3自动统计均正常0；PG18.6共享35SQL/19,327字段、11真实metapage一致；恢复7worker/2477检查。

固定9个正常socket进程测量完整VACUUM FULL及其后7类查询，所有wall/CPU、尾部、控制项和负数见[性能报告](performance-report-2026-10-30.md)及压缩证据（可选本地文件 `benchmarks/brin-maintenance-options-20261106.json.gz`）。该修复保留用户配置，不代表所有操作均提速；REINDEX仅Btree、完整PG BRIN AM/增量维护/WAL、规划重复验证开销、Bloom/GiST非NULL空文本覆盖和P22数组ROLLBACK间歇超时仍开放。前批密集CPU回测+6.64%负面记录保持。本批partial、Goal active。源248/头177，2373旧测试字节不变+1=2374，679注册/659独立E2E；原273item状态不变、历史274；754MD/725保护记录/22旧gzip保持。下方P22及更早段落为历史记录。


<!-- CURRENT_BRIN_TYPED_SCAN_20261105 -->

最新 P22-C10 修复 BRIN 数值边界/NULL 状态并接入真实候选页扫描，支持普通 SQL/绑定后五类比较和 NULL 谓词、反向常量及 pages_per_range。23页 extent 的原生候选 cursor 实际只 fetch1页；完整谓词仍复核。格式组件 O2/ASan各5588、新原生1834、新协议101命令/25路径断言、真实PG18.6共享73SQL/10490字段及7恢复worker/2477检查通过。有限81原生80正常0+1父代TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

固定9个正常socket进程、4096×4、七类查询，服务器CPU减少（首轮／同旧回测）：高端窄范围 41.19%／42.36%；低端窄范围 48.68%／49.29%；非主键等值 25.78%／20.13%；不存在的范围 94.22%／94.12%；密集范围控制 -7.44%／-6.64%；主键控制 30.31%／28.42%；无对应索引列控制 7.79%／4.38%。所有wall/CPU/控制负数、原始帧和尾部见[性能报告](performance-report-2026-10-30.md)与压缩证据（可选本地文件 `benchmarks/brin-typed-scan-20261105.json.gz`）。新BRIN文件V3，旧1/2只作兼容读取/安全回堆，旧二进制不能读取V3；当前物理heap2保持，暂缓format2仍指wire/output，NetworkServer.cpp未改。REINDEX尚仅Btree、部分重写配置保存、PG opclass/revmap/增量维护/WAL/concurrent、SSI及特殊类型/来源路径等仍开放，不声称全部数据库操作或PG性能等价。本批partial，Goal active。源248/头177、测试2373，678注册/658独立E2E；原273对象状态不变、历史273，754MD/725保护记录和21旧gzip保持。下方P21及更早段落为历史记录。


<!-- CURRENT_INDEX_BUILD_NULL_OWNER_20261105 -->

最新 P21-C1 让索引构建/REINDEX 复用当前行物理 NULL 位图，并修复 BRIN 将真实 SQL 全 NULL 整数摘要写成 `[0,0]` 的问题；空字符串范围保留。新原生13366检查、新协议40命令、真实PG18.6共享35命令/2250字段及7恢复worker/2477检查通过。有限相关79原生78正常0+1父代相同TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

正常2048行维护9进程、368640字段核对；相对同旧回测CPU中位数减少：Btree 19.30%，复合 19.02%，写事务 45.10%，回滚 47.67%；BRIN -154.76%（退步）。wall波动大且多项变慢，另9进程完整socket查询控制也没有一致提升，详见[性能报告](performance-report-2026-10-30.md)与压缩证据（可选本地文件 `benchmarks/index-build-scan-owner-20261105.json.gz`）。BRIN数值排序及SQL执行算子等仍为普通开放缺口，本批partial，Goal active。当前物理heap版本2保持；暂缓format2指协议/输出所有权。源248/头177、测试2370，677注册/657独立E2E；原273状态不变、历史272，754MD/725保护记录与20旧gzip保持。下方P20及更早段落为历史记录。


<!-- CURRENT_INDEX_CRC32C_20261105 -->

最新P20-C2为共享索引CRC32C加入通用查表和受CPU能力保护的SSE4.2路径，保留完整校验、所有文件字节与C++17基本构建要求。组件44392检查及ASan/UBSan、无SSE4.2模拟35520检查通过；12真实存储worker/3840字段与六类索引旧新双向读写、重建文件逐字节一致。最终68原生67正常0及1项父代相同TOAST中止、旧32协议29正常0及3项父代相同失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 18.62%／-0.48%；热点等值 3.68%／0.90%；低频前缀 11.82%／14.98%；低频等值 -12.80%／-22.17%；不存在等值 6.78%／0.48%；主键控制 -1.07%／-6.83%；控制差异不作因果归因。源248/头177、测试2368，676注册/656独立E2E；原273状态不变、历史271，754MD/725保护记录与19旧gzip保持。Goal active；下方P19及更早段落为历史记录。详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_GIST_READ_OWNER_20261105 -->

最新P19-C3修正普通当前读事务被GiST误判为重叠写者的问题，并复用可见元组NULL位图。专项1214检查、新39协议、PG18.6共享37命令/1287字段对照通过；候选检索阶段目标堆页fetch由旧版2021降至0，返回行仍正常回表校验。历史/重叠/自身写入/缺失损坏/空文本范围及SERIALIZABLE保留堆后备。旧60原生59正常0和1项父代相同TOAST中止、旧32协议29正常0和3项父代相同严格失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 2.54%／3.42%；热点等值 -0.42%／-1.44%；低频前缀 56.62%／52.19%；低频等值 -12.42%／-10.29%；不存在等值 2.15%／1.44%；主键控制 -6.09%／-3.54%，负数与控制波动也报告。旧GiSTScan/idx_scan只代表算子，不能单独证明底层物理索引访问；下方旧章节按此限制理解。源248/头177、测试2367，676注册/656独立E2E；原273状态不变，历史270，754MD/725保护记录保持，18旧gzip保留并新增1。仍partial，Goal active；详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_TYPED_PLANNER_STATISTICS_20261105 -->

最新批次 P18-C6 将普通等值与二进制文本 LIKE 扫描成本接入拥有真实身份的 typed ANALYZE 统计，并修正事务内显式分析的两份统计文件回滚。新65条协议/15次扫描、原LIKE123/27、最终原生专项及cold exec、组件O2/ASan/UBSan、7个恢复worker和真实PG18.6共享33命令/6732字段对照通过；相关旧协议29通过、3项父代完全相同严格失败，首次自动数组/range回滚超时及后续原deadline固定旧/新/旧六次通过均保留。完整socket性能中位数减少（首轮／回测）：热点前缀 73.72%／73.25%；热点等值 51.25%／15.72%；低频前缀 58.53%／-8.58%；低频等值 44.30%／38.10%；不存在等值 24.66%／24.31%；主键控制 30.77%／29.00%，负数也保留，只代表该负载。原273与22/166/70/15状态保持，历史269，测试2365，源248/头177，675注册/655独立E2E，754MD/725保护记录、17旧压缩包保持。仍是partial，Goal active；事务内ANALYZE需要临时数据库物理备份，旧统计格式、未转换消费者和用户暂缓模块未整体完成。详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据。


<!-- CURRENT_LITERAL_LIKE_SCAN_20261105 -->

## LIKE前缀的真实索引读取与密集前缀成本（2026-11-05，P17-C6）

普通无参数SELECT中的TEXT/VARCHAR字面量LIKE现在可实际使用已有GiST侧索引。已绑定AST提供真实关系/列身份；前缀范围只筛选候选，原类型化LIKE、AND剩余条件和NULL语义继续复查。转义百分号/下划线、多字节ESCAPE、UTF8上界以及打开前删除索引均有回归。可用ANALYZE的MCV与完整覆盖直方图桶识别密集前缀，按真实表extent和成本参数保留堆扫描；未知探测至少计1行成本。

同一4096行×3列、父代／最终C6／同一父代回测各3进程交错运行；wall中位数变化（正数为减少，相对首轮／回测）：热点前缀（3200行）6.88%／-3.83%；低频前缀（1行）68.01%／67.77%；不存在前缀（0行）69.40%／68.72%；显式ESCAPE前缀（1行）71.84%／71.06%。仅代表本负载；C3热点超过3倍耗时的失败尝试完整保留。

最终C6通过新原生310检查、原13068与独立冷读105检查、37个相关原生入口；纯前缀组件O2和ASan/UBSan各72567检查，启用泄漏检测。新123条协议命令含27次实际扫描断言；32个相关旧协议入口29正常0、3项严格失败与父代完整观察相同。真实PG18.6以两个会话重放共享65条SQL，935个完整字段/NULL/OID逐项相同，3个EXPLAIN ANALYZE实际行数相同；未比较PG的AM路径、成本或速度。

详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据（可选本地文件 `benchmarks/like-prefix-scan-20261105.json.gz`）。本批仍为partial，Goal active。


<!-- CURRENT_ORDINARY_QUERY_PROFILE_20261105 -->

## 普通查询的规划元数据与解析开销（2026-11-05，P16-C2）

本批在一次普通查询规划内复用关系schema，并按需各读取一次B-tree/Hash/Bloom/GiST列清单；主键与不可能使用位图/GiST的条件省去无用发现。普通表达式先排除不可能的类型常量前缀，避免对数字、表达式分隔符前的列名做推测类型解析及异常处理。真正的限定/引号/修饰/复合类型常量与显式CAST仍使用原声明语法。

交错的8倍调用补测，父代／最终C2／同一父代回测各3进程；wall中位数变化（正数为减少，相对首轮／回测）为：热点等值（3200行）12.15%／-1.12%；低频等值（1行）15.17%／18.32%；不存在等值（0行）17.94%／14.60%；主键等值（1行）25.52%／19.68%。全部短时及补测的正负结果保留，不能推广为全部操作或PG速度。

最终C2通过原生13068与独立冷读105检查、32个规划/存储入口和14个语法入口；另2个冷目录原生入口保留父代完全相同的严格失败。新65条协议命令含10个实际扫描断言，原有73条等值协议通过；另30个相关旧协议入口27通过、3个严格失败与父代完整观察相同。实际PG18.6的共享25条SQL仍为7331/7331逻辑单元格、9/9行估计一致，8/9路径相同；不是完整兼容验收。

详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据（可选本地文件 `benchmarks/ordinary-query-profile-20261105.json.gz`）。本批是普通读取partial进展，Goal active。


<!-- CURRENT_EQUALITY_SCAN_COST_20261105 -->

## 普通等值查询的实际扫描选择与完整网络成本（2026-11-05，P15-C7）

本批让普通原生标量堆表查询实际消费基于ANALYZE频率的扫描选择：热点等值可选顺序扫描，低频等值和主键保留索引路径。此前只修改规划器时，无ORDER BY的普通查询仍绕开物理计划；现在结构化输出已接通，并修复无可用索引OR的显示文本合并，以保留重复行、NULL、空串和换行。B-tree、位图AND/OR与GiST在探测前取得语句快照，并在快照不适合当前索引或探测期间提交边界改变时读取完整可见堆候选，再做原条件检查。

这是普通读取的partial进展，使用现有原生 `.stats` 中的MCV/计数；尚未让优化器全面消费新的typed `.analyzed_statistics` 槽。普通EXPLAIN的文本缓存键现包含精确的实时成本设置；没有修改已暂缓的retained/custom/generic规划、参数、portal/format2或mutation/provider生命周期。当前私有普通后端P15-C7 SHA256为 `3ff5f16a9cc3a223d6618db87d8a5bbe85240dfd4c08cfdceafeb6ba49c03d3a`。默认根程序425与既有服务保持；Goal active，PostgreSQL整体功能仍partial。
同一4096行×3列、同一真实SQL与完整网络返回，父代／最终C7／同一父代回测各3进程，固定9个完成测量的进程，不重试计时查询；另两次在基线装载/启动阶段失败、没有计时样本，完整保留。wall中位数相对首轮父代／回测的变化（正值为减少、负值为增加）：热点等值（3200行）12.51%／2.45%；低频等值（1行）-5.57%／-5.47%；不存在等值（0行）77.31%／77.36%；主键等值（1行）-11.11%／9.44%。正负变化均如实保留；仅代表这四个普通读取负载，不能推广为全部操作、通用吞吐或PG速度。

新原生13068检查与独立冷读105检查通过，纯频率组件O2及ASan/UBSan各17检查（启用泄漏检测）。24个相关原生入口实际正常0，含完整7worker恢复1974检查和14份子进程完整输出；这些入口实际链接最终C7的61个核心对象。新73条普通协议命令正常0，验证实际扫描计数、全部行值/NULL及旧快照；26个旧协议入口中23正常0，另3保留与父代一致的严格失败，未称全套通过。真实PG18.6对共享25条命令的7331个逻辑单元格全部逐字相同，9个EXPLAIN行数相同，8个扫描选择相同、1处成本边界扫描选择不同。

原始测量、失败与独立复核见压缩机器归档（可选本地文件 `benchmarks/equality-scan-cost-20261105.json.gz`）和[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_PG_STATS_SOURCE_20261105 -->

## pg_stats 完整基础查询与读取成本（2026-11-05，P14-C6）

本批接通实际 `pg_catalog.pg_stats` 的17列基础SQL查询、参数为零的纯Describe、文本/二进制/混合结果格式，以及实际 owning anyarray 输出；支持已测的过滤、投影、排序、LIMIT、别名、CTE和JOIN。每次查询只读取当前数据库的真实统计与目录，重用本次查询内的关系/属性索引和每关系一次的物理模式校验，减少逐列重复扫描、模式复制和文件散列。

本节更新下方P13当时“17列SQL尚未接通”的结论；下方历史批次保留当时事实，当前状态以本节为准。当前私有普通后端P14-C6为 `9362b02aba17d45038702211ba42a6a1d797ef3580d97edb8ffabce80e0f63b6`。默认根程序425和既有服务未切换，Goal保持active；整体PostgreSQL功能仍为partial。
同功能未索引C2／最终C6／同一C2回测各3进程，每进程先1次不计时完整查询，再计时3次完整17列SQL查询API。16张表×30列×32行，实际512行/15360存储单元格/1696个NULL；每次查询返回480行×17列。wall中位数384.334／245.178／379.841ms，降低36.21%／35.45%；process CPU中位数392.205／254.151／387.508ms，降低35.20%／34.41%。这是该负载的完整原生SQL API对照，不是任意查询、网络端到端、已发布父代或PostgreSQL速度对照。所有9个样本及长尾保留。

新原生109检查与独立冷读90检查、投影组件O2和ASan/UBSan各548检查通过；34个相关原生入口全部正常0，包含完整7worker恢复1974检查及14份完整子进程输出。普通后端新协议26条SQL及旧标量25/数组19/范围19条自动统计协议通过。真实PG18.6同26条协议命令991个字段值：938逐字相同、53处严格差异保留；17列二进制153项中145相同、8处为实际存储宽度差异，六种范围的24个直接二进制值全部相同。另510个有限float4输入的文本和二进制身份均与PG一致。没有全库SAN或整体PG等价结论。

原始测量、失败及逐字复核见压缩机器归档（可选本地文件 `benchmarks/pg-stats-source-20261105.json.gz`）与[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_RANGE_COLLECTOR_20261105 -->

## 范围统计采集与完整 ANALYZE 成本（2026-11-05，P13-C2）

本批新增六种内置范围类型int4range/int8range/numrange/daterange/tsrange/tstzrange及实际范围域的ANALYZE采集：独立排序下界和上界、保存范围边界与长度直方图、空范围比例、NULL比例和实际存储宽度。边界保留包含关系、无界端点和子类型语义；本次样本局部缓存解析键，避免排序时反复解析。

最终私有普通后端P13-C2为 `3593b23e5e142605fe71149daec3347015003fb7a84d8a2505c80578e4e77a5d`，默认根程序425保持。完整pg_stats目录/17列SQL及output-only anyarray尚未接通，两条实际探针仍严格失败0A000。多范围尚缺真实目录/存储拥有者；本批仍为采集阶段的partial进展，Goal active。
本批同功能未缓存C1／缓存C2／同一C1回测，各3进程；每进程2048行、1个整数和六种范围，先1次不计时预检，再计时3次完整ANALYZE API。process CPU中位数388.224／201.363／391.997ms，降低48.13%／48.63%；wall中位数441.019／250.031／456.067ms，降低43.31%／45.18%。所有9次原始测量均保留；这不是任意SELECT、父代已发布后端或PG速度对照。

原生范围845检查及冷读262通过；组件O2和ASan/UBSan各43检查（启用泄漏检测，非全库SAN）。21个相关原生入口含完整7worker恢复1574检查全部正常0。原数组786+冷读191、标量641+冷读2、范围19条/数组19条/标量25条协议SQL通过。真实PG18.6同一520条SQL对照18列243项：229相同，14处严格差异保留（13处存储宽度、1处numeric NaN长度排序）。没有PG整体通过结论。

原始测量、失败和复核见压缩机器归档（可选本地文件 `benchmarks/analyzed-range-collector-20261105.json.gz`）及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_ARRAY_COLLECTOR_20261105 -->

## 数组统计采集与完整 ANALYZE 成本（2026-11-05，P12-C3）

本批把真实数组存储输入接到同一次ANALYZE采样：保存整数组MCV/直方图/相关性，以及常见元素、按非NULL数组计算的成员频率和不同元素数直方图。实际验证integer/text/numeric/boolean/float8数组的NULL、空数组、字面NULL、重复值、多维/下界和冷读；其他元素类型及全部PostgreSQL功能仍需分别验收。

最终私有普通后端P12-C3为 `7d3cc2a1353c105ecac62579a919ec96d088cd325fa74234bc47252061e1029a`，默认根程序425保持。完整pg_stats目录/17列SQL与output-only anyarray尚未接通，两条实际探针仍严格失败0A000；本批是采集阶段的partial进展，Goal active。
完整ANALYZE对照为本批同功能未缓存C2／缓存C3／同一C2回测，各3进程；每进程2048行、1个整数和3类数组，先1次不计时预检，再计时3次完整API。process CPU中位数1243.545／164.180／1296.411ms，降低86.80%／87.34%；wall降低85.86%／86.87%。最终版685.690ms wall长尾保留。此结果限于该负载，未测任意SELECT或PG速度。

原生数组786检查及冷读191通过；计算组件O2和ASan/UBSan各32检查（启用泄漏检测，非全库SAN）；17个相关原生入口含完整7worker恢复1574检查全部正常0。原标量641+冷读2、数组19条协议SQL和原标量25条协议SQL通过。真实PG18.6同一554条SQL对照14列325项：301相同，24处严格差异保留（11宽度、11collation OID、2处相同频率元素的截断选择）。没有PG整体通过结论。

原始测量、失败和复核见压缩机器归档（可选本地文件 `benchmarks/analyzed-array-collector-20261105.json.gz`）及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_SCALAR_COLLECTOR_20261105 -->

## 标量统计采集与 ANALYZE 优化（2026-11-05，P11-C4）

本批把真实ANALYZE扫描接入标量统计快照：保留NULL、实际存储宽度和原始扫描顺序，按统计目标维护有界样本，生成signed float4 distinct、MCV、排除MCV后的直方图及相关性。数组/范围专用采集、完整继承/表达式索引统计、真实pg_stats目录源和anyarray输出仍待完成；不是完整17列SQL功能验收。

最终私有普通后端为P11-C4 `e1cab6f6…`；默认根程序425保持。原生641检查及fresh冷读2检查通过，统计计算组件O2与ASan/UBSan各22检查通过（启用泄漏检测，非全库SAN），16个相关原生入口全部正常0。普通Main的CREATE/INSERT/ANALYZE/SELECT四条命令通过，464B实际快照独立解码核对两列；两条完整pg_stats SQL探针仍严格失败0A000，服务器正常0。

相同输入、匹配en_US默认排序规则的真实PG18.6对照共189项：184相同，保留3处实际存储宽度差异和文本槽2处collation OID差异（本地0/PG100）。样本边界按独立样本检查有效范围，不要求随机结果相同。原C-locale参考的文本相关性差异也保留；没有修改SQL语料以取得通过。

仍不能称为PostgreSQL等价：完整17列目录源/真实view与属性身份、anyarray输出、其他类型采集、继承/索引所有者、DDL/恢复和既有权限消费者尚未完整验收。CPU52%左右为本批同功能ANALYZE对照，非PG速度或全库提速。原273项状态22/166/70/15不变、历史262、Goal active。见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

P10完成完整17字段/7类槽的拥有值快照基础，保存真实的关系/属性/继承键、类型声明、操作符和collation身份及可空numbers/values。真实PG18.6确认数组列MCV的元素OID1007、常见元素23、范围长度701，以及多范围bounds使用范围3904；本模型可保存这些不同身份。

SQL侧尚未接入：两条完整17列探针仍0A000，采集算法、真实view/attribute来源、anyarray输出和现有权限消费者接入仍待实现，不以内部组件通过关闭pg_stats或原功能族。编码CPU对照只针对本批新组件，未测PostgreSQL性能或普通SQL提速。[报告](performance-report-2026-10-30.md#完整统计视图的快照基础2026-11-05p10)与归档（可选本地文件 `benchmarks/analyzed-statistics-snapshot-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

P9-C2 `30c57ca8…` 补的是本项目存储生命周期可靠性：两处实际use-after-free已修复，原七阶段恢复及完整Memcheck本次通过。它不等于PostgreSQL完整恢复/epoch/跨进程准入或所有退出时序等价。正常Main保留5项探针元数据差异，完整17字段pg_stats仍为后续工作。

本批9次完整恢复对照未证明稳定提速；没有重测PostgreSQL速度或全部35操作/36查询。默认425保持。原失败与新版本证据见[报告](performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)及归档（可选本地文件 `benchmarks/relation-list-engine-lifetime-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

本批P8-C3 `d778afbd…` 参照恢复后事务分配器必须超过导入事务号的原则，补入本项目公开恢复的只读水位扫描和持久推进；这不等于PostgreSQL的WAL恢复、epoch/wrap或并发机制。较早目标首次探针1→4行得到实际验证，正常Main仍有5项探针元数据差异，最终七阶段原生验收仍因advance阶段异常退出而严格失败。

第二轮仅比较本项目旧P7-C1/C2/C3/旧P7-C1：C3 wall p50较两次旧版低4.16%/6.42%，process CPU低2.47%/3.17%。第一轮C2慢项、6.8秒与18.5秒长尾不删除；没有PostgreSQL速度或完整35/36新结论，默认425未提升。跨进程准入、完整恢复失败及完整17字段pg_stats仍开放，不能把少数字段renderer当视图完成。详见[完整报告](performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和机器归档（可选本地文件 `benchmarks/restore-transaction-horizon-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

当前 P7-C1 `724c1147…` 按PG18.6跳过virtual列统计，普通/STORED与真实NULL/TOAST控制通过。原生20控制修复6项失败，SQL34仍17项差异，qualified pg_stats视图、部分语法与协议帧尚待补齐；原273项没有功能族关闭。

本批只有内部C8/C1/C8速度对照：完整备份wall p50为737.771→587.376→677.477ms，较两次旧版低20.38%/13.30%；四DDLwall p50为2快/2慢，ALTER与TRUNCATE慢于两次旧版。没有新的PostgreSQL速度采样或整库倍率。原有旧→旧恢复探针失败及独立四组成功对照分别保留，完整35操作/36查询未验收，默认425保持。[完整数据与边界](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

当前源码P6-C8 `45264111…` 的针对性258观察为238通过/20差异，新增82整项通过、无整项回退；另保留6个Simple描述符组件损失。原767观察仍129差异，完整结果与冻结旧版/C6一致。原273族状态不变，未认定完整类型、DML、目录或协议族完成。

正常C2/C8/C2的四条100行/八virtual SQL全部正确，SELECT * p50为923.223/80.538/916.453ms，CASE未选g1为863.623/21.730/871.064ms。PG18.6本批仅有功能参考，没有新的PG速度采样或整库倍率。完整35操作/36查询尚未对C8验收，默认425保持；[全部数据和剩余边界](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

本批是425/C2/425内部实测，没有新PG速度测量或功能族关闭。原273仍22 complete /166 partial /70 unverified /15 deferred_by_user，生成列需求/赋值与更多组合仍开放；Goal active。

详见[完整性能报告](performance-report-2026-10-30.md)和本批机器归档（可选本地文件 `benchmarks/analyze-reindex-main-costs-20261104.json.gz`）。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

固定301仍188匹配/113差异，其他四组结果未变；生成列赋值/需求、历史位图、quoted routine元数据、更多目录/输入/来源形式仍开放。原273为22 complete /166 partial /70 unverified /15 deferred_by_user，251项未标记完成。新数据为内部425/C4/425对照，没有新增PG速度测量或稳定跨版本倍率。

详见[本批报告](performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及机器归档（可选本地文件 `benchmarks/returning-declarations-hotpaths-20261104.json.gz`）。以下为历史记录。

## 本批局部能力与距离（2026-11-03）

C3 `425165af…` 在真实RETURNING所有者内保存已经求值的拥有值，并补入实际目标及OLD/NEW物理数组的输入物化；正常binary结果中的记录字段、数组形状和NULL有新增严格控制。固定301与PG18.6参考为188匹配/113差异，相对S3的201差异改善88项；新56项从C8的20差异降至8。剩余6项别名描述符、虚拟数组和历史位图仍是普通缺口，没有认定完整类型/协议/DML族完成。

辅助求值循环的请求次数与字节减少，但公开游标512行分配不变、单次冷CPU20.750→35.862ms变慢；没有新的PG速度对照，也不能将局部构造收益解释为整SQL或所有数据库操作加速。

只读共享内置函数注册表用于减少重复构造，同时保留实际实例覆盖、时钟及参数求值期间的回调替换；这属于本项目内部实现，不代表与PG执行器等价。C3原生选取57为56/1，协议选取32为24/8；最终S3/C3/S3全部36组查询p50低于两次S3：相对首/末降低2.05–33.76%/1.76–29.81%，基线漂移−22.57%至+11.58%。另一组C8/C3/C8的35类操作仍为18快/2慢/15居间，保留update_range、reindex慢项及−90.28%至+130.91%漂移；两种对照不拼成跨版本稳定倍率，没有新的PG速度对照或统计显著性结论。完整代别与原失败见[本批报告](performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)。原273仍22 complete /166 partial /70 unverified /15 deferred_by_user；独立机器归档见全部原始统计（可选本地文件 `benchmarks/function-registry-returning-20261103.json.gz`）。以下C8/C7为历史评估。

## 当前评估（2026-11-03）

C8 `91d339db…` 保留原始类型行物化的局部分配优化，没有扩大类型、目录或DML的兼容范围。26原生/31协议选取集及301/83/111/68/16固定观察与C7连续，301仍有114差异、REGTYPE68仍有11普通缺口；原273状态不变。

512行公开游标累计请求与字节减少，但完整查询36组为5快/18慢/13居间：64kB为0/9/3，4MB为4/2/6，16MB为1/7/4。两次S3基线漂移全部保留，不能用跨窗口数据构造稳定C7→C8倍率，也没有新的PostgreSQL速度采样。源码保留该优化，默认安装仍为S3；详见[本批报告](performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)及机器归档（可选本地文件 `benchmarks/owned-datum-primitive-20261103.json.gz`）。

前批C7评估（保留原结论）：

C7 `f135335e…` 将真实复合值、声明元数据与普通格式 1 的结果编码连通，并补入 REGTYPE/REGTYPE[] 的持久身份。固定 PG18.6 参考的 301 项观察仍有 114 差异；相对 AA52 的 201 差异改善 87 项，没有把剩余或旧失败算作通过。83 项数组位图/CAST 与 111 项多态数组控制各保留两项 SHOW/UTC 配置差异；REGTYPE 物理 68 项为 57 通过/11 差异，正常参数 16 项通过。

新原生三个入口共 38 控制通过；C7 最终选取的 26 原生入口全过，31 协议入口为 24 通过/7 个既有失败入口。C5 完整 106 原生的 105/1 与 95 协议的 86/9 继续按 C5 代别记录，不能称 C7 全量 106/95 重跑。全部原始差异、来源和未完边界见[本批功能与成本报告](performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)。

复合 binary RETURNING、更多 UPDATE WHERE/SET 输入加载、域数组/命名复合目录、数组位图历史、legacy typmod 和 ALTER USING 仍是普通缺口，未新增暂缓。资源55通过；36组速度为4快/23慢/9居间，C7未通过默认安装性能门，根目录保持S3。分配与字节额外成本也已记录；没有新的PostgreSQL速度对照；原 273 仍为 22 complete /166 partial /70 unverified /15 deferred_by_user。以下为前批记录，保留其当时输入与结论。

上一批未接受研究记录：

最新调查基于 `3d576b8b`：两版窗口暂存候选均未接受，安装程序仍为S3 `aa52b5fe…`。scratch2虽减少重复分配，整SQL36组仍为12快/8慢/16居间，4MB+16MB为2/8/14；基线漂移全部保留，未获得稳定跨预算收益，不能引用单一快项宣称整体改善。
新增固定301类型协议观察的实际PG18.6参考通过严格结构/预期错误/原始值验证；S3和两候选逐项相同，各保留201差异。普通REGTYPE物理声明/存储、domain-array身份、named composite、真实record/array二进制值传递等仍有缺口；隔离提案未应用，R151暂停子项不恢复。没有新的PG性能采样，也不更新旧35操作速度倍率。
原273仍22complete/166partial/70unverified/15originaldeferred；完整数据与接受边界见[本批报告](performance-report-2026-10-30.md#窗口暂存分配与类型输出调研2026-11-03)。以下记录继续按各自历史输入解释。

本页总账及 35 类 PG 性能对照以源码输入 `9b71e118` 为准；随后在 `e7dcd673` 上完成下述 Top-N 批次。
之后在 `17df7ff3` 上补充普通非分区 TableScan 按页扫描，局部验证范围分别记录，不重写历史 PG 样本。
随后以 `bcde5b3e` 补充普通 LIMIT 接入及正常格式下的顶层 SQL 运行错误前缀，见下方最新局部记录。
再以 `d09cb6c1` 实现普通物理排序的内存预算、匿名临时文件归并和受支持的真实 SQL 接入，其他排序分流仍分别开放。
随后基于 `471dad3f` 完成普通物理 Sort 最终按需归并、NULL 绑定恢复和错误清理；其他查询慢值仍未解决。
再基于 `049c0bce` 接通会话 work_mem 与普通物理 Sort 的实际预算，配置函数和完整例程/算子治理仍开放。
随后基于 `d8533bc9` 为普通类型化/多键排序增加预算、完整快照和比较缓存，其他排序/算子边界继续分别开放。
再基于 `8a8532a0` 接入普通 UNION ALL/VALUES 的预算排序、真实 VALUES 查询子句和枚举输出描述符；其他集合分流及完整参数推导继续开放。
前批基于 `baba4bcb` 扩展普通 UNION ALL 全局计数表达式、VALUES/集合根 FETCH `c_expr` 和实际选中上下文的参数声明推导。
前批基于 `9f019ffa`，最终 C9 程序 `c696d3b2` 接入 Main/Core/真实 SQL 子查询共用的类型化窗口宿主；
窗口内存治理、通用 Core JOIN lowering、完整参数推导及其他 PostgreSQL 能力仍开放。
随后基于 `b04db801`，C1 程序 `56d06bca` 仅补入窗口私有整数键缓存；窗口预算和上述功能边界不变。
本批基于 `d7793044`，当前 S3 程序 `aa52b5fe` 保留 S2 的实际窗口定义/排序投影共享规则，并让结果映射和顺序均未改变的 stage 使用原类型化执行循环；仍不是任意表达式或所有 volatile 调用的全局合并。
产品仍为 0.2.0 / C++17，仓库既定行为基线为 **PostgreSQL 18.6**。
本项目已有真实的单机数据库内核，但距离 PostgreSQL 级别的通用功能、规模扩展和生产验收仍有明显差距。
这次更新复核总账、当前源码和测量记录，不重新认定 273 项要求已经验收，也不变更用户暂缓范围。

功能差距不能用 SQL 关键字数估算；性能差距也不能从本项目优化前后的倍率推算。
当前有限子集可用于独立数据目录中的开发和研究；尚无证据支持无差别替换 PostgreSQL、完整扩展生态或生产 SLA。
兼容性声明继续受[契约](compatibility-contract.md)约束：客户端互操作 A 门、行为兼容 B 门均未宣告通过。

## 功能距离：273 项总账仍有 251 项未标记完成

| 总账状态 | 数量 | 应如何理解 |
| --- | ---: | --- |
| `complete` | 22 | 总账已有完成标记及历史引用；本次没有逐项重新验收 |
| `partial` | 167 | 存在实现或局部验证，仍有语义、边界或验收缺口 |
| `unverified` | 69 | 尚待验证是否满足该项要求 |
| `deferred_by_user` | 15 | 原始总账中的用户暂缓项，仍是兼容性缺口 |
| 合计 | 273 | 还剩 251 项未标记完成；排除原始 15 项暂缓后仍有 236 项 |

**22/273 = 8.1% 只是总账条目的标记比例，不是 PostgreSQL 功能完成率。**
22 项中，13 项属于项目语法偏移/拒绝假成功，3 项属于工程与版本策略；各条目工作量也不相同。
不能据此说“只实现了 8.1%”，更不能沿用旧页未证实的“95% 语法覆盖”作为成熟度。
[细化条款](review-requirement-clauses.json)目前只拆解了原始 273 项中的 3 项，另 270 项待拆解；
局部测试通过不能自动关闭整个功能族。

以下计数直接来自 [gap-progress.json](gap-progress.json)，与[原始审计](postgresql-18-gap-audit.md)保持一致：

| 功能族 | 总项数 | complete | partial | unverified | 用户暂缓 |
| --- | ---: | ---: | ---: | ---: | ---: |
| P0前置条件 | 16 | 0 | 15 | 0 | 1 |
| SQL/目录/类型/函数/约束 | 72 | 4 | 67 | 1 | 0 |
| 查询与DML | 19 | 0 | 19 | 0 | 0 |
| 优化器与执行器 | 17 | 0 | 8 | 9 | 0 |
| 索引 | 15 | 0 | 4 | 11 | 0 |
| 事务/锁/VACUUM | 16 | 2 | 10 | 4 | 0 |
| 存储与WAL | 16 | 0 | 15 | 0 | 1 |
| 复制/备份/HA | 17 | 0 | 12 | 5 | 0 |
| 协议与客户端 | 14 | 0 | 8 | 6 | 0 |
| 安全 | 13 | 0 | 0 | 0 | 13 |
| 监控与运维 | 16 | 0 | 7 | 9 | 0 |
| 扩展与FDW | 13 | 0 | 1 | 12 | 0 |
| 工程与生产验收 | 15 | 3 | 1 | 11 | 0 |
| 项目语法偏移 | 14 | 13 | 0 | 1 | 0 |

原始 15 项暂缓包括 P0-13、STO-08 和 SEC-01..13；之后按具体问题追加的暂缓记录另见
[范围说明](deferred-scope.md)，没有合并或改写原始 273 项计数。本次只评估这些缺口，不恢复暂缓调查。

前批类型化排序局部记录：39 原生全过、完整所选 26 协议为 25/1，新 108 在 PG/最终程序全过，既有 463/40 暂缓差异保持。
9 项实际 Main 资源检查证明普通无 ARRAY 的多键/表达式排序也在低预算落盘；输入边界分配显著下降，但全查询峰值与普遍加速没有由此获得证明。
三个预算及一次高预算复测的 1,584 个延迟样本完整列于[本批报告](performance-report-2026-10-30.md#普通类型化排序的预算与比较缓存2026-11-02)，不能沿用历史倍率代表最新版本。

前批集合/VALUES 局部记录：44 原生全过，新 347 协议控制及额外 52 类型阶段观察在 PG/最终程序全过。
完整 31 协议首轮 29/2，未发 SQL 的启动连接失败同源复测通过；既有 463/40 暂缓观察逐字节不变，最终逐入口复核为 30/1。
新增52永久协议入口独立验证通过；全部32个已运行入口按各自最终结果复核为31/1。
七项 Main 资源、35 类操作有限检查及匿名文件内强杀/重开通过；完整新旧速度对照和未解决的慢值见[集合排序与 VALUES 报告](performance-report-2026-10-30.md#集合排序预算与-values-查询子句2026-11-02)，不能用历史倍率代表最新版本。

前批集合键优化：45原生全过，33协议32/1；新66在PG/前版/新版全过，原463/40暂缓日志相同。
实际整数排序窗口改善；1980样本保留全部漂移和慢控制，见[集合键缓存报告](performance-report-2026-10-30.md#集合整数键缓存与比较参数复制2026-11-02)。原273状态与整体PG差距不因该局部优化关闭。

当前已接入的普通 UNION ALL 支持全局 LIMIT/OFFSET 表达式，括号与未括号根共用实际执行路径；VALUES/集合根还支持 FETCH 的参数、函数调用和括号表达式等 `c_expr` 计数。
全局计数只看到空本层及真实祖先作用域，不把集合输出别名或分支 FROM 误当作可用变量；分支自己的局部计数单独生效。
执行时 OFFSET 先于 LIMIT，计数为零时不打开分支；动态 NULL count 不限行数，区别于裸 NULL FETCH WITH TIES 的分析错误。
有限且加法安全的动态上限可用于 Top-N，逐次重置；后续 NULL/不限行数撤销旧界限，WITH TIES 不截断同行输入。
未知 OID 输入按实际选中上下文推导声明的支持仍为 partial，计数结果 bigint 不等于所有输入类型 bigint。
受支持的合法窗口标量子查询现已使用真实窗口宿主；本层计数直接调用窗口函数仍经 AST 分析报 `42P20`。
其他集合运算、普通 SELECT 动态计数、完整函数签名推导和原始 QRY-06 功能族未完成。

窗口宿主执行真实行源、过滤、窗口投影和最终子句，支持已验证的 derived/CTE、Main SourceContexts JOIN 与外层关联，
按实际原始子句保留命名继承、ROWS/RANGE/GROUPS/EXCLUDE 和 frame 初始化身份。
排名及受支持的普通窗口聚合保留实际结果类型、NULL 和对象元数据；有限 INTERVAL 除法及 AVG 修正余数换算与舍入。
完整输入键每行按需求只求值一次，匹配的投影、窗口参数/FILTER 和后续 stage 复用拥有值；
241 项输入键协议的规范化值、计数及消息字节与 PG 18.6 一致，frame offset 不参与这种共享。
实际生产者保证最终 ORDER 为既有顺序的兼容前缀时省去最终 Sort，反向/不同表达式等反例仍保留排序；
窗口 WITH TIES 使用已有键并保留边界行预读、OFFSET 和投影/参数的求值需求。

这仍是有限实现：窗口 stage 排序、分区和历史行驻留内存，不受 work_mem 约束且没有 spill；
统计窗口聚合的合法签名仍报 `0A000`，window DISTINCT、内部 ORDER BY、数组值 ARRAY_AGG、
完整聚合族及 temporal in_range 尚未完成。正常执行的这些改进不改变保留计划、参数帧/provider、
portal 或其他既有暂缓范围，也不关闭 QRY-06、QRY-08 或原 273 项。
前批 C9 的 1,000 行共同窗口对照中，first/last/nth 组合相对旧回测约快 5.15–5.84 倍，其他七类在各预算仍较慢；
旧窗口最终排序与新TypedSort的资源策略也不同，不能称全部查询加速或据此更新历史PG倍率。
前批宿主证据和速度限制见[类型化窗口宿主报告](performance-report-2026-10-30.md#类型化窗口宿主与有序输出复用2026-11-03)；
历史失败与慢值保留，不能据此宣称普遍加速或完整回归全绿。

前批 C1 的唯一生产变化是私有窗口实现 `85aef835`：仅在实际非 NULL、非枚举比较中，
双方原始类型相同且为 `smallint`/`integer`/`bigint`，真实已绑定 OID 为对应的 21/23/20 时，
按拥有值的 stage/key 单元惰性解析并缓存精确整数。OID 0、真实 domain、缺权威 OID 的类型别名或隐藏表达式、
全串解析失败或范围溢出均回到原比较 owner；原本支持的表达式不会因此新增拒绝。
三类参数来源、frame/context/provider、Main、共享比较器和 R185 helper 原文不变。

每个缓冲 Input 都增加 vector 头及外层容量占用，回退行也不例外；payload 和 stage OID 元数据按需分配，
close 释放行内 payload/元数据后，外层容量仍可保留至图销毁。窗口仍无 work_mem/spill，累计分配请求下降不等于 RSS 下降。
额外 allocator 18 进程/162 快照全部完成：整数 1,000 行控制首/末行 uordblks 中位差 +68,160 B，
文本 +22,544 B、单行 +48 B，close 后仍有容量保留；快照不能证明精确 live、真实峰值或 RSS 改善。
该 C1 批次的 71 个 native 入口通过（新 36 控制），61 个协议入口为 56 通过、5 失败；
新 143 控制的 14 项既有差异与 C9 全部 actual 相同，原四个失败入口的观察也保持不变。
共同八类 public cursor 独立执行阶段进程 CPU p50 降低 41.85–61.90%、C++ 分配请求减少 53.89–63.96%。
另行完成的整 SQL 24 组中，4 MB/16 MB 的 16 项 p50 均比两次 C9 低 22.3–39.1%；
64 kB 仅 2 项比两旧都快、3 项比两旧都慢（partition_lag、running_sum、partition_full_sum），其余混合。
C9 首/回测在 64 kB 的漂移 +13.2% 至 +75.8% 全部保留；不据此宣布七类慢值全清、统一加速或改写 old58/PG 倍率。
前批 35 项数据正确但 shutdown 失败的整入口仍为失败，本批未重跑。
方法、准入和限制见[窗口整数键缓存报告](performance-report-2026-10-30.md#窗口整数键缓存与实际执行成本2026-11-03)。

中间候选 S2 在同一真实窗口 stage 内复用等价非 volatile 调用；含 volatile 参数的复用还要求真实最终 Sort 保留下层排序输入投影，
并只用于最后一个实际需要的窗口 stage。先保留必须提前计算的完整目标，再抽取推迟目标中的完整窗口节点，
共享与执行次序都服从该序列；不能跨不同 winref、独立 SQL child 或保留包装表达式的子节点随意合并。
原调用/结果列/需求、参数 frame/context/provider 均保留；代表按本次实际需求选取并分发完整拥有值，末个别名 move，不跨行或重开缓存结果。
准备期 v4 惰性构造可能匹配者的身份，并只在本次准备 memo 原 AST 元数据，不改原规划、分组聚合或共享排序 helper。

新 220 项严格 PG 18.6 对照在 S2 全过，前 C1 的 27 项差异全部消除；原 143 项由 14 失败降至 8，原四个失败入口的 1641 项观察保持原样。
72 个 native 入口通过；首次完整协议 62 个入口为 56/6，其中一次连接启动失败时尚未执行该入口的 190 项 SQL。
保留初失败后，同源 S2 独立 190 项复核 0 预期差异，最终 62 个独立入口为 57/5，未把复核计作新增入口或删除失败历史。
以下为相同 512 行 heap、真实已绑定 public cursor 的 warm 区域累计 C++ 分配请求；包含 restart、完整消费、关闭和 owning 接收，初次建图在外。

| 查询控制 | C1 请求次数 | S2 请求次数 | C1 / S2 请求字节 |
| --- | ---: | ---: | ---: |
| LAG 一份 | 226489 | 226490 | 25625206 / 25625214 |
| LAG 两份 | 419723 | 232335 | 48048290 / 26317466 |
| LAG 四份 | 806565 | 244394 | 92906086 / 27713486 |
| SUM 两份 | 44542 | 43514 | 4621224 / 4432054 |

单份控制的额外请求保留；这些不是 SQL 延迟、malloc 全量、live/峰值或 RSS 数据，也不证明 v4 准备时间已改善。
独立资源 55 项全部符合预期，含 40 次完整读取、一次预期终止及 14 项 SET，校验 40,000 行/118,000 单元；64 kB 最终 TypedSort 最多观察到两个临时 FD，EOF 无残留且同目录重开通过。
资源测试不证明窗口预算治理、任意写入恢复或 R15 停机问题已修复。
三预算整 SQL 对照完成 1296 次计时、324 次预热、18 项 SET/SHOW，全部 1,620,000 行/4,725,000 单元符合固定预期。
36 项中仅 10 项的 S2 p50 低于两次 C1，19 项高于两旧、7 项位于两旧之间或相等；共同八类的 24 项有 17 项高于两旧。
重复两份/四份 LAG(length) 的三预算控制分别较两旧降低 37.95–41.66%/64.46–66.16%，单份和重复 SUM 表现混合，不能用重复调用收益代表整体查询提速。
全部慢值、旧版漂移及资源边界见[窗口结果共享报告](performance-report-2026-10-30.md#窗口结果共享与排序投影2026-11-03)。
尚未补齐完整已分析常量/cast 等价、复合 binary 输出或窗口预算治理，原 273 项和旧 C9 停机失败继续保留，不宣称整体兼容或全面提速。

当前 S3 的原类型化循环只用于全 stage 的 base/sorted owner 均未变化、无外部别名指入且两个过滤后调用顺序都等于原序的情况。
其余形状继续使用 S2 通用路径，包括隐藏 ORDER 调用重排、共享组和仅别名被需求的组；原实际需求、参数 frame/provider 及 owning 结果不变。
S3 的 72 个 native 入口通过，完整 62 个协议入口首轮为 57/5。新 220 和原 143 的所有原始观察与 S2 相同，原四个失败入口的 1641 项也保持。
190 项此次首轮完整通过，全部服务端原始观察与 S2 独立复核相同；仅请求中的随机私有命名不同，S2 的首轮 0 SQL 失败及随后复核均保留。
同一 512 行 warm public cursor 中，S3 的 LAG 一份/两份/四份及 SUM 两份请求分别为 226489/232335/244394/43514 次，
请求字节分别为 25625203/26317460/27713474/4432054 B。单份次数与 C1 相同，3 B 差异不视为内存收益；这些也不是 SQL 时钟或 RSS。
同范围资源 55 项独立通过；S3 三预算整 SQL 对照保留 1296 次计时、324 次预热和 18 项 SET/SHOW，共 1,638 次请求，1,620,000 行/4,725,000 单元全部符合固定预期。
全部 36 项按 p50 数值为 11 项低于两次 C1、19 项高于两旧、6 项居间；共同八类的 24 项为 3 低/16 高/5 居间，64 kB 共同八项全部高于两旧。
这是数值分类，不证明统计显著性或全面加速。重复两份/四份 LAG(length) 的三预算控制较两旧分别降低 37.94–40.83%/63.65–66.34%，
C1 首/回测漂移 -11.01% 至 +11.92% 完整保留。这些独立 C1/S3/C1 窗口不是同窗 S3/S2 比值，也不能沿用前批 C1 的改善当作 S3 结论。
S2 全部 36 项及此前两轮 68 请求、48 进程公开 API 分阶段诊断继续在报告中单列，未用诊断推导所有慢项的单一原因。

## 主要能力差距及实际影响

| 领域 | 当前已有能力 | 距离 PostgreSQL 18 的主要差距 |
| --- | --- | --- |
| SQL、类型、目录与 DML | typed AST、常用查询/约束、集合/子查询和多种数据类型的真实执行子集；UNION ALL/VALUES 计数表达式及 FETCH `c_expr`；Main/Core/SQL 子查询的受支持 typed WindowAgg | parse/analyze/rewrite/planner 边界、其余窗口族与帧/类型组合、普通 SELECT 动态计数、完整参数/函数重载推导、目录字段/依赖和事务化 DDL 仍不完整；命令能解析不等于所有组合语义正确 |
| 优化器与计划 | 顺序/受限索引扫描、三类 JOIN、基础统计和代价选择、部分参数化查询 | 缺完整候选 path 搜索、等价类、参数化路径、多表 JOIN 搜索和统计模型；计划质量不能随 SQL 复杂度稳定扩展，见 [OPT-01](issue-opt-01-path-planner-audit.md) |
| 执行内存与并行 | 部分 heap page-range 多线程扫描、排序协作中断；类型化有序 LIMIT 有界堆、非分区 TableScan 按页游标；普通物理单键、已接入的普通类型化/多键及 UNION ALL/VALUES Sort 的会话预算与外部按需归并 | 聚合/HAVING/未接入集合分流、哈希/窗口/JOIN、结果接收器和部分源仍可能全量物化；配置函数/完整例程 GUC、完整资源治理及 worker 生命周期仍缺，见[条款](review-requirement-clauses.json) |
| 索引与修改路径 | B+Tree、Hash 和其他访问方法的有限实现；部分 SELECT 使用真实索引候选 | 其余 AM/opclass、并发/WAL/谓词范围规则和 index-only/bitmap 能力不完整；本轮 UPDATE/DELETE 整数预筛选仍是堆扫描，不是物理索引定位，见[实现与实测](performance-report-2026-10-30.md) |
| 事务与并发 | MVCC、保存点、行/表锁、死锁检测、SSI/2PC 的子集；advisory lock 和 NOTIFY 有完成标记 | 不能写成“没有行锁”；缺完整 snapshot/command counter、子事务、MultiXact、EPQ、索引范围谓词锁和 DDL 回滚一致性，见审计 TXN/VAC 族 |
| 存储、恢复和维护 | 8 KiB 页、缓冲池、TOAST、WAL/CLOG、校验、归档、单节点恢复/PITR 子集；近期修复 WAL 截断后重开 | 自有磁盘格式，不能打开 PostgreSQL 数据目录；全资源 WAL、freeze/HOT/VM、调度节流、多时间线/升级恢复和长期故障验证仍有缺口 |
| 复制与高可用 | 复制槽/发布管理对象和项目格式变更预览 | [复制模块](../src/replication/README.md)没有完整物理流复制、standby、真实订阅接收/应用；变更流仍在内存中，不能承诺故障切换和远端同步确认 |
| 协议与客户端 | wire protocol 3.0、SCRAM、Simple/Extended Query 和部分 binary I/O 的真实子集 | 全消息/类型/错误/portal/COPY/工具行为和客户端版本矩阵未完整验收；当前 CMake 实测产物为 TLS stub，不能据此通过真实 TLS 的 A 门 |
| 监控、配置和运维 | 部分 `pg_stat_*` / `pg_settings`、配置持久化/reload、维护和备份入口 | 视图列/快照、wait events、进度/资源保护和运维演练不完整；`buffer_pool_frames` 未接到实际池，`checkpoint_timeout` 显示与后台间隔不一致，见[手册](MANUAL.md#25-guc-参数配置) |
| 扩展与生态 | 少量内置能力与项目扩展命令 | `CREATE EXTENSION` / FDW 等没有 PostgreSQL 插件运行时；缺 extension/fmgr ABI、hooks、动态后台 worker 和常用扩展兼容，不应视为可直接运行 PostGIS 等扩展 |

PostgreSQL 的对标能力以官方文档为准：[并行扫描/JOIN/聚合](https://www.postgresql.org/docs/18/parallel-query.html)、
[工作内存和临时文件](https://www.postgresql.org/docs/18/runtime-config-resource.html)、
[物理复制与 standby](https://www.postgresql.org/docs/18/high-availability.html)、
[逻辑复制接收/应用](https://www.postgresql.org/docs/18/logical-replication.html)、
[扩展安装运行时](https://www.postgresql.org/docs/18/sql-createextension.html)。
这些是能力差距，不意味着开启并行或 JIT 就必然加速所有 SQL。

## 性能距离：分别看内部改善与 PostgreSQL 对照

2026-10-30 的[性能报告](performance-report-2026-10-30.md)证明了本项目部分路径的改善：
UPDATE、DELETE、保存点、排序和小表 ALTER 收益明显；普通点查和单行 INSERT 没有证明稳定提速。
35 类操作、315 个最终样本和旧/新各 210 个公共窗口样本，只比较本项目版本，不能用来推算与 PostgreSQL 的速度差。

本次对 PostgreSQL 18.6 补测了相同的 35 类操作，每侧 210 个计时样本，现有 oracle 均通过。
以下为其中几个代表操作，完整 35 类结果和方法见[新增对照](performance-report-2026-10-30.md#postgresql-186-同机对照2026-11-02)：

| 1,000 行夹具中的操作 | 本项目 p50 ms | PostgreSQL 18.6 p50 ms | 本项目/PG 延迟比 |
| --- | ---: | ---: | ---: |
| 普通主键点查 | 15.280 | 0.360 | 42.5× |
| INSERT 单行自动提交 | 20.323 | 0.625 | 32.5× |
| UPDATE 主键单行 | 55.423 | 0.653 | 84.9× |
| DELETE 主键单行 | 26.934 | 0.573 | 47.0× |
| ALTER ADD COLUMN（5 行） | 920.287 | 0.916 | 1004.3× |
| CHECKPOINT | 19.966 | 13.762 | 1.5× |

这是两边采用各自默认内存/维护参数的单客户端、热缓存小样本；没有统一内存额度或进行长时间压测。它只回答指定 SQL、夹具、默认设置及采样窗口的往返延迟，
不证明大表、冷缓存、并发吞吐、p99、故障下的可用性或完整业务负载达到 PostgreSQL 水平。
不能把某一条 SQL 的倍率当作“整体性能百分比”，也不能与 10 月 30 日的内部倍率相乘。

当前代码仍有明确的性能扩展瓶颈：

- 普通修改和部分 DDL 的物理快照、摘要校验及目录/文件工作量随数据库状态增加；5 行 ALTER 变快不等于大表在线 DDL。
- UPDATE/DELETE 的必要条件堆预筛选减少了解码，但没有消除整表扫描；整数 JOIN 预筛选也没有消除全部候选组合和物化。
- 普通类型化排序 LIMIT 已限制候选，普通非分区 TableScan 已按页供给；其他旧分流、分区/RLS/并行等物化及完整工作内存/spill 仍未完成。
- WAL、提交持久化和后台维护等待仍影响单行与尾延迟；不能用关闭 fsync、放宽语义或忽略错误来换取“对标”。
- 完整代价/统计/多表搜索、并行执行和异步 I/O 能力仍有差距；先消除当前常见路径的额外工作，再判断高级加速是否值得。

后续 Top-N 批次已修复可支持的单表有序 LIMIT 在执行分流中丢失多键顺序或提前计算投影的问题：
新增 48 条协议检查在 PG 18.6 与新版全过，旧版有 11 条失败；独立旧/新/旧的 1,000 行 LIMIT/OFFSET 延迟约改善 1.4 倍。
这批比较没有重新测量 PG 延迟，不能用新值除以上表旧 PG 样本；详细边界见[性能报告](performance-report-2026-10-30.md#有界排序-top-n2026-11-02)。

按页扫描批次又完成 23 个原生/17 个协议入口及 35 类操作有限回归。已有类型化数组投影 LIMIT 1 在最终 1,000 行夹具窗口约快 1.38–1.47 倍，
该扫描批次尚未优化普通列投影 LIMIT 1；新增写函数快照 13 条与 PG 18.6 一致。内存改善只指扫描保留行数据，
不是全进程/RSS 或所有查询均已流式，见[扫描报告](performance-report-2026-10-30.md#按页物理扫描2026-11-02)。

最新普通 LIMIT 批次将可支持的单表查询接入该扫描链：完整 62 条专项在 PG 18.6/候选全过，冻结旧版 9 条失败，
普通列 LIMIT 1 在相邻 1,000 行旧/新/旧窗口约快 2.3 倍。24 原生入口通过，完整所选 21 协议为 20 通过、1 失败；
后者是 R304/R305 已暂缓的完整 463，旧/新相同 40 处差异原样保留。没有恢复其调查或把整批写成全绿。
不限 portal 行数的运行错误前缀已支持，完整 portal/网络流式和其余原始功能族仍开放；详见[普通 LIMIT 报告](performance-report-2026-10-30.md#普通-limit-按需执行与错误前缀2026-11-02)。

普通物理排序批次以 `d09cb6c1` 为输入，把实例工作预算和临时文件归并接入受支持的真实单表单键排序。
完整 47 条 PG 18.6/最终程序全过，冻结旧版 1 条整数序号排序失败；当批 core 29 原生通过，实际 Main 所选 22 协议为 21/1。
已有暂缓 Bind 的 463/40 观察一致；前一候选 COPY 超时及隔离旧/新/旧失败仍保留，最终通过不证明 R197 已修复。
单键无界排序在最终两个窗口约快 1.27–1.40 倍，部分其他控制更慢；没有重测 PG 延迟或证明所有查询无回退。
该排序历史批次尚未接入会话 SET，类型化/多键等其余排序、其他算子及结果缓冲仍需推进，见[外部排序报告](performance-report-2026-10-30.md#普通物理排序的内存预算与外部归并2026-11-02)。

最新普通物理 Sort 完成最终按需归并及绑定恢复，31 原生通过，完整 22 协议仍为 21/1，排序 47 条 PG/新版全过。
64 KiB、8,192×2 KiB 的原生外排对照约快 1.78–1.86 倍；504 个小表 SQL 样本未证明稳定提速，不推算与 PG 的延迟倍率。
既有暂缓差异保留，完整范围及慢值见[最终归并报告](performance-report-2026-10-30.md#最终归并按需输出与排序绑定恢复2026-11-02)。

最新会话 work_mem 批次支持 utility SET/LOCAL/SHOW/RESET、启动与重载优先级、事务/保存点，并接入上述普通物理 Sort。
36 原生及新 163 控制通过；完整所选 25 协议为 24/1，新 470 条在 PG/最终程序全过，旧版 404 不符，原有 463/40 暂缓差异保留。
实际 Main 31 个资源控制/4,000 单元格核验通过；504 SQL 样本与 600 CPU 诊断仍含慢值，不宣称性能等价或全面无回退，见[会话工作内存报告](performance-report-2026-10-30.md#会话工作内存与实际排序预算2026-11-02)。

## 应如何缩小差距

以下是基于现有缺口的技术顺序，不是工期承诺，也不恢复用户暂缓项：

1. **先明确可支持的子集及正确性门槛。** 对常用 SELECT/INSERT/UPDATE/DELETE/事务/DDL 建立值、类型、SQLSTATE、可见性、回滚和重开的一致回归。
2. **降低常见操作的固定和随库增长的开销。** 在保留 MVCC/SSI/RLS 和持久化保证的前提下减少物理快照/扫描/物化、增加真正的 DML 索引候选定位。
3. **完成资源治理和计划选择。** 把内存参数接入执行、实现 spill 与可取消的 worker/I/O；补非暂缓范围内的统计与访问路径/多表计划选择。
4. **验证规模和长期运行。** 同机分级数据量、1/4/16/32 客户端、只读/写多/混合负载，记录吞吐、p50/p95/p99、CPU/RSS/I/O/WAL、错误与恢复；明确后台维护及相同耐久性语义。
5. **再扩大生产与生态目标。** 完成非暂缓的恢复/备份/复制/工具链门；安全、权限、TDE 和已明确暂缓的专项仍按原范围处理，缺口继续如实列出。

PostgreSQL [pgbench 的测试建议](https://www.postgresql.org/docs/18/pgbench.html)要求足够长的重复测量，并提醒数据规模、
竞争、死元组/维护和客户端瓶颈会改变结果。正式性能目标应预先约定工作负载与验收线，不能先拿小表最佳样本再反推“达到 PostgreSQL”。

## 本次更新与核查范围

本页替换了旧对比页中失效的代码行数、开发年限、137+2 测试数、95% 语法覆盖等估计，以及“没有行锁/完全没有并行”等过时总结。
旧版本可以从 Git 历史查看；逐项失败和局部修复证据仍在[审查索引](review-index.md)，没有因重写对比页删除。
273 项总账、原子条款和暂缓文件未改；[最新构建与文档核查](documentation-inventory.md)和[功能测试报告](test-report.md)继续限定各自批次。
