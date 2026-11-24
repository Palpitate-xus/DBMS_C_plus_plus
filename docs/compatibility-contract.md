# 兼容版本契约

> 历史基准原始数据不随 Git 仓库提供。本文中的 `benchmarks/` 路径仅指可能存在的本地可选证据；新克隆仓库没有这些文件，原始结果只能在另行取得数据后复核。文中的测试与性能摘要仍保留。

## 2026-11-24 · 系统目录函数的返回类型

查询绑定到内置 `format_type`、`pg_get_indexdef`、`pg_get_constraintdef` 或 `pg_get_expr` 时，执行沿用该调用确定的结果类型。明确使用 `pg_catalog` 的调用保留内置函数的文本结果。未指定模式的调用按搜索路径及参数类型选择函数。 [测试报告](test-report.md#bound-catalog-results-20261124)

## 2026-11-24 · 查看显式索引的列

普通表和临时表上用列名创建的 B 树、哈希索引可在 `pg_attribute` 读取列信息。B 树按键列和 `INCLUDE` 列的顺序编号。索引键保留排序规则，附带列的排序规则编号为 0。哈希索引按 PostgreSQL 的目录约定显示 `integer` 键，原表列的类型不变。表列改名时保留原索引列名，`pg_get_indexdef` 显示当前表列名。表达式索引、分区索引和其他索引类型的完整列详情仍待实现。 [使用说明](MANUAL.md#用-psql-查看索引的列) · [测试报告](test-report.md#index-attributes-20261124)

## 2026-11-24 · 查看部分索引的条件

普通 B 树的完整索引返回空的 `pg_index.indpred`。部分索引中支持的整数比较、布尔条件和空值判断以 `pg_node_tree` 返回，可由 `pg_get_expr` 输出 SQL。索引定义也会包含这些条件。目录暂不支持转换函数、文本比较和表达式索引的键。 [查询示例](MANUAL.md#查看部分索引的条件) · [测试报告](test-report.md#index-predicate-20261124)

## 2026-11-24 · 查看普通表的主键约束

普通非临时表的主键约束拥有独立的 `pg_constraint.oid`。可读取列号、定义和索引编号 `conindid`。外键引用已登记的主键时使用同一个索引编号。`conindid` 对尚未登记的唯一键返回空值。 [查询示例](MANUAL.md#查询主键和外键使用的索引) · [测试报告](test-report.md#primary-constraint-20261124)

## 2026-11-24 · 查看普通表的主键索引

普通非临时表的主键索引有独立的目录编号，通过 `pg_index.indisprimary` 标识。可按编号查看索引定义、键列及其类型，也能在 `psql` 中列出索引及所有者。分区表、临时表的隐式索引和唯一约束自动创建的索引仍待补齐。 [查询示例](MANUAL.md#查询主键索引) · [测试报告](test-report.md#primary-index-20261124)

## 2026-11-24 · 按表查询索引

`pg_index` 对已登记的显式索引提供索引编号、表编号、列数和状态字段。总列数包括附带列。`indisclustered` 和 `indisreplident` 读取保存的索引指定项。表达式和列向量字段仍待支持。 [查询示例](MANUAL.md#按表查询索引) · [测试报告](test-report.md#pg-index-20261124)

## 2026-11-24 · 查询索引的定义

`pg_get_indexdef` 按索引编号读取已登记的普通 B 树、唯一 B 树和哈希索引。未知编号或普通表编号返回空值。用三参数形式指定列序号，可查看索引键及附带列。读出的名称随表或列改名更新。表达式索引的完整定义仍待支持。 [查询示例](MANUAL.md#查询索引的定义) · [测试报告](test-report.md#index-definition-20261124)

## 2026-11-24 · 把查询结果收进数组

`ARRAY(SELECT ...)` 接受恰好一列，按子查询给出的顺序收集元素。零行产生相应类型的空数组。标量列里的 SQL 空值作为数组元素保留。数组列会增加一维，输入中的空数组、空值或不同的维度和下标均报错。简单和扩展协议使用实际数组类型。 [查询示例](MANUAL.md#把查询结果收进数组) · [测试报告](test-report.md#array-subquery-20261124)

## 2026-11-24 · 列出数据库角色

`pg_catalog.pg_roles` 提供实际角色编号、名称、权限标志、登录选项和密码有效期。视图返回固定掩码 `********`。配置列 `rolconfig` 返回 SQL `NULL`，角色级配置仍待支持。本项目协议字段引用实际 `pg_roles` 视图及其列编号。 [查询示例](MANUAL.md#列出数据库角色) · [测试报告](test-report.md#roles-view-20261124)

## 2026-11-24 · 查询分区归属

`pg_catalog.pg_partition_ancestors(regclass)` 返回 `regclass` 行。分区根表返回自身，旧目录中有可确认父表的子分区按由近到远的顺序返回自身和父表。普通表、普通继承表、未知表编号和 SQL `NULL` 返回零行。若按名称指定的表不存在，则返回 `42P01`。旧目录记录与分区定义不能确定唯一父表时会报错。独立分区表、分区索引及完整的关联函数查询仍待补齐。 [查询示例](MANUAL.md#查询分区归属) · [测试记录](test-report.md#partition-ancestors-20261124)

## 2026-11-24 · 查询列的排序规则

`pg_collation` 提供 `default`、`C`、`POSIX`、`ucs_basic` 及项目自带的 `nocase`、`reverse` 和 `C.utf8`。目录列保留各自的 SQL 类型，包括名称、对象编号、编码和可空的区域设置。普通表显式使用这些规则时，`pg_attribute.attcollation` 指向对应记录。用户创建的排序规则及完整区域设置目录仍待补齐。 [查询示例](MANUAL.md#查询列的排序规则) · [测试记录](test-report.md#collation-catalog-20261124)

## 2026-11-24 · 查询列的默认值

`pg_attrdef.adbin` 可返回受支持的 `pg_node_tree` 常量节点。`pg_get_expr` 用两种参数形式将该节点显示为 SQL，支持常见 `integer`、`bigint`、`boolean`、`text` 常量和既有的 `regclass`、`regtype` 标量与数组常量。对象引用根据编号和当前搜索路径显示。旧数据目录中的已登记普通常量也可以读取。所选表达式没有支持的节点时返回 `0A000`，该行的默认值编号和列号仍可查询。通用节点序列化和用户输入的 `pg_node_tree` 仍未开放。 [查询示例](MANUAL.md#查询列的默认值) · [测试记录](test-report.md#column-default-expression-20261124)

## 2026-11-24 · 查询列的类型

`pg_catalog.format_type(oid, integer)` 返回 `text`，用列目录中的类型编号和修饰值显示名称、长度和精度。覆盖内置的常见标量、数组和目录登记的枚举及域。查找普通类型名时使用当前连接的搜索路径和真实类型编号。空类型编号返回 SQL `NULL`，编号 `0` 返回 `-`，不存在的编号返回 `???`。自定义类型的修饰值输出函数和未登记到 `pg_type` 的用户复合类型仍待补齐。 [查询示例](MANUAL.md#查询列的类型) · [测试记录](test-report.md#format-type-20261124)

## 2026-11-24 · 列出普通表及其访问方式

原版 `psql` 18.6 的 `\dt` 可列出普通表，覆盖按名称筛选、指定模式、搜索路径及临时表。所有者名称来自当前目录中的实际角色。 `pg_am` 提供访问方式编号、名称和种类，普通堆表与已登记的显式索引通过 `pg_class.relam` 关联。项目专有方法采用系统保留编号。 `amhandler`、约束隐式索引的完整目录、`\d` 的列详情以及 `\di` 仍待补齐。 [测试记录](test-report.md#pg-access-method-20261124)

## 2026-11-24 · 按名称和搜索路径查找表

`pg_catalog.pg_table_is_visible(oid)` 根据当前连接的 `search_path` 检查该编号对应的关系。能在路径中优先找到时返回 `true`，路径中没有该模式或前面有同名关系时返回 `false`。编号不存在或为空时返回 SQL `NULL`。查询读取真实目录，识别当前连接的临时表。完整的模式授权检查仍待补齐。

普通查询支持用 `OPERATOR(pg_catalog.~)` 按模式匹配名称，也支持 `~*`、`!~`、`!~*` 及 `COLLATE pg_catalog.default`。正则语法和字符分类沿用 C++ 标准库。PostgreSQL 的完整正则语法和区域设置规则仍待补齐。 [测试记录](test-report.md#psql-catalog-discovery-20261124)

## 2026-11-23 · 在 FROM 中展开数组

受支持的 `FROM unnest` 将单个数组的元素按顺序送入查询，未限定名称的多参数调用按位置展开所有数组。较短的数组补 `NULL`，每列保留元素类型。行号在外层筛选和排序前生成，普通查询与扩展协议使用同一列定义。返回记录的列定义、通过别名取完整多列记录和引用同级其他表列的用法仍有差距。[测试与限制](test-report.md#unnest-from-20261123)

## 2026-11-23 · 并排读取表函数

受支持的 `ROWS FROM` 成员各提供一列。最长的一组决定结果行数，较短的一组按本列类型补 `NULL`。单值函数贡献一行。`WITH ORDINALITY` 在筛选和排序前添加一个从 1 开始的 `bigint` 行号。已覆盖逐列别名、重复列名、普通联接和只读 `WITH`。相关函数来源和返回记录的函数仍有差异。[测试与限制](test-report.md#rows-from-20261123)

## 2026-11-23 · 读取 `generate_series` 的结果

受支持的查询可将 `generate_series` 作为 `FROM` 来源。绑定阶段确定函数重载、列名和类型。函数开始被读取时才求值参数，`WITH ORDINALITY` 增加从 1 开始的 `bigint` 行号。普通查询、扩展协议的列说明及执行使用同一列定义。相关函数来源和写入 CTE 与函数来源的组合仍有差异。[测试与限制](test-report.md#series-source-20261123)

## 2026-11-23 · 写入 CTE 的可读取输出

在受支持的普通语句里，把没有 `RETURNING` 的写入 CTE 当作表读取，会在语句执行前返回 `0A000`。名称检查按查询作用域进行，覆盖同名实体表、内层同名定义、其他 CTE 和集合查询。未被引用的写入仍可执行，并省去创建空临时表的操作。[测试](test-report.md#cte-command-visibility-20261123) · [性能](performance-report-2026-10-30.md#cte-command-visibility-cost-20261123)

## 2026-11-23 · 查询返回零列结果

受支持的普通 `SELECT` 查询可以省略输出列。每条满足条件且进入结果的记录发送一条零字段行消息。规划错误直接返回错误，执行错误保留已完成的空行。`LIMIT`、`OFFSET`、只读 CTE、派生表和简单 `UNION ALL` 均有覆盖。函数来源和零列 `UNION` 去重仍有差异。[测试](test-report.md#zero-width-read-20261123)

## 2026-11-23 · 按需读取只读子查询

符合条件的只读 `WITH` 和 `FROM (SELECT ...)` 通过绑定后的结果列向外层逐行提供数据。适用的内部查询最终读取一张普通表，各层只输出直接列或字面值。已覆盖外层算术、筛选、计数、求和与固定 `LIMIT/OFFSET`。执行过程共享 CTE 结果和事务快照，`EXPLAIN ANALYZE` 执行相同的读取路径。直接由 `VALUES` 提供数据的 CTE、递归 CTE、写入 CTE 及多处引用的 `NOT MATERIALIZED` 适用各自的规则。[测试](test-report.md#logical-read-demand-20261123)

## 2026-11-23 · 普通查询的执行错误

普通 `Query` 消息运行受支持的只读 `SELECT` 时，语句通过绑定和计划创建后，如果求值出错，客户端先收到列说明和执行器已产出的行，再收到错误和事务状态。解析、绑定或计划创建失败时直接返回错误。失败的语句没有成功结束消息，同一请求中排在错误后的语句不会执行。显式事务与保存点沿用原有的回滚规则。[测试](test-report.md#simple-query-runtime-result-20261123)

## 2026-11-23 · 判断计数条件是否可能成立

普通表的计数可用已绑定的同列整数范围、文本等值与不等值、文字长度和空值条件，判断 `AND` 条件能否同时为真。证明使用声明的整数取值范围。多个计数分别合并 `WHERE` 和自己的 `FILTER`，只有所有计数都确定为零时才跳过行读取。数据库在表锁下重新核对关系与字段存储位置，并保留权限检查和读快照。自定义函数、算术表达式或行级权限策略要求的求值沿用正常执行规则。[测试](test-report.md#ordinary-count-constraints-20261123)

## 2026-11-23 · 单表计数

普通非分区表只含受支持的文本和有符号整数列时，裸 `COUNT(*)` 和 `COUNT(列名)` 可以在扫描中累计。多个计数共用一次扫描，各自的 `FILTER` 可使用已支持的空值、文本、文字长度和整数比较。结果为 `bigint`，空结果返回 `0`，SQL NULL 与空字符串分开处理。计数使用语句快照和现有行可见性规则，同样检查读取大字段时发生的错误。

复杂表达式、分组、去重、带排序的聚合及需要执行行级权限策略的表沿用已有路径。普通非去重、无聚合排序的 `COUNT(表达式)` 也会即时累计，表达式和 `FILTER` 仍按正常规则求值。[测试](test-report.md#ordinary-stored-count-20261123) · [性能](performance-report-2026-10-30.md#ordinary-stored-count-cost-20261123)

## 2026-11-23 · 复合主键的前导等值与末列范围

普通非分区表的复合主键末列为有符号 `smallint`、`integer` 或 `bigint` 时，新建或重建索引可以定位一个前导键组内的末列范围。前面的所有列都须给出受支持的等值条件。末列可省略条件，或用 `<`、`<=`、`>`、`>=`、`BETWEEN` 缩小范围。普通语句及受支持的命名语句参数均可使用。原 SQL 条件继续核对实际行，宽范围、旧格式及旧快照可使用原有扫描。跨不同前导键组的索引排序和范围搜索仍有差距。[验证](test-report.md#composite-primary-range-20261123)

只按末列升序或降序排序，且执行实际选择了范围索引时，可以使用索引顺序。受支持的直接列投影和固定 `LIMIT/OFFSET` 可以从区间端点读起，确认完整键、事务可见性和筛选结果后再提前结束。可附加用 `AND` 连接的普通文本和有符号整数列的 `IS NULL` 或 `IS NOT NULL`、按字节的文本等值或不等比较、整数常量比较，以及将内置 `length` 的文本长度与整数常量比较。符合条件的行不足一页时，可分批继续查找。索引页数、候选数、`work_mem` 或读页成本超出限制时，回到原路径取得完整结果。用户自定义函数和可能抛出算术或转换错误的表达式沿用原流程。[分页验证](test-report.md#composite-primary-refill-20261123)

## 2026-11-23 · 提前筛选普通表的文本和整数

普通非分区表只含受支持的文本和有符号整数列时，已绑定的单表查询可在行进入上层执行器前检查受支持的 `AND` 筛选条件。文字长度按字符计数，SQL NULL、空字符串与文字 `NULL` 各自保留含义。旧行格式、需要执行行级策略的表、用户自定义函数和可能报错的表达式沿用普通路径。读表仍按事务快照判断可见性。[筛选验证](test-report.md#ordinary-stored-predicate-20261123)

## 2026-11-23 · 复合主键索引按完整键定位

普通非分区表的 `0xC555` 索引使用完整键的摘要定位候选。即使多条记录共用同一个店铺编号，订单号也能参与定位。如果摘要相同，数据库仍核对各列的真实值。新旧索引按文件格式继续读取、写入。完整主键的等值定位适用下述条件。[索引格式](PACKAGING.md#复合主键索引格式) · [验证](test-report.md#composite-primary-fingerprint-20261123)

## 2026-11-23 · 按完整复合主键修改或删除

普通表的单表 `UPDATE` 和 `DELETE` 可以在事务首次写入时，用完整复合主键的等值条件查找目标。适用的列类型和比较规则与按复合主键查询相同。数据库按语句快照检查可见性、核对完整键，再进入原有的加锁、触发器和约束流程。`0xC552` 复合索引、旧快照及事务内已有写入使用表扫描。[协议测试](test-report.md#composite-primary-mutation-20261123)

## 2026-11-23 · 按完整复合主键查找

普通表的等值查询可以用完整复合主键查找记录。支持受测的文本、变长字符和有符号整数列，要求使用默认或按字节比较的规则、当前快照和完整索引。读取候选后核对各列的实际值，再执行查询的其余条件。使用 `0xC552` 复合索引、旧事务快照或部分键时沿用普通扫描。带别名的部分参数查询及复合键范围查询仍使用原有路径。[测试](test-report.md#composite-primary-lookup-20261123)

## 2026-11-23 · 复合主键按每列的值查重

在普通非分区表上，复合主键的各列都相等时，插入或改键才会因主键重复报 `23505`。同一个索引键可以关联多个记录位置。旧数据目录可直接打开，重建索引后，使用默认比较规则的写入可从索引定位候选。分区表的跨分区约束仍待验证。[索引格式](PACKAGING.md#复合主键索引格式) · [测试范围](test-report.md#composite-primary-keys-20261123)

## 2026-11-23 · 单列文本主键

在普通非分区表上，两个单列文本主键可以有相同的长前缀。按完整值查找、插入、更新、删除或引用这些键时，数据库会区分它们。完全相等的主键仍报 `23505`。旧数据可以直接读取，重建索引后，用索引的等值查询可直接定位候选记录。[索引格式](PACKAGING.md#单列文本主键索引格式) · [测试范围](test-report.md#long-text-primary-keys-20261123)

## 2026-11-22 · 文本键和唯一索引

默认规则下，文本等值比较区分大小写、重音写法和空字符串。`CHAR` 比较忽略尾部空格。在已有重复值的列上建立普通唯一索引会返回 `23505`。[验证](test-report.md#stored-text-equality-20261122)

<a id="ordinary-text-function-filter-contract"></a>
## 2026-11-22 · 按文字的一部分查找记录

普通表可用 `WHERE substring(label FROM 1 FOR 1) = '张'` 按名称的第一个字符查找。`FROM` 和 `FOR` 接受整数表达式，按字符计数。输入为空值时返回 `NULL`，负长度返回 `22011`。

这种关键字写法调用系统函数。用逗号传参时，数据库按当前的 `search_path` 查找函数。可用 `pg_catalog.substring(label, 1, 1)` 明确指定系统函数。[测试范围](test-report.md#ordinary-text-function-filter-20261122)

<a id="pg-settings-expressions-contract"></a>
## 2026-11-22 · 查询当前设置

`pg_catalog.pg_settings` 的 `name`、`setting` 和 `unit` 三列可用于普通查询。已验证 `CASE`、类型转换、数值排序、`DISTINCT`、`COUNT(*)`、列别名、派生表及与用户表连接。没有单位的设置返回 SQL `NULL`，`SET LOCAL` 与保存点回滚会反映在查询结果中。

`SELECT *`、未实现的设置列以及受测的 `GROUP BY` 聚合返回 `0A000`。设置清单及字段仍是 PostgreSQL 18.6 的子集。[测试范围](test-report.md#pg-settings-expressions-20261122)

<a id="schema-rename-contract"></a>
## 2026-11-22 · schema 改名

管理员可重命名空 schema，或仅含受支持的普通表、索引、普通视图和序列的用户 schema。原 schema 编号、表数据、外键、已记录的普通视图引用和受支持的授权随事务保留。序列保留对象编号、已发编号和对应的 `SERIAL` 或 `IDENTITY` 列。新建或替换普通视图时，已解析的序列字面量按对象编号保存。显式 `text` 参数执行时按当前名称查找。

命名冲突或无法确定归属的旧序列引用会阻止改名。含自定义类型、例程或受影响的物化视图时仍会拒绝改名。更改所有者、按 PostgreSQL 的所有者权限模型执行改名、`public` 改名以及类型默认值和例程里的序列引用仍待补齐。[表与视图测试](test-report.md#schema-rename-20261122) · [序列测试](test-report.md#schema-rename-sequences-20261122)

## 2026-11-22 · 表改名或迁移后保留普通视图

普通表改名或移到另一个 schema 时，受测的依赖视图仍读取原表，结果列名和类型保持原样。视图定义和表变更一起提交或恢复。提交后创建同名表，旧视图仍读原表。缺少依赖记录的旧定义、已被同名对象遮住的旧定义和物化视图的相应行为仍需补齐。[验证范围](test-report.md#view-table-rename-20261122)

## 2026-11-22 · 普通视图保留来源表所在的 schema

新建或替换普通视图时，查询定义保存了这次绑定选中的表及视图所在的 schema。查询者更改 `search_path` 或创建同名临时表后，受测的直接查询、连接及导出仍读取原先选中的来源。旧视图中未写明 schema 的引用和视图所有者的完整权限规则仍待补齐。[对照范围](test-report.md#view-source-identity-20261122)

## 2026-11-22 · 查询时连接普通视图

受支持的 SELECT 可以连接普通视图和表，也可以连接两个普通视图。所测的外连接保留空值，结果列的名称和类型在扩展协议准备阶段由绑定结果提供。本项目对这条执行路径检查整个视图及实际读取的基础来源的权限。视图所有者权限的完整 PostgreSQL 行为、更多聚合与特殊 FROM 形式仍需补齐。[对照范围](test-report.md#join-ordinary-views-20261122)

## 2026-11-22 · 普通视图中的列引用随基础表改名

改普通表列名会更新受支持的普通视图定义。输出列名和类型按原目录提供。受测的计算、条件、显式连接、派生查询、相关子查询及层叠视图可继续查询。受影响的 `JOIN USING` 键和因查找顺序变化而指向其他表的定义会导致改名撤销。物化视图、写入视图及更多依赖形式仍需验证和补齐。[对照范围](test-report.md#rename-dependent-views-20261122)

## 2026-11-22 · 改列名时更新保存的表达式

普通表改列名时会更新受支持的 `CHECK`、生成列及已保存的默认表达式中的实际列引用。带双引号、大小写不同及带表名限定的引用按解析后的列名处理。[测试范围](test-report.md#rename-quoted-column-references-20261122)

## 2026-11-22 · 客户端导入中的自定义域约束

文本和 CSV 导入会在读取字段时检查所支持的标量域和数组域。显式值违反 `CHECK` 或 `NOT NULL` 时，可按 `ON_ERROR ignore` 跳过。域检查表达式的运行错误、无效默认值和表约束错误仍会撤销本次导入。历史表结构中缺少来源信息的约束仍按表检查处理。[对照范围](test-report.md#copy-domain-input-20261122)

## 2026-11-22 · 客户端导入的坏行处理

文本和 CSV `COPY … FROM STDIN` 接受 `ON_ERROR stop`、`ON_ERROR ignore`、`REJECT_LIMIT` 和 `LOG_VERBOSITY`。所测的整数和布尔转换错误可跳过，主键、表约束、格式及默认值错误会撤销导入。[用法](MANUAL.md#用客户端复制数据) · [对照范围](test-report.md#copy-ignore-bad-rows-20261122)

## 2026-11-22 · 导入默认值的求值顺序

客户端文本和 CSV 导入按列清单的顺序处理行中的默认值标记。在默认的遇错停止模式中，后续字段出现转换错误或缺列时，已取过的序列编号会保留，导入记录随语句回滚。域约束的检查顺序和部分类型转换的错误码仍有差距。[对照范围](test-report.md#copy-default-evaluation-order-20261122)

## 2026-11-22 · 导入已有编号

普通表的文本和 CSV `COPY … FROM STDIN` 支持给 `GENERATED ALWAYS` 和 `GENERATED BY DEFAULT` 身份列导入显式值，也可以省略这些列或用 `DEFAULT` 标记生成编号。[对照范围](test-report.md#copy-identity-input-20261122)

## 2026-11-22 · COPY 中的默认值标记

CSV 和文本 `COPY … FROM STDIN` 支持 `DEFAULT '标记'`。遇到标记时采用列的当前默认值，列没有默认值时返回 `22P04`。原有的目标表与列限制仍适用。用 `DEFAULT NULL::text` 创建普通文本列时，本项目保留了默认表达式，PostgreSQL 会在所测写法下省略它，使用标记时两者行为有差异。[用法](MANUAL.md#用客户端复制数据) · [对照范围](test-report.md#copy-default-marker-20261122)

## 2026-11-22 · CSV 导入

`COPY … FROM STDIN` 支持 UTF-8 CSV，包括带换行的字段、`HEADER`、`HEADER MATCH`、自定义分隔符与空值、`FORCE_NULL` 和 `FORCE_NOT_NULL`。每次导入沿用已有的列、写入权限和事务限制。二进制及服务器文件的 CSV 选项仍待实现。[用法](MANUAL.md#用客户端复制数据) · [验证范围](test-report.md#copy-csv-in-20261122)

## 2026-11-22 · CSV 导出

`COPY … TO STDOUT` 支持 CSV 和可选列名行，适用于原有的表和查询来源。CSV 导出支持单字节分隔符、引号、转义字符、自定义空值和指定列加引号。默认文本格式也可输出列名。[用法](MANUAL.md#用客户端复制数据) · [验证范围](test-report.md#copy-csv-out-20261122)

## 2026-11-22 · 事务开始时的锁等待

同一进程内有连接正在保存目录时，等待中的 `BEGIN` 或查询可在保存完成后继续。锁等待超过 `lock_timeout` 返回 `55P03`。真实的文件错误仍按原错误报告。[验证范围](test-report.md#begin-catalog-contention-20261122)

## 2026-11-22 · 导出查询结果

客户端可以用 `COPY (查询) TO STDOUT` 导出结果。支持查询执行器已实现的 `SELECT`、`VALUES` 和只读 `WITH`。导出会采用查询的列、筛选和排序，在事务中按实际读取的列检查权限。通过视图导出需要有读取整个视图的权限。向服务器文件导出查询、写入并返回记录的查询、需要锁住结果行的查询以及二进制格式仍待补齐。[用法](MANUAL.md#用客户端复制数据) · [验证范围](test-report.md#copy-query-stdout-20261122)

## 2026-11-22 · COPY 查找同名表

通过客户端导入或导出时，数据库在事务内确定实际表和列。`COPY pg_type` 按连接的 `search_path` 选择系统、用户或临时表。只读事务只可向选中的临时表导入。系统 `pg_type` 支持按列导出已实现的字段，整表、函数引用列和系统表导入仍有差距。[使用手册](MANUAL.md#用客户端复制数据) · [验证范围](test-report.md#copy-relation-selection-20261122)

## 2026-11-22 · 查找同名的 `pg_type`

普通查询及 `INSERT`、`UPDATE`、`DELETE` 和 `MERGE` 选择实际的 `pg_type`。默认查当前连接的临时表，再查系统目录。明确列出的 `pg_temp` 和 `pg_catalog` 按各自位置查找，写出模式名可直接选择表。支持查询系统表的已实现列，写入系统表会返回 `0A000`。部分无效写入的报错时机、参数类型推断、带写入的 `WITH` 结果描述以及其他系统表的名称查找仍有差距。[使用手册](MANUAL.md#查找与系统表同名的-pg_type) · [验证范围](test-report.md#pg-type-search-path-20261122)

## 2026-11-22 · 单独查询连接设置

单独选择一个带 `pg_catalog` 限定的设置函数，且传入文本、布尔值、空值或对应的直接参数时，查询可在另一连接持有改表锁时完成。扩展请求的设置仍在同步时提交，后续语句失败时会撤销。显式事务中的解析和绑定会按 PostgreSQL 规则记录本事务已开始查询。同名用户函数和复合查询走通常的类型和名称解析。[验证范围](test-report.md#session-setting-path-20261122)

## 2026-11-22 · 查询里的连接设置函数

`current_setting` 和 `set_config` 支持 `work_mem`、`lock_timeout`、`statement_timeout`、`deadlock_timeout`，返回与 `SHOW` 一致的文本。查询可以绑定参数，准备阶段不会修改设置。执行失败或回滚时恢复对应的值。`search_path`、其他参数和自定义设置仍待接入这两个函数。更改死锁检查时间需要管理员权限。[使用手册](MANUAL.md#在-sql-表达式里读取和修改设置) · [验证范围](test-report.md#session-setting-functions-20261122)

## 2026-11-22 · 超时设置随事务恢复

`lock_timeout`、`statement_timeout` 和 `deadlock_timeout` 支持当前连接和当前事务两种作用域。提交保留普通 `SET`，回滚撤销它，保存点恢复当时的设置。普通或扩展请求中的失败也会恢复该次事务的设置。显式 `SET` 和连接启动选项可以覆盖服务器默认值。继承服务器默认值的连接会在后续请求中采用重载后的值。`deadlock_timeout` 的独立设置和重置限管理员，参数级授权仍待实现。[使用手册](MANUAL.md#在事务里临时调整超时) · [验证范围](test-report.md#transactional-timeouts-20261122)

## 2026-11-22 · 表锁和数据页锁升级

单表 `LOCK TABLE` 支持 `ACCESS SHARE`、`ACCESS EXCLUSIVE`、省略模式和 `NOWAIT`。等待升级期间，原表锁和页锁继续生效。回滚到保存点可以将升级成功的锁恢复到原模式。跨进程文件锁的转换也会保留原锁直到升级成功。其余 PostgreSQL 表锁模式、复杂的锁目标和完整跨进程等待图仍有差距。[使用手册](MANUAL.md#在事务里锁住一张表) · [验证范围](test-report.md#table-page-lock-upgrade-20261122)

## 2026-11-22 · 共享行锁升级和回滚

普通表和受支持的分区表在等待升级期间保留共享行锁。发生锁超时后回滚到保存点，原锁仍生效。回滚已成功的升级时，可恢复共享模式。同一服务进程内，两个共享持有人同时升级可检测到死锁。跨进程的等待图和更多行锁模式仍有差距。[使用手册](MANUAL.md#查询时锁住分区表记录) · [测试报告](test-report.md#row-lock-upgrade-20261122)

## 2026-11-22 · 分区表查询的共享行锁和分页

符合条件的现有持久分区表支持单表 `FOR SHARE` 和 `FOR UPDATE`。锁定记录使用实际分区身份，同一行可有多个共享锁，写入需等待。锁定截止点遵循受测的 `LIMIT` 和 `OFFSET`，`SKIP LOCKED` 会继续寻找下一条可锁记录。加入行级规则、其他锁强度和复杂查询组合仍有缺口。[使用手册](MANUAL.md#查询时锁住分区表记录) · [验证范围](test-report.md#partition-share-20261122)

## 2026-11-22 · 分区表的 UPDATE 和行移动

符合条件的现有范围、列表和哈希分区支持单表 `UPDATE`、`ONLY` 和 `RETURNING`。修改分区键时可移动记录，并支持保存点和事务回滚。两笔事务更新同一行时，默认隔离级别下可重新检查同一分区中的新记录，遇到跨分区移动则返回 `40001`。分区索引、独立分区关系、使用来源表的更新和适用的行级访问规则仍有差距。[使用手册](MANUAL.md#修改分区表中的记录) · [验证范围](test-report.md#partition-update-20261122)

## 2026-11-22 · 分区表读取和排序

受测的单表查询保留每条匹配的分区记录。`OR` 多次匹配同一条记录时只返回一次，两条记录有同样的值时仍各自返回。空值标记用于筛选、排序和返回结果。单独的分区行锁身份和并行分区扫描仍待验证或补齐。[使用手册](MANUAL.md#15-分区表) · [测试范围](test-report.md#plain-partition-or-20261122)

## 2026-11-22 · 分区表的 INSERT 和回滚

已有分区可以接受父表的普通 `INSERT`，包括多行插入和 `RETURNING`。范围和列表分区在没有匹配项和默认分区时返回 `23514`。列表分区的空分区键可以进入默认分区，哈希分区也可以存储空分区键。

此路径适用于有物理编号且记录写入日志的持久分区表，要求没有主键、唯一约束、外键、排斥约束、可延迟约束或索引。仍有分区写入待提交时，改变文件的表定义操作和 `PREPARE TRANSACTION` 会被拒绝。已经保存用于回滚的全库副本的事务也会拒绝分区插入。独立子表的 SQL 定义仍待实现。[使用手册](MANUAL.md#向分区表写入) · [验证范围](test-report.md#transactional-partition-insert-20261122)

已配好范围、列表或哈希分区的部分持久表支持单表 `DELETE` 和普通 `RETURNING`，包括提交、回滚和保存点。`ONLY` 指定父表自身时返回零行。分区表上的索引、删除触发器、需要执行的行级规则、外键引用及 `USING` 仍有限制。[使用手册](MANUAL.md#删除分区表中的记录) · [验证范围](test-report.md#partition-delete-20261122)

## 2026-11-21 · 按事务结果恢复清空表

新版可读取旧的 `TRUNCATE` 日志。单独执行的 SQL `TRUNCATE` 会在支持物理编号的持久表日志中写入事务号。恢复涉及这种记录的日志时，需要使用本次或更新的程序。[恢复测试](test-report.md#transactional-truncate-wal-20261121)

## 2026-11-20 · 连接和子查询中的继承表

普通表可以作为受测的内连接、左连接、右连接和全连接的数据来源，也可以放进派生表、公共表表达式及读取外层行的标量子查询。每次表引用独立处理 `ONLY`，继承数据按父表列名对应，外连接仍会为没有匹配的行补空值。新增路径适用于具备整表读取权限的普通表。分区、视图组合、递归查询、行权限及扩展协议仍需分别验证。部分协议列描述中的来源表和列编号仍为零。[测试范围](test-report.md#nested-inherited-tables-20261120)

<a id="inherited-table-source"></a>
## 2026-11-20 · 普通父表查询会读取各层子表

建立或移除普通表的继承关系时，父表名可以写成 `schema.table`，也可以使用带引号的标识符。省略 schema 时使用会话的搜索路径。当前继承文件以竖线和换行分隔数据，表名含竖线或换行时会拒绝建立关系。 [跨 schema 验证](test-report.md#qualified-inheritance-20261123)

以一张普通表为来源的查询会按父表的列名和类型读取后代表，每张表只参与一次。所测的列和函数、条件、汇总、排序、分页以及引用外层列的子查询共享这批输入行。`ONLY` 支持带括号或不带括号的写法。分区、临时表、虚拟生成列、行权限、分组、窗口查询和扩展协议不在这组验证范围内。[测试范围](test-report.md#inherited-table-source-20261120)

## 2026-11-20 · 单表查询中的字符串函数参数

普通单表查询中的 `upper`、`lower`、`length` 和 `replace` 接收完整的参数表达式。查询会检查引号内的内容、列的实际来源、类型和所选函数。可以提前确定的 `NULL` 与执行中读到的空值按不同规则处理。所测运行时参数错误进入失败事务状态。这个范围涵盖普通查询消息，完整的继承查询组合和扩展协议仍待验证。[测试范围](test-report.md#scalar-string-arguments-20261120)

## 2026-11-20 · 重启后撤销未提交的普通建表

显式事务在首次写入普通表之前，保存建表会改动的目录文件，并记录要创建的表的物理编号。事务提交或完成回滚后清理记录，仍有未决记录时保留所需的日志。异常退出后，启动先处理可能在同一事务中稍后生成的整库备份，再用本次建表记录还原目录和清理已确认归属的文件。较旧的可执行程序无法处理这类未决记录，升级后的崩溃副本应使用支持该格式的版本恢复。[恢复验证](test-report.md#ordinary-create-durable-undo-20261120)

待撤销的新表位于外部表空间时，启动先检查数据库子目录是否可用。目录缺失会让这次启动失败，并保留撤销记录和表定义。目录恢复后，启动会先同步文件删除，再移除表定义。[外部表空间测试](test-report.md#external-create-recovery-20261120)

删除建表撤销记录时，先在原数据库目录同步清理凭据。删除中断后，启动按凭据核对目录身份和剩余文件，再继续清理。新版也识别仍在固定位置、带有有效旧凭据的记录目录。旧版本留下的随机暂存目录不会被自动清除。[清理测试](test-report.md#create-undo-cleanup-20261120)

## 2026-11-20 · 表清单写入后重新同步目录

表清单已完成文件同步和更名后，如果父目录同步失败，数据库再同步一次父目录。再次失败仍报告写入错误。更名前的错误直接返回。[故障测试](test-report.md#table-list-directory-sync-20261120)

## 2026-11-20 · 建表写入完整定义后再发布

存储引擎完成表定义的写入和同步后，才发布正式的定义文件。目标路径已被占用时，建表保留原路径并返回错误。显式事务中新建普通表的崩溃撤销仍有缺口，包括带保存点的场景。[故障测试与对照](test-report.md#create-schema-publication-20261120)

## 2026-11-20 · 时间点恢复先验证归档

时间点恢复在发布基础备份之前，先合并其选中时间线的归档段并核对重叠字节。将要发布的全部时间线及新导入的日志都参与事务编号上界检查。归档缺失、重叠不符或日志损坏会让作业在替换当前数据库前失败。成功的作业准备好恢复目标，重新启动时回放日志。[恢复测试](test-report.md#pitr-stage-archive-20261120)

## 2026-11-19 · 跨段读取 WAL 与处理遗留事务备份

普通重启会检查 `SWITCH WAL` 产生的零填充，并读到后续有效记录。新写入的记录接到最新的实际记录。旧版产生的跳跃链接在活动日志中，只有指向已验证的首个切段前驱时才能兼容读取。活动日志已回收、且需要判定旧事务备份时，数据库会核对备份里的日志字节与同一时间线的归档。证据不足时停止启动，并保留备份。归档证明要求连续的记录链。指定时间点的恢复使用已有流程，其限制见测试报告。[恢复与兼容性测试](test-report.md#wal-switch-terminal-recovery-20261119)

## 2026-11-19 · 自动提交建表先做检查

数据库从检查表名开始就持有原有的独占数据库锁。自动提交的普通建表遇到已存在的表或能提前发现的无效定义时，直接返回相应提示或错误。需要创建新表时，首次写入表文件或新建自增序列之前会保存事务备份。[测试范围](test-report.md#create-validate-before-backup-20261119)

## 2026-11-19 · 恢复表结构时保留序列值

普通启动恢复识别到可用的事务备份时，撤销未提交的结构变更并保留仍存在的序列当前的分配状态。备份恢复失败会阻止服务启动。显式物理恢复和指定时间点的恢复仍沿用各自的备份状态。旧版本生成的崩溃副本如果缺少建表撤销记录，仍可能残留未提交的表。[详细验证](test-report.md#ddl-image-sequence-recovery-20261119)

## 2026-11-19 · 崩溃恢复中的临时表

启动恢复会验证页镜像记录的格式，再用创建日志核对物理关系编号和临时表的会话归属。创建记录已归档时，会按需沿同一时间线验证归档日志。归属一致且没有持久关系编号冲突时，跳过失效会话的临时页，随后清理临时文件和目录记录。同一会话内重命名的临时表也适用。缺少可信的创建记录时，仍会按身份检查的结果停止启动。[恢复与保护性失败测试](test-report.md#temporary-table-crash-recovery-20261119)

## 2026-11-19 · 建表时保存填充比例

`CREATE TABLE ... WITH (FILLFACTOR = ...)` 保存普通堆表的填充比例，供后续插入和重启后的写入读取。`CREATE TABLE` 和 `ALTER TABLE ... SET` 按 10 至 100 校验，重复或非法的 `FILLFACTOR` 返回 `22023`。`IF NOT EXISTS` 跳过已有表时保留它的设置。默认模式下分区父表不能设置此项。项目把有效值保存为规范整数，`pg_class.reloptions` 的显示仍待实现。自动清理仍使用实例级配置。[使用手册](MANUAL.md#建表) · [验证范围](test-report.md#create-table-fillfactor-20261119)

## 2026-11-19 · 插入时可以跳过忙页

`INSERT` 为已有页选位置时，最多尝试两个空闲空间提示和最后一页，尝试获取页锁时不等待。在实际写入的表或分区中核对页容量和 `fillfactor`，这些位置均不可用时，仍按顺序查找和扩页。新的备选页读取失败时继续查找，原有首个提示或顺序扫描上的读取错误、写入开始前的日志错误仍会报告。`ctid` 反映实际写入位置。[测试范围](test-report.md#insert-page-choice-20261119)

## 2026-11-19 · 更新优先复用旧记录所在页

事务性 `UPDATE` 优先尝试原页，再尝试空闲空间映射给出的页。两次都是非等待尝试，只有持有独占页锁且页结构和实际空间检查通过才写入。空间记录未知、过期或候选页忙时，回到原来的逐页扫描和扩页逻辑。索引维护、日志恢复和旧版本可见性继续生效。访问路径只检查实际读取的数据页，离线校验仍检查整份文件。[测试范围](test-report.md#update-version-placement-20261119)

## 2026-11-19 · 更新和删除遇到其他事务写入同一行

隐式 SQL 事务从默认的 `READ COMMITTED` 开始。普通表的单目标 `UPDATE` 和 `DELETE` 等到行锁后检查提交状态。先前写入回滚时可沿用原行。对已提交的更新，`READ COMMITTED` 和 `READ UNCOMMITTED` 可找到最新版本，在该版本上重新检查原有条件。已删除的行或条件不再成立的行会跳过。`REPEATABLE READ` 和 `SERIALIZABLE` 遇到其他事务已经提交的改动时返回 `40001`。

当前支持范围包括受测的普通写入、无来源表的 `WITH UPDATE` 和扩展协议执行。常用的 `UPDATE ... FROM` 和 `DELETE ... USING` 在重查时会保留最初匹配的来源记录，用最新目标值重新执行条件、赋值和 `RETURNING`。旧的按行值缓存的执行路径遇到这种冲突时仍返回 `40001`。触发器重新写入同一行、分区行移动和更细的行锁级别仍需验证或实现。[使用手册](MANUAL.md#同时修改同一行) · [带来源表的验证](test-report.md#joined-row-writes-20261119)

## 2026-11-19 · 按整数主键更新和删除

单列 `smallint`、`integer` 或 `bigint` 主键的简单整数等值条件可用新版主键索引定位目标。事务必须使用 `READ COMMITTED` 或 `REPEATABLE READ`，且尚无其他写入，快照仍可使用当前索引。索引命中后验证实际行、主键和语句快照。命令接着走现有的行锁、约束、索引维护和事务流程。索引未命中、读页失败或快照变化时继续使用表扫描。[使用手册](MANUAL.md#按整数主键更新或删除)

带来源表的常用 `UPDATE ... FROM` 和 `DELETE ... USING` 可以检查位于 `WHERE` 开头的同类型整数主键等值条件。索引未命中或事务不能使用当前索引时，候选查找和来源计算沿用原方式。命中后也会检查完整的来源匹配，遇到并发更新时重新检查原条件。目标有虚拟生成列时继续用原方式查找。[锁页与并发验证](test-report.md#joined-primary-key-target-20261119)

## 2026-11-19 · 分区表统计进度和并行清理参数

`ANALYZE` 会报告父表的分区采样阶段、要扫描和已完成的实际分区数，以及当前分区的页数。分区文件由父表管理，当前分区编号为 `0`。单独更新每个子表的统计仍待实现。`VACUUM FULL` 在修改文件前拒绝正数 `PARALLEL`，允许串行的 `PARALLEL 0`。普通清理支持 0 到 1,024 的并行工作数。[使用手册](MANUAL.md#查看表维护进度) · [验证范围](test-report.md#partition-analyze-vacuum-parallel-20261119)

## 2026-11-19 · 重写时更快找到可写的页

`VACUUM FULL` 重写主表时按各页真实空位选择最靠前的可写页。每行在写入前确定页号和槽号。现有表和索引格式可继续使用。原文件的替换和恢复沿用原有事务流程。[使用手册](MANUAL.md#分开清理主表和长值)

## 2026-11-19 · 查看表维护进度

`VACUUM`、`ANALYZE` 和 `VACUUM FULL` 分别使用 `pg_stat_progress_vacuum`、`pg_stat_progress_analyze` 和 `pg_stat_progress_cluster`。三个视图支持单表投影、`SELECT *`、简单筛选和分页。详情可以由发起角色、有其权限的角色、管理员或 `pg_read_all_stats` 读取。其他角色看到进程号和数据库名。数据库编号及无法确认的表编号以 SQL `NULL` 表示。[使用示例](MANUAL.md#查看表维护进度)

## 2026-11-19 · 只更新指定列的统计

`ANALYZE` 和 `VACUUM (ANALYZE)` 可按表指定要更新的列。未选列保留原有统计和采样行数。已有的统计能在 `VACUUM FULL` 完成或恢复后继续使用。带引号的列名按原拼写查找，统计的类型覆盖沿用现有收集器。项目的联合统计命令仍可使用。[使用手册](MANUAL.md#只更新指定列的统计) · [验证范围](test-report.md#vacuum-target-columns-20261119)

## 2026-11-19 · 默认重写同时整理主表和长值

`VACUUM FULL` 在一次事务中替换主表、已有的长值文件及相应索引，保留仍在使用的长值编号。两组文件及其加密配套文件使用同一份恢复记录。限定的写入故障和重启测试验证了整组恢复。跨越该重写的时间点恢复仍待验证。[使用手册](MANUAL.md#分开清理主表和长值) · [恢复测试](test-report.md#vacuum-combined-full-20261119)

## 2026-11-18 · 选择普通清理的对象

普通 `VACUUM` 可用 `PROCESS_MAIN FALSE` 跳过主表清理，也可用 `PROCESS_TOAST FALSE` 跳过长值清理。多个同名选项采用最后一个有效值。`ANALYZE` 独立于两个清理选项。只做普通长值清理时，如果当前库内有活跃事务，会延后回收。

`VACUUM (FULL, PROCESS_MAIN FALSE)` 重写长值文件和对应的索引，保留主表文件、主表索引和长值编号。在替换文件前保留原文件副本，同一次提交决定整组文件的结果。[使用手册](MANUAL.md#分开清理主表和长值) · [验证范围](test-report.md#vacuum-toast-full-20261118)

## 2026-11-18 · 普通清理的长值处理

`VACUUM` 在没有长值文件，或有效长值堆只有文件头时，跳过长值回收阶段的主表扫描。已有长值仍按原有快照和文件检查规则回收。[使用手册](MANUAL.md#清理表占用的空间) · [验证范围](test-report.md#vacuum-empty-toast-20261118)

## 2026-11-18 · BRIN 在表重写后的可用性

`VACUUM FULL` 完成时，BRIN 摘要包含当前表标识和磁盘上的当前表定义。原有范围宽度与文件格式可以继续使用。回滚恢复旧表和旧索引，读取方仍会校验表标识、范围、文件校验和和快照。[验证范围](test-report.md#brin-vacuum-full-generation-20261118)

## 2026-11-18 · 写入后的索引维护

写入和部分表重写一次遍历目录来发现全文、GIN、GiST、SP-GiST 和 BRIN 索引。原有索引文件、本地表空间布局及 BRIN 范围宽度可以继续使用。需要维护这些索引的表在修改数据前会写入失效标记。提交、回滚和恢复沿用各自的原有流程。[测试范围](test-report.md#specialized-index-discovery-20261118)

## 2026-11-18 · 整数主键分页

默认表空间中无过滤条件的普通单表查询，可按单列 `smallint`、`integer` 或 `bigint` 主键加常量 `LIMIT` 和 `OFFSET` 读取分页。主键按数值升降序排列，仍会计算 `OFFSET` 跳过行的输出表达式。索引能用于当前快照且预算允许时，只读取需要的表行。新格式索引可以沿指定方向取够就停，原有十进制索引仍遍历所有键。其余查询由已有计划处理。[实际测试](test-report.md#signed-primary-key-prefix-20261118) · [索引格式](PACKAGING.md#整数主键索引格式)

新格式索引也支持用同一个主键与整数字面量、非空的有符号整数参数或已确认有效的内置整数常量转换作 `<`、`<=`、`>` 或 `>=` 比较。比较可反写，也可用 `AND` 取交集。空参数、小数及含列引用或函数的转换采用原查询计划。[范围与参数测试](test-report.md#prepared-integer-primary-key-range-20261118)

由普通有符号整数、布尔和文本列组成的表，可以在按主键分页时直接筛选列的 `IS NULL` 或 `IS NOT NULL`，也可用 `AND` 与上述主键范围组合。`OFFSET` 只计算符合条件的记录。生成列、域、数组及表达式筛选沿用原查询计划。[空值筛选测试](test-report.md#integer-primary-key-null-filter-20261118)

实际打开的整数主键页与所需排序列和方向一致时，直接保留输入顺序。需要临时文件时按该顺序写入并读出。表达式仍按原有顺序计算，回退到表扫描时仍按查询要求排序。[排序测试](test-report.md#presorted-integer-primary-key-page-20261118)

## 2026-11-18 · 排序查询的结果求值

支持的普通查询按基表的单列整数主键排序时，在实际取行时计算非排序输出。带 `LIMIT` 的普通堆表排序也可推迟识别出的高成本内置整数数组与文本转换。排序键、对应输出别名及序号仍在排序前计算。`OFFSET` 会求值它跳过的行。PostgreSQL 所选计划还受索引、数据量和代价等因素影响。[实际测试](test-report.md#ordered-projection-20261118)

## 2026-11-18 · 普通嵌套查询写入

普通 `INSERT … SELECT` 接受所测的含联接 `FROM` 子查询，包括多层子查询和联接的一侧来自子查询的情形。数组值保留实际类型和下标，同域空值可原样复制，转入其他域时按目标规则检查。`LIMIT` 取得所需行后结束，`OFFSET` 会读取它跳过的行。排序等必须先消耗输入的操作仍按其计划执行。若读取需要的行时报错，整条写入不提交任何一行。[实际测试](test-report.md#nested-insert-20261118)

## 2026-11-18 · 整数数组联接与带域类型的空值

使用 `smallint[]`、`integer[]` 或 `bigint[]` 作为全部 `USING` 联接键时，已绑定的直接列可在合并两行前排除明显不相等的键。所测重复行、数组下界和形状、两种 NULL、外联接补行和逐步读取行为与原路径一致。

如果右侧已经读完且需要处理下一条左侧输入，普通查询和普通查询写入可在当前会话的 `work_mem` 内建立一张临时查找表。每个候选仍按数组值比较，重复匹配仍逐条返回。需要更多内存时继续逐行查找。`work_mem` 在这里限制新增的查找表，联接此前保存的右侧行和比较摘要另外占用内存。[实际测试](test-report.md#array-lookup-20261118)

所测普通 `INSERT … SELECT` 可以把外联接补出的空值写回同一个数组域，即使域本身声明 `NOT NULL`。目标列声明 `NOT NULL` 时会返回 `23502`。没有同域身份的空值仍需执行域检查。[空值测试](test-report.md#integer-array-join-20261118) · [嵌套查询写入](test-report.md#nested-insert-20261118)

## 2026-11-18 · 数组列的联接

所测 `JOIN … USING` 和 `NATURAL JOIN` 保留已绑定的数组类型。相同域的两列合并后仍是原域，不同域降为共同的普通数组。普通数组的长度限制一致时保留该限制。`RIGHT JOIN` 的合并值来自右侧，`FULL JOIN` 取有值的一侧。只有未经转换、直接来自原列的合并值才报告原表和列编号。不同基础数组类型在绑定阶段返回 `42883`，包括 `LIMIT 0`。

已经检查过的同域值通过普通 `INSERT … SELECT` 复制时复用检查结果，混合域的结果写入目标域时重新检查。部分 `JOIN ON` 显式转换仍有缺口。[实际测试](test-report.md#array-using-20261118) · [外联接空值](test-report.md#integer-array-join-20261118) · [嵌套查询写入](test-report.md#nested-insert-20261118)

## 2026-11-18 · 派生表数组

所测普通派生查询和 `INSERT … SELECT` 保留数组的实际类型、维度、下界、NULL 和引用编号。结果直接来自原表字段时保留该字段的来源。相同数组域的直接引用保持域身份。普通数组与域值通过 `UNION ALL` 或 `VALUES` 合并时按共同类型处理，写入域列会执行域检查。

普通派生查询可按所测数组类型排序、去重并返回与边界行并列的结果。`LIMIT 0`、假条件和空来源保留列描述。非法字面量在绑定时报错，执行时的转换错误带有列描述。视图写入和可展开派生表达式的域检查次数仍有差距。[实际测试](test-report.md#derived-arrays-20261118)

## 2026-11-18 · 数组域

`CREATE DOMAIN ... AS integer[]` 等已实现的内置数组声明保留独立的域类型编号。元素转换后执行整个数组的约束。普通表可使用所测域的继承、默认值、显式转换及增改操作。直接赋值或选中分支完整保留已检查的同域值时复用检查结果。混用域与普通数组的条件表达式以及切片会产生普通数组，写入域列时执行域检查。

`CREATE DOMAIN` 和 `ALTER DOMAIN ... SET DEFAULT` 会检查普通数组字符串的格式和元素声明。默认值中的函数与域规则仍在实际写入时执行。已验证的修改可随事务和保存点回滚。

非空表添加数组域列、把现有列改成或改出数组域、修改域的规则及重命名目前会返回 `0A000`。所测先前登记为标量的错误数组域在使用时会返回 `XX001`。项目保留原目录，需要单独迁移此类旧声明。派生表类型、数组元素域、已有域类型的 NULL、触发器时序和长数组仍有差距。[测试与限制](test-report.md#array-domains-20261118)

## P42 当前事务提交补充（2026-11-07）

无写入事务仍保留内部txid、快照、SSI和锁生命周期；同一物理WAL owner位置未变且所有保守条件满足时，不为未被持久变更引用的保留ID写COMMIT或CLOG。不能据READ ONLY或SQL前缀省略写事务同步。目录失效与序列写入单独标记，UNLOGGED DDL／TEMP提交动作及检测到共享WAL推进时保留原路径。没有引入异步写入提交，没有改变磁盘／协议编码或校验。

新95条SQL与旧版回复逐字节一致，完整数据与实际崩溃恢复通过；原PG描述符来源差异仍可见，未声称整体PG等价。仍C++17／O2，根目录默认程序未提升。参见[最新边界、失败和性能证据](performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)；原273和既有暂缓保持，Goal active。

## 2026-11-17：变长字段目录损坏

读取非 NULL 变长字段时，目录不完整、越界、重叠或不符合现有连续布局会返回 `XX001`，不再成功返回空字符串或其他字段字节。检查完整的行内目录，但不为检查而读取未选中的外部大字段。固定字段与由位图确定的 NULL 保留各自读取路径。实现仍使用项目原有磁盘与临时行格式，不与 PostgreSQL 数据目录互通；结构合法的内容替换及其他损坏类型并未全部验证。[测试与限制](test-report.md#variable-row-directory-20261117)。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## VARCHAR逻辑长度与新旧元数据边界

新增VARCHAR逻辑字符长度元数据：显式长度支持1..10485760字符，未指定长度使用独立的无界标记，名义物理dsize仍不超过65535。列声明、pg_attribute／RowDescription、普通写入／更新、数组元素、domain／generated赋值、ALTER及项目逻辑导出使用真实长度。标量不再把65535字节误当成字符上限；已验证70000字符／245000字节UTF8和1MiB无界值。内部无界字节上限为1073741819，但没有实际验收1GiB值或整个PG字符类型族。CHAR旧限制、完整encoding／collation语义仍开放。

旧typeMod=-1按原dsize字符上限读取，不从65535猜测成无界；新显式正长度和私有-2保存在已有ITM1扩展，-2输出PG时为-1。旧数据可以由新版读取；旧程序没有识别这些新VARCHAR修饰符，不能承诺降级读取新写的schema。旧无界意图缺少可判定来源时不自动迁移，须明确新DDL／ALTER。名义dsize、SQL字符数和实际UTF8字节数分别处理。没有改变元组布局、校验、TOAST指针格式或WAL格式；仍是项目自定义存储，不是PG物理格式等价。

新141条VARCHAR SQL在独立PG18.6、旧bf9、候选C6均完整执行，三角色共423条核心SQL、305行／1249单元／132 NULL、228,538,937字节；10个正常Main0及2个预期SIGKILL -9。全值、顺序、重复数、NULL、状态／tag／Ready和全部七个描述字段与实际PG核对，关系OID只用各自真实探针映射。C6普通数据要求通过，仍有17个pg_class来源描述差异，以及3个大常量ADD DEFAULT的PG实现限制差异：PG对70000字节常量报54000，随后读取／DROP列报42703，项目接受并正确存取；这些差异保持开放，strict PG=false。137条V1参考模型的4个假设被真实PG否定，首次actual1及全部原始数据保留；141条V2在任何候选执行前冻结，另加4097字节默认值成功控制，没有按候选结果降低要求。

相关48个原有正式测试按当前要求：46个actual0；toast_test仍在604行 !corruptScan.open() 断言actual -6、后续NOTRUN；declared_type_cold_binding_test旧bf9与C6都actual1，12项中2个初始catalog条件失败，保留OPEN。全套未运行，full suite=false。原type_registry_test把65536声明当错误，旧源码在C6实际-6的首次观察保留；依据已冻结PG声明范围，扩充为65536／10485760成功及10485761／非法修饰符拒绝，更新源码在旧版-6、C6为0。没有删除负向或改掉其余要求。加上两个新测试，共50个不同正式测试，当前48个0和上述两个失败；另外的历史原断言失败单列保存。旧26项对照来自相同bf9正常核心／源码的P40实测，另外22项旧版在本批真实重跑；70个原源码终态及全库文件／模式独立核对。

所有正常窗口关闭后，项目DUMP扩展补测15条完整SQL、3个正常Main0：有界／无界VARCHAR及两类数组的真实逻辑导出，在全新数据库恢复并再次冷开，全值／NULL／全部七个描述字段正确。只验收这组类型声明与值，不声称项目DUMP已完整保留所有PG对象／约束。

[完整证据与保留差异](performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。不关闭整个类型或存储族，用户暂缓范围不变。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 当前FULL刷盘、备份与恢复边界

普通VACUUM FULL在备份前与重建后，只刷新目标逻辑关系及其分区／TOAST缓存，避免把其他表的脏页一并刷出。物理文件已同步，且在任何重建写入前全部unlink：这些文件优先用硬链接保留旧inode作为回滚映像，失败则完整复制；schema和specialized-dirty元数据继续独立复制。每份备份的fsync、完整大小／模式／SHA256校验，以及manifest、pending、替换文件／schema、WAL、COMMIT和undo的持久化顺序全部保留。本批只改TableManage.cpp的vacuumFull，没有修改全局缓存、index_file、WAL格式、CRC或恢复授权规则。

新增vacuum_full_relation_snapshot_test，以完全相同源码链接旧c8ca与正常C1核心。两个模式各有256行目标表／删除后16行、12行无关表、真实脏缓存、主键／二级／hash／bloom索引、70003字节以上TEXT、UTF8、空串／字面NULL／SQL NULL。旧版父actual1：6个优化断言失败，两次setup为1、四次冷开均0；所有热态／冷态完整值和RID NULL位图已正确，不是旧版数据损坏复现。C1父actual0且6子阶段全部0：成功模式实际10份同inode／nlink≥2物理备份；故障钩子对10次硬链接实际返回EXDEV，随后完整复制回退并通过热态及两次新进程冷开。无关堆同步2→0，无关脏页保留给自身owner，之后自身flush正常完成。所有拦截实际调用真实系统同步，未跳过fsync；EXDEV是显式故障控制，不冒充真实跨文件系统部署验证。

原22、62、72、28条SQL及实际wide OID控制全部重新执行，共185完整请求／2588行／11613单元／969NULL／45,817,233请求回复字节，8个正常Main0和1个预期SIGKILL -9，2315个闭库文件核对。全部完整请求与回复逐字节等于P39真实c8ca观察，再由独立解码器检查完整值／NULL／全部T7／错误／NOTICE／tag／Ready。原PG18.6参照逐字节复用，未声称本批新跑PG；62条中的9项、72条中的7项、28条中的3项SQL子分区差异继续开放，strict PG=false。

六种FULL专属有限故障控制在C1均首次尝试通过：schema发布前强杀、发布后提交前强杀、COMMIT后清理前强杀、发布报错后回滚schema期间再次强杀、提交后清理失败、回滚后清理失败。共120条完整SQL与4条真实中断FULL、10400行／48966单元／3696NULL／64,393,755字节，4个预期SIGKILL -9、8个正常Main0，6285个实际映像文件，一个预期XX000。6087条／47,972,984字节WAL独立核对CRC／prev链、COMMIT／ABORT／未提交边界和代次不可复用；两种清理失败后的后续DML与显式CHECKPOINT顺序及冷开全值正确。使用P39已接受V2钩子的同一字节，仅初开进程插桩，冷开正常C1；P39原V1漏拦截remove的失败属于历史保留，不是本批重试，未覆盖原记录。

本批仍只验收未来普通关系FULL的有限重写／关闭／强杀／回滚恢复。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际损坏库继续保留。跨FULL的PITR／物理基备未验收；数据库排他锁范围仍大于PG表级锁，未验收分区FULL冷开或高并发等价。硬链接需要文件系统支持，实际失败会走复制回退；不宣称所有平台／文件系统已验证。原15项安全／TDE及额外用户暂缓范围保持，这六种FULL故障不是已暂缓的通用IO／分配器／保存点矩阵，也未恢复执行图／child-provider或retained planning／portal工作。

正式测试库存2402个文件（1135顶层／1140总C++），2401个旧测试逐字节不变；注册E2E684／独立E2E664不变。250个生产文件只改TableManage.cpp，正常CMake C++17/O2 C1为bf9ad7ae，旧c8ca0215、更早84bbadb4；61核心对象冻结。默认425165af不变。754个Markdown路径、725份保护记录保持，只在已有8文档插入说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变；289条历史保持，新增partial第290条；IDX-07仍unverified/evidence[]。Goal active，直接master提交，未推送或替换默认程序。下方各批是各自历史程序的记录。 [完整报告](performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 当前FULL恢复能力与边界

修复普通VACUUM FULL仍沿用旧物理身份、冷启动把旧页镜像重放到压缩后堆的问题。每次重写分配新物理代次；同一内部事务记录出生、旧代次退休意图／完成及COMMIT。替换前先持久化只属于该关系的原文件、完整模式／大小／SHA256清单及pending记录，替换文件与schema全部落盘后才提交。失败和未提交崩溃先恢复旧文件／schema，再重放WAL；已提交只清理备份。完成回滚的durable receipt防止遗留备份覆盖后续已提交DML。旧09 schema仅在FULL成功时发布新代次及专门的0x23完成记录，普通旧名称重用仍拒绝错误归属。没有放宽CRC、物理身份或页缺口校验。

原先因冷开退出-6中断的同一旧库22条SQL计划，现在C1完整执行22条／2130行／9813单元／738NULL／12,433,869请求回复字节，两个正常Main实际0，944个闭库文件核对。初开16条完整请求和回复与P38旧84bb原观察逐字节一致，新增完成原先未运行的6条冷开。原始旧库种子334个名字／模式／字节不变；P38原失败、被旧观察器遮蔽的旧终态及缺失阶段保持原记录。

六种FULL专属有限故障控制全部经独立完整值／NULL／全部T7／错误／tag／Ready核对：schema发布前强杀、发布后提交前强杀、COMMIT后清理前强杀、发布报错后回滚schema期间再次强杀、提交后清理失败、回滚后清理失败。共120条完整SQL、4条确实发出后被中断的FULL、10400行／48966单元／3696NULL／64,393,755字节，4个预期SIGKILL -9和8个正常Main0，6285个实际映像文件；一个预期XX000保留。6087条WAL／47,972,984字节独立CRC与prev链核对，并核实各自COMMIT／ABORT／未提交及不可复用代次。两种清理失败后继续UPDATE，再显式CHECKPOINT，检查点确实晚于该DML提交；冷开值未被旧备份覆盖。只在初开Main加载版本化syscall钩子，冷开使用未插桩的同一正常C1。

原62、72、28条SQL及一条实际wide OID控制在本批C1重新执行，共163请求、458行／1800单元／231NULL／33,383,364字节，6个正常Main0和1个预期SIGKILL -9。所有完整请求与回复均与P38实际84bb逐字节一致，独立第二解码器核对全部值、NULL、T7、错误、NOTICE、tag和Ready；复用的是已认证归档中的真实PG18.6参照，没有本批新跑PG的声明。原62的9项差异、72的7项差异及28的3项SQL子分区差异继续保留，strict PG=false。

本批验收只覆盖未来普通关系FULL的有限重写／关闭／强杀／回滚恢复情形。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际失败库仍保留。跨FULL的PITR／物理基备重建未验收，不能仅凭新生命周期记录重建重写后的完整堆。内部事务暂时使用数据库排他锁，再取表锁和cache锁，并非PG表级并发等价；热态分区既有回归通过，不宣称分区冷开闭环。原始15项安全／TDE暂缓与额外用户暂缓记录保持；这六项不是已暂缓的通用IO／分配器／保存点故障矩阵，也未修改执行图／child-provider、retained planning／portal或CPU-sort interrupt。

正式库存2401个测试文件（1134顶层／1139总C++），2399个旧测试逐字节不变，注册E2E684／独立E2E664不变。源码250个文件，正常CMake C++17/O2候选C1为c8ca0215，旧84bbadb4、更早56aad0c0；默认425165af不变。754个Markdown路径和725份保护记录保持，只在8份已有文档插入本批说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有288条历史保持、新增partial第289条；IDX-07仍unverified/evidence[]。Goal active，直接master分批提交，未推送或替换默认程序。下方各批均为对应历史程序的记录。 [完整报告](performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：标量TEXT长值及实际兼容边界

修复标量TEXT把持久化名义宽度65535误当逻辑值上限的问题：统一的字符字节限制函数对非数组TEXT使用1,073,741,819字节上限，并让旧式UPDATE校验也使用该函数。既有TOAST读回、插入和行构造因此一致处理完整长值；表结构dsize仍为65535，内联引用和存储格式不变。实际验证到1MiB，未进行接近1GiB的端到端容量验证。旧数据可由新程序读取；旧程序不能读取新写入的超64KiB TEXT，不能据此承诺降级后的值兼容。

新增72条SQL分别在真实PG18.6、旧版56aad0c0和当前程序执行：216条核心请求、507行、1935单元、80NULL、73,199,459请求／回复字节，7个Main正常0、2个主动SIGKILL终态-9。独立解码器核对完整值、NULL、全部T7、tag、Ready、错误及NOTICE，当前目标TEXT用例通过；严格PG比较仍有7项差异：4项系统目录列来源OID元数据，以及大声明／无界／多字节大VARCHAR的3项差异。初轮强杀前已删除1MiB行，其最大存活TEXT为200006字节；该组1MiB重写后的冷开来自正常关闭，不把它计作1MiB崩溃恢复证据。

实际旧版30412452创建、正常闭库并冷开验证的after-DELETE种子，334个名字／模式／字节在本批前后完全一致，固定／单长／多长字段人口241／241／113。另做不含FULL的18条普通维护控制，每角色18条，共36请求／3070行／14670单元／1110NULL／16,337,172字节，4个Main正常0；当前在旧模式中写入70003字节及1MiB TEXT后，普通VACUUM和新Main冷开全值／T7通过，未改动固定／单长字段完整wire。再以相同原始种子做独立18条崩溃控制，共36请求及相同全值工作量，2个主动SIGKILL -9、2个新Main正常0，确认提交后完整1MiB TEXT存活并读回。两项控制各有1600个闭库／崩溃映像文件核对；它们不替代下面失败的原始FULL测试。

原62条SQL重新用旧版和当前执行，共124请求、4个Main正常0，完整请求与回复逐字节一致，真实PG18.6原始oracle和既有9项差异保持。P37原28条维护SQL计划字节不变，另在真实PG／旧版／最终C2重新执行，共84核心请求、144行、553单元、129NULL、2,873,498字节、6个Main正常0；额外4条实际OID／PG版本控制单列。原TEXT70003插入、立即读回、冷开读回3个确切缺口现与PG完整值和T7相符，其余25条旧／新完整回复字节不变。28条中24项值／状态／tag匹配、1项按各库真实正OID核对，仍有SQL子分区创建0A000、子表目录身份及冷开身份缺行3项差异，strict PG=false。

新发现的普通VACUUM FULL／WAL恢复缺陷仍OPEN，属于下一项必须处理的非暂缓工作。原始旧模式22条矩阵，两版各完成初开16条并正常退出0；当前16条全值正确，但之后冷启动失败，两版各6条冷开SQL未运行。旧版首次观察器的cleanup断言遮蔽了已退出子进程的wait状态，因此其冷开actual exit未记录；修正观察器后只继续原先未启动的当前角色，实际冷开退出-6，未重跑或补写旧版状态。提前运行的独立审计也因缺少完整receipt实际退出1，失败保留。独立CRC32C核对定位LSN6784528的有效8192字节页镜像：cv_sig、物理身份3、旧块22；FULL后同身份的堆仅18页，重放拒绝跨越4页的缺口。CRC为0xd116b066，名义行宽0是合法format2而非本次原因。本批未修改恢复代码；需要正确的重写物理代次／恢复边界，不能靠忽略错误或无界补页关闭。实际失败快照和正常旧模式混合长值种子均保留在本地。

10项相关原生测试在旧版与当前均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test、vacuum_toast_physical_reference_test、alter_column_toast_generation_test。旧版前9项使用既有相同源码／对象／ELF的真实终态，第10项重新执行；当前10项均本批执行。既有toast_test两版仍在第604行 `!corruptScan.open()` 断言中止（actual -6），后续检查NOTRUN，未改预期，full suite=false。

strict PG=false，没有关闭整个功能族。[完整报告](performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：物理TOAST引用不依赖调用者NULL绑定

当调用者仍绑定一次真实 TableScan 的正 RID，特别是另一个数据库中同名表的 NULL 行时，旧收集器可能漏掉仍在使用的 TOAST 引用并回收这些对象；新代码只使用正在检查的物理元组。该触发由原生维护接口复现，未将正常 SQL 对照中的旧／新相同结果伪称为 SQL 数据丢失复现。

修复普通 VACUUM 的长字段引用收集：在原有页锁和 pin 内，直接读取所属物理元组的 NULL 位图、变长字段目录和有界字节跨度，不再通过可能读取调用者 RID/NULL 状态的执行器取值接口。固定区大小与变长列索引每次维护预先计算，普通内联值无需复制整行或逐值构造字符串。活动快照保护、所有物理存活槽的遍历、锁／pin、索引／分块校验和回收策略保持。

原62条SQL在旧版30412452和C1全部执行，4个初开／冷开Main正常0，完整请求／回复逐字节相同，沿用并核对真实PostgreSQL18.6原始oracle，既有9条协议差异仍在。另以真实PG18.6、旧版和C1各执行28条新维护SQL及冷开读取，共84请求／142行／549单元／129NULL／2733516字节，6个Main正常0；第二套解码器核对全值、NULL、完整T7、tag、Ready和原始错误／NOTICE。28条中旧／新完整wire一致；21项值／状态／tag与PG相符，1项用各库真实正OID分别核对，另6项PG差异保留，strict PG=false。

本批实际PG缺口包括：TEXT70003字符INSERT在产品返回22023而PG成功，立即读取与冷开读取缺该行；SQL `CREATE TABLE ... PARTITION OF` 返回0A000，子表目录身份及冷开身份读取缺行。原生分区物理存储控制不代表这项SQL功能已实现，也不代表逻辑分区扫描已通过。

8项相关原生测试在旧版和C1均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test。其中旧版后3项复用相同源码／冻结对象／ELF的既有正常终态，其余均本批实际执行。既有toast_test在旧版与C1仍于第604行 `!corruptScan.open()` 断言中止（actual -6），保留失败，后续断言未运行；未修改测试预期，full suite=false。

未扩大PG兼容承诺；[真实正负开销与边界](performance-report-2026-10-30.md#vacuum-physical-references-20261107)。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：当前物理行 NULL 状态的局部复用

扫描展开长字段时，借用该物理行已捕获的 NULL 位图；排序后的投影在核对真实来源、具体 Sort 类型和全部物理列声明后，借用排序已保存的位图。借用仅存在于一次取行调用内，退出与异常均恢复原绑定；表结构不匹配、Sort 子类及其他来源继续原路径。没有新增页／allocator 缓存或改变文件身份检查本身。

选中的虚拟列仍读取实际依赖并保留运行时错误；未扩大投影需求或修改 Sort 子类的虚调用。内存／落盘排序与NULL/空串/文本NULL、二进制、长字段均有完整值检查。

原 62 条 SQL 在旧版与 C1 全部重新运行，初开／冷开 4 个 Main 正常0，完整请求／回复逐字节一致；全部 T7、值／NULL、SQLSTATE、诊断、tag 与 Ready 保留。既有 9 条 PostgreSQL 差异仍在，strict PG=false；沿用并核对真实 PostgreSQL18.6 的固定原始 oracle，没有运行 PG 速度对比。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

只接受有限执行优化，未承诺所有操作变快；[完整开销与未解决项](performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
<a id="view-column-dependencies-current"></a>
## 2026-11-06：普通 SQL 视图依赖与删除契约

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

CREATE/REPLACE 从纯绑定声明记录真实关系 OID 与列号，不执行表达式推断值；星号固定当前展开，列别名映射回实际来源。有效旧视图在删除前只读绑定，单来源旧星号需历史输出前缀的明确证据；歧义旧定义会失败，不猜测已丢失的数据。编译目录来源不要求物理堆／view 文件。

删除保护在注释清理、堆重写前执行；IF EXISTS 的缺失列不删除视图。CASCADE 包含依赖视图与其子视图，失败恢复由已有 DDL 事务快照负责。普通保存点恢复经真实 SQL 验证，未扩展到通用注入故障矩阵。

本批范围是普通 SQL 的静态 DDL 关系／列依赖。视图来源 T7 的零 origin、列别名运行时重绑定、部分嵌套视图、字面量／envelope 运行时、含歧义历史的旧视图、物化视图、函数／类型依赖及大量视图的扩展性仍有缺口；普通 pg_attribute 查询尚未补齐。用户暂缓范围保持，未修改执行图／child-provider、portal 或通用故障矩阵。

完整 T7、NOTICE／错误明细等 PG 差异仍保留，未声称完整协议等价。[测试](test-report.md#view-column-dependencies-current)及[真实开销](performance-report-2026-10-30.md#view-column-dependencies-20261106)给出本批界限。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
<a id="projected-toast-demand-current"></a>
## 2026-11-06：普通物理部分列投影

普通物理游标上的部分列投影按实际选择解码字段，复用当前行 NULL 位图，仅解析被选中长字段的 TOAST 引用。选中长字段读取失败仍在行输出前报错，输出值、NULL、RID 与旧路径的列顺序保持。全列/星号投影继续原 materialization 路径；选中虚拟列保留原完整输入。过滤、索引、排序、缓冲和不同 schema 的子路径仍由各自已有执行路径处理。

原生用例覆盖真实缺失 TOAST 文件、SQL NULL/空串/字面 NULL、marker-looking 字符串、UTF8、换行、BYTEA 和虚拟列错误。普通协议的完整 RowDescription/数据/NULL/错误/tag/Ready 与旧版一致；原 PostgreSQL 差异仍保留。本批适用于当前普通执行器，保留既有存储格式；retained/参数/portal/wire-format2、未选中显式 CAST 分析和用户暂缓边界保持。详见[完整报告](performance-report-2026-10-30.md#projected-toast-demand-20261106)。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
<a id="pg-class-declaration-current"></a>
## 2026-11-06 pg_class declaration and value boundary

The accepted candidate is normal CMake C++17/O2 C2 `280ad4a8`; the first C1 is rejected and retained as evidence. This is a partial catalog feature, with no speed improvement or full PostgreSQL compatibility claim.

The mutable CatalogService startup owner now creates and persists actual relation1259, rowtype83/array273 and34 sequential attributes. The extra actual types are `pg_node_tree`194, `aclitem`1033 (16 bytes/alignment `d`) and `_aclitem`1034. Cold metadata snapshots neither bootstrap nor allocate or publish. A loaded incompatible class/type/attribute/dependency declaration raises `XX001`; the finite tested conflicts do not claim an exhaustive corruption/fault matrix.

Type discoverability does not grant a value codec. Selected CAST/typed-literal conversions for these metadata-only declarations, including NULL/array input, raise `0A000`. No input/output function OID is invented. The generic declaration resolver and unselected explicit-CAST input analysis are unchanged. Ordinary qualified user types with the same basename remain separate.

The ordinary `pg_class` renderer still implements only `oid, relname, relnamespace, relkind, relnatts, relpersistence, relowner`; the other27 values and wildcard remain unsupported. Virtual source origins still differ from PG; the tested `pg_attribute` SQL route still fails `42P01`. This does not provide a catalog heap/MVCC, ACL/security semantics, retained planning/portal/format2 work or full CAT02 closure. [Full unfiltered evidence](performance-report-2026-10-30.md#pg-class-declaration-20261106).
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 本批有限日志契约（P32-C1）

普通日志 `::log` 仍按 time、四个空格、user、四个空格、operation 和换行追加原始字节；每次实际打开当前路径，以 `O_APPEND` 和六段 `writev` 写入，EINTR 重试、正短写继续消费未写后缀，零进展或其他写错误停止并关闭。打开失败仍输出 `error 1` 并返回0；权限、已有前缀、rename/unlink 与真实 symlink 重新解析保持。`getTime`、`auditLog` 正文字节未变，没有持久缓存日志FD或改服务端协议发送。

验证限本地普通文件与默认char-codecvt环境；真实短记录并发、轮转及有限系统调用控制不构成NFS、大记录事务原子性、fsync持久性或完整故障矩阵承诺。本批不改协议、目录/参数帧/retained/provider边界；58目标仍53通过、4冷锚仍失败，共9项普通差异未豁免。产品、PG行为目标和wire格式版本仍分别定义。[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与可逆原始证据（可选本地文件 `benchmarks/legacy-log-append-20261106.json.gz`）。以下原契约不变。

[R145](issue-review-r145-search-path-value-identity.md)有限支持SQL SET search_path的
逐参数list-quote、原样名字数据、空名字及FROM CURRENT真实事务赋值。
PG3573、Root18/clean10及独立复核通过；不据此宣称startup raw GUC格式、
全部数字/Unicode标识符语法、quote_all_identifiers或其它参数事务语义完整。
R144应用名规范化按用户要求暂缓，没有在本项接受。

本契约定义 DBMS_C_plus_plus 可以对外作出的兼容性声明。它把产品版本、
PostgreSQL 行为目标、wire protocol 和项目扩展分开版本化，避免把“psql 能连接”
误写成“与 PostgreSQL 等价”。当前总账状态仍只取自
[`postgresql-18-gap-audit.md`](postgresql-18-gap-audit.md) 与
[`gap-progress.json`](gap-progress.json)。

## 四个独立版本轴

| 版本轴 | 当前值 | 含义 | 不代表什么 |
| --- | --- | --- | --- |
| 产品版本 | `0.2.0` | 本项目自己的 SemVer；唯一代码事实源是 `src/common/version.h`，并与 CMake、包和 CHANGELOG 校验 | 不等于 PostgreSQL server 版本 |
| 行为兼容目标 | `PostgreSQL 18`，差分基线 `18.6` | `postgresql18` 模式要逐项逼近的 SQL、catalog、事务、错误和协议行为 | 当前尚未达到该目标 |
| wire protocol 上限 | `3.0` (`196608`) | 本地实现的最高协议版本；3.x 新 minor 会通过 `NegotiateProtocolVersion` 降到 3.0 | 不表示实现 PostgreSQL 18 的全部前后端消息或 libpq 行为 |
| 扩展契约 | `extended` | 显式启用项目自己的非 PostgreSQL SQL 和管理入口 | 不是 PostgreSQL、MySQL 或其磁盘格式兼容模式 |

这四个值可以独立变化。产品发版不能暗示 PostgreSQL 行为目标已经完成；修改
wire protocol 不能自动提升行为兼容声明；增加 extended 命令也不能扩大
`postgresql18` 模式的声明范围。

## 对外定位和模式边界

在 273 项审计全部达到验收门之前，本项目的准确定位是：

> 一个提供 PostgreSQL wire protocol 3.0 子集、可供部分 PostgreSQL 客户端连接的
> 自有 DBMS，正在以 PostgreSQL 18.6 为差分基线建设行为兼容性。

不得使用“PostgreSQL 18 compatible”、“drop-in replacement”、“PG18 equivalent”
或同义的无范围限定声明。

`postgresql18` 是默认模式。它只能接受 PostgreSQL 18 语法或项目已经实现并验证的
对应行为；未实现能力必须返回稳定且合适的 SQLSTATE，不能写入 sidecar 后伪造成功。
项目语法必须被拒绝，不能因默认值、客户端或调用入口不同而泄漏。

`extended` 只能由 `DBMS_COMPATIBILITY_MODE=extended` 或会话级
`SET compatibility_mode = extended` 显式进入。它可以开放项目命令，但不会把没有
运行时的 PostgreSQL 对象变成成功操作。切换模式不改变产品版本、wire protocol
版本、数据格式或当前 database。

模式分派只匹配完整目标参数名，不扫描普通SET的值内容；相似或带点名称
不是该builtin。普通参数的TO/等号分隔符从引号外定位，不把数据中的等号当语法。
[R141](issue-review-r141-set-parameter-identity.md)仅验证这些有限边界，不代表完整GUC
语法、参数域、quoted模式值或事务级LOCAL已经实现。

[R143](issue-review-r143-show-guc-protocol-results.md)仅覆盖application_name、
client_encoding、search_path、lc_monetary四个已实现标量SHOW的固定text结构与值传输：
支持Simple及已Describe的Extended text/binary，空串与文字NULL不冒SQL NULL，
路径换行不拆行。这不代表完整SHOW族、应用名值规范化或全部GUC事务语义完成。

[R142](issue-review-r142-quoted-set-parameter.md)将完整单token的quoted普通SET目标
按GUC大小写无关规则解码，覆盖已实现应用名/编码/路径/locale与LOCAL搜索路径；
原始@变量语法不由quoted目标进入。值解析、未知参数错误映射与其它事务作用域
没有扩展，不意味着完整GUC或TimeZone暂缓语法完成。

进程启动必须通过 `-D/--data-dir` 或 `DBMS_DATA_DIR` 显式选择本项目数据根。
`DBMS_CONTROL` 的项目 magic 和 system identifier 只声明 DBMS-C++ 自有 cluster
身份，不代表磁盘格式与 PostgreSQL 兼容；含 `PG_VERSION` 的 PostgreSQL cluster
会被拒绝，运行状态不会写入调用者的启动 CWD。

## 本项目时间列的物理版本

schema9/A/B/C中的Legacy秒列保持原物理单位；新SQL标量TIME/TIMESTAMP/
TIMESTAMPTZ使用schema-D的TCM1逐列标签及int64微秒。timestamp基准为2000年，
不能按槽宽或typmod猜单位，也不能用旧程序打开新格式后静默降级解释。
具体写入、转换、索引及冷读的限定验证见[R136](issue-review-r136-temporal-microsecond-storage.md)；
完整升级/降级、TIMETZ、数组precision与全部时间输入域仍不得推导为已验收。
这不改变产品0.2.0、wire3.0、目标18.6或A/B门状态。

## 两级验收标准

### A. “PostgreSQL 客户端可连接”

只有同时满足下列条件，才可对明确列出的客户端版本声明“通过 PostgreSQL wire
protocol 子集连接”：

1. 真实 TLS 构建完成 startup、认证、ParameterStatus、简单查询、扩展查询、取消、
   通知和错误恢复的协议测试；未覆盖的消息必须列为限制。
2. 服务器正确协商其真实最高 wire 版本；不得把 `server_version` 或协议协商值用作
   行为兼容证据。
3. 对支持的消息和 SQL，返回的帧顺序、字段、OID、command tag、SQLSTATE 和事务状态
   有差分回归；畸形输入 fail-closed，不能崩溃或使后续连接失步。
4. 发布说明列出测试过的客户端、版本、TLS/认证方式及已知缺口。仅“psql 能启动”
   或单条 `SELECT 1` 成功不通过此门。

通过 A 门只允许声明 wire/client interoperability 的限定子集，不能声明 PostgreSQL
行为兼容。

### B. “PostgreSQL 18 行为兼容”

只有同时满足下列条件，才可声明 PostgreSQL 18 行为兼容：

1. [`postgresql-18-gap-audit.md`](postgresql-18-gap-audit.md) 的 273 项全部为
   `complete`；`partial`、`unverified` 和 `deferred_by_user` 均阻止该声明。
2. 183 个 PostgreSQL 18 命令及 SQL/type/function/catalog/transaction/storage/
   replication/protocol/operations 各功能族都有可重复的 PostgreSQL 18.6 差分矩阵；
   值、类型元数据、错误、可见性和副作用均一致，不能只比较文本成功与否。
3. PostgreSQL regression/isolation、协议客户端、崩溃恢复、故障注入、升级/降级、
   sanitizer、fuzz、soak 和并发 history 验证达到审计中对应门槛，并为 clean worktree
   生成机器可读发布报告。
4. `postgresql18` 模式下所有项目扩展均有负向测试；未实现能力明确失败，不存在
   compatibility-record 假成功。
5. 发布文档逐项限定二进制、扩展 ABI、catalog、逻辑/物理复制及 on-disk 格式。
   除非它们各自通过专项验收，不得推导为兼容。

## 发布声明矩阵

| 状态 | 允许的声明 | 禁止的推导 |
| --- | --- | --- |
| A 门未通过 | “实现中的 PostgreSQL protocol 接口” | 任何客户端兼容或 PG 行为兼容声明 |
| 仅 A 门通过 | “已验证客户端 X/Y 可通过 wire 3.0 子集连接”，并链接报告和限制 | PostgreSQL-compatible、SQL/catalog/transaction 等价 |
| A、B 门均通过 | “对 PostgreSQL 18.6 行为兼容”，并链接全量报告与仍不兼容的非目标接口 | PostgreSQL 二进制、extension ABI、物理复制或磁盘格式兼容，除非另有专项证明 |

当前状态是 **A 门和 B 门均未宣告通过**。任何发版、README、包描述和客户端握手
字段都必须遵守这张矩阵；实时进度变化由总账决定，而不是修改本文中的结论。

## 变更规则

- PostgreSQL 主版本目标、差分 patch baseline 或 wire 上限变化，需要独立变更记录、
  对应矩阵刷新和测试更新。
- 行为差异必须进入 273 项总账对应条目；没有条目时先新增审计项，不能只改营销文案。
- 只有 `scripts/check_gap_progress.py --require-complete` 成功且 B 门的发布证据齐全，
  才能把 B 门状态改为通过。
- `server_version` 为客户端解析所需的互操作字段，不是产品身份或兼容认证；产品身份
  始终使用 `DBMS_VERSION_STRING`。
