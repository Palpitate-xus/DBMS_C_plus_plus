# 更新日志

<details>
<summary>可以给一组表所在的命名空间改名 · 2026-11-22</summary>

例如，把「销售」改成「销售归档」后，可以用新名字访问里面的表和视图，之前建好的报表也能继续使用。

[用法](docs/MANUAL.md#schema-改名) · [测试报告](docs/test-report.md#schema-rename-20261122) · [性能报告](docs/performance-report-2026-10-30.md#schema-rename-cost-20261122)

</details>

<details>
<summary>表改名或换位置后，原来的报表还能用 · 2026-11-22</summary>

例如，订单表从销售区移到归档区后，原来的报表仍会查询这张表。即使用旧名字新建一张表，报表也会继续读原来的订单。

[用法](docs/MANUAL.md#9-视图与物化视图) · [测试报告](docs/test-report.md#view-table-rename-20261122) · [性能报告](docs/performance-report-2026-10-30.md#view-table-rename-cost-20261122)

</details>

<details>
<summary>新建的报表会记住用的是哪张表 · 2026-11-22</summary>

例如，报表创建时用的是销售部门的订单表。即使后来有人建了同名的临时表，报表仍会读取原来的订单。

[用法](docs/MANUAL.md#9-视图与物化视图) · [测试报告](docs/test-report.md#view-source-identity-20261122) · [性能报告](docs/performance-report-2026-10-30.md#view-source-identity-cost-20261122)

</details>

<details>
<summary>更早的更新</summary>

## 2026-11-22 · 可以把表和视图里的数据合起来查

例如，销售汇总视图里只有产品编号和金额。用产品编号匹配产品表，就能同时查出产品名称和销售总额。

[用法](docs/MANUAL.md#9-视图与物化视图) · [测试报告](docs/test-report.md#join-ordinary-views-20261122) · [性能报告](docs/performance-report-2026-10-30.md#join-ordinary-views-cost-20261122)

## 2026-11-22 · 表里的列改名后，已有视图照常可用

例如，订单表把「数量」改成「订购数量」后，已有视图里的这一列仍叫「数量」。

[用法](docs/MANUAL.md#9-视图与物化视图) · [测试报告](docs/test-report.md#rename-dependent-views-20261122) · [性能报告](docs/performance-report-2026-10-30.md#rename-dependent-views-cost-20261122)

## 2026-11-22 · 列改名后，原来的检查规则仍然生效

例如，订单数量改名后，系统仍会拒绝负数，订单金额也能继续按数量和单价计算。

[用法](docs/MANUAL.md#修改表) · [测试报告](docs/test-report.md#rename-quoted-column-references-20261122) · [性能报告](docs/performance-report-2026-10-30.md#rename-quoted-column-references-cost-20261122)

## 2026-11-22 · 导入时可以跳过数值超出自定义范围的行

例如，给数量定义了「必须为正数」的类型后，可以设置跳过数量为负的行。如果遇到重复的订单号，整批订单仍会撤销。

[用法](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-domain-input-20261122) · [性能报告](docs/performance-report-2026-10-30.md#copy-domain-input-cost-20261122)

## 2026-11-22 · 数字列误填文字时，导入可以继续

例如，把「三件」填进整数列时，可以只跳过这一行。也可以设定最多跳过多少行，超出上限就撤销整批导入。

[用法](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-ignore-bad-rows-20261122) · [性能报告](docs/performance-report-2026-10-30.md#copy-ignore-bad-rows-cost-20261122)

## 2026-11-22 · 导入失败后，已取过的编号不会重用

例如，导入订单时先生成了订单号 40，随后因数量填错而撤销这批订单。下次导入会从 41 继续。

[用法](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-default-evaluation-order-20261122) · [性能报告](docs/performance-report-2026-10-30.md#copy-default-evaluation-order-cost-20261122)

## 2026-11-22 · 导入旧数据时可以保留原来的编号

例如，迁移订单时可以保留旧订单号。文件里省略编号时，数据库可以自动生成。

[用法](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-identity-input-20261122) · [性能报告](docs/performance-report-2026-10-30.md#copy-identity-input-cost-20261122)

## 2026-11-22 · 导入数据时可以使用列的默认值

例如，导入订单时可以用一个约定的标记，让数据库填上默认状态。

[用法](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-default-marker-20261122) · [性能报告](docs/performance-report-2026-10-30.md#copy-default-marker-cost-20261122)

## 2026-11-22 · 从 CSV 导入数据

可以把表格软件保存的 CSV 导入数据库，并在导入前核对第一行的列名和顺序。单元格里有换行也可以导入。

[使用说明](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-csv-in-20261122) · [性能报告](docs/performance-report-2026-10-30.md#copy-csv-in-cost-20261122)

## 2026-11-22 · 把查询结果保存为 CSV

例如，只导出本月的订单，按金额排序，并把列名写在第一行。

[使用说明](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-csv-out-20261122) · [性能报告](docs/performance-report-2026-10-30.md#copy-csv-out-cost-20261122)

## 2026-11-22 · 查询可以等表结构保存完成

数据库正在保存表结构时，另一个连接发来的查询可以等保存完成再继续。达到设置的等待时间才报超时。

[测试报告](docs/test-report.md#begin-catalog-contention-20261122) · [性能报告](docs/performance-report-2026-10-30.md#begin-catalog-contention-cost-20261122)

## 2026-11-22 · 可以导出查询筛选后的数据

例如，只导出本月的订单，并按金额排序。客户端可以用 `COPY (SELECT ...) TO STDOUT` 逐行接收结果。

[使用说明](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-query-stdout-20261122) · [性能报告](docs/performance-report-2026-10-30.md#copy-query-stdout-cost-20261122)

## 2026-11-22 · 按编号查数据类型更快

例如，查编号 23 对应的类型名时，数据库会先按编号找到对应的类型，再返回名称。

[测试报告](docs/test-report.md#pg-type-oid-source-20261122) · [性能报告](docs/performance-report-2026-10-30.md#pg-type-oid-source-cost-20261122)

## 2026-11-22 · 修复导入和导出时选错表的问题

如果你建了一张叫 `pg_type` 的表，用 `COPY` 导入或导出时，数据库会按当前连接的设置在你的表和自带的类型表之间选择。导出自带的类型表时，可以只取类型名等需要的列。

[使用说明](docs/MANUAL.md#用客户端复制数据) · [测试报告](docs/test-report.md#copy-relation-selection-20261122)

## 2026-11-22 · 查询和写入时会区分自带的类型表和同名表

如果你在 `public` 下建了一张叫 `pg_type` 的表，可以用 `public.pg_type` 明确指定它。只写表名时，数据库会按当前连接设置的查找顺序选表。

[使用说明](docs/MANUAL.md#查找与系统表同名的-pg_type) · [测试报告](docs/test-report.md#pg-type-search-path-20261122)

## 2026-11-22 · 按一长串值筛选时，更快看到查询计划

在一项按 32 个值筛选的测试中，用 `EXPLAIN` 查看查询计划的时间约减半。

[测试报告](docs/test-report.md#explain-filter-statistics-20261122) · [性能报告](docs/performance-report-2026-10-30.md#explain-filter-statistics-cost-20261122)

## 2026-11-22 · 开始事务时少做一轮表结构检查

测试中，执行 `BEGIN` 少了 22 次文件或目录状态检查。

[测试报告](docs/test-report.md#catalog-publication-one-scan-20261122) · [性能报告](docs/performance-report-2026-10-30.md#catalog-publication-one-scan-cost-20261122)

## 2026-11-22 · 别人改表时，仍可查看和调整自己的连接设置

例如，别人正在改表时，你仍可单独查看或调整遇到锁时最多等多久。

[使用说明](docs/MANUAL.md#在-sql-表达式里读取和修改设置) · [并发测试](docs/test-report.md#session-setting-path-20261122) · [性能报告](docs/performance-report-2026-10-30.md#session-setting-path-cost-20261122)

## 2026-11-22 · 查询里也能查看和修改连接设置

可以在 SQL 查询里查看或调整当前连接的锁等待时间。也可以只为当前事务调整。

[使用示例](docs/MANUAL.md#在-sql-表达式里读取和修改设置) · [测试报告](docs/test-report.md#session-setting-functions-20261122) · [性能报告](docs/performance-report-2026-10-30.md#session-setting-functions-cost-20261122)

## 2026-11-22 · 回滚后会恢复原来的超时设置

例如，事务中临时缩短了锁等待时间，回滚后就会恢复到事务开始前的值。

[使用示例](docs/MANUAL.md#在事务里临时调整超时) · [测试报告](docs/test-report.md#transactional-timeouts-20261122) · [性能报告](docs/performance-report-2026-10-30.md#transactional-timeouts-cost-20261122)

## 2026-11-22 · 等不到更严格的表锁，原来的锁会保留

如果你已锁住一张表，再请求更严格的锁时遇到超时或取消，原来的锁仍然有效。也可以让数据库一遇到冲突就报错。

[使用示例](docs/MANUAL.md#在事务里锁住一张表) · [测试报告](docs/test-report.md#table-page-lock-upgrade-20261122) · [性能报告](docs/performance-report-2026-10-30.md#table-page-lock-upgrade-cost-20261122)

## 2026-11-22 · 改记录时等不到锁，原来的锁会保留

如果你先锁住一条记录来核对，之后尝试修改时等到超时，回到保存点后，其他人仍要等你的事务结束才能修改这条记录。

[使用说明](docs/MANUAL.md#查询时锁住分区表记录) · [测试报告](docs/test-report.md#row-lock-upgrade-20261122) · [性能报告](docs/performance-report-2026-10-30.md#row-lock-upgrade-cost-20261122)

## 2026-11-22 · 多人可以同时核对分区表里的同一条记录

核对时可以锁住记录，让其他人也能查看。要修改这条记录，得等核对的人都结束各自的事务。

[使用示例](docs/MANUAL.md#查询时锁住分区表记录) · [测试报告](docs/test-report.md#partition-share-20261122) · [性能报告](docs/performance-report-2026-10-30.md#partition-share-cost-20261122)

## 2026-11-22 · 改了分区键，记录会移到对应分区

部分分区表现在可以修改记录。比如把一笔订单的编号从 5 改为 15，数据库会按新编号把它放到对应的分区。提交前可以撤销。

[适用范围和示例](docs/MANUAL.md#修改分区表中的记录) · [测试报告](docs/test-report.md#partition-update-20261122) · [性能报告](docs/performance-report-2026-10-30.md#partition-update-cost-20261122)

## 2026-11-22 · 部分分区表现在可以删记录

可以按条件删除记录、查看删掉了哪些行，也可以在提交前撤销。

[适用范围和示例](docs/MANUAL.md#删除分区表中的记录) · [测试报告](docs/test-report.md#partition-delete-20261122) · [性能报告](docs/performance-report-2026-10-30.md#partition-delete-cost-20261122)

## 2026-11-22 · 查整张分区表时，少做重复工作

读取不同分区时，数据库会共用已经查到的表结构。在一张有 60 个分区的测试表上，查询整张表比上一个版本少花了约 13% 到 18% 的时间。

[使用说明](docs/MANUAL.md#15-分区表) · [测试报告](docs/test-report.md#partition-scan-reuse-20261122) · [性能报告](docs/performance-report-2026-10-30.md#partition-scan-reuse-cost-20261122)

## 2026-11-22 · 按编号范围查找时，跳过无关分区

比如订单按编号分段存放。查找 100 到 199 号订单时，数据库可以跳过肯定没有这些订单的分区。

[使用说明](docs/MANUAL.md#15-分区表) · [测试报告](docs/test-report.md#integer-range-pruning-20261122) · [性能报告](docs/performance-report-2026-10-30.md#integer-range-pruning-cost-20261122)

## 2026-11-22 · 翻到后面的页面时，少读长文本

给分区表按编号排序后，比如翻到第 5 页，符合条件的查询只读取这一页的长文本。

[使用说明](docs/MANUAL.md#15-分区表) · [测试报告](docs/test-report.md#partition-offset-output-20261122) · [性能报告](docs/performance-report-2026-10-30.md#partition-offset-output-cost-20261122)

## 2026-11-22 · 按编号取前几条时，少读长文本

比如在分区表里找编号最小的 10 条记录，查询可以先比较编号，只为可能入选的记录读取长文本。

[使用说明](docs/MANUAL.md#15-分区表) · [测试报告](docs/test-report.md#partition-order-input-20261122) · [性能报告](docs/performance-report-2026-10-30.md#partition-order-input-cost-20261122)

## 2026-11-22 · 只取几条记录时，读够就停

查询分区表时，如果只取一条且不要求排序，读到符合条件的记录就可以停。测试中，需要打开的分区文件从 60 个减少到 1 个。

[使用说明](docs/MANUAL.md#15-分区表) · [测试报告](docs/test-report.md#partition-limit-prefix-20261122) · [性能报告](docs/performance-report-2026-10-30.md#partition-limit-prefix-cost-20261122)

## 2026-11-22 · 分区较多的表，重复查询用时缩短

查询多个分区时，可以共用已经查到的表结构。在有 60 个分区的测试表上，连续查询的耗时缩短了约 16% 到 18%。

[测试报告](docs/test-report.md#partition-scan-schema-20261122) · [性能报告](docs/performance-report-2026-10-30.md#partition-scan-schema-cost-20261122)

## 2026-11-22 · 修复部分跨分区查询漏掉记录的问题

用 `OR` 连接筛选条件时，符合任一条件的记录都会返回。一条记录同时符合多个条件时，也只返回一次。排序和查询结果仍能区分空值、空字符串和数字零。

[测试报告](docs/test-report.md#plain-partition-or-20261122) · [性能报告](docs/performance-report-2026-10-30.md#plain-partition-or-cost-20261122)

## 2026-11-22 · 可以向分区表写入数据

可以对部分已建好分区的表执行 `INSERT`。数据库会把记录放到对应的分区，提交前还能撤销写入。

[写入条件](docs/MANUAL.md#向分区表写入) · [测试报告](docs/test-report.md#transactional-partition-insert-20261122) · [性能报告](docs/performance-report-2026-10-30.md#transactional-partition-insert-cost-20261122)

## 2026-11-22 · 清空分区表后及时腾出磁盘空间

数据库会及时关闭旧分区文件，让操作系统回收这些文件占用的空间。单独清空默认数据目录里的分区表时，可以只保存恢复这张表所需的文件。

[测试报告](docs/test-report.md#truncate-partition-undo-20261122) · [性能报告](docs/performance-report-2026-10-30.md#truncate-partition-undo-cost-20261122)

## 2026-11-22 · 同时清空多张表，出错时一起恢复

用一条命令清空默认数据目录里的多张普通表时，数据库可以只保存这些表恢复所需的文件。提交前出错，已清空的表会一起恢复。

[测试报告](docs/test-report.md#truncate-multi-table-undo-20261122) · [性能报告](docs/performance-report-2026-10-30.md#truncate-multi-table-undo-cost-20261122)

## 2026-11-22 · 清空一张表时减少备份文件

单独清空默认数据目录里的一张普通表时，数据库可以只备份恢复这张表所需的文件。提交前意外退出，重启后可以找回原来的数据。也修复了部分操作记录可能漏写到磁盘的问题。

[测试报告](docs/test-report.md#truncate-table-undo-20261122) · [性能报告](docs/performance-report-2026-10-30.md#truncate-table-undo-cost-20261122)

## 2026-11-21 · 重启时会检查清空表是否已提交

对新建的普通表，单独执行 `TRUNCATE` 时如果数据库意外退出，重启后会根据这次清空是否已提交，保留空表或恢复原来的数据。

[测试报告](docs/test-report.md#transactional-truncate-wal-20261121) · [性能报告](docs/performance-report-2026-10-30.md#transactional-truncate-wal-cost-20261121)

## 2026-11-21 · 备份时减少重复打开文件

核对备份文件和把它写到磁盘时，只需打开一次文件。备份做到一半中断后，数据库重启时会跳过那份备份。

[测试报告](docs/test-report.md#backup-manifest-sync-20261121) · [性能报告](docs/performance-report-2026-10-30.md#backup-manifest-sync-cost-20261121)

## 2026-11-21 · 清空表时只重建一次索引

清空订单表后，可以照常写入新订单，继续用原来的索引按订单号查询。

[测试报告](docs/test-report.md#truncate-single-index-rebuild-20261121) · [性能报告](docs/performance-report-2026-10-30.md#truncate-single-index-rebuild-cost-20261121)

## 2026-11-21 · 清空表或改列名时会保护其他表的数据

例如，清空 `orders` 后，仍能查询 `orders_items`。如果改列名会覆盖另一张表的索引，数据库会拒绝改名。

[测试报告](docs/test-report.md#truncate-column-ownership-20261121)

## 2026-11-21 · 建索引时会保留其他表已有的索引

如果新索引会覆盖另一张表已有的索引，数据库会拒绝创建，原来的数据仍能查询。

[测试报告](docs/test-report.md#index-file-ownership-20261121)

## 2026-11-21 · 表改名后可以用原名建一张新表

例如，把 `orders` 改名为 `orders_archive` 后，可以再建一张 `orders`。旧订单和新订单可以分别查询。

[测试报告](docs/test-report.md#rename-table-file-ownership-20261121)

## 2026-11-21 · 删除一张表时保留名称相近的其他表

例如，删除 `orders` 后，`orders_items` 的索引和长文本仍可以读取。如果两张表已在共用同一个索引文件，数据库会拒绝删除。

[测试报告](docs/test-report.md#drop-table-file-ownership-20261121)

## 2026-11-21 · 部分查询能更快找出两列文字相同的记录

在测试的单表查询中，如果建表时将一列设为按字符编码比较（`COLLATE "C"`），另一列使用数据库默认规则，筛出两列内容相同的记录用时更短。比较 `TEXT` 和 `VARCHAR` 时，末尾空格仍参与比较。

[测试报告](docs/test-report.md#declared-binary-comparison-20261121) · [性能报告](docs/performance-report-2026-10-30.md#declared-binary-comparison-cost-20261121)

## 2026-11-21 · 两列文字的比较规则冲突时会提示错误

可以在查询里用 `COLLATE` 为两列指定同一种规则。空表里没有记录需要比较，查询会正常返回空结果。

[使用手册](docs/MANUAL.md#基础查询) · [测试报告](docs/test-report.md#implicit-collation-filter-20261121) · [性能报告](docs/performance-report-2026-10-30.md#implicit-collation-filter-cost-20261121)

## 2026-11-21 · 筛选文字时可以指定比较规则

例如 `WHERE title COLLATE "C" < 'm'` 会按字符编码比较，找出排在 `m` 前面的标题。也可以使用自己创建的规则。规则名写错时，即使表是空的，也会收到错误提示。

[使用手册](docs/MANUAL.md#基础查询) · [测试报告](docs/test-report.md#explicit-collation-filter-20261121) · [性能报告](docs/performance-report-2026-10-30.md#explicit-collation-filter-cost-20261121)

## 2026-11-21 · 修复比较同一行两列时的漏查

例如 `WHERE id = peer_id` 会查找两列值相等的记录。以前给 `id` 建索引后，这种查询有时会漏掉符合条件的记录。现在也能区分只差 1 的大整数及 `'01'` 和 `'1'` 这样的文本。

[使用手册](docs/MANUAL.md#基础查询) · [测试报告](docs/test-report.md#same-row-select-20261121) · [性能报告](docs/performance-report-2026-10-30.md#same-row-select-cost-20261121)

## 2026-11-21 · 修复把数字误当成列名的问题

`WHERE id = 7` 查找编号为 7 的记录。`WHERE id = "7"` 比较同一条记录里的 `id` 和名为 `"7"` 的列。

[使用手册](docs/MANUAL.md#基础查询) · [测试报告](docs/test-report.md#narrow-select-20261121) · [性能报告](docs/performance-report-2026-10-30.md#narrow-select-cost-20261121)

## 2026-11-21 · 按编号删除时少读数据

按整数编号删除时，即使编号列没有索引，数据库也能减少重复读取。表里有名为 `"7"` 的列时，`DELETE FROM orders WHERE id = 7` 仍按编号 7 删除。

[使用手册](docs/MANUAL.md#删除) · [测试报告](docs/test-report.md#narrow-delete-20261121) · [性能报告](docs/performance-report-2026-10-30.md#narrow-delete-cost-20261121)

## 2026-11-21 · 按编号更新时更早跳过无关记录

例如更新某笔订单时，数据库可以先核对订单号，只对编号符合的记录检查其他条件。

[测试报告](docs/test-report.md#narrow-query-write-20261121) · [性能报告](docs/performance-report-2026-10-30.md#narrow-query-write-cost-20261121)

## 2026-11-20 · 普通表最多可以建 1,600 列

列数上限从 30 提高到 1,600。实际能用多少列还取决于列的类型和每行数据的大小。同时修复了某些大批写入尚未提交时意外退出、导致数据库无法重启的问题。

[使用手册](docs/MANUAL.md#建表) · [测试报告](docs/test-report.md#wide-table-20261120) · [性能报告](docs/performance-report-2026-10-30.md#wide-table-narrow-cost-20261120)

## 2026-11-20 · 反复查看查询步骤时，省去重新计算

用 `EXPLAIN` 再次查看同一条简单的两表查询时，数据库可以沿用上次的执行步骤。想看查询这次实际处理了多少行，可以用 `EXPLAIN ANALYZE`。

[使用手册](docs/MANUAL.md#explain) · [测试报告](docs/test-report.md#legacy-join-explain-reuse-20261120) · [性能报告](docs/performance-report-2026-10-30.md#legacy-join-explain-reuse-cost-20261120)

## 2026-11-20 · 两表查询加了筛选或分页，也能查看执行步骤

用整数编号匹配两张表时，即使还要筛选、排序或分批取结果，也可以用 `EXPLAIN` 查看数据库怎样查找和匹配记录。

[使用手册](docs/MANUAL.md#explain) · [测试报告](docs/test-report.md#integer-join-explain-20261120) · [性能报告](docs/performance-report-2026-10-30.md#integer-join-explain-cost-20261120)

## 2026-11-20 · 两表按连续编号匹配时，可以直接找到记录

按整数编号匹配两张表时，如果右表的编号连续且不重复，数据库可以用编号直接找到已读过的记录，省去另建查找表的步骤。

[使用手册](docs/MANUAL.md#整数等值连接) · [测试报告](docs/test-report.md#integer-join-direct-20261120) · [性能报告](docs/performance-report-2026-10-30.md#integer-join-direct-cost-20261120)

## 2026-11-20 · 用整数列连接时减少重复查找

例如用订单编号连接两张表。需要反复查找右表时，数据库会利用已经读过的记录找出相同编号。无需排序时，取够结果就停。

[使用手册](docs/MANUAL.md#整数等值连接) · [测试报告](docs/test-report.md#integer-join-lookup-20261120) · [性能报告](docs/performance-report-2026-10-30.md#integer-join-lookup-cost-20261120)

## 2026-11-20 · 连接和子查询也能读取继承表

在连接或子查询中使用父表时，也能查到多层子表里的记录。同一张表在查询里出现两次时，`ONLY` 只限制写了它的那一次。

[使用手册](docs/MANUAL.md#查询继承表) · [测试报告](docs/test-report.md#nested-inherited-tables-20261120) · [性能报告](docs/performance-report-2026-10-30.md#nested-inherited-tables-cost-20261120)

## 2026-11-20 · 查询父表时也会读到孙表

统计父表的记录时，会算上各层子表里的记录。排序和分页也会处理这些记录。加上 `ONLY` 就可以只查指定的表。

[使用手册](docs/MANUAL.md#查询继承表) · [测试报告](docs/test-report.md#inherited-table-source-20261120) · [性能报告](docs/performance-report-2026-10-30.md#inherited-table-source-cost-20261120)

## 2026-11-20 · 修复查询中的引号和文字拼接

在单表查询中，`upper('we''ird')` 会保留中间的引号，返回 `WE'IRD`。在文字前加上 `prefix:` 再转成大写，`AbA` 会变成 `PREFIX:ABA`。遇到 SQL 空值（`NULL`），结果仍是空值。

[使用手册](docs/MANUAL.md#基础查询) · [测试报告](docs/test-report.md#scalar-string-arguments-20261120)

## 2026-11-20 · 重启后会继续清理建表留下的旧文件

如果意外退出打断了清理，下次启动会继续删除已经用完的建表恢复文件，避免它们一直占用磁盘空间。

[测试报告](docs/test-report.md#create-undo-cleanup-20261120)

## 2026-11-20 · 找回表文件目录后可以继续恢复

意外退出后，如果启动时找不到保存表文件的外部目录，数据库会报错并保留恢复所需的记录。目录重新可用后，再启动就能继续清理还没提交的新表。

[使用手册](docs/MANUAL.md#建表) · [测试报告](docs/test-report.md#external-create-recovery-20261120)

## 2026-11-20 · 重启后会清理没有提交的新表

如果在事务里新建了普通表，还没提交数据库就意外退出，重启后之前已提交的数据仍可查询，原来的表名也能再次使用。

[使用手册](docs/MANUAL.md#建表) · [测试报告](docs/test-report.md#ordinary-create-durable-undo-20261120) · [性能报告](docs/performance-report-2026-10-30.md#ordinary-create-durable-undo-cost-20261120)

## 2026-11-20 · 保存表清单时可重试一次

如果新清单已写好，只是没能确认写入磁盘，数据库会再确认一次。重试仍失败时会报错。

[测试报告](docs/test-report.md#table-list-directory-sync-20261120) · [性能报告](docs/performance-report-2026-10-30.md#table-list-directory-sync-cost-20261120)

## 2026-11-20 · 建表时先把表定义完整写入磁盘

如果保存表定义时遇到同名文件，建表会报错并保留原文件。

[测试报告](docs/test-report.md#create-schema-publication-20261120) · [性能报告](docs/performance-report-2026-10-30.md#create-schema-publication-cost-20261120)

## 2026-11-20 · 归档缺失或损坏时，恢复操作会保留当前数据

例如，备份之后新增了一笔订单。如果找不到恢复所需的日志，数据库会报错，订单仍然可以查询。

[使用手册](docs/MANUAL.md#时间点恢复) · [测试报告](docs/test-report.md#pitr-stage-archive-20261120) · [性能报告](docs/performance-report-2026-10-30.md#pitr-stage-archive-cost-20261120)

## 2026-11-19 · 意外退出后保留已提交的表结构修改

如果数据库切换到新的日志文件后意外退出，重启时会继续读取这个文件，避免用旧备份覆盖已经提交的表结构修改。

[使用手册](docs/MANUAL.md#201-wal-归档与时间点恢复-pitr) · [测试报告](docs/test-report.md#wal-switch-terminal-recovery-20261119) · [性能报告](docs/performance-report-2026-10-30.md#wal-switch-terminal-recovery-cost-20261119)

## 2026-11-19 · 重复运行建表脚本时跳过多余备份

用 `CREATE TABLE IF NOT EXISTS` 再运行脚本，遇到已经存在的表会直接跳过。建表选项填错时，数据库会先报错，省去这次备份。

[使用手册](docs/MANUAL.md#建表) · [测试报告](docs/test-report.md#create-validate-before-backup-20261119) · [性能报告](docs/performance-report-2026-10-30.md#create-validate-before-backup-cost-20261119)

## 2026-11-19 · 恢复表结构时保留序列已经给出的编号

数据库异常退出后，如果需要撤销表结构修改，恢复时会保留仍存在的序列已经给出的编号，避免再次发放同一个值。

[使用手册](docs/MANUAL.md#7-事务控制) · [测试报告](docs/test-report.md#ddl-image-sequence-recovery-20261119) · [性能报告](docs/performance-report-2026-10-30.md#ddl-image-sequence-recovery-cost-20261119)

## 2026-11-19 · 修复临时表导致的重启失败

数据库异常退出后，重启时会清理确认是上次连接留下的临时表。已提交的普通表数据仍可读取。

[使用手册](docs/MANUAL.md#建表) · [测试报告](docs/test-report.md#temporary-table-crash-recovery-20261119) · [性能报告](docs/performance-report-2026-10-30.md#temporary-table-crash-cost-20261119)

## 2026-11-19 · 建表时为以后更新记录留出空间

用 `WITH (fillfactor = 90)` 建表，写入时会尽量留出约一成空间，方便以后更新记录。这个设置重启后仍然有效。

[使用手册](docs/MANUAL.md#建表) · [测试报告](docs/test-report.md#create-table-fillfactor-20261119) · [性能报告](docs/performance-report-2026-10-30.md#create-table-fillfactor-cost-20261119)

## 2026-11-19 · 插入记录遇到占用时，先试别的空位

如果原本要写入的位置正被占用，数据库会先试别的空位。有地方可写时，就不用等原来的位置腾出来。

[使用说明](docs/MANUAL.md#插入时如何找空位) · [测试报告](docs/test-report.md#insert-page-choice-20261119) · [性能报告](docs/performance-report-2026-10-30.md#insert-page-choice-cost-20261119)

## 2026-11-19 · 更新记录时，先在原来的位置附近找空位

如果原来的位置附近放不下更新后的记录，数据库会优先尝试之前记下的空位，减少从头查找的次数。

[使用说明](docs/MANUAL.md#更新时如何选择写入位置) · [测试报告](docs/test-report.md#update-version-placement-20261119) · [性能报告](docs/performance-report-2026-10-30.md#update-version-placement-cost-20261119)

## 2026-11-19 · 用另一张表更新或删除记录时，可以先按编号查找

例如，根据付款表更新或删除某笔订单。如果订单号是整数主键，数据库可以先通过索引找到订单，再检查付款表中的条件。

[使用说明](docs/MANUAL.md#按整数主键更新或删除) · [测试报告](docs/test-report.md#joined-primary-key-target-20261119) · [性能报告](docs/performance-report-2026-10-30.md#joined-primary-key-cost-20261119)

## 2026-11-19 · 同时修改同一条记录时，会按新值再检查

默认设置下，按另一张表更新或删除记录时，如果有人先修改并提交了这条记录，数据库会用新值再次检查条件。条件不再成立时，就跳过这条记录。

[使用说明](docs/MANUAL.md#同时修改同一行) · [测试报告](docs/test-report.md#joined-row-writes-20261119) · [性能报告](docs/performance-report-2026-10-30.md#joined-row-write-cost-20261119)

## 2026-11-19 · 同时改一条记录，后一次按新值处理

默认设置下，如果另一个连接先改了这条记录，等它提交后，更新或删除会用新值再检查条件。记录已被删，或者条件不再成立，就跳过它。

[使用说明](docs/MANUAL.md#同时修改同一行) · [测试报告](docs/test-report.md#same-row-writes-20261119) · [性能报告](docs/performance-report-2026-10-30.md#same-row-write-cost-20261119)

## 2026-11-19 · 按编号修改或删除记录时少读数据

例如按订单号删除一条订单。如果订单号是整数主键，数据库可以用索引找到这条记录，省去逐页查找。

[使用示例](docs/MANUAL.md#按整数主键更新或删除) · [测试报告](docs/test-report.md#integer-primary-key-mutation-20261119) · [性能报告](docs/performance-report-2026-10-30.md#integer-primary-key-mutation-cost-20261119)

## 2026-11-19 · 可以查看分区表的统计更新进度

更新分区表的统计信息时，可以看到已经处理了多少个分区，以及当前分区读到了哪里。

如果要求额外的进程一起整理表，`VACUUM FULL` 会提示此选项不受支持。

[使用示例](docs/MANUAL.md#查看表维护进度) · [测试报告](docs/test-report.md#partition-analyze-vacuum-parallel-20261119) · [性能报告](docs/performance-report-2026-10-30.md#partition-analyze-observation-cost-20261119)

## 2026-11-19 · 整理表时少花时间找空位

用 `VACUUM FULL` 整理表时，数据库会记住哪里还有空位，搬动记录时少找几次。

[使用示例](docs/MANUAL.md#分开清理主表和长值) · [测试报告](docs/test-report.md#vacuum-full-placement-20261119) · [性能报告](docs/performance-report-2026-10-30.md#vacuum-full-placement-cost-20261119)

## 2026-11-19 · 可以查看表清理的进度

清理表或更新查询统计时，可以从另一个连接查看正在处理哪张表、进行到了哪一步。

[使用示例](docs/MANUAL.md#查看表维护进度) · [测试报告](docs/test-report.md#maintenance-progress-views-20261119) · [性能报告](docs/performance-report-2026-10-30.md#maintenance-progress-views-cost-20261119)

## 2026-11-19 · 可以单独更新几列的统计

数据只改了几列时，可以只更新这些列的统计。数据库用这些统计选择怎样查找数据。

[使用示例](docs/MANUAL.md#只更新指定列的统计) · [测试报告](docs/test-report.md#vacuum-target-columns-20261119) · [性能报告](docs/performance-report-2026-10-30.md#vacuum-target-columns-cost-20261119)

## 2026-11-19 · 一次整理就能收回表和长文本的空间

运行 `VACUUM FULL` 可以同时收回两处空闲空间，一处在表文件里，另一处在单独存放长文本等大内容的文件里。

[使用示例](docs/MANUAL.md#分开清理主表和长值) · [恢复测试](docs/test-report.md#vacuum-combined-full-20261119) · [性能报告](docs/performance-report-2026-10-30.md#vacuum-combined-full-cost-20261119)

## 2026-11-18 · 删除长文本后，可以单独腾出磁盘空间

删除大量单独存放的长文本后，可以只缩小存放这些内容的文件，把空出的空间还给磁盘。

[使用示例](docs/MANUAL.md#分开清理主表和长值) · [恢复测试](docs/test-report.md#vacuum-toast-full-20261118) · [性能报告](docs/performance-report-2026-10-30.md#vacuum-toast-full-cost-20261118)

## 2026-11-18 · 清理表时，可以选择清理哪些数据

用 `VACUUM` 时，可以只清理表里的普通数据，也可以只清理单独存放的长文本。还可以单独更新查询用的统计信息。

[使用示例](docs/MANUAL.md#分开清理主表和长值) · [测试报告](docs/test-report.md#vacuum-process-options-20261118) · [性能报告](docs/performance-report-2026-10-30.md#vacuum-process-options-cost-20261118)

## 2026-11-18 · 清理表时可以少读一遍数据

如果表没有单独存放长文本等大内容，`VACUUM` 会省去检查这些内容的一遍读取。

[使用示例](docs/MANUAL.md#清理表占用的空间) · [测试报告](docs/test-report.md#vacuum-empty-toast-20261118) · [性能报告](docs/performance-report-2026-10-30.md#vacuum-empty-toast-cost-20261118)

## 2026-11-18 · 整理表后，范围查询可以少读数据

运行 `VACUUM FULL` 整理表后，符合条件的查询可以马上用 BRIN 索引跳过无关数据。

[使用示例](docs/MANUAL.md#重写表后的-brin-查询) · [测试报告](docs/test-report.md#brin-vacuum-full-generation-20261118) · [性能报告](docs/performance-report-2026-10-30.md#brin-vacuum-full-generation-cost-20261118)

## 2026-11-18 · 写入数据时少找几遍索引文件

新增、修改或删除数据时，数据库会一起查找相关的索引文件，少做几次重复查找。新增列、删除列和清空表时也有同样的改进。

[使用手册](docs/MANUAL.md#写入后的索引维护) · [测试报告](docs/test-report.md#specialized-index-discovery-20261118) · [性能报告](docs/performance-report-2026-10-30.md#specialized-index-discovery-cost-20261118)

## 2026-11-18 · 按编号翻页时少排一次序

用整数主键翻页时，如果索引给出的记录已经排好，数据库就直接按这个顺序返回。

[使用示例](docs/MANUAL.md#查看分页是否复用索引顺序) · [测试报告](docs/test-report.md#presorted-integer-primary-key-page-20261118) · [性能报告](docs/performance-report-2026-10-30.md#presorted-integer-primary-key-page-cost-20261118)

## 2026-11-18 · 翻页时可以跳过空值

例如只看有备注的记录，数据库可以按主键顺序查找，凑够一页就停。

[使用示例](docs/MANUAL.md#翻页时跳过空值) · [测试报告](docs/test-report.md#integer-primary-key-null-filter-20261118) · [性能报告](docs/performance-report-2026-10-30.md#integer-primary-key-null-filter-cost-20261118)

## 2026-11-18 · 同一条分页查询可以反复用

每次传入上一页最后一个整数主键，数据库就从对应的位置继续查找。

[使用示例](docs/MANUAL.md#重复执行带参数的分页) · [测试报告](docs/test-report.md#prepared-integer-primary-key-range-20261118) · [性能报告](docs/performance-report-2026-10-30.md#prepared-integer-primary-key-range-cost-20261118)

## 2026-11-18 · 从上一页最后一个编号接着查

如果主键是整数，把上一页最后一个编号写进 `WHERE`，查询就能从下一条开始。可以从小到大或从大到小，也可以同时指定起点和终点。

[使用示例](docs/MANUAL.md#从上页主键继续读取) · [测试报告](docs/test-report.md#integer-primary-key-range-20261118) · [性能报告](docs/performance-report-2026-10-30.md#integer-primary-key-range-cost-20261118)

## 2026-11-18 · 只查一页，取够就停

按整数主键排序时，新建或重建的索引可以跳过这一页之后的记录。已有索引可以继续用，也可以通过 `REINDEX TABLE` 重建。

修复了添加主键成功后，进程突然中断可能导致重启后无法按主键查找的问题。

[使用示例](docs/MANUAL.md#按整数主键翻页) · [测试报告](docs/test-report.md#signed-primary-key-prefix-20261118) · [性能报告](docs/performance-report-2026-10-30.md#signed-primary-key-prefix-cost-20261118)

## 2026-11-18 · 查一页时少读表记录

按整数主键排序时，可以先找到当前页的编号，再读取对应的表记录。需要读取较早的数据版本，或索引不可用时，数据库会从表中查找并排序。

[使用示例](docs/MANUAL.md#按整数主键翻页) · [测试报告](docs/test-report.md#integer-primary-key-page-20261118) · [性能报告](docs/performance-report-2026-10-30.md#integer-primary-key-page-cost-20261118)

## 2026-11-18 · 排序时少做数组转换

部分查询会先排序，再转换实际需要的数组。比如只取排序后的第一行时，后面行的无效数组不会提前报错。如果排序本身要用到转换结果，仍会先转换。

[使用示例](docs/MANUAL.md#排序后再转换数组) · [测试报告](docs/test-report.md#ordered-projection-20261118) · [性能报告](docs/performance-report-2026-10-30.md#ordered-projection-cost-20261118)

## 2026-11-18 · 能把更多查询结果写入表

部分 `INSERT … SELECT` 语句现在可以先连接多张表，再写入结果，并保留数组的下标、类型和空值。用 `LIMIT` 指定写入几行时，取够就停止。

[使用示例](docs/MANUAL.md#从包含表连接的子查询写入) · [测试报告](docs/test-report.md#nested-insert-20261118) · [性能报告](docs/performance-report-2026-10-30.md#nested-insert-cost-20261118)

## 2026-11-18 · 按整数数组匹配时少检查一些行

用整数数组连接两张表时，数据库可以先找出可能匹配的行，减少逐行比较。多条记录匹配时仍会各自返回。内存不足时会继续逐行比较。

[测试报告](docs/test-report.md#array-lookup-20261118) · [性能报告](docs/performance-report-2026-10-30.md#array-lookup-cost-20261118)

## 2026-11-18 · 整数数组连接少做重复工作

用整数数组连接表时，可以更早跳过不匹配的行，省去重复解析数组和复制整行。普通查询写入也可原样复制外联接产生的同域空值，目标列自己的 `NOT NULL` 仍会检查。[测试](docs/test-report.md#integer-array-join-20261118) · [性能](docs/performance-report-2026-10-30.md#integer-array-join-cost-20261118)

## 2026-11-18 · 连接表时保留数组类型

用 `JOIN … USING` 连接数组列时，结果保留正确的数组类型、长度限制和列来源。两列使用同一种数组规则时，原样复制已经检查过的值可省掉重复检查。不同的基础数组类型需要先显式转换。[使用示例](docs/MANUAL.md#用数组列连接表) · [测试](docs/test-report.md#array-using-20261118) · [性能](docs/performance-report-2026-10-30.md#array-using-cost-20261118)

## 2026-11-18 · 修复子查询中的数组读写和排序

读取或复制子查询的数组时会保留类型、下界和空值。排序和去重按元素的实际值处理，比如 `{2}` 排在 `{10}` 前面。通过视图复制数组仍有已知问题。[使用示例](docs/MANUAL.md#从子查询读取和复制数组) · [测试](docs/test-report.md#derived-arrays-20261118) · [性能](docs/performance-report-2026-10-30.md#derived-array-cost-20261118)

## 2026-11-18 · 给数组加默认值和检查规则

可以把一套数组默认值和检查规则用在多张表里。非法数组默认值会在设置时报错。原样复制已通过检查的数组时会保留检查结果。对现有非空表添加这种列，以及修改域规则，仍属于待实现的操作。[使用示例](docs/MANUAL.md#给数组设置默认值和约束) · [测试](docs/test-report.md#array-domains-20261118) · [性能](docs/performance-report-2026-10-30.md#array-domain-cost-20261118)

## 2026-11-17：修复查询结果中的名称显示

类型和表引用的查询结果显示对应名称，保留内部排序、去重和临时结果使用的编号。补齐引用数组比较、临时表字段类型和父子表统一排序分页，增加客户端、事务和重启回归。当时的数组域、视图写入、继承 CAST 和来源属性差异见该批记录；本次测量未证明稳定提速。[测试报告](docs/test-report.md#stored-reference-output-20261117)、[性能报告](docs/performance-report-2026-10-30.md#stored-reference-output-cost-20261117)。

## 2026-11-17：减少查询中未使用字段的处理

表达式省去未引用字段的 REGTYPE/数组转换，保留已引用字段的转换时机；生成列、输出强制转换和未知语法节点采用原有处理。新增按需取列接口、真实类型/生成列测试，以及保留全部预期的函数参数缺口脚本。原有参数问题仍开放，该次记录的裸引用显示问题见上方后续修复。[测试报告](docs/test-report.md#scalar-storage-demand-20261117)、[性能报告](docs/performance-report-2026-10-30.md#scalar-storage-demand-cost-20261117)。

## 2026-11-17：减少函数查询的额外开销，修复目录写入故障

函数查找改为只复制需要的 schema 列表。修复内存不足时可能把不完整目录内容写入文件的问题，新增快照、回滚和内存故障测试。选定查询和重启对照完成；原有函数存储测试失败及与 PostgreSQL 的差异仍保留。[测试报告](docs/test-report.md#namespace-snapshot-20261117)、[性能测试报告](docs/performance-report-2026-10-30.md#namespace-snapshot-cost-20261117)。

## 2026-11-17：补充数据读取测试和性能测试结果

新增 22 项数据读取检查，覆盖空值、空字符串和部分字段读取等情况。尝试的两种优化未能带来稳定提速，因此保留原来的实现。报告列出了全部结果，包括变慢的查询，以及此前已有、仍未修复的长文本存储测试失败。[测试报告](docs/test-report.md#row-directory-reuse-20261117)、[性能报告](docs/performance-report-2026-10-30.md#row-directory-reuse-cost-20261117)。

## 2026-11-17：修复损坏数据被当成正常值返回的问题

读取字段时，如果记录字段位置的信息已损坏，现在会报错，避免返回空字符串或错误文本。新增损坏数据、空值、事务和重启测试；没有运行完整测试套件。性能测量有快有慢，波动较大，暂不能确认提速。详见[测试报告](docs/test-report.md#variable-row-directory-20261117)与[性能报告](docs/performance-report-2026-10-30.md#variable-row-directory-cost-20261117)。

## 2026-11-08：记录并发访问中尚未修复的问题

检查了多个事务争用数据、内存不足，以及准备提交的事务移交锁时的行为。发现的问题尚未修复，测试方法、结果和限制见[测试报告](docs/test-report.md#native-lock-fault-review-20261108)。

## 2026-11-08：普通表达式和行格式差距复核

扩展 PostgreSQL 对照，记录函数参数副作用、活动查询中的表结构修改及项目私有变长字段损坏的错误成功值；这些缺口均未修复，实测范围和限制见[测试报告](docs/test-report.md#ordinary-scalar-effects-20261108)。

## 2026-11-08：行读取页帧生命周期

按行号读取时在复制数据结束前保持缓冲页固定，并在普通和可见行读取的异常路径释放 pin。新增针对多页、NULL 和分配失败的原生回归；协议与冷重启对照、尚未解决的并发边界见[测试报告](docs/test-report.md#row-read-page-pin-20261108)。有限查询性能对照波动较大，不能判断性能变化，详见[性能报告](docs/performance-report-2026-10-30.md#row-pin-query-cost-20261108)。

## 2026-11-08：零宽查询和函数参数诊断

补充与 PostgreSQL 的零宽结果、TOAST 需求及函数参数反例；所测写入／冷重启控制和未完成范围见[测试报告](docs/test-report.md#zero-projection-argument-audit-20261108)。投影语法缓存候选未证明收益并未采纳，计时与限制见[性能报告](docs/performance-report-2026-10-30.md#projection-syntax-rejected-20261108)。

## 2026-11-08：numeric 十进制对数

为 numeric `ln`、`log` 和 `log10` 引入十进制计算，补充协议、默认值、回滚与冷重启回归。已测范围、1000 位限制和诊断差异见[测试报告](docs/test-report.md#numeric-log-decimal-20261108)；新旧查询性能测量波动及负向结果见[性能报告](docs/performance-report-2026-10-30.md#numeric-log-cost-20261108)。

## 2026-11-08：PostgreSQL 小表查询对照

补充 PostgreSQL 18.6 与 D43 单连接小表查询的有限性能对照，包括未达计时门槛的查询及共享主机限制；原始记录留在本地。[性能报告](docs/performance-report-2026-10-30.md#pg18-d43-cross-20261108)。

## 2026-11-08：开源仓库清理

将 benchmark 脚本、原始数据、压缩证据及其生成索引移出 Git 跟踪并加入忽略规则；本机已有文件保留。移除误跟踪的 Python 字节码、日志和测试数据。保留项目代码、测试夹具、构建脚本和文字报告；历史提交中的大对象不会因新提交自动消失。

## 2026-11-08：内置函数签名缓存

内置函数的固定签名改为只读、进程生命周期的共享声明，避免在绑定和调用时反复创建；数组位置函数的参数相关签名仍逐次生成，同名函数覆盖和参数求值路径保持既有语义。选定回归与异常分配注入的范围见[测试报告](docs/test-report.md#d43-signature-cache-20261108)。

在共享主机上，同项目新旧版本的 64 行查询中，部分函数有改善，但旧版自身漂移较大，不能据此宣称全库提速；逐项结果与限制见[性能报告](docs/performance-report-2026-10-30.md#d43-signature-cache-20261108)。D43 的对数精度与零列缺陷当时仍未修复，详见测试报告。

## 2026-11-08：十进制指数与浮点函数类型

`exp(numeric)` 改用十进制计算，保留 PostgreSQL 18.6 的结果标度与中间舍入行为；`exp`、`ln`、`log` 和 `log10` 按参数类型区分 numeric 与 double precision 重载。此前 `exp(100::numeric)` 和 `exp(-40::integer)` 的错误结果可能被写入默认值，现有针对这些示例的查询及持久化回归用于防止再次发生。

本项目 Numeric 的有效精度和 scale 仍各限 1000 位，部分 PostgreSQL 可计算的结果返回 `22003`；D40 的 `ln`／`log` 高精度数值路径存在差距（当前状态见上方），D39 的部分 binary Numeric 输入边界在 D40 尚未重新验证。有限测试不代表全面 PostgreSQL 兼容；六条简单 SQL 的新旧测量波动较大，部分变慢，不能宣称全库提速。详见[测试报告](docs/test-report.md#d40-numeric-exp-20261108)及[性能报告](docs/performance-report-2026-10-30.md#d40-numeric-exp-cost-20261108)。

## 2026-11-08：大数格式与除法（D39 历史）

本轮修复部分大 numeric 定点／`V` 格式在展示补零或移位时触及项目内部精度上限的问题；限定的模板与类型现在按十进制舍入。整数长除法改为复用局部余数缓冲区，减少反复分配；不改变 1000 位上限。`exp`／`ln`／`log` 不再把部分超出 double 范围的 numeric 静默当成零，精确的 10 的幂采用逐位验证和范围检查。无法精确支持的部分输入会明确返回 `22003`，不是 PostgreSQL 等价。

**D39 历史风险：** `exp(100::numeric)` 曾与 PostgreSQL 不同，某些 integer／float8 的极小 `exp` 曾错误返回零，这些值可作为默认值持久化。后续实现及当前限制见上方最新状态；D39 并未解决完整十进制超越函数、默认值来源或复杂格式模板。

D39 正常冻结候选的 61 个选定原生入口与 27 组既有协议对照通过，完整套件未运行。针对定点格式的每版本 1,365 个动作涵盖直接查询、事务和冷重启，已测 `FMPL` 默认值与 PG 一致；复杂模板仍有差距。组件基准有 CPU 改善，真实 SQL 窗口有快有慢且漂移明显，不能声称整体数据库提速。详情见[测试报告](docs/test-report.md#d39-numeric-20261108)与[性能报告](docs/performance-report-2026-10-30.md#d39-numeric-cost-20261108)。



## 2026-11-08：NaN、科学数字格式与目录扫描

本轮修正 `div(NaN, 0)` 的处理顺序：NaN 先于零除数处理；有限值和无穷除以零仍报 `22012`。简单 `9`／`0`／小数点模板的 numeric 和 bigint `EEEE`／`eeee` 科学记数格式改为精确十进制舍入，避免极大或极小值因转换成二进制浮点而静默显示为零。integer 等其他类型仍沿原路径；含 `G`、引号、`FM` 等复杂模板的既有差异未在本轮解决。

Linux 上，目录发布状态的一次扫描复用已打开的父目录句柄，仍逐个检查目录文件的身份和时间戳，并在结束前核对路径仍指向同一目录；描述符不可用时回退旧检查路径，非 Linux 保留旧路径。扫描期间目录换出后又换回等竞态仍不能保证检出，不能称作跨进程线性一致。

D37 正常冻结候选的 58 个选定原生入口实际退出 0；27 组既有协议的 1,105 对回复没有未解释变化。NaN 的 82 个动作在 PG18.6 对照中结果、SQLSTATE 和事务状态一致；简单科学格式的 210 个样例与 PG 的结果及描述符一致。完整套件未运行，其他模板、无约束 numeric 范围、错误回复帧及默认值来源仍有差距。此前文档称 `div(NaN,0)` 不同属于历史结果，已被本节覆盖。详情见[测试报告](docs/test-report.md#d37-numeric-catalog-20261108)和[性能报告](docs/performance-report-2026-10-30.md#d37-catalog-scan-20261108)。



## 2026-11-08：数值范围和 to_char 默认值

本轮修正部分 numeric 范围错误被 DEFAULT 当作字面量回退的问题：已求值的算术范围异常转换为 `22003`，整数商和余数使用直接整数除法，避免舍入或过大的中间乘积；`to_char` 已绑定的数值格式化范围异常也转换为 `22003`。这些处理仅覆盖对应异常和已测路径，其他无结构错误仍可能回退原文。旧式原生默认值兼容有变化：原文恰为这些会越界的表达式时，现在报错。

PostgreSQL 18 的无约束 numeric 支持的范围远大于项目当前 1000 位内部上限；合法输入、中间值和结果都可能触及项目限制。[PostgreSQL 数值类型](https://www.postgresql.org/docs/18/datatype-numeric.html)。`div(NaN, 0)` 的既有差异、其他函数的未结构化错误回退、目录锁等待和 `pg_attrdef.adbin` 等差距仍未解决。旧 D30 正文对所测算术默认值“写入原文”的描述属于历史结果，已由本节局部修复；不代表所有默认值问题均已修复。

D35 冻结候选 55 个标准原生入口与 1 个二进制入口均实际退出 0；11 组数值和默认值协议对照、27 组 D34→D35 逐动作比较和选定 SSI 对照已完成。`to_char` 大数默认值现返回 `22003`，已测 INSERT、UPDATE、批量插入和 ADD COLUMN 不再写入 SQL 原文；PostgreSQL 对该合法输入返回格式化值，仍不等价。未运行完整测试套件。详细计数、旧失败与性能测量限制见[测试报告](docs/test-report.md#d35-numeric-tochar-20261108)及[性能报告](docs/performance-report-2026-10-30.md#d35-numeric-tochar-performance-20261108)。



## 2026-11-08：D30 目录投影与活动事务退出（草稿）

本轮对普通 `pg_attrdef`／`pg_depend` SELECT 使用实际目录字段的按需快照，声明与数据在同一把锁下复制；公共完整快照仍保留默认表达式和依赖。`pg_attrdef.adbin` 的 PostgreSQL 节点树仍未实现，不声称目录完整等价。活动事务与 SSI 登记改为进程寿命，避免全局数据库对象退出时访问已析构的登记容器。

D28 新投影测试因误以为 ALTER 后目录行顺序不变而断言失败，原进程真实退出 `-11`；隔离 ASan 运行时拦截重放实际退出 `-6`（未强杀），报告活动事务登记容器在退出时二次释放。D29 修正测试的列身份断言，但未修生产退出问题；D30 对普通及 SERIALIZABLE 活动事务正常退出、重启后未提交行不可见／已提交前缀保留、后续写入提交作了实际控制，普通和 ASan runtime-only 均实际退出 0。后者仅加载 ASan 运行时库，不是全核心插桩；记录保留原有失败。

D30 实际通过 52 个选定标准原生入口及 1 个独立二进制入口，合计 53；23 组协议与 D28 的 994 条成对回复在行、tag、SQLSTATE、Ready、T7 和错误／通知上相同。针对投影的 PG／D27／D28／D30 热冷对照共 76 条回复，按实际 OID 对应后选定行、tag、SQLSTATE、Ready 无差异，但 PG 原始行描述符 T7 仍有 4 项差异。完整发现审计的 2,023 条回复包含历史和参照，不能说成 2,023 条新测试；未运行完整测试套件。

已知缺陷仍未修复：合法默认表达式 `(('1e500'::numeric * '1e500'::numeric)::text)` 在 PostgreSQL 18.6 得到 1001 位结果，而 D27／D28／D30 会把 SQL 源文写入 TEXT；现有无约束 numeric、持久默认值来源和无 SQLSTATE 回退均需继续修复。并发对照中 PostgreSQL 可在另一会话 DDL 未提交时立即读取旧目录，D27／D28／D30 的同类查询等待超过 5 秒，ROLLBACK 后才返回；未采集线程等待栈，库级锁路径只是依据代码和时间顺序的推断。

仅对选定单连接暖缓存工作负载测量：D27→D30→D27 固定顺序的目录读取窗口为 27 个实际正常退出进程、27,648 条 SQL；事务窗口为 9 个进程、3,456 条计时 SQL，另有 324 条控制 SQL。两窗口完整回复字节一致、无 mismatch。各场景结果有正有负，ordinary `pg_attrdef` 相对前／后基线墙钟分别退化 5.8%／6.5%，两项生产变更同时存在且每角色仅 3 次，不能据此归因或证明投影改善，也不能宣称整体提速或与 PostgreSQL 性能等价。


## 2026-11-08：DEFAULT 错误与目录快照（D27，未接受草稿）

本轮修正 INSERT 遗漏列 DEFAULT 求值中已有 SQLSTATE 或有效结尾 SQLSTATE 标记的错误传播，并对 substring 负长度保留 `22011`；目录声明读取按需省去 DEFAULT 对象图复制，公共默认快照仍完整。没有 SQLSTATE 且没有有效标记的旧/native 求值失败仍可退回原始字面量；域、旧数据及全部错误类型不构成全面兼容承诺。

D24 的 `catalog_default_snapshot_demand_test` 实际退出 `-11`，诊断指向进程退出时内置目录声明生命周期问题；D27 的 ASan 只注入运行时库而非全核心插桩，该控制正常退出 0，不能称为完整 ASan 构建。D26 的 `regexp_replace('abc','[','z')` DEFAULT 在热态和冷重启后均错误写入表达式文本；D27 返回 PostgreSQL 的 `2201B` 且未写入行。历史失败记录保留。

最终 D27 49 个常规原生入口加 1 个二进制控制，共 50 个实际退出 0；22 个协议族独立审计覆盖 1,998 SQL、76 个正常结束 Main。五族 DEFAULT 错误独立对照记录 342 SQL、48 个正常结束 Main，所比较语义字段没有候选差异；两个族的原始行描述符 T7 仍有差异，不能称为完整 wire 或 PostgreSQL 等价。未运行完整测试套件，功能仍为未接受草稿。 指定成本测试为共享服务器暖缓存、单连接、32 行热表：D23 与 D27 按每场景旧版前/新版/旧版后各 3 个进程固定顺序执行，共 27 个正常结束 Main、36,720 条 SQL；各场景完整回复字节一致。24 组墙钟和 CPU 结果有正有负，narrow 前后基线波动尤大；没有证明整体加速，也没有 PostgreSQL 速度对比。 [完整证据](docs/performance-report-2026-10-30.md#default-runtime-snapshot-evidence-20261108)。


## 2026-11-07：DEFAULT 对象依赖（P43/D23，未接受草稿）

- 增加独立的持久默认值身份、所属列自动依赖和真实对象常量正常依赖；SET DEFAULT更换身份，DROP DEFAULT／删列／删所属表清理对象，列压缩时重映射所属序号。
- 绑定REGCLASS／REGTYPE常量及实际内建nextval参数，保留显式TEXT的运行期查找；选定RESTRICT／CASCADE、SERIAL、NULL默认值、回滚和冷读通过新回归。
- 46项原生入口actual0、两份正式协议脚本通过，正常性能NOTRUN。原始大JSON仍仅跟踪gzip，另清理build冗余副本。[证据和未完项](docs/performance-report-2026-10-30.md#attribute-default-objects-draft-20261107)。


<!-- P43-D20 unaccepted draft checkpoint -->
## 2026-11-07：旧物理数组目录与首次启动修复（P43/D20，草稿）

- 直接读取持久schema核实数组元素，完整预检后同步真实数组OID、物理属性和已有类型依赖；保留长度修饰符、统计设置及合法存储／排序规则，用户定义类型保留其目录声明。纯冷读仍显示原始存储值。
- 修复全局引擎早于动态类型映射初始化时查找返回0的问题；新增真实新进程启动回归。发布后的冲突拒绝修复，失败不暴露部分修改。
- 42个选定原生入口、正式35／9条协议回归及真实旧库21条操作链通过。D19原断连失败、初版审计器错误均保留。正常性能NOTRUN，原273项不关闭；[证据与边界](docs/performance-report-2026-10-30.md#physical-array-catalog-upgrade-draft-20261107)。


<!-- P43-D18 unaccepted draft checkpoint -->
## 2026-11-07：P43/D18 草稿版本检查点（未接受）

- 保存身份列 DEFAULT／序列类型参数／REGCLASS 值和系统目录实现草稿；校正实际目录的6个 `C` 排序规则字段，并从真实绑定来源生成 `pg_type` 的协议来源信息。
- 39个选定原生入口、35条身份冲突与9条目录正式协议回归实际通过；全套未运行，正常性能 `NOTRUN`，不关闭功能项。
- REGCLASS 默认依赖、旧数组元数据迁移、序列完整生命周期、生成列 ALTER TYPE 和其他协议／恢复缺口继续开放。默认程序不变、未推送。证据与边界见[报告](docs/performance-report-2026-10-30.md#identity-sequence-catalog-draft-20261107)。


## 2026-11-07：无写入事务提交优化（P42，partial）

- 为同一WAL owner记录事务起始位置，严格无变更条件下省去无用COMMIT／CLOG发布；写入、DDL、序列与TEMP终止动作保留保守路径。
- 新正式原生回归验证零WAL、完整数据、并发快照、真实fsync和新进程恢复；95条SQL旧新逐字节相同，33个选定原生入口仍有2项未通过。
- [性能报告](docs/performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)保留621次新正常时钟、135行比较及全部负向；八份原文只插入新块，新增压缩证据，校验后清理本批副本。原273状态与所有用户暂缓保持，默认程序未提升，Goal active。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## 2026-11-07：VARCHAR声明／长值与TOAST搜索

TOAST每次写入一个长值时，等长2KB分块从本次上一个成功页继续搜索，避免每块重新扫描前面的已满页；较短末块从头寻找空洞，插入／索引失败仍保留原回退。游标不跨值保存，页格式、Fletcher-16校验、zlib、WAL／CRC32C、锁与持久化屏障保持。这个局部游标没有实现PostgreSQL关系级FSM或bulk-insert完整策略。

本批功能有进展，长值写入部分提速；全面提速尚未成立。新增1MiB混合插入墙钟中位1801.508072ms，相对旧bf9首测／同旧回测-10.720%／-10.799%；更新RETURNING 411.759537ms，-13.372%／-12.280%；重新插入1431.222130ms，-9.197%／-10.414%。对应服务器CPU分别-12.689%／-12.822%、-20.934%／-21.267%、-12.666%／-12.762%。

负向保持OPEN：新长值DELETE RETURNING墙钟43.553088ms，+9.954%／+23.830%，服务器CPU+67.962%／+39.067%；首次VACUUM墙钟25.753918ms，+22.574%／+1.265%；FULL墙钟218.226818ms，+32.204%／+21.215%，服务器CPU+7.756%／+1.774%。新增完整值读取墙钟+2.080%／-2.014%，结果混合。通用270项矩阵的30个区域，墙钟6低于两旧／18高于两旧／6居间；服务器CPU10／8／12，客户端CPU6／14／10。没有排除、重采或把漂移归因为已证实的产品原因；全部负向与原因继续开放。

旧typeMod=-1按原dsize字符上限读取，不从65535猜测成无界；新显式正长度和私有-2保存在已有ITM1扩展，-2输出PG时为-1。旧数据可以由新版读取；旧程序没有识别这些新VARCHAR修饰符，不能承诺降级读取新写的schema。旧无界意图缺少可判定来源时不自动迁移，须明确新DDL／ALTER。名义dsize、SQL字符数和实际UTF8字节数分别处理。没有改变元组布局、校验、TOAST指针格式或WAL格式；仍是项目自定义存储，不是PG物理格式等价。

原273状态不变，新增partial历史291。[完整报告](docs/performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。Goal active。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 2026-11-07：FULL限定关系刷盘与可回退硬链接备份

普通VACUUM FULL在备份前与重建后，只刷新目标逻辑关系及其分区／TOAST缓存，避免把其他表的脏页一并刷出。物理文件已同步，且在任何重建写入前全部unlink：这些文件优先用硬链接保留旧inode作为回滚映像，失败则完整复制；schema和specialized-dirty元数据继续独立复制。每份备份的fsync、完整大小／模式／SHA256校验，以及manifest、pending、替换文件／schema、WAL、COMMIT和undo的持久化顺序全部保留。本批只改TableManage.cpp的vacuumFull，没有修改全局缓存、index_file、WAL格式、CRC或恢复授权规则。

本批只证明固定字段FULL墙钟低于两侧旧版：-11.57%／-1.51%；单长／多长FULL服务器CPU均较低，墙钟各为-3.35%／+1.84%、-1.35%／+3.87%，结果混合。没有证明数据库全面提速，也没有追回P39相对更早84bb的全部恢复成本。

明显负向保持OPEN：首次普通VACUUM多长字段墙钟C1中位67.010218ms，旧c8ca首测30.951480ms／回测25.722458ms，即+116.501%／+160.512%。原三次样本分别为C1 [71.510152,26.679347,67.010218]ms、旧首测[35.992784,30.951480,25.821788]ms、旧回测[25.722458,23.927581,29.365169]ms，全部保留。单长字段大批重新插入的墙钟也+51.70%／+58.00%。未因负向重跑、排除样本或更改预期。

新增完整值／冷开／脏页owner／复制回退正式测试，原273状态不变，新增partial历史290，Goal active。[报告](docs/performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 2026-11-07：FULL物理代次与持久化回滚

修复普通VACUUM FULL仍沿用旧物理身份、冷启动把旧页镜像重放到压缩后堆的问题。每次重写分配新物理代次；同一内部事务记录出生、旧代次退休意图／完成及COMMIT。替换前先持久化只属于该关系的原文件、完整模式／大小／SHA256清单及pending记录，替换文件与schema全部落盘后才提交。失败和未提交崩溃先恢复旧文件／schema，再重放WAL；已提交只清理备份。完成回滚的durable receipt防止遗留备份覆盖后续已提交DML。旧09 schema仅在FULL成功时发布新代次及专门的0x23完成记录，普通旧名称重用仍拒绝错误归属。没有放宽CRC、物理身份或页缺口校验。

新增vacuum_full_cold_recovery_test使用同一源码对照：旧84bb两种情形均在首次冷开出现重复可见主键（父actual1，4个子阶段已执行／2失败／2后续NOTRUN）；C1为6个子阶段全部符合实际终态，正常关闭或主动SIGKILL -9后连续两次新进程冷开全值／NULL／主键通过。原堆352256字节（43页）压到32768字节（4页），之后真实UPDATE到1MiB TEXT、INSERT、DELETE并提交。两份WAL共1194条独立CRC32C核对，旧代次578个页镜像最大块42、新代次7个镜像，退休完成及提交顺序正确，0个检查点，不能归因于checkpoint绕过。

本批没有证明全面提速。270项原样读写矩阵保留27个全新数据库、54个正常Main及1098条完整回复。30个区域的墙钟：7低于两旧／13高于两旧／10居间；服务器CPU：5低于两旧／14高于两旧／11居间；客户端CPU：7低于两旧／12高于两旧／11居间。所有90项比较均列出，性能原因OPEN。 vacuum-full-fixed墙钟 +64.55%／+78.39%；vacuum-full-fixed服务器CPU +10.12%／+10.45%；vacuum-full-single墙钟 +78.30%／+112.77%；vacuum-full-single服务器CPU +17.97%／+38.94%；vacuum-full-overflow墙钟 +131.53%／+114.90%；vacuum-full-overflow服务器CPU +18.05%／+13.79%（相对旧84首测／同旧回测，正数为更慢）。

原273状态不变，新增partial历史289，Goal active。[报告](docs/performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：解开标量TEXT逻辑值与名义堆宽度

修复标量TEXT把持久化名义宽度65535误当逻辑值上限的问题：统一的字符字节限制函数对非数组TEXT使用1,073,741,819字节上限，并让旧式UPDATE校验也使用该函数。既有TOAST读回、插入和行构造因此一致处理完整长值；表结构dsize仍为65535，内联引用和存储格式不变。实际验证到1MiB，未进行接近1GiB的端到端容量验证。旧数据可由新程序读取；旧程序不能读取新写入的超64KiB TEXT，不能据此承诺降级后的值兼容。

新增large_text_toast_test在同一冻结源码上形成33检查：旧版21失败／actual1，当前0失败／actual0。全字节覆盖65535、65536、70003、UTF-8 90012字节／30006字符、可压缩131076字节、随机1MiB、NULL、空串、文本NULL及类似marker的普通串；同时验证第二长字段100003字节、旧式和optional更新的完整返回值、事务回滚／提交、普通VACUUM、热态VACUUM FULL、TRUNCATE后重插1MiB，以及有界VARCHAR的字符数和尾空格约束。

本批完整1500次正常计时没有证明全面提速。仅ID查询服务器CPU比旧版首测／回测高1.78%／1.86%；单长字段首次VACUUM CPU高10.98%／13.76%，墙钟高3.66%／9.08%。多长字段首次VACUUM墙钟低18.80%／高3.52%，CPU低0.33%／高3.27%，属于混合结果。全部负向、旧版漂移、首轮与长尾保留，原因OPEN；本批交付为TEXT容量修复，没有用局部结果宣称整体性能改善。

另发现旧／新均存在的普通FULL恢复缺陷，未关闭；273项状态不变，新增partial历史288。[完整报告](docs/performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：使用所属物理元组识别TOAST引用

修复普通 VACUUM 的长字段引用收集：在原有页锁和 pin 内，直接读取所属物理元组的 NULL 位图、变长字段目录和有界字节跨度，不再通过可能读取调用者 RID/NULL 状态的执行器取值接口。固定区大小与变长列索引每次维护预先计算，普通内联值无需复制整行或逐值构造字符串。活动快照保护、所有物理存活槽的遍历、锁／pin、索引／分块校验和回收策略保持。

新增 vacuum_toast_physical_reference_test 在同一冻结源码上执行全部16种情形：旧版160检查／48失败／actual1，C1为160／0／actual0。覆盖普通表与原生范围分区存储、vacuumToast及外层vacuum、真实同库／跨库同名表调用者、跨位图字节的NULL、空串、文本NULL、UTF-8、类似marker的普通字符串、TEXT11003／40003及BYTEA70003／11009。独立完整值物理解码核对16种情形中的80个存活对象全部原始字节，以及每个真实孤儿的回收、调用者绑定恢复和第二次维护幂等。

本批1500项完整正常计时没有证明全面提速。在共同的真实after-DELETE种子上，单长字段首次VACUUM服务器CPU比旧版及旧版回测低8.36%／7.73%；多长字段首次VACUUM墙钟高9.83%／27.97%，CPU分别低1.92%／高35.29%。在共同after-UPDATE种子上，仅选ID的CPU高8.43%／8.39%，墙钟高9.77%／14.77%；三个全列查询墙钟也高于两次旧版，完整正负结果保留，原因OPEN。

原273项状态不变，新增partial历史287；既有toast_test失败和PG差异保留。[完整报告](docs/performance-report-2026-10-30.md#vacuum-physical-references-20261107)。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：普通物理扫描／排序投影 NULL 元数据复用

扫描展开长字段时，借用该物理行已捕获的 NULL 位图；排序后的投影在核对真实来源、具体 Sort 类型和全部物理列声明后，借用排序已保存的位图。借用仅存在于一次取行调用内，退出与异常均恢复原绑定；表结构不匹配、Sort 子类及其他来源继续原路径。没有新增页／allocator 缓存或改变文件身份检查本身。

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

原273项及暂缓范围保持，新增1156项检查与完整990操作开销证据。[性能报告](docs/performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
## 2026-11-06 — 视图关系／列依赖与破坏性 DDL 保护

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

合法编译目录读来源由完整查询绑定校验，别名保留实际列身份；旧视图依赖只读恢复。新增原生 242 检查、7 既有专项、真实 PG／旧库／保存点对照通过有限范围验证；[报告](docs/performance-report-2026-10-30.md#view-column-dependencies-20261106)保留 270+27 操作全部开销及退化。本批没有整体性能提升结论，PG 家族仍 partial、Goal active。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
## 2026-11-06 — 普通部分列投影按需求读取 TOAST

- 仅为直接物理游标的部分列投影解码所需字段；维持选中长字段错误、NULL 与虚拟列完整输入。全列使用原路径。
- 新增 14 组/83 项回归，并复核 5 项相关原生测试及全部普通协议回复。
- 完整保留 C1 回退及 C2 的各 270 项正常性能对照、漂移和长尾；更新[性能报告](docs/performance-report-2026-10-30.md#projected-toast-demand-20261106)，证据仅以压缩归档提交。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
## 2026-11-06 — owned pg_class declaration (partial)

- Persist the actual `pg_class` identity, all34 attributes, rowtype83/array273 and required metadata-only types through the existing locked startup owner. Validate loaded declarations/dependencies and reject conflicting bootstrap input before edits.
- Use the PostgreSQL18.6 `aclitem` declaration (16 bytes, alignment `d`); keep unavailable value/result codecs explicit, including NULL input `0A000`.
- Add the550-check native regression, full unfiltered41-query PG comparison, real old-image upgrade/cold evidence and one complete270-timing control cohort. Performance regressions, virtual origins and remaining catalog/value functionality stay open. [Report](docs/performance-report-2026-10-30.md#pg-class-declaration-20261106).
<!-- P33 pg-class-declaration end -->

本项目的所有显著变更记录于此文件。
格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

- 普通日志以每次O_APPEND/writev追加，保留原字节、权限、轮转及打开错误行为；新增13组原生回归。正常30案例服务器CPU为16低/6高/8居间，多LOB回退及原9项协议差异保持，未证明一致提速，默认程序未替换。见[完整报告](docs/performance-report-2026-10-30.md#legacy-log-append-20261106)。

- 补充真实主键查询CPU诊断：同一三表闭库状态、三个正常进程、1,536次完整目标查询及1,575响应独立复核；原始PC/符号与全wire压缩归档，保留P30回退和目录差异。明确pg_class完整声明的类型准入依赖，本批没有生产修复或提速验收。
- 整理文档入口：补齐 754 份 Markdown 的分类清单，近期重复状态段落集中到审查索引；增加本地文件链接、空文档、整份重复与清单覆盖检查工具，明确当前验证与历史记录的边界，并同步手册中的排序取消检查范围。
- 修复检查点截断后首个保留 WAL 段从记录中间开始时无法重启的问题：利用现有已持久化检查点和经过 CRC/链衔接检查的前驱记录确定恢复起点，继续校验保留的完整记录；缺少有效锚点的非对齐截断在删除前拒绝。增加跨段重开及损坏锚点/记录回归。
- 优化数据库写入和维护路径：CLOG 原子发布跳过全零块并合并连续数据块，使用文件身份和时间戳校验缓存；DML、约束校验和 DDL 重写复用已有表结构读取 NULL 位图；在实际持有完整 DDL 回滚快照时消除重写行的重复整库保存点；绑定 UPDATE/DELETE 的同类型整数等值条件先过滤候选，再执行原类型表达式。协调后台刷盘、自动 VACUUM、CHECKPOINT 与 DROP DATABASE，重叠的同一引擎 CHECKPOINT 复用实际结果，保留活动用户事务拒绝及持久化错误传播。增加[性能测试报告](docs/performance-report-2026-10-30.md)与可复现基准脚本。
- 优化查询热路径：目录文件未变化时跳过全目录序列化，扫描复用已有表结构读取 NULL 位图；同类型 SMALLINT/INTEGER/BIGINT 单列主键参数等值查询可使用物理索引，旧快照、自身写入及重叠事务继续使用堆扫描并提前过滤整数键；整数等值 JOIN 在构造组合行上下文前跳过不匹配键，保留原表达式复核、读取顺序及事务可见性检查。
- 增加全仓库代码审查、可复现 SQL/协议问题及逐项修复方案；文档索引区分当前结论与历史证据，修正文档中的旧测试数量、复制示例和失效链接。审查记录本身不代表产品问题已修复。
- 已合入引号类型名称归一化修复，以及递归 SQL 执行期间 Session 指针的异常安全恢复；具体范围和验证见对应 issue 文档。
- 修复 NUMERIC 声明精度/scale 在存储和赋值中丢失的问题，覆盖标量及数组元素舍入、溢出检查、ALTER 重写和协议类型修饰符；支持负 scale 与大于 precision 的 scale。裸 NUMERIC 与旧无修饰符声明仍保持原语义。
- 带 NUMERIC 修饰符的声明也使用 schema 格式 `0x4442000C`；仅支持 INTERVAL 修饰符的早期 C 格式读取器不能读取这类 NUMERIC 声明。请保留升级前备份，不要直接降级打开已写入新声明的数据目录。
- 修复 INTERVAL 列字段范围和小数秒精度在 INSERT、UPDATE、数组元素及 ALTER TYPE 中丢失的问题，并为实际表列返回对应的协议类型修饰符。
- 带 INTERVAL 修饰符的表声明使用 schema 格式 `0x4442000C` 保存精度；未带修饰符的旧声明仍可读取且保持原格式。写入新格式的声明不能由旧版程序读取，请保留升级前备份，不要直接降级打开这类数据目录。
- PostgreSQL 18 兼容审计仍在逐项执行；完成状态只以 `docs/postgresql-18-gap-audit.md` 与 `docs/gap-progress.json` 为准。
- GitHub Actions workflow 按维护要求保持 `.disabled`；`scripts/build_tests.sh` 是本地完整验证入口。

## [0.2.0] - 2026-08-22

并发硬化与写放大根除（"concurrency hardening"）。
Tag 内容：156 个 `*_test.cpp` 测试源 + 7 个 Python E2E；发布时记录崩溃恢复矩阵 12/12、ASAN/TSAN 核心集 CLEAN。

### 并发正确性
- TypeRegistry 单例 bootstrap 竞态：`static bool` 旗标不受 Meyers 单例保护，两线程并发 bootstrap 重写注册表——TSAN 1288 条报告的单一根因，`call_once` 修复后 0 报告
- stats-map 擦除竞态：closeDatabaseCaches 无锁擦 deadTupleCounts_/modifyCounts_ 与连接线程读写竞争，补对应 mutex
- 事务级堆页 flush：commit 只刷本事务写过的页（仍在事务页锁下，无竞态无死锁）；曾试 bgwriter 逐页加锁（锁序死锁,array_test 复现）已否决
- LockManager 锁序反转定性为设计权衡（调用协议文档化）

### 性能
- TCP_NODELAY：服务器 socket Nagle+delayed-ACK 造成每语句 41ms 固定地板（含空查询），修复后 SELECT 1 0ms / INSERT 19ms
- WAL group commit：fsync 队列与插入锁解耦，N 个串行 fsync 合一
- 索引 WAL 图像按 checkpoint 纪元去重：每 commit 整索引文件×2 写 WAL（实测 40 字节 INSERT 写 120KB WAL）→ 同纪元只写一次；WAL 量降两个数量级（70MB/30s → 120KB/60s）
- auto-vacuum 移出语句路径：阈值(50 dead tuples)触发的同步全表 vacuum 曾把全部并发语句串到 30s+ 饿死；改为后台循环执行
- commit 去掉全池堆页 flush（jbd2 world-stop 主放大器，207MB/90s）

### 运维定性
- 残余 ~30s 周期停顿取证结案：worker 单次 fsync 在 ext4 jbd2 等待 25s,由宿主机邻居进程周期性大写入（iostat 225 w/s burst）拖慢 jbd2 提交所致——部署要求独占 WAL 卷,非引擎缺陷
- kill-storm 验证方法论：必须带 commit 确认屏障（无屏障会把 kill-before-commit 误判为丢行）

## [0.1.0] - 2026-08-21

首个公开发布版本（v0.1.0 "first public cut"）。
Tag 内容：156 个 `*_test.cpp` 测试源 + 7 个 Python E2E；发布时记录全绿。

### 核心引擎
- 堆表存储：8 KiB PostgreSQL 风格页布局（页头 + line pointer + tuple 从页尾生长），Fletcher-16 页校验，Free Space Map
- BufferPool：Clock-Sweep 驱逐、pin/usage 分离、并发加载单加载者规则、孤儿帧语义；堆池 256 帧/索引池 128 帧可配
- WAL：段式 WAL、CRC 校验、记录链验证、增量尾部状态、崩溃恢复页像重放（redo/undo）、XID 事务提交日志
- MVCC：快照隔离、事务级读写集合、页级 SIREAD、单列 B+Tree 逻辑索引谓词（SERIALIZABLE 部分实现，见已知限制）
- TOAST：线外大字段存储，zlib 压缩，chunk header 保留压缩标记与原始长度
- 事务：BEGIN/COMMIT/ROLLBACK、SAVEPOINT、语句级隐式事务、DDL 快照回滚、两阶段边界
- 锁：行级/表级/意图锁、死锁检测、锁超时、简化 gap 锁

### SQL 兼容面
- DDL：CREATE/DROP/ALTER TABLE（含分区、继承、表空间）、VIEW、SEQUENCE、DOMAIN、TYPE、FUNCTION（PL/pgSQL）、TRIGGER、INDEX（B+Tree/Hash/GIN/BRIN/SP-GiST/Bloom）、POLICY（RLS）、PUBLICATION、REPLICATION SLOT、STATISTICS、CAST、CONVERSION、AGGREGATE、EXTENSION 兼容对象
- DML：INSERT（VALUES/SELECT/DEFAULT/ON CONFLICT）、UPDATE（FROM/RETURNING）、DELETE（USING/RETURNING）、MERGE 窄版、RETURNING（受限表达式）
- 查询：SELECT 全语法（WHERE/GROUP BY/HAVING/ORDER BY/LIMIT）、JOIN（INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI）、子查询（IN/EXISTS/ANY/ALL/标量）、窗口函数（排名/偏移/聚合 + ROWS/RANGE/GROUPS frame）、UNION/INTERSECT/EXCEPT、ROLLUP/CUBE/GROUPING SETS、CTE、LATERAL
- 执行器：Volcano 风格结构化算子（SeqScan/IndexScan/BitmapHeapScan/HashJoin/MergeJoin/NestLoop/GroupAggregate/Window/SetOp/SemiJoin 等），复杂路径回退 legacy
- PL/pgSQL 最小运行时：变量、控制流、EXCEPTION 之外的游标
- 协议：PostgreSQL wire protocol v3（SCRAM-SHA-256 认证、Parse/Bind/Execute、扩展查询）

### 近期完成的特性（2026-08 批次）
- P1-7 PL/pgSQL 运行时（最小实现）
- P1-8 并行 Vacuum（leader/workers 分索引回收）
- P1-9 自定义代价函数（CostModel 钩子 + GUC 同步）
- P2-2 Bloom 索引访问方法
- P2-4 PgBouncer 式连接池（session/transaction/statement 三模式 + 3 GUC + SHOW POOLS）
- P2-5 逻辑解码（pgoutput 帧式 + test_decoding 文本双输出插件、PublicationCatalog、按槽变更流、CREATE/DROP PUBLICATION|REPLICATION SLOT + 4 个 SHOW 面）
- P2-8 TDE 透明数据加密（SHA-256-CTR + EtM 页级加密、边车信封 `<file>.tde`、keyring 0600、WAL 崩溃恢复兼容）

### 发布工程（本批次新增）
- 版本号单一事实源：CMakeLists project version ↔ src/common/version.h ↔ CHANGELOG.md（package.sh 打包前校验一致）
- `dbms_main --version/-V`
- CI：scripts/ci.sh（全量构建 + 回归 + E2E）+ GitHub Actions workflow
- Sanitizer 例程：scripts/sanitizer.sh（ASAN/TSAN 编译跑核心并发/存储测试）
- 崩溃恢复矩阵：tests/crash_matrix_test.sh（kill -9 × {WAL, TDE, 连接池} × commit 前/后）
- Soak 负载：scripts/soak.sh（可配置并发/时长的多客户端压测）
- 打包：scripts/package.sh（源码 tarball，含目录约定）

### 已知限制（v0.1 明示）
1. **DML 双执行入口**（P1-0）：复杂 INSERT SELECT、部分/索引推断 conflict target、引用子查询或其他关系的 ON CONFLICT DO UPDATE、复杂/子查询/窗口 RETURNING、外连接/复杂 UPDATE FROM/DELETE USING、多 WHEN MERGE、视图写入仍回退 legacy 字符串路径；错误码与行为一致性受限
2. **SERIALIZABLE 非 PostgreSQL 完整 SSI**：复合/表达式/部分索引及其他访问方法的精确 phantom 推理不完整；空结果关系级兜底牺牲部分串行化并发度
3. **TDE 覆盖范围**：未经 BufferPool 页路径的索引文件（.idx/.hidx/.fti 等）未加密；无密钥轮换、无 per-database 密钥隔离
4. **逻辑解码**：无 CREATE SUBSCRIPTION 拉取端；变更不经 WAL 重放（写路径直采），崩溃恢复后逻辑槽不 replay
5. **代价模型**（P1-12）：直方图/MCV 已采集但 planner 消费不全，范围选择率硬编码 0.3，EXPLAIN 行数估计有误导性
6. **并行执行**：仅并行表扫描（page range 分片 Gather）；并行 join/aggregate、GatherMerge 未实现
7. **prepared statements**（P1-10）：SQL 级语法为自有 `PREPARE name FROM 'sql'` + `?` + `USING`，非 PG 的 `(types) AS` + `$n`；无类型推断与计划复用
8. **统计收集器**：无独立后台采样线程；planner 深度反馈缺失

## [0.1.0 之前]
开发期 874+ commits 不在此列，演进细节见 docs/production-status.md 与 docs/all-gaps-todo.md。

</details>

[Unreleased]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/releases/tag/v0.2.0
[0.1.0]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/releases/tag/v0.1.0
