# DBMS C++

用 C++ 编写的关系型数据库。可以用 SQL 建表、查询和修改数据，也支持索引、事务和备份恢复。

[安装与运行](docs/MANUAL.md#1-快速开始) · [功能与限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>删除一张表时保留名称相近的其他表 · 2026-11-21</summary>

例如，删除 `orders` 后，`orders_items` 的索引和长文本仍可以读取。如果两张表已在共用同一个索引文件，数据库会拒绝删除。

</details>

<details>
<summary>部分查询能更快找出两列文字相同的记录 · 2026-11-21</summary>

在测试的单表查询中，如果建表时将一列设为按字符编码比较，另一列使用数据库默认规则，筛出两列内容相同的记录用时更短。

</details>

<details>
<summary>两列文字的比较规则冲突时会提示错误 · 2026-11-21</summary>

可以在查询里为两列指定同一种规则。空表里没有记录需要比较，查询会正常返回空结果。

</details>

[完整更新日志](CHANGELOG.md)
