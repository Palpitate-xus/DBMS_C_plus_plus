# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引、事务以及数据备份和恢复。

[快速开始](docs/MANUAL.md#1-快速开始) · [支持的功能与限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>清空表或改列名时会保护其他表的数据 · 2026-11-21</summary>

例如，清空 `orders` 后，仍能查询 `orders_items`。如果改列名会覆盖另一张表的索引，数据库会拒绝改名。

</details>

<details>
<summary>建索引时会保留其他表已有的索引 · 2026-11-21</summary>

如果新索引会覆盖另一张表已有的索引，数据库会拒绝创建，原来的数据仍能查询。

</details>

<details>
<summary>表改名后可以用原名建一张新表 · 2026-11-21</summary>

例如，把 `orders` 改名为 `orders_archive` 后，可以再建一张 `orders`。旧订单和新订单可以分别查询。

</details>

[完整更新日志](CHANGELOG.md)
