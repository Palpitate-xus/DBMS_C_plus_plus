# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引、事务以及数据备份和恢复。

[快速开始](docs/MANUAL.md#1-快速开始) · [支持的功能与限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>生成备份时少走一遍文件目录 · 2026-11-21</summary>

数据库核对备份文件时，也会确认它已写到磁盘，省去重新打开文件的步骤。重启时会跳过还在写入的备份目录。

</details>

<details>
<summary>清空表后只重建一次索引 · 2026-11-21</summary>

例如，清空 `orders` 后，订单号和商品名的索引都会变为空。重新插入订单后，可以继续用原来的索引查询。

</details>

<details>
<summary>清空表或改列名时会保护其他表的数据 · 2026-11-21</summary>

例如，清空 `orders` 后，仍能查询 `orders_items`。如果改列名会覆盖另一张表的索引，数据库会拒绝改名。

</details>

[完整更新日志](CHANGELOG.md)
