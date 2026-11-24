# DBMS C++

用 C++ 编写的关系型数据库。可以用 SQL 建表、查询和修改数据，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [安装编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>用 psql 查看索引用了哪些列 · 2026-11-24</summary>

输入 `\d 索引名`，可以看到普通索引中各列的类型，以及哪些列用来查找、哪些列随索引一起保存。

</details>

<details>
<summary>查看索引按什么条件收录记录 · 2026-11-24</summary>

在 `psql` 里用 `\d 索引名` 可以查看部分索引的筛选条件。例如，商品索引只收录库存大于 0 的商品。

</details>

<details>
<summary>查一张表有哪些索引更快 · 2026-11-24</summary>

例如，列出订单表的索引时，数据库可以少检查其他表的索引记录。

</details>

[完整更新日志](CHANGELOG.md)
