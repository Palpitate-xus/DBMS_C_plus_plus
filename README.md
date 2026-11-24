# DBMS C++

一个用 C++ 写的关系型数据库。支持用 SQL 建表、查询和修改数据，可以用 `psql` 连接。

## 快速开始

[安装编译依赖](docs/PACKAGING.md#编译依赖) · [编译并运行](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>可以查看部分索引的条件 · 2026-11-24</summary>

例如，可以查到一个索引只收录数量大于零的记录。建索引时写下的条件括号也会保留下来。

</details>

<details>
<summary>按表查询索引更快 · 2026-11-24</summary>

查询订单表的索引时，数据库会先筛出与订单表有关的记录，减少其他表带来的开销。

</details>

<details>
<summary>可以查询主键定义和它使用的索引 · 2026-11-24</summary>

例如，可以查到订单表用哪几列作为主键，也可以查到哪些外键引用了这个主键。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
