# DBMS C++

一个用 C++ 写的关系型数据库。支持用 SQL 建表、查询和修改数据，可以用 `psql` 连接。

## 快速开始

[安装编译依赖](docs/PACKAGING.md#编译依赖) · [编译并运行](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>可以查看数据库自动创建的主键索引 · 2026-11-24</summary>

例如，在 `psql` 中可以列出订单表的主键索引，也可以查询这个索引用了哪几列。

</details>

<details>
<summary>列出一张表上手动创建的索引 · 2026-11-24</summary>

例如，可以列出订单表上的索引，看看哪些索引要求值不能重复。

</details>

<details>
<summary>查看某个索引用了哪些列 · 2026-11-24</summary>

例如，可以查到订单表的一个索引按客户编号查找，同时保存了订单日期。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
