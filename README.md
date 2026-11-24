# DBMS C++

一个用 C++ 写的关系型数据库。支持用 SQL 建表、查询和修改数据，可以用 `psql` 连接。

## 快速开始

[安装编译依赖](docs/PACKAGING.md#编译依赖) · [编译并运行](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>列出一张表上手动创建的索引 · 2026-11-24</summary>

例如，可以列出订单表上的索引，看看哪些索引要求值不能重复。

</details>

<details>
<summary>查看某个索引用了哪些列 · 2026-11-24</summary>

例如，可以查到订单表的一个索引按客户编号查找，同时保存了订单日期。

</details>

<details>
<summary>把多行查询结果放进一个数组 · 2026-11-24</summary>

例如，可以把某位客户的所有订单编号放在同一个数组里。没有订单时会得到空数组。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
