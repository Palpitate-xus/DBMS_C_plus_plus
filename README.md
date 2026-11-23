# DBMS C++

用 C++ 写的关系型数据库，支持 SQL 查询、索引和事务。可以用 `psql` 连接。

## 快速开始

[编译依赖](docs/PACKAGING.md#编译依赖) · [下载并编译](docs/MANUAL.md#编译) · [启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [更多文档](docs/README.md)

## 最近更新

<details>
<summary>开头一样的长编号可以分别保存和查找 · 2026-11-23</summary>

如果用一个文本编号来区分订单，即使两个编号开头一样，也能按完整编号找到各自的订单。

</details>

<details>
<summary>按编号查订单时可以少读数据 · 2026-11-22</summary>

例如查某个产品的全部订单。数据库查完可能匹配的部分后，可以跳过后面无关的数据。

</details>

<details>
<summary>按完整名称找记录更快了 · 2026-11-22</summary>

例如查商品名正好是「绿茶」的记录。给已有数据的表加上「名称不能重复」的规则时，查重也更快了。

</details>

[完整更新日志](CHANGELOG.md)
