# DBMS C++

用 C++ 写的关系型数据库，支持 SQL 查询、索引和事务。可以用 `psql` 连接。

## 快速开始

[编译依赖](docs/PACKAGING.md#编译依赖) · [下载并编译](docs/MANUAL.md#编译) · [启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [支持的功能](docs/compatibility-contract.md) · [更多文档](docs/README.md)

## 最近更新

<details>
<summary>编号相同的订单也能按店铺区分 · 2026-11-23</summary>

即使两个店铺的编号很长、开头也一样，各自的第 42 号订单也能分别保存和修改。

</details>

<details>
<summary>开头一样的长编号可以分别保存和查找 · 2026-11-23</summary>

如果用一个文本编号来区分订单，即使两个编号开头一样，也能按完整编号找到各自的订单。

</details>

<details>
<summary>按编号查订单时可以少读数据 · 2026-11-22</summary>

例如查某个产品的全部订单。数据库查完可能匹配的部分后，可以跳过后面无关的数据。

</details>

[完整更新日志](CHANGELOG.md)
