# DBMS C++

一个用 C++ 写的关系型数据库。可以用 SQL 建表、查询和修改数据，支持索引和事务，也可以用 `psql` 连接。

## 快速开始

[编译和运行](docs/MANUAL.md#1-快速开始) · [Linux 编译依赖](docs/PACKAGING.md#编译依赖) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>按订单号翻页时，减少重复读取 · 2026-11-23</summary>

例如，查编号最小的 128 笔订单时，数据库可以把存放在一起的订单一次读出来，最后按编号返回。

</details>

<details>
<summary>同时查几段订单号，可以跳过中间的订单 · 2026-11-23</summary>

例如，查 1 到 16 号和 8177 到 8192 号时，数据库可以直接找到这两批订单。两段重叠时，同一笔订单只返回一次。

</details>

<details>
<summary>查一段订单号时，起止编号可以反着写 · 2026-11-23</summary>

用 `BETWEEN SYMMETRIC` 查询时，把 1100 写在 1000 前面，也能查出 1000 到 1100 号的所有订单。

</details>

[完整更新日志](CHANGELOG.md)
