# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务，可用 `psql` 连接。

## 快速开始

[安装编译依赖](docs/PACKAGING.md#编译依赖) · [编译源码](docs/MANUAL.md#编译) · [启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>逐行查找时，按文字和数字筛选更快 · 2026-11-23</summary>

比如查备注超过两个字、金额大于零的订单。数据库提前准备好比较条件，筛掉不合条件的记录后再组装结果。

</details>

<details>
<summary>符合条件的订单很少时，翻页也能更快 · 2026-11-23</summary>

比如查一家店里备注较长的订单，数据库可以继续按订单号在这家店里找，直到凑够一页。

</details>

<details>
<summary>按备注筛选后取前几笔订单更快 · 2026-11-23</summary>

比如按订单号从大到小查备注超过两个字的订单，数据库可以边读边筛，找够就停。

</details>

[完整更新日志](CHANGELOG.md) · [项目文档](docs/README.md)
