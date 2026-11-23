# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务，可用 `psql` 连接。

## 快速开始

[安装编译依赖](docs/PACKAGING.md#编译依赖) · [编译源码](docs/MANUAL.md#编译) · [启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>按备注筛选后取前几笔订单更快 · 2026-11-23</summary>

比如按订单号从大到小查备注超过两个字的订单，数据库可以边读边筛，找够就停。

</details>

<details>
<summary>一家店有很多订单时，翻页也更快 · 2026-11-23</summary>

即使一家店有几千笔订单，按订单号查前几笔时，数据库也可以只读这一页需要的记录。

</details>

<details>
<summary>查订单号最大的几笔订单更快 · 2026-11-23</summary>

比如查看一家店订单号最大的 5 笔订单，数据库可以按号码从大到小取出这 5 笔，省去另外排序。

</details>

[完整更新日志](CHANGELOG.md) · [项目文档](docs/README.md)
