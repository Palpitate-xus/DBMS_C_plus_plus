# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务，可用 `psql` 连接。

## 快速开始

[安装编译依赖](docs/PACKAGING.md#编译依赖) · [编译源码](docs/MANUAL.md#编译) · [启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>查订单号最大的几笔订单更快 · 2026-11-23</summary>

比如查看一家店订单号最大的 5 笔订单，数据库可以按号码从大到小取出这 5 笔，省去另外排序。

</details>

<details>
<summary>逐条按名称或数字筛选记录更快 · 2026-11-23</summary>

比如在商品表里逐条查找名为「绿茶」的商品，数据库只需在查找前处理一次要比较的名称。

</details>

<details>
<summary>查一家店指定号码范围内的订单更快 · 2026-11-23</summary>

比如查一家店订单号在 100 到 110 之间的订单，数据库可以直接找到这一段。

</details>

[完整更新日志](CHANGELOG.md) · [项目文档](docs/README.md)
