# DBMS C++

用 C++ 写的关系型数据库，支持用 SQL 建表、查询和修改数据，也支持索引和事务。

## 快速开始

安装依赖后，下载源码并编译。[Linux 编译依赖](docs/PACKAGING.md#编译依赖)

```sh
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [项目文档](docs/README.md)

## 最近更新

<details>
<summary>按起止订单号查订单更快 · 2026-11-23</summary>

例如，查 1000 到 1100 号订单时，两头都算在内。记录集中时，数据库可以直接用订单号找到它们。

</details>

<details>
<summary>连续取多笔订单可以减少重复读取 · 2026-11-23</summary>

例如，最早的 128 笔订单都存在同一页时，数据库可以只取一次这一页，再逐条读取订单。

</details>

<details>
<summary>按订单号查一小段记录更快 · 2026-11-23</summary>

如果订单号是表的整数主键，查 1000 到 1100 号订单时，数据库可以用索引直接找这些记录。

</details>

[完整更新日志](CHANGELOG.md)
