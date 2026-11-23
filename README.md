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
<summary>同时查几段订单号更快 · 2026-11-23</summary>

例如，同时查 1000 到 1100 号和 5000 到 5100 号时，数据库可以按订单号分别找到这两批记录。两段重叠时，同一笔订单只返回一次。

</details>

<details>
<summary>查一段订单号时，可以把两端反着写 · 2026-11-23</summary>

用 `BETWEEN SYMMETRIC` 查询时，从 1100 到 1000 和从 1000 到 1100 会找到同一批订单。

</details>

<details>
<summary>跳过一段订单号，可以直接查两头 · 2026-11-23</summary>

例如，只查最早和最新的几笔订单时，数据库可以按订单号找到这两批记录，省去读取中间的订单。

</details>

[完整更新日志](CHANGELOG.md)
