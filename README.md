# DBMS C++

一个用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务。可以用 `psql` 连接。

## 快速开始

在 Linux 上安装编译依赖后，下载源码并编译。最后一条命令会显示版本号。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>按订单号取前几条记录更快 · 2026-11-23</summary>

例如，查看某家店订单号最大的 5 笔订单时，数据库可以沿着索引读取，取够就结束。

</details>

<details>
<summary>带参数的查询在扫描整张表时更快 · 2026-11-23</summary>

例如应用传入店铺编号查询订单时，数据库会先准备好比较条件，再逐行检查。

</details>

<details>
<summary>可以更快地查某家店的一段订单号 · 2026-11-23</summary>

例如查一家店的第 100 到 110 号订单，数据库可以直接去找这一段。只按店铺查询时，订单少的店铺也可以用同样的办法定位。

</details>

[完整更新日志](CHANGELOG.md) · [支持的功能](docs/compatibility-contract.md) · [文档目录](docs/README.md)
