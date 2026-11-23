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
<summary>同一家店的订单很多时，按订单号查找更快 · 2026-11-23</summary>

同时给出店铺编号和订单号后，数据库可以少检查同一家店的其他订单。按这两个编号修改或删除订单，以及检查新订单是否重号，也能少读记录。

</details>

<details>
<summary>修改或删除指定店铺的订单更快 · 2026-11-23</summary>

同时给出店铺编号和订单号，数据库就能少翻一些记录，再修改或删除找到的订单。

</details>

<details>
<summary>查找指定店铺的订单更快 · 2026-11-23</summary>

比如查某家店的第 42 号订单时，数据库会一起用上店铺编号和订单号，少检查一些记录。

</details>

[完整更新日志](CHANGELOG.md) · [支持的功能](docs/compatibility-contract.md) · [文档目录](docs/README.md)
