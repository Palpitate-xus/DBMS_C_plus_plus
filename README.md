# DBMS C++

一个用 C++ 编写的关系型数据库。可以用 SQL 建表、查询和更新数据，也可以用 `psql` 连接。

## 快速开始

在 Linux 上[安装编译依赖](docs/PACKAGING.md#编译依赖)后，下载并编译。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>在 psql 中查看数据库账号 · 2026-11-24</summary>

输入 `\du` 就能查看哪些账号可以登录、同时连接的数量上限，以及密码何时过期。

</details>

<details>
<summary>查出一个分区属于哪张表 · 2026-11-24</summary>

例如，订单按月份分开存放时，现在可以从某个月的分区查到订单总表。

</details>

<details>
<summary>查看删除或修改记录时会怎样影响关联数据 · 2026-11-24</summary>

例如，可以查到删除客户时，数据库会一起删除该客户的订单，还是拒绝删除这位客户。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
