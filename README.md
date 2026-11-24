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
<summary>可以按表查已创建的索引 · 2026-11-24</summary>

例如，可以列出订单表上手动创建的索引，查看每个索引用了几列、是否要求值不重复。

</details>

<details>
<summary>查看索引建在哪张表和哪些列上 · 2026-11-24</summary>

例如，可以查到订单索引按客户编号查找，也顺带保存了订单日期。

</details>

<details>
<summary>把一列查询结果收进数组 · 2026-11-24</summary>

例如，用一条查询把某位客户的订单编号按顺序放进一个数组。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
