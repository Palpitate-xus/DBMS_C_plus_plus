# DBMS C++

一个用 C++ 写的关系型数据库。用 SQL 建表、查询和修改数据，支持索引和事务。

## 快速开始

装好依赖后，在项目目录运行以下命令。[Linux 编译依赖](docs/PACKAGING.md#编译依赖)

```sh
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [项目文档](docs/README.md)

## 最近更新

<details>
<summary>加零的整数条件筛选更快 · 2026-11-23</summary>

例如，筛选 `订单号 + 0` 是否小于某个值时，数据库可以直接比较订单号。

</details>

<details>
<summary>部分查询少做一遍整数检查 · 2026-11-23</summary>

例如，用商品编号和库存量筛选记录时，数据库可以直接用扫描时的检查结果。

</details>

<details>
<summary>部分按编号筛选的查询更快 · 2026-11-23</summary>

例如，从 2,048 条记录中找出编号不超过 128 的记录，再合并查询结果。测试中这条查询的用时约减半。

</details>

[完整更新日志](CHANGELOG.md)
