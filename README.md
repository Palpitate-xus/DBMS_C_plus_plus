# DBMS C++

一个用 C++ 写的关系型数据库。可以用 SQL 建表、查询和修改数据，支持索引和事务，也可以用 `psql` 连接。

## 快速开始

安装[编译依赖](docs/PACKAGING.md#编译依赖)后，在项目目录运行以下命令，最后一条会显示版本号。

```sh
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>普通查询合并时也能保留空值 · 2026-11-23</summary>

例如，把表里查出的编号和一行空值合在一起，结果仍是一列整数，空值也会保留下来。

</details>

<details>
<summary>合并数组结果时可以直接写空值 · 2026-11-23</summary>

例如，把一组编号和一个 `NULL` 合并成一列时，编号仍是整数，空值也会保留下来。

</details>

<details>
<summary>数组里的值可以和其他查询结果合并 · 2026-11-23</summary>

例如，可以把手头的一组订单号和从表里查出的订单号放在同一份结果里，也可以去掉重复项。

</details>

[完整更新日志](CHANGELOG.md)
