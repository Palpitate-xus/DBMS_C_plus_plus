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
<summary>合并查询时可以保留空值 · 2026-11-23</summary>

把查出的编号和一行空值合在一起，结果仍是一列整数，其中一行是空值。

</details>

<details>
<summary>展开一组数字时可以直接加上空值 · 2026-11-23</summary>

例如，把 10、20、30 展开成三行，再补一行 `NULL`，结果会有四行。

</details>

<details>
<summary>手头的订单号可以与查询结果合并 · 2026-11-23</summary>

可以在一条 SQL 里写入一组订单号，与表里查到的订单号合并，并按需去掉重复项。

</details>

[完整更新日志](CHANGELOG.md)
