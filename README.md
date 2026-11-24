# DBMS C++

用 C++ 写的关系型数据库。可以用 SQL 建表、查数据和改数据，也可以用 `psql` 连接。

## 快速开始

在 Linux 上装好依赖后，在项目目录编译。[编译依赖](docs/PACKAGING.md#编译依赖)

```sh
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>查看文本列怎么排序 · 2026-11-24</summary>

如果建表时选了按字节顺序排序（`C`），查询列信息时就会显示这个设置。

</details>

<details>
<summary>查看一列没填时用什么默认值 · 2026-11-24</summary>

例如，可以查到订单数量默认是 `1`，备注默认是空字符串。

</details>

<details>
<summary>查看列的类型和建表时指定的长度 · 2026-11-24</summary>

可以查到一列存的是整数、日期还是文本。例如，查询 `varchar(12)` 的列会显示长度 12。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
