# DBMS C++

用 C++ 编写的关系型数据库。可以用 SQL 建表、查询和修改数据，支持事务和 `psql` 连接。

## 快速开始

先在 Linux 上安装[编译依赖](docs/PACKAGING.md#编译依赖)，再运行以下命令。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[运行数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [功能和限制](docs/compatibility-contract.md) · [文档索引](docs/README.md)

## 最近更新

<details>
<summary>修复导入时自动取号的顺序 · 2026-11-22</summary>

如果一行先读到自动编号，再读到无法转换的字段，数据库会记住已经取过的编号，并撤销这一批导入的记录。

</details>

<details>
<summary>导入旧数据时可以保留原来的编号 · 2026-11-22</summary>

例如，迁移订单时可以保留旧订单号。文件里省略编号时，数据库可以自动生成。

</details>

<details>
<summary>导入数据时可以使用列的默认值 · 2026-11-22</summary>

例如，导入订单时可以用一个约定的标记，让数据库填上默认状态。

</details>

[完整更新日志](CHANGELOG.md)
