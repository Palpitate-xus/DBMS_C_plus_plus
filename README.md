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
<summary>从 CSV 导入数据 · 2026-11-22</summary>

可以把表格软件保存的 CSV 导入数据库，并在导入前核对第一行的列名和顺序。单元格里有换行也可以导入。

</details>

<details>
<summary>把查询结果保存为 CSV · 2026-11-22</summary>

例如，只导出本月的订单，按金额排序，并把列名写在第一行。

</details>

<details>
<summary>有人改表时，其他查询可以等一下 · 2026-11-22</summary>

数据库正在保存表结构时，另一个连接发来的查询可以等保存完成再继续。达到设置的等待时间才报超时。

</details>

[完整更新日志](CHANGELOG.md)
