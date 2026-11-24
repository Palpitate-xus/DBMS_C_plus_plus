# DBMS C++

一个用 C++ 编写的关系型数据库。可以用 SQL 建表、查询和修改数据，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>读取表和列的信息时少做重复工作 · 2026-11-24</summary>

数据库读取表名、列名等信息时，省掉了准备写回文件的步骤。单独测试这项读取时，耗时减少约三成。

</details>

<details>
<summary>反复查看同一张表的索引更快 · 2026-11-24</summary>

反复查看索引时，数据库需要读的文件更少。在一张有 512 列的测试表上，`psql` 用来显示索引列的查询从约 10 毫秒降到约 8.5 毫秒。

</details>

<details>
<summary>用 psql 查看索引列更快 · 2026-11-24</summary>

在本地测试中，用 `psql` 列出一个五列索引的列名和类型，这条查询比上一版快约 6%–11%。

</details>

[完整更新日志](CHANGELOG.md)
