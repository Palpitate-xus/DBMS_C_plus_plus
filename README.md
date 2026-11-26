# DBMS C++

一个用 C++ 编写的关系型数据库。可以用 SQL 建表、查询和修改数据，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>按表名查看列信息更快 · 2026-11-26</summary>

用 `'表名'::regclass` 查询系统目录时，数据库会复用已找到的表编号。在额外建了 32 张表的测试中，列出四列从约 95 毫秒降到约 6 毫秒。

</details>

<details>
<summary>按表编号查看列信息更快 · 2026-11-26</summary>

查询系统目录时，写了 `::oid` 或 `CAST` 也能先定位到这张表。在额外建了 32 张表的测试中，列出四列的查询从约 97 毫秒降到约 5 毫秒。八进制编号也能查到正确的列。

</details>

<details>
<summary>修复回滚后关闭数据库时的崩溃 · 2026-11-26</summary>

从 C++ 调用数据库时，撤销表结构修改后再关闭数据库，有时会在保存表信息时崩溃。现在这一过程可以正常完成。

</details>

[完整更新日志](CHANGELOG.md)
