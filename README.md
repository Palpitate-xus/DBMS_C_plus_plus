# DBMS C++

用 C++ 编写的数据库。你可以用 SQL 建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>同时统计表和列的注释更快 · 2026-11-26</summary>

在额外写入 1,024 条列注释的测试库里，一次查询分别统计表注释、列注释和总数，用时从约 50 毫秒缩短到约 7.7 毫秒。

</details>

<details>
<summary>按编号统计表注释更快 · 2026-11-26</summary>

在额外写入 1,024 条列注释的测试库里，按编号范围统计表注释，从约 264 毫秒缩短到约 28 毫秒。

</details>

<details>
<summary>可以查看数据库怎样查找注释 · 2026-11-26</summary>

想知道数据库怎样查找表或列的注释，可以在查询前加上 `EXPLAIN` 查看步骤。

</details>

[完整更新日志](CHANGELOG.md)
