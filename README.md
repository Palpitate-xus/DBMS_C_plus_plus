# DBMS C++

DBMS C++ 是一个用 C++ 编写的数据库，可以用 SQL 建表、查询和修改数据。

[安装与运行](docs/MANUAL.md#1-快速开始) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>修改表时可以跳过不存在的表 · 2026-11-26</summary>

用 `ALTER TABLE IF EXISTS` 给表加列、删列或改名时，如果表不存在，数据库会提示并跳过。

</details>

<details>
<summary>改表名时更快发现重名 · 2026-11-26</summary>

在额外建了 32 张表的磁盘测试中，发现表名已被占用的时间从约 2 秒缩短到约 40 毫秒。

</details>

<details>
<summary>磁盘上的表改名更快 · 2026-11-26</summary>

在额外建了 128 张表的磁盘测试中，改一张表名的时间从约 3.6 秒缩短到约 1.8 秒。

</details>

[完整更新日志](CHANGELOG.md)
