# DBMS C++

DBMS C++ 是一个用 C++ 编写的数据库，可以用 SQL 建表、查询和修改数据。

[安装与运行](docs/MANUAL.md#1-快速开始) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>可以给视图改名 · 2026-11-26</summary>

用 `ALTER VIEW` 改名后，依赖这个视图的查询仍能运行，原有注释也会保留。

</details>

<details>
<summary>改表时少读一遍备份文件 · 2026-11-26</summary>

在额外建了 128 张表的磁盘测试中，改表名的时间从约 1.8 秒缩短到约 1.1 秒。

</details>

<details>
<summary>修改表时可以跳过不存在的表 · 2026-11-26</summary>

用 `ALTER TABLE IF EXISTS` 给表加列、删列或改名时，如果表不存在，数据库会提示并跳过。

</details>

[完整更新日志](CHANGELOG.md)
