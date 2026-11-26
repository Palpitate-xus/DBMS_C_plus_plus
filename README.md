# DBMS C++

DBMS C++ 是一个用 C++ 编写的数据库，可以用 SQL 建表、查询和修改数据。

[安装与运行](docs/MANUAL.md#1-快速开始) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>可以给视图换个位置 · 2026-11-26</summary>

可以把 `sales.monthly` 移到 `archive.monthly`。视图仍然使用原来的数据表，依赖它的查询也能继续运行。

</details>

<details>
<summary>可以给视图改名 · 2026-11-26</summary>

用 `ALTER VIEW` 改名后，依赖这个视图的查询仍能运行，原有注释也会保留。

</details>

<details>
<summary>改表时少读一遍备份文件 · 2026-11-26</summary>

在额外建了 128 张表的磁盘测试中，改表名的时间从约 1.8 秒缩短到约 1.1 秒。

</details>

[完整更新日志](CHANGELOG.md)
