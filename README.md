# DBMS C++

DBMS C++ 是一个用 C++ 编写的数据库，可以用 SQL 建表、查询和修改数据。

[安装与运行](docs/MANUAL.md#1-快速开始) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>给视图改名或换位置更快 · 2026-11-26</summary>

在额外建了 64 张空表的磁盘测试中，单独移动视图的中位耗时从 532–644 毫秒缩短到 67–90 毫秒。

</details>

<details>
<summary>可以给视图换个位置 · 2026-11-26</summary>

可以把 `sales.monthly` 移到 `archive.monthly`。视图仍然使用原来的数据表，依赖它的查询也能继续运行。

</details>

<details>
<summary>可以给视图改名 · 2026-11-26</summary>

用 `ALTER VIEW` 改名后，依赖这个视图的查询仍能运行，原有注释也会保留。

</details>

[完整更新日志](CHANGELOG.md)
