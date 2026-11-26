# DBMS C++

一个用 C++ 编写的数据库，支持用 SQL 建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>撤销更新时可以少写一次磁盘 · 2026-11-26</summary>

更新前后的数据存放在同一页时，撤销更新只需把这页写一次。

</details>

<details>
<summary>通过视图写入时可以检查新数据 · 2026-11-26</summary>

例如，只显示正数的视图开启检查后，会拒绝把零或负数写进去。

</details>

<details>
<summary>可以给视图的列改名了 · 2026-11-26</summary>

例如，可以把报表视图里的 `qty` 改成 `quantity`。原表和引用它的其他视图保留各自的列名。

</details>

[完整更新日志](CHANGELOG.md)
