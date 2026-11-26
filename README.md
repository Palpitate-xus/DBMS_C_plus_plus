# DBMS C++

一个用 C++ 编写的数据库，支持用 SQL 建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>已有视图也能开启或关闭写入检查 · 2026-11-26</summary>

例如，可以给已有的报表视图开启检查，让它拒绝写入不符合筛选条件的数据。

</details>

<details>
<summary>撤销更新时可以少写一次磁盘 · 2026-11-26</summary>

更新前后的数据存放在同一页时，撤销更新只需把这页写一次。

</details>

<details>
<summary>通过视图写入时可以检查新数据 · 2026-11-26</summary>

例如，只显示正数的视图开启检查后，会拒绝把零或负数写进去。

</details>

[完整更新日志](CHANGELOG.md)
