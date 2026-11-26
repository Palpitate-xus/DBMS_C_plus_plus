# DBMS C++

一个用 C++ 编写的数据库，支持用 SQL 建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>表的列改名后，已有报表还能查 · 2026-11-26</summary>

例如，把订单表的 `qty` 改成 `quantity` 后，通过已有视图做的报表仍可用 `qty` 查询。

</details>

<details>
<summary>保存视图的说明更快了 · 2026-11-26</summary>

在测试中，给视图保存一条说明比上一版快了约 7 到 12 毫秒。

</details>

<details>
<summary>给视图改名、换位置更快了 · 2026-11-26</summary>

例如，单独把视图从 `sales.monthly` 移到 `archive.monthly` 时，只需要备份这次改动涉及的文件。

</details>

[完整更新日志](CHANGELOG.md)
