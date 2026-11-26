# DBMS C++

一个用 C++ 编写的数据库，支持用 SQL 建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>可以给视图的列改名了 · 2026-11-26</summary>

例如，可以把报表视图里的 `qty` 改成 `quantity`。原表和引用它的其他视图保留各自的列名。

</details>

<details>
<summary>表的列改名后，还能通过视图改数据 · 2026-11-26</summary>

例如，订单表的 `qty` 改名后，通过原有的单表视图写入 `qty`，值仍会存到原来对应的那一列。

</details>

<details>
<summary>表的列改名后，已有报表还能查 · 2026-11-26</summary>

例如，把订单表的 `qty` 改成 `quantity` 后，通过已有视图做的报表仍可用 `qty` 查询。

</details>

[完整更新日志](CHANGELOG.md)
