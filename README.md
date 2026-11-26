# DBMS C++

用 C++ 编写的 SQL 数据库，支持建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>报表查上千个编号时也可以少读数据</summary>

例如，查一段订单号时，如果这些订单集中存放，数据库可以用索引只读取相关的那部分数据。

</details>

<details>
<summary>从报表里查一段编号更快了</summary>

例如，查编号 100 到 115 的订单时，数据库可以先用原表的索引找到这段编号，再按报表的条件筛选和分页。

</details>

<details>
<summary>从多层报表按编号找记录更快了</summary>

例如，按订单号查一笔订单时，数据库可以用原表的索引找到它，再确认它是否该出现在报表里。

</details>

[完整更新日志](CHANGELOG.md)
