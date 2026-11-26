# DBMS C++

用 C++ 编写的 SQL 数据库，支持建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>查多层报表时少占内存</summary>

例如，一份订单报表来自另一份报表时，数据库可以逐条传递记录，省去在每一步留一份副本的开销。排序或多次使用同一批记录时，仍会保留需要的数据。

</details>

<details>
<summary>查上千个订单号也能少读数据</summary>

例如，要查 1,000 个连续的订单号，如果这些订单集中存放，数据库可以只读取表里相关的记录。

</details>

<details>
<summary>按一小段订单号查报表更快了</summary>

例如，查订单号 100 到 115 时，数据库可以先找到这批订单，再按报表上的条件筛选。

</details>

[完整更新日志](CHANGELOG.md)
