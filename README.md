# DBMS C++

用 C++ 编写的 SQL 数据库，支持建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>客户端能显示报表的列名了</summary>

用 psql 查看多层报表时，可以看到每列的名称和数据类型。即使没有符合条件的记录，也能查看列说明。

</details>

<details>
<summary>查多层报表时少占内存</summary>

例如，一份订单报表来自另一份报表时，数据库可以逐条传递记录，省去在每一步留一份副本的开销。排序或多次使用同一批记录时，仍会保留需要的数据。

</details>

<details>
<summary>查上千个订单号也能少读数据</summary>

例如，要查 1,000 个连续的订单号，如果这些订单集中存放，数据库可以只读取表里相关的记录。

</details>

[完整更新日志](CHANGELOG.md)
