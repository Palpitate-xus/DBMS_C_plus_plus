# DBMS C++

用 C++ 编写的 SQL 数据库，支持建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>部分报表打开得更快了</summary>

例如，查看按条件筛出的订单时，数据库可以直接返回结果，省去先把结果另外存一遍的步骤。

</details>

<details>
<summary>可以按报表算出的金额修改订单</summary>

例如，可以找出总价超过 100 元的订单，修改数量后立即看到新的总价。

</details>

<details>
<summary>汇总报表只能查看</summary>

例如，按月统计销量的报表只供查询。试图在报表里添加、修改或删除记录时，数据库会拒绝操作，保留原始数据。

</details>

[完整更新日志](CHANGELOG.md)
