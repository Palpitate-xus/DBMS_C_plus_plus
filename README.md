# DBMS C++

用 C++ 编写的数据库，支持用 SQL 建表、查询、添加、更新和删除数据。

[安装与快速开始](docs/MANUAL.md#1-快速开始) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>按编号和状态批量处理订单更快了</summary>

例如，要更新编号 300 到 308 中待处理的订单，数据库可以先找到这段订单，再检查哪些尚未处理。删除时也可以这样筛选。

</details>

<details>
<summary>按一段编号批量更新或删除更快了</summary>

例如，要删除编号 42 到 49 的订单，数据库可以直接找到这几笔。

</details>

<details>
<summary>表多时，查询可以更快返回列名</summary>

例如，查订单报表时，数据库只准备这次查询用到的表的列名和数据类型。

</details>

[完整更新日志](CHANGELOG.md)
