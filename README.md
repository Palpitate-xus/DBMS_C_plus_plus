# DBMS C++

用 C++ 编写的 SQL 数据库，支持建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>按一段订单号修改或删除更快了</summary>

例如，要给订单号 42 到 49 的记录标记为已复核，数据库可以先直接找到这几笔订单，再逐条检查并修改。删除一段编号也可以用这个方法。

</details>

<details>
<summary>库里表多时，查报表更快了</summary>

例如，库里还有上百张表时，打开订单报表前只整理查询涉及的表的列信息。客户端也能更快拿到列名和类型。

</details>

<details>
<summary>客户端能显示报表的列名了</summary>

用 psql 查看多层报表时，可以看到每列的名称和数据类型。即使没有符合条件的记录，也能查看列说明。

</details>

[完整更新日志](CHANGELOG.md)
