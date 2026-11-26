# DBMS C++

用 C++ 编写的 SQL 数据库。可以创建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>查普通视图可以省去临时表</summary>

例如，打开订单视图时，数据库可以直接读取查询结果，省去写入磁盘临时表的步骤。

</details>

<details>
<summary>改数据时可以用视图里的计算结果</summary>

例如，可以按视图里的订单金额挑出要修改的订单，并在修改后返回新的金额。

</details>

<details>
<summary>不能再通过汇总视图改数据</summary>

例如，按月统计销量的视图只供查询。试图通过它增删数据时，数据库会报错并保留原表的数据。

</details>

[完整更新日志](CHANGELOG.md)
