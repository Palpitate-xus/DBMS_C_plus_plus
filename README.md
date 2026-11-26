# DBMS C++

用 C++ 编写的 SQL 数据库。可以创建表、查询和修改数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [查看文档](docs/README.md)

## 最近更新

<details>
<summary>不能再通过汇总视图改数据</summary>

例如，按月统计销量的视图只供查询。试图通过它增删数据时，数据库会报错并保留原表的数据。

</details>

<details>
<summary>已有视图可以拒绝不符合条件的数据</summary>

例如，一个视图只显示有库存的商品。开启检查后，通过这个视图把库存改成零就会报错。这项检查也可以关闭。

</details>

<details>
<summary>撤销部分数据修改时少写一次磁盘</summary>

修改前后的数据存放在磁盘同一页时，撤销修改只需写回这页一次。

</details>

[完整更新日志](CHANGELOG.md)
