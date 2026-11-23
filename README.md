# DBMS C++

一个用 C++ 编写的关系型数据库，支持 SQL 查询、数据修改、索引和事务。

[编译和启动](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>只取几个数字时，可以按需生成 · 2026-11-23</summary>

例如，用 SQL 生成从 1 到 10 万的数字，只取开头的 3 个。数据库生成这 3 个就能返回结果。

</details>

<details>
<summary>先检查修改结果能否读取，再改数据 · 2026-11-23</summary>

例如，一条语句先修改订单，再读取修改结果。如果这次修改没有指定要返回的内容，数据库会在改动订单前报错。

</details>

<details>
<summary>查询可以返回没有列的结果行 · 2026-11-23</summary>

例如，按店铺筛选订单，不选择订单号或金额等列。找到 3 笔订单时，查询会返回 3 行空白记录。

</details>

[完整更新日志](CHANGELOG.md)
