# DBMS C++

一个用 C++ 编写的关系型数据库，支持 SQL 查询、数据修改、索引和事务。

[编译和启动](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>两组结果可以并排读，短的一组补空值 · 2026-11-23</summary>

例如，把 3 个编号和 2 个名字并排查询，结果会有 3 行，最后一行的名字为空。

</details>

<details>
<summary>只取几个数字时，可以按需生成 · 2026-11-23</summary>

例如，用 SQL 生成从 1 到 10 万的数字，只取开头的 3 个。数据库生成这 3 个就能返回结果。

</details>

<details>
<summary>先检查修改结果能否读取，再改数据 · 2026-11-23</summary>

例如，一条语句先修改订单，再读取修改结果。如果这次修改没有指定要返回的内容，数据库会在改动订单前报错。

</details>

[完整更新日志](CHANGELOG.md)
