# DBMS C++

用 C++ 编写的开源关系型数据库。支持用 SQL 查询和修改数据、创建索引和运行事务，也可以用 `psql` 连接。

[编译和安装](docs/PACKAGING.md#编译依赖) · [启动并执行第一条 SQL](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>查子表的父表名称更快 · 2026-11-23</summary>

例如，数据库里有 128 条表继承关系。在这组测试中，查一张子表的父表名称从约 170 毫秒降到了约 15 毫秒。

</details>

<details>
<summary>可以查询一张表继承了哪些表 · 2026-11-23</summary>

例如，迁移工具可以查询 `pg_inherits`，找到订单子表的直接父表，以及各父表的顺序。

</details>

<details>
<summary>建子表时可以指明父表的位置 · 2026-11-23</summary>

例如，`live.orders` 和 `archive.orders` 同名。创建订单子表时，写 `INHERITS (live.orders)` 就会选中 `live` 里的父表。

</details>

[完整更新日志](CHANGELOG.md)
