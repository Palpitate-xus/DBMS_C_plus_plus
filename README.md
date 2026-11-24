# DBMS C++

用 C++ 编写的开源关系型数据库。可以用 SQL 建表、查数据，也可以用 `psql` 连接。

[编译和启动](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>修复 psql 密码登录 · 2026-11-24</summary>

使用 SCRAM 密码认证的本地 TCP 连接，现在可以用原版 psql 18.6 登录并查询。密码错误时，连接仍会被拒绝。

</details>

<details>
<summary>用同一条 SQL 反复翻页更快 · 2026-11-24</summary>

在 psql 中准备好查询后，可以每次传入要取几条、要跳过几条。按对象编号查小页时，数据库读够就停。

</details>

<details>
<summary>按对象编号排序，取前几条更快 · 2026-11-24</summary>

给原表编号建好索引后，查询编号最小或最大的几条迁移记录时，数据库可以读够就停。在 2,050 行的测试中，取最小的 3 条从约 16 毫秒降到约 3 毫秒。

</details>

[完整更新日志](CHANGELOG.md)
