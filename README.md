# DBMS C++

用 C++ 编写的关系型数据库。可以用 SQL 建表、查询和修改数据，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [安装编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>在 psql 里查看索引列更快 · 2026-11-24</summary>

读取索引列的查询耗时中位数降低约 6%–11%。有同名自定义函数时，明确指定 `pg_catalog` 也能得到正确的结果类型。

</details>

<details>
<summary>表多时，查索引的列更快 · 2026-11-24</summary>

在额外建了 256 张表的测试库里，查看一个索引的列名和类型，耗时中位数降低约 25%–31%。

</details>

<details>
<summary>用 psql 查看索引用了哪些列 · 2026-11-24</summary>

输入 `\d 索引名`，可以看到普通索引中各列的类型，以及哪些列用来查找、哪些列随索引一起保存。

</details>

[完整更新日志](CHANGELOG.md)
