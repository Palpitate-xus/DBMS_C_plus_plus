# DBMS C++

这是一个用 C++ 编写的 SQL 数据库。你可以在终端里建表、写入和查询数据，也可以用 `psql` 等 PostgreSQL 客户端连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [支持的功能和限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>读取分区表时减少重复查找 · 2026-11-22</summary>

各分区共用扫描开始时读到的表定义，数据库逐个核对并读取分区文件。测试中，分区较多的表在重复查询时耗时更短。

</details>

<details>
<summary>修复跨分区查询时漏掉记录的问题 · 2026-11-22</summary>

用 `OR` 查询多个分区时，数据库会返回所有匹配记录。同一条记录即使命中多个条件，也只返回一次。

</details>

<details>
<summary>可以用 SQL 向分区表添加记录 · 2026-11-22</summary>

已准备好分区且没有索引的表可以用 `INSERT` 写入。数据库会把每条记录放进对应的分区，提交前可以撤销写入。

</details>

[完整更新日志](CHANGELOG.md)
