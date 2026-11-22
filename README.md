# DBMS C++

这是一个用 C++ 编写的数据库。你可以在终端用 SQL 建表、写入和查询数据，也可以用 `psql` 连接。

[编译和运行](docs/MANUAL.md#1-快速开始) · [支持的功能](docs/compatibility-contract.md) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>分区查询取够结果后可以提前结束 · 2026-11-22</summary>

用 `LIMIT` 查询分区表时，符合条件的查询取够记录后就会停止。测试中，查询一张有 60 个分区的表并只取一条记录，从打开 60 个分区文件减少到 1 个。[使用说明](docs/MANUAL.md#15-分区表)

</details>

<details>
<summary>分区较多的表，重复查询用时缩短 · 2026-11-22</summary>

查询多个分区时，可以共用已经查到的表结构。在有 60 个分区的测试表上，连续查询的耗时缩短了约 16% 到 18%。

</details>

<details>
<summary>修复部分跨分区查询漏掉记录的问题 · 2026-11-22</summary>

用 `OR` 连接筛选条件时，符合任一条件的记录都会返回。一条记录同时符合多个条件时，也只返回一次。

</details>

[完整更新日志](CHANGELOG.md)
