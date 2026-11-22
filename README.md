# DBMS C++

这是一个用 C++ 编写的 SQL 数据库。你可以在终端里建表、写入和查询数据，也可以用 `psql` 等 PostgreSQL 客户端连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [支持的功能和限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>清空分区表后及时腾出磁盘空间 · 2026-11-22</summary>

数据库会及时关闭旧分区文件，让操作系统回收这些文件占用的空间。

</details>

<details>
<summary>同时清空多张表，出错时一起恢复 · 2026-11-22</summary>

用一条命令清空默认数据目录里的多张普通表时，数据库可以只保存这些表恢复所需的文件。提交前出错，已清空的表会一起恢复。

</details>

<details>
<summary>清空一张表时减少备份文件 · 2026-11-22</summary>

单独清空默认数据目录里的一张普通表时，数据库可以只备份恢复这张表所需的文件。提交前意外退出，重启后可以找回原来的数据。

</details>

[完整更新日志](CHANGELOG.md)
