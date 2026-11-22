# DBMS C++

一个用 C++ 编写的 SQL 数据库。

[编译和运行](docs/MANUAL.md#1-快速开始) · [项目文档](docs/README.md)

## 最近更新

<details>
<summary>清空一张表时少复制文件 · 2026-11-22</summary>

单独清空一张普通表时，数据库会为这张表保留一份恢复用的备份。

</details>

<details>
<summary>重启时会检查清空表是否已提交 · 2026-11-21</summary>

对新建的普通表，单独执行 `TRUNCATE` 时如果数据库意外退出，重启后会根据这次清空是否已提交，保留空表或恢复原来的数据。

</details>

<details>
<summary>备份时减少重复打开文件 · 2026-11-21</summary>

核对备份文件和把它写到磁盘时，只需打开一次文件。备份做到一半中断后，数据库重启时会跳过那份备份。

</details>

[完整更新日志](CHANGELOG.md)
