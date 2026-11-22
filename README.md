# DBMS C++

一个用 C++ 编写的 SQL 数据库。

[编译和运行](docs/MANUAL.md#1-快速开始) · [项目文档](docs/README.md)

## 最近更新

<details>
<summary>清空分区表时及时释放旧文件 · 2026-11-22</summary>

清空分区表时，数据库会释放已打开的旧分区文件，并为这张表保存恢复用的备份。

</details>

<details>
<summary>一次清空多张表时少复制文件 · 2026-11-22</summary>

一次清空多张普通表时，数据库只为涉及的表保存恢复用的备份。提交前中途出错，整次清空都会撤销。

</details>

<details>
<summary>清空一张表时少复制文件 · 2026-11-22</summary>

单独清空一张普通表时，数据库会为这张表保留一份恢复用的备份。

</details>

[完整更新日志](CHANGELOG.md)
