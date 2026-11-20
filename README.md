# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引、事务和备份恢复。

## 快速开始

在 Linux 上[安装编译依赖](docs/PACKAGING.md#编译依赖)后，下载并编译。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
```

按[首次登录说明](docs/MANUAL.md#首次登录)准备数据目录和账号，再启动数据库。把 `./dbms-data` 换成你准备的目录。

```bash
./build/cmake/dbms_main -D ./dbms-data
```

[功能和限制](docs/compatibility-contract.md) · [文档](docs/README.md)

## 最近更新

<details>
<summary>修复查询中的引号和文字拼接 · 2026-11-20</summary>

在单表查询中，`upper('we''ird')` 会保留中间的引号，返回 `WE'IRD`。在文字前加上 `prefix:` 再转成大写，`AbA` 会变成 `PREFIX:ABA`。遇到 SQL 空值（`NULL`），结果仍是空值。

</details>

<details>
<summary>重启后会继续清理建表留下的旧文件 · 2026-11-20</summary>

如果意外退出打断了清理，下次启动会继续删除已经用完的建表恢复文件，避免它们一直占用磁盘空间。

</details>

<details>
<summary>找回表文件目录后可以继续恢复 · 2026-11-20</summary>

意外退出后，如果启动时找不到保存表文件的外部目录，数据库会报错并保留恢复所需的记录。目录重新可用后，再启动就能继续清理还没提交的新表。

</details>

[完整更新日志](CHANGELOG.md)
