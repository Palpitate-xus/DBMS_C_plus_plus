# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引、事务和备份恢复。

## 快速开始

先在 Linux 上[安装编译依赖](docs/PACKAGING.md#编译依赖)。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

按[首次登录说明](docs/MANUAL.md#首次登录)准备数据目录和账号后，用 `./build/cmake/dbms_main -D ./dbms-data` 启动，把 `./dbms-data` 换成你的数据目录。

[功能和限制](docs/compatibility-contract.md) · [文档](docs/README.md)

## 最近更新

<details>
<summary>查询函数会保留参数里的引号和拼接 · 2026-11-20</summary>

在单表查询里，`upper('a''b')` 现在返回 `A'B`。给列值加前缀再转大写时，也会保留每一行的值和空值。

</details>

<details>
<summary>重启后会继续清理旧恢复文件 · 2026-11-20</summary>

如果数据库在删除已经用完的建表恢复文件时意外退出，下次启动会接着清理，避免旧文件一直占用空间。

</details>

<details>
<summary>外部表目录暂时不可用时，保留恢复记录 · 2026-11-20</summary>

如果意外退出后找不到保存表文件的外部目录，数据库会报错并保留清理记录。目录恢复后重新启动，就能继续清理未提交的新表。

</details>

[完整更新日志](CHANGELOG.md)
