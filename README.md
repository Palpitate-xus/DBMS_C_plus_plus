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
<summary>连接和子查询也能读取继承表 · 2026-11-20</summary>

在连接或子查询中使用父表时，也能查到多层子表里的记录。同一张表在查询里出现两次时，`ONLY` 只限制写了它的那一次。

</details>

<details>
<summary>查询父表时也会读到孙表 · 2026-11-20</summary>

统计父表的记录时，会算上各层子表里的记录。排序和分页也会处理这些记录。加上 `ONLY` 就可以只查指定的表。

</details>

<details>
<summary>修复查询中的引号和文字拼接 · 2026-11-20</summary>

在单表查询中，`upper('we''ird')` 会保留中间的引号，返回 `WE'IRD`。在文字前加上 `prefix:` 再转成大写，`AbA` 会变成 `PREFIX:ABA`。遇到 SQL 空值（`NULL`），结果仍是空值。

</details>

[完整更新日志](CHANGELOG.md)
