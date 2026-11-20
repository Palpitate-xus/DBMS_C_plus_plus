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
<summary>重复查看连接计划时省去重新计算 · 2026-11-20</summary>

再次用 `EXPLAIN` 查看同一条简单连接时，如果上次的计划仍可用，就会直接显示它。用 `EXPLAIN ANALYZE` 时，数据库仍会运行查询，以显示这次处理了多少行。

</details>

<details>
<summary>带筛选和分页的连接也能查看执行计划 · 2026-11-20</summary>

按整数编号连接两张表时，即使查询加了筛选、排序或只取前几条，也能查看数据库怎样查找和合并记录。用 `EXPLAIN ANALYZE` 可以看到实际处理了多少行。

</details>

<details>
<summary>连接时可以直接找到连续编号的记录 · 2026-11-20</summary>

如果右表读出的编号连续、没有重复，数据库可以用编号直接找到已经读过的记录，省去把编号再存一遍。

</details>

[完整更新日志](CHANGELOG.md)
