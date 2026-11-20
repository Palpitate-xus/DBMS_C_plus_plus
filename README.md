# DBMS C++

用 C++ 编写的关系型数据库。支持 SQL 查询和写入，也提供索引、事务、备份与恢复功能。

[使用手册](docs/MANUAL.md) · [功能与限制](docs/compatibility-contract.md) · [文档索引](docs/README.md)

## 在 Linux 上试用

安装[编译依赖](docs/PACKAGING.md#编译依赖)，然后下载和编译。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
```

按[首次登录说明](docs/MANUAL.md#首次登录)准备数据目录和账号，再用下面的命令启动。把 `./dbms-data` 换成你准备的目录。

```bash
./build/cmake/dbms_main -D ./dbms-data
```

## 最近更新

<details>
<summary>反复查看查询步骤时，省去重新计算 · 2026-11-20</summary>

用 `EXPLAIN` 再次查看同一条简单的两表查询时，数据库可以沿用上次的执行步骤。想看查询这次实际处理了多少行，可以用 `EXPLAIN ANALYZE`。

</details>

<details>
<summary>两表查询加了筛选或分页，也能查看执行步骤 · 2026-11-20</summary>

用整数编号匹配两张表时，即使还要筛选、排序或分批取结果，也可以用 `EXPLAIN` 查看数据库怎样查找和匹配记录。

</details>

<details>
<summary>两表按连续编号匹配时，可以直接找到记录 · 2026-11-20</summary>

按整数编号匹配两张表时，如果右表的编号连续且不重复，数据库可以用编号直接找到已读过的记录，省去另建查找表的步骤。

</details>

[完整更新日志](CHANGELOG.md)
