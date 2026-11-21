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
<summary>按编号更新时少读无关记录 · 2026-11-21</summary>

例如按订单编号更新记录。编号列没有索引时，数据库会先在扫描中检查编号，再对可能匹配的记录做完整判断。普通查询也会直接沿用已经确定的列类型。

</details>

<details>
<summary>普通表最多可以建 1,600 列 · 2026-11-20</summary>

列数上限从 30 提高到 1,600。实际能用多少列还取决于列的类型和每行数据的大小。同时修复了某些大批写入尚未提交时意外退出、导致数据库无法重启的问题。

</details>

<details>
<summary>反复查看查询步骤时，省去重新计算 · 2026-11-20</summary>

用 `EXPLAIN` 再次查看同一条简单的两表查询时，数据库可以沿用上次的执行步骤。想看查询这次实际处理了多少行，可以用 `EXPLAIN ANALYZE`。

</details>

[完整更新日志](CHANGELOG.md)
