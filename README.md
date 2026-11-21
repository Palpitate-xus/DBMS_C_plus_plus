# DBMS C++

一个用 C++ 编写的关系型数据库，支持 SQL 查询、索引、事务和备份恢复。

## 快速开始

在 Linux 上安装好编译依赖后，下载并编译。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
```

准备好数据目录和登录账号后，用下面的命令启动。把 `./dbms-data` 换成你准备的目录。[首次登录](docs/MANUAL.md#首次登录)

```bash
./build/cmake/dbms_main -D ./dbms-data
```

## 最近更新

<details>
<summary>比较同一条记录里的两列时返回正确结果 · 2026-11-21</summary>

例如 `WHERE id = peer_id` 会比较每条记录里的两个值。给 `id` 建索引后，查询仍会找到全部符合条件的记录。两个相差 1 的大整数也能正确区分。

</details>

<details>
<summary>查询时能区分数字和列名 · 2026-11-21</summary>

`WHERE id = 7` 查找编号为 7 的记录。`WHERE id = "7"` 比较同一条记录里的 `id` 和名为 `"7"` 的列。

</details>

<details>
<summary>按编号删除时少读数据 · 2026-11-21</summary>

按整数编号删除时，即使编号列没有索引，数据库也能减少重复读取。

</details>

[完整更新日志](CHANGELOG.md) · [使用手册](docs/MANUAL.md) · [功能与限制](docs/compatibility-contract.md) · [文档索引](docs/README.md)
