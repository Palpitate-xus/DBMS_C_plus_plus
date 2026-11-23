# DBMS C++

一个用 C++ 编写的关系型数据库。可以用 SQL 存取数据、创建索引和运行事务，也可以用 `psql` 连接。

## 快速开始

[安装编译依赖（Linux）](docs/PACKAGING.md#编译依赖)，然后在仓库目录运行下面的命令。

```bash
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库并运行 SQL](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>建子表时可以指明父表的位置 · 2026-11-23</summary>

例如，`live.orders` 和 `archive.orders` 同名。创建订单子表时，写 `INHERITS (live.orders)` 就会选中 `live` 里的父表。

</details>

<details>
<summary>分页带上并列记录时也可以用索引 · 2026-11-23</summary>

例如，查最早的 3 条预约并带上与第 3 条时间相同的预约。给时间建好索引后，数据库可以读到下一个不同的时间就停。

</details>

<details>
<summary>按时间排序，只取前几条更快 · 2026-11-23</summary>

例如，给预约时间建好索引后，查询最早的 4 条预约时，数据库读够 4 条就可以停。预约时间可以带时区偏移，例如 `09:00:00+08:00`。

</details>

[完整更新日志](CHANGELOG.md)
