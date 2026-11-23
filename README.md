# DBMS C++

一个用 C++ 写的关系型数据库，支持 SQL 读写、索引和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上装好依赖后，从仓库目录运行以下命令。最后一条会显示版本号。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>带时区的时间可以按指定位数四舍五入 · 2026-11-23</summary>

例如，要求保留两位小数时，`14:30:00.125+05:30` 会变成 `14:30:00.13+05:30`。

</details>

<details>
<summary>临时表检查也覆盖还没执行的查询 · 2026-11-23</summary>

有些应用会先登记一笔事务，再决定何时提交。即使应用只准备了涉及临时表的查询，还没有运行，数据库也会拒绝登记，并撤销这笔事务的修改。

</details>

<details>
<summary>在 SQL 里直接列出多行数据时，查询准备更快 · 2026-11-23</summary>

在一次测试中，直接在 SQL 里列出 128 行数据，准备这条查询的时间从约 13 毫秒降到约 5 毫秒。

</details>

[完整更新日志](CHANGELOG.md)
