# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL、索引和事务，可用 `psql` 连接。

## 快速开始

在 Linux 上安装依赖后，在源码目录编译。[下载源码](docs/MANUAL.md#编译) · [编译依赖](docs/PACKAGING.md#编译依赖)

```bash
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
```

[启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [功能和限制](docs/compatibility-contract.md) · [文档索引](docs/README.md)

## 最近更新

<details>
<summary>修复查询或写入 pg_type 时选错表的问题 · 2026-11-22</summary>

数据库按连接的搜索顺序选择 `pg_type`。你可以用 `public.pg_type` 明确指定自己的表。

</details>

<details>
<summary>按一长串值筛选时，更快看到查询计划 · 2026-11-22</summary>

在一项按 32 个值筛选的测试中，用 `EXPLAIN` 查看查询计划的时间约减半。

</details>

<details>
<summary>开始事务时少做一轮表结构检查 · 2026-11-22</summary>

测试中，执行 `BEGIN` 少了 22 次文件或目录状态检查。

</details>

[完整更新日志](CHANGELOG.md)
