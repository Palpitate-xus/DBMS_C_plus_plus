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
<summary>按编号查询系统类型更快 · 2026-11-22</summary>

例如，查 `pg_type` 中编号为 23 的类型时，数据库只为匹配的记录生成查询结果。

</details>

<details>
<summary>修复复制 pg_type 时选错表的问题 · 2026-11-22</summary>

导入和导出 `pg_type` 时会按当前连接的查找顺序选表。也可以指定列来导出系统类型列表。

</details>

<details>
<summary>修复查询或写入 pg_type 时选错表的问题 · 2026-11-22</summary>

数据库按连接的搜索顺序选择 `pg_type`。你可以用 `public.pg_type` 明确指定自己的表。

</details>

[完整更新日志](CHANGELOG.md)
