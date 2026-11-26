# DBMS C++

一个用 C++ 编写的关系数据库，支持 SQL，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>可以查看临时模式的编号和归属 · 2026-11-26</summary>

用 `pg_my_temp_schema()` 可以查当前连接的临时模式编号。列出模式时，也可以用 `pg_is_other_temp_schema()` 排除其他连接的临时模式。

</details>

<details>
<summary>启用临时模式时减少目录处理 · 2026-11-26</summary>

首次启用临时模式时，数据库省去了重新整理表和列信息的步骤。在多建了 128 张表的测试库里，用时从约 16–18 毫秒降到约 14–15 毫秒。

</details>

<details>
<summary>首次查询临时模式更快 · 2026-11-26</summary>

用 `current_schema` 首次启用临时模式时，数据库省去了整库备份。在多建了 128 张表的测试库里，这步从约 108 毫秒降到约 17 毫秒。

</details>

[完整更新日志](CHANGELOG.md)
