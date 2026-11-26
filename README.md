# DBMS C++

DBMS C++ 是一个用 C++ 编写的数据库，支持用 SQL 管理数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [更多文档](docs/README.md)

## 最近更新

<details>
<summary>按编号统计注释更快 · 2026-11-26</summary>

在有 1,024 条额外注释的测试库里，按编号条件统计注释从约 264 毫秒降到约 28 毫秒。

</details>

<details>
<summary>可以查看注释查询怎么执行 · 2026-11-26</summary>

用 `EXPLAIN` 可以查看查询注释的步骤。程序也能按名字找到注释目录 `pg_description`。

</details>

<details>
<summary>修改注释所需的临时备份更小 · 2026-11-26</summary>

在另有 128 张表的测试库里，只修改一条注释时，临时备份从约 131 KB 降到不到 1 KB。

</details>

[完整更新日志](CHANGELOG.md)
