# DBMS C++

一个用 C++ 编写的关系型数据库，可以用 SQL 读写数据，支持索引和事务。

[编译和启动](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>读取和导出生成的多行数据更快 · 2026-11-23</summary>

在一组测试中，读完生成的 4,096 行数据，用时从约 100 毫秒降到约 60 毫秒。导出同样的数据也更快。

</details>

<details>
<summary>可以把多组生成的数据并排显示 · 2026-11-23</summary>

例如，把 3 个编号和 2 个名字并排查询，结果会有 3 行，最后一行的名字为空。

</details>

<details>
<summary>只要前几个数字，就只生成这几个 · 2026-11-23</summary>

例如，用 SQL 生成从 1 到 10 万的数字，只取开头的 3 个。数据库生成这 3 个就能返回结果。

</details>

[完整更新日志](CHANGELOG.md)
