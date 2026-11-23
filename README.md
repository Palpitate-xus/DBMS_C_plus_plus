# DBMS C++

一个用 C++ 编写的关系型数据库，可以用 SQL 读写数据，支持索引和事务。

[编译和启动](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>写有长数组的查询更快返回 · 2026-11-23</summary>

当 SQL 里列出几千个数字时，数据库少读几遍同样的文字。查询即使只取第一行，也能更快返回。

</details>

<details>
<summary>展开长整数数组更快 · 2026-11-23</summary>

数据里有几千个整数时，数据库可以少做很多重复的类型查找，完整读取和只取前几行都能更快返回。

</details>

<details>
<summary>可以把一个或多个数组展开成行 · 2026-11-23</summary>

例如，把 3 个编号和 2 个名字并排展开。结果有 3 行，最后一行的名字为空。还可以给每行编号，再筛选或排序。

</details>

[完整更新日志](CHANGELOG.md)
