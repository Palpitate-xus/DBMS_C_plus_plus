# DBMS C++

一个用 C++ 编写的关系型数据库。可以用 SQL 建表、查询和修改数据，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>按表名查看列信息更快 · 2026-11-26</summary>

把表名直接写在 SQL 里查列名和类型，数据库可以跳过其他表的列。在另有 32 张表的测试库里，列出四列的用时从约 95 毫秒降到约 6 毫秒。

</details>

<details>
<summary>按表编号查看列信息更快 · 2026-11-26</summary>

用表的数字编号查列名和类型时，更多写法可以直接找到这张表。在另有 32 张表的测试库里，列出四列的用时从约 97 毫秒降到约 5 毫秒。

</details>

<details>
<summary>修复撤销表结构改动后关闭数据库时的崩溃 · 2026-11-26</summary>

通过 C++ 使用数据库时，撤销对表结构的修改后，可以正常关闭数据库。

</details>

[完整更新日志](CHANGELOG.md)
