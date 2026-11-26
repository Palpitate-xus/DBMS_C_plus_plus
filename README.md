# DBMS C++

DBMS C++ 是一个开源关系数据库。你可以用 SQL 存储和查询数据，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>可以查询表、列和其他对象的注释 · 2026-11-26</summary>

用 SQL 可以读回给模式、表、列、索引、视图、序列和类型写的注释，换行和引号也会原样保留。

</details>

<details>
<summary>查其他连接的临时表分组更快 · 2026-11-26</summary>

在额外建了 128 个普通分组的测试里，查其他连接的临时分组从约 17 毫秒降到约 3 毫秒。也修复了旧库里同一个分组编号可能查出不同结果的问题。

</details>

<details>
<summary>可以查出临时表放在哪个分组 · 2026-11-26</summary>

每个连接的临时表放在自己的分组里。现在可以查到这个分组的编号，也可以在列表里隐藏其他连接的临时分组。

</details>

[完整更新日志](CHANGELOG.md)
