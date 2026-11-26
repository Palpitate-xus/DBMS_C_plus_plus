# DBMS C++

一个用 C++ 编写的关系数据库，支持 SQL，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>查找不存在的临时表时，错误提示更准确 · 2026-11-26</summary>

查临时表的编号时，报错会区分当前连接还没创建过临时表或序列，还是已经创建过但找不到指定的表。

</details>

<details>
<summary>查表编号时可以带上当前数据库名 · 2026-11-26</summary>

连接到 `shop` 数据库后，可以用 `shop.public.orders` 查订单表的编号。

</details>

<details>
<summary>应用传入表名时，查看列信息更快 · 2026-11-26</summary>

应用把表名作为参数传给查询时，可以更快拿到列名和类型。在另外建了 128 张表的测试库里，列出目标表的四列用时从约 320 毫秒降到约 17 毫秒。

</details>

[完整更新日志](CHANGELOG.md)
