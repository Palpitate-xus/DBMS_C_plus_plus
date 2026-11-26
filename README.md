# DBMS C++

一个用 C++ 编写的关系数据库，支持 SQL，也可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [编译依赖](docs/PACKAGING.md#编译依赖) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>可以查到当前连接的临时模式名 · 2026-11-26</summary>

把 `pg_temp` 放在查找顺序的最前面后，运行 `SELECT current_schema` 会返回这个连接自己的临时模式名。

</details>

<details>
<summary>查找不存在的临时表时，错误提示更准确 · 2026-11-26</summary>

查临时表的编号时，报错会区分当前连接还没创建过临时表或序列，还是已经创建过但找不到指定的表。

</details>

<details>
<summary>查表编号时可以带上当前数据库名 · 2026-11-26</summary>

连接到 `shop` 数据库后，可以用 `shop.public.orders` 查订单表的编号。

</details>

[完整更新日志](CHANGELOG.md)
