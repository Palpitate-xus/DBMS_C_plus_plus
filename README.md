# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务，可用 `psql` 连接。

[编译与启动](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>统计符合条件的记录更快 · 2026-11-23</summary>

比如同时统计有库存的商品和缺货商品各有多少。读一遍商品表就可以得到两个数字。

</details>

<details>
<summary>按文字长度和数字筛选更快 · 2026-11-23</summary>

比如找名称超过两个字、库存大于零的商品。数据库在读取时就能筛掉不符合条件的商品。

</details>

<details>
<summary>符合条件的订单很少时，翻页更快 · 2026-11-23</summary>

比如翻看一家店里带详细备注的订单。即使这样的订单很少，数据库也能沿着订单号继续查找，直到填满一页或查完这家店的订单。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
