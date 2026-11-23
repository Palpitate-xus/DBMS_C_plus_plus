# DBMS C++

用 C++ 写的关系型数据库。支持 SQL 读写、索引和事务，可以用 `psql` 连接。

[快速开始](docs/MANUAL.md#1-快速开始) · [连接数据库](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>部分查询会先返回结果，再报告错误 · 2026-11-23</summary>

例如，前两条记录算出了结果，第三条遇到除以零。客户端会先收到前两条结果，再收到错误消息。

</details>

<details>
<summary>条件互相矛盾时，统计数量可以直接返回 0 · 2026-11-23</summary>

例如，统计库存既大于 10 件又小于 5 件的商品。数据库可以跳过读取商品表，直接返回 0。

</details>

<details>
<summary>多项统计可以只读一遍表 · 2026-11-23</summary>

例如，同时数出有货和缺货的商品各有多少。数据库可以读一遍商品表，算出两个数量。

</details>

[完整更新日志](CHANGELOG.md)
