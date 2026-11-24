# DBMS C++

用 C++ 写的关系型数据库，支持 SQL、索引和事务，可以用 `psql` 连接。

## 快速开始

[安装编译依赖](docs/PACKAGING.md#编译依赖) · [编译并运行](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>可以用 psql 列出数据库角色 · 2026-11-24</summary>

输入 `\du` 就能查看角色能否登录、连接数限制和密码有效期。直接查询角色目录时，密码字段只显示星号。

</details>

<details>
<summary>可以用 SQL 查询分区归属 · 2026-11-24</summary>

分区根表会返回自己的名称。普通表返回空结果。旧库中已有归属记录的子分区也可以查到它的父表。

</details>

<details>
<summary>查看删除或更新数据时的外键规则 · 2026-11-24</summary>

例如，可以查到删除客户时，外键规则是一起删除相关订单，还是阻止删除客户。表或列改名后，会显示新名称。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
