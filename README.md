# DBMS C++

用 C++ 写的关系型数据库，支持 SQL、索引和事务，可以用 `psql` 连接。

## 快速开始

[安装编译依赖](docs/PACKAGING.md#编译依赖) · [编译并运行](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>可以用 SQL 查询分区归属 · 2026-11-24</summary>

分区根表会返回自己的名称。普通表返回空结果。旧库中已有归属记录的子分区也可以查到它的父表。

</details>

<details>
<summary>查看删除或更新数据时的外键规则 · 2026-11-24</summary>

例如，可以查到删除客户时，外键规则是一起删除相关订单，还是阻止删除客户。表或列改名后，会显示新名称。

</details>

<details>
<summary>查看外键引用了哪张表、哪一列 · 2026-11-24</summary>

例如，可以查到订单表的客户编号对应客户表的哪一列，也能看到旧订单是否已经通过外键检查。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
