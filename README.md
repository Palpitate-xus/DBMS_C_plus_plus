# DBMS C++

用 C++ 编写的关系型数据库。支持 SQL 查询、索引和事务，可以用 PostgreSQL 的命令行工具 `psql` 连接。

## 快速开始

在 Linux 上安装[编译依赖](docs/PACKAGING.md#编译依赖)，再下载并编译。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动和登录](docs/MANUAL.md#运行) · [支持的功能](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>改名后，订单号会接着生成 · 2026-11-22</summary>

例如，订单号已经发到 42。把订单表所在的「销售」改名为「销售归档」后，下一笔订单仍从 43 开始。

</details>

<details>
<summary>可以改一组表共用的名称前缀 · 2026-11-22</summary>

例如，把 `sales` 改成 `sales_archive` 后，可以用 `sales_archive.orders` 查原来的订单表，之前建好的报表也能继续使用。

</details>

<details>
<summary>表改名或换位置后，原来的报表还能用 · 2026-11-22</summary>

例如，订单表从销售区移到归档区后，原来的报表仍会查询这张表。即使用旧名字新建一张表，报表也会继续读原来的订单。

</details>

[完整更新日志](CHANGELOG.md)
