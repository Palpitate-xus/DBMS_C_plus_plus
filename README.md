# DBMS C++

一个用 C++ 编写的关系型数据库。支持用 SQL 建表、查询和修改数据，也支持事务。可以用 PostgreSQL 的命令行工具 `psql` 连接。

## 快速开始

在 Linux 上安装[编译依赖](docs/PACKAGING.md#编译依赖)，然后下载并编译。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [功能和限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>查询可以连接视图和普通表 · 2026-11-22</summary>

例如，可以把已保存的销售汇总视图和订单表按订单号连起来，在同一个结果里查看汇总金额和订单明细。

</details>

<details>
<summary>表改列名后，已有视图仍可查询 · 2026-11-22</summary>

例如，订单表把「数量」改成「订购数量」后，已有视图仍用原来的列名显示数量和合计。

</details>

<details>
<summary>列改名后，数据检查规则也会更新 · 2026-11-22</summary>

把「数量」改成「订购数量」后，要求数量为正数的规则仍会生效，自动计算的列也会使用新列名。

</details>

[完整更新日志](CHANGELOG.md)
