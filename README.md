# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上装好[编译依赖](docs/PACKAGING.md#编译依赖)，然后运行下面的命令。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
```

[启动和登录](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [支持的功能和限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>同名表不会让新建的视图读错数据 · 2026-11-22</summary>

例如，报表创建时用的是销售部门的订单表。即使后来有人建了同名的临时表，报表仍会读取原来的订单。

</details>

<details>
<summary>可以把表和视图里的数据合起来查 · 2026-11-22</summary>

例如，销售汇总视图里只有产品编号和金额。用产品编号匹配产品表，就能同时查出产品名称和销售总额。

</details>

<details>
<summary>表里的列改名后，已有视图照常可用 · 2026-11-22</summary>

例如，订单表把「数量」改成「订购数量」后，已有视图里的这一列仍叫「数量」。

</details>

[完整更新日志](CHANGELOG.md)
