# DBMS C++

一个用 C++ 编写的关系型数据库。支持 SQL 查询、索引和事务，可用 `psql` 连接。

## 快速开始

在 Linux 上安装依赖后，运行下列命令编译并查看版本。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [功能和限制](docs/compatibility-contract.md) · [文档索引](docs/README.md)

## 最近更新

<details>
<summary>开事务时，少做一轮表结构检查 · 2026-11-22</summary>

数据库会用同一轮检查判断是否需要保存表结构。目录更新时，已经在用的旧版本仍可继续读取。

[性能报告](docs/performance-report-2026-10-30.md#catalog-publication-one-scan-cost-20261122)

</details>

<details>
<summary>有人改表时，其他连接仍可查看和修改自己的设置 · 2026-11-22</summary>

例如，另一连接正在改表时，单独查询当前连接的锁等待时间，无需等改表结束。手册列出了适用的 SQL 写法。

[使用说明](docs/MANUAL.md#在-sql-表达式里读取和修改设置)

</details>

<details>
<summary>查询里也能查看和修改连接设置 · 2026-11-22</summary>

可以在 SQL 查询里查看或调整当前连接的锁等待时间。也可以只为当前事务调整。

[使用示例](docs/MANUAL.md#在-sql-表达式里读取和修改设置)

</details>

[完整更新日志](CHANGELOG.md)
