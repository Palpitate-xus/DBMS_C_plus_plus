# DBMS C++

用 C++ 编写的关系型数据库。可以用 SQL 建表、写入和查询数据，支持索引和事务。

## 快速开始

在 Linux 上先[安装编译依赖](docs/PACKAGING.md#编译依赖)，再运行下面的命令编译并查看版本。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动和登录](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [功能和限制](docs/compatibility-contract.md)

## 最近更新

<details>
<summary>部分分区表现在可以删记录 · 2026-11-22</summary>

可以按条件删除记录、查看删掉了哪些行，也可以在提交前撤销。[适用范围和示例](docs/MANUAL.md#删除分区表中的记录)

</details>

<details>
<summary>查整张分区表时，少做重复工作 · 2026-11-22</summary>

在一张有 60 个分区的测试表上，查询整张表比上一个版本少花了约 13% 到 18% 的时间。[测试结果](docs/performance-report-2026-10-30.md#partition-scan-reuse-cost-20261122)

</details>

<details>
<summary>按编号范围查找时，跳过无关分区 · 2026-11-22</summary>

比如订单按编号分段存放。查找 100 到 199 号订单时，数据库可以跳过肯定没有这些订单的分区。[使用说明](docs/MANUAL.md#15-分区表)

</details>

[完整更新日志](CHANGELOG.md) · [更多文档](docs/README.md)
