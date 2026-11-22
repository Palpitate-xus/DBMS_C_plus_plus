# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上装好依赖后，下载源码并编译。最后一行会显示版本号。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库并连接](docs/MANUAL.md#18-网络服务) · [支持哪些功能](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>可以导出 CSV · 2026-11-22</summary>

可以导出整张表，也可以先筛选、排序再导出。第一行可以带上列名，方便用表格软件打开。

</details>

<details>
<summary>有连接正在保存表结构时，其他查询可以等待 · 2026-11-22</summary>

多个连接同时开始事务时，短暂的保存工作完成后，等待中的查询可以继续。超过设定的等待时间会报告锁超时。

</details>

<details>
<summary>可以导出查询筛选后的数据 · 2026-11-22</summary>

例如，只导出本月的订单，并按金额排序。客户端可以用 `COPY (SELECT ...) TO STDOUT` 逐行接收结果。

</details>

[完整更新日志](CHANGELOG.md)
