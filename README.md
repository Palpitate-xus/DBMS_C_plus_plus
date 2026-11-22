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
<summary>可以导出查询筛选后的数据 · 2026-11-22</summary>

例如，只导出本月的订单，并按金额排序。客户端可以用 `COPY (SELECT ...) TO STDOUT` 逐行接收结果。

</details>

<details>
<summary>按编号查数据类型更快 · 2026-11-22</summary>

例如，查编号 23 对应的类型名时，数据库会先按编号找到对应的类型，再返回名称。

</details>

<details>
<summary>修复导入和导出时选错表的问题 · 2026-11-22</summary>

如果你建了一张叫 `pg_type` 的表，用 `COPY` 导入或导出时，数据库会按当前连接的设置在你的表和自带的类型表之间选择。导出自带的类型表时，可以只取类型名等需要的列。

</details>

[完整更新日志](CHANGELOG.md)
