# DBMS C++

用 C++ 编写的关系型数据库。可以用 SQL 建表、写入和查询数据，也可以用 `psql` 连接。

## 快速开始

在 Linux 上先安装编译依赖。[依赖清单](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动和首次登录](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [支持的功能](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>可以删除分区表里的记录 · 2026-11-22</summary>

在已经建好分区的表里，可以按条件删除记录。用 `RETURNING` 能看到删掉的行，提交前也可以撤销。[使用说明](docs/MANUAL.md#删除分区表中的记录)

</details>

<details>
<summary>读取多个分区时，少查表结构 · 2026-11-22</summary>

一次查询要读取多个分区时，扫描可以共用已经读到的表结构，省掉为每个分区重复查表结构的工作。[使用说明](docs/MANUAL.md#15-分区表)

</details>

<details>
<summary>按编号查找时，跳过无关分区 · 2026-11-22</summary>

比如订单按编号区间分开存放。查找 100 到 199 号订单时，数据库可以跳过不可能包含这些订单的分区。[使用说明](docs/MANUAL.md#15-分区表)

</details>

[完整更新日志](CHANGELOG.md)
