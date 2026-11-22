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
<summary>列改名后，数据检查规则也会更新 · 2026-11-22</summary>

把「数量」改成「订购数量」后，要求数量为正数的规则仍会生效，自动计算的列也会使用新列名。

</details>

<details>
<summary>导入时可以跳过数值超出自定义范围的行 · 2026-11-22</summary>

例如，给数量定义了「必须为正数」的类型后，可以设置跳过数量为负的行。如果遇到重复的订单号，整批订单仍会撤销。

</details>

<details>
<summary>数字列误填文字时，导入可以继续 · 2026-11-22</summary>

例如，把「三件」填进整数列时，可以只跳过这一行。也可以设定最多跳过多少行，超出上限就撤销整批导入。

</details>

[完整更新日志](CHANGELOG.md)
