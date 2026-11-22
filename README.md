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
<summary>按文字精确查找更快了 · 2026-11-22</summary>

例如，按名字找一项数据库设置，或筛出名称完全相同的几行。

</details>

<details>
<summary>可以用 SQL 比较数据库的当前设置 · 2026-11-22</summary>

例如，把两种超时设置列在一起，再按毫秒数排序。

</details>

<details>
<summary>改名后，订单号会接着生成 · 2026-11-22</summary>

例如，订单号已经发到 42。把订单表所在的「销售」改名为「销售归档」后，下一笔订单仍从 43 开始。

</details>

[完整更新日志](CHANGELOG.md)
