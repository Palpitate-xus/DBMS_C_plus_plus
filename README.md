# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上编译并查看版本。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

运行数据库前需要准备数据目录和登录账号。[服务配置](docs/PACKAGING.md#服务端部署形态) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>可以查看表清理和统计更新的进度 · 2026-11-19</summary>

在另一个连接里，可以查看正在处理的表、当前步骤和已读页数。

</details>

<details>
<summary>可以只更新几列的查询统计 · 2026-11-19</summary>

数据改动集中在几列时，可以只更新这些列的统计，供数据库选择查询方式。

</details>

<details>
<summary>一次整理就能收回表和长文本的空间 · 2026-11-19</summary>

运行 `VACUUM FULL` 可以同时收回两处空闲空间，一处在表文件里，另一处在单独存放长文本等大内容的文件里。

</details>

[查看完整更新日志](CHANGELOG.md)

[使用手册](docs/MANUAL.md) · [支持范围](docs/compatibility-contract.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)
