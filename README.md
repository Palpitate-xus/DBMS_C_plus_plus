# DBMS C++

用 C++ 写的关系型数据库，支持 SQL 查询、索引和事务。可以用 `psql` 连接。

## 快速开始

在 Linux 上下载并编译。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [支持的功能](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>截取文字再筛选更快了 · 2026-11-22</summary>

比如取姓名的第一个字，再找出所有姓张的人。把名称转成小写再比较也更快了。

</details>

<details>
<summary>按名称查找更快了 · 2026-11-22</summary>

比如按名称查一项数据库设置，或在表里查找名称完全相同的记录。

</details>

<details>
<summary>可以把几项数据库设置放在一起比较 · 2026-11-22</summary>

比如列出两项超时设置，按各自的毫秒数排序。

</details>

[完整更新日志](CHANGELOG.md)
