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
<summary>直接比对文字更快了 · 2026-11-22</summary>

按完整名称查找记录更快了。给已有数据的表加唯一约束时，检查重复值也更快了。

</details>

<details>
<summary>截取文字再筛选更快了 · 2026-11-22</summary>

比如取姓名的第一个字，再找出所有姓张的人。把名称转成小写再比较也更快了。

</details>

<details>
<summary>查数据库设置更快了 · 2026-11-22</summary>

比如按名称查当前的工作内存上限。

</details>

[完整更新日志](CHANGELOG.md)
