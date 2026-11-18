# DBMS C++

一个用 C++ 编写的数据库，支持 SQL、索引和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上用 CMake 构建。[所需软件](docs/PACKAGING.md)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

[开始使用](docs/MANUAL.md#首次登录) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>排序时少做数组转换 · 2026-11-18</summary>

部分查询会先排序，再转换实际需要的数组。例如，只取排序后的第一行时，可以只转换这一行的数组。

</details>

<details>
<summary>能把更多查询结果写入表 · 2026-11-18</summary>

一些查询可以先连接多张表，再把结果写入另一张表。指定只取几行时，取够就停止。

</details>

<details>
<summary>按整数数组匹配时少检查一些行 · 2026-11-18</summary>

用整数数组连接两张表时，数据库可以先找出可能匹配的行，减少逐行比较。

</details>

[查看完整更新日志](CHANGELOG.md)

## 项目链接

[使用手册](docs/MANUAL.md) · [性能报告](docs/performance-report-2026-10-30.md) · [文档索引](docs/README.md)

[报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) · [提交改进](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls) · [第三方声明](THIRD_PARTY_NOTICES)
