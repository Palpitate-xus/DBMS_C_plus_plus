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
<summary>可以从上一页最后一个主键接着查 · 2026-11-18</summary>

把上一页最后一个整数主键写进 `WHERE`，数据库可以直接定位到下一页，取够就停。

</details>

<details>
<summary>整数主键翻页时，索引取够就停 · 2026-11-18</summary>

新建或重建的整数主键索引按数字大小排列。只取几行时，可以跳过这页之后的记录。

</details>

<details>
<summary>按整数主键翻页时少读表数据 · 2026-11-18</summary>

按整数主键排序并只取几行时，数据库可以先从索引找出本次需要的记录，再读取对应的表行。

</details>

[查看完整更新日志](CHANGELOG.md)

## 项目链接

[使用手册](docs/MANUAL.md) · [性能报告](docs/performance-report-2026-10-30.md) · [文档索引](docs/README.md)

[报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) · [提交改进](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls) · [第三方声明](THIRD_PARTY_NOTICES)
