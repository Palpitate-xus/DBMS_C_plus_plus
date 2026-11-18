# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务，可用 `psql` 连接。

## 快速开始

在 Linux 上用 CMake 构建。所需软件见[部署说明](docs/PACKAGING.md)。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

[创建数据库并运行第一条 SQL](docs/MANUAL.md#首次登录) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>排序时减少不必要的数组转换 · 2026-11-18</summary>

一些排序查询会先排好行，再转换实际取到的数组。只取第一行时，后面的无效数组不会提前报错。

</details>

<details>
<summary>可以从更多子查询写入数据 · 2026-11-18</summary>

子查询里连接了多张表，也能把结果写入另一张表。只取一行时，取够后就停止读取。

</details>

<details>
<summary>连接表时直接查找可能匹配的数组 · 2026-11-18</summary>

按整数数组连接表时，右侧读完后可以直接找到可能匹配的行，减少逐行检查。

</details>

[查看完整更新日志](CHANGELOG.md)

## 文档

[使用手册](docs/MANUAL.md) · [支持的功能和限制](docs/compatibility-contract.md) · [性能报告](docs/performance-report-2026-10-30.md) · [全部文档](docs/README.md)

[报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) · [提交改进](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls)

## 许可证和致谢

项目授权请向维护者确认。第三方声明见 [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES)。

感谢 [hyrise/sql-parser](https://github.com/hyrise/sql-parser)、[zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree)、[Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS) 和 [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree)。
