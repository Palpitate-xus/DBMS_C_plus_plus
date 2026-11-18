# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务。可以在命令行中使用，也可以用 `psql` 连接。

## 快速开始

在 Linux 上用 CMake 构建。编译环境和依赖见[部署说明](docs/PACKAGING.md)。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

准备好登录账号和数据目录后，就可以创建数据库并运行 SQL。[首次登录](docs/MANUAL.md#首次登录) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>整数数组连接少做重复工作 · 2026-11-18</summary>

用整数数组连接表时，可以更早跳过不匹配的行，省去重复解析数组和复制数据。

</details>

<details>
<summary>连接表时保留数组类型 · 2026-11-18</summary>

用数组列连接两张表后，结果会保留正确的类型。同一种数组规则已经检查过的值，原样复制时可以省掉重复检查。

</details>

<details>
<summary>修复子查询中的数组读写和排序 · 2026-11-18</summary>

读取或复制子查询的数组时会保留类型、起始下标和空值。整数数组按值排序，`{2}` 排在 `{10}` 前面。

</details>

[查看完整更新日志](CHANGELOG.md)

## 文档和贡献

- [使用手册](docs/MANUAL.md) · [已支持的功能和限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)
- [运行测试](docs/MANUAL.md#测试验证) · [测试报告](docs/test-report.md) · [性能报告](docs/performance-report-2026-10-30.md)
- [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) · [提交改进](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls)

## 许可证和致谢

项目授权请向维护者确认。第三方声明见 [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES)。

感谢 [hyrise/sql-parser](https://github.com/hyrise/sql-parser)、[zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree)、[Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS) 和 [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree)。
