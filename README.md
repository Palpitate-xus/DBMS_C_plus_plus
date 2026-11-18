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
<summary>为数组设置默认值和检查规则 · 2026-11-18</summary>

多张表可以共用同一套规则，例如要求每个数组最多有三项。

</details>

<details>
<summary>修复查询结果只显示编号的问题 · 2026-11-17</summary>

受影响的查询现在会显示对应的表名或类型名。

</details>

<details>
<summary>减少查询中的多余处理 · 2026-11-17</summary>

部分查询会跳过未用到的列的类型转换。

</details>

[查看完整更新日志](CHANGELOG.md)

## 文档和贡献

- [使用手册](docs/MANUAL.md) · [已支持的功能和限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)
- [运行测试](docs/MANUAL.md#测试验证) · [测试报告](docs/test-report.md) · [性能报告](docs/performance-report-2026-10-30.md)
- [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) · [提交改进](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls)

## 许可证和致谢

项目授权请向维护者确认。第三方声明见 [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES)。

感谢 [hyrise/sql-parser](https://github.com/hyrise/sql-parser)、[zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree)、[Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS) 和 [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree)。
