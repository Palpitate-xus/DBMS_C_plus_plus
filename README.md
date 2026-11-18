# DBMS C++

用 C++ 编写的关系型数据库。支持 SQL 查询、数据增删改、索引和事务，可用命令行或 `psql` 连接。项目仍在开发中，已支持的功能见[兼容性说明](docs/compatibility-contract.md)。

## 构建和运行

在 Linux 上构建需要支持 C++17 的编译器。Ubuntu 或 Debian 的依赖安装命令如下。

```bash
sudo apt-get update
sudo apt-get install build-essential cmake pkg-config python3 zlib1g-dev libicu-dev libssl-dev
```

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
```

启动命令行程序前，先准备登录账号和独立的数据目录。 [部署说明](docs/PACKAGING.md#服务端部署形态)

```bash
./build/cmake/dbms_main -D ./build/demo-data
```

登录后执行以下 SQL，最后一条查询会返回「SQL 实战」。

```sql
CREATE DATABASE demo;
SET compatibility_mode = extended;
USE DATABASE demo;

CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
INSERT INTO books VALUES (1, '数据库入门'), (2, 'SQL 实战');
SELECT title FROM books WHERE id = 2;
```

也可以启动服务并用 `psql` 连接。 [连接方法](docs/MANUAL.md#18-网络服务)

## 最近更新

点击标题展开。[完整更新日志](CHANGELOG.md)

<details>
<summary>2026-11-17 · 修复查询结果中的表名和类型名</summary>

修复了部分查询只显示编号、没有显示表名或类型名的问题。

[测试结果](docs/test-report.md#stored-reference-output-20261117) · [性能测试](docs/performance-report-2026-10-30.md#stored-reference-output-cost-20261117)

</details>

<details>
<summary>2026-11-17 · 查询少做一些无用的转换</summary>

部分列的值只有在查询用到时才会转换。

[测试结果](docs/test-report.md#scalar-storage-demand-20261117) · [性能测试](docs/performance-report-2026-10-30.md#scalar-storage-demand-cost-20261117)

</details>

<details>
<summary>2026-11-17 · 减少函数查找开销，修复表结构保存问题</summary>

查找 SQL 函数时减少了数据复制。修复了内存不足时可能只保存一部分数据库结构信息的问题。

[测试结果](docs/test-report.md#namespace-snapshot-20261117) · [性能测试](docs/performance-report-2026-10-30.md#namespace-snapshot-cost-20261117)

</details>

## 文档和贡献

- [使用手册](docs/MANUAL.md) · [部署说明](docs/PACKAGING.md) · [文档索引](docs/README.md)
- [测试报告](docs/test-report.md) · [性能测试报告](docs/performance-report-2026-10-30.md) · [与 PostgreSQL 的对比](docs/postgresql-comparison.md)

运行已登记的测试可用 `bash scripts/build_tests.sh`，各测试命令见[使用手册](docs/MANUAL.md#测试验证)。

欢迎通过 [Issue](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) 报告问题，或通过 [Pull Request](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls) 提交改进。报告问题时请附上版本、复现步骤和实际输出。

## 许可证和致谢

项目授权请向维护者确认。第三方声明见 [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES)。

感谢 [hyrise/sql-parser](https://github.com/hyrise/sql-parser)、[zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree)、[Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS) 和 [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree)。
