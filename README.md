# DBMS C++

C++ 编写的 SQL 数据库，支持查询、索引和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上编译。[所需软件](docs/PACKAGING.md)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

编译后，先准备数据目录和登录账号，再启动服务。[准备数据目录和账号](docs/PACKAGING.md#目录约定显式-data-directory) · [启动服务并用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>长值文件可以单独缩小 · 2026-11-18</summary>

删除许多长文本后，可以只整理存放这些内容的文件，把不再使用的空间还给磁盘。

</details>

<details>
<summary>可以分开清理主表和长值 · 2026-11-18</summary>

普通 `VACUUM` 可以只清理主表，也可以只清理单独存放的长值。还可以只更新统计信息。

</details>

<details>
<summary>清理表时可以少读一遍数据 · 2026-11-18</summary>

没有额外存放长值时，`VACUUM` 会省掉查找这些值的第二遍扫描。

</details>

[查看完整更新日志](CHANGELOG.md)

[使用手册](docs/MANUAL.md) · [性能报告](docs/performance-report-2026-10-30.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)
