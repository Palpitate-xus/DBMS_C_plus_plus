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
<summary>整理表后，范围查询可以少读数据 · 2026-11-18</summary>

运行 `VACUUM FULL` 整理表后，符合条件的查询可以马上用索引跳过无关数据。

</details>

<details>
<summary>写入数据时少找几遍索引文件 · 2026-11-18</summary>

新增、修改或删除数据时，数据库会一起查找相关的索引文件，少做几次重复查找。

</details>

<details>
<summary>按编号翻页时少排一次序 · 2026-11-18</summary>

用整数主键翻页时，如果索引给出的记录已经排好，数据库就直接按这个顺序返回。

</details>

[查看完整更新日志](CHANGELOG.md)

[使用手册](docs/MANUAL.md) · [性能报告](docs/performance-report-2026-10-30.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)
