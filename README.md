# DBMS C++

一个用 C++ 编写的关系型数据库。支持 SQL 查询、索引和事务，可用 `psql` 连接。

## 快速开始

在 Linux 上[安装编译依赖](docs/PACKAGING.md#编译依赖)，然后下载并编译项目。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

[配置和启动服务](docs/PACKAGING.md#服务端部署形态) · [使用手册](docs/MANUAL.md) · [支持的功能和限制](docs/compatibility-contract.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)

## 最近更新

<details>
<summary>按整数主键更新或删除时少读数据 · 2026-11-19</summary>

数据库能从主键索引确认目标行时，可以直接读取这一行所在的页。

</details>

<details>
<summary>能看到分区表的统计进度 · 2026-11-19</summary>

更新分区表的统计时，可以查看已经处理了多少个分区，以及当前分区读到了哪里。

</details>

<details>
<summary>整理表时少反复找空位 · 2026-11-19</summary>

`VACUUM FULL` 整理表时会记住哪里还有空间，搬动记录时省去反复查找。

</details>

[查看完整更新日志](CHANGELOG.md)
