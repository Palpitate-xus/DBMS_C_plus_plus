# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引、事务和备份恢复。

## 快速开始

先在 Linux 上[安装编译依赖](docs/PACKAGING.md#编译依赖)。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

按[首次登录说明](docs/MANUAL.md#首次登录)准备数据目录和账号后，用 `./build/cmake/dbms_main -D ./dbms-data` 启动，把 `./dbms-data` 换成你的数据目录。

[功能和限制](docs/compatibility-contract.md) · [文档](docs/README.md)

## 最近更新

<details>
<summary>建表时先把表定义完整写入磁盘 · 2026-11-20</summary>

如果保存表定义时遇到同名文件，建表会报错并保留原文件。

</details>

<details>
<summary>归档缺失或损坏时，恢复操作会保留当前数据 · 2026-11-20</summary>

例如，备份之后新增了一笔订单。如果找不到恢复所需的日志，数据库会报错，订单仍然可以查询。

</details>

<details>
<summary>意外退出后保留已提交的表结构修改 · 2026-11-19</summary>

如果数据库切换到新的日志文件后意外退出，重启时会继续读取这个文件，避免用旧备份覆盖已经提交的表结构修改。

</details>

[完整更新日志](CHANGELOG.md)
