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
<summary>重启后会清理没有提交的新表 · 2026-11-20</summary>

如果在事务里新建了普通表，还没提交数据库就意外退出，重启后之前已提交的数据仍可查询，原来的表名也能再次使用。

</details>

<details>
<summary>保存表清单时可重试一次 · 2026-11-20</summary>

如果新清单已写好，只是没能确认写入磁盘，数据库会再确认一次。重试仍失败时会报错。

</details>

<details>
<summary>建表时先把表定义完整写入磁盘 · 2026-11-20</summary>

如果保存表定义时遇到同名文件，建表会报错并保留原文件。

</details>

[完整更新日志](CHANGELOG.md)
