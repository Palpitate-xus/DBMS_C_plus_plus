# DBMS C++

一个用 C++ 编写的关系型数据库。支持 SQL 查询、索引和事务，可用 `psql` 连接。

## 快速开始

在 Linux 上先安装编译依赖，再下载和编译。[依赖安装说明](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [支持的功能](docs/compatibility-contract.md)

## 最近更新

<details>
<summary>可以在查询里查看和修改连接设置 · 2026-11-22</summary>

应用可以用 SQL 函数查看当前的等待时间，也可以为一次事务临时调整等待时间或查询使用的内存。[使用示例](docs/MANUAL.md#在-sql-表达式里读取和修改设置)

</details>

<details>
<summary>回滚事务时，会恢复之前的超时设置 · 2026-11-22</summary>

可以只为当前事务缩短锁等待时间，结束后恢复原来的设置。用 `SHOW` 可以随时查看当前值。[使用示例](docs/MANUAL.md#在事务里临时调整超时)

</details>

<details>
<summary>等不到更严格的表锁，原来的锁会保留 · 2026-11-22</summary>

如果你已锁住一张表，再请求更严格的锁时遇到超时或取消，原来的锁仍然有效。也可以让数据库一遇到冲突就报错。[使用示例](docs/MANUAL.md#在事务里锁住一张表)

</details>

[完整更新日志](CHANGELOG.md) · [更多文档](docs/README.md)
