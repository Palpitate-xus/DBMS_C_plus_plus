# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务。

## 快速开始

先在 Linux 上[安装编译依赖](docs/PACKAGING.md#编译依赖)。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[首次运行](docs/MANUAL.md#首次登录) · [支持的功能和限制](docs/compatibility-contract.md) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>修复临时表导致的重启失败 · 2026-11-19</summary>

数据库异常退出后，重启时会清理确认是上次连接留下的临时表。已提交的普通表数据仍可读取。

</details>

<details>
<summary>建表时为以后更新记录留出空间 · 2026-11-19</summary>

用 `WITH (fillfactor = 90)` 建表，写入时会尽量留出约一成空间，方便以后更新记录。这个设置重启后仍然有效。

</details>

<details>
<summary>插入记录遇到占用时，先试别的空位 · 2026-11-19</summary>

如果原本要写入的位置正被占用，数据库会先试别的空位。有地方可写时，就不用等原来的位置腾出来。

</details>

[完整更新日志](CHANGELOG.md)
