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
<summary>重复执行建表脚本时减少备份 · 2026-11-19</summary>

用 `CREATE TABLE IF NOT EXISTS` 重跑脚本时，可以直接跳过已有表。数据库也会在备份之前指出填充比例越界等设置错误。

</details>

<details>
<summary>恢复表结构时保留序列已经给出的编号 · 2026-11-19</summary>

数据库异常退出后，如果需要撤销表结构修改，恢复时会保留仍存在的序列已经给出的编号，避免再次发放同一个值。

</details>

<details>
<summary>修复临时表导致的重启失败 · 2026-11-19</summary>

数据库异常退出后，重启时会清理确认是上次连接留下的临时表。已提交的普通表数据仍可读取。

</details>

[完整更新日志](CHANGELOG.md)
