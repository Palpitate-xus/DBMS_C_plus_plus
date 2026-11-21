# DBMS C++

用 C++ 编写的关系型数据库。可以用 SQL 读写数据，支持索引、事务和备份恢复。

## 快速开始

在 Linux 上安装好依赖后，下载源码并编译。最后一条命令会显示版本号。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

启动数据库前，请准备好数据目录和登录账号。[运行与登录](docs/MANUAL.md#1-快速开始)

## 最近更新

<details>
<summary>两列的排序规则冲突时会报错 · 2026-11-21</summary>

两列用不同的规则比较文字时，数据库会提示冲突。可以用 `COLLATE` 指定共同的规则。空表没有文字需要比较，查询会正常返回空结果。

</details>

<details>
<summary>筛选文字时可指定排序规则 · 2026-11-21</summary>

例如 `WHERE title COLLATE "C" < 'm'` 会用 `C` 规则找出排在 `m` 前面的标题。规则名写错时，空表也会提示错误。

</details>

<details>
<summary>修复比较同一行两列时的漏查 · 2026-11-21</summary>

例如 `WHERE id = peer_id` 会查找两列值相等的记录。以前给 `id` 建索引后，这种查询有时会漏掉符合条件的记录。

</details>

[完整更新日志](CHANGELOG.md) · [功能与限制](docs/compatibility-contract.md) · [文档索引](docs/README.md)
