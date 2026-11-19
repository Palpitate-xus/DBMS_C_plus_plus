# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上编译并查看版本。[编译依赖](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

运行数据库前需要准备数据目录和登录账号。[服务配置](docs/PACKAGING.md#服务端部署形态) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>删除长文本后，可以单独腾出磁盘空间 · 2026-11-18</summary>

删除大量单独存放的长文本后，可以只缩小存放这些内容的文件，把空出的空间还给磁盘。

</details>

<details>
<summary>清理表时，可以选择清理哪些数据 · 2026-11-18</summary>

用 `VACUUM` 时，可以只清理表里的普通数据，也可以只清理单独存放的长文本。还可以单独更新查询用的统计信息。

</details>

<details>
<summary>清理表时可以少读一遍数据 · 2026-11-18</summary>

如果表没有单独存放长文本等大内容，`VACUUM` 会省去检查这些内容的一遍读取。

</details>

[查看完整更新日志](CHANGELOG.md)

[使用手册](docs/MANUAL.md) · [支持范围](docs/compatibility-contract.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)
