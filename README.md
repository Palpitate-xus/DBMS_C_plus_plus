# DBMS C++

用 C++ 编写的 SQL 数据库。支持建表、查询、索引和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上用 CMake 构建。[构建依赖](docs/PACKAGING.md)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

准备好数据目录和登录账号后，用 `./build/cmake/dbms_main -D <数据目录>` 启动。[数据目录与账号设置](docs/PACKAGING.md#目录约定显式-data-directory) · [首次登录](docs/MANUAL.md#首次登录) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>写入数据时少查几遍目录 · 2026-11-18</summary>

新增、修改和删除数据时，数据库把多类索引文件一起检查，减少重复遍历目录。

</details>

<details>
<summary>按主键翻页时少排一次序 · 2026-11-18</summary>

索引已经按所需顺序给出一页时，数据库沿用这个顺序。

</details>

<details>
<summary>翻页时可以跳过空值 · 2026-11-18</summary>

例如只看有备注的记录，数据库可以按主键顺序查找，凑够一页就停。

</details>

[查看完整更新日志](CHANGELOG.md)

## 文档与贡献

[使用手册](docs/MANUAL.md) · [性能报告](docs/performance-report-2026-10-30.md) · [全部文档](docs/README.md)

[报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) · [提交改进](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls) · [第三方许可说明](THIRD_PARTY_NOTICES)
