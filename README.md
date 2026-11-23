# DBMS C++

一个用 C++ 写的关系型数据库。可以用 SQL 建表、查询和修改数据，支持索引和事务，也可以用 `psql` 连接。

## 快速开始

安装[编译依赖](docs/PACKAGING.md#编译依赖)后，在项目目录运行以下命令，最后一条会显示版本号。

```sh
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [全部文档](docs/README.md)

## 最近更新

<details>
<summary>从长数组里取几行更快 · 2026-11-23</summary>

比如从一组 2,048 个编号里取第一条。数据库少读几遍同样的 SQL，能更早返回结果。

</details>

<details>
<summary>SQL 里直接写几千个数字，查询更快 · 2026-11-23</summary>

把一长串数字直接写在查询里时，数据库能更快读完这条 SQL。

</details>

<details>
<summary>把一组整数拆成多行更快 · 2026-11-23</summary>

比如查询一组订单号，每行显示一个。读完整组数据所需的时间更短。

</details>

[完整更新日志](CHANGELOG.md)
