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
<summary>SQL 里直接写几千个数字，查询更快 · 2026-11-23</summary>

把一长串数字直接写在查询里时，数据库能更快读完这条 SQL。

</details>

<details>
<summary>把一组整数拆成多行更快 · 2026-11-23</summary>

比如查询一组订单号，每行显示一个。读完整组数据所需的时间更短。

</details>

<details>
<summary>可以把一个或多个数组展开成行 · 2026-11-23</summary>

例如，把 3 个编号和 2 个名字并排展开。结果有 3 行，最后一行的名字为空。还可以给每行编号，再筛选或排序。

</details>

[完整更新日志](CHANGELOG.md)
