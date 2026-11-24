# DBMS C++

用 C++ 写的关系型数据库。可以用 SQL 建表、查数据和改数据，也可以用 `psql` 连接。

## 快速开始

在 Linux 上装好依赖后，在项目目录编译。[编译依赖](docs/PACKAGING.md#编译依赖)

```sh
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务)

## 最近更新

<details>
<summary>可以查看外键的 SQL 定义 · 2026-11-24</summary>

可以查到外键引用的表和列，以及更新或删除被引用数据时设置了什么动作。表或列改名后会显示新名字。

</details>

<details>
<summary>可以查到外键连接了哪些列 · 2026-11-24</summary>

查询表的外键时，可以看到它引用的表和列，以及旧数据是否已经通过检查。

</details>

<details>
<summary>查询表的列信息更快 · 2026-11-24</summary>

在本机测试中，查询一张有五列的表，显示列名、类型、默认值和排序规则，耗时中位数从约 307 毫秒降到约 15 毫秒。

</details>

[完整更新日志](CHANGELOG.md) · [文档目录](docs/README.md)
