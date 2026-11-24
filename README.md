# DBMS C++

用 C++ 写的关系型数据库。可以用 SQL 建表、查询和修改数据，也可以用 `psql` 连接。

## 快速开始

在 Linux 上装好依赖后，在项目目录运行以下命令。[编译依赖](docs/PACKAGING.md#编译依赖)

```sh
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [使用文档](docs/README.md)

## 最近更新

<details>
<summary>可以用 psql 列出表 · 2026-11-24</summary>

在 `psql` 中输入 `\dt` 就能看到表名和所有者。可以按表名筛选，也可以只看某个模式里的表。

</details>

<details>
<summary>可以查出同名表会用到哪一张 · 2026-11-24</summary>

如果 `sales.orders` 和 `archive.orders` 同时存在，现在可以查出直接写 `orders` 会找到哪一张。

</details>

<details>
<summary>修复 psql 密码登录 · 2026-11-24</summary>

用原版 `psql` 18.6 在本机连接数据库时，可以用密码登录并执行查询。

</details>

[完整更新日志](CHANGELOG.md)
