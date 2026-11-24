# DBMS C++

用 C++ 编写的关系型数据库。支持 SQL 读写、索引和事务，可以用 `psql` 连接。

## 快速开始

在 Linux 上装好编译依赖后，在仓库目录运行以下命令。[编译依赖](docs/PACKAGING.md#编译依赖)

```sh
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[编译和启动说明](docs/MANUAL.md#1-快速开始) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [更多文档](docs/README.md)

## 最近更新

<details>
<summary>可以用 psql 列出表 · 2026-11-24</summary>

用 `\dt` 可以看到表的所属模式、名称、类型和所有者。也可以按表名筛选，或只看指定模式里的表。

</details>

<details>
<summary>查询可以分清不同模式里的同名表 · 2026-11-24</summary>

如果两个模式里都有 `orders` 表，现在可以查出直接写 `orders` 会找到哪一张。原版 `psql` 已能完成这一步，显示完整表结构还待补齐。

</details>

<details>
<summary>修复 psql 密码登录 · 2026-11-24</summary>

使用 SCRAM 密码认证时，原版 `psql` 18.6 现在可以通过本地网络连接登录并执行查询。

</details>

[完整更新日志](CHANGELOG.md)
