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
<summary>可以查到列用什么排序规则 · 2026-11-24</summary>

创建表时为文本列指定了排序规则，现在可以查到它。例如，可以看出订单备注是否按字节顺序排序，也就是 `C` 规则。

</details>

<details>
<summary>可以查到常见列默认值 · 2026-11-24</summary>

可以用 SQL 查看一列的默认整数、文本或真假值。例如，可以查出订单备注默认是不是空字符串。

</details>

<details>
<summary>可以查到列的类型和长度 · 2026-11-24</summary>

可以用 SQL 查看一列是整数、日期还是文本。例如，声明为 `varchar(12)` 的列会显示长度 12。

</details>

[完整更新日志](CHANGELOG.md)
