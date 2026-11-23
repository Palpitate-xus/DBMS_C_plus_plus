# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL、索引和事务，可以用 `psql` 连接。

## 快速开始

[安装编译依赖（Linux）](docs/PACKAGING.md#编译依赖)，然后在仓库目录运行下面的命令。

```bash
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#1-快速开始) · [用 `psql` 连接](docs/MANUAL.md#18-网络服务) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>可以用索引查找一段带时区的时间 · 2026-11-23</summary>

例如，可以查找上午 9 点到 9 点半之间的签到记录。范围较小时，数据库可以用索引直接找到这一段。

</details>

<details>
<summary>按带时区的时间查找记录可以用索引 · 2026-11-23</summary>

例如，给签到时间建索引后，查找 `14:30:00+05:30` 时可以直接定位记录。索引确认没有匹配时，也可以直接返回。

</details>

<details>
<summary>可以保存带时区的时间 · 2026-11-23</summary>

例如，把签到时间存为 `14:30:00+05:30`，查询时仍会显示 `+05:30`。

</details>

[完整更新日志](CHANGELOG.md)
