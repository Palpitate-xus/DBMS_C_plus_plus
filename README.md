# DBMS C++

用 C++ 编写的关系型数据库。支持 SQL 查询和写入，也提供索引、事务、备份与恢复功能。

[使用手册](docs/MANUAL.md) · [功能与限制](docs/compatibility-contract.md) · [文档索引](docs/README.md)

## 在 Linux 上试用

安装[编译依赖](docs/PACKAGING.md#编译依赖)，然后下载和编译。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
```

按[首次登录说明](docs/MANUAL.md#首次登录)准备数据目录和账号，再用下面的命令启动。把 `./dbms-data` 换成你准备的目录。

```bash
./build/cmake/dbms_main -D ./dbms-data
```

## 最近更新

<details>
<summary>查询条件能区分数字和带引号的列名 · 2026-11-21</summary>

即使表里有一列叫 `"7"`，`WHERE id = 7` 也会按数字查找。`WHERE id = "7"` 则比较同一行的两列。查看执行计划也能看到这两种条件的实际区别。

</details>

<details>
<summary>按数字筛选删除时正确区分列名 · 2026-11-21</summary>

即使表里有名为 `"7"` 的列，`WHERE id = 7` 也会按数字 7 筛选。按没有索引的整数列删除时，数据库也会少做重复读取。

</details>

<details>
<summary>按编号更新时少读无关记录 · 2026-11-21</summary>

例如按订单编号更新记录。编号列没有索引时，数据库会先在扫描中检查编号，再对可能匹配的记录做完整判断。普通查询也会直接沿用已经确定的列类型。

</details>

[完整更新日志](CHANGELOG.md)
