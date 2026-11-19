# DBMS C++

一个用 C++ 编写的关系型数据库，支持 SQL、索引和事务。

## 快速开始

在 Linux 上先[安装编译依赖](docs/PACKAGING.md#编译依赖)，再下载并编译。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

按[部署说明](docs/PACKAGING.md#服务端部署形态)准备好数据目录和账号后，运行 `./build/cmake/dbms_main -D /path/to/data`，把路径换成你的数据目录。在同一行输入以空格分隔的用户名和密码。登录后可以试试 `SELECT 1;`。

[使用手册](docs/MANUAL.md) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [支持的功能和限制](docs/compatibility-contract.md)

## 最近更新

<details>
<summary>更新记录时，先在原来的位置附近找空位 · 2026-11-19</summary>

如果原来的位置附近放不下更新后的记录，数据库会优先尝试之前记下的空位，减少从头查找的次数。

</details>

<details>
<summary>用另一张表更新或删除记录时，可以先按编号查找 · 2026-11-19</summary>

例如，根据付款表更新或删除某笔订单。如果订单号是整数主键，数据库可以先通过索引找到订单，再检查付款表中的条件。

</details>

<details>
<summary>同时修改同一条记录时，会按新值再检查 · 2026-11-19</summary>

默认设置下，按另一张表更新或删除记录时，如果有人先修改并提交了这条记录，数据库会用新值再次检查条件。条件不再成立时，就跳过这条记录。

</details>

[完整更新日志](CHANGELOG.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)
