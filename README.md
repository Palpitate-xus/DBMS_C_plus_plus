# DBMS C++

用 C++ 编写的关系型数据库，支持 SQL 查询、索引和事务。可在终端使用，也可用 `psql` 连接。

## 快速开始

在 Linux 上[安装编译依赖](docs/PACKAGING.md#编译依赖)，再下载并编译。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --parallel 2
./build/cmake/dbms_main --version
```

按[部署说明](docs/PACKAGING.md#服务端部署形态)准备数据目录和登录账号，把命令中的路径换成你的数据目录。

```bash
./build/cmake/dbms_main -D /path/to/data
```

启动后，在同一行输入用户名和密码，以空格分隔。登录后输入 `SELECT 1;`，看到 `1` 就可以继续运行 SQL。

[用 psql 连接](docs/MANUAL.md#18-网络服务) · [使用手册](docs/MANUAL.md) · [支持哪些功能](docs/compatibility-contract.md)

## 最近更新

<details>
<summary>更新记录时，会先在原来的页找空位 · 2026-11-19</summary>

原来的页放不下新版本时，数据库会先查哪些页还有空位，减少从头查找。

</details>

<details>
<summary>带来源表的更新和删除，也能按主键找记录 · 2026-11-19</summary>

如果条件先用整数主键指定一条记录，数据库核对索引命中后，可以直接找到目标，再检查来源表。

</details>

<details>
<summary>按另一张表更新或删除时，也能处理并发修改 · 2026-11-19</summary>

默认设置下，如果目标记录被另一个连接先改了，数据库会用新值和原来选中的来源记录重新检查条件。

</details>

[完整更新日志](CHANGELOG.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)
