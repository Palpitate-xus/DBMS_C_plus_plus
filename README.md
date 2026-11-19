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
<summary>按编号修改或删除记录时少读数据 · 2026-11-19</summary>

例如按订单号删除一条订单。如果订单号是整数主键，数据库可以用索引找到这条记录，省去逐页查找。

</details>

<details>
<summary>可以查看分区表的统计更新进度 · 2026-11-19</summary>

更新分区表的统计信息时，可以看到已经处理了多少个分区，以及当前分区读到了哪里。

</details>

<details>
<summary>整理表时少花时间找空位 · 2026-11-19</summary>

用 `VACUUM FULL` 整理表时，数据库会记住哪里还有空位，搬动记录时少找几次。

</details>

[完整更新日志](CHANGELOG.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)
