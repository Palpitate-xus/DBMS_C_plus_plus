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
<summary>修复写过临时表后无法重启的问题 · 2026-11-19</summary>

恢复时会核对旧临时表的归属。确认后会清理上次连接留下的临时表，已提交的普通表数据可以继续读取。

</details>

<details>
<summary>建表时可以为日后的更新留出空间 · 2026-11-19</summary>

例如，把填充比例设为 90，就会为日后的更新留出约一成页内空间。重启后仍会按这个设置写入。

</details>

<details>
<summary>插入记录时，可以改用别处的空位 · 2026-11-19</summary>

推荐的位置被另一个操作占用时，数据库会先试别处。有合适的空位，就可以继续插入。

</details>

[完整更新日志](CHANGELOG.md) · [全部文档](docs/README.md) · [报告问题](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues)
