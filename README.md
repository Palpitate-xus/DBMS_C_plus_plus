# DBMS C++

一个用 C++ 编写的关系型数据库。支持 SQL 查询、索引和事务，可用 `psql` 连接。

## 快速开始

在 Linux 上先安装编译依赖，再下载和编译。[依赖安装说明](docs/PACKAGING.md#编译依赖)

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动数据库](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [支持的功能](docs/compatibility-contract.md)

## 最近更新

<details>
<summary>回滚事务时，会恢复之前的超时设置 · 2026-11-22</summary>

可以只为当前事务缩短锁等待时间，结束后恢复原来的设置。用 `SHOW` 可以随时查看当前值。[使用示例](docs/MANUAL.md#在事务里临时调整超时)

</details>

<details>
<summary>等不到更严格的表锁，原来的锁会保留 · 2026-11-22</summary>

如果你已锁住一张表，再请求更严格的锁时遇到超时或取消，原来的锁仍然有效。也可以让数据库一遇到冲突就报错。[使用示例](docs/MANUAL.md#在事务里锁住一张表)

</details>

<details>
<summary>改记录时等不到锁，原来的锁会保留 · 2026-11-22</summary>

如果你先锁住一条记录来核对，之后尝试修改时等到超时，回到保存点后，其他人仍要等你的事务结束才能修改这条记录。[使用说明](docs/MANUAL.md#查询时锁住分区表记录)

</details>

[完整更新日志](CHANGELOG.md) · [更多文档](docs/README.md)
