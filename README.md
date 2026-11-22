# DBMS C++

用 C++ 编写的关系型数据库。可以用 SQL 建表、写入和查询数据，支持索引和事务。

## 快速开始

在 Linux 上先[安装编译依赖](docs/PACKAGING.md#编译依赖)，再运行下面的命令编译并查看版本。

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
cmake -S . -B build/cmake
cmake --build build/cmake --target dbms_main --parallel 2
./build/cmake/dbms_main --version
```

[启动和登录](docs/MANUAL.md#运行) · [用 psql 连接](docs/MANUAL.md#18-网络服务) · [功能和限制](docs/compatibility-contract.md)

## 最近更新

<details>
<summary>更新等不到写锁时，会保留原来的共享锁 · 2026-11-22</summary>

如果你已经锁住一条记录来核对，后来尝试修改时等到超时，回滚到保存点仍会保留原来的锁。 [使用说明](docs/MANUAL.md#查询时锁住分区表记录)

</details>

<details>
<summary>核对分区表记录时，可以让修改先等一等 · 2026-11-22</summary>

用共享锁查看分区表时，多个连接可以同时核对同一条记录。要改动这条记录的连接会等先前的事务结束。 [使用示例](docs/MANUAL.md#查询时锁住分区表记录)

</details>

<details>
<summary>改了分区键，记录会移到对应分区 · 2026-11-22</summary>

部分分区表现在可以修改记录。比如把一笔订单的编号从 5 改为 15，数据库会按新编号把它放到对应的分区。提交前可以撤销。 [适用范围和示例](docs/MANUAL.md#修改分区表中的记录)

</details>

[完整更新日志](CHANGELOG.md) · [更多文档](docs/README.md)
