# DBMS C++

这是一个用 C++ 编写的数据库，可以用 SQL 建表、查询和修改数据。

[安装与运行](docs/MANUAL.md#1-快速开始) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>磁盘上的表改名更快 · 2026-11-26</summary>

在 ext4 测试盘上，额外有 128 张表时，改一张表名的耗时从约 3.6 秒降到约 1.8 秒。

</details>

<details>
<summary>库里表多时，改表名更快 · 2026-11-26</summary>

在内存文件系统上，测试库多了 512 张表后，改一张表名的耗时从约 360–530 毫秒降到约 290–330 毫秒。

</details>

<details>
<summary>库里表多时，开始事务更快 · 2026-11-26</summary>

在额外建了 512 张表的测试库里，开始事务从约 3 毫秒降到不到 1 毫秒。

</details>

[完整更新日志](CHANGELOG.md)
