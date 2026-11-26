# DBMS C++

这是一个用 C++ 编写的数据库，可以用 SQL 建表、查询和修改数据。

[安装与运行](docs/MANUAL.md#1-快速开始) · [文档目录](docs/README.md)

## 最近更新

<details>
<summary>改表名时更快发现重名 · 2026-11-26</summary>

在 ext4 测试盘上，库里多 32 张表时，改名遇到重名从约 2 秒缩短到约 40 毫秒。原表不存在时，可以用 `IF EXISTS` 跳过改名。

</details>

<details>
<summary>磁盘上的表改名更快 · 2026-11-26</summary>

在 ext4 测试盘上，额外有 128 张表时，改一张表名的耗时从约 3.6 秒降到约 1.8 秒。

</details>

<details>
<summary>库里表多时，改表名更快 · 2026-11-26</summary>

在内存文件系统上，测试库多了 512 张表后，改一张表名的耗时从约 360–530 毫秒降到约 290–330 毫秒。

</details>

[完整更新日志](CHANGELOG.md)
