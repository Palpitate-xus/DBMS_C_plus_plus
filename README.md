# DBMS C++

DBMS C++ 是一个用 C++ 编写的数据库，支持用 SQL 管理数据。

[快速开始](docs/MANUAL.md#1-快速开始) · [更多文档](docs/README.md)

## 最近更新

<details>
<summary>修改注释所需的临时备份更小 · 2026-11-26</summary>

在另有 128 张表的测试库里，只修改一条注释时，临时备份从约 131 KB 降到不到 1 KB。

</details>

<details>
<summary>给表和列保存注释更快 · 2026-11-26</summary>

给表或列加注释时，省去了备份整个数据库的步骤。在另有 128 张表的测试库里，给一张表加注释从约 100 毫秒降到约 16–17 毫秒。

</details>

<details>
<summary>可以查到给表和列写的注释 · 2026-11-26</summary>

用 SQL 就能查看之前写下的说明，例如订单表的用途或金额列用的货币。注释里的引号和换行也会原样保留。

</details>

[完整更新日志](CHANGELOG.md)
