# DBMS C++

一个用 C++ 编写的关系型数据库，提供 SQL 查询、数据增删改、索引和事务，支持命令行操作，也可以通过 PostgreSQL 客户端协议连接。

项目仍在开发中，部分 SQL、并发访问和恢复行为存在已知问题。具体范围见[兼容性说明](docs/compatibility-contract.md)和[与 PostgreSQL 的对比](docs/postgresql-comparison.md)。数据库使用自己的存储格式，不能直接读取 PostgreSQL 数据目录。

[构建](#构建) · [运行](#运行) · [SQL 示例](#sql-示例) · [测试](#测试) · [最近更新](#最近更新) · [文档](#文档) · [参与贡献](#参与贡献)

## 主要功能

- **查询与数据管理**：建表、查询、插入、更新和删除，包含连接、排序、聚合与窗口函数。
- **数据存储与索引**：按页存储数据，支持长文本、B+ 树和哈希索引。
- **事务与恢复**：包含提交、回滚、保存点、多版本并发控制、锁和写入日志恢复。
- **客户端连接**：提供交互式命令行，以及 PostgreSQL 的简单查询和预备语句协议接口。

功能的支持范围和使用方法见[使用手册](docs/MANUAL.md)，已知问题和测试范围见[测试报告](docs/test-report.md)。

## 构建

### 环境与依赖

- Linux，支持 C++17 的编译器；脚本构建使用 `g++`。
- Bash、`pkg-config`。
- zlib 与 ICU 开发库。
- OpenSSL 开发库用于加密网络连接；没有该库时仅支持明文连接。
- Python 3 用于测试；CMake 构建需要 CMake 3.16 或更高版本。

Ubuntu / Debian 安装依赖示例：

```bash
sudo apt-get update
sudo apt-get install build-essential cmake pkg-config python3 \
    zlib1g-dev libicu-dev libssl-dev
```

### 脚本构建

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
bash scripts/build.sh
```

生成的程序位于仓库根目录：`./dbms_main`。

### CMake 构建

```bash
cmake -S . -B build/cmake
cmake --build build/cmake --parallel
```

生成的程序为 `build/cmake/dbms_main`。两种构建方式使用同一份[源码清单](cmake/dbms_sources.txt)。

## 运行

启动时用 `-D` / `--data-dir` 或环境变量 `DBMS_DATA_DIR` 指定数据目录。登录角色和访问配置须提前准备，步骤见[打包与部署说明](docs/PACKAGING.md)。

交互式命令行：

```bash
./dbms_main -D /srv/dbms/main
```

网络服务：

```bash
export DBMS_TLS_CERT=/etc/dbms/tls/server.crt
export DBMS_TLS_KEY=/etc/dbms/tls/server.key
./dbms_main -D /srv/dbms/main --server 9999
```

加密连接需要安装 OpenSSL，并准备证书和私钥。本地开发时，可以使用 `--insecure` 启动明文服务。

使用已有角色连接：

```bash
psql "host=localhost port=9999 dbname=info user=admin sslmode=require"
```

仓库也提供 [Dockerfile](Dockerfile) 和 [Compose 配置](docker-compose.yml)。使用 Compose 前需准备角色、访问配置与 `deploy/tls` 下的证书和私钥；数据库文件通过数据卷保存。

### 数据目录使用约定

- 一个数据目录只运行一个服务端进程；多个客户端通过该服务端连接。
- 不要让交互式命令行与服务端同时操作同一个数据目录。
- 数据目录包含数据库状态，请与源码和构建产物分开保存。更换程序或迁移环境前保留备份，并先在副本上验证兼容性。

## SQL 示例

在已连接的数据库中执行：

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    score INTEGER
);

INSERT INTO users VALUES (1, 'Alice', 85), (2, 'Bob', 72);

SELECT id, name, score
FROM users
WHERE score >= 80
ORDER BY id;

BEGIN;
UPDATE users SET score = 90 WHERE id = 1;
SAVEPOINT before_second_update;
UPDATE users SET score = 75 WHERE id = 2;
ROLLBACK TO SAVEPOINT before_second_update;
COMMIT;
```

`USE DATABASE` 等项目扩展命令需要先启用扩展兼容模式，配置方法见[使用手册](docs/MANUAL.md)。

## 测试

运行完整测试：

```bash
bash scripts/build_tests.sh
```

也可以通过 CMake 运行同一组完整测试：

```bash
cmake --build build/cmake --target check
```

运行单个 C++ 测试：

```bash
bash scripts/build_one_test.sh window_functions_test
```

以上命令均在仓库根目录执行。完整测试会编译数据库程序，并运行 C++ 测试和已接入测试脚本的客户端协议测试。

与 PostgreSQL 对比的测试需要另外准备可连接的 PostgreSQL 实例；连接参数、版本和所需工具以对应测试脚本为准。执行前检查测试配置，不要使用已有的业务数据库进行测试。

测试、示例和实验应使用独立的数据目录，不要指向正在使用的数据库。报告测试问题时，请附上运行命令、环境与相关输出，便于复现。

## 项目结构

```text
src/
├── main.cpp       # 命令行与服务启动入口
├── parser/        # 解析 SQL，检查名称和类型
├── catalog/       # 类型与系统目录
├── commands/      # 建表、查询和数据修改命令
├── executor/      # 查询计划与执行算子
├── expression/    # 表达式求值与查询准备
├── storage/       # 页面、缓冲池与存储组件
├── access/        # 索引与访问方法
├── transaction/   # 事务与日志组件
└── network/       # PostgreSQL 协议与 TLS
tests/             # C++ 测试与客户端协议测试
scripts/           # 构建、测试与打包入口
cmake/             # 共享源码清单
docs/              # 使用、兼容性与开发文档
```

## 最近更新

这里只列出最近 3 条记录，点击卡片标题展开。[查看完整更新日志](CHANGELOG.md)。

<details>
<summary><strong>2026-11-08 · 检查并发访问中的锁问题</strong></summary>

检查了锁升级、内存不足和事务锁交接时的行为，记录了尚未修复的问题。

[查看测试结果和已知问题](docs/test-report.md#native-lock-fault-review-20261108)

</details>

<details>
<summary><strong>2026-11-08 · 检查查询和数据读取问题</strong></summary>

检查函数参数的执行和查询期间修改表结构的行为，并与 PostgreSQL 比较结果；另测试了本项目对损坏数据的处理。发现的问题仍待修复。

[查看测试结果和已知问题](docs/test-report.md#ordinary-scalar-effects-20261108)

</details>

<details>
<summary><strong>2026-11-08 · 修复读取记录时的缓存管理</strong></summary>

读取记录时，让缓存中的数据页保持可用，直到复制完成；读取失败时也会释放相关资源。已补充多页、空值和内存不足的测试，并发访问仍有待验证的情况，性能变化尚无明确结论。

[查看测试结果](docs/test-report.md#row-read-page-pin-20261108) · [查看性能测量](docs/performance-report-2026-10-30.md#row-pin-query-cost-20261108)

</details>

## 文档

- [文档索引](docs/README.md)：使用、开发和历史记录的入口。
- [使用手册](docs/MANUAL.md)：SQL 与操作说明。
- [兼容性说明](docs/compatibility-contract.md)：SQL、客户端协议和扩展命令的支持范围。
- [打包与部署](docs/PACKAGING.md)：源码包、数据目录和配置约定。
- [测试报告](docs/test-report.md)：已完成的测试与已知问题。
- [性能测试报告](docs/performance-report-2026-10-30.md)：查询和其他数据库操作的测量结果。
- [与 PostgreSQL 的对比](docs/postgresql-comparison.md)：功能和性能差距。
- [完整更新日志](CHANGELOG.md)：全部变更记录。

## 参与贡献

欢迎通过 [Issue](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) 报告问题、讨论使用与设计，或通过 [Pull Request](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls) 提交改进。

提交问题时请提供：

- 使用的提交或版本、编译器和操作系统。
- 可在独立测试数据目录执行的最小 SQL 复现及启动参数。
- 预期与实际结果，以及相关错误信息。

不要附带真实数据库数据、私钥或凭据。

提交改动时：

- 从自己的分支提交修改，并在 Pull Request 中说明问题、改动与验证结果。
- 补充覆盖问题的回归测试，并说明实际运行的测试范围。
- 保持每个提交独立、易复查，避免混入无关修改。
- 新增生产源码时同步 `cmake/dbms_sources.txt`。
- 新增测试时接入对应测试入口。
- 更新受影响的使用文档，并在 CHANGELOG 中记录面向用户的变更。

## 许可证

仓库未提供独立的 `LICENSE` 文件。授权方式请向项目维护者确认。

## 致谢

- [hyrise/sql-parser](https://github.com/hyrise/sql-parser)
- [zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree)
- [Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS)
- [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree)
