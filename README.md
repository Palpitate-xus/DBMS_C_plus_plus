# DBMS C++

使用 C++17 实现的关系型数据库管理系统，提供 SQL 交互、页式存储、索引、事务和查询执行模块，以及 PostgreSQL wire protocol 3.0 的部分实现。

## 项目定位与兼容性

本项目使用自己的存储格式，不是 PostgreSQL 的直接替代品，也不能直接打开 PostgreSQL 数据目录。客户端能够连接，不代表 SQL、系统目录、事务或存储行为与 PostgreSQL 等价。

SQL 与客户端协议的接口边界见[兼容性契约](docs/compatibility-contract.md)，具体用法见[使用手册](docs/MANUAL.md)。

## 功能概览

- SQL 解析、AST、类型绑定，以及数据定义、查询和数据修改的执行入口。
- 页式堆存储、缓冲池、变长数据和 TOAST。
- B+ 树、Hash 等索引访问方法。
- Volcano 查询执行器，包含扫描、过滤、投影、连接、排序、聚合和窗口算子。
- 事务、MVCC、保存点、锁管理、WAL 和恢复模块。
- 交互式命令行，以及 PostgreSQL Simple / Extended Query 协议入口。

以上是模块概览，不是完整 PostgreSQL 兼容性声明。

## 构建

### 环境与依赖

- Linux，支持 C++17 的编译器；脚本构建使用 `g++`。
- Bash、`pkg-config`。
- zlib 与 ICU 开发库。
- OpenSSL 开发库用于 TLS 网络服务；未检测到 OpenSSL 时构建 TLS stub。
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

生成的程序为 `build/cmake/dbms_main`。两种构建方式使用同一份[生产源码清单](cmake/dbms_sources.txt)。

## 运行

每次启动都必须用 `-D` / `--data-dir` 或 `DBMS_DATA_DIR` 显式指定本项目的数据目录。不要使用 PostgreSQL 的数据目录。登录角色和访问配置须提前准备；数据目录、角色配置与部署约定见[打包与部署说明](docs/PACKAGING.md)。

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

服务端 TLS 需要真实 OpenSSL、证书和私钥。仅在本地开发时，可显式使用 `--insecure` 启动明文服务。

使用已有角色连接：

```bash
psql "host=localhost port=9999 dbname=info user=admin sslmode=require"
```

仓库也提供 [Dockerfile](Dockerfile) 和 [Compose 配置](docker-compose.yml)。使用 Compose 前需准备角色、访问配置与 `deploy/tls` 下的证书和私钥；数据库文件通过数据卷保存。

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

`USE DATABASE` 等项目扩展命令需要显式启用 extended compatibility mode，不属于 PostgreSQL 标准接口。

## 测试

完整本地回归入口：

```bash
bash scripts/build_tests.sh
```

CMake 的 `check` 目标调用同一测试编排器：

```bash
cmake --build build/cmake --target check
```

运行单个 C++ 测试：

```bash
bash scripts/build_one_test.sh window_functions_test
```

专项测试的依赖与运行方式以对应脚本和测试说明为准。报告测试问题时，请附上运行命令、环境与相关输出，便于复现。

## 项目结构

```text
src/
├── main.cpp       # 命令行与服务启动入口
├── parser/        # SQL 解析、AST 与绑定
├── catalog/       # 类型与系统目录
├── commands/      # DDL、DML 与存储引擎接口
├── executor/      # 查询计划与执行算子
├── expression/    # 表达式与预绑定查询执行
├── storage/       # 页面、缓冲池与存储组件
├── access/        # 索引与访问方法
├── transaction/   # 事务与日志组件
└── network/       # PostgreSQL 协议与 TLS
tests/             # 原生测试与协议测试
scripts/           # 构建、测试与打包入口
cmake/             # 共享源码清单
docs/              # 使用、兼容性与开发文档
```

## 文档

- [使用手册](docs/MANUAL.md)：SQL 与操作说明。
- [兼容性契约](docs/compatibility-contract.md)：SQL、协议及扩展模式的边界。
- [兼容性清单](docs/postgresql-18-gap-audit.md)与[验证索引](docs/gap-progress.json)：开发者参考，可用[一致性检查脚本](scripts/check_gap_progress.py)校验。
- [打包与部署](docs/PACKAGING.md)：源码包、数据目录和配置约定。
- [CHANGELOG](CHANGELOG.md)：版本变更记录。

## 参与贡献

欢迎通过 Issue 报告问题，或通过 Pull Request 提交改进。

提交问题时请提供：

- 使用的提交或版本、编译器和操作系统。
- 可在独立测试数据目录执行的最小 SQL 复现及启动参数。
- 预期与实际结果，以及相关错误信息。

不要附带真实数据库数据、私钥或凭据。

提交改动时：

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
