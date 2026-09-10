# 打包与部署（v0.1.0）

`scripts/package.sh` 产出源码 tarball（`dist/dbms-<version>.tar.gz`），打包前三重一致性校验：

1. `CMakeLists.txt` 的 `project(... VERSION x.y.z)` == `src/common/version.h` 的 `DBMS_VERSION_STRING`
2. `CHANGELOG.md` 存在 `## [x.y.z]` 小节
3. 工作树干净（tarball 由 `git archive HEAD` 生成，杜绝未跟踪文件泄漏）

用户侧构建：

```bash
tar xzf dbms-<version>.tar.gz
cd dbms-<version>
./scripts/build.sh        # 产物: 仓库根 ./dbms_main
./scripts/build_tests.sh  # 可选: 全量回归
```

依赖：g++ (C++17)、make/cmake（按构建脚本）、Python 3（E2E 测试）、zlib（`-DHAS_ZLIB=1`，缺失时 WAL 压缩降级）。

## 目录约定（显式 data directory）

每次正常启动都必须用 `-D/--data-dir` 或 `DBMS_DATA_DIR` 显式选择数据根；
启动 CWD 不再决定状态位置。首次打开空目录时会原子写入 V2
`DBMS_CONTROL`（项目 magic、control/catalog/heap format version、8 KiB block size、
native byte order、随机 system identifier）。正常启动只接受完整兼容的 V2；
`--check-data-directory` 执行只读检查，`--upgrade-data-directory` 当前仅执行
V1→V2 control 升级，不会转换 catalog/关系/索引数据。损坏或未知 control、
无 DBMS 标识的任意非空目录以及含 `PG_VERSION` 的 PostgreSQL cluster
都会在引擎全局对象构造前被拒绝。

部署或恢复后可在服务停止时执行严格只读的 heap 校验：

```bash
/path/to/dbms_main -D /srv/dbms-instance --verify-data-checksums
```

该命令验证主/分区/TOAST/unlogged init heap、外部 tablespace 和已有 TDE 边车；
报错定位到文件和 block。当前它不覆盖索引与全部 catalog/metadata，也没有锁机制
替代运维侧的停服保证，因此不得与 server 并发执行。

| 路径 | 内容 |
|---|---|
| `DBMS_CONTROL` | DBMS-C++ cluster identity 与持久化格式边界；不得复制到另一套独立 cluster 或手工编辑 |
| `info/` | 集群级元数据根 |
| `info/pg_catalog/pg_authid.cat` | 角色目录。CSV 行：`oid,"name",super,createdb,createrole,inherit,login,replication,bypassrls,-1,"SCRAM-SHA-256$iter:salt$stored:server",""`（注意 stored/server 之间是 `:`） |
| `info/tlist.lst` | 数据库清单（每行一个数据库名） |
| `pg_hba.conf` | 可选但服务端建议：`host all <user> 127.0.0.1/32 scram-sha-256`。**无匹配行时连接在认证前悬挂** |
| `dbms.conf` | 可选 `key=value`：`tde_keyring`、`pool_mode`、`pool_size`、`checkpoint_interval`（1..1000000，0 非法）、`audit_level` 等。服务端启动时读取一次；CLI 每进程读取 |
| `<dbname>/` | 每数据库一目录：`*.dt` 堆文件、`*.idx` 索引、`<file>.tde` TDE 边车信封（48B/页 nonce+MAC）、`.publication` 逻辑复制目录、`.runtime_stats` |
| `*.wal` / WAL 段 | 每数据库目录下 16MiB 段文件 |
| `dbms.log` / `slow_query.log` / `audit.log` / `auto_explain.log` | data directory 下的运行日志 |

## 服务端部署形态

```bash
mkdir /srv/dbms-instance
# 写 info/pg_catalog/pg_authid.cat、info/tlist.lst、pg_hba.conf（见上表）
/path/to/dbms_main -D /srv/dbms-instance --server 5432            # TLS
/path/to/dbms_main -D /srv/dbms-instance --server 5432 --insecure # 本地明文
```

**约束（务必遵守）**：

- **一个数据目录只能跑一个服务端进程**。多进程共享目录会出现 XID 冲突，恢复时报 contradictory COMMIT。多用户并发 = 单 `--server` 进程 + N 个客户端连接（thread-per-connection）。
- CLI 交互模式（stdin 输 SQL）适合单人运维，与运行中的服务端**不要**同时操作同一数据库目录。
- TDE：`tde_keyring=` 指向密钥文件（0600）；keyring 丢失 = 数据不可恢复（设计如此）。

## v0.1.0 高并发已知问题

见 `RELEASE-NOTES.md` §5a：同页插入竞态、周期性 world-stop、连接风暴退避。负载类场景先读该节。
