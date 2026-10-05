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

依赖：g++ (C++17)、make/cmake（按构建脚本）、Python 3（E2E 测试）、zlib 开发库（TOAST 压缩存储格式必需）、ICU i18n/uc/data 开发库（IANA 时区和夏令时规则必需）。运行时还须安装匹配的 zlib 和 ICU 共享库。OpenSSL 开发库用于网络 TLS；生产部署必须启用真实 TLS。

## 目录约定（显式 data directory）

每次正常启动都必须用 `-D/--data-dir` 或 `DBMS_DATA_DIR` 显式选择数据根；
启动 CWD 不再决定状态位置。首次打开空目录时会原子写入 V3
`DBMS_CONTROL`（项目 magic、control/catalog/heap format version、8 KiB block size、
16 MiB WAL segment size、
native byte order、feature flags、随机 system identifier 和 CRC32C checksum）。
控制文件必须是有大小上限的普通文件，不接受符号链接。正常启动只接受完整兼容的
V3；`--check-data-directory` 执行只读检查，`--upgrade-data-directory` 显式执行
V1/V2→V3 control 升级并保留 system identifier，不会转换 catalog/关系/索引数据。
损坏或未知 control、
无 DBMS 标识的任意非空目录以及含 `PG_VERSION` 的 PostgreSQL cluster
都会在引擎全局对象构造前被拒绝。

V3 `DBMS_CONTROL` 是 ASCII 单行键值格式，行顺序固定，文件以 LF 结束；
当前 canonical 内容为：

```text
DBMS_CPP_CLUSTER_CONTROL_V3
control_format_version=3
catalog_format_version=1
heap_format_version=2
block_size=8192
wal_segment_size=16777216
byte_order=<native: little or big>
feature_flags=00000000
system_identifier=<16 lowercase hex digits>
control_checksum=<8 lowercase hex digits>
```

`byte_order` 必须匹配运行平台；`feature_flags` 当前只接受全零值，其他值表示
未识别的磁盘特性并会被拒绝。CRC32C（Castagnoli，反射多项式 `0x82F63B78`）
覆盖 magic 行至 `system_identifier` 行末尾的全部原始字节（包括 LF），不含
checksum 行；额外字段、尾随行、损坏值均 fail-closed。V3 替换使用同目录临时文件、
文件 `fsync`、原子 rename 和父目录 `fsync`。

部署或恢复后可在服务停止时执行严格只读的 heap、B+Tree 页、Hash、
Bloom、GIN、BRIN 与 GiST 索引校验：

```bash
/path/to/dbms_main -D /srv/dbms-instance --verify-data-checksums
```

该命令验证主/分区/TOAST/unlogged init heap，以及 `.idx`/`.idx_*` B+Tree
索引、`.hidx` Hash、`.bidx` Bloom、`.gin` GIN、`.brin` BRIN 和 `.gist` GiST
侧车，包括从 tablespace marker 解析的外置文件；诊断定位到索引文件和 heap block。
统计会分开列出页号绑定的 `0xC552`、旧内容校验 `0xC551` 与无 checksum 的
zero-marker legacy 页，并单列带 checksum 的 Hash V2/Bloom BLM2/GIN V2/BRIN V2/GiST V2
与未校验的 Hash V1/Bloom BLM1/GIN V1/BRIN V1/GiST V1 文件。GIN V2/BRIN V2/GiST V2
扫描验证格式头、entry-count 边界与 CRC32C，但不解析 GIN postings、BRIN ranges
或 GiST range entries 的完整语义。它不覆盖 SP-GiST 或全部 catalog/metadata，也没有锁机制
替代运维侧停服保证，因此不得与 server 并发执行。

新建及 REINDEX 生成的 B+Tree 页另有逐页 CRC32C，并在页面首次载入时校验；
离线 verifier 也只读检查其每页 checksum，但不会验证树的完整拓扑。旧格式索引
仍可读；`0xC551` 在重建前不能检测页交换，zero-marker 格式没有 checksum。

`CREATE TABLESPACE name LOCATION '/absolute/path'` 使用 data directory 内的严格
`pg_tblspc/<name>.path` marker，并在外部 root 下为当前数据库建立独立子目录。
marker 不得手工编辑或改成 symlink；location 不得与 data directory 重叠，也不能被
另一 tablespace 重复注册。`ALTER TABLE ... SET TABLESPACE`、备份/恢复和离线 checksum
共用该解析规则及跨 backend flock。当前 tablespace 仍是每数据库的项目格式，
不兼容 PostgreSQL 的 cluster-wide OID/symlink locator；服务运行时不要在外部移动目录。

| 路径 | 内容 |
|---|---|
| `DBMS_CONTROL` | DBMS-C++ cluster identity 与持久化格式边界；不得复制到另一套独立 cluster 或手工编辑 |
| `info/` | 集群级元数据根 |
| `info/pg_catalog/pg_authid.cat` | 角色目录。CSV 行：`oid,"name",super,createdb,createrole,inherit,login,replication,bypassrls,-1,"SCRAM-SHA-256$iter:salt$stored:server",""`（注意 stored/server 之间是 `:`） |
| `info/tlist.lst` | 数据库清单（每行一个数据库名） |
| `<dbname>/pg_tblspc/*.path` | 当前数据库的外部 tablespace canonical root marker；只允许普通单行文件 |
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
