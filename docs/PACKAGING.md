# 打包与部署

版本以 `src/common/version.h` 为准。部署前请核对[兼容性契约](compatibility-contract.md)和[已知问题](review-index.md)；历史性能结果不能替代目标环境的验证。

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

## 编译依赖

用 CMake 编译需要 CMake 3.16 或更新版本、支持 C++17 的 g++、make、zlib 和 ICU 的开发包。在 Debian 或 Ubuntu 上可安装以下软件包：

```bash
sudo apt install cmake g++ make zlib1g-dev libicu-dev
```

运行程序需要对应的 zlib 和 ICU 共享库。编译支持加密连接的程序还需安装 OpenSSL 开发包（`libssl-dev`），运行端到端测试需要 Python 3。

二进制分发请同时提供仓库根 [THIRD_PARTY_NOTICES](../THIRD_PARTY_NOTICES) 中备份清单摘要实现的第三方版权、许可条件与免责声明。CMake 安装会将该声明放入安装前缀下的 `share/doc/DBMS_C_plus_plus/THIRD_PARTY_NOTICES`；单独复制 `dbms_main` 时也须附带该声明。

## 重写后的列统计格式

数据库在已有列统计的表上执行 `VACUUM FULL` 时，可写入新版统计快照，记录重写前后的文件身份。当前程序可读前一版及新版统计快照。只支持前一版的程序无法读取新版 `.analyzed_statistics`。需要切回旧程序时使用升级前的数据目录备份。[恢复验证](test-report.md#vacuum-target-columns-20261119)

## 整数主键索引格式

默认表空间的普通表在创建或重建单列有符号整数主键索引时，记录 `0xC553` 格式标记。索引页仍为 4 KiB，保留页号绑定校验。新版按索引文件记录的格式读取，原有十进制索引可以继续使用。符合条件的表可用 `REINDEX TABLE` 重新生成有序索引。需要重建索引的表结构修改和恢复操作也使用当前创建规则。

早于本次变更的程序在打开 `0xC553` 索引时会拒绝该文件。需要保留回到旧版的能力时，使用重建前的数据目录备份。[验证范围](test-report.md#signed-primary-key-prefix-20261118)

## 时间列存储与版本边界

新SQL标量TIME/TIMESTAMP/TIMESTAMPTZ列使用schema-D（0x4442000D）与TCM1逐列
单位标签；物理数据为int64微秒，timestamp基准为2000-01-01，TIME范围含24:00。
已有schema9/A/B/C及native工厂的Legacy秒列保持原单位，不自动重解释原八字节数据。
同类型ALTER必须解码后重写，修改schema字节或改列标签不能代替数据转换。
旧程序不识别D；升级前保留可恢复备份，并在数据副本验证导出、重建/转换、冷启动及索引。
项目磁盘格式仍不兼容PostgreSQL；有限验证与未完成范围见
[R136](issue-review-r136-temporal-microsecond-storage.md)，不能据此推导整个数据目录的升级/
降级或崩溃恢复已全面验收。

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
Bloom、GIN、BRIN、GiST 与 SP-GiST 索引校验：

```bash
/path/to/dbms_main -D /srv/dbms-instance --verify-data-checksums
```

该命令验证主/分区/TOAST/unlogged init heap，以及 `.idx`/`.idx_*` B+Tree
索引、`.hidx` Hash、`.bidx` Bloom、`.gin` GIN、`.brin` BRIN、`.gist` GiST
和 `.spgist` SP-GiST 侧车，包括从 tablespace marker 解析的外置文件；诊断定位到索引文件和 heap block。
统计会分开列出使用页号绑定校验的 `0xC552` 或 `0xC553`、使用旧内容校验的 `0xC551` 与无 checksum 的
zero-marker legacy 页，并单列带 checksum 的 Hash V2/Bloom BLM2/GIN V2/BRIN V2/GiST V2
与未校验的 Hash V1/Bloom BLM1/GIN V1/BRIN V1/GiST V1/SP-GiST V1 文件。GIN V2/BRIN V2/GiST V2/SP-GiST V2
扫描验证格式头、entry-count 边界与 CRC32C，但不解析 GIN postings、BRIN ranges
或 GiST range entries、SP-GiST coordinates 的完整语义。它也不验证 quadtree 拓扑或全部 catalog/metadata，没有锁机制
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
| `pg_hba.conf` | 访问规则示例：`host all <user> 127.0.0.1/32 scram-sha-256`。空规则或无匹配规则返回 `FATAL / 28000` 并关闭连接，不会等待后续认证 |
| `dbms.conf` | 可选 `key=value`：`tde_keyring`、`pool_mode`、`pool_size`、`checkpoint_interval`（1..1000000，0 非法）、`audit_level` 等。进程启动时加载；管理员可用 `SELECT pg_reload_conf()` 应用可 reload 字段，启动字段需重启 |
| `<dbname>/` | 每数据库一目录：`*.dt` 堆文件、`*.idx` 索引、`<file>.tde` TDE 边车信封（48B/页 nonce+MAC）、`.publication` 逻辑复制目录、`.runtime_stats` |
| `*.wal` / WAL 段 | 每数据库目录下 16MiB 段文件 |
| `dbms.log` / `slow_query.log` / `audit.log` / `auto_explain.log` | data directory 下的运行日志 |

容量与调度存在尚未接通的配置：`buffer_pool_frames` 不控制实际堆/索引池，
应使用 `DBMS_BUFFER_FRAMES` / `DBMS_INDEX_BUFFER_FRAMES`；`checkpoint_interval`
是 CLI 的语句计数阈值，不是网络服务后台 checkpoint 的秒数。对应的 `pg_settings`
显示值也不能作为实际容量或周期依据，详见[手册第 25 节](MANUAL.md#25-guc-参数配置)。

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
