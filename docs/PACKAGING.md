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

## 普通表的列数上限

新版可读取旧版常用的 30 列及以下表定义，宽表继续使用相同的磁盘定义格式。早于本次变更的程序无法打开超过 30 列的表定义。调用 C++ 接口的程序应使用 `TableSchema::len()` 和 `fkLen()` 读列数和外键数，并用新头文件重新编译。[旧格式读回和恢复测试](test-report.md#wide-table-20261120)

## 对象编号列的表定义

带 `oid` 列的表定义以 `0x44420010` 开头，标记该列按 4 字节无符号整数读写。新版直接打开旧版数据目录。旧程序遇到这个标记会拒绝读取表定义，如果恢复日志涉及该表，还会拒绝启动。切回旧程序时请从升级前的备份恢复。 [恢复测试](test-report.md#oid-column-20261124)

## 重写后的列统计格式

数据库在已有列统计的表上执行 `VACUUM FULL` 时，可写入新版统计快照，记录重写前后的文件身份。当前程序可读前一版及新版统计快照。只支持前一版的程序无法读取新版 `.analyzed_statistics`。需要切回旧程序时使用升级前的数据目录备份。[恢复验证](test-report.md#vacuum-target-columns-20261119)

## 单列文本主键索引格式

普通非分区表上，单列 `text`、`varchar` 或 `character` 主键的新索引使用标记 `0xC554`。索引保存同前缀记录的多个行位置，等值查找会比较实际的完整键。

旧数据目录可以直接打开。对旧索引中的长键等值查找，新版从表中核对完整值，以覆盖旧索引遗漏的记录。`REINDEX TABLE` 会从表重新建立索引并写入新标记。旧程序无法读取 `0xC554` 文件，切回旧程序时使用升级前的备份。[测试与性能](test-report.md#long-text-primary-keys-20261123)

## 复合主键索引格式

普通非分区表的新建或重建复合主键索引按末列类型选择格式。末列是受支持的有符号整数时使用 `0xC556`。索引键由前面各列摘要的前 12 字节和末列的 8 字节组成。末列整数翻转符号位后以大端顺序写入。其他复合主键索引仍使用 `0xC555`，记录所有列摘要的前 20 字节。摘要输入按主键列的定义顺序依次写入每列规范化值的 8 字节大端长度及内容，再计算 SHA-256。

每个键可关联多个行位置，执行时仍核对表中各列的真实值。旧索引可直接打开和写入。`0xC552` 复合索引使用表扫描核对，`0xC554` 按原来的 20 字节前缀定位，`0xC555` 可按完整键等值定位。运行 `REINDEX TABLE 表名` 可以生成当前格式。如果待决的两阶段事务阻止重建，完成该事务后再运行。切回不识别新索引格式的程序时应恢复升级前的数据目录备份。[升级和恢复验证](test-report.md#composite-primary-range-20261123)

## 整数主键索引格式

默认表空间的普通表在创建或重建单列有符号整数主键索引时，记录 `0xC553` 格式标记。索引页仍为 4 KiB，保留页号绑定校验。新版按索引文件记录的格式读取，原有十进制索引可以继续使用。符合条件的表可用 `REINDEX TABLE` 重新生成有序索引。需要重建索引的表结构修改和恢复操作也使用当前创建规则。

早于本次变更的程序在打开 `0xC553` 索引时会拒绝该文件。需要保留回到旧版的能力时，使用重建前的数据目录备份。[验证范围](test-report.md#signed-primary-key-prefix-20261118)

## 时间列存储与版本边界

新建的 `TIMETZ` 标量列在 `TCM1` 中使用标记 `2`，每个值占 12 字节，分别保存 8 字节本地微秒和 4 字节向西的偏移秒数。可以读取旧列和相邻字段原来的字节。旧程序无法读取使用标记 `2` 的列。

旧 `TIMETZ` 列可能含有旧程序接受的整数。没有可据此恢复的原偏移，直接执行 `ALTER COLUMN … TYPE TIMETZ` 会拒绝这些行。有可靠来源时，可以把原时间与偏移重新导入新表。旧列值全为 `NULL` 时，可以用 `ALTER COLUMN … TYPE TIMETZ` 重写后再写入有效时间。 [旧数据与恢复测试](test-report.md#timetz-storage-20261123)

新SQL标量TIME/TIMESTAMP/TIMESTAMPTZ列使用schema-D（0x4442000D）与TCM1逐列
单位标签；物理数据为int64微秒，timestamp基准为2000-01-01，TIME范围含24:00。
已有schema9/A/B/C及native工厂的Legacy秒列保持原单位，不自动重解释原八字节数据。
同类型ALTER必须解码后重写，修改schema字节或改列标签不能代替数据转换。
旧程序不识别D；升级前保留可恢复备份，并在数据副本验证导出、重建/转换、冷启动及索引。
项目磁盘格式仍不兼容PostgreSQL；有限验证与未完成范围见
[R136](issue-review-r136-temporal-microsecond-storage.md)，不能据此推导整个数据目录的升级/
降级或崩溃恢复已全面验收。

## 继承目录格式

新版可直接读取旧版的两字段 `.inherits` 记录。旧记录没有保存序号，升级时按同一个子表的父表出现顺序推算。新版删除中间父表后会在需要时写入第三个字段，保留其余父表的原序号。切回只识别两字段的程序时使用升级前的数据目录备份。 [恢复验证](test-report.md#pg-inherits-20261123)

## 访问方式目录升级

新版首次打开旧库时添加 `pg_am` 目录声明，并从表定义和已有索引记录补写能确认的访问方式。保留表和索引原有编号。升级完成后写入 `pg_catalog/.access_method_catalog_version`。 [恢复验证](test-report.md#pg-access-method-20261124)

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
统计按校验方式汇总 B+ 树页。`0xC552`、`0xC553`、`0xC554`、`0xC555` 和 `0xC556` 计入页号绑定校验，`0xC551`
计入内容校验，零标记页计入无校验。统计还单列带 checksum 的 Hash V2/Bloom BLM2/GIN V2/BRIN V2/GiST V2
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
- 新版在 Linux 上使用 `F_OFD_SETLK` 保护表锁、行锁和数据页锁的转换。切换版本时，先停止旧服务和访问同一数据目录的旧版进程，再启动新版。Linux 上的新旧锁机制无法互相识别。[Linux 锁文档](https://man7.org/linux/man-pages/man2/fcntl_locking.2.html) · [旧机制](https://man7.org/linux/man-pages/man2/flock.2.html)
- CLI 交互模式（stdin 输 SQL）适合单人运维，与运行中的服务端**不要**同时操作同一数据库目录。
- TDE：`tde_keyring=` 指向密钥文件（0600）；keyring 丢失 = 数据不可恢复（设计如此）。

## v0.1.0 高并发已知问题

见 `RELEASE-NOTES.md` §5a：同页插入竞态、周期性 world-stop、连接风暴退避。负载类场景先读该节。
