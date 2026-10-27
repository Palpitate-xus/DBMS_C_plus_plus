# R275：立即 EXCLUDE 冲突错误地上报 22023

状态：修复已检测到的 INSERT/UPDATE 排斥冲突的错误类别；同语句多行可见性和空字符串/NULL 比较仍未修复，CONS-04 及原 273 项未完成。

## 缺陷及修改方法

| 问题 | 影响 | 修改方法 |
| --- | --- | --- |
| 立即排斥约束检测到冲突后返回通用 `INVALID_VALUE` | 原生调用者收到错误的 DBStatus；Simple/Extended SQL 都报 22023，应用无法按排斥违例处理 | INSERT 清理路径和 UPDATE 释放锁路径改为返回既有 `EXCLUSION_VIOLATION`，由既有公共映射上报 23P01 |

仅修改生产 CPP 的两处返回枚举。拒绝条件、扫描、TOAST 清理、锁释放、回滚路径、错误映射函数、头文件、布局、磁盘格式和公开签名均不改。
23P01 是 PostgreSQL 的排斥违例类别。[PostgreSQL 18 错误码表](https://www.postgresql.org/docs/18/errcodes-appendix.html)
参考端在每次运行前严格核实 `server_version_num=180006`；不使用项目输出生成 PG 期望值。

## 旧新对照

| 验证 | 旧两侧 | 新两侧 |
| --- | --- | --- |
| 新原生夹具 | 各完整 40 控制、8 失败、实际退出 1 | 各完整 40/0、退出 0 |
| 新协议 helper，经既有完整注册驱动运行 | 各完整 210 控制、20 个 SQLSTATE 失配、实际退出 1 | 各完整 210/0、退出 0 |
| 真实 PostgreSQL 18.6 同一协议 helper | 完整 210/0 | 不以项目结果代替参考证据 |

原生四类 TEXT、VARCHAR、INT、int4range，分别覆盖已存在行的冲突 INSERT/UPDATE、失败后全行不变及合法 UPDATE。
协议覆盖上述类型和文本 `NULL`，每类均使用 Simple/Extended；每个冲突操作前建 SAVEPOINT，检查精确 23P01、事务 E 状态、后续 25P02、ROLLBACK TO 后 T 状态、完整行恢复、合法更新及真正 SQL NULL。
这些测试的冲突记录由分开的 SQL 语句写入；没有用本轮仍错误的同命令批量写入来生成“通过”结论。
每个参考案例使用唯一表/约束名，在 BEGIN 内创建并最终 ROLLBACK，没有 reference COMMIT、CHECKPOINT、服务重启或持久化用户表写入。

既有 `exclude_test.cpp` 的五个实际冲突断言从 INVALID_VALUE 更正为 EXCLUSION_VIOLATION，其余语句、断言、数量和完成条件保留。
这是实际 PG 23P01 证据支持的错误类别更正，不是删除冲突检查或把任意非零状态当作通过。
新 helper 仅附加到原注册 `autocommit_deferred_constraint_protocol_e2e_test.py` 的全部原语句和断言之后，不删除原 FK/事务测试，不改注册脚本。
旧新新夹具的 40/210 控制及断言字节保持一致；最初诊断和完整旧失败日志保存。

## 匹配构建与回归

Root 接受生产投影和纯提交投影各 fresh 编译 TableManage CPP、新原生夹具及 11 个既有原生 TU；其他生产 CPP、共享桩、134/131 头文件、编译器、对象和依赖匹配复用。
两侧服务器及 30 个原生/真实 Main 程序重新链接当前库；六个真实 Main 对象是当前头文件匹配复用，不冒充本轮 fresh TU。
不是全生产冷构建，不是标准 normal/repeat 构建或默认协议 MAIN。

两次新原生、24 个完整库原生、6 个真实 Main、16 个完整协议驱动，共 48 个有限执行记录，实际全部退出 0。
包含相同夹具的不同链接上下文，不是 48 个不同测试或全量库存。原生/前端沿用 180 秒审查上限，既有断言及协议内部 deadline 不改。
覆盖既有 exclusion、deferrable/NOT DEFERRABLE、语句原子性、多个 CHECK、约束重命名、INSERT/UPDATE 回滚、WITH command view、ON CONFLICT 和事务失败恢复。
本轮没有新增内存访问或分配逻辑，未运行 sanitizer，不声称全库内存/并发安全验收。

## 明确保留的失败及后续计划

新服务器再次运行原九例诊断，确认下列真实缺陷在本项后仍存在，原结果和新结果均保存：

| 仍未修复 | 真实证据 | 后续修改方法 |
| --- | --- | --- |
| 同一条多行 INSERT 的非空重复也绕过立即 EXCLUDE | PG18.6 在 INSERT 报 23P01，项目接受两条 `x` 或文本 `NULL`；本项后仍如此 | 独立修复约束扫描对本事务当前命令写入的可见性，同时保持普通 SELECT/来源查询的命令快照，取得新的旧新矩阵和独立 commit |
| 空字符串被当作 SQL NULL，延迟校验和更新也漏检 | PG18.6 在 SET CONSTRAINTS IMMEDIATE 报 23P01，项目允许空值重复；真实 NULL 对照合法 | 明确传递真实 NULL 位，不按字符串是否为空推断；覆盖写入新行和已存行、立即/延迟、部分谓词及回滚，不用简单删除 empty 判断代替完整元数据修复 |
| 历史页锁/最终 DROP 清理失败 | R231/R268/R272/R273 原日志和实际非零退出保留 | 不据本项通过关闭根因，也不擅自恢复用户暂缓的 DROP/后台、失败升级保留专项 |

真实 GiST/opclass、任意表达式/操作符、多列和并发冲突等待等完整 CONS-04 要求继续开放。
本项不改错误消息里的 table/constraint 等完整协议诊断字段，不能把 SQLSTATE 修正当作所有错误字段兼容完成。

## 版本管理及保留内容

TableManage.cpp 原本有大量与本项无关的工作区改动。Root 只在其基础上替换两处枚举；提交使用“HEAD 纯版本＋本项两处修改”的已测试 blob，不提交整个 Root CPP，也不还原那些既有改动。
本项 commit 后该 CPP 仍可显示 M，表示原有未接受修改保留；不代表本项两行没有提交。
其他五个路径为新原生、新 helper、既有协议驱动、既有 exclusion 原生断言及本文档。所有 Root/pure 源码差异按原有投影边界保留，不冒充当前整个工作树等于纯提交。
原 3052 个工作区文件除本项授权路径外逐字节核对，原暂存 18 路径保持；不 push，GitHub Actions 保持禁用。
2298 个提交源码输入匹配测试纯投影；972 native +597 注册 Python +3 Main =1572 只是库存，不是全量通过数。
冻结 1492 原始基线、原 273 项历史状态及所有用户暂缓项保持，总目标仍未完成。完成本项后继续处理上表数据完整性问题，不以此重定义成功。

证据目录：`/tmp/dbms-r275-deferred-exclusion.B2dynoyv`。

## 证据 SHA256

| 记录 | SHA256 |
| --- | --- |
| 初始九例实际PG/项目完整诊断 | `7bfedfc76fd723fc015e577ce6619ac27c2f1b14636544208fc3f534d0876e08` |
| 分开语句的22023对照 | `51f3fd3809ba3cf7eabd478c607e0a8920fc0d630664727d8e607c7d8f646126` |
| 完整旧Native40及wire210失败/PG210通过 | `0a045ce3c488842d7c532fb305b2e575c21252ed55f821e4d8bc8d413e00f804` |
| Root原有CPP改动保留及两行修复 | `199920000ba7e26ff16caa857419ec7c40b069f5d796e32a9ccc34dd45bce3fa` |
| 匹配TableManageCPP与新测试构建 | `3b28bb640210fb077c5a661fef1def20dfdaf6d690c6e2b7187757f7c544977c` |
| 新40及完整原驱动附加210通过 | `cd9817f4943c10510ca1cdd0aeb5cedcef26020d4ef9895c1eaca6aa4bbc49cc` |
| 30个当前库链接及依赖 | `ab038b78c65a338a956b074c310b37bf901fc9412a709567e6c9f32d8e6291bd` |
| 48个有限执行记录通过 | `3f38f03cf2af62a9ffdec020eb778ba3def6fb4ad642b84a1351134fd3dfd45c` |
| 新服务器仍有同命令与空字符串缺陷 | `82819ee0097b602279ace73e1bd81d85714d021cb401d3c5632c88e5b8bba347` |
