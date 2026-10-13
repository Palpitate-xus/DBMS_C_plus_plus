# R14：publication 读取失败不能解释为未发布

## 问题与原始证据

`PublicationCatalog::publishes` 的两个重载原来在完整列表读取失败时返回 `false`。普通 DML 因而可能修改已发布的表，却不产生对应 logical change；错误被伪装为正常的“不属于 publication”。这是项目本地逻辑预览的数据完整性问题，不是 PostgreSQL 逻辑复制兼容承诺。

新原生回归先创建合法 publication，随后增加一份不完整的本地 metadata，检查成员查询及四种操作查询。原始实现实际 exit 1，五个查询均吞掉错误；原文件仍保留，修复后同一回归 exit 0，均报告 `58030`。

原始及候选证据位于 `/tmp/dbms-review-fix-donor.N9sdas6f/publication_membership_read_error_baseline` 与 `publication_membership_read_error_fixed`。完整审查历史见[原审查](code-review-2026-10-13.md)，原始日志不因候选通过而删除。

## 修复方法

- 两个成员查询重载传播带 SQLSTATE `58030` 的 `DbError`；有效的非成员、禁用的操作仍返回 `false`。
- INSERT、UPDATE、DELETE 在默认值、触发器或物理修改前取得可靠的 capture 决策；读取错误映射为 `DBStatus::IO_ERROR`，不在修改后重新查询并漏采。
- TRUNCATE 在 WAL/heap 修改前校验 publication 列表；失败释放已取得的表锁。
- 修复 metadata 后可以正常重试，不引入永久的负成员缓存。
- 原逻辑解码测试中把错误当作 `false` 的断言改为要求真实 `58030`；其余原断言保留。

## 实际验证

Root 标准 `scripts/build.sh` 真正重新编译全部 58 个生产 translation unit，exit 0；编译及链接期间输入签名保持不变。最终 Main SHA256：`739ebab4746b9347ca66b9e92a0550adebb71d6cfcf254b56a6f3f50a542ec65`。

从这套 58 个已核对源、header、compiler 与 flags receipt 的生产对象出发，新编译 driver/stubs 并在独立目录运行以下九个完整原生驱动，全部 exit 0：

| 驱动 | 覆盖 |
| --- | --- |
| `publication_membership_read_error_test` | 两个重载、五个读取错误、正常非成员/操作开关、修复重试 |
| `publication_write_preflight_test` | 四种写入失败无行变化、无 logical change、无残留隐式事务；健康 DML；显式事务的早先写入保留；正常 TRUNCATE capture |
| `logical_decoding_test` | 原完整逻辑预览、publication、slot、事务边界及插件驱动 |
| `catalog_value_snapshot_test` | 已合入 R18 的 owned snapshot、增长与并发一致性 |
| `catalog_complete_read_test` | R16 的 691 项完整读取控制 |
| `catalog_optional_directory_test` | 冷启动无 catalog 目录正常路径与 loaded metadata 缺失拒绝 |
| `catalog_snapshot_test` | 原完整 snapshot 隔离、rollback 与冷读取 |
| `table_owner_atomicity_test` | 原 owner 更新、失败及健康重试断言 |
| `table_owner_peer_retry_test` | O01 真实 peer refresh、真实 dirty conflict 和持久健康重试 |

日志：`/tmp/dbms-review-fix-donor.N9sdas6f/root-build-integrated-r14-fresh-header.log`、`root-integrated-r14-catalog-owner-native.log`。驱动完成后再次确认生产输入签名未变。

本项未宣称对并发 publication DDL 提供新的 snapshot 隔离协议，也未扩展到用户暂缓的安全/TDE 专项。合入后的全量注册回归另在[修复总账](review-fixes-2026-10-13.md)记录。
