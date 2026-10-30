# R218：限定表名误用单个标识符长度（候选，未验收）

模式名、表名各自合法，但现有 `schema__table` 物理拼接名称超过 63 字节时，
CREATE TABLE 报 XX000。不能仅放宽创建入口：现有 tlist 固定 64 字节记录、外键引用、
存在性/改名、WAL 生命周期/图像/截断、恢复和预备事务读取均涉及相同物理身份。

## 复现与现状

未注册草稿 `tests/qualified_relation_length_protocol_draft.py` 完整 185 项，覆盖
63 字节边界、UTF-8/引用名称、DML、simple/Describe/Bind、改名、迁移模式、回滚和外键。
PG 18.6 纠正后的完整参考退出 0；首轮仅事务 Ready 状态预期写错的 8 项失败记录也保留。
修复前工作区与纯提交版本各完整 185 项，均退出 1，各 125 项失败。

候选 `RelationNameCodec.h` 分开逻辑标识符与物理身份容量，保留短名旧 tlist 编码，
长名使用带版本、完整长度和校验的列表记录；更新表名消费者及 WAL 读取，
只在外键引用确实过长时写新 schema E 格式，旧 schema 9/D 等布局不统一扩宽。
完整身份不截断，不改变各 SQL 标识符的 63 字节限制。

两侧均完成候选生产编译和链接；首轮完整 185 项仍各 103 失败，尚未接受。
由运行暴露的另一个实际根因已单列 R223：锁文件名十六进制展开超限。
ALTER TABLE SET SCHEMA 另有既有不支持路径；需要独立修复命名空间、目录、事务和依赖，
不能只移动数据文件或把 PG 成功预期改成产品失败。

R223 锁路径修复已单独本地提交 `7b0c351c`，其 38 次有限运行和提交后检查通过。
将该已接受依赖接到 R218 对象后，两侧完整 185 项仍退出 1、各 48 项失败，
均集中于迁移模式及其后续读取/截断/删除。原 185 项断言没有删除或放宽。

名称/列表/WAL 草稿 `qualified_relation_name_codec_test.cpp.draft` 完整 562 项，
两侧 O2 与 ASan/UBSan 实际都退出 0、零失败；包含全部非空截断前缀、逐字节损坏、
旧列表编码、完整限定身份及 WAL 往返。这里只对 inline 编码组件作完整插桩，
不是整库 sanitizer 验收。

真实数据库草稿 `qualified_relation_length_database_test.cpp.draft` 修正后，两侧各有
35 个主检查及两个真实冷进程各 12 个检查，均退出 0、零失败；覆盖完整外键身份、
改名、备份恢复、截断、删除、回滚以及删除长名后恢复旧短列表编码。
同一修正后的夹具对旧版本均退出 1：主检查各 16 失败，两个冷进程各 8 失败。

首版原生夹具把裸 `beginTransaction(true)` 当作自动 DDL 撤销，而该低层接口在头文件中
明确保留手工恢复生命周期；依赖修复后的首轮因此主检查各 5 失败。
已改用生产实际的 DdlTransaction 包装、dirty 标记及 CREATE undo 注册，所有原断言保留，
并重新编译运行旧版和候选；首版源码及所有失败记录保留，不把夹具误用列为数据库已修 bug。

这些通过只证明相应有限层。当前服务器/数据库测试使用按对象组合的依赖投影，
不是最终源码树验收；迁移模式失败解决后还须冻结最终两侧输入重新验证。

## 后续门槛

1. R223 锁路径依赖已独立提交；其原有短文件名和逻辑身份未改变。
2. 处理迁移模式等实际失败；保留全部 185 项及先前失败记录，不删失败断言。
3. 对旧格式兼容、真实冷进程恢复、外键完整身份、回滚、备份恢复和损坏输入作实际验证。
4. 两侧完整新增与相关回归通过，再注册测试、更新本文并单独提交；不得夹带 R222 暂缓改动。

证据目录：`/tmp/dbms-r218-qualified-length.7CIAUiH9`。
参考记录：`/tmp/dbms-private-pg18-full-fixture.3m27ni57/completed-reference-receipt.json`，
SHA256 `15cc21a7a7f3fdf5321ebc10808f17cd1b0a59eff920f7ab6975956a902ec6e5`。
首轮错误夹具参考记录位于 `/tmp/dbms-private-pg18-full-fixture.v03nlo7w`。
产品旧基线和首轮候选分别为 `completed-old-baseline-receipt.json`、
`completed-first-wire-gate-receipt.json`，均保留实际非零产品结果。
后续依赖、修正后的原生及 sanitizer 记录分别为
`completed-dependency-observations-receipt.json`、`completed-corrected-native-receipt.json`、
`completed-codec-sanitizer-receipt.json`。观察驱动退出 0 不代表其中产品执行都通过；
每项实际产品退出码和 footer 均单独保留。
目前不提交为修复、不重写原 273 或冻结 1,492 结果，不 push、不启用 Actions。

后续 R224 迁移候选已使工作区组合版本的原完整 185 项在 V1、V2 各实际通过一次。
R224 还有独立 196 项迁移测试的 30 个目录/系统模式定位失败，记录为 R225/R226；
R218 还未建立包含最终两项修复的纯提交投影、完成最终相关回归或提交。
不能把工作区两次 185 通过提升成 R218 或总清单全部完成。
