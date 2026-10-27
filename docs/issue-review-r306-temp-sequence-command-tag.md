# R306：CREATE SEQUENCE 的真实 CommandComplete 身份

状态：本项标签修复已完成有限验证；仅修复 CommandComplete 身份，不宣称
序列功能全绿。独立 R307 的 20 项错误码失败全部保留，原总目标未完成。

| 问题 / 条件 | PostgreSQL 18.6 | 修复前两侧 | 方法 / 状态 |
|---|---|---|---|
| TEMP/TEMPORARY、UNLOGGED 及混合大小写 | CREATE SEQUENCE | CREATE TEMP / CREATE TEMPORARY / CREATE UNLOGGED | 从已有有效解析结果的 CreateSequence 身份生成标签；已修复 |
| 前导/中间注释、换行与制表符 | CREATE SEQUENCE | 可能将注释或修饰词放入标签 | 同一真实命令身份，不继续拆分 SQL 前两词；已修复 |
| 重名创建错误 | 42P07，无 CommandComplete | XX000，无 CommandComplete | 独立 [R307](issue-review-r307-sequence-duplicate-sqlstate.md) OPEN；保留全部断言 |

错误标签会使依赖 CommandComplete 的客户端错误识别成功创建的对象；序列
实际可以创建并 nextval，并非创建失败。本次只改协议完成标签，使用现有
SQLParser::parseForBinding 的 SqlCommand::CreateSequence，不改 parser/header、
DDL 存储、事务、序列持久性、prepared planning 或 portal 生命周期。

## 完整对照与夹具

新增 tests/sequence_command_tag_protocol_e2e_test.py，共 100 控制：十种前缀
各 Simple/Extended；每组检查创建标签、nextval 实际值 7、同 namespace 重名
错误、Sync 后正常 SELECT、DROP。核对完整消息种类/顺序、CommandComplete
原始零结尾 payload、成功/错误无混发、Ready I、行内容及空 Parse/Bind payload。
这不是仅按 SQL 文本猜命令的单元测试。

候选修改前 PG 全部 100 exit 0；旧 Root/pure 各 36 失败，其中 16 是本项
标签错误，20 是 R307 的 XX000/42P07。固定全部 100 后才修改生产代码，
不得因独立 R307 未修复而删掉重复创建或错误恢复断言。

首次草稿对 TEMP 创建后发送普通 CREATE SEQUENCE，错误地假定一定冲突。
PG 实际可在 public 创建同名持久序列，因此草稿 PG 14 失败不是产品 bug。
保留原草稿和全部三侧日志；将负控改为同创建 namespace（保留 TEMP 等前缀，
仅去除 IF NOT EXISTS），保持 100 控制和全部正控/恢复/DROP 请求，换全新
私有 PG 后上述完整 100 全通过。不能把驱动修正说成产品修复。

本次在 R302 已接受生产源码上应用标签修改后构建，不使用暂缓 R304 的二进制或源码。
Root 未提交 R304 Network hunks、测试注册和三个夹具均保留但排除本次 commit。
R303–R305 及全部既有用户暂缓模块不恢复；不 push，Actions 禁用。
原 273 总清单仍未完成。临时证据目录：
/tmp/dbms-r306-sequence-command-tag.nZJ7AvcS。

## 最终有限验收与仍未解决的问题

两侧完整 100 夹具现各 20 失败，全部为 R307 重名错误码；16 项原标签错误
均消失，其他实际值/消息/恢复/DROP 控制相符。不是完整夹具全绿。原 100 的
SQL、断言、帧检查、期限未变。PG 完整 100 为 0。

正常矩阵完整 100 入口 = 44 native + 6 真实 Main + 50 wire，92 exit 0 / 8
exit 1。全部原 80 入口保留；另加入新 100 夹具、原 view 54 和八项序列协议
回归。失败每侧为 FACT114（4 个 R271）、FACT704（24 R271 + 12 已暂缓 R303）、
MIN602（2 R271）、新序列 100（20 R307）；原八项序列和 view 均 exit 0。
不是只跑标签正控，也没有因本项修复删除旧失败。

生产仅两侧 Network CPP 的两个 O2 TU fresh；120 生产对象、两个 stub、44
native 和六个真实 Main 测试 TU 按源码/头/依赖/编译器/flags 核对复用。
50 个测试程序及两个 server 新链接，Main/header 未改，不称 Main fresh。

专项 UBSan 新编译两侧 Network TU，重用十个匹配的 Evaluator/Planner/三夹具
instrumented TU 并以当前库重链接；六个 native 加两侧完整 100 wire，共八个
实际运行（四个 0、四个上述已知功能 1），无 UBSan 诊断。其他库和 Main 未
全部插桩，不称整库 sanitizer。总共 108 有限记录 96 zero / 12 nonzero，
匹配构建门 0，完整运行门 1；不把已有失败归零。

已接受候选登记 990 native + 605 registered Python + 3 Main = 1598，只是
inventory，非 1598 全量运行。冻结 1492 的 1481 PASS/11 FAIL 历史结果、
原 273 的 22/166/70/15 状态均不变。R307 OPEN；R303–R305 及全部既有用户
暂缓模块不恢复。Root 原有未提交代码和十八处暂存差异保留，不 push。
本记录的 Root 侧为剔除暂缓/未接受源码的受控投影，不等于原始脏工作区
所有草稿均已验收；纯接受副本与最终 commit 的源码必须逐字一致。

| 有限验收收据 | SHA-256 |
|---|---|
| completed-original-tag-v2-reference-old-receipt.json | `b0dcebe4766a84971fc7dca43bb94af6b853d87e72e80aae6b681e29de7402ea` |
| completed-fixtures-frozen-receipt.json | `95e7ac7c7350c9f787401b6c532cabe9856ab91523b45cec44526a888e2d7e35` |
| completed-tag-primary-build-receipt.json | `91b74bfe9efa9c65ee4ab084d5b943778d8cf6951416e42acbdce417cad9e609` |
| completed-tag-regression-build-receipt.json | `dc5938388fa8613d3102545ab6a2571b27498e85adeb17bad2a2fca3bbc9eb63` |
| completed-early-runtime-receipt.json | `26c1772e8ebd6e429dea5dec28dac0224b9ef46477e6175466d80e7d5c439e26` |
| completed-tag-candidate-receipt.json | `c5c9bfbb15462cc04d18829fc790afb4bb27d054a57a6d3c3fcbfcc8787b874d` |
| completed-tag-scoped-ubsan-receipt.json | `f6d46aaa3232b669589a44988c89d05cc70f301628a4271d3c2868c99c93a076` |
