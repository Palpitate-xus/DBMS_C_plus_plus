# R304：失败 Bind 的 portal 生命周期与失败事务结果复用（DEFERRED，候选未接受）

状态：按用户最新“skip it”暂缓，现有候选尚未接受或提交。候选有限验证
涉及失败输入 portal 分配/替换、重名优先级、失败事务旧结果复用及行结果
Describe 检查；以下均为未提交候选证据，不是已接受版本的修复声明。
完整 463 仍因 R305 的 40 个格式阶段场景失败；不宣称模块或总目标完成。

## 真实问题、影响及方法

| 问题 | 已复现的使用影响 | 修改方法 / 状态 |
|---|---|---|
| Bind 失败前未替换 unnamed portal，也未保留新的未定义 portal | 输入转换失败后，Describe/Execute 可能读到旧状态，或误报 34000 | 在语句/参数形状/事务准入后、value codec 前分配 portal；与“查询已定义”分开；候选有限验证，未接受 |
| 已执行/部分输出的结果绕过失败事务检查 | 事务 Ready E 时仍可返回旧 CommandComplete 或剩余 DataRow | 在所有可执行 portal 的缓存/悬停结果复用前统一拒绝非恢复命令，25P02；候选有限验证，未接受 |
| named portal 冲突检查过晚 | 同时有重名 portal 和非法整数输入时，22P02 错误抢先于 PG 的 42P03 | 在 portal 分配边界检查重名，而非转换/规划后；不覆盖现有 named portal；候选有限验证，未接受 |
| 失败事务中仍描述行结果 | Describe statement/portal 错误返回 RowDescription | 有行结果时 25P02；恢复命令的 ParameterDescription/NoData 以及未定义 portal 的 NoData 保持可用；候选有限验证，未接受 |
| 结果格式码错误阶段不符 | PG Bind 接受码 2，在实际输出行时 22023；产品却 Bind 08P01 | 独立 [R305](issue-review-r305-result-format-demand.md) 暂缓，保留全部控制，本次不更改格式策略 |

本地 PostgreSQL 18.6 主源 `postgres.c` 确认：CreatePortal 在输入 codec 和
GetCachedPlan 前，PortalDefineQuery 在获取计划后；未知 commandTag 的 Execute
先返回 EmptyQueryResponse，再考虑普通查询事务检查。不能把所有失败 Bind
一律解释成“portal 不存在”，也不能继续复用被替换的旧结果。

## 完整范围与实际首轮证据

原 395 操作覆盖 33 组合：无旧 portal、已绑定、已完成、悬停；unnamed、新
named、重名 named；输入转换失败、缺失 statement、格式 2。再加入 statement
和恢复命令 Describe 等 68 操作，得到完整 463；原 395 均逐字/逐帧作为子序列
保留。RowDescription 的 origin/OID/width/typmod/format、ParameterDescription、
DataRow bytes/长度、空控制消息 payload、SQLSTATE、tag、Ready 状态都检查。

PG 完整 463 实际 0；旧两侧各 122 失败；当前两侧各 40 失败，均归属于
R305 格式错误阶段，不能称完整 463 通过。R304 的输入失败/旧结果/重名/失败
事务描述控制已在这轮相符。所有断言、33 组合及未修复控制都保留。

最早采集脚本预期计数误填 440，PG 已完成全部 395 操作后在最终 count assert
退出 1，私有 PG 已停止；这是驱动错误而非产品缺陷。保留原日志，按组合公式
修正为 395，重新完成三侧采集；后续严格帧和 Describe 扩展也各有完整收据。
没有通过删 SQL 修正计数。

## 边界和仍需完成

生产修改只涉及一个 Network CPP 的私有 portal 状态与检查入口，不改 header、
Main、磁盘格式、Numerics、planner/custom-generic 策略、角度函数或安全/TDE。
两侧 Network O2 新编译；其余 120 生产对象、两个 stub、44 native 和六个真实
Main 测试对象按当前源码/全头/MMD/flags/toolchain 验证复用，50 程序和两个
server 新链接；不冒称 Main fresh 或独立全量冷构建。

新 Network UBSan 两个 TU fresh；旧 Evaluator/Planner/三夹具 10 个 instrumented
对象按源/头/编译器核对复用、以当前正常库重新链接。六个 native 与两侧完整
463 已运行，无 UBSan 诊断；原 R271 和新 R305 功能失败仍保留。不是整库
sanitizer，也不是全部功能全绿。正常完整矩阵和下面的最终记录已实际完成。

R303 的 cost/custom/generic、参数保真与执行需求未修复、现已暂缓；R305 格式阶段及
[R306 TEMP SEQUENCE 标签](issue-review-r306-temp-sequence-command-tag.md)另列。
所有用户暂缓范围不恢复，原 273 状态仍 22/166/70/15；总目标未完成。
保留工作区和十八处暂存差异，只做独立本地 commit，不 push，Actions 禁用。

临时证据目录 `/tmp/dbms-r303-prepared-planning.S0DOGkOK` 不保证长期保存。

| 已完成收据 | SHA-256 |
|---|---|
| completed-preflight-receipt.json | `d4b1e5763de732060d38eade6cf33c81f5463b63bc268950f25f68a98f5493bb` |
| completed-observations-receipt.json | `41bb9dcced2f3b5fdf071e1d53f4d20fd0d0c59704e9ef66d38a6aefe711e307` |
| completed-observation-summary-receipt.json | `b70fc084ba9ce43574c4f83526ea83a646a45bbfc212537f9a298825e56e7932` |
| completed-portal-observations-v4-receipt.json | `731a55d3dd6876281d0807c5fd64cc0ed124ccb007b556c09cdcf5b85dfd6708` |
| completed-portal-old-reference-fixture-receipt.json | `a1d5243ce6017bfb5df726273d705e74ea431a98299296e641e1796ad9f757c5` |
| completed-portal-fixtures-frozen-receipt.json | `62e622712d9705914319520c62a3b2ab66602631b187e8dbee4fb5944f6ef3cc` |
| completed-portal-integration-receipt.json | `5b49644e743c76dae52df4cf3b46260b5ea6d3ce35a9162f3f87dba12df0b7ee` |
| completed-portal-primary-build-receipt.json | `2e68d37cd451ba64b149925f28d097818b7e25712af649327c0337d389b8a8c7` |
| completed-portal-early-runtime-receipt.json | `799b46f91d283594b4bcb6244f084daff33a61d0cbbcfac7aae17e3a01006633` |
| completed-portal-regression-build-receipt.json | `e13e56755d47709d772ffe15171374d06ea6d4a4926c45b63751a37885edcc47` |
| completed-portal-scoped-ubsan-receipt.json | `f701f6b8d4cc5e0620dc3b5d02260de380cee171b1f12f8db916f94cff9e3d3c` |
| completed-format-effect-observations-receipt.json | `a17d47d5d21aa96e215a47e772fb7c29bc891b9a2949d37f59d3bbf0c75eeba6` |
| completed-format-effect-summary-receipt.json | `28a405e4776bda0f79f39274ad5a3fb0de17566c6577d1e460be2b483dec1239` |

## 最终有限矩阵与仍开放问题

正常完整 82 = 44 native + 6 Main + 32 wire，实际 74 exit 0 / 8 exit 1。
每侧原 FACT114（4 个 R271）、FACT704（24 R271 + 12 R303）、MIN602（2 R271）
以及新 463（40 R305）仍失败；原全部 80 入口、SQL/断言/期限均保留，其余旧
native/Main/协议全部 0。不是 82 全绿，亦不是仅重跑一个易过的子集。

专项八个有限运行中，四个旧 native exit 0，两个 FACT114 与两个新 463
保留上述功能 exit 1；没有 UBSan 诊断。共 90 个记录 78 exit 0 / 12 exit 1，
匹配构建门 0、完整运行门 1。仅两个生产 Network TU 和两个 Network UBSan TU
fresh，120 生产对象、两个 stub、44 native、六个真实 Main 和十个既有
instrumented 对象核对复用。不能称 six Main fresh 或整库 sanitizer。

原 R303 2347 操作在新两侧完整重放，各仍与 PG 有 258 处差异，且与修复前
产品帧/值逐项相同；本次不声称修好 plan_cache_mode/custom/generic/参数帧。
R305 暂缓、R306 仍 OPEN，均未实现；不因候选通过的控制而关闭。

当前登记 990 native + 605 registered Python + 3 Main = 1598，仅为 inventory，
不是 1598 全量运行。原冻结 1492 的 1481/11 历史结果、原 273 的 22/166/70/15
状态、十八处暂存差异和全部用户暂缓范围保持不变。总目标仍未完成。

| 最终收据 | SHA-256 |
|---|---|
| completed-portal-candidate-receipt.json | `218b2d70c13f33918b213c43b239d69c5b57d6bdb69968e515249c73e35b04a0` |
| completed-portal-scoped-ubsan-receipt.json | `f701f6b8d4cc5e0620dc3b5d02260de380cee171b1f12f8db916f94cff9e3d3c` |
| completed-post-plan-observations-receipt.json | `c5929e55b3796d3dbfd2b20aa1f754ca01724b8edd2c2e4eb7fe5c823952c4d3` |

## 用户暂缓边界（2026-10-27）

按用户最新“skip it”，暂停当前 R303 prepared planning 与关联 portal 模块，
包括未提交的 R304 候选和 R305 输出需求问题。普通 continue 不自动恢复；
后续方法仅供明确恢复后使用，当前不继续实现这些模块。

本次只提交问题记录和暂缓状态，不接受或提交 R304 的生产代码、测试注册及
三个夹具文件。工作区候选和完整测试证据保留；已跑完的有限矩阵不等于已
接受版本，也不代表这些问题、原 273 总清单或完整运行门已经通过。
已接受生产版本仍为 R302 `5bf05028`；新发现的独立 R306 命令标签问题仍 OPEN，
不在此次暂缓范围内。全部既有用户暂缓边界、十八处暂存差异、no push 和
Actions 禁用约束不变。
