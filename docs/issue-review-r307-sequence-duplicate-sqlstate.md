# R307：序列重名创建错误缺失 SQLSTATE 42P07

状态：已完成本项有限修复——真实重名错误返回 42P07；不宣称所有重名请求
均被正确拒绝或通知码已修复。独立 R308/R309 和总目标仍未完成。

| 问题 / 控制 | 修复前事实 | 方法 / 状态 |
|---|---|---|
| 已登记关系冲突的错误码 | PG 42P07；产品 XX000 | 在序列 DDL 的真实 existing 分支声明 SQLSTATE 42P07；已修复 |
| 表、view、序列的对象身份与值 | 应保留原 OID/kind、表/view 值、序列配置和分配流 | 原生与协议均检查；不以替换或重建对象取得正确错误码 |
| IF NOT EXISTS | 原对象保持不变、NOTICE 后成功；通知码 PG 42P07、产品 00000 | 独立 [R308](issue-review-r308-notice-sqlstate-carrier.md) OPEN，保留失败断言 |
| TEMP 表与 TEMP 序列同名 | 两个初次冲突场景，PG 42P07；产品错误创建成功 | 独立 [R309](issue-review-r309-temp-sequence-table-conflict.md) OPEN，优先后续处理；不混算为错误码已修复 |

## 原因与修改边界

已接受版本的 DdlExecutor::executeCreateSequence 在 catalog existing 分支
输出 ERROR: relation ... already exists 后返回 true，未携带 SQLSTATE。
协议已有解析旧式 `(SQLSTATE ...)` 诊断的机制。本次只在该真实 DDL 错误
产生处分配 42P07，不在协议层对任意 already exists 文本统一换码。

不改变 header/Main/Network、解析语法、名称解析、catalog/存储结构、事务、
IF NOT EXISTS 通知处理或其成功/失败规则。特别是不引入从任意用户 NOTICE
文字猜 SQLSTATE 的新解析器。R308 需要独立的元数据传递边界，不能靠本项
错误码修正声称通知已经相符。物理/catalog 不一致等未覆盖范围不冒称修复。

## 冻结的完整复现

原 R306 完整 100 在 PG 通过，产品 20 个已报告错误均为 XX000/42P07。
新增协议 260：三种命名空间 public/自定义/temp ×三种已有关系 sequence/
table/view ×Simple/Extended，覆盖真实值、IF NOT EXISTS、显式失败事务、
25P02、保存点恢复、后续值和 DROP；检查完整消息种类/顺序、原始 command
payload、Ready I/T/E、通知 severity/SQLSTATE。PG 260 exit 0；旧产品两侧
各 54 失败，逐条分类为 34 个真实重名错误码、18 个通知码、2 个错误成功。
36 个错误请求并非全部得到错误，不能把错误成功也描述成 XX000。

新增原生 108：两种命名空间 ×三类关系，直接验证 bool 失败契约、诊断、
OID/kind/属性、物理序列不存在或既有序列配置/分配流不变、NOTICE 成功。
旧两侧各 6 个 SQLSTATE 失败。CLI 的 NOTICE 文本不必带 SQLSTATE，原生
仅检查其非错误、已有关系提示和原对象；协议仍严格要求 PG 的通知码。

首次原生夹具将未引用 duplicate_S 直接用于存储 API，未作 SQL 小写折叠，
导致 Root -6 中止；保留原代码和日志，修正调用方名称为同义小写，不改 108
检查或生产代码，然后两侧完整重编译/运行得到上述六个真实差异。该调用方
错误不是数据库缺陷，不用它宣称修复前产品有 XX001 bug。

全部 108/260 固定在生产修改前；不删 R308/R309 失败或旧 R271/R303 断言。
临时证据目录：/tmp/dbms-r307-sequence-duplicate.h7qqyppH。原工作区和十八处
暂存差异保留；全部用户暂缓范围保持，不 push，Actions 禁用，总目标未完成。

## 最终有限验收

两侧原生完整 108 均 0；原 R306 协议完整 100 均 0，原 20 个错误码失败均
消失。新协议完整 260 每侧从 54 降为 20：34 个真实重名错误均已 42P07，
剩下 18 个 R308 通知码与 2 个 R309 错误成功，完整断言不删。PG 完整 260
为 0。不能将新 260 写成全绿或声称所有临时关系名字冲突已修复。

正常完整 104 = 46 native + 6 真实 Main + 52 wire，96 exit 0 / 8 exit 1。
全部原 100 入口保持，加新 native/协议各两侧四入口。每侧 FACT114 的 4 个
R271、FACT704 的 24 R271 + 12 已暂缓 R303、MIN602 的 2 R271，以及新 260
上述 20 个 R308/R309 仍失败；原 100 标签协议、八项序列、view 和其他旧
入口均 0。没有修改旧 SQL、断言、编译优化级别或期限。

生产仅两侧 DdlExecutor 两个 O2 TU fresh；120 生产对象、两个 stub、44
既有 native 和六个真实 Main 对象按源/头/MMD/编译器/flags 核对复用；新
native 两个 TU fresh，全部 52 测试程序和两个 server 当前库新链接。
没有 header/Main/Network 修改；不称六个 Main fresh 或全量冷构建。

专项 UBSan 新编译两侧 DDL 和新原生夹具共四个 TU，核对复用既有 Network/
Evaluator/Planner/三夹具共十二个 instrumented TU，重新链接八个 native 和
两个 server。十二个实际运行中八个 0，四个保留上述 FACT114/260 功能失败，
无 UBSan 诊断；旧八个专项入口全部保留。其他库/Main 并未全部插桩。

合计 116 有限记录 104 exit 0 / 12 nonzero；匹配构建门 0、完整运行门 1。
991 native + 606 registered Python + 3 Main = 1600 只是 inventory，非全量
1600 运行。Root 侧是剔除既定暂缓/未接受源码的受控投影，不是原始脏工作区
全部草稿已验收；最终 commit 必须与纯接受测试副本逐字匹配。

Root 原有 R304 候选和十八处暂存差异保留，所有既有用户暂缓范围不恢复。
冻结 1492 的 1481/11 历史结果、原 273 的 22/166/70/15 状态不变。R309
错误成功优先后续复查，R308 通知元数据仍 OPEN；不 push，Actions 禁用。

| 真实收据 | SHA-256 |
|---|---|
| completed-original-duplicate-reference-old-receipt.json | `52b54fe90b33da6e23fda8979c92fd0e39a7324ca11a993a57b1549b17356371` |
| completed-old-native-v2-receipt.json | `ded9ff25ea24564c12adf2d68916a47bfd772ab122568f5304b00c6dc8ea0d16` |
| completed-fixtures-frozen-receipt.json | `f881f5733c7fe2f7770a8783d1198765c32e9c7e4b4eff2dd3d7b29e167aa31c` |
| completed-primary-build-receipt.json | `67e64280d94c4112f52face70ebafbd7bc83f5718338799c3586145f79c87a31` |
| completed-regression-build-receipt.json | `c7c829b90d18956715b8fad46e1f1170a7c2e6e58ed683402d775de128010b86` |
| completed-early-runtime-receipt.json | `5e2c6c7fdf164b02fc5507e47c75ad4897e15708df38d44a270a5c5c80fbafbb` |
| completed-candidate-receipt.json | `0a17b421e732bb668bc527e2bd87c26efc43241ed4427bef48889603e8de2f17` |
| completed-scoped-ubsan-receipt.json | `47d320c59250bdd0348eb51097094e66c1a3e77e6f03a8793b8efdc2e179b065` |
