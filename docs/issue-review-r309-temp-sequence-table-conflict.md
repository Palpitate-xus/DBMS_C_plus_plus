# R309：TEMP 序列的真实 namespace、隔离与生命周期

状态：本项有限修复已完成两侧实际验证。临时序列具备所测真实隔离/清理行为，
但不宣称 TEMP view、所有临时对象语法、完整数据库或总清单全部完成。
不把“错误码相符”、原生通过或构建成功写成全部临时序列功能完成。

| 问题 | 已复现影响 | 修改方法 / 当前状态 |
|---|---|---|
| CREATE SEQUENCE 丢失 TEMP/TEMPORARY/LOCAL TEMP 修饰 | 序列进入 public、跨连接可见、所有者退出后仍存在 | 在现有 CreateObjectStmt.options 保留 temporary；已修复 |
| 未进入真实会话临时 namespace/清理入口 | 与 TEMP 表同名的首次创建错误成功 | 复用现有临时 namespace、存储名称、访问标记、清理标志；已修复 |
| pg_temp DROP 被当作普通 schema | DROP 返回 3F000，后续重建被旧对象阻塞 | 先解析别名，认可实际自己的临时 namespace，再用既有 drop plan；已修复 |
| DISCARD 保留 namespace 时只删序列文件 | catalog 幽灵行导致 XX001，重复 DISCARD 58030 | 表优先清理；独立序列文件成功删除后按既有 dropClass 退役目录行；已修复 |
| TEMP view 修饰也丢失 | 正确 TEMP 序列暴露原 view 名字冲突问题 | 独立 [R310](issue-review-r310-temp-view-lifecycle.md) OPEN；不以查 public 同名对象伪装修复 |

## 实际对照，不只检查标签

三侧各 92 操作、六组 TEMP/TEMPORARY/LOCAL TEMP ×Simple/Extended，两个
连接及六次所有者 Terminate/EOF。PG pg_temp.nextval 正常、其他连接看不到；
旧产品 pg_temp 查不到，peer 却能取值；所有者退出后 peer 仍能通过 public
取值。普通持久序列是独立正控，证明不是连接/服务器被一起关闭。每个产品
相对 PG 有 60 个语义差异；不是 60 个独立 bug。

扩展到完整 182，保留原 92 全部操作，再加 schema 拒绝、pg_temp 显式创建、
public/temp 同名共存、首个 TEMP 表冲突、创建/删除后的保存点恢复、后续值。
严格夹具比较完整帧种类/顺序、SQLSTATE、Ready、OID、行值、原始 DataRow
payload、长度/空 payload、通知元数据和 EOF。PG 全部 182 exit 0；旧两侧
各 108 失败。通知对的 tuple/list 表示在严格执行前统一为 JSON 列表；请求
和预期未改，PG 完整复核通过，不能把表示修正当产品修复。

原生最初 134 检查实际 parser 标记、两个会话的 namespace/OID/persistence、
物理文件、取值、普通对象不受影响、schema 拒绝和 TEMP 表冲突；旧两侧各
74 失败。第一轮候选两侧 134 全过，但协议 182 各有 34 失败，暴露原生只测
默认删除 namespace，未覆盖 DISCARD 的 preserveNamespace=true。保留全部
首轮源码、对象、程序和日志，未宣称验收完成。

因此保留原 134 所有 SQL/检查，再加 14 个目录保留、重复清理、42P01 和
pg_temp DROP 控制，得到 148。追加控制已用原始 R307 生产库两侧重新编译/
运行，各 80 失败；随后才修改清理和 DROP 分支。原 134 和协议 182 在初始
候选前冻结，完整 148 在追加修改前冻结，不将两个不同冻结时点混说成一次。

## 实现边界与证据

当前只改 parser.cpp 的 CREATE SEQUENCE 修饰保留、DdlExecutor.cpp 的该
对象 namespace/别名入口和 TableManage.cpp 的已有会话临时清理函数。不改
header/Main/Network、磁盘布局、通用 SELECT/PREPARE 语法、完整 catalog
模型或全局缓存/借用引用寿命。已有表/SERIAL 路径、普通序列与跨 schema
语义必须保持；所有用户暂缓模块不恢复。

第二轮使用独立对象/日志/程序/收据，不覆盖首轮；首轮源已按记录的逆向
增量重建归档并核对初始 2335 输入及对象哈希。当前 Parser 两个对象来自
本轮首阶段；DDL/TableManage 四个和新 148 两个测试对象已按第二轮源码新编译。
Main/header 未改，六个真实 Main 仅可核对复用，不冒称 fresh。

临时证据目录 /tmp/dbms-r309-temp-sequence.T03W6FZK。原工作区和十八处暂存
差异保留，不 push，Actions 禁用。原 273 清单、冻结 1492 历史结果不因有限
矩阵而改写；总目标仍未完成。第二轮实际运行及边界记录如下；提交另按纯接受源码和版本核验执行。

## 第二轮实际完成的有限矩阵

两侧原生完整 148、协议完整 182 均 exit 0；标准库逻辑/实际物理文件、两个
连接的同名隔离、public 遮蔽/普通控制、DISCARD 保留 namespace、重复清理、
pg_temp DROP、保存点创建/删除回滚、六次所有者退出后的可见性检查都保留。
不是只改 catalog 的 persistence 字段或只让错误码相符。

正常完整 108 = 48 native + 6 真实 Main + 54 wire，100 exit 0 / 8 exit 1。
全部原 104 入口保留，新原生/协议各两侧四入口追加。剩余每侧 FACT114 的
4 个 R271、FACT704 的 24 R271 + 12 已暂缓 R303、MIN602 的 2 R271，以及
旧 260 的 18 R308 通知码 + 2 R310 TEMP view 错误成功。后两项是旧失败
集合迁移，不因总失败数仍 20 而称失败身份未变。原八项序列、view 标签、
原 R307 原生 108、原 R306 协议 100 及新 148/182 均 0。

当前生产对象六个为本轮新编译：Parser 两个首阶段对象精确核对后沿用，
第二轮 DDL/TableManage 四个 fresh；116 个更早对象、两个 stub、46 个既有
native、六个真实 Main 核对复用，新 148 两个测试 TU fresh。54 个测试程序
及两个 server 当前库新链接。首轮被替换的两个 DDL 和两个 134 测试对象
仍保留，不能把“当前六个 fresh”说成本轮仅发生六次生产编译。

专项 UBSan 当前三生产 CPP 与新 148 每侧共八个 TU fresh，十四个既有
instrumented 对象核对复用，十个 native/两个 server 新链接。全部旧十二
专项入口保留并追加新 148/182 两侧四入口；共十六运行，十二个 0、四个上述
FACT114/旧 260 功能失败，无 UBSan 诊断。Main/其他库没有全部插桩，非整库
sanitizer。合计 124 有限记录 112 zero / 12 nonzero，匹配构建门 0、完整
运行门 1；不能称整套运行全绿。

当前登记 992 native + 607 Python + 3 Main = 1602，只是 inventory，不是
1602 全量运行。受控 Root 投影不等于原始脏工作区全部草稿已接受；最终纯
commit 必须与测试副本逐字匹配。原 273 的 22/166/70/15 和冻结 1492 的
1481 PASS/11 FAIL 历史结果均不改写。R310 优先下一项，R308 仍 OPEN；
全部用户已暂缓模块保持，不 push，Actions 禁用，总目标仍未完成。

| 最终有限收据 | SHA-256 |
|---|---|
| completed-observations-receipt.json | `4ec29d1dd9d2c227b692455efaf7b2f92a1db2cd357cb0258089babb6e255bec` |
| completed-expanded-observations-receipt.json | `a10964e5fa11ad92b2adb85c60a8caf20601ba086e79676555bb9c79c3118a50` |
| completed-original-fixture-reference-old-receipt.json | `8a719b0ae9e7b42243f546be5fe970fec7e8ca01ef8c761478d47641737bbb07` |
| completed-old-native148-receipt.json | `ae4bf1f612444181c527e1f028b15640dc7dfcd68d7c36ed5bedcbe24a8f9313` |
| completed-fixtures-frozen-v2-receipt.json | `6180385e1c4ea838480496f322006accc6007cf50e159024d968e79b80608ce5` |
| completed-primary-build-v2-receipt.json | `e656d084a8e9632d3060bbd5e2f727821dc4c120f5d03541f8eb3d7f02a973de` |
| completed-regression-build-v2-receipt.json | `f48d7a1b5bdede1192fb36e41d8fc56fdb3525b45fcb358f2f11f2fc998060be` |
| completed-early-v2-runtime-v2-receipt.json | `e93a7b4057033c85138103557ef9ee9fa0d7b9cb55b608c75e228b76ec82b6ba` |
| completed-candidate-v2-receipt.json | `9666a97b7016ec5340d15e113904a0fc186964bd1599e1df6a22eb434e550b99` |
| completed-scoped-ubsan-v2-receipt.json | `666773f649455c70d8779d1f47fcf45b18191ebba920822503bd52a0ab18860a` |
| completed-initial-source-archive-receipt.json | `0e6bb65bc70e7691cc0ece22a7fad0029710b7fad55fdedacd6d7d6e3f065bc4` |
