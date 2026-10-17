# R116：TEMP VIEW命令完成标签误报为SQL第二个词

状态：本范围已修复并完成有限验收，按本项独立本地提交；不提升原273项状态。

## 问题与影响

创建普通临时视图实际成功，但CommandComplete错误返回CREATE TEMP、
CREATE TEMPORARY或CREATE OR；带前置注释还可能返回注释中的前两个词。
真实PostgreSQL18.6对这些合法非物化视图语法均返回CREATE VIEW。
依赖命令完成标签的客户端无法可靠识别对象操作；视图可查询不等于协议正确。

最初R114额外19条观察发现5个TEMP VIEW tag差异。此次展开普通/TEMP/
TEMPORARY/OR REPLACE及注释的54条完整协议控制，实际180006参考全部通过，
固定旧程序14条tag失败、实际退出1；其他读取、DROP与Ready控制保留。

## 修改方法

NetworkServer.cpp的commandTagFor原先为少数语法作词组特判，其余CREATE取
原始SQL前两个词。补充纯解析：由真实CreateViewStmt且非materialized判定
CREATE VIEW，不把TEMP、OR、注释或名字当对象类型。
复用parseForBinding，只解析语法，不执行视图查询、表达式、函数、提供者或I/O。
其他事务/DML/DDL标签和fallback保持原路径。

明确不更改物化视图/CTAS标签和实现，用户暂缓范围不恢复；也不把本项扩展
为view rewrite、NULL视图物化、来源metadata或安全视图功能完成。

## 永久验证

`tests/view_command_tag_protocol_e2e_test.py`：54条完整控制。
9种创建前缀各在Simple与真实Parse/Bind/Execute/Sync执行一次，再读取视图
并DROP；检查准确tag、行值、无Error、Ready以及扩展协议ParseComplete/
BindComplete计数。合法注释在前缀与关键字间；普通CREATE VIEW和普通
CREATE OR REPLACE VIEW作为不应改变的对照。

旧参考夹具位于`r116-view-command-tag-protocol-candidate.py`，与永久测试仅
根目录定位从固定路径改为`__file__`相对路径、删除个人过程说明的注释不同；
实际执行SQL和54断言相同。没有降低断言或把失败改成预期成功。
不新增原生测试入口；相邻原生测试按fresh stubs/O2/初始化前隔离规则执行。

日志目录 `/tmp/dbms-review-r46.ifu6SWLp/`：
`r116-reference54-initial.log`、`r116-old54-initial.log`、normal/repeat构建及
`r116-final-{driver,core,native,neighbor}.log`与逐协议结果目录。
固定旧程序`r116-pre-fix-dbms-main` SHA256：
`2c8c1b64fb11188bbd65fc471c7d75cfaba94633b844dcc3290058f0e5d9aa58`。
它含既有保留候选，不冒充clean HEAD。新增仅一个协议注册和9行内部生产实现。

## 最终验收与范围

自身normal/repeat实际重编1/0CPP、均退出0。当前生产及库均O2；最终冻结输入下
8个完整native、13个完整协议，共21个不同入口全部实际退出0，含本项54、
完整parser/view/替换/typed参数、catalog691、R114的102/28、原182/110数组、
R115的228、R113的171、DML/TRUNCATE标签、会话、真实RETURNING及默认完整协议。
native使用fresh stubs及初始化前隔离器，不只挑通过子例，也未改旧断言/超时。
完整门禁及逐协议结果保留，不将历史1391完整回归当成本项新全量。

最终binary：
`3d4a2767eb40b7f594b599ec32f0a7b8cd33603df1b341d6b8747e4aa11a527a`；cache：
`67eb6b262a42b4ed3b9c00c84b11db33c521014417b098dd6445d50e68fa5995`；
src/tests/scripts seal：
`88733d5d3c3c5db6a62e25a4c3e2bd9b3753ab6e450c45dcfdc3f189df3a7a0a`。
双stamp、全部58个生产receipt、57测试库字节、冻结输入与公共头终态检查0；
不声称重新编译58份。永久54协议脚本亦另跑真实180006参考，日志
`r116-reference54-permanent.log`，在其真实终态0后才进入最终提交核对。

工作区540份Markdown、543Python AST、13Bash语法/本地内联目标及状态/注册
检查0；原273一致性0、require-complete真实1。本项精确14路径，仅Network的
9行tag hunk、一个协议测试/注册和文档；接受537MD/890native/519Python，
注册890+3+519=1412，工作区893+3+522=1418含保留候选。原56933字节补丁重建
后的源码与三个暂缓草稿逐字节核对0，原R98/R99/R109代码未混入本项。

原273仍22complete/166partial/70unverified/15deferred，勾选不提升。
R114目录SQL/NULL视图/来源问题、R117 TIME binary/文本舍入及其余codec仍普通
开放；不转换成用户暂缓。无新完整注册、clean HEAD全量、真实TLS或整库
sanitizer证明。R98/R99/R109候选和R106草稿不混入；不push，Actions禁用。
