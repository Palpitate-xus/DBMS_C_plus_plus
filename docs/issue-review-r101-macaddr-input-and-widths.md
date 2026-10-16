# R101：MACADDR/MACADDR8的输入、宽度转换和错误元数据

状态：本范围修复与定向验证已完成，按单项本地commit保存。第二版20native/
15协议/3真实Main全0；补核的头文件依赖问题修正后，正式重新编译全部58TU，
全部对象和主程序逐字节相同，新匹配核心与最终输入核对全0。未运行新全量。
归属原总账TYPE-10、SQL-08、SQL-11、PROTO-04；不提升原273项状态。
本项是普通数据类型I/O，不是认证、ACL、TDE或用户已暂缓的网络安全专项。
R98/R99的跳过范围仍待用户明确，不丢弃或提交其候选。

后续复查确认本项新增源类型白名单遗漏合法NAME显式输入，NULL也错误42846。
已登记为[R103后续回归](issue-review-r102-network-input-notation.md#后续独立问题未计入本项修复)，
尚未修复；下述有限矩阵通过记录保留，不扩大为所有MAC源类型已经正确。

## 已复现的影响

旧共享MAC解析器删除任意位置的 `:`、`-`、`.`，再仅检查十六进制总长度。
因此混用/重复/前置分隔符或在半字节中间断开也能变成合法地址并成功存储。
另一方面，合法的单数字节、八位类型的六字节EUI-48输入、完整字段前的空白等
被拒绝；MAC48/EUI64显式转换及协议binary六字节输入也不符合参考行为。
八位地址转六位时缺少FF:FE可还原性检查，错误SQLSTATE不正确。
跨MACADDR/MACADDR8运算符在参考中为42725歧义，旧程序却成功返回Boolean。
不可达CASE/零行查询中的非法unknown输入没有按分析阶段检查。

参考为[PostgreSQL 18网络地址类型](https://www.postgresql.org/docs/18/datatype-net-types.html)、
官方[MAC48输入实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/mac.c)
及[MAC64输入与binary接收实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/mac8.c)。
实际参考服务另核对 `server_version_num=180006`，不是仅据文档推断输入是否合法。

## 修改方法

MACADDR按七种布局逐字段解析，保留字段的位置、宽度、分隔符、符号与0x形式，
用无符号有界累加避免signed扫描溢出。MACADDR8逐字节解析一致分隔符，六字节
输入插入FF:FE扩成八字节；实际参考接受的旧式单字符后缀也单独保留阳性控制。
解析API保留原bool调用，并可返回InvalidSyntax/OutOfRange结构化原因；原生存储
调用仍返回DBStatus，不因新错误分类向SDK调用者意外抛异常。

CAST与binder核对真正内建名称及源类型，unknown literal只解析其值，不执行
查询/函数以发现类型。无integer转换在NULL或零行时仍是42846；MAC64缩成MAC48
要求中间FF:FE，否则22003。赋值预检查保留unknown输入和真正typed输入的区别。
同类型比较保留真实MAC字节语义，跨宽度运算符报告42725，不猜测一个比较方向。

协议输入支持MACADDR8的6/8-byte binary形式，返回实际八字节datum。
MAC文本参数的SQLSTATE由解析枚举直接传给Bind错误处理，不从用户消息文本猜码。
网络服务器和TableManage文件同时含有未提交R98差异：本项仅修改MAC相关独立块，
提交时必须按块暂存；预存R98六文件差异另行保存核对，不能整文件混入。

## 旧夹具与协议发送方式核对

原 `macaddr_test.cpp` 将 `08:00:2b: 01:02:03` 和MACADDR8六字节输入视为非法，
均与实际18.6的SQL观测相反。保留输入SQL，纠正为阳性并断言真实规范值，另外
添加参考已拒绝的混用分隔符阴性检查；不是删除失败断言以冒充修复。

初版PG验证在共享binary helper的Parse-only等待ParseComplete处超时；原Root
验证在六字节MACADDR8 Bind处assert中断。这两次都没有完整终态检查数。
永久驱动改为完整P/B/E/Sync周期，错误也能消费到ReadyForQuery，不延长期限、
修改SQL或弱化比较。该发送方式与[Extended Query流](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
相符；保留原helper调用栈，不把PG接收等待称为数据库类型实现失败。

## 当前实际证据

| 运行 | 实际结果 | 范围 |
| --- | --- | --- |
| 固定旧纯头组件 | 75检查/38失败，退出1 | 输入接受、规范字节、长前导零及API宽度/NUL |
| 当前纯头组件O2 | 75检查全部通过，退出0 | 无StorageEngine或数据库初始化 |
| 当前纯头组件ASan/UBSan | 75检查全部通过，退出0 | 无StorageEngine/g_engine/ConnectionPool符号，不是全库sanitizer |
| 派生输入与实际PG18.6逐项比较 | 236组全部一致，退出0 | 分隔符移动、单字段符号/前缀、宽度与溢出；只验证codec，不替代RootSQL |
| 初版完整参考协议 | 284检查全过，退出0 | 发送周期修正后保持全部原SQL/期望 |
| 固定旧程序，同一完整284协议 | 160断言失败，退出1 | 含错误写入后的计数差异，不计160个独立bug |
| 扩展完整参考协议 | 310检查全过，退出0 | 含typed TEXT参数、两向隐式赋值、NULL及同类型运算符 |
| 固定旧程序，同一完整310协议 | 168断言失败，退出1 | 固定R100工作区程序，不混用候选产物 |
| 实际PG18.6最终完整协议 | 328检查全过，退出0 | 新增两个宽度的常量/物理列INSERT SELECT及UPDATE |
| 固定旧程序，同一最终328协议 | 186断言失败，退出1 | 失败中含累计写入/计数后果，不计186个独立bug |
| 第一版正式构建 | 正常/repeat实际退出0，真实编译58个TU | 不是借用旧头对象或其它worktree产物 |
| 第一版核心原生 | 退出1，组件75通过；SDK测试编译失败 | 新增测试缺少直接DbError.h包含，已补头；不冒称SDK入口通过 |
| 第一版核心协议 | 314检查/5失败，退出1 | 仍有typed赋值22023、COALESCE错值和VALUES42804，未接受 |
| 第二版正式构建 | 正常/repeat实际退出0，真实重编四个CPP | 承接首版自身fresh58，不冒称又fresh58 |
| 第二版核心原生 | 两个完整入口实际退出0 | 组件75及原完整MAC存储/SDK CAST/NULL/错误和绑定 |
| 第二版核心协议 | 最终328检查全过，实际退出0 | 参考与固定旧程序相同SQL/断言，不缩减原314控制 |
| 第二版有限完整组合 | 二十native、十五协议、三真实Main全部退出0 | 真实Main包含EXPLAIN、58控制窗口和14控制会话；不是完整1400入口 |
| 新头独立包含补核 | 实际退出1 | 原有TU的先行DbError包含掩盖缺失依赖，不能仅据38运行入口宣告代码已完成 |
| 修复后的头文件独立包含 | 实际退出0，最终再次检查0 | 显式包含common/DbError.h，不依赖调用者包含顺序 |
| 最终正式构建 | 正常/repeat实际退出0，真实重编全部58TU | 头文件新签名，不复用旧签名冒充构建 |
| 更新test cache前的产物比较 | 全部58对象及主程序逐字节相同，退出0 | 与已通过20/15/3的自身第二版运行镜像一致；不是声称在新签名下重跑38入口 |
| 最终新输入匹配核心 | 两个完整native及328协议全部退出0 | 新cache准备含57生产对象字节核对与当期stubs重编；原SDK入口不裁剪 |
| 最终独立终态核对 | 实际退出0 | 新源码/头/manifest、双stamp、58 receipt、57 test对象及程序字节均对应，README/Actions未改 |

首版binary为acdf13ad611b9994bd5d7a6bbd73434559e4b173ab6854bb164994ed3ea3b1db，
cache为b354df152d3132a0e3ab5538b395730fcd7c38014322bf370f908af78aff0b7d，另存
`r101-candidate1-mac-input-main`。首版完整58 receipt及57 test对象字节已在cache
准备时核对0，不代表后续五个真实协议失败已解决。

第二版将MAC双向赋值转换放入真实AST，在INSERT VALUES、直接SELECT投影、UPDATE
和conflict赋值保留实际源类型；转换的列投影使用真实目标OID及原始值绑定，不
用字符串猜宽度，不更改query provider的生命周期。COALESCE仍按原本惰性顺序
执行，仅将选中的MAC值转换到已声明共同类型；VALUES使用既有MAC隐式cast图
选择共同类型并开放真实codec。实际PG目录已确认两个方向castcontext均为i。

第二版Main/binder/ExprEvaluator/ExprHelper四个CPP正式重编和核心验收已完成。
自身全部58 receipt、双stamp、57个test库对象字节在cache准备中核对0。
二十native、十五协议、三真实Main实际全0，独立旧输入终态fence也0。随后独立
头包含检查发现缺少common/DbError.h直接依赖，保留原失败1；补头后独立检查及
最终复查均0。最终正式构建真实重编全部58TU并成功，repeat为up to date。

在更新test cache前，逐一比较新的57生产库对象与已验证第二版test对象、Main
对象与预存第二版Main对象、程序与预存第二版程序：全部逐字节相同，实际0。
因此二十native/十五协议/三真实Main是同一运行镜像的有效证据，但不是在新
cache签名下又跑过全部38入口。随后使用新输入准备匹配cache、重编stubs，
完整重跑两个核心native及328协议，全部0；最终新输入独立fence也0。

最终主程序SHA256为
`fb6b3ad3278314ce8e092f668b3bb5cd46a51c8c5819b862ca38fe9186b36caf`，
最终cache为`1710bd0f4b593d774086bc0938ee08987627a5b146c878deaecea61ffbb4ee2f`。
第二版38入口运行时的旧cache为
`e8530c586d23cc04e3c29534d2e32d1246041165258e2e451edb8326ba067c85`，不改写为新签名。
最终新增头SHA256为
`b02f0f1d076103165698d0de049dab6206db8de23a938aa554ea9c430de31f13`。

固定旧binary SHA256为
`17e6e7f676cccf1493df76368980e13ab06c2704480e5bf59370d9a164d77b70`，旧头为
`1f12e8a45e51ec5e710fc64cdd5b311ba2f4197e98565ca8a38be1d75c3daf4c`。
临时证据在 `/tmp/dbms-review-r46.ifu6SWLp/`：
`r101-old-mac-input-component.log`、`r101-mac-input-component.log`、
`r101-mac-input-sanitizer.log`、`r101-mac-input-reference-probe.log`、
`r101-mac-input-old-probe.log`、`r101-mac-input-reference.log`、
`r101-mac-input-old-protocol.log`、`r101-mac-input-final-reference.log`、
`r101-mac-input-final-old-protocol.log`、`r101-mac-input-expanded-reference.log`、
`r101-mac-input-expanded-old-protocol.log` 和 `r101-fresh58-production-build.log`。
最终参考/旧程序在 `r101-mac-input-final328-reference.log` /
`r101-mac-input-final328-old-protocol.log`；首版失败在 `r101-core-native.log` /
`r101-core-protocol.log`；第二版构建在 `r101-candidate2-production-build.log`。
第二版核心在 `r101-candidate2-core-native.log` / `r101-candidate2-core-protocol.log`，
匹配cache准备为 `r101-candidate2-test-cache-preparation.log`。有限门禁记录为
`r101-full-gate-driver.log`、`r101-full-native.log`、`r101-full-protocols.log`及
`r101-full-protocol-results/`，真实前端分别为 `r101-explain-frontend.log`、
`r101-window-frontend.log` 和 `r101-session-frontend.log`。
旧输入终态为 `r101-candidate2-prior-closure-terminal-fence.log`；独立头失败为
`r101-header-self-contained.log`，修复后为 `r101-header-self-contained-fixed.log`。
新全58构建为 `r101-header-closure-fresh58-build.log`；先于test cache更新的全部
对象/程序比较脚本为 `r101-compare-closure-artifacts.sh`。原Main对象和fb6程序
已分别保存为 `r101-candidate2-main.o`、`r101-candidate2-mac-input-main`。
实际比较结果为 `r101-header-closure-artifact-comparison.log`；最终头独立检查为
`r101-final-header-standalone.log`，新cache准备为
`r101-final-header-test-cache-preparation.log`，新核心为
`r101-final-header-core-native.log` / `r101-final-header-core-protocol.log`，
最终独立核对为 `r101-final-header-terminal-fence.log`。
纯codec额外差分在 `r101-codec-properties-reference.log`，236组全0；探针不链接
StorageEngine，不把这项计为全库/协议sanitizer或完整注册回归。
临时路径不是可移植发行产物；永久复现是
`tests/macaddr_input_codec_test.cpp`、`tests/macaddr_input_protocol_e2e_test.py`
和原 `tests/macaddr_test.cpp` 的typed CAST/存储控制。

当前工作区886 native+3真实Main+511 Python=1400；仅R101提交后的注册集合
884+3+509=1396，两者均无新完整注册终态，不挪用旧1391结果。含未提交R98
的工作区组合不等于只含R101的HEAD整仓验收。本项不宣称完整网络类型/函数族或
原273完成；不push、不启用Actions，README保持长期项目入口。
