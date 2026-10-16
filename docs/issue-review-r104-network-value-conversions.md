# R104：INET/CIDR的实际值转换、赋值与共同结果

状态：本范围修复和定向验收完成，按本项独立本地commit保存。第三版正常/
repeat、完整SDK/649/738、21native/17协议/3真实Main及核心/最终独立核对
均实际0；无新完整注册回归，不提升原273状态。前两版失败保留。R105已独立
提交为`b6595d2e`；本项不回改既有通过或
失败，不提升原273状态。R98/R99候选保持，不混入提交。

## 问题与修改方法

| 问题 | 影响 | 修改方法与边界 |
| --- | --- | --- |
| typed INET→CIDR错误使用严格文本输入 | `192.0.2.129/24`报22P02，参考应转为`192.0.2.0/24`；IPv6及非整字节/零掩码也受影响 | 真正INET datum先按INET解析，再清除host bits；UNKNOWN/TEXT/NAME的CIDR文本仍严格校验，不放松公共parser |
| 接受赋值类型但未执行值转换 | VALUES、直接/物理列INSERT SELECT、UPDATE/UPDATE FROM失败22023，ON CONFLICT失败XX000 | 插入真正CastExpr并保持投影provenance；冲突窄路径在丢失ExprValue类型之前转换实际值，不再次执行原表达式，不改变child-provider生命周期 |
| 冲突SET项缺失网络输入预检查 | 非法typed NULL或UNKNOWN在取到/未取到冲突时都可能漏报，甚至留下本不该插入的新行 | 对真正网络目标执行既有纯赋值回调；拒绝typed TEXT/NAME/INTEGER，严格校验UNKNOWN CIDR常量，与值转换分开 |
| COALESCE类型与实际值不一致 | 声明INET却仍带CIDR的`/32`；全NULL结果取第一个CIDR为650而非869，旧标量快捷路径还漏掉未选非法UNKNOWN常量 | 网络共同类型用现有选择器；绑定时作实际CAST与纯常量输入校验，旧快捷路径补仅UNKNOWN字符串常量预检查；保持函数/参数/子查询短路，不重复执行 |
| 混合网络VALUES被拒 | CIDR/INET共同VALUES报42804，还有网络目标白名单缺口 | 复用既有selectCommonType并支持已有INET/CIDR目标，实际转换每个值，不只替换OID |

现图已有CIDR→INET隐式边，INET→CIDR只有assignment/explicit方向；原生测试
明确断言不得新增反向隐式边。typed TEXT/NAME仍不可直接隐式赋给网络列。
当前累计只改`ExprEvaluator.cpp`、`query_binding.cpp`、`expr_helper.cpp`、
`DmlExecutor.cpp`与`main.cpp`五个生产CPP，不改header、网络codec、binary
布局、存储或provider所有权协议。三版分别实际增量重编4/3/2个CPP，normal/
repeat均0；公共编译签名不变，保留并核对全部58receipt，不冒称全58重编。

## 实际证据与验收范围

| 运行 | 实际结果 | 范围 |
| --- | --- | --- |
| 初始PG/固定b172观察 | 各完整采集23条 | 原记录见R102；采集退出0不代表行为通过 |
| 全掩码/共同值参考 | 490控制、0失败，实际退出0 | 实际180006；IPv4全部33个与IPv6全部129个掩码各有CAST/COALESCE/VALUES，保留NULL/CASE阳性 |
| 固定当前旧907，相同490 | 324失败，实际退出1 | 完整矩阵，不缩减失败控制 |
| DML实际参考 | 64控制、0失败，实际退出0 | 两个网络方向、IPv4/IPv6、RETURNING、实际完整行值、TEXT/零行/坏后续行原子性 |
| 固定当前旧907，相同64 | 14失败，实际退出1 | 六种INET→CIDR赋值及最终行值受影响；反向阳性不删除 |
| 永久加强协议实际参考 | 582控制、0失败，实际退出0 | 保留490+64，加入全NULL共同结果及text/binary参数和完整binary结果字节/OID |
| 固定当前旧907，相同582 | 363失败，实际退出1 | 全部执行，不是363个独立bug；不改变SQL/断言/等待期限 |
| 固定旧完整INET入口 | 实际退出134 | 保留R105的15case，再加17case及COALESCE/图断言；首个新增INET→CIDR断言失败，未执行全部新增控制 |
| 候选1正式生产构建 | normal/repeat均实际退出0，实际重编四CPP | 保留全部58receipt；不是全部58TU重编 |
| 候选1匹配核心 | native134、582/3实际1、原738实际0、artifact核对0 | 32case后在全NULL COALESCE类型断言失败；另有两处ON CONFLICT漏值转换，未接受；核对0不表示行为通过 |
| 扩展最终参考 | 649控制、0失败，实际退出0 | 保留582并加39个COALESCE边界、EXCLUDED值/NULL及取到/未取到冲突的拒绝和完整行值控制 |
| 固定旧907，相同最终649 | 419失败，实际退出1 | 原582/363保留，不用较小矩阵替代最终比较 |
| 首版fa553，相同最终649 | 58失败，实际退出1 | 保留首版582/3失败，不扩为首版通过 |
| 第二版正式构建/核心 | 正常/repeat实际0，重编三CPP；2native/原738实际0，649/1实际1，核对0 | 32case、5个COALESCE、惰性回调与图断言完成；唯一剩余为旧标量快捷路径未选UNKNOWN输入，未接受 |
| 第三版正式构建/匹配核心 | 正常/repeat实际0，重编二CPP；2native、649、原738及核对均0 | 相同最终SQL/断言未缩减；保留NAME/严格CIDR输入、实际存储/NULL/OID/binary字节和分析边界 |
| 完整邻接 | 21native/17协议/3真实Main全部实际退出0 | 41个不同完整入口；含原完整default协议，不缩减SQL/断言/等待期限 |
| 组合后独立核对 | 实际退出0 | 当前程序/cache/双stamp/全部58receipt/57test对象字节/冻结source-test-script封印及未改headers对应 |

旧程序：`/tmp/dbms-review-r46.ifu6SWLp/r104-pre-fix-dbms-main`，SHA256为
`9072ec99197f2eac74624ac848b8df534e230fef30a5caa4827b71da6ffbfaf9`。
它及后续完整Engine证据含仍未提交R98，不冒称仅含本项HEAD的全仓证明。
候选1cache为`a89be717c55ce0b25c2f623b58666e8913507aa47ccfa6d990bc2c056c9e6390`；
source/test/script封印为
`47c6abd4bf81d3234b0004144e9d168bdf4c2f0d2742c59638e1035dc4f13dac`。
候选1程序SHA256为
`fa5537aa26b6f0979dd1a5510c814e7bfef4edde5bfc732b460b50c0f7d238dc`，
另存`/tmp/dbms-review-r46.ifu6SWLp/r104-candidate1-dbms-main`。
候选失败须保留日志、按新输入重编，不借用上一项组合结果。

第二版cache为`8df953de7f355526b01e035b4ec481788bbb330d0f096324cbc6340baccf6ecb`，
封印为`afbdc7aaaf420852c58031563d93939aaf3de4052a978a78698575a914297947`，
程序为`71d1acc003ad1a784ecd891453b0bb463b249c19f6340b2525ea47a8b67d120d`，
另存`r104-candidate2-dbms-main`。它的649/1失败不可被artifact核对0掩盖。
第三版cache为`3d83fa126a9880dbed89cc36d837900ef28f00419171f34f4c2b00a765c2bfe8`，
封印为`3f058398266846005f82f21f85205c7dd07a4488e9832dfc2b6b81983098fa49`，
程序为`d6970b8546dded1cbdac5dedfcfbd3a7dc8e2d8514e1aeba0d607a8400ae5519`。
构建/核心与组合期间source/test/script冻结，只更新文档。EXPLAIN与会话
same-TU驱动为O0，窗口驱动为O2，57个库对象为当前正式O2；真实Main入口
不用test_stubs或原程序入口。既有warnings保留，不宣称零警告或全库sanitizer。

## 永久复现与日志

`tests/inet_test.cpp`保留原完整SDK，CAST向量共32case，并单独检查五个COALESCE
值/NULL类型、未选原生回调调用数0和共同类型图。新增协议入口
`tests/network_value_conversion_protocol_e2e_test.py`已注册一次；默认测隔离
的本项目程序，`--reference18`核实参考版本，使用连接自己的TEMP表。

证据目录为`/tmp/dbms-review-r46.ifu6SWLp/`：
`r104-prefix-final-reference.log`、`r104-prefix-old907.log`、
`r104-dml-final-reference.log`、`r104-dml-old907.log`、
`r104-permanent-reference.log`、`r104-permanent-old907-protocol.log`、
`r104-old-whole-inet-native.log`与`r104-candidate1-frozen-inputs.log`。
构建为`r104-normal-production-build.log`和`r104-repeat-production-build.log`；
后续只登记真实终态，不覆盖原失败。最终参考/旧程序/首版的649矩阵为
`r104-final649-reference.log`、`r104-final649-old907.log`、
`r104-candidate1-final649.log`；中间633及纯39参考也保留。
三版各有独立`r104-candidateN-*-production-build.log`、
`r104-candidateN-core-driver.log`、`core-native.log`、`core-conversions.log`、
`core-input.log`、`core-terminal-fence.log`和test-cache准备日志；N=1的正式
构建沿用`r104-normal-production-build.log`，三版均按实际结果读取。

第三版完整组合在`r104-full-gate-driver.log`、`r104-full-native.log`、
`r104-full-protocols.log`和`r104-full-protocol-results/`；真实Main为
`r104-explain-frontend.log`、`r104-window-frontend.log`、`r104-session-frontend.log`。
最终独立核对为`r104-final-terminal-fence.log`，全部已实际终态。只提交本项
24个文件，Main/registry/inventory按块隔离；R98/R99及原273条目不混入或提升。

预计单项提交后885native+3真实Main+511Python=1399；含R98工作区为887+3+513
=1403。无新完整注册回归、全库sanitizer或真实TLS。原273仍为22complete、
166partial、70unverified、15deferred_by_user；明确暂缓项不恢复，Actions禁用，不push。
