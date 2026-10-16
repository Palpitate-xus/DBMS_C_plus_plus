# R103：补回NAME到MAC类型的显式输入

状态：本范围修复与定向验收完成，按单项本地commit保存。实际PG/Root366、
完整SDK、21native/16协议/3真实Main均通过；最终真实全58正常/repeat及核心/
组合后独立fence均0。旧366/30、SDK134及首版358/6、366/12失败全部保留。
没有新完整注册回归，不宣称整个类型家族或原273完成。原TYPE-10、SQL-08、
SQL-11、PROTO-04状态不变；R104/R105后续各独立修复，见文末。

## 问题与修改方法

R101为拒绝无效源类型而新增的MAC CAST白名单遗漏NAME，导致此前合法的
`NULL::name::macaddr`和`'08:00:2b:01:02:03'::name::macaddr8`报42846。
第二个表达式应得到`08:00:2b:ff:fe:01:02:03`。原生ExprValue的name源也被
同一守卫拒绝。这是已复现的回归，不把它改列用户暂缓或当成NAME不受支持。

首版只补canonical name，未识别参数执行中保留的`"pg_catalog"."name"`身份，
所以NAME参数仍报42846。第二版复用纯QualifiedName解析为`builtinIdentity`，
目标`builtinType`仍只接受MACADDR/MACADDR8；源仅按真正内置NAME身份放行，
不会给任意schema或带引号混合大小写的同名类型赋予内置语义。
不更改赋值、公共隐式cast图、MAC解析器、存储或协议布局。NAME显式输入可用，
不等于NAME能直接赋给MAC列；typed NAME及NULL::name的隐式列赋值仍为42804。
BYTEA与INTEGER等缺失显式转换仍为42846，合法源中的非法内容仍按MAC codec
给出22P02或22003。未知字符串的分析阶段处理与动态值的执行顺序保持原样。

## 当前实际证据

| 运行 | 实际结果 | 边界 |
| --- | --- | --- |
| 最初只读Name/网络转换探针 | 参考与旧程序各完整采集10条，8处差异 | 六个NAME→MAC差异归本项，另两个INET→CIDR归R104；采集退出0不是修复通过 |
| 初版加强实际参考协议 | 358检查全过，退出0 | 核实180006，原328控制加30个NAME与拒绝边界控制 |
| 固定旧程序，相同358协议 | 22失败，退出1 | 显式转换、NULL、零行、未选CASE、OID19参数及存储结果 |
| 固定旧原完整SDK | 退出134 | 首个新增合法NAME用例断言失败，未完成全部新增SDK用例，不虚构检查数 |
| 修复后辅助头独立包含 | 退出0 | 直接DbError依赖保留，无包含顺序依赖 |
| 修复后纯MAC组件ASan/UBSan | 75检查全过，退出0 | 无StorageEngine/g_engine/ConnectionPool符号；只证明codec邻居，不证明NAME的整库路径 |
| 首版正式生产构建 | 正常/repeat实际退出0，真实58TU | 不借用旧签名或其它worktree产物 |
| 首版匹配原生 | 两个完整入口实际退出0 | 纯组件及当时完整SDK，typed直接向量为22case，非最终28case |
| 首版匹配358协议 | 6失败，退出1 | OID19参数实际值保留限定NAME，白名单仍误拒绝；未接受 |
| 最终实际参考协议 | 366检查全过，退出0 | 原358控制保留，再加8个限定源身份控制 |
| 固定旧程序，相同最终366协议 | 30失败，退出1 | 固定b172，不是30个独立bug |
| 首版程序，相同最终366协议 | 12失败，退出1 | 保留完整结果，不因普通NAME已过而缩减参数/限定名断言 |
| 第二版辅助头独立包含 | 实际退出0 | 真正源身份解析，不执行函数、参数或查询以发现类型 |
| 第二版正式生产构建 | 正常/repeat退出0，真实全部58TU | 不复用首版签名或对象假冒重编 |
| 第二版匹配原生 | 两个完整入口退出0 | codec及完整SDK，typed向量28case均执行，存储/NULL/错误边界保留 |
| 第二版匹配最终协议 | 366检查全过，退出0 | 与实际参考/固定旧程序相同SQL/断言，包含曾失败的参数和限定源 |
| 第二版核心独立fence | 实际退出0 | 新cache、双stamp、58 receipt、57 test对象字节及最终source/test/script封印对应 |
| 第二版完整邻接回归 | 21native/16协议/3真实Main全0 | 40个不同完整入口，含原完整协议、窗口58控制与会话14控制 |
| 第二版组合后独立fence | 实际退出0 | 程序、cache、双stamp、全部58 receipt、57 test对象字节及source/test/script封印均匹配 |

旧程序为R102提交后的工作区产物，SHA256：
`b172719a60c7f6928ed33596f7fd417879b009b64ec20c81aeb3e28aa8c2c8b8`，另存
`/tmp/dbms-review-r46.ifu6SWLp/r103-pre-fix-dbms-main`。它含仍未提交R98；后续
完整Engine验收也须注明同一范围，不能冒称只含本项的HEAD整仓证明。

首版程序另存`r103-candidate1-name-input-main`，SHA256为
`8d3e7cb039e89aeccbf0d39079baf0d050cd0b734bb158060d8facadef6e8919`，cache为
`2a1660e022c2f353219d06b70786d01b6fa09242bd8fa315ce26ac58a8bad504`；它的核心
fence实际0只证明当时输入/产物对应，不代表六个协议失败已解决。

第二版最终cache为
`e533bb8ad577a8bcb35ba5b0811f356e5062a5fd8b89641056845f09d05a1b53`，
source/test/script封印为
`07a8f105f48d7d5e7d256705ef5722d27475fc0f64a513306a708349ceecc8d4`。
最终辅助头SHA256为
`0ade31a5e6b04dabac6abf7c14beffcbfaf306f4c656a564f00c0b2685093fc7`。
第二版核心程序SHA256为
`892da571ab77fe22848b2082f0571f11adaf277f2964ba07ecfeef4648e68299`。
源码输入、core2native、366协议和完整40入口均实际对应验证。EXPLAIN/会话
same-TU测试驱动为O0，窗口驱动为O2，57个库对象为当期正式O2；真实Main
入口不用test_stubs，也不调用原程序入口。既有编译warnings保留，不宣称零
警告。纯组件sanitizer只覆盖未改变的MAC codec，不冒称NAME整库路径sanitizer。

## 日志、复现与剩余范围

临时证据在`/tmp/dbms-review-r46.ifu6SWLp/`：
`r103-followup-reference.log`、`r103-followup-old.log`、
`r103-expanded-name-reference.log`、`r103-expanded-name-old-protocol.log`、
`r103-old-whole-macaddr-native.log`、`r103-prebuild-header-sanitizer-and-seal.log`
和`r103-fresh58-production-build.log`。永久复现为原完整`tests/macaddr_test.cpp`
及`tests/macaddr_input_protocol_e2e_test.py`；后者支持`--reference18`并核实服务
版本，使用连接自己的TEMP表。最终SDK保留原12case，加10个NAME与6个限定/
伪身份控制，typed向量共28case；不把向量数当成完整SDK入口全部断言数。
扩展记录为`r103-final366-reference.log`、`r103-final366-old-protocol.log`、
`r103-candidate1-final366-protocol.log`、`r103-candidate-core-native.log`、
`r103-candidate-core-protocol.log`、`r103-candidate-core-terminal-fence.log`、
`r103-candidate2-frozen-inputs.log`与`r103-candidate2-fresh58-production-build.log`。
第二版核心为`r103-candidate2-test-cache-preparation.log`、
`r103-candidate2-core-native.log`、`r103-candidate2-core-protocol.log`及
`r103-candidate2-core-terminal-fence.log`；完整组合记录为
`r103-full-gate-driver.log`、`r103-full-native.log`、`r103-full-protocols.log`及
`r103-full-protocol-results/`。真实Main为`r103-explain-frontend.log`、
`r103-window-frontend.log`及`r103-session-frontend.log`；最终独立核对为
`r103-candidate2-final-terminal-fence.log`，全部已实际终态。

注册数量没有增加：仅本项提交后仍为885native+3真实Main+510Python=1398，
含R98候选的工作区为887+3+512=1402；测试内容变化，旧1391及R102的完整
定向组合不能直接充当本项验收。没有新完整注册回归、真实TLS或全库sanitizer。
R104的typed INET→CIDR等值转换后续已独立修复；R98/R99跳过范围仍待明确，
保持候选但不提交；其它明确暂缓范围不恢复，不push、不启用Actions。

构建期间另作R104只读预检，参考与固定b172各完整采集23条，记录到
[R102后续观察](issue-review-r102-network-input-notation.md)：已确认的网络
COALESCE错值、混合VALUES及typed赋值也属于R104后续值转换范围，未在本项
候选中修改，不把采集退出0当作修复通过。

另以实际180006与固定首版8d3e程序各采集四个NAME→INET/CIDR表达式，确认
网络输入辅助头也遗漏限定NAME：普通name源成功，`"pg_catalog"."name"`源
错误42846，参考两者都成功。单列[R105](issue-review-r105-network-name-input.md)
已独立修复：PG/Root738、自身全58及40个邻接入口与最终核对均0，不当作
R103本项修复。原四观察日志为
`r105-qualified-name-network-reference.log`和`r105-qualified-name-network-old.log`。
本项仅修改MAC消费者，不把网络消费者的相同遗漏一并写成已修复；后续仍须
保持精确内置身份、非法源拒绝和隐式赋值边界，不能简单剥离任意schema。

后续[R104网络值转换](issue-review-r104-network-value-conversions.md)已独立
修复：PG/Root649、原738、完整SDK及41邻接/最终核对均0；
其值转换和分析边界不计入R103的历史40入口证明。
