# R105：限定NAME到INET/CIDR的显式输入

状态：本范围修复与定向验收完成，按单项独立本地commit保存。自身全部58TU
生产重编及normal/repeat、匹配核心2native/738协议、21native/16协议/3真实
Main组合及核心/最终独立核对均实际0。无新完整注册回归，不宣称整族或原273
完成。原失败保留；R104仍开放，R98/R99候选不混入提交。
实际PostgreSQL 18.6的最终738控制全过；相同738在固定旧程序中34失败，
旧完整SDK在首个限定NAME控制处退出134。原273项状态不提升；R104仍开放。

## 问题与修改方法

R102新增网络CAST源类型白名单只接受普通`name`拼写，遗漏真正的
`pg_catalog.name`及`"pg_catalog"."name"`。例如
`'192.0.2.129/24'::"pg_catalog"."name"::inet`应保持地址与掩码，旧程序却报
42846。OID19文本参数的执行值也保留限定NAME身份，所以不是单纯的SQL显示
问题；合法CIDR、IPv6参数和非法内容的SQLSTATE均受到影响。

修复复用本辅助头的纯QualifiedName解析，分离`builtinIdentity`和只允许
INET/CIDR目标的`builtinType`。源只按真正的内置NAME身份放行：无schema或
精确`pg_catalog`，保持SQL引号大小写语义；不剥离任意schema或接受`"Name"`。
不改变公共cast图、隐式赋值、网络codec、存储和协议布局，也不执行表达式来
发现类型。NAME显式CAST成功不等于typed NAME可隐式赋给网络列，后者及
NULL::name仍应42804；BYTEA/INTEGER缺失显式转换仍为42846；合法NAME中的
非法地址仍为22P02，CIDR文本/NAME输入的非零host bits仍严格拒绝。

## 实际证据与验收门槛

| 运行 | 实际结果 | 范围 |
| --- | --- | --- |
| 初始只读探针 | 实际180006及旧8d3e各采集4条，2处差异 | 采集退出0只表示记录成功，不表示修复通过 |
| 最终实际参考协议 | 738检查、0失败，退出0 | 保留原672，新增66个限定NAME/OID19/赋值边界与实际存储控制 |
| 固定旧程序，同一最终协议 | 738检查、34失败，退出1 | 参数、限定身份、错误状态及被拒INSERT后的结果；不是34个独立bug |
| 固定旧完整SDK | 实际退出134 | 原有完整入口保留，新增向量前两项普通NAME成功，首个限定NAME断言失败；未执行全部新增15case |
| 修复后辅助头独立包含 | 实际退出0 | 直接包含、使用正式生产编译选项，不依赖偶然包含顺序 |
| 修复后纯网络组件 | O2与ASan/UBSan各188全过，退出0 | 运行前确认没有Engine/g_engine/ConnectionPool符号，只是未改变codec的邻接证据 |
| 自有全部58TU生产重编 | normal/repeat实际退出0，58编译器均实际0 | 标准O2生产选项、全58独立receipt，不借旧签名或其它worktree对象 |
| 匹配核心 | 两个原完整native入口及相同738协议全部退出0 | 新增15case向量全部执行；原存储/更新/比较及738原断言保留 |
| 核心独立校验 | 实际退出0 | 当前程序、cache/双stamp、58receipt、57test对象字节及冻结输入封印 |
| 完整邻接组合 | 21native/16协议/3真实Main全部实际退出0 | 40个不同完整入口；包含原完整default协议，不缩减失败控制或等待期限 |
| 组合后独立校验 | 实际退出0 | 当前程序、cache/双stamp、58receipt、57test对象字节及冻结输入封印 |

旧程序为R103提交后的工作区产物，固定在
`/tmp/dbms-review-r46.ifu6SWLp/r105-pre-fix-dbms-main`，SHA256为
`892da571ab77fe22848b2082f0571f11adaf277f2964ba07ecfeef4648e68299`。
含仍未提交R98；后续完整Engine验收也包括该工作区，不能称为只含本项的
HEAD整仓验收。R98/R99候选保留、不提交，跳过范围仍待用户明确。

本次冻结cache为
`525258ef0db554c1c7064980ca989d3836370c72ed3a08d65923e903d64e3276`，
source/test/script封印为
`fcbbc4bf2dc9117130c01c01c2c7e8c85da9dcd03b390771ebd4d4052232293d`。
网络辅助头SHA256为
`f66ef8c61d3d998ed6865a9177bb7cbd749cb947ac3b0aca61d6bf74375f750c`。
本次实际生产程序SHA256为
`9072ec99197f2eac74624ac848b8df534e230fef30a5caa4827b71da6ffbfaf9`。
输入冻结后只能修改文档，不能借用R103旧cache/产物或组合结果验收本项。

## 复现入口、日志及剩余范围

永久入口为`tests/inet_test.cpp`与`tests/inet_input_protocol_e2e_test.py`。
前者保留全部存储/更新/比较测试，增加15case真实参数ExprValue转换向量；
其中其他schema及混合引号伪身份只是SDK负控制，不冒充不存在类型的SQL参考。
后者保留672个原控制并加66项，默认测试本项目，`--reference18`只连接已配置
且核实版本的实际参考，使用连接自己的TEMP表。新控制包含普通/限定NAME、
NULL、零行、未选CASE、合法/非法内容、OID19参数、binary结果的完整字节/OID、
显式INSERT后读回及隐式赋值失败后行数，未放宽SQL/断言/等待期限。

临时证据目录为`/tmp/dbms-review-r46.ifu6SWLp/`，不是发行依赖：
`r105-qualified-name-network-reference.log`、`r105-qualified-name-network-old.log`、
`r105-name-final-reference.log`、`r105-name-old-protocol.log`、
`r105-name-old-whole-native.log`、`r105-prebuild-header-sanitizer-and-seal.log`及
`r105-fresh58-production-build.log`。当前匹配核心记录为
`r105-test-cache-preparation.log`、`r105-core-native.log`、`r105-core-protocol.log`
及`r105-core-terminal-fence.log`，均已实际终态0。完整组合为
`r105-full-gate-driver.log`、`r105-full-native.log`、`r105-full-protocols.log`及
`r105-full-protocol-results/`；真实Main为`r105-explain-frontend.log`、
`r105-window-frontend.log`、`r105-session-frontend.log`，最终独立核对为
`r105-final-terminal-fence.log`，全部已实际终态。EXPLAIN/会话same-TU驱动为
O0，窗口驱动为O2，57库对象均为当前正式O2；不用test_stubs或原程序入口。
既有编译warnings保留于日志及`/tmp/dbms-r105-fresh58-units.C9LVfWTE/`，不宣称
零警告。纯组件sanitizer只证明未改变的codec邻居，不证明NAME整库路径。

只扩展已有测试，注册数量仍为885native+3真实Main+510Python=1398；工作区
含R98候选时为887+3+512=1402。没有新完整注册回归、全库sanitizer或真实TLS。
R104的typed INET→CIDR host-bit清除、实际赋值和COALESCE/VALUES值转换仍未
修复；本项不能放松CIDR文本校验来掩盖它。原273仍为22complete、166partial、
70unverified、15deferred_by_user。既有明确暂缓项不恢复，不push、不启用Actions。
