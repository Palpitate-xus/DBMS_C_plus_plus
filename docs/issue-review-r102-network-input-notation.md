# R102：INET/CIDR合法输入与类型边界

状态：本范围修复与定向验收完成，按单项本地commit保存。最终188组件、
ASan/UBSan、PG/Root672、2558组codec对照及21native/16协议/3真实Main均通过。
最终自身58TU正常/repeat构建与独立终态核对均0；首版672/23失败保留，不冒新全量。
原273项TYPE-10、SQL-08、SQL-11、PROTO-04状态不变。本项是普通数据类型输入，
不涉及认证、ACL、TDE或暂缓的安全专项。
R98/R99的跳过范围仍待明确，保留其工作区候选且不混入本项提交。

## 影响与修改方法

旧解析器用libc inet_pton处理所有地址，拒绝PostgreSQL接受的IPv4前导零、
带显式掩码的INET缩写、CIDR无掩码缩写及packed十六进制网络表示。例如
`10/8`应为`10.0.0.0/8`，旧程序的CAST、参数与存储输入却失败。反过来，
IPv6掩码前导零被错误接受；原生字符串API中的嵌入NUL会被C接口截断后当作
完整地址。SDK与SQL协议是分别验证的路径，不混作同一观测。

IPv4逐个十进制octet有界累加，前导零仍是十进制；拒绝非法分隔、空段、CIDR尾点、
第五个octet及超范围值。INET无掩码仍要求四个octet，显式掩码不得缺少所需
完整octet。CIDR按参考推断classful掩码，保留224的历史/4特例及十六进制奇数
nibble的低半字节补零；缺省地址字节为零，始终检查host bits。IPv6仅补掩码
前导零限制，所有字符串先拒绝嵌入NUL。地址存储布局、wire family标签、
格式化与网络运算符不改。补核确认INET另有历史尾点形式，首版仍误拒绝七个
合法输入；第二版保留这些形式，同时继续按INET的octet/掩码规则拒绝缺段，
不把CIDR尾点或重复点号放宽为合法。

扩展合法文本不能让整数10变成合法CIDR CAST或typed列赋值。新增独立输入
辅助头，按真正内建身份和实际源类型验证：显式CAST允许unknown、TEXT/
VARCHAR/BPCHAR/NAME、INET及CIDR；缺失转换即使NULL也报42846。列赋值只
允许unknown或真实网络类型，typed TEXT/整数等报42804。只在分析阶段解析
真正的unknown字符串常量，不执行函数、参数或child query。因此非法unknown
在零行查询和不可达CASE中也报22P02。SDK仍用bool/DBStatus，不新增DbError。

参考为[PostgreSQL18网络类型](https://www.postgresql.org/docs/18/datatype-net-types.html)
及官方18.6[输入实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/inet_net_pton.c)。
实际连接先验证server_version_num=180006；源类型集合另有实际SQL控制，
`pg_cast`确认CIDR→INET为i、INET→CIDR为a。本项没有据此声称typed INET→CIDR
的host-bit清除已修复；该值转换还需独立核对，不扩成整个网络类型家族完成。

## 实际证据

| 运行 | 实际结果 | 边界 |
| --- | --- | --- |
| 最初只读参考探针 | 70观察/27差异，采集退出0 | 不是修复通过，也不是27个独立bug |
| 扩展只读探针 | 152观察/59差异，采集退出0 | 含重复输入，初版矩阵75输入/150类型组合 |
| 固定旧纯头组件 | 158检查/65失败，退出1 | 150组合及6个NUL/2个长输入控制；无Engine初始化 |
| 首版初始纯头组件O2 | 158检查全过，退出0 | 不覆盖后来补入的尾点控制 |
| 首版新头独立包含、组件ASan/UBSan | 全部退出0，158检查全过 | 无StorageEngine/g_engine/ConnectionPool符号，不是全库sanitizer |
| 初版实际参考协议 | 468检查全过，退出0 | CAST、typed参数、存储、UPDATE保留值及真实binary输出 |
| 初版固定旧协议 | 退出1，未完整采集 | 实际Bind错误缺少RowDescription，collector直接assert中断；原日志保留 |
| 修正采集后的实际参考协议 | 468检查全过，退出0 | 缺失描述作为原严格断言失败计数，不改SQL/期望 |
| 同一完整旧协议 | 468检查/259失败，退出1 | 含累计坏写入及后续读值后果，不是259个独立bug |
| 加强实际参考协议 | 582检查全过，退出0 | NULL/非法CAST、typed赋值、零行/不可达CASE及合格类型名 |
| 同一加强旧协议 | 582检查/345失败，退出1 | 固定旧binary，不混入当前候选 |
| INET尾点补核 | 30组合/7差异，采集退出0 | 首版仍误拒绝参考接受的七个输入，未据初始158绿宣告完成 |
| 固定旧最终组件 | 188检查/72失败，退出1 | 增加15输入/30类型组合，全部原断言保留 |
| 首版最终组件 | 188检查/7失败，退出1 | 仅尾点阳性仍失败，保留原结果 |
| 最终实际参考协议 | 672检查全过，退出0 | 原582控制加尾点的合法/非法及存储计数控制 |
| 同一最终旧协议 | 672检查/389失败，退出1 | 含旧存储22023及累计后果，不计389个独立bug |
| 首版正式构建 | 正常/repeat退出0，真实编译全部58TU | 新源码自有对象，不借用其它worktree |
| 首版匹配SDK原生 | 原完整inet_test退出0 | 不缩减其存储/UPDATE/运算符/OID断言 |
| 首版匹配最终协议 | 672检查/23失败，退出1 | 七个输入各三路径失败加两处存储结果后果，未接受 |
| 第二版最终组件O2与ASan/UBSan | 188检查全部通过，均退出0 | 无Engine符号，完整旧组件矩阵未裁剪 |
| 第二版派生输入对照实际PG18.6 | 2558组全一致，退出0 | class边界、octet数量、非整字节掩码、尾点、packed hex、IPv6限制；不替代RootSQL |
| 第二版辅助头独立包含 | 退出0 | 使用最终头，不依赖包含顺序 |
| 第二版正式构建 | 正常/repeat退出0，真实重编全部58TU | 最终头与自身生产对象，不复用首版签名 |
| 第二版匹配核心原生 | 两个完整入口退出0 | 188组件及原完整inet SDK，均通过owned隔离运行 |
| 第二版匹配核心协议 | 672检查全过，退出0 | 与实际参考/固定旧程序相同最终SQL及断言 |
| 第二版完整邻接组合 | 21native、16协议、3真实Main全部退出0 | 40个不同完整入口，含原完整协议、58控制窗口及14控制会话 |
| 第二版独立终态核对 | 核心后及完整组合后均实际退出0 | binary/cache、双stamp、58 receipt、57 test对象字节及source/test/script封印一致 |

固定旧程序来自7533f27e提交后的工作区，含未提交R98，SHA256为
`fb6b3ad3278314ce8e092f668b3bb5cd46a51c8c5819b862ca38fe9186b36caf`，另存
`/tmp/dbms-review-r46.ifu6SWLp/r102-pre-fix-dbms-main`。旧共享头SHA256为
`8d3b2bd4295e52c85dbf8f13c3586bffac4885004d03afb3fb01351e742ec43c`。
纯头证明独立于R98；完整Engine证明也须注明当前工作区含R98，不冒称仅本项
HEAD整仓验收。未提升原273状态，未push或启用Actions。

首版程序另存`r102-candidate1-network-input-main`，SHA256为
`145419e6f7af0505f01e65070057d020387e08603beeebe85fbbbfbd92be4ed0`，cache为
`d31d03d760cac61b7359c57ceea78614cab8c8c8deba96f41abed8af71ccebe3`。
第二版最终程序SHA256为
`b172719a60c7f6928ed33596f7fd417879b009b64ec20c81aeb3e28aa8c2c8b8`，cache为
`21a81d6e0f2277c2d7d2b1450e85e5357242f61f123be99033faefcb81e1d281`，最终
source/test/script封印为
`5553656d6e7a0d167cf2368bb24573a18def67e7dba0798a653dc810391d53f9`。
两次全58均实际完成，最终cache准备重新编译stubs，并验证自身58 receipt及
57生产test对象字节。完整组合前后均核对输入/程序未变化，不改写首版签名或
失败为第二版通过。EXPLAIN/会话same-TU测试驱动为O0，窗口驱动为O2，其57个
库对象均为当期正式O2；不用test_stubs，也不调用原程序入口。既有warnings
保留在日志，不宣称全库sanitizer或零警告。

## 后续独立问题，未计入本项修复

只读复查另以实际18.6与固定fb6旧程序执行相同10条SQL，完整采集均退出0，
有8条结果不同；采集成功不代表数据库行为正确。日志为
`r103-followup-reference.log`与`r103-followup-old.log`，同在上述证据目录。

| 编号 | 发现行为及后续状态 | 实际参考及修改方向 |
| --- | --- | --- |
| R103 | R101新增白名单遗漏普通/限定NAME，包括NULL；后续已修复 | [R103](issue-review-r103-macaddr-name-input.md)独立补真实源身份，PG/Root366与40个定向入口通过，保留INTEGER/内部char拒绝，不回改旧记录 |
| R104 | typed INET→CIDR把host bits当作非法文本拒绝 | `192.0.2.1/24`应转为`192.0.2.0/24`，IPv6同理；应区分已有网络datum转换与unknown CIDR输入，前者清除host bits，后者仍严格拒绝非网络地址 |
| R105 | 网络消费者遗漏限定NAME，普通NAME成功但参数/限定源被拒；后续已修复 | [R105](issue-review-r105-network-name-input.md)独立补真实源身份，PG/Root738、自身真实全58及40个邻接入口/最终核对均0；不回改旧证据或放宽隐式赋值 |

R103是复查发现的真实回归，R104是既有值转换遗漏；均未因R102格式修复而关闭。
R103现由其后续独立修复与验收关闭，R104仍开放；限定NAME到INET/CIDR的
同类消费者遗漏另列R105，经独立修复与验收关闭，不由MAC修复覆盖。

R104后续只读预检在实际180006与固定b172程序各完整采集23条，日志为
`r104-conversion-preflight-reference.log`和`r104-conversion-preflight-old-b172.log`。
typed INET→CIDR的值转换、INSERT VALUES/SELECT及UPDATE均有差异；此外
`COALESCE(NULL::inet,'192.0.2.129/32'::cidr)`在旧程序中声明INET却仍输出带/32
的CIDR文本，混合CIDR/INET的VALUES报42804。相同CASE和显式CIDR→INET是
已通过的阳性控制，unknown/TEXT CIDR的非法host bits仍应22P02。R104应修复
实际datum转换及已确认的共同值场景，不只更改结果OID或放松CIDR文本校验；
既有共同类型图已含CIDR→INET隐式边，不能误加INET→CIDR隐式边。

## 日志与永久复现

临时证据在`/tmp/dbms-review-r46.ifu6SWLp/`：
`r102-inet-input-reference-probe.log`、`r102-inet-input-expanded-reference-probe.log`、
`r102-inet-input-expanded-reference-json.log`、`r102-network-source-reference.log`、
`r102-old-network-input-codec.log`、`r102-current-network-input-codec.log`、
`r102-header-and-component-sanitizer.log`、`r102-input-final-reference.log`、
`r102-input-old-protocol.log`、`r102-input-complete468-reference.log`、
`r102-input-complete468-old-protocol.log`、`r102-input-expanded-guards-reference.log`、
`r102-input-expanded-guards-old.log`及`r102-fresh58-production-build.log`。
尾点与扩展证据另在`r102-legacy-tail-reference.log`、
`r102-old-network-input-codec188.log`、`r102-candidate1-network-input-codec188.log`、
`r102-final672-reference.log`、`r102-final672-old-protocol.log`、
`r102-candidate1-test-cache-preparation.log`、`r102-candidate1-inet-native.log`、
`r102-candidate1-final672-protocol.log`、`r102-final-codec-and-sanitizer188.log`及
`r102-final-codec-property-reference.log`。第二版输入/构建记录为
`r102-candidate2-frozen-inputs.log`、`r102-candidate2-fresh58-production-build.log`。
最终核心在`r102-candidate2-test-cache-preparation.log`、
`r102-candidate2-core-native.log`、`r102-candidate2-core-protocol.log`和
`r102-candidate2-core-terminal-fence.log`。完整组合在`r102-full-gate-driver.log`、
`r102-full-native.log`、`r102-full-protocols.log`及`r102-full-protocol-results/`；
真实Main分别为`r102-explain-frontend.log`、`r102-window-frontend.log`、
`r102-session-frontend.log`，独立终态为`r102-candidate2-final-terminal-fence.log`。
临时路径不是发行依赖。永久入口为`tests/network_address_input_codec_test.cpp`
与`tests/inet_input_protocol_e2e_test.py`；后者默认测本项目，`--reference18`
只连接预先配置且核实180006的参考服务，使用连接自己的TEMP表。

本项提交后注册集合885native+3真实Main+510Python=1398；工作区含未提交
R98时为887+3+512=1402。两者无新完整注册终态，不借旧1391或R101的38入口
证明本项。仅提交本项22个文件，registry与文档inventory按块隔离，不混入
R98/R99；四份用法/历史对照文档补充当前范围，不重写历史PASS。原273未完成，
R103/R105后续已独立修复，R104仍待修复；本项不是整个网络类型家族或仅
本项HEAD整仓验收。
