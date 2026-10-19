# 2026-10-13 全仓库代码审查

## 当前接受状态：R170/R171/R172/R174/R175

五项有限修复已分别本地提交，版本对应见[审查索引](review-index.md)。
最终组合在工作区及accepted-only clean各完整70入口（34native、33wire、3真实Main）实际全0；
完整791/314/331/906及原956均0，已接受源2195文件与实际测试clean逐字节对应。
首次Root70实际1的清理超时仍保留、原因未确定，复测0不等于超时根因已修；
源码、58生产对象、server及每端96对象/38程序均独立核对，详见[R170最终证据](issue-review-r170-quoted-unqualified-scalar-projection.md)。
已接受596MD、916native+563已注册Python+3Main=1482；工作区599MD、922+569+3=1494，
原12个未接受入口和三份文档保持分离。这不是1482全量、TLS/整库sanitizer或各中间提交独立全矩阵。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成。
R173函数签名类型依赖及R176真实聚合排序仍独立开放；R156等用户暂缓模块不恢复，不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

[R157 普通SET点号目标](issue-review-r157-dotted-set-target.md)完成独立有限修复：
完整目标先于内建分派，LOCAL/SESSION/transaction相似前缀不改变身份；
引用分量保留原拼写，ColId分类、缺失分量、分隔符与FROM CURRENT边界均验证。
PG1538全0；修复前两端各1538/226失败，native Root3075/474、clean3075/3072失败。
修复后两端3075/1538全0，Root52及accepted-only clean37完整入口、严格输入/归属复核全0。
原search_path_values_grammar夹具未改，两端完整跑到254；R152的5197/1675也重新全过，
R152历史clean30终态1及旧R155断言134仍保留，不把旧输入失败改写成通过。
仅一个Parser CPP：Root正常1O2/repeat0，clean fresh1+57已核对R152生产对象；
六Main源/全头flags/对象核对后复用R152三个真实Main对象，新链接当前57库并隔离运行，
不冒fresh TU。未实现全部通用SET值语法或自定义GUC存储，R140/R151等暂缓项不重开。
[R156 REGTYPE物理常量](issue-review-r156-regtype-physical-constants.md)与R150仍开放；
已接受577MD、909native+548Python+3Main=1460；工作区580MD、915+554+3=1472，
仍有12个未接受入口，无新1460全量/TLS/整库sanitizer。
原273仍22/166/70/15，总清单未完成，不push、Actions禁用。

[R152 REGTYPE 最小引用](issue-review-r152-regtype-identifier-output.md)完成独立显示修复：
精确查找串与显示串分离，quote_ident/format %I/路径列表共用既有 ASCII、关键字引用规则。
PG1675全0；修复前两端各1675/66失败、native5197/1718失败；修复后两端5197/1675全0。
Root45完整相邻入口全0；accepted-only clean30完成29通过、1个旧搜索路径AST断言134，
整批终态1，不称全绿。冻结R155旧版同一未改夹具也在第86行退出134，
另列[R157 普通SET目标](issue-review-r157-dotted-set-target.md)开放，原失败/严格检查器终态1保留。
[R156 REGTYPE物理常量](issue-review-r156-regtype-physical-constants.md)另列开放：
PG/旧已接受版/新clean各34条观察，后两者各14条语义差异，不是34项通过；
OID往返身份丢失、内置呈现与JSON语法身份仍需独立修复，不能用本项显示通过关闭。
共享头改变后两端各正常fresh58生产O2/repeat0、各三个真实Main TU fresh，
新链接各自57当前O2库并隔离执行；输入/提交边界审计通过不代表相邻整批通过。
只接受本项五处源/注册、两个完整夹具及文档；R151继续用户暂缓，不重开R140等专项。
已接受577MD、908native+547Python+3Main=1458；工作区580MD、914+553+3=1470，
仍有12个未接受入口。无新1458全量/TLS/整库sanitizer；原273仍22/166/70/15，
总清单未完成，不push、Actions禁用。

以下各轮摘要保留该轮输入和历史计数；后续状态以本段及各问题文档为准。

[R155 枚举辅助名称归属](issue-review-r155-enum-namespace-owner.md)完成有限修复：
DROP SCHEMA 按真实目录 OID 关联识别解码后的标量枚举名，不再重解析/折叠大小写；
public 带点名称保留实际 owner，竞争身份先报 XX001，旧 native fallback 不变。
PG87全过，旧两端各87/25失败、旧native142/42失败；最终两端142/87全0，
Root24完整入口、独立clean10及归属复核通过。仅私有DDL正常1CPP O2/repeat0；
六Main源/全头flags/对象核对后复用R148三Main对象，新链接57当前O2库并隔离执行，
不冒fresh TU。clean fresh1DDL+56已核对R148对象+当前R149 evaluator，无server stubs。
[R152 REGTYPE显示](issue-review-r152-regtype-identifier-output.md)另记开放：
PG1675全过，旧两端各69失败、native5197/1718失败，夹具/注册未接受，尚无生产修复。
R151按用户要求暂缓、不恢复；R150仍开放。已接受575MD、907+546+3=1456，
工作区578MD、914+553+3=1470仍含14未接受入口；无新1456全量/TLS/整库sanitizer。
原273仍22/166/70/15，总清单未完成，不push、Actions禁用。

[R150阈值重载](issue-review-r150-threshold-width-bucket.md)仍开放；实际PG/Root各完成61条
广泛类型观察，保留非数字类型与XML参考构建能力差异，不冒充61项通过。
[R151枚举数组目录](issue-review-r151-enum-array-catalog.md)按用户“skip it”暂缓：
PG103全过，旧Root/accepted clean各103/86失败，旧native341/280失败；
候选两端native341全0，但完整103协议各仍60失败、四入口终态1。
候选源码、两个夹具及注册原样保留，未验收、未提交；普通继续不恢复本项。
独立REGTYPE名称显示问题可另行复现、修复与提交，不以此重开枚举数组或宣称R150完成。
已接受573MD、906native+545Python+3Main=1454不变；工作区576MD、912+551+3=1466，
另含R151两个未接受入口。原273仍22/166/70/15；无新1454全量/TLS/整库sanitizer。
用户暂缓不重开、不push、Actions禁用。

[R149 WIDTH_BUCKET边界与精度](issue-review-r149-width-bucket-boundaries.md)完成四参数路径修复：
按方向处理包含端点，NUMERIC保持十进制精确商，float8保留自身舍入，
count+1超过int4报22003；已声明重载只归实际builtin，原生同名替换不受影响。
PG804+5全过，冻结旧Root/accepted各804/200失败、旧native515/78失败保留；
最终515/804、Root36、accepted-only clean11及定向执行器UBSan515全0。
首轮Root36两处、clean10一处旧SQLSTATE预期错误均保留终态1；按PG核验
校正预期并补quoted超范围22003断言，没有删断言。生产仅1Evaluator O2/repeat0，
fixture修正后正常0/repeat0；核对六Main源输入、全部头/flags及对象字节后
复用R148真实Main对象，新链接当前57 O2库并隔离执行，不冒本轮fresh TU。
已接受571MD、906native+545Python+3Main=1454；工作区574MD、911+550+3=1464。
原273仍22/166/70/15；二参数阈值重载、完整numeric域与其余缺口仍开放。
无新1454全注册/TLS/整库sanitizer证明；用户暂缓不重开、不push、Actions禁用。

[R147日历间隔顺序](issue-review-r147-interval-calendar-order.md)完成有限修复：
日期先按月份调整并截断到月底，再加天，最后处理微秒与跨日；修正月底、闰年、
混合正负间隔及DML中的错值。PG943全过，旧各507/原生890失败与首版11失败保留。
类型依赖先独立提交f59a1098；当前同一Source625帧的Root48完整入口全部通过，
本项核对后复用该证据，不冒再次运行或重编Main。新accepted-base clean4native+
6wire=10全过，原生2043/协议943及类型552全0。clean为fresh1Evaluator O2+
57个核对全头/flags/CPP/原58receipt及前后字节的R148生产donor，server无stubs。
已接受570MD、905native+544Python+3Main=1452；工作区573MD、910+549+3=1462。
原十个未接受入口及R146等用户暂缓不混入；无新1452全注册/TLS/整库sanitizer证明。
完整civil/DST/其它时间算术仍开放，原273的22/166/70/15不变，不push、Actions禁用。

[R148日期算术结果类型](issue-review-r148-temporal-arithmetic-result-types.md)完成有限修复：
共享的已声明运算签名统一DATE/时间戳与INTERVAL、差值及int4天数的结果类型，
不再继承操作数OID或在NULL/零行时猜值。PG552全过，两旧版各259断言失败、
旧147原生70失败均保留；最终147/552、Root48完整入口与独立clean14通过。
共享头改变后双方均58 fresh生产O2/repeat0，三真实Main本轮fresh O0/O2/O0，
新链接57当前O2库、不用stubs并隔离实际执行。相同对象SHA不改写fresh编译事实。
仅本项类型修复独立提交；R147日历字段顺序候选和两夹具/注册不混入本项。
已接受569MD、904native+543Python+3Main=1450；工作区572MD、910+549+3=1462，
仍含原十个及R147两个未接受入口。无新1450全注册/TLS/整库sanitizer证明。
R146/R137/R140及其它用户暂缓保持；原273的22/166/70/15不变，不push、Actions禁用。

[R145搜索路径参数语义](issue-review-r145-search-path-value-identity.md)完成有限修复：
SQL SET按raw AST的独立参数逐项list-quote，不再把Sconst当已格式化列表；
保留逗号、大小写、单双引号、空名字与关键字语义。FROM CURRENT走真实
persistent/LOCAL赋值栈，避免把LOCAL可见值错误当无操作。PG3573全过，
原版各1098断言失败；初稿各4个事务断言失败，均保留原日志。最终Root/clean3573、
Root18完整入口/clean10及独立复核通过，native254和真实session320全过。
共享头改动后首轮Root/clean均fresh58 O2；最后Main修正再正常1Main O2/repeat0，
核对57库未变，fresh三Main TU O0/O2/O0后新链接当前57 O2库、隔离实际运行。
新clean为fresh Main O2+57逐一核对头/flags/CPP/原receipt/字节的生产donor，无stubs。
已接受567MD、903native+542Python+3Main=1448；工作区570MD、908+547+3=1458。
R144/R140/R137与其它用户暂缓保持未修复，原273的22/166/70/15不变；
无新1448全注册/TLS/整库sanitizer证明，不push，Actions禁用。

[R142 quoted SET参数名](issue-review-r142-quoted-set-parameter.md)完成有限修复：
完整单token双引号名称按GUC大小写无关规则解码，LOCAL搜索路径按规范名判断，
原始@变量分支仍用原语法，quoted @目标不改已有变量。PG530全过，原旧版各544/322
失败，初稿136项SHOW metadata失败/14与clean5终态1保留；SHOW先由R143独立提交，
仅含该修复的干净版本仍544/216失败。最终Root/clean544均0，15完整入口/clean6/
独立核对全0。最新Main/头未变，核对后复用R143两fresh前端对象、新编session298
O0，再链接当前57匹配O2库并隔离运行；不冒全3本轮fresh或生产重编。
R140/R137及其它用户暂缓项保留；R144/R145普通开放不关闭，原273的22/166/70/15
不变，无新1446全量/TLS/整库sanitizer证明。不push，Actions禁用。

[R143 普通SHOW协议结果](issue-review-r143-show-guc-protocol-results.md)完成有限修复：
application_name/client_encoding/search_path/lc_monetary固定text描述不再误报NoData；
结构化单行与显式非NULL位保留空串、字面量NULL及路径中的换行/UTF8，不从stdout猜行。
PG150全过，冻结Root/已接受干净旧版各150/111失败；最终Root14完整入口、clean5
与独立核对均0。正常2CPP O2/repeat0，三fresh真实Main TU/当前57库新链接/隔离运行，
session原290控制；未运行R142待定8或R140待定21扩展。检查器跨行标签/转义错误的
首次1保留，独立v2按完整150控制核对0，未改产品/夹具。R142仍为独立待定候选，
R144应用名规范化、R145路径Sconst列表语义另记普通开放及方法，不冒已修复。
原273的22/166/70/15不变，无新1445全量/TLS/整库sanitizer证明；不push，Actions禁用。

[R141 SET参数身份与分隔符](issue-review-r141-set-parameter-identity.md)完成有限修复：
普通值不再误入compatibility_mode分支，相似/带点名称不改模式，TO值内等号不作分隔符；
独立Extended模式设置不再把未执行的规划事务误当BEGIN，真实事务限制保留。
PG普通参数160全过；冻结旧版各389/235失败。初稿81失败、第二稿21失败及各自
Root13/clean4/独立终态1保留，未删断言；最终389、13完整入口、clean4与独立核对均0。
最终仅Network重编O2/repeat0；三个v2fresh真实Main TU经源/头/依赖核对后新链接当前
57库并隔离执行，session为已接受夹具290，不冒最终fresh或重跑R140待定311。
R140/R137继续用户暂缓、候选不混入；原273的22/166/70/15不变，无新1444全量/
TLS/整库sanitizer证明。不push，Actions禁用。

[R140 TimeZone设置语法](issue-review-r140-timezone-set-grammar.md)按用户“skip it”暂缓。
候选实现、4090原生/6403协议夹具及注册保留，但未提交、未验收，不计已修复。
跳过前专项、Root43和clean7完整入口实际全0；独立终态验收未运行，不补作通过。
本次只提交暂缓记录，已接受902native+538Python+3Main=1443不变；工作区另有
未接受候选。R137及其它暂缓项不重开，原273的22/166/70/15不变，不push、Actions禁用。

[R139 TimeZone默认恢复](issue-review-r139-timezone-default.md)完成有限修复：
DEFAULT和专用TIME ZONE LOCAL复用默认值解析，RESET恢复名称及真实offset，
通用TO LOCAL与带引号关键字仍22023。PG280全过，旧Root/干净旧版各280/48失败；
旧Main五观察确认非UTC默认被清成0，原生扩大到290次命令且原17控制保留。
正常1Main/repeat0、三fresh真实Main TU/当前57库新链接/隔离执行、最终12native+
16wire+3Main=31完整入口与clean5均0，独立核对0，原224协议全过。
R140另有22条实际语法观察：合法紧贴等号仍被拒，缺分隔符/专用等号误接受；
新增DEFAULT还放行两条非法语法，明确保留未修复，不冒完整语法/GUC完成。
R137仍用户暂缓；无新1443全量/TLS/整库sanitizer证明，原273的22/166/70/15不变。
不push，Actions禁用。

[R138 SESSION TimeZone](issue-review-r138-session-timezone.md)已有限修复：
显式SESSION仅在时区路由复用既有值解析，其余SET分派不改。PG224全过，
冻结旧Root/干净旧版各224完整/56失败；原112断言保留并增加112个SESSION控制。
正常1Main/repeat0、三个fresh真实Main TU及当前57库新链接/隔离执行通过；
最终12native+15wire+3Main=30完整入口全0、clean4全0，独立核对0。
首次三Main因wrapper日志路径错误未运行、整批1的记录保留，不冒产品失败或通过。
R137仍用户暂缓/未接受；DEFAULT另有13条观察与方法记录、仍普通未修复。
无新1442全量/TLS/整库sanitizer证明，原273的22/166/70/15不变，不push，Actions禁用。

[R137 外键完整父值核对](issue-review-r137-foreign-key-full-value.md)按用户“skip it”暂缓：
候选实现、测试及原始失败保留，未提交、未验收，不计已修复。初稿57/208及clean7
通过不足以验收；扩展后PG364全过，旧版364/96失败，初稿364/48失败、native89/4失败，
暴露合法网络类型被误拒绝。修订版仅正常1TM/repeat0构建通过，未运行其最终门禁。
旧Root45保留44通过/1末尾DROP断言134、终态1；不改R51断言、不重试覆盖。
候选代码/两测试/注册不进入接受版本；只记录暂缓。通用长文本/复合PK写入及
其它普通问题仍未完成，原273的22/166/70/15不变，不push，Actions禁用。

后续[R136 标量微秒持久化](issue-review-r136-temporal-microsecond-storage.md)已有限修复：
新SQL标量TIME/TIMESTAMP/TIMESTAMPTZ采用显式schema-D微秒codec，旧9/A/B/C与native
工厂保持秒；同类型ALTER重写，索引/排序/NULL/epoch0/时区与实际冷重启纳入验证。
PG412全过、冻结旧408/281失败；新916 codec、92布局（同夹具补齐前四处截断失败）、
408协议与4真实CLI全过。两份fresh58 O2后各补两次私有TM/repeat0，干净6入口0；
最终28native+28Python+3本轮新ABI后fresh、核对未变TU再链接当前57库的Main=59
完整入口全0、独立核对0。未跑新1442全量，旧1436的两失败/1TLSskip保留；
原273仍22/166/70/15，不扩大为TIMETZ/数组precision/全部civil范围或安全专项完成。
不push，Actions禁用。

后续[R135 RETURNING时区呈现](issue-review-r135-timestamptz-returning-timezone.md)已有限修复：
PG最终134全过，冻结旧Root/干净旧版各134中73失败，当前134及原微秒266全过。
只在每条语句的协议结果中按OID1184/日期/会话时区呈现，不改DmlResult或heap，
同一Simple Query批次后续SET不能重画先前结果。正常1/repeat0，干净两O2 CPP+
56生产donor含Main的真实server两入口0；最终18native+24Python（23协议/1CLI）+
3核对R132未变TU后新链接当前库并执行的Main=45完整入口全0，独立0，不冒fresh TU。
新1438未全量，旧1436两失败/1TLSskip保留。新表三标量微秒INSERT/UPDATE22023
已另记为普通未修复，不由呈现修复关闭；原273仍22/166/70/15，不push、Actions禁用。

后续[R134 原生回调签名归属](issue-review-r134-core-native-callback-ownership.md)已有限修复：
干净旧实现1320控制/922失败，同一夹具修正后1320全过；只给实际ABS/LENGTH/UPPER/
LOWER builtin callback声明所有权，opaque替换保留原始参数和caller结果提示。
实际PG原132协议全过，干净一CPP+56匹配库的4完整入口0；Root正常1/repeat0，
22native+12wire+3核对R132未变测试TU后新链接当前库并执行的Main=37完整入口全0，
独立核对0。不是fresh Main TU或新1437全量；旧1436的两真实失败/1TLSskip保留。
原273仍22/166/70/15；通用其他callback和微秒heap仍普通开放，不push、Actions禁用。

2026-10-17[干净ecf632e9完整注册回归](issue-full-registered-ecf632e9.md)已结束：
1436个入口各一次、有序唯一、无漏跑/重复；1433实际通过、2失败、1 TLS环境skip。
标准runner/summary/driver及独立终态检查器均实际1；输入与产物独立核对0，不冒全绿。
全部58生产TU fresh O2/repeat0、3真实Main fresh测试TU/新链接57匹配库/无stubs并实际
隔离执行通过。两失败为background_worker的55P03异常和stored_function_engine_owner
末尾DROP断言，分别按原后台专项/R51范围只登记，不删除断言或猜同根因。
R129/R130原完整失败入口本轮通过，R129/R131/R132/R133新增协议入口也全过；
不由此关闭旧R51、普通其余缺口或原273。原状态22/166/70/15保持；下方各轮
“未全量”等保留当时历史，以本节新终态为准。不push，Actions仍禁用。

后续[R133 timestamp binary微秒](issue-review-r133-timestamp-binary-microseconds.md)修复合法
1114/1184非整秒被22P03拒绝：PG266全过、冻结旧版266/204失败，整数解码及负epoch
向下拆分保留精确微秒与R131 UTC标记。正常1/repeat0、核心266和12native+16wire+
3核对R132未变TU后新链接当前库并执行的真实Main=31完整入口全0，独立核对0。
不冒fresh Main、整秒heap升级或完整时间输入域/错误阶段完成；新1436尚未全量。

后续[R132 SET TimeZone TO](issue-review-r132-timezone-set-to.md)已修复旧Main把TO当值：
PG112全过、冻结旧版112/44失败。只消费通用SET的独立TO，不改引号内数据或专用
TIME ZONE值。正常1/repeat0、三个真实Main fresh TU编译/新链接/隔离运行，以及
12native+15wire+3Main=30完整入口均0，独立核对0；旧R124对象此次未复用。
新1435未全量；LOCAL/SESSION/DEFAULT及完整GUC事务/严格语法仍普通开放。

后续[R131 TIMESTAMPTZ binary时区](issue-review-r131-timestamptz-binary-timezone.md)已修复UTC
时刻被再次按会话本地解释：实际PG158全过、干净旧版和冻结Root各158/45失败。
仅在有限1184中间文本明确+00，1114与infinity/NULL不变；正常1/repeat0及12native+
14wire+3核对后新链接当前库并执行的真实Main=29完整入口全0，独立核对0。
新1434未全量；合法SET TimeZone TO拒绝另列普通R132继续，不冒本项已修。

后续[R130 原生函数结果声明](issue-review-r130-native-result-hint-precedence.md)已修复：干净旧库
实际705/1/smallint、168矩阵53失败，当前168全过；PG56参考全过。显式声明不被
UNKNOWN覆盖，真实callee优先，同名原生替换保留自身所有权。正常2/repeat0及
20native+12wire+3核对后新链接当前库并执行的真实Main=35完整入口全0，独立复核0。
F8F-03本项有限修复，旧完整三失败不改写；新1433未全量，R51仍暂缓。

后续[R129 原生数组投影声明](issue-review-r129-native-array-projection-types.md)修复四处类型提示
丢失isArray，并校正旧TEXT夹具；最终typed夹具在干净旧库实际134，PG/干净旧程序
51协议均全过，证明SQL契约与原生API缺陷须分开。正常1/repeat0及14native+
9wire+3核对后新链接当前库并执行的真实Main=26完整入口全0，独立复核0，不冒Main
TU重编或新1432全量。原8f完整三失败保留，F8F-03继续处理，R51不重开。

2026-10-17[干净8f0465fc完整注册回归](issue-full-registered-8f0465fc.md)已结束，实际退出1：
1431入口各一次、按预期顺序无漏跑/重复，1427实际通过、3失败、1 TLS环境skip。
全58 fresh O2/重复0、三真实Main fresh编译执行及输入/产物终态核对完成；
不把环境skip或运行完整当作通过。三项失败及修改方法已登记，普通数组投影
夹具与pg_typeof声明问题分别处理/提交，DROP DATABASE路径按原R51暂缓。
原273状态/勾选不提升，不push，Actions仍禁用；下方各轮保留其历史范围。

## 输入与结论范围

后续[R108数组元素相等](issue-review-r108-array-position-typed-equality.md)新增
typed ARRAY_POSITION、真实输入声明/UNKNOWN身份、用户callee及数组参数元数据
控制。最终参考258零失败、旧258/158失败，首两版协议失败与新增原生夹具的
校正证据均保留；最终354/258和相关完整入口的真实终态以该记录为准，不借旧全量。

后续[R107长度函数签名](issue-review-r107-string-length-signatures.md)记录四名
11条真实声明、UTF8字符/字节/位单位、空结果与Parse前置分析，以及同名用户
函数/原生回调的所有权。旧654协议392失败、607原生440失败及前两版失败保留；
每版验收范围见该记录，不借旧全量或据局部修复关闭原273。R106按用户要求暂缓。

后续[R104 网络值转换](issue-review-r104-network-value-conversions.md)实际PG/
Root649、原738及完整SDK、21native/17协议/3真实Main通过；五CPP实际4/3/2
增量normal/repeat及核心/最终独立核对均0，按单项本地提交。旧649/419、SDK134、
首版582/3与649/58、第二版649/1保留；无新全量，原273不变，R98/R99不混入。

后续[R105 限定NAME→INET/CIDR](issue-review-r105-network-name-input.md)最终
PG/Root738、完整SDK及21native/16协议/3真实Main全过，自身真实全58/
normal/repeat、核心与最终独立核对均0，按单项本地提交。旧738/34、SDK134
保留；无新全量；R104后续独立修复，原273未关闭，R98/R99候选不混入。

后续[R103 NAME→MAC](issue-review-r103-macaddr-name-input.md)已复现R101回归：
最终PG/Root366、完整SDK及21native/16协议/3真实Main通过；最终自身真实全58/
normal/repeat与独立终态fence全0，单项本地提交。旧366/30、SDK134、首版358/6
和366/12均保留；75纯组件sanitizer不冒整库证明，无新全量；R104/R105后续独立修复，原273未关闭。

后续[R102 INET/CIDR输入](issue-review-r102-network-input-notation.md)最终旧188/72、
672/389失败；当前188/ASan/UBSan、PG/Root672、2558对照及21native/16协议/3真实
Main通过；最终真实全58/normal/repeat、匹配核心与fence全0，单项本地提交。
首版672/23保留，无新全量；R103/R104/R105后续各独立修复，原273不变。

后续[R101 MAC输入与宽度](issue-review-r101-macaddr-input-and-widths.md)纠正输入
格式、MAC48/EUI64转换、共同类型值转换及SQLSTATE。旧75/38、328/186失败，
当前75、PG/Root328、236差分及20native/15协议/3真实Main通过；独立头包含缺陷
修正后全58真实重编成功，全部对象/程序字节相同，新匹配核心2/328及fence全0。
本项独立本地提交；未运行新全量，原273状态不变，首版314/5失败保留。

后续[R100几何分隔符](issue-review-r100-geometric-coordinate-delimiters.md)修复非法
坐标被当成合法输入提交的问题。旧232组件104失败、230协议135失败，当前232、
PG/Root230及十个完整native/七个完整协议通过。原273状态不变。

后续[R97 BEFORE行赋值](issue-review-r97-typed-before-row-assignments.md)修复
原生API的NEW/OLD文本、相似列名与NULL错值；原106/65失败，当前154、15个
完整native及十个完整协议通过。新1392组合未全量，不挪用下述1391结果。

最新[冻结5091b39c完整终态](issue-full-registered-5091b39c.md)：2026-10-15
1391入口全部各一次，1390实际通过、1 TLS环境skip、0失败，实际退出0。
清洁HEAD、全部tracked输入、58 receipt及57份test库字节独立终态核对0。
该结果不证明真实TLS、间歇性超时根因已修复或原273完成；下述未全量/失败为历史输入。

后续[R95旧全局BOOL参数](issue-review-r95-legacy-global-boolean-arguments.md)
118控制、PG/Root60、十个完整native及十个完整协议通过；原setup超时与C warning保留。
新881 native+3 Main+507 Python=1391尚未取得全量终态，原273状态不变。

后续[R94配置SHOW](issue-review-r94-show-parallel-workers.md)专项40、原完整
152、六个完整native及十个不同完整协议全部通过；不改写R93历史1。
新880 native+3 Main+506 Python=1389尚未全量运行，原273完成状态不变。

后续[R93布尔聚合参数](issue-review-r93-boolean-aggregate-arguments.md)核心
408+68控制、21个不同完整native及18个完整协议邻居通过；原152协议150通过，
仅独立SHOW两项失败保留，本范围修复不代表整套全绿。
新879 native+3 Main+505 Python=1387组合未全量运行，不修改原273完成状态。

后续[R92缺失TOAST计数器](issue-review-r92-toast-missing-counter.md)已修复
写入报错仍破坏原长值的有限问题，补齐ADD/DROP初始化；30完整native通过、
18/20协议通过，CREATE和定长ALTER超时保留。新1384组合未全量运行，不把本项
或下述1382历史终态推广为当前源码全绿、完整TOAST或原273完成。

前次[冻结332bcff4完整终态](issue-full-registered-332bcff4.md)于2026-10-15
15:15 UTC确认实际退出1：1382入口各一次，1379实际通过/1 TLS环境skip/2失败。
源码/产物及58 receipt、57 test库字节独立核对0。旧六失败入口与新增14入口
本轮均通过；新失败为末尾DROP断言和SQRT准备CREATE SCHEMA超时，未冒称全绿。
下述“尚未全量运行”等为对应修复提交时的历史记录，不再表示1382组合未运行。

本页保留原始审查基准；后续实现状态见[逐项修复](review-fixes-2026-10-13.md)，
新组合的实际全量终态见[冻结27bac6ee记录](issue-full-registered-27bac6ee.md)：
1368入口、1361实际通过/1 TLS环境skip/6失败，无漏跑/重复；原冻结历史保持不变。
后续固定float8签名及原生callback所有权修复见[R83](issue-review-r83-float8-scalar-signatures.md)，
其定向门禁不替代最新全注册组合验收。
后续[R88统计比较开销](issue-review-r88-analyze-sort-comparison-cost.md)已有
真实243→1操作次数及完整邻居验证；原完整COPY仍超时，未关闭原六项失败。
[R89 ROUND单参数声明](issue-review-r89-round-single-result-domains.md)另有独立
旧实现失败及PG对照；完整签名/双参数/TRUNC范围仍未完成。
[R90 TOAST计数发布](issue-review-r90-toast-durable-id-reservations.md)已有
21不同native、原完整COPY固定三次和七协议邻居通过；其余历史失败入口复查
三过一SAVEPOINT超时，原冻结结果不改写，新1380组合尚未全量运行。
后续[R91](issue-review-r91-savepoint-sequence-runtime-images.md)保留校验、减少
非事务序列推进的映像复制，原MV目标固定三次通过；新1382组合将另行冻结运行。
下文的原始失败和统计不代表后续修复仍未完成，也不由局部通过改写。

审查基准为 `a502ffa30df379c88f176c377cdd0f1dae4f3cd4`，产品源码末次提交 `2dc9554e`。
主工作区开始时干净；独立 EXPLAIN 工作区的未提交修改没有纳入本报告的产品结论。
本轮工作是检测、记录和文档更新，产品修复方案需另行实现及独立验证。

基准包含 `src/` 下 205 个文件、201263 行（含注释及模块 README），412 份已跟踪 Markdown。
静态检查覆盖文件清单和模块接口；深入审查按下面的模块表记录，不能推导为每行都已证明正确。

本轮已复现查询结果和存储页数据完整性错误。标准回归已终态：1232个注册驱动全部运行，1222个返回成功、10个失败；成功计数包含一个因缺少OpenSSL而显式跳过的TLS专项，不能宣称全套通过。
原总账 273 项仍为 22 complete、166 partial、70 unverified、15 deferred_by_user。

## 检测计划与覆盖

1. 固定提交、生产二进制和源码摘要；盘点测试、模块、文档和独立工作区。
2. 并行检查查询执行、存储与目录、网络与复制、构建与测试入口；对具体疑点追溯调用链。
3. 使用独立测试目录运行标准注册回归，并以 PostgreSQL 18.6 对照新发现的 SQL 边界。
4. 汇总严重度、触发条件、实际/预期、代码位置、修复方案和验收测试。
5. 修订使用和状态文档，检查所有文档的本地文件链接，保留历史失败和尚未验证的边界。

| 模块 | 文件数 | 检查重点 |
| --- | ---: | --- |
| parser / expression / executor / main.cpp | 35 | 绑定、窗口语义、数值运算、关联子查询、EXPLAIN |
| storage / access / catalog / commands / transaction | 70 | 持久化、索引读取错误、TEMP 启动恢复、目录原子性 |
| network / replication | 16 | 二进制参数、连接池生命周期、复制状态 |
| common / types / process / utils / interfaces | 84 | 类型及错误边界、后台任务、运行状态、调用契约 |
| scripts / cmake / tests / 部署配置 | 单独盘点 | 源码清单、测试注册、语法、构建与文档一致性 |

用户此前暂缓的 15 项安全/TDE 专项，以及 CTAS／后台 checkpoint 专项诊断继续暂缓。
现有标准回归中的相关结果只如实登记，不扩展为该专项已完成。

## 证据登记

生产输入 SHA-256：`280da1dffcf542dbe9708fe9cbdea995609fc3363db8e01464f2198e6f969e72`。
生产二进制 SHA-256：`0a24168fe54d61b1382448b4df58a983a978ad1c12733c37c959930ef1b5c80a`。
标准回归日志：`/tmp/dbms-review-20261013.f9Ywep98/full-regression.log`。
原始运行日志为本机辅助证据，关键复现和结论会保存在本仓库中，避免只依赖临时文件。
完整日志SHA-256：`5c00bd608190058c75ff829f2f4894c81e711a9cc249e04a03ec6b317a59f006`。
原驱动实际exit1；终态源码摘要、58个生产TU对象签名、构建配置及冻结二进制比较均通过。
只有完成该终态校验后才更新`src/`下四份模块README；本轮没有修改产品代码、脚本、测试或构建清单。

实际工具链：g++ 13.3.0、Python 3.13.12、zlib 1.3、ICU 74.2。没有人工删减注册项、修改断言或放宽默认期限；TLS环境跳过按原驱动如实记录。运行期间保持产品源码、测试、脚本和构建清单摘要不变。
编译存在warning，不能沿用旧批次“无警告”的结论。类别包括缺省成员初始化、未使用项、易误读缩进、运算符优先级和字符串地址比较；最后一项作为R29单独记录。一般warning并不自动等同于已复现的数据库错误，后续清理应逐项确认语义并保留回归。

已完成的机械检查：465 个 Python 文件 AST 解析、13 个 shell 文件 `bash -n`，均通过。
412 份 Markdown 的本地文件链接检查发现两处失效链接，均在历史 `all-gaps-todo.md` 中指向已移动的档案。
`check_gap_progress.py` 通过一致性检查；`--require-complete` 按预期失败，因为清单尚未完成。

本轮新增报告后共417份Markdown，本地文件链接复查通过；465 Python / 13 shell 语法检查不代表运行时通过。
现有文档状态、版本、兼容性契约检查和6个总账单元测试通过。
注册集合与实际终态逐项核对，无遗漏、无额外项、无重复；结果如下。

| 注册组 | 数量 | 返回成功 | 失败 |
| --- | ---: | ---: | ---: |
| 普通native | 785 | 783 | 2 |
| 真实Main frontend | 3 | 3 | 0 |
| Python driver | 444 | 436 | 8 |
| 合计 | 1232 | 1222 | 10 |

Python的成功数包含`statement_timeout_tls_protocol_e2e_test.py`输出的明确环境跳过；该项没有进行真实TLS测试。未发现COMPILE FAILED或LINK FAILED。
五个未注册驱动另行运行，均实际exit 0，摘要在 `/tmp/dbms-review-20261013.f9Ywep98/unregistered-summary.log`：复制槽持久化、pg_settings查询、bound DML children、named ON CONFLICT、离线checksum验证。这些通过不消除注册缺口。

本机缺少 `cmake`，配置尝试exit127，未运行CMake构建；未安装额外依赖。标准脚本使用TLS stub/plain TCP，不能当作真实OpenSSL/TLS验收。
本轮未运行全库ASan/UBSan/TSan、fuzz、长期soak或实际多节点复制。前六组新SQL对照共89条语句；49条观测不同，其中包含错误写入造成的后续状态差异，不能当作49个独立bug。SQL的MATCH仅比较行、SQLSTATE和OID，不比较诊断文本、列名及command tag。正常binary float 6项中4项失败。
注册回归新失败的后续定位另有27个协议动作（21个Simple Query、6个Parse/Describe/Sync），其中两个描述动作与PG18不同，归入R30；三个原注册驱动的独立诊断副本只调整仓库定位及增加消息打印、保留全部原SQL/顺序/断言，均实际exit1，其中第三个进一步确认TEMP标量子查询的同源错误。
原实现的独立O2 API检查另有Numeric有效数字舍入4项中3项失败，以及PgPage正常插入/删除/压缩3项中2项失败；这两个驱动的实际退出码均为1。

## 问题与修复方法

P1 表示可能导致错误结果、崩溃或服务不可用；P2 表示受限输入下的行为错误或验证缺口。
以下均未在本轮修改产品实现。完整 SQL、输出行和 OID 已保存到[原始观测](review-evidence-2026-10-13.md)。

| ID | 等级 | 问题 | 证据状态 | 建议修复顺序 |
| --- | --- | --- | --- | ---: |
| R01 | P1 | 大 NUMERIC 的 SQRT 返回负数 | PG18.6 对照复现 | 3 |
| R02 | P1 | 窗口排序、peer、MIN/MAX 猜测类型并丢失整数精度 | PG18.6 对照复现 | 4 |
| R03 | P2 | NUMERIC 等值但不同 scale 被拆成多个窗口分区 | PG18.6 对照复现 | 4 |
| R04 | P1 | 窗口 frame 丢失边界方向，非法组合仍成功 | PG18.6 对照复现 | 5 |
| R05 | P2 | NTILE 桶大小分配不正确，桶号跳号 | PG18.6 对照复现 | 6 |
| R06 | P2 | NTILE 非正桶数被静默改为 1 | PG18.6 对照复现 | 6 |
| R07 | P2 | LAG/LEAD 零偏移及负偏移错误 | PG18.6 对照复现 | 6 |
| R08 | P1 | binary float4/float8 参数以六位小数格式化导致精度损失 | 正常协议精确回传复现 | 2 |
| R09 | P1 | 连接池等待期间保存了可被删除的容器引用 | 源码生命周期确认，未跑并发/ASan | 1 |
| R10 | P1 | 同用户跨数据库连接池可能永久等待 | 源码条件链确认，未跑阻塞探针 | 1 |
| R11 | P1 | 窗口精确 SUM/AVG 仍执行可能溢出的 int64 中间累加 | 静态 UB 风险；O2 结果未失败，未跑 UBSan | 4 |
| R12 | P2 | 独立测试未进入完整注册回归 | 文件与调度列表核对 | 7 |
| R13 | P2 | 逻辑变更行像不能区分 NULL、空串和字段分隔符 | 源码编码链确认，未做运行时流对照 | 3 |
| R14 | P2 | publication 读取失败被当作未发布，可能漏采变更 | 静态错误路径确认，未做故障注入 | 3 |
| R15 | P1 | 服务停机先 join 再唤醒池等待者，可能无法退出 | 源码等待链确认，未跑停机探针 | 1 |
| R16 | P1 | 损坏 catalog 行被跳过，后续可能将缺行状态写回 | 静态路径确认，未做故障注入 | 2 |
| R17 | P1 | DDL 回滚失败仍尝试清理恢复备份 | 静态错误路径确认，未做故障注入 | 2 |
| R18 | P2 | catalog getter 返回的内部行指针可能失效 | API 生命周期风险；SQL 并发可达性待验证 | 2 |
| R19 | P1 | 失效 session-temp 元数据阻断普通启动恢复 | 历史失败在本轮注册回归再次出现，并已追踪源码 | 2 |
| R20 | P1 | INSERT/UPDATE RETURNING 子查询被当成字符串，错误写入提交 | PG18.6 对照及后续表内容复现 | 1 |
| R21 | P1 | ON CONFLICT WHERE 缺少 BOOLEAN 绑定，`'f'` 也更新 | PG18.6 对照及后续表内容复现 | 1 |
| R22 | P2 | 严格模式接受缺少 FROM/INTO 的修改语句 | 显式 postgresql18 模式对照复现 | 3 |
| R23 | P1 | 后台维护状态读取/持久化失败仍执行任务 | 静态控制流确认，未运行故障探针 | 2 |
| R24 | P1 | SQL EXECUTE 参数按引用次数/行数求值，未引用参数不求值 | PG18.6 对照及 sequence 状态复现 | 1 |
| R25 | P2 | Numeric::withPrecision 丢失整数数量级 | 原实现独立O2 API复现；没有生产调用者 | 7 |
| R26 | P2 | 递归CTE未核对共同输出类型/typmod与seed声明 | PG18.6对照复现 | 5 |
| R27 | P1 | 页压缩只处理前256个存活行，错误释放其余行空间 | 正常API操作复现，3个控制中2个失败 | 1 |
| R28 | P2 | 页插入复用空槽时的空间减法可能下溢 | 静态API风险；未跑边界探针，普通INSERT已检查canFit | 2 |
| R29 | P2 | LIKE ESCAPE绑定以字符串地址判断bytea类型 | 编译器-Waddress及源码确认；当前构建未复现SQL失败 | 5 |
| R30 | P2 | Extended Parse/Describe缺少会话上下文，搜索路径与错误前后行为不一致 | 注册回归失败及PG18.6协议对照复现 | 2 |

### R01：大 NUMERIC 平方根

位置：`src/expression/ExprEvaluator.cpp:8809`、`:8817`。

```sql
SELECT sqrt(10000000000000000000000000000000000000000::numeric);
```

预期 `100000000000000000000`，实际 `-9223372036854775808`，双方结果 OID 均为 1700。
输入经 `asDouble()` 丢失精度，整数形状的结果又直接转为 `long long`；超出 int64 的浮点转整数是未定义行为。

修复：按已绑定的 numeric/float8 重载分别计算。NUMERIC 使用十进制算法并保留结果 scale，禁止经 double/int64 中转；float8 使用对应浮点处理。仅检查 int64 上限只能阻止非法转换，不能恢复 NUMERIC 精度。
验收：大数及相邻值、边界、小数、NULL/NaN/Infinity、负数错误、参数和表列路径。

### R02、R03：窗口比较和分区等价关系

位置：`src/executor/ExecutionPlan.cpp:3784` 的 `compareNonNullAggregateValue`、`:3807` 的窗口比较和 `:3817` 的 `sameWindowPartition`。

实证：TEXT `('2','10','1','01')` 被猜成数值比较，`'01'` 与 `'1'` 变成 peer，MIN 返回 `'1'`；BIGINT `9007199254740993` 与 `9007199254740992` 因 double 舍入被当成相同值，RANK 和 MIN 错误；NUMERIC `1.0,1.00,1,2` 的分区 COUNT 实际为 `1,1,1,1`，预期 `3,3,3,1`。
MAX使用同一比较器，属于源码外推；本轮没有独立MAX对照日志。

修复：将声明类型和 collation 传入统一类型化比较器。整数保持精确、NUMERIC 按十进制、文本按文本规则比较；排序、peer 和分区使用一致的 SQL 等价关系，NULL 依照分组语义处理。不能对所有字符串尝试 `strtod`，也不能以原始字符串差异决定 NUMERIC 分区。
验收：数字状 TEXT、空字符串/NULL、2^53 附近 BIGINT、高精度 NUMERIC、不同 scale、复合分区、ASC/DESC、RANK/DENSE_RANK/MIN/MAX 和 peer frame。

### R04：窗口 frame 边界

位置：`src/main.cpp:6022` 的窗口解析；`src/executor/ExecutionPlan.cpp:4085` 的 frame 计算。

共同输入 `w(id,v) = (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)`：

| SUM(v) frame | PostgreSQL 18.6 | 当前程序 |
| --- | --- | --- |
| `ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING` | NULL,10,20,30,40,50 | 10,30,50,70,90,110 |
| `ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING` | 50,70,90,110,60,NULL | 30,60,100,150,210,210 |
| `ROWS BETWEEN CURRENT ROW AND 1 PRECEDING` | SQLSTATE 42P20 | 成功返回当前行的值 |

修复：扩展 typed WindowDef AST 和对应 parser，为两端分别保存 boundary kind 和 offset 表达式，再由执行器消费结构化边界，分别表示 unbounded/current/offset preceding/offset following；绑定阶段验证非法组合，执行时计算、裁剪并保留空 frame。现有 AST 和执行结构都没有完整保存两端方向，仅修改执行器不能解决输入信息丢失。
验收：ROWS/RANGE/GROUPS、全部合法方向组合、简写、非法组合、NULL/负数 offset、分区边缘、EXCLUDE、空输入与 LIMIT 0 的验证阶段。

### R05、R06：NTILE

位置：`src/executor/ExecutionPlan.cpp:4164`、`:4166`；legacy 分支 `src/main.cpp:35825`。
六行输入下 `ntile(4)` 实际为 `1,1,2,3,3,4`，预期 `1,1,2,2,3,4`；`ntile(8)` 实际为 `1,2,3,5,6,7`，预期 `1,2,3,4,5,6`。
`ntile(0)` 和 `ntile(-1)` 应返回 SQLSTATE `22014`，实际均成功返回桶号 1。

修复：用商 `q=rows/buckets` 和余数 `r=rows%buckets` 分配，前 r 桶各 q+1 行，其余各 q 行；桶数超过行数时连续编号。参数非 NULL 且非正时抛结构化 `22014`，保留 NULL 语义。统一两个执行路径，避免乘法溢出和默认值掩盖错误。
验收：6/4、6/8、7/3、整除、单行、多分区、NULL、非正桶数、参数及列表达式。

### R07：LAG/LEAD 偏移

位置：`src/main.cpp:6285`、`:35798`；`src/executor/ExecutionPlan.cpp:4142`。
在上述六行输入中，`lag(v,0)`、`lead(v,0)` 均返回全 NULL，预期返回当前行 v；`lag(v,-1)` 也错误返回全 NULL。
偏移经 `stoull` 解析，零导致 legacy 回退，负数可变成巨大无符号值；执行器还把 offset 强制为至少 1。

修复：按 SQL INTEGER 保存有符号偏移，零取当前行，负数反转方向；使用足够宽的有符号计算后再检查分区边界。保留 NULL offset 和 typed default，避免回退到把 `v,0` 当列名的字符串分支。
验收：0、±1、INT_MIN/MAX、多分区、NULL offset、默认空文本/NULL/字面量 `NULL`。

### R08：二进制浮点参数

位置：`src/network/NetworkServer.cpp:753`、`:764`，Bind 消费位置 `:5992`。
使用正常 Parse/Bind/Describe/Execute/Sync，`SELECT $1` 要求 binary 输入原样作为同 OID 的 binary 输出：

| 输入类型和值 | 当前输出 | 结果 |
| --- | --- | --- |
| float8 0.1234567890123456 | 0.123457 | 丢精度 |
| float8 1e-12 | 0 | 丢值 |
| float4 1.2345678806304932 | 1.2345679998397827 | 位模式 3f9e0652 变为 3f9e0653 |
| float4 约 1e-12 | 0 | 丢值 |
| float4/float8 1.25 | 1.25 | 两个阳性控制通过 |

原因是解码后调用 `std::to_string`，默认只有六位小数，之后 CAST 无法恢复输入。
修复：使用保真的 typed datum；若现有接口必须返回文本，则以 `to_chars` general/max_digits10 或已验证的往返格式化函数输出。保留 NaN/Infinity、负零和次正规值语义。
验收：输入/输出逐字节比较、OID/format code、正常事务状态、float4/8 极值、小值、负零、非有限值和 INSERT/UPDATE 参数。

### R09、R10：连接池生命周期和等待条件

位置：`src/network/ConnectionPool.cpp:58`、`:83`、`:129`；真实消费见 `src/network/NetworkServer.cpp:4953`、`:4967`。

R09：acquire 保存 `pools_[user]` 的 vector 引用后 `cv.wait` 解锁；另一线程 discard 最后一个 context 时删除整个 map 节点，等待谓词和后续遍历继续使用该引用。正常连接关闭与另一同用户连接等待即可形成这条生命周期路径。本轮未运行并发探针或 ASan，不声称已观测到崩溃。

R10：容量按 user 计算，复用却要求 database 一致。pool_size=1 时，db1 的 idle context 占满容量，随后同 user 请求 db2 会等待一个永远不会匹配的 context；等待条件也没有“容量已释放，可以创建”的分支。本轮未执行阻塞探针。

修复：把 acquire 改为持锁重查 key 的循环，任何唤醒后重新寻找容器；以 `(user,database)` 明确池的身份和容量规则，或提供安全的闲置 context 重建策略。等待条件应包含 shutdown、可复用 context、可创建容量；不要跨 wait 保存可删除元素的引用。
验收：pool_size=1、同用户跨库、release/discard、等待期间关闭、并发租还、取消/超时及 ASan/TSan。每个连接的 session 状态重置须单独验证。

### R11：窗口 SUM/AVG 中间有符号溢出

位置：`src/executor/ExecutionPlan.cpp:4267` 的 `sum += number`。
即使最终使用 `Numeric exactSum`，两个 MAX_BIGINT 输入仍先执行 int64 加法。当前 O2 对照结果正确，因此本项是静态 UB 风险，不能登记为已复现错值或 UBSan 已通过。
修复：删除无用的 int64 后备累加，或使用有检查且符合结果类型的累加器；精确累加失败应传播错误，禁止回退到已经溢出的中间值。
验收：实际 WindowOp 的 UBSan 测试，包含 MAX+MAX、MIN+MIN、AVG 和累计 frame。

### R12：测试注册遗漏

`scripts/build_tests.sh` 只调度 `scripts/build_common.sh` 的 `DBMS_E2E_TESTS`，不会自动扫描所有 Python 文件。
以下独立 E2E 未在该列表中：`replication_slot_persistence_e2e_test.py`、`pg_settings_query_protocol_e2e_test.py`、`bound_dml_query_children_protocol_e2e_test.py`、`on_conflict_constraint_protocol_e2e_test.py` 和 `checksum_verification_e2e_test.py`。
源码清单则完整，TLS 的两个实现按配置二选一加入，没有生产 TU 遗漏。

修复：核对每个测试的依赖和预期运行环境，接入规范入口；对需专门环境的驱动明确单独入口和理由。新增测试清单一致性检查，使文件新增却未注册在检查中可见。CMake check、ci 和安静入口均应继续共用该清单。
验收：每个新增注册驱动实际运行，并验证清单检查能捕获人为遗漏；不能只改统计数字。

### R13、R14：逻辑变更预览的值与连续性

位置：`src/commands/TableManage.cpp:26282` 的 INSERT 行像、`:31177` 的 UPDATE 行像；
`src/replication/LogicalDecoder.h` 的 LogicalChange 和 `LogicalDecoder.cpp:24`、`:54`、`:604`。

R13：行像按 `|` 拼接各列，只追加非 NULL 的文本值，因此两列 `(NULL,'x')` 和 `('','x')` 都变成 `|x`；`('a|b','c')` 与 `('a','b|c')` 都变成 `a|b|c`。二进制预览只为整行添加长度，文本预览也没有逐列边界/NULL编码，均不能恢复已丢失的信息。
修复：以 nullable typed cells 保存各列，逐列编码长度/NULL/类型信息，对预览/API输出格式明确版本。当前change stream仅在内存中，不存在可据此宣称已修复的持久化流。测试 INSERT/UPDATE/DELETE 的 NULL、空串、分隔符、多字节和回滚。

R14：`PublicationCatalog::publishes` 调用严格 list 失败后返回 false，与合法“不属于任何 publication”相同；写路径据此跳过逻辑变更缓冲。若已发布对象的元数据发生读取错误，DML 可能成功而变更流没有对应记录。本轮没有实施该故障注入。
修复：返回可区分成员关系与读取失败的结果，向事务传播错误，或明确使受影响槽失效并要求重新同步；不能静默宣称流连续。测试读失败、非法元数据、正常无 publication 和已有槽的连续性。

这些问题属于本项目的 `dbms_pgoutput_preview` / `dbms_test_decoding` 预览功能，不表示真实 PostgreSQL 流复制已经实现。

### R15：停机顺序

位置：`src/network/NetworkServer.cpp:6675` 至 `:6677`。
当前顺序是关闭客户端 socket、join 所有 worker、最后 `ConnectionPool::shutdown()`。
阻塞于 acquire 条件变量的 worker 并不在等 socket，因此关闭 socket 不能唤醒它；join 又阻止后面的池 shutdown 执行。
修复：先设置池 shutdown 并通知等待者，再等待 worker 退出；同时核对其他关停和重启入口的一致性。
验收：满池或跨库等待期间正常请求停机，有限时退出且无遗留 worker，保留已接收事务的正确清理行为。本轮未执行 hang 探针。

### R16：损坏 catalog 行不能静默消失

位置：`src/catalog/catalog.cpp:2215`、`:2276`。
pg_class / pg_attribute 可选尾段解析错误执行 continue；loadAllLocked 随后接受缺行模型为 baseline，后续 persistAll 可能将该模型写回。基础前缀的流状态检查同样不完整。
修复：完整校验可接受格式的所有字段、布尔值和尾部；坏行抛结构化损坏错误，I/O 失败返回读取错误。通过现有 replacement-model 异常恢复保留原缓存和磁盘，禁止跳过坏行后发布。
验收：两种表的截断/坏字段/尾随数据/读取失败，验证未改变已有文件和已加载模型。现有 pg_type 坏行测试不覆盖这两个解析器。

### R17：失败回滚的恢复备份

位置：`src/commands/DdlTransaction.cpp:266`、`:276`；`src/commands/TableManage.cpp:50141`。
rollbackTransaction 返回非 OK 后仅输出 warning，随后仍调用 discardTransactionBackup；事务 ID 已清零时也允许删除已拥有的备份。
修复：只有完整持久回滚确认成功后才清理恢复图像。失败时保留备份及状态，向调用者报告 incomplete rollback；明确下次启动的恢复步骤。同文件 `TableManage.cpp:49939` 在部分回滚步骤失败后仍可能持久化 ABORT，而启动恢复 `:43596` 又可能据此删除备份。因此还需让终结 ABORT 表示 undo 已持久完成，或持久化独立 incomplete-rollback 状态供启动保留并重试；仅修改备份清理调用并不充分。
验收：引擎回滚失败、DDL snapshot 恢复失败及随后重启。正常回滚通过不能证明本错误分支安全。
析构路径也会调用rollback；恢复失败应以显式状态及次级诊断报告，保留原始SQL错误，不在析构中新增抛异常行为。

### R18：catalog 行指针的寿命

位置：`src/catalog/catalog.cpp:646`、`:656` 返回 vector 内部地址；`:485` 的 createClass 可能重分配，peer reload 也替换模型。`src/commands/TableManage.cpp:19625` 在 getter 锁释放后复制返回值。
本轮确认 API 层面的悬空风险，但尚未证明普通 SQL 的外层数据库锁允许具体交错。
修复：返回 `optional<Row>` 值快照，或提供不可变 snapshot/显式生命周期守卫；仅给 getter 加 mutex 不能保护返回后的指针。
验收：查询、创建、刷新和 evict 的并发 API 测试，再核对真实 SQL 的锁层级和吞吐影响。

### R19：失效 TEMP 条目阻断启动

原完整日志 `root-full786-443.log` 的 `stale_temp_startup_recovery_test` 实际异常退出。
本轮原注册驱动再次在同一测试抛出 `WAL crash recovery failed; startup aborted to protect data` 并异常退出；完整套件继续执行，不以重跑覆盖这次失败。
`src/commands/TableManage.cpp:43821` 枚举全部关系建立 physicalRelationId 映射，`:43823` 在临时条目缺少 schema 时直接失败；普通启动构造器据此中止。已有较晚的 specialized-index 临时名过滤无法覆盖该入口。
修复：恢复的持久关系枚举统一排除真正的 reserved session-temp 名称，保留普通持久表缺 schema 的 fail-closed 行为；一并检查同文件 `42835`、`42918`、`43667` 相邻枚举。
验收：原测试不缩减，并覆盖有 lifecycle WAL、残留 TEMP、健康持久表和持久 schema 真缺失的不同情况。

### R20：RETURNING 子查询没有执行

位置：`src/commands/DmlExecutor.cpp:1367`、`:2437`、`:2993`、`:4045`；子查询占位的来源为 `src/parser/parser.cpp:2765`。

```sql
CREATE TABLE e(id INTEGER PRIMARY KEY,v INTEGER);
INSERT INTO e VALUES (1,10),(2,20),(3,30);
INSERT INTO e VALUES (4,40) RETURNING id,(SELECT 1/0);
UPDATE e SET v=99 WHERE id=1 RETURNING id,(SELECT 1/0);
INSERT INTO e VALUES (5,50) RETURNING (SELECT v FROM e);
```

PostgreSQL 18.6 分别报 `22012`、`22012`、`21000`，数据不变。
当前程序把子查询源文本以 VARCHAR/OID1043 返回，随后 SELECT 证明 id4/id5 已插入、id1 已改成99。
正常 COUNT 子查询也返回源文本；DELETE 的对应控制则正常执行，缺陷不能归结为所有 RETURNING 均不支持。

原因：子查询最初以 LiteralExpr 保存并由 binder 赋予 preparedSubquery；INSERT/UPDATE 虽创建绑定计划，RETURNING 仍消费原 `stmt.returning`，普通 evaluator 将未绑定占位当成字符串。错误没有发生，语句正常提交。
修复：使用完整绑定 AST 的 RETURNING 表达式和静态描述符，通过 PreparedQueryExecution 的子查询执行器传入真实 OLD/NEW 行上下文；分析与执行错误保留 SQLSTATE，同一语句边界内清除部分结果并回滚。不能只按字符串外观拒绝子查询。
验收：三种 DML 的正常计数、空子查询、21000/22012、关联子查询、多行、OID/name、PREPARE/EXECUTE，失败后检查表内容和事务状态。

### R21：ON CONFLICT 条件没有 BOOLEAN 上下文

位置：`src/parser/query_binding.cpp:1516`；`src/commands/DmlExecutor.cpp:3376`。

```sql
INSERT INTO e VALUES (2,200)
ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE 'f' RETURNING id,v;
```

预期 `INSERT 0 0`、原 id2 保持20；当前返回 `(2,200)` / `INSERT 0 1`，实际表已修改。
`WHERE 1` 应报42804，`WHERE 'notboolean'` 应报22P02，当前都被当真；没有冲突时非法条件也没有在分析期拒绝。本轮另实测 `PREPARE ... WHERE 1` 错误成功，未单测 `'notboolean'` 的 PREPARE 阶段。

原因：binder 只解析条件，没有 BOOLEAN 验证及 UNKNOWN 转换；执行时又丢弃类型，只把 NULL、空串、`0`、`false` 当假。
修复：分析阶段复用普通 WHERE 的 BOOLEAN 验证/UNKNOWN 转换，执行实际消费已绑定的 typed ExprValue，仅非 NULL 且 asBool 为真才更新。确保无冲突、空源和 PREPARE 也按正确阶段验证。
验收：true/false/NULL、`'t'/'f'/'off'/'notboolean'`、1、NULL::integer，冲突/不冲突、VALUES/SELECT 和预备语句；检查 SQLSTATE、tag 及表内容。

### R22：严格模式中的缺失关键字

位置：`src/parser/parser.cpp:4375`（DELETE）、`:4013`（INSERT）。
显式 `postgresql18` 模式下，`DELETE e WHERE id=3 RETURNING id` 仍实际删行，`INSERT e VALUES(7,700) RETURNING id` 仍实际插行；PostgreSQL 两者均报42601。
修复：通用严格 parser 必须要求 DELETE FROM 和 INSERT INTO，缺失时在名称解析、表达式执行和修改前报42601；如确有项目扩展需求，应由显式 extended 模式单独路由。
验收：parser/native、Simple Query 和 PREPARE；正确/缺失关键字、注释、大小写、ONLY/别名/RETURNING，并确认拒绝后数据和 sequence 不变。

### R23：后台维护任务在持久化不健康时继续执行

位置：`src/process/MaintenanceJobManager.cpp:126`、`:142`、`:264`、`:277`、`:300`、`:329`、`:362`。
loadLocked 先清空 jobs 再逐行插入，后续坏行导致失败却留下已解析的 Queued 前缀；构造器仍启动 worker。worker 不检查 persistenceHealthy，且写入 Running 状态失败后也继续调用 runner。结束状态持久化失败后，剩余队列仍可能执行。
修复：完整解析到临时模型，全部成功后交换；以 persistenceHealthy 控制 dispatch，只有 Running 状态持久成功才调用 runner。失败时保留可恢复状态并向操作方明确报告，不能执行被拒绝状态文件中的部分任务。
验收：有效前缀加坏尾部、Running 写入失败、终态写入失败、正常重启恢复/取消/限速。此项未做动态故障验证。

### R24：SQL EXECUTE 参数求值次数

位置：`src/main.cpp:5131`、`:5146`、`:5153`、`:5186`；`src/utils/prepared_stmts.cpp:275`。

```sql
CREATE SEQUENCE s;
PREPARE p(bigint) AS SELECT $1,$1;
EXECUTE p(nextval('s'));
SELECT currval('s');
```

预期返回 `(1,1)` 且 currval=1；当前返回 `(1,2)` 且 currval=2。
声明了参数但模板只有 `SELECT 7` 时，PG18 仍求值实参一次，当前完全不求值；两行查询的同一 `$1` 则在当前程序返回1和2，PG18两行均为1。
Root 输出列名也受到替换表达式污染。14条语句对照和 sequence 状态已保存到[原始观测](review-evidence-2026-10-13.md)。

原因：EXECUTE 保存原参数表达式文本并附 CAST，在每个 `$n` 位置重复拼入，再重新执行 SQL。求值次数由模板引用和行数决定，而非 EXECUTE 的参数边界。
修复：EXECUTE 入口绑定并按声明类型对每个实参求值一次，保存 typed/nullable cells；使用保存的 prepared AST/plan 和同一参数 slot 执行，保留原输出描述符。未被模板使用的声明参数也按参考行为求值。
验收：重复引用、未引用、多行、NULL、错误、多个参数的求值顺序、类型转换失败以及 DML 消费者。该问题与 wire Bind 已有参数修复不同。

### R25：Numeric 公共 API 的有效数字舍入

位置：`src/types/numeric.cpp:618` 至 `:621`。
withPrecision 删除整数末位后将负 scale 钳为0，却没有补回数量级；静态推导 `Numeric("12345").withPrecision(3)` 为123，预期12300；999取两位会成为100，预期1000。
本轮直接以原 `src/types/numeric.cpp` 和独立C++驱动做O2编译，4个API控制中3个失败，实际exit1；小数1.2345取3位仍正确。源码搜索没有发现生产调用者，不能由此宣称 SQL NUMERIC typmod 当前错误。
这里的12300/1000是该API“保留有效数字”契约的预期，不是PostgreSQL typmod差分结果。
修复：校验 precision，复用保留负 scale 数量级的 withScale 逻辑；验证整数、负数、进位和非法精度，以及现有小数控制。

```text
12345 precision=3 expected=12300 actual=123 match=0
999 precision=2 expected=1000 actual=100 match=0
-12345 precision=3 expected=-12300 actual=-123 match=0
1.2345 precision=3 expected=1.23 actual=1.23 match=1
cases=4 failures=3
```

编译与运行：`g++ -std=c++17 -O2 -Wall -Wextra -Isrc /tmp/dbms-review-20261013.f9Ywep98/numeric_precision_probe.cpp src/types/numeric.cpp -o /tmp/dbms-review-20261013.f9Ywep98/numeric_precision_probe`，随后单独执行该驱动。驱动逐项调用上述API并比较expected，失败时返回1；未修改生产或原测试源。

### R26：递归 CTE 的类型声明

位置：`src/parser/query_binding.cpp:1341` 和 `src/main.cpp:13114`、`:13176`。

```sql
WITH RECURSIVE q(n) AS (
  SELECT 1 UNION ALL SELECT (n+1)::bigint FROM q WHERE n<2
) SELECT n FROM q;
```

PG18.6 报42804（seed是integer、共同类型为bigint），当前返回1、2且OID仍为23。
varchar(1) seed与varchar(2)递归项也错误成功；显式bigint seed的合法控制双方正确返回OID20。
binder和legacy递归执行路径都只检查列数，最终沿用seed类型。

修复：先按集合操作规则解析共同输出类型和typmod，再要求递归CTE的共同声明与seed声明一致，不一致报42804。不能简单要求两侧原始类型完全相同，因为合法的隐式提升仍须保留。两个消费者共享执行前校验，不依赖递归项是否实际产出行。
验收：本次类型/typmod反例、合法bigint seed、合法RHS提升、递归分支为空、非自引用WITH RECURSIVE的普通UNION。
本轮另外9条语句对照中2条不同；最近层范围遮蔽的42703控制正常。嵌套CTE控制被双方相同的ORDER BY歧义挡住，不能作为其缓存正确性证明。

### R27：超过256个存活行的页压缩

位置：`src/storage/PgPage.cpp:362`、`:367`、`:407`；普通VACUUM的调用见 `src/commands/TableManage.cpp:46231`。
compact只收集前256个LP_NORMAL条目，随后却用这部分条目的总长度更新整页pd_upper。合法小行能够在同一页中超过该数量，剩余存活行被错误算进空闲区。checksum重新计算成功并不能证明布局及行内容正确。

本轮使用原PgPage.cpp、正常26字节tuple和公开API：依次insert、remove首行、compact，再经canFit检查做两次insert；未直接改写页头或构造非法页。

| 初始行数 / 删除后存活数 | 压缩前布局/内容 | 压缩后布局检查 | 压缩后空间 | 再插入两行后幸存行内容 |
| --- | --- | --- | --- | --- |
| 257 / 256 | 正确 | 通过 | 454→480，符合回收26字节 | 全部正确，阳性控制 |
| 258 / 257 | 正确 | 失败，checksum却通过 | 424→476，正确应450 | 1行内容改变，布局检查反而重新通过 |
| 270 / 269 | 正确 | 失败，checksum却通过 | 64→428，正确应90 | 1行内容改变 |

独立驱动实际exit1，父审查者重复执行得到相同结果。这里证明的是正常API的数据完整性错误；本轮没有另跑SQL VACUUM端到端复现，不能将源码调用链等同于SQL实测。
修复：根据实际line-pointer数量收集全部存活条目，或使用由页大小和最小条目大小推导且经过检查的容量；完整验证条目布局后再搬移，不允许静默截断。按所有存活行计算pd_upper，保持slot身份、reserved special空间及页标识，避免回收仍被使用的区域。
验收：256/257/269存活行、不同删除位置、多轮压缩和后续插入；核对每个幸存行的字节、页布局、freeSpace与checksum，并补普通SQL小行表DELETE/VACUUM/INSERT及重启后内容对照。完整独立驱动和原始输出保存在[证据文档](review-evidence-2026-10-13.md)。

### R28：复用已删除槽位的空间计算

位置：`src/storage/PgPage.cpp:168`。
复用LP_UNUSED时从pd_upper-pd_lower再次减去sizeof(ItemIdData)，既不必要，又在剩余连续空间少于一个ItemIdData时产生无符号下溢，使本应拒绝的请求可能通过。复用已有槽本来不需要新增line-pointer空间。
当前普通INSERT两条已检查的路径先调用canFit（`src/commands/TableManage.cpp:25853`、`:25889`），能阻止这里的空间不足输入，因此本项仅登记为公共API静态风险，没有宣称SQL端可复现或已经发生损坏。
修复：用先比较再相减的方式计算可用空间；明确区分复用槽和新建槽的开销，使insert本身提供完整容量保证，并让canFit与之采用一致规则。失败必须保持页字节不变。
验收：空闲空间0至一个line-pointer大小的边界、可复用槽/无可复用槽、恰好放下的行、拒绝后checksum与原内容不变；本轮未执行这些新增边界测试。

### R29：模式匹配类型的字符串地址比较

位置：`src/parser/query_binding.cpp:776` 至 `:781`。
条件表达式 `... ? "bytea" : "text"` 使 `const auto target` 成为const char*，后续 `target=="bytea"` 比较字符串地址而非内容。两个相同字面量是否共享地址不能作为类型判断契约；编译器已输出 `comparison with string literal results in unspecified behavior [-Waddress]`。
因此bytea/text ESCAPE的校验可能依赖常量合并行为。本轮当前O2构建没有独立SQL失败证据，不能把可移植性风险写成已经出现的错误结果。
修复：将target声明为std::string/std::string_view，或先保存显式isBytea布尔值再选择目标类型，避免地址比较；继续使用已解析的操作数类型处理ESCAPE输入。
验收：bytea和text模式、正确/错误类型的ESCAPE、unknown字面量和参数、空源准备阶段；至少在正常优化及关闭常量合并的构建中运行同一语义控制，保持合法输入和错误SQLSTATE一致。

### R30：协议元数据分析丢失连接会话上下文

位置：`src/network/NetworkServer.cpp:1932`、`:2435`、`:5024`、`:5848`；`src/commands/TableManage.cpp:32986`；`src/expression/function_namespace.h:15` 和 `src/expression/expr_helper.cpp:529`。
表和函数位于独立非public schema，连接已正确设置search_path。两个原注册驱动失败，追加完整协议消息后确认不是RowDescription字段格式问题：

| 合法SQL | 当前程序的扩展查询响应 | PG18.6 |
| --- | --- | --- |
| `SELECT sum(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS total,avg(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END) AS mean FROM inputs` | Parse即42P01，声称inputs不存在 | 正常描述total为int8、mean为numeric |
| `SELECT sum(CASE WHEN r<'alpha' THEN enum_argument_writer(id) ELSE 0 END) AS total FROM ranks` | Parse成功后Describe报42804，声称CASE混合integer/text | 正常描述total为int8 |

相同BETWEEN语句通过Simple Query可执行；改为i>0的描述控制也成功。第二条语句在新鲜连接首次Describe失败，紧接着完全相同的描述却成功，两次均没有执行writer。
原因是Parse的整数输入校验和Describe类型推导没有建立currentSession：关系binder退回public查找，函数元数据也退回public并把未知函数猜为text。错误回滚abortActiveTransaction反而setCurrentSession且未恢复，使首次错误改变后续分析环境。

修复：向完整元数据分析链显式传递Session，或在协议入口设置异常安全、短寿命的会话守卫，覆盖Parse/Describe及依赖会话的类型、函数、关系查询。错误和成功出口均恢复上下文，不能靠制造错误或重试初始化，也不能删除Main已有生命周期恢复机制。描述仍只能查元数据，禁止执行用户表达式获取类型。
验收：保留两个原驱动全部断言；新鲜连接、非public schema、首次扩展查询、成功/错误前后相同SQL、同名对象和临时schema、连接池模式，以及Describe不执行writer的控制。独立诊断实例均已关闭，Root输入没有修改。
后续原TEMP驱动同样实际exit1：普通`SELECT id FROM prep_empty_...`可Parse，而`SELECT (SELECT id FROM prep_empty_... LIMIT 1)`报42P01。`NetworkServer.cpp:5859 → :1746`调用prepareBoundQuery时也没有当前会话，关系查找遗漏真正的session-temp namespace；此控制也须纳入修复验收。
两个集合查询驱动的含表描述同样在本轮注册回归报42P01；源码路径为`NetworkServer.cpp:5858 → :1687 prepareBoundQuery`，共享同一依赖currentSession的关系查找，纳入R30统一修复和验收。这里的集合路径关联来自原运行结果加源码追踪，没有另做独立集合故障实验。

## 历史失败与待判定项目

| 项目 | 已有证据及正确解释 | 下一步 |
| --- | --- | --- |
| table_owner_atomicity | 历史失败在本轮原注册回归再次出现：第105行健康重试返回非 OK；实际 status 未记录，不能据此认定数据回滚失败 | 记录 status、schema/catalog 发布阶段和 peer generation；确认原因后修复并保留重试断言 |
| owner 测试覆盖说明 | 第82行称 class 先发布、attribute 后失败，但当前 persistAll 在所有写入前检查目录，且 map 顺序为 attribute 在前 | 保留原控制，另加真实分阶段发布失败控制，不能用现有绿色断言证明部分发布恢复 |
| index_corruption / EXISTS | 本轮再次出现原fixture期望XX001、实际t；EXISTS固定读取健康heap，没有证明它访问了被破坏的B-tree | 先核对所要求的访问计划；若接入公共 planner，再验证错误传播，不能无条件检查未访问索引 |
| ALTER ARRAY | 前次同源码专项在建立两个索引后执行 `ALTER TABLE array_values ALTER COLUMN i TYPE BIGINT[]` 超时；本轮注册驱动返回成功，不能抹掉前次失败 | 保留失败和原10秒期限，定位等待/错误边界，不将超时直接归因于环境 |
| EXPLAIN / JOIN | 本轮再次命中OFFSET 1负向fixture：查询实际可正确返回4行，但该路线的EXPLAIN又缺少JOIN/scan子图 | 分别修正合法行为的断言和真实执行图；私有补丁还存在关联重绑计数遗漏，尚未合入 |
| CTAS / background checkpoint | 原正式回归存在58030失败，本轮ctas_test通过，不能据此抹掉历史失败 | 按用户要求暂缓专项调查，保持未修复 |
| HAVING 非 BOOLEAN | 新差分 `HAVING 1` / `HAVING 'notboolean'` 实际成功空结果，PG18分别42804/22P02 | 追踪实际 aggregate/HAVING 消费路径，共用 BOOLEAN 绑定；保留空输入和准备阶段控制 |
| ORDER BY 重复别名 | 新差分 `SELECT id AS x,v AS x FROM d ORDER BY x` 与无FROM双 x 均未返回预期42702 | 在 ORDER BY 名称解析阶段检查多重匹配，区分合法同一表达式引用与歧义；核对标签及行序 |
| 物化视图刷新 | 本轮原注册 `matview_refresh_protocol_e2e_test.py:93` 执行 `REFRESH MATERIALIZED VIEW unpopulated_mv` 时socket超时 | 保留原期限和fixture，下一步记录刷新阶段及等待边界；不能直接归因环境，也不在本轮重开用户暂缓的CTAS/后台checkpoint专项 |

## 本轮注册失败清单

| 原驱动 | 实际失败 | 归属与处理 |
| --- | --- | --- |
| stale_temp_startup_recovery_test | WAL恢复异常中止 | R19 |
| table_owner_atomicity_test | 第105行健康重试非OK | 上表，实际status尚待记录 |
| enum_aggregate_argument_binding_protocol_e2e_test.py | Describe 42804，未返回RowDescription | R30，原断言正确 |
| aggregate_result_root_owner_protocol_e2e_test.py | Parse 42P01，未返回RowDescription | R30，原断言正确 |
| set_operation_type_metadata_known_gap.py | 含表的集合描述42P01，随后Bind26000、SAVEPOINT25P02 | R30调用链关联，保留首错与后续影响 |
| set_operation_clause_ownership_protocol_e2e_test.py | 含表UNION的描述42P01，OID及标签缺失 | R30调用链关联，不能仅修改预期OID |
| matview_refresh_protocol_e2e_test.py | 刷新unpopulated_mv的socket超时 | 上表，原因未定 |
| index_corruption_protocol_e2e_test.py | EXISTS返回t而fixture期望XX001 | 访问计划与断言问题，不能据此宣称返回了损坏行 |
| explain_join_protocol_e2e_test.py | 合法OFFSET 1返回成功而fixture期望错误 | 分开修正过时断言与EXPLAIN执行图缺口 |
| temp_prepared_access_protocol_e2e_test.py | TEMP标量子查询Parse 42P01 | R30，普通TEMP SELECT控制成功 |

关键原始日志已摘录到[证据文档](review-evidence-2026-10-13.md)。以上失败没有通过改断言、扩大超时或替换原驱动消除。

## 实施顺序

优先修页压缩数据完整性、连接池生命周期/等待/关停、回滚备份/catalog读取、参数精度和已确认的数据修改错误，再处理数值与窗口比较、frame、NTILE/LAG及逻辑变更预览；每个问题独立提交并加入能先失败后通过的回归。
修复后先跑匹配的专项及相邻测试，再跑注册回归；性能、并发和 sanitizer 结果分别登记。
本轮的文档提交不改变上述问题的未修复状态，也不改变原 273 项完成统计。
