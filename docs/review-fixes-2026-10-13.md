# 2026-10-13 审查问题修复进度

最新用户暂缓：[R180 POSITION语法/参数约定](issue-review-r180-position-call-identity.md)按最新“skip it”停止实现。
两侧各24次观察及修改方法继续保留；没有生产修改、注册测试或修复通过声明，普通continue不恢复本项。
下一项独立审查为字符串REVERSE的重复UTF-8前缀扫描；目前仅源码观察，尚未验收修复。
已接受仍为下述R179有限修复；R164及总273继续未完成，其他用户暂缓不恢复。不push，Actions禁用。

最新有限修复：[R179 STRPOS声明](issue-review-r179-strpos-signatures.md)已完成真实TEXT/TEXT签名、
错误阶段/空输入/参数推断/同名重载及opaque native覆盖修复。PG294全0；旧294有140断言差异、
旧native76有49失败，均完整记录，非140/49个独立缺陷。Root及accepted-only clean各16入口实际全0，
含native76、完整294/80/190/654和三个本轮fresh编译、新链接当前库的真实Main。
每侧71对象13程序/127头独立核对；Root normal3CPP并链接、repeat零编译/零链接，clean显式3CPP加55已核对donor，
未冒称clean normal/repeat或donor fresh；全部2201已接受源码投影与tested clean一致，Root2320保留原有其他改动。
跟踪600份MD、918native+566注册Python+3Main=1487入口；工作区603份MD、924+572+3=1499，
原12未接受入口及三份文档仍分离。[R180 POSITION](issue-review-r180-position-call-identity.md)仍开放，不用本项关闭。
这不是1487全量/完整72/TLS/整库sanitizer验收；R164两次超时仍未解决，R177等用户暂缓不恢复。
原273仍22 complete/166 partial/70 unverified/15 deferred，总目标未完成；不push，Actions禁用。

最新有限修复：[R178空字符串REPEAT](issue-review-r178-repeat-empty-input.md)在参数校验后直接返回，
避免按次数进行空追加；不是兼容性错值修复，PG也有相似旧耗时。PG完整80项通过，
工作区及accepted-only clean各11入口（8native/3协议）实际全0，包含80/190/654完整协议。
原生增加4个空输入边界控制；首轮8通过/3启动失败及数据目录驱动修正记录保留。
跟踪598份MD、917native+565注册Python+3Main=1485入口；工作区601份MD、923+571+3=1497，
原12个未接受入口及三份文档仍分开保留；这不是1485全量、完整72、TLS或整库sanitizer验收。
R177按用户要求暂缓，不恢复R173/R156等分支；R164超时仍未解决，非新增暂缓。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成；不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

## 当前版本：R176有限排序修复，完整矩阵仍有失败

[R176](issue-review-r176-ordinary-aggregate-order-keys.md)的有限源码修复已本地提交`a35658c0`：
保留全局SUM/COUNT真实ORDER键，纯绑定及错误阶段、FILTER/NULL需求、缓存typed排序、
共享参数/重复键和方向身份有native61及完整协议190回归，两侧专项均0。
完整PG190对照0；旧版本190次完整观察59通过131失败，不把这些重复模式称为131个独立缺陷。
工作区完整72入口实际全0；干净版两次完整72均外层1，各71项通过，均在R164入口socket超时，
无完整956成功footer，清理出现延迟响应错位。超时根因未确定，也未证明与R176无关；
保留全部失败，不提高超时、不改持久化、不移除断言，不宣称双侧完整矩阵通过。
每侧35native/34协议/3真实Main顺序及原70断言保留；每侧97对象/39程序及源文件独立核对。
三个Main采用accepted测试体的路径投影及显式私有数据目录，不夹带工作区session测试改动。
Root fresh58、normal wrapper复用当前58链接、repeat零编译/零链接；clean fresh2+56当前header
donor源自独立fresh58 V3，未伪造clean常规build/repeat。提交的2198源文件与tested clean逐blob一致。
跟踪597份MD、917native+564注册Python+3Main=1484入口；工作区600份MD、923+570+3=1496，
原12个未纳入版本的测试入口和三份文档分离保留。这不是1484全量、TLS或整库sanitizer通过声明。
R176的GROUP BY排序、更多类型/算子/参数/query owner边界仍开放；R164超时仍普通未解决，非用户暂缓。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成。
R173、R156及其他用户暂缓分支不恢复；不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

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

对应[原审查报告](code-review-2026-10-13.md)。修复起点为`b54cc7f0`；原始观测和失败日志保留，不以修复后的结果覆盖历史证据。

每项修复使用独立commit，包含实现、能复现原问题的回归及问题文档。此前独立worktree工作逐项合入master；当前在主工作区继续逐项修复。所有提交留在本地，由用户push。GitHub Actions保持禁用。

## 执行计划

1. 修复页压缩/插入容量、连接池生命周期和等待、协议会话上下文，验证数据保持及正常关停。
2. 修复写入RETURNING/ON CONFLICT、SQL EXECUTE求值，核对错误阶段、事务清理和副作用次数。
3. 修复目录加载/值快照、失败回滚恢复、TEMP启动和维护任务状态，保留正常格式及恢复语义。
4. 修复数值精度、窗口类型比较/frame/排名偏移、递归CTE和严格语法。
5. 修复逻辑预览编码/错误传播、测试注册，以及原回归失败与观察项。
6. 合入后运行新回归与相邻测试，最后重新执行完整注册回归、文档/状态检查，登记真实终态。

用户此前暂缓的安全/TDE及CTAS/后台checkpoint专项仍暂缓；本轮普通数据完整性与现有回归失败照常修复。CMake、TLS及sanitizer的实际验证范围按结果记录。

后续[R104 网络值转换](issue-review-r104-network-value-conversions.md)实际PG/
Root649、原738及完整SDK、21native/17协议/3真实Main通过；五CPP实际4/3/2
增量normal/repeat及核心/最终独立核对均0，按单项本地提交。旧649/419、SDK134、
首版582/3与649/58、第二版649/1保留；无新全量，原273不变，R98/R99不混入。

后续[R105 限定NAME→INET/CIDR](issue-review-r105-network-name-input.md)最终
PG/Root738、完整SDK及21native/16协议/3真实Main全过，自身真实全58/
normal/repeat、核心与最终独立核对均0，按单项本地提交。旧738/34、SDK134
保留；无新全量；R104后续独立修复，原273未关闭，R98/R99候选不混入。

后续[R102 INET/CIDR输入](issue-review-r102-network-input-notation.md)最终旧188/72、
672/389失败；当前188/ASan/UBSan、PG/Root672、2558对照及21native/16协议/3真实
Main通过，最终真实全58/匹配核心/fence均0，单项本地提交。首版672/23保留；
R98/R99不混入；R103/R104/R105后续各独立修复，原273不变，无新全量或借用R101证明。

后续[R101 MAC输入/宽度](issue-review-r101-macaddr-input-and-widths.md)旧75/38、
328/186失败，当前75、PG/Root328及236派生输入通过，二十完整native与十五完整
协议及三个真实Main通过；头缺陷修正后真实全58重编0，所有产物字节相同，
新匹配核心2/328及fence全0，单项本地提交。首版五个真实协议失败及测试编译
问题保留；R98/R99候选不混入，未运行新全量，原273状态不变。

后续[R100几何分隔符](issue-review-r100-geometric-coordinate-delimiters.md)旧最终
232组件104失败、230协议135失败；当前232、PG/Root230及十个完整native、七个
完整协议通过。初次准备超时和旧夹具134保留；不混入R98候选，原273状态不变。

后续[R97 BEFORE行赋值](issue-review-r97-typed-before-row-assignments.md)原106/65
失败保留；当前154、15个完整native及十个完整协议通过。新1392尚未全量验收，
原273状态不变，R96明确暂缓。下方1391完整终态不包含R97。

最新[冻结5091b39c完整终态](issue-full-registered-5091b39c.md)：1391入口各一次，
1390实际通过、1 TLS环境skip、0失败，实际退出0；全部冻结输入、58 receipt及
57 test库字节独立终态核对0。R92–R95合入后的组合已验证，不抹除原失败或
宣称真实TLS、间歇性超时根因及原273完成。下述未全量/失败均保留历史时点。

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

后续[R92缺失TOAST计数器](issue-review-r92-toast-missing-counter.md)已修复实际
写入报错仍破坏已提交长值的问题，补齐合法ADD/DROP重写初始化；30个完整native
通过，协议邻居18/20通过，CREATE/定长ALTER两个超时保留。新877 native+3 Main+
504 Python=1384组合未全量运行，下一项继续原完整普通DDL超时与其余未完要求。

前次完整终态为[冻结332bcff4](issue-full-registered-332bcff4.md)：2026-10-15
15:15 UTC实际退出1，1382入口全部各一次，1379实际通过/1 TLS环境skip/2失败。
全部冻结输入、58 receipt及57份test库字节独立复核0；新增14入口和旧六失败入口
本轮均通过。新失败是stored-function-owner末尾DROP断言和SQRT准备阶段CREATE
SCHEMA超时，不猜测为算法错误或同根因，不重开R51。原273状态不变，后续有限
TOAST修复见R92，普通DDL超时及其余原未完要求继续逐项修复/验证/本地commit。

前次完整终态为[冻结27bac6ee](issue-full-registered-27bac6ee.md)：1368入口，
1361实际通过、1个TLS环境skip、6失败，无遗漏/重复；全部冻结检查、58份
生产receipt及57份test库对象逐字节独立复核一致。R83/R84/R85六个新增入口均0。
该冻结的一个末尾DROP断言和五处超时完整保留。后续R90修复TOAST计数发布开销，
原完整COPY三次通过；另复查默认PG/LATERAL/CHECK通过，MV目标仍在SAVEPOINT
超时，三个通过不证明旧间歇性超时已修复。新1380组合未全量运行，R51仍暂缓。

前次完整终态[冻结d974c80d](issue-full-registered-d974c80d.md)：2026-10-15
01:25 UTC，1362入口、1359 PASSED退出（含1个TLS环境skip）、3 FAILED，
无遗漏/重复，全部冻结fence及独立58 receipt复核一致。上一轮两个失败经
R80 `6456ad4f`、R79 `7c294fe7`修复后，本轮原完整入口均0；R81 `a7f0e404`
真实恢复和完整DIV14也0。新失败为prepared DML末尾DROP清理断言、JOIN准备阶段
socket timeout、bound DML的ROLLBACK TO timeout；逐项保留，未宣布全绿。
先按原完整驱动/期限复现两个普通协议超时，再按确证根因修复；用户暂缓的
DROP/cache专项不自行重开，其余通用函数签名覆盖仍继续，不改原273总账完成状态。

[R83固定float8签名](issue-review-r83-float8-scalar-signatures.md)本范围已修复：
最终906签名/237原生回调及二十完整native全0，实际PG18.6与Root新951协议全0。
完整邻居协议合计16/17通过；完整PG驱动另在CREATE toasted_unicode_varchar
socket超时，精确SQL/栈和原期限保留，尚未确定根因，不称新组合全量通过。

## 逐项清单

| 项目 | 内容 | 状态 | commit / 验证 |
| --- | --- | --- | --- |
| R01 | NUMERIC SQRT | 已修复 | `91951e37`；1023原生控制O2/ASan/UBSan、46协议PG/Root及5完整native邻居通过，见[验证](issue-review-r01-numeric-square-root.md) |
| R02 | 窗口排序、peer、MIN/MAX | 已修复 | `491dae49`；最终28协议原24失败→PG/Root全过，168native、八完整native/十一完整协议通过；C/默认locale区别明确，见[验证](issue-review-r02-window-declared-comparisons.md) |
| R03 | 窗口分区的NUMERIC等价 | 已修复 | `cd3980fe`；原12native全失败/18协议11失败→PG/Root全过，七完整native/十完整协议通过，见[验证](issue-review-r03-window-partition-equivalence.md) |
| R04 | 窗口frame边界 | 已修复 | `c71a7526`；原最终307协议275失败→PG/Root全过；349native、78初始化控制、十五完整native/十八完整协议通过，方向/空区间/EXCLUDE/typed offset/嵌套变量及初始化次数保留，见[验证](issue-review-r04-window-frame-boundaries.md) |
| R05 | NTILE分桶 | 已修复 | `aeaa3257`；24 native与18 PG/Root协议控制及六完整窗口native通过，见[验证](issue-review-r05-ntile-distribution.md) |
| R06 | NTILE非正参数 | 已修复 | `41d45c4a`；20native原8失败、72协议原57失败→PG/Root全过；九完整native/十八完整协议通过，保留NULL、参数、逐分区求值次数，见[验证](issue-review-r06-ntile-argument.md) |
| R07 | LAG/LEAD偏移 | 已修复 | `c94a8714`；原native120中36失败、原合法负偏移协议触发退出→最终122native/152 PG及Root协议全过；最终58当前生产对象、十五完整native/十九完整协议通过，见[验证](issue-review-r07-window-signed-offset.md) |
| R08 | binary float参数往返 | 已修复 | `2d600491`；Root标准构建及86协议控制通过；PG18.6与native邻居见[验证](issue-review-r08-binary-float.md) |
| R09 | 连接池等待期间引用寿命 | 已修复 | `2332382f`；原实现ASan失败；修复O2/ASan及重复测试通过，见[验证](issue-review-r09-pool-wait-reference.md) |
| R10 | 连接池跨数据库等待 | 已修复 | `cb934db9`；Root真实源新编译4控制通过；O2/ASan及重复验证见[验证](issue-review-r10-pool-database-capacity.md) |
| R11 | 窗口SUM中间溢出 | 已修复 | `b8020acc`；原实现UBSan红证据、PG/Root12控制48行及六完整窗口native通过，见[验证](issue-review-r11-window-exact-accumulator.md) |
| R12 | 测试注册 | 已修复 | `a6bb7ef7`；五个原驱动实际0；新增完整注册盘点及构建路由检查通过，见[验证](issue-review-r12-test-registration.md) |
| R13 | 逻辑预览NULL及字段编码 | 已修复 | `123ac811`；原native64中61失败、最终wire53中50失败→Root77native/53wire全过；十二完整native/七完整协议通过，带版本逐列类型/NULL编码及虚拟列空串修复，见[验证](issue-review-r13-logical-tuple-encoding.md) |
| R14 | publication读取错误传播 | 已修复 | 原成员读取五个错误实际失败；Root fresh58 与写入前保护、原逻辑解码及目录/owner九完整native全过，见[验证](issue-review-r14-publication-read-errors.md) |
| R15 | 关停唤醒顺序 | 用户暂缓，未修复 | 此前单独要求跳过的模块，不重开；不据此跳过普通注册集成回归 |
| R16 | catalog坏行及完整加载 | 已修复 | `0d85f6b9`/`a0d2efde`；691、cold正常API及原catalog_snapshot全通过，见[初始验证](issue-review-r16-catalog-complete-read.md)/[兼容修复](issue-review-r16-optional-catalog-directory.md) |
| R17 | 失败回滚恢复图像 | 已修复 | `9edf30ce`；候选fresh58、23完整native及20重复通过；Root fresh58、最终31完整native与完整PGprotocol实际0，见[验证](issue-review-r17-rollback-recovery-ownership.md) |
| R18 | catalog值快照 | 已修复 | `8f6e3c43`；候选fresh58及提交前/后8完整native通过，Root R14 集成 fresh58 后再次通过 snapshot/complete/cold/owner，见[验证](issue-review-r18-catalog-value-snapshots.md) |
| R19 | TEMP启动恢复 | 已修复 | `18e91f2b`；原失败134→0、候选八完整native和Root十二完整native/PGprotocol通过；新文件清理问题独立R40，见[验证](issue-review-r19-stale-temp-startup.md) |
| R20 | RETURNING子查询 | 已修复 | 原Root native29检查失败；PG/Root 35协议、七完整native及14完整协议全过，见[验证](issue-review-r20-returning-query-owner.md) |
| R21 | ON CONFLICT BOOLEAN条件 | 已修复 | `1463b6be`；119 native、75 PG/候选/Root协议及完整邻居、Root十二native/PGprotocol通过，见[验证](issue-review-r21-conflict-boolean-context.md) |
| R22 | 严格模式FROM/INTO | 已修复 | `799636f7`；36 native及拒绝控制通过；完整协议的独立R31后续通过，见[验证](issue-review-r22-dml-required-keywords.md) |
| R23 | 维护任务持久化门禁 | 已修复 | `07f71677`；49故障控制原28失败→0、ASan/UBSan、20重复与原管理器通过；Root十一native、五协议及完整DIV14通过，见[验证](issue-review-r23-maintenance-persistence-gate.md) |
| R24 | EXECUTE参数求值 | 已修复 | `1d5aac14`；59 PG/候选/Root控制、私树两批十一完整协议、Root九native/十三协议全过，见[验证](issue-review-r24-execute-argument-values.md) |
| R25 | Numeric::withPrecision | 已修复 | `021b3031`；原始12345→123失败保留；41控制O2/ASan/UBSan和原Numeric完整驱动通过，见[验证](issue-review-r25-numeric-precision.md) |
| R26 | 递归CTE类型/typmod | 已修复 | `7cae7c1f`；PG/候选/Root57控制及Root六完整native通过，Root SQRT46控制也通过，见[验证](issue-review-r26-recursive-cte-types.md) |
| R27 | 页压缩全部存活行 | 已修复 | `58f1ee3d`；原生26组O2/ASan/UBSan通过；SQL及进程重启通过，见[验证](issue-review-r27-page-compaction.md) |
| R28 | 页插入空间计算 | 已修复 | `261ffb86`；原始满页插入失败证据；198边界O2/ASan/UBSan及26压缩邻居通过，见[验证](issue-review-r28-page-insert-capacity.md) |
| R29 | LIKE ESCAPE类型比较 | 已修复 | `d7a3293d`；Root项目flags的-Werror=address通过；34绑定控制及相邻协议见[验证](issue-review-r29-pattern-escape-type.md) |
| R30 | Parse/Describe会话上下文 | 已修复 | `8ba70380`；Root标准构建及12控制通过；5个原失败驱动在候选通过，见[验证](issue-review-r30-session-metadata.md) |
| R31 | DELETE ONLY回退错删多行 | 已修复 | `eabfc340`；新12控制PG/候选、原R22完整协议、3 native/3协议/完整PGprotocol通过，见[验证](issue-review-r31-delete-only-typed-owner.md) |
| R32 | 科学计数法字面量的NUMERIC写入绑定 | 已修复 | `40e0a152`；203 native、110 PG/候选/Root协议及完整邻居通过；独立微小scale问题由R34关闭，见[验证](issue-review-r32-scientific-numeric-literals.md) |
| R33 | legacy窗口路径丢失最终ORDER BY | 已修复 | `41054ead`；最终44控制原39失败→PG/Root全过，独立CLI、八完整native/十七完整协议通过；中间别名失败均保留，见[验证](issue-review-r33-window-final-order.md) |
| R34 | 科学计数法NUMERIC算术scale | 已修复 | `a36efcb5`；120 native、123 PG/候选/Root协议，原111字节快照/known-gap断言及完整邻居通过，见[验证](issue-review-r34-scientific-numeric-arithmetic.md) |
| R35 | INSERT SELECT的ON CONFLICT分句边界 | 已修复 | `a196264d`；42 native、17 PG/候选/Root协议及完整邻居通过，原失败保留；执行owner另记R39，见[验证](issue-review-r35-insert-select-conflict-boundary.md) |
| R36 | gcd/lcm指数NUMERIC结果OID | 已修复 | `d1cd3cce`；63 native、61 PG/候选/Root协议，六native/五协议邻居通过，见[验证](issue-review-r36-gcd-lcm-result-types.md) |
| R37 | TEXT与NUMERIC算术类型检查 | 已修复 | `b5fc5b73`；109 PG/候选/Root协议与60native通过；Root真实fresh58、十一完整native/PGprotocol通过，见[验证](issue-review-r37-numeric-text-arithmetic.md) |
| R38 | 表列混合指数表达式投影 | 已修复 | `758c375b`；60协议原18失败→PG/候选/Root全过，36native及完整邻居通过；Root十一native/五协议通过，见[验证](issue-review-r38-scientific-column-projections.md) |
| R39 | INSERT SELECT冲突/复杂source执行owner | 已修复 | `2d66c751`；53native原39失败→0、原完整23协议PG/候选/Root全过，保留全部known-gap断言；直接UNKNOWN输出的中间回归已修复，Root十一native/五协议通过，见[验证](issue-review-r39-insert-select-owner.md) |
| R40 | TEMP清理误删空尾相似名称的持久表fork | 已修复 | `707a23f5`；原known-gap断言134→0、新强化file-owner与六完整native邻居通过；Root真实fresh58、十一native/PGprotocol通过，见[验证](issue-review-r40-temp-cleanup-file-ownership.md) |
| R41 | INSERT SELECT来源检查回归 | 已修复 | R39复查新增；38 native原13失败→最终53全过，七完整native和八完整协议（含PGprotocol）通过，见[验证](issue-review-r41-insert-source-preflight.md) |
| R42 | 索引故障测试的失败回滚合约 | 已修正回归夹具 | 原exit134→四完整native全过，五隔离重复通过；保留pending XID、修复后显式retry、原REINDEX/数据断言；未改产品，见[验证](issue-review-r42-index-rollback-fixture.md) |
| R43 | 函数属性与LANGUAGE/AS交错 | 已修复 | `0c3ebac4`；52native原36失败→候选/Root全过，Root真实重编/九native/十三协议通过，见[验证](issue-review-r43-function-attribute-order.md) |
| R44 | legacy窗口临时列名覆盖真实源列 | 已修复 | `80fe342e`；原39控制29失败→PG/Root全通过；十五完整native/二十完整协议通过，独立结果槽不修改源列身份，见[验证](issue-review-r44-window-internal-namespace.md) |
| R45 | 窗口聚合的表达式参数 | 已修复 | 原300协议287失败→PG/Root全0；typed参数/NULL/结果声明、frame推进与VOLATILE重算、LIMIT需求和REAL宽度已修，Root十四完整native/十七完整协议全0，见[验证](issue-review-r45-window-aggregate-arguments.md) |
| R46 | 原生/SQL schema 目录及事务 owner | 已修复，已合主分支 | 原 namespace 18 条断言失败，PG/候选46全过；最终35完整native和十一完整协议全0，中间同引擎/保存点/夹具及原默认协议失败保留，见[验证](issue-review-r46-native-schema-ownership.md) |
| R47 | BIT 与整数转换的真实 cast 关系 | 已修复，已合主分支 | 174native/160协议及十完整native/九完整协议全0；扩展172的BIT输出typmod另列R50，未放宽断言，见[验证](issue-review-r47-bit-integer-casts.md) |
| R48 | 故障注入回归的 pending rollback 合约 | 已修正夹具，已合主分支 | 四个原完整驱动失败；加强后的四驱动在主分支五轮20次全0，候选六完整native全0，未改产品，见[验证](issue-review-r48-pending-rollback-fixtures.md) |
| R49 | INSERT SELECT 物化视图 source lowering | 已修复，已合主分支 | 原完整native错误0A000/原41协议22失败→PG/候选41全0；parser24、十完整native/九完整协议全0，见[验证](issue-review-r49-materialized-insert-sources.md) |
| R50 | BIT/VARBIT 的协议及目录长度描述符 | 已修复，已合主分支 | 原新60全部失败→PG/候选全0；原完整172、42native及七完整native/八完整协议全0，保留未通过邻居及全部known-gap断言，见[验证](issue-review-r50-bit-type-modifiers.md) |
| R51 | recovery graph 邻居的 DROP DATABASE 观察 | 用户暂缓，未修复 | 原失败与早期通过保留；新固定诊断第13次复现状态25(DATABASE_IN_USE)，缓存/目录读锁竞争仍未修正；按用户要求跳过，不改原断言或宣称清零 |
| R52 | 原生测试终态输出换行 | 已修正夹具，已合主分支 | 使用真实换行分隔测试输出与PASSED marker；完整原驱动重新编译执行0，原完整日志及严格解析遗漏保留，见下方复验记录 |
| R53 | INSERT SELECT 的INTERVAL字段修饰符上下文 | 已修复，已合主分支 | 原27中1失败/新133中9失败→PG/候选全0，78纯binder控制及六完整native/七完整协议全0；标量与数组解析阶段分开，见[验证](issue-review-r53-insert-interval-context.md) |
| R54 | 聚合表达式中未提供参数的错误优先级 | 已修复，已合主分支 | 实际PG18.6新54全0，候选原13失败→0；原完整聚合scope及五完整协议全0，保留合法Extended/SQL PREPARE，见[验证](issue-review-r54-missing-parameter-priority.md) |
| R55 | BYTEA 存储别名错误目录身份 | 已修复，已合主分支 | 新28协议原17失败→PG/候选全0；原完整patterns及七完整协议/六完整native全0，7个目录/cold/显式历史修复控制全0，见[验证](issue-review-r55-bytea-catalog-identity.md) |
| R56 | ARRAY_AGG 窗口函数的声明/接收器 | 已修复，已合主分支 | 原42中37失败→PG/候选最终45全0；33native、七完整native/六完整协议全0，原window_type通过，数组值输入仍0A000，见[验证](issue-review-r56-window-array-aggregate.md) |
| R57 | pending rollback后的协议连接中断 | 已修复，已合主分支 | 原进程SIGABRT→加强完整驱动及五次重复全0；五完整native/四完整协议全0，原默认协议role_rows失败保留，见[验证](issue-review-r57-pending-rollback-protocol.md) |
| R58 | 两个协议驱动的启动连接失败 | 原样复验通过，历史原因未定 | 原errno103保留；两个原完整驱动在主分支按默认超时独立复跑0，不宣称已修复启动竞态，见[完整记录](issue-full-registered-f4087f5d.md) |
| R59 | 新组合物化视图target协议超时 | 原样复验通过，历史原因未定 | 71个原target控制及source/事务控制主分支原样复跑0，原超时保留；来源55000缺陷归R49，见[完整记录](issue-full-registered-f4087f5d.md) |
| R60 | 窗口routine的真实身份/标量角色 | 已修复，已合主分支 | Root本地2ae05d2d；集成真实重编后十五完整native/十完整协议全0；PG73全0、原73中53失败→最终全0；59纯metadata、六完整native/十完整协议全0，第一轮8嵌套失败保留；原冻结完整结果独立保留，见[记录](issue-review-r60-window-routine-identity.md) |
| R61 | 普通CREATE TABLE初始化与I/O状态 | 已修复，已合主分支 | Root本地977e71e8；集成真实重编后十五完整native/十完整协议全0；独立核心commit为7ad3ccca，原EIO误报/成功返回已修；首轮完整门禁因R66崩溃保留，修R66后十三完整native/六完整协议及五次完整七故障重复全0，见[记录](issue-review-r61-create-initialization-io.md) |
| R62 | 标量函数的参数签名静态校验 | 原四组及R83十八固定名字，广义范围partial | ABS/upper/lower/length真实签名、同名用户匹配及SQL PREPARE入口；原126中86失败→最终132 PG/Root全0，100native及十一完整native/十一完整协议全0；十八固定float8名字另由R83独立验收，其余回调/UDF通用签名仍待覆盖，见[记录](issue-review-r62-scalar-signatures.md) |
| R63 | DEFAULT的普通值表达式角色 | 已修复，已合主分支 | Root本地3ce5731b；集成真实重编后十五完整native/十完整协议全0；原33中17失败→PG/候选全0，22native及六完整native/十完整协议全0，合法DEFAULT/域/事务/长表达式保留；第一轮legacy仍9失败保留，见[记录](issue-review-r63-default-expression-role.md) |
| R64 | 九个旧测试跨DDL保留过期目录引用 | 已修正夹具，已合主分支 | Root本地ef9e460e/6a4f0e9e/c6717180；集成真实重编后十五完整native/十完整协议全0；首三项150e5440、扩展六项ae583d88；新增runtime/sequence/trigger后累计九个原完整主分支夹具全0，保留当前/旧代阳性及全部原SQL/数据/故障断言；新执行器组合另验，见[记录](issue-review-r64-catalog-generation-fixtures.md) |
| R65 | 未执行mutation图的child provider生命周期 | 用户暂缓，未修复 | 用户要求跳过；原五模式exit134/134/134/134/139及未验收改动已另存，不合入产品。部分模式仍失败，不能宣称修复；见[暂缓范围](deferred-scope.md) |
| R66 | 后台写回的目录错误逃出工作线程 | 已修复，已合主分支 | Root本地9e32343d；集成真实重编后十五完整native/十完整协议全0；固定真实目录EIO原主分支SIGABRT→候选dirty保留/worker存活/真实刷盘恢复全0；十三完整native、六完整协议、五轮十次控制全0，见[记录](issue-review-r66-background-catalog-errors.md) |
| R67 | CREATE OR REPLACE VIEW既有列定义合约 | 已修复，已合主分支 | Root ca554c2f；PG80全0、原80中38失败→候选/Root全0；12严格拒绝/无副作用/旧代/冷目录，候选及Root各八完整native/九完整协议全0；额外完整PG驱动的57014已在无R67产物复现，R69独立验收，见[记录](issue-review-r67-view-replacement-contract.md) |
| R68 | ABS整数范围及空输入常量分析 | 本范围已修复 | 原67wire中23失败、40native中22失败→最终PG67/Root67及加强42native全0；九完整native/五完整协议0，含原132签名邻居，保留首六遗漏/后两UNKNOWN回归，见[记录](issue-review-r68-abs-integer-bounds.md) |
| R69 | 完整协议夹具遗留短会话超时 | 已修正夹具，已合主分支 | Root a3b3b4b9及补齐d52779a0；清理321/123/456ms探针，保留974原断言新增6条AST断言；候选与Root各完整PG/真实取消协议/CLI串行全0，首轮不完整及并行失败保留，见[记录](issue-review-r69-protocol-timeout-fixture.md) |
| R70 | 全文类型的真实目录与协议身份 | 本范围已修复 | tsvector/tsquery及数组补齐真实OID/字段；原67native中64失败、10wire全失败→当前全部0，五完整native/十一完整协议0；不代表全文功能完成，见[记录](issue-review-r70-text-search-type-identities.md) |
| R71 | BEGIN错误发布其他数据库目录 | 本范围已修复 | 真正目标范围及58实际重编；原三个完整generation/snapshot失败与三个事务邻居全0，真实EIO/pending/重试新控制0，保留初始夹具generation冲突，见[记录](issue-review-r71-transaction-catalog-scope.md) |
| R72 | RENAME对同一canonical锁重复获取 | 本范围已修复 | 按真实mutex身份去重并使用实际持有锁；原完整native及协议全0，不改42P04或任何sentinel，见[记录](issue-review-r72-rename-alias-lock.md) |
| R73 | 纯常量查询的类型绑定/折叠来源 | 本范围已修复 | 最终25native/29 PG与Root协议0，原完整extended及共十二完整native/十完整协议0；保留中间UNKNOWN比较遗漏和真实physical fence，见[记录](issue-review-r73-independent-binding.md) |
| R74 | 两个owner夹具的bootstrap/COW/fault角色 | 已修正并验收夹具 | 两个原完整入口及七邻居native0；保留完整字节、当前generation、真实脏冲突、目录58030/锁清理及健康重试，R75产品边界独立提交，见[记录](issue-review-r74-owner-fixture-generations.md) |
| R75 | owner操作的目录读取错误与锁释放 | 本范围已修复 | 58030按IO_ERROR接口返回，其他typed error保留；作用域锁保证清理，两个完整owner及七邻居native0，不改角色/policy，见[记录](issue-review-r74-owner-fixture-generations.md) |
| R76 | restore期间ParameterStatus目录观察崩溃 | 本范围已修复 | 实际58生产单元及标准repeat0；新17控制与完整PG/取消协议/CLI/COPY五入口0，当时DIV14冲突后由R78处理；不改变权限决策，见[记录](issue-review-r76-restore-parameter-status.md) |
| R77 | restore在staging前过早淘汰目录 | 本范围已修复 | 新native原exit134→0；十完整native/五完整协议0，保留真正publication冷读冲突，不改变ownership/管理员判断；原DIV14夹具另记R78，见[记录](issue-review-r77-restore-catalog-staging.md) |
| R78 | 恢复任务轮询的publication冲突 | 已修正并验收夹具 | 完整DIV14、新五组负控及真实restore-stage三个入口0；仅RESTORE原30秒内精确55P03重试，不改管理员规则/成功数据与OID断言，见[记录](issue-review-r78-restore-poll-publication.md) |
| R79 | LENGTH原生二进制别名的签名身份 | 本范围已修复 | 新30控制19失败→0，原九控制及十三完整native/十完整协议0；PG六组语义对照0，签名身份与raw codec分离，见[记录](issue-review-r79-native-binary-length.md) |
| R80 | 未绑定列保护晚于参数类型推断 | 本范围已修复 | 新17 native原8失败→0，原窗口44 PG/Root全0；十一完整native/八完整协议0，不放宽真实TEXT算术，见[记录](issue-review-r80-unbound-scalar-inference.md) |
| R81 | staging消失被当成恢复作业完成 | 已修正并验收夹具 | 首轮三入口、五次真实恢复及七个完整串行协议全0，复用R78精确轮询，保留20秒/真实窗口/数据断言；未改产品，见[记录](issue-review-r81-restore-stage-completion.md) |
| R82 | 新冻结中的普通协议间歇性超时 | COPY本路径修复，其它继续定位 | 原五处超时保留；原COPY诊断捕获auto-ANALYZE及约5.615秒慢sync，后续[R90](issue-review-r90-toast-durable-id-reservations.md)保留持久化屏障后原完整COPY三次通过；其它复查三过一SAVEPOINT超时，不冒共享根因，见[冻结及方法](issue-full-registered-27bac6ee.md)和[原观测](issue-full-registered-d974c80d.md) |
| R83 | 十八个固定float8标量的声明/回调所有权 | 本范围已修复，整体回归未全绿 | 新906签名/237回调控制及二十完整native全0；实际PG18.6与Root新951协议全0；完整协议16/17通过，完整PG在CREATE超时未解决；保留中间OID/前端UDF失败，见[记录](issue-review-r83-float8-scalar-signatures.md) |
| R84 | CEIL/CEILING/FLOOR输出类型与opaque回调 | 本范围已修复，整体回归未全绿 | 旧274/144及266/138失败保留；当前310回调/metadata与Root/PG266通过；21个不同native均有完整通过记录，首批DROP清理134保留；完整协议16/17通过，COPY的CREATE超时未定因，见[记录](issue-review-r84-integral-rounding-result-types.md) |
| R85 | SIGN的numeric/decimal NULL结果域 | 本范围已修复 | 旧21/4失败、加强25/6失败保留；当前25与十一完整native/八完整协议全0，PG/Root对应20协议全0；原完整PG/COPY超时和全注册组合独立保留，不扩为通用SIGN签名完成，见[记录](issue-review-r85-numeric-sign-null.md) |
| R86 | ARRAY_POSITION空数组与NULL起点 | 本范围已修复 | 正式旧基线208/32失败、20查询/2失败；新208、九完整native、七完整协议及PG对应80控制全0；不吞非法非NULL起点或改变opaque callback，见[记录](issue-review-r86-array-position-empty-null-start.md) |
| R87 | MOD的有限整数宽度/numeric NULL结果域 | 本范围已修复 | 旧254/126失败、10查询/4失败；最终254+153、十四完整native/十一完整协议及PG130全0；首版121/90个native回归保留，最终保护无声明复杂输入，不扩完整签名，见[记录](issue-review-r87-mod-result-domains.md) |
| R88 | ANALYZE 重复验证/相同长文本比较开销 | 本范围已修复，COPY 未关闭 | `813f5105`；同32长值的真实locale比较243→1，全部统计值不变；96列/运行控制、十四完整native及六完整统计协议通过；原完整COPY仍line262超时，见[记录](issue-review-r88-analyze-sort-comparison-cost.md) |
| R89 | ROUND 单参数的有限结果声明 | 本范围已修复 | 旧124/48失败、91协议/44失败；最终124+97、十七完整native/十二完整协议及PG91全0；首版native97/6回归保留并收窄输入域，不扩完整签名/双参数/TRUNC范围，见[记录](issue-review-r89-round-single-result-domains.md) |
| R90 | TOAST ID 持久化区间预留 | 本范围已修复 | `66df32ec`；旧64次分配/64次发布→1次；首版UINT64尾部断言失败保留并修正；21不同完整native、原完整COPY固定三次及七完整协议邻居全0，另四历史失败入口三过一SAVEPOINT超时；不扩完整持久性，见[记录](issue-review-r90-toast-durable-id-reservations.md) |
| R91 | 合法非事务序列运行态与保存点映像复用 | 本范围已修复 | `332bcff4`；旧native16份/真实前端71份→当前各1份，保持nextval/setval不回滚及坏状态拒绝；23完整native及18不同完整协议通过；1382组合已完整终1，本项新增与MV通过、两项其它失败保留，见[记录](issue-review-r91-savepoint-sequence-runtime-images.md) |
| R92 | TOAST计数器缺失破坏已提交长值 | 本范围已修复，整体回归未全绿 | `4a4c7254`；原IO_ERROR仍重建计数器且原查询1→0行；当前INSERT/UPDATE拒绝而旧值不变，合法ADD/DROP五次重写通过；30完整native、18/20完整协议通过，两个普通DDL超时保留，见[记录](issue-review-r92-toast-missing-counter.md) |
| R93 | 串行/并行及旧GROUP BY布尔参数 | 本范围已修复 | `9fa6a39d`；旧408/213、68/36及152/56失败；408+68、21完整native/18完整协议邻居通过，原协议150/152两个SHOW失败由后续R94补齐，见[记录](issue-review-r93-boolean-aggregate-arguments.md) |
| R94 | 并行配置SHOW结果与Describe | 本范围已修复 | `ccaf0ac5`；旧10/5、40/33失败；当前10、PG/Root40、原完整152、六个完整native及十个完整协议通过，见[记录](issue-review-r94-show-parallel-workers.md) |
| R95 | 旧全局BOOL表达式及混合JSON聚合 | 本范围已修复 | 旧native118/56及观察60/15失败；当前118、PG/Root60、十个完整native及十个完整协议通过，原setup超时与C warning保留，见[记录](issue-review-r95-legacy-global-boolean-arguments.md) |
| R96 | 内置collation退出期生命周期 | 用户明确暂缓，未修复 | 原纯组件ASan退出1/heap-use-after-free保留，无生产修复；不等同R95 warning或R15，不扩大至全部类型，generic continue不恢复，见[记录](issue-review-r96-collation-exit-user-deferred.md) |
| R97 | 显式BEFORE行赋值的NEW/OLD类型与NULL | 本范围已修复 | 旧106/65失败，当前154及15个完整native/十个完整协议通过；这是StorageEngine扩展，不冒称完整PG触发器/1392组合验收，见[记录](issue-review-r97-typed-before-row-assignments.md) |
| R100 | 几何坐标分隔符的位置验证 | 本范围已修复，独立本地commit | 旧232/104与230/135失败；当前232及PG/Root230通过，十个完整native/七个完整协议通过；旧准备超时/134保留，见[记录](issue-review-r100-geometric-coordinate-delimiters.md) |
| R101 | MAC输入、EUI宽度转换与SQLSTATE | 本范围已修复，独立本地commit | 旧75/38、328/186；当前75、PG/Root328、236差分及20native/15协议/3真实Main通过；头缺陷修正后全58重编/产物逐字节相同/新核心2与328/fence均0，未冒新全量；首版314/5保留，见[记录](issue-review-r101-macaddr-input-and-widths.md) |
| R102 | INET/CIDR输入格式与类型边界 | 本范围已修复，独立本地commit | 旧188/72、672/389；当前188/ASan/UBSan、PG/Root672、2558对照及21native/16协议/3真实Main通过；最终真实全58/匹配核心/fence均0，首版672/23保留，不冒新全量，见[记录](issue-review-r102-network-input-notation.md) |
| R103 | NAME→MAC显式输入回归 | 本范围已修复，独立本地commit | 最终PG/Root366、完整SDK与21native/16协议/3真实Main通过；最终自身全58/normal/repeat/fence全0，旧366/30、SDK134及首版358/6、366/12保留，见[记录](issue-review-r103-macaddr-name-input.md) |
| R104 | 网络typed值转换与共同值结果 | 本范围已修复，按本项独立commit | PG/Root649、原738及完整SDK、21native/17协议/3真实Main全过；实际4/3/2增量normal/repeat与核心/最终核对均0。旧649/419、SDK134及首两版失败保留，不扩为完整类型族或原273完成，见[记录](issue-review-r104-network-value-conversions.md) |
| R105 | 限定NAME→INET/CIDR源身份遗漏 | 本范围已修复，独立本地commit | PG/Root738、完整SDK及21native/16协议/3真实Main全过；自身真实全58/normal/repeat、核心与最终核对均0。精准NAME身份不放宽隐式赋值，旧738/34与SDK134保留，见[记录](issue-review-r105-network-name-input.md) |
| R106 | COALESCE未选显式CAST的UNKNOWN输入分析遗漏 | 用户暂缓，未修复 | 2026-10-16“skip it”；原四条观察及未注册612控制（PG零失败/旧程序168失败）保留，无生产代码改动，不提交测试草稿为已验收修复；普通继续不重开，见[记录](issue-review-r106-coalesce-explicit-input-analysis.md) |
| R107 | 四个字符串长度函数的真实签名与计量单位 | 本范围已修复，独立本地commit | PG/Root654、607原生与24native/18协议/3真实Main均0；四CPP实际2/1/1增量normal/repeat及核心/最终核对0。旧654/392、607/440与首两版失败保留，不扩为通用签名或原273完成，见[记录](issue-review-r107-string-length-signatures.md) |
| R135 | TIMESTAMPTZ RETURNING缺偏移且非会话墙钟 | 本项有限修复完成 | PG134/旧73失败、正常1/repeat0、45完整入口全0，见[记录](issue-review-r135-timestamptz-returning-timezone.md)，仅协议呈现，标量微秒持久化仍开放 |
| R134 | 同名原生回调错误继承四组内置签名 | 已独立提交 | `4d4d1b39`；旧1320/922失败、修正后1320全过，正常1/repeat0、37完整入口全0，见[记录](issue-review-r134-core-native-callback-ownership.md)，通用其他callback仍开放 |
| R133 | 时间戳binary输入拒绝合法微秒 | 已独立提交 | `ecf632e9`；PG266/旧204失败，正常1/repeat0、核心266及31完整入口全0，见[记录](issue-review-r133-timestamp-binary-microseconds.md)，不升级旧heap或冒全日期域完成 |
| R132 | 通用SET TimeZone的TO被当成值 | 已独立提交 | `61cb28f6`；PG112/旧44失败，正常1/repeat0、fresh三Main及30完整入口全0，见[记录](issue-review-r132-timezone-set-to.md)，非全部GUC语法/事务完成 |
| R131 | TIMESTAMPTZ binary输入按session再次偏移 | 已独立提交 | `e782cda1`；PG158/两旧版各45失败，29完整入口全0，见[记录](issue-review-r131-timestamptz-binary-timezone.md)，非fractional receiver或heap升级 |
| R130 | 原生结果提示被UNKNOWN覆盖/浮点替换所有权 | 已独立提交 | `3c8c85d3`；旧705/1/smallint和168/53失败，当前168及35完整入口全0，见[记录](issue-review-r130-native-result-hint-precedence.md) |
| R129 | 原生投影类型提示丢isArray与旧TEXT夹具 | 已独立提交 | `c02aa584`；真实数组旧原生134、PG/旧SQL51均0，四处提示及夹具修复、26完整入口全0，见[记录](issue-review-r129-native-array-projection-types.md)，R109未重开 |
| R128 | TIME binary Bind拒绝微秒和24时，范围/长度状态混淆 | 私有接收codec及结构化inputSqlState独立修复 | PG158全过/旧完整132失败；初版旧fixture112项后缺描述符中断保留并强化聚合，最终55完整入口全0，见[记录](issue-review-r128-time-binary-parameter.md)，不升级旧heap或TIME[]/其他时态接收 |
| R127 | 直接TIME投影过高precision无截断/通知，text未归一 | 值及真实AST声明有效6、Q/P结构化通知独立修复 | PG276/旧246失败、252原生旧225失败，54完整入口全0；Parse一次、Describe/Bind/重复Execute/多行不重发，原122核心语义匹配但不伪造PG内部诊断来源，见[记录](issue-review-r127-time-excess-precision.md)，DDL/组合角色另项 |
| R126 | 非法TIME关键字precision晚报错或错误接受 | 单个无符号int32语法及解析错误桥独立修复 | PG326/旧296失败、严格549原生旧217失败，首版协议19项后重置保留并修正；最终52完整入口全0，原122剩2条TIME(7)通知/typmod，见[记录](issue-review-r126-time-precision-grammar.md)，不混同具名语义或高精度warning |
| R125 | 合法TIME(p)转换透传且表达式typmod丢失 | 0–6值与真实声明独立修复 | PG830/旧830失败、339原生及最终50完整入口全0，Main复用未改TU后重连当前库；原97全匹配/122仍12边界差异，见[记录](issue-review-r125-time-precision-value.md)，不升级旧heap或全部分析/warning |
| R123 | 时间类型scalar/array列precision在schema/目录/结果中丢失 | 合法声明元数据独立修复 | 782原生及104协议，首版六个R124失败保留；不改变4/4/8/8物理宽度，有限终态见[记录](issue-review-r123-temporal-column-precision.md)，显式CAST/过高precision通知/旧行小数另项 |
| R124 | 表投影NULL::timetz被误作列名，报42703 | 单项路由版本与验收见记录 | 实际PG96全过、旧24失败；只补既有TIMETZ后缀路由，原R123六个失败断言保留，见[记录](issue-review-r124-timetz-postfix-projection.md)，非完整TIMETZ存储/codec |
| R121 | TIME默认基本clock输入透传非法/未舍入文本 | 类型输入及text Bind独立修复 | PG177/旧130失败、90native、最终43完整入口全0；原32全匹配/97仍21精度差异，见[记录](issue-review-r121-time-input-normalization.md)，完整语法、显式精度、cast eligibility/heap另项 |
| R119 | 时间戳小数秒失败哨兵被编码成固定远古epoch | 发送器独立修复与验收 | checked整数微秒/时区与真实数组；PG232/旧144失败、235低层及共享41终态见[记录](issue-review-r119-timestamp-binary-result.md)，跨日依赖先由R120单独修复，旧失败不删 |
| R120 | 闰年年初DISCONV可返回非法月份，时间戳进位输入报22008 | 上游日期修复单独验收 | 独立枚举全域3652059天、实际PG152/旧48失败；有限终态与版本见[记录](issue-review-r120-date-cycle-inverse.md)，不与R119发送器候选混合提交 |
| R117 | TIME微秒/24时及相应数组binary结果报0A000 | 本项单独修复与验收 | 真实int64微秒、严格clock范围与整数舍入；PG144/121低层及有限终态见[记录](issue-review-r117-time-binary-result.md)，文本/typmod/4字节heap另项，不伪装全时态完成 |
| R118 | 时态视图物化沿用VARCHAR布局，NULL插入也报INVALID_VALUE/XX000 | 本项单独修复与验收 | 五种时态类型使用真实工厂，PG186全过、旧60/首版36失败；当前实现、有限终态和独立版本见[记录](issue-review-r118-temporal-view-materialization.md)，不放宽存储校验或迁移表行 |
| R116 | TEMP VIEW完成标签错误返回CREATE TEMP/OR或注释词 | 本项单独修复与验收 | 纯解析真实非物化CreateViewStmt，PG54全过、旧14失败；有限回归与版本见[记录](issue-review-r116-view-command-tags.md)，CTAS/物化与其他视图问题不扩大处理 |
| R113 | INTERVAL及非NULL元素数组binary结果报0A000 | 本项单独修复与验收 | 真实16字节独立时钟/天/月，checked parser和双向无限；PG171/117原生及有限回归见[记录](issue-review-r113-interval-binary-result.md)，binary Bind与其他codec仍开放 |
| R114 | attlen/RowDescription泄漏heap长度或遗漏TIMETZ | 本项单独修复与验收 | 声明逻辑长度、守卫旧目录升级、冷读无IO；PG28/102原生与有限回归见[记录](issue-review-r114-physical-attribute-length.md)，R115先独立提交，其余目录/视图问题仍开放 |
| R115 | 含数组的SELECT *丢失CHAR/VARCHAR typmod | 本项单独修复与验收 | 仅规范真实物理列的标量类型别名，PG228零失败、旧13失败；有限门禁与独立版本见[记录](issue-review-r115-scalar-alias-projection-metadata.md)，不混入R114或保留候选 |
| R110 | 非 NULL 数组请求二进制结果时报0A000 | 本项单独修复与验收 | 真实内置数组身份、维度/下界、NULL和已有元素发送 codec；实际参考110零失败、旧75失败，182低层控制与独立版本见[记录](issue-review-r110-array-binary-result.md)，其余标量 codec/二进制输入仍开放 |
| R112 | 具名 BPCHAR/BIT 数组无 typmod 输入被截为长度1 | 本项单独修复与验收 | 实际参考57零失败、旧44失败；保持关键字默认长度和显式 typmod，独立版本与有限门禁见[记录](issue-review-r112-unconstrained-named-array-input.md)，不混入 R109/R110 候选 |
| R108 | ARRAY_POSITION绕过typed元素相等 | 本范围已修复，独立本地commit | 第五版354/258、原208/80/array库/654及32native/29协议/3真实Main共64完整入口均0，28补充JSONB及核心/最终fence0；旧258/158、320/183、各版失败和原API断言保留，不冒新全量，见[记录](issue-review-r108-array-position-typed-equality.md) |
| O01 | owner健康重试及测试覆盖 | 已修复 | `c9a1e3c3`；原全部owner断言、新真实peer/dirty冲突及10完整native提交前/后通过；Root R14集成fresh58后两完整owner再次通过，见[验证](issue-review-o01-table-owner-health-retry.md) |
| O02 | EXISTS索引错误的访问计划/断言 | 已修正回归夹具 | 原完整协议exit 1→0，保留六严格XX001，新增精确t/f，完整EXISTS协议/19native通过；未改产品，见[验证](issue-review-o02-index-access-path-expectations.md) |
| O03 | EXPLAIN JOIN图及过时断言 | 已修复 | 实际执行的source/cache/join/scan图及相关重绑；最终完整60控制、十二native/十协议CLI全过，OFFSET阳性及42601原/PG对照保留，见[验证](issue-review-o03-explain-join-source-graph.md) |
| O04 | HAVING BOOLEAN绑定 | 已修复 | `157e5d2e`；20 native、49 PG/候选/Root协议、五native/六协议邻居与完整PGprotocol通过；原超时及原错误均保留，见[验证](issue-review-o04-having-boolean-context.md) |
| O05 | ORDER BY重复别名 | 已修复 | `c5d10cc3`；原117协议85失败→PG/Root全过；74native、十四完整native/十六完整协议通过，纯绑定歧义、星号真实来源、typed常量及实际输出owner/初始化次数保留，见[验证](issue-review-o05-order-by-duplicate-alias.md) |
| O06 | 物化视图刷新超时 | 当前完整复验通过 | 原驱动未改，默认15秒及原10秒配置两完整exit0；历史原因未定，见[验证](issue-review-o06-matview-refresh-revalidation.md) |
| O07 | ALTER ARRAY历史超时 | 当前完整复验通过 | 原驱动未改，默认15秒及原10秒配置两完整exit0；历史原因未定，见[验证](issue-review-o07-alter-array-revalidation.md) |

原273项功能总账独立维护；关闭这里的缺陷不自动宣称完整PostgreSQL功能兼容。

## R52：测试终态行的真实分隔

`prepared_srf_host_ownership_test` 的成功提示原来输出字面量反斜杠加 `n`，
使随后构建脚本的 `[test-build] ... PASSED` 紧接在同一行。测试确已成功退出，
但按行首严格盘点会漏记该项。修正方法仅为输出实际换行，不改变任何测试断言
或脚本终态解析规则。候选完整驱动以当前全部58份生产对象 receipts 为门禁重新
编译、链接、隔离执行，结果0；证据为
`/tmp/dbms-review-r46.ifu6SWLp/r52-native.log`。
原冻结完整回归已经结束；其输入、日志、严格汇总及独立标记核验均保留，见
[完整终态](issue-full-registered-f4087f5d.md)，不能用修正后结果覆盖原始观测。

## 最新主分支集成记录

`b5fc5b73` 的公共算术类型头变化后真实重编全部58个生产翻译单元，标准构建
实际exit 0（`/tmp/dbms-review-fix-donor.N9sdas6f/root-r37-r40-fresh58-build.log`）。
R37协议109控制、最终十一完整native及完整postgres_protocol均exit 0。首次
native批次在五个驱动通过后因调用者拼错 rollback 驱动文件名而exit 1；保留
`root-r37-r40-native.log`，更正真实文件名并预校验全部输入后完整重跑十一项
exit 0，不将首次批次记为十一项通过。

`07f71677`（已含R38/R39）真实重编Main、DML、binder、maintenance四个变化
翻译单元并链接exit 0；逐对象核对全部58份当前receipts及最终输入不变。
`root-r38-r39-r23-native.log` 的十一完整native、五个
`root-r38-r39-r23-*_test.py.log` 协议和 `root-r23-div14-whole-protocol.log`
完整能力测试均exit 0。私树R23首次DIV14早期DDL超时仍保留，不改断言/timeout。
上述为集成门禁，尚不是最后一次完整注册回归，也不关闭表内其余待修复项。

R46–R57的已验收修改均已逐项合入主工作区并各自commit，未push。主分支对应
R46 `47b4e04b`、R47 `34bff4ea`、R48 `ce332459`、R49 `e1ab8111`、
R50 `9ea19c2d`、R52 `ebbf795c`、R53 `5cdccf4a`、R54 `c5b90fbc`、
R55 `ee303db6`、R57 `b15223ab`、R56 `4533d8db`。R51没有产品修复，按用户
要求暂缓，scope记录为`93443c1f`。

本段证据目录为`/tmp/dbms-review-r46.ifu6SWLp/`，不把私树验收冒充主分支：

- R46公共头变化后主分支真实重编58份生产单元，`root-r46-r49-fresh58.log`为0；
  首轮native在11项通过后因R51失败，不能写成整批通过；原样五项retry为0。
- R50的42native及R53的78native、R55目录身份等最终九完整native为0，
  `root-r55-native.log`；BIT160/172/60及schema/MV协议五入口为0，
  `root-r46-r50-protocols.log`。各后续变化CPP真实标准重编。
- R53新133及原完整INTERVAL、R54新54及原聚合scope、R55新28及原patterns
  主分支完整通过，`root-r53-protocols.log`、`root-r54-r55-protocols.log`。
- R57主分支五完整native、三完整协议为0，`root-r57-native.log`和
  `root-r57-protocols.log`，含加强后的真实故障/显式恢复全过程。
- R56主分支真实O2重编Main和TableManage并链接0，
  `root-r56-standard-build.log`；七完整native、六完整协议为0，
  `root-r56-native.log`、`root-r56-protocols.log`，含新33/45控制与原完整window_type。
  native门禁开始/结束核对当前58份生产receipts及构建输入。

这些是各提交集成验证，不是当前1339注册入口的完整终态。下一轮完整注册回归
须另冻结全部已提交输入、二进制及58份receipts，保留失败、环境skip和中间结果。
