# PostgreSQL 18 vs 本 DBMS 功能对比

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

当前接受版本包含[R169组合记录](issue-review-r169-simple-query-raw-syntax.md)所列九项有限修复，已逐项本地提交。
Root和accepted-only clean各完整58入口（26native、29协议、三个真实Main）实际全0；
这是组合定向回归，不是每个中间提交独立全矩阵，也不是全部1478入口运行或TLS/整库sanitizer验收。
已接受589MD、916native+559已注册Python+三个Main=1478；工作区592MD、922+565+3=1490，
原12个未接受入口及三份文档仍分开保留。原273仍22 complete /166 partial /70 unverified /15 deferred，
总目标未完成；R156、R151、R106及其他用户暂缓模块不恢复，不push，Actions保持禁用。
用法与限制见[手册](MANUAL.md)，实际错误、修改方法及日志见各专属报告。
以下旧轮次按原输入保留，历史计数、失败和待办不被本次改写。

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

数组函数存在不等于元素相等和多态声明兼容；ARRAY_POSITION的已验证类型、
NULL/起点、用户重载及真实参数元数据范围见
[R108](issue-review-r108-array-position-typed-equality.md)。ARRAY_POSITIONS与
原TYPE-16/FUNC-01仍未完成，不把本项扩为完整PostgreSQL数组能力。

四个字符串长度名字的真实11签名、不同计量单位和用户重载的有限范围见
[R107](issue-review-r107-string-length-signatures.md)，不能把旧函数存在性勾选
扩大为完整FUNC-01/SQL-09。[R106](issue-review-r106-coalesce-explicit-input-analysis.md)
按用户要求暂缓，不由普通继续恢复。

> 以下是按日期保留的历史对比，代码规模、测试数量和完成标记不代表当前 HEAD。当前能力声明见[兼容性契约](compatibility-contract.md)，实际 bug、检测范围和修复方法见[代码审查报告](code-review-2026-10-13.md)。

网络类型的历史勾选不表示完整兼容：当前输入格式、EUI宽度与错误状态的实际
范围见[R101](issue-review-r101-macaddr-input-and-widths.md)和
[R102](issue-review-r102-network-input-notation.md)。NAME→MAC源身份回归已由
[R103](issue-review-r103-macaddr-name-input.md)修复，typed网络值转换仍列R104。
网络限定NAME源遗漏已由[R105](issue-review-r105-network-name-input.md)独立修复
并通过自身生产构建/运行时验收；[R104值转换](issue-review-r104-network-value-conversions.md)
也已独立修复并经649/原738与41邻接验收；完整网络函数族未关闭。

> 生成日期: 2026-08-14；2026-08-16 复核修正若干过高标注（EXPLAIN ANALYZE、TSVECTOR/TSQUERY、INTERVAL、INHERITS、统计信息消费）并补充文件级差距说明；2026-08-17 批量补齐 P1-10~13 / P2-9~13 / P3-7 共 10 项差距（PG 语法 PREPARE/EXECUTE、EXPLAIN ANALYZE 每节点统计、统计驱动选择率与 join 成本、并行 HashJoin/聚合/GatherMerge、deferrable UNIQUE/FK、数组构造/切片/包含、CREATE INHERITS+ONLY、interval 算术+AT TIME ZONE、dollar-quoting、全文检索 @@/to_tsvector/to_tsquery/ts_rank）；2026-08-20 第二批 8 项（PL/pgSQL 最小解释器、VACUUM PARALLEL、pg_stats/pg_statistic 视图、unnest 表函数、TimeZone GUC 渲染、多表 join 贪心搜索、deferrable EXCLUDE、ts_rank weights+`<->` 短语）；2026-08-20 续：P1-9 自定义成本函数（CostModel+GUC+hook）；P2-2 Bloom 索引
> 本 DBMS 代码规模: ~66,000 行 C++ (44 .cpp + 56 .h)
> 对照: PostgreSQL 18 (~1,200,000 行 C)
> 测试基线（2026-08-14）: PASS=139 FAIL=0（137 个 C++ 测试 + PostgreSQL 协议 E2E + 窗口函数 E2E；含 Volcano 算子、并发测试、跨 backend/跨进程表锁协调、跨 backend 2PC、数据库生命周期、schema 格式完整性、WAL 损坏恢复、WAL 归档失败重试、复制管理器并发状态和网络启动安全）；2026-08-17 批量补齐后 PASS=153

数据库生命周期边界现已进一步收紧：新数据库的 `tlist.lst`、字符集和 public schema 元数据通过原子发布创建，checkpoint 文件和物理备份标记也通过同一类原子写入；部分创建失败会回收目录，DROP 的统计文件清理失败会向调用方报告 I/O 错误。该改动改善崩溃一致性，但不改变本 DBMS 与 PostgreSQL 在完整 catalog、WAL/PITR、复制和协议语义上的差距。

协议回归现已覆盖扩展查询错误后的 ignore-until-Sync 恢复、事务外 `ReadyForQuery('I')` 状态、显式失败事务的 `25P02`/`ReadyForQuery('E')`、失败状态下 `COMMIT` 转完整回滚、`ROLLBACK TO SAVEPOINT` 恢复及 `COMMIT/ROLLBACK PREPARED` 边界、文本/binary 整数、numeric 及 date/time/timestamp/UUID 参数与结果、statement/portal 的 Describe/Close 生命周期、基础 portal `maxRows` 分页及常见单表 RowDescription 元数据；这只补齐了错误状态机与参数路径的一部分，数组等复杂类型 I/O、复杂表达式的完整类型映射、内部秒精度之外的时间精度、holdable/scrollable portal 和扩展消息仍与 PostgreSQL 有差距。

ACL 回归现已覆盖表/列权限对会话用户、递归继承角色和 `PUBLIC` 的解析；`NOINHERIT` 会话角色不会自动获得成员角色权限，原始成员关系与有效权限关系已分开；角色授权可通过真实 PostgreSQL 协议生效。表 owner 已进入正式 schema/`pg_class.relowner`，并参与 RLS owner bypass；表级 `GRANT OPTION` 已支持独立撤销和基础授权链 `RESTRICT/CASCADE`，完整 ACL 对象范围和对象 owner 传播仍未达到 PostgreSQL 语义。

所有权授权回归已覆盖：非所有者即使带有旧式管理员标志也不能转移表所有权；所有者只有在能够 `SET ROLE` 到目标角色时才能转移；`SET ROLE` 使用成员关系而不是 `rolinherit`，切换后的有效角色用于 `current_user`、RLS 和表 ACL。表 owner 的隐含表/列权限和 `GRANT OPTION` 已进入统一 ACL 查询；完整对象类型 owner/ACL 传播仍有差距。

RLS 回归现已覆盖默认 `WITH CHECK`、显式 `TO PUBLIC`、permissive/restrictive 策略组合、`INHERIT/NOINHERIT` 角色权限、表 owner 绕过、`pg_authid` 的 `SUPERUSER/BYPASSRLS` 绕过和 `FORCE ROW LEVEL SECURITY`；复杂 owner/ACL 组合和完整 PostgreSQL policy catalog 语义仍有差距。

事务回归现已覆盖两个协议 backend 的事务上下文隔离：每个连接线程拥有独立的事务 ID、快照、回滚日志和 savepoint 状态，未提交数据不会被另一连接读取；INSERT、UPDATE、DELETE 的普通回滚与 SAVEPOINT 回滚会恢复主键、二级、复合、Hash 索引和 TOAST，DDL CREATE undo 也会跨语句外层 ROLLBACK 并遵守 SAVEPOINT 边界；保存点检查点会释放保存点之后新增的表/row/page/gap 锁并恢复之前的锁模式；变更前 DDL 快照可恢复外层 DROP/REPLACE，并在恢复后继续撤销同一事务的行变更。快照已污染后 SAVEPOINT 会安全拒绝，DELETE savepoint 回滚后可在同一事务提交并继续读取，重启后仍能读取旧大字段。SERIALIZABLE 现在对非空索引谓词和顺序扫描登记 heap page SIREAD，空谓词保留关系级兜底，非相交页并发和跨页危险结构已有真实回归。Volcano 执行器统一位于 `src/executor/`，`IndexScanOp` 通过 page shared lock/MVCC 边界按 RID 直接回表，不再对每个命中扫描整张 heap。`lock_manager_concurrency_test` 进一步验证表锁重入/升级、row/page token 归属、保存点锁检查点和双线程 wait-for graph 死锁受害者释放，`cross_backend_lock_test` 验证独立 `StorageEngine` backend 共享同库表锁、隔离跨库同名表，并通过 fork 验证表/row/page 锁竞争、释放后获取和 gap 阻塞，`lock_failure_propagation_test` 验证存储层建索引遇到真实表锁竞争时 fail-closed。新增 `prepared_transaction_test` 验证 PREPARE 前刷出 backend 私有 heap/index 缓存、prepared 元数据原子 durable 发布、跨 backend 锁保留、重启后 in-doubt xid 注册、顺序/索引条件查询不可见、提交可见性和回滚不可见性。TCL 解析已结构化保留隔离级别、只读模式和保存点名称，并修复特定回滚命令的前缀分类问题。B+Tree/Hash 刷盘现在记录完整文件 before/after WAL 镜像并参与崩溃恢复；原生 page-level 索引 WAL、索引范围 predicate lock、其他访问方法、跨访问方法原子提交、完整 PostgreSQL 锁模式和 SSI 语义、DEFERRABLE 安全快照仍是差距，含内存闭包式 DDL undo 的事务暂不支持 PREPARE TRANSACTION，不能据此宣称完整 PostgreSQL 事务语义已完成。

2026-08-09 质量验证补充：主程序在 `-Wall -Wextra` 下无编译警告；快速回归、独立测试、窗口函数 E2E、协议 E2E 和 OpenSSL Docker 构建均通过。普通单表 INSERT、受限行级标量表达式 UPDATE、单源表 UPDATE FROM、单源表 DELETE USING、INNER/CROSS JOIN DML、简单谓词 DELETE 已进入独立 AST 执行器，混合 DEFAULT、显式列、多行、字符串字面量、INSERT/UPDATE/DELETE 列投影及受限标量表达式 RETURNING、修改/删除回归和默认值存储语义有 parser/协议覆盖；高级 DML 和复杂表达式仍保留明确 legacy 回退。带表限定的表达式引用优先解析限定命名空间，避免 UPDATE FROM/DELETE USING 中同名列被错误绑定。legacy 文本执行器的协议结果捕获已改为线程局部路由，避免全局 `std::cout` 锁造成会话串行化。该结果只说明当前实现可重复验证，不改变下文列出的 PostgreSQL 语义与运维差距。

DDL CREATE undo 回归已覆盖 view、materialized view、UDF/TVF、procedure、trigger、RLS policy 和 collation；CREATE 记录现在还会进入外层事务及 SAVEPOINT 的逆序回滚队列，变更前物理快照可让外层 `ROLLBACK` 恢复 DROP/REPLACE 旧对象并在之后执行行 undo。整库快照污染后的另一条快照型 DDL 和 SAVEPOINT 会安全拒绝，完整依赖 undo 和全部 PostgreSQL 隐式提交边界仍与 PostgreSQL 有差距。

SQL 可观测性已补强：交互式和协议入口共用线程安全的 `SqlStats`，`SHOW STATEMENTS`/`pg_stat_statements` 风格查询可按归一化 SQL 聚合耗时；`RuntimeStats` 的完整扫描 live-row 估计也会反馈给 Join 成本与 EXPLAIN，并在关系重建/截断时失效。当前格式的数据库级 `.runtime_stats`/`.sql_stats` 快照已在 checkpoint/引擎关闭时持久化并在启动时严格加载；仍缺完整字段、后台采样和扩展生命周期。

---

## 总览

| 维度 | PostgreSQL 18 | 本 DBMS | 差距 |
|------|--------------|---------|------|
| 代码量 | ~1.2M 行 | ~66K 行 | ~18x |
| 开发团队 | 全球数百人/20+年 | 单人/数周 | — |
| 测试覆盖 | ~2000+ 测试 | 137 C++ 单元测试 + 2 个 E2E | 以回归结果为准 |
| 功能完成度 | 不宣称生产级 | DDL/DML 核心已覆盖，高级特性和运维能力仍有缺口 | 以测试和代码路径为准 |

---

## 一、数据类型

| 类型 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| INT/BIGINT/SMALLINT/SERIAL | ✅ | ✅ | ✅ |
| NUMERIC(p,s) / DECIMAL | ✅ | ✅ | ✅ |
| FLOAT/DOUBLE/REAL | ✅ | ✅ | ✅ |
| MONEY | ✅ | ✅ | ✅ |
| VARCHAR/CHAR/TEXT | ✅ | ✅ | ✅ |
| BYTEA (hex/escape) | ✅ | ✅ | ✅ |
| BOOLEAN | ✅ | ✅ | ✅ |
| DATE/TIME/TIMESTAMP/TIMESTAMPTZ | ✅ | ✅ | ✅ (含 infinity) |
| INTERVAL | ✅ | ✅ | 字面量解析/规范化 + `timestamp ± interval`（日历月进位）/`interval ± interval`/`interval * / number` 算术 + `AT TIME ZONE` 双向（UTC/GMT/±HH[:MM]/±HHMM，`timezone()` 函数）；`TimeZone` GUC 会话显示与 interval typmod 字段限定仍缺 |
| UUID | ✅ | ✅ | ✅ |
| INET/CIDR | ✅ | ✅ | ✅ |
| MACADDR/MACADDR8 | ✅ | ✅ | ✅ |
| JSON/JSONB | ✅ | ⚠️ | 18 个 `json_/jsonb_` 函数 + `->`/`->>`/`#>`/`#>>` 操作符与 `@>`/`<@` 递归 containment（本批补齐）；jsonpath 求值、`JSON_TABLE` 与 GIN opclass 仍缺 |
| XML | ✅ | ✅ (well-formedness) | ⚠️ 缺 XPath/XMLTABLE |
| ARRAY (多维) | ✅ | ✅ | 字面量/下标（负数从尾部）/`=ANY`(rewrite) + 本批补齐 `ARRAY[]` 构造、`a[lo:hi]` 切片（钳制/开放边界）、`@>`/`<@`（SQL 数组与 JSON 自动判别）；协议 binary 数组 I/O 仍缺；`unnest` 表函数已于 2026-08-20 补齐（`FROM unnest(...)` 与投影两路径） |
| TSVECTOR/TSQUERY | ✅ | ✅ (基础) | 类型 + `@@` 匹配（&/| 语义）、`to_tsvector`/`to_tsquery`/`plainto_tsquery`、`ts_rank`（含 weights float4[] 与 `<->` 距离-1 短语匹配、`setweight()`，2026-08-20）；GIN 倒排与 ts_headline 仍缺 |
| GEOMETRIC (line/lseg/box/path/polygon/circle) | ✅ | ✅ | ✅ |
| POINT | ✅ | ✅ | ✅ |
| ENUM | ✅ | ✅ | ✅ |
| COMPOSITE TYPE | ✅ | ✅ | ✅ |
| RANGE/MULTIRANGE | ✅ | ✅ (基础) | ⚠️ 缺 operators |
| DOMAIN | ✅ | ✅ | ✅ |
| **Pseudo types (record/anyelement/anyarray)** | ✅ | ✅ (注册) | ⚠️ 缺函数重载 |
| **bytea 存储** | ✅ TOAST | ⚠️ overflow + zlib 压缩 | ⚠️ 缺少 PG pointer/catalog 与 storage strategy 完整语义 |
| **numeric NaN/Infinity** | ✅ | ✅ | wire codec 与基础值语义已覆盖，精确 typmod/完整函数族仍有差距 |

---

## 二、SQL 语法

### 2.1 DDL

| 功能 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| CREATE/DROP/ALTER DATABASE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER SCHEMA | ✅ | ✅ | ✅（DROP SCHEMA 先物理删除、后应用 catalog 依赖计划） |
| CREATE/DROP/ALTER TABLE (基础子命令；全量仍有缺口) | ✅ | ⚠️ | ⚠️ |
| CREATE/DROP/ALTER VIEW | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER MATERIALIZED VIEW | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER INDEX | ✅ | ✅ | ✅ |
| CREATE INDEX CONCURRENTLY | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER SEQUENCE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER FUNCTION/PROCEDURE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER TRIGGER | ✅ | ✅ | ✅ (DDL + 运行时执行) |
| CREATE/DROP/ALTER ROLE/USER | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER TYPE (composite/enum/range) | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER DOMAIN | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER POLICY (RLS) | ✅ | ⚠️ | CREATE 已由 AST/DDL bridge 接管并有运行时执行；表 owner/RLS bypass 已接入；ALTER POLICY、完整角色/owner/ACL 语义仍不完整 |
| CREATE EXTENSION | ✅ | ✅ (DDL) | ⚠️ 运行时缺 |
| CREATE/DROP/ALTER COLLATION | ✅ | ✅ (DDL) | ⚠️ ICU 缺 |
| TABLESPACE | ✅ | ✅ | ⚠️ 关系文件已统一物理路由并支持跨文件系统 SET TABLESPACE/重启恢复；权限、owner、ALTER TABLESPACE 完整语义和 PostgreSQL OID/符号链接布局仍缺 |
| PARTITION BY RANGE/LIST/HASH | ✅ | ✅ | ✅ |
| INHERITS | ✅ | ✅ | `ALTER TABLE INHERIT/NO INHERIT`、`CREATE TABLE ... INHERITS` 列合并 + SELECT/UPDATE/DELETE 的 `ONLY [()]` 修饰；约束/索引/默认值的继承传播仍缺 |
| LIKE ( INCLUDING ALL/DEFAULTS/CONSTRAINTS/INDEXES/IDENTITY) | ✅ | ✅ | ✅ |
| TRUNCATE (ONLY/RESTART IDENTITY/CASCADE) | ✅ | ✅ | ✅ |
| **ALTER TABLE ... ADD COLUMN ... IF NOT EXISTS** | ✅ | ✅ | ✅ |
| **ALTER TABLE ... ADD/DROP EXCLUDE CONSTRAINT** | ✅ | ⚠️ | typed DDL bridge 已支持名称保留、冲突检查、INSERT/UPDATE 执行与删除清理；GiST 加速、完整 operator class 和复杂表达式元素仍缺 |
| **REINDEX CONCURRENTLY** | ✅ | ⚠️ | |

### 2.2 DML

| 功能 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| INSERT (VALUES/SELECT/ON CONFLICT/RETURNING) | ✅ | ⚠️ | 普通单表 VALUES/DEFAULT VALUES、无 JOIN/聚合/排序的 INSERT SELECT（含列表达式）、无 target 或显式匹配主键/唯一约束 target 的 ON CONFLICT DO NOTHING、显式匹配单列或复合主键/唯一约束 target 的常量或只引用 `excluded` 的 evaluator 受限标量表达式 DO UPDATE、目标行/`excluded` 的受限 WHERE 及列投影/受限标量表达式 RETURNING 走 `DmlExecutor` AST；复杂 SELECT、部分/索引推断 conflict target、引用子查询或其他关系的 DO UPDATE/WHERE、复杂/子查询/窗口 RETURNING、视图写入和复杂未支持表达式明确回退 legacy |
| UPDATE (FROM/RETURNING) | ✅ | ⚠️ | 受限行级标量表达式单表 UPDATE、单源表 UPDATE FROM、来源 INNER/CROSS JOIN 的 UPDATE FROM 及列投影/受限标量表达式 RETURNING 走 `DmlExecutor` AST；外连接/复杂 UPDATE FROM、复杂/子查询/窗口 RETURNING、视图写入仍回退 legacy；`UPDATE ... LIMIT` 是 MySQL 语法，本项目已移除 |
| DELETE (USING/RETURNING) | ✅ | ⚠️ | 简单谓词单表 DELETE、单源表 DELETE USING、来源 INNER/CROSS JOIN 的 DELETE USING 及列投影/受限标量表达式 RETURNING 走 `DmlExecutor` AST；外连接/复杂 USING、复杂/子查询/窗口 RETURNING、ONLY、视图写入仍回退 legacy；`DELETE ... LIMIT` 是 MySQL 语法，本项目已移除 |
| MERGE (MATCHED/NOT MATCHED) | ✅ | ✅ | ✅ |
| REPLACE INTO (MySQL 兼容) | ❌ | ✅ | ✅ |
| **COPY (binary/program)** | ✅ | ⚠️ | 仅 CSV |
| **UPSERT 完整语义** | ✅ | ⚠️ | 基础 |

### 2.3 DQL (查询)

| 功能 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| SELECT (投影/WHERE/ORDER BY/LIMIT/OFFSET/DISTINCT) | ✅ | ✅ | ✅ |
| JOIN (INNER/LEFT/RIGHT/FULL/CROSS/NATURAL) | ✅ | ✅ | ✅ |
| **SEMI/ANTI JOIN** | ✅ | ⚠️ | 未关联单列 `IN`/`NOT IN` 已由 `SemiJoinOp`（anti 模式）执行，单列未关联 `ANY/ALL` 已由 `QuantifiedSubqueryFilterOp` 执行；显式 JOIN 语法、关联性和完整 planner 语义仍缺 |
| **LATERAL JOIN** | ✅ | ❌ | 缺 |
| Subquery (IN/EXISTS/ANY/ALL/标量) | ✅ | ⚠️ | 未关联单列 `IN`/`NOT IN` 进入 Volcano semi/anti plan；未关联单表 `EXISTS`/`NOT EXISTS` 进入 `ExistenceFilterOp`；单个未关联标量目标进入 init-plan 并执行 0/1 行语义；单列未关联 `ANY/ALL` 进入 `QuantifiedSubqueryFilterOp` 并执行 NULL/空集三值逻辑；复杂标量、关联和复杂组合回退 legacy |
| CTE (WITH/RECURSIVE) | ✅ | ✅ (parser + executor) | ✅ 基础 + RETURNING |
| UNION/INTERSECT/EXCEPT | ✅ | ⚠️ AST 已按优先级/左结合解析，组合统一走 Volcano `SetOperationOp`；简单 operand 直接计划，复杂 operand 经 `MaterializedRowsOp` 接 legacy producer（含 ALL） | ⚠️ AST 到计划的全量下推、类型合并/collation 和完整作用域仍缺 |
| Window Functions (ROW_NUMBER/RANK/...) | ✅ | ⚠️ `WindowOp` 已执行常见排名/偏移、窗口聚合及 `ROWS/RANGE/GROUPS` frame/exclusion，`OffsetOp` 已接入；复杂窗口仍走 legacy | ⚠️ 复杂表达式和完整 planner/explain 语义仍缺 |
| **GROUP BY ROLLUP/CUBE/GROUPING SETS** | ✅ | ⚠️ 普通聚合与 `GroupAggregateOp` 已消费过滤后的 Volcano 子计划，覆盖常见聚合与基础 grouping sets | ⚠️ 复杂目标、`GROUPING()`/`GROUPING_ID`、完整排序作用域仍缺 |
| GROUPING_ID | ✅ | ❌ | 缺 |
| FOR UPDATE/SHARE/NOWAIT/SKIP LOCKED | ✅ | ✅ | ✅ (行级锁 + 死锁检测) |
| **PREPARE TRANSACTION (2PC)** | ✅ | ⚠️ | PREPARE 前刷 heap/index，prepared 元数据原子 durable 发布并写 PREPARE WAL；表/row/page/gap 锁可跨 backend 与重启恢复；重启保留 in-doubt xid 并统一过滤索引回表可见性；全局 prepared 目录和 in-doubt 决策仍缺 |
| VALUES | ✅ | ✅ | ✅ |
| **Array subscript [n:m]** | ✅ | ❌ | 缺 |
| **JSON path / SQL/JSON** | ✅ | ❌ | 缺 |

---

## 三、索引

| 访问方法 | PG 18 | 本 DBMS | 状态 |
|----------|-------|---------|------|
| B+ Tree | ✅ | ⚠️ | 基础文件 B+Tree 已覆盖跨叶/内部节点分裂、重复键、范围扫描和多值 `(key, RID)` 精确删除；公共键 API 统一按当前 20 字节固定格式规范化，长键/重开索引已有回归；事务刷盘、引擎退出和恢复支持完整文件 before/after WAL 镜像，但 PG 原生 page-level WAL、dedup、page deletion/合并、opclass/collation、skip scan 和完整并发构建仍缺 |
| Hash | ✅ | ⚠️ | 基础 Hash 文件支持严格校验、原子持久化和完整文件 before/after WAL 镜像；PG WAL-safe bucket split、metapage/overflow page、并发维护和完整恢复语义仍缺 |
| GIN | ✅ | ✅ (基础，独立/StorageEngine 文件均严格校验并原子持久化) | ⚠️ |
| GiST | ✅ | ⚠️ | 已有简化文本/范围索引与真实 DDL 路由；缺完整树结构、opclass、WAL、并发构建和几何/全文语义 |
| SP-GiST | ✅ | ✅ (Point quadtree, SPGiSTIndex.cpp) | ⚠️ |
| BRIN | ✅ | ✅ (块范围，独立/StorageEngine 文件均严格校验并原子持久化) | ⚠️ |
| **Bloom** | ✅ | ❌ | 缺 |
| **覆盖索引 (INCLUDE)** | ✅ | ✅ | ✅ |
| **部分索引 (WHERE)** | ✅ | ✅ | ✅ |
| **表达式索引** | ✅ | ✅ | ✅ |
| **唯一索引 + 约束** | ✅ | ✅ | ✅ |
| **多列复合索引** | ✅ | ✅ | ✅ |
| **Index Only Scan** | ✅ | ❌ | 缺 visibility map、heap 可见性与 NULL 位图证明；当前所有索引路径统一安全回表 |
| **Bitmap Index/Heap Scan** | ✅ | ⚠️ | 等值多索引 AND/OR 已由 `BitmapHeapScanOp`/`BitmapOrHeapScanOp` 接入；范围 bitmap 仍缺 |

---

## 四、事务与并发

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| ACID 事务 | ✅ | ✅ | ✅ |
| MVCC (快照隔离) | ✅ | ✅ | ✅ |
| ReadView + CLOG | ✅ | ✅ | ✅ |
| 隔离级别 (RU/RC/RR/SI/SERIALIZABLE) | ✅ | ⚠️ | 框架有 |
| WAL (Write-Ahead Log) | ✅ | ✅ | LSN 0 为首个合法位置；COMMIT WAL 记录和 CLOG 状态均刷盘成功后才报告提交，CLOG 失败会追加 ABORT WAL 并回滚；CLOG 段采用文件锁、按位合并、原子替换并同步段文件和 `pg_xact` 目录，截断在保存和目录同步成功后才删除旧段；已提交 after-image 正序重做、未提交 before-image 逆序 undo；索引镜像严格校验并限制在数据库/已登记 tablespace 目录，恢复失败 fail-closed；物理备份有显式标记不会被误当活动数据库；多 writer 有进程/文件锁与磁盘尾部刷新；完整 WAL resource manager/恢复语义仍有差距 |
| 事务物理快照生命周期 | ✅ | ✅ | 仅文件级 DDL 显式创建按 xid 命名快照；快照事务持有数据库级排他锁，启动恢复按 WAL 提交状态清理或恢复 |
| Checkpoint | ✅ | ✅ | checkpoint WAL/LSN 与已加载 heap/index 缓存刷盘失败会传播；活动事务期间不推进恢复起点；已归档且早于 checkpoint 的完整 WAL 段会回收；完整 restartpoint、节流和 PITR 仍缺 |
| SAVEPOINT | ✅ | ✅ | ✅ |
| 表级锁 (共享/排他) | ✅ | ✅ | ✅（进程内注册表 + 跨进程 `flock` 表级协调） |
| **行级锁** | ✅ | ✅ | ✅（rowLockShared/Exclusive + NOWAIT；数据库 namespace 下跨进程 `flock` 协调） |
| **页级锁** | ✅ | ⚠️ | ✅（进程内 token + 数据库 namespace 下跨进程 `flock` 协调） |
| **Deadlock detection** | ✅ | ✅ | ✅ (wait-for graph + cycle detection + log) |
| **Gap locks / predicate locks** | ✅ | ⚠️ | 有精确进程内 gap range、每表保守跨进程协调、heap page SIREAD、单列 B+Tree 逻辑谓词和空范围关系级兜底；缺真正物理索引范围 predicate lock |
| **SSI (Serializable Snapshot Isolation)** | ✅ | ⚠️ | 行级/页级 rw-conflict + 单列 B+Tree 逻辑谓词 + 关系级空范围兜底；已验证非相交页并发和跨页危险结构，复合/其他访问方法 phantom 推理与完整 SSI 规则仍缺 |
| **两阶段提交 (2PC)** | ✅ | ⚠️ | 基础 `prepareTransaction` + `COMMIT/ROLLBACK PREPARED` 已覆盖 durable prepared 记录、PREPARE WAL、跨 backend 锁所有权和提交/回滚回归；包含内存 DDL undo 的事务会拒绝 PREPARE，PG 全局目录与崩溃后 in-doubt 语义仍缺 |
| **并行查询** | ✅ | ⚠️ | 非分区 heap 按 page range 并行扫描/确定性 Gather，本批补齐 ParallelHashJoin（分片 build）、ParallelGroupAggregate（局部桶+合并终态）与 GatherMerge（ORDER BY 分区排序+归并）；worker 线程池复用与并行索引扫描仍缺 |
| **JIT compilation (LLVM)** | ✅ | ❌ | 缺 |
| **Async I/O (io_uring)** | ✅ (PG18) | ❌ | 缺 |
| HOT Update | ✅ | ✅ | ✅ |
| **谓词锁** | ✅ | ❌ | 缺 |

### 并发测试现状 (2026-07-03 更新)
- ✅ 事务原子性 (BEGIN/COMMIT/ROLLBACK/SAVEPOINT)
- ✅ WAL 顺序写入吞吐 (~1767 行/秒)
- ✅ B+ 树索引插入吞吐 (~1729 行/秒)
- ✅ 索引等值查找 (34 行/3-4ms)
- ✅ 聚合性能 (500 行 COUNT/SUM/MAX/GROUP BY ~78ms)
- ✅ MVCC 快照隔离 (ReadView 可见性)
- ✅ HOT 堆内更新
- ✅ CLOG 提交日志 (位图 + 子事务可见性)
- ✅ Checkpoint 持久化 (脏页刷盘 + 归档后 WAL 段截断)
- ✅ WAL 崩溃恢复 (已提交正序重做 + 未提交多次修改逆序回滚)
- ✅ 多线程并发压力测试（`lock_manager_concurrency_test` 覆盖真实多线程竞争、token 归属、保存点检查点和清理）
- ✅ 死锁检测端到端测试（双线程 wait-for graph 真实触发并验证单一受害者释放）
- ⚠️ 隔离级别验证测试（已有 MVCC/SSI 回归，完整 PostgreSQL 隔离级别矩阵仍缺）

---

## 五、存储

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| Slotted page (8KB) | ✅ | ✅ | ✅ |
| Buffer Pool (clock sweep) | ✅ | ✅ | clock sweep、pinned 页保护、脏页写盘/读取失败 fail-closed；shared buffers 分片与完整并发 contention 语义仍有差距 |
| Free Space Map | ✅ | ✅ | ✅ |
| Visibility Map | ✅ | ✅ | ✅ |
| TOAST (大字段压缩/线外存储) | ✅ | ⚠️ | relation/index + zlib 压缩，TOAST page/index 写入失败会 fail-closed；缺少 PG pointer/catalog 完整语义 |
| 页校验和 | ✅ | ✅ | 页面/文件头 checksum、布局、WAL page image 均 fail-closed；损坏 WAL/非法 page ID 拒绝恢复 |
| 溢出页 | ✅ | ✅ | ✅ |
| 子事务日志 | ✅ | ✅ | ✅ |
| **TOAST 压缩 (lz4/pglz)** | ✅ | ⚠️ zlib | 已有压缩标记/解压；lz4/pglz、压缩策略和 `toast_tuple_target` 仍缺 |
| **大对象 (Large Object)** | ✅ | ✅ | ✅ |
| **Unlogged 表** | ✅ | ✅ | ✅ |
| **临时表** | ✅ | ✅ | ⚠️ | typed/session-isolated subset with ON COMMIT actions and restart cleanup; pg_temp catalog/search_path semantics remain |
| **表空间** | ✅ | ✅ | ✅ |

---

## 六、查询优化器

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| 火山模型执行器 | ✅ | ✅ (12 operators, Phase 5.1 已上线) | ✅ |
| 基于成本的优化 (CBO) | ✅ | ⚠️ | 基础；成本参数可调 + 自定义 hook（2026-08-20） |
| Bloom 索引 | ✅ | ✅ | `USING bloom` AM：bloom filter + 精确侧表，全 DML 维护与扫描探测（2026-08-20，多列签名仍缺） |
| 连接池 (PgBouncer 式) | ✅ | ✅ | `ConnectionPool` 三模式后端池 + `max_client_conn`/`SHOW POOLS`（2026-08-20；短租模式下 LISTEN/游标不跨租借，同 PgBouncer） |
| 逻辑解码 (Logical Decoding) | ✅ | ✅ | 双输出插件 + 发布目录 + 槽级变更流 + SQL 面（2026-08-20；无 SUBSCRIPTION 拉取端、非 WAL 重放） |
| TDE 透明数据加密 | ✅ | ✅ | 页级 SHA-256-CTR+EtM、边车信封、keyring 管理、崩溃恢复兼容（2026-08-21；索引旁路文件未覆盖、无密钥轮换） |
| 统计信息 (pg_statistic) | ✅ | ✅ | 采集（cardinality/MCV/等深直方图/多列）+ planner 消费：MCV 精确等值选择率、直方图范围插值、eqjoinsel join 选择率与成本；`pg_stats`/`pg_statistic` 兼容视图已补（2026-08-20）；多表 join 顺序 DP 仍缺（已有贪心搜索） |
| 索引选择 | ✅ | ✅ | ✅ |
| **等价类 (Equivalence Classes)** | ✅ | ⚠️ | 基础 |
| **PathKeys / 排序路径** | ✅ | ⚠️ | 基础 |
| Join reorder (动态规划) | ✅ | ⚠️ | 贪心搜索（≥2 JOIN 的 FROM 链按 ndistinct 估计选最小对起步、逐步挂接、中间物化，2026-08-20）；DP 动态规划仍缺 |
| Bitmap scan | ✅ | ⚠️ | 等值 Bitmap AND/OR 已接入；范围/并行路径仍缺 |
| **计划缓存** | ✅ | ✅ | ✅ |
| EXPLAIN ANALYZE 真实统计 | ✅ | ✅ | 每节点 `(actual time=… rows=… loops=…)`（21 个算子插桩）+ `BUFFERS` 按查询 shared hit/read 增量 + 总耗时/行数；trigger/JIT 明细仍缺 |
| 多索引组合 (Bitmap AND/OR) | ✅ | ⚠️ | 等值 Bitmap AND/OR 已接入，范围/并行组合仍缺 |
| **参数化路径** | ✅ | ❌ | 缺 |
| **自定义成本函数** | ✅ | ✅ | `QueryPlanner::CostModel`：5 个成本 GUC + enable 开关 + 自定义成本 hook（负值回退），join 选择全参数驱动（2026-08-20） |
| **遗传算法 join reorder** | ✅ | ❌ | 缺 |
| **分区裁剪** | ✅ | ✅ | ✅ |
| **并行扫描/聚合/连接** | ✅ | ✅ | page-range scan 真实 worker + 并行 HashJoin/GroupAggregate/GatherMerge（本批补齐）；worker 池与并行索引路径仍缺 |

---

## 七、进程/连接管理

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| 多进程后端 (fork) | ✅ | ❌ | 线程模型 |
| **Shared memory** | ✅ | ❌ | 缺 |
| Background workers | ✅ | ⚠️ 框架 | 11 种类型 |
| WALWriter / BgWriter / Checkpointer | ✅ | ✅ | ✅ |
| Autovacuum | ✅ | ⚠️ 框架 | `VACUUM (PARALLEL n)` 页分片 worker 池已补（2026-08-20）；独立 worker 进程池与负载均衡仍缺 |
| Stats collector | ✅ | ⚠️ | 有 RuntimeStats 事件计数子集（含顺序/索引扫描与索引取行）及当前格式持久化快照；后台采样和完整索引维度仍缺 |
| **连接池** | ✅ (外部) | ✅ | ✅ |
| **最大连接数** | ✅ | ✅ | ✅ |
| **pg_stat_activity** | ✅ | ⚠️ | 有进程列表风格子集，完整 backend 状态和 wait event 语义仍缺 |

---

## 八、复制与高可用

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| **物理流复制 (WAL shipping)** | ✅ | ⚠️ 框架 | 基础 |
| 复制槽 (物理/逻辑) | ✅ | ✅ | ✅ |
| 同步/异步复制 | ✅ | ✅ | ✅ |
| Hot Standby | ✅ | ⚠️ 框架 | |
| **Failover / Promote** | ✅ | ✅ | ✅ |
| 级联复制 | ✅ | ✅ (框架) | |
| **逻辑复制 (Publication/Subscription)** | ✅ | ✅ (框架) | 无 plugin 实现 |
| **Logical decoding** | ✅ | ❌ | 缺 |
| PITR (时间点恢复) | ✅ | ⚠️ | 基础 |
| pg_basebackup | ✅ | ❌ | 缺 |
| **增量备份** | ✅ | ❌ | 缺 |

---

## 九、安全

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| pg_hba.conf | ✅ | 🔄 | 首条匹配、hostssl/hostnossl、IPv4/IPv6 CIDR、角色/数据库别名和运行时 SCRAM 已实现；其余认证方法仍缺 |
| 用户/角色系统 | ✅ | 🔄 | pg_authid/pg_auth_members、主要角色属性、SCRAM、递归成员匹配、INHERIT/NOINHERIT、SET ROLE、角色 ADMIN OPTION、有效期检查和连接数限制已接入；完整 ACL、owner/依赖语义仍缺 |
| GRANT/REVOKE (ACL) | ✅ | ✅ (DDL) | ⚠️ 表/列 ACL、PUBLIC、有效角色、表 owner、角色 ADMIN OPTION 和表级 GRANT OPTION 基础撤销/级联已执行；ACL item、完整 ADMIN/GRANT OPTION 生命周期及对象全集仍缺 |
| 列级权限 | ✅ | ✅ | ✅ |
| **行级安全 (RLS) 执行** | ✅ | ⚠️ | USING/WITH CHECK 已接入关系感知扫描，默认 WITH CHECK、PUBLIC、基础 PERMISSIVE/RESTRICTIVE 组合、表 owner/`SUPERUSER/BYPASSRLS` 绕过、有效角色和 FORCE RLS 已实现；无适用策略默认拒绝、策略求值失败安全回退；对象 owner/ACL 组合语义仍不完整 |
| **SCRAM-SHA-256 完整协议** | ✅ | 🔄 | 已实现 catalog verifier、challenge/response、pg_hba 运行时决策和 E2E；缺 channel binding 与完整 SASL 语义 |
| **LDAP/Kerberos/GSSAPI/PAM/RADIUS** | ✅ | ❌ | 缺 |
| **SSL 双向认证** | ✅ | ⚠️ TLSWrapper；服务端默认 fail-closed，但缺少 PostgreSQL SSL 协商、客户端证书认证和 channel binding | |
| **安全标签 (SE-PostgreSQL)** | ✅ | ❌ | 缺 |
| **加密 (TDE)** | ✅ | ❌ | 缺 |
| **审计 (pgaudit)** | ✅ | ❌ | 缺 |

---

## 十、扩展生态

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| CREATE EXTENSION | ✅ | ⚠️ 框架 | |
| FDW (外部数据包装器) | ✅ | ⚠️ 框架 | |
| PL/pgSQL 运行时 | ✅ | ⚠️ | 最小解释器：DECLARE/IF/WHILE/FOR/RAISE/RETURN/SQL 执行/SELECT INTO（2026-08-20）；异常块、游标、触发器函数体仍缺 |
| **C 扩展加载 (fmgr)** | ✅ | ❌ | 缺 |
| **Hook 系统** | ✅ | ❌ | 缺 |
| **Background worker API** | ✅ | ⚠️ 框架 | |
| **Shared memory 扩展** | ✅ | ❌ | 缺 |
| 内置扩展 (pg_stat_statements 等) | ✅ | ⚠️ | SQL 统计子集已接入，完整扩展生命周期仍缺 |

---

## 十一、系统目录与监控

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| pg_class / pg_attribute / pg_type | ✅ | ✅ | ✅ |
| pg_namespace / pg_proc / pg_depend | ✅ | ✅ | ✅ |
| pg_authid / pg_auth_members | ✅ | ✅ | ✅ |
| pg_description | ✅ | ✅ | ✅ |
| pg_database | ✅ | ✅ | ✅ |
| information_schema | ✅ | ⚠️ 基础 | |
| pg_stat_* views | ✅ | ⚠️ | 有运行时数据库/表/SQL 统计及若干虚拟统计视图，字段和生命周期不完整 |
| pg_locks / pg_stat_activity | ✅ | ⚠️ | 有风格子集，完整 backend/wait 语义仍缺 |
| **pg_stat_statements** | ✅ | ⚠️ | 有归一化聚合、当前格式数据库快照持久化及 `pg_stat_statements.max` 确定性淘汰，缺 reset 权限和完整扩展语义 |
| **auto_explain** | ✅ | ❌ | 缺 |

---

## 十二、性能特性缺失 (关键差距)

### 🔴 严重缺失 (生产级必需)
1. **并行查询** — 已有非分区 heap page-range 多 worker scan；事务安全回退，parallel join/aggregate、GatherMerge、worker pool 和 parallel-aware planner 仍缺
2. **JIT 编译** — 表达式求值仍为解释执行, 无 LLVM 后端
3. **完整 GiST 索引** — 当前仅有简化文本/范围实现；全文/几何最近邻、opclass、WAL 和并发语义仍缺
4. **Gap locks / predicate locks** — 无法完全防止幻读
5. **SSI (Serializable Snapshot Isolation)** — 已覆盖行级/页级写偏差检测、空谓词关系级 SIREAD、非相交页并发及跨页危险结构，但索引范围 predicate lock 和完整冲突规则仍未实现
6. **Bitmap scan** — 等值多索引 Bitmap AND/OR 已接入；范围 bitmap 和并行路径仍缺
7. **INSTEAD OF 视图触发器** — 已支持简单单表视图上的逐行 INSERT/UPDATE/DELETE action SQL，并覆盖多行协议 E2E；复杂视图映射、transition tables、完整函数/PL 运行时仍缺失

### 🟡 重要缺失 (影响实用性)
1. **Window Function executor** — 常见排名/偏移、窗口聚合和 `ROWS/RANGE/GROUPS` frame/exclusion、OFFSET 已进入 Volcano `WindowOp`/`OffsetOp`，复杂目标仍回退 legacy
2. **复杂子查询 executor** — 简单子查询走 volcano, 关联子查询回退 legacy
3. **UNION/INTERSECT/EXCEPT executor** — 组合语义已统一进入 Volcano，复杂 operand 的 producer 仍待 AST 全量下推；类型合并和结构化列结果也仍缺
4. **GROUP BY/aggregate 完整语义** — 普通聚合与 `GroupAggregateOp` 已覆盖基础路径；复杂目标、`GROUPING()`/`GROUPING_ID`、类型推导和完整排序作用域仍待迁移
5. **GiST 索引完整语义** — 基础文本/范围文件已存在，但全文搜索、几何最近邻、opclass 与并发维护仍缺
6. **TOAST 完整语义** — 当前线外值使用 zlib 压缩；lz4/pglz、压缩策略、`toast_tuple_target` 和 PG pointer/catalog 仍待完成
7. **后台 stats_collector** — 已有 `RuntimeStats` 在执行/存储边界收集数据库和表计数，并接入 `SHOW STATUS`/`pg_stat_database`/`pg_stat_tables`，Volcano 扫描算子也会记录顺序/索引扫描及索引取行；当前格式快照已持久化，后台采样线程和完整索引维度仍缺
8. **PL/pgSQL 运行时** — 最小集已落地（2026-08-20），异常块/游标/触发器函数仍缺
9. **并行 Vacuum** — `VACUUM (PARALLEL n)` 已落地（2026-08-20），独立 worker 进程池仍缺

### 🟢 次要缺失 (易用性/运维)
1. pg_stat_* views / pg_stat_statements
2. Bloom 索引
3. 并行查询
4. 增量备份
5. pg_upgrade 工具
6. Logical decoding / 逻辑复制 plugin
7. 连接池 (PgBouncer 式)
8. 异步 I/O (io_uring)

---

## 总结

本 DBMS 在 **DDL 完整化** 方面达到了很高的完成度（SQL 语法覆盖 ~95%），但**运行时 semantics** 的执行层面还有显著差距：

- **DDL Parser**: ✅ 高度完整 (184 命令)
- **DML Executor**: ⚠️ 普通 INSERT、受限行级标量表达式单表 UPDATE、单源表 UPDATE FROM、单源表 DELETE USING、来源 INNER/CROSS JOIN 的 UPDATE FROM/DELETE USING、简单谓词单表 DELETE 和窄版 MERGE 已进入 AST 执行器；MERGE 的多 WHEN、BY SOURCE/BY TARGET、DELETE、复杂 source query、RETURNING 仍 fail-closed，高级 DML 仍有 legacy 执行路径
- **高级 Query**: ⚠️ parser 就绪, executor 部分完成
- **并发控制**: ⚠️ MVCC 基础有, 行级锁/死锁缺失
- **性能**: ⚠️ 无并行/JIT/高级索引
- **扩展性**: ❌ 框架有, 运行时缺

**下一阶段优先级建议** (2026-07-03 更新):
1. Window Function 完整迁移 + UNION/INTERSECT executor (查询完整性 — 窗口仍有 legacy 语义边界)
2. GiST 索引 (全文搜索/几何)
3. 并行 query exec (性能)
4. 关联子查询解嵌套 (优化器)
5. PL/pgSQL 运行时 (存储过程)
6. pg_stat_statements + 运行时统计 (可观测性)
7. 可串行化隔离 SSI (并发正确性)
