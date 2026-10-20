# 2026-10-13 审查与验证入口

最新有限修复：[R183 STRING_TO_ARRAY回调身份](issue-review-r183-string-to-array-callback-identity.md)
已按实际callback提供数组元数据，opaque替换保持未知声明/原生身份，不再错误强转数组。
旧native60完整1/60失败，非60个独立bug；Root及accepted-only clean各21入口实际全0，
含新native60、原完整数组库、80/96/294/80/190/654及三个真实Main。
每侧75对象17当前程序/127头核对；fresh1生产2native，57生产11native/stub/3Main为匹配donor，
全部新链接当前库；Root normal1CPP并链接、repeat零编译零链接，clean显式编译。
2204已接受源码投影与tested clean相同，Root2323保留外来改动及2322源码/118缓存恢复副本。
跟踪603份MD、920native+567注册Python+3Main=1490入口；Root606份MD、926+573+3=1502，
原12未接受入口分离；[R182拆分性能](issue-review-r182-utf8-split-linear.md)仍未接受，V1失败原样保留，需新版本继续。
R164仍未解决，R180及其他暂缓不恢复；原273仍22/166/70/15，总目标未完成。
不是全1490/完整72/TLS/整库sanitizer验收；不push，Actions禁用。以下记录按当时状态解释。

最新开放：[R182共享UTF-8拆分](issue-review-r182-utf8-split-linear.md)已完成PG329与旧版329全0对照，
候选已编译但未验收；首轮原生检查因错误ARRAY类型期望及真实opaque回调数组强转异常而中止，完整失败保留。
[R183 STRING_TO_ARRAY回调身份](issue-review-r183-string-to-array-callback-identity.md)单列真实缺陷，
先独立修复实际callee元数据，再恢复R182严格验证；不删除失败检查、不将候选计作已修复。
跟踪603份MD、919native+567注册Python+3Main=1489入口；Root606份MD、925+573+3=1501，
新增文档不等于新增接受测试，原12未接受入口仍分离。R164未解决，R180等暂缓不恢复。
原273仍22/166/70/15，总目标未完成；不push，Actions禁用。以下记录按当时状态解释。

最新有限修复：[R181 REVERSE线性扫描](issue-review-r181-reverse-linear.md)消除长文本反转的平方级前缀扫描，
保留UTF-8码点、TEXT/CHAR、NULL、BYTEA及实参求值语义；这是性能修复，不是旧版兼容性错值。
PG完整96、旧版完整96及native80语义全0；Root及accepted-only clean各19入口实际全0，
包含新80/96、原294/80/190/654及三个新链接当前库的真实Main，clean本轮fresh3、Root验证复用3。
每侧73对象15程序/127头已核对；fresh1生产/2native，57生产/9native/stub为已验证donor。
Root normal1CPP并链接、repeat零编译零链接；clean显式编译，未冒称normal/repeat或donor fresh。
全部2203已接受源码投影与tested clean一致；Root2322保留其他改动及2320源码/118缓存恢复副本。
跟踪601份MD、919native+567注册Python+3Main=1489入口；工作区604份MD、925+573+3=1501，
原12未接受入口和三份文档仍分离；不是1489全量、完整72、TLS或整库sanitizer验收。
R180按最新skip it暂缓，其他暂缓不恢复；R164超时仍未解决。原273仍22/166/70/15，总目标未完成。
不push，Actions禁用。以下记录中的状态和数量均指记录当时。

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

本轮按用户要求进行全仓库审查、复现和文档整理，结果与逐项修复方法见[审查报告](code-review-2026-10-13.md)，文件覆盖见[文档清单](documentation-inventory.md)。
下方批次中的“当前”“正在运行”和 PASS 数均属于对应记录日期。此前 Root 60 native / 46 wire 验证已经终止：60/60、45/46 通过，唯一失败为 ALTER ARRAY 超时；不能继续标成运行中或全绿。
原 273 项状态仍为 22 complete、166 partial、70 unverified、15 deferred_by_user；CTAS／后台 checkpoint 另按用户要求暂缓，详见[范围说明](deferred-scope.md)。
本轮新运行的终态以审查报告为准。保留独立本地 commit，不 push，Actions 保持禁用。

## 2026-10-15后续执行计划

[R108数组元素相等](issue-review-r108-array-position-typed-equality.md)已完成
354原生/258协议及32native/29协议/3真实Main的有限验收，按本项独立本地commit，
继续原273其他普通问题。参考258零失败、旧258/158及各版失败保留；不把
1403接受组合或1407含R98工作区说成新全量通过，不重开R106或其他明确暂缓项。

后续[R107长度函数签名](issue-review-r107-string-length-signatures.md)针对四名
11条实际声明逐版复现/修复，首两版失败保留；完成本项匹配构建、核心、有限
相邻回归及独立commit后继续原273其余普通问题，再另行安排冻结新组合的完整
注册验收。不得把含未接受R98的工作区描述为仅HEAD的干净全量。R106已由用户
明确跳过，保留草稿/证据，不因继续而恢复；原273完成状态不提升。

[R92缺失TOAST计数器](issue-review-r92-toast-missing-counter.md)已完成旧实现
数据损坏复现和有限修复：30完整native通过、18/20协议通过，CREATE/定长ALTER
超时仍未解决。下一项按原完整驱动观测普通DDL失败线程，进入暂缓范围则保留
边界而不重开专项。后续两个原完整驱动各一次外部观察均0，未触发原TimeoutError
或取栈；不计验收，不关闭两个原FAILED。继续聚合、数组等原未完要求，新1384
组合尚未全量运行。

最新[冻结332bcff4全量](issue-full-registered-332bcff4.md)已实际终1：1382入口
各一次，1379实际通过/1 TLS环境skip/2失败；独立输入/58 receipt/57 test库
字节核对0。新增14入口和旧六失败入口均通过，但stored-function-owner末尾
DROP断言与SQRT准备阶段CREATE SCHEMA超时仍须保留；R51不重开，SQRT算法
并未进入46项控制。终态已独立提交，TOAST缺失计数器后续基线/修复见上述R92；
普通CREATE超时、聚合/数组等候选与原273未完
要求继续。无源码草案代替执行，无局部/全量回归代替原要求完成。

以下为先前各修复完成时的计划与证据，后续1382组合现已实际运行：
[冻结27bac6ee全量](issue-full-registered-27bac6ee.md)已完整终态：1368入口，
1361实际通过/1 TLS环境skip/6失败，全部输入及58 receipt/57 test库字节复核0。
先逐项保存失败，再用原完整COPY作单次外部诊断，随后对普通数组/数值/聚合
候选先旧基线、再修复/回归/独立commit。不放宽断言/期限，不重开用户暂缓项。
下面曾经的“下一步启动1368”计划已经实际执行，不再表示该轮仍待启动。

随后[R86空数组NULL起点](issue-review-r86-array-position-empty-null-start.md)已通过
208新原生/九完整native/七完整协议及PG对应80控制。新注册组合1370入口尚未
全量运行，原1368失败保留。下一项继续普通数值域/统计维护/聚合问题，先
实际旧基线，再逐根因修复，不以已有局部通过关闭原273总清单。

后续[R87 MOD结果域](issue-review-r87-mod-result-domains.md)最终254+153控制、
十四完整native/十一完整协议及真实PG130全0；首版native回归已保留并修正。
新注册组合1373入口未全量运行，COPY同步ANALYZE/长值发布成本、ROUND声明、
布尔聚合及通用R62仍待独立验证/修复。原六个冻结失败不由本项改写为通过。

[R88统计排序](issue-review-r88-analyze-sort-comparison-cost.md)已将32个相同
长文本的locale比较从243次降到1次，保持统计值/NULL/类型/locale语义；十四
完整native、96列/运行控制及六完整统计协议均0。原完整COPY无插桩/原15秒
仍在line262超时，不关闭R82或猜测其他超时同根因。新1375组合未全量运行；
接着分别验证ROUND有限声明、布尔聚合及剩余普通维护成本，独立commit。

[R89 ROUND单参数](issue-review-r89-round-single-result-domains.md)已有旧124/48
失败、91协议/44失败及PG91全0基线；首版native97/6回归保留，收窄输入域后
最终124/97、十七完整native与十二完整协议均0。新组合1378入口尚未全量
运行，原COPY和其它五项冻结失败仍未关闭；下项继续普通聚合/维护问题。

[R90长值ID预留](issue-review-r90-toast-durable-id-reservations.md)实际旧64次
分配需64次发布，最终为1次；保留文件/父目录同步、坏counter拒绝和真实
SIGKILL/并发/上界回归。首版末尾区间断言失败保留并修正；原完整COPY固定
三次无插桩/原15秒通过，21不同完整native及七完整协议邻居全0。其余四个
历史普通协议失败入口复查为默认PG/LATERAL/CHECK过，MV在SAVEPOINT超时
（前一sentinel65）；不冒共享根因。接着诊断实际SAVEPOINT路径，新1380组合
未全量运行，不能据此关闭原273或其余失败。

[R91保存点运行态](issue-review-r91-savepoint-sequence-runtime-images.md)针对实际
序列推进造成的映像复制放大，保留完整校验后将native16份/真实前端71份都降
为1份。23完整native、同顺序四个原失败入口及MV固定三次、十四完整协议邻居
全0；不把其它历史间歇性超时推断为共享根因。875 native +3真实Main +504
Python=1382入口将在本项清洁提交后运行原始全注册回归，期间冻结tracked
输入/HEAD；证据目录 `/tmp/dbms-review-full-r91.hdjkSELm/`，实际启动/终态以
原始runner日志和进程句柄为准。本段只是计划，不是1382全量通过证明。

本次跳过R65，保留未验收补丁和失败证据，不合入其产品代码。
R67普通视图替换以ca554c2f合入master，Root八完整native/九完整协议（含新80检查）
全0；R69短会话超时夹具以a3b3b4b9/d52779a0独立提交，Root完整PG、真实取消
协议和CLI串行全0。记录见[逐项修复进度](review-fixes-2026-10-13.md)。

R62已完成ABS/upper/lower/length四组声明签名；后续[R83十八固定float8名字](issue-review-r83-float8-scalar-signatures.md)
二十完整native及新951协议已通过、完整协议16/17通过（完整PG的CREATE超时
仍未解决），通用回调/UDF范围仍partial；
R68整数ABS范围及空输入常量分析已验收。R70实际全文类型身份、R71目标目录
事务发布、R72别名锁去重、R73纯常量绑定、R74 owner夹具及R75异常锁释放均
已分别本地commit。原三个generation/snapshot、两个owner、extended literal及
目录guard完整驱动均已实际复验0，不改写原冻结失败。

R76/R77本范围分别验收并提交；[R78恢复轮询](issue-review-r78-restore-poll-publication.md)
保留成功断言和原期限、仅重试真实publication冲突，完整DIV14/真实restore-stage
及五组负控全0。[冻结b5def9ba全量](issue-full-registered-b5def9ba.md)已经终态：
1360入口、1358 PASSED退出（含TLS skip）、2 FAILED，全部入口覆盖/冻结fence一致。
前次20个失败入口本轮均0；新暴露的复杂未绑定参数由R80 `6456ad4f`修复，
原生二进制LENGTH由R79 `7c294fe7`修复，分别有失败复现、完整邻居和独立commit。

2026-10-15新组合[冻结d974c80d全量](issue-full-registered-d974c80d.md)已终态：
1362入口，1359 PASSED退出（含TLS skip）及3 FAILED；无遗漏/重复，全部输入/
binary/cache/58 receipt与独立只读终态检查一致。R79/R80两个原失败均0，三个
新观测不能改写为通过，也不证明共享根因。

当前执行顺序：

1. 已完成[R81恢复阶段夹具](issue-review-r81-restore-stage-completion.md)，`a7f0e404`：
   五次真实恢复及完整PG/取消/CLI/COPY/DIV14等七入口串行全0；不改产品或20秒期限。
2. 已复查504份Markdown、831个本地内联目标、519 Python AST/13 shell，以及
   499注册入口/481独立E2E和六个总账单测，全部0；提交冻结证据和三个失败的
   定位方法。历史503份检查不改写，文档检查不代表所有技术结论当前已验收。
3. 使用原完整JOIN和bound DML协议驱动、原期限、串行观测复现setup和ROLLBACK TO
   timeout；记录具体SQL、耗时及服务端线程状态，按确证根因独立修复/回归/commit。
   初步复验JOIN先再现同位置超时，后原样完整通过；bound DML独立37项通过。
   无产品改动，间歇性失败仍未解决，诊断观察通过不代替修复证明。
   prepared DML末尾DROP失败保留，R51暂缓范围不因通用继续而重开，不猜测为相同根因。
4. 继续R62其余通用函数签名及原未完成普通问题；有产品变更后重新匹配构建和门禁，
   再另行冻结全注册回归。不删原失败、放宽断言或增加期限来宣布全绿。

本次R83新增两个原生和一个注册协议驱动；最新组合为862 native + 3真实Main
frontend + 500 Python = 1365入口，尚未执行这套新组合全量。Markdown505份、
本地inline目标839次、Python AST520、shell13，注册500 unique/482独立E2E均通过。
旧1362完整终态和504份检查属于前次输入，不由本次定向通过改写。

后续[R84取整结果声明](issue-review-r84-integral-rounding-result-types.md)已通过
新310 native及Root/PG266协议；21个不同native均有完整通过记录（首批DROP清理
失败保留），完整协议16/17通过（COPY建表准备超时）。
[R85 SIGN typed numeric NULL](issue-review-r85-numeric-sign-null.md)的新25原生、
十一完整native、八完整协议和对应20 PG/Root协议已通过。
最新实际组合为864 native + 3真实Main frontend + 501 Python = 1368入口；
下一步在本段文档提交后以清洁HEAD启动原始 `bash scripts/build_tests.sh`，不
过滤入口、改断言或期限；先逐字节复制当前Root57个库对象并fresh编译stubs，
全58个receipt及主binary/build stamp复核后冻结，运行期间不改tracked输入/HEAD。
预留证据目录 `/tmp/dbms-review-full-20261015-r85.6HbDjOD6/`；实际运行/终态以
该目录原始runner日志为准，本计划不是已通过证明。前述1365组合没有启动。
新增COPY fsync只读诊断记录见[R82后续观测](issue-full-registered-d974c80d.md)，
插桩通过不代替无插桩验收，未捕获真实超时时的根因。

按[准确暂缓范围](deferred-scope.md)保持已跳过模块，不自动扩展到普通SQL或存储错误。
新组合需要重新运行全注册入口；本轮定向通过不能完成原273总账或把历史FAILED改绿。
总目标尚未全部完成；各原partial/unverified与15项原始用户暂缓状态保持不变。

以下为历史执行记录。

2026-10-06 PL scalar/native follow-ups 两项独立source/test commits：`b6683521` 修复真实native SELECT/WHERE/ORDER/INTO表达式错误从22012/22P02/22003被吞为XX000；`8cd860e7` 用canonical AST位置绑定修复quoted "X"/x变量值/NULL/type混淆，保留BIGINT宽度、session特殊值、EXTRACT语法和显式trigger binding，并拒绝不存在的变量/qualifier/function。旧native/wire实际红、独立development新storage/stubs的3native及2protocol绿均有记录。ROOT正式O2只重编变更storage对象，其他54对象严格复核匹配，normal/repeat build、55/55签名及binary stamp通过；最终matching O2的6native及7项专项/相邻protocol通过。完整默认协议本follow-up未重跑；前一615/7bc组合在main3170 INSERT kw_joined timeout exit1，之前三次完整超时也保留，不能宣称完整protocol/suite通过。6项ledger unit/3项文档版本兼容检查通过，require-complete仍exit1。见 `docs/issue-plpgsql-scalar-binding-and-native-errors.md`。query内variable/source-column歧义、函数错误后写入不原子及statement-image写放大仍独立未完成；SQL-04/FUNC-05/06仍partial。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push、Actions禁用、用户安全/TDE跳过项保持deferred。

2026-10-06 cold recovery / PL/pgSQL 两项独立修复已提交：`615f2c54` 修复真实 SIGKILL 后全新 exec 访问未初始化 database-lock map 的冷启动崩溃；`7bc76204` 保留完整 SELECT INTO 查询、structured typed first-row/NULL、STRICT/FOUND、声明/赋值/返回类型与错误码、stored CTE scope 及 native checked fallback。独立 fixture commit `acaa0bea` 用真实表达式 evaluator 替换旧 native echo host，声明 runaway counter 并精确断言未降低的步数上限；旧 O0 exit137 与 O2 RSS约53.5GiB后主动TERM的失败均保留，纠正fixture后O0 1.39秒/15420KiB通过。最终 development 7项专项/相邻协议及3项native通过；合并正式优化版55对象重编、normal/repeat build、55/55签名及binary stamp通过，10项专项/相邻协议及6项matching native通过。完整默认协议本轮在main3170 INSERT kw_joined处socket timeout exit1；此前两次JOIN完整超时与第三次instrumented CREATE TABLE超时也保留，snapshot写放大未修，不宣称全协议通过。6项ledger unit及3项文档/版本/兼容检查通过；全注册suite/TLS runtime/PG18.6 differential未跑。见 `docs/issue-cold-start-transaction-lock-registry.md` 和 `docs/issue-plpgsql-select-into-typed-query.md`。quoted scalar X/x复合绑定错值与函数P0002/22P02后写CTE仍落盘已独立复现，正在分别修复；FUNC-05/06、SQL-04、WAL-05/08、TXN-05/07仍partial。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push、Actions禁用、用户跳过安全/TDE项保持deferred。

# 工作区与复查清单收尾计划

## 2026-10-13 当前执行入口

最新计划、独立提交及实际验证见[逐项修复进度](review-fixes-2026-10-13.md)。
先完成其未关闭问题，再重新运行当前完整注册回归、更新全部文档检查；旧
批次的运行状态及 PASS 数只属于当时输入，不作为当前全量完成证明。
原273项总账和用户暂缓范围保持独立，不 push、不启用 Actions。

以下为历史执行记录。

## 2026-10-10 当前执行顺序：USER与version之后

1. USER `ff9d75a3`、version `633dc2f4` 已逐项独立合入master。前一Root61841
   实际57native全过、41/42wire过，仅USER；私有USER81549实际13native/14of15wire
   过，仅下一version。最终私有98287实际14native/16wire全过，原fromless/default
   全过；所有首尾58对象/source/cache/frozen检查0。各失败原样保留。
2. Root因USER header已真实own fresh57/Main38420/93741均0，57/1实际编译。
   新Root58native/43wire匹配publication正在运行；不得把私有绿当Root验收。
3. 完成Root组合回归后运行最新完整784auto+2Main/443wire注册组合；当前已有
   quoted enum57761真实134继续优先修复，再处理目录race、TEMP/recovery、索引
   corruption、typed trigger、EXPLAIN及原273清单其他要求。只明确15安全/TDE跳过。
4. 原273仍22complete166partial70unverified15deferred。保持每项本地commit，
   不push或启用Actions，不用专项通过代替全要求完成。

## 2026-10-10 先前执行记录：类型与参数修复之后

1. README已在 `f513ac1f` 整理为长期项目入口，本次链接/契约/版本核对全0。
   日期fixture `dbfc241a` 与四项独立实现修复 `9c7100f8`/`1da68f6e`/
   `7b61abf9`/`1f8232b5` 均已本地提交；不push。
2. 最新私有85985终1：11native全过、14/15wire过，default与原350整数/
   250BIT参数控制全部通过；仅原fromless的USER42703未修。所有旧红矩阵保留。
   主工作区先前两个修复的独立O2/58签名/native均0；新Root own62792/Main45597
   正在构建。完成后做匹配构建/回归，不借私有缓存或把私有结果当作Root验收。
3. 下一项修复bare USER语法/身份，再处理原quoted enum、目录加载race、TEMP/
   recovery、index corruption、typed trigger、EXPLAIN及完整原清单要求。
   新全注册组合未跑；以前完整回归的12项失败逐项表不得删改为已完成。
4. 原273保持22complete166partial70unverified15deferred；仅明确安全/TDE专项
   跳过，不能用专项通过或拒绝未实现路径来宣称总清单全部完成。Actions保持禁用。

## 2026-10-10 先前执行记录

1. 原完整85873终1，769/775native(Main含)、431/437wire通过；12失败表见
   `docs/issue-full-registered-a85e653e.md`。all1212终态/all58/source/frozen一致，
   不冒全套通过，原default/两个Main确实通过。
2. 七项修复/fixture已逐项-x合入master：NAME、Gen绑定正控、结果OID/VOID、
   lazy Gen、限定DROP、procedure namespace、PL intrinsic；最新source249f4fe1，
   196scoped/780auto+2Main/440wire/58TU，own fresh57/Main91787/31765均0，实际
   57/1编译；normal/repeat/all58/cache/source/frozen start均0。53native/36wire
   publication86219终1：53/53native、35/36wire通过，default/all58/repeat/source/
   frozen终态检查0；仅fromless混合pg_typeof失败。Date native AST修正已独立
   合入 `dbfc241a`；不用私有obj/binary。
3. pg_typeof receiver、quoted enum、catalog加载race、
   recovery、index corruption、typed trigger与EXPLAIN失败继续逐项复现修复提交。
   当前没有新Root组合全量结果；所有原273族仍须按完整要求验收。
4. 保持原273的22complete166partial70unverified15deferred；仅明确15安全/TDE
   专项跳过，不扩大deferred，不push或启用Actions。README仍为长期项目入口。

## 2026-10-09 当前执行计划

1. README 长期项目入口已独立 commit `cfcab3cd` / `f513ac1f`；不放临时进度或测试统计。
2. ROW 已独立 commit/import `99c155ee` / `bec026c7`，private26native/16wire
   全0；Root自身fresh57/Main59900/66760、26native/16wire50787与BIT384/99693
   现全部终0，all58 receipts/cache/repeat/source/frozen终态一致。
3. 完整注册回归18383已终1：769/773native/Main、427/435wire通过，12失败
   保留，all58/source/frozen终态一致；不改输入/断言/期限、不冒全量通过。
4. 普通同名关系DML已独立private commit `34d5d956`，完整27native/18wire原样
   复跑终0、首组失败保留。SQL-value
   范围内修复已private commit `217bdcb9`，校正33native全0、19/21wire通过，
   NAME原失败和default超时保留，不冒整轮绿。普通SQL routine错误arity已private
   commit `94eea9bf`：50强控和33native全0、21/23wire通过，原NAME/TEMP超时保留；
   public-parser API已private commit `dc534a93`：旧134保留，新49和33native全0、
   21/23wire通过，NAME与原default超时保留。四项已分别-x合入Root
   `deb49f76`/`86ba021d`/`8eed940c`/`6fc40db8`，own fresh57/Main97646/52631
   全部终0，Root自身publication3492=1：33native全0、22/23wire通过，唯一NAME
   五控仍失败；原default和BIT384/5131通过，all58/source/frozen终态一致。
   完整775/437新driver85873已实际启动并冻结Root输入。private NAME登记已
   独立commit `e55c863e`：原33/5fail -> 新33全0，类型registry单元/actual18参考0。
   普通函数结果OID修复已private `d0c5582c`，包含实际VOID目录前置；初始35/23
   14272=1（Gen与原pg_notify失败）保留，校正36/23的53041=1：35native与全部
   23wire通过、原default/pg_notify断言和BIT384通过、all58终态一致，仅Gen失败。
   合法Gen绑定正控已独立fixture commit `3d9b1a61`，不改负期待；下一项实现真实
   lazy provider/cursor及完整overload/NULL/step/overflow/需求语义，不用cap/eager假替代。
   继续所有普通声明/查询/storage/recovery/EXPLAIN未完成项及native decimal API；
   按[准确跳过范围](deferred-scope.md)只保留15安全/TDE deferred，不扩成普通失败跳过。
5. 原273范围/状态不变22complete166partial70unverified15deferred；不push、
   不启用Actions、不重启跳过专项、不以 scoped 通过宣布原整个 family 完成。

当前190 scoped、773auto-native+2Main、437registered、58TU。完整证据见
[Root ROW 验证](issue-root-row-publication.md)。以下为历史执行计划。

SQL-value提交与arity后续详见[独立修复证据](issue-private-sql-value-repair.md)。
原全量终态和四项导入见[完整回归记录](issue-full-registered-795479c6.md)。
新Root自身证明见[SQL-value发布验证](issue-root-sql-value-publication.md)。

## 2026-10-08 TypeName 声明语法与完整后续计划

独立候选 `e4f16819` 修复 CAST/:: 声明 envelope，保原68断言：Root基线40错
-> 新全0；完整35native/原28whole全0。完整TypeName45仍38错（基线41错），
reference45全0，永久保全45并注册，不能用28绿冒更广29或type族完成。
继续generic typed-constant语法、真实catalog/namespace/OID、输入转换与
静态descriptor，以及原BIT384仍16红和全部原未完成要求。正式source仍ea67/
123，八enum候选另保持HELD，所有原273 scope/status和安全TDE跳过项不变。
证据与具体计划：`docs/issue-typename-current-parser-checkpoint.md`。

## 2026-10-08 枚举候选暂缓合入

README `9603c8ab` 已完成。八项枚举候选分别本地commit至 `12bbf068`，
正式source仍为 `ea67ad29` /123项。真实fresh58 producer、完整39native通过；
协议consumer原完整32通过且39条真实OID失败消除，但新增phase fixture
仍164检查/67失败，reference166/0，不能虚报完整33或总目标完成。
qualified callee执行owner已独立修复：普通INTEGER32基线8错 -> 新0，
current完整native39、专项33（保全部原32）、reference21全0，仍非whole34
或整组approval。继续未闭环TypeName/catalog/typmod等；保留新旧失败fixture，
不重启或改写已保留CREATE失败分支。全部273要求与跳过项继续保留。
计划、各commit和完整证据见 `docs/issue-enum-current-candidate-hold.md`。

## 2026-10-08 当前完整273目标计划（ea67；以下较早记录均历史）

当前sourceea67ad29，728auto+实际Main frontend2/402registered/58TU，123独立
Root source/test修复；原273全部scope、每原红和跳过项保留。

| 阶段 | 原要求 / 下一动作 | 当前真实终态证据 |
| --- | --- | --- |
| 每真实根因逐commit | 个别commit、用户push | ff77/fb58/40ed/ea67四production，8798单独factory更正；master仅FF |
| 当前真实ABI和组合 | physical input/Parse声明/NULL真角色/需求 | 84086 actual4freshCPP54当期Root118逐对象证明normal，all58/repeat/seal0；34native24253/28whole3004/strict15 93608全0 |
| 主目录程序 | matching58/自身receipts/直接复测 | publication25665全58证明迁移/relink/repeat零fresh0，048cSHA/old3194可恢复；direct28 59889真0/all58及inputs/freeze前后不变 |
| 保留全部原红 | 不冒type/SQL/family/full闭环 | 真original384paired89931=1/16VARBIT literal；parent32仅两旧COUNT FILTER；历史INTEGER87full88和source80full1不改绿 |
| 下一真实类型owner | VARBIT TypeName/catalog/typmods/inputOID/OID0 | 稳定新Root Origin及fb58 MetadataPlaceholder，按actual grammar/catalog/input桥修，不用类型名猜；16原SQL/完整强矩阵不减 |
| 其它每原scope | MINMAX/customOID/COUNT/CTE/scalar/UNKNOWN/catalog/storage/recovery/ops | 原273不变22complete166partial70unverified15deferred/gate拒绝，123原full/SAN未跑，逐项根因/测试/commit继续 |

当前direct28已实际终0，总账QA后commit证据；继续上述所有原未闭环。完整出处：
`docs/issue-bit-null-current-root-composition.md`。不assistantpush/启用Actions/
用户跳过securityTDE或filtered fixture重启。

## 2026-10-08 历史完整273目标计划（1866）

当前source18664795，725auto+实际Main frontend2/396registered/58TU，118项独立
Root source/test修复。README9603维护不计；原273完整scope和每旧红保留。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 每真实根因逐commit | 独立历史、用户push | 七production独立1e2/82f/99b/258/dfc/4f7/186，另e49真正INTEGER覆盖更正；master仅FF |
| 当前真实ABI和组合 | origin/typedchild/context/INTEGER/whole-query demand | 真fresh58 core5065；最终4freshCPP54自身ABI证明normal85558；31native70472/22whole97362/strict9 78808全0，原9649含70全部0 |
| 主目录实际程序 | matching58正常对象/自身receipts/直接完整复测 | publication29443 all58迁移/自身path receipts/relink/repeat零fresh全0，actual3194SHA/oldc7可恢复；direct22 20876真0/前后all58和inputs不变 |
| 保留原完整红 | 不删SQL/assert/注册，不借小gate关闭族 | parent32仅同两旧COUNT FILTER失败；core9649 4116错及五whole基线保留；历史INTEGER87完整88仍1 |
| 下一原输入consumer | BIT反向physical owner/真SQL NULL/TypeName/MINMAX/customOID | 已独立复查BIT15d及NULL98f所有source/fixtures，需当前Root composition与真正baseline/gates，不借旧Root104 ABI绿 |
| 所有原scope继续 | COUNT/CTE/scalar/UNKNOWN/catalog/storage/recovery/ops | 不改原273要求：22complete166partial70unverified15deferred/gate拒绝；118原full/SAN未跑，source80full1/all1043历史保留 |

下一动作：当前direct22已实际终0，总账QA后commit checkpoint；分别组合实际BIT reverse
owner和nullable WHERE consumer，在当前origin/lazy/public ABI下验证，再继续真实
VARBIT TypeName与MINMAX及所有原未闭环需求。全出处：
`docs/issue-parameter-integer-current-root-composition.md`。不push/启用Actions/
重启用户跳过securityTDE或filtered fixtures。

## 2026-10-08 历史完整273目标计划（b888）

当前source b8882d04，714auto+实际frontend2/387registered/58TU，110项独立
Root source/test修复。README9603已单独commit，不计作数据库修复；原273范围不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 每真实根因逐commit | 独立commit、用户push | 0e6空窗口准入、b888独立NTH peer frame；master FF，两项独立历史保留 |
| 当前完整窗口组合 | 真实Main/NULL/空/FALSE/cap/arity/frame/type | full84 old37/strict0/admission1；独立80 old10/prefix4/strict0；最终7whole95717/5native35060/真实frontend58 24206全0 |
| 主目录实际程序 | 全58 proof/正常repeat/实际完整复测 |62119 matching迁移/自身receipts零fresh/relink/repeat0；direct7whole1868全0，实际c7SHA，old0d可恢复 |
| 剩余窗口scope | callee/dynamic参数/position/qualified/named/inherited/frame | QRY08仍partial，下一项保持真实metadata/grammar/输入owner并独立基线与完整测试 |
| 当前其它原owner | Origin/typedchild/memo/BIT/TypeName/NULL/MINMAX/customOID | private finite commits须全源码复查/当前Root公共头fresh58/强gate，不能借旧ABI或私树绿直接发布 |
| 保持全部原红 | 不删SQL/assert/注册、不冒族闭环 |INTEGER99native/19post0不替代原88whole1，旧OPEN4/full8/其它原scope保留 |
| 原full与273全scope | 全部原需求真实证明 |当前110原full/SAN未开始；source80full1/all1043历史；273/hash不变22complete166partial70unverified15deferred/gate拒绝 |

继续原全部query/catalog/storage/recovery/ops及未闭环项，不push/启用Actions/
重启用户跳过专项。完整出处 `docs/issue-unordered-window-current-root-composition.md`。

## 2026-10-08 历史完整273目标计划（5386）

当前source53865cec，714auto+实际frontend2/385registered/58TU，108项独立
Root source/test修复。原273范围及未闭环要求不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 每真实根因逐commit | 独立commit、用户push |5386私有窗口方向初始化；保7041/3cc及作者runner失败证据，master仅FF |
| 当前实际窗口组合 | 同真实前端、完整旧强控、正式构建 | 旧58/48红、修正58/0；完整5native27825/5whole58588/正式22632全部0；原正式16887缺目录1保留 |
| 主目录实际程序 | 逐对象proof/正常repeat/直接完整复测 |87796全58正常对象证明迁移零fresh/relink/repeat0，实际0dSHA；直接完整5whole79548全0，旧672可恢复 |
| 接续真正窗口admission | 无PARTITION/ORDER仍要真实Window角色/正确值与类型 |完整24实际PG0错/current9错+2错零行；独立guard未修，不关闭QRY08 |
| 保持每原红 | 不删SQL/assert/注册、不给族虚绿 |INTEGER99native/19post0但88whole1未批准；oldOPEN4/full8/全原scope保留，source80full1/all1043历史 |
| 当前所有其它owner | 真来源/typedchild/memo/BIT/TypeName/NULL/MINMAX/customOID |新公共头须真实当前fresh58/强gate；不借旧ABI，现全部原未闭环继续 |
| 原full和总账全scope | 原发现/注册全部终态、原273全部证明 |新108原full/SAN未开始；273/hash不变22complete166partial70unverified15deferred，完成gate拒绝 |

下一动作：按真实PG参考独立修plain OVER窗口admission并保护错arity/函数owner/
空行/false/cap/NULL/frame/type边界；继续其它原query/catalog/storage/recovery/
ops requirements，不以当前小gate代替全部证明，不push/启用Actions/filtered重启。
完整出处见 `docs/issue-window-default-current-root-composition.md`。

## 2026-10-08 历史完整273目标计划（29f9）

当前新增验证不等于合入：精确INTEGER完整99native0/19post0，但完整88whole1，
87pass及一真实socket超时均保留，未入master/未关闭族，不以19替代88。
独立private窗口排序方向初始化7041 commit：真实旧前端58/48红、五完整native0；
候选正常版/真实前端/完整窗口协议仍验证，完成后才批准实际主目录发布。
完整scope/logs见 `docs/issue-integer-current-root-held-verification.md`。

当前source `29f90184`：714auto+frontend/385registered/58TU，
107项独立source/test修复。原273全部要求及未闭环scope不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 每真实根因逐commit | 独立commit，用户push | f7c DISTINCT原预期纠正、aeac TEXT width、29f NULL Bind，各独立commit/master仅FF |
| 当前强组合 | 正常ABI/receipts/默认磁盘期限/旧强gate | normal59239真1freshNetwork57证明donors，完整94native34779/78whole28991/14post67075全0 |
| 真实主目录可用 | 全对象proof/正常repeat/直接完整复测 | publication3939全58证明迁移零freshCPP正常重链0；实际672SHA，直接14whole99047全0，旧3d可恢复 |
| 每原失败保留 | 不删SQL/assert/注册红，不冒族关闭 | 当前OPEN4whole37221真1/旧CTE11 scalar18；原OPEN5历史真红，DISTINCT原6SQL+38强strict/ours0才独立纠正 |
| BETWEEN实际来源 | 新row/computedNULL omissions复现后正确修producer/copy | strict32全0、旧Root104仅2旧COUNT FILTER、d14候选28红即26新漏调用；未入master，新真实origin字段必须genuine58及全原587/190/20+Root邻居 |
| 接续所有原输入和consumer | INTEGER/VARBIT TypeName/Param、MINMAX/customOID/其它FETCH | 三INTEGER私commit/强矩阵仅private READY；BIT/enum各真实独立owner继续，不能用旧ABI或私绿替代Root批准 |
| 原full和每原family | 原发现/注册完整终态及全部273证明 | 旧80full42496终1/all1043非当前107；新107full未启动，全部原catalog/query/storage/recovery/ops继续 |
| 总账约束 | 原checkbox/证据/commit全scope一致 | 原273与hash不变：22complete166partial70unverified15deferred；完成gate拒绝，不push/启用Actions/安全TDE或filtered重启 |

下一动作：独立修真实Parameter/Datum来源、精确组合INTEGER三issue并保d14
两个比较的真实需求/输入转型与完整9649含70共享参数；全新header/current58
和强完整Root gates后才合入。继续全部其它原未闭环根因，不关闭任何未证族。
完整出处见typed-null current-root组合文档。

## 2026-10-08 历史完整273目标计划（af7d）

当前source `af7d46c8`：713auto+frontend/383registered/58TU，104项独立
source/test修复commit。原273及所有未闭环要求完整保留。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 逐项版本管理 | 每实际根因独立commit，用户push | 四新issue分别1f3a/4906/9cf9/af7d，独立shadow组合后master仅FF |
| 当前完整组合 | 真ABI/inputs/receipts/默认磁盘和期限 | 真fresh58 normal92632，完整93native99777/71whole70736/15post31826全0 |
| 主目录可用binary | 正常build/repeat/逐对象proof/直接复测 | publication20012迁移全58证明正常对象与自身receipts重链0；实际3dSHA，直接15whole86345全0，旧bf33可恢复 |
| 原强红全保留 | 不删SQL/assert/注册红，不冒族关闭 | OPEN5whole35004真1：CTE11/scalar18/旧DISTINCT；原80full42496终1/all1043 |
| 接续协议和输入 | TEXT width、NULL Bind、INTEGER/VARBIT真实owner | NULL两私树独立commit、真1freshNetwork+57当前证明donors；完整94native/78whole实际运行，非master批准；INTEGER/VARBIT另实际强差分 |
| 接续真实需求 | BETWEEN、aggregate runtime cells、各FETCH实际consumer | 新BETWEEN私树全58真构建；严格32聚合来源控制0，Root104完整基线保旧COUNT FILTER两红，其余30真实row/computed NULL效应均0，待候选对照 |
| 每原full/family | 原发现/注册终态及原273全证据 | 新104原full未启动；MINMAX/customOID/其它FETCH/COUNT/UNKNOWN/CASE/all原catalog/query/storage/recovery/ops继续 |
| 总账与约束 | 原checkbox/证据/commit全scope一致 | 273=22complete166partial70unverified15deferred，完成gate拒绝；不push/启用Actions/重启跳过安全TDE |

下一动作：完成两个协议issue当期完整gate并精确逐commit合入；核实BETWEEN
是否误将实际聚合行/计算结果Parameter当输入常量，复现后修真实producer/copy
角色，不凭slot/NULL值猜。继续其它原未闭环根因及完整族验证。
完整出处见public-consumers current-root组合文档。

## 2026-10-07 历史完整273目标计划（f323）

当前source `f32327cd`：708auto+frontend/379registered/58TU，100项独立
source/test修复commit；原273全部范围及每个未闭环要求不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 逐项版本管理 | 每真实根因独立commit，用户push | 新f323 BETWEEN实际输入/参数精确五CPP独立commit并FF，当前基线全2008及四完整whole真红保留 |
| 当前完整组合 | 真ABI/inputs/receipts/defaultdisk/deadline | normal41819真5fresh+53当前7f证明donors，50native7432/61whole71725/post25whole27937全0；非fresh58/SAN |
| 主目录可用binary | 正常build/repeat/逐对象proof/实际完整复测 | publication75748真58证明迁移/重链0，实际./dbms_main同bf33SHA；直接25whole3212全0，旧source99 binary可恢复 |
| 每原强红保留 | 不删SQL/assert/注册红，不冒族关闭 | OPEN3whole42220同旧11红；原7矩阵132=0、384=1/23、1296=1/26、250/40/50=0、writer12=1/6 |
| 接续actual inputs/demand | INTEGER ranges、VARBIT真正type identity与原效应 | 非BIT26、typedVARBIT16/physicalprefix7、真实writer6继续；真实strict证明search_path domain VARBIT非内置BIT |
| 接续新ABI/消费者 | enum rank/SUM metadata、positive/signedFETCH全部Root组合 | 四独立commit只在新current100私树；两公共头真fresh58 normal92632 live，四完整当前基线红/strict全0；之后完整默认磁盘邻居和OPEN原强gate |
| 原full和全部family | 原发现/注册真实终态，每原273证据 | 原80full42496真1/all1043非当前100，新100full未启动；所有原catalog/query/storage/recovery/ops未闭环范围保留 |
| 最终总账与约束 | 原checkbox/证据/commit全scope一致 | 273=22complete166partial70unverified15deferred，完成gate拒绝；无assistantpush/Actions启用/安全TDE恢复 |

下一动作：完成当期新公共头全58正常构建和四issue完整强验证，保旧FETCH18/
DISTINCT/CTE11；继续真实INTEGER/VARBIT输入owner与BETWEEN需求、MINMAX
custom结果和所有原未闭环项，逐根因独立commit。完整出处见BETWEEN组合文档。

## 2026-10-07 历史完整273目标计划（7f77）

当前source `7f778476`：707auto+frontend/375registered/58TU，99项独立
source/test修复commit；原273全部范围及每个未闭环要求不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 逐项版本管理 | 每真实根因独立commit，用户push | BIT九独立issue已精确合并并FF主分支，当前90全12native/15whole红基线保留 |
| 当前完整组合 | 真ABI/source/flags/receipt/defaultdisk/deadline | normal25764真7fresh+51当前566证明donors，49native67270/57whole41612/post21whole51595全0，非fresh58/SAN |
| 主目录可用binary | 正常build/repeat/逐对象proof/实际完整复测 | publication81540真58证明迁移/重链0，实际./dbms_main同e6c7SHA；直接21whole81093全0，旧source90 binary可恢复 |
| 全部强旧红保留 | 不抽SQL/不隐藏注册红 | OPEN3whole53438真1：COUNT1/UNKNOWN1/expanded9旧强红；VARBIT/compact/BETWEEN demand不冒TYPE11关闭 |
| 接续BETWEEN输入/参数 | 实际current99组合和完整强控制 | f5所有源/docs/fixtures亲审，新私树独立issue commit，normal/baseline/reference运行，未主分支批准；随后全50native/61whole和原七完整矩阵 |
| 接续公共头/结果消费者 | enum rank/FILTER/SUM/MINMAX、signed/otherFETCH | 精确当期合成、新头真fresh58，所有旧descriptor/COUNT/UNKNOWN/CASE/reader/index/effects消费者逐根因继续 |
| 原full和全部family | 全discovery/registry真实终态和每原273证据 | 原80full42496真1/all1043非当前99结论，新99full未启动；所有原catalog/query/storage/recovery/ops未闭环范围保留 |
| 最终总账与约束 | 每原checkbox/证据/commit全scope吻合 | 273=22complete166partial70unverified15deferred，完成gate拒绝，无assistantpush/Actions启用/安全TDE恢复 |

下一动作：完成新current99 BETWEEN输入/实际参数全组合和原强矩阵；当前
enum rank/SUM和FETCH新公共头合成时真正全58正常重编；继续确切最新原full、
所有原未闭环要求和逐根因独立commit。详情见BIT current-root composition。

## 2026-10-07 历史完整273目标计划（5661）

当前source `56610199`：695auto+frontend/361registered/58TU，90项独立
source/test修复commit；原273全部范围及每个未闭环要求不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 每根因版本管理 | 实际复现/独立commit/用户push | quoted/compact/projection/reverse/input五issue各commit并FF主分支，当前85完整基线均红保留 |
| 当前完整组合 | 真ABI/source/flags/receipt/defaultdisk/deadline | normal69518真4fresh+54当前7df证明donors，30native36806/40whole33521/post7whole70931全0，非fresh58/SAN |
| 主目录可用binary | 真正常build/repeat/全部对象proof/actual executable | publication36125全58证明迁移/重链0，实际./dbms_main同8e601SHA；直接其7whole78242全0，旧binary可恢复备份 |
| 保留全部旧失败 | 不抽强SQL/不把注册红隐藏在绿组 | OPEN4whole41652真1：COUNT1/UNKNOWN1/expanded9，BIT26首缺能力红不称后续执行 |
| 接续BIT/实际参数/需求 | 全文审/精确当前合成/完整数据与effects | BIT99全部source/docs/fixtures亲审，当期baseline保留/strict14、normal25764、49native67270/57whole41612全0，OPEN3 53438真1/same CTE11；提交后审批继续，进一步BETWEEN trueparameter/demand分别处理 |
| 接续公共头/全部消费者 | enum rank/SUM/MINMAX、signed/other FETCH实际owner | private alias/signed新头需要当期真正fresh58；所有旧COUNT/UNKNOWN/CASE/reader/descriptor/index/需求消费者逐根因继续 |
| 原full与全部families | 原发现/注册真实全终态和每原273证据 | 原80full42496真1/all1043非当前90结论，新90 full未启动；所有原未闭环catalog/query/storage/recovery/ops保范围 |
| 最终总账与约束 | 每原checkbox/证据/commit全scope吻合 | 273=22complete166partial70unverified15deferred，完成gate拒绝，无push/Actions启用/安全TDE恢复 |

下一动作：完成BIT当前49/57/OPEN整gate和提交后验收后才Root合入；全文审
独立BETWEEN输入/实际参数、enum rank/SUM和FETCH修复，公共头变化当期全58
真实正常重编；继续确切最新原full、全部原未闭环与逐根因独立commit。

## 2026-10-07 历史完整273目标计划（7df9）

当前source `7df9f84a`：693auto+frontend/357registered/58TU，85项独立
source/test修复commit；原273总范围和所有未闭环要求不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 每根因版本管理 | 真复现/强SQL/独立commit，用户push | 新7df9 inheritedCTE owner已Root正式独立commit，current84原六invocations红保留 |
| 当前ABI/完整组合 | 真public头/source/flags/receipt/stamp/defaultdisk | soleMain normal85939+57当期fd46逐对象proof，25native97349/31whole61761/post原3whole67205全部实际0，非新fresh58/SAN |
| 全CTE强原验收 | 实际递归/类型/NULL/effects/namespace，不抽原cases | 原21/factor/真nextval3/SPI/全部原CTE neighbours全0；derived全72及expanded完整执行仍各1/9旧强红，clauseUNKNOWN仍1，OPEN单gate84431实际1不混绿色 |
| 接续根因 | 所有旧COUNT/UNKNOWN/nestedCASE/SQLreader/routine消费者 | 这些实际旧红不删不降为known-pass；FETCH/signed、enum arg/FILTER rank/MINMAX各独立真实owner继续 |
| 下一source合成 | integer/quoted各问题commit与BIT有限全type/owner/pair/demand | integer673a已全文源/tests/docs复查，待当期Root合成；BIT1c/5bb/e53源/docs已审大部，全部fixture全审/当期proof继续，UNKNOWNBETWEEN原1296/12真实effects矩阵不缩 |
| 当前原full/所有family | 原discovery/registry真实全终态和完整273证据 | 原80full42496真实1/all1043精确multiset，14receipt非14bugs/85 verdict，新85full未启动；matview及所有其它原query/catalog/storage/recovery/ops仍OPEN |
| 最终验收与约束 | 每原checkbox/状态/证据/commit符合全scope | 273=22complete166partial70unverified15deferred，完成gate拒绝，无push/Actions启用/安全TDE恢复 |

下一动作：当期Root精准集成整数/quoted五有限issue、逐commit验证；BIT full
fixtures再全读并组合已修compact绕过项；接收真实fresh58 enum/signed descriptor
变化时独立当期ABI全重编；随后继续确切当前原full及所有原未闭环要求。

## 2026-10-07 历史完整273目标计划（fd46）

当前source `fd46f1a7`：693auto+frontend/355registered/58TU，84项独立
source/test修复commit；原273总范围不缩，全部未闭环问题继续。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 每根因版本管理 | 真复现、保强SQL、独立commit、用户push | 新fd46 generic dispatch已Root独立commit；原61强红、strict0；两窗口前序独立commits保留 |
| 当前ABI/全对象 | 真源/头/flags/manifest/receipt/stamp/freeze | normal69161 soleMain fresh+57当期4796逐对象证明正常donors，全58当前签名/输入hash/repeat0，非新fresh58/SAN |
| 完整组合验证 | 每整个driver、默认磁盘/期限、pure/NULL/once/type | 52完整native90205/18完整whole39351/post native14714/全18whole63651全0，旧41native真默认磁盘，窗口144/72及wire195/147均保全 |
| 原全量scope | 每原discovery/registry、完整真实终态、保全部旧红 | 原Source80 full42496终1/all1043精确multiset，14失败逐项分类，不14bug/84 verdict；新current84full未启动 |
| 接续CTE/BIT/integer | 真实当前Root全文审/合成，不借private绿审批 | c6bd guard scopedREADY含旧9扩展/COUNT红；integer673a独立链待审；BIT compact member84/284继续；original FETCH scalar-child已另独立owner |
| 全部其它原family | 每原frame/query/catalog/type/storage/recovery/ops要求 | enum aggregate rank/matview58030/所有原未闭环不跳，QRY07/TYPE08等仍partial，不以单dispatch修复冒关闭 |
| 总账交付 | 原273证据/checkbox/commit/未完成gate一致 | 22complete166partial70unverified15deferred，完成gate拒绝；无push/Actions启用/安全TDE恢复 |

下一步先审实证inherited CTE guard所有实际source roles并在当前ABI隔离合成；
integer/quoted真实owner逐commit合入，BIT有限bundle须完整审/compact准入全证，
FETCH与enum rank各独立补齐；再开展确切当前原full，保其它原未闭环要求。

## 2026-10-07 历史完整273目标计划（4796）

当前source `479675f1`：692auto+frontend/354registered/58TU，83项独立
source/test修复commit；原273目标及所有未闭环范围不缩小。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 独立窗口修复 | 原强矩阵、完整consumer、每根因commit | 10989d96 NULL195和479675f1 GROUPS147由真实62/40红变全0，strict198/150全0，原生144/72和完整相邻/提交后gate全0 |
| 当前生产输入 | 实际公共头ABI、全58 receipts/flags/stamp/inputs | NULL normal25277真fresh58；GROUPS normal24221 soleExec+57当前109逐对象证明donors，不冒第二次fresh58/SAN |
| 全量原验收 | 所有原注册/自动native、真实终态、保全部原红 | Source80 full42496实际终1/all1043 receipts精确原multiset；687auto+frontend1+341registered PASS，3auto+11registered FAIL，不算14bugs/83 verdict；逐项分类见integration |
| 递归CTE introduced回归 | 实际可继承frame/session/db/source-role owner | 原第17条42P01，旧7504/82bf同完整driver PASS；private原21/强once及邻居绿，原full另derived/materialized红，要求这两完整consumer补证再import；已有9扩展旧红不削弱 |
| generic/BIT/integer | 完整参数类型、metadata、NULL、once、physical/index owner | generic current82全矩阵61真红、matched严格18同矩阵0；finite BIT及codec私有READY；Root新compact native84控制19162实际1/68红，两个真实owner全member准入继续；UNKNOWN/quoted/index其它边界继续 |
| 其它全部原family | 每原目录/query/storage/recovery/IO/DDL/operations要求 | matview后台58030仍OPEN未修；不以两窗口根因修复冒frame/spill/collation或任何更大family关闭 |
| 总账与版本管理 | 每原条目证据/状态/checkbox/commit一致，用户push | 原273=22complete166partial70unverified15deferred，完成gate拒绝；本地独立commit，无push/Actions启用/安全TDE恢复 |

完整证明见integration与两窗口issue。下一步完成generic当前Root隔离导入/正式
正常构建及全强矩阵合成，接收已实证CTE owner修复并逐问题验证提交，复查有限
BIT bundle及其它原问题；不把 private READY 当 master 已完成。

## 2026-10-07 历史完整273目标计划（7b22）

当前test/source `7b2223cf`：690auto+frontend/352registered/58TU，81项
独立source/test commit；生产逐字同3ffbb516。原273目标/所有未闭环范围不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 独立提交 | 真复现、保原SQL/场景/强断言、每问题commit | 原TRUNCATE完整134；7b2223cf仅fixture身份，原三节+第四拒绝控制/target及11邻接/postcommit全0 |
| 全量原验收 | 所有当前注册/自动native、真实终态、不隐藏旧红 | 冻结Source80 full42496真正LIVE，仍含旧TRUNCATE；非81 testepoch，81 full未启动 |
| 当前matview生产故障 | 真后台consumer、目录publication、dirty/retry边界 | 原首轮0而四轮重复78209为1/两134；真实后台domain目录栈97700为1，独立producer复现/修复继续 |
| BIT / generic / enum aggregate | 完整实际type/owner/NULL/IN/OR/FILTER/rank/once消费者 | BIT新mixed未知左operand误cast已查出，仍held；aggregate真实OR/FALSE及enum绑定独立处理，不import未证source |
| 恢复/CREATE/全部原family | 每原owner/retry/IO/DDL/query/storage/operations要求 | 原scope继续，不以TRUNCATE测试契约修正冒恢复窗口或family关闭 |
| 总账 | 原273逐条证据/状态/checkbox/commit一致 | 22complete166partial70unverified15deferred；完成gate拒绝，无push/Actions/安全TDE恢复 |

完整输入/实际终值/源码映射见integration和独立TRUNCATE issue；当前生产all58
与exact3ffb一致，所有driver/stubs fresh+57正常对象匹配证明，不冒fresh58/SAN。

## 2026-10-07 历史完整273目标计划（3ffb）

当前source `3ffbb516`：690auto+1frontend/352registered/58TU，80项独立
source/test commit。四新Root issue对应原scope保持；最新原full已真实启动。

| 阶段 | 原要求 / 下一动作 | 当前真实证据 |
| --- | --- | --- |
| 逐根因commit | 复现/不弱化SQL/完整consumer/独立提交 | 7ecf、8467、0ffd、3ffb分别commit；原DDL/rank/TYPE/range错误和作者错误均保留 |
| 当前生产输入 | 每公共头ABI正确，全58正常/receipt/flags/freeze | 0ffd真fresh58 normal83683为0；3ffb新soleHelper+57当期proved donors71958为0，非新fresh58/SAN |
| 原完整验收 | 当前全部690+frontend/352原runner/default期限完整终态 | 原full42496 LIVE，独立树原58/57生产层proof迁移；focused15/7与strict0不是全绿 |
| TYPE08/TYPE11所有消费者 | aggregate/rank/OID/DDL事务；全literal/IN/NULL/CAST/descriptor/binary | Rootenum33/6完整0；真实generic/enumaggregate与匹配locale继续，BIT held缺口未虚报闭合 |
| 恢复/CREATE/所有family | 每原owner/retry/IO/catalog/query/storage/ops要求 | temp/CLOG普通owner重试、CREATE与其它原未闭环继续，未证private source不导入 |
| 总账验收 | 每原条目完整证据/状态/checkbox/commit一致 | 22complete166partial70unverified15deferred；完成gate拒绝，无push/Actions/deferred安全恢复 |

旧7504full6359已实际1/all1032标签、旧82bf48181实际1/all1031；不把22失败
当22bug或当前结论。所有helpers/source冻结后启动，无观察到期kill/restart。
完整出处/输入/失败/源码映射见integration。

## 2026-10-07 历史完整273目标计划（7ecf）

当前test/source `7ecfd8eb`：687auto+1frontend/350registered/58TU，77项独立
source/test commit。DDL fixture错误游标单项修复，不改任何生产源/头/格式。

| 阶段 | 原要求 / 下一动作 | 当前真实证据 |
| --- | --- | --- |
| 每问题独立提交 | 不弱化原SQL/19节/断言 | 7ecfd8eb；旧原native134，正确扫描+实际CREATE及同XID COMMIT，完整target30243/邻接十项17482为0 |
| 当前匹配生产 | 全58源头flags/receipts/stamp/freeze | 生产逐字同84e；fresh drivers/stubs+57 proved matching正常objects，非新fresh58/SAN |
| 原全量验收 | 当前687+frontend/350全部完整终态 | 当前full未启动；旧82bf48181实际1/1031标签，75046359仍逐个LIVE；不以旧红批准当前 |
| BIT / ENUM | 完整真实类型、literal/IN/NULL/ORDER、rank/quoted生命周期/OID/ALTER | 2828缺依赖，真实当前33wire32个42883；held literal/Main与ENUM组合继续，不弱化quoted矩阵 |
| 恢复/CREATE/所有family | 原完整owner/retry/IO和全部273要求 | temp/CLOG普通owner、CREATE与其它所有原未闭环继续；不以测试false-negative修正冒恢复闭合 |
| 总账 | 每原条目完整证据/checkbox/commit一致 | 22complete166partial70unverified15deferred；require-complete拒绝，无push/Actions/deferred安全恢复 |

## 2026-10-07 历史完整273目标计划（84e1）

source `84e16a74`：687auto+1frontend/350registered/58TU，76项独立source/
test commit。空enum schema与empty hash/NULL各一个Root issue commit；原273未缩。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真反例/完整控制/每问题commit | 89ea→75b；472+105→84e，同issue先补NULL扫描边界再合入，不改master历史 |
| 当前生产输入 | 全58源/头/flags/receipts/repeat/freeze | normal37842=0，fresh soleTM+57当前21b完整证明normal donors；不冒fresh58/SAN |
| 原完整回归 | 当前687+frontend/350、磁盘/default期限、完整终态 | 当前full未启动；final20native82125/4whole4946/两strict18为0，prefix首wire1及repeat0完整保留 |
| TYPE08剩余 | 所有rank/投影/排序/quoted TYPE/OID/ALTER事务要求 | scalar native绿不代实际wire，quoted控制原样留，两个根因独立再组合全部矩阵 |
| TYPE11剩余 | 全typed operands/unknown/IN/CAST/descriptor/binary要求 | literal候选新TEXT/INT假命中暂缓导入，补强前不借原11/4窄绿批准；constructor已Root单项修复 |
| 恢复/CREATE | 原stale-temp+真实WAL/REINDEX/普通owner/CLOG、所有IO层 | 真exec强反例及普通后台栈已记录；未证WIP不导入，不吞错误/弱化old guard |
| 其它family/总账 | 每原条目完整证据/状态/checkbox/commit/验收一致 | 全部目录/查询/存储/恢复/运维继续，22complete166partial70unverified15deferred，require-complete拒绝 |

旧8694full64688已终1/all1014标签完整，不以失败数量当bug数；82bf48181、
75046359原full逐个live。helpers冻结、原deadline不变，无push/Actions启用/
用户deferred安全TDE恢复。完整source映射与范围见integration。

## 2026-10-07 历史完整273目标计划（21b1）

source `21b105dd`：685auto+1frontend/348registered/58TU，74项独立source/
test commit。BIT ARRAY constructor 隐式无typmod保全独立提交，原273未缩。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真反例/完整控制/每问题commit | private9cdb→Root21b1；完整值/空/NULL/nested/OID及原显式BIT默认保持 |
| 当前生产输入 | 全58源/头/flags/receipts/repeat/freeze | normal71286=0，fresh soleExprEvaluator+57proved d661 normal donors；不冒fresh58/SAN |
| 原完整回归 | 当前685+frontend/348、磁盘/default期限、完整终态 | 当前full未启动，Root11native82410/3whole57467/strict180006为0；旧full不能当当前绿 |
| CREATE三层 | 精确native类别、已有flush结果、真实DDL cause/SQLSTATE | 四点native错分类/七点false success/普通wire XX000均实际确认，各独立commit，最终组合未READY |
| BIT与ENUM | 原全部值/存储/NULL/operator/OID/extended/DDL事务消费者 | 比较/存储literal/descriptor和空enum schema/hash/投影等继续；不以第一项绿代family |
| 目录/查询/恢复/所有family | 所有原namespace/MVCC/WAL/map/EXPLAIN/存储/运维要求 | 原未闭环要求继续，未证私树不导入；所有旧红保留当前源核实 |
| 总账闭合 | 每原条目完整证据/状态/checkbox/commit/验收一致 | 22complete166partial70unverified15deferred，require-complete继续拒绝 |

旧full48518/93414/27869实际终1、完整标签保留；旧8694full64688、82bf48181、
75046359仍逐个live，不因观察时间kill/restart。新helpers冻结、不改单次deadline，
无push、Actions禁用，用户跳过安全/TDE deferred。完整映射见integration。

## 2026-10-07 历史完整273目标计划（9f7d）

source `9f7d55cc`：684auto+1frontend/347registered/58TU，73项独立source/
test commit。新增真PITR选中stream修复及两原测试契约分别commit，未缩原273。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真反例/完整控制/每问题commit | 三新Root映射见integration；weak fault候选先拒绝、最终线程归属实证后才单项合入 |
| 当前生产输入 | 全58 actual源/头/flags/receipts/repeat/freeze | d66110753正常0、fresh soleTM+57proved donors；9f7d生产逐字同d661，7504零CPP/all58迁移38219为0，不冒fresh58 |
| 原完整回归 | 当前全部684+frontend/347、磁盘/default期限、完整终态 | exact7504 full6359与82bf full48181 live，不冒9f7d full；105+front0，旧49whole41pass8fail如实保留 |
| 新强专项 | PITR三历史/原备份、DOMAIN全原driver、精确目录故障线程 | Root15+frontend/原冷backup0；DOMAIN11+强化target/strict18为0；tablelist10native6whole3重复0 |
| CREATE错误分类 | 真native IO与semantic区分、普通DDL原因/SQLSTATE/无effects/retry | 两层实际混淆已确认，分别真实注入/独立源码提交，不全换IO、不删原控制 |
| 目录/查询/恢复/所有family | 全部原namespace/MVCC/WAL/map/EXPLAIN/类型/运维要求 | 现有多段WAL、stale-temp、trigger、shared-engine目录与其它未闭环项不替代/不缩小 |
| 总账闭合 | 每原条目完整证据/状态/checkbox/commit/验收一致 | 22complete166partial70unverified15deferred，require-complete仍拒绝 |

旧full33648/56028/97347已实际1，另外四旧full仍live；每旧红在当前源核实，
不把失败数当bug数。新helpers冻结不改，过去作者EOF/offset/cache-preflight
失败全保，未计PASS。无push、Actions禁用、deferred安全/TDE不恢复。

## 2026-10-07 历史完整273目标计划（82bf）

当前source `82bf3739`：683auto-native+1实际frontend/347registered/58TU，
70项独立source/test commit。数组native物理类型、snapshot exporter传输可见性、
原routine canonical oracle各独立提交；原273目标和所有未闭环要求没有缩小。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真反例、完整原控制、每问题commit | 新三项映射/原失败见integration；原snapshot/array不改，routine原SQL/calls全部保留 |
| 最新组合构建 | 全部生产输入一致、normalO2/repeat/58receipts/freeze | exact82bf97039为0；fresh soleTM+57proved90fe donors，不冒fresh58 |
| 原完整回归 | 原683auto+1frontend/347registered、磁盘/default期限、完整终态 | full48181已运行live，105+1native39059/49whole86895 live；三原完整native45204及真实故障68439为0 |
| 原view触发器错误后复用 | 保原23514/行/NULL/OID与下一SQL原15秒期限 | 实际trace定位未恢复outer session的析构指针；异常安全清理/真正完整wire继续 |
| namespace与目录 | 完整物理/dependency/peer/parent/savepoint/old refs | 私有原组与更强domain外列/descendant/NULL/quoted/rollback矩阵继续，未READY、不以单consumer代CAT01/09 |
| 备份/恢复 | timeline fork后的真实backup+archive/PITR/cold restart | 真timeline2新行漏恢复134已复现；按实际stream owner独立修复，不冒WAL family完成 |
| 完整DML/OPT16及所有family | WITH同图/phase/atomic、所有格式/options/真实指标、目录WALMVCC/存储/类型/查询/运维 | 全部原未闭环条目继续，不以当前专项替代原要求 |
| 总账闭合 | 每条原状态/checkbox/证据/commit/全验收一致 | 22complete/166partial/70unverified/15deferred；require-complete继续拒绝 |

上一1d25 focused入口全部完成但wrapper实际127（运行中改外部helper产生Bash
读offset错误），保留99native98pass/1arrayseed和frontend0、48whole39pass/
9timeouts，不算整组绿。新helpers冻结启动后不改。旧36568/79042 full已1，
另七full实际live；每个旧红必须在最新源确认，不能算独立bug数量或当前批准。
不push、Actions保持禁用、安全/TDE deferred；只有总账真实闭合才报告完成。

## 2026-10-07 历史完整273目标计划（1d25）

当前source `1d25a253`：680 auto-native+1实际frontend native/347registered/
58TU，67项独立source/test commit。六source-DML、两INSERT default/source、
primary-cleanup、compound source与EXPLAIN metadata各独立提交；完整273目标
未缩小。SourceDML中间oracle单独绿不代替USING消费者；原UPDATE DEFAULT不回退。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真复现、保留完整原SQL/控制、逐问题commit | 十一新ROOT映射/原失败见integration；private最终整组而非选取子集 |
| 最新组合构建 | 新AST/全部headers一致、正常O2/repeat/receipts/freeze | 6aec真正fresh58已0；1d25四CPP fresh+54逐字proved donor门73339已0，不冒fresh58 |
| 原完整回归 | 原680 auto-native+1 frontend/347registered、默认磁盘期限、完整终态 | 最新99+1native91615/48whole12909 live，真实GNU fault38306为0，full仅准备；旧5ca618/319已1且937标签完整；其余九full含8694仍live |
| 剩余DML/EXPLAIN | WITH-final writer/CTE同图/atomic/phase、所有格式/options/真实指标 | ordinary/source/default/descriptor专项不代替全部OPT-16和DML；严格34×4原envelope继续 |
| 已复现native边界 | exact array seed/元素类型、namespace完整依赖/物理/目录/peer owner | 数组原assert不改；schema V1新异常修复和V2 committed-clean snapshot/旧refs/parent/rollback验证继续 |
| 当前原full失败核实 | 每项在当前matching58/O2完整旧fixture重现，不以旧红代新红 | actual原三native50319=2fail/1pass：routine int metadata134、snapshot导入后COMMIT可见性134，vacuum_full所有控制0；导出snapshot外部writer身份继续修复 |
| 目录/存储/全部family | catalog WAL-MVCC、完整FSM/VM重建/恢复、所有原273要求 | 每根因修复不代替CAT-01/CAT-09/STO-05架构验收；其它types/query/recovery/operations逐项继续 |
| 总账闭合 | 每条原checkbox/状态/证据/commit/验收范围一致 | 22complete/166partial/70unverified/15deferred；require-complete仍必须拒绝 |

8694 native89=0、whole43=32pass/11fail，GNU map fault两项0；原DELETE六断言、
九入口timeout和初始connect103保持失败。e083正式fresh58/保DEFAULT的merge10
实际0；6aec正式fresh58实际0；不借这些旧组合批准1d25。私有新phase完整14whole/
13native/六CPP scopedSAN/strict180006均0，/dev/shm与O0范围明确。强cleanup
正常/scoped-main同TU driver0，而原数组seed邻接仍134，继续真实修复。
旧5ca full16482已1：610native/243registered pass，8native/76registered fail；
不推断84bugs，原完整日志保留。另八旧full及8694原full逐个handle still live；
不因观察时间结束而kill或restart。不push、Actions保持禁用、安全/TDE deferred。

## 2026-10-07 历史完整273目标计划（8694）

当前生产/测试source `8694f31e`：672native/342registered/58TU，56项独立
source/test commit。map clean-peer、ordinary DML EXPLAIN、public bootstrap、
旧scalar native契约oracle分别提交；这不是四个完整family关闭。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真复现、完整原SQL/控制、每问题独立commit | 四新Root映射见integration；private红/所有timeouts保留 |
| 组合构建 | 最新公共头一致，全58 fresh正常O2/repeat/receipts/freeze | 精确8694 build44782 live，无donors；89native/43whole/fault/full仅准备 |
| 原全量回归 | 原672native/342registered、默认磁盘期限、完整终态 | 最新组合未启动；九更早full逐个核实live，两较新组实际前进，不冒全绿/不restart |
| 剩余DML计划/执行 | 原DELETE/nativeUSING、UPDATE FROM、INSERT default/CASE真实source、WITH-DML/descriptor/phase | actual新强PG18基线/候选继续，不能用oracle改动隐藏真实consumer失败 |
| 目录/存储/所有family | 真namespace完整owner/WAL-MVCC、FSM/VM恢复和所有原273要求 | public/bootstrap及map三个根因修复不替代CAT-01/CAT-09/STO-05架构完成 |
| 总账闭合 | 每个原条目完整证据、状态/checkbox/commit/验收一致 | 22complete/166partial/70unverified/15deferred，require-complete必须继续拒绝 |

旧8f58正常已0、native84=83pass/1旧oracle、whole42=32pass/10fail；
旧a1b58/fault已0、旧d4cad native82=81pass/1fail及whole42=30pass/12fail。
新bootstrap22native/strict180006为0，whole11=8pass/3原timeout。
专项/重复绿不消除失败或转成完整suite/TLS/全部family批准。
不push、不启用Actions、不恢复用户跳过安全/TDE；完成目标没有缩小。

范围：开始时未提交的 `ExecutionPlan.cpp`、`main.cpp`、`grouping_sets_expr.sql`，以及 `code-review-progress.md` 中尚未验证的投影子查询解析。历史 PostgreSQL 全功能路线图不属于本次收尾；此前用户要求跳过的安全专项仍排除。不执行 git push。

| 阶段 | 工作 | 验收标准 | 状态 |
| --- | --- | --- | --- |
| 1 | 完善串行 / 并行 string_agg、array_agg | NULL、空字符串、参数表达式、FILTER、输入排序及串并行一致性有回归；单独提交 | 已完成；第 191、196 项；执行器专项及完整 SQL 入口通过 |
| 2 | 收尾分组表达式与临时调试代码 | GROUPING SETS / ROLLUP / CUBE SQL 有明确结果断言；移除临时调试输出；按问题提交 | 已完成；复查第 192 项；SQL E2E 通过 |
| 3 | 完成投影子查询解析检查 | 引号关键字、空白符、ORDER BY / LIMIT 等组合有回归；修复逐项提交 | 已完成；复查第 193–195 项 |
| 4 | 集成验证、更新清单 | 主程序构建和相关 C++ / SQL 回归通过；工作区干净；未 push | 已完成；31 个 C++ 测试和 3 套 SQL E2E 全部通过；改动全部本地提交 |

执行方式：先复现，再修复和回归，按问题单独提交。本文件记录本次明确范围的完成情况，不宣称整个数据库不存在其他缺陷。

## 完成记录（2026-09-08）

本次新增修复为第 191–196 项，分别提交为 `9d41c9b`、`2c3042d`、`06a6338`、`103c8a7`、`11c76de`、`fc6c1e0`；具体问题、影响和验证见 [复查清单](code-review-progress.md)。

收尾验证覆盖清单对应的 20 个 C++ 测试，以及以下 11 个相邻回归：`text_comparison_type_test`、`expression_evaluator_test`、`boolean_expression_test`、`greatest_least_projection_test`、`group_collection_aggregate_test`、`parser_phase1_test`、`parallel_exec_test`、`volcano_select_phase51_test`、`window_functions_test`、`aggregate_bool_test`、`aggregate_percentile_test`。

SQL 验证使用本次构建的 `build/dbms_review_main`，运行 `review_sql_e2e_test.py`（17 个查询）、`window_e2e_test.py`（13 项）和 `explain_analyze_e2e_test.py`（6 项），均在隔离目录中通过。当前环境使用 zlib 和 TLS stub；未验证 OpenSSL 分支，也未重开已排除的安全专项。Shell / Python 语法检查、`git diff --check` 和调试输出清理检查通过。

全部阶段关闭，未遗留本次范围内待办。仓库 GitHub Actions 工作流保持禁用，不执行 push；历史全功能路线图和这里明确排除的功能不计入完成范围。
## 2026-10-07 历史5ca/c061执行计划

最新执行版本 `c061a38a`（622native/320registered/58TU）：UNKNOWN输入、typed Append、普通UNION ALL入口及跨进程XID分别本地commit；private强矩阵scope见integration文档。新公共ABI的全58 fresh正常O2/repeat/audit/freeze90776实际0，冻结SHA见integration文档；原full622/32036568、matching12native35605及whole9wire48073已实际运行，未称full通过。下面表格记录前一精确5ca验收，不替代c061。5ca156native6042已实际1=154pass/2fail；5ca原full另外复现SETTABLESPACE后backup与BEGIN/drop活跃owner COMMIT两项原断言失败，均独立修复、不放宽。后续顺序：完成不改原断言的整合回归；验证并逐项合入domain/WAL代际、INSERT异常事务owner、限定函数/SRF候选；修复完整模式/domain优先级、集合P/D和剩余273项。原全文37/21与domain链setup强矩阵均保留；模式新增数组envelope拒绝在strict180006通过，当前基础候选全58 privateO047334运行，尚未ROOT合入。没有push、Actions启用或用户跳过专项恢复。

Source `5ca4278e` 已逐项本地提交，618 native / 319 registered / 58 TU；完整总账仍未完成，不沿用以上旧“本次范围关闭”作为273项完成证明。提交映射/实际失败/证据见 `docs/issue-ready-source-integration.md`。

| 下一阶段 | 验收要求 | 当前证据与状态 |
| --- | --- | --- |
| 最新组合正式构建 | 全58 fresh 正常O2、repeat、全部 source/header/flags/stamp、冻结同一binary | 精确5ca快照36316实际exit0，冻结SHA见integration文档 |
| 原完整回归 | 不改原SQL/assertions/deadlines，执行618native/319registered | 5ca full16482与156native6042已运行；75wire93351=70pass/5fail；旧d2 full590/312 96468仍live |
| 存储剩余问题 | 真实rename/drop/recreate代际恢复、正确RR setup的vacuum_toast原值断言通过 | 4c05967b独立修正错误首次读setup且保留所有旧断言/新增lazy控制，PG18双连接与5native通过；恢复生产问题仍独立修复 |
| 查询/类型剩余问题 | 保留whole37 DML/UNION ALL、whole domain/pattern priority、SRF完整known-gap控制 | 新ANY whole19和数组whole12专项绿；typed append、TYPE-19域链、模式、SRF两项仍进行 |
| 总账验收 | 全部原273逐项满足完整证据；用户跳过15保持deferred | 22complete/166partial/70unverified/15deferred；完成gate仍拒绝 |

不push，不启用Actions，不恢复用户跳过安全/TDE。旧e6全58正常构建实际0，但native137=135pass/2fail、wire65=64pass/1fail；旧d2 native120实际0、wire64=60pass/4fail。专项结果不替代新ROOT完整验收。

## 2026-10-07 历史2894完整目标执行计划（最新证据见文末）

生产/测试source `28940634`，642native/330registered/58TU。最新九项修复
已逐项commit，c061 checkpoint后累计19项；详见integration映射与原始证据。
完整273目标未缩小，旧2026-09“本次范围完成”不等于总账完成。

| 阶段 | 必须通过的验收 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真复现、保持原SQL/断言、每问题独立commit | set三项/array三项/退休/域FK/cache原子guard均已分别ROOT commit |
| 组合正式构建 | 公共头全58 fresh正常O2，repeat及source/header/flags/object/stamp，冻结同binary | 25ee/42bd/0fdb/ed82全部实际0；最新2894公共头fresh58 60362 live，35native/15wire仅准备 |
| 完整原回归 | 原全部native/registered，默认期限和完整矩阵，不删失败 | d2/5ca/c061/42bd原full96468/16482/36568/33648及新0fdb56028/ed8248518 live；无全量PASS |
| 组合专项 | 新头匹配native、whole protocol、严格180006原强fixtures | 私有set43/18/12wire、array54/8native/4wire、retire19/3SAN、pattern11/48均有实际0；新ROOT尚待正式结果 |
| 已证实剩余缺陷 | Unicode模式、explicit search_path/SET LOCAL、真实磁盘timeout原因 | Unicode/runtime与错误旧fixture独立核实，search-path dependencies强矩阵在修；cache V3原强whole18/scopedSAN4已0并独立合入 |
| 剩余family | 逐项核实和实现全部尚未达到要求的273项 | domain ALTER/casts/revalidation、其它sets、完整类型/存储/查询/运维等仍OPEN，继续逐项修复 |
| 总账闭合 | 每条checkbox/状态/证据/提交/验收范围一致，完成审计实证 | 22complete/166partial/70unverified/15deferred；require-complete仍应拒绝 |

不push，不启用Actions，用户跳过安全/TDE专项保持deferred且不虚报完成。

## 2026-10-07 历史完整目标执行计划（c465）

生产/测试source `c465f898`：648native/334registered/58TU。c061后27项独立
source/test commit，最新八项已分别合入。完整273目标没有缩小。

| 阶段 | 必须通过的验收 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真复现、保留原SQL/断言、每问题独立commit | Unicode四项、search_path、声明namespace、统一provider、catalog分别commit |
| 组合正式构建 | 公共头全58 fresh正常O2、repeat/source/header/flags/object/stamp、同binary冻结 | 2894正常58/35native已0；c465新58 build28366实际live；58native/25wire仅准备 |
| 原完整回归 | 原648native/334registered、不改默认期限/矩阵 | 新full仅准备；旧七个full均实际live；2894 wire15=8pass/7fail，0fdb wire13=3pass/10fail |
| 组合专项 | 最新头匹配native、完整protocol、严格180006强fixtures | 私有Unicode13/90/8native/4200、namespace14wire/10native/4SAN、catalog12native/fault/磁盘whole通过，scope见integration |
| 已证实剩余问题 | 实际消费者及物理需求修复，不靠兼容fallback掩盖 | legacy模式16个native断言真红；domain DEFAULT原whole11断言真红；无变化SAVEPOINT重建镜像诊断继续 |
| 剩余family | 每个原273条目完整实现/核实/验证 | routine creation/overloads、完整types/catalog/query/storage/运维等继续，不用专项替代 |
| 总账闭合 | 每条checkbox/状态/证据/commit/验收范围一致，完成审计实证 | 22complete/166partial/70unverified/15deferred；require-complete仍拒绝 |

不push，不启用Actions，用户跳过安全/TDE保持deferred。任何尚在运行或仅准备
的 gate 都不写为通过；历史失败不因新候选专项成功而改绿。

## 2026-10-07 历史完整目标计划（6320）

source6320：658native/338registered/58TU，c061后39项独立source/test提交，
最新12项逐项映射见integration；目标始终是原273完整要求，不借旧小范围关闭。

| 下一阶段 | 验收 | 当前证据与动作 |
| --- | --- | --- |
| 已验证独立修复 | 原失败、真实consumer、原assert/deadline、每问题commit | cold/domain五项/模式三项/map/no-effect两项/creation均已分别commit |
| 新公共头组合 | 全58 fresh正常O2/repeat/source/header/flags/object/stamp/冻结 | 精确9772 657/337 build12522 live；首个错误计数guard编译前失败保留；69native/28wire/full仅准备 |
| 原完整回归 | 不改原648/334及旧全量矩阵、实际终态 | c465正常58/native58已0；wire25=16pass/9fail；full79042与七旧full逐个核实live |
| 最新CPP验证 | 6320创建路径同头新对象、完整原whole/冷启动/撤销 | private core/冷启动/八邻接0；原DDL独立58030失败、V2wire7两实际timeout不删；ROOT新组合待验证 |
| 新已证实问题 | UPDATE DEFAULT/DELETE owner与实际RETURNING/nested aliases/域IO | 三个独立owner继续各自复现、修复、原强验证、独立commit；ROOT域IO保原bool断言 |
| 剩余family | 每个原273完整实现与证据 | generaltypes/catalog/queries/storage/recovery/operations仍继续，专项不能替代 |
| 总账验收 | checkbox/状态/证据/commit和每条需求一致 | 22complete/166partial/70unverified/15deferred；require-complete仍拒绝 |

domain旧来源不猜/不迁移、schemaB/D3旧reader拒绝；必要写入恢复Append53.705秒
原15期限失败仍保留，不因no-effect专项绿就关全部延迟。无push/Actions激活或
用户跳过安全/TDE重启；各私有O0/局部SAN/单CPP增量不冒充最新全量正式通过。

## 2026-10-07 历史完整目标计划（b69b）

sourceb69b：663native/340registered/58TU，c061后45项source/test独立commit。
最新六项映射和完整实际日志见integration；总273目标不缩小。

| 阶段 | 必须满足的验收 | 实际状态与下一动作 |
| --- | --- | --- |
| 独立问题版本管理 | 原复现/原assert/deadline/真实consumer，每问题commit | 新owner/alias/域IO/UPDATE三项分别本地commit；原失败不删除 |
| 新public callback ABI | 全58fresh正常O2/repeat/source/header/flags/object/stamp/freeze | exactb69b23520 live，无donors；76native/32wire/原663/340只准备 |
| 原组合比较 | 对应revision一致，不借旧ABI/私有O0 | 9772 fresh58已0，69native/28wire live；11d7两CPP+56证明donors匹配normal0，其helpers未启动 |
| 原全量回归 | 原所有native/registered及已知未注册红，期限不改 | c465原full和七旧full实证live；c465wire25=16pass/9fail；无全量PASS |
| 已证实继续问题 | 真实DML EXPLAIN/DELETE RETURNING/explicit array bounds/跨owner maps | 三个owner各独立tree修复，强fixture原seed不删、不把cascade数成独立bugs |
| 原剩余families | 每条完整功能/实际证据/提交/原gate都满足 | types/query/catalog/storage/recovery/operations等仍OPEN |
| 总账闭合 | 每项状态/checkbox/证据/commit范围一致且完整验收 | 22complete/166partial/70unverified/15deferred；require-complete仍必须拒绝 |

旧必要恢复timeout保持红；alias原Append两轮绿不等于所有延迟闭合。
UPDATE纯prepare EXPLAIN零effects不冒实际plan consumer。新D3/schemaB旧reader拒绝，
旧模糊default来源不猜测/不自动迁移。无push、Actions启用、安全/TDE跳过项重启。

## 2026-10-07 历史完整目标计划（d4cad）

source `d4cad5b3`：667native/341registered/58TU，累计49项source/test独立commit。
新DOMAIN facts、map peer、array oracle/runtime四项分别commit，完整proof/映射见integration。

| 阶段 | 完整验收要求 | 当前实际状态与下一动作 |
| --- | --- | --- |
| 新组合公共头 | 当前全58正常O2/repeat/source/head/flags/object/stamp/freeze | 62553实际0无donors/SHA cf766684；82native74573/42whole59900/原full93414已启动/live |
| CPP-only DOMAIN整合 | 保留ALTER source identity并匹配当前callback头/全部旧对象证据 | a535正常soleDDL+57 proven b69b对象76324实际0；16native0、11wire7pass/4fail原样保留 |
| 原组合门禁 | 各revision对应全部原SQL/assert/期限 | b69b58/76native0，32wire23pass/9fail（DELETE原红+八入口timeout）/full97347 live；9772原69native0、28wire23pass/5timeout |
| 原full终态 | 每条失败原样留存并与当前修复对应复核 | d2 full96468实际1，589native/247registered pass、1/65fail；其余七旧full逐个live，无全量PASS |
| 独立继续修复 | 真实producer/consumer与所有原seed、NULL/OID/owner/effects | map close134与cleanpeer backup134、实际DELETE RETURNING、DML EXPLAIN分别owner继续 |
| 全部原families | 每条273完整功能/强证据/提交，不以局部兼容替代 | 原namespace/catalog/types/arraygrammar/query/storage/recovery/operations等继续OPEN |
| 总账完成审计 | checkbox/状态/证据/commit范围与逐条原需求全部一致 | 22complete/166partial/70unverified/15deferred，require-complete仍拒绝 |

源码修复和错误oracle独立提交，原红/trace/不trace/deadline不删除。私有O0、
单CPP O2或局部SAN不当最新完整source gate。不push、Actions启用或重开用户deferred专项。

## 2026-10-07 历史完整目标计划（a1b7）

sourcea1b7：668native/341registered/58TU，50项source/test分别commit。

| 阶段 | 完整验收 | 当前证据与下一动作 |
| --- | --- | --- |
| 当前完整优化编译 | exacta1b7全部58/head/flags/objects/stamp/repeat/freeze一致 | 92141 fresh正常O2 live无donors；83native/42whole/原full668341/强pwrite fault准备未启动 |
| 前一组合终态 | 原SQL/assert/时间期限，失败不覆写 | d4cad58正常0，82native/42wire/full各live；a53516native0/whole11为7pass4fail；b69b76native0/whole32为23pass9fail |
| 完整namespace owner | cold/warm DROP、dependency/catalog/存储、原所有路径一致 | 新强baseline17assert真1，保旧compile/test作者错误；须真正修复loaded stale及public RESTRICT/CASCADE，不能只改declaration fallback |
| DML真实消费和资源 | 同一实际plan graph/counters/lifetime/null/owner，原完整wire | DELETE consumer/歧义oracle分别处理；prepared DML foundation因真实未open close provider134暂不合入，实际EXPLAIN仍须实现 |
| Map合作peer与全存储 | 实际合作发布/正确durability/restore/失败边界，不接受任意raw VM | close pending独立已commit并私有强proof0；cleanpeer backup134第三因继续，原raw-edit negatives不弱化 |
| 全273闭合 | 每条原scope/功能/真实proof/commit/全量gate完整 | 22complete/166partial/70unverified/15deferred，完成gate仍拒绝；所有原family继续 |

无push、Actions启用、user-deferred安全/TDE重启。每项真实源码/错误oracle分别commit；
正常省略fault宏的普通native绿不冒真正pwrite注入证明，最新whole/full无提前PASS。

## 2026-10-07 当前完整目标计划（8f69；前文均历史）

source8f69：669native/341registered/58TU，52项独立source/test commit。

| 阶段 | 完整验收要求 | 实际证据与下一动作 |
| --- | --- | --- |
| 最新公共API epoch | 全58当前DML/Operator/PCE/map/array/default头和flags/source/对象一致 | exact8f69 fresh正常O2 61223 live无donors；84native/42whole/full/真fault准备未启动 |
| 最新primitive资源边界 | 同一个真实graph/counter和provider关闭、无二次effects | 两项分别commit；原未open close134保留、强native/七相邻/七whole/scopedSAN0，不借私有绿当Root完整证明 |
| 真正SQL EXPLAIN消费 | 原33×4及额外default/descriptor/Parse+Describe/readonly阶段强控 | 前端仍SELECT-only，consumer独立继续；plain metadata不当ANALYZE真实执行 |
| 其它真实DML消费 | DELETE/UPDATE FROM完整AST/source/namespace/NULL/owner和原矩阵 | 原歧义SQL保42702/no-effects负控、限定target正控；oracle/runtime/UPDATE carrier分别提交，LEFT/CURRENT未实现仍OPEN |
| namespace与map owner | 全真实DROP/依赖/catalog/physical及合作durability/restore/fault | nativeDROP原强17assert基线红继续；cleanpeer backup第三因原134、raw negatives不弱化 |
| 所有原scope与总账 | 每条原273完整功能/proof/commit/gate齐全 | 22complete/166partial/70unverified/15deferred不变，完成gate仍必须拒绝 |

每个源码/错误oracle独立commit；不push，不启用Actions，不重开用户跳过安全/TDE。
