# DBMS 功能测试报告

最新有限修复：[R184测试数据目录隔离](issue-review-r184-test-data-directory-isolation.md)使公共helper在子程序启动前选私有数据根，
父环境/cwd不变。旧同一7控制完整4失败，候选及Root完整7通过；两侧各4native/3真实Main通过，调用者标记目录保持不变。
只改脚本及既有测试，全部CPP/127头和匹配程序不变，不冒fresh编译或修复后新完整72；旧72另记。
跟踪605MD/1492入口，Root608MD/1504入口；原12未接受入口与三份文档保留，原273状态22/166/70/15不变。
总目标未完成，历史R164超时原因未定、明确暂缓不恢复；不push，不启用Actions。以下记录按当时输入解释。


当前完整矩阵复验：[原72入口](issue-review-current-full72-revalidation.md)在accepted-only clean与工作区各72实际全0，
原35native/34协议/3真实Main、956及190额外raw门均保留，默认15秒/持久化未弱化。
每侧97对象39程序核对，零C++编译、38新链接，donor不冒fresh；2206accepted源码与tested clean一致，Root2325保留。
这不是全1492或原273验收：仅15条总账含本矩阵直接引用测试，258条不能据此关闭；require-complete两侧仍实际1。
R164旧超时日志和未定根因保留，新通过不倒推原因已修。原22/166/70/15状态及明确暂缓不变。
不push，Actions禁用。以下记录按当时输入和范围解释。


最新总清单复查：accepted-only 17f06229的R164原完整956项实际0，保持默认15秒/原断言，
仅监视本次自有进程；不是当前完整72通过，历史两次超时原因仍未确定，未冒称R164全部关闭。
原273项全文、状态与证据引用已建立逐项映射；22 complete/166 partial/70 unverified/15 deferred，
22个勾选与JSON一致、引用均已跟踪，但文件存在或既有complete标签本身不是当前充分证明。
明确未完成示例：OPT-08执行计划仍注明index-only scan未实现，VisibilityMap头注释不能替代执行证据。
下一阶段先复验完整矩阵，再按P0及原未验证/部分项分解原子要求、补实现与回归，逐项有证据后更新状态。
本轮原功能项未重写；R180等用户暂缓不恢复，总目标未完成。不push，Actions禁用。

最新有限修复：[R182共享UTF-8拆分](issue-review-r182-utf8-split-linear.md)统一修复TRIM、LPAD/RPAD、
TRANSLATE各实参和STRING_TO_ARRAY(NULL delimiter)的平方级前缀扫描，保留各自规则。
PG完整329、旧R183完整329及V2 native76语义全0；这是性能修复，不是旧兼容性错值。
Root与accepted-only clean各23完整入口实际全0，含新76/329及原80/96/294/80/190/654、三个真实Main。
每侧76对象18当前程序/127头已核对；fresh1生产1native，57生产13native/stub/3Main为已验证donor，
所有程序新链接当前库；Root normal1CPP并链接/repeat零编译零链接，clean显式构建。
全部2206已接受源码投影等于tested clean；Root2325保留其他改动及2323源码/118缓存恢复副本。
跟踪603份MD、921native+568注册Python+3Main=1492入口；工作区606份MD、927+574+3=1504，
原12未接受入口与三份文档仍分离；V1九个错误类型期望/-6中止及R183独立修复保留，未删检查。
这不是全1492/完整72/TLS/整库sanitizer验收。
R180及其他用户暂缓不恢复，R164超时仍未解决；原273仍22/166/70/15，总目标未完成。
不push，Actions禁用。以下状态与数量均指记录当时。

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

新增[R128 TIME二进制参数](issue-review-r128-time-binary-parameter.md)158协议控制全过，实际PG158
全过、旧完整132失败；初版旧fixture112项后缺RowDescription中断保留、强化失败
收集后按同一158期望重跑。NetworkServer实际normal1/repeat0均0，最终24native+
28协议+3核对后新链接当前57库并实际执行的真实Main=55不同完整入口全0。
原122核心匹配（原始2条PG内部来源字段不同保留）、原97精确匹配；本项接受
898native+530Python+3Main=1431，工作区901+533+3=1437含原六候选，554AST/13Bash
另检。无新完整注册/TLS/整库sanitizer证明，不冒重编Main或旧heap升级。

新增[R127 直接TIME过高精度](issue-review-r127-time-excess-precision.md)252原生/276协议全过，
PG276全过、固定旧原生225/协议246失败；2/0及复查1/0增量normal/repeat均0。
最终24native+27协议+3核对后重新链接当前57库并执行的真实Main=54不同完整入口
全0，不冒重编Main。原122核心行为全匹配，原始两条PG内部诊断来源不同不伪造；
原97仍精确匹配。本项接受898native+529Python+3Main=1430，工作区901+532+3=1436
含原六候选；553AST/13Bash另检。以下轮次保留各自历史输入，无新完整注册/TLS/
整库sanitizer证明。

新增[R126 TIME关键字精度语法](issue-review-r126-time-precision-grammar.md)严格549原生/326协议，
实际PG326全过，固定旧原生217/协议296失败。首版协议只完成19项后重置的实际1
保留并修正；末版23native+26协议+3核对后重连的真实Main=52不同完整入口全0。
原122现120匹配/2条TIME(7)通知及typmod差异，原97全匹配。本项接受897native+
528Python+3Main=1428，工作区900+531+3=1434含原六候选；552AST/13Bash另检，
无新完整注册/TLS/整库sanitizer证明，Main对象复用不冒称新TU编译。

新增[R125 合法TIME显式精度](issue-review-r125-time-precision-value.md)339原生/830协议，
PG830全过、旧830协议/317原生失败；最终22native+25协议+3真实Main=50完整入口
均0。未改Main TU核对后重连当前57库并执行，不冒重新编译。原97全匹配、122
仍12边界差异；本项接受896native+527Python+3Main=1426，工作区899+530+3=1432
含原候选，无新完整注册/TLS/整库sanitizer证明。

新增[R123 时间列precision](issue-review-r123-temporal-column-precision.md)782项原生/104项协议，
实际PG104全过、旧218原生/94协议失败；首版原生全过、协议六个R124路由失败保留。
最终匹配产物的有限回归见记录；不把metadata检查当微秒heap、CAST值舍入或新全量证明。

新增[R124 TIMETZ后缀表投影](issue-review-r124-timetz-postfix-projection.md)96项协议控制，
实际PG96全过、旧24失败，原schema回归暴露的六个42703断言保留。
本项重编与匹配产物的有限终态见记录，不冒新完整注册、真实TLS或整库sanitizer证明。

新增[R121 默认TIME文本输入](issue-review-r121-time-input-normalization.md)90native/177协议，
实际PG177全过、旧130协议/65native失败。最终18native+22协议+3真实Main=43入口
均0，Main是核对未改单元后重连当前57库并实际执行，未重编TU。原32对照全匹配，
97仍21显式精度差异。本项接受894native+3Main+524Python=1421，工作区897+3+527
=1427含原候选，无新完整注册回归、真实TLS或整库sanitizer证明。

新增[R119 时间戳binary](issue-review-r119-timestamp-binary-result.md)235低层/232协议，
实际PG232全过、旧144协议/192低层失败；保留首版完整36终态1及8个上游输入失败，
R120独立修复后共享41完整入口全0，原64条观察当前全匹配。本项接受组合
893native+3Main+523Python=1419，工作区896+3+526=1425含保留候选；无新完整注册
回归、真实TLS或整库sanitizer证明，不捏造R120之后又一轮编译/41重跑。

新增[R120 日期逆换算](issue-review-r120-date-cycle-inverse.md)独立枚举3652059天
及152协议控制，实际PG152全过、旧48失败；匹配58单元/三真实Main及有限终态见记录。
本项独立提交后892native+3Main+522Python=1417，工作区896+3+526=1425含尚未提交
R119及原候选；无新完整注册、真实TLS或整库sanitizer证明，组件sanitizer不冒充整库。

新增[R117 TIME binary](issue-review-r117-time-binary-result.md)121低层/144协议
控制，实际PG144全过、旧114协议/58低层失败。最终有限匹配终态见记录；本项
提交后891native+3Main+521Python=1415，工作区894+3+524=1421含保留候选，无新
完整注册、真实TLS或整库sanitizer证明，R118真实Main结果不冒充本项重跑。

新增[R118 时态物化布局](issue-review-r118-temporal-view-materialization.md)186条
协议控制，实际PG全过、固定旧60失败、首版仍36失败均保留。最终匹配回归/真实
Main终态见记录。本项提交后的接受组合为890native+3Main+520Python=1413，
工作区893+3+523=1419含保留候选；未获新完整注册回归、真实TLS或整库sanitizer证明。

新增[R116 视图完成tag](issue-review-r116-view-command-tags.md)54条完整协议
控制，实际180006全过、旧14tag失败。有限匹配回归与真实终态见记录；接受
890native+3Main+519Python=1412，工作区893+3+522=1418含保留候选，无新完整注册、
真实TLS或整库sanitizer证明。原273统计不提升。

新增[R113 INTERVAL binary结果](issue-review-r113-interval-binary-result.md)117低层
及171协议控制，实际PG171全过、依赖修复后旧149失败/原生78失败。最终有限回归
与失败原文见记录；接受890native+3Main+518Python=1411，工作区893+3+521=1417
含未接受R98/R109候选，无新完整注册运行，不证明真实TLS或整库sanitizer。

新增[R114 逻辑长度](issue-review-r114-physical-attribute-length.md)102原生及28协议
控制，PG28全过、旧22失败；候选曾剩5个既有CHAR失败，R115另项修复。最终有限
匹配回归见记录。本项接受889native+3Main+517Python=1409，工作区892+3+521=1416，
含保留候选和未接受R113；尚无新完整注册回归，原273统计不提升。

新增[R115 标量别名投影](issue-review-r115-scalar-alias-projection-metadata.md)228条
完整协议控制：实际180006全过、旧13失败。有限匹配产物回归及逐版失败见记录；
本项接受组合888native+3Main+516Python=1407，R114另项。工作区892+3+521=1416
含未接受候选及R113，未跑新完整注册集合，不证明真实TLS或整库sanitizer。

新增[R110 数组二进制结果](issue-review-r110-array-binary-result.md)110条
协议和182条低层控制，实际180006参考110零失败、旧75失败。有限回归和逐版
失败见记录；本项提交后接受组合888native+3Main+515Python=1406，含保留候选
的工作区891+3+518=1412，均无新完整注册回归。未证明真实TLS或整库sanitizer。

新增[R112 具名数组输入](issue-review-r112-unconstrained-named-array-input.md)
57条协议控制，实际参考180006零失败、固定旧程序44失败；逐版失败、正常/repeat
与有限门禁见记录。只新增一个协议注册入口，本项提交后的注册组合为887native+3Main+
514Python=1404，无新完整注册回归；R109/R110 候选另计，不复用旧全量证明。

当前运行结果、失败明细和复现方法见[代码审查索引](review-index.md)。验证入口为 `bash scripts/build_tests.sh`，只运行其已注册集合；未注册测试需单独运行或补入入口。

后续[R108数组元素相等](issue-review-r108-array-position-typed-equality.md)新增
354原生与258协议控制，最终参考258零失败、固定旧版本158失败；旧原生320/183
和早期239/153保留，逐版失败与原生夹具校正细节见记录。第五版
32native/29协议/3真实Main共64完整入口及28补充JSONB均0，核心/最终
fence0。两个新增注册入口对应接受manifest887native+3Main+513Python=1403，
含未接受R98工作区889+3+515=1407；本项有限验收不是新完整注册回归。

后续[R107长度函数签名](issue-review-r107-string-length-signatures.md)增加607
原生及654协议控制；最终参考654零失败、旧程序392失败，旧原生607/440失败。
首两版实际失败和逐版artifact核对分别保留；实际候选与邻接验收以本项记录
为准。新增两个注册入口，本项提交后的组合886native+3真实Main+512Python=1401；
工作区含R98时为888+3+514=1405。两套新组合均无全量终态，R106草稿未注册。

后续[R104 网络值转换](issue-review-r104-network-value-conversions.md)实际PG/
Root649、原738及完整SDK、21native/17协议/3真实Main通过；五CPP实际4/3/2
增量normal/repeat及核心/最终独立核对均0，按单项本地提交。旧649/419、SDK134、
首版582/3与649/58、第二版649/1保留；无新1399/1403全量，原273不变。

2026-10-16后续[R105 限定NAME→INET/CIDR](issue-review-r105-network-name-input.md)
PG/Root738、完整SDK及21native/16协议/3真实Main全过，自身真实全58/
normal/repeat、核心与最终独立核对均0，按单项本地提交。旧738/34、SDK134
保留；无新1398/1402全量；R104后续独立修复，原273未关闭。

2026-10-16后续[R103 NAME→MAC](issue-review-r103-macaddr-name-input.md)实际参考
最终PG/Root366、完整SDK及21native/16协议/3真实Main通过，真实全58/normal/
repeat和独立终态fence全0，单项本地提交。旧366/30、SDK134及首版358/6与
366/12失败保留；75纯组件sanitizer不冒整库证明，无新1398/1402全量。

2026-10-16后续[R102 INET/CIDR输入](issue-review-r102-network-input-notation.md)
最终旧188组件72失败、672协议389失败；当前188/纯组件ASan/UBSan、PG/Root672、
2558对照及21native/16协议/3真实Main通过；最终真实全58/normal/repeat、匹配
核心及最终fence全0，单项本地提交。首版672/23保留；无新1398/1402全量，
R103/R104/R105后续各独立修复，不把本项写成所有网络类型问题均已完成。

2026-10-16后续[R101 MAC输入](issue-review-r101-macaddr-input-and-widths.md)核心75、
PG/Root328及236额外codec差分通过，纯头ASan/UBSan75通过；二十完整native和
十五协议及三个真实Main全0。头依赖修正后全58真实重编0，所有对象/程序字节
相同，新匹配核心2/328及最终fence全0；单项本地提交，不冒新1396或工作区1400
全量。旧75/38、328/186及首版314/5失败保留，不挪用旧1391全量结果。

2026-10-16后续[R100几何输入](issue-review-r100-geometric-coordinate-delimiters.md)
最终232组件、PG/Root230、十个完整native及七个完整协议通过，纯组件ASan/UBSan
通过；旧232/104、230/135、首轮准备超时与旧夹具134均保留。只证明本项范围，
不把下列1391历史全量用于本项后的1394或含未提交R98的1398组合。

2026-10-15最新[冻结5091b39c完整回归](issue-full-registered-5091b39c.md)实际退出0：
881 native、3真实Main及507 Python共1391入口各一次，1390实际通过、1 TLS环境
skip、0失败。源码/生产产物及全部58 receipt、57 test库字节独立终态核对0。
不证明真实TLS、原273全部完成或旧间歇性失败根因已修复；下方保留旧输入结果。

2026-10-13 审查输入为 `a502ffa3`。本轮完整注册回归已终态：普通native 783通过/2失败，真实Main入口3通过，Python驱动436返回成功/8失败，共1232项、10项失败；Python成功数包含一个无OpenSSL的TLS显式跳过。驱动实际exit1，源码及冻结生产二进制终态校验通过，注册集合无遗漏/重复。完整失败清单、实际SQL差分、正常API错值和修复建议见[审查报告](code-review-2026-10-13.md)。前次同源码专项60/60 native、45/46 wire中的ALTER ARRAY超时仍保留，不能用本次单次通过消除；下方历史全绿记录不代表当前HEAD。

## 历史记录：2026-08-14

以下仅保存当时 137 个 C++ 测试和 2 个 E2E 的 PASS=139、FAIL=0 记录，不代表当前 HEAD、当前测试总数或所有功能已验证。
当前功能状态见[原总账](postgresql-18-gap-audit.md)，原始旧依据为[命令手册](commandsList.md)和[历史 TODO](all-gaps-todo.md)。

本轮完成性能与并发硬化轮次的全面复验（13 个提交后）：

- **全量回归**：`./scripts/build_tests.sh` 完整重跑，137 个 C++ 测试 + 协议 E2E + 窗口函数 E2E（13/13）全部通过，0 失败。
- **并发与事务专项独立复跑**：`concurrency_test`（含 INSERT/UPDATE/DELETE 回滚恢复二级/复合/Hash/TOAST、checkpoint 持久化、MVCC 快照隔离、性能子项）、`wal_truncate_test`、`redo_crash_recovery_test`、`prepared_transaction_test` 在独立临时目录中单独复跑，全部通过。
- **线程安全压测（新增，本轮引入）**：针对 `BufferPool` 新的两阶段加载/孤儿帧失效语义，构造多线程压力（8 读者 + 改写者 + 失效器 + 4 帧小池强制驱逐/重载），分别在 TSAN 与 ASAN+UBSan 下运行：**0 数据竞争报告、0 内存错误、0 内容损坏**。修复前同一压测报告约 1 万次内容损坏（`invalidatePage` 在并发读者下重置被 pin 帧导致帧复用污染）。
- **确定性失效语义回归（新增）**：pin → invalidate → unpin → 重取必须返回新鲜正确数据；invalidateAll 后重取同样正确；同线程持 pin 期间重取同页返回不可用而非死锁。
- **性能基准复测**（与优化前同机对比）：事务内带 PK 插入 200 行 86–114ms（1760–2370 行/秒，优化前 11.7 行/秒），commit 12–15ms（优化前 267ms）；并发套件 500 行插入 + COUNT/SUM/MAX/GROUP BY 聚合 1.4ms、插入 265–305ms（优化前约 106s）；索引等值查找 14 次查找 1–2ms。
- **回归发现的缺陷均在当步修复**：removeMulti 二分改造后等值键 RID 扫描、schema 缓存引入期捕获的 RLS/fillfactor/catalog 快照隔离、hashidx 失效误指 secidx 等，均由现有测试套件（gin_brin_index_test、policy_test、fillfactor_test、catalog_snapshot_test 等）拦截后修复，最终保持全绿。

本轮新增 Volcano 执行失败契约回归：合成 `open()` 失败必须由
`executePlanChecked()` 返回 `ok=false` 和错误信息，不能与合法空结果混淆；所有测试调用
方均使用 checked contract，旧空结果兼容入口已删除。

本轮新增运行时统计持久化回归：数据库/表统计可从当前格式快照重载，损坏 magic/版本或
尾随数据会 fail-closed；关系删除标记不会在下一次持久化时恢复旧表统计。

本轮新增协议配置边界回归：不同 backend 的 `statement_timeout` 会话值互不污染，非管理员
不能执行 `SET GLOBAL`，进程级参数不能通过普通 `SET` 修改；planner 参数变化后旧 `EXPLAIN`
缓存不会被复用。

本轮新增配置文件回归：非法键和值、非有限浮点、越界整数和损坏配置加载均拒绝；候选配置
失败不会污染当前对象，成功保存使用临时文件、`fsync` 和原子替换。

本轮新增 typed DDL 类型边界回归：未知列类型和非法类型修饰符在物理变更前拒绝，不再
静默降级为 `varchar`；合法 `SERIAL`、`NCHAR`、`BINARY` 映射及自动序列仍保持可用。

本轮新增 DDL 路由收敛回归：`CREATE/DROP DOMAIN`、`CREATE/DROP SEQUENCE`、
`CREATE/DROP SCHEMA`、角色/用户和数据库均通过 typed bridge 执行，确认标准入口不再依赖
`main.cpp` 旧字符串分支。
同时验证 `ALTER SEQUENCE ... RESTART ... INCREMENT BY ...` 和 `RENAME TO` 由 typed bridge
执行；重命名保持 catalog OID、更新 `nextval` 默认表达式，重启后新名称可用且冲突不会破坏原对象。
存储完整性回归还验证了损坏 heap page、损坏/截断边界被拒绝，以及 page compaction 不覆盖 special space。

本轮新增序列持久化回归：`sequence_full_test` 验证失败的 `ALTER SEQUENCE` 不会遗留启动恢复快照，
重启后 `nextval` 保持已提交状态；序列文件拒绝尾随字段、非法 cycle/范围/cache，写入使用原子发布，
`INT64` 正负边界、`currval`、`setval` 和非循环耗尽行为均通过。

本轮新增 DDL 会话生命周期回归：typed `DdlExecutor` 执行结束后恢复原线程局部会话指针，避免局部
`Session` 销毁后后续嵌入式存储调用解引用悬空指针；完整序列专项测试通过。

本轮新增数据库生命周期持久化回归：新建数据库的 `tlist.lst`、字符集和 public schema 标记必须完整，
checkpoint 文件必须是精确的当前三字段格式且拒绝尾随数据；完整回归继续验证物理备份标记和恢复路径。

本轮新增数据库 DDL 错误传播回归：构造数据库目录创建失败时，`CREATE DATABASE` 返回错误并输出
SQLSTATE，不会伪报成功；`DROP DATABASE` 也只接受 `StorageEngine::OK` 为成功结果。

本轮新增 DDL 路径与重复对象回归：重复 `CREATE SCHEMA` 必须失败，`IF NOT EXISTS` 才跳过；损坏
domain sidecar、非法数据库路径、序列/视图/关系名称路径组件均 fail-closed，schema marker 和
domain 创建使用原子发布。

本轮新增辅助对象 sidecar 回归：view、function、TVF、procedure 的元数据父路径被普通文件占用时
返回 `IO_ERROR`，正常定义使用原子发布，重复定义返回 already-exists 且不会覆盖旧内容。

本轮补强 `DROP TABLE ... CASCADE` 回归：构造 catalog 依赖表及其索引，验证 CASCADE 同时清理
依赖表的物理关系和索引 sidecar，再删除依赖 catalog 行；RESTRICT 预检仍不产生任何修改。

本轮新增索引 sidecar 故障回归：`.secidx` 或 `.idxnames` 被目录占用时，`CREATE INDEX` 返回错误，
不会留下物理索引或名称映射；索引元数据改写采用原子发布，复合索引删除保留剩余元数据选项。

本轮修复 `planner_runtime_stats_test` 的后台 writer 清理竞态：测试 teardown 通过 `dropDatabase()` 释放
数据库缓存与 WAL 所有权后再清理目录；专项测试和完整回归均通过，最终基线保持 `PASS=139 FAIL=0`。

---

## 测试环境

- OS: Linux 6.8.0-117-generic
- Compiler: g++ -std=c++17 -O2 -pthread
- 标准编译命令：`./scripts/build.sh`；规范测试入口：`./scripts/build_tests.sh`；`./scripts/run_all_tests_fast.sh` 是只改变输出形式的安静外壳
- `build_tests.sh` 会在测试前检查 `dbms_main` 的源码、头文件、manifest 和构建配置指纹；二进制缺失或过期时自动原子重建，因此完整回归不依赖调用者预先执行 `build.sh`。
- 生产二进制构建失败时，`build_tests.sh` 会立即 fail-closed，拒绝运行任何可能过期的 E2E 二进制。
- 每个 C++ 测试在独立临时工作目录中运行，回收目录后才进入下一项；顺序回归不再共享 `.txnid`、WAL、catalog 或后台 worker 的相对路径状态。
- 本轮构建质量检查：主程序在 `-Wall -Wextra` 下无编译警告；修复 join cost 参数错误及 parser/测试中的未使用代码。
- schema 格式回归：当前格式 `0x44420009`，长表名、列名和约束名可完整写入/读取；旧格式仍按 fail-closed 策略拒绝。
- 窗口函数端到端测试：由统一回归入口执行 `tests/window_e2e_test.py`（13 用例，包含 Volcano `WindowAgg` 排名/偏移、窗口聚合、`ROWS/RANGE/GROUPS` frame/exclusion、OFFSET 和命名窗口路径，使用隔离临时目录和临时管理员账号）
- Volcano 窗口单元测试：`tests/volcano_select_phase51_test.cpp` 覆盖多窗口独立排序、排名并列、分区边界、`lag`、默认 frame、`EXCLUDE CURRENT ROW`、count/first/last/ntile/percent_rank/cume_dist 及文本/JSON EXPLAIN。
- Volcano 分组单元测试：同一测试覆盖 `GroupAggregateOp` 的常见聚合、`HAVING`、`ROLLUP`/多个 grouping set、省略分组列的 `NULL` 输出及文本/JSON EXPLAIN；主 SQL 手工回归同时验证普通 GROUP BY 与 ROLLUP。
- Semi/Anti/Existence/Quantified/Scalar 回归：`tests/volcano_select_phase51_test.cpp` 验证未关联 `IN`/`NOT IN`、inner 过滤、重复键、NULL 语义、`QuantifiedSubqueryFilterOp` 的 `ANY`/`ALL` 空集与 NULL 语义、`ExistenceFilterOp` 的 `EXISTS`/`NOT EXISTS` 及 `ScalarSubqueryProjectOp` 的 0/1 行与 cardinality error；协议 E2E 验证真实 PostgreSQL 查询路径及 NULL 结果。
- 普通聚合结构化回归：`tests/volcano_select_phase51_test.cpp` 验证带过滤条件的隐式空 grouping set；协议 E2E 验证主 SQL `COUNT(*)`/`SUM()` 已走统一聚合计划路径。
- TOAST 回归：`tests/toast_test.cpp` 验证大值插入/更新/删除、zlib 压缩后的物理体积和解压读取。
- 事务回滚回归：`tests/concurrency_test.cpp` 验证 INSERT、UPDATE、DELETE 的普通 `ROLLBACK` 与 `ROLLBACK TO SAVEPOINT` 一致恢复主键、单列二级、复合、Hash 索引和 TOAST；DELETE savepoint 回滚后在同一事务提交仍可读，UPDATE/DELETE 场景在新 `StorageEngine` 重启后再次校验堆、Hash 与大字段内容。
- 锁管理器并发回归：`tests/lock_manager_concurrency_test.cpp` 使用真实双线程验证等待图死锁只释放一个受害者、表锁重入与共享锁升级、row/page token 归属、等待期间资源生命周期和批量清理；`tests/cross_backend_lock_test.cpp` 验证独立 `StorageEngine` backend 共享同一锁注册表、隔离不同数据库的同名表、backend-local 超时策略，以及独立 fork 进程对同一数据库的表/row/page 锁阻塞、释放后获取和 gap 锁阻塞；重复运行 10 次通过。
- 保存点锁资源回归：`tests/lock_manager_concurrency_test.cpp` 验证保存点后新增的表/row/page/gap token 会在检查点回滚时释放，并验证保存点前共享表锁在后续升级后恢复为共享模式。
- 复制管理器并发回归：`tests/replication_concurrency_test.cpp` 验证复制槽激活/停用、值快照读取、standby/conninfo/sync 状态并发写入及 slot 生命周期；复制槽定义校验和 active slot 禁止删除也有覆盖。
- 锁失败传播回归：`tests/lock_failure_propagation_test.cpp` 持有真实表级排他锁并在另一线程执行建索引，验证调用方收到 `LOCK_CONFLICT`、等待图清理且不会继续发布索引。
- DDL 辅助对象回滚回归：`ddl_transaction_skeleton_test` 验证失败时撤销 view、materialized view、UDF/TVF、procedure、trigger、RLS policy 和 collation；collation 最后一个条目删除后物理文件也会清理。
- DDL 外层事务回归：`ddl_transaction_skeleton_test` 验证多个 CREATE 跨语句由外层 `ROLLBACK` 逆序撤销，并验证 `ROLLBACK TO SAVEPOINT` 只撤销保存点后的 CREATE；含该内存 undo 队列的事务会拒绝 `PREPARE TRANSACTION`。
- 2PC 跨 backend/跨进程重启回归：`prepared_transaction_test` 由独立子进程创建 prepared 事务，验证 PREPARE 前刷出 heap/index 缓存、prepared 文件 durable 原子发布、表/row/page/gap 锁在另一进程完成前持续阻塞、重启后锁 ownership 重建、`COMMIT PREPARED` 后数据可见、`ROLLBACK PREPARED` 后数据不可见、顺序/索引条件查询均不可见，以及事务内拒绝完成 prepared transaction。
- DDL DROP/REPLACE 回归：`ddl_transaction_skeleton_test` 验证 DROP TABLE 与 CREATE OR REPLACE VIEW 的变更前快照恢复，外层回滚先恢复对象再撤销同一事务中的新增行；快照已被 DDL 修改后另一条快照型 DDL 及 SAVEPOINT 会 fail-closed。
- CMake 与脚本构建共同读取 `cmake/dbms_sources.txt`，并在配置阶段校验源文件存在性、重复项和 `src/main.cpp` 入口，避免生产源文件列表漂移。
- CMake 现在提供 `check`/CTest 标准入口；它们调用唯一的 `scripts/build_tests.sh` 完整测试编排器，不再复制 standalone test 的链接、stub 选择或 E2E 调度逻辑。
- `snapshot_export_import_test` 现在覆盖 v2 快照数据库绑定、版本/长度/尾随字节/XID 校验、重复导入、跨数据库导入，以及首次读写后拒绝替换快照；`DELETE`/`TRUNCATE` 也会阻止事务中途导入快照。
- `scripts/build.sh`、`build_tests.sh`、`build_one_test.sh` 和 `run_all_tests_fast.sh` 共同读取 `scripts/build_common.sh`；本轮验证了配置指纹失效与增量单测入口。
- `./scripts/build_tests.sh` 是唯一的完整测试编排实现，会先通过 `build_common.sh` 的共享逻辑检查并构建 `dbms_main`，再缓存生产对象、逐个测试独立链接运行，并自动执行两个 E2E；因此不依赖调用者预先运行 `build.sh`。`run_all_tests_fast.sh` 仅捕获其成功输出并显示 PASS 计数，失败时原样打印完整编译/链接/运行日志。
- 文档一致性检查确认根目录旧版 `MANUAL.md` 已移除，`docs/MANUAL.md` 是唯一受 README 与测试/部署文档引用的完整手册。
- PostgreSQL 协议 E2E 覆盖普通顶层多行 DML 和写 CTE 的 statement atomicity：批量 INSERT 或 CTE INSERT 后续行冲突时，前序行不会残留；成功的 `WITH ... RETURNING` 结果也能通过协议返回。
- PostgreSQL 协议 E2E 覆盖 typed `CREATE TEMP/TEMPORARY TABLE`：临时表在所属会话内持续可用、遮蔽同名永久表，不同 backend 可创建同名临时表，其他会话不可见，断开连接后对象被清理。
- PostgreSQL 协议 E2E 额外覆盖 `ON COMMIT DELETE ROWS` 与 `ON COMMIT DROP`：提交后分别保留空表和删除临时表；parser 单测覆盖 `PRESERVE/DELETE/DROP` AST 状态。
- 新增临时表重启生命周期回归：`tests/temp_restart_cleanup_test.cpp` 模拟临时 relation 已落盘后重启，确认 `tlist.lst`、schema/data/index/TOAST 等残留全部清理；完整测试入口通过。
- 同一回归还覆盖 `CREATE TABLE` 在损坏 heap 初始化失败时的清理：schema、heap 和 `tlist.lst` 均不残留；唯一/索引初始化失败路径同时释放元数据锁。
- `tests/ddl_transaction_skeleton_test.cpp` 新增后处理失败回归：强制约束 metadata 写入失败，验证 DDL executor 的物理表创建会通过事务记录完整回滚。
- 同一测试覆盖 CatalogManager 只读 drop plan 与延后应用边界；`DROP TABLE` 的物理 relation 仍独立于 catalog plan，executor 只在物理删除成功后应用计划。
- 同一测试新增 `DROP SCHEMA` 的 namespace drop plan 延后应用回归，确认 catalog 计划与物理 schema 删除相互独立。
- 新增数据库生命周期回归：`DROP DATABASE` 释放数据库级缓存后，同名重建不会继承旧 CLOG/WAL/page/index 状态。
- 新增 typed `ALTER TABLE` 路由回归：验证 AST bridge 的 ADD COLUMN、DEFAULT、NOT NULL、RENAME COLUMN/TABLE，以及 `RENAME TO` 不再误判为列重命名。
- 新增 ALTER statement atomicity 回归：`ddl_transaction_skeleton_test` 验证多子命令中前一动作已落盘、后一动作失败时，整句 schema 会从 DDL 快照恢复，且事务快照不会残留。
- 新增外置 tablespace 事务快照回归：表移动到自定义 tablespace 后，在事务内移回默认目录并回滚，快照恢复 schema 与外置 heap 数据；同一场景覆盖全新引擎读取。
- 新增 DDL 提交失败回归：延迟 CHECK 使 `StorageEngine::commitTransaction()` 失败时，`DdlTransaction` 返回失败、清理事务状态，并恢复已执行的物理 `ALTER TABLE` 快照，调用方不会输出成功结果。
- DDL 隐式提交错误传播回归：`deferrable_test` 在延迟 CHECK 失败的显式事务中执行 `CREATE DATABASE`，确认隐式提交失败后 DDL 不继续执行、事务清理且目标数据库不存在。
- DDL 隐式提交入口审计：规范执行器和 legacy DDL 分发共用失败即停止的提交守卫，避免兼容路径吞掉 `commitTransaction()` 错误。
- 新增事务资源生命周期回归：普通 COMMIT/ROLLBACK 不会遗留整库快照；DDL 快照在 COMMIT/ROLLBACK 后清理；前一事务的 deferred CHECK 导致隐式提交失败时，后续 `BEGIN` 返回错误且不会开启新事务。
- 新增 DDL 快照并发回归：文件级 DDL 快照使用事务 ID 独立命名，并持有数据库级排他锁；另一 backend 在快照事务结束前阻塞，避免物理恢复覆盖并发提交。
- 新增 DDL 快照重启回归：模拟未完成的 `.txn_backup.<xid>`，新 `StorageEngine` 按无 COMMIT 证据恢复 DDL 前的 schema 并清理快照目录。
- WAL 提交回归继续覆盖 COMMIT 记录与刷盘路径；`WALManager::XLogFlush()` 现在返回 segment 同步结果，提交在 WAL 不可用/刷盘失败时不会先发布 CLOG 可见性。
- CLOG 持久化回归继续验证段重载；段写入现在完整写入临时文件、`fsync` 后原子替换并同步 `pg_xact` 目录，写失败不会提前清除 dirty 状态。
- CLOG 提交故障回归：删除事务的 `pg_xact` 目录后，提交返回 `IO_ERROR`，行级修改被撤销并追加 ABORT WAL；新 `StorageEngine` 重启恢复也不会重新出现该行。
- CLOG 跨 backend 回归验证两个独立 `CommitLog` 实例对同一段的按位合并，以及已打开读缓存发现外部段替换后刷新，不会丢失或永久读取旧事务状态。
- CLOG 截断回归验证旧段正常回收，并在文件锁导致持久化失败时保留最后一个 durable segment image。
- WAL 截断回归验证未归档段不会删除、已归档且早于保留 LSN 的段会回收、最早保留段可被恢复扫描发现，并且后续 checkpoint 不会为已删除段重建 archive marker。
- WAL 归档故障回归验证归档目录不可用时源段和 `.ready` 均保留，修复目录后可重试完成原子归档与 `.done` 发布。
- Catalog 持久化回归验证临时文件替换/目录同步失败会返回错误，并保留原有 catalog 目标文件，不会把部分写入报告为成功。
- WAL 崩溃恢复回归覆盖首个合法 LSN 0、未初始化页 LSN、已提交插入/删除重做和未提交事务回滚；`redo_crash_recovery_test` 同时校验 heap 与主键 B+Tree，并覆盖同一事务连续插入两行后 before-image 逆序 undo 不会留下中间状态；多实例 WAL writer 的尾部刷新/锁也由 catalog snapshot 场景覆盖。
- 新增 `recovery_integrity_test`：构造 CRC 正确但 payload 损坏或路径越界的索引 WAL 记录，验证恢复拒绝非法镜像、不会写出数据库目录外文件，并以异常中止启动；heap/index 应用失败均不再静默忽略。
- `recovery_integrity_test` 同时覆盖损坏 WAL 长度/`xl_prev` 链、非法 heap page image 和非连续 page ID 输入；WAL 截断回归使用完整有效记录构造 segment，并验证记录所在完整 segment 的刷盘边界。
- Checkpoint 回归现在断言 checkpoint 真实成功；页刷盘、checkpoint WAL、checkpoint 文件 fsync 和 archive status 任一失败都会返回失败，不再伪报 `CHECKPOINT completed`。
- Checkpoint 活动事务回归验证：活动事务期间不会推进数据库恢复起点；事务结束后 checkpoint 才刷已加载 heap/index 缓存并成功持久化 checkpoint WAL/LSN。
- SSI 回归：`phase5_remaining_test` 验证非空索引谓词按 heap page 登记 SIREAD，单列主键/二级 B+Tree 比较谓词登记逻辑索引范围并与写键进行重叠检测，非相交页事务可同时提交；跨页读写危险结构仍有一个事务返回 `SERIALIZATION_FAILURE`，空谓词关系级保护继续生效。复合/表达式/部分索引、其他访问方法的物理 predicate lock 和完整 SSI 规则仍未完成。
- BufferPool 回归覆盖一帧缓存的脏页淘汰/重载与 pinned 页保护，确认缓存不足时返回失败而不是强制淘汰活动页。
- GIN/BRIN/B+Tree 回归继续通过，并覆盖 root split/search/range、长键固定宽度规范化、重开索引后的查找和精确删除；B+Tree 的 header/node I/O 失败现在 fail-closed，避免底层页读取失败升级为空指针崩溃。
- B+Tree 多值索引回归补充 `(key, RID)` 精确删除：删除 6000 条跨叶重复键中的单个 RID 后，其余相邻 RID 仍可检索，重复删除会正确失败；生产 DML/事务回滚路径直接使用 `StorageEngine` 的 B+Tree 实现。

- 索引架构清理回归：删除未接入生产执行链的 `IIndexAM`、B+Tree/Hash 适配器及独立 GIN/BRIN 类；保留并增强测试以验证 `StorageEngine` canonical GIN/BRIN 创建、查询和损坏 sidecar fail-closed。
- CREATE INDEX 事务回归：B-tree、Hash、GIN、BRIN 在显式外层事务回滚后均清理物理文件、名称映射和 catalog；重复名称拒绝，`IF NOT EXISTS` 安全跳过。
- 事务索引 WAL 回归通过：B+Tree/Hash `flush()` 和事务边界统一缓存刷盘路径已接入；脏索引刷盘会记录完整文件 before/after image，提交前 WAL 先刷盘；`wal_basic_test` 检查索引 WAL 记录，`redo_crash_recovery_test` 验证未提交插入回滚、已提交插入恢复和已提交删除恢复。
- Hash/GIN/BRIN 持久化回归覆盖 `HashIndex` 二进制格式重载、StorageEngine GIN/BRIN sidecar 原子发布和损坏文件拒绝；Hash dirty 状态在关闭失败时保留。
- StorageEngine GIN/BRIN 回归通过真实 DDL 构建后查询索引内容，验证 `forEachRow()` 页面失败契约、GIN 原子发布和 BRIN 二进制范围文件路径。
- 索引/执行器错误传播回归通过：B-tree/复合/全文/GiST/SP-GiST/Hash/GIN/BRIN 构建与 `REINDEX` 不再吞掉 heap 扫描失败，Volcano 顺序扫描和索引扫描遇到页面读取失败时 fail-closed；`volcano_select_phase51_test` 通过。
- 执行器架构清理回归：删除未使用的 `IExpr`/`PlanNode`/`IExecutionPlanner` 接口后，Volcano 算子仍统一实现 `IOperator`；`IndexScanOp` 改用带 page shared lock 和 MVCC 检查的直接 RID 回表，`volcano_select_phase51_test` 的索引、bitmap 和组合算子路径全部通过。
- RLS 访问路径回归：带 `owner` 索引的启用 RLS 表，Volcano 计划强制选择策略感知 `TableScanOp`，只返回当前用户可见行；索引绕过测试通过。
- 访问方法安全回归：移除不具备 MVCC/NULL 位图证明的伪 `IndexOnlyScanOp`；Bitmap AND/OR 改用受保护 RID 回表并保持原谓词 recheck，Volcano 专项和全量回归通过。
- 统计/查询/DML 错误传播回归通过：`ANALYZE` 失败不发布半成品统计文件，过滤器、聚合、JOIN、FK/EXCLUDE 检查、ON CONFLICT 目标扫描、表重写和 TOAST/page 写入路径不再把页面 I/O 失败当成空结果或成功；Volcano 并行 page-range scan 会向算子返回失败。
- 新增 schema 格式完整性回归：截断 schema 与错误 magic 均 fail-closed，不会返回可写的部分 schema。
- Docker 镜像构建：`docker build -t dbms-cpp:verification .` ✅（OpenSSL 真实 TLS）；Compose 配置检查 ✅。
- CMake：当前验证环境未安装 `cmake`，配置/编译未执行。
- 网络启动安全边界：无 TLS 证书/私钥时默认拒绝启动；明文仅由显式 `--insecure` 开关启用。
- 网络服务生命周期回归：`tests/network_server_lifecycle_test.cpp` 验证端口占用时 `startServer()` 返回失败、显式 shutdown 停止监听并等待客户端 worker，活动连接槽位最终归零。
- 网络连接容量回归：accept 路径原子预留 `maxConnections` 槽位，认证失败和 TLS 握手失败均释放槽位。
- 网络输出并发回归：`tests/network_security_test.cpp` 并发创建 4 个线程局部输出捕获，每个线程写入 32 行并验证没有其他会话内容串入。
- 角色 DDL 回归：`CREATE/ALTER/DROP ROLE`、`CREATE/ALTER/DROP USER` 统一走 Catalog/AST 路径，混合大小写密码通过真实验证。
- PostgreSQL 协议 E2E：`python3 tests/postgres_protocol_test.py` 验证 SSLRequest/plaintext 协商、StartupMessage、真实 SCRAM-SHA-256 握手、simple query 和 Parse/Bind/Execute/Sync，并覆盖 NUMERIC 精确存储、1700 binary 参数/结果与 PostgreSQL INSERT command tag。
- 协议扩展查询回归：验证 `ParameterDescription`、文本和 binary `int4` 参数 `$1`、binary `int4` 结果、statement `Describe`、portal/statement `Close` 的真实 Parse/Bind/Execute 生命周期，以及 OID/长度/属性号/表 OID RowDescription 元数据。
- 协议日期时间/UUID binary 回归：验证 `date`、`time`、秒精度 `timestamp`/`timestamptz` 和 `uuid` 的 binary 参数、binary 结果、OID/格式元数据及单列空格值不被拆分。
- 协议 portal 分页回归：验证 `Execute maxRows` 分批返回、未耗尽时发送 `PortalSuspended`、耗尽时发送 `CommandComplete`，以及耗尽 portal 再执行不重复返回数据。
- 协议 ACL 回归：alice 将 `SELECT` 授予 `analyst`，bob 通过角色继承读取表数据；bob 未获得 `INSERT` 时真实协议返回权限错误。
- 角色继承边界回归：`NOINHERIT` 用户保留原始成员关系但不获得父角色 ACL/RLS 权限，切换为 `INHERIT` 后两条权限路径同时生效；`pg_hba.conf` 角色匹配仍使用原始成员关系。
- 对象授权回归：非所有者即使带有管理员标志也不能执行 `ALTER TABLE ... OWNER TO`；所有者仅可转移给能够 `SET ROLE` 到的目标角色；`SET ROLE` 后超级用户绕过和表 ACL/RLS 有效角色均正确收敛。
- owner ACL 回归：无显式 `.permissions` 记录时，表 owner 仍拥有表级/列级权限、`GRANT OPTION` 和 `all` 权限视图。
- 表级 GRANT OPTION 回归：验证表级 grant marker 仍提供基础列权限，独立撤销只移除 grant option，存在下游授权时 RESTRICT 拒绝、CASCADE 递归撤销并清理 grant chain。
- 角色 ADMIN OPTION 回归：验证角色关系的 ADMIN OPTION 授予、重复授权升级、只撤销 ADMIN OPTION，以及撤销后成员关系仍保留。
- 默认权限回归：验证 typed `ALTER DEFAULT PRIVILEGES` 的表级多权限 GRANT、幂等写入、REVOKE 只影响后续新表，以及不支持对象类型/grant option 时 fail-closed。
- TRUNCATE 回归：验证 typed 多表 AST、`RESTRICT` 在 FK 依赖下不产生部分修改、递归 `CASCADE`、多表截断、`ONLY` 字段及 `RESTART IDENTITY`。
- ALTER TABLE typed 回归：`tests/alter_table_only_test.cpp` 验证 RLS enable/disable/force/no-force、ATTACH/DETACH PARTITION 的 AST 规格传递，以及 trigger enable/disable 的持久化状态。
- EXCLUDE typed 回归：`tests/exclude_test.cpp` 通过 DDL bridge 验证 `ALTER TABLE ... ADD/DROP CONSTRAINT ... EXCLUDE` 的约束名保留、冲突拒绝、删除后放行和持久化清理；CREATE TABLE 的等值与范围排斥回归继续覆盖 INSERT/UPDATE。
- ALTER TABLE typed 清理回归：`tests/ddl_bridge_routing_test.cpp` 验证基础动作在删除 `main.cpp` 重复分支后仍由 typed bridge 执行；`tests/ddl_ast_bridge_test.cpp` 进一步验证 CLUSTER、REPLICA IDENTITY、VALIDATE/ALTER CONSTRAINT 的 catalog、索引归属和持久化结果。
- 协议错误恢复回归：验证扩展查询错误后的 Bind/Execute 被忽略至 Sync、Sync 后连接可继续查询，以及事务外错误返回 `ReadyForQuery('I')`。
- 协议失败事务回归：验证显式事务中的语句错误返回 `ReadyForQuery('E')`/`25P02`、普通命令被拒绝、失败状态下 `COMMIT` 不提交先前 INSERT 而执行完整回滚，以及 `ROLLBACK TO SAVEPOINT` 可恢复外层事务。
- 协议两阶段命令边界回归：验证失败事务中的 `COMMIT PREPARED`/`ROLLBACK PREPARED` 不会被改写为本地恢复命令，未知 prepared transaction 返回错误。
- 协议 backend 隔离回归：双连接验证事务 ID/快照不串线，未提交行不可见，ROLLBACK 后状态清理，连接断开回滚未完成事务，COMMIT 后新事务可见；另覆盖 `START TRANSACTION` 选项与 `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` 路由。
- SQL 统计模块回归：`tests/sql_stats_test.cpp` 验证字符串/数字常量归一化、引号标识符区分、调用次数与耗时聚合、数据库过滤、`pg_stat_statements.max` 确定性淘汰、当前格式快照重载、尾随数据损坏拒绝及 reset。
- 运行时统计模块回归：`tests/runtime_stats_test.cpp` 验证并发 SQL 计数、失败/提交/回滚计数、表扫描和实际 DML 行数统计及 reset。
- 协议运行时统计回归：`postgres_protocol_test.py` 通过真实 PostgreSQL wire 查询验证 `pg_stat_database`/`pg_stat_tables` 返回数据库查询、表 DML 及 Volcano 索引扫描/取行计数，而非固定零值。
- Planner 统计反馈回归：`tests/planner_runtime_stats_test.cpp` 验证完整扫描 live-row 估计参与 Hash/Nested Loop Join 选型和 EXPLAIN，未取得精确证据时回退物理行数，重建同名表后旧估计不会泄漏。
- DML AST 回归：`parser_phase1_test.cpp`、`dml_semantics_test.cpp` 与 `dml_returning_test.cpp` 验证 `INSERT` 多行 AST、混合 `DEFAULT` 位置、`DEFAULT VALUES` 区分、简单单表 INSERT SELECT（含 WHERE/列表达式）、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`、单列及复合主键/唯一约束冲突的常量或只引用 `excluded` 的 evaluator 标量 `DO UPDATE`、目标行/`excluded` 受限 `WHERE`、复合 UNIQUE 中 NULL 不互相冲突、不同唯一约束冲突不会被错误忽略、以当前目标行列值为输入的受限标量表达式 UPDATE、单源表 `UPDATE ... FROM`、单源表 `DELETE ... USING`、INNER/CROSS JOIN 的 UPDATE FROM/DELETE USING、窄版 MERGE、INSERT/UPDATE/DELETE 的受限标量表达式 RETURNING 以及 DML 尾随垃圾 fail-closed；协议 E2E 验证普通单表 INSERT 的多行/表达式/DEFAULT、显式 NULL 不被默认值覆盖、字符串大小写保留、常量及行级表达式 UPDATE、简单谓词 DELETE、INSERT/UPDATE/DELETE 的列投影和受限标量表达式 `RETURNING`、INSERT SELECT/冲突处理返回结果和 `INSERT 0 N` command tag。

---

## 测试汇总

| 功能域 | 测试项数 | 通过 | 失败 | 备注 |
|--------|----------|------|------|------|
| 认证与连接 | 2 | 2 | 0 | — |
| DDL - 数据库 | 3 | 3 | 0 | — |
| DDL - 表 | 14 | 14 | 0 | 含 CTAS / RENAME / POINT / INET/CIDR 类型 |
| DDL - 索引 | 7 | 7 | 0 | B+Tree/Hash/FullText/GIN/GiST/BRIN/SP-GiST 基础路径通过；专用访问方法仍是简化实现，不宣称 PostgreSQL 完整 opclass 兼容；B+Tree 分裂/重复键/范围回归见 `gin_brin_index_test.cpp` |
| DDL - 视图 | 5 | 5 | 0 | 含 ALTER VIEW RENAME TO / SET SCHEMA |
| DDL - 触发器 | 2 | 2 | 0 | — |
| DDL - 用户/角色 | 4 | 4 | 0 | — |
| DML - INSERT | 5 | 5 | 0 | 含 AST bridge 的多行/DEFAULT/显式 NULL 回归 |
| DML - UPDATE/DELETE | 2 | 2 | 0 | — |
| DQL - SELECT | 5 | 5 | 0 | — |
| DQL - JOIN | 3 | 3 | 0 | — |
| DQL - UNION | 1 | 1 | 0 | — |
| TCL - 事务 | 3 | 3 | 0 | — |
| DCL - 权限 | 4 | 4 | 0 | 含 SHOW USERS/SHOW ROLES |
| 工具命令 | 8 | 8 | 0 | — |
| 分区管理 | 3 | 3 | 0 | Range/List/Hash + ATTACH/DETACH |
| 高级特性 | 2 | 2 | 0 | NOTIFY/LISTEN, RLS |
| **合计** | **73** | **73** | **0** | 新增跨 backend 2PC prepared 事务回归 |

---

## 1. 认证与连接

### 1.1 登录（admin）

**输入**
```
admin admin
```

**预期输出**
```
successfully login
```

**实际结果** ✅ `successfully login`

---

### 1.2 退出

**输入**
```
exit
```

**预期输出**
```
Quitting...
```

**实际结果** ✅ `Quitting...`

---

## 2. DDL - 数据库管理

### 2.1 CREATE DATABASE

**输入**
```sql
CREATE DATABASE testdb;
```

**实际结果** ✅ `Create Database succeeded (charset=utf8)`

---

### 2.2 USE DATABASE

**输入**
```sql
USE DATABASE testdb;
```

**实际结果** ✅ `set Database to testdb`

---

### 2.3 DROP DATABASE

**输入**
```sql
DROP DATABASE testdb;
```

**实际结果** ✅ `Database dropped`

---

## 3. DDL - 表管理

### 3.1 CREATE TABLE（圆括号 + 冒号格式）

**输入**
```sql
CREATE TABLE t1 (id:int:0:1, name:varchar(50):0);
```

**实际结果** ✅ `Table create succeeded`
> 注：此前 `(col:type:flags)` 格式因 tokenize 不分割冒号导致零列解析，已修复。

---

### 3.2 CREATE TABLE（花括号格式）

**输入**
```sql
CREATE TABLE t2 {id:int pk, age:int:0};
```

**实际结果** ✅ `Table create succeeded`

---

### 3.3 INSERT + SELECT 验证

**输入**
```sql
INSERT INTO t1 (id, name) VALUES (1, 'alice');
INSERT INTO t1 (id, name) VALUES (2, 'bob');
SELECT * FROM t1;
```

**实际结果** ✅
```
1 row(s) inserted
1 row(s) inserted
id name
1 alice
2 bob
```

---

### 3.4 ALTER TABLE ADD COLUMN

**输入**
```sql
ALTER TABLE t1 ADD COLUMN email varchar(100);
```

**实际结果** ✅ `Column added`

---

### 3.5 ALTER TABLE DROP COLUMN

**输入**
```sql
ALTER TABLE t1 DROP COLUMN email;
```

**实际结果** ✅ `Column dropped`

---

### 3.6 DROP TABLE

**输入**
```sql
DROP TABLE t2;
```

**实际结果** ✅ `Table dropped`

---

### 3.7 ALTER TABLE RENAME COLUMN

**输入**
```sql
ALTER TABLE t1 RENAME COLUMN name TO username;
```

**实际结果** ✅ `Column renamed`

---

### 3.8 ALTER TABLE RENAME TO

**输入**
```sql
ALTER TABLE t1 RENAME TO t1_new;
```

**实际结果** ✅ `Table renamed`

---

### 3.9 CREATE TABLE AS SELECT (CTAS)

**输入**
```sql
CREATE TABLE t3 AS SELECT * FROM t1;
CREATE TABLE t4 AS SELECT id, name FROM t1 WHERE id = 1;
```

**实际结果** ✅ `Table created with N rows`，数据正确复制

---

### 3.10 CREATE TABLE with POINT type

**输入**
```sql
CREATE TABLE geo_points (id:int:0:1, loc:point:0);
```

**实际结果** ✅ `Table create succeeded`

---

### 3.11 INSERT POINT values

**输入**
```sql
INSERT INTO geo_points (id, loc) VALUES (1, '0.0,0.0');
INSERT INTO geo_points (id, loc) VALUES (2, '10.0,10.0');
INSERT INTO geo_points (id, loc) VALUES (3, '20.0,5.0');
INSERT INTO geo_points (id, loc) VALUES (4, '-5.0,15.0');
```

**实际结果** ✅ 全部 `1 row(s) inserted`

---

### 3.12 SELECT with spatial operators

**输入**
```sql
SELECT * FROM geo_points WHERE loc << '15.0,15.0';   -- left of
SELECT * FROM geo_points WHERE loc >> '15.0,15.0';   -- right of
SELECT * FROM geo_points WHERE loc <^ '15.0,15.0';   -- below
SELECT * FROM geo_points WHERE loc >^ '15.0,15.0';   -- above
SELECT * FROM geo_points WHERE loc <@ '10.0,10.0,5.0'; -- within circle
```

**实际结果** ✅
- `<<` 返回 x < 15 的点（id 1, 2, 4）
- `>>` 返回 x > 15 的点（id 3）
- `<^` 返回 y < 15 的点（id 1, 2, 3）
- `>^` 返回 y > 15 的点（无）
- `<@` 返回圆内点（id 2，中心 (10,10) 半径 5）

---

### 3.13 CREATE TABLE with INET / CIDR type

**输入**
```sql
CREATE TABLE networks (id INT, name VARCHAR(50), ip INET, net CIDR);
```

**实际结果** ✅ `Table create succeeded`

---

### 3.14 INSERT and query INET/CIDR with operators

**输入**
```sql
INSERT INTO networks (id, name, ip, net) VALUES (1, 'host1', '192.168.1.1', '192.168.1.0/24');
INSERT INTO networks (id, name, ip, net) VALUES (2, 'host2', '192.168.2.1', '192.168.2.0/24');
INSERT INTO networks (id, name, ip, net) VALUES (3, 'host3', '10.0.0.1', '10.0.0.0/16');
-- subnet-of: MATCH rows 1,2
SELECT * FROM networks WHERE net << '192.168.0.0/16';
-- contains: MATCH row 1
SELECT * FROM networks WHERE net >> '192.168.1.1';
-- overlap: MATCH rows 1,2
SELECT * FROM networks WHERE net && '192.168.0.0/16';
-- equality
SELECT * FROM networks WHERE ip = '192.168.1.1';
```

**实际结果** ✅ 所有操作符返回正确结果：`<<` 子网判断、`>>` 包含判断、`&&` 重叠判断、`=` 精确匹配

---

## 4. DDL - 索引管理

### 4.1 CREATE INDEX (B+Tree)

**输入**
```sql
CREATE INDEX idx_name ON t1(name);
```

**实际结果** ✅ `Index created`

边界回归同时验证了 250 条跨叶唯一键、6000 条跨叶及内部节点重复键的精确查找，以及范围扫描不重复返回。

---

### 4.2 CREATE INDEX USING HASH

**输入**
```sql
CREATE INDEX idx_hash ON t1 USING HASH (id);
```

**实际结果** ✅ `Index created`

---

### 4.3 CREATE FULLTEXT INDEX

**输入**
```sql
CREATE FULLTEXT INDEX idx_ft ON t1(name);
```

**实际结果** ✅ `Fulltext index created`

该命令是项目扩展语法，不属于 PostgreSQL `CREATE INDEX ... USING ...` 标准路径。

---

### 4.4 CREATE GIN INDEX

**输入**
```sql
CREATE INDEX idx_gin ON t1 USING GIN (name);
```

**实际结果** ✅ 创建真实 GIN 文件索引并登记 typed catalog 元数据。

---

### 4.5 CREATE GiST INDEX

**输入**
```sql
CREATE INDEX idx_gist ON t1 USING GiST (id);
```

**实际结果** ✅ 通过 typed DDL 创建 GiST 风格物理索引；当前实现仍是简化 range/spatial 结构，不宣称 PostgreSQL 通用 GiST opclass 兼容。

---

### 4.6 CREATE BRIN INDEX

**输入**
```sql
CREATE INDEX idx_brin ON t1 USING BRIN (id);
```

**实际结果** ✅ `Index created`

---

### 4.7 CREATE SPGIST INDEX

**输入**
```sql
CREATE TABLE locations (id INT PRIMARY KEY, pos CHAR(20));
INSERT INTO locations VALUES (1, '0.0,0.0');
INSERT INTO locations VALUES (2, '10.0,10.0');
INSERT INTO locations VALUES (3, '20.0,20.0');
INSERT INTO locations VALUES (4, '-5.0,5.0');
CREATE INDEX idx_spg ON locations USING SPGIST (pos);
SELECT * FROM locations;
```

**实际结果** ✅ 四叉树空间索引创建成功，查询返回 4 行坐标数据

---

### 4.8 DROP INDEX

**输入**
```sql
DROP INDEX idx_name;
DROP INDEX idx_hash;
DROP INDEX idx_name, idx_hash CASCADE;
DROP INDEX IF EXISTS missing_index;
DROP INDEX idx_name ON t1; -- migration-only legacy suffix
```

**实际结果** ✅ 标准 name-only、多名称、`IF EXISTS`、`CASCADE/RESTRICT` 以及 B-tree/Hash/GIN/GiST/BRIN/SP-GiST 物理索引均通过；物理索引、`pg_class` 和依赖元数据同步清理。

---

## 5. DDL - 视图

### 5.1 CREATE VIEW

**输入**
```sql
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
```

**实际结果** ✅ `View created`

---

### 5.2 SELECT FROM VIEW

**输入**
```sql
SELECT * FROM v1;
```

**实际结果** ✅ 返回视图数据

---

### 5.3 DROP VIEW

**输入**
```sql
DROP VIEW v1;
```

**实际结果** ✅ `View dropped`

---

### 5.4 ALTER VIEW RENAME TO

**输入**
```sql
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
ALTER VIEW v1 RENAME TO v2;
SELECT * FROM v2;
```

**实际结果** ✅ `View renamed`，v2 可正常查询并返回数据

---

### 5.5 ALTER VIEW SET SCHEMA

**输入**
```sql
ALTER VIEW v2 SET SCHEMA other_db;
USE DATABASE other_db;
SELECT * FROM v2;
```

**实际结果** ✅ `View schema changed`
> 注：视图定义中的基表名未带数据库限定，若目标数据库中不存在同名基表，查询会报错。此为已知限制。

---

## 6. DDL - 触发器

### 6.1 CREATE TRIGGER

**输入**
```sql
CREATE TRIGGER trg1 AFTER INSERT ON t1 PRINT inserted;
```

**实际结果** ✅ `Trigger created`

---

### 6.2 DROP TRIGGER

**输入**
```sql
DROP TRIGGER trg1;
```

**实际结果** ✅ `Trigger dropped`

---

## 7. DML

### 7.1 INSERT INTO（显式列名）

**输入**
```sql
INSERT INTO t1 (id, name) VALUES (3, 'charlie');
```

**实际结果** ✅ `1 row(s) inserted`

---

### 7.2 INSERT INTO（省略列名）

**输入**
```sql
INSERT INTO t1 VALUES (4, 'dave', 28);
```

**实际结果** ✅ `1 row(s) inserted`

---

### 7.3 UPDATE

**输入**
```sql
UPDATE t1 SET name = 'alice2' WHERE id = 1;
```

**实际结果** ✅ `1 row(s) updated`

---

### 7.4 DELETE

**输入**
```sql
DELETE FROM t1 WHERE id = 3;
```

**实际结果** ✅ `1 row(s) deleted`

---

## 8. DQL

### 8.1 基本 SELECT

**输入**
```sql
SELECT * FROM t1;
SELECT id, name FROM t1 WHERE id = 1;
SELECT COUNT(*) FROM t1;
```

**实际结果** ✅ 均返回正确结果

---

### 8.2 JOIN

**输入**
```sql
CREATE TABLE t2 {fid:int pk, info:varchar(20)};
INSERT INTO t2 (fid, info) VALUES (1, 'info1');
SELECT * FROM t1 JOIN t2 ON t1.id = t2.fid;
```

**实际结果** ✅ 返回 JOIN 结果

---

### 8.3 UNION

**输入**
```sql
SELECT id FROM t1 UNION SELECT fid FROM t2;
```

**实际结果** ✅ 返回 UNION 结果

`tests/set_operation_volcano_test.cpp` 进一步验证了 Volcano `SetOperationOp` 的
UNION/INTERSECT/EXCEPT 及 ALL 多重集结果，并覆盖投影后的 DISTINCT 去重；协议 E2E
另验证了聚合 legacy producer 经 `MaterializedRowsOp` 进入同一集合节点。

---

### 8.4 GROUP BY + HAVING

**输入**
```sql
SELECT name, COUNT(*) FROM t1 GROUP BY name HAVING COUNT(*) > 0;
```

**实际结果** ✅ 返回分组结果

---

## 9. TCL - 事务控制

### 9.1 BEGIN / COMMIT

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (4, 'dave');
COMMIT;
```

**实际结果** ✅ `Transaction committed`

---

### 9.2 BEGIN / ROLLBACK

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (5, 'eve');
ROLLBACK;
SELECT * FROM t1 WHERE id = 5;
```

**实际结果** ✅ `Transaction rolled back`，后续 SELECT 无 id=5 行

---

### 9.3 SAVEPOINT

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (6, 'frank');
SAVEPOINT sp1;
INSERT INTO t1 (id, name) VALUES (7, 'grace');
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
```

**实际结果** ✅ id=6 保留，id=7 回滚

协议回归还验证 `BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY`、`START TRANSACTION READ COMMITTED READ WRITE`，以及无固定字符串偏移依赖的保存点创建和 `ROLLBACK TO SAVEPOINT`。

---

## 10. DCL - 权限控制

### 10.1 CREATE USER + GRANT

**输入**
```sql
CREATE USER testuser WITH PASSWORD 'testpass' NOSUPERUSER;
GRANT SELECT ON t1 TO testuser;
```

**实际结果** ✅ `User created`, `GRANT succeeded`

---

### 10.2 REVOKE

**输入**
```sql
REVOKE SELECT ON t1 FROM testuser;
```

**实际结果** ✅ `REVOKE succeeded`

---

### 10.3 SHOW USERS

**输入**
```sql
SHOW USERS;
```

**实际结果** ✅ 列出所有用户及其权限级别（admin 1，普通用户 0 等）

---

### 10.4 SHOW ROLES

**输入**
```sql
CREATE ROLE auditor;
SHOW ROLES;
GRANT auditor TO testuser;
SHOW ROLES;
```

**实际结果** ✅ 列出所有角色名称，仅显示角色不显示已授予用户

---

## 11. 工具与诊断

### 11.1 SHOW TABLES

**输入**
```sql
SHOW TABLES;
```

**实际结果** ✅ 列出当前数据库所有表

---

### 11.2 SHOW COLUMNS

**输入**
```sql
SHOW COLUMNS FROM t1;
```

**实际结果** ✅ 显示列定义

---

### 11.3 SHOW INDEXES

**输入**
```sql
SHOW INDEXES FROM t1;
```

**实际结果** ✅ 显示索引列表

---

### 11.4 DESC

**输入**
```sql
DESC t1;
```

**实际结果** ✅ 显示表结构

---

### 11.5 EXPLAIN

**输入**
```sql
EXPLAIN SELECT * FROM t1 WHERE id = 1;
```

**实际结果** ✅ 返回执行计划

---

### 11.6 ANALYZE TABLE

**输入**
```sql
ANALYZE TABLE t1;
```

**实际结果** ✅ `ANALYZE completed`

---

### 11.7 VACUUM

**输入**
```sql
VACUUM t1;
```

**实际结果** ✅ `VACUUM completed, 0 pages freed`

---

### 11.8 CHECKPOINT

**输入**
```sql
CHECKPOINT;
```

**实际结果** ✅ `CHECKPOINT completed`（checkpoint WAL、LSN 文件和关系页刷盘均成功）

---

## 12. 分区管理

### 12.1 CREATE TABLE ... PARTITION BY RANGE

**输入**
```sql
CREATE TABLE sales (id:int:0:1, amount:int:0) PARTITION BY RANGE(id) (
    PARTITION p_low VALUES LESS THAN (100),
    PARTITION p_high VALUES LESS THAN (200)
);
```

**实际结果** ✅ `Table create succeeded`

---

### 12.2 ATTACH PARTITION

**输入**
```sql
CREATE TABLE p_mid {id:int pk, amount:int:0};
ALTER TABLE sales ATTACH PARTITION p_mid FOR VALUES FROM (100) TO (150);
```

**实际结果** ✅ `Partition attached`

---

### 12.3 DETACH PARTITION

**输入**
```sql
ALTER TABLE sales DETACH PARTITION p_mid;
```

**实际结果** ✅ `Partition detached`

---

## 13. 高级特性

### 13.1 NOTIFY / LISTEN

**输入**
```sql
LISTEN mychannel;
NOTIFY mychannel, 'hello';
```

**实际结果** ✅ `LISTEN mychannel`, `NOTIFY mychannel`

---

### 13.2 行级安全 (RLS)

**输入**
```sql
CREATE TABLE users (id:int:0:1, name:varchar(50):0, role:varchar(20):0);
INSERT INTO users (id, name, role) VALUES (1, 'alice', 'admin');
INSERT INTO users (id, name, role) VALUES (2, 'bob', 'user');
CREATE POLICY p1 ON users FOR SELECT USING (role = 'admin');
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
```

**实际结果** ✅ `Policy created`, `RLS enabled`

运行时回归还验证了：`USING (owner = current_user)` 只返回当前用户行；显式 `TO public` 按 PUBLIC 生效；多个默认 permissive 策略按 OR 组合，`AS RESTRICTIVE` 按 AND 组合；`FOR ALL` 省略 `WITH CHECK` 时继承 `USING`，阻止越权写入；`NOINHERIT` 用户不匹配父角色策略；表 owner、`pg_authid.rolsuper`/`rolbypassrls` 绕过普通 RLS，`FORCE ROW LEVEL SECURITY` 重新执行策略；owner 同步持久化到正式 schema 和 `pg_class.relowner`，`ALTER TABLE ... OWNER TO` 会立即改变 RLS 行为。普通查询、删除以及 `UPDATE ... FROM` 的来源扫描均不再暴露其他用户行；启用 RLS 但没有适用策略时默认返回零行。策略解析/求值失败会安全拒绝该扫描。

---

## 14. 已知问题与限制

### 14.0 TABLESPACE 物理路由

`CREATE TABLE ... TABLESPACE`、`ALTER TABLE ... SET TABLESPACE` 已验证真实关系文件迁移：
heap、FSM/VM、索引、分区和 TOAST 文件位于 `<LOCATION>/<DATABASE>/`，目标目录不存在或
表空间 marker 缺失时拒绝操作，不会回退到默认目录。测试还使用全新 `StorageEngine`
重启读取迁移后的行，覆盖了此前“重启打开空文件”的数据损失风险。

剩余限制：表空间权限/owner、`ALTER TABLESPACE` 完整语义和 PostgreSQL OID/符号链接布局。

### 14.1 ALTER TABLE SET SCHEMA

**当前行为**：命令已由 AST bridge 转发到现有存储实现；当前实现的目标参数仍按数据库级迁移处理，不等价于 PostgreSQL 的 namespace-only 语义。

**级别**：部分实现（语义与 catalog namespace 迁移仍待补齐）

---

## 15. 结论

本次测试覆盖的历史手册场景、137 个独立 C++ 回归测试、PostgreSQL 协议 E2E 和窗口函数 E2E 均通过；分组 Volcano 单元与主 SQL 手工回归也通过。系统在以下方面表现稳定：

- ✅ 基本 CRUD（CREATE/INSERT/SELECT/UPDATE/DELETE/DROP）
- ✅ **POINT 数据类型**与空间运算符（`<<` / `>>` / `<^` / `>^` / `<@`）
- ✅ **INET/CIDR 网络类型**与网络运算符（`<<` / `>>` / `&&`）
- ✅ 索引系统基础路径（B+Tree/Hash/FullText/GIN/BRIN/SP-GiST）；GiST 仍列为缺失
- ✅ 视图与触发器（含 ALTER VIEW RENAME TO / SET SCHEMA，以及简单单表视图多行 `INSTEAD OF` INSERT/UPDATE/DELETE 协议回归）
- ✅ 事务控制（BEGIN/COMMIT/ROLLBACK/SAVEPOINT）
- ✅ 权限管理（GRANT/REVOKE）
- ✅ 分区管理（Range/List/Hash + ATTACH/DETACH）
- ✅ 查询能力（JOIN/UNION/INTERSECT/EXCEPT 含 ALL 语义、基础 GROUP BY/窗口函数/CTE/LATERAL、未关联单列 IN/NOT IN、未关联单表 EXISTS/NOT EXISTS、单列未关联 ANY/ALL、单个未关联标量目标）
- ✅ Volcano 并行 heap scan（page-range workers、确定性 Gather、串行结果等价性）
- ✅ 工具命令（SHOW/EXPLAIN/ANALYZE/VACUUM/CHECKPOINT）
- ✅ INSERT 省略列名 / ALTER RENAME / CREATE TABLE AS SELECT

**未发现阻塞性 bug**。

---

*报告生成时间：2026-08-14*
*测试执行人：自动化测试脚本 + 人工验证*
- Parser 边界回归：`parser_phase1_test` 新增非法 `LIMIT/OFFSET/FETCH`、负数、缺失计数以及省略 FETCH count 的测试；非法计数 fail-closed，省略 count 默认 1。
- Parser 数值选项回归：同一测试新增非法函数 `COST/ROWS`、角色 `CONNECTION LIMIT` 和列统计目标测试，确认解析失败不会产生默认 AST。
- Sequence DDL 边界回归：`sequence_full_test` 新增非法 `CREATE SEQUENCE START`、`ALTER SEQUENCE INCREMENT/CACHE` 测试，确认不会创建或改变序列，合法序列、缓存、重命名、依赖和重启行为保持通过。
- DDL 清理回归：`ddl_bridge_routing_test` 验证 `ALTER ROLE/ALTER USER` 继续由 typed bridge 执行；删除 main.cpp 中不可达的重复角色执行器后路径保持一致。
