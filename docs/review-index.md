# 代码审查索引

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

最新开放记录：[R173类型/函数签名依赖](issue-review-r173-routine-signature-type-dependencies.md)
当前分支跳过，候选未安装/编译/验收，旧版超时与不完整原生反证原样保留；
[R176普通聚合排序键](issue-review-r176-ordinary-aggregate-order-keys.md)已完成PG/旧版各101次健康采集，
确认缺失键未分析、排序键未求值、特定需求被拒和ASC浮点结果差异；尚无生产修复。
两份报告只增加发现/方法文档，不增加测试注册或提升原273状态；以下五项已提交修复仍有效。

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

| 问题与修改方法 | 本地提交 | 接受范围 |
| --- | --- | --- |
| [R170](issue-review-r170-quoted-unqualified-scalar-projection.md) | `42ab8685` | quoted标量投影身份 |
| [R171](issue-review-r171-quoted-scalar-protocol-analysis.md) | `6a1c0642` | Parse发布前及Describe纯绑定 |
| [R172](issue-review-r172-qualified-column-scalar-argument.md) | `95e04648` | abs参数的真实列身份 |
| [R174](issue-review-r174-ordinary-source-descriptor.md) | `1e116fb4` | computed来源声明及typmod |
| [R175](issue-review-r175-ordinary-aggregate-identity.md) | `3d13fab2` | SUM/COUNT入口及decoded列身份 |

## 历史记录（以下状态与数量均指记录当时）

[R175普通聚合](issue-review-r175-ordinary-aggregate-identity.md)记录无FROM入口、SQL列身份及DISTINCT正确owner。
组合V1 clean完整791仍26失败（R174八项metadata、R175十八项执行），314全部通过；
两端编译均完成但不宣称完整62入口通过，原失败断言保留，两个新专项仍待PG参考/产品验收。
本次仅登记开放问题：已接受594MD、916native+559Python+3Main=1478；
Root597MD、922+567+3=1492，原273仍22/166/70/15，总目标未完成；不push、Actions禁用。

[R174普通来源描述符](issue-review-r174-ordinary-source-descriptor.md)另记四种既有791形态的独立metadata缺口。
不移除原791/314断言，不假定三候选即可全通过；生产编译/链接完成与Main回归编译/完整运行分开记录。
本次仅登记开放问题：已接受593MD、916native+559Python+3Main=1478；
Root596MD、922+567+3=1492，14个尚未接受入口中含本轮2wire和原12foreign。
原273仍22/166/70/15，总目标未完成；不push，Actions禁用。

已复现的三个独立开放问题：见[R170 quoted投影](issue-review-r170-quoted-unqualified-scalar-projection.md)、[R171 Parse/Describe](issue-review-r171-quoted-scalar-protocol-analysis.md)、[R172列参数引用](issue-review-r172-qualified-column-scalar-argument.md)。
V2和V3各自实际完成PG18.6完整791/0、冻结accepted-only旧产品791/343，外层退出0/1；
343含重复与连带失败，不是343独立bug；新源码/夹具尚未安装、编译或验收。
V1 Describe缺少sourceSpan的静态否决和首次V3启动路径错误均保留，不削减原791。
本次仅登记报告与索引：592MD，916native+559已注册Python+三个Main=1478；
Root595MD、1490入口中的原12个外来入口仍未接受，源文件与注册不变。
原273仍22/166/70/15，总目标未完成；不恢复用户暂缓模块，不push，Actions禁用。

九项有限修复已逐项本地提交；问题/修改方法/专项数量见[R169总表](issue-review-r169-simple-query-raw-syntax.md)。

| 项目 | 本地提交 | 范围 |
| --- | --- | --- |
| R158 | 7648c6ea | 角度数值边界 |
| R161 | 24f10c9d | 限定投影身份 |
| R162 | d27b0af7 | 通用存储签名 |
| R163 | 45a81c0e | Parse签名校验 |
| R164 | 084a171e | 自定义标量声明 |
| R166 | 8461466c | 整式调用参数OID |
| R167 | e6fdb953 | 单目标域删除 |
| R168 | e39636bf | 复合类型owner |
| R169 | c580c8c0 | 有限原始语法预检 |

工作区与accepted-only clean各58完整入口实际全0；接受源码/夹具24路径与实际测试clean逐字节对应。
589MD、916native+559已注册Python+三个Main=1478；Root1490的原12个外来入口仍未接受。
这不是1478全量运行或各中间提交独立全矩阵；当前手册/状态入口同步有限边界，旧失败记录原样保留。
原273仍22/166/70/15，总目标未完成，不恢复用户暂缓模块、不push、Actions保持禁用。

本地 R169 有限修复：见[issue-review-r169-simple-query-raw-syntax](issue-review-r169-simple-query-raw-syntax.md)。专属 native 106；专属 wire 104。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 589 Markdown、916 native、559 Python、3 Main，共 1478 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

本地 R168 有限修复：见[issue-review-r168-composite-namespace-owner](issue-review-r168-composite-namespace-owner.md)。专属 native 207；专属 wire 332。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 589 Markdown、915 native、558 Python、3 Main，共 1476 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

本地 R167 有限修复：见[issue-review-r167-domain-drop-targets](issue-review-r167-domain-drop-targets.md)。专属 native 189；专属 wire 663。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 589 Markdown、914 native、557 Python、3 Main，共 1474 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

本地 R166 有限修复：见[issue-review-r166-scalar-protocol-parameter-inference](issue-review-r166-scalar-protocol-parameter-inference.md)。未新增专属 native fixture；专属 wire 263。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 589 Markdown、913 native、556 Python、3 Main，共 1472 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

本地 R164 有限修复：见[issue-review-r164-routine-custom-type-declarations](issue-review-r164-routine-custom-type-declarations.md)。专属 native 570；专属 wire 956。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 589 Markdown、913 native、555 Python、3 Main，共 1471 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

本地 R163 有限修复：见[issue-review-r163-qualified-parse-validation](issue-review-r163-qualified-parse-validation.md)。未新增专属 native fixture；专属 wire 1017。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 589 Markdown、912 native、554 Python、3 Main，共 1469 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

本地 R162 有限修复：见[issue-review-r162-generic-stored-signatures](issue-review-r162-generic-stored-signatures.md)。专属 native 343；专属 wire 1705。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 589 Markdown、912 native、553 Python、3 Main，共 1468 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

本地 R161-V5 有限修复：见[issue-review-r161-qualified-projection-routine-identity](issue-review-r161-qualified-projection-routine-identity.md)。未新增专属 native fixture；专属 wire 637。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 589 Markdown、911 native、552 Python、3 Main，共 1466 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

本地 R158 有限修复：见[issue-review-r158-angle-conversion-range](issue-review-r158-angle-conversion-range.md)。专属 native 603；专属 wire 1449。本次证据为九项候选组合的 Root/accepted-only clean 两端各 58 项（26 native、29 wire、3 real Main）实际退出全 0，完整源文件与产物 fence 复核 0；不表示本项中间提交单独跑过全矩阵。本项接受后 588 Markdown、911 native、551 Python、3 Main，共 1465 注册入口；不是该总数的全套运行声明。原 273 项 22 complete / 166 partial / 70 unverified / 15 deferred 不变，整体未完成；保留历史失败与未接受边界，不恢复用户跳过模块。No push；Actions disabled。

[R169 多语句语法检查](issue-review-r169-simple-query-raw-syntax.md)登记实际开放问题：
PG完整102/0、冻结旧Root102/27失败；原garbage两例DDL会回滚，但新增被遗漏的字符串等尾片段
会仅返回C/Z并实际删除私有UUID域，另有数字XX000、前缀nextval执行与事务状态差异。
有限候选只证明词法、不可能的命令头和严格DROP DOMAIN语法；不提前分析其他已知命令。
保留全部原控制并补充执行期42601和七类项目维护命令不误拦控制，native104/wire102待验收。
干净版本正以明确已接受base+owned hunks构建；未提交本项源码或夹具，无新产品通过声明。
已跟踪587MD、910+550+3=1463，Root592MD、921+564+3=1488，仍25入口未接受；
原273仍22/166/70/15，总清单未完成，不push、Actions禁用。

[R159 编码边界](issue-review-r159-encoding-boundaries.md)完成三根因有限修复：
hex仅允许完整byte pair之间的四类空白；escape保留非零ASCII控制符和DEL；
base64完整三字节组每76列加LF，保留padding和原解码策略。
PG完整806/0，冻结旧版本806/164失败；独立accepted-base-only版本和Root各11入口全0，
包括完整native676、wire806、既有编码/相邻协议及三个真实Main。
独立版本fresh1Evaluator O2+逐一核对57生产对象；Root正常3CPP O2/repeat0/current58；
两端native新编译、三个真实Main核对源/全头/flags/字节后复用TU并重新链接当前57库，
不称Main本轮fresh TU，也不把有限回归当成整库全量/TLS/sanitizer通过。
按hex、escape、base64分别提交，完整共享676/806夹具在最后一个提交加入；
不宣称两个中间提交单独通过完整矩阵。三项类型目录线索仍未核实，R156不恢复。
已跟踪586MD、910native+550Python+3Main=1463；Root591MD、921+564+3=1488，
仍25个入口未接受，原273仍22/166/70/15，总清单未完成，不push、Actions禁用。

[R167](issue-review-r167-domain-drop-targets.md)/[R168](issue-review-r168-composite-namespace-owner.md)补充实际构建和运行检查点：
Root genuine fresh58生产O2/repeat0，六真实Main TU均fresh成功；23项native中22入口通过，
仅R167固定189有一个空quoted目标Parse失败。本项207及原R164完整956全0，52整轮仍运行。
V2 clean因新NOTICE调用依赖未接受接口而在DDL编译失败，原树/日志不改；
新V3 clean以既有输出接口及空目标修正，真实fresh22 O2+核对36个本轮新共享头V2对象，
组成当前58，不使用旧ABI或冒称V3独自fresh58。23native及三真实V2 Main核对后新链57库，
尚未运行V3整轮。包装器首次签名检查错误及后续改正构建证据分别保留。
本次只提交两个报告和索引，没有验收候选源码；已跟踪586MD、909+549+3=1461，
Root591MD、920+563+3=1486，仍25入口未接受；原273仍22/166/70/15，不push、Actions禁用。

[R159 编码边界](issue-review-r159-encoding-boundaries.md)和
[R160 角度常量规划时机](issue-review-r160-angle-constant-demand.md)补入已跟踪的开放问题记录。
三端原20条发现记录完整保留：四个编码差异、十个常量错误时机差异分别列根因和修改方法；
其余类型目录线索仍仅为静态待核实项。没有将发现记录当成通过，也不恢复R156暂缓模块。
本次只提交两份问题报告和文档索引，无新源码验收；已跟踪586MD，
909native+549Python+3Main=1461，原273仍22/166/70/15，不push、Actions禁用。

[R165 五种内置数组binary输入](issue-review-r165-builtin-array-binary-bind.md)独立有限验收：
PG、Root、组合clean及accepted-base-only本项clean均完整1852/0；
独立clean六协议+三真实Main入口共9项全0。只接收既有int2/int4/int8/text/varchar数组身份，
其他数组和用户暂缓模块不扩展。独立clean实际fresh1 Network O2+57已逐一核对生产对象；
Main输入/全头flags/原始对象核对后复用真实TU，新链接当前57库并隔离运行，不冒fresh TU。
首次Main链接检查错用聚合hash而在链接前失败，修正后的实际链接/运行通过，原日志保留。
组合V6 Root/clean各29入口仍1，仅R164完整956的DROP DOMAIN/SCHEMA四项清理失败；
[R167](issue-review-r167-domain-drop-targets.md)/[R168](issue-review-r168-composite-namespace-owner.md)继续开放。
本次只提交R165一个源码hunk、注册、完整1852夹具及文档，其他生产候选仍未提交。
已跟踪584MD、909native+549Python+3Main=1461；Root591MD、918+561+3=1482，
尚有21个未接受入口。无新1461全量/TLS/整库sanitizer；原273仍22/166/70/15，
总清单未完成，不push，Actions禁用。

[R167 DROP DOMAIN目标](issue-review-r167-domain-drop-targets.md)、
[R168 composite namespace归属](issue-review-r168-composite-namespace-owner.md)登记为新开放问题。
R164完整956的Root候选实际四失败：一个DROP DOMAIN及三个DROP SCHEMA；整轮29终态1保留。
内置数组1852、whole-scalar参数263、通用签名1705、准备校验1017及限定投影637已在Root全0，
不能替代R164清理控制；clean整轮仍运行，尚未验收或提交这些生产候选。
本次只提交两个新问题报告与索引，原273及所有用户暂缓状态不变，不push、Actions禁用。

新增三项已复现的开放问题：[R164 自定义标量函数声明](issue-review-r164-routine-custom-type-declarations.md)、
[R165 内置数组binary Bind](issue-review-r165-builtin-array-binary-bind.md)、
[R166 函数wire参数OID推断](issue-review-r166-scalar-protocol-parameter-inference.md)。
PG完整R162协议1705全0；旧Root/clean各515失败，旧native343/154失败。
组合候选Root/clean的native343各8失败、协议1705各77失败；R163完整1017各2失败。
77项包括六个未限定调用Parse时机、44个数组binary输入、27个OID0推断/后续binary输入。
R163新24个GROUP BY别名正向控制均保留并通过PG；两项剩余失败为aborted Parse发布。
R161的637、R158的603/1449及六个重新链接Main运行在两端均0，不代表整个候选已通过。
Root完整25入口终态1，其中还含两个驱动使用不存在邻居文件的exit2；clean改正驱动后25终态仍1。
首轮全部失败和原始343/1705/1017夹具已归档，不改写为成功。
本次只提交三个开放问题报告与索引；无生产源码/新夹具验收，不push，Actions禁用。
原273仍22/166/70/15，总清单未完成，所有用户暂缓模块保持暂缓。

[R156 类型化常量](issue-review-r156-regtype-physical-constants.md)按用户要求暂缓，不恢复。
R158 数值与 R161 限定调用仍为未验收候选；完整失败断言和历史版本均保留。
新复现的 [R162 通用存储函数签名](issue-review-r162-generic-stored-signatures.md)、
[R163 协议准备校验](issue-review-r163-qualified-parse-validation.md)分别记录根因与修改方法。
R161 V5 两端完整637各18失败（九个执行签名、九个元数据），PG同一637全0；
Root 的 R158 完整1449现已全0，但独立验收与依赖修复尚未完成，不能宣称总清单完成。
本次只提交两个开放问题报告与索引；不提交候选源码/夹具，不push，Actions禁用。

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

[R146主键完整值/前缀碰撞](issue-review-r146-primary-key-full-value.md)按用户“skip it”暂缓。
实际PG1347全过；冻结Root和已接受组合各1365完整控制/696断言失败，原日志保留。
本项未实施生产源码修改，协议草稿未注册、未提交，不计已修复；普通继续不重开。
本次只提交暂缓记录，测试仍903native+542Python+3Main=1448，已接受MD568，
工作区MD571另含未接受草稿。R137等暂缓仍有效，原273状态不变，不push、Actions禁用。

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

用户要求跳过R144应用名规范化，保留已记录问题和后续修改方法；本次仅记录暂缓，
没有应用名实现、专项测试或验收变更。R145路径列表语法随后独立完成有限修复，
原273状态不变；本次R144暂缓记录不宣称其已修复。

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

最新实施状态见[逐项修复进度](review-fixes-2026-10-13.md)，包含各项commit及回归。下方审查结果保留其原始输入和失败证据。

| 审查 | 输入 | 内容 |
| --- | --- | --- |
| [R135 RETURNING时区](issue-review-r135-timestamptz-returning-timezone.md) | 结构化TSTZ行像未呈现 | PG134/两个旧程序73失败；按每语句/日期的协议呈现，正常1/repeat0、45完整入口全0，微秒heap普通未修复另记 |
| [R134 原生回调归属](issue-review-r134-core-native-callback-ownership.md) | 干净ecf四组名字被替换 | 旧1320/922失败；真实callback marker、caller声明和原始参数，正常1/repeat0、37完整入口全0，R132 TU核对后重连执行，不冒新1437全量 |
| [R133 timestamp binary微秒](issue-review-r133-timestamp-binary-microseconds.md) | R132后的冻结接收器 | PG266/旧204失败；整数向下拆分及UTC标记，正常1/repeat0、31完整入口全0，R132 Main TU核对复用而非重编，旧heap/全日期域仍开放 |
| [R132 TimeZone TO](issue-review-r132-timezone-set-to.md) | 通用SET分隔符被Main当成值 | PG112/旧44失败；消费独立TO且保留引号数据，正常1/repeat0、fresh三Main及30完整入口全0，GUC其余范围仍开放 |
| [R131 TIMESTAMPTZ binary时区](issue-review-r131-timestamptz-binary-timezone.md) | `e782cda1` | PG158/干净与冻结旧版各45失败；仅标明UTC，29完整入口全0，非fractional receiver或heap升级 |
| [R130 原生结果声明](issue-review-r130-native-result-hint-precedence.md) | `3c8c85d3` | 旧705/1/smallint和168/53失败；声明/回调所有权修复，35完整入口全0，旧全量红不抹除 |
| [R129 原生数组声明](issue-review-r129-native-array-projection-types.md) | `c02aa584` | 真实数组旧原生仍134，PG/旧SQL51均0；四处提示及夹具修复，26完整入口全0，不是跳过的R109 |
| [R128 TIME二进制参数](issue-review-r128-time-binary-parameter.md) | 接收器整秒/24时限制及错误状态 | PG158全过/旧完整132失败；修复微秒与24时接收，08P01/22008/22P03和错误后恢复，最终55入口全0，不迁移整秒heap或冒其他receiver完成 |
| [R127 直接TIME过高精度](issue-review-r127-time-excess-precision.md) | 有效6与声明一次warning | PG276/旧246失败、252原生旧225失败；最终54入口全0，原122核心值/metadata/通知匹配，PG内部诊断来源不冒充；DDL/其他表达式及完整资格另项 |
| [R126 TIME精度关键字语法](issue-review-r126-time-precision-grammar.md) | 无符号int32 token与结构化42601 | PG326/旧296失败、严格549原生旧217失败；首版DDL Parse重置已修，最终52入口全0、原122仍2条TIME(7)通知/typmod差异 |
| [R125 合法TIME显式精度](issue-review-r125-time-precision-value.md) | 基本clock两阶段舍入与真实AST声明 | PG830/旧830失败、339原生旧317失败；最终50入口全0、原97全匹配/122仍12边界差异，不等于grammar/warning/完整heap完成 |
| [R123 时间列precision持久化](issue-review-r123-temporal-column-precision.md) | 四类型及数组的schema/目录/物理投影 | 旧782原生218失败、PG104全过/旧94失败；首版六个42703由R124另项，冷热加载/格式与有限终态见记录，不等于CAST舍入或heap升级 |
| [R124 TIMETZ后缀表投影](issue-review-r124-timetz-postfix-projection.md) | NULL转换被误判列名42703 | 实际PG96全过、旧24失败；仅补既有投影路由的TIMETZ，有限验收与独立版本见记录，不等于完整TIMETZ codec/grammar |
| [R122 显式TIME精度复现与计划](issue-review-r122-time-explicit-precision.md) | 122个实际参考/当前观察 | 仅保存缺陷与修改方法，尚未实现；值/描述符、schema元数据、声明边界与warning分项，采集0不作通过或完成证明 |
| [R121 TIME基本文本输入](issue-review-r121-time-input-normalization.md) | 非法值/超微秒透传、真实text Bind | PG177/旧130失败、90低层；43完整入口及原32对照全过，97仍21显式精度差异。基本clock修复不等于完整TIME grammar/cast/旧heap升级 |
| [R119 时间戳微秒binary](issue-review-r119-timestamp-binary-result.md) | 固定错误epoch字节、时区与真实数组 | PG232全过、旧144失败/235低层；首版8个跨日输入失败由R120独立修复，最终共享41入口及64对照终态见记录，不升级秒heap或所有日期域 |
| [R120 闰年日期逆换算](issue-review-r120-date-cycle-inverse.md) | 0004年起的非法月份及时间戳跨日 | 逐日独立枚举3652059天、PG152/旧48失败；共享头重编、有限回归及真实Main终态见记录，R119发送器保持独立版本 |
| [R117 TIME微秒binary结果](issue-review-r117-time-binary-result.md) | 真正8字节时钟、24时与数组 | PG144全过、旧114失败；121低层控制、完整有限终态与版本见记录，不把sender修复等同输入/文本/旧heap升级 |
| [R118 时态结果物化布局](issue-review-r118-temporal-view-materialization.md) | DATE/TIME/TIMETZ/TIMESTAMP/TIMESTAMPTZ的真实heap工厂 | PG186全过、旧60失败、首版仍36失败均保留；匹配产物有限验收与版本以记录真实终态为准，不等于完整时态精度/通用view rewrite |
| [R116 非物化视图完成tag](issue-review-r116-view-command-tags.md) | TEMP/TEMPORARY/OR REPLACE与注释 | 实际PG54零失败、旧14tag失败；只按真实语法对象修命令标签，完整有限门禁及独立版本见记录，物化/CTAS不重开 |
| [R113 INTERVAL二进制结果](issue-review-r113-interval-binary-result.md) | 16字节时钟/天/月与真实数组封套 | PG171零失败、依赖修复后旧149失败；117低层控制与有限匹配验收见记录，不等同binary Bind或其他codec完成 |
| [R114 物理字段逻辑长度](issue-review-r114-physical-attribute-length.md) | DDL属性、冷暖目录与协议长度 | PG28零失败、旧22失败；102原生控制；R115先独立提交，其他目录/视图失败及方法保留，非完整注册验收 |
| [R115 标量别名投影metadata](issue-review-r115-scalar-alias-projection-metadata.md) | 含数组表的星号展开与真实列身份 | 实际参考228零失败、固定旧13失败；只修规范类型拼写与typmod，有限回归/独立版本见记录，R114另项 |
| [R110 数组二进制结果](issue-review-r110-array-binary-result.md) | 类型 OID、维度/下界与元素 codec | 实际参考110零失败、旧75失败；182低层控制、有限回归与独立版本见记录，不等同数组二进制输入或所有元素 codec 完成 |
| [R112 无 typmod 具名数组输入](issue-review-r112-unconstrained-named-array-input.md) | BPCHAR/BIT 真实声明与 wire Bind | 实际180006参考57零失败、旧44失败；逐版修复、有限回归和版本范围见记录，不关闭原TYPE-16或原273 |
| [R108 ARRAY_POSITION元素相等](issue-review-r108-array-position-typed-equality.md) | `95be64fb`后对照与逐版修复 | 第五版354/258、原208/80/array库/654及32native/29协议/3真实Main共64完整入口均0；28补充JSONB及核心/最终fence0。旧258/158、320/183及各版失败保留，独立本地commit，不冒新全量 |
| [R107 长度函数签名](issue-review-r107-string-length-signatures.md) | `25925345`后四名11条声明 | PG/Root654、607原生及24native/18协议/3真实Main均0；实际2/1/1增量normal/repeat、核心/最终独立核对0，单项本地commit。旧654/392、607/440和首两版失败保留，不冒新全量，R106暂缓 |
| [R106 COALESCE显式CAST输入分析](issue-review-r106-coalesce-explicit-input-analysis.md) | `2556a3d8`后只读复查 | 用户2026-10-16明确跳过，未修复；四条观察及未注册612控制（参考零失败/旧168失败）保留，无生产改动，不扩R104证明或重开专项 |
| [R104 网络实际值转换](issue-review-r104-network-value-conversions.md) | `b6595d2e`后五CPP增量修复 | PG/Root649、原738及完整SDK、21native/17协议/3真实Main通过；4/3/2增量normal/repeat与核心/最终核对0，单项提交。旧649/419、SDK134及首两版失败保留，不冒新全量 |
| [R105 限定NAME→INET/CIDR](issue-review-r105-network-name-input.md) | `fb131c02`后精准NAME源身份 | PG/Root738、完整SDK及21native/16协议/3真实Main全过，自身真实全58/normal/repeat/核心及最终核对0，单项提交；旧738/34与SDK134保留，R104后续独立关闭 |
| [R103 NAME→MAC显式输入](issue-review-r103-macaddr-name-input.md) | `e1f8cf4a`后精准NAME源身份 | 最终PG/Root366、完整SDK及21native/16协议/3真实Main通过；最终自身全58/repeat/fence0，单项提交；旧366/30、SDK134及首版失败保留，R104/R105后续独立关闭 |
| [R102 INET/CIDR输入](issue-review-r102-network-input-notation.md) | `7533f27e`后共享codec与真实源类型预检查 | 旧188/72、672/389；当前188/纯组件sanitizer、PG/Root672、2558对照及21native/16协议/3真实Main通过；最终全58/匹配核心/fence全0，单项提交，首版672/23保留；R103/R104/R105后续各独立修复 |
| [R101 MAC输入与宽度](issue-review-r101-macaddr-input-and-widths.md) | `3d2e49f6`后共享codec、真实CAST/赋值/COALESCE/VALUES | 旧75/38、328/186；当前75、PG/Root328、236差分、20native/15协议/3真实Main通过；头修正后全58重编及产物相同证明、新核心2/328/fence全0，单项提交不混入R98/R99，不冒新全量 |
| [R100几何坐标分隔符](issue-review-r100-geometric-coordinate-delimiters.md) | `0c7b86c3`后独立GeometryValue.cpp修复 | 旧232/104、230/135失败；当前232、PG/Root230、十个完整native/七个完整协议通过；不混入R98/R99待确认候选提交 |
| [R97 BEFORE行赋值的typed NEW/OLD](issue-review-r97-typed-before-row-assignments.md) | `20db4d58`后本工作区TableManage自有O2重编 | 旧106/65失败，当前154、15个完整native/十个完整协议通过；新1392未全量，不扩完整PG触发器兼容 |
| [R96 collation退出期专项暂缓](issue-review-r96-collation-exit-user-deferred.md) | `5091b39c`纯组件观察 | 用户明确跳过；原ASan失败保留，未修复，不重开本专项，也不扩为全部collation/类型暂缓 |
| [冻结5091b39c的完整注册回归](issue-full-registered-5091b39c.md) | `5091b39c`，源码/产物全程不变 | 1391入口各一次，1390实际通过/1 TLS环境skip/0失败；全部58 receipt及57 test库字节独立终态核对0，不冒真实TLS或原273完成 |
| [旧全局BOOL参数与混合JSON](issue-review-r95-legacy-global-boolean-arguments.md) | `ccaf0ac5`后本工作区TableManage自有O2重编 | 原118/56和观察60/15失败；当前118、PG/Root60、十个完整native/十个完整协议通过，原setup超时/C warning保留，不冒全1391或原273完成 |
| [并行配置SHOW与Describe](issue-review-r94-show-parallel-workers.md) | R93后本工作区Main/Network/parser自有O2重编 | PG/Root40、原完整152、六个完整native及十个完整协议通过；不冒全1389或原273完成 |
| [布尔聚合参数与分组结果](issue-review-r93-boolean-aggregate-arguments.md) | R92后本工作区全部58单元自有O2重编 | 408+68、21完整native及18完整协议邻居通过；新协议150/152、SHOW两项独立失败保留，不冒全1387或原273完成 |
| [缺失TOAST计数器的数据保持](issue-review-r92-toast-missing-counter.md) | `4a4c7254` | 原写入失败破坏已提交长值；本范围修复，30完整native/18of20协议通过，两处普通DDL超时保留 |
| [冻结332bcff4的完整注册回归](issue-full-registered-332bcff4.md) | `332bcff4`，源码/产物全程不变 | 1382入口各一次，1379实际通过/1 TLS环境skip/2失败；全部58 receipt及57 test库字节独立核对一致，失败未抹除 |
| [冻结27bac6ee的完整注册回归](issue-full-registered-27bac6ee.md) | `27bac6ee`，源码/产物全程不变 | 1368入口，1362个PASSED退出（含1个TLS环境skip）、6个FAILED；无漏跑/重复，全部58 receipt及独立终态校验一致，失败与后续方法逐项保留 |
| [冻结d974c80d的完整注册回归](issue-full-registered-d974c80d.md) | `d974c80d`，源码/产物全程不变 | 1362个入口，1359个PASSED退出（含1个TLS环境skip）、3个FAILED，无漏跑/重复；原R79/R80失败入口通过，清理/准备/回滚失败保留 |
| [冻结b5def9ba的完整注册回归](issue-full-registered-b5def9ba.md) | `b5def9ba`，源码/产物全程不变 | 1360个入口，1358个PASSED退出（含1个TLS环境skip）、2个FAILED，R79/R80逐项修复另验 |
| [冻结18d85f4e的完整注册回归](issue-full-registered-18d85f4e.md) | `18d85f4e`，源码/产物全程不变 | 1339个入口，1319个PASSED退出（含1个TLS环境skip）、20个FAILED，全部失败和后续方法保留 |
| [2026-10-13 全仓库审查](code-review-2026-10-13.md) | `a502ffa3`，产品源码末次提交 `2dc9554e` | 模块审查、实际 SQL 差分、已知失败、测试注册、文档检查和逐项修复方法 |

问题分为实际复现、源码确认、历史失败待重验和待验证推断；修复建议不代表实现已经修改。
完成状态仍以[原总清单](postgresql-18-gap-audit.md)和[机器状态](gap-progress.json)为准。
全部文档的检查范围见[文档检查清单](documentation-inventory.md)。
