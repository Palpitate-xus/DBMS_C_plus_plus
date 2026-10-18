# 代码审查索引

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
