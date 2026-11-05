# DBMS_C_plus_plus 对标 PostgreSQL 18.6：逐项完整实施蓝图

<!-- CURRENT_LITERAL_LIKE_SCAN_20261105 -->

## LIKE前缀的真实索引读取与密集前缀成本（2026-11-05，P17-C6）

普通无参数SELECT中的TEXT/VARCHAR字面量LIKE现在可实际使用已有GiST侧索引。已绑定AST提供真实关系/列身份；前缀范围只筛选候选，原类型化LIKE、AND剩余条件和NULL语义继续复查。转义百分号/下划线、多字节ESCAPE、UTF8上界以及打开前删除索引均有回归。可用ANALYZE的MCV与完整覆盖直方图桶识别密集前缀，按真实表extent和成本参数保留堆扫描；未知探测至少计1行成本。

同一4096行×3列、父代／最终C6／同一父代回测各3进程交错运行；wall中位数变化（正数为减少，相对首轮／回测）：热点前缀（3200行）6.88%／-3.83%；低频前缀（1行）68.01%／67.77%；不存在前缀（0行）69.40%／68.72%；显式ESCAPE前缀（1行）71.84%／71.06%。仅代表本负载；C3热点超过3倍耗时的失败尝试完整保留。

最终C6通过新原生310检查、原13068与独立冷读105检查、37个相关原生入口；纯前缀组件O2和ASan/UBSan各72567检查，启用泄漏检测。新123条协议命令含27次实际扫描断言；32个相关旧协议入口29正常0、3项严格失败与父代完整观察相同。真实PG18.6以两个会话重放共享65条SQL，935个完整字段/NULL/OID逐项相同，3个EXPLAIN ANALYZE实际行数相同；未比较PG的AM路径、成本或速度。

详见[性能报告](performance-report-2026-10-30.md)与[压缩原始证据](benchmarks/like-prefix-scan-20261105.json.gz)。本批仍为partial，Goal active。


<!-- CURRENT_ORDINARY_QUERY_PROFILE_20261105 -->

## 普通查询的规划元数据与解析开销（2026-11-05，P16-C2）

本批在一次普通查询规划内复用关系schema，并按需各读取一次B-tree/Hash/Bloom/GiST列清单；主键与不可能使用位图/GiST的条件省去无用发现。普通表达式先排除不可能的类型常量前缀，避免对数字、表达式分隔符前的列名做推测类型解析及异常处理。真正的限定/引号/修饰/复合类型常量与显式CAST仍使用原声明语法。

交错的8倍调用补测，父代／最终C2／同一父代回测各3进程；wall中位数变化（正数为减少，相对首轮／回测）为：热点等值（3200行）12.15%／-1.12%；低频等值（1行）15.17%／18.32%；不存在等值（0行）17.94%／14.60%；主键等值（1行）25.52%／19.68%。全部短时及补测的正负结果保留，不能推广为全部操作或PG速度。

最终C2通过原生13068与独立冷读105检查、32个规划/存储入口和14个语法入口；另2个冷目录原生入口保留父代完全相同的严格失败。新65条协议命令含10个实际扫描断言，原有73条等值协议通过；另30个相关旧协议入口27通过、3个严格失败与父代完整观察相同。实际PG18.6的共享25条SQL仍为7331/7331逻辑单元格、9/9行估计一致，8/9路径相同；不是完整兼容验收。

详见[性能报告](performance-report-2026-10-30.md)与[压缩原始证据](benchmarks/ordinary-query-profile-20261105.json.gz)。本批是普通读取partial进展，Goal active。


<!-- CURRENT_EQUALITY_SCAN_COST_20261105 -->

## 普通等值查询的实际扫描选择与完整网络成本（2026-11-05，P15-C7）

本批让普通原生标量堆表查询实际消费基于ANALYZE频率的扫描选择：热点等值可选顺序扫描，低频等值和主键保留索引路径。此前只修改规划器时，无ORDER BY的普通查询仍绕开物理计划；现在结构化输出已接通，并修复无可用索引OR的显示文本合并，以保留重复行、NULL、空串和换行。B-tree、位图AND/OR与GiST在探测前取得语句快照，并在快照不适合当前索引或探测期间提交边界改变时读取完整可见堆候选，再做原条件检查。

这是普通读取的partial进展，使用现有原生 `.stats` 中的MCV/计数；尚未让优化器全面消费新的typed `.analyzed_statistics` 槽。普通EXPLAIN的文本缓存键现包含精确的实时成本设置；没有修改已暂缓的retained/custom/generic规划、参数、portal/format2或mutation/provider生命周期。当前私有普通后端P15-C7 SHA256为 `3ff5f16a9cc3a223d6618db87d8a5bbe85240dfd4c08cfdceafeb6ba49c03d3a`。默认根程序425与既有服务保持；Goal active，PostgreSQL整体功能仍partial。
同一4096行×3列、同一真实SQL与完整网络返回，父代／最终C7／同一父代回测各3进程，固定9个完成测量的进程，不重试计时查询；另两次在基线装载/启动阶段失败、没有计时样本，完整保留。wall中位数相对首轮父代／回测的变化（正值为减少、负值为增加）：热点等值（3200行）12.51%／2.45%；低频等值（1行）-5.57%／-5.47%；不存在等值（0行）77.31%／77.36%；主键等值（1行）-11.11%／9.44%。正负变化均如实保留；仅代表这四个普通读取负载，不能推广为全部操作、通用吞吐或PG速度。

新原生13068检查与独立冷读105检查通过，纯频率组件O2及ASan/UBSan各17检查（启用泄漏检测）。24个相关原生入口实际正常0，含完整7worker恢复1974检查和14份子进程完整输出；这些入口实际链接最终C7的61个核心对象。新73条普通协议命令正常0，验证实际扫描计数、全部行值/NULL及旧快照；26个旧协议入口中23正常0，另3保留与父代一致的严格失败，未称全套通过。真实PG18.6对共享25条命令的7331个逻辑单元格全部逐字相同，9个EXPLAIN行数相同，8个扫描选择相同、1处成本边界扫描选择不同。

原始测量、失败与独立复核见[压缩机器归档](benchmarks/equality-scan-cost-20261105.json.gz)和[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_PG_STATS_SOURCE_20261105 -->

## pg_stats 完整基础查询与读取成本（2026-11-05，P14-C6）

本批接通实际 `pg_catalog.pg_stats` 的17列基础SQL查询、参数为零的纯Describe、文本/二进制/混合结果格式，以及实际 owning anyarray 输出；支持已测的过滤、投影、排序、LIMIT、别名、CTE和JOIN。每次查询只读取当前数据库的真实统计与目录，重用本次查询内的关系/属性索引和每关系一次的物理模式校验，减少逐列重复扫描、模式复制和文件散列。

本节更新下方P13当时“17列SQL尚未接通”的结论；下方历史批次保留当时事实，当前状态以本节为准。当前私有普通后端P14-C6为 `9362b02aba17d45038702211ba42a6a1d797ef3580d97edb8ffabce80e0f63b6`。默认根程序425和既有服务未切换，Goal保持active；整体PostgreSQL功能仍为partial。
同功能未索引C2／最终C6／同一C2回测各3进程，每进程先1次不计时完整查询，再计时3次完整17列SQL查询API。16张表×30列×32行，实际512行/15360存储单元格/1696个NULL；每次查询返回480行×17列。wall中位数384.334／245.178／379.841ms，降低36.21%／35.45%；process CPU中位数392.205／254.151／387.508ms，降低35.20%／34.41%。这是该负载的完整原生SQL API对照，不是任意查询、网络端到端、已发布父代或PostgreSQL速度对照。所有9个样本及长尾保留。

新原生109检查与独立冷读90检查、投影组件O2和ASan/UBSan各548检查通过；34个相关原生入口全部正常0，包含完整7worker恢复1974检查及14份完整子进程输出。普通后端新协议26条SQL及旧标量25/数组19/范围19条自动统计协议通过。真实PG18.6同26条协议命令991个字段值：938逐字相同、53处严格差异保留；17列二进制153项中145相同、8处为实际存储宽度差异，六种范围的24个直接二进制值全部相同。另510个有限float4输入的文本和二进制身份均与PG一致。没有全库SAN或整体PG等价结论。

原始测量、失败及逐字复核见[压缩机器归档](benchmarks/pg-stats-source-20261105.json.gz)与[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_RANGE_COLLECTOR_20261105 -->

## 范围统计采集与完整 ANALYZE 成本（2026-11-05，P13-C2）

本批新增六种内置范围类型int4range/int8range/numrange/daterange/tsrange/tstzrange及实际范围域的ANALYZE采集：独立排序下界和上界、保存范围边界与长度直方图、空范围比例、NULL比例和实际存储宽度。边界保留包含关系、无界端点和子类型语义；本次样本局部缓存解析键，避免排序时反复解析。

最终私有普通后端P13-C2为 `3593b23e5e142605fe71149daec3347015003fb7a84d8a2505c80578e4e77a5d`，默认根程序425保持。完整pg_stats目录/17列SQL及output-only anyarray尚未接通，两条实际探针仍严格失败0A000。多范围尚缺真实目录/存储拥有者；本批仍为采集阶段的partial进展，Goal active。
本批同功能未缓存C1／缓存C2／同一C1回测，各3进程；每进程2048行、1个整数和六种范围，先1次不计时预检，再计时3次完整ANALYZE API。process CPU中位数388.224／201.363／391.997ms，降低48.13%／48.63%；wall中位数441.019／250.031／456.067ms，降低43.31%／45.18%。所有9次原始测量均保留；这不是任意SELECT、父代已发布后端或PG速度对照。

原生范围845检查及冷读262通过；组件O2和ASan/UBSan各43检查（启用泄漏检测，非全库SAN）。21个相关原生入口含完整7worker恢复1574检查全部正常0。原数组786+冷读191、标量641+冷读2、范围19条/数组19条/标量25条协议SQL通过。真实PG18.6同一520条SQL对照18列243项：229相同，14处严格差异保留（13处存储宽度、1处numeric NaN长度排序）。没有PG整体通过结论。

原始测量、失败和复核见[压缩机器归档](benchmarks/analyzed-range-collector-20261105.json.gz)及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_ARRAY_COLLECTOR_20261105 -->

## 数组统计采集与完整 ANALYZE 成本（2026-11-05，P12-C3）

本批把真实数组存储输入接到同一次ANALYZE采样：保存整数组MCV/直方图/相关性，以及常见元素、按非NULL数组计算的成员频率和不同元素数直方图。实际验证integer/text/numeric/boolean/float8数组的NULL、空数组、字面NULL、重复值、多维/下界和冷读；其他元素类型及全部PostgreSQL功能仍需分别验收。

最终私有普通后端P12-C3为 `7d3cc2a1353c105ecac62579a919ec96d088cd325fa74234bc47252061e1029a`，默认根程序425保持。完整pg_stats目录/17列SQL与output-only anyarray尚未接通，两条实际探针仍严格失败0A000；本批是采集阶段的partial进展，Goal active。
完整ANALYZE对照为本批同功能未缓存C2／缓存C3／同一C2回测，各3进程；每进程2048行、1个整数和3类数组，先1次不计时预检，再计时3次完整API。process CPU中位数1243.545／164.180／1296.411ms，降低86.80%／87.34%；wall降低85.86%／86.87%。最终版685.690ms wall长尾保留。此结果限于该负载，未测任意SELECT或PG速度。

原生数组786检查及冷读191通过；计算组件O2和ASan/UBSan各32检查（启用泄漏检测，非全库SAN）；17个相关原生入口含完整7worker恢复1574检查全部正常0。原标量641+冷读2、数组19条协议SQL和原标量25条协议SQL通过。真实PG18.6同一554条SQL对照14列325项：301相同，24处严格差异保留（11宽度、11collation OID、2处相同频率元素的截断选择）。没有PG整体通过结论。

原始测量、失败和复核见[压缩机器归档](benchmarks/analyzed-array-collector-20261105.json.gz)及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_SCALAR_COLLECTOR_20261105 -->

## 标量统计采集与 ANALYZE 优化（2026-11-05，P11-C4）

本批把真实ANALYZE扫描接入标量统计快照：保留NULL、实际存储宽度和原始扫描顺序，按统计目标维护有界样本，生成signed float4 distinct、MCV、排除MCV后的直方图及相关性。数组/范围专用采集、完整继承/表达式索引统计、真实pg_stats目录源和anyarray输出仍待完成；不是完整17列SQL功能验收。

最终私有普通后端为P11-C4 `e1cab6f6…`；默认根程序425保持。原生641检查及fresh冷读2检查通过，统计计算组件O2与ASan/UBSan各22检查通过（启用泄漏检测，非全库SAN），16个相关原生入口全部正常0。普通Main的CREATE/INSERT/ANALYZE/SELECT四条命令通过，464B实际快照独立解码核对两列；两条完整pg_stats SQL探针仍严格失败0A000，服务器正常0。

相同输入、匹配en_US默认排序规则的真实PG18.6对照共189项：184相同，保留3处实际存储宽度差异和文本槽2处collation OID差异（本地0/PG100）。样本边界按独立样本检查有效范围，不要求随机结果相同。原C-locale参考的文本相关性差异也保留；没有修改SQL语料以取得通过。

P10完整模型/持久化之后，P11已接上真实标量采集、原生及普通Main发布，比较缓存复用通过完整API成本对照。下步仍按原完整17列目标接数组/范围专用槽、继承/索引实际owner、真实目录源/来源身份和output-only anyarray；同时处理实际collation元数据及所有未完成验收。采样后的schema检查不代替并发DDL隔离，两统计文件也非共同原子事务。原暂停边界不变，不以四字段视图或全零槽收尾。

[报告与差异](performance-report-2026-10-30.md)、[原始证据](benchmarks/analyzed-scalar-collector-20261105.json.gz)。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

完整17字段目标不变。P10已落地第一层：全字段声明契约、键为真实relation/attribute/inherited的拥有值模型、全部7类槽、带界限/版本/校验的无损快照及单代原子发布；原`.stats`和后端执行路径保持。实际PG参考、独立字节夹具、冷进程/并发合并和组件SAN通过，完整SQL两项仍严格失败。

下一层必须将真实ANALYZE扫描接到该模型，产出width、带符号distinct、排除MCV的histogram、物理顺序correlation、数组/范围槽和真实继承/表达式索引所有者；随后注册真实pg_stats view和17属性，贯通Binder/Main/Describe与output-only anyarray，复用现有权限消费者。当前持久化不会解析旧文件来猜测缺失统计，也不发布一个只含少数字段的替代视图。全源验收仍须标量/数组/范围/冷读/DDL失效/来源/正常0或1格式对照。

该阶段不改变既有暂缓政策或原273项状态，已确认的持久化能力与未接入的SQL能力分列在[报告](performance-report-2026-10-30.md#完整统计视图的快照基础2026-11-05p10)和[归档](benchmarks/analyzed-statistics-snapshot-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

P9-C2 `30c57ca8…` 将表名列表版本标记改为64字节常量存储，并让维护登记表在引擎构造完成前初始化，消除两处实际退出期失效。原七阶段的最终正常及Memcheck通过，旧P8-C2/C3和P9-C1失败保留；没有重写断言或靠隔离的advance成功取代完整验收。

后续继续处理跨服务进程恢复准入、正常Main五项描述符/目录身份，以及真实pg_stats全部17字段和typed数组/来源。R218、恢复功能族及用户暂缓模块均未关闭或扩大。本批9次恢复成本对照无稳定提速结论。[报告](performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)和[归档](benchmarks/relation-list-engine-lifetime-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

P8-C3 `d778afbd…` 已将checkpoint、真实CLOG状态、主heap的xmin/xmax及WAL证据汇总到公开物理恢复发布前，持久推进分配器；BEGIN的实际分配—登记窗口由进程内屏障保护。当前进程内低活动事务保守拒绝、高活动事务可继续及登记窗口控制通过。原共享SHA、原共享校验、安全路径和null/savepoint分流不变。

后续普通工作仍包括：定位同一七阶段验收advance阶段的实际异常、贯通跨服务进程准入、补齐五项探针描述符/真实目录身份，以及实现真实pg_stats全部17字段与有类型数组/来源。已冻结的完整视图计划不是四字段替代终点。用户已暂缓模块和原273项状态不动，不因单个正确性或计时控制通过关闭功能族。

独立较早目标数据与两次正常Main上下文通过；最终C3七阶段整体严格FAIL、后四阶段NOTRUN。第二轮完整恢复调用p50较两次旧版低4.16%/6.42%，第一轮负值和所有长尾保留；默认425和既有服务保持。证据与范围见[完整报告](performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和[机器归档](benchmarks/restore-transaction-horizon-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

本批 P7-C1 `724c1147…` 已将备份创建端SHA块处理接入真实physicalBackup，并在ANALYZE的收集和结果生成两处排除virtual列；同页位图继续使用真实物理ordinal。原校验、恢复、共享SHA、安全认证及用户暂缓代码保持字节不变。

新增永久20+864控制及原相关12入口通过，四组既有探针目标交叉恢复通过；首轮旧→旧在较早全局状态目标的恢复探针失败仍是普通开放缺口，不新增deferred。PG34还有17项严格差异。下一步统计表应补真实当前数据库的typed pg_stats行源、namespace/attribute身份，不能只删除原failclosed守卫或把八列文本renderer当PG视图。

完整备份wall p50为737.771→587.376→677.477ms，较两次旧版低20.38%/13.30%；四类DDLwall p50为2快/2慢，ALTER与TRUNCATE慢于两次旧版。完整35操作/36查询和默认提升未验收。其他已列明的查询需求、类型/来源及语法缺口继续开放。 见[证据与后续边界](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

本批源码P6-C8 `45264111…` 已将虚拟生成列接入真实行输入、声明赋值及落盘身份验证。读取需求/赋值258严格观察新增82整项通过，剩余20；旧767完整记录连续。C++17、原规划/portal/format策略和用户暂停边界保持。

下一步普通缺口包括错误前缀和来源元数据、字符数组新接收器声明一致性、表达式索引AST、ANALYZE virtual统计、legacy LATERAL/EXCLUDED/default/BEFORE组合及目录/identity语法。备份私有摘要原型864组件控制通过，真实备份/SQL收益尚未验证。

C8四条100行生成列SQL实测均快于两C2，但完整35操作/36查询成本未对C8验收，默认425保持。执行依据见[当前报告](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

真实Main分段确认阈值50的同步自动统计是范围修改的大成本；原selected父区间包含后置ANALYZE，不能当独占行体。点查/单行写入/DDL/事务/完整REINDEX负值及更高动作后RSS待进一步定位，不能据快照归因；普通生成列reader prototype仍隔离未交付。

详见[完整性能报告](performance-report-2026-10-30.md)和[本批机器归档](benchmarks/analyze-reindex-main-costs-20261104.json.gz)。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

纯声明API与私有计数语法用途不开放普通动态SELECT计数执行；names-only Parse校验不分析修改输入值。已有参数帧、provider、原Binder/共享求值回调主体保持。部分StorageEngine函数增补默认空的数据输出形参，依赖已重编；未增大既有值/求值器/执行帧字段布局。下一步核对Main真实prepared修改的OLD行物化/绑定上下文，并分开测REINDEX索引键物化与持久化。直接typedexpr API未证明实际Main路径加速，不能沿用静态约2000次解析推断。

详见[本批报告](performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及[机器归档](benchmarks/returning-declarations-hotpaths-20261104.json.gz)。以下为历史记录。

## 内置注册表与RETURNING的本批增量（2026-11-03）

C3 `425165af…` 将无实例状态的内置回调放入只读共享表，实际this/clock回调及用户覆盖仍由各求值器持有；别名副本与绑定标记保持原语义；参数求值可以替换外层回调，后参数判定使用真实最新覆盖，原输入转换与最终调用职责保持。共享表有一次进程初始化和常驻成本，普通查找仍先查实例覆盖，具体收益由独立运行测量确认。

实际辅助求值夹具验证了重复注册减少，但共享表首次初始化仍多1次请求/96B；公开游标512行分配保持、冷CPU20.750→35.862ms变慢。最终36组完整查询中位数全部低于两次S3对照；35类操作仍有2项慢于两次C8，故同时保留局部冷CPU负值与操作慢项，不仅报告构造器或helper收益。

RETURNING复用原绑定声明与表达式，只在实际有请求时保存已有拥有值；真实目标、OLD与NEW的非虚拟存储数组在原source-slot边界按真实schema物化，已拥有结构不解析显示文本。原事务回滚、行锁、mutation graph、provider、参数帧、Describe/Bind/portal和格式2策略不变。新增原生14+20控制通过；PG301为188匹配/113差异，新56项仍有8差异。尚缺别名描述符、虚拟数组载荷、历史物理位图，以及更多WHERE/SET输入、域数组/命名目录、typmod和ALTER USING等普通能力；不以本批关闭原273或B1 typed I/O。

最终实际程序与完整C2/选取C3的不同回归范围、原始失败及完整查询/操作的全部性能结果见[本批报告](performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)。原始273、既有暂缓和历史测量保持，Goal active；独立机器归档见[全部原始统计](benchmarks/function-registry-returning-20261103.json.gz)。以下C8及更早实施记录按当时状态解释。

## 原始类型行物化优化（2026-11-03）

C8 `91d339db…` 只在私有 `PreparedSelectState::cells` 收紧实际数据处理：只有真实非虚拟数组/REGTYPE加载需要新增的物理来源查询；原NULL来源查询、解码、值和collation保留。仅当原来源已有完整行、值/NULL宽度匹配且schema有界时，为原始类型值数组预留容量；不造来源、借用datum或复用参数帧/provider。三列夹具避免重复数据库名复制及1/2/4逐级容量，实际512行比C7少1536次分配/393216B；ExprValue的ABI增量并未消失。

功能观察与C7相同，普通剩余能力与原273不关闭。完整查询36组仍为5快/18慢/13居间，不能将局部分配改进解释为普遍查询加速；源码保留，默认安装仍为S3。方法、全部慢项和后续工作见[本批报告](performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)及[机器归档](benchmarks/owned-datum-primitive-20261103.json.gz)。以下C7记录和其失败性能证据保持。

## 已落地的类型输出增量与下一步（2026-11-03）

基于 `6c27d232` 的 C7 `f135335e…`，以本轮 fresh ABI 谱系的 226 个源码输入验证了真实拥有值输出的有限垂直链路：原分析位置冻结声明；值保留 OID、递归字段、数组元素及形状；结果接收器按合法格式 1 的实际需求捕获；协议编码复用已经匹配的声明。没有用显示文本反推结构或按 search_path 猜结果 OID，也没有用新元数据扩大原 CASE/未选输入分析。

新 REGTYPE/REGTYPE[] 使用真实目录身份和 uint32 持久化，实际读写、索引、COPY、冷重开及正常参数均有控制。但 PreparedReturningExecution 的复合 binary 捕获、WHERE/SET 的更多真实输入加载、域数组/命名复合目录、旧数组位图历史、legacy typmod 冻结和 ALTER USING 仍是普通未完工作；不能将该增量称为 B1 typed I/O 或原类型/协议功能族完成。

成熟数据库的借鉴范围、原始严格失败、ABI 成本及下一步验收见[本批功能与成本报告](performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)。R151 顶层枚举/枚举数组、R106、R65、保留规划/参数帧/portal/format2、R177/R185 等原暂缓边界均保持；原 273 状态及总目标不关闭。资源55通过；1638对照36组为4快/23慢/9居间，公开游标512行多512次分配。下一步在实际数据路径去除不需要的来源复制与临时容量成本，不将慢项统一归因噪声；C7未替换根目录S3。以下前批研究记录按原代别保留。

## 当前持续目标与分批实施（2026-11-03）

最新 `3d576b8b` 后续批次拒绝两版窗口暂存性能提案，生产源码/程序保持S3；真实分配减少未形成稳定跨预算wholeSQL收益，不能以局部请求计数关闭优化目标。
后续类型输出方向参考PG18.6 record_send/array_send、SQLite原始类型与值生命周期、DuckDB2.0类型化DataChunk：保留真实声明/owning值/NULL和维度，不从文本重建结构。独立301参考与三版201差异、隔离提案及静态阻断详见[本批报告](performance-report-2026-10-30.md#窗口暂存分配与类型输出调研2026-11-03)。
先补已有正确描述符下的真实值producer/codec，再分别验证ordinary物理REGTYPE、domain/named catalog依赖；新纯metadata不得触发原不需求的CASE分析，不恢复R106/R151/retained planning/portal/format2等暂停工作。新ExprValue载体的全路径成本、真实ABI链接/完整回归/资源与时钟未通过前不应用或计功能完成。Goal active，原273不变。

已按用户要求建立持续目标：调研成熟数据库实现，逐项补齐非用户暂缓的 PostgreSQL 功能与性能缺口，
在 `master` 上按可验收批次提交代码、测试及文档。原始 273 项要求和暂缓边界继续有效，不以局部优化关闭整个功能族。

本轮调研与采用方向：

| 数据库与一手来源 | 借鉴的做法 | 本项目的应用边界 |
| --- | --- | --- |
| [PostgreSQL 18 tuplesort](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)与[工作内存](https://www.postgresql.org/docs/18/runtime-config-resource.html) | 工作预算属于每个操作；超预算排序生成有序段并归并 | 有序 LIMIT 保留有界候选；普通物理及已接入的普通类型化 Sort 按实例预算生成匿名临时段并有限 k 路归并，最终一轮按需输出；受支持 UNION ALL/VALUES 排序复用该布局；聚合及其他未接入分流/算子的完整治理仍开放 |
| [PostgreSQL SET](https://www.postgresql.org/docs/18/sql-set.html)与[RESET](https://www.postgresql.org/docs/18/sql-reset.html) | 会话覆盖和 LOCAL 分开，回滚/保存点恢复；RESET 使用当前默认来源 | 会话 work_mem 接入普通物理、普通类型化及已接入的 UNION ALL/VALUES Sort，保留启动/文件优先级；配置函数、例程作用域与其他算子治理仍开放 |
| [PostgreSQL tuple slots](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/execTuples.c) | 行槽保留已解码属性及 NULL 标记，避免反复拆解相同属性 | 按物理行读取一次 NULL 位图并复用，去除排序的重复解码；仍保留文件身份核验、TOAST 和虚拟列的原有解码职责 |
| [PostgreSQL SeqScan](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/nodeSeqscan.c)与[heap scan](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c) | 扫描状态保留 snapshot，按需求获取后续 tuple，结束时释放扫描资源 | 普通非分区 TableScan 按页供给、复制拥有的行/NULL 位图，页锁和 pin 不跨表达式求值；分区/RLS/其他分流仍分别验收 |
| [PostgreSQL LIMIT/OFFSET](https://www.postgresql.org/docs/18/queries-limit.html)与[Limit 算子](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/nodeLimit.c) | 在子计划行流上计数，零条不请求输入；OFFSET 行仍被计算 | 普通单表 LIMIT 进入既有类型化链；已接入 VALUES/UNION ALL 的计数表达式使用实际上下文、OFFSET 先求值及逐次更新的安全 Top-N 上限；保留 WHERE/投影、聚合与 DISTINCT 的输入职责，不放宽默认 checked 失败契约 |
| [DuckDB TopN 优化](https://duckdb.org/2024/11/14/optimizers)及[排序设计](https://duckdb.org/2025/09/24/sorting-again) | ORDER BY 与 LIMIT 联合优化，保留有限候选；外部排序需要独立的可溢写布局 | OFFSET 纳入候选数量，保留类型化键与原始行上下文，避免重新求值和丢失 NULL/对象身份 |
| [SQLite 临时文件](https://www.sqlite.org/tempfiles.html)与[原子提交](https://sqlite.org/atomiccommit.html) | 可用有序索引时减少排序；临时排序结构与事务日志承担不同职责，日志按变更页保护回滚 | 普通物理排序临时文件与 WAL 分离；修改快照和细粒度撤销仍需推进，未删除持久化或回滚保护 |

首批已完成的有限范围：

- 普通 `ORDER BY ... LIMIT ... OFFSET ...` 在溢出安全的 `LIMIT+OFFSET` 边界内保留候选，最终按完整键及原输入顺序输出。
- 全部原始排序键仍按原需求求值；排序键的副作用、错误、类型及 NULL 语义不因丢弃候选而跳过。
- DISTINCT、WITH TIES、聚合和分组等不能直接使用普通候选上限的路径保持原有行为。
- 有序 LIMIT 的单表查询在执行阶段进入既有类型化执行器，修复旧分流丢失多列排序键或提前求值的问题；不修改暂缓的 R244 Bind 常量规划。
- 内置 smallint/integer/bigint 的已求值排序键缓存精确整数值；其余类型继续使用原比较职责，numeric 不转 double，OID/对象身份及 NaN/Infinity 各按自身语义比较。
- 最终 11 个原生、11 个协议入口及 35 类操作有限检查通过；新增完整 48 条在 PG 18.6/新版全过，旧版有 11 条失败。
  独立旧/新/旧中 LIMIT/OFFSET 约快 1.4 倍，内存观测和早期失败见[性能报告](performance-report-2026-10-30.md#有界排序-top-n2026-11-02)。

第二批基于 `17df7ff3` 完成普通非分区 TableScan 的按页游标，保留固定 MVCC、堆范围、SSI 及捕获的 NULL 位图，
在页锁/pin 释放后交付行并展开当前 TOAST。23 个原生、17 个协议入口及 35 类操作检查通过；
四种事务场景的跨后端修改、同语句写函数/回滚、部分扫描统计及 I/O 失败释放均有实际控制，新增完整 13 条协议也与 PG 18.6 对照通过。
内存和旧/新/旧 216 样本见[按页扫描报告](performance-report-2026-10-30.md#按页物理扫描2026-11-02)。
第三批基于 `bcde5b3e` 将可支持的普通单表 LIMIT 接入同一链，修复未请求行错误和过量副作用、负数计数及单列星号类型，
新增顶层 SQL 错误前缀接收器。完整 62 条 PG 18.6/候选全过，旧版 9 条失败；24 原生入口通过。
所选完整 21 协议为 20 通过、1 个既有暂缓入口失败，旧/新完整 463 的 40 处差异逐条相同。
1,000 行普通 LIMIT 1 的相邻对照约快 2.3 倍，见[报告](performance-report-2026-10-30.md#普通-limit-按需执行与错误前缀2026-11-02)。
网络端仍缓冲结果；有限行数 portal 的错误需求没有完成。R303/R304/R305 已有暂缓条目同步到中央索引，
不据此恢复 retained planning、portal 生命周期或格式 2 输出需求；接下来在非暂缓范围推进剩余扫描/排序资源治理和修改开销。

第四批基于 `d09cb6c1` 实现普通物理 Sort 的工作内存预算和临时文件归并，执行阶段接入受支持的单表物理单键排序，
保留原始行、NULL 位图、行来源及结构化结果；列别名/序号降低到真实列，修复普通 ORDER BY 1 对整数作文本排序。
实例配置不等同于 PostgreSQL 会话 SET；完整范围和验证见[外部排序报告](performance-report-2026-10-30.md#普通物理排序的内存预算与外部归并2026-11-02)。
首个仅改算子的候选没有覆盖实际 SQL 分流；随后补接入，且把判定移出大型 SQL 函数，再独立复验。
COPY 响应中断已有 R197 用户暂缓记录补入中央索引；完整回归中的失败仍保留，不恢复其修复或将失败排除后宣称全绿。

第五批基于 `471dad3f` 完成上述普通物理 Sort 的最终按需归并，补 NULL 绑定恢复和错误终止清理。
31 个原生通过；所选完整 22 协议仍为 21/1，新增 40 原生控制全过；宽行原生约快 1.78–1.86 倍，小表 SQL 未证明稳定改善，见[最终归并报告](performance-report-2026-10-30.md#最终归并按需输出与排序绑定恢复2026-11-02)。

第六批基于 `049c0bce` 接通会话 work_mem、启动/文件/显式值优先级、事务/保存点以及普通物理 Sort 的有效预算。
36 原生通过，新 163 控制全过；完整所选 25 协议为 24/1，新 470 在 PG/最终程序全过；504 延迟仍有慢值，见[会话工作内存报告](performance-report-2026-10-30.md#会话工作内存与实际排序预算2026-11-02)。

第七批基于 `d8533bc9` 为普通类型化/多键/表达式 Sort 接入完整值快照、外部归并与惰性比较缓存，保留原单键物理快路径及必要表达式需求。
39 原生全过，新 85 控制全过；完整所选 26 协议为 25/1，新 108 在 PG/最终程序全过，旧版两处 NaN ties 不符；实际 Main 9 项资源检查全过。
三个预算及一次高预算复测的完整 1,584 个延迟样本及所有慢值见[类型化排序报告](performance-report-2026-10-30.md#普通类型化排序的预算与比较缓存2026-11-02)；原始功能项不因该局部通过关闭。

第八批基于 `8a8532a0` 为普通 UNION ALL/VALUES Sort 复用有预算的完整值归并，补真实 VALUES ORDER/计数表达式与实际枚举结果描述符。
44 原生全过；新 347 协议与 52 类型阶段控制在 PG/最终程序全过。完整 31 首轮 29/2，连接前失败同源复测通过，既有 463/40 完整观察不变。
新增52永久协议入口独立验证通过；全部32个已运行入口按各自最终结果复核为31/1。
该批记录时，动态 Top-N、FETCH/全局集合表达式计数及未知 OID 参数推导仍开放；
研究与当时完整速度数据保留在[集合排序与 VALUES 历史报告](performance-report-2026-10-30.md#集合排序预算与-values-查询子句2026-11-02)，当前支持范围见下述更新。

第九批基于bb74ae97缓存精确集合整数键、减少原比较参数的重复owning复制，保留共享堆/中断/执行owner。
45原生通过、33协议32/1（原463/40不变）、新66在PG/旧/新全过；1980延迟含所有慢值见[集合键缓存报告](performance-report-2026-10-30.md#集合整数键缓存与比较参数复制2026-11-02)，未关闭原273条款。

前批基于 `baba4bcb` 接通普通 UNION ALL 全局 LIMIT/OFFSET 表达式，以及 VALUES/集合根的 FETCH `c_expr` 计数。
全局计数使用空本层命名空间和真实祖先作用域，不能借用集合 ORDER 输出名或分支 FROM；局部分支计数保持自己的作用域。
同一实际执行上下文每次重开更新后，先求 OFFSET 再求 LIMIT；零计数不打开分支，NULL offset 为零、NULL count 不限行数。
裸 NULL FETCH WITH TIES 在分析时报错，typed/computed NULL 在执行时不限行数；WITH TIES 不使用普通候选截断。
安全的有限 LIMIT+OFFSET 可驱动动态 Top-N，每次执行重置，后续不限行数时撤销旧上限，保留有序输入的表达式需求。
未声明 OID 参数已支持实际选中上下文的普通元数据推导，保持原身份、槽位与来源；完整函数签名和所有参数组合仍为 partial。
该批记录时，合法窗口计数子查询仍明确报 `0A000`，实际宿主由下述后续批次补入；原失败记录保留。
保留计划、实际参数帧/provider、portal、共享排序 helper 和所有专属暂缓边界不变。
该批验证终态及性能另见[性能报告](performance-report-2026-10-30.md)的对应记录；QRY-06 及原 273 项状态保持。

2026-11-03，前批基于 `9f019ffa`，最终 C9 程序 `c696d3b2` 为 Main、Core 和真实 SQL 子查询
接入共用的类型化 WindowAgg，执行实际 source/filter、窗口投影和最终子句，沿用原查询上下文及外层关联。
支持受验收的 derived/CTE/Main SourceContexts JOIN；通用 Core JOIN 的完整 lowering 仍开放。
命名窗口保留真实父名与原始子句身份，支持合法继承及 ROWS/RANGE/GROUPS/EXCLUDE 的已接入边界；
stage 按原始 WindowDef/winref 分组，frame offset 的初始化不会因共享排序键而合并。
每行实际选中的完整 partition/order 键只求值一次，后续 stage、匹配的投影子树及窗口参数/FILTER 复用拥有值；
frame offset 保持独立。输入键专项的 241 项规范化值、计数及消息字节与 PG 18.6 一致。

已接入排名、NTILE、LAG/LEAD、FIRST/LAST/NTH_VALUE，以及合法签名的 COUNT/SUM/AVG/MIN/MAX、
BOOL_AND/BOOL_OR/EVERY、标量 ARRAY_AGG、TEXT/BYTEA STRING_AGG、整数/BIT 位聚合。
保留真实参数、默认值和 FILTER 需求、实际结果类型与 NULL/OID/对象身份；
有限 INTERVAL 除法及窗口 AVG 修正余数换算和舍入，NUMERIC 不借浮点替代精确结果。
仅在实际 WindowAgg 生产者能保证最终 ORDER BY 的兼容顺序前缀时省去最终 Sort，
不兼容方向/表达式/分区仍排序；窗口 WITH TIES 复用已有键，保留 OFFSET、同行边界预读和错误前缀，
零行上限不打开输入。本层 LIMIT/OFFSET 的非法窗口调用经真实 AST 分析报 `42P20`，合法子查询角色单独处理。

窗口排序、分区和历史行仍无 work_mem 约束或 spill；统计窗口聚合的合法签名仍明确 `0A000`，
window DISTINCT、内部 ORDER BY、数组值 ARRAY_AGG、完整聚合族及 temporal in_range 仍有缺口。
普通 SELECT 动态计数、完整参数推导与其他 PostgreSQL 能力继续为 partial。
本批不改变 R177 分组聚合执行、保留计划、实际参数帧/provider、portal、共享排序 helper 或原暂缓边界，
不关闭 QRY-06、QRY-08 或原始 273 项。
完整证据和速度限制见[类型化窗口宿主报告](performance-report-2026-10-30.md#类型化窗口宿主与有序输出复用2026-11-03)，
历史失败及旧性能记录保留，不据此推断普遍加速或完整回归全绿。

随后基于 `b04db801` 的 C1 程序 `56d06bca`，唯一生产变化是窗口私有实现 `85aef835` 的整数键缓存。
准入要求实际非 NULL 比较、无枚举比较绑定、双方 raw type 同为 `smallint`/`integer`/`bigint`，
并具有对应的权威 OID 21/23/20。每个 stage 拥有的已求值键单元只在首次实际比较时解析，
全串解析及 int16/int32/int64 范围验证失败则交原比较 owner；不缓存原比较器的结果或异常。
ColumnRef 的真实 sourceRange OID（含 0）优先，不能由旧 binding 或输出 OID 救回未知身份；
真实 domain 即使显示为基类型也回退。仅有类型别名/显示名、无权威 OID 的隐藏表达式保留原路径，
不是新增语法或类型拒绝。NULL、方向、同行及原始稳定顺序保持，缓存不跨执行重开。

所有 buffered Input 都增加 vector 头与外层容量字节，纯文本等回退行也有固定代价；
仅整数 payload 和 stage OID 元数据按比较需求分配。close 销毁行内 payload、释放 OID 元数据，
外层容量仍可保留至图销毁。窗口分区/stage 排序仍无 work_mem 上限或 spill，不能将累计请求减少解释为 RSS 或峰值内存下降。
独立 allocator 控制的 18 个进程、162 次快照全部完成；1,000 行整数首/末行 uordblks 中位差为 +68,160 B，
文本回退 +22,544 B，单行 +48 B。它们是进程分配器快照，不能分解成精确 cache live 字节或证明真实峰值/RSS 改善。
三种参数 origin、真实 frame/context/provider、Main、共享 SetOutputComparisons、R185 helper 及 R177 执行原文均不变。
该 C1 批次的 71 个 native 入口通过（新增 36 控制），61 个协议入口为 56/5；新增 143 控制中的 14 项差异与 C9 的全部 actual 相同，
原四个失败入口也无新观察差异，不将这些普通差异一概归入暂停范围。
共同八类 public cursor 独立执行阶段进程 CPU p50 降低 41.85–61.90%、C++ 分配请求减少 53.89–63.96%。
另行完成的整 SQL 24 组中，4 MB/16 MB 的 16 项 p50 均低于两次 C9 22.3–39.1%；
64 kB 为 2 项比两旧都快、3 项比两旧都慢（partition_lag、running_sum、partition_full_sum），其余混合，
并保留 C9 首/回测 +13.2% 至 +75.8% 的漂移。不能宣布原七类慢项全清、统一加速或更新 old58/PG 倍率；
原 C9 历史慢值和本批未重跑的旧 35 项停机失败继续保留。
完整方法及内存限制见[窗口整数键缓存报告](performance-report-2026-10-30.md#窗口整数键缓存与实际执行成本2026-11-03)，
不据此关闭原 273 项或宣称完整 PostgreSQL 能力。

本批基于 `d7793044`，中间候选 S2 程序 `779617cb` 在保留每个原 WindowFunc 调用、结果列、需求和实际 winref/stage 的前提下增加结果 owner 映射；以下 S2 证据按原代别保留。
同一实际 stage 的等价非 volatile producer 可共享；含 volatile 参数的复用则取决于真实最终 Sort 是否保留独立的排序输入投影，
只作用于最后一个实际被需求的窗口 stage，不能跨不同窗口定义或 SQL child 站点合并。
虚拟排序输入先保留排序等必须提前计算的完整目标，再抽取推迟投影中的完整 WindowFunc；
仅匹配已有的完整目标，不把 retained wrapper 中的子 WindowFunc 当作独立可用槽位。
计算顺序也按该目标序列保留，因此“调用总数不变但可见值次序不同”的反例同样受到约束；无实际 Sort 时不能照搬此映射。

open 从每组实际需要的原调用中选代表，next 仅计算或推进代表，再向原结果列分发完整 owning ExprValue；
各组末个别名 move，其余必要别名 copy，单结果保留原 move 路径。
不改 targetOwners、原 registerAggregateColumn/demand、窗口键和 frame、实际参数/context/provider、R177/R185 或普通标量求值。
共享结果向该行的 owning 上下文分发，不替代后续行求值；close/restart 重置别名和 transition，不缓存跨执行值。
准备期 v4 按真实 stage/名称/输入及结果类型筛选可能等价的调用后惰性构造身份；同一原 Expr 的字符串只在本次准备内 memo。
没有可推迟目标时省去整目标身份构造，仍保留虚拟根遍历及隐藏排序目标的原执行序；不求值、fold 或重解析用户表达式，也不改保留计划职责。

固定新 220 项在独立 PG 18.6 与 S2 全部符合预期，前 C1 的 27 项差异消除；原 143 项由 14 失败降至 8，原四个失败入口的 1641 项完整观察保持。
72 个 native 入口通过；协议首次完整 62 个入口为 56/6，包含一个 0 SQL 的连接启动失败。
初失败原文保留后，同源 S2 的独立 190 项入口复核完整采集且 0 预期差异，最终 62 个独立入口为 57/5，未改源、预期或入口数量。
512 行真实 public cursor 的 warm 区域包含 restart、消费、关闭及 owning 接收；LAG 两份/四份累计 C++ new 请求减少 44.65%/69.70%，
单份控制增加 1 请求/8 B，SUM 两份减少 2.31%。初次绑定和建图在区域外，这些是请求成本，不是准备耗时、SQL 速度或 live/峰值/RSS。
独立资源 55 项全部符合预期：40 次完整读取、一次预期终止、14 项 SET，共校验 40,000 行/118,000 单元；64 kB 最终 TypedSort 观察到的 FD 上限为 2，EOF 清理及同目录重开成功。
这些资源观察不等于窗口 work_mem/spill 支持、任意写入恢复或旧停机失败修复。
三预算整 SQL 固定对照完成 1296 次计时、324 次预热和 18 项 SET/SHOW，完整校验 1,620,000 行/4,725,000 单元。
共同八类的 24 项中，17 项的 S2 p50 高于两次 C1；重复两份/四份 LAG(length) 的三预算控制则分别降低 37.95–41.66%/64.46–66.16%。
不据局部改善宣称全面提速；全部 36 项和旧版漂移见[窗口结果共享报告](performance-report-2026-10-30.md#窗口结果共享与排序投影2026-11-03)。
PG 已分析常量/无操作 cast 的完整等价、复合 binary 输出及窗口 work_mem/spill 仍开放，不关闭 QRY-08 或原 273 项，不新增暂缓范围。

当前 S3 程序 `aa52b5fe` 只补入可证明结果映射未变化的私有执行路径：该 stage 的全部 producer 在 base/sorted 两映射中都指向自身，
没有 stage 外别名指入，且两种调用顺序过滤到该 stage 后均等于原 `stage.producers` 顺序，才使用原类型化 open 需求检查及 next owning move 循环。
证明基于全部原调用，不随本次 active 子集缩小；共享、隐藏排序重排及仅别名被需求的组仍保持 S2 的实际代表选择和广播。
该判断在 stage 构造后固定，每次 open 仍查询真实执行 owner 的需求；不改来源、窗口键/frame、provider、默认 Core 规划或共享聚合/排序算法。
S3 同代 72 个 native 入口通过，完整 62 个协议入口首轮为 57/5；新 220 和原 143 的 fullactual/raw/normalized/match 与 S2 完整相同，原四个失败入口的 1641 项保持。
190 项首轮完整采集、0 预期差异，全部服务端观察及原始帧与 S2 独立复核相同；请求仅有夹具私有命名差异，不改写 S2 的原始启动失败。
512 行 warm 公开游标的单份 LAG 请求为 226489，与 C1 相同；重复两份/四份 LAG 仍减少 44.65%/69.70%，这些仍是累计请求而非 SQL 速度或 RSS。
S3 同范围资源 55 项独立通过；三预算整 SQL 对照也完整完成 1296 次计时、324 次预热及 18 项 SET/SHOW，1,620,000 行/4,725,000 单元全部符合固定预期。
共同八类的 24 项按 p50 数值为 3 项低于两次 C1、16 项高于两旧、5 项居间，64 kB 的八项全部高于两旧；不把这些数值分类当作统计显著性或整体提速证明。
重复两份/四份 LAG(length) 的三预算控制分别较两旧降低 37.94–40.83%/63.65–66.34%；C1 首/回测漂移为 -11.01% 至 +11.92%。
这些独立 C1/S3/C1 窗口不构成同窗 S3/S2 倍率，也不解释所有此前慢值；S2 全部 36 项和既有诊断继续按原代别保留在报告中。

此后依次评估剩余分流/物化、常见修改快照工作量、完整工作内存与 spill、统计和计划选择，再扩大数据量与并发验证。
聚合/HAVING/未接入集合分流、配置函数/例程 GUC 完整作用域、分区/RLS/并行等剩余物化及虚拟生成列表达式失败返回 NULL 的既有差异仍开放；
原 273 项状态与原子条款不因本批通过而关闭。各批次最终通过范围以测试报告、性能报告及对应提交为准。

> 蓝图描述目标和验收方法，不代表实现已完成。本轮实际 bug 及修复方案见[代码审查报告](code-review-2026-10-13.md)；完成状态仍取自原总账。

> 编制日期：2026-08-23
> 源码基线：`5a31aa3` 加当前动态工作区
> 对应审计：[postgresql-18-gap-audit.md](postgresql-18-gap-audit.md)
> 覆盖口径：审计中的 16 个 P0、243 个能力差距和 14 个非 PostgreSQL 偏移，共 273 项；另给出 183 条 SQL 命令的闭环办法。

## 1. 目标和不可打折的完成定义

目标是 **PostgreSQL 18 行为兼容**，不是复用若干 PostgreSQL 关键字。除项目显式扩展模式外，同一输入在 PostgreSQL 18.6 与本项目上必须得到相同的：

1. 解析结果和对象解析，包括 identifier folding、`search_path`、类型推导、collation 和依赖。
2. 可见行、列顺序、NULL、类型 OID/typmod、文本/二进制值、command tag 和异步消息。
3. 成功/失败边界、SQLSTATE、severity、position、detail、hint、context 和事务状态。
4. 并发可见性、锁等待/冲突、触发顺序、权限/RLS 和崩溃恢复后的持久状态。
5. catalog 可观察结果，以及 dump/restore、客户端、复制和运维接口行为。

“语法可解析”“把定义写入 `.pg_compat_objects`”“单线程 happy path 成功”都不能关闭差距。每项只有在 PostgreSQL differential、并发 isolation、协议字节级和必要的 crash/fault-injection 测试全部通过后才能勾选。

本蓝图不要求数据页或 WAL 与 PostgreSQL **二进制同格式**；它要求 SQL、协议、catalog 和运维行为兼容。磁盘格式采用本项目可演进的版本化格式，通过导入/升级工具保真。若未来要求物理文件级兼容，必须改为 PostgreSQL fork，这不应和当前独立内核路线混在一起。

## 2. 输入输出无损契约

### 2.1 输入必须原样保留

- 新增 `src/sql/SourceText.{h,cpp}`：保存客户端原始字节、解码后的 UTF-8、`client_encoding` 和 byte/character offset 映射。parser 只引用 token span，不改写源串。
- 新增 `src/sql/Lexer`、`GrammarParser`、`ParseAnalyze`、`Binder`、`Rewriter`。`rawSql` 只用于错误位置、审计和 query jumble；执行不得再对它做 `substr/find/tokenize` 二次解释。
- protocol Bind 参数以 `{type_oid, format, is_null, bytes}` 保存；不得先转成带引号 SQL 再重新 parse。SQL 字面量和 Bind 参数走同一类型的 `typinput/typreceive`，但保留各自错误位置规则。
- 所有名称保存原始拼写和规范化名称；quoted identifier 不折叠，未 quoted identifier 按 PostgreSQL 规则折叠和截断，并发出相同 notice。

### 2.2 内部值不能再是“空格分隔字符串”

新增 `src/types/Datum.h`、`TypeDescriptor.h`、`TupleDesc.h` 和 `src/executor/TupleSlot.h`：

```text
Datum = fixed-width scalar | varlena reference | composite reference
Value = {Datum datum, bool is_null, Oid type_oid, int32 typmod, Oid collation}
TupleSlot = {TupleDesc descriptor, Value[] values, optional ItemPointer}
```

固定宽类型按平台无关格式存储；varlena 使用 statement/tuple memory context 管理，NULL 独立于零长度值。`IOperator::next(std::string&)` 先加 `next(TupleSlot&)` 适配器，再逐算子迁移，最后删除字符串接口和 `MaterializedRowsOp` 的字符串形态。热路径不为每列创建 `std::string`。

### 2.3 输出只编码一次

新增 `src/executor/ExecutionResult.h`：

```text
ExecutionResult = RowStream(TupleDesc, next) | CommandResult(tag, affected_rows) |
                  CopyStream | EmptyResult
DbError = {sqlstate, severity, message, detail, hint, position, internal_position,
           internal_query, where, schema, table, column, datatype, constraint}
```

- CLI sink 调用类型的 `typoutput`；PostgreSQL protocol sink 调用 `typoutput`/`typsend`；COPY sink 调用 COPY codec。三者消费同一 `ExecutionResult`，不得捕获或解析 `std::cout`。
- RowDescription 直接来自 `TupleDesc`，包括 name、table OID、attnum、type OID、typlen、typmod、format。NULL 发送 `-1`，空串发送长度 0。
- command tag 由 utility/ModifyTable 节点产生；notice/notification 独立排队；ErrorResponse 全字段来自 `DbError`。
- 浮点、numeric、date/time、bytea、array、range、record、JSONB 等均只有一套 canonical text/binary codec，协议、COPY、cast、函数 I/O 复用它。

### 2.4 性能硬约束

功能兼容不能靠全量 materialize 或每行文件 I/O 达成。所有实现同时满足：

- scan/join/aggregate/protocol 使用流式 `TupleSlot`；可增加 256～1024 行的 `TupleBatch` 快路径，但逐行和批量路径必须语义相同。
- statement/portal/transaction `MemoryContext` 与 `ResourceOwner` 负责统一释放；长查询的内存受 `work_mem`/`maintenance_work_mem` 限制，超限 spill，而不是 OOM。
- catalog 走 syscache/relcache，失效消息按 OID 定位；禁止逐行读 CSV/JSON/sidecar。
- buffer、WAL、锁、统计采用分区锁或无锁计数；禁止新的进程全局大锁。WAL 采用 group commit，索引采用 page-level WAL。
- 每项都建立 microbenchmark 和端到端 benchmark。默认门槛：相同 plan 下相对 PostgreSQL 18 的 p95 延迟不高于 2 倍、吞吐不低于 50%，且不能出现随数据量增长而额外增加一个复杂度阶；最终发布门槛再按目标 workload 收紧。

## 3. 目标模块和迁移方式

| 层 | 在当前代码上的落点 | 最终职责 |
|---|---|---|
| SQL 前端 | 保留 `src/parser/ast.*`，新增 `src/sql/*`，逐步收缩 `main.cpp` | lexer、grammar、analyze、bind、rewrite，输出 fully typed query tree |
| 类型/fmgr | 扩展 `src/catalog/type_registry.*`、`src/types/*`，新增 `src/fmgr/*` | Datum、I/O、cast、operator/function resolution、调用 ABI |
| catalog | 迁移 `src/catalog/*` 和 `TableManage` 虚拟目录 | MVCC/WAL system relations、OID、dependency、syscache/relcache |
| planner | 重构 `src/executor/ExecutionPlan.*` 为 `src/planner/*` + plan nodes | RelOptInfo/Path、统计、成本、plan cache 和 invalidation |
| executor | 由 `IOperator` 迁移到 typed `TupleSlot` | 流式执行、spill、interrupt、instrumentation、ModifyTable |
| transaction | 扩展 `src/transaction/*` | snapshot、CLOG/SubTrans/MultiXact、lock、SSI、resource owner |
| storage | 扩展 `src/storage/*`，拆分 `TableManage.cpp` | relation/fork/page/buffer/FSM/VM/TOAST/large object/control file |
| WAL/recovery | 由 `storage/WAL.*` 拆出 `src/wal/*`、`src/recovery/*` | resource manager、redo、checkpoint、timeline、归档和恢复 |
| protocol | 扩展 `src/network/PostgresProtocol.*`，拆除本地字符串 `QueryResult` | 完整 v3/v3.2、COPY、cancel、pipeline、replication |
| HA/backup | 替换 `ReplicationManager` 骨架和写路径 `LogicalDecoder` | walsender/receiver、standby、WAL decoding、apply、backup |
| process/ops | 扩展 `src/process/*`、`src/common/Config.*` | postmaster/backend、GUC、信号、统计、日志、资源治理 |

迁移采用 strangler pattern：旧入口在每个 statement family 完成前仍可存在，但新旧路径绝不能同时宣称拥有同一语义。先引入 typed 适配器和 differential test；每迁移一个 family，就删除对应字符串 fallback。任何尚未迁移的 PostgreSQL 命令统一返回 `0A000`，不写兼容记录。

## 4. 实施批次和发布门

1. **B0 兼容基线**：冻结 PG 18.6 Docker/reference cluster，生成 SQL/protocol/catalog/isolation 差分用例；建立 273 项机器可读 manifest。
2. **B1 typed I/O**：`Datum`、`TupleSlot`、`ExecutionResult`、`DbError`、类型 codec；CLI/网络共享 sink；移除 `OutputCapture` 的结果职责。
3. **B2 analyze/rewrite**：正式 lexer/grammar、binder、cast/function resolution、rewrite、权限标记；DQL/DML 全进 typed plan。
4. **B3 catalog/DDL**：system relation、dependency、syscache、事务化 DDL；迁移 sidecar，关闭隐式提交。
5. **B4 storage/transaction**：control file、locator/fork、page WAL、MultiXact/freeze、完整 lock/SSI、autovacuum 和所有索引 durability。
6. **B5 planner/executor**：Path/CBO、统计、spill、参数化/parallel/AIO、完整 node 和 EXPLAIN。
7. **B6 protocol/security/ops**：COPY、portal、cancel、binary I/O、TLS/auth、ACL/RLS、监控、工具兼容。
8. **B7 backup/HA**：多 timeline recovery、base/incremental backup、physical/logical replication、subscription、rewind。
9. **B8 extension/ecosystem**：fmgr、AM、hooks、worker、FDW、PL、extension upgrade 和常用扩展。
10. **B9 release proof**：长期故障/并发/升级/安全/性能验证。只有这一批通过后才能使用“PostgreSQL 18 compatible”表述。

每批都提供 feature flag 仅用于开发；公开兼容模式不得静默降级。磁盘格式迁移使用 `dbms_upgrade --check/--link/--copy`，先离线校验、写新 control/catalog、逐 relation 转换并保留 rollback manifest；原目录在成功原子切换前保持可恢复。

## 5. P0 差距逐项方案

### P0-01 统一 SQL 执行管线

在 `main.cpp::execute` 前建立唯一 `QueryPipeline::execute(SourceText, Session)`；Parser 输出 AST，Analyzer 输出 typed query tree，Rewriter 输出权限/RLS/view/rule 展开树，Planner 输出 plan，UtilityExecutor 只处理 utility。先给 legacy 分支加计数和 `0A000` 边界，再按 statement family 迁移并删除。原始输入只通过 span 引用；计划缓存使用 normalized jumble，不改原文。验收为全量 SQL corpus 中 legacy counter 恒为 0，CLI/协议结果一致；parse/analyze 的 p95 不高于 PG 基线 2 倍且 prepared execution 不重复 parse。

### P0-02 结构化结果与错误

落地第 2 节 `ExecutionResult/DbError`，将 `DmlResult`、`PlanExecutionResult`、网络层局部 `QueryResult` 合并；先为旧字符串 executor 写只读适配器，禁止新增调用，再逐算子返回 `TupleSlot`。错误用异常边界或 `Expected<T,DbError>` 单向传播，EOF 与 error 分离。验收覆盖 NULL/空串/空格/NUL/多字节、所有 command tag、SQLSTATE 和 error fields 的 wire capture；流式百万行时内存保持有界。

### P0-03 catalog 唯一事实源

以 `CatalogManager/CatalogService` 为迁移入口，bootstrap 固定 OID 的 system relations，再把 schema/compat/ACL/cast/publication 等 sidecar 导入 catalog。所有 lookup 只能经 `CatalogAccessor`，虚拟 view 改为查询真实 catalog，旧文件只由一次性 importer 读取。syscache/relcache 以 OID 和 `(namespace,name)` 为键并接收事务提交 invalidation。验收随机 DDL 后磁盘、内部 lookup、`pg_catalog`、dump 四者一致；catalog 热查不产生文件系统调用。

### P0-04 完整事务化 DDL

删除 `DdlExecutor::checkAndImplicitCommit`；`DdlTransaction` 扩成 catalog tuple MVCC、pending file create/unlink、relcache invalidation 和 resource owner undo。CREATE 先写 temp/新 relfilenode，commit 原子发布；DROP 在 commit 后安全 unlink；ALTER rewrite 使用新 relfilenode swap。按 PG lock level 获取 heavyweight lock。验收在 BEGIN/savepoint/2PC 与每个 fsync/rename kill point 差分对象集合、文件泄漏和 WAL replay。

### P0-05 完整持久性证明

为 heap/index/catalog/FSM/VM/TOAST/sequence/slot/control/config 定义 WAL owner 和 flush dependency；每个 dirty page 携带 page LSN，写数据页前保证 WAL durable，commit tag 只在 commit record flush 后返回。建立 fault-injection registry 覆盖 write/fsync/fdatasync/rename/unlink。验收自动枚举 kill point 后重启，并验证 committed 全在、aborted 全无、索引/约束/catalog 一致；性能记录 WAL bytes/txn、fsync/txn 和 group commit 合并率。

### P0-06 索引 WAL 重构

废弃 `RM_INDEX_ID` 整文件 before/after image；给每个 AM 定义 meta/page insert/delete/split/vacuum WAL record 和幂等 redo，首次 checkpoint 后修改写 FPI。索引页使用 buffer content lock、page LSN 和 checksum；sidecar 先转 relation fork。验收逐 AM 做 split 中每个指令点崩溃、redo 两次幂等、heap-index cross-check；WAL 放大随修改页数增长而非索引文件大小增长。

### P0-07 完整 MVCC/SSI

在 `LockManager` 增加 SIREAD relation/page/tuple/index-range lock，维护 rw-conflict 图、pivot/dangerous-structure 检测、safe snapshot 和 read-only DEFERRABLE wait。所有 scan AM 报告访问范围，空结果也加 gap lock；commit 清理按 oldest active serializable horizon。验收移植 PostgreSQL isolation specs 并用 history checker 验证 serializability；predicate lock 内存有升级阈值和 spill/拒绝策略。

### P0-08 XID 生命周期

新增 `pg_control` 风格 nextXid/checkpoint horizon，SLRU 化 CLOG/SubTrans/MultiXact，tuple 支持 MultiXact xmax；VACUUM 计算 global xmin、catalog xmin、slot xmin，执行 freeze/all-frozen。启动和分配 XID 前执行 wraparound stop limit。验收用缩小 XID 位宽加速 wrap 测试、长事务/slot/standby horizon 测试；SLRU 分页缓存避免每 tuple 文件 I/O。

### P0-09 单实例并发与故障隔离

采用 postmaster + 独立 backend 进程：postmaster 持有 data-dir lock、shared memory、监听 socket 并监督子进程；buffer/lock/WAL/proc array 放共享内存，backend crash 触发 sibling terminate、recovery 后重启。嵌入式单进程模式只用于测试且不得共享目录。验收双启动互斥、backend SIGKILL、postmaster crash、共享锁 owner cleanup；多核吞吐不得被进程全局 mutex 串行化。

### P0-10 真实备份和恢复链

实现 backup start/stop WAL record、checkpoint、backup_label、tablespace map、manifest/checksum；base backup 流式读取 relation 并包含所需 WAL，增量按 block change summary。restore 先验证 manifest，再按 target 重放并创建新 timeline。验收持续写入下 base/incremental/PITR 恢复，逐文件损坏诊断，恢复点事务边界差分；限速与并行复制可配置且不阻塞 checkpoint。

### P0-11 物理复制和 hot standby

新增 `WalSender/WalReceiver/StartupRecovery/StandbySnapshot`，复用 replication protocol CopyBoth；receiver 落盘并 flush 后反馈 write/flush/apply LSN，startup 逐 record redo，reached consistency 后开放只读。promotion 写 timeline history；同步复制在 commit wait queue 等满足 priority/quorum。验收网络断连、级联、slot retention、promotion/rejoin 和冲突取消；sender/receiver 使用流式 buffer，不累计整段 WAL。

### P0-12 wire protocol 合规

扩展 `PostgresProtocol` 为独立 frame codec/state machine；Portal 保存 plan、params、snapshot、row cursor 和 formats，不保存完整结果集。实现 COPY、CancelRequest、Flush/Sync/pipeline、SSL/GSS negotiation、Notification 和 replication mode。验收 psql/libpq/JDBC/ORM matrix 与 golden pcap；百万行 portal/COPY 有 backpressure，内存为 O(fetch-size)。

### P0-13 权限边界统一

Analyzer 为每个 RangeTblEntry/object 记录 required privileges 和 checkAsUser；Rewriter 注入 RLS/security-barrier，Executor 在打开 relation/function/COPY/LO/maintenance 时统一调用 `AclCheck`。SECURITY DEFINER 通过栈式 security context 和受控 search_path，异常自动恢复。验收按对象/角色/列/RLS 建差分矩阵并覆盖 view/trigger/function 旁路；权限缓存按 membership/ACL invalidation。

### P0-14 资源治理

引入 MemoryContext、ResourceOwner、TempFileSet、InterruptToken 和 workload limit；sort/hash/window/aggregate/bitmap/reorder buffer 达到 work_mem 后分区 spill，所有锁等待、I/O、worker 定期 `CHECK_FOR_INTERRUPTS`。磁盘满保持 WAL/数据不变量并返回精确错误。验收超时/cancel/OOM/ENOSPC/EMFILE/慢客户端测试无泄漏或半提交；spill 算法复杂度和 I/O bytes 有基准。

### P0-15 可升级性

`ControlFile` 保存 system identifier、format/catalog version、checksum、block size、feature flags；每版提供 catalog migration 和 relation converter。`dbms_upgrade --check` 检查 extension/collation/endianness，`--link/--copy` 生成可回滚 manifest；旧 v2 仅由只读 importer 读取。验收 N-2→N、失败回滚、崩溃续跑、dump diff 和升级前后 workload checksum。

### P0-16 差分兼容测试

建立 `tests/compat/manifest.yaml`，每项记录 reference SQL、expected rows/types/errors/catalog、isolation schedule、protocol bytes、crash points 和 perf case；runner 同时驱动 PG 18.6 和本项目，规范化仅限 PID/OID 等声明过的非稳定字段。任何差异必须显式 allowlist 且带过期版本。CI 按快速/夜间/发布分层，发布要求 allowlist 为零（项目扩展除外）。

## 6. 183 条 SQL 命令的闭环方案

183 条命令不再各写一个字符串 handler。每条命令在 `tests/compat/sql_commands.yaml` 有独立记录，覆盖 grammar、analyze、owner/ACL、dependency、transaction、lock、command tag、SQLSTATE、catalog 和 dump/restore。实现归入以下可复用 utility family：

| 命令集合 | 完整实现路径 | 关闭条件 |
|---|---|---|
| 全部 `CREATE/ALTER/DROP` | typed utility AST → `UtilityExecutor` → object-specific catalog API → dependency/lock/WAL；EXT/FDW/复制对象必须有真实 runtime | 审计中的 42 ALTER、42 CREATE、43 DROP 每条均有独立 PG differential 和 crash rollback case |
| `SELECT/VALUES` | Analyzer/Rewriter → Path planner → typed executor | QRY/OPT/TYPE/SEC 对应项通过，RowDescription 与 PG 相同 |
| `INSERT/UPDATE/DELETE/MERGE/COPY` | 统一 `ModifyTable`/`CopyFrom/CopyTo`，共享 constraint/trigger/RLS/partition/WAL path | DML/CONS/TXN/PROTO 项通过，所有入口无 legacy fallback |
| transaction/locking/cursor/prepared | `TransactionManager`、Portal、PreparedPlan、LockManager | 状态机、snapshot、SQLSTATE、ReadyForQuery 和并发 schedule 与 PG 一致 |
| `GRANT/REVOKE/SET ROLE/SESSION AUTHORIZATION` | catalog ACL/membership + security-context stack | SEC 全矩阵与依赖/回滚通过 |
| `VACUUM/ANALYZE/CLUSTER/REINDEX/CHECKPOINT` | maintenance dispatcher + progress + interrupt/resource owner | 存储/WAL/索引/VAC/监控项通过 |
| `LISTEN/NOTIFY/UNLISTEN` | transactional shared notification queue + protocol async sink | commit/rollback、跨 backend、queue full 和异步 wire 测试通过 |
| `EXPLAIN` | 对任意 plannable/utility statement 序列化统一 instrumentation tree | 所有 option/format 的 schema 与 PG 差分通过 |
| `IMPORT FOREIGN SCHEMA` | FDW API 调用远端 introspection 后生成 typed utility command | FDW runtime 完整，事务和错误可回滚 |
| `LOAD` | 仅在 fmgr ABI 和受控动态加载完成后启用，否则 `0A000` | 真实加载/卸载、权限、ABI、错误/信号安全测试通过 |

`P/S/X` 只用于现状审计，不作为实现阶段。命令只有“全语义通过”或“明确 `0A000`”两种公开状态。

## 7. SQL 前端、名称解析和语义分析

### SQL-01

用可生成 LALR/GLR 表的 grammar 或经过 corpus 验证的 hand-written grammar 替换二次字符串解析；保留 `ast.h` 但让每个节点含 source span。新增 scanner differential corpus，覆盖全部 183 命令。lexer 单遍 O(n)，token 用 `string_view`；验收原始 query 不变、尾随 token/position/SQLSTATE 与 PG 一致。

### SQL-02

在 Lexer 实现 quoted/U& identifier、U&/E/string、dollar quote、bit/hex、nested comment 和 `standard_conforming_strings` 状态；统一 client encoding 转码和 character position。token 延迟解码以避免复制；按 PG regression 的合法/非法字节和 escape 输出逐字节比对。

### SQL-03

删除 `main.cpp` 的 boolean/array/CASE/ANY/ALL rewrite；Parser 生成专用表达式节点，Analyzer 做类型和三值逻辑转换。原始 literal span 用于错误，deparse 由 AST 产生；表达式编译后复用，避免逐行 parse，结果/NULL 与 PG differential。

### SQL-04

新增 `ParseState/NamespaceItem/RangeTblEntry`，分层解析 relation、column、function、operator、type，记录 lateral visibility、outer reference 和歧义。lookup 批量走 syscache；验收 nested scope、alias shadow、ambiguous/undefined 的 SQLSTATE 和 error position。

### SQL-05

把 `search_path` 解析为 namespace OID 列表，隐式注入 `pg_catalog`、session `pg_temp_N`；所有 qualified/unqualified lookup 复用 Binder。缓存键含 role/temp namespace/path generation；测试跨 schema 同名、prepared plan path 变化和权限结果。

### SQL-06

实现 `NAMEDATALEN=64` 兼容规范：未 quoted UTF-8 identifier 折叠并按字符安全截断，quoted 保留大小写；catalog 使用 OID locator，物理路径不拼名称。名称规范化一次并缓存；比对 notice、collision、rename 和 multibyte 边界。

### SQL-07

在 Analyzer 实现 unknown pseudo-type、type category/preferred type、common supertype 和 `$n` 推断，信息来自 `pg_type/pg_cast`。使用候选剪枝和 memoization；验证 UNION/CASE/ARRAY/VALUES/operator/function 参数类型、OID 和 42P18/42804 等错误。

### SQL-08

把 cast context（implicit/assignment/explicit）、method（function/inout/binary）和 dependency 写入 `pg_cast`；Analyzer 用最短合法 cast path，不允许循环/歧义。built-in path 预计算，catalog invalidation 后重建；验证 CREATE/DROP CAST 立即影响 plan 与执行、binary-coercible 不复制 datum。

### SQL-09

实现候选收集、named/default/variadic 展开、exact/preferred/polymorphic 打分以及 search_path tie-break；procedure/function/aggregate/operator 使用同一 fmgr signature。signature cache 按 namespace generation 失效；差分所有重载歧义、默认值、VARIADIC 和 anycompatible 推导。

### SQL-10

每个 expression 带 input/result collation，Analyzer 按 explicit/implicit/none 规则合并并报 conflict；`pg_collation` 保存 provider、locale、deterministic、version。sort/hash/index path 携带 collation OID；版本变化标记相关索引需重建，验证排序/比较/错误与 PG/ICU 版本。

### SQL-11

Parser 必须消费 EOF；`DbError` 从 token span 计算字符 position，并支持 detail/hint/context/object fields。建立错误码映射表而非统一 `XX000`；错误构造不做昂贵字符串格式化直到发送，回归比对全部负例和事务 abort 状态。

### SQL-12

以 `TupleSlot` 替换 `StorageEngine::vector<string>` 和算子空格行；先提供 schema-aware decode adapter 只读旧 v2 row，再将 heap tuple 存为 typed datum。热路径用 slot/arena 复用，验证空格、delimiter、NULL、empty、NUL byte、Unicode、复合值和百万行内存上界。

### SQL-13

统一 `PreparedStatement{raw, analyzed_tree, param_types, result_desc, dependencies}` 与 `Portal{plan, params, formats, cursor,snapshot}`；SQL PREPARE 只是 session catalog entry，协议 prepared 是同一核心对象。generic/custom plan 按执行统计选择，依赖/GUC/role/path 改变失效；比对 Describe、unnamed replacement、错误时机和 portal suspension。

### SQL-14

新增 copy-on-write `QueryTree`、RTE permission info、rewrite provenance 和 dependency list；view/rule/RLS 展开用深度/循环保护。共享不可变子树减少复制，plan cache 以 dependency generation 失效；差分 view rule、RETURNING、permission check-as-user 和递归错误。

## 8. Catalog、OID、对象和 DDL

### CAT-01

bootstrap `pg_class/attribute/type/proc/namespace/depend` 为普通 heap/index relation，使用 MVCC、buffer、WAL；CSV/sidecar 由 `CatalogImporter` 一次导入并写 migration marker。syscache 避免每次 heap scan；crash/downgrade 测试证明原子切换和 catalog tuple 可见性。

### CAT-02

将 `queryPgCatalog` 虚拟拼装改成 catalog view SQL/内置 scan；DDL、binder、planner 只能依赖 `CatalogAccessor`。启动时做 bootstrap index scan，禁止读取另一套 schema 文件；内部 lookup 与用户 SELECT 同 snapshot，差分列 OID/type/visibility。

### CAT-03

按 PG18 catalog schema 建完整 relation/unique index/toast，先生成声明式 catalog descriptor 再 bootstrap，避免手写结构漂移。按功能批次填 `pg_constraint/index/am/opclass/operator/cast/collation/rewrite/trigger/policy/auth/default_acl/database/tablespace/statistic/replication`；catalog init 和升级由同一 descriptor 生成，逐表对比 `information_schema.columns` 和关键约束。

### CAT-04

OID allocator 持久化 nextOid，bootstrap OID 固定，用户 OID 在 cluster 范围防冲突；所有对象用 ObjectAddress `(classId,objectId,subId)` 引用。实现 reg* I/O 和 qualified deparse；批量 restore 可保留/映射 OID，缓存 OID lookup；验证 dump/restore 后引用、dependency 和 reg* round-trip。

### CAT-05

统一 `pg_authid/acl/comment/security_label/depend/extension`，对象创建默认写 owner/dependency，ALTER OWNER 重写 ACL 并检查 membership。读取走 ACL/syscache；验证所有对象的 owner/comment/label/extension membership 在 rename/drop/restore 后一致。

### CAT-06

DependencyManager 从 root ObjectAddress 做有向图遍历，区分 normal/auto/internal/pin/extension；RESTRICT 先收集并报告，CASCADE 排序执行，循环使用 visited set。按 OID index 查询避免全表扫；比对 NOTICE 顺序、错误对象、subobject drop 和事务回滚。

### CAT-07

DDL catalog tuple 和 physical pending action 注册到同一 transaction/resource owner；WAL 先行，commit 发布 relcache invalidation 和 deferred unlink，abort 删除未发布文件。文件操作批处理 fsync；随机 DDL kill-point 后 catalog/文件/索引无孤儿。

### CAT-08

实现 cluster 级 `pg_database`、template clone strategy、encoding/locale/ICU/collation version/tablespace/connlimit；复制 template 时阻止并发连接并 WAL/manifest 记录。clone 优先 reflink/copy_file_range 后回退 buffered copy；比对 option defaults、权限、template0/1 和失败清理。

### CAT-09

物理表名改 `RelFileLocator`，schema 仅为 `pg_namespace` OID；实现 CREATE/ALTER/DROP SCHEMA、USAGE/CREATE ACL 和 search_path。迁移器解析旧 `schema__table` 并处理碰撞；namespace lookup 有 cache，差分 rename/owner/concurrent object 与临时 schema。

### CAT-10

每个 ALTER TABLE action 先 analyze 并计算 lock/rewrite/dependency，再在一个 utility transaction 执行；类型改变用 `USING` typed expression，新 relfilenode rewrite/swap，no-rewrite action 只改 catalog。支持 ONLY/递归和多 action 顺序；rewrite 流式并可并行读，验证中途错误完全回滚。

### CAT-11

catalog 表示 partition key/bound/default/inheritance；Analyzer canonicalize bound，attach 前用约束证明减少 scan，否则在受控 lock 下验证；planner 做 runtime/startup pruning。partition index/unique 追踪 parent validity；差分 DEFAULT move、concurrent detach/finalize 和跨分区 DML。

### CAT-12

用 inheritance graph 合并列、constraint/default/generated/identity/statistics/ACL，记录 `attinhcount/islocal` 并检测多父冲突。图 lookup 缓存且 DDL invalidation；验证 ONLY、递归 DDL、drop inherited column/constraint 和权限。

### CAT-13

backend 启动分配 `pg_temp_N` OID namespace，temp relation 使用 backend-local locator 和 catalog rows；commit 执行 PRESERVE/DELETE/DROP，2PC 明确拒绝含 temp state。backend exit resource owner 清理；temp catalog/relcache 本地化避免全局 invalidation，差分多 session 同名和异常退出。

### CAT-14

unlogged relation 建 main/init forks；checkpoint 保证 init fork，crash recovery 从 init 重置 main 并清索引，正常 shutdown 保留。WAL 仅记录必要 catalog/init 操作，复制/base backup遵循 PG可见性；验证 crash/non-crash 与 standby 行为。

### CAT-15

sequence 作为 relation + `pg_sequence`，用轻量 sequence lock、cache block、WAL log-ahead 实现；nextval 非事务，setval/currval/lastval session state，OWNED BY dependency。缓存减少锁/WAL频率；差分 rollback/crash/cache gap/cycle/concurrency/identity。

### CAT-16

CREATE VIEW 保存 analyzed query 到 `pg_rewrite`，Rewriter 展开并检测 recursive；判断 simple view 自动可更新，执行 LOCAL/CASCADED CHECK OPTION、security barrier/invoker。不可变 query tree 缓存按 dependency 失效；验证 DML、列权限、RLS、rename/deparse。

### CAT-17

materialized view 保存 typed query、populate flag、dependency 和 heap relation；refresh 普通模式新 relfilenode swap，CONCURRENTLY 要求合格唯一索引并以 diff/upsert 事务应用。使用 maintenance_work_mem/spill；验证未填充访问错误、并发读写和失败回滚。

### CAT-18

`pg_trigger` 引用真实 trigger function OID，Executor 建 BEFORE/INSTEAD/AFTER 与 statement/row event queue；transition table 按 statement tuplestore，constraint trigger 接 deferred queue，按名称排序并处理递归深度。tuplestore 可 spill；比对 OLD/NEW、WHEN scope、RETURNING 后值、savepoint。

### CAT-19

event trigger 订阅 DDL command lifecycle 并提供 command/drop object SRF；rule 保存 action/qualification query tree，由 Rewriter 执行 ALSO/INSTEAD。事件命令列表在 transaction context 中有界累积；比对 firing order、disabled state、递归和回滚。

### CAT-20

cluster `pg_tablespace` 保存 OID/location/owner/ACL/options，data dir 使用受控 symlink/locator；CREATE/DROP 做路径所有权、空目录、跨设备 fsync 和 WAL/backup map。I/O 层按 tablespace 分离并发；验证 rename/drop busy、备份恢复和 ENOSPC。

### CAT-21

COMMENT/SECURITY LABEL 统一以 ObjectAddress+subId 存 catalog，provider validate hook 决定 label；覆盖 PG 对象全集并随 dependency cascade。索引 lookup O(1)；比对 NULL 删除、shared object、column label 和 dump。

### CAT-22

删除 compat object 成功路径；manifest 标记未完成 feature 时，Analyzer/UtilityExecutor 在任何写入前返回 `0A000`。只有对应 catalog+runtime+tests feature gate 完成才注册命令。验证每个原 S/X 命令既不产生文件/catalog，也不改变 transaction，完成后再切换成功结果。

## 9. 数据类型、I/O、函数和操作符

所有 TYPE/FUNC 项共用第 2 节 Datum 与 `TypeCodec{input,output,receive,send,compare,hash}`，类型 OID/typmod/collation 全程保留。每种类型在 text、binary protocol、COPY text/CSV/binary、cast、heap、index、sort/hash 六条路径使用同一实现和 golden vector。

### TYPE-01

实现 base-10000 可变长 `NumericDatum`、weight/sign/dscale/NaN/±Infinity，算术使用 guard digits 并按 PG rounding/overflow；send/receive 严格按 PG numeric binary frame。小值内联、聚合用可复用 accumulator；以 PG 生成的随机高精度向量验证算术、sort/hash/index 和 wire bytes。

### TYPE-02

int2/4/8 使用定宽 little-endian storage+显式字节序 codec，所有算术用 checked operation；float 遵循 PG NaN ordering、Infinity 和 shortest-roundtrip output。执行器专门化常见 cast 避免 fmgr 开销；覆盖边界、除零、溢出、不同优化器/架构和 binary protocol。

### TYPE-03

money 内部用 int64 最小货币单位，输入输出读取 session locale 的 decimal/thousands/currency，算术检查溢出，禁止 double。locale descriptor 缓存；对各 lc_monetary、负数格式、cast/numeric 运算和 8-byte wire/storage 做差分。

### TYPE-04

text/char/varchar/name 使用 varlena，无 65535 人为上限；typmod 在 assignment cast 检查，bpchar 比较忽略尾空格但输出保真。encoding/collation 由 codec/operator 处理，大值自动 TOAST；验证多字节截断、invalid encoding、padding、LIKE/regex/sort 和 binary bytes。

### TYPE-05

bytea 保存任意字节并实现 hex/escape I/O、substring/overlay/bitwise/encode/decode/digest 等函数，NUL 不特殊；protocol binary 直接发送 payload，text 走 bytea_output。大值流式 TOAST，避免十六进制中间副本；验证 0～大对象、非法 escape、COPY/索引/hash。

### TYPE-06

date/time/timestamp/timestamptz/interval 用 PG epoch 和微秒 int64，导入 IANA tzdata 并实现 BC/infinity/DST gap/fold/timezone abbrev/typmod。timezone cache 按 zone/version，共用 parser/formatter；以 PG 的全时区边界和 binary epoch vector 比对运算、`AT TIME ZONE`、输出。

### TYPE-07

boolean Datum 仅含 true/false，NULL 在 Value 标志；Lexer/codec 接受 PG 合法缩写，输出固定 `t/f`，不再转换成整数。表达式实现 SQL 三值 truth table 和 short-circuit；bit-packed batch 可优化但与标量差分，覆盖 CASE/WHERE/CHECK/ANY/ALL。

### TYPE-08

enum label/order 存 `pg_enum`，Datum 为 enum OID；ADD VALUE 在事务提交后才可安全使用并处理 BEFORE/AFTER，rename 保持 OID。比较优先 OID/order cache，异常序列回退 catalog；验证并发 snapshot、sort/index/hash、dump restore。

### TYPE-09

按 PG double-based representation 实现 point/line/lseg/box/path/polygon/circle，补 parser、operator、distance/containment 和 NaN 规则；GiST/SP-GiST support function 走 fmgr。SIMD 只用于等价距离批处理；用随机/退化几何 differential 和 KNN recheck 验证。

### TYPE-10

inet/cidr 使用 family+prefix+16-byte address，macaddr/8 定宽；实现网络包含、mask、算术、排序/hash 和 PG binary layout。prefix operation 用位运算且可被 GiST/SP-GiST/BRIN opclass 复用；差分 IPv4/IPv6 映射、非法 host bits、wire bytes。

### TYPE-11

bit/varbit 用 bit length+packed bytes，typmod 强制长度；实现 concat、shift、substring、compare/hash 和 text/binary codec，尾部 unused bits 清零。字长批处理优化；验证非字节对齐、负/大 shift、TOAST 和索引顺序。

### TYPE-12

建立 text search catalog runtime：parser→token→dictionary chain→lexeme/position/weight `tsvector`，`tsquery` 为 typed tree；实现 rank/headline 和 GIN/GiST opclass。词典/配置缓存按 catalog invalidation，共享词干资源；用 PG 配置语料比对 binary/text、match/rank 和 index recheck。

### TYPE-13

UUID Datum 固定 16 byte，parser/output 遵循 PG 宽松输入和 canonical 小写输出，比较为 unsigned byte order；实现 v4/v7/extract timestamp/version。避免字符串化参与 hash/index；对 RFC/PG vector、时间单调边界和 binary protocol 验证。

### TYPE-14

链接 libxml2 实现 xmloption document/content、encoding declaration、xpath/XMLTABLE/XMLNAMESPACES/serialize；构建时无 libxml2 则明确禁用并 `0A000`。解析树限深/限实体并在 memory context 释放；与 PG/libxml 版本固定环境差分，加入 XXE/DoS 安全用例。

### TYPE-15

JSON 保留原文本并验证；JSONB 转成 versioned container（object/array/scalar、offset table、canonical key order）和 numeric Datum，补 SQL/JSON/jsonpath。GIN `jsonb_ops/path_ops` 从容器抽 key；迭代器流式避免反复 parse，差分 duplicate key、number、Unicode、path errors、binary和索引。

### TYPE-16

ArrayDatum 保存 element OID、ndim、dims、lower bounds、null bitmap 和 aligned elements；subscripting/assignment、comparison/hash、unnest/array functions 均操作 typed datum。大数组迭代和 slice 不复制，必要时 TOAST；差分非 1 lower bound、多维空数组、record array 和 binary frame。

### TYPE-17

CompositeDatum 为 TupleDesc OID/typmod + TupleSlot，匿名 record 由 typmod registry 管理；实现 field select/update、row compare、text/binary record codec 和 RETURNS record descriptor negotiation。descriptor cache 按 type alteration 失效；验证 dropped columns、NULL record/field、function/portal metadata。

### TYPE-18

RangeDatum 保存 subtype OID/collation、empty/lower/upper inclusive/infinite 与 typed bounds；调用 catalog canonical/subdiff，Multirange 是排序合并的不交叠 range vector。GiST/SP-GiST 操作 typed bounds；差分 discrete/continuous、NaN/infinity、aggregate、binary和索引。

### TYPE-19

domain Datum 复用 base type但保留 domain OID；Analyzer 递归展开 coercion，assignment/函数返回时执行 NOT NULL 和全部 CHECK，新 constraint 用 table scan revalidate 后原子生效。constraint expression cache按 dependency 失效；验证嵌套 domain、array/composite domain、错误 constraint 名。

### TYPE-20

OID/reg* Datum 使用 uint32 OID，input 通过 namespace/signature resolver，output 在可见且无歧义时最短 deparse否则 qualified；支持 numeric OID 输入和 snapshot。syscache 加速，验证 rename/drop/search_path/overload 和 dump round-trip。

### TYPE-21

实现 xid/xid8/cid/tid/pg_lsn/pg_snapshot/aclitem/name/char/cstring 的定宽或专用 varlena layout 和 codec；内部-only 类型拒绝普通表使用。LSN/快照算术连接 transaction state；逐类型比对 text/binary、sort/hash、wrap 和 privilege visibility。

### TYPE-22

在 fmgr call frame 表示 pseudo/polymorphic constraints，Analyzer 解出 anyelement/array/range/multirange/compatible family；internal/trigger/event_trigger/handler 类型只能出现在允许签名。resolution 结果缓存；负例必须在 analyze 阶段给 PG 相同 SQLSTATE，正例比对 result OID/typmod。

### FUNC-01

以 PG18 `pg_proc` manifest 生成函数注册和差分用例，按数学、字符串、格式、日期、网络、全文、XML、JSON、array/range、系统/管理/统计/复制分批实现；每个函数声明 strict/volatility/parallel/cost。底层算法共享 typed codec，不做 SQL 字符串往返；关闭条件是 signature/结果/错误/权限全集 manifest 无缺口。

### FUNC-02

`pg_aggregate` 保存 trans/final/combine/serial/deserial/inverse 函数、state type 和 init value；Aggregate node 为每 group 管 typed state，支持 DISTINCT/ORDER/FILTER、ordered/hypothetical 和 partial/final。hash state 超 work_mem 分区 spill、sort aggregate 外排；差分空输入、parallel、moving window 和序列化。

### FUNC-03

`pg_operator` 连接 fmgr function、commutator/negator、restrict/join selectivity、hash/merge 标记并写 dependency；Analyzer 走重载解析，planner 只在声明且 opfamily 验证后使用 hash/merge。operator lookup cache；测试自定义 cross-type operator 对 result、plan 和 drop cascade 的影响。

### FUNC-04

Analyzer/Planner 使用 volatility 决定 constant fold/index/parallel，Executor 落 strict NULL shortcut、security definer/leakproof/SET context；cost/rows 进入 path cost。fmgr 元数据缓存，调用可内联 fast path；用恶意 leak function、volatile counter、parallel plan 和 EXPLAIN cost 验证。

### FUNC-05

SQL function 保存 analyzed body或延迟 parse body，Planner 对安全 scalar/table body做 inlining，support function可简化/估算/index condition；调用绑定 polymorphic/default/named/variadic。缓存依赖 body/object，差分 search_path、DDL invalidation、SRF 和递归。

### FUNC-06

重写 PL/pgSQL 为 bytecode/statement tree runtime，支持 records/%ROWTYPE、exception/subtransaction、GET DIAGNOSTICS、EXECUTE USING、cursor、trigger vars、RETURN QUERY 和 plan cache。每 block memory context、SPI prepared plan复用；移植 PG plpgsql regression 并比对 context stack、SQLSTATE、transaction restriction。

## 10. 约束和数据完整性

约束统一进入 `ConstraintManager`，由 `ModifyTable/CopyFrom` 在同一顺序调用：BEFORE trigger→generated→NOT NULL/CHECK/RLS WCO→partition route→unique/exclusion/FK→heap/index→AFTER/deferred→RETURNING。任何入口不得旁路。

### CONS-01

PK/UNIQUE 由 catalog constraint+unique index支撑，支持多列、NULLS NOT DISTINCT、DEFERRABLE；immediate 用 speculative insertion/index conflict，deferred 保存 typed key event 到提交检查。分区唯一要求覆盖 partition key；批量冲突排序降低锁抖动，差分并发 upsert/deadlock和错误 constraint。

### CONS-02

FK 保存两侧列/operator OID和 action；statement/commit 用 RI trigger执行 indexed lookup并取得 key-share，支持多列 MATCH FULL、循环和 partition。批量 key 去重与按 index order检查；移植 PG ri/isolation tests，验证 snapshot race、cascade order、savepoint。

### CONS-03

CHECK 保存 cooked expression+dependency，DML 对新行执行；NOT VALID 跳历史 scan，VALIDATE 在合适 lock/snapshot 下并发扫描。immutable restriction和partition proof共用 predicate engine；表达式编译一次，差分 UNKNOWN pass、error context和domain behavior。

### CONS-04

exclusion constraint 通过 catalog operator/opclass 的 GiST scan查冲突并锁候选，支持 expression、多列、where、deferrable；插入采用 wait/recheck loop避免竞态。性能由GiST选择性与batch排序保证；并发 isolation验证重叠、NULL、abort/wakeup。

### CONS-05

generated expression在 Analyzer 验证依赖/volatility；stored 在写入时计算入 heap/WAL，virtual 在读取/索引/复制时按 PG18规则计算并保留 column metadata。表达式 program缓存；差分 ALTER、COPY、trigger、logical replication 和 privilege。

### CONS-06

identity 绑定 owned sequence，区分 ALWAYS/BY DEFAULT和 OVERRIDING SYSTEM/USER，全部 sequence option可ALTER；partition/inheritance复制或引用规则按 PG catalog。nextval cache提供性能；验证COPY/INSERT、rollback、restart、drop dependency。

### CONS-07

constraint trigger事件存 transaction级 deferred queue，SET CONSTRAINTS切换时按创建顺序立即drain；subtransaction记录queue watermark以便rollback，2PC序列化事件并恢复。队列可spill且key去重；验证savepoint/crash/prepare/commit错误时机。

### CONS-08

删除 `TableManage`、legacy view/COPY/MERGE的独立constraint代码，所有写路径构造 `ModifyTableContext` 并调用同一 pipeline；写入测试hook记录每阶段。验收每种入口注入相同违规行得到同SQLSTATE/constraint、无heap/index残留，批量路径吞吐不因逐行catalog lookup退化。

## 11. 查询、DML 和执行语义

### DML-01

Planner 为 INSERT/UPDATE/DELETE/MERGE 生成 `ModifyTablePlan`，source 是任意 plan，target含relation/partition/view mapping；Executor逐slot修改并共享constraint/trigger/RLS/RETURNING。删除 `tryDmlBridge` fallback 后才关闭；流式消费 INSERT SELECT，差分CTE/view/partition和affected row tag。

### DML-02

Analyzer 将 conflict target解析为唯一/排除索引并验证predicate/expression/collation/opclass；Executor做 speculative heap insert+index token，冲突时等待/recheck并执行 typed DO UPDATE。缓存 inference result随index失效；移植 PG upsert isolation，验证 RETURNING、trigger和高冲突吞吐。

### DML-03

UPDATE FROM/DELETE USING 的 source由普通join tree规划，target ctid作为resjunk；一个target遇多source遵循PG行为，WHERE CURRENT OF从portal取position。修改前做 EvalPlanQual recheck；参数化index path避免全量materialize，差分outer/lateral/subquery/self-join。

### DML-04

MERGE Analyzer生成有序action list，支持MATCHED、NOT MATCHED BY TARGET/SOURCE、DELETE/UPDATE/INSERT/NOTHING和RETURNING；Executor每candidate只选择首个true action并在并发变化时EPQ。source流式hash/merge join；差分重复match cardinality violation、trigger/RLS和并发。

### DML-05

RETURNING 是 ModifyTable上方projection，slot同时携带 OLD/NEW/final trigger row并支持 alias、subquery/window所需plan；TupleDesc直接给协议。禁止从打印文本推类型；差分PG18 WITH OLD/NEW、partition/view/ON CONFLICT/MERGE和binary metadata。

### DML-06

实现 `CopyState` 和 incremental text/CSV/binary parser/formatter，支持STDIN/OUT/PROGRAM/FREEZE/ON_ERROR/REJECT_LIMIT/HEADER MATCH/encoding；protocol CopyData有backpressure和CopyFail abort。64KiB～1MiB buffer批量转换/insert但共享constraint；wire/copy文件逐字节比对并测慢客户端/坏行。

### QRY-01

target list 用 typed `TargetEntry`（resno/resname/resjunk/origin），支持任意expression、SRF ProjectSet、whole-row/star expansion和alias scope。TupleDesc在analyze后确定；expression program编译并批量evaluate，差分列名/type/attorigin/SRF交错。

### QRY-02

FROM item统一为RTE，支持LATERAL、table function、ROWS FROM、ordinality、TABLESAMPLE、XMLTABLE/JSON_TABLE；parameter slots显式传递外层值。SRF/scan流式且sample用AM callback，差分null padding、alias descriptor、repeatable seed。

### QRY-03

Join tree保存join type和USING alias；Analyzer合并USING/NATURAL输出并生成coalesce列，Executor正确null extension，parameterized path支持lateral。hash/merge/nestloop共享joinqual/otherqual；全组合差分列顺序、FULL NULL和嵌套scope。

### QRY-04

SubLink分析为SubPlan/InitPlan，外部引用变Param；实现scalar cardinality、EXISTS、IN/ANY/ALL和row compare三值逻辑，安全时转semi/anti join。相关结果可Memoize且键含NULL/collation；差分空集/NULL/多行错误与相关百万行性能。

### QRY-05

CTE保存materialization policy和同snapshot计划；recursive用WorkTable+RecursiveUnion迭代，SEARCH/CYCLE增加隐藏列，data-modifying CTE共享command ID并按PG可见性。tuplestore受work_mem并spill；差分递归终止/循环、执行一次和RETURNING。

### QRY-06

SetOperation tree按grammar precedence分析每支列的common type/collation，UNION/INTERSECT/EXCEPT ALL用计数算法，顶层ORDER/LIMIT绑定正确scope。hash或sort均可spill；差分NULL、duplicate multiplicity、nested parentheses和metadata。

### QRY-07

Expand GROUPING SETS/CUBE/ROLLUP为canonical grouping sets并保留grouping bitmap，Planner选sort/hash/mixed aggregate；functional dependency来自PK/unique catalog。共享transition state减少重复计算并可spill；与PG差分GROUPING bits、ordered/distinct aggregate和空组。

### QRY-08

WindowPlanner把兼容spec分组排序，WindowAgg实现ROWS/RANGE/GROUPS全部bounds/exclusion/peer和inverse transition；named/inherited window在Analyzer展开。partition tuplestore受work_mem并支持外排/滑窗；差分所有frame边界、NULL/collation和大partition。

### QRY-09

DISTINCT用hash或sort unique，DISTINCT ON强制首部pathkeys与ORDER BY匹配并保留每组首行；typed equality遵循NULL/collation。两种实现均spill，差分错误条件、NaN/NULL/nondeterministic collation和稳定结果。

### QRY-10

Sort key保存expression/operator/collation/nulls/direction，使用类型sortsupport；实现bounded heap top-N、incremental sort和外部多路merge。tie不额外保证除非pathkeys要求；EXPLAIN显示方法/内存/磁盘，差分operator USING和大数据排序。

### QRY-11

LockRows节点按RTE执行UPDATE/NO KEY UPDATE/SHARE/KEY SHARE、OF、NOWAIT/SKIP LOCKED，tuple并发变化走EPQ并返回最新可见row。锁批次遵循扫描顺序且及时响应cancel；移植PG rowlocks isolation并验证partition/inheritance。

### QRY-12

Rewriter/Planner统一展开inheritance、partition、ONLY、view和RLS为Append/permission quals；runtime pruning使用Param变化，禁止executor旁路扫描。relcache缓存partition descriptor；差分prepared plan、role/path变更、partition attach和权限。

### QRY-13

把PG statement execution order写成可执行phase machine：snapshot/command counter、rewrite/RLS、BEFORE、constraint、heap/index、AFTER、deferred、RETURNING；rule/view route仍生成ModifyTable。通过trace hook与PG触发函数日志逐步比对，并确保phase本身无逐row catalog lookup。

## 12. 优化器和执行器

### OPT-01

新增 `PlannerInfo/RelOptInfo/Path/ParamPathInfo/EquivalenceClass/PathKey`；各AM/foreign/custom贡献Path，最终选最小cost再create plan，替换直接拼树。Path放planner arena并剪除dominated paths；用PG plan shape corpus和cost单元测试验证。

### OPT-02

小join数用动态规划枚举合法left/right/bushy joinrel，大于阈值使用GEQO；SpecialJoinInfo约束outer/semi/anti顺序。joinrel hash key用relation bitmap，等价path剪枝控制爆炸；TPC-H join order、outer join correctness和planning time设门槛。

### OPT-03

建立canonical predicate/equivalence/proof engine，做constant propagation、implication、join removal、outer reduction和静态/运行时partition pruning；只对immutable/leakproof安全变换。表达式hash-cons避免重复；随机query与关闭优化的结果差分，plan收益基准。

### OPT-04

ANALYZE采用block/tuple reservoir sample，写nullfrac/ndistinct/MCV/histogram/correlation及extended dependencies/MCV/expressions；统计有版本和inheritance/partition合并。采样内存受maintenance_work_mem；与PG统计列和估算误差分布比较。

### OPT-05

selectivity dispatcher调用operator restrict/join和type stats，cost模型计seq/random page、CPU、parallel、cache、collation；缺统计有明确default。estimate结果memoize；以实际rows校准q-error并确保自定义support function能改变plan。

### OPT-06

Path携带required outer relids；NestLoop给inner Param并支持index scan、SubPlan/InitPlan/Memoize。Memoize键是typed datum+collation并按work_mem淘汰/spill策略；差分相关子查询和LATERAL，验证不退化为N次全表扫。

### OPT-07

为Seq/TID/TIDRange/Sample/Function/Values/CTE/WorkTable/Foreign/Custom/Append/MergeAppend定义typed plan/operator、cost、rescan、parallel和instrumentation契约。各scan流式、Append按需打开child；每node都有PG EXPLAIN与结果差分。

### OPT-08

IndexOnlyScan从index tuple返回key+INCLUDE，先查VM all-visible，否则heap fetch并做visibility；page不是all-visible时不能猜。VM buffer缓存，heap fetch按block排序/预取；验证HOT/vacuum并发、heap fetch count和PG EXPLAIN。

### OPT-09

实现block bitmap含exact offsets/lossy pages，AND/OR与range candidate，超过work_mem转lossy/分段spill；BitmapHeap按block排序prefetch并recheck predicate。parallel bitmap使用共享iterator；差分lossy recheck和内存/I/O统计。

### OPT-10

统一 `WorkMemAccount`；sort外排runs、hash join/agg按hash bit分批、window/tuplestore写临时block、skew bucket有限额。temp文件由ResourceOwner删除并支持加密；测试极低work_mem结果不变、cancel无残留、复杂度和spill bytes。

### OPT-11

补IncrementalSort/Memoize/Materialize/Unique/LockRows/ModifyTable/ProjectSet/RecursiveUnion的open/next/rescan/end和参数变化语义；Materialize按随机访问需要选择内存/磁盘。统一instrumentation；逐node移植PG regression和EXPLAIN字段。

### OPT-12

postmaster worker pool分配parallel context、DSM和tuple queue；实现parallel-aware append/bitmap/hash、partial/final aggregate、Gather/GatherMerge、leader participation和错误/取消传播。worker数服从GUC/成本；验证worker crash、snapshot/role安全及扩展性曲线。

### OPT-13

I/O abstraction提供sync/worker/io_uring三种method，AIO queue携带buffer tag/resource owner/interrupt，scan和vacuum发prefetch并按`io_combine_limit`合并相邻block。queue深度受限；对不同method结果差分，记录pg_stat_io/aios和冷缓存吞吐。

### OPT-14

可选LLVM模块将expression和tuple deform编译为typed函数，达到jit_above_cost才触发，失败安全回退解释器；缓存键含plan/type/collation generation。构建无LLVM明确显示off；差分结果/错误并验证编译时间、EXPLAIN JIT和长query收益。

### OPT-15

PlanCache保存analyzed tree、generic/custom plans和ObjectAddress/GUC/role/search_path/RLS dependencies；执行统计决定custom与generic，invalidation只标记相关entry。使用分段LRU和内存限额；差分DDL/ANALYZE/SET ROLE/path改变后的重新规划与Describe metadata。

### OPT-16

每个plan node输出统一ExplainProperty tree，再序列化text/JSON/XML/YAML；支持ANALYZE/VERBOSE/COSTS/BUFFERS/WAL/MEMORY/SETTINGS/SERIALIZE/SUMMARY。instrumentation计时可按开关避免热路径开销；schema/golden输出与PG比对，ANALYZE失败保持error context。

### OPT-17

Session持有atomic InterruptToken，网络cancel/timeout/signal设置它；所有operator loop、lock wait、AIO、spill和worker queue在有界间隔检查。error unwinder按ResourceOwner反序释放pin/file/lock/context；压测取消延迟、无泄漏/脏portal，关闭检查时性能差异可量化。

## 13. 索引和访问方法

### IDX-01

定义 `TableAmRoutine/IndexAmRoutine` fmgr handler及build/insert/delete/scan/rescan/cost/vacuum/validate callback，`pg_am`/handler OID驱动planner/executor，不按名称switch。routine syscache后为直接函数指针；写最小测试AM验证API、事务/WAL/错误安全。

### IDX-02

`pg_opclass/opfamily/amop/amproc` 表示strategy/support/cross-type/collation/sortsupport；Analyzer校验定义，planner/AM按OID调用。cache构建按family generation失效；用自定义cross-type opclass验证search/order/unique和drop dependency。

### IDX-03

把BPTree迁成8KiB buffer page：meta/root/internal/leaf、high key/right link、suffix truncation/dedup，crabbing lock split、page delete/recycle和amcheck。page WAL/FPI+checksum；随机并发insert/delete/scan与PG排序差分，每次crash后amcheck且无整文件rewrite。

### IDX-04

B-tree path builder在前导列低ndistinct且后列有约束时生成skip-scan，executor循环定位下一前缀，成本用multi-column stats。设最大probe防退化；与seq/普通index结果差分并在选择性矩阵验证正确plan和收益。

### IDX-05

Hash AM实现metapage、bucket/overflow bitmap、incremental split、bucket/page lock，key只存hash+TID并recheck equality；support function来自opclass。每结构变化有WAL且vacuum回收overflow；并发倾斜key/crash/amcheck及扩容延迟测试。

### IDX-06

GIN实现entry tree、posting list/tree、pending list/fastupdate、extract/consistent/triConsistent和lossy recheck，支持array/jsonb/tsvector opclass。maintenance_work_mem批量build/cleanup，page WAL；高频term、长posting、并发vacuum/crash与PG结果/plan差分。

### IDX-07

替换flat `.gist` sidecar为buffered balanced GiST，调用consistent/union/compress/decompress/penalty/picksplit/same/distance，支持KNN priority queue。split使用page WAL和right-link恢复；随机range/geometry、bad opclass、concurrent split/vacuum/amcheck验证。

### IDX-08

SP-GiST实现meta/inner/leaf/redirect/dead tuple页和choose/picksplit/inner-consistent/leaf-consistent callback，支持radix/quad/k-d；scan维护search stack并recheck。page级WAL/vacuum；文本prefix/network/point opclass差分及退化树性能。

### IDX-09

BRIN每range保存summary tuple，revmap定位，insert更新或标dirty，autosummarize queue；支持minmax/minmax-multi/bloom/inclusion并可desummarize。summary小且顺序I/O；差分相关/乱序数据、vacuum/concurrent append/crash和false-positive recheck。

### IDX-10

expression/partial index在catalog保存analyzed expression/predicate和dependency，只允许immutable；Planner用predicate proof和结构等价匹配，HOT eligibility检查所有引用列。缓存表达式program；差分函数变更、prepared plan、NULL/collation和错误SQLSTATE。

### IDX-11

index tuple区分key与INCLUDE payload，AM返回TupleDesc；IndexOnlyScan按VM决定heap fallback，included列不参与ordering/unique。大payload受index tuple size规则；验证UPDATE non-key、HOT/VM race、heap fetch统计和binary metadata。

### IDX-12

partitioned index为无storage parent catalog对象，child attach需结构/opclass/collation一致；validity聚合，detach同步更新dependency。unique/exclusion必须覆盖partition key。批量DDL并行child build但原子发布；差分attach invalid index与constraint行为。

### IDX-13

CONCURRENTLY按多事务阶段：catalog invalid→build snapshot→wait writers→validate second scan→wait old snapshots→valid；失败保留invalid可drop/reindex。REINDEX用new relfilenode swap。progress可见且阶段可恢复；高并发DML/kill点/unique race验证。

### IDX-14

每AM实现bulkdelete/vacuumcleanup/page deletion和统计，维护pending/recycle安全horizon；提供amcheck结构/parent-link/key-order/heap-TID检查和REINDEX恢复建议。vacuum按maintenance_work_mem批量TID；损坏注入、progress/pg_stat和并发snapshot测试。

### IDX-15

所有AM只通过BufferManager/Smgr/WAL/TDE/backup block interface读写，禁止独立fstream sidecar；DDL注册pending file，DML page LSN，reindex原子swap。静态检查阻止access模块直接文件I/O；逐AM跑加密、备份、恢复、PITR和WAL retention矩阵。

## 14. 事务、MVCC、锁和 VACUUM

### TXN-01

TransactionManager将 READ UNCOMMITTED映射READ COMMITTED；每条statement开始取新snapshot，CommandCounterIncrement控制同事务命令可见性，tuple visibility读取xmin/xmax/cid。snapshot从ProcArray一次复制并缓存到statement；移植PG mvcc/isolation用例，比对游标、函数内多命令和ReadyForQuery。

### TXN-02

RR在首个非事务控制statement固定snapshot，Serializable在其上注册SSI；read-only/deferrable等待safe snapshot并使用PG相同限制/SQLSTATE。snapshot wait用condition variable非轮询；验证write skew、只读异常、BEGIN options组合和长事务horizon。

### TXN-03

完成HeapTupleHeader的xmin/xmax/cmin/cmax、combo CID、ctid、infomask hint、HOT/redirect/dead line pointer规则；可见性集中于 `HeapTupleSatisfies*`，所有AM/maintenance复用。hint bit写受content lock且可不WAL；真值表、HOT chain并发和page corruption测试覆盖。

### TXN-04

新增SubTrans SLRU和SubTransaction stack，记录parent SubXID、command/resource owner、locks和state；overflow后snapshot按top xid+pg_subtrans判定。子事务commit把资源转父owner，abort局部释放；验证深嵌套、exception block、overflow/crash和性能上界。

### TXN-05

每个savepoint保存subtransaction、catalog/file pending action、deferred event、portal/notify/listen和GUC watermark；ROLLBACK TO创建新的同名savepoint状态并释放之后资源，sequence非事务行为按PG保留。watermark O(1) rollback加局部undo；逐资源差分和重复savepoint测试。

### TXN-06

PREPARE把xid/subxid、locks、invalidation、notify、pending files、deferred state写两阶段文件并WAL flush后从backend detach；启动恢复到shared prepared array，任意授权backend可COMMIT/ROLLBACK。文件有CRC/version并限制大小；crash每阶段、duplicate gid、max_prepared和备份恢复验证。

### TXN-07

Heavyweight LockManager实现PG locktag、8种table mode、page/tuple/transaction/advisory/object locks、fast path、FIFO wait queue和deadlock wait-for graph含soft edge reorder。锁表分区，wait用latch；移植deadlock/isolation并测高并发无单mutex瓶颈。

### TXN-08

tuple lock用xmax+infomask/MultiXact表达key-share/share/no-key-update/update，等待transaction/MultiXact后重读tuple；update/delete/unique/FK/EPQ共享接口。MultiXact成员SLRU+cache；全冲突矩阵、lock upgrade、HOT update、crash wrap测试。

### TXN-09

PredicateLockManager支持relation/page/tuple及B-tree/各AM逻辑range，空scan锁gap；超过per-xact/per-relation阈值安全升级。锁hash分区且只记录Serializable；用PG isolation和随机history验证phantom/false positive仅导致40001、不允许漏冲突。

### TXN-10

advisory lock key规范化为(dbOid,key64)或(dbOid,key1,key2)，支持session/xact、shared/exclusive、blocking/try；复用heavy lock并在session/resource owner清理。验证两个key namespace不碰撞、savepoint/commit/disconnect和pg_locks字段。

### TXN-11

commit顺序固定：precommit checks→WAL records→commit record→按synchronous_commit flush/wait replica→CLOG/procarray visible→notify/invalidation→reply；abort不需flush但清资源。group commit leader/follower合并flush，记录commit timestamp可选；kill-point和延迟分布验证。

### TXN-12

LISTEN状态在commit生效，NOTIFY在commit后写shared ring/SLRU queue，payload按UTF-8和长度验证；backend在查询间和可中断点发送NotificationResponse，rollback丢弃。queue按最慢listener回收并告警；跨backend、queue full、2PC和协议时序差分。

### VAC-01

Vacuum按page prune HOT/dead tuple、收集dead TID批量index cleanup、freeze、设置VM all-visible/all-frozen和安全truncate；failsafe在接近wrap时跳非关键工作。maintenance_work_mem限制TID batch；移植vacuum tests并验证old snapshot、slot horizon和crash。

### VAC-02

postmaster启动autovacuum launcher，按n_dead_tup/insert/scale factor/freeze age选表并派worker；支持per-table options、cost delay、worker slots和anti-wraparound抢占/取消冲突。调度用priority queue；长期混合负载验证bloat、wrap保护和前台p99。

### VAC-03

VACUUM FULL/CLUSTER创建新relfilenode、按snapshot复制live rows并重建index，短锁原子swap，失败回滚；VACUUM/ANALYZE option由typed utility解析，progress共享槽更新。copy流式/parallel build；逐option差分及swap kill-point验证。

### VAC-04

HOT eligibility查询relcache汇总的所有index key/expression/predicate依赖列且新tuple能放同page；prune遵循oldest xmin并维护redirect/ctid链，VM失效与page WAL同原子操作。依赖bitmap缓存；并发表达式/partial index下做heap-index cross-check和链长性能测试。

## 15. 存储、WAL、checkpoint 和恢复

### STO-01

新增双副本 `ControlFile`，包含magic、format/catalog version、system identifier、block/WAL segment size、endianness、checksum/TDE flags、checkpoint/timeline并带CRC；启动不匹配即拒绝。更新采用write+fsync+原子rename+目录fsync；损坏一/两副本、跨版本和错误平台测试。

### STO-02

用 `RelFileLocator{spcOid,dbOid,relNumber}` 和 fork(main/fsm/vm/init)统一Smgr，relation按固定segment blocks切文件，temp加backend id。旧名称只在upgrader解析；locator直算path避免catalog查找，验证tablespace/database move、segment boundary和备份清单。

### STO-03

定义versioned 8KiB PageHeader（LSN/checksum/flags/lower/upper/special）、ItemId和aligned tuple/varlena/toast pointer；页初始化记录version，读取严格校验bounds。tuple deform缓存offset，固定列快速路径；fuzz任意page bytes、torn page和跨端序upgrade。

### STO-04

BufferPool提升为共享buffer table：partitioned tag hash、pin/usage、content/io locks、I/O-in-progress wait、clock sweep、prefetch和bulk ring；ResourceOwner记录pin，异常自动unpin。读写锁不跨I/O持有；并发同页单次读取、eviction race、worker crash和hit-rate/scaling测试。

### STO-05

FSM/VM作为relation fork经Buffer/WAL管理；FSM tree近似空闲空间，VM两个bit为all-visible/all-frozen，heap修改先清VM，vacuum设置前保证heap/index状态。损坏可离线/启动重建；index-only/vacuum/crash差分并测FSM避免全表找空页。

### STO-06

实现1/4-byte varlena、pglz/lz4 compressed和external toast pointer，按PLAIN/MAIN/EXTERNAL/EXTENDED与toast_tuple_target选择；toast relation分chunk并有index，update复用未变datum，delete/vacuum清理。detoast支持slice/stream，验证超大value、崩溃、logical/backup和压缩收益。

### WAL-01

在 `src/wal/rmgr` 为xact/heap/heap2/btree/hash/gin/gist/spgist/brin/smgr/catalog/fsm/vm/toast/multixact/sequence/standby/slot定义typed record、describe、redo；所有mutation API要求WAL context。构建期覆盖检查防未注册资源，逐rmgr golden decode和redo幂等测试。

### WAL-02

WALInsertLock分区预留LSN并并发copy到WAL buffers，record支持block reference、FPI、compression、continuation和CRC；page LSN在record insert后设置，segment switch/preallocate/recycle遵循timeline。group flush保持，压测writers扩展和跨segment/torn record恢复。

### WAL-03

Checkpointer记录redo horizon后分批刷当时dirty buffers，completion更新control file再回收WAL；bgwriter平滑写脏页，checkpoint_completion_target节流。dirty queue按LSN/年龄调度；kill任意阶段恢复，监测checkpoint latency、backend fsync和WAL retention。

### WAL-04

StartupRecovery读取checkpoint/timeline history，redo到一致点建立standby snapshot；checkpoint期间做restartpoint，冲突按lock/snapshot/buffer pin/deadlock/tablespace处理。promotion写end-of-recovery record和新timeline；多timeline、pause/resume、hot query与冲突测试。

### WAL-05

生产恢复改为ARIES式物理/逻辑redo：只重放page LSN较旧的record，未提交tuple由MVCC隐藏并由vacuum清理；移除before-image undo作为一致性依赖。若保留仅用于测试/flashback须隔离格式。steal/no-force、并发checkpoint每个write窗口经model/crash test证明。

### WAL-06

所有page含基于block number的checksum，WAL/meta/control/manifest各自CRC；`dbms_checksums --check/--enable/--disable` 在离线或安全在线流程重写并记录进度。Buffer load集中校验避免重复CPU；损坏定位到relation/fork/block并验证工具可续跑。

### WAL-07

封装DurableFile API明确create/write/fsync/rename/link/unlink/dir-fsync顺序和允许错误；tablespace跨设备不用rename假设，partial write循环并保留dirty。ext4/XFS和dm-flakey/powercut matrix验证；批量目录fsync降低DDL开销但不越过commit边界。

### WAL-08

恢复状态机分别处理unlogged init reset、temp delete、2PC restore、sequence log-ahead、pending DDL file action和slot xmin/restartLSN；每类有control/catalog marker防重复。启动批处理清理；每类在commit前后kill验证，且不扫描无关data file。

### STO-07

large object用 `pg_largeobject_metadata/pg_largeobject` catalog+chunk heap，ACL/owner/dependency/transaction统一；实现64-bit seek/read/write/truncate、lo_*函数和libpq fast-path protocol。chunk index支持流式随机访问，不全量载入；差分权限、savepoint、vacuum、dump/restore。

### STO-08

用OpenSSL EVP或libsodium AEAD封装envelope encryption：cluster KMS master→per-db/data key→page/WAL/temp/backup object nonce+tag；key catalog只存wrapped key并支持在线rotation generation。TDE在Buffer/Smgr层覆盖所有fork/AM，保留checksum语义；外部密码学审计、lost/old key恢复、性能/AES-NI测试后才能发布。

## 16. 复制、高可用和备份

### REPL-01

protocol replication startup建立WalSender，支持IDENTIFY_SYSTEM/TIMELINE_HISTORY/START_REPLICATION；按LSN从WAL segment流CopyData并发keepalive，接收standby status feedback。WalReceiver写临时segment、fsync后原子发布；bounded send buffer和零拷贝，golden wire/断线续传测试。

### REPL-02

standby由StartupRecovery持续replay，ProcArray发布recovery snapshot并只允许read-only；记录KnownAssignedXids和running-xacts WAL，冲突按max_standby_*取消，feedback反馈xmin。apply和query共享buffer content lock；延迟/长查询/bloat权衡及一致性点测试。

### REPL-03

slot作为持久化目录+catalog object，双文件CRC原子保存type/plugin/restart/confirmed/xmin/catalog_xmin/two_phase/failover/sync；WAL回收和vacuum horizon读取最小值。状态更新节流但关键advance先fsync；crash、磁盘满、inactive retention和copy/sync验证。

### REPL-04

WalSender维护每standby write/flush/apply LSN，SyncRepConfig解析FIRST/ANY quorum并动态选sync standbys；commit waiter按目标LSN/mode入有序队列，配置/断线唤醒重算。批量唤醒降低锁开销；差分remote_write/flush/apply、超时和p99延迟。

### REPL-05

standby可作为sender形成cascading，receiver按timeline history follow；promotion加timeline且fence旧primary，`dbms_rewind`用page checksums/WAL找divergence并复制changed blocks。文档定义外部STONITH边界；网络分区/双promote/rejoin演练验证不静默分叉。

### REPL-06

删除DML旁路采集作为真源；heap/catalog/transaction WAL带logical identity和commit ordering，LogicalDecodingContext从restart LSN读WAL，经ReorderBuffer按xid/subxid组装，只在commit输出。decode不阻塞WAL writer；结果与写路径无关且crash重启不丢不重。

### REPL-07

实现pgoutput v1-v4 message codec：Begin/Commit/Relation/Type/Insert/Update/Delete/Truncate/Origin/Stream/2PC，按publication和binary option发送精确type/replica identity。relation metadata cache按schema invalidation；使用PG subscriber/decoder双向golden wire和large transaction测试。

### REPL-08

publication catalog保存all tables、schema、table column list、row filter和publish actions/root option；Analyzer验证filter/column，decoder在正确old/new tuple上评估并处理partition root映射。compiled filter缓存；ALTER立即对新change生效，差分PG publication视图和输出。

### REPL-09

postmaster管理launcher/apply/table-sync workers；CREATE SUBSCRIPTION建slot/conninfo/origin，initial copy取得一致snapshot和start LSN，再apply，支持disable/refresh/skip/conflict/2PC/failover。apply批量transaction但保留commit边界；断线、schema change、unique conflict和resume验证。

### REPL-10

定义LogicalOutputPlugin callbacks（startup/begin/change/commit/filter/shutdown）和DecodingContext；ReorderBuffer按work_mem将大事务spill为checksummed segments，支持snapshot export和subxid/TOAST重组。plugin在受限memory/resource owner运行；crash/cancel/plugin error清理和顺序验证。

### BACKUP-01

BaseBackup API执行checkpoint并写backup-start WAL，返回backup_label/tablespace_map，遍历snapshot文件集合流式发送并在stop写end WAL，选fetch/stream WAL且支持rate limit。relation segment读取处理并发truncate规则；持续DML下恢复和慢客户端测试。

### BACKUP-02

生成PG兼容概念的manifest：文件path/size/mtime/checksum、WAL range/system id；verify先校验manifest自身再并行校验文件并输出精确missing/extra/corrupt诊断。I/O限速和分块hash；随机bit flip/truncate/rename、加密备份和退出码测试。

### BACKUP-03

checkpointer/WAL维护block change summary，incremental backup只发base LSN后changed blocks+new/deleted files；combine工具验证chain/system id后重建full image并生成新manifest。summary分段/压缩，避免扫描全数据；多级chain、missing base、timeline change和结果hash验证。

### BACKUP-04

启动读取recovery.signal/standby.signal和typed recovery GUC，支持name/time/xid/LSN/immediate、inclusive、promote/shutdown/pause；replay在transaction boundary判断target并持续可观测。restore_command有重试/超时；每种target与PG恢复点差分。

### BACKUP-05

archive按timeline WAL文件和.history管理，archive_command使用受控placeholder expansion且不经不必要shell，失败保留.ready并指数退避；cleanup只删不再被backup/slot/timeline需要的segment。多timeline restore/promote/retry/命令注入安全测试。

### BACKUP-06

优先让真实pg_dump通过完整protocol/catalog工作；实现缺失的catalog query、COPY、large object和ACL/owner，验证plain/custom/directory archive，再让pg_restore并行按TOC dependency执行。对象deparse使用catalog OID；与PG dump做语义规范化diff和跨库round-trip。

### BACKUP-07

实现rewind后自动生成standby配置/slot关联和追赶检查，提供promote/rejoin runbook、dry-run和不可rewind原因；每次发布执行自动HA演练并记录RPO/RTO。changed-block并行复制且限速；故障期间transaction ledger证明零/已声明范围内丢失和无双写。

## 17. PostgreSQL 协议和客户端兼容

### PROTO-01

把PostgresProtocol改成startup/auth/query有限状态机，协商3.0/3.2和不支持minor，保存application_name/client_encoding/options/replication；逐项发送PG相同ParameterStatus、BackendKeyData和事务ReadyForQuery。参数表来自GUC，golden packet和非法长度fuzz验证。

### PROTO-02

Simple Query用grammar-aware splitter一次parse整串，在idle时包implicit transaction；一个statement失败后跳余下并最终ReadyForQuery，空串发EmptyQueryResponse，tag来自ExecutionResult。逐statement流式结果；多语句/DDL/error/explicit txn packet序列与PG逐帧比对。

### PROTO-03

完整Parse/Bind/Describe/Execute/Close/Flush/Sync状态机；Bind保存原始typed params，Portal流式游标，maxRows到限额发PortalSuspended，error后忽略到Sync，pipeline维护sync groups。portal不物化结果；libpq pipeline和随机message sequence model test验证。

### PROTO-04

RowDescription直接从TupleDesc和TargetEntry origin填name/tableOid/attnum/typeOid/typlen/typmod/format；DataRow逐Value调用send/output，NULL=-1。descriptor在Parse/Describe即可获得时稳定；覆盖join/expression/domain/array/RETURNING和binary混合format的wire bytes。

### PROTO-05

每个TypeCodec注册send/receive，Bind和DataRow按列format独立选择；unknown/user type通过pg_type fmgr，不支持binary时按PG错误而非猜text。编码buffer预估长度并scatter/gather；全部built-in/array/range/record binary用PG producer/consumer互测。

### PROTO-06

实现CopyIn/Out/BothResponse、CopyData/Done/Fail和CopyState连接；网络读写用bounded queue与socket backpressure，CopyFail使statement/transaction进入PG相同状态。支持混合text/csv/binary和flush；pcap、慢端、半包/断线、GB级流内存测试。

### PROTO-07

postmaster维护backend PID/128-bit secret映射，CancelRequest校验后只置目标InterruptToken且不回包；锁/AIO/worker/executor传播。secret启动随机且退出清除；伪造/旧key/并发cancel、阻塞锁和长函数取消延迟测试。

### PROTO-08

DbError编码完整S/V/C/M/D/H/P/p/q/W/s/t/c/d/n/F/L/R字段；Notice和Notification独立异步队列，在协议允许边界发送并保序。限制单消息/队列大小防DoS；与PG错误golden、notice during query和通知交错比对。

### PROTO-09

raw socket先处理SSLRequest/GSSENCRequest，再创建TLS socket；OpenSSL配置server cert/key/CA/CRL、SNI、client cert、min/max protocol/cipher和SCRAM-PLUS channel binding，reload用新context原子换。sslmode矩阵、证书轮换、恶意握手和TLS性能测试。

### PROTO-10

startup `replication=database/true` 切到ReplicationCommand parser，仅允许IDENTIFY_SYSTEM、TIMELINE_HISTORY、CREATE/DROP_REPLICATION_SLOT、START_REPLICATION等，CopyBoth承载physical/pgoutput。与普通SQL权限/状态隔离；真实pg_receivewal/pg_recvlogical互操作验证。

### PROTO-11

NetworkServer支持Unix socket目录/权限、IPv4/IPv6 bind、keepalive/user timeout和listen reload；client_encoding在SourceText/codec边界转换，lc_*为session GUC。event loop避免慢连接占worker；多地址、非UTF8、socket cleanup和连接风暴测试。

### PROTO-12

CI矩阵固定libpq/psql/JDBC/Npgsql/psycopg/pgx及SQLAlchemy/Django/Hibernate版本，跑connect/auth/prepare/txn/COPY/introspection/migration/pool/error/cancel。保存失败packet和server trace；每次协议/catalog改动必跑，项目专用driver workaround视为失败。

### CLIENT-01

不重写psql；以协议/catalog兼容为主，让官方psql的describe、变量、脚本ON_ERROR_STOP、COPY、password、watch等工作。缺服务端能力按对应项实现；CI运行psql regression scripts并比对stdout/stderr/exit code，输出格式不由服务端伪造。

### CLIENT-02

提供薄CLI `dbms_init/ctl/ready/bench` 或让PG工具直接工作：createdb/dropdb/createuser等依赖协议即可复用；vacuumdb/reindexdb/clusterdb/pg_isready/pgbench列入互操作。工具遵守data-dir lock、exit code和machine-readable输出；端到端安装/服务/失败测试。

## 18. 安全、认证和权限

### SEC-01

pg_hba parser支持local/host/hostssl/hostnossl/hostgssenc/hostnogssenc、CIDR/samehost/samenet、database/role列表/replication、include/include_dir/map/options；配置parse成不可变rule set，SIGHUP成功才原子替换。逐行error position和PG hba matrix差分。

### SEC-02

定义AuthenticationProvider接口并分模块实现peer/ident/cert/LDAP/PAM/RADIUS/GSS/SSPI/BSD/OAuth；构建/平台不支持的provider在配置load时报明确错误，绝不降级password。认证I/O有timeout/rate limit；用真实服务容器、失败/回退/审计测试验收。

### SEC-03

扩展SCRAM实现SHA-256-PLUS、tls-server-end-point binding、可配置iteration和server nonce；verifier只存SASL string，ALTER ROLE按password_encryption生成并支持轮换。使用常数时间比较和成熟crypto；RFC/PG/libpq互操作、downgrade和credential日志泄漏测试。

### SEC-04

发布构建强制OpenSSL且TLS stub仅测试可用；GUC控制cert/key/CA/CRL/cipher/protocol/passphrase，SIGHUP原子重载，pg_stat_ssl暴露version/cipher/bits/client DN。OCSP按明确策略实现；CI扫描弱协议/cipher并做并发握手基准。

### SEC-05

`pg_auth_members`保存role/member/grantor及admin/inherit/set options，授权检查图遍历防循环并尊重session SET ROLE；DROP/REASSIGN OWNED走dependency。membership closure按role generation缓存；PG18 GRANT ROLE组合、grant chain revoke和并发变更差分。

### SEC-06

ACLItem用grantee/grantor/privilege+grant-option bitmap，AclCheck按对象类型和PUBLIC/owner/superuser/predefined role处理database到parameter全集及column fallback。ACL cache按object/role invalidation；每privilege/对象/命令正负矩阵和error message验证。

### SEC-07

`pg_default_acl`以owner/namespace/object type为键，在CREATE时合并硬编码default后写对象ACL，ALTER DEFAULT PRIVILEGES事务化并有dependency。lookup命中syscache；差分多owner/schema、FUNCTION/ROUTINE、large object和dump/restore。

### SEC-08

policy catalog在Rewriter按command/role注入USING/WITH CHECK，owner/bypass/force规则在统一security context判定；partition/inheritance各child映射列，prepared plan依赖policy/role。leakproof qualifier排序受安全屏障；恶意函数、role切换和旁路入口验证。

### SEC-09

fmgr调用建立SecurityContextFrame，分别跟踪session/current user、SECURITY DEFINER、proconfig和受控search_path；任何return/error/subtransaction都RAII恢复。函数计划缓存键含effective role/path；对象劫持、异常、nested definer和SET LOCAL差分。

### SEC-10

view RTE标记security_barrier/invoker和securityQuals，Planner只有leakproof qual可越过barrier，RLS qual同理；function metadata来自pg_proc且只有superuser可设leakproof。用side-channel counter函数验证无提前求值，并对plan/result与PG比对。

### SEC-11

SecurityLabelProvider API校验/应用label并在object access hook执行MAC decision；未加载provider时按PG规则保存/拒绝对应provider而不声称MAC。sepgsql需独立SELinux集成与审计；label lookup cache受invalidation，策略正负测试证明强制执行。

### SEC-12

集中 `PrivilegedOperation` 检查COPY PROGRAM/文件、server file roles、large object、extension/LOAD和目录边界；文件使用openat+canonical root+O_NOFOLLOW，PROGRAM参数不拼shell。审计记录脱敏；路径穿越/symlink/command injection和预定义角色矩阵。

### SEC-13

AuditEvent是结构化不可变record，写append-only受限目录或远端sink，包含session/role/object/result/SQLSTATE且参数按policy脱敏；异步bounded queue在overflow按配置阻塞或fail-closed并告警。rotation/retention签名链可选；篡改、磁盘满和敏感数据测试，同时文档明确它是项目扩展。

## 19. 系统目录、information_schema、监控和运维

### MON-01

从PG18 catalog descriptor生成 `pg_catalog` relation/view/function兼容清单，真实catalog列保持OID/type/attnum，view以SQL定义并执行ACL过滤；项目额外列放独立schema。syscache/普通planner执行避免手工分支；用psql `\d`、pg_dump和逐view schema/result差分关闭。

### MON-02

导入SQL标准information_schema view定义并适配真实catalog，不再由 `queryInformationSchema` 硬编码；所有view按current user过滤并返回标准domain/identifier类型。planner可内联安全view；逐表column descriptor和多角色结果与PG比较。

### MON-03

建立共享StatsShmem分片计数器和持久snapshot文件，支持transactional/nontransactional snapshot语义、reset timestamp和track_*；覆盖table/index/function/SLRU/WAL/checkpointer/bgwriter/I/O。热计数per-backend/per-CPU聚合降低cacheline竞争；crash/reset/权限和开销基准。

### MON-04

BackendStatus array由每backend seqlock更新pid/datid/user/state/query_id/xact/query times/wait/backend type/client/leader；reader取一致快照并按角色脱敏query。query文本放bounded shared buffer；状态转移、并行worker、idle-in-xact和权限与PG差分。

### MON-05

LockManager将完整LockTag/mode/granted/fastpath/waitstart和predicate lock导出只读snapshot，事务/virtualxid/object/page/tuple/advisory均映射PG列。分批复制避免长持锁；构造每种lock和wait queue，与pg_locks行及blocking函数差分。

### MON-06

WalSender/Receiver/Archiver/Slot/Subscription/Recovery/TLS/GSS各维护typed stats provider，catalog view统一读取并按权限隐藏conninfo。计数原子、低频字段seqlock；运行复制/归档/SSL场景逐列与PG视图结构和状态变化比对。

### MON-07

Buffer/AIO/WAL/SLRU/MemoryContext在操作边界打统一I/O event，聚合到backend type/object/context/read-write-extend/fsync timing；实现pg_stat_io、pg_aios、wal/checkpointer/slru和memory context views。采样/计时可配置，关闭时热路径开销设<1%；计数守恒测试。

### MON-08

为ANALYZE/VACUUM/CREATE INDEX/CLUSTER/COPY/base backup定义固定progress slot和phase enum，worker按节流频率更新blocks/tuples/bytes；退出/error清slot。更新不逐tuple做共享锁；各phase故障/取消与PG progress列、单位和生命周期差分。

### MON-09

parser生成query jumble/queryid，SqlStats扩展plans/calls/rows/time/block/temp/WAL/JIT/parallel和minmax；共享hash按queryid+db+user，容量满用clock/LRU淘汰，文本单独arena。reset权限和时间原子；跑pg_stat_statements regression、并发计数和开销基准。

### MON-10

LoggingCollector从backend pipe接收结构化LogRecord并输出stderr/csvlog/jsonlog/syslog，支持rotation、line prefix、verbosity、statement/duration/sampling；超大消息分帧重组。队列有背压/丢失计数且敏感参数脱敏；格式golden、rotate/crash/disk full测试。

### MON-11

在EXT完成前，auto_explain/pg_buffercache明确为内置compat provider但保持extension相同schema/权限；EXT完成后改标准control/SQL库实现。auto_explain复用Explain tree，buffercache取Buffer snapshot；extension install/drop、字段和开销与PG比较。

### OPS-01

Config重构为GUC registry，记录type/unit/context/source/default/reset/pending_restart和assign/check/show hook；启动显式 `-D`/env定位data dir，control file决定cluster状态，禁止依赖CWD。配置层级与PG一致且SIGHUP原子；路径/来源/错误差分。

### OPS-02

`SHOW/pg_settings`直接查询GUC registry，输出name/setting/unit/category/short_desc/extra_desc/context/vartype/source/min/max/enum/boot/reset/pending_restart；敏感项权限过滤。registry索引O(1)；逐GUC descriptor golden和ALTER SYSTEM/SET/RESET行为测试。

### OPS-03

postmaster处理SIGHUP、smart/fast/immediate shutdown、startup PID/lock和child supervision：smart等session、fast取消事务、immediate直接退出后强制recovery。signal handler只写self-pipe/latch；双启动、每阶段信号、backend crash loop和PID复用测试。

### OPS-04

init工具创建data/WAL/log/temp/archive目录并校验owner/mode/umask，服务用户不可被其他用户读写；所有路径由DataDir对象解析，tablespace/归档明确跨设备。安装包提供systemd/container约定；权限篡改、只读目录、symlink和升级测试。

### OPS-05

ResourceGovernor监控free space/inode/FD/memory/CPU/I/O/connections/output queue，设reserved WAL/control FD和superuser connection slots；阈值触发拒绝新work、取消temp-heavy query或告警，不破坏active commit。压力/慢读/ENOSPC/EMFILE/OOM测试证明fail-safe与恢复。

## 20. 扩展、FDW、过程语言和生态

### EXT-01

ExtensionManager解析`.control`，按version graph在指定schema事务执行install/update SQL，写pg_extension和member dependency；relocatable检查对象资格，dump只输出CREATE EXTENSION及config tables。脚本有受控search_path；升级/回滚/并发安装和常用extension control corpus测试。

### EXT-02

定义稳定fmgr C ABI、Datum/NullableDatum、FunctionCallInfo、PG_FUNCTION_INFO和MemoryContext/error trampoline；动态库用dlopen/dlsym但校验ABI/version、路径/权限，backend退出卸载。调用fast path避免虚函数；C/C++测试扩展覆盖NULL/error/interrupt/leak并做ABI兼容策略。

### EXT-03

CREATE TYPE关联input/output/receive/send/typmod/analyze/subscript handler，composite/range/multirange复用catalog descriptor；varlena/datum ownership写入ABI。Planner调用自定义analyze，协议调用codec；测试extension自定义base+array+range的heap/index/COPY/binary/dump。

### EXT-04

function/procedure/aggregate/operator/cast/collation/conversion对象全部写真实catalog并由Analyzer/fmgr/Planner执行，删除compat record；定义/alter/drop触发dependency/invalidation。lookup走syscache；编写一个extension组合所有对象，验证schema visibility、plan变化、错误和restore。

### EXT-05

对外暴露IDX-01/02的Table/Index AM handler和WAL generic page API，validator检查callback全集/opclass；extension AM只能经Buffer/Smgr/WAL写page。generic WAL记录页diff/FPI且有大小限制；测试AM build/scan/vacuum/crash/upgrade。

### EXT-06

HookRegistry按load order提供planner/executor/utility/object access/emit log和CustomScan methods，调用链支持next hook并在error时resource owner清理；hook generation进入plan cache key。空hook为零/极低分支开销；多hook顺序、卸载禁止、并行安全和异常测试。

### EXT-07

实现shared memory allocator、named LWLock tranche、DSM segment/TOC和shared/session preload生命周期；postmaster启动前确定固定shmem，DSM运行时创建并由resource owner回收。锁统计/死锁规则明确；扩展跨backend counter、worker crash和容量/安全测试。

### EXT-08

BackgroundWorker注册name/flags/start/restart time/library/function，postmaster按时机fork并监督signal/restart；worker可安全初始化DB connection和shmem。crash频率退避防fork bomb；动态/静态worker、shutdown、upgrade和权限测试。

### EXT-09

`pg_language`关联call/inline/validator handler，CREATE FUNCTION调用validator，执行经fmgr/SPI；先完整plpgsql，再为PL/Python/Perl/Tcl定义可信/非可信sandbox边界和可选包。语言runtime有per-backend cache/内存限额；官方语言regression及异常/事务/安全测试。

### FDW-01

实现FdwRoutine handler/validator和GetForeignRelSize/Paths/Plan、Begin/Iterate/ReScan/End scan、modify/direct modify、transaction callbacks；server/user mapping/foreign table options来自catalog并权限过滤。批量fetch/insert和连接复用；最小mock FDW与postgres_fdw互操作测试。

### FDW-02

IMPORT FOREIGN SCHEMA调用FDW callback生成CREATE FOREIGN TABLE AST并在一个事务执行；Planner支持parameterized/join/aggregate/async paths和remote quals，EXPLAIN脱敏remote SQL。async event loop有backpressure；远端错误/事务/savepoint/cancel和pushdown result差分。

### EXT-10

将logical output plugin、archive module、OAuth validator和injection point都建成versioned callback ABI，明确process/context/interrupt/thread约束；配置只加载受信路径。热callback直接函数指针；分别提供reference module与生命周期、错误、reload、升级测试。

### EXT-11

按依赖顺序建立真实互操作门：先pg_stat_statements/auto_explain（hooks），再pg_trgm/btree_gin/gist（type/opclass），hstore/citext/uuid-ossp（fmgr/type），最后postgres_fdw（FDW）。每个使用上游源码未经项目patch构建，跑官方regression、dump/upgrade和性能；未通过不得列兼容。

## 21. 工程质量、发布和生产化

### ENG-01

生成 `docs/generated/feature-status.md`，唯一来源为compat manifest和当次机器报告；README/RELEASE/CHANGELOG只引用，不手填PASS数。CI校验文档链接、版本和声明不漂移；历史文档顶端标日期/基线/已替代，禁止作为当前证据。

### ENG-02

建立单一`VERSION`文件或CMake project version，构建生成`version.h`、`--version`、协议server_version、包名和文档变量；删除手写常量。CI断言全部输出等于manifest，release tag必须匹配且dirty build带明确后缀。

### ENG-03

release pipeline从clean tag在固定GCC/Clang、dependency lock和真实OpenSSL容器构建，运行unit/regression/isolation/protocol/crash/compat并产JUnit/JSON、编译器和环境指纹。制品只在所有门通过后签名；报告长期归档且可复现。

### ENG-04

接入SQLLogicTest、PG regression/isolation schedule转换器、ORM suite和grammar-based differential generator；每个bug先加最小case。按feature manifest分片并固定seed，失败保存数据库和query；结果规范化只允许声明字段。

### ENG-05

为Lexer/Parser/Expr/Protocol/WAL/Page/Catalog/Backup提供libFuzzer/AFL入口和结构化mutator，seed来自生产/PG corpus，crash自动minimize并进回归。每target设内存/时间，sanitizer常开；覆盖率和无新crash时长是release指标。

### ENG-06

所有durable/alloc/thread/lock操作经可注入wrapper并有稳定point ID；runner枚举errno/partial write/power cut，重启后验证control/WAL/catalog/heap-index约束。采用状态空间削减但关键commit/DDL/checkpoint组合全覆盖；生成kill-point覆盖报告。

### ENG-07

CI矩阵运行ASAN/UBSAN/LSAN、TSAN独立job、debug assertions、O0/O2/O3、GCC/Clang；磁盘格式以显式字节序支持不同主机，32-bit不支持则control/config明确拒绝。任何suppression需owner/expiry，race/UB为release blocker。

### ENG-08

test hook记录transaction invocation/response和real-time order，导出Elle/Jepsen checker可读history；覆盖RC/RR/Serializable、DDL、failover和network fault。generator聚焦冲突key但周期加入全随机；发现nonserializable history保存最小schedule。

### ENG-09

建立24h/7d soak：连接/锁风暴、checkpoint/vacuum/backup/DDL/DML/replication混合并注入慢/满/错误磁盘；持续采集p99、RSS、FD、bloat、WAL、lag和invariant。阈值自动失败，结束执行amcheck/manifest/ledger核对，无“只有crash才算失败”。

### ENG-10

同机同FS/配置运行pgbench、TPC-C/H/DS，固定数据、warm/cold cache、client数，记录TPS/p50/95/99、CPU/RSS/I/O/WAL/temp/plan。结果存版本化JSON并做统计置信区间；性能回归预算按模块，功能不得靠禁用durability取胜。

### ENG-11

维护N-2 data-directory fixtures和catalog migration chain，CI做正常/崩溃中断upgrade、续跑/rollback、升级后crash recovery及逻辑downgrade（dump restore）。每format变更必须给converter和compat test；无法降级须在check阶段明确。

### ENG-12

为支持平台产deb/rpm/tar/container，生成CycloneDX/SPDX SBOM，锁依赖并扫描CVE、签名制品/provenance、验证reproducible build。容器不以root运行且volume权限正确；安装/升级/卸载不得删除data dir，供应链策略进release gate。

### ENG-13

每个release执行threat model和代码审查，覆盖crypto/TLS/auth/protocol/parser/ACL/RLS/path/extension/backup/DoS/log；使用SAST、dependency scan、fuzz和外部渗透。发现项带severity/SLA，critical/high未关闭不发布；安全测试不输出secret。

### ENG-14

定义支持平台/workload/容量、可用性与数据持久SLA、RPO/RTO和明确限制；编写故障诊断、WAL/archive损坏、backup restore、failover/rejoin、key recovery和incident流程。季度演练用自动证据验证手册可执行，不能仅文档评审。

### ENG-15

正式选择“PG18行为兼容”为默认目标；项目扩展置 `compatibility_mode=extended` 和独立schema/命令空间。manifest为每项标exact/extension/unsupported，server启动和version输出不冒充PostgreSQL二进制fork；发布声明必须匹配P0-16证据。

## 22. 非 PostgreSQL 语法和行为偏移

这一组在审计中补充稳定 ID `DIV-01`～`DIV-14`。处理原则统一为：默认 `compatibility_mode=postgresql18` 时使用PG grammar/SQLSTATE；扩展只能在显式 `extended` 模式启用，`SHOW dbms.extensions` 可审计当前偏移。mode是session-start/DB级受限配置，不能在prepared statement执行中偷偷改变语义。

### DIV-01（USE DATABASE）

PG模式Lexer可识别后返回42601/0A000且不切连接；extended模式实现为重新建立session上下文前强制无事务、无portal/temp/listen，并明确它不是原子SQL。推荐客户端重连；验证PG模式状态完全不变，extended模式清理无跨库泄漏。

### DIV-02（REPLACE INTO）

PG模式按syntax error；extended模式在Analyzer转换为明确的INSERT ON CONFLICT策略，但要求用户给唯一target，不能模拟MySQL“delete then insert”副作用。输出tag/RETURNING走ModifyTable；触发器/FK差异写文档和差分测试。

### DIV-03（LOAD DATA INFILE）

PG模式拒绝；extended模式只做COPY FROM的语法糖并使用同一CopyState、ACL、encoding和error options，不另写导入器。EXPLAIN/日志显示规范化COPY；GB级输入性能与COPY相同且路径安全测试通过。

### DIV-04（SELECT INTO OUTFILE）

PG模式严格把SELECT INTO解释为建表，OUTFILE产生syntax error；extended导出使用独立 `EXPORT` 命令或客户端COPY，不劫持SELECT grammar。结果经过CopyTo codec、权限/path检查；验证PG ORM迁移不会误导出文件。

### DIV-05（DESC/VIEW/SHOW 项目命令）

PG模式不接受服务端元命令，只通过catalog支持psql `\d`；extended命令转成只读catalog query并返回typed TupleDesc，不打印自定义文本。每个命令有等价SQL和权限过滤，性能依赖syscache/catalog index。

### DIV-06（MySQL/非 PG 类型别名）

PG模式仅保留PG真实别名，TINYINT/DATETIME/BLOB/NCHAR/unsigned/AUTO_INCREMENT等按PG报错；extended模式在Analyzer显式映射并发NOTICE，catalog保存实际PG兼容type而非假type。dump默认输出canonical type，边界/溢出差异有测试。

### DIV-07（索引快捷语法）

PG模式只接受`CREATE INDEX ... USING am`；extended parser将FULLTEXT/HASH快捷语法生成同一CreateIndexStmt并要求真实AM/opclass完成，否则0A000。catalog/deparse始终canonical，性能和durability走IDX路径。

### DIV-08（CREATE ASSERTION）

两个模式默认返回0A000，因为PG18也未实现且当前无全局constraint runtime；不得保存compat object或返回created。若未来作为项目扩展，必须有增量/并发全局constraint设计、dependency、deferred/crash和成本证明后另立feature。

### DIV-09（普通 SQL 复制槽/逻辑消费命令）

PG模式只支持系统函数与replication protocol规定接口；extended SQL wrapper调用同一Slot/LogicalDecoding API并执行相同权限、事务限制和typed output。禁止另存slot状态；协议/函数/wrapper三路advance结果与WAL retention一致。

### DIV-10（DUMP/BACKUP/RESTORE/CLEAR PLAN CACHE）

PG模式拒绝非reference命令，使用pg_dump/base backup API和DISCARD/函数；extended wrapper只提交后台job并返回typed job id/status，不在SQL线程复制整库。job复用BACKUP/PlanCache实现，有限速/取消/权限和crash resume。

### DIV-11（SET GLOBAL/SHOW）

PG模式按GUC的SET SESSION/LOCAL、ALTER SYSTEM和SHOW规则；extended SET GLOBAL映射ALTER SYSTEM前做superuser/context检查并NOTICE，不能直接改全局内存。GUC registry是唯一状态，SIGHUP/restart/pending结果与canonical命令一致。

### DIV-12（内置连接池/TDE/备份扩展）

保留为`dbms_*`命名扩展或外部进程，catalog/view不占用PG保留对象并清楚暴露版本；连接池transaction/statement mode必须执行session-state reset审计。各扩展有独立SLA/性能/安全测试，不能用于宣称PG核心差距已关闭。

### DIV-13（端口/目录/CWD 身份偏移）

可以保留自有默认端口和目录，但启动必须显式data dir、写本项目magic/system id，握手server_version说明compat层且不声称文件可被PG打开。所有运行状态离开CWD；工具面对错误cluster立即拒绝，双产品同机测试无路径冲突。

### DIV-14（骨架命令假成功）

立即把所有仅写compat record的路径改为先检查feature manifest并返回0A000；迁移时只读旧记录生成诊断，不自动激活。命令完成必须由真实runtime返回且事务/错误/command tag通过对应测试；CI扫描 `.pg_compat_objects` 写调用和误导tag为零。

## 23. 每项实施工单的固定模板

开始任何条目前，把本蓝图条目展开为工单，并必须同时提交：

```text
gap_id / PG18 reference section / current source evidence
accepted SQL + rejected SQL + raw input/encoding cases
typed AST/query/plan/catalog/storage/WAL/protocol changes
success rows/types/tag + error SQLSTATE/fields + transaction status
lock/snapshot/crash invariants + migration/rollback
memory limit + I/O complexity + contention analysis + benchmark
unit + differential + isolation + wire + fault/crash + upgrade tests
docs/status manifest update (only after all tests pass)
```

禁止拆成“先返回成功，runtime以后再补”的工单；一个大项可以按内部里程碑拆PR，但feature gate只能在完整闭环后开启。

## 24. 可执行的覆盖和验收

新增代码时应同步创建：

- `tests/compat/manifest.yaml`：273个gap ID、183个command、PG文档锚点、状态和测试集合。
- `tests/compat/run_differential.py`：同驱动PG18.6与本项目，比较typed rows/errors/catalog。
- `tests/protocol/golden/`：startup/auth/simple/extended/COPY/cancel/pipeline/replication字节序列。
- `tests/isolation/specs/`：移植并扩展PG schedule。
- `tests/fault/points.yaml`：持久化kill point和期望恢复invariant。
- `bench/baselines/pg18/`：同机plan、吞吐、延迟、CPU/RSS/I/O/WAL基线。

完成判据由脚本计算，不能人工改README勾选：manifest中某gap的必需测试全绿、无未过期allowlist、性能未超预算且所依赖gap已完成，才允许状态从`open`变`complete`。

## 25. 依赖关键路径

```text
typed Datum/result/error
    -> lexer/analyzer/binder/rewrite
        -> catalog唯一事实源 -> 事务化DDL -> ACL/RLS/extension
        -> typed planner/executor -> 完整query/DML -> protocol/client
storage control/page/buffer
    -> 全资源WAL/checkpoint -> MVCC/MultiXact/VACUUM/index AM
        -> crash/PITR/base backup -> physical standby
        -> WAL logical decoding -> pgoutput/subscription
以上全部 + differential/isolation/fault/perf/upgrade证据
    -> PostgreSQL 18 compatible 发布声明
```

不能把复制、备份或扩展提前接到旧字符串/sidecar真源上；那会产生第二轮必须推翻的实现。可并行的是同一稳定接口后的不同类型codec、不同index AM、不同catalog view和客户端测试。

## 26. PostgreSQL 18 规范来源

- [PostgreSQL 18.6 Documentation](https://www.postgresql.org/docs/18/)
- [SQL Commands](https://www.postgresql.org/docs/18/sql-commands.html)
- [Frontend/Backend Protocol](https://www.postgresql.org/docs/18/protocol.html)
- [System Catalogs](https://www.postgresql.org/docs/18/catalogs.html)
- [Type Conversion](https://www.postgresql.org/docs/18/typeconv.html)
- [Indexes](https://www.postgresql.org/docs/18/indexes.html)
- [Concurrency Control](https://www.postgresql.org/docs/18/mvcc.html)
- [Reliability and WAL](https://www.postgresql.org/docs/18/wal.html)
- [High Availability and Replication](https://www.postgresql.org/docs/18/high-availability.html)
- [Backup and Restore](https://www.postgresql.org/docs/18/backup.html)
- [Extending SQL](https://www.postgresql.org/docs/18/extend.html)
- [Monitoring Database Activity](https://www.postgresql.org/docs/18/monitoring.html)

这份蓝图是实现约束，不是工期承诺。数据库兼容和可靠性只能由可重复证据证明；任何未通过上述完成定义的条目都保持未完成。
