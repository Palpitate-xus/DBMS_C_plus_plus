# DBMS 使用手册

<!-- CURRENT_GIST_READ_OWNER_20261105 -->

最新P19-C3修正普通当前读事务被GiST误判为重叠写者的问题，并复用可见元组NULL位图。专项1214检查、新39协议、PG18.6共享37命令/1287字段对照通过；候选检索阶段目标堆页fetch由旧版2021降至0，返回行仍正常回表校验。历史/重叠/自身写入/缺失损坏/空文本范围及SERIALIZABLE保留堆后备。旧60原生59正常0和1项父代相同TOAST中止、旧32协议29正常0和3项父代相同严格失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 2.54%／3.42%；热点等值 -0.42%／-1.44%；低频前缀 56.62%／52.19%；低频等值 -12.42%／-10.29%；不存在等值 2.15%／1.44%；主键控制 -6.09%／-3.54%，负数与控制波动也报告。旧GiSTScan/idx_scan只代表算子，不能单独证明底层物理索引访问；下方旧章节按此限制理解。源248/头177、测试2367，676注册/656独立E2E；原273状态不变，历史270，754MD/725保护记录保持，18旧gzip保留并新增1。仍partial，Goal active；详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_TYPED_PLANNER_STATISTICS_20261105 -->

最新批次 P18-C6 将普通等值与二进制文本 LIKE 扫描成本接入拥有真实身份的 typed ANALYZE 统计，并修正事务内显式分析的两份统计文件回滚。新65条协议/15次扫描、原LIKE123/27、最终原生专项及cold exec、组件O2/ASan/UBSan、7个恢复worker和真实PG18.6共享33命令/6732字段对照通过；相关旧协议29通过、3项父代完全相同严格失败，首次自动数组/range回滚超时及后续原deadline固定旧/新/旧六次通过均保留。完整socket性能中位数减少（首轮／回测）：热点前缀 73.72%／73.25%；热点等值 51.25%／15.72%；低频前缀 58.53%／-8.58%；低频等值 44.30%／38.10%；不存在等值 24.66%／24.31%；主键控制 30.77%／29.00%，负数也保留，只代表该负载。原273与22/166/70/15状态保持，历史269，测试2365，源248/头177，675注册/655独立E2E，754MD/725保护记录、17旧压缩包保持。仍是partial，Goal active；事务内ANALYZE需要临时数据库物理备份，旧统计格式、未转换消费者和用户暂缓模块未整体完成。详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据。


<!-- CURRENT_LITERAL_LIKE_SCAN_20261105 -->

## LIKE前缀的真实索引读取与密集前缀成本（2026-11-05，P17-C6）

普通无参数SELECT中的TEXT/VARCHAR字面量LIKE现在可实际使用已有GiST侧索引。已绑定AST提供真实关系/列身份；前缀范围只筛选候选，原类型化LIKE、AND剩余条件和NULL语义继续复查。转义百分号/下划线、多字节ESCAPE、UTF8上界以及打开前删除索引均有回归。可用ANALYZE的MCV与完整覆盖直方图桶识别密集前缀，按真实表extent和成本参数保留堆扫描；未知探测至少计1行成本。

同一4096行×3列、父代／最终C6／同一父代回测各3进程交错运行；wall中位数变化（正数为减少，相对首轮／回测）：热点前缀（3200行）6.88%／-3.83%；低频前缀（1行）68.01%／67.77%；不存在前缀（0行）69.40%／68.72%；显式ESCAPE前缀（1行）71.84%／71.06%。仅代表本负载；C3热点超过3倍耗时的失败尝试完整保留。

最终C6通过新原生310检查、原13068与独立冷读105检查、37个相关原生入口；纯前缀组件O2和ASan/UBSan各72567检查，启用泄漏检测。新123条协议命令含27次实际扫描断言；32个相关旧协议入口29正常0、3项严格失败与父代完整观察相同。真实PG18.6以两个会话重放共享65条SQL，935个完整字段/NULL/OID逐项相同，3个EXPLAIN ANALYZE实际行数相同；未比较PG的AM路径、成本或速度。

使用已有CREATE INDEX ... USING gist(column)与ANALYZE table_name后查询LIKE字面量；用pg_catalog.pg_stat_tables真实seq/idx增量检查执行。SET random_page_cost影响成本选择；密集前缀保留堆扫描是正常行为。参数、动态模式和其他未支持声明保留既有路径。

详见[性能报告](performance-report-2026-10-30.md)与[压缩原始证据](benchmarks/like-prefix-scan-20261105.json.gz)。本批仍为partial，Goal active。


<!-- CURRENT_ORDINARY_QUERY_PROFILE_20261105 -->

## 普通查询的规划元数据与解析开销（2026-11-05，P16-C2）

本批在一次普通查询规划内复用关系schema，并按需各读取一次B-tree/Hash/Bloom/GiST列清单；主键与不可能使用位图/GiST的条件省去无用发现。普通表达式先排除不可能的类型常量前缀，避免对数字、表达式分隔符前的列名做推测类型解析及异常处理。真正的限定/引号/修饰/复合类型常量与显式CAST仍使用原声明语法。

交错的8倍调用补测，父代／最终C2／同一父代回测各3进程；wall中位数变化（正数为减少，相对首轮／回测）为：热点等值（3200行）12.15%／-1.12%；低频等值（1行）15.17%／18.32%；不存在等值（0行）17.94%／14.60%；主键等值（1行）25.52%／19.68%。全部短时及补测的正负结果保留，不能推广为全部操作或PG速度。

最终C2通过原生13068与独立冷读105检查、32个规划/存储入口和14个语法入口；另2个冷目录原生入口保留父代完全相同的严格失败。新65条协议命令含10个实际扫描断言，原有73条等值协议通过；另30个相关旧协议入口27通过、3个严格失败与父代完整观察相同。实际PG18.6的共享25条SQL仍为7331/7331逻辑单元格、9/9行估计一致，8/9路径相同；不是完整兼容验收。

每条新查询重新取得schema/索引信息；正常DDL后继续发出查询即可，无需手动清理规划元数据缓存。使用pg_catalog.pg_stat_tables实际seq/idx差值检查执行路径。常规声明/类型常量用法保持，性能不等于C++标准升级收益。

详见[性能报告](performance-report-2026-10-30.md)与[压缩原始证据](benchmarks/ordinary-query-profile-20261105.json.gz)。本批是普通读取partial进展，Goal active。


<!-- CURRENT_EQUALITY_SCAN_COST_20261105 -->

## 普通等值查询的实际扫描选择与完整网络成本（2026-11-05，P15-C7）

本批让普通原生标量堆表查询实际消费基于ANALYZE频率的扫描选择：热点等值可选顺序扫描，低频等值和主键保留索引路径。此前只修改规划器时，无ORDER BY的普通查询仍绕开物理计划；现在结构化输出已接通，并修复无可用索引OR的显示文本合并，以保留重复行、NULL、空串和换行。B-tree、位图AND/OR与GiST在探测前取得语句快照，并在快照不适合当前索引或探测期间提交边界改变时读取完整可见堆候选，再做原条件检查。

这是普通读取的partial进展，使用现有原生 `.stats` 中的MCV/计数；尚未让优化器全面消费新的typed `.analyzed_statistics` 槽。普通EXPLAIN的文本缓存键现包含精确的实时成本设置；没有修改已暂缓的retained/custom/generic规划、参数、portal/format2或mutation/provider生命周期。当前私有普通后端P15-C7 SHA256为 `3ff5f16a9cc3a223d6618db87d8a5bbe85240dfd4c08cfdceafeb6ba49c03d3a`。默认根程序425与既有服务保持；Goal active，PostgreSQL整体功能仍partial。
同一4096行×3列、同一真实SQL与完整网络返回，父代／最终C7／同一父代回测各3进程，固定9个完成测量的进程，不重试计时查询；另两次在基线装载/启动阶段失败、没有计时样本，完整保留。wall中位数相对首轮父代／回测的变化（正值为减少、负值为增加）：热点等值（3200行）12.51%／2.45%；低频等值（1行）-5.57%／-5.47%；不存在等值（0行）77.31%／77.36%；主键等值（1行）-11.11%／9.44%。正负变化均如实保留；仅代表这四个普通读取负载，不能推广为全部操作、通用吞吐或PG速度。

新原生13068检查与独立冷读105检查通过，纯频率组件O2及ASan/UBSan各17检查（启用泄漏检测）。24个相关原生入口实际正常0，含完整7worker恢复1974检查和14份子进程完整输出；这些入口实际链接最终C7的61个核心对象。新73条普通协议命令正常0，验证实际扫描计数、全部行值/NULL及旧快照；26个旧协议入口中23正常0，另3保留与父代一致的严格失败，未称全套通过。真实PG18.6对共享25条命令的7331个逻辑单元格全部逐字相同，9个EXPLAIN行数相同，8个扫描选择相同、1处成本边界扫描选择不同。

已测用法：先建立实际单列B-tree并执行`ANALYZE table_name;`，再查询等值；普通无ORDER BY查询使用已选路径。`SET random_page_cost`与`SET seq_page_cost`影响普通扫描选择及EXPLAIN显示；用`pg_catalog.pg_stat_tables`的实际seq/idx计数验证执行，不能只看EXPLAIN。统计缺失时保留原选择，并非所有索引或类型均完成PG成本模型。

原始测量、失败与独立复核见[压缩机器归档](benchmarks/equality-scan-cost-20261105.json.gz)和[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_PG_STATS_SOURCE_20261105 -->

## pg_stats 完整基础查询与读取成本（2026-11-05，P14-C6）

本批接通实际 `pg_catalog.pg_stats` 的17列基础SQL查询、参数为零的纯Describe、文本/二进制/混合结果格式，以及实际 owning anyarray 输出；支持已测的过滤、投影、排序、LIMIT、别名、CTE和JOIN。每次查询只读取当前数据库的真实统计与目录，重用本次查询内的关系/属性索引和每关系一次的物理模式校验，减少逐列重复扫描、模式复制和文件散列。

本节更新下方P13当时“17列SQL尚未接通”的结论；下方历史批次保留当时事实，当前状态以本节为准。当前私有普通后端P14-C6为 `9362b02aba17d45038702211ba42a6a1d797ef3580d97edb8ffabce80e0f63b6`。默认根程序425和既有服务未切换，Goal保持active；整体PostgreSQL功能仍为partial。
同功能未索引C2／最终C6／同一C2回测各3进程，每进程先1次不计时完整查询，再计时3次完整17列SQL查询API。16张表×30列×32行，实际512行/15360存储单元格/1696个NULL；每次查询返回480行×17列。wall中位数384.334／245.178／379.841ms，降低36.21%／35.45%；process CPU中位数392.205／254.151／387.508ms，降低35.20%／34.41%。这是该负载的完整原生SQL API对照，不是任意查询、网络端到端、已发布父代或PostgreSQL速度对照。所有9个样本及长尾保留。

新原生109检查与独立冷读90检查、投影组件O2和ASan/UBSan各548检查通过；34个相关原生入口全部正常0，包含完整7worker恢复1974检查及14份完整子进程输出。普通后端新协议26条SQL及旧标量25/数组19/范围19条自动统计协议通过。真实PG18.6同26条协议命令991个字段值：938逐字相同、53处严格差异保留；17列二进制153项中145相同、8处为实际存储宽度差异，六种范围的24个直接二进制值全部相同。另510个有限float4输入的文本和二进制身份均与PG一致。没有全库SAN或整体PG等价结论。

已测用法：`ANALYZE table_name;` 后执行 `SELECT * FROM pg_catalog.pg_stats WHERE tablename='table_name' ORDER BY attname;`，也可只选择attname/null_frac/n_distinct等列。新建空库立即有真实17列描述符；旧库缺失声明需由实际可变初始化拥有者发布，冷SELECT不会创建系统目录。

原始测量、失败及逐字复核见[压缩机器归档](benchmarks/pg-stats-source-20261105.json.gz)与[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_RANGE_COLLECTOR_20261105 -->

## 范围统计采集与完整 ANALYZE 成本（2026-11-05，P13-C2）

本批新增六种内置范围类型int4range/int8range/numrange/daterange/tsrange/tstzrange及实际范围域的ANALYZE采集：独立排序下界和上界、保存范围边界与长度直方图、空范围比例、NULL比例和实际存储宽度。边界保留包含关系、无界端点和子类型语义；本次样本局部缓存解析键，避免排序时反复解析。

最终私有普通后端P13-C2为 `3593b23e5e142605fe71149daec3347015003fb7a84d8a2505c80578e4e77a5d`，默认根程序425保持。完整pg_stats目录/17列SQL及output-only anyarray尚未接通，两条实际探针仍严格失败0A000。多范围尚缺真实目录/存储拥有者；本批仍为采集阶段的partial进展，Goal active。
本批同功能未缓存C1／缓存C2／同一C1回测，各3进程；每进程2048行、1个整数和六种范围，先1次不计时预检，再计时3次完整ANALYZE API。process CPU中位数388.224／201.363／391.997ms，降低48.13%／48.63%；wall中位数441.019／250.031／456.067ms，降低43.31%／45.18%。所有9次原始测量均保留；这不是任意SELECT、父代已发布后端或PG速度对照。

原生范围845检查及冷读262通过；组件O2和ASan/UBSan各43检查（启用泄漏检测，非全库SAN）。21个相关原生入口含完整7worker恢复1574检查全部正常0。原数组786+冷读191、标量641+冷读2、范围19条/数组19条/标量25条协议SQL通过。真实PG18.6同一520条SQL对照18列243项：229相同，14处严格差异保留（13处存储宽度、1处numeric NaN长度排序）。没有PG整体通过结论。

普通ANALYZE命令和SET STATISTICS 0/1按原方式使用。当前仍无法通过完整pg_stats SQL读取这些新记录；上面的范围是实际验证范围。

原始测量、失败和复核见[压缩机器归档](benchmarks/analyzed-range-collector-20261105.json.gz)及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_ARRAY_COLLECTOR_20261105 -->

## 数组统计采集与完整 ANALYZE 成本（2026-11-05，P12-C3）

本批把真实数组存储输入接到同一次ANALYZE采样：保存整数组MCV/直方图/相关性，以及常见元素、按非NULL数组计算的成员频率和不同元素数直方图。实际验证integer/text/numeric/boolean/float8数组的NULL、空数组、字面NULL、重复值、多维/下界和冷读；其他元素类型及全部PostgreSQL功能仍需分别验收。

最终私有普通后端P12-C3为 `7d3cc2a1353c105ecac62579a919ec96d088cd325fa74234bc47252061e1029a`，默认根程序425保持。完整pg_stats目录/17列SQL与output-only anyarray尚未接通，两条实际探针仍严格失败0A000；本批是采集阶段的partial进展，Goal active。
完整ANALYZE对照为本批同功能未缓存C2／缓存C3／同一C2回测，各3进程；每进程2048行、1个整数和3类数组，先1次不计时预检，再计时3次完整API。process CPU中位数1243.545／164.180／1296.411ms，降低86.80%／87.34%；wall降低85.86%／86.87%。最终版685.690ms wall长尾保留。此结果限于该负载，未测任意SELECT或PG速度。

原生数组786检查及冷读191通过；计算组件O2和ASan/UBSan各32检查（启用泄漏检测，非全库SAN）；17个相关原生入口含完整7worker恢复1574检查全部正常0。原标量641+冷读2、数组19条协议SQL和原标量25条协议SQL通过。真实PG18.6同一554条SQL对照14列325项：301相同，24处严格差异保留（11宽度、11collation OID、2处相同频率元素的截断选择）。没有PG整体通过结论。

普通ANALYZE继续按原命令使用；SET STATISTICS 0关闭该列、重新分析后不保留其旧数组记录。当前无法通过pg_stats SQL读取新记录，不能据此编写依赖完整视图的示例。

原始测量、失败和复核见[压缩机器归档](benchmarks/analyzed-array-collector-20261105.json.gz)及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_SCALAR_COLLECTOR_20261105 -->

## 标量统计采集与 ANALYZE 优化（2026-11-05，P11-C4）

已接入真实ANALYZE的标量采集和快照发布；旧优化器/原生8列统计保持。普通服务器四条SQL流程与独立快照读回通过，完整pg_stats查询仍有2项0A000失败。PG对照的3处存储宽度及2处collation OID差异仍开放。

本批同功能版本的完整ANALYZE对照（4096行、4类型，每进程计时5次，前/新/回测各3进程）CPU中位数降低51.93%/54.81%，wall降低50.64%/54.17%；9份完整数据/RID/NULL和统计快照逐字一致。对照是本批P11-C2/P11-C4，不是父提交后端或任意SELECT查询提速。

源236/头166；原234生产文件仅TableManage.cpp变更，164旧头和2342旧测试保持。新增3测试后2345（顶层1094 CPP含桩、去桩1093，另5子目录CPP）。原273项22/166/70/15及261条历史保持，仅加1条partial后262；Goal active。

普通`ANALYZE table;`现会额外产生有类型的标量统计快照；旧8列原生API保留。`SELECT * FROM pg_catalog.pg_stats`仍未接通，不应将它写成可用命令。详见[性能报告](performance-report-2026-10-30.md)。

最终C4还修复默认自动分析的可见性：旧C3在批量INSERT达到阈值时生成空列快照；现在普通事务成功COMMIT、清理原命令视图与锁后，按已有改动表集合触发分析。失败/回滚不提前发布；只有分析成功才扣除已统计修改数，保留并发新增计数。维护错误不把已持久提交伪装成失败。25条普通协议SQL验证INSERT55、显式事务提交110、UPDATE110、DELETE50、回滚不改快照、ALTER SYSTEM+reload关闭自动分析后显式分析105，并核对5次EXPLAIN基数；服务器正常0。原恢复入口1574检查及7个worker全部正常0。共享IO/分配失败、两阶段提交的自动统计调度及显式事务内ANALYZE的全部回滚语义仍未完整验收。

按用户清理要求删除93209个旧编译文件，实际空闲增加约658GiB；分区85%→72%、空闲约1.5TiB，项目约111→28GiB。源码、原2342测试、全部已跟踪文件、当前C4/default425及PG18参考工具已核对保持。后续以现有CMake缓存和当前core复用构建，证据归档后清理可重建产物。删除在9样本全部结束后进行；只读du盘点与最终对照的回测段重叠，全部结果保留，不称为无并发负载测量。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

本批先实现完整pg_stats所需的类型化统计快照内部组件，尚未增加可用的pg_stats SQL入口；完整17列的两个普通查询实际均为0A000。现有ANALYZE执行路径及`.stats`兼容行为保持，普通后端仍为P9-C2。

新快照保留实际类型身份、NULL/空数组差别及统计槽的元数据；旧`.stats`不足以重建这些字段，后续仍需新采集器重分析、真实目录来源和输出通路。实现与剩余门槛见[报告](performance-report-2026-10-30.md#完整统计视图的快照基础2026-11-05p10)及[归档](benchmarks/analyzed-statistics-snapshot-20261105.json.gz)，默认服务未切换。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

当前源码P9-C2 `30c57ca8…` 保持C++17和原V2表名列表字节格式。版本标记采用无析构的常量存储，维护状态表在每个存储引擎构造完成前初始化，覆盖后台线程的寿命。SQL恢复命令及后台作业使用方式见下方既有说明。

原七阶段恢复及其完整Memcheck本次通过，正常Main数据和冷启动通过，5项元数据检查仍失败；跨服务进程在线恢复准入仍开放。默认425未替换。[报告](performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)与[机器归档](benchmarks/relation-list-engine-lifetime-20261105.json.gz)区分各版本失败及本次通过。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

当前源码P8-C3 `d778afbd…` 在公开 `RESTORE DATABASE` 的发布前读取真实恢复文件的事务号证据，持久推进本机分配器且不回退既有高水位；未改变项目V2清单/V3备份标记。此项目格式与PostgreSQL物理备份格式不同。

普通SQL仍需显式extended模式和管理员权限，通过 `RESTORE DATABASE 数据库 FROM '路径'` 提交后台作业，再用 `SHOW MAINTENANCE JOB 作业号` 查看终态。作业succeeded不等于全部兼容或恢复验收通过：固定22操作的值、NULL、后续INSERT/CREATE与两个正常Main上下文（含冷启动）通过，但5项探针类型描述符/目录身份检查仍失败；最终七阶段原生控制在advance阶段异常退出，后四阶段未执行。

准入屏障仅覆盖当前进程的分配及活动登记窗口；其他实际活动事务号不高于导入水位时保守拒绝恢复。跨服务进程的在线准入仍有缺口。本批未提升默认425或重启现有服务，完整边界与两轮计时见[完整报告](performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和[机器归档](benchmarks/restore-transaction-horizon-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

当前源码 P7-C1 `724c1147…` 的无列列表 `ANALYZE 表名` 统计普通列和STORED生成列，跳过virtual生成列。未选择的溢出或除零virtual表达式仍不应由统计操作求值；真正读取该列仍返回相应错误。ANALYZE列列表及qualified pg_catalog.pg_stats入口尚有差异，详见[测试与性能范围](performance-report-2026-10-30.md)。

物理备份仍使用本项目原V2清单与V3标记，创建端新摘要与旧校验器逐文件兼容；这不是PostgreSQL备份格式。已有较早目标全局状态的恢复探针缺口仍开放，请按[完整报告](performance-report-2026-10-30.md)所列范围验证。二进制分发同时附带[第三方声明](../THIRD_PARTY_NOTICES)，CMake会安装到share/doc/DBMS_C_plus_plus。默认程序仍是425，需构建当前源码获得本批改动。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

本批源码 P6-C8，对应正常程序 SHA256 `45264111…`，保持 0.2.0 / C++17 / GCC 13.3 / O2 / ICU 74.2 / zlib 1.3 / TLS stub。根目录默认程序仍为 `425165af…`，现有服务未重启。

普通虚拟生成列在读取时执行声明赋值；未被读取的列保留其延迟求值。以下SQL的id查询得到40000，读取v返回22003；NOT NULL/CHECK等真实需求仍会要求读值。

```sql
CREATE TABLE demand_demo(id integer, v smallint GENERATED ALWAYS AS(id) VIRTUAL);
INSERT INTO demand_demo(id) VALUES(40000);
SELECT id FROM demand_demo;
SELECT v FROM demand_demo;
```

隐式INSERT按物理前N列对应，生成列位不能被非DEFAULT值占用。字符数组、SQLNULL与缺失OLD/NEW行分别处理。尚未完整兼容来源描述符、运行时错误前缀及所有索引表达式；详见[当前测试与性能范围](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

使用本批源码优化需自行构建当前源码；根目录程序仍为425。命令覆盖及PostgreSQL兼容性不因性能控制通过而扩大。

详见[完整性能报告](performance-report-2026-10-30.md)和[本批机器归档](benchmarks/analyze-reindex-main-costs-20261104.json.gz)。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

C4支持范围内的目标/OLD/NEW以及MERGE实际USING限定列使用真实值和目录身份。声明API仍有quoted stored extract类型差异；历史数组位图、生成列赋值/按需错误、更多来源名称/星号与类型目录仍开放。Parse名字检查不提前求值修改输入，不能把132控制通过理解为完整DML兼容。

详见[本批报告](performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及[机器归档](benchmarks/returning-declarations-hotpaths-20261104.json.gz)。以下为历史记录。

## RETURNING 拥有值的新增支持（2026-11-03）

根目录本地默认程序现为同批实测C3 `425165af…`，原S3已备份；现有服务未因本次安装重启。

C3 `425165af…` 在原 RETURNING 表达式求值后保留实际 `ExprValue`，按已有请求传递 record 字段、record[] 元素及数组形状、NULL 和引用身份。真实目标行及 OLD/NEW 的非虚拟存储数组在原行绑定处物化一次；已经拥有的值不从显示文本重建。默认关闭捕获时仍使用原文本结果路径；协议格式、描述符匹配规则及暂停的顶层枚举/枚举数组路径不变。

独立辅助求值循环减少分配；公开游标512行的分配不变，单次冷执行CPU从20.750ms升至35.862ms。该负值保留，不能用辅助循环的收益保证实际查询提速。

固定PG18.6参考的56项中，C8有20差异，C3有8差异。DELETE二进制内容与提交后状态已修复，但6项OLD/NEW别名的Statement/Portal Describe仍缺RowDescription，另有虚拟生成数组载荷及历史物理数组NULL位图各1项；不能将这些正例记成通过。新增20项原生控制通过，验证NULL/空值/下界、真实OLD/NEW、零行、volatile每站点一次、原错误回滚及查询对象销毁后的值寿命。其余WHERE/SET输入、目录、typmod和ALTER USING普通缺口保留，见[本批报告](performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)。最终36组查询均低于两次S3中位数，但35类操作仍有2项慢于两次C8对照，不能保证任意SQL提速；独立机器归档见[全部原始统计](benchmarks/function-registry-returning-20261103.json.gz)。以下C8/C7使用边界按当时版本保留。

## 复合类型输出与 REGTYPE 使用边界（2026-11-03）

C8 `91d339db…` 仅减少原始类型行物化中的多余来源复制和临时容量分配，类型与操作边界沿用C7。独立512行公开游标的累计分配请求减少，但完整查询36组仍为5快/18慢/13居间，默认安装继续为S3；使用当前源码功能仍需自行构建。全部结果见[本批报告](performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)及[机器归档](benchmarks/owned-datum-primitive-20261103.json.gz)。以下C7能力与测量记录按原代别保留，不把两批独立时钟拼成C7→C8稳定倍率。

C7 `f135335e…` 在已有真实声明、正确 RowDescription 和完整拥有值的查询路径上补入正常二进制结果输出：保留 record 的实际字段类型及 NULL、record[] 的逐元素记录形状、数组元素/维度/下界和已有位图信息。格式 0 继续使用原文本输出；只有所有格式码合法为 0/1 且确有格式 1 的请求才启用可选结果载体。未知或不匹配的旧描述符，以及暂停的顶层枚举/枚举数组族继续原路径，不能把“客户端请求 binary”理解为任意类型均已支持。

新 REGTYPE 标量按真实 uint32 OID 持久化，SQL NULL 与 OID 0 分开；名字在输入时按实际目录解析，读取保留原 OID 后生成显示文本。REGTYPE[] 同时保留真实元素身份和数组形状；无符号高位值、索引、COPY 与同目录冷重开已有固定控制。普通 UPDATE RETURNING 的原真实行绑定已修正，但其复合 binary 结果捕获、更多 WHERE/SET 输入物化、域/命名复合目录和 ALTER USING 仍开放；旧文本存储也无法恢复全部历史数组位图状态。

严格 68 项物理控制为 57 通过/11 差异，16 项正常文本/二进制参数控制全部通过。功能并不等于全类型、全操作兼容；全部缺口和测试代别见[本批功能与成本报告](performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)。资源55通过，速度对照为4快/23慢/9居间，C7未替换根目录S3。运行本节功能需要按当前源码构建；不沿用旧窗口倍率作为这次输出链的收益。以下旧章节按记录当时解释。

性能优化、35 类操作的耗时对照及复现方法见 [2026-10-30 性能测试报告](performance-report-2026-10-30.md)。
查询使用 `scripts/benchmark_queries.py`，增删改、事务、DDL、COPY、索引和维护使用 `scripts/benchmark_operations.py`。
两者均要求显式指定待测二进制与新的输出目录，并创建私有数据目录；默认持久化设置保持开启。
报告区分可比较的完整测量窗口、失败记录与剩余瓶颈；下文按版本保留的兼容性结论和暂缓范围继续按各自证据解释。

近期修复与历次验证集中见[审查索引](review-index.md#近期修复与验证记录)，当前实现和性能范围见[状态说明](production-status.md#当前状态与验证范围)。

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

DATE/时间戳加减同时含月份、天数的INTERVAL时，先调整月份并截断到目标月底，
再加天，最后处理微秒和跨日。例如2024-01-30加1月1天得到2024-03-01，
不是2024-02-29。实际表写入、返回值和保存点撤销亦有有限回归；
范围与尚未完成的civil/DST等边界见[R147](issue-review-r147-interval-calendar-order.md)。

日期算术的结果类型按完整已声明的运算决定：DATE加/减INTERVAL为TIMESTAMP，
INTERVAL加DATE或时间戳同样返回对应时间戳；DATE差值为integer，时间戳差值为interval。
SQL NULL和零行结果不改变这些类型，明确类型的text Bind及text/binary结果沿用同一声明。
有限范围及未完成边界见[R148](issue-review-r148-temporal-arithmetic-result-types.md)；
这不等于完整时间类型运算、UNKNOWN重载或所有时区行为已实现。

`SET search_path TO 'a,b'`设置名称为`a,b`的一个schema；
`SET search_path TO a,b`才是两个schema。Sconst内的大小写和引号属于名字数据，
例如`'Upper'`不折成`upper`，空串设置的GUC显示为`""`。
`SET search_path FROM CURRENT`是普通会话赋值：事务中当前LOCAL可见值可由此保留至COMMIT后；
`SET LOCAL search_path FROM CURRENT`仍随事务结束恢复，ROLLBACK撤销普通赋值。
普通DEFAULT重置与带引号的`'default'`名字不同。验证范围见[R145](issue-review-r145-search-path-value-identity.md)。

已实现普通参数的完整双引号名称按GUC大小写无关规则查找，例如
`SET "APPLICATION_NAME" TO 'client'`与`SET application_name TO 'client'`设置同一字段。
事务内`SET LOCAL "search_path" TO pg_catalog`沿用已有搜索路径作用域。
quoted `"@name"`是参数标识符，不是项目`@变量`赋值；完整语法和值域不据此扩展。
验证边界见[R142](issue-review-r142-quoted-set-parameter.md)。

`SHOW application_name`、`SHOW client_encoding`、`SHOW search_path`与
`SHOW lc_monetary`的协议结果为一列text；空串是一行空值，不是零行或SQL NULL，
字符串NULL仍是普通文字。Describe只提供固定结构，不执行查询；路径值中的换行
保留在一个字段里。范围与未完成边界见[R143](issue-review-r143-show-guc-protocol-results.md)。

[R140时区语法候选](issue-review-r140-timezone-set-grammar.md)已按用户要求暂缓，
不能因工作区保留候选就视为发布能力。HEAD仍保留紧贴等号被拒、非法分隔符
被接受等已记录边界；不承诺完整SET语法或事务级LOCAL。本次没有接受代码修复。

> 本手册说明用法，不作为全功能兼容或测试通过的证明。当前边界见[兼容性契约](compatibility-contract.md)，已发现的问题与修复方法见[代码审查索引](review-index.md)。
> 产品版本由 `src/common/version.h` 定义；存储和升级边界见[打包与部署](PACKAGING.md)。

> 数据目录说明：当前版本使用 v2、8 KiB heap page 和当前 schema 格式。读取路径检查heap文件头、页布局、line pointer和checksum，WAL读取还检查记录长度、CRC、8字节对齐和前向链。页压缩、损坏catalog读取和失败回滚已补充专项修复及故障回归；实际范围见[修复清单](review-fixes-2026-10-13.md)，不等于所有损坏和并发组合都已被验证。旧关系数据不会自动迁移；升级前保留备份，在副本上验证SQL导出及新目录重建/导入，不要直接删除唯一副本。WAL的LSN 0是合法首位置，提交先刷COMMIT WAL再刷CLOG；失败回滚会保留待恢复图及图像供显式健康重试，不能据此保证所有故障均已恢复。

> 路由说明：基础 `ALTER TABLE`、RLS enable/disable/force、分区 ATTACH/DETACH、trigger enable/disable、CLUSTER、REPLICA IDENTITY、约束验证/延迟属性、`CREATE TABLE` 分区、RLS 可见性扫描和简单单表视图 `INSTEAD OF` DML 路径由统一执行链处理；ALTER 多子命令失败会恢复整句当前格式快照，DROP/REPLACE 在变更前建立快照，外层 `ROLLBACK` 先恢复对象再撤销行级变更；规范和 legacy DDL 入口在隐式提交失败时都会报告 SQLSTATE 并停止执行；快照已被 DDL 修改后继续执行另一条快照型 DDL、创建或回滚 SAVEPOINT 会 fail-closed；普通事务提交/回滚会清理物理事务备份，DDL CREATE undo 会跨语句外层事务和 SAVEPOINT 回放；触发器函数运行时和完整对象 owner/ACL 组合语义仍未完成。含内存 DDL undo 的事务暂不支持 PREPARE TRANSACTION。RLS 已支持默认 `WITH CHECK`、`PUBLIC` 角色、基础 `PERMISSIVE/RESTRICTIVE` 组合、表 owner 绕过、基于 `pg_authid` 的 `SUPERUSER/BYPASSRLS` 绕过和 `FORCE ROW LEVEL SECURITY`。

---

## 目录

1. [快速开始](#1-快速开始)
2. [数据类型](#2-数据类型)
3. [数据库操作](#3-数据库操作)
4. [表操作](#4-表操作)
5. [数据操作 (DML)](#5-数据操作-dml)
6. [查询 (DQL)](#6-查询-dql)
7. [事务控制](#7-事务控制)
8. [索引](#8-索引)
9. [视图与物化视图](#9-视图与物化视图)
10. [约束](#10-约束)
11. [用户与权限](#11-用户与权限)
12. [角色管理](#12-角色管理)
13. [存储过程与函数](#13-存储过程与函数)
14. [触发器](#14-触发器)
15. [分区表](#15-分区表)
16. [全文搜索](#16-全文搜索)
17. [JSON/JSONB](#17-jsonjsonb)
18. [网络服务](#18-网络服务)
19. [预编译语句](#19-预编译语句)
20. [导入导出](#20-导入导出)
    - [WAL 归档与时间点恢复 (PITR)](#201-wal-归档与时间点恢复-pitr)
21. [pg_hba 访问控制](#21-pg_hba-访问控制)
22. [复制与高可用](#22-复制与高可用)
23. [大对象](#23-大对象)
24. [多进程管理](#24-多进程管理)
25. [GUC 参数配置](#25-guc-参数配置)

---

## 1. 快速开始

### 编译

```bash
# 方式一: 自动构建 (推荐)
./scripts/build.sh

# 方式二: CMake
cmake -S . -B build && cmake --build build -j$(nproc)

# CMake 标准完整回归（等价于 scripts/build_tests.sh）
cmake --build build --target check
# 或
ctest --test-dir build --output-on-failure
```

### 运行

```bash
# 交互模式
./dbms_main -D /srv/dbms/main

# 网络服务模式
./dbms_main -D /srv/dbms/main --server 9999
```

### 首次登录

按[部署说明](PACKAGING.md)准备数据目录、登录角色与访问配置后，可在交互模式下创建并选择数据库：

```sql
CREATE DATABASE mydb;
SET compatibility_mode = extended;
USE DATABASE mydb;
--- 现在可以开始建表和操作数据
```

---

## 2. 数据类型

### 数值类型
| 类型 | 说明 | 示例 |
|------|------|------|
| `INT` / `INTEGER` | 32位整数 | `42` |
| `BIGINT` | 64位整数 | `9999999999` |
| `SMALLINT` | 16位整数 | `100` |
| `FLOAT` / `REAL` | 单精度浮点 | `3.14` |
| `DOUBLE PRECISION` | 双精度浮点 | `3.1415926535` |
| `NUMERIC(p,s)` / `DECIMAL(p,s)` | 精确数值 | `NUMERIC(10,2)` |
| `MONEY` | 货币类型 | `$100.50` |
| `SERIAL` | 自增整数 (同 IDENTITY) | |

### 字符串类型
| 类型 | 说明 |
|------|------|
| `VARCHAR(n)` | 变长字符串，最大 n 字符 |
| `CHAR(n)` | 定长字符串 |
| `TEXT` | 长文本 |

长度函数的输入签名和验证范围见[R107](issue-review-r107-string-length-signatures.md)。
`char_length`/`character_length`接收TEXT或CHAR，按字符计数；`octet_length`
按字节计数，`bit_length`按位计数，后两者还接收BYTEA和BIT。UTF8文本`你好é`
分别为3字符、8字节、64位；CHAR的字符数忽略末尾填充，OCTET_LENGTH的CHAR
重载计入填充，BIT_LENGTH则经TEXT转换去除填充。整数/BOOL等不能因可转为
可打印文本就直接调用这些函数，应显式转换为适当输入类型。

具名数组输入省略长度时，`pg_catalog.bpchar[]` 和 `pg_catalog.bit[]` 保留
元素原长度；例如 `CAST('{abcd}' AS pg_catalog.bpchar[])` 返回 `{abcd}`。
普通 SQL 关键字 `CHARACTER[]` / `BIT[]` 仍默认长度 1，显式 `(n)` 仍生效；
协议数组参数声明只有 OID，没有隐含长度 1。边界和回归见
[R112](issue-review-r112-unconstrained-named-array-input.md)。

### 日期时间类型
| 类型 | 说明 | 特殊值 |
|------|------|--------|
| `DATE` | 日期 | `'2024-01-01'`, `infinity`, `-infinity` |
| `TIME` | 时间 | `'14:30:00'` |
| `TIMESTAMP` | 日期时间 | `'2024-01-01 14:30:00'` |
| `TIMESTAMPTZ` | 带时区时间戳 | `'2024-01-01 14:30:00+08'` |
| `INTERVAL` | 时间间隔 | `'1 year 2 months'`, `'90 minutes'` |

### 二进制类型
| 类型 | 说明 |
|------|------|
| `BYTEA` | 二进制数据，支持 hex (`\xDEADBEEF`) 和 escape 格式 |
| `BLOB` | 大字节对象 (MySQL 兼容) |

数组请求 PostgreSQL 二进制结果格式时，已有标量二进制发送 codec 的内置
元素可作为数组输出，保留维度、真实下界、元素 OID、SQL NULL 和空数组的
区别；不要把二进制结果与二进制 Bind 输入混同，后者不由本项实现。
TIMETZ、JSON/JSONB、TSVECTOR、range 等缺失标量发送 codec 的非 NULL
数组元素仍未支持。数组封套边界见[R110](issue-review-r110-array-binary-result.md)，
INTERVAL的后续真实发送实现见[R113](issue-review-r113-interval-binary-result.md)。

CHAR/VARCHAR与数组共存时，SELECT *仍应保留字符列声明长度；普通表、TEMP、
空结果和NULL不应改变typmod或真实列来源。修复和边界见[R115](issue-review-r115-scalar-alias-projection-metadata.md)。

DATE/TIME/TIMETZ/INTERVAL对外逻辑长度分别为4/8/12/16，数组为-1；这不等于
本引擎内部存储槽宽度。已知旧目录在读取模型中升级，冷读不发布文件，显式目录
持久化后才落盘。细节与尚存独立目录/视图问题见[R114](issue-review-r114-physical-attribute-length.md)。

INTERVAL二进制结果为网络字节序int64微秒、int32天、int32月，共16字节，
日历字段不会折成总秒；正负无限使用对应三个字段全最大/最小值。INTERVAL[]
保留维度、下界和NULL，复用真实元素发送；binary Bind输入仍不由此实现。
详见[R113](issue-review-r113-interval-binary-result.md)。

普通非物化视图的CREATE、CREATE TEMP/TEMPORARY和CREATE OR REPLACE语法
均以CREATE VIEW命令标签完成；合法注释不改变对象类型。Simple和Extended行为
一致，修复及边界见[R116](issue-review-r116-view-command-tags.md)，不据此声称物化/CTAS完成。

视图/派生结果内部的DATE/TIME/TIMETZ/TIMESTAMP/TIMESTAMPTZ列应使用真实
存储布局，不能只把VARCHAR列改名为时态类型；NULL也遵守codec布局。内部宽度
12/4/4/8/8不同于对外逻辑长度，修正范围与验收见[R118](issue-review-r118-temporal-view-materialization.md)。
这不代表已有TIME整秒物理格式已升级到完整微秒存储。

TIME二进制结果为网络字节序int64微秒，范围0至86400000000（含24:00:00）；
TIME[]元素使用同一真实codec，保留维度/下界/NULL。修复与有限验证见
[R117](issue-review-r117-time-binary-result.md)。这不同于binary Bind接收、文本输入规范化
和现有4字节整秒标量heap；这些边界不由发送器自动补齐。

共享民用日期逆换算在现有1–9999年域内使用有界Gregorian周期分解，闰年年初
和400年边界不得产生非法月份；独立全域枚举与SQL跨日验证见
[R120](issue-review-r120-date-cycle-inverse.md)。这不等于完整PostgreSQL BC/大年份支持。

TIMESTAMP/TIMESTAMPTZ的binary结果是相对2000年epoch的网络字节序int64微秒；
正负infinity为max/min，真实数组元素使用同一codec。TIMESTAMP忽略已校验offset、
TIMESTAMPTZ按offset转UTC，不能把整秒parser的失败值发作日期；见
[R119](issue-review-r119-timestamp-binary-result.md)。输入/typmod、既有秒heap和BC/大年份范围仍有边界。

默认TIME的基本clock文本输入按整数微秒校验及规范化，真实TIME/TIME[]的
text Bind也须在Bind阶段拒绝坏输入；短时钟、半偶舍入/进位和24时见
[R121](issue-review-r121-time-input-normalization.md)。显式精度/完整grammar/cast eligibility、
旧4字节整秒heap及binary Bind仍有边界，输入修复不迁移既有表行。

表上的NULL::timetz应作为CAST表达式返回SQL NULL或空结果，不能被误判为
列名42703；其声明为TIMETZ、逻辑长度12，表达式不借同名列的来源。路由修正
和验证范围见[R124](issue-review-r124-timetz-postfix-projection.md)，不表示非NULL TIMETZ
binary发送、完整时区/转换资格或物理精度已经支持。

TIME/TIMETZ/TIMESTAMP/TIMESTAMPTZ及数组列的合法precision会保存到schema、
目录和真实列结果描述中，省略仍为-1、显式0与6分别保留；空结果和NULL不改变
声明。Legacy列的modifier可保存在0x4442000c扩展；显式微秒codec的schema-D
同时保存ITM1和TCM1，不能把precision或槽宽当成单位标记。
旧声明没有保存的precision无法从值推回；备份后按可信原DDL
在副本验证重建/导入。边界见[R123](issue-review-r123-temporal-column-precision.md)，
列元数据本身不完成值转换；合法TIME精度转换后续见下方R125，旧整秒heap仍未升级。

显式TIME(p)（0–6）的基本clock转换会先按整数微秒规范输入，再按声明
precision单独舍入；例如0.500000在p=0为1秒，0.0004996先成为500微秒、
在p=3再成为1000微秒。NULL、空结果和Describe也保留实际声明precision，
不从首行或同名列猜元数据。验证及边界见[R125](issue-review-r125-time-precision-value.md)，
这不升级旧4字节整秒heap；语法/通知和完整转换资格仍有未完项。

未引号、未限定schema的TIME关键字precision须为一个无符号32位整数token；
允许0x/0b/0o及合法数字分隔符，负数/负零、+号、小数、多参数、引号、NULL和
超int32范围在解析阶段报42601。qualified/quoted类型保留具名语法区别，详见
[R126](issue-review-r126-time-precision-grammar.md)。

直接SELECT投影声明中，合法整数精度超过6时按有效6归一化text并描述结果；
Simple Q或Extended Parse各对真实声明发送一次WARNING/22023，Describe、Bind、
重复Execute及多行不重复通知。NULL和空结果也保留有效6，scalar/array均适用。
边界见[R127](issue-review-r127-time-excess-precision.md)：不表示DDL、SQL PREPARE、嵌套或
其他表达式、消息过滤GUC、其他时态类型和完整转换资格已完成；旧heap不升级。

实际声明OID1083的TIME参数支持八字节大端整数微秒binary Bind，范围
0–86400000000（包含24:00:00）；保留完整微秒后再由实际SQL声明执行0–6舍入。
不足八字节报08P01、越界22008，合法前八字节后有多余字节报22P03；错误后
连接可继续使用。范围优先于尾部检查，详见[R128](issue-review-r128-time-binary-parameter.md)。
这仅修接收codec，不迁移旧4字节TIME表行；不要据此认为旧表可保存全部微秒，
TIME[]、TIMETZ及时间戳微秒binary接收也不由此项声明完成。

TIMESTAMPTZ已支持的整秒binary参数按UTC时刻解释，不再被会话TimeZone二次
偏移；普通TIMESTAMP仍保留无时区时钟值，infinity/NULL行为保持。六时区、
text对照及named rebind的158控制见[R131](issue-review-r131-timestamptz-binary-timezone.md)。
R131只验收整秒时区，后续fractional输入的有限支持见下。

实际OID1114/1184的binary参数在基础civil范围（1..9999）支持精确整数微秒；
负epoch向下拆成整秒和正余数，`-1µs`仍是前一秒加999999µs，1184保持UTC时刻。
三时区/正负微秒/跨日/边界及named rebind验证见[R133](issue-review-r133-timestamp-binary-microseconds.md)。
这不迁移旧整秒heap，也不声明所有BC/大年份、typmod或错误状态边界完成。

基础TIMESTAMPTZ标量的DML RETURNING会按语句执行时会话TimeZone呈现时间和偏移，
同批后续SET不追溯修改先前结果；binary保持UTC时刻，NULL/infinity及TIMESTAMP/
TEXT对照不变。有限范围见[R135](issue-review-r135-timestamptz-returning-timezone.md)。
新SQL普通表的TIME/TIMESTAMP/TIMESTAMPTZ标量使用显式TemporalMicros列codec，
支持小数秒INSERT/UPDATE；TIME是八字节微秒（含24:00:00），timestamp是相对2000年
epoch的有符号微秒，epoch0不是NULL。声明precision在标量写入时舍入，TSTZ按UTC保存、
按会话时区显示；比较/索引查询不把查询值重新舍入到列precision。方法、证据与限制见
[R136](issue-review-r136-temporal-microsecond-storage.md)。

旧schema9/A/B/C列及既有native工厂继续使用Legacy秒codec，不自动迁移旧行。
同SQL类型的ALTER也会重写被转换列，不能只改标签后按微秒解释旧八字节；操作前备份，
在副本上验证。新表的TCM1单位扩展不能由旧程序读取；TIMETZ、数组precision、
BC/完整大年份及所有日期函数输入不由此声明完成。上方各R117–R133描述保留各自范围。

通用`SET TimeZone TO 'Asia/Shanghai'`支持TO分隔符，也保留等号和专用
`SET TIME ZONE 'Asia/Shanghai'`写法。TO在引号内仍是值的一部分，无效设置
不会改变原时区。Simple/Extended及六时区验证见[R132](issue-review-r132-timezone-set-to.md)。
显式`SET SESSION TimeZone TO 'Asia/Shanghai'`、等号及`SET SESSION TIME ZONE`
复用相同时区处理，见[R138](issue-review-r138-session-timezone.md)的224协议与真实前端验证。
合法的TimeZone `TO DEFAULT`、`= DEFAULT`及`TIME ZONE DEFAULT`（含SESSION）
恢复已保存默认值；专用`SET TIME ZONE LOCAL`也重置，而通用`SET TimeZone TO LOCAL`
仍报22023。带引号的DEFAULT/LOCAL不是关键字。RESET TimeZone/TIME ZONE/ALL按
已保存时区恢复名称和offset，见[R139](issue-review-r139-timezone-default.md)。
严格语法仍有R140已知问题：紧贴参数名的等号写法被拒，缺分隔符或专用等号
可能被错误接受，DEFAULT也存在两条非法语法放行，不能据上述功能宣称完整兼容。
事务级SET LOCAL、其它SESSION参数、非UTC startup和完整GUC事务语义仍有限。

### 其他类型
| 类型 | 说明 |
|------|------|
| `BOOLEAN` | true/false |
| `UUID` | 全局唯一标识符，如 `'550e8400-e29b-41d4-a716-446655440000'` |
| `JSON` / `JSONB` | JSON 数据，插入时自动验证 |
| `INET` | IPv4/IPv6 地址，如 `'192.168.1.1'` |
| `CIDR` | 网络地址，如 `'192.168.0.0/24'` |
| `MACADDR` | MAC 地址，如 `'08:00:2b:01:02:03'` |
| `MACADDR8` | 八字节EUI地址，如 `'08:00:2b:01:02:03:04:05'` |
| `ARRAY` | 数组，如 `INT[]` |
| `POINT` | 几何点 |
| `LINE` / `LSEG` / `BOX` / `PATH` / `POLYGON` / `CIRCLE` | 几何类型 |
| `TSVECTOR` / `TSQUERY` | 全文搜索 |
| `COMPOSITE` | 复合类型 (CREATE TYPE) |
| `ENUM` | 枚举类型 (CREATE TYPE ... AS ENUM) |
| `RANGE` | 范围类型，如 `INT4RANGE` |
| `DOMAIN` | 域类型 |

`DROP DOMAIN [IF EXISTS] schema.name [RESTRICT|CASCADE]`保留限定及quoted标识符，按实际选中的域身份执行。
当前只支持单目标；逗号多目标在事务和任何修改前报`0A000`，所有目标保留，不能当作PG多目标实现。
依赖对象的CASCADE仍有限，详见[R167](issue-review-r167-domain-drop-targets.md)。

新SQL composite定义在同一原子`.types`记录内保存decoded schema/localName及字段；
`DBMS_COMPOSITE_V2:`版本记录用hex封装身份和字段，不是heap格式升级、完整pg_type登记或通用wire复合值codec。
旧无点号physical-only记录可证明归public；旧带点号记录无法区分public."s.t"与s.t，
破坏性操作对歧义报`XX001`，不拆点号猜owner或自动迁移。先备份，在副本上确认真实身份；
不能保证导出或新建目录会自动解决歧义。两种SQL身份共用同一物理键的冲突仍不承诺支持。
原生`StorageEngine::CompositeType.name`仍是精确物理键；schema/localName须同时为空或同时合法，
同时为空只表示旧physical-only语义。详见[R168](issue-review-r168-composite-namespace-owner.md)。

`array_position(array, needle [, start])`返回首个匹配元素的真实下标，未找到时
为INTEGER SQL NULL（不是0）；显式数组下界保留，start必须匹配INTEGER声明。
查找按共同元素类型比较，`array_position('{1.00,2}'::numeric[],1::numeric)`
返回1，TEXT中的01/1仍不同。CHAR填充、精确数值、浮点NaN/±0、JSONB/range等
已验证范围及NULL/start顺序见[R108](issue-review-r108-array-position-typed-equality.md)。
非空JSON/几何数组的非NULL查找没有默认相等操作符，不能把任意文本比较当作
实现。用户同名重载保留真实返回声明；明确需要builtin可使用`pg_catalog.array_position`。

原生SelectExpr/queryExpr同样依赖真实数组列声明，不能把序列化数组的TEXT列
当作INTEGER[]。用TypeRegistry的resolveColumnType创建数组列可保留isArray；
四处原生投影类型提示修复及永久验证见[R129](issue-review-r129-native-array-projection-types.md)。
起点裸2147483648是BIGINT，按签名拒绝42883；显式INTEGER转换溢出为22003。

`pg_typeof(expr)`返回声明类型的regtype，包括typed NULL；分析/Describe不执行
参数，真正求值仍执行参数一次并保留其错误。原生AST的resolvedResultType不能
被opaque回调的unknown回退覆盖；真实builtin/stored声明优先，同名原生替换
不继承builtin固定类型。有限范围及验证见[R130](issue-review-r130-native-result-hint-precedence.md)。
ABS/LENGTH/UPPER/LOWER也按实际callback拥有签名；C++ registerFunction同名替换
不继承被替换函数的arity/参数转换/结果类型。未声明结果为UNKNOWN，显式AST
结果提示保留，分析不执行callback；有限验证见[R134](issue-review-r134-core-native-callback-ownership.md)。
`array_positions(array,needle)`收集全部匹配下标，返回INTEGER[]；例如
`array_positions('{1.00,2,1.0}'::numeric[],1::numeric)`得到`{1,3}`。无匹配/空
输入为`{}`，SQL NULL数组输入仍为SQL NULL，不提供start参数。真实Bind、数据
行与同名callee的验证范围见[R109](issue-review-r109-array-positions.md)。
通用复合/域多态和完整binary I/O仍未完成，已确认的数组binary结果与ARRAY
构造器规划问题也在该记录中列为独立待修复项。

网络类型的格式、CAST源类型和协议验收范围见[R102输入检查](issue-review-r102-network-input-notation.md)
与[R101 MAC检查](issue-review-r101-macaddr-input-and-widths.md)，不能据类型存在
推断整个PostgreSQL网络函数族已完成。INET无掩码要求完整IPv4地址，`'10/8'`
是带掩码缩写；CIDR文本输入不允许非零host bits，`'10.1'`表示`10.1.0.0/16`。
MACADDR8接受六字节EUI-48时会插入FF:FE，而不是在末尾补零。文本列/参数可
通过显式CAST输入，typed TEXT不能直接当作网络列的隐式赋值。NAME→MAC源
身份已由[R103](issue-review-r103-macaddr-name-input.md)独立修复；typed
INET→CIDR及网络赋值/共同值转换已按[R104](issue-review-r104-network-value-conversions.md)
独立修复并经649/原738及41邻接验收；typed INET按掩码清除host bits，
共同型优选INET，反向边只用于explicit/assignment，不扩为普通隐式转换。
限定NAME→INET/CIDR已按独立[R105](issue-review-r105-network-name-input.md)
修复并验证；NAME同样只能显式CAST，不能直接隐式赋给网络列。整族兼容未完成。

---

## 3. 数据库操作

```sql
-- 创建数据库
CREATE DATABASE dbname;

-- 使用数据库
SET compatibility_mode = extended;
USE DATABASE dbname;

-- 删除数据库
DROP DATABASE dbname;

-- 删除数据库 (级联)
DROP DATABASE dbname CASCADE;

-- 查看所有数据库
SHOW DATABASES;
```

---

## 4. 表操作

### 建表

```sql
-- 基本建表
CREATE TABLE users (
    ID INT PRIMARY KEY,
    NAME VARCHAR(50) NOT NULL,
    EMAIL VARCHAR(100) UNIQUE,
    AGE INT DEFAULT 0,
    ACTIVE BOOLEAN DEFAULT TRUE
);

-- 自增列
CREATE TABLE items (
    ID INT GENERATED ALWAYS AS IDENTITY,
    LABEL TEXT
);

-- 或使用 SERIAL (兼容语法)
CREATE TABLE items (
    ID SERIAL PRIMARY KEY,
    LABEL TEXT
);

-- 临时表
CREATE TEMPORARY TABLE tmp_data (X INT, Y INT);
-- 临时表仅在当前连接可见，断开连接后自动清理；支持 ON COMMIT
-- PRESERVE ROWS/DELETE ROWS/DROP，完整 pg_temp schema/catalog/search_path
-- 语义尚未实现。服务器重启时会清理异常退出遗留的临时物理文件。

-- Unlogged 表 (不写 WAL)
CREATE UNLOGGED TABLE cache (KEY TEXT, VALUE TEXT);

-- 分区表
CREATE TABLE events (
    ID INT,
    EVENT_TIME TIMESTAMP
) PARTITION BY RANGE (EVENT_TIME);

-- List 分区
CREATE TABLE logs (
    ID INT,
    REGION TEXT
) PARTITION BY LIST (REGION);

-- Hash 分区
CREATE TABLE hash_tbl (
    ID INT,
    HASH_KEY TEXT
) PARTITION BY HASH (HASH_KEY);

-- OF type (typed table)
CREATE TABLE typed_tbl OF some_composite_type;

-- LIKE 复制表结构
CREATE TABLE users_copy (LIKE src INCLUDING ALL);
CREATE TABLE users_defaults (LIKE src INCLUDING DEFAULTS);
CREATE INDEX users_indexes (LIKE src INCLUDING INDEXES);
CREATE TABLE users_constraints (LIKE src INCLUDING CONSTRAINTS);
CREATE TABLE users_identity (LIKE src INCLUDING IDENTITY);

-- 带 storage 参数
CREATE TABLE t (ID INT) WITH (FILLFACTOR = 90);
```

### 修改表

```sql
-- 添加列
ALTER TABLE users ADD COLUMN PHONE VARCHAR(20);
ALTER TABLE users ADD COLUMN SCORE INT DEFAULT 0;

-- 添加列 (IF NOT EXISTS)
ALTER TABLE users ADD IF NOT EXISTS PHONE VARCHAR(20);

-- 删除列
ALTER TABLE users DROP COLUMN PHONE;
ALTER TABLE users DROP IF EXISTS PHONE;

-- 修改列类型
ALTER TABLE users ALTER COLUMN AGE TYPE BIGINT;
ALTER TABLE users ALTER COLUMN AGE SET DATA TYPE VARCHAR(10);

-- 设置/删除默认值
ALTER TABLE users ALTER COLUMN AGE SET DEFAULT 18;
ALTER TABLE users ALTER COLUMN AGE DROP DEFAULT;

-- 设置/删除 NOT NULL
ALTER TABLE users ALTER COLUMN AGE SET NOT NULL;
ALTER TABLE users ALTER COLUMN AGE DROP NOT NULL;

-- 重命名列
ALTER TABLE users RENAME COLUMN AGE TO USER_AGE;
ALTER TABLE users RENAME COLUMN AGE TO USER_AGE; -- IF EXISTS

-- 重命名表
ALTER TABLE users RENAME TO app_users;

-- 添加约束
ALTER TABLE users ADD CONSTRAINT CHK_AGE CHECK (AGE >= 0);
ALTER TABLE users ADD PRIMARY KEY (ID);
ALTER TABLE users ADD UNIQUE (EMAIL);
ALTER TABLE users ADD FOREIGN KEY (DEPT_ID) REFERENCES depts(ID);
ALTER TABLE users ADD CONSTRAINT EXCL_EMAIL EXCLUDE USING btree (EMAIL WITH =);

-- 删除约束
ALTER TABLE users DROP CONSTRAINT CHK_AGE;
ALTER TABLE users DROP CONSTRAINT IF EXISTS CHK_AGE;
ALTER TABLE users DROP CONSTRAINT EXCL_EMAIL;

-- 重命名约束
ALTER TABLE users RENAME CONSTRAINT CHK_AGE TO CHK_USER_AGE;

-- SET / RESET storage 参数
ALTER TABLE users SET (FILLFACTOR = 80);
ALTER TABLE users RESET (FILLFACTOR);

-- 修改 replica identity
ALTER TABLE users REPLICA IDENTITY DEFAULT;
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE users REPLICA IDENTITY USING INDEX idx_name;

-- CLUSTER ON / SET WITHOUT CLUSTER
ALTER TABLE users CLUSTER ON idx_name;
ALTER TABLE users SET WITHOUT CLUSTER;

-- ENABLE/DISABLE TRIGGER
ALTER TABLE users DISABLE TRIGGER trigger_name;
ALTER TABLE users ENABLE TRIGGER trigger_name;

-- ROW LEVEL SECURITY
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE users FORCE ROW LEVEL SECURITY;

-- SET SCHEMA
ALTER TABLE users SET SCHEMA new_schema;

-- SET TABLESPACE
-- 先执行 CREATE TABLESPACE name LOCATION 'absolute/path'
ALTER TABLE users SET TABLESPACE my_space;

-- SET LOGGED / UNLOGGED
ALTER TABLE users SET LOGGED;
ALTER TABLE users SET UNLOGGED;

-- ALTER COLUMN SET STATISTICS
ALTER TABLE users ALTER COLUMN AGE SET STATISTICS 500;

-- INHERIT / NO INHERIT (表继承)
ALTER TABLE child INHERIT parent;
ALTER TABLE child NO INHERIT parent;

-- ATTACH / DETACH PARTITION
ALTER TABLE events ATTACH PARTITION events_p2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
ALTER TABLE events DETACH PARTITION events_p2024;

-- SET DEFAULT VALUES
ALTER TABLE users ALTER COLUMN ID SET DEFAULT 42;
```

已知限制：普通INSERT的单列PRIMARY KEY外键检查仍可能把20字节索引前缀
命中误当成完整父键存在，造成无效引用。候选修复与扩展测试见
[R137](issue-review-r137-foreign-key-full-value.md)，本项按用户要求暂缓，
未进入已接受实现；不能据初稿有限通过宣称外键完整性已修复。通用长PK写入、
全部复合/MATCH/deferred/partitioned/concurrent FK也不因该候选完成。

### 删除表

```sql
DROP TABLE users;
DROP TABLE IF EXISTS users;
DROP TABLE users CASCADE;
```

### 截断表

```sql
TRUNCATE TABLE users;
TRUNCATE TABLE users, logs, cache;
TRUNCATE ONLY users;
TRUNCATE TABLE parent RESTRICT;
TRUNCATE TABLE parent RESTART IDENTITY CASCADE;
```

`RESTRICT` 是默认行为：如果有外键依赖，语句会在任何表修改前失败；`CASCADE` 会递归截断外键依赖表。`RESTART IDENTITY` 重置被截断表的 identity 序列，`ONLY` 不扩展到继承子表。

---

## 5. 数据操作 (DML)

### 插入

```sql
INSERT INTO users (ID, NAME, AGE) VALUES (1, 'Alice', 25);
INSERT INTO users (ID, NAME) VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie', 30);

-- 多行插入
INSERT INTO users (ID, NAME) VALUES
    (4, 'Dave'),
    (5, 'Eve'),
    (6, 'Frank');

-- INSERT INTO ... SELECT
INSERT INTO users_backup SELECT * FROM users WHERE AGE > 20;

-- ON CONFLICT (UPSERT；当前 AST 路径为显式匹配单列/复合主键或 UNIQUE 约束 target + 常量或只引用 excluded 的受限标量表达式 SET/WHERE)
INSERT INTO users (ID, NAME, AGE) VALUES (1, 'Alice Updated', 26)
    ON CONFLICT (ID) DO UPDATE SET NAME = 'Alice Updated', AGE = 26;

-- DEFAULT VALUES
INSERT INTO users DEFAULT VALUES;

-- OVERRIDING
INSERT INTO users (ID, NAME) OVERRIDING SYSTEM VALUE VALUES (10, 'Test');

-- RETURNING
INSERT INTO users (ID, NAME) VALUES (100, 'New') RETURNING ID, NAME;
```

### 更新

```sql
UPDATE users SET AGE = 26 WHERE ID = 1;
UPDATE users SET AGE = AGE + 1 WHERE NAME LIKE 'A%';
UPDATE users SET NAME = 'Bob Updated', AGE = 31 WHERE ID = 2;

-- UPDATE FROM (多表更新)
UPDATE users SET AGE = orders.derived_age
FROM orders WHERE users.ID = orders.user_id;

-- RETURNING
UPDATE users SET AGE = 18 WHERE AGE < 18 RETURNING *;
```

### 删除

```sql
DELETE FROM users WHERE ID = 1;
DELETE FROM users WHERE AGE < 18;

-- DELETE USING
DELETE FROM users USING orders WHERE users.ID = orders.user_id;

-- RETURNING
DELETE FROM users WHERE ACTIVE = FALSE RETURNING ID;
```

### MERGE

```sql
MERGE INTO target USING source ON target.ID = source.ID
    WHEN MATCHED THEN UPDATE SET NAME = source.NAME
    WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (source.ID, source.NAME);
```

当前 typed executor 仅支持单源表、一个 `MATCHED` 分支和一个 `NOT MATCHED` 分支；支持 `UPDATE`/`INSERT`/`DO NOTHING`。多 `WHEN`、`BY SOURCE`/`BY TARGET`、`DELETE`、复杂 source query 和 `RETURNING` 会明确拒绝。

### REPLACE INTO (MySQL 兼容)

仅在显式 `extended` 兼容模式下可用；默认 `postgresql18` 模式返回
`42601` 并提示使用 `INSERT ... ON CONFLICT`。冲突替换按主键、列/表级
UNIQUE 及独立唯一索引查找所有冲突行，删除和插入属于同一语句事务，任一
行失败会整体回滚。

```sql
REPLACE INTO users (ID, NAME) VALUES (1, 'New Alice');
-- 等价于: DELETE + INSERT (冲突时)
```

---

## 6. 查询 (DQL)

### 基础查询

```sql
SELECT * FROM users;
SELECT NAME, AGE FROM users;
SELECT * FROM users WHERE AGE > 20;
SELECT * FROM users WHERE AGE > 20 AND NAME LIKE 'A%';
SELECT * FROM users WHERE ID IN (1, 2, 3);
SELECT * FROM users WHERE AGE BETWEEN 18 AND 65;
SELECT * FROM users WHERE NAME IS NULL;
SELECT * FROM users WHERE ACTIVE IS NOT NULL;

-- DISTINCT
SELECT DISTINCT DEPT FROM users;
SELECT DISTINCT ON (DEPT) NAME, SALARY FROM employees;

-- LIMIT/OFFSET
SELECT * FROM users LIMIT 10;
SELECT * FROM users LIMIT 10 OFFSET 20;
SELECT * FROM users FETCH FIRST 5 ROWS ONLY;
SELECT * FROM users FETCH FIRST 5 ROWS WITH TIES;

-- ORDER BY
SELECT * FROM users ORDER BY AGE DESC;
SELECT * FROM users ORDER BY AGE ASC, NAME DESC;
SELECT * FROM users ORDER BY AGE NULLS FIRST;
```

裸 ORDER BY 输出名字遇到不同表达式的同名列时返回 42702；同一已分析
表达式允许重复名字，并共享实际排序输出值。需要明确选择不同输出时可用
序号（例如 ORDER BY 2）。这与 ORDER BY 表达式中按输入列解析名字不同，
详见[重复别名规则](issue-review-o05-order-by-duplicate-alias.md)。

2026-11-02 起，类型化执行器中的普通有序 `LIMIT` 优先使用最多 `LIMIT+OFFSET` 个候选的有界堆，
覆盖执行阶段可接入的单表查询及参数化查询，保留多列方向、NULL 顺序、类型值和原始同行顺序。
所有必要排序键仍对全部输入求值；不参与排序的 volatile 输出按实际输出需求求值。
`DISTINCT`、`WITH TIES`、聚合/分组及边界溢出的路径不使用这个普通候选上限。
候选超过 `work_mem` 预算时转为有序段与外部归并，仍返回同一批前 `LIMIT+OFFSET` 行；
转换前已淘汰的行不必重新保留。排序键及原有提前投影仍对必要输入求值，不能因候选被淘汰而跳过原本应发生的副作用或错误。
候选数量与排序预算只约束排序节点：普通非分区 `TableScanOp` 已按页供给，分区/RLS、并行扫描及其他旧分流仍可能全量物化。
测量与适用范围见[Top-N 性能记录](performance-report-2026-10-30.md#有界排序-top-n2026-11-02)。

按页扫描为现有类型化消费者保留固定 MVCC 视图及打开时的堆页范围，每次只保留一页未展开的行，
释放页锁和 pin 后才执行表达式/展开当前行 TOAST。提前停止时统计实际交付行数，只有读到 EOF 才更新完整行数估计。
这不表示每种 SELECT 都已进入同一执行器；随后补充的普通单表 `SELECT id FROM t LIMIT 1` 已与数组等投影共用按需执行，
可支持形状之外的 JOIN、分组、视图和 LIMIT 表达式等仍按各自既有路径解释，见[普通 LIMIT 实测](performance-report-2026-10-30.md#普通-limit-按需执行与错误前缀2026-11-02)。

普通 LIMIT 路径保留真实投影需求：LIMIT 0 不打开输入；OFFSET 跳过的行仍可计算投影并产生副作用；
未被请求的后续行不会额外执行 WHERE/投影函数。DISTINCT 和聚合仍先完成各自所需输入，不把 LIMIT 下推为错误的输入截断。
支持的顶层类型化 SELECT 若在输出期间发生 SQL 错误，Simple Query 及不限 portal 行数的 Extended Execute 会先返回已产生的类型化行，再发送 ErrorResponse，不发送成功标签。
这不是网络端完全流式或有限行数 portal 的完成声明；R303/R304/R305 仍遵守[暂缓边界](deferred-scope.md)。
回归使用固定堆夹具观察行顺序；没有 ORDER BY 的生产查询不承诺固定的结果子集。

普通物理 `SortOp` 现在按 `dbms.conf` 中的 `work_mem_kb`（默认 4096 KiB）限制排序批次，
超预算时生成有序段并作临时文件归并；内存内完成时不创建排序文件。文件立即取消目录链接，最多使用两个文件描述符，
关闭、异常、取消及进程退出时释放。最终一轮归并随输出需求执行，提前关闭不再读后继记录；排序读取失败会立即释放其执行资源。
预算控制排序工作数据，单个超大行和有限的归并读头可超出预算；不是全进程 RSS 上限。

普通单表、单个物理列 ORDER BY 的受支持内置整数、浮点、字符及日期/时间键，在 Simple Query 或不限 portal 行数的 Execute 中可接入该路径；
能够解析到真实列的输出别名、ORDER BY 序号，以及普通 AND 筛选也可接入。保留 NULL、空串、文本 NULL、类型元数据与列顺序。
这条单键物理快路径继续保留。普通 `TypedSort` 另按打开时的有效 `work_mem` 限制排序批次，
覆盖已经接入类型化执行器的多个键、表达式键及 `SourceContexts` 逻辑/连接输入；
排序继续使用原有类型比较；NUMERIC、数组/对象身份和显式排序 COLLATE 等值的类型与元数据不会因临时往返丢失，
但不扩展这些类型原有的算子支持范围。
每个临时快照保留完整 `ExprValue` 的类型、NULL、collation、对象/数组 OID 与递归 record 字段，
以及行上下文的 SQL 值、名称值、参数值和绑定列值；缓存的键与提前投影值直接往返，不重新执行表达式。
排序以原始输入 ordinal 作为同行的最终次序，跨批次和归并保持稳定；`WITH TIES` 判断同行时只比较原有键，不把 ordinal 当作 SQL 键。
已接入的普通 `DISTINCT`/`WITH TIES` 可使用该排序预算，但 DISTINCT 自身保留状态不因此获得预算。
普通类型化比较描述按键惰性缓存，NULL 放置和浮点 NaN 等仍由原有类型规则决定，不提前触发原本没有比较需求的错误。

真实 Main 入口还接入了普通无 LIMIT 单表查询中的多键、表达式键及不能使用单键物理快路径的受支持类型；
单键物理快路径不为此改走类型化排序。这项入口扩展仍受现有类型化查询形状约束，不表示所有 SQL 都会进入同一排序器。
虚拟生成列、有限行数 portal，以及未接入的 OR/其他旧分流仍按各自既有路径解释。
已接入的 `UNION ALL` 全局 `SetSort` 也使用相同的会话预算、完整类型快照和匿名段归并；
静态计数或每次执行求出的非零有限计数，在没有 WITH TIES 且 LIMIT+OFFSET 加法安全时保留有界候选。
动态上限在每次打开或重开时重新设置，计数变为 NULL 时撤销上次上限；有序查询仍计算两个分支所需的全部输入。
浮点 NaN/Infinity/float4 精度和枚举次序使用实际绑定的比较规则；同行的输入 ordinal 只用于稳定输出。
已绑定为内置 `smallint`、`integer`、`bigint` 的集合排序键可在求值后缓存精确 int64 值，
比较仅在两侧原类型相同、缓存有效且没有枚举比较绑定时使用该值；NULL、升降序和同行规则保持。
解析失败、越界、域、OID、NUMERIC、浮点及其他类型仍使用原比较器，不新增提前校验或扩展类型转换规则。
缓存随完整行通过临时文件往返，独立校验缓存数量，不重复保存输出键；其向量容量计入排序预算。
普通集合查询如果所有排序键都是非整数类型，不为这项优化另分配整数缓存；VALUES 隐藏键复用原有缓存槽位。
原表达式比较仍使用每次调用自己的 owning 参数上下文，减少 initializer_list 引起的重复深拷贝，不跨比较保留 datum 或参数帧。
实现与完整新旧速度、分配请求和边界见[集合整数键与比较参数报告](performance-report-2026-10-30.md#集合整数键缓存与比较参数复制2026-11-02)。
聚合、GROUP BY/HAVING、其他未接入的集合分流、窗口、哈希、JOIN 本身及结果接收器的完整内存治理仍开放；
即使 JOIN 输出接入普通 `TypedSort`，预算也只约束其下游排序快照。
普通类型化排序同样允许单个超大行与固定数量归并读头超出预算；分配器开销和批次稳定排序的临时工作区也不在拥有值的容量估算内。
该估算约束批次及固定读头，不是全查询或全进程 RSS 上限。
完整快照只保存值，不改动实际执行、参数帧、相关子查询 provider 或 cursor 的 owner/lifecycle，
R303/R304/R305 继续遵守既有[暂缓边界](deferred-scope.md)。普通类型化排序的实现与性能证据见
[普通类型化排序报告](performance-report-2026-10-30.md#普通类型化排序的预算与比较缓存2026-11-02)。

普通 `VALUES` 查询支持自身的 ORDER BY 表达式及 LIMIT/OFFSET/FETCH 子句。输入各列先按共同类型绑定，
排序表达式随后看到真实的 `column1`、`column2` 等输出列；这些名称不因此在 VALUES 输入表达式中可见。
排序键可引用本层输出或通过标量子查询关联，复用实际执行上下文；落盘保留已经求值的隐藏键，WITH TIES 不重新执行键表达式。
LIMIT/OFFSET 支持有实际声明类型的参数与表达式，转换为 bigint，每次执行先求 OFFSET 再求 LIMIT；
OFFSET NULL 按零行处理，LIMIT NULL 表示不限行数；OFFSET/LIMIT 负数分别报 `2201X`/`2201W`。
本层输出列不能作为计数变量，报 `42P10`；外层关联和子查询自身的局部列分别按真实作用域绑定。
LIMIT 零仍先求计数表达式，不打开行源或排序器；已接入排序的动态计数也使用上述逐次更新的 Top-N 上限。

普通 `UNION ALL` 的全局 LIMIT/OFFSET 也支持表达式、参数和真实标量子查询，括号与未括号集合根使用同一计数路径。
全局计数没有集合输出列或任一分支 FROM 的本层命名空间；只存在于集合输出中的别名报 `42703`，引用不可见的分支限定名报 `42P01`。
真实祖先引用仍可关联，子查询自己的局部列可遮蔽祖先同名列；分支括号内的局部计数与全局计数分别生效。
执行使用该集合查询原有的实际上下文，每次重开先更新上下文，再按 OFFSET、LIMIT 顺序求值一次。
OFFSET 失败不会继续求 LIMIT；计数失败或为零时不打开两个分支。无排序时保持按需读取，有排序时保留全部输入表达式的原有需求。

已接入的 VALUES/集合根支持 `FETCH FIRST|NEXT` 的参数、函数调用、括号表达式等 `c_expr` 及带符号数字字面量；省略计数时为 1。
FETCH 的任意算术表达式应加括号，例如 `FETCH FIRST (1+1) ROWS ONLY`；`FETCH FIRST 1+1 ROWS ONLY` 报 `42601`。
带 ROW/ROWS 后缀的 OFFSET 使用相同的简单值表达式语法；普通 LIMIT/OFFSET 不带该后缀时可使用完整表达式。
裸 NULL（包括只加括号的 NULL）配合 FETCH WITH TIES 在分析时报 `2201W`；
`(NULL::bigint)`、CASE 或参数等表达式在执行时得到 NULL 则不限行数，WITH TIES 也不把它改成零或错误。
WITH TIES 仍要求 ORDER BY，判断同行只使用真实排序键，并取消普通 Top-N 截断。

未指定类型 OID 的参数已支持按实际选中的计数、算术、CAST、CASE/COALESCE 等上下文推导输入声明，
同一参数的多处引用保持实际身份与槽位；计数结果为 bigint 不表示每个输入都声明为 bigint。
这仍是普通元数据分析的有限子集，完整函数签名推导和任意多处参数组合继续开放。
受支持的合法窗口标量子查询（例如计数中的 `SELECT row_number() OVER()`）已进入真实类型化窗口宿主，
可使用自身的行源、过滤和最终子句；关联值及重开继续沿用实际查询上下文。
本层计数直接使用窗口函数经真实 AST 分析报 `42P20`；聚合和集合返回函数也按各自真实角色拒绝，
子查询自身的合法角色不因此被当作本层调用。普通 SELECT 的动态计数及其他未接入组合仍未完整覆盖。
这些扩展不改变保留计划、参数帧、provider、portal 或无效结果格式的既有边界，QRY-06 与原 273 项不因此关闭。

```sql
VALUES (3, 'c'), (1, 'a'), (2, 'b')
ORDER BY column1 + 1 DESC LIMIT 1 OFFSET 1;

(VALUES (3), (1), (2) UNION ALL VALUES (4))
ORDER BY 1 LIMIT (1+1) OFFSET (1-1);

VALUES (3), (1), (1), (2)
ORDER BY column1 FETCH FIRST (1+0) ROWS WITH TIES;
```

查询结果中的实际绑定描述符继续携带枚举类型 OID、长度和 typmod，正常 text/binary 输出使用枚举标签；
SQL NULL、空标签和文本 `NULL` 区分。该支持不等于枚举数组二进制输入/输出、任意自定义类型编解码或有限行数 portal 完成。
预算排序和枚举结果描述符的历史实测见[集合排序与 VALUES 报告](performance-report-2026-10-30.md#集合排序预算与-values-查询子句2026-11-02)；
全局计数与 FETCH 的本轮验证进度、实际性能和开放项以[性能报告](performance-report-2026-10-30.md)的后续记录为准。

会话 `SET [SESSION|LOCAL] work_mem` 现在驱动上述普通物理、普通类型化及已接入的 UNION ALL/VALUES 排序路径，支持 DEFAULT、FROM CURRENT、SHOW、RESET/RESET ALL/DISCARD ALL 与启动 options。
SQL 值范围为 64..2147483647 kB，支持 B/kB/MB/GB/TB（单位区分大小写）及按 PostgreSQL 规则舍入。
普通 SET 在提交后保留，回滚取消；LOCAL 在事务结束时恢复，保存点回滚也恢复。块外 LOCAL 警告 25P01 且无持久效果，非法值仍报错。
没有显式覆盖时跟随最新实例重载，启动 options 的重置默认值保留，FROM CURRENT 成为显式覆盖。排序打开后不改变其已截取的预算。

```sql
SET work_mem = '16MB';
SHOW work_mem;
BEGIN;
SET LOCAL work_mem = '64kB';
SELECT * FROM users ORDER BY AGE;
COMMIT;
RESET work_mem;
```

`pg_settings.setting/unit` 与现有 EXPLAIN SETTINGS 观察有效会话值；目录仍是已有有限列子集。
`current_setting`/`set_config`、函数 SET 子句和内部例程 GUC 完整作用域尚未实现/验收；上述范围以外的排序/算子完整预算也保持开放。
实例 `work_mem_kb` 仍保留项目原生的微型测试预算，区别于 PG SQL 的 64 kB 下限。验证及性能见[会话工作内存报告](performance-report-2026-10-30.md#会话工作内存与实际排序预算2026-11-02)。

最新按需归并的验证、性能与仍开放的资源边界见[最终归并报告](performance-report-2026-10-30.md#最终归并按需输出与排序绑定恢复2026-11-02)。
排序中断仍按 [R185](issue-review-r185-sort-interrupt-propagation.md)及本批报告所列实际范围解释，不据此宣称完整 PostgreSQL cancel 语义；
[R197 COPY 响应/中断归属](issue-review-r197-copy-response-interrupt-scope.md)仍依照其专属记录，不由普通排序资源修复关闭。

### 聚合

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(DISTINCT DEPT) FROM users;
SELECT MAX(SALARY), MIN(SALARY), AVG(SALARY) FROM employees;
SELECT SUM(AMOUNT) FROM orders;

-- GROUP BY
SELECT DEPT, COUNT(*) FROM employees GROUP BY DEPT;
SELECT DEPT, AVG(SALARY) FROM employees GROUP BY DEPT HAVING AVG(SALARY) > 50000;
SELECT DEPT, COUNT(*) FROM employees GROUP BY ROLLUP (DEPT);
SELECT DEPT, TEAM, COUNT(*) FROM employees GROUP BY CUBE (DEPT, TEAM);
SELECT DEPT, COUNT(*) FROM employees GROUP BY GROUPING SETS ((DEPT), ());
```

### JOIN

```sql
SELECT * FROM users INNER JOIN orders ON users.ID = orders.USER_ID;
SELECT * FROM users LEFT JOIN orders ON users.ID = orders.USER_ID;
SELECT * FROM users RIGHT JOIN orders ON users.ID = orders.USER_ID;
SELECT * FROM users FULL OUTER JOIN orders ON users.ID = orders.USER_ID;
SELECT * FROM users CROSS JOIN orders;

-- NATURAL JOIN
SELECT * FROM users NATURAL JOIN user_profiles;

-- JOIN USING
SELECT * FROM users JOIN orders USING (ID);

-- 多表 JOIN
SELECT u.NAME, o.AMOUNT, p.PRODUCT_NAME
FROM users u
JOIN orders o ON u.ID = o.USER_ID
JOIN products p ON o.PRODUCT_ID = p.ID;

-- FOR UPDATE
SELECT * FROM users WHERE ID = 1 FOR UPDATE;
SELECT * FROM users WHERE ID = 1 FOR SHARE;
SELECT * FROM users WHERE ID = 1 FOR NO KEY UPDATE;
SELECT * FROM users WHERE ID = 1 FOR KEY SHARE;
SELECT * FROM users WHERE ID = 1 FOR UPDATE NOWAIT;
SELECT * FROM users WHERE ID = 1 FOR UPDATE SKIP LOCKED;
```

### 子查询

```sql
-- IN 子查询
SELECT * FROM users WHERE ID IN (SELECT USER_ID FROM orders);

-- EXISTS 子查询
SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.USER_ID = u.ID);

-- ANY/ALL
SELECT * FROM employees WHERE SALARY > ANY (SELECT SALARY FROM managers);
SELECT * FROM employees WHERE SALARY > ALL (SELECT SALARY FROM interns);

-- 标量子查询
SELECT NAME, (SELECT COUNT(*) FROM orders o WHERE o.USER_ID = u.ID) AS order_count FROM users u;

-- 派生表
SELECT * FROM (SELECT ID, NAME FROM users WHERE AGE > 20) AS adults;
```

### UNION / INTERSECT / EXCEPT

```sql
SELECT NAME FROM active_users
UNION
SELECT NAME FROM archived_users;

SELECT NAME FROM active_users
UNION ALL
SELECT NAME FROM archived_users;

SELECT A FROM t1 INTERSECT SELECT A FROM t2;
SELECT A FROM t1 EXCEPT SELECT A FROM t2;
```

### CTE (公用表表达式)

```sql
WITH vip_users AS (SELECT ID, NAME FROM users WHERE VIP = 1)
SELECT vip_users.NAME, orders.AMOUNT
FROM vip_users JOIN orders ON vip_users.ID = orders.USER_ID;

-- RECURSIVE CTE
WITH RECURSIVE cte AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 100
)
SELECT * FROM cte;
```

### 窗口函数

2026-11-03，基于源码 `9f019ffa` 的前批最终 C9 程序 `c696d3b2` 已将受支持的普通窗口查询接入
Main、Core 和 SQL 子查询共用的类型化 WindowAgg。它执行真实行源、WHERE、窗口计算、投影及最终子句；
支持已接入的 derived table、CTE、实际 Main/SourceContexts JOIN 和关联子查询，
同一算子图重开时保留真实外层上下文与拥有值的类型、NULL 和对象元数据。
这不代表通用 Core JOIN 已完成 lowering，也不扩大保留计划或参数帧的既有职责。

支持命名窗口及合法继承、ROWS/GROUPS 和数值排序键的 RANGE、合法空帧及
EXCLUDE CURRENT ROW/GROUP/TIES/NO OTHERS。frame offset 按原始 OVER/WINDOW 子句的实际 stage 初始化；
共享排序键不合并独立 frame 的初始化。带 offset 的 DATE/INTERVAL RANGE 仍缺少完整 temporal in_range 支持。
排名、NTILE、LAG/LEAD、FIRST/LAST/NTH_VALUE 使用真实窗口行与帧；
聚合支持已绑定签名的 COUNT/SUM/AVG/MIN/MAX、BOOL_AND/BOOL_OR/EVERY、标量 ARRAY_AGG、
TEXT/BYTEA STRING_AGG 及整数/BIT 的 BIT_AND/BIT_OR/BIT_XOR。

每行按需求只求值一次的完整分区/排序键可供后续 stage 和匹配的投影子树、窗口参数及 FILTER 复用；
frame offset 不使用这种输入值复用。241 项输入键协议控制与 PG 18.6 的规范化值、计数及消息字节一致。
参数、LAG/LEAD 默认值和聚合 FILTER 的实际求值需求保留，FILTER 先于被过滤行的聚合参数。
仅在实际窗口生产者保证最终 ORDER BY 为已有顺序的兼容前缀时省去最终 Sort；
方向、表达式或分区顺序不兼容时仍排序。窗口 WITH TIES 使用已有键值判断同行，
仍读取首个非同行边界行，OFFSET 跳过的行及边界行保留相应求值和错误需求；LIMIT 0 不打开输入。

AVG 使用实际声明的结果类型；NUMERIC 保留精确十进制，受支持的 INTERVAL 除法及窗口 AVG
已补月到日、日到微秒的余数换算与舍入，未据此宣称完整 interval 运算兼容。
统计窗口聚合（方差/标准差、相关/协方差、回归）的合法签名仍报 `0A000`；
window DISTINCT、聚合内部 ORDER BY、数组值 ARRAY_AGG 及完整聚合族仍未完成。
窗口 stage 排序、分区和历史行仍可全量驻留内存，尚无 work_mem 上限或 spill，
已有序输出按需消费不等于窗口查询已具备完整内存治理。

随后基于 `b04db801` 的 C1 程序 `56d06bca`，只在窗口私有实现 `85aef835` 中增加整数键缓存。
仅当实际比较已遇到非 NULL、无枚举比较绑定的两个键，原始类型同为 `smallint`、`integer` 或 `bigint`，
且真实已绑定 OID 分别为 21、23 或 20 时，才在该 stage 拥有的键单元首次比较时解析并缓存精确整数。
OID 为 0、真实 domain、仅有类型显示名而缺少权威 OID 的别名/隐藏表达式，以及未完整解析或越界的文本，
继续交原比较器处理；缓存覆盖范围不足不会新增 SQL 拒绝。原 NULL 方向、同行、参数来源、frame/provider、
Main、共享比较器及 R185 排序 helper 均未改动，重开不复用上次执行的解析值。

缓存有内存代价：所有缓冲 Input 行都增加 vector 头和外层容量占用，含纯回退类型；
行内整数 payload 和 stage 类型元数据按实际比较需求分配。close 释放行内 payload 和类型元数据，
外层缓冲容量仍可保留至算子图销毁；这既不是窗口 work_mem/spill 支持，也不是 RSS 降低的证据。
额外 18 个进程的 162 次 allocator 快照全部完成：1,000 行整数控制首/末行的 uordblks 中位差为 +68,160 B，
文本回退为 +22,544 B、单行为 +48 B；这些进程分配器快照不是精确 live 对象或真实峰值测量。
该 C1 批次的 71 个 native 入口全部通过（新增 36 项控制）；61 个协议入口为 56 通过、5 失败。
新增 143 项协议控制保留 14 项与 C9 完全相同的既有差异，其余四个失败入口的观察也未变化。
共同八类查询的 public cursor 独立执行阶段进程 CPU p50 降低 41.85–61.90%，C++ 分配请求减少 53.89–63.96%。
另行完成的整 SQL 24 组对照中，4 MB/16 MB 的 16 项 p50 均比两次 C9 低 22.3–39.1%；
64 kB 只有 2 项比两旧都快，3 项比两旧都慢（partition_lag、running_sum、partition_full_sum），其余混合。
64 kB 的 C9 首/回测漂移为 +13.2% 至 +75.8%，仍无统一加速结论，也不重写前批对 old58 的七类慢项或 PG 倍率。
前批 35 项数据正确、但停机失败的记录继续保留，本批未重跑。
缓存证据与限制见[窗口整数键缓存报告](performance-report-2026-10-30.md#窗口整数键缓存与实际执行成本2026-11-03)。

基于 `d7793044` 的中间候选 S2 程序 `779617cb` 补入受支持形状的窗口结果共享，以下 S2 测量按原代别保留。
同一真实窗口定义中，等价且非 volatile 的窗口调用可以复用结果；是否 volatile 按实际函数声明及参数/FILTER 判断。
含 volatile 参数的调用不会全局合并：只有实际最终 Sort 保留下层排序输入投影时，最后一个实际需要的窗口 stage
才按“先保留必须提前计算的完整目标，再抽取可推迟目标中的窗口节点”的规则共享等价完整结果。
对这种 volatile 排序投影复用，没有最终 Sort、已由窗口顺序省去 Sort、不同窗口定义或独立 SQL 子查询站点继续保留各自需求；
保留包装表达式内的窗口子节点也不等于已有独立结果，普通标量调用不会因此共享。
每次打开算子从实际需要的别名中选代表，每行计算后向各原结果列分发拥有值，保留类型、NULL、record/OID/数组元数据；
新增共享只向该行分发，不以跨行或跨重开结果缓存替代后续求值，也不替换原参数 frame/provider。

S2 的准备期 v4 只按必要候选构造身份，并在本次准备内复用同一原 AST 的元数据字符串；没有新增表达式求值或 SQL 重写。
固定新 220 项在 PG 18.6、S2 均无差异，前版 C1 的 27 项差异全部消除；原 143 项的失败从 14 降至 8，原四个失败入口的 1641 项观察不变。
72 个 native 入口通过。首次完整 62 个协议入口为 56/6，其中一个入口在连接启动时失败、尚未执行其 190 项 SQL；
保留该记录后，同源 S2 独立复核完整采集 190 项、0 预期差异，最终 62 个独立入口为 57 通过、5 保留失败。
512 行真实 heap 的已绑定游标成本控制中，重复两份/四份 LAG 的累计 C++ 分配请求分别减少 44.65%/69.70%，
单份 LAG 仍多 1 次请求、8 B；这不代表 SQL 速度、峰值内存或 RSS 改善。
另行完成的资源 55 项全部符合预期，包括 40 次完整读取、一次预期终止及 14 项 SET；40,000 行、118,000 单元值校验通过。
64 kB 的最终 TypedSort 最多观察到两个临时 FD，EOF 后无残留且同目录重开成功；这不证明窗口内存受预算限制，也不关闭旧停机失败。
三预算整 SQL 对照保留 1296 次计时、324 次预热和 18 项 SET/SHOW，共 1,638 次请求、1,620,000 行和 4,725,000 单元，均符合固定值预期。
速度没有全面改善：共同八类的 24 项中，17 项的 S2 p50 高于两次 C1；重复两份/四份 LAG(length) 的三预算控制则较两旧分别降低 37.95–41.66%/64.46–66.16%。
全部 36 项、旧版漂移、规则及剩余范围见[窗口结果共享报告](performance-report-2026-10-30.md#窗口结果共享与排序投影2026-11-03)。
窗口内存治理、完整已分析表达式等价及复合 binary 输出等仍有缺口，原 273 项及旧停机失败记录不因本批通过而关闭。

当前 S3 程序 `aa52b5fe` 保留同一共享规则，只让结果映射和调用顺序均未变化的 stage 使用原类型化需求检查和 owning move 循环。
任何可能共享、隐藏 ORDER 重排或仅别名被需求的组继续走原 S2 通用路径；实际参数 frame/provider、窗口键、帧和普通 Core 规划职责不变。
S3 的 72 个 native 入口通过，完整 62 个协议入口首轮为 57 通过、5 保留失败。
新 220 项及原 143 项的全部原始观察与 S2 相同；原四个失败入口的 1641 项也保持原响应。
190 项此次首轮完整通过，服务端原始帧与 S2 独立复核逐项相同；随机私有命名使请求日志不同，S2 原启动失败仍保留。
512 行公开游标控制中，单份 LAG 的分配请求恢复为与 C1 相同的 226489 次；两份/四份 LAG 的请求仍分别减少 44.65%/69.70%。
S3 同范围资源 55 项独立通过，仍不证明窗口预算、任意写入恢复或旧停机问题已完成。
S3 三预算整 SQL 对照已完成 1296 次计时、324 次预热及 18 项 SET/SHOW，共 1,638 次请求；1,620,000 行/4,725,000 单元均符合固定预期。
共同八类的 24 项按 p50 数值为 3 项低于两次 C1、16 项高于两旧、5 项居间；64 kB 的共同八项全部高于两旧，不能称整体提速。
重复两份/四份 LAG(length) 的三预算控制较两旧分别降低 37.94–40.83%/63.65–66.34%。这些是独立 C1/S3/C1 窗口，
上面的 S2 全部 36 项、慢值和诊断继续保留，不将两轮结果当作同窗 S3/S2 速度比；完整样本与漂移见[窗口结果共享报告](performance-report-2026-10-30.md#窗口结果共享与排序投影2026-11-03)。

最新暂存容量优化的两版候选均未接受，当前程序仍为S3；减少分配不代表查询稳定提速。
完整三预算对照、冷/热执行成本及尚未接受的类型输出提案见[本批报告](performance-report-2026-10-30.md#窗口暂存分配与类型输出调研2026-11-03)。
SQL接口和C++17标准不因这轮研究变化，未宣称补齐record/array二进制输出或窗口内存预算。

前批宿主证据与性能边界见[类型化窗口宿主报告](performance-report-2026-10-30.md#类型化窗口宿主与有序输出复用2026-11-03)；
历次专项保留在[逐项修复进度](review-fixes-2026-10-13.md)和[frame 说明](issue-review-r04-window-frame-boundaries.md)。
以下示例展示已支持语法的使用方式，不代表完整 PostgreSQL 窗口兼容。

```sql
SELECT NAME, ROW_NUMBER() OVER (ORDER BY SCORE DESC) FROM users;
SELECT NAME, RANK() OVER (ORDER BY SCORE DESC) FROM users;
SELECT NAME, DENSE_RANK() OVER (ORDER BY SCORE DESC) FROM users;
SELECT NAME, LAG(NAME, 1) OVER (ORDER BY AGE) FROM users;
SELECT NAME, LEAD(NAME, 1) OVER (ORDER BY AGE) FROM users;
SELECT NAME, FIRST_VALUE(NAME) OVER (ORDER BY SCORE) FROM users;
SELECT NAME, LAST_VALUE(NAME) OVER (ORDER BY SCORE) FROM users;
SELECT NAME, NTILE(4) OVER (ORDER BY SCORE) FROM users;
SELECT NAME, PERCENT_RANK() OVER (ORDER BY SCORE) FROM users;
SELECT NAME, CUME_DIST() OVER (ORDER BY SCORE) FROM users;

-- PARTITION BY + ORDER BY
SELECT NAME, DEPT, ROW_NUMBER() OVER (PARTITION BY DEPT ORDER BY SALARY DESC)
FROM employees;

-- 聚合参数也可以是表达式，结果保留声明类型和 SQL NULL
SELECT NAME, SUM(SALARY + 100) OVER (
    PARTITION BY DEPT ORDER BY AGE ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
) AS total FROM employees;

-- EXCLUDE CURRENT ROW / GROUP / TIES / NO OTHERS
SELECT NAME, AVG(SALARY) OVER (ORDER BY AGE ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW)
FROM employees;
```

### EXPLAIN

```sql
EXPLAIN SELECT * FROM users WHERE ID = 1;
EXPLAIN (ANALYZE) SELECT * FROM users WHERE AGE > 20;
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM users;
EXPLAIN (FORMAT JSON) SELECT * FROM users;
```

---

## 7. 事务控制

```sql
BEGIN;
BEGIN TRANSACTION;
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN READ ONLY;
BEGIN TRANSACTION READ WRITE;

-- DML 操作...
COMMIT;
ROLLBACK;

-- AND CHAIN / AND NO CHAIN
COMMIT AND CHAIN;
COMMIT AND NO CHAIN;
ROLLBACK AND CHAIN;
ROLLBACK AND NO CHAIN;

-- SAVEPOINT
SAVEPOINT sp1;
ROLLBACK TO SAVEPOINT sp1;
RELEASE SAVEPOINT sp1;

-- PREPARE TRANSACTION / COMMIT PREPARED
PREPARE TRANSACTION 'txn_001';
COMMIT PREPARED 'txn_001';
ROLLBACK PREPARED 'txn_001';
```

包含跨语句 DDL CREATE undo 的事务暂不支持 `PREPARE TRANSACTION`；请先完成 `COMMIT` 或 `ROLLBACK`。

### 顾问锁

顾问锁按当前数据库和 key 隔离；单个 `bigint` 与两个 `integer` 使用不同
namespace。session 锁保持到匹配次数的 unlock、`pg_advisory_unlock_all()`、
`DISCARD ALL`、切库或断连；transaction 锁在提交/回滚时自动释放，并遵循
保存点回滚。shared holder 可以并存，exclusive holder 与其他 session 冲突；
`pg_try_*` 立即返回 `boolean`，非 try 形式会等待。

```sql
SELECT pg_advisory_lock(42);                 -- void
SELECT pg_try_advisory_lock(42);             -- boolean
SELECT pg_advisory_unlock(42);               -- boolean
SELECT pg_advisory_lock_shared(1, 2);
SELECT pg_try_advisory_lock_shared(1, 2);
SELECT pg_advisory_unlock_shared(1, 2);

BEGIN;
SELECT pg_advisory_xact_lock(-42);
SELECT pg_try_advisory_xact_lock_shared(7, 9);
COMMIT;

SELECT pg_advisory_unlock_all();             -- void
```

函数也接受显式 `pg_catalog.` 限定名和输出列别名。`pg_locks` 当前会列出
顾问锁的 key、mode 与 granted/waiting 状态；完整 PostgreSQL locktag、
fastpath、waitstart 及 predicate-lock 列仍由 `MON-05` 跟踪。

### LISTEN / NOTIFY

```sql
LISTEN job_finished;
NOTIFY job_finished, 'job 42';
SELECT pg_notify('job_finished', 'job 43');
SELECT pg_listening_channels();
SELECT pg_notification_queue_usage();
UNLISTEN job_finished;
UNLISTEN *;
```

订阅变更和通知都服从事务边界：COMMIT 后生效，ROLLBACK（包括回滚到
SAVEPOINT）丢弃对应变更；接收端处于事务中时，已经提交的通知保留到该事务
结束后发送。相同事务内 channel/payload 完全相同的通知会合并。channel 是
identifier，按 UTF-8 安全边界限制为 63 bytes；payload 必须短于 8000 bytes，
不能包含零字节。通知按数据库和 backend 隔离，以 PostgreSQL
`NotificationResponse` 帧异步发送，同一角色的不同连接拥有各自队列。

队列容量由启动参数 `max_notify_queue_pages` 控制；提交前会预留容量，队列满
返回 `54000` 并保持事务可回滚。含 LISTEN/UNLISTEN/NOTIFY 动作的事务不能
`PREPARE TRANSACTION`，返回 `0A000`。

---

## 8. 索引

```sql
-- B+ 树索引
CREATE INDEX idx_name ON users (NAME);
CREATE UNIQUE INDEX idx_email ON users (EMAIL);

-- 复合索引
CREATE INDEX idx_name_age ON users (NAME, AGE);

-- Hash 索引
CREATE INDEX idx_email_hash ON users USING HASH (EMAIL);
CREATE INDEX idx_body_gin ON users USING GIN (BODY);
CREATE INDEX idx_id_gist ON users USING GiST (ID);
CREATE INDEX idx_id_brin ON users USING BRIN (ID);
CREATE INDEX idx_id_spgist ON users USING SPGIST (ID);

-- 覆盖索引 (INCLUDE)
CREATE INDEX idx_name_include ON users (NAME) INCLUDE (AGE, EMAIL);

-- 部分索引
CREATE INDEX idx_active ON users (NAME) WHERE ACTIVE = TRUE;

-- 表达式索引
CREATE INDEX idx_upper_name ON users ((UPPER(NAME)));

-- 带排序方向
CREATE INDEX idx_age_desc ON users (AGE DESC NULLS LAST);

-- 并发创建
CREATE INDEX CONCURRENTLY idx_name ON users (NAME);

-- 删除索引
DROP INDEX idx_name;
DROP INDEX IF EXISTS idx_a, idx_b CASCADE;

-- 重索引
REINDEX TABLE users;
REINDEX INDEX idx_name;

-- 查看索引信息
SHOW INDEX FROM users;
```

---

## 9. 视图与物化视图

普通视图的 `CREATE OR REPLACE` 保留既有列名、类型和类型修饰符，允许末尾追加
列；不兼容替换返回 `42P16` 并保留旧定义。创建时静态绑定输出，不提前执行
查询或消耗序列。物化视图的支持范围独立于普通视图替换。

```sql
-- 视图
CREATE VIEW vip_users AS SELECT * FROM users WHERE VIP = 1;
CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE ACTIVE = TRUE;

-- WITH CHECK OPTION
CREATE VIEW young_users AS SELECT * FROM users WHERE AGE < 30 WITH CHECK OPTION;

-- 删除视图
DROP VIEW vip_users;

-- 物化视图
CREATE MATERIALIZED VIEW mv_dept_stats AS
    SELECT DEPT, COUNT(*), AVG(SALARY) FROM employees GROUP BY DEPT;

CREATE MATERIALIZED VIEW mv_empty WITH NO DATA;

-- 刷新
REFRESH MATERIALIZED VIEW mv_dept_stats;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dept_stats;
REFRESH MATERIALIZED VIEW mv_dept_stats WITH NO DATA;
```

---

## 10. 约束

```sql
-- CHECK 约束 (支持多个命名)
CREATE TABLE products (
    ID INT PRIMARY KEY,
    PRICE INT CONSTRAINT POSITIVE CHECK (PRICE > 0),
    CONSTRAINT MAX_PRICE CHECK (PRICE < 10000)
);

-- 表级约束全名约束名
ALTER TABLE products ADD CONSTRAINT CHK_PRICE_RANGE CHECK (PRICE > 0 AND PRICE < 10000);

-- 外键级联
CREATE TABLE orders (
    ID INT PRIMARY KEY,
    USER_ID INT REFERENCES users(ID) ON DELETE CASCADE ON UPDATE SET NULL
);
```

---

## 11. 用户与权限

```sql
-- 创建用户
CREATE USER alice WITH PASSWORD 'secret';

-- 创建超级用户
CREATE USER admin WITH PASSWORD 'change-this-password' SUPERUSER;

-- 删除用户
DROP USER alice;

-- 权限管理
GRANT SELECT ON users TO alice;
GRANT SELECT, INSERT, UPDATE ON users TO alice;
GRANT ALL PRIVILEGES ON DATABASE mydb TO admin;
GRANT SELECT (ID, NAME) ON users TO alice;  -- 列级权限
GRANT SELECT ON ALL TABLES IN SCHEMA public TO alice;
GRANT ALL ON users TO alice WITH GRANT OPTION;

-- 撤销权限
REVOKE SELECT ON users FROM alice;
REVOKE ALL PRIVILEGES ON DATABASE mydb FROM admin;
REVOKE GRANT OPTION FOR INSERT ON users FROM alice CASCADE;

-- ALTER DEFAULT PRIVILEGES
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO alice;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT ON TABLES TO alice, bob;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM alice;
```

当前仅支持表级 `TABLE`/`TABLES` 默认权限。规则只影响之后创建的表，重复 GRANT 不会重复写入；`WITH GRANT OPTION`、`GRANT OPTION FOR` 以及 sequence/function 等对象类型会明确报错。

---

## 12. 角色管理

```sql
-- 创建角色
CREATE ROLE readonly_role;
CREATE ROLE dev WITH CREATEDB LOGIN CONNECTION LIMIT 100;

-- 创建用户 (用户的特殊形式)
CREATE USER bob WITH PASSWORD 'secret' LOGIN IN ROLE readonly_role;

-- 授予/撤销角色
GRANT readonly_role TO bob;
GRANT readonly_role TO bob WITH ADMIN OPTION;
REVOKE ADMIN OPTION FOR readonly_role FROM bob;
REVOKE readonly_role FROM bob;

-- 修改角色属性
ALTER ROLE bob WITH SUPERUSER CREATEROLE REPLICATION BYPASSRLS;
ALTER ROLE bob WITH PASSWORD 'new_secret' VALID UNTIL '2026-12-31';
ALTER ROLE bob CONNECTION LIMIT 50;
ALTER ROLE bob NOLOGIN;
ALTER ROLE bob RENAME TO robert;
```

---

## 13. 存储过程与函数

受支持的schema-qualified标量表查询保留完整AST与callee身份，不把schema.name整体当作单个函数名。
原生`SelectExpr.funcName`仍是已解码名称，不是含SQL双引号的片段；不可全局strip、lowercase或拆分其中点号。
未限定quoted builtin表投影仍有已记录缺口，不能从[R161](issue-review-r161-qualified-projection-routine-identity.md)推广为已修复。

普通命名存储标量函数按实际输入声明选择及转换，metadata/Describe不执行函数体来猜类型；
范围见[R162](issue-review-r162-generic-stored-signatures.md)。普通enum/domain/composite的SQL/PLpgSQL
标量声明及canonical namespace身份见[R164](issue-review-r164-routine-custom-type-declarations.md)，
不等于通用复合值执行、全部重载/typmod或用户数组工厂完成。
`degrees`/`radians`的实际builtin数值范围与次正规数修复见[R158](issue-review-r158-angle-conversion-range.md)；
纯常量的完整规划错误时机仍由[R160](issue-review-r160-angle-constant-demand.md)独立跟踪。

```sql
-- 创建函数
CREATE FUNCTION add_one(INT) RETURNS INT AS 'return $1 + 1;' LANGUAGE sql;

CREATE FUNCTION greet(NAME VARCHAR) RETURNS VARCHAR AS
'BEGIN RETURN ''Hello, '' || NAME || ''!''; END;' LANGUAGE plpgsql;

-- 多参数函数
CREATE FUNCTION add(INT, INT) RETURNS INT AS 'return $1 + $2' LANGUAGE sql;

-- 聚合函数
CREATE AGGREGATE my_sum(INT) (SFUNC = INT4PLUS, STYPE = INT4, INITCOND = '0');

-- 存储过程
CREATE PROCEDURE transfer(INT, INT, NUMERIC) AS $$
BEGIN
    UPDATE accounts SET balance = balance - $3 WHERE id = $1;
    UPDATE accounts SET balance = balance + $3 WHERE id = $2;
END;
$$ LANGUAGE plpgsql;

-- 调用
SELECT add_one(5);
CALL transfer(1, 2, 100);

-- 删除
DROP FUNCTION add_one(INT);
DROP PROCEDURE transfer;
```

---

## 14. 触发器

```sql
-- 创建触发器
CREATE TRIGGER update_modified
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER check_age
    BEFORE INSERT ON users
    FOR EACH ROW
    WHEN (NEW.age < 0)
    EXECUTE FUNCTION reject_negative_age();

-- 删除触发器
DROP TRIGGER update_modified ON users;
```

---

## 15. 分区表

```sql
-- RANGE 分区
CREATE TABLE events (
    ID INT, EVENT_TIME TIMESTAMP
) PARTITION BY RANGE (EVENT_TIME);

-- ATTACH 已有表作为分区
CREATE TABLE events_2024 (LIKE events);
ALTER TABLE events ATTACH PARTITION events_2024
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- LIST 分区
CREATE TABLE logs (
    ID INT, REGION TEXT
) PARTITION BY LIST (REGION);

-- HASH 分区
CREATE TABLE h (
    ID INT, K TEXT
) PARTITION BY HASH (K);
```

---

## 16. 全文搜索

```sql
-- TSVECTOR 列
CREATE TABLE docs (ID INT PRIMARY KEY, CONTENT TEXT, TSV TSVECTOR);

-- 生成 TSVECTOR
SELECT TO_TSVECTOR('english', 'The quick brown fox');

-- TSQUERY
SELECT * FROM docs WHERE TSV @@ TO_TSQUERY('english', 'fox & quick');

-- RANK
SELECT TS_RANK(TSV, TO_TSQUERY('english', 'fox')) FROM docs;
```

---

## 17. JSON/JSONB

```sql
CREATE TABLE configs (ID INT PRIMARY KEY, DATA JSONB);

INSERT INTO configs VALUES (1, '{"name":"Alice","age":25,"tags":["admin","user"]}');

-- 提取
SELECT DATA->>'name' FROM configs;
SELECT JSONB_EXTRACT_TEXT(DATA, '$.name') FROM configs;
SELECT JSONB_PRETTY(DATA) FROM configs;

-- 包含
SELECT * FROM configs WHERE DATA @> '{"age":25}';
SELECT JSONB_CONTAINS(DATA, '{"admin"}') FROM configs;
```

---

## 17b. 运行时性能参数（环境变量）

```bash
# 堆表缓冲池帧数（每帧 8 KiB，默认 256 = 2 MiB/关系）
export DBMS_BUFFER_FRAMES=1024

# 索引缓冲池帧数（默认 128）
export DBMS_INDEX_BUFFER_FRAMES=256
```

两个变量均接受 16..4096 范围内的整数；未设置、空值、无法完整解析或越界时，
分别回退到 256 / 128 帧，不会夹取到上下限。每个值在进程首次创建相应缓冲池时读取并缓存；
修改后需重启进程，`SET` 或 `pg_reload_conf()` 不会改变已缓存的帧数。
堆池按关系、索引池按索引实例独立创建，帧内存约为各池帧数 × 8 KiB 的总和，另有管理开销。
`dbms.conf` 的 `buffer_pool_frames` 当前没有接入这些池的容量，详见第 25 节。
堆页完整性校验（Fletcher-16 + 结构校验）在从磁盘加载时执行，缓冲命中不重复校验；
新格式 B+Tree 页使用 CRC32C，格式与旧页限制见[部署说明](PACKAGING.md)。

---

## 18. 网络服务

```bash
# 生产服务端：证书和私钥必须预先准备好
export DBMS_TLS_CERT=/etc/dbms/tls/server.crt
export DBMS_TLS_KEY=/etc/dbms/tls/server.key
./dbms_main -D /srv/dbms/main --server 9999

# 仅限本地开发的明文模式（生产环境禁止）
./dbms_main -D /srv/dbms/main --server 9999 --insecure

# 客户端连接（libpq/psql；当前支持 SCRAM-SHA-256 与基础协议流程）
psql "host=localhost port=9999 dbname=info user=admin sslmode=require"

# 连接监控
SHOW CONNECTIONS;
SHOW PROCESSLIST;
SHOW STATUS;
SHOW LOCKS;
SHOW DEADLOCKS;
```

服务端默认 fail-closed：OpenSSL 不可用、证书/私钥缺失或 TLS 初始化失败时不会启动明文监听。证书路径也可以保留默认值 `server.crt` / `server.key`。

服务进程收到 `SIGINT` 或 `SIGTERM` 后会停止接收新连接，关闭活动连接并等待客户端 worker 退出；监听端口失败会以非零退出码结束。`--insecure` 仅用于本地开发。

StartupMessage 支持 wire 3.0，并对 3.x 的更高 minor（含 PostgreSQL 18
使用的 3.2）返回标准 NegotiateProtocolVersion 和未知 `_pq_.` option 列表。
未给 `database` 时按 PostgreSQL 规则使用用户名。`application_name`、UTF-8
`client_encoding`、常规连接模式、`options` 中的 `-c/--name=value` 以及当前
实现的会话超时参数会成为真实会话默认值；未知参数、畸形 options、复制连接和
非 UTF-8 编码会在 startup 阶段明确失败，不会静默忽略。复制协议和非 UTF-8
转码分别仍由 PROTO-10、PROTO-11 清单项覆盖。

认证成功后服务端发送 PostgreSQL 18 的 15 项 ParameterStatus、BackendKeyData，
最后发送 idle ReadyForQuery。应用名、编码、搜索路径、时区、session authorization
或有效角色的 superuser 状态变化时，会在一个 Simple Query 或 Extended Sync
周期结束前至多发送一次最终 ParameterStatus；事务状态由 ReadyForQuery 的
`I/T/E` 精确表示。BackendKeyData 的 secret 每次连接随机生成；标准 16-byte
CancelRequest 只有在 PID/secret 同时匹配且目标正在执行查询时才设置共享中断
令牌，辅助连接无论匹配与否都不返回消息。当前中断点覆盖表/行/页锁等待、heap
逐页/逐行扫描、`generate_series` 展开以及 parallel scan/group/hash-join/
GatherMerge worker 的安全汇合，取消返回 `57014` 并保持原连接可复用。不可中断
的单次内核文件 I/O、单次类型比较内部以及尚未接入统一检查的遗留长循环
仍由 PROTO-07 覆盖。执行器的 12 处排序已增加开始、结束和周期性的比较边界检查，
具体范围见 [R185](issue-review-r185-sort-interrupt-propagation.md)；该有限修复不代表完整 PostgreSQL cancel 语义。

Simple Query 的一个 `Q` 消息可以包含多条顶层分号分隔语句；分句器识别
普通/escape 字符串、quoted identifier、dollar quote、行注释和嵌套块注释。
没有显式事务控制的多语句消息属于同一个 implicit transaction，遇到首个错误
即停止后续语句并回滚；显式事务则保留 `T/E` ReadyForQuery 状态。每条已执行
语句各自产生准确的 RowDescription/DataRow/CommandComplete，整个消息只发送
一次 ReadyForQuery。空串、纯注释或只有分号的消息返回一次
EmptyQueryResponse，不伪造 command tag。

执行首句前的原始语法预检目前仅证明词法错误、不可能命令头和严格DROP DOMAIN目标语法；
其他已知命令仍交原接收者，不是完整SQL raw grammar。CREATE-then-use及到达执行阶段的语义错误
不提前猜拒绝，不能仅因42601就将错误前移。范围见[R169](issue-review-r169-simple-query-raw-syntax.md)。

Extended Query 的 named statement/portal 禁止被同名 Parse/Bind 覆盖，空名称仍按
协议替换。Parse 只接受一条语句；发生错误后丢弃消息直到 Sync。关闭不存在的
statement/portal 仍返回 CloseComplete。一个 Sync group 内的 Execute 共用隐式
事务，Sync 统一提交或回滚并释放事务级 portal；需要分页的 portal 必须在 Sync
前继续 Execute。空 prepared query 的 Execute 返回 EmptyQueryResponse。
当前 Describe 的结果列分析和大型 portal 的真正流式执行仍未完成，因此客户端
不应把这一子集当作完整的 PostgreSQL Extended Query 实现。

[R163](issue-review-r163-qualified-parse-validation.md)在发布statement前纯绑定受支持的已知签名标量形状；
不是任意SQL的完整Parse分析，也不为取描述执行callee。
[R166](issue-review-r166-scalar-protocol-parameter-inference.md)只对source-free、无ORDER/LIMIT/OFFSET/FETCH等容器的
整式标量调用中直接$n从真实输入声明推断OID，保留显式OID及冲突/编号缺口错误。
带表、排序、分页和跨表达式统一推断仍开放；额外八条范围外收集不是兼容通过。

---

## 19. 预编译语句

```sql
PREPARE stmt AS SELECT * FROM users WHERE ID = $1;
EXECUTE stmt USING (1);
DEALLOCATE PREPARE stmt;
```

---

## 20. 导入导出

`LOAD DATA INFILE` 仅是 `extended` 模式中的严格 `COPY table FROM file`
语法糖，复用相同权限、CSV 解码、SQLSTATE 和整语句回滚；它不会猜测表头或
静默跳过坏行。默认 `postgresql18` 模式返回 `42601`，应直接使用 `COPY`。
`SELECT ... INTO OUTFILE` 在两种模式中都返回 `42601`；它不会劫持 PostgreSQL
的 `SELECT ... INTO table` 建表语法。服务端 CSV 导出统一使用 `COPY ... TO`。

```sql
-- CSV 导入
LOAD DATA INFILE 'data.csv' INTO TABLE users;

-- COPY
COPY users FROM 'data.csv';
COPY users TO 'output.csv';

-- 以下维护命令仅在 extended 模式可用，立即返回 job_id/status
DUMP DATABASE mydb TO 'mydb.sql' RATE 10240;
BACKUP DATABASE mydb TO 'mydb.bak' RATE 10240;
RESTORE DATABASE mydb FROM 'mydb.bak' RATE 10240;
SHOW MAINTENANCE JOBS;
SHOW MAINTENANCE JOB 1;
CANCEL MAINTENANCE JOB 1;
```

`DUMP` 产生逻辑 SQL 文件；`RESTORE DATABASE` 只接受 `BACKUP DATABASE`
产生的物理备份，不能读取 DUMP 文件。后台作业状态持久化为
`queued/running/succeeded/failed/cancelled`，进程异常退出时遗留的 running
作业会在下次启动重新排队。维护 mutation 需要管理员权限且不能在事务块内执行；
可选 `RATE` 的单位为 KiB/s。逻辑 dump 和物理备份只在完整成功后原子发布目标，
取消不会发布半成品。

物理恢复持有数据库排他锁。目录真正替换时，`SHOW MAINTENANCE JOB`所需的
管理员目录读取可能暂时返回`55P03`（cold catalog read conflict）；这不是
任务完成或失败。客户端应在原有总期限内稍后重试，并等待实际终态。不要因
该冲突重复提交RESTORE，也不要忽略其他错误。会话参数的协议状态观察不应
因此终止服务；普通数据访问仍遵守数据库锁与事务规则。

---

## 20.1 WAL 归档与时间点恢复 (PITR)

### 配置归档

`dbms.conf` 中设置 `archive_command`（单引号值,可含空格和 `=`）:

```
# 内建安全模式: 段被原子复制到目录 (无 shell)
archive_command='dir:/var/dbms/archive'

# 外部命令形式已保留 (dir: 前缀以外的值暂不执行)
```

归档触发在 checkpoint 边界: 完整写满的段先标记 `archive_status/<seg>.ready`,
复制成功后翻转为 `.done`; 失败保留 `.ready`,后台维护循环后续重试。

### 强制切换段 (pg_switch_wal)

小负载不会自然填满 16 MiB 段。要在关键时刻确保 WAL 已归档:

```sql
PG_SWITCH_WAL;   -- 当前段零填充关闭, 后续记录写入新段
CHECKPOINT;      -- 关闭的段此刻被归档
```

### 时间点恢复

```sql
-- 1. 基础备份
BACKUP DATABASE mydb TO '/backup/mydb_base';
SHOW MAINTENANCE JOB 1; -- 等待 succeeded

-- 2. (持续运行中) 事务不断写入并被归档

-- 3. 灾难发生: 恢复到过去某一时刻
RESTORE DATABASE mydb FROM '/backup/mydb_base'
    PITR '2026-08-23 12:34:56' ARCHIVE '/var/dbms/archive';

-- 4. 等待 restore job succeeded，然后重启进程：恢复重放归档 WAL
```

语义:

- 恢复目标时刻**含**该时刻: 恰在目标时刻提交的事务保留。
- 目标之后提交的事务按未提交处理,其 before-image 被恢复(等于这些写入从未发生)。
- 恢复目标是单次的: 重放成功后切换到一个未使用的新 WAL 时间线,再消费 `recovery_target`; 后续重启只恢复新时间线,不会重新提交被目标排除的旧事务。
- 提交时间戳以 v2 格式写在 WAL 提交记录里; 旧格式记录按"无限制"重放。
- 归档回填当前仍只读取时间线 1; 成功恢复后的时间线分叉已经持久化,完整的多时间线归档选择仍是差距(见 feature-gaps.md)。

---

## 21. pg_hba 访问控制

系统支持类 PostgreSQL 的 `pg_hba.conf` 访问控制：

```
# TYPE  DATABASE  USER      ADDRESS        METHOD
local   all       all                      trust
host    all       all       127.0.0.1/32    md5
host    all       all       192.168.0.0/16  scram-sha-256
hostssl all       admin     10.0.0.0/8      cert
host    all       all       0.0.0.0/0       reject
```

解析器识别 `trust`, `md5`, `scram-sha-256`, `password`, `ident`, `peer`, `cert`, `pam`, `ldap`, `radius`, `reject`。
网络运行时当前实际执行 `trust`、`password`、`scram-sha-256`、使用 SCRAM verifier 的 `md5`、以及 `reject`；其他方法 fail-closed 返回未实现错误。支持首条匹配、`hostssl`/`hostnossl`、IPv4/IPv6 CIDR、`sameuser`、`samerole`/`samegroup` 和 `+role`。

---

## 22. 复制与高可用

当前提供复制槽/发布等管理对象和项目格式的逻辑变更预览，不能据此建立 PostgreSQL 物理或逻辑复制拓扑。
实际插件名为 `dbms_test_decoding`、`dbms_pgoutput_preview`；保留名 `test_decoding`、`pgoutput` 被拒绝，预览格式与 PostgreSQL 插件不兼容。

`CREATE SUBSCRIPTION` 尚无真实拉取/应用运行时，不能作为可运行的订阅示例；设置 `synchronous_commit` 也不证明已有远端同步确认。
项目管理语法受 `extended` 模式约束，具体边界见[兼容性契约](compatibility-contract.md)和[复制模块说明](../src/replication/README.md)。
预览行像的 NULL/字段边界和错误传播还有待修复问题，见[审查报告](code-review-2026-10-13.md)。

---

## 23. 大对象

```sql
-- 大对象通过 LargeObjectManager API 管理
-- 创建/读取/写入/截断/删除/导入/导出
-- 存储在 {db}/.lobjects/ 目录中
```

---

## 24. 多进程管理

`ProcessManager` 当前提供后端信息的登记、列举和关闭标记接口。`spawnBackend()`
只添加内存记录，`pid` 为内部编号，不会创建操作系统进程或线程。其类别枚举包括：

```cpp
// BackendType: ClientBackend, WalWriter, BgWriter, Checkpointer,
//             AutoVacuumLauncher/Worker, ReplicationSender, Archiver,
//             StatsCollector, LogicalLauncher/Worker
```

枚举存在不代表已有对应的独立进程运行时。网络服务当前在一个服务端进程中为每个
客户端创建工作线程；存储引擎另行启动后台维护线程。这些线程不是由 `ProcessManager`
派生的 PostgreSQL 式多进程后端池。部署形态见[打包与部署说明](PACKAGING.md#服务端部署形态)。

---

## 25. GUC 参数配置

```sql
-- 当前连接参数
SET timezone = 'Asia/Shanghai';
SET statement_timeout = 30000;
SET lock_timeout = 1000;
SET deadlock_timeout = 1000;

-- 重置
RESET search_path;
RESET ALL;

-- 查询当前实现支持的 pg_settings 列（完整视图仍未实现）
SELECT name, setting, unit FROM pg_settings;

-- 配置文件位于显式 data directory 的 dbms.conf；修改后由管理员请求重新加载
SELECT pg_reload_conf();

-- 事务隔离级别
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- ALTER SYSTEM 只持久化，不立即改当前进程或会话
ALTER SYSTEM SET work_mem = 65536;
SELECT pg_reload_conf();                 -- 应用可 reload 参数
ALTER SYSTEM SET max_connections = 128; -- 启动参数，重启后生效
-- 当前版本只支持 ALTER SYSTEM SET；如需恢复默认值请删除/修改 dbms.conf

-- extended 模式中的 SET GLOBAL 是 ALTER SYSTEM SET 的持久化别名；
-- 同样需要 reload 或重启，且不能在事务中执行
SET compatibility_mode = extended;
SET GLOBAL AUTO_VACUUM = ON;
SET GLOBAL AUTO_VACUUM_THRESHOLD = 1000;
SHOW VARIABLES;                          -- 仅 extended 模式提供

-- 兼容模式（DIV 框架；会话开始处受限，事务中不可更改）
SHOW compatibility_mode;                     -- 默认 postgresql18
SET compatibility_mode = extended;           -- 显式启用项目扩展层
SET compatibility_mode = postgresql18;       -- 回到 PostgreSQL 18 语义
SHOW dbms.extensions;                        -- 审计当前会话启用的偏移
```

### 配置值与实际运行资源的边界

参数能写入配置或出现在 `pg_settings` 中，并不表示已实现 PostgreSQL 的同名调度或资源语义。
当前两项需特别区分：

| 项目 | 当前实现 |
| --- | --- |
| `buffer_pool_frames` | 配置默认 16，可保存、加载，并显示为 `pg_settings.shared_buffers`（单位 `8kB`）；reload 保留启动值。当前没有传给堆/索引池，不代表实际池容量。实际容量由第 17b 节的两个环境变量决定。 |
| `checkpoint_interval` | 默认 30，合法范围 1..1000000；控制交互式 CLI 主循环的语句计数阈值，达到阈值后尝试 checkpoint。网络查询不走该计数器。存储引擎后台 checkpoint 的默认检查间隔另为 300000 毫秒（5 分钟），此配置未接入该间隔。 |

`pg_settings.checkpoint_timeout` 当前将 `checkpoint_interval` 显示为单位 `s`；这是尚未修正的映射/单位问题，
不能据此推断后台 checkpoint 每 30 秒执行。后台检查也可能因事务或维护状态跳过，5 分钟不是完成期限。
修改配置后，可 reload 的字段需管理员执行 `SELECT pg_reload_conf()`；启动字段须重启。
`ALTER SYSTEM SET` 和 extended 模式的 `SET GLOBAL` 都只负责持久化。

### 25.1 兼容模式与能力门控（0A000）

默认 `postgresql18` 模式下，以下命令**没有真实运行时**，一律返回
`feature_not_supported`（SQLSTATE `0A000`），不再写入兼容对象记录后假报成功：

- `CREATE/ALTER/DROP EXTENSION、OPERATOR [CLASS/FAMILY]、RULE、LANGUAGE、
  AGGREGATE、TRANSFORM、ASSERTION、ACCESS METHOD、EVENT TRIGGER、
  SERVER、FOREIGN DATA WRAPPER、FOREIGN TABLE、USER MAPPING、
  SUBSCRIPTION、TEXT SEARCH CONFIGURATION/DICTIONARY/PARSER/TEMPLATE`
- `IMPORT FOREIGN SCHEMA`（无 FDW 运行时）
- `LOAD 'library'`（无动态加载运行时）

其中 `ASSERTION` 在两种模式下都返回 `0A000`（PostgreSQL 18 本身也未实现
SQL assertion）。显式 `SET compatibility_mode = extended` 只启用已有真实运行时的
项目 SQL 扩展；上述没有运行时的对象命令仍返回 `0A000`。旧的兼容对象记录层及其
SHOW 入口已经删除。该模式是会话级设置，进入事务后不可切换。

普通SET按完整参数名分派，不从值内容识别模式。例如
`SET application_name TO 'compatibility_mode=extended'`只设置应用名称；
`SET search_path TO compatibility_mode`只设置schema查找路径。
`compatibility_mode_extra`及`compatibility_mode.extra`都不是模式参数。
TO值中的等号仍是数据。这里不承诺完整GUC语法/参数域或事务级LOCAL，
模式值示例沿用已支持的不带引号写法；有限修复见[R141](issue-review-r141-set-parameter-identity.md)。

同一批门控的项目语法（`postgresql18` 模式下的行为）：

- `USE [DATABASE]`（DIV-01）→ 默认模式返回 `42601` 并提示重连；extended
  模式仅在无事务、临时表、LISTEN、cursor/portal 时允许切换。目标不存在返回
  `3D000` 且保持原数据库；成功后清理 prepared statement、`currval` 和项目会话变量，
  并为新数据库重建锁命名空间
- `REPLACE INTO`（DIV-02）→ `42601`，提示 `INSERT ... ON CONFLICT`
- `LOAD DATA INFILE`（DIV-03）→ `42601`，提示 `COPY ... FROM`
- `SELECT ... INTO OUTFILE`（DIV-04）→ 两种模式均返回 `42601`，提示
  `COPY ... TO`；PostgreSQL 的 `SELECT ... INTO table` 建表语义保持可用
- `DESC/DESCRIBE`、`VIEW TABLE/DATABASE`、`SHOW USERS/ROLES/POOLS`（DIV-05）→
  默认模式分别返回 `42601`/`42704`，提示查询
  `information_schema`/`pg_catalog`/`pg_roles`；extended 模式将它们作为只读
  catalog 视图返回 typed rows 和 SQL NULL，`DESC`/`VIEW` 按表 ACL 过滤，
  `SHOW USERS/ROLES` 不暴露口令字段
- `CREATE/DROP REPLICATION SLOT` SQL 形式与 `SHOW LOGICAL/REPLICATION SLOTS`
  （DIV-09）→ 默认模式返回 `42601`，提示使用 replication protocol 或
  `pg_*_replication_slot()` 函数；extended 模式仅把真实 logical slot API 暴露为
  typed wrapper，创建、确认、删除不能在事务块内执行。没有物理复制运行时，
  `CREATE REPLICATION SLOT ... PHYSICAL` 在 extended 模式仍返回 `0A000`
- `SET GLOBAL`（DIV-11）→ `42601`，提示 `ALTER SYSTEM`
- MySQL/SQL Server 类型别名 `TINYINT`、`LONG`、`DATETIME`、`BLOB`、
  `BINARY/VARBINARY`、`NCHAR/NVARCHAR`（DIV-06）→ `42704` 类型错误，
  错误信息给出等价的 PostgreSQL 类型（`smallint`/`bigint`/`timestamp`/
  `bytea`/`char`/`varchar`）；列属性 `AUTO_INCREMENT`、`UNSIGNED` →
  `42601`，分别提示使用 identity 和 `CHECK`。只有显式 `extended` 模式
  保留这些别名与属性，CREATE/ALTER TABLE 使用相同规则
- `CREATE/DROP FULLTEXT INDEX`、`CREATE HASH INDEX`（DIV-07）→ `42601`，
  提示 `CREATE INDEX ... USING gin (to_tsvector(col))`、
  `CREATE INDEX ... USING hash` 与 `DROP INDEX`。显式 `extended` 模式下，
  快捷语法走统一的 typed index、目录命名和物理生命周期；创建后也可由
  标准 `DROP INDEX name` 删除
- `DUMP`、`BACKUP/RESTORE DATABASE`、`CLEAR PLAN CACHE` 以及
  `SHOW/CANCEL MAINTENANCE JOB`（DIV-10）→ 默认模式返回 `42601` 并提示
  PostgreSQL 的 `pg_dump`、base backup/restore 工具或 project extension；
  extended 模式只提交持久化后台作业并返回 typed job id/status，支持管理员权限、
  非事务边界、`RATE` 限速、取消、状态查询和 crash resume
- 项目连接池（DIV-12）：默认模式的 `SHOW POOLS` 返回 `42704`，
  `pool_mode`/`pool_size`/`max_client_conn` 在默认 `SHOW ALL`/`pg_settings` 中隐藏，
  单独 `SHOW` 返回 `42704`。显式 extended 模式提供只读 `SHOW POOLS`、
  池参数 `SHOW` 和 `pg_settings` 展示；池运行配置由服务器配置入口管理。
  池等待、生命周期和停机的已知问题见[审查报告](code-review-2026-10-13.md)。
- TDE（DIV-12）：`SHOW TDE STATUS` 返回 `0A000`；该专项仍按[用户暂缓范围](deferred-scope.md)处理，本轮未重新验收。
- `CREATE ASSERTION`（DIV-08）→ 两种模式都返回 `0A000`（PostgreSQL 18
  同样未实现 SQL assertion）
- 启动标识（DIV-13）：服务端 banner 标明自身版本与兼容模式，并明确
  提示“这不是 PostgreSQL server cluster”；`server_version` 参数上报
  `18.0 DBMS-C++ 0.2.0`：前导主版本供 PostgreSQL 客户端解析，后缀标识
  实际产品版本；该字段不是 PostgreSQL 18 兼容认证。所有正常启动必须用
  `-D/--data-dir` 或 `DBMS_DATA_DIR` 显式选取数据根；首次启动写入 V3
  `DBMS_CONTROL`，其中包含 control/catalog/heap format version、8 KiB block size、
  16 MiB WAL segment size、
  native byte order、feature flags、随机 system identifier 和 CRC32C。控制文件
  限定为小型普通文件并拒绝符号链接。正常启动不会自动升级旧格式：
  `--check-data-directory` 只读检查，`--upgrade-data-directory` 当前仅显式支持
  V1/V2→V3 control 升级并保留 system identifier；该操作不迁移 catalog、heap
  或索引数据。损坏或未知 control、不匹配的
  format/block size/byte order、非空无标识目录，以及含 `PG_VERSION` 的
  PostgreSQL cluster 会在全局存储对象构造前被拒绝。服务端
  无默认端口，必须显式 `--server PORT`

会话默认模式可由环境变量 `DBMS_COMPATIBILITY_MODE=extended|postgresql18`
设定（默认 `postgresql18`），CLI 与网络会话一致生效。

### 25.2 差分兼容测试（P0-16）

`tests/compat/pg_diff_runner.py` 将 `tests/compat/cases/*.sql` 中的同一组
语句分别发往 PostgreSQL 18.6 参考实例（默认读取 Docker 容器 `pgref` 的
连接信息，也可设置 `PGREF_HOST` / `PGREF_PORT` / `PGREF_PASSWORD`）和本
DBMS。两端都使用 wire protocol，比较无损行数据、NULL、列名/type OID、
SQLSTATE 和 command tag。运行器在启动本地 DBMS 前读取参考端
`server_version_num`，仅接受 `180006`；旧版本结果不能充当 18.6 验收。
当前运行器无 allowlist，任何差异都会直接报错。

参考库应使用 `en_US.utf8` 数据库排序规则，并将 `lc_monetary` 设为
`en_US.utf8`；当前项目的默认文本排序与货币符号测试按这组设置比较。
若参考库使用 `C.UTF-8`，大小写排序和 `to_char(..., 'L9999')` 会因区域
设置产生差异，不能当作内核回归。2026-09-23 使用官方 18.6 源码构建的
临时参考实例，设置 `PGREF_HOST`、`PGREF_PORT`、`PGREF_USER`、
`PGREF_DATABASE`、`PGREF_PASSWORD` 后，129 组差分为 `failed=0`。
`version()` 的产品/构建身份由本项目 E2E 单独验证，不做逐字跨产品比较。
这组用例不覆盖 273 项总清单的完整语义。

当前覆盖（129 个用例文件）：算术、字符串函数、布尔/NULL 三值逻辑
（含 `NOT NULL`→NULL 三值逻辑、`NULL::text IS NULL` 的 `::` 后缀 cast
与 postfix `IS NULL` 解析修复、`NULL = NULL IS NULL` 的比较级 IS 绑定、
bool_and/bool_or/every 表达式参数聚合以 t/f 渲染、存储 NULL 语义（物理 NULL 经由
heap null bitmap + NullRowBinding/TableScanOp 绑定：`SELECT *`、`count(v)` NULL 跳过、
`min/avg` 空值语义、`v + 1`/`v::text` NULL 传播、ORDER BY NULLS LAST/FIRST（ASC/DESC）、
DISTINCT NULL 语义、GROUP BY 空 NULL 组键（groupAggregate 逐行 NullRowBinding）全部与
PG 一致；语句起始统一 unbindNullRow 防止排序重绑定跨语句泄漏）、pg_views 目录视图
（schemaname/viewname/viewowner/definition，会话库视图、限定与非限定名一致，definition 内部空白以下划线渲染——表格 wire 格式的已知限制）、concat/concat_ws 跳过 NULL 参数（concat(a,NULL,b)→ab、列路径 concat_ws 注册）、
current_database()/current_schema()/pg_typeof() 会话伪函数、generate_series 选择列 SRF 展开
（升降序/步长/多列重复）、WHERE 中括号表达式
`(v > 5) IS [NOT] NULL` 的常量折叠语义、输出列命名 `?column?`/cast 列名/类型名
（`(v+1)::text`→text、`v::text`→v、`CAST(2 AS int)`→int4 的 figure_colname 规则，
且差分运行器现已逐语句比较 RowDescription 输出列名：函数名列（to_char/round/
trim→btrim/ltrim）、CASE→case、AT TIME ZONE→timezone、OVERLAPS→overlaps、负数字面量
与字符串字面量→?column?、JOIN `SELECT *` 的无限定列名等全部对齐））、
整数/numeric 除法（含 PG `select_div_scale` 的 16/20 位小数规则）、
CASE 表达式、聚合（sum/count/avg/min/max、GROUP BY、FILTER 子句、
聚合间算术 `count(*) - count(v)`/`sum(v)/count(*)`/`avg(v)*2` 单行求值）、
显式/隐式类型转换（CAST(x AS t) 前缀语法、`::` 后缀语法、舍入与
布尔规则）、有状态 DDL/DML 往返、JOIN（inner/left/right/cross 及
join 上的纯聚合）、子查询（WHERE 标量子查询、IN/NOT IN、HAVING、
无 FROM 投影中的标量子查询、相关 EXISTS/NOT EXISTS —— 单等值
相关条件经半连接（SemiJoinOp）下推执行、相关标量聚合
子查询 v > (SELECT min(v) ... WHERE g = s.g) —— 按相关值分组
一次性物化内表聚合，再逐行比较）、错误面（除零 22012、非法整型输入
22P02、缺失表/函数）、窗口函数（row_number/rank/dense_rank/lag/lead/first_value/
last_value/sum/avg/min/max/count、PARTITION BY 与 ORDER BY 组合、
命名窗口 WINDOW 子句、ntile/lag(v,n) 带偏移量、ROWS/RANGE/
GROUPS 帧含无 ORDER BY 的 PARTITION 帧、EXCLUDE CURRENT ROW/
GROUP/TIES 排除子句、表头别名或裸函数名、avg 的 PG
select_div_scale 精确数值语义）、ROLLUP 分组集（含关键字后带
空格的 ROLLUP (g) 写法与总计行的 NULL 键）、CUBE 多列与 GROUPING SETS
（空键单元格在结果行中保持其位置，排序时空键居末）、NULL 排序（ASC 时
NULLS LAST、DESC 时 NULLS FIRST，PG 默认语义）与 NULL 单元格
的协议空值显示、投影列序（SELECT 列表顺序而非表定义顺序，
含别名投影，重复列（SELECT a, b, a）按列表重复输出）、聚合输出上的 ORDER BY（按别名、聚合表达式或
分组列排序，含 DESC 与 NULL 语义）、INTERSECT/EXCEPT/UNION 集合运算
（尾部 ORDER BY/LIMIT 作用于整个集合结果，含 UNION ALL）、CAST 列头
按目标类型命名（float8/text/bpchar 等）与一元正负号投影
（SELECT -v，头为 ?column?），数值转整数按 PG 四舍五入，裸小数
字面量按 NUMERIC 精确运算、除法精确性判定与 select_div_scale 的
20 位宽结果规则（1.5/1、2.2/2 与 PG 逐位一致）、GROUP BY 选择
列表中的相关标量子查询（按分组值相关求值，列头取子查询自身的
输出列名）、EXISTS/NOT EXISTS 的多列相关（复合键半连接/反连
接，含相关列与内表同名的歧义消解）、混合等值/范围相关谓词的
EXISTS/NOT EXISTS 逐行求值、GROUP BY 选择列表中
"sum(id) + (select ...)" 聚合与相关标量子查询的算术组合
（逐组求值聚合与子查询后按 Numeric 精确运算，列头 ?column?）、SELECT 列表中普通列与算术表达式混合
（修复公共列序重排误删表达式列：重排仅在纯普通列查询执行）、字符串连接 ||（列/字面量/内联 ::text 转换/函数调用操作数，顶层切分保留嵌套括号）、函数调用与算术组合（coalesce(a,b)+1 等，顶层运算符切分、NULL 传播、数值字面量与一元符号处理、全 NULL 行经协议保留）、日期函数（EXTRACT year/month/day/dow/doy 的 FROM 参数形式与 NULL、日期 ± 整数天返回日期、日期 − 日期返回天数、DATE '...' 类型字面量）、区间运算与 age（date ± interval 'N day/week/month/year' 返回时间戳、age(t, s) 按 PG 格式 "N years M mons K days" 借位渲染、类型字面量参数）、LIKE/ILIKE 谓词（LIKE 区分大小写、ILIKE 折叠大小写、NOT 变体、下划线通配符）、LIKE ESCAPE 子句（normalize 阶段将 esc+X 编码为 0x01+字面量，匹配器解编码）、负整数（parseInt 支持符号、定长整数解码符号扩展、% 与 int/int 截断除法向零取整）、numeric 标度渲染（+− 取最大标度、* 标度相加、/ 精确十进制长除法 16 位小数、整数无小数点）、round/trunc（round 负数位、trunc(x)/trunc(x,n) 向零截断含负 n 位）、div(x,y) 整数商向零截断（mod 已对齐）、gcd/lcm（绝对值运算、lcm=|ab|/gcd）、width_bucket（trunc((op-lb)*cnt/(ub-lb))+1，越界 0/cnt+1）、类型化 numeric 转换（v::numeric(4,2) 重写为 cast、十进制半进位、按标度渲染）、left/right/repeat（负 n 分别去尾/去头、n=0 空串）、btrim/ltrim/rtrim（trim 字符集合、单参 btrim 去空白；lpad/rpad 已对齐）、strpos（1 基找不到 0）与 overlay（placing/from/for 语法重写为位置参数）、translate（from→to 逐字符映射，to 较短删除、to 较长忽略多余；字面量路径原本已通，列路径补路由+处理器）、to_char 数值模式（未用 9 位渲染空格、全零整数无 0 模式渲染空、0 位补零、G 千分位逗号、D 小数点、MI/PL/SG/PR/L 符号与货币模式）、to_number（符号/数字/单小数点提取，分组与模板字符跳过；求值器与引擎双路径）、to_date（YYYY/MM/DD 模式解析双路径，字面量分隔符逐字符对齐）、to_timestamp（YYYY/MM/DD+HH24/MI/SS 模式双路径，渲染 +00 时区）、日期 to_char（Mon/Day/Dy 模式；typed literal `date '...'` 在表达式解析前解包）、组合算术中的 typed cast（`v::numeric(4,2) + 1`；`::` 修饰符在求值器中规范化、嵌套 cast AS 形式在算术操作数中拆分，+/- 结果 scale 取操作数文本 scale 的最大值）、关键字 CAST（解析器 CAST(expr AS type) 类型名循环停在 `(`，修饰符正确落入 typeMods → numeric(p,s)/varchar(n) 精确求值）、to_char MI 符号位（前缀/后缀位置感知：负数减号、正数空格、FM 抑制空格；引擎列路径新增数字模板渲染；另修复 find_first_of 多字符常量 '90' 的老 bug → 改为字符串 "90"）、to_char S 符号位（`S` 与数字相邻、在填充区内：`S9999` -42 → `  -42`；`9999S` 后缀；FM 保留符号只去空格；`SG` 前缀仍走默认分支渲染 `- 482`）、to_char TH/V/RN 模板（TH 序数后缀随模板大小写：`9999TH`→`   42ND`、`9999th`→`    1st`；V 小数点移位 `9V99` 4.2→` 420`，溢出渲染 `###`；RN 罗马数字右对齐宽 15：`RN` 42→`           XLII`；求值器与引擎列路径双实现，列路径数字模板识别扩展到 RN）、to_char EEEE 科学计数法（有效位数 = 模板 9 数；PG 风格 `4.20e-03`，负号占符号槽、正数空槽、指数至少 2 位；双路径实现）、to_char 时间戳模板扩展（HH12/MI AM、HH24:MI:SS、Dy 组合已覆盖，新增 J 儒略日：`2026-08-15` → `2461268`，公式 jd = d + (153m+2)/5 + 365y + y/4 − y/100 + y/400 + 1721119，m 为从三月起的月序）、justify_hours/justify_days/justify_interval（双路径：求值器 lambda + 引擎处理器；`25 hours`→`1 day 01:00:00`、`70 days`→`2 mons 10 days`、混合符号 `-2 mons -3 days 04:05:06`→`-2 mons -2 days -19:54:54`；顺带修复 parseIntervalText 不支持负号分量 token、负数分量用复数 `days` 的 PG 规则）、trunc 数值渲染（单参整数值输出 `3` 而非 `3.000000`；双参裁剪尾零 `trunc(3.14159,2)`→`3.14`）、make_interval（命名参数 years/months/weeks/days/hours/mins/secs — 求值器读取 FunctionCallExpr::namedArgs，此前解析器产物从未被求值；`make_interval(days=>3, hours=>2)`→`3 days 02:00:00`）、overlay 关键字语法（`overlay(s placing r from st for n)`：列路径在 main.cpp 投影层重写为位置参数；无 FROM 标量路径在求值器 lambda 内过滤空关键字参数 — 该路径中 placing/from 产生空值参数、for 被丢弃，过滤后 [s,r,st,n] 正确计算；字面量与列双路径 PG 精确一致）、trim 关键字形式（`trim(both|leading|trailing [chars] from s)`：解析器在 parsePrimaryExpr 内把 both/leading/trailing 重写为带引号的方向字面量、from 重写为逗号（通过 const_cast 别名修改调用方 token 缓冲）；overlay 的 placing/from/for 与 extract 的 from 同机制归一为位置参数；求值器 trim lambda 识别方向字执行 both/leading/trailing 三种裁剪；顺带修复此前 overlay 解析器补丁因 const tokens 无法编译被静默丢弃的问题）、杂项算子（`position(x in y)`、`x SIMILAR TO y` 已有覆盖；新增正则匹配算子 `~`、`~*`、`!~`、`!~*`：tokenizer 支持 `!~*` 三字符折叠、parseComparisonExpr 接受这四个符号、求值器以 std::regex 判定并渲染 t/f；`timestamp AT TIME ZONE 'zone'`：naive 输入按该时区读取墙钟换算为 UTC 并追加 `+00` 后缀（shift 方向取负），parseTimeZoneOffset 增加常见命名时区固定偏移表如 Asia/Tokyo=+9、America/New_York=-5）、集合与数组算子（`substring(x from n [for m])` 关键字形式已有覆盖；新增 `(a,b) OVERLAPS (c,d)`：tokenizer 后处理将行值形式重写为 `overlaps(a,b,c,d)` 函数调用（修正后向括号扫描把行闭括号误计为嵌套的缺陷），求值器实现 PG 半开区间点/区间边界规则；数组 `@>`/`<@` 沿用现有包含判定，`&&` 重叠算子补全解析层（JSON 算子级接受）与求值（元素集合相交判定）；CLI 标量与列两路选择列表切分器及 splitTopLevelComma 均跟踪方括号深度，`array[1,2,3]` 不再被逗号拆裂——期间误改 VALUES 行解析器的括号分支已回滚）、数组函数与正则类（POSIX 字符类 `[[:alpha:]]`、`w` 转义在 `~` 算子下与 PG 一致；`array || array` 现按数组语义合并外层花括号（原为文本直连渲染 `{1,2}{3,4}`）；新增 `array_dims` 渲染 `[1:n]`，`array_position`/`array_length`/`array_upper`/`array_lower`/`string_to_array`/`array_to_string`/`cardinality` 一并纳入差分覆盖与 isScalarFunc 路由）、format 与引号折叠（发现并修复求值器 `unquote()` 不折叠 SQL 双写单引号的通用缺陷：`'it''s'` 此前求值为 `it''s`，现按 SQL 标准折叠为 `it's`，连带修正 `format('%L', ...)` 的双重转义；`regexp_replace` g 标志、`format` 的 %s/%I/%L/%% 均与 PG 逐字符一致）、文本函数与 regexp_matches（`initcap`/`ascii`/`chr`/`to_hex`/`octet_length`/`bit_length`/`md5`/`split_part` 越界空串/`substr` 负起点/`reverse`/`repeat`/`concat_ws` 已有覆盖；新增 `regexp_matches(text, pattern[, flags])`：std::regex 首个匹配按捕获组渲染 `{g1,g2}` 文本数组（无组则整匹配），flags 支持 i 忽略大小写并按被匹配文本原大小写输出，差分含 `{a,b}`、icase `{B}` 两类探针）、字符串边界与多字节（新增 utf8CharCount/utf8ByteAt 辅助：`left`/`right` 支持负偏移（丢弃对应端字符）且按字符而非字节计数，`position`/`strpos` 命中位置由字节偏移换算为字符序号，`héllo` 类多字节探针与 PG 一致；`regexp_split_to_array`、`starts_with` 已有覆盖）、数值舍入语义（PG numeric 为半上舍入、float8 为半偶舍入：修复 `divideByPowerOf10` 在全部数位被丢弃时直接归零不做进位的缺陷（`round(0.5::numeric)`→0）；`isNumericTypeName` 收窄为精确十进制类型、未定型小数字面量按 `numeric` 求值，float 走双精度路径 `std::nearbyint`（`round(2.5::float8)`→2）；ceil/floor 等一元数学函数整值结果渲染为裸整数而非 `-2.000000`）、power 的 numeric 呈现（PG `power(numeric,numeric)` 语义：两参均整且结果精确时渲染裸整数（`power(2,10)`→1024），否则按 16 位小数呈现并以 long double `powl` 复刻 PG 扩展精度 exp/ln 的第 16 位舍入（`power(2,0.5)`→1.4142135623730950、`power(2.5,2)`→6.2500000000000000，逐字符与 PG 一致）；`trunc` 保留位数、`mod`/`%` 负数取模语义已有覆盖）、exp/ln/log/sqrt 的 numeric 呈现（按 PG 各函数的显示标度逐一对齐：`exp` 整数入参 15 位小数、小数入参 16 位；`ln` 恒 16 位；`log`/`log10` 精确整值且整数入参时渲染裸整数（`log(100)`→2），否则 16 位（`log(2,64)`→6.0000000000000000）；`sqrt` 整数入参精确值渲染裸整数（`sqrt(4)`→2），其余 15 位（`sqrt(9.0)`→3.000000000000000）；数值一律以 long double 计算再按位取整，第 15/16 位与 PG 逐字符一致）、三角/双曲/浮点函数的 float8 最短表示（新增 `float8Text`：按 15→17 位有效数字尝试并回解析校验的最短往返渲染，接入 `sin/cos/tan/asin/acos/atan/atan2/cbrt/sinh/cosh/tanh/asinh/acosh/atanh/degrees/radians/cot/pi/pow`，与 PG 的 Ryu 风格 dtoa 逐字符一致，如 `sin(1)`→0.8414709848078965、`degrees(1)`→57.29577951308232、`pi()`→3.141592653589793）、数值函数的列路径（engine）一致性（`applyScalarFunc` 对 round/ceil/floor/trunc/power/exp/ln/log/sqrt/三角等 30+ 数学函数统一委托给表达式求值器，列引用以 `cast(值 as 列类型)` 形式携带类型信息（float8 列走银行家舍入、numeric 列走半上舍入），函数实参中的裸算术（`ceil(x - 2.8)`）在 `getVal` 中按左结合折叠求值；`round(x, 负数)` 补齐向十/百位舍入（修复 `withScale` 负标度导致的越界字节输出，如 `12z9`），`trunc(numeric, n)` 文本截断且补齐到 n 位小数（`trunc(42.4, 2)`→42.40、绝不四舍五入），`power` 的 17 位有效数字显示标度（`17-整数位数`：`power(2.5,2)`→6.2500000000000000、`power(42.4382,2)`→1801.0008192400000，整数指数用 `Numeric` 精确乘幂）、DIV 差异命令的线上错误语义（DIV-01～14：`USE DATABASE`/`DESC`/`SHOW USERS`/`CLEAR PLAN CACHE`/`CREATE REPLICATION SLOT`/`BACKUP DATABASE` 等返回 0A000 feature-not-supported 并附 PG 替代指引，`SET GLOBAL`/`LOAD DATA INFILE`/`INTO OUTFILE` 返回 42601；修复线上层 `substr(6)` 只剥 `ERROR:` 前缀导致 `SQL syntax error:` 消息被截成 `ntax error` 的缺陷——现在按 `ERROR:`/`SQL syntax error:`/`SQL error:` 前缀完整剥离，线上携带与 PG 一致的裸消息）））））。运行：`python3 tests/compat/pg_diff_runner.py
[--only NAME]`（需要 docker 参考库）。

差分驱动已修复的语义：`CASE` 生成真正的 `CaseExpr`；`NULL AND/OR x`
三值逻辑；`IS [NOT] DISTINCT FROM`；一元负号保留整数类型（`-7/2 = -3`）；
`SUBSTRING(s FROM n FOR m)`/`TRIM(... FROM s)` 关键字调用语法；float8
字面量算术输出最短可回环十进制表示；`avg()` 用 numeric 精确除法（不再
丢失小数位，`avg(10.5)` 常量参数可求值）；numeric/decimal 列在聚合器中
按数值而非词法聚合；`CAST(expr AS type)` 前缀表达式解析（此前仅支持
`::`）；无 FROM 投影的 `AS` 别名只在顶层括号深度切分（`cast(1 as text)`
不再被截断）；int 转换按 PG 舍入（`3.7→4`，`-3.7→-4`，`true→1`）；
date 输出 ISO 零填充格式（`2020-01-02`）；聚合/函数投影列头按 PG 规则
命名（函数名或 `?column?`，防止多词表头撑爆协议列数）。

---

## 测试验证

完整测试套件运行方式：

```bash
./scripts/build.sh              # 编译生产二进制
./scripts/run_all_tests_fast.sh # 同一注册回归的安静输出入口
./scripts/build_tests.sh        # 自包含地构建并运行完整输出的规范测试入口
```

`build_tests.sh` 是唯一负责生产二进制、生产对象编译、测试链接、桩对象选择和 E2E 调度的测试实现；即使项目根目录没有 `dbms_main`，它也会先通过共享构建逻辑生成当前二进制。`run_all_tests_fast.sh` 只是它的安静输出外壳，成功时输出计数，失败时保留完整诊断。上述 shell 入口共享 `scripts/build_common.sh` 的编译配置；`scripts/build_one_test.sh <test_name>` 可用于单测试增量编译，编译配置变化会自动使对象缓存失效。

“完整”指该入口注册的集合：原生测试由 `tests/*_test.cpp` 枚举，真实 frontend 测试另行调用，
Python 测试取自 `build_common.sh` 的列表。目录里存在测试文件不等于已注册；当前遗漏和实际运行结果见[审查报告](code-review-2026-10-13.md)。

每个 C++ 测试都在独立的临时目录中执行，E2E 服务端还显式传入独立
`--data-dir`；测试结束后自动删除，因此 `.txnid`、WAL、catalog、日志和
`__t_*` 数据库不会跨测试共享或落入启动 CWD。
