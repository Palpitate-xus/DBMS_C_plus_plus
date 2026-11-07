# 数据库操作性能测试报告（2026-10-30）

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-20261107"></a>
## 2026-11-07：标量TEXT容量修复、1500项完整成本与FULL恢复缺陷（P38-C2）

修复标量TEXT把持久化名义宽度65535误当逻辑值上限的问题：统一的字符字节限制函数对非数组TEXT使用1,073,741,819字节上限，并让旧式UPDATE校验也使用该函数。既有TOAST读回、插入和行构造因此一致处理完整长值；表结构dsize仍为65535，内联引用和存储格式不变。实际验证到1MiB，未进行接近1GiB的端到端容量验证。旧数据可由新程序读取；旧程序不能读取新写入的超64KiB TEXT，不能据此承诺降级后的值兼容。

新增large_text_toast_test在同一冻结源码上形成33检查：旧版21失败／actual1，当前0失败／actual0。全字节覆盖65535、65536、70003、UTF-8 90012字节／30006字符、可压缩131076字节、随机1MiB、NULL、空串、文本NULL及类似marker的普通串；同时验证第二长字段100003字节、旧式和optional更新的完整返回值、事务回滚／提交、普通VACUUM、热态VACUUM FULL、TRUNCATE后重插1MiB，以及有界VARCHAR的字符数和尾空格约束。

10项相关原生测试在旧版与当前均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test、vacuum_toast_physical_reference_test、alter_column_toast_generation_test。旧版前9项使用既有相同源码／对象／ELF的真实终态，第10项重新执行；当前10项均本批执行。既有toast_test两版仍在第604行 `!corruptScan.open()` 断言中止（actual -6），后续检查NOTRUN，未改预期，full suite=false。

新增72条SQL分别在真实PG18.6、旧版56aad0c0和当前程序执行：216条核心请求、507行、1935单元、80NULL、73,199,459请求／回复字节，7个Main正常0、2个主动SIGKILL终态-9。独立解码器核对完整值、NULL、全部T7、tag、Ready、错误及NOTICE，当前目标TEXT用例通过；严格PG比较仍有7项差异：4项系统目录列来源OID元数据，以及大声明／无界／多字节大VARCHAR的3项差异。初轮强杀前已删除1MiB行，其最大存活TEXT为200006字节；该组1MiB重写后的冷开来自正常关闭，不把它计作1MiB崩溃恢复证据。

实际旧版30412452创建、正常闭库并冷开验证的after-DELETE种子，334个名字／模式／字节在本批前后完全一致，固定／单长／多长字段人口241／241／113。另做不含FULL的18条普通维护控制，每角色18条，共36请求／3070行／14670单元／1110NULL／16,337,172字节，4个Main正常0；当前在旧模式中写入70003字节及1MiB TEXT后，普通VACUUM和新Main冷开全值／T7通过，未改动固定／单长字段完整wire。再以相同原始种子做独立18条崩溃控制，共36请求及相同全值工作量，2个主动SIGKILL -9、2个新Main正常0，确认提交后完整1MiB TEXT存活并读回。两项控制各有1600个闭库／崩溃映像文件核对；它们不替代下面失败的原始FULL测试。

新发现的普通VACUUM FULL／WAL恢复缺陷仍OPEN，属于下一项必须处理的非暂缓工作。原始旧模式22条矩阵，两版各完成初开16条并正常退出0；当前16条全值正确，但之后冷启动失败，两版各6条冷开SQL未运行。旧版首次观察器的cleanup断言遮蔽了已退出子进程的wait状态，因此其冷开actual exit未记录；修正观察器后只继续原先未启动的当前角色，实际冷开退出-6，未重跑或补写旧版状态。提前运行的独立审计也因缺少完整receipt实际退出1，失败保留。独立CRC32C核对定位LSN6784528的有效8192字节页镜像：cv_sig、物理身份3、旧块22；FULL后同身份的堆仅18页，重放拒绝跨越4页的缺口。CRC为0xd116b066，名义行宽0是合法format2而非本次原因。本批未修改恢复代码；需要正确的重写物理代次／恢复边界，不能靠忽略错误或无界补页关闭。实际失败快照和正常旧模式混合长值种子均保留在本地。

原62条SQL重新用旧版和当前执行，共124请求、4个Main正常0，完整请求与回复逐字节一致，真实PG18.6原始oracle和既有9项差异保持。P37原28条维护SQL计划字节不变，另在真实PG／旧版／最终C2重新执行，共84核心请求、144行、553单元、129NULL、2,873,498字节、6个Main正常0；额外4条实际OID／PG版本控制单列。原TEXT70003插入、立即读回、冷开读回3个确切缺口现与PG完整值和T7相符，其余25条旧／新完整回复字节不变。28条中24项值／状态／tag匹配、1项按各库真实正OID核对，仍有SQL子分区创建0A000、子表目录身份及冷开身份缺行3项差异，strict PG=false。

实现参考[PostgreSQL18字符类型](https://www.postgresql.org/docs/18/datatype-character.html)、[固定源码varlena.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/utils/adt/varlena.c)和[memutils.h](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/utils/memutils.h)：TEXT没有声明字符宽度，MaxAllocSize为0x3fffffff，逻辑载荷还需扣除4字节varlena头。[SQLite限制](https://www.sqlite.org/limits.html)区分可配置逻辑长度和记录表示；[DuckDB2.0文本类型](https://duckdb.org/docs/2.0/sql/data_types/text.html)不强制VARCHAR(n)的n，本项目没有据此取消已有有界宽度约束。没有三库速度比较。v2为7份原文保存实际HTTP200、响应头和SHA；v1的DuckDB HTML实际403及前5份未持久化响应头的下载记录保留，没有补造响应头或成功状态。

本批完整1500次正常计时没有证明全面提速。仅ID查询服务器CPU比旧版首测／回测高1.78%／1.86%；单长字段首次VACUUM CPU高10.98%／13.76%，墙钟高3.66%／9.08%。多长字段首次VACUUM墙钟低18.80%／高3.52%，CPU低0.33%／高3.27%，属于混合结果。全部负向、旧版漂移、首轮与长尾保留，原因OPEN；本批交付为TEXT容量修复，没有用局部结果宣称整体性能改善。

### 正常计时的实际范围

三组共1500计时操作、114个正常退出Main、2733个完整请求／回复、431349行、1544448单元、109569NULL、825088503字节和14856个闭库文件。正常窗口无本任务编译、压缩、采样器或其他SQL，没有CPU绑定；自然缓存，未声称宿主机完全空闲。墙钟和客户端CPU覆盖完整wire至ReadyI；服务器CPU是整个Main进程并含后台线程，不能视为单请求线程独占。未对本批另做诊断采样，旧P37采样和负例继续保留。

原样270操作：旧56 → C2 → 同旧56回测，3轮27个全新数据库／54正常Main／1098完整回复，沿用同一SQL和独立全值model。覆盖固定／单长／多长字段的插入、更新、删除、读取、VACUUM、TRUNCATE、重写和直接全列／仅ID查询；90项三时钟比较全部列出。

原样270操作矩阵的30个区域，墙钟11低于两旧／10高于两旧／9居间，服务器CPU为9／9／12，客户端CPU为5／10／15。服务器CPU高于两旧的区域包括固定更新16行（+66.28%／+22.94%）、单长重插／TRUNCATE／更新、多长删除／插入1行／维护后读取／VACUUM和固定ID直读；完整数值见全部90行，不能省略退步区域。

固定状态1200查询：每轮更早304 → 旧56 → C2 → 同旧56 → 同更早304，共3轮15个数据库／30正常Main／1380完整回复。每个查询每上下文20次，先取raw20中位数，再取3个上下文中位数；完整raw20、raw3、min/p50/max和长尾保留。所有角色复制同一实际闭库after-UPDATE种子，人口257／257／129。四个查询的墙钟均居于旧版首测与回测之间，且旧版漂移明显，不能将当前相对首测的变化直接归因于代码。仅ID对更早304的CPU为-1.96%／+2.07%，混合控制不能关闭P36/P37原始负例。

固定状态30次首次VACUUM：相同五角色×3轮，共15数据库／30正常Main／255完整回复，复制同一实际after-DELETE种子241／241／113；每个新克隆只计时单长／多长各一次首次VACUUM，完整核对前后和新Main冷开值。没有用第二次幂等VACUUM代替首次工作量；本批核对既有种子和实际准备终态，不额外重跑种子SQL。

### 五角色对照：全部72项比较

单位毫秒，保留相对首测／回测百分比、旧版漂移和分类。每行四组比较为当前对旧56、当前对更早304，以及旧56首测／回测对更早304；不筛选有利组。

#### 1200次查询

| 比较 | 操作 | 时钟 | 首测 | 当前 | 回测 | 当前/首测 | 当前/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|---|---|
| candidate_vs_before56aa | fixed_full_ordered | wall_ns | 26.103119 | 35.181289 | 35.227454 | +34.778% | -0.131% | +34.955% | between_or_equal |
| candidate_vs_earlier304 | fixed_full_ordered | wall_ns | 33.262751 | 35.181289 | 34.965882 | +5.768% | +0.616% | +5.120% | higher_than_both |
| before56aa_first_vs_earlier304 | fixed_full_ordered | wall_ns | 33.262751 | 26.103119 | 34.965882 | -21.524% | -25.347% | +5.120% | lower_than_both |
| before56aa_return_vs_earlier304 | fixed_full_ordered | wall_ns | 33.262751 | 35.227454 | 34.965882 | +5.907% | +0.748% | +5.120% | higher_than_both |
| candidate_vs_before56aa | fixed_full_ordered | server_process_cpu_ns | 18.357693 | 18.211900 | 17.974105 | -0.794% | +1.323% | -2.090% | between_or_equal |
| candidate_vs_earlier304 | fixed_full_ordered | server_process_cpu_ns | 18.326146 | 18.211900 | 18.224711 | -0.623% | -0.070% | -0.553% | lower_than_both |
| before56aa_first_vs_earlier304 | fixed_full_ordered | server_process_cpu_ns | 18.326146 | 18.357693 | 18.224711 | +0.172% | +0.730% | -0.553% | higher_than_both |
| before56aa_return_vs_earlier304 | fixed_full_ordered | server_process_cpu_ns | 18.326146 | 17.974105 | 18.224711 | -1.921% | -1.375% | -0.553% | lower_than_both |
| candidate_vs_before56aa | fixed_full_ordered | client_process_cpu_ns | 4.936088 | 4.965632 | 4.935498 | +0.599% | +0.611% | -0.012% | higher_than_both |
| candidate_vs_earlier304 | fixed_full_ordered | client_process_cpu_ns | 4.791880 | 4.965632 | 5.415015 | +3.626% | -8.299% | +13.004% | between_or_equal |
| before56aa_first_vs_earlier304 | fixed_full_ordered | client_process_cpu_ns | 4.791880 | 4.936088 | 5.415015 | +3.009% | -8.844% | +13.004% | between_or_equal |
| before56aa_return_vs_earlier304 | fixed_full_ordered | client_process_cpu_ns | 4.791880 | 4.935498 | 5.415015 | +2.997% | -8.855% | +13.004% | between_or_equal |
| candidate_vs_before56aa | single_full_ordered | wall_ns | 49.023780 | 56.666926 | 57.019660 | +15.591% | -0.619% | +16.310% | between_or_equal |
| candidate_vs_earlier304 | single_full_ordered | wall_ns | 49.172886 | 56.666926 | 58.750650 | +15.240% | -3.547% | +19.478% | between_or_equal |
| before56aa_first_vs_earlier304 | single_full_ordered | wall_ns | 49.172886 | 49.023780 | 58.750650 | -0.303% | -16.556% | +19.478% | lower_than_both |
| before56aa_return_vs_earlier304 | single_full_ordered | wall_ns | 49.172886 | 57.019660 | 58.750650 | +15.958% | -2.946% | +19.478% | between_or_equal |
| candidate_vs_before56aa | single_full_ordered | server_process_cpu_ns | 40.963397 | 41.615220 | 40.421987 | +1.591% | +2.952% | -1.322% | higher_than_both |
| candidate_vs_earlier304 | single_full_ordered | server_process_cpu_ns | 42.250543 | 41.615220 | 42.290506 | -1.504% | -1.597% | +0.095% | lower_than_both |
| before56aa_first_vs_earlier304 | single_full_ordered | server_process_cpu_ns | 42.250543 | 40.963397 | 42.290506 | -3.046% | -3.138% | +0.095% | lower_than_both |
| before56aa_return_vs_earlier304 | single_full_ordered | server_process_cpu_ns | 42.250543 | 40.421987 | 42.290506 | -4.328% | -4.418% | +0.095% | lower_than_both |
| candidate_vs_before56aa | single_full_ordered | client_process_cpu_ns | 5.882175 | 5.901789 | 5.886713 | +0.333% | +0.256% | +0.077% | higher_than_both |
| candidate_vs_earlier304 | single_full_ordered | client_process_cpu_ns | 5.656456 | 5.901789 | 6.000086 | +4.337% | -1.638% | +6.075% | between_or_equal |
| before56aa_first_vs_earlier304 | single_full_ordered | client_process_cpu_ns | 5.656456 | 5.882175 | 6.000086 | +3.990% | -1.965% | +6.075% | between_or_equal |
| before56aa_return_vs_earlier304 | single_full_ordered | client_process_cpu_ns | 5.656456 | 5.886713 | 6.000086 | +4.071% | -1.890% | +6.075% | between_or_equal |
| candidate_vs_before56aa | overflow_full_no_order | wall_ns | 57.937731 | 62.506515 | 62.815493 | +7.886% | -0.492% | +8.419% | between_or_equal |
| candidate_vs_earlier304 | overflow_full_no_order | wall_ns | 54.157280 | 62.506515 | 62.712143 | +15.417% | -0.328% | +15.796% | between_or_equal |
| before56aa_first_vs_earlier304 | overflow_full_no_order | wall_ns | 54.157280 | 57.937731 | 62.712143 | +6.981% | -7.613% | +15.796% | between_or_equal |
| before56aa_return_vs_earlier304 | overflow_full_no_order | wall_ns | 54.157280 | 62.815493 | 62.712143 | +15.987% | +0.165% | +15.796% | higher_than_both |
| candidate_vs_before56aa | overflow_full_no_order | server_process_cpu_ns | 47.412259 | 46.897725 | 47.744082 | -1.085% | -1.773% | +0.700% | lower_than_both |
| candidate_vs_earlier304 | overflow_full_no_order | server_process_cpu_ns | 47.513261 | 46.897725 | 46.845338 | -1.296% | +0.112% | -1.406% | between_or_equal |
| before56aa_first_vs_earlier304 | overflow_full_no_order | server_process_cpu_ns | 47.513261 | 47.412259 | 46.845338 | -0.213% | +1.210% | -1.406% | between_or_equal |
| before56aa_return_vs_earlier304 | overflow_full_no_order | server_process_cpu_ns | 47.513261 | 47.744082 | 46.845338 | +0.486% | +1.919% | -1.406% | higher_than_both |
| candidate_vs_before56aa | overflow_full_no_order | client_process_cpu_ns | 4.060607 | 3.902137 | 3.800082 | -3.903% | +2.686% | -6.416% | between_or_equal |
| candidate_vs_earlier304 | overflow_full_no_order | client_process_cpu_ns | 3.981911 | 3.902137 | 4.019234 | -2.003% | -2.913% | +0.937% | lower_than_both |
| before56aa_first_vs_earlier304 | overflow_full_no_order | client_process_cpu_ns | 3.981911 | 4.060607 | 4.019234 | +1.976% | +1.029% | +0.937% | higher_than_both |
| before56aa_return_vs_earlier304 | overflow_full_no_order | client_process_cpu_ns | 3.981911 | 3.800082 | 4.019234 | -4.566% | -5.453% | +0.937% | lower_than_both |
| candidate_vs_before56aa | single_id_no_order | wall_ns | 30.196561 | 31.821492 | 32.849944 | +5.381% | -3.131% | +8.787% | between_or_equal |
| candidate_vs_earlier304 | single_id_no_order | wall_ns | 24.877060 | 31.821492 | 32.274552 | +27.915% | -1.404% | +29.736% | between_or_equal |
| before56aa_first_vs_earlier304 | single_id_no_order | wall_ns | 24.877060 | 30.196561 | 32.274552 | +21.383% | -6.438% | +29.736% | between_or_equal |
| before56aa_return_vs_earlier304 | single_id_no_order | wall_ns | 24.877060 | 32.849944 | 32.274552 | +32.049% | +1.783% | +29.736% | higher_than_both |
| candidate_vs_before56aa | single_id_no_order | server_process_cpu_ns | 15.107368 | 15.376183 | 15.095398 | +1.779% | +1.860% | -0.079% | higher_than_both |
| candidate_vs_earlier304 | single_id_no_order | server_process_cpu_ns | 15.684061 | 15.376183 | 15.064257 | -1.963% | +2.071% | -3.952% | between_or_equal |
| before56aa_first_vs_earlier304 | single_id_no_order | server_process_cpu_ns | 15.684061 | 15.107368 | 15.064257 | -3.677% | +0.286% | -3.952% | between_or_equal |
| before56aa_return_vs_earlier304 | single_id_no_order | server_process_cpu_ns | 15.684061 | 15.095398 | 15.064257 | -3.753% | +0.207% | -3.952% | between_or_equal |
| candidate_vs_before56aa | single_id_no_order | client_process_cpu_ns | 4.823811 | 4.861122 | 4.837129 | +0.773% | +0.496% | +0.276% | higher_than_both |
| candidate_vs_earlier304 | single_id_no_order | client_process_cpu_ns | 4.835623 | 4.861122 | 4.902988 | +0.527% | -0.854% | +1.393% | between_or_equal |
| before56aa_first_vs_earlier304 | single_id_no_order | client_process_cpu_ns | 4.835623 | 4.823811 | 4.902988 | -0.244% | -1.615% | +1.393% | lower_than_both |
| before56aa_return_vs_earlier304 | single_id_no_order | client_process_cpu_ns | 4.835623 | 4.837129 | 4.902988 | +0.031% | -1.343% | +1.393% | between_or_equal |

#### 30次首次VACUUM

| 比较 | 操作 | 时钟 | 首测 | 当前 | 回测 | 当前/首测 | 当前/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|---|---|
| candidate_vs_before56aa | vacuum-single | wall_ns | 18.512841 | 19.190731 | 17.593222 | +3.662% | +9.080% | -4.967% | higher_than_both |
| candidate_vs_earlier304 | vacuum-single | wall_ns | 22.498414 | 19.190731 | 18.500575 | -14.702% | +3.730% | -17.769% | between_or_equal |
| before56aa_first_vs_earlier304 | vacuum-single | wall_ns | 22.498414 | 18.512841 | 18.500575 | -17.715% | +0.066% | -17.769% | between_or_equal |
| before56aa_return_vs_earlier304 | vacuum-single | wall_ns | 22.498414 | 17.593222 | 18.500575 | -21.802% | -4.904% | -17.769% | lower_than_both |
| candidate_vs_before56aa | vacuum-single | server_process_cpu_ns | 10.091119 | 11.198858 | 9.844691 | +10.977% | +13.755% | -2.442% | higher_than_both |
| candidate_vs_earlier304 | vacuum-single | server_process_cpu_ns | 11.414452 | 11.198858 | 11.010305 | -1.889% | +1.713% | -3.541% | between_or_equal |
| before56aa_first_vs_earlier304 | vacuum-single | server_process_cpu_ns | 11.414452 | 10.091119 | 11.010305 | -11.593% | -8.348% | -3.541% | lower_than_both |
| before56aa_return_vs_earlier304 | vacuum-single | server_process_cpu_ns | 11.414452 | 9.844691 | 11.010305 | -13.752% | -10.587% | -3.541% | lower_than_both |
| candidate_vs_before56aa | vacuum-single | client_process_cpu_ns | 0.234750 | 0.218712 | 0.232890 | -6.832% | -6.088% | -0.792% | lower_than_both |
| candidate_vs_earlier304 | vacuum-single | client_process_cpu_ns | 0.196746 | 0.218712 | 0.260609 | +11.165% | -16.077% | +32.460% | between_or_equal |
| before56aa_first_vs_earlier304 | vacuum-single | client_process_cpu_ns | 0.196746 | 0.234750 | 0.260609 | +19.316% | -9.923% | +32.460% | between_or_equal |
| before56aa_return_vs_earlier304 | vacuum-single | client_process_cpu_ns | 0.196746 | 0.232890 | 0.260609 | +18.371% | -10.636% | +32.460% | between_or_equal |
| candidate_vs_before56aa | vacuum-overflow | wall_ns | 30.295591 | 24.601222 | 23.763595 | -18.796% | +3.525% | -21.561% | between_or_equal |
| candidate_vs_earlier304 | vacuum-overflow | wall_ns | 26.075458 | 24.601222 | 24.273995 | -5.654% | +1.348% | -6.909% | between_or_equal |
| before56aa_first_vs_earlier304 | vacuum-overflow | wall_ns | 26.075458 | 30.295591 | 24.273995 | +16.184% | +24.807% | -6.909% | higher_than_both |
| before56aa_return_vs_earlier304 | vacuum-overflow | wall_ns | 26.075458 | 23.763595 | 24.273995 | -8.866% | -2.103% | -6.909% | lower_than_both |
| candidate_vs_before56aa | vacuum-overflow | server_process_cpu_ns | 16.153725 | 16.100506 | 15.590094 | -0.329% | +3.274% | -3.489% | between_or_equal |
| candidate_vs_earlier304 | vacuum-overflow | server_process_cpu_ns | 15.025219 | 16.100506 | 16.252087 | +7.157% | -0.933% | +8.165% | between_or_equal |
| before56aa_first_vs_earlier304 | vacuum-overflow | server_process_cpu_ns | 15.025219 | 16.153725 | 16.252087 | +7.511% | -0.605% | +8.165% | between_or_equal |
| before56aa_return_vs_earlier304 | vacuum-overflow | server_process_cpu_ns | 15.025219 | 15.590094 | 16.252087 | +3.760% | -4.073% | +8.165% | between_or_equal |
| candidate_vs_before56aa | vacuum-overflow | client_process_cpu_ns | 0.201602 | 0.212452 | 0.229893 | +5.382% | -7.587% | +14.033% | between_or_equal |
| candidate_vs_earlier304 | vacuum-overflow | client_process_cpu_ns | 0.228710 | 0.212452 | 0.236537 | -7.109% | -10.182% | +3.422% | lower_than_both |
| before56aa_first_vs_earlier304 | vacuum-overflow | client_process_cpu_ns | 0.228710 | 0.201602 | 0.236537 | -11.853% | -14.769% | +3.422% | lower_than_both |
| before56aa_return_vs_earlier304 | vacuum-overflow | client_process_cpu_ns | 0.228710 | 0.229893 | 0.236537 | +0.517% | -2.809% | +3.422% | between_or_equal |

### 原样270操作：全部90项三时钟比较

每角色3次原始样本的中位数，单位毫秒；所有raw3/min/p50/max和270原始计时均在压缩证据中。

#### 墙钟

| 区域 | 旧版 | C2 | 旧版回测 | C2/旧版 | C2/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|
| canonical-fixed-single/fixed/delete16 | 46.469 | 49.415 | 67.393 | +6.34% | -26.68% | +45.03% | between_or_equal |
| canonical-fixed-single/fixed/insert1 | 18.254 | 18.774 | 20.545 | +2.85% | -8.62% | +12.55% | between_or_equal |
| canonical-fixed-single/fixed/reinsert_population | 736.396 | 626.226 | 629.920 | -14.96% | -0.59% | -14.46% | lower_than_both |
| canonical-fixed-single/fixed/select_after_update | 19.661 | 19.639 | 21.691 | -0.11% | -9.46% | +10.33% | lower_than_both |
| canonical-fixed-single/fixed/select_after_vacuum | 18.897 | 17.900 | 22.357 | -5.28% | -19.94% | +18.31% | lower_than_both |
| canonical-fixed-single/fixed/truncate | 240.212 | 177.368 | 278.961 | -26.16% | -36.42% | +16.13% | lower_than_both |
| canonical-fixed-single/fixed/update16 | 60.501 | 90.525 | 74.246 | +49.63% | +21.93% | +22.72% | higher_than_both |
| canonical-fixed-single/fixed/vacuum | 1.565 | 1.861 | 2.164 | +18.94% | -14.00% | +38.31% | between_or_equal |
| canonical-fixed-single/single/delete16 | 82.944 | 82.800 | 80.048 | -0.17% | +3.44% | -3.49% | between_or_equal |
| canonical-fixed-single/single/insert1 | 21.170 | 20.123 | 21.154 | -4.95% | -4.87% | -0.08% | lower_than_both |
| canonical-fixed-single/single/reinsert_population | 1105.348 | 1129.678 | 1109.926 | +2.20% | +1.78% | +0.41% | higher_than_both |
| canonical-fixed-single/single/select_after_update | 44.462 | 38.267 | 40.691 | -13.93% | -5.96% | -8.48% | lower_than_both |
| canonical-fixed-single/single/select_after_vacuum | 46.697 | 39.122 | 41.468 | -16.22% | -5.66% | -11.20% | lower_than_both |
| canonical-fixed-single/single/truncate | 217.931 | 263.686 | 298.080 | +20.99% | -11.54% | +36.78% | between_or_equal |
| canonical-fixed-single/single/update16 | 147.068 | 157.513 | 149.377 | +7.10% | +5.45% | +1.57% | higher_than_both |
| canonical-fixed-single/single/vacuum | 8.931 | 8.481 | 8.426 | -5.04% | +0.65% | -5.65% | between_or_equal |
| canonical-overflow/overflow/delete16 | 79.719 | 82.780 | 77.875 | +3.84% | +6.30% | -2.31% | higher_than_both |
| canonical-overflow/overflow/insert1 | 27.196 | 28.103 | 18.215 | +3.34% | +54.29% | -33.02% | higher_than_both |
| canonical-overflow/overflow/reinsert_population | 1093.149 | 1059.522 | 1112.573 | -3.08% | -4.77% | +1.78% | lower_than_both |
| canonical-overflow/overflow/select_after_update | 45.664 | 39.052 | 36.310 | -14.48% | +7.55% | -20.48% | between_or_equal |
| canonical-overflow/overflow/select_after_vacuum | 48.099 | 49.750 | 42.706 | +3.43% | +16.49% | -11.21% | higher_than_both |
| canonical-overflow/overflow/truncate | 157.225 | 204.931 | 165.479 | +30.34% | +23.84% | +5.25% | higher_than_both |
| canonical-overflow/overflow/update16 | 173.870 | 166.977 | 159.953 | -3.96% | +4.39% | -8.00% | between_or_equal |
| canonical-overflow/overflow/vacuum | 9.113 | 11.944 | 11.137 | +31.07% | +7.25% | +22.21% | higher_than_both |
| direct-hit/fixed/direct_full_no_order | 24.749 | 22.389 | 24.446 | -9.54% | -8.41% | -1.23% | lower_than_both |
| direct-hit/fixed/direct_id_only_no_order | 21.516 | 22.291 | 21.482 | +3.60% | +3.77% | -0.16% | higher_than_both |
| direct-hit/overflow/direct_full_no_order | 52.899 | 50.250 | 53.338 | -5.01% | -5.79% | +0.83% | lower_than_both |
| direct-hit/overflow/direct_id_only_no_order | 17.963 | 18.511 | 22.687 | +3.06% | -18.41% | +26.30% | between_or_equal |
| direct-hit/single/direct_full_no_order | 45.723 | 44.146 | 47.852 | -3.45% | -7.74% | +4.66% | lower_than_both |
| direct-hit/single/direct_id_only_no_order | 19.617 | 25.364 | 24.542 | +29.30% | +3.35% | +25.11% | higher_than_both |

#### 服务器CPU

| 区域 | 旧版 | C2 | 旧版回测 | C2/旧版 | C2/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|
| canonical-fixed-single/fixed/delete16 | 36.656 | 41.928 | 47.589 | +14.38% | -11.90% | +29.83% | between_or_equal |
| canonical-fixed-single/fixed/insert1 | 8.904 | 8.862 | 9.945 | -0.47% | -10.89% | +11.70% | lower_than_both |
| canonical-fixed-single/fixed/reinsert_population | 594.003 | 586.713 | 582.216 | -1.23% | +0.77% | -1.98% | between_or_equal |
| canonical-fixed-single/fixed/select_after_update | 9.001 | 10.022 | 11.663 | +11.35% | -14.07% | +29.58% | between_or_equal |
| canonical-fixed-single/fixed/select_after_vacuum | 11.139 | 9.501 | 12.297 | -14.70% | -22.74% | +10.40% | lower_than_both |
| canonical-fixed-single/fixed/truncate | 105.692 | 83.957 | 101.807 | -20.56% | -17.53% | -3.68% | lower_than_both |
| canonical-fixed-single/fixed/update16 | 48.176 | 80.105 | 65.157 | +66.28% | +22.94% | +35.25% | higher_than_both |
| canonical-fixed-single/fixed/vacuum | 1.460 | 1.731 | 2.081 | +18.52% | -16.84% | +42.52% | between_or_equal |
| canonical-fixed-single/single/delete16 | 73.707 | 67.742 | 69.394 | -8.09% | -2.38% | -5.85% | lower_than_both |
| canonical-fixed-single/single/insert1 | 14.080 | 10.778 | 9.003 | -23.45% | +19.71% | -36.05% | between_or_equal |
| canonical-fixed-single/single/reinsert_population | 1025.588 | 1043.117 | 1026.806 | +1.71% | +1.59% | +0.12% | higher_than_both |
| canonical-fixed-single/single/select_after_update | 37.699 | 32.226 | 31.467 | -14.52% | +2.41% | -16.53% | between_or_equal |
| canonical-fixed-single/single/select_after_vacuum | 38.247 | 32.119 | 34.184 | -16.02% | -6.04% | -10.62% | lower_than_both |
| canonical-fixed-single/single/truncate | 129.486 | 134.845 | 131.393 | +4.14% | +2.63% | +1.47% | higher_than_both |
| canonical-fixed-single/single/update16 | 126.571 | 129.150 | 124.592 | +2.04% | +3.66% | -1.56% | higher_than_both |
| canonical-fixed-single/single/vacuum | 8.586 | 8.066 | 7.975 | -6.06% | +1.14% | -7.12% | between_or_equal |
| canonical-overflow/overflow/delete16 | 69.511 | 72.512 | 67.415 | +4.32% | +7.56% | -3.01% | higher_than_both |
| canonical-overflow/overflow/insert1 | 12.328 | 17.960 | 11.228 | +45.68% | +59.96% | -8.93% | higher_than_both |
| canonical-overflow/overflow/reinsert_population | 1054.258 | 1035.462 | 1053.835 | -1.78% | -1.74% | -0.04% | lower_than_both |
| canonical-overflow/overflow/select_after_update | 40.234 | 31.809 | 29.928 | -20.94% | +6.29% | -25.62% | between_or_equal |
| canonical-overflow/overflow/select_after_vacuum | 42.678 | 44.112 | 36.187 | +3.36% | +21.90% | -15.21% | higher_than_both |
| canonical-overflow/overflow/truncate | 88.296 | 86.149 | 91.239 | -2.43% | -5.58% | +3.33% | lower_than_both |
| canonical-overflow/overflow/update16 | 148.815 | 138.773 | 142.686 | -6.75% | -2.74% | -4.12% | lower_than_both |
| canonical-overflow/overflow/vacuum | 8.382 | 11.561 | 10.861 | +37.93% | +6.45% | +29.58% | higher_than_both |
| direct-hit/fixed/direct_full_no_order | 14.004 | 14.542 | 14.951 | +3.84% | -2.74% | +6.76% | between_or_equal |
| direct-hit/fixed/direct_id_only_no_order | 14.039 | 14.304 | 13.864 | +1.89% | +3.18% | -1.25% | higher_than_both |
| direct-hit/overflow/direct_full_no_order | 43.484 | 42.775 | 45.017 | -1.63% | -4.98% | +3.52% | lower_than_both |
| direct-hit/overflow/direct_id_only_no_order | 11.486 | 12.178 | 12.381 | +6.02% | -1.64% | +7.79% | between_or_equal |
| direct-hit/single/direct_full_no_order | 35.962 | 37.623 | 39.144 | +4.62% | -3.89% | +8.85% | between_or_equal |
| direct-hit/single/direct_id_only_no_order | 13.266 | 16.030 | 16.820 | +20.84% | -4.70% | +26.79% | between_or_equal |

#### 客户端CPU

| 区域 | 旧版 | C2 | 旧版回测 | C2/旧版 | C2/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|
| canonical-fixed-single/fixed/delete16 | 0.231 | 0.243 | 0.202 | +5.20% | +20.33% | -12.57% | higher_than_both |
| canonical-fixed-single/fixed/insert1 | 0.260 | 0.236 | 0.128 | -9.36% | +84.32% | -50.83% | between_or_equal |
| canonical-fixed-single/fixed/reinsert_population | 0.258 | 0.232 | 0.215 | -9.79% | +8.02% | -16.49% | between_or_equal |
| canonical-fixed-single/fixed/select_after_update | 4.759 | 4.911 | 4.708 | +3.21% | +4.32% | -1.07% | higher_than_both |
| canonical-fixed-single/fixed/select_after_vacuum | 4.606 | 4.498 | 4.523 | -2.33% | -0.55% | -1.79% | lower_than_both |
| canonical-fixed-single/fixed/truncate | 0.263 | 0.254 | 0.206 | -3.38% | +23.17% | -21.56% | between_or_equal |
| canonical-fixed-single/fixed/update16 | 0.221 | 0.238 | 0.230 | +7.59% | +3.33% | +4.12% | higher_than_both |
| canonical-fixed-single/fixed/vacuum | 0.200 | 0.223 | 0.115 | +11.25% | +94.13% | -42.69% | higher_than_both |
| canonical-fixed-single/single/delete16 | 0.270 | 0.225 | 0.233 | -16.84% | -3.72% | -13.63% | lower_than_both |
| canonical-fixed-single/single/insert1 | 0.229 | 0.223 | 0.196 | -2.51% | +13.81% | -14.33% | between_or_equal |
| canonical-fixed-single/single/reinsert_population | 0.598 | 0.580 | 0.555 | -3.11% | +4.45% | -7.24% | between_or_equal |
| canonical-fixed-single/single/select_after_update | 5.951 | 5.951 | 5.732 | -0.01% | +3.81% | -3.68% | between_or_equal |
| canonical-fixed-single/single/select_after_vacuum | 5.427 | 5.059 | 4.856 | -6.76% | +4.18% | -10.51% | between_or_equal |
| canonical-fixed-single/single/truncate | 0.225 | 0.242 | 0.262 | +7.66% | -7.50% | +16.39% | between_or_equal |
| canonical-fixed-single/single/update16 | 0.241 | 0.229 | 0.246 | -4.82% | -6.78% | +2.11% | lower_than_both |
| canonical-fixed-single/single/vacuum | 0.210 | 0.223 | 0.216 | +5.82% | +3.15% | +2.59% | higher_than_both |
| canonical-overflow/overflow/delete16 | 0.250 | 0.245 | 0.216 | -1.84% | +13.68% | -13.65% | between_or_equal |
| canonical-overflow/overflow/insert1 | 0.224 | 0.256 | 0.168 | +14.43% | +52.79% | -25.11% | higher_than_both |
| canonical-overflow/overflow/reinsert_population | 0.768 | 0.896 | 0.785 | +16.70% | +14.11% | +2.27% | higher_than_both |
| canonical-overflow/overflow/select_after_update | 3.960 | 4.047 | 3.681 | +2.19% | +9.94% | -7.05% | higher_than_both |
| canonical-overflow/overflow/select_after_vacuum | 3.644 | 3.728 | 3.328 | +2.32% | +12.03% | -8.67% | higher_than_both |
| canonical-overflow/overflow/truncate | 0.227 | 0.249 | 0.260 | +9.71% | -4.42% | +14.79% | between_or_equal |
| canonical-overflow/overflow/update16 | 0.250 | 0.247 | 0.260 | -0.99% | -4.93% | +4.14% | lower_than_both |
| canonical-overflow/overflow/vacuum | 0.205 | 0.231 | 0.226 | +12.76% | +2.14% | +10.40% | higher_than_both |
| direct-hit/fixed/direct_full_no_order | 4.946 | 4.842 | 4.841 | -2.09% | +0.03% | -2.12% | between_or_equal |
| direct-hit/fixed/direct_id_only_no_order | 4.760 | 4.801 | 4.812 | +0.87% | -0.23% | +1.11% | between_or_equal |
| direct-hit/overflow/direct_full_no_order | 3.790 | 3.908 | 3.990 | +3.11% | -2.06% | +5.28% | between_or_equal |
| direct-hit/overflow/direct_id_only_no_order | 2.107 | 1.862 | 2.069 | -11.63% | -9.99% | -1.82% | lower_than_both |
| direct-hit/single/direct_full_no_order | 4.465 | 5.598 | 6.037 | +25.38% | -7.28% | +35.23% | between_or_equal |
| direct-hit/single/direct_id_only_no_order | 3.418 | 3.767 | 3.839 | +10.22% | -1.87% | +12.32% | between_or_equal |

完整证据压缩为[scalar-text-logical-limit-20261107.json.gz](benchmarks/scalar-text-logical-limit-20261107.json.gz)，49,147,970字节（46.87MiB），SHA256 `eeba9391c2f4be8b8624e4589879a5d8451ed984ba8f2584d25e9fe72d700cc1`；内层完整证据JSON为3,229,936,943字节。59,525个非ELF文件、2,157个目录、3,227个内容块、280个本地ELF引用均在清理前后独立核对。删除55,639个已逐字节归档的生成副本，逻辑文件量5,362,749,517字节（4.994GiB），不等同于独占文件系统释放量。保留源码、ELF、任务stdout/stderr、错误和失败页原始字节、目录、实际失败旧库及普通维护／1MiB长值正常闭库；外部P37 after-DELETE和P36 after-UPDATE种子不动。生成数据目录中的dbms.log副本保存在归档中，旧38个压缩包不动。首轮64MiB字典压缩得到75,818,006字节，被49MiB上限拦截而actual1，没有写入Git或清理数据；512MiB字典得到51,533,706字节，低于GitHub的50MiB建议但仍高于本地49MiB保护线，同样未写入Git或清理数据。两次失败记录已嵌入最终证据，最终使用1024MiB字典压缩完整内容。

用户指出的analyze-reindex-main-costs-20261104原始JSON为56,047,459字节（53.45MiB），当前Git使用5,977,159字节（5.70MiB）的.json.gz保存完整内容，manifest记录原始SHA及逐字节解压验证。原始大JSON不再跟踪，忽略规则拦截后续同目录原始JSON（小manifest除外）；保留压缩证据用于复核实际耗时和错误，未重写历史提交。

可逐字节提取三组完整raw3/min/p50/max表；完整raw20、原始三时钟括号和尾部在同目录独立摘要及原始wire中：

```sh
python3 - <<'PY'
import base64, gzip, hashlib, json, lzma, pathlib, sys
e = json.loads(gzip.decompress(pathlib.Path('docs/benchmarks/scalar-text-logical-limit-20261107.json.gz').read_bytes()))
raw = lzma.decompress(base64.b64decode(e['payload_xz_base64'], validate=True))
assert hashlib.sha256(raw).hexdigest() == e['payload_raw_sha256']
b = json.loads(raw)
names = [
    'batch/scalar-text-cost27-C2-v1/all-performance-tables-v3.md',
    'batch/matched-reads5-C2-v1/all-performance-tables-v1.md',
    'batch/matched-vacuum5-C2-v1/all-performance-tables-v1.md',
]
for name in names:
    f = b['artifacts'][name]
    v = base64.b64decode(b['blobs'][f['blob']], validate=True)
    assert hashlib.sha256(v).hexdigest() == f['sha256']
    sys.stdout.buffer.write(v)
PY
```

正式库存2399个测试文件（1132顶层／1137总C++），注册E2E684／独立E2E664不变。754个Markdown路径、725份保护记录保持，8份既有文档只插入本批说明。原273项、22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有287条历史保持并新增partial第288条，IDX-07仍unverified/evidence[]。

只修复标量TEXT逻辑容量限制，未关闭整个TYPE／STO功能族；大VARCHAR、原62条协议差异、SQL子分区、既有TOAST断言、普通FULL恢复故障及所有历史负向性能继续OPEN。执行图／child-provider生命周期、retained planning／portal、CPU-sort interrupt和用户明确暂缓的故障矩阵保持暂缓；普通FULL恢复没有归入暂缓范围。Goal active，直接master分批提交，未推送或替换默认程序。正常CMake C++17/O2最终C2为84bbadb4，旧版56aad0c0，更早30412452，默认425165af不变。C2只澄清一行注释，真实重新编译后61个对象和完整ELF均与已执行功能测试的C1逐字节一致，保存该复用证明，没有重采功能样本。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-20261107"></a>
## 2026-11-07：VACUUM长字段引用安全、1500项正负性能对照（P37-C1）

本批1500项完整正常计时没有证明全面提速。在共同的真实after-DELETE种子上，单长字段首次VACUUM服务器CPU比旧版及旧版回测低8.36%／7.73%；多长字段首次VACUUM墙钟高9.83%／27.97%，CPU分别低1.92%／高35.29%。在共同after-UPDATE种子上，仅选ID的CPU高8.43%／8.39%，墙钟高9.77%／14.77%；三个全列查询墙钟也高于两次旧版，完整正负结果保留，原因OPEN。

当调用者仍绑定一次真实 TableScan 的正 RID，特别是另一个数据库中同名表的 NULL 行时，旧收集器可能漏掉仍在使用的 TOAST 引用并回收这些对象；新代码只使用正在检查的物理元组。该触发由原生维护接口复现，未将正常 SQL 对照中的旧／新相同结果伪称为 SQL 数据丢失复现。

修复普通 VACUUM 的长字段引用收集：在原有页锁和 pin 内，直接读取所属物理元组的 NULL 位图、变长字段目录和有界字节跨度，不再通过可能读取调用者 RID/NULL 状态的执行器取值接口。固定区大小与变长列索引每次维护预先计算，普通内联值无需复制整行或逐值构造字符串。活动快照保护、所有物理存活槽的遍历、锁／pin、索引／分块校验和回收策略保持。

设计依据：[PostgreSQL18 TOAST说明](https://www.postgresql.org/docs/18/storage-toast.html)和[固定源码heaptoast.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/heap/heaptoast.c)使用所属tuple的值与NULL处理外部字段；源码说明一次展开可避免逐属性重复访问的开销。[SQLite文件格式](https://www.sqlite.org/fileformat.html#cell_payload_overflow_pages)将溢出链及指针映射与实际存储页联系。本项目据此选择在已锁定的物理元组中识别引用，没有引入另一个执行器上下文。原文和HTTP元数据存档。第四次HTTP200取得的DuckDB current/VACUUM页面实际是指向2.0页面的HTML重定向，只保留为访问记录，未据此判断其VACUUM行为；没有三库速度对比。

新增 vacuum_toast_physical_reference_test 在同一冻结源码上执行全部16种情形：旧版160检查／48失败／actual1，C1为160／0／actual0。覆盖普通表与原生范围分区存储、vacuumToast及外层vacuum、真实同库／跨库同名表调用者、跨位图字节的NULL、空串、文本NULL、UTF-8、类似marker的普通字符串、TEXT11003／40003及BYTEA70003／11009。独立完整值物理解码核对16种情形中的80个存活对象全部原始字节，以及每个真实孤儿的回收、调用者绑定恢复和第二次维护幂等。

机制控制中，每7个物理行，已绑定调用者下的文件身份校验由旧版25／26次降至C1的4／5次；未绑定控制两版均4／5。计数来自真实BufferPool接口包装，并不等同于完整操作提速，不能替代下面的实际成本。

8项相关原生测试在旧版和C1均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test。其中旧版后3项复用相同源码／冻结对象／ELF的既有正常终态，其余均本批实际执行。既有toast_test在旧版与C1仍于第604行 `!corruptScan.open()` 断言中止（actual -6），保留失败，后续断言未运行；未修改测试预期，full suite=false。

原62条SQL在旧版30412452和C1全部执行，4个初开／冷开Main正常0，完整请求／回复逐字节相同，沿用并核对真实PostgreSQL18.6原始oracle，既有9条协议差异仍在。另以真实PG18.6、旧版和C1各执行28条新维护SQL及冷开读取，共84请求／142行／549单元／129NULL／2733516字节，6个Main正常0；第二套解码器核对全值、NULL、完整T7、tag、Ready和原始错误／NOTICE。28条中旧／新完整wire一致；21项值／状态／tag与PG相符，1项用各库真实正OID分别核对，另6项PG差异保留，strict PG=false。

本批实际PG缺口包括：TEXT70003字符INSERT在产品返回22023而PG成功，立即读取与冷开读取缺该行；SQL `CREATE TABLE ... PARTITION OF` 返回0A000，子表目录身份及冷开身份读取缺行。原生分区物理存储控制不代表这项SQL功能已实现，也不代表逻辑分区扫描已通过。

### 正常计时的工作量与边界

三组完整对照共1500计时操作、114个正常退出Main、2733个完整请求／回复、431349行、1544448单元、109569NULL、825088503请求／回复字节和14856个闭库文件。所有初轮／热态／尾部、原始三时钟括号、完整错误与最长尾部保留。正常窗口无本任务编译、采样器、压缩、CPU绑定或竞争SQL；自然缓存，未宣称宿主机空闲。墙钟／客户端CPU覆盖完整wire直至ReadyI，服务器CPU为整个Main进程，包含后台线程，不能视为请求线程独占成本。

原样270操作：旧304 → C1 → 同一旧304回测，3轮，27个全新数据库／54个正常Main／1098完整回复；与既有矩阵相同的SQL和全值model。下面按三时钟列出全部90个区域中位数，不剔除原始样本。

原样270操作矩阵的30个区域中，墙钟22低于两旧／3高于两旧／5居间，服务器CPU为16／4／10，客户端CPU为10／9／11。固定／单长字段／多长字段的插入、更新、删除、全列读取、VACUUM、TRUNCATE和重写，以及直接全列／仅ID查询全部保留；不能用部分区域改善覆盖其他操作的回退。

固定状态1200次查询：每轮依次更早774 → 旧304 → C1 → 同旧304 → 同更早774，3轮共15个数据库／30个正常Main／1380完整回复。各查询每上下文20次，先求20次中位数，再求3上下文中位数，全部raw20与raw3/min/p50/max及两旧回测漂移留存。所有角色克隆同一实际正常闭库after-UPDATE种子，人口257／257／129。

固定状态30次首次VACUUM：同样5角色×3轮，共15数据库／30正常Main／255完整回复。共同种子是在上述实际数据上执行原始DELETE16后正常关闭并冷开全值核对的241／241／113人口；每个全新克隆仅对单长字段、多长字段分别计时第一次VACUUM。每次前后及冷开全列值均核对，未用幂等重复VACUUM替代首轮工作量。种子准备另15个未计时请求／2正常Main。

与更早774的控制中，仅ID的当前C1 CPU高7.23%／4.83%，而旧304的首测和回测均低于两次774约1%～3%。这次共同after-UPDATE状态未重现P36旧304的ID负例；P36原测量仍保留，不能以本批结果宣称其历史负例已修复。当前C1相对304的新负例也未关闭。

### 完整五角色对照：全部72项比较

单位毫秒，两个百分比分别相对首测／回测；每行四组比较分别保留C1对旧304、C1对更早774，以及旧304首测／回测对更早774。各行保留比较自身的旧版漂移与分类；raw3分布见压缩证据提取命令。

#### 1200次查询

| 比较 | 操作 | 时钟 | 首测 | 当前 | 回测 | 当前/首测 | 当前/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|---|---|
| candidate_vs_before304 | fixed_full_ordered | wall_ns | 25.184097 | 26.665354 | 24.881898 | +5.882% | +7.168% | -1.200% | higher_than_both |
| candidate_vs_earlier774 | fixed_full_ordered | wall_ns | 35.341190 | 26.665354 | 34.200996 | -24.549% | -22.033% | -3.226% | lower_than_both |
| before304_first_vs_earlier774 | fixed_full_ordered | wall_ns | 35.341190 | 25.184097 | 34.200996 | -28.740% | -26.364% | -3.226% | lower_than_both |
| before304_return_vs_earlier774 | fixed_full_ordered | wall_ns | 35.341190 | 24.881898 | 34.200996 | -29.595% | -27.248% | -3.226% | lower_than_both |
| candidate_vs_before304 | fixed_full_ordered | server_process_cpu_ns | 18.284492 | 18.115202 | 16.886789 | -0.926% | +7.274% | -7.644% | between_or_equal |
| candidate_vs_earlier774 | fixed_full_ordered | server_process_cpu_ns | 26.896024 | 18.115202 | 26.470136 | -32.647% | -31.564% | -1.583% | lower_than_both |
| before304_first_vs_earlier774 | fixed_full_ordered | server_process_cpu_ns | 26.896024 | 18.284492 | 26.470136 | -32.018% | -30.924% | -1.583% | lower_than_both |
| before304_return_vs_earlier774 | fixed_full_ordered | server_process_cpu_ns | 26.896024 | 16.886789 | 26.470136 | -37.215% | -36.204% | -1.583% | lower_than_both |
| candidate_vs_before304 | fixed_full_ordered | client_process_cpu_ns | 4.907517 | 4.926501 | 4.865310 | +0.387% | +1.258% | -0.860% | higher_than_both |
| candidate_vs_earlier774 | fixed_full_ordered | client_process_cpu_ns | 4.909381 | 4.926501 | 4.804075 | +0.349% | +2.548% | -2.145% | higher_than_both |
| before304_first_vs_earlier774 | fixed_full_ordered | client_process_cpu_ns | 4.909381 | 4.907517 | 4.804075 | -0.038% | +2.153% | -2.145% | between_or_equal |
| before304_return_vs_earlier774 | fixed_full_ordered | client_process_cpu_ns | 4.909381 | 4.865310 | 4.804075 | -0.898% | +1.275% | -2.145% | between_or_equal |
| candidate_vs_before304 | single_full_ordered | wall_ns | 46.407216 | 48.664895 | 47.770474 | +4.865% | +1.872% | +2.938% | higher_than_both |
| candidate_vs_earlier774 | single_full_ordered | wall_ns | 65.840513 | 48.664895 | 77.389769 | -26.087% | -37.117% | +17.541% | lower_than_both |
| before304_first_vs_earlier774 | single_full_ordered | wall_ns | 65.840513 | 46.407216 | 77.389769 | -29.516% | -40.034% | +17.541% | lower_than_both |
| before304_return_vs_earlier774 | single_full_ordered | wall_ns | 65.840513 | 47.770474 | 77.389769 | -27.445% | -38.273% | +17.541% | lower_than_both |
| candidate_vs_before304 | single_full_ordered | server_process_cpu_ns | 40.537676 | 40.800626 | 41.059326 | +0.649% | -0.630% | +1.287% | between_or_equal |
| candidate_vs_earlier774 | single_full_ordered | server_process_cpu_ns | 56.800852 | 40.800626 | 59.863915 | -28.169% | -31.844% | +5.393% | lower_than_both |
| before304_first_vs_earlier774 | single_full_ordered | server_process_cpu_ns | 56.800852 | 40.537676 | 59.863915 | -28.632% | -32.284% | +5.393% | lower_than_both |
| before304_return_vs_earlier774 | single_full_ordered | server_process_cpu_ns | 56.800852 | 41.059326 | 59.863915 | -27.714% | -31.412% | +5.393% | lower_than_both |
| candidate_vs_before304 | single_full_ordered | client_process_cpu_ns | 5.911600 | 5.858821 | 5.801787 | -0.893% | +0.983% | -1.858% | between_or_equal |
| candidate_vs_earlier774 | single_full_ordered | client_process_cpu_ns | 5.684582 | 5.858821 | 5.747815 | +3.065% | +1.931% | +1.112% | higher_than_both |
| before304_first_vs_earlier774 | single_full_ordered | client_process_cpu_ns | 5.684582 | 5.911600 | 5.747815 | +3.994% | +2.850% | +1.112% | higher_than_both |
| before304_return_vs_earlier774 | single_full_ordered | client_process_cpu_ns | 5.684582 | 5.801787 | 5.747815 | +2.062% | +0.939% | +1.112% | higher_than_both |
| candidate_vs_before304 | overflow_full_no_order | wall_ns | 55.095008 | 56.053177 | 53.453280 | +1.739% | +4.864% | -2.980% | higher_than_both |
| candidate_vs_earlier774 | overflow_full_no_order | wall_ns | 61.435305 | 56.053177 | 70.224788 | -8.761% | -20.180% | +14.307% | lower_than_both |
| before304_first_vs_earlier774 | overflow_full_no_order | wall_ns | 61.435305 | 55.095008 | 70.224788 | -10.320% | -21.545% | +14.307% | lower_than_both |
| before304_return_vs_earlier774 | overflow_full_no_order | wall_ns | 61.435305 | 53.453280 | 70.224788 | -12.993% | -23.883% | +14.307% | lower_than_both |
| candidate_vs_before304 | overflow_full_no_order | server_process_cpu_ns | 47.216376 | 47.434463 | 46.992216 | +0.462% | +0.941% | -0.475% | higher_than_both |
| candidate_vs_earlier774 | overflow_full_no_order | server_process_cpu_ns | 52.034360 | 47.434463 | 53.440602 | -8.840% | -11.239% | +2.703% | lower_than_both |
| before304_first_vs_earlier774 | overflow_full_no_order | server_process_cpu_ns | 52.034360 | 47.216376 | 53.440602 | -9.259% | -11.647% | +2.703% | lower_than_both |
| before304_return_vs_earlier774 | overflow_full_no_order | server_process_cpu_ns | 52.034360 | 46.992216 | 53.440602 | -9.690% | -12.066% | +2.703% | lower_than_both |
| candidate_vs_before304 | overflow_full_no_order | client_process_cpu_ns | 3.983411 | 3.957764 | 3.852718 | -0.644% | +2.727% | -3.281% | between_or_equal |
| candidate_vs_earlier774 | overflow_full_no_order | client_process_cpu_ns | 3.858651 | 3.957764 | 3.926626 | +2.569% | +0.793% | +1.762% | higher_than_both |
| before304_first_vs_earlier774 | overflow_full_no_order | client_process_cpu_ns | 3.858651 | 3.983411 | 3.926626 | +3.233% | +1.446% | +1.762% | higher_than_both |
| before304_return_vs_earlier774 | overflow_full_no_order | client_process_cpu_ns | 3.858651 | 3.852718 | 3.926626 | -0.154% | -1.882% | +1.762% | lower_than_both |
| candidate_vs_before304 | single_id_no_order | wall_ns | 24.309490 | 26.684755 | 23.250245 | +9.771% | +14.772% | -4.357% | higher_than_both |
| candidate_vs_earlier774 | single_id_no_order | wall_ns | 24.428358 | 26.684755 | 32.278663 | +9.237% | -17.330% | +32.136% | between_or_equal |
| before304_first_vs_earlier774 | single_id_no_order | wall_ns | 24.428358 | 24.309490 | 32.278663 | -0.487% | -24.689% | +32.136% | lower_than_both |
| before304_return_vs_earlier774 | single_id_no_order | wall_ns | 24.428358 | 23.250245 | 32.278663 | -4.823% | -27.970% | +32.136% | lower_than_both |
| candidate_vs_before304 | single_id_no_order | server_process_cpu_ns | 15.099545 | 16.372758 | 15.105557 | +8.432% | +8.389% | +0.040% | higher_than_both |
| candidate_vs_earlier774 | single_id_no_order | server_process_cpu_ns | 15.269316 | 16.372758 | 15.617999 | +7.227% | +4.833% | +2.284% | higher_than_both |
| before304_first_vs_earlier774 | single_id_no_order | server_process_cpu_ns | 15.269316 | 15.099545 | 15.617999 | -1.112% | -3.320% | +2.284% | lower_than_both |
| before304_return_vs_earlier774 | single_id_no_order | server_process_cpu_ns | 15.269316 | 15.105557 | 15.617999 | -1.072% | -3.281% | +2.284% | lower_than_both |
| candidate_vs_before304 | single_id_no_order | client_process_cpu_ns | 4.812009 | 4.901135 | 4.844860 | +1.852% | +1.162% | +0.683% | higher_than_both |
| candidate_vs_earlier774 | single_id_no_order | client_process_cpu_ns | 4.871014 | 4.901135 | 4.873200 | +0.618% | +0.573% | +0.045% | higher_than_both |
| before304_first_vs_earlier774 | single_id_no_order | client_process_cpu_ns | 4.871014 | 4.812009 | 4.873200 | -1.211% | -1.256% | +0.045% | lower_than_both |
| before304_return_vs_earlier774 | single_id_no_order | client_process_cpu_ns | 4.871014 | 4.844860 | 4.873200 | -0.537% | -0.582% | +0.045% | lower_than_both |

#### 30次首次VACUUM

| 比较 | 操作 | 时钟 | 首测 | 当前 | 回测 | 当前/首测 | 当前/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|---|---|
| candidate_vs_before304 | vacuum-single | wall_ns | 25.607211 | 18.459418 | 19.986439 | -27.913% | -7.640% | -21.950% | lower_than_both |
| candidate_vs_earlier774 | vacuum-single | wall_ns | 18.586620 | 18.459418 | 20.482993 | -0.684% | -9.879% | +10.203% | lower_than_both |
| before304_first_vs_earlier774 | vacuum-single | wall_ns | 18.586620 | 25.607211 | 20.482993 | +37.772% | +25.017% | +10.203% | higher_than_both |
| before304_return_vs_earlier774 | vacuum-single | wall_ns | 18.586620 | 19.986439 | 20.482993 | +7.531% | -2.424% | +10.203% | between_or_equal |
| candidate_vs_before304 | vacuum-single | server_process_cpu_ns | 11.509201 | 10.547032 | 11.430612 | -8.360% | -7.730% | -0.683% | lower_than_both |
| candidate_vs_earlier774 | vacuum-single | server_process_cpu_ns | 10.737185 | 10.547032 | 9.988706 | -1.771% | +5.590% | -6.971% | between_or_equal |
| before304_first_vs_earlier774 | vacuum-single | server_process_cpu_ns | 10.737185 | 11.509201 | 9.988706 | +7.190% | +15.222% | -6.971% | higher_than_both |
| before304_return_vs_earlier774 | vacuum-single | server_process_cpu_ns | 10.737185 | 11.430612 | 9.988706 | +6.458% | +14.435% | -6.971% | higher_than_both |
| candidate_vs_before304 | vacuum-single | client_process_cpu_ns | 0.204081 | 0.213040 | 0.211724 | +4.390% | +0.622% | +3.745% | higher_than_both |
| candidate_vs_earlier774 | vacuum-single | client_process_cpu_ns | 0.199411 | 0.213040 | 0.219357 | +6.835% | -2.880% | +10.002% | between_or_equal |
| before304_first_vs_earlier774 | vacuum-single | client_process_cpu_ns | 0.199411 | 0.204081 | 0.219357 | +2.342% | -6.964% | +10.002% | between_or_equal |
| before304_return_vs_earlier774 | vacuum-single | client_process_cpu_ns | 0.199411 | 0.211724 | 0.219357 | +6.175% | -3.480% | +10.002% | between_or_equal |
| candidate_vs_before304 | vacuum-overflow | wall_ns | 25.062038 | 27.525710 | 21.508917 | +9.830% | +27.973% | -14.177% | higher_than_both |
| candidate_vs_earlier774 | vacuum-overflow | wall_ns | 25.536737 | 27.525710 | 31.286806 | +7.789% | -12.021% | +22.517% | between_or_equal |
| before304_first_vs_earlier774 | vacuum-overflow | wall_ns | 25.536737 | 25.062038 | 31.286806 | -1.859% | -19.896% | +22.517% | lower_than_both |
| before304_return_vs_earlier774 | vacuum-overflow | wall_ns | 25.536737 | 21.508917 | 31.286806 | -15.773% | -31.252% | +22.517% | lower_than_both |
| candidate_vs_before304 | vacuum-overflow | server_process_cpu_ns | 15.374816 | 15.079384 | 11.145824 | -1.922% | +35.292% | -27.506% | between_or_equal |
| candidate_vs_earlier774 | vacuum-overflow | server_process_cpu_ns | 15.473383 | 15.079384 | 17.225761 | -2.546% | -12.460% | +11.325% | lower_than_both |
| before304_first_vs_earlier774 | vacuum-overflow | server_process_cpu_ns | 15.473383 | 15.374816 | 17.225761 | -0.637% | -10.745% | +11.325% | lower_than_both |
| before304_return_vs_earlier774 | vacuum-overflow | server_process_cpu_ns | 15.473383 | 11.145824 | 17.225761 | -27.968% | -35.296% | +11.325% | lower_than_both |
| candidate_vs_before304 | vacuum-overflow | client_process_cpu_ns | 0.214480 | 0.216395 | 0.232744 | +0.893% | -7.024% | +8.515% | between_or_equal |
| candidate_vs_earlier774 | vacuum-overflow | client_process_cpu_ns | 0.202617 | 0.216395 | 0.182957 | +6.800% | +18.276% | -9.703% | higher_than_both |
| before304_first_vs_earlier774 | vacuum-overflow | client_process_cpu_ns | 0.202617 | 0.214480 | 0.182957 | +5.855% | +17.230% | -9.703% | higher_than_both |
| before304_return_vs_earlier774 | vacuum-overflow | client_process_cpu_ns | 0.202617 | 0.232744 | 0.182957 | +14.869% | +27.212% | -9.703% | higher_than_both |

### 原样270操作：全部90项三时钟比较

每角色3次原始样本的中位数，单位毫秒；完整raw3/min/p50/max、270项原始计时、文件和进程快照均在证据中。

#### 墙钟

| 区域 | 旧版 | C1 | 旧版回测 | C1/旧版 | C1/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|
| canonical-fixed-single/fixed/delete16 | 62.375 | 54.676 | 58.340 | -12.34% | -6.28% | -6.47% | lower_than_both |
| canonical-fixed-single/fixed/insert1 | 20.141 | 15.266 | 23.796 | -24.20% | -35.85% | +18.15% | lower_than_both |
| canonical-fixed-single/fixed/reinsert_population | 646.119 | 617.547 | 627.206 | -4.42% | -1.54% | -2.93% | lower_than_both |
| canonical-fixed-single/fixed/select_after_update | 23.901 | 19.670 | 23.646 | -17.71% | -16.82% | -1.07% | lower_than_both |
| canonical-fixed-single/fixed/select_after_vacuum | 25.732 | 22.232 | 24.232 | -13.60% | -8.25% | -5.83% | lower_than_both |
| canonical-fixed-single/fixed/truncate | 262.955 | 208.509 | 208.662 | -20.71% | -0.07% | -20.65% | lower_than_both |
| canonical-fixed-single/fixed/update16 | 79.625 | 77.567 | 88.370 | -2.58% | -12.22% | +10.98% | lower_than_both |
| canonical-fixed-single/fixed/vacuum | 2.491 | 2.025 | 1.967 | -18.69% | +2.93% | -21.01% | between_or_equal |
| canonical-fixed-single/single/delete16 | 81.357 | 77.670 | 82.907 | -4.53% | -6.32% | +1.91% | lower_than_both |
| canonical-fixed-single/single/insert1 | 21.666 | 18.829 | 22.631 | -13.09% | -16.80% | +4.45% | lower_than_both |
| canonical-fixed-single/single/reinsert_population | 1148.082 | 1074.340 | 1179.016 | -6.42% | -8.88% | +2.69% | lower_than_both |
| canonical-fixed-single/single/select_after_update | 50.506 | 37.354 | 48.176 | -26.04% | -22.46% | -4.61% | lower_than_both |
| canonical-fixed-single/single/select_after_vacuum | 46.292 | 46.145 | 50.021 | -0.32% | -7.75% | +8.06% | lower_than_both |
| canonical-fixed-single/single/truncate | 410.600 | 379.301 | 539.502 | -7.62% | -29.69% | +31.39% | lower_than_both |
| canonical-fixed-single/single/update16 | 181.773 | 147.266 | 172.799 | -18.98% | -14.78% | -4.94% | lower_than_both |
| canonical-fixed-single/single/vacuum | 9.218 | 8.222 | 10.457 | -10.81% | -21.37% | +13.43% | lower_than_both |
| canonical-overflow/overflow/delete16 | 82.904 | 78.556 | 86.698 | -5.24% | -9.39% | +4.58% | lower_than_both |
| canonical-overflow/overflow/insert1 | 27.046 | 23.470 | 24.152 | -13.22% | -2.82% | -10.70% | lower_than_both |
| canonical-overflow/overflow/reinsert_population | 1180.841 | 1061.901 | 1138.609 | -10.07% | -6.74% | -3.58% | lower_than_both |
| canonical-overflow/overflow/select_after_update | 49.448 | 46.605 | 42.945 | -5.75% | +8.52% | -13.15% | between_or_equal |
| canonical-overflow/overflow/select_after_vacuum | 52.986 | 47.811 | 48.997 | -9.77% | -2.42% | -7.53% | lower_than_both |
| canonical-overflow/overflow/truncate | 243.416 | 178.674 | 250.824 | -26.60% | -28.77% | +3.04% | lower_than_both |
| canonical-overflow/overflow/update16 | 169.198 | 160.448 | 162.778 | -5.17% | -1.43% | -3.79% | lower_than_both |
| canonical-overflow/overflow/vacuum | 9.962 | 11.630 | 12.381 | +16.75% | -6.07% | +24.29% | between_or_equal |
| direct-hit/fixed/direct_full_no_order | 20.107 | 20.349 | 19.274 | +1.20% | +5.58% | -4.14% | higher_than_both |
| direct-hit/fixed/direct_id_only_no_order | 20.074 | 21.491 | 17.391 | +7.06% | +23.57% | -13.36% | higher_than_both |
| direct-hit/overflow/direct_full_no_order | 53.057 | 42.508 | 40.383 | -19.88% | +5.26% | -23.89% | between_or_equal |
| direct-hit/overflow/direct_id_only_no_order | 18.555 | 19.181 | 17.146 | +3.37% | +11.87% | -7.60% | higher_than_both |
| direct-hit/single/direct_full_no_order | 46.837 | 43.382 | 45.305 | -7.38% | -4.25% | -3.27% | lower_than_both |
| direct-hit/single/direct_id_only_no_order | 20.923 | 21.792 | 22.068 | +4.15% | -1.25% | +5.47% | between_or_equal |

#### 服务器CPU

| 区域 | 旧版 | C1 | 旧版回测 | C1/旧版 | C1/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|
| canonical-fixed-single/fixed/delete16 | 50.706 | 45.594 | 47.430 | -10.08% | -3.87% | -6.46% | lower_than_both |
| canonical-fixed-single/fixed/insert1 | 7.771 | 7.509 | 8.997 | -3.37% | -16.53% | +15.77% | lower_than_both |
| canonical-fixed-single/fixed/reinsert_population | 596.314 | 576.272 | 586.422 | -3.36% | -1.73% | -1.66% | lower_than_both |
| canonical-fixed-single/fixed/select_after_update | 10.715 | 11.643 | 15.402 | +8.66% | -24.41% | +43.74% | between_or_equal |
| canonical-fixed-single/fixed/select_after_vacuum | 15.822 | 14.949 | 17.213 | -5.52% | -13.15% | +8.79% | lower_than_both |
| canonical-fixed-single/fixed/truncate | 104.235 | 95.093 | 104.478 | -8.77% | -8.98% | +0.23% | lower_than_both |
| canonical-fixed-single/fixed/update16 | 67.252 | 65.791 | 74.965 | -2.17% | -12.24% | +11.47% | lower_than_both |
| canonical-fixed-single/fixed/vacuum | 2.354 | 1.894 | 1.834 | -19.56% | +3.25% | -22.09% | between_or_equal |
| canonical-fixed-single/single/delete16 | 69.357 | 69.675 | 70.438 | +0.46% | -1.08% | +1.56% | between_or_equal |
| canonical-fixed-single/single/insert1 | 8.723 | 10.559 | 9.311 | +21.04% | +13.40% | +6.74% | higher_than_both |
| canonical-fixed-single/single/reinsert_population | 1055.634 | 1015.268 | 1060.756 | -3.82% | -4.29% | +0.49% | lower_than_both |
| canonical-fixed-single/single/select_after_update | 41.726 | 28.700 | 40.854 | -31.22% | -29.75% | -2.09% | lower_than_both |
| canonical-fixed-single/single/select_after_vacuum | 39.453 | 40.054 | 40.839 | +1.52% | -1.92% | +3.51% | between_or_equal |
| canonical-fixed-single/single/truncate | 133.505 | 115.145 | 135.355 | -13.75% | -14.93% | +1.39% | lower_than_both |
| canonical-fixed-single/single/update16 | 132.241 | 126.612 | 134.092 | -4.26% | -5.58% | +1.40% | lower_than_both |
| canonical-fixed-single/single/vacuum | 8.913 | 7.963 | 10.160 | -10.65% | -21.62% | +13.99% | lower_than_both |
| canonical-overflow/overflow/delete16 | 71.477 | 68.993 | 73.424 | -3.47% | -6.03% | +2.73% | lower_than_both |
| canonical-overflow/overflow/insert1 | 16.662 | 11.159 | 13.617 | -33.03% | -18.05% | -18.27% | lower_than_both |
| canonical-overflow/overflow/reinsert_population | 1092.691 | 1025.687 | 1100.864 | -6.13% | -6.83% | +0.75% | lower_than_both |
| canonical-overflow/overflow/select_after_update | 42.422 | 40.577 | 36.948 | -4.35% | +9.82% | -12.90% | between_or_equal |
| canonical-overflow/overflow/select_after_vacuum | 43.365 | 42.801 | 42.648 | -1.30% | +0.36% | -1.65% | between_or_equal |
| canonical-overflow/overflow/truncate | 90.589 | 66.712 | 71.855 | -26.36% | -7.16% | -20.68% | lower_than_both |
| canonical-overflow/overflow/update16 | 142.041 | 143.152 | 137.732 | +0.78% | +3.94% | -3.03% | higher_than_both |
| canonical-overflow/overflow/vacuum | 9.857 | 11.407 | 12.037 | +15.72% | -5.24% | +22.12% | between_or_equal |
| direct-hit/fixed/direct_full_no_order | 9.563 | 11.069 | 12.072 | +15.75% | -8.31% | +26.24% | between_or_equal |
| direct-hit/fixed/direct_id_only_no_order | 7.491 | 10.852 | 10.036 | +44.86% | +8.13% | +33.97% | higher_than_both |
| direct-hit/overflow/direct_full_no_order | 44.673 | 34.855 | 33.737 | -21.98% | +3.31% | -24.48% | between_or_equal |
| direct-hit/overflow/direct_id_only_no_order | 12.293 | 12.482 | 12.085 | +1.54% | +3.29% | -1.69% | higher_than_both |
| direct-hit/single/direct_full_no_order | 40.245 | 38.832 | 40.262 | -3.51% | -3.55% | +0.04% | lower_than_both |
| direct-hit/single/direct_id_only_no_order | 14.981 | 15.332 | 16.382 | +2.34% | -6.41% | +9.36% | between_or_equal |

#### 客户端CPU

| 区域 | 旧版 | C1 | 旧版回测 | C1/旧版 | C1/回测 | 旧漂移 | 分类 |
|---|---|---|---|---|---|---|---|
| canonical-fixed-single/fixed/delete16 | 0.211 | 0.227 | 0.217 | +7.41% | +4.20% | +3.08% | higher_than_both |
| canonical-fixed-single/fixed/insert1 | 0.198 | 0.203 | 0.232 | +2.87% | -12.27% | +17.25% | between_or_equal |
| canonical-fixed-single/fixed/reinsert_population | 0.229 | 0.217 | 0.224 | -5.30% | -2.83% | -2.54% | lower_than_both |
| canonical-fixed-single/fixed/select_after_update | 4.906 | 4.876 | 4.933 | -0.61% | -1.17% | +0.56% | lower_than_both |
| canonical-fixed-single/fixed/select_after_vacuum | 4.472 | 4.587 | 4.671 | +2.57% | -1.81% | +4.47% | between_or_equal |
| canonical-fixed-single/fixed/truncate | 0.230 | 0.231 | 0.237 | +0.25% | -2.64% | +2.96% | between_or_equal |
| canonical-fixed-single/fixed/update16 | 0.211 | 0.229 | 0.212 | +8.86% | +8.41% | +0.41% | higher_than_both |
| canonical-fixed-single/fixed/vacuum | 0.214 | 0.203 | 0.205 | -5.20% | -1.07% | -4.18% | lower_than_both |
| canonical-fixed-single/single/delete16 | 0.242 | 0.220 | 0.224 | -9.00% | -1.87% | -7.27% | lower_than_both |
| canonical-fixed-single/single/insert1 | 0.210 | 0.211 | 0.230 | +0.72% | -8.15% | +9.65% | between_or_equal |
| canonical-fixed-single/single/reinsert_population | 0.647 | 0.663 | 0.817 | +2.60% | -18.76% | +26.30% | between_or_equal |
| canonical-fixed-single/single/select_after_update | 5.858 | 5.845 | 6.089 | -0.22% | -4.00% | +3.94% | lower_than_both |
| canonical-fixed-single/single/select_after_vacuum | 5.466 | 6.520 | 5.660 | +19.29% | +15.20% | +3.54% | higher_than_both |
| canonical-fixed-single/single/truncate | 0.231 | 0.275 | 0.260 | +19.04% | +5.76% | +12.55% | higher_than_both |
| canonical-fixed-single/single/update16 | 0.256 | 0.229 | 0.249 | -10.69% | -8.17% | -2.75% | lower_than_both |
| canonical-fixed-single/single/vacuum | 0.206 | 0.233 | 0.217 | +13.33% | +7.42% | +5.50% | higher_than_both |
| canonical-overflow/overflow/delete16 | 0.237 | 0.243 | 0.234 | +2.58% | +3.94% | -1.30% | higher_than_both |
| canonical-overflow/overflow/insert1 | 0.192 | 0.205 | 0.188 | +6.88% | +9.19% | -2.11% | higher_than_both |
| canonical-overflow/overflow/reinsert_population | 0.967 | 0.802 | 0.754 | -17.00% | +6.46% | -22.03% | between_or_equal |
| canonical-overflow/overflow/select_after_update | 4.397 | 4.221 | 4.158 | -4.01% | +1.51% | -5.44% | between_or_equal |
| canonical-overflow/overflow/select_after_vacuum | 3.530 | 3.536 | 3.729 | +0.18% | -5.18% | +5.66% | between_or_equal |
| canonical-overflow/overflow/truncate | 0.234 | 0.229 | 0.233 | -2.25% | -1.79% | -0.47% | lower_than_both |
| canonical-overflow/overflow/update16 | 0.252 | 0.229 | 0.202 | -8.98% | +13.29% | -19.66% | between_or_equal |
| canonical-overflow/overflow/vacuum | 0.230 | 0.207 | 0.219 | -10.11% | -5.80% | -4.57% | lower_than_both |
| direct-hit/fixed/direct_full_no_order | 4.844 | 4.882 | 4.944 | +0.79% | -1.25% | +2.06% | between_or_equal |
| direct-hit/fixed/direct_id_only_no_order | 4.834 | 4.528 | 4.879 | -6.34% | -7.20% | +0.92% | lower_than_both |
| direct-hit/overflow/direct_full_no_order | 4.331 | 4.154 | 4.027 | -4.09% | +3.15% | -7.03% | between_or_equal |
| direct-hit/overflow/direct_id_only_no_order | 2.026 | 2.086 | 2.032 | +2.95% | +2.62% | +0.32% | higher_than_both |
| direct-hit/single/direct_full_no_order | 5.809 | 5.615 | 5.828 | -3.33% | -3.65% | +0.33% | lower_than_both |
| direct-hit/single/direct_id_only_no_order | 3.552 | 3.764 | 3.683 | +5.98% | +2.21% | +3.69% | higher_than_both |

### ID查询的独立采样诊断

所有正常窗口关闭后，另做旧304 → C1 → 同旧304的3个CPU8／libprofiler-only1000Hz窗口，每窗口512次ID查询；共1536次采样查询／1569完整回复、400161行、412515单元、1230NULL、14321970字节，3个Main正常0，24个TID快照。三份原始profile分别8758／9348／8685样本，按真实ELF/maps和有长度符号范围解释，未推测stripped共享库函数名。C1中的Project::next含子调用17.70%、物理HeapScanCursor::next13.29%、旧式log14.28%、CatalogManager::publicationState7.34%、投影解码1.94%、文件身份校验0.18%。这些调用子树重叠，不能相加，也不能证明正常窗口回退原因。未采样到某符号不等于零开销。投影固定字段已调用静态取值器，未做推测性的wrapper优化；未引入日志缓冲、关闭日志或allocator缓存。

全部试验失败原样保留：v1因不存在的makeByteaColumn编译actual1；v2/v3用70003字符TEXT建原生数据时拒绝，目标情形NOTRUN；v4较大原生列声明在首行遇IO_ERROR、回滚未完成，退出actual -6／double free，原因OPEN，未调查或修改用户暂缓模块；v5真实SQL声明下原生写TEXT70003仍拒绝。v6的前8个“绑定”控制实际未绑定，不能作为NULL安全证据，后续SQL子分区DDL为0A000并停止。v7改用真实正RID后普通表复现失败，但逻辑分区读取的初始全值前置检查失败，余8种情形NOTRUN；未将其改称通过。v8调用私有路径／行头接口而编译actual1。最终v9改用受控默认路径与既有完整物理解码器作为独立oracle，才完整执行16种情形并形成160／48旧版红测；全部初稿源码、编译／运行终态和首错均留存。

完整证据压缩为[vacuum-physical-references-20261107.json.gz](benchmarks/vacuum-physical-references-20261107.json.gz)，34,381,025字节（32.79MiB），SHA256 `0b4576a981b223747eb97287378931d41e79bbdc27c2ae998211b8149ae01c1e`；内层完整证据JSON为2,675,881,552字节。51,608个非ELF文件、2,014个目录、2,934个内容块和221个本地ELF引用已在清理前后独立逐字节核对。删除48,458个已归档生成副本，逻辑文件量4,625,982,422字节（4.308GiB），不等同于独占文件系统释放量。保留全部源码、ELF、原始PC采样、任务stdout/stderr和错误记录、目录、新after-DELETE实际冷开种子及外部P36 after-UPDATE实际种子；生成数据目录中的dbms.log副本随数据压缩保存。旧37个压缩包保持。

用户指出的analyze-reindex-main-costs-20261104原始JSON为56,047,459字节（53.45MiB）；当前版本已使用5,977,159字节（5.70MiB）的.json.gz保存，原始SHA与逐字节解压核对保存在manifest，当前Git不跟踪原始大JSON，忽略规则阻止未来同目录原始JSON误加入（小manifest除外）。该证据用于复核原始耗时与错误，保留压缩内容；未重写旧提交历史。

可逐字节提取三组完整raw3/min/p50/max表（正常270、1200查询、30次首次VACUUM；完整raw20与尾部同时在对应独立摘要中）：

```sh
python3 - <<'PY'
import base64, gzip, hashlib, json, lzma, pathlib, sys
e = json.loads(gzip.decompress(pathlib.Path('docs/benchmarks/vacuum-physical-references-20261107.json.gz').read_bytes()))
raw = lzma.decompress(base64.b64decode(e['payload_xz_base64'], validate=True))
assert hashlib.sha256(raw).hexdigest() == e['payload_raw_sha256']
b = json.loads(raw)
names = [
    'batch/vacuum-physical-reference-cost27-C1-v1/all-performance-tables-v3.md',
    'batch/matched-reads5-C1-v1/all-performance-tables-v1.md',
    'batch/matched-vacuum5-C1-v1/all-performance-tables-v1.md',
]
for name in names:
    f = b['artifacts'][name]
    v = base64.b64decode(b['blobs'][f['blob']], validate=True)
    assert hashlib.sha256(v).hexdigest() == f['sha256']
    sys.stdout.buffer.write(v)
PY
```

正式库存2398个测试文件（1131顶层／1136总C++），注册E2E684／独立E2E664不变。754个Markdown路径和725份保护记录保持，8份既有文档仅插入本批说明。原273项及22 complete／166 partial／70 unverified／15 deferred_by_user不变，历史增至287，IDX-07仍unverified/evidence[]。

本批只关闭物理TOAST引用识别这一有限数据安全问题，功能总账仍partial。旧P35/P36负向性能矩阵、当前ID与多长字段VACUUM回退、既有TOAST断言及PG协议／新SQL差异继续OPEN。执行图／child-provider生命周期、retained planning／portal、CPU-sort interrupt及通用故障矩阵等用户暂缓范围不变。Goal active，直接master分批提交，未推送或替换默认程序；正常CMake C++17/O2候选SHA前缀56aad0c0，旧版30412452，更早对照77418e9c，默认425165af保持。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-20261106"></a>
## 2026-11-06：物理行 NULL 位图复用及完整性能对照（P36-C1）

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

原样 270 操作矩阵的 30 个区域，墙钟 19 低于两旧／2 高于两旧／9 居间，服务器 CPU 为 12／9／9，客户端 CPU 为 9／16／5。单长字段 VACUUM 墙钟高约 22.90%／28.87%，多长字段 VACUUM 高约 19.10%／4.85%；另有 9 个区域服务器 CPU 高于两旧，全部保留，原因仍 OPEN。

扫描展开长字段时，借用该物理行已捕获的 NULL 位图；排序后的投影在核对真实来源、具体 Sort 类型和全部物理列声明后，借用排序已保存的位图。借用仅存在于一次取行调用内，退出与异常均恢复原绑定；表结构不匹配、Sort 子类及其他来源继续原路径。没有新增页／allocator 缓存或改变文件身份检查本身。

设计依据：[固定 PostgreSQL18 源码 execTuples.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/executor/execTuples.c)让 tuple slot 保存已解码值与 NULL 状态，并按需求增量展开；[SQLite 字节码文档](https://www.sqlite.org/opcode.html)的 OpenPseudo/Column 从排序输出记录中取列；[DuckDB Vector 文档](https://duckdb.org/docs/current/clients/c/vector)让列数据携带对应 validity mask。本项目据此复用已有行元数据，仍保留现有逐行执行模型。三个官方 HTTP200 原文及响应元数据已存档，未借这些机制推导三库速度排名。

新增 sorted_projection_null_bitmap_cost_test 在相同冻结测试源码上，旧版 1156 检查／8 失败／exit1，C1 1156／0／exit0；每 64 行排序后投影的文件校验从 353 降到 0，两个回退控制仍为 233／353。覆盖内存与真实落盘排序、升降序、NULL/空串/文本 NULL、UTF-8、二进制、长字段、过滤来源、schema 不匹配、子类虚调用、LIMIT 与虚拟列错误。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

原 62 条 SQL 在旧版与 C1 全部重新运行，初开／冷开 4 个 Main 正常0，完整请求／回复逐字节一致；全部 T7、值／NULL、SQLSTATE、诊断、tag 与 Ready 保留。既有 9 条 PostgreSQL 差异仍在，strict PG=false；沿用并核对真实 PostgreSQL18.6 的固定原始 oracle，没有运行 PG 速度对比。

定位阶段先在同一真实 P35 三表闭库种子上执行 720 次普通查询：旧73 → 当前774 → 旧73，9 上下文／18 Main 正常0，828 回复、179388 行、605601 单元、42435 NULL、304584957 请求／回复字节。原多长字段全列的大幅退化没有在这个固定状态中重现；单长字段排序 CPU 仍高约 11.09%／3.42%。该数据为 after-UPDATE 人口，不能替换 P35 原 after-VACUUM 测量或宣称此前负例已修复。

随后单独执行 CPU8、libprofiler-only1000Hz 的 12 个诊断窗口：1536 次采样查询／1587 完整回复、354867 行、1170231 单元、81180 NULL、582842892 字节，3 个 Main 正常0、96 个 TID 快照。原始 PC 按真实 ELF/maps 与有长度符号范围解释，未猜测 stripped DSO 函数名。当前774的单长字段排序窗口中，Project::next 含子调用约25.46%、Sort::open46.49%、storedRowColumnIsNull34.22%、getPageAllocator33.59%、文件身份检查32.71%；这些调用子树重叠，不能相加，亦不能代替正常耗时或证明旧版差异的原因。实际位图复用分支另由上面的真实文件校验计数验证。

正式普通矩阵使用旧 P35-C4 `77418e9c` → 候选 C1 `30412452` → 同旧回测：原样 27 个全新数据库、54 个正常 Main、270 计时点、1098 回复、96624 行、386388 单元、27864 NULL、231410943 字节，5796 个闭库文件均核对。固定／单长字段／多长字段的 INSERT1、UPDATE16、全列 SELECT、DELETE16、VACUUM、全列 SELECT、TRUNCATE、重新写入，以及另列的全列／仅 ID 无排序查询全部保留。

新的固定状态查询对照再次完整运行 9 个上下文、18 个正常 Main、720 计时点、828 回复与相同完整值总量。每个上下文每条查询连续20次，先求20次中位数，再汇总3个上下文的 min/p50/max；首轮、热态及尾部均保留。两套正式对照共990个计时操作／72个正常Main／1926回复。所有正常窗口均无本任务编译、采样器、压缩、CPU绑定或其他 SQL；自然缓存，未宣称宿主机空闲。服务器 CPU 是整个 Main 进程，包含后台线程；不是请求线程的独占 CPU。

### 固定状态720次查询：三上下文中位数（毫秒）

| 查询 | 时钟 | 旧版 | C1 | 旧版回测 | C1/旧版 | C1/回测 |
|---|---|---:|---:|---:|---:|---:|
| fixed_full_ordered | wall_ns | 48.897203 | 35.791450 | 50.461500 | -26.803% | -29.072% |
| fixed_full_ordered | server_process_cpu_ns | 30.241319 | 18.024002 | 29.992283 | -40.399% | -39.905% |
| fixed_full_ordered | client_process_cpu_ns | 4.853256 | 4.915216 | 4.956652 | +1.277% | -0.836% |
| single_full_ordered | wall_ns | 77.067546 | 59.098637 | 77.606820 | -23.316% | -23.849% |
| single_full_ordered | server_process_cpu_ns | 59.193275 | 42.047863 | 59.877958 | -28.965% | -29.777% |
| single_full_ordered | client_process_cpu_ns | 6.092505 | 6.181285 | 5.817166 | +1.457% | +6.259% |
| overflow_full_no_order | wall_ns | 73.198169 | 65.559572 | 73.809732 | -10.436% | -11.178% |
| overflow_full_no_order | server_process_cpu_ns | 53.462404 | 48.582261 | 53.899868 | -9.128% | -9.866% |
| overflow_full_no_order | client_process_cpu_ns | 4.157331 | 4.162324 | 4.229456 | +0.120% | -1.587% |
| single_id_no_order | wall_ns | 34.132010 | 33.406312 | 32.743275 | -2.126% | +2.025% |
| single_id_no_order | server_process_cpu_ns | 15.395940 | 17.300562 | 16.476689 | +12.371% | +5.000% |
| single_id_no_order | client_process_cpu_ns | 4.888833 | 4.877959 | 4.867878 | -0.222% | +0.207% |

### 原样270操作：全部30区域、三时钟中位数

下表均为各角色3次原始样本的中位数，单位毫秒；两个百分比分别对旧版和旧版回测，漂移为回测相对旧版。所有 raw3/min/p50/max、完整270项时钟括号、最长尾部与负例在压缩证据内，未剔除样本。

#### 墙钟

| 区域 | 旧版 | C1 | 旧版回测 | C1/旧版 | C1/回测 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 81.951 | 78.315 | 74.051 | -4.44% | +5.76% | -9.64% |
| canonical-fixed-single/fixed/insert1 | 42.295 | 33.903 | 42.006 | -19.84% | -19.29% | -0.68% |
| canonical-fixed-single/fixed/reinsert_population | 715.393 | 669.824 | 720.327 | -6.37% | -7.01% | +0.69% |
| canonical-fixed-single/fixed/select_after_update | 51.398 | 38.915 | 49.903 | -24.29% | -22.02% | -2.91% |
| canonical-fixed-single/fixed/select_after_vacuum | 44.547 | 32.630 | 44.514 | -26.75% | -26.70% | -0.08% |
| canonical-fixed-single/fixed/truncate | 360.099 | 341.642 | 341.393 | -5.13% | +0.07% | -5.19% |
| canonical-fixed-single/fixed/update16 | 118.403 | 103.428 | 112.625 | -12.65% | -8.17% | -4.88% |
| canonical-fixed-single/fixed/vacuum | 2.547 | 2.485 | 2.262 | -2.42% | +9.86% | -11.18% |
| canonical-fixed-single/single/delete16 | 99.523 | 95.932 | 100.216 | -3.61% | -4.27% | +0.70% |
| canonical-fixed-single/single/insert1 | 39.689 | 34.829 | 43.667 | -12.24% | -20.24% | +10.02% |
| canonical-fixed-single/single/reinsert_population | 1227.538 | 1201.412 | 1236.122 | -2.13% | -2.81% | +0.70% |
| canonical-fixed-single/single/select_after_update | 81.043 | 59.053 | 78.143 | -27.13% | -24.43% | -3.58% |
| canonical-fixed-single/single/select_after_vacuum | 71.921 | 62.165 | 80.311 | -13.56% | -22.59% | +11.67% |
| canonical-fixed-single/single/truncate | 542.789 | 542.469 | 548.314 | -0.06% | -1.07% | +1.02% |
| canonical-fixed-single/single/update16 | 202.008 | 200.940 | 200.127 | -0.53% | +0.41% | -0.93% |
| canonical-fixed-single/single/vacuum | 9.780 | 12.020 | 9.327 | +22.90% | +28.87% | -4.63% |
| canonical-overflow/overflow/delete16 | 97.942 | 96.431 | 94.252 | -1.54% | +2.31% | -3.77% |
| canonical-overflow/overflow/insert1 | 43.909 | 42.637 | 38.728 | -2.90% | +10.09% | -11.80% |
| canonical-overflow/overflow/reinsert_population | 1228.176 | 1202.718 | 1221.517 | -2.07% | -1.54% | -0.54% |
| canonical-overflow/overflow/select_after_update | 79.017 | 60.161 | 82.217 | -23.86% | -26.83% | +4.05% |
| canonical-overflow/overflow/select_after_vacuum | 78.305 | 63.918 | 77.374 | -18.37% | -17.39% | -1.19% |
| canonical-overflow/overflow/truncate | 342.121 | 329.838 | 337.775 | -3.59% | -2.35% | -1.27% |
| canonical-overflow/overflow/update16 | 199.895 | 205.552 | 213.276 | +2.83% | -3.62% | +6.69% |
| canonical-overflow/overflow/vacuum | 12.476 | 14.859 | 14.172 | +19.11% | +4.85% | +13.59% |
| direct-hit/fixed/direct_full_no_order | 35.187 | 35.078 | 31.650 | -0.31% | +10.83% | -10.05% |
| direct-hit/fixed/direct_id_only_no_order | 29.254 | 30.324 | 36.177 | +3.66% | -16.18% | +23.66% |
| direct-hit/overflow/direct_full_no_order | 72.564 | 63.133 | 66.128 | -13.00% | -4.53% | -8.87% |
| direct-hit/overflow/direct_id_only_no_order | 28.680 | 25.766 | 28.800 | -10.16% | -10.53% | +0.42% |
| direct-hit/single/direct_full_no_order | 62.607 | 56.984 | 60.331 | -8.98% | -5.55% | -3.64% |
| direct-hit/single/direct_id_only_no_order | 35.422 | 31.691 | 33.867 | -10.53% | -6.43% | -4.39% |

#### 服务器 CPU

| 区域 | 旧版 | C1 | 旧版回测 | C1/旧版 | C1/回测 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 56.128 | 51.033 | 52.744 | -9.08% | -3.24% | -6.03% |
| canonical-fixed-single/fixed/insert1 | 14.245 | 8.499 | 14.961 | -40.33% | -43.19% | +5.03% |
| canonical-fixed-single/fixed/reinsert_population | 598.212 | 592.905 | 591.064 | -0.89% | +0.31% | -1.19% |
| canonical-fixed-single/fixed/select_after_update | 30.861 | 15.870 | 30.935 | -48.58% | -48.70% | +0.24% |
| canonical-fixed-single/fixed/select_after_vacuum | 30.547 | 9.595 | 28.277 | -68.59% | -66.07% | -7.43% |
| canonical-fixed-single/fixed/truncate | 104.386 | 116.267 | 112.366 | +11.38% | +3.47% | +7.64% |
| canonical-fixed-single/fixed/update16 | 78.923 | 79.319 | 80.715 | +0.50% | -1.73% | +2.27% |
| canonical-fixed-single/fixed/vacuum | 2.414 | 2.350 | 2.153 | -2.66% | +9.14% | -10.81% |
| canonical-fixed-single/single/delete16 | 69.863 | 72.659 | 69.113 | +4.00% | +5.13% | -1.07% |
| canonical-fixed-single/single/insert1 | 13.565 | 11.490 | 15.399 | -15.30% | -25.39% | +13.52% |
| canonical-fixed-single/single/reinsert_population | 1043.398 | 1060.506 | 1111.719 | +1.64% | -4.61% | +6.55% |
| canonical-fixed-single/single/select_after_update | 60.796 | 41.370 | 62.061 | -31.95% | -33.34% | +2.08% |
| canonical-fixed-single/single/select_after_vacuum | 56.173 | 43.958 | 58.699 | -21.74% | -25.11% | +4.50% |
| canonical-fixed-single/single/truncate | 119.344 | 150.097 | 119.901 | +25.77% | +25.18% | +0.47% |
| canonical-fixed-single/single/update16 | 138.997 | 142.686 | 140.812 | +2.65% | +1.33% | +1.31% |
| canonical-fixed-single/single/vacuum | 9.482 | 11.119 | 9.022 | +17.27% | +23.25% | -4.85% |
| canonical-overflow/overflow/delete16 | 72.282 | 71.560 | 71.453 | -1.00% | +0.15% | -1.15% |
| canonical-overflow/overflow/insert1 | 12.144 | 17.990 | 13.027 | +48.14% | +38.11% | +7.26% |
| canonical-overflow/overflow/reinsert_population | 1100.141 | 1068.883 | 1114.268 | -2.84% | -4.07% | +1.28% |
| canonical-overflow/overflow/select_after_update | 60.181 | 45.437 | 62.323 | -24.50% | -27.09% | +3.56% |
| canonical-overflow/overflow/select_after_vacuum | 59.624 | 45.756 | 59.753 | -23.26% | -23.42% | +0.22% |
| canonical-overflow/overflow/truncate | 94.325 | 97.081 | 93.706 | +2.92% | +3.60% | -0.66% |
| canonical-overflow/overflow/update16 | 143.213 | 152.466 | 158.014 | +6.46% | -3.51% | +10.34% |
| canonical-overflow/overflow/vacuum | 11.652 | 13.474 | 13.866 | +15.64% | -2.83% | +19.01% |
| direct-hit/fixed/direct_full_no_order | 14.092 | 16.911 | 16.817 | +20.00% | +0.56% | +19.33% |
| direct-hit/fixed/direct_id_only_no_order | 13.074 | 14.143 | 14.736 | +8.18% | -4.02% | +12.71% |
| direct-hit/overflow/direct_full_no_order | 50.146 | 45.628 | 50.300 | -9.01% | -9.29% | +0.31% |
| direct-hit/overflow/direct_id_only_no_order | 12.410 | 11.637 | 11.422 | -6.23% | +1.88% | -7.96% |
| direct-hit/single/direct_full_no_order | 45.470 | 39.830 | 46.335 | -12.40% | -14.04% | +1.90% |
| direct-hit/single/direct_id_only_no_order | 14.708 | 18.009 | 15.875 | +22.44% | +13.44% | +7.93% |

#### 客户端 CPU

| 区域 | 旧版 | C1 | 旧版回测 | C1/旧版 | C1/回测 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 0.249 | 0.199 | 0.251 | -20.12% | -20.64% | +0.66% |
| canonical-fixed-single/fixed/insert1 | 0.217 | 0.232 | 0.218 | +6.59% | +6.46% | +0.12% |
| canonical-fixed-single/fixed/reinsert_population | 0.262 | 0.238 | 0.265 | -9.10% | -10.04% | +1.05% |
| canonical-fixed-single/fixed/select_after_update | 5.020 | 5.025 | 5.013 | +0.10% | +0.23% | -0.13% |
| canonical-fixed-single/fixed/select_after_vacuum | 4.719 | 4.666 | 4.758 | -1.12% | -1.92% | +0.82% |
| canonical-fixed-single/fixed/truncate | 0.205 | 0.223 | 0.246 | +8.50% | -9.53% | +19.93% |
| canonical-fixed-single/fixed/update16 | 0.223 | 0.243 | 0.245 | +9.02% | -0.88% | +9.98% |
| canonical-fixed-single/fixed/vacuum | 0.210 | 0.152 | 0.218 | -27.80% | -30.33% | +3.64% |
| canonical-fixed-single/single/delete16 | 0.235 | 0.231 | 0.252 | -1.73% | -8.45% | +7.34% |
| canonical-fixed-single/single/insert1 | 0.222 | 0.209 | 0.244 | -6.04% | -14.63% | +10.07% |
| canonical-fixed-single/single/reinsert_population | 0.561 | 0.605 | 0.578 | +7.85% | +4.78% | +2.93% |
| canonical-fixed-single/single/select_after_update | 6.160 | 6.182 | 6.066 | +0.37% | +1.92% | -1.52% |
| canonical-fixed-single/single/select_after_vacuum | 5.299 | 6.367 | 5.887 | +20.16% | +8.15% | +11.10% |
| canonical-fixed-single/single/truncate | 0.249 | 0.234 | 0.223 | -6.11% | +5.07% | -10.64% |
| canonical-fixed-single/single/update16 | 0.259 | 0.246 | 0.263 | -4.96% | -6.25% | +1.38% |
| canonical-fixed-single/single/vacuum | 0.196 | 0.251 | 0.202 | +28.19% | +24.32% | +3.11% |
| canonical-overflow/overflow/delete16 | 0.251 | 0.265 | 0.229 | +5.36% | +15.70% | -8.94% |
| canonical-overflow/overflow/insert1 | 0.238 | 0.231 | 0.241 | -2.78% | -4.15% | +1.43% |
| canonical-overflow/overflow/reinsert_population | 0.773 | 0.829 | 0.814 | +7.24% | +1.84% | +5.30% |
| canonical-overflow/overflow/select_after_update | 4.354 | 4.294 | 4.450 | -1.37% | -3.49% | +2.20% |
| canonical-overflow/overflow/select_after_vacuum | 3.893 | 4.019 | 4.014 | +3.25% | +0.12% | +3.12% |
| canonical-overflow/overflow/truncate | 0.222 | 0.238 | 0.223 | +7.19% | +6.99% | +0.18% |
| canonical-overflow/overflow/update16 | 0.266 | 0.277 | 0.260 | +4.08% | +6.38% | -2.17% |
| canonical-overflow/overflow/vacuum | 0.216 | 0.225 | 0.229 | +3.89% | -1.76% | +5.76% |
| direct-hit/fixed/direct_full_no_order | 4.940 | 5.016 | 4.858 | +1.54% | +3.25% | -1.66% |
| direct-hit/fixed/direct_id_only_no_order | 4.827 | 4.920 | 5.661 | +1.94% | -13.08% | +17.29% |
| direct-hit/overflow/direct_full_no_order | 3.961 | 4.386 | 4.075 | +10.72% | +7.62% | +2.88% |
| direct-hit/overflow/direct_id_only_no_order | 2.051 | 2.132 | 1.878 | +3.94% | +13.49% | -8.42% |
| direct-hit/single/direct_full_no_order | 5.484 | 6.289 | 6.162 | +14.68% | +2.07% | +12.36% |
| direct-hit/single/direct_id_only_no_order | 4.167 | 4.590 | 3.893 | +10.15% | +17.89% | -6.56% |

旧版墙钟漂移 -11.80%～+23.66%，服务器 CPU -10.81%～+19.33%，客户端 CPU -10.64%～+19.93%。单长字段 TRUNCATE CPU 仍高约25.77%／25.18%，多长字段 INSERT1 CPU 高约48.14%／38.11%；绝对耗时见上表，不能用查询收益抵消这些负例。只选 ID 的新固定状态 CPU 回退也未关闭。

完整失败记录包括：最初测试源码误把 StorageCodec 枚举当指针，编译 actual1；原始编译日志保留，初稿由已记录补丁重建并明确标注，未伪称独立原始快照。初次独立归约 v1 有重复 case 参数，v2 在完整摘要写入后显示时发生变量遮蔽；后续 v3 仅离线核对全部原始828回复并正常完成，没有重采 SQL。旧 TOAST 断言导致的首次控制器停止及仅未启动项的继续均保留。

完整证据压缩为 [physical-row-null-reuse-20261106.json.gz](benchmarks/physical-row-null-reuse-20261106.json.gz)：31,777,550 字节（30.31 MiB），SHA256 `28613515ced123a2f04161749f67e7f0b898e0cf18f7c7dca42a9458121f672d`；内层完整证据 JSON 为 2,933,021,207 字节。38,167 个非 ELF 文件、1,449 个目录、2,285 个内容块及 152 个本地 ELF 引用均独立重建核对。随后删除 34,702 个已备份生成文件，逻辑副本 5.247 GiB；这不是独占文件系统释放量。保留源码、可执行／对象文件、本地 ELF 引用、stdout/stderr 与错误记录、目录和一个最终正常关闭的三表工作数据集；生成数据目录中的运行日志随其副本压缩保存，旧 36 个压缩包保持。

可从压缩证据中逐字节提取完整 raw3/min/p50/max 与尾部表：

```sh
python3 - <<'PY'
import base64, gzip, hashlib, json, lzma, pathlib, sys
e = json.loads(gzip.decompress(pathlib.Path('docs/benchmarks/physical-row-null-reuse-20261106.json.gz').read_bytes()))
raw = lzma.decompress(base64.b64decode(e['payload_xz_base64'], validate=True))
assert hashlib.sha256(raw).hexdigest() == e['payload_raw_sha256']
b = json.loads(raw)
f = b['artifacts']['batch/physical-null-bitmap-cost27-C1-v1/all-performance-tables-v3.md']
v = base64.b64decode(b['blobs'][f['blob']], validate=True)
assert hashlib.sha256(v).hexdigest() == f['sha256']
sys.stdout.buffer.write(v)
PY
```

正式库存 2397 个测试文件（1130 顶层／1135 总 C++），注册 E2E 684／独立 E2E 664 不变；754 个 Markdown 路径与 725 份保护记录保持。原 273 项及 22 complete／166 partial／70 unverified／15 deferred_by_user 不变，历史增至 286，IDX-07 仍 unverified/evidence[]。

本批只接受普通物理扫描／排序投影的有限优化，未关闭 PostgreSQL 功能家族或整体性能目标。P35 原始负向矩阵、ID CPU 与本批 VACUUM 等负例继续有效。用户暂缓的执行图／child-provider、retained planning／portal、CPU-sort interrupt 和通用故障矩阵等范围保持。Goal active，直接 master 分批提交；正常 CMake C++17/O2 候选 SHA 前缀 30412452，默认程序 SHA425165af 保持，未推送。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
<a id="view-column-dependencies-20261106"></a>
## 2026-11-06：视图列依赖保护与真实开销（P35-C4）

本轮没有接受整体提速结论。30 个普通操作区域中，墙钟中位数 5 低于两旧／19 高于两旧／6 居间，服务器 CPU 为 6／16／8，客户端 CPU 为 5／16／9。旧版墙钟漂移 -28.19%～+27.35%，服务器 CPU -26.58%～+29.13%。查询执行路径源码未改，负向原因尚未隔离，继续 OPEN。

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

设计参考：[PostgreSQL 18 依赖追踪](https://www.postgresql.org/docs/18/ddl-depend.html)与[列删除规则](https://www.postgresql.org/docs/18/sql-altertable.html)要求 RESTRICT 保护依赖、CASCADE 递归处理依赖对象；[固定 PG18 源码 dependency.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/catalog/dependency.c)从解析树的真实关系／列身份收集引用。这里沿用本项目已有 pg_depend 列号格式，直接记录视图身份，未实现 PG 的完整 rewrite-rule 体系。[SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html)通过修改后重解析 schema 检查引用；[DuckDB DROP 文档](https://www.duckdb.org/docs/stable/sql/statements/drop)明确说明其视图依赖追踪限制。这些是机制与边界参考，没有运行三库速度对比。

新视图的星号只记录创建时的展开列；后加列不会自动成为依赖。有效旧定义通过只读绑定恢复缺失的关系／列引用；单来源旧星号仅在历史输出与原始列前缀吻合时恢复，歧义历史返回明确错误。编译目录是合法读来源；部分目录被别名覆盖的字段先只读取原始声明，再映射实际属性身份，不执行数据或视图表达式。

最终 C4 正常 CMake C++17/O2（程序 SHA 前缀 77418e9c）通过新增原生 242 项检查；同一最终测试旧版实际 120 项失败，中间 C3 实际 2 项失败。7 个既有相关原生测试在旧版及最终 C4 均实际 exit0。28 条真实依赖删除返回 2BP01；29 条普通事务／保存点对照的值、tag、SQLSTATE 和 Ready 与 PostgreSQL18.6 一致，保留先前写入、失败 CASCADE、恢复与冷重启均有记录。

新 201 条协议矩阵仍有 3 条别名运行查询差异，真实旧库升级 42 条仍有 5 条嵌套／字面量运行差异；整表／视图删除 13 条的值与状态匹配 PG，编译目录视图 12 条仍有 1 条别名执行 0A000。全部请求、完整 T7、NULL、诊断和 Ready 原样保留，完整 PG 协议等价仍为 false。原 62 条 SQL 完整回复在旧／C4 间逐字节相同，41 条目录查询及 7 条控制回复相同，原有 9／24 条 PG 差异保持。另保留完整 pg_attribute 89 条调研。

正常性能对照为旧 P34-C2 `73bb61eb` → C4 `77418e9c` → 同旧回测。普通矩阵完整保留 27 个数据库／54 个正常 Main／270 个计时操作、1098 回复、96624 行、386388 单元、27864 NULL、231410943 请求／回复字节；5796 个闭库文件核对。范围包含 INSERT、单条 UPDATE16／DELETE16、VACUUM、TRUNCATE、重填、维护前后 SELECT，以及直接全列／id 查询。固定／单长字段人口 256，多长字段 128，直接投影使用真实更新后的 257／257／129 行；direct-hit 的 id 查询没有 WHERE 或 ORDER BY。

另有 9 个独立数据库／18 个正常 Main／27 个 DDL 计时操作。每库同样插入 65 行，视图返回其中 64 行；CREATE VIEW、兼容 REPLACE、删除未引用列各三次，99 个完整回复跨全部 9 库逐字节相同，2340 行／4662 单元／18 NULL／450 闭库文件独立核对。候选增加依赖检查与元数据持久化，记录的是该特性的开销。旧版错误地接受 RESTRICT 或没有删除依赖视图的工作没有计为等价加速对照。

共 72 个正常 Main、1197 回复、98964 行、391050 单元、27882 NULL。时钟覆盖请求到 ReadyI 的帧接收；服务器 CPU 为整个 Main 进程，完整解码、值校验、输出写盘和 proc 读取在区间外。时钟／proc 不原子，端点不是峰值。计时期间没有本任务的编译、分析器、压缩、CPU 绑定或额外 SQL；自然缓存状态，40 个既有数据库进程仅观察，主机非独占。全部三样本／min／p50／max、客户端 CPU、尾部和旧版漂移保留，不作稳定比率或 PostgreSQL 性能等价结论。

创建视图墙钟为旧首测 102.639 → C4 145.206 → 旧回测 94.602 ms，相对两旧为 +41.47%／+53.49%；服务器 CPU 28.954 → 28.315 → 26.796 ms，为 -2.21%／+5.67%。兼容替换墙钟 +3.63%／-33.05%，删除未引用列 +14.64%／-3.20%；所有其余原始值见完整表。

**wall_ns，ms，全部三次样本的中位数**

| Cohort / table / case | Old first | C4 | Old return | C4 vs first | C4 vs return | Old drift |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 38.565 | 44.839 | 47.171 | +16.27% | -4.94% | +22.31% |
| canonical-fixed-single/fixed/insert1 | 17.076 | 22.299 | 15.365 | +30.59% | +45.13% | -10.02% |
| canonical-fixed-single/fixed/reinsert_population | 575.191 | 611.631 | 545.745 | +6.34% | +12.07% | -5.12% |
| canonical-fixed-single/fixed/select_after_update | 24.167 | 29.172 | 22.061 | +20.71% | +32.23% | -8.71% |
| canonical-fixed-single/fixed/select_after_vacuum | 25.830 | 26.894 | 26.203 | +4.12% | +2.64% | +1.45% |
| canonical-fixed-single/fixed/truncate | 170.233 | 187.925 | 204.392 | +10.39% | -8.06% | +20.07% |
| canonical-fixed-single/fixed/update16 | 75.901 | 84.360 | 64.909 | +11.14% | +29.97% | -14.48% |
| canonical-fixed-single/fixed/vacuum | 1.407 | 1.581 | 1.617 | +12.37% | -2.22% | +14.92% |
| canonical-fixed-single/single/delete16 | 70.656 | 74.300 | 67.908 | +5.16% | +9.41% | -3.89% |
| canonical-fixed-single/single/insert1 | 16.896 | 17.245 | 18.546 | +2.07% | -7.01% | +9.77% |
| canonical-fixed-single/single/reinsert_population | 1057.601 | 1121.333 | 1085.456 | +6.03% | +3.31% | +2.63% |
| canonical-fixed-single/single/select_after_update | 51.666 | 66.980 | 43.916 | +29.64% | +52.52% | -15.00% |
| canonical-fixed-single/single/select_after_vacuum | 53.790 | 70.679 | 55.162 | +31.40% | +28.13% | +2.55% |
| canonical-fixed-single/single/truncate | 369.849 | 322.837 | 336.096 | -12.71% | -3.95% | -9.13% |
| canonical-fixed-single/single/update16 | 136.667 | 147.036 | 141.788 | +7.59% | +3.70% | +3.75% |
| canonical-fixed-single/single/vacuum | 7.584 | 8.294 | 6.700 | +9.36% | +23.80% | -11.66% |
| canonical-overflow/overflow/delete16 | 66.391 | 81.636 | 80.388 | +22.96% | +1.55% | +21.08% |
| canonical-overflow/overflow/insert1 | 24.213 | 22.614 | 25.980 | -6.60% | -12.96% | +7.30% |
| canonical-overflow/overflow/reinsert_population | 1132.334 | 1209.218 | 1087.865 | +6.79% | +11.16% | -3.93% |
| canonical-overflow/overflow/select_after_update | 60.828 | 65.668 | 50.719 | +7.96% | +29.48% | -16.62% |
| canonical-overflow/overflow/select_after_vacuum | 66.966 | 62.883 | 62.399 | -6.10% | +0.78% | -6.82% |
| canonical-overflow/overflow/truncate | 193.081 | 170.525 | 204.691 | -11.68% | -16.69% | +6.01% |
| canonical-overflow/overflow/update16 | 166.765 | 182.010 | 159.055 | +9.14% | +14.43% | -4.62% |
| canonical-overflow/overflow/vacuum | 8.559 | 11.189 | 10.900 | +30.73% | +2.66% | +27.35% |
| direct-hit/fixed/direct_full_no_order | 25.341 | 19.349 | 23.977 | -23.64% | -19.30% | -5.38% |
| direct-hit/fixed/direct_id_only_no_order | 23.961 | 17.479 | 24.177 | -27.05% | -27.70% | +0.90% |
| direct-hit/overflow/direct_full_no_order | 49.846 | 60.799 | 47.989 | +21.97% | +26.69% | -3.73% |
| direct-hit/overflow/direct_id_only_no_order | 24.290 | 23.029 | 17.443 | -5.20% | +32.02% | -28.19% |
| direct-hit/single/direct_full_no_order | 54.083 | 56.223 | 40.934 | +3.96% | +37.35% | -24.31% |
| direct-hit/single/direct_id_only_no_order | 23.898 | 27.566 | 21.082 | +15.35% | +30.75% | -11.78% |

**server_process_cpu_ns，ms，全部三次样本的中位数**

| Cohort / table / case | Old first | C4 | Old return | C4 vs first | C4 vs return | Old drift |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 32.291 | 38.165 | 40.368 | +18.19% | -5.46% | +25.02% |
| canonical-fixed-single/fixed/insert1 | 8.578 | 9.231 | 8.079 | +7.62% | +14.26% | -5.81% |
| canonical-fixed-single/fixed/reinsert_population | 549.493 | 579.192 | 519.939 | +5.40% | +11.40% | -5.38% |
| canonical-fixed-single/fixed/select_after_update | 17.159 | 20.832 | 15.220 | +21.41% | +36.87% | -11.30% |
| canonical-fixed-single/fixed/select_after_vacuum | 20.260 | 20.096 | 18.508 | -0.81% | +8.58% | -8.65% |
| canonical-fixed-single/fixed/truncate | 84.574 | 94.282 | 95.994 | +11.48% | -1.78% | +13.50% |
| canonical-fixed-single/fixed/update16 | 67.132 | 73.605 | 56.498 | +9.64% | +30.28% | -15.84% |
| canonical-fixed-single/fixed/vacuum | 1.271 | 1.480 | 1.494 | +16.46% | -0.97% | +17.60% |
| canonical-fixed-single/single/delete16 | 62.653 | 64.195 | 57.602 | +2.46% | +11.45% | -8.06% |
| canonical-fixed-single/single/insert1 | 9.019 | 10.064 | 10.073 | +11.59% | -0.08% | +11.68% |
| canonical-fixed-single/single/reinsert_population | 982.473 | 1064.258 | 1044.683 | +8.32% | +1.87% | +6.33% |
| canonical-fixed-single/single/select_after_update | 43.319 | 46.196 | 37.538 | +6.64% | +23.06% | -13.34% |
| canonical-fixed-single/single/select_after_vacuum | 47.221 | 58.744 | 47.867 | +24.40% | +22.72% | +1.37% |
| canonical-fixed-single/single/truncate | 133.017 | 121.895 | 125.418 | -8.36% | -2.81% | -5.71% |
| canonical-fixed-single/single/update16 | 116.050 | 126.546 | 123.555 | +9.04% | +2.42% | +6.47% |
| canonical-fixed-single/single/vacuum | 7.304 | 8.039 | 6.389 | +10.06% | +25.83% | -12.53% |
| canonical-overflow/overflow/delete16 | 56.086 | 70.770 | 67.506 | +26.18% | +4.84% | +20.36% |
| canonical-overflow/overflow/insert1 | 10.401 | 13.113 | 11.345 | +26.07% | +15.59% | +9.07% |
| canonical-overflow/overflow/reinsert_population | 1050.819 | 1101.319 | 1040.266 | +4.81% | +5.87% | -1.00% |
| canonical-overflow/overflow/select_after_update | 50.404 | 53.958 | 41.483 | +7.05% | +30.07% | -17.70% |
| canonical-overflow/overflow/select_after_vacuum | 55.343 | 53.935 | 55.284 | -2.54% | -2.44% | -0.11% |
| canonical-overflow/overflow/truncate | 84.536 | 68.453 | 91.659 | -19.03% | -25.32% | +8.43% |
| canonical-overflow/overflow/update16 | 137.782 | 135.132 | 138.516 | -1.92% | -2.44% | +0.53% |
| canonical-overflow/overflow/vacuum | 8.206 | 10.567 | 10.596 | +28.78% | -0.27% | +29.13% |
| direct-hit/fixed/direct_full_no_order | 16.368 | 11.861 | 15.417 | -27.53% | -23.06% | -5.81% |
| direct-hit/fixed/direct_id_only_no_order | 15.527 | 11.447 | 15.194 | -26.28% | -24.66% | -2.15% |
| direct-hit/overflow/direct_full_no_order | 43.874 | 54.935 | 40.742 | +25.21% | +34.84% | -7.14% |
| direct-hit/overflow/direct_id_only_no_order | 11.447 | 11.882 | 12.221 | +3.79% | -2.77% | +6.76% |
| direct-hit/single/direct_full_no_order | 45.784 | 47.946 | 33.614 | +4.72% | +42.64% | -26.58% |
| direct-hit/single/direct_id_only_no_order | 17.011 | 16.002 | 14.600 | -5.93% | +9.60% | -14.17% |

**client_process_cpu_ns，ms，全部三次样本的中位数**

| Cohort / table / case | Old first | C4 | Old return | C4 vs first | C4 vs return | Old drift |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 0.214 | 0.227 | 0.218 | +6.12% | +4.06% | +1.99% |
| canonical-fixed-single/fixed/insert1 | 0.199 | 0.221 | 0.123 | +11.41% | +79.87% | -38.06% |
| canonical-fixed-single/fixed/reinsert_population | 0.214 | 0.257 | 0.220 | +20.22% | +16.50% | +3.19% |
| canonical-fixed-single/fixed/select_after_update | 4.952 | 4.797 | 4.668 | -3.12% | +2.77% | -5.73% |
| canonical-fixed-single/fixed/select_after_vacuum | 3.905 | 4.489 | 4.595 | +14.94% | -2.31% | +17.66% |
| canonical-fixed-single/fixed/truncate | 0.229 | 0.238 | 0.272 | +4.23% | -12.26% | +18.79% |
| canonical-fixed-single/fixed/update16 | 0.229 | 0.246 | 0.218 | +7.41% | +12.56% | -4.58% |
| canonical-fixed-single/fixed/vacuum | 0.203 | 0.214 | 0.196 | +5.59% | +9.30% | -3.40% |
| canonical-fixed-single/single/delete16 | 0.207 | 0.259 | 0.161 | +24.94% | +60.47% | -22.14% |
| canonical-fixed-single/single/insert1 | 0.243 | 0.203 | 0.279 | -16.66% | -27.34% | +14.70% |
| canonical-fixed-single/single/reinsert_population | 0.576 | 0.613 | 0.584 | +6.32% | +4.90% | +1.35% |
| canonical-fixed-single/single/select_after_update | 5.986 | 6.111 | 5.417 | +2.09% | +12.81% | -9.50% |
| canonical-fixed-single/single/select_after_vacuum | 5.217 | 5.382 | 5.186 | +3.16% | +3.77% | -0.59% |
| canonical-fixed-single/single/truncate | 0.210 | 0.246 | 0.246 | +17.10% | +0.23% | +16.84% |
| canonical-fixed-single/single/update16 | 0.231 | 0.260 | 0.242 | +12.30% | +7.21% | +4.74% |
| canonical-fixed-single/single/vacuum | 0.226 | 0.210 | 0.169 | -6.84% | +24.56% | -25.21% |
| canonical-overflow/overflow/delete16 | 0.241 | 0.296 | 0.214 | +22.99% | +38.38% | -11.12% |
| canonical-overflow/overflow/insert1 | 0.211 | 0.219 | 0.214 | +3.78% | +2.30% | +1.44% |
| canonical-overflow/overflow/reinsert_population | 0.857 | 0.784 | 0.703 | -8.48% | +11.56% | -17.96% |
| canonical-overflow/overflow/select_after_update | 4.334 | 4.626 | 4.463 | +6.74% | +3.65% | +2.98% |
| canonical-overflow/overflow/select_after_vacuum | 4.067 | 3.773 | 3.907 | -7.23% | -3.45% | -3.92% |
| canonical-overflow/overflow/truncate | 0.255 | 0.237 | 0.234 | -7.15% | +1.23% | -8.28% |
| canonical-overflow/overflow/update16 | 0.263 | 0.263 | 0.257 | -0.00% | +2.42% | -2.37% |
| canonical-overflow/overflow/vacuum | 0.199 | 0.234 | 0.216 | +17.62% | +8.49% | +8.42% |
| direct-hit/fixed/direct_full_no_order | 4.906 | 4.934 | 4.792 | +0.55% | +2.96% | -2.34% |
| direct-hit/fixed/direct_id_only_no_order | 4.808 | 4.725 | 6.674 | -1.72% | -29.20% | +38.81% |
| direct-hit/overflow/direct_full_no_order | 4.369 | 4.050 | 4.015 | -7.29% | +0.88% | -8.10% |
| direct-hit/overflow/direct_id_only_no_order | 2.396 | 2.222 | 2.281 | -7.28% | -2.58% | -4.82% |
| direct-hit/single/direct_full_no_order | 7.798 | 6.480 | 6.560 | -16.90% | -1.22% | -15.87% |
| direct-hit/single/direct_id_only_no_order | 4.315 | 3.862 | 3.861 | -10.49% | +0.05% | -10.54% |

**视图 DDL 全部原始样本与分布，ms**

| case | metric | old raw3/min/p50/max ms | candidate raw3/min/p50/max ms | old return raw3/min/p50/max ms | candidate vs old % | candidate vs return % | old drift % |
|---|---|---|---|---|---:|---:|---:|
| create_view | wall_ns | 102.639,95.438,121.699 / 95.438 / 102.639 / 121.699 | 89.054,158.243,145.206 / 89.054 / 145.206 / 158.243 | 91.636,94.602,135.105 / 91.636 / 94.602 / 135.105 | 41.47 | 53.49 | -7.83 |
| create_view | server_process_cpu_ns | 39.488,27.587,28.954 / 27.587 / 28.954 / 39.488 | 28.315,35.956,26.465 / 26.465 / 28.315 / 35.956 | 33.986,24.495,26.796 / 24.495 / 26.796 / 33.986 | -2.21 | 5.67 | -7.45 |
| create_view | client_process_cpu_ns | 0.219,0.222,0.210 / 0.210 / 0.219 / 0.222 | 0.210,0.217,0.252 / 0.210 / 0.217 / 0.252 | 0.214,0.199,0.213 / 0.199 / 0.213 / 0.214 | -0.63 | 1.81 | -2.39 |
| replace_view | wall_ns | 78.808,85.632,102.520 / 78.808 / 85.632 / 102.520 | 75.066,120.384,88.744 / 75.066 / 88.744 / 120.384 | 143.964,132.551,105.484 / 105.484 / 132.551 / 143.964 | 3.63 | -33.05 | 54.79 |
| replace_view | server_process_cpu_ns | 37.281,40.177,43.348 / 37.281 / 40.177 / 43.348 | 36.176,41.200,23.275 / 23.275 / 36.176 / 41.200 | 36.857,36.394,40.359 / 36.394 / 36.857 / 40.359 | -9.96 | -1.85 | -8.26 |
| replace_view | client_process_cpu_ns | 0.203,0.228,0.244 / 0.203 / 0.228 / 0.244 | 0.220,0.229,0.178 / 0.178 / 0.220 / 0.229 | 0.213,0.227,0.246 / 0.213 / 0.227 / 0.246 | -3.76 | -3.33 | -0.44 |
| drop_unreferenced_column | wall_ns | 211.993,246.507,274.857 / 211.993 / 246.507 / 274.857 | 282.585,291.106,256.959 / 256.959 / 282.585 / 291.106 | 257.657,321.032,291.917 / 257.657 / 291.917 / 321.032 | 14.64 | -3.20 | 18.42 |
| drop_unreferenced_column | server_process_cpu_ns | 149.772,172.050,173.166 / 149.772 / 172.050 / 173.166 | 170.174,179.491,137.793 / 137.793 / 170.174 / 179.491 | 154.304,179.689,167.932 / 154.304 / 167.932 / 179.689 | -1.09 | 1.33 | -2.39 |
| drop_unreferenced_column | client_process_cpu_ns | 0.215,0.286,0.215 / 0.215 / 0.215 / 0.286 | 0.223,0.235,0.167 / 0.167 / 0.223 / 0.235 | 0.228,0.256,0.224 / 0.224 / 0.228 / 0.256 | 3.54 | -2.19 | 5.86 |

新增压缩证据 21,397,678 字节（20.41 MiB），SHA256 `0b906588b6991c839d0355c94a8ae10b60c179286174b026d5e090a7adb43b72`；54,132 个非 ELF 文件、2,771 个目录、3,988 个内容块和 376 个本地 ELF 引用经过独立重建核对。随后清理 50,311 个已备份的生成文件，逻辑副本 2.008 GiB；这不是独占文件系统释放量。源码、原始错误／日志、所有 ELF、一个最终工作数据集和一个真实旧库冷关闭种子保留。 [压缩证据](benchmarks/view-column-dependencies-20261106.json.gz)包含完整 SQL、回复、数据、源代码快照、正常与失败记录。大体积原始 JSON 只在忽略的 build 目录工作，不加入 Git。最初 53.45 MiB 报告仍以 5.70 MiB gzip 保留，用于核对测量和追溯；没有重写历史提交。

在仓库根目录恢复完整普通性能原始表；将 member 改为 `batch/view-ddl-cost9-C4-v1/all-DDL-performance-tables-v1.md` 可恢复 DDL 表：

```sh
python3 - <<'PYEXTRACT'
import base64, gzip, json, lzma, sys
with gzip.open("docs/benchmarks/view-column-dependencies-20261106.json.gz", "rb") as f:
    envelope = json.load(f)
bundle = json.loads(lzma.decompress(base64.b64decode(envelope["payload_xz_base64"])))
member = "batch/view-column-dependency-cost27-C4-v1/all-performance-tables-v3.md"
entry = bundle["artifacts"][member]
sys.stdout.buffer.write(base64.b64decode(bundle["blobs"][entry["blob"]]))
PYEXTRACT
```

保留的失败包括：C1 升级首条查询超时、0 回复／42 NOTRUN，实际强制 -9 且进程组已消失，原因 OPEN；C3 目录视图前置存在性检查导致的原生 abort，以及 TypeRegistry 启动诊断；最终 C4 修复真实前置检查，旧版缺少能力以失败检查记录。外围进度脚本在普通 270 操作及独立校验成功后发生 exclusive-create FileExistsError，只继续尚未启动的 DDL 测量，没有重采样。首次输入审计的暂缓状态键拼写错误已由 v2 对原始对象逐项核对；首次第六来源 HTTP403 及后续完整成功的六来源抓取记录均保留。

本批范围是普通 SQL 的静态 DDL 关系／列依赖。视图来源 T7 的零 origin、列别名运行时重绑定、部分嵌套视图、字面量／envelope 运行时、含歧义历史的旧视图、物化视图、函数／类型依赖及大量视图的扩展性仍有缺口；普通 pg_attribute 查询尚未补齐。用户暂缓范围保持，未修改执行图／child-provider、portal 或通用故障矩阵。

正式库存为 2396 个测试文件（1129 顶层／1134 总 C++），注册 E2E 驱动 684／其中独立 E2E 664 不变；本批没有声称全套通过。原 273 项、22 complete／166 partial／70 unverified／15 deferred_by_user 保持，历史记录增至 285，IDX-07 仍 unverified/evidence[]。754 个 Markdown 路径、725 份保护记录及旧 35 个压缩包保持。 当前仍是 partial，Goal active；下一步优先定位性能负向并继续 pg_attribute 等目录功能。默认程序 SHA425165af 未更新、没有推送。下方条目保留各自版本的历史结果。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
<a id="projected-toast-demand-20261106"></a>
## 2026-11-06：部分列查询按需求读取长字段（P34-C2）

最终候选为正常 CMake C++17/O2 `73bb61eb`，旧版为 P33-C2 `280ad4a8`。仅取 id 的单长字段表墙钟相对旧首测/旧回测为 -54.47% / -47.06%，多长字段表为 -61.28% / -61.63%。这是本轮三次样本中位数的有限结果，全部 30 个操作区域、长尾、回退与旧版漂移一并列于下方。下方更早批次保留各自源码版本的历史结果。

普通物理游标上的部分列投影按实际选择解码字段，复用当前行 NULL 位图，仅解析被选中长字段的 TOAST 引用。选中长字段读取失败仍在行输出前报错，输出值、NULL、RID 与旧路径的列顺序保持。全列/星号投影继续原 materialization 路径；选中虚拟列保留原完整输入。过滤、索引、排序、缓冲和不同 schema 的子路径仍由各自已有执行路径处理。

设计参考：[PostgreSQL 18.6 tuple slot](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/executor/tuptable.h) 的按需展开及资源归属、[SQLite Column 指令](https://www.sqlite.org/opcode.html#Column) 的列读取、[DuckDB v1.4.0 table-function API](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.0/src/include/duckdb.h) 的投影下推声明与请求列索引。这些是实现机制参考，没有运行它们的速度比较。

诊断先使用 P33 实际测试正常关闭后的三张表数据，运行旧程序/旧目录、旧程序/新目录、P33 程序280/新目录、旧程序回测四组；每组 512 次 `SELECT id FROM perf_single`，共 2048 次采样目标/2100 次完整响应，前后校验三表全部字段。4 个 Main 实际正常退出。相同 CPU8 与 libprofiler-only 1000Hz 窗口仅用于诊断，原始 PC 经真实 ELF/maps 和有长度的符号范围解释；目录调用栈占比约 13.6%～14.1%，Project 约 65.5%～66.1%。组间重叠不可相加，采样包含前端/后台工作；这不足以判定先前目录特性导致退化。实际源码与缺失未选中 LOB 的正向回归共同定位到扫描阶段全量解码开销。

C1 曾让全列查询也使用新解码器。其首轮完整矩阵中，多 LOB 全列的墙钟比两旧高 9.69%/14.21%，服务器 CPU 高 2.36%/6.36%。该候选不作为最终实现；C2 明确让全列走原路径，再运行一整轮矩阵。全列路径中位数的变化保留为测量观察，不能单凭两轮数据归因于该分支选择。C1 的所有原始样本及负向仍在归档中；没有筛选 C1 的好结果或仅重测不利项目。

新增原生回归 14 组、83 项检查全部通过；同一测试在旧版实际失败 6 项。另 5 个既有相关原生测试在旧版及最终 C2 均通过，包含多 LOB 正常冷开/恢复生命周期、缺文件错误、BYTEA 和目录声明 550 项检查。原有 62 条完整协议回复、额外 41 条目录查询及 7 条控制回复，在旧版/C1/C2 间逐字节一致；原 9 项与新 24 条 PostgreSQL 查询差异继续保留。本次比较复用实际冻结的旧版记录，最终 C2 新运行 4 个初始/冷启动 Main；其余 4 个旧版 Main 的原始正常退出记录与完整回复独立复核。没有重新运行旧 PostgreSQL 参考，也没有把严格 CLI 的失败标成通过。正式测试库存为 2395 个文件（1128 个顶层/1133 个总 C++ 文件），本批运行上述专项，未声称全套通过。

C1 与最终 C2 各执行完整 27 个数据库上下文/54 个正常 Main/270 个计时区间；每轮 1098 响应、96624 行、386388 单元、27864 NULL、231410943 请求/回复字节均经独立解码检查。最终 C2 的 5796 个闭库文件逐字节验证。每轮采用旧版→候选→同旧版，各三轮，完整保留 INSERT、UPDATE16、DELETE16、VACUUM、TRUNCATE、重填、维护前后 SELECT 及直接全列/仅 id 查询。UPDATE16/DELETE16 各为一条影响 16 行的 SQL。

正常性能窗口没有 CPU 绑定、分析器、分配器替换、编译、额外 SQL 或压缩。使用自然缓存状态，既有其他数据库进程仅观察且保持运行，因此不作独占主机结论。服务器 CPU 为整个 Main 进程 CPU；墙钟/客户端 CPU 覆盖请求及帧缓冲至 ReadyI；完整解码、值校验、文件落盘和 proc 快照在计时之后。时钟和 proc 采集非原子，RSS/IO 端点不是峰值。

固定/单长字段 canonical 人口为 256，多 LOB 为 128；直接投影使用真实更新后的 257/257/129 行。继承标签 direct-hit 下的 `SELECT id` 是全部行的列投影，没有 WHERE 或 ORDER BY。全列查询仍读取全部字段，本报告没有将此处收益外推至所有查询或全部写入操作。

最终 C2 的 30 区域方向：墙钟 7 低于两旧 / 12 高于两旧 / 11 居间或相等；服务器 CPU 7 低于两旧 / 11 高于两旧 / 12 居间或相等；客户端 CPU 11 低于两旧 / 13 高于两旧 / 6 居间或相等。方向依据未四舍五入的原始纳秒中位数，含极小差异，不代表统计显著性。旧版墙钟漂移范围为 -27.30%～+17.42%，服务器 CPU 为 -30.52%～+18.26%。所有高于两旧及居间结果完整保留，原 P30/P32/P33 的原因追踪和本轮负向继续 OPEN。

wall_ns (milliseconds; medians of all3 raw timings)

| Cohort / table / case | Old first | C2 | Old return | C2 vs first | C2 vs return | Old drift |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 50.260 | 46.538 | 47.171 | -7.40% | -1.34% | -6.15% |
| canonical-fixed-single/fixed/insert1 | 19.756 | 23.636 | 15.354 | +19.64% | +53.95% | -22.28% |
| canonical-fixed-single/fixed/reinsert_population | 622.272 | 665.249 | 663.366 | +6.91% | +0.28% | +6.60% |
| canonical-fixed-single/fixed/select_after_update | 31.925 | 30.719 | 27.438 | -3.78% | +11.96% | -14.06% |
| canonical-fixed-single/fixed/select_after_vacuum | 33.060 | 30.341 | 26.502 | -8.22% | +14.49% | -19.84% |
| canonical-fixed-single/fixed/truncate | 228.025 | 197.998 | 181.871 | -13.17% | +8.87% | -20.24% |
| canonical-fixed-single/fixed/update16 | 88.089 | 90.384 | 79.773 | +2.61% | +13.30% | -9.44% |
| canonical-fixed-single/fixed/vacuum | 1.774 | 1.728 | 1.615 | -2.60% | +7.00% | -8.97% |
| canonical-fixed-single/single/delete16 | 67.314 | 84.546 | 65.911 | +25.60% | +28.27% | -2.08% |
| canonical-fixed-single/single/insert1 | 20.926 | 24.759 | 19.348 | +18.32% | +27.97% | -7.54% |
| canonical-fixed-single/single/reinsert_population | 1093.917 | 1085.646 | 1045.824 | -0.76% | +3.81% | -4.40% |
| canonical-fixed-single/single/select_after_update | 49.232 | 60.806 | 51.342 | +23.51% | +18.43% | +4.28% |
| canonical-fixed-single/single/select_after_vacuum | 55.448 | 54.705 | 51.179 | -1.34% | +6.89% | -7.70% |
| canonical-fixed-single/single/truncate | 261.984 | 279.257 | 255.862 | +6.59% | +9.14% | -2.34% |
| canonical-fixed-single/single/update16 | 149.486 | 173.179 | 142.357 | +15.85% | +21.65% | -4.77% |
| canonical-fixed-single/single/vacuum | 7.525 | 7.757 | 7.035 | +3.08% | +10.26% | -6.51% |
| canonical-overflow/overflow/delete16 | 77.535 | 138.909 | 80.809 | +79.16% | +71.90% | +4.22% |
| canonical-overflow/overflow/insert1 | 24.073 | 20.099 | 17.502 | -16.51% | +14.84% | -27.30% |
| canonical-overflow/overflow/reinsert_population | 1108.433 | 1098.243 | 1131.886 | -0.92% | -2.97% | +2.12% |
| canonical-overflow/overflow/select_after_update | 58.337 | 57.781 | 54.829 | -0.95% | +5.39% | -6.01% |
| canonical-overflow/overflow/select_after_vacuum | 63.393 | 68.218 | 55.032 | +7.61% | +23.96% | -13.19% |
| canonical-overflow/overflow/truncate | 213.693 | 246.123 | 250.922 | +15.18% | -1.91% | +17.42% |
| canonical-overflow/overflow/update16 | 155.714 | 163.048 | 169.142 | +4.71% | -3.60% | +8.62% |
| canonical-overflow/overflow/vacuum | 9.938 | 11.339 | 8.171 | +14.10% | +38.78% | -17.78% |
| direct-hit/fixed/direct_full_no_order | 26.699 | 19.417 | 20.626 | -27.28% | -5.86% | -22.75% |
| direct-hit/fixed/direct_id_only_no_order | 22.749 | 18.678 | 23.470 | -17.90% | -20.42% | +3.17% |
| direct-hit/overflow/direct_full_no_order | 57.918 | 37.233 | 58.094 | -35.71% | -35.91% | +0.30% |
| direct-hit/overflow/direct_id_only_no_order | 49.027 | 18.983 | 49.470 | -61.28% | -61.63% | +0.90% |
| direct-hit/single/direct_full_no_order | 52.300 | 50.776 | 49.727 | -2.91% | +2.11% | -4.92% |
| direct-hit/single/direct_id_only_no_order | 54.255 | 24.703 | 46.662 | -54.47% | -47.06% | -13.99% |

server_process_cpu_ns (milliseconds; medians of all3 raw timings)

| Cohort / table / case | Old first | C2 | Old return | C2 vs first | C2 vs return | Old drift |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 40.234 | 39.035 | 40.966 | -2.98% | -4.71% | +1.82% |
| canonical-fixed-single/fixed/insert1 | 9.008 | 8.815 | 7.602 | -2.15% | +15.96% | -15.62% |
| canonical-fixed-single/fixed/reinsert_population | 592.785 | 582.959 | 590.351 | -1.66% | -1.25% | -0.41% |
| canonical-fixed-single/fixed/select_after_update | 21.388 | 21.668 | 20.277 | +1.31% | +6.86% | -5.19% |
| canonical-fixed-single/fixed/select_after_vacuum | 22.446 | 21.746 | 19.854 | -3.12% | +9.53% | -11.55% |
| canonical-fixed-single/fixed/truncate | 101.612 | 96.730 | 87.604 | -4.80% | +10.42% | -13.79% |
| canonical-fixed-single/fixed/update16 | 73.248 | 72.818 | 67.390 | -0.59% | +8.06% | -8.00% |
| canonical-fixed-single/fixed/vacuum | 1.656 | 1.584 | 1.480 | -4.32% | +7.04% | -10.61% |
| canonical-fixed-single/single/delete16 | 58.308 | 68.705 | 58.587 | +17.83% | +17.27% | +0.48% |
| canonical-fixed-single/single/insert1 | 8.642 | 13.349 | 10.220 | +54.46% | +30.62% | +18.26% |
| canonical-fixed-single/single/reinsert_population | 1014.239 | 1036.257 | 1015.952 | +2.17% | +2.00% | +0.17% |
| canonical-fixed-single/single/select_after_update | 43.323 | 48.706 | 42.575 | +12.42% | +14.40% | -1.73% |
| canonical-fixed-single/single/select_after_vacuum | 48.980 | 45.624 | 44.381 | -6.85% | +2.80% | -9.39% |
| canonical-fixed-single/single/truncate | 116.090 | 120.319 | 133.263 | +3.64% | -9.71% | +14.79% |
| canonical-fixed-single/single/update16 | 122.510 | 139.442 | 117.824 | +13.82% | +18.35% | -3.83% |
| canonical-fixed-single/single/vacuum | 7.230 | 7.513 | 6.752 | +3.91% | +11.27% | -6.61% |
| canonical-overflow/overflow/delete16 | 67.010 | 72.364 | 68.328 | +7.99% | +5.91% | +1.97% |
| canonical-overflow/overflow/insert1 | 11.815 | 10.910 | 9.361 | -7.66% | +16.55% | -20.78% |
| canonical-overflow/overflow/reinsert_population | 1056.145 | 1036.280 | 1055.697 | -1.88% | -1.84% | -0.04% |
| canonical-overflow/overflow/select_after_update | 51.671 | 49.501 | 46.658 | -4.20% | +6.09% | -9.70% |
| canonical-overflow/overflow/select_after_vacuum | 56.319 | 61.465 | 48.125 | +9.14% | +27.72% | -14.55% |
| canonical-overflow/overflow/truncate | 86.677 | 88.327 | 88.677 | +1.90% | -0.40% | +2.31% |
| canonical-overflow/overflow/update16 | 130.402 | 137.520 | 132.065 | +5.46% | +4.13% | +1.28% |
| canonical-overflow/overflow/vacuum | 9.595 | 11.022 | 7.851 | +14.87% | +40.40% | -18.18% |
| direct-hit/fixed/direct_full_no_order | 14.333 | 11.149 | 9.958 | -22.21% | +11.96% | -30.52% |
| direct-hit/fixed/direct_id_only_no_order | 14.107 | 11.263 | 13.848 | -20.17% | -18.67% | -1.84% |
| direct-hit/overflow/direct_full_no_order | 46.401 | 30.542 | 51.197 | -34.18% | -40.34% | +10.34% |
| direct-hit/overflow/direct_id_only_no_order | 41.871 | 11.015 | 42.870 | -73.69% | -74.31% | +2.39% |
| direct-hit/single/direct_full_no_order | 45.340 | 43.835 | 42.996 | -3.32% | +1.95% | -5.17% |
| direct-hit/single/direct_id_only_no_order | 38.764 | 16.019 | 37.756 | -58.68% | -57.57% | -2.60% |

client_process_cpu_ns (milliseconds; medians of all3 raw timings)

| Cohort / table / case | Old first | C2 | Old return | C2 vs first | C2 vs return | Old drift |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 0.231 | 0.248 | 0.202 | +7.19% | +22.82% | -12.73% |
| canonical-fixed-single/fixed/insert1 | 0.203 | 0.248 | 0.200 | +22.56% | +24.50% | -1.56% |
| canonical-fixed-single/fixed/reinsert_population | 0.255 | 0.244 | 0.220 | -4.16% | +10.99% | -13.65% |
| canonical-fixed-single/fixed/select_after_update | 4.992 | 4.782 | 4.912 | -4.21% | -2.66% | -1.59% |
| canonical-fixed-single/fixed/select_after_vacuum | 4.648 | 4.659 | 4.639 | +0.25% | +0.43% | -0.18% |
| canonical-fixed-single/fixed/truncate | 0.230 | 0.242 | 0.183 | +5.32% | +32.50% | -20.51% |
| canonical-fixed-single/fixed/update16 | 0.223 | 0.258 | 0.218 | +15.70% | +18.39% | -2.28% |
| canonical-fixed-single/fixed/vacuum | 0.205 | 0.218 | 0.201 | +6.65% | +8.82% | -2.00% |
| canonical-fixed-single/single/delete16 | 0.240 | 0.280 | 0.213 | +16.60% | +31.25% | -11.16% |
| canonical-fixed-single/single/insert1 | 0.210 | 0.244 | 0.205 | +16.42% | +19.29% | -2.40% |
| canonical-fixed-single/single/reinsert_population | 0.768 | 0.678 | 0.708 | -11.80% | -4.26% | -7.88% |
| canonical-fixed-single/single/select_after_update | 6.038 | 5.969 | 5.989 | -1.13% | -0.33% | -0.80% |
| canonical-fixed-single/single/select_after_vacuum | 5.755 | 5.184 | 5.289 | -9.91% | -1.98% | -8.09% |
| canonical-fixed-single/single/truncate | 0.234 | 0.240 | 0.236 | +2.40% | +1.41% | +0.98% |
| canonical-fixed-single/single/update16 | 0.237 | 0.308 | 0.235 | +30.08% | +31.17% | -0.83% |
| canonical-fixed-single/single/vacuum | 0.219 | 0.258 | 0.189 | +17.94% | +36.65% | -13.69% |
| canonical-overflow/overflow/delete16 | 0.221 | 0.263 | 0.254 | +19.03% | +3.59% | +14.90% |
| canonical-overflow/overflow/insert1 | 0.205 | 0.206 | 0.232 | +0.56% | -11.23% | +13.29% |
| canonical-overflow/overflow/reinsert_population | 0.734 | 0.760 | 0.841 | +3.49% | -9.65% | +14.54% |
| canonical-overflow/overflow/select_after_update | 4.400 | 4.249 | 4.433 | -3.44% | -4.16% | +0.76% |
| canonical-overflow/overflow/select_after_vacuum | 3.953 | 3.953 | 3.496 | -0.02% | +13.05% | -11.56% |
| canonical-overflow/overflow/truncate | 0.216 | 0.207 | 0.241 | -4.12% | -14.06% | +11.57% |
| canonical-overflow/overflow/update16 | 0.253 | 0.233 | 0.272 | -7.88% | -14.04% | +7.17% |
| canonical-overflow/overflow/vacuum | 0.206 | 0.193 | 0.260 | -6.40% | -25.58% | +25.78% |
| direct-hit/fixed/direct_full_no_order | 5.061 | 4.942 | 4.914 | -2.37% | +0.57% | -2.92% |
| direct-hit/fixed/direct_id_only_no_order | 4.992 | 4.925 | 4.965 | -1.34% | -0.81% | -0.54% |
| direct-hit/overflow/direct_full_no_order | 4.011 | 3.698 | 4.197 | -7.80% | -11.90% | +4.65% |
| direct-hit/overflow/direct_id_only_no_order | 2.021 | 2.315 | 2.029 | +14.54% | +14.11% | +0.38% |
| direct-hit/single/direct_full_no_order | 6.059 | 5.851 | 6.197 | -3.42% | -5.57% | +2.28% |
| direct-hit/single/direct_id_only_no_order | 4.280 | 3.792 | 3.655 | -11.39% | +3.75% | -14.59% |


原始三样本/min/p50/max、三类 CPU/墙钟、长尾、完整 SQL/帧/数据/源码与两代候选证据均在 [projected-toast-demand-20261106.json.gz](benchmarks/projected-toast-demand-20261106.json.gz)，21,927,356 字节（20.91 MiB），SHA256 `82f53d6ec18f0f194af4d68db48ffa665220f4a730c32d29b71fcb80aa03ae1d`。全部非 ELF 字节独立解码重建并与原件比较后才清理；CPU 诊断、原始 PC 与采样树另存 [catalog-query-profile-20261106.json.gz](benchmarks/catalog-query-profile-20261106.json.gz)，28,037,375 字节（26.74 MiB），SHA256 `6f1cedfa278c53063fbfdf3edf7daff9786fc4350833789a96daafa93a52071d`。两包合并的文件名集合与本批全部输入一致，每包均低于 49 MiB。ELF 为明确 SHA 引用且保留在本机。压缩结构为 gzip JSON→base64/XZ→内容寻址 JSON；没有将大体积原始 JSON 加入 Git。

在仓库根目录提取最终完整原始表（将路径里的 C2 改为 C1 可提取首轮表）：

```sh
python3 - <<'PYEXTRACT'
import base64, gzip, json, lzma, sys
with gzip.open("docs/benchmarks/projected-toast-demand-20261106.json.gz", "rb") as f:
    envelope = json.load(f)
bundle = json.loads(lzma.decompress(base64.b64decode(envelope["payload_xz_base64"])))
entry = bundle["artifacts"]["batch/projected-toast-demand-cost27-C2-v1/all-performance-tables-v3.md"]
sys.stdout.buffer.write(base64.b64decode(bundle["blobs"][entry["blob"]]))
PYEXTRACT
```

原 273 项状态与既有 283 条历史逐项保持；新增部分进展一条，总历史 284。IDX-07 仍 unverified/evidence[]，725 份受保护历史 Markdown、33 份旧压缩归档及用户暂缓范围均保持，Markdown 路径仍 754。默认425未替换；工作直接提交 master。原始 `analyze-reindex-main-costs-20261104.json` 继续仅以已验证 gzip 保留，53.45 MiB→5.70 MiB；Git 历史未重写。Goal active，完整 PostgreSQL 功能、稳定吞吐及一般写入提速仍待完成。

清理在两包独立完整还原校验通过后进行：删除 41,010 个本批生成副本、三个实际私有原生测试目录中的 4,935 个文件（共 444 个目录），以及两份重复临时压缩包。合计 45,947 个常规文件、3,116,489,621 逻辑字节（2.902 GiB）；被删除单链接文件 st_blocks 之和为 3,151,708,160 字节（2.935 GiB），这不是整台服务器独占空闲空间增量。213 个 ELF 引用、源码、原始 main.prof/maps/nm、最终 C2 的真实20-candidate工作数据和 6484 个其余归档输入文件均逐字节保持；所有非 ELF 原始数据可由两份压缩包恢复。清理采用精确文件白名单与完整 SHA 校验，既有数据库进程保持运行。

<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
<a id="pg-class-declaration-20261106"></a>
## 2026-11-06: owned pg_class declaration and ordinary SQL cost controls

The accepted candidate is normal CMake C++17/O2 C2 `280ad4a8`; the first C1 is rejected and retained as evidence. This is a partial catalog feature, with no speed improvement or full PostgreSQL compatibility claim.

The source adds a real persisted34-column `pg_class` declaration, its rowtype/array and actual required types under the existing locked startup owner. It keeps the seven value renderers, unsupported27 values/wildcard and virtual-source-origin gap separate. Actual PG18.6 proves `aclitem`16 bytes/alignment `d`; C1's12-byte declaration and NULL output with TEXT25 are rejected. C2 explicitly rejects unavailable selected type input, including NULL, with0A000. It adds no ACL semantics, heap/MVCC, retained/portal/format2 or unselected CAST analysis policy.

Finite correctness: final native six groups/550 checks pass; three existing whole native tests pass on old and C2. All62 old protocol requests remain:60 before/C2 replies are exact and two self-identity replies gain the actual row; strict9 PG differences remain. The new41-query PG reference passes, product17/41 matches with24 query differences still open. Actual persisted34 attribute rows match six PG fields each, and five type rows match nine PG fields each. The `pg_attribute` SQL route still fails42P01. A real old-image upgrade/cold pair preserves prior metadata and has20 replies byteexact with fresh C2, both normalwait0.

The normal cost control uses before P32C1 `310a2c01` → C2 `280ad4a8` → the identical before program. It retains the inherited full270 timings/27 DB contexts/54 normal Main processes/1098 replies, without taskset/profiler/allocator substitution, parallel native work, extra SQL or compression during sampling. CPU clocks include the whole server process; wall/client CPU include request/envelope buffering through ReadyI. Decoding, full oracle checks, raw file writes and process snapshots occur after timing. Clocks/proc reads are non-atomic; endpoint RSS/IO is not a peak. Natural cache state is used. Other preexisting DB processes are observed and untouched, so this is not an exclusive-host result.

Canonical fixed/single populations are256, overflow128; UPDATE16/DELETE16 each use one statement affecting16 rows. Direct full/id projections use actual post-insert/update populations257/257/129. Despite the inherited cohort label `direct-hit`, `SELECT id FROM perf_fixed` and its peers are **all-row projections without WHERE/index lookup or ORDER BY**. This workload does not time `pg_class` queries or catalog initialization. It is an ordinary SQL regression control for the feature, not a PostgreSQL performance comparison.

Independent readback verifies all270 timed records,96624 rows/386388 cells/27864 NULLs and231,410,943 request/response bytes, all5796 terminal files, all270 raw-three distributions and90 metric comparisons. Root also rederives every median and percentage from raw records (540 checks). No warm/tail/negative sample is excluded.

Across30 named regions, wall directions are8 below both old medians/13 above/9 between; server CPU4 below/16 above/10 between; client CPU8 below/13 above/9 between. A direction compares raw nanosecond medians before table rounding, including tiny differences; it is not statistical significance. Old wall drift ranges−16.45% to+36.82%, server CPU−29.62% to+36.93%. All regressions, including the prior P32 cases, and their causes remain **OPEN**. No general or stable acceleration is established.

Measured wall time (milliseconds; candidate percentages relative to each old median).

| Cohort / scope / region | Old first | C2 | Old return | C2 vs first | C2 vs return | Old drift |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 55.025 | 55.414 | 54.856 | +0.71% | +1.02% | -0.31% |
| canonical-fixed-single/fixed/insert1 | 22.277 | 20.512 | 22.568 | -7.92% | -9.11% | +1.30% |
| canonical-fixed-single/fixed/reinsert_population | 677.136 | 654.409 | 698.395 | -3.36% | -6.30% | +3.14% |
| canonical-fixed-single/fixed/select_after_update | 34.724 | 34.138 | 29.010 | -1.69% | +17.67% | -16.45% |
| canonical-fixed-single/fixed/select_after_vacuum | 33.715 | 35.752 | 28.988 | +6.04% | +23.34% | -14.02% |
| canonical-fixed-single/fixed/truncate | 276.975 | 328.911 | 298.350 | +18.75% | +10.24% | +7.72% |
| canonical-fixed-single/fixed/update16 | 84.095 | 86.145 | 79.945 | +2.44% | +7.75% | -4.93% |
| canonical-fixed-single/fixed/vacuum | 1.941 | 1.926 | 1.815 | -0.77% | +6.12% | -6.49% |
| canonical-fixed-single/single/delete16 | 77.339 | 86.794 | 89.708 | +12.23% | -3.25% | +15.99% |
| canonical-fixed-single/single/insert1 | 23.526 | 107.463 | 20.820 | +356.78% | +416.16% | -11.50% |
| canonical-fixed-single/single/reinsert_population | 1200.887 | 1162.139 | 1173.899 | -3.23% | -1.00% | -2.25% |
| canonical-fixed-single/single/select_after_update | 50.064 | 64.148 | 67.373 | +28.13% | -4.79% | +34.57% |
| canonical-fixed-single/single/select_after_vacuum | 47.555 | 68.621 | 65.066 | +44.30% | +5.46% | +36.82% |
| canonical-fixed-single/single/truncate | 232.182 | 308.062 | 255.270 | +32.68% | +20.68% | +9.94% |
| canonical-fixed-single/single/update16 | 159.660 | 174.236 | 176.559 | +9.13% | -1.32% | +10.58% |
| canonical-fixed-single/single/vacuum | 7.392 | 8.473 | 8.460 | +14.63% | +0.15% | +14.45% |
| canonical-overflow/overflow/delete16 | 71.799 | 79.037 | 78.280 | +10.08% | +0.97% | +9.03% |
| canonical-overflow/overflow/insert1 | 20.027 | 20.048 | 21.732 | +0.10% | -7.75% | +8.51% |
| canonical-overflow/overflow/reinsert_population | 1224.300 | 1116.937 | 1531.007 | -8.77% | -27.05% | +25.05% |
| canonical-overflow/overflow/select_after_update | 50.958 | 52.928 | 67.836 | +3.86% | -21.98% | +33.12% |
| canonical-overflow/overflow/select_after_vacuum | 55.235 | 66.698 | 60.974 | +20.75% | +9.39% | +10.39% |
| canonical-overflow/overflow/truncate | 272.933 | 227.895 | 308.065 | -16.50% | -26.02% | +12.87% |
| canonical-overflow/overflow/update16 | 154.870 | 160.693 | 183.829 | +3.76% | -12.59% | +18.70% |
| canonical-overflow/overflow/vacuum | 10.709 | 10.001 | 10.649 | -6.61% | -6.08% | -0.56% |
| direct-hit/fixed/direct_full_no_order | 22.590 | 23.865 | 20.802 | +5.64% | +14.72% | -7.91% |
| direct-hit/fixed/direct_id_only_no_order | 23.643 | 20.982 | 21.128 | -11.25% | -0.69% | -10.63% |
| direct-hit/overflow/direct_full_no_order | 59.218 | 64.094 | 50.192 | +8.23% | +27.70% | -15.24% |
| direct-hit/overflow/direct_id_only_no_order | 53.900 | 49.219 | 52.678 | -8.69% | -6.57% | -2.27% |
| direct-hit/single/direct_full_no_order | 51.373 | 56.005 | 52.548 | +9.02% | +6.58% | +2.29% |
| direct-hit/single/direct_id_only_no_order | 46.527 | 45.681 | 45.006 | -1.82% | +1.50% | -3.27% |

Measured server process CPU (milliseconds; candidate percentages relative to each old median).

| Cohort / scope / region | Old first | C2 | Old return | C2 vs first | C2 vs return | Old drift |
|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single/fixed/delete16 | 41.808 | 44.646 | 39.371 | +6.79% | +13.40% | -5.83% |
| canonical-fixed-single/fixed/insert1 | 12.376 | 6.919 | 8.710 | -44.09% | -20.56% | -29.62% |
| canonical-fixed-single/fixed/reinsert_population | 601.954 | 593.058 | 569.630 | -1.48% | +4.11% | -5.37% |
| canonical-fixed-single/fixed/select_after_update | 20.774 | 22.728 | 19.967 | +9.41% | +13.83% | -3.88% |
| canonical-fixed-single/fixed/select_after_vacuum | 18.632 | 24.066 | 20.368 | +29.17% | +18.15% | +9.32% |
| canonical-fixed-single/fixed/truncate | 102.970 | 103.103 | 84.420 | +0.13% | +22.13% | -18.01% |
| canonical-fixed-single/fixed/update16 | 72.061 | 72.610 | 68.746 | +0.76% | +5.62% | -4.60% |
| canonical-fixed-single/fixed/vacuum | 1.838 | 1.779 | 1.684 | -3.19% | +5.68% | -8.39% |
| canonical-fixed-single/single/delete16 | 65.926 | 68.746 | 77.587 | +4.28% | -11.39% | +17.69% |
| canonical-fixed-single/single/insert1 | 9.607 | 13.144 | 9.793 | +36.82% | +34.22% | +1.94% |
| canonical-fixed-single/single/reinsert_population | 1059.396 | 1007.230 | 1070.279 | -4.92% | -5.89% | +1.03% |
| canonical-fixed-single/single/select_after_update | 44.373 | 52.581 | 56.279 | +18.50% | -6.57% | +26.83% |
| canonical-fixed-single/single/select_after_vacuum | 42.414 | 55.744 | 55.728 | +31.43% | +0.03% | +31.39% |
| canonical-fixed-single/single/truncate | 119.627 | 129.936 | 124.392 | +8.62% | +4.46% | +3.98% |
| canonical-fixed-single/single/update16 | 128.739 | 142.648 | 130.130 | +10.80% | +9.62% | +1.08% |
| canonical-fixed-single/single/vacuum | 7.167 | 8.153 | 8.133 | +13.76% | +0.25% | +13.47% |
| canonical-overflow/overflow/delete16 | 59.169 | 65.798 | 66.226 | +11.20% | -0.65% | +11.93% |
| canonical-overflow/overflow/insert1 | 9.189 | 10.472 | 11.033 | +13.97% | -5.08% | +20.08% |
| canonical-overflow/overflow/reinsert_population | 1091.806 | 1077.514 | 1143.019 | -1.31% | -5.73% | +4.69% |
| canonical-overflow/overflow/select_after_update | 43.168 | 42.545 | 58.452 | -1.44% | -27.21% | +35.41% |
| canonical-overflow/overflow/select_after_vacuum | 49.209 | 57.948 | 53.101 | +17.76% | +9.13% | +7.91% |
| canonical-overflow/overflow/truncate | 63.745 | 87.300 | 87.285 | +36.95% | +0.02% | +36.93% |
| canonical-overflow/overflow/update16 | 127.354 | 126.752 | 124.881 | -0.47% | +1.50% | -1.94% |
| canonical-overflow/overflow/vacuum | 8.423 | 9.731 | 10.030 | +15.52% | -2.99% | +19.08% |
| direct-hit/fixed/direct_full_no_order | 15.249 | 14.506 | 10.854 | -4.87% | +33.65% | -28.82% |
| direct-hit/fixed/direct_id_only_no_order | 13.627 | 13.323 | 11.142 | -2.23% | +19.57% | -18.23% |
| direct-hit/overflow/direct_full_no_order | 49.162 | 50.666 | 43.171 | +3.06% | +17.36% | -12.19% |
| direct-hit/overflow/direct_id_only_no_order | 41.604 | 44.115 | 42.246 | +6.04% | +4.42% | +1.54% |
| direct-hit/single/direct_full_no_order | 44.509 | 44.509 | 43.184 | +0.00% | +3.07% | -2.98% |
| direct-hit/single/direct_id_only_no_order | 39.059 | 40.536 | 37.946 | +3.78% | +6.83% | -2.85% |


The complete raw-three/min/p50/max/client CPU, per-case old drift and long-tail table is retained inside [pg-class-declaration-20261106.json.gz](benchmarks/pg-class-declaration-20261106.json.gz), 16,696,087 bytes (15.92 MiB), SHA256 `0b56d74a328f44120ee1a216c9e323e8317a80b552bdb1d80fc291e7afca67c2`. The archive contains full selected nonELF source/SQL/wire/data/native and rejected-first-attempt bytes, directory modes and symlink texts. Executable/core ELFs are explicit SHA references kept locally, not embedded executable images. The inner content-addressed JSON is XZ-compressed, then base64 JSON and gzip; independent decoding compares every embedded file byte and every kept ELF reference before cleanup. No raw benchmark JSON is added to Git.

From the repository root, recover the complete table with Python standard library only (the exact stdout bytes are tested against the original table):

```sh
python3 - <<'PY'
import base64, gzip, json, lzma, sys
with gzip.open("docs/benchmarks/pg-class-declaration-20261106.json.gz", "rb") as stream:
    outer = json.load(stream)
bundle = json.loads(lzma.decompress(base64.b64decode(outer["payload_xz_base64"])))
entry = bundle["artifacts"]["batch/class-declaration-cost27-C2-v1/all-performance-tables-v3.md"]
sys.stdout.buffer.write(base64.b64decode(bundle["blobs"][entry["blob"]]))
PY
```

Original273 status objects/282 prior history records,725 protected Markdown records and32 prior archives remain unchanged; history advances to283, Markdown paths stay754. Default425 and the old baseline remain intact. This batch does not close CAT02, the goal or any deferred issue.
After independent byte reconstruction, cleanup removed 19,969 generated duplicate regular files: 1,396,915,572 logical bytes; their single-link st_blocks sum was 1,407,332,352 bytes (1.311 GiB), not an exclusive whole-filesystem free-space measurement. All214 ELF references, directories, source/trace/SQL metadata, normal20-candidate working data, functional working data and the final PG reference working image remain. Postcheck verifies the exact removal whitelist and all4,932 remaining embedded files.

<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 普通日志追加与实际查询成本（2026-11-06，P32-C1）

<a id="legacy-log-append-20261106"></a>

普通日志 `::log` 仍按 time、四个空格、user、四个空格、operation 和换行追加原始字节；每次实际打开当前路径，以 `O_APPEND` 和六段 `writev` 写入，EINTR 重试、正短写继续消费未写后缀，零进展或其他写错误停止并关闭。打开失败仍输出 `error 1` 并返回0；权限、已有前缀、rename/unlink 与真实 symlink 重新解析保持。`getTime`、`auditLog` 正文字节未变，没有持久缓存日志FD或改服务端协议发送。

正常CMake C++17/O2、旧版→C1→同旧版各三轮的固定矩阵完整结束：27个数据库上下文、54个正常Main、270个计时区间，校验1098响应、96624行/386388单元/27864个NULL。30案例的服务器CPU为16项低于两旧、6项高于两旧、8项居间；墙钟为13低/5高/12居间。固定/单BYTEA直接仅id查询有有限收益，但多LOB直接全列和仅id的服务器CPU均高于两旧，其中仅id墙钟也回退，全列墙钟居间。全部负向、旧回测漂移及长尾保留，未证明一致SQL提速、稳定吞吐或PG速度等价；原因仍OPEN，P30原负向证据保持。

正式只新增1个CPP回归，最终源码 `d2669466…`。完整61对象/core加本代stub的实际配对中，before为6组普通行为通过、7组新机制能力缺失NOTRUN；C1共13组、105项检查通过，测试作用域内观察38次writev封装层命中（含控制返回，非物理写盘次数），两代最终测试编译stderr均0字节。覆盖NUL/UTF8/空串/换行/长值、追加、轮转、权限、symlink、实际打开错误和有限EINTR/短写/零进展/EIO控制；128条并发短记录比较完整无序集合，不宣称任意大记录跨短写原子性。两项既有TRUNCATE测试两代均正常0，完整stdout/stderr逐字节一致，实际分别记录5次/3次TRUNCATE日志。

原58个严格协议目标及4个额外冷锚控制全部保留，两代产品各完成62请求、目标各53/58，原9项普通差异未关闭，strict CLI仍为1；全部62请求/响应原始字节一致。每代实际207行/848单元/156个NULL，4个初始/冷Main均取得真实子进程wait0、PID/组消失。复用此前真实PG18.6参考，没有重跑PG，没有把目录来源元数据或选中错误的TEZ/EZ差异改成PASS或暂缓。

本批before为已验证P30-C2正常程序（SHA `472e093d40b8…`），C1仅重编main和DdlExecutor两个TU，保持C++17/O2。固定/单BYTEA两表9个常规上下文、多LOB单表9个常规上下文、共享三表的9个直接查询上下文，均按旧版→C1→同旧版运行三轮；初始人口256/256/128。INSERT1、UPDATE16、全SELECT、DELETE16、VACUUM、全SELECT、TRUNCATE及原人口重新写入全部保留，UPDATE16/DELETE16各是一条影响16行的SQL。直接仅id查询执行`SELECT id FROM perf_fixed`等无WHERE/ORDER查询，更新后分别返回257/257/129行；表中的direct-hit是旧工作量标识，不表示索引命中。

wall从发送完整SimpleQuery到Ready，包含帧头处理和原始块复制；完整解码、值校验、hash及保存wire在计时后。serverCPU计整个Main，clientCPU另列；时钟与/proc端点非原子，RSS/IO端点不是峰值。正常窗口不绑CPU、不清OS缓存，setup自然预热；本批没有同时进行其他SQL、C++编译、profile或压缩。各格只有3样本，旧版前后墙钟漂移范围−27.073%至+43.946%，不能据此证明频率/负载等回退原因。客户端CPU方向另为13低/10高/7居间，全部原样保留。

下表包括全部30案例，常规ORDER BY流程24案例与独立无ORDER直接查询6案例分列，不互换或筛选。三列耗时均为各自原3样本中位数，单位ms；变化=`(C1/旧−1)×100`，正值增加、负值减少；旧漂移=`(回测/首次−1)×100`。客户端接收墙钟和服务端进程CPU不是profile样本；保留完整返回、正常冷开和所有失败观察。

### 全部30案例：查询/操作墙钟

| 批次 | 表 | 操作 | 旧首次ms | C1 ms | 旧回测ms | 对首次变化% | 对回测变化% | 旧回测漂移% |
|---|---|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single | fixed | delete16 | 46.653655 | 44.508280 | 48.186311 | -4.599 | -7.633 | +3.285 |
| canonical-fixed-single | fixed | insert1 | 18.360097 | 16.984138 | 13.389397 | -7.494 | +26.848 | -27.073 |
| canonical-fixed-single | fixed | reinsert_population | 596.261920 | 616.169480 | 620.768775 | +3.339 | -0.741 | +4.110 |
| canonical-fixed-single | fixed | select_after_update | 25.209886 | 25.822615 | 28.042068 | +2.431 | -7.915 | +11.234 |
| canonical-fixed-single | fixed | select_after_vacuum | 26.534910 | 26.613811 | 29.181030 | +0.297 | -8.798 | +9.972 |
| canonical-fixed-single | fixed | truncate | 290.862646 | 266.510508 | 408.287939 | -8.372 | -34.725 | +40.371 |
| canonical-fixed-single | fixed | update16 | 68.860966 | 67.970559 | 85.788637 | -1.293 | -20.770 | +24.582 |
| canonical-fixed-single | fixed | vacuum | 1.893278 | 1.364028 | 1.673151 | -27.954 | -18.475 | -11.627 |
| canonical-fixed-single | single | delete16 | 67.906581 | 79.579625 | 71.348408 | +17.190 | +11.537 | +5.068 |
| canonical-fixed-single | single | insert1 | 18.896445 | 22.736136 | 16.780296 | +20.320 | +35.493 | -11.199 |
| canonical-fixed-single | single | reinsert_population | 1081.484412 | 1077.394995 | 1146.464049 | -0.378 | -6.025 | +6.008 |
| canonical-fixed-single | single | select_after_update | 55.965436 | 54.688421 | 53.757097 | -2.282 | +1.732 | -3.946 |
| canonical-fixed-single | single | select_after_vacuum | 60.672890 | 55.166821 | 59.175170 | -9.075 | -6.774 | -2.469 |
| canonical-fixed-single | single | truncate | 473.266988 | 465.002277 | 399.481030 | -1.746 | +16.402 | -15.591 |
| canonical-fixed-single | single | update16 | 158.124178 | 148.173800 | 144.325444 | -6.293 | +2.666 | -8.727 |
| canonical-fixed-single | single | vacuum | 7.314450 | 8.250711 | 6.921662 | +12.800 | +19.201 | -5.370 |
| canonical-overflow | overflow | delete16 | 78.195697 | 75.511622 | 90.568367 | -3.433 | -16.625 | +15.823 |
| canonical-overflow | overflow | insert1 | 21.468954 | 18.460315 | 29.669677 | -14.014 | -37.781 | +38.198 |
| canonical-overflow | overflow | reinsert_population | 1252.960154 | 1214.638759 | 1185.215609 | -3.058 | +2.483 | -5.407 |
| canonical-overflow | overflow | select_after_update | 66.943787 | 60.998071 | 70.587013 | -8.882 | -13.585 | +5.442 |
| canonical-overflow | overflow | select_after_vacuum | 57.615791 | 62.793941 | 62.073291 | +8.987 | +1.161 | +7.737 |
| canonical-overflow | overflow | truncate | 179.765263 | 203.165049 | 229.206616 | +13.017 | -11.362 | +27.503 |
| canonical-overflow | overflow | update16 | 174.518641 | 160.537580 | 251.211902 | -8.011 | -36.095 | +43.946 |
| canonical-overflow | overflow | vacuum | 8.672119 | 8.882708 | 11.768352 | +2.428 | -24.520 | +35.703 |
| direct-hit | fixed | direct_full_no_order | 25.308857 | 21.914329 | 23.179452 | -13.412 | -5.458 | -8.414 |
| direct-hit | fixed | direct_id_only_no_order | 22.758355 | 20.700487 | 25.621755 | -9.042 | -19.207 | +12.582 |
| direct-hit | overflow | direct_full_no_order | 56.809573 | 56.151831 | 47.785452 | -1.158 | +17.508 | -15.885 |
| direct-hit | overflow | direct_id_only_no_order | 46.845459 | 49.554183 | 47.934762 | +5.782 | +3.378 | +2.325 |
| direct-hit | single | direct_full_no_order | 55.641604 | 48.205033 | 44.394162 | -13.365 | +8.584 | -20.214 |
| direct-hit | single | direct_id_only_no_order | 45.475530 | 42.743512 | 47.182188 | -6.008 | -9.408 | +3.753 |

### 全部30案例：服务器进程CPU

| 批次 | 表 | 操作 | 旧首次ms | C1 ms | 旧回测ms | 对首次变化% | 对回测变化% | 旧回测漂移% |
|---|---|---|---:|---:|---:|---:|---:|---:|
| canonical-fixed-single | fixed | delete16 | 35.720972 | 34.029949 | 40.133365 | -4.734 | -15.208 | +12.352 |
| canonical-fixed-single | fixed | insert1 | 9.805053 | 8.319537 | 6.532070 | -15.151 | +27.364 | -33.381 |
| canonical-fixed-single | fixed | reinsert_population | 570.403910 | 568.771393 | 590.589725 | -0.286 | -3.694 | +3.539 |
| canonical-fixed-single | fixed | select_after_update | 14.430047 | 17.467122 | 19.146805 | +21.047 | -8.773 | +32.687 |
| canonical-fixed-single | fixed | select_after_vacuum | 17.493728 | 17.262458 | 20.543718 | -1.322 | -15.972 | +17.435 |
| canonical-fixed-single | fixed | truncate | 90.766686 | 94.787844 | 107.003514 | +4.430 | -11.416 | +17.889 |
| canonical-fixed-single | fixed | update16 | 59.160480 | 59.566230 | 78.256099 | +0.686 | -23.883 | +32.278 |
| canonical-fixed-single | fixed | vacuum | 1.781049 | 1.232617 | 1.517644 | -30.793 | -18.781 | -14.789 |
| canonical-fixed-single | single | delete16 | 55.684873 | 67.567525 | 62.393768 | +21.339 | +8.292 | +12.048 |
| canonical-fixed-single | single | insert1 | 10.059306 | 10.849717 | 9.738803 | +7.858 | +11.407 | -3.186 |
| canonical-fixed-single | single | reinsert_population | 1053.301526 | 1034.377984 | 1103.494397 | -1.797 | -6.263 | +4.765 |
| canonical-fixed-single | single | select_after_update | 46.799848 | 46.494605 | 46.808623 | -0.652 | -0.671 | +0.019 |
| canonical-fixed-single | single | select_after_vacuum | 54.781644 | 49.573408 | 50.144655 | -9.507 | -1.139 | -8.464 |
| canonical-fixed-single | single | truncate | 138.515437 | 133.393926 | 136.857065 | -3.697 | -2.530 | -1.197 |
| canonical-fixed-single | single | update16 | 131.688465 | 127.961166 | 128.069559 | -2.830 | -0.085 | -2.748 |
| canonical-fixed-single | single | vacuum | 7.025082 | 7.892161 | 6.510332 | +12.343 | +21.225 | -7.327 |
| canonical-overflow | overflow | delete16 | 68.404652 | 67.032039 | 70.028724 | -2.007 | -4.279 | +2.374 |
| canonical-overflow | overflow | insert1 | 12.502999 | 9.647244 | 12.102614 | -22.841 | -20.288 | -3.202 |
| canonical-overflow | overflow | reinsert_population | 1143.445550 | 1145.370260 | 1108.550287 | +0.168 | +3.321 | -3.052 |
| canonical-overflow | overflow | select_after_update | 54.480986 | 53.353869 | 62.946410 | -2.069 | -15.239 | +15.538 |
| canonical-overflow | overflow | select_after_vacuum | 51.638020 | 56.802074 | 57.736283 | +10.000 | -1.618 | +11.810 |
| canonical-overflow | overflow | truncate | 75.358964 | 75.022532 | 91.905863 | -0.446 | -18.370 | +21.957 |
| canonical-overflow | overflow | update16 | 146.035519 | 130.160953 | 144.080814 | -10.870 | -9.661 | -1.339 |
| canonical-overflow | overflow | vacuum | 8.331543 | 8.600265 | 10.790086 | +3.225 | -20.295 | +29.509 |
| direct-hit | fixed | direct_full_no_order | 15.119406 | 13.748687 | 11.300508 | -9.066 | +21.664 | -25.258 |
| direct-hit | fixed | direct_id_only_no_order | 14.154106 | 13.051541 | 14.213758 | -7.790 | -8.177 | +0.421 |
| direct-hit | overflow | direct_full_no_order | 44.506625 | 47.071040 | 36.533296 | +5.762 | +28.844 | -17.915 |
| direct-hit | overflow | direct_id_only_no_order | 37.708028 | 43.367602 | 38.911726 | +15.009 | +11.451 | +3.192 |
| direct-hit | single | direct_full_no_order | 46.917075 | 42.275101 | 36.249776 | -9.894 | +16.622 | -22.736 |
| direct-hit | single | direct_id_only_no_order | 38.489230 | 37.022726 | 39.474735 | -3.810 | -6.212 | +2.560 |

归档实际8,381,352B（7.99MiB），SHA `e4f81ec92c054b80c434e5558d7e95df2093afaaa0bacc65c8eecde7f89b7eae`；共32份，旧31份字节保持。全部原三样本、min/p50/max、客户端CPU及长尾在归档的 `legacy-read-log-cost27-C1-v1/all-performance-tables-v3.md` 与原始回执中，未以本表中位数替代。

归档保存15,827个非ELF文件的完整字节、780个目录和4个符号链接记录。程序/core/对象作为137个精确SHA引用留在工作区，未嵌入归档；回放程序仍需要对应的执行文件和依赖。可用Python标准库从归档取出完整原始分布，无需依赖本机build目录：

```python
from pathlib import Path
import base64, gzip, json, lzma
outer = json.loads(gzip.decompress(Path("docs/benchmarks/legacy-log-append-20261106.json.gz").read_bytes()))
bundle = json.loads(lzma.decompress(base64.b64decode(outer["payload"])))
record = next(r for r in bundle["files"] if r["path"].endswith("/legacy-read-log-cost27-C1-v1/all-performance-tables-v3.md"))
print(base64.b64decode(bundle["blobs"][record["blob"]]["data"]).decode())
```

当前仅生产头 `src/common/logs.h` 改动，私有正常C1程序SHA `310a2c01d84c1cd92dcf05ee5a5825fecface8dc0446adc312269be2bd144eb4`；默认安装程序仍为 `425165af…`，现有服务未替换。源248/头177，正式测试2393（顶层CPP1126、含子目录总CPP1131、子目录5），登记684个driver/664个独立E2E；754 Markdown路径、725保护记录不变。原273项状态仍22 complete/166 partial/70 unverified/15 deferred_by_user，旧281条历史原文保持，仅追加第282条partial；IDX-07仍unverified且evidence=[]。不扩用户暂停范围，无分支切换、推送或默认程序晋升，Goal active。

[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与[可逆原始证据](benchmarks/legacy-log-append-20261106.json.gz)。以下各批历史原文保持。

归档逐文件校验完成后，本轮仅清理13,510个已归档的正常测试生成副本，nlink=1分配空间1,187,860,480字节（约1.11GiB）。独立新进程后验确认，其余2,317个非ELF工件、137个精确引用、780个目录和4个链接/目标保持；`20-candidate/data`完整工作库及全部协议数据保留，源码、程序和对象未删除。

<!-- CURRENT_FIXED_ID_PROFILE_20261106 -->

## 主键查询回退的执行路径与CPU采样（2026-11-06，P31）

<a id="fixed-id-query-profile-20261106"></a>

本批完成独立诊断，生产实现仍为P30-C2，没有新候选或提速验收。下方P30的全部540个正常计时点保持原样，固定表主键查询服务器CPU中位数仍为10.705477→13.508781→10.734883ms（C2相对前后旧版+26.1857%/+25.8400%）；原因仍为OPEN。

### 实际执行与正确性

复用P30第20个上下文实际正常关闭后的三表状态，而非另建较简单夹具；原278个文件、8个目录及18,360,752字节逐一核对完整路径、长度和SHA后复制。固定/单BYTEA/多LOB三表更新后分别257/257/129行。正常C++17/O2程序按P29-C2（b53f853b356e…）→P30-C2（472e093d40b8…）→同一P29-C2运行，每代仅对原样`SELECT id FROM perf_fixed`采样512次；前后另核三表完整值和NULL。3个实际Main及外层全部正常wait0，PID与进程组消失，零重试。

独立结构解码器重解全部1,575份原始响应：1,536次采样查询的394,752个ID单元全部相符；合计400,179行观测、412,533个单元、1,230个NULL，18次前后三表完整模型和1,560次物理SELECT的全部7项RowDescription字段、命令标签及ReadyI均通过。原始wire保留，不以摘要代替全值。6次实际EXPLAIN均为TableScan→Project，真实采样栈也包含这两个程序函数；这些证据没有单独证明位图分支命中。

采样使用本地gperftools 2.15 `libprofiler`，只向实际持有的私有子进程发送经PID/start/exe/pgid及handler检查的SIGUSR2，周期1000微秒；无分配器替换。全部6次端点各4个线程的CPU亲和性均为8。另存5个既有外部服务的同身份CPU端点（各角色约0–0.02秒），从未向它们发信号，也不据此宣称整机空闲。采样期间本批未执行编译、其他SQL或压缩。[工具官方说明](https://gperftools.github.io/gperftools/cpuprofile.html)。

### 有限采样观察

以下总量是原始经典profile中的样本，已与6个实际退出0的pprof输出交叉核对。程序命名要求真实ELF映射、PT_LOAD重定位及声明尺寸内的符号归属；同地址别名候选全部保留，共享库不猜最近符号。百分比包含各函数的子调用，互相重叠，不能相加。

| 角色 | 总样本 | Project自身/含子调用 | log含子调用 | sendDataRow含子调用 | 文件身份检查含子调用 |
|---|---:|---:|---:|---:|---:|
| before-first | 6796 | 13/730 | 19.70% | 23.43% | 6 |
| after | 6990 | 16/727 | 24.36% | 21.73% | 13 |
| before-return | 8522 | 10/893 | 24.67% | 20.79% | 8 |

逐行日志和结果发送是三个角色共同的后续优化候选。当前普通SELECT逐行调用`log`，该函数每次打开追加流、写入并flush/关闭；结果发送逐行调用`sendDataRow`。这里只依据实际程序调用子树及源码确定关注点，没有把共享库中的地址猜作某个具体内存或系统函数，也没有证明这些共同工作导致P30回退。Project自身样本少、文件身份检查样本少，均不证明内联/未采中工作为零。CPU8、分析器、前端验证/写盘间隙和后台活动与下方正常不绑CPU的对照不同，样本总量不能替换正常耗时或关闭负例。

### 目录补全的真实依赖与证据

P30原58个严格目标及9项差异全部保留，未再跑参考或改期望。pg_class自身目录声明和普通来源元数据仍未实现。固定PostgreSQL18.6源码要求34个真实属性、连续attnum1..34及实际类型声明；当前7个可查询字段不能伪装完整34列。调查发现缺失的pg_node_tree/aclitem/数组类型若直接加入当前目录，通用声明解析可能使其进入未知CAST原字节回退，不能仅靠不注册别名阻止。因此本批只保存完整声明矩阵、依赖审计和后续严格验收方案，没有补虚假OID/字段，也未应用生产补丁、测试新类型或扩大暂停范围。[固定pg_class声明](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/catalog/pg_class.h)。

完整驱动、原始profile/maps、同程序尺寸符号表与汇编、全部wire、完整闭库树、首轮工具记录和带许可调研保存在[可逆压缩证据](benchmarks/fixed-id-query-profile-20261106.json.gz)（4.26MiB，SHA `31e2bb007fa648a4133c108e0ecf2bdb54d5eb57f6d40197abb2009b64cea451`）；相同内容按SHA去重，内部JSON使用XZ、外层JSON使用gzip，每个路径的长度、模式与摘要单独保留，旧30份归档保持。ELF及父批源码/依赖按明确SHA引用，未把可重建程序重复装入Git。实际代码/测试/默认程序保持，Goal active。以下P30与更早段落仍是各自代别的真实记录。

首次普通gzip触发49,000,000字节上限，发布前拒绝；改用更紧凑的无损容器后通过独立完整解码检查，首轮失败保留。核验后仅清理5,101个已归档生成副本，分配空间243,703,808字节（232.41MiB）；443份其余工件逐字保持，118个目录保留，未删除ELF、对象、源码或父批工作库。

<!-- CURRENT_ORDINARY_PROJECT_BITMAP_20261106 -->

## 当前行NULL位图复用与按需求捕获（2026-11-06，P30-C2）

<a id="ordinary-project-bitmap-20261106"></a>

普通物理表的直接投影现在按选中列需求，复用当前行已有的完整NULL位图；局部作用域在正常或异常退出时恢复，只选非空物理列时不再复制。既有值提取、虚拟列求值顺序、TOAST处理、文件身份校验函数和暂停范围保持。

正常CMake C++17/O2的最终C2，在固定/单BYTEA/多LOB三类直接完整查询中，墙钟中位数对前后两次旧版降低约9%–32%，服务器CPU降低约11%–35%；这是本机各三样本的有限结果。主键投影负控仍有回退，固定表服务器CPU为+26.19%/+25.84%，单BYTEA主键查询墙钟为+8.03%/+3.65%。跳过多余复制没有消除这些回退，其根因未证，不能据此归因频率、负载或噪声，也不能称全库提速。

### 原因、功能与边界

本批仅优化真实物理 TableScan→Project 的当前行 NULL 读取：选中列需要 NULL 信息时，借用该行已拥有的完整位图，并在局部作用域退出或异常时恢复；C2 在既未选中可空物理列、也未选中虚拟列时不复制位图。新增的普通 native 回归在 before 执行13组并明确保留6组能力缺失 NOTRUN，C1、最终C2各19组全部真实通过。NULL、空串、文本 NULL、TOAST及标记字面量、相反位图、虚拟列需求、嵌套和异常恢复均保留完整值与真实RID观察。外部 current-file lookup 的有限机制计数不等于 SQL 耗时或整体提速。

真实 PostgreSQL 18.6 参考完成全部58个固定目标及7个控制，观测205行、846个单元、156个NULL；before、C1、C2分别完成全部58个目标及4个冷锚控制，各为53/58，观测204行、845个单元、156个NULL。三代产品全部62组查询的请求、响应及差异保持原始字节一致。既有9项普通差异未关闭：4个初始目录源锚和4个冷目录锚涉及 pg_class 自身行及目录RowDescription源OID/列号，另一个是选中虚拟列除零时PG发送TEZ而产品发送EZ。它们不是PASS，也不是用户暂缓；没有删例或放宽期望。PG启用fsync，所有初始及冷上下文都取得实际子进程wait0，PID和进程组消失。

六个既有native测试在before、C1、C2三代共18次全部真实通过，stdout/stderr逐字节一致；C1与C2新增native的完整stdout也一致。正式只新增1个CPP测试，完整58条portable协议草稿仍以known_gap形式保留，未运行、未注册，不能计为通过。本批不改参数帧、provider生命周期、retained规划或暂停的输出format2边界，不宣称目录/协议错误帧已与PG等价，也不据这些功能回归单独宣称全查询性能提升、默认程序晋升或服务重启。

调研采用原始、带版权的固定源码：PostgreSQL18.6的tuple slot将当前属性值与NULL状态共同持有并在换行时重置；SQLite record格式用serial type0区别NULL、12/13区别空BLOB/TEXT；DuckDB向量把validity与同一逻辑行索引配对，RowIsValid极性与本地isNull相反。本地实现仅借用“同一实际行拥有其NULL状态”的原则，没有移植代码或宣称架构、性能等同。原件、14处源码锚和许可随压缩证据保留。 [PostgreSQL tuple slots](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/executor/tuptable.h)。 [SQLite record format](https://www.sqlite.org/fileformat2.html)。 [DuckDB validity](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/include/duckdb/common/types/validity_mask.hpp)。

外部真实BufferPool文件身份校验观察器先以实际缓存命中作正控（每代1次），再观察同一8行：完整nullable投影24→0、nullable子集12→0，TOAST例62→22；Filter/缺位图fallback24→24、非空列0→0。C1/C2各92次行观测、320个单元、NULL位图与真实RID逐字相同。它们是有限调用次数，不是磁盘读次数或SQL计时。C2只取消可证明不需要的复制；负控回退仍在，不把该代码事实当成回退根因已解决。

### 固定性能实验

before是父提交P29C2正常程序（SHA b53f853b356e9c8cf1e8fedf9832201d9bf705cf81d817b35314b4660e334594），本批C1为f9de172592815d62b6610a67654bd1365bb3465c7c3b85254b8945f78e0b536c，最终C2为472e093d40b833bb11715765138d5b4a0ca8002c725afab9d796b1d3a4e2eff3。C1正常构建2个源文件actual0，C2仅重编ExecutionPlan一个源文件actual0；未改变编译标准和O2。

C1与C2各独立执行原样27上下文：canonical固定/单BYTEA两表9上下文（N256），多LOB单表9上下文（N128），每表原8操作完整保留：INSERT1、UPDATE16、全SELECT、DELETE16、VACUUM、全SELECT、TRUNCATE、重新写入原人口；每个UPDATE16/DELETE16是一条影响16行的SQL。另direct9上下文保相同三表catalog/更新后人口，每表完整无ORDER查询与只选id负控。每个角色各3轮before→candidate→same-before，共270计时点，未筛选或重采样。canonical原ORDER BY通常使Project下游为Sort，不据该组的差值断言位图复用命中。两代DELETE均是真CZ，同P28额外空T的旧工作量差别不适用。

每个候选矩阵54次Main上下文均真实正常0并PID/组退出、27个终态树；1098份完整响应逐字段复核，共96624次行观测、386388单元、27864个NULL，单份原始request/response合231410943字节。原始wire每份只存一次，trace只存元数据/计数/摘要，独立reducer重新解码完整wire、核所有原始值而非信任摘要；不再为只读投影加native clone。C1和C2的原始540个计时点均保留，两个实际外层与两个独立reducer全部退出0。

wall从发出完整SimpleQuery到收到Ready，含收发、帧头识别和原始块复制；完整字段解码、值oracle、hash和写盘在计时后。serverCPU是真实整个Main进程，clientCPU另列，时钟与/proc括号不是原子同刻。建表、人口检查自然预热，不是冷查询；cold重启只用于另行正确性复验。正常窗口无编译、分析器、压缩或竞争SQL，不绑CPU、不清OS缓存。每格只有3样本，保留raw/min/p50/max和旧版回测漂移，不作显著性、稳定吞吐或与PG速度比较。

### 最终C2直接完整查询（所有百分比为候选相对该次旧版，负数表示减少）

| schema | rows after UPDATE | clock | before-first p50 ms | C2 p50 ms | before-return p50 ms | C2 vs first | C2 vs return | old-return drift |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| fixed | 257 | wall_ns | 28.718385 | 23.645419 | 26.345005 | -17.664524% | -10.247051% | -8.264323% |
| fixed | 257 | server_process_cpu_ns | 19.857274 | 13.545056 | 20.030613 | -31.787938% | -32.378225% | +0.872924% |
| single | 257 | wall_ns | 65.476482 | 45.241063 | 66.706389 | -30.904866% | -32.178816% | +1.878395% |
| single | 257 | server_process_cpu_ns | 57.933788 | 37.931080 | 57.571894 | -34.526843% | -34.115282% | -0.624668% |
| overflow | 129 | wall_ns | 51.951237 | 47.097845 | 55.293306 | -9.342207% | -14.821796% | +6.433088% |
| overflow | 129 | server_process_cpu_ns | 43.615965 | 38.873304 | 50.295550 | -10.873681% | -22.710252% | +15.314541% |

### 最终C2全部30种case的分布方向

| cohort | clock | lower than both | higher than both | between/equal |
|---|---|---:|---:|---:|
| canonical-fixed-single | wall_ns | 6 | 5 | 5 |
| canonical-fixed-single | server_process_cpu_ns | 3 | 3 | 10 |
| canonical-fixed-single | client_process_cpu_ns | 4 | 3 | 9 |
| canonical-overflow | wall_ns | 2 | 1 | 5 |
| canonical-overflow | server_process_cpu_ns | 4 | 1 | 3 |
| canonical-overflow | client_process_cpu_ns | 4 | 2 | 2 |
| direct-hit | wall_ns | 3 | 2 | 1 |
| direct-hit | server_process_cpu_ns | 3 | 1 | 2 |
| direct-hit | client_process_cpu_ns | 0 | 3 | 3 |

全30种case合计：wall11低/8高/11在中，serverCPU10低/5高/15在中；不能把3个完整投影的收益外推为所有数据库操作变快。id-only仍包含整个TableScan/TOAST/元数据等公共成本，不将不同SQL间的差值当成独占位图复制CPU。

### 最终C2所有同时慢于前后旧版的指标

| cohort | scope | case | clock | C2 vs first | C2 vs return |
|---|---|---|---|---:|---:|
| canonical-fixed-single | single | delete16 | wall_ns | +17.692822% | +4.569202% |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | +9.002969% | +13.438090% |
| canonical-fixed-single | single | reinsert_population | wall_ns | +10.994200% | +8.595565% |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | +7.140620% | +12.186056% |
| canonical-fixed-single | single | select_after_update | wall_ns | +6.117702% | +10.635092% |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | +6.924272% | +12.988533% |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | +5.121606% | +3.413773% |
| canonical-fixed-single | single | update16 | wall_ns | +5.579067% | +13.126636% |
| canonical-fixed-single | single | vacuum | wall_ns | +7.585513% | +4.268881% |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | +5.657442% | +2.304187% |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | +8.422270% | +8.712459% |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | +6.166505% | +5.322946% |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | +13.875960% | +8.031687% |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | +0.062652% | +5.454153% |
| canonical-overflow | overflow | vacuum | wall_ns | +2.145571% | +10.589864% |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | +18.449787% | +6.416175% |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | +26.185699% | +25.840039% |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | +1.361221% | +1.598482% |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | +28.633672% | +26.597675% |
| direct-hit | single | direct_id_only_no_order | wall_ns | +8.026427% | +3.651565% |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | +5.231742% | +5.317329% |

### 最终C2完整raw3/min/p50/max及全部比较

#### P30 finite ordinary operation and direct projection costs

Each raw value is retained from three independent contexts. Canonical ORDER BY samples and separately named direct no-ORDER controls stay separate. No exclusions, stable ratio, PG speed, or whole-database claim. Both generations use P29C2-compatible command-only DELETE.

| Cohort | Scope | Region | Clock | Role | raw3 ms | min ms | p50 ms | max ms |
|---|---|---|---|---|---|---:|---:|---:|
| canonical-fixed-single | fixed | delete16 | wall_ns | before-first | 61.678139, 47.959509, 49.234751 | 47.959509 | 49.234751 | 61.678139 |
| canonical-fixed-single | fixed | delete16 | wall_ns | candidate | 45.129978, 45.689278, 44.389693 | 44.389693 | 45.129978 | 45.689278 |
| canonical-fixed-single | fixed | delete16 | wall_ns | before-return | 55.145040, 52.993271, 66.821127 | 52.993271 | 55.145040 | 66.821127 |
| canonical-fixed-single | fixed | delete16 | server_process_cpu_ns | before-first | 55.403690, 38.403856, 39.378469 | 38.403856 | 39.378469 | 55.403690 |
| canonical-fixed-single | fixed | delete16 | server_process_cpu_ns | candidate | 37.629844, 38.656983, 37.068031 | 37.068031 | 37.629844 | 38.656983 |
| canonical-fixed-single | fixed | delete16 | server_process_cpu_ns | before-return | 47.247166, 44.682215, 53.984623 | 44.682215 | 47.247166 | 53.984623 |
| canonical-fixed-single | fixed | delete16 | client_process_cpu_ns | before-first | 0.228425, 0.276508, 0.218847 | 0.218847 | 0.228425 | 0.276508 |
| canonical-fixed-single | fixed | delete16 | client_process_cpu_ns | candidate | 0.200354, 0.211597, 0.198568 | 0.198568 | 0.200354 | 0.211597 |
| canonical-fixed-single | fixed | delete16 | client_process_cpu_ns | before-return | 0.205621, 0.205673, 0.233710 | 0.205621 | 0.205673 | 0.233710 |
| canonical-fixed-single | fixed | insert1 | wall_ns | before-first | 15.939240, 16.690000, 28.546039 | 15.939240 | 16.690000 | 28.546039 |
| canonical-fixed-single | fixed | insert1 | wall_ns | candidate | 14.120781, 14.561457, 22.928859 | 14.120781 | 14.561457 | 22.928859 |
| canonical-fixed-single | fixed | insert1 | wall_ns | before-return | 17.752418, 22.386141, 41.214144 | 17.752418 | 22.386141 | 41.214144 |
| canonical-fixed-single | fixed | insert1 | server_process_cpu_ns | before-first | 7.152671, 8.902757, 15.849176 | 7.152671 | 8.902757 | 15.849176 |
| canonical-fixed-single | fixed | insert1 | server_process_cpu_ns | candidate | 6.451646, 7.197653, 7.565783 | 6.451646 | 7.197653 | 7.565783 |
| canonical-fixed-single | fixed | insert1 | server_process_cpu_ns | before-return | 8.956039, 13.237487, 13.959768 | 8.956039 | 13.237487 | 13.959768 |
| canonical-fixed-single | fixed | insert1 | client_process_cpu_ns | before-first | 0.194317, 0.213923, 0.241227 | 0.194317 | 0.213923 | 0.241227 |
| canonical-fixed-single | fixed | insert1 | client_process_cpu_ns | candidate | 0.191214, 0.204881, 0.187067 | 0.187067 | 0.191214 | 0.204881 |
| canonical-fixed-single | fixed | insert1 | client_process_cpu_ns | before-return | 0.179119, 0.214007, 0.218726 | 0.179119 | 0.214007 | 0.218726 |
| canonical-fixed-single | fixed | reinsert_population | wall_ns | before-first | 626.361082, 594.085105, 668.499102 | 594.085105 | 626.361082 | 668.499102 |
| canonical-fixed-single | fixed | reinsert_population | wall_ns | candidate | 623.255309, 622.993373, 619.268779 | 619.268779 | 622.993373 | 623.255309 |
| canonical-fixed-single | fixed | reinsert_population | wall_ns | before-return | 613.563893, 642.307962, 681.554662 | 613.563893 | 642.307962 | 681.554662 |
| canonical-fixed-single | fixed | reinsert_population | server_process_cpu_ns | before-first | 590.668757, 563.044478, 541.551423 | 541.551423 | 563.044478 | 590.668757 |
| canonical-fixed-single | fixed | reinsert_population | server_process_cpu_ns | candidate | 586.611211, 582.481644, 594.942187 | 582.481644 | 586.611211 | 594.942187 |
| canonical-fixed-single | fixed | reinsert_population | server_process_cpu_ns | before-return | 576.873186, 609.508000, 587.979871 | 576.873186 | 587.979871 | 609.508000 |
| canonical-fixed-single | fixed | reinsert_population | client_process_cpu_ns | before-first | 0.215184, 0.214051, 0.219873 | 0.214051 | 0.215184 | 0.219873 |
| canonical-fixed-single | fixed | reinsert_population | client_process_cpu_ns | candidate | 0.180521, 0.218750, 0.227297 | 0.180521 | 0.218750 | 0.227297 |
| canonical-fixed-single | fixed | reinsert_population | client_process_cpu_ns | before-return | 0.222893, 0.225348, 0.223470 | 0.222893 | 0.223470 | 0.225348 |
| canonical-fixed-single | fixed | select_after_update | wall_ns | before-first | 59.549066, 26.835633, 30.648422 | 26.835633 | 30.648422 | 59.549066 |
| canonical-fixed-single | fixed | select_after_update | wall_ns | candidate | 29.802467, 27.502386, 25.345057 | 25.345057 | 27.502386 | 29.802467 |
| canonical-fixed-single | fixed | select_after_update | wall_ns | before-return | 28.228887, 31.370980, 31.275709 | 28.228887 | 31.275709 | 31.370980 |
| canonical-fixed-single | fixed | select_after_update | server_process_cpu_ns | before-first | 12.952131, 17.552444, 22.162932 | 12.952131 | 17.552444 | 22.162932 |
| canonical-fixed-single | fixed | select_after_update | server_process_cpu_ns | candidate | 18.696069, 18.103500, 16.323439 | 16.323439 | 18.103500 | 18.696069 |
| canonical-fixed-single | fixed | select_after_update | server_process_cpu_ns | before-return | 17.594721, 23.462109, 21.919702 | 17.594721 | 21.919702 | 23.462109 |
| canonical-fixed-single | fixed | select_after_update | client_process_cpu_ns | before-first | 4.884482, 4.932933, 4.852070 | 4.852070 | 4.884482 | 4.932933 |
| canonical-fixed-single | fixed | select_after_update | client_process_cpu_ns | candidate | 4.823035, 4.852296, 5.131575 | 4.823035 | 4.852296 | 5.131575 |
| canonical-fixed-single | fixed | select_after_update | client_process_cpu_ns | before-return | 4.809449, 4.996146, 3.135456 | 3.135456 | 4.809449 | 4.996146 |
| canonical-fixed-single | fixed | select_after_vacuum | wall_ns | before-first | 30.865446, 29.907802, 25.789369 | 25.789369 | 29.907802 | 30.865446 |
| canonical-fixed-single | fixed | select_after_vacuum | wall_ns | candidate | 27.243735, 28.426400, 25.803037 | 25.803037 | 27.243735 | 28.426400 |
| canonical-fixed-single | fixed | select_after_vacuum | wall_ns | before-return | 22.981485, 23.847395, 42.206686 | 22.981485 | 23.847395 | 42.206686 |
| canonical-fixed-single | fixed | select_after_vacuum | server_process_cpu_ns | before-first | 23.136382, 21.985189, 19.839459 | 19.839459 | 21.985189 | 23.136382 |
| canonical-fixed-single | fixed | select_after_vacuum | server_process_cpu_ns | candidate | 20.828624, 20.336246, 18.541737 | 18.541737 | 20.336246 | 20.828624 |
| canonical-fixed-single | fixed | select_after_vacuum | server_process_cpu_ns | before-return | 13.621685, 16.388426, 28.547773 | 13.621685 | 16.388426 | 28.547773 |
| canonical-fixed-single | fixed | select_after_vacuum | client_process_cpu_ns | before-first | 4.592536, 4.881089, 3.093118 | 3.093118 | 4.592536 | 4.881089 |
| canonical-fixed-single | fixed | select_after_vacuum | client_process_cpu_ns | candidate | 2.726656, 4.520839, 4.482062 | 2.726656 | 4.482062 | 4.520839 |
| canonical-fixed-single | fixed | select_after_vacuum | client_process_cpu_ns | before-return | 4.433692, 3.671436, 4.609172 | 3.671436 | 4.433692 | 4.609172 |
| canonical-fixed-single | fixed | truncate | wall_ns | before-first | 267.082130, 342.789167, 254.248212 | 254.248212 | 267.082130 | 342.789167 |
| canonical-fixed-single | fixed | truncate | wall_ns | candidate | 237.500951, 280.415601, 291.256072 | 237.500951 | 280.415601 | 291.256072 |
| canonical-fixed-single | fixed | truncate | wall_ns | before-return | 301.703156, 302.677775, 338.778943 | 301.703156 | 302.677775 | 338.778943 |
| canonical-fixed-single | fixed | truncate | server_process_cpu_ns | before-first | 97.981723, 82.880267, 86.505336 | 82.880267 | 86.505336 | 97.981723 |
| canonical-fixed-single | fixed | truncate | server_process_cpu_ns | candidate | 95.365218, 84.505780, 87.476074 | 84.505780 | 87.476074 | 95.365218 |
| canonical-fixed-single | fixed | truncate | server_process_cpu_ns | before-return | 87.730891, 90.622895, 119.412203 | 87.730891 | 90.622895 | 119.412203 |
| canonical-fixed-single | fixed | truncate | client_process_cpu_ns | before-first | 0.204539, 0.199967, 0.197672 | 0.197672 | 0.199967 | 0.204539 |
| canonical-fixed-single | fixed | truncate | client_process_cpu_ns | candidate | 0.200806, 0.268904, 0.234459 | 0.200806 | 0.234459 | 0.268904 |
| canonical-fixed-single | fixed | truncate | client_process_cpu_ns | before-return | 0.230237, 0.271527, 0.240893 | 0.230237 | 0.240893 | 0.271527 |
| canonical-fixed-single | fixed | update16 | wall_ns | before-first | 119.412204, 62.201543, 82.234121 | 62.201543 | 82.234121 | 119.412204 |
| canonical-fixed-single | fixed | update16 | wall_ns | candidate | 68.820065, 77.299386, 71.396340 | 68.820065 | 71.396340 | 77.299386 |
| canonical-fixed-single | fixed | update16 | wall_ns | before-return | 69.306560, 86.924545, 88.155234 | 69.306560 | 86.924545 | 88.155234 |
| canonical-fixed-single | fixed | update16 | server_process_cpu_ns | before-first | 71.055657, 53.032207, 72.114038 | 53.032207 | 71.055657 | 72.114038 |
| canonical-fixed-single | fixed | update16 | server_process_cpu_ns | candidate | 59.579636, 69.704474, 63.913948 | 59.579636 | 63.913948 | 69.704474 |
| canonical-fixed-single | fixed | update16 | server_process_cpu_ns | before-return | 61.184192, 76.129572, 77.532239 | 61.184192 | 76.129572 | 77.532239 |
| canonical-fixed-single | fixed | update16 | client_process_cpu_ns | before-first | 0.205661, 0.201164, 0.231913 | 0.201164 | 0.205661 | 0.231913 |
| canonical-fixed-single | fixed | update16 | client_process_cpu_ns | candidate | 0.193026, 0.202487, 0.207184 | 0.193026 | 0.202487 | 0.207184 |
| canonical-fixed-single | fixed | update16 | client_process_cpu_ns | before-return | 0.215484, 0.228000, 0.243680 | 0.215484 | 0.228000 | 0.243680 |
| canonical-fixed-single | fixed | vacuum | wall_ns | before-first | 2.072524, 1.705598, 2.101004 | 1.705598 | 2.072524 | 2.101004 |
| canonical-fixed-single | fixed | vacuum | wall_ns | candidate | 1.787327, 1.698608, 1.733929 | 1.698608 | 1.733929 | 1.787327 |
| canonical-fixed-single | fixed | vacuum | wall_ns | before-return | 1.195343, 1.624792, 2.417087 | 1.195343 | 1.624792 | 2.417087 |
| canonical-fixed-single | fixed | vacuum | server_process_cpu_ns | before-first | 1.929052, 1.567519, 2.022814 | 1.567519 | 1.929052 | 2.022814 |
| canonical-fixed-single | fixed | vacuum | server_process_cpu_ns | candidate | 1.680892, 1.556210, 1.597449 | 1.556210 | 1.597449 | 1.680892 |
| canonical-fixed-single | fixed | vacuum | server_process_cpu_ns | before-return | 1.063656, 1.501589, 2.280070 | 1.063656 | 1.501589 | 2.280070 |
| canonical-fixed-single | fixed | vacuum | client_process_cpu_ns | before-first | 0.214133, 0.235106, 0.160233 | 0.160233 | 0.214133 | 0.235106 |
| canonical-fixed-single | fixed | vacuum | client_process_cpu_ns | candidate | 0.202327, 0.205107, 0.201134 | 0.201134 | 0.202327 | 0.205107 |
| canonical-fixed-single | fixed | vacuum | client_process_cpu_ns | before-return | 0.200024, 0.197857, 0.208703 | 0.197857 | 0.200024 | 0.208703 |
| canonical-fixed-single | single | delete16 | wall_ns | before-first | 65.687808, 62.049388, 79.049954 | 62.049388 | 65.687808 | 79.049954 |
| canonical-fixed-single | single | delete16 | wall_ns | candidate | 77.309835, 108.906609, 68.363983 | 68.363983 | 77.309835 | 108.906609 |
| canonical-fixed-single | single | delete16 | wall_ns | before-return | 73.931744, 67.146180, 78.500418 | 67.146180 | 73.931744 | 78.500418 |
| canonical-fixed-single | single | delete16 | server_process_cpu_ns | before-first | 53.644653, 54.082051, 70.070506 | 53.644653 | 54.082051 | 70.070506 |
| canonical-fixed-single | single | delete16 | server_process_cpu_ns | candidate | 69.759375, 63.088773, 57.305785 | 57.305785 | 63.088773 | 69.759375 |
| canonical-fixed-single | single | delete16 | server_process_cpu_ns | before-return | 66.418794, 59.348727, 66.353934 | 59.348727 | 66.353934 | 66.418794 |
| canonical-fixed-single | single | delete16 | client_process_cpu_ns | before-first | 0.234530, 0.224043, 0.375647 | 0.224043 | 0.234530 | 0.375647 |
| canonical-fixed-single | single | delete16 | client_process_cpu_ns | candidate | 0.231759, 0.216352, 0.225914 | 0.216352 | 0.225914 | 0.231759 |
| canonical-fixed-single | single | delete16 | client_process_cpu_ns | before-return | 0.216760, 0.212995, 0.265852 | 0.212995 | 0.216760 | 0.265852 |
| canonical-fixed-single | single | insert1 | wall_ns | before-first | 22.893469, 16.279011, 56.386545 | 16.279011 | 22.893469 | 56.386545 |
| canonical-fixed-single | single | insert1 | wall_ns | candidate | 23.118507, 18.346362, 18.774264 | 18.346362 | 18.774264 | 23.118507 |
| canonical-fixed-single | single | insert1 | wall_ns | before-return | 17.958382, 19.561102, 26.737648 | 17.958382 | 19.561102 | 26.737648 |
| canonical-fixed-single | single | insert1 | server_process_cpu_ns | before-first | 8.612662, 8.341009, 11.876376 | 8.341009 | 8.612662 | 11.876376 |
| canonical-fixed-single | single | insert1 | server_process_cpu_ns | candidate | 12.159680, 9.101453, 9.903845 | 9.101453 | 9.903845 | 12.159680 |
| canonical-fixed-single | single | insert1 | server_process_cpu_ns | before-return | 11.049182, 9.769099, 10.742190 | 9.769099 | 10.742190 | 11.049182 |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | before-first | 0.210475, 0.198137, 0.237737 | 0.198137 | 0.210475 | 0.237737 |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | candidate | 0.251560, 0.206983, 0.229424 | 0.206983 | 0.229424 | 0.251560 |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | before-return | 0.199659, 0.202246, 0.229944 | 0.199659 | 0.202246 | 0.229944 |
| canonical-fixed-single | single | reinsert_population | wall_ns | before-first | 1099.424242, 1063.129712, 997.803840 | 997.803840 | 1063.129712 | 1099.424242 |
| canonical-fixed-single | single | reinsert_population | wall_ns | candidate | 1124.802906, 1180.012322, 1375.490769 | 1124.802906 | 1180.012322 | 1375.490769 |
| canonical-fixed-single | single | reinsert_population | wall_ns | before-return | 1086.611886, 1003.081027, 1148.161007 | 1003.081027 | 1086.611886 | 1148.161007 |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | before-first | 1036.263219, 1003.437020, 956.034890 | 956.034890 | 1003.437020 | 1036.263219 |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | candidate | 1075.088642, 1121.352639, 1064.765783 | 1064.765783 | 1075.088642 | 1121.352639 |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | before-return | 1041.497193, 958.308615, 951.513476 | 951.513476 | 958.308615 | 1041.497193 |
| canonical-fixed-single | single | reinsert_population | client_process_cpu_ns | before-first | 0.594725, 0.617798, 0.577001 | 0.577001 | 0.594725 | 0.617798 |
| canonical-fixed-single | single | reinsert_population | client_process_cpu_ns | candidate | 0.557391, 0.739560, 0.595867 | 0.557391 | 0.595867 | 0.739560 |
| canonical-fixed-single | single | reinsert_population | client_process_cpu_ns | before-return | 0.660467, 0.628351, 0.585957 | 0.585957 | 0.628351 | 0.660467 |
| canonical-fixed-single | single | select_after_update | wall_ns | before-first | 55.294565, 52.824766, 47.541509 | 47.541509 | 52.824766 | 55.294565 |
| canonical-fixed-single | single | select_after_update | wall_ns | candidate | 56.056428, 105.492166, 45.651023 | 45.651023 | 56.056428 | 105.492166 |
| canonical-fixed-single | single | select_after_update | wall_ns | before-return | 48.585891, 50.667855, 66.279740 | 48.585891 | 50.667855 | 66.279740 |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | before-first | 45.453529, 46.904938, 42.209961 | 42.209961 | 45.453529 | 46.904938 |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | candidate | 48.600855, 60.688800, 36.102887 | 36.102887 | 48.600855 | 60.688800 |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | before-return | 40.489986, 43.013971, 54.033432 | 40.489986 | 43.013971 | 54.033432 |
| canonical-fixed-single | single | select_after_update | client_process_cpu_ns | before-first | 6.707132, 4.167398, 6.294034 | 4.167398 | 6.294034 | 6.707132 |
| canonical-fixed-single | single | select_after_update | client_process_cpu_ns | candidate | 5.547595, 6.699888, 5.900944 | 5.547595 | 5.900944 | 6.699888 |
| canonical-fixed-single | single | select_after_update | client_process_cpu_ns | before-return | 5.809585, 5.967064, 6.042717 | 5.809585 | 5.967064 | 6.042717 |
| canonical-fixed-single | single | select_after_vacuum | wall_ns | before-first | 54.936410, 49.662604, 53.648151 | 49.662604 | 53.648151 | 54.936410 |
| canonical-fixed-single | single | select_after_vacuum | wall_ns | candidate | 56.694185, 59.088630, 62.777364 | 56.694185 | 59.088630 | 62.777364 |
| canonical-fixed-single | single | select_after_vacuum | wall_ns | before-return | 61.402248, 61.627266, 63.136890 | 61.402248 | 61.627266 | 63.136890 |
| canonical-fixed-single | single | select_after_vacuum | server_process_cpu_ns | before-first | 47.369796, 43.682065, 46.856221 | 43.682065 | 46.856221 | 47.369796 |
| canonical-fixed-single | single | select_after_vacuum | server_process_cpu_ns | candidate | 49.542050, 49.627816, 55.987096 | 49.542050 | 49.627816 | 55.987096 |
| canonical-fixed-single | single | select_after_vacuum | server_process_cpu_ns | before-return | 56.135521, 54.716151, 53.265189 | 53.265189 | 54.716151 | 56.135521 |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | before-first | 4.900264, 5.262021, 5.986698 | 4.900264 | 5.262021 | 5.986698 |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | candidate | 5.839955, 5.523482, 5.531521 | 5.523482 | 5.531521 | 5.839955 |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | before-return | 4.779712, 5.570510, 5.348921 | 4.779712 | 5.348921 | 5.570510 |
| canonical-fixed-single | single | truncate | wall_ns | before-first | 270.710618, 239.970308, 278.746431 | 239.970308 | 270.710618 | 278.746431 |
| canonical-fixed-single | single | truncate | wall_ns | candidate | 223.989218, 339.008075, 258.231110 | 223.989218 | 258.231110 | 339.008075 |
| canonical-fixed-single | single | truncate | wall_ns | before-return | 297.124739, 217.601604, 218.816696 | 217.601604 | 218.816696 | 297.124739 |
| canonical-fixed-single | single | truncate | server_process_cpu_ns | before-first | 136.817586, 135.714692, 137.370200 | 135.714692 | 136.817586 | 137.370200 |
| canonical-fixed-single | single | truncate | server_process_cpu_ns | candidate | 131.633234, 111.660610, 138.458396 | 111.660610 | 131.633234 | 138.458396 |
| canonical-fixed-single | single | truncate | server_process_cpu_ns | before-return | 112.493723, 102.612759, 117.642477 | 102.612759 | 112.493723 | 117.642477 |
| canonical-fixed-single | single | truncate | client_process_cpu_ns | before-first | 0.228505, 0.243465, 0.253464 | 0.228505 | 0.243465 | 0.253464 |
| canonical-fixed-single | single | truncate | client_process_cpu_ns | candidate | 0.224061, 0.238600, 0.255101 | 0.224061 | 0.238600 | 0.255101 |
| canonical-fixed-single | single | truncate | client_process_cpu_ns | before-return | 0.211343, 0.244830, 0.233996 | 0.211343 | 0.233996 | 0.244830 |
| canonical-fixed-single | single | update16 | wall_ns | before-first | 148.573249, 160.961433, 146.092162 | 146.092162 | 148.573249 | 160.961433 |
| canonical-fixed-single | single | update16 | wall_ns | candidate | 198.533315, 156.862250, 140.844230 | 140.844230 | 156.862250 | 198.533315 |
| canonical-fixed-single | single | update16 | wall_ns | before-return | 138.660757, 130.599717, 175.271243 | 130.599717 | 138.660757 | 175.271243 |
| canonical-fixed-single | single | update16 | server_process_cpu_ns | before-first | 128.616244, 126.948638, 127.085192 | 126.948638 | 127.085192 | 128.616244 |
| canonical-fixed-single | single | update16 | server_process_cpu_ns | candidate | 123.912751, 129.689701, 118.997293 | 118.997293 | 123.912751 | 129.689701 |
| canonical-fixed-single | single | update16 | server_process_cpu_ns | before-return | 120.288598, 111.251437, 128.505024 | 111.251437 | 120.288598 | 128.505024 |
| canonical-fixed-single | single | update16 | client_process_cpu_ns | before-first | 0.217066, 0.238600, 0.217512 | 0.217066 | 0.217512 | 0.238600 |
| canonical-fixed-single | single | update16 | client_process_cpu_ns | candidate | 0.236658, 0.227911, 0.231891 | 0.227911 | 0.231891 | 0.236658 |
| canonical-fixed-single | single | update16 | client_process_cpu_ns | before-return | 0.267937, 0.228478, 0.257160 | 0.228478 | 0.257160 | 0.267937 |
| canonical-fixed-single | single | vacuum | wall_ns | before-first | 7.298122, 6.501652, 8.135543 | 6.501652 | 7.298122 | 8.135543 |
| canonical-fixed-single | single | vacuum | wall_ns | candidate | 8.061628, 7.851722, 7.394089 | 7.394089 | 7.851722 | 8.061628 |
| canonical-fixed-single | single | vacuum | wall_ns | before-return | 7.469578, 7.530264, 7.973806 | 7.469578 | 7.530264 | 7.973806 |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | before-first | 7.078252, 6.257968, 7.882661 | 6.257968 | 7.078252 | 7.882661 |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | candidate | 7.783174, 7.478700, 7.145878 | 7.145878 | 7.478700 | 7.783174 |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | before-return | 7.237276, 7.310258, 7.686326 | 7.237276 | 7.310258 | 7.686326 |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | before-first | 0.171092, 0.201549, 0.235615 | 0.171092 | 0.201549 | 0.235615 |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | candidate | 0.200733, 0.254578, 0.218524 | 0.200733 | 0.218524 | 0.254578 |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | before-return | 0.201011, 0.200458, 0.242307 | 0.200458 | 0.201011 | 0.242307 |
| canonical-overflow | overflow | delete16 | wall_ns | before-first | 54.622649, 91.536410, 85.337794 | 54.622649 | 85.337794 | 91.536410 |
| canonical-overflow | overflow | delete16 | wall_ns | candidate | 98.560751, 76.413798, 69.683409 | 69.683409 | 76.413798 | 98.560751 |
| canonical-overflow | overflow | delete16 | wall_ns | before-return | 75.807947, 85.622535, 69.666998 | 69.666998 | 75.807947 | 85.622535 |
| canonical-overflow | overflow | delete16 | server_process_cpu_ns | before-first | 45.344001, 71.436405, 73.021092 | 45.344001 | 71.436405 | 73.021092 |
| canonical-overflow | overflow | delete16 | server_process_cpu_ns | candidate | 69.325092, 68.098660, 61.079253 | 61.079253 | 68.098660 | 69.325092 |
| canonical-overflow | overflow | delete16 | server_process_cpu_ns | before-return | 66.893396, 70.145581, 55.160144 | 55.160144 | 66.893396 | 70.145581 |
| canonical-overflow | overflow | delete16 | client_process_cpu_ns | before-first | 0.252303, 0.281867, 0.220581 | 0.220581 | 0.252303 | 0.281867 |
| canonical-overflow | overflow | delete16 | client_process_cpu_ns | candidate | 0.247740, 0.278807, 0.239431 | 0.239431 | 0.247740 | 0.278807 |
| canonical-overflow | overflow | delete16 | client_process_cpu_ns | before-return | 0.279839, 0.253681, 0.237189 | 0.237189 | 0.253681 | 0.279839 |
| canonical-overflow | overflow | insert1 | wall_ns | before-first | 26.495412, 27.523904, 38.665556 | 26.495412 | 27.523904 | 38.665556 |
| canonical-overflow | overflow | insert1 | wall_ns | candidate | 19.810484, 18.629867, 16.089200 | 16.089200 | 18.629867 | 19.810484 |
| canonical-overflow | overflow | insert1 | wall_ns | before-return | 18.915860, 22.522496, 38.072382 | 18.915860 | 22.522496 | 38.072382 |
| canonical-overflow | overflow | insert1 | server_process_cpu_ns | before-first | 9.108295, 17.516687, 19.233188 | 9.108295 | 17.516687 | 19.233188 |
| canonical-overflow | overflow | insert1 | server_process_cpu_ns | candidate | 10.432227, 8.059878, 8.133840 | 8.059878 | 8.133840 | 10.432227 |
| canonical-overflow | overflow | insert1 | server_process_cpu_ns | before-return | 10.817072, 8.372134, 9.923612 | 8.372134 | 9.923612 | 10.817072 |
| canonical-overflow | overflow | insert1 | client_process_cpu_ns | before-first | 0.180877, 0.220052, 0.246534 | 0.180877 | 0.220052 | 0.246534 |
| canonical-overflow | overflow | insert1 | client_process_cpu_ns | candidate | 0.200249, 0.201777, 0.202419 | 0.200249 | 0.201777 | 0.202419 |
| canonical-overflow | overflow | insert1 | client_process_cpu_ns | before-return | 0.198990, 0.107350, 0.186171 | 0.107350 | 0.186171 | 0.198990 |
| canonical-overflow | overflow | reinsert_population | wall_ns | before-first | 1459.137672, 1072.947750, 1245.641057 | 1072.947750 | 1245.641057 | 1459.137672 |
| canonical-overflow | overflow | reinsert_population | wall_ns | candidate | 1233.713081, 1217.456124, 1189.841574 | 1189.841574 | 1217.456124 | 1233.713081 |
| canonical-overflow | overflow | reinsert_population | wall_ns | before-return | 1086.299465, 1058.776273, 1235.186373 | 1058.776273 | 1086.299465 | 1235.186373 |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | before-first | 1041.701215, 978.856767, 1105.559815 | 978.856767 | 1041.701215 | 1105.559815 |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | candidate | 1035.306881, 1105.937772, 1146.743726 | 1035.306881 | 1105.937772 | 1146.743726 |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | before-return | 1050.044468, 1020.912481, 1078.688404 | 1020.912481 | 1050.044468 | 1078.688404 |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | before-first | 0.544997, 0.719734, 0.760920 | 0.544997 | 0.719734 | 0.760920 |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | candidate | 1.106365, 0.819604, 0.764978 | 0.764978 | 0.819604 | 1.106365 |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | before-return | 0.749518, 0.758670, 1.292883 | 0.749518 | 0.758670 | 1.292883 |
| canonical-overflow | overflow | select_after_update | wall_ns | before-first | 66.565880, 67.836175, 62.038982 | 62.038982 | 66.565880 | 67.836175 |
| canonical-overflow | overflow | select_after_update | wall_ns | candidate | 56.777434, 67.860833, 43.651506 | 43.651506 | 56.777434 | 67.860833 |
| canonical-overflow | overflow | select_after_update | wall_ns | before-return | 47.359201, 154.542920, 54.145329 | 47.359201 | 54.145329 | 154.542920 |
| canonical-overflow | overflow | select_after_update | server_process_cpu_ns | before-first | 58.817035, 53.990606, 53.250445 | 53.250445 | 53.990606 | 58.817035 |
| canonical-overflow | overflow | select_after_update | server_process_cpu_ns | candidate | 44.075450, 57.694720, 37.344073 | 37.344073 | 44.075450 | 57.694720 |
| canonical-overflow | overflow | select_after_update | server_process_cpu_ns | before-return | 40.873340, 67.938779, 42.993546 | 40.873340 | 42.993546 | 67.938779 |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | before-first | 4.143527, 4.317272, 3.931197 | 3.931197 | 4.143527 | 4.317272 |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | candidate | 4.534115, 4.146123, 3.963954 | 3.963954 | 4.146123 | 4.534115 |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | before-return | 3.913240, 4.284709, 3.931683 | 3.913240 | 3.931683 | 4.284709 |
| canonical-overflow | overflow | select_after_vacuum | wall_ns | before-first | 51.611641, 70.341704, 67.008167 | 51.611641 | 67.008167 | 70.341704 |
| canonical-overflow | overflow | select_after_vacuum | wall_ns | candidate | 82.155192, 62.177983, 59.057650 | 59.057650 | 62.177983 | 82.155192 |
| canonical-overflow | overflow | select_after_vacuum | wall_ns | before-return | 59.131679, 61.659412, 72.486238 | 59.131679 | 61.659412 | 72.486238 |
| canonical-overflow | overflow | select_after_vacuum | server_process_cpu_ns | before-first | 45.257791, 62.262054, 59.284220 | 45.257791 | 59.284220 | 62.262054 |
| canonical-overflow | overflow | select_after_vacuum | server_process_cpu_ns | candidate | 75.549351, 56.817200, 53.471146 | 53.471146 | 56.817200 | 75.549351 |
| canonical-overflow | overflow | select_after_vacuum | server_process_cpu_ns | before-return | 54.091773, 55.111631, 58.292921 | 54.091773 | 55.111631 | 58.292921 |
| canonical-overflow | overflow | select_after_vacuum | client_process_cpu_ns | before-first | 4.058232, 3.388746, 3.794473 | 3.388746 | 3.794473 | 4.058232 |
| canonical-overflow | overflow | select_after_vacuum | client_process_cpu_ns | candidate | 3.612591, 3.311135, 3.929187 | 3.311135 | 3.612591 | 3.929187 |
| canonical-overflow | overflow | select_after_vacuum | client_process_cpu_ns | before-return | 3.826113, 3.788343, 3.816481 | 3.788343 | 3.816481 | 3.826113 |
| canonical-overflow | overflow | truncate | wall_ns | before-first | 164.379853, 223.164615, 222.395574 | 164.379853 | 222.395574 | 223.164615 |
| canonical-overflow | overflow | truncate | wall_ns | candidate | 168.289006, 149.905403, 203.504509 | 149.905403 | 168.289006 | 203.504509 |
| canonical-overflow | overflow | truncate | wall_ns | before-return | 184.877071, 134.639516, 209.151919 | 134.639516 | 184.877071 | 209.151919 |
| canonical-overflow | overflow | truncate | server_process_cpu_ns | before-first | 85.823160, 91.433927, 89.122556 | 85.823160 | 89.122556 | 91.433927 |
| canonical-overflow | overflow | truncate | server_process_cpu_ns | candidate | 63.169897, 81.281043, 85.811062 | 63.169897 | 81.281043 | 85.811062 |
| canonical-overflow | overflow | truncate | server_process_cpu_ns | before-return | 86.352397, 60.191796, 88.391515 | 60.191796 | 86.352397 | 88.391515 |
| canonical-overflow | overflow | truncate | client_process_cpu_ns | before-first | 0.254056, 0.232215, 0.212495 | 0.212495 | 0.232215 | 0.254056 |
| canonical-overflow | overflow | truncate | client_process_cpu_ns | candidate | 0.236821, 0.224121, 0.240182 | 0.224121 | 0.236821 | 0.240182 |
| canonical-overflow | overflow | truncate | client_process_cpu_ns | before-return | 0.276087, 0.258834, 0.229235 | 0.229235 | 0.258834 | 0.276087 |
| canonical-overflow | overflow | update16 | wall_ns | before-first | 178.297210, 168.245816, 443.483128 | 168.245816 | 178.297210 | 443.483128 |
| canonical-overflow | overflow | update16 | wall_ns | candidate | 162.276724, 162.078530, 149.838661 | 149.838661 | 162.078530 | 162.276724 |
| canonical-overflow | overflow | update16 | wall_ns | before-return | 154.853201, 180.563931, 154.211253 | 154.211253 | 154.853201 | 180.563931 |
| canonical-overflow | overflow | update16 | server_process_cpu_ns | before-first | 141.223868, 139.997483, 156.274261 | 139.997483 | 141.223868 | 156.274261 |
| canonical-overflow | overflow | update16 | server_process_cpu_ns | candidate | 137.565048, 128.374784, 131.625046 | 128.374784 | 131.625046 | 137.565048 |
| canonical-overflow | overflow | update16 | server_process_cpu_ns | before-return | 131.684644, 141.695426, 120.569210 | 120.569210 | 131.684644 | 141.695426 |
| canonical-overflow | overflow | update16 | client_process_cpu_ns | before-first | 0.210062, 0.259639, 0.316326 | 0.210062 | 0.259639 | 0.316326 |
| canonical-overflow | overflow | update16 | client_process_cpu_ns | candidate | 0.226401, 0.253188, 0.243936 | 0.226401 | 0.243936 | 0.253188 |
| canonical-overflow | overflow | update16 | client_process_cpu_ns | before-return | 0.252100, 0.298521, 0.242098 | 0.242098 | 0.252100 | 0.298521 |
| canonical-overflow | overflow | vacuum | wall_ns | before-first | 10.322398, 14.062169, 10.714862 | 10.322398 | 10.714862 | 14.062169 |
| canonical-overflow | overflow | vacuum | wall_ns | candidate | 10.944757, 11.610084, 9.409261 | 9.409261 | 10.944757 | 11.610084 |
| canonical-overflow | overflow | vacuum | wall_ns | before-return | 7.951665, 10.437493, 9.896709 | 7.951665 | 9.896709 | 10.437493 |
| canonical-overflow | overflow | vacuum | server_process_cpu_ns | before-first | 10.076847, 11.860150, 10.493914 | 10.076847 | 10.493914 | 11.860150 |
| canonical-overflow | overflow | vacuum | server_process_cpu_ns | candidate | 10.616654, 9.684166, 9.132073 | 9.132073 | 9.684166 | 10.616654 |
| canonical-overflow | overflow | vacuum | server_process_cpu_ns | before-return | 7.673293, 10.189005, 9.684176 | 7.673293 | 9.684176 | 10.189005 |
| canonical-overflow | overflow | vacuum | client_process_cpu_ns | before-first | 0.235630, 0.197894, 0.211206 | 0.197894 | 0.211206 | 0.235630 |
| canonical-overflow | overflow | vacuum | client_process_cpu_ns | candidate | 0.205571, 0.258678, 0.197557 | 0.197557 | 0.205571 | 0.258678 |
| canonical-overflow | overflow | vacuum | client_process_cpu_ns | before-return | 0.234152, 0.221031, 0.177996 | 0.177996 | 0.221031 | 0.234152 |
| direct-hit | fixed | direct_full_no_order | wall_ns | before-first | 30.670564, 28.718385, 18.755787 | 18.755787 | 28.718385 | 30.670564 |
| direct-hit | fixed | direct_full_no_order | wall_ns | candidate | 25.778541, 18.952379, 23.645419 | 18.952379 | 23.645419 | 25.778541 |
| direct-hit | fixed | direct_full_no_order | wall_ns | before-return | 28.494162, 26.345005, 24.453209 | 24.453209 | 26.345005 | 28.494162 |
| direct-hit | fixed | direct_full_no_order | server_process_cpu_ns | before-first | 20.938926, 19.857274, 12.280454 | 12.280454 | 19.857274 | 20.938926 |
| direct-hit | fixed | direct_full_no_order | server_process_cpu_ns | candidate | 14.167282, 11.313703, 13.545056 | 11.313703 | 13.545056 | 14.167282 |
| direct-hit | fixed | direct_full_no_order | server_process_cpu_ns | before-return | 20.903415, 20.030613, 16.326684 | 16.326684 | 20.030613 | 20.903415 |
| direct-hit | fixed | direct_full_no_order | client_process_cpu_ns | before-first | 4.940999, 4.958276, 3.347394 | 3.347394 | 4.940999 | 4.958276 |
| direct-hit | fixed | direct_full_no_order | client_process_cpu_ns | candidate | 4.838119, 5.017413, 4.851780 | 4.838119 | 4.851780 | 5.017413 |
| direct-hit | fixed | direct_full_no_order | client_process_cpu_ns | before-return | 4.987815, 3.713140, 4.817985 | 3.713140 | 4.817985 | 4.987815 |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | before-first | 19.026843, 22.764981, 12.270446 | 12.270446 | 19.026843 | 22.764981 |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | candidate | 22.537255, 17.831798, 23.092414 | 17.831798 | 22.537255 | 23.092414 |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | before-return | 21.778431, 21.178411, 15.298454 | 15.298454 | 21.178411 | 21.778431 |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | before-first | 11.802242, 10.705477, 5.928947 | 5.928947 | 10.705477 | 11.802242 |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | candidate | 13.617061, 10.250844, 13.508781 | 10.250844 | 13.508781 | 13.617061 |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | before-return | 10.734883, 13.791976, 7.677790 | 7.677790 | 10.734883 | 13.791976 |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | before-first | 4.961019, 4.898763, 3.067973 | 3.067973 | 4.898763 | 4.961019 |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | candidate | 4.836149, 4.965446, 5.164936 | 4.836149 | 4.965446 | 5.164936 |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | before-return | 4.796655, 4.927680, 4.887323 | 4.796655 | 4.887323 | 4.927680 |
| direct-hit | overflow | direct_full_no_order | wall_ns | before-first | 51.951237, 59.104093, 50.820346 | 50.820346 | 51.951237 | 59.104093 |
| direct-hit | overflow | direct_full_no_order | wall_ns | candidate | 47.097845, 61.612734, 43.733209 | 43.733209 | 47.097845 | 61.612734 |
| direct-hit | overflow | direct_full_no_order | wall_ns | before-return | 53.782487, 55.293306, 67.271706 | 53.782487 | 55.293306 | 67.271706 |
| direct-hit | overflow | direct_full_no_order | server_process_cpu_ns | before-first | 43.615965, 52.791412, 40.473502 | 40.473502 | 43.615965 | 52.791412 |
| direct-hit | overflow | direct_full_no_order | server_process_cpu_ns | candidate | 33.626316, 48.992280, 38.873304 | 33.626316 | 38.873304 | 48.992280 |
| direct-hit | overflow | direct_full_no_order | server_process_cpu_ns | before-return | 47.359678, 50.295550, 60.159402 | 47.359678 | 50.295550 | 60.159402 |
| direct-hit | overflow | direct_full_no_order | client_process_cpu_ns | before-first | 3.525811, 4.165848, 3.685737 | 3.525811 | 3.685737 | 4.165848 |
| direct-hit | overflow | direct_full_no_order | client_process_cpu_ns | candidate | 3.916929, 3.441181, 4.061236 | 3.441181 | 3.916929 | 4.061236 |
| direct-hit | overflow | direct_full_no_order | client_process_cpu_ns | before-return | 2.747553, 4.253011, 4.198729 | 2.747553 | 4.198729 | 4.253011 |
| direct-hit | overflow | direct_id_only_no_order | wall_ns | before-first | 37.923058, 51.686001, 42.775527 | 37.923058 | 42.775527 | 51.686001 |
| direct-hit | overflow | direct_id_only_no_order | wall_ns | candidate | 46.727407, 54.493939, 45.101120 | 45.101120 | 46.727407 | 54.493939 |
| direct-hit | overflow | direct_id_only_no_order | wall_ns | before-return | 34.631879, 48.734412, 47.629587 | 34.631879 | 47.629587 | 48.734412 |
| direct-hit | overflow | direct_id_only_no_order | server_process_cpu_ns | before-first | 30.336007, 43.866240, 37.215813 | 30.336007 | 37.215813 | 43.866240 |
| direct-hit | overflow | direct_id_only_no_order | server_process_cpu_ns | candidate | 40.445507, 42.344877, 39.163833 | 39.163833 | 40.445507 | 42.344877 |
| direct-hit | overflow | direct_id_only_no_order | server_process_cpu_ns | before-return | 29.180141, 42.520215, 41.494925 | 29.180141 | 41.494925 | 42.520215 |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | before-first | 1.837721, 1.947014, 1.785558 | 1.785558 | 1.837721 | 1.947014 |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | candidate | 1.730814, 2.363928, 2.368961 | 1.730814 | 2.363928 | 2.368961 |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | before-return | 1.721982, 2.508360, 1.867276 | 1.721982 | 1.867276 | 2.508360 |
| direct-hit | single | direct_full_no_order | wall_ns | before-first | 65.476482, 116.081986, 42.990849 | 42.990849 | 65.476482 | 116.081986 |
| direct-hit | single | direct_full_no_order | wall_ns | candidate | 50.508186, 45.241063, 43.703564 | 43.703564 | 45.241063 | 50.508186 |
| direct-hit | single | direct_full_no_order | wall_ns | before-return | 74.600648, 66.706389, 57.035736 | 57.035736 | 66.706389 | 74.600648 |
| direct-hit | single | direct_full_no_order | server_process_cpu_ns | before-first | 57.933788, 76.782261, 37.010125 | 37.010125 | 57.933788 | 76.782261 |
| direct-hit | single | direct_full_no_order | server_process_cpu_ns | candidate | 44.171553, 37.931080, 34.946935 | 34.946935 | 37.931080 | 44.171553 |
| direct-hit | single | direct_full_no_order | server_process_cpu_ns | before-return | 57.571894, 57.705516, 49.797758 | 49.797758 | 57.571894 | 57.705516 |
| direct-hit | single | direct_full_no_order | client_process_cpu_ns | before-first | 5.830670, 5.334822, 4.555058 | 4.555058 | 5.334822 | 5.830670 |
| direct-hit | single | direct_full_no_order | client_process_cpu_ns | candidate | 6.172691, 7.233967, 5.614178 | 5.614178 | 6.172691 | 7.233967 |
| direct-hit | single | direct_full_no_order | client_process_cpu_ns | before-return | 6.326008, 6.252110, 5.342690 | 5.342690 | 6.252110 | 6.326008 |
| direct-hit | single | direct_id_only_no_order | wall_ns | before-first | 43.117665, 53.219795, 30.292869 | 30.292869 | 43.117665 | 53.219795 |
| direct-hit | single | direct_id_only_no_order | wall_ns | candidate | 47.244320, 45.889961, 46.578473 | 45.889961 | 46.578473 | 47.244320 |
| direct-hit | single | direct_id_only_no_order | wall_ns | before-return | 44.937549, 54.877132, 43.887892 | 43.887892 | 44.937549 | 54.877132 |
| direct-hit | single | direct_id_only_no_order | server_process_cpu_ns | before-first | 37.149316, 46.544258, 24.844469 | 24.844469 | 37.149316 | 46.544258 |
| direct-hit | single | direct_id_only_no_order | server_process_cpu_ns | candidate | 37.527921, 39.876264, 38.707986 | 37.527921 | 38.707986 | 39.876264 |
| direct-hit | single | direct_id_only_no_order | server_process_cpu_ns | before-return | 39.988560, 39.579437, 35.764912 | 35.764912 | 39.579437 | 39.988560 |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | before-first | 3.748197, 5.556986, 3.263656 | 3.263656 | 3.748197 | 5.556986 |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | candidate | 3.726158, 4.483609, 3.944293 | 3.726158 | 3.944293 | 4.483609 |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | before-return | 3.745151, 4.070041, 3.607566 | 3.607566 | 3.745151 | 4.070041 |

| Cohort | Scope | Region | Clock | Candidate vs first % | Candidate vs return % | Return drift % | Class |
|---|---|---|---|---:|---:|---:|---|
| canonical-fixed-single | fixed | delete16 | wall_ns | -8.337146 | -18.161311 | +12.004304 | lower_than_both |
| canonical-fixed-single | fixed | delete16 | server_process_cpu_ns | -4.440561 | -20.355342 | +19.982232 | lower_than_both |
| canonical-fixed-single | fixed | delete16 | client_process_cpu_ns | -12.288935 | -2.586144 | -9.960381 | lower_than_both |
| canonical-fixed-single | fixed | insert1 | wall_ns | -12.753403 | -34.953251 | +34.129065 | lower_than_both |
| canonical-fixed-single | fixed | insert1 | server_process_cpu_ns | -19.152539 | -45.626742 | +48.689749 | lower_than_both |
| canonical-fixed-single | fixed | insert1 | client_process_cpu_ns | -10.615502 | -10.650586 | +0.039266 | lower_than_both |
| canonical-fixed-single | fixed | reinsert_population | wall_ns | -0.537663 | -3.007060 | +2.545956 | lower_than_both |
| canonical-fixed-single | fixed | reinsert_population | server_process_cpu_ns | +4.185590 | -0.232773 | +4.428672 | between_or_equal |
| canonical-fixed-single | fixed | reinsert_population | client_process_cpu_ns | +1.657186 | -2.112140 | +3.850658 | between_or_equal |
| canonical-fixed-single | fixed | select_after_update | wall_ns | -10.264920 | -12.064708 | +2.046719 | lower_than_both |
| canonical-fixed-single | fixed | select_after_update | server_process_cpu_ns | +3.139483 | -17.409917 | +24.881196 | between_or_equal |
| canonical-fixed-single | fixed | select_after_update | client_process_cpu_ns | -0.658944 | +0.890892 | -1.536151 | between_or_equal |
| canonical-fixed-single | fixed | select_after_vacuum | wall_ns | -8.907599 | +14.241975 | -20.263632 | between_or_equal |
| canonical-fixed-single | fixed | select_after_vacuum | server_process_cpu_ns | -7.500245 | +24.089074 | -25.456970 | between_or_equal |
| canonical-fixed-single | fixed | select_after_vacuum | client_process_cpu_ns | -2.405512 | +1.090964 | -3.458743 | between_or_equal |
| canonical-fixed-single | fixed | truncate | wall_ns | +4.992274 | -7.355074 | +13.327603 | between_or_equal |
| canonical-fixed-single | fixed | truncate | server_process_cpu_ns | +1.122171 | -3.472435 | +4.759890 | between_or_equal |
| canonical-fixed-single | fixed | truncate | client_process_cpu_ns | +17.248846 | -2.670895 | +20.466377 | between_or_equal |
| canonical-fixed-single | fixed | update16 | wall_ns | -13.179178 | -17.864005 | +5.703744 | lower_than_both |
| canonical-fixed-single | fixed | update16 | server_process_cpu_ns | -10.050866 | -16.045833 | +7.140762 | lower_than_both |
| canonical-fixed-single | fixed | update16 | client_process_cpu_ns | -1.543316 | -11.189912 | +10.862050 | lower_than_both |
| canonical-fixed-single | fixed | vacuum | wall_ns | -16.337326 | +6.716983 | -21.603224 | between_or_equal |
| canonical-fixed-single | fixed | vacuum | server_process_cpu_ns | -17.189946 | +6.383904 | -22.159226 | between_or_equal |
| canonical-fixed-single | fixed | vacuum | client_process_cpu_ns | -5.513396 | +1.151362 | -6.588896 | between_or_equal |
| canonical-fixed-single | single | delete16 | wall_ns | +17.692822 | +4.569202 | +12.550177 | higher_than_both |
| canonical-fixed-single | single | delete16 | server_process_cpu_ns | +16.653810 | -4.920825 | +22.691231 | between_or_equal |
| canonical-fixed-single | single | delete16 | client_process_cpu_ns | -3.673730 | +4.223104 | -7.576856 | between_or_equal |
| canonical-fixed-single | single | insert1 | wall_ns | -17.992926 | -4.022463 | -14.555972 | lower_than_both |
| canonical-fixed-single | single | insert1 | server_process_cpu_ns | +14.991683 | -7.804228 | +24.725549 | between_or_equal |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | +9.002969 | +13.438090 | -3.909728 | higher_than_both |
| canonical-fixed-single | single | reinsert_population | wall_ns | +10.994200 | +8.595565 | +2.208778 | higher_than_both |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | +7.140620 | +12.186056 | -4.497383 | higher_than_both |
| canonical-fixed-single | single | reinsert_population | client_process_cpu_ns | +0.192022 | -5.169722 | +5.654042 | between_or_equal |
| canonical-fixed-single | single | select_after_update | wall_ns | +6.117702 | +10.635092 | -4.083144 | higher_than_both |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | +6.924272 | +12.988533 | -5.367148 | higher_than_both |
| canonical-fixed-single | single | select_after_update | client_process_cpu_ns | -6.245438 | -1.108083 | -5.194920 | lower_than_both |
| canonical-fixed-single | single | select_after_vacuum | wall_ns | +10.141037 | -4.119339 | +14.873048 | between_or_equal |
| canonical-fixed-single | single | select_after_vacuum | server_process_cpu_ns | +5.915106 | -9.299512 | +16.774571 | between_or_equal |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | +5.121606 | +3.413773 | +1.651457 | higher_than_both |
| canonical-fixed-single | single | truncate | wall_ns | -4.609907 | +18.012526 | -19.169519 | between_or_equal |
| canonical-fixed-single | single | truncate | server_process_cpu_ns | -3.789244 | +17.013848 | -17.778316 | between_or_equal |
| canonical-fixed-single | single | truncate | client_process_cpu_ns | -1.998234 | +1.967555 | -3.889265 | between_or_equal |
| canonical-fixed-single | single | update16 | wall_ns | +5.579067 | +13.126636 | -6.671788 | higher_than_both |
| canonical-fixed-single | single | update16 | server_process_cpu_ns | -2.496311 | +3.012882 | -5.348061 | between_or_equal |
| canonical-fixed-single | single | update16 | client_process_cpu_ns | +6.610670 | -9.826178 | +18.227960 | between_or_equal |
| canonical-fixed-single | single | vacuum | wall_ns | +7.585513 | +4.268881 | +3.180846 | higher_than_both |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | +5.657442 | +2.304187 | +3.277730 | higher_than_both |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | +8.422270 | +8.712459 | -0.266933 | higher_than_both |
| canonical-overflow | overflow | delete16 | wall_ns | -10.457261 | +0.799192 | -11.167206 | between_or_equal |
| canonical-overflow | overflow | delete16 | server_process_cpu_ns | -4.672331 | +1.801768 | -6.359515 | between_or_equal |
| canonical-overflow | overflow | delete16 | client_process_cpu_ns | -1.808540 | -2.341918 | +0.546169 | lower_than_both |
| canonical-overflow | overflow | insert1 | wall_ns | -32.313864 | -17.283293 | -18.171143 | lower_than_both |
| canonical-overflow | overflow | insert1 | server_process_cpu_ns | -53.565192 | -18.035489 | -43.347666 | lower_than_both |
| canonical-overflow | overflow | insert1 | client_process_cpu_ns | -8.304855 | +8.382616 | -15.396815 | between_or_equal |
| canonical-overflow | overflow | reinsert_population | wall_ns | -2.262685 | +12.073711 | -12.791935 | between_or_equal |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | +6.166505 | +5.322946 | +0.800926 | higher_than_both |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | +13.875960 | +8.031687 | +5.409776 | higher_than_both |
| canonical-overflow | overflow | select_after_update | wall_ns | -14.704900 | +4.861186 | -18.659035 | between_or_equal |
| canonical-overflow | overflow | select_after_update | server_process_cpu_ns | -18.364595 | +2.516434 | -20.368469 | between_or_equal |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | +0.062652 | +5.454153 | -5.112649 | higher_than_both |
| canonical-overflow | overflow | select_after_vacuum | wall_ns | -7.208351 | +0.841025 | -7.982243 | between_or_equal |
| canonical-overflow | overflow | select_after_vacuum | server_process_cpu_ns | -4.161343 | +3.094753 | -7.038279 | between_or_equal |
| canonical-overflow | overflow | select_after_vacuum | client_process_cpu_ns | -4.793340 | -5.342356 | +0.580001 | lower_than_both |
| canonical-overflow | overflow | truncate | wall_ns | -24.328977 | -8.972484 | -16.870166 | lower_than_both |
| canonical-overflow | overflow | truncate | server_process_cpu_ns | -8.798573 | -5.872858 | -3.108258 | lower_than_both |
| canonical-overflow | overflow | truncate | client_process_cpu_ns | +1.983507 | -8.504679 | +11.463084 | between_or_equal |
| canonical-overflow | overflow | update16 | wall_ns | -9.096429 | +4.665922 | -13.148837 | between_or_equal |
| canonical-overflow | overflow | update16 | server_process_cpu_ns | -6.796884 | -0.045258 | -6.754683 | lower_than_both |
| canonical-overflow | overflow | update16 | client_process_cpu_ns | -6.048013 | -3.238397 | -2.903647 | lower_than_both |
| canonical-overflow | overflow | vacuum | wall_ns | +2.145571 | +10.589864 | -7.635684 | higher_than_both |
| canonical-overflow | overflow | vacuum | server_process_cpu_ns | -7.716358 | -0.000103 | -7.716263 | lower_than_both |
| canonical-overflow | overflow | vacuum | client_process_cpu_ns | -2.668011 | -6.994494 | +4.651856 | lower_than_both |
| direct-hit | fixed | direct_full_no_order | wall_ns | -17.664524 | -10.247051 | -8.264323 | lower_than_both |
| direct-hit | fixed | direct_full_no_order | server_process_cpu_ns | -31.787938 | -32.378225 | +0.872924 | lower_than_both |
| direct-hit | fixed | direct_full_no_order | client_process_cpu_ns | -1.805687 | +0.701434 | -2.489658 | between_or_equal |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | +18.449787 | +6.416175 | +11.308066 | higher_than_both |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | +26.185699 | +25.840039 | +0.274682 | higher_than_both |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | +1.361221 | +1.598482 | -0.233528 | higher_than_both |
| direct-hit | overflow | direct_full_no_order | wall_ns | -9.342207 | -14.821796 | +6.433088 | lower_than_both |
| direct-hit | overflow | direct_full_no_order | server_process_cpu_ns | -10.873681 | -22.710252 | +15.314541 | lower_than_both |
| direct-hit | overflow | direct_full_no_order | client_process_cpu_ns | +6.272613 | -6.711555 | +13.918302 | between_or_equal |
| direct-hit | overflow | direct_id_only_no_order | wall_ns | +9.238647 | -1.894159 | +11.347750 | between_or_equal |
| direct-hit | overflow | direct_id_only_no_order | server_process_cpu_ns | +8.678284 | -2.529027 | +11.498102 | between_or_equal |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | +28.633672 | +26.597675 | +1.608242 | higher_than_both |
| direct-hit | single | direct_full_no_order | wall_ns | -30.904866 | -32.178816 | +1.878395 | lower_than_both |
| direct-hit | single | direct_full_no_order | server_process_cpu_ns | -34.526843 | -34.115282 | -0.624668 | lower_than_both |
| direct-hit | single | direct_full_no_order | client_process_cpu_ns | +15.705660 | -1.270275 | +17.194351 | between_or_equal |
| direct-hit | single | direct_id_only_no_order | wall_ns | +8.026427 | +3.651565 | +4.220739 | higher_than_both |
| direct-hit | single | direct_id_only_no_order | server_process_cpu_ns | +4.195690 | -2.201777 | +6.541496 | between_or_equal |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | +5.231742 | +5.317329 | -0.081266 | higher_than_both |

Wall/clientCPU include socket envelope framing and raw buffering through ReadyI; full decoding/oracles/raw file writes and proc snapshots are outside wall. Process snapshots and clocks are non-atomic. RSS/IO endpoints are not peaks. Direct id-only and full projection include common scan/TOAST costs; neither latency nor their difference proves an exclusive bitmap-copy cost. Every negative result and maximum remains in the summary.

### 中间C1完整结果（保留原负控，不能与C2跨窗口拼接）

C1出现非空列仍复制位图的代码浪费后，C2仅增加选列声明条件。C1原270点及其三时钟分布全部保留。C1和C2属于独立时间窗口，各自只能与各自匹配的两次旧版比较。

#### P30 finite ordinary operation and direct projection costs

Each raw value is retained from three independent contexts. Canonical ORDER BY samples and separately named direct no-ORDER controls stay separate. No exclusions, stable ratio, PG speed, or whole-database claim. Both generations use P29C2-compatible command-only DELETE.

| Cohort | Scope | Region | Clock | Role | raw3 ms | min ms | p50 ms | max ms |
|---|---|---|---|---|---|---:|---:|---:|
| canonical-fixed-single | fixed | delete16 | wall_ns | before-first | 56.956362, 56.820393, 46.453948 | 46.453948 | 56.820393 | 56.956362 |
| canonical-fixed-single | fixed | delete16 | wall_ns | candidate | 52.458261, 53.964088, 51.479705 | 51.479705 | 52.458261 | 53.964088 |
| canonical-fixed-single | fixed | delete16 | wall_ns | before-return | 52.479297, 56.028028, 32.188001 | 32.188001 | 52.479297 | 56.028028 |
| canonical-fixed-single | fixed | delete16 | server_process_cpu_ns | before-first | 46.902056, 45.393685, 40.926870 | 40.926870 | 45.393685 | 46.902056 |
| canonical-fixed-single | fixed | delete16 | server_process_cpu_ns | candidate | 39.844591, 40.600974, 40.707793 | 39.844591 | 40.600974 | 40.707793 |
| canonical-fixed-single | fixed | delete16 | server_process_cpu_ns | before-return | 42.523860, 47.776001, 25.328383 | 25.328383 | 42.523860 | 47.776001 |
| canonical-fixed-single | fixed | delete16 | client_process_cpu_ns | before-first | 0.235830, 0.233578, 0.227394 | 0.227394 | 0.233578 | 0.235830 |
| canonical-fixed-single | fixed | delete16 | client_process_cpu_ns | candidate | 0.206105, 0.202540, 0.217284 | 0.202540 | 0.206105 | 0.217284 |
| canonical-fixed-single | fixed | delete16 | client_process_cpu_ns | before-return | 0.210007, 0.207266, 0.228620 | 0.207266 | 0.210007 | 0.228620 |
| canonical-fixed-single | fixed | insert1 | wall_ns | before-first | 19.752385, 23.141644, 17.605909 | 17.605909 | 19.752385 | 23.141644 |
| canonical-fixed-single | fixed | insert1 | wall_ns | candidate | 15.569312, 18.718330, 15.671522 | 15.569312 | 15.671522 | 18.718330 |
| canonical-fixed-single | fixed | insert1 | wall_ns | before-return | 22.852766, 22.325886, 13.378862 | 13.378862 | 22.325886 | 22.852766 |
| canonical-fixed-single | fixed | insert1 | server_process_cpu_ns | before-first | 9.600602, 8.671270, 9.276607 | 8.671270 | 9.276607 | 9.600602 |
| canonical-fixed-single | fixed | insert1 | server_process_cpu_ns | candidate | 7.417316, 9.832926, 7.461584 | 7.417316 | 7.461584 | 9.832926 |
| canonical-fixed-single | fixed | insert1 | server_process_cpu_ns | before-return | 6.596799, 9.032617, 6.401796 | 6.401796 | 6.596799 | 9.032617 |
| canonical-fixed-single | fixed | insert1 | client_process_cpu_ns | before-first | 0.244958, 0.240597, 0.259053 | 0.240597 | 0.244958 | 0.259053 |
| canonical-fixed-single | fixed | insert1 | client_process_cpu_ns | candidate | 0.194490, 0.128274, 0.222187 | 0.128274 | 0.194490 | 0.222187 |
| canonical-fixed-single | fixed | insert1 | client_process_cpu_ns | before-return | 0.197711, 0.204930, 0.203176 | 0.197711 | 0.203176 | 0.204930 |
| canonical-fixed-single | fixed | reinsert_population | wall_ns | before-first | 629.129363, 593.626120, 601.697749 | 593.626120 | 601.697749 | 629.129363 |
| canonical-fixed-single | fixed | reinsert_population | wall_ns | candidate | 678.000721, 566.728029, 1008.026919 | 566.728029 | 678.000721 | 1008.026919 |
| canonical-fixed-single | fixed | reinsert_population | wall_ns | before-return | 711.410540, 661.978892, 629.063432 | 629.063432 | 661.978892 | 711.410540 |
| canonical-fixed-single | fixed | reinsert_population | server_process_cpu_ns | before-first | 578.009549, 518.023620, 577.951980 | 518.023620 | 577.951980 | 578.009549 |
| canonical-fixed-single | fixed | reinsert_population | server_process_cpu_ns | candidate | 571.158086, 486.042943, 568.475671 | 486.042943 | 568.475671 | 571.158086 |
| canonical-fixed-single | fixed | reinsert_population | server_process_cpu_ns | before-return | 580.012830, 589.520866, 599.538656 | 580.012830 | 589.520866 | 599.538656 |
| canonical-fixed-single | fixed | reinsert_population | client_process_cpu_ns | before-first | 0.202152, 0.228617, 0.222346 | 0.202152 | 0.222346 | 0.228617 |
| canonical-fixed-single | fixed | reinsert_population | client_process_cpu_ns | candidate | 0.260175, 0.210550, 0.226220 | 0.210550 | 0.226220 | 0.260175 |
| canonical-fixed-single | fixed | reinsert_population | client_process_cpu_ns | before-return | 0.252384, 0.241393, 0.239976 | 0.239976 | 0.241393 | 0.252384 |
| canonical-fixed-single | fixed | select_after_update | wall_ns | before-first | 34.437410, 37.503683, 29.946622 | 29.946622 | 34.437410 | 37.503683 |
| canonical-fixed-single | fixed | select_after_update | wall_ns | candidate | 28.133161, 28.888836, 30.905798 | 28.133161 | 28.888836 | 30.905798 |
| canonical-fixed-single | fixed | select_after_update | wall_ns | before-return | 35.241721, 39.137178, 21.854171 | 21.854171 | 35.241721 | 39.137178 |
| canonical-fixed-single | fixed | select_after_update | server_process_cpu_ns | before-first | 23.457418, 30.297296, 21.523935 | 21.523935 | 23.457418 | 30.297296 |
| canonical-fixed-single | fixed | select_after_update | server_process_cpu_ns | candidate | 19.220559, 19.125526, 20.208365 | 19.125526 | 19.220559 | 20.208365 |
| canonical-fixed-single | fixed | select_after_update | server_process_cpu_ns | before-return | 26.828309, 32.004599, 13.819242 | 13.819242 | 26.828309 | 32.004599 |
| canonical-fixed-single | fixed | select_after_update | client_process_cpu_ns | before-first | 5.007372, 4.976882, 5.035876 | 4.976882 | 5.007372 | 5.035876 |
| canonical-fixed-single | fixed | select_after_update | client_process_cpu_ns | candidate | 4.887063, 4.972843, 4.747969 | 4.747969 | 4.887063 | 4.972843 |
| canonical-fixed-single | fixed | select_after_update | client_process_cpu_ns | before-return | 4.897602, 4.962023, 5.090556 | 4.897602 | 4.962023 | 5.090556 |
| canonical-fixed-single | fixed | select_after_vacuum | wall_ns | before-first | 32.473964, 30.297154, 26.425230 | 26.425230 | 30.297154 | 32.473964 |
| canonical-fixed-single | fixed | select_after_vacuum | wall_ns | candidate | 29.850373, 25.032206, 28.213278 | 25.032206 | 28.213278 | 29.850373 |
| canonical-fixed-single | fixed | select_after_vacuum | wall_ns | before-return | 37.952145, 30.606918, 21.856263 | 21.856263 | 30.606918 | 37.952145 |
| canonical-fixed-single | fixed | select_after_vacuum | server_process_cpu_ns | before-first | 25.330613, 20.413647, 20.602175 | 20.413647 | 20.602175 | 25.330613 |
| canonical-fixed-single | fixed | select_after_vacuum | server_process_cpu_ns | candidate | 21.013018, 16.215058, 19.467629 | 16.215058 | 19.467629 | 21.013018 |
| canonical-fixed-single | fixed | select_after_vacuum | server_process_cpu_ns | before-return | 24.346430, 22.608750, 14.907525 | 14.907525 | 22.608750 | 24.346430 |
| canonical-fixed-single | fixed | select_after_vacuum | client_process_cpu_ns | before-first | 4.712243, 4.735963, 4.686073 | 4.686073 | 4.712243 | 4.735963 |
| canonical-fixed-single | fixed | select_after_vacuum | client_process_cpu_ns | candidate | 4.708119, 4.681336, 4.523662 | 4.523662 | 4.681336 | 4.708119 |
| canonical-fixed-single | fixed | select_after_vacuum | client_process_cpu_ns | before-return | 4.599225, 4.676386, 4.667462 | 4.599225 | 4.667462 | 4.676386 |
| canonical-fixed-single | fixed | truncate | wall_ns | before-first | 299.074751, 276.115235, 261.678870 | 261.678870 | 276.115235 | 299.074751 |
| canonical-fixed-single | fixed | truncate | wall_ns | candidate | 410.799185, 594.700044, 251.184979 | 251.184979 | 410.799185 | 594.700044 |
| canonical-fixed-single | fixed | truncate | wall_ns | before-return | 340.849863, 284.474201, 322.651771 | 284.474201 | 322.651771 | 340.849863 |
| canonical-fixed-single | fixed | truncate | server_process_cpu_ns | before-first | 94.423033, 94.094295, 92.779692 | 92.779692 | 94.094295 | 94.423033 |
| canonical-fixed-single | fixed | truncate | server_process_cpu_ns | candidate | 90.279711, 110.694984, 82.455917 | 82.455917 | 90.279711 | 110.694984 |
| canonical-fixed-single | fixed | truncate | server_process_cpu_ns | before-return | 101.294599, 94.608029, 84.740897 | 84.740897 | 94.608029 | 101.294599 |
| canonical-fixed-single | fixed | truncate | client_process_cpu_ns | before-first | 0.247217, 0.227944, 0.272378 | 0.227944 | 0.247217 | 0.272378 |
| canonical-fixed-single | fixed | truncate | client_process_cpu_ns | candidate | 0.215820, 0.238921, 0.232440 | 0.215820 | 0.232440 | 0.238921 |
| canonical-fixed-single | fixed | truncate | client_process_cpu_ns | before-return | 0.233710, 0.183525, 0.247194 | 0.183525 | 0.233710 | 0.247194 |
| canonical-fixed-single | fixed | update16 | wall_ns | before-first | 95.778802, 126.484778, 81.660753 | 81.660753 | 95.778802 | 126.484778 |
| canonical-fixed-single | fixed | update16 | wall_ns | candidate | 75.858522, 68.177145, 83.903830 | 68.177145 | 75.858522 | 83.903830 |
| canonical-fixed-single | fixed | update16 | wall_ns | before-return | 93.677073, 163.256541, 53.916445 | 53.916445 | 93.677073 | 163.256541 |
| canonical-fixed-single | fixed | update16 | server_process_cpu_ns | before-first | 79.397623, 75.307109, 70.761741 | 70.761741 | 75.307109 | 79.397623 |
| canonical-fixed-single | fixed | update16 | server_process_cpu_ns | candidate | 64.195784, 56.468783, 67.958820 | 56.468783 | 64.195784 | 67.958820 |
| canonical-fixed-single | fixed | update16 | server_process_cpu_ns | before-return | 70.055183, 69.578936, 46.558534 | 46.558534 | 69.578936 | 70.055183 |
| canonical-fixed-single | fixed | update16 | client_process_cpu_ns | before-first | 0.274564, 0.239570, 0.236953 | 0.236953 | 0.239570 | 0.274564 |
| canonical-fixed-single | fixed | update16 | client_process_cpu_ns | candidate | 0.207953, 0.180533, 0.231570 | 0.180533 | 0.207953 | 0.231570 |
| canonical-fixed-single | fixed | update16 | client_process_cpu_ns | before-return | 0.197260, 0.217162, 0.211994 | 0.197260 | 0.211994 | 0.217162 |
| canonical-fixed-single | fixed | vacuum | wall_ns | before-first | 1.708623, 1.513204, 1.637617 | 1.513204 | 1.637617 | 1.708623 |
| canonical-fixed-single | fixed | vacuum | wall_ns | candidate | 1.727810, 1.251736, 1.509834 | 1.251736 | 1.509834 | 1.727810 |
| canonical-fixed-single | fixed | vacuum | wall_ns | before-return | 1.641035, 1.834918, 1.159241 | 1.159241 | 1.641035 | 1.834918 |
| canonical-fixed-single | fixed | vacuum | server_process_cpu_ns | before-first | 1.623498, 1.375773, 1.513919 | 1.375773 | 1.513919 | 1.623498 |
| canonical-fixed-single | fixed | vacuum | server_process_cpu_ns | candidate | 1.586439, 1.189619, 1.405997 | 1.189619 | 1.405997 | 1.586439 |
| canonical-fixed-single | fixed | vacuum | server_process_cpu_ns | before-return | 1.537326, 1.696093, 1.026060 | 1.026060 | 1.537326 | 1.696093 |
| canonical-fixed-single | fixed | vacuum | client_process_cpu_ns | before-first | 0.136635, 0.228810, 0.203616 | 0.136635 | 0.203616 | 0.228810 |
| canonical-fixed-single | fixed | vacuum | client_process_cpu_ns | candidate | 0.231009, 0.123916, 0.195974 | 0.123916 | 0.195974 | 0.231009 |
| canonical-fixed-single | fixed | vacuum | client_process_cpu_ns | before-return | 0.211688, 0.198764, 0.192924 | 0.192924 | 0.198764 | 0.211688 |
| canonical-fixed-single | single | delete16 | wall_ns | before-first | 79.443717, 85.734313, 67.402433 | 67.402433 | 79.443717 | 85.734313 |
| canonical-fixed-single | single | delete16 | wall_ns | candidate | 87.569480, 78.949554, 79.624669 | 78.949554 | 79.624669 | 87.569480 |
| canonical-fixed-single | single | delete16 | wall_ns | before-return | 81.660060, 104.554762, 79.403060 | 79.403060 | 81.660060 | 104.554762 |
| canonical-fixed-single | single | delete16 | server_process_cpu_ns | before-first | 66.019863, 69.011315, 59.629982 | 59.629982 | 66.019863 | 69.011315 |
| canonical-fixed-single | single | delete16 | server_process_cpu_ns | candidate | 67.256469, 64.427977, 65.370911 | 64.427977 | 65.370911 | 67.256469 |
| canonical-fixed-single | single | delete16 | server_process_cpu_ns | before-return | 66.924544, 68.019839, 69.606537 | 66.924544 | 68.019839 | 69.606537 |
| canonical-fixed-single | single | delete16 | client_process_cpu_ns | before-first | 0.217120, 0.182945, 0.205680 | 0.182945 | 0.205680 | 0.217120 |
| canonical-fixed-single | single | delete16 | client_process_cpu_ns | candidate | 0.235985, 0.221664, 0.280920 | 0.221664 | 0.235985 | 0.280920 |
| canonical-fixed-single | single | delete16 | client_process_cpu_ns | before-return | 0.235732, 0.234621, 0.262243 | 0.234621 | 0.235732 | 0.262243 |
| canonical-fixed-single | single | insert1 | wall_ns | before-first | 19.774315, 27.585559, 17.161605 | 17.161605 | 19.774315 | 27.585559 |
| canonical-fixed-single | single | insert1 | wall_ns | candidate | 38.429982, 17.274803, 32.662346 | 17.274803 | 32.662346 | 38.429982 |
| canonical-fixed-single | single | insert1 | wall_ns | before-return | 25.802537, 34.165346, 16.767427 | 16.767427 | 25.802537 | 34.165346 |
| canonical-fixed-single | single | insert1 | server_process_cpu_ns | before-first | 8.960694, 11.474740, 10.310108 | 8.960694 | 10.310108 | 11.474740 |
| canonical-fixed-single | single | insert1 | server_process_cpu_ns | candidate | 16.665117, 8.453622, 12.770672 | 8.453622 | 12.770672 | 16.665117 |
| canonical-fixed-single | single | insert1 | server_process_cpu_ns | before-return | 12.414159, 10.318575, 9.473538 | 9.473538 | 10.318575 | 12.414159 |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | before-first | 0.207741, 0.211147, 0.235427 | 0.207741 | 0.211147 | 0.235427 |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | candidate | 0.220487, 0.219311, 0.280424 | 0.219311 | 0.220487 | 0.280424 |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | before-return | 0.225095, 0.206436, 0.221597 | 0.206436 | 0.221597 | 0.225095 |
| canonical-fixed-single | single | reinsert_population | wall_ns | before-first | 1054.356052, 1206.001312, 1046.602323 | 1046.602323 | 1054.356052 | 1206.001312 |
| canonical-fixed-single | single | reinsert_population | wall_ns | candidate | 1280.689896, 1171.107182, 1133.100757 | 1133.100757 | 1171.107182 | 1280.689896 |
| canonical-fixed-single | single | reinsert_population | wall_ns | before-return | 1124.388725, 1297.057645, 1297.828991 | 1124.388725 | 1297.057645 | 1297.828991 |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | before-first | 1015.938109, 1005.160632, 1001.260124 | 1001.260124 | 1005.160632 | 1015.938109 |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | candidate | 1046.087078, 1059.613119, 1050.790226 | 1046.087078 | 1050.790226 | 1059.613119 |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | before-return | 975.869021, 1075.728236, 1076.012158 | 975.869021 | 1075.728236 | 1076.012158 |
| canonical-fixed-single | single | reinsert_population | client_process_cpu_ns | before-first | 0.445566, 0.583057, 0.619721 | 0.445566 | 0.583057 | 0.619721 |
| canonical-fixed-single | single | reinsert_population | client_process_cpu_ns | candidate | 0.576652, 0.733047, 0.408228 | 0.408228 | 0.576652 | 0.733047 |
| canonical-fixed-single | single | reinsert_population | client_process_cpu_ns | before-return | 0.656154, 0.827458, 0.572598 | 0.572598 | 0.656154 | 0.827458 |
| canonical-fixed-single | single | select_after_update | wall_ns | before-first | 57.638818, 66.682082, 44.831266 | 44.831266 | 57.638818 | 66.682082 |
| canonical-fixed-single | single | select_after_update | wall_ns | candidate | 62.857534, 59.595466, 54.007724 | 54.007724 | 59.595466 | 62.857534 |
| canonical-fixed-single | single | select_after_update | wall_ns | before-return | 61.367688, 58.513167, 48.460008 | 48.460008 | 58.513167 | 61.367688 |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | before-first | 47.474169, 57.928616, 38.206714 | 38.206714 | 47.474169 | 57.928616 |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | candidate | 46.809731, 49.452914, 45.453549 | 45.453549 | 46.809731 | 49.452914 |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | before-return | 53.538900, 47.184728, 42.258164 | 42.258164 | 47.184728 | 53.538900 |
| canonical-fixed-single | single | select_after_update | client_process_cpu_ns | before-first | 5.519942, 6.119009, 5.931742 | 5.519942 | 5.931742 | 6.119009 |
| canonical-fixed-single | single | select_after_update | client_process_cpu_ns | candidate | 6.014108, 6.175658, 6.046671 | 6.014108 | 6.046671 | 6.175658 |
| canonical-fixed-single | single | select_after_update | client_process_cpu_ns | before-return | 5.966102, 6.053548, 5.969063 | 5.966102 | 5.969063 | 6.053548 |
| canonical-fixed-single | single | select_after_vacuum | wall_ns | before-first | 55.513189, 63.496334, 64.460089 | 55.513189 | 63.496334 | 64.460089 |
| canonical-fixed-single | single | select_after_vacuum | wall_ns | candidate | 66.158341, 63.078758, 56.527759 | 56.527759 | 63.078758 | 66.158341 |
| canonical-fixed-single | single | select_after_vacuum | wall_ns | before-return | 53.425548, 67.759942, 57.509317 | 53.425548 | 57.509317 | 67.759942 |
| canonical-fixed-single | single | select_after_vacuum | server_process_cpu_ns | before-first | 50.637087, 51.668932, 51.284656 | 50.637087 | 51.284656 | 51.668932 |
| canonical-fixed-single | single | select_after_vacuum | server_process_cpu_ns | candidate | 59.288191, 56.468172, 46.121074 | 46.121074 | 56.468172 | 59.288191 |
| canonical-fixed-single | single | select_after_vacuum | server_process_cpu_ns | before-return | 46.487711, 58.630100, 50.246526 | 46.487711 | 50.246526 | 58.630100 |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | before-first | 3.468626, 5.713104, 5.428990 | 3.468626 | 5.428990 | 5.713104 |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | candidate | 5.125292, 5.615860, 5.527629 | 5.125292 | 5.527629 | 5.615860 |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | before-return | 4.981683, 5.577364, 5.577939 | 4.981683 | 5.577364 | 5.577939 |
| canonical-fixed-single | single | truncate | wall_ns | before-first | 236.442834, 525.281393, 211.999039 | 211.999039 | 236.442834 | 525.281393 |
| canonical-fixed-single | single | truncate | wall_ns | candidate | 257.454643, 266.822044, 292.793244 | 257.454643 | 266.822044 | 292.793244 |
| canonical-fixed-single | single | truncate | wall_ns | before-return | 278.623175, 264.475115, 247.870215 | 247.870215 | 264.475115 | 278.623175 |
| canonical-fixed-single | single | truncate | server_process_cpu_ns | before-first | 116.233960, 126.925945, 123.654360 | 116.233960 | 123.654360 | 126.925945 |
| canonical-fixed-single | single | truncate | server_process_cpu_ns | candidate | 106.634007, 111.089995, 113.882253 | 106.634007 | 111.089995 | 113.882253 |
| canonical-fixed-single | single | truncate | server_process_cpu_ns | before-return | 140.331445, 126.956839, 117.856219 | 117.856219 | 126.956839 | 140.331445 |
| canonical-fixed-single | single | truncate | client_process_cpu_ns | before-first | 0.167420, 0.280844, 0.275102 | 0.167420 | 0.275102 | 0.280844 |
| canonical-fixed-single | single | truncate | client_process_cpu_ns | candidate | 0.213796, 0.160982, 0.217165 | 0.160982 | 0.213796 | 0.217165 |
| canonical-fixed-single | single | truncate | client_process_cpu_ns | before-return | 0.203012, 0.251142, 0.288372 | 0.203012 | 0.251142 | 0.288372 |
| canonical-fixed-single | single | update16 | wall_ns | before-first | 148.720740, 206.852400, 137.395580 | 137.395580 | 148.720740 | 206.852400 |
| canonical-fixed-single | single | update16 | wall_ns | candidate | 172.656587, 682.296976, 173.933444 | 172.656587 | 173.933444 | 682.296976 |
| canonical-fixed-single | single | update16 | wall_ns | before-return | 170.943780, 173.956297, 148.301935 | 148.301935 | 170.943780 | 173.956297 |
| canonical-fixed-single | single | update16 | server_process_cpu_ns | before-first | 125.209537, 127.437444, 122.824804 | 122.824804 | 125.209537 | 127.437444 |
| canonical-fixed-single | single | update16 | server_process_cpu_ns | candidate | 140.136565, 107.503944, 140.492506 | 107.503944 | 140.136565 | 140.492506 |
| canonical-fixed-single | single | update16 | server_process_cpu_ns | before-return | 132.914983, 130.428820, 127.002784 | 127.002784 | 130.428820 | 132.914983 |
| canonical-fixed-single | single | update16 | client_process_cpu_ns | before-first | 0.205464, 0.246960, 0.224163 | 0.205464 | 0.224163 | 0.246960 |
| canonical-fixed-single | single | update16 | client_process_cpu_ns | candidate | 0.261449, 0.237005, 0.264667 | 0.237005 | 0.261449 | 0.264667 |
| canonical-fixed-single | single | update16 | client_process_cpu_ns | before-return | 0.208457, 0.237596, 0.268134 | 0.208457 | 0.237596 | 0.268134 |
| canonical-fixed-single | single | vacuum | wall_ns | before-first | 7.443456, 9.900620, 7.387852 | 7.387852 | 7.443456 | 9.900620 |
| canonical-fixed-single | single | vacuum | wall_ns | candidate | 8.697954, 8.591340, 8.263111 | 8.263111 | 8.591340 | 8.697954 |
| canonical-fixed-single | single | vacuum | wall_ns | before-return | 9.016777, 11.924844, 8.062179 | 8.062179 | 9.016777 | 11.924844 |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | before-first | 7.066929, 8.236457, 6.923481 | 6.923481 | 7.066929 | 8.236457 |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | candidate | 8.389663, 8.225687, 8.005162 | 8.005162 | 8.225687 | 8.389663 |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | before-return | 8.887188, 11.626561, 7.772967 | 7.772967 | 8.887188 | 11.626561 |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | before-first | 0.294685, 0.253597, 0.196585 | 0.196585 | 0.253597 | 0.294685 |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | candidate | 0.211311, 0.250007, 0.233914 | 0.211311 | 0.233914 | 0.250007 |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | before-return | 0.204732, 0.211040, 0.228676 | 0.204732 | 0.211040 | 0.228676 |
| canonical-overflow | overflow | delete16 | wall_ns | before-first | 201.540685, 82.557518, 72.029647 | 72.029647 | 82.557518 | 201.540685 |
| canonical-overflow | overflow | delete16 | wall_ns | candidate | 81.970246, 66.525256, 70.190848 | 66.525256 | 70.190848 | 81.970246 |
| canonical-overflow | overflow | delete16 | wall_ns | before-return | 82.288720, 50.754271, 77.551788 | 50.754271 | 77.551788 | 82.288720 |
| canonical-overflow | overflow | delete16 | server_process_cpu_ns | before-first | 74.745273, 67.336422, 60.798550 | 60.798550 | 67.336422 | 74.745273 |
| canonical-overflow | overflow | delete16 | server_process_cpu_ns | candidate | 67.138767, 59.535619, 54.370393 | 54.370393 | 59.535619 | 67.138767 |
| canonical-overflow | overflow | delete16 | server_process_cpu_ns | before-return | 66.922601, 43.706604, 62.887771 | 43.706604 | 62.887771 | 66.922601 |
| canonical-overflow | overflow | delete16 | client_process_cpu_ns | before-first | 0.216929, 0.202361, 0.231171 | 0.202361 | 0.216929 | 0.231171 |
| canonical-overflow | overflow | delete16 | client_process_cpu_ns | candidate | 0.218920, 0.256408, 0.255931 | 0.218920 | 0.255931 | 0.256408 |
| canonical-overflow | overflow | delete16 | client_process_cpu_ns | before-return | 0.218588, 0.273158, 0.249684 | 0.218588 | 0.249684 | 0.273158 |
| canonical-overflow | overflow | insert1 | wall_ns | before-first | 38.135013, 20.477450, 24.605831 | 20.477450 | 24.605831 | 38.135013 |
| canonical-overflow | overflow | insert1 | wall_ns | candidate | 24.590844, 16.298524, 29.097553 | 16.298524 | 24.590844 | 29.097553 |
| canonical-overflow | overflow | insert1 | wall_ns | before-return | 20.401567, 20.408625, 25.416382 | 20.401567 | 20.408625 | 25.416382 |
| canonical-overflow | overflow | insert1 | server_process_cpu_ns | before-first | 16.658749, 8.749765, 11.868454 | 8.749765 | 11.868454 | 16.658749 |
| canonical-overflow | overflow | insert1 | server_process_cpu_ns | candidate | 10.838313, 9.520486, 10.333594 | 9.520486 | 10.333594 | 10.838313 |
| canonical-overflow | overflow | insert1 | server_process_cpu_ns | before-return | 8.649516, 10.755549, 11.255325 | 8.649516 | 10.755549 | 11.255325 |
| canonical-overflow | overflow | insert1 | client_process_cpu_ns | before-first | 0.242432, 0.202574, 0.202243 | 0.202243 | 0.202574 | 0.242432 |
| canonical-overflow | overflow | insert1 | client_process_cpu_ns | candidate | 0.241818, 0.204601, 0.219317 | 0.204601 | 0.219317 | 0.241818 |
| canonical-overflow | overflow | insert1 | client_process_cpu_ns | before-return | 0.213000, 0.234754, 0.225304 | 0.213000 | 0.225304 | 0.234754 |
| canonical-overflow | overflow | reinsert_population | wall_ns | before-first | 1183.293057, 1155.410908, 1085.456408 | 1085.456408 | 1155.410908 | 1183.293057 |
| canonical-overflow | overflow | reinsert_population | wall_ns | candidate | 1130.806202, 1226.653702, 1418.307522 | 1130.806202 | 1226.653702 | 1418.307522 |
| canonical-overflow | overflow | reinsert_population | wall_ns | before-return | 1063.290751, 1082.693052, 1484.747853 | 1063.290751 | 1082.693052 | 1484.747853 |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | before-first | 1030.212109, 1046.304710, 1024.986736 | 1024.986736 | 1030.212109 | 1046.304710 |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | candidate | 1081.618982, 1168.058724, 1052.718593 | 1052.718593 | 1081.618982 | 1168.058724 |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | before-return | 948.658340, 1035.052603, 1035.677591 | 948.658340 | 1035.052603 | 1035.677591 |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | before-first | 0.568356, 1.046712, 1.043951 | 0.568356 | 1.043951 | 1.046712 |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | candidate | 0.874457, 0.751343, 0.564031 | 0.564031 | 0.751343 | 0.874457 |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | before-return | 0.512326, 0.906944, 0.733709 | 0.512326 | 0.733709 | 0.906944 |
| canonical-overflow | overflow | select_after_update | wall_ns | before-first | 79.527662, 70.900789, 66.402756 | 66.402756 | 70.900789 | 79.527662 |
| canonical-overflow | overflow | select_after_update | wall_ns | candidate | 66.839702, 45.777177, 58.174108 | 45.777177 | 58.174108 | 66.839702 |
| canonical-overflow | overflow | select_after_update | wall_ns | before-return | 62.669885, 48.182307, 64.171820 | 48.182307 | 62.669885 | 64.171820 |
| canonical-overflow | overflow | select_after_update | server_process_cpu_ns | before-first | 66.873538, 61.677338, 56.413639 | 56.413639 | 61.677338 | 66.873538 |
| canonical-overflow | overflow | select_after_update | server_process_cpu_ns | candidate | 59.543080, 39.312539, 50.093168 | 39.312539 | 50.093168 | 59.543080 |
| canonical-overflow | overflow | select_after_update | server_process_cpu_ns | before-return | 55.632672, 41.991123, 47.669007 | 41.991123 | 47.669007 | 55.632672 |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | before-first | 4.097020, 2.861270, 4.166297 | 2.861270 | 4.097020 | 4.166297 |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | candidate | 3.446885, 4.147805, 4.391432 | 3.446885 | 4.147805 | 4.391432 |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | before-return | 4.238850, 3.973993, 4.711392 | 3.973993 | 4.238850 | 4.711392 |
| canonical-overflow | overflow | select_after_vacuum | wall_ns | before-first | 68.188457, 58.480205, 64.672118 | 58.480205 | 64.672118 | 68.188457 |
| canonical-overflow | overflow | select_after_vacuum | wall_ns | candidate | 65.736727, 54.476366, 60.468130 | 54.476366 | 60.468130 | 65.736727 |
| canonical-overflow | overflow | select_after_vacuum | wall_ns | before-return | 58.290406, 60.951538, 47.861473 | 47.861473 | 58.290406 | 60.951538 |
| canonical-overflow | overflow | select_after_vacuum | server_process_cpu_ns | before-first | 60.939417, 51.980073, 57.181794 | 51.980073 | 57.181794 | 60.939417 |
| canonical-overflow | overflow | select_after_vacuum | server_process_cpu_ns | candidate | 56.286414, 50.028796, 53.738148 | 50.028796 | 53.738148 | 56.286414 |
| canonical-overflow | overflow | select_after_vacuum | server_process_cpu_ns | before-return | 50.419671, 55.822616, 41.584214 | 41.584214 | 50.419671 | 55.822616 |
| canonical-overflow | overflow | select_after_vacuum | client_process_cpu_ns | before-first | 3.953152, 3.441674, 3.854348 | 3.441674 | 3.854348 | 3.953152 |
| canonical-overflow | overflow | select_after_vacuum | client_process_cpu_ns | candidate | 3.429419, 3.082682, 3.758208 | 3.082682 | 3.429419 | 3.758208 |
| canonical-overflow | overflow | select_after_vacuum | client_process_cpu_ns | before-return | 2.919265, 3.953371, 4.065119 | 2.919265 | 3.953371 | 4.065119 |
| canonical-overflow | overflow | truncate | wall_ns | before-first | 204.950948, 278.673789, 316.541473 | 204.950948 | 278.673789 | 316.541473 |
| canonical-overflow | overflow | truncate | wall_ns | candidate | 382.091554, 269.843189, 253.399384 | 253.399384 | 269.843189 | 382.091554 |
| canonical-overflow | overflow | truncate | wall_ns | before-return | 282.690900, 242.207216, 185.353555 | 185.353555 | 242.207216 | 282.690900 |
| canonical-overflow | overflow | truncate | server_process_cpu_ns | before-first | 87.768008, 89.218139, 88.802337 | 87.768008 | 88.802337 | 89.218139 |
| canonical-overflow | overflow | truncate | server_process_cpu_ns | candidate | 79.794507, 85.257494, 72.422356 | 72.422356 | 79.794507 | 85.257494 |
| canonical-overflow | overflow | truncate | server_process_cpu_ns | before-return | 84.697131, 67.437173, 67.900986 | 67.437173 | 67.900986 | 84.697131 |
| canonical-overflow | overflow | truncate | client_process_cpu_ns | before-first | 0.268537, 0.194334, 0.234126 | 0.194334 | 0.234126 | 0.268537 |
| canonical-overflow | overflow | truncate | client_process_cpu_ns | candidate | 0.263667, 0.221327, 0.179691 | 0.179691 | 0.221327 | 0.263667 |
| canonical-overflow | overflow | truncate | client_process_cpu_ns | before-return | 0.156033, 0.276518, 0.179221 | 0.156033 | 0.179221 | 0.276518 |
| canonical-overflow | overflow | update16 | wall_ns | before-first | 228.319702, 213.798903, 180.621650 | 180.621650 | 213.798903 | 228.319702 |
| canonical-overflow | overflow | update16 | wall_ns | candidate | 176.143522, 148.900073, 185.315331 | 148.900073 | 176.143522 | 185.315331 |
| canonical-overflow | overflow | update16 | wall_ns | before-return | 180.047413, 160.947080, 165.107299 | 160.947080 | 165.107299 | 180.047413 |
| canonical-overflow | overflow | update16 | server_process_cpu_ns | before-first | 152.038182, 134.887944, 135.178537 | 134.887944 | 135.178537 | 152.038182 |
| canonical-overflow | overflow | update16 | server_process_cpu_ns | candidate | 140.208762, 132.671234, 158.256747 | 132.671234 | 140.208762 | 158.256747 |
| canonical-overflow | overflow | update16 | server_process_cpu_ns | before-return | 146.194600, 138.531920, 133.974392 | 133.974392 | 138.531920 | 146.194600 |
| canonical-overflow | overflow | update16 | client_process_cpu_ns | before-first | 0.313116, 0.244942, 0.280058 | 0.244942 | 0.280058 | 0.313116 |
| canonical-overflow | overflow | update16 | client_process_cpu_ns | candidate | 0.249920, 0.260208, 0.233693 | 0.233693 | 0.249920 | 0.260208 |
| canonical-overflow | overflow | update16 | client_process_cpu_ns | before-return | 0.272345, 0.318718, 0.258746 | 0.258746 | 0.272345 | 0.318718 |
| canonical-overflow | overflow | vacuum | wall_ns | before-first | 12.501427, 10.079584, 10.260390 | 10.079584 | 10.260390 | 12.501427 |
| canonical-overflow | overflow | vacuum | wall_ns | candidate | 10.809210, 9.176545, 9.752644 | 9.176545 | 9.752644 | 10.809210 |
| canonical-overflow | overflow | vacuum | wall_ns | before-return | 8.275736, 7.918835, 7.751233 | 7.751233 | 7.918835 | 8.275736 |
| canonical-overflow | overflow | vacuum | server_process_cpu_ns | before-first | 12.163291, 9.807407, 9.988835 | 9.807407 | 9.988835 | 12.163291 |
| canonical-overflow | overflow | vacuum | server_process_cpu_ns | candidate | 10.487442, 8.842019, 9.426560 | 8.842019 | 9.426560 | 10.487442 |
| canonical-overflow | overflow | vacuum | server_process_cpu_ns | before-return | 8.060957, 7.570325, 7.505007 | 7.505007 | 7.570325 | 8.060957 |
| canonical-overflow | overflow | vacuum | client_process_cpu_ns | before-first | 0.245375, 0.198529, 0.209126 | 0.198529 | 0.209126 | 0.245375 |
| canonical-overflow | overflow | vacuum | client_process_cpu_ns | candidate | 0.200588, 0.192139, 0.206661 | 0.192139 | 0.200588 | 0.206661 |
| canonical-overflow | overflow | vacuum | client_process_cpu_ns | before-return | 0.172627, 0.246390, 0.138753 | 0.138753 | 0.172627 | 0.246390 |
| direct-hit | fixed | direct_full_no_order | wall_ns | before-first | 32.026741, 24.785944, 33.615739 | 24.785944 | 32.026741 | 33.615739 |
| direct-hit | fixed | direct_full_no_order | wall_ns | candidate | 16.657725, 23.614763, 28.104402 | 16.657725 | 23.614763 | 28.104402 |
| direct-hit | fixed | direct_full_no_order | wall_ns | before-return | 42.256313, 72.155832, 29.883123 | 29.883123 | 42.256313 | 72.155832 |
| direct-hit | fixed | direct_full_no_order | server_process_cpu_ns | before-first | 23.797958, 15.559762, 22.991691 | 15.559762 | 22.991691 | 23.797958 |
| direct-hit | fixed | direct_full_no_order | server_process_cpu_ns | candidate | 12.098126, 18.180070, 18.784090 | 12.098126 | 18.180070 | 18.784090 |
| direct-hit | fixed | direct_full_no_order | server_process_cpu_ns | before-return | 30.650172, 29.575362, 20.378919 | 20.378919 | 29.575362 | 30.650172 |
| direct-hit | fixed | direct_full_no_order | client_process_cpu_ns | before-first | 5.030483, 4.814950, 4.742132 | 4.742132 | 4.814950 | 5.030483 |
| direct-hit | fixed | direct_full_no_order | client_process_cpu_ns | candidate | 2.845009, 5.038590, 4.948286 | 2.845009 | 4.948286 | 5.038590 |
| direct-hit | fixed | direct_full_no_order | client_process_cpu_ns | before-return | 4.864563, 5.029589, 4.859919 | 4.859919 | 4.864563 | 5.029589 |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | before-first | 20.857618, 19.663370, 20.962462 | 19.663370 | 20.857618 | 20.962462 |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | candidate | 18.196583, 24.205558, 22.744455 | 18.196583 | 22.744455 | 24.205558 |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | before-return | 24.807490, 56.226659, 19.564473 | 19.564473 | 24.807490 | 56.226659 |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | before-first | 14.743329, 8.751560, 11.753855 | 8.751560 | 11.753855 | 14.743329 |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | candidate | 9.779952, 15.680986, 14.681994 | 9.779952 | 14.681994 | 15.680986 |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | before-return | 9.387001, 14.142133, 11.684389 | 9.387001 | 11.684389 | 14.142133 |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | before-first | 4.962136, 4.913042, 4.724289 | 4.724289 | 4.913042 | 4.962136 |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | candidate | 4.567834, 4.896496, 4.907530 | 4.567834 | 4.896496 | 4.907530 |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | before-return | 4.755430, 4.934160, 4.845688 | 4.755430 | 4.845688 | 4.934160 |
| direct-hit | overflow | direct_full_no_order | wall_ns | before-first | 67.383094, 64.616293, 59.057867 | 59.057867 | 64.616293 | 67.383094 |
| direct-hit | overflow | direct_full_no_order | wall_ns | candidate | 49.007654, 57.801149, 48.254409 | 48.254409 | 49.007654 | 57.801149 |
| direct-hit | overflow | direct_full_no_order | wall_ns | before-return | 75.298762, 63.397601, 55.572064 | 55.572064 | 63.397601 | 75.298762 |
| direct-hit | overflow | direct_full_no_order | server_process_cpu_ns | before-first | 55.821998, 57.865022, 50.031515 | 50.031515 | 55.821998 | 57.865022 |
| direct-hit | overflow | direct_full_no_order | server_process_cpu_ns | candidate | 41.725245, 44.066907, 43.392765 | 41.725245 | 43.392765 | 44.066907 |
| direct-hit | overflow | direct_full_no_order | server_process_cpu_ns | before-return | 61.722851, 51.811294, 49.275988 | 49.275988 | 51.811294 | 61.722851 |
| direct-hit | overflow | direct_full_no_order | client_process_cpu_ns | before-first | 3.731915, 3.806667, 3.435514 | 3.435514 | 3.731915 | 3.806667 |
| direct-hit | overflow | direct_full_no_order | client_process_cpu_ns | candidate | 3.817442, 3.852525, 3.876909 | 3.817442 | 3.852525 | 3.876909 |
| direct-hit | overflow | direct_full_no_order | client_process_cpu_ns | before-return | 3.810249, 3.671732, 3.853936 | 3.671732 | 3.810249 | 3.853936 |
| direct-hit | overflow | direct_id_only_no_order | wall_ns | before-first | 48.425451, 54.334791, 43.744629 | 43.744629 | 48.425451 | 54.334791 |
| direct-hit | overflow | direct_id_only_no_order | wall_ns | candidate | 51.878421, 54.977198, 53.137514 | 51.878421 | 53.137514 | 54.977198 |
| direct-hit | overflow | direct_id_only_no_order | wall_ns | before-return | 34.695975, 36.067873, 52.272467 | 34.695975 | 36.067873 | 52.272467 |
| direct-hit | overflow | direct_id_only_no_order | server_process_cpu_ns | before-first | 42.360461, 44.476173, 37.183688 | 37.183688 | 42.360461 | 44.476173 |
| direct-hit | overflow | direct_id_only_no_order | server_process_cpu_ns | candidate | 42.092045, 43.436938, 42.541709 | 42.092045 | 42.541709 | 43.436938 |
| direct-hit | overflow | direct_id_only_no_order | server_process_cpu_ns | before-return | 22.748489, 31.341386, 43.886812 | 22.748489 | 31.341386 | 43.886812 |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | before-first | 2.619964, 2.181639, 2.515002 | 2.181639 | 2.515002 | 2.619964 |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | candidate | 1.970973, 1.883626, 2.442074 | 1.883626 | 1.970973 | 2.442074 |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | before-return | 2.367093, 1.929615, 2.401852 | 1.929615 | 2.367093 | 2.401852 |
| direct-hit | single | direct_full_no_order | wall_ns | before-first | 61.836997, 65.939658, 72.897785 | 61.836997 | 65.939658 | 72.897785 |
| direct-hit | single | direct_full_no_order | wall_ns | candidate | 50.255075, 52.259399, 54.573381 | 50.255075 | 52.259399 | 54.573381 |
| direct-hit | single | direct_full_no_order | wall_ns | before-return | 67.132906, 57.707021, 68.883943 | 57.707021 | 67.132906 | 68.883943 |
| direct-hit | single | direct_full_no_order | server_process_cpu_ns | before-first | 51.258260, 58.280732, 61.897321 | 51.258260 | 58.280732 | 61.897321 |
| direct-hit | single | direct_full_no_order | server_process_cpu_ns | candidate | 42.001279, 45.978789, 47.456237 | 42.001279 | 45.978789 | 47.456237 |
| direct-hit | single | direct_full_no_order | server_process_cpu_ns | before-return | 57.111410, 49.409807, 61.478127 | 49.409807 | 57.111410 | 61.478127 |
| direct-hit | single | direct_full_no_order | client_process_cpu_ns | before-first | 6.042448, 5.599858, 5.190885 | 5.190885 | 5.599858 | 6.042448 |
| direct-hit | single | direct_full_no_order | client_process_cpu_ns | candidate | 4.911367, 5.783600, 4.798745 | 4.798745 | 4.911367 | 5.783600 |
| direct-hit | single | direct_full_no_order | client_process_cpu_ns | before-return | 5.334912, 5.309126, 5.774485 | 5.309126 | 5.334912 | 5.774485 |
| direct-hit | single | direct_id_only_no_order | wall_ns | before-first | 45.301404, 47.849701, 46.482293 | 45.301404 | 46.482293 | 47.849701 |
| direct-hit | single | direct_id_only_no_order | wall_ns | candidate | 47.725251, 52.041242, 35.596653 | 35.596653 | 47.725251 | 52.041242 |
| direct-hit | single | direct_id_only_no_order | wall_ns | before-return | 48.764328, 42.213521, 58.024864 | 42.213521 | 48.764328 | 58.024864 |
| direct-hit | single | direct_id_only_no_order | server_process_cpu_ns | before-first | 39.914569, 40.147108, 38.409914 | 38.409914 | 39.914569 | 40.147108 |
| direct-hit | single | direct_id_only_no_order | server_process_cpu_ns | candidate | 38.457270, 40.699973, 29.789493 | 29.789493 | 38.457270 | 40.699973 |
| direct-hit | single | direct_id_only_no_order | server_process_cpu_ns | before-return | 40.006859, 35.887574, 51.956165 | 35.887574 | 40.006859 | 51.956165 |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | before-first | 3.868821, 4.020254, 4.075019 | 3.868821 | 4.020254 | 4.075019 |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | candidate | 4.295344, 3.716816, 3.537290 | 3.537290 | 3.716816 | 4.295344 |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | before-return | 3.789929, 3.559396, 5.452768 | 3.559396 | 3.789929 | 5.452768 |

| Cohort | Scope | Region | Clock | Candidate vs first % | Candidate vs return % | Return drift % | Class |
|---|---|---|---|---:|---:|---:|---|
| canonical-fixed-single | fixed | delete16 | wall_ns | -7.677054 | -0.040084 | -7.640032 | lower_than_both |
| canonical-fixed-single | fixed | delete16 | server_process_cpu_ns | -10.558101 | -4.521899 | -6.322080 | lower_than_both |
| canonical-fixed-single | fixed | delete16 | client_process_cpu_ns | -11.761810 | -1.858033 | -10.091276 | lower_than_both |
| canonical-fixed-single | fixed | insert1 | wall_ns | -20.660103 | -29.805599 | +13.028811 | lower_than_both |
| canonical-fixed-single | fixed | insert1 | server_process_cpu_ns | -19.565591 | +13.109161 | -28.887803 | between_or_equal |
| canonical-fixed-single | fixed | insert1 | client_process_cpu_ns | -20.602716 | -4.275111 | -17.056802 | lower_than_both |
| canonical-fixed-single | fixed | reinsert_population | wall_ns | +12.681279 | +2.420293 | +10.018509 | higher_than_both |
| canonical-fixed-single | fixed | reinsert_population | server_process_cpu_ns | -1.639636 | -3.569881 | +2.001704 | lower_than_both |
| canonical-fixed-single | fixed | reinsert_population | client_process_cpu_ns | +1.742330 | -6.285601 | +8.566379 | between_or_equal |
| canonical-fixed-single | fixed | select_after_update | wall_ns | -16.112054 | -18.026603 | +2.335573 | lower_than_both |
| canonical-fixed-single | fixed | select_after_update | server_process_cpu_ns | -18.061915 | -28.357173 | +14.370256 | lower_than_both |
| canonical-fixed-single | fixed | select_after_update | client_process_cpu_ns | -2.402638 | -1.510674 | -0.905645 | lower_than_both |
| canonical-fixed-single | fixed | select_after_vacuum | wall_ns | -6.878125 | -7.820585 | +1.022419 | lower_than_both |
| canonical-fixed-single | fixed | select_after_vacuum | server_process_cpu_ns | -5.506923 | -13.893386 | +9.739627 | lower_than_both |
| canonical-fixed-single | fixed | select_after_vacuum | client_process_cpu_ns | -0.655887 | +0.297249 | -0.950312 | between_or_equal |
| canonical-fixed-single | fixed | truncate | wall_ns | +48.778167 | +27.319675 | +16.854027 | higher_than_both |
| canonical-fixed-single | fixed | truncate | server_process_cpu_ns | -4.054001 | -4.575001 | +0.545978 | lower_than_both |
| canonical-fixed-single | fixed | truncate | client_process_cpu_ns | -5.977340 | -0.543408 | -5.463621 | lower_than_both |
| canonical-fixed-single | fixed | update16 | wall_ns | -20.798214 | -19.021251 | -2.194357 | lower_than_both |
| canonical-fixed-single | fixed | update16 | server_process_cpu_ns | -14.754683 | -7.736755 | -7.606417 | lower_than_both |
| canonical-fixed-single | fixed | update16 | client_process_cpu_ns | -13.197395 | -1.906186 | -11.510623 | lower_than_both |
| canonical-fixed-single | fixed | vacuum | wall_ns | -7.802984 | -7.995015 | +0.208718 | lower_than_both |
| canonical-fixed-single | fixed | vacuum | server_process_cpu_ns | -7.128651 | -8.542690 | +1.546120 | lower_than_both |
| canonical-fixed-single | fixed | vacuum | client_process_cpu_ns | -3.753143 | -1.403675 | -2.382917 | lower_than_both |
| canonical-fixed-single | single | delete16 | wall_ns | +0.227774 | -2.492517 | +2.789828 | between_or_equal |
| canonical-fixed-single | single | delete16 | server_process_cpu_ns | -0.982965 | -3.894346 | +3.029355 | lower_than_both |
| canonical-fixed-single | single | delete16 | client_process_cpu_ns | +14.734053 | +0.107325 | +14.611046 | higher_than_both |
| canonical-fixed-single | single | insert1 | wall_ns | +65.175613 | +26.585793 | +30.485112 | higher_than_both |
| canonical-fixed-single | single | insert1 | server_process_cpu_ns | +23.865550 | +23.763911 | +0.082123 | higher_than_both |
| canonical-fixed-single | single | insert1 | client_process_cpu_ns | +4.423459 | -0.500909 | +4.949159 | between_or_equal |
| canonical-fixed-single | single | reinsert_population | wall_ns | +11.073217 | -9.710475 | +23.018941 | between_or_equal |
| canonical-fixed-single | single | reinsert_population | server_process_cpu_ns | +4.539533 | -2.318244 | +7.020530 | between_or_equal |
| canonical-fixed-single | single | reinsert_population | client_process_cpu_ns | -1.098520 | -12.116363 | +12.536853 | lower_than_both |
| canonical-fixed-single | single | select_after_update | wall_ns | +3.394671 | +1.849667 | +1.516945 | higher_than_both |
| canonical-fixed-single | single | select_after_update | server_process_cpu_ns | -1.399578 | -0.794742 | -0.609681 | lower_than_both |
| canonical-fixed-single | single | select_after_update | client_process_cpu_ns | +1.937525 | +1.300171 | +0.629174 | higher_than_both |
| canonical-fixed-single | single | select_after_vacuum | wall_ns | -0.657638 | +9.684415 | -9.428918 | between_or_equal |
| canonical-fixed-single | single | select_after_vacuum | server_process_cpu_ns | +10.107343 | +12.382241 | -2.024251 | higher_than_both |
| canonical-fixed-single | single | select_after_vacuum | client_process_cpu_ns | +1.816894 | -0.891729 | +2.732995 | between_or_equal |
| canonical-fixed-single | single | truncate | wall_ns | +12.848438 | +0.887391 | +11.855839 | higher_than_both |
| canonical-fixed-single | single | truncate | server_process_cpu_ns | -10.160875 | -12.497825 | +2.670734 | lower_than_both |
| canonical-fixed-single | single | truncate | client_process_cpu_ns | -22.284825 | -14.870472 | -8.709497 | lower_than_both |
| canonical-fixed-single | single | update16 | wall_ns | +16.953052 | +1.748917 | +14.942798 | higher_than_both |
| canonical-fixed-single | single | update16 | server_process_cpu_ns | +11.921638 | +7.442945 | +4.168439 | higher_than_both |
| canonical-fixed-single | single | update16 | client_process_cpu_ns | +16.633432 | +10.039310 | +5.992514 | higher_than_both |
| canonical-fixed-single | single | vacuum | wall_ns | +15.421385 | -4.718282 | +21.136969 | between_or_equal |
| canonical-fixed-single | single | vacuum | server_process_cpu_ns | +16.396910 | -7.443311 | +25.757426 | between_or_equal |
| canonical-fixed-single | single | vacuum | client_process_cpu_ns | -7.761527 | +10.838704 | -16.781350 | between_or_equal |
| canonical-overflow | overflow | delete16 | wall_ns | -14.979460 | -9.491644 | -6.063324 | lower_than_both |
| canonical-overflow | overflow | delete16 | server_process_cpu_ns | -11.584820 | -5.330372 | -6.606604 | lower_than_both |
| canonical-overflow | overflow | delete16 | client_process_cpu_ns | +17.979154 | +2.501962 | +15.099410 | higher_than_both |
| canonical-overflow | overflow | insert1 | wall_ns | -0.060908 | +20.492409 | -17.057770 | between_or_equal |
| canonical-overflow | overflow | insert1 | server_process_cpu_ns | -12.932266 | -3.923138 | -9.377001 | lower_than_both |
| canonical-overflow | overflow | insert1 | client_process_cpu_ns | +8.265128 | -2.657299 | +11.220591 | between_or_equal |
| canonical-overflow | overflow | reinsert_population | wall_ns | +6.166014 | +13.296534 | -6.293679 | higher_than_both |
| canonical-overflow | overflow | reinsert_population | server_process_cpu_ns | +4.989931 | +4.498938 | +0.469854 | higher_than_both |
| canonical-overflow | overflow | reinsert_population | client_process_cpu_ns | -28.028902 | +2.403405 | -29.718061 | between_or_equal |
| canonical-overflow | overflow | select_after_update | wall_ns | -17.949985 | -7.173744 | -11.609044 | lower_than_both |
| canonical-overflow | overflow | select_after_update | server_process_cpu_ns | -18.781890 | +5.085403 | -22.712282 | between_or_equal |
| canonical-overflow | overflow | select_after_update | client_process_cpu_ns | +1.239559 | -2.147870 | +3.461784 | between_or_equal |
| canonical-overflow | overflow | select_after_vacuum | wall_ns | -6.500464 | +3.735990 | -9.867795 | between_or_equal |
| canonical-overflow | overflow | select_after_vacuum | server_process_cpu_ns | -6.022277 | +6.581711 | -11.825657 | between_or_equal |
| canonical-overflow | overflow | select_after_vacuum | client_process_cpu_ns | -11.024666 | -13.253297 | +2.569125 | lower_than_both |
| canonical-overflow | overflow | truncate | wall_ns | -3.168795 | +11.410054 | -13.085756 | between_or_equal |
| canonical-overflow | overflow | truncate | server_process_cpu_ns | -10.143686 | +17.515977 | -23.536938 | between_or_equal |
| canonical-overflow | overflow | truncate | client_process_cpu_ns | -5.466715 | +23.493899 | -23.451048 | between_or_equal |
| canonical-overflow | overflow | update16 | wall_ns | -17.612523 | +6.684273 | -22.774487 | between_or_equal |
| canonical-overflow | overflow | update16 | server_process_cpu_ns | +3.721171 | +1.210437 | +2.480707 | higher_than_both |
| canonical-overflow | overflow | update16 | client_process_cpu_ns | -10.761342 | -8.234041 | -2.754072 | lower_than_both |
| canonical-overflow | overflow | vacuum | wall_ns | -4.948603 | +23.157560 | -22.821306 | between_or_equal |
| canonical-overflow | overflow | vacuum | server_process_cpu_ns | -5.629035 | +24.519885 | -24.212133 | between_or_equal |
| canonical-overflow | overflow | vacuum | client_process_cpu_ns | -4.082706 | +16.197350 | -17.453114 | between_or_equal |
| direct-hit | fixed | direct_full_no_order | wall_ns | -26.265482 | -44.115420 | +31.940721 | lower_than_both |
| direct-hit | fixed | direct_full_no_order | server_process_cpu_ns | -20.927652 | -38.529679 | +28.635001 | lower_than_both |
| direct-hit | fixed | direct_full_no_order | client_process_cpu_ns | +2.769208 | +1.721080 | +1.030395 | higher_than_both |
| direct-hit | fixed | direct_id_only_no_order | wall_ns | +9.046273 | -8.316178 | +18.937311 | between_or_equal |
| direct-hit | fixed | direct_id_only_no_order | server_process_cpu_ns | +24.912159 | +25.654786 | -0.591006 | higher_than_both |
| direct-hit | fixed | direct_id_only_no_order | client_process_cpu_ns | -0.336777 | +1.048520 | -1.370923 | between_or_equal |
| direct-hit | overflow | direct_full_no_order | wall_ns | -24.155887 | -22.697936 | -1.886044 | lower_than_both |
| direct-hit | overflow | direct_full_no_order | server_process_cpu_ns | -22.265833 | -16.248444 | -7.184809 | lower_than_both |
| direct-hit | overflow | direct_full_no_order | client_process_cpu_ns | +3.231853 | +1.109534 | +2.099030 | higher_than_both |
| direct-hit | overflow | direct_id_only_no_order | wall_ns | +9.730551 | +47.326442 | -25.518767 | higher_than_both |
| direct-hit | overflow | direct_id_only_no_order | server_process_cpu_ns | +0.427871 | +35.736527 | -26.012642 | higher_than_both |
| direct-hit | overflow | direct_id_only_no_order | client_process_cpu_ns | -21.631355 | -16.734450 | -5.881069 | lower_than_both |
| direct-hit | single | direct_full_no_order | wall_ns | -20.746633 | -22.155315 | +1.809606 | lower_than_both |
| direct-hit | single | direct_full_no_order | server_process_cpu_ns | -21.108079 | -19.492814 | -2.006361 | lower_than_both |
| direct-hit | single | direct_full_no_order | client_process_cpu_ns | -12.294794 | -7.939119 | -4.731299 | lower_than_both |
| direct-hit | single | direct_id_only_no_order | wall_ns | +2.674046 | -2.130814 | +4.909472 | between_or_equal |
| direct-hit | single | direct_id_only_no_order | server_process_cpu_ns | -3.651045 | -3.873308 | +0.231219 | lower_than_both |
| direct-hit | single | direct_id_only_no_order | client_process_cpu_ns | -7.547732 | -1.929139 | -5.729116 | lower_than_both |

Wall/clientCPU include socket envelope framing and raw buffering through ReadyI; full decoding/oracles/raw file writes and proc snapshots are outside wall. Process snapshots and clocks are non-atomic. RSS/IO endpoints are not peaks. Direct id-only and full projection include common scan/TOAST costs; neither latency nor their difference proves an exclusive bitmap-copy cost. Every negative result and maximum remains in the summary.

原273项仍为22 complete/166 partial/70 unverified/15 deferred_by_user，`IDX-07`保持`unverified`且`evidence=[]`。完整PG参考58/58；三代产品均完成全58目标与4冷控制，目标各53/58，另4冷锚控制仍失败。原9项目录/普通错误帧差异完整保留、没有改为PASS或新增暂缓。仅新增1个正式CPP回归；whole58 known-gap协议草稿未运行、未登记。Goal active，默认程序未替换，无服务重启、分支切换或推送。

本批实际清单：源248/头177；测试2392，原2391逐字保持，新增1个CPP；顶层CPP1125/全部CPP1130（nested5）；E2E登记684 driver/664 standalone保持；Markdown754/保护725保持；旧29份gzip保持，新归档后总30。原280条历史原文保持，追加第281条partial。

完整原始分布、既有差异、许可和验收边界见[本批性能报告](performance-report-2026-10-30.md#ordinary-project-bitmap-20261106)与[可逆压缩证据](benchmarks/ordinary-project-bitmap-20261106.json.gz)。以下P29及更早内容为对应代别的历史记录。

<!-- CURRENT_MULTILOB_20261106 -->

## 多个大字段存储与真实命令结果（2026-11-06，P29-C2）

<a id="multilob-storage-20261106"></a>

原七列建表同时含两个 BYTEA 和两个 TEXT 时，类型逻辑上限之和会超过堆头的 uint32 字段。现在仅对 format 2 中这种确实含变长列且最小实际元组可装页的溢出定义，将堆头名义宽度记为 0；可表示的旧宽度、列上限、实际变长元组和 TOAST 格式保持。UNLOGGED 初始化分叉在原有维护入口校验自身宽度，不匹配时重建。执行结果另显式标记无 RETURNING 的计数命令，普通 WITH DML 也据实际执行结果发送命令帧。

原19项定义/排序规则诊断在C1及C2均完整通过：每代父进程2154项检查，两个工作进程合计604项（452+152）。原失败发生在旧七列建表前置阶段，不能把它写成19项全已执行。另C1/旧P28C4双代12工作进程共2662项检查、6组真实冷重启RID对照全过；C2正式物理生命周期用例为10工作进程2483项、父进程17809项，覆盖真实大值/NULL/RID、备份恢复、分区、UNLOGGED及维护路径。以上是不同固定夹具和进程角色，不能相加冒充全仓测试数。

固定SimpleQuery文本格式0的37+3条、七列全值/NULL/十六进制BYTEA/列描述七项/命令与Ready检查，PG18.6首次40/40，C1为38/40，C2为40/40；C2自包含正式协议用例另一次同40控制也通过。C1保留的两项是DELETE多发空RowDescription帧，行值/NULL/状态/命令原本正确。C2按真实DML命令角色修正该帧形状，并保留SELECT/RETURNING结果形状；没有靠搜索字符串中的RETURNING判断。PG与本地分别读取真实关系OID进行来源锚定，不要求两个数据库的原始OID或整段原始协议字节相同。

原constraint154协议回归在C2完整通过。独立结果形状mini21参考PG21/21，C1为13/21，C2为19/21，仍保留两个普通缺口：零列一行SELECT缺少一个零列DataRow；零列表RETURNING *的错误代码实际42P01而PG为42601。它们未被改为PASS。

两个产品专属普通WAL角色共实际12条动作：COMMIT确认后强杀与未提交UPDATE确认后强杀，各自退出为预期-9，随后正常Main首先打开同一目录完成恢复并正常退出0；没有由原生读取器抢先恢复，也没有PG崩溃参考。后续在4份正常关闭后的独立副本上运行真实公共HeapScanCursor，34行/238单元/44个NULL和全部真实RID身份检查通过，四观察者均正常0且PID/进程组消失。初始到冷开共同键1..7不变，未提交角色全部8个旧键恢复，提交角色7个未修改旧键保持，实际修改键3及新键使用真实唯一RID。原WAL回执不回写；独立sidecar补充可见性证据，绝不把LP_NORMAL或虚构SQL ctid当可见行。

正常 CMake C++17/O2 固定成本对照已完整执行：旧版—C2—同一旧版各三轮，固定表及单 BYTEA 表各256行，另有C2专属多LOB表128行。168句实际计时、546个响应、24个正常Main及42个只读副本观察者均完成；全值/NULL/来源/冷开与真实RID验证通过。结果不支持本批普遍提速：16个可比操作中，墙钟与服务端CPU各为1项低于两旧、12项高于两旧、3项介于两旧；固定表查询在原双表流程中服务端CPU中位数15.667→30.044→18.536ms，C2增加91.76%/62.08%。全部变慢项、原三样本、长尾及漂移保留。DELETE旧版还多一个空T帧，不能当完全相同协议工作量；新多LOB因旧版建表失败，仅描述新版成本，不计算提速比。

后续固定CPU8的单表、三代正常构建窄诊断另有12次目标观测与24个正常Main；C2查询墙钟中位数22.086ms，对两旧24.181/26.336ms，服务端CPU14.326ms，对两旧15.406/16.605ms；C2相对C1的CPU中位数仍增加4.40%。这个更窄条件未重现原流程的大幅差异，但不能冲销原负向结果、归因于CPU亲和性或证明稳定收益，回退原因仍未确认。四角色用户态CPU采样的512次完整查询均通过，显示共同热点在逐列NULL检查及文件身份核验；采样计时没有混入正常性能表。

### 功能、兼容范围与未通过项

原始七列定义未删改：

```sql
CREATE TABLE cv_sig(id INT PRIMARY KEY,n INT,tag TEXT,other_tag TEXT,bytes BYTEA,other_bytes BYTEA,"select" INT);
```

相关存储原生回归实际是旧P28C4与C1各9例、各8例正常通过；toast_test两代均在原第604行 !corruptScan.open() 断言中止(-6)，原stdout/stderr和后续未执行边界保留，不称问题已解决或正常退出。physical_restore_validation_test整例因用户既有同步故障矩阵边界保持NOTRUN，没有拆出易过子集，也不算通过。本批不是全量测试或全部TOAST/恢复故障能力验收。

这里的物理formatVersion=2不同于用户暂停的协议输出格式码2。新增协议只覆盖SimpleQuery文本格式0，BYTEA的00/FF以真实\x十六进制全值核对，不宣称新增二进制结果格式1/2、参数Portal或retained/provider兼容。双代往返只证明旧可表示固定/单LOB文件在新程序写入并关闭后仍能被旧程序读取；没有证明旧版可建立或读取新多LOB声明。

未将mini21中仍失败的两个零列用例删掉或注册成绿色子集；完整草稿和原始失败保留。只读可见行工具首次编译的Session命名空间错误、profile启动前的路径/序列化错误均保留原记录；修正的是离线夹具和接收器，不是数据库SQL预期，正式正常成本样本没有重采。

### 其他数据库设计调研

设计参考分别来自PG18.6逐属性真实Datum与TOAST处理、SQLite记录串行类型/溢出页按实际载荷长度存储、DuckDB按实际字符串GetSize及溢出块存储。它们支持把声明容量与物理载荷/分配提示分离，不构成三库存储格式或事务恢复等价证明。本地既有BYTEA逻辑容量2147483647字节(2GiB-1)与PG文档约1GB的varlena单值限制不同；本轮保留本地边界，未测试接近2GiB值，也不声称相同单值上限。

官方原始资料：[PostgreSQL18.6](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/heap/heaptoast.c)，[PostgreSQL18](https://www.postgresql.org/docs/18/storage-toast.html)，[SQLite](https://www.sqlite.org/fileformat2.html#record_format)，[DuckDB pinned](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/storage/compression/string_uncompressed.cpp)。

DuckDB按已固定源码commit取证，不宣称最新main；最新commit查询的403失败也保留。本批没有运行SQLite/DuckDB吞吐对比，也没有据资料研究宣布功能验收。

### 正常成本完整原样本

每个UPDATE16/DELETE16都是一条影响16行的SQL。墙钟边界为发出完整SimpleQuery至收到Ready；含收发、5字节帧头识别和原始块复制，完整字段解码/核对及证据文件写盘在计时后。服务器与客户端CPU、/proc观测并非原子同刻；自然的建表和全值核验发生在计时前，不能当冷查询。采样期间无编译、压缩或分析器。初次mixed流程没有记录请求线程频率，不能用leader最后CPU猜测根因。

#### P29 normal fixed-size operation costs

All values are the original three independent contexts. Percentages compare medians; no exclusions or causal/stable-speed claim. Each range UPDATE/DELETE is one SQL statement affecting16 rows. DELETE before includes an extra zero-column T frame; overflow is new-only. Native cloned visible-cursor work occurs after all Main timing in that context.

| Scope | Region | Metric | Program | raw3 ms | min ms | p50 ms | max ms |
|---|---|---|---|---|---:|---:|---:|
| fixed | insert1 | wall_ns | before-first | 14.911379, 15.368281, 15.897223 | 14.911379 | 15.368281 | 15.897223 |
| fixed | insert1 | wall_ns | candidate | 21.906650, 18.961670, 21.699761 | 18.961670 | 21.699761 | 21.906650 |
| fixed | insert1 | wall_ns | before-return | 20.685983, 19.731638, 15.970625 | 15.970625 | 19.731638 | 20.685983 |
| fixed | insert1 | server_process_cpu_ns | before-first | 6.430956, 6.551277, 8.410963 | 6.430956 | 6.551277 | 8.410963 |
| fixed | insert1 | server_process_cpu_ns | candidate | 6.235741, 9.332008, 9.017166 | 6.235741 | 9.017166 | 9.332008 |
| fixed | insert1 | server_process_cpu_ns | before-return | 7.740937, 10.709492, 6.781100 | 6.781100 | 7.740937 | 10.709492 |
| fixed | insert1 | client_process_cpu_ns | before-first | 0.194334, 0.202264, 0.219741 | 0.194334 | 0.202264 | 0.219741 |
| fixed | insert1 | client_process_cpu_ns | candidate | 0.193017, 0.210367, 0.202497 | 0.193017 | 0.202497 | 0.210367 |
| fixed | insert1 | client_process_cpu_ns | before-return | 0.199455, 0.211388, 0.206196 | 0.199455 | 0.206196 | 0.211388 |
| fixed | update16 | wall_ns | before-first | 57.340675, 70.666998, 63.783918 | 57.340675 | 63.783918 | 70.666998 |
| fixed | update16 | wall_ns | candidate | 85.292136, 125.426008, 181.841489 | 85.292136 | 125.426008 | 181.841489 |
| fixed | update16 | wall_ns | before-return | 125.706849, 68.262573, 72.532255 | 68.262573 | 72.532255 | 125.706849 |
| fixed | update16 | server_process_cpu_ns | before-first | 47.321324, 47.142728, 56.614810 | 47.142728 | 47.321324 | 56.614810 |
| fixed | update16 | server_process_cpu_ns | candidate | 67.944380, 72.350463, 64.129337 | 64.129337 | 67.944380 | 72.350463 |
| fixed | update16 | server_process_cpu_ns | before-return | 72.142558, 59.965554, 63.079728 | 59.965554 | 63.079728 | 72.142558 |
| fixed | update16 | client_process_cpu_ns | before-first | 0.189845, 0.251725, 0.208503 | 0.189845 | 0.208503 | 0.251725 |
| fixed | update16 | client_process_cpu_ns | candidate | 0.224314, 0.208751, 0.169523 | 0.169523 | 0.208751 | 0.224314 |
| fixed | update16 | client_process_cpu_ns | before-return | 0.210287, 0.220727, 0.206888 | 0.206888 | 0.210287 | 0.220727 |
| fixed | select_after_update | wall_ns | before-first | 22.930225, 24.698857, 25.015661 | 22.930225 | 24.698857 | 25.015661 |
| fixed | select_after_update | wall_ns | candidate | 30.165556, 41.230261, 102.765130 | 30.165556 | 41.230261 | 102.765130 |
| fixed | select_after_update | wall_ns | before-return | 37.410188, 27.868672, 26.601337 | 26.601337 | 27.868672 | 37.410188 |
| fixed | select_after_update | server_process_cpu_ns | before-first | 12.260123, 16.499056, 15.667344 | 12.260123 | 15.667344 | 16.499056 |
| fixed | select_after_update | server_process_cpu_ns | candidate | 20.793163, 30.043768, 31.134099 | 20.793163 | 30.043768 | 31.134099 |
| fixed | select_after_update | server_process_cpu_ns | before-return | 28.601690, 18.536101, 18.423045 | 18.423045 | 18.536101 | 28.601690 |
| fixed | select_after_update | client_process_cpu_ns | before-first | 4.764929, 4.892809, 6.886161 | 4.764929 | 4.892809 | 6.886161 |
| fixed | select_after_update | client_process_cpu_ns | candidate | 4.990799, 4.833728, 4.948835 | 4.833728 | 4.948835 | 4.990799 |
| fixed | select_after_update | client_process_cpu_ns | before-return | 3.756705, 4.989380, 4.959846 | 3.756705 | 4.959846 | 4.989380 |
| fixed | delete16 | wall_ns | before-first | 40.615603, 42.339548, 40.223311 | 40.223311 | 40.615603 | 42.339548 |
| fixed | delete16 | wall_ns | candidate | 50.358311, 60.365315, 58.569172 | 50.358311 | 58.569172 | 60.365315 |
| fixed | delete16 | wall_ns | before-return | 55.578539, 44.278606, 35.236604 | 35.236604 | 44.278606 | 55.578539 |
| fixed | delete16 | server_process_cpu_ns | before-first | 32.541211, 33.129494, 33.538235 | 32.541211 | 33.129494 | 33.538235 |
| fixed | delete16 | server_process_cpu_ns | candidate | 39.827968, 49.553592, 50.260100 | 39.827968 | 49.553592 | 50.260100 |
| fixed | delete16 | server_process_cpu_ns | before-return | 46.810142, 36.129187, 27.025847 | 27.025847 | 36.129187 | 46.810142 |
| fixed | delete16 | client_process_cpu_ns | before-first | 0.259191, 0.228084, 0.219557 | 0.219557 | 0.228084 | 0.259191 |
| fixed | delete16 | client_process_cpu_ns | candidate | 0.221130, 0.176036, 0.216144 | 0.176036 | 0.216144 | 0.221130 |
| fixed | delete16 | client_process_cpu_ns | before-return | 0.283338, 0.234247, 0.182712 | 0.182712 | 0.234247 | 0.283338 |
| fixed | vacuum | wall_ns | before-first | 1.313317, 1.533314, 1.286283 | 1.286283 | 1.313317 | 1.533314 |
| fixed | vacuum | wall_ns | candidate | 1.485597, 2.906156, 2.084523 | 1.485597 | 2.084523 | 2.906156 |
| fixed | vacuum | wall_ns | before-return | 2.053779, 1.482527, 1.221131 | 1.221131 | 1.482527 | 2.053779 |
| fixed | vacuum | server_process_cpu_ns | before-first | 1.222884, 1.434989, 1.192736 | 1.192736 | 1.222884 | 1.434989 |
| fixed | vacuum | server_process_cpu_ns | candidate | 1.384110, 2.823291, 1.957017 | 1.384110 | 1.957017 | 2.823291 |
| fixed | vacuum | server_process_cpu_ns | before-return | 1.922630, 1.380680, 1.132533 | 1.132533 | 1.380680 | 1.922630 |
| fixed | vacuum | client_process_cpu_ns | before-first | 0.185151, 0.204153, 0.183567 | 0.183567 | 0.185151 | 0.204153 |
| fixed | vacuum | client_process_cpu_ns | candidate | 0.197297, 0.150487, 0.198477 | 0.150487 | 0.197297 | 0.198477 |
| fixed | vacuum | client_process_cpu_ns | before-return | 0.209027, 0.199225, 0.123133 | 0.123133 | 0.199225 | 0.209027 |
| fixed | select_after_vacuum | wall_ns | before-first | 21.851845, 24.790847, 23.518761 | 21.851845 | 23.518761 | 24.790847 |
| fixed | select_after_vacuum | wall_ns | candidate | 30.109219, 32.308897, 28.843535 | 28.843535 | 30.109219 | 32.308897 |
| fixed | select_after_vacuum | wall_ns | before-return | 32.465611, 30.539337, 20.934131 | 20.934131 | 30.539337 | 32.465611 |
| fixed | select_after_vacuum | server_process_cpu_ns | before-first | 15.844468, 16.663150, 16.824282 | 15.844468 | 16.663150 | 16.824282 |
| fixed | select_after_vacuum | server_process_cpu_ns | candidate | 21.869752, 22.955342, 21.791410 | 21.791410 | 21.869752 | 22.955342 |
| fixed | select_after_vacuum | server_process_cpu_ns | before-return | 24.050478, 22.600625, 13.165023 | 13.165023 | 22.600625 | 24.050478 |
| fixed | select_after_vacuum | client_process_cpu_ns | before-first | 3.578215, 4.600109, 4.696975 | 3.578215 | 4.600109 | 4.696975 |
| fixed | select_after_vacuum | client_process_cpu_ns | candidate | 4.606465, 4.611646, 4.586422 | 4.586422 | 4.606465 | 4.611646 |
| fixed | select_after_vacuum | client_process_cpu_ns | before-return | 4.626946, 4.832059, 4.574462 | 4.574462 | 4.626946 | 4.832059 |
| fixed | truncate | wall_ns | before-first | 254.543869, 249.177426, 300.796862 | 249.177426 | 254.543869 | 300.796862 |
| fixed | truncate | wall_ns | candidate | 346.433765, 388.733075, 260.849299 | 260.849299 | 346.433765 | 388.733075 |
| fixed | truncate | wall_ns | before-return | 232.872547, 269.093861, 240.315008 | 232.872547 | 240.315008 | 269.093861 |
| fixed | truncate | server_process_cpu_ns | before-first | 79.615640, 73.255798, 86.058318 | 73.255798 | 79.615640 | 86.058318 |
| fixed | truncate | server_process_cpu_ns | candidate | 115.818387, 104.234914, 80.993695 | 80.993695 | 104.234914 | 115.818387 |
| fixed | truncate | server_process_cpu_ns | before-return | 94.112141, 83.134156, 73.633328 | 73.633328 | 83.134156 | 94.112141 |
| fixed | truncate | client_process_cpu_ns | before-first | 0.249117, 0.179271, 0.230766 | 0.179271 | 0.230766 | 0.249117 |
| fixed | truncate | client_process_cpu_ns | candidate | 0.268835, 0.235243, 0.224522 | 0.224522 | 0.235243 | 0.268835 |
| fixed | truncate | client_process_cpu_ns | before-return | 0.235026, 0.180520, 0.242761 | 0.180520 | 0.235026 | 0.242761 |
| fixed | reinsert_population | wall_ns | before-first | 584.860154, 566.686053, 625.286000 | 566.686053 | 584.860154 | 625.286000 |
| fixed | reinsert_population | wall_ns | candidate | 662.246121, 654.359402, 614.305245 | 614.305245 | 654.359402 | 662.246121 |
| fixed | reinsert_population | wall_ns | before-return | 637.614042, 633.124814, 580.543462 | 580.543462 | 633.124814 | 637.614042 |
| fixed | reinsert_population | server_process_cpu_ns | before-first | 556.108540, 533.518790, 602.113984 | 533.518790 | 556.108540 | 602.113984 |
| fixed | reinsert_population | server_process_cpu_ns | candidate | 585.324844, 569.109756, 559.099173 | 559.099173 | 569.109756 | 585.324844 |
| fixed | reinsert_population | server_process_cpu_ns | before-return | 583.058661, 604.982497, 548.389193 | 548.389193 | 583.058661 | 604.982497 |
| fixed | reinsert_population | client_process_cpu_ns | before-first | 0.213840, 0.227761, 0.251597 | 0.213840 | 0.227761 | 0.251597 |
| fixed | reinsert_population | client_process_cpu_ns | candidate | 0.229869, 0.253189, 0.164165 | 0.164165 | 0.229869 | 0.253189 |
| fixed | reinsert_population | client_process_cpu_ns | before-return | 0.224800, 0.236640, 0.244997 | 0.224800 | 0.236640 | 0.244997 |
| single | insert1 | wall_ns | before-first | 16.536043, 15.236743, 16.567373 | 15.236743 | 16.536043 | 16.567373 |
| single | insert1 | wall_ns | candidate | 24.026323, 32.520507, 20.684454 | 20.684454 | 24.026323 | 32.520507 |
| single | insert1 | wall_ns | before-return | 21.736532, 23.046251, 15.531870 | 15.531870 | 21.736532 | 23.046251 |
| single | insert1 | server_process_cpu_ns | before-first | 7.230427, 8.741299, 8.882624 | 7.230427 | 8.741299 | 8.882624 |
| single | insert1 | server_process_cpu_ns | candidate | 9.657295, 13.887095, 10.081112 | 9.657295 | 10.081112 | 13.887095 |
| single | insert1 | server_process_cpu_ns | before-return | 10.513493, 8.339402, 8.517592 | 8.339402 | 8.517592 | 10.513493 |
| single | insert1 | client_process_cpu_ns | before-first | 0.200283, 0.215494, 0.219960 | 0.200283 | 0.215494 | 0.219960 |
| single | insert1 | client_process_cpu_ns | candidate | 0.172658, 0.206250, 0.140514 | 0.140514 | 0.172658 | 0.206250 |
| single | insert1 | client_process_cpu_ns | before-return | 0.161035, 0.173124, 0.207773 | 0.161035 | 0.173124 | 0.207773 |
| single | update16 | wall_ns | before-first | 122.525452, 132.940008, 148.979316 | 122.525452 | 132.940008 | 148.979316 |
| single | update16 | wall_ns | candidate | 148.891836, 170.538208, 158.909128 | 148.891836 | 158.909128 | 170.538208 |
| single | update16 | wall_ns | before-return | 165.202636, 230.891972, 128.106894 | 128.106894 | 165.202636 | 230.891972 |
| single | update16 | server_process_cpu_ns | before-first | 102.549495, 116.263182, 118.049581 | 102.549495 | 116.263182 | 118.049581 |
| single | update16 | server_process_cpu_ns | candidate | 118.211122, 135.585931, 117.989084 | 117.989084 | 118.211122 | 135.585931 |
| single | update16 | server_process_cpu_ns | before-return | 102.980948, 138.175703, 114.214891 | 102.980948 | 114.214891 | 138.175703 |
| single | update16 | client_process_cpu_ns | before-first | 0.242301, 0.218841, 0.230824 | 0.218841 | 0.230824 | 0.242301 |
| single | update16 | client_process_cpu_ns | candidate | 0.212045, 0.232125, 0.221180 | 0.212045 | 0.221180 | 0.232125 |
| single | update16 | client_process_cpu_ns | before-return | 0.185907, 0.163133, 0.222920 | 0.163133 | 0.185907 | 0.222920 |
| single | select_after_update | wall_ns | before-first | 35.819874, 43.986405, 47.569176 | 35.819874 | 43.986405 | 47.569176 |
| single | select_after_update | wall_ns | candidate | 50.104482, 57.507591, 51.837468 | 50.104482 | 51.837468 | 57.507591 |
| single | select_after_update | wall_ns | before-return | 51.644243, 47.943115, 42.183275 | 42.183275 | 47.943115 | 51.644243 |
| single | select_after_update | server_process_cpu_ns | before-first | 29.249991, 35.767469, 36.568199 | 29.249991 | 35.767469 | 36.568199 |
| single | select_after_update | server_process_cpu_ns | candidate | 40.949064, 45.945792, 42.392317 | 40.949064 | 42.392317 | 45.945792 |
| single | select_after_update | server_process_cpu_ns | before-return | 41.943863, 39.947655, 33.992000 | 33.992000 | 39.947655 | 41.943863 |
| single | select_after_update | client_process_cpu_ns | before-first | 2.978107, 6.166451, 8.133779 | 2.978107 | 6.166451 | 8.133779 |
| single | select_after_update | client_process_cpu_ns | candidate | 5.941226, 5.898526, 6.163726 | 5.898526 | 5.941226 | 6.163726 |
| single | select_after_update | client_process_cpu_ns | before-return | 5.886766, 6.161067, 6.226311 | 5.886766 | 6.161067 | 6.226311 |
| single | delete16 | wall_ns | before-first | 48.681016, 133.542830, 55.529704 | 48.681016 | 55.529704 | 133.542830 |
| single | delete16 | wall_ns | candidate | 57.750556, 81.321329, 58.665659 | 57.750556 | 58.665659 | 81.321329 |
| single | delete16 | wall_ns | before-return | 61.026664, 60.460375, 51.959083 | 51.959083 | 60.460375 | 61.026664 |
| single | delete16 | server_process_cpu_ns | before-first | 39.758936, 74.249297, 48.308573 | 39.758936 | 48.308573 | 74.249297 |
| single | delete16 | server_process_cpu_ns | candidate | 47.653730, 62.614411, 49.126889 | 47.653730 | 49.126889 | 62.614411 |
| single | delete16 | server_process_cpu_ns | before-return | 52.087155, 50.668426, 45.109475 | 45.109475 | 50.668426 | 52.087155 |
| single | delete16 | client_process_cpu_ns | before-first | 0.181027, 0.246948, 0.234007 | 0.181027 | 0.234007 | 0.246948 |
| single | delete16 | client_process_cpu_ns | candidate | 0.173525, 0.230875, 0.225670 | 0.173525 | 0.225670 | 0.230875 |
| single | delete16 | client_process_cpu_ns | before-return | 0.233811, 0.272837, 0.194173 | 0.194173 | 0.233811 | 0.272837 |
| single | vacuum | wall_ns | before-first | 5.499223, 8.348321, 6.494562 | 5.499223 | 6.494562 | 8.348321 |
| single | vacuum | wall_ns | candidate | 5.898434, 7.166589, 6.368668 | 5.898434 | 6.368668 | 7.166589 |
| single | vacuum | wall_ns | before-return | 6.824096, 6.398009, 5.955423 | 5.955423 | 6.398009 | 6.824096 |
| single | vacuum | server_process_cpu_ns | before-first | 5.196209, 8.077382, 6.205124 | 5.196209 | 6.205124 | 8.077382 |
| single | vacuum | server_process_cpu_ns | candidate | 5.688353, 6.894376, 6.093428 | 5.688353 | 6.093428 | 6.894376 |
| single | vacuum | server_process_cpu_ns | before-return | 6.582422, 6.122304, 5.695983 | 5.695983 | 6.122304 | 6.582422 |
| single | vacuum | client_process_cpu_ns | before-first | 0.203910, 0.221176, 0.211244 | 0.203910 | 0.211244 | 0.221176 |
| single | vacuum | client_process_cpu_ns | candidate | 0.138863, 0.197230, 0.208791 | 0.138863 | 0.197230 | 0.208791 |
| single | vacuum | client_process_cpu_ns | before-return | 0.129105, 0.212010, 0.201947 | 0.129105 | 0.201947 | 0.212010 |
| single | select_after_vacuum | wall_ns | before-first | 37.088214, 51.843135, 44.571003 | 37.088214 | 44.571003 | 51.843135 |
| single | select_after_vacuum | wall_ns | candidate | 44.281384, 47.516380, 47.290786 | 44.281384 | 47.290786 | 47.516380 |
| single | select_after_vacuum | wall_ns | before-return | 50.515455, 46.874847, 43.630850 | 43.630850 | 46.874847 | 50.515455 |
| single | select_after_vacuum | server_process_cpu_ns | before-first | 31.553363, 45.510612, 36.579174 | 31.553363 | 36.579174 | 45.510612 |
| single | select_after_vacuum | server_process_cpu_ns | candidate | 36.070559, 40.019267, 37.153596 | 36.070559 | 37.153596 | 40.019267 |
| single | select_after_vacuum | server_process_cpu_ns | before-return | 35.290584, 39.378133, 36.781228 | 35.290584 | 36.781228 | 39.378133 |
| single | select_after_vacuum | client_process_cpu_ns | before-first | 3.168377, 5.695870, 5.672364 | 3.168377 | 5.672364 | 5.695870 |
| single | select_after_vacuum | client_process_cpu_ns | candidate | 5.123526, 5.403636, 5.774138 | 5.123526 | 5.403636 | 5.774138 |
| single | select_after_vacuum | client_process_cpu_ns | before-return | 8.155490, 5.659887, 5.558702 | 5.558702 | 5.659887 | 8.155490 |
| single | truncate | wall_ns | before-first | 390.896665, 423.934982, 421.084433 | 390.896665 | 421.084433 | 423.934982 |
| single | truncate | wall_ns | candidate | 451.273962, 489.557581, 435.221318 | 435.221318 | 451.273962 | 489.557581 |
| single | truncate | wall_ns | before-return | 390.833655, 656.023382, 430.680378 | 390.833655 | 430.680378 | 656.023382 |
| single | truncate | server_process_cpu_ns | before-first | 102.439798, 118.483423, 104.773000 | 102.439798 | 104.773000 | 118.483423 |
| single | truncate | server_process_cpu_ns | candidate | 118.402489, 125.818133, 131.076713 | 118.402489 | 125.818133 | 131.076713 |
| single | truncate | server_process_cpu_ns | before-return | 115.518101, 131.995939, 122.516476 | 115.518101 | 122.516476 | 131.995939 |
| single | truncate | client_process_cpu_ns | before-first | 0.212089, 0.233130, 0.336181 | 0.212089 | 0.233130 | 0.336181 |
| single | truncate | client_process_cpu_ns | candidate | 0.270948, 0.170784, 0.264046 | 0.170784 | 0.264046 | 0.270948 |
| single | truncate | client_process_cpu_ns | before-return | 0.299190, 0.235682, 0.262587 | 0.235682 | 0.262587 | 0.299190 |
| single | reinsert_population | wall_ns | before-first | 1036.686179, 1076.466840, 1075.774511 | 1036.686179 | 1075.774511 | 1076.466840 |
| single | reinsert_population | wall_ns | candidate | 1142.383063, 1141.026326, 1943.413029 | 1141.026326 | 1142.383063 | 1943.413029 |
| single | reinsert_population | wall_ns | before-return | 992.071766, 1039.853398, 1128.246772 | 992.071766 | 1039.853398 | 1128.246772 |
| single | reinsert_population | server_process_cpu_ns | before-first | 1008.470510, 1010.415164, 1008.222837 | 1008.222837 | 1008.470510 | 1010.415164 |
| single | reinsert_population | server_process_cpu_ns | candidate | 1035.106492, 1033.614696, 1113.000870 | 1033.614696 | 1035.106492 | 1113.000870 |
| single | reinsert_population | server_process_cpu_ns | before-return | 936.613806, 996.994019, 1032.138425 | 936.613806 | 996.994019 | 1032.138425 |
| single | reinsert_population | client_process_cpu_ns | before-first | 0.699655, 0.564333, 0.822251 | 0.564333 | 0.699655 | 0.822251 |
| single | reinsert_population | client_process_cpu_ns | candidate | 1.186009, 0.582085, 0.615537 | 0.582085 | 0.615537 | 1.186009 |
| single | reinsert_population | client_process_cpu_ns | before-return | 0.601117, 0.774925, 0.567147 | 0.567147 | 0.601117 | 0.774925 |
| overflow | insert1 | wall_ns | candidate-only | 30.618840, 912.663801, 16.708637 | 16.708637 | 30.618840 | 912.663801 |
| overflow | insert1 | server_process_cpu_ns | candidate-only | 17.686450, 19.315776, 9.294738 | 9.294738 | 17.686450 | 19.315776 |
| overflow | insert1 | client_process_cpu_ns | candidate-only | 0.218150, 0.183761, 0.247067 | 0.183761 | 0.218150 | 0.247067 |
| overflow | update16 | wall_ns | candidate-only | 170.381926, 184.714439, 140.756455 | 140.756455 | 170.381926 | 184.714439 |
| overflow | update16 | server_process_cpu_ns | candidate-only | 140.627918, 110.583841, 122.122418 | 110.583841 | 122.122418 | 140.627918 |
| overflow | update16 | client_process_cpu_ns | candidate-only | 0.248601, 0.157679, 0.272071 | 0.157679 | 0.248601 | 0.272071 |
| overflow | select_after_update | wall_ns | candidate-only | 55.748627, 45.105495, 46.543759 | 45.105495 | 46.543759 | 55.748627 |
| overflow | select_after_update | server_process_cpu_ns | candidate-only | 48.258159, 36.708798, 41.384431 | 36.708798 | 41.384431 | 48.258159 |
| overflow | select_after_update | client_process_cpu_ns | candidate-only | 4.055785, 3.970971, 4.139664 | 3.970971 | 4.055785 | 4.139664 |
| overflow | delete16 | wall_ns | candidate-only | 59.395416, 57.207372, 52.790412 | 52.790412 | 57.207372 | 59.395416 |
| overflow | delete16 | server_process_cpu_ns | candidate-only | 50.265878, 47.751550, 45.066072 | 45.066072 | 47.751550 | 50.265878 |
| overflow | delete16 | client_process_cpu_ns | candidate-only | 0.237147, 0.270983, 0.286086 | 0.237147 | 0.270983 | 0.286086 |
| overflow | vacuum | wall_ns | candidate-only | 7.453795, 7.745876, 7.812579 | 7.453795 | 7.745876 | 7.812579 |
| overflow | vacuum | server_process_cpu_ns | candidate-only | 7.186637, 7.437928, 7.512695 | 7.186637 | 7.437928 | 7.512695 |
| overflow | vacuum | client_process_cpu_ns | candidate-only | 0.200553, 0.215007, 0.235426 | 0.200553 | 0.215007 | 0.235426 |
| overflow | select_after_vacuum | wall_ns | candidate-only | 44.127592, 44.668352, 44.166935 | 44.127592 | 44.166935 | 44.668352 |
| overflow | select_after_vacuum | server_process_cpu_ns | candidate-only | 37.715462, 38.094461, 38.569260 | 37.715462 | 38.094461 | 38.569260 |
| overflow | select_after_vacuum | client_process_cpu_ns | candidate-only | 3.561141, 3.645407, 3.805513 | 3.561141 | 3.645407 | 3.805513 |
| overflow | truncate | wall_ns | candidate-only | 144.252421, 146.657150, 128.593187 | 128.593187 | 144.252421 | 146.657150 |
| overflow | truncate | server_process_cpu_ns | candidate-only | 69.701345, 74.183641, 57.465814 | 57.465814 | 69.701345 | 74.183641 |
| overflow | truncate | client_process_cpu_ns | candidate-only | 0.229252, 0.232780, 0.259222 | 0.229252 | 0.232780 | 0.259222 |
| overflow | reinsert_population | wall_ns | candidate-only | 1096.392369, 1137.886347, 1049.277980 | 1049.277980 | 1096.392369 | 1137.886347 |
| overflow | reinsert_population | server_process_cpu_ns | candidate-only | 1043.161237, 1049.400023, 960.600984 | 960.600984 | 1043.161237 | 1049.400023 |
| overflow | reinsert_population | client_process_cpu_ns | candidate-only | 1.673976, 0.734158, 0.800551 | 0.734158 | 0.800551 | 1.673976 |

| Scope | Region | Metric | C2 vs first % | C2 vs return % | Old return drift % | Class |
|---|---|---|---:|---:|---:|---|
| fixed | insert1 | wall_ns | +41.198362 | +9.974453 | +28.391965 | higher_than_both |
| fixed | insert1 | server_process_cpu_ns | +37.639822 | +16.486751 | +18.159208 | higher_than_both |
| fixed | insert1 | client_process_cpu_ns | +0.115196 | -1.793924 | +1.943994 | between_or_equal |
| fixed | update16 | wall_ns | +96.642056 | +72.924457 | +13.715584 | higher_than_both |
| fixed | update16 | server_process_cpu_ns | +43.580894 | +7.711910 | +33.300852 | higher_than_both |
| fixed | update16 | client_process_cpu_ns | +0.118943 | -0.730430 | +0.855623 | between_or_equal |
| fixed | select_after_update | wall_ns | +66.931858 | +47.944836 | +12.833853 | higher_than_both |
| fixed | select_after_update | server_process_cpu_ns | +91.760441 | +62.082457 | +18.310423 | higher_than_both |
| fixed | select_after_update | client_process_cpu_ns | +1.145068 | -0.222003 | +1.370113 | between_or_equal |
| fixed | delete16 | wall_ns | +44.203625 | +32.274200 | +9.018709 | higher_than_both |
| fixed | delete16 | server_process_cpu_ns | +49.575457 | +37.156676 | +9.054449 | higher_than_both |
| fixed | delete16 | client_process_cpu_ns | -5.234913 | -7.728167 | +2.702075 | lower_than_both |
| fixed | vacuum | wall_ns | +58.721999 | +40.606073 | +12.884170 | higher_than_both |
| fixed | vacuum | server_process_cpu_ns | +60.032922 | +41.742982 | +12.903595 | higher_than_both |
| fixed | vacuum | client_process_cpu_ns | +6.560051 | -0.967750 | +7.601363 | between_or_equal |
| fixed | select_after_vacuum | wall_ns | +28.022131 | -1.408406 | +29.850960 | between_or_equal |
| fixed | select_after_vacuum | server_process_cpu_ns | +31.246205 | -3.233862 | +35.632368 | between_or_equal |
| fixed | select_after_vacuum | client_process_cpu_ns | +0.138171 | -0.442646 | +0.583399 | between_or_equal |
| fixed | truncate | wall_ns | +36.099827 | +44.158190 | -5.589944 | higher_than_both |
| fixed | truncate | server_process_cpu_ns | +30.922660 | +25.381575 | +4.419378 | higher_than_both |
| fixed | truncate | client_process_cpu_ns | +1.940060 | +0.092330 | +1.846026 | higher_than_both |
| fixed | reinsert_population | wall_ns | +11.883054 | +3.353934 | +8.252342 | higher_than_both |
| fixed | reinsert_population | server_process_cpu_ns | +2.337892 | -2.392367 | +4.846198 | between_or_equal |
| fixed | reinsert_population | client_process_cpu_ns | +0.925532 | -2.861308 | +3.898385 | between_or_equal |
| single | insert1 | wall_ns | +45.296689 | +10.534298 | +31.449416 | higher_than_both |
| single | insert1 | server_process_cpu_ns | +15.327390 | +18.356362 | -2.559196 | higher_than_both |
| single | insert1 | client_process_cpu_ns | -19.878048 | -0.269171 | -19.661800 | lower_than_both |
| single | update16 | wall_ns | +19.534466 | -3.809569 | +24.268562 | between_or_equal |
| single | update16 | server_process_cpu_ns | +1.675457 | +3.498870 | -1.761771 | higher_than_both |
| single | update16 | client_process_cpu_ns | -4.178075 | +18.973465 | -19.459415 | between_or_equal |
| single | select_after_update | wall_ns | +17.848840 | +8.122862 | +8.995302 | higher_than_both |
| single | select_after_update | server_process_cpu_ns | +18.521993 | +6.119663 | +11.687117 | higher_than_both |
| single | select_after_update | client_process_cpu_ns | -3.652425 | -3.568229 | -0.087311 | lower_than_both |
| single | delete16 | wall_ns | +5.647347 | -2.968417 | +8.879340 | between_or_equal |
| single | delete16 | server_process_cpu_ns | +1.693935 | -3.042402 | +4.884957 | between_or_equal |
| single | delete16 | client_process_cpu_ns | -3.562714 | -3.481872 | -0.083758 | lower_than_both |
| single | vacuum | wall_ns | -1.938453 | -0.458596 | -1.486675 | lower_than_both |
| single | vacuum | server_process_cpu_ns | -1.800061 | -0.471653 | -1.334703 | lower_than_both |
| single | vacuum | client_process_cpu_ns | -6.634035 | -2.335761 | -4.401072 | lower_than_both |
| single | select_after_vacuum | wall_ns | +6.102136 | +0.887339 | +5.168930 | higher_than_both |
| single | select_after_vacuum | server_process_cpu_ns | +1.570353 | +1.012386 | +0.552374 | higher_than_both |
| single | select_after_vacuum | client_process_cpu_ns | -4.737496 | -4.527493 | -0.219961 | lower_than_both |
| single | truncate | wall_ns | +7.169472 | +4.781640 | +2.278865 | higher_than_both |
| single | truncate | server_process_cpu_ns | +20.086409 | +2.694868 | +16.935161 | higher_than_both |
| single | truncate | client_process_cpu_ns | +13.261271 | +0.555625 | +12.635439 | higher_than_both |
| single | reinsert_population | wall_ns | +6.191683 | +9.860012 | -3.339093 | higher_than_both |
| single | reinsert_population | server_process_cpu_ns | +2.641226 | +3.822738 | -1.138010 | higher_than_both |
| single | reinsert_population | client_process_cpu_ns | -12.022783 | +2.398867 | -14.083798 | between_or_equal |

All negative results and maxima remain in the machine summary; raw process/client brackets and /proc CPU/RSS/IO snapshots are non-atomic and are not memory peaks. Matched data sizes are N256 only; overflow N128 has no usable old CREATE denominator.


### CPU8正常构建窄诊断（独立条件）

仅使用固定表257行的原SELECT-after-update，加入C1中间代；每代三个独立新目录，全部线程限制CPU8，正常Main不加载分析器。保留原建表/核验/手动CHECKPOINT/INSERT1/UPDATE16顺序，但没有初次mixed流程的第二张单BYTEA表，因此这不是原双表基准重跑或替换。216个响应、18,516行/55,500单元/4,824个NULL及1,536个关闭图像文件核对通过。24个频率端点实测1,200,239至2,913,730kHz；没有修改频率策略或按频率筛样本。这组只做协议/数据与调度诊断，没有运行native观察者或声称新增RID验收。

| 指标 | 角色 | 原3样本 ms | 最小 ms | 中位 ms | 最大 ms |
|---|---|---|---:|---:|---:|
| wall_ns | before-first | 24.791555, 21.643514, 24.180781 | 21.643514 | 24.180781 | 24.791555 |
| wall_ns | C1 | 22.416698, 19.825118, 24.556637 | 19.825118 | 22.416698 | 24.556637 |
| wall_ns | C2 | 26.234830, 21.506968, 22.085525 | 21.506968 | 22.085525 | 26.234830 |
| wall_ns | before-return | 26.801870, 26.335544, 23.115601 | 23.115601 | 26.335544 | 26.801870 |
| server_process_cpu_ns | before-first | 15.744346, 14.112677, 15.405517 | 14.112677 | 15.405517 | 15.744346 |
| server_process_cpu_ns | C1 | 14.591040, 13.722106, 12.931775 | 12.931775 | 13.722106 | 14.591040 |
| server_process_cpu_ns | C2 | 16.971065, 12.549032, 14.326150 | 12.549032 | 14.326150 | 16.971065 |
| server_process_cpu_ns | before-return | 16.605246, 17.220134, 15.722027 | 15.722027 | 16.605246 | 17.220134 |
| client_process_cpu_ns | before-first | 4.644053, 4.513725, 4.747710 | 4.513725 | 4.644053 | 4.747710 |
| client_process_cpu_ns | C1 | 4.945619, 2.348159, 7.322048 | 2.348159 | 4.945619 | 7.322048 |
| client_process_cpu_ns | C2 | 4.767529, 4.701989, 5.519954 | 4.701989 | 4.767529 | 5.519954 |
| client_process_cpu_ns | before-return | 7.402150, 4.806372, 4.820543 | 4.806372 | 4.820543 | 7.402150 |

### 用户态调用栈定位（不作为正常计时）

硬件perf因当前权限不可用，未修改内核或系统安装。临时本地libprofiler仅用于函数采样，没有替换分配器；四个实际正常进程各128次完整查询，采样周期1000微秒，样本总数2968/2747/2922/2760。原始profile、映射、线程记录和函数/地址表全部保留；剥离符号的系统库可能只有最近符号名，不能据其别名推断具体内存函数。程序本身的NULL检查→getPageAllocator→文件身份检查是共同热点，不是已证实的C2回退原因。后续普通顺序物理行bitmap复用仍只是审查建议，没有实现或收益验收。[工具官方说明](https://gperftools.github.io/gperftools/cpuprofile.html)。

采样四角色总计572个完整响应、133,648行/400,928单元/36,920个NULL；其中512条为目标查询。独立直接解析原始profile并按实际映射及ELF有大小符号区间核验程序PC，保留地址表及累计表，不从系统库最近别名推断原因。归档清单明确列出4个原始profile和128个无扩展名线程文件，避免仅按后缀归档遗漏。

最终验证对象是候选C2（SHA `b53f853b356e9c8cf1e8fedf9832201d9bf705cf81d817b35314b4660e334594`），默认安装程序未替换，未推送或切换分支。原53.45MiB性能JSON当前保留为5.70MiB gzip，未压缩基准文件已被Git忽略；本批完整证据同样采用可逐字节还原的gzip归档。只对核验可恢复的生成副本安排清理，Git历史不重写。性能目标继续，不能把功能修复称为全库优化完成。

实际清单：源248/头177；测试2391，原2388逐字保持，新增2个CPP和1个协议测试；顶层CPP1124/全部CPP1129（nested5），登记684 driver/664 standalone E2E；Markdown754/保护725保持；旧28份gzip不变，总计29。原279条历史原文保持，新增第280条partial。

这是有限的 partial 进展，Goal active。原 273 项状态保持 22 complete/166 partial/70 unverified/15 deferred_by_user；`IDX-07` 仍为 `unverified`、`evidence=[]`。零列行集、零列表和原 TOAST 损坏夹具的普通缺口仍保留，用户暂缓边界保持。本批不证明完整 PostgreSQL 数据类型上限、文件格式、WAL 或协议等价。

完整定义、首轮失败与测试边界见[本批报告](performance-report-2026-10-30.md#multilob-storage-20261106)和[可逆压缩证据](benchmarks/multilob-storage-20261106.json.gz)。下方 P28 及更早段落是对应代别的历史记录。

<!-- CURRENT_CONSTRAINT_VALIDATION_20261106 -->

## CHECK/FK NOT VALID 与真实历史验证（2026-11-06，P28-C4）

<a id="constraint-validation-20261106"></a>

`ADD CONSTRAINT ... NOT VALID` 仍在真实元数据锁下绑定 CHECK/FK 定义，只跳历史行验证；后续写入仍由原真实约束执行链检查。`VALIDATE CONSTRAINT` 对未验证的 CHECK/FK 扫描真实可见历史行，仅在成功后发布 validated 标记；失败保留定义和未验证状态，已验证约束不重复求值。真实 SQL NULL 与非 NULL 空字符串分别处理。CHECK 的 volatile 函数不因 volatility 标记被一律禁止；定义绑定不执行用户函数，实际验证仍按到达的行求值。

正常候选 C4 为 CMake C++17/O2，程序 SHA256 `5cd5fd9e15f72257d13f2622e180d643e5cc93fffe8a128cbf8e820f64caf539`；before 为 P27-C2 `dd5d9b85…`。原生父测试 4,896 checks/0 failures（含文件快照断言），其中真实 lifecycle worker 779/cold worker 158；两 worker 正常退出，捕获两份当时文件映像，不能将这些层级的计数相加。真实目标 PageAllocator fetch 38 个观测中 18 个为零、20 个为非零 fetch，另有真实读取正对照证明 hook 确实生效；这只说明历史验证访问路径，不代表整句无 I/O。原协议 142 主流程+12 冷控制共154条全部匹配 SQLSTATE、完整值/NULL、真实本地 relation OID 的 T7、tag 和 Ready；58份参数快照、5项保留标志检查通过。修改前同源用例实际83条（56匹配/27差异）及71条 NOTRUN；C3全部154条中有4处不匹配，原失败保留，C4修复后原断言未放宽。独立 append10 全10条通过：已经 validated 的带 nextval CHECK 不再求值，PK/UNIQUE VALIDATE为42809、缺失名42704。新增真实 PostgreSQL18.6 mini20 全20条符合首预测；产品对应19条+6条冷控制全25条、13项真实 `.params` 检查通过，ASCII collation 只比较大小写语义角色，PG使用真实 ICU、本地使用 nocase，不声称 provider/OID/Unicode 等价。mini19 v1接收器在启动前缺少 Path import：0SQL/0owner，原首错误保留后仅修接收器，不称产品重试或产品失败。原159参考的3个预期行快照错误及 reserved RESET 首预测错误都保留，后续 oracle 仅从已记录 PG 离线更正。完整原始帧不是逐字节 PG 等价：mini19主流程12组 raw 相同、7组不同（错误文本字段或真实本地 OID），不抹掉差异。

普通 ADD CHECK/FK 与同样有效数据的旧实现可比较所需验证成本。NOT VALID 可描述去掉不需要的历史扫描，但旧版多做扫描，不能称同扫描工作量提速；旧 VALIDATE 仅改标记，其耗时不是正确验证的性能基线。整个普通 ALTER TABLE 仍有 DDL 物理回滚快照、文件复制/摘要/同步成本，目标 PA fetch=0 不表示整句 O(1)、无堆文件读取或无 I/O。

固定0/128/1024行×三轮before-first/candidate/before-return，27个独立主流程+27次同数据冷重启均正常0；216个ADD/VALIDATE计时点、3024份响应、2384640个完整单元格（157950个NULL）、18360份物理文件映像（其中2700份堆映像）全部独立核对。1,024行时，CHECK NOT VALID wall中位数对前/后旧版分别−63.939%/−61.677%、server CPU −71.962%/−72.695%；FK NOT VALID wall −40.047%/−39.400%、CPU −15.482%/−13.469%，这两项是消除旧版多做的历史扫描。普通ADD CHECK wall −18.399%/−14.808%，CPU +0.124%/−4.360%；普通ADD FK wall −38.575%/−35.423%，CPU −4.131%/−0.022%。补上真实历史检查的CHECK VALIDATE wall +52.762%/+43.290%、CPU +260.317%/+251.282%，属于新增正确性工作成本，不能与旧假验证比较成等价速度。

负结果也保留：空表普通ADD CHECK CPU +29.920%/+35.993%；1,024行已验证FK重复VALIDATE wall对首/返旧版为−36.791%/+51.673%。全样本最长单点17.977415s，旧版wall中位数往返漂移−58.325%～+60.792%，server CPU漂移−28.186%～+4.689%。完整72行metric统计含各角色raw3/min/p50/max、双基准差值与旧版漂移，没有剔除负值/长尾，没有稳定倍数、因果结论或PG速度等价声明。计时是普通socket请求至Ready，包含DDL物理快照成本；process CPU与proc快照非原子、含所有服务线程，不是独占算子成本。

### 普通DDL计时方法、首轮失败与完整分布

固定人口N=0/128/1024，每规模before-first/C4/before-return各三轮；每个主上下文依次执行8条ADD/VALIDATE，单个样本calls=1，不把DDL总时长当每行成本。所有数据在计时前冻结，主/冷共54个成本上下文正常0；保留全部216个DDL样本，未重采样或剔除长尾。

DDL wall从普通Simple-Q发送到完整Ready I接收，含原物理回滚快照、文件复制/摘要/同步和正常提交。设置、全值/NULL/OID核对、解码与hex转换、图像写盘、/proc采样和冷重启在wall外；服务器process CPU含全部服务线程，CPU/proc与wall括号非原子，不是互斥算子成本。DDL时限120s，完整结果SELECT时限15s，正常退出15s；使用普通O2/C++17二进制，不借native fetch观察器的时钟作正常速度结论。

普通ADD CHECK/FK验证相同有效人口。NOT VALID可描述消除旧版不必要的历史扫描，但不称同扫描工作量提速；VALIDATE旧版只是改flag，其比较仅为补全正确性工作成本。所有SELECT只用于完整结果验证，本轮没有重新测DML吞吐或一般SELECT性能，也没有PG性能对照。

实际C1普通构建exit2，因新增caller使用private parser entry而未完成编译；没有C1程序或core，不把它混作正常候选。首失败源码248与完整日志保留。最终C4普通增量构建exit0，实际只重编TableManage.cpp与parser.cpp两TU，耗时192.538845s；其余生产变更来自此前已冻代别，最终整体为TableManage.cpp/h、DdlExecutor.cpp、parser.cpp四文件。

独立数值检查直接读取27份原trace中的216条observation及clock_brackets，重算全部raw3/min/p50/max和72个双基准百分比/漂移/方向，与已运行的独立reducer精确一致（395/395）。核对回执为`constraint-cost27-independent-numeric-review-v1.json`，SHA256 `bf3197878d1c0cbc410c83e7f792e2cafbb377eca050db7d0b6cdb8267656511`；没有再次执行reducer或任何产品SQL。原始完整表`constraint-cost27-full-performance-tables-independent-v1.md`的SHA256为`fc3b1ea5daf7821df16b73f85a6d90087c150924aa8730f8a4e56efe1be110a2`，下表只降低Markdown标题层级，全部数值不变。

#### wall_ns：全部raw3/min/p50/max

| N | Region | Label | raw3 ms | min ms | p50 ms | max ms |
|---:|---|---|---|---:|---:|---:|
| 0 | ordinary_add_check | before-first | 155.761764, 144.794960, 102.017399 | 102.017399 | 144.794960 | 155.761764 |
| 0 | ordinary_add_check | candidate | 178.090169, 133.361275, 191.642754 | 133.361275 | 178.090169 | 191.642754 |
| 0 | ordinary_add_check | before-return | 238.981639, 176.796368, 232.819102 | 176.796368 | 232.819102 | 238.981639 |
| 0 | ordinary_add_fk | before-first | 155.253304, 134.265086, 200.232432 | 134.265086 | 155.253304 | 200.232432 |
| 0 | ordinary_add_fk | candidate | 138.941043, 195.468897, 136.907166 | 136.907166 | 138.941043 | 195.468897 |
| 0 | ordinary_add_fk | before-return | 103.703910, 124.208901, 101.536793 | 101.536793 | 103.703910 | 124.208901 |
| 0 | add_check_not_valid | before-first | 250.483470, 152.284024, 207.739423 | 152.284024 | 207.739423 | 250.483470 |
| 0 | add_check_not_valid | candidate | 211.515082, 477.005840, 129.144454 | 129.144454 | 211.515082 | 477.005840 |
| 0 | add_check_not_valid | before-return | 124.323793, 112.461252, 120.913726 | 112.461252 | 120.913726 | 124.323793 |
| 0 | validate_check | before-first | 130.353334, 197.367726, 153.549836 | 130.353334 | 153.549836 | 197.367726 |
| 0 | validate_check | candidate | 218.080128, 155.348062, 85.988354 | 85.988354 | 155.348062 | 218.080128 |
| 0 | validate_check | before-return | 169.005208, 125.236866, 190.220709 | 125.236866 | 169.005208 | 190.220709 |
| 0 | add_fk_not_valid | before-first | 169.973624, 221.495725, 121.427902 | 121.427902 | 169.973624 | 221.495725 |
| 0 | add_fk_not_valid | candidate | 129.722082, 199.028258, 284.158695 | 129.722082 | 199.028258 | 284.158695 |
| 0 | add_fk_not_valid | before-return | 253.159372, 191.155944, 158.033797 | 158.033797 | 191.155944 | 253.159372 |
| 0 | validate_fk | before-first | 374.102868, 140.002164, 119.417564 | 119.417564 | 140.002164 | 374.102868 |
| 0 | validate_fk | candidate | 137.984535, 201.837331, 90.699390 | 90.699390 | 137.984535 | 201.837331 |
| 0 | validate_fk | before-return | 140.023058, 228.393991, 168.326336 | 140.023058 | 168.326336 | 228.393991 |
| 0 | validate_check_already_valid | before-first | 192.974419, 183.084848, 93.067683 | 93.067683 | 183.084848 | 192.974419 |
| 0 | validate_check_already_valid | candidate | 157.867584, 151.863319, 96.564755 | 96.564755 | 151.863319 | 157.867584 |
| 0 | validate_check_already_valid | before-return | 170.017723, 418.918900, 134.390211 | 134.390211 | 170.017723 | 418.918900 |
| 0 | validate_fk_already_valid | before-first | 352.320746, 220.909057, 87.069476 | 87.069476 | 220.909057 | 352.320746 |
| 0 | validate_fk_already_valid | candidate | 152.752231, 98.104167, 107.679638 | 98.104167 | 107.679638 | 152.752231 |
| 0 | validate_fk_already_valid | before-return | 159.269599, 212.820344, 203.339533 | 159.269599 | 203.339533 | 212.820344 |
| 128 | ordinary_add_check | before-first | 460.540797, 473.186664, 528.302513 | 460.540797 | 473.186664 | 528.302513 |
| 128 | ordinary_add_check | candidate | 414.469641, 361.935864, 398.187797 | 361.935864 | 398.187797 | 414.469641 |
| 128 | ordinary_add_check | before-return | 430.909709, 592.253878, 367.378673 | 367.378673 | 430.909709 | 592.253878 |
| 128 | ordinary_add_fk | before-first | 191.542157, 259.697708, 379.488299 | 191.542157 | 259.697708 | 379.488299 |
| 128 | ordinary_add_fk | candidate | 257.531890, 284.486521, 267.141270 | 257.531890 | 267.141270 | 284.486521 |
| 128 | ordinary_add_fk | before-return | 274.569067, 386.261212, 204.285743 | 204.285743 | 274.569067 | 386.261212 |
| 128 | add_check_not_valid | before-first | 397.781682, 388.933944, 645.036579 | 388.933944 | 397.781682 | 645.036579 |
| 128 | add_check_not_valid | candidate | 305.702115, 413.191233, 273.949059 | 273.949059 | 305.702115 | 413.191233 |
| 128 | add_check_not_valid | before-return | 508.578248, 392.918490, 371.207542 | 371.207542 | 392.918490 | 508.578248 |
| 128 | validate_check | before-first | 295.783394, 220.919231, 308.720576 | 220.919231 | 295.783394 | 308.720576 |
| 128 | validate_check | candidate | 388.709721, 411.173774, 394.436447 | 388.709721 | 394.436447 | 411.173774 |
| 128 | validate_check | before-return | 268.625939, 249.603685, 292.456535 | 249.603685 | 268.625939 | 292.456535 |
| 128 | add_fk_not_valid | before-first | 277.260790, 268.740729, 364.569999 | 268.740729 | 277.260790 | 364.569999 |
| 128 | add_fk_not_valid | candidate | 257.972354, 363.785070, 255.745221 | 255.745221 | 257.972354 | 363.785070 |
| 128 | add_fk_not_valid | before-return | 319.475570, 297.660694, 269.659593 | 269.659593 | 297.660694 | 319.475570 |
| 128 | validate_fk | before-first | 285.735627, 220.503861, 240.605541 | 220.503861 | 240.605541 | 285.735627 |
| 128 | validate_fk | candidate | 290.708986, 286.210104, 217.549909 | 217.549909 | 286.210104 | 290.708986 |
| 128 | validate_fk | before-return | 405.858253, 347.760799, 308.743175 | 308.743175 | 347.760799 | 405.858253 |
| 128 | validate_check_already_valid | before-first | 257.762383, 218.670628, 373.125787 | 218.670628 | 257.762383 | 373.125787 |
| 128 | validate_check_already_valid | candidate | 214.480722, 520.702769, 225.626465 | 214.480722 | 225.626465 | 520.702769 |
| 128 | validate_check_already_valid | before-return | 377.558785, 473.120299, 266.969338 | 266.969338 | 377.558785 | 473.120299 |
| 128 | validate_fk_already_valid | before-first | 365.993510, 245.656323, 303.635288 | 245.656323 | 303.635288 | 365.993510 |
| 128 | validate_fk_already_valid | candidate | 247.336276, 330.264363, 264.323255 | 247.336276 | 264.323255 | 330.264363 |
| 128 | validate_fk_already_valid | before-return | 358.195180, 402.792761, 232.667827 | 232.667827 | 358.195180 | 402.792761 |
| 1024 | ordinary_add_check | before-first | 1991.472841, 1582.072701, 2349.398579 | 1582.072701 | 1991.472841 | 2349.398579 |
| 1024 | ordinary_add_check | candidate | 2197.475451, 1606.542845, 1625.055096 | 1606.542845 | 1625.055096 | 2197.475451 |
| 1024 | ordinary_add_check | before-return | 2554.889309, 1702.605201, 1907.510728 | 1702.605201 | 1907.510728 | 2554.889309 |
| 1024 | ordinary_add_fk | before-first | 1548.954488, 589.258063, 1164.265473 | 589.258063 | 1164.265473 | 1548.954488 |
| 1024 | ordinary_add_fk | candidate | 999.240449, 602.502935, 715.153195 | 602.502935 | 715.153195 | 999.240449 |
| 1024 | ordinary_add_fk | before-return | 1430.145887, 1107.448266, 927.559524 | 927.559524 | 1107.448266 | 1430.145887 |
| 1024 | add_check_not_valid | before-first | 2459.004811, 1676.121227, 2489.280238 | 1676.121227 | 2459.004811 | 2489.280238 |
| 1024 | add_check_not_valid | candidate | 1423.121689, 622.004539, 886.748612 | 622.004539 | 886.748612 | 1423.121689 |
| 1024 | add_check_not_valid | before-return | 2757.937500, 2313.865474, 1832.403551 | 1832.403551 | 2313.865474 | 2757.937500 |
| 1024 | validate_check | before-first | 1246.912806, 657.511829, 1096.364883 | 657.511829 | 1096.364883 | 1246.912806 |
| 1024 | validate_check | candidate | 1745.114230, 1559.196102, 1674.825960 | 1559.196102 | 1674.825960 | 1745.114230 |
| 1024 | validate_check | before-return | 1522.273635, 1168.840632, 790.185478 | 790.185478 | 1168.840632 | 1522.273635 |
| 1024 | add_fk_not_valid | before-first | 1631.760428, 659.352832, 1161.623986 | 659.352832 | 1161.623986 | 1631.760428 |
| 1024 | add_fk_not_valid | candidate | 781.945956, 602.945147, 696.423977 | 602.945147 | 696.423977 | 781.945956 |
| 1024 | add_fk_not_valid | before-return | 10991.405833, 1149.211360, 662.551714 | 662.551714 | 1149.211360 | 10991.405833 |
| 1024 | validate_fk | before-first | 1658.437841, 731.696073, 17977.414798 | 731.696073 | 1658.437841 | 17977.414798 |
| 1024 | validate_fk | candidate | 641.502007, 781.733982, 1141.881860 | 641.502007 | 781.733982 | 1141.881860 |
| 1024 | validate_fk | before-return | 4163.071890, 869.136400, 677.391296 | 677.391296 | 869.136400 | 4163.071890 |
| 1024 | validate_check_already_valid | before-first | 1646.108672, 652.985221, 9608.481767 | 652.985221 | 1646.108672 | 9608.481767 |
| 1024 | validate_check_already_valid | candidate | 568.720819, 636.564631, 1169.093482 | 568.720819 | 636.564631 | 1169.093482 |
| 1024 | validate_check_already_valid | before-return | 1065.534510, 1168.387840, 690.597050 | 690.597050 | 1065.534510 | 1168.387840 |
| 1024 | validate_fk_already_valid | before-first | 1623.608911, 640.724890, 4143.554965 | 640.724890 | 1623.608911 | 4143.554965 |
| 1024 | validate_fk_already_valid | candidate | 1026.269781, 641.607517, 1071.331441 | 641.607517 | 1026.269781 | 1071.331441 |
| 1024 | validate_fk_already_valid | before-return | 676.634387, 1108.398861, 671.261384 | 671.261384 | 676.634387 | 1108.398861 |

#### wall_ns：全部两次旧版对照及漂移

| N | Region | before-first p50 ms | C4 p50 ms | before-return p50 ms | C4/first % | C4/return % | old-return drift % |
|---:|---|---:|---:|---:|---:|---:|---:|
| 0 | ordinary_add_check | 144.794960 | 178.090169 | 232.819102 | +22.994729 | -23.507063 | +60.792269 |
| 0 | ordinary_add_fk | 155.253304 | 138.941043 | 103.703910 | -10.506869 | +33.978596 | -33.203412 |
| 0 | add_check_not_valid | 207.739423 | 211.515082 | 120.913726 | +1.817498 | +74.930580 | -41.795484 |
| 0 | validate_check | 153.549836 | 155.348062 | 169.005208 | +1.171103 | -8.080902 | +10.065378 |
| 0 | add_fk_not_valid | 169.973624 | 199.028258 | 191.155944 | +17.093613 | +4.118268 | +12.462122 |
| 0 | validate_fk | 140.002164 | 137.984535 | 168.326336 | -1.441141 | -18.025582 | +20.231239 |
| 0 | validate_check_already_valid | 183.084848 | 151.863319 | 170.017723 | -17.053038 | -10.677948 | -7.137196 |
| 0 | validate_fk_already_valid | 220.909057 | 107.679638 | 203.339533 | -51.256123 | -47.044416 | -7.953284 |
| 128 | ordinary_add_check | 473.186664 | 398.187797 | 430.909709 | -15.849742 | -7.593682 | -8.934520 |
| 128 | ordinary_add_fk | 259.697708 | 267.141270 | 274.569067 | +2.866241 | -2.705256 | +5.726411 |
| 128 | add_check_not_valid | 397.781682 | 305.702115 | 392.918490 | -23.148267 | -22.197066 | -1.222578 |
| 128 | validate_check | 295.783394 | 394.436447 | 268.625939 | +33.353141 | +46.834832 | -9.181535 |
| 128 | add_fk_not_valid | 277.260790 | 257.972354 | 297.660694 | -6.956785 | -13.333416 | +7.357659 |
| 128 | validate_fk | 240.605541 | 286.210104 | 347.760799 | +18.954078 | -17.699147 | +44.535657 |
| 128 | validate_check_already_valid | 257.762383 | 225.626465 | 377.558785 | -12.467264 | -40.240706 | +46.475518 |
| 128 | validate_fk_already_valid | 303.635288 | 264.323255 | 358.195180 | -12.947123 | -26.206920 | +17.968890 |
| 1024 | ordinary_add_check | 1991.472841 | 1625.055096 | 1907.510728 | -18.399334 | -14.807551 | -4.216081 |
| 1024 | ordinary_add_fk | 1164.265473 | 715.153195 | 1107.448266 | -38.574731 | -35.423332 | -4.880090 |
| 1024 | add_check_not_valid | 2459.004811 | 886.748612 | 2313.865474 | -63.938720 | -61.676743 | -5.902361 |
| 1024 | validate_check | 1096.364883 | 1674.825960 | 1168.840632 | +52.761730 | +43.289505 | +6.610550 |
| 1024 | add_fk_not_valid | 1161.623986 | 696.423977 | 1149.211360 | -40.047383 | -39.399835 | -1.068558 |
| 1024 | validate_fk | 1658.437841 | 781.733982 | 869.136400 | -52.863233 | -10.056237 | -47.593067 |
| 1024 | validate_check_already_valid | 1646.108672 | 636.564631 | 1065.534510 | -61.329125 | -40.258657 | -35.269492 |
| 1024 | validate_fk_already_valid | 1623.608911 | 1026.269781 | 676.634387 | -36.790826 | +51.672720 | -58.325285 |

#### server_process_cpu_ns：全部raw3/min/p50/max

| N | Region | Label | raw3 ms | min ms | p50 ms | max ms |
|---:|---|---|---|---:|---:|---:|
| 0 | ordinary_add_check | before-first | 41.139916, 49.831927, 34.056474 | 34.056474 | 41.139916 | 49.831927 |
| 0 | ordinary_add_check | candidate | 53.711058, 52.925405, 53.448903 | 52.925405 | 53.448903 | 53.711058 |
| 0 | ordinary_add_check | before-return | 34.358600, 39.302717, 45.272365 | 34.358600 | 39.302717 | 45.272365 |
| 0 | ordinary_add_fk | before-first | 41.204043, 34.713468, 34.979677 | 34.713468 | 34.979677 | 41.204043 |
| 0 | ordinary_add_fk | candidate | 46.853141, 44.721303, 34.179940 | 34.179940 | 44.721303 | 46.853141 |
| 0 | ordinary_add_fk | before-return | 26.684868, 36.117697, 43.198729 | 26.684868 | 36.117697 | 43.198729 |
| 0 | add_check_not_valid | before-first | 39.909985, 48.486191, 45.838050 | 39.909985 | 45.838050 | 48.486191 |
| 0 | add_check_not_valid | candidate | 50.395890, 40.742273, 38.687158 | 38.687158 | 40.742273 | 50.395890 |
| 0 | add_check_not_valid | before-return | 31.279620, 32.918104, 43.974577 | 31.279620 | 32.918104 | 43.974577 |
| 0 | validate_check | before-first | 34.224153, 43.319341, 31.117870 | 31.117870 | 34.224153 | 43.319341 |
| 0 | validate_check | candidate | 52.837207, 42.506590, 24.984039 | 24.984039 | 42.506590 | 52.837207 |
| 0 | validate_check | before-return | 30.934421, 30.283603, 41.258610 | 30.283603 | 30.934421 | 41.258610 |
| 0 | add_fk_not_valid | before-first | 44.695810, 42.328964, 31.495354 | 31.495354 | 42.328964 | 44.695810 |
| 0 | add_fk_not_valid | candidate | 40.408512, 33.964532, 31.390675 | 31.390675 | 33.964532 | 40.408512 |
| 0 | add_fk_not_valid | before-return | 43.889297, 51.908870, 29.981564 | 29.981564 | 43.889297 | 51.908870 |
| 0 | validate_fk | before-first | 39.027542, 30.620743, 44.325763 | 30.620743 | 39.027542 | 44.325763 |
| 0 | validate_fk | candidate | 43.243896, 35.802706, 35.802385 | 35.802385 | 35.802706 | 43.243896 |
| 0 | validate_fk | before-return | 31.207348, 40.855009, 44.554233 | 31.207348 | 40.855009 | 44.554233 |
| 0 | validate_check_already_valid | before-first | 43.624838, 42.633738, 28.575570 | 28.575570 | 42.633738 | 43.624838 |
| 0 | validate_check_already_valid | candidate | 42.448319, 40.482198, 39.927949 | 39.927949 | 40.482198 | 42.448319 |
| 0 | validate_check_already_valid | before-return | 23.654843, 42.642411, 43.519244 | 23.654843 | 42.642411 | 43.519244 |
| 0 | validate_fk_already_valid | before-first | 45.272276, 42.887379, 27.149414 | 27.149414 | 42.887379 | 45.272276 |
| 0 | validate_fk_already_valid | candidate | 42.686814, 34.111321, 29.663410 | 29.663410 | 34.111321 | 42.686814 |
| 0 | validate_fk_already_valid | before-return | 29.779514, 34.445401, 33.572809 | 29.779514 | 33.572809 | 34.445401 |
| 128 | ordinary_add_check | before-first | 249.076863, 279.479818, 224.511491 | 224.511491 | 249.076863 | 279.479818 |
| 128 | ordinary_add_check | candidate | 245.379900, 207.678740, 237.569104 | 207.678740 | 237.569104 | 245.379900 |
| 128 | ordinary_add_check | before-return | 246.104017, 226.840826, 226.978007 | 226.840826 | 226.978007 | 246.104017 |
| 128 | ordinary_add_fk | before-first | 83.273338, 87.876531, 89.892899 | 83.273338 | 87.876531 | 89.892899 |
| 128 | ordinary_add_fk | candidate | 83.930983, 76.689232, 82.365920 | 76.689232 | 82.365920 | 83.930983 |
| 128 | ordinary_add_fk | before-return | 82.067177, 86.475314, 87.188262 | 82.067177 | 86.475314 | 87.188262 |
| 128 | add_check_not_valid | before-first | 243.772448, 240.098641, 265.125215 | 240.098641 | 243.772448 | 265.125215 |
| 128 | add_check_not_valid | candidate | 88.732192, 83.924892, 87.676573 | 83.924892 | 87.676573 | 88.732192 |
| 128 | add_check_not_valid | before-return | 263.915715, 236.766467, 245.043854 | 236.766467 | 245.043854 | 263.915715 |
| 128 | validate_check | before-first | 85.758091, 78.568144, 83.463770 | 78.568144 | 83.463770 | 85.758091 |
| 128 | validate_check | candidate | 224.534599, 209.182893, 240.241140 | 209.182893 | 224.534599 | 240.241140 |
| 128 | validate_check | before-return | 76.767518, 75.496089, 100.092410 | 75.496089 | 76.767518 | 100.092410 |
| 128 | add_fk_not_valid | before-first | 86.883662, 82.984699, 91.273629 | 82.984699 | 86.883662 | 91.273629 |
| 128 | add_fk_not_valid | candidate | 79.436090, 81.770854, 81.557516 | 79.436090 | 81.557516 | 81.770854 |
| 128 | add_fk_not_valid | before-return | 83.020983, 90.480786, 86.504173 | 83.020983 | 86.504173 | 90.480786 |
| 128 | validate_fk | before-first | 89.127871, 75.553658, 89.342619 | 75.553658 | 89.127871 | 89.342619 |
| 128 | validate_fk | candidate | 85.948199, 85.433794, 84.573282 | 84.573282 | 85.433794 | 85.948199 |
| 128 | validate_fk | before-return | 86.613655, 82.189930, 85.862827 | 82.189930 | 85.862827 | 86.613655 |
| 128 | validate_check_already_valid | before-first | 85.282189, 72.756976, 92.214456 | 72.756976 | 85.282189 | 92.214456 |
| 128 | validate_check_already_valid | candidate | 74.775276, 84.622263, 86.693388 | 74.775276 | 84.622263 | 86.693388 |
| 128 | validate_check_already_valid | before-return | 84.065211, 86.184201, 78.547970 | 78.547970 | 84.065211 | 86.184201 |
| 128 | validate_fk_already_valid | before-first | 105.233426, 69.938331, 104.865658 | 69.938331 | 104.865658 | 105.233426 |
| 128 | validate_fk_already_valid | candidate | 80.647037, 81.347921, 82.064721 | 80.647037 | 81.347921 | 82.064721 |
| 128 | validate_fk_already_valid | before-return | 110.787478, 83.392047, 80.404632 | 80.404632 | 83.392047 | 110.787478 |
| 1024 | ordinary_add_check | before-first | 1290.540565, 1363.232179, 1434.368258 | 1290.540565 | 1363.232179 | 1434.368258 |
| 1024 | ordinary_add_check | candidate | 1411.256325, 1364.921098, 1358.538195 | 1358.538195 | 1364.921098 | 1411.256325 |
| 1024 | ordinary_add_check | before-return | 1427.150951, 1398.444054, 1451.976770 | 1398.444054 | 1427.150951 | 1451.976770 |
| 1024 | ordinary_add_fk | before-first | 424.749549, 384.839788, 425.617277 | 384.839788 | 424.749549 | 425.617277 |
| 1024 | ordinary_add_fk | candidate | 407.205188, 417.677782, 383.843274 | 383.843274 | 407.205188 | 417.677782 |
| 1024 | ordinary_add_fk | before-return | 397.225375, 424.779471, 407.292821 | 397.225375 | 407.292821 | 424.779471 |
| 1024 | add_check_not_valid | before-first | 1383.197382, 1434.714821, 1484.028373 | 1383.197382 | 1434.714821 | 1484.028373 |
| 1024 | add_check_not_valid | candidate | 402.268373, 390.704953, 442.317481 | 390.704953 | 402.268373 | 442.317481 |
| 1024 | add_check_not_valid | before-return | 1553.008265, 1473.240229, 1441.814333 | 1441.814333 | 1473.240229 | 1553.008265 |
| 1024 | validate_check | before-first | 362.927608, 400.396629, 368.747705 | 362.927608 | 368.747705 | 400.396629 |
| 1024 | validate_check | candidate | 1298.983282, 1328.660334, 1373.965196 | 1298.983282 | 1328.660334 | 1373.965196 |
| 1024 | validate_check | before-return | 378.232358, 395.770601, 372.972231 | 372.972231 | 378.232358 | 395.770601 |
| 1024 | add_fk_not_valid | before-first | 454.256772, 415.281641, 442.274800 | 415.281641 | 442.274800 | 454.256772 |
| 1024 | add_fk_not_valid | candidate | 417.664927, 373.801692, 368.419954 | 368.419954 | 373.801692 | 417.664927 |
| 1024 | add_fk_not_valid | before-return | 425.862624, 431.985968, 447.085192 | 425.862624 | 431.985968 | 447.085192 |
| 1024 | validate_fk | before-first | 422.095171, 410.014501, 468.084013 | 410.014501 | 422.095171 | 468.084013 |
| 1024 | validate_fk | candidate | 420.564651, 428.071407, 375.411284 | 375.411284 | 420.564651 | 428.071407 |
| 1024 | validate_fk | before-return | 455.368084, 403.178464, 418.848869 | 403.178464 | 418.848869 | 455.368084 |
| 1024 | validate_check_already_valid | before-first | 418.469444, 400.938499, 472.949544 | 400.938499 | 418.469444 | 472.949544 |
| 1024 | validate_check_already_valid | candidate | 393.155325, 408.339020, 454.247003 | 393.155325 | 408.339020 | 454.247003 |
| 1024 | validate_check_already_valid | before-return | 414.618446, 420.242159, 405.788784 | 405.788784 | 414.618446 | 420.242159 |
| 1024 | validate_fk_already_valid | before-first | 423.171724, 406.101016, 454.679787 | 406.101016 | 423.171724 | 454.679787 |
| 1024 | validate_fk_already_valid | candidate | 408.206407, 410.830393, 390.579018 | 390.579018 | 408.206407 | 410.830393 |
| 1024 | validate_fk_already_valid | before-return | 418.524773, 409.730498, 414.390961 | 409.730498 | 414.390961 | 418.524773 |

#### server_process_cpu_ns：全部两次旧版对照及漂移

| N | Region | before-first p50 ms | C4 p50 ms | before-return p50 ms | C4/first % | C4/return % | old-return drift % |
|---:|---|---:|---:|---:|---:|---:|---:|
| 0 | ordinary_add_check | 41.139916 | 53.448903 | 39.302717 | +29.919816 | +35.992896 | -4.465733 |
| 0 | ordinary_add_fk | 34.979677 | 44.721303 | 36.117697 | +27.849388 | +23.821026 | +3.253375 |
| 0 | add_check_not_valid | 45.838050 | 40.742273 | 32.918104 | -11.116915 | +23.768589 | -28.186072 |
| 0 | validate_check | 34.224153 | 42.506590 | 30.934421 | +24.200561 | +37.408714 | -9.612311 |
| 0 | add_fk_not_valid | 42.328964 | 33.964532 | 43.889297 | -19.760540 | -22.613178 | +3.686206 |
| 0 | validate_fk | 39.027542 | 35.802706 | 40.855009 | -8.262975 | -12.366422 | +4.682506 |
| 0 | validate_check_already_valid | 42.633738 | 40.482198 | 42.642411 | -5.046567 | -5.065879 | +0.020343 |
| 0 | validate_fk_already_valid | 42.887379 | 34.111321 | 33.572809 | -20.463032 | +1.604012 | -21.718674 |
| 128 | ordinary_add_check | 249.076863 | 237.569104 | 226.978007 | -4.620164 | +4.666134 | -8.872304 |
| 128 | ordinary_add_fk | 87.876531 | 82.365920 | 86.475314 | -6.270856 | -4.752101 | -1.594529 |
| 128 | add_check_not_valid | 243.772448 | 87.676573 | 245.043854 | -64.033436 | -64.220048 | +0.521554 |
| 128 | validate_check | 83.463770 | 224.534599 | 76.767518 | +169.020437 | +192.486464 | -8.022945 |
| 128 | add_fk_not_valid | 86.883662 | 81.557516 | 86.504173 | -6.130204 | -5.718403 | -0.436778 |
| 128 | validate_fk | 89.127871 | 85.433794 | 85.862827 | -4.144693 | -0.499673 | -3.663325 |
| 128 | validate_check_already_valid | 85.282189 | 84.622263 | 84.065211 | -0.773815 | +0.662643 | -1.427001 |
| 128 | validate_fk_already_valid | 104.865658 | 81.347921 | 83.392047 | -22.426538 | -2.451224 | -20.477258 |
| 1024 | ordinary_add_check | 1363.232179 | 1364.921098 | 1427.150951 | +0.123891 | -4.360425 | +4.688766 |
| 1024 | ordinary_add_fk | 424.749549 | 407.205188 | 407.292821 | -4.130519 | -0.021516 | -4.109887 |
| 1024 | add_check_not_valid | 1434.714821 | 402.268373 | 1473.240229 | -71.961789 | -72.694991 | +2.685231 |
| 1024 | validate_check | 368.747705 | 1328.660334 | 378.232358 | +260.316909 | +251.281509 | +2.572125 |
| 1024 | add_fk_not_valid | 442.274800 | 373.801692 | 431.985968 | -15.482028 | -13.469020 | -2.326344 |
| 1024 | validate_fk | 422.095171 | 420.564651 | 418.848869 | -0.362601 | +0.409642 | -0.769092 |
| 1024 | validate_check_already_valid | 418.469444 | 408.339020 | 414.618446 | -2.420828 | -1.514507 | -0.920258 |
| 1024 | validate_fk_already_valid | 423.171724 | 408.206407 | 414.390961 | -3.536464 | -1.492444 | -2.074988 |

#### client_process_cpu_ns：全部raw3/min/p50/max

| N | Region | Label | raw3 ms | min ms | p50 ms | max ms |
|---:|---|---|---|---:|---:|---:|
| 0 | ordinary_add_check | before-first | 0.247363, 0.229260, 0.206534 | 0.206534 | 0.229260 | 0.247363 |
| 0 | ordinary_add_check | candidate | 0.282140, 0.234546, 0.285137 | 0.234546 | 0.282140 | 0.285137 |
| 0 | ordinary_add_check | before-return | 0.248601, 0.214566, 0.271320 | 0.214566 | 0.248601 | 0.271320 |
| 0 | ordinary_add_fk | before-first | 0.308166, 0.234367, 0.225034 | 0.225034 | 0.234367 | 0.308166 |
| 0 | ordinary_add_fk | candidate | 0.269965, 0.248720, 0.217858 | 0.217858 | 0.248720 | 0.269965 |
| 0 | ordinary_add_fk | before-return | 0.172765, 0.237582, 0.271184 | 0.172765 | 0.237582 | 0.271184 |
| 0 | add_check_not_valid | before-first | 0.242992, 0.248710, 0.230274 | 0.230274 | 0.242992 | 0.248710 |
| 0 | add_check_not_valid | candidate | 0.223205, 0.146313, 0.245701 | 0.146313 | 0.223205 | 0.245701 |
| 0 | add_check_not_valid | before-return | 0.136304, 0.261326, 0.256427 | 0.136304 | 0.256427 | 0.261326 |
| 0 | validate_check | before-first | 0.232834, 0.244010, 0.250720 | 0.232834 | 0.244010 | 0.250720 |
| 0 | validate_check | candidate | 0.235893, 0.183267, 0.220390 | 0.183267 | 0.220390 | 0.235893 |
| 0 | validate_check | before-return | 0.203275, 0.188626, 0.271317 | 0.188626 | 0.203275 | 0.271317 |
| 0 | add_fk_not_valid | before-first | 0.217760, 0.185956, 0.201208 | 0.185956 | 0.201208 | 0.217760 |
| 0 | add_fk_not_valid | candidate | 0.243876, 0.264518, 0.254445 | 0.243876 | 0.254445 | 0.264518 |
| 0 | add_fk_not_valid | before-return | 0.228637, 0.236354, 0.243528 | 0.228637 | 0.236354 | 0.243528 |
| 0 | validate_fk | before-first | 0.196461, 0.186956, 0.201820 | 0.186956 | 0.196461 | 0.201820 |
| 0 | validate_fk | candidate | 0.224507, 0.136491, 0.280318 | 0.136491 | 0.224507 | 0.280318 |
| 0 | validate_fk | before-return | 0.208393, 0.209873, 0.269058 | 0.208393 | 0.209873 | 0.269058 |
| 0 | validate_check_already_valid | before-first | 0.235793, 0.248472, 0.190594 | 0.190594 | 0.235793 | 0.248472 |
| 0 | validate_check_already_valid | candidate | 0.220536, 0.217620, 0.248880 | 0.217620 | 0.220536 | 0.248880 |
| 0 | validate_check_already_valid | before-return | 0.194262, 0.351223, 0.326900 | 0.194262 | 0.326900 | 0.351223 |
| 0 | validate_fk_already_valid | before-first | 0.255581, 0.250925, 0.186239 | 0.186239 | 0.250925 | 0.255581 |
| 0 | validate_fk_already_valid | candidate | 0.288930, 0.205252, 0.216244 | 0.205252 | 0.216244 | 0.288930 |
| 0 | validate_fk_already_valid | before-return | 0.223854, 0.236964, 0.210044 | 0.210044 | 0.223854 | 0.236964 |
| 128 | ordinary_add_check | before-first | 0.229166, 0.234372, 0.226020 | 0.226020 | 0.229166 | 0.234372 |
| 128 | ordinary_add_check | candidate | 0.224653, 0.217826, 0.264744 | 0.217826 | 0.224653 | 0.264744 |
| 128 | ordinary_add_check | before-return | 0.181054, 0.223472, 0.244509 | 0.181054 | 0.223472 | 0.244509 |
| 128 | ordinary_add_fk | before-first | 0.235223, 0.193227, 0.237791 | 0.193227 | 0.235223 | 0.237791 |
| 128 | ordinary_add_fk | candidate | 0.205580, 0.206998, 0.236418 | 0.205580 | 0.206998 | 0.236418 |
| 128 | ordinary_add_fk | before-return | 0.132894, 0.210289, 0.177138 | 0.132894 | 0.177138 | 0.210289 |
| 128 | add_check_not_valid | before-first | 0.206334, 0.222502, 0.219649 | 0.206334 | 0.219649 | 0.222502 |
| 128 | add_check_not_valid | candidate | 0.214504, 0.156887, 0.173457 | 0.156887 | 0.173457 | 0.214504 |
| 128 | add_check_not_valid | before-return | 0.210675, 0.212007, 0.189644 | 0.189644 | 0.210675 | 0.212007 |
| 128 | validate_check | before-first | 0.207727, 0.186907, 0.274829 | 0.186907 | 0.207727 | 0.274829 |
| 128 | validate_check | candidate | 0.210589, 0.166714, 0.226088 | 0.166714 | 0.210589 | 0.226088 |
| 128 | validate_check | before-return | 0.160065, 0.253990, 0.169187 | 0.160065 | 0.169187 | 0.253990 |
| 128 | add_fk_not_valid | before-first | 0.219332, 0.185468, 0.209699 | 0.185468 | 0.209699 | 0.219332 |
| 128 | add_fk_not_valid | candidate | 0.200589, 0.189372, 0.222041 | 0.189372 | 0.200589 | 0.222041 |
| 128 | add_fk_not_valid | before-return | 0.185523, 0.236760, 0.203165 | 0.185523 | 0.203165 | 0.236760 |
| 128 | validate_fk | before-first | 0.209520, 0.243083, 0.244500 | 0.209520 | 0.243083 | 0.244500 |
| 128 | validate_fk | candidate | 0.190580, 0.190915, 0.236091 | 0.190580 | 0.190915 | 0.236091 |
| 128 | validate_fk | before-return | 0.183462, 0.215544, 0.186949 | 0.183462 | 0.186949 | 0.215544 |
| 128 | validate_check_already_valid | before-first | 0.218131, 0.241278, 0.246664 | 0.218131 | 0.241278 | 0.246664 |
| 128 | validate_check_already_valid | candidate | 0.162681, 0.197835, 0.188000 | 0.162681 | 0.188000 | 0.197835 |
| 128 | validate_check_already_valid | before-return | 0.221772, 0.242499, 0.257587 | 0.221772 | 0.242499 | 0.257587 |
| 128 | validate_fk_already_valid | before-first | 0.204427, 0.172840, 0.233098 | 0.172840 | 0.204427 | 0.233098 |
| 128 | validate_fk_already_valid | candidate | 0.187113, 0.226725, 0.211526 | 0.187113 | 0.211526 | 0.226725 |
| 128 | validate_fk_already_valid | before-return | 0.239442, 0.241034, 0.219044 | 0.219044 | 0.239442 | 0.241034 |
| 1024 | ordinary_add_check | before-first | 0.219076, 0.201945, 0.289550 | 0.201945 | 0.219076 | 0.289550 |
| 1024 | ordinary_add_check | candidate | 0.263889, 0.216704, 0.243826 | 0.216704 | 0.243826 | 0.263889 |
| 1024 | ordinary_add_check | before-return | 0.271330, 0.265498, 0.236211 | 0.236211 | 0.265498 | 0.271330 |
| 1024 | ordinary_add_fk | before-first | 0.190266, 0.214982, 0.183526 | 0.183526 | 0.190266 | 0.214982 |
| 1024 | ordinary_add_fk | candidate | 0.162304, 0.252545, 0.192330 | 0.162304 | 0.192330 | 0.252545 |
| 1024 | ordinary_add_fk | before-return | 0.222038, 0.215995, 0.233393 | 0.215995 | 0.222038 | 0.233393 |
| 1024 | add_check_not_valid | before-first | 0.210210, 0.162475, 0.144709 | 0.144709 | 0.162475 | 0.210210 |
| 1024 | add_check_not_valid | candidate | 0.229022, 0.211786, 0.250930 | 0.211786 | 0.229022 | 0.250930 |
| 1024 | add_check_not_valid | before-return | 0.238583, 0.200845, 0.205308 | 0.200845 | 0.205308 | 0.238583 |
| 1024 | validate_check | before-first | 0.207889, 0.145847, 0.246081 | 0.145847 | 0.207889 | 0.246081 |
| 1024 | validate_check | candidate | 0.248407, 0.192601, 0.251223 | 0.192601 | 0.248407 | 0.251223 |
| 1024 | validate_check | before-return | 0.178109, 0.172595, 0.251199 | 0.172595 | 0.178109 | 0.251199 |
| 1024 | add_fk_not_valid | before-first | 0.237559, 0.227265, 0.175672 | 0.175672 | 0.227265 | 0.237559 |
| 1024 | add_fk_not_valid | candidate | 0.240319, 0.229439, 0.203680 | 0.203680 | 0.229439 | 0.240319 |
| 1024 | add_fk_not_valid | before-return | 0.224004, 0.215835, 0.235162 | 0.215835 | 0.224004 | 0.235162 |
| 1024 | validate_fk | before-first | 0.217019, 0.204073, 0.225430 | 0.204073 | 0.217019 | 0.225430 |
| 1024 | validate_fk | candidate | 0.207099, 0.258656, 0.245687 | 0.207099 | 0.245687 | 0.258656 |
| 1024 | validate_fk | before-return | 0.228364, 0.222775, 0.187567 | 0.187567 | 0.222775 | 0.228364 |
| 1024 | validate_check_already_valid | before-first | 0.233616, 0.262030, 0.183833 | 0.183833 | 0.233616 | 0.262030 |
| 1024 | validate_check_already_valid | candidate | 0.230052, 0.211124, 0.220347 | 0.211124 | 0.220347 | 0.230052 |
| 1024 | validate_check_already_valid | before-return | 0.221646, 0.242555, 0.197620 | 0.197620 | 0.221646 | 0.242555 |
| 1024 | validate_fk_already_valid | before-first | 0.230926, 0.198871, 0.228120 | 0.198871 | 0.228120 | 0.230926 |
| 1024 | validate_fk_already_valid | candidate | 0.239158, 0.229122, 0.176193 | 0.176193 | 0.229122 | 0.239158 |
| 1024 | validate_fk_already_valid | before-return | 0.239689, 0.199312, 0.182280 | 0.182280 | 0.199312 | 0.239689 |

#### client_process_cpu_ns：全部两次旧版对照及漂移

| N | Region | before-first p50 ms | C4 p50 ms | before-return p50 ms | C4/first % | C4/return % | old-return drift % |
|---:|---|---:|---:|---:|---:|---:|---:|
| 0 | ordinary_add_check | 0.229260 | 0.282140 | 0.248601 | +23.065515 | +13.491096 | +8.436273 |
| 0 | ordinary_add_fk | 0.234367 | 0.248720 | 0.237582 | +6.124156 | +4.688066 | +1.371780 |
| 0 | add_check_not_valid | 0.242992 | 0.223205 | 0.256427 | -8.143066 | -12.955734 | +5.528989 |
| 0 | validate_check | 0.244010 | 0.220390 | 0.203275 | -9.679931 | +8.419629 | -16.693988 |
| 0 | add_fk_not_valid | 0.201208 | 0.254445 | 0.236354 | +26.458690 | +7.654197 | +17.467496 |
| 0 | validate_fk | 0.196461 | 0.224507 | 0.209873 | +14.275607 | +6.972788 | +6.826800 |
| 0 | validate_check_already_valid | 0.235793 | 0.220536 | 0.326900 | -6.470506 | -32.537167 | +38.638552 |
| 0 | validate_fk_already_valid | 0.250925 | 0.216244 | 0.223854 | -13.821261 | -3.399537 | -10.788483 |
| 128 | ordinary_add_check | 0.229166 | 0.224653 | 0.223472 | -1.969315 | +0.528478 | -2.484662 |
| 128 | ordinary_add_fk | 0.235223 | 0.206998 | 0.177138 | -11.999252 | +16.856914 | -24.693589 |
| 128 | add_check_not_valid | 0.219649 | 0.173457 | 0.210675 | -21.029916 | -17.666073 | -4.085609 |
| 128 | validate_check | 0.207727 | 0.210589 | 0.169187 | +1.377770 | +24.471147 | -18.553197 |
| 128 | add_fk_not_valid | 0.209699 | 0.200589 | 0.203165 | -4.344322 | -1.267935 | -3.115895 |
| 128 | validate_fk | 0.243083 | 0.190915 | 0.186949 | -21.460982 | +2.121434 | -23.092524 |
| 128 | validate_check_already_valid | 0.241278 | 0.188000 | 0.242499 | -22.081582 | -22.473907 | +0.506055 |
| 128 | validate_fk_already_valid | 0.204427 | 0.211526 | 0.239442 | +3.472633 | -11.658773 | +17.128364 |
| 1024 | ordinary_add_check | 0.219076 | 0.243826 | 0.265498 | +11.297449 | -8.162773 | +21.189907 |
| 1024 | ordinary_add_fk | 0.190266 | 0.192330 | 0.222038 | +1.084797 | -13.379692 | +16.698727 |
| 1024 | add_check_not_valid | 0.162475 | 0.229022 | 0.205308 | +40.958301 | +11.550451 | +26.362825 |
| 1024 | validate_check | 0.207889 | 0.248407 | 0.178109 | +19.490209 | +39.469089 | -14.324952 |
| 1024 | add_fk_not_valid | 0.227265 | 0.229439 | 0.224004 | +0.956593 | +2.426296 | -1.434889 |
| 1024 | validate_fk | 0.217019 | 0.245687 | 0.222775 | +13.209903 | +10.284817 | +2.652302 |
| 1024 | validate_check_already_valid | 0.233616 | 0.220347 | 0.221646 | -5.679834 | -0.586070 | -5.123793 |
| 1024 | validate_fk_already_valid | 0.228120 | 0.229122 | 0.199312 | +0.439243 | +14.956450 | -12.628441 |

#### 长尾与范围

| Clock | 最大原始值 ms | N | Region | Label | Round | 旧回测漂移范围 % |
|---|---:|---:|---|---|---:|---|
| wall_ns | 17977.414798 | 1024 | validate_fk | before-first | 3 | [-58.325285, +60.792269] |
| server_process_cpu_ns | 1553.008265 | 1024 | add_check_not_valid | before-return | 1 | [-28.186072, +4.688766] |
| client_process_cpu_ns | 0.351223 | 0 | validate_check_already_valid | before-return | 2 | [-24.693589, +38.638552] |

所有24region×3clock方向计数仅描述该固定cohort，包含不同工作量的NV/VALIDATE，不能汇总成整体优化通过率：`{"wall_ns": {"between-or-equal": 6, "higher-both": 4, "lower-both": 14}, "server_process_cpu_ns": {"higher-both": 5, "between-or-equal": 6, "lower-both": 13}, "client_process_cpu_ns": {"higher-both": 10, "lower-both": 7, "between-or-equal": 7}}`。

完整普通SQL成本不同于native fetch探针。原12项native中的旧routine中止、补充signature19全NOTRUN/multiLOB建表缺口、UNKNOWN输入codec/目录与并发证明未完均由功能/限制段另列；本性能数据不能关闭这些缺口。


### 未完成项与首轮失败边界

验收限制与未完成项：旧12项原生回归是11项正常通过，`ddl_ast_bridge` 同修改前/C3一样以 -6 中止，完整368B日志逐字相同（routine drop_tx_fn/42883）；后续约束段未执行，不能计通过。另一份补充诊断的合法多 LOB CREATE 返回真实 engine status12（通用状态映射为22023，原DDL结果未提供SQLSTATE）、`could not initialize table heap`：两个 BYTEA、两个 TEXT 等逻辑最大长度之和为4,295,098,376，超过UINT32_MAX 131,081字节（尚未加行头），暴露逻辑长度被错误用于物理堆初始化的普通缺口。其19个表达式用例均为 NOTRUN；另外两个 collation 组实际完成但整个生命周期201 checks/1 fixture-stage失败，cold/phase images 未运行，未改fixture绕过。最小建表诊断虽 exit0，退出日志仍有次级 std::bad_alloc，原因未分类，不作为干净退出证明，也未扩入暂缓生命周期调查。普通未完成项还包括完整 pg_constraint SQL目录、PG18 NOT NULL NOT VALID、UNKNOWN隐式字面量输入codec、完整定义依赖/函数与类型绑定及并发锁/快照证明。现有物理NULL/空串、TOAST、事务/savepoint、rename/drop及正常冷重启有限覆盖不等于所有约束或generic故障注入矩阵通过；原用户暂缓范围不变。
本轮216个计时点均为ADD/VALIDATE DDL；普通DML的NULL/collation输入已做功能验证，INSERT/UPDATE/DELETE/COPY吞吐与一般SELECT查询性能没有在本批重新计时，不能用这些DDL结果推断它们提速。

本批四个生产文件、两份新增回归、原登记入口和既有文档在 master 做本地版本管理，使用 `Palpitate-xus <palpitate.xus@outlook.com>`。正常验收程序独立冻结，根目录运行程序仍为 `425165af…`。原始证据只以完整可逆 gzip 入库；53.45MB 的旧 analyze/reindex JSON 已由约5.7MiB gzip替换，旧提交历史未重写。恢复步骤和逐流SHA保存在归档内，构建目录中的原始文件不再直接进入Git。

当前实际清单：源248/头177；测试2388（原2386逐字保持，新增1 CPP与1协议）；顶层CPP1122（含链接桩）/全部CPP1127，其中nested 5；登记683 driver/663 standalone E2E。Markdown 754/保护725保持，旧27份gzip保持，归档总计28。原278条历史原文保持，prepend第279条partial。

本批为有限普通约束生命周期的 partial 进展，Goal active。原 273 项状态保持（22 complete/166 partial/70 unverified/15 deferred_by_user）；`IDX-07` 仍是 `unverified`、`evidence=[]`。本批 GiST 页树资料只是未实现研究，不能将 P27 的 batch partial 记录当作 IDX-07 已部分验证。原用户暂缓边界保持。

[完整可逆原始证据](benchmarks/constraint-validation-20261106.json.gz)。以下 P27 及更早内容是各代历史记录，不把旧计数/旧假验证行为或组件诊断当作当前正常 SQL 性能。

<!-- CURRENT_GIST_PROBE_READER_20261106 -->

## GiST 读取开销与空文件回退（2026-11-06，P27-C2）

<a id="gist-probe-reader-20261106"></a>

P26固定正常SQL对照保留了高端与不存在范围的真实回退。本批针对实际flat GiST读取开销：旧路径先完整CRC再逐项读取、复制低高键并创建RID哈希节点；ifstream本身有缓冲，因此读取调用不等于系统调用。当前CRC已有硬件与portable实现，本次没有改CRC算法或据推测把它认作主因。

C2对不超过1MiB的V2/V3文件捕获同一打开流的完整字节，复核extent/header/count与CRC，再用长度视图解析；严格递增RID线性证明唯一，合法乱序只排序私有验证向量。数值保留原实际字符串与比较器；大文件及非空旧文本仍走原路径。所有范围、RID、尾部、空键覆盖及前后snapshot/dirty/ownership/SSI边界保留，晚期失败不发布局部候选。1MiB只限制捕获图像，不限制候选和RID向量总内存。

同一4096行机制夹具在旧版/C2各实际52219检查、20探测通过，每端六个真实阳性控制；独立归约16614项通过。以absent_closed的overlap探测为例，C++read调用10977降到5，new请求4204降到272，累计请求字节188276升到222607，增加34331B（18.2344%）。全部20项均少3932个请求、多34331请求字节，完整原始计数保留；它们不是RSS、存活字节、峰值或C malloc统计。wall/process/thread只是在启用观察器的每项一次诊断，不能作为正常SQL速度分布或因果结论。

同时发现普通正确性缺口：把真实索引截断0B后，旧版/C1仍把空旧文本当有效索引，非空等值、范围、LIKE实际漏行。C2只拒绝没有任何RID的旧文本来源；合法24字节V3零记录不受影响。正式82命令协议旧版12失败、C2全部通过；reader最终110探测旧版4失败、C2为2215检查/0失败。非空旧文本兼容、CRC/部分头回退、实际空键和NULL、恢复V3后usedIndex=true均有独立严格正例。

C2原120命令协议通过，并复用此前实际PG18.6的75条原始记录进行一次离线对照：3231行/19382单元/661NULL，248列描述符的1736个字段一致；原始帧73/75一致，两次DELETE额外零字段T仍保留，严格全协议通过为false。本轮没有新PG服务或PG性能计时。C2有限17个native入口正常0，恢复专项3303检查/18workers（5SIGKILL、13正常0）及36份输出保留；C1的3413计数属于历史。

C1生命周期回执的source_unchanged=false原样保留；正式源、冻结源、计划和基线哈希相同，实际是运行器source被inventory数据文件路径遮蔽，C2以captured_source修正。此前离线归约首版的诊断参数名冲突也保留，修正没有触发任何native重跑。

生产解析器7个原文字节片段及4个完整依赖（含真实Numeric实现）单独使用ASan/UBSan编译并实际运行一次：12,355检查/8组/2021帧/4042次解析全部通过，含217个非零截断、1744个单bit变异、CRC合法结构异常、1MiB两侧、数值及两个visitor形状；开启泄漏检测，stderr为空。它是解析器组件检查，完整SQL/堆行为由上述真实引擎夹具分别验证。

6个旧协议入口全部正常0；现有生命周期all2879及独立cold183通过。所有阶段文件在冷启前复制保存，未重复P26三个统计文件的早期字节缺失。

最终C2为普通CMake C++17/O2程序，SHA256 `dd5d9b8519ad26b893563471fb70b252f835403369d51124b95fcd8ddb10cd56`；同旧首轮和回测均使用P26-C4 `8c76e66f41e10975ebaa97ec94f5d21573bbe797a63cccb6445bf454b1a6aa76`。默认根程序仍为425165…；本批没有替换默认程序或重启服务。

固定4096行×4列(id integer PRIMARY KEY,k text COLLATE C,n bigint,pad text)，人口SHA256 `88c3e35bd89065248a49626a921c9be63ccc9ce089649fc7e1bbf1809717101b`。七条SELECT依次为 k<='key0007'（323行/每批4次）、k>='needle'（8/4）、k='needle4096'（1/6）、k>='absent' AND k<='absenu'（0/6）、k>=''（3855/2）、id=2048（1/8）、n=10（1/4）；另每上下文完整持久REINDEX一次。相同旧/新/旧顺序重复三轮，9个独立正常进程，共306次计时SELECT和9次REINDEX。双方全部27份GiST镜像逐字相同（V3、3855非NULL键含298空串），没有不同写入工作量的解释。

原计划在计时前冻结，既定超时保持：设置、首轮人口、七类功能预检、REINDEX及其后首轮人口检查120秒，随后EXPLAIN、预检、计时查询、计数及末轮人口检查15秒。wall从SimpleQuery发送至完整Ready接收，包含消息捕获；解码、hex转换、oracle和镜像落盘在时钟外。服务器CPU为真实进程所有线程；客户端CPU单列。自动分析/清理关闭，不清OS缓存、不绑CPU；编译和压缩在窗口外。EXPLAIN和正常SELECT计数属于不同实际执行；全部63份路径/计数记录保留。外层采样程序实际wait0，九backend正常0；独立归约实际0、0重采样，核验792回复/1,069,848单元/44,838真实NULL和459镜像。

固定 before-C4-first / C2 / same-C4-return 顺序重复三轮；每标签 n=3。所有 ms 是固定 SQL 批次总计，calls 给每样本调用次数；不把批次当单查询，不剔除慢值。全部正常服务器退出0，outer实际wait0。

完整独立核验：792回复，306次计时SELECT+9次普通持久REINDEX，1,069,848cells、44,838真实SQL NULL，459镜像、27完整GiST bytes严格相同，无RID/order差异。

完整批次 wall（ms）

| Region | calls | Label | raw3 ms（采集顺序） | min ms | p50 ms | max ms |
|---|---:|---|---|---:|---:|---:|
| reindex | 1 | before-first | 549.702812, 598.020210, 2565.257224 | 549.702812 | 598.020210 | 2565.257224 |
| reindex | 1 | after | 596.090146, 533.655730, 463.136100 | 463.136100 | 533.655730 | 596.090146 |
| reindex | 1 | before-return | 483.440619, 542.953440, 600.877239 | 483.440619 | 542.953440 | 600.877239 |
| low_empty_including | 4 | before-first | 604.586068, 836.029456, 2450.847854 | 604.586068 | 836.029456 | 2450.847854 |
| low_empty_including | 4 | after | 986.602629, 581.627307, 526.480633 | 526.480633 | 581.627307 | 986.602629 |
| low_empty_including | 4 | before-return | 2405.317145, 1136.914775, 653.611245 | 653.611245 | 1136.914775 | 2405.317145 |
| high_inclusive | 4 | before-first | 71.535774, 84.889679, 783.606557 | 71.535774 | 84.889679 | 783.606557 |
| high_inclusive | 4 | after | 77.388868, 56.522808, 54.306317 | 54.306317 | 56.522808 | 77.388868 |
| high_inclusive | 4 | before-return | 1726.625271, 86.525854, 473.044081 | 86.525854 | 473.044081 | 1726.625271 |
| equal_nonempty | 6 | before-first | 99.425328, 103.008455, 3116.165554 | 99.425328 | 103.008455 | 3116.165554 |
| equal_nonempty | 6 | after | 99.473518, 2684.219278, 57.037281 | 57.037281 | 99.473518 | 2684.219278 |
| equal_nonempty | 6 | before-return | 2136.562646, 102.961643, 1977.108753 | 102.961643 | 1977.108753 | 2136.562646 |
| absent_closed | 6 | before-first | 103.769836, 116.184066, 1649.995168 | 103.769836 | 116.184066 | 1649.995168 |
| absent_closed | 6 | after | 100.920483, 2371.279097, 59.238959 | 59.238959 | 100.920483 | 2371.279097 |
| absent_closed | 6 | before-return | 1508.795291, 103.419496, 4023.349784 | 103.419496 | 1508.795291 | 4023.349784 |
| dense_nonNULL | 2 | before-first | 936.857977, 948.878849, 1419.441302 | 936.857977 | 948.878849 | 1419.441302 |
| dense_nonNULL | 2 | after | 738.945008, 1391.661802, 637.863016 | 637.863016 | 738.945008 | 1391.661802 |
| dense_nonNULL | 2 | before-return | 1836.502885, 993.567991, 1431.420264 | 993.567991 | 1431.420264 | 1836.502885 |
| primary_control | 8 | before-first | 87.728744, 400.913392, 419.015663 | 87.728744 | 400.913392 | 419.015663 |
| primary_control | 8 | after | 115.131962, 4222.946867, 66.806631 | 66.806631 | 115.131962 | 4222.946867 |
| primary_control | 8 | before-return | 133.693621, 80.985952, 5945.749154 | 80.985952 | 133.693621 | 5945.749154 |
| noindex_control | 4 | before-first | 536.212576, 526.744067, 552.936187 | 526.744067 | 536.212576 | 552.936187 |
| noindex_control | 4 | after | 553.801172, 2355.924344, 498.110105 | 498.110105 | 553.801172 | 2355.924344 |
| noindex_control | 4 | before-return | 509.286868, 524.471342, 544.413019 | 509.286868 | 524.471342 | 544.413019 |

| Region | before-first p50 ms | C2 p50 ms | before-return p50 ms | C2 vs first % | C2 vs return % | baseline drift % |
|---|---:|---:|---:|---:|---:|---:|
| reindex | 598.020210 | 533.655730 | 542.953440 | -10.762927 | -1.712432 | -9.208179 |
| low_empty_including | 836.029456 | 581.627307 | 1136.914775 | -30.429807 | -48.841609 | +35.989799 |
| high_inclusive | 84.889679 | 56.522808 | 473.044081 | -33.416160 | -88.051260 | +457.245694 |
| equal_nonempty | 103.008455 | 99.473518 | 1977.108753 | -3.431696 | -94.968738 | +1819.365505 |
| absent_closed | 116.184066 | 100.920483 | 1508.795291 | -13.137415 | -93.311188 | +1198.624969 |
| dense_nonNULL | 948.878849 | 738.945008 | 1431.420264 | -22.124409 | -48.376796 | +50.853849 |
| primary_control | 400.913392 | 115.131962 | 133.693621 | -71.282585 | -13.883728 | -66.652743 |
| noindex_control | 536.212576 | 553.801172 | 524.471342 | +3.280154 | +5.592266 | -2.189660 |

整个服务器进程 CPU（ms）

| Region | calls | Label | raw3 ms（采集顺序） | min ms | p50 ms | max ms |
|---|---:|---|---|---:|---:|---:|
| reindex | 1 | before-first | 530.212543, 536.144397, 534.456250 | 530.212543 | 534.456250 | 536.144397 |
| reindex | 1 | after | 530.769813, 519.517234, 450.588600 | 450.588600 | 519.517234 | 530.769813 |
| reindex | 1 | before-return | 468.837507, 530.485957, 538.951482 | 468.837507 | 530.485957 | 538.951482 |
| low_empty_including | 4 | before-first | 541.487147, 642.569347, 706.483374 | 541.487147 | 642.569347 | 706.483374 |
| low_empty_including | 4 | after | 630.819610, 531.877691, 502.837222 | 502.837222 | 531.877691 | 630.819610 |
| low_empty_including | 4 | before-return | 730.770026, 689.474903, 595.226640 | 595.226640 | 689.474903 | 730.770026 |
| high_inclusive | 4 | before-first | 49.581284, 58.096132, 79.291695 | 49.581284 | 58.096132 | 79.291695 |
| high_inclusive | 4 | after | 50.728958, 35.201087, 30.264738 | 30.264738 | 35.201087 | 50.728958 |
| high_inclusive | 4 | before-return | 78.023034, 66.314147, 56.092137 | 56.092137 | 66.314147 | 78.023034 |
| equal_nonempty | 6 | before-first | 63.779001, 63.883277, 93.607981 | 63.779001 | 63.883277 | 93.607981 |
| equal_nonempty | 6 | after | 60.343972, 70.497488, 30.219074 | 30.219074 | 60.343972 | 70.497488 |
| equal_nonempty | 6 | before-return | 86.418758, 73.739070, 87.233732 | 73.739070 | 86.418758 | 87.233732 |
| absent_closed | 6 | before-first | 68.155608, 76.452870, 87.672806 | 68.155608 | 76.452870 | 87.672806 |
| absent_closed | 6 | after | 63.640477, 69.352297, 28.491257 | 28.491257 | 63.640477 | 69.352297 |
| absent_closed | 6 | before-return | 85.049011, 74.423556, 83.592116 | 74.423556 | 83.592116 | 85.049011 |
| dense_nonNULL | 2 | before-first | 650.401029, 664.447143, 748.393746 | 650.401029 | 664.447143 | 748.393746 |
| dense_nonNULL | 2 | after | 661.086160, 714.168803, 606.175795 | 606.175795 | 661.086160 | 714.168803 |
| dense_nonNULL | 2 | before-return | 725.159653, 714.551749, 745.226318 | 714.551749 | 725.159653 | 745.226318 |
| primary_control | 8 | before-first | 40.622111, 70.822039, 66.021721 | 40.622111 | 66.021721 | 70.822039 |
| primary_control | 8 | after | 68.559028, 87.147482, 30.523235 | 30.523235 | 68.559028 | 87.147482 |
| primary_control | 8 | before-return | 68.955355, 40.733564, 94.747951 | 40.733564 | 68.955355 | 94.747951 |
| noindex_control | 4 | before-first | 497.261626, 499.482308, 513.893466 | 497.261626 | 499.482308 | 513.893466 |
| noindex_control | 4 | after | 518.261713, 682.414730, 465.919906 | 465.919906 | 518.261713 | 682.414730 |
| noindex_control | 4 | before-return | 468.123937, 494.768250, 512.496935 | 468.123937 | 494.768250 | 512.496935 |

| Region | before-first p50 ms | C2 p50 ms | before-return p50 ms | C2 vs first % | C2 vs return % | baseline drift % |
|---|---:|---:|---:|---:|---:|---:|
| reindex | 534.456250 | 519.517234 | 530.485957 | -2.795180 | -2.067675 | -0.742866 |
| low_empty_including | 642.569347 | 531.877691 | 689.474903 | -17.226414 | -22.857570 | +7.299688 |
| high_inclusive | 58.096132 | 35.201087 | 66.314147 | -39.408897 | -46.917681 | +14.145546 |
| equal_nonempty | 63.883277 | 60.343972 | 86.418758 | -5.540268 | -30.172600 | +35.276025 |
| absent_closed | 76.452870 | 63.640477 | 83.592116 | -16.758551 | -23.867848 | +9.338101 |
| dense_nonNULL | 664.447143 | 661.086160 | 725.159653 | -0.505832 | -8.835777 | +9.137297 |
| primary_control | 66.021721 | 68.559028 | 68.955355 | +3.843140 | -0.574759 | +4.443438 |
| noindex_control | 499.482308 | 518.261713 | 494.768250 | +3.759774 | +4.748377 | -0.943789 |

客户端进程 CPU（ms）

| Region | calls | Label | raw3 ms（采集顺序） | min ms | p50 ms | max ms |
|---|---:|---|---|---:|---:|---:|
| reindex | 1 | before-first | 0.216649, 0.226631, 0.171143 | 0.171143 | 0.216649 | 0.226631 |
| reindex | 1 | after | 0.208246, 0.199102, 0.276337 | 0.199102 | 0.208246 | 0.276337 |
| reindex | 1 | before-return | 0.207716, 0.306365, 0.194027 | 0.194027 | 0.207716 | 0.306365 |
| low_empty_including | 4 | before-first | 26.500350, 27.361206, 29.181000 | 26.500350 | 27.361206 | 29.181000 |
| low_empty_including | 4 | after | 23.928323, 28.584748, 21.436194 | 21.436194 | 23.928323 | 28.584748 |
| low_empty_including | 4 | before-return | 30.011930, 30.717946, 23.391647 | 23.391647 | 30.011930 | 30.717946 |
| high_inclusive | 4 | before-first | 1.579712, 1.337296, 1.656765 | 1.337296 | 1.579712 | 1.656765 |
| high_inclusive | 4 | after | 1.504072, 1.692139, 1.137450 | 1.137450 | 1.504072 | 1.692139 |
| high_inclusive | 4 | before-return | 1.794714, 1.541395, 1.665534 | 1.541395 | 1.665534 | 1.794714 |
| equal_nonempty | 6 | before-first | 1.335635, 1.385791, 1.298282 | 1.298282 | 1.335635 | 1.385791 |
| equal_nonempty | 6 | after | 1.294419, 1.505037, 1.005747 | 1.005747 | 1.294419 | 1.505037 |
| equal_nonempty | 6 | before-return | 1.591512, 1.344091, 1.591819 | 1.344091 | 1.591512 | 1.591819 |
| absent_closed | 6 | before-first | 1.238071, 1.186782, 1.234489 | 1.186782 | 1.234489 | 1.238071 |
| absent_closed | 6 | after | 1.216747, 1.465659, 0.800001 | 0.800001 | 1.216747 | 1.465659 |
| absent_closed | 6 | before-return | 1.403052, 1.333246, 1.592250 | 1.333246 | 1.403052 | 1.592250 |
| dense_nonNULL | 2 | before-first | 91.408465, 89.470922, 84.733578 | 84.733578 | 89.470922 | 91.408465 |
| dense_nonNULL | 2 | after | 85.950005, 89.520331, 100.714444 | 85.950005 | 89.520331 | 100.714444 |
| dense_nonNULL | 2 | before-return | 116.168327, 94.792795, 96.748349 | 94.792795 | 96.748349 | 116.168327 |
| primary_control | 8 | before-first | 1.443932, 1.978006, 1.871092 | 1.443932 | 1.871092 | 1.978006 |
| primary_control | 8 | after | 1.790812, 2.025896, 1.236546 | 1.236546 | 1.790812 | 2.025896 |
| primary_control | 8 | before-return | 1.881773, 1.696421, 2.423169 | 1.696421 | 1.881773 | 2.423169 |
| noindex_control | 4 | before-first | 0.921487, 0.996306, 0.968717 | 0.921487 | 0.968717 | 0.996306 |
| noindex_control | 4 | after | 0.986261, 1.002248, 0.850736 | 0.850736 | 0.986261 | 1.002248 |
| noindex_control | 4 | before-return | 0.831411, 0.944378, 0.880228 | 0.831411 | 0.880228 | 0.944378 |

| Region | before-first p50 ms | C2 p50 ms | before-return p50 ms | C2 vs first % | C2 vs return % | baseline drift % |
|---|---:|---:|---:|---:|---:|---:|
| reindex | 0.216649 | 0.208246 | 0.207716 | -3.878624 | +0.255156 | -4.123259 |
| low_empty_including | 27.361206 | 23.928323 | 30.011930 | -12.546534 | -20.270629 | +9.687892 |
| high_inclusive | 1.579712 | 1.504072 | 1.665534 | -4.788215 | -9.694308 | +5.432762 |
| equal_nonempty | 1.335635 | 1.294419 | 1.591512 | -3.085873 | -18.667343 | +19.157704 |
| absent_closed | 1.234489 | 1.216747 | 1.403052 | -1.437194 | -13.278553 | +13.654476 |
| dense_nonNULL | 89.470922 | 89.520331 | 96.748349 | +0.055224 | -7.470947 | +8.133846 |
| primary_control | 1.871092 | 1.790812 | 1.881773 | -4.290543 | -4.833792 | +0.570843 |
| noindex_control | 0.968717 | 0.986261 | 0.880228 | +1.811055 | +12.046084 | -9.134660 |

方法与负面边界

- 全8regions wall为7低于两旧/1高于两旧(noindex)；serverCPU为6低/1高(noindex)/1居间(PK)。7条query分别wall6/1/0、serverCPU5/1/1。
- high CPU −39.408897%/−46.917681%，absent CPU −16.758551%/−23.867848%。这是本轮完整normalSQL批次描述，不用instrumented native速度替代。
- noindex wall +3.280154%/+5.592266%、CPU +3.759774%/+4.748377%；PK CPU +3.843140%/−0.574759%保留。
- wall旧回测漂移 −66.652743%..+1819.365505%，CPU −0.943789%..+35.276025%；不能用不稳定wall宣稳定倍率，不归因host或某个代码机制。
- 最长wall旧PK8次批5945.749154ms，C2 PK8次批4222.946867ms，全部保留。每标签只有3个独立上下文，未作显著性/因果结论。
- 两端为相同V3写入3855非NULL键(298真实空串)，完整27文件严格byteequal；没有沿用上轮V2/新增空键工作量解释。
- EXPLAIN与normal SELECT counters是不同实际执行，actual usedIndex/source/tuplecount全部63记录保留；不能强制两执行同路或凭计数省略堆/残余谓词。


当前为2386个测试文件：原2383份逐字保持，新增两份CPP和一份协议测试；顶层CPP1121（含链接桩）、全部CPP1126/子目录5，登记682个driver/662个独立E2E。源248/头177，754份Markdown路径和725份保护记录保持；26份旧gzip逐字保持，新增第27份。原273项及22complete/166partial/70unverified/15用户暂缓状态、277条历史保持，仅增加第278条partial。

全部首次结果保留：旧版及C1真实SQL零字节索引漏行、同源before reader四断言失败和新协议十二断言失败；最初离线归约参数名冲突、v4计划生成KeyError、C2性能prepare标签guard失败均独立留存，均不记产品验收通过。C1性能计划只有准备，0计时；最终仅C2固定9实际运行一次。

原53.45MB未压缩JSON在当前Git清单中已由约5.7MiB gzip替代，原始字节可精确恢复，历史提交未改写。新证据同样只提交[gzip归档](benchmarks/gist-probe-reader-20261106.json.gz)，文件镜像和捕获流有长度/SHA及独立还原核验；构建对象、ELF和私有数据库目录不进入归档。

官方设计参考固定版本的[PG18.6 gistScanPage](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/gist/gistget.c)、[SQLite3.50.4 payload读取](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/btree.c)和[DuckDB1.4.3 ART查找](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/index/art/art.cpp)；10份固定源码/3许可证保留。短寿命借用和完整边界验证可借鉴，PG页锁/NSN/recheck、SQLite溢出页协议、ART真正树导航没有因本次缓冲而实现。本地仍每probe完整校验flat sidecar，完整分页GiST/opclass/KNN/WAL/并发和既有普通缺口保持开放，原用户暂缓范围不扩大。

下方P26及更早完整表格、真实慢项、失败与当时默认决定均作为历史保留。

<!-- CURRENT_GIST_EMPTY_BOUNDS_20261106 -->

## GiST 空字符串、边界与 LIKE 检索（2026-11-06，P26-C4）

<a id="gist-empty-bounds-20261106"></a>

当前物理heap格式的存储列构建/REINDEX会写GiST V3，覆盖真实非NULL空字符串；NULL仍由真实位图区分。V2编码本来允许零长度，缺陷在旧producer省略空键，V3标记新的覆盖保证。旧V2/文本文件继续严格读取；无法证明覆盖、CRC、文件归属或可见性时交回原堆来源。有限空端点、缺失端点、严格/非严格边界和字面DEL分别表示；最终完整谓词、MVCC、NULL及TOAST仍检查。

LIKE必要字节前缀使用单独的tryGiSTSearchLikePrefix，保留原pattern/escape角色；默认locale下不把字节前缀的权限推广到SQL标量比较。无有效Unicode上界时保留无上界，避免DEL哨兵截断。普通EXPLAIN的>=空串不再独立触发全宽索引遍历；JSON usedIndex仅在真实索引候选读取成功时为true。

固定九进程、七类相同SQL的wall中位数有4项低于两次旧版、3项高于两次旧版；服务器CPU为5低/2高。非空等值wall减少84.385%/83.171%，密集非NULL查询减少80.038%/77.317%；高端范围wall增加26.674%/50.555%，不存在范围增加62.435%/60.745%，低端含空范围增加0.617%/3.536%。REINDEX wall增加6.065%/6.366%，其V3多写298个真实非NULL空键，不能当等字节重建速度比较。完整首轮/同旧回测和全部三时钟如下，主键与无索引控制变化不作因果归因，未证明统一提速或PG性能等价。

### 固定负载与计时口径

每个独立后端用同一14条setup命令创建4096行×4列：id integer主键、k text COLLATE C、n bigint、pad text（OID23/25/20/25）。8次512行INSERT，另建GiST(k)并ANALYZE；每次查询完整投影id,k,n,pad，含真实NULL、空串、字面NULL、DEL、UTF8/换行和512字节填充。原始人口SHA256为`88c3e35bd89065248a49626a921c9be63ccc9ce089649fc7e1bbf1809717101b`，不是六列负载。

| 区域 | 固定WHERE条件 | 每批调用 | 每次完整返回行数 |
|---|---|---:|---:|
| low_empty_including | `k<='key0007'` | 4 | 323 |
| high_inclusive | `k>='needle'` | 4 | 8 |
| equal_nonempty | `k='needle4096'` | 6 | 1 |
| absent_closed | `k>='absent' AND k<='absenu'` | 6 | 0 |
| dense_nonNULL | `k>=''` | 2 | 3855 |
| primary_control | `id=2048` | 8 | 1 |
| noindex_control | `n=10` | 4 | 1 |

另每进程计时一次完整REINDEX TABLE gist_cost。先对完整人口和七条固定查询作语义预检，再计REINDEX及其后七类查询；全部同代/跨代完整值、NULL、四列所有描述符、CommandComplete和ReadyI均核对。计时SELECT及其后查询阶段期限15秒；setup、前置功能检查、REINDEX及其后首个人口检查沿120秒期限，0重试、0重采样。已知旧版错误的有限空串/严格DEL案例只进入功能验证，不混入等结果速度比较。

wall从普通SimpleQuery发送到完整Ready接收，包含前端、执行、投影、网络及完整消息捕获；值解码、oracle、hex转换、镜像和落盘在计时外。服务器CPU覆盖该进程全部线程，客户端CPU另列；自动ANALYZE/VACUUM关闭。预检和EXPLAIN已触缓存，没有清OS缓存或绑CPU；记录host load但不作因果归因。EXPLAIN与正常SELECT是分别执行的证据，不能将旧版缺失usedIndex字段填成物理索引证明。

百分比=(C4/对应旧版−1)×100%，正数表示变慢/CPU增加，负数表示减少；baseline drift=(旧回测/旧首次−1)×100%。每个标签仅3个独立上下文，表中每格为完整固定批次，不是单条延迟。

固定九上下文：before-first / C4 / before-return，完整顺序重复三轮；各标签 n=3。下列毫秒均为该固定 SQL 批次的总计，calls 列给每个样本的调用次数；不将不同查询权重当单条延迟。负百分比表示本轮 C4 中位数低于对应基线，未删任何慢值或尾部。

完整核验 792 个回复、306 次计时 SELECT、9 次普通持久 REINDEX、1,069,848 个投影单元格（44,838 个真实 SQL NULL），9 个服务器均正常 0 退出。

客户端完整批次 wall（ms）

| Region | calls | 标签 | raw3 ms（采集顺序） | min ms | p50 ms | max ms |
|---|---:|---|---|---:|---:|---:|
| reindex | 1 | before-first | 544.638515, 521.525411, 525.152945 | 521.525411 | 525.152945 | 544.638515 |
| reindex | 1 | after | 557.001728, 592.900512, 542.101307 | 542.101307 | 557.001728 | 592.900512 |
| reindex | 1 | before-return | 541.072934, 523.665125, 461.129414 | 461.129414 | 523.665125 | 541.072934 |
| low_empty_including | 4 | before-first | 724.084937, 695.486343, 647.523662 | 647.523662 | 695.486343 | 724.084937 |
| low_empty_including | 4 | after | 842.204651, 699.774277, 599.563168 | 599.563168 | 699.774277 | 842.204651 |
| low_empty_including | 4 | before-return | 702.902806, 675.873347, 649.702300 | 649.702300 | 675.873347 | 702.902806 |
| high_inclusive | 4 | before-first | 80.253015, 83.008422, 64.546988 | 64.546988 | 80.253015 | 83.008422 |
| high_inclusive | 4 | after | 103.553118, 83.921744, 101.659612 | 83.921744 | 101.659612 | 103.553118 |
| high_inclusive | 4 | before-return | 67.523365, 72.508566, 55.408853 | 55.408853 | 67.523365 | 72.508566 |
| equal_nonempty | 6 | before-first | 787.455725, 964.836004, 733.564704 | 733.564704 | 787.455725 | 964.836004 |
| equal_nonempty | 6 | after | 119.791893, 122.964416, 293.216266 | 119.791893 | 122.964416 | 293.216266 |
| equal_nonempty | 6 | before-return | 750.371326, 729.270494, 730.669579 | 729.270494 | 730.669579 | 750.371326 |
| absent_closed | 6 | before-first | 81.316940, 69.699822, 67.197852 | 67.197852 | 69.699822 | 81.316940 |
| absent_closed | 6 | after | 113.216805, 135.956871, 101.223420 | 101.223420 | 113.216805 | 135.956871 |
| absent_closed | 6 | before-return | 63.084467, 107.795303, 70.432732 | 63.084467 | 70.432732 | 107.795303 |
| dense_nonNULL | 2 | before-first | 3497.010550, 3705.561087, 3401.625675 | 3401.625675 | 3497.010550 | 3705.561087 |
| dense_nonNULL | 2 | after | 938.469503, 698.077551, 660.499479 | 660.499479 | 698.077551 | 938.469503 |
| dense_nonNULL | 2 | before-return | 4053.055191, 3057.716883, 3077.494678 | 3057.716883 | 3077.494678 | 4053.055191 |
| primary_control | 8 | before-first | 89.664547, 1569.026344, 108.120136 | 89.664547 | 108.120136 | 1569.026344 |
| primary_control | 8 | after | 134.400904, 80.167193, 97.741814 | 80.167193 | 97.741814 | 134.400904 |
| primary_control | 8 | before-return | 3074.993519, 150.115052, 120.924520 | 120.924520 | 150.115052 | 3074.993519 |
| noindex_control | 4 | before-first | 507.276825, 614.003438, 502.960931 | 502.960931 | 507.276825 | 614.003438 |
| noindex_control | 4 | after | 572.067272, 484.160207, 490.965423 | 484.160207 | 490.965423 | 572.067272 |
| noindex_control | 4 | before-return | 732.254221, 495.210416, 501.422081 | 495.210416 | 501.422081 | 732.254221 |

| Region | before-first p50 ms | C4 p50 ms | before-return p50 ms | C4 vs first % | C4 vs return % | baseline drift % |
|---|---:|---:|---:|---:|---:|---:|
| reindex | 525.152945 | 557.001728 | 523.665125 | +6.064668 | +6.366015 | -0.283312 |
| low_empty_including | 695.486343 | 699.774277 | 675.873347 | +0.616537 | +3.536303 | -2.820040 |
| high_inclusive | 80.253015 | 101.659612 | 67.523365 | +26.673885 | +50.554718 | -15.861896 |
| equal_nonempty | 787.455725 | 122.964416 | 730.669579 | -84.384593 | -83.170996 | -7.211345 |
| absent_closed | 69.699822 | 113.216805 | 70.432732 | +62.434855 | +60.744588 | +1.051523 |
| dense_nonNULL | 3497.010550 | 698.077551 | 3077.494678 | -80.037877 | -77.316693 | -11.996414 |
| primary_control | 108.120136 | 97.741814 | 150.115052 | -9.598880 | -34.888732 | +38.840976 |
| noindex_control | 507.276825 | 490.965423 | 501.422081 | -3.215483 | -2.085400 | -1.154152 |

整个服务器进程 CPU（ms）

| Region | calls | 标签 | raw3 ms（采集顺序） | min ms | p50 ms | max ms |
|---|---:|---|---|---:|---:|---:|
| reindex | 1 | before-first | 513.011741, 511.585919, 515.808576 | 511.585919 | 513.011741 | 515.808576 |
| reindex | 1 | after | 541.338870, 540.638470, 519.471458 | 519.471458 | 540.638470 | 541.338870 |
| reindex | 1 | before-return | 513.569599, 510.330515, 450.315189 | 450.315189 | 510.330515 | 513.569599 |
| low_empty_including | 4 | before-first | 667.809403, 652.238034, 599.747544 | 599.747544 | 652.238034 | 667.809403 |
| low_empty_including | 4 | after | 582.411628, 603.686172, 538.448202 | 538.448202 | 582.411628 | 603.686172 |
| low_empty_including | 4 | before-return | 660.221627, 623.235403, 608.194363 | 608.194363 | 623.235403 | 660.221627 |
| high_inclusive | 4 | before-first | 52.311859, 58.428601, 43.374473 | 43.374473 | 52.311859 | 58.428601 |
| high_inclusive | 4 | after | 78.954139, 41.872075, 66.793441 | 41.872075 | 66.793441 | 78.954139 |
| high_inclusive | 4 | before-return | 44.667834, 44.454198, 32.082196 | 32.082196 | 44.454198 | 44.667834 |
| equal_nonempty | 6 | before-first | 702.256683, 819.529686, 697.529735 | 697.529735 | 702.256683 | 819.529686 |
| equal_nonempty | 6 | after | 78.360240, 79.025141, 75.269189 | 75.269189 | 78.360240 | 79.025141 |
| equal_nonempty | 6 | before-return | 697.452471, 665.140026, 681.478549 | 665.140026 | 681.478549 | 697.452471 |
| absent_closed | 6 | before-first | 30.482011, 34.103794, 31.763279 | 30.482011 | 31.763279 | 34.103794 |
| absent_closed | 6 | after | 79.503468, 67.903218, 46.570091 | 46.570091 | 67.903218 | 79.503468 |
| absent_closed | 6 | before-return | 30.969053, 67.829711, 34.382461 | 30.969053 | 34.382461 | 67.829711 |
| dense_nonNULL | 2 | before-first | 2976.656854, 3057.351138, 2921.405669 | 2921.405669 | 2976.656854 | 3057.351138 |
| dense_nonNULL | 2 | after | 689.937884, 664.934067, 614.729333 | 614.729333 | 664.934067 | 689.937884 |
| dense_nonNULL | 2 | before-return | 3151.625597, 2980.125569, 2994.285646 | 2980.125569 | 2994.285646 | 3151.625597 |
| primary_control | 8 | before-first | 34.419026, 77.486410, 43.723323 | 34.419026 | 43.723323 | 77.486410 |
| primary_control | 8 | after | 69.877422, 30.128866, 40.057500 | 30.128866 | 40.057500 | 69.877422 |
| primary_control | 8 | before-return | 80.749263, 70.463319, 68.585784 | 68.585784 | 70.463319 | 80.749263 |
| noindex_control | 4 | before-first | 471.719392, 535.754480, 467.847508 | 467.847508 | 471.719392 | 535.754480 |
| noindex_control | 4 | after | 519.864599, 448.621113, 458.684843 | 448.621113 | 458.684843 | 519.864599 |
| noindex_control | 4 | before-return | 566.305464, 462.105589, 459.397258 | 459.397258 | 462.105589 | 566.305464 |

| Region | before-first p50 ms | C4 p50 ms | before-return p50 ms | C4 vs first % | C4 vs return % | baseline drift % |
|---|---:|---:|---:|---:|---:|---:|
| reindex | 513.011741 | 540.638470 | 510.330515 | +5.385204 | +5.938887 | -0.522644 |
| low_empty_including | 652.238034 | 582.411628 | 623.235403 | -10.705663 | -6.550298 | -4.446633 |
| high_inclusive | 52.311859 | 66.793441 | 44.454198 | +27.683172 | +50.252269 | -15.020802 |
| equal_nonempty | 702.256683 | 78.360240 | 681.478549 | -88.841653 | -88.501437 | -2.958766 |
| absent_closed | 31.763279 | 67.903218 | 34.382461 | +113.778993 | +97.493769 | +8.245943 |
| dense_nonNULL | 2976.656854 | 664.934067 | 2994.285646 | -77.661716 | -77.793232 | +0.592235 |
| primary_control | 43.723323 | 40.057500 | 70.463319 | -8.384136 | -43.151273 | +61.157282 |
| noindex_control | 471.719392 | 458.684843 | 462.105589 | -2.763200 | -0.740252 | -2.038034 |

客户端进程 CPU（ms）

| Region | calls | 标签 | raw3 ms（采集顺序） | min ms | p50 ms | max ms |
|---|---:|---|---|---:|---:|---:|
| reindex | 1 | before-first | 0.224645, 0.191009, 0.199863 | 0.191009 | 0.199863 | 0.224645 |
| reindex | 1 | after | 0.229402, 0.216599, 0.247779 | 0.216599 | 0.229402 | 0.247779 |
| reindex | 1 | before-return | 0.178569, 0.211150, 0.201882 | 0.178569 | 0.201882 | 0.211150 |
| low_empty_including | 4 | before-first | 26.495894, 26.479945, 27.026576 | 26.479945 | 26.495894 | 27.026576 |
| low_empty_including | 4 | after | 27.823609, 21.577895, 26.212389 | 21.577895 | 26.212389 | 27.823609 |
| low_empty_including | 4 | before-return | 29.418924, 26.663725, 26.522411 | 26.522411 | 26.663725 | 29.418924 |
| high_inclusive | 4 | before-first | 1.208899, 1.452215, 1.394468 | 1.208899 | 1.394468 | 1.452215 |
| high_inclusive | 4 | after | 1.446627, 1.441250, 1.912761 | 1.441250 | 1.446627 | 1.912761 |
| high_inclusive | 4 | before-return | 1.481320, 1.444598, 1.419946 | 1.419946 | 1.444598 | 1.481320 |
| equal_nonempty | 6 | before-first | 1.079897, 1.581605, 1.341152 | 1.079897 | 1.341152 | 1.581605 |
| equal_nonempty | 6 | after | 1.393397, 1.340721, 1.117139 | 1.117139 | 1.340721 | 1.393397 |
| equal_nonempty | 6 | before-return | 1.339628, 1.251137, 1.459191 | 1.251137 | 1.339628 | 1.459191 |
| absent_closed | 6 | before-first | 1.143266, 1.070402, 1.087380 | 1.070402 | 1.087380 | 1.143266 |
| absent_closed | 6 | after | 1.047449, 1.212253, 1.093258 | 1.047449 | 1.093258 | 1.212253 |
| absent_closed | 6 | before-return | 1.124525, 1.047940, 1.168552 | 1.047940 | 1.124525 | 1.168552 |
| dense_nonNULL | 2 | before-first | 109.741446, 75.047958, 87.505874 | 75.047958 | 87.505874 | 109.741446 |
| dense_nonNULL | 2 | after | 76.339429, 85.691687, 68.333900 | 68.333900 | 76.339429 | 85.691687 |
| dense_nonNULL | 2 | before-return | 95.131623, 76.411492, 91.345651 | 76.411492 | 91.345651 | 95.131623 |
| primary_control | 8 | before-first | 1.605183, 2.096575, 1.536964 | 1.536964 | 1.605183 | 2.096575 |
| primary_control | 8 | after | 1.915087, 1.352990, 1.329274 | 1.329274 | 1.352990 | 1.915087 |
| primary_control | 8 | before-return | 2.106297, 1.984701, 1.854851 | 1.854851 | 1.984701 | 2.106297 |
| noindex_control | 4 | before-first | 0.888915, 0.917452, 0.996463 | 0.888915 | 0.917452 | 0.996463 |
| noindex_control | 4 | after | 0.883176, 0.918488, 1.002051 | 0.883176 | 0.918488 | 1.002051 |
| noindex_control | 4 | before-return | 1.038198, 0.967971, 0.949007 | 0.949007 | 0.967971 | 1.038198 |

| Region | before-first p50 ms | C4 p50 ms | before-return p50 ms | C4 vs first % | C4 vs return % | baseline drift % |
|---|---:|---:|---:|---:|---:|---:|
| reindex | 0.199863 | 0.229402 | 0.201882 | +14.779624 | +13.631725 | +1.010192 |
| low_empty_including | 26.495894 | 26.212389 | 26.663725 | -1.069996 | -1.692697 | +0.633423 |
| high_inclusive | 1.394468 | 1.446627 | 1.444598 | +3.740423 | +0.140454 | +3.594919 |
| equal_nonempty | 1.341152 | 1.340721 | 1.339628 | -0.032137 | +0.081590 | -0.113634 |
| absent_closed | 1.087380 | 1.093258 | 1.124525 | +0.540565 | -2.780463 | +3.416009 |
| dense_nonNULL | 87.505874 | 76.339429 | 91.345651 | -12.760795 | -16.427955 | +4.388022 |
| primary_control | 1.605183 | 1.352990 | 1.984701 | -15.711168 | -31.829026 | +23.643286 |
| noindex_control | 0.917452 | 0.918488 | 0.967971 | +0.112921 | -5.112033 | +5.506446 |

实际物理路径与限制

- equal_nonempty：两旧标签 TableScan/4096，C4 GiSTScan（actual usedIndex=true）/1；dense_nonNULL：旧 reported GiST/3855，C4 TableScan/4096。旧程序没有 EXPLAIN usedIndex 字段，旧 idx 计数不证明其内部没有堆回退。
- low/high/absent 的 C4 实际 GiST usedIndex=true，正常 SELECT counter tuple 数分别为 323/8/0；PK 两代 IndexScan/1，无索引控制两代 TableScan/4096。
- 旧 V2 每张索引 3557 个非空键，C4 V3 为 3855 个非 NULL 键，额外 298 个真实空串键；REINDEX 输出及实际工作量不同，不作为等字节构建速度结论。
- wall 的 8 区域为 4 低于两旧 / 4 高于两旧；7 查询为 4 / 3。服务器 CPU 为 5 / 3（7 查询为 5 / 2）。high、absent、REINDEX 的负面结果完整保留。
- 全局最长 wall 批次是 before-return dense_nonNULL 4053.055191 ms；C4 最长 wall 批次是 dense_nonNULL 938.469503 ms。它们各包含 2 次 SELECT，不冒充单条查询样本。
- 三上下文/标签只作描述性观测；没有 PG 速度、稳定倍率、因果或统计显著性结论。全部 306 个计时回复与 9 个 REINDEX 回复保留，无重采样。


### 正确性、参考数据库与首轮失败

最终C4普通CMake C++17/O2构建退出0。格式组件O2与ASan/UBSan（含泄漏检测）各8434检查；现有API生命周期2879检查及独立进程冷启183检查通过；typed接口806检查/9组/68探测全部目标堆fetch为0，并有4次真实堆fetch正控制；文件归属冲突四个语义探针通过。17个有限native入口（其中2个新、15个旧）和6个旧协议入口全部正常0；迁移后的all-AM native实际12168检查/17场景，恢复实际3579检查/18workers（5次预期SIGKILL、13次正常0，36份完整输出及真实marker/event保留）。新协议120条命令、15个索引图像记录及六阶段真实heap RID/NULL见证通过，含正常双进程冷启。

真实PG18.6安装并加载btree_gist后重放75条共享SQL；最终C4逐项比较3231行/19382单元（661个NULL）、248列描述符的1736个元数据字段。75/75语义及描述符一致，73/75完整帧一致；两次DELETE本项目额外发送零字段T，PG只发CZ，严格全协议通过仍为false。两端关系OID各由真实目录锚定；PG正常退出、PID已回收，未比较PG访问方法格式、路径或速度。

首次结果全部保留：旧版现有API夹具183检查中21失败；文件名冲突v1先因设置顺序失败，v2真实执行后旧版1失败、C1四失败，C2及最终C4修正通过。C2首次协议的117个RID见证解析错误与1个真实全宽索引浪费分开记录；C3旧LIKE严格性能路径失败由C4独立LIKE角色修复。PG首次启动角色设置错误时0条共享SQL；修正initdb角色后才执行首次75条SQL。三个生命周期冷启前.runtime_stats只有原始长度/SHA清单、未保当时字节，明确标为metadata-only；所有成本459图像、实际捕获流与其余可用阶段字节完整归档，不凭冷启后统计伪造早期图像。

### 实现边界与证据

实现依据分别保留固定版本的[PostgreSQL btree_gist源码](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/contrib/btree_gist/btree_utils_var.c)、[PG text consistent](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/contrib/btree_gist/btree_text.c)、[SQLite记录格式](https://www.sqlite.org/fileformat2.html#record_format)和[DuckDB ART键实现](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/index/art/art_key.cpp)。空数据、NULL、端点存在性分别处理；PG btree_gist的text叶项可精确判断，不将PG全部GiST称为有损。23份官方源码/许可证与研究行锚随归档保存；研究不是运行验收。

本地GiST仍是flat sidecar，每次候选检索完整校验文件，并非PG的分页GiST树、opclass/KNN/WAL/concurrent全功能。虚拟键仍保留旧覆盖来源；native-only丢失定义、虚拟Bloom NEW NULL、既有missing-memory协议失败及其不完整旧版配对仍开放。下划线文件名歧义现在安全回堆，物理文件命名/关系身份布局仍需后续改造。原用户暂缓范围保持，Goal active；本批为partial。

当前清单为2383个测试文件：原2378中2374份逐字保持，4份只迁移当前GiST格式断言并加强V2兼容/空串真实RID检查，另增4个CPP及1个协议测试。顶层CPP1119（含链接桩）、全部CPP1124/其中子目录5；登记681个driver/661个独立E2E。源248/头177，754份Markdown、725份保护记录保持；25份旧gzip逐字保持，新增第26份。原273项及22complete/166partial/70unverified/15用户暂缓状态、原276条历史逐字保持，仅prepend第277条partial。

正常CMake C++17/O2最终程序SHA256为`8c76e66f41e10975ebaa97ec94f5d21573bbe797a63cccb6445bf454b1a6aa76`；父代为`99f476a899816ad759cf165817394d3bfca4b4961f56887f07c719871d031e64`。默认根程序仍为425165…，不以候选测试代替部署。所有新旧源码、原始帧、文件镜像、真实失败、计划、三种时钟与独立归约见[第26份压缩证据](benchmarks/gist-empty-bounds-20261106.json.gz)。每一原始流均保长度/SHA并完整还原，25份旧gzip逐字保持；未把压缩或链接检查当作性能/功能验收。

以下 P25 及更早段落均保留当时的实现、计数和未完事项；本轮状态以本节为准。


<!-- CURRENT_REINDEX_ACCESS_METHODS_20261106 -->

## 普通访问方法 REINDEX 与空文本生命周期（2026-11-06）

本批补入普通 `REINDEX TABLE` 的 B-tree/Hash/Bloom/FTI/GIN/GiST/SPGiST/BRIN 重建、物理 Bloom NULL/空文本维护与 UPPER/LOWER 表达式索引 SQL 桥接。有效旧头的 BRIN `pages_per_range` 与 Bloom `k` 保留；`m` 随真实新键集按既有 loader 默认128 bitsPerEntry重算。旧头损坏/缺失时回退宽64/k7。默认程序仍为 `425165…`，没有服务重启或默认升级。

当前统计：2378 测试文件（原2375字节不变加2 CPP及1协议），顶层CPP1115（含链接桩）、总CPP1120/其中子目录5；登记680个driver/660个独立E2E。源248/头177，754 Markdown、725保护记录；24份旧gzip逐字保持，归档总计25份。原273项22/166/70/15及275条历史保持，仅新增partial第276条。

C2 主 native 实际 12096 检查/17 场景通过，恢复 native 实际 3183 检查、8 控制/18 workers （5 次预期 SIGKILL、13 次正常退出）；24 条 PG18.6 共享命令比较 1343 行/8058 单元及全部字段声明。31 个既有 native 与 5 个相关协议及 auto3 通过；另一个相关协议仍失败，旧版配对尚未到相同阶段。

旧版 mixed REINDEX 只重建 B-tree，新版额外重建七种 AM；坏 BRIN 旧版仍坏而新版以宽64修复。两处成本并非等工作量速度比较。表中变化=(new/old−1)×100%，正数表示耗时/CPU增加，负数表示减少；所有 raw3/min/p50/max、正负数、长尾和漂移保留；未测 PG 速度或新的35/36全集。

七查询的 wall p50 有1项低于两次旧版、1项高于两次旧版、5项居间；服务器进程 CPU 为3低/2高/2居间。旧查询 wall 漂移为+0.983%至+93.014%，未证明一致 SQL 提速。mixed健康维护 wall 相对旧首次/回测为+21.316% / +20.669%，损坏BRIN修复维护为+23.223% / +18.842%；两处增加了实际重建工作。无对应索引查询控制 wall 为+3.755% / +2.745%，等值查询 CPU 为+12.318% / +2.471%，这些负面观察全部保留。

### 测量方法与复现边界

固定 `(P24-C2 首次, P25-C2, 同一 P24-C2 回测) × 3`，9个独立正常 socket backend、0重试；旧/新程序 SHA256 分别为 `90f076268aca232fe896a02e19cbcb42e14256686209992a073dc561443a3e58` / `99f476a899816ad759cf165817394d3bfca4b4961f56887f07c719871d031e64`。正常 CMake C++17/O2 候选构建退出0；没有用 C++23 或 profiler 改变这一对照。

每进程以8条512行 INSERT 建立4096×6人口（id、n、body、pad、k、location；OID 23/20/25/25/25/600），含真实NULL、空文本、字面NULL、UTF8和换行，不做DELETE。先测仅两棵B-tree的完整重建，再增加七种辅助AM测完整健康重建；随后对七种健康查询分批计时，最后翻转自有BRIN文件一个CRC位并测一次完整REINDEX。健康双方BRIN宽1；损坏后旧文件仍坏，新版无法从坏头恢复宽度，按原规则重建为64。修复后查询只检查正确性和EXPLAIN，不计入延迟样本。

**下表每格是一次完整批次的总毫秒数，不是单次查询延迟。** 三种REINDEX区域各调用1次；high_range/low_range/equal/absent/dense_control/primary_control/no_index_control每批分别调用4/4/6/6/2/8/4次。共27次完整REINDEX、306次健康查询计时、63次修复后不计时查询；独立归约核对963个完整回复、2,388,312个投影单元（120,006个NULL）、1,818个文件图像及真实扫描计数。

wall从普通SimpleQuery发送计到完整ReadyI接收，包含SQL前端、执行、投影及socket输出/接收；Python值解码、oracle、CRC与文件镜像核对在计时外。服务器CPU覆盖该进程全部线程，客户端CPU另列；自动ANALYZE/VACUUM已关闭。setup、EXPLAIN及预检已触缓存，没有清OS缓存或绑定CPU，记录host load但不据此作因果归因。每组三个样本，不代表稳定提速、显著性或PG速度对比。完整plan、runner、原始帧、raw3与归约回执随压缩证据保留。

本批本地专项协议实际69条命令/40个文件图像通过；PG共享24条、7次完整人口结果共1,343行/8,058单元（441个NULL），42个列描述符逐项检查7个字段。这些是6个数据列的描述元数据，并非7个数据列；两端tableOID各自真实锚定。PG保留4个实际BRIN metapage，损坏/丢失时本地宽64与PG intact宽1明确分列；事务内1个ReadyT图像没有CHECKPOINT或物理页读取。

恢复控制覆盖真实V2持久标记、首次辅助文件旧代备份/新代发布、全部rename及目录fsync后的进程中断，并保留V1兼容控制、私有文件fsync/发布rename的定点EIO与正常控制。5次预期SIGKILL不是普通退出；18个worker的36份完整stdout/stderr均保留。该控制不覆盖发布标记前崩溃的临时文件回收，也不等价完整PG事务WAL/并发恢复；原有混合reindex_atomicity回归仅原样观察一次，不扩展安全/TDE范围。

### wall（毫秒，p50 [min, max]）

| 区域 | P24-C2 首轮 | P25-C2 | P24-C2 回测 | 相对旧首次变化 | 相对旧回测变化 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|
| btree_reindex | 769.023810 [768.504893, 793.110783] | 760.477706 [723.193999, 806.642899] | 769.564103 [765.444398, 858.476909] | -1.111293% | -1.180720% | +0.070257% |
| mixed_reindex_healthy | 857.530095 [821.054197, 873.763234] | 1040.323276 [974.722293, 1148.780612] | 862.126429 [856.218512, 955.254409] | +21.316241% | +20.669456% | +0.535997% |
| mixed_reindex_corrupt | 840.697870 [825.475840, 863.766306] | 1035.936917 [1000.979036, 1222.951099] | 871.693904 [863.857001, 884.509161] | +23.223450% | +18.841822% | +3.686941% |
| high_range | 88.176845 [85.178151, 1830.225809] | 109.604496 [89.472117, 138.529863] | 127.578195 [90.422889, 161.730449] | +24.300769% | -14.088379% | +44.684463% |
| low_range | 83.205627 [68.213472, 122.975449] | 106.972817 [79.230525, 127.139280] | 120.797381 [80.519028, 128.265155] | +28.564402% | -11.444424% | +45.179341% |
| equal | 206.841541 [206.079944, 208.426262] | 236.055351 [210.428589, 351.730942] | 264.857666 [207.519158, 328.814806] | +14.123763% | -10.874639% | +28.048585% |
| absent | 73.045252 [69.067022, 80.354720] | 112.024528 [88.605872, 188.734096] | 140.987671 [89.212271, 172.848942] | +53.363189% | -20.543032% | +93.014148% |
| dense_control | 1473.450013 [953.808062, 1662.914759] | 1003.755303 [1000.174263, 1153.237886] | 1583.630956 [1462.720875, 1693.285423] | -31.877207% | -36.616843% | +7.477752% |
| primary_control | 120.191453 [75.571396, 135.857650] | 129.305964 [114.564916, 197.957880] | 169.157343 [122.673314, 205.024381] | +7.583327% | -23.558764% | +40.739910% |
| no_index_control | 625.137285 [606.264066, 627.594202] | 648.614185 [593.216863, 709.039230] | 631.284950 [578.030619, 661.726244] | +3.755479% | +2.745073% | +0.983410% |

### 服务器进程 CPU（毫秒，p50 [min, max]）

| 区域 | P24-C2 首轮 | P25-C2 | P24-C2 回测 | 相对旧首次变化 | 相对旧回测变化 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|
| btree_reindex | 760.495373 [758.154754, 780.828017] | 728.992368 [711.993525, 791.297525] | 754.841707 [732.564381, 849.410351] | -4.142432% | -3.424471% | -0.743419% |
| mixed_reindex_healthy | 840.912922 [807.233664, 842.639363] | 934.555160 [933.473135, 950.315715] | 853.311537 [831.175532, 912.204772] | +11.135783% | +9.520980% | +1.474423% |
| mixed_reindex_corrupt | 829.611376 [806.128355, 851.059174] | 975.567549 [958.115505, 988.216176] | 844.251477 [843.967440, 864.037574] | +17.593319% | +15.554142% | +1.764694% |
| high_range | 61.620222 [58.678446, 73.935374] | 66.979587 [65.826401, 68.867487] | 68.216917 [67.524823, 68.948337] | +8.697413% | -1.813817% | +10.705406% |
| low_range | 59.849300 [46.286915, 63.442182] | 58.844180 [58.713154, 60.798532] | 61.894841 [60.198859, 64.503536] | -1.679418% | -4.928781% | +3.417819% |
| equal | 172.795035 [171.586223, 174.484479] | 194.080637 [177.072781, 245.379515] | 189.400036 [176.155130, 237.617151] | +12.318411% | +2.471278% | +9.609652% |
| absent | 42.074688 [38.817342, 47.026933] | 68.065939 [60.898229, 75.198735] | 76.364088 [50.668009, 79.191475] | +61.774079% | -10.866559% | +81.496505% |
| dense_control | 961.197484 [890.755675, 1130.015725] | 913.233987 [854.169434, 937.553253] | 1036.949346 [938.376090, 1084.037630] | -4.989973% | -11.930704% | +7.880988% |
| primary_control | 75.716266 [31.609250, 78.642856] | 71.300762 [66.728958, 75.446105] | 74.535972 [70.470697, 74.796790] | -5.831645% | -4.340468% | -1.558838% |
| no_index_control | 584.155897 [577.204649, 589.020839] | 596.236307 [570.525388, 642.466854] | 563.947281 [540.188848, 623.792798] | +2.068011% | +5.725540% | -3.459456% |

### 客户端进程 CPU（毫秒，p50 [min, max]）

| 区域 | P24-C2 首轮 | P25-C2 | P24-C2 回测 | 相对旧首次变化 | 相对旧回测变化 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|
| btree_reindex | 0.225899 [0.177688, 0.249421] | 0.245425 [0.211783, 0.285046] | 0.227512 [0.171473, 0.229346] | +8.643686% | +7.873431% | +0.714036% |
| mixed_reindex_healthy | 0.283262 [0.206829, 0.309652] | 0.223227 [0.222594, 0.246110] | 0.211319 [0.208500, 0.305682] | -21.194159% | +5.635083% | -25.398041% |
| mixed_reindex_corrupt | 0.217227 [0.173527, 0.225649] | 0.221521 [0.198124, 0.287458] | 0.204474 [0.178315, 0.259331] | +1.976734% | +8.337001% | -5.870817% |
| high_range | 1.650068 [1.504410, 1.972418] | 1.607994 [1.219314, 1.692421] | 1.585590 [1.494288, 1.743748] | -2.549834% | +1.412976% | -3.907597% |
| low_range | 1.820440 [1.651990, 1.821442] | 1.826089 [1.695683, 2.010420] | 1.848971 [1.845039, 1.905369] | +0.310310% | -1.237553% | +1.567258% |
| equal | 1.375787 [1.313802, 1.402942] | 1.351533 [1.251561, 1.384049] | 1.302202 [1.290311, 1.442450] | -1.762918% | +3.788276% | -5.348575% |
| absent | 1.183702 [1.115296, 1.380737] | 1.170758 [1.126716, 1.177482] | 1.308615 [1.218996, 1.391164] | -1.093518% | -10.534573% | +10.552740% |
| dense_control | 123.183079 [115.793387, 125.891644] | 119.455352 [117.501864, 125.254888] | 108.131191 [100.971827, 130.953984] | -3.026168% | +10.472613% | -12.219120% |
| primary_control | 1.980927 [1.492968, 2.022905] | 1.749522 [1.706424, 1.913113] | 1.824364 [1.784739, 1.865948] | -11.681652% | -4.102361% | -7.903522% |
| no_index_control | 0.949068 [0.910905, 1.000101] | 0.906724 [0.859504, 0.922158] | 0.840192 [0.776399, 0.885800] | -4.461640% | +7.918666% | -11.471886% |

- GiST 真正空文本编码仍未修复；原生候选包含真实 RID 不能证明编码已覆盖。
- native-only 索引的唯一定义文件删除后无权威定义可恢复；缺失的配置不能从名称/文本猜回。
- 虚拟 Bloom NEW UPDATE/DELETE 的真实 logical NULL owner 仍不足；本批不伪造该数据所有者。
- missing_memory_index_protocol_e2e_test.py 在丢 Hash 后期待行、实际 XX001，仍严格失败；旧版配对运行在 CREATE TABLE 等待超时，未到删除阶段，baseline comparison INCOMPLETE，不能称旧版同失败或证明非回归。
- 仍非完整 AM/opclass/catalog/并发 REINDEX 覆盖；REINDEX INDEX/DATABASE/SYSTEM/CONCURRENTLY 不在本批实现。
- 没有修改既有用户暂缓、安全/TDE、参数帧、retained/portal/format2 或 R65 provider 生命周期。

完整机器证据：[reindex-access-methods-20261106.json.gz](benchmarks/reindex-access-methods-20261106.json.gz)。


### 实现机制与源码依据

[PostgreSQL 18.6 index.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/catalog/index.c) 的普通 TABLE 重建遍历该关系全部索引，使用原目录/关系描述符定义、建立新物理代，并由事务与 WAL 管理。`BuildIndexInfo` 提取键数/列号、表达式和谓词等；AM routine、opclass/collation/reloptions 仍依附实际 relation，不能称都由该函数复制。[SQLite 3.50.4 build.c](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/build.c) 复用 existing Index 定义并经 sorter 重填；其[ pager.c](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/pager.c) 承担 pager/journal 的提交与回滚。[DuckDB 1.4.3 CREATE ART](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/operator/schema/physical_create_art_index.cpp) 是构建机制参考，不作为 PG REINDEX 兼容 oracle。

本地采用真实权威定义、先私有构建后统一发布，并将可共享的一次扫描限制到最多8个 builder；mixed V2 标记提供本项目文件交换/恢复协议，不声称完整 PG transactional WAL 或并发等价。23份固定版本官方源码和许可证均归档；源码研究与最终实际执行的24条 PG18.6 共享对照分别保留证据。

下方 P24 及更早段落为历史记录。


<!-- CURRENT_BRIN_OPERATOR_CONSISTENCY_20261106 -->

## BRIN 运算符专用摘要比较（2026-11-06，P24-C2）

本批只改 TableManage.cpp 的普通BRIN摘要比较：首次真正需要非NULL比较时判定一次原受支持类型；每个范围仍检查min<=max，再按运算符只检查必要的一端，等值仅在确定False时短路。Unknown或无效边界仍拒绝整个可选剪枝证明，NULL flags不新增比较需求。legacy非TemporalMicros timestamp/timestamptz/datetime继续走原三态路径。没有更换StorageEngine::compareValues正文、公开API、文件格式、摘要读/CRC/identity校验、dirty与snapshot fence、真实heap/MVCC/NULL及完整Filter复核。P23配置保存是已提交前提，本批不重复计为新修复。

这是有限普通访问方法性能工作，partial、Goal active。候选实际正常构建退出0，程序SHA256 `90f076268aca232fe896a02e19cbcb42e14256686209992a073dc561443a3e58`；实际构建耗时 `188.76793585531414` 秒。默认程序SHA256 `425165afe19d10ccf9be806c3052e9a09e3875a3826142d2949cde995fdf94d1` 保持，未从本报告推出默认安装或服务重启。

### 来源与设计依据

[PostgreSQL 18.6 minmax](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/brin/brin_minmax.c) 对小于条件检查min、大于条件检查max，等值可在第一端排除后停止；[BRIN访问方法](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/brin/brin.c) 独立处理NULL flags并返回lossy候选，仍须堆上复核。[DuckDB v1.4.3数值统计](https://github.com/duckdb/duckdb/blob/v1.4.3/src/storage/statistics/numeric_stats.cpp) 按运算符使用min/max，[SQLite 3.50.4 R-tree](https://github.com/sqlite/sqlite/blob/version-3.50.4/ext/rtree/rtree.c) 用边界限制树遍历。本批采用必要边界比较与保守候选的原则，同时保留本地原有全部摘要有效性验证；没有移植完整访问方法或成本模型。12份官方源码/许可证快照、版本、URL和SHA256随机器证据保留。

### 实际功能与范围

- 新原生专项（同一源码，修前仅25预算失败，最终全部通过）：实际回执字段 `{"after_checks":23414,"after_probes":325,"before_budget_failures":25,"after_budget_failures":0,"wide_canonical_before":222,"wide_canonical_after":1,"compareValues_internal_calls":"UNAVAILABLE; same-TU wrap cannot measure them, 2/3 call bound remains a static algorithm property"}`；所列门禁逐项核对。
- 25项相关原生测试：实际回执字段 `{"complete":true,"all_passed":true}`；所列门禁逐项核对。
- 6项专项协议测试（维护42命令、类型化BRIN101命令等）：实际回执字段 `{"complete":true,"all_passed":true}`；所列门禁逐项核对。
- 3项自动统计测试（原deadline、串行一次）：实际回执字段 `{"complete":true,"all_passed":true}`；所列门禁逐项核对。
- 真实PostgreSQL18.6共享对照：实际回执字段 `{"shared_commands":35,"strict_result_cells":19327,"independent_PG_effective_width_metapages":11,"ReadyT_images_recorded_without_checkpoint_or_page_read":1,"normal_stop":true,"owned_PID_gone":true}`；所列门禁逐项核对。
- 物理恢复worker完整结果：实际回执字段 `{"workers":7,"worker_checks":2477,"retained_complete_streams":14}`；所列门禁逐项核对。

真实计数以回执为准。canonicalResultTypeName可在跨TU调用处用真实public API计数；compareValues在同TableManage TU内，若未单独观察，则2/3次调用预算只属静态算法推导，不冒充实测CPU或调用次数。NULL/类型/损坏摘要与旧快照回堆不能被局部性能计数替代。

### 首次结果与未完成项

- 同源新专项在真实P23-C1旧程序退出1：23414检查、325探针中仅25项重复类型准入预算失败；全部语义控制通过。最终C2同源0，222→1为inclusive canonical调用实测，compareValues内部次数未实测。完整before日志、源码及归约均保留。（完整原证据 `build/pg-feature-brin-consistent-20261106-e5_u6fjz/brin_operator_pruning_test-before-v1.json`）
- 9个数据库backend全部正常0后，原性能driver在最终标签断言实际退出1，label循环变量覆盖C2；原source/plan/completefalse九样本总回执保留，严格离线v2及独立原帧归约完成，无SQL重采样。（完整原证据 `build/pg-feature-brin-consistent-20261106-e5_u6fjz/measurement-driver-terminal-v1.json`）

### 固定普通SQL测量

原始性能driver在九个backend各自正常0、全部样本已保存后，仍于末尾final-candidate-label断言退出1：循环变量label覆盖了最终C2代别，变成before-return。九个backend正常0不能写成外层runner正常0。原source、plan及complete=false/9samples总回执保留，静态准备proof未发现此变量覆盖也保留。后续只对原样本做独立离线finalize/归约，不重采样；这是harness失败，不能写成产品故障或已修复产品。实际failure回执另随facts完整保存。

固定 `(P23-C1首轮,P24-C2,同一P23-C1回测)×3`，9独立正常backend、0重试。相同4096×4人口、8条512行INSERT、显式width1、DELETE id1024、一次完整普通VACUUM FULL；维护后双方CRC独立解码宽度均1，随后ANALYZE，最终4095行。完整VACUUM命令包含原正常持久化，单独计时；7查询每进程34次，共306次，另9次维护计时。15秒查询/120秒setup维护deadline不变。

全部回复的完整值、真实NULL、UTF8、OID、命令标签与ReadyI，实际source/actualRows/counter，查询前后完整heap/统计/索引镜像按独立reducer核对。实测字段数 `1157472`，NULL `52218`，完整population `45`，预检 `63`，保留镜像 `90`。查询计时包含普通SimpleQuery前端/执行/投影/输出/socket接收；Python解码oracle在外。CPU是backend进程所有线程；不能称为独占比较CPU。setup/EXPLAIN/预检已触缓存，未清OS cache或固定CPU；每组仅三个完整批次，不构成显著性、稳定因果或PG速度结论。

七查询wall分类（相对两次baseline中位数）：`{'faster_both': 5, 'between_or_equal': 1, 'slower_both': 1}`。维护单列，所有负数、长尾、控制项和baseline漂移如下，绝不按收益筛选。

### wall 总时间（毫秒，median [min, max]）

| 操作 | 每进程调用数 | P23-C1 首轮 | P24-C2 | 同一 P23-C1 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| VACUUM FULL（一次完整命令） | 1 | 7833.125 [7777.672, 8194.009] | 8116.686 [8038.389, 8186.480] | 7886.124 [7844.712, 7966.549] | -3.62% | -2.92% | 0.68% |
| 高端窄范围 | 4 | 135.752 [74.042, 202.849] | 90.972 [81.604, 491.228] | 126.651 [101.409, 157.963] | 32.99% | 28.17% | -6.70% |
| 低端窄范围 | 4 | 98.900 [75.864, 125.449] | 87.082 [83.758, 777.883] | 116.699 [103.428, 173.153] | 11.95% | 25.38% | 18.00% |
| 非主键等值 | 6 | 236.663 [180.216, 355.268] | 193.445 [189.589, 234.659] | 464.008 [188.565, 3114.536] | 18.26% | 58.31% | 96.06% |
| 不存在的范围 | 6 | 110.640 [89.007, 179.885] | 86.783 [77.550, 114.881] | 143.689 [126.019, 4856.579] | 21.56% | 39.60% | 29.87% |
| 密集范围控制 | 2 | 801.361 [698.529, 898.432] | 1026.784 [753.538, 1247.293] | 1029.212 [784.112, 1043.906] | -28.13% | 0.24% | 28.43% |
| 主键控制 | 8 | 117.713 [95.277, 347.680] | 122.987 [94.838, 126.153] | 103.380 [90.344, 124.372] | -4.48% | -18.97% | -12.18% |
| 无对应索引列控制 | 4 | 538.879 [501.304, 568.090] | 512.925 [505.820, 563.557] | 531.267 [495.645, 538.516] | 4.82% | 3.45% | -1.41% |

### 服务器进程 CPU（毫秒，median [min, max]）

| 操作 | 每进程调用数 | P23-C1 首轮 | P24-C2 | 同一 P23-C1 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| VACUUM FULL（一次完整命令） | 1 | 7795.473 [7628.075, 8124.350] | 7970.939 [7877.029, 8005.007] | 7687.822 [7668.431, 7688.603] | -2.25% | -3.68% | -1.38% |
| 高端窄范围 | 4 | 86.554 [46.928, 87.826] | 61.268 [55.220, 62.444] | 84.763 [80.434, 97.012] | 29.21% | 27.72% | -2.07% |
| 低端窄范围 | 4 | 75.363 [52.746, 90.262] | 57.128 [48.499, 63.061] | 73.180 [61.883, 80.512] | 24.20% | 21.94% | -2.90% |
| 非主键等值 | 6 | 157.064 [146.505, 186.240] | 147.950 [146.505, 197.177] | 196.740 [142.061, 300.946] | 5.80% | 24.80% | 25.26% |
| 不存在的范围 | 6 | 75.960 [55.326, 110.460] | 37.286 [37.131, 72.705] | 107.446 [74.159, 118.626] | 50.91% | 65.30% | 41.45% |
| 密集范围控制 | 2 | 618.718 [612.785, 739.897] | 725.010 [637.568, 733.990] | 738.727 [710.897, 750.874] | -17.18% | 1.86% | 19.40% |
| 主键控制 | 8 | 66.220 [47.813, 74.213] | 50.335 [44.031, 72.045] | 40.080 [39.665, 51.173] | 23.99% | -25.59% | -39.47% |
| 无对应索引列控制 | 4 | 489.087 [463.120, 504.415] | 485.693 [469.181, 535.554] | 483.370 [458.565, 512.141] | 0.69% | -0.48% | -1.17% |

### 客户端进程 CPU（毫秒，median [min, max]）

| 操作 | 每进程调用数 | P23-C1 首轮 | P24-C2 | 同一 P23-C1 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| VACUUM FULL（一次完整命令） | 1 | 0.214 [0.192, 0.236] | 0.270 [0.221, 0.286] | 0.178 [0.170, 0.348] | -26.14% | -51.89% | -16.95% |
| 高端窄范围 | 4 | 1.615 [1.471, 1.819] | 1.623 [1.616, 1.680] | 1.564 [1.503, 1.596] | -0.52% | -3.76% | -3.12% |
| 低端窄范围 | 4 | 1.736 [1.551, 1.744] | 1.776 [1.570, 1.920] | 1.584 [1.373, 1.611] | -2.33% | -12.14% | -8.74% |
| 非主键等值 | 6 | 1.463 [1.159, 1.510] | 1.276 [1.188, 1.356] | 1.294 [1.106, 1.741] | 12.76% | 1.33% | -11.58% |
| 不存在的范围 | 6 | 1.342 [1.091, 1.497] | 1.108 [0.807, 1.344] | 1.271 [1.091, 1.405] | 17.48% | 12.84% | -5.33% |
| 密集范围控制 | 2 | 94.723 [89.983, 114.334] | 98.488 [85.907, 116.137] | 102.793 [88.594, 105.176] | -3.97% | 4.19% | 8.52% |
| 主键控制 | 8 | 2.018 [1.796, 2.175] | 1.563 [1.378, 1.809] | 1.781 [1.601, 1.850] | 22.52% | 12.24% | -11.72% |
| 无对应索引列控制 | 4 | 0.942 [0.689, 1.002] | 0.790 [0.740, 0.939] | 0.899 [0.878, 0.975] | 16.19% | 12.19% | -4.55% |

### 仍需继续

- 仍完整读取、CRC校验并解码全部摘要；规划探测与open前后复核仍重复，元数据和整个SQL成本未由本局部比较计数隔离。
- 仍是本项目有限builtin比较/NULL摘要/lossy候选实现，不是完整PostgreSQL BRIN opclass/catalog/revmap、增量维护、并发构建或AM专用WAL。
- legacy非TemporalMicros timestamp三种raw spelling保留原三态比较；不把部分字符串解析器声称为全序。
- P22数组ROLLBACK间歇超时、Bloom/GiST非NULL空文本原生覆盖及既有普通功能缺口保持；本批没有修复或新增暂缓。
- 只运行所列实际门禁和固定9进程；没有新增35操作、36查询全集或PostgreSQL速度测量，所有控制项负数、长尾和漂移保留。
- 原R151矩阵未运行，用户此前所有暂缓范围不变；完整REINDEX/PG BRIN AM、规划元数据复用与重复文件验证仍开放。

原2374测试文件字节不变，新增native CPP后2375；顶层CPP含桩1113、全CPP1118（另5子目录）。679注册driver/659独立E2E roster保持，build_common未改。源248/头177、754MD路径、725保护记录保持。原273items/status及274条历史完整保留，只追加第275条partial；23旧gzip保持，新增第24份。用户既有retained/参数/portal/wire2/R151专属与R291通用发布故障等暂缓不扩展。

机器证据：[压缩报告](benchmarks/brin-operator-pruning-20261106.json.gz)、[归档清单](benchmarks/manifest.json)。源、全部首次结果、raw/min/p50/max和原始帧/镜像按v5可恢复；归档生成与门禁另由Root在测量结束后执行。下方P23和更早内容保持当时历史原文。


<!-- CURRENT_BRIN_MAINTENANCE_OPTIONS_20261106 -->

## BRIN 配置在表维护后保持（2026-11-06，P23-C1）

本批修复 `VACUUM FULL`、`ALTER TABLE ADD COLUMN`、`ALTER TABLE DROP COLUMN`、`TRUNCATE` 所用 reset 路径将已有 BRIN `pages_per_range` 重置为原生默认64的问题。用户设置的小范围摘要在整理表后不再悄悄变粗。生产修改只有 TableManage.cpp 中四次重建前捕获有效V3配置、四次重建显式传回宽度，共26行diff；锁、MVCC、RID、回滚、持久化及发布流程保持。原生默认64、SQL默认128和BRIN V3文件格式均保持。

这是配置正确性修复及维护后的查询性能对照，partial，Goal active。它没有证明所有数据库操作都更快。固定9进程的全部 wall/服务器CPU/客户端CPU、控制项、尾部、负数与旧版漂移见下表。

### 来源与边界

[PostgreSQL 18.6 cluster.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/cluster.c) 在物理重写后按原定义重建索引；[storage.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/storage.c) 处理物理 relation 重置；[brin_page.h](https://github.com/postgres/postgres/blob/REL_18_6/src/include/access/brin_page.h) 定义可独立读取的 BRIN metapage 宽度。[SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html) 通过重建整理数据库。本地采用“重建物理数据时保留逻辑配置”的原则，没有移植完整PG目录、事务性存储交换或SQLite实现。来源快照、PG许可证和哈希保存在归档research/。

读取配置继续使用已有结构/CRC验证 helper：健康当前V3保留真实宽度；文件缺失、旧格式或损坏时仍回退64。因此不能从本修复推导所有遗留/损坏索引的配置可恢复。`ALTER COLUMN TYPE` 宽度3用例在父代已通过，本批将其作为控制，不归为新修复。

### 正确性、协议与恢复

- 正常 CMake C++17/O2 构建实际0（183.872秒），只重编TableManage对象；候选程序SHA256 `a9d5b4e0750c1c9a755a027383875f9755426aab4d55365f0d19cb0d9774c244`。当前248源文件/177头不增加，NetworkServer.cpp与默认根程序字节保持。默认根程序SHA256 `425165afe19d10ccf9be806c3052e9a09e3875a3826142d2949cde995fdf94d1`；全部运行使用独立候选和私有库。
- 新协议42命令、12个实际V3文件镜像、4个EXPLAIN ANALYZE路径检查；768×4完整 population，包含NULL、非NULL空文本、literal NULL、UTF8/换行。DELETE、VACUUM FULL、加默认列、删列、事务TRUNCATE回滚、提交TRUNCATE及重新插入后核对全部行/NULL/OID/标签/Ready状态与宽度。SQL默认128及ALTER TYPE宽度3另作控制。
- 修前真实父代fixture完成所有命令并失败：9次宽度变64、4次usedSummary缺失，共13项；完整数据结果保持。C1-v1通过；最终v2另加5个显式VACUUM/TRUNCATE命令标签断言，实际0，无降低断言。失败源、v1/v2源及所有帧分别保留。
- 25项相关原生测试全部正常0，覆盖vacuum/truncate/WAL/unlogged、列重写、专用索引DML、generated/expression/partial索引及physical restore。6项专项协议（本批新用例+5既有用例）全部正常0；array/range/scalar自动统计3项按原超时、串行各运行一次，全部正常0。恢复另保存7 worker、2477检查、14路完整stdout/stderr。未重新运行前批81原生或32旧协议全集；前批TOAST中止及3项既有协议失败仍保持原记录。
- 真实PostgreSQL18.6、en_US.UTF-8、默认fsync、独立Unix socket：35条共享SQL、19,327完整字段、全部NULL/OID/命令标签/SQLSTATE/Ready一致。7条本地GUC或实现特定EXPLAIN不比较；11个真实PG元数据页的宽度与本地V3一致。事务中TRUNCATE的1个ReadyT镜像只记录，不为诊断提交/检查点或读物理页。初始化/启动/正常停止均0，owned PID已退出。没有PG AM文件、访问计划或性能等价结论。

初始PG脚本先在不存在的`SHOW lc_collate`失败（42704），随后在locale二维rows形状断言失败，均在重放共享SQL前发生；原失败帧、源码、正常停止收据保留。改为读取当前数据库datcollate并修正严格shape后，v3共享对照完成。性能独立reducer的EXPLAIN标签假设在取样前由静态审阅修正：EXPLAIN严格检查EXPLAIN，普通表结果仍严格检查SELECT行数/字段/OID；原脚本保留。第一次实际离线归约另因DELETE的零列RowDescription被误当作SELECT而失败；按已有DELETE阶段严格检查DELETE 1、非空表描述仍检查SELECT行数修正后，仅对原9份数据重新离线归约，未重新采样。失败回执和两版reducer保留。没有将工具错误写成产品修复。

### 固定维护与查询测量

固定 `(P22 首轮, P23-C1, 同一P22回测)×3`，9个独立正常socket backend，0重试；创建同一4096×4真实表、8条各512行INSERT、同一显式宽度1 BRIN。DELETE id1024后各执行一次普通完整`VACUUM FULL`，该命令单独计时至ReadyForQuery，包含原有持久化设置。独立LE/CRC解码实际证明：创建时两版宽度均1；维护后旧版64、新版1。随后在查询计时外ANALYZE、完整population、EXPLAIN和预检；最终4095行，物理extent观测[288]页。

七类查询每进程共34次，9进程共306次；查询批次计时包含普通SimpleQuery前端、真实访问/MVCC/完整谓词/投影/输出与socket接收，完整解码和oracle比较在计时外。服务器CPU是整个backend进程的所有线程，不能解读为单独BRIN阶段。旧版也可能选择BrinScan，依据actualRows及实际counter验证，未硬编码旧版必定堆扫描。

独立原始帧reducer核对306个计时查询回复、9个计时VACUUM回复、45次完整population、63次预检，计1157472个字段/52218个NULL。保留90个查询前后堆/统计/索引镜像，逐文件字节一致，并独立解码45个V3镜像；另保存创建/DELETE/VACUUM/ANALYZE四阶段BRIN文件。没有独立解析所有heap页，扫描counter是执行器候选tuple计数，不是物理I/O。

auto_analyze/auto_vacuum关闭，未降低持久化参数，未加载profiler，未使用retained/参数/provider路径；未清OS cache、固定CPU或隔离服务器负载，缓存温度未统一。每组三个完整批次观察值，中位数[min,max]不构成统计显著性结论。配置宽度差异是修复的预期结果，不能当作相同摘要数量的独立AM算法比较；主键/无索引列差异也不能归因于这四条维护路径。

wall中位数减少（相对首轮／回测，负数为变慢）：VACUUM FULL（一次完整命令） -0.63%／-1.16%；高端窄范围 57.60%／51.08%；低端窄范围 68.50%／68.40%；非主键等值 6.53%／7.41%；不存在的范围 -99.01%／-101.10%；密集范围控制 -43.61%／-28.15%；主键控制 2.91%／2.50%；无对应索引列控制 -17.55%／-22.39%。

服务器CPU中位数减少（相对首轮／回测）：VACUUM FULL（一次完整命令） -0.51%／-2.00%；高端窄范围 60.24%／56.43%；低端窄范围 76.26%／76.52%；非主键等值 50.45%／51.55%；不存在的范围 -294.62%／-144.27%；密集范围控制 -21.09%／-8.28%；主键控制 -20.31%／-11.79%；无对应索引列控制 -17.21%／-21.19%。

### wall 总时间（毫秒，median [min, max]）

| 操作 | 每进程调用数 | P22 首轮 | P23-C1 | 同一 P22 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| VACUUM FULL（一次完整命令） | 1 | 7980.563 [7780.756, 8912.127] | 8030.771 [7906.916, 8041.499] | 7938.301 [7802.926, 8177.648] | -0.63% | -1.16% | -0.53% |
| 高端窄范围 | 4 | 249.383 [186.711, 1474.745] | 105.739 [100.241, 106.676] | 216.166 [194.615, 243.185] | 57.60% | 51.08% | -13.32% |
| 低端窄范围 | 4 | 368.301 [334.805, 990.493] | 115.998 [69.441, 287.185] | 367.113 [331.746, 384.341] | 68.50% | 68.40% | -0.32% |
| 非主键等值 | 6 | 384.406 [346.929, 1928.367] | 359.316 [211.561, 5453.368] | 388.073 [333.114, 2291.968] | 6.53% | 7.41% | 0.95% |
| 不存在的范围 | 6 | 83.442 [60.972, 90.466] | 166.061 [127.508, 174.378] | 82.578 [64.068, 1539.317] | -99.01% | -101.10% | -1.04% |
| 密集范围控制 | 2 | 678.399 [648.971, 688.247] | 974.279 [821.950, 1310.790] | 760.294 [665.176, 772.344] | -43.61% | -28.15% | 12.07% |
| 主键控制 | 8 | 120.104 [105.031, 177.334] | 116.604 [114.754, 129.577] | 119.589 [74.197, 169.501] | 2.91% | 2.50% | -0.43% |
| 无对应索引列控制 | 4 | 488.128 [461.655, 509.168] | 573.775 [551.065, 582.486] | 468.810 [466.192, 623.456] | -17.55% | -22.39% | -3.96% |

### 服务器进程 CPU（毫秒，median [min, max]）

| 操作 | 每进程调用数 | P22 首轮 | P23-C1 | 同一 P22 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| VACUUM FULL（一次完整命令） | 1 | 7894.589 [7698.125, 7923.408] | 7934.759 [7818.297, 7947.684] | 7778.843 [7707.063, 8081.035] | -0.51% | -2.00% | -1.47% |
| 高端窄范围 | 4 | 205.575 [158.621, 308.795] | 81.731 [67.885, 81.827] | 187.594 [159.161, 203.031] | 60.24% | 56.43% | -8.75% |
| 低端窄范围 | 4 | 323.967 [306.005, 485.153] | 76.920 [43.491, 77.788] | 327.549 [286.783, 330.431] | 76.26% | 76.52% | 1.11% |
| 非主键等值 | 6 | 324.707 [296.557, 542.782] | 160.880 [153.480, 303.553] | 332.086 [284.541, 460.231] | 50.45% | 51.55% | 2.27% |
| 不存在的范围 | 6 | 28.412 [27.999, 31.602] | 112.120 [68.086, 116.120] | 45.900 [27.842, 69.453] | -294.62% | -144.27% | 61.55% |
| 密集范围控制 | 2 | 606.798 [575.309, 611.750] | 734.760 [660.416, 773.444] | 678.587 [593.708, 687.346] | -21.09% | -8.28% | 11.83% |
| 主键控制 | 8 | 56.248 [47.494, 66.276] | 67.673 [55.199, 68.005] | 60.533 [32.096, 62.166] | -20.31% | -11.79% | 7.62% |
| 无对应索引列控制 | 4 | 452.370 [422.809, 460.881] | 530.217 [509.186, 546.449] | 437.511 [429.364, 473.481] | -17.21% | -21.19% | -3.28% |

### 客户端进程 CPU（毫秒，median [min, max]）

| 操作 | 每进程调用数 | P22 首轮 | P23-C1 | 同一 P22 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| VACUUM FULL（一次完整命令） | 1 | 0.201 [0.200, 0.218] | 0.201 [0.175, 0.214] | 0.212 [0.209, 0.223] | 0.21% | 5.06% | 5.11% |
| 高端窄范围 | 4 | 1.687 [1.066, 1.833] | 1.528 [1.525, 1.632] | 1.595 [1.535, 1.633] | 9.37% | 4.19% | -5.41% |
| 低端窄范围 | 4 | 1.648 [1.448, 1.811] | 1.460 [1.283, 1.721] | 1.614 [1.550, 1.941] | 11.39% | 9.53% | -2.06% |
| 非主键等值 | 6 | 1.223 [1.204, 1.349] | 1.159 [1.080, 1.623] | 1.379 [1.225, 1.381] | 5.27% | 15.99% | 12.76% |
| 不存在的范围 | 6 | 1.090 [0.787, 1.156] | 0.981 [0.846, 1.155] | 0.995 [0.974, 1.366] | 10.02% | 1.40% | -8.74% |
| 密集范围控制 | 2 | 84.742 [76.353, 95.922] | 98.280 [90.532, 112.781] | 103.917 [73.685, 104.793] | -15.98% | 5.42% | 22.63% |
| 主键控制 | 8 | 1.614 [1.611, 1.820] | 1.635 [1.352, 1.736] | 1.713 [1.617, 1.797] | -1.27% | 4.58% | 6.14% |
| 无对应索引列控制 | 4 | 0.881 [0.863, 0.911] | 0.870 [0.719, 0.885] | 0.900 [0.839, 0.904] | 1.25% | 3.32% | 2.14% |

### 仍需继续的普通缺口

- REINDEX 仍只重建 Btree；本批没有完整 PostgreSQL AM/opclass/catalog/revmap、增量 summarize/desummarize/autosummarize、concurrent build 或 AM 专用 WAL。
- 配置保存针对可读取且结构/CRC有效的当前 V3；缺失、旧格式或损坏时沿用既有64回退，尚无独立持久化 reloptions catalog，不能声称恢复了不可读文件中的设置。
- 本批只覆盖四条普通维护重建路径及测试中的事务/恢复控制；未覆盖所有 DDL/恢复/故障阶段，也未加入通用原子发布、分配器或 I/O 故障矩阵。
- 前批密集范围服务器 CPU 回测变慢6.64%仍是历史真实观测；本批保留宽度1会增加摘要数量，查询规划重复验证/解码及密集控制开销仍需优化，当前表格中的负数和漂移不得删去。
- P22 自动数组统计原始 fixture 的 ROLLBACK 15秒超时根因仍未定位；本批三个原 deadline fixture 一次正常0，不构成该间歇故障已解决。
- Bloom/GiST 对真正非NULL空文本的引擎建键/维护覆盖仍开放；只读诊断证明原生索引缺键，SQL可经可见堆回退，未证明SQL假阴性。
- SSI 后某些自动提交数组 NULL 条件保守回堆、特殊类型/排序规则/历史或脏快照回退及其余既有普通功能缺口保持开放。

上述问题保持普通开放，没有新增用户暂缓类别。原R151矩阵不运行；用户已暂缓的参数/portal/wire输出所有权、未选CAST、provider生命周期、通用故障注入等边界保持。物理heap格式2正常支持，暂缓format2仍只指wire/output。

原2373个测试文件字节不变，新增1个协议用例后2374；679唯一注册driver、659独立E2E。源248/头177、顶层CPP含桩1112、另5子目录CPP保持。原273完整item和22/166/70/15状态、273条旧历史保持，只追加第274条partial；8份既有文档、754Markdown路径、725保护记录和22旧gzip保持。

机器证据：[维护配置压缩报告](benchmarks/brin-maintenance-options-20261106.json.gz)、[归档清单](benchmarks/manifest.json)。新报告使用JSON v5完整文件/重复帧共享存储，逐文件可还原原始字节、长度和SHA256；gzip往返及旧档案哈希另验。原始大JSON继续忽略，仅提交gzip。可重建测试ELF和私有临时库在签名/归档核对后清理，保留最终候选及必要缓存。下方P22及更早内容为当时历史记录。


<!-- CURRENT_BRIN_TYPED_SCAN_20261105 -->

## 类型化 BRIN 与真实候选页扫描（2026-11-05，P22-C10）

本批修复 BRIN 把数值 2、10 当字符串排序而漏掉 `n>=8` 匹配页的问题，并把范围摘要接入普通 SQL 和绑定后查询的执行计划。原生物理探针证明 **23 页 extent 中选择 1 页、候选 cursor 实际 fetch 1 页、保留 33 个候选行**；完整谓词继续逐行复核。此物理计数针对候选 cursor，不是整条 SQL 的所有堆/TOAST I/O 次数。

正常 O2 网络 SQL 的固定九进程测量已完成，所有三组样本、负数和尾部保留。下表相对首轮／同一旧版回测的“减少百分比”，负数代表变慢。它只覆盖本批的七类查询，**不能推导全库或所有操作已提速**；本批 partial，Goal active。

### 来源与实现

[PostgreSQL 18.6 BRIN](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/brin/brin.c) 向 bitmap 提供 lossy 页，执行器复核所有候选；[minmax](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/brin/brin_minmax.c) 使用类型/策略/排序规则对应的比较函数并独立保存 NULL 状态。[BRIN 文档](https://www.postgresql.org/docs/18/brin.html) 说明自然聚簇数据的范围摘要与候选页复核；[reloptions](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/common/reloptions.c) 与 brin.h 给出默认128页、范围1..131072；[parse_int](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/misc/guc.c) 与 [defGetString](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/define.c) 规定整数配置的文本和舍入规则。[DuckDB 固定提交的 column segment](https://github.com/duckdb/duckdb/blob/b06f17601700ac89a64c0d50cc224f063f26964e/src/storage/table/column_segment.cpp) 使用 zonemap 检查过滤条件，[SQLite query planner](https://www.sqlite.org/queryplanner.html) 比较索引与顺序访问成本。本地只采用类型化摘要、候选剪枝和完整复核的设计原则；来源快照、许可证和哈希保存在归档 research/，没有声称移植其完整 AM。

- 新 BRIN sidecar V3 用 little-endian 保存 pages_per_range、物理 relation ID、heap extent、完整物理 schema 和当前类型/排序身份、范围 min/max、has_nonnull/has_null/unbounded 标志及 CRC32C。全 NULL 范围明确保存；真正空文本有自己的非 NULL 状态，不再使用空字符串哨兵。未知类型或比较结果不作字符串兜底剪枝。
- 新版仍识别 V1/V2 结构与 checksum，但其旧字符串界限不能证明数值范围，原生搜索改由真实可见堆产生候选；SQL 的 optional API 区分“无法证明、须回堆”和“已证明空集合”。缺失/损坏/身份或 extent 不一致时 SQL 回堆；原生健康代际保留损坏拒绝，脏/未完成代际先回堆，包含损坏文件。
- `BrinScan` 打开前和捕获读视图后重复验证 fence 与候选页，沿用现有 heap cursor 的 MVCC、页面锁/pin、RID 和自有 NULL bitmap，只读取候选页并按需求解析 TOAST。RLS、继承、分区、SERIALIZABLE、旧快照和本事务未提交写入保守回退；完整原始 Filter 保留。EXPLAIN 显示 BrinScan、条件、规划 candidatePages 和实际 usedSummary；这些字段不等同底层 I/O。
- SQL 支持五类比较、反向常量比较、AND 剩余条件、IS NULL/IS NOT NULL；比较仅使用合格的内置标量角色，数组/range 只验证容器 NULL 路径。显式 CAST、函数和保留参数不由此规划器预先求值。普通 compact 条件修复常量左侧误当列名；EXPLAIN 对同形条件使用已绑定查询。
- 修复 CREATE INDEX WITH 的括号消耗和选项列表解析。SQL BRIN `WITH(pages_per_range=...)` 按 PG 默认128和1..131072检查；整数选项按 defGetString/parse_int 接受科学计数、小数 ties-to-even 舍入及引号内八/十六进制，拒绝单位/尾随垃圾/非有限值；非法/未知参数22023，autosummarize 明确0A000。原生 API 默认64保持。其他 AM storage parameters 明确0A000。正常专用索引提交/回滚重建保存有效 V3 的配置宽度。

**格式兼容边界**：V3 是本地 BRIN 新文件格式，旧二进制不能按 V3 读取；没有反向兼容承诺。物理 heap 当前版本2保持；用户暂缓的 format2 是协议/输出所有权，NetworkServer.cpp 字节未改。默认根程序 SHA256 `425165afe19d10ccf9be806c3052e9a09e3875a3826142d2949cde995fdf94d1` 保持，测试均使用私有库和单独候选程序。

### 正确性、协议与恢复

- 最终实际 CMake 构建正常0，程序 SHA256 `8b2dc866cd9a9af939491e11f8e16564b2e7da1e5bf75164c8328e595854ee8c`；默认 C++17/O2。8 个既有生产文件变更，无新生产文件、无 class ABI 布局变化。全部中间 C1..C10 构建/源码/objects/程序签名留存。
- 格式组件 O2 与 ASan/UBSan 各 **5588** 检查：独立 LE/bitwise CRC 黄金帧、每一截断/字节损坏、重算 CRC 的非法字段、4096 次确定性变异和保护页。实际离线只读 verifier **8** 种场景检查 V3、坏 CRC、非法宽度/extent/flags/count、截断及恢复，完整库文件字节不变；这不是语义 min/max 重算或 amcheck。
- 新原生类型/范围专项 **1834** 检查，涵盖 bigint 超过2^53、精确 numeric、NaN、布尔、C文本/空文本/literal NULL、date/time/timestamp 微秒、UUID、数组/range NULL、真实缺页/损坏/旧格式、配置和快照 fence。物理 fetch 包装器对实际 cursor 计数；同一 SQL plan 打开前删除或损坏摘要仍返回完整字段/NULL。
- 新协议 **101** 命令、**25** 次路径断言，检查所有字段、NULL、OID、命令标签、五种比较和反向常量、损坏/缺失摘要、双连接 RR 外部提交、自己写入/回滚、显式 RC 数组 NULL、SSI 匹配与空答案回退、SQL 配置默认值/非法值及完整最终 population。P21 既有40条协议命令/逻辑值/OID/标签保持，只升级其独立 V3 文件解码检查。
- 最终有限相关原生 **81** 入口，**80 正常0 + 1 与父代相同 TOAST SIGABRT**（原 `toast_test.cpp:604`）。旧协议32入口为29正常0 + 3父代相同失败：name257/70、timetz2563/314、table_structured表达式排序；完整比较留存，未调低旧功能预期。另六项专项协议正常0。三个自动统计 fixture 串行且原 deadline：range/scalar 首轮0；array 首轮在 ROLLBACK 读取回复时15秒超时，失败保留；随后只做一次相同源/原 deadline 的父代→候选配对复核，二者均19命令/6快照正常0（2.439/2.198秒）。根因未定位，不称三个 fixture 首轮全通过或父代相同失败。恢复 **7 worker /2477 检查 /14 路完整 stdout/stderr**。
- 真实 PostgreSQL **18.6**，en_US.UTF-8、默认 fsync、独立 Unix socket，init/start/stop 均正常0；共享 **73** SQL、**10490** 完整字段、**13** 错误 SQLSTATE、全部 NULL/OID/标签/Ready 状态一致。排除 **28** 条本地 auto_analyze、实现特定 EXPLAIN、unsupported autosummarize 和本地 array BRIN DDL；数组逻辑查询仍比较。另核对8个真实PG metapage中的有效 pages_per_range。没有完整 PG AM 文件/路径/速度等价结论。

早期失败完整保留：C1 时间类型 alias 不能提供可信边界；C2 的 WITH 语法错误；错误的测试结构化值 API/对可选 AST comparison cell 的假设；C4 未接普通 SQL；C5 反向常量返回0行；C6 EXPLAIN 反向条件0A000；C5/C8 在结束 SSI 后的自动提交数组检查保守回堆（最终显式 RC 读事务通过）；C7 的原有专用索引脏损坏回退测试真实中止，最终按原测试修复。初始格式测试的 NUL 拼接/宽度假设错误也保留。真实PG对照还发现初始本地错误拒绝 pages_per_range=1.5；最终按官方 parse_int 修正，并比较8个PG实际metapage宽度和13种错误SQLSTATE。没有把这些失败样本改写为通过。

### 普通 SQL 测量条件

固定 `(P21 首轮, C10, 同一 P21 回测)×3`，九个私有正常 backend，实际4096行×4列，相关 bigint、NULL/空文本/literal NULL/UTF8/换行及512字节 pad，物理 heap extent 观测 **[288]** 页。七类查询，每进程调用数见表；计时包含普通 SimpleQuery 前端、实际访问路径、MVCC、完整谓词/投影/输出与 socket 接收，记录 wall、整个服务器进程 CPU 和客户端 CPU。未加载 profiler、未用 retained/prepared 参数路径。

两版本使用同一 SQL setup/population：旧 SQL 默认64页/字符串 V2且没有 SQL BRIN 源；新版 SQL 默认128页/类型化 V3。因而这是完整预期功能变化的对照，不能当成相同 pages_per_range 的独立 AM 算法微测量。建库、8条各512行种子 INSERT、创建索引、ANALYZE、两次全量人口读、每类一次 EXPLAIN ANALYZE/预检、统计及镜像检查均在计时外。auto_analyze/auto_vacuum 关闭，未降低持久化设置；未清 OS cache、未固定 CPU 或隔离服务器负载，0重试，旧版漂移和所有尾部保留。

独立原始帧解码核对 **306** 计时回复、**18** 次完整 population、**63** 次预检、**715356** 字段（**29889 NULL**），以及 **45** 个保留堆/统计/BRIN镜像。测量前后同一进程的全部镜像字节一致；V2与V3 BRIN格式故意不同，未要求跨版本索引字节相同。扫描 counter/EXPLAIN 是执行器观察，物理 cursor fetch 的证据来自另一个原生测试；没有独立解析所有存储页。

服务器进程 CPU 中位数减少（首轮／回测）：高端窄范围 41.19%／42.36%；低端窄范围 48.68%／49.29%；非主键等值 25.78%／20.13%；不存在的范围 94.22%／94.12%；密集范围控制 -7.44%／-6.64%；主键控制 30.31%／28.42%；无对应索引列控制 7.79%／4.38%。

wall 中位数减少（首轮／回测）：高端窄范围 36.54%／36.45%；低端窄范围 47.39%／46.50%；非主键等值 22.37%／15.22%；不存在的范围 87.45%／87.20%；密集范围控制 -5.30%／12.46%；主键控制 8.62%／11.51%；无对应索引列控制 9.65%／7.04%。控制查询及负数必须和目标查询一起阅读，不用局部下降推导全数据库提速。

主键与无对应索引列是控制项，本批没有直接优化这两条访问路径，其正向差异尚未隔离原因。密集范围控制的服务器 CPU 中位数较两次旧版分别变慢7.44%/6.64%，因此规划开销仍有普通开放的优化空间。

### wall 总时间（毫秒，median [min, max]）

| 查询 | 每进程调用数 | P21 首轮 | 最终 C10 | 同一 P21 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 高端窄范围 | 4 | 492.998 [487.426, 521.765] | 312.864 [258.749, 366.170] | 492.309 [477.423, 503.998] | 36.54% | 36.45% | -0.14% |
| 低端窄范围 | 4 | 1077.926 [1007.629, 1114.759] | 567.087 [517.166, 591.473] | 1059.979 [1047.790, 1184.584] | 47.39% | 46.50% | -1.67% |
| 非主键等值 | 6 | 787.414 [756.891, 787.568] | 611.289 [580.047, 629.020] | 721.043 [696.275, 724.461] | 22.37% | 15.22% | -8.43% |
| 不存在的范围 | 6 | 736.192 [715.003, 845.982] | 92.375 [51.382, 126.968] | 721.915 [708.009, 731.937] | 87.45% | 87.20% | -1.94% |
| 密集范围控制 | 2 | 978.993 [945.696, 1062.687] | 1030.834 [990.914, 1042.706] | 1177.555 [1033.126, 1180.562] | -5.30% | 12.46% | 20.28% |
| 主键控制 | 8 | 118.096 [110.151, 143.493] | 107.911 [82.791, 133.725] | 121.941 [119.055, 132.245] | 8.62% | 11.51% | 3.26% |
| 无对应索引列控制 | 4 | 573.411 [569.146, 664.412] | 518.056 [511.141, 547.305] | 557.288 [525.250, 670.312] | 9.65% | 7.04% | -2.81% |

### 服务器进程 CPU（毫秒，median [min, max]）

| 查询 | 每进程调用数 | P21 首轮 | 最终 C10 | 同一 P21 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 高端窄范围 | 4 | 451.534 [451.523, 487.248] | 265.568 [239.312, 297.581] | 460.761 [451.568, 462.083] | 41.19% | 42.36% | 2.04% |
| 低端窄范围 | 4 | 1023.101 [986.078, 1040.015] | 525.010 [494.844, 527.966] | 1035.390 [1024.849, 1121.155] | 48.68% | 49.29% | 1.20% |
| 非主键等值 | 6 | 728.258 [700.336, 742.294] | 540.492 [526.998, 577.700] | 676.679 [638.837, 681.108] | 25.78% | 20.13% | -7.08% |
| 不存在的范围 | 6 | 689.863 [671.727, 777.305] | 39.856 [27.069, 57.482] | 678.140 [654.031, 684.840] | 94.22% | 94.12% | -1.70% |
| 密集范围控制 | 2 | 681.829 [619.298, 732.087] | 732.564 [701.688, 749.687] | 686.967 [664.151, 724.860] | -7.44% | -6.64% | 0.75% |
| 主键控制 | 8 | 71.161 [67.833, 72.108] | 49.588 [40.933, 74.605] | 69.276 [67.880, 70.826] | 30.31% | 28.42% | -2.65% |
| 无对应索引列控制 | 4 | 540.502 [528.450, 545.258] | 498.421 [462.809, 506.952] | 521.232 [483.132, 571.556] | 7.79% | 4.38% | -3.57% |

### 客户端进程 CPU（毫秒，median [min, max]）

| 查询 | 每进程调用数 | P21 首轮 | 最终 C10 | 同一 P21 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 高端窄范围 | 4 | 1.655 [1.464, 1.698] | 1.649 [1.540, 1.658] | 1.789 [1.637, 1.802] | 0.33% | 7.80% | 8.11% |
| 低端窄范围 | 4 | 1.705 [1.534, 1.816] | 1.646 [1.376, 1.707] | 1.766 [1.626, 1.783] | 3.42% | 6.77% | 3.60% |
| 非主键等值 | 6 | 1.449 [1.441, 1.482] | 1.296 [1.175, 1.324] | 1.431 [1.316, 1.558] | 10.55% | 9.47% | -1.20% |
| 不存在的范围 | 6 | 1.261 [1.174, 1.343] | 1.153 [0.874, 1.203] | 1.235 [1.123, 1.296] | 8.54% | 6.67% | -2.00% |
| 密集范围控制 | 2 | 94.221 [75.749, 100.370] | 100.933 [91.931, 101.394] | 102.351 [96.463, 115.696] | -7.12% | 1.39% | 8.63% |
| 主键控制 | 8 | 1.804 [1.752, 1.809] | 1.594 [1.528, 1.718] | 1.944 [1.738, 2.631] | 11.64% | 17.99% | 7.74% |
| 无对应索引列控制 | 4 | 0.931 [0.851, 0.988] | 0.887 [0.823, 0.981] | 0.856 [0.783, 0.888] | 4.65% | -3.64% | -7.99% |

### 普通开放缺口与存储管理

- PostgreSQL BRIN opclass/catalog/revmap、增量 summarize/desummarize/autosummarize、concurrent build 和 AM 专用 WAL 尚未实现。
- REINDEX 当前仅重建 Btree。正常专用索引提交/回滚保存有效 V3 的宽度，但既有 ALTER/rewrite/vacuum/restore 的显式默认64重建路径尚未统一保存配置。
- 非二进制排序规则、未支持的比较身份/容器顺序、generated key、分区/继承/RLS 来源以及 SERIALIZABLE/历史/脏快照继续回堆，没有完整类型/opclass 覆盖。
- C5/C8 在前一个 SERIALIZABLE 事务结束后的自动提交数组 NULL EXPLAIN 保守回堆；最终数组剪枝使用显式 READ COMMITTED 读事务验证，没有所有自动提交路径均剪枝的结论。
- 自动数组统计原始 fixture 首轮 ROLLBACK 出现15秒超时；配对复核父代和候选均正常0，但根因尚未定位，不当作干净的首次通过。
- Bloom 和 v2 GiST 遗漏真正空文本 key 的问题仍开放；没有完整工作负载、所有操作延迟或 PostgreSQL 吞吐等价结论。

以上仍为普通开放项，未新增用户暂缓类别。原 R151 矩阵未运行，既有用户暂缓的参数/portal/wire输出所有权、未选 CAST、provider 生命周期、全局故障注入等边界保持；没有用保守回退关闭完整 PostgreSQL 功能族。

源248/头177、测试2373（2368旧文件字节保持；两个依赖旧 BRIN 文件布局的测试改为独立 V1/V3 解码，功能/SQL预期保留；新增2CPP+1Python）。顶层 CPP 含桩1112、去桩1111，另5个子目录CPP；678唯一注册/658独立E2E。原273完整对象及22/166/70/15状态、272条旧历史保持，只追加一条 partial 后273。8份既有文档、754Markdown路径、725份保护记录、21份旧gzip保持。Goal active。

新归档采用 JSON v5：重复 wire frame 以共享表保存，文件中的整数片段指向 wire_frames，字符串片段原样拼接。写入和独立读取均逐文件重建原始字节、校验长度和 SHA256，再检查 gzip 往返；完整测量、失败记录与回复没有删除。

完整机器证据：[brin-typed-scan 压缩归档](benchmarks/brin-typed-scan-20261105.json.gz)，[归档清单](benchmarks/manifest.json)。保留所有样本/失败/源码与来源签名，原始大 JSON 继续忽略，只提交压缩文件；同时修正 manifest 的总字节统计覆盖全部归档。可重建 ELF/objects、私有临时库在签名和归档验证后清理，最终候选/必要构建缓存与父代对照保留。下方 P21 及更早段落均为当时历史记录。


<!-- CURRENT_INDEX_BUILD_NULL_OWNER_20261105 -->

## 索引构建、REINDEX 与事务维护的行位图复用（2026-11-05，P21-C1）

本批减少了索引构建按列重复查堆、检查文件身份的 CPU 开销，并修复 BRIN 全 NULL 整数范围摘要。**固定实测未证明所有操作延迟下降**：维护 wall 波动大，多项相对旧版回测变慢；BRIN CPU 也退步。查询控制组没有一致提升，所有负数与尾部保留。本批仍为 partial，Goal active。

### 实现与来源

仅生产文件 `src/commands/TableManage.cpp` 变更。Hash、Bloom、GIN、GiST、SP-GiST、全文、单列/复合 Btree 和 REINDEX 复用当前扫描已拥有的物理 NULL 位图。仅当当前 heap 格式、natts、最小 header offset 和原 callback payload 身份校验通过，且 payload 非空时启用；每个构建者拥有可复用 scratch，既有 RAII binding 在嵌套和异常退出时恢复。原有列解码、virtual demand、partial/expression 计算顺序、索引发布和维护可见性视图保持。真实 virtual NULL/非 NULL 与除零 SQLSTATE 22012 均有回归。

BRIN 从自己固定住的完整 tuple 取得 bitmap，不借其他扫描 RID。旧版本的真实 SQL `INSERT ... NULL` 后构建 BRIN 会写入一条 `[0,0]` 范围（46 字节），最终 C1 写零条范围（20 字节）；真正空字符串仍写一条空字符串范围（44 字节）。三种完整文件均留原始 hex，逐字段及独立 bitwise CRC32C 检查。已有 CRC/格式版本和默认 C++17/O2 构建要求保持。**物理 heap 当前版本是 2；用户暂缓的 format2 指协议/输出结果所有权**，NetworkServer.cpp 未改。

调研依据：[PostgreSQL 18.6 heap index build](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/heap/heapam_handler.c) 从当前 slot 同时形成 values/isnull；[BRIN minmax](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/brin/brin_minmax.c) 区分全 NULL 状态并按类型/策略选择比较函数。[DuckDB 固定提交的 RebuildIndexes](https://github.com/duckdb/duckdb/blob/b06f17601700ac89a64c0d50cc224f063f26964e/src/storage/data_table.cpp) 将所需列与 row ID 的 scan chunk 传给索引回调；[SQLite Column/NullRow](https://www.sqlite.org/opcode.html#Column) 把列值和 NULL 行状态关联到当前 cursor。本地实现只采用“当前行及 NULL 身份共同拥有”的原则，没有移植这些产品代码或实现其完整 AM。完整来源快照、许可证和哈希在归档 `research/`。

### 正确性、协议与恢复

- 实际 CMake 构建正常退出，仅 1 个 TU 重编；程序 SHA256 `af7bf94ca42fc155323832f21d780050deb2d94f1fe73e9682edac3789adb379`。父代 `0e07881f8bf361c88ed4352daeafc985b47426b8`，默认根程序 SHA256 `425165afe19d10ccf9be806c3052e9a09e3875a3826142d2949cde995fdf94d1` 保持。
- 新原生专项 **13366** 断言通过。192 行、4 个物理页：旧 Hash/Bloom/GIN/全文 384 次目标堆 fstat、386 次 fetch；旧单列/复合 Btree 1478/1480，旧 REINDEX 2955/2957。最终十条构建/重建路径均为 **1 次 fstat、3 次 fetch**。旧 BRIN 本来即 1/3，此处没有减少取页的证据。物理计数只覆盖候选构建阶段，不能代表整个数据库吞吐。
- 新协议 **40** 条命令，完整数据、NULL、UTF8、空文本、literal NULL、point、OID、标签、BEGIN/REINDEX/ROLLBACK、真实 INSERT/UPDATE/DELETE/COMMIT 和 BRIN 文件均通过；父代在第 35 条之后的全 NULL 摘要检查严格失败，原始记录保留。
- 本轮有限相关原生 **79** 入口，78 正常 0；1 个 TOAST 测试 SIGABRT（旧 `toast_test.cpp:604`）与父代完整观察一致。旧协议 **32** 入口，29 正常 0，name 257 检查/70 失败、timetz 2563/314 失败及 table_structured 表达式排序失败与父代完整记录一致；未调整旧预期。旧 GiST39、LIKE123/27 扫描断言、typed ANALYZE65/15 及 checksum verifier 均正常 0。
- 3 个自动数组/range/scalar 统计 fixture 串行、原 deadline 通过。恢复 **7** 个真实冷进程、**2477** 检查、完整 14 路 stdout/stderr 保留。
- 私有真实 PostgreSQL **18.6**（en_US.UTF-8，默认 fsync）init/start/stop 均正常 0；共享 **35** 条 SQL、**2250** 字段/NULL/OID/标签相同。auto_analyze 和本地 Bloom/GIN(text)/GiST(integer)/fulltext DDL 共 5 条排除；未比较 PG 索引结构、路径或速度。

### 普通维护测量条件与限制

独立 9 个正常 O2 StorageEngine 进程，固定 `(P20 首轮, C1, 同一 P20 回测)×3`，每个私有库实际 2048 行×5 列。种子、四次全量验证在计时外；auto_analyze/auto_vacuum 关闭，后台间隔设置 60 秒，持久化参数未降低。进程 CPU 覆盖该进程已有工作线程。13 个区域分别计一次真实创建/重建、三项写入加提交、更新加回滚以及完整堆解码控制；包含其正常文件发布和 fsync，**这里是存储 API，不含 SQL 前端或 socket**。

独立解码 **368640** 字段、73728 完整行及全部 NULL/point；每进程最终 11 个索引文件，共 **99** 完整镜像，跨旧/新九进程逐字节相同，其中 **54** 个文件有独立标量 CRC32C 验证。本数据的 score 全部非 NULL，故此字节一致不覆盖故意修复的全 NULL BRIN 摘要。

所有 3 个样本/组与 min/median/max、主机 load、首轮/回测漂移保留，0 重试。未清 OS 缓存、未隔离服务器 I/O 或固定 CPU。多次数百毫秒甚至秒级 wall 尾部、旧版明显漂移及 CPU/wall 分离均可见，不能将本表全部差异归因于位图复用，不能声称全库或 PG 吞吐提升。

相对同一旧版回测的进程 CPU 中位数减少：Hash 创建 27.96%；Bloom 创建 24.52%；GIN 创建 32.01%；GiST 创建 40.33%；SP-GiST 创建 41.02%；BRIN 创建 -154.76%；全文创建 67.66%；单列 Btree 创建 19.30%；复合 Btree 创建 19.02%；REINDEX 14.13%；INSERT+UPDATE+DELETE+COMMIT 45.10%；UPDATE+ROLLBACK 47.67%；完整堆解码控制 -5.21%。BRIN 是负值，完整堆解码控制也有退步。维护 wall 相对回测：Hash 创建 -564.78%；Bloom 创建 -1100.42%；GIN 创建 -497.25%；GiST 创建 -87.37%；SP-GiST 创建 -158.24%；BRIN 创建 -750.96%；全文创建 -86.18%；单列 Btree 创建 -101.25%；复合 Btree 创建 -151.56%；REINDEX 13.77%；INSERT+UPDATE+DELETE+COMMIT 14.57%；UPDATE+ROLLBACK 39.07%；完整堆解码控制 -5.32%。

### 维护 API wall（毫秒，每格 min / median / max）

| 操作 | P20 首轮 | 最终 P21-C1 | 同一 P20 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|
| Hash 创建 | 34.244 / 233.556 / 737.435 | 14.111 / 230.166 / 713.965 | 32.820 / 34.623 / 452.872 | 1.45% | -564.78% | -85.18% |
| Bloom 创建 | 35.779 / 262.746 / 344.754 | 15.905 / 416.801 / 1135.492 | 34.517 / 34.721 / 359.688 | -58.63% | -1100.42% | -86.79% |
| GIN 创建 | 33.680 / 127.802 / 247.070 | 10.754 / 204.076 / 339.394 | 32.524 / 34.169 / 207.260 | -59.68% | -497.25% | -73.26% |
| GiST 创建 | 31.597 / 140.998 / 156.252 | 8.876 / 57.335 / 264.546 | 28.730 / 30.600 / 162.413 | 59.34% | -87.37% | -78.30% |
| SP-GiST 创建 | 33.892 / 113.709 / 454.368 | 12.457 / 83.259 / 97.581 | 31.502 / 32.241 / 373.847 | 26.78% | -158.24% | -71.65% |
| BRIN 创建 | 8.240 / 107.843 / 415.256 | 10.118 / 73.264 / 74.177 | 7.755 / 8.610 / 781.854 | 32.06% | -750.96% | -92.02% |
| 全文创建 | 38.757 / 117.353 / 603.159 | 12.576 / 70.322 / 78.910 | 37.734 / 37.770 / 564.138 | 40.08% | -86.18% | -67.81% |
| 单列 Btree 创建 | 264.343 / 428.617 / 714.529 | 182.816 / 537.108 / 547.751 | 263.615 / 266.887 / 869.751 | -25.31% | -101.25% | -37.73% |
| 复合 Btree 创建 | 278.380 / 460.498 / 1966.474 | 206.936 / 683.228 / 1084.799 | 255.353 / 271.598 / 545.860 | -48.37% | -151.56% | -41.02% |
| REINDEX | 715.421 / 1322.430 / 1479.202 | 535.279 / 653.903 / 1918.751 | 688.789 / 758.284 / 1736.336 | 50.55% | 13.77% | -42.66% |
| INSERT+UPDATE+DELETE+COMMIT | 240.283 / 2566.183 / 4824.014 | 162.796 / 205.789 / 3017.565 | 213.418 / 240.896 / 2329.720 | 91.98% | 14.57% | -90.61% |
| UPDATE+ROLLBACK | 210.425 / 291.847 / 4801.357 | 113.893 / 124.422 / 135.266 | 200.407 / 204.190 / 302.307 | 57.37% | 39.07% | -30.04% |
| 完整堆解码控制 | 89.303 / 98.916 / 105.705 | 90.551 / 95.496 / 106.866 | 84.254 / 90.669 / 105.719 | 3.46% | -5.32% | -8.34% |

### 维护进程 CPU（毫秒，每格 min / median / max）

| 操作 | P20 首轮 | 最终 P21-C1 | 同一 P20 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|
| Hash 创建 | 29.040 / 66.613 / 88.534 | 8.398 / 21.497 / 25.449 | 28.962 / 29.839 / 66.590 | 67.73% | 27.96% | -55.20% |
| Bloom 创建 | 30.909 / 68.517 / 69.292 | 10.019 / 23.214 / 24.518 | 30.226 / 30.757 / 68.293 | 66.12% | 24.52% | -55.11% |
| GIN 创建 | 31.074 / 66.160 / 69.055 | 8.642 / 21.168 / 21.246 | 28.756 / 31.133 / 68.137 | 68.00% | 32.01% | -52.94% |
| GiST 创建 | 28.882 / 60.278 / 65.654 | 6.661 / 16.998 / 17.049 | 26.531 / 28.488 / 62.453 | 71.80% | 40.33% | -52.74% |
| SP-GiST 创建 | 30.212 / 63.993 / 70.363 | 8.641 / 17.827 / 21.653 | 28.664 / 30.224 / 61.778 | 72.14% | 41.02% | -52.77% |
| BRIN 创建 | 5.787 / 11.643 / 14.801 | 6.433 / 15.157 / 15.333 | 5.948 / 5.949 / 14.891 | -30.18% | -154.76% | -48.90% |
| 全文创建 | 30.543 / 37.341 / 40.532 | 8.469 / 9.935 / 10.998 | 29.727 / 30.719 / 35.396 | 73.39% | 67.66% | -17.73% |
| 单列 Btree 创建 | 254.716 / 324.098 / 347.248 | 177.148 / 211.668 / 218.171 | 257.373 / 262.291 / 329.000 | 34.69% | 19.30% | -19.07% |
| 复合 Btree 创建 | 273.432 / 325.389 / 345.725 | 201.220 / 213.778 / 225.023 | 252.113 / 263.997 / 332.735 | 34.30% | 19.02% | -18.87% |
| REINDEX | 703.869 / 704.015 / 818.019 | 519.374 / 640.998 / 642.494 | 679.631 / 746.465 / 802.459 | 8.95% | 14.13% | +6.03% |
| INSERT+UPDATE+DELETE+COMMIT | 188.460 / 381.762 / 401.330 | 100.703 / 102.248 / 203.553 | 181.526 / 186.237 / 362.998 | 73.22% | 45.10% | -51.22% |
| UPDATE+ROLLBACK | 160.583 / 203.740 / 341.399 | 74.053 / 83.201 / 85.134 | 158.889 / 158.985 / 198.732 | 59.16% | 47.67% | -21.97% |
| 完整堆解码控制 | 89.299 / 98.488 / 105.707 | 90.553 / 95.394 / 106.851 | 84.256 / 90.672 / 105.722 | 3.14% | -5.21% | -7.94% |

### 普通 SQL 网络查询控制

另运行固定 9 个普通完整 socket 进程，同一旧/新/同旧回测各三。4096 行×3 列，512 个 NULL key，3200 个相同热点 key；每进程热点前缀/等值各 2 次，其余各 8 次，下表是**该组调用总时间**。完整接收字段，未把 EXPLAIN 放入计时。全部 **324** 条计时回复、18 次完整 population、**740313** 字段（23706 NULL）、36 个堆/统计镜像由独立解码器核对。扫描 counter 是执行器标签，不能单独证明底层物理索引 I/O。

wall 中位数相对首轮／回测减少：热点前缀 7.23%／11.31%；热点等值 0.15%／-1.15%；低频前缀 -14.76%／10.60%；低频等值 -5.74%／-16.10%；不存在等值 -0.35%／3.56%；主键控制 -7.82%／-0.75%。这是查询控制观察，没有一致提升，不能从维护 CPU 收益推导查询收益。

### 完整 socket wall（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P20 首轮 | 最终 P21-C1 | 同一 P20 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 426.343 / 490.046 / 501.038 | 438.648 / 454.601 / 511.379 | 456.243 / 512.559 / 543.689 | 7.23% | 11.31% | +4.59% |
| 热点等值 | 2 | 608.992 / 632.143 / 887.747 | 602.041 / 631.225 / 659.960 | 623.615 / 624.067 / 731.297 | 0.15% | -1.15% | -1.28% |
| 低频前缀 | 8 | 77.281 / 82.519 / 6602.826 | 80.324 / 94.703 / 166.333 | 88.667 / 105.926 / 137.219 | -14.76% | 10.60% | +28.37% |
| 低频等值 | 8 | 68.397 / 101.258 / 4566.358 | 103.159 / 107.068 / 125.271 | 83.631 / 92.217 / 115.712 | -5.74% | -16.10% | -8.93% |
| 不存在等值 | 8 | 66.861 / 99.828 / 8141.531 | 79.158 / 100.178 / 111.117 | 89.248 / 103.879 / 113.324 | -0.35% | 3.56% | +4.06% |
| 主键控制 | 8 | 68.115 / 100.624 / 2593.557 | 79.177 / 108.492 / 132.777 | 87.484 / 107.686 / 111.337 | -7.82% | -0.75% | +7.02% |

### 查询服务进程 CPU（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P20 首轮 | 最终 P21-C1 | 同一 P20 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 353.353 / 396.849 / 440.320 | 391.979 / 395.753 / 452.484 | 417.096 / 446.707 / 447.288 | 0.28% | 11.41% | +12.56% |
| 热点等值 | 2 | 534.949 / 541.544 / 542.192 | 545.897 / 549.048 / 589.124 | 542.298 / 577.869 / 637.938 | -1.39% | 4.99% | +6.71% |
| 低频前缀 | 8 | 44.053 / 45.856 / 126.128 | 42.779 / 59.940 / 84.855 | 48.476 / 69.357 / 94.754 | -30.71% | 13.58% | +51.25% |
| 低频等值 | 8 | 32.453 / 62.351 / 83.968 | 66.585 / 70.668 / 72.546 | 36.594 / 55.353 / 73.379 | -13.34% | -27.67% | -11.22% |
| 不存在等值 | 8 | 30.071 / 60.268 / 93.538 | 37.822 / 67.582 / 67.853 | 52.265 / 67.899 / 68.720 | -12.14% | 0.47% | +12.66% |
| 主键控制 | 8 | 31.754 / 63.425 / 78.062 | 34.864 / 59.753 / 64.934 | 50.071 / 65.695 / 69.321 | 5.79% | 9.05% | +3.58% |

### 查询客户端接收 CPU（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P20 首轮 | 最终 P21-C1 | 同一 P20 回测 | 相对首轮减少 | 相对回测减少 | 旧版漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 63.858 / 69.221 / 87.345 | 58.432 / 67.148 / 69.452 | 71.593 / 72.042 / 94.772 | 2.99% | 6.79% | +4.08% |
| 热点等值 | 2 | 70.322 / 79.597 / 85.663 | 69.850 / 84.776 / 86.025 | 74.525 / 76.707 / 76.895 | -6.51% | -10.52% | -3.63% |
| 低频前缀 | 8 | 1.092 / 1.391 / 1.793 | 1.472 / 1.668 / 2.045 | 1.542 / 1.602 / 1.633 | -19.94% | -4.10% | +15.21% |
| 低频等值 | 8 | 1.421 / 1.747 / 1.858 | 1.379 / 1.746 / 1.786 | 1.482 / 1.633 / 1.705 | 0.06% | -6.90% | -6.51% |
| 不存在等值 | 8 | 1.277 / 1.572 / 1.913 | 1.358 / 1.473 / 1.490 | 1.404 / 1.526 / 1.586 | 6.29% | 3.48% | -2.91% |
| 主键控制 | 8 | 1.460 / 1.584 / 1.996 | 1.316 / 1.722 / 1.819 | 1.587 / 1.685 / 1.791 | -8.69% | -2.17% | +6.38% |

### 诊断、未完成项与归档

单独 CPU 诊断只用于定位：六类创建的父代 160 个 samples，fstatat/__fstat64 flat 合计 120（75%）；C1 70 个 samples，fstatat 27（38.6%），storedRowColumnIsNull 不再出现在样本。采样数小，不能把百分比当耗时比例。初始诊断工具环境误写 CPUPROFILE_SIGNAL，使 before-v1 在切换诊断时退出 -12；修正为 CPUPROFILESIGNAL 后两个诊断 worker 均正常 0。初始 pprof 输入漏掉 .0 文件后缀的工具错误原因另有记录，随后离线解码同一份现有完整采样，没有重跑挑选。普通测量未加载分析器。

BRIN 仍按字符串比较界限：本轮实测整数值 2、10，`n>=8` 的原生 range probe 返回 0 范围，漏掉 id=2 的真实匹配页。当前普通 SQL 未接 BRIN 执行算子；后续需要类型比较、旧文件安全回退和执行算子。Bloom/v2 GiST 原有空字符串 key 省略、PG opclass/tree/WAL、concurrent builds 等也未完成。这些是普通开放缺口，未新增用户暂缓项。先前用户暂缓范围及 R151 原始矩阵保持。

源248/头177；2368 旧测试逐字节保留，新增2后2370（顶层 CPP 含桩1110、去桩1109，另5个子目录CPP）；677唯一注册/657独立E2E。原273个完整对象及22/166/70/15状态、271条旧历史保留，只追加一条 partial 后272。八份既有 Markdown 更新，路径仍754，725份保护记录和20份旧gzip保持。Goal active。

完整证据：[压缩归档](benchmarks/index-build-scan-owner-20261105.json.gz)，[归档索引和恢复说明](benchmarks/manifest.json)。gzip 保存独立 JSON 字节、全部普通样本与失败观察、源码/构建/程序哈希和离线检查，不把原始大 JSON 放回 Git；ELF/objects/临时库采用哈希和构建记录，清理可重建产物。下方 P20 及更早段落保留其当时记录。


<!-- CURRENT_INDEX_CRC32C_20261105 -->

## 共享索引 CRC32C 与 CPU 分派（2026-11-05，P20-C2）

Hash、Bloom、GIN、BRIN、GiST、SP-GiST 及相应离线校验使用的 IndexChecksum.h 原来每字节做8次位运算。现在使用编译期生成的256项通用查表，并在GNU/Clang x86运行时确认为SSE4.2时调用局部硬件函数；64/32位字通过memcpy安全载入，剩余字节独立处理。原多项式、初始/未终结状态、最终XOR、完整文件校验和所有文件格式保持。没有全局-march=native/-msse4.2要求，也没有关闭或抽样校验。通用路径按字节计算，不依赖本机字节序。

下方P19及更早批次保留为历史证据，其计数和候选哈希按对应批次解释。

### 调研与实现

[PostgreSQL REL_18_6 SSE4.2选择器](https://github.com/postgres/postgres/blob/REL_18_6/src/port/pg_crc32c_sse42_choose.c)按CPU能力选择硬件或软件CRC，[硬件实现](https://github.com/postgres/postgres/blob/REL_18_6/src/port/pg_crc32c_sse42.c)保留任意字节长度与短尾，[软件实现](https://github.com/postgres/postgres/blob/REL_18_6/src/port/pg_crc32c_sb8.c)使用slicing-by-8。本批采用独立实现的单表通用路径及局部SSE4.2硬件函数；未声称实现PG的全部CPU分派或软件算法。[GCC builtin说明](https://gcc.gnu.org/onlinedocs/gcc/x86-Built-in-Functions.html)要求检测代码使用基本指令集；本项目真实CMake仍C++17/O2，只有硬件函数带target属性。最终函数直接读取编译器已初始化的CPU能力位，CRC路径没有新增函数局部初始化guard、分配或锁。C1使用局部static函数指针，虽然相关检查通过，其新增初始化同步没有纳入最终C2设计；保留C1资料，不声称曾观察到该guard死锁。

[SQLite checksum VFS](https://www.sqlite.org/cksumvfs.html)在页读写边界保留完整校验；[DuckDB固定提交checksum.cpp](https://github.com/duckdb/duckdb/blob/b06f17601700ac89a64c0d50cc224f063f26964e/src/common/checksum.cpp)以64位块和短尾计算其自有校验。它们的文件/校验算法与本项目不同，不能换成另一种快哈希而仍声称兼容。本批8份实际来源/许可证及哈希在归档research中；未复制来源实现或比较SQLite/DuckDB运行速度。

### 组件、架构与真实存储兼容

最终CRC专项 44392 检查正常0；独立组件O2及ASan/UBSan各44392检查正常0。以原位递推为独立oracle，验证标准向量、任意初态、0..31偏移、短尾/页/64KiB边界、大输入、任意分块、NULL指针的零长度、保护页无越界读取、8线程并发首次使用，以及父线程计算期间4个子进程的CRC结果。组件以-mno-sse4.2编译，局部硬件函数仍由运行时检测保护；没有整库sanitizer声明。

QEMU8.2.2实际执行同一O2组件：qemu64报告hardware=0、35520检查正常0；max报告hardware=1、44392检查正常0。[QEMU用户态CPU选择说明](https://www.qemu.org/docs/master/user/main.html)对应真实记录的-cpu参数。模拟不是实体旧CPU/其他架构的性能测量，未验证ARM/MSVC整库构建。

同一专用存储worker分别链接父版及最终C2的真实61 core对象。12个新进程正常0，完整校验3840字段/NULL/空串/文本NULL/换行/Unicode/点坐标及native RID命中断言。旧写新读、新写旧读、事务INSERT/UPDATE/DELETE及六类索引重建均覆盖。旧版位算法、新版硬件及QEMU无SSE4.2通用路径重建的六份完整索引文件逐字节一致；旧/新事务变更后的六文件亦一致。42份索引文件内容及独立Python逐位CRC验证保留，当前tuple格式；未修复用户暂缓的格式2所有权问题。

### 回归与保留失败

最终68个原生入口67正常0；TOAST原604行!corruptScan.open()的SIGABRT与父版完整观察相同，未改旧夹具。32旧协议29正常0，name、timetz、table_structured三项完整观察与父版相同，不计为通过。既有GiST39命令、LIKE123/27算子断言、typed ANALYZE65/15及离线heap/index校验与损坏注入夹具正常0；3个自动分析夹具按原deadline串行正常0。7个恢复worker正常0，共2477检查，14完整输出流保留。最终GiST物理探测仍验证当前读候选阶段0次堆页fetch，损坏、缺失、旧快照、自身写入、重叠事务和SERIALIZABLE保留原堆后备；算子idx_scan本身仍不能单独证明物理索引I/O。

真实PG18.6全新私有目录、en_US.UTF-8、默认fsync共享37命令，1287完整字段/NULL/OID/命令标签及事务状态一致。排除本系统auto_analyze与GiST(text) DDL；私有文件损坏动作未移植到PG。这验证共享SQL结果，不证明PG AM/成本/吞吐等价。未跑完整注册套件或R151原341/103矩阵，NetworkServer.cpp逐字保持。

### 采样与正常查询性能

父版既有同一负载2048调用/3317样本中，CRC逐位更新flat24.4%。最终C2同一2048调用/2552样本，CRC硬件函数flat0.3%；完整pprof、maps、raw profile及剩余CRC样本见归档profile-crc-functions-C2-v1.json。采样仅用于CPU定位，不从一次profile推导加速倍数。

以下另用9个正常服务进程：（P19首轮、最终C2、同一P19回测）×3，无计时重试、未加载profiler。4096行×3列，512 NULL键、3200相同热点换行/分隔符键，乘37置换插入、真实B-tree/GiST、显式ANALYZE。热点各2调用，其他各8调用。时钟内包括普通SimpleQuery的解析/绑定/规划、真实访问/完整谓词、投影/编码及完整socket接收；严格解码、预热/建表、计数与文件检查在时钟外。CPU使用活服务进程POSIX clock，另记客户端接收CPU。OS cache未清除/固定，未隔离全机工作、固定CPU频率或绑核；每组3样本，所有尾部、负值与首尾基线漂移保留。

独立帧解码740313字段/23706 NULL、324计时回复、18全人口回复、54对算子计数及36保留物理文件哈希。测量前后全堆与两份统计文件字节一致；该解码不独立解析存储页格式。两版本热点走顺序算子，其余为索引算子。中位数wall减少（首轮／回测，负数为退步）：热点前缀 18.62%／-0.48%；热点等值 3.68%／0.90%；低频前缀 11.82%／14.98%；低频等值 -12.80%／-22.17%；不存在等值 6.78%／0.48%；主键控制 -1.07%／-6.83%。低频LIKE是直接目标，其他控制差异的原因未单独隔离；六类索引写入/重建的兼容通过，也不能代替这些操作的速度测量。

### 完整 socket wall（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P19 首轮 | 最终 C2 | 同一 P19 回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 428.258 / 564.360 / 565.386 | 439.955 / 459.304 / 483.431 | 434.404 / 457.114 / 475.529 | 18.62% | -0.48% | -19.00% |
| 热点等值 | 2 | 580.542 / 634.592 / 768.454 | 609.118 / 611.224 / 622.284 | 593.026 / 616.751 / 685.971 | 3.68% | 0.90% | -2.81% |
| 低频前缀 | 8 | 112.596 / 131.453 / 139.484 | 96.432 / 115.921 / 177.154 | 95.220 / 136.343 / 149.750 | 11.82% | 14.98% | +3.72% |
| 低频等值 | 8 | 82.095 / 100.278 / 124.813 | 98.135 / 113.109 / 121.359 | 68.783 / 92.580 / 114.744 | -12.80% | -22.17% | -7.68% |
| 不存在等值 | 8 | 97.023 / 103.003 / 140.046 | 96.007 / 96.015 / 128.996 | 83.520 / 96.479 / 114.251 | 6.78% | 0.48% | -6.33% |
| 主键控制 | 8 | 82.550 / 101.627 / 131.798 | 79.640 / 102.718 / 137.367 | 83.673 / 96.148 / 108.357 | -1.07% | -6.83% | -5.39% |

### 服务进程 CPU（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P19 首轮 | 最终 C2 | 同一 P19 回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 360.608 / 463.285 / 470.606 | 352.747 / 383.416 / 384.059 | 369.744 / 372.658 / 400.518 | 17.24% | -2.89% | -19.56% |
| 热点等值 | 2 | 502.632 / 553.439 / 609.232 | 507.000 / 534.956 / 538.854 | 516.719 / 548.834 / 619.114 | 3.34% | 2.53% | -0.83% |
| 低频前缀 | 8 | 69.293 / 87.145 / 98.680 | 55.895 / 76.960 / 95.705 | 57.919 / 98.766 / 107.865 | 11.69% | 22.08% | +13.34% |
| 低频等值 | 8 | 48.148 / 57.894 / 63.533 | 54.397 / 72.606 / 76.330 | 32.027 / 54.871 / 72.584 | -25.41% | -32.32% | -5.22% |
| 不存在等值 | 8 | 57.526 / 60.998 / 69.390 | 58.210 / 59.159 / 72.995 | 43.807 / 59.826 / 69.369 | 3.01% | 1.11% | -1.92% |
| 主键控制 | 8 | 42.475 / 64.820 / 66.181 | 41.918 / 60.980 / 73.224 | 45.262 / 48.054 / 57.897 | 5.92% | -26.90% | -25.87% |

### 客户端接收 CPU（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P19 首轮 | 最终 C2 | 同一 P19 回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 68.390 / 93.510 / 97.105 | 72.825 / 76.335 / 95.286 | 66.956 / 68.980 / 90.482 | 18.37% | -10.66% | -26.23% |
| 热点等值 | 2 | 76.219 / 80.400 / 80.457 | 72.190 / 83.642 / 89.394 | 75.138 / 75.380 / 95.083 | -4.03% | -10.96% | -6.24% |
| 低频前缀 | 8 | 1.456 / 1.591 / 1.798 | 1.437 / 1.744 / 1.872 | 1.490 / 1.512 / 1.550 | -9.65% | -15.35% | -4.94% |
| 低频等值 | 8 | 1.621 / 1.640 / 1.860 | 1.674 / 1.856 / 1.980 | 1.330 / 1.499 / 1.595 | -13.23% | -23.88% | -8.60% |
| 不存在等值 | 8 | 1.491 / 1.564 / 1.622 | 1.500 / 1.672 / 1.681 | 1.453 / 1.489 / 1.558 | -6.93% | -12.29% | -4.78% |
| 主键控制 | 8 | 1.479 / 1.609 / 1.611 | 1.630 / 1.977 / 2.043 | 1.535 / 1.600 / 1.646 | -22.87% | -23.52% | -0.52% |

### 版本与剩余

最终私有程序SHA256 `f5f09f8c2ad2efd72914ca0a3de0fafb497d8ccf75a369d557c33639621b0a38`，默认程序仍`425165afe19d10ccf9be806c3052e9a09e3875a3826142d2949cde995fdf94d1`。C1/C2实际CMake正常0，各重编译DataDirectory/TableManage/BloomIndex/HashIndex四个TU，62个真实对象及61 core/显式桩有哈希；类布局和对外CRC API不变。源248/头177；原2367测试逐字保持，新增1个CPP后2368；676注册驱动/656独立E2E，754 Markdown路径/725保护记录保持。原273完整对象、22/166/70/15状态及270历史原文保留，新增partial后271，Goal active。直接master本地提交，无push/default/service promotion。

19份旧gzip和清单项保持，新[压缩机器证据](benchmarks/index-crc32c-20261105.json.gz)采用逐字节roundtrip和内容去重后为第20份；不跟踪原始大JSON、ELF/对象/原始堆镜像。仍未完成：ARM硬件加速、PG完整CPU/算法分派、所有metadata checksum、启停/重写/在线全库校验，以及GiST tree/opclass/KNN/WAL与完整数据库成本/性能校准。B+Tree、heap、WAL及其他独立校验实现未在本批统一重写。所有用户暂缓范围保持，整体Goal继续。


<!-- CURRENT_GIST_READ_OWNER_20261105 -->

## GiST 当前读事务与可见元组回退（2026-11-05，P19-C3）

普通 SELECT 的隐式只读事务此前被 specializedIndexesNeedHeapFallback 当作其他活跃事务，因此 GiSTScan 内部仍遍历全堆。修复使能证明为当前、无重叠且无自身写入的读者使用现有 GiST 文件；取候选前后重验真实耐久事务分配高水位和跨后端 dirty 标记。历史快照、其他活跃事务、自身写入、探测期间高水位变化、缺失/损坏文件继续回退。SERIALIZABLE 继续走既有堆扫描，保留关系/页/元组 SIREAD，包括空结果。本批未改 indexReadSnapshotFence 或 GIN/BRIN/SP-GiST/FTS 的共享判断，也未实现 PostgreSQL GiST tree。

**修正旧报告的解释：下方 P17/P18 及更早章节的 GiSTScan/idx_scan 计数只证明执行算子，不能单独证明底层读取索引文件；该算子内部可能回退全堆。原始时间、行值和负值保持，其“索引路径”表述应按此限制理解。** 本批用真实堆 PageAllocator::fetchPage 链接包装、文件 inode 的 fstat 观察及调用栈采样补足物理证据；计数观察器不参与正常性能时钟。

### 调研与实现边界

[PostgreSQL REL_18_6 indexam.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/index/indexam.c)把扫描快照交给 heap fetch；[nodeIndexscan.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeIndexscan.c)在需要时重验原始索引条件。本项目只有当前版本的 flat GiST sidecar，不能照搬 PG 对历史版本的能力，必须保留完整性 fence 和可见堆后备。[SQLite LIKE 优化说明](https://www.sqlite.org/optoverview.html#the_like_optimization)将适用前缀转成范围，其他模式继续检查 LIKE；本批不去掉既有完整谓词重验。[DuckDB 固定提交的 physical_table_scan.cpp](https://github.com/duckdb/duckdb/blob/b06f17601700ac89a64c0d50cc224f063f26964e/src/execution/operator/scan/physical_table_scan.cpp)把实际表函数扫描状态和过滤输入传给执行源；本次同样检查了物理访问层，而非仅看计划名称。6份实际原始来源/许可证与哈希保存在归档 research 中；未比较 SQLite/DuckDB 速度，也未复制其实现代码。

堆后备在 forEachRow 已经持有的可见物理元组上读取 NULL bitmap，用现有局部 RAII 绑定解码字段，避免逐行逐列重新取得 allocator 与当前文件身份。回调数据已经剥离物理头，不能再把它当 HeapTupleHeader。旧 V2 GiST 文件不存空文本键，所以可能包含空串的文本范围保留堆路径；空串、SQL NULL、文本 NULL 分别处理。索引格式、写入器、旧格式支持及 BufferPool 文件身份验证未改。

### 严格验证与失败记录

最终新原生 1214 检查正常0。同一源码对父版在 fresh RC 的物理断言失败：候选检索阶段从2021次堆页 fetch/2013次目标堆 fstat 降到0/0；无事务/新鲜RC/初始RR可直接读文件，旧RR/自身写入/其他后端/真实耐久分配间隙/CRC损坏/文件缺失/空文本范围与SERIALIZABLE保持堆回退。候选检索的0不表示整条 SQL 不回表：返回行仍验证 MVCC、完整字段/NULL/TOAST和完整谓词。历史RR同时验证peer把空串改NULL、NULL改空串后的旧可见值；反复只读探测保持整份堆/索引字节。

最终60个旧原生入口59正常0；toast_test在原604行 !corruptScan.open() 处SIGABRT，父版同源码亦SIGABRT，完整观察仅标准化私有产物路径后相同。旧32协议29正常0；name、timetz、table_structured三项完整观察与父版一致，原始失败不改为通过。新协议39命令、旧LIKE123命令/27算子断言、typed ANALYZE65命令/15算子断言均通过；另3自动分析夹具按原deadline串行正常0。7恢复worker正常0，共2477检查，14完整stdout/stderr保留。未执行完整注册套件、整库sanitizer或原R151 341/103矩阵；NetworkServer.cpp逐字保持。

真实PG18.6用全新私有目录、en_US.UTF-8及默认fsync运行共享37命令，1287完整字段/NULL/OID/命令标签和事务状态一致。排除本系统auto_analyze与原生GiST(text) DDL；本系统私有文件缺失/CRC损坏动作没有移植到PG。它验证共享SQL可见结果，不是PG AM/计划/成本或速度等价。

开发过程完整保留：早期新夹具把已剥头数据误当物理头、C1后备有同类错误，C2改为真实RID bitmap及已有扫描元组所有权；C2审计发现SERIALIZABLE读依赖需保留，C3恢复该级别堆路径。C3首个批量驱动误复用旧标签，保护检查拒绝覆盖，未编译或执行测试，修正标签后的v2才是最终60入口。C2阶段同一旧TOAST失败、初始编译/解码诊断和未验收候选资料也保留，不算新功能通过。

### CPU定位与正常性能

旧版采样256调用/1443样本，forEachRow累计90.0%；最终C3采样2048调用/3317样本，主要剩余包含CRC32C（flat24.4%）与catalog publicationState（累计12.5%）。采样进程加载libprofiler，仅定位调用栈，不从调用数不同的profile推导加速倍数。完整profile、映射、raw samples和pprof输出保留。

下面是另外预定的9个正常服务进程：（P18首轮、最终C3、同一P18回测）×3；确认没有加载profiler，无计时重试。4096行×3列、512 NULL键、3200相同热点换行/分隔符键、真实B-tree/GiST、显式ANALYZE及乘37置换插入；热点各2调用，其他各8调用。时钟内是普通SimpleQuery的解析/绑定/规划/实际访问/结果编码和完整socket回复接收，严格解码在时钟外。服务CPU取活进程POSIX clock，另记录客户端接收CPU。建表/预热/计数/文件核验在时钟外；OS cache未清除/固定，未隔离全机工作或固定CPU频率，每组仅3样本。所有尾部、负值与首尾基线漂移保留，不作总体推断。

独立原始帧解码740313字段/23706 NULL，324计时回复、18全人口回复、54对原始算子计数及36保留文件哈希；前后全部堆和两份统计字节一致。该解码不独立解析存储页格式。两版本热点都走顺序算子，其余走索引算子；这些计数仍不能单独证明底层物理I/O。

中位数wall减少（相对首轮／回测，负数表示退步）：热点前缀 2.54%／3.42%；热点等值 -0.42%／-1.44%；低频前缀 56.62%／52.19%；低频等值 -12.42%／-10.29%；不存在等值 2.15%／1.44%；主键控制 -6.09%／-3.54%。只有低频LIKE是本批直接优化目标，其他病例作为完整控制一并报告；这些控制差异的原因未单独隔离，不宣称它们加速。

### 完整 socket wall（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P18 首轮 | 最终 C3 | 同一 P18 回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 471.500 / 486.061 / 537.751 | 456.526 / 473.699 / 555.368 | 484.438 / 490.462 / 530.892 | 2.54% | 3.42% | +0.91% |
| 热点等值 | 2 | 670.365 / 680.619 / 750.786 | 651.016 / 683.488 / 687.066 | 671.048 / 673.807 / 716.921 | -0.42% | -1.44% | -1.00% |
| 低频前缀 | 8 | 579.137 / 592.017 / 600.934 | 246.284 / 256.821 / 273.662 | 509.036 / 537.157 / 614.530 | 56.62% | 52.19% | -9.27% |
| 低频等值 | 8 | 175.170 / 196.912 / 208.818 | 197.219 / 221.372 / 221.820 | 199.975 / 200.712 / 229.147 | -12.42% | -10.29% | +1.93% |
| 不存在等值 | 8 | 193.792 / 202.525 / 205.170 | 188.168 / 198.175 / 202.556 | 196.578 / 201.075 / 204.217 | 2.15% | 1.44% | -0.72% |
| 主键控制 | 8 | 183.408 / 189.306 / 202.878 | 188.292 / 200.831 / 211.318 | 193.610 / 193.956 / 202.610 | -6.09% | -3.54% | +2.46% |

### 服务进程 CPU（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P18 首轮 | 最终 C3 | 同一 P18 回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 392.679 / 392.679 / 453.497 | 368.558 / 391.748 / 462.507 | 387.787 / 404.689 / 415.256 | 0.24% | 3.20% | +3.06% |
| 热点等值 | 2 | 550.339 / 611.593 / 676.530 | 535.589 / 606.577 / 611.740 | 543.989 / 565.448 / 628.433 | 0.82% | -7.27% | -7.54% |
| 低频前缀 | 8 | 448.863 / 457.252 / 466.269 | 119.073 / 134.771 / 138.580 | 377.000 / 399.518 / 468.691 | 70.53% | 66.27% | -12.63% |
| 低频等值 | 8 | 48.047 / 69.242 / 75.988 | 67.255 / 72.236 / 73.607 | 62.275 / 72.377 / 72.858 | -4.32% | 0.19% | +4.53% |
| 不存在等值 | 8 | 66.236 / 70.404 / 71.406 | 54.211 / 67.727 / 67.853 | 68.026 / 68.698 / 74.581 | 3.80% | 1.41% | -2.42% |
| 主键控制 | 8 | 61.920 / 66.169 / 70.603 | 62.345 / 66.124 / 66.860 | 67.282 / 67.418 / 72.059 | 0.07% | 1.92% | +1.89% |

### 客户端接收 CPU（毫秒，每格 min / median / max）

| 查询 | 每进程调用数 | P18 首轮 | 最终 C3 | 同一 P18 回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 67.042 / 71.361 / 73.382 | 70.045 / 71.958 / 79.206 | 67.172 / 80.584 / 95.690 | -0.84% | 10.70% | +12.92% |
| 热点等值 | 2 | 64.366 / 80.036 / 97.428 | 63.519 / 68.142 / 91.028 | 67.155 / 92.151 / 110.430 | 14.86% | 26.05% | +15.14% |
| 低频前缀 | 8 | 1.539 / 1.625 / 1.795 | 1.626 / 1.683 / 1.809 | 1.651 / 1.664 / 1.834 | -3.56% | -1.14% | +2.39% |
| 低频等值 | 8 | 1.569 / 1.874 / 1.911 | 1.654 / 1.796 / 1.817 | 1.695 / 1.739 / 1.752 | 4.15% | -3.30% | -7.21% |
| 不存在等值 | 8 | 1.490 / 1.554 / 1.811 | 1.287 / 1.553 / 1.585 | 1.491 / 1.577 / 1.589 | 0.06% | 1.53% | +1.50% |
| 主键控制 | 8 | 1.640 / 1.732 / 1.815 | 1.619 / 1.716 / 1.845 | 1.692 / 1.781 / 1.784 | 0.92% | 3.66% | +2.84% |

### 版本、大小与剩余

私有候选SHA256 `3474059f19a031d86b55e33e3a5511d3f9f7c8906db7af8e0bd5e847e24f1889`，根目录默认程序仍`425165afe19d10ccf9be806c3052e9a09e3875a3826142d2949cde995fdf94d1`。本批CMake正常C++17构建，C1/C2/C3各实际重编译1个TU，62对象/61 core及显式重编译匹配桩均有哈希。源248/头177，原2365测试逐字保持、新2后2367；676注册驱动/656独立E2E；754 Markdown路径与725保护记录保持。原273完整对象及22/166/70/15状态和269历史保持，新增1条partial后270，Goal active。直接master本地提交，无push/default/service promotion。

原53.45MiB分析/重建证据已按原字节压为5.70MiB，原始大JSON不再跟踪；本批也只新增[压缩机器证据](benchmarks/gist-read-owner-20261105.json.gz)，采用内容去重与逐字节roundtrip，不提交ELF/对象/原始堆镜像。18份旧gzip及清单记录保持，新1后19；Git已有历史未重写。

仍有提升空间：GiST仍整文件校验/线性解码而非平衡树，当前读每次验证耐久高水位，catalog身份检查、解析与事务开销仍存在；SERIALIZABLE/历史/自身写入不走新文件路径。空文本完整表示、PG tree/opclass/KNN/WAL、跨AM更细谓词依赖、成本校准及未转换typed统计消费者均未完成。用户暂缓模块保持原范围，未把局部改进宣布为PostgreSQL全部功能或性能等价。


<!-- CURRENT_TYPED_PLANNER_STATISTICS_20261105 -->

## Typed ANALYZE 统计接入扫描成本（2026-11-05，P18-C6）

本批使普通等值扫描选择/索引行估计、NULL/非空与等值不等值选择率、二进制文本 LIKE 密度判断优先读取真实 `.analyzed_statistics`。换行、竖线、分号、逗号、Unicode、空串和文本 NULL 保持独立值；不再把适用 typed MCV 当作旧分隔文本解析。旧 `.stats` 格式和其他消费者未整体重写，本批仍是 partial。

### 实现与调研

[PostgreSQL REL_18_6 selfuncs.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/selfuncs.c)的 var_eq_const 使用 MCV 频率估计命中值，并将非 MCV 的非空剩余质量分配给剩余不同值；负 n_distinct 随人口估计缩放。[PG 行数估计说明](https://www.postgresql.org/docs/18/row-estimation-examples.html)提供相应算例。本批保留真实 binary32 频率，调用现有声明列比较语义；LIKE 只把完全覆盖的残余直方图桶与匹配 MCV 质量相加。此密度下界用于扫描保护，不是完整 PG patternsel、相关性或成本模型。[SQLite ANALYZE](https://www.sqlite.org/lang_analyze.html)说明统计供规划器选择路径，也明确分析不保证所有查询更快。此前 DuckDB 索引/LIKE 调研继续作为历史参考；本批 DuckDB 请求只取得重定向页，不声称获得新的实现资料。

新读取 API 返回拥有整个不可变快照代次的列租约。单引擎只缓存一个当前统计快照及一个私有只读冷catalog代次，仍被调用方持有的旧快照继续存活；每次检查路径、设备/inode、长度、修改/变更时间及快照头/代次/存储校验值，重载时完整解码校验。小型 schema 文件每次重新哈希。验证当前 catalog 关系/属性、物理关系、schema、真实 builtin 标量类型、typmod、target、collation 和槽比较算子；不匹配、外部写者 fence、缺失或损坏时放弃可选证据，保留现有后备路径。查询取消继续抛出；pg_stats 的严格错误路径保持。

事务内显式 ANALYZE 现在使用已有 exclusive DDL 快照及“先恢复快照、再撤销行”的协调方式；两份统计文件随 ROLLBACK 恢复。它需要数据库级临时物理备份，未实现 PG 的统计 catalog MVCC，也未证明这条事务路径更快。没有修改 R291 Runtime/SQL 统计发布失败重试或通用原子 I/O/回滚协调器。

### 严格验证与保留失败

最终专项原生 3292 检查，另独立 cold exec 1454 检查通过；覆盖完整值/NULL、真实扫描、旧槽丢失、文件只读、关系/属性/type/operator/schema/target/collation 不匹配、foreign writer fence、rename/重新分析、DROP/同名重建和旧租约寿命。组件模型 O2 及 ASan/UBSan（含泄漏检测）各通过选择率 8235、读取缓存 2080 检查；只代表概率模型及文件帧缓存两个组件，不含真实catalog租约路径或整库sanitizer。正常 CMake C++17 构建保留 62 个真实对象和 61 个配套 core 对象；初次共享头变化重编译 13 个 TU，后续各重编译一个 TU，另显式重编译匹配桩，未混用旧类布局。

最终 C6 的48个原生入口均正常0。此前原始48项C4批次为 47 正常 0、1 夹具在独占写者期间构造第二引擎而触发已有启动清理错误；专项修订为预先构造peer，再只测试新读取API的fence，C4专项及最终C6整批通过，原始失败不抹掉。新协议 65 命令/15 实际扫描，旧 LIKE 协议 123/27 均通过。相关 32 个旧协议为 29 正常 0，name 257/70 failed、timetz 2563/314 failed、table_structured 表达式别名排序失败的完整观察与 P17 父代相同；不称全套通过。

最终C6三个自动分析夹具均正常0。此前C4自动数组/range两个原夹具首次并行执行在 ROLLBACK 超时，scalar 同批通过；随后预定的每项“父代首轮／C4／同一父代回测”六次均使用原 deadline 正常 0，没有修改旧夹具或超时设置。首次超时完整保留，后续通过不能反推其根因。7 个恢复 worker 正常 0，共 2477 检查，14 个完整 stdout/stderr 保留。

真实 PG18.6 新私有目录、en_US.UTF-8、默认 fsync 对照共享 33 命令，6732 完整逻辑字段及 NULL/OID/命令标签一致，1 个 EXPLAIN ANALYZE 实际行数一致；包含事务内 ANALYZE 后回滚的逻辑结果。排除本系统统计扫描计数、auto_analyze 和原生 GiST(text) DDL；不是 PG AM 路径/成本/速度等价。

开发失败也保留：C1 仅靠文件时间戳的缓存模型在快速同长度覆盖时错误命中，C2 纳入帧代次/校验值并重新哈希 schema 后通过；C1/C2/C3 协议暴露事务内分析后两份文件未恢复，最终 C6 通过相同逐字节断言；新原生夹具还保留早期编译错误及误用 prepared 入口期待普通等值索引的失败。最终同一协议源码在 P17 父代仍因热点 240/320 行走索引而失败。未恢复 R151 原341/103矩阵或用户暂缓模块。

### 未接受为性能终版的C4

C4的固定九进程低频LIKE wall中位数（旧首轮／C4／旧回测）为416.102／2514.770／435.596ms，服务CPU为370.992／699.905／376.196ms，CPU相对首轮增加88.66%。第一after进程还出现不存在等值10.746s、主键5.720s等长尾，不能删除或把后来的通过当作其根因证明。代码中acquire单独使用会使冷catalog保持未缓存；C5尝试复用可变服务缓存，但原等值冷启动夹具的“规划不引入可变catalog缓存”断言失败，故未验收，也未修改原夹具。C6在统计读取器内持有一个私有const catalog代次，仅在服务无model且本调用无独占写事务时复用，每次检查数据库device/inode与原publicationState的catalog文件代次；其他情形仍从服务取得拥有的当前租约，不使用旧缓存代替脏内存元数据。C4的冷catalog诊断夹具曾要求服务缓存命中并失败；该策略随后被旧冷启动契约否定。最终C6保留同一旧等值源码的服务缓存未填充断言，并通过独立cold105检查；新API夹具也验证这一纯读取边界。C4九次全部样本、扫描/文件/帧验证及负数继续保留；下表是代码修改后新预定C6九进程，仍无计时重试。

### 性能方法与完整样本

固定顺序（父代首轮、最终 C6、同一父代回测）×3，9 个独立正常服务进程；无计时重试。4096 行×3列，512 NULL 键、3200 个相同热点换行/分隔符键；真实 B-tree/GiST、显式 ANALYZE，按乘37置换插入。热点两类各2调用，其他各8调用；表构造、全量行对照、每例预热、扫描计数和文件核验在时钟外。时钟内包含普通 SimpleQuery 的服务解析/绑定/规划/真实访问/编码及完整 socket 回复接收，验证解码在时钟外。CPU 用活服务进程 POSIX CPU clock，另记客户端接收 CPU。OS cache 未清除或固定；其他系统工作未隔离。每组只有3个样本，所有尾部与负数保留，不作总体统计推断。机器/编译器/动态库与CPU affinity记录见压缩包environment-v1.json；未固定CPU频率或绑定CPU。

独立原始帧解码复核 740313 字段、23706 NULL、324 次计时回复、18 次完整人口回复、54 对原始扫描计数；核验 36 个保留物理文件哈希及测量前后全部堆与两份统计字节。它没有独立解码所有存储页结构。父代两类热点走索引，C6 两类热点走堆；其余索引路径保留。

正数表示中位数减少，相对首轮／回测：热点前缀 73.72%／73.25%；热点等值 51.25%／15.72%；低频前缀 58.53%／-8.58%；低频等值 44.30%／38.10%；不存在等值 24.66%／24.31%；主键控制 30.77%／29.00%。只代表此负载；低频、缺失、主键控制的开销和退步同样报告，不推广为全部数据库操作加速。

### 完整 socket wall（毫秒，每格为 min / median / max）

| 查询 | 每进程调用数 | 旧版首轮 | 最终 C6 | 同一旧版回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 1725.904 / 1745.523 / 1787.565 | 441.667 / 458.797 / 508.247 | 1689.671 / 1714.916 / 1757.050 | 73.72% | 73.25% | -1.75% |
| 热点等值 | 2 | 1012.864 / 1515.802 / 1750.875 | 615.734 / 738.895 / 886.184 | 803.467 / 876.742 / 1117.647 | 51.25% | 15.72% | -42.16% |
| 低频前缀 | 8 | 420.600 / 1270.995 / 5602.311 | 416.897 / 527.043 / 1280.050 | 425.504 / 485.391 / 5670.306 | 58.53% | -8.58% | -61.81% |
| 低频等值 | 8 | 68.344 / 138.015 / 5013.753 | 72.471 / 76.876 / 77.206 | 66.288 / 124.192 / 5032.973 | 44.30% | 38.10% | -10.02% |
| 不存在等值 | 8 | 66.784 / 111.643 / 111.677 | 66.990 / 84.108 / 118.277 | 59.854 / 111.120 / 694.104 | 24.66% | 24.31% | -0.47% |
| 主键控制 | 8 | 68.937 / 109.323 / 111.119 | 71.546 / 75.685 / 106.765 | 59.835 / 106.599 / 108.463 | 30.77% | 29.00% | -2.49% |

### 服务进程 CPU（毫秒，每格为 min / median / max）

| 查询 | 每进程调用数 | 旧版首轮 | 最终 C6 | 同一旧版回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 1515.170 / 1521.303 / 1572.575 | 381.541 / 391.242 / 439.313 | 1546.477 / 1612.014 / 1664.335 | 74.28% | 75.73% | +5.96% |
| 热点等值 | 2 | 743.425 / 904.886 / 906.110 | 527.068 / 538.256 / 557.547 | 735.396 / 799.965 / 829.814 | 40.52% | 32.72% | -11.59% |
| 低频前缀 | 8 | 382.257 / 519.055 / 735.725 | 379.554 / 483.626 / 595.784 | 381.848 / 404.238 / 712.171 | 6.83% | -19.64% | -22.12% |
| 低频等值 | 8 | 30.104 / 46.713 / 83.133 | 32.889 / 37.230 / 41.012 | 31.023 / 40.333 / 89.687 | 20.30% | 7.69% | -13.66% |
| 不存在等值 | 8 | 28.728 / 56.445 / 68.891 | 29.850 / 44.467 / 68.253 | 27.219 / 64.930 / 67.789 | 21.22% | 31.52% | +15.03% |
| 主键控制 | 8 | 29.300 / 57.172 / 64.299 | 29.029 / 35.438 / 68.271 | 26.739 / 66.265 / 66.396 | 38.01% | 46.52% | +15.91% |

### 客户端接收 CPU（毫秒，每格为 min / median / max）

| 查询 | 每进程调用数 | 旧版首轮 | 最终 C6 | 同一旧版回测 | 相对首轮减少 | 相对回测减少 | 旧版回测漂移 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点前缀 | 2 | 68.949 / 69.600 / 87.444 | 67.209 / 69.953 / 75.539 | 76.483 / 82.362 / 85.756 | -0.51% | 15.07% | +18.34% |
| 热点等值 | 2 | 71.165 / 86.877 / 94.524 | 73.298 / 74.364 / 74.550 | 69.778 / 73.660 / 79.599 | 14.40% | -0.96% | -15.21% |
| 低频前缀 | 8 | 1.536 / 1.802 / 2.109 | 1.651 / 1.693 / 1.715 | 1.760 / 1.768 / 1.934 | 6.06% | 4.22% | -1.92% |
| 低频等值 | 8 | 1.407 / 1.540 / 1.891 | 1.588 / 1.596 / 1.666 | 1.571 / 1.699 / 1.748 | -3.64% | 6.05% | +10.32% |
| 不存在等值 | 8 | 1.421 / 1.491 / 1.582 | 1.400 / 1.462 / 1.480 | 1.435 / 1.499 / 1.585 | 1.98% | 2.47% | +0.51% |
| 主键控制 | 8 | 1.551 / 1.750 / 1.971 | 1.410 / 1.494 / 1.517 | 1.541 / 1.703 / 1.879 | 14.63% | 12.25% | -2.71% |

### 版本与范围

最终私有程序 SHA256 `355bbe7c6ba7e9460b4309b5c9969d9454d10dd6f508407caf34b71dbac22f1b`；默认程序仍为 `425165afe19d10ccf9be806c3052e9a09e3875a3826142d2949cde995fdf94d1`。原273完整对象与22/166/70/15状态、268条历史保持，新增1条partial后269；原2361测试逐字保持，新4后2365；源248/头177，754 Markdown 路径/725保护记录不变。675注册驱动/655独立E2E完整。17旧gzip与清单项保留，新1后18；[完整压缩证据](benchmarks/typed-planner-statistics-20261105.json.gz)使用内容去重及逐字节 roundtrip，不提交原始大 JSON/ELF/对象/数据库镜像。

仍未完成：typed range/array/join/多列统计与大部分 histogram 消费、所有冷读人口后备、任意用户自定义类型/算子、保留参数计划、PG GiST 树/opclass/相关性、完整 SQL/catalog MVCC/成本校准。旧 `.stats` 格式仍可能因分隔符失真，未转换消费者仍有局限。保留原用户暂缓范围；直接 master 本地提交，无 push/default/service promotion，Goal active。


<!-- CURRENT_LITERAL_LIKE_SCAN_20261105 -->

## LIKE前缀的真实索引读取与密集前缀成本（2026-11-05，P17-C6）

普通无参数SELECT中的TEXT/VARCHAR字面量LIKE现在可实际使用已有GiST侧索引。已绑定AST提供真实关系/列身份；前缀范围只筛选候选，原类型化LIKE、AND剩余条件和NULL语义继续复查。转义百分号/下划线、多字节ESCAPE、UTF8上界以及打开前删除索引均有回归。可用ANALYZE的MCV与完整覆盖直方图桶识别密集前缀，按真实表extent和成本参数保留堆扫描；未知探测至少计1行成本。

同一4096行×3列、父代／最终C6／同一父代回测各3进程交错运行；wall中位数变化（正数为减少，相对首轮／回测）：热点前缀（3200行）6.88%／-3.83%；低频前缀（1行）68.01%／67.77%；不存在前缀（0行）69.40%／68.72%；显式ESCAPE前缀（1行）71.84%／71.06%。仅代表本负载；C3热点超过3倍耗时的失败尝试完整保留。

最终C6通过新原生310检查、原13068与独立冷读105检查、37个相关原生入口；纯前缀组件O2和ASan/UBSan各72567检查，启用泄漏检测。新123条协议命令含27次实际扫描断言；32个相关旧协议入口29正常0、3项严格失败与父代完整观察相同。真实PG18.6以两个会话重放共享65条SQL，935个完整字段/NULL/OID逐项相同，3个EXPLAIN ANALYZE实际行数相同；未比较PG的AM路径、成本或速度。

### 实现与主源调研

[PG18.6 like_support.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/like_support.c)从模式导出必要索引条件，并保留原模式复查；[SQLite LIKE优化](https://www.sqlite.org/optoverview.html#the_like_optimization)同样要求合适的声明、模式和排序语义。[DuckDB LikeOptimizationRule](https://github.com/duckdb/duckdb/blob/main/src/optimizer/rule/like_optimizations.cpp)识别常量、前缀等模式并重写表达式。这些是实现调研，没有运行SQLite/DuckDB速度对照。PG主源/COPYRIGHT、DuckDB固定commit源码/LICENSE与SQLite页面及SHA随归档保留；不是直接移植这些数据库的索引实现。

只从当前Select真实source occurrence、scopeDepth=0且非mergedUsing的已绑定列提取必要条件；需要原生text/varchar声明、无参数帧、非分区、无生成/域/数组及可用现有索引，并沿用RLS边界。只有UNKNOWN字符串字面量及binder的内置隐式text包裹可供检查；不执行参数、例程、子查询、显式CAST或任意常量树。OR/NOT等不能证明的条件保留原路径。AND可取其中必要的LIKE前缀，整个原谓词仍由既有PreparedFilter执行，没有改provider或retained-plan生命周期。

LIKE前缀按Unicode字符和实际ESCAPE解析；整个模式均检查，尾部非法escape或无固定前缀时不提前改变错误需求。范围上界保持有效UTF8，跨U+00FF/U+07FF/代理区及U+10FFFF正确进位；GiST包含式上界可能多取一个候选，由原谓词排除。原生Condition路径也使用同一规则，修正此前把下划线或转义符当作固定前缀而漏行的情况。GiST在打开时没有探测到索引则读取完整可见堆；既有前后snapshot fence、旧快照/读己写fallback、TOAST、NULL与损坏错误仍在原执行路径。

GiST的JSON EXPLAIN现在有实际GiSTScan节点和条件字段；索引条件内控制字符正确转义。显示成本仍为旧候选上界模型，不是完整PG cost/选择率模型。新前缀成本只读取现有native .stats的可用MCV与完整覆盖桶；不把跨桶位置当作精确行数，不把未知探测当成0成本。未接入typed .analyzed_statistics槽，未修旧统计持久化格式或R291发布/重试问题。[PG costsize.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/path/costsize.c)的clamp_row_est同样对普通估计使用至少1行；这里保留实际空结果，不把成本下限写成实际行数。

### 正确性与完整观察

最终C6通过新原生310检查、原13068与独立冷读105检查、37个相关原生入口；纯前缀组件O2和ASan/UBSan各72567检查，启用泄漏检测。新123条协议命令含27次实际扫描断言；32个相关旧协议入口29正常0、3项严格失败与父代完整观察相同。真实PG18.6以两个会话重放共享65条SQL，935个完整字段/NULL/OID逐项相同，3个EXPLAIN ANALYZE实际行数相同；未比较PG的AM路径、成本或速度。

本批私有普通后端C6 SHA256：e106a4c3c1e855892a5a7bdc410d4818191e20440416969a1f85fdc6c14559c0。

新原生同源父代、新旧候选与最终版本的完整输出均保留。直接原生API、普通SQL、text/varchar值、真实JSON节点、通配符/ESCAPE、全部行/NULL/空串/字面NULL/UTF8/换行、密集前缀成本切换及建计划后DROP INDEX均检查。新普通协议还验证真实seq/idx计数、AND/OR、NOT/ILIKE/非固定前缀和列模式、别名、旧快照历史键、读己写与DROP后的读取。强制低random_page_cost用于功能路径验证，另恢复普通成本验证密集前缀回堆；SQL语义不要求每种LIKE必须使用索引。

补齐7项缺失E2E注册（本批1项、此前6项），原有667项顺序及其余构建脚本逐字保持；现在674个唯一入口覆盖654份独立E2E文件，库存检查通过。补跑新增调度的3个统计auto协议均正常0，未运行完整注册套件。

37个相关原生入口实际链接最终C6同一61核心对象；恢复入口含7个exec隔离worker、1974检查和14份完整子进程输出。纯组件SAN只覆盖该前缀组件，不是全库SAN。32个旧协议保留name257控制70失败、timetz2563控制314失败及table_structured表达式别名排序原断言；完整观察与父代相同（仅name私有UUID归一），未删改原2358个测试。原R151整个NetworkServer.cpp保持，原341/103矩阵未运行。

最初C1的Unicode上界性质测试及真实JSON文本校验失败，C2改为有效Unicode上界；C3再统一原生模式提取和JSON转义。C3实际网络计时显示稀疏前缀获益、热点因大量随机堆读取退化，最终加入密度成本选择。C4编译因使用C++20的starts_with而被项目实际C++17配置拒绝，未生成新程序/对象；C5改用兼容比较。C5协议成本控制又暴露旧.stats换行记录导致槽不完整、未知密度直接跳过成本比较，C6补成本下限。诊断和所有失败保留，未冒称修复旧格式。一次37项驱动误用了不存在的桩对象，只有编译/链接失败、没有实际测试退出；更正路径后在最终C6重新构建并运行。当前生产和原生配置仍为C++17，收益不是语言标准升级。

PG18.6/en_US.UTF-8私有集群默认fsync，init/start/stop实际0；共享65条SQL包括完整模式结果、三个ANALYZE实际行数、两个真实会话RR和自身写入。排除58条DBMS专有auto_analyze、访问计数及本地GiST(text) DDL；没有把它们翻译成声称相同的PG索引。无ORDER BY按完整多重集比较，NULL和重复完整保留。

### 真实网络成本

两轮均固定9个独立服务，(父代、候选、同一父代回测)×3；同一4096行/3列/512NULL键/3200热点键，i×37置换插入，真实GiST与ANALYZE。热点每段2次，低频/不存在/显式ESCAPE各8次。计时从SimpleQuery发送至完整网络收包，包含解析/绑定/物理源/完整谓词/投影/编码；建表、装载、统计、完整总体、每项一次预检与计数读取在时钟外。运行时验证全部堆和两种统计映像逐字不变，实际扫描增量逐项记录。

所有值为整段毫秒中位数[min,max]；服务CPU取存活整个服务进程POSIX CPU clock，另保留客户端CPU。OS缓存未清空或固定，只有每组3个进程，所有尾部保留，无计时查询重试。不能将它们推广为所有操作、统计显著性或PG速度。

| C3未做密度选择 wall（ms） | 父代首轮 | 候选 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点前缀（3200行） | 515.511 [511.283, 528.725] | 1672.119 [1666.821, 1685.991] | 463.215 [459.018, 526.455] |
| 低频前缀（1行） | 1512.279 [1435.194, 1552.228] | 440.717 [416.462, 452.562] | 1516.853 [1457.653, 1524.777] |
| 不存在前缀（0行） | 1506.485 [1442.421, 1546.203] | 436.849 [416.215, 458.133] | 1444.483 [1410.042, 1535.446] |
| 显式ESCAPE前缀（1行） | 1844.807 [1808.003, 1859.143] | 444.208 [427.079, 444.820] | 1890.533 [1838.830, 1890.625] |

| 最终C6 wall（ms） | 父代首轮 | 候选 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点前缀（3200行） | 503.333 [496.029, 519.791] | 468.726 [459.447, 1273.544] | 451.419 [439.751, 458.533] |
| 低频前缀（1行） | 1452.066 [1443.031, 1573.533] | 464.535 [410.993, 5152.559] | 1441.343 [1414.375, 1458.524] |
| 不存在前缀（0行） | 1457.111 [1432.544, 1479.739] | 445.888 [420.630, 4060.971] | 1425.428 [1420.883, 1607.018] |
| 显式ESCAPE前缀（1行） | 1807.962 [1752.703, 1880.186] | 509.105 [423.692, 2359.130] | 1759.097 [1736.511, 2898.080] |

| 最终C6 server process CPU（ms） | 父代首轮 | 候选 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点前缀（3200行） | 445.300 [435.961, 471.037] | 394.886 [384.146, 509.872] | 393.527 [364.385, 432.037] |
| 低频前缀（1行） | 1392.850 [1378.555, 1457.672] | 397.430 [370.164, 768.994] | 1379.956 [1345.988, 1394.235] |
| 不存在前缀（0行） | 1386.687 [1355.736, 1412.784] | 382.361 [376.702, 727.983] | 1362.046 [1338.449, 1372.523] |
| 显式ESCAPE前缀（1行） | 1705.883 [1677.427, 1772.100] | 443.488 [380.616, 719.382] | 1689.967 [1676.036, 1960.649] |

热点前缀（3200行）：wall相对首轮/回测变化（正数为减少）6.88%/-3.83%，基线漂移-10.31%；server CPU相对首轮/回测变化（正数为减少）11.32%/-0.35%，基线漂移-11.63%；

低频前缀（1行）：wall相对首轮/回测变化（正数为减少）68.01%/67.77%，基线漂移-0.74%；server CPU相对首轮/回测变化（正数为减少）71.47%/71.20%，基线漂移-0.93%；

不存在前缀（0行）：wall相对首轮/回测变化（正数为减少）69.40%/68.72%，基线漂移-2.17%；server CPU相对首轮/回测变化（正数为减少）72.43%/71.93%，基线漂移-1.78%；

显式ESCAPE前缀（1行）：wall相对首轮/回测变化（正数为减少）71.84%/71.06%，基线漂移-2.70%；server CPU相对首轮/回测变化（正数为减少）74.00%/73.76%，基线漂移-0.93%；

unsuccessful_C3_cost：9/9服务正常0，逐帧独立复核234计时回复、18完整总体、480870字段/18630NULL与36物理映像哈希。完整索引候选数和真正堆/索引扫描增量独立核对；不冒称独立解析全部存储页。

cost：9/9服务正常0，逐帧独立复核234计时回复、18完整总体、480870字段/18630NULL与36物理映像哈希。完整索引候选数和真正堆/索引扫描增量独立核对；不冒称独立解析全部存储页。

### 尚未完成的范围与版本

Typed .analyzed_statistics optimizer consumption is still incomplete. Existing native .stats delimiter/newline values can make some legacy slots unreadable; dense-prefix costing uses available MCV/fully covered histogram buckets and a one-row unknown-probe floor, not a repaired persistence format.

This is not a full PG patternsel/opclass/correlation/startup model. The native GiST sidecar still scans its file linearly; cost calibration, full PG GiST trees/operator classes and typed histogram selection remain open.

Positive plain-string literal LIKE over the existing single physical text/varchar source is covered. Parameter frames, computed/nonconstant patterns, explicit CAST trees, ILIKE/NOT LIKE, nonbinary explicit collations and other source/type owners retain their existing path; no deferred retained/custom/generic/portal/format2 or mutation/provider lifecycle work resumed.

All short measurements, before-return drift and C3 hot regressions are retained; three processes per group are descriptive evidence, not statistical significance, all-operation throughput or PostgreSQL speed equivalence.

Original name257/70failed, timetz2563/314failed and table_structured expression-alias-sort strict observations remain parent-identical, not accepted full-suite success. Original R151341/103 matrix not run.

Original273 objects/status22complete166partial70unverified15deferred and267 histories remain exact; append1partial=>268.754MDpaths725protected,16oldgzip preserved. Goal active, direct master, no push/default/service promotion.

原273完整对象和22/166/70/15状态、267条历史保持，追加1条partial后268。原2358测试逐字保持，新3后2361；源246/头175，顶层CPP1104含桩/嵌套5。更新8份现有文档，无新增Markdown；754路径/725保护记录保持。旧16份gzip与清单项保留，新1后17，原大JSON压缩政策继续。直接master本地提交，默认425程序与服务保持；Goal active。
详见[性能报告](performance-report-2026-10-30.md)与[压缩原始证据](benchmarks/like-prefix-scan-20261105.json.gz)。本批仍为partial，Goal active。


<!-- CURRENT_ORDINARY_QUERY_PROFILE_20261105 -->

## 普通查询的规划元数据与解析开销（2026-11-05，P16-C2）

本批在一次普通查询规划内复用关系schema，并按需各读取一次B-tree/Hash/Bloom/GiST列清单；主键与不可能使用位图/GiST的条件省去无用发现。普通表达式先排除不可能的类型常量前缀，避免对数字、表达式分隔符前的列名做推测类型解析及异常处理。真正的限定/引号/修饰/复合类型常量与显式CAST仍使用原声明语法。

交错的8倍调用补测，父代／最终C2／同一父代回测各3进程；wall中位数变化（正数为减少，相对首轮／回测）为：热点等值（3200行）12.15%／-1.12%；低频等值（1行）15.17%／18.32%；不存在等值（0行）17.94%／14.60%；主键等值（1行）25.52%／19.68%。全部短时及补测的正负结果保留，不能推广为全部操作或PG速度。

最终C2通过原生13068与独立冷读105检查、32个规划/存储入口和14个语法入口；另2个冷目录原生入口保留父代完全相同的严格失败。新65条协议命令含10个实际扫描断言，原有73条等值协议通过；另30个相关旧协议入口27通过、3个严格失败与父代完整观察相同。实际PG18.6的共享25条SQL仍为7331/7331逻辑单元格、9/9行估计一致，8/9路径相同；不是完整兼容验收。

### 实现与调研

[PostgreSQL REL_18_6 plancat.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/util/plancat.c)的get_relation_info打开关系并从关系描述符取得索引清单，为规划构造关系信息。本批借鉴一次规划持有关系元数据的方法，不移植PG共享relcache、锁或失效生命周期；原始源码、COPYRIGHT、来源哈希随归档保留。此前[SQLite规划器](https://www.sqlite.org/queryplanner.html)与[DuckDB索引指南](https://duckdb.org/docs/2.0/guides/performance/indexing.html)的ANALYZE/选择率调研仍适用，没有新增这两个数据库的速度测量。

PlanningRelationMetadata只在buildSelectPlan/buildDisjunctiveSelectPlan栈上存在，引用该调用已取得的schema，懒加载索引列清单；下游算子仍拥有自己的schema副本，并在open时执行原校验。原统计成本、路径优先级、快照fence、MVCC、NULL、谓词复查和RLS路径保持。新查询重新取得元数据，未增加跨语句缓存，未改prepared/retained/custom/generic、portal/参数/format2、mutation/provider所有权。

解析预判只作用于普通表达式的推测类型常量入口，标识符起始规则与原声明解析相同。数字/字符串/运算符本身不能引入类型名，后继逗号、结束括号、二元运算符或语句末尾也不能构成类型常量；点号、修饰括号、数组后缀和复合类型词仍进入原语法。显式CAST、::、DDL、类型规格解析未改，没有实现R106未选显式CAST输入分析，也未恢复R303-R305规划/portal模块。

### CPU定位与工具边界

perf实际探测因perf_event_paranoid=4拒绝，保留退出255，没有修改内核权限。gprofng两次smoke存在timer changed to 0警告、采样数量与CPU工作量不符，其原始实验/日志保留而不作性能依据。gperftools 2.15项目局部包按APT元数据SHA256核验；仅为诊断预加载libprofiler，没有替换分配器。按[官方CPU profiler说明](https://gperftools.github.io/gperftools/cpuprofile.html)，用SIGUSR2在装载/预检后开始、1024条实际点查结束后停止，采样100Hz；计时版本不预加载profiler。

共8个有效诊断服务全部正常0，每份1024条查询的全部结果与NULL、前后4096行总体以及堆/两种统计映像逐字检查。另保留最初一次因清理前缀校验失败的profile；该次没有完整持久化trace/正常退出收据，不计入8个有效服务。符号被剥离的系统库别名不能用来断言DNS或具体分配器实现问题。以下inclusive比例会相互覆盖，不相加，也不直接换算为查询加速：

| 程序／查询 | 样本数 | getTableSchema | buildSelectPlan | domainColumns |
| --- | ---: | ---: | ---: | ---: |
| before／主键等值（1行） | 935 | 17.3% | 10.7% | 14.0% |
| before／低频等值（1行） | 1004 | 15.7% | 9.9% | 12.2% |
| legacy／主键等值（1行） | 847 | 13.1% | 未采到该符号 | 8.7% |
| legacy／低频等值（1行） | 881 | 10.7% | 未采到该符号 | 8.7% |
| C1／主键等值（1行） | 864 | 13.5% | 5.6% | 10.5% |
| C1／低频等值（1行） | 1016 | 12.7% | 4.0% | 9.0% |
| C2／主键等值（1行） | 757 | 13.5% | 5.7% | 9.5% |
| C2／低频等值（1行） | 856 | 15.2% | 5.0% | 11.7% |


legacy是P14-C6普通读取的额外定位对照；C1只改规划，C2再加类型常量前缀预判。legacy没有采到buildSelectPlan，保留缺失而非填0。当前仍有前端重复解析、catalog publication-state文件探测、事务开始/提交和索引页开销，未用本批局部改动宣称这些已解决。

### 正确性与原有差异

全部最终原生入口实际链接最终C2同一61个核心对象和固定桩。恢复入口包含7个exec隔离worker、1974检查，14份完整子进程输出均保留。16个语法入口两侧各14通过；declared_type_cold_binding的12检查中2失败、declared_modifier_owner的50检查中1失败，完整stdout与父代逐字相同，涉及createDatabase之后冷目录前提；没有删改原2357测试或把它们算作通过。

新永久协议测试65条覆盖B-tree/Hash/Bloom/GiST创建与删除后的实际seq/idx计数、完整NULL/空串/字面NULL/UTF8/换行、列重命名、同名DROP/recreate及类型OID。父代、C1、最终C2均通过，证明元数据按查询重取；性能变化由独立计时判断。初稿在父代和C1都错误要求LIKE必走GiST，完整失败保留；最终保留LIKE完整行和OID检查，用真实可达GiST范围检查扫描路径，未宣称修复普通LIKE前缀路径。

30个旧协议入口27正常0；另name257控制70失败、timetz2563控制314失败、table_structured表达式别名排序原断言失败。前两者完整stdout、后者完整stderr与父代相同（仅name私有UUID归一），均保留为未解决缺口。原R151整个NetworkServer.cpp逐字保持，原341/103矩阵未运行。

实际PG18.6/en_US.UTF-8私有集群正常init/start/stop、默认fsync，25条共享SQL完整重放；最终C2的7331个逻辑字段逐字相同，9个估计一致，8个路径相同，另1处成本临界TableScan/PG Index Scan差异保留。新DBMS专有AM/实际计数测试单独验收，不假称在PG等价重放。

### 真实普通查询网络计时

同一4096行×3列，512个NULL键、3200热点键、384低频键，i×37置换物理插入、真实B-tree与ANALYZE。每个worker从创建独立数据目录开始；计时包括实际SimpleQuery解析、绑定、规划、读取/谓词/投影/结果编码与完整收包，建表/装载/ANALYZE、完整总体检查及每种查询1次预检在计时外。记录wall、整个存活服务进程POSIX CPU clock与客户端CPU；数据预热，OS缓存未清空或固定，无prepared或PG速度计时。

初轮C1与最终C2短时负载各固定9worker，先父代3、候选3、同一父代回测3；hot每段3次，其余每段32次。C2短时热点与低频退化，基线/长尾波动也很大。为检查短时测量影响，预先固定且只执行一轮8倍调用补测：hot24次，其余256次，顺序为(父代、C2、同一父代回测)×3。程序、SQL、数据、物理形状、setup、预检和计时范围保持；调用数/顺序变化明确记录。不以补测替换短时失败，不重试或排除任何计时样本。第6个服务在监听前绑定端口失败、没有计时回复，完整退出1收据/日志保留；前5个已完成worker全部原样复用，仅继续剩余4个预定worker，并改用INADDR_ANY探测非临时端口。完整计时仍为原定9个不同进程，没有重新测量前5个。

以下为整段毫秒中位数[min,max]；每组仅3个进程，是描述性观察。所有三批原始wall/server CPU/client CPU及长尾随归档保留。

| P16-C1短时，3/32次 wall（ms） | 父代首轮 | 候选 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 603.699 [581.287, 674.654] | 599.267 [597.605, 604.763] | 597.247 [596.579, 616.828] |
| 低频等值（1行） | 343.187 [327.724, 16473.993] | 529.245 [296.835, 801.557] | 509.506 [362.990, 529.516] |
| 不存在等值（0行） | 494.879 [417.338, 564.122] | 500.067 [492.772, 556.020] | 552.100 [502.328, 801.817] |
| 主键等值（1行） | 521.214 [428.240, 800.574] | 495.525 [485.458, 545.308] | 533.792 [517.819, 534.520] |

| P16-C2短时，3/32次 wall（ms） | 父代首轮 | 候选 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 607.154 [582.502, 677.691] | 903.702 [642.922, 2666.118] | 585.053 [583.929, 620.282] |
| 低频等值（1行） | 428.734 [351.654, 501.554] | 664.554 [595.751, 2342.746] | 450.427 [397.896, 682.224] |
| 不存在等值（0行） | 450.654 [432.784, 579.846] | 529.739 [397.449, 624.435] | 590.226 [580.169, 787.387] |
| 主键等值（1行） | 508.477 [485.227, 536.439] | 508.396 [488.923, 529.415] | 529.979 [436.913, 538.599] |

| P16-C2短时，3/32次 server process CPU（ms） | 父代首轮 | 候选 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 512.734 [474.962, 524.117] | 586.487 [481.094, 694.197] | 521.146 [481.815, 524.820] |
| 低频等值（1行） | 260.689 [172.809, 310.050] | 310.115 [303.609, 340.559] | 264.007 [230.005, 285.699] |
| 不存在等值（0行） | 267.610 [240.328, 373.881] | 308.402 [224.388, 324.468] | 361.557 [356.033, 367.670] |
| 主键等值（1行） | 305.020 [274.229, 319.668] | 265.750 [264.463, 278.201] | 325.877 [277.311, 329.723] |

| P16-C2交错补测，24/256次 wall（ms） | 父代首轮 | 候选 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 5570.150 [4714.484, 12811.399] | 4893.189 [4890.154, 5112.705] | 4838.782 [4838.546, 4994.860] |
| 低频等值（1行） | 4859.875 [4132.848, 10984.664] | 4122.803 [4056.771, 4234.366] | 5047.797 [4213.065, 9433.966] |
| 不存在等值（0行） | 4862.499 [3926.394, 5220.796] | 3990.219 [3535.916, 4292.758] | 4672.141 [4219.512, 4815.719] |
| 主键等值（1行） | 4550.086 [3855.577, 5019.247] | 3388.742 [3085.793, 3733.774] | 4219.060 [4010.085, 4464.040] |

| P16-C2交错补测，24/256次 server process CPU（ms） | 父代首轮 | 候选 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 4450.316 [3875.270, 4562.222] | 4220.047 [3769.108, 4442.241] | 4201.012 [4184.779, 4268.056] |
| 低频等值（1行） | 2627.073 [2321.486, 2652.095] | 2223.358 [2121.337, 2308.888] | 2806.386 [2539.808, 2828.936] |
| 不存在等值（0行） | 2699.923 [2220.372, 2709.729] | 2102.919 [2076.208, 2198.733] | 2599.074 [2181.074, 2650.435] |
| 主键等值（1行） | 2384.693 [2369.785, 2438.140] | 1804.192 [1660.140, 1821.477] | 2307.013 [2265.669, 2369.264] |

热点等值（3200行）补测：wall相对首轮/回测变化（正数为减少）12.15%/-1.12%，基线前后漂移-13.13%；server CPU相对首轮/回测变化（正数为减少）5.17%/-0.45%，基线前后漂移-5.60%；

低频等值（1行）补测：wall相对首轮/回测变化（正数为减少）15.17%/18.32%，基线前后漂移+3.87%；server CPU相对首轮/回测变化（正数为减少）15.37%/20.78%，基线前后漂移+6.83%；

不存在等值（0行）补测：wall相对首轮/回测变化（正数为减少）17.94%/14.60%，基线前后漂移-3.91%；server CPU相对首轮/回测变化（正数为减少）22.11%/19.09%，基线前后漂移-3.74%；

主键等值（1行）补测：wall相对首轮/回测变化（正数为减少）25.52%/19.68%，基线前后漂移-7.28%；server CPU相对首轮/回测变化（正数为减少）24.34%/21.80%，基线前后漂移-3.26%；

cost_C1：9/9服务正常0，独立逐帧检查891个计时回复、18个完整总体、568566个字段/20322个NULL和36份映像哈希。

cost_short_C2：9/9服务正常0，独立逐帧检查891个计时回复、18个完整总体、568566个字段/20322个NULL和36份映像哈希。

cost：9/9服务正常0，独立逐帧检查7128个计时回复、18个完整总体、2395062个字段/55854个NULL和36份映像哈希。

运行中的worker逐字验证全部堆和两种统计映像前后不变；独立复核另验证保存映像的长度/哈希，不冒称独立解析全部存储页。没有全库SAN或全操作吞吐结论。没有同时改善两组基线的场景仍开放，不能用局部采样占比或平均值覆盖退化。

### 尚未完成的范围与版本

Ordinary query CPU still includes repeated frontend parsing, catalog publication-state file probes, transaction setup/commit and index-page work. This batch does not change catalog publication retry, generic I/O atomicity or any deferred owner.

Whole-query timing is descriptive, three processes per group, with baseline drift and all tails retained; no global throughput, every-operation or PostgreSQL speed claim. Any case without improvement against both baseline groups remains open.

Ordinary LIKE-prefix frontend eligibility and expression-alias sorting remain open. The corrected new metadata fixture keeps full LIKE rows/types and uses an actual eligible GiST range to assert GiST execution; no claim of a LIKE access-path implementation.

Typed .analyzed_statistics optimizer consumption, full PG cost calibration, multicolumn/partition statistics and index-family costing remain partial. PG25 shared commands match7331 cells/9estimates but one of9 scan choices differs.

Two original cold-catalog native fixtures and three original protocol entries remain strict failures with complete parent-identical observations; not counted as accepted full-suite success.

273 original objects/status22complete166partial70unverified15deferred and266 histories remain exact, append1partial=>267. All user-deferred scopes including R106/R151/R303-R305 retained; original R151341/103 matrix not run. Goal active, direct master, no push/default/service promotion.

原273完整对象和22/166/70/15状态、266条历史保持，追加1条partial后267。原2357测试逐字保持，新1后2358；源245/头174、顶层CPP1102含桩及嵌套5。更新8份现有文档，无新增Markdown，754路径/725保护记录保持；旧15个gzip与清单项保留，新1后16。原53.45MiB JSON已在452fe958无损压缩为5.70MiB，未改写Git历史；本批仍只提交压缩证据，不提交程序、对象或数据库映像。直接master本地提交，默认425程序与服务保持，Goal active。
详见[性能报告](performance-report-2026-10-30.md)与[压缩原始证据](benchmarks/ordinary-query-profile-20261105.json.gz)。本批是普通读取partial进展，Goal active。


<!-- CURRENT_EQUALITY_SCAN_COST_20261105 -->

## 普通等值查询的实际扫描选择与完整网络成本（2026-11-05，P15-C7）

本批让普通原生标量堆表查询实际消费基于ANALYZE频率的扫描选择：热点等值可选顺序扫描，低频等值和主键保留索引路径。此前只修改规划器时，无ORDER BY的普通查询仍绕开物理计划；现在结构化输出已接通，并修复无可用索引OR的显示文本合并，以保留重复行、NULL、空串和换行。B-tree、位图AND/OR与GiST在探测前取得语句快照，并在快照不适合当前索引或探测期间提交边界改变时读取完整可见堆候选，再做原条件检查。

这是普通读取的partial进展，使用现有原生 `.stats` 中的MCV/计数；尚未让优化器全面消费新的typed `.analyzed_statistics` 槽。普通EXPLAIN的文本缓存键现包含精确的实时成本设置；没有修改已暂缓的retained/custom/generic规划、参数、portal/format2或mutation/provider生命周期。当前私有普通后端P15-C7 SHA256为 `3ff5f16a9cc3a223d6618db87d8a5bbe85240dfd4c08cfdceafeb6ba49c03d3a`。默认根程序425与既有服务保持；Goal active，PostgreSQL整体功能仍partial。
同一4096行×3列、同一真实SQL与完整网络返回，父代／最终C7／同一父代回测各3进程，固定9个完成测量的进程，不重试计时查询；另两次在基线装载/启动阶段失败、没有计时样本，完整保留。wall中位数相对首轮父代／回测的变化（正值为减少、负值为增加）：热点等值（3200行）12.51%／2.45%；低频等值（1行）-5.57%／-5.47%；不存在等值（0行）77.31%／77.36%；主键等值（1行）-11.11%／9.44%。正负变化均如实保留；仅代表这四个普通读取负载，不能推广为全部操作、通用吞吐或PG速度。

新原生13068检查与独立冷读105检查通过，纯频率组件O2及ASan/UBSan各17检查（启用泄漏检测）。24个相关原生入口实际正常0，含完整7worker恢复1974检查和14份子进程完整输出；这些入口实际链接最终C7的61个核心对象。新73条普通协议命令正常0，验证实际扫描计数、全部行值/NULL及旧快照；26个旧协议入口中23正常0，另3保留与父代一致的严格失败，未称全套通过。真实PG18.6对共享25条命令的7331个逻辑单元格全部逐字相同，9个EXPLAIN行数相同，8个扫描选择相同、1处成本边界扫描选择不同。
### 实现与研究依据

[PG18行数估计](https://www.postgresql.org/docs/18/row-estimation-examples.html)和[REL_18_6 selfuncs.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/selfuncs.c)按MCV频率、NULL比例和剩余不同值总体估计等值选择率，再由[costsize.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/path/costsize.c)比较候选路径。[SQLite查询规划](https://sqlite.org/queryplanner.html)使用ANALYZE辅助索引选择；[DuckDB索引指南](https://duckdb.org/docs/2.0/guides/performance/indexing.html)强调高选择性，ART扫描默认阈值为MAX(2048,0.001×行数)。这些是实现调研，不是SQLite/DuckDB性能测量；原始PG源码、COPYRIGHT和来源哈希随归档保留。

本地共用等值频率组件，使用列的真实比较语义合并匹配MCV，非MCV使用剩余非NULL频率/不同值，并受最小MCV频率约束。统计缺失、零样本或语义不能确定时保持旧选择，输入错误仍交实际所选执行路径处理。当前行数优先既有运行时估计，页数读取真实表空间的物理extent；不扫描元组来规划，不新增全局统计缓存、不改磁盘统计格式或发布重试策略。真正单列主键使用唯一性已保证的最多1行，省去每次为此重新读取/解析MCV，但仍比较实际顺序/索引成本；不跳过成本GUC或自定义回调。已有bitmap/GiST优先级保留，未声称完成这些索引族的完整成本比较。

普通原生标量查询沿既有物理Project/Filter发布完整单元格与NULL元数据；支持当前基础堆表、普通条件与现有安全边界。缺索引的OR改用同一堆源加DNF过滤，避免按投影文本去重、丢失重复物理行和拆分换行。数组、枚举、域、虚拟列、分区、DISTINCT/ties、表达式排序及已有输出需求边界仍走原拥有者，未重新定义已暂缓的执行图生命周期。

当前版本索引不能代表旧快照、读己写或并行写入中的全部历史键。物理B-tree/bitmap/GiST先取得语句快照，再在探测前后核对既有提交fence；不安全或发生变化时收集完整可见堆RID，并由原谓词和MVCC读取检查。实际扫描计数按真实fallback记为seq，缺失B-tree文件仍报原XX001，不用fallback掩盖损坏。位图OR在原条件复查时绑定实际RID的NULL元数据，并在正常/异常退出恢复之前的绑定，防止旧快照fallback把整数NULL当作0。GiST补齐TOAST值解析；权限/行策略仍由既有关系级路径处理。

普通EXPLAIN文本缓存键包含五个成本参数的classic-locale hexfloat精确值与三个join开关，原生customCost回调存在时不缓存该显示结果。重复同一SQL的两个接近成本值可实际跨过扫描边界，避免六位小数碰撞。这只修普通EXPLAIN显示缓存，不是已暂缓的参数/保留计划策略。

### 正确性、参考数据库与原有严格差异

1024行实表含128个NULL键、800个热点键和96个低频键，打乱物理插入顺序；原生13068及独立exec冷读105检查核对真实数据、全量行/NULL、MCV行数、页成本、自定义成本、冷热选择、等价整数字面值及规划读取不修改文件。父代用同一最终原生源失败于热点仍选索引且只估10行，现估800行；热点有界q-error由80变1。O2和ASan/UBSan各17仅证明纯频率组件，不是全库SAN。

新73条普通后端协议命令验证原有有序查询、SET成本切换与精确边界、全部数据/NULL/空串/UTF8、无ORDER BY真实seq/idx计数、未索引OR的重复行与嵌入换行，以及B-tree、位图AND/OR、GiST旧快照读取。当前索引候选命中与更新提交后旧键不可由sidecar找回的场景均有实际结果和计数；旧快照三个侧索引路径各完整读取4个可见堆候选，不漏历史行。

24个相关原生入口全部正常0；全部实际链接与普通C7后端一致的61个核心对象。完整7worker恢复1974检查及14份原始子进程stdout/stderr保留。26个相关旧协议入口保留全部结果：23正常0；name257控制有70处严格失败、timetz2563控制有314处严格失败，与父代完整观察相同（仅name私有命名空间随机UUID归一）；table_structured仍在表达式别名排序处失败，与父代同一原断言和结果相同。未修这些既有缺口，未宣称整个注册套件通过；原R151 guard逐字保持，原341/103矩阵未运行。

旧analyze_null协议fixture原来期待pg_stats返回0A000，与已经接受的P14完整17列契约冲突；原始fixture在父代和本批均失败的证据保留。仅此旧fixture按真实17列、全NULL、SQL NULL/空串/字面NULL槽值加强检查，更新后在父代及候选均通过，其余2353个原测试文件逐字保持。没有删除失败断言来宣称产品通过。

真实PG18.6/en_US.UTF-8私有集群、Unix socket、默认fsync，init/start/stop正常0；共享原26条命令前缀中仅排除DBMS专有auto_analyze设置，实际重放其余25条。7331/7331结果单元格逐字一致，9/9行估计一致，8/9扫描路径一致；1处本地TableScan/PG Index Scan差异保留。新增DBMS专有计数/侧索引快照扩展单独验证，未伪称这部分也在PG重放。EXPLAIN显示cost仍有旧row-touch单位，不能等同选择路径时的成本函数或PG真实cost；现有GiST显示Unknown与OR解释/实际路径不同仍开放。

初版C1在SET后暴露普通EXPLAIN缓存键缺成本；C2只改变规划/显示，实际无序查询仍绕开物理路径。C3接通后暴露bitmap旧快照漏行；C4暴露取得快照过晚、缺文件被fallback掩盖；C5扩展回归暴露未索引OR显示文本合并的真实回归；均保留失败；C6随后被新增诊断发现旧快照bitmap OR把NULL当作0，最终C7修复该回归并新增3条永久协议检查。C7的73条最终源在父代失败于热点计划，在C6失败于NULL/0历史键；最终同源全部通过。非存在的两个猜测测试名、C5脚本误指不存在的stub对象、程序复制前误启动协议、参考工具语法/数量错误及原始资料下载403属于工具/准备失败，未计为成功产品测试。

### 完整普通网络查询性能

基线为已提交187ea9c5的真实P14-C6普通后端；候选为本批最终C7；回测重新使用同一基线程序。固定顺序3/3/3共9个完成测量的独立进程，不重试计时查询、不丢计时样本。此前两次在首个基线装载/启动阶段失败，均未产生计时区间：一次4096行INSERT超过15秒且清理后-9，另一次20秒内未连上监听。失败receipt/trace保留，未归因或修复任何已暂缓模块。最终全组统一装载超时120秒、计时查询超时仍15秒；另保留每进程启动前身份和完整服务日志，并用新socket轮询启动。此为测量工具变化，未改生产程序或SQL/数据/调用次数。每个进程相同SQL创建4096行×3列（512个NULL键、3200个热点键），按i×37置换物理插入顺序，真实B-tree与ANALYZE。热点查询每段3次，低频/不存在/主键每段各32次。时钟覆盖真实SimpleQuery解析/绑定、路径构造、堆/索引/MVCC/谓词、完整投影与网络帧接收；建表/插入/ANALYZE/完整总体检查/各1次预检在计时外。没有prepared、EXPLAIN或PG速度计时。

所有时长是整段毫秒中位数[min,max]；另保留客户端CPU，server CPU取运行中整个服务进程的POSIX CPU clock。数据预热与操作系统缓存未清空/固定；每组仅3个样本，属于描述性观察，不证明统计显著性。

| wall（ms） | 父代首轮 | 最终C7 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 686.919 [638.433, 783.425] | 600.959 [596.125, 2112.871] | 616.033 [577.750, 1462.805] |
| 低频等值（1行） | 385.685 [343.042, 563.188] | 407.175 [337.976, 8772.021] | 386.042 [341.942, 9398.978] |
| 不存在等值（0行） | 2525.870 [2521.379, 2536.569] | 573.072 [351.712, 577.666] | 2531.696 [2531.333, 2765.847] |
| 主键等值（1行） | 423.599 [379.460, 797.582] | 470.663 [414.416, 520.251] | 519.747 [286.821, 593.138] |

| server process CPU（ms） | 父代首轮 | 最终C7 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 586.126 [524.728, 639.621] | 532.585 [532.266, 679.087] | 509.493 [501.335, 700.270] |
| 低频等值（1行） | 211.017 [185.482, 351.169] | 222.052 [163.658, 434.809] | 224.742 [177.837, 369.335] |
| 不存在等值（0行） | 2270.426 [2252.394, 2350.438] | 342.512 [186.015, 349.670] | 2331.174 [2296.963, 2352.068] |
| 主键等值（1行） | 197.241 [186.199, 259.039] | 262.186 [229.659, 333.830] | 207.463 [132.933, 282.647] |

热点等值（3200行）：wall相对首轮/回测变化（正值为减少）12.51%/2.45%，基线前后漂移-10.32%；server CPU相对首轮/回测变化（正值为减少）9.13%/-4.53%，基线前后漂移-13.07%；

低频等值（1行）：wall相对首轮/回测变化（正值为减少）-5.57%/-5.47%，基线前后漂移+0.09%；server CPU相对首轮/回测变化（正值为减少）-5.23%/1.20%，基线前后漂移+6.50%；

不存在等值（0行）：wall相对首轮/回测变化（正值为减少）77.31%/77.36%，基线前后漂移+0.23%；server CPU相对首轮/回测变化（正值为减少）84.91%/85.31%，基线前后漂移+2.68%；

主键等值（1行）：wall相对首轮/回测变化（正值为减少）-11.11%/9.44%，基线前后漂移+22.70%；server CPU相对首轮/回测变化（正值为减少）-32.93%/-26.38%，基线前后漂移+5.18%；

先前C2版本同一9进程负载未加速；其所有数据和长尾完整保留，不能用规划器局部成功覆盖实际网络退化：

| C2未加速尝试wall（ms） | 父代首轮 | C2 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 657.148 [621.542, 742.096] | 664.710 [652.927, 732.987] | 629.390 [611.446, 648.152] |
| 低频等值（1行） | 451.329 [406.964, 483.361] | 956.542 [428.602, 17135.114] | 528.777 [526.154, 532.991] |
| 不存在等值（0行） | 2650.274 [2638.014, 2896.516] | 3104.175 [2558.123, 4297.617] | 2684.749 [2668.547, 2813.185] |
| 主键等值（1行） | 454.191 [329.396, 840.172] | 661.778 [609.910, 694.801] | 471.063 [356.758, 612.920] |

中间C6同负载9次也全部保留：其后发现与本负载不同的NULL/0快照回归，因此不是接受版本；低频/主键的性能退化亦未隐藏。

| C6中间版wall（ms） | 父代首轮 | C6 | 同一父代回测 |
| --- | ---: | ---: | ---: |
| 热点等值（3200行） | 643.928 [633.557, 711.806] | 622.917 [604.314, 633.639] | 690.935 [633.629, 719.963] |
| 低频等值（1行） | 867.124 [858.245, 870.216] | 940.001 [863.956, 967.482] | 863.084 [811.219, 899.013] |
| 不存在等值（0行） | 3050.418 [3008.481, 3078.710] | 971.995 [893.259, 1021.908] | 2903.748 [2870.686, 2961.412] |
| 主键等值（1行） | 822.599 [762.834, 843.955] | 872.958 [867.883, 905.790] | 769.605 [741.387, 788.662] |

C2的低频查询约17.135秒、最终C7约8.772秒及其基线回测约9.399秒单段长尾也保留，不作为重试或排除依据。最终负载的查询、数据、调用次数、setup SQL、计时/收包范围与C2尝试相同；替换最终普通后端与对应验收前置条件，并采用上述统一装载超时及启动日志。

三批各9/9服务正常0。每批独立解码891次计时回复、18次完整总体回复及全部预检，检查568566个真实字段/20322个NULL、真实字段OID、每行全部值及空串/UTF8；逐一复核36份保留物理映像的字节/哈希。每worker在运行时逐字检查全部堆文件和两种ANALYZE映像读取前后不变；独立复核验证保留映像哈希，不冒称独立解析了全部存储页格式。所有9个计时、CPU、长尾和原始wire帧可由压缩归档复核；不提交ELF、对象文件或完整数据库映像。

### 后续门槛、文档与版本

Measured finalC7 workloads without improvement against both baseline groups remain open: hot server_process_cpu_ns: change percentages (positive=faster) 9.13/-4.53; cold wall_ns: change percentages (positive=faster) -5.57/-5.47; cold server_process_cpu_ns: change percentages (positive=faster) -5.23/1.20; primary wall_ns: change percentages (positive=faster) -11.11/9.44; primary server_process_cpu_ns: change percentages (positive=faster) -32.93/-26.38. Do not relabel a regression as a win or replace it with an overall mean.

Full PostgreSQL cost calibration/startup/order/correlation/caching/visibility estimates and typed .analyzed_statistics slot consumption, extended/multicolumn/inheritance/partition statistics and index-family path costing remain partial. This implementation consumes the existing native .stats MCV/counts only.

One of9 PG scan choices differs at a close cost boundary; actual logical7331 cells and9 estimates match. Display costs retain older row-touch units and are not the exact choice-cost model.

Existing GiST JSON/text node rendering and OR EXPLAIN physical-plan divergence remain open, with actual counters retained.

All operations/general throughput/PG speed and full database sanitizers unmeasured. Current scalar physical bridge excludes arrays/enums/domains/virtual/partition rows and unsupported output-demand/expression-order/distinct/ties owners.

Pre-existing strict name DDL and array/protocol phase controls, and the existing expression-alias sort mismatch, remain failed and independently equal to parent observations; no user-paused owners reopened.

All dedicated user pauses and original15 deferred items unchanged. Retained/generic/parameter planning, portal/format2 ownership, mutation graph/provider lifecycle, shared CPU sort and publication failure/retry scopes not changed.

原273个完整对象与状态22 complete/166 partial/70 unverified/15 deferred_by_user不变；原265条历史逐字保留，只追加1条partial后266。原2354测试仅上述旧NULL契约fixture调整，其余2353逐字保持；新增3后2357，顶层CPP1102含桩、嵌套CPP5。当前源245/头174。更新8份现有入口文档，不新增Markdown；754路径与725份保护记录保持。旧14份gzip归档及清单项逐字保留，新1份后15；原大JSON的452fe958压缩提交保持，不重写历史。直接master本地提交，不推送、不切换默认程序/服务，Goal active。

原始测量、失败与独立复核见[压缩机器归档](benchmarks/equality-scan-cost-20261105.json.gz)和[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_PG_STATS_SOURCE_20261105 -->

## pg_stats 完整基础查询与读取成本（2026-11-05，P14-C6）

本批接通实际 `pg_catalog.pg_stats` 的17列基础SQL查询、参数为零的纯Describe、文本/二进制/混合结果格式，以及实际 owning anyarray 输出；支持已测的过滤、投影、排序、LIMIT、别名、CTE和JOIN。每次查询只读取当前数据库的真实统计与目录，重用本次查询内的关系/属性索引和每关系一次的物理模式校验，减少逐列重复扫描、模式复制和文件散列。

本节更新下方P13当时“17列SQL尚未接通”的结论；下方历史批次保留当时事实，当前状态以本节为准。当前私有普通后端P14-C6为 `9362b02aba17d45038702211ba42a6a1d797ef3580d97edb8ffabce80e0f63b6`。默认根程序425和既有服务未切换，Goal保持active；整体PostgreSQL功能仍为partial。
同功能未索引C2／最终C6／同一C2回测各3进程，每进程先1次不计时完整查询，再计时3次完整17列SQL查询API。16张表×30列×32行，实际512行/15360存储单元格/1696个NULL；每次查询返回480行×17列。wall中位数384.334／245.178／379.841ms，降低36.21%／35.45%；process CPU中位数392.205／254.151／387.508ms，降低35.20%／34.41%。这是该负载的完整原生SQL API对照，不是任意查询、网络端到端、已发布父代或PostgreSQL速度对照。所有9个样本及长尾保留。

新原生109检查与独立冷读90检查、投影组件O2和ASan/UBSan各548检查通过；34个相关原生入口全部正常0，包含完整7worker恢复1974检查及14份完整子进程输出。普通后端新协议26条SQL及旧标量25/数组19/范围19条自动统计协议通过。真实PG18.6同26条协议命令991个字段值：938逐字相同、53处严格差异保留；17列二进制153项中145相同、8处为实际存储宽度差异，六种范围的24个直接二进制值全部相同。另510个有限float4输入的文本和二进制身份均与PG一致。没有全库SAN或整体PG等价结论。
### 生产实现与研究依据

PG18把pg_stats定义为pg_statistic之上的视图，通过槽角色公开17列，并以anyarray输出不同实际元素类型；本地沿用“真实统计采集/持久化”和“目录视图消费”分离的结构。来源为[PG18 pg_stats契约](https://www.postgresql.org/docs/18/view-pg-stats.html)、[REL_18_6系统视图](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/catalog/system_views.sql)、[伪类型输出实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/pseudotypes.c)及[float4最短输出实现](https://github.com/postgres/postgres/blob/REL_18_6/src/common/f2s.c)。原始代码、COPYRIGHT和来源哈希随证据归档保存；既有[SQLite ANALYZE](https://www.sqlite.org/lang_analyze.html)和[DuckDB ANALYZE](https://duckdb.org/docs/stable/sql/statements/analyze.html)的采集/消费调研继续适用，本批没有两者性能测量。

CREATE DATABASE现在由创建者在成功返回前发布真实系统目录。pg_stats具有实际view OID、独立复合行类型、17个真实属性及真实字段OID/宽度/对齐；新增2277 anyarray物理内置身份和协议名称映射。旧库的可变初始化拥有者在文件锁内初始化并发布这些已分配身份。纯绑定、Describe和冷目录读取不调用该初始化，不分配OID、不插入可变缓存，也不写目录文件；旧库缺少这些真实声明时仍需实际可变初始化，不能由冷查询虚构视图。

来源读取真实relation/attribute/namespace及类型声明，匹配实际物理关系与模式指纹；沿用已有读权限/行策略边界，不新增ACL/RLS语义。关系、命名空间、属性索引和每关系的物理模式指纹只属于本次读调用，不进入全局、会话或provider缓存。新读来源沿用既有查询图，不改已暂缓的DML/provider生命周期。旧8列原生兼容渲染API保留。

槽1/2/3/4/5/6/7分别投影MCV/直方图/相关性/数组成员/元素计数/范围长度与空比例/范围边界。NULL、缺席槽、空数组、数组下界、NULL位图、实际元素/域OID和已拥有的嵌套数据结构被保留，不从显示文本重建数组或记录。静态anyarray声明保持元素未定，每个实际输出携带真实元素声明；新增格式1编码仅从该实际拥有者取得元素身份，并单独拒绝已暂缓的顶层枚举/枚举数组家族。原R151 guard函数体逐字保持，未运行其原341/103矩阵。

float4采用无locale的最短往返输出与PG中点排除/科学计数阈值；float8沿用既有输出。实际内置六种范围编码保留empty、包含关系、无界、实际日期/时间无穷、numeric精度和epoch微秒。选中的字符串输入anyarray分别严格报0A000/42846；NULL及来自视图的anyarray恒等转换通过。此处未完成一般多态强制转换、未选中CAST输入分析或参数/portal扩展。

### 正确性、实际PG对照和已知差异

新原生测试验证实际建库、17列空描述符、真实DDL与数据、NULL/数组/范围槽、别名/CTE、持久身份及独立exec冷读；109+90检查正常0。父代用相同最终测试源在缺少新建库声明时失败，原先直接SQL缺失42P01的证据也保留。投影组件O2和ASan/UBSan各548检查，启用泄漏检测，仅代表该组件。原有三个目录测试显式构造“旧库无目录映像”场景，保留全部原始断言并增加准备检查；其余2348个原测试文件逐字保持。不能把原生断言打印通过但程序退出非0计为通过。

普通C6后端26条新协议SQL验证真实17列描述符/原始来源身份、153个完整二进制单元格、文本/混合格式、别名列表、纯Describe不改统计文件、零行、CTE/JOIN/count、anyarray恒等转换/NULL和两种输入错误。6种范围24个普通二进制单元格逐字等于真实PG；所有协议服务最终正常0。34个相关原生入口全部正常0，包括完整7worker恢复1974检查与全部14份stdout/stderr；旧标量/数组/范围自动统计的COMMIT、UPDATE、DELETE、ROLLBACK和target变化继续通过。

真实PG18.6私有en_US.UTF-8集群保持默认fsync，只使用私有Unix socket；init/start/26条相同协议SQL/补充实际目录观察/stop均正常结束。991个实际字段值逐字比较，938相同，53处差异全部保留：48处重复查询中的本地实际存储宽度、4处tstzrange文本保留输入+01而PG的UTC会话显示+00、1处本地分配view OID与PG固定身份的数值差异。首个完整二进制结果153项中145相同，8处均为宽度；时间偏移对应的二进制值一致，未用此理由抹去原文本失败。

225个描述符字段逐项保留原始值，除两侧实际view OID分别核对外，另有4处属性来源差异：既有pg_class的3列缺失实际属性来源，以及anyarray恒等CAST的来源为0而PG保留原view属性8。名称、类型OID、宽度、typmod和格式其余相同；4处来源差异仍开放。510个有限float4输入独立核对原始bits和PG文本，510/510一致。上述通过不替代原有P9/P13的严格差异，也不证明完整目录、时区显示、存储格式或PG等价。

初版C1缺失实际2277内置目录导致原生失败和普通启动-6；补齐后C2仍因旧静态数组guard无法输出未定元素而失败。C3暴露SimpleQuery来源OID为0，C4暴露字符串anyarray输入被接受，C5扩展NULL用例暴露类型回退text，分别在C4/C5/C6修正。最初全NULL整数宽度测试预期0被真实PG的4纠正；三个旧fixture的“目录应不存在”假设与计数按新创建契约调整，原断言保留。命名空间36检查后的非0退出源于仍期待35的返回条件，不能误报为已证明的并发/关闭故障。PG参考工具的信任认证/无结果描述符假设、bytes键SQLSTATE解码、首版64列benchmark超过实际30列限制及独立帧解析器的NUL长度错误均记录为工具问题，不是成功的性能样本或产品等价证据。

### 同功能完整SQL API性能

对照为本批完整查询API未索引C2 `40ec4438…` 和最终C6 `9362b02a…`，不是缺少17列来源的父代。C2的二进制网络输出在该阶段尚不完整，因此只将已通过完整原生105+冷读90检查的同功能查询API用作基线；不称其为同功能完整网络后端。两版benchmark源完全相同，C++17/O2/ICU/zlib和显式既有TLS stub相同；普通网络验收使用各自真实普通主程序。CMake分阶段实际执行并保留全部62对象前后哈希与61个原生core血缘，不冒充全对象清理重编或真实TLS验收。当前生产源码244文件/173头，旧12文件修改、新增5文件。

固定C2前／C6／同一C2回测，各3个新进程；每个进程真实建立16张30列的表，各32行，包含整数、UTF8/空/字面NULL文本、NULL/空/下界0数组、int4range/巨大numrange/带偏移微秒tstzrange。实际512行、15360存储单元格和1696个NULL。先真实ANALYZE，再执行1次不计时完整预检，然后计时3次完整prepareBoundQuery→buildPreparedQueryPlan→executePlanChecked，返回480×17个拥有数据和结构化文本。证明序列化、全行/RID/NULL核对与文件比较均在计时外；准备/预检会触及缓存，OS缓存未清除或固定。共27次计时查询与9次不计时预检，无重新采样。

| 指标，ms／3次完整查询：p50 [min, max] | 未索引C2前 | 最终C6 | 同一C2回测 |
| --- | ---: | ---: | ---: |
| wall | 384.334 [364.288, 426.656] | 245.178 [221.084, 249.545] | 379.841 [367.061, 390.046] |
| process CPU | 392.205 [381.276, 434.369] | 254.151 [228.846, 269.319] | 387.508 [376.077, 398.272] |
| thread CPU | 384.315 [364.281, 426.527] | 243.201 [221.069, 249.468] | 379.812 [366.999, 390.039] |

wall中位数降低36.21%／35.45%，基线前后漂移-1.17%。process CPU中位数降低35.20%／34.41%，基线前后漂移-1.20%。调用线程CPU中位数降低36.72%／35.97%，基线前后漂移-1.17%。每段仅3样本，属于描述性统计，未证明统计显著性或所有负载提速；426.656ms等原始长尾保留。

9/9实际进程正常0。9份341692B全堆证明、1508041B完整查询证明、780277B统计快照和299520B旧统计文件分别逐字一致。独立脚本重读全部9份原始计时、512个物理行/RID、15360个单元格/NULL、480条真实统计和8160个查询字段；验证FNV/帧长度、真实类型、数组结构/下界/NULL位图及每个槽的值，而非只相信汇总。查询没有修改原数据或两种统计文件。本结果不能推广为全部数据库操作提速或PG速度对照。

### 后续门槛、文档和版本

完整pg_statistic目录消费、一般多态转换、无操作CAST/pg_class来源、所有时区文本显示、重命名/模式变更后的统计连续性、间接用户视图路径及优化器实际消费仍开放。当前按完整模式指纹拒绝过时记录，重命名等变更可能使统计在重新ANALYZE前不可见；不能等同PG完整DDL生命周期。原生多范围、自定义范围、范围数组、枚举/记录元素、继承/分区/索引统计、两文件共同原子性、完整恢复/事务语义及所有用户暂缓事项保持开放；未据局部通过关闭整个功能族。

原273项完整对象和状态22 complete /166 partial /70 unverified /15 deferred_by_user不变；原264条历史逐字保留，新加1条partial后265。原2351个测试文件中2348逐字保持，仅上述3个旧库fixture调整；新增3个测试后2354，顶层CPP1100含桩（去桩1099），另5个子目录CPP。更新8份现有入口文档，Markdown路径仍754，725份保护记录逐字保持，不新增Markdown。旧13份gzip归档逐字保持，本批新增1份后14份；机器证据仅以标准gzip入Git，含原始/压缩哈希，不提交ELF、对象文件或数据库映像。既有大JSON压缩提交452fe958保留，未重写Git历史。直接master本地提交，Goal active，默认程序和既有服务保持。

原始测量、失败及逐字复核见[压缩机器归档](benchmarks/pg-stats-source-20261105.json.gz)与[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_RANGE_COLLECTOR_20261105 -->

## 范围统计采集与完整 ANALYZE 成本（2026-11-05，P13-C2）

本批新增六种内置范围类型int4range/int8range/numrange/daterange/tsrange/tstzrange及实际范围域的ANALYZE采集：独立排序下界和上界、保存范围边界与长度直方图、空范围比例、NULL比例和实际存储宽度。边界保留包含关系、无界端点和子类型语义；本次样本局部缓存解析键，避免排序时反复解析。

最终私有普通后端P13-C2为 `3593b23e5e142605fe71149daec3347015003fb7a84d8a2505c80578e4e77a5d`，默认根程序425保持。完整pg_stats目录/17列SQL及output-only anyarray尚未接通，两条实际探针仍严格失败0A000。多范围尚缺真实目录/存储拥有者；本批仍为采集阶段的partial进展，Goal active。
本批同功能未缓存C1／缓存C2／同一C1回测，各3进程；每进程2048行、1个整数和六种范围，先1次不计时预检，再计时3次完整ANALYZE API。process CPU中位数388.224／201.363／391.997ms，降低48.13%／48.63%；wall中位数441.019／250.031／456.067ms，降低43.31%／45.18%。所有9次原始测量均保留；这不是任意SELECT、父代已发布后端或PG速度对照。

原生范围845检查及冷读262通过；组件O2和ASan/UBSan各43检查（启用泄漏检测，非全库SAN）。21个相关原生入口含完整7worker恢复1574检查全部正常0。原数组786+冷读191、标量641+冷读2、范围19条/数组19条/标量25条协议SQL通过。真实PG18.6同一520条SQL对照18列243项：229相同，14处严格差异保留（13处存储宽度、1处numeric NaN长度排序）。没有PG整体通过结论。
### 生产实现与边界

AnalyzedRangeInput只由实际物理输入拥有者调用；通过真实目录解析内置基类型及域，保留实际属性OID。AnalyzedRangeStatistics接受真实范围边界，由实际子类型比较、subdiff和范围构造回调产生槽7/6；槽7的元素是基范围类型，槽6为float8，长度比较算子672、collation0。域的实际属性身份和槽内基范围身份分开验证，未为缺失类型编造OID。

边界包含关系和上下界角色参与排序；上下界分别取分位点后组成范围。空范围不进入长度/边界直方图，空比例分母为非NULL范围数。全NULL列不造槽；空表不造记录；非空范围不足2个时，真实PG发布的长度值为SQL NULL，保留该缺席状态。target1保留2个分位点；450行真实reservoir只取300行，独立采样不强迫与PG选择相同端点。target0在分析后移除旧列记录。实际宽度来自本地存储跨度，不能将其解释为PG内存格式宽度。

数值边界使用实际Numeric语义，长度先作精确差再转float8；超出float8上下限均报22003。整数/日期按PG将两个操作数分别转double再相减，覆盖超过2^53的int8。时间戳先保留PG epoch微秒及实际无穷哨兵，再按PG的浮点操作数运算；普通timestamp保留墙钟语义，timestamptz保留绝对时刻。字面infinity与无界端点不同。组件覆盖微秒舍入、偏移、9999年精度以及NaN/Infinity。解析键只属于本次有界样本；不写入快照，不进入会话/provider或全局缓存。共享Numeric、既有比较缓存、TableManage.cpp、Main和网络源码保持。

研究来源为[PG18范围统计源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/rangetypes_typanalyze.c)、[PG18范围实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/rangetypes.c)及[pg_stats契约](https://www.postgresql.org/docs/18/view-pg-stats.html)。原始主来源、COPYRIGHT和哈希随归档保存。本批没有SQLite/DuckDB性能测量，既有统计采集与消费分离调研继续适用。

### 实际正确性和开放差异

同一最终永久原生源在父代实际建表、写入、ANALYZE后因缺少范围记录失败。最终C2实际845检查、冷读262正常0；六类范围包含NULL/empty/包含边界/无界/NaN/Infinity/巨大numeric/时间偏移与微秒，另有真实CREATE DOMAIN、VIRTUAL列跳过、目标和独立采样。两个numeric长度溢出/下溢负例与PG同报22003；原生验证失败前后旧.stats及新快照均逐字不变。此负例不代表所有I/O、分配失败或完整事务恢复验收。

初版C1原生脚本错误地沿用父代TableManage对象，实际失败和错误配置保留；纠正文件名后C1为844检查正常0。最终C2永久测试新增一条真实域目录断言，父代before-v3与C2使用相同测试源；520条正向SQL与C1完全相同。普通协议C1从开始即链接正确普通程序。一次独立对照脚本语法错误也保留，未进入任何产品比较。上述工具失败不冒充产品失败或通过证据。

范围19条普通协议SQL验证自动INSERT55、显式COMMIT110、UPDATE110、DELETE至60、回滚快照不变、target0/1及6份完整持久快照。另4条普通CREATE/INSERT/ANALYZE/SELECT覆盖NULL/empty/有界/无界，实际描述符OID3904/3906/3908及3列真实快照独立解码通过；live PID/exe核对最终C2，服务器正常0。完整17列pg_stats两探针仍0A000且无描述符；旧P9五项目录/输出身份差异未据此替代验收。

真实PG18.6私有en_US.UTF-8集群保持fsync，init/start/520正向SQL/两个预期22003/stop均正常结束。18列243项比较中229相同；13处宽度差异来自实际存储跨度。另一处为numeric含NaN的长度直方图顺序：PG18的该比较器对NaN使用“<／==／否则1”，没有给NaN定义严格顺序；本地C++排序使用NaN置后的严格顺序。两侧30个长度的完整多重集相同，但原严格顺序差异保持为失败项，不改成匹配。独立解码PG array_send/range_send及子类型二进制，逐项比较包含关系、无界和字面无穷、精确数值、float4统计、槽类型/算子/角色；实际域身份分别核对。未声明SQL二进制输出或整体PG等价。

### 同功能完整API性能

对照为本批同功能未缓存C1 `ea28f1a9…` 与最终缓存C2 `3593b23e…`；父代没有新范围采集，不能作为同功能提速基线。两版同一不可变benchmark源、C++17/O2/ICU/zlib、相同TLS stub和61个core对象血缘。两次普通CMake各真实重编TableManage一个TU，其余61对象核对；当前源239/头169，仅旧私有采集头变更，新增两个范围头文件。这不是全62对象重新编译或真实TLS运行验收。

每worker真实建表、事务写入2048行，1个integer及六种范围，共14336单元格、957个NULL范围单元格。包含empty、超过2^53的int8、巨大numeric的0.25差值、日期和带偏移微秒时间。准备和一次不计时完整预检会触及缓存，未清除或固定OS缓存。计时覆盖3次完整StorageEngine::analyzeTable，包含扫描、原统计和两种文件的正常持久发布；全数据/RID/NULL、旧.stats全部字节和新快照每个字段在计时外核对。固定前／新／回测各3进程，无重采，共27次计时调用和9次不计时预检。

| 指标，ms／3次完整ANALYZE：p50 [min, max] | 本批C1前 | 最终C2 | 同一C1回测 |
| --- | ---: | ---: | ---: |
| wall | 441.019 [434.795, 472.880] | 250.031 [247.782, 261.813] | 456.067 [450.075, 461.735] |
| process CPU | 388.224 [376.872, 410.179] | 201.363 [201.201, 203.299] | 391.997 [385.562, 398.716] |
| thread CPU | 386.467 [375.487, 408.789] | 200.863 [200.650, 202.251] | 390.438 [384.333, 397.316] |

wall中位数降低43.31%／45.18%，未缓存前后漂移+3.41%。process CPU中位数降低48.13%／48.63%，未缓存前后漂移+0.97%。调用线程CPU中位数降低48.03%／48.55%，未缓存前后漂移+1.03%。每段3样本仅为描述性统计，未证明统计显著性或所有负载提速；所有原始长尾保留。

9/9进程正常0。9份805158B全行证明及9份69467B最终generation4快照分别逐字相同。独立脚本重读9条计时、2048个真实RID/14336单元格/957个NULL、完整帧长度/版本/FNV，以及六类范围的每个边界分位点、长度分位点、空比例和实际类型身份；未只复核汇总json。本指标不能推广为任意SELECT、所有数据库操作或PG速度。

### 后续门槛和版本管理

完整pg_stats目录/view/attribute来源、output-only anyarray及普通0/1格式仍待接通。原生多范围缺少真实目录/存储类型，不能靠名称或假OID补上统计；范围数组、自定义范围、枚举/记录元素、完整继承/分区/表达式索引、全程DDL隔离、两文件共同原子性、恢复和完整事务回滚仍开放。已有用户暂缓边界全部保持。原273项22/166/70/15不变，263条历史逐字保留并加1条partial后264。原2348测试文件逐字保持，新增3后2351（顶层CPP1098含桩、去桩1097，另5个子目录CPP）；Markdown路径仍754，725保护记录保持。

本批新增机器证据使用标准gzip及原始/压缩哈希清单，总计13份；旧12份gzip载荷逐字保持，不提交未压缩大载荷。既有452fe958压缩提交保留，未重写Git历史。Goal保持active；直接在master本地提交，默认程序和现有服务未切换。

原始测量、失败和复核见[压缩机器归档](benchmarks/analyzed-range-collector-20261105.json.gz)及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_ARRAY_COLLECTOR_20261105 -->

## 数组统计采集与完整 ANALYZE 成本（2026-11-05，P12-C3）

本批把真实数组存储输入接到同一次ANALYZE采样：保存整数组MCV/直方图/相关性，以及常见元素、按非NULL数组计算的成员频率和不同元素数直方图。实际验证integer/text/numeric/boolean/float8数组的NULL、空数组、字面NULL、重复值、多维/下界和冷读；其他元素类型及全部PostgreSQL功能仍需分别验收。

最终私有普通后端P12-C3为 `7d3cc2a1353c105ecac62579a919ec96d088cd325fa74234bc47252061e1029a`，默认根程序425保持。完整pg_stats目录/17列SQL与output-only anyarray尚未接通，两条实际探针仍严格失败0A000；本批是采集阶段的partial进展，Goal active。
完整ANALYZE对照为本批同功能未缓存C2／缓存C3／同一C2回测，各3进程；每进程2048行、1个整数和3类数组，先1次不计时预检，再计时3次完整API。process CPU中位数1243.545／164.180／1296.411ms，降低86.80%／87.34%；wall降低85.86%／86.87%。最终版685.690ms wall长尾保留。此结果限于该负载，未测任意SELECT或PG速度。

原生数组786检查及冷读191通过；计算组件O2和ASan/UBSan各32检查（启用泄漏检测，非全库SAN）；17个相关原生入口含完整7worker恢复1574检查全部正常0。原标量641+冷读2、数组19条协议SQL和原标量25条协议SQL通过。真实PG18.6同一554条SQL对照14列325项：301相同，24处严格差异保留（11宽度、11collation OID、2处相同频率元素的截断选择）。没有PG整体通过结论。
### 生产实现与边界

新AnalyzedArrayStatistics只接收拥有值的真实数组及元素声明，不从显示文本猜测结构。物理输入拥有者沿用已有存储解码器，保留实际类型OID、维度/下界、NULL子元素和可证明的bitmap；空数组与NULL数组不同。整数组比较按实际元素语义、NULL顺序、元素数、维数、维长和下界排序，不使用数组字符串的字典序代替数值比较。

整数组沿用标量槽计算；专用采集按REL_18_6的Lossy Counting参数、每数组成员去重、target×10上限、最小/最大/NULL成员频率尾部和不同元素数分位点生成槽4/5。元素频率分母只计参与专用采集的非NULL数组。整数组阈值1024B、专用元素采集阈值65536B；阈值针对本地逻辑存储输入的长度，实际宽度仍取本地存储跨度/真实TOAST引用，不能当成PG内存格式的宽度。纯组件覆盖宽数组和跨bucket裁剪；这些阈值控制不冒充所有大数组物理存储路径验收。

采样继续复用同一扫描、有界reservoir和原始ordinal。450行/target1实际保留300行。数组MCV的槽元素身份为实际数组类型（如1007），常见元素槽为标量元素23，未发明数组的数组目录OID。目标0禁采并移除旧列记录，普通协议实际验证0→1恢复。

C1真实浮点数组分析报0A000：新构造的SQL描述符使用double precision/real，而物理比较器接受double/float。真实旧API诊断证实两者不同；C2改为复制实际物理元素描述。最终C3在本列/本次分析中复用已有AnalyzeComparisonCache，缓存解析键/locale；原始拼写只定位键，元素相等仍由真实比较语义决定。键数超过本列300×target上限或原缓存不支持该类型时，整列回到原比较器。没有进程/会话/provider缓存；旧比较缓存、TableManage.cpp、Main和网络代码逐字保持。

实现研究见[PG18数组统计源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/array_typanalyze.c)、[PG18数组比较源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/arrayfuncs.c)和[pg_stats契约](https://www.postgresql.org/docs/18/view-pg-stats.html)。原始主来源/COPYRIGHT及哈希随归档保存；本批没有SQLite/DuckDB性能测量，先前的采集与消费分离调研继续适用。

### 实际正确性和开放差异

同一永久原生源在父代真实建表、写入40行、ANALYZE后，第47次检查因缺少数组统计失败。C1在第43次检查报真实0A000，原数据和失败保留。修正后C2与最终C3各786检查正常0，各冷读191；五种元素类型包含numeric等价拼写/大数、浮点NaN/Infinity、空文本和带分隔符/UTF8文本，另有真正VIRTUAL列跳过。组件初版缺include目录、参考脚本拼接语法、相对路径复制和一次测试脚本错选来源/程序的失败均保留；不作为产品失败或最终通过证据。最终普通Main的live PID/exe已核对当前C3。

19条普通协议SQL验证自动INSERT55、显式COMMIT110、UPDATE110、DELETE至60、回滚快照不变、target0/1和6份完整持久快照。原25条标量协议仍过。另4条普通数组CREATE/INSERT/ANALYZE/SELECT的NULL/空值/字面NULL和两列实际OID1007/1009描述符通过，真实快照独立解码；完整17列pg_stats两探针仍0A000且无描述符，服务器正常0。旧P9五项描述符/目录身份差异没有被这四条数组SQL替代验收。

真实PG18.6私有en_US.UTF-8集群执行相同554条SQL，正常init/start/query/stop且保持fsync。14列325项中301相同，24处严格差异逐项保留：11处存储宽度、11处文本槽collation身份0/100，另两处为target×10截断边界上的相同频率元素选择不同。后两处独立核对各自30个值及对应频率：不同选择全部处于同一最低频率；没有把原严格差异改成匹配。独立随机样本只检查类型/角色/范围/频率/维度等不变量，不强迫两套采样端点或选值相同。未声明二进制SQL输出或整体PG等价。

### 同功能完整API性能

对照是本批正确的未缓存C2 `f3c581fa…` 与最终C3 `7d3cc2a1…`，父代后端不含数组采集，不能用作同功能提速基线。两版同一不可变benchmark源、C++17/O2/ICU/zlib/TLS stub及61个core血缘。3代普通CMake均真实只重编TableManage一个TU，其他61对象核对；源237/头167，旧236文件只变更私有采集头。

每worker真实建表、事务写入2048行、1个integer及integer[]/float8[]/text[]三类数组；638个NULL数组单元格。准备和一次不计时完整预检会触及缓存，未清除或固定OS缓存。计时覆盖3次完整StorageEngine::analyzeTable，包括扫描、原统计和两种文件的正常持久发布；全数据/RID/NULL、旧.stats全部字节和新快照每个字段在计时外核对。固定前／新／回测各3进程，无重采，共27次计时调用和9次不计时预检。

| 指标，ms／3次完整ANALYZE：p50 [min, max] | 本批C2前 | 最终C3 | 同一C2回测 |
| --- | ---: | ---: | ---: |
| wall | 1284.848 [1217.714, 1322.482] | 181.618 [174.083, 685.690] | 1382.728 [1319.314, 1411.373] |
| process CPU | 1243.545 [1198.176, 1290.624] | 164.180 [162.405, 294.679] | 1296.411 [1235.028, 1382.621] |
| thread CPU | 1239.348 [1194.163, 1286.183] | 163.520 [161.684, 291.675] | 1291.698 [1231.011, 1377.282] |

process CPU中位数降低86.80%/87.34%，调用线程CPU降低86.81%/87.34%，wall降低85.86%/86.87%。未缓存前后漂移分别为process CPU +4.25%、thread CPU +4.22%、wall +7.62%。C3第3样本wall685.690ms、process CPU294.679ms的长尾完整保留；每段3样本是描述性统计，不证明统计显著性或所有负载提速。

9/9进程正常0。9份247598B全行证明及9份6740B最终generation4快照分别逐字相同。独立脚本重读每条计时、2048个真实RID/8192单元格/638个NULL、全部帧长度/版本/FNV，以及每个数组成员频率、不同元素数直方图、拥有MCV的维度和类型；未只复核汇总json。本指标限于该同功能完整ANALYZE负载，不能推广为任意SELECT、所有数据库操作或PG速度。

### 后续门槛和版本管理

完整pg_stats目录/view/attribute来源、output-only anyarray及普通0/1格式仍待接通；范围/多范围、自定义/枚举/记录元素、完整继承/分区/表达式索引、全程DDL隔离、两文件共同原子性、恢复和完整事务回滚仍开放。所有已有用户暂缓边界保持，原273项22/166/70/15不变，262条历史保留并加1条partial后263。原2345测试文件逐字保持，新增3后2348（顶层CPP1096含桩、去桩1095，另5个子目录CPP）；Markdown路径仍754，725保护记录保持。

本轮另按用户要求以独立提交 `452fe958` 将11份旧机器证据无损压缩；所有解压字节与原哈希一致，旧历史保留。新增证据继续使用标准gzip及原始/压缩哈希清单，避免提交未压缩大载荷。Goal保持active；默认程序和现有服务未切换。

原始测量、失败和复核见[压缩机器归档](benchmarks/analyzed-array-collector-20261105.json.gz)及[性能报告](performance-report-2026-10-30.md)。


<!-- BENCHMARK_ARCHIVE_COMPRESSION_20261105 -->

## 测试证据压缩（2026-11-05）

原始测量、失败记录和源码证据有复核价值，现统一保存为标准 `.json.gz`，不删除有效测量。11份机器归档由 154.71 MiB 压缩为 29.50 MiB；触发GitHub警告的ANALYZE/REINDEX档由53.45 MiB压缩为5.70 MiB。每份解压后的长度与SHA-256均和原文件逐字相同，结果、功能状态和273项总账状态不变。

读取可使用Python的 `json.load(gzip.open(path, "rt"))`。原始文件名/哈希和压缩文件名/哈希见[机器归档清单](benchmarks/manifest.json)；历史载荷中的原文件名通过该清单定位。文档和总账的文件链接已更新。新未压缩机器载荷默认被Git忽略，避免再次误提交。当前压缩提交保留已有Git历史，因此旧大对象仍存在于历史中；没有重写共享历史。


<!-- CURRENT_ANALYZED_SCALAR_COLLECTOR_20261105 -->

## 标量统计采集与 ANALYZE 优化（2026-11-05，P11-C4）

本批把真实ANALYZE扫描接入标量统计快照：保留NULL、实际存储宽度和原始扫描顺序，按统计目标维护有界样本，生成signed float4 distinct、MCV、排除MCV后的直方图及相关性。数组/范围专用采集、完整继承/表达式索引统计、真实pg_stats目录源和anyarray输出仍待完成；不是完整17列SQL功能验收。

最终私有普通后端为P11-C4 `e1cab6f6…`；默认根程序425保持。原生641检查及fresh冷读2检查通过，统计计算组件O2与ASan/UBSan各22检查通过（启用泄漏检测，非全库SAN），16个相关原生入口全部正常0。普通Main的CREATE/INSERT/ANALYZE/SELECT四条命令通过，464B实际快照独立解码核对两列；两条完整pg_stats SQL探针仍严格失败0A000，服务器正常0。

相同输入、匹配en_US默认排序规则的真实PG18.6对照共189项：184相同，保留3处实际存储宽度差异和文本槽2处collation OID差异（本地0/PG100）。样本边界按独立样本检查有效范围，不要求随机结果相同。原C-locale参考的文本相关性差异也保留；没有修改SQL语料以取得通过。

### 实现和边界

新AnalyzedScalarStatistics按真实类型/比较角色与原始非NULL、非过宽值的物理秩生成槽；非全表样本使用distinct估计和MCV显著性筛选，空表不造记录、target0停采、过宽值仍参与宽度/NULL计数。ScalarStatisticsCapture从同一次真实物理扫描采样，默认目标100保留至多30000行（300×目标）；450行/target1实际保留300行。字段按当前数据库真实目录relation/attnum及physicalRelationId保存，未给旧原生孤立表制造目录OID。

宽度取固定存储槽或变长数据实际跨度，包括实际TOAST引用时的跨度；不拿输出字符串长度冒充存储宽度。它依赖本地格式，PG numeric18/13、普通text4/5、wide text1500/1504的差异保留。默认文本比较沿用本地en_US规则；现有pg_attribute的默认collation身份为0，两个槽与PG100的差异未伪造修补。

采样结束核对冻结schema文件像和目录属性，发现定义变化则拒绝；这不是全程并发DDL隔离或全部DROP/ALTER/重命名失效验收。发布复用已有OFD锁与StatsPersistence。旧.stats先发布，新快照随后发布；两文件不是一个共同的原子事务。新文件损坏仍报错而不覆盖；本批未注入共享IO/分配失败或重做跨进程恢复验收。数组/范围、自定义无排序类型、完整继承/分区与表达式索引采集，目录/输出/权限消费者和全部未完成总账仍在目标内。

未缓存的本批C1在组件坏输入检查发现uint32宽度溢出，C2增加int32边界拒绝并在生成槽后释放样本。最终C4对新采集路径复用既有AnalyzeComparisonCache：实际自比较验证完成后，仅在本列/本次分析内保存整数/浮点键或locale；unsupported缓存走原比较器。原缓存、旧排序/MCV和bitmap主体逐字保持，无进程/会话/参数/提供者缓存。新索引比较入口由组件SAN及真实DB数据验证。

实现参考[PG18.6 analyze.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/analyze.c)的采样目标、distinct、MCV、残余直方图和相关性公式，及[pg_stats声明](https://www.postgresql.org/docs/18/view-pg-stats.html)。下载的主来源与COPYRIGHT随机器档保存，产品未链接上游C文件。[SQLite ANALYZE](https://www.sqlite.org/lang_analyze.html)和[DuckDB ANALYZE](https://duckdb.org/docs/2.0/sql/statements/analyze)的持久统计/优化器消费分离调研延续P10；没有运行两者性能比较。

### 实际正确性证据

同一新原生测试先链接父代core，真实建表/写入/ANALYZE后在第46次检查以“缺少新统计快照”失败；该失败不是编译失败替代。P11-C1、C2、C3及最终C4各641检查正常0，各独立冷进程2检查正常0。包含标量整数、文本、numeric大精度等价表示、boolean、float NaN/Infinity、空文本/字面NULL/真NULL、STORED/未选中无效VIRTUAL、正反物理顺序、全NULL、单行、单常值、过宽值和真实有界采样。计算组件初始缺include构建失败与坏宽度失败均保留，最终O2/SAN各22；本次未把568次旧快照检查称为SQL场景数。正式16入口包含新计算入口及旧ANALYZE/统计/持久化回归。

普通C4 Main实际执行4条SQL命令并正常退出。完整快照独立检查4行、两列的NULL/空文本/字面NULL、MCV、直方图与相关性；保留6条查询的原始wire消息。另2条完整17列pg_stats探针依然0A000，无描述符，属于严格失败。源与Main对象血缘保持P9的旧5项元数据差异记录；本批没有重跑全部35操作/36查询或声称这5项已修复。

真实PG用同一批实际执行日志抽出的552条SQL。初始C-locale参考有文本相关性差异；核对本地默认比较规则与系统locale后，新建en_US.UTF-8私有PG集群，SQL语料未改。原始Oid JSON字符串只作数字类型规范化，原始不匹配适配记录保留。最终15列189项逐项核对：184匹配，3个宽度和2个collation身份差异仍是差异，不作PG整体通过。真实独立样本的端点不要求逐值相同；其类型、比较角色、NULL/distinct/相关性及边界范围保持严格检查。

### 完整ANALYZE成本：本批未缓存／缓存／同一未缓存回测

这两个版本均已采集并持久化同一完整标量功能；父提交后端30c没有此功能，因此未被拿来声称同功能加速。P11-C2普通后端1891529a…，最终C4为 `e1cab6f63405be794fac305e0c9c27b9853b4ee62265e213148395fc53d24752`。两者同一不可变benchmark源、相同C++17/O2/ICU/zlib/TLS-stub、原生61core血缘，普通CMake每代实际仅重编TableManage一个TU、核对其余61依赖。

每worker新目录/新进程，真实DDL创建4类型表，事务写入4096行。每worker先做一次不计时完整ANALYZE与全字段/全行预检，再连续计时5次完整API调用，包括扫描、旧统计和两种文件的正常持久发布。共9进程、45次计时调用和9次不计时预检，无重采。准备及预检会触及缓存，未清除/固定OS缓存；wall包含IO/调度，process CPU包含后台线程，thread CPU为调用线程。

| 指标，ms／5次完整ANALYZE：p50 [min, max] | 本批P11-C2前 | 最终P11-C4 | 本批P11-C2回测 |
| --- | ---: | ---: | ---: |
| wall | 2070.365 [2056.270, 2151.850] | 1021.941 [1015.185, 1029.609] | 2229.714 [2133.796, 2372.005] |
| process CPU | 2044.824 [2020.664, 2126.706] | 982.911 [957.003, 984.710] | 2175.106 [2073.949, 2322.884] |
| thread CPU | 2038.268 [2012.946, 2120.036] | 979.040 [953.576, 980.948] | 2167.958 [2065.468, 2314.453] |

process CPU p50较两轮未缓存对照低51.93%/54.81%，调用线程CPU低51.97%/54.84%，wall低50.64%/54.17%。未缓存回测漂移：process CPU +6.37%、thread CPU +6.36%、wall +7.70%。缓存版wall1015.185..1029.609ms、process CPU957.003..984.710ms的波动全部保留；每段只有3个样本，描述性统计不证明统计显著性。

9/9进程正常0。每worker在计时外核对4096行、16384单元格、1275个真实NULL和全RID，前后全行不变；旧统计文件字节不变，新快照除已预定generation增长外每字段/值不变。9份189213B全行证明与9份3659B最终快照分别逐字相同。独立脚本重读全部9条计时、每个真实RID/值/NULL及帧长度/版本/FNV/generation6/4列；不只是核对汇总json。

这是这组4类型负载的完整ANALYZE提速，不是任意SELECT、所有数据库操作、冷盘、数据库整体或PostgreSQL速度比较。全部原始源码/配置/哈希、失败与正常结果见[机器归档](benchmarks/analyzed-scalar-collector-20261105.json.gz)。

已接入真实ANALYZE的标量采集和快照发布；旧优化器/原生8列统计保持。普通服务器四条SQL流程与独立快照读回通过，完整pg_stats查询仍有2项0A000失败。PG对照的3处存储宽度及2处collation OID差异仍开放。

本批同功能版本的完整ANALYZE对照（4096行、4类型，每进程计时5次，前/新/回测各3进程）CPU中位数降低51.93%/54.81%，wall降低50.64%/54.17%；9份完整数据/RID/NULL和统计快照逐字一致。对照是本批P11-C2/P11-C4，不是父提交后端或任意SELECT查询提速。

源236/头166；原234生产文件仅TableManage.cpp变更，164旧头和2342旧测试保持。新增3测试后2345（顶层1094 CPP含桩、去桩1093，另5子目录CPP）。原273项22/166/70/15及261条历史保持，仅加1条partial后262；Goal active。

最终C4还修复默认自动分析的可见性：旧C3在批量INSERT达到阈值时生成空列快照；现在普通事务成功COMMIT、清理原命令视图与锁后，按已有改动表集合触发分析。失败/回滚不提前发布；只有分析成功才扣除已统计修改数，保留并发新增计数。维护错误不把已持久提交伪装成失败。25条普通协议SQL验证INSERT55、显式事务提交110、UPDATE110、DELETE50、回滚不改快照、ALTER SYSTEM+reload关闭自动分析后显式分析105，并核对5次EXPLAIN基数；服务器正常0。原恢复入口1574检查及7个worker全部正常0。共享IO/分配失败、两阶段提交的自动统计调度及显式事务内ANALYZE的全部回滚语义仍未完整验收。

按用户清理要求删除93209个旧编译文件，实际空闲增加约658GiB；分区85%→72%、空闲约1.5TiB，项目约111→28GiB。源码、原2342测试、全部已跟踪文件、当前C4/default425及PG18参考工具已核对保持。后续以现有CMake缓存和当前core复用构建，证据归档后清理可重建产物。删除在9样本全部结束后进行；只读du盘点与最终对照的回测段重叠，全部结果保留，不称为无并发负载测量。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

本批是完整pg_stats的持久化基础阶段，尚未接入ANALYZE采集器、SQL来源或输出通路。普通后端仍为P9-C2 `30c57ca82d55d6a18b6a305d8595ae74f8edde52062a9eca63ba6448a4be26c2`，默认根程序仍425。新增3头文件后源234/头164；旧231源码及161头逐字保持，正常CMake0重编/62对象保持。新头文件在组件和正式原生入口实际编译，匹配fresh stub794；不是一次新增后端功能的冷编译或发布。

### 设计依据与实际范围

[PG18的完整pg_stats说明](https://www.postgresql.org/docs/18/view-pg-stats.html)与[REL_18_6 pg_statistic](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_statistic.h)使用relation/attribute/inherited键和至多5个槽，以kind、operator、collation、可空numbers/values表示不同类型统计。[SQLite ANALYZE](https://www.sqlite.org/lang_analyze.html)将统计保存在内部表供优化器使用；[DuckDB2.0 ANALYZE说明](https://duckdb.org/docs/2.0/sql/statements/analyze)强调其重计算统计的join-order用途。本批取其采集/持久化/消费分离原则，没有运行SQLite/DuckDB速度比较。

新增AnalyzedStatistics模型保留完整17字段契约及全部1..7类槽、signed float4 distinct/频率、实际声明与域/元素/记录元数据。值拥有自己的文本、NULL、objectOid/elementOids、record字段、数组维度/下界/NULL-bitmap及子值；类型身份按元数据保存，未从显示字符串重新猜测。Values可缺失、存在但为空或含NULL子值，三者保持不同。schema/表/属性显示标签留给未来当前数据库目录源按真实键连接，不存伪造viewOID。

codec采用明确小端字段、版本1、长度边界与FNV64完整帧校验；限制128MiB、100万结构节点、深度128和数组6维。编码资源超限为54000，损坏输入为XX001。Store复用现有StatsFileLock和StatsPersistence；关系更新在同一OFD锁内reload/merge，整组替换该relation的base/inherited记录并保留其他关系，64位generation耗尽拒绝回绕。PublishedNotDurable枚举透传既有发布语义；本次正常发布为Durable，没有注入共享持久化故障。

新组件使用独立文件，由调用者提供路径。现有`.stats`文件格式、ANALYZE、旧优化器和8列原生renderer保持；旧分隔符局限仍未在这些路径中修复。新文件缺失返回unavailable，旧文件不能被当作已拥有avg_width/correlation等新字段。采集器仍需实际填充并发布记录，不能用未计算的零值伪装完整视图。

### 真实参考、组件检查与SQL失败

私有PG18.6新集群实际核对全部17个名称/类型OID/宽度，并对8个属性采集到1..7全部槽。原始anyarray_send证实：数组列MCV元素1007，常见元素23，范围长度701，多范围bounds元素3904；全NULL没有使用槽。全empty-range本次参考的values为SQLNULL、numbers为[1]，并非先前草稿假定的空数组；保留该参考结果，模型仍区分两种表示，采集/输出适用性必须按实际PG验证。这里只确认声明及表示契约，不是产品统计数值对照通过。

同一测试在未优化O2、最终O2、最终ASan/UBSan和正式core入口分别通过：每次568父进程检查，真实冷读及两个写入子进程各1次检查，均正常0。包括独立Python生成的220B固定帧、全部220个单字节损坏和220个截断前缀、重算校验后的非法字段/重复键、类型/值环及深度边界、完整拥有字段、缺失/损坏读入、generation耗尽及保持旧文件。两个线程实际合并不同关系，两个fresh进程在父锁持有时均被观察到阻塞于目标`.lock`的F_OFD_SETLKW，再释放并验证更新都保留；未用sleep当作锁成立的证明。组件SAN包含StatsPersistence且启用泄漏检测，非全库SAN。

正式core入口使用冻结新头、61个经哈希核对的旧core对象和新编译stub；既有公共头ABI不变。当前普通Main两条完整17列零行探针均返回0A000“system catalog relation is not implemented”，2项严格失败；服务器正常0。观测驱动退出0表示结果采集完成，不表示功能通过。P9的5项元数据差异和其他旧失败原文保持；本批未重测全部35操作/36查询。

### 组件成本：本批未优化版本／最终版本／同一未优化版本

父提交不存在此新组件，因此对照是本批初始std::set路径检查版本，不是旧版数据库。最终用7类槽位图及有界祖先指针栈保留重复kind/环检查，减少逐节点树结点分配。每段3个新进程，同一源码、O2、1024个合成列记录，单帧543797B；每worker先做1次不计时encode/decode完整字段预检，再各计时100次encode与100次decode。内存已被预检触及，不是零热身/冷盘或fsync测试。循环检查长度/校验尾、行数/代别，计时后核对完整字节和完整字段；9份payload实际SHA一致。

| 指标，ms／100次：p50 [min, max] | 本批未优化前 | 最终组件 | 本批未优化后 |
| --- | ---: | ---: | ---: |
| encode wall | 353.947 [345.669, 357.605] | 302.901 [280.857, 312.181] | 353.924 [350.057, 358.122] |
| encode CPU | 353.931 [345.666, 357.303] | 302.898 [280.846, 312.159] | 353.907 [350.054, 358.119] |
| decode wall | 370.230 [354.107, 370.990] | 353.671 [350.004, 396.033] | 363.036 [355.689, 369.287] |
| decode CPU | 370.229 [354.027, 370.988] | 353.669 [350.002, 395.952] | 363.034 [355.687, 369.285] |

编码CPU p50较两次本批未优化对照低14.42%/14.41%，解码低4.47%/2.58%。编码对照前后漂移−0.0066%，解码−1.94%；最终解码第2样本CPU395.952ms长尾保留，高于两侧旧样本，未删样本或重采。只有3样本/段，均为描述性组件统计，不证明所有样本占优或统计显著性，更不是数据库查询、ANALYZE、持久化fsync或PostgreSQL速度提升。

9/9进程正常0，18组计时记录及9份完整payload独立重读校验，所有原始min/p50/max、源码/配置/对象哈希、正常和失败记录见[机器归档](benchmarks/analyzed-statistics-snapshot-20261105.json.gz)。最初PG套接字路径过长在启动期终止、组件初版API拼写、参考空值假设及两版SQL结果采集脚本错误均保留；原数据与失败预期没有被改成成功。

### 后续验收

必须接入真实ANALYZE采样与宽度/distinct/correlation/标量、数组、范围槽，覆盖真实继承/分区/表达式索引所有者和DDL/冷读失效；随后注册真实pg_stats view/17属性、当前数据库冻结源和来源身份，并贯通output-only anyarray及普通格式0/1/Describe，复用既有权限消费者。当前2项0A000是开放失败，不能以模型通过替代。全部原暂缓政策不变；原273对象/22-166-70-15、260条历史、725保护记录、9份旧归档和754 Markdown路径保持，仅加1条partial后历史261；Goal active。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

最终正常程序P9-C2 SHA256为 `30c57ca82d55d6a18b6a305d8595ae74f8edde52062a9eca63ba6448a4be26c2`；本批基线为770d7dfc / P8-C3 `d778afbd…`，中间P9-C1为 `708b02c5…`。源231/头161，C++17/O2/ICU/zlib/TLS stub保持。两代各真实重编TableManage一个TU，另61依赖核对，沿原fresh62构建血缘；匹配测试桩794。默认根程序425与现有服务保持。

### 捕获的错误与实现

完整Memcheck在旧版seed退出时捕获 `relation_name_codec::decode → getTableNames → flushSelectedCaches → ~StorageEngine` 读取已释放65B字符串。`listMagic()` 的动态局部static晚于全局引擎初始化，其析构可能先于引擎。现用64字节 `static constexpr char[]` 和按值string_view，标记、填零、旧短名列表及V2长度/校验字节不变。独立持久化固定夹具在main及更早owner的析构各作9个控制：旧O2第11次检查失败、旧ASan报告UAF，新O2及ASan/UBSan各18次通过；永久入口使用同一源码。

仅修标记后的P9-C1仍在正常advance阶段报corrupted double-linked list；完整Memcheck继续到cold-low，捕获后台 `backgroundBufferFlush → maintenanceStateFor` 访问已销毁std::map节点。现把原mutex和weak_ptr表合为一个登记对象，在每个StorageEngine构造完成及线程启动前初始化，使其析构晚于所有引擎和已join的工作线程。不添加数据库条目或故意泄漏；原状态查找、锁、任务调度和完成通知保持。机械反向替换后TableManage全旧字节精确恢复，仅有登记对象及构造预初始化两处变化；R15网络唤醒、CTAS/checkpoint调度、R96 collation及其他暂缓路径代码保持。

[C++17 N4659生命周期规则](https://timsong-cpp.github.io/cppwp/n4659/basic.start.term)说明动态静态对象的退出顺序与析构后使用约束；[SQLite格式说明](https://www.sqlite.org/fileformat2.html#the_database_header)的固定文件头是保持格式标记为确定字节的参照，不表示SQLite格式兼容。使用[Memcheck官方说明](https://valgrind.org/docs/manual/mc-manual.html)的分配/释放历史核实实际缺陷，没有仅从崩溃文字推断根因。

### 完整回归与保留的失败

| 版本／方式 | 实际结果 | 后续阶段 |
| --- | --- | --- |
| 原P8-C3正常 | advance wait134，corrupted double-linked list | 后4阶段NOTRUN |
| 本批P9-C1正常（仅标记修复） | advance wait134，同样文字 | 后4阶段NOTRUN |
| 旧版完整Memcheck | seed退出时表名标记UAF，工具exit97 | 后6阶段NOTRUN |
| P9-C1完整Memcheck | cold-low维护状态表UAF，工具exit97 | 后2阶段NOTRUN |
| 最终P9-C2正常 | 原7worker全部正常0 | 原全部断言通过 |
| 最终P9-C2完整Memcheck | 父进程及7worker全部正常0，8日志各0错误 | 原全部断言通过 |

七阶段仍是同一控制体，覆盖真实较早/较晚目标、INSERT/UPDATE/DELETE、已结算PREPARE提交/回滚、最高已分配但中止事务号、真实新tuple xmin及关系身份、完整RID/NULL/值和冷进程续写；未修改SQL、断言、时限或用退出钩子跳过析构。分离advance三次、两种GDB设置和分离Memcheck曾正常0，均未当作整体通过。旧SIGABRT本次未取得同一次执行的abort调用栈，不能把两个已捕获UAF当作所有旧崩溃的排他归因。最终完整Memcheck未启用泄漏检查，不是全库ASan/TSan验收。

最终23个相关入口（含WAL32及进程内准入控制）和新生命周期入口正常0。原562控制的名称/列表/WAL草稿在旧/新O2与ASan/UBSan四次均0，只证明有限编码兼容性，未注册该草稿或关闭R218。原2340测试文件不改，新增1永久入口后2341；顶层1091 CPP含桩、去桩1090入口，另5个子目录CPP。

正常Main仍保留原5项描述符/真实目录身份失败：P9-C1为219/224，最终C2为212/217；固定22个actual对象（含6次真实目录读取）与P8-C3相同。总数差别来自后台作业实际轮询。两版数据、NULL、后续写入及两个正常Main上下文（含cold）正确，均正常0；探针的OID20/size8及缺pg_class身份仍开放。

### 完整恢复成本对照：旧P8-C3／最终P9-C2／旧P8-C3

每段3次、零额外热身；直接复用上一批的精确C3原生二进制，只编译新C2。两者同一c038原生源、默认分配器，无测试包裹器。使用相同正确闭合H72 seed与原镜像，每样本新进程、新目标与镜像副本。计时仅包围实际完整physicalRestore调用；克隆、SHA、全1000+4行/RID/NULL扫描、2001哨兵和输出在计时外。准备会触及缓存，未清除或锁定OS缓存；登记对象初始化在引擎构造中，不计入本API区间，不声称启动提速。

| 指标，ms：p50 [min, max] | 旧P8-C3前 | 最终P9-C2 | 旧P8-C3后 |
| --- | ---: | ---: | ---: |
| wall | 1985.683 [1789.234, 2024.291] | 1728.068 [1589.105, 1764.977] | 1716.633 [1596.170, 1741.020] |
| process CPU | 810.095 [795.673, 832.848] | 818.376 [782.026, 832.238] | 825.474 [795.689, 864.813] |
| thread CPU | 807.882 [794.053, 830.354] | 815.419 [779.091, 830.112] | 823.267 [793.656, 823.619] |

新版本wall p50较旧前低12.97%，较旧后高0.67%；CPU分别高1.02%、低0.86%。旧版前后wall漂移−13.55%、CPU+1.90%，未证明稳定加速；每段仅3样本，全部原始值和长尾保留，不筛选或重采。这是生命周期修复的成本控制，不是查询提速结论。

9/9调用正常0、108/108检查通过；27081行、108252单元、36个真实NULL逐值核对，完整前后RID与哨兵存在/移除检查通过，全部原镜像文件字节不变。独立从原stdout重新汇总每个计时值及min/p50/max，未把组件或全进程时间替代实际API耗时。

### 证据及剩余范围

工具启动时的缺符号/路径、初版include配置及两个汇总脚本假设错误保留，均未改动数据库样本或原断言。全部实际命令、源码/配置/对象哈希、原失败、worker stdout/stderr、九次原始时间及PID归属记录见[机器归档](benchmarks/relation-list-engine-lifetime-20261105.json.gz)。

正常Main五项元数据、跨服务进程在线准入、完整17字段pg_stats及其他原始功能族仍开放。本批未重测完整35操作/36查询或PostgreSQL性能，未切换默认425。原273对象与22/166/70/15状态、259条历史、725保护记录、8份旧归档和754 Markdown路径保持，仅追加一条partial后历史260；Goal active。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

最终正常程序P8-C3 SHA256为 `d778afbd3351c97bace23202ea257819cf16a0bfe9fef8853d26597c6056d7c6`；基线P7-C1为 `724c1147…`，中间P8-C1为 `2523431c…`、P8-C2为 `adc9cee2…`。保持0.2.0 / C++17 / GCC13.3 / O2 / ICU74.2 / zlib1.3 / TLS stub。源231/头161，C1真实依赖重编16个TU，C2仅WAL一个、C3仅TableManage一个，均沿原fresh62对象与正常CMake依赖/SHA验证；fresh测试桩794。默认根目录程序仍为 `425165af…`，未重启现有服务。

### 实现及原始缺口

原始较早目标的全局分配状态低于镜像中已提交元组的事务号，可能在读取CLOG前被当前可见性水位排除。本批公开恢复在既有来源验证与完整暂存完成后、目录发布前，从实际暂存代别只读汇总checkpoint、CLOG实际非零状态、主heap的xmin/xmax及WAL证据，随后持久推进分配器；保留旧的更高水位。heap读取校验文件头、页校验和、实际slot/tuple边界，并兼容只读legacy4与绑定页号的layout5，不升级或写页；不会把整块CLOG预分配范围当事务号已被使用。文件/格式缺陷仍按原失败路径处理。

恢复持有当前进程的排他准入屏障，BEGIN仅在真实事务号分配到活动登记之间取共享屏障。存在其他实际活动事务号不高于导入H时保守拒绝；较高活动事务控制可继续且不重写其已存在快照。该屏障是进程内对象，跨服务进程在线准入仍有实际缺口；已验证的分配器fresh-peer持久重载不等于已解决跨进程BEGIN/恢复准入。

C2只为只读WAL快照读者增加局部等价CRC，C3只让非null公开恢复准入的两处完整源校验选择已验证的块SHA收集器，保留原逐文件完整读取、文件/目录集合、size/SHA、标记、提交、锁和同步。原共享SHA/CRC、共享校验器、安全认证及null内部事务/savepoint分流保持；私有摘要算法和许可证体不变，只扩展其明确的公开准入用途注释。未靠省略SHA、WAL、fsync或改变旧恢复格式取得收益。

[PG18.6 varsup](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/transam/varsup.c)在恢复/两阶段启动中推进nextXid，[PG18.6 xlogrecovery](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/transam/xlogrecovery.c)在重放记录前推进超过记录事务号；它们用于推导分配水位不应落后于恢复数据的约束，不表示本项目实现了PG的epoch或恢复体系。[SQLite Online Backup](https://www.sqlite.org/backup.html)提供完整快照复制及锁范围的设计参照，本批没有声称实现SQLite增量在线并发、格式或速度等价。

### 实际正确性与严格失败

独立公开原生扫描对原镜像及较早目标验证完整1000主表行与4行探针，包含真实RID、SQLNULL、空串、字面NULL、整数极值及6000B+UTF8 TOAST；哨兵被实际插入并在恢复后移除。正常Main固定22操作的首次探针旧版仅1/4，C1/C2/C3均4/4；主表、后续INSERT/CREATE、CHECKPOINT及两次正常进程上下文（含cold reopen）数据正确。SQL路径没有实际ctid绑定，不能把原生RID验证说成SQL RID对照。

正常Main三版22个固定操作的完整actual对象（含帧体）相同，但每版仍有5项严格元数据检查失败：原生创建的探针在真实pg_class中没有对应行，id/n还发布OID20,size8而非固定期望23,size4。缺失身份不以零归一化、猜测OID或修预期消除。含实际后台轮询的检查数为C1 249/254、C2 234/239、C3 209/214；总数受真实轮询次数影响。旧版原始冷启动−6保留、原因未知，未重试；后续版本两上下文正常0不抹掉它。

| 同源七阶段原生验收 | 实际结果 | 未运行范围 |
| --- | --- | --- |
| 旧P7-C1 | seed/later/advance成功，restore-low数据严格失败 | 后3阶段NOTRUN |
| P8-C1 | 原七阶段全部通过 | 无 |
| P8-C2 | seed/later成功，advance wait状态134，Invalid argument | 后4阶段NOTRUN |
| 最终P8-C3 | seed/later成功，advance wait状态134，corrupted double-linked list | 后4阶段NOTRUN |

C2/C3 advance内已实际发生写入循环，精确异常位置和原因未知；不是启动前零工作失败，也不能推定由某恢复或注册逻辑导致。原SQL、断言、源及日志全部保留，无重试，不将其改列暂缓或由其他成功控制抵消。最终七阶段入口仍为严格FAIL。

相关21个旧入口全部通过。只读文件组件19控制、分配器独立8个顶层+3个fresh-peer控制、WAL原20+新12共32控制通过。C3当前进程准入的低活动拒绝、高活动允许、分配—登记窗口三个语义由5个工作进程验证，全部正常0；不证明其他进程或所有故障时序。新增三个永久入口及一个夹具头，2336→2340；顶层1090 CPP含test_stubs（排除后1089入口），另5个子目录CPP。注册首轮缺同源夹具头、随后缺项目wrap注解的构建失败保留；补齐同源夹具头及首行wrap注解后，93335注册入口实际编译运行exit0；最终夹具头及七阶段入口仅清理原型注释，控制体和断言保持。没有“全部新增入口通过”的结论。

### 快照组件控制

明确预读后的完整只读WAL快照API，旧/新/旧各三次，CPU p50为485.591→157.523→445.475ms，降低67.56%/64.64%；独立九次完整原样保留。它包含实际目录、文件、记录、链及CRC处理，但不含恢复发布、SQL或提交；预读不等于已测量/锁定物理缓存驻留。该组件比例不能替代下面完整physicalRestore比例。

### 第一轮：旧P7-C1／未优化P8-C1／P8-C2／旧P7-C1

每代三次、零额外热身，同一c038原生源使用默认分配器，无Main/分配包裹器。目标是原P7已含4探针且真实H72的正确闭合seed，区别于原始低目标失败；每次新进程、新目标与新镜像副本。克隆、完整1000+4扫描及2001哨兵、所有输出和SHA核对在计时外，计时仅包围实际完整physicalRestore至返回。准备扫描会预热目标，镜像复制/hash会触及缓存，未清理或锁定OS缓存，不能称冷磁盘测量。

| 指标，ms：p50 [min, max] | 旧P7-C1前 | P8-C1未优化 | P8-C2 CRC | 旧P7-C1后 |
| --- | ---: | ---: | ---: | ---: |
| wall | 1649.601 [1591.698, 1854.793] | 2080.188 [2029.132, 2093.020] | 1775.514 [1754.973, 1851.537] | 1697.333 [1313.056, 6800.138] |
| process CPU | 826.186 [796.988, 865.184] | 1343.778 [1307.079, 1351.039] | 968.691 [961.997, 1027.469] | 858.226 [841.394, 882.833] |
| thread CPU | 824.288 [795.275, 862.988] | 1341.171 [1304.837, 1348.628] | 966.674 [940.962, 1025.152] | 849.586 [839.723, 880.822] |

C2较同轮C1 wall/CPU p50低14.65%/27.91%，但仍比两旧版wall高7.63%/4.61%、CPU高17.25%/12.87%。旧版前后wall漂移+2.89%、CPU+3.88%；旧后第3次6800.138ms长尾不删除、不归因。第一轮负值保留，未据组件提速宣称完整调用已优化到旧版水平。

### 第二轮：旧P7-C1／P8-C2／最终P8-C3／旧P7-C1

独立再冻结的四段各三次、零额外热身，仍使用同一c038源和同一完整值/RID/NULL预期。旧版及C2直接复用第一轮的精确二进制，只有C3另按真实匹配配置编译；不将第一轮样本拼入本轮统计，也不按结果筛选或重采。

| 指标，ms：p50 [min, max] | 旧P7-C1前 | P8-C2 CRC | 最终P8-C3 | 旧P7-C1后 |
| --- | ---: | ---: | ---: | ---: |
| wall | 1633.279 [1481.494, 1768.533] | 1743.687 [1737.344, 18517.621] | 1565.301 [1564.572, 2170.976] | 1672.605 [1606.681, 1819.338] |
| process CPU | 850.929 [798.954, 866.409] | 1033.063 [1026.514, 1090.238] | 829.923 [819.414, 831.779] | 857.086 [851.758, 861.679] |
| thread CPU | 849.133 [796.781, 864.279] | 1029.232 [1024.560, 1068.796] | 827.419 [807.558, 829.677] | 855.166 [849.579, 859.698] |

C3较同轮C2 wall/CPU p50低10.23%/19.66%；较两次旧版wall低4.16%/6.42%，CPU低2.47%/3.17%。旧版前后wall漂移+2.41%、CPU+0.72%。C2第3次18517.621ms与C3最大2170.976ms全部保留；每段仅三样本，这些是固定夹具的描述性统计，不宣称统计显著性、全部样本占优或所有数据库操作加速。

两轮分别12/12调用、144/144检查通过；每轮12进程正常0且PID/组无残留，36108行输出/144336单元/48个真实NULL均逐值核对，包含全1000+4的前后RID、哨兵真实存在/移除、清单及全部镜像文件字节不变。原命令、配置、来源/目标清单及SHA、各3原始时间、stdout/stderr、PID/startticks与首轮失败保留在[机器归档](benchmarks/restore-transaction-horizon-20261105.json.gz)；两轮成功不覆盖前述七阶段FAIL。

### 剩余范围

跨进程在线准入、最终七阶段实际异常、5项描述符/真实目录身份仍开放；完整pg_stats的17字段与typed数组/来源仅有后续计划，不能以四字段简化表作为终点。原用户暂缓的安全/ACL/RLS及其他边界保持，不新增泛化暂缓。原273对象及22/166/70/15状态、258条历史原文、725保护记录和七份旧归档保留，新增一条partial后历史259；Markdown路径仍754。默认425、既有服务保持，未运行本批完整35操作/36查询或PostgreSQL速度矩阵，未作整体提升/功能族完成结论。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

本批 P7-C1 的正常程序 SHA256 为 `724c11472e383be3720997521a2cba962a8da57fd2bf2b2e309e11035078787b`；基线是已提交 d748ebab 的 C8 `45264111…`，不是根目录默认425。保持0.2.0 / C++17 / GCC13.3 / O2 / ICU74.2 / zlib1.3 / TLS stub。实际正常CMake构建重编TableManage一个翻译单元、复用61个经依赖与SHA核验的对象，追溯到原零缓存62单元构建；源230/头160，原159公共头逐字节不变，测试桩重新编译并冻结。

本批实现两项改动。ANALYZE在值收集与统计结果生成两处跳过virtual列，真实物理ordinal、普通列、STORED、NULL、TOAST、原比较/MCV/持久化保持；既有同页位图调用方因此可用于含virtual的表。备份仅在创建清单时采用便携C++17的SHA-256块处理与展开压缩，仍完整读取每个文件；原文件校验器、共享SHA、恢复、savepoint及同步/提交路径保持。未通过省略读回、WAL、fsync或降低维护频率取得收益。

调研依据是[PG18.6 ANALYZE源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/analyze.c)中对virtual属性的直接排除，以及[PG18.6 SHA实现](https://github.com/postgres/postgres/blob/REL_18_6/src/common/sha2.c)的整块处理和展开变换。上游版权、条件及免责声明保留于源码与[第三方声明](../THIRD_PARTY_NOTICES)，私有前缀实际CMake安装已验证程序和声明SHA一致。[PostgreSQL备份清单](https://www.postgresql.org/docs/18/backup-manifest-format.html)用于备份验证；本项目继续使用自己的V2格式。[SQLite在线备份](https://www.sqlite.org/backup.html)通过增量页复制缩短锁持有，[DuckDB CHECKPOINT](https://duckdb.org/docs/2.0/sql/statements/checkpoint.html)将WAL同步至数据文件；这两项作为架构参照，本批未实现其并发或checkpoint策略，也无跨数据库速度结论。

### 实际正确性及剩余差异

- 新永久[virtual统计测试](../tests/analyze_virtual_statistics_test.cpp)同源20控制：C8为14通过/6失败，C1为20/0。旧14项保持，六个含virtual统计排除的断言得到修复；复合旧断言没有单独子分量日志，不扩展其观测范围。
- 新永久[清单摘要测试](../tests/backup_manifest_creation_sha256_test.cpp)实际864/0；144个独立固定摘要乘六种分块模式，同时校验旧实现、新实现和非对齐输入，覆盖空串、abc、百万a、二进制、填充与块边界、1MiB输入。固定预期独立由hashlib复核；生产头独立864控制也通过。两次864不是1728个不同用例。
- 相关原12个入口全部通过，覆盖备份清单、恢复/一致性、替换、目标保护/锁和普通ANALYZE统计。C8原备份清单测试也通过。原2334测试源字节保持，现2336；新增884控制通过并非全仓重跑。
- PG18.6参考34条全部捕获并保留两个最初帧顺序预测错误；C1正常Main同34为17匹配/17严格差异，零服务故障、正常退出。11个qualified pg_stats入口为42P01，REINDEX INDEX及ANALYZE列列表各1差异；selected虚拟列的22003/22012正确但缺PG错误前缀T，两个DELETE命令/值正确但多空T。实际关闭后的.stats中，av_stats为3行且仅id/p/s，p/s各2 NULL；av_allnull最终1行，仅id/p/s且无NULL。该持久层证据不代表缺失的SQL视图已补齐。

两代实际创建的备份清单全文、全部目录和每个载荷文件的size/SHA完全一致，完整主表1000行及NULL/空串/字面NULL/整数极值/6000B+UTF8 TOAST探针均保持真实RID和NULL。

首轮旧版镜像→旧版读者恢复到尚无探针且全局状态较早的目标：physicalRestore返回true，主表1000行/RID和sentinel移除正确，但探针行数不等4，进程返回1。此真实普通恢复缺口仍开放。13个探针文件和CLOG与镜像字节一致，保存的target根.txnid下一号70、seed为73，探针事务69–72均COMMITTED；这只是事务可见性方向的线索，未完成因果验证或修复。首次四调用在此停止，后三个恢复未执行。

随后独立控制使用已包含探针且全局状态匹配的闭合seed作目标，不重做seed/镜像、不改原生程序和断言，四组C8/C1交叉恢复均实际正常0，完整1000+4行/真实RID/NULL与对应镜像相符；原失败不改为通过。准备期另有Path/str预检错误（零原生进程）和所有权根目录守卫拒绝（初始化数据库前返回1），原脚本/日志保留；修正仅指向同一P7拥有目录的公共祖先，原守卫与断言不变。

### 分层速度实测

32MiB驻内存真实归档内容的独立旧/新/旧摘要进程各一次，无额外热身：wall为274.545/207.997/279.041ms，process CPU为273.558/207.993/279.037ms，新CPU低23.97%/25.46%，旧CPU漂移+2.00%。所有摘要一致、进程正常退出；这是摘要组件单次观测，不是p50或数据库操作速度。

完整公开physicalBackup使用默认分配器、无插桩/包裹器。旧C8/新C1/旧C8各3次，零额外热身，每次全新1000行闭合夹具克隆和新输出路径。计时覆盖实际完整调用至持久化返回；克隆、全行校验、目录/清单/hashlib验证、输出及进程观察在计时外。未清理OS缓存，不能称冷缓存测量。全部9次成功，逐文件清单及完整值/RID/NULL通过，进程及组全部退出。

| 指标，ms：p50 [min, max] | 旧C8前 | 新C1 | 旧C8后 | 新比旧前／后 |
| --- | ---: | ---: | ---: | ---: |
| wall | 737.771 [656.323, 14690.372] | 587.376 [514.417, 968.229] | 677.477 [539.220, 1037.003] | -20.38% / -13.30% |
| process CPU | 378.942 [373.040, 461.922] | 287.206 [281.068, 290.443] | 363.727 [362.374, 376.234] | -24.21% / -21.04% |
| thread CPU | 378.457 [372.361, 448.506] | 286.634 [280.631, 289.583] | 362.952 [361.836, 375.702] | -24.26% / -21.03% |

完整API的旧版wall漂移−8.17%，CPU漂移−4.02%。首个旧版14,690.372ms异常长样本完整保留，未归因；新版本最大968.229ms也保留。每代仅三样本，结果描述本夹具，不作统计显著性或普遍性能保证。全9次请求前后共18,000行/72,000单元逐值校验。

正常Main使用未改的原benchmark_operations.py及协议帮助器，完整选择CREATE TABLE、ALTER ADD COLUMN、TRUNCATE、DROP TABLE四族。每代1000行/batch50/ddlRows5、3轮×2样本，每类6个计时动作、每代24动作，总72；零额外热身。保留原setup/verify/cleanup、随机顺序和完整动作；计时包含实际操作持久化返回。三代普通程序上下文均正常退出、自有PID/组无残留；72动作不是SQL请求条数，也不是原全部35操作。

| DDL，wall ms：p50 [min, max] | 旧C8前 | 新C1 | 旧C8后 | 新比旧前／后 |
| --- | ---: | ---: | ---: | ---: |
| CREATE TABLE | 783.432 [678.709, 920.223] | 722.987 [665.855, 1187.788] | 725.227 [639.269, 796.445] | -7.72% / -0.31% |
| ALTER ADD COLUMN | 824.595 [707.256, 896.079] | 891.861 [758.255, 1223.207] | 866.099 [706.380, 997.151] | 8.16% / 2.97% |
| TRUNCATE | 795.304 [662.611, 1035.160] | 935.407 [538.751, 1113.128] | 774.237 [660.862, 904.940] | 17.62% / 20.82% |
| DROP TABLE | 831.162 [737.217, 1280.812] | 676.199 [570.094, 964.471] | 764.568 [685.773, 829.548] | -18.64% / -11.56% |

四项wall为2快/2慢，旧版wall漂移范围−8.01%至+5.03%。CREATE比后一次旧版仅低0.31%，不足以宣称稳定收益；ALTER慢2.97%–8.16%，TRUNCATE慢17.62%–20.82%。对应server CPU p50（旧前/新/旧后）分别为CREATE 470/385/465ms、ALTER 515/425/510ms、TRUNCATE 485/380/470ms、DROP 495/370/490ms，均低于两次旧版；/proc计时有时钟粒度限制，不据此把wall慢值归因于IO。全72动作的各IO字段、动作后RSS（不是峰值）、min/max及逐样本均保留。

只有上述完整备份及四DDL对照，本批未重跑全部35操作/36查询、没有新的PG速度样本。旧整体慢项、动作后RSS负值及P6-C8的20项新观察差异/129项旧观察差异不因本批局部通过而清零。原query来源/错误前缀、字符数组声明、表达式索引、LATERAL/EXCLUDED/default/BEFORE及identity语法等仍开放。qualified pg_stats需要真实当前数据库行源与namespace/attribute身份，不能直接挂旧八列文本renderer或删除failclosed守卫。原273项仍22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。

根目录默认程序仍`425165af…`，现有服务未重启；仅验证私有前缀安装，未提升默认程序。所有研究、构建/ABI、实际SQL/raw、首失败、完整计时、负值、进程身份和独立审查见[机器归档](benchmarks/backup-analyze-20261104.json.gz)；其中嵌入原始文本/二进制记录的精确字节和SHA，数据库与对象/可执行文件本身由实际receipt中的哈希识别。原754个Markdown路径、725保护记录、六份旧归档及273项/257历史原文保留，新partial后历史258。


纯静态汇总首次样本索引断言也保留，修正只影响reduce脚本，未重复任何数据库运行。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

本批源码 P6-C8，对应正常程序 SHA256 `45264111…`，保持 0.2.0 / C++17 / GCC 13.3 / O2 / ICU 74.2 / zlib 1.3 / TLS stub。根目录默认程序仍为 `425165af…`，现有服务未重启。

本批修正普通虚拟生成列的实际读值需求、声明类型赋值及真实行输入传递。固定258项严格对照从C2的156通过/102差异改善为C8的238通过/20差异；原156个整项通过全部保留，新增82个整项通过。独立组件检查另保留6项描述符检查损失，不能把“零整项回退”写成每个检查都无退化。

### 正常 Main 的查询性能

比较对象为上一提交源码的正常C2程序 `a3baaca6…` 与本批正常C8程序。实际顺序为 **C2 / C8 / C2**，没有插桩。每个新建私有数据库包含100行、一列实际整数及八个虚拟整数列，`work_mem=4MB`。每条SQL每代1次预热、5次计时，共72个完整Simple Query请求（12预热/60计时）、7200行、25200单元格。

| 查询 | C2 首轮 p50 ms | C8 p50 ms | C2 末轮 p50 ms | C2 漂移 |
| --- | ---: | ---: | ---: | ---: |
| id | 34.750 | 22.158 | 25.273 | -27.27% |
| id,g1 | 142.855 | 38.203 | 136.402 | -4.52% |
| 全部列 | 923.223 | 80.538 | 916.453 | -0.73% |
| CASE 未选 g1 | 863.623 | 21.730 | 871.064 | +0.86% |

每个计时覆盖客户端发出请求到收到完整响应。完整结果、NULL、类型、七个RowDescription字段、命令及Ready状态全部验证，72次零失败；三个实际私有进程均正常退出且PID消失。PG18.6四条功能参考另核400行/1400单元，未采集PG查询时钟。

原始SQL全部保留，四条都有 `WHERE id>=1 ORDER BY id`；第四条为 `SELECT id,CASE WHEN id<0 THEN g1 ELSE id END AS guarded_value ...`。完整表与逐次样本见机器归档。

| 查询 | C2 首轮 min–max ms | C8 min–max ms | C2 末轮 min–max ms |
| --- | ---: | ---: | ---: |
| id | 21.629–37.385 | 18.005–25.899 | 25.210–25.973 |
| id,g1 | 132.237–160.782 | 35.474–40.626 | 120.731–141.661 |
| 全部列 | 846.666–1182.301 | 79.491–83.891 | 855.435–1023.057 |
| CASE 未选 g1 | 845.689–907.474 | 16.191–25.734 | 845.767–973.349 |

四条p50都低于两次C2。C2前后漂移为−27.27%至+0.86%，每条仅五个计时样本；这是固定数据量和四种查询形状的描述性结果。单个查询的CPU/峰值RSS、统计显著性、PostgreSQL速度倍率及整库吞吐均未由本组测量证明。进程资源只留整代前后的粗快照。

本批没有重新执行全部35类操作或36组查询。P5-C2原35操作的4快/28慢/3居间、36组查询的4快/6慢/26居间与更高动作后RSS继续保留；四条生成列查询的收益不改变整体性能尚未验收、默认425保持的结论。C6历史Main72样本独立保留，未重贴为C8。

### 实现及其他数据库调研

[PostgreSQL18生成列](https://www.postgresql.org/docs/18/ddl-generated-columns.html)区分写入时计算的stored列与读取时计算的virtual列。本批保存真实生成表达式、列、类型及修饰符，按实际读取调用原求值器；复用表达式解析/声明元数据，每次真实读取重新求值，不缓存行值。原生回调覆盖及参数求值期间的回调变化保留。

真实堆行的NULL位图、来源发生项及行是否存在分别传递；未匹配的外连接行与“存在但基础列全NULL”的行分别处理。新prepared物理输入解码器跳过virtual占位，原完整行解码器保持；已有完整行matcher仍读完整值。落盘排序传递真实生成程序和输入，恢复时验证来源/列/程序身份；已有真实上下文损坏控制在旧C5有4个漏拒，当前相同核心对象的10控制全部通过。

声明赋值落实smallint范围、numeric标度、varchar/char及其数组的长度和UTF8字符规则。已拥有的数组/记录继续传递实际子值、形状、NULL位图及类型身份，不从格式化显示重建。对遗留catalog元素OID与真实字符数组声明不一致的已绑定输出，新增SELECT入口在执行前保留原有真实值接收器；C5完整90控制证明该旧路径，C8复验为88通过/2错误前缀差异。

[PostgreSQL INSERT](https://www.postgresql.org/docs/18/sql-insert.html)的隐式目标列表按物理前N列对应，不跳过中间生成列。本批修复短VALUES/SELECT输入，保留显式列表的宽度检查和生成列DEFAULT位置。空列列表及逗号空项在原语法入口返回42601。

备份成本另对照[PG18.6摘要实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/common/sha2.c)、[PG备份清单](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/backup/backup_manifest.c)、[SQLite pager备份](https://raw.githubusercontent.com/sqlite/sqlite/version-3.51.0/src/backup.c)及[DuckDB校验实现](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.2/src/common/checksum.cpp)。据其实现，本项目的优化候选保留完整快照和SHA256格式，采用仅清单创建调用端的私有bulk-block原型；SQLite的pager路径和DuckDB的快速校验不直接替代本库清单协议。原型带PG许可证，未加入生产。

### 严格验证与保留差异

| 固定PG对照 | 观察 | C2差异 | C8差异 |
| --- | ---: | ---: | ---: |
| virtual读取需求 | 72 | 19 | 13 |
| 约束/索引边界 | 63 | 33 | 3 |
| char/varchar数组赋值 | 90 | 44 | 2 |
| 原声明赋值控制 | 33 | 6 | 2 |
| 合计 | 258 | 102 | 20 |

258项的行值全部与参考一致，状态255项一致；另外3项是虚拟列表达式索引应0A000而返回XX000。其余17项保留来源描述符或普通格式运行时错误的前缀/阶段差异。独立检查264项组件改善，6项Simple描述符检查损失完整列出：demand6/19、array12/27、assignment13/31；其余检查的改善没有掩盖这些损失。所有原SQL、预期、参数、阶段、原始响应和未归一化诊断均保存。

原56/132/301/83/111/68/16共767观察仍有1/0/113/2/2/11/0差异（合129），服务失败0。全部767完整记录含post-check、checks及raw与冻结旧产品和C6逐字段一致；301中的九项暂停enum原始记录也一致。八个实际上下文正常退出，未执行341 native或103 enum专属入口。

永久新增两个原生入口36+10=46控制全部通过，实际运行代别C6的61个链接核心对象、159个头文件与C8逐字相同，C8只重新编译Main。C5相关25入口为24通过/1失败；旧C2同一RETURNING声明API的46控制中第36项也失败；该普通quoted EXTRACT失败保留，本批未修。既有R180暂停边界保持，未把这条失败新增为deferred。原2332个测试源文件字节不变；当前2334，顶层1085个CPP（排除桩1084），另有5个子目录CPP。未声称全仓测试重跑。

原INSERT前缀102观察全部保留：状态84项、值96项匹配；真实pg_attribute服务缺失、identity START WITH101建表不支持及生成值错误优先级分别列出，不能称102个值回退。额外列列表语法36观察与6次完整表检查全部通过。PG参考135/90/102/36及既有33按各自来源保留；v3接收器的835项离线检查重新解码全部258个原PG预期payload。

### 私有剖析与未应用的备份候选

普通列建索引及REINDEX的当前真实依赖掩码会跳过无关virtual。ANALYZE仍对virtual写统计，和[PG18.6直接跳过virtual](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/analyze.c)不同；新SQL行的virtual物理位为NULL，不能据此编造未观察到的22003维护回退。

Main私有插桩v5完成24目标动作，另v4保留旧12目标；这36条/684阶段是诊断，不是正常SQL速度样本。v5的C2停止时实际exit−6，整实例生命周期未通过，未调查暂停的退出模块。原包装器和首次失败均保留。

一次独立真实publicBackup叶级调用正常退出：前后各1000行的RID/NULL/完整值、163文件清单和所有文件摘要/目录验证通过。实际payload35,708,488字节，其中两个归档文件33,554,432字节。单次插桩总线程420.210ms/墙钟954.699ms，清单读取加digest线程342.288ms/墙钟342.349ms，stage sync线程30.446ms/墙钟523.192ms。165次文件fsync和7次目录sync只计原stage，AtomicFile内部另有同步；各阶段含重叠，不能相加或全归因纯SHA。

私有bulk-block摘要原型静态19项和864项独立hashlib/已知向量/分块/二进制/非对齐正确性通过。shared SHA、SCRAM、SQL、RoutineStore及R267验证/savepoint路径字节保持，原型未应用；真实备份或完整SQL加速、resident digest基准均未运行。

### 首次失败及尚未完成的范围

初始fresh编译缺ArrayDatumInput头、原生草稿接口/包含错误、批处理父日志占用防覆盖前缀等均保留；修正没有改变原SQL或控制预期。C4原36控制的10失败来自C2批量解码器提前计算virtual，新增真实物理输入角色后C5及相同当前核心的36控制通过。

C5接收器v2在62条已保存观察后，实际执行了第63条但独立1043编码器抛错，原始响应没有持久化。这个缺口不可恢复，未从后代拼回；真实cleanup为exit0。v3补全编码器并在验证前落盘完整响应，离线835检查通过；后续C5完整90及最终C8完整258各自独立保存。

未接受C7只改数据捕获调用但仍有24数组差异，array实例停止时实际exit−6，assignment33未运行。其132队列在case114目标SQL前重置TRUNCATE rd_source超时，服务端记录16,245.89ms（C6对应644.66ms），随后18条响应错位；19条记录不能当19个独立语义回退，原因未知。该调用已撤回，完整源码/二进制/raw保留；C8同一132完整通过不擦掉旧失败。

旧native1000行成本草稿仅完成前三形状18样本（3预热/15计时），第四条因ORDER BY id歧义42702终止，整个队列失败；准备过的v2没有执行。它与本批100行正常Main队列不合并。最终Main72首次启动包装器有0SQL语法笔误，原件保留，实际队列唯一执行。

下一步仍包括普通错误前缀、JOIN/CTE物理来源声明、字符数组新接收器的遗留声明一致性、表达式索引AST、ANALYZE virtual统计、legacy LATERAL/EXCLUDED/default/BEFORE组合、目录和identity语法、完整操作/查询成本。既有R106/R177/R185/R180/R151及planning/portal/format2等用户暂停边界保持，未把这些普通剩余项新增为deferred。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。

完整原始数据见[本批机器归档](benchmarks/generated-column-demand-20261104.json.gz)。归档采用UTF8/base64/gzip+base64逐项保存原始字节并校验SHA256，摘要和计数可直接读取；压缩只改变归档表示。新增一份非Markdown归档，既有754个Markdown路径、原273项、原256条历史原文、725保护记录及五份旧归档保持。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批基于 `019cd761`，本批候选 P5-C2 `a3baaca6…`：0.2.0 / C++17 / GCC 13.3 / O2 / ICU 74.2 / zlib 1.3 / TLS stub。真实默认对照始终是已安装的 `425165af…`，不是前批源码 C4 `9d85c688…`。本批未迁移 C++23，未替换根目录默认程序或重启服务。Goal 继续 active，273 项仍为 22 complete /166 partial /70 unverified /15 deferred_by_user。

实际收益集中在统计维护及范围修改：ANALYZE p50 为 **369.369 → 73.629 → 402.820 ms**，范围 UPDATE 为 **599.605 → 388.647 → 649.714 ms**，范围 DELETE 为 **483.209 → 181.173 → 500.434 ms**（原版首次 / 候选 / 原版末次）。但完整 35 操作为 **4 快 /28 慢 /3 居间**，36 组查询为 **4 快 /6 慢 /26 居间**。整体性能提升仍未验收，默认继续保持 425；不能把局部收益概括为所有操作已优化完成。

### 调研和实际瓶颈

PostgreSQL 的自动 ANALYZE 使用后台 worker、变化行数阈值及规模因子，并通过采样收集统计；SQLite 的 PRAGMA optimize 按需要选择并限制 ANALYZE 工作；DuckDB 的 ANALYZE 重算供 join-order 优化使用的统计。这些实现说明统计维护本身需要单独控制和测量，不能只看查询算子。[PostgreSQL 18 手册](https://www.postgresql.org/docs/18/routine-vacuuming.html)、[SQLite 官方说明](https://www.sqlite.org/lang_analyze.html)、[DuckDB 官方说明](https://duckdb.org/docs/2.0/sql/statements/analyze)。另核对了固定版本 PostgreSQL 18.6、SQLite 3.51.0、DuckDB 1.4.2 的相关比较/统计源码，URL 和 SHA 在机器归档中；未把不同版本当作同一实测程序。

真实 Main 路径的原始诊断共 32 次 UPDATE（1450 条实际 SQL、2 个 prepare 标记），追加的 selected 与 tail 诊断各 4 次 UPDATE（各 366 条实际 SQL、1 个 prepare 标记）。它们使用独立的原 425/C4 观察程序及原始源码/头文件/真实 Main；不是正常 SQL 速度样本。`perf stat` 因内核权限退出 255，未修改内核设置，之后用私有计时观察器定位。

默认 `autoAnalyzeEnabled=true`、阈值 50，原 50 行 UPDATE 每次同步触发 ANALYZE。四次 C4 tail 诊断的线程 CPU 为 412.707–431.121 ms，其中 heap 收集 79.382–80.698 ms、统计排序/MCV 332.226–351.135 ms、统计载入/保留/原子写入 0.486–0.618 ms；heap 每次实际 6001 个 getPageAllocator 调用。原 `selected_processing_inclusive` 包含 AFTER trigger 和自动统计后置部分，不能称为独占的 50 行更新体。在独立 selected4 诊断中，八段行体合计 process CPU 为 145.347–152.653 ms。

前批“真实 Main 约 2000 次重复解析”的估计已撤回；实际 prepared matcher 仍绕过 filterRows 的语法缓存。未变主键的唯一性检查已有提前返回，也没有证据支持 50×1000 次未变主键全表检查的推断。

### 生产改动和边界

- ANALYZE 在原 self-comparability 校验后，为同一次/同一列复用整数、布尔、浮点键或拥有的系统 locale/facet，逐对仍调用原 facet.compare，不使用 transform/strxfrm 权重。保留默认排序的原字节平局规则、显式 locale 错误、未知值分区、原字符串代表值与稳定 MCV 频次平局。numeric、temporal、enum、nocase 等不适合的路径继续使用原比较器。可选缓存准备失败则整列回到原路径。
- ANALYZE 仅在现行物理格式、无虚拟列的表上，在首个实际需要的非 NULL nullable 字段处校验当前文件及同一 pinned tuple，再为当前回调拥有 NULL 位图。原物理 NULL 跳过顺序、提取顺序和原子统计写入保持；all-NULL/NOT-NULL-only、虚拟列或不能连贯捕获的情况保留原提取。文件校验频率变为每个实际使用行一次，不声称保留原每个单元格的瞬时故障顺序或修复 R285。
- REINDEX 保留原 2–8 个简单私有树的单次 heap 扫描，只为真实索引依赖保留逻辑 map 条目；所有物理字段仍按原顺序解码，主键辅助函数也仍有原完整解码。INCLUDE/复合列使用真实元数据，未知或歧义声明退回同一单次扫描的完整 map。原私有构建、校验、交换和失败回滚主体保持。

没有关闭/延后自动统计、提高阈值、减少采样、削弱持久化或改共享排序/比较器、PageAllocator、StoredRowNull、AtomicFile、参数帧与用户暂缓模块。位图缓冲绑定本身保持 RAII 恢复；原 forEachRow 既有异常路径的手工 TLS 清理没有修复。未制造 legacy tuple、瞬时每 cell 文件故障或并发文件代际测试。每列标量键增加 O(N) 空间，ANALYZE 原全量收集仍为 O(N)，不是 PostgreSQL 完整统计/采样实现。

### 正确性和版本来源

正常 CMake 从前批实际 fresh62 源流重编 1 TU、依赖检查后复用 61 TU；228 份源码输入、158 份原头文件、62 对象依赖及新编译的 test stub 均有独立冻结与 SHA。原 2329 份测试文件未改，新增三个永久原生入口后为 2332 文件；顶层 CPP 为 1083，扣链接桩后为 1082 个顶层入口，另有原五个子目录 CPP。没有宣称全部入口重跑。

[比较缓存回归](../tests/analyze_comparison_cache_test.cpp)在 8 个规模 ×15 列 ×2 次分析中完成 240 个完整列统计检查，另有 3 个原 Unknown/NULL 转换控制；旧 C4 与 C2 的 16 份完整落盘统计字节一致，包含精确大整数、浮点 NaN/Inf/零、布尔、numeric 回退、默认/显式/二进制/反向/nocase/system locale、char 平局、UUID、真 NULL 和长 TOAST。[NULL caller 回归](../tests/analyze_bitmap_caller_test.cpp)旧 C4/C2 各 12/0，覆盖空/单行/all-NULL/NOT-NULL、空串和字面 NULL、最大合法整数、numeric、stored generated、公开真实绑定/嵌套分析、virtual 回退、其他表及多列统计保存、冷读、真实 owner 析构后缺失 heap 时不发布统计。[REINDEX 物化回归](../tests/reindex_materialization_test.cpp)8/0，核对所有值/真 NULL、真实主辅索引 RID/顺序、INCLUDE/复合/引号名称、冷读和活跃修改后的回滚。该永久入口是值/RID 功能断言，解码需求来自另一个私有配对观察器。

原有相关 22 个原生入口实际全部 exit 0；独立静态复核 18/18。原 273 条状态及 255 条近期历史保留，新增本批 partial 历史后为 256；725 份保护记录、原四份机器归档保持。未将独立未冻结/未编译的生成列需求 prototype 或仅 PostgreSQL 参考结果算成本批产品通过。

### 独立 REINDEX 成本，不能替代完整 SQL

真实公开 API 四进程（旧 C4/C1 ×默认/计数分配器）完成全部 60 个阶段：12 setup、48 durable REINDEX，原 N=0/1/1000 及每个规模四次重建全部保留。1000 行每次少 3000 个 C++ new 请求、287984 B 累计请求字节，默认分配器 CPU 中位数 278.704→239.090 ms（-14.21%）。N=0 请求数相同但多 16 B；N=1 少 3 请求/272 B。计数 lane 的 N=0/1 CPU 和 wall 有负值，全部保留；旧默认 N=1000 wall 有秒级 I/O 波动，不能以该 wall 比例称为稳定 SQL 加速。请求字节不是 live memory/RSS/峰值。

初版 8 项私有解码观察中仅空表通过，其他 7 项因错误预期每列 N 次而提前失败；真实主键辅助与逻辑 map 各一遍，原算法实际为 2N。保留初版并只修观察预期为 2N 后，旧 C4/C1 各 8/0、全 stdout 相同，实际各 3344 个 decoder 入口；全部数据/RID/回滚后置检查均执行。没有把减少 map 节点说成跳过了物理字段。

### 55 项资源与恢复控制

全部 55 请求、63 检查完成：40 个完整读取、14 个 SET/SHOW、1 个在途只读 spool 进程的预期 owned SIGKILL；完整校验 40000 行/118000 单元格，5 个普通 server context 正常退出，测试 owned server 为空。同目录重开和清理均保留真实校验；这不是任意写入崩溃恢复或所有算子的硬内存上限证明。FD、RSS、CPU 和 I/O 原始观测保留在归档，不把观测快照当峰值。

### 全部 35 类操作：原版/候选/原版

原 benchmark_operations.py、全部 35 项、1000 行、batch 50、DDL rows 5、3 rounds ×2 samples；三次独立环境各 210，共 630 个持久完成并经原结果验证的动作，0 warmup 丢弃，全部驱动正常 exit 0。动作数不是 SQL 请求数。动作调用至返回（含其持久完成，以及 SELECT 等动作内部本身执行的原结果校验）计入 wall；setup、动作返回后的原 verify(context)、cleanup，以及计时两侧的 /proc 采样均在 wall 外。CPU/I/O 来自两次非原子快照，边界不与 wall 完全等长；原数据照录。

| 操作/查询 | 原版首次 ms | C2 ms | 原版末次 ms | 对首次 % | 对末次 % | 原版漂移 % | 结论 |
|---|---|---|---|---|---|---|---|
| select_constant | 1.164 | 1.705 | 1.050 | +46.54 | +62.37 | -9.75 | 慢于两次原版 |
| select_pk_lookup | 18.278 | 37.595 | 20.454 | +105.68 | +83.80 | +11.90 | 慢于两次原版 |
| select_pk_prepared | 24.251 | 45.463 | 52.475 | +87.46 | -13.36 | +116.38 | 居间或相等 |
| select_unindexed_lookup | 67.086 | 103.209 | 73.592 | +53.85 | +40.25 | +9.70 | 慢于两次原版 |
| select_range_sum | 110.819 | 155.724 | 159.259 | +40.52 | -2.22 | +43.71 | 居间或相等 |
| select_group_aggregate | 68.709 | 96.899 | 76.879 | +41.03 | +26.04 | +11.89 | 慢于两次原版 |
| select_sort_limit | 36.561 | 74.346 | 59.944 | +103.35 | +24.03 | +63.96 | 慢于两次原版 |
| select_join_count | 147.316 | 181.295 | 177.449 | +23.07 | +2.17 | +20.45 | 慢于两次原版 |
| insert_autocommit | 24.849 | 63.091 | 29.581 | +153.90 | +113.29 | +19.04 | 慢于两次原版 |
| insert_multi_values | 198.039 | 266.721 | 259.855 | +34.68 | +2.64 | +31.21 | 慢于两次原版 |
| insert_explicit_transaction | 313.568 | 403.481 | 369.903 | +28.67 | +9.08 | +17.97 | 慢于两次原版 |
| update_primary_key | 77.537 | 104.030 | 83.551 | +34.17 | +24.51 | +7.76 | 慢于两次原版 |
| update_secondary_index | 78.372 | 125.990 | 113.226 | +60.76 | +11.27 | +44.47 | 慢于两次原版 |
| update_cte_returning | 31.067 | 70.931 | 57.510 | +128.31 | +23.34 | +85.11 | 慢于两次原版 |
| update_range | 599.605 | 388.647 | 649.714 | -35.18 | -40.18 | +8.36 | 快于两次原版 |
| delete_primary_key | 37.441 | 70.920 | 38.189 | +89.42 | +85.71 | +2.00 | 慢于两次原版 |
| delete_cte_returning | 29.643 | 71.776 | 36.255 | +142.13 | +97.98 | +22.30 | 慢于两次原版 |
| delete_range | 483.209 | 181.173 | 500.434 | -62.51 | -63.80 | +3.56 | 快于两次原版 |
| transaction_read_commit | 11.977 | 31.989 | 17.169 | +167.10 | +86.32 | +43.36 | 慢于两次原版 |
| transaction_write_rollback | 29.028 | 75.932 | 30.473 | +161.58 | +149.18 | +4.98 | 慢于两次原版 |
| transaction_savepoint | 130.519 | 215.902 | 177.793 | +65.42 | +21.43 | +36.22 | 慢于两次原版 |
| sequence_nextval | 15.717 | 51.476 | 41.210 | +227.53 | +24.91 | +162.21 | 慢于两次原版 |
| copy_from | 212.067 | 230.337 | 263.310 | +8.61 | -12.52 | +24.16 | 居间或相等 |
| copy_to | 96.503 | 126.674 | 120.360 | +31.26 | +5.25 | +24.72 | 慢于两次原版 |
| toast_insert_6k | 50.165 | 90.104 | 55.713 | +79.61 | +61.73 | +11.06 | 慢于两次原版 |
| ddl_create_table | 1769.263 | 2523.164 | 1960.200 | +42.61 | +28.72 | +10.79 | 慢于两次原版 |
| ddl_alter_add_column | 1723.163 | 2716.079 | 1926.419 | +57.62 | +40.99 | +11.80 | 慢于两次原版 |
| ddl_truncate | 1512.184 | 2617.500 | 2205.474 | +73.09 | +18.68 | +45.85 | 慢于两次原版 |
| ddl_drop_table | 1758.319 | 2586.176 | 1877.069 | +47.08 | +37.78 | +6.75 | 慢于两次原版 |
| index_create_btree | 1678.042 | 2754.873 | 2315.906 | +64.17 | +18.95 | +38.01 | 慢于两次原版 |
| index_create_hash | 1535.697 | 2574.963 | 1993.874 | +67.67 | +29.14 | +29.84 | 慢于两次原版 |
| maintenance_analyze | 369.369 | 73.629 | 402.820 | -80.07 | -81.72 | +9.06 | 快于两次原版 |
| maintenance_vacuum | 5.395 | 4.610 | 4.631 | -14.54 | -0.45 | -14.15 | 快于两次原版 |
| maintenance_reindex | 278.582 | 331.909 | 326.213 | +19.14 | +1.75 | +17.10 | 慢于两次原版 |
| maintenance_checkpoint | 25.307 | 66.545 | 44.547 | +162.95 | +49.38 | +76.03 | 慢于两次原版 |


分类仅比较候选 p50 与两次原版 p50，不是置信区间或普遍速度承诺。全 630 样本的 CPU/RSS/I/O、顺序、原慢值及两次原版漂移均在归档。28 个 wall 慢项中 15 项 server CPU p50 也高于两次原版，不能统称仅墙钟噪声；CPU 来自 /proc 的约 10ms tick，0/相等也不表示无成本。范围 UPDATE CPU 为 565→290→575 ms，DELETE 为 445→140→480，ANALYZE 为 370→60→400；完整 REINDEX CPU 为 260→285→280，与独立旧 C4/C1 API 夹具是不同对照。

全部 35 项 rchar/syscr/read_bytes 的中位数三代相同，14 个非零 read_bytes 单样本仍保留。候选动作后 RSS 快照全 35 项更高：较首轮 +204..2146 KiB，较末轮 +324..2400 KiB；不能归因为某个缓存或当作真实峰值，也不能据此推断源码因果。

### 全部 36 组完整查询

固定 1072 文件夹具、原 12 SQL/全值 oracle、64kB/4MB/16MB 三预算，原版/候选/原版各 12 timed +3 warm：1296 timed、324 warm、18 SET/SHOW，共 1638 SQL，完整验证 1620000 行/4725000 单元格。9 个普通 server context 正常 exit 0，原全部 1620 JSONL/CSV 读取记录通过静态核对，没有挑选样本或重跑快项。

| 预算 | 操作/查询 | 原版首次 ms | C2 ms | 原版末次 ms | 对首次 % | 对末次 % | 原版漂移 % | 结论 |
|---|---|---|---|---|---|---|---|---|
| 64kB | partition_ranking | 188.570 | 198.087 | 137.682 | +5.05 | +43.87 | -26.99 | 慢于两次原版 |
| 64kB | partition_lag | 155.753 | 137.811 | 119.275 | -11.52 | +15.54 | -23.42 | 居间或相等 |
| 64kB | partition_lead | 127.152 | 144.036 | 122.394 | +13.28 | +17.68 | -3.74 | 慢于两次原版 |
| 64kB | running_sum | 157.685 | 145.204 | 113.123 | -7.92 | +28.36 | -28.26 | 居间或相等 |
| 64kB | sliding_sum | 143.442 | 143.102 | 119.055 | -0.24 | +20.20 | -17.00 | 居间或相等 |
| 64kB | partition_full_sum | 145.318 | 143.606 | 117.458 | -1.18 | +22.26 | -19.17 | 居间或相等 |
| 64kB | full_first_last_nth | 205.881 | 192.603 | 181.771 | -6.45 | +5.96 | -11.71 | 居间或相等 |
| 64kB | mixed_same_keys | 190.857 | 195.339 | 166.486 | +2.35 | +17.33 | -12.77 | 慢于两次原版 |
| 64kB | length_lag_1 | 528.852 | 464.503 | 452.472 | -12.17 | +2.66 | -14.44 | 居间或相等 |
| 64kB | length_lag_2 | 530.525 | 485.993 | 468.001 | -8.39 | +3.84 | -11.79 | 居间或相等 |
| 64kB | length_lag_4 | 557.464 | 570.292 | 519.425 | +2.30 | +9.79 | -6.82 | 慢于两次原版 |
| 64kB | duplicate_running_sum | 155.448 | 134.097 | 113.483 | -13.73 | +18.17 | -27.00 | 居间或相等 |
| 4MB | partition_ranking | 113.154 | 110.573 | 125.222 | -2.28 | -11.70 | +10.67 | 快于两次原版 |
| 4MB | partition_lag | 101.966 | 100.725 | 101.227 | -1.22 | -0.50 | -0.73 | 快于两次原版 |
| 4MB | partition_lead | 101.380 | 98.603 | 100.127 | -2.74 | -1.52 | -1.24 | 快于两次原版 |
| 4MB | running_sum | 106.583 | 102.551 | 95.108 | -3.78 | +7.83 | -10.77 | 居间或相等 |
| 4MB | sliding_sum | 109.918 | 110.871 | 100.194 | +0.87 | +10.66 | -8.85 | 慢于两次原版 |
| 4MB | partition_full_sum | 102.955 | 99.474 | 96.204 | -3.38 | +3.40 | -6.56 | 居间或相等 |
| 4MB | full_first_last_nth | 118.745 | 120.471 | 120.526 | +1.45 | -0.05 | +1.50 | 居间或相等 |
| 4MB | mixed_same_keys | 117.221 | 117.708 | 116.860 | +0.42 | +0.73 | -0.31 | 慢于两次原版 |
| 4MB | length_lag_1 | 460.128 | 436.651 | 434.540 | -5.10 | +0.49 | -5.56 | 居间或相等 |
| 4MB | length_lag_2 | 450.679 | 443.327 | 438.316 | -1.63 | +1.14 | -2.74 | 居间或相等 |
| 4MB | length_lag_4 | 473.007 | 463.460 | 462.535 | -2.02 | +0.20 | -2.21 | 居间或相等 |
| 4MB | duplicate_running_sum | 105.425 | 100.493 | 106.611 | -4.68 | -5.74 | +1.13 | 快于两次原版 |
| 16MB | partition_ranking | 103.546 | 105.592 | 527.032 | +1.98 | -79.96 | +408.99 | 居间或相等 |
| 16MB | partition_lag | 100.645 | 108.378 | 318.033 | +7.68 | -65.92 | +215.99 | 居间或相等 |
| 16MB | partition_lead | 98.013 | 108.868 | 255.223 | +11.08 | -57.34 | +160.40 | 居间或相等 |
| 16MB | running_sum | 100.120 | 111.536 | 784.193 | +11.40 | -85.78 | +683.25 | 居间或相等 |
| 16MB | sliding_sum | 98.722 | 103.210 | 770.881 | +4.55 | -86.61 | +680.86 | 居间或相等 |
| 16MB | partition_full_sum | 98.210 | 102.008 | 601.821 | +3.87 | -83.05 | +512.79 | 居间或相等 |
| 16MB | full_first_last_nth | 109.874 | 128.297 | 569.538 | +16.77 | -77.47 | +418.35 | 居间或相等 |
| 16MB | mixed_same_keys | 111.101 | 126.075 | 1444.012 | +13.48 | -91.27 | +1199.73 | 居间或相等 |
| 16MB | length_lag_1 | 426.220 | 436.804 | 1122.720 | +2.48 | -61.09 | +163.41 | 居间或相等 |
| 16MB | length_lag_2 | 437.973 | 457.089 | 1121.360 | +4.36 | -59.24 | +156.03 | 居间或相等 |
| 16MB | length_lag_4 | 460.515 | 488.468 | 556.529 | +6.07 | -12.23 | +20.85 | 居间或相等 |
| 16MB | duplicate_running_sum | 100.497 | 102.006 | 323.521 | +1.50 | -68.47 | +221.92 | 居间或相等 |


### 首次失败和剩余工作

初次失败全部保留：REINDEX 观察器错误 N 预期、raw 官方网页下载 403、私有 native helper 初次相对 fresh-origin 路径 guard、永久 REINDEX 首次 stub 路径预检、22 项外层 driver 日志误占 nested helper 前缀造成的 0 编译/0 执行预检失败、NULL 草稿三处 private closeAllPageAllocators 编译错误、tail 请求 prepare 标记造成的静态归约 schema 错误、归约准备清单 literal\n 格式错误。后续分别修正观察/脚本/真实 owner fixture，保留原源码、日志、退出和原断言，没有修改产品接口权限、删 case 或宽松期望；不能把这些 harness 阶段叫作产品功能失败或通过。

本批没有新的 PostgreSQL 速度倍率测量。默认 425 继续保持；下一步对点查、单行写入、DDL、事务及完整 REINDEX 的实际慢路径做正常 Main 分段与分配/CPU 调研，查明原因后继续优化。普通生成列赋值/未选中需求、历史数组位图、quoted stored routine 元数据、更多目录和组合语义仍开放；273 项没有关闭。独立 API 的收益和 normal 全量的负值均须保留。

全部构建、原失败、控制、原始分布及静态核对见[本批机器归档](benchmarks/analyze-reindex-main-costs-20261104.json.gz)。下方为较早批次的历史记录，“当前”仅指其当时版本。

## RETURNING 声明、真实行值与操作热路径（2026-11-04）

本批基于 `5df73e63`，最终程序 C4 `9d85c688…`，仍为 0.2.0 / C++17 / GCC 13.3 / O2 / ICU 74.2 / zlib 1.3 / TLS stub。所有验证使用不可变候选和各自原始头文件、对象及新编译 test stub；没有用源码更新冒充根目录程序更新。Goal 保持 active，原 273 项状态仍为 22 complete /166 partial /70 unverified /15 deferred_by_user。

### 调研与实现

[PostgreSQL 18.6 的 analyze.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/analyze.c)从真实目标命名空间建立 OLD/NEW 后转换 RETURNING；[PG RETURNING 文档](https://www.postgresql.org/docs/18/dml-returning.html)说明行版本及 MERGE 的来源值。[SQLite trigger.c](https://github.com/sqlite/sqlite/blob/master/src/trigger.c)和[DuckDB bind_update.cpp](https://github.com/duckdb/duckdb/blob/main/src/planner/binder/statement/bind_update.cpp)用于对照投影与绑定组织方式，不以它们各自的限制代替 PG 要求。下载时间、完整 URL 和 SHA 均保留；最初 SQLite returning.c 的 404 及更正也保留。

本项目增加纯声明 API：读取真实目标/来源/CTE 元数据，只处理 RETURNING 命名与输出声明。网络 Parse 检查名字时不求值 SET、WHERE、VALUES、默认值或计数输入；私有解析用途只在两个返回数据的入口生效，异常后恢复原解析模式，既有完整 Binder 和参数/执行帧保持原路径。返回值按真实物理列 OID/attnum、实际数组元素类型和 typmod 构造拥有的描述符，不伪造 relation OID、不用列名猜 RID、不从已经拥有的复合值显示文本重建内容。

虚拟生成列在原求值位置保留实际拥有值，按同类型声明匹配接入 INSERT/UPDATE/DELETE 和三类 MERGE 动作。NULL 使用原计算结果，避免虚拟列误读堆 NULL 位图。MERGE 来源列从实际 USING 关系的选中元组携带进入返回投影；原 ON/WHEN/SET 调度不变，失败后清空未完成的公开结果并保留原异常。普通格式 0 的引用值，以及受支持普通数组在格式 0/1 的结果描述符，也使用真实类型身份；暂停的顶层枚举族沿原路径。该数据传递未增加 retained 参数帧或改变 provider 生命周期。

普通 legacy 行过滤（包括使用此路径的查询与修改）在一次 filterRows 调用内惰性保存原语法，每行仍克隆独立可变 AST，并执行原绑定、类型准备和求值。首次需求、空表、短路、时钟、异常顺序均保持；缓存只复用语法，没有复用行结果、回调、值、计划或参数帧。PG 的表达式初始化、SQLite VDBE 和 DuckDB 绑定表达式是研究参照；本实现仍有逐行克隆/绑定成本，不等同于编译表达式或向量化。

符合条件的普通 REINDEX 在 2–8 个私有树之间共享一次实际堆扫描；表达式/部分/虚拟索引和其他树数继续原路径，持久替换及回滚后缀保留。[PG B-tree 的 nbtsort.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/nbtree/nbtsort.c)实现索引排序与 bulk build，不能据此声称 PG 也使用本项目的多索引单扫描方式。此优化同时打开多个私有池和文件，有资源成本：8 指树数，不是总 FD 或 work_mem 硬上限；默认 128×4096B 页池每树约512KiB，8树帧数据约4MiB；若每池配置4096页，8树仅帧数据即可约128MiB，实际总内存还含其他结构。任意 I/O 故障先后顺序并未证明完全相同。

### 功能验证代别与普通剩余

C1 全新 CMake 实际编译62单元；C2 修改依赖重编16/复用46，C3 重编1/复用61，C4 重编2/复用60。每代均检查62对象及头依赖，原生链接排除 Main 的61对象并使用实际新编译 stub。二进制、源码228、命令、对象、依赖和完整日志均保留，不能把 C3 全选取集称为 C4 重跑。

新增5个原生入口123项：声明46为45通过/1差异，虚拟生成列28、语法复用14、REINDEX16、MERGE来源19全部通过，合计122通过/1差异。声明第36项仍严格要求 quoted stored extract 返回integer，实际text25；没有弱化断言，也没有为此改动用户暂停的 R180 共享被调函数路径。原生早期编译失败、错误观察前提修正、C3实际17/19及最终修复全部保留。

严格固定56项由旧425的48通过/8差异改善为C4的55/1，六个 OLD/NEW Describe 与虚拟数组正例已通过，物理历史数组 NULL 位图第53项仍失败。新增44场景×Describe/text/binary的132项，实际 PG18.6 全过，旧425为9通过/123差异，C4为132通过/0差异；检查真正目录 relation OID/attnum、声明、全部原始协议帧、NULL/数组下界/复合字段、错误阶段及修改后状态。另一组33项生成列赋值/需求观察，PG33全过、旧425和C4均27通过/6差异；smallint范围、varchar长度、numeric刻度及虚拟除零需求仍是普通缺口，不能通过预先转换全部未选中列来修复。

原五组301/83/111/68/16共579观察保持原结果：188/113、81/2、109/2、57/11、16/0，共451匹配/128差异。暂停的顶层枚举九项原始记录保持，未运行用户暂停的原341/103。来源星号/更多名称形式、生成列赋值语义、物理历史位图、更多目录/域/命名复合/输入分析仍开放；没有新增用户暂缓或关闭整个功能族。

C3 完整选取148原生为146通过/2个既有SIGABRT；95协议为86通过/9个非零入口，其中 CTE owner 首次启动103、0 SQL 是覆盖缺口。旧425原生146/148，协议85/95，routine body 一次180s超时有1695条完整已打印观察，C3则完成1696项。共同13910条中13909在源码实际fixture UUID重命名后相同，唯一PID值不同但两侧原谓词均True；旧True到新False为0。另有13个非标准输出/缺失采集对照限制及旧未观察后缀；原始PID、失败组件和超时均保留，不把覆盖缺口或只打印的前缀算作通过。C4最终57原生为56通过/1个既有math失败，31协议为24通过/7个既有断言失败入口，无启动失败或超时。10533个共同观察中10532在实际fixture重命名后相同，唯一PID原始值不同但两侧谓词均True，旧True到新False为0。CTE本代完整执行747项/29个既有差异，全部actual/expected与旧425一致；补齐了C3首次启动失败的覆盖缺口。

### 实际性能与资源

本批未通过本机默认程序的整体性能提升验收，根目录继续使用425（425165af…）。C4源码及严格功能增量保留，全部测量一次执行、不选择性重跑。范围UPDATE和REINDEX的回退继续作为优化任务。

35类操作按425/C4/425在三个独立目录各完整执行3轮×2样本，1000行、batch50、DDL5行，共630次操作，无预热或丢弃。SELECT校验在原计时内，修改后验证/夹具建立/清理在计时外；原WAL/fsync与持久化不变。三个上下文均正常退出，全部结果校验通过。7快/19慢/9居间，旧基线漂移−42.00%至+56.30%。范围UPDATE为565.975→640.828→546.169ms，慢13.23%/17.33%，CPU快照中位数540→570→540ms；REINDEX275.836→282.127→262.427ms，慢2.28%/7.51%。单行自动提交INSERT为26.645→47.940→17.595ms，CHECKPOINT22.432→40.802→18.361ms；这些负值也未删除。

变化为C4/旧−1，负数表示本次观测更快。分类只按p50是否低于/高于两次旧版，不代表统计显著性或已证明某项实现是原因。全部原始6值、min/p50/max、CPU/RSS/I/O见机器归档；CPU/RSS是进程计数/采样，不是真实峰值。

| 操作/查询 | 旧首 / C4 / 旧末 p50 ms | 对旧首 / 旧末变化 | 旧基线漂移 | 分类 |
| --- | ---: | ---: | ---: | --- |
| select_constant | 1.158 / 1.032 / 1.069 | -10.93% / -3.44% | -7.75% | 快 |
| select_pk_lookup | 15.948 / 16.984 / 14.580 | +6.49% / +16.49% | -8.58% | 慢 |
| select_pk_prepared | 24.291 / 25.260 / 16.809 | +3.99% / +50.27% | -30.80% | 慢 |
| select_unindexed_lookup | 64.525 / 61.193 / 57.238 | -5.16% / +6.91% | -11.29% | 居间 |
| select_range_sum | 120.396 / 141.923 / 111.153 | +17.88% / +27.68% | -7.68% | 慢 |
| select_group_aggregate | 73.120 / 67.483 / 62.523 | -7.71% / +7.93% | -14.49% | 居间 |
| select_sort_limit | 34.891 / 51.185 / 40.251 | +46.70% / +27.16% | +15.36% | 慢 |
| select_join_count | 150.042 / 148.971 / 172.592 | -0.71% / -13.69% | +15.03% | 快 |
| insert_autocommit | 26.645 / 47.940 / 17.595 | +79.92% / +172.46% | -33.97% | 慢 |
| insert_multi_values | 194.130 / 217.526 / 176.259 | +12.05% / +23.41% | -9.21% | 慢 |
| insert_explicit_transaction | 300.537 / 299.027 / 293.612 | -0.50% / +1.84% | -2.30% | 居间 |
| update_primary_key | 65.029 / 81.691 / 76.890 | +25.62% / +6.24% | +18.24% | 慢 |
| update_secondary_index | 77.020 / 84.484 / 73.446 | +9.69% / +15.03% | -4.64% | 慢 |
| update_cte_returning | 27.838 / 36.227 / 29.177 | +30.14% / +24.16% | +4.81% | 慢 |
| update_range | 565.975 / 640.828 / 546.169 | +13.23% / +17.33% | -3.50% | 慢 |
| delete_primary_key | 46.279 / 42.509 / 26.841 | -8.15% / +58.37% | -42.00% | 居间 |
| delete_cte_returning | 25.859 / 23.300 / 23.669 | -9.89% / -1.56% | -8.47% | 快 |
| delete_range | 480.938 / 477.370 / 465.490 | -0.74% / +2.55% | -3.21% | 居间 |
| transaction_read_commit | 13.865 / 15.925 / 12.943 | +14.86% / +23.04% | -6.65% | 慢 |
| transaction_write_rollback | 33.619 / 26.168 / 28.026 | -22.16% / -6.63% | -16.64% | 快 |
| transaction_savepoint | 127.363 / 114.757 / 116.221 | -9.90% / -1.26% | -8.75% | 快 |
| sequence_nextval | 12.941 / 18.296 / 13.604 | +41.39% / +34.49% | +5.13% | 慢 |
| copy_from | 197.452 / 215.985 / 186.612 | +9.39% / +15.74% | -5.49% | 慢 |
| copy_to | 95.882 / 110.394 / 105.997 | +15.13% / +4.15% | +10.55% | 慢 |
| toast_insert_6k | 38.640 / 51.277 / 35.292 | +32.70% / +45.29% | -8.67% | 慢 |
| ddl_create_table | 1069.143 / 1002.735 / 1033.474 | -6.21% / -2.97% | -3.34% | 快 |
| ddl_alter_add_column | 1175.517 / 1175.805 / 1003.459 | +0.02% / +17.18% | -14.64% | 慢 |
| ddl_truncate | 903.786 / 1327.016 / 1412.644 | +46.83% / -6.06% | +56.30% | 居间 |
| ddl_drop_table | 1466.711 / 1317.559 / 998.893 | -10.17% / +31.90% | -31.90% | 居间 |
| index_create_btree | 2021.792 / 1658.844 / 1266.874 | -17.95% / +30.94% | -37.34% | 居间 |
| index_create_hash | 1336.537 / 1592.833 / 1300.619 | +19.18% / +22.47% | -2.69% | 慢 |
| maintenance_analyze | 375.294 / 373.866 / 378.819 | -0.38% / -1.31% | +0.94% | 快 |
| maintenance_vacuum | 5.712 / 4.158 / 3.408 | -27.22% / +21.98% | -40.33% | 居间 |
| maintenance_reindex | 275.836 / 282.127 / 262.427 | +2.28% / +7.51% | -4.86% | 慢 |
| maintenance_checkpoint | 22.432 / 40.802 / 18.361 | +81.89% / +122.22% | -18.15% | 慢 |

完整查询固定12条、1000行、三档预算，每个版本/查询/预算保留12个计时样本和各轮预热。旧首/C4/旧末九个上下文正常退出；1296计时+324预热+18次SET/SHOW=1638请求，1620000行/4725000单元格与完整Python预期逐值校验，全部1620次实际结果JSONL和CSV分别核对。36组为25快/2慢/9居间；64kB为9/1/2、4MB为4/1/7、16MB为12/0/0。两慢项为64kB length_lag_2（+2.42%/+2.48%）和4MB length_lag_4（+0.13%/+2.22%），旧基线漂移−14.46%至+11.53%。全部108组raw12、min/max及首轮慢样本保留，没有新增PostgreSQL速度倍率。

| 预算 | 操作/查询 | 旧首 / C4 / 旧末 p50 ms | 对旧首 / 旧末变化 | 旧基线漂移 | 分类 |
| --- | --- | ---: | ---: | ---: | --- |
| 64kB | partition_ranking | 141.380 / 118.992 / 151.689 | -15.84% / -21.56% | +7.29% | 快 |
| 64kB | partition_lag | 107.831 / 105.627 / 112.683 | -2.04% / -6.26% | +4.50% | 快 |
| 64kB | partition_lead | 105.937 / 105.300 / 103.062 | -0.60% / +2.17% | -2.71% | 居间 |
| 64kB | running_sum | 120.431 / 102.101 / 119.997 | -15.22% / -14.91% | -0.36% | 快 |
| 64kB | sliding_sum | 123.557 / 106.557 / 112.829 | -13.76% / -5.56% | -8.68% | 快 |
| 64kB | partition_full_sum | 107.265 / 102.967 / 106.164 | -4.01% / -3.01% | -1.03% | 快 |
| 64kB | full_first_last_nth | 141.273 / 131.115 / 156.790 | -7.19% / -16.38% | +10.98% | 快 |
| 64kB | mixed_same_keys | 148.492 / 132.055 / 156.198 | -11.07% / -15.46% | +5.19% | 快 |
| 64kB | length_lag_1 | 464.423 / 445.538 / 445.190 | -4.07% / +0.08% | -4.14% | 居间 |
| 64kB | length_lag_2 | 451.302 / 462.226 / 451.040 | +2.42% / +2.48% | -0.06% | 慢 |
| 64kB | length_lag_4 | 509.779 / 476.482 / 504.564 | -6.53% / -5.57% | -1.02% | 快 |
| 64kB | duplicate_running_sum | 142.090 / 108.293 / 121.546 | -23.79% / -10.90% | -14.46% | 快 |
| 4MB | partition_ranking | 106.148 / 109.617 / 118.389 | +3.27% / -7.41% | +11.53% | 居间 |
| 4MB | partition_lag | 95.967 / 97.179 / 102.658 | +1.26% / -5.34% | +6.97% | 居间 |
| 4MB | partition_lead | 111.451 / 97.936 / 98.740 | -12.13% / -0.81% | -11.41% | 快 |
| 4MB | running_sum | 101.807 / 100.386 / 93.690 | -1.40% / +7.15% | -7.97% | 居间 |
| 4MB | sliding_sum | 96.296 / 98.074 / 101.051 | +1.85% / -2.95% | +4.94% | 居间 |
| 4MB | partition_full_sum | 97.856 / 97.464 / 90.001 | -0.40% / +8.29% | -8.03% | 居间 |
| 4MB | full_first_last_nth | 119.035 / 115.035 / 116.154 | -3.36% / -0.96% | -2.42% | 快 |
| 4MB | mixed_same_keys | 115.997 / 112.603 / 118.544 | -2.93% / -5.01% | +2.20% | 快 |
| 4MB | length_lag_1 | 432.236 / 431.706 / 430.352 | -0.12% / +0.31% | -0.44% | 居间 |
| 4MB | length_lag_2 | 466.676 / 439.133 / 437.787 | -5.90% / +0.31% | -6.19% | 居间 |
| 4MB | length_lag_4 | 465.203 / 465.795 / 455.680 | +0.13% / +2.22% | -2.05% | 慢 |
| 4MB | duplicate_running_sum | 102.839 / 99.219 / 105.223 | -3.52% / -5.71% | +2.32% | 快 |
| 16MB | partition_ranking | 113.506 / 107.665 / 109.511 | -5.15% / -1.69% | -3.52% | 快 |
| 16MB | partition_lag | 99.889 / 92.450 / 102.289 | -7.45% / -9.62% | +2.40% | 快 |
| 16MB | partition_lead | 107.280 / 91.436 / 100.448 | -14.77% / -8.97% | -6.37% | 快 |
| 16MB | running_sum | 103.992 / 95.699 / 98.446 | -7.97% / -2.79% | -5.33% | 快 |
| 16MB | sliding_sum | 110.305 / 95.845 / 112.354 | -13.11% / -14.69% | +1.86% | 快 |
| 16MB | partition_full_sum | 98.620 / 91.884 / 99.408 | -6.83% / -7.57% | +0.80% | 快 |
| 16MB | full_first_last_nth | 115.858 / 105.755 / 125.822 | -8.72% / -15.95% | +8.60% | 快 |
| 16MB | mixed_same_keys | 111.359 / 105.175 / 111.596 | -5.55% / -5.75% | +0.21% | 快 |
| 16MB | length_lag_1 | 433.762 / 428.493 / 434.094 | -1.21% / -1.29% | +0.08% | 快 |
| 16MB | length_lag_2 | 435.196 / 431.115 / 446.303 | -0.94% / -3.40% | +2.55% | 快 |
| 16MB | length_lag_4 | 461.323 / 444.644 / 471.337 | -3.62% / -5.66% | +2.17% | 快 |
| 16MB | duplicate_running_sum | 107.830 / 94.820 / 113.955 | -12.07% / -16.79% | +5.68% | 快 |

独立公开StorageEngine夹具另有4进程，默认分配器和C++new计数模式分开：108阶段记录，96个测量语句，8208查询行/32更新行及真实RID/主键/NULL验证。512行查询每次请求212976→143005、字节17632392→10082539；默认分配器四次进程CPU为145.231/149.010/149.652/141.030→82.459/80.256/82.625/82.522ms。512行UPDATE每次减少52611请求/3697887B，四次CPU101.816/101.578/101.170/96.244→68.788/70.849/68.861/69.431ms。四次是首次语句及三个后续独立语句，不是四个独立实验或同图重复执行。

空表请求数不变但每次多11B，UPDATE四次CPU2.945/2.153/1.935/1.881→3.133/3.963/3.690/3.469ms全部更慢；1行查询多36请求/7837B，UPDATE多22请求/3797B且CPU有快有慢。所有wall/process/threadCPU、计数模式时钟及初始化阶段保留。累计请求字节不等于live/RSS/峰值。

实际Main范围UPDATE经过prepared修改的matcher/resolver，以空条件调用updateRows，在真实堆扫描后逐行物化OLD SqlRow并求值绑定AST，未进入本批filterRows语法缓存。因此撤回此前把这条SQL归入约2000次legacy重复解析的推断；直接API的typedexpr夹具走另一条真实路径。下一步应量化实际Main行物化/绑定上下文，以及REINDEX索引键物化和持久化成本，不能用局部API收益解释本次SQL回退。

资源55为40次完整读取+14次SET/SHOW+1次按计划中断的只读spool，共40000行118000单元格，63/63记录检查通过。五个正常上下文退出，实际只读强杀后同目录重开，值/顺序正确；低64kB/高16MB分别观察到最多2/0个排序临时FD，EOF/关闭后为空。采样0不证明从未打开FD；不推导WindowAgg硬预算、真实峰值或任意写入崩溃恢复。


### 复现、文档与版本管理

研究与功能控制在11月3日形成，资源和最终性能采样在11月4日完成，各原始UTC保留。机器归档包含实际命令、35操作/12查询SQL和完整预期、1072文件夹具清单、三个原驱动源码及辅助脚本。操作对照使用scripts/benchmark_operations.py，依次指定旧425/C4/旧425及三个新输出目录，参数均为--rows 1000 --batch 50 --ddl-rows 5 --rounds 3 --samples 2。

完整查询使用归档中的runtime_benchmark_window_sharing_v1.py与原1072文件fixture，分别以--work-mem 64kB/4MB/16MB --rows 1000 --rounds 3 --samples 4执行，每档自动旧首/C4/旧末。归档保留原本地路径；移机只替换仓库、二进制和输出绝对路径，保持SQL、预期与夹具身份。完整实际返回行仍在原JSONL并逐值验证；机器归档保存每次验证hash、完整固定预期与JSONL位置/SHA，没有重复嵌入162万行。

本批下一步继续真实Main修改路径和REINDEX成本调查。性能回退及普通功能差距没有改成用户暂缓；严格失败没有弱化。


新增一份非 Markdown [机器归档](benchmarks/returning-declarations-hotpaths-20261104.json.gz)集中保存全量测量、函数/协议/资源结果、原失败、研究来源与复现命令；旧归档字节保持。原2324测试未改，新增5个入口后2329；原754个Markdown路径集合及725保护记录保持。九份既有文档同步，一条partial近期记录追加后255，原273对象/状态及254条历史原文保留。直接在 master 使用 Palpitate-xus <palpitate.xus@outlook.com> 提交，不建分支、不推送、不重启现有服务。

前批记录见[内置函数注册与复合RETURNING](#内置函数注册与复合-returning2026-11-03)：本机默认已更新为C3；固定查询36组p50均低于两次S3，35操作18快/2慢/15居间。PG仍有113/301差异，全部原始数据、负向成本与旧历史保留。

2026-11-02 已补充 [PostgreSQL 18.6 同机对照](#postgresql-186-同机对照2026-11-02)，原 10 月 30 日内部优化记录继续按原批次解释。

本轮在已有查询优化的基础上，继续优化提交状态、增删改、约束校验、DDL 重写和后台维护。编译标准仍为 C++17；收益来自执行路径和写放大优化，没有把语言标准升级计作加速因素。

## 测试范围与口径

- 35 类操作：8 类 SELECT，单条/批量/显式事务 INSERT，UPDATE/DELETE（含范围、索引维护和 CTE RETURNING），提交/回滚/保存点，序列，COPY 输入输出，6 KiB TOAST，建表/改表/TRUNCATE/删表，B-tree/Hash 建索引，以及 ANALYZE/VACUUM/REINDEX/CHECKPOINT。
- 主表 1,000 行，关联表 100 行，另有三个各 1,000 行的修改表；批量修改 20 行，ALTER/TRUNCATE/DROP 的独立夹具为 5 行。数据库整体大小、WAL 和其他对象也会影响这些操作。
- 每轮固定种子打乱操作顺序，每类连续采样 3 次。旧版第三轮 CHECKPOINT 返回真实错误，因此主表只比较两侧均完整的前两轮：每类 6 个样本，每侧 210 个样本。旧版原始记录仍为 `complete=false`，没有把失败样本当成延迟值或删除第三轮记录。
- 延迟为复用连接的客户端往返时间，包含修改及提交/持久化完成。建夹具、修改后验证和清理在计时外；查询结果校验在计时内。默认 WAL、fsync 和持久化设置保持开启，使用本机回环连接及 TLS stub。
- SELECT 与修改结果按预期值检查，RETURNING 同时检查返回值与落库结果，COPY TO 校验完整输出，TOAST 校验完整 6,144 字节。只读事务用例检查协议完成和错误；序列用例检查随后 `currval`。这两项不宣称逐项核验计时语句的所有返回单元格。
- 取样数小，p95 使用最近秩法；6 个样本的 p95 就是最大值。共享主机、操作顺序、后台维护、文件缓存和 WAL 大小会影响结果，表内倍率仅适用于本次夹具，不代表任意规模、并发或 SQL 组合。
- 两版各运行一次完整计划，按旧、新顺序进行，未交替重复；没有统计置信区间。CPU 与 I/O 是服务器进程的计数增量，包含同一时段的后台任务。

## 实现改动

1. **CLOG 减少写放大。** 保留每段 256 KiB 的完整逻辑格式、进程间锁、最新状态位合并、文件 fsync、原子 rename 和目录 fsync；临时文件的全零块用空洞表示，连续非零块合并写入。缓存校验用一次 stat 获取设备、inode、长度、mtime 和 ctime；发布时先在锁内关联缓存字节与文件身份，再释放锁，防止并发实例发布后误认旧缓存为新文件。
2. **复用已加载的表结构。** INSERT/UPDATE/DELETE、外键和唯一性校验、索引构建、DDL，以及排序/聚合/窗口内部路径读取存储行的 NULL 位图时复用当前表结构，减少同一行上的目录读取与解析。原生公开入口仍读取当前结构，NULL、空字符串和文本 `NULL` 保持区分。
3. **DDL 重写复用真实的外层回滚边界。** 仅在 DdlExecutor 持有活动、已标脏、完整物理回滚快照，且引擎、数据库和目标表一致时，内部重写行使用该语句边界，消除逐行创建整库保存点的开销。普通 INSERT、触发器发出的 INSERT、直接原生重写及缺少回滚所有者的调用保留原有保护。
4. **绑定 DML 提前筛选整数候选。** 无 FROM/USING 的 UPDATE/DELETE 对同类型 SMALLINT/INTEGER/BIGINT 直接列与字面量或语句参数的纯等值条件添加必要的堆过滤。原类型表达式继续复核，MVCC、RLS、SSI 和错误检查保留；不把这项写成 UPDATE/DELETE 已实现物理索引定位。
5. **协调维护操作。** 后台刷盘、自动 VACUUM、CHECKPOINT 与 DROP DATABASE 使用按数据库的维护所有权；前台可等待内部维护完成，同一 StorageEngine 的重叠 CHECKPOINT 复用实际成功或失败结果。不同引擎各自刷脏缓存；活动用户事务仍导致拒绝，刷盘/归档失败仍向上传播。后台检查点不再持有全局缓存锁等待其他维护，等待路径不持有维护锁读取全局事务集合。
6. **修复检查点截断后的 WAL 重开。** 旧版按段截断后，第一保留段可能从跨段记录的中间开始，启动完整性扫描会拒绝该 WAL。最终版从已有持久化检查点的精确位置出发，沿 `xl_prev` 验证 CRC、前驱方向和记录首尾衔接，找出保留段中的首条完整记录，再按 LSN 递增校验所有保留的完整记录。损坏/无效的锚点或记录继续报错；没有有效恢复起点的非对齐截断在删除文件前拒绝。复用现有检查点格式，没有增加磁盘格式标记。

主要源码：[CommitLog.cpp](../src/storage/CommitLog.cpp)、[WAL.cpp](../src/storage/WAL.cpp)、[TableManage.cpp](../src/commands/TableManage.cpp)、[DdlRewrite.h](../src/commands/DdlRewrite.h)、[DdlExecutor.cpp](../src/commands/DdlExecutor.cpp)、[DmlExecutor.cpp](../src/commands/DmlExecutor.cpp)。

## 实测结果

倍率为旧版 p50 ÷ 新版 p50；大于 1 表示本次观测更快，小于 1 表示更慢。所有操作均列出，未按收益筛选。

| 操作（一次的规模） | 优化前 p50 / p95 ms | 优化后 p50 / p95 ms | p50 倍率 |
| --- | ---: | ---: | ---: |
| SELECT 1 | 2.73 / 3.07 | 2.30 / 3.05 | 1.19× |
| 普通主键点查 | 32.56 / 70.60 | 32.12 / 40.78 | 1.01× |
| 参数化主键点查 int4 | 50.19 / 58.60 | 47.19 / 58.03 | 1.06× |
| 无索引文本等值 | 106.41 / 206.19 | 89.59 / 100.36 | 1.19× |
| 范围 SUM（100 行） | 157.73 / 338.56 | 150.86 / 202.51 | 1.05× |
| GROUP BY 聚合 | 111.65 / 267.98 | 100.86 / 108.56 | 1.11× |
| 双键排序 LIMIT 20 | 387.75 / 492.73 | 92.61 / 187.68 | 4.19× |
| JOIN 后 COUNT | 345.52 / 404.43 | 346.72 / 371.26 | 1.00× |
| INSERT 单行自动提交 | 46.08 / 190.62 | 48.21 / 76.88 | 0.96× |
| INSERT 多 VALUES（20 行） | 151.16 / 333.45 | 124.47 / 161.22 | 1.21× |
| INSERT 显式事务（20 条） | 215.76 / 362.47 | 193.38 / 230.89 | 1.12× |
| UPDATE 主键单行 | 366.84 / 437.98 | 106.59 / 139.28 | 3.44× |
| UPDATE 单行并维护二级索引 | 401.02 / 486.51 | 113.31 / 153.63 | 3.54× |
| CTE UPDATE 单行 RETURNING | 298.56 / 421.88 | 60.89 / 75.51 | 4.90× |
| UPDATE 范围（20 行） | 540.06 / 792.51 | 196.59 / 477.15 | 2.75× |
| DELETE 主键单行 | 392.52 / 541.14 | 57.48 / 62.50 | 6.83× |
| CTE DELETE 单行 RETURNING | 360.36 / 429.73 | 61.58 / 70.66 | 5.85× |
| DELETE 范围（20 行） | 415.41 / 860.36 | 158.96 / 526.54 | 2.61× |
| 只读事务 BEGIN/SELECT/COMMIT | 37.36 / 188.88 | 30.62 / 39.63 | 1.22× |
| 写事务 INSERT 后 ROLLBACK | 84.18 / 219.53 | 60.08 / 94.86 | 1.40× |
| 事务 UPDATE/保存点/回滚/提交 | 646.97 / 892.20 | 191.22 / 216.51 | 3.38× |
| 序列 nextval | 40.14 / 42.67 | 38.72 / 57.48 | 1.04× |
| COPY FROM（20 行） | 150.46 / 277.36 | 129.17 / 167.11 | 1.16× |
| COPY TO（1,000 行） | 127.12 / 260.54 | 103.73 / 130.70 | 1.23× |
| TOAST INSERT（6 KiB） | 103.06 / 392.90 | 78.10 / 148.28 | 1.32× |
| CREATE TABLE 空表 | 2030.07 / 2188.64 | 1705.77 / 1962.53 | 1.19× |
| ALTER ADD COLUMN（5 行） | 18453.43 / 21758.37 | 1970.73 / 2049.80 | 9.36× |
| TRUNCATE（5 行） | 2109.86 / 2307.75 | 1630.56 / 1937.31 | 1.29× |
| DROP TABLE（5 行） | 2210.86 / 2972.99 | 1906.12 / 2238.40 | 1.16× |
| CREATE INDEX B-tree（1,000 行） | 2451.75 / 3071.46 | 1941.16 / 2056.72 | 1.26× |
| CREATE INDEX Hash（1,000 行） | 2330.76 / 3075.81 | 1614.65 / 2640.75 | 1.44× |
| ANALYZE（1,000 行） | 397.83 / 491.68 | 372.10 / 418.68 | 1.07× |
| VACUUM 删除表 | 4.80 / 6.43 | 4.86 / 159.06 | 0.99× |
| REINDEX（1,000 行） | 335.05 / 447.28 | 318.68 / 354.31 | 1.05× |
| CHECKPOINT | 67.37 / 204.94 | 57.61 / 70.72 | 1.17× |

旧版共保留 297 个成功样本及第三轮失败记录；新版完整完成 315 个样本，35 类各 9 次，结果检查与协议错误数均为 0。主表使用双方前两轮各 210 个样本，第三轮新版数据另留在原始 JSON 中，没有混入共同窗口。

普通点查接近持平，单行 INSERT 的 p50 为 46.08→48.21 ms，VACUUM 为 4.80→4.86 ms；不能据此声称所有操作都稳定加速。较明显的改善集中在排序、UPDATE/DELETE、保存点及 ALTER 重写。

第三轮单行 INSERT 的三个原始样本为 195.24、56.76、203.29 ms，服务器进程 CPU 增量分别为 20、30、20 ms，显示这一时段有较大的等待开销；现有计数不足以确定每次等待的唯一来源。第三轮数据和新版本的全部长尾继续保留。

### 相邻补测

主测中单行 INSERT 等项目收益不明显，因此补充相邻旧版—新版—旧版对照。三次均使用同一成熟夹具的独立副本、相同 SQL 顺序和默认持久化/后台维护设置，每项 6 个样本。为让旧读取器能重开该夹具，在每个副本中恢复其已归档的完整 WAL 前缀；该准备不计时，三个输入完全相同，源目录文件哈希未变。

| 操作 | 旧版先测 p50 ms | 最终版 p50 ms | 旧版后测 p50 ms |
| --- | ---: | ---: | ---: |
| 范围 SUM | 161.80 | 165.69 | 157.88 |
| 单行 INSERT | 57.93 | 56.21 | 51.81 |
| CREATE TABLE | 3203.51 | 3157.45 | 3253.59 |

补测显示明显的时段和夹具影响，单行 INSERT 与范围 SUM 没有证明稳定、普遍的收益；建表仍受较大的固定开销影响。这是独立补测，未替换主表样本。54 次计时均完成且结果校验通过，原始记录和驱动见 `adjacent-regression-probes-v2/{results.json,summary.json}` 与 `adjacent_regression_probes.py`。

### 前一轮查询优化记录

前一轮先优化了目录发布检查、扫描 NULL 解码、参数化整数主键索引访问与整数 JOIN 候选过滤。本轮的“优化前”已经包含这些改动。
以下是原始版 `abe34a27…` 到本轮基线 `fbb066f4…` 的历史结果，使用同一 1,000 行查询夹具的独立副本、三轮热缓存测量；不是最终版的另一次对照，倍率不能与上表相乘。

| 查询 | 原始版 p50 ms | 本轮基线 p50 ms | 旧/新样本数 |
| --- | ---: | ---: | ---: |
| SELECT 1 | 1.390 | 1.392 | 900 / 900 |
| 普通主键点查 | 44.938 | 47.703 | 30 / 18 |
| 参数化主键点查 int4 | 2,947.000 | 95.425 | 9 / 17 |
| 无索引文本等值 | 739.058 | 166.212 | 9 / 9 |
| 范围 SUM | 2,993.472 | 198.014 | 9 / 9 |
| GROUP BY 聚合 | 1,917.560 | 178.732 | 9 / 10 |
| 双键排序 LIMIT 20 | 2,637.167 | 414.301 | 9 / 9 |
| JOIN 后 COUNT | 4,818.074 | 379.715 | 9 / 9 |

该轮普通点查没有证明稳定加速：单客户端吞吐主对照从 21.312 降至 11.615 QPS，相邻旧/新/旧补测也有明显波动；4 客户端聚合吞吐为 0.636→18.942 QPS，二者均保留。慢查询旧版样本较少。完整数据、不同查询的取样规则、早期失败与 23 项原生/26 项协议验证见本机 `build/query-opt-20261030-lwpnox22/{report.txt,comparison.json,validation.json}`。

## CLOG 写入字节对照

独立探针分别使用两版匹配的头文件与 CommitLog 对象，截取一次 `flush()` 的 write 调用，逐字节校验落盘文件，并重新打开读取状态；没有把文件空洞或 write 字节数当作端到端速度。

| 夹具 | 旧版 write 字节/次数 | 新版 write 字节/次数 | 两版逻辑文件长度 |
| --- | ---: | ---: | ---: |
| 仅一个已提交状态的年轻段 | 262,144 / 1 | 4,096 / 1 | 262,144 字节 |
| 每字节均有状态的稠密段 | 262,144 / 1 | 262,144 / 1 | 262,144 字节 |

年轻段本次发布的 write 字节减少 98.44%；稠密段仍为一次连续写入。稀疏文件的实际磁盘分配和收益取决于文件系统、事务数量和状态分布。

## 正确性与故障回归

最终候选使用 `stage-m-wal-retained-prefix` 的实际生产核心对象及二进制：**123 项原生入口（122 项主矩阵及 1 项独立退出回归）、45 项真实 PostgreSQL wire protocol 入口全部退出 0**。原生主矩阵与协议组串行运行，独立退出回归另行执行。原生测试链接实际生产核心对象，在私有临时数据目录执行；协议测试明确使用该候选。

- 原生覆盖 CLOG、WAL、INSERT/UPDATE/DELETE 故障原子性、触发器、DDL 回滚、保存点、目录、约束/RLS、NULL/TOAST、统计、VACUUM、备份恢复与索引。WAL 追加失败矩阵完整输出 `246 FAILURES 0`，不是将单个入口算成 246 个独立测试程序。
- 新增 [CLOG 稀疏发布](../tests/clog_sparse_publication_test.cpp)保留 14 项字节、短写、EINTR、截断/定位、fsync/rename 失败及重试检查；[发布身份竞态](../tests/clog_generation_publication_test.cpp)在实际解锁瞬间插入另一实例发布，修复前稳定断言失败，最终候选通过。
- [DDL 重写所有权](../tests/ddl_rewrite_row_owner_test.cpp)检查增加/删除/转换列、NULL/空串/最小整数，以及第三次 AFTER 触发器抛错后的整条语句回滚、审计表回滚、外层事务前缀与用户保存点保留。触发器发出的普通 INSERT 继续有自己的保护。
- [CHECKPOINT 合并](../tests/checkpoint_coalescing_test.cpp)与[后台维护占锁](../tests/background_maintenance_drop_test.cpp)用 fsync 门控制真实并发，覆盖等待、同引擎合并、不同引擎分别刷盘、真实 I/O 错误与重试、活动用户拒绝及 DROP；旧候选的对应控制稳定失败，最终通过。
- 新增[绑定整数 DML](../tests/dml_integer_prefilter_protocol_e2e_test.py)完整 49 项控制，含重复键、NULL、负数、反向条件、别名、volatile 求值次数、text/binary Bind、声明类型不匹配、大整数和异常列名回退。既有参数化 SELECT 索引 69 项、JOIN 44 项控制也在最终版本重跑通过。

早期候选 H 的协议运行曾有 43 项通过、1 项 `dml_explain_execution_protocol_e2e_test` 在动态 CAST 错误路径超时，随后清理 ROLLBACK 也超时。最终在原生重 I/O 任务结束后，以原始 15 秒 socket 期限串行跑完全部 45 项，包括该完整测试；原失败日志保留，不据此宣称历史 R164 根因已查明。Python 脚本语法检查及不重叠 DELETE 夹具的参数拒绝检查另行通过。

新增 [WAL 检查点保留前缀](../tests/wal_checkpoint_prefix_test.cpp)的 17 项控制，覆盖一条记录跨越一/两个段、检查点前完整记录保留、重开后追加，以及损坏前缀/检查点/后缀拒绝和无锚点截断拒绝；旧 L 在重开断言失败。PITR、PITR 分叉备份归档、恢复完整性、redo、UNLOGGED、回滚与临时文件恢复一并重跑。

压测结束后另用最终 M 读取器重开旧版、候选 L、最终 M 三份数据库的独立副本，按各自成功测量序列重算全部 11 张表的所有单元格，检查序列下一值与临时 DDL 对象清理；原始数据目录文件哈希保持不变。结果见 `restart-verification/results.json`。

这是 168 个指定入口的回归，未运行全仓库全部入口、TLS 或整库 sanitizer，也不自动关闭兼容性总账。

## 失败记录与测量边界

- 首个 TOAST 试跑用了不被当前声明接受的 64 KiB TEXT 值，返回 `22023`。正式夹具改用可支持的 `VARCHAR(12000)` 与 6 KiB 值；未将首次试跑计为通过。
- 初版 ALTER 夹具为 20 行，旧版混合负载下超过性能客户端的 60 秒期限；该记录保留。后续采用独立的 5 行 DDL 夹具，保证双方能完成共同测量窗口。另一次新建数据库的 CPU 采样中，旧版 20 行 ALTER 用时 44.19 秒，产生约 763 MB 的进程 write_bytes 增量；该受插桩数据只用于定位逐行整库保存点，未混入主表倍率。
- 旧版正式 35 类测试在第三轮 CHECKPOINT 返回 `XX000`。原日志没有指出具体失败阶段，不能仅凭后续通过判定该次失败的唯一原因；新增确定性并发测试分别验证后台维护占锁、重叠检查点与真实 fsync 失败。
- 一次候选原生测试将 WAL 故障矩阵截在自定义外层 180 秒监视期限。项目原测试没有该期限；最终对这一完整矩阵给 900 秒外层监视，未删故障项、改 WAL 设置或放宽协议客户端默认 15 秒期限。
- 目录发布测试原夹具将同一 inode 移出再移回，粗粒度时间戳下可能没有文件身份变化。修正为保留旧 inode 时复制产生新文件；原有新内容可见性断言保留。统计/VACUUM 测试曾在最终 DROP 清理处失败，数据断言已通过；补充状态诊断及独立后台占锁控制，不据此倒推每次旧失败的根因。
- CLOG 字节探针初次沿用头部注释的 `pg_xact/0000`，实际实现的段名是 `pg_xact/0`，因读取错误路径而断言失败。修正路径后两版均保留完整字节和重开读取断言。
- 第一版写入优化 L 完成了 315 个性能样本，但补测复用其数据库副本时，旧版读取器报 `cannot open WAL`。从完全未优化的本轮旧版数据也复现同一错误；新增跨段测试在 L 上稳定失败，确认是既有截断/重开缺陷。随后增加恢复起点修复 M，并重新进行功能、性能及真实数据库重开验证。L 的成功计时与重开失败均保留，主表最终仅使用 M 的测量。
- 退出阶段的初次诊断夹具在主线程的 atexit 回调中复用已结束生存期的线程局部引擎上下文而崩溃；该夹具不能证明产品退出缺陷。修正为先建立事务/目录/WAL 所有者，再在退出回调中新建工作线程执行 CHECKPOINT/DROP，保留原断言；有效夹具在 M 上通过。原始失败和修正后的来源分别保留在 `native-shutdown-control/` 与 `native-shutdown-accepted/`。

## 复现与证据

运行基准会新建私有数据库目录，不打开现有用户数据，也不会自动重编译指定二进制。示例命令：

```bash
python3 scripts/benchmark_operations.py \
  --binary /absolute/path/to/dbms_main \
  --output build/operations-benchmark-new-run \
  --rows 1000 --batch 20 --ddl-rows 5 --rounds 3 --samples 3

python3 scripts/benchmark_queries.py \
  --binary /absolute/path/to/dbms_main \
  --output build/query-benchmark-new-run --rows 1000
```

每次选用新的 output 路径。两版正式操作测试使用同一份固定的驱动和协议 helper；根目录脚本另补 DELETE 夹具不重叠的参数检查，并修正文档对校验范围的说明，正式参数不受影响。操作记录含每个样本的延迟、CPU、进程 I/O 计数、结果状态、二进制 SHA-256 和启动参数。`units_per_second` 中批量 DML/COPY/建索引按行计数，其他用例按整次操作计数，不能混用。

本机证据位于 `build/database-all-ops-20261030-kl187edl/`，含起始工作区清单、源文件快照、逐阶段构建清单、原始失败、完整测试日志、固定驱动、CLOG 探针和结果汇总。该目录由 build 忽略规则管理；如需跨机器审阅，请一并保存该目录，不能仅凭文档中的本地路径重建旧版工作区。

测量环境：Linux 6.8.0-136-generic x86_64，Intel Xeon E5-2678 v3 @ 2.50 GHz（2 路、48 逻辑 CPU），共享主机；GCC 13.3.0、`-std=c++17 -O2 -g1 -pthread`，ICU、zlib、TLS stub。两版编译和链接配置相同，未启用 LTO 或 `-march=native`。

| 版本 | 二进制 SHA-256 |
| --- | --- |
| 本轮基线（已含上一轮查询优化） | `fbb066f45cc975bea6df338bfc6623ead0b841b360390465ab6ce4a583efe354` |
| 本轮最终版 | `f47176388afe16f9e346db82e5e89b5af7588232921f63d9cf4a2ac10df4490e` |

起始 HEAD 为 `c365ff7902f57f9c7ee496f62c9056a6c93f9bfd`，当时工作区已有未提交修改；这两个二进制标识不能替换成 HEAD 的净源码。冻结快照与初始文件哈希保留原工作区内容。

共 62 个生产编译单元，按生产清单链接。每阶段对源文件及传递项目头文件逐字节核对后复用相同配置的 donor 对象，变更单元实际重编；最终阶段因 WAL 头文件变更重编 7 个单元并复用 55 个，其他修改单元已在前面阶段重编。最终根目录与冻结快照的 206 个生产文件逐一核对一致，不将增量构建写成全量新编译，也未伪造常规构建缓存签名。

证据相对本轮目录：

- `operations-before-v2/operations.json`：旧版完整原始记录（`complete=false` 及失败原因保留）。
- `operations-after-m/operations.json`：最终版完整 315 样本。
- `operations-{before,after}-common-window.json`：明确标注父记录状态的共同前两轮。
- `operations-comparison.{json,csv}`：35 类 p50/p95、CPU 与写入字节汇总；`clog-write-comparison.json` 为独立字节探针。
- `stage-m-wal-retained-prefix/{build.json,native-final/,protocol-final/}`、`final-provenance.json`：构建依赖、每个测试源文件/日志 SHA-256 与实际退出码。
- `driver/sha256.json`：两侧实际使用的固定驱动和 helper 哈希；其中操作驱动 SHA-256 为 `0ddc77735c997b7b9fe5b4ec5700830e069972922432298d8ff6012d6e6cffd7`。
- `original/`、`metadata.json`、`optimization.patch` 与各候选阶段日志：起始内容、本轮增量和失败追溯。

## PostgreSQL 18.6 同机对照（2026-11-02）

本节是新增的小表探索性对照，与上文 10 月 30 日的本项目版本比较分开解释。
本节保留当时程序及 PG 的配对结果；随后源码优化见下文[有界排序批次](#有界排序-top-n2026-11-02)，未重写这里的历史样本。
新建夹具、数据库文件和后台状态与旧批次不同，不能把本节较低的绝对延迟解释成新增源码优化收益。
两边各 35 类操作、每类 6 个计时样本，共 **420 个计时样本**；另各执行一轮 35 类不计时预热。
最终记录完整结束，既有驱动的结果/协议检查均通过。这里的数值回答指定夹具的客户端往返延迟，不能代表整体数据库性能。

### 环境、方法和限制

- 复用 [benchmark_operations.py](../scripts/benchmark_operations.py) 的 35 类操作与既有 oracle，新增 [benchmark_postgresql.py](../scripts/benchmark_postgresql.py) 负责两个独立的本地进程及配对计时。没有使用或修改已有数据库；临时服务均已正常退出。
- 主表 1,000 行、关联表 100 行，修改批量 20 行、DDL 夹具 5 行；同一主机、同一文件系统、回环 TCP_NODELAY，各复用一个连接，无 TLS。两轮每类各 3 次，固定种子打乱操作顺序；第一轮每项先本项目后 PostgreSQL，第二轮反过来。预热后再测，未清 OS 缓存。
- PostgreSQL 精确核验为 18.6（`server_version_num=180006`），发行二进制来自 `public.ecr.aws/docker/library/postgres:18.6-bookworm`，提取后以本机进程运行；manifest digest 为 `sha256:5c4783a8cc3f0074912164f94dfa1be4e043438f0f707cf968d7af9a0d31a2c8`，所需额外库仅作用于该进程。
- PostgreSQL 的 `fsync`、`synchronous_commit`、`full_page_writes`、`data_checksums` 均为 `on`；`shared_buffers=128MB`、`work_mem=4MB`、`checkpoint_timeout=5min`、`jit=on`。本项目使用默认持久化与后台维护，堆/索引池默认 256/128 帧。**未统一两边的内存额度或调优参数**，不能声称完全相同配置。
- 本项目使用 11 月 1 日 CMake 4.4.4 全新构建的 C++17 / `-O2` / TLS stub 程序，SHA-256 为 `996c0b6ee761116f413f4594e722138e5972c58229f42f48bc98b3cc3f72702f`；生产输入为 `9b71e118`，运行期间未修改生产代码。根目录原程序未替换。
- 计时包括 action、响应及客户端解码；SELECT oracle 在 action 中，修改后的额外读取验证、setup 与 cleanup 不计时。只读事务和序列仍沿用原驱动的有限检查，见本报告开头。COPY TO 也包含流量和客户端处理，不是纯服务器执行耗时。
- 这是共享主机、两轮配对的小样本观察，单项 PostgreSQL 测量时间很短，客户端/调度噪声会影响亚毫秒值；6 个样本的 p95 就是最大值，没有置信区间，不能推导 p99、并发吞吐、冷缓存或大表曲线。CPU/I/O 统计范围在 PG 多进程与本项目多线程间不同，本节不比较这些计数。
- 没有运行标准 pgbench/TPC 或长时间压力测试；[PostgreSQL 官方 pgbench 建议](https://www.postgresql.org/docs/18/pgbench.html)强调足够长的重复测量和规模/维护/客户端瓶颈控制。下面倍率仅是本轮对应操作的延迟比，不是总体“落后倍数”。

### 完整结果

延迟比 = 本项目 p50 / PostgreSQL p50；大于 1 表示本项目在该操作样本中延迟更高。
原始延迟和 p95 均保留在 JSON/CSV；表内保留 p50，避免将六个样本的最大值解读为稳定尾延迟。

| 操作 | 本项目 p50 ms | PostgreSQL 18.6 p50 ms | 延迟比 |
| --- | ---: | ---: | ---: |
| SELECT 1 | 1.780 | 0.246 | 7.2× |
| 普通主键点查 | 15.280 | 0.360 | 42.5× |
| 参数化主键点查 int4 | 16.024 | 0.320 | 50.2× |
| 无索引文本等值 | 57.339 | 0.569 | 100.8× |
| 范围 SUM（100 行） | 118.290 | 0.537 | 220.4× |
| GROUP BY 聚合 | 75.109 | 2.946 | 25.5× |
| 双键排序 LIMIT 20 | 60.028 | 1.417 | 42.4× |
| JOIN 后 COUNT | 279.252 | 0.999 | 279.5× |
| INSERT 单行自动提交 | 20.323 | 0.625 | 32.5× |
| INSERT 多 VALUES（20 行） | 92.355 | 0.925 | 99.9× |
| INSERT 显式事务（20 条） | 133.274 | 4.225 | 31.5× |
| UPDATE 主键单行 | 55.423 | 0.653 | 84.9× |
| UPDATE 单行并维护二级索引 | 63.590 | 0.756 | 84.2× |
| CTE UPDATE 单行 RETURNING | 23.411 | 0.796 | 29.4× |
| UPDATE 范围（20 行） | 130.900 | 0.858 | 152.5× |
| DELETE 主键单行 | 26.934 | 0.573 | 47.0× |
| CTE DELETE 单行 RETURNING | 34.143 | 0.729 | 46.8× |
| DELETE 范围（20 行） | 110.730 | 0.923 | 120.0× |
| 只读事务 BEGIN/SELECT/COMMIT | 12.390 | 0.569 | 21.8× |
| 写事务 INSERT 后 ROLLBACK | 27.660 | 0.658 | 42.0× |
| 事务 UPDATE/保存点/回滚/提交 | 113.760 | 1.538 | 74.0× |
| 序列 nextval | 12.049 | 0.250 | 48.2× |
| COPY FROM（20 行） | 76.849 | 0.813 | 94.5× |
| COPY TO（1,000 行） | 83.468 | 15.481 | 5.4× |
| TOAST INSERT（6 KiB） | 33.381 | 0.774 | 43.1× |
| CREATE TABLE 空表 | 913.311 | 2.027 | 450.5× |
| ALTER ADD COLUMN（5 行） | 920.287 | 0.916 | 1004.3× |
| TRUNCATE（5 行） | 1081.687 | 1.191 | 908.1× |
| DROP TABLE（5 行） | 1027.397 | 1.517 | 677.4× |
| CREATE INDEX B-tree（1,000 行） | 992.384 | 2.293 | 432.9× |
| CREATE INDEX Hash（1,000 行） | 922.623 | 3.216 | 286.9× |
| ANALYZE（1,000 行） | 309.694 | 2.951 | 105.0× |
| VACUUM 删除表 | 2.515 | 0.521 | 4.8× |
| REINDEX（1,000 行） | 235.688 | 3.768 | 62.5× |
| CHECKPOINT | 19.966 | 13.762 | 1.5× |

本轮普通点查约 42.5×、单行 INSERT 约 32.5×、主键 UPDATE 约 84.9×、主键 DELETE 约 47.0×；
5 行表 ALTER ADD COLUMN 约 1,004.3×。CHECKPOINT 约 1.5×，不能据此推导 WAL/恢复能力已等价。
不同操作差别很大，未计算一个综合性能百分比，也没有将这些倍率与上文内部优化倍率相乘。

### 复现与原始证据

需要独立可运行的 PostgreSQL 18.6 安装；驱动自行初始化新 cluster，拒绝把其他版本当参考：

```bash
python3 scripts/benchmark_postgresql.py \
  --binary "$PWD/build/cmake/dbms_main" \
  --pg-bin /path/to/postgresql-18.6/bin \
  --output build/pg-comparison-new-run \
  --rows 1000 --batch 20 --ddl-rows 5 --rounds 2 --samples 3
```

可选 `--pg-library-dir /path/to/private/libs` 只给参考进程设置库查找路径；output 必须是新目录。
本机证据在 `build/pg-gap-assessment-20261102-i3wba7p7/`：`postgres-bundle.json`、`inputs.json`、
`paired-run-v3/comparison.{json,csv}`、`paired-run-v3.log`、双方启动日志和原始 EXPLAIN。
`comparison.json` 包含两个轮次的每个样本、预热记录、PostgreSQL 设置、双方操作计数、二进制/驱动哈希；
`assessment-verification.json` 记录对样本数、oracle、总账/生产输入未变及文档表格的复核。
这是本地 build 证据；跨机器复核需保存该目录和所用 PostgreSQL 安装/镜像，不能只依赖本地路径。

环境准备的失败也保留：原默认 `pgref` 实际为 17.2，未用作 18.6 对照；Docker Hub 首次拉取超时后改用镜像渠道。
前两次私有初始化分别缺 `libpq.so.5` 和配置样本的链接目标，记录在 `paired-run/`、`paired-run-v2/`；
这两次没有计时样本，补齐运行文件后才完成 v3，未将失败尝试写成通过。未重启已有参考服务或用户数据库。

## 有界排序 Top-N（2026-11-02）

### 实现与适用边界

从 `e7dcd673` 开始，对照 PostgreSQL 18 的 [tuplesort](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)
及 [tuple slots](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/execTuples.c)、
[DuckDB TopN](https://duckdb.org/2024/11/14/optimizers)和[外部排序](https://duckdb.org/2025/09/24/sorting-again)、
[SQLite 临时结构](https://www.sqlite.org/tempfiles.html)与[原子提交](https://sqlite.org/atomiccommit.html)，独立实现本项目的有限改动：

- `PreparedSortOp` 对普通正数 LIMIT 使用最大堆，候选上限为溢出安全的 `K=LIMIT+OFFSET`，最终排序后交给原投影/OFFSET/LIMIT 算子。
  必要排序键和排序前输出仍对全部原输入求值；原始序号保持同键输入顺序，周期检查取消。
- 可支持的单表排序 LIMIT 在执行阶段进入已有类型化执行器，修复参数化路径的多键顺序、OID 表达式和 volatile 投影需求。
  Parse/Bind 规划未改，R244 暂缓边界不变。
- 物理行只读取一次 NULL 位图，复用已有属性解码并恢复嵌套绑定，避免同一排序行重复解码；文件身份检查保留。
  已求值的精确内置整数键只转换一次；其他类型保留原比较职责，浮点特殊值改由表达式比较器处理。
- 排序候选约为 O(min(N,K))，比较工作约为 O(N log K + K log K)。这不是整个执行器的 O(K) 内存承诺：
  **底层 TableScan 仍可能物化全部原始行**；无界排序、DISTINCT、WITH TIES、聚合/分组和边界溢出继续原路径，没有完整 work_mem/spill。

### 相邻旧／新／旧延迟

冻结旧程序为此前 CMake 产物 `996c0b6e…`；最终候选 SHA-256 为
`c7f832ddbef2dd1811b87f25bf96474a5fb71b20b564f53b881c391b1df22d15`。
两者同为 GCC 13.3 / C++17 / O2 / ICU / zlib / TLS stub；本批正常增量编译变更单元，未宣称重新 fresh 编译全部 62 单元。
验收后已将同一候选原子安装到根目录 `dbms_main`，原根目录程序备份为证据目录内的 `root-before-dbms_main`；未重启既有数据库服务。

同一份正常关闭的 1,000 行夹具复制为三个独立目录，源文件逐项哈希不变；依次旧、新、旧，每次独立启动，
每类 3 次校验预热后做 3 轮×4 个样本，固定打乱每轮顺序，总计 **144 个计时样本**，全部校验完整结果。
回环 TCP_NODELAY、单连接、热缓存；计时包含响应与结果验证；默认持久化不变、socket 15 秒，不与编译或其他测试重叠。

| 操作 | 旧首轮 p50 ms | 新 p50 ms | 旧末轮 p50 ms | 结论 |
| --- | ---: | ---: | ---: | --- |
| amount DESC,id ASC LIMIT 20 | 56.219 | 41.612 | 58.482 | 延迟约降低 26%–29%，约快 1.35–1.41 倍 |
| 同排序 LIMIT 20 OFFSET 500 | 57.896 | 42.462 | 57.481 | 延迟约降低 26%–27%，约快 1.35–1.36 倍 |
| 主键点查控制 | 9.816 | 10.736 | 9.482 | 未证明提速，候选在此窗口略慢 |
| 同排序无 LIMIT 控制 | 86.066 | 87.517 | 85.673 | 未证明提速 |

这是共享主机上的小样本，不推导 p99、吞吐或大表收益，也不将新版延迟除以前一批独立 PG 样本来制造新的 PG 倍率。

### 内存与正确性

`prepared_topn_memory_test` 使用真正的类型化算子和 12,000 行生成流（每行 2 KiB payload），
在子算子 EOF、输出消费之前用 `mallinfo2` 读取活跃分配增量；不是 RSS、峰值内存或实际 TableScan 的整条查询占用。
同一冻结测试旧版完整 8 项有 3 项内存失败、结果均正确；最终新版 8 项全过。
LIMIT 20 保留量由 72,817,360 字节（约 69.4 MiB）降到 131,952 字节（约 129 KiB）；
OFFSET 13/LIMIT 7 为 124,656 字节，LIMIT 1 为 18,224 字节。
LIMIT 0 原输入读取为 0；超量 LIMIT、无界、DISTINCT/WITH TIES 控制仍保留完整输入，证明观测没有隐藏全量缓冲。

最终 11 个原生入口、11 个协议入口全过；新增完整 48 条在严格 PG 18.6 和新版各全过，旧版 11 条失败。
10 个相邻协议入口涵盖完整 190 条聚合排序、69 条参数索引及取消/上下文等检查；原生含原完整 80 项排序中断检查。
另外重跑既有 35 类操作，一轮 35 个样本和预热的现有结果检查均通过；与部分原生编译重叠，只作正确性 smoke，不用于性能结论。
本批不是全仓库、真实 TLS、并发、崩溃恢复或完整 PG 兼容验收，原始总账保持 22/166/70/15。

### 失败保留与待办

- 早期仅接入类型化有界排序时，LIMIT 20 的 p50 实测回退到 160.678 ms（两侧旧版 59.503/62.479）；
  独立 CPU 采样显示文件身份核验开销占主导，继而修复重复解码/按列位图读取。中间候选为 61.700 ms；
  后续采样确认类型名反复规范化的成本，再加入整数键缓存，才得到上述最终结果。各候选与原始样本均保留。
- 接入新分流曾暴露 2 条 Infinity 比较失败，按浮点比较语义修复后才通过；没有删除断言。
  NULL 位图回归首次编译缺少 DbError 头文件，补齐后完整运行通过。
- 10,000 行夹具在加载阶段触及 15 秒期限，清理等待 30 秒后被 helper 强制终止；未产生完整计时样本，
  没有提高期限、降低持久化或称为大表通过。加载瓶颈仍待调查，不据此重开用户暂缓的退出/CTAS 专项。
- 三方额外探针确认虚拟生成列失败返回 NULL 的既有差异，见[测试报告](test-report.md#2026-11-02-有界排序与类型化执行)；
  流式物理扫描、完整工作内存/spill、修改路径快照开销仍是后续目标。

### 复现与证据

```bash
python3 scripts/benchmark_topn.py \
  --before /path/to/frozen-before-dbms_main \
  --after "$PWD/build/cmake/dbms_main" \
  --output build/topn-new-run --rows 1000 --rounds 3 --samples 4
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/prepared_topn_protocol_e2e_test.py
```

`--fixture` 可指定同一驱动创建并正常关闭的夹具（必须匹配 `--rows`），各次运行只修改其副本。
证据目录 `build/pg-feature-topn-20261102-bc582b_s/`：
`latency-keycache/comparison.json` 为最终 144 样本；`latency-comparison.json` 与 `latency-decode/comparison.json` 为早期候选；
`native-before.log`、`native-keycache.json`、`protocol-keycache.json`、`protocol-{old,reference}-48.log` 保留完整检查；
`operations-smoke/operations.json`、`fixture-10000-failure.json`、`virtual-demand-three-way.json` 记录操作回归及未关闭差异；
`final-provenance.json` 记录生产输入、实际对象、测试与程序哈希。

## 按页物理扫描（2026-11-02）

### 实现与验证

以 `17df7ff3` 的 Top-N 版本为旧版继续优化。参考 PostgreSQL 18 的
[SeqScan](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/nodeSeqscan.c)和
[heap scan](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c)：
保留扫描状态、snapshot 和资源生命周期，由上层按需求取后续行，避免 open 时就读完整张表。
实现为本项目自有游标，没有移植 PostgreSQL 文件格式。

- 普通非分区 `TableScanOp` 打开时保留一个 MVCC 视图和堆页范围，逐页复制可见行及 NULL 位图，
  页锁/pin 在交付前释放，避免持锁调用写函数；新出现的 combo ID 仅补充解码表，不推进该扫描的 command ID 或可见性边界。
- 同一套扫描初始化和 SSI 登记被回调扫描与游标复用；空扫描仍登记 relation SIREAD，读取页内候选时保留原 tuple/page 覆盖。
  游标拥有 allocator 引用，继续使用原有文件身份、缺失文件、tablespace 和格式核验。
- 最多保留一页未展开的行；只对当前交付行展开 TOAST，类型化消费者直接使用捕获的 NULL 位图。
  扫描行缓冲不再随表内行数线性增长；buffer pool、snapshot/SSI 跟踪、上层结果缓存和单个大值不包含在这项界限内。
- 提前停止记录实际交付行数，只有到 EOF 才发布完整 live-row 估计；重复 close 不重复计数，析构、I/O 错误与未打开子算子均核验资源/NULL 绑定。

最终 **23 个原生入口、17 个真实协议入口** 全过，另 35 类操作预热及一轮既有结果检查全过。
新增物理扫描完整 6 项旧版 1 个内存失败、新版全过；跨后端修改和 fresh scan 共 8 项覆盖自提交、RC、RR、SERIALIZABLE，均保留正确 OLD/NEW 与 TOAST 值。
新增写函数排序协议完整 13 条在冻结旧版、PG 18.6 和新版均全过；相邻完整 48/80/190/69 及其他范围见[测试报告](test-report.md#2026-11-02-按页物理扫描与固定快照)。

### 内存与延迟

同一原生夹具含 192 行，普通非 NULL payload 为 16 KiB，并含 NULL、空串和字面文本 NULL；
先完整读过 heap/TOAST 而不保留行，再以 `mallinfo2` 测扫描 open 前后的活跃分配增量。
冻结旧版为 **2,820,192 字节（约 2.69 MiB）**，新版为 **17,088 字节（约 16.7 KiB）**，约降低 99.4%。
这不是 RSS、峰值或整条查询内存；最终仍检查完整 192 行、TOAST 值、NULL、提前关闭及重开。

延迟使用与前批相同的闭合 1,000 行夹具，三个独立副本按旧/新/旧启动；源文件逐项哈希不变。
每类预热 3 次，3 轮×4 个已完整校验样本，六类共 **216 个计时样本**，不与构建或其他测试重叠。
默认耐久性、15 秒 socket、单连接、回环 TCP_NODELAY、热缓存及计时内结果验证均保持不变。

| 操作 | 旧首轮 p50 ms | 新 p50 ms | 旧末轮 p50 ms | 结论 |
| --- | ---: | ---: | ---: | --- |
| amount DESC,id ASC LIMIT 20 | 53.883 | 50.111 | 55.118 | 约快 1.08–1.10 倍 |
| 同排序 LIMIT 20 OFFSET 500 | 56.884 | 57.428 | 56.940 | 未证明提速 |
| 已有类型化 ARRAY[id] LIMIT 1 | 31.893 | 23.128 | 33.955 | 约快 1.38–1.47 倍 |
| 普通列投影 id LIMIT 1 | 43.985 | 43.106 | 48.122 | 仍走旧分流，未证明稳定提速 |
| 主键点查控制 | 22.702 | 24.593 | 24.419 | 未证明提速，此窗口略慢 |
| 无 LIMIT 排序控制 | 107.620 | 108.408 | 107.282 | 未证明提速 |

最终统计锁发布修正前的六类 **216 样本**也保留：按上表顺序，旧/新/旧分别为
37.827/32.737/37.298、38.450/33.965/38.552、17.274/9.739/16.965、
22.969/22.755/23.789、9.947/9.387/10.209、87.579/90.310/89.795 ms。
该窗口类型化 LIMIT 的改善约 1.75 倍，最终窗口约 1.4 倍；包括未改旧程序在内的绝对延迟明显波动，不能把前阶段较好的倍率当作最终稳定收益。

此前还完成四类旧/新/旧 **144 样本**：排序 LIMIT 20 为 39.574/34.960/37.829 ms，
OFFSET 500 为 40.559/39.668/39.993 ms，点查为 12.067/11.577/9.656 ms，无界排序为 89.799/92.357/88.234 ms。
三次观察不合并择优；OFFSET 等收益会受窗口波动影响。此处不推导大表、并发、p99、完整业务负载或新的 PG 性能比。

### 构建、失败与未完成范围

最终程序 SHA-256 为 `531a7ef325c604ec23a57a707d38e3b45607c535e84c50953a6506c56076d02c`，
GCC 13.3 / C++17 / O2 / ICU / zlib / TLS stub 未变。公共头变化触发 13 个单元重编；
另明确重编两次 ExecutionPlan，保证资源释放及统计锁边界修改已进入对象；共链接当前 62 个生产对象，复建不再编译/链接。
统计在释放关系锁前发布，避免 DROP/重建重置统计后又被旧扫描写回；最后修正后完整重跑上述 23/17 入口及 35 类操作。
验收后的同一候选原子替换根目录程序，旧程序保留备份，既有服务未重启。

PG 参考首次监听后尚未就绪，返回 57P03，专项 SQL 尚未运行；原日志保留。
辅助驱动补 `pg_isready` 探针，在原 15 秒启动期限内确认就绪后才认证、核验 18.6 并完成 13 条检查，没有放宽协议断言或期限。
新增扫描测量前试选的 `~id` 投影在冻结旧版报 42703，保留探针记录；最终采用已支持的数组投影，未据此认定整数位非功能已完成。

分区/RLS 保留原路径，ParallelTableScan 和其他 legacy 消费者仍可能全量缓冲；普通 `SELECT id ... LIMIT 1` 尚未普遍接入按需执行，
是下一批明确的执行分流缺口。无界排序/work_mem/spill、生成列表达式错误传播、修改快照成本及大表加载失败仍开放。
本批不修改 Bind 规划、用户暂缓项或原始 273 项状态，也不是全仓库/真实 TLS/崩溃恢复验收。

### 复现与证据

```bash
python3 scripts/benchmark_topn.py \
  --before /path/to/17df7ff3-dbms_main --after "$PWD/build/cmake/dbms_main" \
  --output build/scan-new-run --rows 1000 --rounds 3 --samples 4 --include-scan-limit
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/table_scan_effect_snapshot_protocol_e2e_test.py
```

本机证据 `build/pg-feature-scan-20261102-i7t0b3bo/`：`latency-release/comparison.json` 为最终 216 样本，
`latency-with-scan/comparison.json` 与 `latency/comparison.json` 为前阶段 216/144 样本；`native-before.log`、`native-release.json`、`protocol-release.json`、
`statistics-final.json`、`table_scan_snapshot_test-release.log`、`protocol-reference-v2.log` 保留实际退出码和完整控制。
`operations-release/operations.json`、`limit-probe-v2.json`、首个 PG 启动失败日志、CMake 编译/复建日志及 `final-provenance.json` 保留来源与边界。

## 普通 LIMIT 按需执行与错误前缀（2026-11-02）

### 实现与适用范围

以 `bcde5b3e` 为旧版，参考 PostgreSQL 的 [LIMIT/OFFSET 语义](https://www.postgresql.org/docs/18/queries-limit.html)、
[Limit 算子](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/nodeLimit.c)及
[SELECT 求值说明](https://www.postgresql.org/docs/18/sql-select.html)，把普通、可支持的单表 LIMIT 接入已有类型化算子链。
之前只有数组、特定标量角色或有序 LIMIT 等会接入，普通列投影仍先做全表工作。

- 执行阶段接入普通有限 LIMIT；LIMIT 0 不打开输入，OFFSET 行仍按原投影需求求值，未请求的后续行不额外执行 WHERE/投影。
  DISTINCT/聚合仍完成其所需输入，不直接截断原输入。没有 ORDER BY 时不承诺固定子集，本轮验证使用固定无索引堆夹具。
- 共享计数辅助函数对可识别的负数 LIMIT/OFFSET 返回 2201W/2201X；保留负零、NULL/ALL 等原有解析边界。
  单列星号保留已绑定列类型，避免扩大接入后把星号推断为 unknown/text。
- 新增显式 `executePlanWithPrefix` 接收契约：失败仍为失败，保留原 SQL 异常及恰好一次清理，但允许顶层 SQL 接收器保留已经产生的类型化行/NULL 位图。
  默认 `executePlanChecked` 和嵌套调用继续失败即清空。Simple Query 和不限 portal 行数的 Extended Execute 在正常格式下先发送该前缀再报错，不发送成功标签。
- 这仍是结果缓冲接收器，不是网络端完全流式。有限行数 portal 的错误/悬停需求没有完成；未修改 Bind/Parse 规划、参数帧、custom/generic 或格式 2 策略。
  R303/R304/R305 继续按[已有用户暂缓](deferred-scope.md)处理，不能据普通 SQL LIMIT 修复恢复其共享生命周期调查。

### 正确性、未通过项与构建

**24 个原生入口通过**，其中新增前缀接收完整 16 项：默认清空与显式保留、SQL/非 SQL 异常身份、open/close/hasError、主错误不被清理覆盖、清理一次、NULL/空串/文本 NULL/UTF-8 和正常 EOF/限额。
完整所选 **21 个协议入口为 20 通过、1 失败**；新增完整 **62 条**在 PostgreSQL 18.6/候选各全过，冻结旧版有 9 条失败。
覆盖 Simple、text/binary Bind 及结果、计数与求值需求、序列/写函数次数、阻塞 DISTINCT/聚合、OID/typmod/属性来源及错误前原始帧。

首份 54 条夹具误用“错误前无行”的通用检查，PG 在第 6 行除零前已经发出 5 行；原失败保留，随后改成精确检查 5 行及 T/D/E/Z 顺序，再扩展为 62 条。
仅修改分流的中间候选仍有错误前缀和负数 LIMIT 2 条失败，补生产实现后才全过。
未通过入口为既有 R304/R305 完整 463：旧/新均 40 处差异，463 条观察逐字节一致；不是 40 个独立 bug，也没有排除后称全绿。
额外有限行数 portal 共 6 条观察，PG 全过、旧/新各相同 2 条差异，保留而不接受为修复；相关共享模块继续暂缓。
中央索引此前遗漏 2026-10-27 的专属暂缓记录，本轮补入原引用，不创建新暂缓项或改变原始 273 项状态。

35 类操作完成预热及一轮现有 oracle 检查；与相邻编译/回归重叠，只作有限正确性 smoke。
生产程序 SHA-256 为 `8a428080cc42e1809cf41ef8f3d09f5658d6d42f095623326d0d18b5ae00ebf6`，仍为 GCC 13.3 / C++17 / O2 / ICU / zlib / TLS stub。
先重编 1 个 Main，再因接收器 API/头变化重编 6 个单元，共 7 次编译、6 个不同单元；链接当前 62 个生产对象，不冒称 fresh 62。
原生前缀测试先链接已完成的 61 个 core 对象，再逐项哈希确认与最终对象相同；其余原生重新编译/链接，未以 stub 代替真实协议 Main。
验收后同一程序原子安装到根目录，原版保留备份，既有服务未重启；不是全仓库、真实 TLS、恢复或完整 PG 行为验收。

### 六类相邻旧／新／旧延迟

与前批相同的闭合 1,000 行夹具复制为三个独立目录，源文件哈希不变；每类预热 3 次、3 轮×4 个计时样本，六类共 **216 个样本**。
不与构建或其他测试重叠；默认耐久性、15 秒 socket、单连接、热缓存、回环 TCP_NODELAY 和完整结果校验均未变。

| 操作 | 旧首轮 p50 ms | 新 p50 ms | 旧末轮 p50 ms | 结论 |
| --- | ---: | ---: | ---: | --- |
| 普通列投影 id LIMIT 1 | 22.893 | 9.791 | 22.670 | 约快 2.32–2.34 倍，延迟约降低 57% |
| 已有类型化 ARRAY[id] LIMIT 1 | 10.166 | 10.136 | 11.438 | 作为原路径控制，不认定稳定提速 |
| amount DESC,id ASC LIMIT 20 | 33.646 | 33.696 | 34.348 | 未证明稳定提速 |
| 同排序 LIMIT 20 OFFSET 500 | 36.969 | 37.363 | 36.131 | 未证明提速 |
| 主键点查 | 9.453 | 10.118 | 11.224 | 窗口波动，不认定提速 |
| 无 LIMIT 排序 | 94.457 | 84.531 | 85.651 | 旧两侧漂移，不认定稳定提速 |

只将普通列 LIMIT 1 的改善归属于本批接入，不用其他窗口倍率相乘，也不推导大表、并发、p99、全业务或新的 PG 性能比。
主结果仍在主机共享负载下测量，不能把绝对延迟当作固定保证。

### 复现与原始证据

```bash
python3 scripts/benchmark_topn.py \
  --before /path/to/bcde5b3e-dbms_main --after "$PWD/build/cmake/dbms_main" \
  --output build/ordinary-limit-new-run --rows 1000 --rounds 3 --samples 4 --include-scan-limit
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/physical_limit_demand_protocol_e2e_test.py
```

证据目录 `build/pg-feature-limit-20261102-tafn38be/`：`latency/comparison.json`、`native-final.json`、`protocol-final.json`、
`protocol-{before,reference,after}-62.log`、`protocol-before-reference-62.json`、`executor_result_prefix_test.log`、
`portal-existing-failure-comparison.json`、两侧完整生命周期日志及 `portal-quota-{before,reference,after}.log` 保留全部观察。
`operations-smoke/operations.json`、各次 CMake 编译/复建日志与 `final-provenance.json` 记录结果、来源和实际构建范围。

## 普通物理排序的内存预算与外部归并（2026-11-02）

以下是 `d09cb6c1`→`471dad3f` 批次的历史记录；其中最终按需归并的当时缺口已在[后续批次](#最终归并按需输出与排序绑定恢复2026-11-02)补充，数字和未通过项保持原口径。

### 研究、实现与实际 SQL 接入

基于 `d09cb6c1`，参考 PostgreSQL 的[每操作工作内存](https://www.postgresql.org/docs/18/runtime-config-resource.html)和
[tuplesort 有序段/归并](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)，
结合 [DuckDB 减少排序搬运和归并 I/O 的设计](https://duckdb.org/2025/09/24/sorting-again)及
[SQLite 对排序临时数据与事务日志的区分](https://www.sqlite.org/tempfiles.html)，独立实现普通物理 Sort 的工作预算与外部归并。

- 原先全量行、多个辅助向量及排序后副本改成一个行载体，保留原始字节、已求值键、完整物理 NULL 位图和实际行来源。
  按 `work_mem_kb` 估算批次的字符串及向量容量；预算内直接排序，超预算生成有序段，按行宽/预算作 2–16 路归并。
  段头在文件中串联，不保留随输入增长的段目录，不为每段打开描述符；每个 Sort 最多两个文件描述符。
- 临时文件创建后立即 unlink，设置 CLOEXEC；正常结束、提前关闭、异常和进程退出均释放。短读/短写与读写 EINTR 保留完整字节，
  ENOSPC/描述符耗尽/I/O 错误分别传递 53100/53000/58030；输入错误保留原异常，子节点清理一次。
  没有修改 WAL、降低 fsync/耐久性或放宽 socket 期限。
- **首个仅改算子的候选并未覆盖真实 SQL**：原前端为捕获结构化结果而主动绕过 Volcano；实际 SQL 文件探针失败，原日志保留。
  随后接入能够保留结构化单元格的普通单表、物理单键排序，保留列顺序/别名、NULL/空串/文本 NULL 和 OID；
  将可解析的 ORDER BY 别名/序号降低到真实列，同时修复 `ORDER BY 1` 把整数当文本排序的既有差异。
- 接入只用于正常 Simple 或不限 portal 行数的 Execute、受支持内置键及普通 AND 条件。
  多键、NUMERIC、数组/对象身份、显式排序 COLLATE、DISTINCT/WITH TIES、OR 分流、虚拟生成列及有限行数 portal 等保持各自既有消费者。
  `SET work_mem` 会话语义、类型化排序/Top-N 的工作预算、最终归并按需输出及 hash/aggregate/window/join 仍未完成。
  单个超大行、有限读头及元数据可超出预算；结果接收器仍可能保留全部输出，**不是整条查询/RSS 的硬上限**。

### 内存、正确性和未通过项

最终 **29 个原生入口通过**：前批 24、三个新排序入口及两个恢复入口。新入口共 39 控制：内存 17、I/O/清理 16、真实堆表 6；
冻结旧版分别 13/15/4 不符，新版全部通过。这些不符不是 32 个独立 bug。
真实堆表核对 8 KiB TOAST、NULL/空串/文本 NULL、>2^53 BIGINT、行来源、全部值、提前关闭和同一图重开；
I/O 控制含真实 syscall 的短传输/EINTR、ENOSPC、EMFILE、读取 EOF/EIO、截断失败、取消、原始主异常及强杀时匿名文件释放。

| 内存观察，8,192 行、每行额外 2,048 字节 | 旧版，字节 | 新版，字节 |
| --- | ---: | ---: |
| 各输入调用边界的最大活跃分配增量 | 18,219,056–18,231,760 | 50,816–55,376 |
| 子输入 EOF 时保留的活跃分配增量 | 18,219,056–18,231,760 | 10,352–14,912 |

配置为 64 KiB；四个方向/NULL 顺序控制均完整消费全部输出。另有 1 KiB 预算、32×256 KiB 超大记录控制。
这些是该原生进程在指定输入边界的分配观察，**不含所有归并瞬时峰值，也不是 SQL 全进程峰值/RSS**。

新增完整 **47 条协议**在严格 PG 18.6/最终程序全部通过，冻结旧版有 1 条整数序号排序失败。
完整所选 **22 个协议入口为 21 通过、1 失败**：既有暂缓的 Bind/portal 完整 463 中有 40 处不符，旧/新全部观察逐字节相同。
前一 `1865fddb` 候选的同组 22 为 20 通过/2 失败，另一个是 statement_timeout 的部分 COPY 帧关闭检查；
隔离旧/新/旧三次也均未通过，但失败位置不同，未定位共同原因。最终该完整入口通过，**不改写原失败，也不声称 R197 已修复**。
专属 R197 的既有用户暂缓补入中央索引，未恢复 COPY 响应/中断所有权调查；R303/R304/R305、共享 CPU 排序中断 helper 和 NetworkServer 源码均未修改。

首份 37 条配置检查使用 `pg_settings.setting::integer`，PG 全过而本项目返回 0A000；保留原失败，改用原生 text 列严格核验值 64。
这个独立目录表达式缺口没有修复，不能因排序专项通过将其关闭。随后扩展到 47，原 36 个排序/协议控制均保留。
原生内存夹具另增加逐输入边界分配观察；EOF-only 和首版输出消费日志也保留，没有减少最终控制或放宽断言。

实际 O2 Main 在观察到匿名排序文件后被强杀；进程退出后无命名残留，重开逐项核对 **1,000 行/4,000 单元格**，再次排序全值一致。
原生 redo_crash_recovery/recovery_integrity 也完整通过；这不是全部故障、并发、OOM、TLS 或整库恢复矩阵的验收。
同一最终程序的 35 类操作完成预热及一轮现有结果/修改检查；与相邻回归重叠，仅作有限正确性 smoke。

### 性能：最终程序的两个独立窗口

沿用闭合 1,000 行夹具，每个窗口分别复制为旧/新/旧三个目录；每类预热 3 次、3 轮×4 个样本，七类每窗口 **252 样本**。
默认实例预算 4 MiB；另一窗口在独立源副本中配置 64 KiB，旧代码没有消费该预算。各窗口源文件哈希均不变，
默认耐久性、15 秒 socket、完整结果校验、回环 TCP_NODELAY 不变；没有与本任务编译/测试并行计时。

默认 4 MiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 106.927 | 76.283 | 96.752 |
| 类型化双键 LIMIT 20 | 31.798 | 34.313 | 33.428 |
| 同排序 OFFSET 500 | 33.701 | 37.370 | 35.028 |
| 其他分流的双键无界排序 | 86.993 | 88.447 | 81.982 |
| 主键点查 | 8.994 | 10.822 | 10.057 |
| ARRAY[id] LIMIT 1 | 9.459 | 11.065 | 10.345 |
| 普通 id LIMIT 1 | 8.689 | 11.153 | 9.929 |

64 KiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 108.897 | 78.528 | 101.329 |
| 类型化双键 LIMIT 20 | 38.296 | 38.987 | 35.513 |
| 同排序 OFFSET 500 | 35.955 | 40.023 | 35.623 |
| 其他分流的双键无界排序 | 94.835 | 89.597 | 86.612 |
| 主键点查 | 11.344 | 13.258 | 11.729 |
| ARRAY[id] LIMIT 1 | 11.585 | 14.188 | 11.205 |
| 普通 id LIMIT 1 | 10.438 | 12.046 | 10.504 |

普通物理单键无界排序在默认窗口约快 **1.27–1.40 倍**，64 KiB 窗口约快 **1.29–1.39 倍**。
最终两个窗口中的多项控制也有慢值：例如默认普通 LIMIT 为 8.689/11.153/9.929 ms，64 KiB 数组 LIMIT 为 11.585/14.188/11.205 ms。
不能认定这些差异全部由环境造成；本批**未证明所有查询不回退**，后续仍需对混合工作负载及其余控制继续定位。
其他控制按表保留，不概括为全查询提速；这是共享主机的小表热缓存、单连接窗口，不推导大表、并发或 p99。
没有重测 PG 延迟，也没有将不同批次的改善倍率相乘或拿新值除以历史 PG 样本。

### 前一候选的回退、诊断与构建来源

`1865fddb` 的三个窗口各 252、合计 756 个样本原样保留，不与最终结果合并。
单键无界排序三组旧/新/旧 p50 分别为 104.707/80.001/107.823、109.615/77.708/109.362、101.986/77.385/101.631 ms。
同时发现不利控制：默认 Top-N 为 33.726/38.174/36.141，重复默认组为 32.630/36.715/32.302 ms；
重复组双键无界排序 84.516/98.234/85.703 ms。没有删除慢值或据目标项的改善宣称全面加速。

单独控制诊断 400 个完整校验查询，把额外开销缩小到 Main 接入构建；将判定移出大型 executeInternal，保持 AST 临时对象在独立函数。
后续五版本诊断 500 查询中，Top-N 服务进程 CPU 为旧首 29.4、仅算子 28.4、内联接入 31.0、独立判定 29.2、旧末 28.2 ms/次。
点查仍有主机窗口漂移，诊断按每个 50 查询批次的进程计数取值，不能当作逐请求 CPU p50 或与正式延迟混合。

最终生产程序 SHA-256 为 `6ee04376b8276c36de9ea3726582d50125adeb8a6bca15d9a57f4cd7e59dd94f`，仍为 GCC 13.3 / C++17 / O2 / ICU / zlib / TLS stub。
本批初次重编 5 个生产 CPP，再先后完成两次 Main O2 构建，共 7 次成功编译、5 个不同 CPP；另两次 Main 编译因补范围护栏/序号修复而停止，不算通过。
一次实际 Main O0+61 O2 core 的快速功能预验另存，没有将它用于最终性能。最终链接 62 个当前生产对象，未冒称 fresh 62。
29 个原生只链接其实际使用的 61 个 core；后续只改 Main，逐对象哈希核对一致，再在最终实际 Main 重跑协议/操作/崩溃探针。
验收后同一程序原子安装到根目录，原版备份保留，既有服务未重启。
原始 273 项、38 条原子要求及专属暂缓记录保持；本批只新增局部证据，完整 work_mem/资源治理及全 PG 行为仍未完成。

### 复现与证据

```bash
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/sort_spill_protocol_e2e_test.py
python3 scripts/benchmark_topn.py \
  --before /path/to/d09cb6c1-dbms_main --after "$PWD/build/cmake/dbms_main" \
  --fixture /path/to/closed-1000-row-fixture --rows 1000 --rounds 3 --samples 4 \
  --include-scan-limit --include-full-sort --output build/sort-new-run
```

证据目录 `build/pg-feature-sort-1be1avdu/`：最终两组 `latency-final-{default,low64}/comparison.json`，
三个前期窗口 `latency-{default,low64,default-repeat}/comparison.json`、两份 `control-cpu*.json`、
`native-final.json`、`protocol-final.json`、新原生旧/新完整日志、`protocol-{before,reference}-47.log` 及最终排序协议日志均保留。
`protocol-final-first/`、`timeout-isolated.json`、对应三份失败日志和 `portal-existing-failure-comparison.json` 记录未通过范围。
`main-sort-crash-outlined.json`、`operations-outlined/operations.json`、各构建/预验日志与 `final-provenance.json` 记录实际程序和构建来源。


## 最终归并按需输出与排序绑定恢复（2026-11-02）

以下是 `471dad3f`→`049c0bce` 批次的历史记录；会话 SET 的当时缺口已在[后续批次](#会话工作内存与实际排序预算2026-11-02)补充，其他原范围和数据保持。

基于 `471dad3f`，进一步参考 [PostgreSQL 18 tuplesort 的最终归并](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)：
顺序访问的消费者不必把最后一个完整归并结果重新写入临时文件。普通物理 Sort 现在只完成必要中间轮次，剩余至多 2–16 个段时保留有限读头和堆，按 `next()` 合并输出。
输入及必要排序键仍在输出前全部消费/求值；不把这个改动描述为跳过输入或扩展所有排序分流。

获胜行的后继记录延至下一次需求才读取，提前关闭不读后继。这个精确 I/O 需求策略由本项目自行定义；
PG 的 TSS_FINALMERGE 在返回当前行前补读，**没有据此声称注入文件故障时的行前缀与 PG 完全相同**。
读错、分配异常等使本 Sort 立即关闭并保留主异常；再次读取不会从半条记录重试。子节点返回失败也立即清理，保留原诊断。
Sort 同时捕获调用方 NULL 行绑定，在子节点清理后恢复，覆盖正常、提前关闭、失败、析构和嵌套；
未打开/已关闭对象再次关闭不影响后来设置的绑定。没有改公共头、Main 分流、网络、持久化或共享 CPU 排序中断 helper。

### 确定的 I/O 收益与正确性

新增两个原生入口完整 **40 项**：最终归并 8、绑定生命周期 32；冻结旧版分别 6/26 不符，新版全部通过，不能合计为 32 个独立缺陷。
归并夹具为 1 KiB 预算、8 条 >4 KiB 记录，强制 8→4→2 段；实际 syscall 字节计数证明少一次完整输出写入和再读取，
同时完整检查 >2^53 BIGINT、全部原始字节、三列 NULL 位图、ASC/DESC/NULL 顺序和真实空来源。
真实堆表生命周期入口覆盖实际 RID/NULL/空串/文本 NULL、内存/落盘、嵌套与重开、六种子节点失败、异常身份及锁/文件释放。

现有 I/O 的输出阶段 EIO 检查从“0 行后报错”改为严格“1 行后报错”，不是允许零或任意前缀；
新增检查还要求错误后立即无描述符，连续 `next()` 为 EOF 且无额外 I/O。保留原版断言在新程序上的 1 处失败及首次旧版夹具未捕获预期 EIO 而中止的日志，
随后只补测试异常捕获，让旧版完整跑完 8 项，并保留其 6 处不符。

本批 **31 个原生入口通过**，所选完整 **22 个协议入口为 21 通过/1 失败**；同一排序完整 47 条在严格 PG 18.6 和最终程序全过。
未通过项仍为已有暂缓的 Bind/portal 完整 463 中 40 处差异，本批与前版全部观察逐字节相同；未删除失败或恢复 R197/R303/R304/R305 调查。
实际 Main 在匿名排序文件存活时 SIGKILL，重开核对 1,000 行/4,000 单元格和再次排序通过；35 类操作完成现有检查，
该 smoke 与回归重叠，只用于有限正确性观察。没有冒称全仓库、全故障、真实 TLS 或完整 PG 等价。

### 独立延迟与既有慢值诊断

沿用闭合 1,000 行夹具，各窗口独立旧/新/旧副本，七类、各预热 3 次、3 轮×4 样本，每窗口 252、共 **504 个完整校验样本**。
默认耐久性/15 秒 socket/TCP_NODELAY不变；计时阶段没有本任务构建、测试或其他基准并行。没有重测 PG 延迟。

默认 4 MiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 80.034 | 80.373 | 88.792 |
| 类型化双键 LIMIT 20 | 35.310 | 35.762 | 40.847 |
| 同排序 OFFSET 500 | 41.264 | 36.854 | 43.779 |
| 其他分流的双键无界排序 | 101.066 | 91.330 | 101.203 |
| 主键点查 | 12.155 | 11.432 | 13.652 |
| ARRAY[id] LIMIT 1 | 11.449 | 11.673 | 13.307 |
| 普通 id LIMIT 1 | 10.739 | 12.721 | 12.893 |

64 KiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 417.722 | 87.753 | 72.164 |
| 类型化双键 LIMIT 20 | 295.640 | 38.125 | 36.440 |
| 同排序 OFFSET 500 | 408.172 | 42.659 | 35.535 |
| 其他分流的双键无界排序 | 409.825 | 99.639 | 83.271 |
| 主键点查 | 313.746 | 12.295 | 12.019 |
| ARRAY[id] LIMIT 1 | 394.422 | 12.096 | 12.730 |
| 普通 id LIMIT 1 | 311.290 | 11.349 | 11.310 |

默认预算的普通排序在内存内，80.034/80.373/88.792 ms；低预算窗口旧首整体达到 295–418 ms，之后显著回落，
普通单键为 417.722/87.753/72.164 ms，新值仍比旧末慢约 21.6%。控制值亦有慢值；
**本批未证明 SQL 延迟稳定改善或全部查询不回退**，不能把旧首的慢值换算为数倍收益，也不能确定所有差异由主机造成。

宽行原生对照另用 8,192 行、每行额外 2,048 字节、64 KiB 预算；同一源代码链接冻结旧/当前 61 core，
旧/新/旧各预热 1 次、计时 5 次，完整逐字节及 NULL 校验，包含源生成、排序、输出验证和关闭。
三个 p50 为 **811.820/456.265/847.333 ms，约快 1.78–1.86 倍**；共 15 个样本，不能当作 SQL/网络/PG 延迟或大规模并发结果。
在 8 条宽行的 syscall 控制中，输入 EOF 后写入 100,952→67,344 字节，总读取 134,544→100,952 字节；
它们直接证明少一轮数据写入/读取，包含少量私有段头开销，不等同整库 I/O。


本轮另保留 960 次混合诊断：旧/当前/旧各在普通完整排序前后测四类 LIMIT/点查，每类 40 次、预热 3 次、默认耐久性和 15 秒 socket。
全部结果校验通过，但旧末、启动段和各控制明显漂移，未建立“执行一次完整排序后持续变慢”的一致因果关系；这些数据不与独立窗口合并，也不将旧慢值归咎于环境。
反汇编还显示先前四个 executeInternal 版本的栈序言相同（0xd000 探测加 0xf78），没有支持“新增大栈帧”假设；代码布局或其他原因仍未证实。
绑定生命周期缺陷已独立复现和修复，尚无证据证明它就是之前多毫秒慢值的原因。

### 构建、范围与证据

最终程序 SHA-256：`dd5e21a765f87e2a5a8fe432e3443444293d9f86a777d758858e98e78a3fda6e`。
CMake 两次成功重编同一个 ExecutionPlan.cpp 并链接 62 个当前对象；第二次补错误终止清理，第一候选完整保留，未用于最终声明。
只有一个生产 CPP 变化，61 个 core 原生测试和真实 Main 分别验证；没有声称 fresh 编译全部 62。
C++17/O2、ICU/zlib/TLS stub、耐久性、数据格式和 socket 期限保持原配置。
最终按需归并仅用于普通物理 Sort，类型化/多键其他排序、会话 work_mem、其他算子和结果缓冲的资源预算仍开放。
本批不关闭原始 273 项或其原子条款，Goal 保持 active，直接在 master 提交，未推送。

原始证据在 `build/pg-feature-query-oxy0qhou/`：`native-final.json`、`protocol-final.json`、两组新测试旧/新日志、
`protocol-reference.json`、`existing-deferred-comparison.json`、`main-sort-crash-final.json`、`operations/operations.json`、
`mixed-controls.json`、`merge-benchmark.json`、`latency-{default,low64}/comparison.json`、构建日志及 `final-provenance.json`。
复现沿用上一节脚本，将 `--before` 改为本批冻结的 `471dad3f` 程序；新原生源码位于 tests，包装 syscall 的链接注解由现有构建入口读取。

## 会话工作内存与实际排序预算（2026-11-02）

基于 `049c0bce`，按 [PostgreSQL SET](https://www.postgresql.org/docs/18/sql-set.html)、
[RESET](https://www.postgresql.org/docs/18/sql-reset.html)及
[work_mem](https://www.postgresql.org/docs/18/runtime-config-resource.html)的会话/事务语义，补齐原来仅有实例配置的运行接线。
普通物理排序的算法与前一批保持同一职责；这次让每个连接选择自己的预算，不将设置成功等同所有算子均受控。

- 支持 `SET [SESSION|LOCAL] work_mem`、DEFAULT、FROM CURRENT、SHOW、RESET/RESET ALL/DISCARD ALL、启动参数/options 与现有 `pg_settings(setting,unit)` 子集。
  数值按 PG 18.6 的 64..INT_MAX kB、大小写敏感单位和舍入处理；SQL 数值常量与引号内 GUC 文本分别保留各自解释。
  `work_mem` 配置别名使用同一解析器，既有项目 `work_mem_kb` 仍可设置原生微型预算，不把它当 PG SQL 会话别名。
- 状态属于 Session，区分继承文件默认、启动覆盖、显式 SET、事务原值/持久值和本地值；保存点支持重复名称、释放和回滚。
  RESET 继续继承最新重载文件默认，启动 options 的默认优先级保留，FROM CURRENT 则成为显式覆盖。
  顶层隐式语句的失败与显式事务/保存点恢复分别收束；没有把错误清理延至下一次无关查询。
- 普通物理 Sort 在 open 时截取有效预算；输入期间修改设置不影响已打开的排序，重开使用新值。
  SHOW、pg_settings 和现有 EXPLAIN SETTINGS 读取同一有效值，普通 EXPLAIN 文本缓存也区分会话值，避免陈旧或跨连接设置观察。
- 工作内存不是 GUC_REPORT 参数，不新增 ParameterStatus；SHOW 的固定 text 描述无需执行查询读取值。
  新的执行期作用域只区分顶层 SET LOCAL 的显式用户块与协议内部隐式块，块外返回 WARNING/25P01 且不修改设置。
  没有修改何时创建隐式事务、保留计划、Bind/portal/输出格式或 COPY 中断所有权策略。

### 正确性、失败候选与实际资源观察

最终 **36 个原生入口全部通过**：前批 31、两个新入口及 Config/搜索路径/时区语法入口。
新入口完整 **163 个控制**：解析/会话状态 136、实际 Sort/Config 27；其单位/舍入的 82 个字符串输入有独立 PG 18.6 实测，嵌入 NUL 为原生边界检查。
最终真实 Main 的所选完整 **25 个协议入口为 24 通过/1 失败**，新完整 **470 条**在严格 PG 18.6/最终程序全过。
唯一最终失败是已有暂缓 Bind/portal 的 463/40；本批与前版全部 463 条观察逐字节相同。
`b190` 的完整同组 25 曾为 23/2，另一个是部分 COPY 帧超时关闭检查；最终完整入口通过不改写原失败，
没有恢复 R197 的专属调查或宣称它已修复。R303/R304/R305、原子条款及专属暂缓记录保持。

同一最终 Main 的 35 类操作完成现有 oracle 检查，仅作有限 smoke；实际匿名排序文件存活时强杀，
重开完整核对 1,000 行/4,000 单元格并再次排序通过，不概括为全部修改故障/恢复、真实 TLS 或整库兼容性验收。

首候选 `11486506` 的完整 448 条有 **8 处不符**：2 个块外 SET LOCAL 警告、3 个缓存 EXPLAIN 值、2 个 Simple RESET 语法错误码、1 个 Extended SHOW 语法错误阶段。
前两处警告分别是 01000 和未发送警告；其值/行/Ready 检查仍正确。旧/PG/首候选同源完整结果和实际首候选程序均保留。
随后补普通缓存键、Simple 的原语法校验、SHOW 42601 及独立执行期显式块上下文，再完整重跑；没有将断言改成多个可接受值/帧。
原 391 条保持步骤/断言及顺序，追加 13 个缓存和 44 个语法控制后成为 448；该 448 条旧版完整 382 不符，不等同 382 个独立 bug。
进一步核对 [PG SET 执行顺序](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/misc/guc_funcs.c)，发现块外 LOCAL 的非法值/非法多值也必须先警告再报错。
新增独立 22 项，严格校验 Simple 的 N/E/Z、Extended 的 1/2/n/N/E/Z 及语法错误无警告；
原 448 控制与断言保持，扩展为 **470**，PG 全过、旧版 404 不符，`b190c065` 虽通过 448 仍有新增 4 处不符。
保留该候选和全部完整日志，再移动警告至值校验前，没有通过删除错误路径关闭这个差异。
第一版检查器另有 143 个 bytes/str 字段名期待错误，已保留原源码/PG 日志；只修检查器表示后，原 391 在 PG 全过，再扩展 448。

实际 Main 使用测试进程的 LD_PRELOAD/mkstemp 计数，只观察 `.dbms-sort-` 创建，不含生产注入 hook，也不用于计时。
31 个完整控制核对两个连接、64 KiB 与 4 MiB、LOCAL 结束恢复、文件重载/显式值/RESET/FROM CURRENT：
低预算实际 SQL 每次创建一个匿名排序文件，高预算为零，完成后无临时描述符；最后完整核对 1,000 行/4,000 单元格。
原生另完整核验 512×2 KiB、>2^53 整数、三列 NULL、空行来源、两个同时存在的排序、预算打开时快照及错误清理；不把合成来源伪装为堆 RID。

### 性能与尚未完成的范围

沿用闭合 1,000 行夹具，各窗口旧/新/旧独立副本，七类各预热 3 次、3 轮×4 样本，每窗口 252，合计 **504 个完整校验样本**。
默认耐久性、15 秒 socket、TCP_NODELAY 不变；计时阶段没有本任务编译、测试或其他基准并行，没有重测 PG 延迟。

默认 4 MiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 75.976 | 84.046 | 82.820 |
| 类型化双键 LIMIT 20 | 34.814 | 42.266 | 37.495 |
| 同排序 OFFSET 500 | 34.107 | 40.169 | 39.469 |
| 其他分流的双键无界排序 | 90.779 | 94.554 | 96.302 |
| 主键点查 | 10.495 | 14.206 | 11.913 |
| ARRAY[id] LIMIT 1 | 10.768 | 12.779 | 12.931 |
| 普通 id LIMIT 1 | 9.740 | 11.486 | 11.242 |

64 KiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 77.242 | 69.557 | 77.659 |
| 类型化双键 LIMIT 20 | 36.637 | 34.599 | 33.041 |
| 同排序 OFFSET 500 | 36.433 | 35.434 | 35.795 |
| 其他分流的双键无界排序 | 90.035 | 80.373 | 87.935 |
| 主键点查 | 12.462 | 11.798 | 11.297 |
| ARRAY[id] LIMIT 1 | 11.991 | 11.781 | 12.118 |
| 普通 id LIMIT 1 | 10.681 | 9.896 | 10.926 |

默认窗口 Top-N 为 34.814/42.266/37.495 ms、点查 10.495/14.206/11.913 ms，存在明显慢值；
64 KiB 的普通单键排序为 77.242/69.557/77.659 ms。**未证明 SQL 延迟稳定改善或所有查询无回退**，
不能把不同窗口的较低值挑出后推广为全面加速，也未证明全部差异由环境造成。

另有 **600 次**默认预算诊断，旧/新/旧每类 50 查询，CPU 按服务进程的每批累计计数折算：

| 查询 | 旧首 CPU ms/次 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 双键 LIMIT | 31.800 | 30.000 | 31.800 |
| 点查 | 8.400 | 8.800 | 7.800 |
| 普通 LIMIT | 6.600 | 10.200 | 9.800 |
| 数组 LIMIT | 9.600 | 10.600 | 10.800 |

这些不是逐请求 CPU 中位数，不与 504 延迟样本合并。旧末普通 LIMIT 和数组控制也发生漂移，尚未定位既有慢值的共同原因。
本批的确定收益是会话可独立控制实际普通排序预算与生命周期；查询热路径、其他算子的预算及剩余开销继续优化。


配置函数 `current_setting`/`set_config` 仍没有实现，函数 SET 子句与内部 CALL/DML 保存点的完整 GUC 联动尚未验收；
这些是普通未完成工作，不新增用户暂缓项。语法诊断的消息文本/位置还未逐项对齐 PG，work_mem 的共享标量解析分支仍有 TimeZone 措辞。
会话 work_mem 已接普通物理 Sort，类型化/多键/Top-N 其他分流及 hash/aggregate/window/join、结果接收器仍缺完整预算。
没有据本批关闭整个资源治理要求、原始 273 项或完整 PG 兼容性门；Goal 保持 active。

### 构建与证据

反汇编检查显示 dd5/114/b190 三份 executeInternal 的栈序言均为 0xd000 探测加 0xf78；
新设置 helper 在调用处均为独立函数调用，没有支持“新 helper 内联扩大主函数栈”的假设，因此没有加入无证据的 noinline 修改。
代码布局是否影响延迟仍未证实。

最终程序 SHA-256 为 `af67a1cfd6c7e4ce2774e23345e288acaa366038054bd738b81dd97eb04bc8fb`。
公共 Session 头及新增工作内存状态使首次 CMake 重编 16 个生产单元；后续两次 Main 单元重编和一次 Main/Network/Parser 重编，
合计 21 次成功 CPP 编译、16 个不同 CPP，最终链接 62 个当前生产对象，未称 fresh 全部编译。
最后仅移动 Main 中警告顺序，逐对象核对原生所链接的 61 个 core 哈希相同，复用明确不含 Main 的最终 36 原生结果；实际 Main 完整重跑协议/资源/操作/强杀观察。
C++17/O2/ICU/zlib/TLS stub 的构建范围保持，原始 273 项及专属记录不变；验收后原子安装同一程序，直接在 master 提交，不推送或重启既有服务。

证据目录 `build/pg-feature-workmem-_a2m0fg2/`：`native-final.json`、`protocol-final.json`、391/448/470 的旧/PG/各候选完整日志与冻结源码，
`parser-reference-observations.json`、`parser-reference-state-receipt.json`、`runtime-main-accepted.json`、`operations-accepted/operations.json`、
`main-sort-crash-accepted.json`、延迟窗口、实际构建记录及 `final-provenance.json`。

## 普通类型化排序的预算与比较缓存（2026-11-02）

本批输入为 `d8533bc9`，冻结旧程序 `af67a1cf…`，最终程序 `6d6f1724…`。
目标是约束普通类型化排序的输入保留量，减少重复比较准备，并补 NaN WITH TIES 的实际差异；原始 273 项没有关闭。

### 调研与实现

[PostgreSQL 18 tuplesort](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)按调用方工作预算生成有序段、有限路归并，并在顺序消费者请求时执行最终归并。
[DuckDB 排序设计](https://duckdb.org/2025/09/24/sorting-again)将可溢写布局与比较开销一起考虑；
[SQLite 临时文件说明](https://www.sqlite.org/tempfiles.html)区分排序临时结构与事务日志。
本批采用按操作预算与 owning 快照，不实现 DuckDB 的 normalized keys/并行排序，也没有更改本项目的持久化/WAL 格式。

- [BoundedSortRuns.h](../src/executor/BoundedSortRuns.h)把拥有值容量、vector 容量和行上下文计入批次估算，按预算生成段、最多两个匿名已 unlink 文件、有限读头归并。TopN 能放入预算时保留候选堆，超预算转段；之前已淘汰的行仍被保留候选支配。最终上限在下一次 pending refill 前检查。
- [TypedSortSpill.h](../src/executor/TypedSortSpill.h)保留 type/NULL/collation、对象及数组 OID、递归 record、四类 RowContext 值容器、已求值的键/提前投影及输入 ordinal。只序列化 owning 值，不搬动 AST、执行器、参数帧、provider 或 cursor 的所有者。physical cells 不再另存重复副本；Filter 传递真实 typed 值。
- [ExecutionPlan.cpp](../src/executor/ExecutionPlan.cpp)在原比较需求处惰性缓存每键最近类型/排序规则描述符，不改成 double 比较 NUMERIC；同一排序比较定义普通 float4/float8 WITH TIES，修复两项 NaN peer 差异。非 key volatile 仍在输出需求处执行，必要键仍检查全部输入。
- [main.cpp](../src/main.cpp)在执行阶段接通受支持的普通无 LIMIT 多键、表达式及没有单物理键快路径的类型。已有单键物理快路径保留；聚合/HAVING/SetSort 及其 provider/生命周期不在此次接线中。[parser.cpp](../src/parser/parser.cpp)仅修 work_mem 无效语法误写成 TimeZone 的消息，不声明完整 PG 诊断文本/位置一致。

### 内存、资源与正确性

相同 64 KiB 设置、12,000 行×2 KiB payload、真实 child EOF 的活跃分配增量如下；全部值/顺序/NULL/行数继续核验。

| 普通类型化路径 | 冻结旧版 bytes | 最终版 bytes |
| --- | ---: | ---: |
| 大 LIMIT | 73,910,976 | 18,064 |
| DISTINCT | 42,615,632 | 14,768 |
| WITH TIES | 71,604,640 | 50,048 |
| 无界多键 | 73,906,016 | 18,064 |

这不是查询/进程峰值。批次预留约 25% 估算空间，但稳定排序 scratch 与分配器开销没有逐字节纳入 Codec；单个超大行、固定归并读头、上游、AST 和结果接收器还可额外占用。
低预算 SourceContexts 控制实际观测约 155 KB 活跃分配，而不是 64 KiB 硬 RSS 上限。其他算子的完整内存治理仍开放。

最终 39 个原生入口通过，新增 codec 51、通用段 17、真实算子 17 共 85 控制全过。
原 TopN 八个 SQL/输出/需求检查保留，资源要求改为同 64 KiB 下全部小于 1 MiB；旧版四项失败、新版全过。
原八项旧源码和“必须保留全输入”的四个旧资源断言、旧默认预算、候选四失败也完整保留，不能把预期资源契约变化称为四项生产错误。
实际 SourceRows/SourceContexts、OID/嵌套 record、NUMERIC >2^53、TopN、空需求、同图重开、实际 CTE/derived VALUES 和真实堆 volatile 需求均核验。
JOIN 控制使用真实绑定上下文，不表示公共 buildPreparedQueryPlan 已新增 JOIN 算法。

所选完整 26 协议为 25 通过/1 失败；新固定 108 在严格 PG 18.6/最终程序均全过，旧版仅两个 NaN ties 不符。
原 80 顺序与预期不变，追加 28 个无 ARRAY 的普通无 LIMIT 入口；保留正常 text/binary、不限额 Execute。
唯一失败为已有暂缓 Bind/portal 完整 463/40，全部观察与上一批逐字节相同；R197/R303/R304/R305 没有恢复调查。
实际 Main 同一九项旧版六项低预算资源失败、最终九项全过：六项低预算确有匿名文件、三项高预算无文件，完整值正确、最多两个 FD 并全部释放。
35 类操作有限检查通过；实际普通多键类型化排序匿名文件仍打开时 SIGKILL，重开核对 1,000 行/4,000 单元格并再次排序通过。这不是任意修改崩溃或整库恢复验收。

初次完整 38 原生有一个入口失败：新回调用 std::sort，原 80 中十六项 stable_sort 分配观察未发生。
恢复原批次稳定排序回调后最终 80 全过，共享模板/原断言字节不变；没有重开 R185 的旧专项诊断。
初始 codec 声明、AST 字段/命名空间编译错误、NAME 声明/SHOW 标签/statement 名和算子夹具观察时点的失败均保留。
输入 NAME 改用受支持的 cast 后核验，不声称修复 NAME 物理声明；所有最终断言保持单一期望。

### 相邻 SQL 延迟：所有案例与慢值

同一闭合的 1,000 行夹具、共享主机（48 逻辑 CPU）、GCC 13.3/-O2/C++17、ICU/zlib、TLS stub。
每个预算窗口为旧/新/旧独立副本，11 类各 3 轮×4 次，三个 label 共 396 次计时；每 label 每类三次 warmup，固定种子打乱次序。
显式 SET/SHOW work_mem，原默认持久化/后台维护、15 秒 socket 与 TCP_NODELAY 保留。计时含全部结果校验，夹具准备在外。
类型化全排序含 ARRAY 输出以保持旧/新相同接入；普通无 ARRAY 的实际落盘由上述九项独立验证。
NUMERIC 性能夹具是常规小值，超 2^53 精度由独立正确性测试验证，不把两者混为同一性能负载。

首测为 64kB、默认值 4MB、高值 16MB，共 1,188 次。
16MB 的旧基线及点查控制出现大幅漂移，随后仅按完全相同条件增加一次完整 396 次复测；原首测没有被替换。
合计 1,584 次计时、396 次 warmup、24 次 SET/SHOW，即 2,004 条 SQL 全值/前置校验均通过，源夹具 1,072 个文件哈希不变。
表中均为每段全部十二个样本的 p50 ms；没有置信区间，不能从小表共享主机样本推导并发/大数据普遍收益。

#### 64kB

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| C 文本多键全排序 | 162.263 | 155.436 | 177.105 |
| NUMERIC 多键全排序 | 227.480 | 229.655 | 231.684 |
| float8 多键全排序 | 285.572 | 293.548 | 286.770 |
| 取模表达式全排序 | 149.936 | 163.022 | 155.354 |
| 整数多键全排序 | 136.740 | 147.104 | 142.231 |
| 大 LIMIT 10000 | 146.019 | 150.391 | 141.790 |
| LIMIT 20 OFFSET 500 | 117.882 | 122.106 | 115.607 |
| DISTINCT 后 LIMIT | 40.016 | 47.655 | 39.674 |
| WITH TIES | 66.879 | 74.814 | 67.158 |
| 整数 TopN 控制 | 37.712 | 35.178 | 35.432 |
| 主键点查控制 | 13.210 | 11.864 | 14.362 |

#### 4MB（默认值）

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| C 文本多键全排序 | 162.299 | 150.548 | 163.762 |
| NUMERIC 多键全排序 | 221.335 | 210.214 | 221.526 |
| float8 多键全排序 | 280.128 | 249.347 | 287.877 |
| 取模表达式全排序 | 147.507 | 150.333 | 149.163 |
| 整数多键全排序 | 135.663 | 141.992 | 135.832 |
| 大 LIMIT 10000 | 133.343 | 143.584 | 133.667 |
| LIMIT 20 OFFSET 500 | 114.208 | 120.503 | 124.413 |
| DISTINCT 后 LIMIT | 37.323 | 42.030 | 37.634 |
| WITH TIES | 63.615 | 69.909 | 63.920 |
| 整数 TopN 控制 | 33.106 | 37.797 | 35.589 |
| 主键点查控制 | 10.623 | 11.641 | 11.511 |

#### 16MB 首测（基线漂移）

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| C 文本多键全排序 | 669.305 | 146.135 | 667.971 |
| NUMERIC 多键全排序 | 657.308 | 219.671 | 305.462 |
| float8 多键全排序 | 764.393 | 282.616 | 360.045 |
| 取模表达式全排序 | 361.609 | 158.837 | 457.788 |
| 整数多键全排序 | 411.766 | 141.772 | 348.307 |
| 大 LIMIT 10000 | 619.482 | 141.261 | 404.240 |
| LIMIT 20 OFFSET 500 | 569.729 | 120.514 | 302.633 |
| DISTINCT 后 LIMIT | 648.548 | 42.807 | 202.819 |
| WITH TIES | 269.703 | 65.953 | 192.876 |
| 整数 TopN 控制 | 340.562 | 35.545 | 47.400 |
| 主键点查控制 | 551.628 | 11.341 | 18.019 |

#### 16MB 唯一复测

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| C 文本多键全排序 | 162.492 | 141.371 | 172.210 |
| NUMERIC 多键全排序 | 227.946 | 212.380 | 230.624 |
| float8 多键全排序 | 293.755 | 255.990 | 313.377 |
| 取模表达式全排序 | 153.890 | 163.260 | 168.170 |
| 整数多键全排序 | 152.355 | 140.367 | 172.918 |
| 大 LIMIT 10000 | 151.798 | 148.697 | 154.604 |
| LIMIT 20 OFFSET 500 | 115.575 | 114.292 | 120.866 |
| DISTINCT 后 LIMIT | 41.997 | 38.469 | 44.082 |
| WITH TIES | 75.541 | 75.006 | 78.764 |
| 整数 TopN 控制 | 37.792 | 35.852 | 39.168 |
| 主键点查控制 | 12.087 | 28.488 | 16.041 |

默认 4MB 的文本/NUMERIC/float8 全排序在本次窗口有所改善，其他若干路径变慢；64kB 外排的 I/O/编码成本也使多项变慢。
16MB 首测连点查都出现 551.628/11.341/18.019 ms，不把其巨大倍率归因于本次代码。
唯一复测的文本为 162.492/141.371/172.210 ms，但点查仍是 12.087/28.488/16.041 ms；较慢控制与波动原因尚未解决。
复测前后主机/既有进程 CPU/I/O 快照保留，这些计数不能证明初测漂移原因，也不能证明控制项变化与本批无关。
资源保留量改善已证实；不能宣称所有 SQL 稳定提速、无回退或达到 PostgreSQL 性能水平。

### 构建来源与复现

CMake 共 17 次成功 C++ 对象编译、16 个不同 CPP，最终链接全 62 对象（61 core+Main）；最后标准 CMake target 仅链接、零额外重编译。
初次 Main 失败保留，修正后的 Main 输入逐字节冻结；core 构建期间 Main 单独修正的差异另列审计，实际 core 输入不变。
后续仅 ExecutionPlan 的新回调重编译；最终原生 39 和实际 Main 26 使用相同冻结 core/程序。
最终输入、62 对象、61 core、测试来源和产物 SHA 见本批 `release-binary.json`、`native-release-core-hashes.json`、`final-provenance.json`。

可复现驱动为 [benchmark_typed_sort.py](../scripts/benchmark_typed_sort.py)，沿用 [benchmark_queries.py](../scripts/benchmark_queries.py) 的真实服务/客户端。
计时驱动 `59792eb4…` 与仓库驱动 `9a8d8e0b…` 只因所在目录不同改变 REPO parents[2]→parents[1]；逆替换源码与规范化 AST 全等，--help/py_compile 通过。
使用冻结旧二进制、同一个已关闭夹具和每次全新输出目录，例如：

```bash
python3 scripts/benchmark_typed_sort.py \
  --before build/pg-feature-typed-sort-zm9x0zst/before-dbms_main \
  --after build/cmake/dbms_main \
  --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 \
  --work-mem 64kB --rows 1000 --rounds 3 --samples 4 \
  --output build/retest-typed-sort-64k
```

4MB/16MB 使用相同命令的对应预算和新目录；这会启动私有副本，不打开源夹具，也不重启用户服务。
新建夹具可用既有 benchmark_topn.py 的无 --fixture 模式，但新夹具的历史/WAL/缓存不同，不能冒称与本次冻结输入字节相同。
本机证据目录 `build/pg-feature-typed-sort-zm9x0zst/` 保留四个 `typed-latency-*/comparison.json` 全部样本、
初测独立汇总、复测/主机快照、原版本与每个失败候选、PG108、39/26 汇总、实际 Main 九项和强杀重开记录。
这些 build 证据是本机保留工件；仓库提交测试与驱动，不能把未打包工件当作可分发发布物。

聚合/GROUP/HAVING/SetSort、窗口/哈希/JOIN 本身、结果接收器、虚拟生成列/未接入旧分流、完整配置函数/GUC 和其他原始功能族继续开放。
原始 273 项、原子要求与专属暂缓原文不变，Goal 继续 active；无新 Markdown、分支或推送。

## 集合排序预算与 VALUES 查询子句（2026-11-02）

本批以 `8a8532a0` 为输入，冻结旧程序 `6d6f1724…`，最终程序为 `a3bdab66…`。
产品仍为 0.2.0 / C++17、PG18.6 行为基线 / wire3.0，GCC13.3/-O2、ICU/zlib、TLS stub；不将本批写成整库 PG 等价或 TLS 验收。

### 调研与实现边界

| 一手来源 | 本批采用的做法与限制 |
| --- | --- |
| [PostgreSQL18.6 Sort](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeSort.c) | 从真实输出描述符和 work_mem 初始化排序，首次实际需求才读取输入；静态 LIMIT 可给有界候选，仍处理所需全部输入。 |
| [PostgreSQL VALUES](https://www.postgresql.org/docs/18/sql-values.html)与[分析器](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/analyze.c) | VALUES 各列先选择共同类型，随后注册真实输出范围供排序使用；不把列名称伪装成运行时参数或物理表。 |
| [PostgreSQL Limit](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c)与[计数分析](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_clause.c) | 计数是 bigint 表达式，拒绝本查询层变量，按每次实际调用求 OFFSET 再求 LIMIT；零条不要求行源，WITH TIES 使用真实边界键。 |
| [SQLite vdbesort](https://github.com/sqlite/sqlite/blob/master/src/vdbesort.c) | 超预算生成有序段，记录同时保存排序键和输出；复用本项目完整 owning codec，保持 NULL、collation、OID、数组和递归 record 元数据。 |
| [DuckDB 排序设计](https://duckdb.org/2025/09/24/sorting-again) | 比较开销和可溢写布局同样影响速度；本批只惰性缓存原表达式比较描述符，没有实现其字节规范化键或并行向量执行。 |

[ExecutionPlan.cpp](../src/executor/ExecutionPlan.cpp)的普通 UNION ALL SetSort 复用现有 BoundedRuns/TypedSortRowCodec，
按打开时的会话预算分批、最多两个匿名文件 FD、稳定 ordinal、最终按需归并。
存在非零静态 LIMIT、没有 WITH TIES、LIMIT+OFFSET 加法安全时保留有界候选；动态 VALUES 计数目前使用完整有预算排序，没有动态下推 Top-N。
键比较仍由原表达式/绑定类型所有者执行；scalar float4/float8 的 NaN/Infinity 与枚举次序采用真实比较元数据，嵌套复合类型的其他既有比较缺口不因此关闭。

VALUES 的原 token/span、共同类型、真实 column1/column2 范围、关联子查询及隐藏排序键均保留；
LIMIT/OFFSET 表达式在实际生产者的同一执行器中求值，新调用由既有 VALUES owner 刷新自己的 carrier，无新参数帧所有者。
[prepared_query_execution.cpp](../src/expression/prepared_query_execution.cpp)只新增两项 AST 子节点索引，原 retained constant-planning 算法不改。
原 PreparedAppend 整类/执行器工厂 owner 前缀、共享 CPU-sort helpers 和用户暂缓模块保持原字节。

[DmlResult](../src/commands/DmlExecutor.h)拥有真实输出描述符，正常 Network 描述及结果传递实际 OID/typmod/目录长度；
验证为 scalar enum 的二进制输出发送标签字节，保留物理 table/attribute origin 与 NULL/空标签/文本 NULL 区别。
输入解码器、既有 Parse/Bind/portal/format2 策略及 handleClient 主体不改。未知 OID 计数参数推导、enum 数组二进制输入/输出及任意自定义类型 codec 仍不完整。

### 正确性、失败与实际资源

最终 44 个原生入口全部通过；新 SetSort26、grammar860、VALUES execution63 共 949 控制全部通过。
同最终源码完整旧 ABI/旧头/旧 stub 基线分别有 20/694/63 项不符，不能把控制数当成独立缺陷数。

| 固定协议矩阵 | PG18.6 | 最终程序 | 冻结旧版不符 |
| --- | ---: | ---: | ---: |
| 集合排序112 | 112/0 | 112/0 | 8 |
| VALUES查询子句143 | 143/0 | 143/0 | 127 |
| 实际枚举描述符33 | 33/0 | 33/0 | 20 |
| 显式输入 OID 计数59 | 59/0 | 59/0 | 51 |
| 计数类型/语法阶段52 | 52/0 | 52/0 | 48 |

原 31 个协议入口整批首轮为 29通过/2失败；暂缓 Bind/portal 完整 463 项及其中 40 差异整份日志与前版逐字节相同。
另一项在 runner.start_ours 的 sock.connect、发送任何 SQL 之前 ConnectionAbortedError；同程序/源码完整独立重跑通过，最终逐入口复核为 30/1。
原始整批 JSON/连接错误留存，没有改启动重试、socket超时或网络策略。新增52永久入口另按同一程序验证，全32个已运行入口按各自最终结果复核为31/1；不把两次验证冒称一次全库测试。

记录全部失败而不放宽成多种可接受结果：

- C1 SetSort26 有两处真实取消/超时清理失败，修复原 TIES 包装器在成功取得一行后的主异常清理，C2起26/0。
- C2 set112仍有4处 VALUES语法/枚举 RowDescriptionOID 差异；C3 enum33/set112/旧typed108全过，但VALUES143仍7处失败。补真实 Main scalar VALUES admission与计数AST/绑定后最终143/0。
- C3原生44中的两个失败为 grammar child/span oracle 与 collation规范值夹具；按真实单一 AST/运行期 c修正，原始620/14和30/4保留。C4的4个::AST形状及4个参数槽位预期也仅修fixture，参数仍按真实PreparedQuery.uses关联，没有重排生产帧。
- C4 grammar860中缺操作数两处是真语法错误；type52探针完成49观察后在Extended Parse不完整CASE处SIGABRT，另外3观察未执行。修复局部语法边界/异常返回与结构化42601，最终相同完整52/0；绝不将该首轮称作完整52项结果。
- 实际PG/C4反例证明括号inline VALUES集合的全局表达式计数曾被静默忽略；现已解析到该形状时明确0A000，仍列未实现，未括号全局形式还可能42601。9个局部/静态正对照与PG一致；额外工具对未括号形式的错误预测及全部13观察保留。

实际 Main 同一七项全部4,107行/12,321单元值正确。旧版4项资源失败；最终低64kB全排/OFFSET/TIES/超大行实际落盘，高16MB全排/OFFSET和低预算LIMIT1无文件，最多2 FD且结束全部关闭/无命名残留。
首轮采样曾截到mkstemp→unlink间的瞬时命名FD；工具原“所有瞬时目标必须已deleted”断言失败，原源码/报告保留。
修正后的观察器严格校验实际数据目录归属、同FD/path随后deleted或确实关闭且路径消失，且spill必须有真实deleted证据；SQL、预算、全值、FD上限、最终清理要求不变，旧/新完整7项重新验证。
生产匿名文件 helper没有为此修改。预算按拥有值估算，稳定排序scratch、分配器、超大行/固定读头、上游与结果接收器仍可额外占用，不能称硬RSS上限。

35类操作现有有限检查全部通过；真实全局SetSort匿名文件仍打开时，仅向验证exe/data-dir/start-ticks的自建PID发送SIGKILL，
同一私有目录重开核对1,000行/4,000单元，再次核对2,000行集合排序及清理。源fixture1,072文件SHA不变；不是任意修改崩溃矩阵或全库恢复验收。

### 相邻旧/新/旧 SQL 延迟：全部操作

同一已关闭1,000行fixture，每次独立私有副本；集合排序实际有两个分支，共2,000行输入，三列完整输出。
11类各3轮×4次、旧/新/旧共396计时/预算；64kB、4MB、16MB共1,188计时、297预热、18条SET/SHOW，合计1,503条全值/预算校验通过。
固定种子打乱轮内次序，保留默认持久化/后台维护、15秒socket与TCP_NODELAY；准备在计时外，完整结果校验在计时内。
计时前所有自建编译/回归/资源进程已终态，窗口内不启动其他测试或编译；共享主机48逻辑CPU，非并发/冷缓存/大数据验收，无置信区间。
同行顺序只属于冻结heap夹具，不承诺没有完整ORDER键的生产查询固定子集；NUMERIC性能夹具是常规值，超2^53精度另由正确性测试验证。
表中为全部12样本p50毫秒，没有删慢值或追加择优窗口。

#### 64kB

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排序 | 257.193 | 296.194 | 249.444 |
| NUMERIC 全排序 | 380.867 | 410.851 | 374.816 |
| float8 全排序 | 454.178 | 489.810 | 441.665 |
| float4 全排序 | 397.745 | 383.115 | 396.692 |
| 整数全排序 | 241.006 | 268.703 | 228.674 |
| 固定夹具同行稳定排序 | 247.592 | 272.855 | 237.755 |
| LIMIT 20 | 195.501 | 181.917 | 185.988 |
| LIMIT 20 OFFSET 500 | 196.011 | 198.347 | 188.215 |
| WITH TIES | 204.690 | 201.640 | 198.980 |
| 普通整数 TopN 控制 | 35.369 | 35.428 | 34.236 |
| 主键点查控制 | 11.945 | 12.600 | 11.653 |

#### 4MB

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排序 | 253.475 | 260.524 | 255.186 |
| NUMERIC 全排序 | 380.262 | 379.447 | 388.789 |
| float8 全排序 | 443.540 | 436.245 | 453.670 |
| float4 全排序 | 399.986 | 366.521 | 399.462 |
| 整数全排序 | 234.316 | 243.532 | 236.073 |
| 固定夹具同行稳定排序 | 257.177 | 249.853 | 246.380 |
| LIMIT 20 | 192.492 | 185.582 | 195.918 |
| LIMIT 20 OFFSET 500 | 187.691 | 233.081 | 189.220 |
| WITH TIES | 200.362 | 208.650 | 225.586 |
| 普通整数 TopN 控制 | 36.174 | 34.579 | 36.437 |
| 主键点查控制 | 12.045 | 13.442 | 13.992 |

#### 16MB

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排序 | 250.781 | 264.514 | 251.167 |
| NUMERIC 全排序 | 376.221 | 388.562 | 377.866 |
| float8 全排序 | 437.895 | 444.773 | 443.356 |
| float4 全排序 | 397.567 | 361.624 | 390.029 |
| 整数全排序 | 229.612 | 239.280 | 229.361 |
| 固定夹具同行稳定排序 | 242.945 | 235.692 | 243.289 |
| LIMIT 20 | 196.006 | 191.012 | 194.417 |
| LIMIT 20 OFFSET 500 | 195.155 | 227.381 | 189.901 |
| WITH TIES | 195.336 | 210.169 | 207.504 |
| 普通整数 TopN 控制 | 33.919 | 35.731 | 35.035 |
| 主键点查控制 | 12.705 | 11.793 | 12.581 |

float4全排序在本窗口改善；4MB的LIMIT20为192.492/185.582/195.918ms，幅度有限。
低64kB的多项全排变慢，旧SetSort不执行该预算而新版承担临时编码/I/O成本；资源约束不能当成普遍加速。
即使4MB/16MB不以落盘解释，也有未解决慢值：OFFSET分别187.691/233.081/189.220ms与195.155/227.381/189.901ms，文本/整数全排及部分控制也较慢。
有界堆、比较临时上下文/值拷贝及其他执行成本仍需后续profiling与优化；本批未证明全部查询提速、无回退或达到PostgreSQL性能水平。

### 构建与复现

C3/C4/C5标准CMake target分别523.40/522.38/32.60秒，成功重编17/16/3对象，共36次/17不同CPP，最终链接62对象（61core+Main）。
这是按依赖重建，不声称62对象全部fresh；AST/DmlResult ABI变化的所有依赖者与原生test_stubs使用同代头和对象。
最后仅三CPP变化，Main不因此重编；最终44原生与实际Main所有协议/资源使用相同冻结61core/程序。
全部212输入源码、62对象、61core、测试/驱动及工件SHA保存在`final-provenance.json`；初始const Expr*编译错误及所有候选失败留存。

复现驱动[benchmark_set_sort.py](../scripts/benchmark_set_sort.py)复用[benchmark_queries.py](../scripts/benchmark_queries.py)真实服务/客户端。
计时artifact脚本1099dfa1…与仓库脚本39b1450e…仅REPO.parents[2]→[1]，源码逆替换和AST归一完全相等。

```bash
python3 scripts/benchmark_set_sort.py \
  --before build/pg-feature-set-sort-wi9y4qzg/before-dbms_main \
  --after build/cmake/dbms_main \
  --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 \
  --work-mem 64kB --rows 1000 --rounds 3 --samples 4 \
  --output build/retest-set-sort-64k
```

4MB/16MB使用对应参数与新目录。旧程序、fixture与完整raw样本/PG对照/失败/资源/crash/source snapshots在本机`build/pg-feature-set-sort-wi9y4qzg/`，不是已打包可分发发布物；仓库提交永久测试、驱动和文档。
原始273 items/原子要求/725受保护记录未改，R185/R197/R244/R303/R304/R305等用户暂缓保持；Goal继续active。
聚合/其他集合分流/窗口/哈希/JOIN/结果接收器/完整GUC与其他原始功能族继续开放；无新Markdown、分支或推送。

## 集合整数键缓存与比较参数复制（2026-11-02）

输入bb74ae97/前版a3bdab66…，最终95defdc5…；仍为C++17/product0.2.0/PG18.6基线、GCC13.3/-O2、ICU/zlib、TLS stub。
本批针对前次OFFSET慢值先测真实算子，再仅修改ExecutionPlan.cpp的类型化键缓存与比较参数复制。

### 一手调研与实际计数

[PG18.6 tuplesort](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/sort/tuplesort.c)在输入超过2K等条件时转堆；
[DuckDB1.5.6](https://github.com/duckdb/duckdb/blob/v1.5.6/src/optimizer/topn_optimizer.cpp)结合上限/比例选择TopN；
[SQLite3.50.4](https://github.com/sqlite/sqlite/blob/version-3.50.4/src/select.c)以LIMIT+OFFSET控制临时有序索引。
PG/DuckDB在N2000/K520的本例仍选TopN，不能说照抄阈值就能消除回退。
[DuckDB排序设计](https://duckdb.org/2025/09/24/sorting-again)强调紧凑类型化键；本项目只复用精确整数缓存，完整输出值仍拥有自己的metadata。

冻结私有计数overlay不改正式堆/中断helper；两个真实1000行UNION ALL分支、三键amountDESC/idASC/branchASC与独立oracle，完整4040行/16160单元校验通过。

| 输入/路径 | less调用（前/新相同） | 前版标量探测 | 新版标量探测 |
| --- | ---: | ---: | ---: |
| 固定夹具全排 | 12561 | 19553 | 0 |
| 固定夹具OFFSET/K520 | 17040 | 27475 | 0 |
| 固定洗牌全排 | 21511 | 37282 | 0 |
| 固定洗牌OFFSET/K520 | 14367 | 23247 | 0 |

replacement/rejection、每阶段比较和容量遍历次数也相同；没有记录逐比较decision trace，不以计数相同替代完整值验证。
原夹具OFFSET有998次替换/482拒绝，洗牌则687/793；前者增加比较，后者减少，故保留堆策略并降低每次比较成本。
这些命中整数缓存的样本合计107557次表达式探测归零，不代表其他类型/全查询不再调用求值器。
排序phase的new请求186049/291154/391674/246365→13/12/13/12，仅覆盖Runs阶段，不含add之前的缓存构造；不能说全查询零分配。
系统perf_event_paranoid4拒绝perf，本批未改全局设置。instrumented数据只作计数，不用于速度结论。

精确raw smallint/integer/bigint在必要表达式求值之后，用from_chars全串及int16/int32范围检查缓存int64；只有同原类型、非enum且两缓存有效才用关系比较，避免差值溢出。
NULL/方向/ordinal与TIES原等号owner保持；非法/越界值不提前抛错，域/OID/NUMERIC/float等仍走原比较器，不扩类型转换规则。
缓存直接关联原cells/隐藏keys，不复制另一份输出键；私有codec独立验证integerCount，ordinary TypedSort三参数描述符仍默认cacheCount=keys。
K个optional缓存容量计入预算，普通全非整数键Set不加该向量，VALUES复用原槽位；fixed codec对象增16B，缓存不是零内存/硬RSS上限。
原比较使用每次自己的owning vector，再move入RowContext，省去initializer_list额外深拷贝；不跨比较缓存datum/上下文/帧，求值器仍可能复制值。

### 实际分配、资源与回归

同源码真实算子两次同图open并改变输入，独立核对完整递归metadata；下列为epoch0从open到首输出的C++new累计请求，不是活跃内存、RSS或CPU时间。

| 路径 | 前版请求数/bytes | 新版请求数/bytes |
| --- | ---: | ---: |
| 128行整数全排 | 22761 / 817676 | 8468 / 499584 |
| 128行整数OFFSET | 17890 / 662245 | 8467 / 439088 |
| 128行record/8191B尾/OID257 | 见完整记录 / 237506623 | 见完整记录 / 181385965 |

record EOF后累计请求204125226→148004552B，特殊payload/OID尺寸请求各6168→4468；N1无比较控制的EOF后两特殊请求均0。
record仅按record键排序（无次级整数键），first-int决定次序而大尾保留，独立验证复制优化。
整数EOF前请求8339→8467，新增128个缓存向量也被计入总量，没有只报省下的部分。

既有44原生全过，另加永久19控制全过，最终45入口全过；同源码旧19中6项仅新成本预算失败，全部metadata/resource通过，不是6个功能bug。
真实64kB整数spool/重开覆盖keys0/integerCountK独立arity；公共typed-source宽度破坏在已落盘后精确XX000/FD0/重复next无工作，不称私有磁盘header破坏测试。
既有32协议31/1，加严格PG18.6/前版/新版均66/0矩阵后33入口32/1；唯一暂缓Bind/portal完整463/40整份日志逐字节不变。
新66覆盖极值/±2^53邻值、NULL/方向、LIMIT/OFFSET/TIES、类型提升、隐藏键、真实enum/OID/domain/numeric/record/text回退及显式输入OID的正常text/binary不限额Execute。
首版uppercase OID前提共47处失败（2个DDL的42704、45个表不存在42P01级联），无RowDescription错误记录也留存；最终仅物理列BIGINT+查询o::oid，所有非SQL预期逐项相等，DDL缺口未修也未删除。
实际Main七项资源、35类有限操作、匿名文件内强杀/重开1000行4000单元及再次2000行排序通过；源fixture1072文件SHA未变，不是整库/任意修改恢复验收。

### 实际SQL：全部窗口、漂移和慢值

canonical benchmark_set_sort.py未改，同一关闭的1000行fixture/私有副本，两个分支2000输入，11类各3轮×4次/旧新旧396计时每窗口。
三预算前版a3对照，加预先安排的4MB更早6d6对照，共1584样本；首测4MB旧基线漂移后唯一追加同条件396复测，原窗口全部保留。
合计1980计时、495预热、30SET/SHOW=2505校验，默认持久化/后台维护、socket15s/TCP_NODELAY保持；完整结果验证在计时内、准备在外。
所有自建编译/回归/探针终态后才计时，窗口内不跑其他测试；共享48逻辑CPU主机，无置信区间，不是并发/冷缓存/大数据验收。
以下为各label全12样本p50ms，不删慢值；同行顺序仅属于固定heap夹具，NUMERIC性能值不代替超2^53精度验收。

#### 64kB：前版a3

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 306.011 | 283.568 | 298.034 |
| NUMERIC全排 | 424.416 | 403.522 | 420.206 |
| float8全排 | 493.887 | 470.951 | 512.460 |
| float4全排 | 400.367 | 379.857 | 393.336 |
| 整数全排 | 278.636 | 194.661 | 278.608 |
| 固定同行 | 281.674 | 194.388 | 284.325 |
| LIMIT20 | 177.627 | 142.863 | 184.013 |
| LIMIT20OFFSET500 | 203.829 | 155.364 | 203.259 |
| WITH TIES | 205.008 | 148.963 | 209.882 |
| 普通整数TopN控制 | 36.283 | 35.308 | 39.386 |
| 主键点查控制 | 14.468 | 13.073 | 15.317 |

#### 4MB首测：旧基线漂移

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 440.970 | 250.256 | 281.011 |
| NUMERIC全排 | 724.082 | 375.367 | 397.042 |
| float8全排 | 918.079 | 437.287 | 447.548 |
| float4全排 | 385.274 | 366.722 | 377.301 |
| 整数全排 | 250.721 | 198.485 | 236.286 |
| 固定同行 | 265.279 | 201.279 | 263.426 |
| LIMIT20 | 576.116 | 147.216 | 183.965 |
| LIMIT20OFFSET500 | 520.119 | 149.502 | 230.586 |
| WITH TIES | 210.991 | 152.629 | 217.142 |
| 普通整数TopN控制 | 261.315 | 38.425 | 34.789 |
| 主键点查控制 | 13.743 | 13.771 | 14.021 |

#### 16MB：前版a3

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 259.612 | 246.910 | 252.193 |
| NUMERIC全排 | 387.230 | 378.437 | 384.089 |
| float8全排 | 430.923 | 419.164 | 430.148 |
| float4全排 | 359.159 | 357.055 | 357.244 |
| 整数全排 | 244.719 | 184.605 | 241.352 |
| 固定同行 | 249.064 | 188.882 | 244.198 |
| LIMIT20 | 187.448 | 146.256 | 185.549 |
| LIMIT20OFFSET500 | 232.827 | 147.081 | 226.088 |
| WITH TIES | 213.177 | 145.723 | 207.864 |
| 普通整数TopN控制 | 37.848 | 37.105 | 38.944 |
| 主键点查控制 | 13.111 | 12.449 | 13.976 |

#### 4MB：更早6d6对照

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 250.976 | 247.990 | 254.868 |
| NUMERIC全排 | 373.740 | 362.496 | 379.129 |
| float8全排 | 430.850 | 411.550 | 453.733 |
| float4全排 | 386.174 | 354.832 | 396.239 |
| 整数全排 | 233.550 | 194.164 | 237.939 |
| 固定同行 | 243.413 | 190.354 | 245.487 |
| LIMIT20 | 191.975 | 139.974 | 206.527 |
| LIMIT20OFFSET500 | 188.110 | 145.427 | 200.599 |
| WITH TIES | 200.874 | 149.127 | 200.655 |
| 普通整数TopN控制 | 37.968 | 37.321 | 40.359 |
| 主键点查控制 | 11.429 | 13.971 | 11.710 |

#### 4MB唯一同条件复测：前版a3

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 263.290 | 254.034 | 254.771 |
| NUMERIC全排 | 392.293 | 387.081 | 375.557 |
| float8全排 | 459.073 | 441.150 | 438.891 |
| float4全排 | 371.953 | 365.252 | 368.331 |
| 整数全排 | 247.619 | 197.770 | 235.018 |
| 固定同行 | 249.608 | 192.337 | 242.707 |
| LIMIT20 | 187.244 | 150.307 | 183.609 |
| LIMIT20OFFSET500 | 228.916 | 152.181 | 222.064 |
| WITH TIES | 205.166 | 153.472 | 206.106 |
| 普通整数TopN控制 | 37.897 | 36.778 | 38.647 |
| 主键点查控制 | 15.666 | 12.562 | 11.334 |

首4MB的普通TopN控制261.315/38.425/34.789ms及若干旧版先测明显漂移，不能把巨大倍率归因于代码；唯一同条件复测也完整列出。
64kB整数全排278.636/194.661/278.608ms、OFFSET203.829/155.364/203.259ms；16MB OFFSET232.827/147.081/226.088ms，支持这些窗口的实际改善。
预先安排6d6对照OFFSET188.110/145.427/200.599ms，重新核查先前回退；但其点查11.429/13.971/11.710ms较慢，不能称全查询无回退。
其他类型与控制波动也保留，不声明稳定普遍提速或达到PG整体性能。

### 构建与复现

标准CMake45.99秒仅重编ExecutionPlan.cpp并链接62对象；60core/Main不变，最终45原生使用相同61core，同代fresh stub仍0b53…。
212源码仅该CPP变化；私有计数overlay/对象完全隔离，未装入正式程序。全部源码/对象/驱动/测试/失败/样本SHA见本机build/pg-feature-set-key-33l7ewyf/final-provenance.json，工件不是已打包发布物。

```bash
python3 scripts/benchmark_set_sort.py \
  --before build/pg-feature-set-key-33l7ewyf/before-dbms_main \
  --after build/cmake/dbms_main \
  --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 \
  --work-mem 64kB --rows 1000 --rounds 3 --samples 4 \
  --output build/retest-set-key-64k
```

4MB/16MB使用相应参数和新目录，更早对照换前批6d6 before-dbms_main。原273/原子条款/725受保护记录与全部用户暂缓不变，Goal active；全局集合计数/FETCH/未知OID/GUC和其他功能族继续开放。无新Markdown/分支/推送。

## 剩余瓶颈与适用范围

普通修改、保存点和其他 DDL 仍有物理快照、WAL 全文件映像及同步 I/O 开销；大数据库下会继续放大。整数 DML 筛选仍扫描堆，JOIN 优化仍可能枚举大量键对，索引覆盖、统计信息、计划选择、批处理及更细粒度的回滚机制仍有优化空间。

本次未改变 C++ 标准、存储逻辑格式或持久化契约，未做 TLS、跨主机网络、大数据冷缓存、长时间压力测试或全仓库兼容性验收。既有兼容性缺口、明确暂缓项及历史 R164 超时根因不因这一性能批次自动关闭，参见[测试报告](test-report.md)与[兼容性契约](compatibility-contract.md)。

## 全局计数表达式与动态排序上限（2026-11-02）

本批输入baba4bcb/95defdc5…，保持C++17、product0.2.0、PG18.6行为基线、GCC13.3/-O2、ICU/zlib与TLS stub。
证据目录为 `build/pg-feature-global-count-xqbytfo5/`，构建、全部对照、初次失败和最终哈希分别见该目录的回执；这些本地证据不是可移植发行包。

### 参考实现与生产范围

[PG18.6 nodeLimit](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c)在每次执行/重扫重新读取实际参数，先OFFSET后LIMIT，零上限不向子计划取行，动态NULL上限表示无界。
[parse_clause](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_clause.c)区分FETCH WITH TIES的原始NULL常量和运行时NULL；
[gram.y](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/gram.y)限制FETCH和带ROWS后缀的OFFSET为c_expr/有符号数字，括号内才容纳一般算术。
[analyze.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/analyze.c)在全局集合计数分析前恢复临时ORDER命名空间，
[parse_param.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_param.c)按实际输入上下文推导参数声明。
[DuckDB1.5.6 PhysicalLimit](https://github.com/duckdb/duckdb/blob/v1.5.6/src/execution/operator/helper/physical_limit.cpp)把未求值边界保存在执行状态，并处理NULL与最大边界；其求值顺序与本项目的PG目标不能直接等同。
[SQLite3.50.4 select.c](https://github.com/sqlite/sqlite/blob/version-3.50.4/src/select.c)用LIMIT/OFFSET寄存器控制临时排序索引；本项目复用已有有界排序设施，仍按PG规则处理负数、作用域与错误阶段。

真正的UNION ALL根（SELECT、inline VALUES、括号集合及受支持CTE/derived/physical分支）现可绑定并执行全局LIMIT/OFFSET表达式；VALUES/set FETCH保留真实计数AST、角色与源字节。
全局计数只有空的本层范围与真实祖先，不能借用ORDER输出列或分支FROM；子查询仍有自己的局部范围。OFFSET先分析/求值，失败不触碰LIMIT，零结果不打开数据分支。
当前层aggregate/window/SRF按实际函数身份与语义来源拒绝；同名已选stored scalar不从拼写继承aggregate角色。
普通MetadataPlaceholder沿原identity/slot和实际选中输入类型推导声明，更新同一输入既有使用处；Network仅在现有Parse元数据阶段按真实源位置映射到$n，并整体验证后发布。
已选CAST/算术/比较/连接/CASE/COALESCE上下文有覆盖；这不是所有函数重载、域/OID或参数约束的完整PG推导，也没有修改实际参数帧、保留计划或portal所有权。

动态计数借用原Append的执行开始/结束回调；它们移到真实Count/Sort/Append外围，未另建或替换borrowed owner。
关闭时先释放原子图/文件再结束原owner，保留主异常。真正同一图重绑定使用当前祖先行与允许覆盖的原参数来源；StatementInput继续不可被调用方伪装成RuntimeCell。
有限非TIES的limit+offset安全相加后作为本次排序上限；每次open都重置，NULL、TIES和溢出组合回到完整预算排序。全部源值和排序键仍需求值，错误不能因候选被丢弃而消失。
新动态路径也用于已有VALUES表达式计数，K很小时允许不落盘；完整输出metadata、预算和匿名文件上限保持。普通独立SELECT的表达式计数和其他集合算法没有因此全部补齐。
顶层无写CTE的这些真实计数查询复用已有运行错误接收器，执行失败保留真实RowDescription及已产生的行前缀；分析/规划失败不提前发布描述符，不改格式或portal策略。

### 严格对照与保留的失败

最终正式 CMake 18 次成功编译、16 个不同 CPP（另一次非冻结私有诊断编译不作验收证据），两次链接各62对象；相关头文件消费者与新test stub同代，未声称重编全部62对象。
最终程序58c94e05…与212源码/62对象哈希冻结；48原生入口全过，新增global count执行33、binding34、grammar840全过，同最终测试源旧ABI分别33/31/572处失败。
新运行协议75与FETCH协议900在PG18.6/最终程序全过，旧版分别61/604处不符；binding58在PG全过、旧53处不符、新版仍1处窗口宿主差异。上述观察数不是独立bug数。900协议逐项比较rows、SQLSTATE、headers、tag、OID、frames、Ready及parameter OID八字段，完整错误诊断raw保留，未声明所有错误文案与PG相同。
原完整364SQL/910协议观察仍保留：首候选25处不符中，接受范围内16个Simple负FETCH缺RowDescription已修复；最终前900全过，另4个独立SELECT的10观察中9个差异仍开放，没有从原始记录删除。

原860语法控制首候选恰2处失败：旧断言拒绝PG合法的括号参数FETCH。保留原源码/日志后仅迁移为严格合法AST/参数源位置检查，其他858字节/顺序不变，最终860全过。
既有VALUES执行63首候选仅1个资源前提失败：K1已有界排序不应强制落盘。该控制改为真实动态NULL以保证先落盘再排序键错误，所有22012/文件/终止断言保留；新增K1控制由PG核实精确源/键计数25/24，最终64全过，旧64仅新增成本契约失败。
新原生最初32中2个同类落盘前提失败的源码/日志也保留；修正为有限无I/O→NULL落盘→再有限不新增I/O，并追加独立K1被丢弃候选错误，最终33全过。

所选原33协议首候选31/2：除既有暂缓Bind完整463/40外，普通锁前SELECT出现57014；原源码和失败日志保留。最终全部按原参数单独顺序执行，原33为32/1，该超时入口通过；未改超时、连接、重试或中断策略。
加新75/58/900三个入口后，最终36入口按各自完整结果为34通过/2失败：上述Bind日志与前版逐字节相同；另一项是新58中的窗口子查询宿主缺口，未当成暂缓或从严格矩阵排除。


实际Main最终12资源控制全过，共4161行/12483单元完整值和精确描述符，最多2个排序FD，EOF全部释放；固定夹具1072文件未变。
35类操作是每类一次的有限正确性检查，其并行负载下时间不用于速度结论；匿名全局计数排序文件打开时对自建进程SIGKILL，重开核对1000行/4000单元并再排序2000行通过，不能推及任意写入崩溃恢复或真实TLS。


窗口子查询 `LIMIT (SELECT row_number() OVER())` 已在PG合法、当前真实执行宿主仍报0A000；严格58项中的该差异保留，未新设暂缓项或将它算作通过。
未知参数全约束、任意窗口/聚合/其他集合lowering、统一资源治理及全部原始功能要求继续开放。
原273项仍22 complete/166 partial/70 unverified/15 deferred；原始项与725受保护记录保持，整体Goal active。

### 性能方法与完整结果

三个预算64kB/4MB/16MB、每例每label12样本与3次预热，全部串行old/new/old。六个预定窗口完成1620计时样本（VALUES432、既有静态集合1188）、405预热与36次SET/SHOW，共2061次SQL校验，无删除/重测或样本调整。
前版95defdc5…/新版58c94e05…、canonical脚本39b1450e…/VALUES脚本59c59442…、原1072文件夹具均不变。

四个新对照只用前后版本共同支持的VALUES表达式计数：1000行、1536字符payload尾、精确大于2^53的整数或C字节文本键、LIMIT20/OFFSET500，SQL约1.6MB；独立Python oracle校验全部返回单元。
既有静态集合沿用原canonical脚本和1000行fixture，未把旧版不支持的global-set表达式SQL算作速度收益。
两driver均计入请求/解析/网络及完整值比较，raw/hash/JSON写入在单样本计时外。VALUES预先准备SQL/oracle；canonical的execute_case在计时内调用SQL/expected生成器，分别解释各自前后对照。
构建、正确性测试和资源探针都已终止后才开始计时；共享宿主的其他负载没有由此排除。每label12样本，不作置信区间或普遍无回退声明。

下表为全部45例的中位数，单位ms；最后一列依次是“新版对旧首/新版对旧回测”的百分比，正数表示较慢。全部min/max、漂移和每样本完整值见 `runtime-performance-summary.csv/.json` 及其六个原始comparison路径。

| 预算 | 路径 | 旧首 | 新版 | 旧回测 | 新对旧首/旧回测 |
| --- | --- | ---: | ---: | ---: | ---: |
| 64kB | `VALUES integer_limit20` | 2326.700 | 2440.648 | 2458.232 | +4.9% / -0.7% |
| 64kB | `VALUES integer_offset500` | 2338.878 | 2415.781 | 2451.795 | +3.3% / -1.5% |
| 64kB | `VALUES text_limit20` | 2490.506 | 2507.286 | 2529.758 | +0.7% / -0.9% |
| 64kB | `VALUES text_offset500` | 2537.030 | 2569.724 | 2612.343 | +1.3% / -1.6% |
| 64kB | `union_text_full` | 291.040 | 272.397 | 274.903 | -6.4% / -0.9% |
| 64kB | `union_numeric_full` | 408.769 | 396.188 | 424.088 | -3.1% / -6.6% |
| 64kB | `union_float_full` | 485.349 | 478.176 | 473.033 | -1.5% / +1.1% |
| 64kB | `union_real_full` | 384.783 | 370.679 | 374.494 | -3.7% / -1.0% |
| 64kB | `union_integer_full` | 193.483 | 196.648 | 190.306 | +1.6% / +3.3% |
| 64kB | `union_stable_peers` | 199.489 | 195.239 | 180.443 | -2.1% / +8.2% |
| 64kB | `union_limit` | 149.324 | 143.600 | 144.436 | -3.8% / -0.6% |
| 64kB | `union_offset` | 158.530 | 153.155 | 151.618 | -3.4% / +1.0% |
| 64kB | `union_ties` | 150.937 | 149.658 | 164.121 | -0.8% / -8.8% |
| 64kB | `integer_topn_control` | 37.769 | 35.013 | 35.159 | -7.3% / -0.4% |
| 64kB | `pk_lookup_control` | 12.986 | 13.566 | 12.131 | +4.5% / +11.8% |
| 4MB | `VALUES integer_limit20` | 2339.081 | 2348.506 | 2288.715 | +0.4% / +2.6% |
| 4MB | `VALUES integer_offset500` | 2329.020 | 2287.373 | 2320.789 | -1.8% / -1.4% |
| 4MB | `VALUES text_limit20` | 2332.329 | 2334.331 | 2346.631 | +0.1% / -0.5% |
| 4MB | `VALUES text_offset500` | 2344.556 | 2379.820 | 2328.149 | +1.5% / +2.2% |
| 4MB | `union_text_full` | 236.213 | 254.126 | 343.138 | +7.6% / -25.9% |
| 4MB | `union_numeric_full` | 370.573 | 386.552 | 424.901 | +4.3% / -9.0% |
| 4MB | `union_float_full` | 417.952 | 421.002 | 473.911 | +0.7% / -11.2% |
| 4MB | `union_real_full` | 351.501 | 370.111 | 391.872 | +5.3% / -5.6% |
| 4MB | `union_integer_full` | 184.642 | 204.147 | 197.171 | +10.6% / +3.5% |
| 4MB | `union_stable_peers` | 186.323 | 202.277 | 204.564 | +8.6% / -1.1% |
| 4MB | `union_limit` | 144.879 | 173.353 | 170.487 | +19.7% / +1.7% |
| 4MB | `union_offset` | 145.566 | 156.832 | 156.697 | +7.7% / +0.1% |
| 4MB | `union_ties` | 145.595 | 173.395 | 172.651 | +19.1% / +0.4% |
| 4MB | `integer_topn_control` | 36.817 | 40.839 | 40.206 | +10.9% / +1.6% |
| 4MB | `pk_lookup_control` | 13.729 | 18.029 | 17.819 | +31.3% / +1.2% |
| 16MB | `VALUES integer_limit20` | 2327.953 | 2456.218 | 2311.315 | +5.5% / +6.3% |
| 16MB | `VALUES integer_offset500` | 2280.406 | 2303.361 | 2311.418 | +1.0% / -0.3% |
| 16MB | `VALUES text_limit20` | 2319.640 | 2332.816 | 2335.783 | +0.6% / -0.1% |
| 16MB | `VALUES text_offset500` | 2343.659 | 2317.537 | 2406.004 | -1.1% / -3.7% |
| 16MB | `union_text_full` | 246.626 | 248.347 | 247.733 | +0.7% / +0.2% |
| 16MB | `union_numeric_full` | 382.807 | 410.088 | 373.104 | +7.1% / +9.9% |
| 16MB | `union_float_full` | 433.061 | 762.679 | 425.205 | +76.1% / +79.4% |
| 16MB | `union_real_full` | 360.739 | 361.523 | 368.218 | +0.2% / -1.8% |
| 16MB | `union_integer_full` | 206.268 | 192.747 | 192.664 | -6.6% / +0.0% |
| 16MB | `union_stable_peers` | 204.092 | 211.344 | 193.783 | +3.6% / +9.1% |
| 16MB | `union_limit` | 145.802 | 166.316 | 149.940 | +14.1% / +10.9% |
| 16MB | `union_offset` | 148.587 | 149.714 | 148.416 | +0.8% / +0.9% |
| 16MB | `union_ties` | 169.116 | 150.759 | 148.876 | -10.9% / +1.3% |
| 16MB | `integer_topn_control` | 35.913 | 37.896 | 36.363 | +5.5% / +4.2% |
| 16MB | `pk_lookup_control` | 11.179 | 12.442 | 10.911 | +11.3% / +14.0% |

整条VALUES SQL未证明普遍提速；16MB整数LIMIT20为2327.953/2456.218/2311.315ms，新值比两旧值慢约5.5%/6.3%，继续开放。
4MB静态旧首/旧回漂移明显，例如text236.213→343.138ms，不能把单一前后倍率直接归因于代码。
16MB静态float8为433.061/762.679/425.205ms，新12个原样本的后6个约1034–1783ms，前6个约415–492ms，整个慢簇保留，没有剔除或另测一组替代。
原生资源证明动态小K可不落盘，未据此推导端到端速度；这些慢值与其他瓶颈仍需后续独立归因和优化。

### 已绑定执行阶段的独立对照

在上述六窗口全部结束后，再串行编译/运行一个仅用既有public API的同源原生驱动，旧头/旧61core/旧stub与新头/新61core/新stub分别匹配，没有私有算子克隆、SQL重写或参数帧替代。
固定四条SQL/独立Python oracle与wholeSQL的specifications对象逐项相等；同样1000行/1536字符尾、三预算、每例3预热/12样本、old/new/old，四例按固定洗牌三轮执行。
初始prepareBoundQuery（解析+绑定合并）和buildPreparedQueryPlan/makePreparedCursor放计时外并单独保留36条preparation记录；计时含真实同图restart(RowContext{})→consume→close→完整owning值/metadata及图身份校验，evidence写入在计时外。
原生432计时+108预热共540调用、10800行/32400单元校验全过，9进程exit0、原夹具与源码/core/header/stub输入均稳定。
它测public cursor执行的墙钟时间；每次重启仍包含真实执行owner/表达式准备，不能当成排序CPU、wire prepared/保留计划性能或R303修复，也不能和wholeSQL直接相减归因。

| 预算 | 路径 | 旧首ms | 新版ms | 旧回测ms |
| --- | --- | ---: | ---: | ---: |
| 64kB | `integer_limit20` | 601.459 | 575.879 | 588.664 |
| 64kB | `integer_offset500` | 599.521 | 576.055 | 582.680 |
| 64kB | `text_limit20` | 678.158 | 648.592 | 655.284 |
| 64kB | `text_offset500` | 679.743 | 658.013 | 681.877 |
| 4MB | `integer_limit20` | 572.658 | 570.643 | 572.208 |
| 4MB | `integer_offset500` | 586.738 | 583.943 | 576.529 |
| 4MB | `text_limit20` | 627.241 | 597.790 | 609.378 |
| 4MB | `text_offset500` | 623.136 | 627.820 | 614.868 |
| 16MB | `integer_limit20` | 566.255 | 561.893 | 574.040 |
| 16MB | `integer_offset500` | 566.110 | 569.077 | 583.433 |
| 16MB | `text_limit20` | 613.485 | 587.727 | 621.266 |
| 16MB | `text_offset500` | 610.354 | 611.377 | 619.679 |

这组也未证明普遍/显著提速：4MB textOFFSET为623.136/627.820/614.868ms，新值比两个旧值均慢；若干LIMIT有小幅改善，基线也波动。
初始合并解析+绑定206.307–242.332ms、建图/初始准备453.918–524.358ms另记；没有细分parser/analyzer CPU或断言某一阶段解释了全部开销。
后续还需要优化并归因执行准备、非整数比较与整条SQL开销。原wholeSQL六窗的慢值仍独立保留，未用这组替代它们。
完整每次值/CSV/图身份、初始准备、旧新编译输入见 `bound-values-results/execution-summary.json` 与 `prepared-values-cursor-plan.json` 的冻结方法和命令。


复现：以本目录 `runtime-performance-plan.json` 和 `runtime-performance-windows-receipt.json` 的六条冻结命令/输出为准；
例如 `python3 scripts/benchmark_set_sort.py --before build/pg-feature-global-count-xqbytfo5/before-dbms_main --after build/pg-feature-global-count-xqbytfo5/candidate2-dbms_main --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 --output build/pg-feature-global-count-reproduce --work-mem 64kB --rows 1000 --rounds 3 --samples 4`。
输出目录必须是新的私有目录。VALUES driver、固定SQL和独立oracle同样留在本批build证据目录；原始输入/前后旧版本、raw慢值和方法缺一不可，当前代码重建不能冒充旧二进制。

最终文档检查的初次脚本误继承前批scope-audit命令，覆盖了本地前批的一份 `scope-audit.json`；该旧JSON已不匹配原证据索引的哈希。
误运行报告、原预期哈希和完整性说明保存在本批 `prior-audit-overwrite-incident.json`，旧通过日志/原证据索引仍保留；没有改写原哈希来假装恢复。
源码、已提交文档、原生/协议原始日志、性能raw与二进制未被该误运行修改。本批范围检查改为独立对照baba4bcb与更早未改的保护索引；不再用被覆盖的旧JSON证明本批通过。
另一次本地链接检查器未识别旧文档合法的`:行号`文件链接，已仅修检查器；受保护旧文档未动。最初检查结果保留。


## 类型化窗口宿主与有序输出复用（2026-11-03）

### 范围、研究与实现

本批输入 `9f019ffa8367a472bff9801c5fd6e43dfdcacc05`，直接在 master 开发；前版程序 SHA256 `58c94e05ce38e1744e5ec792de97a73d960c8ec284834f764ebb78d5374951ab`，
最终 C9 程序 `c696d3b224748ee010d1f8bb9994494430730f2ffaeb9a5581247f7142f2abeb`。
本批补入受支持查询形状的类型化窗口执行宿主，**没有实现普遍提速**：按同预算旧版回测，`full_first_last_nth` 在三个预算约快5.15/5.33/5.84倍；
其余7类查询的21组新版p50延迟均更高，为旧回测的约1.92–2.70倍。64kB旧首/旧回测漂移很大，完整表和原始慢值一并保留，不能只引用旧首声称加速。
这不是新的PostgreSQL速度对照，也没有把减少复制次数当作已证明的整库收益。

工件目录 `build/pg-feature-window-host-557k7fi8/` 保存旧源/完整62对象、C1..C9程序/输入、原生/协议原始失败、PG预期、资源和性能记录；本地build工件不纳入Git。
产品仍是0.2.0/C++17，GCC13.3/-O2、ICU/zlib及TLS stub，未通过切换语言标准宣称查询提速。
通过验证的C9已原子安装到根目录dbms_main，文件和父目录已fsync；根目录、CMake产物和冻结C9 SHA一致，原58备份保留，没有重启既有服务。

研究以 PostgreSQL 18.6 实际源码和独立协议观察为准：[窗口语义](https://www.postgresql.org/docs/18/functions-window.html)、
[planner](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/plan/planner.c)、
[setrefs](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/plan/setrefs.c)、
[parse_agg](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_agg.c)、
[WindowAgg](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeWindowAgg.c)、
[windowfuncs](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/windowfuncs.c)与
[timestamp interval除法](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/timestamp.c)。
据此实现真实输入键投影、窗口栈排序复用、逐行aggregate transition与按需结果；WindowClause的原始身份和语义排序键身份分别保存，避免把两个初始化不同的窗口错误合并。

另参考[SQLite窗口接口](https://www.sqlite.org/windowfunctions.html)的逆转移机制，以及DuckDB官方的
[窗口执行说明](https://www.duckdb.org/2021/10/13/windowing)、[窗口优化文章](https://duckdb.org/2025/02/14/window-flying)和
[窗口函数文档](https://duckdb.org/docs/2.0/sql/functions/window_functions)。
逆转移、完整segment tree/vectorized窗口及窗口spill仍是后续方向，本批没有把这些系统的实现或性能当成本项目已完成项。

实际管线为真实行源/过滤→既有普通聚合→拥有类型和值的窗口输入槽→一个或多个窗口阶段→必要时普通最终排序→投影/DISTINCT/OFFSET/计数。
完整分区/排序表达式只投影一次；SELECT、最终ORDER、窗口参数/FILTER/default内相同深层键引用真实槽，窗口键自身的生产者不任意做子表达式消除。
命名继承、真实作用域/sourceOccurrence/ordinal、声明比较类型/枚举OID和NULL/方向均保留。
窗口内核支持已验证的ROWS/GROUPS/数值RANGE、peer/exclusion、排名、偏移/取值、分桶及有限聚合；无键阶段保持已有顺序；相同语义键的阶段复用已有排序，最终ORDER的已证明兼容前缀可省去冗余Sort。不同前缀stage的组内incremental sort尚未实现。
运行前缀聚合复用累计状态；移动帧头/exclusion按需重算，尚无一般逆转移或segment tree。

C9的私有窗口read把真实上游拥有的current context直接用于同步的原execution.context复制，不再先经过lastPreparedContext产生一个临时完整副本。
返回的输入context仍独立拥有数据，原execution.context方法/调用次数与参数frame/provider不改，不保存任何跨调用借用；已有current_/currentKeys_输出快照、close/restart均保持。
输入键存储由每行K+P+O个datum降为K个，分区/排序/RANGE通过原索引读取唯一keys；保留原read位置的防御性边界检查。这里是复制次数/数据结构变化，不是实测RSS或整库内存倍率。

只有真实无条件投影窗口保证排序存在、且所有随后可能排序阶段都保持最终ORDER前缀时，才去掉冗余最终排序。
LIMIT 1 的兼容前缀样例窗口参数求值从6次降至PG要求的1次；不兼容/条件窗口负控制仍需6次。
这是求值需求证明，不是整库6倍速度声明；有序WITH TIES使用真实窗口保存键和投影前瞻，OFFSET、零LIMIT及错误前行均逐项核对。
Main、Core与真实SQL子查询共享类型化宿主；Main真实SourceContexts JOIN已验证，通用Core JOIN lowering仍报不支持。
Describe只用真实绑定/目录/来源推导，不打开游标或执行SQL；物理来源使用实际表OID/attnum，view使用自己的OID，表达式/合并USING等保持无来源。

### 正确性、保留失败与资源

最终61core/Main/新stub全部与头文件匹配，C9 link冻结src下216文件无漂移，生产对象为61core加Main共62个。主回归68原生全过，加两个独立入口共70个不同原生入口全过。
六个新增入口分别223/37/3/39/30/16，共348控制全过；测试实现真实公共游标、同图重启、原始参数来源、heap/FD/NULL和异常清理，不以私有替身代替产品执行。
新协议窗口宿主226、输入键241、有序输出201有独立PG18.6严格预期；类型/修饰/格式/来源/行值/ParameterDescription/错误阶段都参与比较。
固定目录身份只凭实际物理LIMIT0/pg_type探针归一，不把本项目OID伪装成PG数值，也不改已有测试的固定预期。

interval datum除法修正月/日小数向下级联、TSROUND/rint与边界检查，有限窗口AVG随之正确；原乘法及其他求值器逻辑保持字节一致。
严格interval106保留10个常量错误的Bind/Execute阶段差异；值、类型、SQLSTATE、20个AVG控制均符合。它们属于既有R303等暂缓范围，未过滤、改预期或恢复该模块。
最终同一C9的60个不同协议入口为56通过/4失败；主58协议为55通过/3失败，另键241通过、interval106/10保留失败。原Bind/portal完整463/40日志与前版字节相同，另外CTE/window-owner747/29及nested-window-projection325/42仍有严格差异。
两个旧版完整结果分别747/453与325/77，改进不等于全部通过；剩余问题不能一律归为暂缓参数阶段。
本批没有重新宣告前批完整910或全部数据库入口验收，新宿主消除了前批binding58中合法窗口子查询的0A000缺口。

C1..C9全部候选、失败与harness版本保留。中途原生JOIN使用错误计划入口、错误阶段前提、排序图遍历未展开真实LIMIT/OFFSET子节点、metadata入口数前提均分别纠正并留原始失败；
这些测试前提改动没有改既有2302个测试。实际窗口放置/继承、键复用、Frame初始化身份、ntile首行NULL、interval级联、清理与行需求问题由生产实现修复。

逐观察审计发现C7相对旧58新增14个回退，不能因总失败数下降就称无回退：8个CTE串联Simple查询错误、6个LIMIT/OFFSET副作用计数过量。
C8在真实whole-read图准入处保留CTE原生产者图，COALESCE builtin首参数必经证明去除冗余最终Sort；完整1072观察确认14项全部恢复，且无新增退化；最终C9逐观察另行复核。
两完整入口仍有71个差异：CTE29为3个真实非零OID数值、2个42P01错误阶段、24个缺RowDescription；其所有行值/SQLSTATE已符合PG，没有剩余XX000。
Nested42为18个OID数值、18个合法ANY/SOME/ALL仍被42883拒绝、3个重复volatile真实行值和3个对应少调用计数；它们不是71个独立bug，也不全属R303。
新增native16首轮15/1仅最后控制的序列值1→5，计划/返回行正确；它使用默认未启用root常量规划的低层Core图。
真实C8 Main同源PG14/0；仅新增15/16改为公开PreparedQueryExecution.planStatementConstants及真实executionProvider图后16/0，原14完整保留。
原未规划结果、诊断和源码不删除；没有更改通用Core默认常量规划策略或宣告未规划API与PG等价。

资源阶段不计时：共同8查询旧/新/旧共24次1,000行完整核对；普通最终TypedSort在64kB采到最多2个匿名FD，在16MB未采到FD，均验证1,000行/3,000单元，EOF无遗留FD/命名临时文件。
强杀验证捕获自有PID/数据目录/启动身份及实际匿名FD后执行，退出-9并断连；同目录重开核对基表1,000行/4,000单元及再次排序1,000行。
三阶段共43请求（28个完成全值读取、1个强杀中的读取、14个SET/SHOW），1072夹具文件不变，自有进程均结束。35类操作另以每类一次完成35个结果核对、0个操作错误，但服务器正常退出时抛std::system_error: Invalid argument并SIGABRT（-6），driver exit1/complete=false，该完整入口失败。
原log/operations.json及operations-candidate9-failed-shutdown-receipt.json保留；既有R15停机链用户暂缓保持，未恢复调查/改代码/重跑挑选通过样本，未从源码未改就推断因果无关。
窗口分区缓存和阶段内部排序仍无work_mem上限，只有保留的普通最终排序具有上述预算/落盘证明；没有声称硬RSS上限、所有窗口spill、任意写入崩溃恢复、并发或真实TLS通过。

### 性能方法与完整结果

性能使用固定1072文件、1,000行夹具，前版58/最终C9/前版58；构建、正确性和资源进程均结束后再计时。
共同支持的8条SQL在64kB/4MB/16MB顺序执行，每budget每label3轮×4样本及3次预热；最终完成864计时、216预热、18次SET/SHOW，共1098次校验；1080个查询结果合计1,080,000行/2,970,000单元完整相等。
计时包含整条SQL的解析、执行、网络和完整结果比较；逐样本原始数据与所有慢值/漂移保留，不将旧版拒绝的新SQL算作性能改善，也没有重新测PG性能。
共同夹具base payload是row-i（5–8字节），不是前批VALUES基准的1536字符尾。
八查询均有grp分区、最终ORDER id；当前保留普通TypedSort并使用会话预算，完整typed上下文含未投影源字段；旧WindowOp的最终排序在内存vector中，不使用该work_mem或落盘。
相同SET不代表资源策略等同，不能直接把全部慢值因果归为I/O。已有匿名FD实证来自另行repeat(payload,512)宽投影控制，未逐条采样common8的FD。

冻结driver为本批 `runtime_benchmark_windows.py`（SHA256 `f3d025e21e9ddfabff7cd9980faa27a295222b2d7b6d01178a5c2fbd8fb7450b`）；实际完成结果在下表及本批原始comparison文件中。

### C9 全部24项结果

每个p50来自12个样本，单位ms；新相对旧首/旧回测百分比的正值表示较慢。全部min/max、原序12个样本和9份CSV/JSONL见本批 `runtime-window-performance-candidate9-summary-v1.json/.csv` 及其raw_artifacts。
三个预算全部自然exit0、无重测或剔除；原夹具1072文件/驱动/二进制前后哈希不变，自有进程均退出。
内部构建、测试和资源任务已停止后才计时，但共享宿主其他负载未被排除；每label12样本不支持置信区间、全负载或无回退结论。

| 预算 | 查询 | 旧首p50 ms | C9 p50 ms | 旧回测p50 ms | 新对旧首 | 新对旧回测 | 旧漂移 |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 349.26 | 227.63 | 95.78 | -34.8% | +137.7% | -72.6% |
| 64kB | partition_lag | 710.17 | 195.43 | 88.12 | -72.5% | +121.8% | -87.6% |
| 64kB | partition_lead | 554.23 | 199.50 | 76.65 | -64.0% | +160.3% | -86.2% |
| 64kB | running_sum | 377.79 | 212.39 | 78.52 | -43.8% | +170.5% | -79.2% |
| 64kB | sliding_sum | 487.96 | 186.21 | 87.15 | -61.8% | +113.7% | -82.1% |
| 64kB | partition_full_sum | 407.95 | 188.53 | 90.89 | -53.8% | +107.4% | -77.7% |
| 64kB | full_first_last_nth | 1869.42 | 231.01 | 1190.03 | -87.6% | -80.6% | -36.3% |
| 64kB | mixed_same_keys | 384.88 | 240.49 | 122.94 | -37.5% | +95.6% | -68.1% |
| 4MB | partition_ranking | 98.25 | 190.63 | 90.83 | +94.0% | +109.9% | -7.6% |
| 4MB | partition_lag | 81.33 | 164.60 | 75.27 | +102.4% | +118.7% | -7.5% |
| 4MB | partition_lead | 77.76 | 165.18 | 68.58 | +112.4% | +140.9% | -11.8% |
| 4MB | running_sum | 78.71 | 164.75 | 72.62 | +109.3% | +126.9% | -7.7% |
| 4MB | sliding_sum | 87.40 | 165.81 | 77.93 | +89.7% | +112.8% | -10.8% |
| 4MB | partition_full_sum | 85.54 | 146.53 | 72.97 | +71.3% | +100.8% | -14.7% |
| 4MB | full_first_last_nth | 1136.89 | 213.54 | 1139.02 | -81.2% | -81.3% | +0.2% |
| 4MB | mixed_same_keys | 100.39 | 215.90 | 98.35 | +115.1% | +119.5% | -2.0% |
| 16MB | partition_ranking | 96.80 | 181.23 | 93.93 | +87.2% | +92.9% | -3.0% |
| 16MB | partition_lag | 75.68 | 160.17 | 83.49 | +111.7% | +91.8% | +10.3% |
| 16MB | partition_lead | 79.49 | 160.13 | 69.43 | +101.5% | +130.6% | -12.6% |
| 16MB | running_sum | 81.68 | 160.87 | 71.96 | +97.0% | +123.5% | -11.9% |
| 16MB | sliding_sum | 76.07 | 165.41 | 77.34 | +117.4% | +113.9% | +1.7% |
| 16MB | partition_full_sum | 80.94 | 144.00 | 73.58 | +77.9% | +95.7% | -9.1% |
| 16MB | full_first_last_nth | 1148.02 | 195.17 | 1139.79 | -83.0% | -82.9% | -0.7% |
| 16MB | mixed_same_keys | 100.16 | 204.05 | 97.18 | +103.7% | +110.0% | -3.0% |

4MB/16MB的7类查询比前后两个旧版都慢，未关闭这些慢项。64kB旧首/旧回测下降36.3%–87.6%，不能用对旧首的负百分比掩盖对旧回测的回退。
first/last/nth组合相对旧回测的5.15/5.33/5.84倍收益只适用于本1,000行、100分区、热缓存、单连接夹具；不是PostgreSQL对照或全部窗口收益。
这些结果覆盖功能/执行/资源策略的整批差异，没有用跨候选C7→C9未配对数据估算复制优化的独立倍率。

### C7 已完成性能窗口（保留历史）

C7在发现14个新增正确性回退前，已完成64kB旧/新/旧的288计时、72预热和6次SET/SHOW，共366次全值校验；该完整窗口未删除或被C9替代。
发现回退后当前budget自然完成，后续4MB在创建输出目录时受控拒绝、未启动任何server/SQL/样本，16MB未启动；控制性exit1不记为产品性能失败。
下表保留全部8例的p50/min/max和旧版漂移；C7是失败候选历史，不能作为最终C9的测量结果。原CSV/JSONL、启动闸门和控制收据均留在本批工件中。

| 查询 | 旧版 p50 [min,max] ms | C7 p50 [min,max] ms | 旧版回测 p50 [min,max] ms | 旧版漂移 |
|---|---:|---:|---:|---:|
| partition_ranking | 159.52 [103.35, 417.16] | 205.12 [184.83, 268.37] | 103.56 [92.55, 117.98] | -35.1% |
| partition_lag | 151.29 [103.71, 217.30] | 227.08 [160.14, 451.93] | 85.10 [75.09, 214.51] | -43.7% |
| partition_lead | 125.09 [90.13, 210.61] | 177.94 [164.53, 192.14] | 86.34 [73.62, 101.88] | -31.0% |
| running_sum | 150.58 [81.51, 217.52] | 191.16 [178.94, 240.35] | 87.38 [77.53, 90.70] | -42.0% |
| sliding_sum | 97.09 [76.20, 247.98] | 197.18 [170.32, 303.56] | 90.47 [78.45, 93.80] | -6.8% |
| partition_full_sum | 170.25 [144.42, 370.49] | 209.13 [144.75, 712.67] | 93.20 [87.03, 102.17] | -45.3% |
| full_first_last_nth | 1216.49 [1093.03, 1265.98] | 257.77 [207.17, 299.29] | 1128.10 [1123.14, 1248.50] | -7.3% |
| mixed_same_keys | 138.15 [101.90, 200.23] | 229.35 [208.28, 364.00] | 106.71 [89.87, 186.39] | -22.8% |

旧首/旧回测漂移为-6.8%到-45.3%，原样保留；不能把单一前后比值直接归因于代码。C9另用独立新目录完整执行冻结的三预算计划。

### 开放项、范围与复现

性能下一步优先诊断窗口键比较中的typed datum复制/重复数值解析、上下文准备及最终TypedSort成本；可评估仅Window-owned的精确内置整数键缓存，保留enum/OID/domain/NaN/NULL等真实回退。
静态热点尚不证明其解释了全部慢值；不改共享Set/R185助手或用legacy接收器绕过类型/资源正确性。当前7类慢项未关闭。

完整窗口内存治理、统计/更多窗口聚合、temporal RANGE的in_range族、通用Core JOIN lowering、其他普通SELECT动态计数、全部类型/嵌套边界和统一执行预算仍开放。
WITH TIES的枚举/NaN/record/array peer全组合及OOM/double-close故障矩阵未全部覆盖；本批有限核验不能关闭这些要求。
原273项仍22 complete/166 partial/70 unverified/15 deferred；只追加近期partial进展，原完整对象/条款/725受保护记录不变，整体Goal active。
受保护共享排序/中断helper、PreparedAggregateOp/prepareAggregateRoles、legacy WindowOp、PreparedOperatorCursor及真实参数帧/provider/portal等职责保持。
最终只读源码审计33/33通过，既有2302测试不变；原144公共头仅AST/执行注册接口两处授权改动，所有受影响消费方和Main重新编译。

当前复现使用本批 `runtime-candidate9-resources-v1-receipt.json` 的三个实际阶段命令，以及 `runtime-window-latency-candidate9-64kB-v1-receipt.json`、`runtime-window-latency-candidate9-4MB-v1-receipt.json`、`runtime-window-latency-candidate9-16MB-v1-receipt.json` 的最终冻结计时命令，在新的私有输出目录执行。C7 observation/validate计划仅作历史，不能冒充C9命令。
原生入口经 `run_native.py` 与C9冻结core配置，协议入口使用已保存的真实PG18.6预期及对应二进制；不能用当前重建冒充旧58程序。
例如在新的私有输出目录复现64kB（其余预算按各自receipt命令）：

```bash
python3 build/pg-feature-window-host-557k7fi8/runtime_benchmark_windows.py --before build/pg-feature-window-host-557k7fi8/before-dbms_main --after build/pg-feature-window-host-557k7fi8/candidate9-dbms_main --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 --output build/window-host-reproduce-64kB --work-mem 64kB --rows 1000 --rounds 3 --samples 4
```

原始raw与复现输出目录不能覆盖；研究和功能对照、资源、计时三个阶段分别解释。

输入、完整资源记录、每入口日志、源码/core/header/stub收据和最终文档检查统一列入本批 `final-provenance.json`；上一批已披露旧scope-audit误覆盖事件不改写或伪造恢复。


## 窗口整数键缓存与实际执行成本（2026-11-03）

### 实现与研究依据

本批基线 `b04db8010bc478a84dd767d6c6cac08da4bfcd42`、前版C9 SHA256 `c696d3b224748ee010d1f8bb9994494430730f2ffaeb9a5581247f7142f2abeb`；
候选C1 SHA256 `56d06bca0ddacd1d7c801b51d360d1d393765717f8d1aeda5b31b7f0196c6a19`。工件目录 `build/pg-feature-window-key-w4qeo1ai/`。
根目录 `dbms_main` 已以同一C1原子替换并对文件/父目录fsync，C9副本保留；没有重启既有服务，安装收据为 `candidate1-root-install-v1.json`。
本批确有执行收益，但有资源代价与未决慢值：4MB/16MB的16个整SQL对照p50降低约22.3%–39.1%，64kB仍有3项慢于两次C9。
1000行整数控制的allocator uordblks在首/末行多约68KB，close后有保留容量；不是内存普遍降低或完整预算治理。全部快慢值均保留。

仍是0.2.0/C++17、GCC13.3/-O2、ICU/zlib、TLS stub；没有改语言标准或持久化保护。

[PG18.6 WindowAgg](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeWindowAgg.c)在初始化时准备分区/同行比较状态，
[execTuplesMatchPrepare](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/execGrouping.c)解析operator函数OID并构造后续复用的表达式；
[DuckDB排序设计](https://www.duckdb.org/2025/09/24/sorting-again)和[原始论文](https://duckdb.org/pdf/ICDE2023-kuiper-muehleisen-sorting.pdf)说明解释式比较的成本与预生成排序键的方向。
本项目仅借鉴把重复解释移出循环：缓存已求值builtin整数，不声称实现了PG compiled ExprState、完整排序键编码或向量化。

先用真实public cursor分阶段诊断，再实施唯一生产改动 `PreparedWindowOp.inc`。每次原非NULL比较会创建两个owning RuntimeCell并执行1–2次通用表达式比较；
新路径在真正到达的比较处、双侧raw类型相同且为smallint/integer/bigint、真实声明OID21/23/20、无enum绑定时，惰性解析int64一次并缓存到该行实际key槽。
NULL仍首先决定；比较用大小关系而非减法，不引入整数溢出或浮点舍入。全串解析/位宽检查未命中时交回原Set比较owner，不新增SQL拒绝。
真实range OID（包括0）优先于stale binding或投影声明；未绑定Column不以投影救回，domain/enum/unknown0/没有导出权威OID的隐藏表达式保留原路。
不重跑infer/search_path/catalog，不重新执行键表达式，也不缓存或替换实际frame/provider/上下文；RANGE数学、原键值与当前输出快照不变。

这会增加常驻成本：每个buffered Input新增vector头及外层容量，实际整数payload和小型OID元数据按比较需求分配，domain/OID0拒绝前也可能分配小metadata vector。
close释放行payload及metadata，但原input_外层容量可保留到图销毁；purefallback也有新增行header，不能称其零内存成本。
窗口分区、内部排序和新增缓存仍无work_mem/spill；普通最终TypedSort的预算与完整context codec完全保持。

### 完整正确性与范围证据

C1单TU编译/链接exit0，冻结src216文件无drift，61core仅Exec对象变化；147头、Main、ExecCPP整文件（含sharedSet/R185/R177）及实际frame/context/provider/codec均与基线字节一致。
原70native全过，加新native36入口全过，合计71个不同入口。新36在C9仅3个成本断言失败，其余33语义控制全过；原源和所有日志保留。
原60wire仍56pass/4fail，Bind463/40、CTE747/29、nested325/42整log逐字节相同，interval106/10完整观察JSON相同（log仅输出路径变化）。
新增固定143在独立PG18.6全0；C9和C1都14处差异且label/SQL/expected/fullactual/raw消息/错误全部相等，0新增回退。
14为catalog来源2、rank secondary record-json错误前缀3、重复volatilewindow调用共享6、compound二进制结果3；不改PG预期、不删用例，也不笼统归入暂停。
合并为61个不同协议入口56pass/5fail，未把controls/重复执行数当独立入口或独立bug数量。

原2313测试、273完整对象/raw数组、248历史近期记录及725保护文件保持，pre-doc范围29/29通过；本批只追加partial，整个Goal active。
原C9的35类操作数据已核对但正常停机-6失败仍保留；本批不重跑或调查R15，不用只读资源进程正常退出宣称它已解决。

### 独立public游标阶段诊断

8条SQL与前批f3d025e驱动specifications AST逐字语义一致，不改SQL；原1072文件fixture只在私有副本上读取。
before/C1分别用同一原CPP编译两个程序：默认allocator的clocks和覆盖完整new家族的allocations，另独立evidence pass保存所有owning类型/NULL/OID/record字段和公开上下文。
每代48进程/144次结果核对（32warmup、48clock observations、48allocation observations、16evidence），各144000行/396000单元全部正确；无重测/源或fixture漂移。
16组evidence逐行/descriptor/typeOID/来源/公开context/实际图rowsloops相同，只排除跨进程指针数值和运行时间。真实图同址重启由public API核对；8条无参数，不将这份诊断冒称三origin全覆盖。

prepareBoundQuery、PQE构造/根常量规划、build实际计划/cursor、同图restart、first-next、remaining/EOF/close及childcleanup分别记录。
first-next包含真实源、键投影、必要排序、窗口和首结果验证；每node runtimeMs是含child的wall，不能加总或相减后硬称exclusive comparator CPU。
CLOCK_PROCESS_CPUTIME_ID与THREAD分别保留，allocation版elapsed不混clocks。已有背景数据库服务保持运行，/proc采样记录实际背景CPU/IO；不是全机独占测试。
初始绑定单观测约1.27–3.58ms；建图每case只有一次，前版11.73–37.61ms/C1 11.58–38.17ms，不能当三样本分布或被执行倍率掩盖。

下表每格为before/C1，执行观测p50各3个样本；完整min/max、所有raw/warmup/setup/128阶段表和inclusive节点在 `profile-window-public-api-before-C1-comparison-v1.json`、`profile-window-public-api-before-C1-phases128-v1.csv`。

| budget kB | case | wall before/C1 ms | process CPU before/C1 ms | new requests before/C1 | requested bytes before/C1 |
|---|---|---:|---:|---:|---:|
| 64 | partition_ranking | 114.459/55.861 | 113.007/54.972 | 378415/167235 | 27084041/22692189 |
| 64 | partition_lag | 97.826/42.938 | 97.826/42.938 | 349224/138044 | 20374226/15982374 |
| 64 | partition_lead | 88.733/43.067 | 89.297/43.067 | 349224/138044 | 20375242/15983390 |
| 64 | running_sum | 92.462/42.394 | 92.443/42.369 | 348731/137551 | 20470639/16078787 |
| 64 | sliding_sum | 96.009/42.892 | 96.702/42.892 | 351347/140167 | 20773842/16382130 |
| 64 | partition_full_sum | 78.560/41.134 | 78.560/41.134 | 293690/132411 | 18822687/15506147 |
| 64 | full_first_last_nth | 105.651/62.184 | 103.804/60.360 | 391907/180727 | 27480529/23088677 |
| 64 | mixed_same_keys | 142.681/61.721 | 141.507/59.918 | 427214/187589 | 29928150/24860382 |
| 16384 | partition_ranking | 89.697/37.685 | 89.843/37.685 | 344717/133537 | 19391291/14999439 |
| 16384 | partition_lag | 87.763/33.516 | 87.763/33.516 | 330670/119490 | 16443052/12051200 |
| 16384 | partition_lead | 87.185/35.018 | 87.345/34.941 | 330670/119490 | 16444068/12052216 |
| 16384 | running_sum | 88.229/33.614 | 88.229/33.614 | 330177/118997 | 16539465/12147613 |
| 16384 | sliding_sum | 85.023/35.539 | 85.023/35.539 | 332793/121613 | 16842710/12450998 |
| 16384 | partition_full_sum | 71.316/29.808 | 71.595/29.808 | 275136/113857 | 14891513/11574973 |
| 16384 | full_first_last_nth | 89.818/45.167 | 90.332/45.167 | 358209/147029 | 19787779/15395927 |
| 16384 | mixed_same_keys | 102.681/44.807 | 102.565/44.807 | 393516/153891 | 22235400/17167632 |


16组执行wall p50下降41.14%–61.90%、processCPU下降41.85%–61.90%，C++new请求下降53.89%–63.96%、请求字节下降15.98%–26.71%。
allocation程序完整scope（含warm/初建图/公开观察）累计请求23,693,613→10,349,519，bytes1,454,924,054→1,177,938,750；两侧outside-phase观察各1236请求/40346bytes，仍计入whole，没有隐藏暂停计数。
新native36的另一warm实际cursor成本控制为450603→73176请求、19260574→10314925bytes，阈值只是已验证的至少N请求节省；warm已分配的outercapacity不含在该执行差量中。
以上仅C++new主线程请求，未计malloc家族/C库、不是live/truepeak/RSS。不能用诊断的执行倍率直接代表整条SQL，或以跨前批old58倍率推导本次代码独立收益。

### 额外allocator快照控制

额外三条新SQL（兼容最终顺序的integer分区、puretextfallback、单行无窗口键比较）与common8分开，在真实public图、16MB、C9/C1各独立3进程上采样。
mallinfo2的uordblks/hblkhd和/proc VmRSS只作进程allocator/近似resident快照；保留C/C++库、tcache、arena碎片、代码页、采样时点与observer限制，不作truepeak或精确live C++断言。
不注入allocator头、不调tcache/mallopt/trim、不访问私有cache。18进程/18查询全部自然exit0，12006行/24012单元及162快照核对通过，源码/core/两程序/1072fixture哈希不变。
整数控制首次/末次取行的uordblks中位数C9 4,145,824B/C1 4,213,984B（+68,160B）；文本fallback +22,544B，单行 +48B。
EOF/显式close后差量仍为+20,544/+22,560/+48B，符合行payload被销毁而外层Input容量可保留的代码生命周期；不是精确cache对象live分解或泄漏阈值。
所有hblkhd样本266240B并单列；同期VmRSS中位差为−60/−92/+144kB，方向与uordblks不一致，不能把RSS全归cache、声称truepeak下降或认为destroy后必须归零。
完整3rep各点allraw/min/max/median在 `window-memory-snapshots-v3-summary54.csv` 与 `window-memory-snapshots-v3-summary.json`；27行双代表在 `window-memory-snapshots-v3-median27.txt`。
首个观察器私有类型Snapshot与dbms::Snapshot重名导致编译失败，未启动数据库；仅另建v2源将其改名ProcessMemorySnapshot，反向后原字节完全一致，原失败source/log/receipt保留。这是观察器修订，不是产品修复。

### 实际Main资源与整SQL性能

资源三阶段共43请求（28个完成fullreads、1个故意强杀read、14SET/SHOW），28000行/79000单元通过；64kB真实ordinary finalSort采到最多2匿名FD，16MB未采到，两者EOF无FD/命名文件残留。
在真实owned deleted FD上核exe/data/starttick后STOP/KILL，预期退出-9；同私有目录重开基表1000行/4000单元及再次1000行排序通过。
1072fixture与两程序/三个driver哈希不变，自有进程终态。只证明普通最终Sort，不能据此宣称Window/cache预算、硬RSS或任意写崩溃恢复完成。

整SQL最终计划固定C9/C1/C9，64kB→4MB→16MB，原common8与独立oracle、每label3warmup/12样本，计划864timed+216warmup+18SET-SHOW=1098请求。
该新对照两侧均为typed最终Sort资源策略，before不再使用old58；所有慢值/漂移/退出保留，任何失败不自动重跑。
最终完整三预算共864计时、216预热、18SET/SHOW=1098 SQL全部完成，全值1,080,000行/2,970,000单元相等；无失败、删样本、超时调整或重测，9个正常server contexts全部0，自有进程终态。
4MB/16MB的16项p50都低于两次相邻C9约22.3%–39.1%；64kB只有2项比两旧都快，3项比两旧都慢（partition_lag/running_sum/partition_full_sum），其余混合。
64kB基线首/回测漂移+13.2%到+75.8%，不能只挑某一基线或把慢值全归因宿主；该三项低预算慢值仍开放，没有统一加速结论。
原C9对old58的7类慢项是历史事实；本次未重测同轮old58，不用跨历史倍率推断它们已全部消除或更新PG速度差距。
共享宿主既有服务继续运行，内部测试/构建/资源全部终态后才计时；不是全机CPU隔离、并发吞吐或cold-cache验收。

下表为全部24项，单位ms，每label12样本；正百分比表示C1较慢。完整min/max/全部raw12样本、漂移及9份返回行JSONL的摘要在 `sort-audit-performance-candidate1-summary-v1.json/.csv`，不只保存中位数。

| 预算 | 查询 | C9首p50 ms | C1 p50 ms | C9回测p50 ms | C1对首% | C1对回测% | C9漂移% |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 224.051 | 224.390 | 309.619 | +0.2 | -27.5 | +38.2 |
| 64kB | partition_lag | 170.931 | 400.267 | 298.498 | +134.2 | +34.1 | +74.6 |
| 64kB | partition_lead | 177.401 | 140.872 | 206.573 | -20.6 | -31.8 | +16.4 |
| 64kB | running_sum | 172.756 | 434.772 | 283.620 | +151.7 | +53.3 | +64.2 |
| 64kB | sliding_sum | 177.061 | 178.258 | 220.254 | +0.7 | -19.1 | +24.4 |
| 64kB | partition_full_sum | 164.327 | 356.838 | 288.815 | +117.2 | +23.6 | +75.8 |
| 64kB | full_first_last_nth | 228.509 | 238.205 | 258.738 | +4.2 | -7.9 | +13.2 |
| 64kB | mixed_same_keys | 249.660 | 242.527 | 316.148 | -2.9 | -23.3 | +26.6 |
| 4MB | partition_ranking | 210.563 | 150.408 | 199.489 | -28.6 | -24.6 | -5.3 |
| 4MB | partition_lag | 180.954 | 118.696 | 169.690 | -34.4 | -30.1 | -6.2 |
| 4MB | partition_lead | 170.904 | 116.212 | 167.337 | -32.0 | -30.6 | -2.1 |
| 4MB | running_sum | 170.419 | 120.963 | 183.011 | -29.0 | -33.9 | +7.4 |
| 4MB | sliding_sum | 170.152 | 122.839 | 168.741 | -27.8 | -27.2 | -0.8 |
| 4MB | partition_full_sum | 148.926 | 108.010 | 143.067 | -27.5 | -24.5 | -3.9 |
| 4MB | full_first_last_nth | 226.684 | 157.405 | 205.585 | -30.6 | -23.4 | -9.3 |
| 4MB | mixed_same_keys | 239.275 | 163.375 | 210.252 | -31.7 | -22.3 | -12.1 |
| 16MB | partition_ranking | 186.171 | 140.304 | 189.022 | -24.6 | -25.8 | +1.5 |
| 16MB | partition_lag | 170.351 | 119.973 | 191.287 | -29.6 | -37.3 | +12.3 |
| 16MB | partition_lead | 179.062 | 118.965 | 174.545 | -33.6 | -31.8 | -2.5 |
| 16MB | running_sum | 191.407 | 116.495 | 185.632 | -39.1 | -37.2 | -3.0 |
| 16MB | sliding_sum | 171.317 | 118.596 | 194.087 | -30.8 | -38.9 | +13.3 |
| 16MB | partition_full_sum | 168.116 | 108.054 | 154.448 | -35.7 | -30.0 | -8.1 |
| 16MB | full_first_last_nth | 210.216 | 151.616 | 206.870 | -27.9 | -26.7 | -1.6 |
| 16MB | mixed_same_keys | 211.227 | 152.916 | 211.433 | -27.6 | -27.7 | +0.1 |



### 未完成范围与复现

隐藏表达式无权威OID、不同行/跨stage缓存复用、非整数类型的一般预备比较、Window完整内存治理与其他原273能力仍开放。
本批不改保留计划、默认未规划Core策略、portal、格式错误需求、R177/R185、域扩展或任何用户暂停边界。
最终命令使用本批 `candidate1-cmake-receipt.json`、publicAPI冻结plans/矩阵、allocator计划、`sort-audit-resource-candidate1-final-v1.json` 和各预算实际计时receipt，在新的私有输出目录运行；不能覆盖原结果或用新重建冒充前版C9。
例如复现64kB整SQL，使用新的私有输出目录（另两预算按各自原receipt）：

```bash
python3 build/pg-feature-window-key-w4qeo1ai/runtime_benchmark_windows.py --before build/pg-feature-window-key-w4qeo1ai/before-dbms_main --after build/pg-feature-window-key-w4qeo1ai/candidate1-dbms_main --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 --output build/window-integer-key-reproduce-64kB --work-mem 64kB --rows 1000 --rounds 3 --samples 4
```

研究、源/core/header/stub、完整原始数据/所有失败、文档检查与安装/提交证据统一在本批 `final-provenance.json`；原273状态不因缓存或样本改善关闭。


## 窗口结果共享与排序投影（2026-11-03）

### 输入、实现和研究依据

本批基线master `d7793044fc8abdbd5d4d7e28432f7cabf3ada32b`，C1程序SHA256 `56d06bca0ddacd1d7c801b51d360d1d393765717f8d1aeda5b31b7f0196c6a19`。
工件目录 `build/pg-feature-window-compat-xi39wepx/`；S1 `570863d34ffea3c5341e94d4ff6dd8e5ebd4853e7cd7cdc3d572d4cbe43f6808`，S2 `779617cb2afb9067fdf329c7fbd367c8fec53785734b37cd7ebc29721dddda8d`。
S1完整正确性通过后减少准备期重复元数据工作，得到S2；下方保留S2完整首次性能失败事实。随后S3 `aa52b5fe35e1d1ef92a301bcad8b72d95b32edb705e650d3539b999d8c5b6b0f` 继续验证，不将S2慢值抹去或提前宣告回退已修。
仍为0.2.0/C++17、GCC13.3/-O2、ICU/zlib、TLSstub，没有修改持久化格式或语言标准。

[PG18.6 WindowAgg](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeWindowAgg.c#L2684)复用等价且不含易变函数的窗口调用；
[make_sort_input_target](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/plan/planner.c#L6574)在合适的真实最终排序前拆出窗口值，
[add_new_columns_to_pathtarget](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/util/tlist.c#L761)可能让推迟投影中的相同窗口叶引用同一已计算列。
没有实际Sort时该投影会恢复；若两个易变目标都要供排序，它们也不因此合并。不能仅检查有ORDER BY或合并相同SQL文字。
隐藏排序子树还影响调用顺序：原155观察第74项的调用总数相同，但PG可见值是3,5,…，C1为2,4,…，必须保持虚拟输入目标顺序。

仅修改ExecCPP私有状态/接线及PreparedWindowOp.inc，147头、Main、网络/真实frame-context-provider、原共享比较/CPU排序助手与R177/R185及所有用户暂缓策略保持。
每个原call/计算列、targetOwners、实际需求及原窗口声明winref均保留；同一实际stage内选择真正被需求的代表，逐行计算并向原列广播完整owning ExprValue，最后别名move，其余必要copy。
不跨行/跨重启缓存结果或替换真实上下文。真实最终Sort的top demanded stage按完整保留根和抽出窗口叶的顺序处理，中间stage只采用非易变等价复用；完整wrapper不被误当作其子窗口已经是顶层值。
S2另外仅在本次prepare内复用同原Expr的纯身份字符串，以必要函数/窗口/输入结果签名跳过不可能的完整身份计算；无postponed目标时不建没人读取的root身份，但仍按原hidden/priorKey遍历。
没有重跑infer/fold、改写用户AST或改变保留规划/参数帧策略。通用已分析表达式的CAST/常量归一化仍未完成；额外6个PG合法正例24观察单独保留，未混入固定220或冒称候选已通过。

### 正确性、成本与保留失败

新固定220包括53条窗口查询及DDL/序列seed/post，独立PG18.6为220/0，C1为220/27，S1/S2均220/0；S1/S2完整case/fullactual/raw/normalized/match逐对象相同。
原155完整raw保持，唯一对象身份归一化由真实id LIMIT0锚定物理表OID，字段类型/格式/消息顺序/实际值/错误/计数均严格保留。S3新220也完整通过并与S2逐对象相同，其他最终门见文末。
原71native加新10入口共72全部通过；新10实际读取512行物理表，经公开绑定/真实图/cursor验证纯LAG/SUM、递归record及OID数组、重启/提前close、不同命名窗口、LIMIT0与22012。
独立审查后错误前缀加强为固定完整integer行[[1,0,0],[2,-1,-1]]、3列真实OID23和两次重启一致；a585bd7a源在C1/S1/S2/S3均10/0，较弱旧源和原日志保留，不改任何原2316测试文件。

S2旧61协议首次全跑55pass/6fail，其中新增1项在start_ours连接103、planned190/recorded0/attempted0，是0SQL采集失败，不是190项语义错误或验收通过。
首次1158B日志SHA `cc55476f92162b5dd7beb49e57439916caa3db5e64e1d45ea1052b84b4e2893d` 保留；全部任务终态后同源同S2独立一次完整190/0，含53项预期SQL错误，cleanup和两组序列基线健康，前后自有S2进程均无残留。
按各入口最终完成观察旧61为56pass/5fail，加新220后共62不同入口57pass/5fail；首轮合并new220的62入口56pass/6fail没有被改写。
Bind463/40、CTE747/29、nested325/42整log与C1/S1字节一致，interval106/10完整JSON一致（log只输出路径变化），原四1641观察不变。
原143恰42/43/85/86/128/129六项改善，14→8，另外137项fullactual/normalized/match相同；S2整143观察JSON与S1逐字节一致。
剩余catalog来源2、record-json同行错误需求3、compound普通format1二进制输出3仍开放，不笼统归入暂停。二进制完整真实字段OID/域枚举codec/元素结构方案保存在binary-design-v2-complete-metadata.json，尚无产品修复。

下表为同源512行warm公开cursor，计量包括restart→完整读取→显式close及owning接收行，初始绑定/建图在外。仅当前线程累计C++new请求，不是malloc全量、live/truepeak/RSS或整SQL速度。

| 512行warm公开cursor | C1 new请求 | S2 new请求 | C1请求字节 | S2请求字节 |
|---|---:|---:|---:|---:|
| lag-1 | 226489 | 226490 | 25625206 | 25625214 |
| lag-2 | 419723 | 232335 | 48048290 | 26317466 |
| lag-4 | 806565 | 244394 | 92906086 | 27713486 |
| sum-2 | 44542 | 43514 | 4621224 | 4432054 |

双/四LAG请求减少44.65%/69.70%，请求字节减少45.23%/70.17%；单LAG仍多1请求/8B，双SUM请求只少2.31%，不隐藏控制项。
S1/S2所有版本、构建与失败均保留。调查脚本首次PG路径契约断言在SQL前停止，仅新v2 resolve路径；原型v2隔离syntax因if-init/else同名found编译失败，v3只改aggregateFound；首次native runner在读取缺省配置时失败、未编译/执行，v2只优先显式--config。
这些是观察器/原型修订，不冒充产品运行故障修复。新增O(producer)元数据和必要owning别名拷贝，不宣称零成本或窗口work_mem/spill已完成。

### S2实际资源和首轮全部整SQL数据

S2资源55=40完整读取+1预期强杀在途读取+14SET/SHOW，40000行/118000单元严格核对通过，3stage各exit0，5个正常服务context成功。
common12三label全值与digest相等；64kB宽finalTypedSort最多2匿名deletedFD、16MB本次未采到，EOF无FD/命名残留（高预算未偷加硬no-spill断言）。
真实ownedPID/exe/data/starttick和deletedFD确认后SIGKILL -9，同私有目录重开1000×4基表及1000×3再次排序通过。216source/2exe/3driver/1072原fixture前后exact，自有进程全退出。
只证明普通最终Sort及该只读强杀场景，不能推广为Window内部预算、硬RSS或任意写崩溃恢复。原C9的35类操作值0错误但停机-6失败继续保留，本批不重复或调查R15。

最终时钟计划在看到候选速度前已冻结：原common8完整SQL/oracle不变，加length_lag_1/2/4与duplicate_running_sum，共12类；64kB→4MB→16MB，每预算C1/S2/C1、各3warm和12timed样本。
一次完成1296timed+324warm+18SET/SHOW=1638请求，结果查询1620000行/4725000单元全值相等，9正常contexts通过，源/二进制/driver/1072fixture不变，无删样本/超时调整/重复窗口。
全36项为10快于两旧、19慢于两旧、7夹在两旧；原common8的24项仅1快/17慢/6混合。S2不能宣称整体提速。
重复两份LAG(length)相对两旧减少37.95%–41.66%，四份减少64.46%–66.16%；单份及双SUM仍有混合/回退。
16MB lead的C1首307.820→回测115.244ms漂移−62.56%，S2 285.429ms；全部漂移保留，不只挑某一基线，也不归因宿主。
下表单位ms；“降低%”正值较快、负值较慢。完整min/max/每label12个raw、CSV与所有返回行JSONL均在perf-window-sharing-S2-latency-summary-v1.json及所引用工件，未只保存中位数。

| work_mem | case | C1 first p50 ms | S2 p50 ms | C1 last p50 ms | S2 reduction vs first % | S2 reduction vs last % | C1 last drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 173.532 | 177.773 | 182.156 | -2.44 | 2.41 | 4.97 |
| 64kB | partition_lag | 125.788 | 148.910 | 129.082 | -18.38 | -15.36 | 2.62 |
| 64kB | partition_lead | 126.005 | 132.281 | 114.355 | -4.98 | -15.68 | -9.25 |
| 64kB | running_sum | 143.138 | 129.086 | 145.252 | 9.82 | 11.13 | 1.48 |
| 64kB | sliding_sum | 140.393 | 143.582 | 127.438 | -2.27 | -12.67 | -9.23 |
| 64kB | partition_full_sum | 117.366 | 119.709 | 119.305 | -2.00 | -0.34 | 1.65 |
| 64kB | full_first_last_nth | 180.729 | 206.624 | 182.445 | -14.33 | -13.25 | 0.95 |
| 64kB | mixed_same_keys | 182.069 | 196.358 | 171.680 | -7.85 | -14.37 | -5.71 |
| 64kB | length_lag_1 | 469.501 | 459.604 | 478.152 | 2.11 | 3.88 | 1.84 |
| 64kB | length_lag_2 | 819.013 | 491.956 | 831.152 | 39.93 | 40.81 | 1.48 |
| 64kB | length_lag_4 | 1527.467 | 542.869 | 1539.974 | 64.46 | 64.75 | 0.82 |
| 64kB | duplicate_running_sum | 145.805 | 142.622 | 143.136 | 2.18 | 0.36 | -1.83 |
| 4MB | partition_ranking | 137.750 | 150.121 | 131.746 | -8.98 | -13.95 | -4.36 |
| 4MB | partition_lag | 106.364 | 123.369 | 116.593 | -15.99 | -5.81 | 9.62 |
| 4MB | partition_lead | 122.814 | 123.321 | 115.625 | -0.41 | -6.66 | -5.85 |
| 4MB | running_sum | 126.085 | 119.856 | 111.394 | 4.94 | -7.60 | -11.65 |
| 4MB | sliding_sum | 114.992 | 126.621 | 108.795 | -10.11 | -16.39 | -5.39 |
| 4MB | partition_full_sum | 106.378 | 114.607 | 104.872 | -7.74 | -9.28 | -1.41 |
| 4MB | full_first_last_nth | 149.097 | 160.410 | 152.103 | -7.59 | -5.46 | 2.02 |
| 4MB | mixed_same_keys | 171.065 | 176.192 | 151.359 | -3.00 | -16.41 | -11.52 |
| 4MB | length_lag_1 | 451.819 | 452.725 | 447.706 | -0.20 | -1.12 | -0.91 |
| 4MB | length_lag_2 | 818.602 | 478.372 | 770.915 | 41.56 | 37.95 | -5.83 |
| 4MB | length_lag_4 | 1509.066 | 522.556 | 1471.379 | 65.37 | 64.49 | -2.50 |
| 4MB | duplicate_running_sum | 149.271 | 133.864 | 121.386 | 10.32 | -10.28 | -18.68 |
| 16MB | partition_ranking | 137.037 | 135.843 | 133.899 | 0.87 | -1.45 | -2.29 |
| 16MB | partition_lag | 156.787 | 152.173 | 114.846 | 2.94 | -32.50 | -26.75 |
| 16MB | partition_lead | 307.820 | 285.429 | 115.244 | 7.27 | -147.67 | -62.56 |
| 16MB | running_sum | 116.051 | 119.982 | 124.878 | -3.39 | 3.92 | 7.61 |
| 16MB | sliding_sum | 125.055 | 164.343 | 117.692 | -31.42 | -39.64 | -5.89 |
| 16MB | partition_full_sum | 111.952 | 131.466 | 108.724 | -17.43 | -20.92 | -2.88 |
| 16MB | full_first_last_nth | 150.704 | 155.455 | 148.759 | -3.15 | -4.50 | -1.29 |
| 16MB | mixed_same_keys | 154.055 | 228.243 | 152.106 | -48.16 | -50.06 | -1.27 |
| 16MB | length_lag_1 | 456.716 | 443.929 | 455.575 | 2.80 | 2.56 | -0.25 |
| 16MB | length_lag_2 | 799.178 | 466.274 | 787.610 | 41.66 | 40.80 | -1.45 |
| 16MB | length_lag_4 | 1529.988 | 517.803 | 1474.087 | 66.16 | 64.87 | -3.65 |
| 16MB | duplicate_running_sum | 130.807 | 132.309 | 130.052 | -1.15 | -1.74 | -0.58 |

### 为定位回退而另行完成的诊断

前批64kB三慢项另用C9/C1单64kB交错旋转：16warm+48观察+4SET/SHOW=68请求，64000行/176000单元全过，真实自有PID/TID的CPU/IO/schedstat及first/lastDataRow/Ready另列。
该短实验C1三旧慢项lag/running/fullsum的wall中位158.609/143.099/143.357ms、serverCPU粗样130/130/110ms；C9为231.649/218.707/202.783ms和200/190/180ms。
旧慢值按case×执行位置成簇；新短交错未复现旧高段不等于旧回退修复。该数据既不替代前批864原样本，也不是S3加速证据。

S2首轮普通查询回退后，又冻结单4MB C1/S2交错同common8的68请求，64000行/176000单元全部正确，2正常contexts和输入哈希通过；不是重测/替换原36表。
8类wall p50为S2五高三低，processCPU三高三低二同；排名、lead、sliding的线程运行时间中位132.761→163.345、117.614→136.233、122.466→136.571ms，增长主要在首DataRow前。
但lag/running/mixed CPU较低、fullsum/firstlast相同，不能把所有回退解释为CPU或某一种等待。7/8类logical IO跨六观察相等，read_bytes均0不能排除写等待/锁/网络；100Hz CPU/proc非原子窗口较clientwall宽3.609–8.461ms。
外层wrapper首轮误用helper路径scripts/而非tests/，在0SQL预检前失败，原receipt保留；新wrapper只修路径，frozen诊断driver/SQL/序列完全不变。

然后采用逐字原CPP6437e216、独立默认allocator clocks和覆盖new的allocation可执行文件，以真实prepareBoundQuery→新PQE/原rootplanner→实际图/provider→cursor执行。
单4MB、C1/S2/C1、common8共48独立进程，各首cold+3同图真实重开=192执行、192000行/528000单元全部校验通过，48PID全终态；未运行可选16个额外evidence，不能冒称该lane也通过。
warm执行CPU的C1首/S2/C1回测（ms）依次为：ranking41.758/39.632/42.871，lag35.141/35.301/34.580，lead34.987/35.456/34.173，running35.838/34.791/32.700，sliding38.181/42.261/35.371，fullsum33.622/32.645/32.303，first-last-nth48.792/51.132/49.387，mixed48.017/48.392/49.143。
暖CPU仍1低/4高/3混合，新请求只在每次open多1/6/7个，并未观察到逐行新增请求；不是“所有phase无回退”。初始setup每进程只有一次，S2 12.829–42.504ms CPU不是建图分布。
label私有路径长度不同（S2比C1-first短6、比C1-last短1字符），请求bytes含真实路径字符串，少量几KB差异不能归因共享节省。所有原始phase和inclusive节点wall保留，不相减命名exclusive comparator CPU。

### S3改进与仍未完成范围

静态核对确认common8的完整共享身份构造均为0次：单窗口早return，三不同函数以必要签名前缀拒绝；allretained不建root身份。
有限的metadata/open额外请求不足以静态解释全部10–20ms；因此既不猜测主因，也不删除正确共享语义。S3只对完整stage的两套owner均self、无stage外incoming alias、两套order投影均原producer次序的情况恢复原C1类型化demand/move热循环。
这个条件对实际demand任意子集和top-stage切换都成立；隐藏重排第74项及任何可能共享/only-alias需求仍走S2通用路径，构造期flag跨close/restart保持。不是恢复legacy receiver、伪造上下文或改变默认Core准入。
S3构建与新220通过后继续执行完整回归、资源和原计划性能；其终态及全部慢值在下方追加，恢复旧循环本身不作为加速证据。

原273完整对象、249历史、2316既有测试文件与725保护记录保持，22complete/166partial/70unverified/15originaldeferred不变，整体Goal active。
本批额外请求不证明内存live/peak下降，Window partition/stage排序仍无完整预算。所有原始慢值、初始失败、S1/S2版本和诊断保存在同工件目录；最终安装/提交及命令收据见文末验收说明。

### S3最终完整结果

最终候选S3 `aa52b5fe…` 的原71native与新10入口全部通过，共72不同入口；完整旧61协议首轮56pass/5fail，加新220后62不同入口57pass/5fail，本轮190首次完整通过、没有startup失败或复试。
原三失败入口全文log、interval106/10及143/8完整JSON与S2字节一致。190的全部服务observation/rawFrames/oracle及终态相同，只有请求中的自有schema UUID不同，不声称整log byteequal或归一化服务响应。严格wholelog首版complete=false和后续逐服务对象正确对照均保留。

S3同源512行warm cursor的实际累计请求如下；单份请求数已等于C1，-3B只是微小布局记账，不作为内存收益。S2原表和所有版本继续保留。

| 512行warm公开cursor | C1 new请求 | S3 new请求 | C1请求字节 | S3请求字节 |
|---|---:|---:|---:|---:|
| lag-1 | 226489 | 226489 | 25625206 | 25625203 |
| lag-2 | 419723 | 232335 | 48048290 | 26317460 |
| lag-4 | 806565 | 244394 | 92906086 | 27713474 |
| sum-2 | 44542 | 43514 | 4621224 | 4432054 |

独立S3实际Main资源55请求全部通过：40完整读取+1指定强杀读取+14SET/SHOW，40000行/118000单元，5正常context成功退出。64kB宽finalSort最多2个匿名deleted FD、16MB未采到，EOF/退出无残留；确认自有PID/exe/data/starttick与FD后SIGKILL -9，同目录重开完整1000×4基表及1000×3排序。
216源、2程序、3driver、1072夹具文件前后exact，自有进程无残留。范围仍是普通最终Sort及该只读强杀场景；不证明窗口内部预算或任意写崩溃恢复，旧35操作停机-6失败不被改写。

S3沿用观察任何候选速度前冻结的同一12类、三预算、C1/S3/C1顺序，完整运行一次1296timed+324warm+18SET/SHOW=1638请求，1620000行/4725000单元全部校验通过，9正常contexts成功，输入哈希不变；没有删样本或为慢值重测。
全部36组按p50数值为11快于两旧/19慢于两旧/6夹两旧；原common8×3为3快/16慢/5夹两旧。64kB共同8项全部高于两旧；4MB滑动SUM仍高8.13%–10.16%，不能称整体回退已修。
重复两份LAG(length)在三预算相对两旧降低37.94%–40.83%，四份降低63.65%–66.34%；单份和双SUM控制仍有较慢项。全矩阵最大增长17.48%，两旧p50漂移−11.01%…+11.92%，均原样保留。
分类只是数值大小，不是显著性检验；4MB first/last/nth较old-last只高约0.004ms，也按严格规则计入慢项。样本中较大离群值没有剔除，也不凭数据自动归因宿主或某一等待。
单位ms；降低百分比正为更快、负为更慢。下表完整保留36组；每label原12样本/min/p50/max在perf-window-sharing-S3-latency-summary-v1.json的108组数据、labels108/all1296 CSV和9份完整返回值JSONL中，没有只保留中位数。

| work_mem | case | C1 first p50 ms | S3 p50 ms | C1 last p50 ms | S3 reduction vs first % | S3 reduction vs last % | C1 last drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 195.299 | 196.812 | 175.287 | -0.77 | -12.28 | -10.25 |
| 64kB | partition_lag | 128.646 | 130.350 | 123.430 | -1.32 | -5.61 | -4.05 |
| 64kB | partition_lead | 125.024 | 129.338 | 128.027 | -3.45 | -1.02 | 2.40 |
| 64kB | running_sum | 125.922 | 135.268 | 124.349 | -7.42 | -8.78 | -1.25 |
| 64kB | sliding_sum | 127.261 | 131.225 | 130.674 | -3.11 | -0.42 | 2.68 |
| 64kB | partition_full_sum | 113.076 | 125.923 | 116.411 | -11.36 | -8.17 | 2.95 |
| 64kB | full_first_last_nth | 188.388 | 208.098 | 177.136 | -10.46 | -17.48 | -5.97 |
| 64kB | mixed_same_keys | 181.970 | 200.893 | 189.618 | -10.40 | -5.95 | 4.20 |
| 64kB | length_lag_1 | 468.338 | 469.167 | 457.220 | -0.18 | -2.61 | -2.37 |
| 64kB | length_lag_2 | 830.362 | 495.874 | 799.023 | 40.28 | 37.94 | -3.77 |
| 64kB | length_lag_4 | 1521.358 | 545.786 | 1501.571 | 64.13 | 63.65 | -1.30 |
| 64kB | duplicate_running_sum | 139.884 | 144.773 | 140.875 | -3.49 | -2.77 | 0.71 |
| 4MB | partition_ranking | 146.149 | 145.419 | 144.285 | 0.50 | -0.79 | -1.27 |
| 4MB | partition_lag | 116.119 | 111.141 | 129.964 | 4.29 | 14.48 | 11.92 |
| 4MB | partition_lead | 117.443 | 113.130 | 117.826 | 3.67 | 3.99 | 0.33 |
| 4MB | running_sum | 119.568 | 110.775 | 114.544 | 7.35 | 3.29 | -4.20 |
| 4MB | sliding_sum | 118.785 | 130.852 | 121.009 | -10.16 | -8.13 | 1.87 |
| 4MB | partition_full_sum | 109.375 | 115.551 | 115.172 | -5.65 | -0.33 | 5.30 |
| 4MB | full_first_last_nth | 155.423 | 156.515 | 156.511 | -0.70 | -0.00 | 0.70 |
| 4MB | mixed_same_keys | 157.088 | 158.507 | 157.522 | -0.90 | -0.63 | 0.28 |
| 4MB | length_lag_1 | 454.208 | 450.737 | 461.849 | 0.76 | 2.41 | 1.68 |
| 4MB | length_lag_2 | 800.620 | 485.409 | 793.899 | 39.37 | 38.86 | -0.84 |
| 4MB | length_lag_4 | 1506.153 | 513.692 | 1488.218 | 65.89 | 65.48 | -1.19 |
| 4MB | duplicate_running_sum | 132.668 | 132.040 | 143.253 | 0.47 | 7.83 | 7.98 |
| 16MB | partition_ranking | 143.034 | 140.652 | 138.311 | 1.67 | -1.69 | -3.30 |
| 16MB | partition_lag | 115.122 | 120.195 | 112.400 | -4.41 | -6.93 | -2.36 |
| 16MB | partition_lead | 118.813 | 120.234 | 112.952 | -1.20 | -6.45 | -4.93 |
| 16MB | running_sum | 122.489 | 114.241 | 113.765 | 6.73 | -0.42 | -7.12 |
| 16MB | sliding_sum | 126.976 | 121.956 | 114.475 | 3.95 | -6.54 | -9.85 |
| 16MB | partition_full_sum | 117.731 | 110.108 | 104.769 | 6.48 | -5.10 | -11.01 |
| 16MB | full_first_last_nth | 154.657 | 156.755 | 147.194 | -1.36 | -6.50 | -4.83 |
| 16MB | mixed_same_keys | 153.827 | 154.108 | 148.808 | -0.18 | -3.56 | -3.26 |
| 16MB | length_lag_1 | 448.733 | 452.893 | 442.649 | -0.93 | -2.31 | -1.36 |
| 16MB | length_lag_2 | 798.865 | 472.681 | 783.154 | 40.83 | 39.64 | -1.97 |
| 16MB | length_lag_4 | 1518.115 | 510.973 | 1459.820 | 66.34 | 65.00 | -3.84 |
| 16MB | duplicate_running_sum | 134.385 | 132.332 | 128.922 | 1.53 | -2.64 | -4.07 |

### 复现与当前边界

本地不可变S1/S2/S3、前后源/62对象、61 core+实际test stub、flags、全部协议raw、资源/时钟/phase、首次失败与最终收据均在上述工件目录；它们是本机证据，不是发布包内容。新原生和固定220协议资产已纳入版本管理和标准发现/注册。
使用新的输出目录可按同一driver重放单预算（其他两预算分别改为4MB、16MB）；此命令是复现说明，本批实际执行只计前述一次完整矩阵：

```sh
p=build/pg-feature-window-compat-xi39wepx
python3 "$p/runtime_benchmark_window_sharing_v1.py"   --before "$p/before-dbms_main" --after "$p/sharing3-dbms_main"   --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000   --output build/window-sharing-manual-replay-64kB   --work-mem 64kB --rows 1000 --rounds 3 --samples 4
```

S3解决固定27差异并显著减少重复调用成本，但普通查询仍有回退；一般已分析表达式归一化、完整窗口内存预算、剩余catalog/peer-error/binary及其他原273要求继续开放。没有新的PG速度对照，不能用本项目两版倍率宣称达到PG性能。
原273对象及raw数组、原249历史原文、2316既有测试文件和725保护记录保持，近期记录只新增1条partial（合计250），总账22/166/70/15不变；Goal active。最终范围/元数据/文档检查、安装与提交收据在同目录final-provenance.json及postcommit-provenance-v1.json。

最终结构检查：版本/兼容契约、273总账覆盖、注册脚本语法、新协议AST/fixture JSON及diff空白检查均通过。754份Markdown、2429个本地文件目标无缺失/空文档/整篇重复或清单遗漏；该检查不包含全部外部URL/历史锚点。当前新增报告标题唯一，局部锚点由本批范围审计另核。

## 窗口暂存分配与类型输出调研（2026-11-03）

本批基线为 `3d576b8b` 的 S3 `aa52b5fe…`，并非旧 C1/C9。研究和隔离候选使用 `build/pg-feature-typed-wire-zbkoh3z7`；本节中的未接受提案不代表已发布功能。原273仍为22complete/166partial/70unverified/15originaldeferred，Goal active。

### 原理与验证边界

[PostgreSQL 18.6 WindowAgg](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeWindowAgg.c)保留每个窗口调用的执行状态，并区分结果/聚合上下文与分区缓存；这提供状态复用的参考，不能由此推导本程序查询会更快。
本批候选仅复用实际 WindowOp 的结果暂存容量和调用分类。每行仍依原次序计算普通窗口函数、推进聚合、计算聚合窗口、发布完整拥有的值；成功发布后重置每个暂存值，异常和close清空，不跨行缓存结果。三份vector头增加每算子72B；未接入窗口输入/分区缓存的完整work_mem预算。

scratch1在open时分配，源码 `61eb91c2…`、程序 `c0c8f259…`；scratch2将同一初始化移到原输入初始化/成功读取之后，源码 `23f7b8e0…`、程序 `14b834be…`。后者避免空输入分配并保留原输入先行次序；对CPU的影响仍只是待验证假设，不能作为第一版退化的已证实原因。

### 原生累计分配成本

以下为同一永久原生10控制源码 `a585bd7a…`、真实512行物理表、实际bind/graph/public cursor及完整结果校验。表中new是累计C++请求次数/请求字节，不是live/peak/RSS、预算或延迟。

| control | S3 calls/bytes | scratch1 calls/bytes | scratch2 calls/bytes | scratch2 call reduction vs S3 |
|---|---:|---:|---:|---:|
| lag-1 | 226489/25612768 | 225977/25522653 | 225977/25522656 | 512 |
| lag-2 | 232335/26304944 | 231823/26214826 | 231823/26214832 | 512 |
| lag-4 | 244394/27700796 | 243882/27610672 | 243882/27610684 | 512 |
| sum-2 | 43514/4430452 | 42490/4336244 | 42490/4336244 | 1024 |

另一个隔离6控制源码 `386bc30e…` 对N=0/1/512和LAG/SUM分别新建真实图，再同图重启；每代12次完整cursor执行、2052行4104单元，三代全部通过。引擎已初始化，“cold”仅指新图首次执行，图析构不在测量区间。scratch2相对S3：

| 输入 | LAG cold calls/bytes增量 | SUM cold calls/bytes增量 | warm calls减少 |
|---|---:|---:|---:|
| N=0 | 0 / +72 | 0 / +72 | 0 / 0 |
| N=1 | +1 / +80 | 0 / +72 | 1 / 2 |
| N=512 | -510 / -89856 | -1022 / -93952 | 512 / 1024 |

scratch1的N=0 cold额外2次/256B；scratch2去掉其中2次/184B，剩72B对象头。N1/N512各阶段与scratch1计数/字节相同。永久10控制的三版全部原始字节也保留，scratch2的LAG1/2/4请求字节相较scratch1分别多3/6/12B，不归一化或据此编造原因。详细阶段见 `perf-window-scratch2-cold6-phase-summary-v1.json`。

### scratch1整SQL：拒绝接受的退化候选

冻结原12案例、64kB/4MB/16MB顺序、S3-first/scratch1/S3-last，每组合3warm+12timed，15秒原超时；一次完整1638请求=1296timed+324warm+18SET，完整验证1620000行4725000单元，9正常context退出0，216源码/两程序/三driver/1072fixture前后一致。默认生产allocator和持久化设置；计时从发送查询至完整结果校验，期间不采资源/调度计数。
全部36组p50仅4组低于两次S3、21组高于两次S3、11组夹在两旧间；原common8的24组为3/15/6。最高增幅161.63%，基线首末漂移-26.05%至+13.91%。**scratch1未安装、未提交为优化。** 完整慢值、所有1296样本、每组raw12/min/p50/max保留，不使用后来的短诊断覆盖这轮数据。

| work_mem | case | S3 first p50 ms | scratch p50 ms | S3 last p50 ms | scratch reduction vs first % | scratch reduction vs last % | S3 last drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 203.111 | 354.824 | 176.540 | -74.69 | -100.99 | -13.08 |
| 64kB | partition_lag | 126.282 | 320.000 | 122.308 | -153.40 | -161.63 | -3.15 |
| 64kB | partition_lead | 127.657 | 164.768 | 122.826 | -29.07 | -34.15 | -3.78 |
| 64kB | running_sum | 128.549 | 138.051 | 123.931 | -7.39 | -11.39 | -3.59 |
| 64kB | sliding_sum | 124.450 | 181.079 | 122.034 | -45.50 | -48.38 | -1.94 |
| 64kB | partition_full_sum | 113.881 | 125.901 | 114.627 | -10.55 | -9.84 | 0.65 |
| 64kB | full_first_last_nth | 179.434 | 198.195 | 175.571 | -10.46 | -12.89 | -2.15 |
| 64kB | mixed_same_keys | 206.266 | 189.023 | 173.798 | 8.36 | -8.76 | -15.74 |
| 64kB | length_lag_1 | 470.819 | 454.355 | 464.055 | 3.50 | 2.09 | -1.44 |
| 64kB | length_lag_2 | 492.316 | 496.843 | 483.284 | -0.92 | -2.81 | -1.83 |
| 64kB | length_lag_4 | 607.205 | 612.477 | 545.463 | -0.87 | -12.29 | -10.17 |
| 64kB | duplicate_running_sum | 191.176 | 147.362 | 141.367 | 22.92 | -4.24 | -26.05 |
| 4MB | partition_ranking | 153.456 | 148.074 | 152.613 | 3.51 | 2.97 | -0.55 |
| 4MB | partition_lag | 118.254 | 130.883 | 121.368 | -10.68 | -7.84 | 2.63 |
| 4MB | partition_lead | 117.002 | 127.851 | 115.770 | -9.27 | -10.44 | -1.05 |
| 4MB | running_sum | 117.963 | 127.260 | 134.369 | -7.88 | 5.29 | 13.91 |
| 4MB | sliding_sum | 120.300 | 135.320 | 128.492 | -12.49 | -5.31 | 6.81 |
| 4MB | partition_full_sum | 111.160 | 114.533 | 107.668 | -3.03 | -6.38 | -3.14 |
| 4MB | full_first_last_nth | 157.987 | 158.967 | 168.015 | -0.62 | 5.38 | 6.35 |
| 4MB | mixed_same_keys | 160.712 | 161.856 | 158.707 | -0.71 | -1.98 | -1.25 |
| 4MB | length_lag_1 | 454.972 | 501.635 | 454.137 | -10.26 | -10.46 | -0.18 |
| 4MB | length_lag_2 | 477.550 | 501.047 | 506.483 | -4.92 | 1.07 | 6.06 |
| 4MB | length_lag_4 | 522.525 | 536.967 | 529.167 | -2.76 | -1.47 | 1.27 |
| 4MB | duplicate_running_sum | 135.406 | 136.775 | 134.179 | -1.01 | -1.93 | -0.91 |
| 16MB | partition_ranking | 138.604 | 140.286 | 136.501 | -1.21 | -2.77 | -1.52 |
| 16MB | partition_lag | 116.817 | 116.316 | 116.731 | 0.43 | 0.36 | -0.07 |
| 16MB | partition_lead | 114.565 | 114.882 | 124.229 | -0.28 | 7.52 | 8.44 |
| 16MB | running_sum | 118.356 | 113.567 | 114.579 | 4.05 | 0.88 | -3.19 |
| 16MB | sliding_sum | 122.800 | 118.270 | 116.523 | 3.69 | -1.50 | -5.11 |
| 16MB | partition_full_sum | 116.870 | 108.085 | 103.104 | 7.52 | -4.83 | -11.78 |
| 16MB | full_first_last_nth | 152.575 | 155.131 | 151.999 | -1.67 | -2.06 | -0.38 |
| 16MB | mixed_same_keys | 149.062 | 152.287 | 151.586 | -2.16 | -0.46 | 1.69 |
| 16MB | length_lag_1 | 453.689 | 451.642 | 445.748 | 0.45 | -1.32 | -1.75 |
| 16MB | length_lag_2 | 473.076 | 470.906 | 465.330 | 0.46 | -1.20 | -1.64 |
| 16MB | length_lag_4 | 513.528 | 519.661 | 514.196 | -1.19 | -1.06 | 0.13 |
| 16MB | duplicate_running_sum | 135.170 | 130.543 | 127.859 | 3.42 | -2.10 | -5.41 |

完整108标签分布/1296样本/slow-values分别见 `perf-window-scratch-latency-labels108-v1.csv`、`perf-window-scratch-latency-all1296-v1.csv`、`perf-window-scratch-slow-values-v1.json`，汇总为 `perf-window-scratch-latency-summary-v1.json`。旧S2/S3数据仍各按原输入解释。

### scratch1阶段诊断与资源

真实公开API的默认allocator计时和全局new计数分开执行：共48进程、192次执行、192000行528000单元，全部值/输入/进程清理核对通过。原common8的warm CPU，2项低于两次S3、2项高于两次S3、4项夹两旧间；runningSUM的增长主要出现在firstNext，不能仅归为逐行计算。新图setup只有每进程单个值，不作为分布；节点wall含子调用，不能当互斥阶段相减。诊断没有建立统一的CPU/宿主/调度因果。
另一个真实Main 4MB的68请求诊断（16warm/48observation/4SET）验证64000行176000单元、两个正常context。8项Ready p50有5增3降，进程CPU粗粒度计数3增1同4降；这些是独立短观察，不替代完整1638计时，不将宽时间括号的线程/proc数据称为独占会话CPU。来源为 `perf-scratch-causal-diagnostics-static-v1.json` 与 `perf-main-S3-scratch-static-analysis-v1.json`。

scratch1另有55资源请求：40次完整读取、1次已验证归属的只读进程kill、14SET/SHOW，40000行118000单元通过；5正常context退出0、故意kill退出-9、同目录重开完整base与最终Sort输出通过。64kB最终Sort实际最多2个deleted FD，16MB采样未见，正常EOF和正常context退出后清理通过。采样0不证明绝不spill；不据此宣称窗口预算、写入崩溃一致性或R15停机专项已修。首次静态汇总使用覆盖整个本批目录的过宽进程选择器，触发断言失败且未记录该瞬时PID身份，原失败保留；v2只核对实际资源目录/程序与原owned_after=[]，当时精确扫描为空，未重跑资源以取代失败。

### scratch2完整对照及最终决定

scratch2真实CMake编译退出0；仅Exec对象变化，147公共头和其余生产输入不变。71个既有原生入口及永久10控制全部通过（72个不同入口），额外cold6同源全部通过。既有61协议为56通过/5保留失败，加窗口220为62个不同入口57/5；五失败完整日志或JSON与scratch1/S3逐字相同，原四1641及143/8均未改写。
首轮220保留全部观察和COMPLETE/0失败日志，但未持久化外层进程退出码；为补终态证据，同源独立v2跑220/0、退出0，完整响应与首轮/scratch1/S3相同，未覆盖首轮文件。固定301也完整采集且与S3全部相同，driver仍退出1（201处既有PG差异），其自有服务器正常退出0，不能把它计成PG验收通过。

独立资源55也全部通过：40000行118000单元、5正常context退出0；64kB最终Sort最多2deleted FD、16MB采样0，正常EOF/退出无残留。实际归属PID2960022在有1个deleted FD时被预期kill(-9)，同目录PID2960055重开完整base1000×4、sort1000×3校验通过并正常退出。结果/输入/归属证明见 `perf-resource55-scratch2-final-summary-v1.json`；作用仍限这些实际资源控制。

随后单独一次完整1638请求/1296timed/324warm/18SET、1620000行4725000单元，64kB→4MB→16MB及9正常context全部退出0，216source/两binary/三driver/1072fixture前后一致。原driver/12案例/3warm/12timed/15秒超时未改，最终时钟未并行执行本任务编译、SQL回归或proc采样。共享主机仍非独占，未建立性能因果或置信区间。

全部36组p50为12项低于两次S3、8项高于两次S3、16项居间；原common24为8/4/12。64kB为10/0/2，4MB为1/5/6，16MB为1/3/8；4MB和16MB合计2/8/14。最高增幅13.56%；S3首末漂移-36.89%至+30.93%，如4MB ranking的S3 237.028→149.596ms，不能只引用对首次S3的32.59%下降作为稳定收益。
**最终不接受scratch1或scratch2性能候选。** scratch2的分配减少真实存在，但本轮没有证明稳定的跨预算wholeSQL收益；这不表示它所有查询都慢，也不表示已确定第一版退化原因。原S3源码精确恢复，安装程序一直保持S3 `aa52b5fe…`；两候选、全部慢值/首末基线/原36项数据及提案均保留。不盲目追加候选或重跑以选择较好样本。

| work_mem | case | S3 first p50 ms | scratch2 p50 ms | S3 last p50 ms | scratch2 reduction vs first % | scratch2 reduction vs last % | S3 last drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 168.863 | 165.506 | 194.942 | 1.99 | 15.10 | 15.44 |
| 64kB | partition_lag | 127.816 | 120.470 | 121.470 | 5.75 | 0.82 | -4.97 |
| 64kB | partition_lead | 123.401 | 121.385 | 125.238 | 1.63 | 3.08 | 1.49 |
| 64kB | running_sum | 126.616 | 124.065 | 136.188 | 2.01 | 8.90 | 7.56 |
| 64kB | sliding_sum | 141.045 | 127.873 | 147.104 | 9.34 | 13.07 | 4.30 |
| 64kB | partition_full_sum | 114.080 | 118.556 | 130.882 | -3.92 | 9.42 | 14.73 |
| 64kB | full_first_last_nth | 203.337 | 178.214 | 187.997 | 12.36 | 5.20 | -7.54 |
| 64kB | mixed_same_keys | 171.972 | 179.893 | 192.292 | -4.61 | 6.45 | 11.82 |
| 64kB | length_lag_1 | 464.166 | 454.071 | 510.167 | 2.17 | 11.00 | 9.91 |
| 64kB | length_lag_2 | 495.212 | 471.201 | 482.554 | 4.85 | 2.35 | -2.56 |
| 64kB | length_lag_4 | 547.066 | 536.814 | 543.175 | 1.87 | 1.17 | -0.71 |
| 64kB | duplicate_running_sum | 138.571 | 138.437 | 181.427 | 0.10 | 23.70 | 30.93 |
| 4MB | partition_ranking | 237.028 | 159.783 | 149.596 | 32.59 | -6.81 | -36.89 |
| 4MB | partition_lag | 137.228 | 117.219 | 119.963 | 14.58 | 2.29 | -12.58 |
| 4MB | partition_lead | 118.466 | 121.378 | 119.795 | -2.46 | -1.32 | 1.12 |
| 4MB | running_sum | 157.600 | 131.259 | 118.569 | 16.71 | -10.70 | -24.77 |
| 4MB | sliding_sum | 126.283 | 135.038 | 118.914 | -6.93 | -13.56 | -5.84 |
| 4MB | partition_full_sum | 121.171 | 123.472 | 112.251 | -1.90 | -10.00 | -7.36 |
| 4MB | full_first_last_nth | 181.334 | 173.797 | 160.384 | 4.16 | -8.36 | -11.55 |
| 4MB | mixed_same_keys | 169.199 | 172.107 | 157.288 | -1.72 | -9.42 | -7.04 |
| 4MB | length_lag_1 | 461.927 | 495.500 | 450.207 | -7.27 | -10.06 | -2.54 |
| 4MB | length_lag_2 | 537.071 | 495.075 | 478.965 | 7.82 | -3.36 | -10.82 |
| 4MB | length_lag_4 | 537.250 | 533.582 | 527.479 | 0.68 | -1.16 | -1.82 |
| 4MB | duplicate_running_sum | 150.563 | 143.306 | 133.031 | 4.82 | -7.72 | -11.64 |
| 16MB | partition_ranking | 135.686 | 137.009 | 137.184 | -0.98 | 0.13 | 1.10 |
| 16MB | partition_lag | 110.749 | 116.205 | 121.303 | -4.93 | 4.20 | 9.53 |
| 16MB | partition_lead | 115.357 | 115.780 | 119.256 | -0.37 | 2.91 | 3.38 |
| 16MB | running_sum | 115.231 | 114.998 | 114.320 | 0.20 | -0.59 | -0.79 |
| 16MB | sliding_sum | 121.090 | 120.307 | 118.668 | 0.65 | -1.38 | -2.00 |
| 16MB | partition_full_sum | 105.654 | 106.061 | 109.378 | -0.39 | 3.03 | 3.53 |
| 16MB | full_first_last_nth | 152.534 | 148.212 | 151.688 | 2.83 | 2.29 | -0.55 |
| 16MB | mixed_same_keys | 144.463 | 147.821 | 147.837 | -2.32 | 0.01 | 2.34 |
| 16MB | length_lag_1 | 444.167 | 455.528 | 448.246 | -2.56 | -1.62 | 0.92 |
| 16MB | length_lag_2 | 474.490 | 474.529 | 479.227 | -0.01 | 0.98 | 1.00 |
| 16MB | length_lag_4 | 523.441 | 526.490 | 516.397 | -0.58 | -1.95 | -1.35 |
| 16MB | duplicate_running_sum | 128.771 | 129.585 | 127.052 | -0.63 | -1.99 | -1.34 |

完整108组raw12/min/p50/max与所有1296计时样本分别见 `perf-window-scratch2-latency-labels108-v1.csv`、`perf-window-scratch2-latency-all1296-v1.csv`；汇总、慢值及round4索引为 `perf-window-scratch2-latency-summary-v1.json`、`perf-window-scratch2-slow-values-v1.json`。拒绝/源码恢复凭据为 `scratch-rejection-and-source-restore-v1.json`。这两轮各自只跑一遍预先固定的完整计划，不把独立时窗拼成scratch2/scratch1因果倍率。

### PostgreSQL 类型输出：尚未接受的功能工作

[PG 18.6 record_send](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/rowtypes.c#L687)逐字段写真实声明OID，即使字段为NULL；[array_send](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/arrayfuncs.c#L1548)保留真实元素OID、维度、下界、由真实NULL位图决定的has-null头标志及逐NULL元素的-1长度，线上不发送位图片段；空数组也先检查元素发送能力。[pg_type.dat](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_type.dat)的record[] OID2287为pseudo/P而非category A；NAME虽有typelem仍走scalar sender，ACLITEM没有二进制输出。不能用category/typelem非零或本项目当前typsend=0单一规则判断所有编码能力。
[SQLite列API](https://sqlite.org/c3ref/column_blob.html)区分原始类型与按请求转换后的表示，并限制返回值生命周期；[DuckDB 2.0 LTS DataChunk](https://duckdb.org/docs/2.0/clients/c/data_chunk)把类型化向量和NULL有效性作为数据接口，reset后须重新获取指针。这里据此选择保留真实拥有的结构化值与声明元数据，不从文本重建record/array，不借用已失效行值；这仍是本项目的设计选择，不等于移植了这些数据库的执行器。

新增隔离固定301观察包括73查询形状×Simple/text/binary、两个混合format、51builtin和10user目录观察及DDL/来源锚/副作用postcheck。独立实际PG18.6的301协议结构/预期错误阶段/完整字节/来源锚验证全过，冻结参考未因候选而修改；基线S3、scratch1和scratch2各有201处差异，三版全部301实际响应逐项相同。差异包含42codec、36物理REGTYPE创建失败级联、61目录元数据、7描述/行元数据、12枚举数组文本误标、6domain数组缺失、3domain-over-array基底、6append/prepend重解析、12named目录、6匿名输入阶段及10其他；不是201个独立根因，不能把未实现项改判为预期通过。

最早误连现有PG17.2端点，版本guard在SHOW后终止，退出1；其本任务UUID库在finally删除成功，原日志保留，管理请求另计。随后用本任务拥有的新建原生18.6 cluster重新采集完整301，正常退出并核对PID；没有把17.2数据标成18.6，也未操作其他数据库对象。

类型载体/metadata factory/receiver/routine/codec/array/spill提案仍在隔离目录。统一完整头树下ExprEvaluator、TypedSortSpill和PostgresProtocol的syntax-only通过，不代表新ABI链接或SQL运行通过。首个混用新旧头的语法检查63个AST重定义错误原样保留，统一树复核后0error；没有删除错误或冒称生产编译通过。每个ExprValue新增shared_ptr的全路径成本、真实值producer/物理REGTYPE/domain/named类型及完整协议/恢复/性能仍待验证。
R151明确暂停的顶层enum/enum[]协议描述符与原103门禁、R177/R185/R65、retained planning/frame/portal/format2等范围均不因通用metadata提案而恢复；generic提案尚无全链旧描述符字节连续性证明，未应用到生产。原Bind/CTE/nested/interval/integer-key五失败入口继续独立保留，不能把所有未解决问题一概算作已暂缓。

### 本批最终恢复与文档检查

拒绝候选后正常执行CMake构建，实际重编Exec并链接，退出0、59.60秒（构建时间，非查询性能）。216生产源与基线全文相同，构建程序和安装程序均精确为S3 SHA256 `aa52b5fe35e1d1ef92a301bcad8b72d95b32edb705e650d3539b999d8c5b6b0f`，见 `restored-S3-cmake-receipt-v1.json`；这不是新全库fresh build，也没有把旧S3回归写成恢复后的新测试。
九份既有文档同步，结构检查覆盖754 Markdown及2435个本地文件链接，缺失/空文件/全文重复/清单遗漏均0；一般外部URL和历史fragment不在该检查范围。原273条目和250历史原文保留，新增近期记录仅partial；原2319测试/147头/725保护记录不改，没有新增Markdown或永久测试，Goal active。

## 拥有值的复合类型输出与引用类型存储（2026-11-03）

功能源提交 `89696698`，受测程序 C7 SHA256 为 `f135335e6ce07b23bf325f6303431488842cb418ebf84ef8a8a02c6df63d3482`。本批功能验证、公开接口成本、55项资源检查和1638次SQL对照均已完成。36组中23组的p50高于两次S3回测，C7未通过默认安装的性能门，根目录程序仍为S3 `aa52b5fe…`；功能源码保留，继续优化实际新增成本。没有整体加速、RSS改善或PostgreSQL替代结论，原始慢值与失败保留。

### 实现及设计来源

原分析位置冻结真实声明，区分 actual OID 与基础 codec OID，保留域、枚举角色、字段 typmod、长度和 NULL 字段类型；内部识别的 binary role 不是已修好 `pg_type.typsend` 的声明。`ExprValue` 追加一个共享不可变复合载体，已有 recordFields 继续拥有真实字段；数组保留真实 flat 元素、维度/下界及已知位图状态，NULL 数组、空数组和全 NULL 元素分开。值流经实际 ROW/数组生产者、转换/切片/拼接、排序快照和例程/查询接收器，不从打印结果反推 record 字段或按 search_path 猜 OID。

普通结果捕获默认关闭；仅全部格式码有效为 0/1 且确需格式 1 的原请求才打开载体，混合 0/1 仍保持逐列格式。旧 RowDescription 所有字段保留，只有冻结声明与已有 OID/长度/typmod 匹配时才附 codec 元数据。未知/不匹配声明继续原完整路径；顶层枚举、枚举数组和底层涉及该族的域仍原完整路径，不借 binary 输出改 R151 暂停表现或补数组目录配对。没有增加 Bind/portal 的宽度缓存、参数帧、provider、查询重评或格式 2 的新验证时点。

REGTYPE/REGTYPE[] 是本批新增的真实存储类型：输入名字按实际目录解析一次；标量持久化完整 uint32 OID，SQL NULL 由位图区分，OID 0 不充作 NULL。数组保留真实元素 OID 和形状；读取从原始数值载入 OID 后生成显示，不把 pretty name 当持久身份。新增类型的索引键按无符号身份构造；输入缓存仅属于实际本次 open，close/错误/重开清除。普通格式 1 参数使用原 envelope 检查，2206/2211 对应真实 scalar/array 角色。

C6 修正 schema 重载后丢失通用 unsigned 标志造成的高位符号扩展：真实 REGTYPE 固定读取 uint32，不改磁盘格式或一般整数分类。C7 再修正原 CAST 输入 hint 丢失新 REGTYPE[] 的数组声明，以及真正 `PreparedReturningExecution` 在原 setSourceRow 前的 stored OLD/NEW 值加载；原 AST、行绑定、投影次序、回滚/锁及实际执行 owner 保持。旧 legacy RETURNING 适配和这次真实 prepared receiver 分开记录，不把前者通过当后者完成。

设计参考 [PG18.6 parse_coerce](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_coerce.c)对目标类型、typmod 和转换上下文的处理，[record_send](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/rowtypes.c)从真实记录描述符发出字段 OID/NULL/数据，以及 [array_send](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/arrayfuncs.c)使用真实元素类型、维度、下界和位图。NULL record 字段仍有声明 OID；非 NULL 数组即使为空也有元素输出角色要求，不能用探测值替代类型能力。

[SQLite VDBE](https://www.sqlite.org/opcode.html#registers)将原始类型/NULL/值保存在寄存器中，[DuckDB Vector/DataChunk](https://duckdb.org/docs/2.0/internals/vector.html)将类型化列作为执行容器。本批借鉴“值和类型在明确生命周期内一起传递”的做法，没有实现 VDBE、DataChunk/vectorized executor，也没有据此声称与两种引擎等价或获得其性能。

多态 `array_append/array_prepend` 按原始参数顺序确定共同元素类型，再在同一原参数求值处转换实际值，每个参数仍只求值一次。修正了 CHAR/TEXT 尾部空格丢失和 NUMERIC/INTEGER 混合时小数被取整的问题；原 1133 观察的差异由 69 降为 39，所有原来正确的值组件继续正确，原生数组入口的 481 条检查通过。数组下标/切片还从真实数组声明传递元素 OID、typmod 和记录字段结构，修正了四处逻辑 VALUES 的缺失描述符及三处 CHAR(2) 切片的修饰符。

NUMERIC 的 `Infinity/-Infinity` 二进制输出按实际 PG18.6 金样本保留 scale 头 `0x0020`；NaN、有限值和接收策略不变。新增原生十项逐字节检查通过，两个既有数值输出入口在同批 S3/C5 对照均通过；没有扩大 R271 暂缓的裸 NUMERIC 输入范围。

### 分代构建与功能证据

本轮先建立完整 fresh ABI，再按真实依赖构建后续候选。C7 为两个 CPP 实际重编及 60 个有效同 ABI 对象复用，生产链接共 62 对象；不能称每代均 fresh62。首轮嵌套默认 options 编译问题、早期 copy2/mtime 导致的漏重编及其不可用于当前源码/对象验收的观察均按原记录保留，以真实 `.o.d`/输入哈希/对象谱系核验后的候选作为当前证据。

提交前仅删除 `OwnedBinaryDatum.inc` 末尾一处多余空行；正常 CMake 确实重编 Protocol 并链接，全部 62 个对象与程序均和 C7 逐字节相同。已执行的功能与资源观察属于同一程序，不为这处格式修正重新采样；最终源码清单按该格式核对收据记录。

| 代别/固定范围 | 实际结果 | 保留限制 |
| --- | --- | --- |
| C5 完整原生 106 | 105 通过/1 失败 | 原例程数组签名退出失败，不写成全部通过 |
| C5 完整协议 95 | 86 通过/9 失败入口 | 5011 详细观察无新增错误结果、原220完整相等；不等于 C7 重跑全部95 |
| C7 最终选取原生 26 | 26 通过 | 含新三个入口共38控制；不是原生全量 |
| C7 最终选取协议 31 | 24 通过/7 既有失败入口 | bit、name、Bind、CTE、nested、interval、143；完整原始失败保留 |
| PG18.6 固定301 | 187 匹配/114 差异 | AA52 为100/201，本批改善87；无原通过观察退回错误 |
| 位图/CAST83 | 81 匹配/2 配置差异 | SHOW/UTC 原预期不改 |
| 多态数组111 | 109 匹配/2 配置差异 | SHOW/UTC 原预期不改 |
| REGTYPE物理68 | 57 匹配/11 差异 | 真磁盘、索引、COPY、同目录冷重开；完整11不删 |
| REGTYPE正常参数16 | 16 匹配 | 文本/二进制、NULL、空数组、多维、无符号边界及原错误 |

PG 参考使用实际验证的 18.6（180006）自有上下文；旧误连版本、原输入/配置错误和所有原始帧保留。候选不支持相同 SHOW/version/UTC 行为时仍算真实差异，没有换成 180000 或更宽松预期。每项按固定 SQL、预期、消息结构、真实 OID/长度/typmod、行/NULL及错误验证；上述重复模式观察不能当作独立 PostgreSQL 功能数。

三个新永久原生入口为 `tests/owned_compound_spill_codec_test.cpp`16、`tests/owned_compound_physical_spill_test.cpp`12、`tests/numeric_special_binary_output_test.cpp`10，共38控制；原2319测试文件不变，新增后2322文件。真实物理/排序快照值和 codec 控制分开；原保护记录及原273状态仍为22 complete /166 partial /70 unverified /15 deferred_by_user。

C4 的 REGTYPE68 为29差异，C6为13，C7为11。C7 第10项真实文本为 `-`、`boolean`、`2147483648`、`4294967295`、NULL；第42项 UPDATE RETURNING 为 `(2,20,(bigint,"{boolean,boolean}"))`。与 C6 的完整实际响应/原始帧相比只有10/42改变，其余66完全相等；两个同数据目录的物理epoch与参数进程均干净终止。其余11项为 SHOW（1）、链式CAST（9/50/59）、域rename/drop/recreate及后续identity连锁（25/26/27/28/29/68）、ALTER USING（49），仍是普通未完，不新增用户暂缓。

### 尚未完成的普通能力与保护范围

真正 prepared RETURNING 仍缺按需捕获复合 binary 结果的分支；修正其文本行物化不等于该载体完成。错误前已经产生的部分记录行仍未完整交付；更多 UPDATE WHERE/SET 原 raw 输入的类型加载还需实际控制，68内某个赋值通过不覆盖全族。域数组/命名复合目录、部分真实复杂输入、legacy/unbound typmod 冻结及 PL变量/参数的 payload 传递仍有普通依赖；不以有一个 carrier 指针就称完整 producer。

旧数组文本存储无法恢复所有位图历史，NULL-free 切片可能仍需保留源位图身份；新运行期拥有值与已有可知位图不能伪称旧磁盘格式已经保真。普通 REGTYPE 的域重命名、删除/重建、ALTER USING 及相关 chained CAST 仍开放。R151 的目录/顶层描述符和原103、R65子provider、R106、保留规划/实际参数帧/portal/format2、R177/R185以及其他已暂缓记录均未恢复；这些暂停范围与本节普通剩余问题分开列示。

### 载体成本与测量边界

实测 ABI 增量为每个 `ExprValue` +16 B、`ExprEvalResult` +208 B、`DmlResult` +64 B；无复合值的 primitive 也承担新增对象头大小。不可变 metadata 可共享，但实际物化/快照/接收器仍可能增加分配与复制。私有 prepared RETURNING 另持真实 TableSchema 引用和一份数据库名副本：表局部对象的寿命覆盖接收器，数据库名确有复制成本，不能称零开销。

实际驱动分别记录准备/分析、建图、首次 next、重启执行、完整拥有值接收/验证、协议发送及关闭；默认 allocator 时钟与累计 C++new 观察分开，冷样本不当分布，包含子节点的时间不相减后称 exclusive comparator CPU。同时检查原始非复合 primitive 路径、NULL/空结果、默认关闭捕获与格式0/1控制，完整慢项不得筛掉。旧窗口全部数据、35操作停机失败R15及原速度对照保持，资源/阶段观察不替代这些验收。

### 复现输入

本机工件位于 `build/pg-feature-owned-binary-afdbnxxu`，不可变程序为 `candidate-v7-dbms_main`；固定计划、PG参考、全部初次失败及每次原始帧均保留。REGTYPE复用 `physical-regtype-proposal-v2/run_candidate_oracles_v1.py`，分别 `--mode physical68`/`parameters16`，显式指定上述程序与完整SHA及新的未存在label；68自动在同一私有data目录停机/冷重开，不替换为新空目录。原始68/16计划和预期不改。其它协议与选取回归的精确command见各冻结receipt，不把旧候选命令标成C7；原生只能链接C7同ABI对象/config及fresh stub，不能混用AA52旧头/对象。

功能增量已提交，C7不提升为默认安装。Goal active；原273功能族仍开放，不宣称全面性能优化或PostgreSQL等价。版本仍为0.2.0/C++17/TLSstub；按用户指定身份在master管理，不创建新分支、不push、不触发Actions。


### 本批资源与公开成本实测

独立资源55项通过：40次完整读取、14次SET/SHOW、1次预期在途只读强杀，完整核对40000行/118000单元格，5个正常进程上下文退出0。64kB普通最终排序实际观察最多2个匿名FD，16MB采样为0（不是硬性无spill保证），EOF无残留，瞬态命名文件均解析完成。仅强杀核对过exe/data/start-ticks且存在真实deleted spool的私有进程（退出−9）；同目录重开再次核对1000×4基础值和1000×3排序值。它不证明窗口预算、硬RSS或任意写入恢复，原R15失败不重写。

同一原生驱动分别编译旧/新、默认分配器/计数器四个程序，使用真实0/1/512行堆表、实际绑定和公开游标，每图首次执行及三次重启。四进程完整3例/0失败，共48次游标调用/8208行；所有值、NULL、类型和保留行的寿命均检查。旧捕获API缺失保留“不可用”，默认分配器的计数字段记为未测量；全部442条wall/process/thread CPU和分配指标见[机器归档](benchmarks/owned-datum-20261103.json.gz)。

| 计数区间 | S3 | C7 | 限定解释 |
| --- | ---: | ---: | --- |
| 512行游标，首次执行新增请求 | 基准 | +512次/+204960 B | 实际输入、排序、输出和验证，不是RSS |
| 每次同图重启执行 | 基准 | +512次/+204800 B | 三次都保留，不选最快一次 |
| 辅助求值关闭捕获，512行/1536表达式 | 1218134次/88357760 B | 1218134次/88677248 B | 次数相同，多319488 B |
| C7辅助求值打开捕获 | 旧API不可用 | 1450070次/126939008 B | 比自身关闭多231936次/38261760 B，无旧版加速比 |
| 512行primitive文本发送 | 5419次/71614 B | 相同 | 输入向量和接收校验在发送计量外 |
| 512行primitive binary发送 | 7403次/77595 B | 相同 | 可用metadata/owning组合全部核对，无分配减少结论 |

默认分配器单次辅助求值关闭捕获的process CPU为457.716→476.401 ms，C7打开捕获为620.054 ms；包含实际maps、每行三次helper和校验，不是纯编码CPU。一次冷样本和三次相关重启不构成稳定分布；含子节点的累计时间不相减冒充exclusive CPU。

额外成本已被测出。源码还定位到新物理值桥每行多调用一次 `scanOrigin()`，复制31字节数据库名会分配32字节，与512行多512次请求吻合。这是下一步的具体优化点，尚未改动本次受测程序，也不能用它统一解释全部慢项或所有字节增长。

### 完整SQL时钟与全部慢值

所有构建、功能、资源和成本进程终态后单独测量：固定1000行夹具、12项SQL、64kB/4MB/16MB，按S3-first/C7/S3-last；每项每代3次未计时预热和12次计时，默认分配器、原15秒超时和持久化设置不变，时钟期间无诊断采样器。共1296计时+324预热+18 SET/SHOW=1638请求，完整核对1620000行/4725000单元格，9个正常上下文退出0，输入哈希前后一致。外层脚本首次漏导入sys，发SQL前失败、0样本；仅补导入后原计划执行一次，三个固定驱动不改、未重抽样。

36组样本p50：4项低于两次S3、23项高于两次S3、9项居间；原common8的24组为3/14/7，额外4项的12组为1/9/2。64kB为3/2/7，4MB为1/10/1，16MB为0/11/1。分类只表示p50大小，没有显著性结论；旧基线漂移−6.65%至+79.49%全部保留，不将23个慢项统一归为环境噪声。

最差中位数比为4MB running_sum：133.758/250.745/154.388 ms，新版约为两旧的1.875/1.624倍。最大单样本4MB/C7 duplicate_running_sum index2=1788.786601 ms，完整1000行/3000单元格核对通过，未删除。全部108组原始12样本、min/p50/max、1296计时记录和324未计时预热校验随机器数据纳入版本管理，下表不替代分布。

| work_mem | case | S3 first p50 ms | C7 p50 ms | S3 last p50 ms | C7/S3 first | C7/S3 last | S3 drift % | class |
|---|---|---:|---:|---:|---:|---:|---:|---|
| 64kB | partition_ranking | 177.105 | 168.811 | 168.830 | 0.9532 | 0.9999 | -4.67 | faster_than_both |
| 64kB | partition_lag | 120.671 | 128.821 | 137.482 | 1.0675 | 0.9370 | +13.93 | between_or_equal |
| 64kB | partition_lead | 125.805 | 127.032 | 130.535 | 1.0098 | 0.9732 | +3.76 | between_or_equal |
| 64kB | running_sum | 126.756 | 119.893 | 121.187 | 0.9459 | 0.9893 | -4.39 | faster_than_both |
| 64kB | sliding_sum | 127.373 | 127.607 | 137.622 | 1.0018 | 0.9272 | +8.05 | between_or_equal |
| 64kB | partition_full_sum | 113.652 | 114.464 | 120.809 | 1.0071 | 0.9475 | +6.30 | between_or_equal |
| 64kB | full_first_last_nth | 180.693 | 182.895 | 198.536 | 1.0122 | 0.9212 | +9.88 | between_or_equal |
| 64kB | mixed_same_keys | 172.234 | 199.008 | 196.076 | 1.1555 | 1.0150 | +13.84 | slower_than_both |
| 64kB | length_lag_1 | 461.577 | 457.866 | 459.389 | 0.9920 | 0.9967 | -0.47 | faster_than_both |
| 64kB | length_lag_2 | 483.279 | 483.725 | 489.418 | 1.0009 | 0.9884 | +1.27 | between_or_equal |
| 64kB | length_lag_4 | 545.383 | 555.631 | 540.720 | 1.0188 | 1.0276 | -0.86 | slower_than_both |
| 64kB | duplicate_running_sum | 144.290 | 139.929 | 135.309 | 0.9698 | 1.0341 | -6.22 | between_or_equal |
| 4MB | partition_ranking | 159.118 | 214.920 | 172.492 | 1.3507 | 1.2460 | +8.41 | slower_than_both |
| 4MB | partition_lag | 120.841 | 165.378 | 162.278 | 1.3686 | 1.0191 | +34.29 | slower_than_both |
| 4MB | partition_lead | 120.267 | 178.065 | 200.749 | 1.4806 | 0.8870 | +66.92 | between_or_equal |
| 4MB | running_sum | 133.758 | 250.745 | 154.388 | 1.8746 | 1.6241 | +15.42 | slower_than_both |
| 4MB | sliding_sum | 138.684 | 148.437 | 133.241 | 1.0703 | 1.1140 | -3.92 | slower_than_both |
| 4MB | partition_full_sum | 115.722 | 109.823 | 206.510 | 0.9490 | 0.5318 | +78.45 | faster_than_both |
| 4MB | full_first_last_nth | 182.160 | 268.841 | 180.491 | 1.4758 | 1.4895 | -0.92 | slower_than_both |
| 4MB | mixed_same_keys | 165.160 | 278.162 | 176.703 | 1.6842 | 1.5742 | +6.99 | slower_than_both |
| 4MB | length_lag_1 | 456.399 | 592.242 | 456.426 | 1.2976 | 1.2976 | +0.01 | slower_than_both |
| 4MB | length_lag_2 | 485.116 | 559.404 | 497.562 | 1.1531 | 1.1243 | +2.57 | slower_than_both |
| 4MB | length_lag_4 | 531.987 | 693.537 | 570.986 | 1.3037 | 1.2146 | +7.33 | slower_than_both |
| 4MB | duplicate_running_sum | 141.387 | 258.798 | 253.779 | 1.8304 | 1.0198 | +79.49 | slower_than_both |
| 16MB | partition_ranking | 144.437 | 144.829 | 140.810 | 1.0027 | 1.0285 | -2.51 | slower_than_both |
| 16MB | partition_lag | 120.161 | 135.426 | 127.867 | 1.1270 | 1.0591 | +6.41 | slower_than_both |
| 16MB | partition_lead | 119.163 | 131.580 | 122.470 | 1.1042 | 1.0744 | +2.77 | slower_than_both |
| 16MB | running_sum | 113.511 | 126.380 | 122.711 | 1.1134 | 1.0299 | +8.10 | slower_than_both |
| 16MB | sliding_sum | 128.611 | 136.082 | 120.059 | 1.0581 | 1.1335 | -6.65 | slower_than_both |
| 16MB | partition_full_sum | 108.949 | 118.663 | 119.804 | 1.0892 | 0.9905 | +9.96 | between_or_equal |
| 16MB | full_first_last_nth | 153.109 | 171.241 | 153.643 | 1.1184 | 1.1145 | +0.35 | slower_than_both |
| 16MB | mixed_same_keys | 162.072 | 173.210 | 157.212 | 1.0687 | 1.1018 | -3.00 | slower_than_both |
| 16MB | length_lag_1 | 447.291 | 501.924 | 461.568 | 1.1221 | 1.0874 | +3.19 | slower_than_both |
| 16MB | length_lag_2 | 472.844 | 521.136 | 483.177 | 1.1021 | 1.0786 | +2.19 | slower_than_both |
| 16MB | length_lag_4 | 525.584 | 558.666 | 527.826 | 1.0629 | 1.0584 | +0.43 | slower_than_both |
| 16MB | duplicate_running_sum | 127.019 | 135.541 | 132.014 | 1.0671 | 1.0267 | +3.93 | slower_than_both |

### 可移植数据与后续工作

[owned-datum-20261103.json.gz](benchmarks/owned-datum-20261103.json.gz)保存全部样本、阶段指标、114项残余SQL及实际/期望、三份冻结驱动源码与SHA、固定SQL/Python值预期、夹具清单和依赖哈希。原程序与私有目录留在本机工件区；其他机器可按归档装载函数建立相同逻辑数据，不能声称重现相同文件哈希或延迟。

在仓库根目录提取驱动，再按原参数提供旧/新程序、夹具与新的输出目录；保持 `build/<folder>/` 深度，以保留原依赖定位：

```python
import json
from pathlib import Path
archive = json.loads(Path("docs/benchmarks/owned-datum-20261103.json.gz").read_text())
for driver in archive["reproduction"]["drivers"]:
    target = Path(driver["extract_relative_path"])
    assert not target.exists()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(driver["source"])
```

功能源码已提交；因速度和分配回退，C7未提升为默认安装，根目录仍为S3。下一步先消除实际多余的来源复制和临时容量开销，再以相同严格矩阵验证。原273状态、既有暂缓与Goal active保持。


## 原始类型行物化分配优化（2026-11-03）

本轮只修改 `PreparedSelectState::cells`，基于 `745bed31` 的 C8 程序 SHA256 为 `91d339db4bdfb55922e92d853f9a357e10c7c376b4c711cf302c191282ea6115`。实际分配请求减少；完整查询对照为5快/18慢/13居间，仍未通过默认安装性能门，根目录保留 S3 `aa52b5fe…`。C7 的功能源提交、4/23/9结果、慢样本与[原机器归档](benchmarks/owned-datum-20261103.json.gz)继续保留；两个独立测量窗口不能计算因果性的 C7→C8 查询加速倍率。

### 实现与成本

此前定位的额外来源复制只用于新数组/引用类型的物理值加载。本轮先检查真实 schema，只有非虚拟存储列确实需要该加载器时才取物理来源；普通列不复制这份新来源。旧结构化值提前返回、原 NULL/解码来源路径、数组/REGTYPE声明匹配及错误 owner 保持。只有完整、可用、宽度合法的 primitive 行才预留实际列数，减少 vector 扩容；未知、缺值或异常回退继续原路径。没有改变公开头文件、ABI、SQL 类型或分析范围，也未改动共享排序 helper/分组聚合/保留规划等暂停主体。研究依据沿用上一节实际类型、值生命周期与输出声明的分离；这不是新的向量化执行引擎。

同一驱动实际运行 S3/C7/C8 的默认分配器与 C++ new 计数器共六个程序，保留690条阶段指标、72次真实游标执行、12312行校验和全部原始日志。默认分配器计数字段为未测量，旧捕获 API 缺失仍记为不可用。

| 512行/三列公开游标累计区间 | S3 | C7 | C8 | C8 相比 C7 |
|---|---:|---:|---:|---:|
| 首次执行请求数 | 33414 | 33926 | 32390 | −1536 |
| 首次执行请求字节 B | 3524249 | 3729209 | 3335993 | −393216 |
| 同图每次重启请求数，三次均相同 | 33416 | 33928 | 32392 | −1536 |
| 同图每次重启请求字节 B | 3522761 | 3727561 | 3334345 | −393216 |

相对 S3 每次减少1024次请求，首次少188256B、每次重启少188416B。计数变化只出现在 N=1/512 的首次取行区间；准备、其余游标阶段、辅助表达式求值和文本/二进制发送的请求数及字节均与 C7 相同，载体 ABI 成本也未消失。这里是累计 C++ 分配，不是 live/peak/RSS；默认分配器的单次 CPU 观察在未改阶段也普遍降低，不能归因或宣称稳定 CPU 加速。

### 构建、正确性与资源

正常 CMake 实际重编一个 CPP、复用61个有效同 ABI 对象，最终链接62对象；严格依赖闭包、冻结226源码和 fresh 原生 stub 分别核对。实际 C8 最终选取26原生入口全部通过；31协议入口24通过/7既有失败，逐条完整响应与 C7 相同。固定301为187匹配/114差异，83与111各保留两项配置差异；REGTYPE物理68为57/11、正常参数16全过，以上完整实际均与 C7 相同。这不是 C8 重跑完整106/95，也不关闭原功能族或普通未完能力。

另行完成55资源请求：40完整读取、14 SET/SHOW及1次已核验只读临时文件强杀；完整40000行/118000单元格，五个正常上下文退出0。64kB观察最多两个已删除 FD、16MB观察为0；EOF/关闭后剩余 `{}`、无命名残留。同一私有数据目录重开后完整值正确。它不覆盖任意写事务崩溃恢复、窗口预算上限、硬 RSS 或暂缓的 R15。

### 独立完整查询对照

所有功能、成本和资源进程终态后单独执行一次冻结计划：1000行夹具，原12 SQL与完整 Python 预期，预算顺序64kB/4MB/16MB，每档 S3-first/C8/S3-last；各查询每代3次未计时预热、12次计时，默认分配器和原15秒超时不变，时钟期间无诊断采样器。1296计时+324未计时预热+18 SET/SHOW=1638请求，完整1620000行/4725000单元格，九个正常上下文退出0；二进制、三份驱动、1072文件夹具和226源码前后哈希一致。

36组 p50 为5低于两旧/18高于两旧/13居间；原common8的24组为4/9/11，额外4项的12组为1/9/2。三档分别0/9/3、4/2/6、1/7/4。分类只表示大小，无统计显著性结论。两次 S3 漂移−49.17%至+9.86%，全部保留；不能把18个慢项统一解释为噪声，也不能用4MB的旧版大漂移宣称新版普遍加速。

最坏中位数比为64kB partition_full_sum：118.624/134.446/114.185ms，C8为两旧的1.133/1.177倍。最大单样本为4MB old-first running_sum index2=1455.411885ms；C8自身最大为4MB partition_full_sum index0=1104.842667ms，均完整值校验通过，未删除。全部108组原始12样本及min/p50/max、1296计时记录、324预热校验见[本轮机器归档](benchmarks/owned-datum-primitive-20261103.json.gz)。

| work_mem | case | S3 first p50 ms | C8 p50 ms | S3 last p50 ms | C8/S3 first | C8/S3 last | S3 drift % | class |
|---|---|---:|---:|---:|---:|---:|---:|---|
| 64kB | partition_ranking | 168.123 | 171.888 | 166.031 | 1.0224 | 1.0353 | -1.24 | slower_than_both |
| 64kB | partition_lag | 140.270 | 128.622 | 123.015 | 0.9170 | 1.0456 | -12.30 | between |
| 64kB | partition_lead | 132.540 | 136.746 | 125.268 | 1.0317 | 1.0916 | -5.49 | slower_than_both |
| 64kB | running_sum | 124.877 | 130.726 | 129.290 | 1.0468 | 1.0111 | +3.53 | slower_than_both |
| 64kB | sliding_sum | 136.603 | 134.691 | 128.121 | 0.9860 | 1.0513 | -6.21 | between |
| 64kB | partition_full_sum | 118.624 | 134.446 | 114.185 | 1.1334 | 1.1774 | -3.74 | slower_than_both |
| 64kB | full_first_last_nth | 198.259 | 186.897 | 175.949 | 0.9427 | 1.0622 | -11.25 | between |
| 64kB | mixed_same_keys | 175.393 | 189.592 | 175.763 | 1.0810 | 1.0787 | +0.21 | slower_than_both |
| 64kB | length_lag_1 | 460.063 | 471.537 | 455.230 | 1.0249 | 1.0358 | -1.05 | slower_than_both |
| 64kB | length_lag_2 | 489.683 | 494.518 | 478.440 | 1.0099 | 1.0336 | -2.30 | slower_than_both |
| 64kB | length_lag_4 | 553.787 | 572.783 | 569.365 | 1.0343 | 1.0060 | +2.81 | slower_than_both |
| 64kB | duplicate_running_sum | 147.823 | 151.971 | 143.716 | 1.0281 | 1.0574 | -2.78 | slower_than_both |
| 4MB | partition_ranking | 246.074 | 164.636 | 161.933 | 0.6690 | 1.0167 | -34.19 | between |
| 4MB | partition_lag | 130.976 | 123.580 | 129.625 | 0.9435 | 0.9534 | -1.03 | faster_than_both |
| 4MB | partition_lead | 135.111 | 122.625 | 128.872 | 0.9076 | 0.9515 | -4.62 | faster_than_both |
| 4MB | running_sum | 233.438 | 134.261 | 121.399 | 0.5751 | 1.1060 | -48.00 | between |
| 4MB | sliding_sum | 135.637 | 135.708 | 123.370 | 1.0005 | 1.1000 | -9.04 | slower_than_both |
| 4MB | partition_full_sum | 229.439 | 132.592 | 116.629 | 0.5779 | 1.1369 | -49.17 | between |
| 4MB | full_first_last_nth | 192.664 | 164.259 | 175.810 | 0.8526 | 0.9343 | -8.75 | faster_than_both |
| 4MB | mixed_same_keys | 232.841 | 166.882 | 160.327 | 0.7167 | 1.0409 | -31.14 | between |
| 4MB | length_lag_1 | 497.203 | 464.940 | 455.068 | 0.9351 | 1.0217 | -8.47 | between |
| 4MB | length_lag_2 | 487.978 | 495.494 | 484.960 | 1.0154 | 1.0217 | -0.62 | slower_than_both |
| 4MB | length_lag_4 | 824.225 | 543.393 | 544.423 | 0.6593 | 0.9981 | -33.95 | faster_than_both |
| 4MB | duplicate_running_sum | 246.773 | 138.154 | 134.087 | 0.5598 | 1.0303 | -45.66 | between |
| 16MB | partition_ranking | 148.313 | 144.063 | 139.008 | 0.9713 | 1.0364 | -6.27 | between |
| 16MB | partition_lag | 127.416 | 122.034 | 125.886 | 0.9578 | 0.9694 | -1.20 | faster_than_both |
| 16MB | partition_lead | 129.070 | 122.420 | 121.172 | 0.9485 | 1.0103 | -6.12 | between |
| 16MB | running_sum | 118.442 | 120.637 | 119.855 | 1.0185 | 1.0065 | +1.19 | slower_than_both |
| 16MB | sliding_sum | 123.014 | 123.573 | 123.994 | 1.0045 | 0.9966 | +0.80 | between |
| 16MB | partition_full_sum | 108.288 | 121.580 | 118.966 | 1.1227 | 1.0220 | +9.86 | slower_than_both |
| 16MB | full_first_last_nth | 155.548 | 158.452 | 157.586 | 1.0187 | 1.0055 | +1.31 | slower_than_both |
| 16MB | mixed_same_keys | 155.356 | 155.870 | 156.627 | 1.0033 | 0.9952 | +0.82 | between |
| 16MB | length_lag_1 | 461.072 | 462.021 | 461.881 | 1.0021 | 1.0003 | +0.18 | slower_than_both |
| 16MB | length_lag_2 | 479.174 | 492.839 | 485.383 | 1.0285 | 1.0154 | +1.30 | slower_than_both |
| 16MB | length_lag_4 | 537.580 | 541.167 | 524.229 | 1.0067 | 1.0323 | -2.48 | slower_than_both |
| 16MB | duplicate_running_sum | 131.650 | 135.683 | 132.958 | 1.0306 | 1.0205 | +0.99 | slower_than_both |

### 复现与后续

本机不可变程序为 `build/pg-feature-owned-binary-afdbnxxu/candidate-v8-dbms_main`。精确三档命令、驱动源码与SHA、固定SQL/完整值预期和夹具装载方法都在本轮机器归档；用新输出目录运行，不覆盖首次结果。原生使用同代61 core对象和实际 fresh stub，不混用 S3 头文件。运行环境仍为GCC13.3/O2/C++17/ICU74.2/zlib1.3/TLSstub，未将语言标准升级当作优化。

本轮源码保留，默认安装性能提升拒绝；仍需减少可选拥有值载体在普通路径的对象、辅助求值及窗口输出成本，并分别验证真实 binary RETURNING、更多 UPDATE 输入、域/命名复合目录等普通缺口。原273项状态及历史保持，新增进度仍为partial，Goal active。按指定身份直接在 master 管理，无新分支或推送。


## 内置函数注册与复合 RETURNING（2026-11-03）

基于 `7b87f84f` 的最终 C3 程序 SHA256 为 `425165afe19d10ccf9be806c3052e9a09e3875a3826142d2949cde995fdf94d1`。固定1000行、12条查询、三档预算的36组p50均低于前后两次S3；局部辅助求值也减少分配。35类操作对C8为18快/2慢/15居间，仍有慢项。已将同一受测程序原子替换为本地默认，S3回滚副本保留、既有服务未重启。全部原始数据见[本轮机器归档](benchmarks/function-registry-returning-20261103.json.gz)；C7/C8失败性能门、最大样本及两份旧归档按原代别保留。

### 研究与生产实现

[PostgreSQL18.6 fmgr](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/fmgr/fmgr.c)用内置定义表查地址，[FmgrInfo/FunctionCallInfo](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/include/fmgr.h)区分可复用函数信息与实际调用数据。[SQLite func.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/func.c)初始化内置定义后只读，[callback.c](https://raw.githubusercontent.com/sqlite/sqlite/master/src/callback.c)先查连接自定义函数；[DuckDB注册列表](https://raw.githubusercontent.com/duckdb/duckdb/main/src/function/register_function_list.cpp)把静态定义注册到真实目录。本轮采用定义复用和实例状态分离，下载来源与SHA保存在本批研究记录；未引入查询结果缓存或新的向量执行器。

原每次 `ExprEvaluator` 构造都重建整套注册表。现在182个不依赖求值器实例的回调及固定volatility定义共享一份进程生命周期只读表，9个依赖真实实例/时钟的回调、5个SQL时间关键字仍由各实例拥有。这里的“只读”不表示SQL immutable：random、clock_timestamp、通知等仍在实际调用时产生效果；age原volatility及全部196条原注册赋值保持。局部覆盖优先、pow/iif的副本别名、真实 `std::function::target` 标记和逻辑191槽计数保留。原公开值结构和求值器字段未增大；实测 `ExprEvaluator=248B`、`ExprValue=184B`，`ExprEvalResult=376B`仍与C8一致。

C2扩展回归后新增的原生重入探针发现真实回退：参数回调替换外层abs/lower时，旧共享表指针会导致多余转换。C3在参数完成后读取实际局部覆盖，再决定转换；同源三个完整结果已恢复到C8，调用次数/类型/值/NULL/元数据均一致。该失败及修复证据保留，不能用原大集合通过宣称C2无回退。

真实 `PreparedReturningExecution` 发布原已求得的拥有值，并在需要时附既有声明；真实目标/OLD/NEW存储数组在物理读取边界物化，已拥有值不从显示文本重建。DELETE的已有BoundDml接收器也转运实际存储数组和引用身份；WHERE/SET默认调用保持，未改图、provider、参数帧、存储格式或协议描述符准入。格式0默认不启用可选拥有值载体。

### 构建与正确性

C1从零编译62对象；C2、C3各实际重编两个CPP、复用60个有效对象。62个 `.o.d`、编译日志、227源码、157头文件与逐代fresh stub均核对，不把增量重编称为每代fresh62。安装、全部时钟和最终回归使用同一C3程序。新增两项永久原生入口共14+20控制，原2322测试字节不改，当前2324文件；新头文件及一个旧头的私有声明调整没有增加字段ABI。

| 实际代别/范围 | 结果 | 限制 |
|---|---|---|
| C2扩展147原生 | 145通过/2失败 | 数学与例程数组入口均同源C8复现；不是全1074入口 |
| C8→C2原95协议 | 85/10→86/9 | C8两超时结果不完整；C2一项启动103/0SQL，不能写成9个完整语义失败 |
| 最终C3选取56原生+新RETURNING入口 | 56通过/1旧数学失败 | 含14项注册隔离与20项拥有值控制，不是C3重跑147 |
| 最终C3选取31协议+独立CASE入口 | 24通过/8保留失败入口 | CASE完整3差异保留，不覆盖C2启动失败 |
| 固定PG18.6 301 | 188匹配/113差异 | 相比C8仅254改善：INSERT RETURNING实际数组/record二进制；原187全保留，相比S3累计88改善 |
| 固定83/111/68/16 | 263匹配/15保留差异 | 全278完整观察对象与实际C8相同，含失败内正确分量 |
| 新RETURNING协议56 | C8 36通过→C3 48通过 | 8个严格正例仍失败；C3全部执行后服务检查正确 |
| 新拥有值原生20 | C8 3通过→C3 20通过 | 覆盖INSERT/UPDATE/DELETE、NULL/空/下界/引用、易变计数、回滚、真实owner销毁与删库后值寿命 |

原95中的完整失败及例程1329前缀共6197观察，6196数据/预期/标记保持；另一个真实重启PID元组分别保留原值并按原谓词验证，不伪称PID相同。C8本轮keycache超时无完整观察，另以相同程序的P2完整143/5逐对象核对C2；不能拿更早143/14混为本批基线。最终C3 CASE补测仍保留三个真实差异。

新协议56仍有六个OLD/NEW缺Describe T帧、一个虚拟数组上游丢拥有值、一个物理历史NULL位图不可恢复。DELETE42二进制值/命令/提交及三个执行后检查已正确，整例仍因缺T计失败。新测试最初两个时钟观察错误在真实Session外读取currval；仅修观察上下文后同源20控制在C8/C3分别17/0失败，首轮原文保留。协议测试的初始错误帧预期与变量遮蔽修正均保留，不修改SQL或正例来刷绿。原生隐式复制的this回调限制、更多目录/CAST/更新输入等普通缺口仍开放，不新增用户暂缓；原273项状态不关闭。

### 实际执行成本

两个同源诊断各分默认分配器与C++new计数器、C8/C3，共八个进程完整通过。e127保留496指标/48真实游标执行/8208行；构造诊断保留52指标，各进程首次一次及12×64构造、302592次公共检查。默认分配器计数字段表示未测量；计数器时间不替代默认时钟。

| 计数区域 | 同批C8 | C3 | 限定解释 |
|---|---:|---:|---|
| 首次显式构造/析构 | 383次/27251B | 384次/27347B | 增加1次/96B；不含进程main前初始化 |
| 每组64构造及25216公共检查，12组相同 | 29760次/1804224B | 8704次/247936B | 每组少21056次/1556288B；不是纯构造CPU |
| 512行额外helper关闭捕获 | 1218134次/88677248B | 213590次/14491520B | 1536次helper及输入maps等诊断，不是查询内部总计 |
| 同helper打开捕获 | 1450070次/126917504B | 445526次/52731776B | 两模式均少1004544次/74185728B |
| 512行实际游标首次执行 | 32390次/3334925B | 相同 | 此区域没有减少分配 |
| 512行同图每次重启，三次 | 32392次/3333277B | 相同 | 同批等长私有路径，不能混比旧工件路径字节量 |

默认构造加公共检查的12组process CPU p50为12.971→4.497ms，首次显式构造CPU却为0.364592→0.370973ms；两者均保留。额外helper关闭/打开捕获的单次process CPU为466.525→56.841ms、635.090→242.069ms。实际游标首次CPU反而20.750→35.862ms，三次重启为16.463→19.739、15.256→16.927、15.279→17.030ms。冷样本和相关重启不能证明稳定倍率，helper收益不当成整个游标或SQL加速；这些累计请求也不是live/峰值/RSS结果。全部发送器format/metadata/owning组合与重复阶段都按原顺序存档，没有用非唯一phase键覆盖记录。

### 35类数据库操作对照

使用原 `benchmark_operations.py`、query helper和协议代码，在三个新私有目录按C8/C3/C8执行；1000行、batch50、DDL5行、3轮×2样本，无预热丢弃、无only过滤，持久化和默认分配器不变。每类每代六次，共630个计时**操作**，不是630条SQL；事务操作含多个语句。设置、结果核验和清理在动作计时外，SELECT按原方法含值检查，原资源快照方法不改。全部动作核验通过，三个正常上下文退出0；旧C9的35操作停机失败继续保留，不宣告R15已修复。

18类p50低于两次C8、2类高于两次、15类居间。update_range为587.678/645.132/642.766ms，C3高9.78%/0.37%；maintenance_reindex为290.912/312.440/297.708ms，高7.40%/4.95%。两次C8漂移−90.28%至+130.91%全部保留，不能据此宣称所有操作加速。105组原始6样本及全部CPU/RSS/I/O快照在机器归档；RSS是原动作后快照，不是峰值证明。

| operation | C8 first p50 ms | C3 p50 ms | C8 last p50 ms | C3/first % | C3/last % | C8 drift % | class |
|---|---:|---:|---:|---:|---:|---:|---|
| select_constant | 3.071 | 1.002 | 1.853 | -67.36 | -45.90 | -39.66 | faster_than_both |
| select_pk_lookup | 17.883 | 17.883 | 27.886 | +0.00 | -35.87 | +55.93 | between_or_equal |
| select_pk_prepared | 33.042 | 24.911 | 40.710 | -24.61 | -38.81 | +23.21 | faster_than_both |
| select_unindexed_lookup | 66.144 | 65.632 | 89.698 | -0.77 | -26.83 | +35.61 | faster_than_both |
| select_range_sum | 127.146 | 132.251 | 140.330 | +4.02 | -5.76 | +10.37 | between_or_equal |
| select_group_aggregate | 66.463 | 76.724 | 81.906 | +15.44 | -6.33 | +23.24 | between_or_equal |
| select_sort_limit | 43.567 | 48.114 | 55.907 | +10.44 | -13.94 | +28.33 | between_or_equal |
| select_join_count | 286.995 | 151.499 | 317.647 | -47.21 | -52.31 | +10.68 | faster_than_both |
| insert_autocommit | 35.048 | 32.895 | 42.974 | -6.14 | -23.45 | +22.61 | faster_than_both |
| insert_multi_values | 298.814 | 214.042 | 258.738 | -28.37 | -17.27 | -13.41 | faster_than_both |
| insert_explicit_transaction | 333.382 | 307.246 | 408.043 | -7.84 | -24.70 | +22.40 | faster_than_both |
| update_primary_key | 72.465 | 88.482 | 100.165 | +22.10 | -11.66 | +38.23 | between_or_equal |
| update_secondary_index | 113.560 | 95.042 | 112.934 | -16.31 | -15.84 | -0.55 | faster_than_both |
| update_cte_returning | 50.289 | 37.721 | 52.945 | -24.99 | -28.75 | +5.28 | faster_than_both |
| update_range | 587.678 | 645.132 | 642.766 | +9.78 | +0.37 | +9.37 | slower_than_both |
| delete_primary_key | 584.143 | 44.665 | 56.764 | -92.35 | -21.31 | -90.28 | faster_than_both |
| delete_cte_returning | 37.343 | 38.266 | 56.594 | +2.47 | -32.38 | +51.55 | between_or_equal |
| delete_range | 449.984 | 476.205 | 526.623 | +5.83 | -9.57 | +17.03 | between_or_equal |
| transaction_read_commit | 23.814 | 17.452 | 22.968 | -26.71 | -24.01 | -3.55 | faster_than_both |
| transaction_write_rollback | 29.765 | 46.369 | 49.348 | +55.78 | -6.04 | +65.79 | between_or_equal |
| transaction_savepoint | 171.665 | 174.284 | 182.970 | +1.53 | -4.75 | +6.59 | between_or_equal |
| sequence_nextval | 20.981 | 20.995 | 31.906 | +0.07 | -34.20 | +52.07 | between_or_equal |
| copy_from | 199.161 | 240.952 | 244.909 | +20.98 | -1.62 | +22.97 | between_or_equal |
| copy_to | 95.721 | 104.159 | 106.415 | +8.82 | -2.12 | +11.17 | between_or_equal |
| toast_insert_6k | 43.574 | 50.086 | 66.619 | +14.95 | -24.82 | +52.89 | between_or_equal |
| ddl_create_table | 1620.671 | 1384.353 | 1823.781 | -14.58 | -24.09 | +12.53 | faster_than_both |
| ddl_alter_add_column | 1592.086 | 1423.369 | 1546.947 | -10.60 | -7.99 | -2.84 | faster_than_both |
| ddl_truncate | 1603.844 | 1391.332 | 1496.437 | -13.25 | -7.02 | -6.70 | faster_than_both |
| ddl_drop_table | 1558.347 | 1487.523 | 1510.660 | -4.54 | -1.53 | -3.06 | faster_than_both |
| index_create_btree | 1799.184 | 1619.982 | 1708.636 | -9.96 | -5.19 | -5.03 | faster_than_both |
| index_create_hash | 1495.600 | 1429.792 | 1509.273 | -4.40 | -5.27 | +0.91 | faster_than_both |
| maintenance_analyze | 413.039 | 395.929 | 422.232 | -4.14 | -6.23 | +2.23 | faster_than_both |
| maintenance_vacuum | 4.616 | 4.457 | 3.281 | -3.44 | +35.82 | -28.91 | between_or_equal |
| maintenance_reindex | 290.912 | 312.440 | 297.708 | +7.40 | +4.95 | +2.34 | slower_than_both |
| maintenance_checkpoint | 22.717 | 32.351 | 52.455 | +42.41 | -38.33 | +130.91 | between_or_equal |

### 三档完整查询对照

所有编译、回归、成本和35操作进程终态后，单独执行一次冻结计划：1000行、原12条SQL、64kB/4MB/16MB，各档S3/C3/S3；每条每代3次未计时预热及12次计时，原15秒超时、默认分配器和持久化设置不变，时钟期间无诊断采样器。1296计时+324未计时预热+18 SET/SHOW=1638请求，完整1620000行/4725000单元格，九个正常上下文退出0。三驱动、1072夹具文件、二进制及227源码前后哈希一致；独立346项核对逐值检查全部1620条JSONL及CSV。

全部36组p50低于两次S3：三档各12/0/0，common24和额外12均全快。相对S3首轮降低2.05%–33.76%，相对末轮降低1.76%–29.81%；这是本夹具描述性结果，没有统计显著性、C8历史时钟因果倍率或PostgreSQL速度结论。S3漂移−22.57%至+11.58%保留。最接近基线的16MB length_lag_1为450.306/441.056/448.952ms；最大旧样本64kB partition_lag=1889.284153ms，最大C3样本16MB length_lag_1=1382.993883ms均保留。

| work_mem | case | S3 first p50 ms | C3 p50 ms | S3 last p50 ms | C3/S3 first | C3/S3 last | S3 drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 171.678 | 145.352 | 152.452 | 0.8467 | 0.9534 | -11.20 |
| 64kB | partition_lag | 142.106 | 107.079 | 124.648 | 0.7535 | 0.8591 | -12.29 |
| 64kB | partition_lead | 160.504 | 106.319 | 124.286 | 0.6624 | 0.8554 | -22.57 |
| 64kB | running_sum | 131.572 | 110.199 | 124.696 | 0.8376 | 0.8837 | -5.23 |
| 64kB | sliding_sum | 124.899 | 116.507 | 122.312 | 0.9328 | 0.9525 | -2.07 |
| 64kB | partition_full_sum | 117.256 | 112.902 | 114.995 | 0.9629 | 0.9818 | -1.93 |
| 64kB | full_first_last_nth | 208.427 | 148.811 | 168.463 | 0.7140 | 0.8833 | -19.17 |
| 64kB | mixed_same_keys | 190.857 | 161.332 | 171.179 | 0.8453 | 0.9425 | -10.31 |
| 64kB | length_lag_1 | 461.894 | 438.326 | 461.685 | 0.9490 | 0.9494 | -0.05 |
| 64kB | length_lag_2 | 486.927 | 449.055 | 481.321 | 0.9222 | 0.9330 | -1.15 |
| 64kB | length_lag_4 | 562.790 | 502.809 | 539.792 | 0.8934 | 0.9315 | -4.09 |
| 64kB | duplicate_running_sum | 141.165 | 127.914 | 140.996 | 0.9061 | 0.9072 | -0.12 |
| 4MB | partition_ranking | 144.783 | 104.582 | 143.745 | 0.7223 | 0.7276 | -0.72 |
| 4MB | partition_lag | 116.667 | 95.738 | 115.347 | 0.8206 | 0.8300 | -1.13 |
| 4MB | partition_lead | 115.404 | 94.597 | 114.070 | 0.8197 | 0.8293 | -1.16 |
| 4MB | running_sum | 114.307 | 94.689 | 116.420 | 0.8284 | 0.8133 | +1.85 |
| 4MB | sliding_sum | 117.111 | 92.784 | 123.353 | 0.7923 | 0.7522 | +5.33 |
| 4MB | partition_full_sum | 108.109 | 94.830 | 107.780 | 0.8772 | 0.8798 | -0.30 |
| 4MB | full_first_last_nth | 158.255 | 110.507 | 157.442 | 0.6983 | 0.7019 | -0.51 |
| 4MB | mixed_same_keys | 159.207 | 110.984 | 154.053 | 0.6971 | 0.7204 | -3.24 |
| 4MB | length_lag_1 | 465.213 | 426.301 | 447.640 | 0.9164 | 0.9523 | -3.78 |
| 4MB | length_lag_2 | 468.127 | 433.906 | 471.642 | 0.9269 | 0.9200 | +0.75 |
| 4MB | length_lag_4 | 526.185 | 459.224 | 527.352 | 0.8727 | 0.8708 | +0.22 |
| 4MB | duplicate_running_sum | 134.579 | 99.376 | 134.913 | 0.7384 | 0.7366 | +0.25 |
| 16MB | partition_ranking | 135.414 | 109.759 | 145.676 | 0.8105 | 0.7534 | +7.58 |
| 16MB | partition_lag | 116.901 | 98.021 | 115.328 | 0.8385 | 0.8499 | -1.35 |
| 16MB | partition_lead | 116.911 | 99.183 | 116.115 | 0.8484 | 0.8542 | -0.68 |
| 16MB | running_sum | 113.261 | 96.878 | 121.499 | 0.8553 | 0.7974 | +7.27 |
| 16MB | sliding_sum | 117.325 | 104.963 | 130.914 | 0.8946 | 0.8018 | +11.58 |
| 16MB | partition_full_sum | 109.064 | 98.808 | 108.916 | 0.9060 | 0.9072 | -0.14 |
| 16MB | full_first_last_nth | 150.576 | 114.396 | 153.217 | 0.7597 | 0.7466 | +1.75 |
| 16MB | mixed_same_keys | 155.803 | 119.104 | 152.549 | 0.7645 | 0.7808 | -2.09 |
| 16MB | length_lag_1 | 450.306 | 441.056 | 448.952 | 0.9795 | 0.9824 | -0.30 |
| 16MB | length_lag_2 | 479.871 | 443.472 | 470.239 | 0.9241 | 0.9431 | -2.01 |
| 16MB | length_lag_4 | 526.277 | 466.166 | 513.580 | 0.8858 | 0.9077 | -2.41 |
| 16MB | duplicate_running_sum | 129.728 | 103.439 | 132.507 | 0.7974 | 0.7806 | +2.14 |

### 复现与发布状态

[机器归档](benchmarks/function-registry-returning-20261103.json.gz)保存全部时钟/预热、630操作、局部成本原日志、113残余与新的56对照、来源/fixture/oracle和各次首次测试故障。准确命令在各执行receipt：`run_registry_C3_cost8_v1.py`、`run_operations35_C8_C3_C8_v1.py`、`perf-resource55-registry-C3-executor-v1.py`、`perf-latency1638-registry-C3-executor-v1.py` 均位于 `build/pg-feature-function-registry-45zctwo9`。复现应取新的输出目录，保留原固定驱动/SQL/参数，不能覆盖首次结果。

资源另行55请求/40000行/118000单元格全部核对：64kB最多两个已删除FD，16MB观察0，EOF/close后剩余严格 `{}`、无命名文件；只读临时排序文件强杀后同私有目录重开值正确，五个正常上下文退出0。它不证明任意写入崩溃恢复、窗口硬预算或RSS峰值。

实际默认程序已替换为受测C3；旧S3位于本机工件 `installed-S3-before-C3-dbms_main`，其SHA仍aa52。未重启既有服务。版本0.2.0/C++17/GCC13.3/O2/ICU74.2/zlib1.3/TLSstub保持；按指定身份在master提交，不新建分支、不push。原273、历史及用户暂缓范围保持，Goal active，继续补齐普通缺口与剩余性能慢项。
