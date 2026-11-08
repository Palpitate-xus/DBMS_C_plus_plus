# 与 PostgreSQL 18 的功能和性能差距

> 历史基准原始数据不随 Git 仓库提供。本文中的 `benchmarks/` 路径仅指可能存在的本地可选证据；新克隆仓库没有这些文件，原始结果只能在另行取得数据后复核。文中的测试与性能摘要仍保留。

## 2026-11-08：内置函数签名缓存

内置函数的固定签名改为只读、进程生命周期的共享声明，避免在绑定和调用时反复创建；数组位置函数的参数相关签名仍逐次生成，同名函数覆盖和参数求值路径保持既有语义。选定回归与异常分配注入的范围见[测试报告](test-report.md#d43-signature-cache-20261108)。

在共享主机上，同项目新旧版本的 64 行查询中，部分函数有改善，但旧版自身漂移较大，不能据此宣称全库提速；逐项结果与限制见[性能报告](performance-report-2026-10-30.md#d43-signature-cache-20261108)。对数精度与零列缺陷仍未修复，详见测试报告。

**待修复的数据正确性问题（D40/D42/D43 实测）：** 高精度 `ln`／`log` 可能返回错误成功值，并可通过 DEFAULT 写入、跨重启保留；零列查询可能漏发实际行，零列表部分读写也失败。D43 的零列反例和对数默认值冷重启问题均已复现；不能将隔离原型当作已完成的修复。详见[对数测试](test-report.md#numeric-log-gap-20261108)与[零列测试](test-report.md#zero-column-gap-20261108)。

## 2026-11-08：十进制指数与浮点函数类型

`exp(numeric)` 改用十进制计算，保留 PostgreSQL 18.6 的结果标度与中间舍入行为；`exp`、`ln`、`log` 和 `log10` 按参数类型区分 numeric 与 double precision 重载。此前 `exp(100::numeric)` 和 `exp(-40::integer)` 的错误结果可能被写入默认值，现有针对这些示例的查询及持久化回归用于防止再次发生。

实现参考 [PostgreSQL 18.6 numeric.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/numeric.c) 和 [float.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/float.c)。[SQLite 数学函数](https://www.sqlite.org/lang_mathfunc.html)通常返回近似值，[DuckDB 的 exp 实现](https://github.com/duckdb/duckdb/blob/v1.5.6/extension/core_functions/scalar/math/numeric.cpp)使用 double；两者不能替代 PostgreSQL numeric 结果作为精度判据。

本项目 Numeric 的有效精度和 scale 仍各限 1000 位，部分 PostgreSQL 可计算的结果返回 `22003`；`ln`／`log` 的高精度数值路径仍有差距，D39 的部分 binary Numeric 输入边界在 D40 尚未重新验证。有限测试不代表全面 PostgreSQL 兼容；六条简单 SQL 的新旧测量波动较大，部分变慢，不能宣称全库提速。详见[测试报告](test-report.md#d40-numeric-exp-20261108)及[性能报告](performance-report-2026-10-30.md#d40-numeric-exp-cost-20261108)。

## 2026-11-08：大数和超越函数差距（D39 历史）

本轮修复部分大 numeric 定点／`V` 格式在展示补零或移位时触及项目内部精度上限的问题；限定的模板与类型现在按十进制舍入。整数长除法改为复用局部余数缓冲区，减少反复分配；不改变 1000 位上限。`exp`／`ln`／`log` 不再把部分超出 double 范围的 numeric 静默当成零，精确的 10 的幂采用逐位验证和范围检查。无法精确支持的部分输入会明确返回 `22003`，不是 PostgreSQL 等价。

**D39 历史风险：** `exp(100::numeric)` 曾与 PostgreSQL 不同，某些 integer／float8 的极小 `exp` 曾错误返回零，这些值可作为默认值持久化。后续实现及当前限制见上方最新状态；D39 并未解决完整十进制超越函数、默认值来源或复杂格式模板。

D39 正常冻结候选的 61 个选定原生入口与 27 组既有协议对照通过，完整套件未运行。针对定点格式的每版本 1,365 个动作涵盖直接查询、事务和冷重启，已测 `FMPL` 默认值与 PG 一致；复杂模板仍有差距。组件基准有 CPU 改善，真实 SQL 窗口有快有慢且漂移明显，不能声称整体数据库提速。详情见[测试报告](test-report.md#d39-numeric-20261108)与[性能报告](performance-report-2026-10-30.md#d39-numeric-cost-20261108)。



## 2026-11-08：数值科学格式与目录差距

本轮修正 `div(NaN, 0)` 的处理顺序：NaN 先于零除数处理；有限值和无穷除以零仍报 `22012`。简单 `9`／`0`／小数点模板的 numeric 和 bigint `EEEE`／`eeee` 科学记数格式改为精确十进制舍入，避免极大或极小值因转换成二进制浮点而静默显示为零。integer 等其他类型仍沿原路径；含 `G`、引号、`FM` 等复杂模板的既有差异未在本轮解决。

Linux 上，目录发布状态的一次扫描复用已打开的父目录句柄，仍逐个检查目录文件的身份和时间戳，并在结束前核对路径仍指向同一目录；描述符不可用时回退旧检查路径，非 Linux 保留旧路径。扫描期间目录换出后又换回等竞态仍不能保证检出，不能称作跨进程线性一致。

D37 正常冻结候选的 58 个选定原生入口实际退出 0；27 组既有协议的 1,105 对回复没有未解释变化。NaN 的 82 个动作在 PG18.6 对照中结果、SQLSTATE 和事务状态一致；简单科学格式的 210 个样例与 PG 的结果及描述符一致。完整套件未运行，其他模板、无约束 numeric 范围、错误回复帧及默认值来源仍有差距。此前文档称 `div(NaN,0)` 不同属于历史结果，已被本节覆盖。详情见[测试报告](test-report.md#d37-numeric-catalog-20261108)和[性能报告](performance-report-2026-10-30.md#d37-catalog-scan-20261108)。



## 2026-11-08：数值与默认值差距

本轮修正部分 numeric 范围错误被 DEFAULT 当作字面量回退的问题：已求值的算术范围异常转换为 `22003`，整数商和余数使用直接整数除法，避免舍入或过大的中间乘积；`to_char` 已绑定的数值格式化范围异常也转换为 `22003`。这些处理仅覆盖对应异常和已测路径，其他无结构错误仍可能回退原文。旧式原生默认值兼容有变化：原文恰为这些会越界的表达式时，现在报错。

PostgreSQL 18 的无约束 numeric 支持的范围远大于项目当前 1000 位内部上限；合法输入、中间值和结果都可能触及项目限制。[PostgreSQL 数值类型](https://www.postgresql.org/docs/18/datatype-numeric.html)。`div(NaN, 0)` 的既有差异、其他函数的未结构化错误回退、目录锁等待和 `pg_attrdef.adbin` 等差距仍未解决。旧 D30 正文对所测算术默认值“写入原文”的描述属于历史结果，已由本节局部修复；不代表所有默认值问题均已修复。

D35 冻结候选 55 个标准原生入口与 1 个二进制入口均实际退出 0；11 组数值和默认值协议对照、27 组 D34→D35 逐动作比较和选定 SSI 对照已完成。`to_char` 大数默认值现返回 `22003`，已测 INSERT、UPDATE、批量插入和 ADD COLUMN 不再写入 SQL 原文；PostgreSQL 对该合法输入返回格式化值，仍不等价。未运行完整测试套件。详细计数、旧失败与性能测量限制见[测试报告](test-report.md#d35-numeric-tochar-20261108)及[性能报告](performance-report-2026-10-30.md#d35-numeric-tochar-performance-20261108)。



## 2026-11-08：D30 目录与事务差距

本轮对普通 `pg_attrdef`／`pg_depend` SELECT 使用实际目录字段的按需快照，声明与数据在同一把锁下复制；公共完整快照仍保留默认表达式和依赖。`pg_attrdef.adbin` 的 PostgreSQL 节点树仍未实现，不声称目录完整等价。活动事务与 SSI 登记改为进程寿命，避免全局数据库对象退出时访问已析构的登记容器。

D30 实际通过 52 个选定标准原生入口及 1 个独立二进制入口，合计 53；23 组协议与 D28 的 994 条成对回复在行、tag、SQLSTATE、Ready、T7 和错误／通知上相同。针对投影的 PG／D27／D28／D30 热冷对照共 76 条回复，按实际 OID 对应后选定行、tag、SQLSTATE、Ready 无差异，但 PG 原始行描述符 T7 仍有 4 项差异。完整发现审计的 2,023 条回复包含历史和参照，不能说成 2,023 条新测试；未运行完整测试套件。

已知缺陷仍未修复：合法默认表达式 `(('1e500'::numeric * '1e500'::numeric)::text)` 在 PostgreSQL 18.6 得到 1001 位结果，而 D27／D28／D30 会把 SQL 源文写入 TEXT；现有无约束 numeric、持久默认值来源和无 SQLSTATE 回退均需继续修复。并发对照中 PostgreSQL 可在另一会话 DDL 未提交时立即读取旧目录，D27／D28／D30 的同类查询等待超过 5 秒，ROLLBACK 后才返回；未采集线程等待栈，库级锁路径只是依据代码和时间顺序的推断。

仅对选定单连接暖缓存工作负载测量：D27→D30→D27 固定顺序的目录读取窗口为 27 个实际正常退出进程、27,648 条 SQL；事务窗口为 9 个进程、3,456 条计时 SQL，另有 324 条控制 SQL。两窗口完整回复字节一致、无 mismatch。各场景结果有正有负，ordinary `pg_attrdef` 相对前／后基线墙钟分别退化 5.8%／6.5%，两项生产变更同时存在且每角色仅 3 次，不能据此归因或证明投影改善，也不能宣称整体提速或与 PostgreSQL 性能等价。

设计与差距参考 [PostgreSQL 默认值](https://www.postgresql.org/docs/18/ddl-default.html)、[pg_attrdef](https://www.postgresql.org/docs/18/catalog-pg-attrdef.html)、[numeric 范围](https://www.postgresql.org/docs/18/datatype-numeric.html)、[SQLite DEFAULT](https://www.sqlite.org/lang_createtable.html) 和 [DuckDB v1.4.1 默认值绑定实现](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.1/src/planner/binder/statement/bind_insert.cpp)。SQLite 与 DuckDB 行为不等同于 PostgreSQL。


## D27 DEFAULT 错误对照与限制（2026-11-08）

本轮修正 INSERT 遗漏列 DEFAULT 求值中已有 SQLSTATE 或有效结尾 SQLSTATE 标记的错误传播，并对 substring 负长度保留 `22011`；目录声明读取按需省去 DEFAULT 对象图复制，公共默认快照仍完整。没有 SQLSTATE 且没有有效标记的旧/native 求值失败仍可退回原始字面量；域、旧数据及全部错误类型不构成全面兼容承诺。

D24 的 `catalog_default_snapshot_demand_test` 实际退出 `-11`，诊断指向进程退出时内置目录声明生命周期问题；D27 的 ASan 只注入运行时库而非全核心插桩，该控制正常退出 0，不能称为完整 ASan 构建。D26 的 `regexp_replace('abc','[','z')` DEFAULT 在热态和冷重启后均错误写入表达式文本；D27 返回 PostgreSQL 的 `2201B` 且未写入行。历史失败记录保留。

最终 D27 49 个常规原生入口加 1 个二进制控制，共 50 个实际退出 0；22 个协议族独立审计覆盖 1,998 SQL、76 个正常结束 Main。五族 DEFAULT 错误独立对照记录 342 SQL、48 个正常结束 Main，所比较语义字段没有候选差异；两个族的原始行描述符 T7 仍有差异，不能称为完整 wire 或 PostgreSQL 等价。未运行完整测试套件，功能仍为未接受草稿。

指定成本测试为共享服务器暖缓存、单连接、32 行热表：D23 与 D27 按每场景旧版前/新版/旧版后各 3 个进程固定顺序执行，共 27 个正常结束 Main、36,720 条 SQL；各场景完整回复字节一致。24 组墙钟和 CPU 结果有正有负，narrow 前后基线波动尤大；没有证明整体加速，也没有 PostgreSQL 速度对比。 本次对照只说明所列语义字段，并不证明 PostgreSQL 全功能、协议完全等价或生产性能差距。 [报告](performance-report-2026-10-30.md#default-runtime-snapshot-evidence-20261108)。


## 最新 P43/D23（2026-11-07）：DEFAULT 真实对象依赖

依据PG18.6实际运行和固定版本源码实现对象常量早绑定、默认对象独立身份、所属列自动依赖及正常引用。50条主对照在实际目标OID对应后，返回值／标签／SQLSTATE／Ready无差异；扩展32条仍保留晚绑定序列缺失错误码和删列目录占位差异。完整T7另列，不能从这些用例推导全库兼容。

46个选定原生入口及两份正式协议脚本通过。原273项保持22 complete／166 partial／70 unverified／15用户暂缓，292条历史保持。P43未接受，正常性能NOTRUN，目录全行／通用表达式依赖与其他非暂缓缺口继续推进。[证据与边界](performance-report-2026-10-30.md#attribute-default-objects-draft-20261107)。


<!-- P43-D20 unaccepted draft checkpoint -->
## 最新 P43/D20（2026-11-07）：普通旧数组目录取得有限闭环

本轮对照真实PG18.6的类型／字段目录和内建类型省略依赖的处理，完成有实际物理schema佐证的普通表数组升级；保留已存在的正常类型边并调整其目标，不补造缺失的内建依赖。真实旧库21条操作链的值、NULL、事务状态和tag一致，按各自真实表OID对应后完整7字段T7也一致；这只覆盖选定用例。

原273项保持22 complete／166 partial／70 unverified／15用户暂缓。REGCLASS默认依赖、完整序列与恢复、目录全行、其他非暂缓功能及性能差距仍开放；正常成本未测，不能据此缩小整库速度差距。[结果和未完项](performance-report-2026-10-30.md#physical-array-catalog-upgrade-draft-20261107)；下方D18及更早数据为对应历史版本。


<!-- P43-D18 unaccepted draft checkpoint -->
## 最新源代码检查点：P43/D18 仍未接受（2026-11-07）

目录／类型有实际进展：保存真实 `pg_type`／`pg_attribute`／`pg_class` 声明和系统属性元数据，110个已有内建类型的有限物理属性投影与实际PG参照一致；6个目录字段改用真实 `C` 排序规则，选定 `pg_type` 别名／CTE的完整来源描述匹配PG。元数据声明不等于实现全部值、堆格式或SQL系统列；未提供真实值生产者的字段仍拒绝，不能把短schema或虚构NULL称为完整目录。

原273项仍为22 complete／166 partial／70 unverified／15用户暂缓，未关闭任何项目。REGCLASS依赖、旧数组迁移、序列完整语义和恢复、生成列ALTER TYPE、错误阶段及其他非暂缓功能仍需补齐；本批正常性能未测试，无法据此缩小整库性能差距。版本、全部观察及边界见[草稿报告](performance-report-2026-10-30.md#identity-sequence-catalog-draft-20261107)。下方性能数字保留对应历史程序口径。


## 最新 P42（2026-11-07）：读取提交成本的局部进展

参考PG18.6未分配持久XID／未写WAL的提交分支和DuckDB ChangesMade检查，为本项目已证实无持久事务变更的读取消除无用COMMIT／CLOG同步；项目仍在BEGIN分配内部保留ID，尚非PG完整惰性XID／事务实现。95条SQL在实际PG／旧／新程序完整跑完，模型值／NULL／tag／事务状态正确，旧新请求回复逐字节一致；映射实际items OID后仍保留8处catalog或未映射TEMP／aux来源观察，不称为全PG协议等价。

621次正常成本与全部负向、33项选定原生中的2个未通过项详见[报告](performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)。这是本项目两版对照，没有新增PG速度基准；原273状态仍22／166／70／15，Goal active，继续补齐普通功能和性能。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## 2026-11-07：长VARCHAR接近PG，功能和性能仍partial

新增VARCHAR逻辑字符长度元数据：显式长度支持1..10485760字符，未指定长度使用独立的无界标记，名义物理dsize仍不超过65535。列声明、pg_attribute／RowDescription、普通写入／更新、数组元素、domain／generated赋值、ALTER及项目逻辑导出使用真实长度。标量不再把65535字节误当成字符上限；已验证70000字符／245000字节UTF8和1MiB无界值。内部无界字节上限为1073741819，但没有实际验收1GiB值或整个PG字符类型族。CHAR旧限制、完整encoding／collation语义仍开放。

参考[PostgreSQL18字符类型说明](https://www.postgresql.org/docs/18/datatype-character.html)及[固定varchar.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/utils/adt/varchar.c)的字符计数、上限和无界语义；[固定hio.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/heap/hio.c)的bulk-insert当前缓冲与目标页说明了保留搜索位置的设计。这里仅实现每个长值的局部游标，并保留串行物理布局证明。[SQLite](https://www.sqlite.org/datatype3.html)和[DuckDB](https://duckdb.org/docs/stable/sql/data_types/text)通常忽略VARCHAR(n)长度，不作为PG有界语义依据。PG／SQLite原HTTP字节及固定PG源码、响应头与SHA保存；DuckDB直接抓取HTTP403的响应也保留，另存官方网页提取结果，不将它说成HTTP200原文。未进行跨数据库速度排名。

新141条VARCHAR SQL在独立PG18.6、旧bf9、候选C6均完整执行，三角色共423条核心SQL、305行／1249单元／132 NULL、228,538,937字节；10个正常Main0及2个预期SIGKILL -9。全值、顺序、重复数、NULL、状态／tag／Ready和全部七个描述字段与实际PG核对，关系OID只用各自真实探针映射。C6普通数据要求通过，仍有17个pg_class来源描述差异，以及3个大常量ADD DEFAULT的PG实现限制差异：PG对70000字节常量报54000，随后读取／DROP列报42703，项目接受并正确存取；这些差异保持开放，strict PG=false。137条V1参考模型的4个假设被真实PG否定，首次actual1及全部原始数据保留；141条V2在任何候选执行前冻结，另加4097字节默认值成功控制，没有按候选结果降低要求。

旧typeMod=-1按原dsize字符上限读取，不从65535猜测成无界；新显式正长度和私有-2保存在已有ITM1扩展，-2输出PG时为-1。旧数据可以由新版读取；旧程序没有识别这些新VARCHAR修饰符，不能承诺降级读取新写的schema。旧无界意图缺少可判定来源时不自动迁移，须明确新DDL／ALTER。名义dsize、SQL字符数和实际UTF8字节数分别处理。没有改变元组布局、校验、TOAST指针格式或WAL格式；仍是项目自定义存储，不是PG物理格式等价。

本批功能有进展，长值写入部分提速；全面提速尚未成立。新增1MiB混合插入墙钟中位1801.508072ms，相对旧bf9首测／同旧回测-10.720%／-10.799%；更新RETURNING 411.759537ms，-13.372%／-12.280%；重新插入1431.222130ms，-9.197%／-10.414%。对应服务器CPU分别-12.689%／-12.822%、-20.934%／-21.267%、-12.666%／-12.762%。

负向保持OPEN：新长值DELETE RETURNING墙钟43.553088ms，+9.954%／+23.830%，服务器CPU+67.962%／+39.067%；首次VACUUM墙钟25.753918ms，+22.574%／+1.265%；FULL墙钟218.226818ms，+32.204%／+21.215%，服务器CPU+7.756%／+1.774%。新增完整值读取墙钟+2.080%／-2.014%，结果混合。通用270项矩阵的30个区域，墙钟6低于两旧／18高于两旧／6居间；服务器CPU10／8／12，客户端CPU6／14／10。没有排除、重采或把漂移归因为已证实的产品原因；全部负向与原因继续开放。

原273状态不变，新增partial历史291；无PG性能排名。[完整报告](performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 2026-11-07：FULL开销有限改善，仍非PG功能／性能等价

普通VACUUM FULL在备份前与重建后，只刷新目标逻辑关系及其分区／TOAST缓存，避免把其他表的脏页一并刷出。物理文件已同步，且在任何重建写入前全部unlink：这些文件优先用硬链接保留旧inode作为回滚映像，失败则完整复制；schema和specialized-dirty元数据继续独立复制。每份备份的fsync、完整大小／模式／SHA256校验，以及manifest、pending、替换文件／schema、WAL、COMMIT和undo的持久化顺序全部保留。本批只改TableManage.cpp的vacuumFull，没有修改全局缓存、index_file、WAL格式、CRC或恢复授权规则。

参考[固定PostgreSQL18 bulk_write.c源码](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/smgr/bulk_write.c)的关系fork同步与批量写入/WAL安排；[SQLite原子提交说明](https://www.sqlite.org/atomiccommit.html)说明恢复日志、数据页与提交标志之间的持久化顺序；[DuckDB single_file_block_manager.cpp](https://raw.githubusercontent.com/duckdb/duckdb/main/src/storage/single_file_block_manager.cpp)的WriteHeader在发布新header前后同步。三份本批HTTP200原文、响应头及SHA256均入证据，DuckDB main以保存的快照SHA为准。这些资料支持限定刷盘范围及保留提交屏障的设计判断；本项目仍使用自身备份／代次协议，没有实现三个数据库算法等价，也没有进行三库速度排名。

本批只证明固定字段FULL墙钟低于两侧旧版：-11.57%／-1.51%；单长／多长FULL服务器CPU均较低，墙钟各为-3.35%／+1.84%、-1.35%／+3.87%，结果混合。没有证明数据库全面提速，也没有追回P39相对更早84bb的全部恢复成本。

明显负向保持OPEN：首次普通VACUUM多长字段墙钟C1中位67.010218ms，旧c8ca首测30.951480ms／回测25.722458ms，即+116.501%／+160.512%。原三次样本分别为C1 [71.510152,26.679347,67.010218]ms、旧首测[35.992784,30.951480,25.821788]ms、旧回测[25.722458,23.927581,29.365169]ms，全部保留。单长字段大批重新插入的墙钟也+51.70%／+58.00%。未因负向重跑、排除样本或更改预期。

本批仍只验收未来普通关系FULL的有限重写／关闭／强杀／回滚恢复。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际损坏库继续保留。跨FULL的PITR／物理基备未验收；数据库排他锁范围仍大于PG表级锁，未验收分区FULL冷开或高并发等价。硬链接需要文件系统支持，实际失败会走复制回退；不宣称所有平台／文件系统已验证。原15项安全／TDE及额外用户暂缓范围保持，这六种FULL故障不是已暂缓的通用IO／分配器／保存点矩阵，也未恢复执行图／child-provider或retained planning／portal工作。

原22、62、72、28条SQL及实际wide OID控制全部重新执行，共185完整请求／2588行／11613单元／969NULL／45,817,233请求回复字节，8个正常Main0和1个预期SIGKILL -9，2315个闭库文件核对。全部完整请求与回复逐字节等于P39真实c8ca观察，再由独立解码器检查完整值／NULL／全部T7／错误／NOTICE／tag／Ready。原PG18.6参照逐字节复用，未声称本批新跑PG；62条中的9项、72条中的7项、28条中的3项SQL子分区差异继续开放，strict PG=false。

原273状态不变，新增partial历史290；下方旧批按历史程序解释。[完整报告](performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 2026-11-07：普通FULL恢复取得有限进展

修复普通VACUUM FULL仍沿用旧物理身份、冷启动把旧页镜像重放到压缩后堆的问题。每次重写分配新物理代次；同一内部事务记录出生、旧代次退休意图／完成及COMMIT。替换前先持久化只属于该关系的原文件、完整模式／大小／SHA256清单及pending记录，替换文件与schema全部落盘后才提交。失败和未提交崩溃先恢复旧文件／schema，再重放WAL；已提交只清理备份。完成回滚的durable receipt防止遗留备份覆盖后续已提交DML。旧09 schema仅在FULL成功时发布新代次及专门的0x23完成记录，普通旧名称重用仍拒绝错误归属。没有放宽CRC、物理身份或页缺口校验。

参考[固定PostgreSQL18源码cluster.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/commands/cluster.c)中的新堆／文件交换与事务索引重建，以及[PG18 VACUUM](https://www.postgresql.org/docs/18/sql-vacuum.html)对FULL的ACCESS EXCLUSIVE锁与额外空间说明；[SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html)通过临时库及rollback journal／WAL替换保证恢复。[DuckDB2.0 VACUUM文档](https://duckdb.org/docs/2.0/sql/statements/vacuum.html)不支持FULL，并将ART回收放在checkpoint存储维护中。前三份原文／HTTP200／响应头／SHA已保存，DuckDB原文和原始HTTP200 receipt从P38逐字节复用。本实现采用本项目的每关系备份与代次方案，没有实现三库算法等价或三库速度排名。

本批没有证明全面提速。270项原样读写矩阵保留27个全新数据库、54个正常Main及1098条完整回复。30个区域的墙钟：7低于两旧／13高于两旧／10居间；服务器CPU：5低于两旧／14高于两旧／11居间；客户端CPU：7低于两旧／12高于两旧／11居间。所有90项比较均列出，性能原因OPEN。 vacuum-full-fixed墙钟 +64.55%／+78.39%；vacuum-full-fixed服务器CPU +10.12%／+10.45%；vacuum-full-single墙钟 +78.30%／+112.77%；vacuum-full-single服务器CPU +17.97%／+38.94%；vacuum-full-overflow墙钟 +131.53%／+114.90%；vacuum-full-overflow服务器CPU +18.05%／+13.79%（相对旧84首测／同旧回测，正数为更慢）。

本批验收只覆盖未来普通关系FULL的有限重写／关闭／强杀／回滚恢复情形。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际失败库仍保留。跨FULL的PITR／物理基备重建未验收，不能仅凭新生命周期记录重建重写后的完整堆。内部事务暂时使用数据库排他锁，再取表锁和cache锁，并非PG表级并发等价；热态分区既有回归通过，不宣称分区冷开闭环。原始15项安全／TDE暂缓与额外用户暂缓记录保持；这六项不是已暂缓的通用IO／分配器／保存点故障矩阵，也未修改执行图／child-provider、retained planning／portal或CPU-sort interrupt。

原62、72、28条SQL及一条实际wide OID控制在本批C1重新执行，共163请求、458行／1800单元／231NULL／33,383,364字节，6个正常Main0和1个预期SIGKILL -9。所有完整请求与回复均与P38实际84bb逐字节一致，独立第二解码器核对全部值、NULL、T7、错误、NOTICE、tag和Ready；复用的是已认证归档中的真实PG18.6参照，没有本批新跑PG的声明。原62的9项差异、72的7项差异及28的3项SQL子分区差异继续保留，strict PG=false。

原273状态不变，新增partial历史289；下方旧批的OPEN说明保留其历史身份。[完整报告](performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：TEXT容量进展与仍存在的恢复／性能差距

修复标量TEXT把持久化名义宽度65535误当逻辑值上限的问题：统一的字符字节限制函数对非数组TEXT使用1,073,741,819字节上限，并让旧式UPDATE校验也使用该函数。既有TOAST读回、插入和行构造因此一致处理完整长值；表结构dsize仍为65535，内联引用和存储格式不变。实际验证到1MiB，未进行接近1GiB的端到端容量验证。旧数据可由新程序读取；旧程序不能读取新写入的超64KiB TEXT，不能据此承诺降级后的值兼容。

原62条SQL重新用旧版和当前执行，共124请求、4个Main正常0，完整请求与回复逐字节一致，真实PG18.6原始oracle和既有9项差异保持。P37原28条维护SQL计划字节不变，另在真实PG／旧版／最终C2重新执行，共84核心请求、144行、553单元、129NULL、2,873,498字节、6个Main正常0；额外4条实际OID／PG版本控制单列。原TEXT70003插入、立即读回、冷开读回3个确切缺口现与PG完整值和T7相符，其余25条旧／新完整回复字节不变。28条中24项值／状态／tag匹配、1项按各库真实正OID核对，仍有SQL子分区创建0A000、子表目录身份及冷开身份缺行3项差异，strict PG=false。

本批完整1500次正常计时没有证明全面提速。仅ID查询服务器CPU比旧版首测／回测高1.78%／1.86%；单长字段首次VACUUM CPU高10.98%／13.76%，墙钟高3.66%／9.08%。多长字段首次VACUUM墙钟低18.80%／高3.52%，CPU低0.33%／高3.27%，属于混合结果。全部负向、旧版漂移、首轮与长尾保留，原因OPEN；本批交付为TEXT容量修复，没有用局部结果宣称整体性能改善。

新发现的普通VACUUM FULL／WAL恢复缺陷仍OPEN，属于下一项必须处理的非暂缓工作。原始旧模式22条矩阵，两版各完成初开16条并正常退出0；当前16条全值正确，但之后冷启动失败，两版各6条冷开SQL未运行。旧版首次观察器的cleanup断言遮蔽了已退出子进程的wait状态，因此其冷开actual exit未记录；修正观察器后只继续原先未启动的当前角色，实际冷开退出-6，未重跑或补写旧版状态。提前运行的独立审计也因缺少完整receipt实际退出1，失败保留。独立CRC32C核对定位LSN6784528的有效8192字节页镜像：cv_sig、物理身份3、旧块22；FULL后同身份的堆仅18页，重放拒绝跨越4页的缺口。CRC为0xd116b066，名义行宽0是合法format2而非本次原因。本批未修改恢复代码；需要正确的重写物理代次／恢复边界，不能靠忽略错误或无界补页关闭。实际失败快照和正常旧模式混合长值种子均保留在本地。

参考PG／SQLite／DuckDB的逻辑长度与表示约束，原文存档，没有三库性能排序。原273项状态不变，历史288为partial，Goal active。[完整报告](performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：维护安全修复与仍有差距的成本

修复普通 VACUUM 的长字段引用收集：在原有页锁和 pin 内，直接读取所属物理元组的 NULL 位图、变长字段目录和有界字节跨度，不再通过可能读取调用者 RID/NULL 状态的执行器取值接口。固定区大小与变长列索引每次维护预先计算，普通内联值无需复制整行或逐值构造字符串。活动快照保护、所有物理存活槽的遍历、锁／pin、索引／分块校验和回收策略保持。

本批1500项完整正常计时没有证明全面提速。在共同的真实after-DELETE种子上，单长字段首次VACUUM服务器CPU比旧版及旧版回测低8.36%／7.73%；多长字段首次VACUUM墙钟高9.83%／27.97%，CPU分别低1.92%／高35.29%。在共同after-UPDATE种子上，仅选ID的CPU高8.43%／8.39%，墙钟高9.77%／14.77%；三个全列查询墙钟也高于两次旧版，完整正负结果保留，原因OPEN。

本批实际PG缺口包括：TEXT70003字符INSERT在产品返回22023而PG成功，立即读取与冷开读取缺该行；SQL `CREATE TABLE ... PARTITION OF` 返回0A000，子表目录身份及冷开身份读取缺行。原生分区物理存储控制不代表这项SQL功能已实现，也不代表逻辑分区扫描已通过。

PostgreSQL所属tuple值／NULL及SQLite溢出页引用作为机制参考，原文存档；没有PG速度对比。新28条SQL中21项值／状态／tag相符、1项实际OID分别核对、6项差异保留，原62条中的9项差异仍在。功能总账273项状态不变，历史287仍partial，Goal active。[完整报告](performance-report-2026-10-30.md#vacuum-physical-references-20261107)。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：普通查询执行开销进一步降低

扫描展开长字段时，借用该物理行已捕获的 NULL 位图；排序后的投影在核对真实来源、具体 Sort 类型和全部物理列声明后，借用排序已保存的位图。借用仅存在于一次取行调用内，退出与异常均恢复原绑定；表结构不匹配、Sort 子类及其他来源继续原路径。没有新增页／allocator 缓存或改变文件身份检查本身。

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

原样 270 操作矩阵的 30 个区域，墙钟 19 低于两旧／2 高于两旧／9 居间，服务器 CPU 为 12／9／9，客户端 CPU 为 9／16／5。单长字段 VACUUM 墙钟高约 22.90%／28.87%，多长字段 VACUUM 高约 19.10%／4.85%；另有 9 个区域服务器 CPU 高于两旧，全部保留，原因仍 OPEN。

PostgreSQL tuple slots、SQLite排序记录取列及DuckDB validity mask作为机制参考；没有三库速度对比或完整PG等价结论。原273项状态不变，新增历史记录仍partial；TOAST既有断言与原9条PG协议差异仍OPEN。[完整对照](performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
<a id="view-column-dependencies-current"></a>
## 2026-11-06：进一步补齐静态视图依赖（P35-C4）

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

28 条真实依赖删除得到 2BP01，29 条普通事务／保存点的值与状态匹配 PG；201 条主矩阵、42 条真实旧库升级、13 条整表／视图删除和 12 条编译目录视图仍完整保留。主矩阵 3、旧库 5、编译目录别名 1 条运行差异仍 OPEN；完整声明／诊断并未归一化成 PG 通过。普通 pg_attribute 89 条调研已保存，SQL 来源尚未补齐。

原 273 项和 22／166／70／15 状态不变，新增历史记录为 partial；没有关闭整个 CAT-02/06/07/10/16 家族。正常 270+27 操作表明本轮没有整体提速，创建视图墙钟更高，负向原因继续跟进。[完整结果](performance-report-2026-10-30.md#view-column-dependencies-20261106)。本批范围是普通 SQL 的静态 DDL 关系／列依赖。视图来源 T7 的零 origin、列别名运行时重绑定、部分嵌套视图、字面量／envelope 运行时、含歧义历史的旧视图、物化视图、函数／类型依赖及大量视图的扩展性仍有缺口；普通 pg_attribute 查询尚未补齐。用户暂缓范围保持，未修改执行图／child-provider、portal 或通用故障矩阵。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
<a id="projected-toast-demand-current"></a>
## 2026-11-06：投影需求读取的有限进展

调研 PostgreSQL tuple slot、SQLite Column 和 DuckDB 投影下推后，直接物理部分列查询采用需求字段解码。仅取 id 的单长字段表墙钟相对旧首测/旧回测为 -54.47% / -47.06%，多长字段表为 -61.28% / -61.63%。全列仍使用原路径；所有写入、维护和普通 SELECT 的完整对照及负向见[报告](performance-report-2026-10-30.md#projected-toast-demand-20261106)，没有 PostgreSQL 速度等价结论。

原严格协议 9 项差异和额外 41 查询中的 24 项差异仍开放；`pg_class` 仍为 34 列声明/7 列可执行值的有限实现。总账状态保持 22 complete/166 partial/70 unverified/15 deferred_by_user。本次只增加部分进展，未关闭 QRY-01、QRY-13 或 STO-06。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
<a id="pg-class-declaration-current"></a>
## 2026-11-06 pg_class declaration: implemented subset and open gaps

The accepted candidate is normal CMake C++17/O2 C2 `280ad4a8`; the first C1 is rejected and retained as evidence. This is a partial catalog feature, with no speed improvement or full PostgreSQL compatibility claim.

Primary design references at the pinned PostgreSQL18.6 source commit: [pg_class.h](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/catalog/pg_class.h), [pg_type.dat](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/catalog/pg_type.dat), and [AclItem layout](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/utils/acl.h). Declared identities/layout are kept separate from this engine's implemented value capabilities.

| Area | Actual evidence | Remaining work |
|---|---|---|
| Owned declaration | One real class1259;34 continuous attributes; rowtype83/array273; actual field-type closure. Persisted34 rows match PG on six declaration fields each. | No physical catalog heap/MVCC; the `pg_attribute` SQL route remains42P01. |
| Type declarations | Five actual type rows match PG on nine identity/layout fields each; `pg_type` SQL returns those rows. `aclitem` is16 bytes/alignment `d`. | Value/result codecs, including NULL, are unsupported0A000; no ACL semantics are added. |
| Ordinary pg_class queries | Self identity is now present; seven values remain executable. All60 other replies in the old62-request pair are byteexact. | Source origins,27 value columns and wildcard are unfinished. New41 strict PG reference passes; product17/41 match,24 differ. |
| Persistence/lifecycle | Real old-program database copy upgrades, then restarts normally;20 replies equal fresh C2 bytes; prior class/type/attribute rows remain exact. | This finite normal-path evidence does not close generic IO/OOM/savepoint fault matrices. |
| Ordinary SQL cost | One complete270-timing cohort,27 DB contexts/54 Main normal0, all raw data retained. | Wall medians13/30 and CPU16/30 are above both old medians; no general acceleration claim, causes remain open. |

The first candidate's12-byte `aclitem` and NULL output labeled TEXT25 are rejected, retained observations. They are corrected by C2's16-byte declaration and explicit unavailable-input error. Raw native qualified quoted table DDL has four unchanged failed checks in the initial fixtures; the corresponding normalMain quoted DDL passes. These API observations are kept separately. Original273 ledger states and user deferrals are unchanged. [Performance and artifact report](performance-report-2026-10-30.md#pg-class-declaration-20261106).
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 普通日志追加与实际查询成本（2026-11-06，P32-C1）

正常CMake C++17/O2、旧版→C1→同旧版各三轮的固定矩阵完整结束：27个数据库上下文、54个正常Main、270个计时区间，校验1098响应、96624行/386388单元/27864个NULL。30案例的服务器CPU为16项低于两旧、6项高于两旧、8项居间；墙钟为13低/5高/12居间。固定/单BYTEA直接仅id查询有有限收益，但多LOB直接全列和仅id的服务器CPU均高于两旧，其中仅id墙钟也回退，全列墙钟居间。全部负向、旧回测漂移及长尾保留，未证明一致SQL提速、稳定吞吐或PG速度等价；原因仍OPEN，P30原负向证据保持。

原58个严格协议目标及4个额外冷锚控制全部保留，两代产品各完成62请求、目标各53/58，原9项普通差异未关闭，strict CLI仍为1；全部62请求/响应原始字节一致。每代实际207行/848单元/156个NULL，4个初始/冷Main均取得真实子进程wait0、PID/组消失。复用此前真实PG18.6参考，没有重跑PG，没有把目录来源元数据或选中错误的TEZ/EZ差异改成PASS或暂缓。

本批优化本地普通日志路径，不是PG性能对照；未执行新的PG参考或关闭catalog/CAT02、IDX-07与其他未完成能力。日志字节回归通过不等于全部SQL或事务恢复等价，用户暂停范围保持。[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与可逆原始证据（可选本地文件 `benchmarks/legacy-log-append-20261106.json.gz`）。以下历史保留。

<!-- CURRENT_ORDINARY_PROJECT_BITMAP_20261106 -->

## 当前行NULL位图复用与按需求捕获（2026-11-06，P30-C2）

普通物理表的直接投影现在按选中列需求，复用当前行已有的完整NULL位图；局部作用域在正常或异常退出时恢复，只选非空物理列时不再复制。既有值提取、虚拟列求值顺序、TOAST处理、文件身份校验函数和暂停范围保持。

正常CMake C++17/O2的最终C2，在固定/单BYTEA/多LOB三类直接完整查询中，墙钟中位数对前后两次旧版降低约9%–32%，服务器CPU降低约11%–35%；这是本机各三样本的有限结果。主键投影负控仍有回退，固定表服务器CPU为+26.19%/+25.84%，单BYTEA主键查询墙钟为+8.03%/+3.65%。跳过多余复制没有消除这些回退，其根因未证，不能据此归因频率、负载或噪声，也不能称全库提速。

原273项仍为22 complete/166 partial/70 unverified/15 deferred_by_user，`IDX-07`保持`unverified`且`evidence=[]`。完整PG参考58/58；三代产品均完成全58目标与4冷控制，目标各53/58，另4冷锚控制仍失败。原9项目录/普通错误帧差异完整保留、没有改为PASS或新增暂缓。仅新增1个正式CPP回归；whole58 known-gap协议草稿未运行、未登记。Goal active，默认程序未替换，无服务重启、分支切换或推送。

完整原始分布、既有差异、许可和验收边界见[本批性能报告](performance-report-2026-10-30.md#ordinary-project-bitmap-20261106)与可逆压缩证据（可选本地文件 `benchmarks/ordinary-project-bitmap-20261106.json.gz`）。以下P29及更早内容为对应代别的历史记录。

<!-- CURRENT_MULTILOB_20261106 -->

## 多个大字段存储与真实命令结果（2026-11-06，P29-C2）

原七列建表同时含两个 BYTEA 和两个 TEXT 时，类型逻辑上限之和会超过堆头的 uint32 字段。现在仅对 format 2 中这种确实含变长列且最小实际元组可装页的溢出定义，将堆头名义宽度记为 0；可表示的旧宽度、列上限、实际变长元组和 TOAST 格式保持。UNLOGGED 初始化分叉在原有维护入口校验自身宽度，不匹配时重建。执行结果另显式标记无 RETURNING 的计数命令，普通 WITH DML 也据实际执行结果发送命令帧。

原19项定义/排序规则诊断在C1及C2均完整通过：每代父进程2154项检查，两个工作进程合计604项（452+152）。原失败发生在旧七列建表前置阶段，不能把它写成19项全已执行。另C1/旧P28C4双代12工作进程共2662项检查、6组真实冷重启RID对照全过；C2正式物理生命周期用例为10工作进程2483项、父进程17809项，覆盖真实大值/NULL/RID、备份恢复、分区、UNLOGGED及维护路径。以上是不同固定夹具和进程角色，不能相加冒充全仓测试数。

固定SimpleQuery文本格式0的37+3条、七列全值/NULL/十六进制BYTEA/列描述七项/命令与Ready检查，PG18.6首次40/40，C1为38/40，C2为40/40；C2自包含正式协议用例另一次同40控制也通过。C1保留的两项是DELETE多发空RowDescription帧，行值/NULL/状态/命令原本正确。C2按真实DML命令角色修正该帧形状，并保留SELECT/RETURNING结果形状；没有靠搜索字符串中的RETURNING判断。PG与本地分别读取真实关系OID进行来源锚定，不要求两个数据库的原始OID或整段原始协议字节相同。

原constraint154协议回归在C2完整通过。独立结果形状mini21参考PG21/21，C1为13/21，C2为19/21，仍保留两个普通缺口：零列一行SELECT缺少一个零列DataRow；零列表RETURNING *的错误代码实际42P01而PG为42601。它们未被改为PASS。

两个产品专属普通WAL角色共实际12条动作：COMMIT确认后强杀与未提交UPDATE确认后强杀，各自退出为预期-9，随后正常Main首先打开同一目录完成恢复并正常退出0；没有由原生读取器抢先恢复，也没有PG崩溃参考。后续在4份正常关闭后的独立副本上运行真实公共HeapScanCursor，34行/238单元/44个NULL和全部真实RID身份检查通过，四观察者均正常0且PID/进程组消失。初始到冷开共同键1..7不变，未提交角色全部8个旧键恢复，提交角色7个未修改旧键保持，实际修改键3及新键使用真实唯一RID。原WAL回执不回写；独立sidecar补充可见性证据，绝不把LP_NORMAL或虚构SQL ctid当可见行。

正常 CMake C++17/O2 固定成本对照已完整执行：旧版—C2—同一旧版各三轮，固定表及单 BYTEA 表各256行，另有C2专属多LOB表128行。168句实际计时、546个响应、24个正常Main及42个只读副本观察者均完成；全值/NULL/来源/冷开与真实RID验证通过。结果不支持本批普遍提速：16个可比操作中，墙钟与服务端CPU各为1项低于两旧、12项高于两旧、3项介于两旧；固定表查询在原双表流程中服务端CPU中位数15.667→30.044→18.536ms，C2增加91.76%/62.08%。全部变慢项、原三样本、长尾及漂移保留。DELETE旧版还多一个空T帧，不能当完全相同协议工作量；新多LOB因旧版建表失败，仅描述新版成本，不计算提速比。

后续固定CPU8的单表、三代正常构建窄诊断另有12次目标观测与24个正常Main；C2查询墙钟中位数22.086ms，对两旧24.181/26.336ms，服务端CPU14.326ms，对两旧15.406/16.605ms；C2相对C1的CPU中位数仍增加4.40%。这个更窄条件未重现原流程的大幅差异，但不能冲销原负向结果、归因于CPU亲和性或证明稳定收益，回退原因仍未确认。四角色用户态CPU采样的512次完整查询均通过，显示共同热点在逐列NULL检查及文件身份核验；采样计时没有混入正常性能表。

最终验证对象是候选C2（SHA `b53f853b356e9c8cf1e8fedf9832201d9bf705cf81d817b35314b4660e334594`），默认安装程序未替换，未推送或切换分支。原53.45MiB性能JSON当前保留为5.70MiB gzip，未压缩基准文件已被Git忽略；本批完整证据同样采用可逐字节还原的gzip归档。只对核验可恢复的生成副本安排清理，Git历史不重写。性能目标继续，不能把功能修复称为全库优化完成。

这是有限的 partial 进展，Goal active。原 273 项状态保持 22 complete/166 partial/70 unverified/15 deferred_by_user；`IDX-07` 仍为 `unverified`、`evidence=[]`。零列行集、零列表和原 TOAST 损坏夹具的普通缺口仍保留，用户暂缓边界保持。本批不证明完整 PostgreSQL 数据类型上限、文件格式、WAL 或协议等价。

完整定义、首轮失败与测试边界见[本批报告](performance-report-2026-10-30.md#multilob-storage-20261106)和可逆压缩证据（可选本地文件 `benchmarks/multilob-storage-20261106.json.gz`）。下方 P28 及更早段落是对应代别的历史记录。

<!-- CURRENT_CONSTRAINT_VALIDATION_20261106 -->

## CHECK/FK NOT VALID 与真实历史验证（2026-11-06，P28-C4）

`ADD CONSTRAINT ... NOT VALID` 仍在真实元数据锁下绑定 CHECK/FK 定义，只跳历史行验证；后续写入仍由原真实约束执行链检查。`VALIDATE CONSTRAINT` 对未验证的 CHECK/FK 扫描真实可见历史行，仅在成功后发布 validated 标记；失败保留定义和未验证状态，已验证约束不重复求值。真实 SQL NULL 与非 NULL 空字符串分别处理。CHECK 的 volatile 函数不因 volatility 标记被一律禁止；定义绑定不执行用户函数，实际验证仍按到达的行求值。

正常候选 C4 为 CMake C++17/O2，程序 SHA256 `5cd5fd9e15f72257d13f2622e180d643e5cc93fffe8a128cbf8e820f64caf539`；before 为 P27-C2 `dd5d9b85…`。原生父测试 4,896 checks/0 failures（含文件快照断言），其中真实 lifecycle worker 779/cold worker 158；两 worker 正常退出，捕获两份当时文件映像，不能将这些层级的计数相加。真实目标 PageAllocator fetch 38 个观测中 18 个为零、20 个为非零 fetch，另有真实读取正对照证明 hook 确实生效；这只说明历史验证访问路径，不代表整句无 I/O。原协议 142 主流程+12 冷控制共154条全部匹配 SQLSTATE、完整值/NULL、真实本地 relation OID 的 T7、tag 和 Ready；58份参数快照、5项保留标志检查通过。修改前同源用例实际83条（56匹配/27差异）及71条 NOTRUN；C3全部154条中有4处不匹配，原失败保留，C4修复后原断言未放宽。独立 append10 全10条通过：已经 validated 的带 nextval CHECK 不再求值，PK/UNIQUE VALIDATE为42809、缺失名42704。新增真实 PostgreSQL18.6 mini20 全20条符合首预测；产品对应19条+6条冷控制全25条、13项真实 `.params` 检查通过，ASCII collation 只比较大小写语义角色，PG使用真实 ICU、本地使用 nocase，不声称 provider/OID/Unicode 等价。mini19 v1接收器在启动前缺少 Path import：0SQL/0owner，原首错误保留后仅修接收器，不称产品重试或产品失败。原159参考的3个预期行快照错误及 reserved RESET 首预测错误都保留，后续 oracle 仅从已记录 PG 离线更正。完整原始帧不是逐字节 PG 等价：mini19主流程12组 raw 相同、7组不同（错误文本字段或真实本地 OID），不抹掉差异。

普通 ADD CHECK/FK 与同样有效数据的旧实现可比较所需验证成本。NOT VALID 可描述去掉不需要的历史扫描，但旧版多做扫描，不能称同扫描工作量提速；旧 VALIDATE 仅改标记，其耗时不是正确验证的性能基线。整个普通 ALTER TABLE 仍有 DDL 物理回滚快照、文件复制/摘要/同步成本，目标 PA fetch=0 不表示整句 O(1)、无堆文件读取或无 I/O。

固定0/128/1024行×三轮before-first/candidate/before-return，27个独立主流程+27次同数据冷重启均正常0；216个ADD/VALIDATE计时点、3024份响应、2384640个完整单元格（157950个NULL）、18360份物理文件映像（其中2700份堆映像）全部独立核对。1,024行时，CHECK NOT VALID wall中位数对前/后旧版分别−63.939%/−61.677%、server CPU −71.962%/−72.695%；FK NOT VALID wall −40.047%/−39.400%、CPU −15.482%/−13.469%，这两项是消除旧版多做的历史扫描。普通ADD CHECK wall −18.399%/−14.808%，CPU +0.124%/−4.360%；普通ADD FK wall −38.575%/−35.423%，CPU −4.131%/−0.022%。补上真实历史检查的CHECK VALIDATE wall +52.762%/+43.290%、CPU +260.317%/+251.282%，属于新增正确性工作成本，不能与旧假验证比较成等价速度。

负结果也保留：空表普通ADD CHECK CPU +29.920%/+35.993%；1,024行已验证FK重复VALIDATE wall对首/返旧版为−36.791%/+51.673%。全样本最长单点17.977415s，旧版wall中位数往返漂移−58.325%～+60.792%，server CPU漂移−28.186%～+4.689%。完整72行metric统计含各角色raw3/min/p50/max、双基准差值与旧版漂移，没有剔除负值/长尾，没有稳定倍数、因果结论或PG速度等价声明。计时是普通socket请求至Ready，包含DDL物理快照成本；process CPU与proc快照非原子、含所有服务线程，不是独占算子成本。

本批四个生产文件、两份新增回归、原登记入口和既有文档在 master 做本地版本管理，使用 `Palpitate-xus <palpitate.xus@outlook.com>`。正常验收程序独立冻结，根目录运行程序仍为 `425165af…`。原始证据只以完整可逆 gzip 入库；53.45MB 的旧 analyze/reindex JSON 已由约5.7MiB gzip替换，旧提交历史未重写。恢复步骤和逐流SHA保存在归档内，构建目录中的原始文件不再直接进入Git。

本批为有限普通约束生命周期的 partial 进展，Goal active。原 273 项状态保持（22 complete/166 partial/70 unverified/15 deferred_by_user）；`IDX-07` 仍是 `unverified`、`evidence=[]`。本批 GiST 页树资料只是未实现研究，不能将 P27 的 batch partial 记录当作 IDX-07 已部分验证。原用户暂缓边界保持。

完整代别、首轮失败、普通成本全表与范围见[本批报告](performance-report-2026-10-30.md#constraint-validation-20261106)和压缩原始证据（可选本地文件 `benchmarks/constraint-validation-20261106.json.gz`）。下方 P27 及更早段落保持历史口径。

<!-- CURRENT_GIST_PROBE_READER_20261106 -->

## GiST 读取开销与空文件回退（2026-11-06，P27-C2）

本次优化完整验证后才发布候选，零字节旧文本文件不再被误认为可信空集。合法V2/V3和非空旧文本继续兼容，V2空键覆盖限制不变。与先前实际PG18.6记录作75项离线对照，全部值及七字段描述符相同，原始帧73项相同；两处DELETE额外T差异仍使严格全协议通过为false。两端关系OID仅按各自真实目录锚定，未重跑PG或比较PG速度。

读取设计参考固定版本的[PG18.6 gistScanPage](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/gist/gistget.c)、[SQLite3.50.4 payload读取](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/btree.c)和[DuckDB1.4.3 ART查找](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/index/art/art.cpp)。PG有页锁、split NSN和一致性重检；SQLite借用本地payload并为溢出提供拥有缓冲；ART有真正树查找。本地小文件缓冲仍完整遍历flat sidecar，没有这些引擎的树、WAL和并发等价。

机制计数减少C++read调用和new请求，同时增加累计请求字节；正常SQL是否改善只能看[本轮完整对照](performance-report-2026-10-30.md#gist-probe-reader-20261106)，不能用instrumented单次时钟替代。既有普通缺口、完整GiST目标和用户暂缓边界均保持。

<!-- CURRENT_GIST_EMPTY_BOUNDS_20261106 -->

## GiST 空字符串、边界与 LIKE 检索（2026-11-06，P26-C4）

当前物理heap格式的存储列构建/REINDEX会写GiST V3，覆盖真实非NULL空字符串；NULL仍由真实位图区分。V2编码本来允许零长度，缺陷在旧producer省略空键，V3标记新的覆盖保证。旧V2/文本文件继续严格读取；无法证明覆盖、CRC、文件归属或可见性时交回原堆来源。有限空端点、缺失端点、严格/非严格边界和字面DEL分别表示；最终完整谓词、MVCC、NULL及TOAST仍检查。

LIKE必要字节前缀使用单独的tryGiSTSearchLikePrefix，保留原pattern/escape角色；默认locale下不把字节前缀的权限推广到SQL标量比较。无有效Unicode上界时保留无上界，避免DEL哨兵截断。普通EXPLAIN的>=空串不再独立触发全宽索引遍历；JSON usedIndex仅在真实索引候选读取成功时为true。

实现依据分别保留固定版本的[PostgreSQL btree_gist源码](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/contrib/btree_gist/btree_utils_var.c)、[PG text consistent](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/contrib/btree_gist/btree_text.c)、[SQLite记录格式](https://www.sqlite.org/fileformat2.html#record_format)和[DuckDB ART键实现](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/index/art/art_key.cpp)。空数据、NULL、端点存在性分别处理；PG btree_gist的text叶项可精确判断，不将PG全部GiST称为有损。23份官方源码/许可证与研究行锚随归档保存；研究不是运行验收。

真实PG18.6安装并加载btree_gist后重放75条共享SQL；最终C4逐项比较3231行/19382单元（661个NULL）、248列描述符的1736个元数据字段。75/75语义及描述符一致，73/75完整帧一致；两次DELETE本项目额外发送零字段T，PG只发CZ，严格全协议通过仍为false。两端关系OID各由真实目录锚定；PG正常退出、PID已回收，未比较PG访问方法格式、路径或速度。

固定九进程、七类相同SQL的wall中位数有4项低于两次旧版、3项高于两次旧版；服务器CPU为5低/2高。非空等值wall减少84.385%/83.171%，密集非NULL查询减少80.038%/77.317%；高端范围wall增加26.674%/50.555%，不存在范围增加62.435%/60.745%，低端含空范围增加0.617%/3.536%。REINDEX wall增加6.065%/6.366%，其V3多写298个真实非NULL空键，不能当等字节重建速度比较。完整首轮/同旧回测和全部三时钟如下，主键与无索引控制变化不作因果归因，未证明统一提速或PG性能等价。

本地GiST仍是flat sidecar，每次候选检索完整校验文件，并非PG的分页GiST树、opclass/KNN/WAL/concurrent全功能。虚拟键仍保留旧覆盖来源；native-only丢失定义、虚拟Bloom NEW NULL、既有missing-memory协议失败及其不完整旧版配对仍开放。下划线文件名歧义现在安全回堆，物理文件命名/关系身份布局仍需后续改造。原用户暂缓范围保持，Goal active；本批为partial。

当前清单为2383个测试文件：原2378中2374份逐字保持，4份只迁移当前GiST格式断言并加强V2兼容/空串真实RID检查，另增4个CPP及1个协议测试。顶层CPP1119（含链接桩）、全部CPP1124/其中子目录5；登记681个driver/661个独立E2E。源248/头177，754份Markdown、725份保护记录保持；25份旧gzip逐字保持，新增第26份。原273项及22complete/166partial/70unverified/15用户暂缓状态、原276条历史逐字保持，仅prepend第277条partial。

详细表格见[本轮报告](performance-report-2026-10-30.md#gist-empty-bounds-20261106)。以下 P25 及更早段落均保留当时的实现、计数和未完事项；本轮状态以本节为准。


<!-- CURRENT_REINDEX_ACCESS_METHODS_20261106 -->

## 普通访问方法 REINDEX 与空文本生命周期（2026-11-06）

本批补入普通 `REINDEX TABLE` 的 B-tree/Hash/Bloom/FTI/GIN/GiST/SPGiST/BRIN 重建、物理 Bloom NULL/空文本维护与 UPPER/LOWER 表达式索引 SQL 桥接。有效旧头的 BRIN `pages_per_range` 与 Bloom `k` 保留；`m` 随真实新键集按既有 loader 默认128 bitsPerEntry重算。旧头损坏/缺失时回退宽64/k7。默认程序仍为 `425165…`，没有服务重启或默认升级。

当前统计：2378 测试文件（原2375字节不变加2 CPP及1协议），顶层CPP1115（含链接桩）、总CPP1120/其中子目录5；登记680个driver/660个独立E2E。源248/头177，754 Markdown、725保护记录；24份旧gzip逐字保持，归档总计25份。原273项22/166/70/15及275条历史保持，仅新增partial第276条。

GiST 空文本编码、native-only 丢失定义、虚拟 Bloom NEW NULL 和上述失败均保留普通未完；不关闭原273项。

完整代别、首轮失败、工作量差异与全部成本表见[本批报告](performance-report-2026-10-30.md#普通访问方法-reindex-与空文本生命周期2026-11-06)。以下P24及更早段落保留当时历史，不把旧数字当作当前统计。


### 实现机制与源码依据

[PostgreSQL 18.6 index.c](https://raw.githubusercontent.com/postgres/postgres/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/catalog/index.c) 的普通 TABLE 重建遍历该关系全部索引，使用原目录/关系描述符定义、建立新物理代，并由事务与 WAL 管理。`BuildIndexInfo` 提取键数/列号、表达式和谓词等；AM routine、opclass/collation/reloptions 仍依附实际 relation，不能称都由该函数复制。[SQLite 3.50.4 build.c](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/build.c) 复用 existing Index 定义并经 sorter 重填；其[ pager.c](https://raw.githubusercontent.com/sqlite/sqlite/8ed5e7365e6f12f427910188bbf6b254daad2ef6/src/pager.c) 承担 pager/journal 的提交与回滚。[DuckDB 1.4.3 CREATE ART](https://raw.githubusercontent.com/duckdb/duckdb/d1dc88f950d456d72493df452dabdcd13aa413dd/src/execution/operator/schema/physical_create_art_index.cpp) 是构建机制参考，不作为 PG REINDEX 兼容 oracle。

本地采用真实权威定义、先私有构建后统一发布，并将可共享的一次扫描限制到最多8个 builder；mixed V2 标记提供本项目文件交换/恢复协议，不声称完整 PG transactional WAL 或并发等价。23份固定版本官方源码和许可证均归档；源码研究与最终实际执行的24条 PG18.6 共享对照分别保留证据。

下方 P24 及更早段落为历史记录。


<!-- CURRENT_BRIN_OPERATOR_CONSISTENCY_20261106 -->

最新P24-C2将普通BRIN摘要比较改为运算符专用路径；仍校验min<=max、所有文件/CRC/identity/dirty/snapshot fences与完整heap Filter复核，NULL flags不新增求值，legacy timestamp原路径保留。实际功能门禁及全部首次结果、固定9进程的完整维护/查询wall、CPU、长尾、负数与漂移见[性能报告](performance-report-2026-10-30.md)和压缩证据（可选本地文件 `benchmarks/brin-operator-pruning-20261106.json.gz`）；不称完整PG BRIN、所有SQL提速或因果已隔离。性能外层runner因label覆盖退出1、九backend正常0，二者分开记录；原source/plan/complete=false回执保留，后续仅离线finalize，不重采样、不当产品故障。默认425保持，partial、Goal active。旧2374测试逐字不变+1 native=2375；679/659 roster、源248/头177、顶层CPP1113/全部1118、754MD/725保护记录保持，历史275，旧23gzip保持+新24。下方P23及更早段落为历史记录。


<!-- CURRENT_BRIN_MAINTENANCE_OPTIONS_20261106 -->

最新P23-C1修复BRIN在VACUUM FULL、加列、删列和TRUNCATE/reset重建时丢失pages_per_range：健康V3配置在重建前保存，宽度1和SQL默认128均保持；缺失/旧格式/损坏仍沿用64回退。生产只改TableManage.cpp四处捕获/四次传参。新协议42命令/12镜像/4路径检查、25相关原生、6专项协议、3自动统计均正常0；PG18.6共享35SQL/19,327字段、11真实metapage一致；恢复7worker/2477检查。

固定9个正常socket进程测量完整VACUUM FULL及其后7类查询，所有wall/CPU、尾部、控制项和负数见[性能报告](performance-report-2026-10-30.md)及压缩证据（可选本地文件 `benchmarks/brin-maintenance-options-20261106.json.gz`）。该修复保留用户配置，不代表所有操作均提速；REINDEX仅Btree、完整PG BRIN AM/增量维护/WAL、规划重复验证开销、Bloom/GiST非NULL空文本覆盖和P22数组ROLLBACK间歇超时仍开放。前批密集CPU回测+6.64%负面记录保持。本批partial、Goal active。源248/头177，2373旧测试字节不变+1=2374，679注册/659独立E2E；原273item状态不变、历史274；754MD/725保护记录/22旧gzip保持。下方P22及更早段落为历史记录。


<!-- CURRENT_BRIN_TYPED_SCAN_20261105 -->

最新 P22-C10 修复 BRIN 数值边界/NULL 状态并接入真实候选页扫描，支持普通 SQL/绑定后五类比较和 NULL 谓词、反向常量及 pages_per_range。23页 extent 的原生候选 cursor 实际只 fetch1页；完整谓词仍复核。格式组件 O2/ASan各5588、新原生1834、新协议101命令/25路径断言、真实PG18.6共享73SQL/10490字段及7恢复worker/2477检查通过。有限81原生80正常0+1父代TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

固定9个正常socket进程、4096×4、七类查询，服务器CPU减少（首轮／同旧回测）：高端窄范围 41.19%／42.36%；低端窄范围 48.68%／49.29%；非主键等值 25.78%／20.13%；不存在的范围 94.22%／94.12%；密集范围控制 -7.44%／-6.64%；主键控制 30.31%／28.42%；无对应索引列控制 7.79%／4.38%。所有wall/CPU/控制负数、原始帧和尾部见[性能报告](performance-report-2026-10-30.md)与压缩证据（可选本地文件 `benchmarks/brin-typed-scan-20261105.json.gz`）。新BRIN文件V3，旧1/2只作兼容读取/安全回堆，旧二进制不能读取V3；当前物理heap2保持，暂缓format2仍指wire/output，NetworkServer.cpp未改。REINDEX尚仅Btree、部分重写配置保存、PG opclass/revmap/增量维护/WAL/concurrent、SSI及特殊类型/来源路径等仍开放，不声称全部数据库操作或PG性能等价。本批partial，Goal active。源248/头177、测试2373，678注册/658独立E2E；原273对象状态不变、历史273，754MD/725保护记录和21旧gzip保持。下方P21及更早段落为历史记录。


<!-- CURRENT_INDEX_BUILD_NULL_OWNER_20261105 -->

最新 P21-C1 让索引构建/REINDEX 复用当前行物理 NULL 位图，并修复 BRIN 将真实 SQL 全 NULL 整数摘要写成 `[0,0]` 的问题；空字符串范围保留。新原生13366检查、新协议40命令、真实PG18.6共享35命令/2250字段及7恢复worker/2477检查通过。有限相关79原生78正常0+1父代相同TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

正常2048行维护9进程、368640字段核对；相对同旧回测CPU中位数减少：Btree 19.30%，复合 19.02%，写事务 45.10%，回滚 47.67%；BRIN -154.76%（退步）。wall波动大且多项变慢，另9进程完整socket查询控制也没有一致提升，详见[性能报告](performance-report-2026-10-30.md)与压缩证据（可选本地文件 `benchmarks/index-build-scan-owner-20261105.json.gz`）。BRIN数值排序及SQL执行算子等仍为普通开放缺口，本批partial，Goal active。当前物理heap版本2保持；暂缓format2指协议/输出所有权。源248/头177、测试2370，677注册/657独立E2E；原273状态不变、历史272，754MD/725保护记录与20旧gzip保持。下方P20及更早段落为历史记录。


<!-- CURRENT_INDEX_CRC32C_20261105 -->

最新P20-C2为共享索引CRC32C加入通用查表和受CPU能力保护的SSE4.2路径，保留完整校验、所有文件字节与C++17基本构建要求。组件44392检查及ASan/UBSan、无SSE4.2模拟35520检查通过；12真实存储worker/3840字段与六类索引旧新双向读写、重建文件逐字节一致。最终68原生67正常0及1项父代相同TOAST中止、旧32协议29正常0及3项父代相同失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 18.62%／-0.48%；热点等值 3.68%／0.90%；低频前缀 11.82%／14.98%；低频等值 -12.80%／-22.17%；不存在等值 6.78%／0.48%；主键控制 -1.07%／-6.83%；控制差异不作因果归因。源248/头177、测试2368，676注册/656独立E2E；原273状态不变、历史271，754MD/725保护记录与19旧gzip保持。Goal active；下方P19及更早段落为历史记录。详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_GIST_READ_OWNER_20261105 -->

最新P19-C3修正普通当前读事务被GiST误判为重叠写者的问题，并复用可见元组NULL位图。专项1214检查、新39协议、PG18.6共享37命令/1287字段对照通过；候选检索阶段目标堆页fetch由旧版2021降至0，返回行仍正常回表校验。历史/重叠/自身写入/缺失损坏/空文本范围及SERIALIZABLE保留堆后备。旧60原生59正常0和1项父代相同TOAST中止、旧32协议29正常0和3项父代相同严格失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 2.54%／3.42%；热点等值 -0.42%／-1.44%；低频前缀 56.62%／52.19%；低频等值 -12.42%／-10.29%；不存在等值 2.15%／1.44%；主键控制 -6.09%／-3.54%，负数与控制波动也报告。旧GiSTScan/idx_scan只代表算子，不能单独证明底层物理索引访问；下方旧章节按此限制理解。源248/头177、测试2367，676注册/656独立E2E；原273状态不变，历史270，754MD/725保护记录保持，18旧gzip保留并新增1。仍partial，Goal active；详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_TYPED_PLANNER_STATISTICS_20261105 -->

最新批次 P18-C6 将普通等值与二进制文本 LIKE 扫描成本接入拥有真实身份的 typed ANALYZE 统计，并修正事务内显式分析的两份统计文件回滚。新65条协议/15次扫描、原LIKE123/27、最终原生专项及cold exec、组件O2/ASan/UBSan、7个恢复worker和真实PG18.6共享33命令/6732字段对照通过；相关旧协议29通过、3项父代完全相同严格失败，首次自动数组/range回滚超时及后续原deadline固定旧/新/旧六次通过均保留。完整socket性能中位数减少（首轮／回测）：热点前缀 73.72%／73.25%；热点等值 51.25%／15.72%；低频前缀 58.53%／-8.58%；低频等值 44.30%／38.10%；不存在等值 24.66%／24.31%；主键控制 30.77%／29.00%，负数也保留，只代表该负载。原273与22/166/70/15状态保持，历史269，测试2365，源248/头177，675注册/655独立E2E，754MD/725保护记录、17旧压缩包保持。仍是partial，Goal active；事务内ANALYZE需要临时数据库物理备份，旧统计格式、未转换消费者和用户暂缓模块未整体完成。详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据。


<!-- CURRENT_LITERAL_LIKE_SCAN_20261105 -->

## LIKE前缀的真实索引读取与密集前缀成本（2026-11-05，P17-C6）

普通无参数SELECT中的TEXT/VARCHAR字面量LIKE现在可实际使用已有GiST侧索引。已绑定AST提供真实关系/列身份；前缀范围只筛选候选，原类型化LIKE、AND剩余条件和NULL语义继续复查。转义百分号/下划线、多字节ESCAPE、UTF8上界以及打开前删除索引均有回归。可用ANALYZE的MCV与完整覆盖直方图桶识别密集前缀，按真实表extent和成本参数保留堆扫描；未知探测至少计1行成本。

同一4096行×3列、父代／最终C6／同一父代回测各3进程交错运行；wall中位数变化（正数为减少，相对首轮／回测）：热点前缀（3200行）6.88%／-3.83%；低频前缀（1行）68.01%／67.77%；不存在前缀（0行）69.40%／68.72%；显式ESCAPE前缀（1行）71.84%／71.06%。仅代表本负载；C3热点超过3倍耗时的失败尝试完整保留。

最终C6通过新原生310检查、原13068与独立冷读105检查、37个相关原生入口；纯前缀组件O2和ASan/UBSan各72567检查，启用泄漏检测。新123条协议命令含27次实际扫描断言；32个相关旧协议入口29正常0、3项严格失败与父代完整观察相同。真实PG18.6以两个会话重放共享65条SQL，935个完整字段/NULL/OID逐项相同，3个EXPLAIN ANALYZE实际行数相同；未比较PG的AM路径、成本或速度。

详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据（可选本地文件 `benchmarks/like-prefix-scan-20261105.json.gz`）。本批仍为partial，Goal active。


<!-- CURRENT_ORDINARY_QUERY_PROFILE_20261105 -->

## 普通查询的规划元数据与解析开销（2026-11-05，P16-C2）

本批在一次普通查询规划内复用关系schema，并按需各读取一次B-tree/Hash/Bloom/GiST列清单；主键与不可能使用位图/GiST的条件省去无用发现。普通表达式先排除不可能的类型常量前缀，避免对数字、表达式分隔符前的列名做推测类型解析及异常处理。真正的限定/引号/修饰/复合类型常量与显式CAST仍使用原声明语法。

交错的8倍调用补测，父代／最终C2／同一父代回测各3进程；wall中位数变化（正数为减少，相对首轮／回测）为：热点等值（3200行）12.15%／-1.12%；低频等值（1行）15.17%／18.32%；不存在等值（0行）17.94%／14.60%；主键等值（1行）25.52%／19.68%。全部短时及补测的正负结果保留，不能推广为全部操作或PG速度。

最终C2通过原生13068与独立冷读105检查、32个规划/存储入口和14个语法入口；另2个冷目录原生入口保留父代完全相同的严格失败。新65条协议命令含10个实际扫描断言，原有73条等值协议通过；另30个相关旧协议入口27通过、3个严格失败与父代完整观察相同。实际PG18.6的共享25条SQL仍为7331/7331逻辑单元格、9/9行估计一致，8/9路径相同；不是完整兼容验收。

详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据（可选本地文件 `benchmarks/ordinary-query-profile-20261105.json.gz`）。本批是普通读取partial进展，Goal active。


<!-- CURRENT_EQUALITY_SCAN_COST_20261105 -->

## 普通等值查询的实际扫描选择与完整网络成本（2026-11-05，P15-C7）

本批让普通原生标量堆表查询实际消费基于ANALYZE频率的扫描选择：热点等值可选顺序扫描，低频等值和主键保留索引路径。此前只修改规划器时，无ORDER BY的普通查询仍绕开物理计划；现在结构化输出已接通，并修复无可用索引OR的显示文本合并，以保留重复行、NULL、空串和换行。B-tree、位图AND/OR与GiST在探测前取得语句快照，并在快照不适合当前索引或探测期间提交边界改变时读取完整可见堆候选，再做原条件检查。

这是普通读取的partial进展，使用现有原生 `.stats` 中的MCV/计数；尚未让优化器全面消费新的typed `.analyzed_statistics` 槽。普通EXPLAIN的文本缓存键现包含精确的实时成本设置；没有修改已暂缓的retained/custom/generic规划、参数、portal/format2或mutation/provider生命周期。当前私有普通后端P15-C7 SHA256为 `3ff5f16a9cc3a223d6618db87d8a5bbe85240dfd4c08cfdceafeb6ba49c03d3a`。默认根程序425与既有服务保持；Goal active，PostgreSQL整体功能仍partial。
同一4096行×3列、同一真实SQL与完整网络返回，父代／最终C7／同一父代回测各3进程，固定9个完成测量的进程，不重试计时查询；另两次在基线装载/启动阶段失败、没有计时样本，完整保留。wall中位数相对首轮父代／回测的变化（正值为减少、负值为增加）：热点等值（3200行）12.51%／2.45%；低频等值（1行）-5.57%／-5.47%；不存在等值（0行）77.31%／77.36%；主键等值（1行）-11.11%／9.44%。正负变化均如实保留；仅代表这四个普通读取负载，不能推广为全部操作、通用吞吐或PG速度。

新原生13068检查与独立冷读105检查通过，纯频率组件O2及ASan/UBSan各17检查（启用泄漏检测）。24个相关原生入口实际正常0，含完整7worker恢复1974检查和14份子进程完整输出；这些入口实际链接最终C7的61个核心对象。新73条普通协议命令正常0，验证实际扫描计数、全部行值/NULL及旧快照；26个旧协议入口中23正常0，另3保留与父代一致的严格失败，未称全套通过。真实PG18.6对共享25条命令的7331个逻辑单元格全部逐字相同，9个EXPLAIN行数相同，8个扫描选择相同、1处成本边界扫描选择不同。

原始测量、失败与独立复核见压缩机器归档（可选本地文件 `benchmarks/equality-scan-cost-20261105.json.gz`）和[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_PG_STATS_SOURCE_20261105 -->

## pg_stats 完整基础查询与读取成本（2026-11-05，P14-C6）

本批接通实际 `pg_catalog.pg_stats` 的17列基础SQL查询、参数为零的纯Describe、文本/二进制/混合结果格式，以及实际 owning anyarray 输出；支持已测的过滤、投影、排序、LIMIT、别名、CTE和JOIN。每次查询只读取当前数据库的真实统计与目录，重用本次查询内的关系/属性索引和每关系一次的物理模式校验，减少逐列重复扫描、模式复制和文件散列。

本节更新下方P13当时“17列SQL尚未接通”的结论；下方历史批次保留当时事实，当前状态以本节为准。当前私有普通后端P14-C6为 `9362b02aba17d45038702211ba42a6a1d797ef3580d97edb8ffabce80e0f63b6`。默认根程序425和既有服务未切换，Goal保持active；整体PostgreSQL功能仍为partial。
同功能未索引C2／最终C6／同一C2回测各3进程，每进程先1次不计时完整查询，再计时3次完整17列SQL查询API。16张表×30列×32行，实际512行/15360存储单元格/1696个NULL；每次查询返回480行×17列。wall中位数384.334／245.178／379.841ms，降低36.21%／35.45%；process CPU中位数392.205／254.151／387.508ms，降低35.20%／34.41%。这是该负载的完整原生SQL API对照，不是任意查询、网络端到端、已发布父代或PostgreSQL速度对照。所有9个样本及长尾保留。

新原生109检查与独立冷读90检查、投影组件O2和ASan/UBSan各548检查通过；34个相关原生入口全部正常0，包含完整7worker恢复1974检查及14份完整子进程输出。普通后端新协议26条SQL及旧标量25/数组19/范围19条自动统计协议通过。真实PG18.6同26条协议命令991个字段值：938逐字相同、53处严格差异保留；17列二进制153项中145相同、8处为实际存储宽度差异，六种范围的24个直接二进制值全部相同。另510个有限float4输入的文本和二进制身份均与PG一致。没有全库SAN或整体PG等价结论。

原始测量、失败及逐字复核见压缩机器归档（可选本地文件 `benchmarks/pg-stats-source-20261105.json.gz`）与[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_RANGE_COLLECTOR_20261105 -->

## 范围统计采集与完整 ANALYZE 成本（2026-11-05，P13-C2）

本批新增六种内置范围类型int4range/int8range/numrange/daterange/tsrange/tstzrange及实际范围域的ANALYZE采集：独立排序下界和上界、保存范围边界与长度直方图、空范围比例、NULL比例和实际存储宽度。边界保留包含关系、无界端点和子类型语义；本次样本局部缓存解析键，避免排序时反复解析。

最终私有普通后端P13-C2为 `3593b23e5e142605fe71149daec3347015003fb7a84d8a2505c80578e4e77a5d`，默认根程序425保持。完整pg_stats目录/17列SQL及output-only anyarray尚未接通，两条实际探针仍严格失败0A000。多范围尚缺真实目录/存储拥有者；本批仍为采集阶段的partial进展，Goal active。
本批同功能未缓存C1／缓存C2／同一C1回测，各3进程；每进程2048行、1个整数和六种范围，先1次不计时预检，再计时3次完整ANALYZE API。process CPU中位数388.224／201.363／391.997ms，降低48.13%／48.63%；wall中位数441.019／250.031／456.067ms，降低43.31%／45.18%。所有9次原始测量均保留；这不是任意SELECT、父代已发布后端或PG速度对照。

原生范围845检查及冷读262通过；组件O2和ASan/UBSan各43检查（启用泄漏检测，非全库SAN）。21个相关原生入口含完整7worker恢复1574检查全部正常0。原数组786+冷读191、标量641+冷读2、范围19条/数组19条/标量25条协议SQL通过。真实PG18.6同一520条SQL对照18列243项：229相同，14处严格差异保留（13处存储宽度、1处numeric NaN长度排序）。没有PG整体通过结论。

原始测量、失败和复核见压缩机器归档（可选本地文件 `benchmarks/analyzed-range-collector-20261105.json.gz`）及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_ARRAY_COLLECTOR_20261105 -->

## 数组统计采集与完整 ANALYZE 成本（2026-11-05，P12-C3）

本批把真实数组存储输入接到同一次ANALYZE采样：保存整数组MCV/直方图/相关性，以及常见元素、按非NULL数组计算的成员频率和不同元素数直方图。实际验证integer/text/numeric/boolean/float8数组的NULL、空数组、字面NULL、重复值、多维/下界和冷读；其他元素类型及全部PostgreSQL功能仍需分别验收。

最终私有普通后端P12-C3为 `7d3cc2a1353c105ecac62579a919ec96d088cd325fa74234bc47252061e1029a`，默认根程序425保持。完整pg_stats目录/17列SQL与output-only anyarray尚未接通，两条实际探针仍严格失败0A000；本批是采集阶段的partial进展，Goal active。
完整ANALYZE对照为本批同功能未缓存C2／缓存C3／同一C2回测，各3进程；每进程2048行、1个整数和3类数组，先1次不计时预检，再计时3次完整API。process CPU中位数1243.545／164.180／1296.411ms，降低86.80%／87.34%；wall降低85.86%／86.87%。最终版685.690ms wall长尾保留。此结果限于该负载，未测任意SELECT或PG速度。

原生数组786检查及冷读191通过；计算组件O2和ASan/UBSan各32检查（启用泄漏检测，非全库SAN）；17个相关原生入口含完整7worker恢复1574检查全部正常0。原标量641+冷读2、数组19条协议SQL和原标量25条协议SQL通过。真实PG18.6同一554条SQL对照14列325项：301相同，24处严格差异保留（11宽度、11collation OID、2处相同频率元素的截断选择）。没有PG整体通过结论。

原始测量、失败和复核见压缩机器归档（可选本地文件 `benchmarks/analyzed-array-collector-20261105.json.gz`）及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_SCALAR_COLLECTOR_20261105 -->

## 标量统计采集与 ANALYZE 优化（2026-11-05，P11-C4）

本批把真实ANALYZE扫描接入标量统计快照：保留NULL、实际存储宽度和原始扫描顺序，按统计目标维护有界样本，生成signed float4 distinct、MCV、排除MCV后的直方图及相关性。数组/范围专用采集、完整继承/表达式索引统计、真实pg_stats目录源和anyarray输出仍待完成；不是完整17列SQL功能验收。

最终私有普通后端为P11-C4 `e1cab6f6…`；默认根程序425保持。原生641检查及fresh冷读2检查通过，统计计算组件O2与ASan/UBSan各22检查通过（启用泄漏检测，非全库SAN），16个相关原生入口全部正常0。普通Main的CREATE/INSERT/ANALYZE/SELECT四条命令通过，464B实际快照独立解码核对两列；两条完整pg_stats SQL探针仍严格失败0A000，服务器正常0。

相同输入、匹配en_US默认排序规则的真实PG18.6对照共189项：184相同，保留3处实际存储宽度差异和文本槽2处collation OID差异（本地0/PG100）。样本边界按独立样本检查有效范围，不要求随机结果相同。原C-locale参考的文本相关性差异也保留；没有修改SQL语料以取得通过。

仍不能称为PostgreSQL等价：完整17列目录源/真实view与属性身份、anyarray输出、其他类型采集、继承/索引所有者、DDL/恢复和既有权限消费者尚未完整验收。CPU52%左右为本批同功能ANALYZE对照，非PG速度或全库提速。原273项状态22/166/70/15不变、历史262、Goal active。见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

P10完成完整17字段/7类槽的拥有值快照基础，保存真实的关系/属性/继承键、类型声明、操作符和collation身份及可空numbers/values。真实PG18.6确认数组列MCV的元素OID1007、常见元素23、范围长度701，以及多范围bounds使用范围3904；本模型可保存这些不同身份。

SQL侧尚未接入：两条完整17列探针仍0A000，采集算法、真实view/attribute来源、anyarray输出和现有权限消费者接入仍待实现，不以内部组件通过关闭pg_stats或原功能族。编码CPU对照只针对本批新组件，未测PostgreSQL性能或普通SQL提速。[报告](performance-report-2026-10-30.md#完整统计视图的快照基础2026-11-05p10)与归档（可选本地文件 `benchmarks/analyzed-statistics-snapshot-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

P9-C2 `30c57ca8…` 补的是本项目存储生命周期可靠性：两处实际use-after-free已修复，原七阶段恢复及完整Memcheck本次通过。它不等于PostgreSQL完整恢复/epoch/跨进程准入或所有退出时序等价。正常Main保留5项探针元数据差异，完整17字段pg_stats仍为后续工作。

本批9次完整恢复对照未证明稳定提速；没有重测PostgreSQL速度或全部35操作/36查询。默认425保持。原失败与新版本证据见[报告](performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)及归档（可选本地文件 `benchmarks/relation-list-engine-lifetime-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

本批P8-C3 `d778afbd…` 参照恢复后事务分配器必须超过导入事务号的原则，补入本项目公开恢复的只读水位扫描和持久推进；这不等于PostgreSQL的WAL恢复、epoch/wrap或并发机制。较早目标首次探针1→4行得到实际验证，正常Main仍有5项探针元数据差异，最终七阶段原生验收仍因advance阶段异常退出而严格失败。

第二轮仅比较本项目旧P7-C1/C2/C3/旧P7-C1：C3 wall p50较两次旧版低4.16%/6.42%，process CPU低2.47%/3.17%。第一轮C2慢项、6.8秒与18.5秒长尾不删除；没有PostgreSQL速度或完整35/36新结论，默认425未提升。跨进程准入、完整恢复失败及完整17字段pg_stats仍开放，不能把少数字段renderer当视图完成。详见[完整报告](performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和机器归档（可选本地文件 `benchmarks/restore-transaction-horizon-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

当前 P7-C1 `724c1147…` 按PG18.6跳过virtual列统计，普通/STORED与真实NULL/TOAST控制通过。原生20控制修复6项失败，SQL34仍17项差异，qualified pg_stats视图、部分语法与协议帧尚待补齐；原273项没有功能族关闭。

本批只有内部C8/C1/C8速度对照：完整备份wall p50为737.771→587.376→677.477ms，较两次旧版低20.38%/13.30%；四DDLwall p50为2快/2慢，ALTER与TRUNCATE慢于两次旧版。没有新的PostgreSQL速度采样或整库倍率。原有旧→旧恢复探针失败及独立四组成功对照分别保留，完整35操作/36查询未验收，默认425保持。[完整数据与边界](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

当前源码P6-C8 `45264111…` 的针对性258观察为238通过/20差异，新增82整项通过、无整项回退；另保留6个Simple描述符组件损失。原767观察仍129差异，完整结果与冻结旧版/C6一致。原273族状态不变，未认定完整类型、DML、目录或协议族完成。

正常C2/C8/C2的四条100行/八virtual SQL全部正确，SELECT * p50为923.223/80.538/916.453ms，CASE未选g1为863.623/21.730/871.064ms。PG18.6本批仅有功能参考，没有新的PG速度采样或整库倍率。完整35操作/36查询尚未对C8验收，默认425保持；[全部数据和剩余边界](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

本批是425/C2/425内部实测，没有新PG速度测量或功能族关闭。原273仍22 complete /166 partial /70 unverified /15 deferred_by_user，生成列需求/赋值与更多组合仍开放；Goal active。

详见[完整性能报告](performance-report-2026-10-30.md)和本批机器归档（可选本地文件 `benchmarks/analyze-reindex-main-costs-20261104.json.gz`）。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

固定301仍188匹配/113差异，其他四组结果未变；生成列赋值/需求、历史位图、quoted routine元数据、更多目录/输入/来源形式仍开放。原273为22 complete /166 partial /70 unverified /15 deferred_by_user，251项未标记完成。新数据为内部425/C4/425对照，没有新增PG速度测量或稳定跨版本倍率。

详见[本批报告](performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及机器归档（可选本地文件 `benchmarks/returning-declarations-hotpaths-20261104.json.gz`）。以下为历史记录。

## 本批局部能力与距离（2026-11-03）

C3 `425165af…` 在真实RETURNING所有者内保存已经求值的拥有值，并补入实际目标及OLD/NEW物理数组的输入物化；正常binary结果中的记录字段、数组形状和NULL有新增严格控制。固定301与PG18.6参考为188匹配/113差异，相对S3的201差异改善88项；新56项从C8的20差异降至8。剩余6项别名描述符、虚拟数组和历史位图仍是普通缺口，没有认定完整类型/协议/DML族完成。

辅助求值循环的请求次数与字节减少，但公开游标512行分配不变、单次冷CPU20.750→35.862ms变慢；没有新的PG速度对照，也不能将局部构造收益解释为整SQL或所有数据库操作加速。

只读共享内置函数注册表用于减少重复构造，同时保留实际实例覆盖、时钟及参数求值期间的回调替换；这属于本项目内部实现，不代表与PG执行器等价。C3原生选取57为56/1，协议选取32为24/8；最终S3/C3/S3全部36组查询p50低于两次S3：相对首/末降低2.05–33.76%/1.76–29.81%，基线漂移−22.57%至+11.58%。另一组C8/C3/C8的35类操作仍为18快/2慢/15居间，保留update_range、reindex慢项及−90.28%至+130.91%漂移；两种对照不拼成跨版本稳定倍率，没有新的PG速度对照或统计显著性结论。完整代别与原失败见[本批报告](performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)。原273仍22 complete /166 partial /70 unverified /15 deferred_by_user；独立机器归档见全部原始统计（可选本地文件 `benchmarks/function-registry-returning-20261103.json.gz`）。以下C8/C7为历史评估。

## 当前评估（2026-11-03）

C8 `91d339db…` 保留原始类型行物化的局部分配优化，没有扩大类型、目录或DML的兼容范围。26原生/31协议选取集及301/83/111/68/16固定观察与C7连续，301仍有114差异、REGTYPE68仍有11普通缺口；原273状态不变。

512行公开游标累计请求与字节减少，但完整查询36组为5快/18慢/13居间：64kB为0/9/3，4MB为4/2/6，16MB为1/7/4。两次S3基线漂移全部保留，不能用跨窗口数据构造稳定C7→C8倍率，也没有新的PostgreSQL速度采样。源码保留该优化，默认安装仍为S3；详见[本批报告](performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)及机器归档（可选本地文件 `benchmarks/owned-datum-primitive-20261103.json.gz`）。

前批C7评估（保留原结论）：

C7 `f135335e…` 将真实复合值、声明元数据与普通格式 1 的结果编码连通，并补入 REGTYPE/REGTYPE[] 的持久身份。固定 PG18.6 参考的 301 项观察仍有 114 差异；相对 AA52 的 201 差异改善 87 项，没有把剩余或旧失败算作通过。83 项数组位图/CAST 与 111 项多态数组控制各保留两项 SHOW/UTC 配置差异；REGTYPE 物理 68 项为 57 通过/11 差异，正常参数 16 项通过。

新原生三个入口共 38 控制通过；C7 最终选取的 26 原生入口全过，31 协议入口为 24 通过/7 个既有失败入口。C5 完整 106 原生的 105/1 与 95 协议的 86/9 继续按 C5 代别记录，不能称 C7 全量 106/95 重跑。全部原始差异、来源和未完边界见[本批功能与成本报告](performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)。

复合 binary RETURNING、更多 UPDATE WHERE/SET 输入加载、域数组/命名复合目录、数组位图历史、legacy typmod 和 ALTER USING 仍是普通缺口，未新增暂缓。资源55通过；36组速度为4快/23慢/9居间，C7未通过默认安装性能门，根目录保持S3。分配与字节额外成本也已记录；没有新的PostgreSQL速度对照；原 273 仍为 22 complete /166 partial /70 unverified /15 deferred_by_user。以下为前批记录，保留其当时输入与结论。

上一批未接受研究记录：

最新调查基于 `3d576b8b`：两版窗口暂存候选均未接受，安装程序仍为S3 `aa52b5fe…`。scratch2虽减少重复分配，整SQL36组仍为12快/8慢/16居间，4MB+16MB为2/8/14；基线漂移全部保留，未获得稳定跨预算收益，不能引用单一快项宣称整体改善。
新增固定301类型协议观察的实际PG18.6参考通过严格结构/预期错误/原始值验证；S3和两候选逐项相同，各保留201差异。普通REGTYPE物理声明/存储、domain-array身份、named composite、真实record/array二进制值传递等仍有缺口；隔离提案未应用，R151暂停子项不恢复。没有新的PG性能采样，也不更新旧35操作速度倍率。
原273仍22complete/166partial/70unverified/15originaldeferred；完整数据与接受边界见[本批报告](performance-report-2026-10-30.md#窗口暂存分配与类型输出调研2026-11-03)。以下记录继续按各自历史输入解释。

本页总账及 35 类 PG 性能对照以源码输入 `9b71e118` 为准；随后在 `e7dcd673` 上完成下述 Top-N 批次。
之后在 `17df7ff3` 上补充普通非分区 TableScan 按页扫描，局部验证范围分别记录，不重写历史 PG 样本。
随后以 `bcde5b3e` 补充普通 LIMIT 接入及正常格式下的顶层 SQL 运行错误前缀，见下方最新局部记录。
再以 `d09cb6c1` 实现普通物理排序的内存预算、匿名临时文件归并和受支持的真实 SQL 接入，其他排序分流仍分别开放。
随后基于 `471dad3f` 完成普通物理 Sort 最终按需归并、NULL 绑定恢复和错误清理；其他查询慢值仍未解决。
再基于 `049c0bce` 接通会话 work_mem 与普通物理 Sort 的实际预算，配置函数和完整例程/算子治理仍开放。
随后基于 `d8533bc9` 为普通类型化/多键排序增加预算、完整快照和比较缓存，其他排序/算子边界继续分别开放。
再基于 `8a8532a0` 接入普通 UNION ALL/VALUES 的预算排序、真实 VALUES 查询子句和枚举输出描述符；其他集合分流及完整参数推导继续开放。
前批基于 `baba4bcb` 扩展普通 UNION ALL 全局计数表达式、VALUES/集合根 FETCH `c_expr` 和实际选中上下文的参数声明推导。
前批基于 `9f019ffa`，最终 C9 程序 `c696d3b2` 接入 Main/Core/真实 SQL 子查询共用的类型化窗口宿主；
窗口内存治理、通用 Core JOIN lowering、完整参数推导及其他 PostgreSQL 能力仍开放。
随后基于 `b04db801`，C1 程序 `56d06bca` 仅补入窗口私有整数键缓存；窗口预算和上述功能边界不变。
本批基于 `d7793044`，当前 S3 程序 `aa52b5fe` 保留 S2 的实际窗口定义/排序投影共享规则，并让结果映射和顺序均未改变的 stage 使用原类型化执行循环；仍不是任意表达式或所有 volatile 调用的全局合并。
产品仍为 0.2.0 / C++17，仓库既定行为基线为 **PostgreSQL 18.6**。
本项目已有真实的单机数据库内核，但距离 PostgreSQL 级别的通用功能、规模扩展和生产验收仍有明显差距。
这次更新复核总账、当前源码和测量记录，不重新认定 273 项要求已经验收，也不变更用户暂缓范围。

功能差距不能用 SQL 关键字数估算；性能差距也不能从本项目优化前后的倍率推算。
当前有限子集可用于独立数据目录中的开发和研究；尚无证据支持无差别替换 PostgreSQL、完整扩展生态或生产 SLA。
兼容性声明继续受[契约](compatibility-contract.md)约束：客户端互操作 A 门、行为兼容 B 门均未宣告通过。

## 功能距离：273 项总账仍有 251 项未标记完成

| 总账状态 | 数量 | 应如何理解 |
| --- | ---: | --- |
| `complete` | 22 | 总账已有完成标记及历史引用；本次没有逐项重新验收 |
| `partial` | 166 | 存在实现或局部验证，仍有语义、边界或验收缺口 |
| `unverified` | 70 | 未达到该项要求的证据门槛，不等于 70 项全部没有代码 |
| `deferred_by_user` | 15 | 原始总账中的用户暂缓项，仍是兼容性缺口 |
| 合计 | 273 | 还剩 251 项未标记完成；排除原始 15 项暂缓后仍有 236 项 |

**22/273 = 8.1% 只是总账条目的标记比例，不是 PostgreSQL 功能完成率。**
22 项中，13 项属于项目语法偏移/拒绝假成功，3 项属于工程与版本策略；各条目工作量也不相同。
不能据此说“只实现了 8.1%”，更不能沿用旧页未证实的“95% 语法覆盖”作为成熟度。
[细化条款](review-requirement-clauses.json)目前只拆解了原始 273 项中的 3 项，另 270 项待拆解；
局部测试通过不能自动关闭整个功能族。

以下计数直接来自 [gap-progress.json](gap-progress.json)，与[原始审计](postgresql-18-gap-audit.md)保持一致：

| 功能族 | 总项数 | complete | partial | unverified | 用户暂缓 |
| --- | ---: | ---: | ---: | ---: | ---: |
| P0前置条件 | 16 | 0 | 15 | 0 | 1 |
| SQL/目录/类型/函数/约束 | 72 | 4 | 67 | 1 | 0 |
| 查询与DML | 19 | 0 | 19 | 0 | 0 |
| 优化器与执行器 | 17 | 0 | 8 | 9 | 0 |
| 索引 | 15 | 0 | 4 | 11 | 0 |
| 事务/锁/VACUUM | 16 | 2 | 10 | 4 | 0 |
| 存储与WAL | 16 | 0 | 15 | 0 | 1 |
| 复制/备份/HA | 17 | 0 | 12 | 5 | 0 |
| 协议与客户端 | 14 | 0 | 8 | 6 | 0 |
| 安全 | 13 | 0 | 0 | 0 | 13 |
| 监控与运维 | 16 | 0 | 6 | 10 | 0 |
| 扩展与FDW | 13 | 0 | 1 | 12 | 0 |
| 工程与生产验收 | 15 | 3 | 1 | 11 | 0 |
| 项目语法偏移 | 14 | 13 | 0 | 1 | 0 |

原始 15 项暂缓包括 P0-13、STO-08 和 SEC-01..13；之后按具体问题追加的暂缓记录另见
[范围说明](deferred-scope.md)，没有合并或改写原始 273 项计数。本次只评估这些缺口，不恢复暂缓调查。

前批类型化排序局部记录：39 原生全过、完整所选 26 协议为 25/1，新 108 在 PG/最终程序全过，既有 463/40 暂缓差异保持。
9 项实际 Main 资源检查证明普通无 ARRAY 的多键/表达式排序也在低预算落盘；输入边界分配显著下降，但全查询峰值与普遍加速没有由此获得证明。
三个预算及一次高预算复测的 1,584 个延迟样本完整列于[本批报告](performance-report-2026-10-30.md#普通类型化排序的预算与比较缓存2026-11-02)，不能沿用历史倍率代表最新版本。

前批集合/VALUES 局部记录：44 原生全过，新 347 协议控制及额外 52 类型阶段观察在 PG/最终程序全过。
完整 31 协议首轮 29/2，未发 SQL 的启动连接失败同源复测通过；既有 463/40 暂缓观察逐字节不变，最终逐入口复核为 30/1。
新增52永久协议入口独立验证通过；全部32个已运行入口按各自最终结果复核为31/1。
七项 Main 资源、35 类操作有限检查及匿名文件内强杀/重开通过；完整新旧速度对照和未解决的慢值见[集合排序与 VALUES 报告](performance-report-2026-10-30.md#集合排序预算与-values-查询子句2026-11-02)，不能用历史倍率代表最新版本。

前批集合键优化：45原生全过，33协议32/1；新66在PG/前版/新版全过，原463/40暂缓日志相同。
实际整数排序窗口改善；1980样本保留全部漂移和慢控制，见[集合键缓存报告](performance-report-2026-10-30.md#集合整数键缓存与比较参数复制2026-11-02)。原273状态与整体PG差距不因该局部优化关闭。

当前已接入的普通 UNION ALL 支持全局 LIMIT/OFFSET 表达式，括号与未括号根共用实际执行路径；VALUES/集合根还支持 FETCH 的参数、函数调用和括号表达式等 `c_expr` 计数。
全局计数只看到空本层及真实祖先作用域，不把集合输出别名或分支 FROM 误当作可用变量；分支自己的局部计数单独生效。
执行时 OFFSET 先于 LIMIT，计数为零时不打开分支；动态 NULL count 不限行数，区别于裸 NULL FETCH WITH TIES 的分析错误。
有限且加法安全的动态上限可用于 Top-N，逐次重置；后续 NULL/不限行数撤销旧界限，WITH TIES 不截断同行输入。
未知 OID 输入按实际选中上下文推导声明的支持仍为 partial，计数结果 bigint 不等于所有输入类型 bigint。
受支持的合法窗口标量子查询现已使用真实窗口宿主；本层计数直接调用窗口函数仍经 AST 分析报 `42P20`。
其他集合运算、普通 SELECT 动态计数、完整函数签名推导和原始 QRY-06 功能族未完成。

窗口宿主执行真实行源、过滤、窗口投影和最终子句，支持已验证的 derived/CTE、Main SourceContexts JOIN 与外层关联，
按实际原始子句保留命名继承、ROWS/RANGE/GROUPS/EXCLUDE 和 frame 初始化身份。
排名及受支持的普通窗口聚合保留实际结果类型、NULL 和对象元数据；有限 INTERVAL 除法及 AVG 修正余数换算与舍入。
完整输入键每行按需求只求值一次，匹配的投影、窗口参数/FILTER 和后续 stage 复用拥有值；
241 项输入键协议的规范化值、计数及消息字节与 PG 18.6 一致，frame offset 不参与这种共享。
实际生产者保证最终 ORDER 为既有顺序的兼容前缀时省去最终 Sort，反向/不同表达式等反例仍保留排序；
窗口 WITH TIES 使用已有键并保留边界行预读、OFFSET 和投影/参数的求值需求。

这仍是有限实现：窗口 stage 排序、分区和历史行驻留内存，不受 work_mem 约束且没有 spill；
统计窗口聚合的合法签名仍报 `0A000`，window DISTINCT、内部 ORDER BY、数组值 ARRAY_AGG、
完整聚合族及 temporal in_range 尚未完成。正常执行的这些改进不改变保留计划、参数帧/provider、
portal 或其他既有暂缓范围，也不关闭 QRY-06、QRY-08 或原 273 项。
前批 C9 的 1,000 行共同窗口对照中，first/last/nth 组合相对旧回测约快 5.15–5.84 倍，其他七类在各预算仍较慢；
旧窗口最终排序与新TypedSort的资源策略也不同，不能称全部查询加速或据此更新历史PG倍率。
前批宿主证据和速度限制见[类型化窗口宿主报告](performance-report-2026-10-30.md#类型化窗口宿主与有序输出复用2026-11-03)；
历史失败与慢值保留，不能据此宣称普遍加速或完整回归全绿。

前批 C1 的唯一生产变化是私有窗口实现 `85aef835`：仅在实际非 NULL、非枚举比较中，
双方原始类型相同且为 `smallint`/`integer`/`bigint`，真实已绑定 OID 为对应的 21/23/20 时，
按拥有值的 stage/key 单元惰性解析并缓存精确整数。OID 0、真实 domain、缺权威 OID 的类型别名或隐藏表达式、
全串解析失败或范围溢出均回到原比较 owner；原本支持的表达式不会因此新增拒绝。
三类参数来源、frame/context/provider、Main、共享比较器和 R185 helper 原文不变。

每个缓冲 Input 都增加 vector 头及外层容量占用，回退行也不例外；payload 和 stage OID 元数据按需分配，
close 释放行内 payload/元数据后，外层容量仍可保留至图销毁。窗口仍无 work_mem/spill，累计分配请求下降不等于 RSS 下降。
额外 allocator 18 进程/162 快照全部完成：整数 1,000 行控制首/末行 uordblks 中位差 +68,160 B，
文本 +22,544 B、单行 +48 B，close 后仍有容量保留；快照不能证明精确 live、真实峰值或 RSS 改善。
该 C1 批次的 71 个 native 入口通过（新 36 控制），61 个协议入口为 56 通过、5 失败；
新 143 控制的 14 项既有差异与 C9 全部 actual 相同，原四个失败入口的观察也保持不变。
共同八类 public cursor 独立执行阶段进程 CPU p50 降低 41.85–61.90%、C++ 分配请求减少 53.89–63.96%。
另行完成的整 SQL 24 组中，4 MB/16 MB 的 16 项 p50 均比两次 C9 低 22.3–39.1%；
64 kB 仅 2 项比两旧都快、3 项比两旧都慢（partition_lag、running_sum、partition_full_sum），其余混合。
C9 首/回测在 64 kB 的漂移 +13.2% 至 +75.8% 全部保留；不据此宣布七类慢值全清、统一加速或改写 old58/PG 倍率。
前批 35 项数据正确但 shutdown 失败的整入口仍为失败，本批未重跑。
方法、准入和限制见[窗口整数键缓存报告](performance-report-2026-10-30.md#窗口整数键缓存与实际执行成本2026-11-03)。

中间候选 S2 在同一真实窗口 stage 内复用等价非 volatile 调用；含 volatile 参数的复用还要求真实最终 Sort 保留下层排序输入投影，
并只用于最后一个实际需要的窗口 stage。先保留必须提前计算的完整目标，再抽取推迟目标中的完整窗口节点，
共享与执行次序都服从该序列；不能跨不同 winref、独立 SQL child 或保留包装表达式的子节点随意合并。
原调用/结果列/需求、参数 frame/context/provider 均保留；代表按本次实际需求选取并分发完整拥有值，末个别名 move，不跨行或重开缓存结果。
准备期 v4 惰性构造可能匹配者的身份，并只在本次准备 memo 原 AST 元数据，不改原规划、分组聚合或共享排序 helper。

新 220 项严格 PG 18.6 对照在 S2 全过，前 C1 的 27 项差异全部消除；原 143 项由 14 失败降至 8，原四个失败入口的 1641 项观察保持原样。
72 个 native 入口通过；首次完整协议 62 个入口为 56/6，其中一次连接启动失败时尚未执行该入口的 190 项 SQL。
保留初失败后，同源 S2 独立 190 项复核 0 预期差异，最终 62 个独立入口为 57/5，未把复核计作新增入口或删除失败历史。
以下为相同 512 行 heap、真实已绑定 public cursor 的 warm 区域累计 C++ 分配请求；包含 restart、完整消费、关闭和 owning 接收，初次建图在外。

| 查询控制 | C1 请求次数 | S2 请求次数 | C1 / S2 请求字节 |
| --- | ---: | ---: | ---: |
| LAG 一份 | 226489 | 226490 | 25625206 / 25625214 |
| LAG 两份 | 419723 | 232335 | 48048290 / 26317466 |
| LAG 四份 | 806565 | 244394 | 92906086 / 27713486 |
| SUM 两份 | 44542 | 43514 | 4621224 / 4432054 |

单份控制的额外请求保留；这些不是 SQL 延迟、malloc 全量、live/峰值或 RSS 数据，也不证明 v4 准备时间已改善。
独立资源 55 项全部符合预期，含 40 次完整读取、一次预期终止及 14 项 SET，校验 40,000 行/118,000 单元；64 kB 最终 TypedSort 最多观察到两个临时 FD，EOF 无残留且同目录重开通过。
资源测试不证明窗口预算治理、任意写入恢复或 R15 停机问题已修复。
三预算整 SQL 对照完成 1296 次计时、324 次预热、18 项 SET/SHOW，全部 1,620,000 行/4,725,000 单元符合固定预期。
36 项中仅 10 项的 S2 p50 低于两次 C1，19 项高于两旧、7 项位于两旧之间或相等；共同八类的 24 项有 17 项高于两旧。
重复两份/四份 LAG(length) 的三预算控制分别较两旧降低 37.95–41.66%/64.46–66.16%，单份和重复 SUM 表现混合，不能用重复调用收益代表整体查询提速。
全部慢值、旧版漂移及资源边界见[窗口结果共享报告](performance-report-2026-10-30.md#窗口结果共享与排序投影2026-11-03)。
尚未补齐完整已分析常量/cast 等价、复合 binary 输出或窗口预算治理，原 273 项和旧 C9 停机失败继续保留，不宣称整体兼容或全面提速。

当前 S3 的原类型化循环只用于全 stage 的 base/sorted owner 均未变化、无外部别名指入且两个过滤后调用顺序都等于原序的情况。
其余形状继续使用 S2 通用路径，包括隐藏 ORDER 调用重排、共享组和仅别名被需求的组；原实际需求、参数 frame/provider 及 owning 结果不变。
S3 的 72 个 native 入口通过，完整 62 个协议入口首轮为 57/5。新 220 和原 143 的所有原始观察与 S2 相同，原四个失败入口的 1641 项也保持。
190 项此次首轮完整通过，全部服务端原始观察与 S2 独立复核相同；仅请求中的随机私有命名不同，S2 的首轮 0 SQL 失败及随后复核均保留。
同一 512 行 warm public cursor 中，S3 的 LAG 一份/两份/四份及 SUM 两份请求分别为 226489/232335/244394/43514 次，
请求字节分别为 25625203/26317460/27713474/4432054 B。单份次数与 C1 相同，3 B 差异不视为内存收益；这些也不是 SQL 时钟或 RSS。
同范围资源 55 项独立通过；S3 三预算整 SQL 对照保留 1296 次计时、324 次预热和 18 项 SET/SHOW，共 1,638 次请求，1,620,000 行/4,725,000 单元全部符合固定预期。
全部 36 项按 p50 数值为 11 项低于两次 C1、19 项高于两旧、6 项居间；共同八类的 24 项为 3 低/16 高/5 居间，64 kB 共同八项全部高于两旧。
这是数值分类，不证明统计显著性或全面加速。重复两份/四份 LAG(length) 的三预算控制较两旧分别降低 37.94–40.83%/63.65–66.34%，
C1 首/回测漂移 -11.01% 至 +11.92% 完整保留。这些独立 C1/S3/C1 窗口不是同窗 S3/S2 比值，也不能沿用前批 C1 的改善当作 S3 结论。
S2 全部 36 项及此前两轮 68 请求、48 进程公开 API 分阶段诊断继续在报告中单列，未用诊断推导所有慢项的单一原因。

## 主要能力差距及实际影响

| 领域 | 当前已有能力 | 距离 PostgreSQL 18 的主要差距 |
| --- | --- | --- |
| SQL、类型、目录与 DML | typed AST、常用查询/约束、集合/子查询和多种数据类型的真实执行子集；UNION ALL/VALUES 计数表达式及 FETCH `c_expr`；Main/Core/SQL 子查询的受支持 typed WindowAgg | parse/analyze/rewrite/planner 边界、其余窗口族与帧/类型组合、普通 SELECT 动态计数、完整参数/函数重载推导、目录字段/依赖和事务化 DDL 仍不完整；命令能解析不等于所有组合语义正确 |
| 优化器与计划 | 顺序/受限索引扫描、三类 JOIN、基础统计和代价选择、部分参数化查询 | 缺完整候选 path 搜索、等价类、参数化路径、多表 JOIN 搜索和统计模型；计划质量不能随 SQL 复杂度稳定扩展，见 [OPT-01](issue-opt-01-path-planner-audit.md) |
| 执行内存与并行 | 部分 heap page-range 多线程扫描、排序协作中断；类型化有序 LIMIT 有界堆、非分区 TableScan 按页游标；普通物理单键、已接入的普通类型化/多键及 UNION ALL/VALUES Sort 的会话预算与外部按需归并 | 聚合/HAVING/未接入集合分流、哈希/窗口/JOIN、结果接收器和部分源仍可能全量物化；配置函数/完整例程 GUC、完整资源治理及 worker 生命周期仍缺，见[条款](review-requirement-clauses.json) |
| 索引与修改路径 | B+Tree、Hash 和其他访问方法的有限实现；部分 SELECT 使用真实索引候选 | 其余 AM/opclass、并发/WAL/谓词范围规则和 index-only/bitmap 能力不完整；本轮 UPDATE/DELETE 整数预筛选仍是堆扫描，不是物理索引定位，见[实现与实测](performance-report-2026-10-30.md) |
| 事务与并发 | MVCC、保存点、行/表锁、死锁检测、SSI/2PC 的子集；advisory lock 和 NOTIFY 有完成标记 | 不能写成“没有行锁”；缺完整 snapshot/command counter、子事务、MultiXact、EPQ、索引范围谓词锁和 DDL 回滚一致性，见审计 TXN/VAC 族 |
| 存储、恢复和维护 | 8 KiB 页、缓冲池、TOAST、WAL/CLOG、校验、归档、单节点恢复/PITR 子集；近期修复 WAL 截断后重开 | 自有磁盘格式，不能打开 PostgreSQL 数据目录；全资源 WAL、freeze/HOT/VM、调度节流、多时间线/升级恢复和长期故障验证仍有缺口 |
| 复制与高可用 | 复制槽/发布管理对象和项目格式变更预览 | [复制模块](../src/replication/README.md)没有完整物理流复制、standby、真实订阅接收/应用；变更流仍在内存中，不能承诺故障切换和远端同步确认 |
| 协议与客户端 | wire protocol 3.0、SCRAM、Simple/Extended Query 和部分 binary I/O 的真实子集 | 全消息/类型/错误/portal/COPY/工具行为和客户端版本矩阵未完整验收；当前 CMake 实测产物为 TLS stub，不能据此通过真实 TLS 的 A 门 |
| 监控、配置和运维 | 部分 `pg_stat_*` / `pg_settings`、配置持久化/reload、维护和备份入口 | 视图列/快照、wait events、进度/资源保护和运维演练不完整；`buffer_pool_frames` 未接到实际池，`checkpoint_timeout` 显示与后台间隔不一致，见[手册](MANUAL.md#25-guc-参数配置) |
| 扩展与生态 | 少量内置能力与项目扩展命令 | `CREATE EXTENSION` / FDW 等没有 PostgreSQL 插件运行时；缺 extension/fmgr ABI、hooks、动态后台 worker 和常用扩展兼容，不应视为可直接运行 PostGIS 等扩展 |

PostgreSQL 的对标能力以官方文档为准：[并行扫描/JOIN/聚合](https://www.postgresql.org/docs/18/parallel-query.html)、
[工作内存和临时文件](https://www.postgresql.org/docs/18/runtime-config-resource.html)、
[物理复制与 standby](https://www.postgresql.org/docs/18/high-availability.html)、
[逻辑复制接收/应用](https://www.postgresql.org/docs/18/logical-replication.html)、
[扩展安装运行时](https://www.postgresql.org/docs/18/sql-createextension.html)。
这些是能力差距，不意味着开启并行或 JIT 就必然加速所有 SQL。

## 性能距离：分别看内部改善与 PostgreSQL 对照

2026-10-30 的[性能报告](performance-report-2026-10-30.md)证明了本项目部分路径的改善：
UPDATE、DELETE、保存点、排序和小表 ALTER 收益明显；普通点查和单行 INSERT 没有证明稳定提速。
35 类操作、315 个最终样本和旧/新各 210 个公共窗口样本，只比较本项目版本，不能用来推算与 PostgreSQL 的速度差。

本次对 PostgreSQL 18.6 补测了相同的 35 类操作，每侧 210 个计时样本，现有 oracle 均通过。
以下为其中几个代表操作，完整 35 类结果和方法见[新增对照](performance-report-2026-10-30.md#postgresql-186-同机对照2026-11-02)：

| 1,000 行夹具中的操作 | 本项目 p50 ms | PostgreSQL 18.6 p50 ms | 本项目/PG 延迟比 |
| --- | ---: | ---: | ---: |
| 普通主键点查 | 15.280 | 0.360 | 42.5× |
| INSERT 单行自动提交 | 20.323 | 0.625 | 32.5× |
| UPDATE 主键单行 | 55.423 | 0.653 | 84.9× |
| DELETE 主键单行 | 26.934 | 0.573 | 47.0× |
| ALTER ADD COLUMN（5 行） | 920.287 | 0.916 | 1004.3× |
| CHECKPOINT | 19.966 | 13.762 | 1.5× |

这是两边采用各自默认内存/维护参数的单客户端、热缓存小样本；没有统一内存额度或进行长时间压测。它只回答指定 SQL、夹具、默认设置及采样窗口的往返延迟，
不证明大表、冷缓存、并发吞吐、p99、故障下的可用性或完整业务负载达到 PostgreSQL 水平。
不能把某一条 SQL 的倍率当作“整体性能百分比”，也不能与 10 月 30 日的内部倍率相乘。

当前代码仍有明确的性能扩展瓶颈：

- 普通修改和部分 DDL 的物理快照、摘要校验及目录/文件工作量随数据库状态增加；5 行 ALTER 变快不等于大表在线 DDL。
- UPDATE/DELETE 的必要条件堆预筛选减少了解码，但没有消除整表扫描；整数 JOIN 预筛选也没有消除全部候选组合和物化。
- 普通类型化排序 LIMIT 已限制候选，普通非分区 TableScan 已按页供给；其他旧分流、分区/RLS/并行等物化及完整工作内存/spill 仍未完成。
- WAL、提交持久化和后台维护等待仍影响单行与尾延迟；不能用关闭 fsync、放宽语义或忽略错误来换取“对标”。
- 完整代价/统计/多表搜索、并行执行和异步 I/O 能力仍有差距；先消除当前常见路径的额外工作，再判断高级加速是否值得。

后续 Top-N 批次已修复可支持的单表有序 LIMIT 在执行分流中丢失多键顺序或提前计算投影的问题：
新增 48 条协议检查在 PG 18.6 与新版全过，旧版有 11 条失败；独立旧/新/旧的 1,000 行 LIMIT/OFFSET 延迟约改善 1.4 倍。
这批比较没有重新测量 PG 延迟，不能用新值除以上表旧 PG 样本；详细边界见[性能报告](performance-report-2026-10-30.md#有界排序-top-n2026-11-02)。

按页扫描批次又完成 23 个原生/17 个协议入口及 35 类操作有限回归。已有类型化数组投影 LIMIT 1 在最终 1,000 行夹具窗口约快 1.38–1.47 倍，
该扫描批次尚未优化普通列投影 LIMIT 1；新增写函数快照 13 条与 PG 18.6 一致。内存改善只指扫描保留行数据，
不是全进程/RSS 或所有查询均已流式，见[扫描报告](performance-report-2026-10-30.md#按页物理扫描2026-11-02)。

最新普通 LIMIT 批次将可支持的单表查询接入该扫描链：完整 62 条专项在 PG 18.6/候选全过，冻结旧版 9 条失败，
普通列 LIMIT 1 在相邻 1,000 行旧/新/旧窗口约快 2.3 倍。24 原生入口通过，完整所选 21 协议为 20 通过、1 失败；
后者是 R304/R305 已暂缓的完整 463，旧/新相同 40 处差异原样保留。没有恢复其调查或把整批写成全绿。
不限 portal 行数的运行错误前缀已支持，完整 portal/网络流式和其余原始功能族仍开放；详见[普通 LIMIT 报告](performance-report-2026-10-30.md#普通-limit-按需执行与错误前缀2026-11-02)。

普通物理排序批次以 `d09cb6c1` 为输入，把实例工作预算和临时文件归并接入受支持的真实单表单键排序。
完整 47 条 PG 18.6/最终程序全过，冻结旧版 1 条整数序号排序失败；当批 core 29 原生通过，实际 Main 所选 22 协议为 21/1。
已有暂缓 Bind 的 463/40 观察一致；前一候选 COPY 超时及隔离旧/新/旧失败仍保留，最终通过不证明 R197 已修复。
单键无界排序在最终两个窗口约快 1.27–1.40 倍，部分其他控制更慢；没有重测 PG 延迟或证明所有查询无回退。
该排序历史批次尚未接入会话 SET，类型化/多键等其余排序、其他算子及结果缓冲仍需推进，见[外部排序报告](performance-report-2026-10-30.md#普通物理排序的内存预算与外部归并2026-11-02)。

最新普通物理 Sort 完成最终按需归并及绑定恢复，31 原生通过，完整 22 协议仍为 21/1，排序 47 条 PG/新版全过。
64 KiB、8,192×2 KiB 的原生外排对照约快 1.78–1.86 倍；504 个小表 SQL 样本未证明稳定提速，不推算与 PG 的延迟倍率。
既有暂缓差异保留，完整范围及慢值见[最终归并报告](performance-report-2026-10-30.md#最终归并按需输出与排序绑定恢复2026-11-02)。

最新会话 work_mem 批次支持 utility SET/LOCAL/SHOW/RESET、启动与重载优先级、事务/保存点，并接入上述普通物理 Sort。
36 原生及新 163 控制通过；完整所选 25 协议为 24/1，新 470 条在 PG/最终程序全过，旧版 404 不符，原有 463/40 暂缓差异保留。
实际 Main 31 个资源控制/4,000 单元格核验通过；504 SQL 样本与 600 CPU 诊断仍含慢值，不宣称性能等价或全面无回退，见[会话工作内存报告](performance-report-2026-10-30.md#会话工作内存与实际排序预算2026-11-02)。

## 应如何缩小差距

以下是基于现有缺口的技术顺序，不是工期承诺，也不恢复用户暂缓项：

1. **先明确可支持的子集及正确性门槛。** 对常用 SELECT/INSERT/UPDATE/DELETE/事务/DDL 建立值、类型、SQLSTATE、可见性、回滚和重开的一致回归。
2. **降低常见操作的固定和随库增长的开销。** 在保留 MVCC/SSI/RLS 和持久化保证的前提下减少物理快照/扫描/物化、增加真正的 DML 索引候选定位。
3. **完成资源治理和计划选择。** 把内存参数接入执行、实现 spill 与可取消的 worker/I/O；补非暂缓范围内的统计与访问路径/多表计划选择。
4. **验证规模和长期运行。** 同机分级数据量、1/4/16/32 客户端、只读/写多/混合负载，记录吞吐、p50/p95/p99、CPU/RSS/I/O/WAL、错误与恢复；明确后台维护及相同耐久性语义。
5. **再扩大生产与生态目标。** 完成非暂缓的恢复/备份/复制/工具链门；安全、权限、TDE 和已明确暂缓的专项仍按原范围处理，缺口继续如实列出。

PostgreSQL [pgbench 的测试建议](https://www.postgresql.org/docs/18/pgbench.html)要求足够长的重复测量，并提醒数据规模、
竞争、死元组/维护和客户端瓶颈会改变结果。正式性能目标应预先约定工作负载与验收线，不能先拿小表最佳样本再反推“达到 PostgreSQL”。

## 本次更新与核查范围

本页替换了旧对比页中失效的代码行数、开发年限、137+2 测试数、95% 语法覆盖等估计，以及“没有行锁/完全没有并行”等过时总结。
旧版本可以从 Git 历史查看；逐项失败和局部修复证据仍在[审查索引](review-index.md)，没有因重写对比页删除。
273 项总账、原子条款和暂缓文件未改；[最新构建与文档核查](documentation-inventory.md)和[功能测试报告](test-report.md)继续限定各自批次。
