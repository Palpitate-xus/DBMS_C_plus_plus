# Changelog

## 2026-11-08：numeric 十进制对数

为 numeric `ln`、`log` 和 `log10` 引入十进制计算，补充协议、默认值、回滚与冷重启回归。已测范围、1000 位限制和诊断差异见[测试报告](docs/test-report.md#numeric-log-decimal-20261108)；新旧查询性能测量波动及负向结果见[性能报告](docs/performance-report-2026-10-30.md#numeric-log-cost-20261108)。

## 2026-11-08：PostgreSQL 小表查询对照

补充 PostgreSQL 18.6 与 D43 单连接小表查询的有限性能对照，包括未达计时门槛的查询及共享主机限制；原始记录留在本地。[性能报告](docs/performance-report-2026-10-30.md#pg18-d43-cross-20261108)。

## 2026-11-08：开源仓库清理

将 benchmark 脚本、原始数据、压缩证据及其生成索引移出 Git 跟踪并加入忽略规则；本机已有文件保留。移除误跟踪的 Python 字节码、日志和测试数据。保留项目代码、测试夹具、构建脚本和文字报告；历史提交中的大对象不会因新提交自动消失。

## 2026-11-08：内置函数签名缓存

内置函数的固定签名改为只读、进程生命周期的共享声明，避免在绑定和调用时反复创建；数组位置函数的参数相关签名仍逐次生成，同名函数覆盖和参数求值路径保持既有语义。选定回归与异常分配注入的范围见[测试报告](docs/test-report.md#d43-signature-cache-20261108)。

在共享主机上，同项目新旧版本的 64 行查询中，部分函数有改善，但旧版自身漂移较大，不能据此宣称全库提速；逐项结果与限制见[性能报告](docs/performance-report-2026-10-30.md#d43-signature-cache-20261108)。D43 的对数精度与零列缺陷当时仍未修复，详见测试报告。

## 2026-11-08：十进制指数与浮点函数类型

`exp(numeric)` 改用十进制计算，保留 PostgreSQL 18.6 的结果标度与中间舍入行为；`exp`、`ln`、`log` 和 `log10` 按参数类型区分 numeric 与 double precision 重载。此前 `exp(100::numeric)` 和 `exp(-40::integer)` 的错误结果可能被写入默认值，现有针对这些示例的查询及持久化回归用于防止再次发生。

本项目 Numeric 的有效精度和 scale 仍各限 1000 位，部分 PostgreSQL 可计算的结果返回 `22003`；D40 的 `ln`／`log` 高精度数值路径存在差距（当前状态见上方），D39 的部分 binary Numeric 输入边界在 D40 尚未重新验证。有限测试不代表全面 PostgreSQL 兼容；六条简单 SQL 的新旧测量波动较大，部分变慢，不能宣称全库提速。详见[测试报告](docs/test-report.md#d40-numeric-exp-20261108)及[性能报告](docs/performance-report-2026-10-30.md#d40-numeric-exp-cost-20261108)。

## 2026-11-08：大数格式与除法（D39 历史）

本轮修复部分大 numeric 定点／`V` 格式在展示补零或移位时触及项目内部精度上限的问题；限定的模板与类型现在按十进制舍入。整数长除法改为复用局部余数缓冲区，减少反复分配；不改变 1000 位上限。`exp`／`ln`／`log` 不再把部分超出 double 范围的 numeric 静默当成零，精确的 10 的幂采用逐位验证和范围检查。无法精确支持的部分输入会明确返回 `22003`，不是 PostgreSQL 等价。

**D39 历史风险：** `exp(100::numeric)` 曾与 PostgreSQL 不同，某些 integer／float8 的极小 `exp` 曾错误返回零，这些值可作为默认值持久化。后续实现及当前限制见上方最新状态；D39 并未解决完整十进制超越函数、默认值来源或复杂格式模板。

D39 正常冻结候选的 61 个选定原生入口与 27 组既有协议对照通过，完整套件未运行。针对定点格式的每版本 1,365 个动作涵盖直接查询、事务和冷重启，已测 `FMPL` 默认值与 PG 一致；复杂模板仍有差距。组件基准有 CPU 改善，真实 SQL 窗口有快有慢且漂移明显，不能声称整体数据库提速。详情见[测试报告](docs/test-report.md#d39-numeric-20261108)与[性能报告](docs/performance-report-2026-10-30.md#d39-numeric-cost-20261108)。



## 2026-11-08：NaN、科学数字格式与目录扫描

本轮修正 `div(NaN, 0)` 的处理顺序：NaN 先于零除数处理；有限值和无穷除以零仍报 `22012`。简单 `9`／`0`／小数点模板的 numeric 和 bigint `EEEE`／`eeee` 科学记数格式改为精确十进制舍入，避免极大或极小值因转换成二进制浮点而静默显示为零。integer 等其他类型仍沿原路径；含 `G`、引号、`FM` 等复杂模板的既有差异未在本轮解决。

Linux 上，目录发布状态的一次扫描复用已打开的父目录句柄，仍逐个检查目录文件的身份和时间戳，并在结束前核对路径仍指向同一目录；描述符不可用时回退旧检查路径，非 Linux 保留旧路径。扫描期间目录换出后又换回等竞态仍不能保证检出，不能称作跨进程线性一致。

D37 正常冻结候选的 58 个选定原生入口实际退出 0；27 组既有协议的 1,105 对回复没有未解释变化。NaN 的 82 个动作在 PG18.6 对照中结果、SQLSTATE 和事务状态一致；简单科学格式的 210 个样例与 PG 的结果及描述符一致。完整套件未运行，其他模板、无约束 numeric 范围、错误回复帧及默认值来源仍有差距。此前文档称 `div(NaN,0)` 不同属于历史结果，已被本节覆盖。详情见[测试报告](docs/test-report.md#d37-numeric-catalog-20261108)和[性能报告](docs/performance-report-2026-10-30.md#d37-catalog-scan-20261108)。



## 2026-11-08：数值范围和 to_char 默认值

本轮修正部分 numeric 范围错误被 DEFAULT 当作字面量回退的问题：已求值的算术范围异常转换为 `22003`，整数商和余数使用直接整数除法，避免舍入或过大的中间乘积；`to_char` 已绑定的数值格式化范围异常也转换为 `22003`。这些处理仅覆盖对应异常和已测路径，其他无结构错误仍可能回退原文。旧式原生默认值兼容有变化：原文恰为这些会越界的表达式时，现在报错。

PostgreSQL 18 的无约束 numeric 支持的范围远大于项目当前 1000 位内部上限；合法输入、中间值和结果都可能触及项目限制。[PostgreSQL 数值类型](https://www.postgresql.org/docs/18/datatype-numeric.html)。`div(NaN, 0)` 的既有差异、其他函数的未结构化错误回退、目录锁等待和 `pg_attrdef.adbin` 等差距仍未解决。旧 D30 正文对所测算术默认值“写入原文”的描述属于历史结果，已由本节局部修复；不代表所有默认值问题均已修复。

D35 冻结候选 55 个标准原生入口与 1 个二进制入口均实际退出 0；11 组数值和默认值协议对照、27 组 D34→D35 逐动作比较和选定 SSI 对照已完成。`to_char` 大数默认值现返回 `22003`，已测 INSERT、UPDATE、批量插入和 ADD COLUMN 不再写入 SQL 原文；PostgreSQL 对该合法输入返回格式化值，仍不等价。未运行完整测试套件。详细计数、旧失败与性能测量限制见[测试报告](docs/test-report.md#d35-numeric-tochar-20261108)及[性能报告](docs/performance-report-2026-10-30.md#d35-numeric-tochar-performance-20261108)。



## 2026-11-08：D30 目录投影与活动事务退出（草稿）

本轮对普通 `pg_attrdef`／`pg_depend` SELECT 使用实际目录字段的按需快照，声明与数据在同一把锁下复制；公共完整快照仍保留默认表达式和依赖。`pg_attrdef.adbin` 的 PostgreSQL 节点树仍未实现，不声称目录完整等价。活动事务与 SSI 登记改为进程寿命，避免全局数据库对象退出时访问已析构的登记容器。

D28 新投影测试因误以为 ALTER 后目录行顺序不变而断言失败，原进程真实退出 `-11`；隔离 ASan 运行时拦截重放实际退出 `-6`（未强杀），报告活动事务登记容器在退出时二次释放。D29 修正测试的列身份断言，但未修生产退出问题；D30 对普通及 SERIALIZABLE 活动事务正常退出、重启后未提交行不可见／已提交前缀保留、后续写入提交作了实际控制，普通和 ASan runtime-only 均实际退出 0。后者仅加载 ASan 运行时库，不是全核心插桩；记录保留原有失败。

D30 实际通过 52 个选定标准原生入口及 1 个独立二进制入口，合计 53；23 组协议与 D28 的 994 条成对回复在行、tag、SQLSTATE、Ready、T7 和错误／通知上相同。针对投影的 PG／D27／D28／D30 热冷对照共 76 条回复，按实际 OID 对应后选定行、tag、SQLSTATE、Ready 无差异，但 PG 原始行描述符 T7 仍有 4 项差异。完整发现审计的 2,023 条回复包含历史和参照，不能说成 2,023 条新测试；未运行完整测试套件。

已知缺陷仍未修复：合法默认表达式 `(('1e500'::numeric * '1e500'::numeric)::text)` 在 PostgreSQL 18.6 得到 1001 位结果，而 D27／D28／D30 会把 SQL 源文写入 TEXT；现有无约束 numeric、持久默认值来源和无 SQLSTATE 回退均需继续修复。并发对照中 PostgreSQL 可在另一会话 DDL 未提交时立即读取旧目录，D27／D28／D30 的同类查询等待超过 5 秒，ROLLBACK 后才返回；未采集线程等待栈，库级锁路径只是依据代码和时间顺序的推断。

仅对选定单连接暖缓存工作负载测量：D27→D30→D27 固定顺序的目录读取窗口为 27 个实际正常退出进程、27,648 条 SQL；事务窗口为 9 个进程、3,456 条计时 SQL，另有 324 条控制 SQL。两窗口完整回复字节一致、无 mismatch。各场景结果有正有负，ordinary `pg_attrdef` 相对前／后基线墙钟分别退化 5.8%／6.5%，两项生产变更同时存在且每角色仅 3 次，不能据此归因或证明投影改善，也不能宣称整体提速或与 PostgreSQL 性能等价。


## 2026-11-08：DEFAULT 错误与目录快照（D27，未接受草稿）

本轮修正 INSERT 遗漏列 DEFAULT 求值中已有 SQLSTATE 或有效结尾 SQLSTATE 标记的错误传播，并对 substring 负长度保留 `22011`；目录声明读取按需省去 DEFAULT 对象图复制，公共默认快照仍完整。没有 SQLSTATE 且没有有效标记的旧/native 求值失败仍可退回原始字面量；域、旧数据及全部错误类型不构成全面兼容承诺。

D24 的 `catalog_default_snapshot_demand_test` 实际退出 `-11`，诊断指向进程退出时内置目录声明生命周期问题；D27 的 ASan 只注入运行时库而非全核心插桩，该控制正常退出 0，不能称为完整 ASan 构建。D26 的 `regexp_replace('abc','[','z')` DEFAULT 在热态和冷重启后均错误写入表达式文本；D27 返回 PostgreSQL 的 `2201B` 且未写入行。历史失败记录保留。

最终 D27 49 个常规原生入口加 1 个二进制控制，共 50 个实际退出 0；22 个协议族独立审计覆盖 1,998 SQL、76 个正常结束 Main。五族 DEFAULT 错误独立对照记录 342 SQL、48 个正常结束 Main，所比较语义字段没有候选差异；两个族的原始行描述符 T7 仍有差异，不能称为完整 wire 或 PostgreSQL 等价。未运行完整测试套件，功能仍为未接受草稿。 指定成本测试为共享服务器暖缓存、单连接、32 行热表：D23 与 D27 按每场景旧版前/新版/旧版后各 3 个进程固定顺序执行，共 27 个正常结束 Main、36,720 条 SQL；各场景完整回复字节一致。24 组墙钟和 CPU 结果有正有负，narrow 前后基线波动尤大；没有证明整体加速，也没有 PostgreSQL 速度对比。 [完整证据](docs/performance-report-2026-10-30.md#default-runtime-snapshot-evidence-20261108)。


## 2026-11-07：DEFAULT 对象依赖（P43/D23，未接受草稿）

- 增加独立的持久默认值身份、所属列自动依赖和真实对象常量正常依赖；SET DEFAULT更换身份，DROP DEFAULT／删列／删所属表清理对象，列压缩时重映射所属序号。
- 绑定REGCLASS／REGTYPE常量及实际内建nextval参数，保留显式TEXT的运行期查找；选定RESTRICT／CASCADE、SERIAL、NULL默认值、回滚和冷读通过新回归。
- 46项原生入口actual0、两份正式协议脚本通过，正常性能NOTRUN。原始大JSON仍仅跟踪gzip，另清理build冗余副本。[证据和未完项](docs/performance-report-2026-10-30.md#attribute-default-objects-draft-20261107)。


<!-- P43-D20 unaccepted draft checkpoint -->
## 2026-11-07：旧物理数组目录与首次启动修复（P43/D20，草稿）

- 直接读取持久schema核实数组元素，完整预检后同步真实数组OID、物理属性和已有类型依赖；保留长度修饰符、统计设置及合法存储／排序规则，用户定义类型保留其目录声明。纯冷读仍显示原始存储值。
- 修复全局引擎早于动态类型映射初始化时查找返回0的问题；新增真实新进程启动回归。发布后的冲突拒绝修复，失败不暴露部分修改。
- 42个选定原生入口、正式35／9条协议回归及真实旧库21条操作链通过。D19原断连失败、初版审计器错误均保留。正常性能NOTRUN，原273项不关闭；[证据与边界](docs/performance-report-2026-10-30.md#physical-array-catalog-upgrade-draft-20261107)。


<!-- P43-D18 unaccepted draft checkpoint -->
## 2026-11-07：P43/D18 草稿版本检查点（未接受）

- 保存身份列 DEFAULT／序列类型参数／REGCLASS 值和系统目录实现草稿；校正实际目录的6个 `C` 排序规则字段，并从真实绑定来源生成 `pg_type` 的协议来源信息。
- 39个选定原生入口、35条身份冲突与9条目录正式协议回归实际通过；全套未运行，正常性能 `NOTRUN`，不关闭功能项。
- REGCLASS 默认依赖、旧数组元数据迁移、序列完整生命周期、生成列 ALTER TYPE 和其他协议／恢复缺口继续开放。默认程序不变、未推送。证据与边界见[报告](docs/performance-report-2026-10-30.md#identity-sequence-catalog-draft-20261107)。


## 2026-11-07：无写入事务提交优化（P42，partial）

- 为同一WAL owner记录事务起始位置，严格无变更条件下省去无用COMMIT／CLOG发布；写入、DDL、序列与TEMP终止动作保留保守路径。
- 新正式原生回归验证零WAL、完整数据、并发快照、真实fsync和新进程恢复；95条SQL旧新逐字节相同，33个选定原生入口仍有2项未通过。
- [性能报告](docs/performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)保留621次新正常时钟、135行比较及全部负向；八份原文只插入新块，新增压缩证据，校验后清理本批副本。原273状态与所有用户暂缓保持，默认程序未提升，Goal active。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## 2026-11-07：VARCHAR声明／长值与TOAST搜索

TOAST每次写入一个长值时，等长2KB分块从本次上一个成功页继续搜索，避免每块重新扫描前面的已满页；较短末块从头寻找空洞，插入／索引失败仍保留原回退。游标不跨值保存，页格式、Fletcher-16校验、zlib、WAL／CRC32C、锁与持久化屏障保持。这个局部游标没有实现PostgreSQL关系级FSM或bulk-insert完整策略。

本批功能有进展，长值写入部分提速；全面提速尚未成立。新增1MiB混合插入墙钟中位1801.508072ms，相对旧bf9首测／同旧回测-10.720%／-10.799%；更新RETURNING 411.759537ms，-13.372%／-12.280%；重新插入1431.222130ms，-9.197%／-10.414%。对应服务器CPU分别-12.689%／-12.822%、-20.934%／-21.267%、-12.666%／-12.762%。

负向保持OPEN：新长值DELETE RETURNING墙钟43.553088ms，+9.954%／+23.830%，服务器CPU+67.962%／+39.067%；首次VACUUM墙钟25.753918ms，+22.574%／+1.265%；FULL墙钟218.226818ms，+32.204%／+21.215%，服务器CPU+7.756%／+1.774%。新增完整值读取墙钟+2.080%／-2.014%，结果混合。通用270项矩阵的30个区域，墙钟6低于两旧／18高于两旧／6居间；服务器CPU10／8／12，客户端CPU6／14／10。没有排除、重采或把漂移归因为已证实的产品原因；全部负向与原因继续开放。

旧typeMod=-1按原dsize字符上限读取，不从65535猜测成无界；新显式正长度和私有-2保存在已有ITM1扩展，-2输出PG时为-1。旧数据可以由新版读取；旧程序没有识别这些新VARCHAR修饰符，不能承诺降级读取新写的schema。旧无界意图缺少可判定来源时不自动迁移，须明确新DDL／ALTER。名义dsize、SQL字符数和实际UTF8字节数分别处理。没有改变元组布局、校验、TOAST指针格式或WAL格式；仍是项目自定义存储，不是PG物理格式等价。

原273状态不变，新增partial历史291。[完整报告](docs/performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。Goal active。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 2026-11-07：FULL限定关系刷盘与可回退硬链接备份

普通VACUUM FULL在备份前与重建后，只刷新目标逻辑关系及其分区／TOAST缓存，避免把其他表的脏页一并刷出。物理文件已同步，且在任何重建写入前全部unlink：这些文件优先用硬链接保留旧inode作为回滚映像，失败则完整复制；schema和specialized-dirty元数据继续独立复制。每份备份的fsync、完整大小／模式／SHA256校验，以及manifest、pending、替换文件／schema、WAL、COMMIT和undo的持久化顺序全部保留。本批只改TableManage.cpp的vacuumFull，没有修改全局缓存、index_file、WAL格式、CRC或恢复授权规则。

本批只证明固定字段FULL墙钟低于两侧旧版：-11.57%／-1.51%；单长／多长FULL服务器CPU均较低，墙钟各为-3.35%／+1.84%、-1.35%／+3.87%，结果混合。没有证明数据库全面提速，也没有追回P39相对更早84bb的全部恢复成本。

明显负向保持OPEN：首次普通VACUUM多长字段墙钟C1中位67.010218ms，旧c8ca首测30.951480ms／回测25.722458ms，即+116.501%／+160.512%。原三次样本分别为C1 [71.510152,26.679347,67.010218]ms、旧首测[35.992784,30.951480,25.821788]ms、旧回测[25.722458,23.927581,29.365169]ms，全部保留。单长字段大批重新插入的墙钟也+51.70%／+58.00%。未因负向重跑、排除样本或更改预期。

新增完整值／冷开／脏页owner／复制回退正式测试，原273状态不变，新增partial历史290，Goal active。[报告](docs/performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 2026-11-07：FULL物理代次与持久化回滚

修复普通VACUUM FULL仍沿用旧物理身份、冷启动把旧页镜像重放到压缩后堆的问题。每次重写分配新物理代次；同一内部事务记录出生、旧代次退休意图／完成及COMMIT。替换前先持久化只属于该关系的原文件、完整模式／大小／SHA256清单及pending记录，替换文件与schema全部落盘后才提交。失败和未提交崩溃先恢复旧文件／schema，再重放WAL；已提交只清理备份。完成回滚的durable receipt防止遗留备份覆盖后续已提交DML。旧09 schema仅在FULL成功时发布新代次及专门的0x23完成记录，普通旧名称重用仍拒绝错误归属。没有放宽CRC、物理身份或页缺口校验。

新增vacuum_full_cold_recovery_test使用同一源码对照：旧84bb两种情形均在首次冷开出现重复可见主键（父actual1，4个子阶段已执行／2失败／2后续NOTRUN）；C1为6个子阶段全部符合实际终态，正常关闭或主动SIGKILL -9后连续两次新进程冷开全值／NULL／主键通过。原堆352256字节（43页）压到32768字节（4页），之后真实UPDATE到1MiB TEXT、INSERT、DELETE并提交。两份WAL共1194条独立CRC32C核对，旧代次578个页镜像最大块42、新代次7个镜像，退休完成及提交顺序正确，0个检查点，不能归因于checkpoint绕过。

本批没有证明全面提速。270项原样读写矩阵保留27个全新数据库、54个正常Main及1098条完整回复。30个区域的墙钟：7低于两旧／13高于两旧／10居间；服务器CPU：5低于两旧／14高于两旧／11居间；客户端CPU：7低于两旧／12高于两旧／11居间。所有90项比较均列出，性能原因OPEN。 vacuum-full-fixed墙钟 +64.55%／+78.39%；vacuum-full-fixed服务器CPU +10.12%／+10.45%；vacuum-full-single墙钟 +78.30%／+112.77%；vacuum-full-single服务器CPU +17.97%／+38.94%；vacuum-full-overflow墙钟 +131.53%／+114.90%；vacuum-full-overflow服务器CPU +18.05%／+13.79%（相对旧84首测／同旧回测，正数为更慢）。

原273状态不变，新增partial历史289，Goal active。[报告](docs/performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：解开标量TEXT逻辑值与名义堆宽度

修复标量TEXT把持久化名义宽度65535误当逻辑值上限的问题：统一的字符字节限制函数对非数组TEXT使用1,073,741,819字节上限，并让旧式UPDATE校验也使用该函数。既有TOAST读回、插入和行构造因此一致处理完整长值；表结构dsize仍为65535，内联引用和存储格式不变。实际验证到1MiB，未进行接近1GiB的端到端容量验证。旧数据可由新程序读取；旧程序不能读取新写入的超64KiB TEXT，不能据此承诺降级后的值兼容。

新增large_text_toast_test在同一冻结源码上形成33检查：旧版21失败／actual1，当前0失败／actual0。全字节覆盖65535、65536、70003、UTF-8 90012字节／30006字符、可压缩131076字节、随机1MiB、NULL、空串、文本NULL及类似marker的普通串；同时验证第二长字段100003字节、旧式和optional更新的完整返回值、事务回滚／提交、普通VACUUM、热态VACUUM FULL、TRUNCATE后重插1MiB，以及有界VARCHAR的字符数和尾空格约束。

本批完整1500次正常计时没有证明全面提速。仅ID查询服务器CPU比旧版首测／回测高1.78%／1.86%；单长字段首次VACUUM CPU高10.98%／13.76%，墙钟高3.66%／9.08%。多长字段首次VACUUM墙钟低18.80%／高3.52%，CPU低0.33%／高3.27%，属于混合结果。全部负向、旧版漂移、首轮与长尾保留，原因OPEN；本批交付为TEXT容量修复，没有用局部结果宣称整体性能改善。

另发现旧／新均存在的普通FULL恢复缺陷，未关闭；273项状态不变，新增partial历史288。[完整报告](docs/performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：使用所属物理元组识别TOAST引用

修复普通 VACUUM 的长字段引用收集：在原有页锁和 pin 内，直接读取所属物理元组的 NULL 位图、变长字段目录和有界字节跨度，不再通过可能读取调用者 RID/NULL 状态的执行器取值接口。固定区大小与变长列索引每次维护预先计算，普通内联值无需复制整行或逐值构造字符串。活动快照保护、所有物理存活槽的遍历、锁／pin、索引／分块校验和回收策略保持。

新增 vacuum_toast_physical_reference_test 在同一冻结源码上执行全部16种情形：旧版160检查／48失败／actual1，C1为160／0／actual0。覆盖普通表与原生范围分区存储、vacuumToast及外层vacuum、真实同库／跨库同名表调用者、跨位图字节的NULL、空串、文本NULL、UTF-8、类似marker的普通字符串、TEXT11003／40003及BYTEA70003／11009。独立完整值物理解码核对16种情形中的80个存活对象全部原始字节，以及每个真实孤儿的回收、调用者绑定恢复和第二次维护幂等。

本批1500项完整正常计时没有证明全面提速。在共同的真实after-DELETE种子上，单长字段首次VACUUM服务器CPU比旧版及旧版回测低8.36%／7.73%；多长字段首次VACUUM墙钟高9.83%／27.97%，CPU分别低1.92%／高35.29%。在共同after-UPDATE种子上，仅选ID的CPU高8.43%／8.39%，墙钟高9.77%／14.77%；三个全列查询墙钟也高于两次旧版，完整正负结果保留，原因OPEN。

原273项状态不变，新增partial历史287；既有toast_test失败和PG差异保留。[完整报告](docs/performance-report-2026-10-30.md#vacuum-physical-references-20261107)。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：普通物理扫描／排序投影 NULL 元数据复用

扫描展开长字段时，借用该物理行已捕获的 NULL 位图；排序后的投影在核对真实来源、具体 Sort 类型和全部物理列声明后，借用排序已保存的位图。借用仅存在于一次取行调用内，退出与异常均恢复原绑定；表结构不匹配、Sort 子类及其他来源继续原路径。没有新增页／allocator 缓存或改变文件身份检查本身。

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

原273项及暂缓范围保持，新增1156项检查与完整990操作开销证据。[性能报告](docs/performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
## 2026-11-06 — 视图关系／列依赖与破坏性 DDL 保护

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

合法编译目录读来源由完整查询绑定校验，别名保留实际列身份；旧视图依赖只读恢复。新增原生 242 检查、7 既有专项、真实 PG／旧库／保存点对照通过有限范围验证；[报告](docs/performance-report-2026-10-30.md#view-column-dependencies-20261106)保留 270+27 操作全部开销及退化。本批没有整体性能提升结论，PG 家族仍 partial、Goal active。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
## 2026-11-06 — 普通部分列投影按需求读取 TOAST

- 仅为直接物理游标的部分列投影解码所需字段；维持选中长字段错误、NULL 与虚拟列完整输入。全列使用原路径。
- 新增 14 组/83 项回归，并复核 5 项相关原生测试及全部普通协议回复。
- 完整保留 C1 回退及 C2 的各 270 项正常性能对照、漂移和长尾；更新[性能报告](docs/performance-report-2026-10-30.md#projected-toast-demand-20261106)，证据仅以压缩归档提交。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
## 2026-11-06 — owned pg_class declaration (partial)

- Persist the actual `pg_class` identity, all34 attributes, rowtype83/array273 and required metadata-only types through the existing locked startup owner. Validate loaded declarations/dependencies and reject conflicting bootstrap input before edits.
- Use the PostgreSQL18.6 `aclitem` declaration (16 bytes, alignment `d`); keep unavailable value/result codecs explicit, including NULL input `0A000`.
- Add the550-check native regression, full unfiltered41-query PG comparison, real old-image upgrade/cold evidence and one complete270-timing control cohort. Performance regressions, virtual origins and remaining catalog/value functionality stay open. [Report](docs/performance-report-2026-10-30.md#pg-class-declaration-20261106).
<!-- P33 pg-class-declaration end -->

本项目的所有显著变更记录于此文件。
格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

- 普通日志以每次O_APPEND/writev追加，保留原字节、权限、轮转及打开错误行为；新增13组原生回归。正常30案例服务器CPU为16低/6高/8居间，多LOB回退及原9项协议差异保持，未证明一致提速，默认程序未替换。见[完整报告](docs/performance-report-2026-10-30.md#legacy-log-append-20261106)。

- 补充真实主键查询CPU诊断：同一三表闭库状态、三个正常进程、1,536次完整目标查询及1,575响应独立复核；原始PC/符号与全wire压缩归档，保留P30回退和目录差异。明确pg_class完整声明的类型准入依赖，本批没有生产修复或提速验收。
- 整理文档入口：补齐 754 份 Markdown 的分类清单，近期重复状态段落集中到审查索引；增加本地文件链接、空文档、整份重复与清单覆盖检查工具，明确当前验证与历史记录的边界，并同步手册中的排序取消检查范围。
- 修复检查点截断后首个保留 WAL 段从记录中间开始时无法重启的问题：利用现有已持久化检查点和经过 CRC/链衔接检查的前驱记录确定恢复起点，继续校验保留的完整记录；缺少有效锚点的非对齐截断在删除前拒绝。增加跨段重开及损坏锚点/记录回归。
- 优化数据库写入和维护路径：CLOG 原子发布跳过全零块并合并连续数据块，使用文件身份和时间戳校验缓存；DML、约束校验和 DDL 重写复用已有表结构读取 NULL 位图；在实际持有完整 DDL 回滚快照时消除重写行的重复整库保存点；绑定 UPDATE/DELETE 的同类型整数等值条件先过滤候选，再执行原类型表达式。协调后台刷盘、自动 VACUUM、CHECKPOINT 与 DROP DATABASE，重叠的同一引擎 CHECKPOINT 复用实际结果，保留活动用户事务拒绝及持久化错误传播。增加[性能测试报告](docs/performance-report-2026-10-30.md)与可复现基准脚本。
- 优化查询热路径：目录文件未变化时跳过全目录序列化，扫描复用已有表结构读取 NULL 位图；同类型 SMALLINT/INTEGER/BIGINT 单列主键参数等值查询可使用物理索引，旧快照、自身写入及重叠事务继续使用堆扫描并提前过滤整数键；整数等值 JOIN 在构造组合行上下文前跳过不匹配键，保留原表达式复核、读取顺序及事务可见性检查。
- 增加全仓库代码审查、可复现 SQL/协议问题及逐项修复方案；文档索引区分当前结论与历史证据，修正文档中的旧测试数量、复制示例和失效链接。审查记录本身不代表产品问题已修复。
- 已合入引号类型名称归一化修复，以及递归 SQL 执行期间 Session 指针的异常安全恢复；具体范围和验证见对应 issue 文档。
- 修复 NUMERIC 声明精度/scale 在存储和赋值中丢失的问题，覆盖标量及数组元素舍入、溢出检查、ALTER 重写和协议类型修饰符；支持负 scale 与大于 precision 的 scale。裸 NUMERIC 与旧无修饰符声明仍保持原语义。
- 带 NUMERIC 修饰符的声明也使用 schema 格式 `0x4442000C`；仅支持 INTERVAL 修饰符的早期 C 格式读取器不能读取这类 NUMERIC 声明。请保留升级前备份，不要直接降级打开已写入新声明的数据目录。
- 修复 INTERVAL 列字段范围和小数秒精度在 INSERT、UPDATE、数组元素及 ALTER TYPE 中丢失的问题，并为实际表列返回对应的协议类型修饰符。
- 带 INTERVAL 修饰符的表声明使用 schema 格式 `0x4442000C` 保存精度；未带修饰符的旧声明仍可读取且保持原格式。写入新格式的声明不能由旧版程序读取，请保留升级前备份，不要直接降级打开这类数据目录。
- PostgreSQL 18 兼容审计仍在逐项执行；完成状态只以 `docs/postgresql-18-gap-audit.md` 与 `docs/gap-progress.json` 为准。
- GitHub Actions workflow 按维护要求保持 `.disabled`；`scripts/build_tests.sh` 是本地完整验证入口。

## [0.2.0] - 2026-08-22

并发硬化与写放大根除（"concurrency hardening"）。
Tag 内容：156 个 `*_test.cpp` 测试源 + 7 个 Python E2E；发布时记录崩溃恢复矩阵 12/12、ASAN/TSAN 核心集 CLEAN。

### 并发正确性
- TypeRegistry 单例 bootstrap 竞态：`static bool` 旗标不受 Meyers 单例保护，两线程并发 bootstrap 重写注册表——TSAN 1288 条报告的单一根因，`call_once` 修复后 0 报告
- stats-map 擦除竞态：closeDatabaseCaches 无锁擦 deadTupleCounts_/modifyCounts_ 与连接线程读写竞争，补对应 mutex
- 事务级堆页 flush：commit 只刷本事务写过的页（仍在事务页锁下，无竞态无死锁）；曾试 bgwriter 逐页加锁（锁序死锁,array_test 复现）已否决
- LockManager 锁序反转定性为设计权衡（调用协议文档化）

### 性能
- TCP_NODELAY：服务器 socket Nagle+delayed-ACK 造成每语句 41ms 固定地板（含空查询），修复后 SELECT 1 0ms / INSERT 19ms
- WAL group commit：fsync 队列与插入锁解耦，N 个串行 fsync 合一
- 索引 WAL 图像按 checkpoint 纪元去重：每 commit 整索引文件×2 写 WAL（实测 40 字节 INSERT 写 120KB WAL）→ 同纪元只写一次；WAL 量降两个数量级（70MB/30s → 120KB/60s）
- auto-vacuum 移出语句路径：阈值(50 dead tuples)触发的同步全表 vacuum 曾把全部并发语句串到 30s+ 饿死；改为后台循环执行
- commit 去掉全池堆页 flush（jbd2 world-stop 主放大器，207MB/90s）

### 运维定性
- 残余 ~30s 周期停顿取证结案：worker 单次 fsync 在 ext4 jbd2 等待 25s,由宿主机邻居进程周期性大写入（iostat 225 w/s burst）拖慢 jbd2 提交所致——部署要求独占 WAL 卷,非引擎缺陷
- kill-storm 验证方法论：必须带 commit 确认屏障（无屏障会把 kill-before-commit 误判为丢行）

## [0.1.0] - 2026-08-21

首个公开发布版本（v0.1.0 "first public cut"）。
Tag 内容：156 个 `*_test.cpp` 测试源 + 7 个 Python E2E；发布时记录全绿。

### 核心引擎
- 堆表存储：8 KiB PostgreSQL 风格页布局（页头 + line pointer + tuple 从页尾生长），Fletcher-16 页校验，Free Space Map
- BufferPool：Clock-Sweep 驱逐、pin/usage 分离、并发加载单加载者规则、孤儿帧语义；堆池 256 帧/索引池 128 帧可配
- WAL：段式 WAL、CRC 校验、记录链验证、增量尾部状态、崩溃恢复页像重放（redo/undo）、XID 事务提交日志
- MVCC：快照隔离、事务级读写集合、页级 SIREAD、单列 B+Tree 逻辑索引谓词（SERIALIZABLE 部分实现，见已知限制）
- TOAST：线外大字段存储，zlib 压缩，chunk header 保留压缩标记与原始长度
- 事务：BEGIN/COMMIT/ROLLBACK、SAVEPOINT、语句级隐式事务、DDL 快照回滚、两阶段边界
- 锁：行级/表级/意图锁、死锁检测、锁超时、简化 gap 锁

### SQL 兼容面
- DDL：CREATE/DROP/ALTER TABLE（含分区、继承、表空间）、VIEW、SEQUENCE、DOMAIN、TYPE、FUNCTION（PL/pgSQL）、TRIGGER、INDEX（B+Tree/Hash/GIN/BRIN/SP-GiST/Bloom）、POLICY（RLS）、PUBLICATION、REPLICATION SLOT、STATISTICS、CAST、CONVERSION、AGGREGATE、EXTENSION 兼容对象
- DML：INSERT（VALUES/SELECT/DEFAULT/ON CONFLICT）、UPDATE（FROM/RETURNING）、DELETE（USING/RETURNING）、MERGE 窄版、RETURNING（受限表达式）
- 查询：SELECT 全语法（WHERE/GROUP BY/HAVING/ORDER BY/LIMIT）、JOIN（INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI）、子查询（IN/EXISTS/ANY/ALL/标量）、窗口函数（排名/偏移/聚合 + ROWS/RANGE/GROUPS frame）、UNION/INTERSECT/EXCEPT、ROLLUP/CUBE/GROUPING SETS、CTE、LATERAL
- 执行器：Volcano 风格结构化算子（SeqScan/IndexScan/BitmapHeapScan/HashJoin/MergeJoin/NestLoop/GroupAggregate/Window/SetOp/SemiJoin 等），复杂路径回退 legacy
- PL/pgSQL 最小运行时：变量、控制流、EXCEPTION 之外的游标
- 协议：PostgreSQL wire protocol v3（SCRAM-SHA-256 认证、Parse/Bind/Execute、扩展查询）

### 近期完成的特性（2026-08 批次）
- P1-7 PL/pgSQL 运行时（最小实现）
- P1-8 并行 Vacuum（leader/workers 分索引回收）
- P1-9 自定义代价函数（CostModel 钩子 + GUC 同步）
- P2-2 Bloom 索引访问方法
- P2-4 PgBouncer 式连接池（session/transaction/statement 三模式 + 3 GUC + SHOW POOLS）
- P2-5 逻辑解码（pgoutput 帧式 + test_decoding 文本双输出插件、PublicationCatalog、按槽变更流、CREATE/DROP PUBLICATION|REPLICATION SLOT + 4 个 SHOW 面）
- P2-8 TDE 透明数据加密（SHA-256-CTR + EtM 页级加密、边车信封 `<file>.tde`、keyring 0600、WAL 崩溃恢复兼容）

### 发布工程（本批次新增）
- 版本号单一事实源：CMakeLists project version ↔ src/common/version.h ↔ CHANGELOG.md（package.sh 打包前校验一致）
- `dbms_main --version/-V`
- CI：scripts/ci.sh（全量构建 + 回归 + E2E）+ GitHub Actions workflow
- Sanitizer 例程：scripts/sanitizer.sh（ASAN/TSAN 编译跑核心并发/存储测试）
- 崩溃恢复矩阵：tests/crash_matrix_test.sh（kill -9 × {WAL, TDE, 连接池} × commit 前/后）
- Soak 负载：scripts/soak.sh（可配置并发/时长的多客户端压测）
- 打包：scripts/package.sh（源码 tarball，含目录约定）

### 已知限制（v0.1 明示）
1. **DML 双执行入口**（P1-0）：复杂 INSERT SELECT、部分/索引推断 conflict target、引用子查询或其他关系的 ON CONFLICT DO UPDATE、复杂/子查询/窗口 RETURNING、外连接/复杂 UPDATE FROM/DELETE USING、多 WHEN MERGE、视图写入仍回退 legacy 字符串路径；错误码与行为一致性受限
2. **SERIALIZABLE 非 PostgreSQL 完整 SSI**：复合/表达式/部分索引及其他访问方法的精确 phantom 推理不完整；空结果关系级兜底牺牲部分串行化并发度
3. **TDE 覆盖范围**：未经 BufferPool 页路径的索引文件（.idx/.hidx/.fti 等）未加密；无密钥轮换、无 per-database 密钥隔离
4. **逻辑解码**：无 CREATE SUBSCRIPTION 拉取端；变更不经 WAL 重放（写路径直采），崩溃恢复后逻辑槽不 replay
5. **代价模型**（P1-12）：直方图/MCV 已采集但 planner 消费不全，范围选择率硬编码 0.3，EXPLAIN 行数估计有误导性
6. **并行执行**：仅并行表扫描（page range 分片 Gather）；并行 join/aggregate、GatherMerge 未实现
7. **prepared statements**（P1-10）：SQL 级语法为自有 `PREPARE name FROM 'sql'` + `?` + `USING`，非 PG 的 `(types) AS` + `$n`；无类型推断与计划复用
8. **统计收集器**：无独立后台采样线程；planner 深度反馈缺失

## [0.1.0 之前]
开发期 874+ commits 不在此列，演进细节见 docs/production-status.md 与 docs/all-gaps-todo.md。

[Unreleased]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/releases/tag/v0.2.0
[0.1.0]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/releases/tag/v0.1.0
