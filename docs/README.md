# 文档索引

> 历史基准原始数据不随 Git 仓库提供。本文中的 `benchmarks/` 路径仅指可能存在的本地可选证据；新克隆仓库没有这些文件，原始结果只能在另行取得数据后复核。文中的测试与性能摘要仍保留。

## 当前 P42：无写入事务提交和621次正常成本

最新状态以[本批性能／正确性报告](performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)为准：完整压缩证据（可选本地文件 `benchmarks/no-effect-transaction-publication-20261107.json.gz`）含所有原始结果、失败和提取方式。新增一个正式回归，保留95条完整SQL旧新字节比较；33个选定原生入口有31个0、2项未通过，全套未跑。源码已更新，默认程序未替换。八份既有文档插入更新，没有新增Markdown；原754路径和725保护记录保持。Goal active；下方为历史索引。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## 当前批次：长VARCHAR与完整性能证据

本批功能有进展，长值写入部分提速；全面提速尚未成立。新增1MiB混合插入墙钟中位1801.508072ms，相对旧bf9首测／同旧回测-10.720%／-10.799%；更新RETURNING 411.759537ms，-13.372%／-12.280%；重新插入1431.222130ms，-9.197%／-10.414%。对应服务器CPU分别-12.689%／-12.822%、-20.934%／-21.267%、-12.666%／-12.762%。

负向保持OPEN：新长值DELETE RETURNING墙钟43.553088ms，+9.954%／+23.830%，服务器CPU+67.962%／+39.067%；首次VACUUM墙钟25.753918ms，+22.574%／+1.265%；FULL墙钟218.226818ms，+32.204%／+21.215%，服务器CPU+7.756%／+1.774%。新增完整值读取墙钟+2.080%／-2.014%，结果混合。通用270项矩阵的30个区域，墙钟6低于两旧／18高于两旧／6居间；服务器CPU10／8／12，客户端CPU6／14／10。没有排除、重采或把漂移归因为已证实的产品原因；全部负向与原因继续开放。

[全部225行性能、原始证据与低内存提取](performance-report-2026-10-30.md#large-varchar-toast-search-20261107)、[测试](test-report.md#large-varchar-toast-search-current)。

正式库存2404个文件（1137顶层／1142总C++），2401个旧测试逐字节不变、一个类型注册测试按上述范围扩充、另加两个新测试；注册E2E684／独立E2E664不变。生产251个文件，改6个cpp并加CharacterColumn.h，正常CMake C++17/O2程序C6为587f898d，旧bf9ad7ae、更早c8ca0215，61核心对象冻结；没有靠升级C++23宣称提速。默认425165af不变。754个Markdown路径／725份保护记录保留，仅已有8文档插入说明；原273项及22 complete／166 partial／70 unverified／15 deferred_by_user不变，290条旧历史原样，新增partial第291条，IDX-07仍unverified/evidence[]。全部用户暂缓范围不变。Goal active，直接master提交，未推送或替换默认程序。下方各批均为各自历史版本。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 当前批次：FULL有限开销优化

本批只证明固定字段FULL墙钟低于两侧旧版：-11.57%／-1.51%；单长／多长FULL服务器CPU均较低，墙钟各为-3.35%／+1.84%、-1.35%／+3.87%，结果混合。没有证明数据库全面提速，也没有追回P39相对更早84bb的全部恢复成本。

明显负向保持OPEN：首次普通VACUUM多长字段墙钟C1中位67.010218ms，旧c8ca首测30.951480ms／回测25.722458ms，即+116.501%／+160.512%。原三次样本分别为C1 [71.510152,26.679347,67.010218]ms、旧首测[35.992784,30.951480,25.821788]ms、旧回测[25.722458,23.927581,29.365169]ms，全部保留。单长字段大批重新插入的墙钟也+51.70%／+58.00%。未因负向重跑、排除样本或更改预期。

[测试](test-report.md#vacuum-full-relation-snapshot-current)、[全部198项比较与压缩证据](performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。

正式测试库存2402个文件（1135顶层／1140总C++），2401个旧测试逐字节不变；注册E2E684／独立E2E664不变。250个生产文件只改TableManage.cpp，正常CMake C++17/O2 C1为bf9ad7ae，旧c8ca0215、更早84bbadb4；61核心对象冻结。默认425165af不变。754个Markdown路径、725份保护记录保持，只在已有8文档插入说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变；289条历史保持，新增partial第290条；IDX-07仍unverified/evidence[]。Goal active，直接master提交，未推送或替换默认程序。下方各批是各自历史程序的记录。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 当前批次：FULL恢复与完整成本

本批没有证明全面提速。270项原样读写矩阵保留27个全新数据库、54个正常Main及1098条完整回复。30个区域的墙钟：7低于两旧／13高于两旧／10居间；服务器CPU：5低于两旧／14高于两旧／11居间；客户端CPU：7低于两旧／12高于两旧／11居间。所有90项比较均列出，性能原因OPEN。 vacuum-full-fixed墙钟 +64.55%／+78.39%；vacuum-full-fixed服务器CPU +10.12%／+10.45%；vacuum-full-single墙钟 +78.30%／+112.77%；vacuum-full-single服务器CPU +17.97%／+38.94%；vacuum-full-overflow墙钟 +131.53%／+114.90%；vacuum-full-overflow服务器CPU +18.05%／+13.79%（相对旧84首测／同旧回测，正数为更慢）。

原22条旧库恢复及六种专属故障控制通过，既有toast断言、PG差异和跨FULL的PITR仍开放。[测试](test-report.md#vacuum-full-wal-generation-current)、[完整报告与压缩证据](performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。

正式库存2401个测试文件（1134顶层／1139总C++），2399个旧测试逐字节不变，注册E2E684／独立E2E664不变。源码250个文件，正常CMake C++17/O2候选C1为c8ca0215，旧84bbadb4、更早56aad0c0；默认425165af不变。754个Markdown路径和725份保护记录保持，只在8份已有文档插入本批说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有288条历史保持、新增partial第289条；IDX-07仍unverified/evidence[]。Goal active，直接master分批提交，未推送或替换默认程序。下方各批均为对应历史程序的记录。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 当前批次：TEXT长值与完整性能报告

本批完整1500次正常计时没有证明全面提速。仅ID查询服务器CPU比旧版首测／回测高1.78%／1.86%；单长字段首次VACUUM CPU高10.98%／13.76%，墙钟高3.66%／9.08%。多长字段首次VACUUM墙钟低18.80%／高3.52%，CPU低0.33%／高3.27%，属于混合结果。全部负向、旧版漂移、首轮与长尾保留，原因OPEN；本批交付为TEXT容量修复，没有用局部结果宣称整体性能改善。

原28条SQL中3个确切TEXT缺口已修复；1MiB提交后强杀读回通过。普通FULL恢复缺陷、既有TOAST断言和SQL子分区仍OPEN。[测试报告](test-report.md#scalar-text-logical-limit-current)、[完整成本和归档](performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。

正式库存2399个测试文件（1132顶层／1137总C++），注册E2E684／独立E2E664不变。754个Markdown路径、725份保护记录保持，8份既有文档只插入本批说明。原273项、22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有287条历史保持并新增partial第288条，IDX-07仍unverified/evidence[]。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 当前批次：VACUUM引用安全与完整成本

本批1500项完整正常计时没有证明全面提速。在共同的真实after-DELETE种子上，单长字段首次VACUUM服务器CPU比旧版及旧版回测低8.36%／7.73%；多长字段首次VACUUM墙钟高9.83%／27.97%，CPU分别低1.92%／高35.29%。在共同after-UPDATE种子上，仅选ID的CPU高8.43%／8.39%，墙钟高9.77%／14.77%；三个全列查询墙钟也高于两次旧版，完整正负结果保留，原因OPEN。

新测试160／0通过，旧版160／48复现；8项相关回归旧／新均actual0，既有TOAST断言仍actual -6。[测试报告](test-report.md#vacuum-physical-references-current)、[1500操作与完整压缩证据](performance-report-2026-10-30.md#vacuum-physical-references-20261107)。

正式库存2398个测试文件（1131顶层／1136总C++），注册E2E684／独立E2E664不变。754个Markdown路径和725份保护记录保持，8份既有文档仅插入本批说明。原273项及22 complete／166 partial／70 unverified／15 deferred_by_user不变，历史增至287，IDX-07仍unverified/evidence[]。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 当前批次：扫描与排序投影执行优化

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

原样 270 操作矩阵的 30 个区域，墙钟 19 低于两旧／2 高于两旧／9 居间，服务器 CPU 为 12／9／9，客户端 CPU 为 9／16／5。单长字段 VACUUM 墙钟高约 22.90%／28.87%，多长字段 VACUUM 高约 19.10%／4.85%；另有 9 个区域服务器 CPU 高于两旧，全部保留，原因仍 OPEN。

新增原生1156检查通过，既有TOAST断言在旧／新两版均失败，未称全套通过。[测试报告](test-report.md#physical-row-null-reuse-current)、[性能与压缩证据](performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。正式库存 2397 个测试文件（1130 顶层／1135 总 C++），注册 E2E 684／独立 E2E 664 不变；754 个 Markdown 路径与 725 份保护记录保持。原 273 项及 22 complete／166 partial／70 unverified／15 deferred_by_user 不变，历史增至 286，IDX-07 仍 unverified/evidence[]。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
## 最新验证：视图列依赖与 DDL 开销（2026-11-06）

[性能报告](performance-report-2026-10-30.md#view-column-dependencies-20261106)提供 270 个普通操作和 27 个视图 DDL 操作的全部样本、负向及压缩恢复方法；[测试报告](test-report.md#view-column-dependencies-current)记录 242 新检查、7 既有专项及原始 PG 差异；[契约](compatibility-contract.md#view-column-dependencies-current)列出删除保护、旧库和边界。正式库存 2396，Markdown 路径仍 754，历史 285，Goal active。下方为各自版本的历史结果。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
## 最新验证：部分列 TOAST 需求读取（2026-11-06）

[性能报告](performance-report-2026-10-30.md#projected-toast-demand-20261106)记录完整 C1/C2 两轮矩阵、收益与全部回退；[测试报告](test-report.md#projected-toast-demand-current)列出 83 项新检查、5 项既有专项和完整协议复核；[契约](compatibility-contract.md#projected-toast-demand-current)说明物理部分列适用范围。正式测试库存 2395，Markdown 路径仍 754，Goal active。后续各批条目保留各自版本的历史结果。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
## Latest: 2026-11-06 pg_class declaration

The [comparison](postgresql-comparison.md#pg-class-declaration-current) and [compatibility contract](compatibility-contract.md#pg-class-declaration-current) distinguish the actual34-column declaration from its seven executable values. The [test report](test-report.md#pg-class-declaration-current) records550 native checks, PG41 strict pass/product17of41, and actual legacy upgrade/cold results. The [performance report](performance-report-2026-10-30.md#pg-class-declaration-20261106) retains all270 timings, negative cases and both old medians. The new compressed evidence is 15.92 MiB; no extra Markdown file was added. The earlier P31 source-only proposal and rejected P33-C1 runs remain historical evidence, superseded only for the accepted declaration scope.
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 最新验证入口（P32-C1）

普通日志改为逐次O_APPEND/writev追加，新增13组原生检查；当前正式测试2393，本批没有全套重跑。30案例服务器CPU为16低/6高/8居间，多LOB负向保留，未宣称一致提速。完整协议仍有原9项普通差异、默认425未换；见[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与可逆原始证据（可选本地文件 `benchmarks/legacy-log-append-20261106.json.gz`）。下方索引和历史保持。

使用项目从 [README](../README.md)、[使用手册](MANUAL.md)和[打包与部署](PACKAGING.md)开始。
接口范围以[兼容性契约](compatibility-contract.md)为准；命令示例不代表所有组合均已实现。

当前实现、已验证范围及剩余瓶颈集中见[状态说明](production-status.md#当前状态与验证范围)。
[与 PostgreSQL 的差距评估](postgresql-comparison.md)汇总功能总账、实际能力边界及 18.6 同机小表性能对照。
文档清单覆盖仓库 Markdown；历史问题和发布快照保留对应批次结论，不作为当前版本的全部能力声明。

## 审查、问题与验证

- [代码审查索引](review-index.md)：当前审查报告、复现、修复方法与验证边界。
- [逐项修复进度](review-fixes-2026-10-13.md)：审查问题的修复、独立提交和验收结果。
- [全部文档检查清单](documentation-inventory.md)：逐文件分类、本地链接和文档与系统一致性核查范围。
- [测试报告](test-report.md)：当前验证入口与按版本保留的结果。
- [数据库性能测试报告](performance-report-2026-10-30.md)：35 类操作及 PG 18.6 对照，含有界排序、按页扫描、普通 LIMIT、外部排序、回归范围与剩余瓶颈。
- [PostgreSQL 18 总清单](postgresql-18-gap-audit.md)与[机器可读状态](gap-progress.json)：唯一的功能完成状态来源。
- [实现蓝图](postgresql-18-implementation-blueprint.md)与[复查计划](review-closeout-plan.md)：工作顺序和验收方法。
- [用户暂缓范围](deferred-scope.md)：明确暂缓的模块及保留的未修复状态。

## 开发参考

- [SQL 命令参考](commandsList.md)
- [解析器](../src/parser/README.md)、[执行器](../src/executor/README.md)、[系统目录](../src/catalog/README.md)、[复制模块](../src/replication/README.md)
- [变更记录](../CHANGELOG.md)与[生产化状态](production-status.md)
- 文档维护检查：在仓库根目录运行 `python3 scripts/check_docs.py`；新增、移动或删除文档后用 `--update-inventory` 更新[清单](documentation-inventory.md)。

## 如何阅读历史文档

`issue-*.md` 保存对应提交、失败和局部验证证据；其中的 PASS 只适用于文档明确标注的输入与范围。
历史计划、旧 TODO、`archive/`、旧版本发布说明和按日期追加的批次记录用于追溯，不是当前版本的能力声明。
后续审查可以推翻旧结论，最新报告会说明依据，原始失败记录继续保留。

完整回归、静态审查、差分测试、sanitizer 和并发测试覆盖不同问题。任何单项通过都不能证明没有 bug。
