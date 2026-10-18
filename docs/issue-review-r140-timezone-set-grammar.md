# R140：TimeZone设置语法与Parse/Execute阶段不一致（用户暂缓）

状态：用户于2026-10-18要求“skip it”。候选实现、测试及注册保留在工作区，
未提交、未验收，不计已修复。普通继续不恢复本项；本次仅提交暂缓记录。
跳过前的有限门禁确已完成，但尚未运行独立终态验收，不能因此接受代码或报告总账完成。

## 问题与修改方法

| 问题 | 已确认事实与候选处理方法 |
| --- | --- |
| 合法紧贴等号/引号被拒 | TimeZone='UTC'、SESSION、带引号参数名、TO'UTC'等在旧版XX000/22023，PG成功；候选以同一AST分派而非固定前缀取值 |
| 非法分隔符被接受 | 通用SET缺TO/=、专用TIME ZONE写=被放行；R139新增DEFAULT又放行两条非法形态。应42601并保留原时区 |
| 错误阶段不对 | Extended语法错误应在Parse失败，不得先发ParseComplete/BindComplete/NoData再到执行阶段报错 |
| 原因 | SQLParser::parseSet和Main固定前缀各自解释，规范化SQL还折叠引号外换行，丢失合法字符串续接 |
| 候选方法 | 专用严格AST处理通用TO/=、专用语法、DEFAULT/LOCAL、FROM CURRENT与列表；Main使用同一原始SQL AST |
| 语法与值 | 通用CSV语法合法、执行22023；专用CSV语法42601。引号内DEFAULT/LOCAL仍是数据，FROM CURRENT不等于DEFAULT |
| 保留边界 | E/dollar与换行字符串、关键字类别、带点GUC名；SETTINGS/SETUP不是SET。不得执行函数或子查询来发现语法 |
| 非本项完成声明 | INTERVAL只有语法元数据；完整参数域、事务级SET LOCAL和非UTC startup均未完成 |

## 跳过前证据

规则核对使用本机实际PostgreSQL18.6的gram.y、scan.l和kwlist.h，
包含494项关键字类别，不用项目旧isKeyword近似替代。
实际参考server_version_num为180006；最初6373控制全过记录保留，最终6403也全部通过。

| 输入与完整入口 | 实际结果与限制 |
| --- | --- |
| 冻结已接受f8ee11cb Root程序、对应干净程序 | 各6403完整控制/1316断言失败/实际1；包含后续SHOW断言，不是1316个独立bug |
| 旧原生解析契约 | 4090完整控制/2384失败/实际1；含新统一AST表示契约，不全是SQL行为问题 |
| 候选专项 | 新原生4090、协议6403及真实Main前端311次命令控制，三个完整入口实际0 |
| 候选Root相邻入口 | 17native+23protocol+3真实Main=43，有序唯一完整入口全部实际0，输入fence0 |
| 候选干净组合 | 1native+6protocol=7个完整入口实际0，输入/产物检查0 |
| 独立终态验收 | 检查器已准备，用户跳过前未执行；不补作0或据此接受候选 |
| 静态草稿检查 | 566MD本地目标、564Python AST、13Bash、文档/注册与原273一致性均0；require-complete仍实际1 |

候选文件为`tests/timezone_set_grammar_test.cpp`和
`tests/timezone_set_grammar_protocol_e2e_test.py`，分别4090及6403控制。
它们、生产Parser/Main改动、注册和真实前端290→311扩展均未进入接受版本。
原R138的224与R139的280协议在上述Root43及clean7内实际通过，
不反向重写原R139的31/clean5历史。

## 构建事实与保留记录

正常O2实际编译Main和Parser两个CPP并成功。最初外部wrapper沿用单CPP计数，
实际退出2，未运行repeat；没有停止编译或修改运行脚本。
原driver、日志和退出记录保留；另一个补完脚本核对原正常两CPP成功后，
实际repeat0并检查当前58receipt及冻结输入，终态0。这是脚本计数错误，不是产品编译失败。

三个真实Main测试TU本轮fresh（explain/session既有O0、window O2），新链接
当前57个生产O2库，无stubs并隔离执行。干净候选以f8加六路径候选构成，
生产server为2个fresh O2 Main/Parser加56个逐一核对头/CPP/receipt/对象SHA的R136 donor；
不是normal fresh58。原生测试另外使用fresh测试TU/stubs及57匹配库。
干净候选、旧二进制与donor保持冻结，普通后续工作不能改写这些历史输入。

本机证据目录`/tmp/dbms-review-r46.ifu6SWLp/`保留：
`r140-reference6403.log`、`r140-old-babb6403.log`、
`r140-clean-old6403.log`、`r140-old-native4090.log`、正常/finish/repeat、
`r140-focused-gate.log`、`r140-final-gate.log`、`r140-clean-gate.log`及静态日志。
跳过时另存`r140-user-skip-candidate.tar.gz`及`r140-user-skip-working.patch`；
压缩包SHA256为`dce699a8b02319b1c7208d5341414b6959ae0276dde32fa34d4b9b23608b809c`。
包内报告为跳过前草稿，当前页为本次暂缓记录。源/测试字节保持不变。

候选Root binary为`fa7af302acd72fd9df09dfa3860a304384a6b57b0e790d88dc8a609a8786feb4`，
cache为`a3505df1678762b45406540abbc348afc7804f4985e14548a0ec8cbbf2d2cf14`，
构建源/测试/脚本封印为`2fadfff5a6393f534c92faefae61a73381387da9f10c25f2df6946f8616085b0`。
这些是未接受候选的历史证据，不是发行验收或当前HEAD的构建证明。

## 版本与范围

本次只提交文档，不提交候选实现/两测试/注册。已接受测试仍为
902native+538Python+3Main=1443，Markdown含本记录为563；
工作区566MD及907native+543Python+3Main=1453另含原六个、R137两个和本项两个候选。
[R137](issue-review-r137-foreign-key-full-value.md)及其他用户暂缓项不恢复。
原273及22complete/166partial/70unverified/15deferred不变；总清单未完成。
没有新1443全注册、TLS或整库sanitizer证明。不push，GitHub Actions保持禁用。
