# R139：TimeZone重置关键字和默认偏移恢复

后续[R140](issue-review-r140-timezone-set-grammar.md)已按用户“skip it”暂缓；其语法
候选与验证日志仅保留，未接受为修复。本页R139提交中的语法缺口仍然开放，
包括两条非法DEFAULT写法被放行，不能用未提交候选覆盖本页历史输入。

状态：合法重置与offset恢复的有限修复已验证；最终31完整入口、clean5及独立核对均0。
严格语法未完成，另见下方R140；新增关键字对两条非法语法的放行明确保留，不冒全模块完成。
本项不恢复[R137](issue-review-r137-foreign-key-full-value.md)或其它用户暂缓模块。

## 事实、影响与处理方法

| 问题 | 已确认事实与处理方法 |
| --- | --- |
| DEFAULT被当成时区名 | 六种SET/SESSION/TO/等号/TIME ZONE形式应恢复已保存启动值，旧版报22023并保留先前时区 |
| LOCAL语义依赖语法 | 实际PG只有专用SET [SESSION] TIME ZONE LOCAL重置；通用SET TimeZone TO LOCAL仍报22023，不能统一替换 |
| 引号边界 | 单/双引号DEFAULT和LOCAL均是普通值，应报22023且保留状态，不是关键字 |
| RESET名称与偏移不一致 | 实际旧Main处理器的五个观察中UTC为0，其余上海、纽约、+05:30和-03:30仅恢复名称、offset也全部0 |
| 处理方法 | 私有Main共享既有值解析，DEFAULT和专用LOCAL调用默认恢复；RESET TimeZone/TIME ZONE/ALL也通过同一解析器计算offset，不再写死0 |
| 已保存数值时区 | 对已有数值SET产生的固定POSIX显示形式校验两侧相反偏移、恢复真实offset并保留默认名称；不改通用POSIX语法支持范围 |
| 错误原子性 | 无效已保存默认值先失败，尤其RESET ALL不能先改其它字段；不修改或验收认证/角色规则 |
| 未改范围 | 无公共头、布局、heap或参数ABI变更，不实现事务级SET LOCAL、完整GUC事务恢复或所有startup参数 |

## 参考和旧版证据

实际PG18.6/180006的新280控制全过；冻结已接受b6bfacb1的Root程序和相应
干净旧程序各280完整/48失败/实际1；48包含后续状态/值断言，不是48个独立根因。13条DEFAULT与34条LOCAL/RESET诊断也保留，
观察器0只是完成采集，不表示兼容通过。PG启动Etc/UTC与Root启动UTC本身
不是该bug；回归分别与每个连接自己的启动SHOW值和真实TSTZ结果比较。

旧原生探针包含实际Main.cpp，fresh O0测试TU与57个匹配O2生产库链接，
没有stubs，并在全局对象构造前隔离目录。五个默认值的真实RESET处理器均
返回成功，但四个非UTC偏移错误；该进程0是观察完成，不是行为通过或整库sanitizer。

## 回归与验收范围

[timezone_default_protocol_e2e_test.py](../tests/timezone_default_protocol_e2e_test.py)
280控制覆盖Simple/Extended完整消息帧、SQLSTATE、SET/RESET tag、Ready、SHOW元数据、
默认恢复后的实际TSTZ值、全部六种DEFAULT写法、专用LOCAL、RESET三个写法、
24种带引号负例和四种通用LOCAL负例。原[R138](issue-review-r138-session-timezone.md)
224控制与所有断言保持，不借旧通过替代本轮执行。

[真实session前端](../tests/frontend/session_execution_lifetime.cpp)保留原17次控制，
增加11种已保存默认值、11种重置命令、无效默认值原子性；本轮290次实际
dispatcher命令。命名时区含当前纽约DST、固定偏移和数值SET的既有canonical显示；
原生命周期/触发器断言不删除。原生自定义Session默认值不代表已支持非UTC wire
startup TimeZone；该入口目前仍有限，须独立处理。

| 本轮验证 | 实际结果 |
| --- | --- |
| 正常/repeat | 仅Main正常O2重编1个CPP、repeat0，两次0；完整头/compiler/flags仍348f43c0 |
| 真实Main | 三个测试TU全部fresh，explain/session既有O0、window O2；当前57份生产O2库新链接，无stubs并实际隔离执行。session290/window58/explain错误清理均0 |
| 最终Root门禁 | 12native+16protocol+3Main共31个有序、唯一、完整入口全部0；新增280与原224均0 |
| 干净组合 | 已接受b6加自有四路径；1个fresh O2 Main和57个逐一核对头/CPP/原58receipt/预先对象SHA的R136生产donor库，真实server无stubs；五个完整协议入口0，不是normal fresh58构建 |
| 独立核对 | 上述名字/顺序/次数、旧新280、原224、native290、Main源/对象/链接字节、当前58receipt/双cache/57测试库字节、两个候选与donor冻结输入均0 |

## R140：仍须修复的语法边界

实际参考、旧cb786及本轮babb程序各完成22条观察，日志为
`r140-tz-grammar-reference-discovery.log`、
`r140-tz-grammar-oldcb786-discovery.log`和
`r140-tz-grammar-rootbabb-discovery.log`；观察器退出0不是行为通过。

| 输入 | PG18.6 | 旧cb786 / 本轮babb |
| --- | --- | --- |
| TimeZone紧贴等号（含SESSION） | 成功 | 两版均XX000 |
| 通用TimeZone缺分隔符，后接'UTC' | 42601且保持原值 | 两版均成功改值 |
| 专用TIME ZONE后写= 'UTC' | 42601且保持原值 | 两版均成功改值 |
| 专用TIME ZONE后写TO 'UTC' | 42601 | 两版均22023 |
| SET TimeZone DEFAULT | 42601且保持原值 | 旧22023，本轮成功 |
| SET TIME ZONE = DEFAULT | 42601且保持原值 | 旧22023，本轮成功 |

最后两条是新关键字放大旧宽松语法后的错误放行，不能只称历史问题或隐藏在
本轮31全过之后。R140保持普通未修复，下一项应核对参数名token边界、支持合法
无空白等号、通用强制TO/=、专用禁止TO/=，保留引号内值，并在值校验之前报42601；
新增Simple/Extended及错误后状态控制。不借此恢复R137或认证/事务LOCAL专项。

## 证据与版本管理

本机证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：
`r139-reference280.log`、`r139-oldcb786-280.log`、`r139-clean-old280.log`、
两组DEFAULT/LOCAL/RESET诊断、`r139-old-native-reset-discovery.log`；
旧Root字节在`/tmp/dbms-r139-old.8YS8KJfK/dbms_main`，SHA为
`cb786166796fb95e576524aa4ca7c6a1f9598a6f7e6e6593b148302ac02b8cf9`。
Root源/测试/脚本构建封印
`66f05bc245758c2b7d0df0ce10ec8f20072e94179cbe3662dd4631428644d772`，
完整头/compiler/flags仍348f43c0。干净候选及R136 donor开始构建后全tracked保持冻结。

最终Root日志`r139-final-gate.log`、`r139-final-native-results/`、
`r139-final-protocol-results/`及三个frontend日志；另有
`r139-focused-session.log`、`r139-clean-prep.log`、`r139-clean-gate.log`、
`r139-independent-final.log`。Root binary为
`babb333cae22090fdc0b12172062c4b5293546da11cf1794ab159f364b49838f`，
cache为`5c8c60d59afe03deabcaa7d7413b49e37a18a268d2de148f6e0c4ee73fc72dcf`。
干净源码`/tmp/dbms-r139-clean-source.axieWkaR`和所有tracked保持冻结，
程序`/tmp/dbms-r139-clean-binaries.p9XN2Tl1/dbms_main`的SHA为
`6850729b060a1295b0347a9b0e54f3b95d950eb7f64b33de7dfaedbebef085ca`。

接受562MD、902native+538Python+3Main=1443；工作区565MD、
906native+542Python+3Main=1451，另含原六个未接受入口和R137两个候选。
563份Python AST/13份Bash及本地文档目标另核对。没有新1443全量、真实TLS或
整库sanitizer证明；历史ecf1436仍1433实际通过/2失败/1TLSskip、终态1。

只提交本项自有hunk/新协议测试/相关文档，原始未提交候选和R137跳过快照保留。
原273条目/勾选22complete/166partial/70unverified/15deferred不提升，
总清单尚未完成。不push，GitHub Actions保持禁用。
