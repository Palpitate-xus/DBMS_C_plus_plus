# R142：普通SET的带引号参数名被拒绝

状态：完成有限修复。最终Root/clean544专项、15完整入口、clean6及独立复核均实际0。
SHOW依赖先由[R143](issue-review-r143-show-guc-protocol-results.md)独立提交b3e53b8c；
本项不混入其实现或把原失败覆盖为通过。[R140](issue-review-r140-timezone-set-grammar.md)
与R137及其它用户暂缓项继续保留，不恢复专项实现或测试，原273总账未完成。

## 问题与修改方法

| 问题 | 事实与有限处理方法 |
| --- | --- |
| quoted GUC名称被当原始字符串 | Main用含双引号/大小写的param比较应用名、路径、编码、locale，合法设置报XX000；复用已有decodeQuotedIdentifier和unsigned-char安全的SQLParser::toLower规范完整单token名称 |
| LOCAL误拒绝 | SET LOCAL "search_path"在真实BEGIN内旧版0A000，PG成功；支持判断改为规范名，仍只有原有search_path事务作用域 |
| quoted目标不能变成@变量 | 原始@分支继续使用原语法，规范GUC名在其后才交普通分派；quoted "@name"不得修改变量 |
| 未扩范围 | 不改值解析、未知参数错误映射、其它LOCAL/GLOBAL政策、完整GUC语法或TimeZone暂缓语法 |

不把任意token串去掉首尾引号；只解码完整单个quoted标识符，双写双引号按现有函数
处理。未知/近似名字不能折成已有字段。未知参数仍只验证真实错误、不误改状态，
不把旧XX000映射宣称为PG已经兼容。

## 参考、旧红与依赖

| 输入与全部控制 | 实际结果与限制 |
| --- | --- |
| 实际PG18.6/180006 | 普通参数共同530控制全部0；不拿PG不存在的项目变量接口作同义参考 |
| 原始0c旧Root90b3f70a、干净878094b7 | 各完整544/322断言失败/实际1，含后续SHOW断言，不是322独立根因 |
| 最初参数名候选 | Root14为13通过/1失败、clean5为4通过/1失败，新544有136失败；正确值却缺Extended SHOW列名/OID，发现独立既有SHOW问题 |
| R143独立修复后Meta-only干净153dc495 | 同544完整/216失败/实际1，保留参数名问题的独立旧红，不借Root两候选组合替代干净验证 |
| 当前参数名修正 | Root和独立clean各544全0，15完整入口/clean6/独立v2核对均0 |

所有旧程序、候选、原始失败和观察保留。42条普通参数及5条变量观察的0仅表示
采集完成。最初完整性检查器因正则过度转义而1的日志也保留；更正版本核对
原14/clean5完整性0、真实失败总态1，不伪造首次验收成功。

## 永久回归

[quoted_set_parameter_protocol_e2e_test.py](../tests/quoted_set_parameter_protocol_e2e_test.py)
同一字节SHA256始终为
`9fb717d7f7e84cc57824e909e3dd17af28b6708304441dfd8aafe38a7002d47d`。
530共同控制覆盖bare/quoted小写/大写/混合名，bare/SESSION、TO/等号、
Simple/Extended、四个字段、未知/近似/双写引号名字不误匹配、真实LOCAL搜索路径回滚。
另14项目控制验证真实@变量及quoted @失败后值与模式不变，不删旧断言或放宽期望。

真实session前端保留原290次控制，新增8个实际命令/字段/父会话归属控制至298，
包含@7后quoted @9直接legacy bool错误、仍为7及父会话归属恢复。
其余入口完整执行4native、8protocol及3实际Main，不抽取其中断言；
Meta150、原R141的389、配置、搜索路径事务、会话元数据、事务模式、字面量保真均0。

## 构建与来源

初稿正常1Main O2/repeat0并fresh编译三前端，输入是SHOW依赖修复前，不能直接当最终证据。
R143之后又正常2Main/Network O2/repeat0并fresh全部三Main TU；最终R142工作区CPP
已经包含本项修正且没有再变，实际运行生产repeat0、不重编CPP、核对58当前receipt。
Main源b8c58e1b、Network999239b6及全部头/compiler/flags348f43c0保持不变。

最终前端不是冒称本轮全部重新编译：核对四Main/测试源、头/flags、源封印、生产者
cache及对象SHA后，复用R143最新Main的fresh explain O0/window O2对象，
再fresh编译含本项8控制的session298 O0；新链接当前57匹配生产O2库，无测试stubs，
通过既有隔离机制实际执行。没有复用SHOW修复前Main e390e6a0的旧对象。
测试体来自已接受b3e53b8c加本项native块，仅投影Main包含路径；
工作区319还含用户暂缓R140的21控制，没有拿它替代本项298执行。

独立干净b3e只加本项Main/注册/native块/544夹具四路径。
生产server为fresh O2 Main、核对原CPP/全部头/原编译证据/SHA的R143 Network对象，
及56个逐一核对CPP/头/58原receipt/SHA的R136生产donor，不是normal fresh58。
旧新clean候选及donor保持全部tracked冻结，独立检查逐项核对来源和完整性。

## 证据、版本与边界

最终Root仍为R143以来同一可复核产物
`907709970334ae3284bc6d88a0b771b778facb9ee8af54d75b884f69cf46bb92`，
cache为`6f13509754b596e531084ff40f9dfcdd6a98bb5477945e323b79a279e7c66c3d`，
src/tests/scripts封印为`97bf8523795dd2fdb837bd175f75da032a20106447c2ea732514bdbbdb0ba52d`。
最终clean binary为`a799873c7e3dcef589fb150c436615083ee28198be8d23c9487ecc4aff7655a2`。
本机/tmp/dbms-review-r46.ifu6SWLp/保留reference530、旧544两份322失败、
初稿14/clean5及136失败、更正完整性记录、Meta-only216失败、新15/clean6和独立日志。

本项只接受Main两个私有块、native八控制、一条注册、新544夹具及相关文档。
无公共头/ABI/布局/heap修改；原56933字节补丁和三份草稿原样核对。
R140六路径archive按只撤回本项及已接受R141/R143自身块后逐字节核对，
R137 TM/两测试及R98/R99/R109等候选继续保留，不混入提交。
已接受566MD、902native+541Python+3Main=1446；
工作区569MD、907native+546Python+3Main=1456还含十项未接受入口。
完整GUC语法/值域/其它事务政策、R144应用名规范化、R145路径Sconst列表语义仍普通开放。
无新1446全注册/TLS/整库sanitizer证明；旧1436两失败/1TLSskip不改写。
原273及22complete/166partial/70unverified/15deferred不变。不push，Actions保持禁用。
