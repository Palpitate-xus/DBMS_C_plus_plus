# R07：LAG/LEAD 有符号偏移、typed value/default 和需求

原转换器使用stoull且拒绝0，WindowOp又把偏移强制至少1；legacy把完整参数
`v,0` 当列名。因此0没有返回当前行，负偏移错误或触发unsigned边界溢出。
本轮原协议控制在 `lag(v,-1,99)` 处连接关闭，保留日志，不能记为完整驱动
已经跑完。正常合法SQL可以触发这个退出，不是只改变了错误码。

## 修复方法

WindowFunctionSpec保存有符号SQL INTEGER；计算时扩展到int64，按偏移符号
检查距离与分区边界后才形成目标索引。0读取当前行，负数反转方向，INT_MIN
的取反在int64中计算，不做unsigned加法或不受检查的下标访问。

Main复用R06建立的prepared窗口参数接收器，绑定实际范围与descriptor。
LAG/LEAD的offset在当前行求值；SQL NULL offset直接输出NULL，不执行value
或default。命中目标行时只在目标行求值value，目标的真实NULL不会触发default；
越过分区边界时才在当前行求值default。参数和子查询保留真实执行站点，不能
通过提前求值或SQL文本替换实现。所有签名/unknown转换/常量规划先于开源。

value/default使用真实共同声明并插入类型转换；metadata来自纯绑定结果，
不从第一列名字猜类型。发生真实类型提升时走保留AST的receiver，不能只把
OID改宽却不转换值。有限原生入口只接受真实常量offset/default，字符串转义
及NULL按AST解码，默认值按已绑定声明规范化；函数/列默认值保持按需求值。
缺少value或非法arity也在公共纯binder入口返回42883，不能访问空参数数组。

PG18的一/二参数重载在value仍是unknown时返回42804；三参数共同声明允许
全unknown解析为TEXT。该区别不通过执行NULL/用户函数猜类型。互不兼容的
value/default签名返回42883，错误unknown常量在分析时返回22P02。

## 实际验证

证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`。原版本 `41d45c4a`，冻结程序
`root-before-r07.frozen` 的SHA-256为
`8a6be9e2c6d8abe473bbbd54e6d2a64fb0907477b4b3eb4f5fe0c2e29c6fb27f`。

| 检查 | 实际终态 |
| --- | --- |
| 原native120配置，真实planner | 36失败，exit 1；`r07-original-native.log` |
| 原协议完整驱动的尝试 | 合法负偏移处后端退出，客户端exit 1；`r07-original-protocol.log`，没有完整控制计数 |
| 最终152协议，真实PostgreSQL18.6 / 180006 | 全通过，exit 0；`r07-final152-postgres-reference.log` |
| Root最终152协议 | 全通过，exit 0；`r07-first-candidate-protocol.log` |
| Root最终122native及十四完整原生邻居 | 十五完整驱动exit 0；`r07-candidate-native.log`，核对全部58当前receipts与最终源不变 |
| Root十九完整协议/CLI驱动 | 全部exit0；`r07-final-*_test.py.log`，含R06/R33/声明比较/NULL/frame/EXECUTE及完整postgres_protocol |
| 文档与注册检查 | exit0；464份UTF8/本地文件链接/inventory全覆盖，475唯一注册/458 standalone e2e |

原生覆盖两方向、两函数、BIGINT极值/TEXT（空串/字面量NULL/空格/SQL NULL）、
0/±1/INTEGER边界、默认缺省/NULL/字面量，共120配置；最终另加两项纯binder
缺参保护。协议覆盖两个实际分区、原生/强制legacy、列/表达式偏移、NULL偏移、
准备语句参数、空源、LIMIT0、空串/引号列/转义默认值、共同类型提升和错误
阶段，逐行核对值/NULL/标签/OID。序列控制检查default仅在越界时执行，NULL
目标不触发default、NULL offset不执行value/default，value按实际目标行求值。

初始参考138全通过。扩展148曾有2项夹具错误：假定二参数unknown NULL可
自动变成TEXT，而PG返回42804；保留 `r07-final148-postgres-reference.log`，
改为实际错误预期并补显式TEXT和三参数NULL阳性控制，最终152全部通过。
未放宽任何行值、偏移、NULL或副作用次数断言。

公共结构改变后实际重编58生产单元，保留每单元日志。编译过程中补充纯binder
缺参保护，首次并行批次有57单元通过；binder编译后源签名不符，拒绝发布，
批次exit123（`root-r07-fresh58-build.log`），没有标成全部编译通过。随后
`root-r07-final-standard-build.log` 真实重编最终binder并标准链接exit0；其余
57个对象与当前相同头/flags/source receipts全部匹配。原生门禁再次核对全部
58份生产receipt及最终源不变，不借旧ABI对象。专项门禁
不能替代最后完整注册回归，也不关闭frame R04或逻辑预览R13。
