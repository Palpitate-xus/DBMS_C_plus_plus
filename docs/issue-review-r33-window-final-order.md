# R33：legacy 窗口结果保留最终 SQL ORDER BY

R11 新控制发现：加入 nth_value 后进入 legacy 窗口执行路径，窗口输入按
partition/窗口 ORDER 排列，却直接把这个顺序作为最终结果。合法的最终
`ORDER BY id` 被忽略，精确 SUM/AVG 的值虽然正确，返回行序却错误。

## 修复方法

最终 ORDER BY 与窗口输入排序分开绑定。输出别名/位置引用消费已有真实
投影值和声明类型，不重新执行窗口函数；源列引用消费原源的类型/排序规则。
一般标量键使用一个保留的 prepared query，在实际源 descriptor 上绑定，
每行缓存一次键值后再排序。重复排序表达式复用同一键槽，不在比较器内调用
表达式，也不合并 SELECT 内独立的执行站点。常量规划在开源/副作用之前进行。

NULL 使用独立 bitmap；声明 NUMERIC/BIGINT 使用现有精确比较器，文本消费
实际 collation。排序后的行索引同时用于 DISTINCT、分页、显示和 wire 结果。
元数据 descriptor 的纯构建不依赖是否发布协议元数据，发布仍遵循原入口范围。

保留原排序语法后再降低别名：裸整数是输出位置，其他裸常量按 PostgreSQL
返回 42601；位置越界/非正返回 42P10。显式 cast 和一元加号是普通表达式。
引号标识符由 AST 识别，不能因名称含 `+/-/()/` 而误判为表达式，也不能把
`"NULL"` 或 `"1"` 猜成常量。带 COLLATE 的名称仍只绑定输入范围。

LIMIT 0 不执行排序键副作用；后面的 `1/0` 常量错误在前面的 nextval 执行
之前返回 22012。该修复不关闭 frame 方向 R04、NTILE 参数 R06、偏移 R07，
也不宣称所有窗口/复合排序表达式已完整兼容 PostgreSQL。

## 验证与保留的失败

证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`。原版本为 `491dae49`，
冻结二进制 `root-before-r33.frozen` 的 SHA-256 为
`45e886bead0e6258ed217174bf276fbaea3522aacce8444346a172ea0d9175f1`。
最终驱动保留原 R11 SQL、八行极值/NULL 精确值断言，再扩充至 44 wire 控制；
另含一个隔离数据目录的正常 CLI 控制，不计入 PostgreSQL wire 控制数量。

控制覆盖最终物理列/投影/序号/表达式、多键顺序、ASC/DESC、NULLS、精确值、
分页、C collation、零需求、nextval 调用次数和错误先于副作用、非法常量、
带空格/运算符/括号/点号的引号别名，以及名为 NULL 的实际输入列。

| 检查 | 实际结果 |
| --- | --- |
| 最终44控制，冻结原版本 | 39失败，exit 1；`r33-final44-original-protocol.log` |
| 最终44控制，真实 PostgreSQL 18.6 / 180006 | 全通过，exit 0；`r33-final44-postgres-reference.log` |
| 原版本独立 CLI | 行序错误，exit 1；`r33-original-cli.log` |
| 第一候选独立 CLI | exit 0；`r33-first-cli.log`，不能写成候选仍丢 CLI descriptor |
| 最终44控制与独立 CLI，Root候选 | 全通过，exit 0；`r33-accepted-window_legacy_result_order_protocol_e2e_test.py.log` |
| 八完整原生窗口驱动 | exit 0；`r33-accepted-native.log`，包含168声明比较、12分区、144 NULL排序、72GROUPS控制 |
| 十七完整协议/CLI驱动 | 全部exit 0；`r33-accepted-*_test.py.log`，含完整postgres_protocol及EXECUTE、引号别名、普通/存储函数排序 |

完整最终门禁失败仍保留。第一24控制候选有2失败（一般标量键 SQL / 大常量
错误码）；第二34控制有7失败（裸常量角色）。后续37控制有2个引号别名失败，
40控制有1个 `"sum+result"` 失败。最后一次40控制完整批次也因该别名失败
而提前终止，不能记为十七完整协议通过。最终修正剩余字节分类器并新增四个
引号别名控制，必须重新执行整个门禁。

第一参考24控制曾有1个夹具错误：极大裸整数应为42601，不是42P10；保留
`r33-expanded-postgres-reference.log` 后按实际 PG18.6 语法阶段修正，未放宽
任何产品行序/值/NULL/副作用断言。原初始/扩展24/34/40红日志均保留。

生产对象不借用私树 ABI；每次源变化真实重编 Main 并标准链接。一次旧编译
在源尚未定稿时由调用者停止（`root-r33-final-build.log`），不是产品编译失败。
最终 `root-r33-quoted-token-build.log` 实际重编 Main 并标准链接exit 0；门禁
逐对象检查全部58份当前生产receipts及最终源签名不变。R33驱动注册后、下一项
R06测试开发前，文档状态和注册盘点均exit 0（473注册/456 standalone e2e）。
这些专项不代表全注册回归或全项目 sanitizer 已通过。
