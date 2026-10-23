# R242：INTERVAL 的 EXTRACT/date_part 字段语义

状态：interval值内核有限修复已验证；完整新夹具仍有签名、语法、规划阶段和显示差异，不能宣称全绿。
对应原TYPE-06/FUNC-01；不关闭整个日期时间函数族或原273项。

## 缺陷与修改方法

| 已复现问题 | 影响 | 修复方法 |
| --- | --- | --- |
| epoch把所有month按30天计算 | 一年错误返回31104000秒，PG应为31557600秒 | 完整年按365.25天，残余month按30天；NUMERIC使用int128精确微秒总和 |
| 负month的quarter丢失符号 | -1 month、-1 year等错误为正quarter | 保留负字段规律；INT32_MIN采用下述明确PG端点兼容，不执行有符号溢出 |
| week被列入不支持字段 | PG支持的day/7提取报0A000 | 带符号整数除法，仅取day字段，不重解释month/time |
| 单位别名与字段命名空间缺失 | years/qtr/ms/us等被错误拒绝 | interval内局部使用单位词表、大小写折叠和前10字节比较；不随意trim字段值，不改其他时间类型规则 |
| infinity一律早退NULL | year/day/hour/epoch等应返回有符号Infinity，未知单位错误也被吞掉 | 区分单调字段Infinity、振荡字段NULL、22023/0A000；识别三字段极值保留码 |
| DATE_PART interval仍用NUMERIC内核/返回域 | 返回类型、精度与float8顺序求和不同 | interval分支使用真实float8，有序加time/year/month/day；EXTRACT仍为精确NUMERIC |

PG18.6实际在INT32_MIN month端点提取quarter返回1。不可变参考已记录该例，修复显式
安全模拟此结果；不靠有符号溢出，也不宣称该端点符合一般负quarter对称性。
没有为了取得通过而修改gold。此兼容选择绑定本轮实际PG180006证据。

表行DATE_PART与EXTRACT共用带真实列类型/heap NULL的表达式路径，移出旧strict预取
列表，保持DATE_PART首参数是实际值而非EXTRACT语法标签。独立13项SQL门覆盖字段列、
NULL、不支持单位、元数据零执行和序列实际恰好一次执行。旧13门只失败两项数值，
旧版副作用计数已通过：不把旧getVal/NULL重复求值风险冒称已复现SQL缺陷。
DATE_PART其他时间类型原有分支、通用格式化器、AST/公开头/存储/wire布局均未更改。

## 计划与完整实际结果

| 步骤/完整检查 | 修复前Root / 纯提交投影 | 当前Root / 纯提交投影 |
| --- | --- | --- |
| 实际PG18.6不可变996结果 | 捕获text/binary/Describe/错误阶段与类型 | gold/native内容保持不变 |
| 完整原生996 | 各698失败，退出1 | 各38失败，退出1；未报全绿 |
| 完整协议4474 | 各1338失败，退出1 | 各386失败，退出1；未报全绿 |
| 实际字段列/NULL/副作用13 | 各2失败，退出1 | 两侧13全过，退出0 |
| 同一完整协议对真实PG18.6 | 修正夹具后4474/0；另13/0 | 私有实例正常停止，默认fsync未改 |
| 每侧14原生/23协议相关回归 | 本轮不虚报旧版全回归 | 仅原R238完整2863各114暂缓差异；其余72次均通过 |

共80次完整程序运行74退出0、六次退出1：两个新996、两个新4474、两个原R2382863。
所有新失败原生标签均在旧基线中，全部4474逐控制位置及期望与旧版核对，没有新增失败
位置，不用重复参数标签集合掩盖同SQL不同输入的差异。进一步按Decimal数值与float8
原始位核对剩余实际返回值，NULL和错误状态保持原断言；分类不改变测试的失败结果。

首轮PG4474有12条夹具错误：fresh unnamed参数的custom plan在Bind阶段常量折叠
非法单位，尚无BindComplete/portal RowDescription，而statement Describe已有纯类型。
只修正精确阶段期望并加强BindComplete计数，全部SQL/4474控制/SQLSTATE/gold/native
未变；V1源码、失败日志与收据保留。修正后整个PG4474取得0，才运行旧产品。

996覆盖两种函数、14个规范单位、20组值、单位别名/大小写/空白/长前缀/不支持单位、
NULL字段、整数/布尔/interval[]错误签名；含正负、微秒、int32/int64端点与infinity。
4474保留每条Simple、Extended text/binary、Describe；另有真实OID1186 text及16字节
binary输入、动态字段参数、实际表行/NULL/WHERE false/LIMIT0、元数据与单次求值。

## 未修复问题及修改方法

以下失败仍在完整验收中，不是将它们排除出完成门或自动新增用户暂缓。

| 后续问题 | 现象与修改方向 |
| --- | --- |
| R243 输入签名与语法角色 | integer/bool/interval[]即使NULL或空数组也应在绑定期拒绝；EXTRACT(NULL FROM...)应42601，而date_part(NULL,interval)是合法严格NULL。应给实际内置callback加有限签名、保留native替换所有权，并在真正语法角色拒绝NULL token，不能让运行时NULL掩盖静态资格。 |
| R244 规划/错误阶段 | 非法单位错误在产品Execute而非PG的Bind custom-plan折叠时出现，产生多余BindComplete/RowDescription。须审核分析/规划结果保留与纯元数据边界；不能用SQL字符串特判替代真实规划，也不能借此恢复R156完整载体。 |
| 原R156显示边界 | EXTRACT整数SECOND的0与0.000000、两条大DATE_PART epoch固定/科学记数法；Decimal或float8位相同，严格输出仍失败。原用户暂缓继续保留，不改通用格式化器或移除断言。 |

新原生38=21签名+10NULL语法+7显示差异。按真实SQLSTATE分类，而非只按样本组名称：
其中7条带不合格输入的EXTRACT(NULL FROM...)也先报42601，不是42883。
完整4474还有同值但不同字节的NUMERIC表示差异；其精确分类见下方收据与附表。
这些是重复控制失败数，不是独立bug数量。新R243/R244仍开放，不与已有用户暂缓混淆。

R238剩余R239/R240/R241及其他用户暂缓不恢复。原完整2863两侧114失败标签仍与父版
一致；这些先前暂缓与本项新开放签名/语法问题分开记录。总273/冻结1492历史未重写。

## 构建与版本边界

两侧各fresh ExprEvaluator/TableManage、新native996与14个回归native，新链接服务器；
其他各自生产对象、编译器、公开头和stub核对后复用。Root62生产/134头，纯提交60/131，
Root Main仍明确使用pre-R229匹配donor，不运行当前跳过的Main草稿或默认旧dbms_main。
不是normal/repeat、所有CPP fresh、全工作区源码、全套、TLS或sanitizer验收。

2268纯源码输入与实际测试投影逐blob一致；947 native+596 Python+3 Main=1546只是清单，
本轮没有全跑1546。独立index只提交本项八路径，原暂存18和其他工作区改动原样保留。
总清单仍未完成，require-complete仍应退出1。未push，Actions保持禁用。

本地证据：`/tmp/dbms-r242-interval-extract.pfBNZTMw`。

## 精确剩余分类

- root：{"signature": 84, "extractNullGrammar": 40, "plannedErrorPhase": 172, "presentation": 90}；新增失败位置0。
- accepted：{"signature": 84, "extractNullGrammar": 40, "plannedErrorPhase": 172, "presentation": 90}；新增失败位置0。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 实际PG不可变996 | `f9def20eda2817ad03b528538de360271fe5677e7355e89f05940911f31d53d0` |
| 首轮4474含12夹具错误 | `6291117f4929b8e194e66c0697439b42fa122d6eab967c6d9f2ec488ef435b37` |
| 精确阶段修正 | `38a7cf28d4a7f7a49df9f2079894a3b67700244c10a556de622b207b5bb84fed` |
| 完整PG4474 | `dd63c4451885bcf856ed89a9702a4e1fff9a95e07d5abe6d8647aba4ec10acaf` |
| 独立PG13 | `bfac306fa9325337423d2b43d8d6f1351553ec5156bdd9bc8ea71efb5df2bbeb` |
| 完整旧996/4474 | `b1560ae44b6ffa4187e5a64c39017d606c603c48c54b8d3a472f23092bdb70bf` |
| 独立旧13 | `7d597774906828ba0a39e71cd5864c360935afc2e42d87b803a60d153d81dcf6` |
| 实际构建 | `ec13c192d3def7b14b119d4dcbbb5c0f05782c877b49b531106ffd9c8ee8dfca` |
| 完整primary | `aa8df5a0cd5163945706dcac1238a5d1eed9b87448126e75c9fc45c8dd092087` |
| 剩余逐位置/值/原始位及帧分类 | `780874dd4a8f3b5b0c06a53102d6600344f6b048e26e26be5c57640e592e3f7e` |
| 完整80次运行 | `0a9ba08d470633928949b40f48b70330ab060afa2d389882c8d504d55a24e76a` |
| 原暂缓2863标签核实 | `2763a9aad20a03e6f09081c545896e81ffeaf079e911e42e473c019f5aa7eacb` |
