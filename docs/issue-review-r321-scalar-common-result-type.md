# R321：共同类型标量结果没有转换实际单元

状态：V3 共同类型标量值交接达到下述有限验收条件；R322/R323 OPEN，273 总清单未完成。

## 复现、影响与原因

```sql
CREATE TABLE r321_t(id integer,b bigint,i integer);
INSERT INTO r321_t VALUES(1,NULL,7),(2,9,8);
SELECT COALESCE(b,i) AS v FROM r321_t
UNION ALL SELECT b AS v FROM r321_t ORDER BY 1;
```

PG18.6 正常返回 BIGINT；旧产品报 XX000：
`typed append child cell differs from its bound input type`。
COALESCE、GREATEST、LEAST 绑定 BIGINT，但实际返回选中的 INTEGER。
普通文本输出会掩盖这个错误；UNION ALL/标量 set 子查询的 typed 边界
按声明检查真实单元，因此拒绝不一致，不能删除这道检查来规避错误。

原无窗口/无 WITH 的 29 请求两侧各 12 个执行失败。独立扩展到 144
请求后，PG18.6 全成功，两个旧 R318 产品各 72 处严格差异，其中
66 处执行差异；不是 66 个独立 bug。原 29 输入及全部期望帧原样保留。
真实 evaluator 原生探针两侧各 144 检查、49 失败；保留原始日志。
最初原生夹具的两个手工 REAL 输入改用 PG 参考的规范科学计数法，
修订在生产代码变更前完成，所有类型/值/NULL/副作用断言均未变；最初
144/50 日志与夹具另行保留，协议 oracle 未改。

## 修改方法与边界

在真实 evaluator 使用已绑定的共同类型；只对 COALESCE 真正选中的值
执行现有 implicit CAST，保持未选分支不执行。全 NULL 仍携带声明类型。
GREATEST/LEAST 逐参数求值并先转换再比较，不只是重命名最终结果；保留
求值顺序、NULL 忽略、collation 与真正的 CHAR/BIT 无 typmod 转换语义。
转换由 RuntimeCell 参数携带实际值，不拼接 SQL，不重新执行表达式，
不删除 typed append 断言。不改公共头、Parser、CASE/NULLIF 或通用常量规划。

仅修改 ExprEvaluator、注册独立协议测试，并新增原生探针、冻结 144
请求 oracle 与本文档；R319/R320 的未验收 Main 与注册保持在原工作区，
从本项两份可执行投影及未来独立修复提交排除。现阶段尚无候选运行通过
或验收声明；必须实际编译、完整协议及既有回归、scoped UBSan 后再更新。

| 检查 | 实际状态 |
| --- | --- |
| 独立 PG18.6 完整 144 请求 | 全成功，私有集群已停止 |
| 旧 Root/accepted-only 144 请求 | 各 72 严格/66 执行差异 |
| 旧 evaluator 原生 144 检查 | 两侧各 49 失败 |
| 新候选编译/运行/回归/UBSan | 尚未验收 |

覆盖 Simple、Extended、二进制，typed NULL、整型/NUMERIC/REAL/DOUBLE、
文本/日期/数组、显式类型参数及序列计数的短路/全参数需求正向控制。
全部原始帧、严格断言与默认超时保留；不把既有元数据差异自动当作通过。
证据：`/tmp/dbms-r321-common-result.96fNbKSX/` 与
`/tmp/dbms-r321-coalesce-type-observation.Ly8jjvOy/`。
用户暂缓模块不恢复；原 273 总账仍未完成，未 push，Actions 保持禁用。

## V1 检查保留与 V2 声明前提修正（仍未验收）

上文为 V1 阶段记录，以下是后续真实状态，不覆盖旧日志。
V1 两侧真实 evaluator 原生 144 项均通过；完整 144 协议各 20 处严格
差异，其中 8 处执行差异，不能只凭消除了 XX000 就验收。两侧完整旧
747 的 raw 与严格检查也已完成，仍保留既有严格失败，未将其自动当作通过。
普通 212 条矩阵只完成构建，V1 未运行完整矩阵或 scoped 运行验收。

独立 PG/旧 R318 的 32 条对照证明：直接读取和 CAST TIMESTAMP 的值
是正确的。V1 的 LEAST(date,timestamp) 丢失小时，是原共同类型推断
错误地选择 DATE，随后正确的值转换截掉了时间，不能误报为存储故障。
字符族同理：把 VARCHAR/BPCHAR 和 TEXT 一律合为 TEXT，会错误改变
声明，并在真正转换时去掉 BPCHAR 尾部空格，属于本修复必须满足的前提。

V2 只在已确认的内置字符族、DATE/TIMESTAMP/TIMESTAMPTZ 族（包括其
数组、UNKNOWN）复用现有 selectCommonType；其他已有声明路径保持不变，
不恢复 NAME/REGTYPE、窗口声明、通用常量规划或用户暂缓模块。
新增私有 ExprHelper 实现改动，没有修改公共头或 Main。
新增 24 个反向字符/字符数组请求，组成 168 条完整独立 PG 参考；原
144 条请求和期望帧原样保留。旧产品两侧各 84 严格/78 执行差异。
原生探针保留原 144 断言，加入纯声明检查：168/9 后再加日期声明检查，
174/12 均在修改 ExprHelper 前实际观察到，两份旧探针和全部日志保留。

V2 两侧 ExprHelper 已真实新编译并与 V1 的真实 ExprEvaluator 对象链接，
其余生产对象按实际源码/头/MMD 匹配；旧六个真实 Main TU 无源码/头变化，
匹配复用后连同其余对象重链接全部 94 个回归二进制。第二版 Root 原生
174 项已通过；另一侧及 full 168/747 仍在收集，普通 212/scoped 96 尚未
完成验收。Root 168 已完整检查，只有下述两个独立问题的五处执行差异，
其余全部严格帧与参考一致；这不等于完整 168 通过或整项已提交。

## 独立 OPEN R322：FLOAT8 的文本输出格式

```sql
SELECT CAST(16777217 AS double precision) AS v;
```

PG18.6 文本返回 `16777217`，旧 R318 两侧返回 `1.6777217e+07`，
二进制值相同。32 条独立无共同类型函数的对照已在生产修改前确认，
列输入的显式 CAST 也复现；不是 R321 新引入，不能把它归入已暂缓
REAL 模块或当作格式任意、删除严格断言。当前 168 中两处文本模式
double-coalesce 差异属于同一个已独立复现问题，仍保持失败。
修改方向：核对 FLOAT8 的专属最短往返输出及固定/指数表示阈值，保留
精度、正负零、极值和二进制值；不要改变 REAL 的既有输出规则来凑结果。
证据：`/tmp/dbms-r321-independent-cast-controls.GkP3TiI0/`。

## 独立 OPEN R323：ARRAY[CHAR 列] 错误使用长度一转换

```sql
CREATE TABLE r321_cast(c character(4));
INSERT INTO r321_cast VALUES('abc ');
SELECT ARRAY[c] AS v FROM r321_cast;
```

PG18.6 返回 `{"abc "}`，旧 R318 两侧却返回 `{a}`；Simple、Extended
和二进制均实际复现。二进制元素长度也从 4 错变为 1，不是展示差异。
这是直接 ARRAY 构造，不含 COALESCE、窗口或 WITH。独立 41 请求的
参考全成功，旧两侧均复现，VARCHAR 数组/显式 TEXT 数组正向控制保留。
新 168 的三处 bpchar-array-common 差异同样保留，不能归成已通过。
源码线索：ARRAY 元素适配调用了显式 CHAR 默认长度一的 CAST 路径。
修改方向：使用真实已绑定元素类型的隐式、无 typmod 转换，保留 CHAR
填充、NULL、数组维度及二进制长度，不能全局改变显式 CHAR(1)。
证据：`/tmp/dbms-r323-bpchar-array-observation.z105vat5/`；参考集群已停止。
该诊断复制的早期比较脚本曾把观察标记误改为 R411，断言在比较前拒绝；
修正读取标记后重新比较原封不动的日志，未重跑或改写任何协议结果。

两个新问题均 OPEN，需各自修复、验证并独立 commit；不在本项偷改。
原总清单未完成，所有用户暂缓决定保留，不 push，Actions 保持禁用。

## V2 最终检查拒绝验收：原生接口回归与 scoped 超时

普通 212 条已完成，196 成功、16 非零；实际核对旧断言身份未改变，
但这不能替代补充的原生 API 验证。12 程序×两投影×新旧库的 48 条配对
现已全部完成：46 成功，两条 after 均在 coalesce_projection_null 异常终止；
旧 R318 两侧 before 均成功。原测试和全部新旧日志原样保留，不能更改
测试来掩盖这条真实回归，V2 不验收、不提交生产修复。

```cpp
// StorageEngine::queryExpr 的原生 SelectExpr/funcArgs 接口，非已绑定查询 AST
projectCoalesce(database, {"'first'", "1 / 0"});
```

旧接口按已有短路契约返回 first，V2 却报 22P02，把 first 转成 INTEGER。
原因是 evaluator 将未绑定接口的推测类型当成了查询已绑定的共同声明。
ExprHelper::evalStringWithNulls 只有 parse/私有行名映射与 scalar binding，
没有 QueryBinder 的共同结果声明；必须保留它的现有原生接口契约。
下一版应按真实 FunctionCall.resolvedResultType 拥有者转换准备好查询的
值，而非对未绑定调用强套推测声明；未绑定的 MAC/INET/CIDR 既有转换
行为仍须保留。不恢复通用常量规划、未知字面量全局转换或用户暂缓模块。
这不是拒绝 SQL 已绑定结果的正确转换，也不删除 typed append 的检查。

scoped 96 也已全部记录，84 成功、12 非零，无项目 UB 报告；但不能按
退出码总数宣布旧失败相同。accepted 侧 747 严格程序在第 287 请求出现
默认 socket 超时，最终 505 处严格失败（Root 侧仍为 457）。验收器已
正确拒绝此结果，完整原始 96、验收失败日志及超时均保留；不声称超时
原因已查明或 R51 已修复，不增加默认超时或放宽断言。

另外，普通 Root 最初完成 681 请求后第 682 超时的 raw747 已用相同
二进制、全部输入、观察器及默认超时完整复查成功；747 全部消息体与旧
R318 相同，仅比较时归一化跨私有库 T 六个来源字节。这是独立补充，
不替换初次失败，也不是 scoped 的相同二进制复查，不能混作同一次通过。

后续修复版须重新实际验证全部主矩阵及 48 条原生配对，并保留 V2 的
回归与超时证据。原 273 清单未完成，不 push，Actions 保持禁用。

## V3：真实绑定声明与原生接口提示分离（尚未整项验收）

源码调用链确认 queryExpr 的原生 coalesce 接口转给 evalStringWithNulls，
后者并没有为这个调用生成 QueryBinder 的结果声明。V3 只用调用节点
已有的真实 resolvedResultType 转换结果；无该声明时不把推测的显示类型
当作转换权限。未绑定的地址类型旧转换单独保留；不修改公共头、Main、
通用常量规划或接口契约，不删除 typed append 的类型检查。

原 174 个检查完整保留，增加 12 个未绑定接口与 NULL 载体控制；修改
owner guard 前在两侧 V2 库上各 186 检查、6 失败，原始夹具/对象/日志
均保留。修复后真正的 queryExpr coalesce_projection_null 在两侧均通过。
新增测试最初误将未绑定 'first' 的原生类型写成 TEXT，导致 V3 仅剩
一条类型断言失败；独立重链接未改的旧 R318 两侧，实际确认原生载体
是 character varying 且值为 first，因此只校正这一个新增期望，不改
生产代码、原 174 检查、其余 12 个行为条件或协议 oracle。初始 186
夹具和编译对象不覆盖，新校正夹具另存并真实新编译两侧测试 TU。

当前两侧 186 检查均通过；完整 168 原始与严格协议也已两侧实际检查，
仍只有旧 R322/R323 五处 data 差异，无新增类型/状态/元数据差异。
普通 212/scoped 96 全矩阵、完整 747 与 48 条配对须对 V3 重新验证；
不得沿用 V2 失败结果作为通过，不把修复版当作 V2 相同二进制重跑。
V2 普通 212、scoped 96 的 accepted 第 287 超时/505 失败，以及 48
配对中两侧原生接口回归均永久保留。尚未提交生产修复或声称总账完成。

## V3 最终有限验收与版本边界

此前“进行中/尚未验收”和 V2 拒绝记录均为真实过程，下面是 V3 终态，
不覆盖任何旧日志。V3 仅允许真实 FunctionCall.resolvedResultType 声明
驱动转换，不再把原生接口的推断提示当作绑定声明。由此保留原生接口
COALESCE('first',1/0) 的原有短路行为，同时 SQL 真实绑定的共同类型
仍转换实际 datum；旧地址族转换、NULL 和 collation 合约保持。

两侧真实 evaluator 各 186 项通过。新增测试有一个手工字面量类型期望
曾误写 TEXT；源码及实际链接旧 R318 库的两侧探针证明其原有类型是
character varying，故只更正这一条新增断言。原 174 条及其他断言未放宽，
原夹具、对象、失败日志保留，协议参考期望未改。

独立 PG18.6 完整 168 请求成功，参考集群已停止。两个当前产品的完整
168 请求各仍有五处严格失败：37/74 是独立 R322 浮点显示问题，
151/159/167 是独立 R323 字符数组问题；不是通过、不是用户暂缓。
其余 163 项严格相同，最初 29 项全部通过。没有删除请求或改变 oracle。

| V3 实际矩阵 | 完整程序记录 | 退出成功 | 非零退出 |
| --- | --- | --- | --- |
| 普通回归 | 212 | 196 | 16 |
| scoped UBSan | 96 | 84 | 12 |

上述 308 条主矩阵保留旧 R318 全部 300 条输入，再增加两侧新原生/协议
程序。旧非零断言身份逐项核对，747 请求的原严格签名全部保持；不能
声称全绿，不能把未证实的旧 metadata/Describe 残余自动归入暂缓项。
跨私有数据库只在另行声明的比较中归一化 T 消息的六个来源字节；原始
包不变。factorial 仅随机 schema 的 SQL 标签做 alpha-renaming，不改包体。

scoped 是实际当前 evaluator/helper、服务器及原生探针，不是全库
instrumentation。故意溢出的独立 canary 实测持久 log_path，在输出
丢弃时仍生成报告，并核对它与新服务器加载相同 libubsan。96 条项目
运行没有 UB 报告；canary 预期报告独立保存，不算项目 bug。

另有 12 个既有标量/NULL/字符/日期/网络/数组程序，在两侧合计 24 条
当前库运行均成功；与此前实际旧 R318 的 24 条成功日志组成 48 条配对。
旧 24 条是复用已有真实日志，新 24 条是实际重新链接和运行，不是本轮
新运行 48 条。V2 曾在两侧 coalesce_projection_null 各崩溃，V3 均通过，
V2 失败和被拒绝的全部 normal/scoped 结果仍独立保留。

V3 accepted raw747 初次在完成 359 请求后，第 360 请求默认 socket
超时，原日志和 SHA 保留。相同二进制、747 输入、观察器和默认超时
完整复查已结束，与旧 R318 的所有 Error/Notice/行/命令/Ready 包体
逐个相同，只有上述跨库 T 来源归一化。Root 初次完整 747 也已核对。
这是补充复查，未删除原失败，未声称超时根因已查明或 R51 已修复。
V2 的 raw/scoped 超时、旧 R318 的历史 96 条附加观察及 TEMP 超时也保留。

普通生产对象本轮 V3 新编译 evaluator 两个，匹配复用 120 个（包含
V2 实际新编译的 helper 两个）；按本项相对 R318 合计四个新生产 TU，
其余 118 个匹配、两个 stubs 匹配。新 186 夹具两侧真实编译，六个真实
Main TU 因源码/头未变而匹配复用，全部 94 个回归二进制重新链接。
scoped 本轮四个新 TU、56 个匹配对象，共 60 个；包括 V2 helper 的
两份真实 instrumentation，当前修改的两个实现均被覆盖；38 个原生
和两台服务器真实链接。编译器/flags/MMD/头/对象/链接均逐项核对。
accepted Main 单字节 cosmetic 例外仅沿用 R318 已证明记录，没有新增
例外，没有混入物理 Root 的 R319/R320 Main 或其他未验收改动。

本项仅改善 SQL-07 共同类型实际值交接、QRY-06 typed set 输入和
TYPE-02/04/06 的已验证场景，不能关闭完整条款。完整类型系统和全部
数据库能力未证明。独立本地提交只含两个私有实现、注册、新原生/协议
测试、冻结参考与本文档七个路径；其他工作区及原 18 个暂存路径保留。
R319/R320 和全部既有用户暂缓项仍暂缓；R322/R323 分别继续修复和提交。
273 总清单未完成，不 push，Actions 保持禁用。
