# R176：普通聚合调用内 ORDER BY 的分析与执行

状态：Open，专项修复已通过两侧冒烟验证，完整回归及源码提交待验收。首次工作区72项有69项通过、三个Main入口因测试环境缺少显式数据目录而启动失败；已保留失败并重跑完整矩阵，不宣称全部问题已解决。[R175](issue-review-r175-ordinary-aggregate-identity.md)只修复输入身份，整数求和正确不等于排序正确。

## 完整101次发现观察

相同冻结采集器在 PG18.6 与未含本项修复的 accepted-only clean 各执行101次操作，
两端外层均0、101次全部实际发送、无传输/结构失败、序列基线健康、私有schema清理成功。
外层0仅表示采集完整，绝不是101项兼容通过；采集器没有预填 ORDER 语义答案。

| 实际问题 | PostgreSQL | 旧产品 | 修改方法 |
| --- | --- | --- | --- |
| SUM/COUNT 的缺失排序键，含真实行、空表、WHERE false、LIMIT0及三种协议，共24次 | 42703；extended/binary 在Parse阶段拒绝 | 全部成功并忽略缺失键 | 纯绑定真实排序键，错误先于发布statement，不能靠到达行才发现 |
| 排序表达式为真实 nextval，四个保留statement分别执行及重复Bind/Execute | 每次执行产生3次排序键求值 | 零次排序键求值 | 在正确行需求处求值、保存typed键并排序；不能只做静态验证 |
| Simple Query的 FILTER false、单NULL输入，共4次 | 成功；FILTER false不求键，NULL输入仍求1次键 | 0A000 | 保留FILTER、NULL、空输入与LIMIT的实际需求顺序 |
| float8输入1、-2^53、+2^53，独立键顺序的ASC求和，三种协议 | 0 | 1 | 真实排序且保留正确类型的逐步累加语义 |

DESC浮点三种协议两端均1，不能把一个方向相等当作排序完成。
两个初始 nextval 观察实际为1、2，验证观察器自身；每次后继观察也自增1，
上述执行次数已扣除观察器自身增量。四组 Parse/Describe、Bind/Describe 两端都不求值，
这些已经正确的纯metadata行为必须保留，不能为补执行而引入早期副作用。
真实命名statement和每次独立命名portal均使用正确生命周期；不会把过期portal错误归于本项。

## 已定位路径与修复边界

Parser把调用内ORDER文本存入第二个synthetic参数，不能把它当成普通第二个函数实参。
Main的legacy适配仍保留原ORDER消费路径；Volcano的Group/Parallel普通SUM/COUNT reducer
和storage fallback尚缺少一致的排序键分析、求值与消费所有者。
只有collection aggregate的排序路径不能证明普通聚合已经排序。

修复需要明确区分：排序键的纯分析和错误发布、逐行FILTER/参数/键的需求、typed排序及实际归约。
不得只删除synthetic参数、重写SQL文本、用整数和代替排序证据，或只校验名称却不执行键。
float8的扩展精度累加与排序还可能是独立差异，不能用同一错误值反推唯一根因。
多键、ASC/DESC、NULL位置、重复键、DISTINCT、重载及相邻query owner仍需完整验证。

## 补充31次观察与严格回归

补充采集在 PostgreSQL 与旧 accepted-only clean 均完整执行31次操作，无传输/结构失败，
序列基线与私有schema清理成功。输入是`(n,k)=(2,3),(1,1),(NULL,2)`，与上面101次的数据独立。

| 补充规则 | PostgreSQL实际结果 | 旧产品实际表现 | 修改方法 |
| --- | --- | --- | --- |
| 参数与ORDER键是同一`nextval`，含DISTINCT与COUNT | 每行只求值一次，不是参数、键各一次 | 0A000，无副作用 | 通过已分析表达式身份共享一个输入生产者 |
| 两个相同的易变排序键 | 每行只求一次键 | 0A000 | 保存并复用键，比较器不能再次求值 |
| `sum(DISTINCT n ORDER BY k)` | 42P10，零副作用 | 成功返回0 | 纯绑定校验ORDER表达式属于真实参数列表 |
| `sum(DISTINCT n ORDER BY n)` | 3 | 0 | 保留真正参数、DISTINCT及排序节点 |
| `ORDER BY projected`引用本次输出别名 | 42703，零副作用 | 成功返回3 | 在输入命名空间分析键，不能借用输出别名 |
| 数字常量多键与`USING <` | 均成功返回3 | 分别误报缺失`desc`、`n` | 数字键是值而非输出序号；保留多键、方向和NULL位置 |

已将101+31的全部132次操作保留为严格协议回归草稿，校验结果、SQLSTATE、错误阶段、
OID/宽度/格式/来源、NULL、命名statement/portal、重复执行及序列绝对值。
先以PG原始记录离线检查132/132期望一致，再真正完整运行同一夹具：

- PostgreSQL：外层0，132次全部发送、132次断言通过，完整footer为`132 FAILURES 0`。
- 旧 accepted-only clean：外层1，132次全部发送，51次断言通过、81次失败，无传输/结构失败。
  81是操作断言失败数，不是81个独立缺陷。

夹具终端汇总的`attempted`等局部字段只描述最后31次采集器；总132次已独立核对全部
连续编号的原始记录。另有10次source-free PG语法/角色观察：star与ORDER组合、缺失键表达式、
尾逗号等报42601，`USING =`报42809，嵌套聚合/窗口键报42803，SRF键及窗口聚合内ORDER报0A000。
这些不是生产修复通过结果。

## 扩大190次回归与候选失败记录

第二版严格夹具保留原132次操作，新增58次边界操作：三种协议下的计算式、方向身份、
歧义输出别名、多键、NULL位置、`USING >`、DISTINCT、缺失键、非法operator及窗口/SRF角色。
原132次的SQL、顺序、命名statement/portal与期望经UUID归一化逐项核对，均未删除或更换。
完整夹具的真实对照结果：PG外层0、190次全部观察且190次通过；旧 accepted-only clean
外层1、190次完整观察，59次通过、131次失败，无传输/结构失败，三个私有schema均清理成功。

这些是对照基线，不是候选通过。第一版候选已实际运行，结果如下：

| 候选检查 | 真实结果 | 定位及修改方法 |
| --- | --- | --- |
| 53项native夹具 | SIGABRT（进程结果-6），无完整成功footer | 聚合角色准备早于物理范围初始化，不能调用依赖物理schema的排序身份函数；改用已分析的scope/range/column身份 |
| 完整132次协议夹具 | 外层1；132次均完整观察，58次通过、74次失败，无采集失败 | 保留完整失败证据，不将成功编译当成执行正确 |
| 扩大190次夹具对第一版 | 外层1；172次完整观察，1次发送后连接重置、17次未发送；69次断言通过 | 第173次为扩展协议的`USING =`；不能把121次总失败都称为已执行的SQL差异 |

连接重置来自候选在raw parser中直接抛出语义异常。第三版将USING符号保留为真正AST字段，
在受保护的纯绑定阶段发布42809；解析器只负责语法。它也修正上述早期身份调用，并保留
计算式入口、ASC/DESC身份以及unsupported shape中真实排序键的纯角色校验。
native草稿增至61项；已验证的190项协议夹具未降断言、未增加超时或改动持久化策略。

第一版实际fresh58生产对象、native TU和stub均编译成功，normal build repeat无新编译；
第二版仅有编译/链接结果，没有运行或通过声明。其两个显式生产TU实参额外含`-Itests`，
四次预处理对照证明两个单元与普通生产include列表的预处理字节完全一致，但没有伪造
normal-wrapper cache stamp或声称它执行过常规repeat。

所有候选源码、对象与失败日志保持冻结。第三版public AST再次变化，实际全新编译58个生产对象，
没有复用第一、二版对象。其native检查仍因所有权索引漏访排序键而中止；协议190次全部完整观察，
83次通过、107次失败。连接重置已经消除，但窗口聚合内ORDER仍错误延迟到Execute发布0A000，
不能只比较SQLSTATE相同而忽略PG在Parse阶段拒绝。

## 当前工作区修复与验收边界

第四版补齐`indexExpression`对排序键的所有权登记，并在受保护的协议Parse入口纯绑定真实键，
使窗口聚合内ORDER的错误先于statement发布。全部190次SQL、顺序、期望与协议操作未改；
新的夹具另修正终端汇总，使其反映所有190次操作及两组真实序列基线，而不是最后一个采集器。
该夹具完整PG对照仍为190/190；旧accepted-only版本完整观察190次、59次通过、131次失败。

| 当前有限验证 | 实际结果 | 限制 |
| --- | --- | --- |
| accepted-only clean第四版 | native 61项与完整协议190次均外层0、完整成功footer；无采集失败 | 不是全部注册测试或原273清单验收 |
| 保留既有未提交改动的工作区版本 | 同样native61及协议190完整通过，源码/对象/签名/二进制前后核对通过 | 仍需完整相邻回归 |
| 工作区合并 | apply_patch安装13个源码/测试/脚本路径、51个唯一修改片段；全部2317个源文件符合原快照加本项变更 | 未夹带既有foreign修改，暂存区仍空，源码尚未commit |
| 工作区构建 | 实际fresh58生产对象、native TU和stub；常规wrapper复用当前58对象链接，repeat零编译/零链接 | 仅plain TCP配置，未声明TLS/sanitizer覆盖 |

回归矩阵保留已验收的全部70项，新增本项native与协议入口，每侧共72项：35 native、34协议、
3个真实Main入口。真实Main的accepted测试体只替换源码include路径；工作区原有session driver
未提交内容保留且不替代accepted测试体。两侧补充编译均完成，每侧固定97个对象和39个程序，
74条补充编译/链接命令均成功；编译成功不是72项运行通过声明。

首次工作区完整72项实际外层1：35个native、34个协议入口全部通过，包括native61及协议190；
最后三个Main入口在运行断言前均报`FATAL: an explicit data directory is required`，实际退出1。
它们没有执行成功断言，不能算作产品通过或产品回归；失败receipt和日志完整保留。
修正仅限外部测试启动环境：分别创建私有数据目录并显式传入`DBMS_DATA_DIR`，不改产品源码、
测试体、断言、超时、持久化策略或入口顺序。随后工作区及clean各三个Main预检均实际0。
该六项预检不能替代完整矩阵；现在重新从第一项完整执行工作区72项，之后串行执行clean72项。

当前证据覆盖夹具中的全局SUM/COUNT、计算式、键分析与错误阶段、元数据纯度、重复执行、
FILTER/NULL/空输入、多键/方向/NULL位置及共享生产者。它不证明GROUP BY内排序、全部自定义
类型/算子、全部参数推断或任意嵌套query owner；原273状态不提升，待验证边界不能标为全部完成。

## 固定证据

采集器冻结于 `/tmp/dbms-r176-ordered-aggregate-discovery-v1.2nCAf0ee`，
SHA256 `bdc1876eeecd1078961933fe82c69122ce8a3253d8433828a4fb0a1000447d6e`。
证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

- `r176-v1-reference101-discovery.log`，SHA256 `8ca781e2e801fb70b31815fc15da95429e98e8a8852e548978317cd3d46c5330`。
- `r176-v1-old-accepted-clean101-discovery.log`，SHA256 `dca13ca84e5724050374647b888040e4f553b15f572d8c60d21d8f65d33fe709`。
- `r176-shared-keys-reference31-discovery.log`，SHA256 `e87c2bb726da96b94637fef65f3a729ab8e21766bf7c59babf2abcd3f4517f46`。
- `r176-shared-keys-old-accepted-clean31-discovery.log`，SHA256 `327f3807cf3e7a6ec2256aad4f4b5d4a504111b2b81451a4cfad6f3f3cec9df5`。
- `r176-strict132-reference-v1.log`，SHA256 `0b18de9e9aaa830ddc6f84c8ef8f71c7267d0c11dc63e8a03b13b2da7b956974`。
- `r176-strict132-old-accepted-v1.log`，SHA256 `1360e6faa38c334b3cbdbb9121e1c6621eb96323caa232ab10ac45f70d2b7a5b`。
- `r176-strict190-reference-v2.log`，SHA256 `2a686e8881470f01796dd7c198f2383c647f3ccd8696b68891dbd2e9c59b8bcb`。
- `r176-strict190-old-accepted-v2.log`，SHA256 `56e858076798dbbfebba4bf0884d7add872fee815026702574370ab1b6ecefc1`。
- `r176-strict190-reference-v4.log`，SHA256 `cc39d00450f1799990c4637912ad12072e63282a7b394ae5e6210dfa1ad617d0`。
- `r176-strict190-old-accepted-v4.log`，SHA256 `19fdbcea1317229dab93d577563987dfd45c8e92e41696af3f0e6177da62cc57`。

第一版候选完整runtime receipt位于`/tmp/dbms-r176-v1-smoke-runtime.u72ordcc/completed-runtime-receipt.json`，
SHA256 `5185f42baf948fac228fab7dc3941e945a81921125765aa906ce17fadcd42d84`，固定全部三项失败结果及日志hash；
private native DATA保留于`/tmp/dbms-test-r176-v1-native.9qcxd1ds`，没有删除失败证据。

第四版clean专项runtime receipt：`/tmp/dbms-r176-v4-smoke-runtime.7e0jqvsu/completed-runtime-receipt.json`，
SHA256 `2f00739807de0e27f4b52da361bc0b9cd96174a314df6ef8a7815d19b6626bd0`。
工作区专项receipt：`/tmp/dbms-r176-root-smoke-runtime.xqbnndrd/completed-runtime-receipt.json`，
SHA256 `97ac8f148f9cea05a46a84be3b7f1b303279caf33a45730218a275a74f9e4523`。

首次工作区72项失败receipt：`/tmp/dbms-r176-root-full72-runtime.7zxeuezo/completed-runtime-receipt.json`，
SHA256 `cc20d7259381d34d5ff5ddcb02c70ac56684391645e4394095e9fa9927b46525`，实际69项通过、三个Main启动失败。
显式私有数据目录的六项Main预检receipt：`/tmp/dbms-r176-main-data-dir-preflight.wf0vxkm7/completed-runtime-receipt.json`，
SHA256 `ff20ea1098d39a0d15d8e3a8508874d752ec6e213b2e1492365a7aaa545705f4`，六项实际0，非完整72项结果。

完整原始帧、RowDescription、DataRow二进制字节、错误阶段、操作顺序与序列观察均保留。
24次缺失键等是模式组合，不是24个独立根因；生产修改尚未完成完整回归验收，不作已提交修复声明。
原273状态不提升；[R173](issue-review-r173-routine-signature-type-dependencies.md)当前分支保持未验收，
其他用户暂缓模块不恢复，不push，Actions保持禁用。
