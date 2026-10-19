# R176：普通聚合调用内 ORDER BY 的分析与执行

状态：Open，已实际复现，尚未修复。[R175](issue-review-r175-ordinary-aggregate-identity.md)只修复输入身份，整数求和正确不等于排序正确。

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

候选实现只在独立临时副本冻结，尚未安装到生产工作区。第一阶段compiler-only验证目标为
全新58个生产对象、一个native测试TU和stub；public AST头已变化，不能混用旧对象。
53项native检查及132项候选协议检查尚未运行；没有最终双侧回归或修复验收声明。

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

完整原始帧、RowDescription、DataRow二进制字节、错误阶段、操作顺序与序列观察均保留。
24次缺失键等是模式组合，不是24个独立根因；本轮未改生产源码，没有新修复通过声明。
原273状态不提升；[R173](issue-review-r173-routine-signature-type-dependencies.md)当前分支保持未验收，
其他用户暂缓模块不恢复，不push，Actions保持禁用。
