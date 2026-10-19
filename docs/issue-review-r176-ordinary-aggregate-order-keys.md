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

## 固定证据

采集器冻结于 `/tmp/dbms-r176-ordered-aggregate-discovery-v1.2nCAf0ee`，
SHA256 `bdc1876eeecd1078961933fe82c69122ce8a3253d8433828a4fb0a1000447d6e`。
证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

- `r176-v1-reference101-discovery.log`，SHA256 `8ca781e2e801fb70b31815fc15da95429e98e8a8852e548978317cd3d46c5330`。
- `r176-v1-old-accepted-clean101-discovery.log`，SHA256 `dca13ca84e5724050374647b888040e4f553b15f572d8c60d21d8f65d33fe709`。

完整原始帧、RowDescription、DataRow二进制字节、错误阶段、操作顺序与序列观察均保留。
24次缺失键等是模式组合，不是24个独立根因；本轮未改生产源码，没有新修复通过声明。
原273状态不提升；[R173](issue-review-r173-routine-signature-type-dependencies.md)当前分支保持未验收，
其他用户暂缓模块不恢复，不push，Actions保持禁用。
