# R04：窗口 frame 的方向、空区间和初始化阶段

旧实现把 PRECEDING/FOLLOWING 压缩成非负整数，结束边界又强制包含当前行；
因此 `ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING`、合法反向空区间、
GROUPS/RANGE 和 NTH_VALUE 都可能返回错误的行。BIGINT offset 经 `stoi`
还会抛 XX000。非法边界、NULL/负数和当前层变量缺少正确的分析/执行阶段检查。

## 复现与修复方法

```sql
CREATE TEMP TABLE frame_example(id INT, v INT);
INSERT INTO frame_example VALUES (1,10),(2,20),(3,30);
SELECT id, sum(v) OVER (
  ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
) AS total FROM frame_example ORDER BY id;
-- 正确结果：(1,NULL)、(2,10)、(3,20)，不是包含当前行的累计和。
```

Parser 保留每个边界的实际类别、offset AST、BETWEEN、显式方向及 NULLS
语法；结构上非法的组合返回 42P20，同类边界的反向 offset 则是合法空帧。
PARTITION/ORDER 中名为 rows/range/groups 的列不再被误识别为 frame 起点。

Binder 在原始查询的真实命名空间绑定 offset：ROWS/GROUPS 使用 BIGINT
输入，RANGE 按排序键声明选择整数、NUMERIC 或浮点输入。当前层变量在
嵌套 CTE/派生表中同样返回 42P10；独立子查询的局部列不是当前层变量。
这些纯检查先于任何行需求，不能由 WHERE FALSE 或 LIMIT 0 绕过。

新的共享 WindowFrame 使用半开区间。ROWS 先比较距离再形成下标，避免
INT64 极值溢出；GROUPS 按 peer 边界移动；RANGE 对整数/NUMERIC 使用精确
十进制算术，保留 DESC、NULL 排序和 peer 身份。原生 WindowOp 与 legacy
聚合、FIRST/LAST/NTH_VALUE 接收同一套边界和 EXCLUDE 语义，不再把空帧
强行扩成一行。NTH_VALUE 保留真实目标行的 NULL。

执行 owner 在打开输入前初始化 offset。ROWS/GROUPS 的 NULL/负数即使空
输入也报 22004/22013；RANGE 的 NULL 在初始化时报错，负值则在真实
in_range 边界需求时报错，空输入或忽略 frame 的排名函数不提前触发。
LIMIT 0 跳过运行期初始化，但不跳过纯分析错误。

相同 parsed window 定义的 offset 只初始化一次，不随函数数目、行数或
分区数重复求值。原始 AST 保留限定符、显式 ASC/NULLS/BETWEEN 和显式 cast
等定义差异；不按求值结果或规范化后的 SQL 猜测是否共享。初始化测试用
返回固定距离的 VOLATILE 函数与序列观察次数，不要求不同 WindowAgg 的
未承诺先后顺序。

## 验证记录

证据目录：`/tmp/dbms-review-fix-donor.N9sdas6f/`。当前原版本为 `0706d4c9`，
冻结程序 `root-before-r04.frozen` SHA-256：
`845694d4f4ace98799780c326e0e6e5273777ca0450d9d6c13e653fcae6af5e0`。

| 检查 | 实际结果 |
| --- | --- |
| 原版本最终 307 协议控制 | 275 失败，exit 1；`r04-original307-protocol.log` |
| PostgreSQL 18.6 / 180006，307 边界控制 | 全通过，exit 0；`r04-final307-postgres-reference.log` |
| 原版本 78 初始化控制 | 43 失败，exit 1；`r04-original78-initialization.log` |
| PostgreSQL 18.6 / 180006，78 初始化控制 | 全通过，exit 0；`r04-final78-initialization-postgres-reference.log` |
| 最终 Root 307 边界及 78 初始化协议控制 | 全通过，exit 0；`r04-final-accepted-window_frame_*_protocol_e2e_test.py.log` |
| 最终 Root 349 native 及十四完整原生邻居 | 十五完整驱动 exit 0；`r04-final-accepted-native.log`，58 当前 receipts 与终态输入一致 |
| 最终 Root 十八完整协议驱动 | 全部 exit 0；`r04-final-accepted-protocols.log`，含 R06/R07/R33/R44、EXPLAIN JOIN、逻辑解码及完整 postgres_protocol |
| 最终标准生产构建 | exit 0；`root-r04-early-preflight-build.log`，实际重编 Main 并标准链接 |
| 文档/注册/语法门禁 | 468 Markdown UTF-8/本地文件链接/inventory、479 唯一注册/462 standalone、498 Python AST/13 shell 语法全通过；文档状态/版本/兼容声明及六个总账单元测试通过 |

原生驱动覆盖 336 个实际 WindowOp 配置及 13 个纯绑定负向控制；协议驱动
覆盖三种模式、单独原生及 NTH 强制 legacy、双向 offset、空区间、四种
EXCLUDE、BIGINT 极值、精确 NUMERIC、小数距离、DESC/NULLS 和错误阶段。
初始化另有 78 个独立控制。

扩展的 native 349 控制通过，包含 query-host 的六个嵌套来源拒绝控制。
首轮最终入口协议 305 控制还有 3 个 42P01/42P10 阶段差异，日志
`r04-accepted-window_frame_boundaries_protocol_e2e_test.py.log` 保留：原本的
preflight 位于 legacy FROM 处理之后，派生表已先被误处理。当前已将纯
绑定移到 SELECT 的更早入口，最终 307 协议全部通过。两个独立 CTE/派生
offset 阳性控制另确认不能把子查询自身局部列一律拒绝为当前层变量。

保留中间失败：初始 SUM 窗口 metadata 被当作 scalar 函数的 42883，已在
真正 query-host 角色处修复；限定列被 legacy normalizer 消掉导致的两个
初始化计数失败也保留。最初 PostgreSQL 初始化夹具把不同定义的执行次序
当成承诺，7 项失败；改用固定返回值后仍严格核对行和副作用次数。首次
142 参考的 RANGE 负 offset/空输入预期错误也保留并按实际参考纠正。

公共结构变化后的真实 fresh58 第三批通过。第二批编译期间头文件改变，
构建签名保护拒绝发布，exit 123，不记为编译器错误或成功。随后一次标准
构建期间 binder 变化同样被输入栅栏拒绝发布。最终当前输入仍需重新编译、
逐对象核对并运行门禁，不借旧 ABI 对象。最终标准构建和上述两个门禁已
完成该验证，未把中间失败轮次改写成成功。

这是具体缺陷的修复，不宣称完整 PostgreSQL window 兼容。带 offset 的
DATE/INTERVAL RANGE 仍返回 0A000；特殊浮点 NaN/Infinity 等未在此验收。
专项结果不能替代最后完整注册回归，也不改变 273 项功能总账状态。
