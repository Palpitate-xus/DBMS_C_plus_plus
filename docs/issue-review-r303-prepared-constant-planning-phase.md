# R303：prepared 常量规划与 custom/generic 失败阶段（DEFERRED）

状态：按用户最新“skip it”暂缓；未修复。R302 声明修复后，完整 704 在两侧仍各有
12 个此类阶段差异；R271 的 24 个大数场景另列，不混计。本项没有生产候选。

| 条件 | PostgreSQL 18.6 | 当前产品两侧 | 方法 / 状态 |
|---|---|---|---|
| fresh typed/unknown 参数，4 类×text/binary 的负阶乘 Bind | 22003、无 BindComplete/RowDescription，显式事务 Ready E | BindComplete + RowDescription + Ready T | 在真正选中 custom plan、实际输入准备后规划，不执行来源或用户 routine；暂缓 |
| 四条 literal 负阶乘（普通、WHERE false、LIMIT 0、空表）Bind | 22003、无 BindComplete，Ready I | BindComplete + RowDescription + Ready I | 对真正 generic/literal 常量也在 Bind planning 边界处理；暂缓 |
| warm 参数 generic plan，坏值 Execute | BindComplete/RowDescription 后 Execute 22003/E | 当前相同 | 保留正控；不能据相同表象声称产品已有真实 generic cache |
| 保存点恢复后的健康重绑定、执行 | 120/T | 当前相同 | 保留恢复控制，不改成仅检查错误码 |

704 的 PG 全通过及产品完整失败记录见 [R302](issue-review-r302-factorial-declaration.md)。
12 是场景数，不是 12 个独立 bug；没有删改这些断言或将它们算作已通过。

明确恢复后须审查实际 prepared statement、portal、参数帧、cached-plan owner 和失效
生命周期；用真实 callee 身份、不可变性及类型化输入做规划，Parse/Describe
保持纯分析。复用既有扩展协议错误/事务 abort 边界，在成功后才发布 portal。
须同时证明 custom/generic、首次/复用、失败不污染缓存、Close/替换/失效、
保存点恢复和序列/用户回调不提前执行；不是按函数名字执行一次来猜类型。

本地已读的 PostgreSQL 18.6 主源 `src/backend/utils/cache/plancache.c` 的
`choose_custom_plan` 区分无参数、强制模式、最初五个 custom 和 generic 成本
与平均 custom（含规划成本）的比较。不能实现成“第五次 Bind 后必定 generic”
或计入失败的规划尝试；应验证实际可用的 cache/成本 owner 并保留不支持边界。
这是后续调查方法，不是产品已实现的说明。

R302 未改 Bind/cache-policy；本项不恢复 R301/R160 角度函数、R156 完整常量、
R217 泛化 CTE Describe 或任何用户明确暂缓模块。若需要 header/API 变更，
须按当前全头重新编译 122 个生产 TU、stub 及六个真实 Main 测试 TU。
原 273 总清单仍未完成；不 push，Actions 禁用。

## 扩展复查：真实计划模式与共享 portal 前提（尚未修复）

在 R302 已接受版本上，三侧各完整 2347 操作、180 场景、五类目标（factorial、
abs、gcd、lcm、整数除法）、三模式、六种零行/来源形状及 0/8 次 warm 对照
均采集完成。两侧各 258 处原始帧/值差异：6 个 SET/SHOW、95 个 Bind、157 个
Execute；不是 258 个独立 bug。合法 plan_cache_mode 的 SET/SHOW 在产品为
XX000。auto 预热 8 次后无 FROM 转为 generic，但普通未分析空表仍 custom；
WHERE false/LIMIT 0 又可不求值。不能实现成固定第五次切换或对展开 SQL
一律预求值。现有全局 cache 存 EXPLAIN 文本，不等同于 retained prepared plan。

Bind 失败的未知 commandTag portal 在 PG 仍可返回 NoData/EmptyQueryResponse；
需与真正已定义的可执行 portal 区分。“成功才发布”指可执行计划，不能据此
假定所有失败 Bind 后都没有 portal。[R304](issue-review-r304-bind-portal-lifecycle.md)
保留共享生命周期和失败事务旧结果复用的未提交候选；候选不是 R303 的规划策略修复。
R305 的按行输出副作用也要求审查真实 typed cursor。R303 仍未修复、现已暂缓，没有
修改 generic/custom policy、GUC 或参数帧，也未恢复角度函数/完整常量模块。

收据：`/tmp/dbms-r303-prepared-planning.S0DOGkOK/completed-observations-receipt.json`
与 `completed-observation-summary-receipt.json`。完整 2347 原请求和所有失败保留。

## 用户暂缓边界（2026-10-27）

按用户最新“skip it”，暂停当前 R303 prepared planning 与关联 portal 模块，
包括未提交的 R304 候选和 R305 输出需求问题。普通 continue 不自动恢复；
后续方法仅供明确恢复后使用，当前不继续实现这些模块。

本次只提交问题记录和暂缓状态，不接受或提交 R304 的生产代码、测试注册及
三个夹具文件。工作区候选和完整测试证据保留；已跑完的有限矩阵不等于已
接受版本，也不代表这些问题、原 273 总清单或完整运行门已经通过。
已接受生产版本仍为 R302 `5bf05028`；新发现的独立 R306 命令标签问题仍 OPEN，
不在此次暂缓范围内。全部既有用户暂缓边界、十八处暂存差异、no push 和
Actions 禁用约束不变。
