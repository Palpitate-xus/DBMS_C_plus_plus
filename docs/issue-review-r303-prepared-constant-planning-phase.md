# R303：prepared 常量规划与 custom/generic 失败阶段（OPEN）

状态：新发现、未修复、非用户暂缓。R302 声明修复后，完整 704 在两侧仍各有
12 个此类阶段差异；R271 的 24 个大数场景另列，不混计。本项没有生产候选。

| 条件 | PostgreSQL 18.6 | 当前产品两侧 | 方法 / 状态 |
|---|---|---|---|
| fresh typed/unknown 参数，4 类×text/binary 的负阶乘 Bind | 22003、无 BindComplete/RowDescription，显式事务 Ready E | BindComplete + RowDescription + Ready T | 在真正选中 custom plan、实际输入准备后规划，不执行来源或用户 routine；OPEN |
| 四条 literal 负阶乘（普通、WHERE false、LIMIT 0、空表）Bind | 22003、无 BindComplete，Ready I | BindComplete + RowDescription + Ready I | 对真正 generic/literal 常量也在 Bind planning 边界处理；OPEN |
| warm 参数 generic plan，坏值 Execute | BindComplete/RowDescription 后 Execute 22003/E | 当前相同 | 保留正控；不能据相同表象声称产品已有真实 generic cache |
| 保存点恢复后的健康重绑定、执行 | 120/T | 当前相同 | 保留恢复控制，不改成仅检查错误码 |

704 的 PG 全通过及产品完整失败记录见 [R302](issue-review-r302-factorial-declaration.md)。
12 是场景数，不是 12 个独立 bug；没有删改这些断言或将它们算作已通过。

后续须审查实际 prepared statement、portal、参数帧、cached-plan owner 和失效
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
