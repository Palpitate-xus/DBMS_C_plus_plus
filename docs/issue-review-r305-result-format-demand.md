# R305：结果格式错误的按行需求与副作用边界（DEFERRED）

状态：按用户最新“skip it”暂缓；未修复。R304 的完整 463 仍在每侧留下 40 个该类场景差异，
全部保留，不算 40 个独立 bug，也不把 result format 2 实现成新编码功能。

| 场景 | PostgreSQL 18.6 | 当前产品两侧 | 修改方法 / 状态 |
|---|---|---|---|
| result format 2 的普通 SELECT Bind/Describe | BindComplete，描述 format=2 | Bind 08P01，事务提前失败 | 分开格式声明与实际输出编码边界；暂缓 |
| 非空 SELECT 的 Execute | 第一次真正输出时 22023 | 未定义 portal EmptyQueryResponse（R304 前还可能 34000/旧结果） | 在行接收器执行所需编码检查；不执行完全部查询后再补错误；暂缓 |
| WHERE false/LIMIT 0 零行 SELECT | 无行输出，无格式错误，SELECT 0 | Bind 08P01 | 不提前触发未被需求的输出函数；暂缓 |
| 两行 nextval 的格式失败与恢复 | 文本/二进制 0/1 完整后下一值为 3；格式 2 报错后下一值为 2 | 格式 0/1 相符；格式 2 在 Bind 拒绝，下一值为 1 | 保留真实副作用需求，不能预求值全部两行、也不能一律提前拒绝；暂缓 |

三侧各 123 条完整采集均 exit 0，仅表示采集完整。所有 setup 均成功；每侧有
10 个原始帧/值差异，其中一个是 [R306](issue-review-r306-temp-sequence-command-tag.md)
的 CREATE TEMP SEQUENCE 命令标签，其余涉及本项。不能将采集成功写成产品通过。

明确恢复后应审查 actual cursor、portal 流式接收器、元数据和输出函数的边界；精确
检查 0/1/其他格式、NULL/零行、行限制/悬停、失败事务、保存点恢复和 nextval。
需要与 R303 的 retained typed plan/参数帧协同，不以字符串重写或执行样本值
推断类型。当前没有本项生产候选；R304 不修改格式接纳策略。

收据：`/tmp/dbms-r303-prepared-planning.S0DOGkOK/completed-format-effect-observations-receipt.json`、
`completed-format-effect-summary-receipt.json`；[R304](issue-review-r304-bind-portal-lifecycle.md)
记录完整 463 以及固定原始帧。原 273 仍未完成，不 push，Actions 禁用。

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
