# R271：无约束 NUMERIC 运行时范围扩展（用户暂缓）

2026-10-26 用户要求“skip it”，本项停止并暂缓。普通 continue 不自动恢复。
本项仅完成源码审查，未修改生产代码、测试或存储格式，没有新构建或通过声明。
已接受的 R269/R270 修复保留；原 273 项总清单尚未全部完成。

| 未解决问题 | 已有证据 | 后续修改方法（本次不实施） |
| --- | --- | --- |
| 非零 `round(1::numeric,1000)` 被误拒绝 | R270 实际 PostgreSQL 18.6 返回 1002 字符，项目两侧仍为 22003 | 分离显式声明的 precision 上限与无约束值的运行时范围，统一解析、运算、存储和协议往返 |
| 更大目标 scale 的零值准入依赖输入 scale | R270 保留的 90 个原生控制记录现状，不是兼容通过 | 在同一运行时范围设计下处理零值及正负 scale 边界，先取得真实参考端旧新对照 |
| 同一常量承担声明、运算预算、文本长度和 binary 准入 | 源码涉及 Numeric、表达式、类型注册、表存储及 PostgresNumeric | 不直接提高全局 `kMaxPrecision`；保留声明校验，检查历史 numeric 列、TOAST、text/binary 参数及结果 |

源码审查发现 numeric 列已经使用变长文本和 TOAST，不能把现有实现描述为仅有固定宽度存储。
未来修复需要同时检查历史 schema 的 dsize 解释及重启后的完整值，不能只改 ROUND 输出。
显式 NUMERIC 声明上限与无约束值范围是不同概念。[PostgreSQL 18 数值类型说明](https://www.postgresql.org/docs/18/datatype-numeric.html)

R270 的真实证据及未解决范围见 [R270 审查记录](issue-review-r270-numeric-zero-scale-boundary.md)。
其证据目录 `/tmp/dbms-r270-numeric-zero-scale.mabTejMX`、原日志和已接受提交 `030ef8c5` 不改写。
R268 首轮清理失败仍待查；R267、安全/TDE及其他所有用户暂缓项保持暂缓。
本次独立提交只记录用户决定，不把暂缓算作修复完成；不 push，GitHub Actions 保持禁用。
