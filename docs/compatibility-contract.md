# 兼容版本契约

本契约定义 DBMS_C_plus_plus 可以对外作出的兼容性声明。它把产品版本、
PostgreSQL 行为目标、wire protocol 和项目扩展分开版本化，避免把“psql 能连接”
误写成“与 PostgreSQL 等价”。当前总账状态仍只取自
[`postgresql-18-gap-audit.md`](postgresql-18-gap-audit.md) 与
[`gap-progress.json`](gap-progress.json)。

## 四个独立版本轴

| 版本轴 | 当前值 | 含义 | 不代表什么 |
| --- | --- | --- | --- |
| 产品版本 | `0.2.0` | 本项目自己的 SemVer；唯一代码事实源是 `src/common/version.h`，并与 CMake、包和 CHANGELOG 校验 | 不等于 PostgreSQL server 版本 |
| 行为兼容目标 | `PostgreSQL 18`，差分基线 `18.6` | `postgresql18` 模式要逐项逼近的 SQL、catalog、事务、错误和协议行为 | 当前尚未达到该目标 |
| wire protocol 上限 | `3.0` (`196608`) | 本地实现的最高协议版本；3.x 新 minor 会通过 `NegotiateProtocolVersion` 降到 3.0 | 不表示实现 PostgreSQL 18 的全部前后端消息或 libpq 行为 |
| 扩展契约 | `extended` | 显式启用项目自己的非 PostgreSQL SQL 和管理入口 | 不是 PostgreSQL、MySQL 或其磁盘格式兼容模式 |

这四个值可以独立变化。产品发版不能暗示 PostgreSQL 行为目标已经完成；修改
wire protocol 不能自动提升行为兼容声明；增加 extended 命令也不能扩大
`postgresql18` 模式的声明范围。

## 对外定位和模式边界

在 273 项审计全部达到验收门之前，本项目的准确定位是：

> 一个提供 PostgreSQL wire protocol 3.0 子集、可供部分 PostgreSQL 客户端连接的
> 自有 DBMS，正在以 PostgreSQL 18.6 为差分基线建设行为兼容性。

不得使用“PostgreSQL 18 compatible”、“drop-in replacement”、“PG18 equivalent”
或同义的无范围限定声明。

`postgresql18` 是默认模式。它只能接受 PostgreSQL 18 语法或项目已经实现并验证的
对应行为；未实现能力必须返回稳定且合适的 SQLSTATE，不能写入 sidecar 后伪造成功。
项目语法必须被拒绝，不能因默认值、客户端或调用入口不同而泄漏。

`extended` 只能由 `DBMS_COMPATIBILITY_MODE=extended` 或会话级
`SET compatibility_mode = extended` 显式进入。它可以开放项目命令，但不会把没有
运行时的 PostgreSQL 对象变成成功操作。切换模式不改变产品版本、wire protocol
版本、数据格式或当前 database。

## 两级验收标准

### A. “PostgreSQL 客户端可连接”

只有同时满足下列条件，才可对明确列出的客户端版本声明“通过 PostgreSQL wire
protocol 子集连接”：

1. 真实 TLS 构建完成 startup、认证、ParameterStatus、简单查询、扩展查询、取消、
   通知和错误恢复的协议测试；未覆盖的消息必须列为限制。
2. 服务器正确协商其真实最高 wire 版本；不得把 `server_version` 或协议协商值用作
   行为兼容证据。
3. 对支持的消息和 SQL，返回的帧顺序、字段、OID、command tag、SQLSTATE 和事务状态
   有差分回归；畸形输入 fail-closed，不能崩溃或使后续连接失步。
4. 发布说明列出测试过的客户端、版本、TLS/认证方式及已知缺口。仅“psql 能启动”
   或单条 `SELECT 1` 成功不通过此门。

通过 A 门只允许声明 wire/client interoperability 的限定子集，不能声明 PostgreSQL
行为兼容。

### B. “PostgreSQL 18 行为兼容”

只有同时满足下列条件，才可声明 PostgreSQL 18 行为兼容：

1. [`postgresql-18-gap-audit.md`](postgresql-18-gap-audit.md) 的 273 项全部为
   `complete`；`partial`、`unverified` 和 `deferred_by_user` 均阻止该声明。
2. 183 个 PostgreSQL 18 命令及 SQL/type/function/catalog/transaction/storage/
   replication/protocol/operations 各功能族都有可重复的 PostgreSQL 18.6 差分矩阵；
   值、类型元数据、错误、可见性和副作用均一致，不能只比较文本成功与否。
3. PostgreSQL regression/isolation、协议客户端、崩溃恢复、故障注入、升级/降级、
   sanitizer、fuzz、soak 和并发 history 验证达到审计中对应门槛，并为 clean worktree
   生成机器可读发布报告。
4. `postgresql18` 模式下所有项目扩展均有负向测试；未实现能力明确失败，不存在
   compatibility-record 假成功。
5. 发布文档逐项限定二进制、扩展 ABI、catalog、逻辑/物理复制及 on-disk 格式。
   除非它们各自通过专项验收，不得推导为兼容。

## 发布声明矩阵

| 状态 | 允许的声明 | 禁止的推导 |
| --- | --- | --- |
| A 门未通过 | “实现中的 PostgreSQL protocol 接口” | 任何客户端兼容或 PG 行为兼容声明 |
| 仅 A 门通过 | “已验证客户端 X/Y 可通过 wire 3.0 子集连接”，并链接报告和限制 | PostgreSQL-compatible、SQL/catalog/transaction 等价 |
| A、B 门均通过 | “对 PostgreSQL 18.6 行为兼容”，并链接全量报告与仍不兼容的非目标接口 | PostgreSQL 二进制、extension ABI、物理复制或磁盘格式兼容，除非另有专项证明 |

当前状态是 **A 门和 B 门均未宣告通过**。任何发版、README、包描述和客户端握手
字段都必须遵守这张矩阵；实时进度变化由总账决定，而不是修改本文中的结论。

## 变更规则

- PostgreSQL 主版本目标、差分 patch baseline 或 wire 上限变化，需要独立变更记录、
  对应矩阵刷新和测试更新。
- 行为差异必须进入 273 项总账对应条目；没有条目时先新增审计项，不能只改营销文案。
- 只有 `scripts/check_gap_progress.py --require-complete` 成功且 B 门的发布证据齐全，
  才能把 B 门状态改为通过。
- `server_version` 为客户端解析所需的互操作字段，不是产品身份或兼容认证；产品身份
  始终使用 `DBMS_VERSION_STRING`。
