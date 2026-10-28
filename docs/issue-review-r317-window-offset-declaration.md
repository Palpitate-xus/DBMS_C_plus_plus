# R317：LAG/LEAD 的协议声明与参数输入上下文

状态：已完整复现并冻结夹具；尚未实施生产修复，未验收。

## 已核实的问题

| 问题 | 实际观察 | 修改方法 |
| --- | --- | --- |
| 值与默认值共同类型丢失 | `lag(v,0,2147483648)` 的 PostgreSQL 声明是 BIGINT/OID 20；产品 Extended 的 statement/portal 描述仍可能为 INTEGER/OID 23。Simple 已使用真实共同类型。 | Parse 与两种 Describe 共用真实 query-bound 窗口声明；不按第一个实参或函数拼写猜类型。 |
| 错误签名发布过早 | 零/过多实参、错误 offset 类型、quoted 大小写错误等可能先发布 ParseComplete、描述甚至 BindComplete，之后才返回 42883/22P02。 | 在发布 prepared statement 前做同一纯声明绑定；错误时不留下已经发布的语句或伪描述。 |
| 未知参数未获得输入上下文 | `$1` 作为偏移或默认值时，ParameterDescription 可能仍为 OID 0；部分二进制 Bind 因而返回 0A000。 | 从真实选中的 INTEGER offset、anycompatible 值/默认值及显式 CAST 约束推断参数；按原始参数位置统一并验证，再发布。 |
| 共享参数约束遗漏 | `lag(s,$1,$1)` 中同一未知参数不能同时作为 INTEGER 偏移和 TEXT 默认值，PG 返回 42P08，产品可能执行成功。 | 保留绑定来源、原始参数 identity 和显式/隐式约束顺序；发现不一致返回 42P08，不能取第一个碰到的类型后忽略其他用途。 |

同样覆盖 LEAD、负/零/NULL/逐行偏移、动态默认值、空输入、Simple、Extended、
二进制输出/部分二进制输入、多个/共享/显式 OID 参数、显式 CAST、算术及
COALESCE 输入、named window、derived source 和多窗口目标。

原始源码线索不是已证实缺陷：`WindowFunctionSpec.offset` 虽是 int32，但
NULL/动态表达式会进入已有 prepared 接收路径。初始 678 项中 Simple 均与
参考相符，不需要为此添加 NULL 字段或修改公共头。后续 800 项另发现了
下面两个真实运行问题，不能继续宣称所有 Simple 情况都正确。

## 修改边界与实现路线

1. 真实声明所有者是 `TableManage.cpp` 的 window metadata 分支；只在已经
   解析到真实 LAG/LEAD 后，允许 genuine UNKNOWN metadata parameter 取得
   INTEGER offset 上下文。保留已有 scalar shadowing、schema、角色与 arity
   检查，不能按相同拼写把任意存储/原生回调当作内建窗口函数。
2. `NetworkServer.cpp` 在 Parse/Describe 使用同一纯 `prepareBoundQuery` 输出，
   覆盖所有实际窗口输出列；保留空行时的精确类型、宽度、typmod、format。
3. 参数推断必须消费真实绑定 AST、输入 cast/共同类型及 source-use 映射，
   区分共享参数与不同 occurrence；显式 OID 不覆盖。显式 smallint CAST 与
   外层 INTEGER offset 接收不是相同输入声明，不可把 wire OID 硬设为 23。
4. metadata placeholder 不能求值。原生夹具故意给参数放不可转换的内容，
   只检查声明，防止以运行参数、函数或扫描表行的方式猜 metadata。
5. 不恢复完整 prepared constant planning/portal、全局 presentation、通用
   raw analyzer 或完整 routine identity 改造。R315 仍按用户要求暂缓。

## 完整旧基线与未解决范围

独立 PostgreSQL 18.6、Root 投影、accepted-only 源码投影各完整观察 800
请求；同一冻结永久夹具在参考端 800/0 失败、旧 Root 800/248 失败、
旧 accepted-only 800/260 失败。原生 54 项旧两侧各 14 失败。数量为断言，
不是独立 bug 数；Root 与纯副本额外 12 项差异来自显式错误 OID 的报错时机。
没有生产、公共头、永久注册或测试预期修改。

完整 800 项保留：12 个常量 CAST 溢出在 Bind/Execute 阶段的差异属于用户
已暂缓的 R303 constant planning；PG 在 statement Describe 之后、BindComplete
之前报 22003，产品更晚报错，不在本项提前求值或伪造 Bind 错误。
CTE 执行 6 个失败另见 [R318](issue-review-r318-cte-window-owner.md)，
外层表达式执行 6 个失败另见 [R319](issue-review-r319-nested-window-receiver.md)。
两者仍 OPEN，未被用户暂缓；不得把它们归为既有允许忽略的问题。
修复本项不代表这 800 项或整个 QRY-08 已全部通过。

本机证据：`/tmp/dbms-r317-window-offset-audit.vcBaloAt/`。
`completed-observation-comparison-final-receipt.json` 保留全部报文差异；
`*-old-observation-final.log` 是原始帧；`*-old-observation-fixture.log` 是
同一 800 项严格断言，参考退出 0，旧产品两侧退出 1；
`completed-full-fixtures-frozen-receipt.json` 记录三个夹具文件的独立来源和 SHA。
夹具暂存 OUT，尚未注册或提交为已接受测试；原始 678/734 输入和日志也保留。
一次冻结元数据脚本错误地假设两侧均 248 失败而退出 1；核实纯副本实际 260
后只恢复 receipt/runner，参考生成的 golden 字节未变，未改任何测试期望。

声明依据：[PostgreSQL 18 窗口函数](https://www.postgresql.org/docs/18/functions-window.html)。
原 273 状态 22 complete /166 partial /70 unverified /15 deferred 不变。
总目标未完成，不 push，Actions 禁用。
