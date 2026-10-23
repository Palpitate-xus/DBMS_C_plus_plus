# R237：interval 显式声明的协议 typmod

最新后续：[R238](issue-review-r238-interval-text-input-context.md)已修复原TEXT值上下文错误，原360和143两侧均全0；扩大后的2863仍各114个分析/描述/参数差异，不能报全绿。以下保留本次提交时历史状态。

提交时状态：有限声明元数据修复已验证；完整新夹具仍有独立 TEXT 输入值错误，不是全绿。

## 缺陷与修改方法

R236 的完整 143 项在两侧各剩一个错误：interval 值已正确按秒精度舍入，
但 RowDescription typmod 为 -1，丢失字段范围/精度。协议辅助函数只接收了
CHAR/BIT/TIME modifier，未接入真实 interval / interval[] 声明。

新增只读 declaration adapter：从显式 CastExpr 的原 typeName/独立 typeMods、
:: 右侧完整类型声明及 typed literal 的声明读取语法，不执行任何数据或函数。
先按实际 catalog 解析原始基类型，只有 OID1186/1187 才使用 interval modifier
规则，不能靠同名字符串把用户对象改成内置类型。保留原字段 mask 和 precision。
qualified internal `(mask,precision)` 不改写成另一段 keyword SQL；数组声明保留
数组身份。隐式/coercion、无修饰及计算包装不凭值继承长度/精度。
声明语法错误仍由已有分析负责，I/O、取消和目录损坏错误继续向上传递。

## 全部有限验收与历史失败

| 完整检查 | 旧版两侧 | 当前两侧 |
| --- | --- | --- |
| 新 360 项声明矩阵 | 各 329 个断言失败，退出 1 | 各剩 1 个独立值错误，退出 1 |
| 保留的 R236 完整 143 项 | 各 1 个 typmod 失败 | 全部通过 |
| 每侧 4 原生 +其余 9 协议回归 | 非本轮旧版全回归 | 全部通过 |
| 实际 PostgreSQL 18.6，完整 360 项 | 修正夹具后全 0，私有实例正常停止 | 同一控制和默认 fsync |

最终 30 次程序/驱动运行中，28 次退出0，两个完整新360夹具各退出1；不能报30全绿。
当前唯一失败 SQL 是 `CAST(CAST(nextval(...) AS text) AS interval DAY)`：
modifier 589823 已正确保留，Describe/Bind 没有副作用、Execute 调用也精确一次，
但值错为一天；实际 PG 先把已有 TEXT '1' 解析为一秒，再被 DAY 截成零。
源代码对所有裸数字文本都附加 target 最小单位，未区分 UNKNOWN input 和已经存在的
TEXT 值。独立 R238 继续修复，本项不删除 SQL/错误值断言来验收。
历史 329 是重复断言数，不是独立 bug 数；
R236 初次 143/1 失败、其完整 22 次有红记录均保留，不倒推成当时已经全绿。

最初 PG360 仅一个夹具值期望错误：TEXT '1' 先成为一秒，再被 DAY typmod 截为零，
不是一天。只修正原 SQL 的 text/binary 期望，原 Describe/Bind 无执行与 Execute
仅一次的 sequence 检查、全部 SQL 和 360 控制不变。首源码/日志/失败收据保留。
修正后完整 PG360 退出 0，然后才取得上述完整旧版 329 失败与候选 1 失败。

矩阵包含全部现有 interval field mask、秒精度、scalar/array、keyword/qualified
internal 类型、CAST/::/typed keyword literal，Simple Query、statement Describe、
portal Describe 和 Execute；NULL、真空表、LIMIT 0、计算包装，以及真实 sequence
未被元数据阶段执行、实际执行仅一次。它不是所有命名类型/优化器/函数的完成证明。

回归还覆盖 interval 接收/发送、存储/格式/输入错误、算术/日历顺序/赋值、参数
身份、旧 builtin binary arrays、TIME grammar precision、CHAR Describe 和 BIT
cast descriptor。没有修改测试超时、持久化选项、旧断言或默认协议主入口。

## 构建与提交边界

两侧 fresh NetworkServer TU与各四个原生测试；自己的其他对象、编译器、头/stub
核验后复用再链接。Root62生产对象/134头，纯提交60/131，不混布局，不冒 normal/
repeat、全部 fresh、全工作区构建或全套/TLS/sanitizer。Root Main 仍是 pre-R229
匹配 donor，跳过草稿/default dbms_main 不编译/运行。

2258 纯源码输入与实际验收组合一致，943 native+592 Python+3 Main=1538 仅清单。
独立 index 提交本项五路径（含 R236 的后续状态文档），原暂存18及其他工作区保留。
R238 值错误与 R235 算法仍未修复；后续需保留本项360及R235全部985/997，不删除控制。
R233 及其他暂缓不恢复；原273/冻结1492不改写，总目标未完成。不 push，Actions禁用。

本地证据目录：`/tmp/dbms-r237-interval-cast-typmod.fDYk1geM`。

| 记录 | SHA256 |
| --- | --- |
| 首次PG360，夹具错误保留 | `df43f060dde2e07eae7b6be801ffa6113f62d7464085d44ba88a80e6713be2a2` |
| 合法值期望修正 | `d20169a6819f493c5ce70cf4360ef7c31341299e04be846f94e0e58f8b0830d7` |
| 完整PG360 | `4547ad96473e3cdef11f42f79479dfb59056fa3988999a1b60175fe8413e572d` |
| 旧完整360 | `028ac635b314737a01572775e636103289286c48475e2ad53a82b2ea733902de` |
| 实际构建 | `4c2f28a573f77fc694f964956ef22a56c35bb8a545183dd56ed2212b53da2248` |
| 完整30次运行，两个新夹具各1值错误保留 | `0332a51750b46591b514d0dacc1c7656cce9879469189ff76a920a4462cb6c35` |
