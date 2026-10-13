# R24：SQL EXECUTE 的实参是一次求值的 typed 输入

旧实现把实参表达式替换到模板每个 `$n`，使 repeated/multirow 重复执行
volatile 参数，unused/empty source 不执行参数，且丢失原投影标签。
实参中的序列、volatile SQL 函数与写入参数都受影响。

现在先整体准备 fromless 实参 SELECT，完成全部类型、禁止的子查询/aggregate/
window/SRF 角色及结构常量检查，再求值唯一一行 typed 参数；未引用的参数
同样求值。冻结为 StatementInput datum，带原声明类型、nullable 值与位置，
重新准备原模板；每个 `$n` 消费同一槽，不把参数表达式反复塞进 SQL。

支持的 read/DML 使用真实 prepared runtime。原 unsupported owner（包括
ON CONFLICT INSERT）通过纯 admission 判断接收冻结值的 legacy SQL，不能
在 typed query 已打开或产生目标副作用后回退重试。保留原输出标签和类型。
已有 `?` 模板行为不变；这里不宣称实现了完整 PG assignment cast resolver。

证据目录 `/tmp/dbms-review-root-conflict.Kdz38nRU/`：

| 实际检查 | 结果 |
| --- | --- |
| 初始51协议控制 | 21失败、exit 1 |
| 第一个51候选 | 后面的1/0未在首个nextval前规划，1失败、exit 1 |
| 修正全部参数结构常量规划 | 51全过、exit 0 |
| 扩展55：非法aggregate/window/SRF、typed TEXT | 首个扩展候选3失败，最终55全过 |
| 最终59：保留合法交错属性的volatile SQL函数 | Root 原实现28失败，PG18.6全过；R24-only受独立R43影响3失败 |
| R24+R43最终59 | 全过、exit 0 |
| R24前后完整11个协议邻居 | 两个完整批次分别exit 0；后者在R43 parser变化后运行 |

原与中间失败完整保留为 `r24-original-protocol.log`、
`r24-candidate-protocol-initial.log`、`r24-parameter-role-initial-protocol.log`、
`r24-final59-protocol.log`、`r24-root-original59-protocol.log`。PG最终是
`r24-final59-postgres-reference.log`；最终候选是
`r24-r43-final59-protocol.log`、`r24-r43-neighbor-*.log`。

首次邻居 ON CONFLICT owner 报0A000另保留
`r24-initial-neighbor-conflict_where_boolean_context_protocol_e2e_test.py.log`；
修正纯 admission 后原75项全过，未过滤 SQL 或改错误期望。
邻居包含 PREPARE lifecycle、冲突、INSERT SELECT、NULL bitmap、global
aggregate、TEMP、RETURNING、required keywords、参数 identity、投影标签和
完整 postgres_protocol。R43 的独立 parser 修复单独提交，不能用重排函数
定义 SQL 掩盖它。所有修改后的生产源实际重编标准链接；主分支集成另需
使用主分支当前头文件对象，不借用私树旧 ABI 对象。

## 主分支集成终态

`1d5aac14` 实际重编 Main 和 parser 并标准链接 exit 0；日志为
`/tmp/dbms-review-fix-donor.N9sdas6f/root-r43-r24-build.log`。
Root 新59控制及十二完整协议邻居全部 exit 0，含原完整 postgres_protocol、
HAVING BOOLEAN 与原冲突75控制（`root-r24-r43-*_test.py.log`）。九完整
native（`root-r24-r43-final-native.log`）全部 exit 0，包括真实 INSERT
SELECT source preflight、恢复图像及新函数属性52控制。新链接前核对全部
58份当前源/头/flags receipts，批次结束核对输入未变。

首次原生批次在编译前因调用者拼错 source-preflight 驱动名 exit 1，保留
`root-r24-r43-native.log`；使用真实文件名完整重跑九者通过，不宣称失败
批次通过。未修改公共头文件，不使用私树对象。这仍不是最终全注册回归。
