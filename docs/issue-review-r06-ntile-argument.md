# R06：NTILE 参数的类型、NULL 和分区需求

原两条执行路径把无法解析的桶数和非正桶数静默换成1，导致 `ntile(0)`、
`ntile(-1)` 错误成功，NULL 也返回1。列表达式被当作数字文本猜测，并未按
实际源行求值。R05 的合法正数分桶公式保持，不能用它掩盖参数错误。

## 修复方法

Main 在开源前通过实际关系/范围绑定 NTILE 的 INTEGER 签名，持有原参数的
prepared AST 和执行 carrier。unknown 参数按真实 INTEGER 输入转换，规划
结构化常量；不执行函数/子查询来猜类型。BIGINT/BOOLEAN 的错误签名返回
42883，错误 unknown 字符串返回22P02；这类分析/常量错误不能被空源或LIMIT 0
隐藏。合法非正桶数是执行错误22014，没有执行需求时不制造该错误。

legacy 按窗口分区保留独立桶状态。每个参数表达式只在分区尚未得到非 NULL
桶数时求值；NULL 输出真正的 SQL NULL，不推进已输出桶序号。首个有效参数
必须为正数，随后复用它且不再求值后面的参数（即使后来是0、负数或除零表达式）。
桶大小仍根据整个分区行数计算。全部 NULL 的分区需要逐行求值，不能只读取首行。
实际源 descriptor、独立 NULL bitmap、绑定列 ordinal 和 typed cells 保持。

WindowOp 的有限常量入口改用有符号 INTEGER 检查，支持NULL/0/负数，不再
强制改为1或经unsigned解析。复杂参数使用上述 prepared legacy 接收器，
不把列名/函数/参数拼成数值。LIMIT 0 不扫描 legacy 源或执行参数副作用，
但开源前的签名/常量检查仍进行。

## 实际验证

原版本 `41054ead`，冻结程序 `root-before-r06.frozen` 的 SHA-256 为
`a0e7d9f70dbf6c60c1dbca1269e042f9dde7004e62f294f0a063a4f684d09885`。
证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`；不覆盖早期52/66控制的日志。

| 检查 | 实际终态 |
| --- | --- |
| 原20配置native，真实planner | 8失败，exit 1；`r06-original-native.log` |
| 原版本，最终72协议控制 | 57失败，exit 1；`r06-final72-original-protocol.log` |
| PostgreSQL18.6 / 180006，同一72控制 | 全通过，exit 0；`r06-final72-postgres-reference.log` |
| Root标准构建 | 实际重编Main/ExecutionPlan并链接，exit 0；`root-r06-build.log` |
| Root最终72协议 | 全通过，exit 0；`r06-first-candidate-protocol.log`及`r06-final-window_ntile_argument_protocol_e2e_test.py.log` |
| Root20配置及八完整原生邻居 | 九完整驱动exit 0；`r06-candidate-native.log`，全部58生产receipts及最终源签名不变 |
| Root十八完整协议/CLI驱动 | 全部exit 0；`r06-final-*_test.py.log`，含R33/声明比较/NULL/frame/NTILE分桶/EXECUTE及完整postgres_protocol |
| 文档状态与测试注册 | exit 0；474唯一注册/457 standalone e2e，全部Markdown本地文件链接与inventory另验 |

协议同时核对值、行序、列名、OID23、NULL和SELECT标签，覆盖原生路径和
nth_value 强制legacy路径、多分区、空输入、LIMIT0、±边界、NULL、unknown
字符串、错误签名、列表达式、准备语句参数，以及序列调用次数。分区开头为
NULL后初始化的预期序列来自实际PG，不用 `当前位置*buckets/rows` 重写夹具。
volatile 全NULL的9行控制应调用nextval18次；常量除零的后续错误不得推进序列。

验证生产对象须使用Root真实输入/flags/headers；不借私树对象或伪造receipt。
本项不关闭窗口frame方向R04、LAG/LEAD偏移R07，不宣称全注册回归或所有
窗口表达式已完成。
