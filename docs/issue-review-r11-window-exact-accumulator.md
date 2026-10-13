# R11：窗口 SUM/AVG 移除溢出的整数旁路

## 根因与修改

`WindowOp::open` 虽已使用 `Numeric` 计算窗口 SUM/AVG，仍同时执行未检查的
`int64_t sum += number`。两个合法 BIGINT 最大值的和超出 int64，但 SUM(bigint)
的 NUMERIC 结果完全合法。最终输出使用精确结果不能消除已经发生的有符号溢出。
旧 Main 窗口分支也保留同样的整数累加和失败回退。

两条路径现在只保留精确累加器和非 NULL 计数。精确数值处理失败时不再静默跳过
输入、返回部分和或转换为 double/int64 的回退结果，而是交给既有错误处理边界。
旧分支全 NULL 的 SUM 原来返回初始化值 0，现在和 AVG 一样返回 SQL NULL；
AVG 的分母采用实际参与精确累加的非 NULL 个数。
COUNT、MIN/MAX、排序和 frame 计算不在本项修改范围。

## 实证与回归

新增 `window_exact_accumulator_test.cpp` 使用真实 QueryPlanner 构造并检查
`WindowOp`，不是复制累加代码的测试。6 组控制、24 行覆盖 BIGINT 最大值和最小值
重复相加、最大值与最小值抵消、小数、部分 NULL、全 NULL、空表，以及完整分区
和累积 ROWS frame。预期和/平均值以独立十进制字面量给出，按精确 Numeric 值比较，
不把小数显示位数变化算作数值差异。

基线为本私树 R26 提交 `caceec567198929d00d8ecdd37c588396bdd2d06`。
冻结 `ExecutionPlan.cpp` 经 `cmp` 校验，SHA-256 为
`27ea86af23bdd2c0cf54c9cc35cfb05dd6c9ef8daf50ac9bd4ac07019b74eae8`。
在独立临时目录用项目原有 flags 加
`-fsanitize=signed-integer-overflow -fno-sanitize-recover=signed-integer-overflow`
重编此真实生产翻译单元；其余生产对象逐一通过当前源码、headers 和 flags 的签名
检查后链接。候选同样重编并运行，没有覆盖标准对象或伪造缓存签名。

| 检查 | 基线 | 候选 |
| --- | --- | --- |
| 新 native，常规 O2 | 6 组通过 | 6 组通过 |
| 新 native，ExecutionPlan 单元 signed-overflow UBSan | exit 1；4267 行报告 `9223372036854775807 + 9223372036854775807` 溢出 | exit 0，6 组通过 |
| 新协议驱动的普通 WindowOp 部分 | 6 组通过 | 6 组通过 |
| 新协议驱动增加旧窗口分支 | 全 NULL SUM 实际 0，要求 NULL，exit 1 | 增加的 6 组通过；完整驱动 12 组通过 |
| PostgreSQL 18.6，新协议驱动 | 12 组、48 行通过 | 同一预期 |

这将原审查的静态未定义行为风险提升为真实执行器 UBSan 复现；没有把原 O2 正常
输出描述为数值失败。sanitizer 只覆盖上述生产翻译单元和测试，不是全项目 UBSan，
也未声明已用 UBSan 运行体量更大的 Main 翻译单元。

协议驱动 `window_exact_accumulator_protocol_e2e_test.py` 已纳入标准 E2E 清单，
检查数值、NULL 位、列名、OID 和 SELECT 标签。包含 `nth_value` 的合法查询强制旧
窗口分支。最初用 `ORDER BY id` 的控制同时暴露该分支忽略最终排序，已单独登记
R33，并在 `window_legacy_result_order_protocol_e2e_test.py` 保留原 SQL 和完整有序
结果断言；R11 的旧分支控制明确请求 `ORDER BY p,id` 隔离累加行为。普通 WindowOp
控制仍使用 `ORDER BY id`。原始混合失败日志保留，不能把 R11 完成解释为 R33 完成。
R11 最终候选上的 R33 控制仍实际 exit 1，此时所有数值和 NULL 已正确，差异仅是
最终行顺序 `5,6,1,2,3,4,7,8`。该独立测试与修复将在 R33 提交中交付。

第一版旧分支候选在新控制中报 `XX000 invalid numeric character`：旧 receiver
将 SQL NULL 渲染为字符串 `"NULL"`，此前依赖吞掉 Numeric 构造异常。最终候选在
仅 SUM/AVG 的数值输入处明确识别这个旧 receiver 的 NULL 标记，不把它作为数值
解析；其它精确处理失败仍正常传播。首次候选日志保留，没有删除对应断言。

证据目录：`/tmp/dbms-review-r11-evidence.JUBOhsDv/`，包括源快照、独立 sanitizer
构建脚本和对象、baseline/candidate native、PG18 协议和最初旧分支失败日志。
标准构建真实重编 ExecutionPlan 和 Main，再重新链接。最终候选 SHA-256：
`5cc7fc081a10af7d31fc89f6713dbde65a3f64010a4e6e94d0afcae1fd5e025a`。

最终候选的 4 个 native 均 exit 0：新精确累加测试、原有 `window_null_order_test`
（144 控制）、`window_groups_unbounded_test`（72 控制）、`window_query_metadata_test`。
以下 4 个原有完整协议驱动也均 exit 0，未修改原断言：

- `window_type_protocol_e2e_test.py`
- `window_null_order_protocol_e2e_test.py`（195 控制）
- `window_groups_unbounded_protocol_e2e_test.py`（147 控制）
- `unordered_window_protocol_e2e_test.py`（84 控制）

最终候选的完整 `postgres_protocol_test.py` 也实际 exit 0。
这是本项定向与相邻回归结果，不是项目全套回归或其余窗口问题全部完成的声明。
