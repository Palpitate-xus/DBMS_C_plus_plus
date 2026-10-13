# R05：NTILE 按商和余数分配桶

## 根因和修改

原 WindowOp 与 Main 旧窗口分支均使用 `rowIndex * buckets / rows + 1`。
这会把多出的行摊到中间的桶，并在桶数多于行数时跳号：6 行分 4 桶实际为
`1,1,2,3,3,4`，应为 `1,1,2,2,3,4`；6 行分 8 桶实际为 `1,2,3,5,6,7`，
应为 `1,2,3,4,5,6`。

两条路径改为同一商/余数分配算法：`base = rows / buckets`，
`extra = rows % buckets`。前 extra 个桶各放 base+1 行，其余桶各放 base 行。
当桶数不少于行数时直接用分区内行号，避免空桶导致跳号。
计算不再包含 `rowIndex * buckets`；较大桶覆盖的行数不超过整个分区行数。
余数为零时不计算 `base+1`，也不在多桶情形执行零作除数。

本项仅修合法正整数参数下的分桶。零、负数、NULL、列表达式及参数的类型与
求值规则由 R06 单独处理，不能把这次通过解释为这些行为已经修好。
旧窗口分支忽略独立最终 ORDER BY 的 R33 也仍单独跟踪。

## 实际验证

基线：R11 提交 `43f2a21089afc2be54c593576ee9dfc8dd6228c8`，冻结程序 SHA-256
`5cc7fc081a10af7d31fc89f6713dbde65a3f64010a4e6e94d0afcae1fd5e025a`。

新增 `window_ntile_distribution_test.cpp` 使用真实 QueryPlanner / WindowOp，
每次同时有 6 行、7 行及单行三个分区。桶数为 1、2、3、4、8、INT_MAX，
预期桶序列全部显式列出，不复制产品计算公式。ASC/DESC 和空输入共 24 组。
基线实际在桶数 4、第 4 行处断言失败：实际 3，预期 2，exit 134。

新增并注册 `window_ntile_distribution_protocol_e2e_test.py`：同样的数据和桶数，
普通 WindowOp 及附加 `nth_value` 强制的旧窗口分支共 12 组非空控制，另有 6 组
空输入；同时断言行序、列名、OID 和 SELECT 标签。各分区的 id 区间在这里按
最终 ORDER BY id 排列，以隔离已经单独保留失败回归的 R33，而非放宽行序断言。

| 实际检查 | 基线 | 候选 |
| --- | --- | --- |
| 新 native，24 组 | exit 134，6/4 分桶错误 | 24 组通过 |
| 新协议驱动，18 组 | 12 组通过、6 组失败；两路径均有 6/4 与多桶错误 | 18 组通过 |
| PostgreSQL 18.6，新协议驱动 | 18 组通过 | 同一预期 |

证据目录：`/tmp/dbms-review-r05-evidence.fJQdkoHr/`。R26 冻结程序的先行结果也
保留，但上表使用 R11 基线的重新实跑结果。标准构建真实重编 Main 和
ExecutionPlan 后重新链接，实际构建 exit 0。

候选冻结程序 SHA-256：
`03942334c9007513cccc4a98e90e032889bb02b54dac84a0ffbb73a0ec855b18`。
原有 `window_type_protocol_e2e_test.py`、`window_null_order_protocol_e2e_test.py`
（195 控制）、`window_groups_unbounded_protocol_e2e_test.py`（147 控制）、
`unordered_window_protocol_e2e_test.py`（84 控制）以及 R11 新协议驱动（12 控制）
均实际 exit 0；没有修改原断言。本项不声称再次运行项目全套回归。
新 native 与原有 `window_exact_accumulator_test`、`window_null_order_test`、
`window_groups_unbounded_test` 共 4 个 native 驱动也全部 exit 0。
