# R02：窗口排序、peer 和 MIN/MAX 消费真实声明类型

旧 WindowOp 对所有文本尝试 strtod，既把 TEXT 的 `01/1` 合并为 peer，也
把大 BIGINT 和高精度 NUMERIC 舍入。legacy 排序/MIN/MAX 还把值转换为 int64，
忽略非数字文本或空串，丢失 NULL 身份和部分文本输出。

现在 WindowOp 的窗口 ORDER、peer、MIN/MAX 和最终排序统一消费真实 Column
的类型/排序规则，通过已有类型比较器比较，不经浮点或整数猜测。legacy
同样使用声明比较器，保留 MIN/MAX 的原值；默认 ORDER frame 延伸至实际
peer 尾部，NULL peer、GROUPS 和 EXCLUDE 的 peer 判定不比较裸字符串。
COUNT(column) 不计 SQL NULL，但计空串与文本 `NULL`。

legacy 结果携带独立 NULL bitmap，直接发布结构化值，不从带空格的显示行
重猜值；MIN/MAX/nth_value 返回文本 `NULL`、空串与真正 NULL 均可区分。
DISTINCT/分页携带同一输出行索引，不能只对显示行处理而留下错误 wire 行。
[R03](issue-review-r03-window-partition-equivalence.md) 的精确分区保持；frame
方向/offset 算术属于 R04，legacy 最终 ORDER BY 属于 R33，未冒充一起修完。

## 实际验证

证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`。

| 检查 | 实际终态 |
| --- | --- |
| 初始原生20配置，R02原实现 | 16失败，exit 1 |
| 最终28协议，原实现、同一明确C声明 | 24失败，exit 1 |
| 最终28协议，实际PostgreSQL18.6 | 全通过，exit 0 |
| 最终28协议，Root候选 | 全通过，exit 0 |
| 最终扩展168原生配置，Root候选 | 全通过，exit 0 |
| 新驱动与七完整原生邻居 | 八完整native exit 0 |
| 新驱动与十完整协议邻居 | 十一完整协议exit 0，含EXECUTE和完整postgres_protocol |

七组数据为 TEXT、BIGINT、精确NUMERIC、NaN/±Infinity、全NULL、空串/文本NULL、
显式C排序。原生包含窗口方向、最终物理id/实际值排序、最终方向及默认/显式
NULLS FIRST/LAST，逐行核对值与bitmap。协议还核对OID、标签、nullable COUNT、
强制legacy的nth_value；无最终SQL ORDER BY时不强求相等peer的物理先后。

首次参考20项有6项因夹具要求相等peer固定物理先后而失败，保留
`r02-postgres-reference.log`。修正为精确行袋和nth_value(k)后参考全过，未放宽
任何 rank/peer/聚合/NULL 断言。首次候选168有24失败、28协议有4失败，均为
同一混合大小写TEXT组：参考库默认C和项目默认en_US文本排序不同。保留
`r02-candidate-native.log`、`r02-window_declared_comparison_protocol_e2e_test.py.log`；
该组显式声明同一 `COLLATE "C"` 后全部通过，所有原数据和SELECT保留。
没有把产品默认locale改成C来迎合测试；其余NULL/字面量夹具仍用默认声明。

原失败是 `r02-original-native.log` 和
`r02-final28-common-c-original-protocol.log`；最终是 `root-r02-build.log`、
`r02-final-native.log`、`r02-final-*_test.py.log` 及
`r02-final28-common-c-postgres-reference.log`。实际重编 Main/ExecutionPlan 并
标准链接exit 0；全部58份当前生产receipt与输入不变检查通过，未改公共头，
无借用私树对象、隐藏失败或全注册/全项目sanitizer声明。
