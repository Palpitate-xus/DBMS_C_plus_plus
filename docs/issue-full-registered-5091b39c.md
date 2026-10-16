# 冻结5091b39c的完整注册回归终态

2026-10-15，未过滤的 `bash scripts/build_tests.sh` 已完整结束，canonical和
包装器实际退出均为0。1391个预期入口各运行一次，顺序一致，无遗漏、额外项
或重复。1391个PASSED退出包含1个明确TLS环境skip：**1390实际通过、1跳过、
0失败**。这不是全部功能、真实TLS或原273项总账完成的证明。

| 输入/产物 | SHA256或固定值 |
| --- | --- |
| HEAD | 5091b39c45d5b299f98b54448c8e9b47cdab2278 |
| 全部tracked输入 | cdffee413358b5083d85e9e0378fe327dc7eb55e97280d020a1496a90f4cff18 |
| 生产binary | 73272c6b40888d127417f329e787837248382c936445abdbdd0d5b2c05304128 |
| 生产cache输入 | ee134ce09405aaba982f45a7b56fcff889efcde16d98b220c12e1085ff2ad20e |
| 完整日志 | b4dbf2cf58c5274269f578a0cc992d287ba0aa227ff43d270a55bf5dcdeac827 |

| 类别 | 全部入口 | PASSED退出 | 实际通过 | 环境skip | 失败 |
| --- | ---: | ---: | ---: | ---: | ---: |
| native | 881 | 881 | 881 | 0 | 0 |
| 真实Main frontend | 3 | 3 | 3 | 0 | 0 |
| Python注册入口 | 507 | 507 | 506 | 1 | 0 |
| 合计 | 1391 | 1391 | 1390 | 1 | 0 |

`statement_timeout_tls_protocol_e2e_test.py` 因binary没有OpenSSL支持而显式
跳过真实TLS控制。构建使用TLS stub/plain TCP；不能把该入口的退出0当作TLS
握手、加密或超时控制已通过。编译有warning，不宣称无warning或全库sanitizer
通过；本轮没有CMake、九客户端兼容矩阵、fuzz、长期soak或多节点复制验收。

## 冻结与独立终态核对

18:42:37 UTC启动唯一完整运行（原句柄2606），日志于21:05 UTC收尾；没有因为
轮询暂时无输出而重启。包装器记录canonical及最终退出0，运行期间HEAD、全部
tracked字节、生产binary/cache及58份生产receipt不变。原始目录为
`/tmp/dbms-review-full-r95.rG1utofx/`，包含runner.log、full-regression.log、
expected-drivers.txt和driver-summary.log；临时路径是本机辅助证据，不是
可移植发行产物。

23:58:14 UTC，另一个只读终态脚本实际退出0，独立核对清洁HEAD、全部tracked
输入摘要、binary/cache、生产及测试双stamp、58份生产receipt、57份test库
与对应Root生产对象逐字节相同；从当前注册清单重新生成881+3+507预期入口，
与冻结expected文件及1391条实际终态逐项、按顺序一致。日志摘要也再次匹配。
证据为该目录的independent-terminal-fence.log。核对完成后才解除冻结、编辑
本页及状态入口，没有在运行中改变源码、注册集、断言或期限。

开跑前复用同一Root的57份匹配O2生产库并fresh编译stubs，不冒称全58冷编译。
每个native驱动仍逐一实际编译、链接、隔离执行；三个真实Main前端不链接stubs。
本轮是[R92](issue-review-r92-toast-missing-counter.md)、
[R93](issue-review-r93-boolean-aggregate-arguments.md)、
[R94](issue-review-r94-show-parallel-workers.md)及
[R95](issue-review-r95-legacy-global-boolean-arguments.md)合入后的组合验证，
不是用各项定向通过数拼成完整结果。

## 历史失败与下一步

| 原始问题 | 本轮观测 | 后续方法及不能推出的结论 |
| --- | --- | --- |
| 冻结332bcff4的owner末尾DROP断言、SQRT准备CREATE SCHEMA超时 | 两个原完整入口本轮通过 | 保留[上一轮实际失败](issue-full-registered-332bcff4.md)；不猜测DROP返回值或根因，不重开R51，不称SQRT算法修复 |
| R92完整COPY准备CREATE TABLE、事务隔离ALTER超时 | 两个原完整入口本轮通过 | 未取得原失败线程/阶段证据，不以一次通过宣称间歇性超时修复；后续普通DDL调查保留原期限并记录确切SQL及阶段 |
| R95最初setup超时及C collation warning | 新完整协议入口通过；原失败与warning记录保留 | 不用本轮0覆盖原超时，不把component检查等同原warning调用栈；按[用户暂缓范围](deferred-scope.md)继续其它独立问题 |
| ENG-03与兼容性门禁 | 注册回归完整，但真实TLS缺失 | 固定真实TLS依赖后另行编译并完整验证，不能把stub结果登记为完整工程门禁；原九客户端及其它证据缺口仍需逐项验证 |

原总账仍为273项：22 complete、166 partial、70 unverified、15 deferred_by_user。
本轮不改变任何原项状态；`--require-complete` 仍应拒绝。独立暂缓专项也不算
完成，不恢复安全/TDE、CTAS/后台checkpoint、R15/R51/R65及新跳过项。
所有提交只留在本地，不push，GitHub Actions保持禁用，README保持长期项目入口。
