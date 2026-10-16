# 代码审查索引

最新实施状态见[逐项修复进度](review-fixes-2026-10-13.md)，包含各项commit及回归。下方审查结果保留其原始输入和失败证据。

| 审查 | 输入 | 内容 |
| --- | --- | --- |
| [冻结5091b39c的完整注册回归](issue-full-registered-5091b39c.md) | `5091b39c`，源码/产物全程不变 | 1391入口各一次，1390实际通过/1 TLS环境skip/0失败；全部58 receipt及57 test库字节独立终态核对0，不冒真实TLS或原273完成 |
| [旧全局BOOL参数与混合JSON](issue-review-r95-legacy-global-boolean-arguments.md) | `ccaf0ac5`后本工作区TableManage自有O2重编 | 原118/56和观察60/15失败；当前118、PG/Root60、十个完整native/十个完整协议通过，原setup超时/C warning保留，不冒全1391或原273完成 |
| [并行配置SHOW与Describe](issue-review-r94-show-parallel-workers.md) | R93后本工作区Main/Network/parser自有O2重编 | PG/Root40、原完整152、六个完整native及十个完整协议通过；不冒全1389或原273完成 |
| [布尔聚合参数与分组结果](issue-review-r93-boolean-aggregate-arguments.md) | R92后本工作区全部58单元自有O2重编 | 408+68、21完整native及18完整协议邻居通过；新协议150/152、SHOW两项独立失败保留，不冒全1387或原273完成 |
| [缺失TOAST计数器的数据保持](issue-review-r92-toast-missing-counter.md) | `4a4c7254` | 原写入失败破坏已提交长值；本范围修复，30完整native/18of20协议通过，两处普通DDL超时保留 |
| [冻结332bcff4的完整注册回归](issue-full-registered-332bcff4.md) | `332bcff4`，源码/产物全程不变 | 1382入口各一次，1379实际通过/1 TLS环境skip/2失败；全部58 receipt及57 test库字节独立核对一致，失败未抹除 |
| [冻结27bac6ee的完整注册回归](issue-full-registered-27bac6ee.md) | `27bac6ee`，源码/产物全程不变 | 1368入口，1362个PASSED退出（含1个TLS环境skip）、6个FAILED；无漏跑/重复，全部58 receipt及独立终态校验一致，失败与后续方法逐项保留 |
| [冻结d974c80d的完整注册回归](issue-full-registered-d974c80d.md) | `d974c80d`，源码/产物全程不变 | 1362个入口，1359个PASSED退出（含1个TLS环境skip）、3个FAILED，无漏跑/重复；原R79/R80失败入口通过，清理/准备/回滚失败保留 |
| [冻结b5def9ba的完整注册回归](issue-full-registered-b5def9ba.md) | `b5def9ba`，源码/产物全程不变 | 1360个入口，1358个PASSED退出（含1个TLS环境skip）、2个FAILED，R79/R80逐项修复另验 |
| [冻结18d85f4e的完整注册回归](issue-full-registered-18d85f4e.md) | `18d85f4e`，源码/产物全程不变 | 1339个入口，1319个PASSED退出（含1个TLS环境skip）、20个FAILED，全部失败和后续方法保留 |
| [2026-10-13 全仓库审查](code-review-2026-10-13.md) | `a502ffa3`，产品源码末次提交 `2dc9554e` | 模块审查、实际 SQL 差分、已知失败、测试注册、文档检查和逐项修复方法 |

问题分为实际复现、源码确认、历史失败待重验和待验证推断；修复建议不代表实现已经修改。
完成状态仍以[原总清单](postgresql-18-gap-audit.md)和[机器状态](gap-progress.json)为准。
全部文档的检查范围见[文档检查清单](documentation-inventory.md)。
