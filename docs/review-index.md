# 代码审查索引

最新实施状态见[逐项修复进度](review-fixes-2026-10-13.md)，包含各项commit及回归。下方审查结果保留其原始输入和失败证据。

| 审查 | 输入 | 内容 |
| --- | --- | --- |
| [冻结d974c80d的完整注册回归](issue-full-registered-d974c80d.md) | `d974c80d`，源码/产物全程不变 | 1362个入口，1359个PASSED退出（含1个TLS环境skip）、3个FAILED，无漏跑/重复；原R79/R80失败入口通过，清理/准备/回滚失败保留 |
| [冻结b5def9ba的完整注册回归](issue-full-registered-b5def9ba.md) | `b5def9ba`，源码/产物全程不变 | 1360个入口，1358个PASSED退出（含1个TLS环境skip）、2个FAILED，R79/R80逐项修复另验 |
| [冻结18d85f4e的完整注册回归](issue-full-registered-18d85f4e.md) | `18d85f4e`，源码/产物全程不变 | 1339个入口，1319个PASSED退出（含1个TLS环境skip）、20个FAILED，全部失败和后续方法保留 |
| [2026-10-13 全仓库审查](code-review-2026-10-13.md) | `a502ffa3`，产品源码末次提交 `2dc9554e` | 模块审查、实际 SQL 差分、已知失败、测试注册、文档检查和逐项修复方法 |

问题分为实际复现、源码确认、历史失败待重验和待验证推断；修复建议不代表实现已经修改。
完成状态仍以[原总清单](postgresql-18-gap-audit.md)和[机器状态](gap-progress.json)为准。
全部文档的检查范围见[文档检查清单](documentation-inventory.md)。
