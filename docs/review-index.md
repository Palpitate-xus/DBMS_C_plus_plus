# 代码审查索引

最新实施状态见[逐项修复进度](review-fixes-2026-10-13.md)，包含各项commit及回归。下方审查结果保留其原始输入和失败证据。

| 审查 | 输入 | 内容 |
| --- | --- | --- |
| [R112 无 typmod 具名数组输入](issue-review-r112-unconstrained-named-array-input.md) | BPCHAR/BIT 真实声明与 wire Bind | 实际180006参考57零失败、旧44失败；逐版修复、有限回归和版本范围见记录，不关闭原TYPE-16或原273 |
| [R108 ARRAY_POSITION元素相等](issue-review-r108-array-position-typed-equality.md) | `95be64fb`后对照与逐版修复 | 第五版354/258、原208/80/array库/654及32native/29协议/3真实Main共64完整入口均0；28补充JSONB及核心/最终fence0。旧258/158、320/183及各版失败保留，独立本地commit，不冒新全量 |
| [R107 长度函数签名](issue-review-r107-string-length-signatures.md) | `25925345`后四名11条声明 | PG/Root654、607原生及24native/18协议/3真实Main均0；实际2/1/1增量normal/repeat、核心/最终独立核对0，单项本地commit。旧654/392、607/440和首两版失败保留，不冒新全量，R106暂缓 |
| [R106 COALESCE显式CAST输入分析](issue-review-r106-coalesce-explicit-input-analysis.md) | `2556a3d8`后只读复查 | 用户2026-10-16明确跳过，未修复；四条观察及未注册612控制（参考零失败/旧168失败）保留，无生产改动，不扩R104证明或重开专项 |
| [R104 网络实际值转换](issue-review-r104-network-value-conversions.md) | `b6595d2e`后五CPP增量修复 | PG/Root649、原738及完整SDK、21native/17协议/3真实Main通过；4/3/2增量normal/repeat与核心/最终核对0，单项提交。旧649/419、SDK134及首两版失败保留，不冒新全量 |
| [R105 限定NAME→INET/CIDR](issue-review-r105-network-name-input.md) | `fb131c02`后精准NAME源身份 | PG/Root738、完整SDK及21native/16协议/3真实Main全过，自身真实全58/normal/repeat/核心及最终核对0，单项提交；旧738/34与SDK134保留，R104后续独立关闭 |
| [R103 NAME→MAC显式输入](issue-review-r103-macaddr-name-input.md) | `e1f8cf4a`后精准NAME源身份 | 最终PG/Root366、完整SDK及21native/16协议/3真实Main通过；最终自身全58/repeat/fence0，单项提交；旧366/30、SDK134及首版失败保留，R104/R105后续独立关闭 |
| [R102 INET/CIDR输入](issue-review-r102-network-input-notation.md) | `7533f27e`后共享codec与真实源类型预检查 | 旧188/72、672/389；当前188/纯组件sanitizer、PG/Root672、2558对照及21native/16协议/3真实Main通过；最终全58/匹配核心/fence全0，单项提交，首版672/23保留；R103/R104/R105后续各独立修复 |
| [R101 MAC输入与宽度](issue-review-r101-macaddr-input-and-widths.md) | `3d2e49f6`后共享codec、真实CAST/赋值/COALESCE/VALUES | 旧75/38、328/186；当前75、PG/Root328、236差分、20native/15协议/3真实Main通过；头修正后全58重编及产物相同证明、新核心2/328/fence全0，单项提交不混入R98/R99，不冒新全量 |
| [R100几何坐标分隔符](issue-review-r100-geometric-coordinate-delimiters.md) | `0c7b86c3`后独立GeometryValue.cpp修复 | 旧232/104、230/135失败；当前232、PG/Root230、十个完整native/七个完整协议通过；不混入R98/R99待确认候选提交 |
| [R97 BEFORE行赋值的typed NEW/OLD](issue-review-r97-typed-before-row-assignments.md) | `20db4d58`后本工作区TableManage自有O2重编 | 旧106/65失败，当前154、15个完整native/十个完整协议通过；新1392未全量，不扩完整PG触发器兼容 |
| [R96 collation退出期专项暂缓](issue-review-r96-collation-exit-user-deferred.md) | `5091b39c`纯组件观察 | 用户明确跳过；原ASan失败保留，未修复，不重开本专项，也不扩为全部collation/类型暂缓 |
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
