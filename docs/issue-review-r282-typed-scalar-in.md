# R282：标量 IN / NOT IN 查询（按用户要求暂缓）

状态：2026-10-27 按最新 `skip it` 停止当前项。候选仍未通过完整验收，
生产代码和测试保持未提交；本次仅提交跳过记录，不计作修复。
普通 `continue` 不自动恢复 R282。依赖它的 R281 也保留为未完成候选，
不得绕过本项失败或合并提交；其余独立问题可继续处理。

## 问题与待验收方法

已观察到标量 IN / NOT IN 的查询右值在部分入口落入旧文本/列表路径，
SELECT、WITH、VALUES、NULL、空集和错误传播的行为不一致。
候选让普通解析器和绑定解析器共用类型化 `= ANY` 表达式；NOT IN 包裹
同一表达式的 NOT，行构造器查询保持原角色。绑定器识别 VALUES 查询，
并在发布结果描述前校验真正 UNKNOWN 的比较输入；协议候选补充不执行
查询行的描述分析。未修改公共头、Main、磁盘格式，也不宣称完整 SQL 兼容。

上述均为未接受的候选方法。最后一条失败为 Extended 查询
`SELECT 1 IN(SELECT 1/0)`：SQLSTATE 22012 已正确，但仍提前发布布尔列
RowDescription，实际收到 OID 16；参考端同阶段不发布该描述。
完整时机差异尚未解决，不以 native 通过代替协议验收。

## 实际结果与未运行范围

| 固定范围 | 实际结果 | 边界 |
|---|---|---|
| PostgreSQL 18.6，70 个基础查询和 8 个 sequence 副作用案例 | 890 控制、0 失败 | Simple/Extended、结果、NULL、SQLSTATE、OID、调用次数 |
| 旧基线，两侧同一最终 native 夹具 | 各 172 控制、156 失败；API 8 控制、7 失败 | 最终普通解析器 IR 控制已在旧基线重新执行 |
| 旧基线，同一协议夹具 | 两侧各 890 控制、136 失败 | helper SHA 相同；最终 native 调整没有改 SQL helper |
| V5 候选 native | 两侧各 172 控制、0 失败；API 各 8 控制、0 失败 | 不等于全部功能已修复 |
| V5 候选 accepted 协议 | 890 控制、1 失败，进程 exit 1 | Root 协议未执行，无完整 primary 通过 receipt |

最早参考夹具漏发 Portal Describe，导致缺失 RowDescription 的 70 个
断言失败；只补发 Describe 后固定最终 890 控制，保留原行值、错误码和
OID 断言。此测试协议修正发生在产品候选运行前，旧 helper 和失败日志保留。
候选 V1/V2/V3 的失败及 V4 的编译失败也均保留；最后 V5 编译成功，
不覆盖前面的失败证据。没有删断言、增加超时或把失败项计作跳过通过。

规划中的邻接回归和 scoped UBSan 未运行，没有全库存或原 273 条全验收。
R281 两侧各 196 个 native 控制仍有 7 个失败，API 21 控制通过；
它尚无完整通过门槛，也没有修复提交。此记录没有把其候选判为完成。

## 保留的修改与后续隔离

R282 保留 `src/parser/parser.cpp`、`src/parser/query_binding.cpp`、
`src/network/NetworkServer.cpp`、`tests/typed_scalar_in_test.cpp`、
`tests/compat/typed_scalar_in.py` 和既有
`tests/quantified_query_demand_protocol_e2e_test.py` 的追加内容。
parser / NetworkServer 原已有其他未接受修改，不覆盖、不删除。
R281 的两个生产 CPP、两个新测试文件和既有协议驱动追加内容也全部保留。

后续独立修复须从已接受的 R280 生产基线构造投影，排除本项和 R281 的
未接受内容，同时保留更早的其他工作区修改；不得误用 V5 对象作为已接受
生产 donor。若将来明确恢复，先保留并重跑同一失败断言，再修改真正根因。

实验目录：`/tmp/dbms-r282-typed-scalar-in.MsEGpbj2`；
依赖候选：`/tmp/dbms-r281-plpgsql-scalar-query.hMUtercG`。
临时目录不是长期归档承诺，以下摘要与哈希记录在 Git 文档中。
已接受生产基线仍是 `b9744f1d1981e9d8e75d73e1ab09fc4ba23f1a7a`。
本次只增加文档；原暂存区和其他工作区文件保持原样。不 push，不启用 Actions。
原总清单仍未完成，所有更早的明确暂缓项保持原状态。

| R282 证据文件 | SHA-256 |
|---|---|
| `typed_scalar_in.py` | `b0c7441b3876260a7118d3a5c9a432838a855c19fe375474d8d661f9aafdceda` |
| `completed-reference-v2-receipt.json` | `6c901a4231019746e08792f926a9784934c8069e72ab02b48de03d1fab1d89e1` |
| `reference-v2-full.log` | `573bb7974d5c75082182615cf9f383a5ba3e4d0616764f6ee4ab49825ebbb9ff` |
| `completed-old-v3-receipt.json` | `9c58e714916baa1872e9c3b9b200e438b91ffe702c0995c11ebba17aee3a537f` |
| `completed-integration-v5-receipt.json` | `0cf5a5b04dffef5d837a240ea1046224bb39f364d6c140074e01f3ee3408fb99` |
| `completed-build-v5-receipt.json` | `c81793c5268f419b5659c04f6b1efefa4ba1a56121bf0a9f70dedfc93cdb1058` |
| `accepted-v5-primary-wire-failed-receipt.json` | `b22c06833e050a6e5e4f5c2982c7da0f805f1d7666f906d1be8466cb6f91c428` |
| `root-v5-primary-native.log` | `c644f4c60f64f34e99b3972e21d36bb67ef728ef0bf70d92b30ac8301e7b10ea` |
| `accepted-v5-primary-native.log` | `c644f4c60f64f34e99b3972e21d36bb67ef728ef0bf70d92b30ac8301e7b10ea` |
| `accepted-v5-primary-entire-wire.log` | `e613295138a54e5565cee4d96740f788f0b6b8ba04f40a2a8d107d1baf994c9f` |
| `root-parser-before.cpp` | `00d91745352697becb16dfa4c7dbc65d141a6a64e7c9f00f2bd1848f0fb706f6` |
| `root-v3-NetworkServer.cpp` | `c3567b6f225bbe330230c5781dcdb4b4f2b0faa52945ecce8afa6b80cd8e0d67` |

| R281 证据文件 | SHA-256 |
|---|---|
| `completed-old-receipt.json` | `fcbb959c89ef839b9099d8a7c7fde7a89ce00fe9bbde942f195822b388daec56` |
| `completed-integration-receipt.json` | `d0af535e9e2e193338328034130531a51fe2c15f8f22d5ef765cfd28d5335a1a` |
| `completed-initial-candidate-failure-receipt.json` | `21eb8a667a300c1a4d62bdb189c4a923df40aba293fb2131ee126cddc79fe636` |
