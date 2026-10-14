# R56：ARRAY_AGG 窗口的声明身份与真实值接收器

## 问题与修改方法

冻结完整window_type在已有ARRAY_AGG窗口上报42883。实际PostgreSQL18.6的原
32控制全0，未修正候选27失败；扩展42参考全0、未修正主分支37失败。

第一层问题是纯元数据边界只承认SUM/COUNT等窗口聚合，没有ARRAY_AGG的真实
标量输入→数组输出声明。补上该边界后，原完整window_type通过，但32中仍有8
个值/类型失败：pg_catalog限定名、表达式参数和FILTER落入legacy的错误接收器，
出现空字符串/TEXT描述符，而不是正确的数组。

最终修复：

- 在真实query-host边界解析ARRAY_AGG标量签名和数组结果类型，拒绝歧义UNKNOWN、
  星号和不支持的DISTINCT OVER，不通过执行输入来Describe。
- 窗口调用从实际AST取得规范routine名称，ARRAY_AGG归入聚合角色；集合参数走
  prepared value receiver，保留原有NULL、声明、类型转换和输出owner。
- FILTER在逐frame成员的参数求值之前执行；空frame/全过滤得到SQL NULL，含一个
  NULL成员则得到非NULL的`{NULL}`，不混淆两者。
- 聚合状态保留真实成员，用既有SQL数组编码器表示空串、文本NULL、引号和空白；
  不从显示行拆词，也不把NULL成员丢掉。BIT/NUMERIC转换的成员精度保留，数组
  输出typmod仍为-1。移动frame/EXCLUDE复用原方向边界与实际求值需求。

本项仅覆盖标量输入的ARRAY_AGG窗口。数组值输入需要额外维度、shape及NULL数组
检查，当前明确报0A000，不能把数组字面量当普通字符串收集并返回错误数据。
这仍是兼容性缺口，不宣称完整ARRAY_AGG类型族完成。

## 验证

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 实际结果 |
| --- | --- |
| 最终45条实际PG18.6控制 | 全0，r56-reference45.log |
| 原42条未修正主分支控制 | 37失败，r56-root-original42.log |
| 第一metadata候选32 | 原window_type通过，但8条值/类型失败，r56-first-metadata32-candidate.log |
| 最终45条与原完整window_type | 全0，r56-second45-candidate.log、r56-second-original-window-type.log及最终批次 |
| 新native纯描述符控制 | 33条全0，包含空来源、NULL、9种输入/表达式、结果OID及签名/未实现边界 |
| 完整native门禁 | 七个完整入口全0，r56-final-native.log；当前58份生产receipts及结束输入一致 |
| 完整协议门禁 | 六个完整入口全0，r56-final-protocols.log；含原300窗口参数、307 frame、117 ORDER别名及152偏移控制 |
| 标准生产构建 | 第一轮TableManage、第二轮Main真实O2重编/链接0，r56-first/second-standard-build.log；没有公共头变化，不冒另一轮fresh58 |

控制保留INTEGER/BIGINT/TEXT/NUMERIC/BOOLEAN/BIT声明、真正NULL与文本NULL、空串、
空输入/LIMIT0、partition、滑动/空frame、EXCLUDE、FILTER、named window、限定
routine及结果OID/typmod。原无条件失败与中间8失败不覆盖。

`r56-first32-candidate.log`启动在第一轮构建结束之前，实际仍使用上一代二进制，
不能当成新源失败或验收；构建真正终态后另存上述metadata候选及最终日志。
用户暂缓的R51、R15、CTAS/后台checkpoint专项与安全/TDE工作均不重开。
主分支独立集成和下一轮全注册回归仍须另行验证，原273总账不虚改完成。
