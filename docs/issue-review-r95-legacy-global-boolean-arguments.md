# R95：旧全局布尔聚合表达式与混合JSON聚合

状态：本范围已修复；核心118、PG/Root60、十个完整native及十个完整协议通过。
原setup超时/C warning保留，原273未完成，新1391尚未取得全量终态。

## 问题与独立复现

R94输入`ccaf0ac5`上，独立60控制的PostgreSQL 18.6（180006）参考完整0。
普通DISTINCT布尔聚合、GROUP BY与混合json_agg的组合都应保留BOOLEAN结果。
例如四行中的bool_and(DISTINCT b)为f、bool_or为t，混合json_agg不能将它们
改成NULL。JSON列OID为114，无aggregate ORDER BY时只比较整数多重集，不要求
未承诺的输入顺序或JSON空白；BOOL值、NULL位、行与所有OID仍严格比较。

第一次无插桩Root运行r95-current-boolean-distinct-wire.log实际1，在setup循环
等待响应超时，尚未执行任何60控制。未在发送前打印SQL，不能猜测是CREATE TEMP
还是随后INSERT超时；原15秒期限及失败保留。这不是DISTINCT错值的原始反例。

预定的一次完整client日志观察r95-boolean-distinct-observed-once-client.log实际1：
CREATE约110ms、INSERT约44ms，全部60控制实际执行，15个混合JSON的全局BOOL
结果错误为NULL，普通30与混合分组15通过。观察只记录SQL/耗时；没有preload、
信号、重试或改期限，未出现本次Timeout，也未读取线程wait channel。
日志会扰动时序，该轮不作为延迟验收或原超时修复。它也不是反复重试到绿，
整轮仍因真实15项结果失败而退出1。

进一步用无插桩原生API独立复现：r95-old-legacy-native.log完整118控制/56失败、
实际1。不存在协议JSON格式或client日志才能触发的前提。两种失败及观察前后
输入/binary/全部58 receipts/57 test库字节独立核对均0，binary仍为R94的
933b81aca0e482ddbe2853ecf1e4082645603cb3c13abe5f9166b74572352650。
旧native收尾另有C collation目录warning，未定位，不把它猜成56个断言失败根因。

## 调用链与修改方法

普通SQL常先投影参数或走物理聚合，json_agg不在Main的Volcano aggregate集合内，
混合查询进入旧StorageEngine::aggregate。该函数仍有未完成的布尔表达式分支：
只创建getValB而不赋值val，空值被跳过，boolSeen始终false，最终输出NULL。
这与R93已修的groupAggregate是不同入口；不改写R93已验收的有限范围。

旧全局的非列名BOOL参数现复用R93共享BooleanArgument，按实际schema、值/NULL位、
collation及engine/database求值，FILTER先于逐行求值；结果确定也不提前停止。
BOOLEAN折叠本身幂等，但DISTINCT参数仍逐行求值，不能在求值前去重而丢掉volatile
副作用。保留原正常裸列、算术、JSON/集合、行读取及序列化分支；新错误路径释放
本层共享锁后传播结构化SQLSTATE，不把错误吞成NULL。不改provider生命周期。

## 永久回归及当前验证

- legacy_boolean_aggregate_argument_test.cpp：118控制，3函数、8种裸列/比较/
  compact/CAST/COALESCE/DISTINCT参数、四种WHERE/FILTER/全NULL/零输入范围；
  22012/22P02、FILTER先于错误、错误后锁释放；volatile参数全4次/过滤后2次，
  独立扫描副作用行与NULL位、另一engine不可见、实际事务owner和rollback。
- boolean_aggregate_distinct_protocol_e2e_test.py：上述60控制，普通/混合JSON、
  全局/分组、三函数、NULL与FILTER，保留原全部SQL/断言。外部夹具发布到tests
  改用仓库相对定位并补发送前SQL日志，未改变SQL/断言/控制数量；永久路径的PG
  参考另行复验。

r95-build.log实际0：标准O2只重编TableManage.cpp、链接及重复构建；公共头
未改，其余自有对象receipt匹配，不称全58冷编译。当前binary为
`73272c6b40888d127417f329e787837248382c936445abdbdd0d5b2c05304128`，生产输入
`ee134ce09405aaba982f45a7b56fcff889efcde16d98b220c12e1085ff2ad20e`。
r95-test-cache-preparation.log只复制同一Root的57份库并fresh stubs，独立fence
核对全部58 receipts/57 test库字节、双stamp、binary及R93/R94其它生产组件0。

r95-core-native.log实际0、118/0失败：结果、真实错误码、FILTER-before-error、
错误后锁释放、全4次/过滤2次volatile副作用、实际engine owner/私有NULL位和
rollback全部通过。旧C warning仍存在，不宣称修复或没有warning。
r95-core-wire.log实际0、完整60全通过，原15个全局混合JSON错值均修复；末次
包含发送前日志的相同永久源码在PG参考也完整0。补日志方便今后辨认超时SQL，
不将本次0叫作原setup超时修复。输入/binary/全部58 receipts首尾核对0。
r95-neighbor-native.log另外九个不同完整入口全部0，含原布尔/集合/DISTINCT/
FILTER、表达式参数/作用域/根owner、完整68上下文以及R92缺失TOAST计数器及合法
重写。与新118入口合计十个完整native，通过不代表无C warning或全suite完成。
r95-protocol-neighbors.log另外九个不同完整协议全部0，含原完整152、SHOW40、
参数/作用域、DISTINCT/FILTER、prepared aggregate、pg_settings及原完整
PostgreSQL协议；与新60入口合计十个不同完整协议0，输入/binary/全部58
receipts首尾一致。默认协议只证明plaintext协商、Simple/Extended/错误恢复
控制通过，不是TLS。不用该十个0抹掉原setup超时或冒充全1391验收。
519份Markdown/946本地内联目标、527 Python AST、13 shell语法、881 native、
507唯一注册/489 standalone、文档/契约/版本和六个总账单测均通过；
require-complete实际1。新1391组合尚未取得全量终态。
全部临时日志与辅助脚本位于`/tmp/dbms-review-r46.ifu6SWLp/`。

## 未完成范围

原setup超时仍需精确失败SQL/线程证据，不改期限、不用观察通过抹掉失败；
其它普通DDL超时、完整suite失败、C warning及原未完要求仍保留。
不自行恢复R15/R51/R65、安全/TDE、CTAS/后台checkpoint等暂缓专项。
本项不是完整聚合/绑定/类型或原273完成；仍22 complete/166 partial/70 unverified/
15 deferred。不push、Actions禁用、README保持长期入口。
