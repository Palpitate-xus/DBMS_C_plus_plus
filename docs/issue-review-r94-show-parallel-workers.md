# R94：并行worker配置的SHOW与协议metadata

状态：本范围已修复；PG18.6/Root40、原完整152、六个完整native与十个完整
协议入口全部通过，新1389组合未全量运行，原273未完成。

## 实际问题及输入

R93已独立提交`9fa6a39d`。该输入上的完整152布尔聚合协议中，150控制通过，
仅SHOW max_parallel_workers_per_gather的0/2两项返回XX000 Unknown SHOW command。
设置已由自有server的ALTER SYSTEM加真实pg_reload_conf应用，不是配置未生效。

独立40协议控制在同一Root binary
`30f85ee7f0369b65b6a5f9b333514cbedb9c361afdd28225738a4aae3ee4f3f2`
上完整实际1：r94-old-show-wire.log共33项不匹配。六个pg_settings值/NULL-unit
正向控制通过；普通SHOW、大小写/引号别名的Simple/Extended结果失败，Statement
Describe只有NoData、没有TEXT字段。显式块中首个SHOW失败也使后续请求25P02，
这些差异不是33个独立bug。r94-old-show-grammar.log完整10控制实际1、5失败：
三种大写/引号拼写未规范化，两个尾随junk被原parser接受。其余原SHOW正控通过。
两批前后输入/binary/全部58 receipts/57 test库字节独立核对0。

同一40控制在真实PostgreSQL 18.6（180006）完整0；最终永久路径的源码又单独
运行，日志r94-final-show-pg18-reference.log。外部旧夹具仅仓库定位绝对路径
不同，发布回tests时恢复相对定位，未改变SQL、预期、断言或控制数量。

## 修改方法

parseShow只为该有限配置名补齐大小写/引号规范化和完整尾部消费，复用原
transaction_isolation规则；不把SHOW TABLES/COLUMNS等其它项目语法一并改写。
Main读取与pg_settings相同的live g_config，发布单列非NULL TEXT、canonical
字段名、SHOW command tag；不再依赖无metadata的打印文本。Network的纯
Statement Describe发布相同TEXT形状，不执行SHOW或其它SQL获得metadata。

保留原参数范围、配置持久化及reload机制。Root仍把该设置视为process-global，
本项不增加普通session SET兼容性，不修改权限检查、auth/ACL或provider生命周期。

## 永久回归与当前验收

- show_parallel_workers_test.cpp：10个parser控制，普通/大写/引号/嵌套comment、
  尾随junk，以及timezone和transaction isolation的原正向语法。
- show_parallel_workers_protocol_e2e_test.py：40控制，实际配置0/2/7、Simple/
  Extended执行、Statement Describe、pg_settings值及NULL unit、显式块Ready=T，
  以及两种完整错误语法在两条协议路径均42601。PG参考用SET；Root只修改自有
  临时server配置并检查真实reload的BOOLEAN t，不更改用户持久库。

r94-build.log标准O2实际重编Main/parser/Network三个单元、链接及重复构建0，
公共头未变，其余自有对象全部receipt匹配；不称全58冷编译。当前binary为
`933b81aca0e482ddbe2853ecf1e4082645603cb3c13abe5f9166b74572352650`，生产输入
`ad3750317df278fcf6c6451f1aab70c358b76eebc9e70e42386c60e89f049b04`。
r94-test-cache-preparation.log复制同一Root的57份库并fresh stubs；独立fence
核对全部58 receipts/57 test库字节、双stamp、binary和R93未变生产组件均0。

r94-show-core-wire.log实际0：原40控制在当前Root全部通过、输入/binary/58
receipts首尾一致。保留原33项差异、相同40断言及15秒期限，不靠删断言或重试
到绿。r94-native.log六个完整入口全部0：新10语法控制、原SHOW隔离级别、
Config严格校验/回滚/持久化、布尔分组，以及R93完整408+68矩阵。新的同源
driver/stubs与当前Root库重链执行，未复用首版候选程序；全部58 receipts/输入
一致，后置独立fence0。新880 native+3 Main+506 Python=1389组合未全量运行，
原完整152已实际0，包括原SHOW 0/2两项；原SHOW隔离级别、pg_settings、
FILTER、聚合参数、prepared aggregate、EXPLAIN布尔及事务隔离邻居也已通过。
r94-protocol-neighbors.log九个完整入口实际0，含原完整PostgreSQL协议；连同
单独SHOW专项共十个不同完整协议0，未把控制数当作驱动数。两个batch的输入/
binary/全部58 receipts首尾一致。默认协议只证明plaintext协商及原Simple/
Extended/错误恢复控制通过，不是TLS。R92事务隔离入口本次0不证明旧DDL超时
根因修复，原失败保留。518份Markdown/939本地内联目标、526 Python AST、
13 shell语法、880 native、506唯一注册/488 standalone、文档/契约/版本和六个
总账单测通过；require-complete实际1。所有本机日志/辅助脚本位于
`/tmp/dbms-review-r46.ifu6SWLp/`。

## 未完成边界

R93提交时的150/152实际1保留，不因后续修复改写历史；旧全量的DROP与普通DDL
超时也不因SHOW修复而关闭。原273仍22 complete/166 partial/70 unverified/
15 deferred，暂缓专项不重开；不push、Actions保持禁用、README保持长期入口。
