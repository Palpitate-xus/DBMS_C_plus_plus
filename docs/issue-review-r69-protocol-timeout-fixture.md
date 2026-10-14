# R69：会话参数探针不能污染后续协议检查

原完整postgres_protocol_test在末尾role_rows断言得到空列表。最初额外门禁
r67-verified-full-pg-logs/postgres_protocol_test.log失败保留；Root原驱动独立
复跑也在同位置失败，root-r67-full-pg-logs/postgres_protocol_test.log。

只观察原simple_query响应，不改SQL、默认socket/startup/shutdown超时、返回值
或任何断言，分别完整运行修复前的已验收产物和R67候选。两者最后一次读取
都得到57014 canceling statement due to statement timeout，并非成功查询零行。
完整原响应见r67-prior-full-protocol-observation.log及
r67-current-full-protocol-observation.log；wrapper脚本同目录保留。
修复前产物SHA256：88cd9b86fa2b847a6679d90af5830b2488f4f3505f99cb7983dfea1ba97905a6。

驱动前面验证role连接statement_timeout=456及另一个连接仍为0，却把456ms
遗留给后续不相关的数据/权限检查。这些检查没有456ms性能要求；受并行构建/
回归负载影响可被真正的计时器取消。不能把这个空列表当成权限过滤或丢数据。

修改仅在夹具结束参数探针、进行后续数据检查前，RESET此连接的statement_timeout，
并新增命令成功、Ready I和参数确实恢复原默认0的断言。保留原456/peer0、
全部数据/权限语句与断言，不更改产品、认证/权限实现、全局超时或驱动超时。
静态AST核对原974条断言全部原样保留，新977条，移除/改写0条。
这不是取消真实超时测试；原完整statement_timeout协议和CLI驱动在同一候选
全部0，r69-cancellation-protocols.log及逐项日志，前后生产输入/binary/58份
receipt一致。TLS stub环境不能算作TLS runtime验收。

修复后的完整原协议驱动已实际退出0，r69-timeout-cleanup-full-pg.log及逐项日志；
完整驱动执行完成，源码AST共977条断言（含helper/分支，不代表逐分支覆盖），
前后生产输入/binary/58份receipt一致。这是候选全驱动
验收，主分支仍需单独复验。所有本机证据位于`/tmp/dbms-review-r46.ifu6SWLp/`，
临时路径不是发行产物。保留上述真实失败，不将其改写成历史全绿。

## 首轮修正后的补充复查

首轮候选只在最后一次role读取前RESET：5177b3b4/Root a3b3b4b9。候选完整通过，
但Root完整重验更早在application_name扩展协议检查收到57014，说明只清理456ms
不足。原记录root-r69-full-pg-logs/postgres_protocol_test.log保留。启动options
的321ms此前也只用于参数验证，却继续约束无关后续帧检查；另一个连接的123ms
同样需要在其跨连接隔离探针完成后清理。

补充修正将321ms的会话在其配置断言之后显式SET statement_timeout=0；在
123/456/peer0跨连接隔离断言全部完成后，将相应两个会话也显式SET为0，新增
成功/Ready/有效值断言，不改产品配置、权限断言或socket/startup/shutdown超时。
从R67原驱动对比，974原断言全部保留，新980条，移除/改写0条，见
r69-all-probe-original-assertions.log。先前977是第一轮的真实数量，不能改写。

Root并行负载下独立statement_timeout驱动也曾在锁释放后的读取收到57014，
root-r69-cancellation-protocol-logs/保留；批次随之停止，CLI并未运行，不记作两项
通过。其原断言及真实超时阈值均不修改。补充验收按完整PG、真实取消协议、CLI
串行运行，避免并发重编/重回归引入额外时延。补充候选三个完整原驱动实际
全部0，r69-all-probes-serial.log及逐项日志，前后生产输入/binary及58份receipt
一致。后续还需主分支同样串行验收，不将此前Root失败改为通过，也不宣称
所有负载下的产品时序问题已消失。
