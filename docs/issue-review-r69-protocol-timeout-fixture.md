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
所有977条断言执行完成，前后生产输入/binary/58份receipt一致。这是候选全驱动
验收，主分支仍需单独复验。所有本机证据位于`/tmp/dbms-review-r46.ifu6SWLp/`，
临时路径不是发行产物。保留上述真实失败，不将其改写成历史全绿。
