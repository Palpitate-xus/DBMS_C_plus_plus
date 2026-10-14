# R78：维护恢复轮询对真实publication冲突的预期

状态：夹具修正已验收，完整DIV14及相邻门禁通过。

R76修复真实server abort，R77修复staging前过早catalog eviction；原样DIV14
仍在恢复真正publication时得到55P03。此时实际管理员目录不可读，必须保留
冷读保护，不能为测试绕过管理员检查或沿用另一个权限结论。

只给刚提交的RESTORE任务轮询显式开启临时冲突重试：精确要求SQLSTATE及
`cold catalog read conflicts with an active DDL owner`消息，连接必须返回idle，
不得混有CommandComplete/数据/行描述，且原有deadline绝不重置。其他任务和
所有其他错误保持原立即失败行为。每次成功查询的12个字段OID、任务ID、最终
succeeded及恢复后行内容断言均未删除；原30秒总期限未放宽。不修改产品权限。

新增maintenance_restore_poll_test使用实际wire RowDescription/DataRow编码
测试五组边界：同一job重试后严格成功、非restore/其他错误拒绝、Ready状态及
假成功拒绝、不延长deadline、成功元数据仍严格核对。它只是夹具单测，不代替
真实server集成。真实restore-stage连接、数据和GUC仍由R76完整入口覆盖。
使用手册补充该并发窗口的客户端处理方法；不能将暂时冲突视为任务成功。

原失败日志保留在`/tmp/dbms-review-r46.ifu6SWLp/r76-original-protocol-logs/`
及`r77-final-protocol-logs/`，不改写历史全量的20个FAILED。

`r78-final-protocols.log`三个完整入口均0：新增五组单测、完整DIV14、新增真实
restore-stage协议。结束时生产输入、binary及58份对象receipt保持一致。
DIV14的全部既有功能门禁和恢复后的数据断言保留，没有产品权限修改或超时放宽。
静态499 Markdown/798本地目标、519 Python AST、13 shell、499注册入口检查0。
这是定向验收，不是新1360入口完整终态；后者单独冻结已提交输入运行。
