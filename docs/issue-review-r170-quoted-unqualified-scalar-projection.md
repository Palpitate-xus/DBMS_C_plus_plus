# R170：quoted 未限定标量函数的表投影身份

状态：有限修复已验收，按独立本地提交纳入。五项联合最终源码在工作区及独立干净版本各完整70入口通过；不代表全部数据库问题完成。

## 已复现的问题

实际接受基线为 `073e47367e18163d5aa1d1707ad1c6253b303614`。
在有真实行的表上，`SELECT "degrees"(x) FROM t` 等合法 quoted 标量调用
会被旧执行路径当成包含 SQL 双引号的原生函数名，再次引用后报 `42883`。
未限定标识符中的大小写、字面点号和双引号是身份的一部分，不能简单删引号或小写化。
空表、WHERE false、LIMIT 0 也必须进行必要的名称/签名分析，不能依靠没有到达行执行来隐藏错误。

完整发现夹具 V2 固定 791 个操作，在 PG18.6/180006 上全部通过；
冻结 accepted-only V4 产品完整执行 791 个操作、343 项断言失败，实际外层退出分别为 0/1。
343 包含重复输入、格式组合及错误状态的后继影响，不代表 343 个独立根因。
其中 Parse/Describe 的独立所有者记录在[R171](issue-review-r171-quoted-scalar-protocol-analysis.md)，
普通未引用 abs 参数引用错误另记[R172](issue-review-r172-qualified-column-scalar-argument.md)。

## 根因与修改方法

修复前[Main](../src/main.cpp)的完整 AST 入口只为真实限定 callee 扩展表投影；
旧文本入口将 SQL quoted callee 原片段拷入 `SelectExpr.funcName`。
原生 API 的该字段本来就是已解码身份，[TableManage](../src/commands/TableManage.cpp)
据此再引用一次是正确的，不能为修 SQL 入口而破坏原生 API。

有限候选仅将真实 SQL 源范围内的“一个 quoted identifier 后紧接左括号”
交给既有完整 AST 绑定/执行，保留原 FROM-less 入口以及 aggregate/window/SRF 的所有者。
字面点号和 doubled quotes 保持单个标识符；不全局 strip、lowercase、拆点号或重写 native callee。
需验证嵌套、别名、shadowing、缺失名称、空输入、保留绑定、binary 格式和 volatile 函数只执行一次。

## 固定证据与验证计划

V2 草稿：`/tmp/dbms-r170-quoted-scalar-v2.UQ1T1QsP`。
原始日志位于 `/tmp/dbms-review-r46.ifu6SWLp/`：

- `r170-v2-reference-full791-protocol.log`，791/0，实际退出 0。
- `r170-v2-old-accepted-clean-full791-protocol.log`，791/343，实际退出 1。

旧 binary SHA256 为 `2daeb9653b5844be53f08e6841e58615b1011bb4ed3a615470faf109f8e9e9ad`。
V3 只改两行 docstring，反向还原后与 V2 逐字节一致，保留全部 SQL、断言、791 数量和顺序。
V3 位于 `/tmp/dbms-r170-quoted-scalar-v3.dTmZKtbg`，
夹具 SHA256 为 `d3a9e71c179912534409700f9542901350a484b87022580fe9ddc1053fd8576f`；
V3 也已严格串行实际完成参考 791/0、旧 accepted-only 产品 791/343，外层退出分别为 0/1；
日志为同证据目录的 `r170-v3-reference-full791-protocol.log` 和
`r170-v3-old-accepted-clean-full791-protocol-retry1.log`，两次进程均已收取消费终态。
这是 V3 本身的完整运行，不以 V2 日志替代；仍不是新候选产品通过。
首次 V3 产品包装器误用不存在的 binary 路径，启动前失败、0 控制/退出 1；
原日志保留为环境错误，不计 SQL 失败，也不修改断言掩盖。

Main 修改后必须在 Root 和新 accepted-base-only clean 各新编译三个真实 Main 测试 TU；
旧 Main 对象不得作为本轮 fresh 证据。共享头未改时可逐一证明其他生产对象来源后复用。
完整 791 及原 58 相邻入口、两个既有原生参数契约检查计划保留，不以编译成功替代运行。
不恢复用户暂缓模块；原 273 项状态不变，不 push，Actions 保持禁用。

## 联合候选的历史失败与最终验收

最初三项组合 V1 clean 实际完整791仍有26项失败：8项另属
[R174描述符](issue-review-r174-ordinary-source-descriptor.md)，18项另属
[R175聚合执行](issue-review-r175-ordinary-aggregate-identity.md)。V1 的完整314及28native+
3真实Main各已通过；未执行的完整62不冒称通过，所有旧失败日志继续保存。
最终五项组合 V2 的两端编译实际0，Root48条/clean49条命令均0：每端fresh2生产TU、
核对复用56生产TU、fresh6/复用28原生TU、复用stub、三个真正fresh Main TU并重新链接当前库。
每端96对象/38程序，源及产物清单复核0。Root首轮70个入口实际退出1：
34native、三个真实Main及791/314/331/906均0，合计69个入口通过；唯一未通过的是
既有R164完整956夹具的finally清理阶段。它已记录834次match True、零match False，
随后等待下一条DROP FUNCTION响应发生15秒socket超时，无956完成footer。
实际原因尚未确定，不把I/O长尾假设写成结论，也不把这次退出1算作通过。
原始失败日志`r170-r175-v2-root-runtime-results/custom_scalar_routine_declaration_protocol_e2e_test.log`
及整轮`r170-r175-v2-root-runtime.log`完整保留，后续原样串行复测，不改断言、timeout或持久化设置。
随后独立clean的同一完整956已实际0、footer956/0；既有失败位置也已通过。
clean期间只读线程采样多次观察到`jbd2_log_wait_commit`，但不能倒推Root首轮超时原因。
独立clean完整70已实际全0，34native+33wire+3真实Main及源/58生产对象/server/96对象/38程序
均经独立复核0。随后Root保持同一源及产物，原样完整70的retry1也实际退出0：
34native+33wire+3真实Main全部通过，完整791/314/331/906及原956均为零失败。
首轮失败日志不覆盖；复测成功不代表原超时原因已查明或修复。
最终独立复核两端完整源字节及src/tests/scripts路径集合、58生产对象、server、
96对象/38程序与冻结receipt精确映射全部0；Root还复核58缓存stamp。
clean使用显式fresh2+核对复用56，不宣称运行正常构建包装器/repeat或新生成58缓存stamp。

最终证据为同目录`r170-r175-v2-final-two-sided-runtime-evidence.json`。
Root通过日志`r170-r175-v2-root-retry1-runtime.log` SHA256
`8f81ad973b17690dadb6fbf26bfafeea4ffca48fcbd485bdc8c16bd29e1aa972`；
clean通过日志`r170-r175-v2-clean-runtime.log` SHA256
`075ae568dde142c80cd017acf6e3938080e20c3dde41a100af145b71d944a00f`。
本项只接受上述有限SQL身份修复；两端旧源/产物保持冻结。
这是五项最终组合的定向70入口，不是1482注册入口全量、TLS/整库sanitizer，
也不是每个中间提交独立完整运行。R173依赖和R176真实聚合排序仍独立开放，原273不提升。
