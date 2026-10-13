# R30：扩展协议元数据分析的会话上下文

## 问题与范围

审查基准 `b54cc7f0` 的产品源码中，普通 Simple Query 在 Main 的会话守卫内执行，
但 Parse/Bind/Describe 的元数据分析在该守卫外运行。关系、类型和函数查找读取
`currentSession()`；缺少该上下文时，关系与函数查找退回 `public`，也遗漏真正的
session-temp namespace。合法查询因此出现与实际连接 search_path 不符的错误。

| 首次扩展查询，非 public schema 或 TEMP 来源 | 基准响应 | 正确描述 |
| --- | --- | --- |
| `sum(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END)` | Parse 42P01，称 inputs 不存在 | int8 |
| `sum(CASE WHEN i>0 THEN writer(i) ELSE 0 END)`，writer 返回 integer | Describe 42804，称 CASE 混合 integer/text | int8 |
| `SELECT (SELECT id FROM source LIMIT 1)`，source 是 TEMP | Parse 42P01 | integer |
| `SELECT id FROM source UNION ALL SELECT wide FROM source`，wide 是 bigint | Parse 42P01 | bigint |

原错误回滚路径还会设置会话指针却不恢复，使失败后的相同查询意外成功。
这不是 RowDescription 断言错误，也不能用重试掩盖。

## 修改

在 `NetworkServer.cpp` 内添加不可复制的 `ProtocolSessionScope`：每轮协议处理使用
当前连接的 Session，成功、continue、退出和异常展开均恢复先前指针。作用域覆盖
Parse/Bind/Describe、快照、Sync 和超时处理，不跨连接或后端租借保存错误的会话。
错误回滚也使用局部守卫，不能再给后续消息留下隐式初始化效果。

Main 现有的 `CurrentSessionScope` 保持不变；实际执行或嵌套调用仍可临时安装自己的
会话并恢复协议调用者。没有通过执行函数、读取源行或获取结果来推断元数据。
没有修改任何原失败驱动的 SQL、顺序、预期或断言。

新增注册驱动 `tests/protocol_metadata_session_scope_e2e_test.py`。四类查询分别使用
全新实例/连接，检查首次、正常重复、错误恢复后三个边界，共 12 个控制；同时检查
statement/portal Describe 的名称、OID、无数据行/完成标签，以及 writer sequence
没有被元数据操作推进。

## 验证

冻结基准 Main 的 SHA-256：
`0a24168fe54d61b1382448b4df58a983a978ad1c12733c37c959930ef1b5c80a`。
新增驱动在该基准实际 exit 1：四类首次查询全部失败，相同查询在错误后重复成功。
同一驱动在真实 PostgreSQL 18.6 上 exit 0，12 个控制完整通过。

候选 Main 的 SHA-256：
`d9d0bc3c624db08d464dfaaed68a242d270f2be9628e8776aa384abecc91585a`。
新增驱动在候选上 exit 0，全部 12 个控制通过。

以下原有完整驱动在候选上均实际 exit 0，未缩减测试：

- `enum_aggregate_argument_binding_protocol_e2e_test.py`
- `aggregate_result_root_owner_protocol_e2e_test.py`
- `temp_prepared_access_protocol_e2e_test.py`
- `set_operation_type_metadata_known_gap.py`
- `set_operation_clause_ownership_protocol_e2e_test.py`
- `extended_protocol_error_abort_e2e_test.py`
- `search_path_transaction_protocol_e2e_test.py`
- `integer_between_parse_protocol_e2e_test.py`（312 个控制）
- `postgres_protocol_test.py`（完整启动、Simple/Extended、错误恢复等协议回归）

命令为 `DBMS_MAIN=<binary> DBMS_COMPATIBILITY_MODE=postgresql18 python3 tests/<driver>`；
参考端使用新增驱动的 `--reference18`，每组随机 schema 位于最终回滚的独立事务中。
本机完整日志与构建核验记录保存在 `/tmp/dbms-review-r30-evidence.kIHW0dni/`，
上述关键失败、候选结果和复现驱动均不依赖该临时目录才能解释或重跑。

构建使用私有 worktree：复用前核对 58 个生产 TU、所有头文件、manifest、编译器和
flags，以及完整生产签名和每个对象签名；私有复制后逐对象比较字节。路径相关签名
仅在完整匹配后重算。标准构建随后只重编 `NetworkServer.cpp` 并重新链接，没有写入
主工作区缓存或二进制。此结果不是全套回归、所有连接池模式或 sanitizer 验收声明。
