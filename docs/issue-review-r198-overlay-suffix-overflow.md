# R198：OVERLAY 的后缀位置必须保留 int4 溢出检查

## 问题与修改方法

`OVERLAY` 的起点和替换长度均使用 int4，但旧回调把二者相加后只按输入长度截断。
即使两个参数各自合法，只要和超出 int4，也会返回成功结果而非 22003。例如：

```sql
SELECT overlay('abc' placing 'X' from 2147483647 for 1);
```

旧版返回 `abcX`；实际 PostgreSQL 18.6 返回 `22003 integer out of range`。
TEXT、BYTEA 以及省略 FOR、从替换串长度计算后缀的路径共用这一缺陷。

修复只在现有宽整数加法后检查 int4 上下界，再进入原后缀截取逻辑。
仍使用宽整数完成加法，不引入 C++ 有符号溢出；不改变函数解析、声明、求值次数或公共布局。
起点小于 1 仍优先 22011；负长度依旧允许，NULL 仍按严格函数规则返回 NULL。
合法边界和空替换串不得被误拒绝，TEXT 默认长度按字符、BYTEA 按字节计算的原规则保留。

## 回归范围

| 检查 | 修复前 | 修复后 |
| --- | --- | --- |
| 新完整协议夹具，每套程序 94 项 | 两套均完整执行，24 项不符、退出 1 | 两套均 94 项通过 |
| 原 OVERLAY 原生夹具，保留原断言并增加 12 项 | 两套均在首个新增错误断言中止 | 两套均完整通过 |
| 私有 PostgreSQL 18.6 参考 | 同一完整 94 项通过 | 不需要修改参考期望 |

24 项差异是同一根因在 Simple/Extended、TEXT/BYTEA、显式/默认长度等路径的重复覆盖，
不是 24 个独立 bug。原生旧红只执行到第一个新断言，不冒称执行了其余新增断言。
新协议夹具保留确切结果、OID、SQLSTATE、错误后的 ReadyForQuery I 和成功命令标记；
涵盖 INT_MAX 临界和、INT_MIN 负长度、空源/替换串、NULL、UTF-8，并已注册到正常测试入口。

每套程序完成 5 个完整原生入口和 8 个完整协议入口，共 26 个实际进程：

- 原生：OVERLAY、string_functions、substring_projection_semantics、trim_projection_semantics、utf8_split_linear。
- 协议：新 OVERLAY 94、utf8_split_linear 329、string_length_signature 654、BYTEA 声明 28、
  computed_query_host_metadata 331、prepared_global_aggregate 67、pg_class_query 和 integer_arithmetic_width。

两个生产表达式单元分别 fresh 编译、链接；工作区实际正常构建只编译此 1 个 CPP，
随后正常重复构建零编译、零链接。其余每侧 57 个生产对象、stub、127 个公共头、
编译器与参数逐字节核对后复用；不是本轮 fresh 编译全部生产代码或 Main。
每侧 5 个原生测试单元另行 fresh 编译，全部链接本侧匹配生产库并使用私有测试目录。

首次驱动因备份程序未复制执行位、原生与协议日志名称相撞而暂停；修正驱动后，
已完成步骤按原日志和对象核验复用，其余检查继续执行。原失败日志保留，
没有修改夹具断言、放宽超时或关闭持久化设置。

## 证据与未完成边界

- 所有本项构建、旧红、新绿和逐命令 receipt 均在 `/tmp/dbms-r198-overlay.5bHXDhne/`。
- 旧版本完整对照：`completed-baseline-receipt.json`，SHA256 `b14cadfc018f1a663c91ecb6fe1d8ab8e9a5f855946ac01b4c98343bb0c7c834`。
- 私有 PG18 参考：`/tmp/dbms-private-pg18-full-fixture._596uwc3/completed-reference-receipt.json`，
  SHA256 `3a89a164d7e1c8003a55483fa005ab92c8a7adba6479b4d55f4a10ede82d7273`；正常停止实例，默认 fsync 保留。
- 最终候选：`completed-candidate-receipt.json`，SHA256 `c88737f6cad9ffa9ac13c47867f7bfcc4c3c52ff6acc865037ed8569679fd157`。
- 提交核验：`completed-commit-receipt.json`；提交后文档、注册和来源核对：`completed-postcommit-receipt.json`。

这些 `/tmp` 路径是本机证据，不是可移植发布包。本项在已提交的 R196 基础上独立组合，
不纳入其他工作区候选；实际工作区程序包含未接受改动，其通过不表示接受它们。
用户最新跳过的 R197 COPY 原样保留、未提交、不再继续该专项；默认协议 search_path、
R185 和其他已暂缓项也不恢复。所有提交仅本地保存，不 push，Actions 保持禁用。

本项不是全部字符串功能、TLS、sanitizer、修复后完整 1492 或总 273 项验收。
冻结全量的 1481 PASS / 11 FAIL 原结果不改写；原 273 项状态仍为 22 complete、
166 partial、70 unverified、15 deferred，总清单未完成。
