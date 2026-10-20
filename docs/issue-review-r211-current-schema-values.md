# R211：current_schema() 快捷路径返回实际模式

状态：本项实际返回值修复已验收；原总清单仍未全部完成。

## 问题与修改方法

`main.cpp` 对两个无 FROM 拼写 `current_schema()`、`current_schema ( )` 的快捷分支
直接生成非 NULL 的 NAME 值 `public`，未读取真实会话/命名空间上下文。
显式 search_path 的首个存在模式不是 public，或者没有任何存在模式时，会得到错误的
模式名而不是实际名称或 SQL NULL，影响客户端默认模式判断及后续对象定位。

候选保留既有分支和列名规则，调用已有 `ExprHelper::evalString`，显式传入当前数据库、
用户和实际 storage owner，复用当前函数/命名空间选择，而不另写 search_path 解析器。
将真实值、NULL 标记和返回声明一起交给已有结构化输出；错误保留 SQLSTATE，
不以字符串 `NULL` 代替 SQL NULL，不将空串或字面量 `NULL` 当成空值。

不修改 search_path 启动默认值、函数签名、catalog、物理格式、NAME 列 DDL、
协议编码器或全局执行 carrier。R210 默认值和用户暂缓的 R214 等项不恢复。

## 原始验证与边界

完整 `tests/current_schema_projection_protocol_e2e_test.py` 为 760 项，保持原字节不变。
覆盖十二种显式路径、首个模式缺失、全缺失、空路径、显式 pg_catalog、`$user`、
含点/引号/空格/中文的名称、字面量 `NULL` 模式、六种调用拼写、显式/默认列名，
以及真实行、零行、CAST、同名用户函数与 DROP 后重新选择。
同名函数返回正常值、SQL NULL、空串和字面量 `NULL`，同时保留语法关键字与限定内建的区别。
Simple、Extended text/binary 严格检查完整元数据、值、NULL 位、tag 和消息顺序；
非法调用核对 SQLSTATE，不声称修复所有错误发生阶段。

当前父版本 `8963d15b` 的纯已提交代码和旧工作区各完整执行 760 项，实际退出 1，
各 144 次断言失败：Simple、Extended text、binary 各 48 次。
逐条解析全部失败，数据均错误地为 `public`；只更正值后所有原严格断言成立，
说明此次基线并无额外列名、类型、framing 或命令标签错误。
144 是重复路径的断言次数，不是独立 bug 数量。同名函数作为必须保持的回归，
不将已经正确的路径虚报为旧版失败。

同一完整 760 项在本轮独立新建 PostgreSQL 18.6 实例全部通过，退出 0；
默认 fsync 未改，服务停止退出 0。旧 R211 目录中混有早期编码/列名问题的失败计数
不是本轮基线，不用于声称当前版本的缺陷数量。

另用独立私有序列计数检查同名 VOLATILE 函数的零行、仅描述、正常及逐行求值需求，
其结果单独记录，不删减或降低原 760 项期望。

关联 FUNC-01 的系统信息函数和 TYPE-21 的 NAME 结果子范围，
不由此关闭整个函数族、内部类型、catalog 或全部原 273 要求。
R215/R216 的类型差异和 R217 的 Simple 普通 case_when 命名保持已记录未完成。
不 push，不启用 GitHub Actions，不夹带其他工作区候选或 staged 18 路径。

## 本地资料

`/tmp/dbms-r211-schema-value.PNTIZ7GV` 为本地诊断资料，不是发布依赖。

| 记录 | SHA256 |
| --- | --- |
| prepared receipt | `cf8ef469418eee2737f51d1a2832b1d5920f83c57c646b46d3b24cfb27affa1c` |
| 当前完整旧基线 | `9d1a60439b0b2ebc42207b6773eddad418a7d667392e39cadd36b194f2f3f4b5` |
| 本轮 PG 18.6 完整参考 | `17bb39525262ce1f6b6be057314bdffd01cb942cd575536c26a4441ffafad9e3` |

PG 参考路径：`/tmp/dbms-private-pg18-full-fixture.l34wrm6n/completed-reference-receipt.json`。
冻结原 1,492 结果和原 273 标签不改写，不声称全部要求完成。


## 最终结果及版本边界

纯已接受代码与工作区完整 760 项均全部通过，实际退出 0；夹具仍为原始
`21a17a2d2e0774fa13515d27353f3f563fdbc7795c4be5d3e7acbacde8340ba3`。
全部 144 次错误返回值被消除，既有正常、NULL、字面量 NULL、空串、路径选择和影子函数行为保持。

补充 `tests/current_schema_demand_protocol_e2e_test.py` 的 29 项真实计数检查也已验收并注册。
旧版两侧、PostgreSQL 18.6 和候选两侧均完整通过：WHERE false、LIMIT 0、空表、
Parse/Describe 和 Bind/Describe 不消耗私有序列；实际查询每次调用消耗一次，
真实两行的 VOLATILE 影子函数逐行执行。草稿到正式名称只改路径，内容摘要不变；
正式注册后的两侧也重新完整通过。此正确旧行为作为本次必须保持的回归，不计作另修一个 bug。

每侧另有三个原生入口通过：sql_value_keyword、function_namespace_provider 和 R212 NAME 发送；
既有六个协议入口通过：R213 严格 1,072、sql_value_keyword、schema_namespace_atomicity、
scalar_signature、array_binary_result、bytea_input_grammar。
原 R213 广覆盖 1,262 仍各有 108 次失败，实际退出 1，全部精确归为既有
92 次 R215 拼接类型和 16 次 R216 引号系统函数描述差异，全部 2,080 个列名字段仍正确。
最终 24 次正式运行中实际 22 次退出 0、两次退出 1，不将已知失败冒充整套通过。
R215/R216/R217 仍为普通待修，R210 默认值未改，R214 及其他用户暂缓项不恢复。

两侧各只新编译 Main 一个生产 TU，其他 57 个生产对象按源文件、各自 127 个头文件、
编译器/选项和对象摘要匹配复用，没有混用 Root 与纯已接受版本的 OutputCapture 头文件。
正常增量构建后重复为零编译/零链接；新增测试注册只额外重链一次，重复仍为零。
原生 sql_value_keyword TU 新编译，另两个测试 TU 同摘要复用，全部重新链接。

本项独立提交 Main 的单个返回值分支、两个完整测试、对应注册和本记录。
工作区其他候选与 staged 18 路径均保持，不 push，不启用 Actions。

| 最终本地记录 | SHA256 |
| --- | --- |
| 完整旧基线分类 | `36f1f2dff987ffdeb95260a1ad053728037bc89a6aef8a091fb1f141e7f90605` |
| 候选构建 | `356608ca9e6882ee6a69d2b3223d87846f073d0a21a11391ee7bf0884f7124e0` |
| 原 760 与回归 | `2cc1ee657b8dcacc47db5f4757e6581ea7ca47aadddfe47c7e7ac9d8fa251297` |
| 计数旧版及参考 | `c00385f83bf96ed39081fd94f9b28b6d6a8e44bc8135719e0e27bfb5f3b0128d` |
| 含正式计数注册的最终验收 | `ace43c31e2e4060ab6d40ff772b2ed538d6855d04e02f310e232e73d6c567b8c` |

计数 PG 参考：`/tmp/dbms-private-pg18-full-fixture.j3jvbx3z/completed-reference-receipt.json`，SHA256 `c0f169c2468591789f6fda7953ef9ee10be4c1462f8720d814bbb5e4fb301b1c`。
