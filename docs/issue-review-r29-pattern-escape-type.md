# R29：LIKE ESCAPE 使用类型内容而非字符串地址

## 根因和修改

`src/parser/query_binding.cpp` 的 ESCAPE 绑定分支用条件表达式选择 `"bytea"`
或 `"text"`。原 `const auto target` 推导为 `const char*`，随后
`target == "bytea"` 比较地址。相同字面量是否被编译器合并不能决定 SQL 类型规则。

修改仅将 target 明确声明为 `const std::string`，使比较按内容进行。
沿用已经绑定的操作数类型、ESCAPE 类型检查和输入转换，不执行查询或用户函数
推断类型，也没有改变 LIKE/ILIKE 的匹配算法。

这是已确认的源码/编译诊断问题，不是本次当前工具链已复现的错误查询结果：
原实现的常规 O2 和关闭常量合并版本均通过下面 34 项语义检查。不能因此依赖
字符串合并，也不能将该结果写成“原 SQL 测试失败”。

## 验收

新增 `tests/pattern_escape_type_identity_test.cpp`，由现有 native 测试发现机制
自动纳入。34 项只读绑定检查覆盖 bytea/text、LIKE/NOT LIKE/ILIKE/NOT ILIKE、
unknown 字面量、NULL、明确的正确/错误 ESCAPE 类型和 typed 参数；来源只有
复制的空表元数据，没有真实数据行或执行器。合法结果描述为 boolean；错误类型
要求 42883，即使来源为空也不能跳过类型检查。

| 实际检查 | 修改前 | 修改后 |
| --- | --- | --- |
| 项目 flags 加 `-Werror=address -fsyntax-only` 编译 query_binding.cpp | exit 1，地址比较诊断 | exit 0 |
| 标准 O2，34 项新语义控制 | exit 0 | exit 0 |
| O2 加 `-fno-merge-constants -fno-merge-all-constants`，34 项控制 | exit 0 | exit 0 |

关闭常量合并的变体仅将该翻译单元编译到独立临时对象；其它对象按私树当前
源文件/flags 的缓存签名校验后链接，没有覆盖标准对象或伪造签名。
标准产品构建真实重编该翻译单元并重新链接。

原有 `pattern_predicate_priority_protocol_e2e_test.py` 在 PostgreSQL 18.6 上
实际 exit 0，包含 bytea ESCAPE、错误类型、NULL 优先级和合法匹配控制。
候选上的新 native 驱动、原有 `pattern_predicate_binding_test.cpp` 和
`query_binding_test.cpp` 均 exit 0；以下三个原有完整协议驱动也均 exit 0：

- `pattern_predicate_priority_protocol_e2e_test.py`
- `pattern_predicate_protocol_e2e_test.py`
- `legacy_sql_pattern_consumers_protocol_e2e_test.py`

本次候选 Main SHA-256：
`bca85d326d1af70441ee069dc117dfedb660b0e9f565382b44b91b48b3735b5d`。
原始编译诊断、normal/no-merge 两版本日志和协议输出保存在本机
`/tmp/dbms-review-r29-evidence.WNb6inxt/`。这是定向验证，不是所有编译器或全套回归声明。
