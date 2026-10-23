# R243：EXTRACT/date_part 输入签名与语法角色（用户暂缓）

本项记录R242保留的输入资格问题：21个不支持类型与10个EXTRACT NULL字段语法控制。
真实PG18.6已捕获948条额外不可变结果，覆盖494关键字、六个重载、NULL、引号/限定名、
参数数量与普通函数角色；另有180个实际callback所有权检查。

需保持EXTRACT(field FROM value)与普通quoted/qualified extract(text,value)的区别，
使用真正输入类型选择重载，不能让NULL/空输入绕过资格，也不能将类型规则套给opaque
native替换。完整协议包含NULL实际表行、WHERE false、LIMIT0及纯Describe。

## 状态与实测

按用户最新“skip it”停止本模块；没有实施生产源码修复，也没有注册或接受新增测试。
现有四份未跟踪夹具与日志保留，不能将这些失败计为已修复。
此时最新已接受代码为 `70bd05db216ac5a3c20fc8069aeb6f079ef5a49d`。

| 完整检查 | PostgreSQL 18.6 | 工作区 / 纯提交投影旧实现 |
| --- | --- | --- |
| 948 条表达式参考捕获 | 完整捕获实际结果 | 原生各退出 1，775 失败 |
| 4516 项协议检查 | 修正版完整退出 0 | 各退出 1，3454 失败 |
| 180 项 callback 所有权检查 | 原生注册接口，不作 PG 对照 | 各退出 1，180 失败 |

失败数字是控制/断言数，不代表同等数量的独立缺陷。协议首版有 600 个夹具失败：
生成时 SQL 字段字符串和引用函数名丢失引号。首版及日志保留；只修正夹具后，
同一 4516 项在真实 PG18 上通过，随后两侧旧产品完整运行得到上表结果。
私有 PG 实例正常停止，未关闭 fsync；没有将轮询超时解释为产品失败或重新启动基线。

## 后续修改方法（本轮未实施）

1. 为实际内建 callback 声明六组 `(text, source)` 重载，source 为 date、time、
   timetz、timestamp、timestamptz、interval；EXTRACT 返回 NUMERIC，date_part 返回 float8。
   仅实际声明所有者采用规则；同名 opaque native replacement 保留自己的 arity、类型与波动性。
2. 在解析层区分关键字 `EXTRACT(field FROM value)` 与普通引用/限定函数调用。
   按真实语法验证 field，不允许 NULL 或任意关键字被误当合法字段；不改写嵌套表达式的 token。
3. 使用纯类型绑定检查 NULL、空表、LIMIT 0 和 Describe 的输入资格，不执行实参来猜元数据。
4. 保留完整参考结果，再独立检查值、结果域、错误状态与错误阶段；不能删掉不通过的控制。

证据目录：`/tmp/dbms-r243-temporal-input.8tIcvgYg`。
完整记录为 `completed-gold-generation-receipt.json`、
`completed-v2-reference-receipt.json`、`completed-old-baseline-receipt.json`、
`completed-old-callback-receipt.json`；首版参考失败在 `completed-reference-receipt.json`。

R244规划/错误阶段仍开放，并未随本次 skip 自动暂缓；R156完整载体/显示、R238剩余及
其他用户暂缓不恢复。原273总清单未完成，不push，Actions保持禁用。
