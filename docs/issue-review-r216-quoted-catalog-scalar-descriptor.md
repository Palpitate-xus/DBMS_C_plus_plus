# R216：带引号系统函数的静态结果类型

状态：本项静态类型修复已验收；总清单仍未完成。

## 问题与修改方法

`SELECT pg_catalog."abs"(n)` 和 `SELECT "pg_catalog"."abs"(n)` 的实际数值正确，
但 Extended Parse/Describe、Bind/Describe 会把 integer/bigint/float8 结果声明为 text，
宽度为 -1。二进制 DataRow 仍是数值字节，依赖声明解码的客户端会误读。
空表、WHERE false、LIMIT 0 同样受到影响，不能靠执行第一行推断声明。

已有标量身份解析器能识别该调用；在缺少完整绑定声明时，静态 AST 类型回退却直接
对函数 token 转小写，`"abs"` 并不等于 `abs`，于是落入 text 默认值。
修改仅在显式 pg_catalog 命名空间下解码 SQL 函数标识符，并保持引号的大小写。
已解析的用户函数、原生不透明回调、显式声明及 set-returning 结果仍优先，
不改变函数执行、输入签名、权限、存储或 search_path 默认值。

## 完整测试及旧基线

新增 `tests/quoted_catalog_scalar_descriptor_protocol_e2e_test.py`，完整 576 项：
八种限定/引号/非引号拼写，integer/bigint/double precision，标量、SQL NULL、
真实四行、实际空表、WHERE false、LIMIT 0，Simple 与 Extended text/binary，
独立 Parse/Describe、两种 Bind/Describe、Close，以及用户同名函数优先和带引号大小写拒绝。
严格校验值、NULL 位、OID、宽度、typmod、来源、格式、tag 及消息顺序。

完整 576 项在新建私有 PostgreSQL 18.6 实例全部通过，实际退出 0；默认 fsync 不变，
停止退出 0。纯已提交版本与工作区分别完整执行，各 66 次失败、实际退出 1，
不删检查、不将参考失败当产品期望。失败次数是重复路径检查，不是 66 个独立 bug。

R213 原完整 1,262 项夹具保持原字节，继续作为边界诊断；R215 拼接类型不由本项关闭。
R217 普通 case_when 列名仍开放。R210 已按用户要求暂缓，本项不恢复默认路径/当前角色工作；
R214 及其他暂缓项保持。冻结原 1,492 结果和原 273 标签不改写。

## 本地证据

目录 `/tmp/dbms-r216-catalog-descriptor.dWFjYJ6u` 保存输入、旧基线、原文件及候选构建。
参考 receipt：`/tmp/dbms-private-pg18-full-fixture.hubz4yg9/completed-reference-receipt.json`，
SHA256 `3c0e90c168eff41e19f1ef4984111b02fca4248f2b2bf02badbba2d579244330`。
只接受本项通过验证的最小源码、测试、注册及记录；不夹带工作区其他改动或 staged 18 路径。
不 push，不启用 GitHub Actions，不声称已全面检测通过。


## 最终结果与版本边界

两侧完整 576 项全部通过，实际退出 0，测试与 PG 参考使用同一份完整源码。
旧版各 66 次失败精确对应 114 个错误描述字段；仅纠正 OID/宽度后的离线诊断确认：
原始值、NULL 位、二进制数据、来源、typmod、格式、tag 和消息顺序均正确。
离线诊断不是产品通过；产品验收依据为修复后的完整运行，历史失败不改写。

每侧八个原生入口全部退出 0：native_scalar_signature_ownership、
unbound_scalar_signature_preparation、scalar_signature、mod_result_domain、
integral_rounding_result_type、generic_stored_signature、prepared_projection_label、
function_namespace_provider。它们保留不透明回调、未绑定声明、原生签名与命名空间的既有契约。
每侧十二个协议入口退出 0，包括本项 576、既有 R213 严格 1,072、R211 的 760/29、
ABS 列身份、引号非限定函数、标量签名、整数取整、MOD、存储签名、数组发送和 BYTEA 语法。

原 R213 广覆盖 1,262 项仍各有 92 次失败、实际退出 1，全部为原 R215 拼接类型；
原 R216 的 16 次描述差异已消除，不将该完整夹具称为通过。
正式共 42 次运行，实际 40 次退出 0、两次退出 1，已知差异保持开放。

首个汇总驱动实际退出 1：在纯版本完整 21 次运行结束后，分类代码直接比较 Python
repr 日志中的 SQL，未解码单引号转义而中断；这不是产品验收通过。
原驱动及全部日志保留，独立解析器逐条检查原 1,262 项和 2,080 个名称字段后确认
仅余 92 次 R215，继续完成工作区 21 次运行，未删减或放宽任何夹具断言。

两侧仅重新编译 expr_helper 一个生产 TU，其他 57 个对象按源文件、编译器与选项、
对象和各自 127 个头文件摘要核对后复用；不是 58 个全部 fresh 编译。
八个原生测试 TU 均分别新编译并重新链接。工作区正常构建为一编译一链接，重复构建零编译零链接。
纯已接受与 Root 的 OutputCapture 头文件不同，未混用。

本项独立提交只含 expr_helper 的该身份解码分支、一条测试注册、完整 576 夹具及本记录。
Root 原有 array_positions 等未接受变更、其他工作区改动与 staged 18 路径保持不变。
R210/R214 等用户暂缓项不恢复，R215/R217 普通开放；R218/R219 另有记录，不计作本项解决。
不 push，Actions 保持禁用，冻结原 1,492 和原 273 状态不变，不声称总目标完成。

| 本地记录 | SHA256 |
| --- | --- |
| 完整旧基线 | `8366862649b10ee655cca877b968dc005c1da761cabd67a68ec853dba6275030` |
| 旧基线严格字段分类 | `afdfefc6749971e888057beb873de946c16f214141767f39a2240a2f66e52e50` |
| 候选构建 | `99a9443de173f09f63afbe975f810ca356993b261d7e0104538b9c6139db7712` |
| 首个汇总驱动拒绝记录 | `29aa03e425460588e99d1f8b6fc876216fd68adf573561f9b6b2dd00c354622f` |
| 完整有限门禁 | `58abb853bdbac8eadc8f67b58a43ee41e79d01f06d58e19ffee8e7ad51aac291` |
