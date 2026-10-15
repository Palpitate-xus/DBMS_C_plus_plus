# R62：标量函数必须先解析真实参数签名

状态：本次已复现的ABS、upper/lower、length签名已修复并通过定向门禁；
后续十八个固定float8名字由[R83](issue-review-r83-float8-scalar-signatures.md)独立扩展。
其他回调及任意名称stored routine的完整通用签名覆盖仍未完成，R62广义范围保持partial。
R60修正窗口routine身份，不等于完成全部标量签名校验。

实际PostgreSQL18.6/180006对upper(1)、upper()、upper(text,text)、abs(text)、
length(1)报42883；无FROM、空表WHERE false和LIMIT 0均如此。加OVER仍先报
42883，不应在错误签名上先报42809。主分支R60集成后重新对照，普通调用仍可
返回值或空结果；错误OVER调用先被识别成标量而返回42809。

源码resolveScalarFunction的pg_catalog注册表主要按名字判断可用，只有少数
专门callback检查arity。普通builtin和部分stored routine没有完整参数类型匹配。
不能仅在运行callback中增加参数个数检查：空来源/未执行分支仍须正确分析。

修改方法：建立元数据驱动的arity、声明类型及合法隐式转换匹配；先匹配真实
命名空间和签名，再检查窗口/聚合角色。全程纯绑定，不求值参数选签名，不把
同名合法用户函数误当builtin。用同一元数据同时验证普通执行、prepared、
窗口及空来源，并保留合法重载、NULL和声明输出类型。完成前不宣称全部标量
函数已经兼容。ABS整数范围观察另列R68，不能混为签名修复。

本机证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：
scalar-over-signature-discovery.log是较早候选；
r62-plain-signature-discovery.log和r62-plain-signature-expanded-discovery.log是
对实际PG18.6及R60集成后主分支的独立只读对照，脚本保留。TEMP数据随连接清理，
未修改共享参考表，未显示参考连接密码。这些临时证据路径不属于发行包。

## 当前实现与验收

真实resolver新增上述四个名字的声明表：arity、输入类型、结果类型、合法隐式转换、
exact/preferred/UNKNOWN类别选择及search_path同签名遮蔽。同名用户routine参与真实
参数匹配，不再由按名命中的catalog callback抢走upper(integer)。未知字面量只转换
选定签名的输入codec，不执行表达式；实际PG验证普通abs('bad')为22P02，而带OVER
的有效签名先报42809。错误类型/arity带OVER仍先报42883。

纯binder负责无FROM、空来源、LIMIT 0和未执行分支；Main的真实AST前置检查及
SQL PREPARE声明入口复用它，PREPARE不计划常量或执行参数。执行使用相同声明做
隐式coercion，保留legacy带typed RowContext但尚无列binding的消费者。length的
lseg/path/tsvector重载补齐结果值和声明，真实全文目录身份单独以R70提交。

新增100项native控制及132项协议控制，实际PG18.6协议全0；主工作区最终十一完整
native、十一完整协议全0，生产输入/binary/58份receipt首尾一致。相关日志：
`r62-final-native.log`、`r62-final-protocols.log`及同名前缀独立gate/driver日志目录。
真实标准O2三次构建分别保留`r62-root-build.log`、`r62-second-root-build.log`、
`r62-third-root-build.log`；没有公共头改变或借用其他树对象。

原126协议矩阵86失败；第一版126仍有6失败（几何结果OID、SQL PREPARE、全文
目录），没有改写为通过。新增六项codec/OVER控制曾有三个错误参考预期，实际
PG纠正后132全0，原`r62-expanded-reference.log`与纠正后的独立日志均保留。
第一版98native已过不当作最终100覆盖；helper初次直接启动exit126不计产品失败。
这里没有重新运行全部注册入口，也未补齐所有builtin/UDF重载、命名参数或
自定义cast/domain功能，不能把四组签名修复升级为整个功能族完成。

后续冻结b5def9ba全注册运行暴露legacy结构指纹中的复杂未绑定参数遗漏：
保护顺序必须先于任何参数类型推断。该独立回归由[R80](issue-review-r80-unbound-scalar-inference.md)
修正，17原生控制及原窗口44/七协议邻居验收，不改变本节原100/132历史数量。

原生BINARY/VARBINARY还需把真实overload身份与raw-byte codec区别开，
[R79](issue-review-r79-native-binary-length.md)保留原九投影控制并新增30控制。
这些具体遗漏已独立验收，不因此将通用签名覆盖从partial改为complete。

R83补齐十八个固定float8名字的arity、输入转换、输出身份及角色顺序，并保护
registerFunction替换的实际callback所有权。额外验证同名TEXT UDF、UNKNOWN/NULL、
SQL PREPARE、嵌套、空来源及准备阶段不执行参数；真实前端还需保留选中callee的
whole-query绑定，不能以未绑定列的legacy探测结果覆盖它。原100/132验收数字
属于前述四组历史，不改写；全部通用签名及所有接收器形状仍保持partial。
