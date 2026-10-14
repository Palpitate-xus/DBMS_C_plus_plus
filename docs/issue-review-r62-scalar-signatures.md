# R62：标量函数必须先解析真实参数签名

状态：已复现，尚未修复。R60修正窗口routine身份，不等于完成标量签名校验。

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
