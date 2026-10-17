# R134：同名原生回调错误继承四组内置标量签名

状态：本项有限修复通过。旧干净实现1320控制完整结束、922失败，修正后同一夹具
1320零失败；正常O2构建1个CPP/repeat0，最终37个完整入口及独立核对均0。
这是四组callback归属修复，不是922个独立bug，也不关闭通用函数签名或原273总账。

## 缺陷、影响与方法

| 内容 | 已确认事实与修改方法 |
| --- | --- |
| 期望 | registerFunction替换abs/length/upper/lower后，实际callback不继承被替换内置函数的arity、输入coercion或输出类型；显式AST结果声明保留，未声明结果为UNKNOWN，分析不执行callback，真正求值一次 |
| 旧实际 | 干净ecf632e9库上1320控制有922失败。例如实际已注册的零参数abs回调仍在元数据、绑定及求值阶段报42883；其他控制暴露错误的callee声明、identity、结果类型和原始参数处理 |
| 原因 | ownsScalarSignatures只为数组查找、四字符串长度名和固定float8核对实际callback marker，四个R62名字默认按名字拥有声明；构造注册也未给它们marker，scalarFunctionIdentity仍可能把replacement称为builtin。length不在既有四个字符串长度名集合内 |
| 修改 | 仅在私有ExprEvaluator.cpp增加四名集合和DeclaredCoreScalarFunction marker；真正builtin初始化时封装，声明解析/运行coercion检查实际marker。被替换callback归native身份，复用R130的纯结果声明回退，不按熟悉名字套元数据 |
| 保留 | 真正builtin的数字、TEXT、range、几何等重载及优先级，stored routine的实际命名空间/返回声明，显式结果提示、typed NULL、零分析副作用、一次执行及未被内置规则改写的输入 |

不修改公共头、Main、parser、物理heap或SQL重载表，不放宽真正builtin的错误arity。
未知/未声明回调的结果类型不能通过执行它来学习；PG对照用于真实SQL声明，不冒
PostgreSQL拥有本项目的C++ registerFunction接口。

## 永久回归及实际验证

新原生入口[native_scalar_signature_ownership_test](../tests/native_scalar_signature_ownership_test.cpp)
由标准原生glob自动发现，不新增Python注册或纳入R98/R99/R109候选。
四名字×六种输入（零/双参数、TEXT、typed NULL、smallint、range）×三种结果提示
（无/smallint/integer[]）×NULL/值共144次调用，每次9检查；另八个真builtin各3
检查，共1320。两种注册API均覆盖。结果OID、caller声明、callee归属、纯分析、
绑定、一次执行、原始参数及真builtin对照都实际执行，未删旧断言换绿。

| 验证 | 实际结果 |
| --- | --- |
| 干净ecf旧57份O2生产库、fresh stubs、相同夹具、全局初始化前隔离 | 编译/链接0；1320完整控制、922失败，行为实际1；输入/程序/夹具首尾不变 |
| 干净开发组合：仅本项ExprEvaluator.cpp按O2编译，56份已核对干净生产donor及fresh stubs | 新1320全过；原R130结果提示168、原pg_typeof、原R62签名100三个完整入口也均0；不是正常整库构建 |
| PostgreSQL18.6/180006原scalar_signature协议 | 132全过，真实builtin/stored/准备阶段控制，不是原生API结果的PG oracle |
| Root正常构建/重复构建 | 仅ExprEvaluator.cpp重编，实际0；repeat0个CPP、实际0，58 receipt/双cache/57库匹配 |
| 最终有限门禁 | 22native+12wire+3真实Main，共37个不同完整入口按序实际执行，全部0；原生/协议均record-all保留各入口结果 |
| Main版本管理 | 核对R132的Main/三个测试源、头/flags/compiler、正常Main receipt及测试对象SHA后，重新链接57份当前O2库、不链接stubs、三个隔离运行0；不是fresh TU编译或旧已链接程序复用 |
| 独立核对 | 37个名字/顺序/次数/日志/真实Main产物、旧新相同夹具、正常1/repeat0、58 receipt/57库、源码封印均0；干净原1436证据仍保持原终态1及两真实失败 |

证据在`/tmp/dbms-review-r46.ifu6SWLp/`的`r134-clean-old-native.log`、
`r134-clean-candidate.log`、`r134-reference132.log`、`r134-normal-build.log`、
`r134-repeat-build.log`、`r134-final-gate.log`、`r134-final-native-results/`、
`r134-final-protocols.log`、三个frontend日志及`r134-independent-gate.log`。
干净开发源码在`/tmp/dbms-r134-clean-source.LOl6TY9J/`，逐字节确认只有五处本项
修改，不复制Root的R109未接受实现。以上临时证据不是发行包承诺。

Root程序SHA256为
`ca37ee38f007890b725e99db7b3a54f460102b0a2df845ccff71ade947a54f93`，
cache为`b559fe827c476e024999ff8e55ecaff4fe2526f24bd0d97ede9a3e452c659a2d`，
src/tests/scripts封印为
`2d29e0a49f60b0fb37195e618108d38c8060b1e3e7fff4a9032dcb393237fc4d`，
Root头/编译器/flags签名739bf42b保持，不与其他绝对路径的8c签名混用。

## 范围与版本管理

本项接受557MD、900native+534Python+3Main=1437；工作区另含原三个MD和六个
运行入口候选，即560MD、903native+537Python+3Main=1443；Python AST558、Bash13
另作静态检查。新1437组合尚未完整注册回归，不冒全量、真实TLS或整库sanitizer。

[先前干净1436终态](issue-full-registered-ecf632e9.md)仍是1433实际过/2失败/
1TLS环境skip，不能以本项有限门禁的stored_function_engine_owner通过关闭其
旧DROP清理失败或猜测原因已经修复。后台专项、R51及其他用户暂缓项不重开。

原273条目/checkbox与22complete/166partial/70unverified/15deferred不变；其他
按名字回退的原生callback、完整通用签名和时间戳微秒持久化仍为普通开放范围。
独立本地commit仅纳入本项hunk/新测试/相关文档，原56933字节候选差异及三个
暂缓draft逐字节保护，不push，GitHub Actions保持禁用。
