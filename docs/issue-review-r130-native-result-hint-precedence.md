# R130 原生函数结果声明与回调所有权

状态：本项修复及有限验收完成，不等于原273总账或所有函数元数据完成。
来源为[干净8f完整回归](issue-full-registered-8f0465fc.md)的F8F-03。

## 期望、实际及影响

AST的resolvedResultType保存绑定后的真实结果声明。没有回调返回声明时，
该提示不应被默认UNKNOWN覆盖，也不能执行回调来发现类型；运行时仍执行参数
恰好一次。真实builtin/stored元数据优先于冲突提示，原生同名替换不能继承
builtin固定返回类型。NULL的声明与是否执行是两个独立事实。

原完整入口在`pg_typeof(tick())`的OID/次数组合断言失败。拆分断言并链接
干净8f的57份O2库后，实际输出`oid=705 effects=1 declared=smallint`，随后
OID断言实际134：声明仍在、调用恰一次，错误是smallint被误报告为unknown。
这是原生元数据问题，不把它猜成回调重复执行或所有SQL函数都错误。

元数据收集优先生成opaque callback的unknown映射，遮住已有AST结果声明；
固定float8名字的早期unknown回退也忽略显式声明，身份键还将其原生替换当作
builtin。168项原生矩阵的干净旧库完整终态为53失败/实际1，覆盖三条边界。

## 修改方法

| 位置 | 方法 |
| --- | --- |
| expr_helper.cpp | 只有没有结果提示才生成opaque回退；真实callee声明仍最先处理。实际native身份的固定返回名替换采用unknown，不执行回调或继承名称类型；已有ceil/mod/round输入派生回退保持 |
| ExprEvaluator.cpp | 固定float8的早期unknown回退只用于无显式提示；其真实原生替换标记为native，使元数据与身份键都保留所有权，不改真实builtin签名 |
| pg_typeof_test.cpp | 原OID==21、effects==1断言分别保留，失败时打印OID/次数/提示；其后除零、独立engine stored声明/typed NULL/查询descriptor及原清理断言均不删 |
| native_function_result_hint_test.cpp | 168控制：八种声明×NULL/非NULL、绑定前后元数据纯度、未知原生回调、不继承builtin的替换、三类数学旧回退及六种真正固定callee声明优先级 |

不改公共头、Main、存储格式或输入SQL语法；不把任意外部错误提示当作真实SQL
声明，不扩大动态SQRT/MOD等非固定callee的元数据契约。R109等原候选保持不变。

## 证据与验收计划

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`。

- `r130-pg-typeof-old-diagnostic.log`实际705/1/smallint、134。第一次诊断代码
  直接流输出optional OID而编译失败1，未执行；日志另存
  `r130-pg-typeof-diagnostic-initial-compile1.log`，改为value_or后才得到运行证据。
- 最终`r130-native168-old.log`完整168/53失败/实际1，非53个独立bug。首版168
  另有SQRT与MOD被误当固定返回元数据的夹具假设，日志
  `r130-native168-old-initial-fixture.log`保存168/59；改为真正固定sin/pi控制后
  重跑同一干净旧库。未删除未知替换控制或改期望为运行时返回类型。
- 既有永久`pg_typeof_protocol_e2e_test.py`实际PG18.6（180006）56/0、实际0，
  见`r130-pg-typeof-reference.log`。它验证SQL声明、NULL、参数、副作用及错误
  阶段，不声称PG提供本项目的原生registerFunction/AST提示API，也不冒168项
  原生矩阵已经在PG执行。
- 干净开发用两个自有O2 CPP、55份已核对干净O2 donor、fresh stubs与初始化
  前隔离执行新168及原pg_typeof两个完整入口，均实际0；原有独立engine与
  最后的DROP断言也实际执行通过，不跳过后半段。该轮全部tracked输入/原干净
  binary保持不变，记录见`r130-clean-candidate.log`；这不是正常生产构建。
- 正常O2实际重编ExprEvaluator与expr_helper两个CPP、repeat为0个，均实际0。
  20native/12wire/3真实Main=35个不同完整入口均实际0，独立检查亦0。三个真实
  Main核对未改的R124测试TU/source/header/flags/compiler/对象字节，再新链接
  当前57份O2库、无stubs并实际隔离执行；不冒Main TU重新编译。全部58 receipt/
  双缓存/57份测试库字节/冻结输入终态均0。原8f三项FAILED不改写；R129已
  有限修复F8F-02，R51清理路径仍按用户暂缓，不复审其内部或靠删除断言换绿。

正式证据为`r130-normal-build.log`、`r130-repeat-build.log`、`r130-final-driver.log`、
`r130-final-native.log`、`r130-final-protocols.log`、三个frontend日志与
`r130-final-independent.log`。源/测试/脚本冻结seal为
`42df1eaf83f3bfce49a2fe2b776b0ab167c28fea1dbc8148178de3ea2d1484a1`，
程序SHA256为`62888353d361d91c08bcfbf0474ea5594f69edb2e34949227b9b59242c37da72`，
cache为`84866a7f64fab8734ff20eb2a85f30d98bdb7d7e221b58f93a3f1bf00691c6c1`。
主工作区compile仍739bf42b，不与干净开发0b2ff036的头或库混用。

接受清单552MD、899native/531Python/3Main=1433；工作区555MD、902/534/3=1439
另含原六候选，555Python AST/13Bash及本地目标另作静态检查。隔离暂存只含
自身hunk、按本项独立本地commit保存；原56933字节候选patch和三个跳过草稿
逐字节保护核对通过，不混入R98/R99/R109，也没有新1433全量运行。

原273仍22complete/166partial/70unverified/15deferred；不push，Actions禁用。
没有新的完整注册回归、真实TLS或整库sanitizer证明。
