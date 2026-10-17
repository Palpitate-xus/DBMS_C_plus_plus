# R129 原生数组投影丢失声明类型

状态：本项修复与有限验收完成，不等于原273总账或整个数组家族完成。
来源为[干净8f完整回归](issue-full-registered-8f0465fc.md)的F8F-02，不改写其失败。

## 期望、实际和影响

`StorageEngine::queryExpr`的标量投影应使用真实列声明：INTEGER[]仍为数组，
普通TEXT（包括NULL或看起来像数组的文本）仍为TEXT。ARRAY_POSITION对数组
按元素类型解析，TEXT参数按签名拒绝；不能靠放宽SQL签名换取旧测试通过。

原`array_inspection_projection_test`把序列化数组放在普通TEXT列；R108严格
声明解析后，首次`array_position(flat_array,20)`抛42883，整入口abort。
把夹具改为TypeRegistry公开API创建的真实INTEGER[]后，已确认存储元数据为
`dataType=integer,isArray=true`，在干净8f的57份O2库下同一首次调用仍42883。
最终扩充夹具也在相同干净库下实际134，不能把问题只归咎于测试。

根因是TableManage的原生标量路由手工构造typeHints时只读dataType，丢掉
isArray；类型解析看见INTEGER而非INTEGER[]。普通SQL的真实表绑定保留数组，
所以同组50条协议对照在实际PG18.6（180006）和干净8f程序均全过。
此缺陷影响使用SelectExpr/queryExpr的原生数组查询，不是用户跳过的复数
ARRAY_POSITIONS（R109），也不证明整个数组函数家族已完成。

## 修改方法与永久回归

| 位置 | 方法 |
| --- | --- |
| TableManage.cpp | expreval、array_dims/cardinality/array_position、array_length及NULL结果判定四个类型提示构造点复用既有buildTypeHints，保留数组声明；不推断文本内容、不新增隐式转换 |
| array_inspection_projection_test.cpp | 五个真实INTEGER[]列，经公开resolveColumnType建表并核对存储元数据；原查找、NULL、空、负start及多维0A000断言保留，22003用显式INTEGER输入转换，另保留原bare BIGINT起点的42883检查；增加generic expreval和真实TEXT/NULL TEXT控制 |
| array_inspection_projection_protocol_e2e_test.py | 注册51控制，真实数组/TEXT行、NULL/空、BIGINT签名/INTEGER范围/多维、WHERE false/LIMIT 0、Simple与Parse/Describe、输出OID及错误后恢复，支持实际PG18参考 |

没有修改公共头、物理存储格式、SQL签名或已跳过候选。其它原生路由中的手工
类型提示尚须逐项验证，不把四处修改声称整个旧投影引擎已经完成。

## 已有证据与验收计划

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`。

- 原完整运行日志F8F-02保留。`r129-final-native-old134.log`是最终扩充
  夹具链接干净8f当前头/57个O2库/fresh stubs、初始化前隔离的实际134；没有
  使用主工作区R109候选掩盖错误，也不声称该失败入口已执行所有后续断言。
- `r129-reference50.log`与`r129-clean-accepted50.log`均50控制/0失败/实际0。
  首版参考50中两项夹具错误地禁止运行时错误前RowDescription，实际PG允许
  22004/0A000执行阶段先发T；首版日志另存`r129-reference50-initial-fixture.log`。
  只纠正错误阶段断言：42883仍禁止T，所有错误仍无D/C且恰一个E；期望值与
  SQLSTATE不变。不是数据库从两失败修到通过。
- 修复类型提示后的首版干净开发运行到裸2147483648起点：旧夹具把BIGINT
  签名拒绝误期望为22003。`r129-clean-candidate-initial-start134.log`保留实际134。
  新增真实PG的相同bare表达式42883控制，并将原22003控制明确写为
  `2147483648::integer`；两个断言都保留，不放宽签名或吞错误。
- 最终协议51控制的`r129-reference51.log`、`r129-clean-accepted51.log`均实际0。
  `r129-clean-final-native.log`最终原生全部通过：自有TableManage一个O2对象、
  56份已核对的干净O2 donor、fresh stubs与初始化前隔离。TableManage沿用该轮
  已成功O2编译的原对象，source/object SHA核对不变；不冒重新编译或正常生产构建。
  临时复用driver首版有shell引用错误、实际2且未执行夹具，日志
  `r129-clean-final-helper-initial2.log`保留，修正并语法检查后才实际执行通过。
- 实际正常O2重编TableManage一个CPP、repeat为0个，均实际0；最终14native/
  9wire/3真实Main共26个不同完整入口全部实际0，独立检查亦0。真实Main的
  未变R124测试TU/source/头/flags/compiler/对象字节先核对，再新链接57份
  当前O2库、无stubs并实际隔离运行三个前端；不冒Main TU重新编译。
  全58 receipt、双缓存/57库字节与冻结输入终态均0。证据为
  `r129-normal-build.log`、`r129-repeat-build.log`、`r129-final-driver.log`、
  `r129-final-native.log`、`r129-final-protocols.log`、三个frontend日志及
  `r129-final-independent.log`。暂存隔离仅含自身hunk，原56933字节候选patch
  及三个跳过草稿逐字节保护检查0；本项独立本地commit，不混入R98/R99/R109。
  不把定向通过改写成重新执行完整1432，F8F-01暂缓、F8F-03另项仍开放。

本项正常构建程序SHA256为
`600a5f1dc8bb00776519f3cdf70cfaf423174c06eec20e92546951dae0ccd3f9`，
cache为`9767ad7ce87cda03a85b7d1d685b332889945e1d9e2849ee74ea5f442d0bbfc9`，
冻结源/测试/脚本seal为
`3b03dfd9e9e240e18d75e6b737b902611beb5ad0d059cc99511c181a6be4b1b0`。
主工作区头/flags签名仍为739bf42b，不与干净开发0b2ff036头混用。
接受清单551MD、898native/531Python/3Main=1432，工作区554MD、901/534/3=1438
另含原六个候选入口；555Python AST/13Bash及本地文件目标静态检查另行核对。

原273状态/勾选维持22complete/166partial/70unverified/15deferred。
不push，Actions保持禁用；真实TLS与整库sanitizer未因本项获得证明。
