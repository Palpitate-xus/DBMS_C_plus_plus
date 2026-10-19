# R163：限定函数的Parse/Describe签名校验（有限修复）

状态：受支持标量形状的有限修复已通过两端组合验收，不由执行阶段报错代替协议准备阶段校验。

## 已复现的九条元数据错误

R161协议夹具对九个无参数、真实表来源的非法限定调用发送Parse+Describe+Sync。
PG18.6每条报42883；旧版与R161 V5两端均错误发送ParseComplete、
ParameterDescription(0)、RowDescription：六条为TEXT25，三条为FLOAT8701。
包括现有namespace中不存在的函数、builtin错误输入/arity、错误大小写用户名字、
用户函数错误arity及错误参数类型。不能把随意返回TEXT视为成功描述。

其中八条在执行阶段正确报错；另一条用户mix反序类型执行也错误接受，
由[R162通用存储签名](issue-review-r162-generic-stored-signatures.md)处理。
这两个owner必须分别验证，不能从修复执行类型匹配推断所有Parse已正确。

## 原因与修改方法

[协议准备入口](../src/network/NetworkServer.cpp)的validatePreparedStoredSignatures
在有FROM时直接返回，且只探测部分未限定名字；validatePreparedScalarArithmetic
又只覆盖有限函数集合。无参真实表路径未触发完整prepareBoundQuery，
Describe后退到ExprHelper::inferParsedResultType——该API负责类型发现，不是有效性校验。
于是不存在的调用也可能得到猜测列描述。

[实际绑定](../src/commands/TableManage.cpp)的metadata.functionType已有
hasScalarFunction失败后抛42883的入口，执行阶段可走到它；Parse阶段没有。
应在发布ParseComplete和登记statement之前，按真正保留的AST与查询来源执行纯绑定，
使用实际列、参数及namespace声明，保留绑定错误及SQLSTATE，不为取类型执行SQL或回调。
不能仅在Describe补throw：失败的named Parse也不能留下占用名称的半成品。

需审查现有不支持的语句形状、未指定参数类型、协议事务归属及错误恢复边界，
不能通过对所有语句无条件执行函数、伪造TEXT描述或改变未知参数契约来绕过。
用户跳过的权限、安全、provider生命周期及其它模块不恢复。

## 后续验收

保留原九条输入，覆盖Simple与Extended的实际阶段、named/unnamed Parse、
失败后同名重试、Statement/Portal Describe、合法重执行、text/binary参数及结果，
NULL/零行、真实表/别名/引用名字/搜索路径、元数据不调用effects、事务及Sync恢复。
合法声明的OID/长度/typmod/格式与UNKNOWN类型错误需完整验证，不能只比较最终行字符串。

PG全637控制0失败；R161 V5 Root与accepted-only clean各637控制18失败，
其中本项九个元数据控制，另九个为R162执行类型错误。原始完整fixture没有删项。
证据在 /tmp/dbms-review-r46.ifu6SWLp/r161-v5-*-protocol.log。
以上为首次登记及修复前证据，当时尚待最终验收，实际终态见末节；原273总账仍未完成，不push，Actions禁用。

## 最终候选的完整运行检查点

R169 V4工作区完整58入口实际全部通过；本项协议1017及共享637均0失败。
最终入口仅对受支持的已知签名标量形状纯绑定，保留失败事务的原Parse错误所有权；
聚合、窗口、SRF、CTE、GROUP/DISTINCT等其他形状保持原接收者，不因“完整校验”误提前拒绝。
原993控制及后补24条GROUP BY别名正向控制全部保留；失败Parse同名重试、
无执行Describe、真实表/零行、参数重复执行和错误恢复均未放宽。
独立干净版本亦完整58实际退出0；两端各26native、29协议、三个真实Main全部通过。
组合构建、范围和原始日志索引见[R169](issue-review-r169-simple-query-raw-syntax.md)。
