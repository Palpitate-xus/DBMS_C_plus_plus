# R166：整式标量调用的协议参数OID（有限修复，范围外开放）

状态：source-free整式标量直接参数的有限修复已通过两端组合验收，通用参数推断仍开放。

## 实测错误

[R162](issue-review-r162-generic-stored-signatures.md)协议矩阵包含真实函数mix(TEXT,INTEGER)。
`SELECT schema.mix($1,$2) AS v`在Parse提供OID0/0后，PG18.6的ParameterDescription为25/23；
项目仍返回0/0。随后26项binary Bind/重复执行因不知道输入codec而报0A000。
这27项与[R165](issue-review-r165-builtin-array-binary-bind.md)的44项数组输入错误不同。
另六项未限定函数Parse过迟报错归[R163](issue-review-r163-qualified-parse-validation.md)。

## 原因与修改方法

[NetworkServer](../src/network/NetworkServer.cpp)扩展缺失参数OID为零，却没有将实际选择的
函数输入声明回填保留语句。prepareBoundQuery的结果类型并不是输入参数类型；
不能复制函数返回OID或把binder的出现次序slot当成SQL的$n编号。

有限的source-free完整标量函数调用可以在私有准备AST中纯绑定实际选中声明，
从真实参数source coordinates关联prepared.uses，推断未指定的wire OID，
验证同一$n的声明一致性，并用最终共享参数类型重新绑定后再发布。
显式OID不得改写，metadata不执行函数体，失败不留下半类型化语句。

一般表达式、跨调用/子查询的共享参数约束、显式cast顺序、裸UNKNOWN输出及IS NULL
需要独立的完整推断上下文。有限入口不能据此宣称全部协议参数推断已实现；
不为避开问题恢复用户暂缓的类型化常量模块。

## 证据与待验收

`/tmp/dbms-review-r46.ifu6SWLp/r162-v3-{root,clean}-results/`
下的generic_stored_signature_protocol_e2e_test.log：retained_8描述及binary控制共27失败。
待完整验证omitted/部分omitted/OID0、编号乱序、重复参数、显式OID、重执行、
NULL/text/binary和metadata纯度；原1705项全部保留。

## 有限候选的范围复查

原263项专用夹具及1705项共享夹具在V6 Root/accepted-only候选树中均为零失败，
但这不是本项最终验收。额外8条仅Parse/Describe的发现记录表明：
`SELECT pg_catalog.sin($1) AS v ORDER BY $2::integer`及其float8变体，
PG18.6分别返回参数OID701/23及701/701；旧V3返回0/0，
未收紧的候选反而在Parse报42P18。这是候选引入的提前拒绝回归，不能隐藏在263项通过之后。

三端完整8条原始frames、描述及错误已保存在
`/tmp/dbms-review-r46.ifu6SWLp/r166-scope-{reference,old-v3,candidate}-before.log`；
每端均明确标记`COLLECTION_ONLY_NOT_PASS`，没有将旧版本的OID0描述称为兼容通过。
同批还保留ORDER BY 1、LIMIT 0、OFFSET 0、FETCH FIRST 0以及参数化LIMIT/OFFSET。

候选入口现增加ORDER BY、显式LIMIT/OFFSET（包括零值）、FETCH及WITH TIES的形状排除，
保持这些容器交由原有推断路径负责。`r166-scope-root-after.log`实际重新收集同八条，
未再出现新增42P18，描述仍为原来的OID0；这些记录仍是`COLLECTION_ONLY_NOT_PASS`，
不宣称修复上述范围外的OID0或参数化分页语法缺口。最终两端完整门禁另行验收。

## 最终候选的完整运行检查点

R169 V4工作区完整58入口实际全部通过；本项完整263及共享1705均0失败。
仅source-free、无分页/排序等容器的完整标量调用中，直接$n从真实输入声明推断OID；
source coordinates关联编号，显式OID不覆盖、冲突42P08、编号缺口42P18及发布前再次绑定均保留。
原R162的27条协议类型错误消除，不执行函数体来猜参数类型。
范围外OID0和完整跨表达式参数推断仍开放；八条发现记录不计为兼容通过。
最终58入口之后，两端另严格串行重新收集原八条，均完整退出0、原始frames与旧after记录逐字节相同；
日志`r166-v4-root-scope-after.log`、`r166-v4-clean-scope-after.log`。
它只证明未新增42P18并保留原OID0，不是额外16项兼容通过。
独立干净版本亦完整58实际退出0；两端各26native、29协议、三个真实Main全部通过。
组合构建和完整日志索引见[R169](issue-review-r169-simple-query-raw-syntax.md)。
