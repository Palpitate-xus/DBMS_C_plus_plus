# R166：普通函数协议参数OID未按选中签名回填（开放）

状态：已复现，有限候选未验收，无生产修复提交。

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
