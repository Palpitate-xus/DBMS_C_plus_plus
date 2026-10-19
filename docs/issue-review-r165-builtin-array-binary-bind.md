# R165：五种内置数组二进制Bind输入（有限完成）

状态：int2[]、int4[]、int8[]、text[]、varchar[]完成独立有限修复与验收。
其他数组输入、R164清理问题及原273总账不由本项关闭。

## 实测错误与根因

[R162](issue-review-r162-generic-stored-signatures.md)完整1705项协议矩阵在PG18.6全0。
组合Root及独立干净候选各77失败，其中44项是既有INTEGER[]/TEXT[]二进制参数的
Bind/Describe及重复执行：返回0A000，信息为binary parameter type is not supported。
相同数组的文本参数、正确声明的NULL等控制仍保留，不能用文本通过替代binary输入支持。

[NetworkServer](../src/network/NetworkServer.cpp)的binaryProtocolParameterLiteral
支持多个标量OID，但没有数组输入分支；数组落入default，尚未到存储函数调用。
这不是R162的声明匹配错误，也不涉及用户暂缓的枚举数组工厂。

## 实现与保留范围

仅接收1005/1007/1016/1009/1015五个既有内置数组身份，按真实元素OID复用对应元素codec；
保留维数、各维长度及下标，正确区分SQL NULL元素、字符串NULL和空字符串。
检查消息长度、元素类型、元素长度、形状及尾随内容，不将用户数组冒认为内置数组。
零维空数组、最多六维、非1下标、文本引用/逗号/反斜杠、UTF8、重复Bind及text/binary
结果描述均保留。声明类型、实际NULL及元素边界先于SQL文本生成，不执行元素SQL。
bool/float/numeric等其他数组binary输入仍开放；本项不造catalog、用户数组或新OID。

证据：`/tmp/dbms-review-r46.ifu6SWLp/r162-v3-{root,clean}-results/`
下的generic_stored_signature_protocol_e2e_test.log；retained_6和retained_7共44失败。
完整1705控制、首轮失败及冻结源/对象记录均保留。原273总清单没有因此关闭。

## 独立验收与版本范围

[完整协议夹具](../tests/binary_builtin_array_parameter_protocol_e2e_test.py)固定1852项：
五类型各27个合法样例、text/binary输入/输出、metadata、反向保留重绑、118个坏帧、
错误后同statement重试、事务回滚。结果数组转TEXT，不能当成新实现了binary数组输出。
实际PG18.6完整1852/0；组合Root及clean也各1852/0。

另从已接受194c027e独立投影，仅添加本项一个Network CPP自有hunk、该夹具及注册，
不含R162/R163/R164/R166/R161/R158或原先的其他工作区改动。
该独立clean完整1852/0，六个协议入口及三个真实Main入口共9项全部退出0。
Network实际fresh1 O2，其他57生产对象逐一核对原始fresh58来源、全部headers、flags、
CPP及对象字节；服务器不链接stubs。Main源码未变，复用已核对输入与字节的R152真实TU，
新链接当前57库并隔离运行，不声称新编译Main TU。
首次Main链接检查误把聚合source receipt当成单文件SHA，在链接前退出1；原日志保留，
改用原收据的明确src/main.cpp SHA后，实际三次新链接和三次运行均0。

组合V6整轮Root/clean各29入口仍实际退出1，仅R164完整956各四项清理失败；
独立本项9项通过不把组合整轮改写为通过。
证据：`r165-solo-clean-{build-driver,main-link-v2,gates}.log`及
`r165-solo-clean-results/`，均位于`/tmp/dbms-review-r46.ifu6SWLp`。
本次只纳入本项源码/注册/1852夹具与文档；不push，Actions保持禁用。
