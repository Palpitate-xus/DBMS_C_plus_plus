# R165：内置数组二进制Bind缺少输入编解码（开放）

状态：已复现，未验收，无生产修复提交。

## 实测错误与根因

[R162](issue-review-r162-generic-stored-signatures.md)完整1705项协议矩阵在PG18.6全0。
组合Root及独立干净候选各77失败，其中44项是既有INTEGER[]/TEXT[]二进制参数的
Bind/Describe及重复执行：返回0A000，信息为binary parameter type is not supported。
相同数组的文本参数、正确声明的NULL等控制仍保留，不能用文本通过替代binary输入支持。

[NetworkServer](../src/network/NetworkServer.cpp)的binaryProtocolParameterLiteral
支持多个标量OID，但没有数组输入分支；数组落入default，尚未到存储函数调用。
这不是R162的声明匹配错误，也不涉及用户暂缓的枚举数组工厂。

## 修改方法与验证要求

只接收实际已支持的内置数组身份，按真实元素OID复用对应元素codec；
保留维数、各维长度及下标，正确区分SQL NULL元素、字符串NULL和空字符串。
检查消息长度、元素类型、元素长度、形状及尾随内容，不将用户数组冒认为内置数组。
零维空数组、多维、非1下标、文本引用/逗号/反斜杠、重复Bind及text/binary结果描述均需验证。
错误SQLSTATE及边界应由完整PG夹具确认，不能凭猜测替换预期。

证据：`/tmp/dbms-review-r46.ifu6SWLp/r162-v3-{root,clean}-results/`
下的generic_stored_signature_protocol_e2e_test.log；retained_6和retained_7共44失败。
完整1705控制、首轮失败及冻结源/对象记录均保留。原273总清单没有因此关闭。
