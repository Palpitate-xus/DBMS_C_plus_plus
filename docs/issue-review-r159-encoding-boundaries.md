# R159：hex、escape 与 base64 编码边界（开放）

状态：实际复现、未修复，不能以现有编码测试通过关闭本项。

| 实测 SQL | PG 18.6 | 旧 Root / accepted-only | 修改方法 |
| --- | --- | --- | --- |
| SELECT encode(decode('6 1','hex'),'hex') | 22023 | 61 | 只在完整 byte pair 前跳过允许的空白，两个 nibble 必须相邻 |
| SELECT encode(decode('61'\|\|chr(11)\|\|'62','hex'),'hex') | 22023 | 6162 | 仅允许空格、tab、LF、CR；不能用 isspace 放行 VT/FF |
| SELECT ascii(encode(decode('07','hex'),'escape')) | 7 | 92 | escape 仅对 NUL 与大于等于128的字节输出八进制，反斜杠加倍；保留非零 ASCII 控制符及 DEL |
| SELECT length(encode(decode(repeat('61',58),'hex'),'base64')) | 81 | 80 | 按 PG 规则每76列换行，验证恰好边界及尾部规则 |

三端分别完整执行原 20 条诊断中的这四条，结果见
/tmp/dbms-review-r46.ifu6SWLp/r158-{reference,old-root,old-clean}-discovery.log。
这是观察记录，不是四项通过。临时表只用于同一连接的私有诊断，没有业务数据修改。

[实现](../src/expression/ExprEvaluator.cpp)的 hexDecode 先删除全部 isspace；
base64Encode 不换行；escape 分支对所有小于0x20或大于0x7e的字节转义。
[既有测试](../tests/encoding_functions_test.cpp)只检查 byte pair 之间的空白，
且 BEL 的预期本身锁定了错误的八进制输出，需要按实际参考纠正并保留该用例。

应分别增加全字节 escape 往返、合法/非法空白位置、空串/NULL、混合大小写、
奇数 nibble、base64 的57/58字节等行边界及多行、padding、真实表、参数与协议原始 datum。
修复前后保留完整参考和失败证据，逐根因提交，不通过全局放宽 bytea 校验绕过。
对照 [PG encode.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/encode.c)。

## 仅登记的类型目录静态线索

以下尚未运行专门对照，均未修复，不能视为已验证 bug；记录以避免丢失审查发现：

- `_record` OID2287 的 bootstrap category 为 P、typelem2249、storage x；
  regtype_output 只按 category A 判断数组可能漏掉真实 record[]。
  需核实 PG 的真实数组/storage 判定，再验证目录、呈现及数组往返，不混入 R151 自动枚举数组。
- parser 的 TIME/TIMESTAMP WITH TIME ZONE 声明语法归约为通用 timetz/timestamptz，
  可能丢失 SQL 标准语法的内置身份，被同名用户类型遮蔽。
  需分别验证标准语法、普通别名、引用与限定声明；不能通过全局固定通用别名消除正常遮蔽。
  这不是用户暂缓的 R140 TimeZone 设置值语法。
- TypeRegistry 宣告 oid[] 为1028，但 builtinTypeRows 缺少对应实际目录行。
  需核实普通声明、pg_type 双向链接、参数/NULL/数组 codec 及持久化身份，不能编造 OID 替代。

这些线索不构成恢复用户暂缓的 R156 类型呈现/载体专项的授权。
总清单仍未完成；不 push，Actions 禁用。
