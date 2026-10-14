# R13：逻辑预览保留 NULL 与逐列边界

原 INSERT/UPDATE/DELETE 捕获将各列直接以 `|` 拼接。SQL NULL 与空字符串
产生同样字节；`('a|b','c')` 和 `('a','b|c')` 也无法区分。插件只为整行
添加长度，不能补回捕获阶段已经丢失的信息。

## 修改方法

新行像使用 `DBMS_TUPLE_V1:` 前缀和 JSON 数组，每个元素是
`[实际存储类型声明, nullable 文本值]`，按当前表 schema 顺序排列。例如：

```text
DBMS_TUPLE_V1:[["int","1"],["text",null],["text",""]]
```

`null` 是真正 SQL NULL；非 NULL 值一律是 JSON 字符串，不将 `NULL`、
数字样式或空字符串重新解释为类型/NULL。类型来自实际列 metadata，不从值
猜测。引号、反斜线和控制字节转义，多字节 UTF-8 保留；分隔符只是值的一部分。
这不是 PostgreSQL pgoutput 或 contrib/test_decoding 的协议格式，不承诺
完整 relation OID、typmod、collation、DDL/schema 演进或实际流复制。

三种 DML 都使用各自 OLD/NEW 行的实际 NULL bitmap。UPDATE 的 OLD 和 NEW
分别绑定，DELETE 在 TOAST 被删除后消费已解析的 OLD 图像。相同编码进入
普通提交和 prepared transaction 保存/完成链，回滚仍不发布。
两个现有插件透传编码行像；手工/历史 opaque 行仍原样透传，不伪造恢复旧值。
原完整驱动仅有三条新存储捕获的字节断言随格式版本更新，旧 opaque 插件断言
及所有 publication、过滤、事务、slot 控制保留。

## 原始证据与验证

证据位于 `/tmp/dbms-review-fix-donor.N9sdas6f/`。原 `80fe342e` Main 冻结为
`root-before-r13.frozen`，SHA-256：
`b9e749f8082f9d7e01abe5a0bf30b92d049ce35adcedb32597174674c9ae2f9f`。

- 原新 native 64 个捕获/插件/事务检查：61 失败、exit1，见
  `r13-original-runtime.log`。另保留首次测试缺少嵌套 SqlRow 限定的编译错误
  `r13-original-native.log`，该日志不是产品运行时证据。
- 最终 wire 53 个控制在原程序完整运行，50 失败、exit1，见
  `r13-final53-original-protocol.log`；先前 44 控制、41 失败日志也保留。
  首次默认严格模式拒绝项目专有 CREATE REPLICATION SLOT 的日志也保留；
  测试显式设置 extended 模式后才执行项目预览接口，不更改产品语法合约。
- 新 native 增加独立 codec 控制，wire 以标准 JSON 解码器核对逐列值，而非
  仅检查是否含 INSERT。覆盖 NULL/空串/字面量NULL、分隔符、引号、反斜线、
  换行和控制字符、UTF-8、多次不确认 peek、确认、事务提交和回滚。
- 大字段 native 覆盖 20,000 字节 TOAST 在 UPDATE/DELETE 的 OLD/NEW 图像，
  prepared commit 保留特殊值；与原完整逻辑解码驱动相邻复验。

首次候选 wire44全过，但扩展的 native77 与 wire53 分别剩三处虚拟生成列
空字符串错误。`extractColumnValue` 的虚拟列路径已收到 buffered NULL bitmap，
却没有将该 bitmap 识别为已知 NULL 来源，仍以空字符串猜 NULL。将真实 buffered
metadata 纳入其来源判断后，三个操作都保持 length('')=0、coalesce('','fallback')
=''，而真正 NULL 仍正确传播。两份三失败候选日志原样保留：
`r13-initial-native57.log`、`r13-final53-first-candidate-protocol.log`。

Root `root-r13-fresh58-build.log` 真实重新编译全部58个生产翻译单元并链接0；
虚拟列后续修复由 `root-r13-final-standard-build.log` 真实重编 TableManage
并标准链接0。当前全部58份源码/header/flags receipts 与 Main stamp 相符。

最终新 native77 全通过，十二完整原生驱动全exit0：编码、原逻辑解码、
publication成员/写入前保护、replication/并发、prepared transaction、
生成列/索引/INSERT trigger、table-owner atomicity、catalog snapshot；
见 `r13-final-native.log`，终态输入签名不变。
最终新 wire53 全通过，七完整协议驱动全exit0：编码、slot持久化、DIV14、
RETURNING OLD/NEW、transition binding、RETURNING子查询和完整postgres_protocol；
见 `r13-final-protocols.log` 及 `r13-final-*_test.py.log`。
477唯一注册/460 standalone e2e与466份Markdown UTF-8/内联本地文件链接通过。
不将本项当作全部注册回归或273项功能总账完成。
