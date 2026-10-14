# R54：缺失参数必须先由实际查询作用域绑定

原完整 aggregate_expression_scope 协议在 `sum($1)+1` 上得到42883，而实际
PostgreSQL 18.6 是42P02。真实异常栈显示：legacy协议结果描述符在执行前用
`inferResultType` 把未提供的参数推成TEXT，继而错误检查TEXT加整数；并非
参数已正确绑定后发生的聚合算术错误。

具体执行的SELECT/WITH dispatcher现在先用既有SQL-aware参数扫描识别真正的
`$n`，再交给正常纯binder检查原始查询作用域，最后才进入legacy结果描述。
FROM缺失、列错误仍按正常binder顺序处理；字符串、dollar-quoted字面量和注释
里的相似文本不成为参数。不是在通用binder中禁止参数，Extended Parse/Bind和
SQL PREPARE/EXECUTE保留自己的输入与类型推导机制。

## 验证

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

- 实际PG18.6新54控制全0：r54-reference.log；未修正候选54中13失败：
  r54-original.log。失败覆盖裸SUM参数、$2和非法$0，原SQL/预期不删改。
- 最终新54全0，原完整aggregate_expression_scope、prepared_global_aggregate、
  SQL EXECUTE逐次求值和参数类型identity共五个完整协议入口全0：
  r54-final-protocols-corrected.log及对应逐驱动日志。
- 控制含空/非空表、WHERE false、LIMIT0、CASE、缺表/缺列优先级，以及合法
  PREPARE/EXECUTE和真正带INT4参数的Extended Parse/Bind/Execute阳性。
- Main真实标准O2重编、链接0：r54-first-standard-build.log。没有公共头变化，
  不冒充另一轮fresh58，也不以此代替下一轮全注册验收。

首次门禁拼错SQL EXECUTE驱动名，预校验即退出、未执行测试，保留原日志；修正为
实际文件名后完整重跑五个入口。并行R55定位时的临时binder基类型诊断未纳入本项
提交；其问题及真实目录OID修复独立处理。
