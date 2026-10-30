# R99：多行函数字面量破坏存储函数元数据（未修复）

状态：普通函数正确性问题，已复现、尚未修复；不是用户暂缓项。R98的消息通道
修复不能据此宣布本项完成，后续应单独实现、验证和本地commit。

## 实际反例

固定旧产物dc5371b9（R98第三候选）接受CREATE FUNCTION与CREATE TRIGGER，但
下面合法PL/pgSQL函数在触发器实际调用时失败，返回42601/unterminated quoted
string；外层INSERT被回滚，随后count(*)为0。

```sql
CREATE FUNCTION body_line() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'first
second'; RETURN 7; END $$;
```

原始外部驱动为`/tmp/dbms-review-r46.ifu6SWLp/r99-diagnostic-channel-probe.py`，
Root日志r99-diagnostic-current.log。第二个WARNING多行字面量同样失败。原完整
12检查/10失败、实际退出1；后续源表累计行数断言也因前两次失败而不同，不能
称为10个独立bug。使用等价RETURNS trigger包装的PG18.6同驱动完整12/0失败、
实际退出0，参考为r99-diagnostic-reference-12.log。文件路径仅为本机辅助证据。

另一个永久诊断通道驱动以`RAISE NOTICE '%', E'first\nsecond'`在运行时构造换行。
这让存储body保持一个物理文本行，原函数可执行，再独立验证消息是否完整传输。
该不同反例不能替代本项真实字面量中的换行控制，更不能把成功叫作本项修复。

## 已确认的调用链

两个createUDF重载将PARAMS2参数行、expression原文、volatility、language、
RETURNS及STRICT以换行串接。getUDF却只用一次getline读取info.expression，
下一次getline就当volatility，随后依次读取language等字段。expression中的
合法LF会截断函数体，也使后续字段错位；不能靠压平所有空白修复，因为字面量
内的LF本身就是SQL值的一部分。

这不涉及函数权限、SECURITY DEFINER/INVOKER或认证专项。应保留真实声明类型、
语言与volatility/strict等普通执行属性，不能把该问题扩大为暂缓整个函数族。

## 修改方法与验收计划

1. 使用明确版本及长度前缀存储完整函数体/参数/属性，或提供同样无歧义的记录边界；
   读取必须验证长度、尾随内容和属性，不能继续按body的物理行数推断字段。
2. 保持合法旧单行记录可读；评估有完整已知footer的旧多行记录是否可无歧义恢复，
   不能猜测已丢失内容、修改用户函数值或把损坏记录伪装成健康元数据。
3. 添加普通SQL及PL/pgSQL函数：真实LF/CRLF、引号内/外换行、空串、文本NULL、
   quoted/dollar body、两种createUDF入口、重载/replace、重启后完整属性与结果。
4. 测试截断、错误长度、缺失/额外footer和失败后旧定义保持；并保留本次原始
   CREATE成功/调用42601、rollback及PG正向控制，不提高期限、不重试到绿。

目前没有接受的产品修复或永久验收通过；原273项总账状态不变，不push，Actions禁用。
