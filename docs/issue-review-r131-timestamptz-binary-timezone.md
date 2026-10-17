# R131 TIMESTAMPTZ二进制输入被会话时区再次解释

状态：本项修复及29入口有限验收完成，不等于原273总账或完整时间戳接收完成。

## 期望、实际与影响

OID1184的8字节二进制参数表示从PostgreSQL 2000 epoch计算的UTC时刻，不因
会话TimeZone改变。普通TIMESTAMP（1114）则保留没有时区的时钟值。相同输入
请求二进制结果时，有限整秒值/正负infinity/NULL应原样往返；text输入已有
明确时区的TIMESTAMPTZ也不应被重复套用会话偏移。

实际PG18.6（180006）永久158控制全部通过。干净8f程序及R130结束时冻结的
主工作区程序分别完整执行相同158，各45失败/实际1：非UTC时区的有限1184值
及named rebind的时刻错位；1114、UTC、infinity、NULL及text输入对照保留。
这是binary接收器问题，不把它当作二进制sender、存储或所有时间精度问题。

原接收器把UTC计数格式化为不带时区的timestamp文本，再交给真正的TIMESTAMPTZ
CAST；后者按会话本地时间解析，产生第二次偏移。时区相关计算与字段描述符
看起来合法，客户端却收到不同的UTC时刻，可能影响时间条件与往返数据。

## 修改方法与永久验证

| 位置 | 方法 |
| --- | --- |
| NetworkServer.cpp | 只为有限1184接收值的中间文本附上明确+00；1114、既有infinity早期分支、NULL、其它codec及外部消息边界不改 |
| timestamptz_binary_timezone_protocol_e2e_test.py | 158控制，六个真实会话时区、1114/1184、正负epoch/跨日/闰日/DST日期、infinity/NULL、text对照、named Parse/Describe/跨时区rebind，精确核对8字节、OID/长度/format、tag和协议完成状态 |

该修复不增加fractional timestamp receiver，也不升级整秒heap、全部输入域、
大年份或错误状态优先级；不把局部codec修改当作整个TYPE-06完成。没有公共头
或Main变更，不重开用户已暂缓的专项，也不混入R98/R99/R109候选。

## 实际证据与后续

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`。

- 最终`r131-reference158.log`为158/0；`r131-clean-old158.log`和
  `r131-root-old158.log`分别158/45、实际1。旧Root程序另存
  `r131-pre-fix-dbms-main`，SHA256为
  `62888353d361d91c08bcfbf0474ea5594f69edb2e34949227b9b59242c37da72`。
  干净程序使用8f全量的1e305110，不把两种来源混称干净HEAD。
- 首版使用合法`SET TimeZone TO 'zone'`，实际PG158全过，但干净旧程序六个
  SET全报22023，后续仍在默认UTC，不能作为receiver复现。日志分别保存为
  `r131-reference-initial-set-to158.log`及`r131-clean-old-initial-set-to158.log`。
  最终本项改用等价且已支持的`SET TIME ZONE 'zone'`建立时区、PG重新158/0、
  旧程序才158/45。原TO语法拒绝单列普通开放问题R132，不改记用户暂缓，也不
  把替换setup说成修好了TO语法；方法是让SET的真正语法节点消费TO/=/LOCAL等
  角色，验证错误阶段、当前值和事务恢复后独立修复/提交。
- 实际正常O2重编NetworkServer一个CPP、repeat为0个，均实际0。最终12native/
  14wire/3真实Main=29个不同完整入口全0，独立检查也0；新158协议完整全过。
  Main的未变R124测试TU/source/header/flags/compiler/对象字节核对后，新链接
  57份当前O2库、无stubs并实际隔离运行三个前端，不冒Main TU重新编译。
  58 receipt/双缓存/57库字节/冻结源测试脚本的终态核对均0。
  没有新1434完整注册、真实TLS或整库sanitizer证明。原8f完整三失败与R129/
  R130各自有限修复仍按原证据读取，R51保持暂缓。

正式日志为`r131-normal-build.log`、`r131-repeat-build.log`、`r131-final-driver.log`、
`r131-final-native.log`、`r131-final-protocols.log`、三个frontend日志及
`r131-final-independent.log`。程序SHA256为
`4ed66b3fbea69226b8372b8e59d387abffca867b8272725c87b0ddd76fedfc7d`，
cache为`529c9c8fbc0c9d26ced653e66ab77a783b8c897b4600d71b3cc5fdd0c39b9b22`，
源/测试/脚本seal为
`b734dfdba5b094dcf9e4f6b365f551b1d573f1ee7463e228e0ed6900698be7de`。
Root头/flags签名仍为739bf42b；正常1个CPP不冒全部58 fresh重编。

接受553MD、899native/532Python/3Main=1434，工作区556MD、902/535/3=1440
另含原六候选；556Python AST/13Bash及本地文件目标另核对。只隔离暂存本项
hunk、独立本地commit；原56933字节候选patch及三个跳过草稿逐字节保护检查0，
不提交R98/R99/R109，不把R132改成用户暂缓或修复完成。

原273统计/勾选仍为22complete/166partial/70unverified/15deferred。
不push，GitHub Actions保持禁用。
