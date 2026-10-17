# R133 时间戳二进制输入拒绝合法微秒

状态：本项修复及31入口有限验收完成，不等于原273总账或完整时间类型完成。
这是R131保留的fractional timestamp receiver缺口，不把R131的时区修复冒称已覆盖。

## 期望、实际及影响

实际OID1114/1184的binary参数为从2000 epoch计数的有符号8字节整数微秒。
合法的1µs、-1µs及其它非整秒值不应仅因有小数被拒绝。TIMESTAMPTZ仍代表UTC
时刻，TIMESTAMP仍是无时区时钟；精确往返不能借double或秒截断实现。

实际PG18.6（180006）永久266控制全部通过；冻结R132程序相同266完整执行，
204失败、实际1，均保留。接收分支原先明确拒绝`micros % 1000000 != 0`，
落入通用22P03；不是sender缺失，也不是运行已经成功后只差显示精度。

## 修改方法与永久回归

| 位置 | 方法 |
| --- | --- |
| NetworkServer.cpp的1114/1184接收分支 | 按有符号二补码数值解码，避免unsigned转signed超界的实现相关转换；先处理infinity；微秒向下拆成整秒与0..999999正余数，再用原civil formatter拼接精确六位补零/尾零裁剪的小数，1184仍明确+00 |
| timestamp_binary_microsecond_parameter_protocol_e2e_test.py | 266控制：41个整数/独立datetime向量、1114/1184、三个会话时区、负epoch/跨秒跨日/闰日/DST日期及基础civil边界、infinity/NULL、text输入对照与named rebind，逐字节比较binary结果和声明/协议状态 |

`-1µs`必须变成前一秒加999999µs；`-1000001µs`是前两秒加999999µs，不能用
向零商配负小数或直接取绝对值。所有运算为整数，没有另一次文本输入舍入。

范围仍是既有基础civil codec（1..9999），不称所有BC/大年份、timestamp精度
CAST/typmod、旧整秒heap、数组receiver或错误状态/外层消息边界已完成。R131的
UTC标记保留，其它codec不改；没有公共头或Main源变更。

## 实际证据与验收计划

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`。

- `r133-reference266.log`实际266/0；`r133-old266.log`完整266/204、实际1。
  冻结旧程序`r133-pre-fix-dbms-main`的SHA256为
  `c3deb2adf12aa29479d372ea86a1846c38fd117fa02aa0ad2f5a4ff4c96d40e7`，
  是R132验收时主工作区程序，不冒称干净接受HEAD。
- 实际正常O2重编NetworkServer一个CPP、repeat为0个，均实际0；整个266控制
  全过，包括基础civil边界、负微秒与非UTC输出，未删除控制或藏入skip。
- 最终12native/16wire/3真实Main=31个不同完整入口全0，独立检查0；正式批次
  新266再次全过。Main源未改，先核对R132的新receipt中Main/test source、头/
  flags/compiler、正常Main receipt和三份TU对象字节，再新链接57份当前O2库、
  无stubs并实际隔离执行。此次复用的是R132未变TU，不是旧R124对象，也不冒
  再次fresh编译。58 receipt/双缓存/57库字节/冻结源测试脚本终态核对0。
- 有限批次helper首版日志guard过度转义，实际退出2且未开始该批次的cache/
  链接/测试，初版空driver日志及`r133-initial-startup-status.txt`保留。只改为
  精确固定字符串匹配后重启完整批次；不把helper失败算成31通过或覆盖原失败。

正式证据为`r133-normal-build.log`、`r133-repeat-build.log`、`r133-focused266.log`、
`r133-final-driver.log`、native/protocols、三个frontend日志及
`r133-final-independent.log`。程序SHA256为
`302c030b0480d20852643bedda7c8328989fd01c366a9831da346d9942656dd6`，
cache为`d87779f01b5656069001074c9bb55417b6c0e3ac3566f17f79e8b21243c3e5a4`，
源/测试/脚本seal为
`f18432b1d0e886f56eaefcd7394753d9edeaad584fd7bc942694ccb74e17130d`。
Root头/生产flags仍739bf42b；正常增量1个CPP不冒全部58重编。
接受555MD、899native/534Python/3Main=1436，工作区558MD、902/537/3=1442
另含原六候选；558Python AST/13Bash及本地文件目标另核对。只隔离暂存本项
hunk、逐项本地commit，原56933字节候选patch及三个跳过草稿逐字节保护检查0。

不push、Actions禁用；R98/R99/R109候选与跳过草稿保留且不混入本项提交。
原273状态/勾选仍22complete/166partial/70unverified/15deferred，没有新完整
注册、真实TLS或整库sanitizer证明。
