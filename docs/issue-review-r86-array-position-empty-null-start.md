# R86：ARRAY_POSITION空数组与NULL起点

状态：本范围已修复。只处理真实builtin空数组与NULL起点，不宣称完整
多态函数签名、其他NULL快捷路径或元素相等语义已经完成。

## 问题与修改方法

真实PG18.6对 `array_position('{}'::integer[],1,NULL::integer)` 返回
INTEGER SQL NULL，而原callback无条件抛22004。INTEGER、TEXT、NUMERIC数组，
两种空数组表示及NULL/non-NULL needle均受影响。

只在真实builtin的NULL-start分支判断数组为空并返回INTEGER NULL；非空数组
保留原检查，非NULL start仍进入parseInt32Argument，保留22P02/22003。
不能对所有空数组提前返回，吞掉非法UNKNOWN起点；多维错误仍先检查。
不在按名字的执行入口增加短路，原生registerFunction替换保留自己的行为。
没有修改公共头、输入声明或非NULL值搜索算法。

## 旧基线、参考与夹具校正

本机证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

- 首次 `r86-old-native.log` 为208控制、45失败，其中parsed路径漏掉正式数组
  preparation，typed空ARRAY出现42P18，不能全部算成产品缺陷。首次源保存为
  `r86-first-unprepared-native-source.cpp`，SHA256为
  `c63cdb01a6e724b8f1c397de839c40091d3da8cb5a3d126bc573b5b43c16cd15`。
- 补齐 `ExprHelper::prepareArrayTypes` 后，不删typed ARRAY正控或改期待值，
  同一旧产品的 `r86-old-prepared-native.log` 为208控制、32失败，均为本项22004。
  两次使用匹配的当前Root对象、fresh driver/stubs及隔离cwd，原失败不覆写。
- `r86-old-protocol.log`：20次查询中2次失败，分别为空NULL-start组和包含
  qualified空NULL-start的普通位置组。组错误会中止整条SELECT，不冒称组内
  每个表达式都独立执行。旧binary仍为冻结27bac6ee的
  `00c89d047b6e3cf91af3c7fe2d1be90f11092edbda97b0f7dd848ec0d645e984`。
- PG草案首轮曾误期望“非空无NULL元素、NULL needle/NULL start”为22004；
  实际PG返回NULL。已纠正夹具并保留明确标记的失败摘录，不伪称完整原log。
  校正草案和最终永久驱动均在真实PG18.6通过20 SELECT/80 scalar controls；
  最终日志为 `r86-pg18-reference.log`。这些参考不创建表或函数。

## 永久回归与实际验收

新原生驱动共208控制：80条SQL分别走parsed正式数组准备和真实PreparedQuery，
27组参数datum保留类型/NULL，20个opaque替换控制及1个完整性guard。
覆盖三个元素域、NULL数组、合法/非法起点、NULL元素、quoted NULL/空TEXT、
正负/非1下界、多维错误；两种注册重载在空/真实DB context中保留原始输入
和任意TEXT结果，包括零参数替换。绑定/推断callback零执行，运行恰一次。

新协议已注册，20 SELECT/80 scalar controls检查值/NULL、成功INTEGER OID23、
command tag、ReadyI和精确错误SQLSTATE；未扩成完整ARRAY_POSITION声明门禁。

`r86-build.log`：标准及重复构建exit0，只实际重编ExprEvaluator.cpp，其余
57份对象保持匹配；生产binary SHA256为
`c0599cbccb771ee2e4d5c5194c4bf5c330de3b5add0f2a2186a700ec5c8b9c9e`。
`r86-final-native.log`：新208全0，九个完整native全0，包含原array函数、
显式下界、真实projection、array expression/concat、math、SIGN25和R83
callback237，全部58份receipt及输入签名一致。
`r86-final-protocols.log`：七完整协议全0，包括本项、显式数组下界、typed
concat、nullable character array、COPY error/lock release、真实取消与CLI
超时；生产输入/binary/全部58份receipt首尾一致。这里没有重跑原完整PG、
大COPY或新全注册组合，不将本项通过扩大为全量全绿。

## 独立未完成范围

以下是R86原先未覆盖的范围，不把R86原验收改写成已证明这些行为：
非空无NULL元素的NULL-needle/NULL-start快捷路径、NULL数组的bare UNKNOWN
start输入转换、UNKNOWN needle隐式转换及TEXT start应拒绝等另需真实多态
声明分析。numeric元素1.00与needle 1的typed相等、ARRAY_POSITIONS缺失也未修。
当前没有这些范围的完整Root基线，不能把源码/PG观察写成已验收的Root结论。

后续[R108](issue-review-r108-array-position-typed-equality.md)独立修复并验收其实际
矩阵中的typed相等、多态输入、NULL bitmap/能力顺序及用户callee，保留原R86
208/80断言不变。R86此处的“未修”是历史边界；新范围的真实验证以R108为准。
ARRAY_POSITIONS、任意复合/域及通用多态能力仍未完成，不由任一局部修复关闭。

ARRAY_POSITIONS后续另列[R109](issue-review-r109-array-positions.md)，此处缺失
描述保留为R86历史范围。共享查找实现仍保留本项全部208/80断言；binary、
构造器规划、一般复合/域等剩余能力不能据新函数存在而关闭。

[最新冻结全量](issue-full-registered-27bac6ee.md)的六个失败及捕获到的COPY
超时保持未解决。新组合为865 native +3真实Main frontend +502 Python =1370
入口，本项定向验收不是新组合全量通过。原273项22 complete/166 partial/
70 unverified/15 deferred及用户暂缓范围不变；仍独立本地commit、不push，
GitHub Actions工作流保持禁用。
