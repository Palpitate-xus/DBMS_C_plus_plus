# R85：SIGN 的 NUMERIC NULL 必须保留 numeric 结果域

状态：本范围已修复。这里只修复 builtin callback 的typed
NULL结果，不宣称SIGN的完整arity、参数签名或所有用户重载已经完成。

## 问题与修改方法

真实 PostgreSQL 18.6 中，SIGN(numeric)返回numeric，SIGN(float8)返回float8；
NULL不会改变选中的结果域。原 callback 在读取参数类型前统一处理空参数和
NULL，直接返回float8 NULL。因此SIGN(NULL::numeric)、DECIMAL和numeric typmod，
以及原生RowContext中的numeric/decimal NULL，都丢失了numeric类型。
SQL wire的helper可能仍给出正确OID，不能以NULL行看起来相同代替原生值验证。

修改方法：单独保留原来的空参数行为，先识别numeric/decimal结果域，再为
NULL返回相应类型；所有非NULL符号、NaN、Infinity和浮点分支保持不变。
原生registerFunction替换不继承builtin结果或提前执行。通用签名范围仍partial。

## 原始复现与参考

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

- `r85-old-sign-native.log`：当期匹配Root原生21控制中4失败，分别为numeric、
  decimal、NUMERIC(12,2)及嵌套SIGN的NULL都返回double precision。
- 补充原生RowContext的未规范化decimal/numeric别名后，最终25控制使用R83
  冻结Root的57个历史缓存对象运行，6失败，见 `r85-old-expanded-native.log`。
  先逐个核对与原producer的SHA256相同，使用同公共头、fresh driver/stubs、
  隔离执行；这是历史差分，不当作变化后源码的当前门禁。
- `r85-sign-reference.log`：对应20个SQL表达式在真实PG18.6/180006全通过，
  核对rows/NULL、OID及ReadyI。额外四个native raw-row控制与callback替换
  控制属于C++ API，不冒称为PG协议控制。

回归保留非NULLnumeric零、负数、极小正数、NaN、正负Infinity；浮点NaN按
实际PG返回0，不能与numeric NaN统一。NULL的整数/REAL/float8输入仍返回
float8 NULL，UNKNOWN NULL同样如此。替换callback收到原numeric NULL、
绑定阶段零调用、执行恰一次，实际INTEGER NULL返回不被改写。

## 验收

标准构建及重复构建均退出0，见 `r85-build.log`：只重编ExprEvaluator.cpp，
其余57对象匹配相同公共头与各自当前源，未改公共头或借用旧树对象。
生产二进制SHA256：
`00c89d047b6e3cf91af3c7fe2d1be90f11092edbda97b0f7dd848ec0d645e984`。
最终 `r85-final-native.log`：25项新原生及十一完整native全部通过，包含
math functions/projection、numeric sqrt、R84取整310、R83签名906/回调237、
原R62签名、ABS范围、未绑定保护和常量规划；全部58份receipt及生产输入
签名首尾一致。`r85-sign-ours.log` 中对应20个SQL在实际Root协议亦全通过。

`r85-final-protocols.log`：八个完整驱动全部退出0，包含R84取整266、R83签名
951、原scalar signature、ABS范围、legacy window type、numeric sqrt、真实
取消协议和CLI超时；生产输入/binary/全部58份receipt首尾一致。这里未重跑
原完整PG、COPY或全注册组合，不将八项定向通过扩大为全量全绿。

507份Markdown、848个本地inline目标、521 Python AST、13 shell语法、501
unique注册/483 standalone E2E、文档状态、兼容性契约及六个总账单测通过。
最新注册组合为864 native + 3真实Main frontend + 501 Python =1368入口，
尚未运行这套新组合；原273总账仍22 complete/166 partial/70 unverified/15 deferred。
原冻结全量、R83完整PG超时及R84清理/COPY准备超时均继续保留，不以本项
定向通过改写。用户暂缓范围、不push、禁用Actions约束保持不变。
