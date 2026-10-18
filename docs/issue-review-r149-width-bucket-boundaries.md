# R149：WIDTH_BUCKET 的边界、精度和 int4 溢出

本项修复四参数 width_bucket 的两种数值重载，属于 TYPE-01、TYPE-02、FUNC-01、
P0-16 的有限进展，不代表这些总清单项完成。用户已跳过的专项不重开。

## 问题与修改方法

| 问题 | 冻结旧程序的实际结果 | PostgreSQL 18.6 / 修复后 | 修改方法 |
| --- | --- | --- | --- |
| 倒序起点和分桶边界错误 | width_bucket(10,10,0,5) 为 0；输入 8 为 1；输入 0 为 5 | 分别为 1、2、6 | 不交换上下界后反转桶号；按原方向判断包含起点、排除终点并计算距离 |
| 丢失 NUMERIC 精度 | 9007199254740993 在 9007199254740992 到 9007199254740994 的两桶区间误入 1；1e-400、1e400 的有限边界被当成相等 | 三例均为 2 | 先按声明选择 numeric 重载，使用统一 scale 的十进制整数距离、乘桶数、精确整除 |
| int4 结果越界未拒绝 | width_bucket(10,0,10,2147483647) 成功返回 2147483648 | SQLSTATE 22003 | count+1 按 int32 上限检查，不再检查不可达的 int64 上限 |
| float8 边界使用更高精度导致不同结果 | width_bucket('0.3'::double precision,0,1,10) 为 3 | 4 | float8 重载保持 binary64 运算顺序；极大有限跨度溢出时先将各输入除以 2 |
| 未声明实际重载 | bigint 桶数进入 int4 输入解析；错误类型可被当成数据执行 | bigint 桶数无匹配签名，42883 | 只为真实 builtin 声明 numeric/numeric/numeric/integer 与 float8/float8/float8/integer 两个签名 |

这些边界和方向遵循 [PostgreSQL 数学函数文档](https://www.postgresql.org/docs/18/functions-math.html)。
同时逐段核对本机 PostgreSQL 18.6 的 float.c/width_bucket_float8、
numeric.c/width_bucket_numeric 和 compute_bucket，并在实际版本号 180006 的参考库执行测试；
不是根据本项目当前输出生成预期。

## 实现范围

[ExprEvaluator.cpp](../src/expression/ExprEvaluator.cpp) 的私有实现负责下列变化：

- numeric 路径先精确比较端点，再计算非负距离。复用十进制乘法/整除，补充无符号加法；
  不经过 double，也不先用会按显示 scale 舍入的 Numeric 除法。
- 距离的临时整数不套用最终 Numeric 值的 1000 位限制，避免合法端点的跨符号差值或
  scale 对齐临时变宽时误报溢出。输入本身仍受项目现有 numeric 域限制。
- float8 路径按自身精度计算，保证有效范围内结果在 1..count；
  NaN、无限端点、相等端点、非正桶数仍为 2201G，允许无限 operand。
- 重载表参与静态绑定、NULL、零行及运行时转换；专属 callback 标记限定声明归属。
  通过 registerFunction 替换同名原生函数时，不强加 builtin 的参数个数、转换或结果提示。

本项没有修改 Main、共享头、schema、heap、索引格式或安全检查。

## 两处旧测试预期的校正

新增声明表后，第一轮完整 Root 36 入口实际为 34 通过、2 失败，clean 10 实际为
9 通过、1 失败；原日志和退出码 1 保留。失败分别发生在
[math_functions_test.cpp](../tests/math_functions_test.cpp) 和
[math_projection_error_test.cpp](../tests/math_projection_error_test.cpp) 的旧 22003 预期。

实际 PostgreSQL 的五条独立控制确认：

| 输入 | 正确 SQLSTATE | 原因 |
| --- | --- | --- |
| 第四参数为未加引号的 2147483648 | 42883 | 该常量是 bigint，没有 bigint 桶数重载 |
| 第四参数为 9223372036854775807::bigint | 42883 | 同上，不能暗中当作 int4 |
| 第四参数为字符串 '2147483648' | 22003 | UNKNOWN 转换到声明的 int4 时越界 |
| 第四参数是合法 int4 最大值且结果须为 count+1 | 22003 | 真实 int4 结果溢出 |
| bigint 桶数调用附加 WHERE false | 42883 | 静态声明检查不能依赖是否有结果行 |

仅校正两处旧预期，保留原来的拒绝断言，并给投影测试新增 quoted int4 输入溢出控制。
不改变生产实现去迎合错误预期，也不删除失败控制。新专项已独立覆盖真实 int4 结果溢出。

## 永久回归

[原生测试](../tests/width_bucket_boundaries_test.cpp) 有 515 项控制：
249 个结果/NULL/SQLSTATE、249 个独立元数据检查、5 个非法签名，以及三个同名 opaque
原生替换场景的解析、执行、结果声明和身份检查，共 12 项。两方向、整桶边界、int4 最大桶数、
超过 2^53 的 numeric、1e±999、异号与宽临时距离、float8 极大跨度、次正规数、特殊值均有覆盖。
旧原生实现完整执行 515 项，其中 78 项失败；最终 Root、clean、定向 UBSan 均为 515/0。

原生夹具在旧版执行后仅去掉一个文件尾空白行；基线副本与最终夹具加一个换行字节完全相等，
没有修改任何 SQL、断言、预期或控制数。

[协议测试](../tests/width_bucket_boundaries_protocol_e2e_test.py) 有 804 项控制：

- 249 个数值场景分别运行 Simple、Extended text result、Extended binary result，共 747 项。
  整数/numeric 的预期来自独立有理数公式，float8 场景另外在参考库核对。
- 三模式下非法签名、错误输入、WHERE false 和 LIMIT 0 共 24 项。
- 声明 OID 的文本 Bind 与真实 binary float8/int4 Bind 共 14 项。
- TEMP 表建表/插入和三模式真实列/空结果共 8 项。
- BEGIN、SAVEPOINT、失败 UPDATE、失败查询、ROLLBACK TO、未变行值、提交与会话恢复共 11 项。

检查 int4 的 OID 23、长度 4、typmod -1、Statement/Portal 格式、
原始四字节网络序或文本 datum、显式 NULL、command tag 和 Ready 状态；
不把 binary 数据按 UTF-8 解码。

实际 PG 804/0；冻结旧 Root 和只含已接受代码的旧 server 各完整 804 项、200 项断言失败、退出 1。
修复后 Root 和两个冻结 clean 候选的 804 项均完整通过。五条 SQLSTATE 参考控制另为 5/0。

常规入口：

~~~sh
bash scripts/build.sh
bash scripts/build_one_test.sh width_bucket_boundaries_test
DBMS_MAIN="$PWD/dbms_main" python3 tests/width_bucket_boundaries_protocol_e2e_test.py
~~~

原生构建脚本会在全局初始化之前使用隔离工作目录；协议驱动自行创建、清理所属 server 和临时目录。
参考运行可配置 PGREF_HOST、PGREF_PORT、PGREF_USER、PGREF_PASSWORD 后增加 --reference18，
驱动会先验证实际参考版本；不要把密码写进仓库或日志。

## 构建、完整入口与输入归属

最终 Root 18 native + 15 protocol + 3 real Main = 36 个完整入口全部通过。
独立 accepted-base clean 的 6 native + 5 protocol = 11 个完整入口全部通过，
包含两处旧测试、同名 callback、GCD/LCM、日历间隔顺序及时间算术结果类型等相邻控制。
各入口有序、唯一、保留实际退出码，不凭日志片段算通过。

Root 第一次正常构建只有一个 ExprEvaluator 生产 O2 TU，重复构建为 0；
两处旧测试校正后再正常构建仍为 0、重复为 0，生产 binary/cache/全部 58 个 receipt 未变。
本项 Main 和所有头/编译参数均未改变，因此核对 R148 的六项源输入、
全部头/flags 签名及三个原生对象字节后复用真实 Main 测试对象，
重新链接当前 57 个 O2 生产库，无 test stubs，并实际隔离执行三个 driver。
不声称本轮重新编译了 Main TU 或 fresh58。

最终 clean 来自已接受 69dee2b0，只有四个局部文件修改和两个新夹具：
Evaluator、注册表、两处旧数学测试，以及新原生/协议测试。
独立 fresh Evaluator O2，加 57 个来自 R148 genuine-fresh58 生产 donor 的对象，
在编译前后核对全部头、flags、57 个未改 CPP、原 58 个 receipt 及对象 SHA；
真实 server 不链接 stubs，不伪造 normal cache stamp。
所有旧/新 clean 和 donor 的已跟踪文件永久冻结。

另外将改动的 Evaluator、515 项 native TU 和 stubs 用 O2+UBSan 单独编译，
链接其余 56 个未插桩生产库，在隔离目录实际执行 515/0，没有 UBSan 诊断；
这不是整库 UBSan 或 ASan 证明。

输入边界复核：

- 仅逆向本项四个局部 source/test/registry 变化，逐字节恢复原 2177 个 src/tests/scripts 文件
  和上轮 Source625；新夹具只有两个，当前输入共有 2179 个文件。
- 在独立临时索引重建开始前的 28 个 dirty tracked 文件，逆向本项文档/源码/夹具变化后完全相等；
  原 R98/R99/R109/R137/R140 候选及 R146 草稿保留、不纳入提交。
- 第一版 source 检查器因归档中原仓库跟踪、却被新副本 .gitignore 排除出索引的一份
  scripts/__pycache__/soak_client.cpython-313.pyc 而退出 1。第二版明确限定这唯一项，
  核对该文件与基线的精确字节，并继续检查所有归档文件；没有在构建后改副本或重做索引。
- 第一版串行汇总读取器误用 Root 的 ENTRY 标签读取 clean 行而退出 1；
  各实际 PG/新 clean/UBSan 子任务退出 0，最终独立核对使用正确标签。
  原退出 1、脚本及日志保留，不用覆盖日志或删断言制造全绿。

原始日志、receipt 和检查器保存在本地 /tmp/dbms-review-r46.ifu6SWLp 的 r149-* 文件中；
专项文档与永久夹具不依赖这些临时文件存在才能理解或重新复现问题。

## 总账与未完成范围

已接受 571 份 Markdown、906 native + 545 Python + 3 Main = 1454 个入口；
工作区还含未接受候选，为 574 份 Markdown、911 + 550 + 3 = 1464。
静态复查逐份检查 Markdown UTF-8/本地文件目标及清单，572 份 Python AST、
13 份 Bash 语法与文档/注册/原总账一致性检查通过。

原 273 项及其 checkbox 仍为 22 complete、166 partial、70 unverified、15 deferred；
require-complete 仍实际退出 1，不把本函数的有限修复当成总账完成。

二参数 width_bucket(operand, thresholds-array) 仍未实现；
本项没有宣称完整 PG numeric 域、所有函数重载/命名参数、完整 binder 或全部 FUNC-01 完成。
没有运行新的 1454 全注册套件、TLS 环境或整库 sanitizer。
历史 ecf632e9 的 1436 入口终态仍是 1433 通过、2 失败、1 TLS 环境 skip、退出 1。
用户暂缓项保持未完成；本项只做本地独立 commit，不 push、不启用 Actions。
