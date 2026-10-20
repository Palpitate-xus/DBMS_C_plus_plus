# R202：三角及双曲函数丢失可表示的 float8 次正规数

状态：此有限修复已通过独立候选与正常工作区构建复验。R201 物理表目标列表 UNNEST 已按用户要求暂缓。

## 错误与修改方法

| 表达式 | 修复前 | PostgreSQL 18.6 |
| --- | --- | --- |
| `sin('1e-310'::double precision)` | `0` | `1e-310` |
| `sin('-5e-324'::double precision)` | `0` | `-5e-324` |
| `cbrt('-5e-324'::double precision)` | `0` | `-1.7031839360032601e-108` |
| `atan2('5e-324'::double precision,'5e-324'::double precision)` | `0` | `0.7853981633974483` |

既有 float8 一元函数、边界检查 helper 及 atan2 使用 `ExprValue::asDouble()`。
其中 `std::stod` 会因可表示的非零次正规数报告 ERANGE 而抛异常，旧 fallback 把输入静默变成 0。
问题发生在数学运算前，不是结果显示精度或表存储的舍入误差。

修改方法：三处表达式复用已有严格 `parseDoubleCastValue`，保留可表示的非零次正规数。
不全局修改 `asDouble`，不改头文件、签名、回调身份、参数求值、结果格式或既有数学运算。
全局其他转换、全部数学范围错误及常量规划时机不由本项宣称完成。

## 永久回归与原失败

[原生夹具](../tests/trigonometric_subnormal_test.cpp)完整 650 项：
12 个函数 × 10 个输入 × 5 条 raw/bound/parsed/helper 路径，以及 atan2 的 12 对输入、
NULL 和平台/私有数据库检查。以独立 libm 结果比较 binary64 位，零符号不能被近似比较掩盖。

[协议夹具](../tests/trigonometric_subnormal_protocol_e2e_test.py)完整 814 项：
同组输入的 Simple/Extended text/binary、真实表与零行、Parse/Bind Describe、
保留 statement 的 text/binary 输入重执行、atan2 两个参数位置、NaN、NULL 与正负零。
复用原角度夹具的协议断言，保持 OID 701、长度 8、typmod -1、命令标签和 Ready 状态检查。
相同夹具在新建的私有 PostgreSQL 18.6 实例完整 814 项、0 失败，使用默认 fsync 并独立停止。

未修改的旧 accepted-only 和工作区库各完整执行：原生 650 项各 204 失败，
协议 814 项各 241 失败，实际均退出 1。重复控制不是 204/241 个独立缺陷。
旧结果与日志保留，不用后续绿色结果改写。

证据目录：`/tmp/dbms-r202-trig-subnormal.Bg4Kpmb9`。
原失败回执：`completed-baseline-receipt.json`，
SHA256 `2e8093cfe6af31afaca422789fcad980ec328e78540044a2f3fe9f22c5f917df`。
PG 完整对照：`/tmp/dbms-private-pg18-full-fixture.r2g4zity/completed-reference-receipt.json`，
SHA256 `cf9a4468321a53f536db039c5a70a519e7d541f31ca75e1ec1ae4ec96a5ccf7c`。

## 最终有限验收

accepted-only 与工作区候选各 14 个完整入口全部实际退出 0：7 个原生、7 个协议。
原生包括新 650、math_functions、math_projection_error、angle_conversion_range、
float8_scalar_signature、native_float8_callback_override、numeric_square_root；
协议包括新 814、float8_scalar_signature、angle_conversion_range、floating_arithmetic_width、
text_case_locale、scalar_signature、qualified_projection_routine_identity。
旧回归保持其全部断言，没有改写输入、预期、超时或持久化设置。

两侧各使用本侧单独核对的 127 个头文件、编译器和设置，各 fresh 一个生产表达式单元，
其余 57 个生产对象与 stub 为匹配源和设置的已验证 donor；不是 fresh 全 58 或 fresh Main。
每侧 7 个原生测试单元本轮 fresh，其中新 650 保留基线编译对象，全部重新链接当前库。
候选回执 `completed-candidate-receipt.json`，
SHA256 `8a43ba9804e19fd30663464840b7753e194917dd75b1dcfba312c33e28a50b90`。

仅把本项三处表达式和一条协议注册应用到工作区，保留其他未接受改动。
实际正常构建重编译 1 个表达式单元并链接，重复构建 0 编译、0 链接；
再将 7 个原生程序重新链接正常构建库，与 7 个完整协议入口复验，14 个入口全部实际退出 0。
连同两侧候选共 42 个完整运行入口通过，不是 42 个独立缺陷或全库通过。
正常构建回执 `completed-normal-receipt.json`，
SHA256 `5717d68501375c6aad46a32818a8fd4f0d89f08f8e79a2d566582707da972f50`。

独立提交只包含本项生产三处表达式、协议注册、两个新夹具和本文档。
此前暂存的 R185 18 个文件及其他来源工作区改动不纳入本项。

原 1492 冻结结果仍为 1481 PASS / 11 FAIL；没有新全量重跑或整库 sanitizer/TLS 验收。
原 273 状态保持 22 complete / 166 partial / 70 unverified / 15 deferred，总目标未完成。
R197、R185、默认协议主测试及其他用户暂缓范围不恢复；不 push，不启用 Actions。
