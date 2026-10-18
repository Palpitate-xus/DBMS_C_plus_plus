# R147：INTERVAL月份、天数和微秒的应用顺序

状态：完成有限修复与独立终态核对（实际0），仅按本项18路径独立提交。
实际PG943、当前冻结Root组合的48个完整入口及新干净组合10个入口均通过；
原生2043、完整943和类型依赖552均为零失败。R148依赖已独立提交f59a1098，
本项只提交日历值修正，不重新提交类型头或其它待定候选。

## 问题与修改方法

| 普通SQL | 旧Root实际结果 | PG18.6结果与修正后结果 |
| --- | --- | --- |
| TIMESTAMP '2024-01-30' + INTERVAL '1 month 1 day' | 2024-02-29 | 2024-03-01 |
| TIMESTAMP '2024-03-31' - INTERVAL '1 month 1 day' | 2024-02-29 | 2024-02-28 |
| 2024-01-30 23:59:59.999999加1月1天1微秒 | 2024-03-01 00:00:00 | 2024-03-02 00:00:00 |
| 混合正负月份/天数、跨年及世纪闰年 | 月底截断发生在错误日期，正反运算均可算错 | 对原日期先改月份并截断到目标月底，再加天，最后加微秒与跨日进位 |

根因是[ExprEvaluator.cpp](../src/expression/ExprEvaluator.cpp)中timestampShift先把
iv.days加到日期，再调整月份。两者在月底截断时不可交换。
修正仅移动天数应用位置，保留月份截断、__int128中间值、微秒向下拆分、
24时输入规范化和既有有限civil域检查，不改schema、heap或索引格式。
DATE结果仍提升为timestamp；本项timestamptz日历证据限定UTC，不冒充完整DST语义。

## 逐版证据与依赖

| 完整输入 | 实际结果 |
| --- | --- |
| 最初14条Root/PG采集 | 确认日期值错误；采集0不表示产品行为通过 |
| 实际PG18.6/180006，最终943协议 | 全部通过，实际0 |
| 冻结Root519efdaa、已接受ef39c0b7，同943 | 各507断言失败，均完整执行/实际1 |
| 旧2043原生 | 890断言失败，完整执行/实际1 |
| 第一版日历值候选Root c1、clean d9 | 原生2043全过，但943仍各有11个类型/二进制断言失败 |
| 第一版完整入口 | Root35为34通过/1失败，clean9为8通过/1失败，两批实际1 |
| R148类型依赖 | 已独立提交f59a1098；147原生/552协议、Root48与Type-only clean14通过 |
| 最终本项 | 沿用同一冻结Root48通过帧；新的accepted-base干净组合4native+6wire=10全过，2043/943及552均0 |
| 独立终态核对 | 旧红/新绿、同帧48、干净10、来源与原工作区28路径逐项核对通过，实际0 |

507/890包含同根因的多个类型、模式和后续DML检查，不是相同数量的独立bug。
首版11个失败ID为284、287、288、601、602、915至920，全部保留：
NULL/DATE参数结果声明错误，以及实际结果被错误编码为interval而报0A000。
先按[R148](issue-review-r148-temporal-arithmetic-result-types.md)独立修类型，
再验收此项，没有删减943中的任何原SQL或期望。

最初参考执行943个控制均匹配，但预计总数误填946，脚本因此实际1；
核实每模式有23个表/DML控制后，只改总数常量为943，SQL和行为断言不变。
第一版Root门禁也曾在运行前因外部Main输入核对漏识别#define包含路径而实际2；
原日志保留，更正核对器后完整35入口实际1，不能把未运行或整批红算绿。

## 永久回归

[interval_calendar_order_test.cpp](../tests/interval_calendar_order_test.cpp)共2043控制：
9个日期、5种月份位移、5种天位移、DATE/TIMESTAMP/UTC TIMESTAMPTZ、
加/减/交换加法共2025，加8个微秒/24时/复合表达式、6个NULL和4个既有域控制。
预期用测试侧独立的月份截断及libc UTC日历归一生成，不调用生产shift/codec。

[interval_calendar_order_protocol_e2e_test.py](../tests/interval_calendar_order_protocol_e2e_test.py)
共943控制：每模式270个日期表达式，加微秒、NULL、真实text Bind和表数据；
检查Simple/Extended text/binary的精确字段、类型、长度、typmod、format、
原始binary、tag和Ready。实际TEMP表覆盖UPDATE RETURNING、INSERT SELECT、
保存点撤销、提交后读取和错误事务恢复，参考对象均清理。
交换加法中的显式结果CAST是原夹具契约；未加结果CAST的类型归属由R148另行覆盖，
不能把首版注释中的独立类型缺口当作当前仍未修复。

原生SHA256：
7b31fc6d10d3233abaa51fa8f2ee6135eef9bd31391af87143db927abbe78b2e。
协议SHA256：
b6ecb02796b6beb1c02ef25bd938c930b11e2b1e3fa1ac8ce630e9b12c9f8a11。

## 构建与同帧验收

最初Root正常重编1个Evaluator O2/repeat0，第一版clean为1个fresh Evaluator
及核对来源的57个生产donor；这些只是仍有11失败的历史候选，不作最终验收。
R148共享头改变后，Root与Type-only clean均重新fresh编译全部58生产O2对象、
repeat0、三真实Main TU fresh O0/O2/O0，并新链接当前57库、无stubs、隔离实际执行。
此时Root已经含本项待定日历值修正，Source625帧的48完整入口全部通过。

R147最终阶段Root的源码、夹具、头、全部生产对象/测试库及三Main输入未变：
正常build实际零重编，binary/cache和当前58receipt/57测试库字节再次核对。
这里明确复用同帧48入口证据，没有声称再次运行48入口或再次fresh编译Main。

新干净版本以f59a1098为基，只加本项两个partial源/注册路径和两个新夹具；
fresh正常O2编译Evaluator，链接57个逐一核对所有头、flags、CPP、原58个receipt及
前后对象SHA的R148 clean生产donor（含真实Main），server无test stubs。
这是1 fresh CPP+57匹配donor，不是新的normal fresh58，也没有伪造cache stamp。
四个原生测试另有fresh TU/stubs及匹配57库，均在全局初始化前隔离执行。
新旧candidate/donor的全部tracked文件和src/tests/scripts从构建起永久冻结。

Root binary：
474c64cb3793d5590db618b293681a0502e2cb3472290911da22cb36641a47b1。
Root cache：
0adb19bb72fbe95a27663215bc020a99d47d915da721624778fd8a28294d7910。
Root源封印：
6257082a5e7b215ce1511b701720a822dd7483f89f9f6f88694b97c29c171f86。
新clean binary：
4150f8a2454d5ea90ecf12e88b708565edb09f3d087cf79b1648c97411f40fa8。

## 文档与范围

本机/tmp/dbms-review-r46.ifu6SWLp/保留R147/R148的reference、各版完整失败、
构建、fresh Main、逐入口、原始working.patch及边界核对记录；
R147首版另存r147-v1-candidate-before-type-dependency.tar.gz。
不改写原候选、旧pin或失败日志，不把类型依赖通过冒成本项独立clean通过。

本项接受570MD、905native+544Python+3Main=1452；
工作区573MD、910native+549Python+3Main=1462仍有原十个未接受入口。
没有新1452全注册、真实TLS或整库sanitizer证明。
完整civil/BC/infinity/错误域、命名时区日历/DST、TIME/TIMETZ运算、全部类型/执行入口
仍开放；本项有限值回归不关闭整个TYPE-06/FUNC-01或原273。
R146主键、R137外键、R140时区语法及其它用户暂缓不重开。
原273及22complete/166partial/70unverified/15deferred不变；不push，Actions禁用。
