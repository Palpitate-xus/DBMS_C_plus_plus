# 干净提交ecf632e9的完整注册回归

状态：进行中，1436为预期入口数，不是实际通过数；尚无完整终态。
原273总账未完成，不把本轮构建开始或各项有限验收当作整库全绿。

## 冻结范围

目标提交`ecf632e9bfd756e9266317b009a29802d044eefb`，包含独立提交的
[R129](issue-review-r129-native-array-projection-types.md)、
[R130](issue-review-r130-native-result-hint-precedence.md)、
[R131](issue-review-r131-timestamptz-binary-timezone.md)、
[R132](issue-review-r132-timezone-set-to.md)及
[R133](issue-review-r133-timestamp-binary-microseconds.md)。

干净detached工作树`/tmp/dbms-clean-r133.BTRQCUGk`，证据目录
`/tmp/dbms-full-r133.stgQVl08`。开始时间为2026-10-17 10:23:26 UTC。
全部已跟踪输入初始封印为
`0ad4e0783c7e1a59a8306525143459c911ddaf45f48114eae7715fa0e6f5eacb`。
主工作区原有候选源码/三个MD及六个运行入口、未注册draft均不复制进来。
不删除或提交用户已跳过的R109及R98/R99未接受候选。

当前新工作树compile签名为
`8c8fe094583501824c98a09b12668b3db1c6c467c4debed304c7b5d4b0cdb5cd`。
该签名还包含绝对header路径，不能跨工作目录只比较签名就猜测ABI改变。
8f至ecf的已跟踪公开头未改变；旧8f工作树的0b2签名仍只对应它当时的路径/输入。
本轮独立测量签名/receipt，不能复用Root739、R132测试TU或旧clean目录的对象
冒充本轮fresh构建。此文在Root记录，不改变干净工作树的任何tracked文件。

## 执行计划

1. 在本目录从零正常O2编译全部58个生产TU，重复构建验证无CPP重编。
2. 按本轮签名验证58 receipt，把57份匹配生产对象用于测试cache；当前头下
   fresh编译stubs，验证双缓存及57份对象字节，不借未核对对象或假stamp。
3. 运行原始`scripts/build_tests.sh`，串行899native、3真实Main和534注册Python，
   共1436个不同完整入口；显式将DBMS_MAIN指向该干净目录程序。真实Main全部
   fresh TU编译，explain/session测试TU按既有O0，window及生产对象按O2；
   不链接stubs，实际运行均在全局初始化前配置好隔离目录。
4. 运行期间冻结干净工作树全部tracked输入及Root的src/tests/scripts，不修改
   运行driver；数据库批次不并行。Root仅可记录文档，所有原候选字节保持。
5. 保留每个失败，不以compile/link错误、缺失入口、未执行或环境skip当通过。
   TLS若打印未构建OpenSSL而skip，需从PASSED退出中扣除并单列，不冒真实TLS。
6. 独立核对1436个ID的顺序/唯一性/完整性，重新核对HEAD/status、全部tracked
   输入、程序/双缓存/58 receipt及预记录57份库字节。完整运行的真实失败仍
   使runner/最终检查器返回1，不把输入核对0摘出来当行为全绿。
7. 将新发现保存为期望/实际/影响/方法/永久回归/版本记录，普通问题逐项修复
   与本地commit。若落在已暂缓R51的DROP路径，仅登记，不重开或删除断言。

## 当前实际阶段

干净工作树已核对HEAD、空git status及预期899native/534Python/58生产/57库。
正常O2构建已启动，首个生产TU为Main；未报告任何生产构建成功、完整入口通过
或最终结果。重复构建、cache准备、1436标准入口及终态核对仍待真正到达。

`driver.log`、`normal-build.log`已实际产生。`repeat-build.log`、
`cache-preparation.log`、`pre-full-fence.log`、`full-regression.log`、
`driver-summary.log`及`post-full-fence.log`只在对应阶段实际执行后才成立。
进度工具输出PROGRESS_ONLY，不是终态，也不能把预期1436当观察数。

## 历史与未完成范围

[旧干净8f完整回归](issue-full-registered-8f0465fc.md)实际1431入口、1427实际通过/
3失败/1TLS环境skip。其F8F-02和F8F-03分别由R129/R130的有限门禁修复，本轮
还需验证干净接受组合；F8F-01的统计断言之前已通过，失败发生于DROP清理，
R51仍按用户原要求暂缓，具体历史同根因没有证明。本轮不预设其必过或必败。

原273统计/勾选保持22complete/166partial/70unverified/15deferred；即使本轮
全过，也不补齐旧整秒heap、其它未实现/未验证功能、全部PG边界或用户暂缓项。
没有整库sanitizer或真实TLS证明，不push，Actions保持禁用。
