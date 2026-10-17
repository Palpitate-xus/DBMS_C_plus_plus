# 干净提交8f0465fc的完整注册回归

状态：进行中，尚无完整终态；1431为预期入口数，不是已通过数。
没有新的完整回归完成结论，也不计原273总账全部完成。

## 冻结范围

目标提交`8f0465fc144d961020315bea346cc6fcad61dc56`，包含独立提交的
[R126](issue-review-r126-time-precision-grammar.md)、
[R127](issue-review-r127-time-excess-precision.md)及
[R128](issue-review-r128-time-binary-parameter.md)。各项有限验收及提交前后检查
仍只证明其对应范围，不用工作区组合替代本轮干净提交证明。

临时干净工作树：`/tmp/dbms-clean-r128.WKnzNc54`；证据目录：
`/tmp/dbms-full-r128.OOGk9Ifa`。所有已跟踪输入初始封印为
`0f2989ea3d0faa8eccd013b860d06c4efdeaac4284bd79e192ec894bff44311d`。
启动时间为2026-10-17 06:02:01 UTC；以`driver.log`的实际阶段为准。

主工作区原有未提交候选不复制进该工作树，也不覆盖或删除；其三个原生、
三个注册Python入口及另一个未注册draft不属于本轮1431。用户已要求跳过的
专项不新增实现或复审，Actions保持禁用，不push。

## 执行计划

1. 在干净提交上从零正常O2编译全部58个生产TU，再做重复构建；不复用主工作区
   含待确认头变更的对象或R124的真实Main测试对象。
2. 核对全部58 receipt，将57份匹配生产对象用于测试缓存，并按当前干净头fresh
   编译stubs；双缓存/来源/对象字节分别验证。
3. 标准`scripts/build_tests.sh`串行运行898原生、3真实Main、530注册Python，
   共1431个不同完整入口。链接引擎的原生程序经初始化前隔离器运行。
   真实Main中explain/session测试TU为O0、window及生产代码为O2，不链接stubs。
4. 运行期间冻结该干净工作树全部已跟踪输入、产物及缓存；保留每个失败，
   不把未执行、编译失败、缺失入口或环境skip算作实际通过。
5. 终态独立核对1431个ID的完整性、唯一性、顺序及实际状态，重新核对输入、
   二进制、双缓存、58 receipt和57份测试库字节。TLS若环境skip须单列，
   不冒真实TLS验证；没有整库sanitizer证明。
6. 将确认的新问题登记为期望/实际/影响/方法/永久回归/commit，再逐项修复和
   独立本地提交。普通未完问题不改记为用户暂缓。

## 当前结果

尚无1431入口的完整运行终态；失败清单和实际通过/skip数量待运行后填写。
`normal-build.log`、`repeat-build.log`、`cache-preparation.log`、
`pre-full-fence.log`、`full-regression.log`、`driver-summary.log`及
`post-full-fence.log`仅在对应阶段实际到达后才成立，不把预定文件当现有证据。

原273项目及勾选仍为22complete/166partial/70unverified/15deferred；
完整注册回归即使全过，也不自动补齐未实现功能、所有PG边界或暂缓专项。
