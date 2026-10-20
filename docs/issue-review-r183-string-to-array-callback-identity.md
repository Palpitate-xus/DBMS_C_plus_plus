# R183：STRING_TO_ARRAY原生回调继承builtin数组返回声明

状态：Open，已观察、未验收。独立于R182的UTF-8拆分性能，不是R173类型删除依赖分支。

## 实际缺陷与影响

原生registerFunction("string_to_array", callback)替换为返回普通TEXT的callback后，
eval仍按函数名字推断text[]，把实际返回opaque送入数组输入codec并抛出22P02。
注册回调不含返回类型声明，不能继承被替换builtin的数组结果类型或冒称builtin identity。
元数据应由真实callback的所有者决定，不能执行callback来猜类型。

真实builtin返回text[]本身正确。R182首轮原生夹具要求内部ARRAY标记的九个失败是测试期望错误，
不是另一个产品类型bug；必须透明纠正这个期望，不能混同或删除实际opaque强转失败。

## 修改方法与验收门禁

1. 用私有callback marker标明真正builtin STRING_TO_ARRAY；只有它提供text[]返回元数据。
2. registerFunction替换不具有marker，身份为native、返回声明未知，复用已有opaque推断路径；
   元数据查询和绑定都不能执行callback，不得根据结果值猜测。
3. 保留真实builtin输出规范化、NULL/空数组/null_string、quoted/pg_catalog身份与原生callback的arity、
   volatility及TEXT/INTEGER/BYTEA返回值；不更改公共头/AST、SQL语法或输入声明。
4. 独立60项原生夹具完整记录：6builtin阳性及54native组合，两种volatility、三种名称/arity/返回类型，
   旧版完整反证、新候选/Root及全部相邻回归均需真实执行；异常计失败而不提前截断。
5. 定向验收通过并保持其他工作区字节后才按本项独立本地commit，不与R182性能修改混为完成。

目前只有未接受独立候选/tmp/dbms-r183-array-callback-identity-candidate-v1.EKOOoy62，
没有Root生产修改、新接受测试或通过声明。首轮实际异常及完整PG/旧329见[R182记录](issue-review-r182-utf8-split-linear.md)。
R180及其他暂缓不恢复，原273未完成；不push，Actions禁用。
