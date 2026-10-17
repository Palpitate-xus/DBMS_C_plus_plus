# R118：时态视图物化错误沿用VARCHAR存储布局

状态：本范围已修复并完成有限验收，按本项独立本地提交，不提升原273项状态。

## 问题、原因与影响

源表DATE/TIME/TIMETZ等列可正常读取，但视图SELECT在实际物化行时返回
XX000 could not materialize view query，全NULL结果也失败。外层WHERE false
不一定避开视图体物化；仅Parse/Describe正常或真正空的源表正常，不能据此
声称实际读取可用。最初R114额外19条观察中已保留该独立问题。

Main的createTempTableFromRows仅为整数选择真实工厂，其余类型都先构造
VARCHAR(65535)，再改dataType字符串。DATE/TIME等标记改变而物理宽度和
isVariableLength仍为VARCHAR，插入行时不符合真实codec契约。

隔离的原生布局观察逐项证实：三个旧布局均可建表，但NULL插入返回12
（DBStatus::INVALID_VALUE）；真实DATE12字节、TIME4字节、TIMETZ4字节工厂
布局NULL插入均0。六条采集完整退出0不是旧布局行为通过。它使用匹配57库、
fresh stubs并在全局初始化前切换隔离目录，未直接运行Engine二进制。

初始150条完整协议控制：真实PostgreSQL18.6（180006）全部通过，固定旧
3d4a2767程序44失败、实际退出1。扩大到TIMESTAMP/TIMESTAMPTZ的186条后，
参考仍全部通过、同一旧程序60失败、实际1；不忽略额外两个时态类型。

## 修改方法与范围

物化内部时态列使用既有类型工厂，与实际heap codec对齐，不只替换类型名称；
字段名和可NULL性保留。只修复内部临时结果的物理描述，不修改用户存储格式、
不迁移已有表行，不改变纯声明的PostgreSQL逻辑长度。

整数、数组、INTERVAL和其他既有分支保留。该函数的视图/派生表/CTE消费者
都需相邻回归；不把本项解释为通用view rewrite、lazy demand、完整时态精度、
视图origin或用户暂缓的CTAS/物化视图专项完成。

## 永久测试与证据

`tests/temporal_view_materialization_protocol_e2e_test.py`：186条完整控制。
覆盖DATE/TIME/TIMETZ/TIMESTAMP/TIMESTAMPTZ、INTERVAL和INTEGER[]对照，
真实源表、各单列视图、组合/嵌套视图、NULL字面值视图；源表空/全NULL/实际
DATE和整秒TIME值，以及外层无行结果，分别检查Simple、纯Describe、真实
Bind/Execute的行值、NULL、OID/逻辑长度/typmod/格式与完成tag/Ready。
视图origin为独立已记录问题，未在此新测试中冒充修复，也未删除既有失败记录。

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：
`r118-old-layout-observations.log`、`r118-reference150-initial.log`、
`r118-old150-initial.log`、`r118-reference186.log`、`r118-old186.log`。
最初六条原生诊断源码`r118-temporal-schema-observer.cpp`后来扩为十条，
后续结果须按各自日志/产物登记，不能说初次六条已测过五个时态类型。
固定旧程序`r118-pre-fix-dbms-main` SHA256：
`3d4a2767eb40b7f594b599ec32f0a7b8cd33603df1b341d6b8747e4aa11a527a`。
它含保留候选，不是clean HEAD。原五种类型的值/NULL/metadata检查继续保留，
另加两个时间戳类型，没有把旧失败改成预期成功或忽略TIMESTAMP差异。

## 验收边界

首次候选仅修DATE/TIME/TIMETZ五行，normal/repeat各重编1/0CPP、实际均0，
binary为efbf8048be544d7d87dec7f875fbf76a399bac908e73458df5e9847ee917c360；
该候选186控制仍36失败、实际1，未冒充最终绿色。随后10条匹配库/隔离原生
观察确认全部五种旧布局NULL插入12、真实工厂NULL插入0，时间戳工厂宽度均8；
实际结果见`r118-library-layout-observations10.log`，不是初次六条已测过五类。

第二版补齐TIMESTAMP/TIMESTAMPTZ，Main共新增9行、公共头不变。其normal/repeat
重编1/0个生产Main CPP、实际均0；既有缩进/unused/optional编译warning保留，
不是本项失败。三个真实Main驱动直接从实际测试文件编译/链接57份匹配普通库、
不用test_stubs；仅explain/session测试TU按既有规则O0，window测试TU与生产
Main均O2。编译准备本身无DB测试，之后在初始化前切隔离目录、串行运行全部
三个真实驱动，不调用被改名的程序入口，也不复制被测函数体。

最终冻结输入下，10个完整native、21个完整协议、3个真实Main共34个不同
入口全部实际退出0。含本项186、真实view/替换/typed参数、catalog691、R114
的102/28、R115的228、R113的171、数组/interval/NULL、派生表/CTE/递归/
LATERAL/集合操作/VALUES/DML CTE/RETURNING及默认完整协议等。
`r118-final-{driver,core,native,neighbor}.log`、逐协议目录和三个frontend日志
保留，`r118-real-main-compile.log`区分编译0与实际运行0，不借旧驱动或历史
1391全量替代本项验证，也未缩减旧断言/超时。

最终binary：
`746c947cb8edba15e29d9a226dac9e05d49b4079ed363be6cd7240f095f9a5ad`；cache：
`0ef52884a83f9fc32f645d64c64be2cddb221008ad5f6847dc41a0942fd06da8`；
src/tests/scripts seal：
`75662aafe2149fd51cd1b8acd58c51f88924a3dd88a6e0c5c8994e9e74cca12b`。
双stamp、全部58生产receipt、57测试库字节、冻结输入/公共头终态核对0；
两个候选各仅重编生产Main，不声称新编58份。

工作区541MD、544Python AST、13Bash语法/本地内联目标及状态/注册检查0，
原273一致性0、require-complete真实1。本项精确15路径：Main的9行工厂hunk、
一个协议测试/注册和文档；接受538MD/890native/520Python，组合1413。
工作区893+3+523=1419含保留候选，未获得新的完整注册回归。原56933字节补丁
重建后的源码与三个暂缓草稿逐字节核对0，R98/R99/R109代码不混入。

## 补充观察及普通待做项

另用两个整秒非NULL时间戳做8条参考/旧程序观察，两侧采集退出0。旧源表值
与实际参考一致，但普通视图XX000、派生表42P01（把`(select`当关系名）、
WITH源42P01（把x当物理表）；旧并非8项都通过。最终候选8条也完整采集0，
逐条比较行值/NULL/SQLSTATE/列名/OID/完成tag无差异，比较器退出0。
三处旧差异均随同一布局修复消失；没有额外修改SQL源路由，不能把原次级错误
武断登记为新的已确诊路由缺陷。日志`r118-coarse-timestamp-{reference,old,current}.log`
和`r118-coarse-timestamp-comparison.log`保留，但不等于所有查询图/错误传播已证明。

| 范围 | 后续方法 |
| --- | --- |
| 一般查询图/物化失败传播 | 本组3差异已消失；其他形状仍须按真实AST/作用域与原始错误诊断，不把本组复测等同通用rewrite完成 |
| TIME微秒/24时binary与超微秒规范文本 | 见R117的32条观察/16差异，真实微秒sender和输入规范化分别补齐，不粗改旧4字节行格式 |
| 目录SQL/视图origin | 保留R114方法，由真实目录源及声明来源传播修复，不靠输出名字猜测 |

原273仍22complete/166partial/70unverified/15deferred。R117 TIME微秒/24时
binary与超微秒规范文本、目录SQL和视图origin等仍普通开放，不自动暂缓。
R98/R99/R109候选、R106草稿和其他明确暂缓范围保留；不push，Actions禁用。
