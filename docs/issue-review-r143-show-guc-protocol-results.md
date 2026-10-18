# R143：普通SHOW的列描述缺失与值损坏

后续[R142](issue-review-r142-quoted-set-parameter.md)在本项独立提交基础上完成quoted
参数名有限修复；原Meta-only程序544/216失败继续保留作其旧红，不改写本项来源。
R142最终15/clean6含本项150回归全过；本页290前端仍为本项原输入，不改作298。

状态：完成有限修复。Root14完整入口、clean5、150专项及独立v2核对均实际0。
仅修复application_name、
client_encoding、search_path、lc_monetary四个已实现的标量SHOW。
R142参数名修复仍为待定候选；R140、R137及其它用户暂缓项不恢复。

## 问题与方法

| 问题 | 已确认事实与有限方法 |
| --- | --- |
| Extended Describe返回NoData | 实际PG返回一列text(OID25)，旧版无列描述却在Execute返回值；按四个已实现SHOW的固定形状声明metadata，不执行SHOW或采样当前值发现类型 |
| 空值丢行 | application_name为空时旧版零行，PG一行空字符串；Main发布结构化单行结果 |
| 字面量NULL变SQL NULL | 旧版把字符串NULL按输出标记解码，PG是普通四字节；显式false空值位保留数据 |
| 换行被拆成多行 | 双引号路径名称含换行时旧版两行，PG一行完整值；不再由stdout文本重建协议行 |

## 证据与候选回归

实际PG18.6/180006最终150控制全0；冻结旧Root61db3add及已接受干净878094b7
各完整150/111断言失败/实际1，保留原始旧红，失败不是111个独立根因。
协议候选tests/show_guc_protocol_e2e_test.py检查15组值，每组10个完整请求：
Simple/Extended-text/Extended-binary、大小写SHOW、Statement/Portal Describe-only，
精确列名/tableOID/attnum/OID25/长度-1/typmod-1/格式、原始单行字节、tag与Ready。
Describe-only不得出现DataRow/CommandComplete，不能执行查询或函数发现形状。

值覆盖printable ASCII应用名（空/NULL/引号/ERROR前缀），以及直接双引号路径标识符
里的换行、tab、UTF8和双写双引号。它不把application_name的非ASCII/控制字符规范化
或search_path的Sconst列表语义也宣称完成，另见下方普通开放项。

## 构建、来源与验收

本项Main四个私有SHOW块、Network一处固定形状及一条注册，不改公共头/布局/heap。
正常生产2CPP(Main/Network) O2/repeat0实际0；三个真实Main测试TU均fresh编译，
explain/session沿用基线O0、window O2；来源为已接受0c172ab2夹具仅投影Main路径，
实际执行原290次session控制，不运行R142待定8控制及R140待定21控制。
当前57个匹配生产O2库无测试stubs新链接并隔离运行。

干净0c只加本项Main/Network/注册/150夹具，排除所有R142实现和测试；
fresh O2 Main、Network加56个逐一核对CPP/头/58原receipt/SHA的R136生产donor，
不是fresh58。全部tracked与旧donor保持冻结。最终4native+7protocol+3真实Main=14
有序唯一完整入口全0，clean5全0；原R141的389控制完整重新执行通过。
独立v2逐一核对所有入口完整性、旧红/新绿、四Main/测试源、三fresh对象/新链接、
57匹配O2库、58当前receipt、56donor字节、二进制、冻结候选与暂缓字节，实际0。
不把有限门禁当新1445全注册、真实TLS或整库sanitizer证明。

## 另行登记的普通开放项

| 项目 | 实测事实与后续修改方法 |
| --- | --- |
| R144应用名规范化（用户暂缓） | PG将控制字符/非ASCII转ASCII转义，旧项目保留原字节；用户要求跳过，未修改实现或执行专项验收。重新开启时需核对SET、startup、SHOW/current_setting与ParameterStatus，在赋值边界统一规范，不能仅在SHOW掩饰 |
| R145路径列表语法 | 后由[R145](issue-review-r145-search-path-value-identity.md)独立有限修复：raw AST逐项list-quote，保留Sconst身份及FROM CURRENT事务赋值；本项SHOW历史范围不回写 |

21条应用名、10条Sconst路径与10条直接标识符观察保留在本机证据目录，
其退出0是采集完成而非兼容通过。R144后续已按用户要求暂缓，未修复；
R145后由其独立提交有限修复，不属于本项SHOW已修复范围；其它GUC语法仍开放。

## 检查器记录与产物

首次独立检查器实际1：多行SQL标签使单行正则漏数，部分标记正则又过度转义；
原文件和失败日志保留。新v2按每个控制ID的完整块核对150项及旧111失败/新0，
只改本机检查器，不改产品、协议夹具、门禁或期望。最终v2实际0。
R142初始完整性检查器也保留转义错误的1；更正版本真实确认14/5完整、失败总态1。

最终Root binary为`907709970334ae3284bc6d88a0b771b778facb9ee8af54d75b884f69cf46bb92`，
cache为`6f13509754b596e531084ff40f9dfcdd6a98bb5477945e323b79a279e7c66c3d`，
src/tests/scripts封印为`97bf8523795dd2fdb837bd175f75da032a20106447c2ea732514bdbbdb0ba52d`。
clean binary为`153dc4958a4faf485f3a110bfa10b7823b4b72d1d885ac175d04d81632dc1273`。
本机证据在`/tmp/dbms-review-r46.ifu6SWLp/`：reference150、两份冻结old150、
normal/repeat/freshMain、final14/clean5、首次失败和独立v2日志均保留。
本轮有限产物不替代原始已接受完整1436的两失败/1TLSskip记录。

后续依赖探针使用同一R142完整544夹具：当前Root（还含R142候选）全过，
仅本项的干净Meta程序仍216失败/实际1，证明参数名问题未被本项修复或接受。
这两个额外入口不计本项14/clean5；R142应基于本项独立验收、另行提交。

## 版本边界

R142初稿完整Root14为13通过/1失败，clean5为4通过/1失败，544中136项是SHOW
metadata失败；独立完整性0、失败总态1均保留。本项先独立提交，不混入R142
两个Main块、native8、544夹具、注册、草稿文档及inventory行。
原始56933字节补丁、三份草稿和所有用户暂缓候选字节单独保护。
已接受565MD、902native+540Python+3Main=1445；工作区569MD、
907native+546Python+3Main=1456含十一项未接受入口，不能当作已接受全量。
原273仍22complete/166partial/70unverified/15deferred；不push，Actions禁用。
