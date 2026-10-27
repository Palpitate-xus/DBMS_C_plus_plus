# R298：TRIM_SCALE 核查（用户暂缓）

状态：按用户最新“skip it”暂缓，不继续本项实现；普通 continue 不恢复。
本次只完成当前源码/对象/头文件匹配和工作区隔离核验。源码未找到 TRIM_SCALE
注册；未运行准备中的三侧 SQL 观察，未建立最终回归夹具，不能将源码线索写成
已复现、已修复或已验收的数据库问题。

待用户明确恢复后，可先对照 PostgreSQL 验证 numeric→numeric 的签名、有限值
尾零裁剪、NULL、NaN/正负 Infinity、结果 typmod，以及文本/二进制协议；保留
真实错误阶段和回调所有权，不以本项扩大数值运行时域。
R271 已暂缓的非零 binary NUMERIC dscale=1000 接纳问题继续保持暂缓，不能
为了让新函数测试全绿而旁路、缩小测试或改写参考结果。

没有本项生产代码、测试或注册变更。生产仍为 R297 `5c491b57`；当前匹配构建
不是全绿运行时证明：原 R297 完整协议夹具每侧仍有两处已记录的 R271 失败。
原 273 条总清单仍为 22 complete / 166 partial / 70 unverified / 15 deferred，
尚未全部完成；本次跳过不自动改变原始条目状态。

源码隔离核验收据：`/tmp/dbms-r298-trim-numeric-scale.Vw3auKMU/completed-preflight-receipt.json`。
保留原工作区和十八处暂存差异，不 push，不启用 GitHub Actions。
