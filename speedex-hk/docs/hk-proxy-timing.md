# HK 出口测量：proxy 侧打时间戳（HK 口径端到端）

> **状态：已上线运行**（2026-09-04 初版；2026-09-05 review 多轮修订）。本文现行段描述
> **当前实现**（addon：`deploy/hk-proxy/speedex_hk_timing.py`，安装/单元：
> `deploy/hk-proxy/install.sh` / `speedex-mitm.service`；EU 客户端：
> `scripts/lib/hk-timing-client.mjs` + `scripts/lib/batch-egress.mjs`）。版本事实以代码
> 常量为准：`ADDON_VERSION`（当前 `2026.09.12-head-v4`）经 `/health` 与 timeline 自证——
> 某节点实际部署版本只信该节点 `/health` 回包，不信本文。备选方案、工期估算与 NDJSON
> 落盘等草案内容已移文末「附录 A：历史方案」。

目标：批次走 HK 出口时，报告的「端到端时间」反映 **HK 机器视角**（HK proxy addon 观察到
下单请求 → 观察到平台回应）——均为 **hook 观测点**（request 完整读取 / WS 帧到达 addon
的 hook 入口冻结时刻），**不宣称物理离站/抵站**——而不是 EU 本机视角：否则 EU↔HK 的
网络段（实测 RTT ~231ms 量级，见附录）会把所有指标污染掉。

2026-09-11 head-v2 修复：order 请求在链未知时保持 pendingBind，不预先标 unbound；有效回执实证链后再按原 manifest 容量绑定。窗口关闭仍无链、容量不足或身份冲突继续缺失，不按观察顺序填写授权轮。旧已关闭窗口不改写。

## 1. 指标口径（HK-L1a′ / HK-L3′）

与测量契约对齐的 **独立 cohort：不与 EU 口径混排、不参与排名**（[`../plugin.md`](../plugin.md) §0.2）：

- **HK-L1a′**：HK proxy addon 观察到「下单请求」（hook 入口冻结）→ 观察到「平台成功
  信号」（WS 帧到达 addon）。锚点只取 HK 侧时间戳——**单一钟域**（HK 本机
  `time.monotonic`），比 EU click↔平台 WS 的跨钟域还干净。
- **HK-L3′**：同一出站锚 → 链上回执首观察（`status=1` **且块身份齐全**，R24 门：EVM
  必须 `receipt.transactionHash≡本腿 hash` 且带 `blockHash`/`blockNumber`；Sol 必须带
  `slot`——缺块身份 = 证据不足，L3 留 null +
  `incomplete="receipt-missing-block-identity"`，不猜）。receipt 由 addon 对**腿锁定的
  hash** 直发链 RPC 轮询（HK vantage 自有 RPC，每腿至多一个 poller，0.8s 间隔、90s
  超时）——是 observer latency，不是出块时间。
- EU 段（click→请求到达 HK proxy）不进 HK 口径算术；`/mark` 不接受也不存储
  EU 点击时刻——跨域换算本就不做。

## 2. 支持矩阵（拦截可观测 vs 透传不可观测）

运行事实以 `speedex-mitm.service` 的 `allow_hosts` 为准：

```
allow_hosts=^(.*\.)?(turnkey\.com|padre\.gg|binance\.com|fomo\.family|relay\.link)(:\d+)?$
```

| 平台 | 代理处置 | HK 交易观测 | 出站锚（下单请求） | 入站锚（成功信号） |
| --- | --- | --- | --- | --- |
| Binance | **拦截** | 可观测 | POST 路径含 `/bapi/defi/v2/private/wallet-direct/web-dex/place-order`（按 path 匹配、不限 host）；锚 = 请求体 `clientOrderId` + ack `data.orderId` / `data.clientOrderId`（**分键**，r8）+ `data.signature` 等同节点 hash | `nbstream.binance.com` / `web3-stream.binance.com`（及 `data-stream.binance.vision` / `stream.binance.com`）帧：`bizKey∈WEB3_DEX_*_ORDER_CHANGE\|DEX_ALL_ORDER\|marketOrderInfo` 且 `content.status==="FINISHED"`，正向等锚 `orderId`/`clientOrderId`；PENDING 帧的 `orderTxId` 只是 hash 首见（PENDING≠成功）。nbstream 为二进制信封 → utf8 后取第一个平衡 JSON |
| Padre | **拦截**（turnkey.com + padre.gg） | **暂不可观测（无独立锚）**——r8 PX-01 | 仅有 POST `api.turnkey.com/public/v1/submit/sign_raw_payload` 可作出站时刻（下单走 WS msgpack，方法串未钉死）。**原因**：Turnkey 真实响应是 ActivityResponse `{activity:{id,organizationId,status,type,intent:{signRawPayloadIntentV2:{signWith,payload,…}},result:{signRawPayloadResult:{r,s,v}}}}`——`activity.id` 是签名活动 UUID（非订单号）、`intent.payload` 是签名摘要（非 tx hash）、`r/s/v` 是签名分量，**不含任何订单/交易身份键** → 腿无独立锚 → 任何 DONE 帧都无法正向匹配 → sign 腿永不晋升。r7 及之前的整-body 正则把 `activity.id` 写成 `orderId`，让 DONE 帧「锚冲突」或永无正向匹配（历史 24 份 HK timeline 中 125 条 padre 腿全部 `no-order-observed`，零条 sign 腿获证据） | DONE 门保留（`backend3/backend/backend2/txn-service.padre.gg` `_multiplex` msgpack：`txnStatus==="DONE"` 且**同节点**合法 hash + 新鲜源时间 + 独立锚正向匹配）但**今日无锚可匹配**。未来可观测性只能来自 Padre 自有下单通道（client → `_multiplex` msgpack 请求帧）经**真实脱敏帧**验证后提供的锚——不作推测实现，不放宽关联门 |
| FOMO | **拦截**（fomo.family + relay.link） | 可观测（F-SOL 例外，见下） | POST `prod-api.fomo.family/swaps/v2`——**预览/下单共用，L7 不可区分**：腿先标 `anchorRole:"preview"`（不计 attempted、不消费 manifest 占位、不单独产成功） | `ws.relay.link` `request.status.updated` 且 `data.status==="success"`，正向匹配 `requestId≡relaySwapId`（Relay 只对已执行请求推 success）= 执行证据，命中即原子晋升 `"order"`；fill hash=`data.txHashes[0]`。**F-SOL 同链腿无 Relay 帧**（DFlow 轮询，[`fomo.md`](fomo.md) §7）——HK-L1a′ 恒 incomplete（known limitation） |
| OKX | 按实例 captureIdentity；HK1 实验已拦截 | RH 已观察到合格腿；其他链待验 | schema-known 独立订单锚 | 同订单严格成功与精确 tx 回配 |
| GMGN | 按实例 captureIdentity；HK1 实验已拦截 | RH 已观察到合格腿；其他链待验 | schema-known 独立订单锚 | 同订单严格成功与精确 tx 回配 |

2026-09-10 现场：仅 HK1 实验实例已验证 GMGN/OKX RH 采集；生产与 HK2 当时仍旧五族范围。不能以服务名或提交标题推定拦截能力，必须按当批 captureIdentity 判定。仍透传的平台
在 manifest 登记后只能以占位腿 `incomplete="no-order-observed"` 出现——**不是失败**。
Padre 虽被拦截，但因无独立锚（上表），manifest 占位同样恒为 `no-order-observed`，其 sign
腿关窗后 `no-execution-proof`、`evidence.anchor.keys=[]`——这是**结构性缺失**，不是该批
Padre 下单失败，也不是 HK 观测到 Padre 更慢。
dashboard 最近任务详情的全占位链组标注规则（RHF-15 最终规则，2026-09-08）：**本节
静态矩阵只是当前能力描述，不得用来解释历史批**——每行（platform×chain）的原因只来自
当批能力证据，逐行各自解析（非页面级文本检索）：① 本批采集 degraded
（degraded/degradedReason 在场）→ 显示降级原文，不标「非失败」；② 本批该平台其他链
已有 hook 观测（任一非占位腿带锚/时间戳/hash——拦截能力已证，C04）→ 「具备拦截能力
但未观察到下单请求，原因待核实」；③ 本批持久化的**采集配置身份 `captureIdentity`**
（addon 自报的当批实际 allow_hosts 模式集，见 §5/§8）→ 该平台宿主在当批白名单内
=「当批已拦截（具备观测能力），未观察到下单原因待核实」，不在 =「当批透传——HK
交易不可观测（非失败）」——当批身份证据，不是当前矩阵反填；④ 皆无（纯历史批，无当批
能力快照）→「**未知 / 无当批能力证据**」——不作透传/结构断言、不标「非失败」，绝不拿
当前 unit 的 allow_hosts 反填历史批（切换解密模式后当前配置更不能解释旧批）。
`captureIdentity` 由 addon `_collection_identity()`/`/health` 自报（消费侧接线已通：
`hkTimingEnvironment` → sidecar → HK_TIMING DTO → UI）；**addon 未实装/未部署该字段的
实例与全部历史批一律落 ④**。addon 代码里保留了
okx/gmgn 的信号规则（`RULES`），但透传下 hook 不触发，规则不会命中——若未来改
allow_hosts 拦截这两家，必须先重验产品路径无损再谈观测能力（单次连通观察不写成
永久能力结论）。`/latency` 对五平台的 RTT 探针（§7）是网络延迟探测，**不是**
HK-L1a′/L3′ 交易观测。

链回执（L3′）与平台拦截无关：addon 自持链 RPC 表（`CHAINS`：bsc/robinhood/solana，
与 `configs/chains` 对齐）直发轮询，不观察平台经代理的 RPC 流量。

## 3. 观测语义、clock 与 method

- **hook-observed（R21）**：`tOrderOutMs` / `tFirstRespMs` / `tSuccessPushMs` /
  `tReceiptMs` 均为 **hook 入口冻结的 request-observed 时刻**——「代理观察到请求/帧」，
  不宣称物理离站/抵站；解析与判定耗时**不计入**。
- **单钟域**：全部时长在同一 HK 本机 `time.monotonic` 钟域内计算
  （`hkL1aMs = tSuccessPushMs − tOrderOutMs`，`hkL3Ms = tReceiptMs − tOrderOutMs`）。
  `atWall`（腿级）与 `hkClockAt`（timeline 拉取时刻）是墙钟 ISO，仅作对照。
- **单位**：`*Ms` 字段一律**毫秒**（monotonic×1000），`schemaVersion=2` 起生效。
  历史 v1 批存的是 monotonic **秒**（曾致 UI 把 0.6s 显示成 1ms）——读端换算规则：
  `web/recent-tasks.js` 对 `schemaVersion<2`（缺省按 1）×1000 展示。
- **method**：timeline 自带 `method="hook-observed@v2"`——静态方法版本，标注 hook
  观测点语义；method 串变化 = 观测点语义变化。

## 4. anchorRole 与锚纪律

腿级 `anchorRole`：`"order"`（默认）/ `"preview"`（fomo `/swaps/v2` 预览腿——不计
attempted、不消费 manifest 占位、不单独产成功；Relay success 正向匹配即原子晋升
`"order"`）/ `"sign"`（padre `sign_raw_payload` 签名腿——与 preview 同律：不计
attempted、不占授权槽，DONE 帧正向匹配执行证据时原子晋升 `"order"` 并补槽绑定；
**r8：Turnkey 响应无身份 → sign 腿今日无锚，晋升路径实际不可达**，见 §2 Padre 行）/
`null`（manifest 占位腿）。有明细 manifest 时 order 腿建腿即绑定 platform×chain
授权槽；绑不上（槽满/无该链槽/chain 未知）→ 腿标 `unbound:true`、round=null，计
`counts.unbound` 诊断，不冒充授权尝试。

**观测序号 vs 授权轮号（RH-T04，2026-09-07 r7）**：槽绑定只证明**槽容量占用**，
不证明授权轮号——manifest 只带 platform×chain×rounds 数量、请求不带轮号，addon
从请求本身无法得知授权轮次；首轮未观测时按到达序编号会整体前移（ji2fj 实证：
HK RH 四腿按到达序编号 1-4，经 raw 完整 tx 回配唯一对应 EU Binance 授权第 2-5
轮）。r7 起：

- 腿级 `observationIndex`：该 platform×chain 内的**到达序**（从 1 起，chain 按建腿
  时所知——未知链自成一序；preview/sign/dup/unbound 腿同样编号）——纯观察事实，
  不含授权语义。
- 腿级 `round`：**授权轮号，addon 侧恒 null（未知）**——不按到达序填写。
- 腿级 `authorizedRound`：EU 侧 `hk-timing-client` 在 `closeAndPull` 后唯一回配
  （`applyAuthorizedRounds`）——raw 腿（full=1 拉取的完整 txHash / 请求响应建立的
  锚身份 orderId/clientOrderId）与 EU 批次授权轮记录**唯一**对应才写；缺失/多值
  冲突/平台链不符/身份冲突保持 null。counts/manifest 口径不变（槽绑定照旧——绑的
  是槽容量，不是轮号真伪）。

**订单身份去重（R22 四轮①，2026-09-07 r6）**：绑定前先做同单去重——同 run 内与
**已绑槽腿** `orderId`/`clientOrderId 正向匹配`（至少一个共同身份键且**全部**共同键
相等；共同键冲突或无共同键 = 无法证明同单，不猜）的请求归入**同一授权槽**：腿标
`dup:true`、round=null、释放占用槽，计诊断 `diag.orderReqDuplicate`——不新增
attempted、不占新槽、不计 unbound。去重时机：建腿前（binance 请求体
`clientOrderId`）、响应后（响应抽取 id）、晋升绑定前（preview/sign 腿身份）。N>1
下同订单重复派发（产品重试）不再吃掉真正后续订单的槽位；身份缺失/冲突的腿保持
容量判定与 unbound 旧律。

**去重边界（r9，2026-09-08 RHF-10）**：

- **platform 维度**：同单查找只认同 platform 的已绑槽腿——同 orderId 跨平台不合并
  （r8 及之前无 platform 范围，binance/okx 同 orderId 会把后到腿误判 dup）。
- **formal 资格归最早请求（RHF-10①）**：请求无身份、身份由响应揭示时，后请求可能
  先响应先成 formal；最早请求的响应揭示同身份后（仅响应路径、两腿均 order 角色），
  formal 资格移交最早请求——授权槽、已落地的成功/receipt 证据与在飞 poller 闩锁
  随槽位移交（闩锁**移交而非复制**，旧腿闩锁释放——否则其永挂致 R25 pin 死；
  `_settle_receipt`/`_settle_incomplete` 均经 dup→formal 链重定向，receipt 与失败
  原因同成功一样永不留在 dup 腿），后请求腿改判 dup（`mergedIntoFlowId` 指向
  最早腿）。成功时刻不变、**起点按最早请求**（平台重发含在 L1a′ 内，§4 r8 与
  binance.md §90 同口径）。preview/sign 晋升路径不转移——fomo 同 relaySwapId 取
  最晚 preview 是既定口径。
- **dup 重开（RHF-10③，multi-order-suspect）**：dup 腿收到与 formal 冲突的身份
  （共同身份键两侧都有值且不等——docs/binance.md「一轮 >1 distinct orderId」），
  或 formal 被锚冲突否决 → 同单证明被推翻，腿重开为独立候选：撤销 dup、计数改记
  （`orderReqDuplicate`−1、`orderDupReopened`+1，per-run `timeline.diag` 同步）、
  按自身身份重新绑槽（无槽/链未知 → unbound 旧律）。r8 及之前此类腿永久扣在 dup
  上，其 distinct orderId 的 FINISHED 整帧拒收——第二订单的执行证据被吞。已并入
  formal 的等值锚不回收（同值无害）；冲突值从未覆盖 formal 锚（首写冻结）。

**DONE·receipt 乱序补绑（R22 四轮②，2026-09-07 r6）**：preview/sign 晋升时链未
实证（腿无 chain 且尚无 receipt 实证链）→ 腿标 `pendingBind`（开窗暂态，不计
attempted/unbound），不猜链、不即判 unbound；receipt 回写时以 **receipt 实证链**
（R24 门已过的轮询胜出候选——补链的唯一合法来源）回填 `leg.chain` 并按 manifest
槽补绑。DONE→receipt 与 receipt→DONE 两种事件顺序产出**相同计数与相同腿身份**。
关窗 freeze 时仍未实证链的 pendingBind 腿转 `unbound` 落定（计
diag.orderReqUnbound）；实证链无对应 manifest 槽/容量已满 → unbound 落定。补绑
不取消既有关联/新鲜度门（R07/R08/R23/R24 照旧先过）；关窗后到达的回写仍一律丢弃。

**结构化锚抽取 + 按身份路由（r8，2026-09-07 复审 PX-02 / B-R22 同根因）**：

- **响应锚只从 schema-known 字段结构化抽取**（`RULES[*].extract_ids`）：Binance ack
  `data.orderId` 与 `data.clientOrderId` **分键**写锚（数字 orderId 归一为字串），hash 只取
  `data` 同节点 `orderTxId/txHash/signature/txId` 归一一致的唯一值（多值不一致 → 不取，记
  `evidence.anchor.ambiguous`）；失败 ack（`code≠000000`、无 `data.orderId`）不提供身份
  ——请求体 `clientOrderId` 仍是锚。FOMO：`responseObject.v2Swap.relaySwapId`（EVM 形状）。
  OKX / GMGN（透传、规则休眠）：键**白名单**（OKX `orderId` / `transactionHash|txHash`；GMGN
  `orderId|order_id|oi` / `hash|tx_hash|txHash|signature|txid…`）有界遍历（嵌套层级文档未钉死，
  同 `scripts/lib/tx-hash.mjs` 键名遍历口径），同键多个不同值 → 歧义不取。Padre：Turnkey
  ActivityResponse **恒不提供锚**（§2）。任何平台都**不**把 `id` 或他平台的键当订单号。
  旧整-body 正则（首个 `orderId|order_id|clientOrderId|id` 不分键写 `orderId`、无引号数字
  漏配、嵌套 `"id"` 冒充——曾让 Binance FINISHED 帧「冲突」整帧**静默**丢弃）只保留为诊断
  `evidence.anchor.legacyRegexDiverged`，永不写锚。
- **入站 WS 帧按锚身份路由到目标腿**（`Store.route_ws_frame`）：候选 = 所有开窗 run 内该平台
  未冻结的腿——**不再只看平台最新腿**（最新腿是 dup/后一轮时，前一轮迟到成功帧曾被「冲突」
  丢弃，或成功落在 dup 腿）。判定与 `binance-ws.mjs anchorMatchVerdict` 同词汇：共同身份键
  （`orderId/clientOrderId/txHash`）全部相等 = positive；一等一不等 = conflict（拒）；全不等
  = miss（他单，不毒化）；无共同键 = none。无身份键的帧不路由。候选排序：非 dup > 已绑槽
  （formal）> 出站时刻——order/sign 腿取**最早**（首次下单请求 = EU `order_http_out` 同义，
  平台重发含在 L1a′ 内，与 [`binance.md`](binance.md) §90「L1a 含 ~2.2s 平台重试、不得排名」
  同口径）；preview 腿取**最晚**（fomo 预览/执行 L7 不可区分，同 `relaySwapId` 的最后一次
  `/swaps/v2` 最接近执行 POST）。
- **身份冲突 = 否决条件（r9，2026-09-08 RHF-09）**：腿的 relayId/orderId 一经响应/subscribe
  确证（锚首写冻结），后续冲突身份使锚自相矛盾 → 腿降级 diagnostic（`incomplete=
  "anchor-conflict"` 首写落盘；残余收元起序列化 `anchorVeto:true` + 诊断
  `hkL1aMsVetoed`、合格 `hkL1aMs` 撤销为 null——§5）：路由候选排除（帧只命中被否决腿 → 计
  `wsFrameAnchorVeto`；dup 归并目标被否决 → 同键拒收）、晋升/成功写入/receipt 轮询各入口
  带守卫、**observed 不增加**（receipt 先于否决落定的残余在 counts 层排除）。fomo 反例
  （r8 实测行为）：A preview→relayA，B preview 请求后 A 的迟到 subscribe 挂到 B（B 锚
  =relayA），B 响应明确 relayB——旧码只记 `anchorConflict` 不否决，success(A) 仍把 B 晋升
  成功且起点按 B 的较晚请求（L1a′ 假性偏小，A 永不成功）。r9 起 success(A) 只匹配 A 的腿。
  对照纪律不变：腿已有确证锚时迟到的异值 subscribe 仍静默不动（时序绑定不构成冲突证据）。
- **dup 归并 formal**：dup 腿（同单重复请求）登记 `_dupOf`=formal 腿，其请求体/响应建立的
  独立锚**并入 formal 锚**（首次写入、不覆盖；不等 = 身份冲突，记 `evidence.anchor.conflicts`
  + `diag.anchorConflict`，不猜——r9 起冲突同时推翻 dup 判定，见上「去重边界」）——平台重发
  场景首 ack 失败无 `orderId`、次 ack 带 `orderId`，FINISHED 据此仍正向落 formal。帧命中
  dup 腿 → 归到 formal；formal 与帧冲突 → 整帧不归属（多订单疑似）。**成功永不留在 dup 腿**；
  关窗后 dup 腿 `incomplete="duplicate-of-bound-leg"` + `evidence.mergedIntoFlowId`=formal 的
  `orderFlowId`。
- **缺失可见**：带身份却零命中 → `diag.wsFrameNoLegMatch`；只命中冲突腿 → `diag.wsFrameAnchorConflict`；
  ws_ids 整帧拒收 → `diag.wsFrameRejected`（/health 全局 + `timeline.diag` per-run）。
  `evidence.anchor.keys=[]` = 该腿无独立锚 → 成功帧结构上不可能正向关联。
  `evidence.successAnchorKeys` = 成功帧实际正向命中的锚键（关联依据可审计）。

成功/hash 采纳的锚纪律（2026-09-05 review 落地）：

- **独立锚（R07）**：成功状态/hash/id 必须来自同一 schema-known entry 且与「请求/响应
  建立的独立锚」（`leg._anchor`：响应结构化抽取 id、Binance 请求体 `clientOrderId`、FOMO 页面
  出站 subscribe 绑定）正向匹配——帧自带 id 先写入腿再自证关联**不算数**；缺锚 → 不出
  HK-L1a′。Padre 加严：DONE 节点必须同节点带 hash，时间新鲜性只在同一 DONE 节点子树内
  校验（creationTime ≥ 腿出站墙钟 −5s）——根节点新 timestamp 不给旧子交易背书；DONE
  缺时间戳不采纳（r8 起 Padre 今日无锚，该门只对未来锚源生效，单测以直接注入锚覆盖）。
- **同一 entry（R07 二轮）**：Binance 同一 content entry 的全部 schema hash 字段
  （`orderTxId`/`txHash`/`signature`/`txId`）归一（0x 前缀大小写归一）不一致 →
  **整帧拒收**（与 `scripts/lib/binance-ws.mjs` 同律，Python 侧重写非 import）；
  FINISHED 帧必须正向等于锚（缺 id 或无锚的 FINISHED 不凭时间窗充数）。
- **跨腿去重（R23）**：同 run 内 id/hash 跨腿去重防陈旧重放；**chain 不参与去重**
  （chain 是属性不是身份）。
- **receipt 单 poller 闩锁（R08）**：HTTP/WS 入口共用 `claim_receipt_poll` 原子闩锁——
  每腿至多一个 poller，轮询**腿锁定的 hash**；首次成功时刻只写一次
  （`_settle_receipt` 已有值不覆盖）。
- receipt RPC 显式 UA `speedex-hk-timing/1.0`——robinhood RPC 403 拦截 Python-urllib
  默认 UA（curl 不受影响），曾致 rh 全腿 L3′ 超时。

## 5. timeline schema（`/timeline/<runId>` 实际形状，`schemaVersion=2`）

```json
{
  "schemaVersion": 2,
  "runId": "…",
  "mark": {"epoch": 3, "opened": …, "opened_wall": "…", "closed": …,
           "note": "…", "requested": 36, "manifestLegs": […]},
  "hkClockAt": "2026-09-05T…Z",
  "counts": {"requested": 36, "attempted": 20, "observed": 18, "unbound": 0},
  "legs": [
    {"chain": "bsc", "platform": "binance", "round": null, "observationIndex": 1,
     "authorizedRound": 2, "anchorRole": "order",
     "tOrderOutMs": 178…, "tFirstRespMs": 178…, "tSuccessPushMs": 178…, "tReceiptMs": 178…,
     "hkL1aMs": 812.0, "hkL3Ms": 2210.0,
     "txHash": "0x1234…abcd",
     "evidence": {"orderFlowId": "…", "wsFrameSha": "…",
                  "receipt": {"rpc": "…", "chain": "bsc", "pollIntervalMs": 800,
                              "status": 1, "blockHash": "0x…", "blockNumber": 123},
                  "anchor": {"keys": ["clientOrderId", "orderId"], "legacyRegexDiverged": false,
                             "conflicts": [], "ambiguous": []},
                  "successAnchorKeys": ["orderId", "clientOrderId"],
                  "mergedIntoFlowId": null},
     "incomplete": null, "atWall": "…"}
  ],
  "diag": {},
  "overflow": {"legs": 0, "events": 3},
  "method": "hook-observed@v2", "vantage": "hk-proxy",
  "instanceId": "…", "addonVersion": "2026.09.12-head-v4",
  "mitmproxyVersion": "12.2.3", "pythonVersion": "3.13.x"
}
```

- **r8 新增（只增不改；私有 raw 可见，public DTO 未登记 → fail-closed 不投影）**：顶层
  `diag`（per-run 计数：`wsFrameNoLegMatch` / `wsFrameAnchorConflict` / `anchorConflict` /
  `anchorAmbiguous` / `wsFrameDupNoFormal`；空 `{}` = 本 run 无静默丢弃）；腿级
  `evidence.anchor`（`keys` 独立锚键名——**不含身份值**；`legacyRegexDiverged` 旧正则是否
  与结构化锚分歧；`conflicts` / `ambiguous` 键名列表）、`evidence.successAnchorKeys`（成功帧
  正向命中的锚键）、`evidence.mergedIntoFlowId`（dup 腿 → formal 腿 `orderFlowId`；非 dup 为
  null）。占位腿三键为 null。既有键名/语义/单位不变；`schemaVersion` 仍为 2。

- **r9 新增（2026-09-08 RHF-09/RHF-10）**：diag 新键 `wsFrameAnchorVeto`（帧只命中被否决腿
  或 dup 归并目标被否决）与 `orderDupReopened`（dup 判定被冲突身份/formal 否决推翻后重开为
  独立候选——multi-order-suspect）；腿级 `incomplete` 新枚举 `anchor-conflict`（锚身份自相
  矛盾的否决腿，否决时首写落盘）。observed 计数排除被否决腿（§4 与上文 counts 口径）。
  `timeline.diag` 与 `/health` diag 不被 public DTO 登记（私有 raw only）。

- **late-veto 序列化（2026-09-08 RHF-09 残余收口；只增不改，`schemaVersion` 仍为 2）**：
  否决腿（含 success 先到、冲突响应后到的 late veto）在 timeline 腿级序列化
  `anchorVeto: true`，`incomplete="anchor-conflict"` 保持；**合格 `hkL1aMs` 撤销为
  null**，被否决的原值退到诊断字段 **`hkL1aMsVetoed`**（ms，绝不作合格指标）。更早工件
  无此两键 = 未否决（消费端按缺省处理）；r9 早期工件的否决腿无 `anchorVeto` 键，但
  `incomplete="anchor-conflict"` 自 r9 起只在否决时首写——消费端同为否决处理（其
  hkL1aMs 残余即被否决值）。`anchorVeto`/`hkL1aMsVetoed` 已登记 public DTO（布尔/毫秒
  诊断，不含身份材料）；内部标记 `_multiOrderSuspect` 仍不出 timeline。UI（最近任务
  详情）把否决腿排除在绑定统计/分母/p50/缺证据计数之外，以「锚冲突否决 · 不计入」
  诊断行保留可见——顶部 counts 与明细不再自相矛盾。

- **counts**：`requested`（manifest 登记的授权轮次；`legs` 明细带 per-leg rounds 时
  `requested=Σ legs[i].rounds`，缺失/非法按 1；明细优先于顶层 rounds）/ `attempted`
  （只计 `anchorRole="order"` 且**已绑授权槽**的腿——fomo 预览与 padre 签名不是下单
  尝试；r6 起另排除 `dup` 同单重复腿与 `pendingBind` 待实证链腿）/ `observed`
  （r6 起 = attempted 同一集合中有成功推送或 receipt 观察的腿——**observed ⊆
  attempted**；sign/preview 腿的 receipt 只作诊断与晋升依据，晋升失败不单独计入；
  r9 起另排除锚冲突否决腿——RHF-09：否决腿不写成功/receipt，此处是 receipt 先于
  否决落定的残余防线）/
  `unbound`（有下单/执行观察但绑不上授权槽的腿数——诊断，不进 attempted/observed；
  含关窗 freeze 时由 pendingBind 落定的腿）。腿级另带 `dup`（同单重复诊断，r6）与
  `pendingBind`（待 receipt 实证链补绑的开窗暂态，r6）标记。manifest 登记了但未观察
  到下单请求的轮次以**占位腿**呈现：时间戳全 null、`anchorRole=null`、
  `incomplete="no-order-observed"`。
- **五件采集环境身份**（`_collection_identity()`，与 `/health` build manifest 同源）：
  `vantage`（由 `/egress` 实测 `countryCode` 推导：`hk-proxy`/`sg-proxy`/…，未知
  `unknown-proxy`——不硬编码）+ `instanceId`（启动时间+pid+随机，热重载即变）+
  `addonVersion`（addon 常量）+ `mitmproxyVersion` + `pythonVersion`；另带 `method`。
  sidecar 自证采集环境身份——EU client `hkTimingEnvironment` 取 timeline 优先、
  session 健康兜底，缺则 null，绝不发明。
- **当批采集配置身份 `captureIdentity`（r10 实装，RHF-15 持久化侧收口，2026-09-08）**：
  addon `_capture_identity()` 自报当批**实际生效的 allow_hosts 宿主模式集**——形状
  `{allowHosts: string[]|null, allowHostsSource, ctrlPort}`：`allowHosts` 有界
  （≤16 条、每条 ≤512 字符），**空数组 = 未设白名单即全量拦截**（如实报告），
  `null` = 未知不猜；`allowHostsSource` 标证据等级（`mitmproxy-options` =
  ctx.options 真实生效值 > `env` = unit 声明兜底 > `unavailable`）；`ctrlPort`
  区分实验实例（18072）与生产（8072）。只含配置事实，不落凭据/连接细节。
  消费链路全通：`hkTimingEnvironment`（timeline 优先、session 健康兜底——client
  `verifyTunnelIdentity` runtime 键表 r10 起透传该对象；raw 侧 `sanitizeCaptureIdentity`
  形状纪律与 addon 同界，标量叶 + 字符串数组、秘形键名 fail-closed 丢弃）→ sidecar →
  `HK_TIMING` DTO（显式登记 allowHosts/allowHostsSource/ctrlPort 三键，未登记键不投影，
  数组非标量成员 DROP）→ `/api/hk-timing` → UI（§2 规则③）。**r10 前实例与全部历史批
  该键缺省**（`hkTimingEnvironment` 落 null、投影不补键、UI 落「未知」），行为不变；
  现网三实例（hk1 生产/实验、hk2 生产）均已部署 r10+，当批生效以节点 `/health` 实际回包为准。
- **round / observationIndex / authorizedRound（r7，RH-T04）**：`round`=授权轮号，
  addon 恒 null（未知——到达序不冒充授权轮，ji2fj 实证见 §4）；`observationIndex`=
  platform×chain 内到达序（纯观察事实，占位腿为 null）；`authorizedRound`=EU 侧
  唯一回配的授权轮（`hk-timing-client.applyAuthorizedRounds`，缺失/冲突保持 null；
  仅在 client 收到授权轮记录时标注，否则腿无此键）。
- **txHash 披露分层**：缺省短化（`0x1234…abcd`）；`?full=1` 完整 hash + 请求/响应
  建立的锚身份 `orderId`/`clientOrderId`——**仅供 loopback 控制面证据恢复**
  （orchestrator raw sidecar 以 full=1 拉取；锚身份是 authorizedRound 唯一回配的
  输入）。公有投影走 `public-projector.mjs` 的 `HK_TIMING` DTO：腿级只登记
  chain/platform/round/observationIndex/authorizedRound/hkL1aMs/hkL3Ms/incomplete/
  atWall/anchorRole/anchorVeto/hkL1aMsVetoed/t\*Ms + `txHash:'shortId'` 结构层短化 +
  evidence 登记键（含 receipt 块身份——链上公开信息）；
  顶层登记 schemaVersion/runId/hkClockAt/error/gone/droppedInReload/degraded/
  degradedReason/**state**/**reason**（NR-08 批次级采集状态，见 §8「批次级状态」）/五件身份/method/captureIdentity（map 位——RHF-15）/counts（含
  unbound）/overflow/legs + `mark`（仅
  note/requested/manifestLegs——epoch/opened/closed 是 HK 本机 monotonic 不登记）；
  腿级另登记 `unbound`。r6 新增腿级 `dup`/`pendingBind` 在 DTO 登记前只进私有
  raw（未登记键 fail-closed 不投影）。完整 hash 与 orderId/clientOrderId（锚身份）
  只进私有 raw——DTO 不登记，fail-closed drop；timeline 输出本无原始 events
  数组（full=1 亦不含），腿级事件只以折叠后的事实字段（t\*Ms/anchorRole/已登记
  evidence 键）呈现，未登记的事件细节同样 fail-closed 不投影。
- **manifest 面**（`/mark` 入参，R22）：`{"rounds": n}` 或
  `{"version","runId","rounds","legs":[{"platform","chain","rounds"}…]}`——授权轮次
  登记；有界：明细 ≤256 条、per-leg rounds ∈[1,64]、总轮次 ≤4096。无 manifest 行为
  同旧（requested=null，不占位）。EU 侧由 `buildHkTimingManifest`（batch-egress.mjs）
  按本批授权参数（chains×platforms×n）构造，version=1。

## 6. missing / degraded / gone / overflow 规则

- **missing（腿级）**：关联不上 = 时长 null + `incomplete` 原因——**绝不发明数字**
  （与主仓测量契约同律）。现行枚举：`no-order-observed`（占位腿）、
  `no-chain-for-receipt`、`receipt-missing-block-identity`（R24 块身份门）、
  `receipt status=0 (reverted)`、`signature err`、`receipt poll timeout`、
  `anchor-conflict`（r9，RHF-09：腿锚身份自相矛盾——响应/subscribe 确证后到达的
  冲突身份；腿降级 diagnostic，否决时首写落盘，路由/晋升/成功/receipt 全关闭；
  残余收元起随 `anchorVeto:true` 序列化，被否决值退到 `hkL1aMsVetoed` 诊断——§5）。
  **关窗补写**（2026-09-06 R22，timeline 输出层，不改冻结腿内部态）：关窗后仍无
  incomplete 的腿按锚角色补齐——未晋升 preview/sign 腿 `no-execution-proof`、
  dup 同单重复腿 `duplicate-of-bound-leg`（r8：成功归 formal 腿，见
  `evidence.mergedIntoFlowId`；不是 no-success-observed）、
  order 腿无成功推送 `no-success-observed`、有成功无 receipt `no-receipt-observed`；
  开窗中不补。锚层缺失原因另见 `evidence.anchor`（`keys=[]` 无独立锚；`conflicts` /
  `ambiguous`）与 `timeline.diag`（§4 r8）。
- **gone（run 级，两态区分）**：Store 有界 retention（R25——完成的 run 只保留最近
  50 个，gone 墓碑 200 条；开窗中的 run 与有活动 poller 的腿 pin 住不回收）→
  `{"gone": true, "legs": []}`；**热重载接管前旧实例的 run**（R18——Store 不随重载
  交接）→ `{"gone": true, "droppedInReload": true, "detail": …, "legs": []}`。
  两者都区别于「从未见过的 run」（空 timeline：`legs:[]` 无 gone）。
- **degraded（EU client 判定，R18 残留补）**：`closeAndPull` 拉取前重取 `/health`
  复核实例身份——instanceId 漂移 / 健康复核失败 / 关窗失败 / timeline.gone /
  timeline 实例与开窗时不符 → 结果标 `degraded` + `degradedReason`（ok 仍 true、
  timeline 照回，绝不假装完整采集）；runId 不匹配 → `ok:false, degraded:true` 拒收。
  orchestrator 把 degraded/degradedReason 透传进 raw sidecar（public DTO 已登记两键）；
  dashboard 最近任务详情对 degraded/gone/error/overflow 批次**不汇总时长**（只列腿级
  明细与原因）。
- **overflow（R22 单窗预算）**：每 run ≤200 腿（`MAX_LEGS_PER_RUN`）/ ≤2000 事件
  （`MAX_EVENTS_PER_RUN`），超限截断丢弃并在 timeline 标 `overflow={legs,events}`——
  防预览风暴（fomo 页面键入/报价刷新自发多次 POST）打爆小内存 VM；宁缺毋滥。
- **窗口纪律（R09）**：拒绝多窗并存——`/mark` 开新窗显式关闭其余开窗（旧腿冻结保留
  既有状态）；关窗（close/TTL `MARK_TTL_S=1800s`/被新窗顶掉）即冻结该 run 全部腿并
  撤销 open_legs 映射——迟到 WS 帧不更新、receipt 线程回写前核验腿未冻结；freeze 时
  仍未实证链的 `pendingBind` 腿按 unbound 落定（R22 四轮②，计
  `diag.orderReqUnbound`）。**建腿原子化（二轮）**：请求 hook 入口即绑定窗口
  runId+epoch（`capture_window`），解析后 `order_in_open_window` 只复验不重新选窗——
  复验+事件/腿写入同一 `STORE.lock` 临界区；关窗后到达的下单请求一律拒绝（计
  `diag.orderReqNoWindow`，/health 透出；同单重复归并计 `diag.orderReqDuplicate`）。

## 7. 控制面 API（HK loopback `127.0.0.1:8072`，EU 经 SSH 隧道访问，不公网）

| 端点 | 语义 |
| --- | --- |
| `POST /mark` | `{runId, note?, manifest?}` 开采集窗口；拒绝多窗并存（§6）；`note` 截 200 字符 |
| `POST /mark/close` | `{runId}` 关窗——此后流量不再归集，腿冻结，迟到帧/迟到 receipt 回写一律丢弃 |
| `GET /timeline/<runId>` | 拉取关联结果（§5）；`?full=1` 完整 hash（loopback 证据恢复专用） |
| `GET /health` | `{ok, marks, legs, seenReq, at, diag, addonVersion, instanceId, pythonVersion, mitmproxyVersion, captureIdentity}`（r10 起带 captureIdentity——当批实际 allow_hosts 模式集 + 来源 + 控制面端口，与 timeline 同源，§5）；`diag` 全局计数含 `orderReqNoWindow` / `orderReqDuplicate` / `orderReqUnbound` 与 r8 新增 `wsFrameNoLegMatch` / `wsFrameAnchorConflict` / `wsFrameLegConflict` / `wsFrameRejected` / `wsFrameDupNoFormal` / `anchorConflict` / `anchorAmbiguous`，及 r9 新增 `wsFrameAnchorVeto`（帧只命中被否决腿/归并目标被否决）/ `orderDupReopened`（dup 判定被推翻后重开——multi-order-suspect）；接管自旧实例时另带 `takeover={fromInstanceId, droppedRuns, runIds, at}`；**热重载分裂/bind 失败 → 503 如实报错**（旧控制面+新采集面 = mark 更新旧 Store、采集为零，不再吞掉假装 200） |
| `GET /egress` | `{ok, egress:{ip, label, isp, countryCode}}`（ip-api.com，10min 缓存）——vantage 推导源与隧道身份核验兜底 |
| `GET /latency` | `{ok, latency:{gmgn, okx, padre, binance, fomo, at, vantage}}`（60s SWR 缓存）。探针口径与 EU `platform-ws-latency.mjs` 同律（R20）：okx/padre `ws-ping`（先握手再单计 ping RTT）、binance `http-ping`（同一 keep-alive 连接预热+3 热样本中位，ms=rtt/2）、gmgn `https-warm`（同款 keep-alive 预热+第 2 次热请求整 RTT 不减半）、fomo `ws-challenge`（冷连全链路不减半）。失败一律 null。UI `remoteLatencyText` 逐项标注 method——**RTT 探针不是交易观测** |

**隧道身份核验（R17，EU client `openTunnel`）**：本地转发端口走端口注册表
（`artifacts/inject/hk-tunnels/` 文件锁，host 稳定盐起始，stale 锁按 pid 存活回收）；
隧道就绪后核验身份——注册表声明了 `instanceId` 则必须匹配（远端自报的任意 instanceId
不能自证），否则 `/egress` 出口 IP 必须与期望 host 一致（host 须为 IP 字面量，域名不能
据出口 IP 自证）；核验不过毁隧道；核验通过后复查 SSH 进程仍存活（不交付将死隧道）。
**SSH 目标只出自受信注册表 `configs/hk-nodes.json`**（R31——运维维护、git 跟踪、运行时
API 不可改写；绝不从代理条目派生 root@host）。

## 8. EU 集成与公有投影

- **sidecar 启停**（`scripts/lib/batch-egress.mjs` + `inject-orchestrator.mjs`）：
  `deriveHkTimingPlan` 按**最终有效代理快照**派生（HK env 在 + 本批代理快照非 direct
  且非 restore-failed + 生效代理 host === HK 控制面 host + 受信 sshTarget 在——显式
  `SPEEDEX_HK_SSH` 或注册表派生两路；未启用即 `scrubHkTimingEnv` 显式清继承的
  `SPEEDEX_HK_*`，防 `{...process.env}` 子孙污染；`SPEEDEX_HK_PARENT_HELD=1` 的多链
  自我子进程一律拒开）。单链/多链**共用**同一 open/close 生命周期：每批 open →
  `/mark`（带 manifest）→ 批末 `closeAndPull({full:true})`（可另带
  `authorizedRounds`——EU 批次授权轮记录 `[{platform,chain,round,txHash?,orderId?,
  clientOrderId?}]`，r7 起据此唯一回配腿 `authorizedRound`，缺失/冲突保持 null）→
  timeline 挂进 inject-batch / inject-multi 的 `hkTiming` sidecar（raw 私有域）。
- **批次级状态 `hkTiming.state`（NR-08，2026-09-09）**：采集是否发生与为什么没采集
  必须随批 doc 持久化——`hkTiming` 整键缺失（null）**不能**推定 hk:false。三态：
  - `ok`：`closeAndPull` 成功拉回 timeline（`hkSidecarClose` 落盘）；
  - `degraded`：timeline 照回但采集降级——`degraded`/`degradedReason` 原有两键保留
    （实例漂移/健康复核失败/gone，§6）；
  - `disabled` + `reason`：orchestrator `hkSidecarOpen` 拒开
    （`hkTimingStatusFromPlan`，batch-egress.mjs）——reason 是
    `deriveHkTimingPlan` 的有界枚举：`explicit-off`（条目 hk:false /
    `SPEEDEX_HK_TIMING=0`，当批显式关闭）/ `no-hk-env`（生效出口非受信 HK 节点）/
    `no-effective-proxy-snapshot` / `hk-host-proxy-mismatch` / `no-trusted-ssh-target` /
    `no-trusted-ctrl-port` / `hk-loopback-unresolved`。
  **parent-held 不落盘**：多链自我子进程的批 doc 不写 hkTiming 键——父进程持权威记录
  （timeline 或自身 disabled 标记，按 `results[*].requestId` 可回配），
  `/api/hk-timing` 按 requestId 先到先得，子 doc 落键会遮蔽父记录。
  旧批（本字段落地前）无 `state`/`reason`、甚至无 `hkTiming` 键 = **状态未知**，
  展示层标「无采集记录 · 原因未知」，绝不反填、不推定已关闭或已启用。
  缺 HK 时间线的批**不伪造 HK 数值、不进任何 HK 汇总/排名**；EU 侧数字也**不得按
  RTT/2 或聚合中位数相减「修正」成 HK 口径**（§1：两口径锚点与钟域不同源）。
- **fail-soft**：任何一步失败返回 `{ok:false,error}`——HK 计时是 sidecar，不拖死主批。
- **展示**：dashboard `GET /api/hk-timing/<requestId>`（只读 public 投影，带目录指纹
  缓存，响应前再过一次已登记 DTO 防御纵深）；最近任务详情加 HK 口径行（L1a′/L3′，
  与 EU 行并列、注明独立 cohort 不混排不排名；degraded/overflow 不汇总时长；锚冲突
  否决腿（anchorVeto）不进绑定统计/分母/p50/缺证据计数，以「锚冲突否决 · 不计入」
  诊断行保留可见，`hkL1aMsVetoed` 只作诊断值；全占位平台行按 §2 的 RHF-15 规则
  逐行解析原因——缺当批能力证据显示「未知 / 无当批能力证据」，不按当前矩阵反填）。
  **批次级状态展示（NR-08）**：`state:'disabled'` 按 reason 分档出文案
  （explicit-off=当批显式关闭 / no-hk-env=生效出口非受信 HK 节点 / 其余枚举直标）；
  `state:'degraded'` 并入不汇总时长路径；`hkTiming` 整键缺失 = 旧批/未持久化 →
  「无采集记录 · 原因未知」——三态分开，不得一律显示「未启用」或空白。
- **每批采集配置身份 `captureIdentity`（2026-09-08 RHF-15 持久化侧——消费侧已接线，
  生产生效待 addon 部署）**：每批持久化的身份 = 五件采集环境身份
  （vantage/instanceId/addonVersion/mitmproxyVersion/pythonVersion）+ method +
  degraded 标记 + **`captureIdentity`**（当批实际 allow_hosts 模式集，addon
  `_collection_identity()`/`/health` 自报）。消费链路已全通：`hkTimingEnvironment`
  （timeline 优先；`sanitizeCaptureIdentity` 标量叶/有界/秘形键 fail-closed）→
  orchestrator `hkSidecarClose` → raw sidecar → `HK_TIMING` DTO（map 位登记）→
  `/api/hk-timing` → 最近任务详情（§2 规则③：当批已拦截/当批透传按身份直读）。
  **残余门槛**：addon 侧字段随 r10 实装（`_collection_identity()`/`/health` 同源，
  `ctx.options.allow_hosts` 实读优先、`SPEEDEX_HK_ALLOW_HOSTS` env 兜底、
  unavailable 不猜），client `verifyTunnelIdentity` 键表已透传（session.health
  兜底路径通）——唯部署前所有批该键缺省，UI 保持「未知 / 无当批能力证据」
  （§2 规则④），不按当前矩阵反填；历史批永不回填。
- **可比性**：只有同为 HK 出口的批次才可进入同一统计组（egressAnchor=hk/eu 是可比性
  维度+1）；HK 口径不进排名（[`../plugin.md`](../plugin.md) §0.2 /
  [`fair-comparison.md`](fair-comparison.md) §0）。

## 9. 部署与安全

- **部署**：`deploy/hk-proxy/`——install.sh（幂等：umask 077 前置、依赖锁定
  mitmproxy 12.2.3/11.0.2 按系统 Python minor 分岔 + **websockets 17.1 /
  msgpack 1.1.2（py≥3.12）·1.1.0（py3.11——mitmproxy 11.0.2 钉 msgpack≤1.1.0）**、
  unit/addon/venv/**htpasswd 凭据内容**/**websockets·msgpack 已装版本**变更且服务
  active → restart（2026-09-06 收口：verifier 内存凭据只在
  启动时加载，只轮换凭据也必须重启；2026-09-09 历史线索③残余收口：依赖独立升降级
  同理——运行中 mitmdump 已 import 旧版模块，只换 site-packages 文件不重启不生效，
  按 pip show 已装版本 vs 钉选比对置 RESTART_NEEDED，幂等 install 不变）、
  末尾打印实际版本摘要）+ `speedex-mitm.service`（mitmdump :8443
  公网强制认证 + addon 控制面 127.0.0.1:8072 + MemoryMax=700M）。
- **认证**：`--proxyauth @/opt/speedex-mitm/htpasswd`——htpasswd 为 **{SHA}**
  （base64 sha1，stdlib hashlib，无 bcrypt 依赖：bcrypt 每 CONNECT 全量验算曾在单核
  VM 压满事件循环，1dj3c 事故）；mitmproxy 11（passlib）/12（自研 HtpasswdFile）/13+
  均支持 {SHA}。`gen-htpasswd.sh` 由 python 直读 0600 `proxy.env` 生成——口令**不经
  argv/environ/stdout**；文件创建即 0600。CA 私钥 0600 不出 HK，CA 用 certutil 装进
  研究 Chrome 的 NSS DB（只信本 CA）。
- **隐私纪律**：addon 只记录元数据 + 尺寸 + 内容 hash；**绝不落盘** cookie/
  authorization 头、签名 body、私钥材料；事件默认**内存态**（Store + retention，
  不落 NDJSON——附录 A 的 NDJSON 设计未采用）。WS 消息每条即裁剪（`del msgs[:-1]`——
  长连行情流无界累积曾致 1G 小机两次 OOM-kill）。mitmproxy `allow_hosts` 正则必须匹配
  `host:port`（next_layer.py 追加端口）——`$` 锚定漏配会让过滤静默失效退化为全量拦截。
- **日志事故与节点治理**（journal 脱敏保留范围、清理授权前提、权限整改与现场待复核清单）：
  [plans/hk-node-governance.md](plans/hk-node-governance.md)（NR-16，2026-09-09）。

---

## 附录 A：历史方案（草案存档，非现行合同）

以下为 2026-09-04 草案的选型与计划记录，保留作历史；与现行段冲突时以现行段与代码为准。

### A.1 实测数据（2026-09-04）

| 段 | 实测 |
| --- | --- |
| EU → HK RTT | **~231ms**（污染量级——EU 锚点不可用） |
| HK 机器 | Debian 13 · 1C/1G · Python 3.13 · systemd-timesyncd 已同步 |
| 平台 RTT（HK 直测） | gmgn 36ms · okx 322ms · padre 267ms · binance 319ms · fomo-api 192ms |
| 平台 RTT（EU 直测） | gmgn 33ms · okx 227ms · padre 81ms · binance 258ms · fomo-api 31ms |

### A.2 技术选项（2026-09-04 评估）

**A. mitmproxy + 自研 addon（已采用）**：L7 全可见（HTTP/1.1、h2、WebSocket 帧级时间戳），
Python addon 打戳 + 关联。信任链：HK 本地生成私有 CA（私钥 0600 不出机），CA 装进研究
Chrome 的 NSS DB——比 `--ignore-certificate-errors` 全局放行安全。

**B. gost/Xray + pcap 旁路**：L4 只有 TLS 包时序/大小，看不到 WS 帧语义，无法识别
「成功推送」——只能粗 RTT，不满足需求。

**C. 自研 CONNECT 隧道 + TLS 终结（Go/Node）**：等于重写 mitmproxy，不值。

**D. HK 上跑完整研究 Chrome（CDP over SSH 隧道）**：最真口径（click 都发生在 HK），但
要迁移四平台登录态（secrets 跨机复制）+ 双热会话纪律。二期候选，未实施。

**E. 仅预检延迟走 HK**：已落地为 addon `/latency` 五平台原生探针 + EU 侧 `/api/proxy`
的 HK 节点 remote 画像（`hkRemoteFor` 经 SSH 隧道拉 `/egress`+`/latency`，SWR 60s，
控制门后的写路径才开隧道）——展示用，不进交易时序。原草案的「`/api/net-latency` 加
hk 字段」未采用。

### A.3 NDJSON 落盘设计（未采用）

草案曾设计事件落 NDJSON（只元数据+payload 尺寸/hash，不落 auth body/cookie）。
**现行实现为内存态 Store**（每 runId 的 marks/legs/events 在 addon 进程内存，有界
retention + 单窗预算，见 §5/§6）——无任何事件落盘文件。本文与代码中的「NDJSON」
字样均属草案残留。

### A.4 分期与工作量（2026-09-04 草案口径）

P0 节点就绪（mitmproxy + CA + systemd + EU Chrome 装 CA + 四平台过代理验证，决策点：
某平台拦截异常 → 退 SNI 白名单直通）→ P1 addon（事件流+控制面+关联规则+离线单测）→
P2 EU 集成（mark/pull 接线 + sidecar + UI 行，先 N=1 干跑再 LIVE 对比）→ P3 可选
（D 方案 / E 方案）。原估总计约 2.5 天——仅存档，实际工期不复核。

## 请求起点已观测链头（v1，2026-09-11）

控制面开窗预启动每链独立只读 head 订阅，断流以显式 RPC 轮询补采；hook 入口单调时间之前收到的最新样本冻结到订单腿 `chainHead`。关窗/卸载停止自有采集，不触发交易；旧窗和迟到网络结果不可进入新窗。多链父窗口关闭后以新不可变工件附到相同 requestId/chain 的子批，公开投影递归登记字段。口径由 plugin.md §0.2.1 所有；健康和离线测试不代表已完成真实订单验收。
