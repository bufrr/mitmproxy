# speedex HK timing proxy（基于本 fork 的 mitmproxy）

speedex 延迟研究 harness 的 HK 出口测量代理：mitmproxy addon + 控制面 + 部署件。
**本 fork 的 `main` 分支是 proxy 侧代码的规范主仓**——addon/部署脚本只在
`speedex-hk/` 目录演进（2026-09-14 自 speedex 主仓 `deploy/hk-proxy/` 迁出；
speedex 主仓只保留消费侧 `scripts/lib/hk-timing-client.mjs` 等与部署文档 docs/）。

## 内容

| 文件 | 用途 |
| --- | --- |
| `speedex_hk_timing.py` | mitmproxy addon：下单/成功信号锚关联 + 链 receipt 轮询 + 控制面（`/mark` `/mark/close` `/timeline/<rid>` `/health` `/egress` `/latency`） |
| `test_speedex_hk_timing.py` | addon 离线测试（合成 flow/frame 夹具）：`python3 test_speedex_hk_timing.py` |
| `tests/` | 安装/单元行为测试（Node 22，`node --test tests/*.test.mjs`，21 项） |
| `install.sh` | 幂等安装/受控重启（用法见文件头；实验实例 `SPEEDEX_HK_INSTANCE=experiment`） |
| `gen-htpasswd.sh` | 代理认证 htpasswd 生成（{SHA}，凭据不进 argv） |
| `speedex-mitm.service` | 生产 unit（0.0.0.0:8443 + 控制面 8072） |
| `speedex-mitm-exp.service` | 隔离实验 unit（loopback 18544/18072，默认 OFF 脚手架） |
| `speedex-exp-tunnel.service` | EU 侧常驻 SSH 隧道 unit（18544/18072 → HK 实验实例 loopback） |
| `selective-decrypt.overlay.example` | GMGN/OKX 选择性解密候选 overlay（DO-NOT-ENABLE，须 SOP + 当次授权） |
| `AGENTS.md` | 本目录的硬约束（凭据/隐私/锚纪律——改动前必读） |
| `docs/` | 协议与运维文档副本（speedex 主仓 docs/ 是权威面，此处为随代码快照） |

## 跨仓合同（改动必须双侧同步）

- **控制面 8072**：addon `_ctrl_port()` 缺省 ↔ speedex 主仓 `scripts/lib/hk-timing-client.mjs` `HK_CTRL_PORT`；实验实例 18072 ↔ 主仓 `configs/hk-nodes.json` instances 映射。
- **timeline/health 形状**：speedex 主仓 hk-timing-client / public-projector 的 DTO 消费方。

## 部署目标版本

mitmproxy pip 钉版（`install.sh`）：12.3.0（python ≥3.12 节点；2026-10-22 自 12.2.3 升级，eMudhra CA 泄露后 certifi 去信任。钉选解释器：`SPEEDEX_HK_PYTHON` > `/opt/speedex-python/bin/python3`（uv 独立 3.12，hk-vmiss-2 Debian 12 用）> 系统 python3）/ 11.0.2（python 3.11 节点），
`websockets==17.1`、`msgpack==1.1.2/1.1.0`。上游源码树**不被本目录修改**——
我们只消费 pip 安装的 mitmproxy hook API。

## 部署（scp 源即本目录）

```bash
scp speedex-hk/{speedex_hk_timing.py,install.sh,gen-htpasswd.sh,speedex-mitm.service,speedex-mitm-exp.service} root@HK:/opt/speedex-mitm-src/
ssh root@HK bash /opt/speedex-mitm-src/install.sh                                # 生产实例
ssh root@HK SPEEDEX_HK_INSTANCE=experiment bash /opt/speedex-mitm-src/install.sh # 实验实例
```

## 上游 rebase（定期维护）

fork 的 `main` = 上游 `main` + 本目录（独立路径，与上游文件零重叠，正常不会冲突）：

```bash
git fetch upstream
git rebase upstream/main main
git push --force-with-lease origin main   # rebase 后必须强推（已含 lease 防误盖）
```

## 隐私/安全基线

只记元数据/尺寸/内容 hash；绝不落盘 cookie、authorization 头、签名 body、私钥材料；
凭据只存 0600 文件；事件默认内存态。详见 `AGENTS.md` 与 `docs/hk-proxy-timing.md`。

## 2026.09.17-broadcast-anchor-v6

OKX 已登记的 EVM JSON-RPC `eth_sendRawTransaction` 与 Solana BlockRazor bare-base64
`/v2/sendTransaction` 在 request hook 入口冻结时间与链头；要求请求 Origin 为
`https://web3.okx.com`、精确主机/方法/正文结构命中。JSON-RPC 回应必须同 flow、同 id、
HTTP 200 且无 error，result 为完整 EVM hash；Sol 回应为完整 base58 signature。
未确认身份的 `anchorRole=send` 不占授权槽，关窗仍未知标记 `broadcast-identity-unconfirmed`。
同一精确 tx 与 `/broadcast` 合并，所有共同身份键一致才去重，最早请求接管槽位和原链头。
早到平台成功帧保留源时间后重审；RPC 接收回执从不冒充平台成功。若仍有更早且身份未明的
同链发送（已正向同单归并的 dup 腿不算——其身份随已绑槽 formal 腿落定），合格 HK 计时
和链头暂缺，原因 `broadcast-anchor-unconfirmed`。

新增 `orderRequestSource=platform-order|okx-evm-send|okx-solana-send`，由 speedex 消费者
保留至公开 DTO 与块距说明/导出；旧记录缺失不回填。签名正文、query 和认证头不持久化。
发布时按 speedex 的受信节点注册表，仅在已授权实验实例追加下方 overlay 主机；生产模板仍默认关闭。
参考 speedex 主仓 `docs/hk-operations.md`；健康、无资金兼容检查与新资金样本验收分别记录。

## 2026.11.14-fomo-mobula-status-v10.27

FOMO Mobula 路由（BSC/Robinhood）的 HK 成功信号 = 页面自有 toast 条件：
`GET prod-api.fomo.family/swaps/v3/status?provider=mobula&intentId=<id>&wait=true`（先 CORS 预检）的首个
HTTP 200 + `success===true` + `responseObject.status==="SUCCESS"` 响应，时刻取响应 hook 入口（HK monotonic）。
query 键集合必须恰为 `{provider=mobula,intentId,wait}` 且 intentId 为当前 7-7-3 小写 hex 形状，否则只计
`fomoMobulaStatusRequestInvalid`（OPTIONS 计 `fomoMobulaStatusPreflight`）。

- 候选：带合法 `mobulaSwap.intentId` 的 Mobula preview 更新 run 级 Mobula 候选（Relay 候选不变）；执行腿建立时取
  transport 及之前最新的候选（成交后的再报价不改绑）。
- 绑定（请求 hook）：transport 之后首个 intentId 等于腿候选的 GET 绑定该腿，之后同 id 轮询归该腿计数。无合格执行腿、
  无候选、id 不等、多腿同候选、id 已被他腿绑定 → 不绑定，分别计 `fomoMobulaStatusNoExecutionLeg|NoCandidate|IntentMismatch|AmbiguousLeg`，
  腿上 `bindMissing` 写 `intent-mismatch|ambiguous-execution-candidate|intent-already-bound|status-request-not-observed`。
- 成功（响应 hook）：绑定腿首个 SUCCESS 冻结 `tSuccessPushMs`，`successEvidence={version:1, platform:"fomo",
  channel:"fomo-mobula-status-http", status:"success", txHash:null, txDigest:null, matchedKeys:["intentId"],
  correlation:"status-query-intentId+preview-candidate", observedAtMs, requestObservedAtMs, frameSha}`；链取候选 preview 的
  `destinationNetworkId`（56→bsc、4663→robinhood，否则 `fomoMobulaStatusChainUnproven` 不采纳），pendingBind 腿随之补绑槽。
  `FAILED` 记失败证据（`incomplete=mobula-status-failed`、事件 `fomo_mobula_status_failed`），之后 SUCCESS 只计 `AfterFailed`。
- 推送值：`hkL1aMs = tSuccessPushMs − Privy share order 锚`（与其它 FOMO 执行腿同一锚规则；缺锚 → null，不回退 transport）。
- 每腿私有 `evidence.fomoMobulaStatus`：`statusRequests/responses/pending/firstRequestMs/firstSuccessResponseMs/successRequestMs/
  successHeldMs/maxHeldMs/failedAtMs/failedRequestMs/bindMissing/l3MissingReason`（时刻同 tOrderOutMs 钟域；heldMs = 同 flow
  响应 hook − 请求 hook，含 wait=true 服务端持有与 HK↔FOMO 往返）。
- HK-L3′：status body 无 fill hash，不起 receipt，`l3MissingReason="mobula-status-no-fill-hash"`。补齐需要在成功后对
  目的链 Mobula fill 合约做 intent 事件 log 扫描（同 speedex `matchMobulaFillIntent` 规则），本版未实现。
- 被 Mobula status 绑定的腿不再是 F-SOL swaps 候选。intentId 不进事件/日志/计数键/默认 timeline；仅 `full=1` timeline 的已绑定腿带
  `mobulaIntentId`（与 `orderId` 同暴露级，供 EU 授权轮回配；公开投影未登记即丢弃）。

跨仓：speedex `resolvePushForRun` 目前只认 Relay WS 为 FOMO 推送（`fomo-http-poll-not-push`），新通道要出推送必须同步改
EU 消费侧（hk-timing-client / public-projector / docs/hk-proxy-timing.md）。

## 2026.11.14-fomo-provider-signature-v10.28

FOMO Solana 同链买入（F-SOL）的 HK 成功信号 = 页面自有 "Bought" toast 条件（swaps-v2 `waitForConfirmation`）：
`wss://solana-provider-primary.prod-edge.fomo.family/`（@solana/kit rpc-subscriptions；`?token=` query 不读不存）上
`signatureSubscribe(sig,{commitment:"confirmed"})` 对应的首个 `signatureNotification`，`value.err===null` 为成功，
时刻取帧到达 hook 入口（HK monotonic）。页面 socket 失败时的 `getSignatureStatuses` HTTP 轮询只计 `fomoProviderStatusPoll`，不是成功信号。

- **执行签名身份（owner 规则 2026-11-14）**：每个 FOMO Solana 执行 transport 的签名交易体只在内存中读取，仅用于推导首签名
  （链上公开）；body、tx 字节及其 hash 一律不持久化、不记日志（执行 transport 事件的 `bodySha` 自本版起恒 null）。格式取自 FOMO
  swaps-v2 bundle，同一笔买入所有 transport 携带同一 base64 wire tx：Hudson `sendTransactionWeb` 与 Lamport 为裸 base64 正文；
  Helius `/fast` 与 provider-{1,3} `sendTransaction` 为 `params:[b64,{encoding:"base64"}]`；Circular `sendTransaction` 的
  `params[1]` 无 encoding（页面发 base64，按代码确认的缺省解码）；Jito `sendBundle` 取 `params[0][0]`（`[1]` 是独立 tip tx）。
  显式 `encoding:"base58"` 按 base58 解码；其它缺 encoding / 批量 / 非确认方法均不解码，计 `fomoExecSig.<reason>`。
- **同签名折叠**：携带与上一执行腿相同推导签名的 transport 即同一笔交易（如 Hudson 2s 超时后的 provider-1/3 回退发送），
  不受 2s 窗口限制并入该腿，绝不另建 order 腿（`fomoExecSigFold`）；不同签名仍按原 2s 规则。
- **fanout**：同一逻辑执行（2s 折叠）内所有推导签名必须一致，否则腿无身份（`evidence.fomoExecSig.missingReason=
  fomo-exec-sig-conflict`，`fomoExecSigConflict`）。签名存私有腿字段 `_fomoExecSig`，只在 `full=1` timeline 以
  `fomoExecutionSig` 输出（与 orderId 同暴露级）；默认 timeline 只有 `evidence.fomoExecSig={derived,conflict,missingReason,
  transports:[{transport,sigDerived,reason}]（≤8，余计 overflow）,overflow,previewSource}`。
- **绑定**：provider WS 客户端 `signatureSubscribe` 的 params[0] 等于某开窗执行腿的推导签名、且不早于该腿首个执行 transport →
  绑定（`fomoProviderSubscribeBound`）；无腿 / 多腿 / 早于执行 / 否决或关窗 → 不绑定并计数。通知只经同连接 reqId→subId→sig
  映射还原签名（与 OKX fullnode 同一 `_sol_subs` 映射；OKX 行为不变）。
- **成功**：绑定腿首个映射通知冻结（之后计 `fomoProviderNotifyRepeat`）。`err===null` 且腿的冻结 preview 来源为 `solana`
  （执行 transport 及之前最新 v3 preview 为 v1Swap；Relay/Mobula 路由页面同样订阅源链签名，这些腿记
  `successGateFail=preview-not-solana`，不采纳）→ `tSuccessPushMs`、`chain=solana`、`txHash=sig`、pendingBind 补绑，
  `successEvidence={version:1, platform:"fomo", channel:"fomo-provider-signature-ws", status:"success", txHash, txDigest,
  matchedKeys:["txHash"], correlation:"provider-signature-subscribe+exec-sig", observedAtMs, subscribeObservedAtMs, frameSha}`
  （`subscribeObservedAtMs` 为 raw-only，同 Mobula 的 `requestObservedAtMs`）。`err` 非 null → `evidence.fomoProviderWs.failure`
  （同形，`status:"failed"`），`incomplete=fomo-provider-signature-failed`，事件 `fomo_provider_signature_failed`。
- **优先级**：swaps REST（`fomo-swaps-rest`）仍照旧采纳作诊断；同签名的 WS 成功替换 REST 成功（`fomoProviderReplacedRest`，
  `fomoProviderWs.replacedRestAtMs`），REST 引起的 swaps-* incomplete 移入 `fomoProviderWs.restIncomplete`；其它已采纳成功不被替换。
  有推导签名时 REST 记录 signature 必须相同，否则 `incomplete=swap-record-sig-conflict`（`fomoSwapsExecSigConflict`）。
- **推送值**：`hkL1aMs = 帧到达 − Privy share order 锚`（既有 FOMO order 锚规则；缺锚 → null）。
- **HK-L3′**：WS 成功后以精确签名起既有 Solana receipt/slot 轮询（`claim_receipt_poll`，链已由 preview+精确签名证明）；
  不在 transport 时提前起轮询（链在成功前未证明），故首次 receipt 观察不早于成功帧。
- 每腿 `evidence.fomoProviderWs={bound,bindMissing,subscribes,subscribeMs,notifications,firstNotifyMs,successGateFail,
  replacedRestAtMs,restIncomplete,failure}`（时刻同 tOrderOutMs 钟域）。计数：`fomoProviderWsClient/Server`、`fomoProviderSubscribe(
  Bound|NoLeg|Ambiguous|BeforeExecution|LegIneligible|Repeat|Invalid)`、`fomoProviderNotify(Unmapped|NoLeg|Repeat|BeforeSubscribe)`、
  `fomoProviderSignatureAdopted|Failed`、`fomoProviderSuccessGate.<reason>`、`fomoProviderAfterSuccess`、`fomoProviderWindowClosed`。

跨仓：speedex `resolvePushForRun` 只认 Relay WS 为 FOMO 推送；新通道出推送须同步改 EU（shared-push-timing / public-projector /
docs/hk-proxy-timing.md）。

## 2026.11.15-gmgn-sell-push-v10.29

GMGN 卖单出推送：`tg_order_info` / `tg_processed_order_info` 条目 `st==="successful"` 且 `si∈{"buy","sell"}` 为成功，
其它 `si` 不算。OKX/GMGN `successEvidence` 新增 `side`：GMGN = 同 entry 的 `si`；OKX `dex-*-order-info` 的 dexData 无
schema-known 买卖方向字段 → 恒 `null`（帧里出现的未登记键不采信）。`version` 仍为 1，其它键不变。

- 路由、锚、新鲜度、早到缓冲、首写冻结、冲突/否决门全部不变：卖单成功只落在响应锚（oi/h）正向匹配的腿上；他单卖出
  （共同键全不等）零命中，部分相等计 `wsFrameAnchorConflict`，都不落到买单腿。早到缓冲单元保留 `side`。
- 买单腿成功与 v10.28 完全相同，另带 `side:"buy"`。

跨仓：speedex public-projector 的 success 键白名单未登记 `side`（公开投影丢弃）；EU 卖单推送需由消费侧绑定卖单 run 与腿。

## 2026.11.17-helius-sol-v10.30

OKX Solana 链域到达新增 Helius 通道（owner 决定 2026-11-17）。2026-11-11 起 OKX FULLNODE `signatureNotification` 静默，
OKX Solana 腿 `channelArrivals` 全空；页面另开 `wss://atlas-mainnet.helius-rpc.com/?api-key=…` 做确认订阅。

- **allow_hosts**：生产 `speedex-mitm.service` 新增精确 `atlas-mainnet\\.helius-rpc\\.com`（同组锚定 + `(:\\d+)?`）。不放行
  helius-rpc.com 其它子域；`sender.helius-rpc.com`（FOMO 执行 transport）不变。实验 unit 不镜像生产的 OKX 解密，不加。
- **映射**：分派只用 `pretty_host` 精确等值。出站 `transactionSubscribe`：`params[0]` 为对象、`signature` 合法 base58、
  `failed` 恰为 `false` 时登记 reqId→sig（否则清掉同 reqId 待回应）；入站 `{"id":reqId,"result":subId}` 绑定 subId→sig；
  `transactionUnsubscribe` 清 subId；连接结束清全部。存储为连接级 `flow.metadata`，与 fullnode `_sol_subs` 分开。
- **到达**：`transactionNotification`（result 只有 `slot/signature/transactionIndex`，无 meta/err）只在 subscription 命中映射且
  `result.signature` 等于映射签名时，经 `note_channel_arrival(..., platform="okx")` 记该签名锚的开窗 OKX 腿
  `channelArrivals` `{channel:"helius", api:"transactionNotification", ms}`——与 fullnode 同钟（帧到达 hook 入口，HK monotonic，
  ms 相对 tOrderOutMs）、同形、每腿每通道首到冻结。`failed:false` 订阅只推成功交易，但这仍是链域到达，不进成功/资格/锚管线。
- **诊断计数**（`/health diag` 与 run diag）：`heliusSubscribe|SubscribeIneligible|SubscribeReply|Unsubscribe`、
  `heliusNotify|NotifyUnmapped|NotifySigMismatch|NotifyInvalid`；到达本身计入既有 `chainChannelArrivals`。
- **秘密**：api-key query 不读、不记、不入事件/diag/timeline/health/captureIdentity；帧体不持久化。离线测试钉住全部持久化面。

跨仓：EU 消费侧目前只认 `nodeone|fullnode`——`scripts/lib/public-projector.mjs` channelArrivals.channel 枚举（公开投影丢弃）、
`web/shared-push-timing.mjs` `chainArrivalEligible`（不参与 earliestHkSignal 供值）、`web/history-view.js` `CHAIN_DOMAIN_LABEL`
（不出推送源行）。登记 `helius`（标签 HELIUS）后才展示/供值；同步 docs/hk-proxy-timing.md。

## 2026.11.18-late-send-merge-v10.31

OKX Solana late same-signature sends merge into the formal leg (LA batch 1794929320826-was1x r4).

- **Cause**: the Solana `/multi/broadcast` leg has no chain at request time → `pendingBind`, no slot. Nozomi sends that arrive
  after it with the same signature run request-time dedup while no bound formal exists, so they stay `anchorRole:"send"`,
  non-dup (shared transport cannot self-promote; `bind_shared_sol_send` only takes sends at or before the platform order).
  The formal binds later at receipt backfill and nothing revisits them → every later OKX Solana order leg on the chain is
  `broadcast-anchor-unconfirmed` with `hkL1aMs:null`.
- **Fix**: when an OKX formal order leg binds a slot (receipt backfill, promotion, `bind_shared_sol_send`, reopen), send legs of
  the same run that arrived at or after it with an identical anchor `txHash` go through the existing dedup
  (`_dedup_order_identity_locked`: chain, all shared keys, veto gates) → `dup:true`, `mergedIntoFlowId` = formal; their
  in-flight receipt pollers redirect to the formal leg. Sends earlier than the formal still go only through
  `bind_shared_sol_send` (earliest send keeps the anchor).
- **Unchanged**: a send with no tx identity, a tx matching no bound leg, a formal that never binds, or a merge overturned by
  identity conflict / formal veto (`_multiOrderSuspect`, never re-merged) still poisons later legs (juk09/7ngeq boundary).
  Merged send legs keep their own `incomplete:"broadcast-identity-unconfirmed"` (same as existing merged fan-out sends).
  Timeline/DTO shape unchanged.

## 2026.11.19-gmgn-trade-hash-v10.32

GMGN chain-domain arrival from the page's `TradeHash` race leg (GMGN live page code, 2026-11-19). On `wss://ws.gmgn.ai`,
channel `wallet_trade_data`, an entry `{"t":"hash","ch":…,"h":<block height>,"d":{"h":<txHash>,"s":1,"ec":""}}` is the
page's success toast leg; it usually arrives 30–70 ms before `tg_processed_order_info`, and on BSC 87–387 ms after the tx's block
(a post-execution signal).

- **Capture scope**: `ws.gmgn.ai` is already decrypted (production `allow_hosts`) and classified `gmgn` (`RULES.gmgn.ws_hosts`);
  `allow_hosts` unchanged.
- **First-entry rule** (owner decision; GMGN page `entries.filter(t==="hash").find(e => e.d.h === txHash)`): per frame and per
  hash, only the first `t=="hash"` entry with a non-empty string `d.h` decides, and it is a success iff its `d.s` is truthy.
  Later entries with the same hash (`_ident_norm` txHash: EVM case-insensitive) are ignored: `s:0` then `s:1` records nothing,
  `s:1` then `s:0` records.
- **Arrival**: for each deciding entry with truthy `d.s`,
  `note_channel_arrival(chain, d.h, "gmgn_trade_hash", "wallet_trade_data", tObs, platform="gmgn")` records
  `channelArrivals {channel:"gmgn_trade_hash", api:"wallet_trade_data", ms}` on the open GMGN leg (buy or post-round sell) whose
  anchor `txHash` equals `d.h` exactly — same HK monotonic clock (hook entry), same anchor (`ms` relative to `tOrderOutMs`)
  and first-arrival freeze as `fullnode`/`helius`. The entry's top-level `h` (block height) is never a hash. `chain` = the same
  entry's `ch` when it maps to a known chain (a conflicting leg chain records nothing), else the leg's chain. Falsy `d.s`
  (failure carries `d.ec`), missing/non-string `d.h`, another hash (e.g. a sell's approve tx) or a non-GMGN leg record nothing.
- **Unchanged**: arrival record only — never platform success, eligibility, anchors, `hkL1aMs` or incompleteness; the frame
  still goes through the unit pipeline, where `wallet_trade_data` yields no unit.
- **Diagnostics** (`/health diag` and run diag): `gmgnTradeHashEntries` (deciding success entries seen in an open window),
  `gmgnTradeHashArrivals` (new first arrivals); arrivals also count in the existing `chainChannelArrivals`.
- **Privacy**: only the arrival fields above are stored; the frame body, wallet (`wa`), block height and token data are not.

Cross-repo: the EU consumers register `gmgn_trade_hash` — `scripts/lib/public-projector.mjs` channelArrivals.channel enum,
`web/shared-push-timing.mjs` `chainArrivalEligible` (GMGN legs, supplies a value on BSC only) and the history push-source label
`TRADE_HASH` (`web/history-view.js`). The speedex side applies the same first-entry rule.

## 2026.11.22-axiom-sol-v10.33

Axiom (`axiom.trade`, Solana) as a fifth HK-observed Solana platform (live product facts 2026-11-22, five funded
round trips). New platform `axiom`, `orderRequestSource="axiom-solana-send"`; `schemaVersion=2` / `hook-observed@v2`
and every other platform's behaviour are unchanged (no new `RULES` entry: Axiom has its own exact dispatch).

- **Send anchor (request hook)**: Origin exactly `https://axiom.trade` and one of
  - `POST api.axiom.trade/wo/batched-send-tx-v4`, ASCII body `0|0|<opaque prefix>:<base64 signed tx>` (exactly one tx;
    prefix per v10.34 below);
  - `POST <region>.lunar-lander.hellomoon.io/sendBatch-cors` for the 14 regions
    `lax nyc fra ams ash sgp tyo lon syd uae chi dal sea lit`, binary body = u16 big-endian length covering exactly
    the rest of the body + the raw tx.

  The tx must be Solana v1 (first byte 0x81) with exactly one required signature and pass the full v1 layout check
  (`_sol_v1_first_sig`); the signature is then its trailing 64 bytes (base58). Legacy/v0, multi-signer, bare,
  truncated/trailing, other Origin/path/method/host → no leg, only `axiomSendUnproven.<transport>.<reason>`
  (`origin|size|body-shape|not-v1|not-single-signer|v1-layout`). The FIRST proven send of a signature opens an
  `anchorRole:"order"` leg on chain `solana` (slot bound at once), freezing `tOrderOutMs`, wall time and the chain-head
  snapshot at that hook, and starts the Solana receipt/slot poll from the signature (HK-L3′).
- **Fanout fold / veto**: every later send with the same signature (Hellomoon fanout + Axiom API, any time in the window)
  folds into that leg (`axiomSendFold`, event `axiom_send_req`, per-transport counts in `evidence.axiomSend`). A different
  signature within `AXIOM_SEND_FANOUT_MS` = 1000 ms of the latest leg's first send vetoes that leg
  (`_anchorVeto`, `incomplete="axiom-send-sig-conflict"`, `hkL1aMs` null) and the conflicting signature never gets a
  leg in that run (`axiomSendSigConflict`, `axiomSendConflictSig`). Sends after 1 s with a new signature are a new trade.
- **Success (HK-L1a′)**: a server frame on `cluster[a-z0-9-]*.axiom.trade` (WS opened from a page shared worker)
  `{"room":"sol-v2|user-tx:<id>","content":[<sig>,<iso time>,"buy"|"sell",<pair>,…]}` (accepted shapes per v10.34 below) whose
  `content[0]` equals an open, non-vetoed Axiom leg's signature, at or after its send hook → `tSuccessPushMs` (frame hook
  entry, first adoption frozen), `successEvidence={version:1, platform:"axiom", channel:"axiom-user-tx-ws",
  status:"success", txHash, txDigest, matchedKeys:["txHash"], correlation:"user-tx-room+send-sig", observedAtMs,
  frameSha, side}`. Frames without `user-tx:` (public `sol-v2|t:<mint>` trades, prices, slots, blockhash, gas-prices,
  balance-updates-user) are skipped before decode; malformed user-tx frames count `axiomUserTxInvalid.<reason>`; other
  signatures `axiomUserTxNoLeg`; diag also `axiomUserTxAdopted|Repeat|BeforeSend|LegIneligible`.
- **Helius chain-domain arrival**: on `<name>-fast-mainnet.helius-rpc.com` the page's
  `signatureSubscribe([sig,{commitment:"processed"}])` registers reqId→sig, the `{"id":reqId,"result":subId}` reply binds
  subId→sig (connection-level, separate from OKX maps), and a mapped `signatureNotification` with `value.err` present and
  null records `channelArrivals {channel:"helius", api:"signatureNotification", ms}` on the Axiom leg with that anchor
  signature (`note_channel_arrival(..., platform="axiom")`, first arrival frozen). Not success. Diag `axiomHelius*`.
- **Decrypt scope**: production and experiment units add exactly `api.axiom.trade`, `cluster[a-z0-9-]*.axiom.trade`
  (first-party form) and the 14 Hellomoon hosts (overlay section 2026-11-22). The Axiom page's Helius WS is added
  as one exact host, `greer-651y13-fast-mainnet.helius-rpc.com` (read from the live page 2026-11-22; no query key);
  the addon dispatches the form `[a-z0-9]+(-[a-z0-9]+)*-fast-mainnet.helius-rpc.com` (never atlas-mainnet/sender), so
  if Axiom moves to another name the helius arrival is honestly missing until that exact host is added. Decrypting `api.axiom.trade` exposes all Axiom REST traffic (login/session, cookies,
  auth headers, quotes, balances); the addon reads only host/path/method/Origin, parses only the exact send POST body
  in memory and keeps nothing of bodies, prefixes, headers, query, response bodies, user uuid or pair.
- **Timeline**: `evidence.axiomSend={firstTransport, transports:{axiom-api|hellomoon: n}, conflict}` (counts only).

Cross-repo: the EU consumer must register platform `axiom`, `orderRequestSource` `axiom-solana-send`, success channel
`axiom-user-tx-ws` and the Axiom helius arrival (api `signatureNotification`; the OKX helius gate only accepts
`transactionNotification`) in hk-timing-client / public-projector / shared-push-timing / docs/hk-proxy-timing.md.

## 2026.11.22-axiom-sol-v10.34

Fixes two parser rejections found in the first funded Axiom batch through LA v10.33 (1 buy + 1 sell):
`axiomSendUnproven.axiom-api.body-shape` 2 (Hellomoon anchored and folded) and `axiomUserTxInvalid` 2 (both success frames
seen, rejected → `hkL1aMs` null, `incomplete: no-success-observed`). Every other platform is unchanged.

- **api.axiom.trade body prefix**: the live body is `0|0|<9-char prefix>:<1360-char base64 tx>`. The prefix is not base64
  of 6 bytes and its alphabet is unknown, so it is an opaque bounded token `[^|:]{1,32}`. It is matched by shape only,
  never decoded or kept. `0|0|` stays exact, exactly one `:` separates prefix and tx, the tx is strict base64 and must
  pass the single-signer v1 check. Missing/over-long prefix, a second tx, non-base64 tx → `body-shape`; legacy/multi-signer
  → `not-v1` / `not-single-signer`.
- **user-tx frame shapes**: mitmproxy hands every message as decompressed bytes (wsproto undoes permessage-deflate inside
  the connection; same code in mitmproxy 11.0.2 and 12.3.0), and the opcode stays visible. Accepted: one JSON message, a
  JSON array of messages or newline-delimited JSON (at most 32 items/lines each). In a batch only items whose `room` holds
  `user-tx:` are evaluated; batch entry selection is per v10.35 below. `content` needs ≥ 4 items; the first four are checked and trailing items are ignored. The room id is
  any 8–64 `[0-9a-fA-F-]` (same as the EU consumer). Strictness is unchanged for the signature (base58 shape here, exact
  leg-signature match downstream), the side (`buy|sell`) and the `sol-v2|user-tx:` room class. Public `sol-v2|t:` frames
  never count.
- **Reason-coded rejects**: `axiomUserTxInvalid.<reason>`, reason from the fixed set `AXIOM_USER_TX_INVALID_REASONS` =
  `binary` (BINARY opcode, not UTF-8 JSON), `not-json`, `batch-size`, `top-type-str|num|bool|null`, `room`, `content-type`,
  `content-len-0..3`, `sig`, `at`, `side`, `pair`. There is no un-suffixed `axiomUserTxInvalid` counter any more. A batch with no
  valid item reports the first user-tx item's reason (`room` if none). No values are recorded.

EU consumer: `axiomAddonSupported` (`web/shared-push-timing.mjs`) accepts `-v10.34`; no consumer change.

## 2026.11.23-axiom-sol-v10.35

Review fix for user-tx batches (v10.34 kept only the first valid entry). Every other platform is unchanged.

- `_axiom_user_tx_frame` returns every valid `(sig, side)` entry of a JSON-array / NDJSON batch (at most 32 items/lines;
  one entry per signature, frame order). Each entry is tried on its own against the send legs by exact signature, with
  all existing gates per entry (window closed / vetoed / dup → `axiomUserTxLegIneligible`, `axiomUserTxBeforeSend`,
  `axiomUserTxRepeat`, unknown → `axiomUserTxNoLeg`, first observation frozen). A frame carrying the previous round's sell
  and this round's buy therefore no longer loses the buy. `axiomUserTxAmbiguousBatch` no longer exists.
- One signature with both `buy` and `sell` in one frame is ambiguous: not adopted, `axiomUserTxAmbiguousSide` (once per
  such signature). Other signatures in that frame are still tried.
- `axiomUserTx` counts frames that yield at least one entry. Invalid items keep the v10.34 rule: a frame with no valid
  entry and no ambiguous signature counts `axiomUserTxInvalid.<reason>` for its first user-tx item (`room` if none).
- Timing unchanged: `tSuccessPushMs` is the proxy observation time of the frame for every entry it carries.

EU consumer: `axiomAddonSupported` (`web/shared-push-timing.mjs`) accepts `-v10.35` (≥ v10.33); no consumer change.

## 2026.11.23-axiom-evm-v10.36

Axiom EVM (BSC + Robinhood) order-out legs (live captures 2026-11-23). New `orderRequestSource="axiom-evm-send"` on platform
`axiom`. **EVM has order-out legs only**: Axiom has no hash-carrying platform success push on EVM (the client-socket room
`<bnb-v2|rh-v2>|balance-updates-user:<uuid>` carries no tx hash), so there is no success adoption, no HK-L1a′ and no
receipt poll for these legs. Axiom Solana (`axiom-solana-send`, user-tx success, Helius arrival) is byte-identical.

- **Sends (request hook, Origin exactly `https://axiom.trade`)**: `POST api.axiom.trade/wo/batched-bnb-send-tx`
  (transport `axiom-bnb-api`) and `/wo/batched-rh-send-tx` (`axiom-rh-api`), JSON object whose `txs` is a list of 1–4 raw
  signed txs (other keys are not read); `POST sequencer.mainnet.chain.robinhood.com/` (`rh-sequencer`), one JSON-RPC 2.0
  `eth_sendRawTransaction` with `params == [raw]` (the page's fanout of the same RH tx ~2 ms after the api POST). Other
  sequencer JSON-RPC methods are not sends: counted `axiomEvmSequencerOther` only.
- **Decode (in memory only)**: each raw tx must be an EIP-1559 type-2 tx that decodes exactly (12 fields, canonical
  scalars, `to` 0/20 bytes, yParity 0/1, r/s non-zero). Kept: `txHash = keccak256(raw)`, `chainId` (56 → `bsc`,
  4663 → `robinhood`, read from the signed tx only), `nonce`, and `call` = `approve` iff the signed calldata selector is
  `0x095ea7b3`, else `other`. Raw bytes, bodies, headers, query, r/s and calldata are never kept. The route is only a
  cross-check: a chainId that disagrees with the endpoint (bnb ↔ 56, rh/sequencer ↔ 4663) rejects the body.
- **Rejects**: any undecodable body/tx rejects the whole POST (no partial batch) and counts
  `axiomEvmSendUnproven.<transport>.<reason>`, transport ∈ `axiom-bnb-api|axiom-rh-api|rh-sequencer`, reason from the fixed set
  `AXIOM_EVM_UNPROVEN_REASONS` = `origin|size|body-shape|tx-hex|tx-type|tx-rlp|tx-sig|chain-id|route-chain`. No values.
- **Legs** keyed by `(chainId, txHash)` within the run. The first observation of a tx opens the leg (`tOrderOutMs`, wall
  time and chain-head snapshot frozen at that request hook; chain from the decoded tx). Every later send of the same hash
  (api + sequencer, any order, any time in the window) folds into it (`axiomEvmSendFold`, `fanout` += 1, per-transport
  counts). Different hashes are separate legs and never fold or veto each other (a new-token buy = swap leg + approve
  leg ~8 ms apart).
- **Roles / slots**: a swap (`call:"other"`) is `anchorRole:"order"` and binds its axiom×chain manifest slot; an approve
  (`call:"approve"`) is `anchorRole:"approve"` — its own leg, never a slot, not counted in `attempted`. Diag
  `axiomEvmSendLeg` / `axiomEvmSendApproveLeg`.
- **Chain authorization**: with a detail manifest, a tx whose chain has no axiom (or platform-less) spec gets no leg and
  counts `axiomEvmSendChainUnauthorized`. Without a detail manifest there is nothing to check (same as other platforms).
- **Timeline**: `evidence.axiomEvmSend={chainId, nonce, call, firstTransport, transports:{<transport>: n}, fanout}`;
  `txHash` = the decoded hash; at window close `incomplete="no-success-observed"`, `hkL1aMs`/`hkL3Ms` null. The response
  hook records only `axiom_send_ack` (transport/status), never the response body.
- **Decrypt scope**: production and experiment units add exactly `sequencer\.mainnet\.chain\.robinhood\.com` (overlay
  section 2026-11-23). `api.axiom.trade` was already intercepted for Solana (v10.33) and covers both EVM send paths.
  The experiment unit still does not intercept OKX's `rpc.mainnet.chain.robinhood.com`.

EU consumer: must run with v10.36 — accept `-v10.36` in `axiomAddonSupported` (`web/shared-push-timing.mjs`), register
`orderRequestSource` `axiom-evm-send`, `anchorRole` `approve` (excluded from attempted/metrics like other non-order roles),
`evidence.axiomEvmSend` and the diag keys `axiomEvmSendLeg|ApproveLeg|Fold|ChainUnauthorized`, `axiomEvmSequencerOther`,
`axiomEvmSendUnproven.<transport>.<reason>` in hk-timing-client / public-projector / shared-push-timing /
docs/hk-proxy-timing.md. Axiom EVM legs supply no push/L1a value.
