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
| `tests/` | 安装/单元行为测试（Node 22，`node --test tests/*.test.mjs`，15 项） |
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
