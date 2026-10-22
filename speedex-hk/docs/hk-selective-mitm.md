# HK 选择性 MITM（GMGN/OKX）后续方案：可行性结论、脚手架与实验 SOP

2026-09-08 立项。来源：[2026-09-08 审查](../archive/2026-09-08-rh-fomo-review.md) §11（当前报告为 ../reviews/2026-09-09-final-review.md）
（可行性复核与实施方案）及 RHF-15/RHF-17 保留项。本文是**计划与脚手架清单**，不是授权，
不是测量合同，不声明任何实验已验收。默认状态不变：**生产与实验实例的 allow_hosts 都不含
GMGN/OKX——两家保持刻意透传**（HK 支持矩阵的现行合同归
[hk-proxy-timing.md](../hk-proxy-timing.md) §2，本文不改它）。

## 1. 可行性结论（基于 review §11 已确立的机制事实）

- **可选择性 per-host MITM，且不触碰透传主机。** 已直接读取两台 HK 节点安装版的
  NextLayer 实现并与 mitmproxy 官方 ignore-domains 文档核实：`allow_hosts` 在 **TLS 握手
  之前的 TCP 层**分流——不在集合内的主机整条连接透传，addon hook 不触发；集合内的主机
  才被解密。因此把 GMGN/OKX 候选主机并入 allow_hosts 不影响 Padre/Binance/FOMO 的
  既有拦截，也不影响其余全部透传流量。
- **安装 CA 只提供信任前提，不等于全量解密。** 研究 Chrome 信任实验 CA 后，仍只有
  allow_hosts 命中的连接被 MITM；透传连接的证书链不变。CA 安装本身不是兼容性风险源，
  风险集中在「被拦截主机的产品路径是否容忍中间人」。
- **历史教训是「全量拦截」而非「选择性拦截」。** commit `0410b4f` 记录的是全量拦截导致
  OKX RH 报价失败、GMGN 页面劣化并伴随内存问题，**未定位到唯一根因**（证书固定、
  ALPN、特定 API 失败均未分离）。选择性方案的实施路径成立，但登录/报价/交易的真实
  兼容性**只有受控实验能回答**——不能从历史失败直接推断选择性可行或不可行。
- **真实兼容性风险清单（实验必须逐项分离定位）：**
  1. **证书固定（cert pinning）**：OKX 扩展后端 / GMGN 页面若钉证书，TLS 握手即败；
  2. **ALPN/协议协商**：h2/h1、WS 升级在 MITM 下协商变化；
  3. **长连 WSS 不重握手**：切换 allow_hosts 后已建立的 WSS 连接继续走旧通道——
     **每次切换必须重建实验连接**，否则观察混入旧模式；
  4. **同宿主耦合**：报价 API 与下单端点共享 `web3.okx.com`——**不能在解密前按
     HTTP path 决定是否解密**，拦截下单端点必然同时拦截报价，报价兼容性测试不可豁免；
  5. **资源水位**：1G 小机解密新增 CPU/内存负载，需水位监控（0410b4f 伴随内存问题）。
- **路线分工（两条路线回答的问题不同）：**
  - **选择性 MITM（本方案）**：目标是 HK 出口的**请求/成功/receipt 观测**（HK-L1a′/L3′
    口径），前提是关联修复（RHF-09/17）完成且兼容性实验通过；
  - **HK runner + Chrome/钱包 + CDP**：目标是**完整 HK 点击体验**（click 发生在 HK，
    浏览器自己终止 TLS 提供明文）——必要主机 MITM 持续不兼容、或要测完整 HK 用户
    体验时选它；代价是迁移登录态与双热会话纪律，需独立验证环境与资金样本。
- **OKX 起点的固有限制**：OKX（尤其 Sol）的 `/broadcast` 可能晚于真正的
  sendTransaction、甚至晚于成功消息——只能作 platform-broadcast-http hook 口径，
  锚晚于成功则保持缺失/单列诊断，**不能裁成 0、不能当最早 tx_out**。若只有 WS 能
  解密，得到的是 HK 接收事件，**不能凭 EU 侧上传的 hash/订单声明补造 HK 请求锚**。

## 2. 脚手架清单（本轮交付；默认 OFF，未接线生产）

| 文件 | 内容 | 状态 |
| --- | --- | --- |
| `deploy/hk-proxy/selective-decrypt.overlay.example` | 候选主机 regex（review §11 原值）、WS-only / 订单 HTTP 两阶段与生产集合的合并 systemd 形式、诊断最小化契约、转义核验方法；全文件 DO-NOT-ENABLE | 模板，任何 unit 未引用 |
| `deploy/hk-proxy/speedex-mitm-exp.service` | 隔离实验 unit：loopback-only 代理 `127.0.0.1:18544`、控制面意图 `127.0.0.1:18072`（`Environment=SPEEDEX_HK_CTRL_PORT=18072`）、独立 APP `/opt/speedex-mitm-exp` 与 CA confdir；**allow_hosts 与生产逐字节一致（GMGN/OKX 透传）** | 仓库新件，未部署 |
| `deploy/hk-proxy/install.sh` | 新增 `SPEEDEX_HK_INSTANCE=experiment` 分支（缺省 production 行为不变）：隔离护栏（拒绝生产端口 8443/8072、生产 APP、生产 unit 名；unit 必须 loopback-only；含 flow_detail≥2/save_stream_file 拒装）；实验实例**默认只安装不启动**，`SPEEDEX_HK_EXP_START=1` 才启动 | 已改，bash -n + 离线测试通过 |

**依赖门已解除（2026-09-09）**：addon `speedex_hk_timing.py` 自 2026.09.08-r10 起消费
`SPEEDEX_HK_CTRL_PORT`（`_ctrl_port()`，非法值回退 8072）——实验控制面 18072 与生产
8072 并存不再 bind 冲突（§6 阶段 A–E 已实证）。实验实例默认不启动仍是**策略门**
（§5 SOP + 当次授权），非技术阻塞。

## 3. systemd 转义核验（review §11 标注待验项）

- 现行生产 unit 在 ExecStart 双引号内写 `\\.` / `\\d`，systemd 折叠为 `\.` / `\d` 交给
  mitmdump——该形式已在两台节点实测分流正确（review §11 核验记录），是**已证明的写法**。
  候选 regex 沿用同律：每个反斜杠写两遍。
- 候选 regex 无 `%`（若有须写 `%%`——systemd specifier 展开）、无内层双引号。
- **必须保持 `$` 锚定 + 端口组**（`(:443)?` 或 `(:\\d+)?` 二选一）：next_layer 匹配
  追加端口（host:port），漏配端口组会让过滤静默失效（hk-proxy-timing.md §9 已载）。
- 改 unit 后不许目测通过——核验进程实际收到的串：

  ```sh
  PID=$(systemctl show -p MainPID --value speedex-mitm-exp)
  tr '\0' '\n' < /proc/$PID/cmdline | grep '^allow_hosts='
  ```

  输出必须是单反斜杠逻辑值；仍含 `\\.` = 转义错误、匹配静默失效。

## 4. 诊断最小化规则（实验模式唯一日志面）

允许：host、已登记 path 类别（schema-known 枚举标签，非原始 path+query）、固定错误类别
（`tls-handshake-fail` / `http-403` / `challenge` / `quote-api-fail` / `resource-exhausted`
等固定词表）、HTTP 状态码、协议（h1/h2/ws）、WS 帧计数与尺寸、资源水位、脱敏身份
（与现行 timeline 短化同口径）。

禁止（违反即实验作废并撤销授权）：Authorization/Cookie 等认证头、token、完整 query
string、签名请求体、完整业务抓包（flow dump / save_stream_file / flow_detail≥2）、
请求修改、自动订单重试、任何独立广播。可执行护栏已内嵌 install.sh 实验分支
（unit 有效行含 flow_detail≥2/save_stream_file → 拒装）；字段级纪律由 addon 侧
（平行工作流）落地，overlay 文件为消费方契约。

## 5. 实验 SOP（对应 review §11 六步；每步的退出/回滚条件并列）

1. **先修关联与资格（RHF-09 late veto、RHF-17 逐 entry 解析 + 请求锚 + 早到 WS 缓冲）——
   硬性前置门。** 当前透传使 GMGN/OKX parser 休眠，三个反例在 r9 仍可复现；启用 MITM
   前必须让反例变为正确的生产回归。本项正由平行工作流进行中。**身份、状态、链必须
   来自同一 entry**；缓冲保留单调源时间/窗口/方向，关窗/重放/冲突不采纳。
2. **隔离无资金实验。** 候选节点先核验无活动任务。`SPEEDEX_HK_INSTANCE=experiment`
   安装隔离实例（loopback 18544 / 控制 18072 / 独立 CA），经 SSH 隧道供**独立研究
   Chrome** 访问——不切换生产 8443/8072、不动现有浏览器、不复制既有 profile、不全局
   ignore-certificate-errors；官方流程建立测试登录，只信实验 CA 公共证书，CA 私钥
   0600 不出 HK。**退出/回滚**：节点有活动任务、addon 未支持控制面端口、隧道身份
   核验失败 → 不启动；`systemctl disable --now speedex-mitm-exp` 即回滚（生产零接触）。
3. **同路由 A/B 逐主机验证。** 顺序：透传基线 → 仅 WS（`ws.gmgn.ai` + `wsdexpri.okx.com`）
   → 订单 HTTP（并入 `gmgn.ai` + `web3.okx.com`）。**每次只改一个因素；每次切换后重建
   实验连接**（旧 WSS 不重握手）。观察证书、ALPN、登录、报价、长连。TLS 失败、
   HTTP 403/challenge、报价 API 失败、资源耗尽**分别按固定错误类别定位**，不以一个
   错误统称「MITM 不支持」。**回滚**：任一阶段失败 → allow_hosts 退回上一通过阶段
   或纯生产集合；实验数据全保留。**隧道结果只是兼容性数据，永不当速度成绩。**
4. **只保留必要诊断**（§4 清单）。不改请求、不自动重试、不独立广播。
5. **单独授权的真实产品采样。** 无资金兼容通过 ≠ 交易计时通过。经 dashboard 授权/
   预检路径明确 chain/token/amount/N（dashboard `buyUsd` 权威 + 平台下限）；**先每
   平台×链 1 笔**，再按授权交错扩大；失败/unknown 保留不自动补跑。逐笔核对 HK/CDP
   订单与 exact tx、链、轮次、源时间与链上证据。**新模式单独 cohort，不借旧批成绩。**
6. **消费者接线与生产切换。** 每批持久化实际解密配置身份（allow_hosts 集合/rev）+
   build/node/instance/capture 模式及缺失原因（RHF-15：切换后不得用当前矩阵解释历史
   批，缺证据显示未知）；区分「TLS 可见 / 订单可识别 / 严格成功 / 回执完整」四层能力。
   public 投影/API/UI/export 同步登记。验收后**另行授权**生产部署：无活动批次的排他
   窗口切换、验证版本/健康/新连接、保留回退配置与全部实验数据。

## 6. 实验执行记录（2026-09-09，用户授权「打通 GMGN/OKX 中间人测试 proxy」）

阶段 A–E 已按 §5 顺序执行并通过；生产 unit（8443/8072）全程未改配置。

- **A 准备**：hk2（149.104.4.51）与 hk1（38.207.174.197）各装隔离实验实例
  （loopback 18544/18072、独立 CA/APP、RHF-19 规范化护栏），hk2 随后发现
  **Cloudflare 按 IP 封 gmgn.ai**（直连 curl hk2=403 / hk1=200）——GMGN 实验只能在
  hk1；实验 Chrome 的 CA 信任走**独立 HOME 的 NSS DB**（不进系统/其它浏览器），
  研究 Chrome 经 ~/.pki/nssdb 信任实验 CA（与生产两枚 HK CA 同机制，热生效免重启）。
- **B 透传基线**：GMGN/OKX 页面经实验代理全通（GMGN 需真实 Chrome UA——
  HeadlessChrome 指纹被 CF 拦，与 MITM 无关），HK 侧零 hook（legs 0/seenReq 0），
  captureIdentity=生产集合。
- **C 仅 WSS**：先 ws.gmgn.ai 后 wsdexpri.okx.com 逐变量并入实验 allow_hosts；
  证书 issuer 证明（mitmproxy CA ↔ 其余主机 WE1 透传）、GMGN 365 帧入 / OKX 76 帧入、
  页面功能正常。首个失败原因是实验浏览器未信实验 CA（NSS DB 位置），非产品不兼容。
- **D 订单 HTTP**：gmgn.ai、web3.okx.com 依次并入；页面/报价/长连正常，
  seenReq 381/505；mitmproxy 上游 TLS 指纹未被 Cloudflare 拦。
- **E 产品采样（N=1，RH，gmgn+okx 并行，dashboard 授权快照在案）**：实验控制面
  mark open（manifest 2 槽）→ 批次 1788929547380-airqy confirmed 2/2 → mark/close +
  timeline 拉取。HK 腿逐笔核验：GMGN hkL1a 597.5ms / hkL3 1644.4ms tx
  0xfd54adbd…c4705f；OKX hkL1a 414ms / hkL3 1550.4ms tx 0x3dedfc17…e328ef——
  与 EU 侧 exact tx 逐字节一致，回执 status=1 同块 0x37938cb；counts 2/2/2/0；
  captureIdentity 随批落盘。
- **当前实验状态**：hk1 实验 unit allow_hosts = 生产集合 + GMGN/OKX 四主机（阶段 D/E
  形态，unit 文件仅存在于节点，不回流 repo 模板——模板保持默认 OFF）；实验实例运行中。
  **阶段 F 已完成**：消费者自动接线已上线——funded 批（含 routine 定时槽）一律经
  HK-EXP 条目（loopback 127.0.0.1:18544，注册表 `instances.experiment` 受信映射）由
  orchestrator 自动开窗/关窗采集，逐批落 `hkTiming.identityMethod=registry-instance`
  与当批 `captureIdentity`；透明/直连代理模式已从 UI 与配置面移除（MITM-only，
  2026-09-10 起）。

## 7. 两种可审阅的运维选择（2026-09-10 NR-09/NR-15 收口后）

**两个选择都不由本文授权；选择与执行均需当次另行授权（§8 门禁不变）。**

### 选择 A：启用受信实验采集（GMGN/OKX 选择性 MITM 计时批）

SOP（逐步可审阅；任何一步不符即停）：

1. **注册表核验**：`configs/hk-nodes.json` 的 `instances.experiment` 映射与节点实际一致
   （ctrlPort/proxyPort；可选 instanceId 钉——重启旋转后由运维随 revision 一并更新，
   陈旧钉 fail-closed）。改动注册表 = 运维显式 git 变更，运行时 API 不可改写。
2. **条目与切换**：dashboard 代理条目 HK-EXP（loopback 127.0.0.1:18544，hk:true）经
   `POST /api/proxy` 的 instances 门落盘；Start 选该条目 → 互斥内热切换 + 出口快照 →
   派生 `SPEEDEX_HK_*`（含 CTRL_PORT/实例钉）透传 orchestrator。
3. **开窗身份**：orchestrator 按注册表现状逐字复核（陈旧/错配 → `hk-loopback-unresolved`
   fail-closed）；隧道身份核验 = 注册表实例绑定 + `captureIdentity.ctrlPort` 一致性
   （+实例钉强校验，若钉）；开窗记录会话期望（health 自报 instanceId/ctrlPort/
   addonVersion），关窗复核同实例，漂移/重启 → degraded。
4. **批 doc 核验（每批）**：`hkTiming.identityMethod` 必须为 `registry-instance`；
   `registryInstance='experiment'`；`captureIdentity.allowHosts` 为当批实际命中范围；
   `state='ok'`（degraded 批不当完整采集）；四桶 confirmed+reverted+failed+unknown
   = requested N。
5. **funded 验收单独列明**（当次授权书）：chain/token/account/amount/N/总预算，
   固定 codeRevision / 实例（注册表 revision + 会话 instanceId）/ captureIdentity，
   独立锚（HK 同机 monotonic hook）与同 entry 关联（精确 tx/订单身份唯一回配），
   四桶分母齐全。**先验 GMGN/OKX 两平台，不混入其他平台资金运行**；失败/unknown
   保留不自动补跑。

### 选择 B：恢复正式透传路径

1. 实验 unit 的 allow_hosts 退回生产集合（GMGN/OKX 移出），或
   `systemctl disable --now speedex-mitm-exp` 停实验实例；生产 unit（8443/8072）
   全程不改。
2. dashboard 条目 HK-EXP 翻 hk:false（显式关闭计时 sidecar）或删除；研究 Chrome
   切回生产 HK 条目或直连（apply/Start 的既有互斥与批次 409 不变）。
3. 回读核验：下一非启用批的批 doc 落 `hkTiming:{state:'disabled', reason,
   captureMode:'unknown'}`（采集关闭 ≠ 推断透传；恢复后首批经画像刷新确认实验
   实例 captureIdentity 已回生产集合）。
4. 历史实验批（identityMethod=registry-instance / registryInstance=experiment 且
   captureIdentity 含 GMGN/OKX 主机）属独立 cohort——不与透传批按字段名合并，
   不借旧批成绩。

## 8. 授权门禁汇总（2026-09-14 更新）

2026-09-09：阶段 A–E 经用户「打通 GMGN/OKX 中间人测试 proxy」指令授权并完成（记录见 §6）；
2026-09-10：消费者自动接线（NR-09/NR-15 实例级身份收口）上线，funded 批全走 HK-EXP
实验实例（MITM-only），逐批 captureIdentity/identityMethod 留档——**现场验收已完成**
（逐批证据见 artifacts 批 doc）。仍 gated：

- 任何向生产 unit（8443/8072）的 allow_hosts 变更；
- 实验 unit 的 allow_hosts 变更（新增/移除主机须经 SOP 逐步 A/B + 当次授权）；
- 超出已授权形态的扩大采样（逐批 dashboard 授权，验收口径见 §7 选择 A 第 5 条）。

<details><summary>原授权门禁（2026-09-08 脚手架轮）</summary>

- 启动实验实例（需 addon 控制面端口支持 + 当次实验授权）；
- 任何 allow_hosts 变更（含实验 unit）；
- 无资金实验的 SSH 到 HK 节点与部署动作；
- 真实资金采样（dashboard 授权/预检，逐批）；
- 消费者接线后的生产切换（独立授权 + 排他窗口）。

</details>

（2026-09-08 脚手架轮注记：该轮只交付脚手架与文档——无 git commit、无 SSH、无部署、无服务
重启、无解密启用；阶段 A–E 执行记录见 §6。）
