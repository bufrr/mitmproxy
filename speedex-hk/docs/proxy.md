# 代理支持（SOCKS5 / HTTP）调研与方案

2026-09-03 调研 + 实测 + 实现。目标：研究 Chrome（CDP :9222，四平台产品路径的全部
页面流量）走可配置的 SOCKS5/HTTP 代理出口，**UI 可配、运行时热切换、零重启**。

## 调研结论（均已实测验证）

1. **Chrome 启动参数 `--proxy-server`**：进程级、仅启动时生效——初版用它，后来
   被更优的扩展路径取代（保留为历史注记）。
2. **CDP `Target.createBrowserContext(proxyServer)`**：运行时可为**新** browser
   context 指定代理，但它是 incognito 式隔离 context——不共享默认 context 的
   登录态（我们的平台会话都在默认 context），对本场景无用。
3. **`chrome.proxy` 扩展 API（选定方案）**：扩展声明 `proxy` 权限后
   `chrome.proxy.settings.set/clear` **运行时**改全浏览器代理（含默认 context），
   同一进程零重启。本机实证（同一 Chrome 进程）：直连 → set socks5 活代理 →
   页面经代理；set 死端口 → `ERR_PROXY_CONNECTION_FAILED`（真切换、无回退）；
   clear → 直连恢复。`levelOfControl` 读回 `controlled_by_this_extension`。
   - set 后 level = `controlled_by_this_extension`；2026-09-05（review R15）起
     clear 改为**显式 `mode:'direct'` set**——`settings.clear` 只是释放本扩展
     控制权，底层可能回落 system/PAC，不等于直连；读回核验要求 mode 恰为
     `direct`（无 singleProxy ≠ 直连）。
   - scope `regular` **跨重启持久**（写进 profile prefs）——好处是重启后仍在；
     坑是扩展被移除时代理残留（tailscale 踩过），故启动器每次启动做 reconcile
     对齐（见下）。
4. **DNS**：Chrome SOCKS5 客户端远端解析（启动参数路径下 Playwright 还会加
   `--host-resolver-rules=MAP * ~NOTFOUND` 黑洞本地 DNS，构造上无泄漏）。
5. **认证**：HTTP(S) 代理认证经 `webRequest.onAuthRequired`（isProxy）应答——
   已用 gost 带认证本地实证（gost 日志可见 `user=testuser` 的 Chrome 请求）。
   2026-09-05（review R03/R16）加固：
   - 应答按**挑战者绑定**：仅当 `challenger.host/port` 就是当前生效端点才供凭据
     （A→B 切换后 A 的在途挑战拿不到 B 凭据；其它/系统代理一律不供）；
   - SW 重启从 `chrome.storage.session` 恢复带世代屏障——模块加载后发生过显式
     set/clear，迟到的旧恢复结果直接作废（不覆盖新状态）。
   **SOCKS5 认证不支持**（Chrome/协议层）——带认证 SOCKS5 上游用本地中继：
   `gost -L socks5://127.0.0.1:PORT -F socks5://user:pass@upstream:port`。
6. **范围**：代理作用于 Chrome 全进程网络栈——页面 HTTP/WS、扩展 SW（OKX
   Wallet 签名 API）全覆盖。Node 侧流量（链 RPC 回执/块时间）不经 Chrome，
   不受其影响（那些不是平台可见流量，无需代理）。

## 架构（单控面原则）

**运行时代理只由 `extensions/proxy-switch`（chrome.proxy）控制，不用启动参数**——
两者并存时扩展优先，双写必漂。

- `extensions/proxy-switch/`（repo 自带，MV3）：SW 暴露
  `globalThis.speedexProxy.set(server, auth?) / clear() / get()`；
  onAuthRequired 应答 HTTP 代理认证（凭据存 chrome.storage.session，仅内存）。
- **启动器**（`start-research-chrome.mjs`）：`--load-extension` 加载
  proxy-switch（与 OKX 扩展并列；**只传 load-extension，不传
  `--disable-extensions-except`**——后者会禁掉 profile 里已安装的 OKX 钱包，
  2026-09-03 实证回归）。每次启动按期望配置 **reconcile**（residual 清除 /
  设置对齐 + 读回校验）；配了代理而对齐失败 → 拒启（fail-closed，绝不让
  「以为有代理、实则直连」的会话跑起来）。启动后 egress 探针实测出口并写
  `artifacts/inject/chrome-runtime.json`。
- **dashboard**：
  - `GET /api/proxy`：`proxies[]`（文件列表：id/name/hasAuth/scheme/hk/at + HK 节点
    remote 画像；NR-15 起 HK 条目另带 `hkInstance`（注册表实例名）
    与 `hkRemoteState` 终态机 ok/loading/pending/unavailable/unregistered）、
    `envOverride`（CHROME_PROXY 在役标记 {set,valid}——
    env 优先级只在启动 reconcile 生效，见「配置面」）、
    `effective`（chrome-runtime.json 派生：proxied 三态布尔/proxyId/proxyRev/
    proxyAuthRestore/id/known/at/port——`proxyAuthRestore=failed:…` 时 known=false，
    恢复失败不当生效）、`unknownEffective`（生效不在已保存列表）、`configError`、
    `chromeAlive`、`cdpPort`。P1-02（2026-09-09）地址面退化：url/masked、
    effective.egressIp、remote.egress.ip、envOverride host 一律不透（端点公网可达；
    合同见 export-api.md §3.8 末）。**只读缓存**——HK 画像不经
    GET 触发（2026-09-05 review R31：`?fresh=1` 不再开 SSH；匿名/公网 GET 不得
    派生运维通道）。
  - `POST /api/proxy`：保存/清除期望配置 → `artifacts/inject/proxy-config.json`
    （0600、原子写、先校验后落盘；凭据拆出单列、url 规范化无凭据——凭据
    write-only，GET 永不回显）。`hk:true` 的 host 必须在受信注册表
    `configs/hk-nodes.json`（R31：SSH 目标只出自注册表，条目不存 sshTarget，
    未知节点 400 拒存）；保存成功后在控制门内顺带刷新已知节点画像。
    `hk:false` 按字段存在性生效（R34：可 true→false 改回）。
  - `POST /api/proxy/refresh`（2026-09-05 新增，控制门）：显式刷新 HK 画像——
    注册表内节点才开隧道。
  - `POST /api/proxy/apply`：热切换——CDP connect → 找扩展 SW → set/clear →
    读回校验 → egress 探针 → 更新 chrome-runtime.json（含 proxyId/proxyRev
    无凭据配置身份，R14）。批次进行中 409（切换断平台新连接，与 watchdog
    batch-hold 同律）；2026-09-05（review R05）起与 Start 共享进程内 proxyOp
    互斥——批次锁 + 进行中 claim/child 判定与 CDP 切换在同一临界区，两个并发
    apply 也互斥。P2-02 起临界区内含跨进程 wallet.lock 活持核验；NR-02 起
    核验升级为 possiblyExecuting——锁 holder 或锁文档 executors 登记执行者
    （orchestrator spawn 的 runner 子进程）任一存活即拒，锁损坏 fail-closed 同拒。
- **UI**（dashboard 首页「代理（研究 Chrome 出口）」卡片）：多代理列表管理
  （名称/地址/可选认证，逐条生效/删除），表单「代理」下拉选本次出口；
  apply 后轮询收敛。
- **每次测试可选**：已保存代理时，表单「代理」下拉出现（直连 + 各条）。Start 的
  proxyId 门：选某条 → spawn 前热切换到它；「直连」→ 热清除；缺省
  （routine/旧客户端）不动现状。现状已一致时零 CDP 触碰；切换失败 fail-closed
  拒 Start。  批次进度 params 记录 `chromeProxyId`/`chromeProxy`/`chromeEgressIp`
  （可比性口径——与 server 侧 exitLabel 区分，代理开启时两者不同）；
  2026-09-05（review R12）起 orchestrator 把同一快照持久进 inject-batch
  `productEgress`（raw 私有；public 投影登记 `ip`——值层粗化 /24——与
  `proxyId`/`proxyName`，proxy host 形不进 public），并进 cohort 指纹键
  `productEgress`（docs/fair-comparison.md——缺快照的旧批按 incomplete 处理）。
  2026-09-07（review PX-03/PX-04）：Start 同时把快照落 run-state
  `request.productEgress`，**显式四态** `state ∈ {direct, proxied, restore-failed,
  unknown}`——chrome-runtime 缺失/无 `proxy` 键时快照为 null，run-state 落
  `state:'unknown'`（此前只落 `proxyId=null/chromeProxy=null`，读者误当直连）；
  最近任务/定时 lastSlot 的标注优先取工件 `productEgress`、run-state 兜底，
  `unknown` 显示「出口未知」，旧批无记录显示「未记录」——都**不推定直连**。
  导出（[export-api](export-api.md)）列尾 `product_egress_state` /
  `product_proxy_id` / `product_proxy_name` 同枚举；public 投影 `proxyName` 走
  label 位（缺名条目默认名 = 去凭据代理 URL，URL/host:port/凭据形不出 public）。

## 配置面（多代理，2026-09-04 v2）

- UI（推荐）：dashboard 首页代理卡片管理**代理列表**（名称+地址+可选认证，
  逐条「生效」热切换/「删除」）；测试表单「代理」下拉选择本次用哪个
  （直连 / 各已保存代理）。
- 存储：`artifacts/inject/proxy-config.json`（v2 `proxies[]` 列表，v1 单条读取时
  自动迁移；0600、原子写、先校验后落盘、url 规范化无凭据、凭据 write-only）。
- API：`GET /api/proxy`（列表+生效态，凭据永不回显）；`POST /api/proxy`
  `{op:'upsert',id?,name?,url,auth?}` / `{op:'remove',id}`；`POST /api/proxy/apply`
  `{id}` 热切该条 / `{}` 切回直连（批次进行中 409；未知 id 404）。
- Start：`proxyId=<id>` 本批热切换到该代理；`'direct'` 本批直连；缺省不动现状
  （routine 不传）。批次进度 params 记录 `chromeProxyId`/`chromeProxy`/
  `chromeEgressIp`。
- env 覆盖（单条）：`CHROME_PROXY=socks5://host:port`，可选
  `CHROME_PROXY_AUTH=user:pass`（仅 http/https）。**优先级只在启动器 reconcile
  生效**（`planProxyReconcile`：env 设置 → 对齐 env，env 非法/对齐失败 fail-closed
  拒启）；运行时 apply 与 Start 不强制该优先级（Start 缺省不动现状）——
  `GET /api/proxy` 的 `envOverride` 只是状态报告（env 在、值合法与否）。
- 启动 reconcile（start-research-chrome 每次启动；决策 = 纯函数
  `planProxyReconcile`）：env 设置 → 对齐 env；否则当前生效在列表内 →
  **按条目 ID 恢复配置+认证**（2026-09-05 review R13：扩展 auth 存
  storage.session 不跨重启——此前只保留地址不 set auth，重启后 407；恢复失败
  在 chrome-runtime 落 `proxyAuthRestore: failed:…` 标明，不假装生效）；不在 →
  清除残留；无 singleProxy 但模式未知（system/PAC/按协议分流）→ 不记直连，
  对齐 direct（R15）；明确 `mode:'direct'` → 保持。env 代理对齐失败/扩展缺失且
  有配置 → 拒启（fail-closed）。**reconcile 异常/清除被拒/模式未知时
  chrome-runtime 省略 `proxy` 键 = 出口 unknown**（2026-09-06 R12 收口：只有真实
  读回核验 `mode=direct` 才写 `proxy:null`；unknown 不派生 direct 指纹，
  Start 快路径对无 `proxy` 键的 runtime 一律真实切换，不当「现状已一致」）。
- 配置身份（2026-09-05 review R14）：chrome-runtime.json 记 `proxyId`/`proxyRev`
  （条目写入时刻 at；无凭据）。同 server 不同认证的条目 masked 同形——身份判定
  （buildProxyStatus 的 effective.id、Start proxyId 门的「现状已一致」判等）一律
  用 id，masked 仅显示与 legacy 兜底。Start 快路径要求 **id+rev 双等**（二轮残留
  收紧：同 id 改 server/auth 推进 rev，runtime rev 过期 = 旧配置/旧认证在途，必须
  真实切换）；启动器认证恢复失败（`proxyAuthRestore=failed:…`）时快路径禁用，
  /api/proxy 的 effective.known=false（UI 行内显示恢复失败——R13 二轮残留）。

## HK 选择性解密脚手架（GMGN/OKX，2026-09-08，默认 OFF）

HK 出口测量本身见 [hk-proxy-timing.md](hk-proxy-timing.md)（支持矩阵/口径合同归它，
本节只登记代理配置面事实）。针对 review §11 的选择性 GMGN/OKX MITM 后续方案
（[plans/hk-selective-mitm.md](plans/hk-selective-mitm.md)——可行性结论、实验 SOP、
授权门禁），本轮落地了**脚手架**，全部默认关闭、未接线生产：

- `deploy/hk-proxy/selective-decrypt.overlay.example`：候选主机 regex 模板
  （DO-NOT-ENABLE，未经 SOP 与授权不得并入任何 unit）；
- `deploy/hk-proxy/speedex-mitm-exp.service` + `install.sh` 的
  `SPEEDEX_HK_INSTANCE=experiment` 分支：隔离实验实例（loopback 代理
  `127.0.0.1:18544` + 控制面意图 `127.0.0.1:18072` + 独立 APP/CA，经 SSH 隧道供
  隔离研究 Chrome），拒绝生产端口/路径，**默认只安装不启动**；
- **默认状态不变**：生产与实验 unit 的 allow_hosts 逐字节一致，GMGN/OKX 保持刻意
  透传——dashboard 代理卡片、`POST /api/proxy`、Start 的 proxyId 门与 chrome-runtime
  快照语义均不受本轮影响。

**2026-09-09 实验激活状态**（执行记录见 plans/hk-selective-mitm.md §6）：实验实例
已在 hk1（38.207.174.197）激活运行（loopback 18544/18072，独立 CA），其 unit 的
allow_hosts = 生产集合 + GMGN/OKX 四主机（实验形态，不回流 repo 模板）；hk2 实验实例
已安装未用（Cloudflare 封其 IP 于 gmgn.ai）；研究 Chrome 经 ~/.pki/nssdb 信任实验 CA；
dashboard 注册代理条目 HK-EXP（`p-tma2kp8y`，隧道端点 127.0.0.1:18544）。**生产
unit（8443/8072）配置不变**；实验隧道由 operator 按需开（`ssh -N -L 18544/18072 hk1`）。
plugin.md §0.2「HK 口径支持矩阵」以生产 unit 的 `allow_hosts` 为运行事实；本节实验实例的
GMGN/OKX 主机扩展属实验形态，不回写该矩阵——两处交叉阅读。

**2026-09-09 受信映射（P1-07，消费者接线的代码半，现场验收属阶段 F 未授权）**：
HK-EXP 这类 loopback 条目（SSH 隧道端点）到实验实例的绑定只出自
`configs/hk-nodes.json` 的节点 `instances` 子表（`hk-vmiss-1.instances.experiment =
{ctrlPort: 18072, proxyPort: 18544}`，git 跟踪、运行时 API 不可改写，与 R31 同一受信面）：
`POST /api/proxy` 的 hk:true 门对 loopback host 要求 `instances.proxyPort` 命中（未注册
端口 400）；Start 派生 `SPEEDEX_HK_CTRL_PORT`（连同既有 SSH/CTRL_HOST 键）透传
orchestrator，`deriveHkTimingPlan` 按注册表现状逐字复核（陈旧/错配/缺端口 fail-closed，
绝不漏到生产 8072 控制面）；隧道身份核验附加 `captureIdentity.ctrlPort` 一致性——同宿
生产/实验实例共享 egress IP，「出口 IP == host」兜底对 loopback 无效且不被使用。
instanceId 是进程随机值（重启旋转），注册表不钉实例身份；注册表现要求 `schemaVersion:1`
+ 单调递增 `revision`（缺失/类型错 fail-closed）。

**2026-09-10 MITM-only 政策（业主指令）**：所有代理任务（含 routine 定时批）一律走中间人
解密路径；原透明代理模式（生产 unit 8443，对 GMGN/OKX 为 TLS 透传）弃用——dashboard 代理
条目 HK（`p-mlycqlxs`）与 HK-2（`p-mtmis5yq`）已从界面删除，当前唯一条目 =
HK-EXP（`p-tma2kp8y`，hk:true），routine `proxyId` 同指。HK-EXP 经注册表 instances 映射到
hk1 实验实例（ctrlPort 18072），funded 批的 HK 计时开窗/关窗由 orchestrator 按该映射自动
完成（阶段 F 的消费者自动接线已启用）。产品流量隧道（127.0.0.1:18544/18072 → hk1 loopback）
由 systemd 常驻服务 `speedex-exp-tunnel.service` 维护（Restart=always；unit 文件已入库
`deploy/speedex-exp-tunnel.service`，2026-09-14 起受 git 跟踪；停机/重启前先停该服务，
否则 18072 端口占用会让 hk-timing 离线测试假红）。生产 unit（8443/8072）在节点上仍在跑，
仅不再被 dashboard 使用；节点侧实例退役属现场操作，另行授权。

**2026-09-10 实例级身份与状态终态（NR-09/NR-15）**：

- **会话级实例期望（重启更新机制）**：开窗时把 `/health` 自报的
  `{instanceId, captureIdentity.ctrlPort, addonVersion}` 记为会话期望
  （`session.expectation`）；关窗拉取前重取 `/health` 复核——instanceId 漂移、
  ctrlPort 漂移/缺失（开窗时有自报）、addonVersion 漂移、健康复核失败、
  `timeline.gone`、timeline 自报实例/ctrlPort 不符，一律 degraded（重启后旧会话
  绝不冒充完整采集）。注册表 `instances.<name>.instanceId` 可选钉（重启后运维随
  revision 更新；env 携带值与钉不符 = 陈旧映射 fail-closed `hk-loopback-unresolved`）。
  节点级 instanceId 钉只描述缺省（生产）控制面，不适用于同宿其它实例。
- **身份方法留档**：每条核验通过的身份带 `identityMethod`——`registry-instance`
  （注册表钉 / instances 绑定 + ctrlPort 自报一致）或 `egress-fallback`（裸 IP 兜底 =
  **弱身份**，仅非 loopback 生产路径）。批 doc `hkTiming.identityMethod` /
  `hkTiming.registryInstance` 随批持久化（raw；public DTO 未登记 → 投影 drop）。
- **当批实际捕获模式**：`captureIdentity`（allowHosts 命中范围/来源/ctrlPort）照旧随
  timeline/health 落批 doc；采集关闭时 disabled 标记显式带 `captureMode:'unknown'`
  （raw-only）——解密配置与「是否启用 timing」是两个维度，已解密路径绝不推断成透传，
  缺证据不猜。
- **buildProxyStatus（NR-15）**：HK 条目解析与 Start 同一受信路径（loopback →
  `resolveHkInstance`）；画像隧道带实例 ctrlPort；DTO 新增 `hkInstance`（注册表实例名
  标签）与 `hkRemoteState` 终态机（`ok`/`loading`/`pending`/`unavailable`/
  `unregistered`）——未知/失败/未登记都是终态文案，UI 不无限「拉取中」（GET 只读
  缓存、绝不 spawn 不变）。

## 本机实验件（.tmp/proxy-lab/，gitignored）

microsocks（编译的 socks5）与 gost（http/http+auth）的部署与四个实测脚本
（launchPersistentContext 代理矩阵、扩展热切换矩阵）。复现：
`make -C .tmp/proxy-lab/microsocks-master && .tmp/proxy-lab/microsocks-master/microsocks -i 127.0.0.1 -p 11081`。
