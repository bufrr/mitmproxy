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

mitmproxy pip 钉版（`install.sh`）：12.2.3（python 3.13 节点）/ 11.0.2（python 3.11 节点），
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
同链发送，合格 HK 计时和链头暂缺，原因 `broadcast-anchor-unconfirmed`。

新增 `orderRequestSource=platform-order|okx-evm-send|okx-solana-send`，由 speedex 消费者
保留至公开 DTO 与块距说明/导出；旧记录缺失不回填。签名正文、query 和认证头不持久化。
发布时按 speedex 的受信节点注册表，仅在已授权实验实例追加下方 overlay 主机；生产模板仍默认关闭。
参考 speedex 主仓 `docs/hk-operations.md`；健康、无资金兼容检查与新资金样本验收分别记录。
