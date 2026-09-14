# speedex HK timing proxy（基于本 fork 的 mitmproxy）

speedex 延迟研究 harness 的 HK 出口测量代理：mitmproxy addon + 控制面 + 部署件。
主仓：`git@github.com:bufrr/speedex.git`（dashboard / orchestrator / runners / EU 客户端
`scripts/lib/hk-timing-client.mjs` 等）；本目录是 proxy 侧的**发布副本**——
改动先在主仓 `deploy/hk-proxy/` 完成并过测试，再同步到本分支（目录内容一一对应）。

## 内容

| 文件 | 用途 |
| --- | --- |
| `speedex_hk_timing.py` | mitmproxy addon：下单/成功信号锚关联 + 链 receipt 轮询 + 控制面（`/mark` `/mark/close` `/timeline/<rid>` `/health` `/egress` `/latency`） |
| `test_speedex_hk_timing.py` | addon 离线测试（合成 flow/frame 夹具，144 项）：`python3 test_speedex_hk_timing.py` |
| `install.sh` | 幂等安装/受控重启（用法见文件头；实验实例 `SPEEDEX_HK_INSTANCE=experiment`） |
| `gen-htpasswd.sh` | 代理认证 htpasswd 生成（{SHA}，凭据不进 argv） |
| `speedex-mitm.service` | 生产 unit（0.0.0.0:8443 + 控制面 8072） |
| `speedex-mitm-exp.service` | 隔离实验 unit（loopback 18544/18072，默认 OFF 脚手架） |
| `speedex-exp-tunnel.service` | EU 侧常驻 SSH 隧道 unit（18544/18072 → HK 实验实例 loopback） |
| `selective-decrypt.overlay.example` | GMGN/OKX 选择性解密候选 overlay（DO-NOT-ENABLE，须 SOP + 当次授权） |
| `AGENTS.md` | 本目录的硬约束（凭据/隐私/锚纪律——改动前必读） |
| `docs/` | 协议与运维文档副本（hk-proxy-timing / proxy / hk-selective-mitm） |

## 部署目标版本

mitmproxy pip 钉版（`install.sh`）：12.2.3（python 3.13 节点）/ 11.0.2（python 3.11 节点），
`websockets==17.1`、`msgpack==1.1.2/1.1.0`。上游源码树**不被本目录修改**——
我们只消费 pip 安装的 mitmproxy hook API。

## 上游 rebase（定期维护）

本分支 `speedex-hk` = 上游 `main` + 本目录（独立路径，与上游文件零重叠，正常不会冲突）：

```bash
git fetch upstream
git rebase upstream/main speedex-hk
git push --force-with-lease origin speedex-hk   # rebase 后必须强推（已含 lease 防误盖）
```

fork 的 `main` 保持只跟踪上游（`git push origin upstream/main:main`），不放业务代码。

## 隐私/安全基线

只记元数据/尺寸/内容 hash；绝不落盘 cookie、authorization 头、签名 body、私钥材料；
凭据只存 0600 文件；事件默认内存态。详见 `AGENTS.md` 与 `docs/hk-proxy-timing.md`。
