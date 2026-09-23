"""
speedex HK timing addon（docs/hk-proxy-timing.md）——mitmproxy addon + 控制面。

职责：替 EU dashboard 在 HK 侧打时间戳并关联「下单请求（出站锚）↔ 平台成功信号
（入站锚）」，并从 HK 直接轮询链 RPC 拿 receipt（HK-L3'）。单钟域（HK 本机
time.monotonic）——EU 侧时间戳（mark 里的 tClickEuMs）只作跨域诊断参照，不进
HK 口径算术。

控制面（127.0.0.1:8072，仅 loopback——EU 经 SSH 隧道访问；
SPEEDEX_HK_CTRL_PORT 可覆盖监听端口，缺省/非法一律 8072——隔离实验 unit
speedex-mitm-exp.service 设 18072 与生产同机并存）：
  POST /mark        {"runId":"…","note":…,"manifest":?}  打开采集窗口；manifest
                    （可选）={"rounds": n} 或 {"version","runId","rounds",
                    "legs":[{platform,chain,rounds}…]}——授权轮次登记；legs 明细带
                    per-leg rounds（缺失/非法按 1 计）时 requested=Σ legs[i].rounds，
                    timeline 输出 requested/attempted/observed/unbound + 缺失腿占位
                    （incomplete=no-order-observed）；无 manifest 行为同今。
                    拒绝多窗并存——新窗显式关闭旧窗（旧腿冻结保留既有状态）；
                    建腿原子化——rid 选取+开窗复验+事件/腿写入同一 STORE.lock
                    临界区，关窗后到达的下单请求一律拒绝（计诊断 diag.orderReqNoWindow）。
                    明细 manifest 下 order 腿建腿即绑定授权槽；无槽可绑
                    （链未知/超额）→ unbound 诊断腿（计 diag.orderReqUnbound，不计
                    attempted/占位）；关窗逐腿补缺失状态（no-success-observed/
                    no-receipt-observed/no-execution-proof）。建腿/响应/晋升先按
                    订单身份去重（orderId/clientOrderId 正向匹配已绑槽腿）——同单
                    重复请求归入同一授权槽（dup 诊断，计 diag.orderReqDuplicate，
                    不新增 attempted、不占新槽；身份缺失/冲突不猜，走容量/unbound
                    律）；晋升时链未实证 → pendingBind 等 receipt 实证链回填
                    leg.chain 并补绑（DONE·receipt 两种事件顺序同计数同腿身份；
                    关窗仍未实证 → freeze 时 unbound 落定）；observed ⊆
                    attempted——只计绑槽 order 腿，sign/preview 腿的 receipt 不单独
                    计入。槽绑定只证明槽容量占用，不证明授权轮号——manifest 只带
                    platform×chain×rounds 数量、请求不带轮号（按到达序编号会在
                    首轮未观测时整体前移）。腿 round 恒 null（未知）；到达序另记
                    observationIndex（platform×chain 内从 1 起的纯观察事实）；
                    授权轮由 EU 侧唯一关联（精确 tx / 订单身份）回填
                    authorizedRound，不唯一/缺失保持 null。
  POST /mark/close  {"runId":"…"}                 关闭窗口（此后流量不再归集；腿冻结，
                    迟到帧/迟到 receipt 回写一律丢弃）
  GET  /timeline/<runId>                          拉取该批关联结果（legs[]，hkL1aMs/hkL3Ms 单位=毫秒；
                    counts={requested,attempted,observed,unbound}——attempted 只计
                    绑定了授权槽的 anchorRole=order 腿（另排除 dup 同单重复腿与
                    pendingBind 待实证链腿）；observed ⊆ attempted（绑槽 order 腿中有
                    成功推送或 receipt 观察）；已回收 run 返回 gone:true；
                    热重载丢弃的 run 返回 gone+droppedInReload（跨实例交接）；
                    输出自带方法/采集环境身份 method=hook-observed@v2 + vantage/
                    instanceId/addonVersion/mitmproxyVersion/pythonVersion）
  GET  /health                                    {ok, marks, legs, addonVersion, instanceId,
                    pythonVersion, mitmproxyVersion…}；热重载分裂/bind 失败 → 503 如实报错

时间戳语义：tOrderOut*/tFirstResp*/tSuccessPush* 均为 hook 入口冻结的
request-observed 时刻（HK 本机 monotonic ms）——不宣称物理离站；解析/判定耗时
不计入。

信号规则按仓库侦察定稿（docs/okx.md / gmgn.md / padre.md / binance.md /
fomo.md + tests/classify-ws.test.mjs 等夹具——改规则同步改本注释与文档表）：
  OKX     下单 POST web3.okx.com /priapi/v6/dx/trade/multi/(batch)?Broadcast；
          成功帧 wsdexpri dex-(across|swap)-order-info 且 data.dexData.status==="1"
          （status "0"/abnormalStatus "-1" = 创建帧，不算）；hash=dexData.transactionHash。
          data 为 list 时逐 entry 判定（同一 entry 的身份/状态/链才可组合——
          不 data[0]-only、不跨 entry 拼接）。
  GMGN    下单 POST gmgn.ai /(tapi|td/api|mrtapi)/…/swap_batch_order 或
          /txproxy/v2/send_transaction；成功帧 ws.gmgn.ai 频道 tg_order_info /
          tg_processed_order_info，条目 st==="successful" 且 si==="buy"，按 h/oi 关联。
          data 列表逐 entry 判定（同一 entry 的 h/oi/st/si/ch 才可组合）。
  Padre   下单走 WS msgpack（方法串未钉死）——出站时刻用次优 HTTP：
          POST api.turnkey.com/public/v1/submit/sign_raw_payload；签名调用未证明与
          order 一一对应 → 腿先标 anchorRole:'sign'（不计 attempted、
          不消费 manifest 槽位）；成功帧 = padre 后端（backend3/backend/backend2/
          txn-service.padre.gg）_multiplex msgpack，payload
          节点 txnStatus==="DONE"（严格），hash=同节点 txnHash（键集合见
          runner-core.mjs）——DONE 正向匹配 = 执行证据，命中即原子晋升 'order'。
          **Turnkey ActivityResponse 不含订单/交易身份 → sign 腿无独立锚
          → DONE 永无正向匹配 → Padre HK-L1a′ 暂不可观测（无独立锚）；sign 腿关窗如实
          no-execution-proof，授权槽占位 no-order-observed。DONE/新鲜度门保留给未来
          由 Padre 自有下单通道（client→_multiplex 请求帧，须真实脱敏帧验证）提供的锚。**
  Binance 下单 POST 路径含 /bapi/defi/v2/private/wallet-direct/web-dex/place-order
          （按 path 匹配、不限 host）；响应无 hash（Sol 例外 data.signature）；
          成功帧：bizKey∈WEB3_DEX_*_ORDER_CHANGE|DEX_ALL_ORDER|marketOrderInfo 且
          content.status==="FINISHED"，按 orderId/clientOrderId 强关联；PENDING 帧的
          orderTxId 是 hash 首见（PENDING≠成功，只取 hash 不当成功）。
          nbstream 为二进制信封 → utf8 后取第一个平衡 JSON。
  FOMO    预览/下单共用 POST prod-api.fomo.family/swaps/v2（L7 路径/体不可区分——
          两阶段实证见 docs/fomo.md）：腿一律先标 anchorRole:'preview'（不计
          attempted、不消费 manifest 槽位、不单独产成功）；Relay success 帧正向
          匹配 requestId≡relaySwapId = 执行证据（Relay 只对已执行请求推 success），
          命中即原子晋升 'order' 并补授权槽绑定。requestId=responseObject.v2Swap
          .relaySwapId；成功帧 ws.relay.link request.status.updated 且
          data.status==="success"；fill hash=data.txHashes[0]。F-SOL 同链腿
          无 Relay 帧（DFlow 轮询），HK-L1a' 标 incomplete（known limitation）。

成功判定统一锚纪律：成功状态/hash/id 必须与「请求/响应建立的
独立锚」（leg._anchor：响应抽取 id、binance 请求体 clientOrderId、fomo 页面出站
subscribe 绑定）正向匹配；帧自带 id 先写入腿再自证关联不算 HK-L1a'。
响应锚只从 schema-known 字段结构化抽取
（binance data.orderId 与 data.clientOrderId **分键**、data.signature 等同节点 hash；
okx/gmgn 键白名单有界遍历；fomo responseObject.v2Swap.relaySwapId；padre 恒无锚）——
整-body 正则不写锚，与结构化抽取的分歧只作诊断 legacyRegexDiverged；入站 WS 帧
按锚身份路由（Store.route_ws_frame）到**所有开窗腿**中正向匹配的腿——不只平台
最新腿；dup 腿命中归到其 formal（已绑槽）腿，dup 腿的独立锚并入 formal 锚，
成功永不留在 dup 腿；无身份/零命中/冲突 → 不归属并计 diag（timeline.diag +
/health diag）。
身份冲突 = 否决条件——腿锚一经响应/subscribe
确证，冲突身份到达即降级 diagnostic（_anchorVeto + incomplete=anchor-conflict）：
路由候选排除、晋升/成功/receipt 轮询关闭、observed 不增加；
同单去重带 platform 维度（同 orderId 跨平台不合并）；请求无身份、身份由响应
揭示时 formal 资格归最早请求（成功起点按最早请求，已落地成功/receipt 证据随槽位
移交）；dup 腿收到冲突身份（或 formal 被否决）→ 同单证明被推翻，重开为独立
候选（计 diag.orderDupReopened——multi-order-suspect 如实可见，
orderReqDuplicate/unbound 计数同步改记）。
late veto 收口——否决前已落地的成功/receipt 保留在腿内部（证据不删），
timeline 序列化统一撤销合格指标：hkL1aMs/hkL3Ms → null、撤销值入
hkL1aMsVetoed/hkL3MsVetoed、腿带 anchorVeto:true（不以 bound/eligible 呈现）；
GMGN/OKX 响应与 WS 帧逐 entry 解析——同一 entry 的身份/状态/链才可组合（跨
entry 不拼接）；早到 WS 帧缓冲——先于 HTTP 响应到达的成功/身份帧保留单调源
时间/窗口/方向暂存，锚建立后按实时帧同一管线重审；关窗/重放/冲突不采纳。
Padre 同样要求独立锚：DONE 节点须带合法且一致的交易 hash/签名，并与请求/响应
锚正向匹配；时间新鲜性不能代替关联。无可用锚时缺失 HK 成功/hash 证据。
时间戳只在同一 DONE 节点子树内校验；缺源时间或陈旧帧一概不采纳。
同一 entry 纪律：成功/hash/id/时间必须落在同一
schema-known entry——padre 的 ws_ids 与 ws_success 共用同一 DONE 节点定位
（兄弟节点的 hash 不得拼接给 DONE 节点；无 DONE 节点的帧不提供任何 id/hash）；
binance 同一 content entry 的全部 schema hash 字段（orderTxId/txHash/signature/
txId）归一（0x 前缀大小写归一）不一致 → **整帧拒收**（ids/成功一概不认；与
scripts/lib/binance-ws.mjs anchorMatchVerdict/extractOrderHistoryHash 的同节点
归一 fail-closed 同律——python 侧重写，非 import）。

隐私/安全纪律（AGENTS 同律）：只记录元数据 + 尺寸 + 内容 hash；**绝不落盘**
cookie/authorization 头、签名 body、私钥材料。事件默认内存态（拉取后可清）。
"""

import base64
import hashlib
import http.client
import importlib.metadata
import json
import os
import platform
import re
import sys
import threading
import time
import urllib.request
import uuid
from collections import defaultdict
from collections import deque
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer

try:
    import msgpack  # mitmproxy 依赖自带
except Exception:  # pragma: no cover
    msgpack = None

try:
    from websockets.sync.client import (
        connect as ws_connect,  # pip: websockets（install.sh 装）
    )
except Exception:  # pragma: no cover
    ws_connect = None

# ── 链 RPC（HK 侧 receipt 轮询用；与 speedex configs/chains 对齐）──
CHAINS = {
    "bsc": {
        "family": "evm",
        "rpc": "https://bsc-dataseed1.binance.org",
        "chainIds": {56, "56", "0x38"},
    },
    "robinhood": {
        "family": "evm",
        "rpc": "https://rpc.mainnet.chain.robinhood.com",
        "chainIds": {4663, "4663", "0x1237"},
    },
    "solana": {
        "family": "solana",
        "rpc": "https://api.mainnet-beta.solana.com",
        "chainIds": set(),
    },
    "arc": {
        "family": "evm",
        "rpc": "https://rpc.mainnet.arc.io",
        "chainIds": {5042, "5042", "0x13b2"},
    },
}
CHAIN_ID_TO_NAME = {}
for _name, _c in CHAINS.items():
    for _cid in _c["chainIds"]:
        CHAIN_ID_TO_NAME[_cid] = _name
CHAIN_NAME_STR = {
    "arc": "arc",
    "bsc": "bsc",
    "solana": "solana",
    "sol": "solana",
    "robinhood": "robinhood",
    "rh": "robinhood",
    "bscchain": "bsc",
}

RECEIPT_POLL_INTERVAL_S = 0.8
RECEIPT_POLL_TIMEOUT_S = 90.0
MARK_TTL_S = 1800.0  # 窗口兜底寿命（EU 崩了没 close 时防环境流量误入 + 内存有界）

# build manifest（/health 透出；install.sh 锁 mitmproxy 版本；观测语义变化即升版本）。
# 当前协议要点：授权槽绑定（order 腿建腿绑 platform×chain×round 槽；无槽/链未知 →
# unbound 诊断不计 attempted）、padre sign 角色、订单身份去重（dup 归同槽）、
# pendingBind 待 receipt 实证链、observed ⊆ attempted、腿 round 恒 null（授权轮由
# EU 侧唯一关联回填 authorizedRound）+ observationIndex 到达序、schema-known 结构化
# 响应锚（整-body 正则仅诊断 legacyRegexDiverged）、WS 帧按锚身份路由（dup 归
# formal）、身份冲突否决（anchorVeto + timeline 序列化撤销合格指标）、GMGN/OKX 逐
# entry 解析、早到 WS 帧缓冲、SPEEDEX_HK_CTRL_PORT 覆盖监听端口、captureIdentity
# 当批采集/解密身份持久化（_collection_identity 与 /health 同源：allowHosts 实际
# 生效的宿主模式集——mitmproxy ctx.options 真实生效值优先、SPEEDEX_HK_ALLOW_HOSTS
# env unit 声明兜底、unavailable 不猜——+ allowHostsSource + ctrlPort；切换解密
# 模式后历史批不得拿当前配置解释）、Solana 链头 parent 连续性（slotSubscribe 父
# 槽位必须衔接已接受头、同高异父=分叉；不连续即清空重锚，与 EVM hash/parentHash
# 冲突清空同律；样本保留 parent 字段）。修订经过见 git 历史；行为由
# deploy/hk-proxy/test_speedex_hk_timing.py 与 tests/hk-timing*.test.mjs 钉住。
ADDON_VERSION = "2026.09.23-chain-arrivals-egress-warm-v10.1-pending-receipt-dedup"
# 实例身份——启动时间+pid+短随机；热重载后新旧模块实例 id 不同
INSTANCE_ID = f"{int(time.time())}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

# Store 有界 retention——每 run 完成后只保留最近 N 个 run 的 marks/legs/events；
# 已回收 run 的 timeline 返回明确 gone 标记；活动 poller 腿 pin 住不回收
RETENTION_RUNS = 50
GONE_IDS_MAX = 200

# 单窗采集预算——腿/事件超限截断并计 overflow（timeline 如实标注）。防预览
# 风暴（fomo 页面键入/报价刷新自发多次 POST）与异常流量打爆小内存 VM；超限即丢，
# 宁缺毋滥。
MAX_LEGS_PER_RUN = 200
MAX_EVENTS_PER_RUN = 2000

# 早到 WS 帧缓冲预算——成功/身份帧先于其 HTTP 响应到达时暂存
# （保留单调源时间/窗口/方向），响应建立锚后重审；超限丢弃计 wsBufferOverflow
MAX_WS_BUFFER_PER_RUN = 64

# 整-body 正则**只作诊断**（_legacy_regex_ids_diag → 腿 _anchorDiag.legacyRegexDiverged），
# 不是任何平台的锚来源——首个 `orderId|order_id|clientOrderId|id` 命中不分键名写 orderId、
# 数字 orderId（无引号）漏配、任意嵌套 `"id"` 冒充订单号，均会让 FINISHED 帧「冲突」而整帧
# 静默丢弃（hkL1aMs=None）。锚只从 schema-known 字段结构化抽取（_binance_resp_ids 等）。
_EVM_HASH_RE = re.compile(
    r'"(?:transactionHash|txHash|hash|tx_hash|txid|signature)"\s*:\s*"(0x[0-9a-fA-F]{64}|[1-9A-HJ-NP-Za-km-z]{80,90})"'
)
_ORDER_ID_RE = re.compile(
    r'"(?:orderId|order_id|clientOrderId|id)"\s*:\s*"([0-9A-Za-z\-]{6,64})"'
)

# 腿锚身份键（独立锚 + 路由身份）：成功/hash 采纳与 WS 帧→腿路由只看这三键
_IDENTITY_KEYS = ("orderId", "clientOrderId", "txHash")
_ID_VALUE_MAX = 64


# ── 广播请求体身份推导（不持久化签名正文，只在内存中算身份）────────────────
# keccak-256 纯 Python 实现（keccak-f[1600]）：EVM txHash = keccak256(rawSignedTx)。
# 注意 keccak padding 0x01 与 SHA3-256 的 0x06 不同——不能用 hashlib.sha3_256 替代。
# 正确性由公开向量与真实链上重构（BSC legacy / Arc EIP-1559 回重建 raw 比对）钉死，
# 见 test_speedex_hk_timing.py。
_MASK64 = (1 << 64) - 1
_KECCAK_RC = (
    0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
    0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
    0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
    0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
    0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
    0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
)
_KECCAK_ROT = (
     0,  1, 62, 28, 27,
    36, 44,  6, 55, 20,
     3, 10, 43, 25, 39,
    41, 45, 15, 21,  8,
    18,  2, 61, 56, 14,
)


def _rotl64(v, n):
    return v if n == 0 else (((v << n) | (v >> (64 - n))) & _MASK64)


def _keccak_f1600(s):
    for rc in _KECCAK_RC:
        c = [s[x] ^ s[x + 5] ^ s[x + 10] ^ s[x + 15] ^ s[x + 20] for x in range(5)]
        d = [c[(x + 4) % 5] ^ _rotl64(c[(x + 1) % 5], 1) for x in range(5)]
        for x in range(5):
            for y in range(5):
                s[x + 5 * y] ^= d[x]
        b = [0] * 25
        for x in range(5):
            for y in range(5):
                b[y + 5 * ((2 * x + 3 * y) % 5)] = _rotl64(s[x + 5 * y], _KECCAK_ROT[x + 5 * y])
        for x in range(5):
            for y in range(5):
                s[x + 5 * y] = b[x + 5 * y] ^ ((~b[(x + 1) % 5 + 5 * y] & _MASK64) & b[(x + 2) % 5 + 5 * y])
        s[0] ^= rc


def _keccak256(data):
    """bytes → 32B keccak-256 digest（rate 136B，pad10*1 域 0x01）。"""
    rate = 136
    s = [0] * 25
    padded = bytearray(data)
    padded.append(0x01)
    while len(padded) % rate != rate - 1:
        padded.append(0)
    padded.append(0x80)
    for off in range(0, len(padded), rate):
        blk = padded[off : off + rate]
        for i in range(rate // 8):
            s[i] ^= int.from_bytes(blk[8 * i : 8 * i + 8], "little")
        _keccak_f1600(s)
    return b"".join(x.to_bytes(8, "little") for x in s)[:32]


def _evm_raw_tx_hash(raw_hex):
    """eth_sendRawTransaction 的 0x-hex 已签名原文 → txHash（keccak256(rawBytes)）。"""
    if not isinstance(raw_hex, str) or not raw_hex.startswith("0x"):
        return None
    try:
        raw = bytes.fromhex(raw_hex[2:])
    except ValueError:
        return None
    if len(raw) < 16:
        return None
    return "0x" + _keccak256(raw).hex()


_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58_ALPHABET[r] + out
    pad = 0
    for byte in b:
        if byte == 0:
            pad += 1
        else:
            break
    return "1" * pad + out


def _sol_wire_first_sig(b64_body):
    """solana wire 交易（base64）→ 首个签名（base58）——wire 头 = compact-u16 签名数
    后紧跟 64B 签名表；与链上 getTransaction 回读签名逐字节一致（实证向量见测试）。"""
    try:
        raw = base64.b64decode(b64_body, validate=True)
    except Exception:
        return None
    if not raw:
        return None
    count = raw[0]
    off = 1
    if count & 0x80:  # compact-u16 双字节形
        if len(raw) < 2:
            return None
        count = (count & 0x7F) | (raw[1] << 7)
        off = 2
    if count < 1 or len(raw) < off + 64:
        return None
    return _b58encode(raw[off : off + 64])


# ── v10 链域通道（www.okx.com /fullnode|/nodeone WS）窄提取 ────────────────
# 只取 schema-known 字段的精确 tx 身份；与平台成功判定管线完全分离（这些帧是链上
# 通知，不是平台订单成功——永不进 success/ws_buffer/资格门）。帧体不持久化。
_OKX_CHAIN_WS_PATH = re.compile(r"^/(fullnode|nodeone)/([a-z0-9-]+)/", re.I)
# 2026-09-22：OKX 链域通道 host 扩展——robinhood 链通知实证迁至 www.okx.ac
# （/fullnode/robinhood/discover/ws；路径正则同形命中）。okx.ac 为 OKX 同运营域。
_OKX_CHAIN_WS_HOSTS = frozenset(("www.okx.com", "www.okx.ac"))
_OKX_EVM_TX_KEYS = ("txHash", "transactionHash", "hash")
_OKX_SUCCESS_STATUS = ("0x1", "0x01", "1", 1)


def _okx_chain_frame_identity(payload, channel, chain):
    """返回帧内精确 tx 身份（schema-known 窄提取，成功形态才认）：
    - solana fullnode：signatureNotification 的 params.result.value.signature 精确等值
      且 err === null；
    - EVM nodeone/fullnode：eth_subscription 的 params.result 内 txHash/transactionHash/
      hash 精确 hex64 + 同 result 的 status ∈ {0x1,0x01,1,1}（同子树成功律，与 EU
      classify-ws 同口径）。
    不满足 → None（不猜、不部分采纳）。
    """
    obj = _decode_ws(payload)
    if not isinstance(obj, dict):
        return None
    params = obj.get("params")
    result = params.get("result") if isinstance(params, dict) else None
    if channel == "fullnode" and chain == "solana":
        if not re.search(r"signatureNotification", str(obj.get("method") or "")):
            return None
        if not isinstance(result, dict):
            return None
        value = result.get("value")
        if not isinstance(value, dict) or value.get("err") is not None:
            return None
        sig = value.get("signature")
        return sig if isinstance(sig, str) and 64 <= len(sig) <= 96 else None
    # EVM：eth_subscription
    if not re.search(r"eth_subscription", str(obj.get("method") or "")):
        return None
    if not isinstance(result, dict):
        return None
    status = result.get("status")
    if status not in _OKX_SUCCESS_STATUS and str(status).lower() not in _OKX_SUCCESS_STATUS:
        return None
    for k in _OKX_EVM_TX_KEYS:
        v = result.get(k)
        if isinstance(v, str) and re.fullmatch(r"0x[0-9a-fA-F]{64}", v):
            return v
    return None


def _j(payload):
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = bytes(payload).decode("utf8", "replace")
        except Exception:
            return None
    try:
        return json.loads(payload)
    except Exception:
        return None


def _decode_ws(payload):
    """WS 帧归一 → python 对象或 None。JSON 先试；bytes 再试 msgpack；再试
    nbstream 式二进制信封（utf8 后取第一个平衡 JSON 对象）。"""
    if isinstance(payload, str):
        return _j(payload)
    if not isinstance(payload, (bytes, bytearray)):
        return None
    b = bytes(payload)
    try:
        return json.loads(b.decode("utf8"))
    except Exception:
        pass
    if msgpack is not None:
        try:
            return msgpack.unpackb(b, raw=False)
        except Exception:
            pass
    # nbstream 二进制信封：utf8 replace 后找第一个平衡 {...}
    txt = b.decode("utf8", "replace")
    start = txt.find("{")
    if start < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(txt)):
        ch = txt[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return _j(txt[start : i + 1])
    return None


def _walk(obj, pred):
    """深度遍历 JSON/msgpack 对象，pred(node) 为真即返回该节点。"""
    stack = [obj]
    seen = 0
    while stack and seen < 4000:
        seen += 1
        cur = stack.pop()
        if isinstance(cur, dict):
            if pred(cur):
                return cur
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return None


def _chain_from_text(body):
    """从请求 body 尽力取链：chainId/chainIndex 数字 → 映射；chain:"BSC" 等字符串。"""
    j = _j(body)
    if j:
        data = j.get("data") if isinstance(j.get("data"), dict) else j
        for key in ("chainId", "chainIndex", "chain_id"):
            v = (data or {}).get(key) if isinstance(data, dict) else None
            if v is None and isinstance(j, dict):
                v = j.get(key)
            if v is not None:
                name = CHAIN_ID_TO_NAME.get(v) or CHAIN_ID_TO_NAME.get(str(v))
                if not name:
                    try:
                        name = CHAIN_ID_TO_NAME.get(int(str(v), 0))
                    except Exception:
                        name = None
                if name:
                    return name
        for key in ("chain", "chainName"):
            v = (data or {}).get(key) if isinstance(data, dict) else j.get(key)
            if isinstance(v, str):
                name = CHAIN_NAME_STR.get(v.lower())
                if name:
                    return name
    m = re.search(r"[?&]chain=(\w+)", body or "")
    if m:
        return CHAIN_NAME_STR.get(m.group(1).lower()) or None
    return None


def _legacy_regex_ids_diag(text):
    """旧整-body 正则抽取——**仅诊断**（记 leg._anchorDiag.legacyRegexDiverged，
    对照结构化锚是否分歧），永不写锚。保留它是为了量化键序/数字 id/嵌套 id
    误锚在生产上出现的频率。"""
    if not text:
        return {}
    out = {}
    m = _EVM_HASH_RE.search(text)
    if m:
        out["txHash"] = m.group(1)
    m = _ORDER_ID_RE.search(text)
    if m:
        out["orderId"] = m.group(1)
    return out


def _id_value(v):
    """schema-known 字段的订单身份值归一：非空 str（≤64）或 int（非 bool——数字
    orderId 无引号也归一）→ str；其余类型（float/list/dict/None）不是身份。"""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        s = v.strip()
        return s if 0 < len(s) <= _ID_VALUE_MAX else None
    return None


def _ident_norm(k, v):
    """身份键比较归一：txHash 走交易 ID 形状归一（EVM 0x 小写；base58 保大小写；
    非法形状按原串比较，不猜）；orderId/clientOrderId 按 str 比较（数字/字符串同值）。"""
    if v is None:
        return None
    if k == "txHash":
        return _transaction_id_value(v) or str(v)
    return str(v)


def _anchor_verdict(anchor, ids):
    """帧 ids 与腿独立锚的正向匹配判定（与 scripts/lib/binance-ws.mjs anchorMatchVerdict
    同词汇）：共同身份键（orderId/clientOrderId/txHash）全部相等 → 'positive'；一部分相等
    一部分不等 → 'conflict'（同单身份自相矛盾，优先拒绝）；全部不等 → 'miss'（他单，
    不毒化）；无共同键 → 'none'。返回 (verdict, shared)。"""
    shared = [
        k
        for k in _IDENTITY_KEYS
        if anchor.get(k) is not None and ids.get(k) is not None
    ]
    if not shared:
        return "none", shared
    eq = [_ident_norm(k, anchor[k]) == _ident_norm(k, ids[k]) for k in shared]
    if all(eq):
        return "positive", shared
    if any(eq):
        return "conflict", shared
    return "miss", shared


def _collect_schema_values(obj, keys, max_depth=6):
    """有界遍历 JSON（深度 ≤max_depth、节点 ≤4000）：只收集 schema-known 键名下的
    标量值（str/int）。OKX/GMGN 响应嵌套层级文档未钉死（docs/okx.md 只列键名；
    scripts/lib/tx-hash.mjs 同样按键名有界遍历）——按键名而非路径抽取，但**键集合
    是白名单**（绝无 `id`/任意键）。返回 {key: [values…]}。"""
    found = {k: [] for k in keys}
    stack = [(obj, 0)]
    seen = 0
    while stack and seen < 4000:
        seen += 1
        cur, d = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in found and isinstance(v, (str, int)) and not isinstance(v, bool):
                    found[k].append(v)
                elif isinstance(v, (dict, list)) and d < max_depth:
                    stack.append((v, d + 1))
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)) and d < max_depth:
                    stack.append((v, d + 1))
    return found


def _single_value(values, norm):
    """多候选归一：恰一个不同值 → (value, False)；多个不同值 → (None, True)（歧义
    fail-closed，不取第一个）；无 → (None, False)。"""
    vals = {}
    for v in values:
        n = norm(v)
        if n is not None:
            vals.setdefault(n, v)
    if len(vals) == 1:
        return next(iter(vals)), False
    return None, len(vals) > 1


def _binance_resp_ids(text):
    """Binance place-order ack（docs/binance.md §API：`{code:"000000",success:true,
    data:{orderId, clientOrderId, message}}`，Sol 例外 data.signature）：schema-known
    路径 data.orderId 与 data.clientOrderId **分别**作为独立锚键（
    clientOrderId 值不得冒充 orderId）；hash 只取 data 同节点 orderTxId/txHash/signature/
    txId 归一一致的唯一值（>1 个不同值 = 帧自相矛盾 → 不取 hash，记 ambiguous）。
    失败 ack（code≠000000、无 data.orderId）不提供身份——请求体 clientOrderId 仍是锚。"""
    j = _j(text)
    data = (
        j.get("data")
        if isinstance(j, dict) and isinstance(j.get("data"), dict)
        else None
    )
    if not data:
        return {}
    out = {}
    oid = _id_value(data.get("orderId"))
    if oid:
        out["orderId"] = oid
    cid = _id_value(data.get("clientOrderId"))
    if cid:
        out["clientOrderId"] = cid
    hashes = _binance_entry_hashes(data)
    if len(hashes) == 1:
        h = _transaction_id_value(next(iter(hashes)))
        if h:
            out["txHash"] = h
    elif len(hashes) > 1:
        out["_ambiguous"] = ["txHash"]
    return out


def _resp_entries(obj, max_depth=6, max_entries=256):
    """响应 JSON 的 entry 列表——遍历中遇到的 list 的 dict 元素
    即 entry 边界（不下降进该 list）；无 list 时调用方回退整体单 entry。
    有界：节点 ≤4000、entry ≤max_entries。"""
    entries = []
    stack = [obj]
    seen = 0
    while stack and seen < 4000 and len(entries) < max_entries:
        seen += 1
        cur = stack.pop()
        if isinstance(cur, dict):
            for v in cur.values():
                if isinstance(v, list):
                    for e in v:
                        if isinstance(e, dict):
                            entries.append(e)
                            if len(entries) >= max_entries:
                                break
                elif isinstance(v, dict):
                    stack.append(v)
        elif isinstance(cur, list):  # 根列表：元素即 entry
            for e in cur:
                if isinstance(e, dict):
                    entries.append(e)
                    if len(entries) >= max_entries:
                        break
    return entries


def _collect_envelope_values(obj, keys, max_depth=6):
    """与 _collect_schema_values 同律但**不下降进 list**——envelope 层标量字段
    （列表之外）属于整个逻辑响应，可并入任一 entry；list 元素才是独立 entry。"""
    found = {k: [] for k in keys}
    stack = [(obj, 0)]
    seen = 0
    while stack and seen < 4000:
        seen += 1
        cur, d = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in found and isinstance(v, (str, int)) and not isinstance(v, bool):
                    found[k].append(v)
                elif isinstance(v, dict) and d < max_depth:
                    stack.append((v, d + 1))
    return found


def _walk_resp_ids(text, order_keys, hash_keys):
    """OKX/GMGN 共用：**逐 entry** 键名遍历（白名单键集）——
    identity/hash 必须来自同一 entry（data 列表元素），绝不跨 entry 拼接
    （整 body 归并会把 entry1 的 orderId 与 entry2 的 hash 拼成同一锚——
    真实订单的成功帧 hash 随即与锚「冲突」，制造跨 entry 假冲突/误否决）。
    envelope（所有 list 之外的标量）并入每个 entry；多个身份 entry 正向一致
    （共同键全等且至少一个共同键）才并集，否则 fail-closed 不取并记 _ambiguous。
    单 entry（含无 list 的整体回退）行为不变。"""
    j = _j(text)
    if not isinstance(j, (dict, list)):
        return {}
    keys = tuple(order_keys) + tuple(hash_keys)
    entries = _resp_entries(j) or [j]  # 无 list —— 整体单 entry
    envelope = _collect_envelope_values(j, keys)
    per = []
    amb = []
    for e in entries:
        found = _collect_schema_values(e, keys)
        oid, oid_amb = _single_value(
            [v for k in order_keys for v in envelope[k] + found[k]], _id_value
        )
        h, h_amb = _single_value(
            [v for k in hash_keys for v in envelope[k] + found[k]],
            _transaction_id_value,
        )
        entry = {}
        if oid:
            entry["orderId"] = oid
        if h:
            entry["txHash"] = h
        if oid_amb:
            amb.append("orderId")
        if h_amb:
            amb.append("txHash")
        per.append(entry)
    ident = [
        p for p in per if p.get("orderId") is not None or p.get("txHash") is not None
    ]
    if not ident:
        return {"_ambiguous": sorted(set(amb))} if amb else {}
    # 跨 entry 合并：只并**正向一致**的 entry（≥1 共同身份键且共同键全等）；
    # 无共同键无法证明同单、共同键不等 = 多订单疑似——都不拼接
    groups = []
    conflict = False
    for p in ident:
        placed = False
        for g in groups:
            shared = [
                k
                for k in ("orderId", "txHash")
                if g.get(k) is not None and p.get(k) is not None
            ]
            if not shared:
                continue
            if all(_ident_norm(k, g[k]) == _ident_norm(k, p[k]) for k in shared):
                for k in ("orderId", "txHash"):
                    if g.get(k) is None:
                        g[k] = p.get(k)
                placed = True
                break
            conflict = True
        if not placed:
            groups.append(
                {k: p.get(k) for k in ("orderId", "txHash") if p.get(k) is not None}
            )
    if len(groups) == 1 and not conflict:
        out = dict(groups[0])
        if amb:
            out["_ambiguous"] = sorted(set(amb))
        return out
    for g in groups:
        amb.extend(k for k in ("orderId", "txHash") if g.get(k) is not None)
    return {"_ambiguous": sorted(set(amb))}


def _okx_resp_ids(text):
    """OKX /broadcast 响应（docs/okx.md §2.1：body 含 orderId、transactionHash、
    isSuccess；嵌套未钉死）：键白名单 orderId / transactionHash|txHash。
    逐 entry 解析（_walk_resp_ids——batchBroadcast 的 data 列表逐元素独立，
    不跨 entry 拼接身份）。透传平台，HK 下 hook 不触发——规则保留。"""
    return _walk_resp_ids(text, ("orderId",), ("transactionHash", "txHash"))


# Observed product fanout destinations. No arbitrary host or substring matching.
OKX_RPC_HOSTS = {
    "okx.bsc-rpc.com": "bsc", "okx.rpc.48.club": "bsc",
    "okx.builder.48.club": "bsc", "okx.bsc.blockrazor.xyz": "bsc",
    "okx.bscnetwork.blockrazor.io": "bsc",
    "rpc.mainnet.arc.io": "arc", "rpc.mainnet.chain.robinhood.com": "robinhood",
}
OKX_SOL_SEND_HOSTS = {
    f"{region}{suffix}.solana.blockrazor.io"
    for region in ("frankfurt", "hongkong", "newyork", "tokyo") for suffix in ("", "2")
}


def _okx_broadcast_request_ids(body):
    """Only the observed single-transaction signedInfoList envelope, never signatures."""
    if len(body) > 262144:
        return {}
    obj = _j(body)
    if not isinstance(obj, dict):
        return {}
    rows = obj.get("signedInfoList")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        return {}
    tx = _transaction_id_value(rows[0].get("txHash"))
    if not tx:
        return {}
    out = {"txHash": tx}
    oid = _id_value(obj.get("orderId"))
    if oid:
        out["orderId"] = oid
    return out


def _okx_rpc_request(host, path, method, headers, body):
    """Request classification only. Response on this exact flow supplies tx identity.
    Origin is a product-path filter, never an identity/security credential. Body/query
    and headers are neither retained nor sent anywhere by this observer.
    reqTxHash：请求体数学推导身份（keccak256(raw) / wire 首签名）——响应侧 hash 仍是
    交叉验证，冲突走锚否决律；签名原文不持久化（只有推导 hash 入锚）。
    """
    if method != "POST" or len(body) > 262144 or headers.get("origin") != "https://web3.okx.com":
        return None
    if host in OKX_SOL_SEND_HOSTS and path == "/v2/sendTransaction":
        # Observed BlockRazor endpoint accepts a bare base64 transaction.
        if 80 <= len(body) <= 16400 and re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", body):
            out = {"chain": "solana", "source": "okx-solana-send", "id": None}
            sig = _sol_wire_first_sig(body)
            if sig:
                out["reqTxHash"] = sig
            return out
        return None
    if host not in OKX_RPC_HOSTS or path != "/":
        return None
    obj = _j(body)
    if not isinstance(obj, dict) or obj.get("jsonrpc") != "2.0" or obj.get("method") != "eth_sendRawTransaction":
        return None
    rid, params = obj.get("id"), obj.get("params")
    if isinstance(rid, bool) or not isinstance(rid, (str, int)) or len(str(rid)) > 128:
        return None
    if not isinstance(params, list) or len(params) != 1 or not isinstance(params[0], str):
        return None
    if not re.fullmatch(r"0x(?:[0-9a-fA-F]{2}){16,131000}", params[0]):
        return None
    out = {"chain": OKX_RPC_HOSTS[host], "source": "okx-evm-send", "id": rid}
    txh = _evm_raw_tx_hash(params[0])
    if txh:
        out["reqTxHash"] = txh
    return out


def _okx_rpc_response(meta, response):
    """响应解析。返回 dict 可带 _reject 标记（调用方据此拒绝广播身份晋升）：
    非 200/超尺寸/非 JSON-RPC 信封/响应 id 与请求不配 = 传输或流完整性失败，
    该响应不是本平台受理证据；id 匹配的 JSON-RPC error（already known /
    nonce too low 等）= 端点已处理本请求，tx 身份请求侧数学推导已在锚，不标 _reject。
    """
    if response.status_code != 200:
        return {"_reject": f"http-{response.status_code}"}
    body = response.get_text() or ""
    if len(body) > 4096:
        return {"_reject": "oversize"}
    if meta["source"] == "okx-solana-send":
        tx = _transaction_id_value(body.strip())
        return {"txHash": tx} if tx and not tx.startswith("0x") else {}
    obj = _j(body)
    if not isinstance(obj, dict) or obj.get("jsonrpc") != "2.0":
        return {"_reject": "not-jsonrpc"}
    if obj.get("error") is not None:
        return {}
    if type(obj.get("id")) is not type(meta["id"]) or obj.get("id") != meta["id"]:
        return {"_reject": "id-mismatch"}
    tx = _transaction_id_value(obj.get("result"))
    return {"txHash": tx} if tx and tx.startswith("0x") else {}


_GMGN_ORDER_KEYS = ("orderId", "order_id", "oi")
_GMGN_HASH_KEYS = (
    "hash",
    "tx_hash",
    "txHash",
    "txhash",
    "transactionHash",
    "transaction_hash",
    "txid",
    "tx_id",
    "txId",
    "signature",
)


def _gmgn_resp_ids(text):
    """GMGN swap_batch_order / txproxy send_transaction 响应：键白名单与 WS 条目键
    `oi`/`h` 及 scripts/lib/tx-hash.mjs HASH_KEYS 对齐（hash/tx_hash/txid/signature…）。
    逐 entry 解析（_walk_resp_ids——同一 entry 的身份/hash 才可组合，
    跨 entry 不拼接）。透传平台，HK 下 hook 不触发——规则保留。"""
    return _walk_resp_ids(text, _GMGN_ORDER_KEYS, _GMGN_HASH_KEYS)


def _padre_resp_ids(text):
    """Padre 出站锚是 Turnkey `sign_raw_payload`——真实响应是 ActivityResponse
    `{activity:{id,organizationId,status,type,intent:{signRawPayloadIntentV2:{signWith,
    payload,…}},result:{signRawPayloadResult:{r,s,v}},…}}`：**没有任何订单/交易身份字段**
    （activity.id 是签名活动 UUID，不是订单号；intent.payload 是签名摘要，不是 tx hash；
    r/s/v 是签名分量）。正则猜测会把 activity.id 误写成 orderId，让 DONE 帧「锚冲突」
    或永无正向匹配——sign 腿结构上不可晋升。本函数恒不提供锚：Padre HK-L1a′
    暂不可观测（无独立锚），腿如实 no-execution-proof；未来只能由 Padre 自有下单通道
    （client→_multiplex msgpack 请求帧）经真实脱敏帧验证后提供锚，不在此猜。"""
    return {}


def _fomo_resp_ids(text):
    """FOMO /swaps/v2 响应：schema-known 路径 responseObject.v2Swap.relaySwapId
    （docs/fomo.md §：`v2Swap.relaySwapId = Relay requestId`，EVM 0x+64hex 形状）
    作 orderId 锚；destinationChainId → chain。同链（DFlow）预览响应无 v2Swap → 无锚。"""
    j = _j(text)
    if not isinstance(j, dict):
        return {}
    out = {}
    ro = j.get("responseObject") if isinstance(j.get("responseObject"), dict) else {}
    v2 = ro.get("v2Swap") if isinstance(ro.get("v2Swap"), dict) else {}
    rid = (
        v2.get("relaySwapId")
        if v2.get("relaySwapId") is not None
        else ro.get("relaySwapId")
    )
    if isinstance(rid, str) and re.fullmatch(r"0[xX][0-9a-fA-F]{64}", rid):
        out["orderId"] = rid
    dcid = v2.get("destinationChainId")
    if dcid is not None:
        c = CHAIN_ID_TO_NAME.get(str(dcid)) or CHAIN_ID_TO_NAME.get(dcid)
        if c:
            out["chain"] = c
    return out


# ── 平台规则 ──


def _okx_ws_entries(payload):
    """dex-across/dex-swap-order-info 帧 → **逐 entry** 处理单元
    [{ids, success}…]——data 可为 dict（dexData）或 list（逐元素 dexData/元素本体），
    每个 entry 的身份（orderId/transactionHash|txHash|hash）、状态（status）、链
    （chainId）只取自**同一 entry**；目标在第二 entry 也照常关联（不首-entry-only）。
    success = 同 entry status==="1"（"0"/abnormalStatus "-1" = 创建帧，不算）。
    锚正向匹配由路由层（route_ws_frame）保证——帧自带 id 不得自证关联。
    非订单频道/非 dict 帧 = []（行情噪声静默跳过——**不**计 wsFrameRejected，
    那是「同 entry 自相矛盾整帧拒收」的词汇）；无 entry 身份键的单元同样不路由。"""
    obj = _decode_ws(payload)
    if not isinstance(obj, dict):
        return []
    arg = obj.get("arg") or {}
    if not isinstance(arg, dict) or arg.get("channel") not in ("dex-across-order-info", "dex-swap-order-info"):
        return []
    data = obj.get("data")
    if isinstance(data, dict):
        nodes = [data]
    elif isinstance(data, list):
        nodes = [d for d in data if isinstance(d, dict)]
    else:
        return []
    out = []
    for node in nodes:
        dd = node.get("dexData") if "dexData" in node else node
        if not isinstance(dd, dict):
            continue
        ids = {}
        h = dd.get("transactionHash") or dd.get("txHash") or dd.get("hash")
        if h:
            ids["txHash"] = h
        if dd.get("orderId"):
            ids["orderId"] = dd["orderId"]
        if str(dd.get("chainId") or "") == "501":
            ids["chain"] = "solana"
        elif dd.get("chainId") is not None:
            c = CHAIN_ID_TO_NAME.get(str(dd["chainId"])) or CHAIN_ID_TO_NAME.get(
                dd["chainId"]
            )
            if c:
                ids["chain"] = c
        out.append({"ids": ids, "success": type(dd.get("status")) is str and dd.get("status") == "1"})
    return out


def _gmgn_ws_entries(payload):
    """tg_order_info / tg_processed_order_info 帧 → **逐 entry**
    处理单元——data 列表每个元素独立：身份（oi→orderId / h→txHash）、状态（st）、
    方向（si）、链（ch）只取自同一 entry（docs/gmgn.md：「同 entry h/oi 命中本单
    且 st=successful」同律）；目标在第二 entry 也照常关联（不 data[0]-only）。
    success = 同 entry st==="successful" 且 si==="buy"（卖单不采纳，classify-ws
    同律）。锚正向匹配由路由层保证。非订单频道/无 data 列表 = []
    （行情噪声静默跳过——不计 wsFrameRejected）。"""
    obj = _decode_ws(payload)
    if not isinstance(obj, dict):
        return []
    if str(obj.get("channel")) not in ("tg_order_info", "tg_processed_order_info"):
        return []
    data = obj.get("data")
    if not isinstance(data, list):
        return []
    out = []
    for d0 in data:
        if not isinstance(d0, dict):
            continue
        ids = {}
        if d0.get("h"):
            ids["txHash"] = d0["h"]
        if d0.get("oi"):
            ids["orderId"] = d0["oi"]
        if d0.get("ch"):
            c = CHAIN_NAME_STR.get(str(d0["ch"]).lower())
            if c:
                ids["chain"] = c
        out.append(
            {
                "ids": ids,
                "success": str(d0.get("st")) == "successful"
                and str(d0.get("si")) == "buy",
            }
        )
    return out


_PADRE_HASH_KEYS = ("txnHash", "txHash", "transactionHash", "tx", "signature")


def _padre_done_node(obj):
    """找含 txnStatus=='DONE' 的节点（严格等于）。宽松终态词表
    （filled/completed/success/…）不得使用——非 DONE 帧冒充成功信号；
    非 DONE 帧不提供成功/hash/id。"""

    def is_done(n):
        st = n.get("txnStatus") or n.get("txStatus") or n.get("orderStatus")
        return isinstance(st, str) and st == "DONE"

    return _walk(obj, is_done)


def _padre_frame_fresh(payload, leg, ids):
    """独立锚和源时间分别过门；新鲜 DONE 不能自证属于本次 Turnkey 请求。
    正向 id 匹配也不绕过陈旧门，WS 学到的腿字段不成为独立锚。"""
    if not _padre_ids_correlate(ids, _AnchorView(leg)):
        return False
    obj = _decode_ws(payload)
    if obj is None:
        return False
    node = _padre_done_node(obj)
    if node is None:
        return False
    found = {}

    def grab(n):
        for key in (
            "creationTime",
            "firstUserClickMs",
            "createdAt",
            "createTime",
            "timestamp",
        ):
            v = n.get(key)
            if isinstance(v, (int, float)) and v > 10**9:
                found["ts"] = v
                return True
        return False

    _walk(node, grab)
    ts = found.get("ts")
    if ts is None:
        return False
    ts_ms = ts * 1000 if ts < 10**12 else ts
    leg_wall = leg.get("tOrderOutWallMs")
    if not leg_wall:
        return False
    return ts_ms >= leg_wall - 5000


def _padre_ws(payload, leg):
    # 与 hash 采纳共享同节点解析和独立锚判定；不能出现 ids 拒绝畸形 hash 而
    # 成功谓词仅凭其非空仍写 L1a'。调用方另经 _padre_frame_fresh 校验源时间。
    return _padre_ids_correlate(_padre_ws_ids(payload), leg)


def _transaction_id_value(value):
    """只验证 schema 字段的交易 ID 形状；形状不是交易归属/链上成功证明。"""
    if not isinstance(value, str):
        return None
    if re.fullmatch(r"0[xX][0-9a-fA-F]{64}", value):
        return value.lower()
    if re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{80,90}", value):
        return value
    return None


def _padre_ids_correlate(ids, anchor):
    """同节点合法 hash + 至少一个独立锚正向匹配；任何已知锚冲突均拒绝。"""
    if not ids or not _transaction_id_value(ids.get("txHash")):
        return False
    positive = False
    for key in ("orderId", "txHash"):
        expected, observed = anchor.get(key), ids.get(key)
        if expected is None or observed is None:
            continue
        if key == "txHash":
            expected, observed = (
                _transaction_id_value(expected),
                _transaction_id_value(observed),
            )
            if expected is None or observed is None:
                return False
        if str(expected) != str(observed):
            return False
        positive = True
    return positive


def _padre_ws_ids(payload):
    """与 _padre_ws 共用同一 DONE 节点定位——hash/orderId 只从该节点取；
    无 DONE 节点的帧不提供任何 id/hash（兄弟节点的 hash 不得拼接给 DONE 节点，
    成功节点 hash=B 而腿挂 sibling 的 A = 跨交易拼接）。"""
    obj = _decode_ws(payload)
    if obj is None:
        return {}
    node = _padre_done_node(obj)
    if node is None:
        return {}
    out = {}
    hashes = set()
    for key in _PADRE_HASH_KEYS:
        value = node.get(key)
        if value is None or value == "":
            continue
        value = _transaction_id_value(value)
        if value is None:
            return None  # 畸形/互相矛盾的同 entry 字段 → 整帧拒收
        hashes.add(value)
    if len(hashes) != 1:
        return None
    out["txHash"] = next(iter(hashes))
    for k in ("orderId", "order_id", "oid"):
        if node.get(k):
            out["orderId"] = node[k]
            break
    return out


_BINANCE_BIZ_RE = re.compile(
    r"^(WEB3_DEX_.*_ORDER_CHANGE|DEX_ALL_ORDER|marketOrderInfo)$"
)

# 同一 content entry 的 schema hash 字段集合（与 binance-ws.mjs 同四键）
_BINANCE_HASH_KEYS = ("orderTxId", "txHash", "signature", "txId")


def _binance_entry_hashes(content):
    """同一 content entry 的全部 schema hash 字段归一（0x 前缀大小写归一；base58
    签名保大小写）。与 scripts/lib/binance-ws.mjs anchorMatchVerdict /
    extractOrderHistoryHash 同律（python 侧重写，非 import）——同节点多 hash 字段
    必须归一一致；>1 个不同值 = 帧自相矛盾，fail-closed 不猜。"""
    vals = set()
    for k in _BINANCE_HASH_KEYS:
        v = content.get(k)
        if isinstance(v, str) and v:
            vals.add(v.lower() if v[:2].lower() == "0x" else v)
    return vals


def _binance_frames(payload):
    """WS 帧归一合同：**只取第一个平衡 JSON 对象**（_decode_ws：JSON → msgpack →
    nbstream 式二进制信封 utf8 后首个平衡 {...}；信封内其余 JSON 段不解析不产出）。"""
    obj = _decode_ws(payload)
    return [obj] if isinstance(obj, dict) else []


def _binance_ws(payload, leg):
    for obj in _binance_frames(payload):
        data = obj.get("data") if isinstance(obj.get("data"), dict) else None
        content = data.get("content") if isinstance(data.get("content"), dict) else None
        if not data or not content:
            continue
        if not _BINANCE_BIZ_RE.match(str(data.get("bizKey") or "")):
            continue
        if str(content.get("status")) != "FINISHED":
            continue
        if len(_binance_entry_hashes(content)) > 1:
            continue  # 同 entry hash 归一不一致——帧自相矛盾，不得凭锚匹配充成功
        oid = content.get("orderId")
        cid = content.get("clientOrderId")
        # FINISHED 帧必须正向等于请求/响应锚的
        # orderId/clientOrderId——腿锚由 _AnchorView 供给，帧自带 id 先写入腿再
        # 自证关联不算数；缺 id 或无锚的 FINISHED 不得凭时间窗充数（陈旧重放假成功）。
        if leg.get("orderId") and oid and str(oid) == str(leg.get("orderId")):
            return True
        if (
            leg.get("clientOrderId")
            and cid
            and str(cid) == str(leg.get("clientOrderId"))
        ):
            return True
    return False


def _binance_ws_ids(payload):
    """返回 ids dict；**None = 整帧拒收**（任一 content entry 的 schema hash
    字段归一不一致——帧自相矛盾，调用方整帧弃用，成功判定同弃）。"""
    out = {}
    for obj in _binance_frames(payload):
        data = obj.get("data") if isinstance(obj.get("data"), dict) else None
        content = data.get("content") if isinstance(data.get("content"), dict) else None
        if not data or not content:
            continue
        if len(_binance_entry_hashes(content)) > 1:
            return None  # 归一不一致 → 整帧拒收
        # 归一已校验一致——以下首见即代表全部同值字段
        # PENDING 帧的 orderTxId = hash 首见（PENDING≠成功，只取 hash）
        for hk in _BINANCE_HASH_KEYS:
            if content.get(hk) and not out.get("txHash"):
                out["txHash"] = content[hk]
        if content.get("orderId") and not out.get("orderId"):
            out["orderId"] = content["orderId"]
        if content.get("clientOrderId") and not out.get("clientOrderId"):
            out["clientOrderId"] = content["clientOrderId"]
        bcid = content.get("binanceChainId")
        if bcid is not None and not out.get("chain"):
            c = CHAIN_ID_TO_NAME.get(str(bcid)) or CHAIN_ID_TO_NAME.get(bcid)
            if c:
                out["chain"] = c
    return out


def _fomo_ws(payload, leg):
    obj = _decode_ws(payload)
    if not isinstance(obj, dict):
        return False
    if str(obj.get("event")) != "request.status.updated":
        return False
    d = obj.get("data") if isinstance(obj.get("data"), dict) else obj
    if str(d.get("status") or "").lower() != "success":
        return False
    rid = d.get("requestId") or ((d.get("filters") or {}).get("id")) or d.get("id")
    norm = lambda v: str(v).lower().removeprefix("0x")
    # 必须有腿锚（响应 relaySwapId 或页面出站 subscribe 绑定）且帧
    # requestId 正向相等——腿无锚或帧无 requestId 都不得出 L1a'（缺锚 →
    # incomplete，F-SOL 同链腿无 Relay 帧的 known limitation 同此出口）。
    return bool(leg.get("orderId") and rid and norm(rid) == norm(leg.get("orderId")))


def _fomo_ws_ids(payload):
    obj = _decode_ws(payload)
    if not isinstance(obj, dict):
        return {}
    d = obj.get("data") if isinstance(obj.get("data"), dict) else {}
    out = {}
    if d.get("requestId"):
        out["orderId"] = d["requestId"]
    txh = d.get("txHashes")
    if isinstance(txh, list) and txh:
        out["txHash"] = txh[0]
    dcid = d.get("destinationChainId")
    if dcid is not None:
        c = CHAIN_ID_TO_NAME.get(str(dcid)) or CHAIN_ID_TO_NAME.get(dcid)
        if c:
            out["chain"] = c
    return out


RULES = {
    "okx": {
        "match_order": lambda h, p, m: (
            h == "web3.okx.com"
            and m == "POST"
            and re.search(r"/priapi/v6/dx/trade/multi/(batch)?[Bb]roadcast", p)
            is not None
        ),
        "extract_ids": _okx_resp_ids,
        "ws_hosts": ("wsdexpri.okx.com",),
        # 逐 entry 单元解析取代首-entry-only 的 ws_success/ws_ids
        "ws_entries": _okx_ws_entries,
        "extract_chain": _chain_from_text,
    },
    "gmgn": {
        "match_order": lambda h, p, m: (
            h == "gmgn.ai"
            and m == "POST"
            and ("swap_batch_order" in p or "/txproxy/v2/send_transaction" in p)
        ),
        "extract_ids": _gmgn_resp_ids,
        "ws_hosts": ("ws.gmgn.ai",),
        "ws_entries": _gmgn_ws_entries,
        "extract_chain": _chain_from_text,
    },
    "padre": {
        # 下单走 WS msgpack（方法串未钉死）——出站时刻用次优 HTTP：Turnkey 签名调用。
        # Turnkey ActivityResponse 不含订单/交易身份 → 无独立锚 → sign 腿
        # 结构上不可晋升（HK-L1a′ 暂不可观测）；DONE 门/新鲜度门保留给未来锚源。
        "match_order": lambda h, p, m: (
            h == "api.turnkey.com"
            and m == "POST"
            and "/public/v1/submit/sign_raw_payload" in p
        ),
        "extract_ids": _padre_resp_ids,
        "ws_hosts": (
            "backend3.padre.gg",
            "backend.padre.gg",
            "backend2.padre.gg",
            "txn-service.padre.gg",
        ),
        "ws_success": _padre_ws,
        "ws_ids": _padre_ws_ids,
        "extract_chain": _chain_from_text,
        # 无可用的请求/响应交易身份时保留缺失，不以源时间新鲜自证关联。
        # ws_frame_fresh 在 ids 写入腿之前校验独立锚与源时间。
        "ws_frame_fresh": _padre_frame_fresh,
    },
    "binance": {
        "match_order": lambda h, p, m: (
            m == "POST"
            and "/bapi/defi/v2/private/wallet-direct/web-dex/place-order" in p
        ),
        "extract_ids": _binance_resp_ids,
        "ws_hosts": (
            "nbstream.binance.com",
            "web3-stream.binance.com",
            "data-stream.binance.vision",
            "stream.binance.com",
        ),
        "ws_success": _binance_ws,
        "ws_ids": _binance_ws_ids,
        "extract_chain": _chain_from_text,
    },
    "fomo": {
        "match_order": lambda h, p, m: (
            h == "prod-api.fomo.family" and p.startswith("/swaps/v2") and m == "POST"
        ),
        "extract_ids": _fomo_resp_ids,
        "ws_hosts": ("ws.relay.link",),
        "ws_success": _fomo_ws,
        "ws_ids": _fomo_ws_ids,
        "extract_chain": _chain_from_text,
    },
}


def _jsonrpc(url, method, params, timeout=10):
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode()
    # 默认 Python-urllib UA 被 robinhood RPC 403（HK 出证实测）；换显式 UA 即通
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "content-type": "application/json",
            "User-Agent": "speedex-hk-timing/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode()).get("result")


class KeepAliveJsonRpc:
    """Keep-alive JSON-RPC client for the head poller.

    单次 urlopen 每轮新建 TCP+TLS（HK→RPC 实测 250–1000ms/轮），批量采集窗口内
    轮询线程被事件流量挤压时无法维持 ~1s 节拍（2026-09-12 iyqzt RH 链头样本在
    锚点 5–23s 陈旧、超 maxAgeMs=2000 → 整腿 round-reference-unverified）。
    keep-alive 复用连接把单轮压到 ~30–100ms：轮询循环在事件流量缝隙的 CPU 片内
    即可完成。连接级错误 → 下次调用重建；UA 与 _jsonrpc 一致（robinhood 403 规避）。
    """

    def __init__(self, url, timeout=1.5, user_agent="speedex-hk-timing/1.0"):
        from urllib.parse import urlsplit

        self._parts = urlsplit(url)
        self.timeout = timeout
        self.user_agent = user_agent
        self._conn = None

    def _connect(self):
        if self._parts.scheme != "https":
            raise ValueError("keep-alive rpc only supports https")
        self._conn = http.client.HTTPSConnection(
            self._parts.netloc, timeout=self.timeout
        )

    def __call__(self, method, params, timeout=None):
        body = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        )
        headers = {"content-type": "application/json", "User-Agent": self.user_agent}
        path = self._parts.path or "/"
        for attempt in range(2):
            try:
                if self._conn is None:
                    self._connect()
                self._conn.timeout = timeout if timeout is not None else self.timeout
                self._conn.request("POST", path, body=body, headers=headers)
                r = self._conn.getresponse()
                data = r.read()
                if r.status != 200:
                    raise RuntimeError(f"rpc http {r.status}")
                return json.loads(data.decode()).get("result")
            except Exception:
                try:
                    if self._conn is not None:
                        self._conn.close()
                except Exception:
                    pass
                self._conn = None
                if attempt == 1:
                    raise
        return None


class ChainHeadWindow:
    """Read-only, bounded head observations owned by one active capture window.

    No network is started by Store/tests. Control /mark explicitly starts this
    collector. Hook snapshots only consult samples received before hook entry.
    """

    def __init__(self, rpc_call=_jsonrpc, connect=ws_connect, clock=None):
        self.rpc_call = rpc_call
        self.connect = connect
        self.clock = clock or (lambda: _mono_ms())
        self.lock = threading.Lock()
        self.run_id = None
        self.samples = {}
        self.stop = threading.Event()
        self.sockets = []
        self.threads = []
        self.expiry = None
        # keep-alive 轮询客户端（per-chain 懒建；注入 rpc_call 时尊重注入，不建）
        self._ka = {}
        # 轮询可观测：成功/失败/末次错误（/health.diag.headPoll 透出，2026-09-12 head-v4）
        self.diag = {
            "pollOk": defaultdict(int),
            "pollFail": defaultdict(int),
            "lastErr": {},
        }

    def diag_snapshot(self):
        with self.lock:
            return {
                "pollOk": dict(self.diag["pollOk"]),
                "pollFail": dict(self.diag["pollFail"]),
                "lastErr": dict(self.diag["lastErr"]),
            }

    def _poll_call(self, chain, method, params):
        """单次轮询调用：默认走 keep-alive 客户端；测试注入 rpc_call 时走注入。"""
        if self.rpc_call is not _jsonrpc:
            return self.rpc_call(CHAINS[chain]["rpc"], method, params, timeout=1.5)
        client = self._ka.get(chain)
        if client is None:
            client = KeepAliveJsonRpc(CHAINS[chain]["rpc"], timeout=1.5)
            self._ka[chain] = client
        return client(method, params)

    def close(self):
        self.stop.set()
        if self.expiry is not None:
            self.expiry.cancel()
        with self.lock:
            sockets = list(self.sockets)
        for ws in sockets:
            try:
                ws.close()
            except Exception:
                pass
        for thread in self.threads:
            thread.join(timeout=2)

    def start(self, run_id, chains):
        self.run_id = run_id
        self.expiry = threading.Timer(MARK_TTL_S, self.close)
        self.expiry.daemon = True
        self.expiry.start()
        for chain in chains:
            if chain not in CHAINS:
                continue
            self.samples[chain] = deque(maxlen=512)
            for worker in (self._ws, self._poll):
                thread = threading.Thread(target=worker, args=(chain,), daemon=True)
                self.threads.append(thread)
                thread.start()

    def add(self, chain, result, source, sent=None):
        sol = chain == "solana"
        try:
            height = (
                result.get("slot")
                if sol and isinstance(result, dict)
                else result
                if sol
                else int(result["number"], 16)
            )
            block_hash = None if sol else result["hash"]
            if (
                type(height) is not int
                or height < 0
                or (not sol and not re.fullmatch(r"0x[0-9a-fA-F]{64}", block_hash))
            ):
                return
            # Solana 在 processed 承诺下无区块 hash——slotSubscribe 的 parent 槽位是
            # 唯一的分叉连续性证据；getSlot 轮询是裸整数（无 parent，同今的降级回退：
            # 无连续性证据即不判冲突）。
            parent = None
            if sol and isinstance(result, dict):
                p = result.get("parent")
                if type(p) is int and p >= 0:
                    parent = p
        except (TypeError, ValueError, KeyError):
            return
        at = self.clock()
        with self.lock:
            if self.stop.is_set() or chain not in self.samples:
                return
            buf = self.samples[chain]
            previous = buf[-1] if buf else None
            # A regression/conflicting head breaks continuity; never select a
            # pre-fork buffered head for a later hook. Solana heads carry no hash —
            # the parent slot is the continuity link: a new head must extend the
            # accepted head (parent == previous slot); same height with a different
            # proven parent is a fork. Discontinuity clears and re-anchors, mirroring
            # the EVM hash/parentHash conflict rule.
            if previous and (
                height < previous["height"]
                or (height == previous["height"] and block_hash != previous["hash"])
                or (
                    not sol
                    and height == previous["height"] + 1
                    and result.get("parentHash", "").lower() != previous["hash"].lower()
                )
                or (
                    sol
                    and parent is not None
                    and (
                        (height > previous["height"] and parent != previous["height"])
                        or (
                            height == previous["height"]
                            and previous.get("parent") is not None
                            and previous["parent"] != parent
                        )
                    )
                )
            ):
                buf.clear()
            buf.append(
                {
                    "height": height,
                    "hash": block_hash,
                    "parent": parent,
                    "observedTs": at,
                    "source": source,
                    "roundtripMs": None if sent is None else at - sent,
                }
            )

    def snapshot(self, run_id, at):
        with self.lock:
            if self.run_id != run_id or self.stop.is_set():
                return {}
            out = {}
            for chain, buf in self.samples.items():
                sample = next((x for x in reversed(buf) if x["observedTs"] <= at), None)
                base = {
                    "version": 1,
                    "chain": chain,
                    "epoch": run_id,
                    "anchor": "proxy-order-request",
                    "vantage": "hk",
                    "commitment": "processed" if chain == "solana" else "latest",
                    "anchorTs": at,
                    "clockErrorMs": 0,
                    "intervalMs": 500,
                    "maxAgeMs": 2000,
                }
                if sample:
                    age = at - sample["observedTs"]
                    out[chain] = {
                        **base,
                        **sample,
                        "sampleAgeMs": age,
                        "status": "ok" if age <= 2000 else "stale-head",
                    }
                else:
                    out[chain] = {**base, "status": "no-pre-anchor-head"}
            return out

    def _poll(self, chain):
        cfg = CHAINS[chain]
        while not self.stop.is_set():
            with self.lock:
                buf = self.samples[chain]
                fresh = bool(
                    buf
                    and buf[-1]["source"] == "ws-head"
                    and self.clock() - buf[-1]["observedTs"] < 1000
                )
            if not fresh:
                sent = self.clock()
                try:
                    sol = chain == "solana"
                    result = self._poll_call(
                        chain,
                        "getSlot" if sol else "eth_getBlockByNumber",
                        [{"commitment": "processed"}] if sol else ["latest", False],
                    )
                    self.add(chain, result, "rpc-poll", sent)
                    with self.lock:
                        self.diag["pollOk"][chain] += 1
                except Exception as e:
                    with self.lock:
                        self.diag["pollFail"][chain] += 1
                        self.diag["lastErr"][chain] = (
                            f"{type(e).__name__}: {str(e)[:80]}"
                        )
            self.stop.wait(0.5)

    def _ws(self, chain):
        if self.connect is None:
            return
        url = (
            CHAINS[chain]["rpc"]
            .replace("https://", "wss://")
            .replace("http://", "ws://")
        )
        sol = chain == "solana"
        while not self.stop.is_set():
            try:
                with self.connect(url, open_timeout=2, close_timeout=1) as ws:
                    with self.lock:
                        self.sockets.append(ws)
                    try:
                        ws.send(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "id": 1,
                                    "method": "slotSubscribe"
                                    if sol
                                    else "eth_subscribe",
                                    "params": [] if sol else ["newHeads"],
                                }
                            )
                        )
                        subscription = None
                        while not self.stop.is_set():
                            try:
                                msg = json.loads(ws.recv(timeout=1))
                            except TimeoutError:
                                continue
                            if msg.get("id") == 1:
                                if msg.get("error"):
                                    break
                                subscription = msg.get("result")
                            elif (
                                subscription is not None
                                and msg.get("params", {}).get("subscription")
                                == subscription
                                and msg.get("method")
                                == ("slotNotification" if sol else "eth_subscription")
                            ):
                                self.add(chain, msg["params"]["result"], "ws-head")
                    finally:
                        with self.lock:
                            self.sockets.remove(ws)
                            if (
                                self.samples[chain]
                                and self.samples[chain][-1]["source"] == "ws-head"
                            ):
                                self.samples[chain].clear()
            except Exception:
                pass
            self.stop.wait(2)


HEADS = ChainHeadWindow()
HEAD_CONTROL_LOCK = threading.Lock()


def _start_heads(run_id, manifest):
    global HEADS
    HEADS.close()
    HEADS = ChainHeadWindow()
    legs = (manifest or {}).get("legs") or []
    chains = sorted({x.get("chain") for x in legs if x.get("chain") in CHAINS})
    HEADS.start(run_id, chains or list(CHAINS))


# ── HK 原生探测（/egress + /latency——EU 拉取，不含 EU↔HK 段）──────────────
# 口径与 speedex scripts/lib/platform-ws-latency.mjs 对齐（method 名一致）；
# 失败一律 None——绝不发明数字。


def _probe_http_ping(host, path, timeout=6, conn_cls=None):
    """binance 口径：同一 keep-alive 连接上预热 1 次（丢弃）+ 3 热样本中位数；
    ms=round(rtt/2)，与 EU httpPingRtt 同口径。http.client 显式复用连接
    （body 读空才可复用；urllib/标准库默认每样本新连接，「热」名不副实）。
    失败一律 None。conn_cls 测试注入。"""
    cls = conn_cls or http.client.HTTPSConnection
    try:
        conn = cls(host, timeout=timeout)
        try:

            def once():
                t0 = time.monotonic()
                conn.request(
                    "GET",
                    path,
                    headers={"accept": "*/*", "user-agent": "speedex-hk-probe"},
                )
                r = conn.getresponse()
                r.read()  # 读空 body——连接才可复用（keep-alive）
                return round((time.monotonic() - t0) * 1000)

            once()  # 预热：TCP+TLS 握手吸收在第一次
            samples = sorted(once() for _ in range(3))
            rtt = samples[1]
            return {
                "ms": max(1, round(rtt / 2)),
                "rttMs": rtt,
                "method": "http-ping",
                "at": int(time.time() * 1000),
            }
        finally:
            conn.close()
    except Exception:
        return None


def _probe_https_warm(host, path, timeout=6, conn_cls=None):
    """gmgn 口径（与 EU platform-ws-latency httpsWarmRtt 同律）：同一 keep-alive
    连接预热 1 次（TCP+TLS 握手吸收在第一次）+ 第 2 次热请求整 RTT——ms=rttMs
    **不减半**，method https-warm 如实区分。http.client 显式复用连接（body 读空
    才可复用；标准库 urllib 单次冷连接不算 https-warm）。失败一律 None。
    conn_cls 测试注入。"""
    cls = conn_cls or http.client.HTTPSConnection
    try:
        conn = cls(host, timeout=timeout)
        try:

            def once():
                t0 = time.monotonic()
                conn.request(
                    "GET",
                    path,
                    headers={"accept": "*/*", "user-agent": "speedex-hk-probe"},
                )
                r = conn.getresponse()
                r.read()  # 读空 body——连接才可复用（keep-alive）
                return round((time.monotonic() - t0) * 1000)

            once()  # 预热：握手吸收在第一次
            rtt = once()  # 热连接第二次 ≈ 1×RTT + 服务器处理（EU httpsWarmRtt 同口径）
            return {
                "ms": rtt,
                "rttMs": rtt,
                "method": "https-warm",
                "at": int(time.time() * 1000),
            }
        finally:
            conn.close()
    except Exception:
        return None


def _probe_okx_ws(timeout=6):
    """OKX wsdexpri：text ping|<hex32>|<ts> → pong|<hex>（严格配对）。
    先完成连接握手再单计 ping RTT（与 EU wsPingOnce 同口径）——
    握手不得计入 ping 再除二（会系统性偏大）。"""
    if ws_connect is None:
        return None
    try:
        nonce = hashlib.md5(str(time.monotonic_ns()).encode()).hexdigest()
        with ws_connect(
            "wss://wsdexpri.okx.com/ws/v5/ipublic",
            open_timeout=timeout,
            close_timeout=2,
        ) as ws:
            t0 = time.monotonic()  # 握手完成后才起表
            ws.send(f"ping|{nonce}|{int(time.time() * 1000)}")
            while time.monotonic() - t0 < timeout:
                msg = ws.recv(timeout=max(0.1, timeout - (time.monotonic() - t0)))
                m = re.match(r"^pong\|([0-9a-f]{32})$", str(msg), re.I)
                if m and m.group(1).lower() == nonce:
                    rtt = round((time.monotonic() - t0) * 1000)
                    return {
                        "ms": max(1, round(rtt / 2)),
                        "rttMs": rtt,
                        "method": "ws-ping",
                        "at": int(time.time() * 1000),
                    }
        return None
    except Exception:
        return None


def _probe_padre_ws(timeout=6):
    """Padre _multiplex msgpack [8,id,'/ping/ping',uuid] → [9,id,…]。
    同 OKX：握手不计入 ping。"""
    if ws_connect is None or msgpack is None:
        return None
    try:
        mid = int(time.time() * 1000) % 0x7FFFFFFF
        frame = msgpack.packb([8, mid, "/ping/ping", f"hk-{mid:x}"])
        with ws_connect(
            "wss://backend3.padre.gg/_multiplex", open_timeout=timeout, close_timeout=2
        ) as ws:
            t0 = time.monotonic()  # 握手完成后才起表
            ws.send(frame)
            while time.monotonic() - t0 < timeout:
                msg = ws.recv(timeout=max(0.1, timeout - (time.monotonic() - t0)))
                try:
                    v = msgpack.unpackb(
                        msg if isinstance(msg, bytes) else bytes(msg), raw=False
                    )
                except Exception:
                    continue
                if isinstance(v, list) and len(v) >= 2 and v[0] == 9 and v[1] == mid:
                    rtt = round((time.monotonic() - t0) * 1000)
                    return {
                        "ms": max(1, round(rtt / 2)),
                        "rttMs": rtt,
                        "method": "ws-ping",
                        "at": int(time.time() * 1000),
                    }
        return None
    except Exception:
        return None


def _probe_fomo_ws(timeout=6):
    """FOMO：连接即推 challenge——冷连接全链路（含握手），不减半。"""
    if ws_connect is None:
        return None
    try:
        t0 = time.monotonic()
        with ws_connect(
            "wss://prod-api.fomo.family/ws", open_timeout=timeout, close_timeout=2
        ) as ws:
            while time.monotonic() - t0 < timeout:
                msg = ws.recv(timeout=max(0.1, timeout - (time.monotonic() - t0)))
                if "challenge" in str(msg):
                    rtt = round((time.monotonic() - t0) * 1000)
                    return {
                        "ms": rtt,
                        "rttMs": rtt,
                        "method": "ws-challenge",
                        "at": int(time.time() * 1000),
                    }
        return None
    except Exception:
        return None


_LATENCY_CACHE = {"at": 0, "data": None, "inflight": False}
_LATENCY_TTL_S = 60


def _collect_latency():
    out = {
        "gmgn": _probe_https_warm("gmgn.ai", "/defi/quotation/v1/chains"),
        "okx": _probe_okx_ws(),
        "padre": _probe_padre_ws(),
        "binance": _probe_http_ping("api.binance.com", "/api/v3/ping"),
        "fomo": _probe_fomo_ws(),
        "at": int(time.time() * 1000),
        # vantage 由 /egress 实测 countryCode 推导（hk-proxy/sg-proxy…；
        # 未知 → unknown-proxy），不硬编码 hk-local
        "vantage": _vantage(fetch=True),
    }
    return out


def get_latency():
    now = time.monotonic()
    if _LATENCY_CACHE["data"] and now - _LATENCY_CACHE["at"] < _LATENCY_TTL_S:
        return _LATENCY_CACHE["data"]
    if _LATENCY_CACHE["inflight"]:
        return _LATENCY_CACHE["data"]  # SWR：旧值先回
    _LATENCY_CACHE["inflight"] = True
    try:
        _LATENCY_CACHE["data"] = _collect_latency()
        _LATENCY_CACHE["at"] = time.monotonic()
    finally:
        _LATENCY_CACHE["inflight"] = False
    return _LATENCY_CACHE["data"]


_EGRESS_CACHE = {"at": 0, "data": None}
_EGRESS_TTL_S = 600


def get_egress():
    now = time.monotonic()
    if _EGRESS_CACHE["data"] and now - _EGRESS_CACHE["at"] < _EGRESS_TTL_S:
        return _EGRESS_CACHE["data"]
    try:
        req = urllib.request.Request(
            "http://ip-api.com/json/?fields=status,country,countryCode,regionName,city,isp,query",
            headers={"user-agent": "speedex-hk-probe"},
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            j = json.loads(r.read().decode())
        if j.get("status") == "success":
            _EGRESS_CACHE["data"] = {
                "ip": j.get("query"),
                "label": " / ".join(
                    x
                    for x in (j.get("country"), j.get("regionName"), j.get("city"))
                    if x
                ),
                "isp": j.get("isp"),
                "countryCode": j.get("countryCode"),  # vantage 推导源
            }
            _EGRESS_CACHE["at"] = now
    except Exception:
        pass
    return _EGRESS_CACHE["data"]


def _vantage(fetch=False):
    """采集 vantage 由 /egress 实测 countryCode 推导（'hk-proxy'/'sg-proxy'…），
    未实测/未知 → 'unknown-proxy'——不硬编码 'hk-local'。timeline 拉取路径
    fetch=False 只读缓存（不得为打标签发网）；/latency 路径 fetch=True。"""
    eg = None
    if fetch:
        eg = get_egress()
    elif (
        _EGRESS_CACHE["data"] and time.monotonic() - _EGRESS_CACHE["at"] < _EGRESS_TTL_S
    ):
        eg = _EGRESS_CACHE["data"]
    cc = (eg or {}).get("countryCode")
    return (
        f"{cc.strip().lower()}-proxy"
        if isinstance(cc, str) and cc.strip()
        else "unknown-proxy"
    )


def _collection_identity():
    """timeline 自带方法版本与采集环境身份——sidecar 自证采集口径
    （vantage 实测推导不硬编码；method 静态标注 hook 观测点语义，不宣称物理离站）。
    五件身份 vantage/instanceId/addonVersion/mitmproxyVersion/pythonVersion 与
    /health build manifest 同源（client hkTimingEnvironment 取 timeline 优先）。
    另带 captureIdentity——本实例实际生效的采集/解密身份
    （allow_hosts 宿主模式集 + 来源 + 控制面端口），每批持久化，不按当前矩阵反填。"""
    return {
        "method": "hook-observed@v2",
        "vantage": _vantage(),
        "instanceId": INSTANCE_ID,
        "addonVersion": ADDON_VERSION,
        "mitmproxyVersion": _mitmproxy_version(),
        "pythonVersion": platform.python_version(),
        "captureIdentity": _capture_identity(),
    }


def _ctrl_port():
    """控制面监听端口：SPEEDEX_HK_CTRL_PORT 可覆盖（隔离实验 unit 与生产同机并存
    ——speedex-mitm-exp.service 设 18072）；缺省/非法（非有限整数、越界）一律
    回退 8072——绝不因坏 env 崩溃。只读该具名变量，不 dump environ。"""
    raw = os.environ.get("SPEEDEX_HK_CTRL_PORT")
    if raw is None:
        return 8072
    try:
        p = int(str(raw).strip(), 10)
    except Exception:
        return 8072
    return p if 1 <= p <= 65535 else 8072


def _effective_allow_hosts():
    """addon 实际生效的 allow_hosts 宿主模式集（mitmdump `--set allow_hosts=…`）。
    优先 mitmproxy `ctx.options.allow_hosts`（真实生效值——含空列表 = 未设白名单
    即全量拦截，如实报告）；addon 无法自观测（非 mitmproxy 环境/选项不可读，
    如离线单测）→ 回退 SPEEDEX_HK_ALLOW_HOSTS env（systemd unit 声明值，来源
    如实标注 'env'）；两者都无 → (None, 'unavailable')——未知不猜。
    返回 (patterns|None, source)。只含 host 正则配置事实：有界（≤16 条、每条
    ≤512 字符），不落凭据、不 dump 其他 env。"""
    try:
        from mitmproxy import ctx  # mitmproxy 运行期注入；离线环境 ImportError

        v = ctx.options.allow_hosts  # 可读即权威（真实生效值优先于 env 声明）
    except Exception:
        v = None
    if v is not None:
        try:
            return [str(p)[:512] for p in list(v)[:16]], "mitmproxy-options"
        except Exception:
            pass
    raw = os.environ.get("SPEEDEX_HK_ALLOW_HOSTS")
    if isinstance(raw, str) and raw.strip():
        return [raw.strip()[:512]], "env"
    return None, "unavailable"


def _capture_identity():
    """采集/解密身份——每批持久化该实例实际生效的 allow_hosts
    宿主模式集（决定哪些平台可被 HK 观测——切换解密模式后历史批不得拿当前配置
    解释）+ 控制面端口（运行事实：实验实例 18072 与生产 8072 可区分）。
    全部配置事实；allowHostsSource 标明证据等级（mitmproxy-options 真实生效值 >
    env unit 声明 > unavailable 未知）。"""
    hosts, source = _effective_allow_hosts()
    return {"allowHosts": hosts, "allowHostsSource": source, "ctrlPort": _ctrl_port()}


class Store:
    """线程安全状态：mark 窗口 + 每 runId 的 legs 关联态。

    窗口纪律：拒绝多窗并存——mark_open 显式关闭其余开窗；
    关窗（close/TTL/被新窗顶掉）即冻结该 run 全部腿（windowClosed）并撤销
    open_legs 映射：后续 WS 帧不更新、receipt 线程回写丢弃，保留其既有
    incomplete/成功状态不动。建腿原子化：order_in_open_window 把
    rid 选取+开窗复验+事件/腿写入放进同一 lock 临界区——关窗后到达的下单请求
    一律拒绝（diag['orderReqNoWindow'] 计数）。内存有界：完成的 run 超
    RETENTION_RUNS 即回收（timeline 标 gone），活动 poller 腿 pin 住不回收。
    单 run 腿/事件预算（MAX_LEGS_PER_RUN/MAX_EVENTS_PER_RUN），超限截断
    并计 overflow。明细 manifest 的授权槽绑定（_bind_slot_locked）——
    order 腿建腿绑槽、preview/sign 腿晋升时补绑；无槽可绑标 unbound
    （diag['orderReqUnbound'] 计数），不冒充槽位；关窗逐腿补缺失状态。
    绑定前按订单身份去重（_dedup_order_identity_locked——
    orderId/clientOrderId 正向匹配已绑槽腿即归入同一授权槽，标 dup 诊断；
    身份缺失/冲突不猜）；晋升时链未实证标 pendingBind，receipt 实证链回写时
    回填 leg.chain 并补绑（_receipt_backfill_rebind_locked），关窗 freeze 时
    仍未实证的 pendingBind 腿转 unbound 落定；observed 只计绑槽 order 腿。
    身份冲突 = 否决条件（_veto_leg_locked——锚自相矛盾的腿
    降级 diagnostic：路由/晋升/成功/receipt 轮询关闭，incomplete=anchor-conflict，
    observed 不增加）；同单去重带 platform 维度（同 orderId 跨平台不合并）；
    响应揭示身份且存在更早同身份 order 腿时 formal 资格归最早请求
    （_transfer_formal_locked，响应/receipt 补链路径）；dup 腿身份与 formal 冲突（或 formal
    被否决）→ 重开为独立候选（_reopen_dup_locked，计 diag.orderDupReopened）。
    否决腿的合格指标在 timeline 序列化层统一撤销
    （hkL1aMs/hkL3Ms → null，撤销值入 *Vetoed 诊断键 + anchorVeto:true——内部原始
    事件时间保留）；入站 WS 帧逐单元处理（okx/gmgn 一帧多 entry 逐 entry 独立
    路由/判定，_ws_units/_adopt_ws_unit）；早到 WS 帧缓冲（ws_buffer——零命中
    且该平台还有无锚腿时保留源时间/窗口/方向暂存，锚建立后同一管线重审；freeze
    即弃并按零命中计数）。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.window_epoch = 0
        self.marks = {}
        self.legs = defaultdict(list)
        self.open_legs = {}  # platform -> 等待成功信号的最近腿（关窗即撤销）
        self.events = defaultdict(list)
        self.gone = deque(maxlen=GONE_IDS_MAX)  # 已回收 runId 墓碑
        self.diag = defaultdict(
            int
        )  # 诊断计数（如 orderReqNoWindow——关窗后拒绝的下单请求）
        # per-run 诊断计数（timeline.diag 透出——WS 帧带身份却无腿可归属 /
        # 锚冲突拒收 / 锚身份冲突 / 歧义等「静默丢失」全部可见）
        self.run_diag = defaultdict(lambda: defaultdict(int))
        self.overflow = defaultdict(lambda: {"legs": 0, "events": 0})  # 预算截断计数
        # 早到 WS 帧缓冲——rid → [单元]（源时间/窗口 epoch/方向保留）；
        # 关窗 freeze 即弃并按零命中计数，窗口身份不符不得采纳
        self.ws_buffer = defaultdict(list)

    def _freeze_run_locked(self, run_id):
        """关窗即冻结——腿标 windowClosed（后续帧不更新、receipt 回写丢弃），
        撤销活动腿映射。incomplete/成功状态保留不动（缺失状态由 timeline 输出层
        对冻结腿补写，见 timeline()——不改内部态，活动 poller pin 语义不受影响）。
        freeze 时仍未实证链的 pendingBind 腿
        转 unbound 落定（计 diag.orderReqUnbound——执行观察始终绑不上授权槽，
        不猜链）。关窗即弃本 run 的早到 WS 缓冲——缓冲单元始终
        未获正向匹配的按零命中终态计 wsFrameNoLegMatch（与实时帧同词汇，不静默）。"""
        for ent in self.ws_buffer.pop(run_id, []):
            self.diag["wsFrameNoLegMatch"] += 1
            self.run_diag[run_id]["wsFrameNoLegMatch"] += 1
        for leg in self.legs.get(run_id, []):
            leg["windowClosed"] = True
            if leg.get("pendingBind"):
                leg["pendingBind"] = False
                leg["unbound"] = True
                self.diag["orderReqUnbound"] += 1
        for p, leg in list(self.open_legs.items()):
            if leg.get("runId") == run_id:
                del self.open_legs[p]

    def _run_pinned_locked(self, run_id):
        """pin：开窗中的 run、或有活动 poller（未 settle 未 incomplete）的腿。"""
        m = self.marks.get(run_id)
        if m and m["closed"] is None:
            return True
        return any(
            leg.get("receiptPolling")
            and leg.get("tReceiptMs") is None
            and not leg.get("incomplete")
            for leg in self.legs.get(run_id, [])
        )

    def _prune_locked(self):
        """有界 retention——完成的 run 只留最近 RETENTION_RUNS 个，其余回收
        （墓碑进 gone，timeline 明确标 gone 而非空完整 timeline）。"""
        kept = 0
        for rid in sorted(
            self.marks, key=lambda r: self.marks[r]["opened"], reverse=True
        ):
            if self._run_pinned_locked(rid):
                continue
            if kept < RETENTION_RUNS:
                kept += 1
                continue
            self.marks.pop(rid, None)
            self.legs.pop(rid, None)
            self.events.pop(rid, None)
            self.overflow.pop(rid, None)
            self.run_diag.pop(rid, None)
            self.ws_buffer.pop(rid, None)  # 回收前窗口已 freeze（缓冲已弃），防御性清理
            for p, leg in list(self.open_legs.items()):
                if leg.get("runId") == rid:
                    del self.open_legs[p]
            if rid not in self.gone:
                self.gone.append(rid)

    def mark_open(self, run_id, note="", manifest=None):
        """开窗。manifest（可选）：{"rounds": n} 或 {"version",…,"legs":
        [{platform, chain, rounds}…]}——授权轮次登记，timeline 输出
        requested/attempted/observed/unbound + 缺失腿占位；无 manifest 行为同今。
        legs 明细带 per-leg rounds（缺失/非法按 1 计）时 requested=Σ
        legs[i].rounds（明细优先于顶层 rounds——顶层 rounds 是每腿轮次而非总数）。
        明细在时 order 腿建腿即绑定授权槽（只绑槽位容量，
        不写 round——授权轮号不由到达序推导，见 _new_leg_locked）。
        新窗显式关闭其余开窗（旧腿冻结）。"""
        requested = None
        manifest_legs = None
        if isinstance(manifest, dict):
            ml = manifest.get("legs")
            if isinstance(ml, list):
                # 内存有界：明细 ≤256 条、per-leg rounds ∈[1,64]、总轮次 ≤4096
                manifest_legs = []
                total = 0
                for x in ml:
                    if not isinstance(x, dict):
                        continue
                    r = x.get("rounds")
                    r = (
                        r
                        if isinstance(r, int)
                        and not isinstance(r, bool)
                        and 1 <= r <= 64
                        else 1
                    )
                    if len(manifest_legs) >= 256 or total + r > 4096:
                        break
                    manifest_legs.append(
                        {
                            "platform": (
                                str(x.get("platform"))[:32]
                                if x.get("platform")
                                else None
                            ),
                            "chain": (
                                str(x.get("chain"))[:32] if x.get("chain") else None
                            ),
                            "rounds": r,
                        }
                    )
                    total += r
                requested = total
            else:
                r = manifest.get("rounds")
                if isinstance(r, int) and not isinstance(r, bool) and r >= 0:
                    requested = r
        with self.lock:
            now = time.monotonic()
            for rid, m in self.marks.items():
                if m["closed"] is None:
                    m["closed"] = now  # 拒绝多窗并存
                    self._freeze_run_locked(rid)
            self.window_epoch += 1
            self.marks[run_id] = {
                "epoch": self.window_epoch,
                "opened": now,
                "opened_wall": _wall_iso(),
                "closed": None,
                "note": note,
                "requested": requested,
                "manifestLegs": manifest_legs,
            }
            self._prune_locked()

    def mark_close(self, run_id):
        with self.lock:
            m = self.marks.get(run_id)
            if m and m["closed"] is None:
                m["closed"] = time.monotonic()
                self._freeze_run_locked(run_id)

    def any_open(self):
        with self.lock:
            now = time.monotonic()
            return any(
                m["closed"] is None and now - m["opened"] < MARK_TTL_S
                for m in self.marks.values()
            )

    def _open_run_ids_locked(self, now):
        """（须持锁）开窗 run 列表，最新在前；TTL 到期的窗口顺手关闭+冻结。"""
        out = []
        # 最新在前（插入序取最旧是错的）；TTL 到期自动关窗+冻结
        for rid, m in sorted(
            self.marks.items(), key=lambda kv: kv[1]["opened"], reverse=True
        ):
            if m["closed"] is None and now - m["opened"] < MARK_TTL_S:
                out.append(rid)
            elif m["closed"] is None:
                m["closed"] = now
                self._freeze_run_locked(rid)
        return out

    def open_run_ids(self):
        with self.lock:
            return self._open_run_ids_locked(time.monotonic())

    def _add_event_locked(self, run_id, ev):
        """单 run 事件预算——超限丢弃并计 overflow（内存有界）。"""
        if len(self.events[run_id]) >= MAX_EVENTS_PER_RUN:
            self.overflow[run_id]["events"] += 1
            return False
        self.events[run_id].append(ev)
        return True

    def add_event(self, run_id, ev):
        with self.lock:
            self._add_event_locked(run_id, ev)

    def _bind_slot_locked(self, run_id, platform, chain):
        """把一次下单观察绑定到 manifest 授权槽（platform×chain 槽位容量）。
        返回槽位下标；无明细 manifest / 腿 chain 未知 / 无匹配或已满槽 → None
        （调用方按 unbound 或 pendingBind 诊断处理，绝不冒充授权槽位；
        同单重复请求先经 _dedup_order_identity_locked 归并，不走容量判定）。
        槽位只证明容量占用——授权轮号不从「已绑数量+1」推导
        （首轮未观测时到达序整体前移）；腿 round 恒 null=未知，
        到达序见 observationIndex，授权轮由 EU 侧唯一关联回填 authorizedRound。"""
        mark = self.marks.get(run_id) or {}
        detail = mark.get("manifestLegs")
        if not detail or not platform or not chain:
            return None
        for i, spec in enumerate(detail):
            if spec.get("platform") and spec["platform"] != platform:
                continue
            # 腿 chain 必须正向等于 spec chain——chain 未知的请求不得吞掉某链的槽位
            if spec.get("chain") and str(spec["chain"]) != str(chain):
                continue
            bound = sum(1 for x in self.legs[run_id] if x.get("slot") == i)
            if bound < (spec.get("rounds") or 1):
                return i
        return None

    def _same_order_bound_leg_locked(
        self, run_id, ident, except_leg=None, platform=None, chain=None
    ):
        """（须持锁）同 run 内与 ident 同订单的已绑槽腿。正向匹配——
        至少一个共同身份键（orderId/clientOrderId）且全部共同键相等；共同键冲突或
        无共同键 = 无法证明同单，不匹配（不猜）。dup/未绑槽的腿不作归并目标。
        带 platform 维度——同 orderId 跨平台不合并；被否决
        （_anchorVeto）的腿锚已自相矛盾，同样不作归并目标。"""
        for other in self.legs.get(run_id, []):
            if (
                other is except_leg
                or other.get("dup")
                or other.get("slot") is None
                or other.get("_anchorVeto")
            ):
                continue
            if platform is not None and other.get("platform") != platform:
                continue
            if platform == "okx" and chain and other.get("chain") and other["chain"] != chain:
                continue
            keys = _IDENTITY_KEYS if platform == "okx" else ("orderId", "clientOrderId")
            other_ids = (other.get("_anchor") or {}) if platform == "okx" else other
            shared = [k for k in keys if ident.get(k) is not None and other_ids.get(k) is not None]
            if shared and all(_ident_norm(k, other_ids[k]) == _ident_norm(k, ident[k]) for k in shared):
                return other
        return None

    def _dedup_order_identity_locked(self, leg, allow_transfer=False):
        """（须持锁）本腿与同 run 已绑槽腿正向同单 → 归入同一授权槽——
        释放占用槽、round 置 null、标 dup 诊断（计 diag.orderReqDuplicate，不新增
        attempted、不占新槽、不计 unbound）。身份缺失/冲突不猜（返回 False，调用方
        走容量/unbound 律）。建腿（请求侧身份）/响应（响应抽取身份）/晋升（执行
        证据落地）三处调用。
        已判 dup 的腿收到冲突身份（与 formal 共同身份键两侧都有值
        且不等）= 同单证明被推翻 → 重开为独立候选（_reopen_dup_locked），不永远
        扣在 dup 上吞掉后续帧；allow_transfer（响应/receipt 补链路径）且本腿是
        更早的 order 腿时 formal 资格归最早请求（_transfer_formal_locked）——后请求
        先响应不独占 formal（成功起点按最早请求）。"""
        if leg.get("_anchorVeto"):
            return True  # 锚自相矛盾的腿保持 diagnostic——不改判、不消费新槽
        if leg.get("dup"):
            formal0 = leg.get("_dupOf")
            if (
                isinstance(formal0, dict)
                and not formal0.get("windowClosed")
                and self._dup_identity_conflict_locked(leg, formal0)
            ):
                self._reopen_dup_locked(leg)
                return False
            # A pending request can already be an alias when its own receipt
            # finally proves the chain. Reconsider ownership only with that
            # independent chain proof; never borrow the formal leg's chain.
            if not allow_transfer or not leg.get("chain"):
                return True
        source = (leg.get("_anchor") or {}) if leg.get("platform") == "okx" else leg
        keys = _IDENTITY_KEYS if leg.get("platform") == "okx" else ("orderId", "clientOrderId")
        ident = {k: source.get(k) for k in keys if source.get(k) is not None}
        if not ident:
            return bool(leg.get("dup"))  # 身份缺失——不猜、不重开既有 alias
        formal = self._same_order_bound_leg_locked(
            leg.get("runId"), ident, leg, platform=leg.get("platform"), chain=leg.get("chain")
        )
        if formal is None:
            return bool(leg.get("dup"))
        if (
            allow_transfer
            and leg.get("anchorRole") == "order"
            and not leg.get("pendingBind")
            and formal.get("anchorRole") == "order"
            and isinstance(leg.get("tOrderOutMs"), (int, float))
            and isinstance(formal.get("tOrderOutMs"), (int, float))
            and leg["tOrderOutMs"] < formal["tOrderOutMs"]
        ):
            # 请求无身份、身份由响应揭示时，后请求可能先响应先成 formal；
            # 最早请求的身份揭示后 formal 资格归最早请求（成功时刻按最早请求）
            self._transfer_formal_locked(leg, formal)
            return True
        if leg.get("dup"):
            return True
        if leg.get("unbound"):
            # 建腿时容量耗尽/链未知可能已先标 unbound——正向同单证据到达后改判 dup，
            # unbound 诊断计数同步改记（不留假 unbound）
            leg["unbound"] = False
            self.diag["orderReqUnbound"] = max(0, self.diag["orderReqUnbound"] - 1)
        leg["slot"] = None
        leg["round"] = None
        leg["pendingBind"] = False
        leg["dup"] = True
        self.diag["orderReqDuplicate"] += 1
        self._mark_dup_of_locked(leg, formal)
        return True

    def _dup_identity_conflict_locked(self, leg, formal):
        """（须持锁）dup 腿与其 formal 的同单证明是否被后续身份证据
        推翻——共同身份键（orderId/clientOrderId/txHash）两侧都有值且不等 = 冲突
        （一侧缺失不算——缺失不是反证）；OKX 已知链冲突同样拒绝归并。"""
        if (
            leg.get("platform") == "okx"
            and leg.get("chain")
            and formal.get("chain")
            and leg["chain"] != formal["chain"]
        ):
            return True
        la, fa = leg.get("_anchor") or {}, formal.get("_anchor") or {}
        for k in _IDENTITY_KEYS:
            if (
                la.get(k) is not None
                and fa.get(k) is not None
                and _ident_norm(k, la[k]) != _ident_norm(k, fa[k])
            ):
                return True
        return False

    def _reopen_dup_locked(self, leg):
        """（须持锁）dup 判定被推翻（与 formal 身份冲突 / formal 被否决）
        → 重开为独立候选：撤销 dup 归并、计数改记（orderReqDuplicate−1、
        orderDupReopened+1——multi-order-suspect 如实可见，docs/binance.md：一轮 >1
        distinct orderId）、按自身身份重新绑槽（无槽/链未知 → unbound 律，计数如实）。
        已并入 formal 的等值锚不回收（同值无害）；冲突值从未覆盖 formal 锚（首次写入
        冻结）。被否决的腿自身不重开（保持 diagnostic）。"""
        if not leg.get("dup") or leg.get("_anchorVeto"):
            return
        leg["dup"] = False
        leg["_dupOf"] = None
        leg["_multiOrderSuspect"] = True  # 内部审计标记（不出 timeline，DTO 未登记）
        self.diag["orderReqDuplicate"] = max(0, self.diag["orderReqDuplicate"] - 1)
        self.diag["orderDupReopened"] += 1
        if leg.get("runId"):
            self.run_diag[leg["runId"]]["orderDupReopened"] += 1
        mark = self.marks.get(leg.get("runId")) or {}
        if (
            mark.get("manifestLegs")
            and leg.get("anchorRole") == "order"
            and leg.get("slot") is None
            and not leg.get("unbound")
        ):
            chain = leg.get("chain")
            b = (
                self._bind_slot_locked(leg["runId"], leg["platform"], chain)
                if chain
                else None
            )
            if b is not None:
                leg["slot"] = b
            else:
                leg["unbound"] = True
                self.diag["orderReqUnbound"] += 1

    def _transfer_formal_locked(self, leg, formal):
        """（须持锁）formal 资格归最早请求——本腿（更早的 order 腿）接过
        授权槽与已落地的成功/receipt 证据，后请求腿改判 dup 归并进来。成功时刻不变、
        起点按最早请求（hkL1aMs = tSuccessPushMs − 最早 tOrderOutMs，与 docs/binance.md
        §90「平台重发含在 L1a′ 内」同口径）。formal 在飞的 receipt poller 经
        _settle_receipt 的 dup→formal 重定向落到新 formal（闩锁随之移交，不起第二
        poller）。响应/receipt 补链路径调用——preview/sign 晋升路径不做（fomo 同 relaySwapId
        取最晚 preview 是既定口径，见 route_ws_frame）。"""
        if leg.get("dup"):
            leg["dup"] = False
            leg["_dupOf"] = None
            self.diag["orderReqDuplicate"] = max(0, self.diag["orderReqDuplicate"] - 1)
        if leg.get("slot") is None:
            leg["slot"] = formal.get("slot")
            if leg.get("unbound"):
                # 容量耗尽/链未知先标 unbound 的最早请求接过 formal 槽位——不留假 unbound
                leg["unbound"] = False
                self.diag["orderReqUnbound"] = max(0, self.diag["orderReqUnbound"] - 1)
        formal["slot"] = None
        formal["round"] = None
        formal["pendingBind"] = False
        formal["dup"] = True
        formal["_dupOf"] = leg
        # Keep fanout aliases flat when out-of-order responses repeatedly transfer
        # the slot. Receipt callbacks and identity claims must reach one owner.
        for alias in self.legs.get(leg.get("runId"), []):
            if alias is not formal and alias.get("_dupOf") is formal:
                alias["_dupOf"] = leg
        self.diag["orderReqDuplicate"] += 1
        # 已落地证据随 formal 资格移交（首写不覆盖本腿已有值）；dup 腿清空成功/receipt
        # ——成功永不留在 dup 腿（移交同律）
        for k in (
            "tSuccessPushMs",
            "wsSha",
            "successEvidence",
            "successAnchorKeys",
            "tReceiptMs",
            "receipt",
            "txHash",
        ):
            if leg.get(k) is None and formal.get(k) is not None:
                leg[k] = formal[k]
            if k != "txHash":
                formal[k] = None
        # Chain-channel diagnostics share the same request anchor. An alias was
        # excluded from collection, so retain each channel's first observation
        # when that earlier request becomes formal after receipt backfill.
        for channel, arrival in formal.pop("_channelArrivals", {}).items():
            arrivals = leg.setdefault("_channelArrivals", {})
            first = arrivals.get(channel)
            if first is None or arrival["tMs"] < first["tMs"]:
                arrivals[channel] = arrival
        if formal.get("receiptPolling"):
            # 闩锁移交（不是复制）——在飞 poller 回写经 _settle_receipt/_settle_incomplete
            # 的 dup→formal 重定向落本腿；旧 formal 闩锁必须释放，否则其永挂
            # （无 receipt/无 incomplete）→ run 被 retention pin 死永不回收
            leg["receiptPolling"] = True
            formal["receiptPolling"] = False
        self._merge_anchor_locked(leg, formal.get("_anchor") or {}, source_leg=formal)

    def _mark_dup_of_locked(self, leg, formal):
        """dup 腿登记归并目标 formal（已绑槽腿）并把 dup 腿已建立的独立锚
        （请求体/响应抽取的 orderId/clientOrderId/txHash——**不是** WS 学到的值）并入
        formal 锚：同单的第二次 ack 常带首次失败 ack 缺失的 orderId（100001005 →
        000000，docs/binance.md §90），成功帧据此仍能正向落到 formal 腿。首次观察冻结
        ——formal 已有的键不覆盖；不等 = 身份冲突（不猜，记 diag.anchorConflict）。"""
        leg["_dupOf"] = formal
        self._merge_anchor_locked(formal, leg.get("_anchor") or {}, source_leg=leg)

    def _merge_anchor_locked(self, target, kv, source_leg=None):
        """（须持锁）把独立锚键值并入 target 腿（首次写入；冲突不覆盖、记诊断）。
        被否决（_anchorVeto）的 target 锚已自相矛盾——不再并入任何值。"""
        if target.get("windowClosed") or target.get("_anchorVeto"):
            return
        anchor = target.setdefault("_anchor", {})
        for k, v in kv.items():
            if k not in _IDENTITY_KEYS or v is None:
                continue
            if anchor.get(k) is None:
                anchor[k] = v
                if target.get(k) is None:
                    target[k] = v
            elif _ident_norm(k, anchor[k]) != _ident_norm(k, v):
                diag = target.setdefault("_anchorDiag", {})
                conflicts = diag.setdefault("conflicts", [])
                if k not in conflicts:
                    conflicts.append(k)
                if source_leg is not None:
                    sd = source_leg.setdefault("_anchorDiag", {}).setdefault(
                        "conflicts", []
                    )
                    if k not in sd:
                        sd.append(k)
                self.diag["anchorConflict"] += 1
                if target.get("runId"):
                    self.run_diag[target["runId"]]["anchorConflict"] += 1

    def dedup_order_identity(self, leg):
        """响应建立订单身份后调用（冻结腿不处理）。
        响应路径允许 formal 资格向最早请求转移（allow_transfer）。"""
        with self.lock:
            if leg.get("windowClosed"):
                return
            self._dedup_order_identity_locked(leg, allow_transfer=True)

    def confirm_broadcast_identity(self, leg):
        """A raw RPC response proves identity/acceptance, never platform success.
        Identity-pending requests do not consume slots. On confirmation, deduplicate
        and transfer the formal slot to the earliest request before binding anew.
        """
        with self.lock:
            if leg.get("windowClosed") or leg.get("_anchorVeto") or not (leg.get("_anchor") or {}).get("txHash"):
                return
            leg["anchorRole"] = "order"
            if not self._dedup_order_identity_locked(leg, allow_transfer=True):
                self._promote_leg_locked(leg)

    def _receipt_backfill_rebind_locked(self, leg, receipt):
        """（须持锁，_settle_receipt 写入 receipt 前调用）receipt 实证链
        （receipt 门已过——transactionHash≡腿 hash + status=1 + 块身份齐全的轮询胜出
        候选）回填 leg.chain——不猜链时的唯一合法来源；pendingBind 腿随即按 manifest
        槽补绑（DONE·receipt 两种事件顺序同计数同腿身份）。容量/槽位不匹配 →
        unbound 落定；链仍未实证 → 保持 pendingBind 至关窗。不取消既有关联/新鲜度门。"""
        rchain = receipt.get("chain")
        if rchain and leg.get("chain") is None and CHAINS.get(rchain):
            leg["chain"] = rchain
        if leg.get("dup"):
            self._dedup_order_identity_locked(leg, allow_transfer=True)
        if not leg.get("pendingBind"):
            return
        chain = leg.get("chain")
        if not chain:
            return  # 链仍未实证——继续等（关窗 freeze 时 unbound 落定）
        leg["pendingBind"] = False
        if leg.get("slot") is not None:
            return  # A reopened alias may already have reclaimed its own slot.
        # Receipt backfill is another binding path: use the same exact-order
        # identity gate before consuming capacity, and retain the earliest anchor.
        if self._dedup_order_identity_locked(leg, allow_transfer=True):
            return
        b = self._bind_slot_locked(leg["runId"], leg["platform"], chain)
        if b is not None:
            leg["slot"] = b  # 槽位绑定不写 round（授权轮号不由到达序推导）
        else:
            leg["unbound"] = True
            self.diag["orderReqUnbound"] += 1

    def _promote_leg_locked(self, leg):
        """preview/sign 腿被成功帧正向匹配 = 执行证据（须持锁）——原子晋升 'order'
        并补授权槽绑定。绑定前先做订单身份去重——同单已绑槽 → dup
        诊断归并，不占新槽；链未实证（腿无 chain 且尚无 receipt 实证链）→
        pendingBind 等待 receipt 补绑（不猜链；关窗仍未实证 → freeze 时 unbound
        落定）；链已实证但容量/槽位不匹配 → unbound 诊断。链取自腿自身 chain；
        缺失时退用 receipt 实证链（该 receipt 已经 transactionHash≡腿 hash +
        status=1 + 块身份门核验，非猜测）。被否决腿不晋升。"""
        if leg.get("_anchorVeto"):
            return
        leg["anchorRole"] = "order"
        mark = self.marks.get(leg.get("runId")) or {}
        if (
            mark.get("manifestLegs")
            and leg.get("slot") is None
            and not leg.get("unbound")
        ):
            if self._dedup_order_identity_locked(leg):
                return
            chain = leg.get("chain") or (leg.get("receipt") or {}).get("chain")
            if not chain:
                leg["pendingBind"] = True
                return
            b = self._bind_slot_locked(leg["runId"], leg["platform"], chain)
            if b is not None:
                leg["slot"] = b  # 槽位绑定不写 round（授权轮号不由到达序推导）
            else:
                leg["unbound"] = True
                self.diag["orderReqUnbound"] += 1

    def _new_leg_locked(self, run_id, platform, chain, ev):
        """建腿（须持锁）。fomo 预览/下单 L7 不可区分 → 腿先标 anchorRole=
        'preview'；padre 的 Turnkey sign_raw_payload 未证明与 order 一一对应 →
        'sign'。两者均不计 attempted、不消费 manifest 槽位、不单独产成功；成功帧
        正向匹配 = 执行证据，命中即经 _promote_leg_locked 原子晋升 'order'。
        有明细 manifest 时 order 腿建腿即绑定授权槽；无槽可绑 → unbound
        诊断腿（不计 attempted）。绑定前先做订单身份去重——请求侧
        身份（ev 携带，如 binance clientOrderId）正向匹配同 run 已绑槽腿 → dup
        诊断（不占新槽、不新增 attempted）；身份缺失/冲突不猜（走容量/unbound
        律）。round 恒 None——manifest 只带 platform×chain×rounds
        数量、请求不带轮号，到达序不等于授权轮（首轮未观测时整体前移）；
        到达序另记 observationIndex（platform×chain 内从 1 起，纯观察事实，
        chain 按建腿时所知——未知链自成一序），授权轮由 EU 侧唯一关联
        （精确 tx / 订单身份）回填 authorizedRound。单 run 腿预算超限 → None
        并计 overflow。"""
        if len(self.legs[run_id]) >= MAX_LEGS_PER_RUN:
            self.overflow[run_id]["legs"] += 1
            return None
        mark = self.marks.get(run_id) or {}
        detail = mark.get("manifestLegs")
        role = (
            "preview"
            if platform == "fomo"
            else ("sign" if platform == "padre" else "order")
        )
        if ev.get("orderRequestSource") in ("okx-evm-send", "okx-solana-send"):
            role = "send"  # identity-pending fanout: no authorized slot until exact-flow ack
        slot = None
        unbound = False
        pending_bind = False
        dup = False
        dup_of = None
        if detail and role == "order":
            ident = {
                k: ev.get(k)
                for k in (_IDENTITY_KEYS if platform == "okx" else ("orderId", "clientOrderId"))
                if ev.get(k) is not None
            }
            formal = (
                self._same_order_bound_leg_locked(run_id, ident, platform=platform, chain=chain)
                if ident
                else None
            )
            if formal is not None:
                # 同单重复请求归入同一授权槽（诊断腿，不占新槽）；登记
                # 归并目标——成功帧按身份路由时归到 formal 腿，永不留在 dup 腿
                dup = True
                dup_of = formal
                self.diag["orderReqDuplicate"] += 1
            else:
                b = self._bind_slot_locked(run_id, platform, chain)
                if b is not None:
                    slot = b
                elif chain is None:
                    pending_bind = True
                else:
                    unbound = True
                    self.diag["orderReqUnbound"] += 1
        # 到达序是纯观察事实（含 preview/sign/dup/unbound 腿——到达即序）
        obs_idx = (
            sum(
                1
                for x in self.legs[run_id]
                if x["platform"] == platform and x.get("chain") == chain
            )
            + 1
        )
        leg = {
            "platform": platform,
            "chain": chain,
            "runId": run_id,
            "round": None,  # 授权轮号未知——唯一关联（EU 侧 tx/身份回配）前不填写
            "observationIndex": obs_idx,  # platform×chain 内到达序（纯观察事实）
            "anchorRole": role,
            "slot": slot,  # 绑定的 manifest 槽位下标（内部态，不出 timeline）
            "unbound": unbound,  # 明细 manifest 下无槽可绑的下单观察（诊断）
            "dup": dup,  # 与同 run 已绑槽腿正向同单的重复请求（诊断）
            "pendingBind": pending_bind,  # order 链未实证——等 receipt 补绑
            "tOrderOutMs": ev["t"],
            "_headSnapshots": ev["headSnapshots"] if "headSnapshots" in ev else HEADS.snapshot(run_id, ev["t"]),
            "orderRequestSource": ev.get("orderRequestSource", "platform-order"),
            "tOrderOutWallMs": ev.get("twms"),
            "orderFlowId": ev.get("flowId"),
            "tFirstRespMs": None,
            "tSuccessPushMs": None,
            "tReceiptMs": None,
            "txHash": None,
            "orderId": None,
            "clientOrderId": None,
            "wsSha": None,
            "successEvidence": None,
            "receipt": None,
            "incomplete": None,
            "receiptPolling": False,  # receipt poller 闩锁（每腿至多一个）
            "windowClosed": False,  # 关窗冻结标记
            "_anchor": {},  # 请求/响应建立的独立锚（成功/hash 采纳只认它）
            # 锚诊断（键名/布尔，不含身份值）——旧正则是否与结构化锚分歧、冲突键、
            # 歧义键；成功帧正向命中的锚键；dup 归并目标（内部引用，不出 timeline）
            "_anchorDiag": {},
            "_dupOf": dup_of,
            "successAnchorKeys": None,
            "at_wall": ev["tw"],
        }
        self.legs[run_id].append(leg)
        self.open_legs[platform] = leg
        return leg

    def new_leg(self, run_id, platform, chain, ev):
        with self.lock:
            return self._new_leg_locked(run_id, platform, chain, ev)

    def capture_window(self, observed_ms):
        """hook 入口绑定窗口身份；之后解析不能把旧请求重新归给新窗口。"""
        with self.lock:
            now = time.monotonic()
            for r, m in sorted(
                self.marks.items(), key=lambda kv: kv[1]["opened"], reverse=True
            ):
                if m["closed"] is None and now - m["opened"] < MARK_TTL_S:
                    if observed_ms >= m["opened"] * 1000:
                        return r, m["epoch"]
                    return None  # 观察后才打开的窗口不能接收该事件
                if m["closed"] is None:
                    m["closed"] = now
                    self._freeze_run_locked(r)
            return None

    def order_in_open_window(self, window, platform, chain, ev):
        """解析结束后只复验入口捕获的 runId+epoch；失效则丢弃，绝不重新选窗。
        窗口复验、事件和腿写入同锁，覆盖 close、TTL、新窗与同 runId 重开。"""
        with self.lock:
            rid, epoch = window
            mark = self.marks.get(rid)
            now = time.monotonic()
            if mark and mark["closed"] is None and now - mark["opened"] >= MARK_TTL_S:
                mark["closed"] = now
                self._freeze_run_locked(rid)
            if (
                not mark
                or mark["closed"] is not None
                or mark.get("epoch") != epoch
                or ev["t"] < mark["opened"] * 1000
            ):
                self.diag["orderReqNoWindow"] += 1
                return None, None
            if len(self.legs[rid]) >= MAX_LEGS_PER_RUN:
                self.overflow[rid]["legs"] += 1  # 腿预算超限——事件连同腿一起丢弃
                return None, None
            self._add_event_locked(rid, ev)
            leg = self._new_leg_locked(rid, platform, chain, ev)
            if leg is None:
                return None, None
            return rid, leg

    def bind_fomo_subscribe(self, fid):
        """FOMO 页面出站 subscribe 帧 filters.id → 绑到最新 fomo 腿（
        按到达时序绑定、腿已有 orderId 锚则不动）。这是请求侧锚源而非入站成功帧，
        不按身份路由；已有不同锚时静默不动（时序绑定不构成身份冲突证据，不记 diag）。
        被否决腿（锚已自相矛盾）不再吸收新绑定。"""
        with self.lock:
            leg = self.open_legs.get("fomo")
            if leg is None or leg.get("windowClosed") or leg.get("_anchorVeto"):
                return None
            anchor = leg.setdefault("_anchor", {})
            if anchor.get("orderId") is None and fid is not None:
                anchor["orderId"] = fid
                if leg.get("orderId") is None:
                    leg["orderId"] = fid
            return leg

    def route_ws_frame(self, platform, ids, defer_nomatch=False):
        """入站 WS 帧按**锚身份**路由到目标腿——候选是
        所有开窗 run 内该平台未冻结的腿（不只平台最新腿：最新腿是 dup/后一轮时
        前一轮成功帧不得被整帧丢弃、成功不得落在 dup 腿）。判定：
        ①无身份键（orderId/clientOrderId/txHash）的帧不路由；
        ②与腿独立锚 _anchor 正向匹配（≥1 共同键且全部相等）的腿为候选；候选按
          非 dup > 已绑槽 > 最早出站排序；
        ③命中 dup 腿 → 归到其 formal 腿（_dupOf 链）——成功永不留在 dup 腿；formal
          与帧冲突 → 整帧不归属（多订单疑似，不猜）；
        ④零命中 → 不归属并计 diag（wsFrameNoLegMatch / wsFrameAnchorConflict）——
          缺失可见，不静默。返回 (leg|None, reason)。
        被否决腿（_anchorVeto，锚自相矛盾）不作候选——冲突帧不得驱动
        其晋升/成功；帧只命中被否决腿 → 计 wsFrameAnchorVeto（区别于锚值互斥的
        wsFrameAnchorConflict）；dup 归并目标被否决 → 同键拒收。
        defer_nomatch=True 时 no-leg-match 暂不计数——调用方可能
        把早到帧（响应未到、锚待定）缓冲后重审；冲突/否决仍是终态，照常计数。"""
        with self.lock:
            if not any(ids.get(k) is not None for k in _IDENTITY_KEYS):
                return None, "no-identity"
            now = time.monotonic()
            rids = self._open_run_ids_locked(now)
            positives, conflicts, vetoed = [], 0, 0
            for rid in rids:
                for leg in self.legs.get(rid, []):
                    if leg["platform"] != platform or leg.get("windowClosed"):
                        continue
                    verdict, _ = _anchor_verdict(leg.get("_anchor") or {}, ids)
                    if verdict == "positive":
                        if leg.get("_anchorVeto"):
                            vetoed += 1
                        else:
                            positives.append(leg)
                    elif verdict == "conflict":
                        conflicts += 1
            newest = rids[0] if rids else None
            if not positives:
                if vetoed:
                    reason, key = "anchor-veto", "wsFrameAnchorVeto"
                elif conflicts:
                    reason, key = "anchor-conflict", "wsFrameAnchorConflict"
                else:
                    reason, key = "no-leg-match", "wsFrameNoLegMatch"
                if not (defer_nomatch and key == "wsFrameNoLegMatch"):
                    # 缓冲候选的零命中推迟计数（缓冲/重审后仍零命中才计）；
                    # 冲突/否决是终态，照常即计
                    self.diag[key] += 1
                    if newest:
                        self.run_diag[newest][key] += 1
                return None, reason

            # 候选排序：非 dup > 已绑槽（formal）> 出站时刻——order/sign 腿取最早（首次
            # 下单请求 = EU 侧 order_http_out 同义，平台重发含在 L1a′ 内，与 docs/binance.md
            # §90 口径一致）；preview 腿取最晚（fomo 预览/执行 L7 不可区分，同 relaySwapId
            # 的最后一次 /swaps/v2 最接近执行 POST——沿用 r7 前「最新腿」近似，不放大预览提前量）
            def _pref(x):
                t = x.get("tOrderOutMs") or 0
                return (
                    bool(x.get("dup")),
                    x.get("slot") is None,
                    -t if x.get("anchorRole") == "preview" else t,
                )

            positives.sort(key=_pref)
            target = positives[0]
            reason = "positive"
            if target.get("dup"):
                formal = target.get("_dupOf")
                hops = 0
                while formal is not None and formal.get("dup") and hops < 8:
                    formal = formal.get("_dupOf")
                    hops += 1
                if formal is None or formal.get("windowClosed"):
                    self.diag["wsFrameDupNoFormal"] += 1
                    if newest:
                        self.run_diag[newest]["wsFrameDupNoFormal"] += 1
                    return None, "dup-without-formal"
                if formal.get("_anchorVeto"):
                    self.diag["wsFrameAnchorVeto"] += 1
                    if newest:
                        self.run_diag[newest]["wsFrameAnchorVeto"] += 1
                    return None, "dup-formal-vetoed"
                verdict, _ = _anchor_verdict(formal.get("_anchor") or {}, ids)
                if verdict in ("conflict", "miss"):
                    self.diag["wsFrameAnchorConflict"] += 1
                    if newest:
                        self.run_diag[newest]["wsFrameAnchorConflict"] += 1
                    return None, "dup-formal-conflict"
                target = formal
                reason = "dup-merged"
            return target, reason

    def buffer_ws_unit(
        self, platform, host, payload, ids, success, t, tw, count_diag=True
    ):
        """WS 成功/身份帧可能先于其 HTTP 响应到达（docs/okx.md §2.4：
        Sol 的 /broadcast 常晚于 WS 确认）——零命中且本开窗该平台还有**无锚腿**
        （响应未到、锚待定）时缓冲该单元：保留单调源时间 t / 墙钟 tw / 窗口身份
        （runId+epoch）/ 方向（fromClient），响应/subscribe 建立锚后按与实时帧
        相同的门重审（take_ws_buffer → _adopt_ws_unit）。该平台开窗腿均已有锚 =
        零命中终态 → 不缓冲（调用方即计 wsFrameNoLegMatch）；关窗 freeze 即弃；
        有界（MAX_WS_BUFFER_PER_RUN，超限计 wsBufferOverflow 并拒绝）。返回是否
        已缓冲。"""
        with self.lock:
            now = time.monotonic()
            rids = self._open_run_ids_locked(now)
            rid = rids[0] if rids else None
            if rid is None:
                return False
            anchorless = any(
                leg["platform"] == platform
                and not leg.get("windowClosed")
                and not (leg.get("_anchor") or {})
                for leg in self.legs.get(rid, [])
            )
            if not anchorless:
                return False
            mark = self.marks.get(rid) or {}
            buf = self.ws_buffer[rid]
            if len(buf) >= MAX_WS_BUFFER_PER_RUN:
                self.diag["wsBufferOverflow"] += 1
                self.run_diag[rid]["wsBufferOverflow"] += 1
                return False
            buf.append(
                {
                    "platform": platform,
                    "host": host,
                    "payload": payload,
                    "ids": {k: v for k, v in (ids or {}).items() if v is not None},
                    "success": success,
                    "t": t,  # 单调源时间（hook 入口冻结）——采纳时成功时刻用它，不用重审时刻
                    "tw": tw,
                    "epoch": mark.get("epoch"),
                    "fromClient": False,  # 方向：只缓冲入站帧（出站帧无成功语义）
                }
            )
            if count_diag:
                self.diag["wsFrameBuffered"] += 1
                self.run_diag[rid]["wsFrameBuffered"] += 1
            return True

    def take_ws_buffer(self, run_id, platform):
        """响应/subscribe 建立锚后取出本 run 该平台全部缓冲单元
        重审——窗口必须仍开且 epoch 一致（关窗/重开的缓冲已由 freeze 丢弃；防御性
        不符即弃）。取出的单元由调用方按实时帧同一管线重审，仍零命中且无锚腿待定
        的可回缓冲。"""
        with self.lock:
            mark = self.marks.get(run_id)
            if not mark or mark.get("closed") is not None:
                self.ws_buffer.pop(run_id, None)
                return []
            taken, rest = [], []
            for e in self.ws_buffer.get(run_id) or []:
                if e.get("platform") == platform and e.get("epoch") == mark.get(
                    "epoch"
                ):
                    taken.append(e)
                else:
                    rest.append(e)
            if rest:
                self.ws_buffer[run_id] = rest
            else:
                self.ws_buffer.pop(run_id, None)
            return taken

    def note_frame_diag(self, key):
        """路由层之外（缓冲拒绝/终态）的帧诊断计数——与 route_ws_frame 同
        口径（全局 + 最新开窗 run）。"""
        with self.lock:
            self.diag[key] += 1
            rids = self._open_run_ids_locked(time.monotonic())
            if rids:
                self.run_diag[rids[0]][key] += 1

    def update_leg(self, leg, kv):
        with self.lock:
            if leg.get("windowClosed"):
                return
            for k, v in kv.items():
                if v is not None and leg.get(k) is None:
                    leg[k] = v

    def update_anchor(self, leg, kv):
        """请求/响应（独立锚）建立的 id/chain——腿字段首次写入 + 锚登记。
        成功判定与 hash 采纳只认锚：WS 帧自带 id 先写入腿再自证关联不算数。
        已有锚键值不等 = 身份冲突（不覆盖、记 _anchorDiag.conflicts +
        diag.anchorConflict）；dup 腿的锚同步并入其 formal 腿（_mark_dup_of_locked 同律）。
        身份冲突 = 否决条件——腿的 relayId/orderId 一经响应/subscribe
        确证，冲突身份使锚自相矛盾 → _veto_leg_locked 降级 diagnostic（路由/晋升/
        成功/receipt 轮询全部关闭，observed 不增加）。"""
        with self.lock:
            if leg.get("windowClosed"):
                return
            anchor = leg.setdefault("_anchor", {})
            vetoed = False
            for k, v in kv.items():
                if v is None:
                    continue
                if k in _IDENTITY_KEYS:
                    if anchor.get(k) is None:
                        anchor[k] = v
                        if leg.get(k) is None:
                            leg[k] = v
                    elif _ident_norm(k, anchor[k]) != _ident_norm(k, v):
                        conflicts = leg.setdefault("_anchorDiag", {}).setdefault(
                            "conflicts", []
                        )
                        if k not in conflicts:
                            conflicts.append(k)
                        self.diag["anchorConflict"] += 1
                        if leg.get("runId"):
                            self.run_diag[leg["runId"]]["anchorConflict"] += 1
                        vetoed = True
                elif leg.get(k) is None:
                    leg[k] = v
            if vetoed:
                self._veto_leg_locked(leg)
            formal = leg.get("_dupOf")
            if formal is not None and not leg.get("_anchorVeto"):
                self._merge_anchor_locked(
                    formal, {k: kv.get(k) for k in _IDENTITY_KEYS}, source_leg=leg
                )

    def _veto_leg_locked(self, leg):
        """（须持锁）锚身份自相矛盾的腿降级 diagnostic——
        incomplete=anchor-conflict（首写），route_ws_frame 候选排除 + 成功写入/晋升/
        receipt 闩锁各自带守卫，observed 不增加。被否决 formal 的 dup 子腿同单证明
        同失效 → 重开为独立候选（_reopen_dup_locked）。
        否决前已落地的成功/receipt 不删除（内部原始时间保留），
        合格指标由 timeline 序列化层统一撤销（hkL1aMs/hkL3Ms → null，撤销值入
        hkL1aMsVetoed/hkL3MsVetoed 诊断键 + anchorVeto:true）——late veto 后无合格
        值漏出。"""
        if leg.get("_anchorVeto"):
            return
        leg["_anchorVeto"] = True
        if not leg.get("incomplete"):
            leg["incomplete"] = "anchor-conflict"
        for other in self.legs.get(leg.get("runId"), []):
            if other.get("dup") and other.get("_dupOf") is leg:
                self._reopen_dup_locked(other)

    def note_anchor_diag(self, leg, structured, legacy, ambiguous=None):
        """记录锚来源诊断（仅键名/布尔）：结构化锚键、旧整-body 正则是否分歧
        （legacyRegexDiverged——旧正则会写出的 orderId/txHash 与结构化值不等，或只有
        一侧有值）、歧义键（同节点/白名单键多值不一致 → 不取，记 ambiguous）。"""
        with self.lock:
            diag = leg.setdefault("_anchorDiag", {})
            diverged = False
            for k in ("orderId", "txHash"):
                a, b = structured.get(k), legacy.get(k)
                if (a is None) != (b is None) or (
                    a is not None and _ident_norm(k, a) != _ident_norm(k, b)
                ):
                    diverged = True
            diag["legacyRegexDiverged"] = diverged
            if ambiguous:
                amb = diag.setdefault("ambiguous", [])
                for k in ambiguous:
                    if k not in amb:
                        amb.append(k)
                self.diag["anchorAmbiguous"] += 1
                if leg.get("runId"):
                    self.run_diag[leg["runId"]]["anchorAmbiguous"] += 1

    def claim_receipt_poll(self, leg):
        """receipt poller 原子闩锁（HTTP/WS 入口共用）——每腿至多一个 poller，
        轮询腿锁定的 hash；已 settle/已轮询/已冻结 → None。
        被否决腿不起 poller（observed 不增加）。未知链的更早 OKX order alias
        仍须独立核验自己的精确 tx，才能接回最早请求锚；其余 dup 不起 poller。"""
        with self.lock:
            if (
                leg.get("windowClosed")
                or leg.get("_anchorVeto")
                or leg.get("tReceiptMs") is not None
                or leg.get("receiptPolling")
            ):
                return None
            h = leg.get("txHash")
            if leg.get("dup"):
                formal = leg.get("_dupOf") or {}
                anchor_h = (leg.get("_anchor") or {}).get("txHash")
                formal_h = (formal.get("_anchor") or {}).get("txHash")
                if not (
                    leg.get("platform") == "okx"
                    and leg.get("anchorRole") == "order"
                    and leg.get("chain") is None
                    and formal.get("runId") == leg.get("runId")
                    and formal.get("platform") == "okx"
                    and formal.get("slot") is not None
                    and not formal.get("dup")
                    and not formal.get("windowClosed")
                    and not formal.get("_anchorVeto")
                    and h and anchor_h and formal_h
                    and _ident_norm("txHash", h) == _ident_norm("txHash", anchor_h)
                    and _ident_norm("txHash", anchor_h) == _ident_norm("txHash", formal_h)
                    and not self._dup_identity_conflict_locked(leg, formal)
                    and isinstance(leg.get("tOrderOutMs"), (int, float))
                    and isinstance(formal.get("tOrderOutMs"), (int, float))
                    and leg["tOrderOutMs"] < formal["tOrderOutMs"]
                ):
                    return None
            if not h or not _receipt_candidates(leg.get("chain"), h):
                return None
            leg["receiptPolling"] = True
            return h

    def note_channel_arrival(self, chain, tx_value, channel, api, t_ms):
        """v10：链域通道（nodeone/fullnode，www.okx.com）成功通知到达登记——HK 钟。
        仅精确 tx 身份匹配（锚内 txHash 恒等）的开窗腿；每腿每通道首到冻结。
        与成功判定管线完全分离：不触成功/资格/锚门，纯到达时刻记录。"""
        if not tx_value:
            return
        with self.lock:
            now = time.monotonic()
            for rid in self._open_run_ids_locked(now):
                for leg in self.legs.get(rid, []):
                    if leg.get("dup") or leg.get("windowClosed") or leg.get("_anchorVeto"):
                        continue
                    anchor = (leg.get("_anchor") or {}).get("txHash")
                    if anchor is None:
                        continue
                    if _ident_norm("txHash", anchor) != _ident_norm("txHash", tx_value):
                        continue
                    if chain and leg.get("chain") and leg["chain"] != chain:
                        continue
                    arr = leg.setdefault("_channelArrivals", {})
                    if channel not in arr:
                        arr[channel] = {"tMs": t_ms, "api": api}
                        self.diag["chainChannelArrivals"] = self.diag.get("chainChannelArrivals", 0) + 1

    def id_claimed(self, run_id, key, value, except_leg=None):
        """同 run 内某 id/hash 是否已被其他腿占用（防陈旧/跨批重放帧把旧值挂到新腿）。
        只对 txHash/orderId/clientOrderId 调用——chain 是属性不是身份，不去重。"""
        if value is None:
            return False
        with self.lock:
            for leg in self.legs.get(run_id, []):
                if leg is except_leg:
                    continue
                # r8：dup 腿与其 formal 腿是同一订单的别名——同 id 不算跨腿占用
                if except_leg is not None and (
                    leg.get("_dupOf") is except_leg or except_leg.get("_dupOf") is leg
                ):
                    continue
                if leg.get(key) is not None and _ident_norm(
                    key, leg[key]
                ) == _ident_norm(key, value):
                    return True
        return False

    def timeline(self, run_id, full=False):
        with self.lock:
            legs = []
            for x in self.legs.get(run_id, []):
                c = dict(x)
                # r8：dup 归并目标以 formal 腿的 orderFlowId 输出（不暴露内部引用）
                formal = x.get("_dupOf")
                c["_mergedIntoFlowId"] = (
                    formal.get("orderFlowId") if isinstance(formal, dict) else None
                )
                c["_anchorDiag"] = dict(x.get("_anchorDiag") or {})
                c["_anchor"] = dict(x.get("_anchor") or {})
                legs.append(c)
            mark = dict(self.marks.get(run_id) or {})
            gone = not legs and not mark and run_id in self.gone
            ov = dict(self.overflow.get(run_id) or {})
            rdiag = dict(self.run_diag.get(run_id) or {})
        if gone:
            # 已回收 run 明确标 gone——区别于「从未见过的 run」（空 timeline）
            return {
                "schemaVersion": 2,
                "runId": run_id,
                "gone": True,
                "hkClockAt": _wall_iso(),
                "legs": [],
                **_collection_identity(),
            }
        if not legs and not mark and run_id in _dropped_run_ids():
            # 热重载接管前旧实例的 run——Store 不随重载交接，如实标 dropped
            # （区别于 gone=retention 回收、以及从未见过的空 timeline）
            return {
                "schemaVersion": 2,
                "runId": run_id,
                "gone": True,
                "droppedInReload": True,
                "detail": "addon 热重载接管：旧实例采集态未交接，本 run 证据丢失",
                "hkClockAt": _wall_iso(),
                "legs": [],
                **_collection_identity(),
            }
        out = []
        for x in legs:
            t0 = x["tOrderOutMs"]
            incomplete = x.get("incomplete")
            if incomplete is None and x.get("windowClosed"):
                # 窗口结束逐腿补缺失状态（输出层补写，不改内部态）——
                # 关窗后无任何结果的腿 incomplete=null，缺失不可见。preview/sign 腿
                # 从未获执行证据 → no-execution-proof；order 腿无成功观察 →
                # no-success-observed；有成功无 receipt → no-receipt-observed。
                # dup 腿的成功一律归 formal 腿——关窗后如实标 duplicate-of-bound-leg
                # （不是 no-success-observed：同单证据在 formal 腿上，见 evidence.mergedIntoFlowId）
                if x.get("anchorRole") == "send":
                    incomplete = "broadcast-identity-unconfirmed"
                elif x.get("anchorRole") in ("preview", "sign"):
                    incomplete = "no-execution-proof"
                elif x.get("dup"):
                    incomplete = "duplicate-of-bound-leg"
                elif x.get("tSuccessPushMs") is None:
                    incomplete = "no-success-observed"
                elif x.get("tReceiptMs") is None:
                    incomplete = "no-receipt-observed"
            # 锚冲突否决腿（_anchorVeto）的合格指标统一撤销——
            # 序列化层单点收口，success/promotion/receipt 先于否决落定的所有 late-veto
            # 路径同律：hkL1aMs/hkL3Ms 置 null，被撤销值保留在 hkL1aMsVetoed/
            # hkL3MsVetoed 诊断键；腿带 anchorVeto:true 不再以 bound/eligible 呈现
            # （消费者按 anchorVeto 排除）。内部腿的原始事件时间戳保留不动——证据不删。
            vetoed = bool(x.get("_anchorVeto"))
            # 毒化源 = 身份未解决的更早同链 send（juk09/7ngeq 边界：它可能就是后续
            # 订单的真实广播，锚起点不可信——不放宽）。dup（已归并）腿不是毒化源：
            # 其广播身份已被正向同单匹配归并到已绑槽 formal 腿（成功/receipt/poller
            # 闩锁同律归 formal），身份数学上已解决，不再是未知广播——solana 扇出
            # 形态下页中止的重复扇出流永不获响应（dup 腿不起 poller、保持
            # anchorRole=send），若仍计毒化源，R1 的已归并 send 会毒化全部后续轮
            # （2026-09-22 批 1g7ms 实证：R2-R8 broadcast-anchor-unconfirmed 全灭）。
            # 归并证明被推翻（身份冲突/formal 否决 → _reopen_dup_locked）的腿恢复
            # 非 dup，重新参与毒化。
            unresolved_send = x.get("platform") == "okx" and x.get("anchorRole") == "order" and any(
                prior.get("platform") == "okx" and prior.get("anchorRole") == "send"
                and not prior.get("dup")
                and prior.get("chain") == x.get("chain")
                and prior.get("tOrderOutMs", float("inf")) < t0
                for prior in legs
            )
            if unresolved_send:
                incomplete = "broadcast-anchor-unconfirmed"
            chain_head = _head_with_receipt(x)
            if unresolved_send and chain_head:
                chain_head = {**chain_head, "status": "broadcast-anchor-unconfirmed"}
            l1a = _delta(x.get("tSuccessPushMs"), t0)
            l3 = _delta(x.get("tReceiptMs"), t0)
            adiag = x.get("_anchorDiag") or {}
            anchor_ev = {
                # 独立锚键名（值不出缺省模式；full=1 的 orderId/clientOrderId 另列）——
                # 空列表 = 无独立锚 → 成功帧结构上不可能正向关联（缺失原因可见）
                "keys": sorted(
                    k
                    for k in _IDENTITY_KEYS
                    if (x.get("_anchor") or {}).get(k) is not None
                ),
                "legacyRegexDiverged": adiag.get("legacyRegexDiverged"),
                "conflicts": list(adiag.get("conflicts") or []),
                "ambiguous": list(adiag.get("ambiguous") or []),
            }
            out.append(
                {
                    "chain": x.get("chain"),
                    "platform": x["platform"],
                    # round = 授权轮号，addon 侧恒 None（到达序不等于
                    # 授权轮——首轮未观测后整体前移）；授权轮由 EU 侧唯一
                    # 关联（精确 tx / 订单身份）回填 authorizedRound，见 client
                    "round": x.get("round"),
                    "observationIndex": x.get(
                        "observationIndex"
                    ),  # platform×chain 内到达序（纯观察事实）
                    # fomo 预览腿 anchorRole=preview、padre 签名腿 anchorRole=sign
                    # （均不计 attempted、不消费 manifest 槽位）；成功帧正向匹配 =
                    # 执行证据 → 晋升 order
                    "anchorRole": x.get("anchorRole"),
                    # 明细 manifest 下无槽可绑的下单观察（链未知/超额）
                    # ——留诊断不冒充授权槽位，不计 attempted/observed
                    "unbound": bool(x.get("unbound")),
                    # 与同 run 已绑槽腿正向同单的重复请求（归入同一授权槽，
                    # 不占新槽、不新增 attempted）；pendingBind：已晋升 order 但链未实证、
                    # 等 receipt 补绑的开窗暂态（关窗落定 unbound 或绑槽）
                    "dup": bool(x.get("dup")),
                    "pendingBind": bool(x.get("pendingBind")),
                    "tOrderOutMs": t0,
                    "orderRequestSource": x.get("orderRequestSource"),
                    # v10：链域通道（nodeone/fullnode）首到成功通知——HK 钟、精确 tx 身份
                    # 匹配、与成功判定管线分离；ms 相对本腿 tOrderOutMs（与 hkL1aMs 同锚）
                    "channelArrivals": [
                        {"channel": c, "api": v.get("api"), "ms": _delta(v.get("tMs"), t0)}
                        for c, v in sorted((x.get("_channelArrivals") or {}).items())
                        if _delta(v.get("tMs"), t0) is not None
                    ] or None,
                    "chainHead": chain_head,
                    "tFirstRespMs": x.get("tFirstRespMs"),
                    "tSuccessPushMs": x.get("tSuccessPushMs"),
                    "tReceiptMs": x.get("tReceiptMs"),
                    # 否决腿合格指标 null + 撤销值入诊断键
                    "hkL1aMs": (None if vetoed or unresolved_send else l1a),
                    "hkL3Ms": (None if vetoed or unresolved_send else l3),
                    "anchorVeto": vetoed,
                    "hkL1aMsVetoed": (l1a if vetoed else None),
                    "hkL3MsVetoed": (l3 if vetoed else None),
                    # full=1（loopback 控制面专用，证据恢复用）：完整 hash；缺省截短（公开安全）
                    "txHash": (
                        x.get("txHash") if full else _short_hash(x.get("txHash"))
                    ),
                    # full=1：请求/响应建立的订单身份（独立锚）——EU 侧 authorizedRound
                    # 唯一关联的另一证据；缺省不暴露（公开投影也不登记）
                    "orderId": (
                        (x.get("_anchor") or {}).get("orderId") if full else None
                    ),
                    "clientOrderId": (
                        (x.get("_anchor") or {}).get("clientOrderId") if full else None
                    ),
                    "evidence": {
                        "orderFlowId": x.get("orderFlowId"),
                        "wsFrameSha": x.get("wsSha"),
                        "success": ({**x["successEvidence"], "txHash": x["successEvidence"]["txHash"] if full else _short_hash(x["successEvidence"]["txHash"])} if x.get("successEvidence") else None),
                        "receipt": x.get("receipt"),
                        # 锚诊断（私有 raw；public DTO 未登记 → fail-closed 不投影）：
                        "anchor": anchor_ev,  # 锚键名/诊断（无身份值）
                        "successAnchorKeys": x.get(
                            "successAnchorKeys"
                        ),  # 成功帧正向命中的锚键
                        "mergedIntoFlowId": x.get(
                            "_mergedIntoFlowId"
                        ),  # dup 腿 → formal 腿 orderFlowId
                    },
                    "incomplete": incomplete,
                    "atWall": x.get("at_wall"),
                }
            )
        # 授权轮次计数 + 缺失腿占位（incomplete=no-order-observed）。
        # attempted/observed 同一集合——绑槽 order 腿（anchorRole=order
        # 且排除 unbound 诊断、dup 同单重复、pendingBind 待实证链）；observed 是该
        # 集合中有成功推送或 receipt 观察的子集（observed ⊆ attempted——sign/preview
        # 腿的 receipt 只作诊断与晋升依据，晋升失败不单独计入）。
        # 锚冲突否决腿（_anchorVeto）不计 observed——否决先于成功/
        # receipt 写入（各入口带守卫），此处是 receipt 先于否决落定的残余防线。
        # 否决腿的合格时长已在上方序列化层撤销（hkL1aMs/hkL3Ms
        # null + *Vetoed 诊断键），此计数口径不变（槽已消费的否决腿仍占 attempted）。
        bound_orders = [
            (i, x)
            for i, x in enumerate(out)
            if x.get("anchorRole") == "order"
            and not x.get("unbound")
            and not x.get("dup")
            and not x.get("pendingBind")
        ]
        attempted = len(bound_orders)
        observed = sum(
            1
            for i, x in bound_orders
            if (x["tSuccessPushMs"] is not None or x["tReceiptMs"] is not None)
            and not legs[i].get("_anchorVeto")
        )
        unbound = sum(1 for x in out if x.get("unbound"))
        requested = mark.get("requested")
        detail = mark.get("manifestLegs") or []
        placeholders = []
        if detail:
            # 占位按建腿时的显式槽位绑定计算（不贪心配对——贪心会让
            # chain 未知的请求吞掉某链的缺失占位）；未绑定的授权轮次逐槽出占位
            requested = 0
            for i, spec in enumerate(detail):
                need = spec.get("rounds") or 1  # per-leg rounds（缺失按 1——旧明细形）
                requested += need
                bound = sum(1 for x in legs if x.get("slot") == i)
                for _ in range(max(0, need - bound)):
                    placeholders.append(
                        _placeholder_leg(spec.get("platform"), spec.get("chain"))
                    )
        elif isinstance(requested, int) and requested > attempted:
            for _ in range(requested - attempted):
                placeholders.append(_placeholder_leg(None, None))
        result = {
            "schemaVersion": 2,
            "runId": run_id,
            "mark": mark,
            "hkClockAt": _wall_iso(),
            "counts": {
                "requested": requested,
                "attempted": attempted,
                "observed": observed,
                "unbound": unbound,
            },
            "legs": out + placeholders,
            # per-run 诊断计数——带身份却无腿可归属的帧 / 锚冲突拒收 / 身份冲突 /
            # 歧义（缺失可见；空 dict = 本 run 无此类事件）
            "diag": rdiag,
            # 方法版本 + 采集环境身份（sidecar 自证采集口径）
            **_collection_identity(),
        }
        if ov.get("legs") or ov.get("events"):
            result["overflow"] = ov  # 单窗预算截断如实标注
        return result


def _head_with_receipt(leg):
    sample = (leg.get("_headSnapshots") or {}).get(leg.get("chain"))
    if sample is None:
        return None
    receipt = leg.get("receipt") or {}
    height = receipt.get("blockNumber")
    try:
        height = (
            int(height, 16)
            if isinstance(height, str) and height.startswith("0x")
            else height
        )
    except ValueError:
        height = None
    return {**sample, "inclusion": {"hash": receipt.get("blockHash"), "height": height}}


def _placeholder_leg(platform, chain):
    """授权轮次登记了但未观察到下单请求的缺失腿占位——incomplete 如实标注，
    绝不发明时间戳。"""
    return {
        "chain": chain,
        "platform": platform,
        "round": None,
        "observationIndex": None,  # 占位腿无到达观察
        "anchorRole": None,
        "unbound": False,
        "dup": False,
        "pendingBind": False,
        "tOrderOutMs": None,
        "tFirstRespMs": None,
        "tSuccessPushMs": None,
        "tReceiptMs": None,
        "hkL1aMs": None,
        "hkL3Ms": None,
        "anchorVeto": False,  # 占位腿无否决（形状齐整）
        "hkL1aMsVetoed": None,
        "hkL3MsVetoed": None,
        "txHash": None,
        "evidence": {
            "orderFlowId": None,
            "wsFrameSha": None,
            "receipt": None,
            "anchor": None,
            "successAnchorKeys": None,
            "mergedIntoFlowId": None,
        },
        "incomplete": "no-order-observed",
        "atWall": None,
    }


class _AnchorView:
    """ws_success 判定看到的腿视图：id 键只暴露请求/响应（独立锚）建立的值，
    其余键透传腿本体——帧自带 id 不得先写入腿再自证关联。"""

    __slots__ = ("_leg",)

    def __init__(self, leg):
        self._leg = leg

    def get(self, k, default=None):
        if k in ("orderId", "clientOrderId", "txHash"):
            return (self._leg.get("_anchor") or {}).get(k, default)
        return self._leg.get(k, default)


def _delta(t, t0):
    return (
        round(t - t0, 1)
        if isinstance(t, (int, float)) and isinstance(t0, (int, float))
        else None
    )


def _short_hash(h):
    if not h or not isinstance(h, str):
        return h or None
    return h[:10] + "…" + h[-6:] if len(h) > 20 else h


def _wall_iso():
    return (
        time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        + f".{int((time.time() % 1) * 1000):03d}Z"
    )


def _mono_ms():
    """腿时间戳统一毫秒（字段名 *Ms）。历史 bug：曾存 monotonic 秒，hkL1aMs 0.6 实为 0.6s。"""
    return time.monotonic() * 1000.0


STORE = Store()


def _receipt_candidates(chain, tx_hash):
    """receipt 轮询候选链：chain 已知 → 单候选；未知 → hash 形状推断
    （0x+64hex → 已登记 EVM 链依序；base58 长串 → solana）。空 = 不可轮询。"""
    if chain and CHAINS.get(chain):
        return [chain]
    if isinstance(tx_hash, str) and tx_hash.startswith("0x") and len(tx_hash) == 66:
        return [name for name, cfg in CHAINS.items() if cfg["family"] == "evm"]
    if isinstance(tx_hash, str) and not tx_hash.startswith("0x") and len(tx_hash) >= 80:
        return ["solana"]
    return []


def _poll_receipt(run_id, leg_ref, chain, tx_hash, jsonrpc=None):
    """HK 直发链 RPC 轮询 receipt（HK-L3'）。线程内执行——不阻塞 mitm 事件循环。
    由 Store.claim_receipt_poll 闩锁启动（每腿至多一个，轮询腿锁定 hash）。
    EVM 必须 receipt.transactionHash≡本腿 hash 才采纳，存 blockHash/
    blockNumber；Sol 存 slot——不核身份的回执不算证据。块身份门：
    status=1+hash 匹配但缺 blockHash/blockNumber（或 Sol 缺 slot）= 证据不足——
    L3 留 null + incomplete='receipt-missing-block-identity'，不猜。
    jsonrpc 在 spawn 时捕获（缺省取启动瞬间的模块 _jsonrpc）——测试替换模块级
    _jsonrpc 不影响在飞 poller（否则旧 poller 会把调用记进新用例的桩）。"""
    _rpc = jsonrpc if jsonrpc is not None else _jsonrpc
    candidates = _receipt_candidates(chain, tx_hash)
    if not candidates:
        _settle_incomplete(leg_ref, "no-chain-for-receipt")
        return
    t0 = time.monotonic()
    deadline = t0 + RECEIPT_POLL_TIMEOUT_S
    while time.monotonic() < deadline:
        for cand in candidates:
            spec = CHAINS[cand]
            try:
                if spec["family"] == "evm":
                    r = _rpc(spec["rpc"], "eth_getTransactionReceipt", [tx_hash])
                    if (
                        r
                        and str(r.get("transactionHash") or "").lower()
                        != str(tx_hash).lower()
                    ):
                        r = None  # 交易身份不符的回执不采纳（继续轮询至超时）
                    if r and str(r.get("status", "0x0")).lower() == "0x1":
                        # 块身份门：status=1+hash 匹配但缺 blockHash/blockNumber
                        # = 证据不足（pending/畸形回执）——L3 留 null + incomplete，不猜
                        if not r.get("blockHash") or not r.get("blockNumber"):
                            _settle_incomplete(
                                leg_ref, "receipt-missing-block-identity"
                            )
                            return
                        _settle_receipt(
                            leg_ref,
                            {
                                "rpc": spec["rpc"],
                                "chain": cand,
                                "pollIntervalMs": int(RECEIPT_POLL_INTERVAL_S * 1000),
                                "status": 1,
                                "blockHash": r.get("blockHash"),
                                "blockNumber": r.get("blockNumber"),
                            },
                        )
                        return
                    if (
                        r
                        and str(r.get("status", "")).lower() == "0x0"
                        and r.get("blockNumber")
                    ):
                        _settle_incomplete(leg_ref, "receipt status=0 (reverted)")
                        return
                else:
                    r = _rpc(
                        spec["rpc"],
                        "getSignatureStatuses",
                        [[tx_hash], {"searchTransactionHistory": True}],
                    )
                    v = (r or {}).get("value") or [None]
                    st0 = v[0] if v else None
                    if (
                        st0
                        and st0.get("confirmationStatus") in ("confirmed", "finalized")
                        and st0.get("err") is None
                    ):
                        # 块身份门：缺 slot = 缺块身份——同律不猜
                        if st0.get("slot") is None:
                            _settle_incomplete(
                                leg_ref, "receipt-missing-block-identity"
                            )
                            return
                        _settle_receipt(
                            leg_ref,
                            {
                                "rpc": spec["rpc"],
                                "chain": cand,
                                "pollIntervalMs": int(RECEIPT_POLL_INTERVAL_S * 1000),
                                "status": 1,
                                "slot": st0.get("slot"),
                            },
                        )
                        return
                    if st0 and st0.get("err") is not None:
                        _settle_incomplete(leg_ref, "signature err")
                        return
            except Exception:
                pass
            if time.monotonic() >= deadline:
                break
        time.sleep(RECEIPT_POLL_INTERVAL_S)
    _settle_incomplete(leg_ref, "receipt poll timeout")


def _settle_receipt(leg_ref, receipt):
    promote = None
    with STORE.lock:
        if leg_ref.get("windowClosed") or leg_ref.get("_anchorVeto"):
            leg_ref["receiptPolling"] = False
            return
        # Chain proof belongs to the poller's original request. Resolve pending
        # binding/ownership before writing first receipt, including an earlier
        # request that became an alias while waiting for its own chain proof.
        STORE._receipt_backfill_rebind_locked(leg_ref, receipt)
        # formal 资格转移后，旧 formal 在飞 poller 的回写经
        # dup→formal 链重定向到新 formal——receipt 同成功一样永不留在 dup 腿
        leg = leg_ref
        hops = 0
        while leg.get("dup") and isinstance(leg.get("_dupOf"), dict) and hops < 8:
            leg = leg["_dupOf"]
            hops += 1
        if leg is not leg_ref:
            # 重定向即释放旧腿闩锁（闩锁在转移时已移交/归属落定腿）——否则旧腿
            # 闩锁永挂（无 receipt/无 incomplete）→ run 被 retention pin 死
            leg_ref["receiptPolling"] = False
        if leg.get("windowClosed"):
            # 回写前核验腿未被冻结——关窗后的回写丢弃并释放闩锁
            leg_ref["receiptPolling"] = False
            leg["receiptPolling"] = False
            return
        if leg.get("_anchorVeto"):
            # 被否决腿不收 receipt（observed 不增加），释放闩锁
            leg_ref["receiptPolling"] = False
            leg["receiptPolling"] = False
            return
        if leg.get("tReceiptMs") is not None:
            return  # 首次成功时刻只写一次
        leg["tReceiptMs"] = _mono_ms()
        leg["receipt"] = receipt
        if leg.get("anchorRole") == "send":
            promote = leg
    # v9（2026-09-19）：响应缺失的广播流（OKX 页扇出中止——tFirstRespMs 恒 null
    # 实证）——链上 receipt 落定本身就是受理证据（poller 只轮本腿 hash，身份
    # 数学精确且已过 status=1+块身份门）。send 腿在此经同一 confirm 路径
    # 晋升/绑槽/转移；响应在场时 confirm 已在 response() 先发生（幂等）。
    # 锁外调用：confirm_broadcast_identity 自持锁（锁内嵌套 = 自死锁）。
    if promote is not None:
        STORE.confirm_broadcast_identity(promote)


def _settle_incomplete(leg_ref, why):
    with STORE.lock:
        if leg_ref.get("dup") and leg_ref.get("chain") is None:
            # The earlier alias was polling to establish its own chain. A failed
            # probe cannot establish that chain, so it cannot fail the formal
            # request either. Keep its diagnostic and one-poller latch locally.
            if leg_ref.get("windowClosed"):
                leg_ref["receiptPolling"] = False
            elif leg_ref.get("tReceiptMs") is None and not leg_ref.get("incomplete"):
                leg_ref["incomplete"] = why
            return
        # 与 _settle_receipt 同律——dup 腿/旧 formal 的在飞 poller
        # 失败回写经 dup→formal 链重定向（receipt 路径归落定腿）。闩锁纪律：
        # 落定腿闩锁保持（「每腿至多一个 poller」——incomplete 落定后不起
        # 第二 poller）；leg_ref 重定向后释放。闩锁+incomplete/receipt 必居其一，
        # run 不被 retention pin 死。
        leg = leg_ref
        hops = 0
        while leg.get("dup") and isinstance(leg.get("_dupOf"), dict) and hops < 8:
            leg = leg["_dupOf"]
            hops += 1
        if leg is not leg_ref:
            leg_ref["receiptPolling"] = False
        if leg.get("windowClosed"):
            # 关窗后的回写丢弃并释放闩锁（含重定向目标）
            leg_ref["receiptPolling"] = False
            leg["receiptPolling"] = False
            return
        # 已有 receipt（在飞第二 poller 先落定）或已有 incomplete → 不覆盖
        if leg.get("tReceiptMs") is None and not leg.get("incomplete"):
            leg["incomplete"] = why


def _classify_order(host, path, method):
    for name, rule in RULES.items():
        try:
            if rule["match_order"](host, path, method):
                return name
        except Exception:
            continue
    return None


def _classify_ws(host):
    for name, rule in RULES.items():
        # exact host 或 "."+host 后缀边界——裸 endswith(w) 无点边界，会把
        # notnbstream.binance.com 这类同后缀异主机误分类进平台规则
        if any(host == w or host.endswith("." + w) for w in rule["ws_hosts"]):
            return name
    return None


# ── market tap（行情观测支线，与订单锚管线完全分离；2026-09-22 v10.1-mkt-tap1）──
# 只观测行情 WS host 的 server→client 文本帧：channel、帧长、首见 0x 地址（有界 FIFO）。
# 不存完整 payload/query/auth header；t 为 websocket_message 入口 mono（observer time，
# 同进程同钟，跨 host 直接可比）。不依赖开窗（mark），由控制面显式开关，默认关闭。
_MARKET_TAP_HOSTS = ("ws.gmgn.ai", "wsdexpri.okx.com")
_MARKET_ADDR_RE = re.compile(r"0x[0-9a-fA-F]{40}")
_MARKET_CH_RE = re.compile(r'"channel"\s*:\s*"([^"]{1,80})"')
_MARKET_TAP_MAX_ADDR = 60000


class _MarketTap:
    def __init__(self):
        self.lock = threading.Lock()
        self.enabled = False
        self.since = None
        self.frames = 0
        self.bytes = 0
        self.channels = {}
        self.addr_first = {}
        self._order = deque()

    def reset(self, enabled):
        with self.lock:
            self.enabled = enabled
            self.frames = 0
            self.bytes = 0
            self.channels = {}
            self.addr_first = {}
            self._order.clear()
            self.since = _wall_iso()

    def set_enabled(self, enabled):
        with self.lock:
            self.enabled = enabled

    def note(self, host, content, t_obs):
        # mitmproxy WS 消息 content 是 bytes——utf8 归一后抽取；无法解码则只计体量
        n = len(content) if isinstance(content, (str, bytes, bytearray)) else 0
        if isinstance(content, (bytes, bytearray)):
            try:
                content = bytes(content).decode("utf-8", "ignore")
            except Exception:
                content = None
        head = content[:20000] if isinstance(content, str) else None
        ch = None
        if head:
            m = _MARKET_CH_RE.search(head[:4000])
            ch = m.group(1) if m else None
        with self.lock:
            self.frames += 1
            self.bytes += n
            ck = f"{host}|{ch or '-'}"
            c = self.channels.get(ck)
            if c is None:
                self.channels[ck] = {"frames": 1, "bytes": n, "firstT": t_obs, "lastT": t_obs}
            else:
                c["frames"] += 1
                c["bytes"] += n
                c["lastT"] = t_obs
            if not head:
                return
            for a in set(_MARKET_ADDR_RE.findall(head)):
                # 按 host|channel|addr 记首见——同一地址可能先后到达多个频道，
                # host 级首见由消费端取 min 归并（同订阅面竞速需要频道级粒度）
                key = f"{host}|{ch or '-'}|{a.lower()}"
                if key in self.addr_first:
                    continue
                while len(self._order) >= _MARKET_TAP_MAX_ADDR:
                    self.addr_first.pop(self._order.popleft(), None)
                self._order.append(key)
                self.addr_first[key] = {"t": t_obs, "tw": _wall_iso(), "ch": ch}

    def snapshot(self):
        with self.lock:
            return {
                "enabled": self.enabled,
                "since": self.since,
                "frames": self.frames,
                "bytes": self.bytes,
                "channels": self.channels,
                "addrFirst": self.addr_first,
            }


MARKET_TAP = _MarketTap()


def _market_tap_note(flow, content, t_obs):
    host = flow.request.pretty_host
    if any(host == h or host.endswith("." + h) for h in _MARKET_TAP_HOSTS):
        MARKET_TAP.note(host, content, t_obs)


_EGRESS_REFRESH_EPOCH = 0


class SpeedexHkTiming:
    """mitmproxy addon hooks。"""

    seen_req = 0  # 调试计数：request 钩子触发总数（/health 透出）

    def load(self, loader):
        # v10.1（2026-09-19 c9tar 实证）：进程/热重载后 egress 缓存为空 → timeline
        # vantage 落 'unknown-proxy'，EU canonical 推送全批被 hk-clock-unverified 抑制。
        # 加载即起后台刷新线程：即时一次 + 每 TTL 续期（失败只保留旧缓存，不阻断）。
        # epoch 守卫——addon 热重载会再起一代线程，旧代发现 epoch 漂移即退出。
        global _EGRESS_REFRESH_EPOCH
        _EGRESS_REFRESH_EPOCH += 1
        epoch = _EGRESS_REFRESH_EPOCH

        def _refresh_loop():
            while _EGRESS_REFRESH_EPOCH == epoch:
                try:
                    _vantage(fetch=True)
                except Exception:
                    pass
                time.sleep(_EGRESS_TTL_S)

        threading.Thread(target=_refresh_loop, daemon=True).start()

    def request(self, flow):
        SpeedexHkTiming.seen_req += 1
        # hook 入口立即冻结 observer 时间戳——tOrderOut* 语义是
        # request-observed（代理观察到请求的时刻；不宣称物理离站），解析后只引用冻结值。
        t_obs = _mono_ms()
        window = STORE.capture_window(t_obs)
        if window is None:
            return
        t_wall = _wall_iso()
        t_wall_ms = int(time.time() * 1000)
        host = flow.request.pretty_host
        path = flow.request.path.split("?")[0]
        method = flow.request.method
        platform = _classify_order(host, path, method)
        raw_candidate = method == "POST" and (host in OKX_RPC_HOSTS or host in OKX_SOL_SEND_HOSTS)
        if not platform and not raw_candidate:
            return
        head_snapshots = HEADS.snapshot(window[0], t_obs)
        # body 解析在锁外完成；完成后复验入口捕获的不可变窗口身份再建腿。
        # close A/open B 或同 runId 重开都使该请求失效，不能借新窗口继续采纳。
        body = ""
        try:
            body = flow.request.get_text() or ""
        except Exception:
            pass
        raw = _okx_rpc_request(host, path, method, getattr(flow.request, "headers", {}), body) if raw_candidate else None
        if not platform and not raw:
            return
        if raw:
            platform = "okx"
            flow.metadata["speedex_rpc"] = raw
        request_ids = _okx_broadcast_request_ids(body) if platform == "okx" and not raw else {}
        chain = None
        try:
            chain = raw["chain"] if raw else RULES[platform]["extract_chain"](body)
        except Exception:
            chain = None
        ev = {
            "t": t_obs,
            "tw": t_wall,
            "twms": t_wall_ms,
            "kind": "order_req",
            "platform": platform,
            "host": host,
            "path": path,
            "flowId": flow.id,
            "bodySha": hashlib.sha256(body.encode()).hexdigest()[:16] if body else None,
            "headSnapshots": head_snapshots,
            "orderRequestSource": raw["source"] if raw else "platform-order",
            **request_ids,
        }
        # Binance：出站请求体即知 clientOrderId（比等 ack 更早）——
        # 建腿前抽取为订单身份去重输入（同单重复请求归入同一授权槽，不占新槽）
        if platform == "binance" and body:
            bj0 = _j(body) or {}
            if bj0.get("clientOrderId"):
                ev["clientOrderId"] = str(bj0["clientOrderId"])[:64]
        rid, leg = STORE.order_in_open_window(window, platform, chain, ev)
        if leg is None:
            return
        flow.metadata["speedex_leg"] = (leg, rid)  # 直接持引用
        if raw and raw.get("reqTxHash"):
            # OKX 广播请求体数学推导身份（keccak256(raw)/solana wire 首签名）——
            # 请求到达即知 tx 身份；响应侧 hash 仍是交叉验证（不一致 → 锚冲突否决律）。
            # 签名原文不持久化，只有推导 hash 入锚。
            STORE.update_anchor(leg, {"txHash": raw["reqTxHash"]})
            STORE.dedup_order_identity(leg)
            # v9（2026-09-19）：响应可能永不到达（OKX 页扇出后中止重复请求——
            # tFirstRespMs 恒 null，cjxmw 实证）——请求侧身份在场即起 receipt 轮询；
            # 链上 receipt 落定（status=1+hash 匹配+块身份）本身就是受理证据，
            # _settle_receipt 内经同一 confirm 路径晋升/绑槽。响应到达时
            # response() 的 confirm/poller 闩锁均幂等（每腿至多一个 poller）。
            h = STORE.claim_receipt_poll(leg)
            if h:
                threading.Thread(
                    target=_poll_receipt,
                    args=(rid, leg, leg.get("chain"), h, _jsonrpc),
                    daemon=True,
                ).start()
        if request_ids:
            STORE.update_anchor(leg, request_ids)
            STORE.dedup_order_identity(leg)
            self._drain_ws_buffer(rid, platform)
        # Binance：请求体 clientOrderId 写入请求侧独立锚（成功判定只认锚；
        # 建腿前的去重抽取见上方 ev 构造）
        if platform == "binance" and body:
            bj = _j(body) or {}
            kv = {}
            if bj.get("clientOrderId"):
                kv["clientOrderId"] = bj["clientOrderId"]
            if bj.get("chain"):
                c = CHAIN_NAME_STR.get(str(bj["chain"]).lower())
                if c:
                    kv["chain"] = c
            if kv:
                STORE.update_anchor(leg, kv)

    def response(self, flow):
        t_obs = _mono_ms()  # hook 入口冻结（同 request 侧）
        meta = flow.metadata.get("speedex_leg")
        if not meta:
            return
        leg, rid = meta
        with STORE.lock:
            if not leg.get("windowClosed") and leg.get("tFirstRespMs") is None:
                leg["tFirstRespMs"] = t_obs
        body = ""
        try:
            body = flow.response.get_text() or ""
        except Exception:
            pass
        rule = RULES[leg["platform"]]
        try:
            raw = flow.metadata.get("speedex_rpc")
            ids = _okx_rpc_response(raw, flow.response) if raw else (rule["extract_ids"](body) or {})
        except Exception:
            # 解析崩溃的响应不能充当受理证据（raw RPC 流）：拒绝晋升；锚写入不受影响
            # （_reject 非身份键，update_anchor 键白名单忽略）
            ids = {"_reject": "parse-error"} if flow.metadata.get("speedex_rpc") else {}
        # 响应抽取的 id/chain 是独立锚（成功判定/hash 采纳只认锚）：
        # 结构化 schema-known 抽取——orderId 与 clientOrderId 分键写锚；旧整-body 正则
        # 只算诊断（legacyRegexDiverged），永不写锚。
        STORE.update_anchor(
            leg,
            {k: ids.get(k) for k in ("txHash", "orderId", "clientOrderId", "chain")},
        )
        try:
            STORE.note_anchor_diag(
                leg, ids, _legacy_regex_ids_diag(body), ids.get("_ambiguous")
            )
        except Exception:
            pass
        # 响应建立订单身份后正向去重——同 run 同单已绑槽 → 本腿归入
        # 同一授权槽（释放占用槽、标 dup 诊断）；身份缺失/冲突不猜。
        # 晋升门（2026-09-19 v8）：响应 _reject（非 200/超尺寸/非 JSON-RPC/id 不配）
        # 不晋升——传输/流完整性失败时请求侧推导身份不能单独证明受理；JSON-RPC
        # error（already known/nonce too low 等，id 匹配）= 端点已处理，晋升照常。
        if flow.metadata.get("speedex_rpc") and not ids.get("_reject"):
            STORE.confirm_broadcast_identity(leg)
        STORE.dedup_order_identity(leg)
        # 闩锁启动 poller——每腿至多一个，轮询腿锁定 hash
        h = STORE.claim_receipt_poll(leg)
        if h:
            threading.Thread(
                target=_poll_receipt,
                args=(rid, leg, leg.get("chain"), h, _jsonrpc),
                daemon=True,
            ).start()
        # 响应建立锚后重审该平台本 run 的早到缓冲帧——与实时帧
        # 同一管线（路由/冲突/跨腿去重/新鲜度门），成功时刻用缓冲的单调源时间
        self._drain_ws_buffer(rid, leg["platform"])

    def websocket_message(self, flow):
        # 内存有界：mitmproxy 会把每条 WS 消息无界累积到 flow.websocket.messages，
        # 长连行情流在小内存 VM 上必然 OOM（HK-1 已两次 oom-kill）。无论是否处理，
        # 每条消息过后只保留最新一条——本钩子与 mitmproxy 内部都只读 [-1]。
        t_obs = _mono_ms()  # 入口冻结——decode/判定耗时不计入 tSuccessPush
        try:
            msgs = flow.websocket.messages
            msg = msgs[-1]
        except Exception:
            return
        if not msg.from_client and MARKET_TAP.enabled:
            # 行情支线：入口 mono 时刻即打戳，独立于订单开窗管线
            try:
                _market_tap_note(flow, msg.content, t_obs)
            except Exception:
                pass
        try:
            self._handle_ws_message(flow, msg, t_obs)
        finally:
            try:
                del msgs[:-1]
            except Exception:
                pass

    def _handle_ws_message(self, flow, msg, t_obs):
        if not STORE.any_open():
            return
        host = flow.request.pretty_host
        # v10：链域通道（www.okx.com /fullnode|/nodeone）——成功通知到达时刻（HK 钟）
        # 窄提取，与平台成功判定管线完全分离（不进 ws_entries/ws_buffer/成功资格门）。
        if host in _OKX_CHAIN_WS_HOSTS and not msg.from_client:
            path = (flow.request.path or "").split("?")[0]
            m = _OKX_CHAIN_WS_PATH.match(path)
            if m:
                channel = m.group(1).lower()
                chain = {"sol": "solana"}.get(m.group(2).lower(), m.group(2).lower())
                txv = _okx_chain_frame_identity(msg.content, channel, chain)
                if txv:
                    STORE.note_channel_arrival(chain, txv, channel,
                        "signatureNotification" if chain == "solana" else "eth_subscription", t_obs)
            return
        platform = _classify_ws(host)
        if not platform:
            return
        payload = msg.content
        tw_obs = _wall_iso()  # 与 t_obs 同刻的墙钟——缓冲单元保留源时间
        if msg.from_client:
            # FOMO：页面出站 subscribe 帧带 filters.id——把 ws 通道与 requestId 绑定
            # （出站页面数据，与请求/响应同为独立锚）
            if platform == "fomo":
                obj = _decode_ws(payload)
                if isinstance(obj, dict) and obj.get("type") == "subscribe":
                    fid = ((obj.get("filters") or {}).get("id")) or None
                    if fid:
                        leg = STORE.bind_fomo_subscribe(fid)
                        if leg is not None:
                            # subscribe 也是锚源——建立后重审早到缓冲帧
                            self._drain_ws_buffer(leg.get("runId"), "fomo")
            return
        # WS 帧可先见 hash（OKX sol / Binance PENDING / GMGN hash-ack）——先解析再判成功。
        # 逐单元处理——okx/gmgn 一帧多 entry 逐 entry 独立（身份/状态/
        # 链只取同一 entry；目标在第二 entry 也照常关联），其余平台整帧一单元。
        try:
            units = self._ws_units(platform, payload)
        except Exception:
            units = None
        # ws_ids 返回 None = 整帧拒收（binance 同 entry 多 hash 字段归一
        # 不一致——帧自相矛盾，ids/成功一概不认）
        if units is None:
            with STORE.lock:
                STORE.diag["wsFrameRejected"] += 1
                for rid in STORE._open_run_ids_locked(time.monotonic())[:1]:
                    STORE.run_diag[rid]["wsFrameRejected"] += 1
            return
        for unit in units:
            try:
                self._adopt_ws_unit(
                    platform,
                    host,
                    payload,
                    unit["ids"],
                    unit.get("success"),
                    t_obs,
                    tw_obs,
                )
            except Exception:
                pass

    def _ws_units(self, platform, payload):
        """入站帧 → 处理单元列表 [{ids, success}…]。规则带
        ws_entries（okx/gmgn）时逐 entry 产出（success = 同 entry 状态判定）；
        否则整帧一单元（success=None——采纳时再以 ws_success(payload, 锚视图) 判定）。
        无身份键（orderId/clientOrderId/txHash）的单元不路由（行情
        噪声静默跳过）。返回 None = 整帧拒收（ws_ids 的归一不一致语义）。"""
        rule = RULES[platform]
        entries_fn = rule.get("ws_entries")
        if entries_fn:
            units = []
            for e in entries_fn(payload) or []:
                ids = e.get("ids") or {}
                if any(ids.get(k) is not None for k in _IDENTITY_KEYS):
                    units.append({"ids": ids, "success": bool(e.get("success"))})
            return units
        ids = rule.get("ws_ids", lambda p: {})(payload)
        if ids is None:
            return None
        if not any(ids.get(k) is not None for k in _IDENTITY_KEYS):
            return []
        return [{"ids": ids, "success": None}]

    def _adopt_ws_unit(
        self,
        platform,
        host,
        payload,
        ids,
        success_flag,
        t_obs,
        tw_obs,
        allow_buffer=True,
        rebuffered=False,
    ):
        """单单元（整帧或单 entry）路由+采纳。零命中且本开窗该平台
        还有无锚腿（响应未到、锚待定）→ 缓冲（保留单调源时间/窗口 epoch/方向），
        锚建立后由 _drain_ws_buffer 重审——与实时帧同一管线同一门（新鲜度/锚正向/
        冲突/跨腿去重全保留）；关窗（freeze 弃缓冲）、重放（首写冻结 +
        id_claimed）、冲突（路由层终态）均不采纳。
        按锚身份路由到目标腿——候选为所有开窗腿（不只平台
        最新腿）；dup 腿命中归到 formal 腿；冲突/否决 → 不归属并由路由层计 diag。"""
        leg, route = STORE.route_ws_frame(platform, ids, defer_nomatch=allow_buffer)
        if not leg:
            if route == "no-leg-match" and allow_buffer:
                if not STORE.buffer_ws_unit(
                    platform,
                    host,
                    payload,
                    ids,
                    success_flag,
                    t_obs,
                    tw_obs,
                    count_diag=not rebuffered,
                ):
                    STORE.note_frame_diag(
                        "wsFrameNoLegMatch"
                    )  # 无锚待定腿/超预算——零命中即终态
            return
        t_order = leg.get("tOrderOutMs")
        if (
            isinstance(t_order, (int, float))
            and isinstance(t_obs, (int, float))
            and t_obs < t_order
        ):
            # 新鲜性门：信号源时间先于该腿的下单请求 = 不可能是本单的
            # 回应（陈旧/重放疑似——缓冲帧尤其要防「先到帧撞上后建腿的未来锚」）。
            # 不采纳；与零命中同路回缓冲/计数（该腿永不可能成为本帧的合法目标）。
            if allow_buffer:
                if not STORE.buffer_ws_unit(
                    platform,
                    host,
                    payload,
                    ids,
                    success_flag,
                    t_obs,
                    tw_obs,
                    count_diag=not rebuffered,
                ):
                    STORE.note_frame_diag("wsFrameNoLegMatch")
            return
        try:
            # 整帧重放甄别（如 padre 的跨批 FILLED 重放）——陈旧帧 ids/成功一概不认
            fresh_fn = RULES[platform].get("ws_frame_fresh")
            if fresh_fn and not fresh_fn(payload, leg, ids):
                return
            # 强关联门：陈旧/跨批重放帧不得把别轮的 hash 挂到本轮腿，
            # 也不得触发对不存在 tx 的 receipt 轮询假成功：
            # ①帧自带 orderId/clientOrderId/txHash 与腿已知值冲突 → 整帧弃用；
            # ②成功判定与 txHash 采纳只认「请求/响应独立锚」的正向匹配——帧自带 id
            #   先写入腿再自证关联不算数（padre 另有 ws_frame_fresh 前置门：同 DONE
            #   节点合法 hash 与锚正向匹配 + 同节点新鲜源时间，不过则整帧弃用）；
            # ③同 run 内已被其他腿占用的 id/hash 不再挂载（跨腿去重；chain 是属性
            #   不是身份，不参与去重）。
            conflict = any(
                leg.get(k)
                and ids.get(k)
                and _ident_norm(k, ids[k]) != _ident_norm(k, leg[k])
                for k in _IDENTITY_KEYS
            )
            if conflict:
                with STORE.lock:
                    STORE.diag["wsFrameLegConflict"] += 1
                    if leg.get("runId"):
                        STORE.run_diag[leg["runId"]]["wsFrameLegConflict"] += 1
                return
            anchor = leg.get("_anchor") or {}
            # 正向匹配 = 独立锚任一身份键相等（含响应 hash 锚——OKX/GMGN 成功
            # 本就接受 txHash 相等；路由到此的腿已由 route_ws_frame 保证正向命中）
            positive = any(
                anchor.get(k)
                and ids.get(k)
                and _ident_norm(k, ids[k]) == _ident_norm(k, anchor[k])
                for k in _IDENTITY_KEYS
            )
            rid_leg = leg.get("runId")
            kv = {}
            for k, v in ids.items():
                if (
                    k not in ("txHash", "orderId", "clientOrderId", "chain")
                    or v is None
                ):
                    continue
                if k == "txHash" and not positive:
                    continue
                if (
                    k in ("txHash", "orderId", "clientOrderId")
                    and rid_leg
                    and STORE.id_claimed(rid_leg, k, v, except_leg=leg)
                ):
                    continue
                kv[k] = v
            if kv:
                STORE.update_leg(leg, kv)
            # HTTP/WS 入口共用闩锁——每腿至多一个 poller，轮询腿锁定 hash
            h = STORE.claim_receipt_poll(leg)
            if h:
                threading.Thread(
                    target=_poll_receipt,
                    args=(leg.get("runId"), leg, leg.get("chain"), h, _jsonrpc),
                    daemon=True,
                ).start()
        except Exception:
            pass
        with STORE.lock:
            if leg.get("tSuccessPushMs") is not None or leg.get("windowClosed"):
                return
        try:
            # 逐 entry 平台（ws_entries）的成功 = 同 entry 状态（路由已保证锚
            # 正向）；整帧平台沿用 ws_success(payload, 锚视图) 判定
            ok = (
                success_flag
                if success_flag is not None
                else RULES[platform]["ws_success"](payload, _AnchorView(leg))
            )
        except Exception:
            ok = False
        if not ok:
            return
        with STORE.lock:
            if (
                leg.get("tSuccessPushMs") is not None
                or leg.get("windowClosed")
                or leg.get("_anchorVeto")
            ):
                # 被否决腿不写成功（route_ws_frame 已排除——此为纵深守卫）
                return
            leg["tSuccessPushMs"] = t_obs
            leg["wsSha"] = hashlib.sha256(
                payload if isinstance(payload, bytes) else str(payload).encode()
            ).hexdigest()[:16]
            # Bounded, replayable success witness. Never retain the business frame.
            obj = _decode_ws(payload)
            tx = _transaction_id_value(ids.get("txHash"))
            digest = hashlib.sha256((tx.lower() if tx and tx.startswith("0x") else tx or "").encode()).hexdigest()
            channel = ((obj.get("arg") or {}).get("channel") if platform == "okx" else obj.get("channel")) if isinstance(obj, dict) else None
            if platform in ("okx", "gmgn") and tx:
                leg["successEvidence"] = {
                    "version": 1, "platform": platform, "channel": channel,
                    "status": "1" if platform == "okx" else "successful",
                    "txHash": tx, "txDigest": "-".join(digest[i:i+16] for i in range(0, 64, 16)),
                    "observedAtMs": t_obs, "frameSha": leg["wsSha"],
                }
            # 记录成功帧正向命中的锚键（evidence.successAnchorKeys——关联依据可审计）
            anchor_now = leg.get("_anchor") or {}
            leg["successAnchorKeys"] = [
                k
                for k in _IDENTITY_KEYS
                if anchor_now.get(k) is not None
                and ids.get(k) is not None
                and _ident_norm(k, ids[k]) == _ident_norm(k, anchor_now[k])
            ]
            if leg.get("successEvidence"):
                leg["successEvidence"]["matchedKeys"] = list(leg["successAnchorKeys"])
            # preview（fomo /swaps/v2，Relay success requestId≡锚 relaySwapId）/
            # sign（padre sign_raw_payload，同 DONE 节点合法 hash+独立锚正向匹配）腿
            # 被成功帧正向匹配 = 执行证据——原子晋升 order 并补授权槽绑定；
            # 预览/签名腿自身永不单独产成功
            if leg.get("anchorRole") in ("preview", "sign"):
                STORE._promote_leg_locked(leg)
        # 事件记进腿自己的 run（不 open_run_ids()[0] 取最旧窗）
        STORE.add_event(
            leg["runId"],
            {
                "t": t_obs,
                "tw": tw_obs,
                "kind": "ws_success",
                "platform": platform,
                "host": host,
                "sha": leg["wsSha"],
            },
        )

    def _drain_ws_buffer(self, rid, platform):
        """响应/subscribe 建立锚后，重审本 run 该平台的早到缓冲
        单元——与实时帧同一管线（freshness/anchor/conflict/dedup 全过），成功时刻
        用缓冲时的单调源时间；仍零命中且锚待定的回缓冲等下一锚源，关窗即弃
        （freeze 按零命中计数）。每单元最多每锚源事件重审一次（单批取出——
        回缓冲的单元不在本轮重复取）。"""
        if not rid:
            return
        for ent in STORE.take_ws_buffer(rid, platform):
            try:
                self._adopt_ws_unit(
                    ent["platform"],
                    ent["host"],
                    ent["payload"],
                    ent["ids"],
                    ent.get("success"),
                    ent["t"],
                    ent["tw"],
                    rebuffered=True,
                )
            except Exception:
                pass

    def done(self):
        """mitmproxy 热重载/退出生命周期——本实例的控制面随 done 关停
        （shutdown+join），不留旧控制线程占控制面端口（默认 8072）。"""
        with HEAD_CONTROL_LOCK:
            HEADS.close()
        _stop_ctrl_if_owner()


ADDON = SpeedexHkTiming()
addons = [ADDON]

# ── 控制面（loopback only）──


class _Ctrl(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        try:
            self.send_response(code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # 只吞「明确断连」（EPIPE/ECONNRESET——EU 侧经 SSH 隧道拉取时超时/断开，
            # 响应注定送达不到）：默认 socketserver handle_error 会把完整 traceback
            # 打进 journal（诊断最小化纪律——旧版 _send 的 BrokenPipeError traceback
            # 即此源）。其余异常照常传播（fail loud，不吞）。
            return

    def _body(self):
        try:
            n = int(self.headers.get("content-length") or 0)
            return json.loads(self.rfile.read(min(n, 65536)) or b"{}")
        except Exception:
            return {}

    def do_POST(self):
        if self.path == "/mark":
            b = self._body()
            rid = str(b.get("runId") or "").strip()
            if not rid:
                return self._send(400, {"error": "runId required"})
            # 可选 manifest（{"rounds": n} 或 {"legs": [{platform, chain, rounds}…]}）——
            # 授权轮次登记；无 manifest 行为同今（向后兼容）
            manifest = (
                b.get("manifest") if isinstance(b.get("manifest"), dict) else None
            )
            with HEAD_CONTROL_LOCK:
                STORE.mark_open(rid, str(b.get("note") or "")[:200], manifest)
                _start_heads(rid, manifest)
            _publish_known_runs()  # 跨实例注册表交接清单
            return self._send(200, {"ok": True, "runId": rid})
        if self.path == "/mark/close":
            b = self._body()
            rid = str(b.get("runId") or "").strip()
            with HEAD_CONTROL_LOCK:
                STORE.mark_close(rid)
                if HEADS.run_id == rid:
                    HEADS.close()
            _publish_known_runs()  # 同律
            return self._send(200, {"ok": True})
        if self.path == "/market-tap":
            b = self._body()
            if bool(b.get("enabled")):
                MARKET_TAP.reset(True)  # 开启即清零——窗口边界明确
            else:
                MARKET_TAP.set_enabled(False)  # 停止后数据仍可 GET
            return self._send(200, {"ok": True, "enabled": MARKET_TAP.enabled, "since": MARKET_TAP.since})
        self._send(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/health":
            reg = _ctrl_registry()
            with STORE.lock:
                n_marks = len(STORE.marks)
                n_legs = sum(len(v) for v in STORE.legs.values())
                diag = dict(STORE.diag)
            body = {
                "ok": True,
                "marks": n_marks,
                "legs": n_legs,
                "seenReq": SpeedexHkTiming.seen_req,
                "at": _wall_iso(),
                "diag": {
                    **diag,
                    "headPoll": HEADS.diag_snapshot(),
                },  # 关窗后拒绝的下单请求计数等诊断 + 链头轮询可观测（head-v4）
                # build manifest
                "addonVersion": ADDON_VERSION,
                "instanceId": INSTANCE_ID,
                "pythonVersion": platform.python_version(),
                "mitmproxyVersion": _mitmproxy_version(),
                # 采集/解密身份（实际生效 allow_hosts + 来源 + 控制面
                # 端口）——与 timeline 的 _collection_identity().captureIdentity 同源
                "captureIdentity": _capture_identity(),
            }
            # 接管自旧实例且其已知 run 未交接——如实暴露丢弃事实
            tk = reg.get("takeover")
            if isinstance(tk, dict) and tk.get("droppedRuns"):
                body["takeover"] = {
                    "fromInstanceId": tk.get("fromInstanceId"),
                    "droppedRuns": len(tk["droppedRuns"]),
                    "runIds": list(tk["droppedRuns"])[:50],
                    "at": tk.get("at"),
                }
            # 热重载分裂如实报错——bind 失败 / 控制面非当前采集实例时不
            # 假装 200（旧控制面+新采集面 = mark 更新旧 Store、采集为零）
            if reg.get("bindError"):
                body["ok"] = False
                body["error"] = f"ctrl reload bind failed: {reg['bindError']}"
                return self._send(503, body)
            if (reg.get("instanceId") and reg["instanceId"] != INSTANCE_ID) or (
                reg.get("loadedInstanceId") and reg["loadedInstanceId"] != INSTANCE_ID
            ):
                body["ok"] = False
                body["error"] = "control/collection split: 热重载后旧控制面仍在服务"
                return self._send(503, body)
            return self._send(200, body)
        if self.path == "/market-tap":
            return self._send(200, {"ok": True, "marketTap": MARKET_TAP.snapshot()})
        if self.path == "/egress":
            return self._send(200, {"ok": True, "egress": get_egress()})
        if self.path == "/latency":
            return self._send(200, {"ok": True, "latency": get_latency()})
        if self.path.startswith("/timeline/"):
            rest = self.path[len("/timeline/") :]
            rid, _, qs = rest.partition("?")
            rid = rid.strip("/")
            if not rid:
                return self._send(400, {"error": "runId required"})
            full = "full=1" in qs
            return self._send(200, STORE.timeline(rid, full=full))
        self._send(404, {"error": "not found"})


def _mitmproxy_version():
    try:
        return importlib.metadata.version("mitmproxy")
    except Exception:
        return None


# 跨模块实例共享的控制面注册表（进程内热重载时新旧模块都可见）——
# mitmproxy 重载 -s 脚本会新建模块对象，旧模块对象仍在；registry 挂 sys.modules
# 固定键下，接管/分裂检测都经它。
_CTRL_REG_KEY = "_speedex_hk_timing_ctrl_registry"


def _ctrl_registry():
    reg = sys.modules.get(_CTRL_REG_KEY)
    if reg is None:
        # knownRuns：当前服务实例已知的 runId 清单（/mark、/mark/close 时发布）；
        # takeover：本实例接管时捕获的「旧实例已知 run 丢弃」事实
        reg = {
            "server": None,
            "thread": None,
            "serving": False,
            "instanceId": None,
            "loadedInstanceId": None,
            "bindError": None,
            "knownRuns": None,
            "takeover": None,
        }
        sys.modules[_CTRL_REG_KEY] = reg
    return reg


def _publish_known_runs():
    """控制面事件时把本实例已知 runId 清单发布到跨实例注册表——热重载后新
    实例据此区分「旧实例曾存在但本地没有」的 run（timeline 标 droppedInReload）
    与「从未见过」的 run（空 timeline）。"""
    try:
        reg = _ctrl_registry()
        with STORE.lock:
            runs = sorted(STORE.marks.keys())
        reg["knownRuns"] = {
            "instanceId": INSTANCE_ID,
            "runs": runs[-GONE_IDS_MAX:],
            "at": _wall_iso(),
        }
    except Exception:
        pass


def _dropped_run_ids():
    """本实例接管时旧实例遗留的已知 runId 集合（本地 Store 无数据的 dropped）。"""
    reg = sys.modules.get(_CTRL_REG_KEY)
    tk = (reg or {}).get("takeover") if isinstance(reg, dict) else None
    if isinstance(tk, dict):
        return set(tk.get("droppedRuns") or [])
    return set()


def _stop_ctrl_if_owner():
    """关停本实例的控制面（done hook / 接管前置）。只动自己 bind 的 server。"""
    reg = _ctrl_registry()
    if reg.get("instanceId") != INSTANCE_ID:
        return False
    srv = reg.get("server")
    reg["server"] = None
    reg["instanceId"] = None
    if srv is not None and reg.get("serving"):
        reg["serving"] = False
        try:
            srv.shutdown()
            srv.server_close()
        except Exception:
            pass
        th = reg.get("thread")
        if th:
            th.join(timeout=5)
    reg["thread"] = None
    return True


def _start_ctrl():
    # 热重载接管——mitmproxy 在 -s 脚本变更时进程内热重载，旧 ctrl 线程仍持
    # 控制面端口（默认 8072，SPEEDEX_HK_CTRL_PORT 可覆盖）；软绑定失败仅记录 =
    # 旧控制面+新采集面分裂（mark 更新旧 Store、采集为零但 health 200 的静默失能）。
    # 故：先停旧实例控制面（shutdown/join）再 bind；bind 失败记 registry，
    # /health 如实 503。
    reg = _ctrl_registry()
    reg["loadedInstanceId"] = INSTANCE_ID
    old = reg.get("server")
    if old is not None:
        # 接管事实留存——旧实例已知 run 对新实例 Store 而言全部丢失（Store
        # 不随重载交接）；timeline 对这些 runId 标 droppedInReload，/health 暴露计数
        prev = reg.get("knownRuns")
        if (
            isinstance(prev, dict)
            and prev.get("instanceId")
            and prev["instanceId"] != INSTANCE_ID
            and prev.get("runs")
        ):
            reg["takeover"] = {
                "fromInstanceId": prev["instanceId"],
                "droppedRuns": list(prev["runs"]),
                "at": _wall_iso(),
            }
        if reg.get("serving"):
            reg["serving"] = False
            try:
                old.shutdown()
                old.server_close()
            except Exception:
                pass
            th = reg.get("thread")
            if th:
                th.join(timeout=5)
        reg["server"] = None
        reg["thread"] = None
        reg["instanceId"] = None
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", _ctrl_port()), _Ctrl)
    except OSError as e:
        reg["bindError"] = f"{type(e).__name__}: {e}"
        print(
            f"[speedex_hk_timing] ctrl bind :{_ctrl_port()} failed（/health 将如实报错）: {e}",
            file=sys.stderr,
        )
        return
    reg.update(server=srv, instanceId=INSTANCE_ID, bindError=None, serving=True)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    reg["thread"] = th
    th.start()


_start_ctrl()
