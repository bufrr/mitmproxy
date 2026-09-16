"""speedex_hk_timing addon 离线单测——无需 mitmproxy/网络（flow 用合成桩，
_jsonrpc 轮询打桩）。帧形状全部来自仓库夹具/文档（tests/classify-ws.test.mjs、
fomo-runner.test.mjs、binance-ws.test.mjs、runner-core.mjs 引用的生产样本）。
运行：python3 deploy/hk-proxy/test_speedex_hk_timing.py（或 verify 的 node 包装）。"""

# Socket-boundary fixture: never bind or contact host production/tunnel ports.
# Keep requested port semantics (8072/18072) for resolver/rebind assertions,
# while each real test HTTP server binds an OS-assigned port. Reloaded addon
# modules see the same fixture; all servers are owned by this test process.
import atexit
import http.server
import json
import os
import re
import sys
import time
import types
import unittest
import urllib.parse
import urllib.request
from pathlib import Path

_REAL_HTTP_SERVER = http.server.ThreadingHTTPServer
_TEST_SERVERS = {}
_ALL_TEST_SERVERS = []


class IsolatedHTTPServer(_REAL_HTTP_SERVER):
    def __init__(self, address, handler, *args, **kwargs):
        host, requested_port = address
        if host != "127.0.0.1":
            raise AssertionError("test control server must remain loopback")
        super().__init__((host, 0), handler, *args, **kwargs)
        _TEST_SERVERS[requested_port] = self
        _ALL_TEST_SERVERS.append(self)


def _close_test_servers():
    for server in _ALL_TEST_SERVERS:
        server.server_close()
    http.server.ThreadingHTTPServer = _REAL_HTTP_SERVER


http.server.ThreadingHTTPServer = IsolatedHTTPServer
atexit.register(_close_test_servers)

sys.path.insert(0, str(Path(__file__).parent))
import speedex_hk_timing as A  # noqa: E402

# Control-plane tests never start live head transports. Collector is exercised below.
A._start_heads = lambda *_: None


class FakeWSMsg:
    def __init__(self, content, from_client=False):
        self.content = content
        self.from_client = from_client


class FakeReq:
    def __init__(self, host, path, method="POST", body=""):
        self.pretty_host = host
        self.path = path
        self.method = method
        self._body = body

    def get_text(self):
        return self._body


class FakeResp:
    def __init__(self, body=""):
        self._body = body

    def get_text(self):
        return self._body


class FakeFlow:
    def __init__(self, host, path, method="POST", req_body="", resp_body=""):
        self.request = FakeReq(host, path, method, req_body)
        self.response = FakeResp(resp_body)
        self.metadata = {}
        self.id = "flow-" + str(abs(hash((host, path, time.monotonic_ns()))) % 10**8)
        self.websocket = None


def ws_frame(host, payload, from_client=False):
    f = FakeFlow(host, "/ws")
    f.websocket = types.SimpleNamespace(messages=[FakeWSMsg(payload, from_client)])
    return f


def fresh():
    A.STORE = A.Store()
    return A.STORE


# r8（PX-01）：Turnkey `POST /public/v1/submit/sign_raw_payload` 的**真实响应形状**——
# ActivityResponse 信封（docs.turnkey.com api-reference sign-raw-payload；全部值合成）：
# activity.id 是签名活动 UUID（不是订单号）、intent.payload 是签名摘要（不是 tx hash）、
# result.signRawPayloadResult 只有 r/s/v 签名分量——**没有任何订单/交易身份键**。
TURNKEY_ACTIVITY_ID = "3f1c9e2a-7b4d-4c0e-9a11-0000000000d1"


def turnkey_sign_response(now_ms=None):
    now_ms = now_ms or int(time.time() * 1000)
    return json.dumps(
        {
            "activity": {
                "id": TURNKEY_ACTIVITY_ID,
                "organizationId": "9b2d7c1e-1111-4222-8333-0000000000d2",
                "status": "ACTIVITY_STATUS_COMPLETED",
                "type": "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2",
                "timestampMs": str(now_ms),
                "intent": {
                    "signRawPayloadIntentV2": {
                        "signWith": "0x" + "ab" * 20,
                        "payload": "d1" * 32,  # 签名摘要（64 hex），不是交易 hash
                        "encoding": "PAYLOAD_ENCODING_HEXADECIMAL",
                        "hashFunction": "HASH_FUNCTION_NO_OP",
                    }
                },
                "result": {
                    "signRawPayloadResult": {"r": "1a" * 32, "s": "2b" * 32, "v": "01"}
                },
                "votes": [],
                "fingerprint": "sha256:" + "00" * 8,
                "canApprove": False,
                "canReject": False,
                "createdAt": {"seconds": str(now_ms // 1000), "nanos": "0"},
                "updatedAt": {"seconds": str(now_ms // 1000), "nanos": "0"},
            }
        }
    )


def padre_sign_leg(run, anchor_hash=None, req_body="{}", manifest=None):
    """Turnkey sign 请求 + **真实形状**响应建 sign 腿（生产路径：响应不提供任何锚）。
    anchor_hash 非空时**直接注入**独立锚（A.STORE.update_anchor）——模拟未来 Padre 自有
    下单通道（client→_multiplex 请求帧）提供的锚源，生产今日**不存在**；只为保住
    DONE/新鲜度/同节点/锚门（k2x54 重放等事故边界）的覆盖，不代表 Padre 今日可观测。"""
    if manifest is not None:
        A.STORE.mark_open(run, manifest=manifest)
    else:
        A.STORE.mark_open(run)
    f = FakeFlow(
        "api.turnkey.com",
        "/public/v1/submit/sign_raw_payload",
        "POST",
        req_body,
        resp_body=turnkey_sign_response(),
    )
    A.addons[0].request(f)
    A.addons[0].response(f)
    leg = A.STORE.legs[run][-1]
    if anchor_hash:
        A.STORE.update_anchor(leg, {"txHash": anchor_hash})
    return leg


class TestHkTiming(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_mark_shape(self):
        A.STORE.mark_open("r1", "t")
        self.assertTrue(A.STORE.any_open())
        A.STORE.mark_close("r1")
        self.assertFalse(A.STORE.any_open())

    def test_okx_full_leg(self):
        """OKX：broadcast 响应结构化抽 hash/orderId（键白名单，r8）；dex-swap-order-info status "1" 成功帧。"""
        A.STORE.mark_open("run-okx")
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": "0x" + "ab" * 32,
        }
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
                resp_body=json.dumps(
                    {
                        "code": "0",
                        "data": {
                            "transactionHash": "0x" + "ab" * 32,
                            "orderId": "ord1",
                        },
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            time.sleep(0.05)
            # 创建帧不算成功
            A.addons[0].websocket_message(
                ws_frame(
                    "wsdexpri.okx.com",
                    json.dumps(
                        {
                            "arg": {"channel": "dex-swap-order-info"},
                            "data": {"dexData": {"status": "0", "orderId": "ord1"}},
                        }
                    ),
                )
            )
            self.assertIsNone(A.STORE.timeline("run-okx")["legs"][0]["hkL1aMs"])
            # 成功帧
            A.addons[0].websocket_message(
                ws_frame(
                    "wsdexpri.okx.com",
                    json.dumps(
                        {
                            "arg": {"channel": "dex-swap-order-info"},
                            "data": {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "ord1",
                                    "transactionHash": "0x" + "ab" * 32,
                                }
                            },
                        }
                    ),
                )
            )
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-okx")["legs"][0]
        self.assertEqual(leg["chain"], "bsc")
        self.assertIsNotNone(leg["hkL1aMs"])
        self.assertIsNotNone(leg["hkL3Ms"])
        self.assertEqual(leg["txHash"], "0xabababab…ababab")
        self.assertIsNone(leg["incomplete"])
        # R24：receipt 证据存块身份
        rc = leg["evidence"]["receipt"]
        self.assertEqual(rc["blockHash"], "0x" + "99" * 32)
        self.assertEqual(rc["blockNumber"], "0x1")

    def test_gmgn_processed_order_info(self):
        """GMGN：swap_batch_order 下单；tg_processed_order_info st=successful si=buy。"""
        A.STORE.mark_open("run-g")
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "value": [
                {"confirmationStatus": "confirmed", "err": None, "slot": 287654321}
            ]
        }
        sig = "5" * 88
        try:
            f = FakeFlow(
                "gmgn.ai",
                "/tapi/v1/swap_batch_order",
                "POST",
                resp_body=json.dumps({"data": {"hash": sig, "orderId": "g1"}}),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.gmgn.ai",
                    json.dumps(
                        {
                            "channel": "tg_processed_order_info",
                            "data": [
                                {
                                    "h": sig,
                                    "oi": "g1",
                                    "st": "successful",
                                    "si": "buy",
                                    "ch": "sol",
                                }
                            ],
                        }
                    ),
                )
            )
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-g")["legs"][0]
        self.assertIsNotNone(leg["hkL1aMs"])
        self.assertIsNotNone(leg["hkL3Ms"], "base58 sig → solana 轮询")
        self.assertEqual(
            leg["evidence"]["receipt"]["slot"], 287654321, "R24：Sol receipt 存 slot"
        )
        # 卖单不采纳（新开窗口前先关旧窗口——单批纪律与生产一致）
        A.STORE.mark_close("run-g")
        A.STORE.mark_open("run-g2")
        f = FakeFlow(
            "gmgn.ai",
            "/tapi/v1/swap_batch_order",
            "POST",
            resp_body=json.dumps({"data": {"hash": sig, "orderId": "g2"}}),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        A.addons[0].websocket_message(
            ws_frame(
                "ws.gmgn.ai",
                json.dumps(
                    {
                        "channel": "tg_processed_order_info",
                        "data": [
                            {"h": sig, "oi": "g2", "st": "successful", "si": "sell"}
                        ],
                    }
                ),
            )
        )
        self.assertIsNone(A.STORE.timeline("run-g2")["legs"][0]["hkL1aMs"])

    def test_padre_msgpack_done(self):
        """PX-01（r8，案例 D）：Padre 出站锚只有 Turnkey sign_raw_payload——其**真实**响应
        是 ActivityResponse（activity.id 签名活动 UUID / intent.payload 签名摘要 /
        result.signRawPayloadResult r,s,v），**没有任何订单/交易身份键** → 腿无独立锚 →
        任何 DONE 帧（无 orderId / 回显活动 UUID 作 orderId / 他单 orderId）都不得正向
        匹配：不产 L1a′、不挂 hash、不起 receipt 轮询；关窗后 sign 腿如实
        no-execution-proof，授权槽占位 no-order-observed，counts attempted=observed=0。
        旧夹具 `{"transactionHash": …}` 是捏造的 Turnkey 响应（真实响应无此键），旧正则
        曾把 activity.id 写成 orderId（evidence.anchor.legacyRegexDiverged 如实为 true）。
        DONE 门本身的正向覆盖见 test_padre_stale_replay_rejected（注入锚）。"""
        import msgpack

        polled = []
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: polled.append(a)
        try:
            leg_int = padre_sign_leg(
                "run-p",
                manifest={
                    "version": 1,
                    "runId": "run-p",
                    "rounds": 1,
                    "legs": [{"platform": "padre", "chain": "bsc", "rounds": 1}],
                },
            )
            self.assertEqual(
                leg_int["_anchor"],
                {},
                "真实 Turnkey 响应不得建立任何锚（activity.id 不是订单号）",
            )
            h = "0x" + "dd" * 32
            now_ms = int(time.time() * 1000)
            for node in (
                {"txnStatus": "DONE", "txnHash": h, "creationTime": now_ms},
                {
                    "txnStatus": "DONE",
                    "txnHash": h,
                    "orderId": TURNKEY_ACTIVITY_ID,
                    "creationTime": now_ms,
                },
                {
                    "txnStatus": "DONE",
                    "txnHash": h,
                    "orderId": "padre-order-77",
                    "creationTime": now_ms,
                },
            ):
                A.addons[0].websocket_message(
                    ws_frame("backend3.padre.gg", msgpack.packb([373, 200, node]))
                )
            open_leg = A.STORE.timeline("run-p")["legs"][0]
            self.assertIsNone(
                open_leg["hkL1aMs"],
                "无独立锚 → DONE 不得算成功（含回显活动 UUID 的帧）",
            )
            self.assertIsNone(open_leg["txHash"], "无锚帧 hash 不得挂载")
            self.assertEqual(open_leg["anchorRole"], "sign", "不得晋升 order")
            self.assertEqual(polled, [], "无锚帧不得触发 receipt 轮询")
            A.STORE.mark_close("run-p")
        finally:
            A._poll_receipt = orig
        tl = A.STORE.timeline("run-p")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 0, "observed": 0, "unbound": 0}
        )
        sign, ph = tl["legs"]
        self.assertEqual(
            (sign["anchorRole"], sign["incomplete"]), ("sign", "no-execution-proof")
        )
        self.assertEqual(
            sign["evidence"]["anchor"]["keys"], [], "锚键为空——缺失原因在输出可见"
        )
        self.assertTrue(
            sign["evidence"]["anchor"]["legacyRegexDiverged"],
            "旧正则会把 activity.id 当 orderId——分歧如实记录",
        )
        self.assertEqual(ph["incomplete"], "no-order-observed", "授权槽占位保留")
        self.assertEqual(
            A.STORE.diag.get("wsFrameNoLegMatch"),
            3,
            "三帧带身份却无腿可归属——计诊断，不静默",
        )
        self.assertEqual(
            tl["diag"].get("wsFrameNoLegMatch"), 3, "per-run diag 同步透出"
        )

    def test_padre_stale_replay_rejected(self):
        """k2x54 实证：padre 会重放上一批的 FILLED 帧——带旧 creationTime 的帧整帧弃用。
        R07 加严后分两道门各自验证：①锚匹配但源时间陈旧 → 拒（纯新鲜性门）；
        ②新鲜但 hash 与锚不匹配 → 拒（纯关联门）；③锚匹配+新鲜 → 采纳。
        r8：锚由测试直接注入（生产今日无 Padre 锚源，见 padre_sign_leg）。"""
        import msgpack

        h_anchor = "0x" + "88" * 32
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": h_anchor,
        }
        try:
            padre_sign_leg("run-ps", anchor_hash=h_anchor)
            old_ms = int((time.time() - 3600) * 1000)  # 1 小时前的重放帧
            # ①锚匹配但陈旧：纯新鲜性门拒收（不得凭锚匹配绕过陈旧门）
            stale = msgpack.packb(
                [
                    373,
                    200,
                    {"txnStatus": "DONE", "txnHash": h_anchor, "creationTime": old_ms},
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", stale))
            leg0 = A.STORE.timeline("run-ps")["legs"][0]
            self.assertIsNone(leg0["hkL1aMs"], "陈旧重放 DONE 不得算成功")
            # ②新鲜但锚不匹配：纯关联门拒收（锚 hash 不得被覆盖）
            mismatched = msgpack.packb(
                [
                    373,
                    200,
                    {
                        "txnStatus": "DONE",
                        "txnHash": "0x" + "77" * 32,
                        "creationTime": int(time.time() * 1000),
                    },
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", mismatched))
            leg1 = A.STORE.timeline("run-ps")["legs"][0]
            self.assertIsNone(leg1["hkL1aMs"], "锚不匹配的新鲜 DONE 不得算成功")
            self.assertEqual(
                leg1["txHash"], "0x88888888…888888", "冲突帧不得覆盖锚 hash"
            )
            # ③锚匹配 + 新鲜（creationTime=now）正常采纳
            fresh = msgpack.packb(
                [
                    373,
                    200,
                    {
                        "txnStatus": "DONE",
                        "txnHash": h_anchor,
                        "creationTime": int(time.time() * 1000),
                    },
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", fresh))
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-ps")["legs"][0]
        self.assertIsNotNone(leg["hkL1aMs"], "锚匹配的新鲜 DONE 帧正常算成功")
        self.assertEqual(leg["txHash"], "0x88888888…888888")

    def test_binance_pending_then_finished(self):
        """Binance：place-order 响应只有 orderId；PENDING 帧先见 hash；FINISHED 成功。"""
        A.STORE.mark_open("run-b")
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": "0x" + "ef" * 32,
        }
        try:
            f = FakeFlow(
                "web3.binance.com",
                "/bapi/defi/v2/private/wallet-direct/web-dex/place-order",
                "POST",
                req_body=json.dumps({"clientOrderId": "cid-1", "chain": "BSC"}),
                resp_body=json.dumps(
                    {
                        "code": "000000",
                        "success": True,
                        "data": {"orderId": "oid-1", "clientOrderId": "cid-1"},
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            # PENDING：取 hash 不当成功
            A.addons[0].websocket_message(
                ws_frame(
                    "web3-stream.binance.com",
                    json.dumps(
                        {
                            "stream": "w3pc:x",
                            "data": {
                                "bizKey": "DEX_ALL_ORDER",
                                "content": {
                                    "orderId": "oid-1",
                                    "clientOrderId": "cid-1",
                                    "status": "PENDING",
                                    "orderTxId": "0x" + "ef" * 32,
                                },
                            },
                        }
                    ),
                )
            )
            leg0 = A.STORE.timeline("run-b")["legs"][0]
            self.assertIsNone(leg0["hkL1aMs"], "PENDING 不是成功")
            self.assertEqual(
                leg0["txHash"], "0xefefefef…efefef", "PENDING 帧 hash 已收"
            )
            # 别的订单的 FINISHED 不串单
            A.addons[0].websocket_message(
                ws_frame(
                    "web3-stream.binance.com",
                    json.dumps(
                        {
                            "stream": "w3pc:x",
                            "data": {
                                "bizKey": "DEX_ALL_ORDER",
                                "content": {
                                    "orderId": "oid-OTHER",
                                    "status": "FINISHED",
                                },
                            },
                        }
                    ),
                )
            )
            self.assertIsNone(
                A.STORE.timeline("run-b")["legs"][0]["hkL1aMs"],
                "orderId 不匹配不得采纳",
            )
            A.addons[0].websocket_message(
                ws_frame(
                    "web3-stream.binance.com",
                    json.dumps(
                        {
                            "stream": "w3pc:x",
                            "data": {
                                "bizKey": "DEX_ALL_ORDER",
                                "content": {
                                    "orderId": "oid-1",
                                    "clientOrderId": "cid-1",
                                    "status": "FINISHED",
                                    "orderTxId": "0x" + "ef" * 32,
                                },
                            },
                        }
                    ),
                )
            )
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-b")["legs"][0]
        self.assertEqual(leg["chain"], "bsc", "请求体 chain:'BSC' 应映射 bsc")
        self.assertIsNotNone(leg["hkL1aMs"])
        self.assertIsNotNone(leg["hkL3Ms"], "PENDING 帧已见 hash → 轮询已启动")

    def test_fomo_relay_three_way(self):
        """FOMO：relaySwapId↔subscribe filters.id↔data.requestId 三段相等；success 帧。"""
        A.STORE.mark_open("run-f")
        rid = "0x" + "9a" * 32
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }  # R24：echo 被轮询的 hash
        try:
            f = FakeFlow(
                "prod-api.fomo.family",
                "/swaps/v2",
                "POST",
                resp_body=json.dumps(
                    {
                        "success": True,
                        "responseObject": {
                            "v2Swap": {"relaySwapId": rid, "destinationChainId": 4663}
                        },
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            # 页面出站 subscribe（from_client）——绑定 requestId
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "type": "subscribe",
                            "event": "request.status.updated",
                            "filters": {"id": rid},
                        }
                    ),
                    from_client=True,
                )
            )
            # pending 帧不算
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "event": "request.status.updated",
                            "data": {"status": "pending", "requestId": rid},
                        }
                    ),
                )
            )
            self.assertIsNone(A.STORE.timeline("run-f")["legs"][0]["hkL1aMs"])
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "event": "request.status.updated",
                            "data": {
                                "status": "success",
                                "requestId": rid,
                                "txHashes": ["0x" + "12" * 32],
                                "destinationChainId": 4663,
                            },
                        }
                    ),
                )
            )
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-f")["legs"][0]
        self.assertEqual(
            leg["chain"], "robinhood", "destinationChainId 4663 → robinhood"
        )
        self.assertIsNotNone(leg["hkL1aMs"])
        self.assertIsNotNone(leg["hkL3Ms"], "success 帧 txHashes[0] → EVM 轮询")

    def test_rounds_occurrence_and_no_mark(self):
        """无窗口不记录；同平台多次下单记到达序 observationIndex（r7：授权轮 round
        不再按出现序填写——恒 None，唯一关联由 EU 侧 authorizedRound 回配）。"""
        f = FakeFlow(
            "web3.okx.com", "/priapi/v6/dx/trade/multi/broadcast", "POST", "{}"
        )
        A.addons[0].request(f)
        self.assertEqual(A.STORE.timeline("nope")["legs"], [])
        A.STORE.mark_open("run-n")
        for _ in range(3):
            fx = FakeFlow("prod-api.fomo.family", "/swaps/v2", "POST", "", "{}")
            A.addons[0].request(fx)
            A.addons[0].response(fx)
        legs = A.STORE.timeline("run-n")["legs"]
        self.assertEqual(
            [x["observationIndex"] for x in legs], [1, 2, 3], "到达序连续编号"
        )
        self.assertEqual(
            [x["round"] for x in legs], [None, None, None], "r7：到达序不冒充授权轮"
        )

    def test_stale_replay_no_pollution(self):
        """v5hh3 复盘回归：陈旧/跨批重放帧不得把旧 hash 挂到新腿；缺 id 的 FINISHED 不算成功；
        同 hash 不得挂两条腿。"""
        A.STORE.mark_open("run-sr")
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        h1 = "0x" + "aa" * 32
        h2 = "0x" + "bb" * 32
        po = "/bapi/defi/v2/private/wallet-direct/web-dex/place-order"

        def fin(oid, cid, h, **kw):
            c = {"status": "FINISHED", "orderTxId": h}
            if oid:
                c["orderId"] = oid
            if cid:
                c["clientOrderId"] = cid
            c.update(kw)
            return ws_frame(
                "web3-stream.binance.com",
                json.dumps(
                    {
                        "stream": "w3pc:x",
                        "data": {"bizKey": "DEX_ALL_ORDER", "content": c},
                    }
                ),
            )

        try:
            # r1 正常成交 oid-A/h1
            f1 = FakeFlow(
                "web3.binance.com",
                po,
                "POST",
                req_body=json.dumps({"clientOrderId": "c1", "chain": "BSC"}),
                resp_body=json.dumps(
                    {
                        "code": "000000",
                        "data": {"orderId": "oid-A", "clientOrderId": "c1"},
                    }
                ),
            )
            A.addons[0].request(f1)
            A.addons[0].response(f1)
            A.addons[0].websocket_message(fin("oid-A", "c1", h1))
            time.sleep(0.3)
            # r2 新腿 oid-B；重放 r1 的 FINISHED → 不得污染
            f2 = FakeFlow(
                "web3.binance.com",
                po,
                "POST",
                req_body=json.dumps({"clientOrderId": "c2", "chain": "BSC"}),
                resp_body=json.dumps(
                    {
                        "code": "000000",
                        "data": {"orderId": "oid-B", "clientOrderId": "c2"},
                    }
                ),
            )
            A.addons[0].request(f2)
            A.addons[0].response(f2)
            A.addons[0].websocket_message(fin("oid-A", "c1", h1))
            leg2 = A.STORE.timeline("run-sr")["legs"][1]
            self.assertIsNone(leg2["txHash"], "重放帧 hash 不得挂到新腿")
            self.assertIsNone(leg2["hkL1aMs"], "重放 FINISHED 不得算新腿成功")
            # 缺 id 的 FINISHED 也不算成功、hash 也不采纳
            A.addons[0].websocket_message(fin(None, None, h2))
            leg2 = A.STORE.timeline("run-sr")["legs"][1]
            self.assertIsNone(leg2["txHash"])
            self.assertIsNone(leg2["hkL1aMs"])
            # r2 自己的正向匹配 FINISHED → 采纳
            A.addons[0].websocket_message(fin("oid-B", "c2", h2))
            time.sleep(0.3)
        finally:
            A._jsonrpc = orig
        legs = A.STORE.timeline("run-sr")["legs"]
        self.assertEqual(len(legs), 2)
        self.assertIsNotNone(legs[0]["hkL1aMs"])
        self.assertIsNotNone(legs[1]["hkL1aMs"])
        self.assertIsNotNone(legs[1]["hkL3Ms"])
        self.assertNotEqual(
            legs[0]["txHash"], legs[1]["txHash"], "同 hash 不得挂两条腿"
        )

    def test_incomplete_no_fabrication(self):
        """关联不上 = null + incomplete，绝不发明数字。"""
        A.STORE.mark_open("run-z")
        f = FakeFlow(
            "web3.okx.com", "/priapi/v6/dx/trade/multi/broadcast", "POST", "{}", "{}"
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        leg = A.STORE.timeline("run-z")["legs"][0]
        self.assertIsNone(leg["hkL1aMs"])
        self.assertIsNone(leg["hkL3Ms"])

    def test_ws_messages_trimmed(self):
        """长连行情流内存有界：每条消息后 messages 只剩最新一条（开窗/关窗都裁剪）。"""
        # 关窗（无任何 mark）：未分类主机关窗流也要裁
        f = FakeFlow("nbstream.binance.com", "/ws")
        f.websocket = types.SimpleNamespace(
            messages=[FakeWSMsg("{}") for _ in range(50)]
        )
        for _ in range(3):
            f.websocket.messages.append(FakeWSMsg("{}"))
            A.addons[0].websocket_message(f)
            self.assertEqual(len(f.websocket.messages), 1)
        # 开窗 + 平台主机：处理后同样只剩 1 条，且最新条保持可读
        A.STORE.mark_open("run-t")
        f2 = ws_frame("ws.gmgn.ai", json.dumps({"channel": "tick"}))
        f2.websocket.messages = [
            FakeWSMsg("{}") for _ in range(20)
        ] + f2.websocket.messages
        A.addons[0].websocket_message(f2)
        self.assertEqual(len(f2.websocket.messages), 1)
        self.assertIn("tick", f2.websocket.messages[-1].content)

    def test_leg_fields_are_milliseconds(self):
        """单位回归：腿时间戳与 hkL1aMs 必须是毫秒（曾存 monotonic 秒，UI 把 0.6s 显示成 1ms）。"""
        A.STORE.mark_open("run-ms")
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
                resp_body=json.dumps(
                    {
                        "code": "0",
                        "data": {"transactionHash": "0x" + "cd" * 32, "orderId": "o1"},
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            time.sleep(0.25)
            A.addons[0].websocket_message(
                ws_frame(
                    "wsdexpri.okx.com",
                    json.dumps(
                        {
                            "arg": {"channel": "dex-swap-order-info"},
                            "data": {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "o1",
                                    "transactionHash": "0x" + "cd" * 32,
                                }
                            },
                        }
                    ),
                )
            )
            time.sleep(0.2)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-ms")["legs"][0]
        self.assertGreaterEqual(
            leg["hkL1aMs"], 200, "0.25s 睡眠后 hkL1aMs 应 ≥200ms（秒 bug 会给 0.2）"
        )
        self.assertLess(leg["hkL1aMs"], 5000)
        for k in ("tOrderOutMs", "tFirstRespMs", "tSuccessPushMs"):
            self.assertGreater(
                leg[k], 10**6, f"{k} 应是毫秒级绝对值（秒域 uptime 需 >11 天才到 1e6）"
            )


# ── R07：成功判定缺正向关联 ──


class TestR07PositiveCorrelation(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_fomo_success_requires_anchor(self):
        """R07：FOMO 成功帧必须与请求/响应建立的锚（relaySwapId/subscribe 绑定）
        正向匹配——腿无锚或帧无 requestId 都不得出 L1a'。"""
        A.STORE.mark_open("run-fx")
        rid = "0x" + "9a" * 32
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        try:
            # 腿1：响应提取失败（无 relaySwapId）→ 腿无锚；成功帧带 requestId 也不算
            f1 = FakeFlow("prod-api.fomo.family", "/swaps/v2", "POST", resp_body="{}")
            A.addons[0].request(f1)
            A.addons[0].response(f1)
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "event": "request.status.updated",
                            "data": {
                                "status": "success",
                                "requestId": rid,
                                "txHashes": ["0x" + "12" * 32],
                                "destinationChainId": 4663,
                            },
                        }
                    ),
                )
            )
            leg1 = A.STORE.timeline("run-fx")["legs"][0]
            self.assertIsNone(leg1["hkL1aMs"], "腿无独立锚 → 成功帧不得采纳")
            self.assertIsNone(leg1["txHash"], "无锚帧 hash 不得挂载")
            # 腿2：有锚（响应 relaySwapId），帧缺 requestId → 不得算成功
            f2 = FakeFlow(
                "prod-api.fomo.family",
                "/swaps/v2",
                "POST",
                resp_body=json.dumps(
                    {
                        "success": True,
                        "responseObject": {
                            "v2Swap": {"relaySwapId": rid, "destinationChainId": 4663}
                        },
                    }
                ),
            )
            A.addons[0].request(f2)
            A.addons[0].response(f2)
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "event": "request.status.updated",
                            "data": {
                                "status": "success",
                                "txHashes": ["0x" + "34" * 32],
                            },
                        }
                    ),
                )
            )
            leg2 = A.STORE.timeline("run-fx")["legs"][1]
            self.assertIsNone(leg2["hkL1aMs"], "帧无 requestId → 不得凭时间窗充数")
            # 正向控制：帧 requestId ≡ 锚 → 采纳
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "event": "request.status.updated",
                            "data": {
                                "status": "success",
                                "requestId": rid,
                                "txHashes": ["0x" + "34" * 32],
                                "destinationChainId": 4663,
                            },
                        }
                    ),
                )
            )
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        leg2 = A.STORE.timeline("run-fx")["legs"][1]
        self.assertIsNotNone(leg2["hkL1aMs"], "正向匹配成功帧正常采纳")
        self.assertIsNotNone(leg2["hkL3Ms"])

    def test_binance_frame_cannot_self_certify(self):
        """R07：腿的 orderId 不得由待判成功帧自己先写入再自证关联——锚只能来自
        请求/响应。腿无锚时 FINISHED 帧（哪怕带 orderId+hash）不算成功。"""
        A.STORE.mark_open("run-sc")
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        try:
            # 响应体无 orderId（提取失败）且请求体无 clientOrderId → 腿无锚
            f = FakeFlow(
                "web3.binance.com",
                "/bapi/defi/v2/private/wallet-direct/web-dex/place-order",
                "POST",
                req_body=json.dumps({"chain": "BSC"}),
                resp_body="{}",
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            A.addons[0].websocket_message(
                ws_frame(
                    "web3-stream.binance.com",
                    json.dumps(
                        {
                            "stream": "w3pc:x",
                            "data": {
                                "bizKey": "DEX_ALL_ORDER",
                                "content": {
                                    "orderId": "oid-X",
                                    "status": "FINISHED",
                                    "orderTxId": "0x" + "ef" * 32,
                                },
                            },
                        }
                    ),
                )
            )
            leg = A.STORE.timeline("run-sc")["legs"][0]
            self.assertIsNone(
                leg["hkL1aMs"], "帧自带 orderId 先写入腿再自证 → 不得算成功"
            )
            self.assertIsNone(leg["txHash"], "无正向锚匹配 → hash 不得挂载")
        finally:
            A._jsonrpc = orig

    def test_padre_done_needs_same_node_hash_and_fresh_ts(self):
        """R07：padre DONE ①必须同节点带合法 hash；②时间新鲜性只在同一 DONE 节点子树
        内校验——根节点新 timestamp 不得给旧子交易背书；③DONE hash 必须与请求/响应
        独立锚正向匹配。各负例用锚匹配的 hash 隔离被测门（除无 hash 反例）。"""
        import msgpack

        h_anchor = "0x" + "88" * 32
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        now_ms = int(time.time() * 1000)
        old_ms = now_ms - 3600_000
        try:
            padre_sign_leg(
                "run-pd", anchor_hash=h_anchor
            )  # r8：锚注入（生产无 Padre 锚源）
            # ①DONE 无 hash（有时间戳）→ 整帧拒收，不算成功
            A.addons[0].websocket_message(
                ws_frame(
                    "backend3.padre.gg",
                    msgpack.packb(
                        [373, 200, {"txnStatus": "DONE", "creationTime": now_ms}]
                    ),
                )
            )
            leg = A.STORE.timeline("run-pd")["legs"][0]
            self.assertIsNone(leg["hkL1aMs"], "DONE 无同节点 hash → 不算成功")
            # ②跨 entry 拼接：根节点新 timestamp + 子 DONE（锚匹配 hash）旧 creationTime → 拒
            stitched = msgpack.packb(
                [
                    373,
                    200,
                    {
                        "timestamp": now_ms,
                        "sub": {
                            "txnStatus": "DONE",
                            "txnHash": h_anchor,
                            "creationTime": old_ms,
                        },
                    },
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", stitched))
            leg = A.STORE.timeline("run-pd")["legs"][0]
            self.assertIsNone(leg["hkL1aMs"], "根新 timestamp 不得给旧子 DONE 背书")
            # ②b DONE 节点无时间戳 → 缺源时间不新鲜
            A.addons[0].websocket_message(
                ws_frame(
                    "backend3.padre.gg",
                    msgpack.packb(
                        [373, 200, {"txnStatus": "DONE", "txnHash": h_anchor}]
                    ),
                )
            )
            leg = A.STORE.timeline("run-pd")["legs"][0]
            self.assertIsNone(leg["hkL1aMs"], "DONE 节点缺时间戳 → 不算成功")
            # 正向控制：同节点锚匹配 hash + 新鲜 creationTime → 采纳
            good = msgpack.packb(
                [
                    373,
                    200,
                    {"txnStatus": "DONE", "txnHash": h_anchor, "creationTime": now_ms},
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", good))
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-pd")["legs"][0]
        self.assertIsNotNone(leg["hkL1aMs"], "同节点锚匹配 hash+新鲜时间戳正常采纳")
        self.assertEqual(leg["txHash"], "0x88888888…888888")
        self.assertIsNotNone(leg["hkL3Ms"])


# ── R08：receipt 单 poller + 首次时刻只写一次 ──


class TestR08ReceiptPoller(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_single_receipt_poller_latch(self):
        """R08：run+chain+hash 原子闩锁——每腿至多一个 poller，轮询用腿锁定 hash；
        冲突 hash 整帧拒收（不得起第二个 poller、不得覆盖腿 hash）。"""
        A.STORE.mark_open("run-pl")
        h1 = "0x" + "ab" * 32
        h2 = "0x" + "cd" * 32
        starts = []
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: starts.append(a)
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                resp_body=json.dumps(
                    {"code": "0", "data": {"transactionHash": h1, "orderId": "o1"}}
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            time.sleep(0.05)
            self.assertEqual(len(starts), 1, "响应见 hash → 恰好一个 poller")
            # 同 hash 成功帧 → 不得再起 poller
            A.addons[0].websocket_message(
                ws_frame(
                    "wsdexpri.okx.com",
                    json.dumps(
                        {
                            "arg": {"channel": "dex-swap-order-info"},
                            "data": {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "o1",
                                    "transactionHash": h1,
                                }
                            },
                        }
                    ),
                )
            )
            time.sleep(0.05)
            # 冲突 hash 帧 → 整帧拒收
            A.addons[0].websocket_message(
                ws_frame(
                    "wsdexpri.okx.com",
                    json.dumps(
                        {
                            "arg": {"channel": "dex-swap-order-info"},
                            "data": {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "o1",
                                    "transactionHash": h2,
                                }
                            },
                        }
                    ),
                )
            )
            time.sleep(0.05)
        finally:
            A._poll_receipt = orig
        self.assertEqual(len(starts), 1, "同腿不得起第二个 poller")
        self.assertEqual(starts[0][3], h1, "poller 必须轮询腿锁定的 hash")
        leg = A.STORE.timeline("run-pl")["legs"][0]
        self.assertEqual(leg["txHash"], "0xabababab…ababab", "冲突帧不得覆盖腿 hash")

    def test_settle_receipt_first_write_wins(self):
        """R08：首次成功时刻只写一次——_settle_receipt 已有值不再覆盖。"""
        leg = A.STORE.new_leg("run-x", "okx", "bsc", {"t": 1.0, "tw": "x"})
        A._settle_receipt(leg, {"status": 1, "blockNumber": "0x1"})
        t1 = leg["tReceiptMs"]
        r1 = dict(leg["receipt"])
        time.sleep(0.02)
        A._settle_receipt(leg, {"status": 1, "blockNumber": "0x2"})
        self.assertEqual(leg["tReceiptMs"], t1, "tReceiptMs 不得被第二次 settle 覆盖")
        self.assertEqual(leg["receipt"], r1, "receipt 证据不得被覆盖")


# ── R09：跨批窗口隔离 ──


class TestR09WindowIsolation(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_window_close_freezes_legs(self):
        """R09：A 批关闭后其腿冻结——迟到 WS 帧不得更新 A 的腿，事件不得记进 B。"""
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        h1 = "0x" + "ab" * 32
        try:
            A.STORE.mark_open("run-A")
            f1 = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                resp_body=json.dumps(
                    {"code": "0", "data": {"transactionHash": h1, "orderId": "oA"}}
                ),
            )
            A.addons[0].request(f1)
            A.addons[0].response(f1)
            A.STORE.mark_close("run-A")
            A.STORE.mark_open("run-B")
            # 属于 A 腿的迟到成功帧
            A.addons[0].websocket_message(
                ws_frame(
                    "wsdexpri.okx.com",
                    json.dumps(
                        {
                            "arg": {"channel": "dex-swap-order-info"},
                            "data": {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "oA",
                                    "transactionHash": h1,
                                }
                            },
                        }
                    ),
                )
            )
            leg_a = A.STORE.timeline("run-A")["legs"][0]
            self.assertIsNone(leg_a["hkL1aMs"], "关窗后帧不得更新 A 的腿")
            self.assertEqual(
                A.STORE.timeline("run-B")["legs"], [], "B 不得收到 A 腿的腿记录"
            )
            self.assertEqual(
                list(A.STORE.events.get("run-B", [])), [], "A 腿的事件不得记进 B"
            )
        finally:
            A._jsonrpc = orig

    def test_mark_open_closes_other_windows(self):
        """R09：拒绝多窗并存——新开窗显式关闭旧窗（保留其 incomplete 状态）。"""
        A.STORE.mark_open("run-old")
        f = FakeFlow(
            "web3.okx.com", "/priapi/v6/dx/trade/multi/broadcast", "POST", "{}", "{}"
        )
        A.addons[0].request(f)
        A.STORE.mark_open("run-new")
        self.assertIsNotNone(
            A.STORE.marks["run-old"]["closed"], "新窗开启须显式关闭旧窗"
        )
        self.assertEqual(A.STORE.open_run_ids(), ["run-new"], "open_run_ids 只含最新窗")
        leg = A.STORE.timeline("run-old")["legs"][0]
        self.assertIsNone(leg["hkL1aMs"], "旧窗腿保留未成功状态，不得被新窗流量更新")

    def test_receipt_writeback_requires_open_window(self):
        """R09：receipt 线程回写前核验腿未被冻结——关窗后的回写丢弃。"""
        A.STORE.mark_open("run-fz")
        f = FakeFlow(
            "web3.okx.com", "/priapi/v6/dx/trade/multi/broadcast", "POST", "{}", "{}"
        )
        A.addons[0].request(f)
        leg = A.STORE.legs["run-fz"][0]
        A.STORE.mark_close("run-fz")
        A._settle_receipt(leg, {"status": 1, "blockNumber": "0x1"})
        self.assertIsNone(leg["tReceiptMs"], "关窗后的 receipt 回写必须丢弃")
        A._settle_incomplete(leg, "receipt poll timeout")
        self.assertIsNone(leg["incomplete"], "关窗后的 incomplete 回写同样丢弃")


# ── R18/R29：热重载控制面/采集面分裂 + build manifest ──


def _http_json(method, url, obj=None):
    parsed = urllib.parse.urlsplit(url)
    if parsed.hostname != "127.0.0.1" or parsed.port not in _TEST_SERVERS:
        raise AssertionError("request has no owned test server")
    server = _TEST_SERVERS[parsed.port]
    if server.socket.fileno() < 0:
        raise urllib.error.URLError("owned test server is closed")
    url = urllib.parse.urlunsplit(
        parsed._replace(netloc=f"127.0.0.1:{server.server_address[1]}")
    )
    data = json.dumps(obj).encode() if obj is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"content-type": "application/json"} if data else {},
    )
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        req, timeout=5
    ) as r:
        return r.status, json.loads(r.read().decode())


class TestR18R29ControlPlane(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_health_build_manifest(self):
        """R29：/health 带 build manifest（addon 版本/运行环境/实例 id）。"""
        code, body = _http_json("GET", "http://127.0.0.1:8072/health")
        self.assertEqual(code, 200)
        self.assertTrue(body["ok"])
        self.assertEqual(body["addonVersion"], A.ADDON_VERSION)
        self.assertEqual(body["instanceId"], A.INSTANCE_ID)
        import platform as _pf

        self.assertEqual(body["pythonVersion"], _pf.python_version())
        self.assertIn("mitmproxyVersion", body, "键必须存在（离线环境值可为 null）")

    def test_ctrl_takeover_and_split_reporting(self):
        """R18：重载接管（旧 server shutdown/join 后 rebing 成功，health 仍 200）；
        bind 失败/分裂时 health 如实 503 而非假装正常；done() 关停控制面。"""
        # 模拟进程内热重载：再次 _start_ctrl —— 先停旧 server 再绑新 server
        A._start_ctrl()
        code, body = _http_json("GET", "http://127.0.0.1:8072/health")
        self.assertEqual(
            (code, body["ok"], body["instanceId"]), (200, True, A.INSTANCE_ID)
        )
        # 模拟新实例 bind 失败留下的分裂标记 → health 如实报错
        reg = A._ctrl_registry()
        reg["bindError"] = "injected-test"
        try:
            _http_json("GET", "http://127.0.0.1:8072/health")
            self.fail("bindError 时 /health 必须非 2xx")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 503)
            body = json.loads(e.read().decode())
            self.assertFalse(body["ok"])
            self.assertIn("bind", body["error"])
        reg["bindError"] = None
        # 模拟 loadedInstanceId 与 serving instanceId 分裂
        reg["loadedInstanceId"] = "other-instance"
        try:
            _http_json("GET", "http://127.0.0.1:8072/health")
            self.fail("实例分裂时 /health 必须非 2xx")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 503)
        reg["loadedInstanceId"] = A.INSTANCE_ID
        code, body = _http_json("GET", "http://127.0.0.1:8072/health")
        self.assertEqual(code, 200)
        # done()：本实例控制面关停 → 连接被拒；再次 _start_ctrl 恢复
        A.addons[0].done()
        with self.assertRaises(urllib.error.URLError):
            _http_json("GET", "http://127.0.0.1:8072/health")
        A._start_ctrl()
        code, body = _http_json("GET", "http://127.0.0.1:8072/health")
        self.assertEqual((code, body["ok"]), (200, True))


# ── 控制面 _send 断连纪律（NR-16：journal 诊断最小化）──


class _FakeWfile:
    """_Ctrl._send 的 wfile 桩：可注入写异常；成功路径记录全部写入字节。"""

    def __init__(self, error=None):
        self.error = error
        self.writes = []

    def write(self, b):
        if self.error is not None:
            raise self.error
        self.writes.append(bytes(b))


def _bare_ctrl(wfile):
    """绕过 BaseHTTPRequestHandler.__init__（不开 socket）构造裸 _Ctrl 实例。"""
    h = A._Ctrl.__new__(A._Ctrl)
    h.requestline = "GET /health HTTP/1.1"
    h.request_version = "HTTP/1.1"
    h.wfile = wfile
    return h


class TestCtrlSendDisconnect(unittest.TestCase):
    """_send 只吞「明确断连」（BrokenPipeError/ConnectionResetError——客户端经 SSH
    隧道中途断开，响应注定送达不到；默认 handle_error 会把完整 traceback 打进
    journal）。其余异常必须照常传播（fail loud，不吞）。纯离线：裸实例 + wfile 桩，
    不占用任何端口。"""

    def setUp(self):
        fresh()

    def test_broken_pipe_swallowed(self):
        for err in (BrokenPipeError("EPIPE"), ConnectionResetError("ECONNRESET")):
            h = _bare_ctrl(_FakeWfile(error=err))
            h._send(200, {"ok": True})  # 不抛即通过

    def test_other_errors_propagate(self):
        import errno

        for err in (
            OSError(
                errno.EAGAIN, "buffer full"
            ),  # 非断连 OSError（BlockingIOError）不吞
            ValueError("boom"),
            RuntimeError("boom"),
        ):
            h = _bare_ctrl(_FakeWfile(error=err))
            with self.assertRaises(type(err)):
                h._send(200, {"ok": True})

    def test_normal_send_writes_headers_and_body(self):
        w = _FakeWfile()
        _bare_ctrl(w)._send(200, {"ok": True, "n": 1})
        blob = b"".join(w.writes)
        self.assertIn(b"200 OK", blob)  # 默认 protocol_version=HTTP/1.0
        self.assertIn(b"content-type: application/json", blob.lower())
        # 响应体原样完整送达（最后一次 write 即 body）
        self.assertEqual(w.writes[-1], json.dumps({"ok": True, "n": 1}).encode())


# ── R20：/latency 探针口径 ──


class _FakeWsConn:
    """websockets.sync.client.connect 桩：__enter__ 模拟握手耗时；ping 立即回。"""

    def __init__(self, enter_delay):
        self.enter_delay = enter_delay
        self.sent = None

    def __enter__(self):
        time.sleep(self.enter_delay)
        return self

    def __exit__(self, *a):
        return False

    def send(self, data):
        self.sent = data

    def recv(self, timeout=None):
        m = re.match(r"^ping\|([0-9a-f]{32})\|", str(self.sent))
        if m:
            return f"pong|{m.group(1)}"
        import msgpack

        v = msgpack.unpackb(
            self.sent if isinstance(self.sent, bytes) else bytes(self.sent), raw=False
        )
        return msgpack.packb([9, v[1], "pong"])


class TestR20LatencyProbes(unittest.TestCase):
    def test_ws_ping_excludes_handshake(self):
        """R20：okx/padre ws-ping 不得把握手计入 ping——先连上再计时。"""
        orig = A.ws_connect
        A.ws_connect = lambda url, **kw: _FakeWsConn(0.4)
        try:
            r = A._probe_okx_ws()
            self.assertIsNotNone(r)
            self.assertEqual(r["method"], "ws-ping")
            self.assertLess(r["ms"], 150, "握手 0.4s 若计入 ping 则 ≥200ms")
            r2 = A._probe_padre_ws()
            self.assertIsNotNone(r2)
            self.assertLess(r2["ms"], 150, "padre 同律：握手不计入")
        finally:
            A.ws_connect = orig

    def test_http_ping_reuses_connection(self):
        """R20：http-ping 预热+3 热样本必须复用同一 keep-alive 连接。"""

        class FakeConn:
            instances = 0

            def __init__(self, host, timeout=None):
                FakeConn.instances += 1
                self.requests = 0

            def request(self, method, path, headers=None):
                self.requests += 1

            def getresponse(self):
                return types.SimpleNamespace(read=lambda *a: b"{}")

            def close(self):
                pass

        FakeConn.instances = 0
        holder = {}

        def factory(host, timeout=None):
            c = FakeConn(host, timeout=timeout)
            holder["conn"] = c
            return c

        r = A._probe_http_ping("api.binance.com", "/api/v3/ping", conn_cls=factory)
        self.assertIsNotNone(r)
        self.assertEqual(r["method"], "http-ping")
        self.assertEqual(FakeConn.instances, 1, "预热+样本须在同一连接上")
        self.assertEqual(holder["conn"].requests, 4, "1 预热 + 3 热样本")


# ── R21：时间戳在 hook 入口冻结 ──


class TestR21FrozenTimestamps(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_request_ts_frozen_at_entry(self):
        """R21：tOrderOut 是 request-observed——hook 入口冻结，解析耗时不计入。"""
        A.STORE.mark_open("run-fr")
        orig_extract = A.RULES["okx"]["extract_chain"]
        A.RULES["okx"]["extract_chain"] = lambda b: (time.sleep(0.6), "bsc")[1]
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
            )
            t_before = A._mono_ms()
            A.addons[0].request(f)
        finally:
            A.RULES["okx"]["extract_chain"] = orig_extract
        leg = A.STORE.timeline("run-fr")["legs"][0]
        self.assertLess(
            leg["tOrderOutMs"], t_before + 400, "解析 0.6s 之后采样 = 入口未冻结"
        )
        self.assertEqual(leg["chain"], "bsc")

    def test_ws_success_ts_frozen_at_entry(self):
        """R21：tSuccessPush 在 WS 钩子入口冻结——decode/判定耗时不计入。
        r10：okx 改逐 entry 解析（ws_entries 取代 ws_success/ws_ids）——同一边界
        改在 ws_entries 上打桩（解析耗时不得污染入口冻结时刻）。"""
        A.STORE.mark_open("run-fw")
        orig_rpc = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        orig_entries = A.RULES["okx"]["ws_entries"]
        slow = lambda p: (
            time.sleep(0.6),
            [{"ids": {"orderId": "o1", "txHash": "0x" + "ab" * 32}, "success": True}],
        )[1]
        A.RULES["okx"]["ws_entries"] = slow
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                resp_body=json.dumps(
                    {
                        "code": "0",
                        "data": {"transactionHash": "0x" + "ab" * 32, "orderId": "o1"},
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            t_before = A._mono_ms()
            A.addons[0].websocket_message(
                ws_frame(
                    "wsdexpri.okx.com",
                    json.dumps(
                        {
                            "arg": {"channel": "dex-swap-order-info"},
                            "data": {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "o1",
                                    "transactionHash": "0x" + "ab" * 32,
                                }
                            },
                        }
                    ),
                )
            )
        finally:
            A._jsonrpc = orig_rpc
            A.RULES["okx"]["ws_entries"] = orig_entries
        leg = A.STORE.timeline("run-fw")["legs"][0]
        self.assertIsNotNone(leg["tSuccessPushMs"])
        self.assertLess(
            leg["tSuccessPushMs"], t_before + 400, "多次 decode 后才采样 = 入口未冻结"
        )


# ── R22：mark manifest 轮次登记 ──


class TestR22Manifest(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_mark_manifest_counts_and_placeholders(self):
        """R22：/mark 接受 manifest（rounds 数）；timeline 输出 requested/attempted/
        observed 计数 + 缺失腿占位（incomplete=no-order-observed）。attempted 只计
        anchorRole=order 腿——fomo 预览语义见 TestR22PreviewRoleAndBudgets。"""
        code, _ = _http_json(
            "POST",
            "http://127.0.0.1:8072/mark",
            {"runId": "run-mf", "manifest": {"rounds": 3}},
        )
        self.assertEqual(code, 200)
        f = FakeFlow(
            "web3.okx.com", "/priapi/v6/dx/trade/multi/broadcast", "POST", "{}", "{}"
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        code, tl = _http_json("GET", "http://127.0.0.1:8072/timeline/run-mf")
        self.assertEqual(code, 200)
        self.assertEqual(
            tl["counts"], {"requested": 3, "attempted": 1, "observed": 0, "unbound": 0}
        )
        self.assertEqual(len(tl["legs"]), 3, "缺失轮次须有占位腿")
        ph = tl["legs"][1:]
        for x in ph:
            self.assertEqual(x["incomplete"], "no-order-observed")
            self.assertIsNone(x["hkL1aMs"])

    def test_manifest_detail_matching_and_backward_compat(self):
        """R22：明细 manifest 按 platform/chain 配对；无 manifest 时行为同今
        （无占位，counts.requested=null）。"""
        A.STORE.mark_open(
            "run-md",
            manifest={
                "legs": [
                    {"platform": "okx", "chain": "bsc"},
                    {"platform": "okx", "chain": "bsc"},
                ]
            },
        )
        f = FakeFlow(
            "web3.okx.com",
            "/priapi/v6/dx/trade/multi/broadcast",
            "POST",
            req_body=json.dumps({"data": {"chainId": "56"}}),
        )
        A.addons[0].request(f)
        tl = A.STORE.timeline("run-md")
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 1, "observed": 0, "unbound": 0}
        )
        self.assertEqual(len(tl["legs"]), 2)
        self.assertEqual(tl["legs"][1]["incomplete"], "no-order-observed")
        self.assertEqual(tl["legs"][1]["platform"], "okx")
        # 向后兼容：无 manifest
        A.STORE.mark_open("run-bc")
        A.addons[0].request(
            FakeFlow(
                "web3.okx.com", "/priapi/v6/dx/trade/multi/broadcast", "POST", "{}"
            )
        )
        tl2 = A.STORE.timeline("run-bc")
        self.assertIsNone(tl2["counts"]["requested"])
        self.assertEqual(tl2["counts"]["attempted"], 1)
        self.assertEqual(len(tl2["legs"]), 1, "无 manifest 不得生成占位腿")

    def test_manifest_per_leg_rounds_denominator(self):
        """R22 二轮：client manifest 真实形状 {version,runId,rounds,legs:[{platform,
        chain,rounds}]}——requested=Σ legs[i].rounds（3 链×4 平台 N=3 → 36，而非
        明细腿数 12）；缺失轮次按 spec 的 rounds 逐个占位。"""
        legs_spec = [
            {"platform": p, "chain": c, "rounds": 3}
            for c in ("bsc", "robinhood", "solana")
            for p in ("gmgn", "okx", "padre", "binance")
        ]
        A.STORE.mark_open(
            "run-mr",
            manifest={"version": 1, "runId": "run-mr", "rounds": 3, "legs": legs_spec},
        )
        # 实际只观察到 okx/bsc × 2 个下单请求
        for _ in range(2):
            A.addons[0].request(
                FakeFlow(
                    "web3.okx.com",
                    "/priapi/v6/dx/trade/multi/broadcast",
                    "POST",
                    req_body=json.dumps({"data": {"chainId": "56"}}),
                )
            )
        tl = A.STORE.timeline("run-mr")
        self.assertEqual(
            tl["counts"],
            {"requested": 36, "attempted": 2, "observed": 0, "unbound": 0},
            "requested=Σ per-leg rounds（12 腿×3 轮）",
        )
        self.assertEqual(len(tl["legs"]), 36, "2 真实腿 + 34 占位腿")
        ph = tl["legs"][2:]
        self.assertTrue(
            all(
                x["incomplete"] == "no-order-observed" and x["hkL1aMs"] is None
                for x in ph
            )
        )
        okx_bsc_ph = [x for x in ph if x["platform"] == "okx" and x["chain"] == "bsc"]
        self.assertEqual(len(okx_bsc_ph), 1, "okx/bsc 授权 3 轮实到 2 → 恰好 1 个占位")

    def test_manifest_per_leg_rounds_preview_not_consumed(self):
        """per-leg rounds manifest 下 fomo 预览腿同样不计 attempted、不消费授权轮次
        占位；Relay success 正向匹配晋升 order 后才消费 1 个占位。"""
        rid = "0x" + "9a" * 32
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        try:
            A.STORE.mark_open(
                "run-mp",
                manifest={
                    "version": 1,
                    "runId": "run-mp",
                    "rounds": 2,
                    "legs": [{"platform": "fomo", "chain": "robinhood", "rounds": 2}],
                },
            )
            f = FakeFlow(
                "prod-api.fomo.family",
                "/swaps/v2",
                "POST",
                resp_body=json.dumps(
                    {
                        "success": True,
                        "responseObject": {
                            "v2Swap": {"relaySwapId": rid, "destinationChainId": 4663}
                        },
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            tl = A.STORE.timeline("run-mp")
            self.assertEqual(
                tl["counts"],
                {"requested": 2, "attempted": 0, "observed": 0, "unbound": 0},
                "预览腿不计 attempted",
            )
            self.assertEqual(
                len(tl["legs"]), 3, "1 预览腿 + 2 占位（预览不消费授权轮次）"
            )
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "event": "request.status.updated",
                            "data": {
                                "status": "success",
                                "requestId": rid,
                                "txHashes": ["0x" + "12" * 32],
                                "destinationChainId": 4663,
                            },
                        }
                    ),
                )
            )
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        tl = A.STORE.timeline("run-mp")
        self.assertEqual(tl["counts"]["attempted"], 1, "晋升 order 后计 1 次下单")
        self.assertEqual(tl["counts"]["observed"], 1)
        self.assertEqual(
            len(tl["legs"]), 2, "晋升后消费 1 个占位（1 order 腿 + 1 占位）"
        )


# ── R23：chain 不参与跨腿去重 ──


class TestR23ChainNotDeduped(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_chain_not_deduped_across_legs(self):
        """R23：同 run 前腿已登记 chain=bsc，后腿 WS 帧补录 chain=bsc 不得被丢弃
        （去重只针对 txHash/orderId/clientOrderId）。"""
        A.STORE.mark_open("run-ch")
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        try:
            for i, oid in enumerate(("o1", "o2")):
                h = "0x" + ("ab" if i == 0 else "cd") * 32
                f = FakeFlow(
                    "web3.okx.com",
                    "/priapi/v6/dx/trade/multi/broadcast",
                    "POST",
                    resp_body=json.dumps(
                        {"code": "0", "data": {"transactionHash": h, "orderId": oid}}
                    ),
                )
                A.addons[0].request(f)
                A.addons[0].response(f)
                A.addons[0].websocket_message(
                    ws_frame(
                        "wsdexpri.okx.com",
                        json.dumps(
                            {
                                "arg": {"channel": "dex-swap-order-info"},
                                "data": {
                                    "dexData": {
                                        "status": "1",
                                        "orderId": oid,
                                        "transactionHash": h,
                                        "chainId": "56",
                                    }
                                },
                            }
                        ),
                    )
                )
            time.sleep(0.25)
        finally:
            A._jsonrpc = orig
        legs = A.STORE.timeline("run-ch")["legs"]
        self.assertEqual(legs[0]["chain"], "bsc")
        self.assertEqual(legs[1]["chain"], "bsc", "后腿 chain=bsc 不得被跨腿去重丢弃")
        self.assertIsNotNone(legs[1]["hkL1aMs"])


# ── R24：receipt 证据核对交易/块身份 ──


class TestR24ReceiptIdentity(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_evm_receipt_transaction_hash_checked(self):
        """R24：EVM receipt 必须 transactionHash≡本腿 hash 才采纳——不核身份的回执
        不得 settle（轮询至超时 → incomplete）。"""
        A.STORE.mark_open("run-ri")
        orig = A._jsonrpc
        to, iv = A.RECEIPT_POLL_TIMEOUT_S, A.RECEIPT_POLL_INTERVAL_S
        A.RECEIPT_POLL_TIMEOUT_S = 0.5
        A.RECEIPT_POLL_INTERVAL_S = 0.1
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "transactionHash": "0x" + "00" * 32,
        }
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                resp_body=json.dumps(
                    {
                        "code": "0",
                        "data": {"transactionHash": "0x" + "ab" * 32, "orderId": "o1"},
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            time.sleep(0.9)
        finally:
            A._jsonrpc = orig
            A.RECEIPT_POLL_TIMEOUT_S, A.RECEIPT_POLL_INTERVAL_S = to, iv
        leg = A.STORE.timeline("run-ri")["legs"][0]
        self.assertIsNone(leg["hkL3Ms"], "transactionHash 不匹配的回执不得 settle")
        self.assertEqual(leg["incomplete"], "receipt poll timeout")


# ── R25：Store 有界 retention ──


class TestR25BoundedStore(unittest.TestCase):
    def setUp(self):
        fresh()

    def _mk_run(self, rid):
        A.STORE.mark_open(rid)
        A.addons[0].request(
            FakeFlow(
                "web3.okx.com", "/priapi/v6/dx/trade/multi/broadcast", "POST", "{}"
            )
        )
        A.STORE.mark_close(rid)

    def test_bounded_retention_with_gone_marker(self):
        """R25：marks/legs/events 有界——超 retention 的已完成 run 回收，timeline
        返回明确 gone 标记而非空完整 timeline。"""
        orig_n = A.RETENTION_RUNS
        A.RETENTION_RUNS = 3
        try:
            for i in range(6):
                self._mk_run(f"run-rr{i}")
            # 保留最近 3 个已完成 run（rr3/rr4/rr5）；rr0/rr1/rr2 依序回收
            # （prune 在 mark_open 触发，rr2 在最后一次 open 时恰好仍是倒数第 4 个——
            # 保留窗为 rr3..rr5，故 gone 的是 rr0..rr2 中最老两个 + 再开一个触发）
            self._mk_run("run-rr6")
            self.assertTrue(
                A.STORE.timeline("run-rr0").get("gone"), "最老 run 须标 gone"
            )
            self.assertTrue(A.STORE.timeline("run-rr2").get("gone"))
            self.assertIsNone(A.STORE.timeline("run-rr6").get("gone"))
            self.assertEqual(
                len(A.STORE.timeline("run-rr6")["legs"]), 1, "最近 run 数据完整"
            )
            # 未见过的 run：保持空 timeline 形状（向后兼容），不标 gone
            tl = A.STORE.timeline("run-never")
            self.assertIsNone(tl.get("gone"))
            self.assertEqual(tl["legs"], [])
        finally:
            A.RETENTION_RUNS = orig_n

    def test_retention_pins_active_poller(self):
        """R25：有活动 poller 的腿 pin 住不回收；poller 结束后可被回收。"""
        orig_n = A.RETENTION_RUNS
        A.RETENTION_RUNS = 1
        try:
            A.STORE.mark_open("run-pin")
            leg = A.STORE.new_leg("run-pin", "okx", "bsc", {"t": 1.0, "tw": "x"})
            leg["receiptPolling"] = True  # 模拟 inflight poller
            A.STORE.mark_close("run-pin")
            for i in range(3):
                self._mk_run(f"run-q{i}")
            self.assertIsNone(
                A.STORE.timeline("run-pin").get("gone"), "活动 poller 腿须 pin 住"
            )
            leg["receiptPolling"] = False  # poller 结束
            self._mk_run("run-q3")
            self.assertTrue(
                A.STORE.timeline("run-pin").get("gone"), "poller 结束后可回收"
            )
        finally:
            A.RETENTION_RUNS = orig_n


# ── R07①：padre 成功/hash/id 必须同一 DONE 节点（跨节点拼接拒收）──


class TestR07SameEntryPadre(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_padre_hash_from_done_node_not_sibling(self):
        """合成帧 done:{DONE,txnHash=B,creationTime=now} + 兄弟 other:{txnHash=A}：
        成功判定节点的 hash 是 B——腿 hash 与 poller 目标都必须是 B（bug：ws_ids 整帧
        深度遍历先撞见兄弟节点的 A 并挂载，成功节点却是 B → 跨交易拼接出 L1a'）。
        R07 加严：B 还须正向匹配请求/响应建立的独立锚（响应抽取 txnHash=B）。"""
        import msgpack

        hb = "0x" + "cd" * 32
        ha = "0x" + "ef" * 32
        polled = []
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: polled.append(a)
        try:
            padre_sign_leg("run-p7", anchor_hash=hb)  # r8：锚注入（生产无 Padre 锚源）
            frame = msgpack.packb(
                [
                    373,
                    200,
                    {
                        "done": {
                            "txnStatus": "DONE",
                            "txnHash": hb,
                            "creationTime": int(time.time() * 1000),
                        },
                        "other": {"txnHash": ha},
                    },
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", frame))
            time.sleep(0.1)
        finally:
            A._poll_receipt = orig
        leg = A.STORE.timeline("run-p7")["legs"][0]
        self.assertIsNotNone(
            leg["hkL1aMs"], "DONE 节点同节点锚匹配 hash+新鲜时间戳 → 成功"
        )
        self.assertEqual(
            leg["txHash"],
            "0xcdcdcdcd…cdcdcd",
            "hash 必须取自 DONE 节点本身，不得跨节点拼接",
        )
        self.assertEqual(len(polled), 1)
        self.assertEqual(polled[0][3], hb, "receipt poller 必须轮询 DONE 节点的 hash")

    def test_padre_no_done_node_no_ids(self):
        """无 DONE 节点的帧不提供任何 id/hash（含同层 orderId/hash）——非 DONE 帧
        的 hash 不得挂载、不得触发 receipt 轮询。"""
        import msgpack

        padre_sign_leg("run-p7b")
        frame = msgpack.packb(
            [
                373,
                200,
                {
                    "txnStatus": "FILLED",
                    "txnHash": "0x" + "55" * 32,
                    "orderId": "po-1",
                    "creationTime": int(time.time() * 1000),
                },
            ]
        )
        self.assertEqual(
            A._padre_ws_ids(frame),
            {},
            "非 DONE 帧不提供任何 id/hash（含同层 orderId/hash）",
        )
        A.addons[0].websocket_message(ws_frame("backend3.padre.gg", frame))
        leg = A.STORE.timeline("run-p7b")["legs"][0]
        self.assertIsNone(leg["hkL1aMs"])
        self.assertIsNone(leg["txHash"], "非 DONE 帧的 hash 不得挂载")

    def test_padre_done_without_anchor_rejected(self):
        """R07 加严负例：腿无独立锚（响应未抽取到 id）——哪怕 DONE 帧同节点带合法
        hash + 新鲜时间戳也不得产 L1a'、不得挂载 hash、不得起 receipt 轮询
        （帧自带 id 先写腿再自证不算数；缺锚 → 缺失 HK 成功/hash 证据）。"""
        import msgpack

        polled = []
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: polled.append(a)
        try:
            padre_sign_leg("run-p7c")  # 真实 Turnkey 响应 = 无锚（生产路径）
            frame = msgpack.packb(
                [
                    373,
                    200,
                    {
                        "txnStatus": "DONE",
                        "txnHash": "0x" + "ab" * 32,
                        "creationTime": int(time.time() * 1000),
                    },
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", frame))
            time.sleep(0.1)
        finally:
            A._poll_receipt = orig
        leg = A.STORE.timeline("run-p7c")["legs"][0]
        self.assertIsNone(leg["hkL1aMs"], "无锚 DONE 不得算成功")
        self.assertIsNone(leg["txHash"], "无锚帧 hash 不得挂载")
        self.assertEqual(polled, [], "无锚帧不得触发 receipt 轮询")

    def test_padre_done_anchor_mismatch_rejected(self):
        """R07 加严负例：锚=H1 的腿收到 H2 的新鲜 DONE——关联门拒收：不得算成功、
        不得覆盖锚 hash；poller 始终只轮询锚锁定的 H1。"""
        import msgpack

        h1 = "0x" + "ab" * 32
        h2 = "0x" + "cd" * 32
        polled = []
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: polled.append(a)
        try:
            leg_int = padre_sign_leg(
                "run-p7d", anchor_hash=h1
            )  # r8：锚注入（生产无 Padre 锚源）
            # 锚注入不经 response() 的闩锁——按 R08 由 HTTP 入口同款闩锁起 poller（轮询锚 hash）
            ph = A.STORE.claim_receipt_poll(leg_int)
            if ph:
                A._poll_receipt(
                    "run-p7d", leg_int, leg_int.get("chain"), ph, A._jsonrpc
                )
            frame = msgpack.packb(
                [
                    373,
                    200,
                    {
                        "txnStatus": "DONE",
                        "txnHash": h2,
                        "creationTime": int(time.time() * 1000),
                    },
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", frame))
            time.sleep(0.1)
        finally:
            A._poll_receipt = orig
        leg = A.STORE.timeline("run-p7d")["legs"][0]
        self.assertIsNone(leg["hkL1aMs"], "锚不匹配的新鲜 DONE 不得算成功")
        self.assertEqual(leg["txHash"], "0xabababab…ababab", "冲突帧不得覆盖锚 hash")
        self.assertEqual(len(polled), 1)
        self.assertEqual(polled[0][3], h1, "poller 只轮询锚锁定的 hash")


# ── R07②：binance 同 entry 多 hash 字段归一不一致 → 整帧拒收 ──


class TestR07BinanceHashConsistency(unittest.TestCase):
    PO = "/bapi/defi/v2/private/wallet-direct/web-dex/place-order"

    def setUp(self):
        fresh()

    def _fin(self, oid, cid, **content_kw):
        c = {"status": "FINISHED", "orderId": oid, "clientOrderId": cid}
        c.update(content_kw)
        return ws_frame(
            "web3-stream.binance.com",
            json.dumps(
                {"stream": "w3pc:x", "data": {"bizKey": "DEX_ALL_ORDER", "content": c}}
            ),
        )

    def test_same_entry_hash_mismatch_rejects_whole_frame(self):
        """同一 content entry orderTxId=A/txHash=B 归一不一致 → 整帧拒收（hash 不挂载、
        FINISHED 不算成功、不起 receipt 轮询）——与 scripts/lib/binance-ws.mjs
        anchorMatchVerdict/extractOrderHistoryHash 的同节点归一 fail-closed 同律。"""
        A.STORE.mark_open("run-b7")
        rpc_calls = []
        orig = A._jsonrpc
        A._jsonrpc = lambda *a, **k: rpc_calls.append(a) and None
        try:
            f = FakeFlow(
                "web3.binance.com",
                self.PO,
                "POST",
                req_body=json.dumps({"clientOrderId": "c7", "chain": "BSC"}),
                resp_body=json.dumps(
                    {
                        "code": "000000",
                        "data": {"orderId": "oid-7", "clientOrderId": "c7"},
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            A.addons[0].websocket_message(
                self._fin(
                    "oid-7", "c7", orderTxId="0x" + "aa" * 32, txHash="0x" + "bb" * 32
                )
            )
            time.sleep(0.3)
            leg = A.STORE.timeline("run-b7")["legs"][0]
            self.assertIsNone(
                leg["txHash"], "归一不一致帧的 hash 不得挂载（bug：先选 orderTxId=A）"
            )
            self.assertIsNone(leg["hkL1aMs"], "归一不一致帧不得算成功")
            self.assertEqual(rpc_calls, [], "整帧拒收——不得起 receipt 轮询")
            # 正向控制：同 entry 多 hash 字段归一一致（大小写变体同值）→ 采纳+成功+轮询
            A._jsonrpc = lambda url, m, p, timeout=10: {
                "status": "0x1",
                "blockNumber": "0x1",
                "blockHash": "0x" + "99" * 32,
                "transactionHash": p[0],
            }
            A.addons[0].websocket_message(
                self._fin(
                    "oid-7",
                    "c7",
                    orderTxId="0x" + "cc" * 32,
                    txHash="0x" + "cC" * 16 + "cc" * 16,
                )
            )
            time.sleep(0.3)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-b7")["legs"][0]
        self.assertIsNotNone(leg["hkL1aMs"], "归一一致帧正常采纳")
        self.assertIsNotNone(leg["hkL3Ms"], "归一一致帧 hash → receipt 轮询")


# ── R09：建腿原子临界区（rid 选取+开窗复验+建腿同锁）──


class TestR09AtomicLegCreation(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_leg_creation_rejected_after_close(self):
        """hook 解析期间窗口被关（模拟控制线程 interleave）——关窗后到达的下单请求
        一律拒绝：不得给已关窗 run 建新腿，并计诊断计数。"""
        A.STORE.mark_open("run-r9")
        orig = A.RULES["okx"]["extract_chain"]

        def closing_extract(b):
            A.STORE.mark_close("run-r9")  # 控制线程在 get_text/解析期间关窗
            return "bsc"

        A.RULES["okx"]["extract_chain"] = closing_extract
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
            )
            A.addons[0].request(f)
        finally:
            A.RULES["okx"]["extract_chain"] = orig
        self.assertEqual(A.STORE.timeline("run-r9")["legs"], [], "关窗后不得创建新腿")
        self.assertEqual(A.STORE.diag.get("orderReqNoWindow"), 1, "拒绝须计诊断计数")

    def test_inflight_request_rejected_when_window_replaced(self):
        """R09 二轮（r4）：hook 入口捕获不可变窗口身份（runId+epoch），解析后只复验——
        解析期间旧窗被新窗顶掉 → 该请求一律拒绝（绝不重新选窗、不得借新窗建腿）；
        新窗只接纳其后到达的请求。"""
        A.STORE.mark_open("run-r9a")
        orig = A.RULES["okx"]["extract_chain"]

        def swapping_extract(b):
            A.STORE.mark_open("run-r9b")  # 新窗顶旧窗（旧窗关闭+冻结）
            return "bsc"

        A.RULES["okx"]["extract_chain"] = swapping_extract
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
            )
            A.addons[0].request(f)
        finally:
            A.RULES["okx"]["extract_chain"] = orig
        self.assertEqual(
            A.STORE.timeline("run-r9a")["legs"], [], "旧窗（已关）不得接新腿"
        )
        self.assertEqual(
            A.STORE.timeline("run-r9b")["legs"], [], "失效请求不得借新窗建腿"
        )
        self.assertEqual(A.STORE.diag.get("orderReqNoWindow"), 1, "拒绝须计诊断计数")
        # 新窗开后到达的请求正常入窗
        A.addons[0].request(
            FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
            )
        )
        self.assertEqual(
            len(A.STORE.timeline("run-r9b")["legs"]), 1, "新窗只接纳其后到达的请求"
        )

    def test_same_runid_reopen_invalidates_inflight_request(self):
        """R09 二轮（r4）：同 runId 关窗重开 → window_epoch+1——旧窗身份捕获的
        在飞请求解析后复验 epoch 失败，不得向重开后的窗建腿；重开后的新请求正常。"""
        A.STORE.mark_open("run-r9c")
        orig = A.RULES["okx"]["extract_chain"]

        def reopen_extract(b):
            A.STORE.mark_close("run-r9c")
            A.STORE.mark_open("run-r9c")  # 同 runId 重开 → epoch 递增
            return "bsc"

        A.RULES["okx"]["extract_chain"] = reopen_extract
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
            )
            A.addons[0].request(f)
        finally:
            A.RULES["okx"]["extract_chain"] = orig
        self.assertEqual(
            A.STORE.timeline("run-r9c")["legs"],
            [],
            "旧 epoch 捕获的请求不得借重开窗建腿",
        )
        self.assertEqual(
            A.STORE.diag.get("orderReqNoWindow"), 1, "epoch 失效拒绝须计诊断计数"
        )
        A.addons[0].request(
            FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
            )
        )
        self.assertEqual(
            len(A.STORE.timeline("run-r9c")["legs"]), 1, "重开后到达的请求正常入窗"
        )


# ── R18：热重载跨实例 Store 交接（dropped 标记 + health 暴露）──


class TestR18StoreHandover(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_takeover_marks_dropped_runs(self):
        """旧实例活动 run 不随热重载交接——新实例对「曾存在但本地没有」的 run 须标
        gone+droppedInReload（区别于 retention 回收与从未见过）；/health 暴露接管事实。"""
        code, _ = _http_json("POST", "http://127.0.0.1:8072/mark", {"runId": "run-ho1"})
        self.assertEqual(code, 200)
        A.addons[0].request(
            FakeFlow(
                "web3.okx.com", "/priapi/v6/dx/trade/multi/broadcast", "POST", "{}"
            )
        )
        self.assertEqual(len(A.STORE.timeline("run-ho1")["legs"]), 1)
        # 模拟热重载：注册表留下旧实例（不同 instanceId）的 knownRuns；新实例 = 空 Store
        reg = A._ctrl_registry()
        reg["knownRuns"] = {
            "instanceId": "old-instance-x",
            "runs": ["run-ho1"],
            "at": "t",
        }
        fresh()
        A._start_ctrl()  # 接管：停旧控制面 + rebind + 捕获 droppedRuns
        try:
            tl = A.STORE.timeline("run-ho1")
            self.assertTrue(tl.get("gone"), "旧实例 run 应标 gone")
            self.assertTrue(
                tl.get("droppedInReload"),
                "须区分 dropped（重载丢失）与从未见过/retention 回收",
            )
            self.assertIsNone(
                A.STORE.timeline("run-never").get("droppedInReload"),
                "从未见过的 run 不标 dropped",
            )
            code, body = _http_json("GET", "http://127.0.0.1:8072/health")
            self.assertEqual(code, 200)
            tk = body.get("takeover")
            self.assertTrue(
                tk and tk.get("droppedRuns") >= 1,
                "/health 须暴露「接管自旧实例，N 个 run 被丢弃」",
            )
            self.assertIn("run-ho1", tk.get("runIds") or [])
        finally:
            reg["takeover"] = None  # 不污染后续测试


# ── R20：gmgn https-warm 须真 keep-alive 热样本（与 EU httpsWarmRtt 同口径）──


class TestR20GmgnWarmProbe(unittest.TestCase):
    def test_https_warm_keepalive_second_request_not_halved(self):
        """同一 keep-alive 连接预热 1 次 + 第 2 次热请求整 RTT（ms=rttMs 不减半）——
        此前 urllib 单次冷连接请求冒充 https-warm，名不副实。"""
        import time as _t

        class FakeConn:
            instances = 0

            def __init__(self, host, timeout=None):
                FakeConn.instances += 1
                self.requests = 0

            def request(self, method, path, headers=None):
                self.requests += 1
                _t.sleep(0.1)

            def getresponse(self):
                return types.SimpleNamespace(read=lambda *a: b"{}")

            def close(self):
                pass

        FakeConn.instances = 0
        holder = {}

        def factory(host, timeout=None):
            c = FakeConn(host, timeout=timeout)
            holder["conn"] = c
            return c

        r = A._probe_https_warm(
            "gmgn.ai", "/defi/quotation/v1/chains", conn_cls=factory
        )
        self.assertIsNotNone(r)
        self.assertEqual(r["method"], "https-warm")
        self.assertEqual(FakeConn.instances, 1, "预热+热样本须同一连接")
        self.assertEqual(
            holder["conn"].requests, 2, "1 预热 + 1 热样本（EU httpsWarmRtt 同口径）"
        )
        self.assertEqual(r["ms"], r["rttMs"], "https-warm 整 RTT 不减半")
        self.assertGreaterEqual(
            r["rttMs"], 80, "单次 0.1s → 热样本 ~100ms；减半/冷样本口径给不出"
        )


# ── R21/R29：timeline 方法版本 + 采集环境身份（vantage 推导）──


class TestR21R29TimelineIdentity(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_timeline_method_and_identity(self):
        """timeline 自带 method=hook-observed@v2 + 五件采集环境身份（vantage/
        instanceId/addonVersion/mitmproxyVersion/pythonVersion——client
        hkTimingEnvironment 取 timeline 优先）；vantage 由 /egress 实测 countryCode
        推导，未知不发明（unknown-proxy），timeline 拉取路径不得为打标签发网。"""
        import platform as _pf

        orig_get = A.get_egress
        A.get_egress = lambda: (_ for _ in ()).throw(
            AssertionError("timeline 不得为 vantage 发网")
        )
        try:
            A.STORE.mark_open("run-id1")
            tl = A.STORE.timeline("run-id1")
            self.assertEqual(tl["method"], "hook-observed@v2")
            self.assertEqual(tl["instanceId"], A.INSTANCE_ID)
            self.assertEqual(tl["addonVersion"], A.ADDON_VERSION)
            self.assertEqual(
                tl["pythonVersion"],
                _pf.python_version(),
                "与 /health build manifest 同源",
            )
            self.assertEqual(
                tl["mitmproxyVersion"],
                A._mitmproxy_version(),
                "键必须存在（离线环境值可为 null）",
            )
            self.assertEqual(
                tl["vantage"], "unknown-proxy", "无实测 egress → unknown-proxy"
            )
            A._EGRESS_CACHE["data"] = {"countryCode": "SG", "label": "Singapore"}
            A._EGRESS_CACHE["at"] = time.monotonic()
            self.assertEqual(
                A.STORE.timeline("run-id1")["vantage"],
                "sg-proxy",
                "实测 countryCode → 小写-proxy",
            )
            # gone 响应同样带身份字段
            A.STORE.gone.append("run-gone1")
            gl = A.STORE.timeline("run-gone1")
            self.assertEqual(gl["method"], "hook-observed@v2")
            self.assertEqual(gl["vantage"], "sg-proxy")
            self.assertEqual(gl["pythonVersion"], _pf.python_version())
            self.assertIn("mitmproxyVersion", gl)
        finally:
            A.get_egress = orig_get
            A._EGRESS_CACHE["data"] = None
            A._EGRESS_CACHE["at"] = 0

    def test_latency_vantage_derived_from_egress(self):
        """/latency vantage 不再硬编码 hk-local——由 /egress countryCode 推导。"""
        orig_get = A.get_egress
        saved = {}
        for fn in (
            "_probe_https_warm",
            "_probe_okx_ws",
            "_probe_padre_ws",
            "_probe_http_ping",
            "_probe_fomo_ws",
        ):
            saved[fn] = getattr(A, fn)
            setattr(A, fn, lambda *a, **k: None)
        try:
            A.get_egress = lambda: {"countryCode": "HK", "label": "Hong Kong"}
            self.assertEqual(A._collect_latency()["vantage"], "hk-proxy")
            A.get_egress = lambda: None
            self.assertEqual(A._collect_latency()["vantage"], "unknown-proxy")
        finally:
            A.get_egress = orig_get
            for fn, v in saved.items():
                setattr(A, fn, v)

    def test_egress_country_code_cached(self):
        """get_egress 必须缓存 countryCode（vantage 推导源；文档保留网段地址）。"""

        class R:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return json.dumps(
                    {
                        "status": "success",
                        "query": "203.0.113.7",
                        "country": "Hong Kong",
                        "countryCode": "HK",
                        "regionName": "HK",
                        "city": "HK",
                        "isp": "doc-test-net",
                    }
                ).encode()

        orig = A.urllib.request.urlopen
        A.urllib.request.urlopen = lambda *a, **k: R()
        A._EGRESS_CACHE["data"] = None
        A._EGRESS_CACHE["at"] = 0
        try:
            eg = A.get_egress()
            self.assertEqual(eg["countryCode"], "HK")
            self.assertEqual(A._vantage(), "hk-proxy")
        finally:
            A.urllib.request.urlopen = orig
            A._EGRESS_CACHE["data"] = None
            A._EGRESS_CACHE["at"] = 0


# ── R22：fomo 预览腿 anchorRole + attempted 口径 + 单窗预算 ──


class TestR22PreviewRoleAndBudgets(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_fomo_preview_leg_not_attempted_until_executed(self):
        """FOMO /swaps/v2 = 点击前预览（L7 路径/体不可区分执行）——腿先标
        anchorRole=preview：不计 attempted、不消费 manifest 占位；Relay success 帧
        正向匹配 requestId≡relaySwapId（=执行证据，Relay 只对已执行请求推 success）
        才原子晋升 order 并计 attempted。"""
        A.STORE.mark_open("run-fp", manifest={"rounds": 1})
        rid = "0x" + "9a" * 32
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        try:
            # 两次预览（键入/报价刷新页面自发）——均不得计 attempted
            for _ in range(2):
                f = FakeFlow(
                    "prod-api.fomo.family",
                    "/swaps/v2",
                    "POST",
                    resp_body=json.dumps(
                        {
                            "success": True,
                            "responseObject": {
                                "v2Swap": {
                                    "relaySwapId": rid,
                                    "destinationChainId": 4663,
                                }
                            },
                        }
                    ),
                )
                A.addons[0].request(f)
                A.addons[0].response(f)
            tl = A.STORE.timeline("run-fp")
            self.assertEqual(
                tl["counts"]["attempted"],
                0,
                "预览不计 attempted（bug：每个预览 POST 都当下单 attempt）",
            )
            previews = [x for x in tl["legs"] if x["platform"] == "fomo"]
            self.assertTrue(
                previews and all(x.get("anchorRole") == "preview" for x in previews),
                "fomo 腿须标 anchorRole=preview",
            )
            self.assertEqual(
                len(tl["legs"]), 3, "2 预览腿 + 1 manifest 占位（预览不消费授权轮次）"
            )
            # 执行证据：Relay success 正向匹配最新预览腿 → 晋升 order + L1a'
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "event": "request.status.updated",
                            "data": {
                                "status": "success",
                                "requestId": rid,
                                "txHashes": ["0x" + "12" * 32],
                                "destinationChainId": 4663,
                            },
                        }
                    ),
                )
            )
            time.sleep(0.3)
        finally:
            A._jsonrpc = orig
        tl = A.STORE.timeline("run-fp")
        self.assertEqual(tl["counts"]["attempted"], 1, "执行证据落地后计 1 次下单")
        self.assertEqual(tl["counts"]["observed"], 1)
        exec_legs = [x for x in tl["legs"] if x.get("anchorRole") == "order"]
        self.assertEqual(len(exec_legs), 1)
        self.assertIsNotNone(exec_legs[0]["hkL1aMs"])
        self.assertEqual(len(tl["legs"]), 2, "晋升后 manifest 占位被 order 腿消费")

    def test_window_budgets_legs_and_events(self):
        """单 run 腿/事件预算——超限截断并标 overflow（内存有界；防预览风暴）。"""
        orig_l, orig_e = A.MAX_LEGS_PER_RUN, A.MAX_EVENTS_PER_RUN
        try:
            A.MAX_LEGS_PER_RUN, A.MAX_EVENTS_PER_RUN = 3, 100
            A.STORE.mark_open("run-bud")
            for _ in range(5):
                A.addons[0].request(
                    FakeFlow(
                        "web3.okx.com",
                        "/priapi/v6/dx/trade/multi/broadcast",
                        "POST",
                        "{}",
                    )
                )
            tl = A.STORE.timeline("run-bud")
            self.assertEqual(len(tl["legs"]), 3, "腿按预算截断")
            self.assertEqual(
                (tl.get("overflow") or {}).get("legs"), 2, "超出腿数如实标 overflow"
            )
            A.MAX_LEGS_PER_RUN, A.MAX_EVENTS_PER_RUN = 100, 2
            A.STORE.mark_open("run-bud2")
            for _ in range(4):
                A.addons[0].request(
                    FakeFlow(
                        "web3.okx.com",
                        "/priapi/v6/dx/trade/multi/broadcast",
                        "POST",
                        "{}",
                    )
                )
            tl2 = A.STORE.timeline("run-bud2")
            self.assertEqual(
                (tl2.get("overflow") or {}).get("events"),
                2,
                "超出事件数如实标 overflow",
            )
        finally:
            A.MAX_LEGS_PER_RUN, A.MAX_EVENTS_PER_RUN = orig_l, orig_e


# ── R22 三轮：授权槽绑定 / unbound 诊断 / padre sign 角色 / 关窗缺失补写 ──


class TestR22SlotBinding(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_duplicate_same_order_request_merges_into_slot(self):
        """B/R22 复现：Binance/BSC 授权 1 槽、同订单两次 place-order 请求。
        旧→新断言映射（R22 四轮①，r6）：r5 时同单第二次请求落 unbound=1；r6 起
        正向同单（clientOrderId/orderId 匹配已绑槽腿）归入同一授权槽——重复请求标
        dup 诊断（round=None，不占新槽、不新增 attempted、不计 unbound），
        diag.orderReqDuplicate 计数。「身份缺失/冲突的重复保持 unbound 不猜」由
        TestR22OrderIdentityDedup 保留。安全性质不变：attempted 恒为 1。"""
        po = "/bapi/defi/v2/private/wallet-direct/web-dex/place-order"
        A.STORE.mark_open(
            "run-dup",
            manifest={
                "version": 1,
                "runId": "run-dup",
                "rounds": 1,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 1}],
            },
        )
        for _ in range(2):  # 同订单两次请求（产品重试/重复派发）
            f = FakeFlow(
                "web3.binance.com",
                po,
                "POST",
                req_body=json.dumps({"clientOrderId": "cid-dup", "chain": "BSC"}),
                resp_body=json.dumps(
                    {
                        "code": "000000",
                        "data": {"orderId": "oid-dup", "clientOrderId": "cid-dup"},
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
        tl = A.STORE.timeline("run-dup")
        self.assertEqual(
            tl["counts"],
            {"requested": 1, "attempted": 1, "observed": 0, "unbound": 0},
            "同单重复请求归入同一授权槽——不冒充第二次授权尝试（更早旧 bug：attempted=2）",
        )
        real = [x for x in tl["legs"] if x["anchorRole"] is not None]
        self.assertEqual(len(real), 2, "两条请求腿都留作证据")
        bound = [x for x in real if not x["dup"]]
        dup = [x for x in real if x["dup"]]
        self.assertEqual(len(bound), 1)
        self.assertEqual(
            bound[0]["round"],
            None,
            "r7：绑定只证明槽容量——授权轮号不由槽位序推导（null=未知）",
        )
        self.assertIsNotNone(bound[0]["observationIndex"], "到达序照常记录")
        self.assertIsNone(dup[0]["round"], "重复腿无授权轮次（不占新槽）")
        self.assertFalse(dup[0]["unbound"], "正向同单不是 unbound——是 dup 诊断")
        self.assertEqual(
            len([x for x in tl["legs"] if x["incomplete"] == "no-order-observed"]),
            0,
            "槽已被首请求消费——不再有缺失占位",
        )
        self.assertEqual(
            A.STORE.diag.get("orderReqDuplicate", 0), 1, "同单重复须留诊断计数"
        )
        self.assertEqual(
            A.STORE.diag.get("orderReqUnbound", 0), 0, "正向同单不再误记 unbound 诊断"
        )

    def test_unknown_chain_request_cannot_claim_chain_slot(self):
        """B/R22：chain 未知的请求不得吞掉某链的缺失占位（旧贪心配对会把 chain=None
        的腿配给任一 chain spec）。binance 授权 bsc/solana 各 1 槽，只来 1 个 chain
        不可解析的请求 → unbound 诊断，两个授权槽均保留缺失占位。"""
        po = "/bapi/defi/v2/private/wallet-direct/web-dex/place-order"
        A.STORE.mark_open(
            "run-uc",
            manifest={
                "version": 1,
                "runId": "run-uc",
                "rounds": 1,
                "legs": [
                    {"platform": "binance", "chain": "bsc", "rounds": 1},
                    {"platform": "binance", "chain": "solana", "rounds": 1},
                ],
            },
        )
        f = FakeFlow(
            "web3.binance.com",
            po,
            "POST",
            req_body=json.dumps({"clientOrderId": "cid-x"}),  # 无 chain 字段
            resp_body=json.dumps(
                {
                    "code": "000000",
                    "data": {"orderId": "oid-x", "clientOrderId": "cid-x"},
                }
            ),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        self.assertTrue(A.STORE.legs["run-uc"][0]["pendingBind"])
        A.STORE.mark_close("run-uc")
        tl = A.STORE.timeline("run-uc")
        self.assertEqual(
            tl["counts"],
            {"requested": 2, "attempted": 0, "observed": 0, "unbound": 1},
            "chain 未证明——不计任何链槽的 attempted",
        )
        ph = [x for x in tl["legs"] if x["incomplete"] == "no-order-observed"]
        self.assertEqual(len(ph), 2, "chain 未知的请求不得吞占位——两槽均如实缺失")
        self.assertEqual({x["chain"] for x in ph}, {"bsc", "solana"})

    def test_padre_sign_role_not_attempted_until_done(self):
        """B/R22：Padre sign_raw_payload 未证明与 order 一一对应——腿先标
        anchorRole=sign（不计 attempted、不占授权槽）；同 DONE 节点合法 hash +
        独立锚正向匹配 + 新鲜源时间 = 执行证据 → 原子晋升 order 并绑槽。"""
        import msgpack

        h = "0x" + "cd" * 32
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: (
            None
        )  # 本例不验 receipt——成功推送即 observed
        try:
            # r8：锚注入（生产今日无 Padre 锚源——真实 Turnkey 响应无身份，见 test_padre_msgpack_done）
            padre_sign_leg(
                "run-sign",
                anchor_hash=h,
                req_body=json.dumps({"data": {"chainId": "56"}}),
                manifest={
                    "version": 1,
                    "runId": "run-sign",
                    "rounds": 1,
                    "legs": [{"platform": "padre", "chain": "bsc", "rounds": 1}],
                },
            )
            tl0 = A.STORE.timeline("run-sign")
            self.assertEqual(
                tl0["legs"][0]["anchorRole"],
                "sign",
                "签名调用先标 sign（未证明一一对应 order）",
            )
            self.assertEqual(
                tl0["counts"],
                {"requested": 1, "attempted": 0, "observed": 0, "unbound": 0},
                "sign 腿不计 attempted",
            )
            self.assertEqual(len(tl0["legs"]), 2, "sign 腿不消费槽位——缺失占位保留")
            frame = msgpack.packb(
                [
                    373,
                    200,
                    {
                        "txnStatus": "DONE",
                        "txnHash": h,
                        "creationTime": int(time.time() * 1000),
                    },
                ]
            )
            A.addons[0].websocket_message(ws_frame("backend3.padre.gg", frame))
        finally:
            A._jsonrpc = orig
        tl = A.STORE.timeline("run-sign")
        self.assertEqual(
            tl["counts"],
            {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0},
            "DONE 执行证据落地后计 1 次下单",
        )
        leg = [x for x in tl["legs"] if x["anchorRole"] == "order"][0]
        self.assertEqual(
            leg["round"],
            None,
            "r7：晋升绑槽不写授权轮号（到达序≠授权轮；唯一关联由 EU 侧回配 authorizedRound）",
        )
        self.assertFalse(leg["unbound"])
        self.assertEqual(len(tl["legs"]), 1, "晋升消费槽位——占位归零")
        self.assertIsNotNone(leg["hkL1aMs"])

    def test_padre_sign_without_done_gets_no_execution_proof(self):
        """sign 腿始终无 DONE 执行证据 → 关窗后 incomplete=no-execution-proof，
        授权槽占位保留（no-order-observed），attempted 恒 0。"""
        padre_sign_leg(
            "run-sx",
            manifest={
                "version": 1,
                "runId": "run-sx",
                "rounds": 1,
                "legs": [{"platform": "padre", "chain": "bsc", "rounds": 1}],
            },
        )
        A.STORE.mark_close("run-sx")
        tl = A.STORE.timeline("run-sx")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 0, "observed": 0, "unbound": 0}
        )
        sign_leg = tl["legs"][0]
        self.assertEqual(sign_leg["anchorRole"], "sign")
        self.assertEqual(
            sign_leg["incomplete"],
            "no-execution-proof",
            "无执行证据的 sign 腿关窗后须如实标注（旧 bug：incomplete=null）",
        )
        self.assertEqual(
            tl["legs"][1]["incomplete"], "no-order-observed", "授权槽占位仍在"
        )

    def test_window_close_backfills_missing_states(self):
        """B/R22 复现②：关窗不补缺失状态（无结果腿 L1a/L3=null 而 incomplete=null）。
        现：关窗后 order 腿无成功观察 → no-success-observed；有成功无 receipt →
        no-receipt-observed；未晋升 preview 腿 → no-execution-proof。开窗中不补
        （仍在采集——incomplete=null 是正确的进行态）。"""
        A.STORE.mark_open(
            "run-ms",
            manifest={
                "version": 1,
                "runId": "run-ms",
                "rounds": 2,
                "legs": [{"platform": "okx", "chain": "bsc", "rounds": 2}],
            },
        )
        h1 = "0x" + "ab" * 32
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: None  # 无 receipt——poller 落空
        try:
            # 腿1：无任何结果
            A.addons[0].request(
                FakeFlow(
                    "web3.okx.com",
                    "/priapi/v6/dx/trade/multi/broadcast",
                    "POST",
                    req_body=json.dumps({"data": {"chainId": "56"}}),
                    resp_body="{}",
                )
            )
            # 腿2：有成功无 receipt
            f2 = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                req_body=json.dumps({"data": {"chainId": "56"}}),
                resp_body=json.dumps(
                    {"code": "0", "data": {"transactionHash": h1, "orderId": "o2"}}
                ),
            )
            A.addons[0].request(f2)
            A.addons[0].response(f2)
            A.addons[0].websocket_message(
                ws_frame(
                    "wsdexpri.okx.com",
                    json.dumps(
                        {
                            "arg": {"channel": "dex-swap-order-info"},
                            "data": {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "o2",
                                    "transactionHash": h1,
                                }
                            },
                        }
                    ),
                )
            )
            open_tl = A.STORE.timeline("run-ms")
            self.assertIsNone(
                open_tl["legs"][0]["incomplete"], "开窗中仍在采集——不补缺失状态"
            )
            A.STORE.mark_close("run-ms")
        finally:
            A._jsonrpc = orig
        tl = A.STORE.timeline("run-ms")
        self.assertEqual(
            tl["legs"][0]["incomplete"],
            "no-success-observed",
            "关窗后无结果腿须补 no-success",
        )
        self.assertEqual(
            tl["legs"][1]["incomplete"],
            "no-receipt-observed",
            "有成功无 receipt 须补 no-receipt",
        )
        self.assertIsNotNone(tl["legs"][1]["hkL1aMs"], "成功观察不受影响")
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 2, "observed": 1, "unbound": 0}
        )

    def test_unpromoted_preview_close_marked_no_execution_proof(self):
        """fomo 预览腿未获 Relay 执行证据 → 关窗后 no-execution-proof（含 F-SOL
        同链腿 known limitation 的如实出口）；预览仍不消费授权槽。"""
        A.STORE.mark_open(
            "run-pv",
            manifest={
                "version": 1,
                "runId": "run-pv",
                "rounds": 1,
                "legs": [{"platform": "fomo", "chain": "robinhood", "rounds": 1}],
            },
        )
        A.addons[0].request(
            FakeFlow("prod-api.fomo.family", "/swaps/v2", "POST", "", "{}")
        )
        A.STORE.mark_close("run-pv")
        tl = A.STORE.timeline("run-pv")
        self.assertEqual(tl["legs"][0]["anchorRole"], "preview")
        self.assertEqual(tl["legs"][0]["incomplete"], "no-execution-proof")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 0, "observed": 0, "unbound": 0}
        )
        self.assertEqual(
            tl["legs"][1]["incomplete"], "no-order-observed", "预览不占槽——占位保留"
        )

    def test_freeze_backfill_is_terminal(self):
        """关窗补写在输出层确定性成立：重复拉取一致；已有 incomplete（poll 失败写入）
        不被补写覆盖；内部腿态不被 timeline 修改（R25 pin 语义不受影响）。"""
        A.STORE.mark_open("run-tm", manifest={"rounds": 1})
        A.addons[0].request(
            FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                "{}",
                "{}",
            )
        )
        leg = A.STORE.legs["run-tm"][0]
        A._settle_incomplete(leg, "receipt poll timeout")  # 先写入的 poll 失败原因
        A.STORE.mark_close("run-tm")
        tl1 = A.STORE.timeline("run-tm")
        tl2 = A.STORE.timeline("run-tm")
        self.assertEqual(
            tl1["legs"][0]["incomplete"],
            "receipt poll timeout",
            "已有 incomplete 不被补写覆盖",
        )
        self.assertEqual(
            tl1["legs"][0]["incomplete"],
            tl2["legs"][0]["incomplete"],
            "重复拉取补写一致",
        )
        self.assertEqual(leg.get("incomplete"), "receipt poll timeout")
        # 对照：无既有 incomplete 的冻结腿补写在输出层，内部态不变
        A.STORE.mark_open("run-tm2")
        A.addons[0].request(
            FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                "{}",
                "{}",
            )
        )
        leg2 = A.STORE.legs["run-tm2"][0]
        A.STORE.mark_close("run-tm2")
        self.assertEqual(
            A.STORE.timeline("run-tm2")["legs"][0]["incomplete"], "no-success-observed"
        )
        self.assertIsNone(leg2.get("incomplete"), "输出层补写不改内部腿态")


# ── R24：块身份证据门（缺 blockHash/blockNumber/slot → L3 null + incomplete）──


class TestR24BlockIdentityGate(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_evm_receipt_missing_block_identity_incomplete(self):
        """EVM：status=1 + transactionHash 匹配但缺 blockHash/blockNumber = 证据不足
        ——L3 留 null + incomplete='receipt-missing-block-identity'（不猜）。"""
        A.STORE.mark_open("run-r24")
        orig = A._jsonrpc
        to, iv = A.RECEIPT_POLL_TIMEOUT_S, A.RECEIPT_POLL_INTERVAL_S
        A.RECEIPT_POLL_TIMEOUT_S = 1.0
        A.RECEIPT_POLL_INTERVAL_S = 0.1
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "transactionHash": p[0],
        }  # 无块身份
        try:
            f = FakeFlow(
                "web3.okx.com",
                "/priapi/v6/dx/trade/multi/broadcast",
                "POST",
                resp_body=json.dumps(
                    {
                        "code": "0",
                        "data": {"transactionHash": "0x" + "ab" * 32, "orderId": "o1"},
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            time.sleep(0.6)
        finally:
            A._jsonrpc = orig
            A.RECEIPT_POLL_TIMEOUT_S, A.RECEIPT_POLL_INTERVAL_S = to, iv
        leg = A.STORE.timeline("run-r24")["legs"][0]
        self.assertIsNone(leg["hkL3Ms"], "缺块身份的 receipt 不得 settle L3")
        self.assertEqual(leg["incomplete"], "receipt-missing-block-identity")

    def test_sol_status_missing_slot_incomplete(self):
        """Sol：confirmed+err=null 但缺 slot = 缺块身份——同门。"""
        A.STORE.mark_open("run-r24s")
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "value": [{"confirmationStatus": "confirmed", "err": None}]
        }  # 无 slot
        sig = "5" * 88
        try:
            f = FakeFlow(
                "gmgn.ai",
                "/tapi/v1/swap_batch_order",
                "POST",
                resp_body=json.dumps({"data": {"hash": sig, "orderId": "g1"}}),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
            time.sleep(0.6)
        finally:
            A._jsonrpc = orig
        leg = A.STORE.timeline("run-r24s")["legs"][0]
        self.assertIsNone(leg["hkL3Ms"], "缺 slot 的 Sol 状态不得 settle L3")
        self.assertEqual(leg["incomplete"], "receipt-missing-block-identity")


# ── P3 清理：padre 死分支 / _classify_ws 点边界 ──


class TestP3Cleanup(unittest.TestCase):
    def setUp(self):
        fresh()

    def test_padre_done_node_strict_only(self):
        """宽松终态词表（_PADRE_DONE_EQ/filled/completed/…）无生产调用且会让非 DONE
        帧冒充成功——已删除；_padre_done_node 只剩严格 txnStatus=='DONE'。"""
        node = A._padre_done_node({"txnStatus": "DONE", "txnHash": "0x" + "ab" * 32})
        self.assertIsNotNone(node)
        self.assertIsNone(
            A._padre_done_node({"orderStatus": "filled"}),
            "宽松词表反例：filled 不得当 DONE",
        )
        self.assertFalse(hasattr(A, "_PADRE_DONE_EQ"), "死词表已删除")
        with self.assertRaises(TypeError):
            A._padre_done_node({}, strict=False)  # 宽松分支参数已随死代码删除

    def test_classify_ws_dot_boundary(self):
        """_classify_ws = exact host 或 '.'+host 后缀边界——裸 endswith 会把
        notnbstream.binance.com 这类同后缀异主机误分类进 binance。"""
        self.assertIsNone(A._classify_ws("notnbstream.binance.com"), "无点边界误分类")
        self.assertIsNone(A._classify_ws("evilwsdexpri.okx.com"))
        self.assertEqual(A._classify_ws("nbstream.binance.com"), "binance")
        self.assertEqual(A._classify_ws("x.wsdexpri.okx.com"), "okx", "合法子域仍命中")


# ── §7.3：Padre fresh DONE 脱敏真实帧夹具（两路径区分 + 父/子时间字段 DONE 子树边界）──


class TestP3PadreFreshDoneFixture(unittest.TestCase):
    """§7.3 证据补足：按文档化的真实帧形状做脱敏离线夹具——msgpack _multiplex
    信封 [373, 200, payload]，payload[2] 即直接交易节点（runner-core.mjs 帧形状注释、
    docs/hk-proxy-timing.md「准确性硬门」）；重放帧时间字段 creationTime/firstUserClickMs
    见 k2x54 复盘。两道门各自独立、缺一不可：①请求/响应独立锚的 ID 正相关；
    ②同一 DONE 节点子树内的新鲜源时间。「时间兜底」路径不存在——无锚帧仅凭新鲜
    时间不得自证关联。父/子时间字段只在同一 DONE 节点子树内生效（根/父节点的
    新鲜 timestamp 不给 DONE 子树背书）。hash 全合成，无真实批数据。
    N=1 实批属另行授权采集，本夹具不覆盖。"""

    def setUp(self):
        fresh()
        self.polled = []
        self._orig_poll = A._poll_receipt
        A._poll_receipt = lambda *a, **k: self.polled.append(a)

    def tearDown(self):
        A._poll_receipt = self._orig_poll

    @staticmethod
    def _frame(payload):
        import msgpack

        return ws_frame("backend3.padre.gg", msgpack.packb([373, 200, payload]))

    @staticmethod
    def _open_leg(run, anchor_hash):
        """Turnkey sign 请求 + 真实形状响应（无锚）；anchor_hash 非空时直接注入独立锚
        （r8：生产今日无 Padre 锚源，注入只为覆盖 DONE 门边界）；None = 生产现状（无锚）。"""
        padre_sign_leg(run, anchor_hash=anchor_hash)

    def test_first_user_click_ms_fresh_accept_stale_reject(self):
        """k2x54 重放帧字段 firstUserClickMs 与 creationTime 同为新鲜源时间：
        锚正相关 + 新鲜 firstUserClickMs → 采纳；锚正相关 + 6.7h 前（fszyx 重放量级）
        → 拒（正相关不绕过陈旧门，两路径都过时间门）。"""
        h = "0x" + "ab" * 32
        now_ms = int(time.time() * 1000)
        # 锚正相关 + 陈旧 firstUserClickMs → 拒
        self._open_leg("run-f1", h)
        A.addons[0].websocket_message(
            self._frame(
                {
                    "txnStatus": "DONE",
                    "txnHash": h,
                    "firstUserClickMs": now_ms - 6.7 * 3600_000,
                }
            )
        )
        self.assertIsNone(
            A.STORE.timeline("run-f1")["legs"][0]["hkL1aMs"],
            "陈旧 firstUserClickMs 重放不得算成功",
        )
        A.STORE.mark_close("run-f1")
        # 锚正相关 + 新鲜 firstUserClickMs → 采纳
        self._open_leg("run-f2", h)
        A.addons[0].websocket_message(
            self._frame({"txnStatus": "DONE", "txnHash": h, "firstUserClickMs": now_ms})
        )
        leg = A.STORE.timeline("run-f2")["legs"][0]
        self.assertIsNotNone(leg["hkL1aMs"], "锚正相关+新鲜 firstUserClickMs 正常采纳")
        self.assertEqual(leg["txHash"], "0xabababab…ababab")

    def test_no_time_fallback_without_anchor(self):
        """「时间兜底」路径不存在的钉死：腿无独立锚（响应未抽取 id）——DONE 帧同节点
        合法 hash + 新鲜 firstUserClickMs 也整帧拒收；新鲜时间永不自证关联。"""
        self._open_leg("run-f3", None)
        now_ms = int(time.time() * 1000)
        A.addons[0].websocket_message(
            self._frame(
                {
                    "txnStatus": "DONE",
                    "txnHash": "0x" + "cd" * 32,
                    "firstUserClickMs": now_ms,
                }
            )
        )
        leg = A.STORE.timeline("run-f3")["legs"][0]
        self.assertIsNone(leg["hkL1aMs"], "无锚：新鲜时间兜底不得充关联")
        self.assertIsNone(leg["txHash"])
        self.assertEqual(self.polled, [], "无锚帧不得触发 receipt 轮询")

    def test_time_fields_scoped_to_done_subtree(self):
        """父/子时间字段只在同一 DONE 节点子树内生效：
        ①子生效——新鲜 creationTime 在 DONE 节点的嵌套子节点内 → 采纳；
        ②父不生效——DONE 子树无时间字段、仅根/父节点带新鲜 timestamp → 拒
        （根新 timestamp 不得给无源时间的 DONE 子树背书）。"""
        h = "0x" + "88" * 32
        now_ms = int(time.time() * 1000)
        # ①DONE 节点自身无时间字段、嵌套子节点 exec.creationTime 新鲜 → 采纳
        self._open_leg("run-f4", h)
        A.addons[0].websocket_message(
            self._frame(
                {"txnStatus": "DONE", "txnHash": h, "exec": {"creationTime": now_ms}}
            )
        )
        leg = A.STORE.timeline("run-f4")["legs"][0]
        self.assertIsNotNone(leg["hkL1aMs"], "DONE 子树内嵌套子节点的新鲜时间字段生效")
        A.STORE.mark_close("run-f4")
        # ②根节点新鲜 timestamp + DONE 子树全无时间字段 → 拒
        self._open_leg("run-f5", h)
        A.addons[0].websocket_message(
            self._frame(
                {"timestamp": now_ms, "sub": {"txnStatus": "DONE", "txnHash": h}}
            )
        )
        leg = A.STORE.timeline("run-f5")["legs"][0]
        self.assertIsNone(
            leg["hkL1aMs"], "DONE 子树外（根/父）的新鲜 timestamp 不得背书"
        )
        # 腿 txHash 是响应锚合法写入，不能据此区分帧挂载——「拒收帧不挂 hash」由
        # test_no_time_fallback_without_anchor（无锚腿）钉死

    def test_creation_time_seconds_epoch_converted(self):
        """creationTime 秒级 epoch（>1e9 且 <1e12）按 ×1000 归一——新鲜秒级帧正常采纳。"""
        h = "0x" + "99" * 32
        self._open_leg("run-f6", h)
        A.addons[0].websocket_message(
            self._frame(
                {"txnStatus": "DONE", "txnHash": h, "creationTime": int(time.time())}
            )
        )
        leg = A.STORE.timeline("run-f6")["legs"][0]
        self.assertIsNotNone(
            leg["hkL1aMs"], "秒级 epoch creationTime ×1000 后判新鲜 → 采纳"
        )


# ── R22 四轮（2026-09-07 r6）①：N>1 订单身份去重 ──


class TestR22OrderIdentityDedup(unittest.TestCase):
    """槽绑定先做订单身份去重：同 run 内与已绑槽腿 orderId/clientOrderId 正向匹配
    （至少一个共同身份键且全部共同键相等）的请求归入同一授权槽——dup 诊断，不占新槽、
    不新增 attempted、不计 unbound；不同订单各占其槽；身份缺失/冲突不猜（保持容量
    判定与 unbound 旧律）。旧→新映射：r5「同单重复 → unbound」→ r6「同单重复 → dup」
    （见 TestR22SlotBinding.test_duplicate_same_order_request_merges_into_slot）。"""

    PO = "/bapi/defi/v2/private/wallet-direct/web-dex/place-order"

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: None  # 不验 receipt——只考去重与槽位语义
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    def _binance(self, cid=None, oid=None, chain="BSC"):
        req = {"chain": chain}
        if cid:
            req["clientOrderId"] = cid
        data = {}
        if oid:
            data["orderId"] = oid
        if cid:
            data["clientOrderId"] = cid
        f = FakeFlow(
            "web3.binance.com",
            self.PO,
            "POST",
            req_body=json.dumps(req),
            resp_body=json.dumps({"code": "000000", "data": data}) if data else "{}",
        )
        A.addons[0].request(f)
        A.addons[0].response(f)

    def _okx(self, oid, h):
        f = FakeFlow(
            "web3.okx.com",
            "/priapi/v6/dx/trade/multi/broadcast",
            "POST",
            req_body=json.dumps({"data": {"chainId": "56"}}),
            resp_body=json.dumps(
                {"code": "0", "data": {"transactionHash": h, "orderId": oid}}
            ),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)

    def test_n2_same_order_repeat_merges(self):
        """N=2 同订单请求两次：第二请求归入同一授权槽——attempted=1/unbound=0，
        第二槽保留为缺失占位（旧 bug：两请求占 round 1/2 两槽、attempted=2）。"""
        A.STORE.mark_open(
            "run-d2",
            manifest={
                "version": 1,
                "runId": "run-d2",
                "rounds": 2,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 2}],
            },
        )
        self._binance("cid-A", "oid-A")
        self._binance("cid-A", "oid-A")
        tl = A.STORE.timeline("run-d2")
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 1, "observed": 0, "unbound": 0}
        )
        internal = A.STORE.legs["run-d2"]
        self.assertEqual((internal[0]["slot"], internal[0]["round"]), (0, None))
        self.assertIsNone(internal[1]["slot"])
        self.assertTrue(internal[1]["dup"])
        self.assertEqual([x["round"] for x in tl["legs"][:2]], [None, None])
        self.assertTrue(tl["legs"][1]["dup"])
        self.assertFalse(tl["legs"][1]["unbound"])
        ph = [x for x in tl["legs"] if x["incomplete"] == "no-order-observed"]
        self.assertEqual(len(ph), 1, "重复请求不消费第二槽——该槽仍为缺失占位")
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate"), 1)
        self.assertEqual(A.STORE.diag.get("orderReqUnbound", 0), 0)

    def test_n2_two_distinct_orders_take_own_slots(self):
        """N=2 两不同订单：各占其槽——attempted=2/unbound=0，无占位。"""
        A.STORE.mark_open(
            "run-dd",
            manifest={
                "version": 1,
                "runId": "run-dd",
                "rounds": 2,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 2}],
            },
        )
        self._binance("cid-A", "oid-A")
        self._binance("cid-B", "oid-B")
        tl = A.STORE.timeline("run-dd")
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 2, "observed": 0, "unbound": 0}
        )
        self.assertEqual(
            [x["round"] for x in tl["legs"]],
            [None, None],
            "r7：各占其槽（容量绑定），授权轮号不由到达序推导",
        )
        self.assertEqual(
            [x["observationIndex"] for x in tl["legs"]], [1, 2], "到达序连续"
        )
        self.assertFalse(any(x["dup"] for x in tl["legs"]))
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate", 0), 0)

    def test_n2_repeat_then_distinct_binds_released_slot(self):
        """重复+不同混合（okx——身份在响应才建立）：第二请求建腿时身份未知先占槽，
        响应揭示同单 → 释放槽位并改记 dup；随后真正的第二订单绑上释放出的槽。
        attempted=2/unbound=0（旧 bug：attempted=2 全是同单、第三请求反而 unbound）。"""
        A.STORE.mark_open(
            "run-dm",
            manifest={
                "version": 1,
                "runId": "run-dm",
                "rounds": 2,
                "legs": [{"platform": "okx", "chain": "bsc", "rounds": 2}],
            },
        )
        h1, h2 = "0x" + "ab" * 32, "0x" + "cd" * 32
        self._okx("ord-A1", h1)
        self._okx("ord-A1", h1)  # 同单重复：响应 orderId=ord-A1 正向匹配已绑槽腿
        internal = A.STORE.legs["run-dm"]
        self.assertIsNone(internal[1]["slot"], "响应去重后重复腿须释放占用槽")
        self.assertTrue(internal[1]["dup"])
        self._okx("ord-B1", h2)  # 真正的第二订单
        tl = A.STORE.timeline("run-dm")
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 2, "observed": 0, "unbound": 0}
        )
        self.assertEqual(
            [x["round"] for x in tl["legs"]],
            [None, None, None],
            "去重释放的槽归真正的第二订单（容量绑定）；r7 起轮号不由到达序推导",
        )
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate"), 1)
        self.assertEqual(A.STORE.diag.get("orderReqUnbound", 0), 0)

    def test_identity_from_response_converts_unbound_to_dup(self):
        """N=1 okx 同单两次：第二请求建腿时容量已满先标 unbound；响应身份正向同单 →
        改判 dup（unbound 计数与诊断同步改记，不留假 unbound）。"""
        A.STORE.mark_open(
            "run-dc",
            manifest={
                "version": 1,
                "runId": "run-dc",
                "rounds": 1,
                "legs": [{"platform": "okx", "chain": "bsc", "rounds": 1}],
            },
        )
        h1 = "0x" + "ab" * 32
        self._okx("ord-A1", h1)
        self._okx("ord-A1", h1)
        tl = A.STORE.timeline("run-dc")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 0, "unbound": 0}
        )
        internal = A.STORE.legs["run-dc"]
        self.assertTrue(internal[1]["dup"])
        self.assertFalse(internal[1]["unbound"])
        self.assertEqual(
            A.STORE.diag.get("orderReqUnbound", 0), 0, "改判 dup 后不留假 unbound 诊断"
        )
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate"), 1)

    def test_repeat_without_identity_stays_unbound(self):
        """身份缺失不猜：N=1、两次请求都无 clientOrderId/orderId —— 无法证明同单，
        第二请求保持容量判定的 unbound 诊断（r5 安全性质保留）。"""
        A.STORE.mark_open(
            "run-dn",
            manifest={
                "version": 1,
                "runId": "run-dn",
                "rounds": 1,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 1}],
            },
        )
        self._binance()  # 无 clientOrderId，响应无 id
        self._binance()
        tl = A.STORE.timeline("run-dn")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 0, "unbound": 1}
        )
        internal = A.STORE.legs["run-dn"]
        self.assertFalse(internal[1].get("dup", False), "身份缺失不得猜同单")
        self.assertTrue(internal[1]["unbound"])
        self.assertEqual(A.STORE.diag.get("orderReqUnbound"), 1)
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate", 0), 0)

    def test_conflicting_identity_not_deduped(self):
        """身份冲突不猜：同 orderId 但 clientOrderId 不同 —— 共同键一同一异 =
        无法证明同单，不归并；容量已满 → 保持 unbound 诊断。（r8 起 ack 的 orderId /
        clientOrderId 由 data.* 结构化分键抽取——两键都参与匹配）"""
        A.STORE.mark_open(
            "run-dx",
            manifest={
                "version": 1,
                "runId": "run-dx",
                "rounds": 1,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 1}],
            },
        )
        self._binance("cid-AA", "oid-001")
        self._binance("cid-BB", "oid-001")  # orderId 同、clientOrderId 冲突
        tl = A.STORE.timeline("run-dx")
        self.assertEqual(
            tl["counts"],
            {"requested": 1, "attempted": 1, "observed": 0, "unbound": 1},
            "身份冲突不归并——保持 unbound 诊断，不猜",
        )
        internal = A.STORE.legs["run-dx"]
        self.assertEqual(
            internal[1].get("orderId"), "oid-001", "响应身份已建立——确为冲突而非缺失"
        )
        self.assertFalse(internal[1].get("dup", False))
        self.assertTrue(internal[1]["unbound"])

    def test_fomo_duplicate_execution_not_rebound(self):
        """fomo：preview 晋升绑槽后，同 relaySwapId 的再次预览/再次 success 帧
        （路由到最新预览腿）不再消费第二个槽——attempted 恒 1。"""
        rid = "0x" + "9a" * 32
        A.STORE.mark_open(
            "run-fd",
            manifest={
                "version": 1,
                "runId": "run-fd",
                "rounds": 1,
                "legs": [{"platform": "fomo", "chain": "robinhood", "rounds": 1}],
            },
        )

        def preview():
            f = FakeFlow(
                "prod-api.fomo.family",
                "/swaps/v2",
                "POST",
                resp_body=json.dumps(
                    {
                        "success": True,
                        "responseObject": {
                            "v2Swap": {"relaySwapId": rid, "destinationChainId": 4663}
                        },
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)

        def success():
            A.addons[0].websocket_message(
                ws_frame(
                    "ws.relay.link",
                    json.dumps(
                        {
                            "event": "request.status.updated",
                            "data": {
                                "status": "success",
                                "requestId": rid,
                                "txHashes": ["0x" + "12" * 32],
                                "destinationChainId": 4663,
                            },
                        }
                    ),
                )
            )

        preview()
        success()  # 晋升腿1并绑槽
        preview()  # 同 relaySwapId 的重复预览——响应身份正向同单 → dup
        success()  # 重复执行证据路由到腿2——晋升但不再占新槽
        tl = A.STORE.timeline("run-fd")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0}
        )
        internal = A.STORE.legs["run-fd"]
        self.assertEqual((internal[0]["slot"], internal[0]["round"]), (0, None))
        self.assertTrue(internal[1]["dup"], "同单重复预览腿须标 dup")
        self.assertIsNone(internal[1]["slot"])
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate"), 1)


# ── R22 四轮（2026-09-07 r6）②：Padre DONE·receipt 乱序补绑 ──


class TestR22DoneReceiptOutOfOrder(unittest.TestCase):
    """请求链未知、响应带独立 hash 的 padre 腿：新鲜 DONE 与有效 RH receipt 到齐时，
    两种事件顺序必须产出相同计数与相同腿身份。receipt 实证链（R24 门已过的轮询
    胜出候选）回写时回填 leg.chain 并按 manifest 槽补绑；DONE 先到 → pendingBind
    等 receipt；不猜链（receipt 实证链是唯一合法来源）；既有关联/新鲜度门不变。"""

    RH_RECEIPT = {
        "rpc": "https://rpc.mainnet.chain.robinhood.com",
        "chain": "robinhood",
        "pollIntervalMs": 800,
        "status": 1,
        "blockHash": "0x" + "99" * 32,
        "blockNumber": "0x1",
    }

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: (
            None
        )  # receipt 由测试手动 settle——事件顺序确定性
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    def _padre_sign_leg(self, run, manifest_chain, h):
        """chain 未知的 sign 腿（body "{}"）+ 注入独立 hash 锚（r8：生产无 Padre 锚源）。"""
        return padre_sign_leg(
            run,
            anchor_hash=h,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 1,
                "legs": [{"platform": "padre", "chain": manifest_chain, "rounds": 1}],
            },
        )

    @staticmethod
    def _done(h):
        import msgpack

        return ws_frame(
            "backend3.padre.gg",
            msgpack.packb(
                [
                    373,
                    200,
                    {
                        "txnStatus": "DONE",
                        "txnHash": h,
                        "creationTime": int(time.time() * 1000),
                    },
                ]
            ),
        )

    def _assert_final(self, tl):
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0}
        )
        self.assertEqual(len(tl["legs"]), 1, "授权槽被补绑消费——无占位")
        leg = tl["legs"][0]
        self.assertEqual(
            (leg["chain"], leg["anchorRole"], leg["round"], leg["unbound"]),
            ("robinhood", "order", None, False),
        )
        self.assertFalse(leg["pendingBind"])
        self.assertIsNotNone(leg["hkL1aMs"])
        self.assertIsNotNone(leg["hkL3Ms"])

    def test_okx_unknown_chain_waits_for_receipt_before_manifest_binding(self):
        run = "okx-unknown-chain"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 1,
                "legs": [{"platform": "okx", "chain": "solana", "rounds": 1}],
            },
        )
        with A.STORE.lock:
            leg = A.STORE._new_leg_locked(
                run, "okx", None, {"t": time.monotonic() * 1000, "tw": "synthetic"}
            )
        self.assertTrue(leg["pendingBind"])
        self.assertFalse(leg["unbound"])
        A._settle_receipt(leg, {"chain": "solana", "status": 1, "slot": 123})
        self.assertEqual(leg["chain"], "solana")
        self.assertIsNotNone(leg["slot"])
        self.assertFalse(leg["pendingBind"])
        self.assertFalse(leg["unbound"])
        # Capacity is still enforced for another independent order.
        with A.STORE.lock:
            extra = A.STORE._new_leg_locked(
                run, "okx", None, {"t": time.monotonic() * 1000, "tw": "synthetic"}
            )
        A._settle_receipt(extra, {"chain": "solana", "status": 1, "slot": 124})
        self.assertTrue(extra["unbound"])
        self.assertIsNone(extra["slot"])

    def test_done_then_receipt_and_reverse_same_outcome(self):
        """DONE→receipt 与 receipt→DONE 产出相同计数与相同腿身份（旧 bug：前者
        1/0/0/1、后者 1/1/1/0 且 leg.chain=null——相同证据不同统计）。"""
        h1, h2 = "0x" + "ab" * 32, "0x" + "cd" * 32
        # 顺序 1：DONE 先到——链未实证 → pendingBind（不即判 unbound），receipt 补绑
        leg1 = self._padre_sign_leg("run-o1", "robinhood", h1)
        A.addons[0].websocket_message(self._done(h1))
        self.assertTrue(
            leg1.get("pendingBind"),
            "链未实证——等 receipt 实证链补绑（旧 bug：直接 unbound）",
        )
        mid = A.STORE.timeline("run-o1")
        self.assertEqual(
            mid["counts"],
            {"requested": 1, "attempted": 0, "observed": 0, "unbound": 0},
            "pendingBind 腿开窗中暂不计 attempted/unbound",
        )
        self.assertTrue(mid["legs"][0]["pendingBind"])
        A._settle_receipt(leg1, dict(self.RH_RECEIPT))
        A.STORE.mark_close("run-o1")
        tl1 = A.STORE.timeline("run-o1")
        # 顺序 2：receipt 先到——实证链立即回填；DONE 后补晋升+绑槽
        leg2 = self._padre_sign_leg("run-o2", "robinhood", h2)
        A._settle_receipt(leg2, dict(self.RH_RECEIPT))
        self.assertEqual(
            leg2.get("chain"),
            "robinhood",
            "receipt 实证链立即回填（旧 bug：leg.chain 恒 null）",
        )
        mid2 = A.STORE.timeline("run-o2")
        self.assertEqual(
            mid2["counts"]["observed"], 0, "sign 腿 receipt 不单独计 observed（③门槛）"
        )
        A.addons[0].websocket_message(self._done(h2))
        A.STORE.mark_close("run-o2")
        tl2 = A.STORE.timeline("run-o2")
        self._assert_final(tl1)
        self._assert_final(tl2)

    def test_done_without_receipt_settles_unbound_at_close(self):
        """DONE 先到但 receipt 始终未实证链：开窗中 pendingBind 不猜链；关窗落定
        unbound（计 diag.orderReqUnbound），授权槽占位保留。"""
        h = "0x" + "ef" * 32
        leg = self._padre_sign_leg("run-o3", "robinhood", h)
        A.addons[0].websocket_message(self._done(h))
        self.assertTrue(leg.get("pendingBind"))
        self.assertEqual(
            A.STORE.diag.get("orderReqUnbound", 0), 0, "开窗中不提前判 unbound"
        )
        A.STORE.mark_close("run-o3")
        tl = A.STORE.timeline("run-o3")
        self.assertEqual(
            tl["counts"],
            {"requested": 1, "attempted": 0, "observed": 0, "unbound": 1},
            "关窗仍无实证链——执行观察按 unbound 落定（不猜链）",
        )
        lego = tl["legs"][0]
        self.assertIsNone(lego["chain"])
        self.assertTrue(lego["unbound"])
        self.assertFalse(lego["pendingBind"])
        self.assertEqual(lego["incomplete"], "no-receipt-observed")
        self.assertEqual(A.STORE.diag.get("orderReqUnbound"), 1)
        self.assertEqual(
            tl["legs"][1]["incomplete"], "no-order-observed", "授权槽未被消费——占位保留"
        )

    def test_receipt_chain_without_manifest_slot_not_bound(self):
        """receipt 实证链无对应 manifest 槽（授权 bsc、实证 robinhood）——不猜不占
        别的链槽：回填 chain 如实记录，补绑失败落定 unbound。"""
        h = "0x" + "77" * 32
        leg = self._padre_sign_leg("run-o4", "bsc", h)
        A.addons[0].websocket_message(self._done(h))
        self.assertTrue(leg.get("pendingBind"))
        A._settle_receipt(leg, dict(self.RH_RECEIPT))
        tl = A.STORE.timeline("run-o4")
        self.assertEqual(
            tl["counts"],
            {"requested": 1, "attempted": 0, "observed": 0, "unbound": 1},
            "实证链无授权槽——不得吞掉 bsc 槽",
        )
        lego = tl["legs"][0]
        self.assertEqual(lego["chain"], "robinhood", "实证链如实回填")
        self.assertTrue(lego["unbound"])
        self.assertEqual(tl["legs"][1]["chain"], "bsc")
        self.assertEqual(
            tl["legs"][1]["incomplete"],
            "no-order-observed",
            "bsc 授权槽不被实证 robinhood 的腿消费",
        )


# ── R22 四轮（2026-09-07 r6）③：observed 只计绑槽 order 腿 ──


class TestR22ObservedBoundOrderGate(unittest.TestCase):
    """observed ⊆ attempted（绑槽 order 腿）：sign/preview 腿的 receipt 只作诊断与
    晋升依据，晋升失败不单独计入 observed；绑槽 order 腿的 receipt 仍计 observed。"""

    RH_RECEIPT = TestR22DoneReceiptOutOfOrder.RH_RECEIPT

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: None
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    def test_sign_leg_receipt_alone_not_observed(self):
        """只有有效 receipt、没有 DONE 的 sign 腿：关窗 counts.requested=1/
        attempted=0/observed=0（旧 bug：observed=1——observed 未要求 order+绑槽）。
        receipt 证据保留在腿上（诊断/晋升依据），incomplete=no-execution-proof。"""
        h = "0x" + "ab" * 32
        leg = padre_sign_leg(
            "run-ob",
            anchor_hash=h,
            manifest={
                "version": 1,
                "runId": "run-ob",
                "rounds": 1,
                "legs": [{"platform": "padre", "chain": "robinhood", "rounds": 1}],
            },
        )
        A._settle_receipt(leg, dict(self.RH_RECEIPT))
        A.STORE.mark_close("run-ob")
        tl = A.STORE.timeline("run-ob")
        self.assertEqual(
            tl["counts"],
            {"requested": 1, "attempted": 0, "observed": 0, "unbound": 0},
            "未晋升 sign 腿的 receipt 不单独计 observed（旧 bug：observed=1）",
        )
        lego = tl["legs"][0]
        self.assertEqual(lego["anchorRole"], "sign")
        self.assertEqual(lego["chain"], "robinhood", "receipt 实证链如实回填（诊断）")
        self.assertIsNotNone(lego["hkL3Ms"], "receipt 证据保留在腿上")
        self.assertEqual(lego["incomplete"], "no-execution-proof")
        self.assertEqual(
            tl["legs"][1]["incomplete"], "no-order-observed", "授权槽占位保留"
        )

    def test_preview_leg_receipt_alone_not_observed(self):
        """fomo preview 腿同律：只有 receipt 没有 Relay success → observed=0。"""
        rid = "0x" + "9a" * 32
        A.STORE.mark_open(
            "run-op",
            manifest={
                "version": 1,
                "runId": "run-op",
                "rounds": 1,
                "legs": [{"platform": "fomo", "chain": "robinhood", "rounds": 1}],
            },
        )
        f = FakeFlow(
            "prod-api.fomo.family",
            "/swaps/v2",
            "POST",
            resp_body=json.dumps(
                {
                    "success": True,
                    "responseObject": {
                        "v2Swap": {"relaySwapId": rid, "destinationChainId": 4663}
                    },
                }
            ),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        leg = A.STORE.legs["run-op"][0]
        A._settle_receipt(leg, dict(self.RH_RECEIPT))
        A.STORE.mark_close("run-op")
        tl = A.STORE.timeline("run-op")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 0, "observed": 0, "unbound": 0}
        )
        self.assertEqual(tl["legs"][0]["anchorRole"], "preview")
        self.assertEqual(tl["legs"][0]["incomplete"], "no-execution-proof")

    def test_bound_order_leg_receipt_counts_observed(self):
        """超集语义的另一端：绑槽 order 腿只有 receipt（无成功推送）仍计 observed
        ——observed = 绑槽 order 腿中有成功推送或 receipt 观察的子集。"""
        h = "0x" + "ab" * 32
        A.STORE.mark_open(
            "run-oo",
            manifest={
                "version": 1,
                "runId": "run-oo",
                "rounds": 1,
                "legs": [{"platform": "okx", "chain": "bsc", "rounds": 1}],
            },
        )
        f = FakeFlow(
            "web3.okx.com",
            "/priapi/v6/dx/trade/multi/broadcast",
            "POST",
            req_body=json.dumps({"data": {"chainId": "56"}}),
            resp_body=json.dumps(
                {"code": "0", "data": {"transactionHash": h, "orderId": "o1"}}
            ),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        leg = A.STORE.legs["run-oo"][0]
        A._settle_receipt(
            leg,
            {
                "rpc": "https://bsc-dataseed1.binance.org",
                "chain": "bsc",
                "pollIntervalMs": 800,
                "status": 1,
                "blockHash": "0x" + "99" * 32,
                "blockNumber": "0x1",
            },
        )
        A.STORE.mark_close("run-oo")
        tl = A.STORE.timeline("run-oo")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0}
        )
        self.assertEqual(tl["legs"][0]["incomplete"], "no-success-observed")


# ── r7（RH-T04，2026-09-07）：观测序号 vs 授权轮号；padre WS 新 host 分类 ──


class TestR7ObservationVsAuthorizedRound(unittest.TestCase):
    """round=授权轮号只在唯一关联时写——manifest 只带 platform×chain×rounds 数量、
    请求不带轮号，addon 无法从请求得知授权轮次（ji2fj：首轮未广播时 r6 按到达序
    写的 round 1-4 实为授权第 2-5 轮）。r7：腿 round 恒 null，到达序记
    observationIndex；授权轮由 EU 侧精确 tx/订单身份唯一回配 authorizedRound。"""

    PO = "/bapi/defi/v2/private/wallet-direct/web-dex/place-order"

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: None  # 不验 receipt——只考轮号/序号语义
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    def _binance(self, cid, chain="Robinhood"):
        f = FakeFlow(
            "web3.binance.com",
            self.PO,
            "POST",
            req_body=json.dumps({"clientOrderId": cid, "chain": chain}),
            resp_body=json.dumps(
                {
                    "code": "000000",
                    "data": {"orderId": "oid-" + cid, "clientOrderId": cid},
                }
            ),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)

    def test_first_round_missing_no_round_impersonation(self):
        """ji2fj 复现：授权 5 轮、首轮未广播，观察到 4 腿——r6 按到达序写
        round 1-4（冒充授权轮，实为第 2-5 轮）；r7 起 round 全 null、
        observationIndex 1-4 连续，counts/占位口径不变。"""
        A.STORE.mark_open(
            "run-r7",
            manifest={
                "version": 1,
                "runId": "run-r7",
                "rounds": 5,
                "legs": [{"platform": "binance", "chain": "robinhood", "rounds": 5}],
            },
        )
        for cid in ("cid-r2", "cid-r3", "cid-r4", "cid-r5"):  # 授权 r1 未广播
            self._binance(cid)
        tl = A.STORE.timeline("run-r7")
        real = [x for x in tl["legs"] if x["anchorRole"] is not None]
        self.assertEqual(
            [x["round"] for x in real], [None] * 4, "授权轮未知——不按到达序冒充"
        )
        self.assertEqual(
            [x["observationIndex"] for x in real],
            [1, 2, 3, 4],
            "到达序连续编号（纯观察事实）",
        )
        self.assertEqual(
            tl["counts"],
            {"requested": 5, "attempted": 4, "observed": 0, "unbound": 0},
            "槽绑定/计数口径不变",
        )
        ph = [x for x in tl["legs"] if x["incomplete"] == "no-order-observed"]
        self.assertEqual(len(ph), 1, "未观测轮次仍出占位")
        self.assertIsNone(ph[0]["observationIndex"], "占位腿无到达观察")

    def test_observation_index_covers_dup_and_unbound(self):
        """到达序是纯观察事实：dup 重复腿与 unbound 诊断腿同样按到达编号。"""
        A.STORE.mark_open(
            "run-oi",
            manifest={
                "version": 1,
                "runId": "run-oi",
                "rounds": 1,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 1}],
            },
        )
        for cid in (
            "cid-a",
            "cid-a",
            "cid-b",
        ):  # 第二次同单 → dup；第三次容量已满 → unbound
            self._binance(cid, chain="BSC")
        legs = A.STORE.timeline("run-oi")["legs"]
        self.assertEqual([x["observationIndex"] for x in legs], [1, 2, 3])
        self.assertEqual([bool(x["dup"]) for x in legs], [False, True, False])
        self.assertEqual([bool(x["unbound"]) for x in legs], [False, False, True])
        self.assertEqual([x["round"] for x in legs], [None] * 3)

    def test_full_mode_exposes_anchor_identity(self):
        """full=1（loopback 证据恢复）暴露请求/响应建立的锚身份（EU 侧
        authorizedRound 唯一关联用）；缺省不暴露。"""
        A.STORE.mark_open("run-fid", manifest={"rounds": 1})
        self._binance("cid-idc")
        short = A.STORE.timeline("run-fid", full=False)["legs"][0]
        full = A.STORE.timeline("run-fid", full=True)["legs"][0]
        self.assertIsNone(short["orderId"])
        self.assertIsNone(short["clientOrderId"])
        self.assertEqual(full["orderId"], "oid-cid-idc")
        self.assertEqual(full["clientOrderId"], "cid-idc")

    def test_padre_done_new_hosts_classified(self):
        """RH-T04②：backend2.padre.gg / txn-service.padre.gg 纳入 padre WS 分类
        （生产实证出现）；DONE 严格门/新鲜度/锚纪律不变，只扩 host 匹配。"""
        import msgpack

        for host in ("backend2.padre.gg", "txn-service.padre.gg"):
            self.assertEqual(A._classify_ws(host), "padre")
        self.assertIsNone(
            A._classify_ws("nottxn-service.padre.gg"), "点边界——同后缀异主机不误分类"
        )
        # 端到端：新 host 的 DONE 帧走同一份锚+新鲜度门 → 晋升 order 成功
        # （r8：锚注入——生产今日无 Padre 锚源，见 test_padre_msgpack_done）
        h = "0x" + "cd" * 32
        padre_sign_leg(
            "run-b2",
            anchor_hash=h,
            req_body=json.dumps({"data": {"chainId": "56"}}),
            manifest={
                "version": 1,
                "runId": "run-b2",
                "rounds": 1,
                "legs": [{"platform": "padre", "chain": "bsc", "rounds": 1}],
            },
        )
        frame = msgpack.packb(
            [
                373,
                200,
                {
                    "txnStatus": "DONE",
                    "txnHash": h,
                    "creationTime": int(time.time() * 1000),
                },
            ]
        )
        A.addons[0].websocket_message(ws_frame("txn-service.padre.gg", frame))
        leg = A.STORE.timeline("run-b2")["legs"][0]
        self.assertEqual(
            leg["anchorRole"], "order", "新 host DONE 正向匹配 = 执行证据 → 晋升 order"
        )
        self.assertIsNotNone(leg["hkL1aMs"])


# ── r8（2026-09-07 复审 PX-02 + B/R22 同根因）：结构化锚抽取 + WS 帧按身份路由 ──


class TestR8StructuredAnchors(unittest.TestCase):
    """响应锚只从 schema-known 字段结构化抽取；旧整-body 正则仅诊断。单元级覆盖各平台
    extract_ids（binance 分键/数字 id/嵌套 id 不取/歧义 fail-closed；okx/gmgn 键白名单；
    padre Turnkey 恒无锚；fomo 结构化路径）。"""

    def test_binance_resp_ids_separate_keys_key_order_numeric_nested_id(self):
        """PX-02 案例 B/C/E：clientOrderId 先于 orderId、orderId 无引号数字、嵌套 "id"
        ——结构化抽取给出 orderId/clientOrderId **分键**且值正确；失败 ack 无 data.orderId
        → 不提供身份（请求体 clientOrderId 仍是锚）。"""
        cid = "c0ffee00-0000-4000-8000-0000000000b1"
        body_b = (
            '{"code":"000000","success":true,"data":{"clientOrderId":"%s","orderId":"oid-B-0001","message":"Waiting for Signing"}}'
            % cid
        )
        self.assertEqual(
            A._binance_resp_ids(body_b), {"orderId": "oid-B-0001", "clientOrderId": cid}
        )
        body_c = (
            '{"code":"000000","success":true,"data":{"orderId":1234567890123456789,"clientOrderId":"%s"}}'
            % cid
        )
        self.assertEqual(
            A._binance_resp_ids(body_c),
            {"orderId": "1234567890123456789", "clientOrderId": cid},
            "数字 orderId 归一为字串（旧正则要求引号 → 漏配 → clientOrderId 值冒充 orderId）",
        )
        body_e = (
            '{"code":"000000","success":true,"data":{"wallet":{"id":"wallet-000001"},"orderId":"oid-E-0001","clientOrderId":"%s"}}'
            % cid
        )
        self.assertEqual(
            A._binance_resp_ids(body_e)["orderId"], "oid-E-0001", '嵌套 "id" 不是订单号'
        )
        self.assertEqual(
            A._binance_resp_ids('{"code":"100001005","success":false,"message":"x"}'),
            {},
            "失败 ack 无身份",
        )
        self.assertEqual(
            A._binance_resp_ids('{"code":"000000","data":{"orderId":true}}'),
            {},
            "bool 不是身份",
        )
        self.assertEqual(A._binance_resp_ids("not json"), {})
        # 旧正则对同一 body 的分歧（诊断口径）：案例 B 把 clientOrderId 值写成 orderId
        self.assertEqual(A._legacy_regex_ids_diag(body_b).get("orderId"), cid)

    def test_binance_resp_ids_same_node_hash_normalized_or_ambiguous(self):
        """data 同节点 schema hash 字段（orderTxId/txHash/signature/txId）归一一致 → 取唯一 hash；
        >1 个不同值 → 不取 hash（_ambiguous 记 txHash，fail-closed）。Sol data.signature 亦取。"""
        h = "0x" + "ab" * 32
        out = A._binance_resp_ids(
            json.dumps(
                {
                    "code": "000000",
                    "data": {
                        "orderId": "o1",
                        "orderTxId": h,
                        "txHash": h.upper().replace("0X", "0x"),
                    },
                }
            )
        )
        self.assertEqual(out["txHash"], h, "大小写变体同值 → 归一后取 hash")
        amb = A._binance_resp_ids(
            json.dumps(
                {
                    "code": "000000",
                    "data": {
                        "orderId": "o1",
                        "orderTxId": h,
                        "txHash": "0x" + "cd" * 32,
                    },
                }
            )
        )
        self.assertNotIn("txHash", amb)
        self.assertEqual(amb["_ambiguous"], ["txHash"])
        sig = "5" * 88
        self.assertEqual(
            A._binance_resp_ids(
                json.dumps(
                    {"code": "000000", "data": {"orderId": "o2", "signature": sig}}
                )
            )["txHash"],
            sig,
        )

    def test_okx_gmgn_resp_ids_whitelist_and_ambiguity(self):
        """OKX：键白名单 orderId / transactionHash|txHash（docs/okx.md §2.1），忽略 id /
        clientOrderId；同键多个不同值 → 歧义不取。GMGN：oi/orderId/order_id + hash/tx_hash/
        signature… 白名单，hash 须合法形状。"""
        h = "0x" + "ab" * 32
        okx = A._okx_resp_ids(
            json.dumps(
                {
                    "code": "0",
                    "data": {
                        "id": "row-000001",
                        "clientOrderId": "not-okx",
                        "orderId": "ord-1",
                        "transactionHash": h,
                        "isSuccess": True,
                    },
                }
            )
        )
        self.assertEqual(okx, {"orderId": "ord-1", "txHash": h})
        okx_amb = A._okx_resp_ids(
            json.dumps({"data": [{"orderId": "ord-1"}, {"orderId": "ord-2"}]})
        )
        self.assertNotIn("orderId", okx_amb)
        self.assertEqual(okx_amb["_ambiguous"], ["orderId"])
        self.assertEqual(
            A._okx_resp_ids(json.dumps({"data": {"id": "row-000001"}})),
            {},
            "id 不是订单号",
        )
        sig = "5" * 88
        gm = A._gmgn_resp_ids(
            json.dumps({"code": 0, "data": {"hash": sig, "orderId": "g1"}})
        )
        self.assertEqual(gm, {"orderId": "g1", "txHash": sig})
        self.assertEqual(
            A._gmgn_resp_ids(json.dumps({"data": {"hash": "0xshort", "oi": "g2"}})),
            {"orderId": "g2"},
            "非法 hash 形状不取",
        )

    def test_padre_turnkey_response_has_no_anchor_legacy_regex_diverged(self):
        """PX-01：真实 Turnkey ActivityResponse → 结构化抽取恒 {}（activity.id 非订单号、
        intent.payload 非 tx hash、r/s/v 非身份）；旧正则会把 activity.id 当 orderId（分歧）。"""
        body = turnkey_sign_response()
        self.assertEqual(A._padre_resp_ids(body), {})
        self.assertEqual(
            A._legacy_regex_ids_diag(body),
            {"orderId": TURNKEY_ACTIVITY_ID},
            "旧正则误锚：activity.id → orderId",
        )

    def test_fomo_resp_ids_structured_path(self):
        rid = "0x" + "9a" * 32
        out = A._fomo_resp_ids(
            json.dumps(
                {
                    "success": True,
                    "responseObject": {
                        "v2Swap": {"relaySwapId": rid, "destinationChainId": 4663}
                    },
                }
            )
        )
        self.assertEqual(out, {"orderId": rid, "chain": "robinhood"})
        self.assertEqual(
            A._fomo_resp_ids(
                json.dumps({"responseObject": {"v2Swap": {"relaySwapId": "nope"}}})
            ),
            {},
            "非 EVM 形状不取",
        )
        self.assertEqual(
            A._fomo_resp_ids(
                json.dumps({"statusCode": 200, "message": "Successful v2 swap"})
            ),
            {},
            "同链 DFlow 预览无 v2Swap → 无锚",
        )


class TestR8IdentityRouting(unittest.TestCase):
    """入站 WS 帧按锚身份路由到所有开窗腿（不只平台最新腿）；dup 腿命中归 formal 腿；
    成功永不留在 dup 腿；无腿可归属/锚冲突计 diag（缺失可见）。"""

    PO = "/bapi/defi/v2/private/wallet-direct/web-dex/place-order"

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: None  # 不验 receipt——只考路由/归属/计数
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    def _manifest(self, run, rounds=1, platform="binance", chain="bsc"):
        return {
            "version": 1,
            "runId": run,
            "rounds": rounds,
            "legs": [{"platform": platform, "chain": chain, "rounds": rounds}],
        }

    def _place(self, cid, resp_body, chain="BSC"):
        f = FakeFlow(
            "web3.binance.com",
            self.PO,
            "POST",
            req_body=json.dumps({"clientOrderId": cid, "chain": chain}),
            resp_body=resp_body,
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        return f

    @staticmethod
    def _fin(oid, cid, h=None, status="FINISHED"):
        c = {"status": status}
        if oid is not None:
            c["orderId"] = oid
        if cid is not None:
            c["clientOrderId"] = cid
        if h:
            c["orderTxId"] = h
        return ws_frame(
            "web3-stream.binance.com",
            json.dumps(
                {"stream": "w3pc:x", "data": {"bizKey": "DEX_ALL_ORDER", "content": c}}
            ),
        )

    def test_key_order_finished_attributed(self):
        """案例 B：ack 里 clientOrderId 先于 orderId——旧正则把 cid 值写成 orderId → FINISHED
        「冲突」整帧丢弃（hkL1aMs=None 静默）。r8：分键锚 → observed=1，L1a′ 落腿，
        evidence.anchor 键名/旧正则分歧/成功命中键可审计。"""
        run = "run-r8b"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        cid = "c0ffee00-0000-4000-8000-000000000b01"
        self._place(
            cid,
            '{"code":"000000","success":true,"data":{"clientOrderId":"%s","orderId":"oid-B-0001"}}'
            % cid,
        )
        A.addons[0].websocket_message(self._fin("oid-B-0001", cid, "0x" + "b1" * 32))
        tl = A.STORE.timeline(run, full=True)
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0}
        )
        leg = tl["legs"][0]
        self.assertIsNotNone(leg["hkL1aMs"])
        self.assertEqual(
            (leg["orderId"], leg["clientOrderId"]), ("oid-B-0001", cid), "分键锚"
        )
        self.assertEqual(
            leg["evidence"]["anchor"]["keys"],
            ["clientOrderId", "orderId"],
            "WS 学得的 hash 不是锚（腿字段有、锚键无）",
        )
        self.assertEqual(
            leg["txHash"], "0x" + "b1" * 32, "正向匹配帧的 hash 挂载为腿字段"
        )
        self.assertTrue(
            leg["evidence"]["anchor"]["legacyRegexDiverged"],
            "旧正则在此 body 上会误锚——分歧如实记录",
        )
        self.assertEqual(
            leg["evidence"]["successAnchorKeys"], ["orderId", "clientOrderId"]
        )
        self.assertEqual(tl["diag"], {}, "无静默丢弃")

    def test_numeric_order_id_finished_attributed(self):
        """案例 C：ack orderId 为无引号数字（帧里可为数字或字串）——旧正则漏配 → cid 值冒充
        orderId → 冲突丢帧。r8：数字/字串同值比较 → observed=1。"""
        run = "run-r8c"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        cid = "c0ffee00-0000-4000-8000-000000000c01"
        self._place(
            cid,
            '{"code":"000000","success":true,"data":{"orderId":1234567890123456789,"clientOrderId":"%s"}}'
            % cid,
        )
        # 帧 orderId 为数字
        A.addons[0].websocket_message(
            self._fin(1234567890123456789, cid, "0x" + "c1" * 32)
        )
        tl = A.STORE.timeline(run, full=True)
        self.assertEqual(tl["counts"]["observed"], 1)
        self.assertEqual(tl["legs"][0]["orderId"], "1234567890123456789")
        # 第二腿：帧 orderId 为字串 —— 同值
        A.STORE.mark_open("run-r8c2", manifest=self._manifest("run-r8c2"))
        self._place(
            cid + "-2",
            '{"code":"000000","success":true,"data":{"orderId":987654321,"clientOrderId":"%s-2"}}'
            % cid,
        )
        A.addons[0].websocket_message(
            self._fin("987654321", cid + "-2", "0x" + "c2" * 32)
        )
        self.assertEqual(A.STORE.timeline("run-r8c2")["counts"]["observed"], 1)

    def test_platform_resend_success_lands_on_formal_leg(self):
        """案例 A（docs/binance.md §90：平台自身 ~2.2s 后重发 place-order，首 ack 100001005
        → 次 ack 000000）：同 clientOrderId 两请求 + FINISHED。旧：成功落最新腿（dup），
        formal 腿 no-success-observed、observed=0。r8：dup 的 ack 身份并入 formal 锚，
        FINISHED 按身份归 formal → observed=1，L1a′ 以**首次**下单请求为起点（EU
        order_http_out 同义，含平台重试）；dup 腿无成功、关窗标 duplicate-of-bound-leg
        并指向 formal（evidence.mergedIntoFlowId）。"""
        run = "run-r8a"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        cid = "c0ffee00-0000-4000-8000-000000000a01"
        f1 = self._place(cid, '{"code":"100001005","success":false,"message":"err"}')
        f2 = self._place(
            cid,
            '{"code":"000000","success":true,"data":{"orderId":"oid-A-0001","clientOrderId":"%s"}}'
            % cid,
        )
        internal = A.STORE.legs[run]
        self.assertEqual(
            (internal[0]["slot"], internal[0]["dup"]),
            (0, False),
            "首请求 = formal（绑槽）",
        )
        self.assertTrue(internal[1]["dup"])
        self.assertIs(internal[1]["_dupOf"], internal[0])
        self.assertEqual(
            internal[0]["_anchor"].get("orderId"),
            "oid-A-0001",
            "dup 腿 ack 的 orderId 并入 formal 锚（独立锚，非 WS 学得）",
        )
        A.addons[0].websocket_message(self._fin("oid-A-0001", cid, "0x" + "a1" * 32))
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run, full=True)
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0}
        )
        formal, dup = tl["legs"][0], tl["legs"][1]
        self.assertFalse(formal["dup"])
        self.assertIsNotNone(formal["hkL1aMs"], "L1a′ 落 formal 腿")
        self.assertEqual(
            formal["tOrderOutMs"], internal[0]["tOrderOutMs"], "起点 = 首次下单请求"
        )
        self.assertEqual(formal["txHash"], "0x" + "a1" * 32, "hash 挂 formal")
        self.assertEqual(formal["incomplete"], "no-receipt-observed")
        self.assertEqual(
            formal["evidence"]["successAnchorKeys"], ["orderId", "clientOrderId"]
        )
        self.assertTrue(dup["dup"])
        self.assertIsNone(dup["hkL1aMs"], "成功永不留在 dup 腿")
        self.assertEqual(
            dup["incomplete"],
            "duplicate-of-bound-leg",
            "dup 腿关窗如实标注（非 no-success-observed）",
        )
        self.assertEqual(
            dup["evidence"]["mergedIntoFlowId"], formal["evidence"]["orderFlowId"]
        )
        self.assertEqual(dup["evidence"]["mergedIntoFlowId"], f1.id)
        self.assertEqual(dup["evidence"]["orderFlowId"], f2.id)
        self.assertEqual(
            len([x for x in tl["legs"] if x["incomplete"] == "no-order-observed"]), 0
        )
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate"), 1)

    def test_nested_id_key_not_order_anchor(self):
        """案例 E：data 内嵌套对象的 "id" 先于 orderId——旧正则把它当 orderId。r8：observed=1。"""
        run = "run-r8e"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        cid = "c0ffee00-0000-4000-8000-000000000e01"
        self._place(
            cid,
            '{"code":"000000","success":true,"data":{"wallet":{"id":"wallet-000001"},"orderId":"oid-E-0001","clientOrderId":"%s"}}'
            % cid,
        )
        A.addons[0].websocket_message(self._fin("oid-E-0001", cid, "0x" + "e1" * 32))
        tl = A.STORE.timeline(run, full=True)
        self.assertEqual(tl["counts"]["observed"], 1)
        self.assertEqual(tl["legs"][0]["orderId"], "oid-E-0001")

    def test_dup_with_conflicting_order_id_not_attributed(self):
        """多订单疑似：同 clientOrderId 两次成功 ack 但 orderId 不同（docs/binance.md：
        一轮 >1 distinct orderId = multiOrderSuspect）。旧→新映射（r9，RHF-10③）：
        r8 时 dup 腿身份与 formal 冲突仍永久扣在 dup 上——FINISHED(oid-2) 命中 dup 但
        formal 冲突 → 整帧拒收（wsFrameAnchorConflict），第二订单的执行证据被吞；
        r9 起同单证明被响应身份推翻 → dup 腿重开为独立候选（计 orderDupReopened；
        rounds=1 无空槽 → unbound 诊断如实），FINISHED(oid-2) 落该腿（unbound 有执行
        观察但不计 attempted/observed），FINISHED(oid-1) 落 formal。"""
        run = "run-r8f"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        cid = "c0ffee00-0000-4000-8000-000000000f01"
        self._place(
            cid,
            '{"code":"000000","success":true,"data":{"orderId":"oid-F-0001","clientOrderId":"%s"}}'
            % cid,
        )
        self._place(
            cid,
            '{"code":"000000","success":true,"data":{"orderId":"oid-F-0002","clientOrderId":"%s"}}'
            % cid,
        )
        internal = A.STORE.legs[run]
        self.assertFalse(
            internal[1].get("dup"), "r9：冲突身份推翻 dup 判定——重开为独立候选"
        )
        self.assertIsNone(internal[1].get("_dupOf"))
        self.assertTrue(
            internal[1].get("unbound"), "无空槽——重开后按容量判定落 unbound（计数如实）"
        )
        self.assertTrue(
            internal[1].get("_multiOrderSuspect"), "multi-order-suspect 内部审计标记"
        )
        self.assertEqual(
            A.STORE.diag.get("orderReqDuplicate", 0), 0, "重开后 dup 计数同步改记"
        )
        self.assertEqual(A.STORE.diag.get("orderDupReopened"), 1)
        A.addons[0].websocket_message(self._fin("oid-F-0002", cid, "0x" + "f2" * 32))
        A.addons[0].websocket_message(self._fin("oid-F-0001", cid, "0x" + "f1" * 32))
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run, full=True)
        self.assertEqual(
            tl["counts"],
            {"requested": 1, "attempted": 1, "observed": 1, "unbound": 1},
            "formal 计 attempted/observed；重开腿 unbound 诊断不计",
        )
        self.assertIsNotNone(
            tl["legs"][1]["hkL1aMs"], "第二订单的执行证据落重开腿（旧码：整帧吞掉）"
        )
        self.assertIsNotNone(tl["legs"][0]["hkL1aMs"])
        self.assertEqual(
            tl["legs"][0]["orderId"],
            "oid-F-0001",
            "formal 锚不被 dup 的冲突 orderId 覆盖",
        )
        self.assertIn("orderId", tl["legs"][1]["evidence"]["anchor"]["conflicts"])
        self.assertGreaterEqual(tl["diag"].get("anchorConflict", 0), 1)
        self.assertEqual(
            tl["diag"].get("orderDupReopened"), 1, "重开如实可见（multi-order-suspect）"
        )
        self.assertNotIn("wsFrameAnchorConflict", tl["diag"], "两帧各归其腿——无拒收")

    def test_late_success_for_previous_round_routed_by_identity(self):
        """N=2 两不同订单：r1 的 FINISHED 迟到——在 r2 下单请求之后才抵达。旧：只看平台最新腿
        （r2）→ orderId 冲突 → 整帧静默丢弃 → r1 no-success-observed。r8：按身份路由到 r1 腿。"""
        run = "run-r8g"
        A.STORE.mark_open(run, manifest=self._manifest(run, rounds=2))
        self._place(
            "cid-g1",
            '{"code":"000000","success":true,"data":{"orderId":"oid-G-0001","clientOrderId":"cid-g1"}}',
        )
        self._place(
            "cid-g2",
            '{"code":"000000","success":true,"data":{"orderId":"oid-G-0002","clientOrderId":"cid-g2"}}',
        )
        A.addons[0].websocket_message(
            self._fin("oid-G-0001", "cid-g1", "0x" + "91" * 32)
        )  # r1 迟到成功
        mid = A.STORE.timeline(run)
        self.assertIsNotNone(
            mid["legs"][0]["hkL1aMs"], "迟到成功帧按身份落 r1 腿（旧 bug：整帧丢弃）"
        )
        self.assertIsNone(mid["legs"][1]["hkL1aMs"])
        A.addons[0].websocket_message(
            self._fin("oid-G-0002", "cid-g2", "0x" + "92" * 32)
        )
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 2, "observed": 2, "unbound": 0}
        )
        self.assertEqual(tl["diag"], {})

    def test_ws_frame_without_leg_match_is_counted_not_silent(self):
        """带身份却无腿可归属的成功帧不归属任何腿，但计入 timeline.diag 与 /health diag
        ——缺失可见；无身份键的帧（行情噪声）不计。词汇与 binance-ws.mjs anchorMatchVerdict
        同：共同键**全不等** = 他单 miss（wsFrameNoLegMatch）；一等一不等 = 同单身份自相
        矛盾 conflict（wsFrameAnchorConflict）。"""
        run = "run-r8n"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        self._place(
            "cid-n1",
            '{"code":"000000","success":true,"data":{"orderId":"oid-N-0001","clientOrderId":"cid-n1"}}',
        )
        A.addons[0].websocket_message(
            self._fin("oid-OTHER", "cid-other", "0x" + "77" * 32)
        )  # 他单：全不等
        A.addons[0].websocket_message(
            ws_frame(
                "web3-stream.binance.com",
                json.dumps(
                    {
                        "stream": "w3pc:x",
                        "data": {"bizKey": "ticker", "content": {"p": "1.0"}},
                    }
                ),
            )
        )
        tl = A.STORE.timeline(run)
        self.assertEqual(tl["counts"]["observed"], 0)
        self.assertEqual(tl["diag"], {"wsFrameNoLegMatch": 1})
        self.assertEqual(A.STORE.diag.get("wsFrameNoLegMatch"), 1)
        self.assertIsNone(tl["legs"][0]["txHash"], "他单 hash 不得挂载")
        A.addons[0].websocket_message(
            self._fin("oid-OTHER", "cid-n1", "0x" + "78" * 32)
        )  # 一等一不等：冲突
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["diag"], {"wsFrameNoLegMatch": 1, "wsFrameAnchorConflict": 1}
        )
        self.assertIsNone(tl["legs"][0]["hkL1aMs"])
        self.assertIsNone(tl["legs"][0]["txHash"])

    def test_no_manifest_resend_earliest_order_leg_wins(self):
        """无 manifest（无槽、不做 dup 归并）下同身份两腿：成功归**最早**出站的 order 腿
        （首次下单请求 = formal 语义），不再取最新腿。"""
        run = "run-r8nm"
        A.STORE.mark_open(run)
        cid = "c0ffee00-0000-4000-8000-0000000000aa"
        self._place(cid, '{"code":"100001005","success":false}')
        self._place(
            cid,
            '{"code":"000000","success":true,"data":{"orderId":"oid-NM-1","clientOrderId":"%s"}}'
            % cid,
        )
        A.addons[0].websocket_message(self._fin("oid-NM-1", cid, "0x" + "aa" * 32))
        legs = A.STORE.timeline(run)["legs"]
        self.assertIsNotNone(legs[0]["hkL1aMs"], "最早腿（首请求）得成功")
        self.assertIsNone(legs[1]["hkL1aMs"])

    def test_fomo_same_relay_id_prefers_latest_preview(self):
        """fomo 预览/执行 L7 不可区分：同 relaySwapId 多个 preview 腿时成功归**最晚**的
        preview（最接近执行 POST——沿用 r7 前「最新腿」近似，不放大预览提前量）。"""
        rid = "0x" + "9a" * 32
        A.STORE.mark_open(
            "run-r8fo",
            manifest=self._manifest("run-r8fo", platform="fomo", chain="robinhood"),
        )
        for _ in range(2):
            f = FakeFlow(
                "prod-api.fomo.family",
                "/swaps/v2",
                "POST",
                resp_body=json.dumps(
                    {
                        "success": True,
                        "responseObject": {
                            "v2Swap": {"relaySwapId": rid, "destinationChainId": 4663}
                        },
                    }
                ),
            )
            A.addons[0].request(f)
            A.addons[0].response(f)
        A.addons[0].websocket_message(
            ws_frame(
                "ws.relay.link",
                json.dumps(
                    {
                        "event": "request.status.updated",
                        "data": {
                            "status": "success",
                            "requestId": rid,
                            "txHashes": ["0x" + "12" * 32],
                            "destinationChainId": 4663,
                        },
                    }
                ),
            )
        )
        internal = A.STORE.legs["run-r8fo"]
        self.assertIsNone(internal[0]["tSuccessPushMs"])
        self.assertEqual(internal[1]["anchorRole"], "order", "最晚 preview 腿晋升")
        self.assertIsNotNone(internal[1]["tSuccessPushMs"])
        self.assertEqual(
            A.STORE.timeline("run-r8fo")["counts"],
            {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0},
        )

    def test_timeline_schema_additions_backward_compatible(self):
        """r8 输出只做**新增**：顶层 diag（dict）、evidence.anchor/successAnchorKeys/
        mergedIntoFlowId；既有键与占位腿形状不变；缺省模式不暴露身份值。"""
        run = "run-r8s"
        A.STORE.mark_open(run, manifest=self._manifest(run, rounds=2))
        self._place(
            "cid-s1",
            '{"code":"000000","success":true,"data":{"orderId":"oid-S-0001","clientOrderId":"cid-s1"}}',
        )
        tl = A.STORE.timeline(run)
        self.assertEqual(tl["schemaVersion"], 2)
        self.assertIsInstance(tl["diag"], dict)
        leg, ph = tl["legs"]
        for k in (
            "chain",
            "platform",
            "round",
            "observationIndex",
            "anchorRole",
            "unbound",
            "dup",
            "pendingBind",
            "tOrderOutMs",
            "tFirstRespMs",
            "tSuccessPushMs",
            "tReceiptMs",
            "hkL1aMs",
            "hkL3Ms",
            "txHash",
            "orderId",
            "clientOrderId",
            "evidence",
            "incomplete",
            "atWall",
        ):
            self.assertIn(k, leg)
        self.assertEqual(
            set(leg["evidence"]),
            {
                "orderFlowId",
                "wsFrameSha",
                "receipt",
                "anchor",
                "successAnchorKeys",
                "mergedIntoFlowId",
            },
        )
        self.assertEqual(
            set(leg["evidence"]["anchor"]),
            {"keys", "legacyRegexDiverged", "conflicts", "ambiguous"},
        )
        self.assertIsNone(leg["orderId"], "缺省模式不暴露身份值")
        self.assertNotIn(
            "oid-S-0001", json.dumps(leg["evidence"]), "evidence 不含身份值"
        )
        self.assertEqual(ph["evidence"]["anchor"], None)
        self.assertEqual(ph["incomplete"], "no-order-observed")
        self.assertNotIn("_dupOf", leg)
        self.assertNotIn("_anchor", leg)


# ── r9（2026-09-08）：RHF-09 锚身份冲突否决 / RHF-10 去重边界 ──


class TestR9AnchorConflictVeto(unittest.TestCase):
    """RHF-09：身份冲突 = 否决条件。fomo 页面出站 subscribe 按到达时序绑到最新腿——
    A 的迟到 subscribe 挂到 B 后，B 自己的响应明确另一 relayId → 锚自相矛盾 → B 降级
    diagnostic（_anchorVeto + incomplete=anchor-conflict 首写）：冲突帧不得驱动其
    晋升/成功、不起 receipt 轮询、observed 不增加；A 的成功只匹配 A 的腿（成功腿起点
    按独立请求/响应证据）。旧码（r8）只记 conflict 不否决——success(A) 落在 B 且
    起点按 B 的较晚请求（HK-L1a′ 假性偏小）。"""

    RELAY_A = "0x" + "a1" * 32
    RELAY_B = "0x" + "b2" * 32

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        self.polls = []
        A._poll_receipt = lambda *a, **k: self.polls.append(a)
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    def _manifest(self, run, rounds=2):
        return {
            "version": 1,
            "runId": run,
            "rounds": rounds,
            "legs": [{"platform": "fomo", "chain": "robinhood", "rounds": rounds}],
        }

    def _preview(self, relay=None, respond=True):
        body = "{}"
        if relay:
            body = json.dumps(
                {
                    "success": True,
                    "responseObject": {
                        "v2Swap": {"relaySwapId": relay, "destinationChainId": 4663}
                    },
                }
            )
        f = FakeFlow("prod-api.fomo.family", "/swaps/v2", "POST", resp_body=body)
        A.addons[0].request(f)
        if respond:
            A.addons[0].response(f)
        return f

    def _respond(self, f, relay):
        f.response = FakeResp(
            json.dumps(
                {
                    "success": True,
                    "responseObject": {
                        "v2Swap": {"relaySwapId": relay, "destinationChainId": 4663}
                    },
                }
            )
        )
        A.addons[0].response(f)

    def _subscribe(self, rid):
        A.addons[0].websocket_message(
            ws_frame(
                "ws.relay.link",
                json.dumps(
                    {
                        "type": "subscribe",
                        "event": "request.status.updated",
                        "filters": {"id": rid},
                    }
                ),
                from_client=True,
            )
        )

    def _status(self, rid, status, txh=None):
        d = {"status": status, "requestId": rid, "destinationChainId": 4663}
        if txh:
            d["txHashes"] = [txh]
        A.addons[0].websocket_message(
            ws_frame(
                "ws.relay.link",
                json.dumps({"event": "request.status.updated", "data": d}),
            )
        )

    def test_interleaved_ab_cross_bound_subscribe_vetoed(self):
        """报告反例（A/B 交错）：A preview→relayA；B preview 请求；A 的 subscribe 挂到
        B；B 响应明确 relayB；success(A) 不得落 B、B 不得晋升/计 observed。"""
        run = "run-r9ab"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        self._preview(self.RELAY_A)  # A 请求+响应（relayA 确证 A 的锚）
        time.sleep(0.02)  # 保证 B 更晚出站
        fb = self._preview(respond=False)  # B 请求（响应稍后）
        self._subscribe(self.RELAY_A)  # A 的 subscribe 迟到 → 挂到最新腿 B
        self._respond(fb, self.RELAY_B)  # B 响应明确 relayB → 锚自相矛盾 → 否决
        la, lb = A.STORE.legs[run]
        self.assertTrue(
            lb.get("_anchorVeto"), "B 锚自相矛盾 → 否决（旧码：只记 conflict 不动）"
        )
        self.assertEqual(
            lb.get("incomplete"), "anchor-conflict", "否决即时落盘（首写）"
        )
        self.assertEqual(
            (lb.get("_anchor") or {}).get("orderId"),
            self.RELAY_A,
            "锚首写冻结——冲突值不覆盖",
        )
        self.assertEqual(A.STORE.diag.get("anchorConflict"), 1)
        self._status(self.RELAY_A, "success", "0x" + "12" * 32)  # A 的真实成功帧
        self.assertEqual(la.get("anchorRole"), "order", "A 晋升（成功只匹配 A 的腿）")
        self.assertIsNotNone(la.get("tSuccessPushMs"))
        self.assertIsNone(
            lb.get("tSuccessPushMs"), "冲突帧不得驱动 B 成功（旧码：落在 B）"
        )
        self.assertEqual(lb.get("anchorRole"), "preview", "被否决腿不晋升")
        self.assertIsNone(lb.get("slot"))
        self._status(
            self.RELAY_B, "success", "0x" + "34" * 32
        )  # B 的成功帧——B 锚冻结 relayA，正向不可达
        self.assertIsNone(lb.get("tSuccessPushMs"))
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"],
            {"requested": 2, "attempted": 1, "observed": 1, "unbound": 0},
            "只有 A 计 attempted/observed——observed 不因冲突帧增加",
        )
        out_a, out_b = tl["legs"][0], tl["legs"][1]
        self.assertLess(la["tOrderOutMs"], lb["tOrderOutMs"])
        self.assertEqual(
            out_a["hkL1aMs"],
            round(la["tSuccessPushMs"] - la["tOrderOutMs"], 1),
            "成功腿起点 = A 自己的请求（独立请求/响应证据；旧码按 B 的较晚请求）",
        )
        self.assertEqual(out_b["incomplete"], "anchor-conflict")
        self.assertEqual(out_b["evidence"]["anchor"]["conflicts"], ["orderId"])
        self.assertEqual(
            tl["diag"].get("wsFrameNoLegMatch"),
            1,
            "relayB 成功帧无腿可归属——如实计数不静默",
        )
        self.assertEqual(
            [p[3] for p in self.polls],
            ["0x" + "12" * 32],
            "只有 A 的 hash 起 receipt 轮询；被否决腿/未命中帧不起 poller",
        )

    def test_subscribe_first_http_response_later(self):
        """subscribe 先到、HTTP 后到：A/B 请求均未响应时 A 的 subscribe 挂到 B；
        随后 A 响应 relayA、B 响应 relayB → B 否决；success(A) 落 A。"""
        run = "run-r9sf"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        fa = self._preview(respond=False)
        fb = self._preview(respond=False)
        self._subscribe(self.RELAY_A)  # 两腿都无锚 → 挂最新腿 B
        self._respond(fa, self.RELAY_A)
        self._respond(fb, self.RELAY_B)  # B 响应揭示冲突 → 否决
        la, lb = A.STORE.legs[run]
        self.assertTrue(lb.get("_anchorVeto"))
        self.assertFalse(la.get("_anchorVeto", False), "A 锚一致——不误伤")
        self._status(self.RELAY_A, "success", "0x" + "12" * 32)
        self.assertIsNotNone(la.get("tSuccessPushMs"))
        self.assertIsNone(lb.get("tSuccessPushMs"))
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 1, "observed": 1, "unbound": 0}
        )

    def test_late_subscribe_after_response_no_veto(self):
        """对照：B 响应先于 A 的迟到 subscribe——B 已有确证锚，subscribe 静默不动
        （时序绑定不构成冲突证据）；两单各自成功，无否决无误伤。"""
        run = "run-r9ct"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        self._preview(self.RELAY_A)  # A 请求+响应 relayA
        self._preview(self.RELAY_B)  # B 请求+响应 relayB
        self._subscribe(self.RELAY_A)  # A 的迟到 subscribe → B 已有锚 → 不动
        lb = A.STORE.legs[run][1]
        self.assertFalse(lb.get("_anchorVeto", False))
        self.assertEqual((lb.get("_anchor") or {}).get("orderId"), self.RELAY_B)
        self._status(self.RELAY_B, "success", "0x" + "34" * 32)
        self._status(self.RELAY_A, "success", "0x" + "12" * 32)
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 2, "observed": 2, "unbound": 0}
        )
        self.assertEqual(tl["diag"], {}, "无冲突无否决无静默丢弃")
        self.assertIsNotNone(tl["legs"][0]["hkL1aMs"])
        self.assertIsNotNone(tl["legs"][1]["hkL1aMs"])

    def test_cancel_repeat_success_ordering(self):
        """重复/取消/成功多序：cancel 帧不算成功；同 relayId 重复预览后 success 归
        最晚**未被否决**的 preview（既定口径不变——否决只剔除自相矛盾的腿）。"""
        run = "run-r9cr"
        rid = "0x" + "9a" * 32
        A.STORE.mark_open(run, manifest=self._manifest(run, rounds=1))
        self._preview(rid)  # P1
        self._status(rid, "canceled")  # 取消帧 —— 非成功
        self.assertIsNone(A.STORE.legs[run][0].get("tSuccessPushMs"))
        time.sleep(0.02)
        self._preview(rid)  # P2 同 relayId 重复预览
        self._status(rid, "success", "0x" + "12" * 32)
        p1, p2 = A.STORE.legs[run]
        self.assertIsNone(p1.get("tSuccessPushMs"))
        self.assertEqual(
            p2.get("anchorRole"), "order", "最晚一致 preview 晋升（口径不变）"
        )
        self.assertIsNotNone(p2.get("tSuccessPushMs"))
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0}
        )
        self.assertEqual(tl["legs"][0]["incomplete"], "no-execution-proof")
        self.assertEqual(tl["diag"], {})

    def test_vetoed_leg_only_match_counted_visible(self):
        """单腿自毒化（subscribe 与自身响应身份交叉）：帧只命中被否决腿 → 不归属且
        计 wsFrameAnchorVeto（区别于锚值互斥的 wsFrameAnchorConflict）——缺失可见。"""
        run = "run-r9vo"
        A.STORE.mark_open(run, manifest=self._manifest(run, rounds=1))
        f = self._preview(respond=False)
        self._subscribe(self.RELAY_A)  # 绑到唯一腿
        self._respond(f, self.RELAY_B)  # 自身响应揭示 relayB → 冲突 → 否决
        leg = A.STORE.legs[run][0]
        self.assertTrue(leg.get("_anchorVeto"))
        self._status(self.RELAY_A, "success", "0x" + "12" * 32)  # 只命中被否决腿
        self.assertIsNone(leg.get("tSuccessPushMs"))
        self.assertEqual(A.STORE.diag.get("wsFrameAnchorVeto"), 1)
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 0, "observed": 0, "unbound": 0}
        )
        self.assertEqual(
            tl["diag"].get("wsFrameAnchorVeto"), 1, "per-run diag 同步透出"
        )
        self.assertEqual(tl["legs"][0]["incomplete"], "anchor-conflict")
        self.assertEqual(self.polls, [], "被否决腿不起 receipt 轮询")


class TestR9OrderIdentityBoundaries(unittest.TestCase):
    """RHF-10：同单去重边界。①请求无身份、身份由响应揭示时，后请求先响应不再独占
    formal——formal 资格归最早请求（成功时刻按最早请求）；②同单查找加 platform
    维度——同 orderId 跨平台不合并；③dup 腿收到冲突身份（或 formal 被否决）→
    重开为独立候选（计 orderDupReopened，multi-order-suspect 如实可见），不再永久
    扣在 dup 上吞掉后续帧。"""

    PO = "/bapi/defi/v2/private/wallet-direct/web-dex/place-order"

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: None  # 不验 receipt——只考归属/槽位/计数
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    def _req(self, cid=None, chain="BSC"):
        req = {"chain": chain}
        if cid:
            req["clientOrderId"] = cid
        f = FakeFlow("web3.binance.com", self.PO, "POST", req_body=json.dumps(req))
        A.addons[0].request(f)
        return f

    def _resp(self, f, oid, cid, h=None):
        data = {"orderId": oid, "clientOrderId": cid}
        if h:
            data["txHash"] = h
        f.response = FakeResp(json.dumps({"code": "000000", "data": data}))
        A.addons[0].response(f)

    @staticmethod
    def _receipt_ok():
        return {
            "rpc": "https://rpc.invalid",
            "chain": "bsc",
            "pollIntervalMs": 800,
            "status": 1,
            "blockHash": "0x" + "11" * 32,
            "blockNumber": 7,
        }

    @staticmethod
    def _fin(oid, cid, h):
        return ws_frame(
            "web3-stream.binance.com",
            json.dumps(
                {
                    "stream": "w3pc:x",
                    "data": {
                        "bizKey": "DEX_ALL_ORDER",
                        "content": {
                            "orderId": oid,
                            "clientOrderId": cid,
                            "status": "FINISHED",
                            "orderTxId": h,
                        },
                    },
                }
            ),
        )

    def test_response_identity_earliest_request_becomes_formal(self):
        """①X 先请求、Y 后请求（请求均无 clientOrderId）；Y 先响应揭示身份成 formal；
        X 后响应揭示同身份 → formal 资格归 X（最早请求），Y 改判 dup 归并。
        旧码（r8）：X 变 dup、成功起点按 Y（后请求先响应者），L1a′ 不含前段重试。"""
        run = "run-r9e"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 2,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 2}],
            },
        )
        fx = self._req()
        time.sleep(0.02)
        fy = self._req()
        self._resp(fy, "oid-E1", "cid-E1")  # 后请求先响应——旧码在此独占 formal
        self._resp(fx, "oid-E1", "cid-E1")  # 最早请求身份揭示 → 接管 formal
        lx, ly = A.STORE.legs[run]
        self.assertEqual(lx.get("slot"), 0, "最早请求 = formal（保住自己的槽）")
        self.assertFalse(lx.get("dup"))
        self.assertTrue(ly.get("dup"), "后请求改判 dup 归并")
        self.assertIs(ly.get("_dupOf"), lx)
        self.assertIsNone(ly.get("slot"))
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate"), 1)
        A.addons[0].websocket_message(self._fin("oid-E1", "cid-E1", "0x" + "e1" * 32))
        self.assertIsNotNone(lx.get("tSuccessPushMs"))
        self.assertIsNone(ly.get("tSuccessPushMs"), "成功永不留在 dup 腿")
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 1, "observed": 1, "unbound": 0}
        )
        self.assertLess(lx["tOrderOutMs"], ly["tOrderOutMs"])
        self.assertEqual(
            tl["legs"][0]["hkL1aMs"],
            round(lx["tSuccessPushMs"] - lx["tOrderOutMs"], 1),
            "成功时刻按最早请求（平台重发含在 L1a′ 内，docs/binance.md §90 同口径）",
        )
        self.assertEqual(tl["legs"][1]["incomplete"], "duplicate-of-bound-leg")
        self.assertEqual(
            tl["legs"][1]["evidence"]["mergedIntoFlowId"],
            fx.id,
            "后请求 mergedIntoFlowId 指向最早腿",
        )
        self.assertEqual(tl["legs"][0]["evidence"]["orderFlowId"], fx.id)

    def test_transfer_moves_settled_success_to_earliest_leg(self):
        """①乱序残余：成功帧先到（落 Y）再揭示 X 的同身份——已落地成功随 formal 资格
        移交 X（时长按最早请求），Y 清空成功改判 dup；成功时刻本身不变。"""
        run = "run-r9t"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 2,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 2}],
            },
        )
        fx = self._req()
        time.sleep(0.02)
        fy = self._req()
        self._resp(fy, "oid-T1", "cid-T1")
        A.addons[0].websocket_message(
            self._fin("oid-T1", "cid-T1", "0x" + "71" * 32)
        )  # 成功先落 Y
        ly = A.STORE.legs[run][1]
        self.assertIsNotNone(ly.get("tSuccessPushMs"))
        self._resp(fx, "oid-T1", "cid-T1")  # X 身份揭示 → 移交
        lx, ly = A.STORE.legs[run]
        self.assertIsNotNone(
            lx.get("tSuccessPushMs"), "已落地成功随 formal 资格移交最早请求"
        )
        self.assertIsNotNone(lx.get("wsSha"))
        self.assertIsNone(ly.get("tSuccessPushMs"), "成功永不留在 dup 腿（移交同律）")
        self.assertIsNone(ly.get("wsSha"))
        self.assertTrue(ly.get("dup"))
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 1, "observed": 1, "unbound": 0}
        )
        self.assertEqual(
            tl["legs"][0]["hkL1aMs"], round(lx["tSuccessPushMs"] - lx["tOrderOutMs"], 1)
        )
        self.assertGreater(
            tl["legs"][0]["hkL1aMs"], 15.0, "起点前移到最早请求——时长含 X→Y 间隔"
        )

    def test_cross_platform_same_order_id_not_merged(self):
        """②同 orderId 跨平台不合并：binance 与 okx 腿同 orderId 各占其槽、各自成功；
        旧码（r8）去重查找无 platform 范围——后到的 okx 腿被误判 dup（attempted 少计 1）。"""
        run = "run-r9x"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 2,
                "legs": [
                    {"platform": "binance", "chain": "bsc", "rounds": 1},
                    {"platform": "okx", "chain": "bsc", "rounds": 1},
                ],
            },
        )
        fb = self._req("cid-X1")
        self._resp(fb, "SAME-1", "cid-X1")
        fo = FakeFlow(
            "web3.okx.com",
            "/priapi/v6/dx/trade/multi/broadcast",
            "POST",
            req_body=json.dumps({"data": {"chainId": "56"}}),
            resp_body=json.dumps(
                {
                    "code": "0",
                    "data": {"transactionHash": "0x" + "ab" * 32, "orderId": "SAME-1"},
                }
            ),
        )
        A.addons[0].request(fo)
        A.addons[0].response(fo)
        lb, lo = A.STORE.legs[run]
        self.assertFalse(lo.get("dup"), "跨平台同 orderId 不合并")
        self.assertEqual((lb.get("slot"), lo.get("slot")), (0, 1), "各占其平台槽")
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate", 0), 0)
        A.addons[0].websocket_message(self._fin("SAME-1", "cid-X1", "0x" + "b1" * 32))
        A.addons[0].websocket_message(
            ws_frame(
                "wsdexpri.okx.com",
                json.dumps(
                    {
                        "arg": {"channel": "dex-swap-order-info"},
                        "data": {
                            "dexData": {
                                "status": "1",
                                "orderId": "SAME-1",
                                "transactionHash": "0x" + "ab" * 32,
                            }
                        },
                    }
                ),
            )
        )
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"],
            {"requested": 2, "attempted": 2, "observed": 2, "unbound": 0},
            "两平台各自 attempted/observed——互不吞并",
        )
        self.assertIsNotNone(tl["legs"][0]["hkL1aMs"])
        self.assertIsNotNone(tl["legs"][1]["hkL1aMs"])

    def test_dup_leg_conflicting_order_id_reopens_independent(self):
        """③同 clientOrderId 两次成功 ack 但 orderId 不同（docs/binance.md：一轮 >1
        distinct orderId = multiOrderSuspect）——dup 判定被响应身份推翻：重开为独立
        候选、按自身身份重新绑槽（无空槽 → unbound 诊断如实），后续帧各归其腿。
        旧码（r8）：永久 dup + FINISHED(oid-2) 整帧拒收——第二订单执行证据被吞。
        （本用例与 TestR8IdentityRouting.test_dup_with_conflicting_order_id_not_attributed
        互补：那边考帧归属/计数，这边考重开机制本身——含 formal 被否决时子腿重开）。"""
        run = "run-r9m"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 2,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 2}],
            },
        )
        f1 = self._req("cid-M1")
        self._resp(f1, "oid-M1", "cid-M1")
        f2 = self._req("cid-M1")  # 建腿时 dup（请求侧 cid 正向同单）
        lz = A.STORE.legs[run][1]
        self.assertTrue(lz.get("dup"))
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate"), 1)
        self._resp(f2, "oid-M2", "cid-M1")  # 冲突身份 → 同单证明被推翻 → 重开
        self.assertFalse(lz.get("dup"), "冲突身份到达后重开为独立候选")
        self.assertIsNone(lz.get("_dupOf"))
        self.assertTrue(
            lz.get("_multiOrderSuspect"), "multi-order-suspect 内部审计标记"
        )
        self.assertEqual(lz.get("slot"), 0, "有空槽——按自身身份重新绑槽")
        self.assertEqual(
            A.STORE.diag.get("orderReqDuplicate", 0), 0, "重开改记 dup 计数"
        )
        self.assertEqual(A.STORE.diag.get("orderDupReopened"), 1)
        A.addons[0].websocket_message(self._fin("oid-M2", "cid-M1", "0x" + "2d" * 32))
        A.addons[0].websocket_message(self._fin("oid-M1", "cid-M1", "0x" + "1d" * 32))
        lf = A.STORE.legs[run][0]
        self.assertIsNotNone(lz.get("tSuccessPushMs"), "重开腿收自己的成功帧")
        self.assertIsNotNone(lf.get("tSuccessPushMs"))
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"],
            {"requested": 2, "attempted": 2, "observed": 2, "unbound": 0},
            "两个 distinct order 各占其槽、各自 observed——计数如实",
        )
        self.assertEqual(tl["diag"].get("orderDupReopened"), 1, "per-run diag 透出重开")

    def test_formal_veto_reopens_dup_children(self):
        """③二态：dup 腿的 orderId 先并入 formal 锚（首写）；formal 自己的响应揭示
        另一 orderId → formal 锚自相矛盾被否决 → 其 dup 子腿同单证明同失效，重开为
        独立候选。formal 降级 anchor-conflict、observed 不增加；重开腿收自己的成功帧。"""
        run = "run-r9v"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 2,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 2}],
            },
        )
        f1 = self._req("cid-V1")  # formal（请求侧锚只有 cid）
        f2 = self._req("cid-V1")  # 建腿时 dup
        self._resp(f2, "oid-V2", "cid-V1")  # dup 腿 orderId 并入 formal 锚（首写）
        self._resp(f1, "oid-V1", "cid-V1")  # formal 自身响应冲突 → 否决 → 子腿重开
        lf, lz = A.STORE.legs[run]
        self.assertTrue(
            lf.get("_anchorVeto"), "formal 锚（含 dup 并入值）与自身响应冲突 → 否决"
        )
        self.assertEqual(lf.get("incomplete"), "anchor-conflict")
        self.assertEqual(
            (lf.get("_anchor") or {}).get("orderId"),
            "oid-V2",
            "锚首写冻结——冲突值不覆盖",
        )
        self.assertFalse(lz.get("dup"), "formal 被否决 → dup 子腿重开为独立候选")
        self.assertTrue(lz.get("_multiOrderSuspect"))
        self.assertEqual(lz.get("slot"), 0, "重开后按容量补绑")
        self.assertEqual(A.STORE.diag.get("orderDupReopened"), 1)
        self.assertEqual(A.STORE.diag.get("orderReqDuplicate", 0), 0)
        A.addons[0].websocket_message(self._fin("oid-V2", "cid-V1", "0x" + "2v" * 32))
        self.assertIsNotNone(lz.get("tSuccessPushMs"), "重开腿收 oid-V2 成功帧")
        self.assertIsNone(lf.get("tSuccessPushMs"), "被否决 formal 不写成功")
        A.addons[0].websocket_message(self._fin("oid-V1", "cid-V1", "0x" + "1v" * 32))
        self.assertIsNone(
            lf.get("tSuccessPushMs"), "oid-V1 帧与两腿均冲突——不归属（不猜）"
        )
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"],
            {"requested": 2, "attempted": 2, "observed": 1, "unbound": 0},
            "被否决 formal 占 attempted（槽已消费）但不计 observed",
        )
        self.assertEqual(
            tl["diag"].get("wsFrameAnchorConflict"), 1, "oid-V1 帧拒收可见"
        )
        self.assertEqual(tl["diag"].get("orderDupReopened"), 1)

    def test_transfer_inflight_poller_receipt_redirects_to_new_formal(self):
        """①闩锁残余（成功路径）：Y 先响应带 hash 成 formal 并起 poller；X 身份揭示
        触发移交后，Y 的在飞 poller 成功回写必须经 dup→formal 重定向落 X，且闩锁是
        **移交**不是复制——Y 闩锁释放，否则关窗后 run 被 R25 pin 死永不回收。"""
        run = "run-r9p"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 2,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 2}],
            },
        )
        h = "0x" + "ab" * 32
        fx = self._req()
        time.sleep(0.02)
        fy = self._req()
        self._resp(fy, "oid-P1", "cid-P1", h)  # Y formal + txHash → 闩锁+poller（桩）
        ly = A.STORE.legs[run][1]
        self.assertTrue(ly.get("receiptPolling"))
        self._resp(fx, "oid-P1", "cid-P1", h)  # X 身份揭示 → formal 资格移交
        lx = A.STORE.legs[run][0]
        self.assertTrue(lx.get("receiptPolling"), "闩锁随 formal 资格移交 X")
        self.assertFalse(ly.get("receiptPolling"), "移交而非复制——Y 闩锁释放")
        A._settle_receipt(ly, self._receipt_ok())  # Y 的在飞 poller 成功回写
        self.assertIsNotNone(
            lx.get("tReceiptMs"), "receipt 经 dup→formal 重定向落新 formal"
        )
        self.assertIsNone(ly.get("tReceiptMs"), "receipt 同成功一样永不留在 dup 腿")
        A.STORE.mark_close(run)
        with A.STORE.lock:
            self.assertFalse(
                A.STORE._run_pinned_locked(run), "无遗留闩锁——run 可正常回收（R25）"
            )
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 1, "observed": 1, "unbound": 0}
        )
        self.assertEqual(
            tl["legs"][0]["hkL3Ms"],
            round(lx["tReceiptMs"] - lx["tOrderOutMs"], 1),
            "L3′ 起点同样按最早请求",
        )

    def test_transfer_inflight_poller_timeout_redirects_to_new_formal(self):
        """①闩锁残余（失败路径）：移交后 Y 的在飞 poller 超时——incomplete 同样重定向
        到 X（receipt 路径归新 formal），Y 闩锁释放。旧码：超时记在 dup 腿 Y 上、
        X 闩锁永挂（无 receipt 无 incomplete）→ run 被 R25 pin 死。"""
        run = "run-r9pt"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 2,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 2}],
            },
        )
        h = "0x" + "ab" * 32
        fx = self._req()
        time.sleep(0.02)
        fy = self._req()
        self._resp(fy, "oid-PT1", "cid-PT1", h)
        self._resp(fx, "oid-PT1", "cid-PT1", h)  # 移交
        lx, ly = A.STORE.legs[run]
        A._settle_incomplete(ly, "receipt poll timeout")  # Y 的在飞 poller 超时回写
        self.assertEqual(
            lx.get("incomplete"),
            "receipt poll timeout",
            "失败回写同样重定向到新 formal",
        )
        self.assertIsNone(ly.get("incomplete"))
        self.assertFalse(ly.get("receiptPolling"))
        A.STORE.mark_close(run)
        with A.STORE.lock:
            self.assertFalse(
                A.STORE._run_pinned_locked(run),
                "闩锁+incomplete/receipt 必居其一——不 pin 死",
            )
        tl = A.STORE.timeline(run)
        self.assertEqual(
            tl["counts"], {"requested": 2, "attempted": 1, "observed": 0, "unbound": 0}
        )
        self.assertEqual(tl["legs"][0]["incomplete"], "receipt poll timeout")
        self.assertEqual(tl["legs"][1]["incomplete"], "duplicate-of-bound-leg")

    def test_veto_after_receipt_excluded_from_observed(self):
        """RHF-09 残余防线：receipt 先于否决落定的腿，observed 仍不增加（counts 层
        排除）——腿级时间戳保留作诊断，incomplete=anchor-conflict 首写落盘。"""
        run = "run-r9rv"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 1,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 1}],
            },
        )
        h = "0x" + "cd" * 32
        f = self._req("cid-R1")
        self._resp(f, "oid-R1", "cid-R1", h)  # 锚 + hash → 闩锁+poller（桩）
        leg = A.STORE.legs[run][0]
        A._settle_receipt(leg, self._receipt_ok())  # receipt 先落定
        self.assertIsNotNone(leg.get("tReceiptMs"))
        A.STORE.update_anchor(leg, {"orderId": "oid-R9"})  # 冲突身份后到 → 否决
        self.assertTrue(leg.get("_anchorVeto"))
        self.assertEqual(leg.get("incomplete"), "anchor-conflict")
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run)
        self.assertEqual(tl["counts"]["attempted"], 1)
        self.assertEqual(
            tl["counts"]["observed"],
            0,
            "receipt 先于否决落定 → counts 层排除（observed 不增加）",
        )
        self.assertEqual(tl["legs"][0]["incomplete"], "anchor-conflict")


# ── r10（2026-09-08）：RHF-09 残余 late veto 撤销 / RHF-17 休眠 parser 逐 entry + 早到帧缓冲 ──


class TestR10LateVetoRevocation(unittest.TestCase):
    """RHF-09 残余（r10）：success/promotion/receipt 先于否决落定的 late-veto 路径——
    合格指标在 timeline 序列化层统一撤销（hkL1aMs/hkL3Ms → null，撤销值入
    hkL1aMsVetoed/hkL3MsVetoed 诊断键，腿带 anchorVeto:true），内部腿的原始事件
    时间保留（证据不删）；observed 不增加。报告反例：请求→subscribe A→success A
    →HTTP 响应明确 B——旧码（r9）veto 后仍导出 hkL1aMs≈1000ms，页面照计
    （「获证据 0 笔；推送 1000ms；推送 1/1」矛盾）。"""

    RELAY_A = "0x" + "a1" * 32
    RELAY_B = "0x" + "b2" * 32

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        self.polls = []
        A._poll_receipt = lambda *a, **k: self.polls.append(a)
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    def test_success_then_conflict_revokes_qualified_metrics(self):
        """报告残余形状（单腿）：B preview 请求（无响应）→ A 的 subscribe 挂到 B
        → success(relayA) 帧落 B（晋升 order、绑槽、hkL1a 已有值）→ B 的迟到 HTTP
        响应明确 relayB → 锚自相矛盾 → 否决。timeline 不得再导出合格时长。"""
        run = "run-r10lv"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 2,
                "legs": [{"platform": "fomo", "chain": "robinhood", "rounds": 2}],
            },
        )
        fb = FakeFlow(
            "prod-api.fomo.family",
            "/swaps/v2",
            "POST",
            req_body=json.dumps({"chainId": 4663}),
        )
        A.addons[0].request(fb)  # B 请求（响应稍后）
        A.addons[0].websocket_message(
            ws_frame(
                "ws.relay.link",
                json.dumps(
                    {
                        "type": "subscribe",
                        "event": "request.status.updated",
                        "filters": {"id": self.RELAY_A},
                    }
                ),
                from_client=True,
            )
        )
        A.addons[0].websocket_message(
            ws_frame(
                "ws.relay.link",
                json.dumps(
                    {
                        "event": "request.status.updated",
                        "data": {
                            "status": "success",
                            "requestId": self.RELAY_A,
                            "txHashes": ["0x" + "12" * 32],
                            "destinationChainId": 4663,
                        },
                    }
                ),
            )
        )
        leg = A.STORE.legs[run][0]
        self.assertIsNotNone(
            leg.get("tSuccessPushMs"), "成功先于否决落定（内部原始时间保留——证据不删）"
        )
        self.assertEqual(
            leg.get("anchorRole"), "order", "已被成功帧晋升（内部事实保留）"
        )
        recorded = round(leg["tSuccessPushMs"] - leg["tOrderOutMs"], 1)
        fb.response = FakeResp(
            json.dumps(
                {
                    "success": True,
                    "responseObject": {
                        "v2Swap": {
                            "relaySwapId": self.RELAY_B,
                            "destinationChainId": 4663,
                        }
                    },
                }
            )
        )
        A.addons[0].response(fb)  # 迟到响应明确 relayB → 冲突 → 否决
        self.assertTrue(leg.get("_anchorVeto"))
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run)
        out = tl["legs"][0]
        self.assertEqual(
            out["anchorVeto"], True, "腿带 anchorVeto:true——不再以 bound/eligible 呈现"
        )
        self.assertIsNone(
            out["hkL1aMs"], "合格 L1a′ 撤销（r9 残余：仍导出 1000ms 入页面统计）"
        )
        self.assertEqual(out["hkL1aMsVetoed"], recorded, "被撤销值保留在诊断键")
        self.assertIsNone(out["hkL3Ms"])
        self.assertIsNone(out["hkL3MsVetoed"], "无 receipt 落地 → 无撤销值")
        self.assertEqual(
            out["incomplete"], "anchor-conflict", "否决原因保留（首写落盘）"
        )
        self.assertIsNotNone(
            out["tSuccessPushMs"], "原始事件时间仍在私有 raw（诊断用，不删证据）"
        )
        self.assertIsNotNone(out["evidence"]["wsFrameSha"])
        self.assertEqual(
            tl["counts"],
            {"requested": 2, "attempted": 1, "observed": 0, "unbound": 0},
            "槽已消费仍占 attempted；observed 不增加",
        )
        ph = tl["legs"][1]
        self.assertEqual(
            (ph["anchorVeto"], ph["hkL1aMsVetoed"], ph["hkL3MsVetoed"]),
            (False, None, None),
            "占位腿形状齐整",
        )

    def test_receipt_then_conflict_revokes_l3(self):
        """receipt-already-latched-then-conflict：receipt 先落定（hkL3 有值），冲突身份
        后到 → 否决 → hkL3Ms 撤销为 null、撤销值入 hkL3MsVetoed；observed 不增加。"""
        run = "run-r10rv"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 1,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 1}],
            },
        )
        h = "0x" + "cd" * 32
        f = FakeFlow(
            "web3.binance.com",
            "/bapi/defi/v2/private/wallet-direct/web-dex/place-order",
            "POST",
            req_body=json.dumps({"clientOrderId": "cid-R1", "chain": "BSC"}),
            resp_body=json.dumps(
                {
                    "code": "000000",
                    "data": {
                        "orderId": "oid-R1",
                        "clientOrderId": "cid-R1",
                        "txHash": h,
                    },
                }
            ),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        leg = A.STORE.legs[run][0]
        A._settle_receipt(
            leg,
            {
                "rpc": "https://rpc.invalid",
                "chain": "bsc",
                "pollIntervalMs": 800,
                "status": 1,
                "blockHash": "0x" + "11" * 32,
                "blockNumber": 7,
            },
        )
        self.assertIsNotNone(leg.get("tReceiptMs"))
        recorded_l3 = round(leg["tReceiptMs"] - leg["tOrderOutMs"], 1)
        A.STORE.update_anchor(leg, {"orderId": "oid-R9"})  # 冲突身份后到 → 否决
        self.assertTrue(leg.get("_anchorVeto"))
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run)
        out = tl["legs"][0]
        self.assertIsNone(out["hkL3Ms"], "合格 L3′ 撤销")
        self.assertEqual(out["hkL3MsVetoed"], recorded_l3, "被撤销 L3′ 值保留在诊断键")
        self.assertIsNotNone(out["tReceiptMs"], "原始 receipt 观察时间保留（诊断）")
        self.assertIsNotNone(out["evidence"]["receipt"], "receipt 证据本体保留（诊断）")
        self.assertEqual(tl["counts"]["observed"], 0)

    def test_clean_leg_serialization_shape(self):
        """非否决腿：anchorVeto=False、*Vetoed 键恒 null（新增键只增不改，形状稳定）。"""
        run = "run-r10ok"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 1,
                "legs": [{"platform": "binance", "chain": "bsc", "rounds": 1}],
            },
        )
        f = FakeFlow(
            "web3.binance.com",
            "/bapi/defi/v2/private/wallet-direct/web-dex/place-order",
            "POST",
            req_body=json.dumps({"clientOrderId": "cid-OK", "chain": "BSC"}),
            resp_body=json.dumps(
                {
                    "code": "000000",
                    "data": {"orderId": "oid-OK", "clientOrderId": "cid-OK"},
                }
            ),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        A.addons[0].websocket_message(
            ws_frame(
                "web3-stream.binance.com",
                json.dumps(
                    {
                        "stream": "w3pc:x",
                        "data": {
                            "bizKey": "DEX_ALL_ORDER",
                            "content": {
                                "orderId": "oid-OK",
                                "clientOrderId": "cid-OK",
                                "status": "FINISHED",
                                "orderTxId": "0x" + "0a" * 32,
                            },
                        },
                    }
                ),
            )
        )
        out = A.STORE.timeline(run)["legs"][0]
        self.assertEqual(out["anchorVeto"], False)
        self.assertIsNone(out["hkL1aMsVetoed"])
        self.assertIsNone(out["hkL3MsVetoed"])
        self.assertIsNotNone(out["hkL1aMs"], "非否决腿合格指标不受影响")


class TestR10GmgnOkxPerEntry(unittest.TestCase):
    """RHF-17a/b（r10）：GMGN/OKX 休眠 parser 逐 entry 化。①HTTP 响应：同一 entry 的
    身份/hash 才可组合——跨 entry 拼接曾制造假锚（entry1 的 oi + entry2 的 hash），
    真实成功帧随即「冲突」被拒/误否决（跨 entry 假冲突）；②WS 帧：data 列表逐
    entry 独立路由/判定——目标在第二 entry 不再漏关联（旧码 data[0]-only）。"""

    def setUp(self):
        fresh()
        orig = A._poll_receipt
        A._poll_receipt = lambda *a, **k: None
        self.addCleanup(setattr, A, "_poll_receipt", orig)

    SIG_A = "5" * 88
    SIG_B = "4" * 88

    def _gmgn_order(self, run, resp_body):
        f = FakeFlow(
            "gmgn.ai",
            "/tapi/v1/swap_batch_order",
            "POST",
            req_body=json.dumps({"chain": "sol"}),
            resp_body=resp_body,
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        return f

    def test_gmgn_cross_entry_no_manufactured_conflict(self):
        """响应 data=[{"oi": ordA}, {"hash": hB}]（不同 entry 的 disjoint 字段）：
        旧码整 body 归并 → 锚 {orderId: ordA, txHash: hB}——ordA 的真实成功帧（hA）
        随即与锚「冲突」被拒（假冲突）。r10：跨 entry 不拼接 → 锚 fail-closed 为空
        + ambiguous 可见；成功帧零命中如实计数；腿不否决、不冲突。"""
        body = json.dumps({"code": 0, "data": [{"oi": "ordA"}, {"hash": self.SIG_B}]})
        ids = A._gmgn_resp_ids(body)
        self.assertNotIn(
            "orderId", ids, "跨 entry 不拼接身份（旧码：oi(ordA)+hash(hB) 拼成同一锚）"
        )
        self.assertNotIn("txHash", ids)
        self.assertEqual(
            ids.get("_ambiguous"),
            ["orderId", "txHash"],
            "无法证明同单 → fail-closed + 歧义可见",
        )
        run = "run-r10xa"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 1,
                "legs": [{"platform": "gmgn", "chain": "solana", "rounds": 1}],
            },
        )
        self._gmgn_order(run, body)
        leg = A.STORE.legs[run][0]
        self.assertEqual(
            leg.get("_anchor") or {}, {}, "无锚——成功帧结构上不可能正向关联"
        )
        A.addons[0].websocket_message(
            ws_frame(
                "ws.gmgn.ai",
                json.dumps(
                    {
                        "channel": "tg_processed_order_info",
                        "data": [
                            {
                                "h": "3" * 88,
                                "oi": "ordA",
                                "st": "successful",
                                "si": "buy",
                                "ch": "sol",
                            }
                        ],
                    }
                ),
            )
        )
        self.assertFalse(leg.get("_anchorVeto", False), "不制造假冲突/误否决")
        self.assertIsNone(leg.get("tSuccessPushMs"))
        A.STORE.mark_close(run)
        tl = A.STORE.timeline(run)
        self.assertNotIn(
            "anchorConflict",
            tl["diag"],
            "无跨 entry 拼接 → 无锚冲突计数（旧码 wsFrameAnchorConflict）",
        )
        self.assertNotIn("wsFrameAnchorConflict", tl["diag"])
        self.assertEqual(
            tl["diag"].get("wsFrameNoLegMatch"), 1, "无锚腿的成功帧零命中——如实计数不吞"
        )
        self.assertEqual(tl["legs"][0]["incomplete"], "no-success-observed")
        self.assertEqual(
            tl["legs"][0]["evidence"]["anchor"]["ambiguous"], ["orderId", "txHash"]
        )
        self.assertEqual(tl["counts"]["observed"], 0)

    def test_gmgn_second_entry_identity_associated(self):
        """目标在第二 entry（RHF-17b）：响应 data=[{噪声}, {oi, hash}]——身份在第二
        entry 也照常建锚（deferred binding，不再首-entry-only）；WS 帧目标条目在
        data[1] 也照常路由/采纳（旧码 ids 只取 data[0] → 整帧漏关联）。"""
        run = "run-r10x2"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 1,
                "legs": [{"platform": "gmgn", "chain": "solana", "rounds": 1}],
            },
        )
        self._gmgn_order(
            run,
            json.dumps(
                {
                    "code": 0,
                    "data": [{"note": "noise-entry"}, {"oi": "g1", "hash": self.SIG_A}],
                }
            ),
        )
        leg = A.STORE.legs[run][0]
        self.assertEqual(
            (leg.get("_anchor") or {}).get("orderId"),
            "g1",
            "第二 entry 的身份照常建立锚",
        )
        A.addons[0].websocket_message(
            ws_frame(
                "ws.gmgn.ai",
                json.dumps(
                    {
                        "channel": "tg_order_info",
                        "data": [
                            {
                                "h": self.SIG_B,
                                "oi": "gOther",
                                "st": "successful",
                                "si": "buy",
                                "ch": "sol",
                            },  # 他单成功（entry 0）
                            {
                                "h": self.SIG_A,
                                "oi": "g1",
                                "st": "successful",
                                "si": "buy",
                                "ch": "sol",
                            },
                        ],
                    }
                ),
            )
        )  # 本单成功（entry 1）
        self.assertIsNotNone(
            leg.get("tSuccessPushMs"),
            "目标在第二 entry 照常采纳（旧码：data[0]-only 漏报）",
        )
        self.assertEqual(
            leg.get("txHash"), self.SIG_A, "hash 取本单 entry——不取 entry 0 的他单 hash"
        )
        tl = A.STORE.timeline(run)
        self.assertEqual(tl["counts"]["observed"], 1)
        self.assertEqual(
            tl["diag"].get("wsFrameNoLegMatch"), 1, "他单 entry 零命中如实计数"
        )
        self.assertEqual(
            tl["legs"][0]["evidence"]["successAnchorKeys"], ["orderId", "txHash"]
        )

    def test_okx_second_entry_associated(self):
        """OKX 同律：data 列表逐 dexData entry——目标在第二 entry 照常关联；
        HTTP 侧 batchBroadcast 的 data 列表同样逐 entry（身份/hash 同 entry 才组合）。"""
        h1, h2 = "0x" + "ab" * 32, "0x" + "cd" * 32
        # 单元级：跨 entry disjoint 字段不拼接
        amb = A._okx_resp_ids(
            json.dumps(
                {"code": "0", "data": [{"orderId": "ord-1"}, {"transactionHash": h1}]}
            )
        )
        self.assertNotIn("orderId", amb)
        self.assertEqual(amb.get("_ambiguous"), ["orderId", "txHash"])
        # 单元级：正向一致的多个 entry 并集（同单证据分散在多 entry 仍可关联）
        same = A._okx_resp_ids(
            json.dumps(
                {
                    "code": "0",
                    "data": [
                        {"orderId": "ord-1", "transactionHash": h1},
                        {"orderId": "ord-1"},
                    ],
                }
            )
        )
        self.assertEqual(
            same, {"orderId": "ord-1", "txHash": h1}, "共同键全等的 entry 可并集"
        )
        run = "run-r10o2"
        A.STORE.mark_open(
            run,
            manifest={
                "version": 1,
                "runId": run,
                "rounds": 1,
                "legs": [{"platform": "okx", "chain": "bsc", "rounds": 1}],
            },
        )
        f = FakeFlow(
            "web3.okx.com",
            "/priapi/v6/dx/trade/multi/batchBroadcast",
            "POST",
            req_body=json.dumps({"data": {"chainId": "56"}}),
            resp_body=json.dumps(
                {"code": "0", "data": [{"orderId": "ord1", "transactionHash": h1}]}
            ),
        )
        A.addons[0].request(f)
        A.addons[0].response(f)
        A.addons[0].websocket_message(
            ws_frame(
                "wsdexpri.okx.com",
                json.dumps(
                    {
                        "arg": {"channel": "dex-across-order-info"},
                        "data": [
                            {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "ordOther",
                                    "transactionHash": h2,
                                    "chainId": "56",
                                }
                            },
                            {
                                "dexData": {
                                    "status": "1",
                                    "orderId": "ord1",
                                    "transactionHash": h1,
                                    "chainId": "56",
                                }
                            },
                        ],
                    }
                ),
            )
        )
        leg = A.STORE.legs[run][0]
        self.assertIsNotNone(leg.get("tSuccessPushMs"), "目标在第二 entry 照常采纳")
        self.assertEqual(leg.get("txHash"), h1, "hash 取本单 entry")
        tl = A.STORE.timeline(run)
        self.assertEqual(tl["counts"]["observed"], 1)
        self.assertEqual(
            tl["diag"].get("wsFrameNoLegMatch"), 1, "他单 entry 零命中如实计数"
        )


class TestR10EarlyWsBuffer(unittest.TestCase):
    """RHF-17c（r10）：WS 成功帧先于其 HTTP 响应到达（docs/okx.md §2.4：Sol 的
    /broadcast 常晚于 WS 确认）——缓冲保留单调源时间/窗口 epoch/方向，响应建立锚
    后按实时帧同一管线重审采纳（成功时刻用源时间）；关窗/重放/冲突不采纳。"""

    H1 = "0x" + "ab" * 32

    def setUp(self):
        fresh()
        orig = A._jsonrpc
        A._jsonrpc = lambda url, m, p, timeout=10: {
            "status": "0x1",
            "blockNumber": "0x1",
            "blockHash": "0x" + "99" * 32,
            "transactionHash": p[0],
        }
        self.addCleanup(setattr, A, "_jsonrpc", orig)

    def _manifest(self, run):
        return {
            "version": 1,
            "runId": run,
            "rounds": 1,
            "legs": [{"platform": "okx", "chain": "bsc", "rounds": 1}],
        }

    def _okx_req(self, resp_oid="ord1", resp_h=H1):
        return FakeFlow(
            "web3.okx.com",
            "/priapi/v6/dx/trade/multi/broadcast",
            "POST",
            req_body=json.dumps({"data": {"chainId": "56"}}),
            resp_body=json.dumps(
                {"code": "0", "data": {"transactionHash": resp_h, "orderId": resp_oid}}
            ),
        )

    def _okx_success(self, oid="ord1", h=H1):
        return ws_frame(
            "wsdexpri.okx.com",
            json.dumps(
                {
                    "arg": {"channel": "dex-swap-order-info"},
                    "data": {
                        "dexData": {
                            "status": "1",
                            "orderId": oid,
                            "transactionHash": h,
                            "chainId": "56",
                        }
                    },
                }
            ),
        )

    def test_success_before_http_response_adopted_with_source_time(self):
        """核心形状：请求 →（响应未到）WS 成功帧 → 缓冲（无成功、计 wsFrameBuffered）
        → 响应建立锚 → 重审采纳，成功时刻 = 帧源时间（非重审时刻）。"""
        run = "run-r10eb"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        f = self._okx_req()
        A.addons[0].request(f)
        time.sleep(0.05)
        A.addons[0].websocket_message(self._okx_success())  # 成功帧先于响应
        leg = A.STORE.legs[run][0]
        self.assertIsNone(
            leg.get("tSuccessPushMs"), "锚未建立——早到帧不得直接采纳（缓冲待定）"
        )
        self.assertEqual(A.STORE.diag.get("wsFrameBuffered"), 1)
        self.assertNotIn("wsFrameNoLegMatch", A.STORE.diag, "锚待定 → 零命中推迟计数")
        time.sleep(0.05)
        A.addons[0].response(f)  # 响应建立锚 → 重审
        self.assertIsNotNone(leg.get("tSuccessPushMs"), "响应落地后早到帧被采纳")
        self.assertLess(
            leg["tSuccessPushMs"],
            leg["tFirstRespMs"],
            "成功时刻 = 帧源时间（先于响应）",
        )
        time.sleep(0.25)
        tl = A.STORE.timeline(run)
        out = tl["legs"][0]
        self.assertEqual(
            out["hkL1aMs"], round(leg["tSuccessPushMs"] - leg["tOrderOutMs"], 1)
        )
        self.assertGreaterEqual(
            out["hkL1aMs"], 40.0, "L1a′ 含请求→成功帧真实间隔（不从响应起算）"
        )
        self.assertIsNotNone(out["hkL3Ms"], "hash 来自响应锚/帧一致 → receipt 轮询正常")
        self.assertEqual(
            tl["counts"], {"requested": 1, "attempted": 1, "observed": 1, "unbound": 0}
        )
        self.assertNotIn("wsFrameNoLegMatch", tl["diag"])

    def test_early_frame_window_closed_not_adopted(self):
        """关窗不采纳：缓冲后未等响应即关窗 → freeze 弃缓冲并按零命中终态计数；
        迟到的响应（腿已冻结）不再建锚、不再重审。"""
        run = "run-r10ec"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        f = self._okx_req()
        A.addons[0].request(f)
        A.addons[0].websocket_message(self._okx_success())
        self.assertEqual(A.STORE.diag.get("wsFrameBuffered"), 1)
        A.STORE.mark_close(run)  # 关窗 → 弃缓冲
        self.assertEqual(
            A.STORE.diag.get("wsFrameNoLegMatch"),
            1,
            "缓冲单元零命中终态——如实计数不静默",
        )
        A.addons[0].response(f)  # 迟到响应不得复活
        leg = A.STORE.legs[run][0]
        self.assertIsNone(leg.get("tSuccessPushMs"))
        self.assertEqual((leg.get("_anchor") or {}), {}, "冻结腿不再建锚")
        tl = A.STORE.timeline(run)
        self.assertEqual(tl["counts"]["observed"], 0)
        self.assertEqual(tl["legs"][0]["incomplete"], "no-success-observed")

    def test_early_frame_conflict_not_adopted(self):
        """冲突不采纳：缓冲帧 {orderId: ord1, txHash: hWrong}；响应锚 {ord1, hRight}
        → 重审路由判冲突（同单一等一不等）→ 终态拒收，不缓冲、不采纳、hash 不被覆盖。"""
        orig_poll = A._poll_receipt
        A._poll_receipt = lambda *a, **k: None  # 隔离 receipt 路径——只考冲突门
        self.addCleanup(setattr, A, "_poll_receipt", orig_poll)
        run = "run-r10ecf"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        f = self._okx_req()
        A.addons[0].request(f)
        h_wrong = "0x" + "77" * 32
        A.addons[0].websocket_message(self._okx_success(h=h_wrong))
        self.assertEqual(A.STORE.diag.get("wsFrameBuffered"), 1)
        A.addons[0].response(f)
        leg = A.STORE.legs[run][0]
        self.assertIsNone(leg.get("tSuccessPushMs"), "与响应锚冲突的缓冲帧不得采纳")
        self.assertEqual(leg.get("txHash"), self.H1, "锚 hash 不被冲突帧覆盖")
        self.assertEqual(
            A.STORE.diag.get("wsFrameAnchorConflict"), 1, "冲突终态如实计数"
        )
        self.assertEqual(A.STORE.ws_buffer.get(run) or [], [], "冲突是终态——不回缓冲")
        tl = A.STORE.timeline(run)
        self.assertEqual(tl["counts"]["observed"], 0)

    def test_early_frame_replay_not_double_adopted(self):
        """重放不采纳第二份：两份相同成功帧先于响应到达 → 都缓冲 → 重审时第一份
        采纳、第二份首写冻结拦截；采纳后实时重放同样不写。成功时刻 = 首帧源时间。"""
        run = "run-r10er"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        f = self._okx_req()
        A.addons[0].request(f)
        time.sleep(0.03)
        A.addons[0].websocket_message(self._okx_success())  # 首帧
        time.sleep(0.03)
        A.addons[0].websocket_message(self._okx_success())  # 重放副本（同缓冲）
        A.addons[0].response(f)
        leg = A.STORE.legs[run][0]
        t_first = leg.get("tSuccessPushMs")
        self.assertIsNotNone(t_first)
        evs = [e for e in A.STORE.events[run] if e.get("kind") == "ws_success"]
        self.assertEqual(len(evs), 1, "重放副本不产生第二次成功事件")
        A.addons[0].websocket_message(self._okx_success())  # 采纳后实时重放
        self.assertEqual(leg.get("tSuccessPushMs"), t_first, "首写冻结——重放不覆盖")
        time.sleep(0.25)
        tl = A.STORE.timeline(run)
        self.assertEqual(tl["counts"]["observed"], 1)

    def test_buffer_gate_requires_anchorless_leg(self):
        """缓冲门：该平台开窗腿均已有锚（响应已到）→ 零命中即终态，不缓冲——
        他单/陈旧帧不占用预算（与 r8/r9 实时帧计数口径一致）。"""
        run = "run-r10eg"
        A.STORE.mark_open(run, manifest=self._manifest(run))
        f = self._okx_req()
        A.addons[0].request(f)
        A.addons[0].response(f)  # 锚已建立
        A.addons[0].websocket_message(
            self._okx_success(oid="ord-OTHER", h="0x" + "55" * 32)
        )
        self.assertNotIn("wsFrameBuffered", A.STORE.diag, "无锚待定腿 → 不缓冲")
        self.assertEqual(
            A.STORE.diag.get("wsFrameNoLegMatch"), 1, "零命中即终态计数（不推迟）"
        )
        self.assertEqual(A.STORE.ws_buffer.get(run) or [], [])


# ── r10 续：控制面端口 env 覆盖 / 采集身份 captureIdentity 持久化 ──


class _EnvGuard:
    """具名 env 变量的保存/恢复（绝不 dump/打印 environ 内容）。"""

    def __init__(self, test, *names):
        self._saved = {n: os.environ.get(n) for n in names}
        test.addCleanup(self.restore)

    def restore(self):
        for n, v in self._saved.items():
            if v is None:
                os.environ.pop(n, None)
            else:
                os.environ[n] = v


class TestR10CtrlPortEnv(unittest.TestCase):
    """r10：SPEEDEX_HK_CTRL_PORT 覆盖控制面监听端口（隔离实验 unit 与生产同机并存，
    speedex-mitm-exp.service 设 18072）——缺省/非法一律 8072，绝不因坏 env 崩溃。"""

    def setUp(self):
        fresh()
        self._env = _EnvGuard(self, "SPEEDEX_HK_CTRL_PORT")

    def test_port_resolver_default_and_valid(self):
        os.environ.pop("SPEEDEX_HK_CTRL_PORT", None)
        self.assertEqual(A._ctrl_port(), 8072, "未设置 → 缺省 8072（逐字节旧行为）")
        os.environ["SPEEDEX_HK_CTRL_PORT"] = "18072"
        self.assertEqual(A._ctrl_port(), 18072)
        os.environ["SPEEDEX_HK_CTRL_PORT"] = " 18072 "
        self.assertEqual(A._ctrl_port(), 18072, "空白容忍")
        os.environ["SPEEDEX_HK_CTRL_PORT"] = "1"
        self.assertEqual(A._ctrl_port(), 1, "下界合法")
        os.environ["SPEEDEX_HK_CTRL_PORT"] = "65535"
        self.assertEqual(A._ctrl_port(), 65535, "上界合法")

    def test_port_resolver_invalid_falls_back(self):
        for bad in (
            "",
            "   ",
            "abc",
            "18072.5",
            "0x46A8",
            "-1",
            "0",
            "65536",
            "99999",
            "True",
            "1e4",
        ):
            os.environ["SPEEDEX_HK_CTRL_PORT"] = bad
            self.assertEqual(
                A._ctrl_port(), 8072, f"非法值 {bad!r} → 回退 8072（不崩溃）"
            )

    def test_listener_follows_env_port(self):
        """集成：env=18072 → 重绑后控制面在 18072 服务且 8072 不再监听；恢复 env
        后回到 8072。takeover/bindError 注册表不残留污染。"""
        os.environ["SPEEDEX_HK_CTRL_PORT"] = "18072"
        A._start_ctrl()

        def restore_8072():
            os.environ.pop("SPEEDEX_HK_CTRL_PORT", None)
            A._start_ctrl()

        self.addCleanup(restore_8072)
        code, body = _http_json("GET", "http://127.0.0.1:18072/health")
        self.assertEqual((code, body["ok"]), (200, True))
        self.assertEqual(
            body["captureIdentity"]["ctrlPort"], 18072, "health 自报实际监听端口"
        )
        with self.assertRaises(urllib.error.URLError):
            _http_json("GET", "http://127.0.0.1:8072/health")
        reg = A._ctrl_registry()
        self.assertIsNone(reg.get("bindError"))
        self.assertFalse(reg.get("takeover"), "空 Store 重绑不制造 takeover 记录")


class TestR10CaptureIdentity(unittest.TestCase):
    """r10（RHF-15 持久化）：每批采集/解密身份 captureIdentity——实际生效的
    allow_hosts 宿主模式集（mitmproxy ctx.options 真实生效值优先于 env 声明；
    不可观测 → unavailable 不猜）+ 控制面端口；timeline 与 /health 同源。
    只含 host 正则配置事实：有界（≤16 条、每条 ≤512 字符），不落凭据/env dump。"""

    PAT = r"^(.*\.)?(turnkey\.com|padre\.gg|binance\.com|fomo\.family|relay\.link)(:\d+)?$"

    def setUp(self):
        fresh()
        self._env = _EnvGuard(self, "SPEEDEX_HK_ALLOW_HOSTS", "SPEEDEX_HK_CTRL_PORT")
        os.environ.pop("SPEEDEX_HK_ALLOW_HOSTS", None)
        os.environ.pop("SPEEDEX_HK_CTRL_PORT", None)
        self.assertNotIn(
            "mitmproxy", sys.modules, "离线环境无 mitmproxy——env/不可观测路径"
        )

    def _fake_mitmproxy(self, allow_hosts):
        """注入假 mitmproxy 模块（ctx.options.allow_hosts）——离线验证真实生效值路径。"""
        mod = types.ModuleType("mitmproxy")
        mod.ctx = types.SimpleNamespace(
            options=types.SimpleNamespace(allow_hosts=allow_hosts)
        )
        sys.modules["mitmproxy"] = mod
        self.addCleanup(sys.modules.pop, "mitmproxy", None)

    def test_unavailable_when_unobservable(self):
        ci = A._collection_identity()["captureIdentity"]
        self.assertEqual(
            ci,
            {"allowHosts": None, "allowHostsSource": "unavailable", "ctrlPort": 8072},
            "无法自观测且无 env 声明 → 未知不猜（不按当前矩阵反填）",
        )

    def test_env_fallback_declared_source(self):
        os.environ["SPEEDEX_HK_ALLOW_HOSTS"] = self.PAT
        ci = A._collection_identity()["captureIdentity"]
        self.assertEqual(ci["allowHosts"], [self.PAT])
        self.assertEqual(ci["allowHostsSource"], "env", "unit 声明值来源如实标注")
        self.assertEqual(ci["ctrlPort"], 8072)

    def test_mitmproxy_options_preferred_over_env(self):
        os.environ["SPEEDEX_HK_ALLOW_HOSTS"] = "^env-claim$"
        pats = [self.PAT, r"^example\.com$"]
        self._fake_mitmproxy(pats)
        ci = A._collection_identity()["captureIdentity"]
        self.assertEqual(ci["allowHosts"], pats, "真实生效值优先于 env 声明")
        self.assertEqual(ci["allowHostsSource"], "mitmproxy-options")
        # 空列表 = 未设白名单（全量拦截）——是真实生效值，不回退 env
        ci2 = A._capture_identity()
        self.assertEqual(ci2["allowHosts"], pats)
        sys.modules.pop("mitmproxy")
        self._fake_mitmproxy([])
        self.assertEqual(
            A._capture_identity()["allowHosts"],
            [],
            "空 allow_hosts 如实报告（全量拦截事实）",
        )
        self.assertEqual(A._capture_identity()["allowHostsSource"], "mitmproxy-options")

    def test_patterns_bounded(self):
        self._fake_mitmproxy([f"^h{i}\\.example$" for i in range(20)] + ["x" * 600])
        hosts, source = A._effective_allow_hosts()
        self.assertEqual(source, "mitmproxy-options")
        self.assertEqual(len(hosts), 16, "模式集有界 ≤16")
        self.assertTrue(all(len(h) <= 512 for h in hosts), "每条 ≤512 字符")

    def test_identity_in_timeline_and_health(self):
        os.environ["SPEEDEX_HK_ALLOW_HOSTS"] = self.PAT
        tl = A.STORE.timeline("run-ci-none")
        self.assertEqual(
            tl["captureIdentity"]["allowHosts"],
            [self.PAT],
            "timeline 自带采集身份（每批持久化）",
        )
        self.assertEqual(tl["captureIdentity"]["allowHostsSource"], "env")
        code, body = _http_json("GET", "http://127.0.0.1:8072/health")
        self.assertEqual(code, 200)
        self.assertEqual(
            body["captureIdentity"], tl["captureIdentity"], "/health 与 timeline 同源"
        )
        blob = json.dumps(body["captureIdentity"])
        self.assertNotIn("SPEEDEX_", blob, "只含配置事实——不含 env 名/值 dump")


class ChainHeadTests(unittest.TestCase):
    def test_pre_anchor_stale_null_and_closed_window(self):
        now = [1000]
        c = A.ChainHeadWindow(clock=lambda: now[0])
        c.run_id = "head-window"
        from collections import deque

        c.samples = {"solana": deque(maxlen=512), "robinhood": deque(maxlen=512)}
        for value in [None, True, "42", {}]:
            c.add("solana", value, "rpc-poll")
        self.assertEqual(
            c.snapshot("head-window", 1100)["solana"]["status"], "no-pre-anchor-head"
        )
        c.add("solana", 100, "ws-head")
        now[0] = 1200
        c.add("solana", 101, "ws-head")
        self.assertEqual(c.snapshot("head-window", 1100)["solana"]["height"], 100)
        self.assertEqual(
            c.snapshot("head-window", 4000)["solana"]["status"], "stale-head"
        )
        self.assertEqual(c.snapshot("wrong", 1300), {})
        c.close()
        c.add("solana", 200, "ws-head")
        self.assertEqual(c.snapshot("head-window", 1300), {})

    def test_hook_freezes_same_window_head_without_network(self):
        from collections import deque

        original = A.HEADS
        c = A.ChainHeadWindow(clock=lambda: 1000)
        c.run_id = "head-window"
        c.samples = {"robinhood": deque(maxlen=512)}
        c.add("robinhood", {"number": "0x64", "hash": "0x" + "aa" * 32}, "ws-head")
        A.HEADS = c
        try:
            store = A.Store()
            store.mark_open("head-window")
            with store.lock:
                leg = store._new_leg_locked(
                    "head-window", "okx", "robinhood", {"t": 1100, "tw": "synthetic"}
                )
            captured = leg["_headSnapshots"]["robinhood"]
            self.assertEqual(captured["height"], 100)
            self.assertEqual(captured["anchorTs"], 1100)
            self.assertEqual(captured["vantage"], "hk")
            c.add("robinhood", {"number": "0x65", "hash": "0x" + "bb" * 32}, "ws-head")
            self.assertEqual(captured["height"], 100)
            self.assertEqual(
                store.timeline("head-window")["legs"][0]["chainHead"]["height"], 100
            )
        finally:
            c.close()
            A.HEADS = original


class TestChainHeadSolForkContinuity(unittest.TestCase):
    """Solana 链头在 processed 承诺下无 hash——slotSubscribe 的 parent 槽位是唯一
    分叉连续性证据：新头必须衔接已接受头（parent == 前一已接受 slot）；同高不同
    parent = 分叉；parent 跳跃 / processed 回滚后在新分叉推进到更高 slot 同样判
    不连续——清空重锚（与 EVM hash/parentHash 冲突清空同律），分叉前的缓冲头
    不得给后续 hook 出值（plugin.md：回退/冲突不可出值）。getSlot 轮询是裸整数
    （无 parent）——同今的降级回退：无连续性证据即不判冲突。"""

    @staticmethod
    def _window(now):
        from collections import deque

        c = A.ChainHeadWindow(clock=lambda: now[0])
        c.run_id = "sol-heads"
        c.samples = {"solana": deque(maxlen=512)}
        return c

    def test_sol_parent_continuity_and_fork_reanchor(self):
        now = [1000]
        c = self._window(now)
        try:
            c.add("solana", {"parent": 99, "slot": 100, "root": 98}, "ws-head")
            now[0] = 1100
            c.add("solana", {"parent": 100, "slot": 101, "root": 100}, "ws-head")
            snap = c.snapshot("sol-heads", 1150)["solana"]
            self.assertEqual(snap["status"], "ok")
            self.assertEqual(
                (snap["height"], snap["parent"]), (101, 100), "parent 槽位随样本保留"
            )
            self.assertIsNone(snap["hash"], "processed 头仍无区块 hash")
            # 同高不同 parent = 分叉——清空重锚；分叉前的头不得出值
            now[0] = 1200
            c.add("solana", {"parent": 99, "slot": 101, "root": 98}, "ws-head")
            self.assertEqual(
                c.snapshot("sol-heads", 1150)["solana"]["status"], "no-pre-anchor-head"
            )
            snap = c.snapshot("sol-heads", 1250)["solana"]
            self.assertEqual((snap["status"], snap["parent"]), ("ok", 99))
            # parent 跳跃（未衔接已接受头）= 不连续
            now[0] = 1300
            c.add("solana", {"parent": 102, "slot": 104, "root": 101}, "ws-head")
            self.assertEqual(
                c.snapshot("sol-heads", 1250)["solana"]["status"], "no-pre-anchor-head"
            )
            self.assertEqual(c.snapshot("sol-heads", 1350)["solana"]["height"], 104)
            # processed 回滚后在新分叉推进到更高 slot——纯高度检查看不见，parent 衔接拒收
            now[0] = 1400
            c.add("solana", {"parent": 102, "slot": 105, "root": 102}, "ws-head")
            self.assertEqual(
                c.snapshot("sol-heads", 1350)["solana"]["status"], "no-pre-anchor-head"
            )
            snap = c.snapshot("sol-heads", 1450)["solana"]
            self.assertEqual(
                (snap["status"], snap["height"], snap["parent"]), ("ok", 105, 102)
            )
            # 高度回退（更低 slot）同样清空（与 EVM 同律）
            now[0] = 1500
            c.add("solana", {"parent": 101, "slot": 102, "root": 101}, "ws-head")
            self.assertEqual(
                c.snapshot("sol-heads", 1450)["solana"]["status"], "no-pre-anchor-head"
            )
            self.assertEqual(c.snapshot("sol-heads", 1550)["solana"]["height"], 102)
        finally:
            c.close()

    def test_sol_poll_without_parent_interleaves_without_false_conflict(self):
        """getSlot 轮询头（裸整数、无 parent）与 ws 头交错：无 parent 证据不判冲突，
        也不破坏后续 ws 头的 parent 衔接判定（降级回退同今）。"""
        now = [1000]
        c = self._window(now)
        try:
            c.add("solana", {"parent": 99, "slot": 100, "root": 98}, "ws-head")
            now[0] = 1050
            c.add("solana", 101, "rpc-poll")  # 轮询无 parent——照常采纳
            now[0] = 1100
            c.add("solana", {"parent": 101, "slot": 102, "root": 101}, "ws-head")
            snap = c.snapshot("sol-heads", 1150)["solana"]
            self.assertEqual(
                (snap["status"], snap["height"]),
                ("ok", 102),
                "轮询头可被后续 ws 头正向衔接",
            )
            # 同高 ws 头与轮询头（poll 样本无 parent）不制造假冲突
            now[0] = 1200
            c.add("solana", {"parent": 101, "slot": 102, "root": 101}, "ws-head")
            self.assertEqual(c.snapshot("sol-heads", 1250)["solana"]["height"], 102)
        finally:
            c.close()


class TestChainHeadControlPlaneWiring(unittest.TestCase):
    """控制面 /mark→采集窗接线（经 IsolatedHTTPServer——绑 OS 分配端口，绝不占
    生产/隧道端口）：/mark 带 legs 明细开窗即按明细启动链头采集；/mark/close 只在
    runId 匹配时关停采集窗；re-mark 新窗顶掉旧窗，旧窗迟到的头与快照一律拒绝。
    采集器传输打桩（rpc 恒空、无 WS 连接）——控制面接线走真实 HTTP 处理器。"""

    def setUp(self):
        fresh()
        self._orig_start_heads = A._start_heads  # import 时的 no-op——测后复原
        self.addCleanup(self._restore)

    def _restore(self):
        A.HEADS.close()
        A.HEADS = A.ChainHeadWindow()
        A._start_heads = self._orig_start_heads

    @staticmethod
    def _isolated_start_heads(run_id, manifest):
        """与生产 _start_heads 同律但传输打桩（无网络）：rpc 恒空、connect=None。"""
        A.HEADS.close()
        A.HEADS = A.ChainHeadWindow(rpc_call=lambda *a, **k: None, connect=None)
        legs = (manifest or {}).get("legs") or []
        chains = sorted({x.get("chain") for x in legs if x.get("chain") in A.CHAINS})
        A.HEADS.start(run_id, chains or list(A.CHAINS))

    def test_mark_starts_window_close_stops_and_remark_replaces(self):
        A._start_heads = self._isolated_start_heads
        manifest = {
            "version": 1,
            "runId": "run-hw1",
            "rounds": 1,
            "legs": [{"platform": "gmgn", "chain": "solana", "rounds": 1}],
        }
        code, body = _http_json(
            "POST",
            "http://127.0.0.1:8072/mark",
            {"runId": "run-hw1", "manifest": manifest},
        )
        self.assertEqual((code, body["ok"]), (200, True))
        self.assertEqual(A.HEADS.run_id, "run-hw1", "/mark 开窗即启动链头采集")
        self.assertEqual(
            sorted(A.HEADS.samples), ["solana"], "链集合按 legs 明细（非全链默认）"
        )
        # 确定性喂头（不经网络）：开窗后的快照出值
        A.HEADS.add("solana", {"parent": 99, "slot": 100, "root": 98}, "ws-head")
        snap = A.HEADS.snapshot("run-hw1", A.HEADS.clock() + 100)["solana"]
        self.assertEqual(snap["status"], "ok")
        # runId 不匹配的 close 不动采集窗
        code, _ = _http_json(
            "POST", "http://127.0.0.1:8072/mark/close", {"runId": "other"}
        )
        self.assertEqual(code, 200)
        self.assertFalse(A.HEADS.stop.is_set(), "runId 不符不关采集窗")
        # matching runId close → 采集窗关停，快照不再出值
        code, _ = _http_json(
            "POST", "http://127.0.0.1:8072/mark/close", {"runId": "run-hw1"}
        )
        self.assertEqual(code, 200)
        self.assertTrue(A.HEADS.stop.is_set())
        self.assertEqual(
            A.HEADS.snapshot("run-hw1", A.HEADS.clock() + 100), {}, "关窗后快照为空"
        )
        # re-mark → 新窗显式顶掉旧窗；旧窗迟到的头与旧 runId 快照一律拒绝
        old = A.HEADS
        code, _ = _http_json(
            "POST",
            "http://127.0.0.1:8072/mark",
            {"runId": "run-hw2", "manifest": manifest},
        )
        self.assertEqual(code, 200)
        self.assertIsNot(A.HEADS, old)
        self.assertEqual(A.HEADS.run_id, "run-hw2")
        self.assertIsNotNone(
            A.STORE.marks["run-hw1"]["closed"], "新窗显式关闭旧窗（单窗纪律）"
        )
        old.add("solana", {"parent": 100, "slot": 101, "root": 100}, "ws-head")
        self.assertEqual(
            len(old.samples["solana"]), 1, "旧窗已关停——迟到头丢弃，既有样本不删"
        )
        self.assertEqual(old.snapshot("run-hw1", old.clock() + 100), {}, "旧窗快照拒绝")
        self.assertEqual(
            A.HEADS.snapshot("run-hw1", A.HEADS.clock() + 100), {}, "新窗不认旧 runId"
        )


class TestKeepAliveJsonRpc(unittest.TestCase):
    """keep-alive 轮询客户端（head-v4）：复用连接、错误后重建、UA 保持（robinhood
    403 规避）、超时生效、非 200 拒绝、仅 https。全程桩 http.client——零网络。"""

    def test_reuses_connection_and_preserves_ua(self):
        class FakeResp:
            status = 200

            def read(self):
                return json.dumps(
                    {"result": {"number": "0x64", "hash": "0x" + "aa" * 32}}
                ).encode()

        class FakeConn:
            def __init__(self):
                self.calls = 0
                self.ua = None

            def request(self, method, path, body=None, headers=None):
                self.calls += 1
                self.ua = headers.get("User-Agent")

            def getresponse(self):
                return FakeResp()

            def close(self):
                pass

        fake = FakeConn()
        ka = A.KeepAliveJsonRpc("https://rpc.example/v1", timeout=0.7)
        ka._connect = lambda: setattr(ka, "_conn", fake)
        r1 = ka("eth_getBlockByNumber", ["latest", False])
        r2 = ka("eth_getBlockByNumber", ["latest", False])
        self.assertEqual(r1["number"], "0x64")
        self.assertEqual(fake.calls, 2, "两次调用复用同一连接")
        self.assertEqual(
            fake.ua,
            "speedex-hk-timing/1.0",
            "UA 与 _jsonrpc 一致（robinhood 403 规避）",
        )

    def test_error_reconnects_and_non200_raises(self):
        class FakeResp:
            def __init__(self, status):
                self.status = status

            def read(self):
                return b"{}"

        states = {"n": 0}

        class FlakyConn:
            def __init__(self, fail_once):
                self.fail_once = fail_once
                self.calls = 0

            def request(self, *a, **k):
                self.calls += 1
                if self.fail_once and self.calls == 1:
                    raise ConnectionResetError("rst")

            def getresponse(self):
                return FakeResp(200) if not self.fail_once or self.calls > 1 else None

            def close(self):
                pass

        ka = A.KeepAliveJsonRpc("https://rpc.example/")
        built = []
        ka._connect = lambda: (
            built.append(FlakyConn(True)) or setattr(ka, "_conn", built[-1])
        )
        ok_resp = json.dumps(
            {"result": {"number": "0x65", "hash": "0x" + "bb" * 32}}
        ).encode()
        # 第一次连接被 reset → 重建第二次成功
        ka2 = A.KeepAliveJsonRpc("https://rpc.example/")
        seq = []

        class C1:
            def request(self, *a, **k):
                seq.append("c1")
                raise ConnectionResetError("rst")

            def close(self):
                pass

        class C2:
            def request(self, *a, **k):
                seq.append("c2")

            def getresponse(self):
                class R:
                    status = 200

                    def read(self):
                        return ok_resp

                return R()

            def close(self):
                pass

        conns = [C1(), C2()]
        ka2._connect = lambda: setattr(ka2, "_conn", conns.pop(0))
        r = ka2("eth_getBlockByNumber", ["latest", False])
        self.assertEqual(r["number"], "0x65")
        self.assertEqual(seq, ["c1", "c2"], "连接错误后重建再试")
        # 非 200 → raise
        ka3 = A.KeepAliveJsonRpc("https://rpc.example/")

        class C3:
            def request(self, *a, **k):
                pass

            def getresponse(self):
                return FakeResp(403)

            def close(self):
                pass

        ka3._connect = lambda: setattr(ka3, "_conn", C3())
        with self.assertRaises(Exception):
            ka3("eth_getBlockByNumber", ["latest", False])
        # https 以外直接拒
        with self.assertRaises(ValueError):
            A.KeepAliveJsonRpc("http://rpc.example/")("m", [])

    def test_timeout_passed_to_connection(self):
        ka = A.KeepAliveJsonRpc("https://rpc.example/", timeout=0.3)
        seen = {}

        class C:
            def request(self, *a, **k):
                seen["timeout"] = self.timeout

            def getresponse(self):
                class R:
                    status = 200

                    def read(self):
                        return json.dumps({"result": 1}).encode()

                return R()

            def close(self):
                pass

        ka._connect = lambda: setattr(ka, "_conn", C())
        ka._conn = None
        ka("m", [], timeout=0.9)
        self.assertEqual(seen["timeout"], 0.9, "调用级 timeout 覆盖默认值")


class TestChainHeadPollDiagAndKeepaliveRouting(unittest.TestCase):
    """_poll 路由（head-v4）：注入 rpc_call 时尊重注入（不建 keep-alive）；
    默认路径用 keep-alive 客户端；成功/失败计数入 diag（/health.diag.headPoll 透出）。"""

    def test_injected_rpc_call_respected_and_diag_counts(self):
        from collections import deque

        now = [1000]
        calls = []
        chain = {"h": 100, "hash": "0x" + "aa" * 32}

        def fake_rpc(url, method, params, timeout=None):
            calls.append((method, timeout))
            if len(calls) % 3 == 0:
                raise TimeoutError("rpc timeout")
            chain["h"] += 1
            parent = chain["hash"]
            chain["hash"] = "0x" + f"{chain['h']:064x}"[-64:]
            return {
                "number": hex(chain["h"]),
                "hash": chain["hash"],
                "parentHash": parent,
            }

        c = A.ChainHeadWindow(rpc_call=fake_rpc, clock=lambda: now[0])
        c.run_id = "poll-diag"
        c.samples = {"robinhood": deque(maxlen=512)}
        try:
            for _ in range(5):
                sent = c.clock()
                try:
                    result = c._poll_call(
                        "robinhood", "eth_getBlockByNumber", ["latest", False]
                    )
                    c.add("robinhood", result, "rpc-poll", sent)
                    with c.lock:
                        c.diag["pollOk"]["robinhood"] += 1
                except Exception as e:
                    with c.lock:
                        c.diag["pollFail"]["robinhood"] += 1
                        c.diag["lastErr"]["robinhood"] = (
                            f"{type(e).__name__}: {str(e)[:80]}"
                        )
                now[0] += 600
            self.assertEqual(
                len(c.samples["robinhood"]), 4, "第 3 次失败被跳过、其余入缓冲"
            )
            self.assertEqual(c.diag["pollOk"]["robinhood"], 4)
            self.assertEqual(c.diag["pollFail"]["robinhood"], 1)
            self.assertIn("TimeoutError", c.diag["lastErr"]["robinhood"])
            self.assertFalse(c._ka, "注入 rpc_call 时不建 keep-alive 客户端")
            snap = c.diag_snapshot()
            self.assertEqual(snap["pollOk"]["robinhood"], 4)
        finally:
            c.close()

    def test_default_path_builds_keepalive(self):
        c = A.ChainHeadWindow(clock=lambda: 1000)
        self.assertIs(c.rpc_call, A._jsonrpc)
        seen = {}

        class FakeKA:
            def __init__(self, url, timeout=None):
                seen["url"], seen["timeout"] = url, timeout

            def __call__(self, method, params):
                return {"number": "0x64", "hash": "0x" + "aa" * 32}

        original = A.KeepAliveJsonRpc
        A.KeepAliveJsonRpc = FakeKA
        try:
            out = c._poll_call("robinhood", "eth_getBlockByNumber", ["latest", False])
            self.assertEqual(out["number"], "0x64")
            self.assertEqual(seen["url"], A.CHAINS["robinhood"]["rpc"])
            self.assertIsNot(c.rpc_call, None)
        finally:
            A.KeepAliveJsonRpc = original
            c.close()


class TestChainHeadFreshnessAtAnchors(unittest.TestCase):
    """iyqzt 回归（2026-09-12 head-v4）：锚点节拍 ~15s 下，健康轮询（rtt 远小于
    maxAgeMs）的链头样本在每个锚点必须 ≤2000ms 新鲜——快照恒 ok，绝不出
    stale-head/round-reference-unverified 的整腿失真。"""

    def test_anchors_get_fresh_heads_under_healthy_poll(self):
        from collections import deque

        now = [1000]
        chain = {"h": 61162715, "hash": "0x" + "ab" * 32}

        def fast_rpc(url, method, params, timeout=None):
            chain["h"] += 1
            parent = chain["hash"]
            chain["hash"] = "0x" + f"{chain['h']:064x}"[-64:]
            return {
                "number": hex(chain["h"]),
                "hash": chain["hash"],
                "parentHash": parent,
            }

        c = A.ChainHeadWindow(rpc_call=fast_rpc, clock=lambda: now[0])
        c.run_id = "fresh-rh"
        c.samples = {"robinhood": deque(maxlen=512)}
        try:
            # 模拟轮询循环节拍：每 ~800ms 一次成功调用；锚点 ~15s 节拍穿插其间
            anchors = [now[0] + 1200 + k * 15000 for k in range(5)]
            next_anchor = 0
            for _ in range(100):
                result = c._poll_call(
                    "robinhood", "eth_getBlockByNumber", ["latest", False]
                )
                c.add("robinhood", result, "rpc-poll", now[0])
                now[0] += 800
                while next_anchor < len(anchors) and now[0] >= anchors[next_anchor]:
                    anchor = anchors[next_anchor]
                    snap = c.snapshot("fresh-rh", anchor)["robinhood"]
                    self.assertEqual(
                        snap["status"], "ok", f"anchor {anchor}: 快照必须新鲜"
                    )
                    self.assertLessEqual(snap["sampleAgeMs"], 2000)
                    self.assertEqual(snap["anchorTs"], anchor)
                    next_anchor += 1
            self.assertEqual(next_anchor, 5, "五个锚点全部核验")
        finally:
            c.close()

    def test_stale_and_missing_semantics_preserved(self):
        from collections import deque

        c = A.ChainHeadWindow(clock=lambda: 1000)
        c.run_id = "stale-keep"
        c.samples = {"robinhood": deque(maxlen=512)}
        try:
            c.add("robinhood", {"number": "0x64", "hash": "0x" + "aa" * 32}, "rpc-poll")
            snap = c.snapshot("stale-keep", 4001)["robinhood"]
            self.assertEqual(
                snap["status"], "stale-head", "超龄样本如实 stale-head（不谎报新鲜）"
            )
            self.assertEqual(
                c.snapshot("stale-keep", 4001)["robinhood"]["sampleAgeMs"], 3001
            )
            c2 = A.ChainHeadWindow(clock=lambda: 1000)
            c2.run_id = "stale-keep"
            c2.samples = {"robinhood": deque(maxlen=512)}
            self.assertEqual(
                c2.snapshot("stale-keep", 1300)["robinhood"]["status"],
                "no-pre-anchor-head",
            )
            c2.close()
        finally:
            c.close()


class TestArcSupport(unittest.TestCase):
    def test_arc_identity_and_frozen_head(self):
        from collections import deque
        for body in [{"chain":"arc"}, {"chainId":5042}, {"chainId":"0x13b2"}]:
            self.assertEqual(A._chain_from_text(json.dumps(body)), "arc")
        self.assertEqual(A._receipt_candidates("arc", "0x"+"aa"*32), ["arc"])
        self.assertIn("arc", A._receipt_candidates(None, "0x"+"aa"*32))
        now = [1000]
        c = A.ChainHeadWindow(clock=lambda: now[0])
        c.run_id = "arc-synthetic"
        c.samples = {"arc": deque(maxlen=512)}
        c.add("arc", {"number":"0x64", "hash":"0x"+"aa"*32}, "ws-head")
        snap = c.snapshot("arc-synthetic", 1100)["arc"]
        self.assertEqual(snap["height"], 100)
        self.assertEqual(snap["anchor"], "proxy-order-request")
        self.assertEqual(snap["vantage"], "hk")
        now[0] = 1200
        c.add("arc", {"number":"0x65", "hash":"0x"+"bb"*32}, "ws-head")
        self.assertEqual(snap["height"],100)
        self.assertEqual(c.snapshot("other",1300), {})
        c.close()


if __name__ == "__main__":
    unittest.main()
