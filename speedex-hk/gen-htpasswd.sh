#!/usr/bin/env bash
# 从 $APP/proxy.env（PROXY_AUTH=user:pass）生成 mitmdump --proxyauth @file 用的 htpasswd。
# review R04（2026-09-05；二轮残留修订 + 1dj3c 性能事故修订）：
#  - 口令绝不打印、绝不上 argv、**也不经子进程 environ**（AGENTS 2026-09-05：代理凭据
#    属 secret，stdout/logs/argv/process-environ 邻接面一律不得出现）——python 直接
#    读 0600 proxy.env（root-only 文件，口令不离开既有的文件边界）；argv 只传路径；
#  - **{SHA}（base64 sha1）而非 bcrypt**：bcrypt 每次 CONNECT 都全量验算——单核 HK VM
#    上 Chrome 一次页面装载=几十条 CONNECT，主事件循环被 bcrypt 压满、accept 队列积压
#    → 全页 ERR_TIMED_OUT（1dj3c 批实锤）。{SHA} 验证 O(μs)。离线爆破抗性牺牲是
#    可接受的：htpasswd 与明文 proxy.env 同 0600 root-only 边界，凭据本就是一次性
#    研究凭据。mitmproxy 11（passlib）/12（自研 HtpasswdFile）/13+ 都支持 {SHA}——
#    apr1 MD5 在 13+ 会被拒（"Unsupported htpasswd format"），不要改用 openssl apr1；
#  - umask 077：产出创建即 0600。
set -euo pipefail
umask 077

APP="${APP:-/opt/speedex-mitm}"
PY="${HTPASSWD_PYTHON:-$APP/venv/bin/python}"
OUT="${HTPASSWD_OUT:-$APP/htpasswd}"

[ -f "$APP/proxy.env" ] || { echo "gen-htpasswd: 缺 $APP/proxy.env" >&2; exit 1; }
# argv 只传路径（非 secret）；口令由 python 在 0600 文件边界内读取，不经 environ
"$PY" - "$APP/proxy.env" "$OUT" <<'PYEOF'
import base64
import hashlib
import sys

env_path, out_path = sys.argv[1], sys.argv[2]
auth = None
with open(env_path, "r") as f:  # 0600 root-only——与口令同一文件边界
    for line in f:
        if line.startswith("PROXY_AUTH="):
            auth = line[len("PROXY_AUTH="):].strip()
            break
if not auth:
    sys.exit("gen-htpasswd: proxy.env 缺 PROXY_AUTH")
user, sep, pw = auth.partition(":")
if not sep or not user or not pw:
    sys.exit("gen-htpasswd: PROXY_AUTH 需为 user:pass")
# {SHA} = 'sha1' 前缀 + base64(sha1(pass))——mitmproxy 11/12/13 全支持，验证 O(μs)
h = "{SHA}" + base64.b64encode(hashlib.sha1(pw.encode()).digest()).decode()
with open(out_path, "w") as f:  # umask 077 → 创建即 0600
    f.write(f"{user}:{h}\n")
PYEOF
chmod 600 "$OUT"  # 已有文件重验收紧（幂等重装）
