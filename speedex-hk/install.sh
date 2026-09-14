#!/usr/bin/env bash
# speedex HK timing proxy 一键安装（幂等）。用法：
#   scp deploy/hk-proxy/* root@HK:/opt/speedex-mitm-src/ && ssh root@HK bash /opt/speedex-mitm-src/install.sh
# 产出：mitmdump :8443（公网，--proxyauth @htpasswd 强制认证）+ addon 控制面 127.0.0.1:8072。
# CA：~/.mitmproxy（confdir=/opt/speedex-mitm/.mitmproxy）首启生成；CA pem 供 EU 研究 Chrome 导入。
#
# 行为不变量（历年 review 收敛；逐条考古见 git 历史）：
#  - umask 077 前置：一切产出创建即 0600/0700；已有安装逐条重验收紧（幂等）；
#  - 口令永不打印：stdout 只打无凭据地址；凭据只在 /opt/speedex-mitm/proxy.env（0600）；
#    mitmdump 认证走 htpasswd 文件（凭据不进进程 argv）——生成见 gen-htpasswd.sh（{SHA}）；
#  - unit/addon/venv/htpasswd/依赖版本任一变更且服务 active → 受控 restart（幂等）；
#    无变化不重启。sha256 指纹比对 fail-closed（缺失/失败即中止安装）；
#  - 复用旧 venv 前校验其 python 版本与钉选集匹配，不匹配重建；末尾打印实际运行版本摘要；
#  - SPEEDEX_HK_INSTANCE=experiment 安装**隔离实验实例**：独立 APP（/opt/speedex-mitm-exp）、
#    独立 unit（speedex-mitm-exp）、loopback-only 代理 18544 + 控制面 18072（经 SSH 隧道）、
#    独立 CA confdir；硬护栏拒绝生产端口（8443/8072）/生产 APP/生产 unit 名与
#    flow_detail≥2/save_stream_file（全量抓包）；默认**只安装不启动**——启动须
#    SOP（docs/plans/hk-selective-mitm.md）+ 当次授权 SPEEDEX_HK_EXP_START=1。
#    addon 经 SPEEDEX_HK_CTRL_PORT 区分控制面端口（_ctrl_port()，非法回退 8072）。
set -euo pipefail
umask 077

INSTANCE="${SPEEDEX_HK_INSTANCE:-production}"
case "$INSTANCE" in
  production)
    APP_DEFAULT=/opt/speedex-mitm
    UNIT=speedex-mitm
    LISTEN_PORT=8443
    CTRL_PORT=8072
    CA_BOOTSTRAP_PORT=18443
    ;;
  experiment)
    APP_DEFAULT=/opt/speedex-mitm-exp
    UNIT=speedex-mitm-exp
    LISTEN_PORT=18544
    CTRL_PORT=18072
    CA_BOOTSTRAP_PORT=28443
    ;;
  *)
    echo "install.sh: SPEEDEX_HK_INSTANCE 仅支持 production|experiment（收到：$INSTANCE）" >&2
    exit 1
    ;;
esac
APP="${APP:-$APP_DEFAULT}"
SYSTEMD_SYSTEM="${SYSTEMD_SYSTEM:-/etc/systemd/system}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# RHF-19（2026-09-08 review）：实验/生产隔离门必须按**规范化真实路径**比较——裸字符串
# 比较可被尾斜杠（/opt/speedex-mitm/）、`.`/`..` 段、符号链接别名绕过，随后 venv/addon
# 仍写入生产目录。realpath -m 解析已存在组件的 symlink 并压平 . / .. / 重复斜杠；
# 生产根与其**子目录**一律不得作实验 APP（安装根与 unit 实际使用根必须一致）。
APP="$(realpath -m -- "$APP")"
PROD_APP_REAL="$(realpath -m -- /opt/speedex-mitm)"

if [ "$INSTANCE" = "experiment" ]; then
  # 隔离护栏（fail-closed）：实验实例绝不得占用生产端口/路径/unit 名
  [ "$APP" != "$PROD_APP_REAL" ] && [ "${APP#"$PROD_APP_REAL"/}" = "$APP" ] || { echo "拒绝：实验实例不得使用生产 APP 目录或其子目录（规范化后：$APP）" >&2; exit 1; }
  [ "$LISTEN_PORT" != "8443" ] && [ "$CTRL_PORT" != "8072" ] || { echo "拒绝：实验实例不得使用生产端口 8443/8072" >&2; exit 1; }
  [ "$UNIT" != "speedex-mitm" ] || { echo "拒绝：实验实例不得使用生产 unit 名" >&2; exit 1; }
  # unit 模板自证（防模板漂移成生产形状）；不打印文件内容
  [ -f "$SRC/$UNIT.service" ] || { echo "拒绝：缺实验 unit 模板 $UNIT.service" >&2; exit 1; }
  grep -q -- "--listen-host 127.0.0.1" "$SRC/$UNIT.service" || { echo "拒绝：实验 unit 必须 loopback-only" >&2; exit 1; }
  grep -q -- "--listen-port $LISTEN_PORT" "$SRC/$UNIT.service" || { echo "拒绝：实验 unit 端口与 install 常量（$LISTEN_PORT）不符" >&2; exit 1; }
  ! grep -q -- "--listen-port 8443" "$SRC/$UNIT.service" || { echo "拒绝：实验 unit 含生产端口 8443" >&2; exit 1; }
  ! grep -q -- "--listen-host 0.0.0.0" "$SRC/$UNIT.service" || { echo "拒绝：实验 unit 不得公网监听" >&2; exit 1; }
  grep -q "SPEEDEX_HK_CTRL_PORT=$CTRL_PORT" "$SRC/$UNIT.service" || { echo "拒绝：实验 unit 缺 SPEEDEX_HK_CTRL_PORT=$CTRL_PORT" >&2; exit 1; }
  # 诊断最小化护栏：实验 unit 不得开启全量抓包/落流（overlay 契约的可执行部分）；
  # 只查有效行——注释里对禁令的说明不触发
  ! grep -v '^[[:space:]]*#' "$SRC/$UNIT.service" | grep -Eq 'flow_detail=[23]|save_stream_file' || { echo "拒绝：实验 unit 含全量抓包选项（flow_detail≥2/save_stream_file）" >&2; exit 1; }
fi

apt-get update -qq
apt-get install -y -qq python3-venv python3-pip
# 目录创建即 0700；已有安装重验收紧
install -d -m 700 "$APP"
chmod 700 "$APP"
# review R29（2026-09-05）：依赖全部锁定——同源码不同 runtime 的部署漂移已实测发生
# （两节点曾 12.2.3/11.0.2 并存）。升降级走改这里+重装，不靠「可执行文件存在」。
# mitmproxy 12.x 需要 Python≥3.12；py3.11 节点锁 11.0.2（对齐路径 = OS/Python 升级）。
PYVER="$(python3 -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if [ "$(printf '%s\n3.12\n' "$PYVER" | sort -V | head -1)" = "3.12" ]; then
  MITM_PROXY_VER=12.2.3
  MSGPACK_VER=1.1.2
else
  MITM_PROXY_VER=11.0.2
  MSGPACK_VER=1.1.0  # mitmproxy 11.0.2 钉 msgpack<=1.1.0
fi
WEBSOCKETS_VER=17.1

RESTART_NEEDED=0
# R29（二轮残留）：venv 复用前校验其 python 版本与当前选定版本集匹配——OS python
# 跨 minor 升级后旧 venv 仍跑旧 minor，而上方钉选集按 PYVER 分岔；不匹配即重建。
if [ -x "$APP/venv/bin/python" ]; then
  VENV_PYVER="$("$APP/venv/bin/python" -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  if [ "$VENV_PYVER" != "$PYVER" ]; then
    echo "== venv python $VENV_PYVER ≠ 系统 $PYVER（钉选集随 minor 分岔）——重建 venv =="
    rm -rf "$APP/venv"
    RESTART_NEEDED=1
  fi
fi
[ -x "$APP/venv/bin/mitmdump" ] || {
  python3 -m venv "$APP/venv"
  "$APP/venv/bin/pip" install -q --upgrade pip
}
# 幂等收敛：版本不符即重装到锁定集（含既有节点的升级/降级）
INSTALLED_MITM="$([ -x "$APP/venv/bin/pip" ] && "$APP/venv/bin/pip" show mitmproxy 2>/dev/null | awk '/^Version:/{print $2}' || true)"
if [ "$INSTALLED_MITM" != "$MITM_PROXY_VER" ]; then
  "$APP/venv/bin/pip" install -q "mitmproxy==$MITM_PROXY_VER"
  RESTART_NEEDED=1
fi
# addon 探测依赖（/latency 的 WS 探针 / msgpack 解码）。htpasswd 已改 {SHA}（stdlib
# hashlib，无 bcrypt 依赖）——bcrypt 每 CONNECT 全量验算曾在单核 VM 压满事件循环（1dj3c 事故）
# 2026-09-09（历史线索③残余）：websockets/msgpack 独立升降级（mitmproxy 本体不变）也
# 置 RESTART_NEEDED——运行中 mitmdump 已 import 旧版模块，只换 site-packages 文件
# 不重启不生效。比对法与上方 mitmproxy 版本门同构（pip show 已装版本 vs 钉选）。
INSTALLED_WS="$([ -x "$APP/venv/bin/pip" ] && "$APP/venv/bin/pip" show websockets 2>/dev/null | awk '/^Version:/{print $2}' || true)"
INSTALLED_MSGPACK="$([ -x "$APP/venv/bin/pip" ] && "$APP/venv/bin/pip" show msgpack 2>/dev/null | awk '/^Version:/{print $2}' || true)"
if [ "$INSTALLED_WS" != "$WEBSOCKETS_VER" ] || [ "$INSTALLED_MSGPACK" != "$MSGPACK_VER" ]; then
  RESTART_NEEDED=1
fi
"$APP/venv/bin/pip" install -q "websockets==$WEBSOCKETS_VER" "msgpack==$MSGPACK_VER"

# 认证凭据（只生成一次；已存在则保留——重装不改密码，EU 侧配置不失效）。
# umask 077 → 创建即 0600；stdout 只打无凭据地址（口令见 proxy.env，只此一份）。
if [ ! -f "$APP/proxy.env" ]; then
  PASS="$(head -c 18 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 20)"
  printf 'PROXY_AUTH=speedex:%s\n' "$PASS" > "$APP/proxy.env"
  echo "== 已生成代理凭据（不打印——凭据见 $APP/proxy.env，0600）=="
  if [ "$INSTANCE" = "experiment" ]; then
    echo "   proxy 地址: 127.0.0.1:$LISTEN_PORT（loopback-only——经 SSH 隧道供隔离研究 Chrome）"
  else
    echo "   proxy 地址: $(curl -s -4 ifconfig.me || echo HK_IP):$LISTEN_PORT"
  fi
fi
chmod 600 "$APP/proxy.env"  # 已有安装重验修复（先写后 chmod 的遗留窗口）

# mitmdump 认证的 htpasswd（--proxyauth @file；凭据不上 argv/environ——python 直读
# 0600 proxy.env）。每次安装从 proxy.env 重新生成（幂等且与手工轮换后保持同步）。
# D（R04/R29 三轮残留）：运行中 mitmproxy 的 proxyauth verifier 只在初始化时把
# htpasswd 加载进内存——凭据轮换后文件内容变了但不重启，旧进程继续收旧凭据。
# 生成前后比 sha256 指纹（不复制/不打印凭据材料）：变化（含服务在跑而文件缺失的
# 漂移）→ RESTART_NEEDED，走下方统一受控 restart；无变化保持幂等不重启。
# NR-01（2026-09-09 review）fail-closed：sha256sum 缺失/非零退出/空输出 →
# htpasswd_sha256 显式非零返回、调用处立即中止安装——旧实现 AFTER 摘要在 [ ]
# 条件内做命令替换，失败不触发 set -e；「htpasswd 缺失漂移（BEFORE 留空）+
# sha256sum 不可用/空输出（AFTER 为空）」会把两个空摘要判相等、漏掉受控重启。
htpasswd_sha256() {
  local digest
  digest="$(sha256sum "$1" | cut -d' ' -f1)" || return 1
  [ -n "$digest" ] || return 1
  printf '%s\n' "$digest"
}

HTPASSWD_BEFORE=""
if [ -f "$APP/htpasswd" ]; then
  HTPASSWD_BEFORE="$(htpasswd_sha256 "$APP/htpasswd")" || {
    echo "install.sh: 既有 htpasswd 无法取 sha256 指纹（sha256sum 缺失/失败/空输出）——中止安装（无法比对，禁止空空相等）" >&2
    exit 1
  }
fi
APP="$APP" bash "$SRC/gen-htpasswd.sh"
HTPASSWD_AFTER="$(htpasswd_sha256 "$APP/htpasswd")" || {
  echo "install.sh: 新 htpasswd 无法取 sha256 指纹（sha256sum 缺失/失败/空输出）——中止安装（禁止空空相等）" >&2
  exit 1
}
if [ "$HTPASSWD_AFTER" != "$HTPASSWD_BEFORE" ]; then
  if [ -n "$HTPASSWD_BEFORE" ]; then
    echo "== 代理凭据内容变更（htpasswd 已重生成，不打印凭据）——纳入受控重启 =="
  fi
  RESTART_NEEDED=1
fi

# R04（二轮残留）：unit/addon 变更检测——install(1) 幂等覆盖但不报告变化，先 cmp 留证；
# active 旧部署升级时必须 restart，否则新 ExecStart/新 addon 不生效。
# 实验实例复用同一 addon 源文件（addon 代码归平行工作流，本脚本不改它），unit 用 $UNIT。
cmp -s "$SRC/speedex_hk_timing.py" "$APP/speedex_hk_timing.py" 2>/dev/null || RESTART_NEEDED=1
cmp -s "$SRC/$UNIT.service" "$SYSTEMD_SYSTEM/$UNIT.service" 2>/dev/null || RESTART_NEEDED=1
install -m 600 "$SRC/speedex_hk_timing.py" "$APP/speedex_hk_timing.py"
install -m 644 "$SRC/$UNIT.service" "$SYSTEMD_SYSTEM/$UNIT.service"

# Existing confdirs may predate umask hardening. No private key may remain
# traversable by other users; permission errors stop installation.
mkdir -p "$APP/.mitmproxy"
chmod 700 "$APP/.mitmproxy"
find "$APP/.mitmproxy" -type f -exec chmod 600 {} +
# 首启生成 CA（几秒钟后退出的方式触发 confdir 初始化；实验实例独立 confdir/引导端口）
if [ ! -f "$APP/.mitmproxy/mitmproxy-ca.pem" ]; then
  set +e
  timeout 8 "$APP/venv/bin/mitmdump" --set confdir="$APP/.mitmproxy" --listen-host 127.0.0.1 --listen-port "$CA_BOOTSTRAP_PORT" --proxyauth "x:x" -q >/dev/null 2>&1
  set -e
  [ -f "$APP/.mitmproxy/mitmproxy-ca.pem" ] || { echo "CA 生成失败"; exit 1; }
fi

systemctl daemon-reload
if [ "$INSTANCE" = "experiment" ] && [ "${SPEEDEX_HK_EXP_START:-0}" != "1" ]; then
  # 默认 OFF：实验实例只安装不启动（纯策略门）。addon 自 2026.09.08-r10 起消费
  # SPEEDEX_HK_CTRL_PORT（_ctrl_port()）——实验控制面 18072 与生产 8072 并存
  # 不再 bind 冲突。启动前提只剩 SOP（docs/plans/hk-selective-mitm.md）+ 当次
  # 实验授权；满足后以 SPEEDEX_HK_EXP_START=1 重跑本脚本。
  echo "== 实验实例已安装但未启动（默认 OFF）=="
  echo "   启动需经 SOP 授权（docs/plans/hk-selective-mitm.md）；"
  echo "   届时：SPEEDEX_HK_INSTANCE=experiment SPEEDEX_HK_EXP_START=1 bash install.sh"
elif [ "$RESTART_NEEDED" = 1 ] && systemctl is-active --quiet "$UNIT"; then
  # 幂等：unit/addon/mitmproxy/websockets/msgpack/venv/htpasswd 凭据变更过且服务
  # 在跑 → restart 让新 ExecStart/代码/内存凭据生效（新 PID；下方 is-active +
  # 版本摘要即健康核验）
  systemctl restart "$UNIT"
else
  systemctl enable --now "$UNIT"
fi
if [ "$INSTANCE" = "production" ] || [ "${SPEEDEX_HK_EXP_START:-0}" = "1" ]; then
  sleep 2
  systemctl is-active "$UNIT"
fi
# R29（二轮残留）：部署核验——打印实际安装版本摘要（无凭据），与钉选集对账
echo "== 版本摘要（部署核验）=="
"$APP/venv/bin/python" - <<'PYEOF'
import sys
import importlib.metadata as md
print(f"python {sys.version.split()[0]}")
for pkg in ("mitmproxy", "websockets", "msgpack"):
    try:
        print(f"{pkg} {md.version(pkg)}")
    except md.PackageNotFoundError:
        print(f"{pkg} MISSING")
PYEOF
echo "== OK ==  CA: $APP/.mitmproxy/mitmproxy-ca.pem  控制面: 127.0.0.1:$CTRL_PORT"
