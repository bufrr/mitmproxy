// HK install.sh 凭据轮换受控重启（review R04/R29 三轮残留 D，2026-09-06）行为测试：
// 运行中 mitmproxy 的 proxyauth verifier 只在初始化时把 htpasswd 加载进内存——
// 只轮换 proxy.env/htpasswd 内容而不重启，旧进程继续收旧凭据。install.sh 现把
// htpasswd 内容变更（sha256 指纹比对，不复制/不打印凭据材料）纳入 RESTART_NEEDED。
//
// 全离线：临时 APP/SYSTEMD_SYSTEM 目录夹具 + PATH stub（systemctl/apt-get/curl/
// sleep/python3/venv pip + 无条件安装的 realpath -m/sha256sum/sort -V shim）。
// NR-01（2026-09-09 review）：旧实现先探测宿主能力再决定装不装 shim，但探测走
// 测试进程 PATH（macOS 上可见 Homebrew 工具）、runInstall 用受限 PATH（fixture
// bin+/usr/bin+/bin）——探测/执行环境不一致，sha256sum 不在执行 PATH → 6 项失败。
// 现 shim 无条件进 fixture bin，探测与执行同一 PATH：shim 内部优先转发候选目录
// （缺省 /usr/bin:/bin，绝对路径不经 PATH 查找、不递归回自身）里的真实 GNU 工具
// ——Linux 上真实 GNU 语义仍被真实执行；找不到/非 GNU 时回退 REALPY 同语义子集，
// 行为用例跨平台一致可跑。unit/addon 与 SRC 逐字节相同、pip stub 缺省报钉选版本
// （排除 unit/addon/依赖门干扰，restart 只能归因于凭据门）。生产 gen-htpasswd.sh
// 经 install.sh 真实调用；{SHA} 校验往返用 node:crypto（sha1+base64，与
// mitmproxy 11/12/13 同算法）。
// 绝不碰真实 systemctl/apt/网络/节点。断言：
//  - 凭据轮换+active → restart（且 daemon-reload 先于 restart），enable --now 不调用；
//  - 无变化+active → 不 restart（连跑两次幂等），enable --now；
//  - htpasswd 缺失+active（漂移）→ restart（无法证明运行中 verifier 状态，fail-closed）；
//  - websockets/msgpack 已装版本 ≠ 钉选（FIXTURE_WS_VER/FIXTURE_MSGPACK_VER 注入旧版）
//    +active → restart（2026-09-09 历史线索③残余：依赖独立升降级——运行中 mitmdump
//    已 import 旧版模块，只换 site-packages 文件不重启不生效）；不符+inactive → 不
//    restart（is-active 门）；
//  - 凭据轮换+inactive → 不 restart（is-active 门），enable --now 起新进程自载新凭据；
//  - NR-01：sha256sum 缺失（PATH 影子目录模拟 macOS 复现场景）/stub 非零退出/空输出
//    → 安装 fail-closed 非零退出，不 restart、不 enable --now、stdout/stderr 无凭据
//    （禁止两个空摘要被判相等 = 无变化）；
//  - NR-01：shim 无条件安装；GNU_TOOL_DIRS 置空强制 python 回退时，sha256sum/sort -V/
//    realpath -m 输出与 GNU 语义期望一致；
//  - 各路径 stdout 绝不含新旧口令/{SHA} 材料；产出 0600/0700。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { readFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

// 2026-09-14 起本测试随代码迁到 fork（speedex-hk/tests/）。HK_CTRL_PORT=8072 是
// 跨仓合同：addon `_ctrl_port()` 缺省 + units + speedex 主仓 scripts/lib/
// hk-timing-client.mjs 的 HK_CTRL_PORT——改 8072 必须三处同步。
const HK_CTRL_PORT = 8072;
const HK_DIR = path.join(path.dirname(new URL(import.meta.url).pathname), '..');
const INSTALL = path.join(HK_DIR, 'install.sh');
const GEN = path.join(HK_DIR, 'gen-htpasswd.sh');
const OLD_PASS = 'TestPass-R04D-old-7q';
const NEW_PASS = 'TestPass-R04D-new-3z';
// 全合成 CA（非真实证书材料）；203.0.113.7 = TEST-NET-3 文档保留段
const SYNTH_CA =
  '-----BEGIN CERTIFICATE-----\nU1lOVEhFVElDRkFLRUNBMDAwMDAwMDAwMDAwMDAwMA==\n-----END CERTIFICATE-----\n';

const REALPY = execFileSync('which', ['python3'], { encoding: 'utf8' }).trim();
// python3 硬门（hashlib 经 gen-htpasswd 实跑——缺失即环境坏，不许静默 skip）
execFileSync(REALPY, ['-c', 'import hashlib, base64'], { stdio: 'pipe' });

// GNU 工具 shim（无条件写入夹具 bin——NR-01 2026-09-09：旧实现按宿主探测结果
// 条件安装，探测走测试进程 PATH、执行走受限 PATH，两者不一致即假探测；现探测
// 与执行同一 PATH，行为用例跨平台一致）。shim 内部优先在候选目录
// （GNU_TOOL_DIRS，缺省 /usr/bin:/bin——只认绝对路径，不经 PATH 查找，避免递归
// 回自身）找真实 GNU 工具并 exec 转发（Linux 上真实 GNU 语义仍被真实执行，shim
// 不稀释 Linux 覆盖）；找不到/非 GNU（BSD sort/realpath 无 -V/-m，探针落选）时
// 回退 $REALPY 同语义子集（一律转发 REALPY——夹具 python3 stub 会截获 -c，不能
// 走它）。语义子集按 install.sh 实际调用形钉：
// realpath -m --（解析已存在组件 symlink + 压平 ./..//、缺失尾部不报错 =
// os.path.realpath strict=False）、sha256sum <file>（输出 '<hex>  <path>'，
// install.sh 经 cut -d' ' -f1 取指纹）、sort -V（点分数字段元组比较——
// install.sh 只比 3.x/3.12 形态，stdin 过滤空行原样回吐）。
const GNU_SHIMS = {
  realpath: `#!/bin/sh
OLD_IFS=$IFS; IFS=:
for d in \${GNU_TOOL_DIRS:-/usr/bin:/bin}; do
  if [ -x "$d/realpath" ] && [ "$("$d/realpath" -m -- / 2>/dev/null)" = "/" ]; then
    IFS=$OLD_IFS
    exec "$d/realpath" "$@"
  fi
done
IFS=$OLD_IFS
exec "$REALPY" -c '
import os, sys
args = sys.argv[1:]
if args[:1] == ["-m"]:
    args = args[1:]
if args[:1] == ["--"]:
    args = args[1:]
for p in args:
    print(os.path.realpath(p))
' "$@"
`,
  sha256sum: `#!/bin/sh
OLD_IFS=$IFS; IFS=:
for d in \${GNU_TOOL_DIRS:-/usr/bin:/bin}; do
  if [ -x "$d/sha256sum" ]; then
    IFS=$OLD_IFS
    exec "$d/sha256sum" "$@"
  fi
done
IFS=$OLD_IFS
exec "$REALPY" -c '
import hashlib, sys
for f in sys.argv[1:]:
    with open(f, "rb") as fh:
        print(hashlib.sha256(fh.read()).hexdigest() + "  " + f)
' "$@"
`,
  sort: `#!/bin/sh
if [ "\${1:-}" = "-V" ]; then
  OLD_IFS=$IFS; IFS=:
  for d in \${GNU_TOOL_DIRS:-/usr/bin:/bin}; do
    if [ -x "$d/sort" ] && "$d/sort" -V </dev/null >/dev/null 2>&1; then
      IFS=$OLD_IFS
      shift
      exec "$d/sort" -V "$@"
    fi
  done
  IFS=$OLD_IFS
  shift
  exec "$REALPY" -c '
import sys
def key(s):
    return tuple(int(x) if x.isdigit() else 0 for x in s.strip().split("."))
for line in sorted((l for l in sys.stdin if l.strip()), key=key):
    sys.stdout.write(line)
' "$@"
fi
exec /usr/bin/sort "$@"
`,
};

const shaEntry = (user, pw) =>
  `${user}:{SHA}` + createHash('sha1').update(pw).digest('base64');

function writeStub(file, body) {
  fs.writeFileSync(file, body, { mode: 0o755 });
}

// systemctl stub：记录 argv 到 SYSTEMCTL_LOG；is-active 由 FIXTURE_ACTIVE/FIXTURE_STATE
// 决定（enable/restart/start 后置 state——模拟「启动后变 active」），绝不触真实 systemd。
const STUBS = {
  systemctl: `#!/bin/sh
printf 'systemctl %s\\n' "$*" >> "$SYSTEMCTL_LOG"
case "$1" in
  enable|restart|start) : > "$FIXTURE_STATE"; exit 0 ;;
  is-active)
    if [ "$FIXTURE_ACTIVE" = "1" ] || [ -f "$FIXTURE_STATE" ]; then
      [ "$2" = "--quiet" ] || echo active
      exit 0
    fi
    [ "$2" = "--quiet" ] || echo inactive
    exit 3 ;;
esac
exit 0
`,
  // python3 shim：-c 版本探测钉 3.12（锁定集 12.2.3 分支），其余转真实解释器
  python3: `#!/bin/sh
if [ "$1" = "-c" ]; then echo "3.12"; exit 0; fi
exec "$REALPY" "$@"
`,
  'apt-get': `#!/bin/sh
exit 0
`,
  curl: `#!/bin/sh
echo 203.0.113.7
`,
  sleep: `#!/bin/sh
exit 0
`,
};
const VENV_STUBS = {
  python: STUBS.python3,
  // pip stub：show 报夹具钉选版本（FIXTURE_*_VER 可覆盖——依赖漂移用例注入旧版；
  // 缺省 = python3 stub 钉的 3.12 分支钉选集 12.2.3/17.1/1.1.2）；install 幂等空转
  pip: `#!/bin/sh
if [ "$1" = "show" ]; then
  case "$2" in
    mitmproxy) printf 'Name: mitmproxy\\nVersion: %s\\n' "\${FIXTURE_MITM_VER:-12.2.3}" ;;
    websockets) printf 'Name: websockets\\nVersion: %s\\n' "\${FIXTURE_WS_VER:-17.1}" ;;
    msgpack) printf 'Name: msgpack\\nVersion: %s\\n' "\${FIXTURE_MSGPACK_VER:-1.1.2}" ;;
  esac
fi
exit 0
`,
  mitmdump: `#!/bin/sh
exit 0
`,
};

function buildFixture({ active, rotate = false, htpasswdMissing = false }) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'speedex-hk-restart-'));
  const app = path.join(root, 'app');
  const systemdDir = path.join(root, 'systemd-system');
  const bin = path.join(root, 'bin');
  fs.mkdirSync(path.join(app, 'venv', 'bin'), { recursive: true });
  fs.mkdirSync(path.join(app, '.mitmproxy'), { recursive: true });
  fs.mkdirSync(systemdDir, { recursive: true });
  fs.mkdirSync(bin, { recursive: true });
  for (const [name, body] of Object.entries(STUBS)) writeStub(path.join(bin, name), body);
  // GNU shim 无条件进 fixture bin（NR-01：不再按宿主探测条件安装——探测/执行
  // 同一 PATH）。shim 优先转发 /usr/bin:/bin 真实 GNU 工具，缺失/非 GNU 回退
  // REALPY 子集；Linux 上真实 GNU 语义仍被真实执行
  for (const [name, body] of Object.entries(GNU_SHIMS)) writeStub(path.join(bin, name), body);
  for (const [name, body] of Object.entries(VENV_STUBS)) {
    writeStub(path.join(app, 'venv', 'bin', name), body);
  }
  // 既有部署：proxy.env 持旧口令 → 生产 gen-htpasswd.sh 生成旧 htpasswd
  fs.writeFileSync(path.join(app, 'proxy.env'), `PROXY_AUTH=speedex:${OLD_PASS}\n`, { mode: 0o600 });
  execFileSync('bash', [GEN], {
    env: { PATH: '/usr/bin:/bin', APP: app, HTPASSWD_PYTHON: REALPY },
    stdio: 'pipe',
  });
  if (htpasswdMissing) fs.rmSync(path.join(app, 'htpasswd'));
  // 轮换：proxy.env 换新口令（install.sh 重生成 htpasswd → 内容应变化）
  if (rotate) {
    fs.writeFileSync(path.join(app, 'proxy.env'), `PROXY_AUTH=speedex:${NEW_PASS}\n`, { mode: 0o600 });
  }
  // unit/addon 与 SRC 逐字节相同、pip stub 缺省报钉选版本——unit/addon/依赖
  // 变更门保持关闭，restart 只能归因于凭据门
  fs.copyFileSync(path.join(HK_DIR, 'speedex_hk_timing.py'), path.join(app, 'speedex_hk_timing.py'));
  fs.copyFileSync(path.join(HK_DIR, 'speedex-mitm.service'), path.join(systemdDir, 'speedex-mitm.service'));
  // CA 已存在 → 首启 CA 生成分支整体跳过（mitmdump stub 不会被执行）
  fs.writeFileSync(path.join(app, '.mitmproxy', 'mitmproxy-ca.pem'), SYNTH_CA, { mode: 0o600 });
  return {
    root,
    app,
    systemdDir,
    bin,
    log: path.join(root, 'systemctl.log'),
    state: path.join(root, 'systemd.state'),
    active,
  };
}

function installEnv(fx, extraEnv = {}) {
  return {
    PATH: `${fx.bin}:/usr/bin:/bin`,
    APP: fx.app,
    SYSTEMD_SYSTEM: fx.systemdDir,
    HTPASSWD_PYTHON: REALPY,
    SYSTEMCTL_LOG: fx.log,
    FIXTURE_ACTIVE: fx.active ? '1' : '0',
    FIXTURE_STATE: fx.state,
    REALPY,
    HOME: fx.root,
    ...extraEnv,
  };
}

function runInstall(fx, extraEnv = {}) {
  fs.rmSync(fx.log, { force: true });
  fs.rmSync(fx.state, { force: true });
  return execFileSync('bash', [INSTALL], {
    encoding: 'utf8',
    env: installEnv(fx, extraEnv),
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout: 60000,
  });
}

// 非零退出台（NR-01 fail-closed 用例）：不抛异常，回传 status/stdout/stderr
function runInstallRaw(fx, extraEnv = {}) {
  fs.rmSync(fx.log, { force: true });
  fs.rmSync(fx.state, { force: true });
  try {
    const stdout = execFileSync('bash', [INSTALL], {
      encoding: 'utf8',
      env: installEnv(fx, extraEnv),
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: 60000,
    });
    return { status: 0, stdout, stderr: '' };
  } catch (e) {
    return { status: e.status ?? -1, stdout: String(e.stdout || ''), stderr: String(e.stderr || '') };
  }
}

function assertStdoutHygiene(stdout) {
  for (const secret of [OLD_PASS, NEW_PASS, shaEntry('speedex', OLD_PASS), shaEntry('speedex', NEW_PASS)]) {
    assert.ok(!stdout.includes(secret), 'stdout 绝不含口令/{SHA} 凭据材料');
  }
  // 控制面端口行与 hk-timing-client HK_CTRL_PORT 漂移门（行为输出，非源码刮取）
  assert.ok(stdout.includes(`控制面: 127.0.0.1:${HK_CTRL_PORT}`), '末尾必须打控制面地址（无凭据）');
  assert.ok(stdout.includes('版本摘要'), '末尾必须打实际运行版本摘要（升级健康核验）');
}

async function verifyHtpasswd(file, goodPass, badPass) {
  const line = (await readFile(file, 'utf8')).trim();
  assert.equal(line, shaEntry('speedex', goodPass), 'htpasswd 必须匹配当前口令的 {SHA}');
  assert.notEqual(line, shaEntry('speedex', badPass), 'htpasswd 绝不得仍匹配旧口令');
  assert.equal(fs.statSync(file).mode & 0o777, 0o600, 'htpasswd 必须 0600');
}

test('R04/R29-D：凭据轮换+服务 active → 受控 restart，新凭据生效，stdout 无凭据', async (t) => {
  const fx = buildFixture({ active: true, rotate: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  const out = runInstall(fx);
  const log = await readFile(fx.log, 'utf8');
  assert.ok(log.includes('systemctl daemon-reload'), '必须先 daemon-reload');
  assert.ok(log.includes('systemctl restart speedex-mitm'), 'htpasswd 内容变更+active → 必须 restart');
  assert.ok(
    log.indexOf('daemon-reload') < log.indexOf('restart speedex-mitm'),
    'daemon-reload 必须先于 restart'
  );
  assert.ok(!log.includes('enable --now'), 'restart 分支不得再走 enable --now');
  await verifyHtpasswd(path.join(fx.app, 'htpasswd'), NEW_PASS, OLD_PASS);
  assert.equal(fs.statSync(path.join(fx.app, 'proxy.env')).mode & 0o777, 0o600, 'proxy.env 必须 0600');
  assert.equal(fs.statSync(fx.app).mode & 0o777, 0o700, 'APP 目录必须 0700');
  assertStdoutHygiene(out);
});

test('R04/R29-D：无变化+active → 不 restart（连跑两次幂等），enable --now', async (t) => {
  const fx = buildFixture({ active: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  for (let i = 0; i < 2; i += 1) {
    const out = runInstall(fx);
    const log = await readFile(fx.log, 'utf8');
    assert.ok(!log.includes('restart speedex-mitm'), `第 ${i + 1} 次：无变化不得 restart`);
    assert.ok(log.includes('enable --now speedex-mitm'), `第 ${i + 1} 次：幂等走 enable --now`);
    assertStdoutHygiene(out);
  }
  await verifyHtpasswd(path.join(fx.app, 'htpasswd'), OLD_PASS, NEW_PASS);
});

test('R04/R29-D：htpasswd 缺失+active（漂移）→ restart（无法证明运行中 verifier 状态）', async (t) => {
  const fx = buildFixture({ active: true, htpasswdMissing: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  const out = runInstall(fx);
  const log = await readFile(fx.log, 'utf8');
  assert.ok(log.includes('systemctl restart speedex-mitm'), '服务在跑而 htpasswd 缺失 → fail-closed restart');
  await verifyHtpasswd(path.join(fx.app, 'htpasswd'), OLD_PASS, NEW_PASS);
  assertStdoutHygiene(out);
});

test('R04/R29-D：凭据轮换+服务 inactive → 不 restart，enable --now 起新进程自载新凭据', async (t) => {
  const fx = buildFixture({ active: false, rotate: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  const out = runInstall(fx);
  const log = await readFile(fx.log, 'utf8');
  assert.ok(!log.includes('restart speedex-mitm'), '服务未在跑 → 不得 restart（is-active 门）');
  assert.ok(log.includes('enable --now speedex-mitm'), '未在跑 → enable --now 启动（新进程自载新 htpasswd）');
  // runInstall 零退出的前提是末尾 is-active 通过——即 enable --now 后服务确已 active
  await verifyHtpasswd(path.join(fx.app, 'htpasswd'), NEW_PASS, OLD_PASS);
  assertStdoutHygiene(out);
});

test('R04/R29-D：install.sh / gen-htpasswd.sh bash -n 语法门', () => {
  execFileSync('bash', ['-n', INSTALL]);
  execFileSync('bash', ['-n', GEN]);
});

test('依赖钉选漂移（历史线索③残余）：websockets 独立不符+active → restart', async (t) => {
  // mitmproxy 本体不变、websockets 已装 17.0 ≠ 钉选 17.1——运行中 mitmdump 已 import
  // 旧版模块，只换 site-packages 文件不重启不生效 → 必须纳入受控 restart
  const fx = buildFixture({ active: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  const out = runInstall(fx, { FIXTURE_WS_VER: '17.0' });
  const log = await readFile(fx.log, 'utf8');
  assert.ok(log.includes('systemctl daemon-reload'), '必须先 daemon-reload');
  assert.ok(log.includes('systemctl restart speedex-mitm'), 'websockets 版本不符+active → 必须 restart');
  assert.ok(!log.includes('enable --now'), 'restart 分支不得再走 enable --now');
  assertStdoutHygiene(out);
});

test('依赖钉选漂移（历史线索③残余）：msgpack 不符+active → restart；不符+inactive → 不 restart', async (t) => {
  const fxActive = buildFixture({ active: true });
  t.after(() => fs.rmSync(fxActive.root, { recursive: true, force: true }));
  const outActive = runInstall(fxActive, { FIXTURE_MSGPACK_VER: '1.1.1' });
  const logActive = await readFile(fxActive.log, 'utf8');
  assert.ok(logActive.includes('systemctl restart speedex-mitm'), 'msgpack 已装 1.1.1 ≠ 钉选 1.1.2 + active → 必须 restart');
  assertStdoutHygiene(outActive);
  // is-active 门与凭据门同律：服务未在跑 → 不 restart，enable --now 起新进程自载新依赖
  const fxIdle = buildFixture({ active: false });
  t.after(() => fs.rmSync(fxIdle.root, { recursive: true, force: true }));
  const outIdle = runInstall(fxIdle, { FIXTURE_MSGPACK_VER: '1.1.1' });
  const logIdle = await readFile(fxIdle.log, 'utf8');
  assert.ok(!logIdle.includes('restart speedex-mitm'), '服务未在跑 → 不得 restart（is-active 门）');
  assert.ok(logIdle.includes('enable --now speedex-mitm'), '未在跑 → enable --now（新进程自载新依赖）');
  assertStdoutHygiene(outIdle);
});

// ── NR-01（2026-09-09 review）：htpasswd sha256 比对 fail-closed ──
// install.sh 的摘要命令缺失/非零退出/空输出必须显式非零退出——旧实现 AFTER 摘要
// 在 [ ] 条件内做命令替换，失败不触发 set -e，「htpasswd 缺失漂移（BEFORE 留空）+
// sha256sum 不可用（AFTER 为空）」会把两个空摘要判相等、漏掉受控重启（假绿）。

// PATH 影子目录：/usr/bin+/bin 全量软链、剔除指定工具——在 GNU 宿主机上离线模拟
// 「工具不在执行 PATH」（审查方 macOS 复现场景的同构模拟）
function buildPathShadow(root, exclude) {
  const shadow = path.join(root, 'path-shadow');
  fs.mkdirSync(shadow, { recursive: true });
  for (const dir of ['/usr/bin', '/bin']) {
    for (const name of fs.readdirSync(dir)) {
      if (exclude.includes(name)) continue;
      try {
        fs.symlinkSync(path.join(dir, name), path.join(shadow, name));
      } catch {
        // 重名（merged /bin→/usr/bin）或不可链接条目跳过
      }
    }
  }
  return shadow;
}

async function assertDigestFailClosed(fx, r) {
  assert.notEqual(r.status, 0, '摘要不可用 → 安装必须 fail-closed 非零退出');
  assert.ok(r.stderr.includes('sha256'), `stderr 必须说明摘要失败原因：${r.stderr.slice(0, 200)}`);
  assert.ok(!r.stdout.includes('== OK =='), '失败路径不得打印 OK 摘要（假绿）');
  for (const secret of [OLD_PASS, NEW_PASS, shaEntry('speedex', OLD_PASS), shaEntry('speedex', NEW_PASS)]) {
    assert.ok(!r.stdout.includes(secret) && !r.stderr.includes(secret), '失败路径 stdout/stderr 绝不含口令/{SHA} 凭据材料');
  }
  const log = fs.existsSync(fx.log) ? await readFile(fx.log, 'utf8') : '';
  assert.ok(!log.includes('restart speedex-mitm'), '中止于摘要门 → 不得 restart');
  assert.ok(!log.includes('enable --now'), '中止于摘要门 → 不得 enable --now');
}

test('NR-01：sha256sum 非零退出 + htpasswd 存在 → BEFORE 摘要失败，fail-closed 中止', async (t) => {
  const fx = buildFixture({ active: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  // 覆盖 fixture 里的转发 shim——模拟摘要命令本身坏掉（非零退出）
  writeStub(path.join(fx.bin, 'sha256sum'), '#!/bin/sh\nexit 1\n');
  await assertDigestFailClosed(fx, runInstallRaw(fx));
});

test('NR-01：sha256sum 不在执行 PATH（影子目录）+ htpasswd 缺失漂移 → 非零退出（旧实现空空相等假绿）', async (t) => {
  const fx = buildFixture({ active: true, htpasswdMissing: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  // 撤掉 shim + 影子 PATH 剔除 sha256sum——旧实现：BEFORE 跳过留空、AFTER 命令
  // 替换失败留空，"" == "" 误判无变化 → 漏 restart 仍零退出；现必须 fail-closed
  fs.rmSync(path.join(fx.bin, 'sha256sum'));
  const shadow = buildPathShadow(fx.root, ['sha256sum']);
  await assertDigestFailClosed(fx, runInstallRaw(fx, { PATH: `${fx.bin}:${shadow}` }));
});

test('NR-01：sha256sum 零退出但空输出 → 空摘要视同失败，fail-closed 中止', async (t) => {
  const fx = buildFixture({ active: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  writeStub(path.join(fx.bin, 'sha256sum'), '#!/bin/sh\nexit 0\n');
  await assertDigestFailClosed(fx, runInstallRaw(fx));
});

test('NR-01：GNU shim 无条件安装；GNU_TOOL_DIRS 置空强制 python 回退时输出与 GNU 语义一致', (t) => {
  const fx = buildFixture({ active: true });
  t.after(() => fs.rmSync(fx.root, { recursive: true, force: true }));
  for (const name of ['realpath', 'sha256sum', 'sort']) {
    assert.ok(fs.statSync(path.join(fx.bin, name)).mode & 0o111, `shim ${name} 必须无条件安装且可执行（不再依赖宿主探测）`);
  }
  // 强制 python 回退（候选目录置空）——macOS 上走的就是这条路径，本地离线验证其语义
  const empty = path.join(fx.root, 'no-gnu');
  fs.mkdirSync(empty);
  const env = { PATH: '/usr/bin:/bin', REALPY, GNU_TOOL_DIRS: empty };
  // sha256sum 回退：GNU 形 "<hex>  <path>"（install.sh 经 cut -d' ' -f1 取指纹）
  const dataFile = path.join(fx.root, 'digest-input.bin');
  fs.writeFileSync(dataFile, 'synthetic-sha256-input-NR01\n');
  const expectedHex = createHash('sha256').update('synthetic-sha256-input-NR01\n').digest('hex');
  const shaOut = execFileSync(path.join(fx.bin, 'sha256sum'), [dataFile], { encoding: 'utf8', env }).trim();
  assert.equal(shaOut, `${expectedHex}  ${dataFile}`, 'sha256sum 回退输出必须是 GNU 形 "<hex>  <path>"');
  // sort -V 回退：点分版本元组序（install.sh 只比 3.x/3.12 形态）
  const sortOut = execFileSync(path.join(fx.bin, 'sort'), ['-V'], {
    encoding: 'utf8',
    env,
    input: '3.12\n3.9\n3.12.1\n3.11\n',
  });
  assert.equal(sortOut, '3.9\n3.11\n3.12\n3.12.1\n', 'sort -V 回退必须按点分数字段元组排序');
  // realpath -m 回退：解析已存在组件 + 压平 ./..//、缺失尾部不报错
  const rpOut = execFileSync(path.join(fx.bin, 'realpath'), ['-m', '--', `${fx.root}//a/./b/../c`], {
    encoding: 'utf8',
    env,
  }).trim();
  assert.equal(rpOut, path.join(fs.realpathSync(fx.root), 'a', 'c'), 'realpath -m 回退必须压平 ./..// 段');
});
