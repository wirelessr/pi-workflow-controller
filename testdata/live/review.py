"""正式 binary／真 PR／現有 hub 唯讀驗收。僅操作本次自有 Controller。"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.error

binary, pr_url, destination = sys.argv[1:]
root = Path(destination).resolve()
root.mkdir(mode=0o700, parents=True, exist_ok=False)
bridge = Path(os.environ.get("PI_BRIDGE_DIR", str(Path.home() / ".pi/agent/extensions/pi-webui-extension/data")))
extension = Path.home() / ".pi/agent/extensions/pi-webui-extension"


def save(name, value):
    (root / name).write_text(json.dumps(value, ensure_ascii=False, indent=2))


def preflight(label):
    rows = []
    for path in bridge.iterdir():
        if path.name.endswith(".recovering"):
            raise RuntimeError("BLOCKED recovery claim")
        if path.suffix != ".json":
            continue
        try:
            d = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        if not isinstance(d, dict) or not d.get("pid"):
            continue
        pid = d["pid"]
        if type(pid) is not int or pid <= 0:
            raise RuntimeError("BLOCKED unsupported parent pid")
        os.kill(pid, 0)
        rows.append({"file": path.name, "parent_pid": pid})
    # Preserve actual deployed source hashes, not just a remembered version label.
    hashes = {f: hashlib.sha256((extension / f).read_bytes()).hexdigest() for f in ("index.ts", "session-helpers.js")}
    save("preflight-" + label + ".json", {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(), "rows": rows, "source_hashes": hashes, "not_a_lock": True})
    return hashes


def get(path):
    with urllib.request.urlopen("http://127.0.0.1:8730" + path, timeout=5) as response:
        if response.status != 200:
            raise RuntimeError("hub HTTP failure")
        raw = response.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            raise RuntimeError("hub response limit")
        return json.loads(raw)


source_hashes = preflight("initial")
get("/api/sessions")  # Availability only; never persist other sessions' content.
env = dict(os.environ, NODE_TLS_REJECT_UNAUTHORIZED="1", PI_AUTO_NAME="0")
process = None
run_dir = None
seen = {}
started = time.monotonic()
try:
    with (root / "stdout.log").open("wb") as out, (root / "stderr.log").open("wb") as err:
        process = subprocess.Popen([str(Path(binary).resolve()), "run", "code-review", pr_url], cwd=root, env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True)
        save("launch.json", {"pid": process.pid, "binary": str(Path(binary).resolve()), "sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(), "pr": pr_url, "cwd": str(root), "tls_verification": True})
        while process.poll() is None:
            if time.monotonic() - started > 125 * 60:
                raise TimeoutError("live verification deadline")
            if run_dir is None:
                for line in (root / "stdout.log").read_text(errors="replace").splitlines():
                    if line.startswith("Run path: "):
                        run_dir = Path(line[len("Run path: "):])
                        save("run-location.json", {"run_dir": str(run_dir)})
                        break
            if run_dir:
                listing = get("/api/sessions")
                own = [s for s in listing.get("sessions", []) if s.get("pid") == process.pid]
                for session in own:
                    sid = session["sessionId"]
                    if sid not in seen:
                        # Controller performs its own preflight before each Start.
                        if preflight(sid) != source_hashes:
                            raise RuntimeError("BLOCKED deployed recovery source changed")
                        seen[sid] = session
                    try:
                        save(sid + "-status.json", get("/s/" + sid + "/api/status"))
                        save(sid + "-history.json", get("/s/" + sid + "/api/history?limit=0"))
                    except urllib.error.HTTPError as e:
                        if e.code != 404:
                            raise
                        save(sid + "-closed-during-observation.json", {"status": 404})
                save("own-sessions.json", list(seen.values()))
            # Poll an actual process/session state boundary, not a guessed completion time.
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
        save("exit.json", {"exit": process.returncode, "elapsed_seconds": time.monotonic() - started, "hub_sessions_observed": len(seen)})
        if run_dir:
            for name in ("run.json", "result.json", "cleanup.json"):
                save(name, json.loads((run_dir / name).read_text()))
        print(json.dumps({"artifacts": str(root), "run": str(run_dir), "exit": process.returncode, "hub_sessions": len(seen)}))
        if process.returncode != 0:
            raise SystemExit(process.returncode if process.returncode > 0 else 128 - process.returncode)
finally:
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=25)
        except subprocess.TimeoutExpired:
            # Insurance signals only processes still parented by this live Controller.
            if run_dir and (run_dir / "run.json").exists():
                snapshot = json.loads((run_dir / "run.json").read_text())
                for session in snapshot.get("sessions", {}).values():
                    identity = session.get("identity", {})
                    pid = identity.get("PID")
                    if pid:
                        row = subprocess.run(["ps", "-o", "ppid=,pgid=", "-p", str(pid)], capture_output=True, text=True, timeout=5)
                        if row.stdout.split() == [str(process.pid), str(pid)]:
                            os.killpg(pid, signal.SIGKILL)
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
