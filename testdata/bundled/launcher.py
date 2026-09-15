"""透明 RPC tee。Pi 以 exec 保留 Controller 擁有的 PID/PPID。

旁路程序僅轉送原始 bytes；lifeline EOF 時只終止仍以它為父程序的自有 PG。
不載入、替換或模擬任何 Pi 模組或 RPC event。
"""
import json
import os
import select
import signal
import socket
import sys

executable, control, trace_dir, *args = sys.argv[1:]
if "--version" in args:
    os.execv(executable, [executable, *args])

parent = os.getpid()
to_pi_r, to_pi_w = os.pipe()
from_pi_r, from_pi_w = os.pipe()
if os.fork() == 0:
    os.close(to_pi_r)
    os.close(from_pi_w)
    conn = socket.create_connection(("127.0.0.1", int(control)), timeout=5)
    conn.sendall((json.dumps({"direction": "hello", "pid": parent}) + "\n").encode())
    buffers = {0: b"", from_pi_r: b""}
    files = {
        0: open(os.path.join(trace_dir, f"{parent}-stdin.jsonl"), "wb", buffering=0),
        from_pi_r: open(os.path.join(trace_dir, f"{parent}-stdout.jsonl"), "wb", buffering=0),
    }
    written = {0: 0, from_pi_r: 0}
    sources = [0, from_pi_r, conn]

    def stop_owned():
        if os.getppid() == parent:
            os.killpg(parent, signal.SIGKILL)
        os._exit(0)

    try:
        while True:
            readable, _, _ = select.select(sources, [], [])
            for source in readable:
                if source is conn:
                    if not conn.recv(1):
                        stop_owned()
                    continue
                data = os.read(source, 65536)
                if not data:
                    if source == from_pi_r:
                        os._exit(0)
                    os.close(to_pi_w)
                    sources.remove(0)
                    continue
                written[source] += len(data)
                if written[source] > 8 * 1024 * 1024:
                    raise RuntimeError("bounded RPC trace exceeded")
                files[source].write(data)
                buffers[source] += data
                while b"\n" in buffers[source]:
                    line, buffers[source] = buffers[source].split(b"\n", 1)
                    frame = json.loads(line)
                    conn.sendall((json.dumps({"direction": "in" if source == 0 else "out", "pid": parent, "frame": frame}) + "\n").encode())
                destination = to_pi_w if source == 0 else 1
                while data:
                    data = data[os.write(destination, data):]
    except BaseException:
        stop_owned()

os.close(to_pi_w)
os.close(from_pi_r)
os.dup2(to_pi_r, 0)
os.dup2(from_pi_w, 1)
os.close(to_pi_r)
os.close(from_pi_w)
os.execv(executable, [executable, *args])
