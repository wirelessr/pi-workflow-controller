"""Live bash barrier; closing the test-owned socket is cleanup insurance."""
import json
import os
import socket
import sys

port, nonce = sys.argv[1:]
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    identity = {
        "pid": os.getpid(),
        "pgid": os.getpgrp(),
        "ppid": os.getppid(),
        "nonce": nonce,
        "port": listener.getsockname()[1],
    }
    with socket.create_connection(("127.0.0.1", int(port)), timeout=10) as callback:
        callback.sendall((json.dumps(identity) + "\n").encode())
    print(json.dumps(identity), flush=True)
    listener.settimeout(300)
    conn, _ = listener.accept()
    with conn:
        conn.settimeout(None)
        while True:
            data = conn.recv(4096)
            if not data:
                break
            conn.sendall(data)
