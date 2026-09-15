"""供真實內建 bash 執行的自有 child。Socket EOF 是獨立清理保險。"""
import json
import os
import socket
import sys

port, nonce = sys.argv[1:]
with socket.create_connection(("127.0.0.1", int(port)), timeout=10) as conn:
    conn.settimeout(None)
    conn.sendall((json.dumps({"pid": os.getpid(), "nonce": nonce}) + "\n").encode())
    while True:
        data = conn.recv(4096)
        if not data:
            break
        conn.sendall(data)
