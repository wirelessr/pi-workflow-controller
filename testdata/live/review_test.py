"""Exercise the live verifier with an external Controller and local HTTP hub."""
import contextlib
import io
import json
import os
from pathlib import Path
import runpy
import signal
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
import urllib.request


class ReviewExitTest(unittest.TestCase):
    def test_controller_exit_is_propagated_and_recorded(self):
        script = Path(__file__).with_name("review.py")
        requests = []

        class Hub(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                if self.path != "/api/sessions":
                    self.send_error(404)
                    return
                body = b'{"sessions":[]}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Hub) as hub:
            thread = threading.Thread(target=hub.serve_forever)
            thread.start()
            original_urlopen = urllib.request.urlopen

            def local_hub(url, *args, **kwargs):
                # Redirect only the external HTTP boundary, leaving verifier logic real.
                prefix = "http://127.0.0.1:8730"
                self.assertTrue(url.startswith(prefix + "/"), url)
                return original_urlopen(
                    f"http://127.0.0.1:{hub.server_port}" + url[len(prefix):],
                    *args,
                    **kwargs,
                )

            try:
                for controller_exit in (0, 7, -signal.SIGTERM):
                    with self.subTest(controller_exit=controller_exit), tempfile.TemporaryDirectory() as directory:
                        home = Path(directory)
                        bridge = home / "bridge"
                        bridge.mkdir()
                        extension = home / ".pi/agent/extensions/pi-webui-extension"
                        extension.mkdir(parents=True)
                        for name in ("index.ts", "session-helpers.js"):
                            (extension / name).write_text("fixture deployed recovery source\n")
                        controller = home / "controller"
                        controller.write_text(
                            f"#!{sys.executable}\n"
                            "import os, signal, sys\n"
                            "assert sys.argv[1:3] == ['run', 'code-review']\n"
                            f"code = {int(controller_exit)}\n"
                            "if code < 0:\n"
                            "    os.kill(os.getpid(), -code)\n"
                            "sys.exit(code)\n"
                        )
                        controller.chmod(0o700)
                        destination = home / "artifacts"
                        output = io.StringIO()
                        argv = [str(script), str(controller), "https://github.com/owner/repo/pull/17", str(destination)]
                        with patch.dict(os.environ, {"HOME": str(home), "PI_BRIDGE_DIR": str(bridge)}), patch.object(sys, "argv", argv), patch("urllib.request.urlopen", side_effect=local_hub), contextlib.redirect_stdout(output):
                            if controller_exit == 0:
                                runpy.run_path(str(script), run_name="__main__")
                            else:
                                with self.assertRaises(SystemExit) as raised:
                                    runpy.run_path(str(script), run_name="__main__")
                                expected = controller_exit if controller_exit > 0 else 128 - controller_exit
                                self.assertEqual(raised.exception.code, expected)
                        recorded = json.loads((destination / "exit.json").read_text())
                        self.assertEqual(recorded["exit"], controller_exit)
                        summary = json.loads(output.getvalue())
                        self.assertEqual(summary["exit"], controller_exit)
                        self.assertEqual(summary["artifacts"], str(destination.resolve()))
                        self.assertEqual(list(bridge.iterdir()), [])
                self.assertTrue(requests)
                self.assertEqual(set(requests), {"/api/sessions"})
            finally:
                hub.shutdown()
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
