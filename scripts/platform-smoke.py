"""Exercise native server startup, durable publishing, delivery and settlement.

Compilation alone misses filesystem failures during queue materialization. This
bounded smoke workload runs on each native CI runner and retains logs on failure.
"""

import argparse
import base64
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import time
import urllib.request


def free_ports():
    with socket.socket() as broker, socket.socket() as admin:
        broker.bind(("127.0.0.1", 0))
        admin.bind(("127.0.0.1", 0))
        return broker.getsockname()[1], admin.getsockname()[1]


def get(port, route):
    request = urllib.request.Request(f"http://127.0.0.1:{port}{route}", headers={
        "Authorization": "Basic " + base64.b64encode(b"fibril:fibril").decode()})
    with urllib.request.urlopen(request, timeout=2) as response:
        return response.read().decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    data = args.output / "data"
    data.mkdir()
    broker_port, admin_port = free_ports()
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("FIBRIL_", "KERATIN_", "STROMA_", "TOKIO_"))}
    env["RUST_LOG"] = "warn"
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with (args.output / "server.log").open("w") as server_log:
        server = subprocess.Popen([
            str(args.server.resolve()), "--data-dir", str(data),
            "--broker-bind", f"127.0.0.1:{broker_port}",
            "--admin-bind", f"127.0.0.1:{admin_port}",
        ], cwd=data, env=env, stdout=server_log, stderr=subprocess.STDOUT, creationflags=flags)
        try:
            deadline = time.monotonic() + 30
            while True:
                if server.poll() is not None:
                    raise RuntimeError(f"Server exited during startup: {server.returncode}")
                try:
                    if get(admin_port, "/healthz") == "ok":
                        break
                except (OSError, TimeoutError):
                    pass
                if time.monotonic() >= deadline:
                    raise TimeoutError("Server health deadline exceeded")
                time.sleep(0.1)
            with (args.output / "client.log").open("w") as client_log:
                subprocess.run([
                    str(args.client.resolve()), "--broker-addr", f"127.0.0.1:{broker_port}",
                    "--mode", "mixed", "--writers", "1", "--readers", "1",
                    "--rate-per-sec", "500", "--warmup-secs", "1", "--duration-secs", "3",
                    "--drain-timeout-secs", "10", "--size", "1024", "--prefetch", "128",
                    "--confirmed", "--confirm-window", "64",
                ], cwd=args.output, env=env, stdout=client_log, stderr=subprocess.STDOUT,
                    check=True, timeout=30, creationflags=flags)
            text = (args.output / "client.log").read_text()
            counts = {key: int(value) for key, value in re.findall(r"^([^:\n]+): (\d+)$", text, re.M)}
            for key in ("Publish errors", "Confirm errors", "Measured missing"):
                if counts.get(key) != 0:
                    raise AssertionError(f"{key}: {counts.get(key)}")
            sent = counts.get("Sent total", 0)
            if not sent or counts.get("Confirmed total") != sent or counts.get("Received total") != sent:
                raise AssertionError(f"Incomplete confirmed delivery: {counts}")
            if counts.get("Measured sent", 0) <= 0:
                raise AssertionError("No measured messages")
            deadline = time.monotonic() + 10
            while True:
                queues = json.loads(get(admin_port, "/admin/api/queues"))
                (args.output / "queues.json").write_text(json.dumps(queues, indent=2) + "\n")
                queue = queues.get("queues", {}).get("topic1", {})
                if queue.get("status") == "ok" and queue.get("ready_count") == 0 and queue.get("inflight_count") == 0:
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(f"Queue did not settle: {queues}")
                time.sleep(0.2)
            print(f"Native durable smoke passed: {sent} confirmed, delivered and settled messages")
        finally:
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=10)


if __name__ == "__main__":
    main()
