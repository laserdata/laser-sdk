import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
RESOLVER = REPOSITORY_ROOT / "scripts" / "resolve-test-iggy-server.sh"


class IggyTestServer:
    def __init__(self):
        self._temporary = None
        self._log = None
        self._process = None
        self._port = None

    @property
    def endpoint(self):
        if self._port is None:
            raise RuntimeError("Iggy test server has not started")
        return f"iggy:iggy@127.0.0.1:{self._port}"

    def start(self):
        resolved = subprocess.run(
            [RESOLVER],
            check=True,
            capture_output=True,
            text=True,
            timeout=310,
        )
        binary = resolved.stdout.strip()
        self._temporary = tempfile.TemporaryDirectory(prefix="laser-iggy-")
        data_path = Path(self._temporary.name)
        self._port = _free_port()
        self._log = (data_path / "test-server.log").open("w")
        env = os.environ.copy()
        env.update(
            {
                "IGGY_SYSTEM_PATH": str(data_path),
                "IGGY_TCP_ADDRESS": f"127.0.0.1:{self._port}",
                "IGGY_HTTP_ENABLED": "false",
                "IGGY_QUIC_ENABLED": "false",
                "IGGY_WEBSOCKET_ENABLED": "false",
                "IGGY_ROOT_USERNAME": "iggy",
                "IGGY_ROOT_PASSWORD": "iggy",
                "IGGY_SHARD_RUNTIME_CAPACITY": "256",
                "IGGY_SYSTEM_SHARDING_RECONCILE_PERIODIC_INTERVAL": "200 ms",
            }
        )
        self._process = subprocess.Popen(
            [binary],
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        try:
            self._wait_for_tcp()
        except Exception:
            self.stop()
            raise
        return self

    def stop(self):
        if self._process is not None:
            if self._process.poll() is None:
                self._process.terminate()
                try:
                    self._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait(timeout=5)
            self._process = None
        if self._log is not None:
            self._log.close()
            self._log = None
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None

    def _wait_for_tcp(self):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            return_code = self._process.poll()
            if return_code is not None:
                self._log.flush()
                log_path = Path(self._temporary.name) / "test-server.log"
                raise RuntimeError(
                    f"Iggy test server exited with {return_code}:\n{log_path.read_text()}"
                )
            try:
                with socket.create_connection(("127.0.0.1", self._port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.1)
        raise TimeoutError("Iggy test server did not accept TCP connections within 30s")


def _free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]
