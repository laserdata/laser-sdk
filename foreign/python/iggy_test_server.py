import json
import os
import select
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
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


class IggyTestCluster:
    def __init__(self):
        self._binary = None
        self._temporaries = []
        self._logs = []
        self._processes = []
        self._tcp_ports = []
        self._replica_ports = []
        self._http_ports = []
        self._proxy_port = None
        self._proxy_target = None
        self._proxy = None
        self._proxy_thread = None

    @property
    def endpoint(self):
        if not self._tcp_ports:
            raise RuntimeError("Iggy test cluster has not started")
        return f"iggy:iggy@127.0.0.1:{self._proxy_port}"

    def start(self):
        resolved = subprocess.run(
            [RESOLVER], check=True, capture_output=True, text=True, timeout=310
        )
        self._binary = resolved.stdout.strip()
        ports = _free_ports(10)
        self._tcp_ports = ports[:3]
        self._replica_ports = ports[3:6]
        self._http_ports = ports[6:9]
        self._proxy_port = ports[9]
        self._proxy_target = self._tcp_ports[0]
        self._proxy = _StableProxy(("127.0.0.1", self._proxy_port), self)
        self._proxy_thread = threading.Thread(target=self._proxy.serve_forever, daemon=True)
        self._proxy_thread.start()
        for replica_id in range(3):
            temporary = tempfile.TemporaryDirectory(prefix="laser-iggy-cluster-")
            self._temporaries.append(temporary)
            self._logs.append(None)
            self._processes.append(None)
            self._spawn(replica_id)
        try:
            self._wait_for_mesh()
        except Exception:
            self.stop()
            raise
        return self

    def restart_node(self, replica_id):
        self.stop_node(replica_id)
        self.start_node(replica_id)

    def stop_node(self, replica_id):
        self._stop_process(replica_id)

    def start_node(self, replica_id):
        self._spawn(replica_id)
        self._wait_for_mesh(replica_id)

    def route_endpoint_to(self, replica_id):
        self._proxy_target = self._tcp_ports[replica_id]

    def leader_and_follower(self):
        base_url = f"http://127.0.0.1:{self._http_ports[0]}"
        login_body = json.dumps({"username": "iggy", "password": "iggy"}).encode()
        deadline = time.monotonic() + 30
        while True:
            try:
                login = urllib.request.Request(
                    f"{base_url}/users/login",
                    data=login_body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(login, timeout=2) as response:
                    identity = json.load(response)
                token = identity["access_token"]["token"]
                metadata_request = urllib.request.Request(
                    f"{base_url}/cluster/metadata",
                    headers={"Authorization": f"Bearer {token}"},
                )
                with urllib.request.urlopen(metadata_request, timeout=2) as response:
                    metadata = json.load(response)
                leader = next(
                    int(node["name"].removeprefix("node-"))
                    for node in metadata["nodes"]
                    if node["role"] == "leader"
                )
                follower = next(node for node in range(3) if node != leader)
                return leader, follower
            except (KeyError, StopIteration, OSError, urllib.error.URLError):
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "Iggy cluster leader was not discoverable within 30s"
                    ) from None
                time.sleep(0.1)

    def stop(self):
        for replica_id in range(len(self._processes)):
            self._stop_process(replica_id)
        for temporary in self._temporaries:
            temporary.cleanup()
        self._temporaries.clear()
        self._logs.clear()
        self._processes.clear()
        if self._proxy is not None:
            self._proxy.shutdown()
            self._proxy.server_close()
            self._proxy = None
        if self._proxy_thread is not None:
            self._proxy_thread.join(timeout=5)
            self._proxy_thread = None

    def _spawn(self, replica_id):
        data_path = Path(self._temporaries[replica_id].name)
        log = (data_path / "test-server.log").open("w")
        self._logs[replica_id] = log
        env = os.environ.copy()
        env.update(
            {
                "IGGY_SYSTEM_PATH": str(data_path),
                "IGGY_CLUSTER_ENABLED": "true",
                "IGGY_CLUSTER_NAME": "laser-sdk-rolling-restart",
                "IGGY_MESSAGE_BUS_RECONNECT_PERIOD": "100ms",
                "IGGY_HTTP_ENABLED": "true",
                "IGGY_HTTP_ADDRESS": f"127.0.0.1:{self._http_ports[replica_id]}",
                "IGGY_QUIC_ENABLED": "false",
                "IGGY_WEBSOCKET_ENABLED": "false",
                "IGGY_ROOT_USERNAME": "iggy",
                "IGGY_ROOT_PASSWORD": "iggy",
                "IGGY_SHARD_RUNTIME_CAPACITY": "256",
                "IGGY_SYSTEM_SHARDING_CPU_ALLOCATION": "0..1",
                "IGGY_SYSTEM_SHARDING_RECONCILE_PERIODIC_INTERVAL": "200 ms",
            }
        )
        for node in range(3):
            prefix = f"IGGY_CLUSTER_NODES_{node}"
            env.update(
                {
                    f"{prefix}_NAME": f"node-{node}",
                    f"{prefix}_IP": "127.0.0.1",
                    f"{prefix}_REPLICA_ID": str(node),
                    f"{prefix}_PORTS_TCP": str(self._tcp_ports[node]),
                    f"{prefix}_PORTS_TCP_REPLICA": str(self._replica_ports[node]),
                    f"{prefix}_PORTS_HTTP": str(self._http_ports[node]),
                }
            )
        self._processes[replica_id] = subprocess.Popen(
            [self._binary, "--replica-id", str(replica_id)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    def _stop_process(self, replica_id):
        process = self._processes[replica_id]
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self._processes[replica_id] = None
        log = self._logs[replica_id]
        if log is not None:
            log.close()
        self._logs[replica_id] = None

    def _wait_for_mesh(self, replica_id=None):
        deadline = time.monotonic() + 30
        selected = range(3) if replica_id is None else [replica_id]
        while time.monotonic() < deadline:
            ready = True
            for node in selected:
                process = self._processes[node]
                if process.poll() is not None:
                    path = Path(self._temporaries[node].name) / "test-server.log"
                    raise RuntimeError(f"Iggy cluster node exited:\n{path.read_text()}")
                self._logs[node].flush()
                path = Path(self._temporaries[node].name) / "test-server.log"
                ready = ready and "replica mesh complete" in path.read_text()
            if ready:
                return
            time.sleep(0.1)
        raise TimeoutError("Iggy cluster did not form its replica mesh within 30s")


def _free_ports(count):
    listeners = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            listeners.append(listener)
        return [listener.getsockname()[1] for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


class _StableProxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, cluster):
        self.cluster = cluster
        super().__init__(address, _StableProxyHandler)


class _StableProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            upstream = socket.create_connection(
                ("127.0.0.1", self.server.cluster._proxy_target), timeout=2
            )
        except OSError:
            return
        with upstream:
            sockets = [self.request, upstream]
            while True:
                readable, _, _ = select.select(sockets, [], [], 1)
                for source in readable:
                    target = upstream if source is self.request else self.request
                    data = source.recv(64 * 1024)
                    if not data:
                        return
                    target.sendall(data)
