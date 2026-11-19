"""Run one language against a catalog that refuses its first three binding reads."""

import os
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "foreign/python"))
from iggy_test_server import IggyTestServer, StandInCatalog  # noqa: E402


def main(command):
    with ExitStack() as resources:
        catalog = StandInCatalog(nodes=1, unanswered_binding_reads=3)
        resources.callback(catalog.stop)
        catalog.start()
        server = IggyTestServer(environment=catalog.node_environment(0))
        resources.callback(server.stop)
        server.start()
        return subprocess.run(
            command,
            env={
                **os.environ,
                "LASER_BDD_RECOVERING_CATALOG": "1",
                "LASER_BDD_URL": server.endpoint,
                "LASER_CONNECTION_STRING": server.endpoint,
            },
        ).returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
