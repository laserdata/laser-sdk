import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ManagedBddFixtureTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "the managed BDD runner uses Bash")
    def test_given_a_managed_node_when_started_then_should_use_the_test_reconcile_interval(self):
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            server = tmp_path / "server"
            server.write_text(
                "#!/bin/sh\n"
                'printf "%s\\n" "$IGGY_SHARDING_RECONCILE_PERIODIC_INTERVAL"\n'
                'kill -TERM "$PPID"\n'
            )
            server.chmod(0o755)
            environment = os.environ.copy()
            environment.pop("IGGY_SHARDING_RECONCILE_PERIODIC_INTERVAL", None)
            subprocess.run(
                [ROOT / "scripts/run-managed-bdd.sh", "stack"],
                env=environment
                | {
                    "LASER_MANAGED_IGGY_SERVER": str(server),
                    "LASER_MANAGED_BACKEND": str(server),
                    "TMPDIR": str(tmp_path),
                },
                capture_output=True,
                timeout=10,
            )
            (log,) = tmp_path.glob("laser-managed-bdd.*/iggy.log")
            self.assertEqual(log.read_text().strip(), "200 ms")
