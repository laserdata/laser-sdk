import importlib.metadata
import pathlib

import laser_sdk as ls

version = importlib.metadata.version("laser-sdk")
print(f"installed laser-sdk {version}")
package = pathlib.Path(ls.__file__).parent
stub = package / "__init__.pyi"
assert stub.is_file(), "the typing stub must ship inside the wheel"
assert (package / "py.typed").is_file(), "the PEP 561 marker must ship inside the wheel"
key = ls.SigningKey(bytes(range(32)))
registry = ls.KeyRegistry()
registry.enroll("smoke", key.verifying_key)
provenance = ls.Provenance(agent="smoke")
assert provenance.conversation_id
print("public surface smoke passed")
