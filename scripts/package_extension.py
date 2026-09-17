"""Build the canonical Ruby extension without requiring the rubyzip gem."""
import json
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root = Path(__file__).resolve().parents[1]
source = root / "su_mcp"
version = json.loads((source / "extension.json").read_text())["version"]
output = root / "dist" / f"su_mcp_v{version}.rbz"
output.parent.mkdir(exist_ok=True)
with ZipFile(output, "w", ZIP_DEFLATED) as archive:
    for name in ("su_mcp.rb", "su_mcp/main.rb", "extension.json"):
        archive.write(source / name, name)
with ZipFile(output) as archive:
    assert archive.testzip() is None
    assert set(archive.namelist()) == {"su_mcp.rb", "su_mcp/main.rb", "extension.json"}
print(output)
