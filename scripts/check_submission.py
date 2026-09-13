"""Validate offline README links and the tracked submission archive layout."""
from __future__ import annotations

from io import BytesIO
from pathlib import Path
import re
import subprocess
import tarfile
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
REQUIRED_ARCHIVE_PATHS = {
    "README.md",
    "data/merchants.csv",
    "deploy/helm/fraud-pipeline/Chart.yaml",
    "docs/architecture.svg",
    "docs/screenshots/hpa-scale-up-2026-09-13.png",
    "services/processor/app.py",
    "services/producer/app.py",
    "services/serving/app.py",
    "services/ui/html/app.js",
}
FORBIDDEN_ARCHIVE_PREFIXES = (
    "tasks/",
    ".smart-pdf-cache/",
    "docs/evidence/review-2026-09-10/",
)
FORBIDDEN_ARCHIVE_PATHS = {
    "docs/compliance-review-ru.md",
    "docs/review-2026-09-10-ru.md",
    "docs/screenshots/scaling.png",
}


def local_readme_targets() -> set[str]:
    targets = set()
    for raw in re.findall(r"!?\[[^]]*]\(([^)]+)\)", README.read_text(encoding="utf-8")):
        target = raw.strip().strip("<>").split("#", 1)[0]
        if target and not re.match(r"^[a-z][a-z0-9+.-]*:", target, re.IGNORECASE):
            targets.add(unquote(target))
    return targets


def archive_paths() -> set[str]:
    archive = subprocess.run(
        ["git", "archive", "--format=tar", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout
    with tarfile.open(fileobj=BytesIO(archive), mode="r:") as bundle:
        return {member.name.rstrip("/") for member in bundle.getmembers() if member.name}


def check() -> None:
    missing_links = sorted(
        target for target in local_readme_targets() if not (ROOT / target).exists()
    )
    assert not missing_links, f"README has missing local targets: {missing_links}"

    archived = archive_paths()
    missing_required = sorted(REQUIRED_ARCHIVE_PATHS - archived)
    assert not missing_required, f"Archive misses required paths: {missing_required}"
    forbidden = sorted(
        path for path in archived
        if path in FORBIDDEN_ARCHIVE_PATHS
        or any(path == prefix.rstrip("/") or path.startswith(prefix)
               for prefix in FORBIDDEN_ARCHIVE_PREFIXES)
    )
    assert not forbidden, f"Archive contains internal or superseded paths: {forbidden}"
    print(f"PASS {len(local_readme_targets())} local README targets resolve")
    print(f"PASS archive contains {len(archived)} clean tracked paths")


if __name__ == "__main__":
    check()
