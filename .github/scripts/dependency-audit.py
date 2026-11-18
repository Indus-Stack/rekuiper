#!/usr/bin/env python3
"""Check the workspace policy or the standalone core/SQL consumer graph."""

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

CHRONO = {"chrono", "chrono-tz", "chrono-tz-build"}
RUST_CRATES = ("rekuiper-core", "rekuiper-sql")


def read_manifest(path):
    return tomllib.loads(path.read_text(encoding="utf-8"))


def exception_errors(root, strict):
    config = root / "deny.toml"
    if not config.exists() and not strict:
        return
    exceptions = read_manifest(config).get("advisories", {}).get("ignore", [])
    today = datetime.now(timezone.utc).date()
    for entry in exceptions:
        if not isinstance(entry, dict):
            if strict:
                yield f"Exception requires an ID and rationale: {entry}"
            continue
        rationale, *metadata = entry.get("reason", "").split(" | ")
        fields = dict(
            segment.split("=", 1)
            for segment in metadata
            if "=" in segment
        )
        if strict and not all(fields.get(key) for key in ("owner", "tracking", "profiles", "expiry")):
            yield f"{entry['id']} requires owner, tracking, profiles and expiry in its reason"
        if expiry := fields.get("expiry"):
            try:
                expired = date.fromisoformat(expiry) <= today
            except ValueError:
                yield f"{entry['id']} has invalid expiry {expiry!r}"
                continue
            if expired:
                yield f"{entry['id']} expired on {expiry}: {rationale}"


def check_chrono_edges(metadata, consumer=False):
    packages = {p["id"]: p for p in metadata["packages"]}
    for package in packages.values():
        if consumer and package["name"] in CHRONO:
            raise SystemExit(f"Unexpected {package['name']} in standalone consumer graph")
        if package["source"] is None:
            bad = [d["name"] for d in package["dependencies"] if d["name"] in CHRONO]
            if bad:
                raise SystemExit(f"{package['name']} directly declares banned crates: {bad}")
    # Only these external parents may introduce Chrono into the workspace.
    approved_parents = {"rskafka", "arrow-arith", "arrow-array", "arrow-cast", "arrow-ipc", "parquet"}
    for node in metadata["resolve"]["nodes"]:
        parent = packages[node["id"]]["name"]
        for dependency in node["dependencies"]:
            name = packages[dependency]["name"]
            if name in CHRONO and (name != "chrono" or parent not in approved_parents):
                raise SystemExit(f"Unexpected dependency edge {parent} -> {name}")


def metadata(root, locked=True):
    command = ["cargo", "metadata", "--format-version", "1"]
    if locked:
        command.append("--locked")
    return json.loads(subprocess.check_output(command, cwd=root))


def audit(root, offline):
    command = ["cargo", "deny", "--format", "json"]
    if offline:
        command.append("--offline")
    result = subprocess.run(command + ["check", "advisories"], cwd=root, capture_output=True, text=True)
    output = result.stderr + result.stdout
    summaries = []
    for line in output.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        fields = record.get("fields", {})
        if record.get("type") != "diagnostic" or fields.get("severity") not in ("error", "warning"):
            continue
        advisory = fields.get("advisory") or {}
        heading = f"{advisory.get('id') or fields.get('code')}: {fields.get('message')}"
        summaries.append(heading)
        for graph in fields.get("graphs", []):
            def paths(branch, prefix=()):
                crate = branch.get("Krate", {})
                entry = f"{crate.get('name')} {crate.get('version')}"
                parents = branch.get("parents", [])
                if not parents or branch.get("repeat"):
                    yield " <- ".join((*prefix, entry))
                else:
                    for parent in parents:
                        yield from paths(parent, (*prefix, entry))

            summaries.extend(list(paths(graph))[:3])
    if output.strip():
        print(output, end="")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as file:
            ref = os.environ.get("DEPENDENCY_AUDIT_REF", root.name)
            file.write(f"### Advisory audit for {ref}\n\n")
            for item in summaries:
                file.write(f"- {item.replace(chr(10), ' ')}\n")
            if not summaries:
                file.write(f"- cargo-deny exited with status {result.returncode}\n")
    return result.returncode


def consumer_audit(root, offline):
    with tempfile.TemporaryDirectory(prefix="rekuiper-consumer-") as temp:
        fixture = Path(temp)
        # Both crates have no opt-in features at the audited revision. This
        # resolves the same default profile used by the downstream consumer.
        dependencies = "\n".join(
            f'{name} = {{ path = {json.dumps(str(root / "crates" / name))} }}'
            for name in RUST_CRATES
        )
        (fixture / "Cargo.toml").write_text(
            '[package]\nname = "rekuiper-dependency-check"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n'
            f"\n[dependencies]\n{dependencies}\n",
            encoding="utf-8",
        )
        (fixture / "src").mkdir()
        (fixture / "src/main.rs").write_text("fn main() {}\n", encoding="utf-8")
        shutil.copyfile(root / "Cargo.lock", fixture / "Cargo.lock")
        (fixture / "deny.toml").write_text('[advisories]\nyanked = "deny"\nunmaintained = "all"\nignore = []\n', encoding="utf-8")
        check_chrono_edges(metadata(fixture, locked=False), consumer=True)
        status = audit(fixture, offline)
        if status:
            return status
        return subprocess.call(
            ["cargo", "check", "--locked", "--target-dir", str(root / "target")],
            cwd=fixture,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=("workspace", "consumer", "historical"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    if args.offline:
        os.environ["CARGO_NET_OFFLINE"] = "true"
    if args.profile == "consumer":
        return consumer_audit(root, args.offline)
    errors = list(exception_errors(root, strict=args.profile == "workspace"))
    if errors:
        print("\n".join(errors))
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(summary, "a", encoding="utf-8") as file:
                file.writelines(f"- {error}\n" for error in errors)
    if args.profile == "workspace":
        check_chrono_edges(metadata(root))
    return audit(root, args.offline) or bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
