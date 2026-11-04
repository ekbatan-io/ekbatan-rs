"""The weekly dependency report for ekbatan-rs: known vulnerabilities and newer releases.

    python3 report.py --out report.md --state state.json

- Known vulnerabilities: every crate pinned in Cargo.lock (transitive ones included), looked up
  on osv.dev.
- Newer versions: each crate a Cargo.toml names, against its releases on crates.io. A release
  counts only once it is 14 days old (the project's 2-week rule); younger ones are listed as
  waiting. Crates held back on purpose (held-back.txt) are listed apart, with the reason.
- The Rust toolchain in rust-toolchain.toml, against the newest stable release 14 days old.

`state.json` tells issue.py what to do: whether anything is left to act on, and which advisories
the report names. Standard library only.
"""

from __future__ import annotations

import argparse
import datetime
import fnmatch
import json
import pathlib
import re
import tomllib
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
HERE = pathlib.Path(__file__).resolve().parent
MIN_AGE = datetime.timedelta(days=14)
OSV = "https://api.osv.dev/v1"
CRATES = "https://crates.io/api/v1/crates"
AGENT = {"User-Agent": "ekbatan-rs dependency check (github.com/ekbatan-io/ekbatan-rs)"}


def get_json(url: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {**AGENT, **({"Content-Type": "application/json"} if data else {})}
    with urllib.request.urlopen(urllib.request.Request(url, data, headers), timeout=60) as r:
        return json.load(r)


def version_key(version: str) -> tuple:
    core = version.split("+")[0].split("-")[0]
    return tuple(int(p) if p.isdigit() else 0 for p in core.split("."))


def locked() -> dict[str, list[str]]:
    """Each crates.io crate in Cargo.lock, with its locked versions."""
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    crates: dict[str, list[str]] = {}
    for package in lock["package"]:
        if package.get("source", "").startswith("registry+"):
            crates.setdefault(package["name"], []).append(package["version"])
    return crates


def declared() -> set[str]:
    """Every crate a Cargo.toml in the workspace names with a version."""
    names: set[str] = set()
    for manifest in [ROOT / "Cargo.toml", *ROOT.glob("*/Cargo.toml"), *ROOT.glob("*/*/Cargo.toml")]:
        if "target" in manifest.parts:
            continue
        data = tomllib.loads(manifest.read_text())
        tables = [data.get("workspace", {}).get("dependencies", {})]
        tables += [data.get(k, {}) for k in ("dependencies", "dev-dependencies", "build-dependencies")]
        for table in tables:
            for name, spec in table.items():
                if isinstance(spec, str) or (
                    isinstance(spec, dict) and "version" in spec and "path" not in spec
                ):
                    names.add(spec.get("package", name) if isinstance(spec, dict) else name)
    return names


def held_back() -> list[tuple[str, str, str]]:
    entries = []
    for line in (HERE / "held-back.txt").read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            name, version, reason = (line.split(None, 2) + ["", ""])[:3]
            entries.append((name, version, reason.strip()))
    return entries


def held_reason(entries, name: str, version: str) -> str | None:
    for pattern, version_pattern, reason in entries:
        if fnmatch.fnmatch(name, pattern) and fnmatch.fnmatch(version, version_pattern):
            return reason
    return None


def releases(name: str) -> list[tuple[str, datetime.date]]:
    """crates.io releases of `name`: final (no pre-release), not yanked, newest first."""
    out = []
    for v in get_json(f"{CRATES}/{name}/versions?per_page=100")["versions"]:
        if v["yanked"] or "-" in v["num"]:
            continue
        out.append((v["num"], datetime.date.fromisoformat(v["created_at"][:10])))
    return sorted(out, key=lambda r: version_key(r[0]), reverse=True)


def newer_versions(crates: dict[str, list[str]], names: set[str], today: datetime.date):
    entries = held_back()
    updates, waiting, held = [], [], []
    for name in sorted(names):
        if name not in crates:
            continue
        current = max(crates[name], key=version_key)
        for version, published in releases(name):
            if version_key(version) <= version_key(current):
                break
            reason = held_reason(entries, name, version)
            if reason:
                held.append((name, current, version, reason))
            elif today - published >= MIN_AGE:
                updates.append((name, current, version, published))
                break
            elif not any(w[0] == name for w in waiting):
                # The newest younger release only; an older one may still be takeable.
                waiting.append((name, current, version, published + MIN_AGE))
    return updates, waiting, held


def toolchain(today: datetime.date):
    """(pinned, newest stable 14 days old) if the pin is behind, else None."""
    pinned = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    manifest = urllib.request.urlopen(
        urllib.request.Request("https://static.rust-lang.org/dist/channel-rust-stable.toml", None, AGENT),
        timeout=60,
    ).read().decode()
    version = re.search(r'\[pkg\.rust\]\s*version = "([0-9.]+)', manifest).group(1)
    date = datetime.date.fromisoformat(re.search(r'^date = "([0-9-]+)"', manifest, re.M).group(1))
    if version_key(version) > version_key(pinned) and today - date >= MIN_AGE:
        return pinned, version, date
    return None


def vulnerabilities(crates: dict[str, list[str]]):
    pairs = [(n, v) for n, versions in crates.items() for v in versions]
    found: dict[tuple[str, str], list[str]] = {}
    for start in range(0, len(pairs), 500):
        chunk = pairs[start : start + 500]
        queries = [{"package": {"ecosystem": "crates.io", "name": n}, "version": v} for n, v in chunk]
        for (n, v), result in zip(chunk, get_json(f"{OSV}/querybatch", {"queries": queries})["results"]):
            ids = [vuln["id"] for vuln in result.get("vulns", [])]
            if ids:
                found[(n, v)] = ids
    return found


def summary_of(vid: str) -> str:
    advisory = get_json(f"{OSV}/vulns/{vid}")
    return (advisory.get("summary") or advisory.get("details", "")[:120]).replace("|", "/").strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=pathlib.Path, default=pathlib.Path("report.md"))
    parser.add_argument("--state", type=pathlib.Path, default=pathlib.Path("state.json"))
    parser.add_argument("--today", type=datetime.date.fromisoformat, default=datetime.date.today())
    args = parser.parse_args()

    crates = locked()
    found = vulnerabilities(crates)
    updates, waiting, held = newer_versions(crates, declared(), args.today)
    rust = toolchain(args.today)

    lines = ["# Dependency report", ""]
    lines.append(f"Checked {sum(len(v) for v in crates.values())} crates in `Cargo.lock` on {args.today}.")
    lines += ["", "## Known vulnerabilities", ""]
    if found:
        lines += ["| crate | version | advisory |", "|---|---|---|"]
        for (name, version), ids in sorted(found.items()):
            for vid in ids:
                lines.append(f"| {name} | {version} | [{vid}](https://osv.dev/vulnerability/{vid}): {summary_of(vid)} |")
    else:
        lines.append("None.")
    lines += ["", "## Newer releases, 2 weeks old or more", ""]
    if updates or rust:
        lines += ["| crate | locked | newer | released |", "|---|---|---|---|"]
        lines += [f"| {n} | {c} | {v} | {d} |" for n, c, v, d in updates]
        if rust:
            lines.append(f"| Rust toolchain | {rust[0]} | {rust[1]} | {rust[2]} |")
    else:
        lines.append("None.")
    if waiting:
        lines += ["", "## Newer releases still under 2 weeks old", "", "| crate | locked | newer | takeable from |", "|---|---|---|---|"]
        lines += [f"| {n} | {c} | {v} | {d} |" for n, c, v, d in waiting]
    if held:
        lines += ["", "## Held back on purpose", "", "| crate | locked | newer | why |", "|---|---|---|---|"]
        lines += [f"| {n} | {c} | {v} | {r} |" for n, c, v, r in held]
    ids = sorted({vid for vids in found.values() for vid in vids})
    lines += ["", f"<!-- vulnerabilities: {' '.join(ids)} -->"]
    state = {
        "clean": not found and not updates and not rust,
        "vulnerabilities": ids,
        "updates": len(updates) + (1 if rust else 0),
        "waiting": len(waiting),
    }
    args.out.write_text("\n".join(lines) + "\n")
    args.state.write_text(json.dumps(state, indent=2) + "\n")
    print(f"{len(ids)} advisories, {state['updates']} newer versions, {state['waiting']} waiting")


if __name__ == "__main__":
    main()
