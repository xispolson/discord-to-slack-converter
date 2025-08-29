#!/usr/bin/env python3
"""
Discord -> Slack CSV importer with built-in sanitization + preflight.

Output CSV columns (quoted): timestamp, channel, username, text
Sorted oldest->newest. Compatible with Slack's CSV import.

Examples:
  # Convert multiple JSON exports into one CSV
  python importer.py *.json -o all-slack-import.csv

  # Force everything into a specific channel
  python importer.py *.json --channel general -o all-slack-import.csv

  # Map Discord channel_id -> Slack channel name (json or 2-col csv)
  python importer.py *.json --channel-map channel_map.json -o all-slack-import.csv

  # Preflight an existing CSV (no conversion)
  python importer.py --preflight-only all-slack-import.csv
"""
import argparse
import glob
import csv
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

# -----------------------------
# Helpers: parsing + extraction
# -----------------------------

def to_epoch_seconds(ts_val: Any) -> int:
    """Best-effort parse of timestamps into epoch seconds (UTC)."""
    if isinstance(ts_val, (int, float)):
        return int(ts_val)
    if not ts_val:
        return 0
    s = str(ts_val)
    # ISO8601-ish
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        pass
    # Fallback: bare digits (maybe ms)
    try:
        n = int(re.sub(r"[^\d]", "", s))
        return int(n // 1000) if n > 10**12 else n
    except Exception:
        return 0

def extract_messages(obj: Any) -> List[Dict]:
    """
    Find the message list inside common Discord export shapes.
    Returns a list of dicts (messages). Unknown shapes -> [].
    """
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for k in ("data", "messages", "results", "records"):
            if k in obj and isinstance(obj[k], list):
                return [x for x in obj[k] if isinstance(x, dict)]
        # dict-of-dicts (e.g., channel -> {id -> msg})
        vals = list(obj.values())
        if vals and isinstance(vals[0], dict):
            inner = list(vals[0].values())
            if inner and isinstance(inner[0], dict):
                return inner
            return vals
    return []

def attachments_to_text(attachments: Any, embeds: Any) -> str:
    """Make a printable list of attachment + embed URLs (one per line)."""
    parts: List[str] = []
    if isinstance(attachments, list):
        for a in attachments:
            if isinstance(a, dict):
                u = a.get("url") or a.get("proxy_url") or a.get("filename")
                if u:
                    parts.append(str(u))
            else:
                parts.append(str(a))
    if isinstance(embeds, list):
        for e in embeds:
            if isinstance(e, dict) and e.get("url"):
                parts.append(str(e["url"]))
    return "\n".join(parts).strip()

def channel_name_from_filename(path: str) -> str:
    base = os.path.basename(path)
    name = re.split(r"\.json$", base, flags=re.I)[0]
    name = name.split("_page")[0]
    return sanitize_channel(name)

def load_channel_map(path: str) -> Dict[str, str]:
    if not path:
        return {}
    mapping: Dict[str, str] = {}
    if path.lower().endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            mapping = json.load(f)
    else:
        # 2-column CSV: channel_id,channel_name
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) >= 2:
                    mapping[row[0].strip()] = row[1].strip()
    return mapping

# -----------------------------
# Sanitizers
# -----------------------------

def strip_mentions(text: str) -> str:
    if not text:
        return ""
    # soften @everyone / @here
    text = re.sub(r"@everyone\b", "everyone", text)
    text = re.sub(r"@here\b", "here", text)
    # turn @username into username (drop leading @)
    text = re.sub(r"@([A-Za-z0-9_.-]{1,80})", r"\1", text)
    return text

def strip_code_fences(text: str) -> str:
    if not text:
        return ""
    # remove triple-backtick fences while keeping content
    text = re.sub(r"```(?:\w+)?\n?", "", text)
    text = text.replace("```", "")
    return text

def remove_problem_controls(s: str) -> str:
    """Remove control/format/surrogate/private-use chars except \n and \t."""
    if not s:
        return ""
    out = []
    for ch in s:
        cat = unicodedata.category(ch)
        if cat in ("Cc", "Cf", "Cs", "Co") and ch not in ("\n", "\t"):
            continue
        out.append(ch)
    return "".join(out)

def clean_text(s: str) -> str:
    """Make message text safe for Slack CSV import."""
    if not s:
        return ""
    # strip one stray outer-quote pair if present
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    # unescape common sequences that sometimes sneak in
    s = s.replace(r"\\r\\n", "\n")  # literal backslashes first
    s = s.replace(r"\\n", "\n").replace(r"\\r", "\n")
    s = s.replace('\\"', '"').replace("\\'", "'")
    # normalize platform newlines
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # drop bidi controls/zero-width, etc. (except \n/\t)
    s = remove_problem_controls(s)
    return s

def sanitize_channel(ch: str) -> str:
    ch = (ch or "").lower().strip().replace(" ", "-").replace(".", "-")
    ch = re.sub(r"[^a-z0-9_-]", "-", ch)
    ch = re.sub(r"-{2,}", "-", ch).strip("-")
    return ch[:80] or "general"

def sanitize_username(u: str) -> str:
    u = (u or "").strip()
    # drop leading/trailing punctuation
    u = re.sub(r"^[^\w]+", "", u)
    u = re.sub(r"[^\w.-]+$", "", u)
    # replace remaining disallowed with underscore
    u = re.sub(r"[^\w.-]", "_", u)
    if not u or not re.match(r"[A-Za-z0-9]", u):
        u = "user_import"
    return u[:80]

# -----------------------------
# CSV preflight
# -----------------------------

CHAN_RE = re.compile(r"^[a-z0-9_-]{1,80}$")
USER_RE = re.compile(r"^[A-Za-z0-9._-]{1,80}$")

def preflight_slack_csv(path: str) -> Dict[str, int]:
    """
    Validate a CSV for Slack import: 4 columns, numeric ts,
    safe channel/username patterns, ascending timestamps.
    """
    issues = {
        "wrong_cols": 0,
        "bad_ts": 0,
        "bad_channel": 0,
        "bad_username": 0,
        "unsorted": 0,
        "rows": 0,
    }
    last_ts = -10**18
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            issues["rows"] += 1
            if len(row) != 4:
                issues["wrong_cols"] += 1
                continue
            ts, ch, user, _ = row
            # timestamp numeric?
            if not re.fullmatch(r"\d+", ts or ""):
                issues["bad_ts"] += 1
            else:
                it = int(ts)
                if it < last_ts:
                    issues["unsorted"] += 1
                last_ts = it
            # channel & username patterns
            if not CHAN_RE.fullmatch(ch or ""):
                issues["bad_channel"] += 1
            if not USER_RE.fullmatch(user or ""):
                issues["bad_username"] += 1
    return issues

def print_preflight(issues: Dict[str, int], label: str = "CSV"):
    print(f"[Preflight] {label}")
    for k in ("rows","wrong_cols","bad_ts","unsorted","bad_channel","bad_username"):
        print(f"  {k}: {issues[k]}")

# -----------------------------
# Conversion (JSON -> rows)
# -----------------------------

def rows_from_json_file(infile: str, force_channel: str, chan_map: Dict[str, str]) -> List[Tuple[int,str,str,str]]:
    with open(infile, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = extract_messages(data)
    rows: List[Tuple[int,str,str,str]] = []

    for m in messages:
        if not isinstance(m, dict):
            continue
        # standard chat messages only (Discord type == 0)
        if m.get("type", 0) != 0:
            continue

        ts = to_epoch_seconds(m.get("timestamp"))
        username = sanitize_username(m.get("userName") or (m.get("author") or {}).get("username") or "user_import")

        # channel selection priority:
        # 1) --channel (force)
        # 2) --channel-map using m["channel_id"]
        # 3) derived from filename
        discord_chan_id = str(m.get("channel_id", "")).strip()
        if force_channel:
            channel = force_channel
        elif discord_chan_id and discord_chan_id in chan_map:
            channel = chan_map[discord_chan_id]
        else:
            channel = channel_name_from_filename(infile)
        channel = sanitize_channel(channel)

        text = clean_text(
            strip_code_fences(
                strip_mentions(m.get("content") or "")
            )
        )

        extra = attachments_to_text(m.get("attachments", []), m.get("embeds", []))
        if extra:
            text = (text + ("\n" if text else "") + extra).strip()

        rows.append((ts, channel, username, text))

    return rows

def expand_inputs(patterns):
    """Expand *.json and **/*.json patterns and also allow directories."""
    files = []
    for pat in patterns:
        if os.path.isdir(pat):
            # take all JSONs under this dir, recursively
            files.extend(glob.glob(os.path.join(pat, "**", "*.json"), recursive=True))
            continue
        hits = glob.glob(pat, recursive=True)
        if hits:
            files.extend(hits)
        else:
            # if it looks like a file path, keep it if it exists
            if pat.lower().endswith(".json") and os.path.exists(pat):
                files.append(pat)
            else:
                print(f"Warning: pattern matched no files: {pat}", file=sys.stderr)
    return files

# -----------------------------
# CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Convert Discord JSON to a Slack-importable CSV with sanitization and preflight.")
    ap.add_argument("inputs", nargs="+",
                    help="Discord JSON files (patterns or dirs ok) OR a single CSV with --preflight-only")
    ap.add_argument("-o", "--outfile", default="discord-to-slack.csv", help="Output CSV path")
    ap.add_argument("--channel", help="Force all rows into this Slack channel name (e.g., general)")
    ap.add_argument("--channel-map", help="JSON or 2-col CSV: Discord channel_id -> Slack channel name")
    ap.add_argument("--preflight-only", action="store_true", help="Only run preflight on the given CSV (no conversion)")
    args = ap.parse_args()

    # Preflight-only mode
    if args.preflight_only:
        expanded = expand_inputs(args.inputs)
        if len(expanded) != 1 or not expanded[0].lower().endswith(".csv"):
            print("In --preflight-only mode, pass exactly one CSV file (patterns/dirs allowed).", file=sys.stderr)
            return 2
        issues = preflight_slack_csv(expanded[0])
        print_preflight(issues, label=os.path.basename(expanded[0]))
        return 0

    chan_map = load_channel_map(args.channel_map) if args.channel_map else {}
    file_args = expand_inputs(args.inputs)
    # keep only .json
    file_args = [p for p in file_args if p.lower().endswith(".json")]
    if not file_args:
        print("No JSON files found from inputs/patterns. Example: *.json or data/**/*.json", file=sys.stderr)
        return 2

    # Build rows from all JSON inputs
    all_rows = []
    forced_channel = sanitize_channel(args.channel) if args.channel else ""
    for infile in file_args:
        all_rows.extend(rows_from_json_file(infile, forced_channel, chan_map))

    # Sort ascending by timestamp
    all_rows.sort(key=lambda r: r[0])

    # Write CSV (all fields quoted; raw newlines allowed inside text)
    with open(args.outfile, "w", encoding="utf-8", newline="") as out:
        w = csv.writer(out, quoting=csv.QUOTE_ALL)
        for ts, ch, user, text in all_rows:
            w.writerow([str(int(ts)), ch, user, text])

    # Preflight the result and print a summary
    issues = preflight_slack_csv(args.outfile)
    print_preflight(issues, label=os.path.basename(args.outfile))

    # Friendly summary
    if all(issues[k] == 0 for k in ("wrong_cols","bad_ts","unsorted","bad_channel","bad_username")):
        print(f"\n✅ Wrote {len(all_rows)} rows to {args.outfile} (looks Slack-ready).")
    else:
        print(f"\n⚠️  Wrote {len(all_rows)} rows to {args.outfile}, but preflight found issues above.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
