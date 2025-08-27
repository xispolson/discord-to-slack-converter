# Discord → Slack CSV Importer

A tiny Python utility that converts Discord JSON exports into a Slack-importable CSV, with built-in sanitization and a preflight validator.

## Highlights

- One or many JSON files → one CSV (Slack’s importer prefers a single file)
- Sanitizes troublesome content:
  - strips stray outer quotes, unescapes \" and literal \n
  - normalizes newlines
  - removes control/format characters (keeps real newlines/tabs)
  - softens mentions (@everyone → everyone, @user → user)
  - removes triple-backtick code fences (keeps content)
  - appends attachment/embed URLs (each on its own line)
- Normalizes usernames and channel names to Slack-safe forms
- Filters to standard Discord messages (type == 0) by default
- Preflight check for CSVs: shape, timestamps, sorting, channel/user format

## Requirements
- Python 3.8+
- No external packages required (stdlib only)

## Install / Setup
1. Save the script as importer.py in a working folder with your Discord JSON exports.
2. (Optional) Create a virtual environment:
```
python -m venv .venv && source .venv/bin/activate   # macOS/Linux
# OR
.venv\Scripts\activate                              # Windows
```

## Quick Start
Convert one or more Discord JSON files into a single Slack-ready CSV:
```
python importer.py *.json -o all-slack-import.csv
```

## Usage
Convert JSON → CSV
```
python importer.py channel1.json channel2.json -o all-slack-import.csv
```
Force everything into a specific channel
```
python importer.py *.json --channel general -o all-slack-import.csv
```
(OPTIONAL) Map Discord channel IDs → Slack channel names<br>
JSON example (`channel_map.json`):
```
{
  "1223484965862510655": "general",
  "1223484965862510666": "sweet-slack-channel"
}
```
CSV example (`channel_map.csv`):
```
1223484965862510655,general
1223484965862510666,sweet-slack-channel
```
Run:
```
python importer.py *.json --channel-map channel_map.json -o all-slack-import.csv
# or
python importer.py *.json --channel-map channel_map.csv -o all-slack-import.csv
```

> Channel selection priority per message:
> --channel (if provided)
> --channel-map by channel_id
> Derived from the input filename (sanitized)

### Preflight an existing CSV (no conversion)
```
python importer.py --preflight-only all-slack-import.csv
```
Outputs accounts for:
- wrong_cols (must be exactly 4 columns)
- bad_ts (non-numeric timestamps)
- unsorted (timestamps not ascending)
- bad_channel (violates [a-z0-9_-]{1,80})
- bad_username (violates [A-Za-z0-9._-]{1,80})


