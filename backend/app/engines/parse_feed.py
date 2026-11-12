"""Parse local fixture feeds into normalized staging records."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = ("business_key", "title", "price", "category", "event_ts", "source_revision")


def _normalize_item(raw: dict[str, Any]) -> dict[str, Any]:
    missing = [k for k in REQUIRED_FIELDS if k not in raw]
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")
    return {
        "business_key": str(raw["business_key"]).strip(),
        "title": str(raw["title"]).strip(),
        "price": float(raw["price"]),
        "category": str(raw["category"]).strip(),
        "event_ts": str(raw["event_ts"]).strip(),
        "source_revision": int(raw["source_revision"]),
        "payload": {
            "business_key": str(raw["business_key"]).strip(),
            "title": str(raw["title"]).strip(),
            "price": float(raw["price"]),
            "category": str(raw["category"]).strip(),
            "event_ts": str(raw["event_ts"]).strip(),
            "source_revision": int(raw["source_revision"]),
        },
    }


def parse_json(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "items" in data:
        items = data["items"]
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError("JSON feed must be a list or an object with items[]")
    return [_normalize_item(x) for x in items]


def parse_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            rows.append(_normalize_item(json.loads(text)))
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"jsonl line {line_no}: {exc}") from exc
    return rows


def parse_xml(path: Path) -> list[dict[str, Any]]:
    root = ET.fromstring(path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for node in root.findall(".//item"):
        raw = {child.tag: child.text for child in list(node)}
        rows.append(_normalize_item(raw))
    return rows


PARSERS = {
    "json": parse_json,
    "jsonl": parse_jsonl,
    "xml": parse_xml,
}


def parse_fixture(path: Path, fmt: str) -> list[dict[str, Any]]:
    parser = PARSERS.get(fmt)
    if parser is None:
        raise ValueError(f"unsupported format: {fmt}")
    if not path.exists():
        raise FileNotFoundError(str(path))
    return parser(path)


def iter_fixture_records(path: Path, fmt: str) -> list[tuple[bool, dict[str, Any]]]:
    """Lenient per-row parse used by the RPA step queue.

    Returns one ``(ok, value)`` tuple per fixture row:
    - ``ok=True``  -> value is a normalized item dict
    - ``ok=False`` -> value is ``{"raw": ..., "error": "..."}`` for a row the
      form fill must reject; the row still becomes a queue step that fails closed.

    Whole-file structural failures (missing file, bad XML / JSON envelope) still raise.
    """
    if not path.exists():
        raise FileNotFoundError(str(path))
    if fmt == "json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "items" in data:
            raw_rows = data["items"]
        elif isinstance(data, list):
            raw_rows = data
        else:
            raise ValueError("JSON feed must be a list or an object with items[]")
    elif fmt == "jsonl":
        raw_rows = []
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            text = line.strip()
            if not text:
                continue
            try:
                raw_rows.append(json.loads(text))
            except Exception as exc:  # noqa: BLE001
                raw_rows.append({"__parse_error__": f"jsonl line {line_no}: {exc}", "__raw__": text})
    elif fmt == "xml":
        root = ET.fromstring(path.read_text(encoding="utf-8"))
        raw_rows = [{child.tag: child.text for child in list(node)} for node in root.findall(".//item")]
    else:
        raise ValueError(f"unsupported format: {fmt}")

    records: list[tuple[bool, dict[str, Any]]] = []
    for raw in raw_rows:
        if isinstance(raw, dict) and "__parse_error__" in raw:
            records.append((False, {"raw": raw.get("__raw__"), "error": raw["__parse_error__"]}))
            continue
        try:
            records.append((True, _normalize_item(raw)))
        except Exception as exc:  # noqa: BLE001
            records.append((False, {"raw": raw, "error": str(exc)}))
    return records
