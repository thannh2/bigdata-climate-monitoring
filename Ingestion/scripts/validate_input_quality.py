from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.quality_rules import evaluate_record_expectations, get_expectation_suite


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate normalized ingestion records against GE-aligned quality rules.")
    parser.add_argument("--entity", choices=("weather", "air_quality"), required=True)
    parser.add_argument("--input", required=True, help="Path to a JSON object or JSONL file.")
    return parser.parse_args()


def load_records(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    parsed = json.loads(text)
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        return [parsed]
    raise ValueError("Input must be a JSON object, JSON array, or JSONL file")


def main() -> None:
    args = parse_args()
    records = load_records(Path(args.input))
    suite = get_expectation_suite(args.entity)

    failures: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        errors = evaluate_record_expectations(record, suite)
        if errors:
            failures.append({"index": index, "errors": errors, "record": record})

    report = {
        "entity": args.entity,
        "suite_name": f"{args.entity}_input_quality_suite",
        "record_count": len(records),
        "success_count": len(records) - len(failures),
        "failure_count": len(failures),
        "success": not failures,
        "failures": failures,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
