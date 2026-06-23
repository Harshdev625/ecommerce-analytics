"""Generate 30 days of simulated daily transaction CSV files for Airflow demos."""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path


def generate_files(output_dir: Path, days: int = 30, seed: int = 42) -> list[Path]:
    random.seed(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    start = date(2024, 1, 1)

    for offset in range(days):
        day = start + timedelta(days=offset)
        path = output_dir / f"transactions_{day.isoformat()}.csv"
        rows = []
        for i in range(random.randint(5, 15)):
            rows.append(
                {
                    "order_id": f"SIM-{day.strftime('%Y%m%d')}-{i:03d}",
                    "transaction_date": day.isoformat(),
                    "amount": round(random.uniform(10.0, 500.0), 2),
                    "status": random.choice(["delivered", "shipped", "processing"]),
                }
            )
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["order_id", "transaction_date", "amount", "status"],
            )
            writer.writeheader()
            writer.writerows(rows)
        paths.append(path)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "airflow" / "data" / "daily_transactions",
    )
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    paths = generate_files(args.output, days=args.days)
    print(f"Wrote {len(paths)} files to {args.output}")


if __name__ == "__main__":
    main()
