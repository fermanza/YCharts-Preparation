"""Deterministic fictional prices; weekday calendar is not an exchange calendar."""

import csv
import math
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HEADER = ["symbol", "date", "close", "adjusted_close", "volume", "provider_revision"]


def write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(HEADER)
        writer.writerows(rows)


def main():
    day, end, index = date(2026, 9, 1), date(2026, 10, 9), 0
    while day <= end:
        if day.weekday() < 5:
            rows = []
            for offset, symbol in enumerate(("ALFA", "BETA", "GAMMA")):
                value = Decimal(
                    str(80 + offset * 40 + index * 0.4 + math.sin(index / 3 + offset) * 2)
                )
                price = value.quantize(Decimal("0.000001"))
                rows.append(
                    [symbol, str(day), str(price), str(price), 100000 + index * 1000 + offset, 1]
                )
            write(ROOT / "data/provider" / f"{day}.csv", rows)
            if day == end:
                correction = [row.copy() for row in rows]
                correction[0][2:4] = ["110.000000", "110.000000"]
                correction[0][5] = 2
                write(ROOT / "data/scenarios/correction.csv", correction)
                invalid = [row.copy() for row in rows]
                invalid[1][3] = "-5.000000"
                write(ROOT / "data/scenarios/invalid.csv", invalid)
                conflict = [row.copy() for row in rows]
                conflict[0][2:4] = ["999.000000", "999.000000"]
                write(ROOT / "data/scenarios/conflict.csv", conflict)
            index += 1
        day += timedelta(days=1)
    print(f"Created {index} daily partitions for three synthetic securities.")


if __name__ == "__main__":
    main()
