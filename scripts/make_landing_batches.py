"""Split the raw survey CSV into two landing batches to simulate data arriving over time."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "raw" / "online_food_delivery.csv"
LANDING = ROOT / "data" / "raw" / "landing"
BATCH_1_ROWS = 200


def main() -> None:
    lines = SOURCE.read_bytes().splitlines(keepends=True)
    header, rows = lines[0], lines[1:]

    LANDING.mkdir(parents=True, exist_ok=True)
    batches = {
        "batch_01.csv": rows[:BATCH_1_ROWS],
        "batch_02.csv": rows[BATCH_1_ROWS:],
    }
    for name, batch_rows in batches.items():
        (LANDING / name).write_bytes(header + b"".join(batch_rows))
        print(f"{name}: {len(batch_rows)} rows")


if __name__ == "__main__":
    main()
