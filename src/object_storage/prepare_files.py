"""Prepare part 3 upload files from data/object_storage/ (run locally, stdlib only).

1. Rewrites image_metadata.csv so claim_no / chassis_no match part 2's source data:
   row n -> claim CLM%08d(n); chassis = CHS%06d of that claim's policy, POL = (n * 7919) % 12000 + 1.
2. Writes two one-row CSVs for the schema-evolution demo (new_column_1, then new_column_2).
The original CSV is not modified.
"""

import csv
from pathlib import Path

DATA = Path(__file__).resolve().parents[2] / "data" / "object_storage"
SOURCE_CSV = DATA / "claims" / "metadata" / "image_metadata.csv"
PREPARED = DATA / "prepared"

N_CLAIMS = 13_000  # part 2: claims CLM00000001..CLM00013000
N_POLICIES = 12_000  # part 2: policies POL0000001..POL0012000
COLUMNS = ["image_name", "image_id", "claim_no", "chassis_no"]


def policy_number(claim_n: int) -> int:
    """Same formula as src/source_database/seed.sql: which policy claim n belongs to."""
    return (claim_n * 7919) % N_POLICIES + 1


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>6} rows -> {path.relative_to(DATA.parent.parent)}")


def main() -> None:
    with SOURCE_CSV.open(newline="") as f:
        original = list(csv.DictReader(f))

    rows = [
        {
            "image_name": row["image_name"],
            "image_id": row["image_id"],
            "claim_no": f"CLM{n:08d}",
            "chassis_no": f"CHS{policy_number(n):06d}",
        }
        for n, row in enumerate(original[:N_CLAIMS], start=1)
    ]
    write_csv(PREPARED / "claims_metadata" / "image_metadata.csv", COLUMNS, rows)

    # Schema-evolution demo: same shape as the real rows, plus extra columns.
    write_csv(
        PREPARED / "schema_evolution" / "image_metadata_new_column_1.csv",
        COLUMNS + ["new_column_1"],
        [{**rows[0], "new_column_1": "new column value 1"}],
    )
    write_csv(
        PREPARED / "schema_evolution" / "image_metadata_new_column_2.csv",
        COLUMNS + ["new_column_1", "new_column_2"],
        [{**rows[1], "new_column_1": "new column value 1", "new_column_2": "new column value 2"}],
    )


if __name__ == "__main__":
    main()
