from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "bank.csv"
STREAM_ROOT = PROJECT_ROOT / "data" / "stream_chunks"
SOURCE_BATCHES = STREAM_ROOT / "source_batches"
LIVE_INPUT = STREAM_ROOT / "input"


def main(chunk_size: int = 150) -> None:
    SOURCE_BATCHES.mkdir(parents=True, exist_ok=True)
    LIVE_INPUT.mkdir(parents=True, exist_ok=True)

    # Old chunks are cleared so each streaming demo starts from a clean folder.
    for file_path in SOURCE_BATCHES.glob("*.csv"):
        file_path.unlink()
    for file_path in LIVE_INPUT.glob("*.csv"):
        file_path.unlink()

    df = pd.read_csv(RAW_DATA_PATH).copy()

    # A few rows are nudged backwards in time so the watermark logic has something real to manage.
    base_time = datetime(2026, 4, 22, 10, 0, 0)
    event_times = []
    for row_index in range(len(df)):
        event_time = base_time + timedelta(seconds=row_index * 4)
        if row_index % 37 == 0:
            event_time -= timedelta(minutes=3)
        event_times.append(event_time)
    df["event_time"] = event_times

    total_batches = math.ceil(len(df) / chunk_size)
    for batch_number in range(total_batches):
        start = batch_number * chunk_size
        end = start + chunk_size
        batch_df = df.iloc[start:end].copy()
        # Small numbered files make it easy to simulate a steady stream into the input folder.
        batch_path = SOURCE_BATCHES / f"batch_{batch_number + 1:03d}.csv"
        batch_df.to_csv(batch_path, index=False)

    print(f"Created {total_batches} streaming batch files in {SOURCE_BATCHES}")


if __name__ == "__main__":
    main()
