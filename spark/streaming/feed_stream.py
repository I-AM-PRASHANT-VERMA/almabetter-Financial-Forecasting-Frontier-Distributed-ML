from __future__ import annotations

import shutil
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_BATCHES = PROJECT_ROOT / "data" / "stream_chunks" / "source_batches"
LIVE_INPUT = PROJECT_ROOT / "data" / "stream_chunks" / "input"


def main(delay_seconds: int = 3) -> None:
    LIVE_INPUT.mkdir(parents=True, exist_ok=True)
    batch_files = sorted(SOURCE_BATCHES.glob("batch_*.csv"))

    for batch_file in batch_files:
        destination = LIVE_INPUT / batch_file.name
        # Copying one batch at a time gives Structured Streaming something that feels like live arrival.
        shutil.copy2(batch_file, destination)
        print(f"Copied {batch_file.name} into the live input folder")
        time.sleep(delay_seconds)


if __name__ == "__main__":
    main()
