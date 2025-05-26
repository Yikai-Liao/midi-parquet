import polars as pl
import symusic as sm
from tqdm import tqdm
import faulthandler

if __name__ == "__main__":
    # Load the dataset
    faulthandler.enable()
    start = 412238
    end = 412239

    df = pl.read_parquet("/dev/shm/midiset-1M5.parquet")[start:end]
    for d in tqdm(df.iter_rows(named=True), total=df.height):
        try:
            sm.Score.from_midi(d["content"], strict_mode=False)
        except Exception as e:
            print(f"Error processing MIDI content: {e}")
            continue