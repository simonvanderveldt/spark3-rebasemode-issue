from datetime import date, datetime
from pathlib import Path

import pandas as pd

output_path = Path("./output/pandas")
output_path.mkdir(exist_ok=True)

date_data = {
    "row": [1, 2],
    "date": pd.to_datetime([
        date(day=1, month=10, year=1880),
        date(day=1, month=10, year=2020),
    ])
}

date_df = pd.DataFrame(date_data)
print(date_df)
print(date_df.dtypes)
date_df.to_parquet("output/pandas/date.parquet", compression=None)


timestamp_data = {
    "row": [1, 2],
    "date": pd.to_datetime([
        datetime(day=1, month=10, year=1880, hour=10, minute=10, second=10),
        datetime(day=1, month=10, year=2020, hour=10, minute=10, second=10),
    ])
}

timestamp_df = pd.DataFrame(timestamp_data)
print(timestamp_df)
print(timestamp_df.dtypes)
timestamp_df.to_parquet("output/pandas/datetime.parquet", compression=None)
