import pandas as pd
import sys
import numpy

"""
Verify that there are no missing days in the output dataframe.

Example:
 python3 utils/show_missing_days.py \
   data_output/scratch/schedule_vs_realtime_all_day_types_routes_2022-05-20_to_2024-03-28.json
"""

def print_missing_days(datecol):
    print(f'Range: {datecol.iloc[0].date()} to {datecol.iloc[-1].date()}')
    prev = None
    for x in datecol.unique():
        if prev:
            dt = pd.Timedelta(x - prev)
            if dt != pd.Timedelta(1, 'D'):
                print(f' Missing day(s) between {prev.date()}, {x.date()}')
        prev = x

if __name__ == "__main__":
    filename = sys.argv[1]
    date_column_name = 'date'
    if len(sys.argv) > 1:
        date_column_name = sys.argv[2]
    df = pd.read_json(filename)
    print_missing_days(df[date_column_name])
