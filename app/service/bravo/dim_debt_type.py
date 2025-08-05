# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_debt_type.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_debt_type: pl.DataFrame

  # generate
  df_dim_debt_type = pl.DataFrame({
    "debt_type_code": ["D00", "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D09", "D10"],
    "debt_type_name": [
      "00:Lệch",
      "01:Dư",
      "02:ĐH >30",
      "03:ĐH 11-30",
      "04:ĐH 00-10",
      "05:QH 01-30",
      "06:QH 31-90",
      "07:QH 91-6t",
      "08:QH 6t-1n",
      "09:QH 1n-2n",
      "10:QH >2n",
    ],
    "debt_group_code": ["G00", "G01", "G02", "G02", "G02", "G03", "G03", "G04", "G04", "G05", "G06"],
    "debt_group_name": [
      "00:Lệch",
      "01:Dư",
      "02.Chưa ĐH",
      "02.Chưa ĐH",
      "02.Chưa ĐH",
      "03:QH 01-90",
      "03:QH 01-90",
      "04:QH 90-1n",
      "04:QH 90-1n",
      "05:QH 1n-2n",
      "06:QH >2n",
    ],
  })

  return df_dim_debt_type
