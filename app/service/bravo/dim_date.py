# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_date.parquet")

def from_date_default() -> str:
  return "2021-10-01"

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("end_of_next_fiscal_year", ffmonth=10)

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_dim_date: pl.DataFrame

  # generate
  df_dim_date = dx.dt.gen_dim_date(from_date=from_date, to_date=to_date, ffmonth=10)

  return df_dim_date
