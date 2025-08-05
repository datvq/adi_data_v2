# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_process.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_process: pl.DataFrame

  # transform
  query_dim_process = """
  select
    process.IsActive is_active,
    process.Code process_code,
    process.Name process_name,
    dateadd(hour, 7, process.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (process.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20WorkProcess process
  where process.IsGroup = 0
  """
  df_dim_process = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_process, params=None)

  return df_dim_process
