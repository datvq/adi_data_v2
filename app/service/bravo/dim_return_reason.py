# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_return_reason.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_return_reason: pl.DataFrame

  # transform
  query_return_reason = """
  select
    return_reason.IsActive is_active,
    return_reason.Code return_reason_code,
    return_reason.Name return_reason_name,
    dateadd(hour, 7, return_reason.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (return_reason.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20ReasonReceipt return_reason
  where return_reason.IsGroup = 0
  """
  df_dim_return_reason = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_return_reason, params=None)

  return df_dim_return_reason
