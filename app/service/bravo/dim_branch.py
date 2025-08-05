# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_branch.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_branch: pl.DataFrame

  # transform
  query_dim_branch = """
  select
    branch.IsActive is_active,
    branch.AreaCode branch_code,
    branch.AreaName2 branch_name,
    dateadd(hour, 7, branch.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (branch.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20Area branch
  where branch.IsGroup = 0
  """
  df_dim_branch = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_branch, params=None)

  return df_dim_branch
