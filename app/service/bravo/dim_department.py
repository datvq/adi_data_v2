# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_department.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_department: pl.DataFrame

  # transform
  query_dim_department = """
  select
    department.IsActive is_active,
    department.Id department_id,
    department.Code department_code,
    department.Name department_name,
    dateadd(hour, 7, department.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (department.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20Dept department
  where department.IsGroup = 0
  """
  df_dim_department = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_department, params=None)

  return df_dim_department
