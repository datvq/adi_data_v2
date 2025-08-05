# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_project.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_project: pl.DataFrame

  # transform
  query_dim_project = """
  select
    project.IsActive is_active,
    project.Code project_code,
    project.Name project_name,
    dateadd(hour, 7, project.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (project.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20Product project
  where project.IsGroup = 0
  """
  df_dim_project = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_project, params=None)

  return df_dim_project
