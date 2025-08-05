# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_category.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_category: pl.DataFrame

  # transform
  query_dim_category = """
  select
    category.IsActive is_active,
    category.Code category_code,
    category.Name category_name,
    dateadd(hour, 7, category.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (category.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20ItemCatg category
  where category.IsGroup = 0
  """
  df_dim_category = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_category, params=None)

  return df_dim_category
