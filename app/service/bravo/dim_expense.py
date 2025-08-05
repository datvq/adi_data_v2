# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_expense.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_expense: pl.DataFrame

  # transform
  query_dim_expense = """
  select
    expense.IsActive is_active,
    expense.Id expense_id,
    expense.Code expense_code,
    expense.Name expense_name,
    egroup.Code group_expense_code,
    egroup.Name group_expense_name,
    eroot.Code root_expense_code,
    eroot.Name root_expense_name,
    dateadd(hour, 7, expense.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (egroup.ModifiedAt),
      (eroot.ModifiedAt),
      (expense.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20ExpenseCatg expense
    left join B20ExpenseCatg egroup on egroup.Id = expense.ParentId
    left join B20ExpenseCatg eroot on eroot.Id = egroup.ParentId
  where expense.IsGroup = 0
  """
  df_dim_expense = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_expense, params=None)

  return df_dim_expense
