# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "flat_expense_incurred.parquet")

def df_fact_expense_incurred_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "fact_expense_incurred.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_date_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_date.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_expense_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_expense.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_branch_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_branch.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_department_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_department.parquet")
  return dx.df.read_data_file(data_file)

def execute(df_fact_expense_incurred: pl.DataFrame, df_dim_date: pl.DataFrame, df_dim_expense: pl.DataFrame, df_dim_branch: pl.DataFrame, df_dim_department: pl.DataFrame) -> pl.DataFrame:
  # return
  df_flat_expense_incurred: pl.DataFrame

  # context
  ctx_dwh: pl.SQLContext = pl.SQLContext()
  
  ctx_dwh.register("fact_expense_incurred", df_fact_expense_incurred)
  ctx_dwh.register("dim_date", df_dim_date)
  ctx_dwh.register("dim_expense", df_dim_expense)
  ctx_dwh.register("dim_branch", df_dim_branch)
  ctx_dwh.register("dim_department", df_dim_department)
  
  ctx_dwh.tables()

  # transform
  query_flat_expense_incurred = """
  select
    ei.incurred_type incurred_type,
    ei.doc_date doc_date,
  
    dim_date.fiscal_year fiscal_year,
    dim_date.fiscal_year_index fiscal_year_index,
    dim_date.fiscal_quarter fiscal_quarter,
    dim_date.fiscal_quarter_index fiscal_quarter_index,
    dim_date.fiscal_month fiscal_month,
    dim_date.fiscal_month_index fiscal_month_index,
    dim_date.year year,
    dim_date.year_index year_index,
    dim_date.quarter quarter,
    dim_date.quarter_index quarter_index,
    dim_date.month month,
    dim_date.month_index month_index,
    dim_date.day day,
    dim_date.day_index day_index,
    dim_date.week week,
    dim_date.week_index week_index,
    dim_date.weekday weekday,
    dim_date.weekday_index weekday_index,
    dim_date.yyqq yyqq,
    dim_date.yymm yymm,
  
    ei.doc_code doc_code,
    ei.doc_no doc_no,
    ei.description description,
    
    ei.root_debit_account_code root_debit_account_code,
    ei.group_debit_account_code group_debit_account_code,
    ei.debit_account_code debit_account_code,
    ei.root_credit_account_code root_credit_account_code,
    ei.group_credit_account_code group_credit_account_code,
    ei.credit_account_code credit_account_code,
    
    ei.expense_code expense_code,
    e.expense_name expense_name,
    e.group_expense_code group_expense_code,
    e.group_expense_name group_expense_name,
    e.root_expense_code root_expense_code,
    e.root_expense_name root_expense_name,
    
    ei.branch_code branch_code,
    b.branch_name branch_name,
    ei.department_code department_code,
    d.department_name department_name,
  
    ei.amount amount,
  
  from fact_expense_incurred ei
    left join dim_date dim_date on sales_data.doc_date = dim_date.date
    left join dim_expense e on ei.expense_code = e.expense_code
    left join dim_branch b on ei.branch_code = b.branch_code
    left join dim_department d on ei.department_code = d.department_code
  """
  df_flat_expense_incurred = ctx_dwh.execute(query=query_flat_expense_incurred, eager=True)

  return df_flat_expense_incurred
