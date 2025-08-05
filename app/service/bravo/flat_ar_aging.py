# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "flat_ar_aging.parquet")

def df_fact_ar_aging_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "fact_ar_aging.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_date_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_date.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_branch_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_branch.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_manage_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_manage.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_customer_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_customer.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_category_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_category.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_debt_type_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_debt_type.parquet")
  return dx.df.read_data_file(data_file)

def execute(df_fact_ar_aging: pl.DataFrame, df_dim_date: pl.DataFrame, df_dim_branch: pl.DataFrame, df_dim_manage: pl.DataFrame, df_dim_customer: pl.DataFrame, df_dim_category: pl.DataFrame, df_dim_debt_type: pl.DataFrame) -> pl.DataFrame:
  # return
  df_flat_ar_aging: pl.DataFrame

  # context
  ctx_dwh: pl.SQLContext = pl.SQLContext()
  
  ctx_dwh.register("fact_ar_aging", df_fact_ar_aging)
  ctx_dwh.register("dim_date", df_dim_date)
  ctx_dwh.register("dim_branch", df_dim_branch)
  ctx_dwh.register("dim_manage", df_dim_manage)
  ctx_dwh.register("dim_customer", df_dim_customer)
  ctx_dwh.register("dim_category", df_dim_category)
  ctx_dwh.register("dim_debt_type", df_dim_debt_type)
  
  ctx_dwh.tables()

  # transform
  query_flat_ar_aging = """
  select
    fact_ar_aging.view_date view_date,
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
  
    fact_ar_aging.doc_date doc_date,
    fact_ar_aging.doc_code doc_code,
    fact_ar_aging.doc_no doc_no,
  
    fact_ar_aging.due_days due_days,
    fact_ar_aging.due_date due_date,
  
    fact_ar_aging.doc_branch_code doc_branch_code,
    dim_branch.branch_name doc_branch_name,
  
    fact_ar_aging.manage_code manage_code,
    dim_manage.saleman_code saleman_code,
    dim_manage.saleman_name saleman_name,
    dim_manage.saleman_department_code saleman_department_code,
    dim_manage.saleman_department_name saleman_department_name,
    dim_manage.superior_code superior_code,
    dim_manage.superior_name superior_name,
    dim_manage.superior_department_code superior_department_code,
    dim_manage.superior_department_name superior_department_name,
    dim_manage.saleman_branch_code saleman_branch_code,
    dim_manage.saleman_branch_name saleman_branch_name,
  
    fact_ar_aging.customer_code customer_code,
    dim_customer.customer_name customer_name,
    dim_customer.latitude latitude,
    dim_customer.longitude longitude,
    dim_customer.province_name province_name,
    dim_customer.district_name district_name,
    dim_customer.commune_name commune_name,
    dim_customer.customer_branch_code customer_branch_code,
    dim_customer.customer_branch_name customer_branch_name,
  
    fact_ar_aging.category_code category_code,
    dim_category.category_name category_name,
  
    fact_ar_aging.is_direct_payment is_direct_payment,
    fact_ar_aging.promotion_type promotion_type,
    fact_ar_aging.promotion_applied promotion_applied,
  
    fact_ar_aging.invoice_amount invoice_amount,
    fact_ar_aging.paid_amount paid_amount,
    fact_ar_aging.remain_amount remain_amount,
    fact_ar_aging.over_due_days over_due_days,
  
    fact_ar_aging.debt_type_code debt_type_code,
    dim_debt_type.debt_type_name debt_type_name,
    dim_debt_type.debt_group_code debt_group_code,
    dim_debt_type.debt_group_name debt_group_name,
  
  from fact_ar_aging fact_ar_aging
    left join dim_date dim_date on fact_ar_aging.view_date = dim_date.date
    left join dim_branch dim_branch on fact_ar_aging.doc_branch_code = dim_branch.branch_code
    left join dim_manage dim_manage on fact_ar_aging.manage_code = dim_manage.manage_code
    left join dim_customer dim_customer on fact_ar_aging.customer_code = dim_customer.customer_code
    left join dim_category dim_category on fact_ar_aging.category_code = dim_category.category_code
    left join dim_debt_type dim_debt_type on fact_ar_aging.debt_type_code = dim_debt_type.debt_type_code
  """
  df_flat_ar_aging = ctx_dwh.execute(query=query_flat_ar_aging, eager=True)

  return df_flat_ar_aging
