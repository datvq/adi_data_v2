# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "flat_sales_data.parquet")

def df_fact_sales_actual_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "fact_sales_actual.parquet")
  return dx.df.read_data_file(data_file)

def df_fact_sales_plan_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "fact_sales_plan.parquet")
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

def df_dim_item_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_item.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_employee_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_employee.parquet")
  return dx.df.read_data_file(data_file)

def execute(df_fact_sales_actual: pl.DataFrame, df_fact_sales_plan: pl.DataFrame, df_dim_date: pl.DataFrame, df_dim_branch: pl.DataFrame, df_dim_manage: pl.DataFrame, df_dim_customer: pl.DataFrame, df_dim_category: pl.DataFrame, df_dim_item: pl.DataFrame, df_dim_employee: pl.DataFrame) -> pl.DataFrame:
  # return
  df_flat_sales_data: pl.DataFrame

  # context
  ctx_dwh: pl.SQLContext = pl.SQLContext()
  
  ctx_dwh.register("fact_sales_actual", df_fact_sales_actual)
  ctx_dwh.register("fact_sales_plan", df_fact_sales_plan)
  ctx_dwh.register("dim_date", df_dim_date)
  ctx_dwh.register("dim_branch", df_dim_branch)
  ctx_dwh.register("dim_manage", df_dim_manage)
  ctx_dwh.register("dim_customer", df_dim_customer)
  ctx_dwh.register("dim_category", df_dim_category)
  ctx_dwh.register("dim_item", df_dim_item)
  ctx_dwh.register("dim_employee", df_dim_employee)
  
  ctx_dwh.tables()

  # transform
  query_flat_sales_data = """
  with sales_data as ((
    select
      'actual' data_source,
      fact_sales_actual.doc_date doc_date,
      fact_sales_actual.doc_code doc_code,
      fact_sales_actual.doc_no doc_no,
      fact_sales_actual.doc_branch_code doc_branch_code,
      fact_sales_actual.manage_code manage_code,
      fact_sales_actual.customer_code customer_code,
      fact_sales_actual.category_code category_code,
      fact_sales_actual.item_code item_code,
      fact_sales_actual.quantity quantity,
      fact_sales_actual.net_price net_price,
      fact_sales_actual.net_amount net_amount,
      fact_sales_actual.assumed_unit_cost unit_cost,
      (fact_sales_actual.quantity * fact_sales_actual.assumed_unit_cost) cost_amount,
      round(fact_sales_actual.net_price - fact_sales_actual.assumed_unit_cost, 0) gross_profit_margin,
      (fact_sales_actual.quantity * round(fact_sales_actual.net_price - fact_sales_actual.assumed_unit_cost, 0)) gross_profit,
      fact_sales_actual.due_days due_days,
      fact_sales_actual.lot_code lot_code,
      fact_sales_actual.item_lot_code item_lot_code,
      fact_sales_actual.is_gift_item is_gift_item,
      fact_sales_actual.is_direct_payment is_direct_payment,
      fact_sales_actual.doc_code_promotion doc_code_promotion,
      fact_sales_actual.doc_no_promotion_applied doc_no_promotion_applied,
      fact_sales_actual.plate_number plate_number,
      fact_sales_actual.driver_name driver_name,
      fact_sales_actual.order_employee_code order_employee_code,
    from fact_sales_actual fact_sales_actual
  ) union all (
    select
      'plan_sm' data_source,
      fact_sales_plan.from_date doc_date,
      null doc_code,
      null doc_no,
      fact_sales_plan.saleman_branch_code doc_branch_code,
      fact_sales_plan.manage_code manage_code,
      fact_sales_plan.customer_code customer_code,
      fact_sales_plan.category_code category_code,
      fact_sales_plan.item_code item_code,
      fact_sales_plan.quantity quantity,
      fact_sales_plan.net_price net_price,
      fact_sales_plan.net_amount net_amount,
      round(fact_sales_plan.gross_profit_margin - fact_sales_plan.net_price, 0) unit_cost,
      (fact_sales_plan.quantity * round(fact_sales_plan.gross_profit_margin - fact_sales_plan.net_price, 0)) cost_amount,
      fact_sales_plan.gross_profit_margin gross_profit_margin,
      fact_sales_plan.gross_profit gross_profit,
      null due_days,
      null lot_code,
      null item_lot_code,
      null is_gift_item,
      null is_direct_payment,
      null doc_code_promotion,
      null doc_no_promotion_applied,
      null plate_number,
      null driver_name,
      null order_employee_code,
    from fact_sales_plan fact_sales_plan
  ))
  """
  query_flat_sales_data += """
  select
    sales_data.data_source data_source,
  
    sales_data.doc_date doc_date,
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
  
    sales_data.doc_code doc_code,
    sales_data.doc_no doc_no,
  
    sales_data.doc_branch_code doc_branch_code,
    dim_branch.branch_name doc_branch_name,
  
    sales_data.manage_code manage_code,
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
  
    sales_data.customer_code customer_code,
    dim_customer.customer_name customer_name,
    dim_customer.latitude latitude,
    dim_customer.longitude longitude,
    dim_customer.province_name province_name,
    dim_customer.district_name district_name,
    dim_customer.commune_name commune_name,
    dim_customer.customer_branch_code customer_branch_code,
    dim_customer.customer_branch_name customer_branch_name,
  
    sales_data.category_code category_code,
    dim_category.category_name category_name,
  
    sales_data.item_code item_code,
    dim_item.item_name item_name,
    dim_item.parent_code parent_code,
    dim_item.parent_name parent_name,
    dim_item.size_code size_code,
    dim_item.size_name size_name,
    dim_item.product_code product_code,
    dim_item.product_name product_name,
    dim_item.category_code product_category_code,
    dim_item.category_name product_category_name,
    dim_item.unit unit,
    dim_item.net_weight net_weight,
    dim_item.bag_botle_weight bag_botle_weight,
    dim_item.units_per_package units_per_package,
    dim_item.package_type package_type,
    dim_item.item_type item_type,
  
    sales_data.quantity quantity,
    sales_data.net_price net_price,
    sales_data.net_amount net_amount,
    sales_data.unit_cost unit_cost,
    sales_data.cost_amount cost_amount,
    sales_data.gross_profit_margin gross_profit_margin,
    sales_data.gross_profit gross_profit,
    sales_data.due_days due_days,
    sales_data.doc_date + interval '1 day' * sales_data.due_days due_date,
    sales_data.lot_code lot_code,
    sales_data.item_lot_code item_lot_code,
    sales_data.is_gift_item is_gift_item,
    sales_data.is_direct_payment is_direct_payment,
    sales_data.doc_code_promotion doc_code_promotion,
    sales_data.doc_no_promotion_applied doc_no_promotion_applied,
    sales_data.plate_number plate_number,
    sales_data.driver_name driver_name,
    sales_data.order_employee_code order_employee_code,
  
    dim_employee_order.employee_name order_employee_name,
  
    (sales_data.quantity * dim_item.net_weight) weight,
    (sales_data.quantity / nullif(dim_item.units_per_package, 0)) packages,
  
  from sales_data sales_data
    left join dim_date dim_date on sales_data.doc_date = dim_date.date
    left join dim_manage dim_manage on sales_data.manage_code = dim_manage.manage_code
    left join dim_branch dim_branch on sales_data.doc_branch_code = dim_branch.branch_code
    left join dim_customer dim_customer on sales_data.customer_code = dim_customer.customer_code
    left join dim_category dim_category on sales_data.category_code = dim_category.category_code
    left join dim_item dim_item on sales_data.item_code = dim_item.item_code
    left join dim_employee dim_employee_order on sales_data.order_employee_code = dim_employee_order.employee_code
  """
  df_flat_sales_data = ctx_dwh.execute(query=query_flat_sales_data, eager=True)
  df_flat_sales_data = dx.dt.add_ytd_column(df=df_flat_sales_data, date_column="doc_date", ffmonth=10)

  return df_flat_sales_data
