# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "flat_inventory_item.parquet")

def df_fact_inventory_item_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "fact_inventory_item.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_date_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_date.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_warehouse_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_warehouse.parquet")
  return dx.df.read_data_file(data_file)

def df_dim_item_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "dim_item.parquet")
  return dx.df.read_data_file(data_file)

def execute(df_fact_inventory_item: pl.DataFrame, df_dim_date: pl.DataFrame, df_dim_warehouse: pl.DataFrame, df_dim_item: pl.DataFrame) -> pl.DataFrame:
  # return
  df_flat_inventory_item: pl.DataFrame

  # context
  ctx_dwh: pl.SQLContext = pl.SQLContext()
  
  ctx_dwh.register("fact_inventory_item", df_fact_inventory_item)
  ctx_dwh.register("dim_date", df_dim_date)
  ctx_dwh.register("dim_item", df_dim_item)
  ctx_dwh.register("dim_warehouse", df_dim_warehouse)
  
  ctx_dwh.tables()

  # transform
  query_flat_inventory_item = """
  select
    flat_inventory_item.view_date view_date,
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
  
    flat_inventory_item.warehouse_code warehouse_code,
    dim_warehouse.warehouse_name warehouse_name,
    dim_warehouse.warehouse_branch_code warehouse_branch_code,
    dim_warehouse.warehouse_branch_name warehouse_branch_name,
    dim_warehouse.receive_warehouse_branch_code receive_warehouse_branch_code,
    dim_warehouse.receive_warehouse_branch_name receive_warehouse_branch_name,
  
    flat_inventory_item.item_code item_code,
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
  
    flat_inventory_item.unit_cost unit_cost,
    flat_inventory_item.quantity quantity,
    flat_inventory_item.amount amount,
  
    (flat_inventory_item.quantity * dim_item.net_weight) weight,
    (flat_inventory_item.quantity / nullif(units_per_package, 0)) packages,
  
  from fact_inventory_item flat_inventory_item
    left join dim_date dim_date on flat_inventory_item.view_date = dim_date.date
    left join dim_warehouse dim_warehouse on flat_inventory_item.warehouse_code = dim_warehouse.warehouse_code
    left join dim_item dim_item on flat_inventory_item.item_code = dim_branch.item_code
  """
  df_flat_inventory_item = ctx_dwh.execute(query=query_flat_inventory_item, eager=True)

  return df_flat_inventory_item
