# import
import os
import timeit
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

import app.service.bravo.dim_branch as dim_branch
import app.service.bravo.dim_category as dim_category
import app.service.bravo.dim_customer as dim_customer
import app.service.bravo.dim_date as dim_date
import app.service.bravo.dim_debt_type as dim_debt_type
import app.service.bravo.dim_employee as dim_employee
import app.service.bravo.dim_item as dim_item
import app.service.bravo.dim_lot as dim_lot
import app.service.bravo.dim_manage as dim_manage
import app.service.bravo.dim_return_reason as dim_return_reason
import app.service.bravo.dim_warehouse as dim_warehouse
import app.service.bravo.fact_sales_actual as fact_sales_actual
import app.service.bravo.fact_sales_plan as fact_sales_plan
import app.service.bravo.fact_ar_object as fact_ar_object
import app.service.bravo.fact_ar_aging as fact_ar_aging
import app.service.bravo.fact_inventory_item as fact_inventory_item
import app.service.bravo.fact_inventory_lot as fact_inventory_lot
import app.service.bravo.flat_sales_data as flat_sales_data
import app.service.bravo.flat_ar_aging as flat_ar_aging
import app.service.bravo.flat_inventory_item as flat_inventory_item
import app.service.bravo.flat_inventory_lot as flat_inventory_lot

dx.reload(
  shared,
  dim_branch,
  dim_category,
  dim_customer,
  dim_date,
  dim_debt_type,
  dim_employee,
  dim_item,
  dim_lot,
  dim_manage,
  dim_return_reason,
  dim_warehouse,
  fact_sales_actual,
  fact_sales_plan,
  fact_ar_object,
  fact_ar_aging,
  fact_inventory_item,
  fact_inventory_lot,
  flat_sales_data,
  flat_ar_aging,
  flat_ar_aging,
  flat_inventory_item,
  flat_inventory_lot,
)

# default
def output_dir_default() -> str:
  return os.path.join(shared.env.data_dir, "warehouse", "model_sales")

def execute(output_dir) -> None:
  # get data
  
  start_all = timeit.default_timer()
  
  # output
  
  dim_branch_file = dim_branch.output_file_default()
  dim_category_file = dim_category.output_file_default()
  dim_customer_file = dim_customer.output_file_default()
  dim_date_file = dim_date.output_file_default()
  dim_debt_type_file = dim_debt_type.output_file_default()
  dim_employee_file = dim_employee.output_file_default()
  dim_item_file = dim_item.output_file_default()
  dim_lot_file = dim_lot.output_file_default()
  dim_manage_file = dim_manage.output_file_default()
  dim_return_reason_file = dim_return_reason.output_file_default()
  dim_warehouse_file = dim_warehouse.output_file_default()
  
  fact_sales_actual_file = fact_sales_actual.output_file_default()
  fact_sales_plan_file = fact_sales_plan.output_file_default()
  fact_ar_object_file = fact_ar_object.output_file_default()
  fact_ar_aging_file = fact_ar_aging.output_file_default()
  fact_inventory_item_file = fact_inventory_item.output_file_default()
  fact_inventory_lot_file = fact_inventory_lot.output_file_default()
  
  flat_sales_data_file = flat_sales_data.output_file_default()
  flat_ar_aging_file = flat_ar_aging.output_file_default()
  flat_inventory_item_file = flat_inventory_item.output_file_default()
  flat_inventory_lot_file = flat_inventory_lot.output_file_default()
  
  # dim data
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_branch_file, timedelta(seconds=60)):
    df_dim_branch = dim_branch.execute()
    dx.df.write_data_file(df=df_dim_branch, data_file=dim_branch_file)
  print(f"""get data dim_branch in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_category_file, timedelta(seconds=60)):
    df_dim_category = dim_category.execute()
    dx.df.write_data_file(df=df_dim_category, data_file=dim_category_file)
  print(f"""get data dim_category in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_customer_file, timedelta(seconds=60)):
    df_dim_customer = dim_customer.execute(
      view_date=dim_customer.view_date_default(),
    )
    dx.df.write_data_file(df=df_dim_customer, data_file=dim_customer_file)
  print(f"""get data dim_customer in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_date_file, timedelta(seconds=60)):
    df_dim_date = dim_date.execute(
      from_date=dim_date.from_date_default(),
      to_date=dim_date.to_date_default(),
    )
    dx.df.write_data_file(df=df_dim_date, data_file=dim_date_file)
  print(f"""get data dim_date in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_debt_type_file, timedelta(seconds=60)):
    df_dim_debt_type = dim_debt_type.execute()
    dx.df.write_data_file(df=df_dim_debt_type, data_file=dim_debt_type_file)
  print(f"""get data dim_debt_type in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_employee_file, timedelta(seconds=60)):
    df_dim_employee = dim_employee.execute(
      view_date=dim_employee.view_date_default(),
    )
    dx.df.write_data_file(df=df_dim_employee, data_file=dim_employee_file)
  print(f"""get data dim_employee in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_item_file, timedelta(seconds=60)):
    df_dim_item = dim_item.execute()
    dx.df.write_data_file(df=df_dim_item, data_file=dim_item_file)
  print(f"""get data dim_item in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_lot_file, timedelta(seconds=60)):
    df_dim_lot = dim_lot.execute()
    dx.df.write_data_file(df=df_dim_lot, data_file=dim_lot_file)
  print(f"""get data dim_lot in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_manage_file, timedelta(seconds=60)):
    df_dim_manage = dim_manage.execute(
      view_date=dim_manage.view_date_default(),
    )
    dx.df.write_data_file(df=df_dim_manage, data_file=dim_manage_file)
  print(f"""get data dim_manage in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_return_reason_file, timedelta(seconds=60)):
    df_dim_return_reason = dim_return_reason.execute()
    dx.df.write_data_file(df=df_dim_return_reason, data_file=dim_return_reason_file)
  print(f"""get data dim_return_reason in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(dim_warehouse_file, timedelta(seconds=60)):
    df_dim_warehouse = dim_warehouse.execute()
    dx.df.write_data_file(df=df_dim_warehouse, data_file=dim_warehouse_file)
  print(f"""get data dim_warehouse in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  # fact data
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(fact_sales_actual_file, timedelta(seconds=60)):
    df_fact_sales_actual = fact_sales_actual.execute(
      from_date=fact_sales_actual.from_date_default(),
      to_date=fact_sales_actual.to_date_default(),
      stable_date=fact_sales_actual.stable_date_default(),
    )
    dx.df.write_data_file(df=df_fact_sales_actual, data_file=fact_sales_actual_file)
  print(f"""get data df_fact_sales_actual in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(fact_sales_plan_file, timedelta(days=1000)):
    df_fact_sales_plan = fact_sales_plan.execute(
      from_date=fact_sales_plan.from_date_default(),
      to_date=fact_sales_plan.to_date_default(),
    )
    dx.df.write_data_file(df=df_fact_sales_plan, data_file=fact_sales_plan_file)
  print(f"""get data fact_sales_plan in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(fact_ar_object_file, timedelta(seconds=60)):
    df_fact_ar_object = fact_ar_object.execute(
      view_date=fact_ar_object.view_date_default(),
    )
    dx.df.write_data_file(df=df_fact_ar_object, data_file=fact_ar_object_file)
  print(f"""get data fact_ar_object in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(fact_ar_aging_file, timedelta(seconds=60)):
    df_fact_ar_aging = fact_ar_aging.execute(
      view_date=fact_ar_aging.view_date_default(),
      df_fact_ar_object=df_fact_ar_object,
    )
    dx.df.write_data_file(df=df_fact_ar_aging, data_file=fact_ar_aging_file)
  print(f"""get data fact_ar_aging in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(fact_inventory_item_file, timedelta(seconds=60)):
    df_fact_inventory_item = fact_inventory_item.execute(
      view_date=fact_inventory_item.view_date_default(),
    )
    dx.df.write_data_file(df=df_fact_inventory_item, data_file=fact_inventory_item_file)
  print(f"""get data fact_inventory_item in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  if not dx.io.is_valid_cache(fact_inventory_lot_file, timedelta(seconds=60)):
    df_fact_inventory_lot = fact_inventory_lot.execute(
      view_date=fact_inventory_lot.view_date_default(),
      df_fact_inventory_item=df_fact_inventory_item,
    )
    dx.df.write_data_file(df=df_fact_inventory_lot, data_file=fact_inventory_lot_file)
  print(f"""get data fact_inventory_lot in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  # flat data
  
  start_step = timeit.default_timer()
  df_flat_sales_data = flat_sales_data.execute(
    df_fact_sales_actual=dx.df.read_data_file(fact_sales_actual_file),
    df_fact_sales_plan=dx.df.read_data_file(fact_sales_plan_file),
    df_dim_date=dx.df.read_data_file(dim_date_file),
    df_dim_branch=dx.df.read_data_file(dim_branch_file),
    df_dim_manage=dx.df.read_data_file(dim_manage_file),
    df_dim_customer=dx.df.read_data_file(dim_customer_file),
    df_dim_category=dx.df.read_data_file(dim_category_file),
    df_dim_item=dx.df.read_data_file(dim_item_file),
    df_dim_employee=dx.df.read_data_file(dim_employee_file),
  )
  dx.df.write_data_file(df=df_flat_sales_data, data_file=flat_sales_data_file)
  print(f"""get data flat_sales_data in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  df_flat_ar_aging = flat_ar_aging.execute(
    df_fact_ar_aging=dx.df.read_data_file(fact_ar_aging_file),
    df_dim_date=dx.df.read_data_file(dim_date_file),
    df_dim_branch=dx.df.read_data_file(dim_branch_file),
    df_dim_manage=dx.df.read_data_file(dim_manage_file),
    df_dim_customer=dx.df.read_data_file(dim_customer_file),
    df_dim_category=dx.df.read_data_file(dim_category_file),
    df_dim_debt_type=dx.df.read_data_file(dim_debt_type_file),
  )
  dx.df.write_data_file(df=df_flat_ar_aging, data_file=flat_ar_aging_file)
  print(f"""get data flat_ar_aging in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  df_flat_inventory_item = flat_inventory_item.execute(
    df_fact_inventory_item=dx.df.read_data_file(fact_inventory_item_file),
    df_dim_date=dx.df.read_data_file(dim_date_file),
    df_dim_warehouse=dx.df.read_data_file(dim_warehouse_file),
    df_dim_item=dx.df.read_data_file(dim_item_file),
  )
  dx.df.write_data_file(df=df_flat_inventory_item, data_file=flat_inventory_item_file)
  print(f"""get data flat_inventory_item in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  start_step = timeit.default_timer()
  df_flat_inventory_lot = flat_inventory_lot.execute(
    df_fact_inventory_lot=dx.df.read_data_file(fact_inventory_lot_file),
    df_dim_date=dx.df.read_data_file(dim_date_file),
    df_dim_warehouse=dx.df.read_data_file(dim_warehouse_file),
    df_dim_item=dx.df.read_data_file(dim_item_file),
    df_dim_lot=dx.df.read_data_file(dim_lot_file),
  )
  dx.df.write_data_file(df=df_flat_inventory_lot, data_file=flat_inventory_lot_file)
  print(f"""get data flat_inventory_lot in {round(timeit.default_timer() - start_step, 2)} seconds""")
  
  # copy
  
  dx.io.copy_file(dim_branch_file, os.path.join(output_dir, "dim_branch.parquet"))
  dx.io.copy_file(dim_category_file, os.path.join(output_dir, "dim_category.parquet"))
  dx.io.copy_file(dim_customer_file, os.path.join(output_dir, "dim_customer.parquet"))
  dx.io.copy_file(dim_date_file, os.path.join(output_dir, "dim_date.parquet"))
  dx.io.copy_file(dim_debt_type_file, os.path.join(output_dir, "dim_debt_type.parquet"))
  dx.io.copy_file(dim_employee_file, os.path.join(output_dir, "dim_employee.parquet"))
  dx.io.copy_file(dim_item_file, os.path.join(output_dir, "dim_item.parquet"))
  dx.io.copy_file(dim_lot_file, os.path.join(output_dir, "dim_lot.parquet"))
  dx.io.copy_file(dim_manage_file, os.path.join(output_dir, "dim_manage.parquet"))
  dx.io.copy_file(dim_return_reason_file, os.path.join(output_dir, "dim_return_reason.parquet"))
  dx.io.copy_file(dim_warehouse_file, os.path.join(output_dir, "dim_warehouse.parquet"))
  dx.io.copy_file(fact_ar_object_file, os.path.join(output_dir, "fact_ar_object.parquet"))
  dx.io.copy_file(fact_ar_aging_file, os.path.join(output_dir, "fact_ar_aging.parquet"))
  dx.io.copy_file(fact_inventory_item_file, os.path.join(output_dir, "fact_inventory_item.parquet"))
  dx.io.copy_file(fact_inventory_lot_file, os.path.join(output_dir, "fact_inventory_lot.parquet"))
  dx.io.copy_file(fact_sales_plan_file, os.path.join(output_dir, "fact_sales_plan.parquet"))
  dx.io.copy_file(fact_sales_actual_file, os.path.join(output_dir, "fact_sales_actual.parquet"))
  dx.io.copy_file(flat_ar_aging_file, os.path.join(output_dir, "flat_ar_aging.parquet"))
  dx.io.copy_file(flat_inventory_item_file, os.path.join(output_dir, "flat_inventory_item.parquet"))
  dx.io.copy_file(flat_inventory_lot_file, os.path.join(output_dir, "flat_inventory_lot.parquet"))
  dx.io.copy_file(flat_sales_data_file, os.path.join(output_dir, "flat_sales_data.parquet"))
  
  print(f"""get model sales in {round(timeit.default_timer() - start_all, 2)} seconds""")
