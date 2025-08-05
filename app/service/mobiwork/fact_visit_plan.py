# import
import os
import calendar
import requests
import polars as pl
import lib.dx as dx
import lib.adi as adi
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(adi, shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "mobiwork", "fact_visit_plan.parquet")

def yymm_default() -> str:
  return dx.dt.datetime_to_string(dx.dt.string_to_datetime("end_of_last_month", f"%Y-%m-%d"), "%y%m")

def max_visit_default() -> int:
  return 4

def execute(yymm: str, max_visit: int) -> pl.DataFrame:
  # return
  pl_fact_visit_plan: pl.DataFrame

  # fact_visit_plan
  from_date = dx.dt.string_to_datetime(yymm, "%y%m")
  to_date = dx.dt.end_of_month(from_date)
  url = f"""{
    shared.env.mobiwork_visit_base_url
  }/VisitPlanReportDetails?orgid=57bea55c94dec72c0c5b4e0b&fromdate={
    dx.dt.datetime_to_string(from_date, f"%d/%m/%Y")
  }&todate={
    dx.dt.datetime_to_string(to_date, f"%d/%m/%Y")
  }&firstday={
    dx.dt.datetime_to_string(dx.dt.begin_of_month(from_date, -3), f"%d/%m/%Y")
  }&lastday={
    dx.dt.datetime_to_string(dx.dt.end_of_month(to_date, -1), f"%d/%m/%Y")
  }"""
  header = adi.mobiwork.create_mobiwork_header(
    email=shared.env.mobiwork_email,
    password=shared.env.mobiwork_password,
    base_url=shared.env.mobiwork_login_base_url,
  )
  res: requests.models.Response = requests.get(url=url, headers=header)
  data: dict = res.json()
  
  dict_employees: dict = data.get("objU")
  list_employees = [{"email": email, **info} for email, info in dict_employees.items()]
  df_employees_dms = pl.from_dicts(list_employees).drop(["doibh_ten"])
  df_employees_dms = df_employees_dms.rename({
    "email": "employee_email",
    "nv_ma": "employee_code",
    "nv_ten": "employee_name",
    "doibh_ma": "branch_code",
  })
  
  dict_customers: dict = data.get("objCus")
  list_customers = [{**info} for id, info in dict_customers.items()]
  df_customers_dms = pl.from_dicts(list_customers)
  df_customers_dms = df_customers_dms.rename({
    "_id": "customer_uuid",
    "ma_kh": "customer_code",
    "ten_kh": "customer_name",
  })
  
  list_data: list[dict] = data.get("result")
  pl_fact_visit_plan = pl.from_dicts(list_data)
  pl_fact_visit_plan = pl_fact_visit_plan.with_columns(
    pl.col("vt_cuoi").replace([""], None).str.strptime(pl.Date, f"%d/%m/%Y"),
  )
  pl_fact_visit_plan = pl_fact_visit_plan.rename({
    "so_luotkh": "visit_plan",
    "so_luotvt": "visit_actual",
    "vt_cuoi": "last_visit_date",
    "nam": "year",
    "thang": "month",
    "e": "employee_email",
    "c": "customer_uuid",
  })
  
  pl_fact_visit_plan = pl_fact_visit_plan. \
    join(
      df_customers_dms,
      on=["customer_uuid"],
      how="left",
      suffix="_",
      coalesce=True,
    ). \
    join(
      df_employees_dms,
      on=["employee_email"],
      how="left",
      suffix="_",
      coalesce=True,
    )
  
  pl_fact_visit_plan = pl_fact_visit_plan.with_columns(
    pl.lit(yymm).alias("yymm"),
    pl.min_horizontal(pl.max_horizontal(pl.col("visit_plan"), max_visit), pl.col("visit_actual")).alias("visit_actual_fixed"),
  )
  
  pl_fact_visit_plan = dx.df.rename_columns(pl_fact_visit_plan, selected_cols={
    "yymm": "yymm",
    "year": "year",
    "month": "month",
    "employee_code": "employee_code",
    "employee_name": "employee_name",
    "employee_email": "employee_email",
    "customer_uuid": "customer_uuid",
    "customer_code": "customer_code",
    "customer_name": "customer_name",
    "visit_plan": "visit_plan",
    "visit_actual": "visit_actual",
    "visit_actual_fixed": "visit_actual_fixed",
    "last_visit_date": "last_visit_date",
  })

  return pl_fact_visit_plan
