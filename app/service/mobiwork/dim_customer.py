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
  return os.path.join(shared.env.data_dir, "staged", "mobiwork", "dim_customer.parquet")

def execute() -> pl.DataFrame:
  # return
  pl_dim_customer: pl.DataFrame

  # fact_visit_plan
  url = f"""{
    shared.env.mobiwork_login_base_url
  }/RecordsV2?next=1000000&skip=0&formID=57bea55d94dec72c0c5b4e10"""
  header = adi.mobiwork.create_mobiwork_header(
    email=shared.env.mobiwork_email,
    password=shared.env.mobiwork_password,
    base_url=shared.env.mobiwork_base_url,
  )
  res: requests.models.Response = requests.post(
    url=url,
    headers=header,
    json={"objfind": {}, "objorder": {"createdDate": -1}},
  )
  data: dict = res.json()
  
  result = data.get("result", [])
  rows: list[dict] = []
  for item in result:
    item: dict
    row: dict = {
      "locked": item.get("isArchived"),
      "customer_uuid": item.get("_id"),
      "customer_id": item.get("data", {}).get("id_bravo", {}).get("viewData"),
      "customer_code": item.get("data", {}).get("ma_khach_hang", {}).get("viewData"),
      "customer_name": item.get("data", {}).get("khach_hang", {}).get("viewData"),
      "phone_number": item.get("data", {}).get("sdt", {}).get("viewData"),
      "mobile_account": item.get("settings", {}).get("phoneacc"),
      "mobile_account_updated_by": item.get("settings", {}).get("phoneaccby", {}).get("name"),
      "latitude": item.get("lat"),
      "longitude": item.get("long"),
      "address": item.get("data", {}).get("dia_chi", {}).get("viewData"),
      "channel_code": item.get("data", {}).get("kenh", {}).get("choice_values"),
      "channel_name": item.get("data", {}).get("kenh", {}).get("viewData"),
      "biz_type_code": item.get("data", {}).get("nhom_khach_hang", {}).get("choice_values"),
      "biz_type_name": item.get("data", {}).get("nhom_khach_hang", {}).get("viewData"),
      "customer_type_code": item.get("data", {}).get("loai_khach_hang", {}).get("choice_values"),
      "customer_type_name": item.get("data", {}).get("loai_khach_hang", {}).get("viewData"),
      "province_code": item.get("data", {}).get("tinh", {}).get("choice_values"),
      "province_name": item.get("data", {}).get("tinh", {}).get("viewData"),
      "district_code": item.get("data", {}).get("quan", {}).get("choice_values"),
      "district_name": item.get("data", {}).get("quan", {}).get("viewData"),
      "commune_code": item.get("data", {}).get("xa", {}).get("choice_values"),
      "commune_name": item.get("data", {}).get("xa", {}).get("viewData"),
      "created_by": item.get("createdBy", {}).get("name"),
      "created_at": datetime.fromtimestamp(item.get("createdDate", 0) / 1000),
      "updated_by": item.get("modifiedBy", {}).get("name"),
      "updated_at": datetime.fromtimestamp(item.get("modifiedDate", 0) / 1000),
    }
    rows.append(row)
  
  pl_dim_customer = pl.from_dicts(rows, infer_schema_length=1000)

  return pl_dim_customer
