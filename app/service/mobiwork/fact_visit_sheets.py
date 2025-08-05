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
  return os.path.join(shared.env.data_dir, "staged", "mobiwork", "fact_visit_sheets.parquet")

def yymm_default() -> str:
  return dx.dt.datetime_to_string(dx.dt.string_to_datetime("end_of_last_month", f"%Y-%m-%d"), "%y%m")

def execute(yymm: str) -> pl.DataFrame:
  # return
  pl_fact_visit_sheets: pl.DataFrame

  # fact_visit_plan
  year = 2000 + int(yymm[:2])
  month = int(yymm[2:])
  eom = calendar.monthrange(year, month)[1]
  url = f"""{
    shared.env.mobiwork_visit_base_url
  }/Visitsheets?orgid=57bea55c94dec72c0c5b4e0b&year={
    str(year)
  }&month={
    str(month).zfill(2)
  }&assignTo=&fromday=01&today={
    str(eom).zfill(2)
  }&disIn=50&disOut=50"""
  header = adi.mobiwork.create_mobiwork_header(
    email=shared.env.mobiwork_email,
    password=shared.env.mobiwork_password,
    base_url=shared.env.mobiwork_login_base_url,
  )
  res: requests.models.Response = requests.get(url=url, headers=header)
  data: dict = res.json()
  
  result = data.get("result", [])
  rows: list[dict] = []
  for branch_data in result:
    branch_data: dict
    branch_name_joined: str = branch_data.get("name", None)
    branh_parts = branch_name_joined.split("-")
    branch_code = branh_parts[0]
    branch_name = branh_parts[1]
    for employee_data in branch_data.get("employee", []):
      employee_data: dict
      employee_email = employee_data.get("email", None)
      employee_name = employee_data.get("name", None)
      for d in range(1, 32):
        day = str(d)
        day_data = employee_data.get(day, None)
        if day_data is not None:
          for checkin_data in day_data:
            checkin_data: dict
            customer_code: str = None
            if checkin_data.get("discode", None) not in ["", None]:
              customer_code = checkin_data.get("discode")
  
            customer_name: str = None
            if checkin_data.get("discode", None) not in ["", None]:
              customer_name = checkin_data.get("distitle")
  
            checkin_name: str = None
            if checkin_data.get("discode", None)not in ["", None]:
              checkin_name = "Checkin khách hàng"
            else:
              checkin_name = checkin_data.get("distitle")
  
            image = None if checkin_data.get("photo", None) in [[], None] else checkin_data.get("photo")[0]
            checkin_image = None if checkin_data.get("photo_checkIn", None) in [[], None] else checkin_data.get("photo_checkIn")[0]
            checkout_image = None if checkin_data.get("photo_checkOut", None) in [[], None] else checkin_data.get("photo_checkOut")[0]
            try:
              row: dict = {
                "yymm": yymm,
                "year": year,
                "month": month,
                "checkin_uuid": checkin_data.get("_id", None),
                "working_date": datetime.strptime(f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}", f"%Y-%m-%d").date(),
                "branch_code": branch_code,
                "branch_name": branch_name,
                "employee_email": employee_email,
                "employee_name": employee_name,
                "device": checkin_data.get("device", None),
                "checkin_code": checkin_data.get("type", None),
                "checkin_name": checkin_name,
                "checkin_time": checkin_data.get("hms", None),
                "checkout_time": checkin_data.get("hmso", None),
                "sync_time": checkin_data.get("timeSyn", None),
                "valid_sync_time": checkin_data.get("validTimeSyn", None),
                "checkin_lat": checkin_data.get("lat", None),
                "checkin_long": checkin_data.get("long", None),
                "checkin_address": checkin_data.get("address", None),
                "customer_code": customer_code,
                "customer_name": customer_name,
                "direction": checkin_data.get("direction", {}).get("distance", None),
                "image": image,
                "checkin_image": checkin_image,
                "checkout_image": checkout_image,
              }
              rows.append(row)
            except:
              print(row)
  pl_fact_visit_sheets = pl.from_dicts(rows, infer_schema_length=1000)

  return pl_fact_visit_sheets
