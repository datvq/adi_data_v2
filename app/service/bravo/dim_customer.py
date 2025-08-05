# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_customer.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(view_date: str) -> pl.DataFrame:
  # return
  df_dim_customer: pl.DataFrame

  # transform
  query_dim_customer = """
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  with
  rc as (
    select
      rc.CustomerId customer_id,
      rc.RegionCode region_code,
      rc.StartDate assigned_date
    from (
      select
        CustomerId,
        RegionCode,
        StartDate,
        row_number() over(
          partition by
            CustomerId
          order by StartDate desc
        ) TopN
      from B20RegionCustomer
      where StartDate <= @view_date
    ) rc
    where rc.TopN = 1
  )
  select
    customer.IsActive is_active,
    customer.IsCustomer is_customer,
    customer.Id customer_id,
    customer.Code customer_code,
    customer.Name customer_name,
    nullif(customer.Address, '') address,
    nullif(replace(replace(replace(coalesce(customer.Tel, customer.PersonTel, null), ' ', ''), '.', ''), '-', ''), '') phone_number,
    nullif(customer.Email, '') email,
    nullif(customer.Latitude, '') latitude,
    nullif(customer.Longitude, '') longitude,
    commune.Name commune_name,
    district.Name district_name,
    province.Name province_name,
    branch.AreaCode customer_branch_code,
    branch.AreaName2 customer_branch_name,
    nullif(customer.Person, '') person,
    nullif(customer.TaxRegNo, '') tax_reg_no,
    nullif(customer.IdCardNo, '') id_card_no,
    customer.IsSendZalo is_send_zalo,
    nullif(customer.TelSendZalo, '') tel_send_zalo,
    region.Code region_code,
    region.Name region_name,
    dateadd(hour, 7, customer.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (customer.ModifiedAt),
      (branch.ModifiedAt),
      (province.ModifiedAt),
      (district.ModifiedAt),
      (commune.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20Customer customer
    left join B20Area branch on customer.AreaCode = branch.AreaCode
    left join B20HrmProvince province on customer.ProvinceCode = province.Code
    left join B20HrmProvince district on customer.DistrictCode = district.Code
    left join B20HrmProvince commune on customer.CommuneCode = commune.Code
    left join rc on customer.Id = rc.customer_id
    left join B20Region region on rc.region_code = region.Code
  where customer.IsGroup = 0
  """
  df_dim_customer = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_customer, params={
    "view_date": view_date,
  })

  return df_dim_customer
