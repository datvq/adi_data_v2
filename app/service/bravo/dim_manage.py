# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_manage.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(view_date: str) -> pl.DataFrame:
  # return
  df_dim_manage: pl.DataFrame

  # transform
  query_dim_manage = f"""
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  with
  rc as (
    select
      CustomerId customer_id,
      RegionCode region_code,
      StartDate assigned_date
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
  ),
  rs as (
    select
      rs.RegionCode region_code,
      rs.ItemCatgCode category_code,
      rs.EmployeeCode saleman_code,
      rs.StartDate assigned_date
    from (
      select
        RegionCode,
        ItemCatgCode,
        EmployeeCode,
        StartDate,
        row_number() over(
          partition by
            RegionCode,
            ItemCatgCode
          order by StartDate desc
        ) TopN
      from B20EmployeeCustomer
      where StartDate <= @view_date
    ) rs
    where rs.TopN = 1
  ),
  es as (
    select
      es.EmployeeCode saleman_code,
      es.EmpSuperiorCode superior_code,
      es.StartDate assigned_date
    from (
      select
        EmployeeCode,
        EmpSuperiorCode,
        StartDate,
        row_number() over(
          partition by
            EmployeeCode
          order by StartDate desc
        ) TopN
      from B20EmployeeSuperior
      where StartDate <= @view_date
    ) es
    where es.TopN = 1
  )
  select
    concat(customer.Code, '_', rs.category_code) manage_code,
    customer.Code customer_code,
    customer.Name customer_name,
    rs.category_code category_code,
    category.Name category_name,
    rc.region_code region_code,
    region.Name region_name,
    rs.saleman_code saleman_code,
    saleman.Name saleman_name,
    saleman_department.Code saleman_department_code,
    saleman_department.Name saleman_department_name,
    saleman.AreaCode saleman_branch_code,
    saleman_branch.AreaName2 saleman_branch_name,
    es.superior_code superior_code,
    superior.Name superior_name,
    superior_department.Code superior_department_code,
    superior_department.Name superior_department_name,
    superior.AreaCode superior_branch_code,
    superior_branch.AreaName2 superior_branch_name,
    rc.assigned_date assigned_date_region_for_customer,
    rs.assigned_date assigned_date_saleman_for_region,
    es.assigned_date assigned_date_superior_for_saleman
  from rc
    full join rs on rc.region_code = rs.region_code
    left join es on rs.saleman_code = es.saleman_code
    left join B20Customer customer on rc.customer_id = customer.Id
    left join B20ItemCatg category on rs.category_code = category.Code
    left join B20Region region on rs.region_code = region.Code
    left join B20Employee saleman on rs.saleman_code = saleman.Code
    left join B20Dept saleman_department on saleman.DeptId = saleman_department.Id
    left join B20Area saleman_branch on saleman.AreaCode = saleman_branch.AreaCode
    left join B20Employee superior on es.superior_code = superior.Code
    left join B20Dept superior_department on superior.DeptId = superior_department.Id
    left join B20Area superior_branch on superior.AreaCode = superior_branch.AreaCode
  where rc.customer_id is not null
  """
  df_dim_manage = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_manage, params={
    "view_date": view_date,
  })

  return df_dim_manage
