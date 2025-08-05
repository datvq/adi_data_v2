# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim", "dim_employee", "v1.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(view_date: str) -> pl.DataFrame:
  # return
  df_dim_employee: pl.DataFrame

  # transform
  query_dim_employee = """
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  with es as (
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
        ) top_n
      from B20EmployeeSuperior
      where StartDate <= @view_date
    ) es
    where es.top_n = 1
  )
  select
    employee.IsActive is_active,
    superior_branch.AreaCode superior_branch_code,
    superior_branch.AreaName2 superior_branch_name,
    superior_department.Code superior_department_code,
    superior_department.Name superior_department_name,
    vsuperior.JobTitleName0 superior_job_title_name,
    superior.Code superior_code,
    superior.Name superior_name,
    branch.AreaCode employee_branch_code,
    branch.AreaName2 employee_branch_name,
    department.Code employee_department_code,
    department.Name employee_department_name,
    vemployee.JobTitleName0 employee_job_title_name,
    employee.Code employee_code,
    employee.Name employee_name,
    employee.Email private_email,
    employee.Email2 company_email,
    employee.Gender gender,
    employee.IsHana is_hana,
    employee.WifeInCompany wife_in_company,
    employee.BirthDate birth_date,
    employee.Tel phone_number,
    employee.PersonalTaxCode personal_tax_code,
    employee.PersonalTaxCodeSupplier personal_tax_code_supplier,
    employee.IdCardNo id_card_number,
    employee.IdCardDate id_card_date,
    employee.IdCardIssuePlace id_card_issue_place,
    employee.GraduteYear gradute_year,
    employee.GraduteSchool gradute_school,
    employee.Ethnicity ethnicity,
    employee.Religion religion,
    employee.Nationality nationality,
    employee.HomeCountry home_country,
    employee.Address address,
    employee.Domicile domicile,
    employee.BankName bank_name,
    employee.BankAccount bank_account,
    employee.TempStartDate temp_start_date,
    employee.FirstWorkingDate first_working_date,
    employee.ResignDate resign_date,
    employee.InsuranceNo insurance_no,
    employee.LicensePlate license_plate,
    employee.DriveLicense drive_license,
    employee.DriveLicenseDate drive_license_date,
    employee.DriveLicensePlace drive_license_place,
    dateadd(hour, 7, employee.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (employee.ModifiedAt),
      (branch.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20Employee employee
    left join B20Area branch on employee.AreaCode = branch.AreaCode
    left join B20Dept department on employee.DeptId = department.Id
    left join es on employee.Code = es.saleman_code
    left join B20Employee superior on es.superior_code = superior.Code
    left join B20Area superior_branch on superior.AreaCode = superior_branch.AreaCode
    left join B20Dept superior_department on superior.DeptId = superior_department.Id
    left join vB20Employee vemployee on employee.Code = vemployee.Code
    left join vB20Employee vsuperior on superior.Code = vsuperior.Code
  where employee.IsGroup = 0
  """
  df_dim_employee = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_employee, params={
    "view_date": view_date,
  })

  return df_dim_employee
