# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_salary_history.parquet")

def execute() -> pl.DataFrame:
  # return
  df_fact_salary_history: pl.DataFrame

  # transform
  query_fact_salary_history = """
  select
    header.DocCode doc_code,
    header.DocDate doc_date,
    header.DocNo doc_no,
    header.DocStatus doc_status,
    header.Loai_Ps entry_type,
    header.ChangeType change_type,
    header.[Description] description,
    header.AreaCode branch_code,
    branch.AreaName2 branch_name,
    department.Code department_code,
    department.Name department_name,
    header.EmployeeCode employee_code,
    employee.Name employee_name,
    detail.EffectiveDate effective_date,
    header.StartDate start_date,
    header.EndDate end_date,
    detail.ContractType contract_type,
    detail.ContractDate contract_date,
    detail.ContractNo contract_no,
    detail.ProbationaryRate probationary_rate,
    detail.ProbationaryStartDate start_date_probationary,
    detail.ProbationaryEndDate end_date_probationary,
    detail.BaseSalary base_salary,
    detail.TotalAllowance total_allowance,
    detail.SalariesInsurance salaries_insurance,
    detail.InsuranceCoeff insurance_coeff,
    detail.RegMinWage reg_min_wage,
    dateadd(hour, 7, header.CreatedAt) created_at,
    header.CreatedBy created_by,
    dateadd(hour, 7, header.ModifiedAt) modified_at,
    header.ModifiedBy modified_by
  from B30HrmDoc header
    left join B30HrmLabourContract detail on header.HrmDocId = detail.HrmDocId
    left join B20Area branch on header.AreaCode = branch.AreaCode
    left join B20Employee employee on header.EmployeeCode = employee.Code
    left join B20Dept department on employee.DeptId = department.Id
  where header.IsActive = 1
    and header.DocCode in (N'LA', N'LD')
  """
  df_fact_salary_history = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_salary_history, params=None)

  return df_fact_salary_history
