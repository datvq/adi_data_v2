# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_expense_incurred.parquet")

def from_date_default() -> str:
  return dx.dt.resolve_datetime_string("begin_of_this_fiscal_year")

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_expense_incurred: pl.DataFrame

  # transform
  query_fact_expense_incurred = """
  declare @from_date nvarchar(10) = '2025-05-01';
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  with gl as (
    select
      gl.UniqueId uuid,
      gl.DocDate doc_date,
      gl.DocCode doc_code,
      info.DocNo doc_no,
      info.Description description,
      case when right(gl.EntryNo, 1) = 'A' then 'N' else 'C' end entry_type,
      left(gl.Account, 1) root_account_code,
      left(gl.Account, 3) group_account_code,
      gl.Account account_code,
      debit_customer.Code customer_code,
      gl.ItemCatgCode category_code,
      case when right(gl.EntryNo, 1) = 'B' then 'N' else 'C' end entry_type_crsp,
      left(gl.CrspAccount, 1) root_account_code_crsp,
      left(gl.CrspAccount, 3) group_account_code_crsp,
      gl.CrspAccount account_code_crsp,
      credit_customer.Code customer_code_crsp,
      gl.CrspItemCatgCode category_code_crsp,
      gl.Amount amount,
      gl.AreaCode branch_code,
      department.Code department_code,
      case when (gl.CreditAccount like '5151%' or gl.DebitAccount like '6356%') then 'CT22' else expense.Code end expense_code
    from B30GeneralLedger gl
      left join (
        select Stt, DocCode, DocNo, Description from B30AccDoc union all
        select Stt, DocCode, DocNo, Description from B30AccDocItemHdr union all
        select Stt, DocCode, DocNo, Description from B30AccDocPurchaseHdr union all
        select Stt, DocCode, DocNo, Description from B30AccDocSalesHdr
      ) info on info.Stt = gl.Stt and info.DocCode = gl.DocCode
      left join B20Customer debit_customer on debit_customer.Id = gl.CustomerId
      left join B20Customer credit_customer on credit_customer.Id = gl.CrspCustomerId
      left join B20ExpenseCatg expense on expense.Id = gl.ExpenseCatgId
      left join B20Dept department on gl.DeptId = department.Id
    where gl.IsActive = 1 and gl.BranchCode = 'A01' and gl.Amount <> 0
      and gl.DocDate between @from_date and @to_date
      and left(gl.Account, 3) in ('635', '641', '642', '811', '821')
      and left(gl.CrspAccount, 3) <> '911'
  )
  (
    select
      'increase' incurred_type,
      gl.doc_date doc_date,
      gl.doc_code doc_code,
      gl.doc_no doc_no,
      gl.description description,
      gl.root_account_code root_debit_account_code,
      gl.group_account_code group_debit_account_code,
      gl.account_code debit_account_code,
      gl.root_account_code_crsp root_credit_account_code,
      gl.group_account_code_crsp group_credit_account_code,
      gl.account_code_crsp credit_account_code,
      gl.branch_code branch_code,
      gl.department_code department_code,
      gl.expense_code expense_code,
      gl.amount amount
    from gl
    where
      gl.entry_type = 'N'
  )
  union all
  (
    select
      'decrease' incurred_type,
      gl.doc_date doc_date,
      gl.doc_code doc_code,
      gl.doc_no doc_no,
      gl.description description,
      gl.root_account_code_crsp root_debit_account_code,
      gl.group_account_code_crsp group_debit_account_code,
      gl.account_code_crsp debit_account_code,
      gl.root_account_code root_credit_account_code,
      gl.group_account_code group_credit_account_code,
      gl.account_code credit_account_code,
      gl.branch_code branch_code,
      gl.department_code department_code,
      gl.expense_code expense_code,
      -gl.amount amount
    from gl
    where
      gl.entry_type = 'C'
  )
  """
  df_fact_expense_incurred = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_expense_incurred, params={
    "from_date": from_date,
    "to_date": to_date,
  })

  return df_fact_expense_incurred
