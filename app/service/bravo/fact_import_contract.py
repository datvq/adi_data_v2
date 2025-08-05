# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact", "fact_import_contract", "v1.parquet")

def from_date_default() -> str:
  return dx.dt.resolve_datetime_string("begin_of_this_fiscal_year")

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_import_contract: pl.DataFrame

  # transform
  query_fact_import_contract = """
  declare @from_date nvarchar(10) = '2021-01-01';
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  select
    header.Closed closed,
    header.DocDate doc_date,
    header.DocCode doc_code,
    header.DocNo doc_no,
    header.Description description,
    header.CurrencyCode currency_code,
    header.ExchangeRate exchange_rate,
    header.CustomerId customer_id,
    header.DepositDate deposit_date,
    header.FinishedDate dinished_date,
    header.HandoverDate handover_date,
    header.DeptOfDays due_days,
    detail.ItemId item_id,
    detail.Unit unit,
    detail.Quantity9 quantity,
    detail.OriginalUnitCost origin_unit_cost,
    detail.OriginalAmount origin_total_cost,
    header.PaymentTermsCode payment_method_code,
    detail.UnitCost unit_cost,
    detail.Amount3 tax_amount,
    detail.Amount9 total_cost,
    detail.TaxCode tax_code,
    detail.Remark detail_note
  from B30BizDocDetailPO detail
    left join B30BizDoc header on detail.BizDocId = header.BizDocId
  where header.IsActive = 1 and header.DocStatus in (3, 4) and header.BranchCode = 'A01'
    and header.DocCode = 'PO' and header.ContractType = 3
    and header.DocDate between @from_date and @to_date
  """
  df_fact_import_contract = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_import_contract, params={
    "from_date": from_date,
    "to_date": to_date,
  })

  return df_fact_import_contract
