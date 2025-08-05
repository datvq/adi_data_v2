# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_invoice_payment.parquet")

def from_date_default() -> str:
  return dx.dt.resolve_datetime_string("begin_of_this_fiscal_year")

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_invoice_payment: pl.DataFrame

  # transform
  query_fact_invoice_payment = """
  declare @from_date nvarchar(10) = '2024-05-01';
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  execute usp_ChiTietThanhToanHoaDon
    @_DocDate1 = @from_date,
    @_DocDate2 = @to_date,
    @_Account = '131',
    @_BranchCode = 'A01',
    @_CurrencyCode0 = 'VND'
  """
  df_fact_invoice_payment = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_invoice_payment, params={
    "from_date": from_date,
    "to_date": to_date,
  })
  df_fact_invoice_payment = df_fact_invoice_payment.with_columns(
    (pl.col("CustomerCode") + "_" + pl.col("ItemCatgCode")).alias("ManageCode"),
  )
  df_fact_invoice_payment = dx.df.rename_columns(df_fact_invoice_payment, selected_cols={
    "DocDate": "doc_date",
    "DocCode": "doc_code",
    "DocNo": "doc_no",
    "DocDate_SourceDoc": "payment_doc_date",
    "DocCode_SourceDoc": "payment_doc_code",
    "DocNo_SourceDoc": "payment_doc_no",
    "ManageCode": "manage_code",
    "CustomerId": "customer_id",
    "CustomerCode": "customer_code",
    "ItemCatgCode": "category_code",
    "TotalAmount": "total_amount",
    "IsOpenDue": "is_open_due",
    "Sx": "payment_count",
    "Amount": "payment_amount",
    "Remain": "debt_remain",
  })

  return df_fact_invoice_payment
