# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_invoice_payment_detail.parquet")

def from_date_default() -> str:
  return dx.dt.resolve_datetime_string("begin_of_this_fiscal_year")

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_invoice_payment_detail: pl.DataFrame

  # transform
  query_fact_invoice_payment_detail = """
  declare @from_date nvarchar(10) = '2024-05-01';
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  execute usp_Kcd_ChiTietThanhToanHoaDonGiamTruCongNo
    @_DocDate1 = @from_date,
    @_DocDate2 = @to_date,
    @_BranchCode = 'A01',
    @_CustomerId = '',
    @_EmployeeCode = ''
  """
  df_fact_invoice_payment_detail = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_invoice_payment_detail, params={
    "from_date": from_date,
    "to_date": to_date,
  })
  df_fact_invoice_payment_detail = df_fact_invoice_payment_detail.with_columns(
    (pl.col("CustomerCode") + "_" + pl.col("ItemCatgCode")).alias("ManageCode"),
  )
  df_fact_invoice_payment_detail = dx.df.rename_columns(df_fact_invoice_payment_detail, selected_cols={
    "DocDate": "doc_date",
    "DocNo_SourceDoc": "doc_code",
    "Description": "description",
    "AreaCode": "branch_code",
    "CustomerId": "customer_id",
    "CustomerId": "customer_code",
    "ItemCatgCode": "category_code",
    "DocCodePromotion": "promotion_type",
    "DocNoPromotion": "promotion_applied",
    "CreditAmount": "amount",
  })

  return df_fact_invoice_payment_detail
