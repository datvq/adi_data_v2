# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_ar_object.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(view_date: str) -> pl.DataFrame:
  # return
  df_fact_ar_object: pl.DataFrame

  # transform
  query_fact_ar_object = """
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  execute usp_Kcd_SoTongHopCongNo
    @_DocDate1 = @view_date, 
    @_DocDate2 = @view_date,
    @_Account = '131', 
    @_BranchCode = 'A01', 
    @_CurrencyCode0 = 'VND'
  """
  df_fact_ar_object: pl.DataFrame = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_ar_object, params={
    "view_date": view_date,
  })
  df_fact_ar_object = df_fact_ar_object.with_columns(
    pl.lit(view_date).cast(pl.Date).alias("ViewDate"),
    (pl.col("DebitBal2") - pl.col("CreditBal2")).alias("CloseBal"),
  )
  df_fact_ar_object = df_fact_ar_object.filter(
    (pl.col("CustomerCode").is_not_null())
      & (pl.col("CloseBal") != 0)
  )
  df_fact_ar_object = dx.df.rename_columns(df_fact_ar_object, selected_cols={
    "ViewDate": "view_date",
    "CustomerId": "customer_id",
    "CustomerCode": "customer_code",
    "CustomerName": "customer_name",
    "AreaCodeOfCus": "customer_branch_code",
    "AreaNameOfCus": "customer_branch_name",
    "CloseBal": "close_amount",
  })

  return df_fact_ar_object
