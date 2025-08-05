# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_list_import.parquet")

def from_date_default() -> str:
  return dx.dt.resolve_datetime_string("begin_of_this_fiscal_year")

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_list_import: pl.DataFrame

  # transform
  query_fact_list_import = """
  declare @from_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  execute usp_Vct_BangKeChungTu
    @_DocDate1 = @from_date,
    @_DocDate2 = @to_date,
    @_BranchCode = N'A01',
    @_CurrencyCode0 = N'VND',
    @_DocGroup = 1, -- 1:import, 2:export
    @_RepType = 1,
    @_CustomerId = '',
    @_EmployeeCode = ''
  """
  df_fact_list_import = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_list_import, params={
    "from_date": from_date,
    "to_date": to_date,
  })
  df_fact_list_import = dx.df.rename_columns(df_fact_list_import, selected_cols={
    "DocDate": "doc_date",
    "DocCode": "doc_code",
    "DocNo": "doc_no",
    "Description": "description",
    "TransCode": "transCode",
    "ItemId": "item_id",
    "ItemCode": "item_code",
    "ItemLotCode": "category_code",
    "WarehouseId": "warehouse_id",
    "WarehouseCode": "warehouse_code",
    "TaxCode": "tax_code",
    "DebitAccount": "debit_account",
    "CreditAccount": "credit_account",
    "Quantity": "quantity",
    "UnitCost": "price",
    "Amount": "amount",
    "OriginalUnitCost": "origin_price",
    "OriginalAmount": "origin_amount",
    "UnitPrice": "import_price",
    "Amount2": "import_amount",
    "OriginalUnitPrice": "origin_import_price",
    "OriginalAmount2": "origin_import_amount",
  })

  return df_fact_list_import
