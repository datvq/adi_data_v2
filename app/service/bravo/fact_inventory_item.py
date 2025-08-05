# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_inventory_item.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(view_date: str) -> pl.DataFrame:
  # return
  df_fact_inventory_item: pl.DataFrame

  # transform
  query_fact_inventory_item = """
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  execute usp_Vcd_BaoCaoTonKho
    @_DocDate2 = @view_date,
    @_BranchCode = 'A01',
    @_GroupByListField = 'WarehouseId'
  """
  df_fact_inventory_item = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_inventory_item, params={
    "view_date": view_date,
  })
  df_fact_inventory_item = df_fact_inventory_item.with_columns(
    pl.lit(view_date).cast(pl.Date).alias("ViewDate"),
    pl.col("WarehouseName").str.split(" - ").list.get(0).alias("WarehouseCode"),
  )
  df_fact_inventory_item = dx.df.rename_columns(df_fact_inventory_item, selected_cols={
    "ViewDate": "view_date",
    "WarehouseCode": "warehouse_code",
    "ItemCode": "item_code",
    "UnitCost": "unit_cost",
    "CloseInventory": "quantity",
    "CloseAmount": "amount",
  })

  return df_fact_inventory_item
