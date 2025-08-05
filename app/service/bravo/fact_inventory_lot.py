# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_inventory_lot.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def df_fact_inventory_item_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "fact_inventory_item.parquet")
  return dx.df.read_data_file(data_file)

def execute(view_date: str, df_fact_inventory_item: pl.DataFrame) -> pl.DataFrame:
  # return
  df_fact_inventory_lot: pl.DataFrame

  # transform
  query_fact_inventory_lot = """
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  execute dbo.usp_Vth_EntryAndAxistByLot 
    @_DocDate1 = @view_date,
    @_DocDate2 = @view_date,
    @_GroupByExpr = 1, -- 0: không phân kho, 1: phân kho
    @_BranchCode = 'A01',
    @_RepType = 1 -- 0: tổng hợp NXT theo lô, 1: báo cáo tồn kho theo lô
  """
  df_fact_inventory_lot = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_inventory_lot, params={
    "view_date": view_date,
  })
  df_fact_inventory_lot = df_fact_inventory_lot.with_columns(
    pl.lit(view_date).cast(pl.Date).alias("ViewDate"),
    (pl.col("ItemCode") + "_" + pl.col("ItemLotCode")).alias("LotCode"),
  )
  df_fact_inventory_lot = df_fact_inventory_lot.filter(
    (pl.col("CloseQuantity") != 0),
  )
  df_fact_inventory_lot = dx.df.rename_columns(df_fact_inventory_lot, selected_cols={
    "ViewDate": "view_date",
    "WarehouseCode": "warehouse_code",
    "LotCode": "lot_code",
    "ItemCode": "item_code",
    "ItemLotCode": "item_lot_code",
    "CloseQuantity": "quantity",
  })
  
  # Diff amount
  df_diff = df_fact_inventory_item.select(["warehouse_code", "item_code", "quantity"]).join(
    df_fact_inventory_lot.group_by(["warehouse_code", "item_code"]).agg(pl.col("quantity").sum().alias("quantity_lot")),
    on=["warehouse_code", "item_code"],
    how="full",
    suffix="_",
    coalesce=True,
  )
  df_diff = df_diff.with_columns(
    pl.col("quantity").replace([None], 0),
    pl.col("quantity_lot").replace([None], 0),
  )
  df_diff = df_diff.with_columns(
    (pl.col("quantity") - pl.col("quantity_lot")).alias("diff_quantity")
  )
  df_diff = df_diff.drop(["quantity", "quantity_lot"])
  df_diff = df_diff.rename({"diff_quantity": "quantity"})
  df_diff = df_diff.filter(pl.col("quantity") != 0)
  df_diff = df_diff.with_columns(
    pl.lit(view_date).cast(pl.Date).alias("view_date"),
    pl.lit(None).cast(pl.String).alias("lot_code"),
  )
  df_fact_inventory_lot = dx.df.rename_columns(df_fact_inventory_lot, selected_cols={
    "view_date": "view_date",
    "warehouse_code": "warehouse_code",
    "lot_code": "lot_code",
    "item_code": "item_code",
    "item_lot_code": "item_lot_code",
    "quantity": "quantity",
  })
  df_fact_inventory_lot = pl.concat(
    [df_fact_inventory_lot, df_diff],
    how="align",
    rechunk=True,
  )

  # add missing item
  
  # filter missing items
  df_item_missing = df_fact_inventory_item.join(
    df_fact_inventory_lot.select(["warehouse_code", "item_code"]),
    left_on=["warehouse_code", "item_code"],
    right_on=["warehouse_code", "item_code"],
    how="anti",
    suffix="_",
    coalesce=True,
  )
  
  # append missing items to result
  if len(df_item_missing) > 0:
    df_fact_inventory_lot = pl.concat(
      [df_fact_inventory_lot, df_item_missing],
      how="align",
      rechunk=True,
    )

  return df_fact_inventory_lot
