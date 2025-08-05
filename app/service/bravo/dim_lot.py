# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_lot.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_lot: pl.DataFrame

  query_dim_lot = f"""
  select
    lot.IsActive is_active,
    concat(item.Code, '_', lot.ItemLotCode) lot_code,
    item.Code item_code,
    lot.ItemLotCode item_lot_code,
    lot.MfgDate mfg_date,
    lot.ExpiryDate exp_date,
    dateadd(hour, 7, lot.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (item.ModifiedAt),
      (lot.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20ItemLotDetail lot
    left join B20Item item on lot.ItemId = item.Id
  where lot.IsGroup = 0
  """
  
  df_dim_lot = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_lot, params=None)
  df_dim_lot = df_dim_lot.unique(subset=["lot_code"], keep="last", maintain_order=False)

  return df_dim_lot
