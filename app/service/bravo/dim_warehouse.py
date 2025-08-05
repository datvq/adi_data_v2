# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_warehouse.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_warehouse: pl.DataFrame

  # transform
  query_dim_warehouse = """
  select
    warehouse.IsActive is_active,
    warehouse.Id warehouse_id,
    warehouse.Code warehouse_code,
    warehouse.Name warehouse_name,
    branch.AreaCode warehouse_branch_code,
    branch.AreaName2 warehouse_branch_name,
    receive_branch.AreaCode receive_warehouse_branch_code,
    receive_branch.AreaName2 receive_warehouse_branch_name,
    dateadd(hour, 7, warehouse.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (branch.ModifiedAt),
      (receive_branch.ModifiedAt),
      (warehouse.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20Warehouse warehouse
    left join B20Area branch on warehouse.AreaCode = branch.AreaCode
    left join B20Area receive_branch on warehouse.ReceiveAreaCode = receive_branch.AreaCode
  where warehouse.IsGroup = 0
  """
  df_dim_warehouse = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_warehouse, params=None)

  return df_dim_warehouse
