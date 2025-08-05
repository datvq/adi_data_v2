# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_bom.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(view_date: str) -> pl.DataFrame:
  # return
  df_dim_bom: pl.DataFrame

  # transform
  query_dim_bom = """
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  select
    header.WorkProcessCode process_code,
    header.Id bom_id,
    header.BOMType bom_type,
    header.Version bom_version,
    header.IsOriginal is_origin,
    header.EffectiveDate effective_date,
    header.FinishedDate finished_date,
    item.Id item_id,
    item.Code item_code,
    detail.ItemId material_id,
    material.Code material_code,
    detail.Unit material_unit,
    detail.IsMainItem is_main_material,
    coalesce(detail.Quantity9 / nullif(detail.ProductQuantity, 0), 0) material_per_item,
    coalesce(detail.ProductQuantity / nullif(detail.Quantity9, 0), 0) item_per_material,
    detail.ScrapRate scrap_rate,
    dateadd(hour, 7, detail.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (project.ModifiedAt),
      (item_info.ModifiedAt),
      (item.ModifiedAt),
      (material.ModifiedAt),
      (detail.ModifiedAt),
      (header.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20BOM header
    left join B20BOMDetail detail on detail.BOMId = header.Id
    left join B20Product project on project.Id = header.ProductId
    left join B20ItemInfo item_info on item_info.ProductId = project.Id
    left join B20Item item on item.Id = item_info.ItemId
    left join B20Item material on material.Id = detail.ItemId
  where header.IsActive = 1 and header.IsGroup = 0
    and @view_date between isnull(header.EffectiveDate, @view_date) and isnull(header.FinishedDate, @view_date)
  """
  df_dim_bom = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_bom, params={
    "view_date": view_date,
  })

  return df_dim_bom
