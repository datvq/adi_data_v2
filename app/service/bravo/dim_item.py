# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_item.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_item: pl.DataFrame

  # transform
  query_dim_item = """
  with
  ipacking as (select * from B20ItemUnit where IsUnitConvert = 1),
  iweight as (select * from B20ItemUnit where Unit = 'LitKgMet'),
  ispec as (select * from B20ItemUnit where Unit = 'PACKAGE')
  select
    item.IsActive is_active,
    item.Id item_id,
    item.Code item_code,
    item.Name item_name,
    iparent.Code parent_code,
    iparent.Name parent_name,
    isize.Code size_code,
    isize.Name size_name,
    product.Code product_code,
    product.Name product_name,
    category.Code category_code,
    category.Name category_name,
    item.Unit unit,
    round(1 / nullif(iweight.ConvertRate, 0), 4) net_weight,
    nullif(ispec.ConvertRate, 0) bag_botle_weight,
    nullif(ipacking.ConvertRate, 0) units_per_package,
    ipacking.Unit package_type,
    item.Type2 item_type,
    dateadd(hour, 7, item.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (iparent.ModifiedAt),
      (isize.ModifiedAt),
      (product.ModifiedAt),
      (category.ModifiedAt),
      (ipacking.ModifiedAt),
      (iweight.ModifiedAt),
      (ispec.ModifiedAt),
      (item.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20Item item
    left join B20Item iparent on item.ParentId = iparent.Id
    left join B20ItemSize isize on item.ItemSizeCode = isize.Code
    left join B20ItemGroup product on item.ItemGroupCode = product.Code
    left join B20ItemCatg category on product.ItemCatgCode = category.Code
    left join ipacking ipacking on item.Id = ipacking.ItemId
    left join iweight iweight on item.Id = iweight.ItemId
    left join ispec ispec on item.Id = ispec.ItemId
  where item.IsGroup = 0
  """
  df_dim_item = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_item, params=None)

  return df_dim_item
