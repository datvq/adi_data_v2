# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_unit_cost.parquet")

def from_date_default() -> str:
  return dx.dt.resolve_datetime_string("begin_of_last_fiscal_year")

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_unit_cost: pl.DataFrame

  # transform
  query_fact_unit_cost = """
  declare @from_date nvarchar(10) = '2024-10-01'
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  
  /* drop temp talbe */
  if object_id(N'TempDb..#tmp_z_san_pham') is not null drop table #tmp_z_san_pham
  
  /* create temp table */
  create table #tmp_z_san_pham
  (
    WorkProcessCode nvarchar(10),
    Year nvarchar(4),
    Month nvarchar(2),
    ProductId int,
    Amount float,
    FinishedProductQuantity float,
    UnitProductCost float
  )
  
  /* fill data to temp table - process CD1 */
  execute usp_B30Cdz_GetData 
    @_Date1		= @from_date, 
    @_Date2		= @to_date, 
    @_Key1		= N'WorkProcessCode = ''CD1''', 
    @_CtTmp		= N'#tmp_z_san_pham', 
    @_nUserId = 0,
    @_LangId  = 0, 
    @_BranchCode = 'A01'
  
  /* select data from temp table - process CD1 */
  update #tmp_z_san_pham
  set WorkProcessCode = 'CD1'
  where WorkProcessCode is null
  
  /* fill data to temp table - process CD2 */
  execute usp_B30Cdz_GetData 
    @_Date1		= @from_date, 
    @_Date2		= @to_date, 
    @_Key1		= N'WorkProcessCode = ''CD2''', 
    @_CtTmp		= N'#tmp_z_san_pham', 
    @_nUserId = 0,
    @_LangId  = 0, 
    @_BranchCode = 'A01'
  
  /* select data from temp table - process CD2 */
  update #tmp_z_san_pham
  set WorkProcessCode = 'CD2'
  where WorkProcessCode is null
  
  /* fill data to temp table - process TAICHE */
  execute usp_B30Cdz_GetData 
    @_Date1		= @from_date, 
    @_Date2		= @to_date, 
    @_Key1		= N'WorkProcessCode = ''TAICHE''', 
    @_CtTmp		= N'#tmp_z_san_pham', 
    @_nUserId = 0,
    @_LangId  = 0, 
    @_BranchCode = 'A01'
  
  /* select data from temp table - process TAICHE */
  update #tmp_z_san_pham
  set WorkProcessCode = 'TAICHE'
  where WorkProcessCode is null
  
  /* delete waste data */
  delete from #tmp_z_san_pham
  where isnull(FinishedProductQuantity, 0) = 0 and isnull(UnitProductCost, 0) = 0
  
  /* TEMP TABLE FOR ITEMS */
  
  /* drop temp talbe */
  if object_id(N'TempDb..#tmp_z_vat_tu') is not null drop table #tmp_z_vat_tu
  
  /* create temp table */
  create table #tmp_z_vat_tu
  (
    WorkProcessCode nvarchar(10),
    Year nvarchar(4),
    Month nvarchar(2),
    ProductId int,
    ItemId int,
    Quantity float,
    Amount float,
  )
  
  /* fill data to temp table - process CD1 */
  execute usp_B30ZDetail_GetData 
    @_Date1		= @from_date, 
    @_Date2		= @to_date, 
    @_Key1		= N'WorkProcessCode = ''CD1''', 
    @_CtTmp		= N'#tmp_z_vat_tu', 
    @_BranchCode	= 'A01'
  
  /* select data from temp table - process CD1 */
  update #tmp_z_vat_tu
  set WorkProcessCode = 'CD1'
  where WorkProcessCode is null
  
  /* fill data to temp table - process CD2 */
  execute usp_B30ZDetail_GetData 
    @_Date1		= @from_date, 
    @_Date2		= @to_date, 
    @_Key1		= N'WorkProcessCode = ''CD2''', 
    @_CtTmp		= N'#tmp_z_vat_tu', 
    @_BranchCode	= 'A01'
  
  /* select data from temp table - process CD2 */
  update #tmp_z_vat_tu
  set WorkProcessCode = 'CD1'
  where WorkProcessCode is null
  
  /* fill data to temp table - process TAICHE */
  execute usp_B30ZDetail_GetData 
    @_Date1		= @from_date, 
    @_Date2		= @to_date, 
    @_Key1		= N'WorkProcessCode = ''TAICHE''', 
    @_CtTmp		= N'#tmp_z_vat_tu', 
    @_BranchCode	= 'A01'
  
  /* select data from temp table - process TAICHE */
  update #tmp_z_vat_tu
  set WorkProcessCode = 'TAICHE'
  where WorkProcessCode is null
  
  /* select data from temp tables */
  select
    vt.WorkProcessCode process_code,
    process.Name process_name,
    concat(right(vt.[Year], 2), right('0' + convert(varchar(2), vt.[Month]), 2)) year_month,
    vt.[Year] [year],
    vt.[Month] [month],
    category.Code category_code,
    category.Name category_name,
    parent_item.Code parent_finished_item_code,
    parent_item.Name parent_finished_item_name,
    product.Code product_code,
    product.Name product_name,
    item_size.Code size_code,
    item_size.Name size_name,
    finished_item.Code finished_item_code,
    finished_item.Name finished_item_name,
    case
      when product.ItemCatgCode = 'TBI' then 20
      else round(1 / nullif(item_weight.ConvertRate, 0), 4)
    end net_weight,
    nullif(item_packing.ConvertRate, 0) units_per_package,
    item_packing.Unit package_type,
    item.Code item_code,
    item.Name item_name,
    vt.Quantity item_quantity,
    vt.Amount item_amount,
    sp.FinishedProductQuantity finished_item_quantity,
    sp.Amount finished_item_amount,
    round(vt.Amount/sp.FinishedProductQuantity, 0) item_cost,
    round(sp.Amount/sp.FinishedProductQuantity, 0) finished_item_cost
  from
    (
      select
        t.WorkProcessCode WorkProcessCode,
        t.Year [Year],
        t.Month [Month],
        t.ProductId ProductId,
        t.ItemId ItemId,
        sum(t.Quantity) Quantity,
        sum(t.Amount) Amount
      from #tmp_z_vat_tu t
      group by t.WorkProcessCode, t.Year, t.Month, t.ProductId, t.ItemId
    ) vt
    left join
    (
      select
        t.WorkProcessCode WorkProcessCode,
        t.Year [Year],
        t.Month [Month],
        t.ProductId ProductId,
        sum(t.Amount) Amount,
        sum(t.FinishedProductQuantity) FinishedProductQuantity
      from #tmp_z_san_pham t
      group by t.WorkProcessCode, t.Year, t.Month, t.ProductId
    ) sp
    on vt.WorkProcessCode = sp.WorkProcessCode and vt.Year = sp.Year and vt.Month = sp.Month and vt.ProductId = sp.ProductId
    left join B20WorkProcess process on process.Code = vt.WorkProcessCode
    left join B20Product finished_item on finished_item.Id = sp.ProductId
    left join B20ItemInfo item_info on item_info.ProductId = finished_item.Id
    left join B20Item item on item.Id = item_info.ItemId
    left join B20ItemUnit item_packing on item_packing.ItemId = item.Id and item_packing.IsUnitConvert = 1
    left join B20ItemUnit item_weight on item_weight.ItemId = item.Id and item_weight.Unit = 'LitKgMet'
    left join B20Item parent_item on parent_item.Id = item.ParentId
    left join B20ItemSize item_size on item_size.Code = item.ItemSizeCode
    left join B20ItemGroup product on product.Code = item.ItemGroupCode
    left join B20ItemCatg category on category.Code = product.ItemCatgCode
    left join B20Item material on material.Id = vt.ItemId
  
  /* drop temp table when done */
  drop table #tmp_z_san_pham
  drop table #tmp_z_vat_tu
  """
  df_fact_unit_cost = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_unit_cost, params={
    "from_date": from_date,
    "to_date": to_date,
  })
  df_fact_unit_cost = df_fact_unit_cost.with_columns([
    pl.col(col).replace([None], 0)
    for col, dtype in zip(df_fact_unit_cost.columns, df_fact_unit_cost.dtypes) if dtype in [pl.Int64, pl.Float64]
  ])

  return df_fact_unit_cost
