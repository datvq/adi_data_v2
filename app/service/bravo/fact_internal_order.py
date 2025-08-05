# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_internal_order.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(view_date: str) -> pl.DataFrame:
  # return
  df_fact_internal_order: pl.DataFrame

  # transform
  query_fact_internal_order = """
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  
  /* drop temp talbe */
  if object_id(N'tempdb..#tmp_internal_order') is not null drop table #tmp_internal_order
  
  /* create temp table */
  create table #tmp_internal_order
  (
    DocNo nvarchar(max),
    DocDate date,
    ItemId int,
    ItemCode nvarchar(max),
    ItemName nvarchar(max),
    Quantity float,
    Description nvarchar(max),
    Stt_IP nvarchar(max),
    Quantity_PN float,
    EstimatedTimeDelivery date,
    Unit nvarchar(max),
    AreaCode nvarchar(max),
    Id_IP int,
    Dif float,
    _Group nvarchar(max),
    Quantity_GC float,
    Quantity_SX float,
    Quantity_KD float,
    Quantity_ID float,
    DocNo_GC nvarchar(max),
    DocNo_SX nvarchar(max),
    DocNo_KD nvarchar(max),
    DocNo_ID nvarchar(max),
    ManufacturingAreacode nvarchar(max),
    UnitKG nvarchar(max),
    QuantityKG float,
    QuantityKG_PN float,
    ConvertRate float,
    Closed bit,
    DescriptionDetail nvarchar(max),
    ItemCatgCode nvarchar(max),
    DiffKG float
  )
  
  /* fill data to temp table */
  insert into #tmp_internal_order
  execute usp_Tracking_IP
    @_DocDate1 = '2021-01-01',
    @_DocDate2 = @view_date,
    @_BranchCode = 'A01',
    @_Closed = 0
  
  /* select data from temp table */
  -- select value from openjson((
  select
    tmp.Closed closed,
    tmp.DocDate doc_date,
    tmp.DocNo doc_no,
    tmp.AreaCode branch_code,
    tmp.ItemCatgCode category_code,
    tmp.Quantity quantity_order,
    tmp.Quantity_PN quantity_done,
    tmp.Dif quantity_remain,
    tmp.EstimatedTimeDelivery estimated_time_delivery,
    tmp.DescriptionDetail description_detail,
    tmp.Quantity_GC quantity_processing,
    tmp.DocNo_GC doc_no_processing,
    tmp.Quantity_SX quantity_manufacturing,
    tmp.DocNo_SX doc_no_manufacturing,
    tmp.Quantity_KD quantity_forwarding,
    tmp.DocNo_KD doc_no_forwarding,
    tmp.Quantity_ID quantity_buying,
    tmp.DocNo_ID doc_no_buying
  from #tmp_internal_order tmp
  -- for json path))
  
  /* drop temp table when done */
  drop table #tmp_internal_order
  """
  df_fact_internal_order = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_internal_order, params={
    "view_date": view_date,
  })
  df_fact_internal_order = df_fact_internal_order.with_columns([
    pl.col(col).replace([None], 0)
    for col, dtype in zip(df_fact_internal_order.columns, df_fact_internal_order.dtypes) if dtype in [pl.Int64, pl.Float64]
  ])

  return df_fact_internal_order
