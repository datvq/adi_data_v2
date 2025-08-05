# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_sales_actual.parquet")

def from_date_default() -> str:
  return "2021-10-01"

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def stable_date_default() -> str:
  return dx.dt.datetime_to_string(dx.dt.end_of_month(date.today(), offset_days=-55), f"%Y-%m-%d")

def execute(from_date: str, to_date: str, stable_date: str) -> pl.DataFrame:
  # return
  df_fact_sales_actual: pl.DataFrame

  # transform
  query_fact_sales_actual = f"""
  declare @from_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  execute usp_Vct_BangKeChungTu
    @_DocDate1 = @from_date,
    @_DocDate2 = @to_date,
    @_IsSalesList = 1,
    @_RepType = '1',
    @_DocCodeLst = 'H2,DV,TL',
    @_BranchCode = 'A01',
    @_CurrencyCode0 = 'VND'
  """
  df_fact_sales_actual = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_sales_actual, params={
    "from_date": from_date,
    "to_date": to_date,
  })
  df_fact_sales_actual = df_fact_sales_actual.with_columns(
    (pl.col("CustomerCode") + "_" + pl.col("ItemCatgCode")).alias("ManageCode"),
    (pl.col("ItemCode") + "_" + pl.col("ItemLotCode")).alias("LotCode"),
  )
  df_fact_sales_actual = df_fact_sales_actual.with_columns(
    pl.when(pl.col("DocCode") == "TL").then(-pl.col("Quantity")).otherwise(pl.col("Quantity")).alias("Quantity"),
    pl.when(pl.col("DocCode") == "TL").then(-pl.col("TotalAmount")).otherwise(pl.col("TotalAmount")).alias("TotalAmount"),
    pl.when(pl.col("DocCode") == "TL").then(-pl.col("Amount2")).otherwise(pl.col("Amount2")).alias("Amount2"),
    pl.when(pl.col("DocCode") == "TL").then(-pl.col("Amount3")).otherwise(pl.col("Amount3")).alias("Amount3"),
    pl.when(pl.col("DocCode") == "TL").then(-pl.col("Amount")).otherwise(pl.col("Amount")).alias("Amount"),
  )
  
  selected_cols = {
    "DocDate": "doc_date",
    "DocCode": "doc_code",
    "DocNo": "doc_no",
    "AreaCode": "doc_branch_code",
    "ManageCode": "manage_code",
    "CustomerCode": "customer_code",
    "ItemCatgCode": "category_code",
    "LotCode": "lot_code",
    "ItemCode": "item_code",
    "ItemLotCode": "item_lot_code",
    "DueDate": "due_days",
    "Quantity": "quantity",
    "UnitPriceIncludeVat": "invoice_price",
    "UnitPrice": "net_price",
    "UnitCost": "unit_cost",
    "TotalAmount": "invoice_amount",
    "Amount2": "net_amount",
    "Amount3": "tax_amount",
    "Amount": "cost_amount",
    "IsDirectPayment": "is_direct_payment",
    "IsGiftItem": "is_gift_item",
    "WarehouseCode": "warehouse_code",
    "DocCodePromotion": "doc_code_promotion",
    "DocNo_PromotionHdr": "doc_no_promotion",
    "DocNoPromotion": "doc_no_promotion_applied",
    "LicensePlate": "plate_number",
    "DriverName": "driver_name",
    "EmployeeCodeSO": "order_employee_code",
    "EmployeeNameSO": "order_employee_name",
  }
  df_fact_sales_actual = df_fact_sales_actual[list(selected_cols.keys())]
  df_fact_sales_actual = df_fact_sales_actual.rename(selected_cols)

  # fill assumed_unit_cost
  
  # lấy size cho item
  query_size = """
  select
    item.Code item_code,
    isize.Code size_code
  from B20Item item
    left join B20ItemSize isize on item.ItemSizeCode = isize.Code
  where item.IsGroup = 0
  """
  df_size = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_size, params=None)
  
  df_fact_sales_actual = df_fact_sales_actual.join(
    df_size,
    on=["item_code"],
    how="left",
    suffix="_",
    coalesce=True,
  )
  
  # giá vốn gần nhất cả item và size
  df_unit_cost = df_fact_sales_actual.filter(
    (pl.col("doc_date") < dx.dt.string_to_datetime(stable_date, "%Y-%m-%d"))
    & (pl.col("doc_code") == "H2")
    & (pl.col("doc_branch_code").is_in(["A01", "A02", "A03", "A04"]))
  ).select(
    ["doc_date", "item_code", "size_code", "unit_cost"]
  ).sort(
    by=["doc_date"],
    descending=[True],
  ).unique(
    subset=["item_code"],
    keep="first",
    maintain_order=False,
  )
  
  # giá vốn theo item
  df_item_costs = df_unit_cost.unique(
    subset=["item_code"],
    keep="first",
    maintain_order=False,
  ).select(
    ["item_code", "unit_cost"]
  ).rename({
    "unit_cost": "item_unit_cost",
  })
  
  # giá vốn theo size
  df_size_costs = df_unit_cost.unique(
    subset=["size_code"],
    keep="first",
    maintain_order=False,
  ).select(
    ["size_code", "unit_cost"]
  ).rename({
    "unit_cost": "size_unit_cost"
  })
  
  # join để thêm cột assumed_unit_cost
  df_fact_sales_actual = df_fact_sales_actual.join(
    df_item_costs,
    on=["item_code"],
    how="left",
    suffix="_",
    coalesce=True,
  )
  df_fact_sales_actual = df_fact_sales_actual.join(
    df_size_costs,
    on=["size_code"],
    how="left",
    suffix="_",
    coalesce=True,
  )
  df_fact_sales_actual = df_fact_sales_actual.with_columns(
    pl.when(pl.col("doc_date") <= pl.lit(stable_date).cast(pl.Date)).then(pl.col("unit_cost"))
      .when(pl.col("item_unit_cost") > 0).then(pl.col("item_unit_cost"))
      .when(pl.col("size_unit_cost") > 0).then(pl.col("size_unit_cost"))
      .otherwise(0.8 * pl.col("net_price"))
      .alias("assumed_unit_cost")
  )
  df_fact_sales_actual = df_fact_sales_actual.drop(["size_code", "item_unit_cost", "size_unit_cost"])

  return df_fact_sales_actual
