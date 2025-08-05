# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_ar_aging.parquet")

def view_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def df_fact_ar_object_default() -> str:
  data_file = os.path.join(shared.env.data_dir, "staged", "bravo", "fact_ar_object.parquet")
  return dx.df.read_data_file(data_file)

def execute(view_date: str, df_fact_ar_object: pl.DataFrame) -> pl.DataFrame:
  # return
  df_fact_ar_aging: pl.DataFrame

  # transform
  query_fact_ar_aging = """
  declare @view_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  set nocount on;
  set ansi_warnings off;
  execute usp_Kcd_AgingReport
    @_DocDate1 = @view_date,
    @_DocDate2 = @view_date,
    @_Account = '131',
    @_Period = 30,
    @_RepType = 1,
    @_IsPrepaymentInclude = 1,
    @_BranchCode = 'A01',
    @_CurrencyCode0 = 'VND'
  """
  df_fact_ar_aging: pl.DataFrame = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_ar_aging, params={
    "view_date": view_date,
  })
  df_fact_ar_aging = df_fact_ar_aging.filter(pl.col("DocNo") != "")
  df_fact_ar_aging = df_fact_ar_aging.with_columns(
    pl.lit(view_date).cast(pl.Date).alias("ViewDate"),
    (pl.col("CustomerCode").cast(pl.String) + "_" + pl.col("ItemCatgCode")).alias("ManageCode"),
    pl.col("IsDirectPayment").cast(pl.Boolean).alias("IsDirectPayment"),
    pl.when(pl.col("CloseBal") <= 0).then(pl.lit("D01"))
      .when(pl.col("OverDue") < -30).then(pl.lit("D02"))
      .when(pl.col("OverDue") < -10).then(pl.lit("D03"))
      .when(pl.col("OverDue") < 1).then(pl.lit("D04"))
      .when(pl.col("OverDue") < 31).then(pl.lit("D05"))
      .when(pl.col("OverDue") < 91).then(pl.lit("D06"))
      .when(pl.col("OverDue") < 181).then(pl.lit("D07"))
      .when(pl.col("OverDue") < 361).then(pl.lit("D08"))
      .when(pl.col("OverDue") < 721).then(pl.lit("D09"))
      .when(pl.col("OverDue") >= 721).then(pl.lit("D10"))
      .otherwise(pl.lit("D00"))
      .alias("DebtTypeCode"),
  )
  df_fact_ar_aging = dx.df.rename_columns(df_fact_ar_aging, selected_cols={
    "ViewDate": "view_date",
    "DocDate": "doc_date",
    "DocCode": "doc_code",
    "DocNo": "doc_no",
    "DueDate": "due_days",
    "DateDue": "due_date",
    "AreaCode": "doc_branch_code",
    "ManageCode": "manage_code",
    "CustomerCode": "customer_code",
    "ItemCatgCode": "category_code",
    "IsDirectPayment": "is_direct_payment",
    "DocCodePromotion": "promotion_type",
    "DocNoPromotion": "promotion_applied",
    "DueAmount": "invoice_amount",
    "PaidAmount": "paid_amount",
    "CloseBal": "remain_amount",
    "OverDue": "over_due_days",
    "DebtTypeCode": "debt_type_code",
  })
  
  # Diff amount
  view_date_aging = df_fact_ar_aging.select(pl.col("view_date").first()).item()
  view_date_object = df_fact_ar_object.select(pl.col("view_date").first()).item()
  if view_date_aging != view_date_object:
    raise ValueError(f"view_date on fact_ar_aging is different from fact_ar_object")
  
  df_diff = df_fact_ar_object.select(["customer_code", "close_amount"]).join(
    df_fact_ar_aging.group_by("customer_code").agg(pl.col("remain_amount").sum()),
    on=["customer_code"],
    how="full",
    suffix="_",
    coalesce=True,
  )
  df_diff = df_diff.with_columns(
    pl.col("close_amount").replace([None], 0),
    pl.col("remain_amount").replace([None], 0),
  )
  df_diff = df_diff.with_columns(
    (pl.col("close_amount") - pl.col("remain_amount")).alias("diff_amount")
  )
  df_diff = df_diff.drop(["close_amount", "remain_amount"])
  df_diff = df_diff.rename({"diff_amount": "remain_amount"})
  df_diff = df_diff.filter(pl.col("remain_amount") != 0)
  df_diff = df_diff.with_columns(
    pl.lit(view_date_object).cast(pl.Date).alias("doc_date"),
    pl.lit("D00").alias("debt_type_code"),
    pl.lit("CHU").alias("category_code"),
    (pl.col("customer_code").cast(pl.String) + "_CHU").alias("manage_code"),
  )
  df_fact_ar_aging = pl.concat(
    [df_fact_ar_aging, df_diff],
    how="align",
    rechunk=True,
  )

  return df_fact_ar_aging
