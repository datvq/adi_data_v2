# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_sales_order.parquet")

def from_date_default() -> str:
  return "2021-10-01"

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_sales_order: pl.DataFrame

  # transform
  query_fact_sales_order = f"""
  declare @from_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  select
    header.IsActive is_active,
    header.DocCode doc_code,
    header.DocDate doc_date,
    header.DocNo doc_no,
    header.TransCode trans_code,
    header.DocStatus doc_status,
    header.DueDate due_days,
    header.Description description,
    header.AreaCode branch_code,
    header.ReasonForCanceled reason_for_canceled,
    concat(customer.Code, '_', header.ItemCatgCode) manage_code,
    customer.Code customer_code,
    header.ItemCatgCode category_code,
    header.FromApp order_from_app,
    header.CurrencyCode currency_code,
    header.ExchangeRate exchange_rate,
    header.PortOfLoading port_of_loading,
    header.EmployeeCode employee_code,
    header.DocCodePromotion doc_code_promotion,
    promotion.DocNo doc_no_promotion,
    item.Code item_code,
    detail.Quantity quantity,
    header.IsDirectPayment is_direct_payment,
    detail.IsGiftItem is_gift_item,
    detail.SalesForC2 sale_for_c2,
    detail.Remark detail_note,
    header.IdDMS id_dms,
    header.DocNoOfDMS doc_no_dms,
    header.DMSCreatedAt created_at_dms,
    header.CreatedAt created_at,
    header.ModifiedAt modified_at
  from B30BizDocDetailSO detail
    left join B30BizDoc header on header.BizDocId = detail.BizDocId
    left join B20Customer customer on header.CustomerId = customer.Id
    left join B20Item item on detail.ItemId = item.Id
    left join vPromotion promotion on header.Stt_Promotion = promotion.Stt
  where header.IsActive = 1
    and header.DocStatus < 9
    and header.BranchCode = 'A01'
    and header.DocCode = 'SO'
    and header.DocDate between @from_date and @to_date
  """
  df_fact_sales_order = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_sales_order, params={
    "from_date": from_date,
    "to_date": to_date,
  })

  return df_fact_sales_order
