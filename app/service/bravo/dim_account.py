# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_account.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_account: pl.DataFrame

  # transform
  query_dim_account = """
  select
    account.IsActive is_active,
    left(account.Code, 1) root_account_code,
    groupacc.Code group_account_code,
    groupacc.Name group_account_name,
    account.Code account_code,
    account.Name account_name,
    account.ForeignCurrAccount is_foreign_currency_account,
    account.BankAccount is_bank_account,
    account.LongTermAccount is_long_term_account,
    account.IsParentAccount is_parent_account,
    account.GLAccount has_general_ledger,
    account.CustomerAccount has_customer_account,
    account.ProductAccount has_product_account,
    account.ItemCatgAccount has_category_account,
    dateadd(hour, 7, account.CreatedAt) created_at,
    dateadd(hour, 7, (select max(d) from (values
      (groupacc.ModifiedAt),
      (account.ModifiedAt)
    ) all_dates(d))) modified_at
  from B20ChartOfAccount account
    left join B20ChartOfAccount groupacc on groupacc.Code = left(account.Code, 3)
  where account.IsGroup = 0
  """
  df_dim_account = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_account, params=None)

  return df_dim_account
