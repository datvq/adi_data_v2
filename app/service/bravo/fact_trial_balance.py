# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_trial_balance.parquet")

def from_date_default() -> str:
  return dx.dt.resolve_datetime_string("begin_of_this_fiscal_year")

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("tomorrow")

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_trial_balance: pl.DataFrame

  # transform
  query_fact_trial_balance = """
  declare @from_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  declare @to_date nvarchar(10) = convert(nchar(10), getdate(), 23);
  --[QUERY]--
  select
    -- tb.branch_code branch_code, -- branch
    -- tb.category_code category_code, -- category
    -- tb.customer_id customer_id, -- customer
    left(tb.account_code, 1) root_account_code,
    left(tb.account_code, 3) group_account_code,
    tb.account_code account_code,
    sum(tb.open_debit_amount) open_debit_amount,
    sum(tb.open_credit_amount) open_credit_amount,
    sum(tb.incurred_debit_amount) incurred_debit_amount,
    sum(tb.incurred_credit_amount) incurred_credit_amount,
    sum(tb.close_debit_amount) close_debit_amount,
    sum(tb.CloseCreditAmount) close_credit_amount
  from (
    select
      -- tb.branch_code branch_code, -- branch
      -- tb.category_code category_code, -- category
      -- tb.customer_id customer_id, -- customer
      tb.account_code account_code,
      tb.open_debit_amount open_debit_amount,
      tb.open_credit_amount open_credit_amount,
      tb.incurred_debit_amount incurred_debit_amount,
      tb.incurred_credit_amount incurred_credit_amount,
      tb.close_debit_amount close_debit_amount,
      tb.close_credit_amount CloseCreditAmount
    from (
      select
        -- tb.branch_code branch_code, -- branch
        -- tb.category_code category_code, -- category
        -- tb.customer_id customer_id, -- customer
        tb.account_code account_code,
        case
          when sum(tb.open_debit_amount) > sum(tb.open_credit_amount)
            then sum(tb.open_debit_amount) - sum(tb.open_credit_amount)
          else 0
        end open_debit_amount,
        case
          when sum(tb.open_credit_amount) > sum(tb.open_debit_amount)
            then sum(tb.open_credit_amount) - sum(tb.open_debit_amount)
          else 0
        end open_credit_amount,
        sum(tb.incurred_debit_amount) incurred_debit_amount,
        sum(tb.incurred_credit_amount) incurred_credit_amount,
        case
          when sum(tb.open_debit_amount) + sum(tb.incurred_debit_amount) > sum(tb.open_credit_amount) + sum(tb.incurred_credit_amount)
            then sum(tb.open_debit_amount) + sum(tb.incurred_debit_amount) - sum(tb.open_credit_amount) - sum(tb.incurred_credit_amount)
          else 0
        end close_debit_amount,
        case
          when sum(tb.open_credit_amount) + sum(tb.incurred_credit_amount) > sum(tb.open_debit_amount) + sum(tb.incurred_debit_amount)
            then sum(tb.open_credit_amount) + sum(tb.incurred_credit_amount) - sum(tb.open_debit_amount) - sum(tb.incurred_debit_amount)
          else 0
        end close_credit_amount
      from
        ((
          select
            -- ob.AreaCode branch_code, -- branch
            -- ob.ItemCatgCode category_code,  -- category
            -- ob.CustomerId customer_id, -- customer
            ob.Account account_code,
            ob.DebitAmount open_debit_amount,
            ob.CreditAmount open_credit_amount,
            0 incurred_debit_amount,
            0 incurred_credit_amount
          from B30OpenBalance ob
          where ob.IsActive = 1 and ob.IsGroup = 0 and ob.BranchCode = 'A01'
            and ob.Year = year(@from_date)
        ) union all (
          select
            -- gl.AreaCode branch_code, -- branch
            -- gl.ItemCatgCode category_code, -- category
            -- gl.CustomerId customer_id, -- customer
            gl.Account account_code,
            gl.DebitAmount open_debit_amount,
            gl.CreditAmount open_credit_amount,
            0 incurred_debit_amount,
            0 incurred_credit_amount
          from B30GeneralLedger gl
          where gl.IsActive = 1 and gl.BranchCode = 'A01'
            and gl.DocDate between datefromparts(year(@from_date), 1, 1) and dateadd(day, -1, @from_date)
        ) union all (
          select
            -- gl.AreaCode branch_code, -- branch
            -- gl.ItemCatgCode category_code, -- category
            -- gl.CustomerId customer_id, -- customer
            gl.Account account_code,
            0 open_debit_amount,
            0 open_credit_amount,
            gl.DebitAmount incurred_debit_amount,
            gl.CreditAmount incurred_credit_amount
          from B30GeneralLedger gl
          where gl.IsActive = 1 and gl.BranchCode = 'A01'
            and gl.DocDate between @from_date and @to_date
        )) tb
      group by
        -- tb.branch_code, -- branch
        -- tb.category_code, -- category
        -- tb.customer_id, -- customer
        tb.account_code
    ) tb
    where 
      tb.open_debit_amount <> 0
      or tb.open_credit_amount <> 0
      or tb.incurred_debit_amount <> 0
      or tb.incurred_credit_amount <> 0
      or tb.close_debit_amount <> 0
      or tb.close_credit_amount <> 0
  ) tb
  group by
    -- tb.branch_code, -- branch
    -- tb.category_code, -- category
    -- tb.customer_id, -- customer
    tb.account_code
  """
  df_fact_trial_balance = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_fact_trial_balance, params={
    "from_date": from_date,
    "to_date": to_date,
  })

  return df_fact_trial_balance
