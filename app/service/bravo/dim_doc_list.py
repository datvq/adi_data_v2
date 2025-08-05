# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "dim_doc_list.parquet")

def execute() -> pl.DataFrame:
  # return
  df_dim_doc_list: pl.DataFrame

  # transform
  query_dim_branch = """
  select
    dmct.IsActive is_active,
    dmct.DocNoFormatStyle doc_no_format,
    dmct.Ma_Ct doc_code,
    dmct.Ten_Ct doc_name,
    dmct.Ten_Ct_English doc_name_en,
    dmct.TransTypeCode trans_type_code,
    trans_type.Name trans_type_name,
    dmct.KeyColumnName key_name,
    dmct.TableHeader header_table,
    dmct.Ct0_TableName detail_table,
    dmct.TableDetail relate_tables,
    dmct.Stt_Ntxt use_fifo,
    dmct.UsingEmployeeCode use_EmployeeCode,
    dmct.UsingDeptId use_DeptId,
    dmct.UsingCustomerId use_CustomerId,
    dmct.UsingExpenseCatgId use_ExpenseCatgId,
    dmct.UsingProductId use_ProductId,
    dmct.UsingBizDocId_C1 use_BizDocId_C1,
    dmct.UsingBizDocId_C2 use_BizDocId_C2,
    dmct.UsingBizDocId_LC use_BizDocId_LC,
    dmct.UsingBizDocId_PO use_BizDocId_PO,
    dmct.UsingBizDocId_SO use_BizDocId_SO,
    dmct.UsingStatsDocId_WO use_StatsDocId_WO,
    dmct.UsingWarehouseId use_WarehouseId,
    dmct.UsingTransCode use_TransCode,
    dmct.UsingAccount use_Account,
    dmct.UsingCrspAccount use_CrspAccount,
    dmct.UsingItemLotCode use_ItemLotCode,
    dmct.UsingRemitInfo use_RemitInfo,
    dmct.UsingWorkProcessCode use_WorkProcessCode,
    dmct.UsingCostCentreCode use_CostCentreCode,
    dmct.UsingUniformNormId use_UniformNormId,
    dmct.UsingJobCode use_JobCode
  from B00DmCt dmct
    left join B00TransType trans_type on trans_type.Code = dmct.TransTypeCode
  """
  df_dim_doc_list = dx.ms.read_mssql(uri=shared.env.bravo_uri, query=query_dim_branch, params=None)

  return df_dim_doc_list
