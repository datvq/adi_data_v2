# import
import os
import polars as pl
import lib.dx as dx
import app.shared as shared
from datetime import date, datetime, timedelta

dx.reload(shared)

# default
def output_file_default() -> str:
  return os.path.join(shared.env.data_dir, "staged", "bravo", "fact_sales_plan.parquet")

def from_date_default() -> str:
  return "2024-10-01"

def to_date_default() -> str:
  return dx.dt.resolve_datetime_string("end_of_next_fiscal_year", ffmonth=10)

def execute(from_date: str, to_date: str) -> pl.DataFrame:
  # return
  df_fact_sales_plan: pl.DataFrame

  # transform
  query_fact_sales_plan = """
  declare @from_date nvarchar(10) = '2024-10-01';
  declare @to_date nvarchar(10) = '2025-09-30';
  --[QUERY]--
  select
    kh.Namtaichinh fiscal_year,
    kh.Nam year,
    kh.Thang month,
    datefromparts(kh.Nam, kh.Thang, 1) from_date,
    eomonth(datefromparts(kh.Nam, kh.Thang, 1)) to_date,
    kh.Vu season_code,
    nv.MaChiNhanh saleman_branch_code,
    nv.Manhanvien saleman_code,
    nv.Tennhanvien saleman_name,
    concat(kh.Makh, '_', case
      when sp.Nhom_hang_1 = N'Chung' then N'CHU'
      when sp.Nhom_hang_1 = N'Giống' then N'GIO'
      when sp.Nhom_hang_1 = N'Lúa' then N'LUA'
      when sp.Nhom_hang_1 = N'Phân bón' then N'PHB'
      when sp.Nhom_hang_1 = N'Phân bón lá' then N'PBL'
      when sp.Nhom_hang_1 = N'Rau' then N'RAU'
      when sp.Nhom_hang_1 = N'Thiết bị' then N'TBI'
      when sp.Nhom_hang_1 = N'Thuốc' then N'THU'
      when sp.Nhom_hang_1 = N'Thuốc cỏ' then N'TCO'
      else NULL
    end) manage_code,
    dt.BravoID customer_id,
    kh.Makh customer_code,
    case
      when sp.Nhom_hang_1 = N'Chung' then N'CHU'
      when sp.Nhom_hang_1 = N'Giống' then N'GIO'
      when sp.Nhom_hang_1 = N'Lúa' then N'LUA'
      when sp.Nhom_hang_1 = N'Phân bón' then N'PHB'
      when sp.Nhom_hang_1 = N'Phân bón lá' then N'PBL'
      when sp.Nhom_hang_1 = N'Rau' then N'RAU'
      when sp.Nhom_hang_1 = N'Thiết bị' then N'TBI'
      when sp.Nhom_hang_1 = N'Thuốc' then N'THU'
      when sp.Nhom_hang_1 = N'Thuốc cỏ' then N'TCO'
      else NULL
    end category_code,
    sp.BravoID item_id,
    sp.Ma item_code,
    kh.Soluong quantity,
    case
      when kh.Namtaichinh >= 2024 then round(coalesce(kh.Thanhtien / nullif(kh.Soluong, 0), 0), 1)
      else round(coalesce(isnull(tl.TyleDTT_Giatreo, 0) * kh.Thanhtien / nullif(kh.Soluong, 0), 0), 1)
    end net_price,
    case
      when kh.Namtaichinh >= 2024 then kh.Thanhtien
      else isnull(tl.TyleDTT_Giatreo, 0) * kh.Thanhtien
    end net_amount,
    case
      when kh.Namtaichinh >= 2024 then round(coalesce(kh.lng / nullif(kh.Thanhtien, 0), 0), 4)
      else round(coalesce(isnull(tl.TyleLNG_Giatreo, 0) * kh.Thanhtien / nullif(kh.Soluong, 0), 0), 4)
    end gross_profit_margin,
    case
      when kh.Namtaichinh >= 2024 then kh.lng
      else isnull(tl.TyleLNG_Giatreo, 0) * kh.Thanhtien
    end gross_profit
  from Banhang.dbo.Kehoachbanhang kh
    left join TTChung.dbo.Doituong_ePacific dt on kh.Makh =  dt.Ma
    left join TTChung.dbo.Sanpham sp on kh.Mahang = sp.iD
    left join TTChung.dbo.Phanvung_Log pv on dt.[Vung thi truong] = pv.Vungthitruong and sp.Nhom_hang_1 = pv.Nhomhang
    left join TTChung.dbo.DMNhanvien nv on pv.Ma_nhan_vien = nv.Manhanvien
    left join Banhang.dbo.TyleLNGKehoach tl on nv.MaChiNhanh = tl.Machinhanh and sp.Bravo_Manhomhang4 = tl.Nhomhang
  where kh.TH_KH = N'Kế hoạch đầu vụ'
    and datefromparts(kh.Nam, kh.Thang, 1) between @from_date and @to_date
  order by kh.Nam, kh.Thang asc
  """
  df_fact_sales_plan: pl.DataFrame = dx.ms.read_mssql(uri=shared.env.sm_uri, query=query_fact_sales_plan, params={
    "from_date": from_date,
    "to_date": to_date,
  })

  return df_fact_sales_plan
