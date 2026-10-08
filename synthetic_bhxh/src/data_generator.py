import pandas as pd
import numpy as np
import yaml
import os
import uuid
import time
from pathlib import Path

# Load config
with open("synthetic_bhxh/config/params.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

RATES = config['rates']
MIN_WAGES = config['min_wage_regions']

PROV_KEYS = list(config['provinces'].keys())
PROV_WEIGHTS = [config['provinces'][k]['weight'] for k in PROV_KEYS]

LAST_NAMES = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý"]
FIRST_NAMES_M = ["Anh", "Bình", "Cường", "Dũng", "Đức", "Hải", "Hiếu", "Hoàng", "Huy", "Hùng", "Khánh", "Minh", "Nam", "Phong", "Phúc", "Thành", "Thiên", "Thịnh", "Trung", "Tuấn", "Việt"]
FIRST_NAMES_F = ["An", "Châu", "Chi", "Diệp", "Hà", "Hân", "Hoa", "Hương", "Hồng", "Linh", "Ly", "Mai", "Ngọc", "Nhung", "Phương", "Quỳnh", "Thảo", "Thu", "Trang", "Trâm", "Uyên", "Yến"]

def generate_units_table(num_units, seed):
    np.random.seed(seed)
    unit_ids = np.arange(1, num_units + 1)
    ma_don_vi = np.char.add("TZ", np.char.zfill(unit_ids.astype(str), 6))
    
    tinh = np.random.choice(PROV_KEYS, size=num_units, p=PROV_WEIGHTS)
    vung = np.random.choice(["V1", "V2", "V3", "V4"], size=num_units, p=[0.4, 0.3, 0.2, 0.1])
    
    prefixes = ["Công ty TNHH", "Công ty CP", "Doanh nghiệp TN", "HTX"]
    suffixes = ["Thương Mại", "Dịch Vụ", "Sản Xuất", "Xây Dựng", "Công Nghệ", "Phát Triển", "Đầu Tư"]
    
    names = np.random.choice(prefixes, size=num_units)
    names = np.char.add(names, " ")
    names = np.char.add(names, np.random.choice(suffixes, size=num_units))
    names = np.char.add(names, " ")
    names = np.char.add(names, ma_don_vi)
    
    df_units = pd.DataFrame({
        'MA_DON_VI': ma_don_vi,
        'TEN_DON_VI': names,
        'MA_TINH': tinh,
        'VUNG_LUONG_TOI_THIEU': vung
    })
    
    # Pareto distribution for details per unit (less extreme than Zipf)
    probs = np.random.pareto(2.0, size=num_units)
    probs = probs / probs.sum()
    df_units['PROB'] = probs
    return df_units

def get_master_counts(target_masters, seed):
    np.random.seed(seed)
    # Lognormal, then clip
    batch = np.random.lognormal(mean=2.2, sigma=0.8, size=target_masters)
    batch = np.clip(np.round(batch), 1, 60).astype(int)
    return batch

def get_min_wage_for_date(date_str, vung_arr):
    # Map date_str (YYYY-MM) to min wage
    year = int(date_str[:4])
    if year < 2015: y_key = "2010"
    elif year < 2020: y_key = "2015"
    elif year < 2022: y_key = "2020"
    elif year < 2024: y_key = "2022"
    else: y_key = "2024"
    
    wages = config['min_wage_regions'][y_key]
    return pd.Series(vung_arr).map(wages).values

def generate_chunk(start_master_idx, lengths, seed_offset, global_seed, df_units):
    np.random.seed(global_seed + seed_offset)
    num_masters = len(lengths)
    num_details = lengths.sum()

    master_ids = np.arange(start_master_idx, start_master_idx + num_masters)
    so_so_bhxh = np.char.add("79", np.char.zfill(master_ids.astype(str), 8))

    # Gen Master
    is_male = np.random.choice([True, False], size=num_masters, p=config['demographics']['gender_ratio'])
    gioi_tinh = np.where(is_male, "Nam", "Nữ")
    
    ln = np.random.choice(LAST_NAMES, size=num_masters)
    mn = np.where(is_male, np.random.choice(["Văn", "Đức", "Hữu", "Thái", "Trọng"], size=num_masters), 
                           np.random.choice(["Thị", "Ngọc", "Thu", "Thanh", "Bích"], size=num_masters))
    fn = np.where(is_male, np.random.choice(FIRST_NAMES_M, size=num_masters), 
                           np.random.choice(FIRST_NAMES_F, size=num_masters))
    ho_ten = np.char.add(np.char.add(ln, " "), np.char.add(np.char.add(mn, " "), fn))

    # Tuổi phân bố tự nhiên hơn (18-59)
    # Lognormal skewed to younger age
    age = np.random.lognormal(mean=3.2, sigma=0.3, size=num_masters)
    age = np.clip(np.round(age), 18, 59).astype(int)
    birth_years = config['dates']['end_year'] - age
    birth_months = np.random.randint(1, 13, size=num_masters)
    birth_days = np.random.randint(1, 29, size=num_masters)
    ngay_sinh = np.char.add(birth_years.astype(str), np.char.add("-", np.char.add(np.char.zfill(birth_months.astype(str), 2), np.char.add("-", np.char.zfill(birth_days.astype(str), 2)))))
    
    # SO_CCCD synthetic
    cccd_prefix = np.char.add(np.random.choice(["991", "979", "937", "948"], size=num_masters), 
                              np.where(is_male, "0", "1"))
    # Use master_idx to guarantee unique cccd suffix within seed
    cccd_suffix = np.char.zfill((master_ids + 123456).astype(str), 8)
    so_cccd = np.char.add(cccd_prefix, cccd_suffix) 

    # Gen Detail basic tracking
    detail_so_so = np.repeat(so_so_bhxh, lengths)
    detail_master_idx = np.repeat(master_ids, lengths)
    detail_idx_within = np.concatenate([np.arange(l) for l in lengths])
    
    # T7: ID_CHI_TIET unique and deterministic
    # master_idx << 8 | detail_idx_within (max 60 details < 256)
    # This guarantees uniqueness up to 2^56 masters, safe for int64
    id_chi_tiet = (detail_master_idx.astype(np.int64) << 8) | detail_idx_within.astype(np.int64)
    
    df_details = pd.DataFrame({
        'ID_CHI_TIET': id_chi_tiet.astype(str),
        'SO_SO_BHXH': detail_so_so,
        'master_idx': detail_master_idx,
        'detail_idx_within': detail_idx_within
    })
    
    # Start Dates Logic
    start_year_allowed = np.maximum(birth_years + 18, config['dates']['start_year'])
    master_start_year = start_year_allowed + np.random.randint(0, 5, size=num_masters)
    master_start_year = np.minimum(master_start_year, config['dates']['end_year']) # Cap at end_year
    master_start_month = np.random.randint(1, 13, size=num_masters)
    # Ensure start date is not after end date
    master_start_month = np.where((master_start_year == config['dates']['end_year']) & (master_start_month > config['dates']['end_month']), config['dates']['end_month'], master_start_month)
    
    master_start_dates = np.char.add(master_start_year.astype(str), np.char.add("-", np.char.zfill(master_start_month.astype(str), 2)))
    
    # Create duration and gaps
    durations = np.random.randint(1, 36, size=num_details)
    gaps = np.where(detail_idx_within == 0, 0, np.random.randint(0, 12, size=num_details))
    
    # Small probability of overlap for T8 (overlap by negative gap)
    overlap_mask = (np.random.rand(num_details) < 0.01) & (detail_idx_within > 0)
    gaps[overlap_mask] = -1
    
    df_details['duration'] = durations
    df_details['gap'] = gaps
    
    # Calculate TU_THANG / DEN_THANG
    # We must do cumsum inside each master_idx
    df_details['cum_duration'] = df_details.groupby('master_idx')['duration'].cumsum()
    df_details['cum_gap'] = df_details.groupby('master_idx')['gap'].cumsum()
    
    # Get master start dates broadcasted to details
    master_start_dates_repeated = np.repeat(master_start_dates, lengths)
    base_dates = pd.to_datetime(master_start_dates_repeated + "-01")
    
    start_offsets = df_details['cum_duration'] - df_details['duration'] + df_details['cum_gap']
    
    tu_thang_dt = base_dates + pd.to_timedelta(start_offsets * 30.44, unit='D')
    den_thang_dt = tu_thang_dt + pd.to_timedelta((df_details['duration'] - 1) * 30.44, unit='D')
    
    max_dt = pd.to_datetime(f"{config['dates']['end_year']}-{config['dates']['end_month']:02d}-01")
    
    # Cap dates
    tu_thang_dt = tu_thang_dt.clip(upper=max_dt)
    den_thang_dt = den_thang_dt.clip(upper=max_dt)
    
    df_details['TU_THANG_DT'] = tu_thang_dt
    df_details['DEN_THANG_DT'] = den_thang_dt
    
    # Format dates. T8: Mixed formats (YYYY-MM vs YYYYMM)
    date_format_mask = np.random.rand(num_details) < 0.2
    tu_thang = tu_thang_dt.dt.strftime("%Y-%m")
    tu_thang = np.where(date_format_mask, tu_thang_dt.dt.strftime("%Y%m"), tu_thang)
    
    den_thang = den_thang_dt.dt.strftime("%Y-%m")
    den_thang = np.where(date_format_mask, den_thang_dt.dt.strftime("%Y%m"), den_thang)
    
    # For ongoing periods (DEN_THANG reached max_dt), set DEN_THANG = null
    ongoing_mask = (den_thang_dt >= max_dt) & (detail_idx_within == np.repeat(lengths - 1, lengths))
    den_thang = np.where(ongoing_mask, None, den_thang)
    
    df_details['TU_THANG'] = tu_thang
    df_details['DEN_THANG'] = den_thang
    df_details['SO_THANG_DONG'] = df_details['duration']
    
    # Update master NGAY_THAM_GIA_DAU_TIEN
    master_df = pd.DataFrame({
        'SO_SO_BHXH': so_so_bhxh,
        'HO_TEN': ho_ten,
        'NGAY_SINH': ngay_sinh,
        'GIOI_TINH': gioi_tinh,
        'SO_CCCD': so_cccd,
        'MA_TINH_THUONG_TRU': np.random.choice(PROV_KEYS, size=num_masters, p=PROV_WEIGHTS),
        'DIA_CHI': np.char.add("Số ", np.char.add(np.random.randint(1, 999, size=num_masters).astype(str), " Đường XYZ, Tỉnh ABC")),
        'SO_DIEN_THOAI': np.char.add(np.random.choice(["09", "08", "03", "07"], size=num_masters), np.char.zfill((master_ids * 17 % 100000000).astype(str), 8)),
        'DAN_TOC': np.random.choice(["Kinh", "Tày", "Thái", "Mường", "Khmer"], size=num_masters, p=[0.85, 0.05, 0.05, 0.03, 0.02]),
        'QUOC_TICH': "Việt Nam",
        'NGAY_THAM_GIA_DAU_TIEN': master_start_dates,
        'TRANG_THAI_SO': np.random.choice(["Đang tham gia", "Đã chốt sổ", "Bảo lưu"], size=num_masters, p=[0.7, 0.1, 0.2]),
        'NGAY_CAP_NHAT': "2024-10-01",
        'MA_DOT_NAP': f"BATCH_{global_seed}_2024"
    })
    
    # Assign units
    # A master has a base unit, and changes with some prob
    master_base_units = np.random.choice(df_units['MA_DON_VI'], size=num_masters, p=df_units['PROB'])
    detail_units = np.repeat(master_base_units, lengths)
    unit_change_mask = (np.random.rand(num_details) < 0.1) & (detail_idx_within > 0)
    changed_units = np.random.choice(df_units['MA_DON_VI'], size=num_details, p=df_units['PROB'])
    detail_units = np.where(unit_change_mask, changed_units, detail_units)
    
    # Merge unit info
    unit_info = df_units.set_index('MA_DON_VI').reindex(detail_units).reset_index()
    
    df_details['MA_DON_VI'] = detail_units
    df_details['TEN_DON_VI'] = unit_info['TEN_DON_VI'].values
    df_details['MA_TINH'] = unit_info['MA_TINH'].values
    df_details['VUNG_LUONG_TOI_THIEU'] = unit_info['VUNG_LUONG_TOI_THIEU'].values
    df_details['MA_NGANH_KT'] = np.random.choice(["C", "G", "J", "K", "O"], size=num_details)
    df_details['LOAI_HINH_DN'] = np.random.choice(["FDI", "NN", "Cổ phần", "TNHH"], size=num_details)
    
    # Salary logic. Continues, increases over time
    base_salaries = np.random.lognormal(mean=np.log(8000000), sigma=0.4, size=num_details)
    
    # Apply inflation (~5% per year) based on start_year
    elapsed_years = tu_thang_dt.dt.year - config['dates']['start_year']
    inflation_multiplier = np.power(1.05, elapsed_years)
    base_salaries = base_salaries * inflation_multiplier
    
    tu_thang_dt_str = tu_thang_dt.dt.strftime("%Y-%m")
    min_wages_mapped = get_min_wage_for_date(tu_thang_dt_str.iloc[0], df_details['VUNG_LUONG_TOI_THIEU'].values) # Simplified
    # Correct mapping per row:
    min_wages_mapped = np.array([get_min_wage_for_date(d, [v])[0] for d, v in zip(tu_thang_dt_str, df_details['VUNG_LUONG_TOI_THIEU'].values)])
    
    muc_luong = np.maximum(base_salaries, min_wages_mapped).astype(int)
    
    # Mixed rounding (some round 100k, some exact)
    round_mask = np.random.rand(num_details) > 0.3
    muc_luong = np.where(round_mask, (muc_luong // 100000) * 100000, muc_luong)
    
    df_details['MUC_LUONG'] = muc_luong
    
    # HE_SO_LUONG only for state-owned (NN)
    df_details['HE_SO_LUONG'] = np.where(df_details['LOAI_HINH_DN'] == 'NN', np.random.choice([2.34, 3.00, 4.40], size=num_details), None)
    df_details['PHU_CAP'] = np.where(np.random.rand(num_details) > 0.8, np.random.randint(500000, 2000000, size=num_details), None)
    
    phu_cap_val = pd.Series(df_details['PHU_CAP']).fillna(0).values
    base_for_insurance = df_details['MUC_LUONG'] + phu_cap_val
    cap = config['base_salary'] * 20
    base_for_insurance = np.minimum(base_for_insurance, cap)
    
    df_details['MUC_DONG_BHXH'] = base_for_insurance
    
    df_details['LOAI_HOP_DONG'] = np.random.choice(["HĐ KTH", "HĐ 1-3 năm", "HĐ < 1 năm", "HĐ Thử việc"], size=num_details, p=[0.4, 0.4, 0.1, 0.1])
    
    # Tiền BHTN, TNLĐ không áp dụng cho hợp đồng thử việc hoặc < 1 tháng (giả định < 1 năm có tỉ lệ không đóng)
    bhtn_mask = df_details['LOAI_HOP_DONG'].isin(["HĐ KTH", "HĐ 1-3 năm"])
    
    df_details['TIEN_BHXH_NLD'] = (base_for_insurance * RATES['bhxh'] * 8 / 25.5).astype(int)
    df_details['TIEN_BHXH_NSDLD'] = (base_for_insurance * RATES['bhxh'] * 17.5 / 25.5).astype(int)
    df_details['TIEN_BHYT'] = (base_for_insurance * RATES['bhyt']).astype(int)
    df_details['TIEN_BHTN'] = np.where(bhtn_mask, (base_for_insurance * RATES['bhtn']).astype(int), None)
    df_details['TIEN_TNLD_BNN'] = np.where(bhtn_mask, (base_for_insurance * RATES['tnld_bnn']).astype(int), None)
    
    df_details['CHUC_DANH'] = np.random.choice(["Nhân viên", "Chuyên viên", "Trưởng phòng", "Công nhân"], size=num_details)
    df_details['TRANG_THAI_DONG'] = np.random.choice(["Đã đóng", "Nợ đọng"], size=num_details, p=[0.95, 0.05])
    
    # NGAY_NOP: allow mixed format
    delay = np.random.exponential(scale=15, size=num_details)
    ngay_nop_dt = den_thang_dt.fillna(pd.Timestamp.now()) + pd.to_timedelta(delay, unit='d')
    ngay_nop_mask = np.random.rand(num_details) < 0.2
    ngay_nop = ngay_nop_dt.dt.strftime("%Y-%m-%d")
    ngay_nop = np.where(ngay_nop_mask, ngay_nop_dt.dt.strftime("%d/%m/%Y"), ngay_nop)
    df_details['NGAY_NOP'] = ngay_nop
    
    df_details['KENH_NOP'] = np.random.choice(["Điện tử", "Trực tiếp"], size=num_details, p=[0.9, 0.1])
    df_details['CO_QUAN_BHXH'] = "BHXH " + df_details['MA_TINH']
    df_details['MA_DOT_NAP'] = f"BATCH_{global_seed}_2024"
    df_details['NGAY_CAP_NHAT'] = "2024-10-01"
    
    # NGUON_DU_LIEU 
    df_details['NGUON_DU_LIEU'] = np.random.choice(["HR_SYS_V1", "HR_SYS_V2", "WEB_PORTAL", "MANUAL"], size=num_details, p=[0.6, 0.2, 0.15, 0.05])
    
    # GHI_CHU rỗng chủ yếu, có nhiễu văn bản
    ghi_chu = np.where(np.random.rand(num_details) < 0.05, 
                       np.random.choice(["Đã kiểm tra", "Chờ duyệt", "Sai lệch 1 đồng", "Cập nhật bù"], size=num_details), 
                       None)
    df_details['GHI_CHU'] = ghi_chu
    
    # Intention Errors Injection
    error_rate = config['generation']['error_rate']
    num_errors = int(num_details * error_rate)
    error_idx = np.random.choice(num_details, size=num_errors, replace=False)
    
    error_types = []
    error_keys = []
    for idx in error_idx:
        err_type_choice = np.random.randint(0, 6)
        id_chi_tiet = df_details.at[idx, 'ID_CHI_TIET']
        if err_type_choice == 0:
            df_details.at[idx, 'SO_SO_BHXH'] = None
            error_types.append("NULL_FK")
        elif err_type_choice == 1:
            df_details.at[idx, 'MUC_LUONG'] = -100000
            error_types.append("NEGATIVE_SALARY")
        elif err_type_choice == 2:
            df_details.at[idx, 'TU_THANG'] = "2025-12"
            df_details.at[idx, 'DEN_THANG'] = "2020-01"
            error_types.append("TIME_LOGIC_INVALID")
        elif err_type_choice == 3:
            dup_idx = np.random.randint(0, num_details)
            df_details.at[idx, 'ID_CHI_TIET'] = df_details.at[dup_idx, 'ID_CHI_TIET']
            error_types.append("DUPLICATE_PK")
        elif err_type_choice == 4:
            df_details.at[idx, 'VUNG_LUONG_TOI_THIEU'] = "V5"
            error_types.append("INVALID_CAT_VALUE")
        elif err_type_choice == 5:
            df_details.at[idx, 'CHUC_DANH'] = "Nhân viên"
            error_types.append("UNICODE_FORMAT")
        
        error_keys.append(id_chi_tiet)

    error_df = pd.DataFrame({'ID_CHI_TIET': error_keys, 'LOAI_LOI': error_types})

    # ML Labels Generation
    # M5: Signal depends on features. 
    # ngat_quang_12t based on gap
    master_gap_avg = df_details.groupby('master_idx')['gap'].mean().reindex(master_ids).fillna(0).values
    label_ngat_quang_12t = (master_gap_avg > 2) & (np.random.rand(num_masters) < 0.8)
    
    # tang_luong_12t based on age (younger more likely to increase)
    label_tang_luong_12t = (age < 30) & (np.random.rand(num_masters) < 0.7)
    
    ml_labels = pd.DataFrame({
        'SO_SO_BHXH': so_so_bhxh,
        'as_of_date': "2024-10-01",
        'label_ngat_quang_12t': label_ngat_quang_12t.astype(int),
        'label_tang_luong_12t': label_tang_luong_12t.astype(int)
    })
    
    # M6: Anomaly label. Don't use duration directly. Use combination of odd features.
    # E.g., PHU_CAP > MUC_LUONG + Nợ đọng
    # This combination is not deterministically linked to one single feature column easily.
    is_anomaly = (phu_cap_val > df_details['MUC_LUONG']) & (np.random.rand(num_details) < 0.6)
    ml_anomaly_labels = pd.DataFrame({
        'ID_CHI_TIET': df_details['ID_CHI_TIET'],
        'is_anomaly': is_anomaly.astype(int)
    })

    # Drop internal columns
    df_details = df_details.drop(columns=['master_idx', 'detail_idx_within', 'duration', 'gap', 'cum_duration', 'cum_gap', 'TU_THANG_DT', 'DEN_THANG_DT'])

    return master_df, df_details, error_df, ml_labels, ml_anomaly_labels
