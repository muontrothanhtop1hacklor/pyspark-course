"""
Canonical Provinces and Normalization Dictionary
================================================
Tap hop 10 tinh thanh pho chuan dung xuyen suot cac bai tap,
kem tu dien chuan hoa cac bien the chinh ta, viet tat, dau cach.
"""

CANONICAL_PROVINCES = [
    "HaNoi",
    "HoChiMinh",
    "DaNang",
    "CanTho",
    "HaiPhong",
    "NgheAn",
    "ThanhHoa",
    "BinhDuong",
    "DongNai",
    "KhanhHoa",
]

# Bang tra bien the -> ten chuan PascalCase
PROVINCE_NORMALIZATION_MAP = {
    # Ha Noi
    "hanoi": "HaNoi",
    "ha noi": "HaNoi",
    "ha_noi": "HaNoi",
    "hn": "HaNoi",
    "hà nội": "HaNoi",
    # Ho Chi Minh
    "hochiminh": "HoChiMinh",
    "ho chi minh": "HoChiMinh",
    "hcm": "HoChiMinh",
    "tphcm": "HoChiMinh",
    "sai gon": "HoChiMinh",
    "saigon": "HoChiMinh",
    "hồ chí minh": "HoChiMinh",
    # Da Nang
    "danang": "DaNang",
    "da nang": "DaNang",
    "đà nẵng": "DaNang",
    # Can Tho
    "cantho": "CanTho",
    "can tho": "CanTho",
    "cần thơ": "CanTho",
    # Hai Phong
    "haiphong": "HaiPhong",
    "hai phong": "HaiPhong",
    "hải phòng": "HaiPhong",
    # Nghe An
    "nghean": "NgheAn",
    "nghe an": "NgheAn",
    "nghệ an": "NgheAn",
    # Thanh Hoa
    "thanhhoa": "ThanhHoa",
    "thanh hoa": "ThanhHoa",
    "thanh hoá": "ThanhHoa",
    # Binh Duong
    "binhduong": "BinhDuong",
    "binh duong": "BinhDuong",
    "bình dương": "BinhDuong",
    # Dong Nai
    "dongnai": "DongNai",
    "dong nai": "DongNai",
    "đồng nai": "DongNai",
    # Khanh Hoa
    "khanhhoa": "KhanhHoa",
    "khanh hoa": "KhanhHoa",
    "khánh hòa": "KhanhHoa",
}


def normalize_province(prov_str: str) -> str:
    """Chuan hoa chuoi ten tinh/thanh ve dang PascalCase chuan."""
    if not prov_str:
        return None
    cleaned = prov_str.strip().lower()
    return PROVINCE_NORMALIZATION_MAP.get(cleaned, prov_str.strip())
