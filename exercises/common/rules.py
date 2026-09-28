"""
Shared Business Rules and Classifications
=========================================
Chuan hoa cac nguong nghiep vu va trang thai giao dich xuyen suot du an.
"""

# Tap hop cac trang thai hop le
VALID_STATUSES = [
    "SUCCESS",
    "COMPLETED",
    "PENDING",
    "SHIPPING",
    "FAILED",
    "CANCELLED",
]

# Bang sua loi chinh ta / viet tat cua status
STATUS_NORMALIZATION_MAP = {
    "succes": "SUCCESS",
    "success": "SUCCESS",
    "completed": "COMPLETED",
    "pending": "PENDING",
    "shipping": "SHIPPING",
    "fail": "FAILED",
    "failed": "FAILED",
    "canceled": "CANCELLED",
    "cancelled": "CANCELLED",
}

# Nguong phan loai don hang / khach hang theo gia tri (VND)
VIP_THRESHOLD = 5_000_000        # >= 5 trieu: VIP / HIGH
STANDARD_THRESHOLD = 1_000_000   # >= 1 trieu: STANDARD / MEDIUM
# Con lai: BASIC / LOW


def classify_segment(amount) -> str:
    """Phan loai khach hang / cap bac don hang theo gia tri giao dich."""
    if amount is None or amount <= 0:
        return "UNKNOWN"
    if amount >= VIP_THRESHOLD:
        return "VIP"
    if amount >= STANDARD_THRESHOLD:
        return "STANDARD"
    return "BASIC"
