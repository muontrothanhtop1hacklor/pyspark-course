"""
Common Data Contract, Schemas & Utilities for PySpark Exercises
"""

from .schemas import (
    RAW_ORDER_SCHEMA,
    VALID_ORDER_SCHEMA,
    EXTENDED_ORDER_SCHEMA,
    CUSTOMER_SCHEMA,
)
from .provinces import CANONICAL_PROVINCES, normalize_province
from .rules import classify_segment, VALID_STATUSES
