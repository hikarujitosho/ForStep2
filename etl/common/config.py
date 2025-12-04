import os
import logging
from pathlib import Path

# プロジェクトルート
PROJECT_ROOT = Path(__file__).parent.parent.parent

# データベース設定
DATABASE_PATH = PROJECT_ROOT / "database" / "data_lake.db"

# データパス
BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "Bronze"
SILVER_DATA_PATH = PROJECT_ROOT / "data" / "Silver"
GOLD_DATA_PATH = PROJECT_ROOT / "data" / "Gold"

# ログ設定
LOG_PATH = PROJECT_ROOT / "logs"
LOG_LEVEL = logging.INFO

# ビジネスロジック設定
BUSINESS_RULES = {
    "ev_criteria": {"item_hierarchy": "EV"},
    "safety_equipment_criteria": {"detail_category": "safety_equipped"},
    "emergency_transport_type": "expedite",
    "direct_material_account_group": "DIRECT_MATERIAL",
    "indirect_material_account_group": "MRO"
}

# 通貨・時間設定
DEFAULT_CURRENCY = "JPY"
DAYS_PER_MONTH = 30
DEFAULT_TIMEZONE = "Asia/Tokyo"