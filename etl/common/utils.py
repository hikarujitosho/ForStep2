import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from .config import LOG_PATH, LOG_LEVEL

def setup_logging(module_name: str) -> logging.Logger:
    """ログ設定をセットアップ"""
    LOG_PATH.mkdir(exist_ok=True)
    
    # ログファイル名に日付を含める
    log_file = LOG_PATH / f"{module_name}_{datetime.now().strftime('%Y%m%d')}.log"
    
    # ロガー設定
    logger = logging.getLogger(module_name)
    logger.setLevel(LOG_LEVEL)
    
    # ファイルハンドラー
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(LOG_LEVEL)
    
    # コンソールハンドラー
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    
    # フォーマッター
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # ハンドラーを追加（重複回避）
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

def validate_dataframe(df: pd.DataFrame, required_columns: List[str], table_name: str) -> bool:
    """DataFrameの基本検証"""
    logger = logging.getLogger(__name__)
    
    # 空データチェック
    if df.empty:
        logger.warning(f"{table_name}: データが空です")
        return False
    
    # 必須カラムチェック
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"{table_name}: 必須カラムが不足: {missing_columns}")
        return False
    
    # 重複チェック（主キーが存在する場合）
    if 'id' in df.columns or any('_id' in col for col in df.columns):
        id_columns = [col for col in df.columns if col.endswith('_id') or col == 'id']
        if id_columns:
            duplicates = df.duplicated(subset=id_columns).sum()
            if duplicates > 0:
                logger.warning(f"{table_name}: 重複レコード {duplicates} 件を検出")
    
    logger.info(f"{table_name}: データ検証完了 ({len(df)} rows, {len(df.columns)} columns)")
    return True

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrameのクリーニング"""
    # 先頭・末尾の空白除去
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    # 空文字列をNaNに変換
    df = df.replace('', pd.NA)
    
    return df

def convert_to_date(date_series: pd.Series, format_list: List[str] = None) -> pd.Series:
    """日付文字列を統一フォーマットに変換"""
    if format_list is None:
        format_list = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']
    
    result = pd.Series(index=date_series.index, dtype='datetime64[ns]')
    
    for fmt in format_list:
        try:
            mask = result.isna()
            if mask.any():
                parsed = pd.to_datetime(date_series[mask], format=fmt, errors='coerce')
                result.loc[mask] = parsed
        except:
            continue
    
    return result

def calculate_year_month(date_series: pd.Series) -> pd.Series:
    """日付からYYYY-MM形式の年月文字列を生成"""
    return pd.to_datetime(date_series).dt.strftime('%Y-%m')

def safe_divide(numerator: pd.Series, denominator: pd.Series, default_value=None) -> pd.Series:
    """安全な除算（ゼロ除算対応）"""
    result = pd.Series(index=numerator.index, dtype=float)
    mask = (denominator != 0) & denominator.notna() & numerator.notna()
    result.loc[mask] = numerator.loc[mask] / denominator.loc[mask]
    
    if default_value is not None:
        result = result.fillna(default_value)
    
    return result

def format_currency(amount: float, currency: str = 'JPY') -> str:
    """金額フォーマット"""
    if pd.isna(amount):
        return 'N/A'
    
    if currency == 'JPY':
        return f"¥{amount:,.0f}"
    else:
        return f"{currency} {amount:,.2f}"

def get_file_list(directory: Path, pattern: str = "*.csv") -> List[Path]:
    """ディレクトリ内のファイル一覧を取得"""
    if not directory.exists():
        return []
    return list(directory.glob(pattern))