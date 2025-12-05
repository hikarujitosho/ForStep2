import pandas as pd
from pathlib import Path

def split_monthly_inventory(input_file, year_month_col, cutoff_year=2025):
    """
    月次在庫履歴をyear_monthカラムで分割
    """
    print(f"\n{'='*100}")
    print(f"月次在庫履歴 分割処理")
    print(f"{'='*100}")
    
    # ファイル読み込み
    print(f"\n📂 ファイル読み込み中: {input_file}")
    df = pd.read_csv(input_file)
    print(f"   総レコード数: {len(df):,}")
    print(f"   カラム: {', '.join(df.columns.tolist())}")
    
    # year_monthから年を抽出（形式: YYYY-MM）
    print(f"\n🔍 年月解析中: カラム '{year_month_col}'")
    df['year'] = df[year_month_col].str[:4].astype(int)
    
    # 年別レコード数
    year_counts = df['year'].value_counts().sort_index()
    print(f"\n📊 年別レコード数:")
    for year, count in year_counts.items():
        print(f"   {year}年: {count:,} レコード")
    
    # 分割
    df_before = df[df['year'] < cutoff_year].copy()
    df_after = df[df['year'] >= cutoff_year].copy()
    
    # year列を削除
    df_before = df_before.drop('year', axis=1)
    df_after = df_after.drop('year', axis=1)
    
    print(f"\n✂️ 分割結果:")
    print(f"   {cutoff_year}年以前: {len(df_before):,} レコード")
    print(f"   {cutoff_year}年以降: {len(df_after):,} レコード")
    
    # 出力ファイル名生成
    input_path = Path(input_file)
    output_before = input_path.parent / f"{input_path.stem}_2024以前.csv"
    output_after = input_path.parent / f"{input_path.stem}_2025以降.csv"
    
    # ファイル保存
    print(f"\n💾 ファイル保存中:")
    df_before.to_csv(output_before, index=False, encoding='utf-8-sig')
    print(f"   ✅ {output_before.name}")
    
    df_after.to_csv(output_after, index=False, encoding='utf-8-sig')
    print(f"   ✅ {output_after.name}")
    
    print(f"\n{'='*100}")
    print("✅ 分割完了")
    print(f"{'='*100}")
    
    return True

def verify_wms_consistency():
    """
    WMSデータの整合性検証
    """
    print(f"\n{'='*100}")
    print("WMSデータ整合性検証")
    print(f"{'='*100}")
    
    base_path = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze\WMS")
    
    # 月次在庫履歴の検証
    df_before = pd.read_csv(base_path / '月次在庫履歴_2024以前.csv')
    df_after = pd.read_csv(base_path / '月次在庫履歴_2025以降.csv')
    
    print(f"\n📊 月次在庫履歴:")
    print(f"   2024年以前: {len(df_before):,} レコード")
    print(f"   2025年以降: {len(df_after):,} レコード")
    print(f"   合計: {len(df_before) + len(df_after):,} レコード")
    
    # 重複チェック
    print(f"\n🔍 重複チェック:")
    # product_id + location_id + year_monthで一意性確認
    df_before['key'] = df_before['product_id'] + '_' + df_before['location_id'] + '_' + df_before['year_month']
    df_after['key'] = df_after['product_id'] + '_' + df_after['location_id'] + '_' + df_after['year_month']
    
    keys_before = set(df_before['key'])
    keys_after = set(df_after['key'])
    overlap = keys_before & keys_after
    
    if len(overlap) == 0:
        print(f"   ✅ 重複なし - 全ての在庫レコードがユニーク")
    else:
        print(f"   ❌ 重複あり: {len(overlap)}件")
        print(f"      例: {list(overlap)[:5]}")
    
    # 年月範囲の確認
    print(f"\n📅 年月範囲:")
    print(f"   2024年以前:")
    year_months_before = sorted(df_before['year_month'].unique())
    print(f"      最小: {year_months_before[0]}, 最大: {year_months_before[-1]}")
    print(f"      月数: {len(year_months_before)}ヶ月")
    
    print(f"   2025年以降:")
    year_months_after = sorted(df_after['year_month'].unique())
    print(f"      最小: {year_months_after[0]}, 最大: {year_months_after[-1]}")
    print(f"      月数: {len(year_months_after)}ヶ月")
    
    # 製品・拠点の統計
    print(f"\n📦 製品・拠点統計:")
    print(f"   2024年以前:")
    print(f"      ユニーク製品数: {df_before['product_id'].nunique()}")
    print(f"      ユニーク拠点数: {df_before['location_id'].nunique()}")
    
    print(f"   2025年以降:")
    print(f"      ユニーク製品数: {df_after['product_id'].nunique()}")
    print(f"      ユニーク拠点数: {df_after['location_id'].nunique()}")
    
    # 現在在庫の確認
    df_current = pd.read_csv(base_path / '現在在庫.csv')
    print(f"\n📌 現在在庫（分割対象外）:")
    print(f"   レコード数: {len(df_current):,}")
    print(f"   ユニーク製品数: {df_current['product_id'].nunique()}")
    print(f"   ユニーク拠点数: {df_current['location_id'].nunique()}")
    print(f"   最終更新: {df_current['last_updated_timestamp'].iloc[0]}")
    
    print(f"\n{'='*100}")
    print("✅ WMSデータ整合性検証完了")
    print(f"{'='*100}")

def main():
    # 月次在庫履歴を分割
    input_file = r"C:\Users\PC\dev\ForStep2\data\Bronze\WMS\月次在庫履歴.csv"
    split_monthly_inventory(input_file, 'year_month', cutoff_year=2025)
    
    # 整合性検証
    verify_wms_consistency()

if __name__ == "__main__":
    main()
