import pandas as pd
from pathlib import Path
import sys

def split_csv_by_year(input_file, date_column, cutoff_year=2025):
    """
    CSVファイルを指定年で分割する
    
    Parameters:
    - input_file: 入力CSVファイルパス
    - date_column: 日付を含むカラム名
    - cutoff_year: 分割基準年（この年以降が新ファイル）
    """
    print(f"\n{'='*80}")
    print(f"CSVファイル分割処理: {Path(input_file).name}")
    print(f"{'='*80}")
    
    # CSVファイル読み込み
    print(f"\n📂 ファイル読み込み中: {input_file}")
    df = pd.read_csv(input_file)
    print(f"   総レコード数: {len(df):,}")
    print(f"   カラム: {', '.join(df.columns.tolist())}")
    
    # 日付カラムの確認
    if date_column not in df.columns:
        print(f"\n❌ エラー: カラム '{date_column}' が見つかりません")
        print(f"   利用可能なカラム: {', '.join(df.columns.tolist())}")
        return False
    
    # 日付カラムをdatetime型に変換
    print(f"\n🔍 日付解析中: カラム '{date_column}'")
    df[date_column] = pd.to_datetime(df[date_column])
    
    # 年を抽出
    df['year'] = df[date_column].dt.year
    
    # データの年範囲を表示
    year_counts = df['year'].value_counts().sort_index()
    print(f"\n📊 年別レコード数:")
    for year, count in year_counts.items():
        print(f"   {year}年: {count:,} レコード")
    
    # 2024年以前と2025年以降に分割
    df_before = df[df['year'] < cutoff_year].copy()
    df_after = df[df['year'] >= cutoff_year].copy()
    
    # year列を削除（元のデータ構造を維持）
    df_before = df_before.drop('year', axis=1)
    df_after = df_after.drop('year', axis=1)
    
    print(f"\n✂️ 分割結果:")
    print(f"   {cutoff_year}年以前: {len(df_before):,} レコード")
    print(f"   {cutoff_year}年以降: {len(df_after):,} レコード")
    
    # 出力ファイル名を生成
    input_path = Path(input_file)
    stem = input_path.stem  # 拡張子なしのファイル名
    
    output_before = input_path.parent / f"{stem}_2024以前.csv"
    output_after = input_path.parent / f"{stem}_2025以降.csv"
    
    # ファイル保存
    print(f"\n💾 ファイル保存中:")
    df_before.to_csv(output_before, index=False, encoding='utf-8-sig')
    print(f"   ✅ {output_before}")
    
    df_after.to_csv(output_after, index=False, encoding='utf-8-sig')
    print(f"   ✅ {output_after}")
    
    # サマリー表示
    print(f"\n{'='*80}")
    print("✅ 分割完了")
    print(f"{'='*80}")
    print(f"元ファイル: {input_file}")
    print(f"  → {len(df):,} レコード")
    print(f"\n分割後:")
    print(f"  📁 {output_before.name}")
    print(f"     {len(df_before):,} レコード ({cutoff_year}年以前)")
    print(f"  📁 {output_after.name}")
    print(f"     {len(df_after):,} レコード ({cutoff_year}年以降)")
    
    return True

def main():
    # テスト: 調達伝票_item.csv
    input_file = r"C:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_item.csv"
    date_column = "ship_date"
    
    success = split_csv_by_year(input_file, date_column, cutoff_year=2025)
    
    if success:
        print(f"\n{'='*80}")
        print("🎉 処理が正常に完了しました")
        print(f"{'='*80}")
    else:
        print(f"\n{'='*80}")
        print("❌ 処理中にエラーが発生しました")
        print(f"{'='*80}")
        sys.exit(1)

if __name__ == "__main__":
    main()
