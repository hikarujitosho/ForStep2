import pandas as pd
from pathlib import Path

def split_payroll_data(input_file, period_col, cutoff_year=2025):
    """
    給与データをpayment_periodで分割
    """
    print(f"\n{'='*100}")
    print(f"HR給与データ 分割処理")
    print(f"{'='*100}")
    
    # ファイル読み込み
    print(f"\n📂 ファイル読み込み中: {input_file}")
    df = pd.read_csv(input_file)
    print(f"   総レコード数: {len(df):,}")
    print(f"   カラム: {', '.join(df.columns.tolist())}")
    
    # payment_periodから年を抽出（形式: YYYY-MM）
    print(f"\n🔍 支払期間解析中: カラム '{period_col}'")
    df['year'] = df[period_col].str[:4].astype(int)
    
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

def verify_payroll_consistency():
    """
    給与データの詳細検証
    """
    print(f"\n{'='*100}")
    print("HR給与データ詳細検証")
    print(f"{'='*100}")
    
    base_path = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze\HR")
    
    df_before = pd.read_csv(base_path / '給与テーブル_2024以前.csv')
    df_after = pd.read_csv(base_path / '給与テーブル_2025以降.csv')
    
    print(f"\n📊 給与データ:")
    print(f"   2024年以前: {len(df_before):,} レコード")
    print(f"   2025年以降: {len(df_after):,} レコード")
    print(f"   合計: {len(df_before) + len(df_after):,} レコード")
    
    # 重複チェック
    print(f"\n🔍 重複チェック:")
    payroll_ids_before = set(df_before['payroll_id'])
    payroll_ids_after = set(df_after['payroll_id'])
    overlap = payroll_ids_before & payroll_ids_after
    
    if len(overlap) == 0:
        print(f"   ✅ 重複なし - 全てのpayroll_idがユニーク")
    else:
        print(f"   ❌ 重複あり: {len(overlap)}件")
    
    # 期間範囲
    print(f"\n📅 支払期間範囲:")
    print(f"   2024年以前:")
    periods_before = sorted(df_before['payment_period'].unique())
    print(f"      最小: {periods_before[0]}, 最大: {periods_before[-1]}")
    print(f"      月数: {len(periods_before)}ヶ月")
    
    print(f"   2025年以降:")
    periods_after = sorted(df_after['payment_period'].unique())
    print(f"      最小: {periods_after[0]}, 最大: {periods_after[-1]}")
    print(f"      月数: {len(periods_after)}ヶ月")
    
    # 従業員統計
    print(f"\n👥 従業員統計:")
    print(f"   2024年以前:")
    print(f"      ユニーク従業員数: {df_before['employee_id'].nunique()}")
    print(f"      平均月給支給数: {len(df_before) / len(periods_before):.1f} 人/月")
    
    print(f"   2025年以降:")
    print(f"      ユニーク従業員数: {df_after['employee_id'].nunique()}")
    print(f"      平均月給支給数: {len(df_after) / len(periods_after):.1f} 人/月")
    
    # 部門別統計
    print(f"\n🏢 部門別統計:")
    print(f"   2024年以前:")
    dept_before = df_before['department'].value_counts()
    for dept, count in dept_before.items():
        print(f"      {dept}: {count:,}件")
    
    print(f"   2025年以降:")
    dept_after = df_after['department'].value_counts()
    for dept, count in dept_after.items():
        print(f"      {dept}: {count:,}件")
    
    # 給与統計
    print(f"\n💰 給与統計:")
    print(f"   2024年以前:")
    print(f"      平均基本給: ¥{df_before['base_salary'].mean():,.0f}")
    print(f"      平均残業代: ¥{df_before['overtime_pay'].mean():,.0f}")
    print(f"      平均手当: ¥{df_before['allowances'].mean():,.0f}")
    print(f"      平均控除: ¥{df_before['deductions'].mean():,.0f}")
    print(f"      平均手取り: ¥{df_before['net_salary'].mean():,.0f}")
    print(f"      総人件費: ¥{df_before['net_salary'].sum():,.0f}")
    
    print(f"   2025年以降:")
    print(f"      平均基本給: ¥{df_after['base_salary'].mean():,.0f}")
    print(f"      平均残業代: ¥{df_after['overtime_pay'].mean():,.0f}")
    print(f"      平均手当: ¥{df_after['allowances'].mean():,.0f}")
    print(f"      平均控除: ¥{df_after['deductions'].mean():,.0f}")
    print(f"      平均手取り: ¥{df_after['net_salary'].mean():,.0f}")
    print(f"      総人件費: ¥{df_after['net_salary'].sum():,.0f}")
    
    # 雇用形態
    print(f"\n📋 雇用形態:")
    print(f"   2024年以前:")
    emp_type_before = df_before['employment_type'].value_counts()
    for emp_type, count in emp_type_before.items():
        pct = count / len(df_before) * 100
        print(f"      {emp_type}: {count:,}件 ({pct:.1f}%)")
    
    print(f"   2025年以降:")
    emp_type_after = df_after['employment_type'].value_counts()
    for emp_type, count in emp_type_after.items():
        pct = count / len(df_after) * 100
        print(f"      {emp_type}: {count:,}件 ({pct:.1f}%)")

def main():
    # 給与データを分割
    input_file = r"C:\Users\PC\dev\ForStep2\data\Bronze\HR\給与テーブル.csv"
    split_payroll_data(input_file, 'payment_period', cutoff_year=2025)
    
    # 詳細検証
    verify_payroll_consistency()
    
    print(f"\n{'='*100}")
    print("🎉 HR給与データの分割と検証が完了しました！")
    print(f"{'='*100}")

if __name__ == "__main__":
    main()
